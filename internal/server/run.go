package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
)

func Run() {
	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	server := NewServer(logger)

	// User-Agent policy (block / advertise Metrolist / rickroll freeloaders).
	policyFile := os.Getenv("UA_POLICY_FILE")
	ua, err := loadUAPolicy(policyFile)
	if err != nil {
		logger.Fatal("Failed to load UA policy", zap.Error(err))
	}
	server.SetUAPolicy(ua)
	logger.Info("User-Agent policy loaded",
		zap.Int("block", len(ua.block)),
		zap.Int("rickroll", len(ua.rickroll)),
		zap.Int("advert", len(ua.advert)))

	databaseFile := os.Getenv("DATABASE_FILE")
	if databaseFile == "" {
		databaseFile = DefaultDatabaseFile
	}
	server.database, err = openDatabase(databaseFile)
	if err != nil {
		logger.Fatal("Failed to open database", zap.Error(err))
	}
	defer server.database.Close()

	// Serving an empty server after a failed restore would overwrite the
	// recoverable state on the next flush.
	if err := server.LoadState(); err != nil {
		logger.Fatal("Failed to load previous state", zap.Error(err))
	}
	go server.runStateFlusher()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", server.handleWebSocket)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	if adminToken := os.Getenv("UA_ADMIN_TOKEN"); adminToken != "" {
		mux.HandleFunc("/uas", server.userAgentsHandler(adminToken))
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Validate port
	if len(port) == 0 || len(port) > 5 {
		logger.Fatal("Invalid port", zap.String("port", port))
	}

	logger.Info("Server starting",
		zap.String("port", port))

	// Configure HTTP server with timeouts for production
	httpServer := &http.Server{
		Addr:           ":" + port,
		Handler:        mux,
		ReadTimeout:    ReadTimeout,
		WriteTimeout:   WriteTimeout,
		IdleTimeout:    IdleTimeout,
		MaxHeaderBytes: MaxHeaderBytes,
	}

	// SIGHUP reloads the User-Agent policy for new connections.
	reload := make(chan os.Signal, 1)
	signal.Notify(reload, syscall.SIGHUP)
	go func() {
		for range reload {
			policy, err := loadUAPolicy(policyFile)
			if err != nil {
				logger.Error("Failed to reload UA policy; keeping the current one", zap.Error(err))
				continue
			}
			server.SetUAPolicy(policy)
			logger.Info("User-Agent policy reloaded")
		}
	}()

	// Set up graceful shutdown
	shutdown := make(chan os.Signal, 1)
	shutdownDone := make(chan struct{})
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-shutdown
		defer close(shutdownDone)
		signal.Stop(shutdown)
		logger.Info("Shutdown signal received")
		// Everything below shares one budget that leaves time for the final save.
		deadline := time.Now().Add(ShutdownTimeout - 2*time.Second)

		server.announceRestart()
		server.Close()
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			logger.Warn("HTTP shutdown incomplete; closing remaining connections", zap.Error(err))
			_ = httpServer.Close()
		}
		server.drainClients(deadline)

		logger.Info("Saving state")
		if err := server.SaveState(); err != nil {
			logger.Error("Failed to save state", zap.Error(err))
		}
		logger.Info("Shutdown complete")
	}()

	if err := httpServer.ListenAndServe(); err != nil {
		if err == http.ErrServerClosed {
			<-shutdownDone
			return
		}
		logger.Fatal("Server failed", zap.Error(err))
	}
}
