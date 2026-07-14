package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

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

	// Load previous state if exists
	if err := server.LoadState(); err != nil {
		logger.Error("Failed to load previous state", zap.Error(err))
		// Continue anyway - not fatal
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", server.handleWebSocket)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/uas", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		server.mu.RLock()
		uas := make(map[string]int, len(server.userAgents))
		for k, v := range server.userAgents {
			uas[k] = v
		}
		server.mu.RUnlock()
		json.NewEncoder(w).Encode(uas)
	})

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

	// Set up graceful shutdown
	shutdown := make(chan os.Signal, 1)
	shutdownDone := make(chan struct{})
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-shutdown
		defer close(shutdownDone)
		signal.Stop(shutdown)
		logger.Info("Shutdown signal received")

		ctx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			logger.Error("HTTP shutdown failed", zap.Error(err))
		}
		server.closeAllClients()

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
