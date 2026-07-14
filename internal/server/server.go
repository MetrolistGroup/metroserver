package server

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mathrand "math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type Session struct {
	UserID       string
	Username     string
	RoomCode     string
	IsHost       bool
	DisconnectAt time.Time
}

type Room struct {
	Code               string
	Host               *Client
	Clients            map[string]*Client
	PendingJoins       map[string]*Client     // Users waiting for approval
	PendingSuggestions map[string]*Suggestion // Track suggestions waiting for host action
	DisconnectedUsers  map[string]*Session    // Users temporarily disconnected
	State              *RoomState
	BufferingUsers     map[string]bool // Track which users are still buffering
	HostStartPosition  int64           // Host's position when buffering started
	HostDisconnectedAt *time.Time      // When the host disconnected (nil if connected)
	EmptySince         *time.Time      // When the room became empty (nil if not empty)
	mu                 sync.RWMutex
}

// Suggestion represents a track suggestion from a guest
type Suggestion struct {
	ID           string
	FromUserID   string
	FromUsername string
	Track        *TrackInfo
}

// Server is the main WebSocket server
type Server struct {
	rooms      map[string]*Room
	sessions   map[string]*Session // sessionToken -> Session
	clients    map[*Client]bool
	userAgents map[string]int // user agent -> count
	upgrader   websocket.Upgrader
	mu         sync.RWMutex
	rngMu      sync.Mutex
	logger     *zap.Logger
	rng        *mathrand.Rand
	startTime  time.Time // Track when server started for room retention logic
}

const (
	// Grace period for reconnection (increased from 5 to 15 minutes for better recovery)
	ReconnectGracePeriod = 15 * time.Minute
	// How often to clean up expired sessions
	SessionCleanupInterval = 1 * time.Minute
	// How long to keep empty rooms before deleting them
	EmptyRoomCleanupTimeout = 5 * time.Minute
	// How often to check for empty rooms
	EmptyRoomCleanupInterval = 30 * time.Second
	// Minimum time to keep empty rooms after server restart (for reconnection)
	MinRoomRetentionAfterRestart = 2 * time.Minute
	// Security limits
	MaxUsernameLength     = 50
	MaxRoomCodeLength     = 10
	MaxTrackTitleLength   = 200
	MaxTrackArtistLength  = 200
	MaxTrackURLLength     = 2048
	MaxTrackDuration      = 24 * 60 * 60 * 1000 // 24 hours in milliseconds
	MaxQueueSize          = 1000
	MaxPendingJoins       = 100
	MaxPendingSuggestions = 100
	// Connection limits
	MaxClients           = 10000
	MaxRooms             = 10000
	MaxClientsPerRoom    = 100
	MaxReadMessageSize   = 524288 // 512KB (reasonable for queue syncs)
	MaxHeaderBytes       = 65536
	ReadTimeout          = 60 * time.Second
	WriteTimeout         = 10 * time.Second
	IdleTimeout          = 120 * time.Second
	ShutdownTimeout      = 10 * time.Second
	MessageRateWindow    = time.Second
	MaxMessagesPerWindow = 60
)

func NewServer(logger *zap.Logger) *Server {
	s := &Server{
		rooms:      make(map[string]*Room),
		sessions:   make(map[string]*Session),
		clients:    make(map[*Client]bool),
		userAgents: make(map[string]int),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
		},
		logger:    logger,
		rng:       mathrand.New(mathrand.NewSource(time.Now().UnixNano())),
		startTime: time.Now(),
	}

	// Start cleanup goroutines
	go s.cleanupExpiredSessions()
	go s.cleanupEmptyRooms()

	return s
}

func (s *Server) generateRoomCode() string {
	const chars = "1234567890QWERTYUPASDFGHJLKZXCVBNM"
	code := make([]byte, 8)
	s.rngMu.Lock()
	for i := range code {
		code[i] = chars[s.rng.Intn(len(chars))]
	}
	s.rngMu.Unlock()
	return string(code)
}

func (s *Server) generateUserID() string {
	s.rngMu.Lock()
	randNum := s.rng.Intn(10000)
	s.rngMu.Unlock()
	return fmt.Sprintf("user_%d_%d", time.Now().UnixNano(), randNum)
}

func (s *Server) generateSessionToken() string {
	// Use crypto/rand for secure token generation
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		s.logger.Error("Failed to generate secure token", zap.Error(err))
		// Fallback to less secure but functional token
		s.rngMu.Lock()
		tokenNum := s.rng.Intn(1000000)
		s.rngMu.Unlock()
		return fmt.Sprintf("token_%d_%d", time.Now().UnixNano(), tokenNum)
	}
	return hex.EncodeToString(b)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	clientCount := len(s.clients)
	s.mu.RUnlock()
	if clientCount >= MaxClients {
		http.Error(w, "server at connection capacity", http.StatusServiceUnavailable)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Warn("WebSocket upgrade error", zap.Error(err))
		return
	}

	// Track user agent
	ua := r.UserAgent()
	if ua != "" {
		s.mu.Lock()
		s.userAgents[ua]++
		s.mu.Unlock()
	}

	// Use Protobuf codec with compression enabled
	client := newClient(s.generateUserID(), conn)

	s.mu.Lock()
	if len(s.clients) >= MaxClients {
		s.mu.Unlock()
		_ = conn.Close()
		return
	}
	s.clients[client] = true
	s.mu.Unlock()

	go client.writePump(s.logger)
	go client.readPump(s)

	s.logger.Info("Client connected", zap.String("client_id", client.clientID()))
}

func (s *Server) handleMessage(c *Client, data []byte) {
	// Decode message using protobuf codec
	msgType, payloadBytes, err := c.codec.Decode(data)
	if err != nil {
		s.logger.Debug("Invalid message received", zap.String("client_id", c.clientID()), zap.Error(err))
		c.sendError(s.logger, "invalid_message", "Invalid message format")
		return
	}

	if msgType == "" {
		c.sendError(s.logger, "invalid_message", "Message type is required")
		return
	}

	s.logger.Debug("Message received", zap.String("client_id", c.clientID()), zap.String("message_type", msgType), zap.String("format", "protobuf"))

	switch msgType {
	case MsgTypeCreateRoom:
		s.handleCreateRoom(c, payloadBytes)
	case MsgTypeJoinRoom:
		s.handleJoinRoom(c, payloadBytes)
	case MsgTypeLeaveRoom:
		s.leaveRoom(c)
	case MsgTypeApproveJoin:
		s.handleApproveJoin(c, payloadBytes)
	case MsgTypeRejectJoin:
		s.handleRejectJoin(c, payloadBytes)
	case MsgTypePlaybackAction:
		s.handlePlaybackAction(c, payloadBytes)
	case MsgTypeBufferReady:
		s.handleBufferReady(c, payloadBytes)
	case MsgTypeKickUser:
		s.handleKickUser(c, payloadBytes)
	case MsgTypeTransferHost:
		s.handleTransferHost(c, payloadBytes)
	case MsgTypePing:
		c.sendMessage(s.logger, MsgTypePong, nil)
	case MsgTypeRequestSync:
		s.handleRequestSync(c)
	case MsgTypeReconnect:
		s.handleReconnect(c, payloadBytes)
	case MsgTypeSuggestTrack:
		s.handleSuggestTrack(c, payloadBytes)
	case MsgTypeApproveSuggestion:
		s.handleApproveSuggestion(c, payloadBytes)
	case MsgTypeRejectSuggestion:
		s.handleRejectSuggestion(c, payloadBytes)
	case MsgTypeClientCapabilities:
		s.handleClientCapabilities(c, payloadBytes)
	default:
		c.sendError(s.logger, "unknown_message_type", fmt.Sprintf("Unknown message type: %s", msgType))
	}
}

func (s *Server) handleClientCapabilities(c *Client, payload []byte) {
	var p ClientCapabilitiesPayload
	if err := decodePayload(payload, MsgTypeClientCapabilities, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid client capabilities payload")
		return
	}
	if !p.SupportsProtobuf {
		c.sendError(s.logger, "unsupported_client", "Protobuf support is required")
		return
	}
	c.sendMessage(s.logger, MsgTypeServerCapabilities, ServerCapabilitiesPayload{
		SupportsProtobuf:    true,
		SupportsCompression: true,
		ServerVersion:       "1",
	})
}

func (s *Server) closeAllClients() {
	s.mu.RLock()
	clients := make([]*Client, 0, len(s.clients))
	for client := range s.clients {
		clients = append(clients, client)
	}
	s.mu.RUnlock()

	for _, client := range clients {
		if client == nil || client.Conn == nil {
			continue
		}
		_ = client.Conn.Close()
		client.closeSend()
	}
}
