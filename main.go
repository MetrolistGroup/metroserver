package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	mathrand "math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// Message types
const (
	// Client -> Server
	MsgTypeCreateRoom         = "create_room"
	MsgTypeJoinRoom           = "join_room"
	MsgTypeLeaveRoom          = "leave_room"
	MsgTypeApproveJoin        = "approve_join"
	MsgTypeRejectJoin         = "reject_join"
	MsgTypePlaybackAction     = "playback_action"
	MsgTypeBufferReady        = "buffer_ready"
	MsgTypeKickUser           = "kick_user"
	MsgTypeTransferHost       = "transfer_host"
	MsgTypePing               = "ping"
	MsgTypeRequestSync        = "request_sync"
	MsgTypeReconnect          = "reconnect"
	MsgTypeSuggestTrack       = "suggest_track"
	MsgTypeApproveSuggestion  = "approve_suggestion"
	MsgTypeRejectSuggestion   = "reject_suggestion"
	MsgTypeClientCapabilities = "client_capabilities"

	// Server -> Client
	MsgTypeRoomCreated        = "room_created"
	MsgTypeJoinRequest        = "join_request"
	MsgTypeJoinApproved       = "join_approved"
	MsgTypeJoinRejected       = "join_rejected"
	MsgTypeUserJoined         = "user_joined"
	MsgTypeUserLeft           = "user_left"
	MsgTypeSyncPlayback       = "sync_playback"
	MsgTypeBufferWait         = "buffer_wait"
	MsgTypeBufferComplete     = "buffer_complete"
	MsgTypeError              = "error"
	MsgTypePong               = "pong"
	MsgTypeHostChanged        = "host_changed"
	MsgTypeKicked             = "kicked"
	MsgTypeSyncState          = "sync_state"
	MsgTypeReconnected        = "reconnected"
	MsgTypeUserReconnected    = "user_reconnected"
	MsgTypeUserDisconnected   = "user_disconnected"
	MsgTypeSuggestionReceived = "suggestion_received"
	MsgTypeSuggestionApproved = "suggestion_approved"
	MsgTypeSuggestionRejected = "suggestion_rejected"
	MsgTypeServerCapabilities = "server_capabilities"
)

// Playback actions
const (
	ActionPlay        = "play"
	ActionPause       = "pause"
	ActionSeek        = "seek"
	ActionSkipNext    = "skip_next"
	ActionSkipPrev    = "skip_prev"
	ActionChangeTrack = "change_track"
	ActionQueueAdd    = "queue_add"
	ActionQueueRemove = "queue_remove"
	ActionQueueClear  = "queue_clear"
	ActionSyncQueue   = "sync_queue"
	ActionSetVolume   = "set_volume"
)

// CreateRoomPayload is for creating a new room
type CreateRoomPayload struct {
	Username string `json:"username"`
}

// RoomCreatedPayload is the response for room creation
type RoomCreatedPayload struct {
	RoomCode     string `json:"room_code"`
	UserID       string `json:"user_id"`
	SessionToken string `json:"session_token"`
}

// JoinRoomPayload is for joining a room
type JoinRoomPayload struct {
	RoomCode string `json:"room_code"`
	Username string `json:"username"`
}

// JoinRequestPayload is sent to the host when someone wants to join
type JoinRequestPayload struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
}

// ApproveJoinPayload is for approving a join request
type ApproveJoinPayload struct {
	UserID string `json:"user_id"`
}

// RejectJoinPayload is for rejecting a join request
type RejectJoinPayload struct {
	UserID string `json:"user_id"`
	Reason string `json:"reason,omitempty"`
}

// JoinApprovedPayload is sent to the user when they are approved
type JoinApprovedPayload struct {
	RoomCode     string     `json:"room_code"`
	UserID       string     `json:"user_id"`
	SessionToken string     `json:"session_token"`
	State        *RoomState `json:"state"`
}

// JoinRejectedPayload is sent to the user when they are rejected
type JoinRejectedPayload struct {
	Reason string `json:"reason"`
}

// UserJoinedPayload is sent when a user joins the room
type UserJoinedPayload struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
}

// UserLeftPayload is sent when a user leaves the room
type UserLeftPayload struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
}

// PlaybackActionPayload is for playback control actions
type PlaybackActionPayload struct {
	Action     string      `json:"action"`
	TrackID    string      `json:"track_id,omitempty"`
	Position   int64       `json:"position,omitempty"` // milliseconds
	TrackInfo  *TrackInfo  `json:"track_info,omitempty"`
	InsertNext bool        `json:"insert_next,omitempty"`
	Queue      []TrackInfo `json:"queue,omitempty"`
	QueueTitle string      `json:"queue_title,omitempty"`
	Volume     float64     `json:"volume"`
	ServerTime int64       `json:"server_time,omitempty"`
}

// Suggestion payloads
type SuggestTrackPayload struct {
	TrackInfo *TrackInfo `json:"track_info"`
}

type SuggestionReceivedPayload struct {
	SuggestionID string     `json:"suggestion_id"`
	FromUserID   string     `json:"from_user_id"`
	FromUsername string     `json:"from_username"`
	TrackInfo    *TrackInfo `json:"track_info"`
}

type ApproveSuggestionPayload struct {
	SuggestionID string `json:"suggestion_id"`
}

type RejectSuggestionPayload struct {
	SuggestionID string `json:"suggestion_id"`
	Reason       string `json:"reason,omitempty"`
}

type SuggestionApprovedPayload struct {
	SuggestionID string     `json:"suggestion_id"`
	TrackInfo    *TrackInfo `json:"track_info"`
}

type SuggestionRejectedPayload struct {
	SuggestionID string `json:"suggestion_id"`
	Reason       string `json:"reason,omitempty"`
}

// TrackInfo contains information about a track
type TrackInfo struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Artist      string `json:"artist"`
	Album       string `json:"album,omitempty"`
	Duration    int64  `json:"duration"` // milliseconds
	Thumbnail   string `json:"thumbnail,omitempty"`
	SuggestedBy string `json:"suggested_by,omitempty"`
}

// BufferReadyPayload is sent when a user has finished buffering
type BufferReadyPayload struct {
	TrackID string `json:"track_id"`
}

// BufferWaitPayload is sent to tell users to wait for buffering
type BufferWaitPayload struct {
	TrackID    string   `json:"track_id"`
	WaitingFor []string `json:"waiting_for"` // user IDs still buffering
}

// BufferCompletePayload is sent when all users have buffered
type BufferCompletePayload struct {
	TrackID string `json:"track_id"`
}

// ErrorPayload is for error messages
type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// RoomState contains the current state of a room
type RoomState struct {
	RoomCode     string      `json:"room_code"`
	HostID       string      `json:"host_id"`
	Users        []UserInfo  `json:"users"`
	CurrentTrack *TrackInfo  `json:"current_track,omitempty"`
	IsPlaying    bool        `json:"is_playing"`
	Position     int64       `json:"position"`    // milliseconds
	LastUpdate   int64       `json:"last_update"` // unix timestamp ms
	Volume       float64     `json:"volume"`
	Queue        []TrackInfo `json:"queue,omitempty"`
}

// UserInfo contains information about a user
type UserInfo struct {
	UserID      string `json:"user_id"`
	Username    string `json:"username"`
	IsHost      bool   `json:"is_host"`
	IsConnected bool   `json:"is_connected"`
}

// KickUserPayload is for kicking a user from the room
type KickUserPayload struct {
	UserID string `json:"user_id"`
	Reason string `json:"reason,omitempty"`
}

// TransferHostPayload is for transferring host role to another user
type TransferHostPayload struct {
	NewHostID string `json:"new_host_id"`
}

// KickedPayload is sent to the user when they are kicked
type KickedPayload struct {
	Reason string `json:"reason"`
}

// HostChangedPayload is sent when the host changes
type HostChangedPayload struct {
	NewHostID   string `json:"new_host_id"`
	NewHostName string `json:"new_host_name"`
}

// SyncStatePayload is sent to a guest when they request current playback state
type SyncStatePayload struct {
	CurrentTrack *TrackInfo  `json:"current_track,omitempty"`
	IsPlaying    bool        `json:"is_playing"`
	Position     int64       `json:"position"`    // milliseconds
	LastUpdate   int64       `json:"last_update"` // unix timestamp ms
	Volume       float64     `json:"volume"`
	Queue        []TrackInfo `json:"queue,omitempty"`
}

// ReconnectPayload is for reconnecting to a room
type ReconnectPayload struct {
	SessionToken string `json:"session_token"`
}

type ClientCapabilitiesPayload struct {
	SupportsProtobuf    bool   `json:"supports_protobuf"`
	SupportsCompression bool   `json:"supports_compression"`
	ClientVersion       string `json:"client_version"`
}

type ServerCapabilitiesPayload struct {
	SupportsProtobuf    bool   `json:"supports_protobuf"`
	SupportsCompression bool   `json:"supports_compression"`
	ServerVersion       string `json:"server_version"`
}

// ReconnectedPayload is sent when successfully reconnected
type ReconnectedPayload struct {
	RoomCode string     `json:"room_code"`
	UserID   string     `json:"user_id"`
	State    *RoomState `json:"state"`
	IsHost   bool       `json:"is_host"`
}

// UserReconnectedPayload is sent to other users when someone reconnects
type UserReconnectedPayload struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
}

// UserDisconnectedPayload is sent when a user temporarily disconnects
type UserDisconnectedPayload struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
}

// Session holds information about a disconnected user for reconnection
type Session struct {
	UserID       string
	Username     string
	RoomCode     string
	IsHost       bool
	DisconnectAt time.Time
}

// Client represents a connected WebSocket client
type Client struct {
	id               atomic.Value // string
	username         atomic.Value // string
	sessionToken     atomic.Value // string
	room             atomic.Pointer[Room]
	Conn             *websocket.Conn
	Send             chan []byte
	closed           bool
	rateWindowStart  time.Time
	rateMessageCount int
	mu               sync.Mutex
	codec            *MessageCodec // Message codec for encoding/decoding
}

func newClient(id string, conn *websocket.Conn) *Client {
	c := &Client{
		Conn:  conn,
		Send:  make(chan []byte, 256),
		codec: NewMessageCodec(true),
	}
	c.setClientID(id)
	return c
}

func loadAtomicString(v *atomic.Value) string {
	raw := v.Load()
	if raw == nil {
		return ""
	}
	return raw.(string)
}

func (c *Client) clientID() string {
	return loadAtomicString(&c.id)
}

func (c *Client) setClientID(id string) {
	c.id.Store(id)
}

func (c *Client) userName() string {
	return loadAtomicString(&c.username)
}

func (c *Client) setUsername(username string) {
	c.username.Store(username)
}

func (c *Client) session() string {
	return loadAtomicString(&c.sessionToken)
}

func (c *Client) setSessionToken(token string) {
	c.sessionToken.Store(token)
}

func (c *Client) currentRoom() *Room {
	return c.room.Load()
}

func (c *Client) setRoom(room *Room) {
	c.room.Store(room)
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *Client) closeSend() {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		if c.Send != nil {
			close(c.Send)
		}
	}
	c.mu.Unlock()
}

func (c *Client) allowMessage(now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.rateWindowStart.IsZero() || now.Sub(c.rateWindowStart) >= MessageRateWindow {
		c.rateWindowStart = now
		c.rateMessageCount = 0
	}

	if c.rateMessageCount >= MaxMessagesPerWindow {
		return false
	}

	c.rateMessageCount++
	return true
}

// Room represents a listening room
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

func (s *Server) cleanupExpiredSessions() {
	ticker := time.NewTicker(SessionCleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.cleanupExpiredSessionsOnce(time.Now())
	}
}

func (s *Server) cleanupExpiredSessionsOnce(now time.Time) {
	minRetentionTime := s.startTime.Add(MinRoomRetentionAfterRestart)

	// First, determine which sessions have expired without holding any room locks.
	s.mu.Lock()
	expired := make([]*Session, 0)
	for token, session := range s.sessions {
		if now.Sub(session.DisconnectAt) > ReconnectGracePeriod {
			expired = append(expired, session)
			delete(s.sessions, token)
			s.logger.Info("Session expired",
				zap.String("user_id", session.UserID),
				zap.String("room_code", session.RoomCode))
		}
	}
	s.mu.Unlock()

	// Now process the side effects for each expired session without
	// ever taking the server lock and a room lock at the same time.
	for _, session := range expired {
		s.mu.RLock()
		room, exists := s.rooms[session.RoomCode]
		s.mu.RUnlock()
		if !exists || room == nil {
			continue
		}

		room.mu.Lock()

		delete(room.DisconnectedUsers, session.UserID)
		expiredWasHost := session.IsHost || room.State.HostID == session.UserID

		// Remove from room state users if still there
		newUsers := make([]UserInfo, 0, len(room.State.Users))
		for _, u := range room.State.Users {
			if u.UserID != session.UserID {
				newUsers = append(newUsers, u)
			}
		}
		room.State.Users = newUsers

		var hostChanged *HostChangedPayload
		if expiredWasHost {
			var newHost *Client
			for _, client := range room.Clients {
				if client != nil {
					newHost = client
					break
				}
			}
			room.Host = newHost
			room.HostDisconnectedAt = nil
			if newHost != nil {
				newHostID := newHost.clientID()
				room.State.HostID = newHostID
				for i := range room.State.Users {
					room.State.Users[i].IsHost = room.State.Users[i].UserID == newHostID
				}
				hostChanged = &HostChangedPayload{
					NewHostID:   newHostID,
					NewHostName: newHost.userName(),
				}
			} else {
				room.State.HostID = ""
			}
		}

		// Capture information needed after releasing the room lock
		shouldDeleteRoom := len(room.Clients) == 0 && len(room.DisconnectedUsers) == 0 && now.After(minRetentionTime)
		roomCode := room.Code
		remainingClients := make([]*Client, 0, len(room.Clients))
		for _, client := range room.Clients {
			if client != nil {
				remainingClients = append(remainingClients, client)
			}
		}

		room.mu.Unlock()

		// If the room is now empty and past the retention window, delete it.
		if shouldDeleteRoom {
			s.mu.Lock()
			// Re-check to avoid races where the room might have been recreated.
			if currentRoom, exists := s.rooms[roomCode]; exists && currentRoom == room {
				delete(s.rooms, roomCode)
				s.logger.Info("Deleted empty room",
					zap.String("room_code", roomCode))
			}
			s.mu.Unlock()
			continue
		}

		// Notify remaining users that the expired session permanently left.
		for _, client := range remainingClients {
			client.sendMessage(s.logger, MsgTypeUserLeft, UserLeftPayload{
				UserID:   session.UserID,
				Username: session.Username,
			})
			if hostChanged != nil {
				client.sendMessage(s.logger, MsgTypeHostChanged, *hostChanged)
			}
		}
	}
}

func (s *Server) cleanupEmptyRooms() {
	// Wait 5 minutes before first cleanup to avoid deleting rooms during startup
	time.Sleep(EmptyRoomCleanupTimeout)

	ticker := time.NewTicker(EmptyRoomCleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		minRetentionTime := s.startTime.Add(MinRoomRetentionAfterRestart)

		s.mu.Lock()
		for roomCode, room := range s.rooms {
			if room == nil {
				continue
			}

			room.mu.RLock()
			isEmpty := len(room.Clients) == 0 && len(room.DisconnectedUsers) == 0
			emptySince := room.EmptySince
			room.mu.RUnlock()

			if !isEmpty {
				continue
			}

			// If room just became empty, mark it
			if emptySince == nil {
				room.mu.Lock()
				nowPtr := now
				room.EmptySince = &nowPtr
				room.mu.Unlock()
				s.logger.Info("Room became empty, scheduling cleanup",
					zap.String("room_code", roomCode))
				continue
			}

			// Check if room has been empty long enough and past retention window
			if now.Sub(*emptySince) > EmptyRoomCleanupTimeout && now.After(minRetentionTime) {
				delete(s.rooms, roomCode)
				s.logger.Info("Deleted empty room after inactivity",
					zap.String("room_code", roomCode),
					zap.Duration("empty_for", now.Sub(*emptySince)))
			}
		}
		s.mu.Unlock()
	}
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

func (c *Client) writePump(logger *zap.Logger) {
	// Reduce ping frequency for efficiency (60s is sufficient for idle detection)
	ticker := time.NewTicker(60 * time.Second)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			if err := c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
				logger.Debug("Failed to set write deadline", zap.String("client_id", c.clientID()), zap.Error(err))
				return
			}
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.Conn.WriteMessage(websocket.BinaryMessage, message); err != nil {
				logger.Debug("Write error for client", zap.String("client_id", c.clientID()), zap.Error(err))
				return
			}

		case <-ticker.C:
			if err := c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
				logger.Debug("Failed to set write deadline", zap.String("client_id", c.clientID()), zap.Error(err))
				return
			}
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) readPump(s *Server) {
	defer func() {
		s.removeClient(c)
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(MaxReadMessageSize)
	if err := c.Conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
		s.logger.Debug("Failed to set read deadline", zap.String("client_id", c.clientID()), zap.Error(err))
	}
	c.Conn.SetPongHandler(func(string) error {
		if err := c.Conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
			s.logger.Debug("Failed to set read deadline in pong handler", zap.String("client_id", c.clientID()), zap.Error(err))
		}
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				s.logger.Debug("Read error for client", zap.String("client_id", c.clientID()), zap.Error(err))
			}
			break
		}

		if !c.allowMessage(time.Now()) {
			c.sendError(s.logger, "rate_limited", "Too many messages")
			continue
		}

		if err := c.Conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
			s.logger.Debug("Failed to refresh read deadline", zap.String("client_id", c.clientID()), zap.Error(err))
			break
		}
		s.handleMessage(c, message)
	}
}

func (s *Server) removeClient(c *Client) {
	s.mu.Lock()
	delete(s.clients, c)
	s.mu.Unlock()

	if c.currentRoom() != nil {
		s.handleClientDisconnect(c)
	} else {
		s.removePendingJoin(c)
	}

	c.closeSend()

	s.logger.Info("Client disconnected", zap.String("client_id", c.clientID()))
}

func (s *Server) removePendingJoin(c *Client) {
	clientID := c.clientID()
	if clientID == "" {
		return
	}

	s.mu.RLock()
	rooms := make([]*Room, 0, len(s.rooms))
	for _, room := range s.rooms {
		rooms = append(rooms, room)
	}
	s.mu.RUnlock()

	for _, room := range rooms {
		room.mu.Lock()
		if pending, exists := room.PendingJoins[clientID]; exists && pending == c {
			delete(room.PendingJoins, clientID)
		}
		room.mu.Unlock()
	}
}

// handleClientDisconnect handles a client disconnecting - creates a session for reconnection
func (s *Server) handleClientDisconnect(c *Client) {
	room := c.currentRoom()
	if room == nil {
		return
	}

	clientID := c.clientID()
	username := c.userName()
	sessionToken := c.session()
	if sessionToken == "" {
		sessionToken = s.generateSessionToken()
		c.setSessionToken(sessionToken)
	}

	room.mu.Lock()

	wasHost := room.Host == c

	// Create session for reconnection
	session := &Session{
		UserID:       clientID,
		Username:     username,
		RoomCode:     room.Code,
		IsHost:       wasHost,
		DisconnectAt: time.Now(),
	}

	// Remove from active clients but add to disconnected users
	delete(room.Clients, clientID)
	if room.BufferingUsers != nil {
		delete(room.BufferingUsers, clientID)
	}

	if room.DisconnectedUsers == nil {
		room.DisconnectedUsers = make(map[string]*Session)
	}
	room.DisconnectedUsers[clientID] = session

	// Mark user as disconnected in room state
	for i := range room.State.Users {
		if room.State.Users[i].UserID == clientID {
			room.State.Users[i].IsConnected = false
			break
		}
	}

	// Track if host disconnected
	if wasHost {
		now := time.Now()
		room.HostDisconnectedAt = &now
	}

	c.setRoom(nil)

	// Collect clients to notify before unlocking
	clientsToNotify := make([]*Client, 0, len(room.Clients))
	for _, client := range room.Clients {
		if client != nil {
			clientsToNotify = append(clientsToNotify, client)
		}
	}

	room.EmptySince = nil
	room.mu.Unlock()

	// Store the session without holding the room lock to keep lock ordering consistent.
	s.mu.Lock()
	s.sessions[sessionToken] = session
	s.mu.Unlock()

	// Notify other users about the temporary disconnect
	for _, client := range clientsToNotify {
		client.sendMessage(s.logger, MsgTypeUserDisconnected, UserDisconnectedPayload{
			UserID:   clientID,
			Username: username,
		})
	}

	s.logger.Info("User temporarily disconnected",
		zap.String("username", username),
		zap.String("user_id", clientID),
		zap.String("room_code", room.Code),
		zap.Bool("was_host", wasHost))
}

// handleReconnect handles a client trying to reconnect to their room
func (s *Server) handleReconnect(c *Client, payload []byte) {
	var p ReconnectPayload
	if err := decodePayload(payload, MsgTypeReconnect, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid reconnect payload")
		return
	}

	if p.SessionToken == "" {
		c.sendError(s.logger, "missing_session_token", "Session token is required")
		return
	}
	if c.currentRoom() != nil {
		c.sendError(s.logger, "already_in_room", "Leave the current room before reconnecting")
		return
	}

	s.mu.RLock()
	session, exists := s.sessions[p.SessionToken]
	s.mu.RUnlock()

	if !exists {
		c.sendError(s.logger, "session_not_found", "Session not found or expired")
		return
	}

	// Check if session is expired
	if time.Since(session.DisconnectAt) > ReconnectGracePeriod {
		s.mu.Lock()
		delete(s.sessions, p.SessionToken)
		s.mu.Unlock()
		c.sendError(s.logger, "session_expired", "Session has expired")
		return
	}

	s.mu.RLock()
	room, roomExists := s.rooms[session.RoomCode]
	s.mu.RUnlock()

	if !roomExists {
		s.mu.Lock()
		delete(s.sessions, p.SessionToken)
		s.mu.Unlock()
		c.sendError(s.logger, "room_not_found", "Room no longer exists")
		return
	}

	room.mu.Lock()

	// Restore the client
	c.setClientID(session.UserID)
	c.setUsername(session.Username)
	c.setSessionToken(p.SessionToken)
	c.setRoom(room)

	// Add back to room clients
	room.Clients[session.UserID] = c
	delete(room.DisconnectedUsers, session.UserID)
	room.EmptySince = nil

	// Mark user as connected in room state
	for i := range room.State.Users {
		if room.State.Users[i].UserID == session.UserID {
			room.State.Users[i].IsConnected = true
			break
		}
	}

	// Restore host status if they were the host
	if session.IsHost {
		room.Host = c
		room.HostDisconnectedAt = nil

		// Update IsHost flag in users list
		for i := range room.State.Users {
			room.State.Users[i].IsHost = room.State.Users[i].UserID == session.UserID
		}
	}

	// Calculate live position for reconnect state
	nowMs := time.Now().UnixMilli()
	liveState := cloneRoomState(room.State)
	liveState.Position = livePlaybackPosition(room.State, nowMs)
	liveState.LastUpdate = nowMs

	isHost := room.Host == c
	pendingJoinRequests := make([]JoinRequestPayload, 0, len(room.PendingJoins))
	pendingSuggestions := make([]SuggestionReceivedPayload, 0, len(room.PendingSuggestions))
	if isHost {
		for _, pendingClient := range room.PendingJoins {
			if pendingClient == nil || pendingClient.isClosed() {
				continue
			}
			pendingJoinRequests = append(pendingJoinRequests, JoinRequestPayload{
				UserID:   pendingClient.clientID(),
				Username: pendingClient.userName(),
			})
		}
		for _, suggestion := range room.PendingSuggestions {
			if suggestion == nil {
				continue
			}
			pendingSuggestions = append(pendingSuggestions, SuggestionReceivedPayload{
				SuggestionID: suggestion.ID,
				FromUserID:   suggestion.FromUserID,
				FromUsername: suggestion.FromUsername,
				TrackInfo:    cloneTrackInfo(suggestion.Track),
			})
		}
	}

	clientsToNotify := make([]*Client, 0, len(room.Clients))
	for _, client := range room.Clients {
		if client != nil && client != c {
			clientsToNotify = append(clientsToNotify, client)
		}
	}

	room.mu.Unlock()

	// Remove session since reconnection succeeded
	s.mu.Lock()
	delete(s.sessions, p.SessionToken)
	s.mu.Unlock()

	// Send reconnected message to the client with LIVE state
	c.sendMessage(s.logger, MsgTypeReconnected, ReconnectedPayload{
		RoomCode: room.Code,
		UserID:   c.clientID(),
		State:    liveState,
		IsHost:   isHost,
	})

	if isHost {
		for _, joinRequest := range pendingJoinRequests {
			c.sendMessage(s.logger, MsgTypeJoinRequest, joinRequest)
		}
		for _, suggestion := range pendingSuggestions {
			c.sendMessage(s.logger, MsgTypeSuggestionReceived, suggestion)
		}

		if len(pendingJoinRequests) > 0 {
			s.logger.Info("Replayed pending join requests to reconnected host",
				zap.String("host_id", c.clientID()),
				zap.String("room_code", room.Code),
				zap.Int("pending_count", len(pendingJoinRequests)))
		}
	}

	// Notify other users
	for _, client := range clientsToNotify {
		client.sendMessage(s.logger, MsgTypeUserReconnected, UserReconnectedPayload{
			UserID:   c.clientID(),
			Username: c.userName(),
		})
	}

	s.logger.Info("User reconnected",
		zap.String("username", c.userName()),
		zap.String("user_id", c.clientID()),
		zap.String("room_code", room.Code),
		zap.Bool("is_host", isHost))
}

// sanitizeString removes potentially dangerous characters and limits length
func sanitizeString(s string, maxLen int) string {
	// Remove null bytes and other control characters
	s = strings.Map(func(r rune) rune {
		if r == 0 || (r < 32 && r != '\t' && r != '\n' && r != '\r') {
			return -1
		}
		return r
	}, s)

	// Trim whitespace
	s = strings.TrimSpace(s)

	// Validate UTF-8
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "")
	}

	// Limit length
	if len(s) > maxLen {
		// Ensure we don't cut in the middle of a multibyte character
		for i := maxLen; i > 0 && i > maxLen-4; i-- {
			if utf8.ValidString(s[:i]) {
				return s[:i]
			}
		}
		return s[:maxLen]
	}

	return s
}

func sanitizeTrackInfo(track *TrackInfo) bool {
	if track == nil {
		return false
	}

	track.ID = sanitizeString(track.ID, 200)
	track.Title = sanitizeString(track.Title, MaxTrackTitleLength)
	track.Artist = sanitizeString(track.Artist, MaxTrackArtistLength)
	track.Album = sanitizeString(track.Album, MaxTrackArtistLength)
	track.Thumbnail = sanitizeString(track.Thumbnail, MaxTrackURLLength)
	track.SuggestedBy = sanitizeString(track.SuggestedBy, MaxUsernameLength)

	if track.ID == "" || track.Title == "" {
		return false
	}
	if track.Duration <= 0 {
		track.Duration = 180000
	} else if track.Duration > MaxTrackDuration {
		track.Duration = MaxTrackDuration
	}

	return true
}

func cloneTrackInfo(track *TrackInfo) *TrackInfo {
	if track == nil {
		return nil
	}
	copyTrack := *track
	return &copyTrack
}

func cloneRoomState(state *RoomState) *RoomState {
	if state == nil {
		return nil
	}
	copyState := *state
	copyState.CurrentTrack = cloneTrackInfo(state.CurrentTrack)
	if state.Users != nil {
		copyState.Users = append([]UserInfo(nil), state.Users...)
	}
	if state.Queue != nil {
		copyState.Queue = append([]TrackInfo(nil), state.Queue...)
	}
	return &copyState
}

func (s *Server) deleteRoomIfEmpty(room *Room) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	currentRoom, exists := s.rooms[room.Code]
	if !exists || currentRoom != room {
		return false
	}

	room.mu.Lock()
	defer room.mu.Unlock()
	if len(room.Clients) != 0 || len(room.DisconnectedUsers) != 0 {
		return false
	}

	delete(s.rooms, room.Code)
	return true
}

func livePlaybackPosition(state *RoomState, nowMs int64) int64 {
	if state == nil {
		return 0
	}

	position := state.Position
	if position < 0 {
		position = 0
	}

	if state.IsPlaying && state.LastUpdate > 0 {
		elapsed := nowMs - state.LastUpdate
		if elapsed > 0 {
			position += elapsed
		}
	}

	if state.CurrentTrack != nil && state.CurrentTrack.Duration > 0 && position > state.CurrentTrack.Duration {
		return state.CurrentTrack.Duration
	}

	return position
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

func (s *Server) handleSuggestTrack(c *Client, payload []byte) {
	var p SuggestTrackPayload
	if err := decodePayload(payload, MsgTypeSuggestTrack, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid suggest track payload")
		return
	}

	room := c.currentRoom()
	if room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	if p.TrackInfo == nil {
		c.sendError(s.logger, "missing_track_info", "Track info is required")
		return
	}

	if !sanitizeTrackInfo(p.TrackInfo) {
		c.sendError(s.logger, "invalid_track_info", "Track must have ID and title")
		return
	}

	clientID := c.clientID()
	username := c.userName()
	room.mu.Lock()

	if room.Clients[clientID] != c {
		room.mu.Unlock()
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	// Host cannot suggest to themselves; ignore silently
	if room.State.HostID == clientID {
		room.mu.Unlock()
		return
	}

	if room.PendingSuggestions == nil {
		room.PendingSuggestions = make(map[string]*Suggestion)
	}
	if len(room.PendingSuggestions) >= MaxPendingSuggestions {
		room.mu.Unlock()
		c.sendError(s.logger, "suggestions_full", "Too many pending suggestions")
		return
	}

	// Generate suggestion ID
	s.rngMu.Lock()
	sugID := fmt.Sprintf("sug_%d_%d", time.Now().UnixNano(), s.rng.Intn(10000))
	s.rngMu.Unlock()
	room.PendingSuggestions[sugID] = &Suggestion{
		ID:           sugID,
		FromUserID:   clientID,
		FromUsername: username,
		Track:        p.TrackInfo,
	}

	host := room.Host
	hostConnected := host != nil && room.HostDisconnectedAt == nil
	notification := SuggestionReceivedPayload{
		SuggestionID: sugID,
		FromUserID:   clientID,
		FromUsername: username,
		TrackInfo:    p.TrackInfo,
	}
	room.mu.Unlock()

	if hostConnected {
		host.sendMessage(s.logger, MsgTypeSuggestionReceived, notification)
	}

	s.logger.Info("Suggestion received",
		zap.String("room_code", room.Code),
		zap.String("from_user", username),
		zap.String("track_id", p.TrackInfo.ID))
}

func (s *Server) handleApproveSuggestion(c *Client, payload []byte) {
	var p ApproveSuggestionPayload
	if err := decodePayload(payload, MsgTypeApproveSuggestion, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid approve suggestion payload")
		return
	}
	if p.SuggestionID == "" {
		c.sendError(s.logger, "missing_suggestion_id", "Suggestion ID is required")
		return
	}
	room := c.currentRoom()
	if room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}
	room.mu.Lock()
	defer room.mu.Unlock()
	if room.Host == nil || room.Host != c || room.HostDisconnectedAt != nil {
		c.sendError(s.logger, "not_host", "Only the host can approve suggestions")
		return
	}
	suggestion, exists := room.PendingSuggestions[p.SuggestionID]
	if !exists || suggestion == nil {
		c.sendError(s.logger, "suggestion_not_found", "Suggestion not found")
		return
	}

	// Update room state queue: insert next (front of upcoming queue)
	if suggestion.Track != nil {
		if len(room.State.Queue) >= MaxQueueSize {
			c.sendError(s.logger, "queue_full", "Queue is full")
			return
		}
		suggestion.Track.SuggestedBy = suggestion.FromUsername
		room.State.Queue = append([]TrackInfo{*suggestion.Track}, room.State.Queue...)
	}

	// Remove from pending after all validation succeeds.
	delete(room.PendingSuggestions, p.SuggestionID)

	// Broadcast queue add (insert next) so clients apply immediately
	qa := PlaybackActionPayload{
		Action:     ActionQueueAdd,
		TrackInfo:  suggestion.Track,
		InsertNext: true,
	}
	for _, client := range room.Clients {
		if client != nil {
			client.sendMessage(s.logger, MsgTypeSyncPlayback, qa)
		}
	}

	// Notify suggester of approval
	if target, ok := room.Clients[suggestion.FromUserID]; ok && target != nil {
		target.sendMessage(s.logger, MsgTypeSuggestionApproved, SuggestionApprovedPayload{
			SuggestionID: p.SuggestionID,
			TrackInfo:    suggestion.Track,
		})
	}

	trackID := ""
	if suggestion.Track != nil {
		trackID = suggestion.Track.ID
	}

	s.logger.Info("Suggestion approved",
		zap.String("room_code", room.Code),
		zap.String("track_id", trackID))
}

func (s *Server) handleRejectSuggestion(c *Client, payload []byte) {
	var p RejectSuggestionPayload
	if err := decodePayload(payload, MsgTypeRejectSuggestion, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid reject suggestion payload")
		return
	}
	if p.SuggestionID == "" {
		c.sendError(s.logger, "missing_suggestion_id", "Suggestion ID is required")
		return
	}
	room := c.currentRoom()
	if room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}
	room.mu.Lock()
	defer room.mu.Unlock()
	if room.Host == nil || room.Host != c || room.HostDisconnectedAt != nil {
		c.sendError(s.logger, "not_host", "Only the host can reject suggestions")
		return
	}
	suggestion, exists := room.PendingSuggestions[p.SuggestionID]
	if !exists || suggestion == nil {
		c.sendError(s.logger, "suggestion_not_found", "Suggestion not found")
		return
	}
	delete(room.PendingSuggestions, p.SuggestionID)

	// Notify suggester of rejection
	reason := p.Reason
	if len(reason) > 200 {
		reason = reason[:200]
	}
	if target, ok := room.Clients[suggestion.FromUserID]; ok && target != nil {
		target.sendMessage(s.logger, MsgTypeSuggestionRejected, SuggestionRejectedPayload{
			SuggestionID: p.SuggestionID,
			Reason:       reason,
		})
	}

	trackID := ""
	if suggestion.Track != nil {
		trackID = suggestion.Track.ID
	}

	s.logger.Info("Suggestion rejected",
		zap.String("room_code", room.Code),
		zap.String("track_id", trackID))
}

func (s *Server) handleCreateRoom(c *Client, payload []byte) {
	var p CreateRoomPayload
	if err := decodePayload(payload, MsgTypeCreateRoom, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid create room payload")
		return
	}

	if p.Username == "" {
		c.sendError(s.logger, "missing_username", "Username is required")
		return
	}
	if c.currentRoom() != nil {
		c.sendError(s.logger, "already_in_room", "Leave the current room before creating another")
		return
	}

	// Sanitize and validate username
	p.Username = sanitizeString(p.Username, MaxUsernameLength)
	if p.Username == "" {
		c.sendError(s.logger, "invalid_username", "Username is invalid")
		return
	}

	// Generate unique room code with retry limit
	var (
		code   string
		exists bool
	)
	maxRetries := 100
	for i := 0; i < maxRetries; i++ {
		code = s.generateRoomCode()
		s.mu.RLock()
		_, exists = s.rooms[code]
		s.mu.RUnlock()
		if !exists {
			break
		}
	}

	if code == "" || exists {
		s.logger.Error("Failed to generate unique room code after retries")
		c.sendError(s.logger, "server_error", "Failed to create room")
		return
	}

	s.mu.RLock()
	roomCount := len(s.rooms)
	s.mu.RUnlock()
	if roomCount >= MaxRooms {
		c.sendError(s.logger, "room_limit_reached", "Server is at room capacity")
		return
	}

	c.setUsername(p.Username)
	c.setSessionToken(s.generateSessionToken())
	clientID := c.clientID()
	username := c.userName()
	sessionToken := c.session()

	room := &Room{
		Code:              code,
		Host:              c,
		Clients:           make(map[string]*Client),
		PendingJoins:      make(map[string]*Client),
		DisconnectedUsers: make(map[string]*Session),
		BufferingUsers:    make(map[string]bool),
		State: &RoomState{
			RoomCode:   code,
			HostID:     clientID,
			Users:      []UserInfo{{UserID: clientID, Username: username, IsHost: true, IsConnected: true}},
			IsPlaying:  false,
			Position:   0,
			LastUpdate: time.Now().UnixMilli(),
			Volume:     1.0,
			Queue:      []TrackInfo{},
		},
	}

	room.Clients[clientID] = c

	s.mu.Lock()
	if len(s.rooms) >= MaxRooms {
		s.mu.Unlock()
		c.sendError(s.logger, "room_limit_reached", "Server is at room capacity")
		return
	}
	if _, exists := s.rooms[code]; exists {
		s.mu.Unlock()
		c.sendError(s.logger, "server_error", "Failed to create room")
		return
	}
	s.rooms[code] = room
	s.mu.Unlock()
	c.setRoom(room)

	s.logger.Info("About to send RoomCreated response",
		zap.String("room_code", code),
		zap.String("client_id", clientID),
		zap.Int("session_token_len", len(sessionToken)))

	c.sendMessage(s.logger, MsgTypeRoomCreated, RoomCreatedPayload{
		RoomCode:     code,
		UserID:       clientID,
		SessionToken: sessionToken,
	})

	s.logger.Info("Room created",
		zap.String("room_code", code),
		zap.String("host_name", username),
		zap.String("host_id", clientID))
}

func (s *Server) handleJoinRoom(c *Client, payload []byte) {
	var p JoinRoomPayload
	if err := decodePayload(payload, MsgTypeJoinRoom, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid join room payload")
		return
	}

	if p.Username == "" {
		c.sendError(s.logger, "missing_username", "Username is required")
		return
	}
	if c.currentRoom() != nil {
		c.sendError(s.logger, "already_in_room", "Leave the current room before joining another")
		return
	}

	// Sanitize and validate username
	p.Username = sanitizeString(p.Username, MaxUsernameLength)
	if p.Username == "" {
		c.sendError(s.logger, "invalid_username", "Username is invalid")
		return
	}

	if p.RoomCode == "" {
		c.sendError(s.logger, "missing_room_code", "Room code is required")
		return
	}

	// Sanitize and validate room code
	p.RoomCode = sanitizeString(strings.ToUpper(p.RoomCode), MaxRoomCodeLength)
	if p.RoomCode == "" {
		c.sendError(s.logger, "invalid_room_code", "Room code is invalid")
		return
	}

	s.mu.RLock()
	room, exists := s.rooms[p.RoomCode]
	s.mu.RUnlock()

	if !exists {
		c.sendError(s.logger, "room_not_found", "Room not found")
		return
	}

	c.setUsername(p.Username)
	clientID := c.clientID()
	username := c.userName()

	room.mu.Lock()
	// Check if user is already in the room or pending
	if _, exists := room.Clients[clientID]; exists {
		room.mu.Unlock()
		c.sendError(s.logger, "already_in_room", "You are already in this room")
		return
	}

	if _, exists := room.PendingJoins[clientID]; exists {
		room.mu.Unlock()
		c.sendError(s.logger, "already_pending", "Your join request is already pending")
		return
	}
	if len(room.Clients) >= MaxClientsPerRoom {
		room.mu.Unlock()
		c.sendError(s.logger, "room_full", "Room is full")
		return
	}
	if len(room.PendingJoins) >= MaxPendingJoins {
		room.mu.Unlock()
		c.sendError(s.logger, "too_many_pending", "Too many pending join requests")
		return
	}

	// Validate room isn't in an invalid state
	if room.State.HostID == "" {
		room.mu.Unlock()
		c.sendError(s.logger, "room_invalid", "Room is no longer valid")
		return
	}

	// Add to pending joins
	room.PendingJoins[clientID] = c
	host := room.Host
	hostConnected := host != nil && room.HostDisconnectedAt == nil
	room.mu.Unlock()

	// Notify host of join request if host is currently connected.
	if hostConnected {
		host.sendMessage(s.logger, MsgTypeJoinRequest, JoinRequestPayload{
			UserID:   clientID,
			Username: username,
		})
	} else {
		s.logger.Info("Host unavailable, join request queued",
			zap.String("username", username),
			zap.String("user_id", clientID),
			zap.String("room_code", p.RoomCode))
	}

	s.logger.Info("Join request received",
		zap.String("username", username),
		zap.String("user_id", clientID),
		zap.String("room_code", p.RoomCode))
}

func (s *Server) handleApproveJoin(c *Client, payload []byte) {
	var p ApproveJoinPayload
	if err := decodePayload(payload, MsgTypeApproveJoin, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid approve join payload")
		return
	}

	if p.UserID == "" {
		c.sendError(s.logger, "missing_user_id", "User ID is required")
		return
	}

	room := c.currentRoom()
	if room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room.mu.Lock()
	defer room.mu.Unlock()

	if room.Host == nil || room.Host != c || room.HostDisconnectedAt != nil {
		c.sendError(s.logger, "not_host", "Only the host can approve join requests")
		return
	}

	joiningClient, exists := room.PendingJoins[p.UserID]
	if !exists {
		c.sendError(s.logger, "join_request_not_found", "Join request not found")
		return
	}

	// Verify joining client is still valid
	if joiningClient == nil || joiningClient.isClosed() {
		delete(room.PendingJoins, p.UserID)
		c.sendError(s.logger, "user_disconnected", "User has disconnected")
		return
	}
	if len(room.Clients) >= MaxClientsPerRoom {
		c.sendError(s.logger, "room_full", "Room is full")
		return
	}

	joiningID := joiningClient.clientID()
	joiningUsername := joiningClient.userName()
	joiningToken := s.generateSessionToken()

	// Remove from pending and add to room
	delete(room.PendingJoins, p.UserID)
	room.Clients[joiningID] = joiningClient
	joiningClient.setRoom(room)
	joiningClient.setSessionToken(joiningToken)

	// Clear empty status since room is no longer empty
	room.EmptySince = nil

	// Update room state
	room.State.Users = append(room.State.Users, UserInfo{
		UserID:      joiningID,
		Username:    joiningUsername,
		IsHost:      false,
		IsConnected: true,
	})

	// Send approval to the joining user
	joiningClient.sendMessage(s.logger, MsgTypeJoinApproved, JoinApprovedPayload{
		RoomCode:     room.Code,
		UserID:       joiningID,
		SessionToken: joiningToken,
		State:        cloneRoomState(room.State),
	})

	// If there is a current track, immediately send buffer-complete + seek (+ play if host is playing)
	if room.State.CurrentTrack != nil {
		syncPosition := livePlaybackPosition(room.State, time.Now().UnixMilli())
		trackID := room.State.CurrentTrack.ID
		joiningClient.sendMessage(s.logger, MsgTypeBufferComplete, BufferCompletePayload{TrackID: trackID})
		joiningClient.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
			Action:   ActionSeek,
			TrackID:  trackID,
			Position: syncPosition,
		})
		if room.State.IsPlaying {
			joiningClient.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
				Action:   ActionPlay,
				TrackID:  trackID,
				Position: syncPosition,
			})
		}
	}

	// Notify all other users
	for _, client := range room.Clients {
		if client != nil && client.clientID() != joiningID {
			client.sendMessage(s.logger, MsgTypeUserJoined, UserJoinedPayload{
				UserID:   joiningID,
				Username: joiningUsername,
			})
		}
	}

	s.logger.Info("User approved to join room",
		zap.String("username", joiningUsername),
		zap.String("user_id", joiningID),
		zap.String("room_code", room.Code))
}

func (s *Server) handleRejectJoin(c *Client, payload []byte) {
	var p RejectJoinPayload
	if err := decodePayload(payload, MsgTypeRejectJoin, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid reject join payload")
		return
	}

	if p.UserID == "" {
		c.sendError(s.logger, "missing_user_id", "User ID is required")
		return
	}

	room := c.currentRoom()
	if room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room.mu.Lock()
	defer room.mu.Unlock()

	if room.Host == nil || room.Host != c || room.HostDisconnectedAt != nil {
		c.sendError(s.logger, "not_host", "Only the host can reject join requests")
		return
	}

	joiningClient, exists := room.PendingJoins[p.UserID]
	if !exists {
		c.sendError(s.logger, "join_request_not_found", "Join request not found")
		return
	}

	delete(room.PendingJoins, p.UserID)

	reason := p.Reason
	if reason == "" {
		reason = "Join request rejected by host"
	}

	if len(reason) > 200 {
		reason = reason[:200]
	}

	joiningClient.sendMessage(s.logger, MsgTypeJoinRejected, JoinRejectedPayload{
		Reason: reason,
	})

	s.logger.Info("User rejected from room",
		zap.String("username", joiningClient.userName()),
		zap.String("user_id", joiningClient.clientID()),
		zap.String("room_code", room.Code))
}

func (s *Server) handlePlaybackAction(c *Client, payload []byte) {
	var p PlaybackActionPayload
	if err := decodePayload(payload, MsgTypePlaybackAction, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid playback action payload")
		return
	}

	if p.Action == "" {
		c.sendError(s.logger, "missing_action", "Action is required")
		return
	}

	room := c.currentRoom()
	if room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room.mu.Lock()
	defer room.mu.Unlock()

	if room.Host == nil || room.Host != c || room.HostDisconnectedAt != nil {
		c.sendError(s.logger, "not_host", "Only the host can control playback")
		return
	}

	switch p.Action {
	case ActionPlay:
		// Block play if no track is set
		if room.State.CurrentTrack == nil {
			s.logger.Debug("Play blocked - no current track", zap.String("room_code", room.Code))
			c.sendError(s.logger, "no_track", "Cannot play without a track")
			return
		}
		if p.Position < 0 {
			c.sendError(s.logger, "invalid_position", "Position cannot be negative")
			return
		}
		nowMs := time.Now().UnixMilli()
		if room.State.CurrentTrack != nil && room.State.CurrentTrack.Duration > 0 && p.Position > room.State.CurrentTrack.Duration {
			p.Position = room.State.CurrentTrack.Duration
		}
		room.State.IsPlaying = true
		room.State.Position = p.Position
		room.State.LastUpdate = nowMs
		p.ServerTime = nowMs
		if p.TrackID == "" && room.State.CurrentTrack != nil {
			p.TrackID = room.State.CurrentTrack.ID
		}

	case ActionPause:
		// Pause is always allowed
		if p.Position < 0 {
			c.sendError(s.logger, "invalid_position", "Position cannot be negative")
			return
		}
		nowMs := time.Now().UnixMilli()
		if room.State.CurrentTrack != nil && room.State.CurrentTrack.Duration > 0 && p.Position > room.State.CurrentTrack.Duration {
			p.Position = room.State.CurrentTrack.Duration
		}
		room.State.IsPlaying = false
		room.State.Position = p.Position
		room.State.LastUpdate = nowMs
		if p.TrackID == "" && room.State.CurrentTrack != nil {
			p.TrackID = room.State.CurrentTrack.ID
		}

	case ActionSeek:
		if p.Position < 0 {
			c.sendError(s.logger, "invalid_position", "Position cannot be negative")
			return
		}
		nowMs := time.Now().UnixMilli()
		if room.State.CurrentTrack != nil && room.State.CurrentTrack.Duration > 0 && p.Position > room.State.CurrentTrack.Duration {
			p.Position = room.State.CurrentTrack.Duration
		}
		room.State.Position = p.Position
		room.State.LastUpdate = nowMs
		if p.TrackID == "" && room.State.CurrentTrack != nil {
			p.TrackID = room.State.CurrentTrack.ID
		}

	case ActionChangeTrack:
		if p.TrackInfo == nil {
			c.sendError(s.logger, "missing_track_info", "Track info is required for track change")
			return
		}

		if !sanitizeTrackInfo(p.TrackInfo) {
			c.sendError(s.logger, "invalid_track_info", "Track must have ID and title")
			return
		}

		room.State.CurrentTrack = p.TrackInfo
		room.State.Position = 0
		room.State.IsPlaying = false
		room.State.LastUpdate = time.Now().UnixMilli()

		// For new tracks, always start at position 0
		room.HostStartPosition = 0
		s.logger.Debug("Track changed", zap.String("room_code", room.Code), zap.String("track_id", p.TrackInfo.ID))

		// We do not require guests to wait for everyone to buffer.
		// Immediately notify clients and sync them to position 0 so guests can proceed.
		room.BufferingUsers = nil // disable per-room buffering tracking

		// Broadcast track change and immediate sync
		for _, client := range room.Clients {
			if client != nil {
				// Send track change
				client.sendMessage(s.logger, MsgTypeSyncPlayback, p)

				// Ensure everyone is paused at position 0 during transition
				client.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
					Action:   ActionPause,
					TrackID:  p.TrackInfo.ID,
					Position: 0,
				})

				// Immediately notify buffer complete so clients that wait for it will apply seek/play
				client.sendMessage(s.logger, MsgTypeBufferComplete, BufferCompletePayload{
					TrackID: p.TrackInfo.ID,
				})

				// Seek everyone to the new start position (0)
				client.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
					Action:   ActionSeek,
					TrackID:  p.TrackInfo.ID,
					Position: 0,
				})

				// If the room was marked playing, start playback immediately
				if room.State.IsPlaying {
					client.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
						Action:   ActionPlay,
						TrackID:  p.TrackInfo.ID,
						Position: 0,
					})
				}
			}
		}
		return

	case ActionSkipNext, ActionSkipPrev:
		room.State.Position = 0
		room.State.LastUpdate = time.Now().UnixMilli()

	case ActionQueueAdd:
		if p.TrackInfo == nil {
			c.sendError(s.logger, "missing_track_info", "Track info is required for queue add")
			return
		}

		if !sanitizeTrackInfo(p.TrackInfo) {
			c.sendError(s.logger, "invalid_track_info", "Track must have ID and title")
			return
		}

		// Limit queue size to prevent memory issues
		if len(room.State.Queue) >= MaxQueueSize {
			c.sendError(s.logger, "queue_full", "Queue is full")
			return
		}

		if p.InsertNext {
			// Insert right after current track: at the front of upcoming queue
			room.State.Queue = append([]TrackInfo{*p.TrackInfo}, room.State.Queue...)
		} else {
			// Append to end of upcoming queue
			room.State.Queue = append(room.State.Queue, *p.TrackInfo)
		}

	case ActionQueueRemove:
		if p.TrackID == "" {
			c.sendError(s.logger, "missing_track_id", "Track ID is required for queue remove")
			return
		}

		// Remove track from queue by ID
		newQueue := make([]TrackInfo, 0, len(room.State.Queue))
		for _, t := range room.State.Queue {
			if t.ID != p.TrackID {
				newQueue = append(newQueue, t)
			}
		}
		room.State.Queue = newQueue

	case ActionQueueClear:
		room.State.Queue = []TrackInfo{}

	case ActionSyncQueue:
		if p.Queue == nil {
			// Allow empty queue sync (clearing) but log it
			room.State.Queue = []TrackInfo{}
		} else {
			// Limit queue size
			if len(p.Queue) > MaxQueueSize {
				p.Queue = p.Queue[:MaxQueueSize]
			}

			// Validate and sanitize each track in the queue
			sanitizedQueue := make([]TrackInfo, 0, len(p.Queue))
			for _, track := range p.Queue {
				if !sanitizeTrackInfo(&track) {
					continue
				}

				sanitizedQueue = append(sanitizedQueue, track)
			}
			room.State.Queue = sanitizedQueue
			// Pass sanitized queue back to payload for broadcast
			p.Queue = sanitizedQueue
		}

	case ActionSetVolume:
		if p.Volume < 0 || p.Volume > 1 {
			c.sendError(s.logger, "invalid_volume", "Volume must be between 0 and 1")
			return
		}
		room.State.Volume = p.Volume

	default:
		c.sendError(s.logger, "unknown_action", fmt.Sprintf("Unknown action: %s", p.Action))
		return
	}

	// Broadcast to all clients
	for _, client := range room.Clients {
		if client != nil {
			client.sendMessage(s.logger, MsgTypeSyncPlayback, p)
		}
	}

	s.logger.Debug("Playback action processed",
		zap.String("action", p.Action),
		zap.String("room_code", room.Code),
		zap.String("host_name", c.userName()))
}

func (s *Server) handleBufferReady(c *Client, payload []byte) {
	var p BufferReadyPayload
	if err := decodePayload(payload, MsgTypeBufferReady, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid buffer ready payload")
		return
	}

	if p.TrackID == "" {
		c.sendError(s.logger, "missing_track_id", "Track ID is required")
		return
	}

	room := c.currentRoom()
	if room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room.mu.Lock()
	defer room.mu.Unlock()
	clientID := c.clientID()
	username := c.userName()

	s.logger.Debug("Buffer ready received",
		zap.String("username", username),
		zap.String("user_id", clientID),
		zap.String("track_id", p.TrackID))

	// Mark user as ready
	delete(room.BufferingUsers, clientID)

	// If buffering is disabled for this room, respond per-client so late buffer_ready still receives SEEK/PLAY
	if room.BufferingUsers == nil {
		s.logger.Debug("Buffering disabled for room - per-client ACK", zap.String("room_code", room.Code), zap.String("user_id", clientID))
		syncPosition := livePlaybackPosition(room.State, time.Now().UnixMilli())
		syncTrackID := p.TrackID
		if room.State.CurrentTrack != nil && room.State.CurrentTrack.ID != "" {
			syncTrackID = room.State.CurrentTrack.ID
		}
		// Send buffer-complete and sync to this specific client so they will apply seek/play
		c.sendMessage(s.logger, MsgTypeBufferComplete, BufferCompletePayload{TrackID: syncTrackID})
		c.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
			Action:   ActionSeek,
			TrackID:  syncTrackID,
			Position: syncPosition,
		})
		if room.State.IsPlaying {
			c.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
				Action:   ActionPlay,
				TrackID:  syncTrackID,
				Position: syncPosition,
			})
		}
		return
	}

	// Check if all users are ready
	if len(room.BufferingUsers) == 0 {
		// All users ready - sync everyone to position 0 for new track
		syncPosition := int64(0)
		syncTrackID := p.TrackID
		if room.State.CurrentTrack != nil && room.State.CurrentTrack.ID != "" {
			syncTrackID = room.State.CurrentTrack.ID
		}
		room.State.Position = syncPosition
		room.State.LastUpdate = time.Now().UnixMilli()

		s.logger.Debug("All users buffered",
			zap.String("track_id", p.TrackID),
			zap.String("room_code", room.Code))

		for _, client := range room.Clients {
			if client != nil {
				// Step 1: Send buffer complete notification
				client.sendMessage(s.logger, MsgTypeBufferComplete, BufferCompletePayload{
					TrackID: syncTrackID,
				})

				// Step 2: SEEK everyone to exact position
				client.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
					Action:   ActionSeek,
					TrackID:  syncTrackID,
					Position: syncPosition,
				})

				// Step 3: Only PLAY if the host actually started playback
				if room.State.IsPlaying {
					client.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
						Action:   ActionPlay,
						TrackID:  syncTrackID,
						Position: syncPosition,
					})
				}
			}
		}
	} else {
		// Notify all users of who is still buffering
		waitingFor := make([]string, 0, len(room.BufferingUsers))
		for id := range room.BufferingUsers {
			waitingFor = append(waitingFor, id)
		}

		for _, client := range room.Clients {
			if client != nil {
				client.sendMessage(s.logger, MsgTypeBufferWait, BufferWaitPayload{
					TrackID:    p.TrackID,
					WaitingFor: waitingFor,
				})
			}
		}
	}
}

func (s *Server) handleKickUser(c *Client, payload []byte) {
	var p KickUserPayload
	if err := decodePayload(payload, MsgTypeKickUser, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid kick user payload")
		return
	}

	if p.UserID == "" {
		c.sendError(s.logger, "missing_user_id", "User ID is required")
		return
	}

	room := c.currentRoom()
	if room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	clientID := c.clientID()
	room.mu.Lock()

	if room.Host == nil || room.Host != c || room.HostDisconnectedAt != nil {
		room.mu.Unlock()
		c.sendError(s.logger, "not_host", "Only the host can kick users")
		return
	}

	if p.UserID == clientID {
		room.mu.Unlock()
		c.sendError(s.logger, "cannot_kick_self", "You cannot kick yourself")
		return
	}

	targetClient, exists := room.Clients[p.UserID]
	if !exists {
		room.mu.Unlock()
		c.sendError(s.logger, "user_not_found", "User not found in room")
		return
	}

	if targetClient == nil {
		room.mu.Unlock()
		c.sendError(s.logger, "user_not_found", "User not found in room")
		return
	}

	// Remove from room
	delete(room.Clients, p.UserID)
	delete(room.BufferingUsers, p.UserID)

	// Update room state users list
	newUsers := make([]UserInfo, 0, len(room.State.Users))
	for _, u := range room.State.Users {
		if u.UserID != p.UserID {
			newUsers = append(newUsers, u)
		}
	}
	room.State.Users = newUsers

	kickedUsername := targetClient.userName()
	targetClient.setRoom(nil)

	// Collect clients to notify before unlocking
	clientsToNotify := make([]*Client, 0, len(room.Clients))
	for _, client := range room.Clients {
		if client != nil {
			clientsToNotify = append(clientsToNotify, client)
		}
	}

	room.mu.Unlock()

	// Notify the kicked user
	reason := p.Reason
	if reason == "" {
		reason = "You have been kicked from the room"
	}

	if len(reason) > 200 {
		reason = reason[:200]
	}

	targetClient.sendMessage(s.logger, MsgTypeKicked, KickedPayload{
		Reason: reason,
	})

	// Notify other users
	for _, client := range clientsToNotify {
		client.sendMessage(s.logger, MsgTypeUserLeft, UserLeftPayload{
			UserID:   p.UserID,
			Username: kickedUsername,
		})
	}

	s.logger.Info("User kicked from room",
		zap.String("username", kickedUsername),
		zap.String("user_id", p.UserID),
		zap.String("room_code", room.Code))
}

func (s *Server) handleTransferHost(c *Client, payload []byte) {
	var p TransferHostPayload
	if err := decodePayload(payload, MsgTypeTransferHost, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid transfer host payload")
		return
	}

	if p.NewHostID == "" {
		c.sendError(s.logger, "missing_user_id", "New host user ID is required")
		return
	}

	room := c.currentRoom()
	if room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room.mu.Lock()
	defer room.mu.Unlock()

	// Only current host can transfer ownership
	if room.Host == nil || room.Host != c || room.HostDisconnectedAt != nil {
		c.sendError(s.logger, "not_host", "Only the host can transfer ownership")
		return
	}

	// Cannot transfer to self
	if p.NewHostID == c.clientID() {
		c.sendError(s.logger, "cannot_transfer_to_self", "You are already the host")
		return
	}

	// Find new host client
	newHostClient, exists := room.Clients[p.NewHostID]
	if !exists || newHostClient == nil {
		c.sendError(s.logger, "user_not_found", "Target user not found in room")
		return
	}

	// Transfer host role
	oldHostID := c.clientID()
	oldHostName := c.userName()
	newHostID := newHostClient.clientID()
	newHostName := newHostClient.userName()

	room.Host = newHostClient
	room.State.HostID = newHostID

	// Update users list in state
	for i := range room.State.Users {
		if room.State.Users[i].UserID == oldHostID {
			room.State.Users[i].IsHost = false
		}
		if room.State.Users[i].UserID == p.NewHostID {
			room.State.Users[i].IsHost = true
		}
	}

	// Notify all users about the host change
	hostChangedPayload := HostChangedPayload{
		NewHostID:   newHostID,
		NewHostName: newHostName,
	}

	for _, client := range room.Clients {
		if client != nil {
			client.sendMessage(s.logger, MsgTypeHostChanged, hostChangedPayload)
		}
	}

	s.logger.Info("Host transferred",
		zap.String("room_code", room.Code),
		zap.String("old_host", oldHostName),
		zap.String("new_host", newHostName))
}

func (s *Server) handleRequestSync(c *Client) {
	room := c.currentRoom()
	if room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room.mu.RLock()

	// Calculate live position
	nowMs := time.Now().UnixMilli()
	currentPosition := livePlaybackPosition(room.State, nowMs)
	elapsed := int64(0)
	if room.State.IsPlaying && currentPosition > room.State.Position {
		elapsed = currentPosition - room.State.Position
	}

	responsePlaying := room.State.IsPlaying

	s.logger.Debug("Sync request received",
		zap.String("username", c.userName()),
		zap.String("user_id", c.clientID()),
		zap.Bool("has_track", room.State.CurrentTrack != nil),
		zap.Bool("server_playing", room.State.IsPlaying),
		zap.Bool("response_playing", responsePlaying),
		zap.Int64("position", currentPosition),
		zap.Int64("elapsed_ms", elapsed))

	response := SyncStatePayload{
		CurrentTrack: cloneTrackInfo(room.State.CurrentTrack),
		IsPlaying:    responsePlaying,
		Position:     currentPosition,
		LastUpdate:   nowMs,
		Queue:        append([]TrackInfo(nil), room.State.Queue...),
		Volume:       room.State.Volume,
	}
	room.mu.RUnlock()

	c.sendMessage(s.logger, MsgTypeSyncState, response)
}

func (s *Server) leaveRoom(c *Client) {
	room := c.currentRoom()
	if room == nil {
		return
	}

	clientID := c.clientID()
	username := c.userName()
	sessionToken := c.session()
	room.mu.Lock()

	delete(room.Clients, clientID)
	delete(room.BufferingUsers, clientID)
	delete(room.PendingJoins, clientID)
	delete(room.DisconnectedUsers, clientID)

	wasHost := room.Host == c

	// Update room state users list
	newUsers := make([]UserInfo, 0, len(room.State.Users))
	for _, u := range room.State.Users {
		if u.UserID != clientID {
			newUsers = append(newUsers, u)
		}
	}
	room.State.Users = newUsers

	c.setRoom(nil)

	// If room is empty (no active or disconnected users), mark it as empty
	if len(room.Clients) == 0 && len(room.DisconnectedUsers) == 0 {
		now := time.Now()
		room.EmptySince = &now
		room.mu.Unlock()
		if sessionToken != "" {
			s.mu.Lock()
			delete(s.sessions, sessionToken)
			s.mu.Unlock()
		}
		s.logger.Info("Room became empty",
			zap.String("room_code", room.Code))
		return
	}

	// If host left, transfer to another user
	var newHost *Client
	if wasHost {
		for _, client := range room.Clients {
			newHost = client
			break
		}
		if newHost != nil {
			room.Host = newHost
			room.State.HostID = newHost.clientID()

			// Update IsHost flag in users list
			for i := range room.State.Users {
				room.State.Users[i].IsHost = room.State.Users[i].UserID == newHost.clientID()
			}
		}
	}

	// Collect clients and host info before unlocking
	clientsToNotify := make([]*Client, 0, len(room.Clients))
	for _, client := range room.Clients {
		if client != nil {
			clientsToNotify = append(clientsToNotify, client)
		}
	}
	notifyHostChanged := wasHost && newHost != nil
	hostID := ""
	hostName := ""
	if newHost != nil {
		hostID = newHost.clientID()
		hostName = newHost.userName()
	}

	room.mu.Unlock()
	if sessionToken != "" {
		s.mu.Lock()
		delete(s.sessions, sessionToken)
		s.mu.Unlock()
	}

	// Notify other users
	for _, client := range clientsToNotify {
		client.sendMessage(s.logger, MsgTypeUserLeft, UserLeftPayload{
			UserID:   clientID,
			Username: username,
		})

		if notifyHostChanged {
			client.sendMessage(s.logger, MsgTypeHostChanged, HostChangedPayload{
				NewHostID:   hostID,
				NewHostName: hostName,
			})
		}
	}

	s.logger.Info("User left room",
		zap.String("username", username),
		zap.String("user_id", clientID),
		zap.String("room_code", room.Code),
		zap.Bool("was_host", wasHost))
}

func (c *Client) sendMessage(logger *zap.Logger, msgType string, payload interface{}) {
	if c == nil || c.codec == nil || c.Send == nil {
		return
	}

	// Use the client's codec to encode the message
	msgData, err := c.codec.Encode(msgType, payload)
	if err != nil {
		logger.Error("Error encoding message", zap.String("message_type", msgType), zap.String("payload_type", fmt.Sprintf("%T", payload)), zap.Error(err))
		return
	}

	logger.Debug("Message encoded successfully",
		zap.String("message_type", msgType),
		zap.String("payload_type", fmt.Sprintf("%T", payload)),
		zap.Int("encoded_size_bytes", len(msgData)))

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		logger.Debug("Attempted to send to closed client", zap.String("client_id", c.clientID()))
		return
	}

	select {
	case c.Send <- msgData:
		logger.Debug("Message queued for sending", zap.String("message_type", msgType), zap.Int("size", len(msgData)))
	default:
		logger.Warn("Client send buffer full; closing slow client", zap.String("client_id", c.clientID()))
		c.closed = true
		close(c.Send)
	}
}

func (c *Client) sendError(logger *zap.Logger, code, message string) {
	c.sendMessage(logger, MsgTypeError, ErrorPayload{
		Code:    code,
		Message: message,
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

func main() {
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
