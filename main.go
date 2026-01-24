package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// Message types
const (
	// Client -> Server
	MsgTypeCreateRoom     = "create_room"
	MsgTypeJoinRoom       = "join_room"
	MsgTypeLeaveRoom      = "leave_room"
	MsgTypeApproveJoin    = "approve_join"
	MsgTypeRejectJoin     = "reject_join"
	MsgTypePlaybackAction = "playback_action"
	MsgTypeBufferReady    = "buffer_ready"
	MsgTypeKickUser       = "kick_user"
	MsgTypePing           = "ping"
	MsgTypeChat           = "chat"
	MsgTypeRequestSync    = "request_sync"
	MsgTypeReconnect      = "reconnect"

	// Server -> Client
	MsgTypeRoomCreated      = "room_created"
	MsgTypeJoinRequest      = "join_request"
	MsgTypeJoinApproved     = "join_approved"
	MsgTypeJoinRejected     = "join_rejected"
	MsgTypeUserJoined       = "user_joined"
	MsgTypeUserLeft         = "user_left"
	MsgTypeSyncPlayback     = "sync_playback"
	MsgTypeBufferWait       = "buffer_wait"
	MsgTypeBufferComplete   = "buffer_complete"
	MsgTypeError            = "error"
	MsgTypePong             = "pong"
	MsgTypeRoomState        = "room_state"
	MsgTypeChatMessage      = "chat_message"
	MsgTypeHostChanged      = "host_changed"
	MsgTypeKicked           = "kicked"
	MsgTypeSyncState        = "sync_state"
	MsgTypeReconnected      = "reconnected"
	MsgTypeUserReconnected  = "user_reconnected"
	MsgTypeUserDisconnected = "user_disconnected"
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
)

// Message is the base message structure
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

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
	Action    string     `json:"action"`
	TrackID   string     `json:"track_id,omitempty"`
	Position  int64      `json:"position,omitempty"` // milliseconds
	TrackInfo *TrackInfo `json:"track_info,omitempty"`
}

// TrackInfo contains information about a track
type TrackInfo struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Artist    string `json:"artist"`
	Album     string `json:"album,omitempty"`
	Duration  int64  `json:"duration"` // milliseconds
	Thumbnail string `json:"thumbnail,omitempty"`
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
	Queue        []TrackInfo `json:"queue,omitempty"`
}

// UserInfo contains information about a user
type UserInfo struct {
	UserID      string `json:"user_id"`
	Username    string `json:"username"`
	IsHost      bool   `json:"is_host"`
	IsConnected bool   `json:"is_connected"`
}

// ChatPayload is for chat messages
type ChatPayload struct {
	Message string `json:"message"`
}

// ChatMessagePayload is sent to all users in a room
type ChatMessagePayload struct {
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

// KickUserPayload is for kicking a user from the room
type KickUserPayload struct {
	UserID string `json:"user_id"`
	Reason string `json:"reason,omitempty"`
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
	CurrentTrack *TrackInfo `json:"current_track,omitempty"`
	IsPlaying    bool       `json:"is_playing"`
	Position     int64      `json:"position"`    // milliseconds
	LastUpdate   int64      `json:"last_update"` // unix timestamp ms
}

// ReconnectPayload is for reconnecting to a room
type ReconnectPayload struct {
	SessionToken string `json:"session_token"`
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
	ID           string
	Username     string
	SessionToken string
	Conn         *websocket.Conn
	Room         *Room
	Send         chan []byte
	mu           sync.Mutex
}

// Room represents a listening room
type Room struct {
	Code               string
	Host               *Client
	Clients            map[string]*Client
	PendingJoins       map[string]*Client  // Users waiting for approval
	DisconnectedUsers  map[string]*Session // Users temporarily disconnected
	State              *RoomState
	BufferingUsers     map[string]bool // Track which users are still buffering
	HostStartPosition  int64           // Host's position when buffering started
	HostDisconnectedAt *time.Time      // When the host disconnected (nil if connected)
	mu                 sync.RWMutex
}

// Server is the main WebSocket server
type Server struct {
	rooms    map[string]*Room
	sessions map[string]*Session // sessionToken -> Session
	clients  map[*Client]bool
	upgrader websocket.Upgrader
	mu       sync.RWMutex
	logger   *zap.Logger
	rng      *rand.Rand
}

const (
	// Grace period for reconnection (5 minutes)
	ReconnectGracePeriod = 5 * time.Minute
	// How often to clean up expired sessions
	SessionCleanupInterval = 1 * time.Minute
)

func NewServer(logger *zap.Logger) *Server {
	s := &Server{
		rooms:    make(map[string]*Room),
		sessions: make(map[string]*Session),
		clients:  make(map[*Client]bool),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for mobile app
			},
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		logger: logger,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	// Start session cleanup goroutine
	go s.cleanupExpiredSessions()

	return s
}

func (s *Server) cleanupExpiredSessions() {
	ticker := time.NewTicker(SessionCleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		expiredTokens := make([]string, 0)

		for token, session := range s.sessions {
			if now.Sub(session.DisconnectAt) > ReconnectGracePeriod {
				expiredTokens = append(expiredTokens, token)
			}
		}

		for _, token := range expiredTokens {
			session := s.sessions[token]
			delete(s.sessions, token)
			s.logger.Info("Session expired",
				zap.String("user_id", session.UserID),
				zap.String("room_code", session.RoomCode))

			// Also clean up from room's disconnected users
			if room, exists := s.rooms[session.RoomCode]; exists {
				room.mu.Lock()
				delete(room.DisconnectedUsers, session.UserID)

				// Remove from room state users if still there
				newUsers := make([]UserInfo, 0, len(room.State.Users))
				for _, u := range room.State.Users {
					if u.UserID != session.UserID {
						newUsers = append(newUsers, u)
					}
				}
				room.State.Users = newUsers

				// Notify remaining users
				for _, client := range room.Clients {
					if client != nil {
						client.sendMessage(s.logger, MsgTypeUserLeft, UserLeftPayload{
							UserID:   session.UserID,
							Username: session.Username,
						})
					}
				}
				room.mu.Unlock()
			}
		}
		s.mu.Unlock()
	}
}

func (s *Server) generateRoomCode() string {
	const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789" // Removed confusing chars
	code := make([]byte, 6)
	for i := range code {
		code[i] = chars[s.rng.Intn(len(chars))]
	}
	return string(code)
}

func (s *Server) generateUserID() string {
	return fmt.Sprintf("user_%d_%d", time.Now().UnixNano(), s.rng.Intn(10000))
}

func (s *Server) generateSessionToken() string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	token := make([]byte, 32)
	for i := range token {
		token[i] = chars[s.rng.Intn(len(chars))]
	}
	return string(token)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Warn("WebSocket upgrade error", zap.Error(err))
		return
	}

	client := &Client{
		ID:   s.generateUserID(),
		Conn: conn,
		Send: make(chan []byte, 256),
	}

	s.mu.Lock()
	s.clients[client] = true
	s.mu.Unlock()

	go client.writePump(s.logger)
	go client.readPump(s)

	s.logger.Info("Client connected", zap.String("client_id", client.ID))
}

func (c *Client) writePump(logger *zap.Logger) {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
				logger.Debug("Write error for client", zap.String("client_id", c.ID), zap.Error(err))
				return
			}

		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
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

	c.Conn.SetReadLimit(65536)
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				s.logger.Debug("Read error for client", zap.String("client_id", c.ID), zap.Error(err))
			}
			break
		}

		c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		s.handleMessage(c, message)
	}
}

func (s *Server) removeClient(c *Client) {
	s.mu.Lock()
	delete(s.clients, c)
	s.mu.Unlock()

	if c.Room != nil {
		s.handleClientDisconnect(c)
	}

	// Check if send channel is still open before closing
	c.mu.Lock()
	select {
	case _, ok := <-c.Send:
		if ok {
			close(c.Send)
		}
	default:
		close(c.Send)
	}
	c.mu.Unlock()

	s.logger.Info("Client disconnected", zap.String("client_id", c.ID))
}

// handleClientDisconnect handles a client disconnecting - creates a session for reconnection
func (s *Server) handleClientDisconnect(c *Client) {
	if c.Room == nil {
		return
	}

	room := c.Room
	room.mu.Lock()

	wasHost := room.Host == c
	username := c.Username

	// Create session for reconnection
	session := &Session{
		UserID:       c.ID,
		Username:     c.Username,
		RoomCode:     room.Code,
		IsHost:       wasHost,
		DisconnectAt: time.Now(),
	}

	// Generate session token if not already present
	if c.SessionToken == "" {
		c.SessionToken = s.generateSessionToken()
	}

	// Store the session
	s.mu.Lock()
	s.sessions[c.SessionToken] = session
	s.mu.Unlock()

	// Remove from active clients but add to disconnected users
	delete(room.Clients, c.ID)
	delete(room.BufferingUsers, c.ID)

	if room.DisconnectedUsers == nil {
		room.DisconnectedUsers = make(map[string]*Session)
	}
	room.DisconnectedUsers[c.ID] = session

	// Mark user as disconnected in room state
	for i := range room.State.Users {
		if room.State.Users[i].UserID == c.ID {
			room.State.Users[i].IsConnected = false
			break
		}
	}

	// Track if host disconnected
	if wasHost {
		now := time.Now()
		room.HostDisconnectedAt = &now
	}

	c.Room = nil

	// If room has no active clients and no disconnected users, delete it
	if len(room.Clients) == 0 && len(room.DisconnectedUsers) == 0 {
		roomCode := room.Code
		room.mu.Unlock()
		s.mu.Lock()
		delete(s.rooms, roomCode)
		s.mu.Unlock()
		s.logger.Info("Room deleted (empty)", zap.String("room_code", roomCode))
		return
	}

	// If host disconnected but there are other active clients, notify them
	// Don't transfer host yet - wait for reconnection grace period
	room.mu.Unlock()

	// Notify other users about the temporary disconnect
	room.mu.RLock()
	for _, client := range room.Clients {
		if client != nil {
			client.sendMessage(s.logger, MsgTypeUserDisconnected, UserDisconnectedPayload{
				UserID:   c.ID,
				Username: username,
			})
		}
	}
	room.mu.RUnlock()

	s.logger.Info("User temporarily disconnected",
		zap.String("username", username),
		zap.String("user_id", c.ID),
		zap.String("room_code", room.Code),
		zap.Bool("was_host", wasHost),
		zap.String("session_token", c.SessionToken))
}

// handleReconnect handles a client trying to reconnect to their room
func (s *Server) handleReconnect(c *Client, payload json.RawMessage) {
	var p ReconnectPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid reconnect payload")
		return
	}

	if p.SessionToken == "" {
		c.sendError(s.logger, "missing_session_token", "Session token is required")
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
	c.ID = session.UserID
	c.Username = session.Username
	c.SessionToken = p.SessionToken
	c.Room = room

	// Add back to room clients
	room.Clients[c.ID] = c
	delete(room.DisconnectedUsers, c.ID)

	// Mark user as connected in room state
	for i := range room.State.Users {
		if room.State.Users[i].UserID == c.ID {
			room.State.Users[i].IsConnected = true
			break
		}
	}

	// Restore host status if they were the host
	if session.IsHost && room.HostDisconnectedAt != nil {
		room.Host = c
		room.HostDisconnectedAt = nil

		// Update IsHost flag in users list
		for i := range room.State.Users {
			room.State.Users[i].IsHost = room.State.Users[i].UserID == c.ID
		}
	}

	isHost := room.Host == c
	stateCopy := *room.State

	room.mu.Unlock()

	// Remove session since reconnection succeeded
	s.mu.Lock()
	delete(s.sessions, p.SessionToken)
	s.mu.Unlock()

	// Send reconnected message to the client
	c.sendMessage(s.logger, MsgTypeReconnected, ReconnectedPayload{
		RoomCode: room.Code,
		UserID:   c.ID,
		State:    &stateCopy,
		IsHost:   isHost,
	})

	// Notify other users
	room.mu.RLock()
	for _, client := range room.Clients {
		if client != nil && client.ID != c.ID {
			client.sendMessage(s.logger, MsgTypeUserReconnected, UserReconnectedPayload{
				UserID:   c.ID,
				Username: c.Username,
			})
		}
	}
	room.mu.RUnlock()

	s.logger.Info("User reconnected",
		zap.String("username", c.Username),
		zap.String("user_id", c.ID),
		zap.String("room_code", room.Code),
		zap.Bool("is_host", isHost))
}

func (s *Server) handleMessage(c *Client, data []byte) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		s.logger.Debug("Invalid message received", zap.String("client_id", c.ID), zap.Error(err))
		c.sendError(s.logger, "invalid_message", "Invalid message format")
		return
	}

	if msg.Type == "" {
		c.sendError(s.logger, "invalid_message", "Message type is required")
		return
	}

	s.logger.Debug("Message received", zap.String("client_id", c.ID), zap.String("message_type", msg.Type))

	switch msg.Type {
	case MsgTypeCreateRoom:
		s.handleCreateRoom(c, msg.Payload)
	case MsgTypeJoinRoom:
		s.handleJoinRoom(c, msg.Payload)
	case MsgTypeLeaveRoom:
		s.leaveRoom(c)
	case MsgTypeApproveJoin:
		s.handleApproveJoin(c, msg.Payload)
	case MsgTypeRejectJoin:
		s.handleRejectJoin(c, msg.Payload)
	case MsgTypePlaybackAction:
		s.handlePlaybackAction(c, msg.Payload)
	case MsgTypeBufferReady:
		s.handleBufferReady(c, msg.Payload)
	case MsgTypeKickUser:
		s.handleKickUser(c, msg.Payload)
	case MsgTypePing:
		c.sendMessage(s.logger, MsgTypePong, nil)
	case MsgTypeChat:
		s.handleChat(c, msg.Payload)
	case MsgTypeRequestSync:
		s.handleRequestSync(c)
	case MsgTypeReconnect:
		s.handleReconnect(c, msg.Payload)
	default:
		c.sendError(s.logger, "unknown_message_type", fmt.Sprintf("Unknown message type: %s", msg.Type))
	}
}

func (s *Server) handleCreateRoom(c *Client, payload json.RawMessage) {
	var p CreateRoomPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid create room payload")
		return
	}

	if p.Username == "" {
		c.sendError(s.logger, "missing_username", "Username is required")
		return
	}

	// Validate username length
	if len(p.Username) > 100 {
		c.sendError(s.logger, "username_too_long", "Username must be 100 characters or less")
		return
	}

	// Generate unique room code with retry limit
	var code string
	maxRetries := 100
	for i := 0; i < maxRetries; i++ {
		code = s.generateRoomCode()
		s.mu.RLock()
		_, exists := s.rooms[code]
		s.mu.RUnlock()
		if !exists {
			break
		}
	}

	if code == "" {
		s.logger.Error("Failed to generate unique room code after retries")
		c.sendError(s.logger, "server_error", "Failed to create room")
		return
	}

	c.Username = p.Username
	c.SessionToken = s.generateSessionToken()

	room := &Room{
		Code:              code,
		Host:              c,
		Clients:           make(map[string]*Client),
		PendingJoins:      make(map[string]*Client),
		DisconnectedUsers: make(map[string]*Session),
		BufferingUsers:    make(map[string]bool),
		State: &RoomState{
			RoomCode:   code,
			HostID:     c.ID,
			Users:      []UserInfo{{UserID: c.ID, Username: c.Username, IsHost: true, IsConnected: true}},
			IsPlaying:  false,
			Position:   0,
			LastUpdate: time.Now().UnixMilli(),
			Queue:      []TrackInfo{},
		},
	}

	room.Clients[c.ID] = c
	c.Room = room

	s.mu.Lock()
	s.rooms[code] = room
	s.mu.Unlock()

	c.sendMessage(s.logger, MsgTypeRoomCreated, RoomCreatedPayload{
		RoomCode:     code,
		UserID:       c.ID,
		SessionToken: c.SessionToken,
	})

	s.logger.Info("Room created",
		zap.String("room_code", code),
		zap.String("host_name", c.Username),
		zap.String("host_id", c.ID))
}

func (s *Server) handleJoinRoom(c *Client, payload json.RawMessage) {
	var p JoinRoomPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid join room payload")
		return
	}

	if p.Username == "" {
		c.sendError(s.logger, "missing_username", "Username is required")
		return
	}

	if len(p.Username) > 100 {
		c.sendError(s.logger, "username_too_long", "Username must be 100 characters or less")
		return
	}

	if p.RoomCode == "" {
		c.sendError(s.logger, "missing_room_code", "Room code is required")
		return
	}

	s.mu.RLock()
	room, exists := s.rooms[p.RoomCode]
	s.mu.RUnlock()

	if !exists {
		c.sendError(s.logger, "room_not_found", "Room not found")
		return
	}

	c.Username = p.Username

	room.mu.Lock()
	// Check if user is already in the room or pending
	if _, exists := room.Clients[c.ID]; exists {
		room.mu.Unlock()
		c.sendError(s.logger, "already_in_room", "You are already in this room")
		return
	}

	if _, exists := room.PendingJoins[c.ID]; exists {
		room.mu.Unlock()
		c.sendError(s.logger, "already_pending", "Your join request is already pending")
		return
	}

	// Validate room isn't in an invalid state
	if room.Host == nil {
		room.mu.Unlock()
		c.sendError(s.logger, "room_invalid", "Room is no longer valid")
		return
	}

	// Add to pending joins
	room.PendingJoins[c.ID] = c
	room.mu.Unlock()

	// Notify host of join request - with nil check
	if room.Host != nil {
		room.Host.sendMessage(s.logger, MsgTypeJoinRequest, JoinRequestPayload{
			UserID:   c.ID,
			Username: c.Username,
		})
	}

	s.logger.Info("Join request received",
		zap.String("username", c.Username),
		zap.String("user_id", c.ID),
		zap.String("room_code", p.RoomCode))
}

func (s *Server) handleApproveJoin(c *Client, payload json.RawMessage) {
	var p ApproveJoinPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid approve join payload")
		return
	}

	if p.UserID == "" {
		c.sendError(s.logger, "missing_user_id", "User ID is required")
		return
	}

	if c.Room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room := c.Room
	room.mu.Lock()
	defer room.mu.Unlock()

	if room.Host == nil || room.Host != c {
		c.sendError(s.logger, "not_host", "Only the host can approve join requests")
		return
	}

	joiningClient, exists := room.PendingJoins[p.UserID]
	if !exists {
		c.sendError(s.logger, "join_request_not_found", "Join request not found")
		return
	}

	// Verify joining client is still valid
	if joiningClient == nil {
		delete(room.PendingJoins, p.UserID)
		c.sendError(s.logger, "user_disconnected", "User has disconnected")
		return
	}

	// Remove from pending and add to room
	delete(room.PendingJoins, p.UserID)
	room.Clients[joiningClient.ID] = joiningClient
	joiningClient.Room = room
	joiningClient.SessionToken = s.generateSessionToken()

	// Update room state
	room.State.Users = append(room.State.Users, UserInfo{
		UserID:      joiningClient.ID,
		Username:    joiningClient.Username,
		IsHost:      false,
		IsConnected: true,
	})

	// Send approval to the joining user
	joiningClient.sendMessage(s.logger, MsgTypeJoinApproved, JoinApprovedPayload{
		RoomCode:     room.Code,
		UserID:       joiningClient.ID,
		SessionToken: joiningClient.SessionToken,
		State:        room.State,
	})

	// Notify all other users
	for _, client := range room.Clients {
		if client != nil && client.ID != joiningClient.ID {
			client.sendMessage(s.logger, MsgTypeUserJoined, UserJoinedPayload{
				UserID:   joiningClient.ID,
				Username: joiningClient.Username,
			})
		}
	}

	s.logger.Info("User approved to join room",
		zap.String("username", joiningClient.Username),
		zap.String("user_id", joiningClient.ID),
		zap.String("room_code", room.Code))
}

func (s *Server) handleRejectJoin(c *Client, payload json.RawMessage) {
	var p RejectJoinPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid reject join payload")
		return
	}

	if p.UserID == "" {
		c.sendError(s.logger, "missing_user_id", "User ID is required")
		return
	}

	if c.Room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room := c.Room
	room.mu.Lock()
	defer room.mu.Unlock()

	if room.Host == nil || room.Host != c {
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
		zap.String("username", joiningClient.Username),
		zap.String("user_id", joiningClient.ID),
		zap.String("room_code", room.Code))
}

func (s *Server) handlePlaybackAction(c *Client, payload json.RawMessage) {
	var p PlaybackActionPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid playback action payload")
		return
	}

	if p.Action == "" {
		c.sendError(s.logger, "missing_action", "Action is required")
		return
	}

	if c.Room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room := c.Room
	room.mu.Lock()
	defer room.mu.Unlock()

	if room.Host == nil || room.Host != c {
		c.sendError(s.logger, "not_host", "Only the host can control playback")
		return
	}

	// Update room state based on action
	room.State.LastUpdate = time.Now().UnixMilli()

	switch p.Action {
	case ActionPlay:
		// Block play if no track is set
		if room.State.CurrentTrack == nil {
			s.logger.Debug("Play blocked - no current track", zap.String("room_code", room.Code))
			c.sendError(s.logger, "no_track", "Cannot play without a track")
			return
		}
		room.State.IsPlaying = true
		room.State.Position = p.Position

	case ActionPause:
		// Pause is always allowed
		room.State.IsPlaying = false
		room.State.Position = p.Position

	case ActionSeek:
		if p.Position < 0 {
			c.sendError(s.logger, "invalid_position", "Position cannot be negative")
			return
		}
		room.State.Position = p.Position

	case ActionChangeTrack:
		if p.TrackInfo == nil {
			c.sendError(s.logger, "missing_track_info", "Track info is required for track change")
			return
		}

		if p.TrackInfo.ID == "" || p.TrackInfo.Title == "" {
			c.sendError(s.logger, "invalid_track_info", "Track must have ID and title")
			return
		}

		// Allow 0 or negative duration - some tracks don't have duration metadata initially
		// Use a default duration of 3 minutes if not provided
		if p.TrackInfo.Duration <= 0 {
			p.TrackInfo.Duration = 180000 // 3 minutes in ms
			s.logger.Debug("Track duration not provided, using default", zap.String("track_id", p.TrackInfo.ID))
		}

		room.State.CurrentTrack = p.TrackInfo
		room.State.Position = 0
		room.State.IsPlaying = false

		// For new tracks, always start at position 0
		room.HostStartPosition = 0
		s.logger.Debug("Track changed", zap.String("room_code", room.Code), zap.String("track_id", p.TrackInfo.ID))

		// Initialize buffering for all users EXCEPT the host
		room.BufferingUsers = make(map[string]bool)
		waitingFor := make([]string, 0, len(room.Clients)-1)
		for id := range room.Clients {
			if id != c.ID {
				room.BufferingUsers[id] = true
				waitingFor = append(waitingFor, id)
			}
		}

		// Broadcast track change
		for _, client := range room.Clients {
			if client != nil {
				// Send track change
				client.sendMessage(s.logger, MsgTypeSyncPlayback, p)

				// Ensure everyone is paused at position 0 during buffering
				client.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
					Action:   ActionPause,
					Position: 0,
				})

				if len(waitingFor) > 0 {
					client.sendMessage(s.logger, MsgTypeBufferWait, BufferWaitPayload{
						TrackID:    p.TrackInfo.ID,
						WaitingFor: waitingFor,
					})
				} else {
					// If no guests need to buffer, immediately notify
					client.sendMessage(s.logger, MsgTypeBufferComplete, BufferCompletePayload{
						TrackID: p.TrackInfo.ID,
					})
				}
			}
		}
		return

	case ActionSkipNext, ActionSkipPrev:
		room.State.Position = 0

	case ActionQueueAdd:
		if p.TrackInfo == nil {
			c.sendError(s.logger, "missing_track_info", "Track info is required for queue add")
			return
		}

		if p.TrackInfo.ID == "" || p.TrackInfo.Title == "" {
			c.sendError(s.logger, "invalid_track_info", "Track must have ID and title")
			return
		}

		// Limit queue size to prevent memory issues
		if len(room.State.Queue) >= 1000 {
			c.sendError(s.logger, "queue_full", "Queue is full")
			return
		}

		room.State.Queue = append(room.State.Queue, *p.TrackInfo)

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
		zap.String("host_name", c.Username))
}

func (s *Server) handleBufferReady(c *Client, payload json.RawMessage) {
	var p BufferReadyPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid buffer ready payload")
		return
	}

	if p.TrackID == "" {
		c.sendError(s.logger, "missing_track_id", "Track ID is required")
		return
	}

	if c.Room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room := c.Room
	room.mu.Lock()
	defer room.mu.Unlock()

	s.logger.Debug("Buffer ready received",
		zap.String("username", c.Username),
		zap.String("user_id", c.ID),
		zap.String("track_id", p.TrackID))

	// Mark user as ready
	delete(room.BufferingUsers, c.ID)

	// Check if all users are ready
	if len(room.BufferingUsers) == 0 {
		// All users ready - sync everyone to position 0 for new track
		syncPosition := int64(0)
		room.State.Position = syncPosition
		room.State.LastUpdate = time.Now().UnixMilli()

		s.logger.Debug("All users buffered",
			zap.String("track_id", p.TrackID),
			zap.String("room_code", room.Code))

		for _, client := range room.Clients {
			if client != nil {
				// Step 1: Send buffer complete notification
				client.sendMessage(s.logger, MsgTypeBufferComplete, BufferCompletePayload{
					TrackID: p.TrackID,
				})

				// Step 2: SEEK everyone to exact position
				client.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
					Action:   ActionSeek,
					Position: syncPosition,
				})

				// Step 3: Only PLAY if the host actually started playback
				if room.State.IsPlaying {
					client.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
						Action:   ActionPlay,
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

func (s *Server) handleKickUser(c *Client, payload json.RawMessage) {
	var p KickUserPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid kick user payload")
		return
	}

	if p.UserID == "" {
		c.sendError(s.logger, "missing_user_id", "User ID is required")
		return
	}

	if c.Room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room := c.Room
	room.mu.Lock()

	if room.Host == nil || room.Host != c {
		room.mu.Unlock()
		c.sendError(s.logger, "not_host", "Only the host can kick users")
		return
	}

	if p.UserID == c.ID {
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

	kickedUsername := targetClient.Username
	targetClient.Room = nil
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
	room.mu.RLock()
	for _, client := range room.Clients {
		if client != nil {
			client.sendMessage(s.logger, MsgTypeUserLeft, UserLeftPayload{
				UserID:   p.UserID,
				Username: kickedUsername,
			})
		}
	}
	room.mu.RUnlock()

	s.logger.Info("User kicked from room",
		zap.String("username", kickedUsername),
		zap.String("user_id", p.UserID),
		zap.String("room_code", room.Code))
}

func (s *Server) handleChat(c *Client, payload json.RawMessage) {
	var p ChatPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid chat payload")
		return
	}

	if c.Room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	if p.Message == "" {
		return // Silently ignore empty messages
	}

	// Limit message length
	const maxMessageLength = 500
	if len(p.Message) > maxMessageLength {
		p.Message = p.Message[:maxMessageLength]
	}

	room := c.Room
	room.mu.RLock()
	defer room.mu.RUnlock()

	if len(room.Clients) == 0 {
		return // Room is empty, don't send
	}

	chatMsg := ChatMessagePayload{
		UserID:    c.ID,
		Username:  c.Username,
		Message:   p.Message,
		Timestamp: time.Now().UnixMilli(),
	}

	for _, client := range room.Clients {
		if client != nil {
			client.sendMessage(s.logger, MsgTypeChatMessage, chatMsg)
		}
	}
}

func (s *Server) handleRequestSync(c *Client) {
	if c.Room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room := c.Room
	room.mu.RLock()
	defer room.mu.RUnlock()

	// Calculate current position based on time elapsed since last update
	currentPosition := room.State.Position
	if room.State.IsPlaying {
		elapsed := time.Now().UnixMilli() - room.State.LastUpdate
		currentPosition = room.State.Position + elapsed
	}

	s.logger.Debug("Sync request received",
		zap.String("username", c.Username),
		zap.String("user_id", c.ID),
		zap.Bool("has_track", room.State.CurrentTrack != nil),
		zap.Bool("is_playing", room.State.IsPlaying),
		zap.Int64("position", currentPosition))

	c.sendMessage(s.logger, MsgTypeSyncState, SyncStatePayload{
		CurrentTrack: room.State.CurrentTrack,
		IsPlaying:    room.State.IsPlaying,
		Position:     currentPosition,
		LastUpdate:   time.Now().UnixMilli(),
	})
}

func (s *Server) leaveRoom(c *Client) {
	if c.Room == nil {
		return
	}

	room := c.Room
	room.mu.Lock()

	delete(room.Clients, c.ID)
	delete(room.BufferingUsers, c.ID)
	delete(room.PendingJoins, c.ID)
	delete(room.DisconnectedUsers, c.ID)

	// Also remove any session token for this user (intentional leave = no reconnect)
	if c.SessionToken != "" {
		s.mu.Lock()
		delete(s.sessions, c.SessionToken)
		s.mu.Unlock()
	}

	username := c.Username
	wasHost := room.Host == c

	// Update room state users list
	newUsers := make([]UserInfo, 0, len(room.State.Users))
	for _, u := range room.State.Users {
		if u.UserID != c.ID {
			newUsers = append(newUsers, u)
		}
	}
	room.State.Users = newUsers

	c.Room = nil

	// If room is empty (no active or disconnected users), delete it
	if len(room.Clients) == 0 && len(room.DisconnectedUsers) == 0 {
		roomCode := room.Code
		room.mu.Unlock()
		s.mu.Lock()
		delete(s.rooms, roomCode)
		s.mu.Unlock()
		s.logger.Info("Room deleted (empty)", zap.String("room_code", roomCode))
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
			room.State.HostID = newHost.ID

			// Update IsHost flag in users list
			for i := range room.State.Users {
				room.State.Users[i].IsHost = room.State.Users[i].UserID == newHost.ID
			}
		}
	}

	room.mu.Unlock()

	// Notify other users
	room.mu.RLock()
	for _, client := range room.Clients {
		if client != nil {
			client.sendMessage(s.logger, MsgTypeUserLeft, UserLeftPayload{
				UserID:   c.ID,
				Username: username,
			})

			if wasHost && newHost != nil {
				client.sendMessage(s.logger, MsgTypeHostChanged, HostChangedPayload{
					NewHostID:   newHost.ID,
					NewHostName: newHost.Username,
				})
			}
		}
	}
	room.mu.RUnlock()

	s.logger.Info("User left room",
		zap.String("username", username),
		zap.String("user_id", c.ID),
		zap.String("room_code", room.Code),
		zap.Bool("was_host", wasHost))
}

func (c *Client) sendMessage(logger *zap.Logger, msgType string, payload interface{}) {
	data, err := json.Marshal(payload)
	if err != nil {
		logger.Debug("Error marshaling payload", zap.String("message_type", msgType), zap.Error(err))
		return
	}

	msg := Message{
		Type:    msgType,
		Payload: data,
	}

	msgData, err := json.Marshal(msg)
	if err != nil {
		logger.Debug("Error marshaling message", zap.String("message_type", msgType), zap.Error(err))
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	select {
	case c.Send <- msgData:
	default:
		logger.Debug("Client send buffer full", zap.String("client_id", c.ID))
	}
}

func (c *Client) sendError(logger *zap.Logger, code, message string) {
	c.sendMessage(logger, MsgTypeError, ErrorPayload{
		Code:    code,
		Message: message,
	})
}

func main() {
	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	defer logger.Sync()

	server := NewServer(logger)

	http.HandleFunc("/ws", server.handleWebSocket)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
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

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		logger.Fatal("Server failed", zap.Error(err))
	}
}
