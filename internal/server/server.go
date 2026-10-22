package server

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	mathrand "math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
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
	claimed      bool // a reconnect is restoring this session; guarded by Server.mu
}

type Room struct {
	Code               string
	Host               *Client
	Clients            map[string]*Client
	PendingJoins       map[string]*Client     // Users waiting for approval
	PendingSuggestions map[string]*Suggestion // Track suggestions waiting for host action
	DisconnectedUsers  map[string]*Session    // Users temporarily disconnected
	State              *RoomState
	HostDisconnectedAt *time.Time // When the host disconnected (nil if connected)
	EmptySince         *time.Time // When the room became empty (nil if not empty)
	// OwnerID is the host replaced by automatic failover; they take the room
	// back if they reconnect within the grace period.
	OwnerID string
	// LastHostSequence is the newest host_sequence accepted from the current host.
	LastHostSequence uint64
	// TrackRevision is the revision of the last change_track broadcast.
	TrackRevision uint64
	// PendingNotices holds suggestion outcomes for disconnected guests.
	PendingNotices map[string][]pendingNotice
	syncMu         sync.Mutex // Serializes playback snapshots and their delivery.
	mu             sync.RWMutex
}

type pendingNotice struct {
	MsgType string
	Payload interface{}
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
	rooms           map[string]*Room
	sessions        map[string]*Session // sessionToken -> Session
	clients         map[*Client]bool
	upgrader        websocket.Upgrader
	mu              sync.RWMutex
	rngMu           sync.Mutex
	logger          *zap.Logger
	uaPolicy        *uaPolicy
	database        *database
	connectionSlots chan struct{}
	rng             *mathrand.Rand
	startTime       time.Time                // Track when server started for room retention logic
	sources         map[string]int           // open connections per source address
	roomCreates     map[string]*sourceWindow // room creations per source address
	stop            chan struct{}
	stopOnce        sync.Once
	pumps           sync.WaitGroup // read pumps, so shutdown can wait for disconnect cleanup
	shuttingDown    atomic.Bool
	persistMu       sync.Mutex
	persisted       map[string][]byte // last room records written to the database
	lastFlush       time.Time
}

type sourceWindow struct {
	start time.Time
	count int
}

const (
	// Grace period for reconnection (increased from 5 to 15 minutes for better recovery)
	ReconnectGracePeriod = 15 * time.Minute
	// How long guests wait for a disconnected host before a deputy takes over.
	HostFailoverTimeout       = 30 * time.Second
	HostFailoverCheckInterval = 5 * time.Second
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
	MaxSuggestionsPerUser = 10
	// MaxQueueBytes keeps every queue snapshot decodable by clients and
	// sendable uncompressed within MaxReadMessageSize.
	MaxQueueBytes = 400 * 1024
	// Connection limits
	MaxClients              = 10000
	MaxRooms                = 10000
	MaxClientsPerRoom       = 100
	MaxConnectionsPerSource = 32
	MaxRoomCreatesPerSource = 10
	RoomCreateWindow        = time.Minute
	MaxReadMessageSize      = 524288 // 512KB (reasonable for queue syncs)
	MaxHeaderBytes          = 65536
	ReadTimeout             = 60 * time.Second
	WriteTimeout            = 10 * time.Second
	PingInterval            = 30 * time.Second
	IdleTimeout             = 120 * time.Second
	ShutdownTimeout         = 10 * time.Second // whole graceful shutdown; fits Docker's default grace
	RestartEta              = 5 * time.Second  // advertised reconnect delay in server_restarting
	StateFlushInterval      = 2 * time.Second
	MessageRateWindow       = time.Second
	MaxMessagesPerWindow    = 60
	SyncResponseInterval    = time.Second
)

func NewServer(logger *zap.Logger) *Server {
	s := &Server{
		rooms:    make(map[string]*Room),
		sessions: make(map[string]*Session),
		clients:  make(map[*Client]bool),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
		},
		logger:          logger,
		uaPolicy:        defaultUAPolicy(),
		connectionSlots: make(chan struct{}, MaxClients),
		rng:             mathrand.New(mathrand.NewSource(time.Now().UnixNano())),
		startTime:       time.Now(),
		sources:         make(map[string]int),
		roomCreates:     make(map[string]*sourceWindow),
		stop:            make(chan struct{}),
		persisted:       make(map[string][]byte),
	}

	// Start cleanup goroutines
	go s.cleanupExpiredSessions()
	go s.cleanupEmptyRooms()

	return s
}

// Close stops the background workers.
func (s *Server) Close() {
	s.stopOnce.Do(func() { close(s.stop) })
}

func (s *Server) policy() *uaPolicy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.uaPolicy
}

// SetUAPolicy swaps the policy used for new connections.
func (s *Server) SetUAPolicy(policy *uaPolicy) {
	s.mu.Lock()
	s.uaPolicy = policy
	s.mu.Unlock()
}

func (s *Server) generateRoomCode() string {
	const chars = "1234567890QWERTYUPASDFGHJLKZXCVBNM"
	// Reject bytes past the last whole multiple of len(chars) to avoid bias.
	const limit = 256 - 256%len(chars)
	code := make([]byte, 0, 8)
	buf := make([]byte, 16)
	for len(code) < cap(code) {
		if _, err := rand.Read(buf); err != nil {
			s.rngMu.Lock()
			code = append(code, chars[s.rng.Intn(len(chars))])
			s.rngMu.Unlock()
			continue
		}
		for _, b := range buf {
			if int(b) < limit && len(code) < cap(code) {
				code = append(code, chars[int(b)%len(chars)])
			}
		}
	}
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

func (s *Server) tryAcquireConnectionSlot() bool {
	if s.connectionSlots == nil {
		return true
	}
	select {
	case s.connectionSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Server) releaseConnectionSlot() {
	if s.connectionSlots != nil {
		<-s.connectionSlots
	}
}

// requestSource identifies where a connection comes from. Production runs
// behind Cloudflare Tunnel, which reports the real client in CF-Connecting-IP.
func requestSource(r *http.Request) string {
	if ip := strings.TrimSpace(r.Header.Get("CF-Connecting-IP")); ip != "" {
		return ip
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) acquireSource(source string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sources[source] >= MaxConnectionsPerSource {
		return false
	}
	s.sources[source]++
	return true
}

func (s *Server) releaseSource(source string) {
	s.mu.Lock()
	if s.sources[source] <= 1 {
		delete(s.sources, source)
	} else {
		s.sources[source]--
	}
	s.mu.Unlock()
}

// allowRoomCreate rate-limits room creation per source across connections, so
// create-and-disconnect loops cannot fill the room table.
func (s *Server) allowRoomCreate(source string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	window := s.roomCreates[source]
	if window == nil || now.Sub(window.start) >= RoomCreateWindow {
		for key, old := range s.roomCreates {
			if now.Sub(old.start) >= RoomCreateWindow {
				delete(s.roomCreates, key)
			}
		}
		window = &sourceWindow{start: now}
		s.roomCreates[source] = window
	}
	if window.count >= MaxRoomCreatesPerSource {
		return false
	}
	window.count++
	return true
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if s.shuttingDown.Load() {
		http.Error(w, "server restarting", http.StatusServiceUnavailable)
		return
	}
	s.mu.RLock()
	clientCount := len(s.clients)
	s.mu.RUnlock()
	if clientCount >= MaxClients {
		http.Error(w, "server at connection capacity", http.StatusServiceUnavailable)
		return
	}
	source := requestSource(r)
	if !s.acquireSource(source) {
		http.Error(w, "too many connections", http.StatusTooManyRequests)
		return
	}
	sourceOwned := true
	defer func() {
		if sourceOwned {
			s.releaseSource(source)
		}
	}()
	if !s.tryAcquireConnectionSlot() {
		http.Error(w, "server at connection capacity", http.StatusServiceUnavailable)
		return
	}
	slotOwned := true
	defer func() {
		if slotOwned {
			s.releaseConnectionSlot()
		}
	}()

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Warn("WebSocket upgrade error", zap.Error(err))
		return
	}

	// Use Protobuf codec with compression enabled
	client := newClient(s.generateUserID(), conn)

	userAgent := r.Header.Get("User-Agent")
	policy := s.policy()
	tier := policy.resolve(userAgent)
	if err := s.database.recordUserAgent(userAgent); err != nil {
		s.logger.Warn("Failed to record User-Agent", zap.Error(err))
	}
	client.uaTier = tier
	client.policy = policy
	client.source = source
	if tier == uaBlock {
		// Write synchronously so blocked connections cannot create untracked pumps.
		s.logger.Debug("Blocked client", zap.String("client_id", client.clientID()))
		// Uncompressed: the client has not negotiated gzip support yet.
		message, encodeErr := NewMessageCodec(false).Encode(MsgTypeError, ErrorPayload{Code: "blocked_client", Message: BlockedClientMessage})
		if encodeErr != nil {
			s.logger.Error("Failed to encode blocked-client response", zap.Error(encodeErr))
		} else if err := conn.SetWriteDeadline(time.Now().Add(WriteTimeout)); err == nil {
			_ = conn.WriteMessage(websocket.BinaryMessage, message)
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "blocked client"), time.Now().Add(WriteTimeout))
		}
		_ = conn.Close()
		return
	}

	s.mu.Lock()
	// Re-check under the lock so no pump starts after shutdown began draining.
	if len(s.clients) >= MaxClients || s.shuttingDown.Load() {
		s.mu.Unlock()
		_ = conn.Close()
		return
	}
	s.clients[client] = true
	s.pumps.Add(1)
	s.mu.Unlock()

	client.connectionSlot = true
	slotOwned = false
	sourceOwned = false
	go client.writePump(s.logger)
	go client.readPump(s)

	s.logger.Info("Client connected", zap.String("client_id", client.clientID()), zap.String("ua_tier", tier.String()))
}

func (s *Server) handleMessage(c *Client, data []byte, receivedAt int64) {
	// Decode message using protobuf codec
	msgType, payloadBytes, requestID, err := c.codec.DecodeRequest(data)
	c.requestID.Store(requestID)
	c.replied.Store(false)
	defer func() {
		// Commands that succeed silently are acknowledged so capable clients can
		// correlate every request with an outcome.
		if requestID != 0 && !c.replied.Load() && c.hasFeature(FeatureRequestID) {
			c.reply(s.logger, MsgTypeAck, nil)
		}
		c.requestID.Store(0)
	}()
	if err != nil {
		s.logger.Debug("Invalid message received", zap.String("client_id", c.clientID()), zap.Error(err))
		if errors.Is(err, errMessageTooLarge) {
			c.sendError(s.logger, "message_too_large", fmt.Sprintf("Message exceeds %d bytes after decompression", MaxDecodedPayloadSize))
			return
		}
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
		s.handlePing(c, payloadBytes, receivedAt)
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

func (s *Server) handlePing(c *Client, payload []byte, receivedAt int64) {
	p := PingPayload{}
	if len(payload) > 0 {
		if err := decodePayload(payload, MsgTypePing, &p); err != nil {
			c.sendError(s.logger, "invalid_payload", "Invalid ping payload")
			return
		}
	}

	pong := PongPayload{
		ClientTime:        p.ClientTime,
		ServerReceiveTime: receivedAt,
		Sequence:          p.Sequence,
	}
	var snapshot *SyncStatePayload
	if room := c.currentRoom(); room != nil && (p.HasPosition || p.HasStateHash) {
		room.mu.RLock()
		if room.Clients[c.clientID()] == c {
			state := room.State
			if p.HasPosition && room.Host != c && state.CurrentTrack != nil && p.TrackID == state.CurrentTrack.ID {
				capturedAt := p.PositionCapturedAtServerTime
				if capturedAt <= 0 {
					capturedAt = receivedAt
				}
				// Samples far from the server clock come from an uncalibrated client.
				// Samples from before the last host action describe an old timeline.
				if age := receivedAt - capturedAt; capturedAt >= state.LastUpdate && age >= -maxDriftSampleSkew.Milliseconds() && age <= maxDriftSampleSkew.Milliseconds() {
					pong.DriftValid = true
					pong.DriftMs = p.Position - livePlaybackPosition(state, capturedAt)
				}
			}
			// Rickrolled clients see a rewritten queue, so their hash never matches.
			if p.HasStateHash && c.uaTier != uaRickroll && p.StateHash != roomStateHash(state) {
				pong.StateMismatch = true
				if c.allowSyncResponse(time.Now()) {
					snapshot = syncSnapshotLocked(room, serverNowMs())
				}
			}
		}
		room.mu.RUnlock()
	}

	c.queuePong(pong)
	if snapshot != nil {
		c.sendMessage(s.logger, MsgTypeSyncState, *snapshot)
	}
}

// maxDriftSampleSkew bounds how far a drift sample's capture time may be from
// the server clock before it is ignored.
const maxDriftSampleSkew = 10 * time.Second

// roomStateHash is the desync canary: CRC-32 (IEEE) of the current track ID
// and each upcoming queue track ID, each terminated by a newline.
func roomStateHash(state *RoomState) uint32 {
	hash := crc32.NewIEEE()
	if state.CurrentTrack != nil {
		hash.Write([]byte(state.CurrentTrack.ID))
	}
	hash.Write([]byte{'\n'})
	for i := range state.Queue {
		hash.Write([]byte(state.Queue[i].ID))
		hash.Write([]byte{'\n'})
	}
	return hash.Sum32()
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
	c.negotiationMu.Lock()
	if c.affiliationStarted {
		c.negotiationMu.Unlock()
		c.sendError(s.logger, "capabilities_too_late", "Client capabilities must be sent before joining a room")
		return
	}
	if c.capabilitiesSet || c.codec == nil {
		c.negotiationMu.Unlock()
		c.sendError(s.logger, "capabilities_already_set", "Client capabilities have already been configured")
		return
	}
	c.capabilitiesSet = true
	c.codec.setCompressionEnabled(p.SupportsCompression)
	features := c.setFeatures(p.Features)
	c.reply(s.logger, MsgTypeServerCapabilities, ServerCapabilitiesPayload{
		SupportsProtobuf:    true,
		SupportsCompression: true,
		ServerVersion:       "2",
		Features:            features,
		CanHost:             canHostClient(c),
	})
	c.negotiationMu.Unlock()
}

func (s *Server) snapshotClients() []*Client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	clients := make([]*Client, 0, len(s.clients))
	for client := range s.clients {
		clients = append(clients, client)
	}
	return clients
}

// closeAllClients hard-closes every connection.
func (s *Server) closeAllClients() {
	for _, client := range s.snapshotClients() {
		if client == nil || client.Conn == nil {
			continue
		}
		_ = client.Conn.Close()
		client.closeSend()
	}
}

// announceRestart tells capable clients the server is restarting and how long
// to wait before reconnecting.
func (s *Server) announceRestart() {
	s.mu.Lock()
	s.shuttingDown.Store(true)
	s.mu.Unlock()
	notice := ServerRestartingPayload{EtaMs: RestartEta.Milliseconds(), Reason: "Server restarting"}
	for _, client := range s.snapshotClients() {
		if client != nil && client.hasFeature(FeatureServerRestarting) {
			client.sendMessage(s.logger, MsgTypeServerRestarting, notice)
		}
	}
}

// drainClients lets every writer flush its queue and send close 1012 (service
// restart), then waits for read pumps to finish their disconnect cleanup.
// Connections still open at the deadline are closed forcibly.
func (s *Server) drainClients(deadline time.Time) {
	for _, client := range s.snapshotClients() {
		if client != nil {
			client.closeSendWith(websocket.CloseServiceRestart, "server restarting")
		}
	}
	done := make(chan struct{})
	go func() {
		s.pumps.Wait()
		close(done)
	}()
	select {
	case <-done:
		return
	case <-time.After(time.Until(deadline)):
	}
	s.closeAllClients()
	select {
	case <-done:
	case <-time.After(time.Second):
		s.logger.Warn("Read pumps did not finish before shutdown")
	}
}
