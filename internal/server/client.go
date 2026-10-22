package server

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	// MaxQueuedBytes bounds one client's encoded outbound backlog.
	MaxQueuedBytes = 8 * 1024 * 1024
	pongQueueSize  = 8
)

type queuedPong struct {
	payload   PongPayload
	requestID uint64
}

type Client struct {
	id                 atomic.Value // string
	username           atomic.Value // string
	sessionToken       atomic.Value // string
	room               atomic.Pointer[Room]
	pendingRoom        atomic.Pointer[Room]
	capabilitiesSet    bool
	affiliationStarted bool
	Conn               *websocket.Conn
	Send               chan []byte
	closed             bool
	rateWindowStart    time.Time
	rateMessageCount   int
	lastSyncResponse   time.Time
	uaTier             uaTier
	policy             *uaPolicy
	connectionSlot     bool
	mu                 sync.Mutex
	negotiationMu      sync.Mutex
	codec              *MessageCodec // Message codec for encoding/decoding
	pongs              chan queuedPong
	queuedBytes        int
	rateLimitNotified  bool
	closeCode          int
	closeReason        string
	source             string
	features           atomic.Pointer[map[string]bool]
	requestID          atomic.Uint64 // envelope request ID of the message being handled
	replied            atomic.Bool   // a reply or error was sent for requestID
	lastPingSent       atomic.Int64  // unix nanos of the last control PING
	rttMs              atomic.Int64  // last control PING round trip; 0 = unknown
}

func newClient(id string, conn *websocket.Conn) *Client {
	c := &Client{
		Conn:  conn,
		Send:  make(chan []byte, 256),
		pongs: make(chan queuedPong, pongQueueSize),
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
	if room != nil {
		c.markAffiliationStarted()
	}
	c.room.Store(room)
}

func (c *Client) trySetRoom(room *Room) bool {
	if room == nil {
		return false
	}
	c.markAffiliationStarted()
	return c.room.CompareAndSwap(nil, room)
}

func (c *Client) clearRoom(room *Room) bool {
	return room != nil && c.room.CompareAndSwap(room, nil)
}

func (c *Client) currentPendingRoom() *Room {
	return c.pendingRoom.Load()
}

func (c *Client) trySetPendingRoom(room *Room) bool {
	if room == nil || c.currentRoom() != nil {
		return false
	}
	c.markAffiliationStarted()
	if !c.pendingRoom.CompareAndSwap(nil, room) {
		return false
	}
	if c.currentRoom() != nil {
		c.pendingRoom.CompareAndSwap(room, nil)
		return false
	}
	return true
}

func (c *Client) clearPendingRoom(room *Room) bool {
	return room != nil && c.pendingRoom.CompareAndSwap(room, nil)
}

func (c *Client) markAffiliationStarted() {
	c.negotiationMu.Lock()
	c.affiliationStarted = true
	c.negotiationMu.Unlock()
}

func (c *Client) hasFeature(name string) bool {
	features := c.features.Load()
	return features != nil && (*features)[name]
}

func (c *Client) setFeatures(names []string) []string {
	enabled := make(map[string]bool)
	accepted := make([]string, 0, len(names))
	for _, name := range names {
		for _, supported := range serverFeatures {
			if name == supported && !enabled[name] {
				enabled[name] = true
				accepted = append(accepted, name)
			}
		}
	}
	c.features.Store(&enabled)
	return accepted
}

// roundTripMs is the last measured WebSocket PING round trip, or 0 if unknown.
func (c *Client) roundTripMs() int64 {
	return c.rttMs.Load()
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *Client) closeSend() {
	c.closeSendWith(0, "")
}

// closeSendWith stops accepting messages; the writer drains what is queued and
// then sends a close frame with code (if non-zero).
func (c *Client) closeSendWith(code int, reason string) {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		c.closeCode = code
		c.closeReason = reason
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
	c.rateLimitNotified = false
	return true
}

// shouldNotifyRateLimit reports whether this window's rate_limited error is
// still unsent, so flooding cannot turn into one response per message.
func (c *Client) shouldNotifyRateLimit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rateLimitNotified {
		return false
	}
	c.rateLimitNotified = true
	return true
}

func (c *Client) allowSyncResponse(now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.Send == nil || len(c.Send) >= cap(c.Send) {
		return false
	}
	if !c.lastSyncResponse.IsZero() && now.Sub(c.lastSyncResponse) < SyncResponseInterval {
		return false
	}
	c.lastSyncResponse = now
	return true
}

func (c *Client) writePump(logger *zap.Logger) {
	ticker := time.NewTicker(PingInterval)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case pong := <-c.pongs:
			if !c.writePong(logger, pong) {
				return
			}
		case <-ticker.C:
			if !c.writePing() {
				return
			}
		case message, ok := <-c.Send:
			// Heartbeats and clock samples must not wait behind queued data.
			if !c.writePriority(logger, ticker) {
				return
			}
			if err := c.Conn.SetWriteDeadline(time.Now().Add(WriteTimeout)); err != nil {
				logger.Debug("Failed to set write deadline", zap.String("client_id", c.clientID()), zap.Error(err))
				return
			}
			if !ok {
				c.mu.Lock()
				code, reason := c.closeCode, c.closeReason
				c.mu.Unlock()
				frame := []byte{}
				if code != 0 {
					frame = websocket.FormatCloseMessage(code, reason)
				}
				c.Conn.WriteMessage(websocket.CloseMessage, frame)
				return
			}
			c.mu.Lock()
			c.queuedBytes -= len(message)
			c.mu.Unlock()

			if err := c.Conn.WriteMessage(websocket.BinaryMessage, message); err != nil {
				logger.Debug("Write error for client", zap.String("client_id", c.clientID()), zap.Error(err))
				return
			}
		}
	}
}

func (c *Client) writePriority(logger *zap.Logger, ticker *time.Ticker) bool {
	for {
		select {
		case pong := <-c.pongs:
			if !c.writePong(logger, pong) {
				return false
			}
		case <-ticker.C:
			if !c.writePing() {
				return false
			}
		default:
			return true
		}
	}
}

func (c *Client) writePing() bool {
	if err := c.Conn.SetWriteDeadline(time.Now().Add(WriteTimeout)); err != nil {
		return false
	}
	c.lastPingSent.Store(time.Now().UnixNano())
	return c.Conn.WriteMessage(websocket.PingMessage, nil) == nil
}

// writePong stamps server_send_time immediately before writing so queueing is
// not mistaken for network delay by the client's clock estimator.
func (c *Client) writePong(logger *zap.Logger, pong queuedPong) bool {
	pong.payload.ServerSendTime = serverNowMs()
	data, err := c.codec.EncodeReply(MsgTypePong, pong.payload, pong.requestID)
	if err != nil {
		logger.Error("Error encoding pong", zap.Error(err))
		return true
	}
	if err := c.Conn.SetWriteDeadline(time.Now().Add(WriteTimeout)); err != nil {
		return false
	}
	return c.Conn.WriteMessage(websocket.BinaryMessage, data) == nil
}

// queuePong hands a pong to the writer. Dropping one when the small priority
// queue is full is harmless: clients treat pongs as samples.
func (c *Client) queuePong(payload PongPayload) {
	c.replied.Store(true)
	if c.pongs == nil {
		return
	}
	select {
	case c.pongs <- queuedPong{payload: payload, requestID: c.requestID.Load()}:
	default:
	}
}

func (c *Client) readPump(s *Server) {
	defer func() {
		s.removeClient(c)
		c.Conn.Close()
		if c.connectionSlot {
			s.releaseConnectionSlot()
			s.releaseSource(c.source)
			s.pumps.Done()
		}
	}()

	c.Conn.SetReadLimit(MaxReadMessageSize)
	if err := c.Conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
		s.logger.Debug("Failed to set read deadline", zap.String("client_id", c.clientID()), zap.Error(err))
	}
	c.Conn.SetPongHandler(func(string) error {
		if sent := c.lastPingSent.Load(); sent > 0 {
			if rtt := (time.Now().UnixNano() - sent) / int64(time.Millisecond); rtt >= 0 {
				c.rttMs.Store(max(rtt, 1))
			}
		}
		if err := c.Conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
			s.logger.Debug("Failed to set read deadline in pong handler", zap.String("client_id", c.clientID()), zap.Error(err))
		}
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		receivedAt := serverNowMs()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				s.logger.Debug("Read error for client", zap.String("client_id", c.clientID()), zap.Error(err))
			}
			break
		}

		if !c.allowMessage(time.Now()) {
			if c.shouldNotifyRateLimit() {
				c.sendError(s.logger, "rate_limited", "Too many messages")
			}
			continue
		}

		if err := c.Conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
			s.logger.Debug("Failed to refresh read deadline", zap.String("client_id", c.clientID()), zap.Error(err))
			break
		}
		s.handleMessage(c, message, receivedAt)
	}
}

// rewritePayload applies this client's User-Agent tier to an outgoing payload.
// uaTier and policy are written before the client's pumps start and read-only
// afterwards.
func (c *Client) rewritePayload(payload interface{}) interface{} {
	if c.policy == nil {
		return payload
	}
	return rewriteForUATier(c.uaTier, c.policy.advertTitle, payload)
}

func (c *Client) sendMessage(logger *zap.Logger, msgType string, payload interface{}) {
	if c == nil || c.codec == nil || c.Send == nil {
		return
	}
	payload = c.rewritePayload(payload)

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

	c.sendEncodedMessage(logger, msgType, msgData)
}

// reply sends a direct response to the message currently being handled,
// echoing its envelope request ID. Only call it from that message's handler.
func (c *Client) reply(logger *zap.Logger, msgType string, payload interface{}) {
	if c == nil || c.codec == nil || c.Send == nil {
		return
	}
	c.replied.Store(true)
	msgData, err := c.codec.EncodeReply(msgType, c.rewritePayload(payload), c.requestID.Load())
	if err != nil {
		logger.Error("Error encoding reply", zap.String("message_type", msgType), zap.Error(err))
		return
	}
	c.sendEncodedMessage(logger, msgType, msgData)
}

func (c *Client) sendEncodedMessage(logger *zap.Logger, msgType string, msgData []byte) {
	if c == nil || c.Send == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		logger.Debug("Attempted to send to closed client", zap.String("client_id", c.clientID()))
		return
	}

	if c.queuedBytes+len(msgData) <= MaxQueuedBytes {
		select {
		case c.Send <- msgData:
			c.queuedBytes += len(msgData)
			logger.Debug("Message queued for sending", zap.String("message_type", msgType), zap.Int("size", len(msgData)))
			return
		default:
		}
	}
	// Dropping messages would leave the client permanently out of sync; drop the
	// connection instead so it reconnects and receives a fresh snapshot.
	logger.Warn("Client send buffer full; closing slow client", zap.String("client_id", c.clientID()))
	c.closed = true
	close(c.Send)
	if c.Conn != nil {
		_ = c.Conn.Close()
	}
}

func sendMessageToClients(logger *zap.Logger, clients []*Client, msgType string, payload interface{}) {
	if len(clients) == 0 {
		return
	}

	// Encode once per (tier, compression) combination: rewritten payloads must
	// not leak into the bytes other recipients get.
	var encoded [uaTierCount][2][]byte
	var encodedReady [uaTierCount][2]bool
	for _, client := range clients {
		if client == nil {
			continue
		}
		compressionIndex := 0
		if client.codec != nil && client.codec.compressionEnabled.Load() {
			compressionIndex = 1
		}
		if !encodedReady[client.uaTier][compressionIndex] {
			msgData, err := NewMessageCodec(compressionIndex == 1).Encode(msgType, client.rewritePayload(payload))
			if err != nil {
				logger.Error("Error encoding broadcast", zap.String("message_type", msgType), zap.Error(err))
				return
			}
			encoded[client.uaTier][compressionIndex] = msgData
			encodedReady[client.uaTier][compressionIndex] = true
		}
		client.sendEncodedMessage(logger, msgType, encoded[client.uaTier][compressionIndex])
	}
}

func (c *Client) sendError(logger *zap.Logger, code, message string) {
	c.reply(logger, MsgTypeError, ErrorPayload{
		Code:    code,
		Message: message,
	})
}
