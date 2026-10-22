package server

import (
	"time"

	"go.uber.org/zap"
)

func (s *Server) cleanupExpiredSessions() {
	sessions := time.NewTicker(SessionCleanupInterval)
	failover := time.NewTicker(HostFailoverCheckInterval)
	defer sessions.Stop()
	defer failover.Stop()

	for {
		select {
		case <-s.stop:
			return
		case now := <-sessions.C:
			s.cleanupExpiredSessionsOnce(now)
		case now := <-failover.C:
			s.failoverHostsOnce(now)
		}
	}
}

func (s *Server) cleanupExpiredSessionsOnce(now time.Time) {
	minRetentionTime := s.startTime.Add(MinRoomRetentionAfterRestart)

	// First, determine which sessions have expired without holding any room locks.
	s.mu.Lock()
	expired := make([]*Session, 0)
	for token, session := range s.sessions {
		if session == nil {
			delete(s.sessions, token)
			continue
		}
		if now.Sub(session.DisconnectAt) > ReconnectGracePeriod {
			expired = append(expired, session)
			delete(s.sessions, token)
			s.logger.Info("Session expired",
				zap.String("user_id", session.UserID),
				zap.String("room_code", session.RoomCode))
		}
	}
	s.mu.Unlock()

	for _, session := range expired {
		s.mu.RLock()
		room, exists := s.rooms[session.RoomCode]
		s.mu.RUnlock()
		if !exists || room == nil {
			continue
		}
		s.expireSession(room, session, now.After(minRetentionTime))
	}
}

// expireSession permanently removes a disconnected user whose grace period ran out.
func (s *Server) expireSession(room *Room, session *Session, pastRetention bool) {
	room.syncMu.Lock()
	defer room.syncMu.Unlock()
	room.mu.Lock()

	if _, ok := room.DisconnectedUsers[session.UserID]; !ok {
		// The user reconnected or left after we took the token.
		room.mu.Unlock()
		return
	}
	delete(room.DisconnectedUsers, session.UserID)
	delete(room.PendingNotices, session.UserID)
	removeUserSuggestionsLocked(room, session.UserID)
	if room.OwnerID == session.UserID {
		room.OwnerID = ""
	}
	room.State.Users = withoutUser(room.State.Users, session.UserID)

	var change *hostChange
	if room.State.HostID == session.UserID {
		change = assignHostLocked(room, pickDeputy(room.Clients))
	}

	shouldDeleteRoom := len(room.Clients) == 0 && len(room.DisconnectedUsers) == 0 && pastRetention
	dead := hostlessDeadLocked(room)
	remainingClients := roomClientsLocked(room)
	room.mu.Unlock()

	if shouldDeleteRoom {
		if s.deleteRoomIfEmpty(room) {
			s.logger.Info("Deleted empty room", zap.String("room_code", room.Code))
		}
		return
	}
	sendMessageToClients(s.logger, remainingClients, MsgTypeUserLeft, UserLeftPayload{
		UserID:   session.UserID,
		Username: session.Username,
	})
	s.deliverHostChange(remainingClients, change)
	if dead {
		s.closeRoom(room, noHostLeftReason)
	}
}

func withoutUser(users []UserInfo, userID string) []UserInfo {
	kept := make([]UserInfo, 0, len(users))
	for _, u := range users {
		if u.UserID != userID {
			kept = append(kept, u)
		}
	}
	return kept
}

func (s *Server) cleanupEmptyRooms() {
	// Wait before first cleanup to avoid deleting rooms during startup
	select {
	case <-s.stop:
		return
	case <-time.After(EmptyRoomCleanupTimeout):
	}

	ticker := time.NewTicker(EmptyRoomCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stop:
			return
		case now := <-ticker.C:
			s.cleanupEmptyRoomsOnce(now)
		}
	}
}

func (s *Server) cleanupEmptyRoomsOnce(now time.Time) {
	minRetentionTime := s.startTime.Add(MinRoomRetentionAfterRestart)

	s.mu.RLock()
	rooms := make(map[string]*Room, len(s.rooms))
	for roomCode, room := range s.rooms {
		rooms[roomCode] = room
	}
	s.mu.RUnlock()

	for roomCode, room := range rooms {
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
			if s.deleteRoomIfEmpty(room) {
				s.logger.Info("Deleted empty room after inactivity",
					zap.String("room_code", roomCode),
					zap.Duration("empty_for", now.Sub(*emptySince)))
			}
		}
	}
}

func (s *Server) removeClient(c *Client) {
	s.mu.Lock()
	delete(s.clients, c)
	s.mu.Unlock()
	c.closeSend()

	if c.currentRoom() != nil {
		s.handleClientDisconnect(c)
	} else {
		s.removePendingJoin(c)
		if c.currentRoom() != nil {
			s.handleClientDisconnect(c)
		}
	}

	s.logger.Info("Client disconnected", zap.String("client_id", c.clientID()))
}

func (s *Server) removePendingJoin(c *Client) {
	room := c.currentPendingRoom()
	if room == nil {
		return
	}
	clientID := c.clientID()
	room.mu.Lock()
	if pending, exists := room.PendingJoins[clientID]; exists && pending == c {
		delete(room.PendingJoins, clientID)
	}
	c.clearPendingRoom(room)
	room.mu.Unlock()
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

	// syncMu keeps this presence change ordered with snapshots and broadcasts.
	room.syncMu.Lock()
	defer room.syncMu.Unlock()
	s.mu.Lock()
	room.mu.Lock()
	if room.Clients[clientID] != c {
		room.mu.Unlock()
		s.mu.Unlock()
		c.clearRoom(room)
		return
	}

	wasHost := room.Host == c
	now := time.Now()

	// Create session for reconnection
	session := &Session{
		UserID:       clientID,
		Username:     username,
		RoomCode:     room.Code,
		IsHost:       wasHost,
		DisconnectAt: now,
	}

	// Remove from active clients but add to disconnected users
	delete(room.Clients, clientID)

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

	if wasHost {
		// HostID keeps the seat; dropping the pointer releases the old
		// connection's buffers while the host is away.
		room.Host = nil
		room.HostDisconnectedAt = &now
	}

	c.clearRoom(room)
	clientsToNotify := roomClientsLocked(room)

	room.EmptySince = nil
	s.sessions[sessionToken] = session
	room.mu.Unlock()
	s.mu.Unlock()

	sendMessageToClients(s.logger, clientsToNotify, MsgTypeUserDisconnected, UserDisconnectedPayload{
		UserID:   clientID,
		Username: username,
	})

	s.logger.Info("User temporarily disconnected",
		zap.String("username", username),
		zap.String("user_id", clientID),
		zap.String("room_code", room.Code),
		zap.Bool("was_host", wasHost))
}

// replaceLiveSession detaches another connection that still holds token, so a
// client that switched networks can resume before the old socket times out.
// It reports whether a connection was replaced.
func (s *Server) replaceLiveSession(c *Client, token string) bool {
	var old *Client
	s.mu.RLock()
	for client := range s.clients {
		if client != c && client.session() == token && client.currentRoom() != nil {
			old = client
			break
		}
	}
	s.mu.RUnlock()
	if old == nil {
		return false
	}
	s.handleClientDisconnect(old)
	old.closeSend()
	if old.Conn != nil {
		_ = old.Conn.Close()
	}
	s.logger.Info("Replaced live connection on reconnect", zap.String("user_id", old.clientID()))
	return true
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
	if c.currentPendingRoom() != nil {
		c.sendError(s.logger, "already_pending", "Cancel the pending join request before reconnecting")
		return
	}

	// claim marks the session as taken so concurrent attempts fail fast; the
	// token itself is only deleted together with the membership change.
	claim := func() (*Session, string) {
		s.mu.Lock()
		defer s.mu.Unlock()
		session, exists := s.sessions[p.SessionToken]
		switch {
		case !exists || (session != nil && session.claimed):
			return nil, "session_not_found"
		case session == nil || time.Since(session.DisconnectAt) > ReconnectGracePeriod:
			// Expired sessions stay indexed so cleanup can remove the user and
			// elect a new host; deleting the token here would orphan them.
			return nil, "session_expired"
		case session.IsHost && !canHostClient(c):
			return nil, "host_not_allowed"
		}
		session.claimed = true
		return session, ""
	}
	session, failure := claim()
	if failure == "session_not_found" && s.replaceLiveSession(c, p.SessionToken) {
		session, failure = claim()
	}
	switch failure {
	case "session_not_found":
		c.sendError(s.logger, failure, "Session not found or expired")
		return
	case "session_expired":
		c.sendError(s.logger, failure, "Session has expired")
		return
	case "host_not_allowed":
		c.sendError(s.logger, failure, "This client is not allowed to host rooms")
		return
	}
	consumed := false
	defer func() {
		if !consumed {
			s.mu.Lock()
			session.claimed = false
			s.mu.Unlock()
		}
	}()

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

	room.syncMu.Lock()
	defer room.syncMu.Unlock()
	s.mu.Lock()
	room.mu.Lock()
	disconnectedSession, disconnected := room.DisconnectedUsers[session.UserID]
	if s.sessions[p.SessionToken] != session || !disconnected || disconnectedSession == nil || room.Clients[session.UserID] != nil {
		room.mu.Unlock()
		s.mu.Unlock()
		c.sendError(s.logger, "session_not_found", "Session is no longer reconnectable")
		return
	}
	if !c.trySetRoom(room) {
		room.mu.Unlock()
		s.mu.Unlock()
		c.sendError(s.logger, "already_in_room", "Leave the current room before reconnecting")
		return
	}

	// Restore the client. The token is consumed in the same critical section so
	// a state snapshot sees either the session or the active member, never neither.
	delete(s.sessions, p.SessionToken)
	consumed = true
	c.setClientID(session.UserID)
	c.setUsername(session.Username)
	c.setSessionToken(p.SessionToken)

	room.Clients[session.UserID] = c
	delete(room.DisconnectedUsers, session.UserID)
	room.EmptySince = nil

	for i := range room.State.Users {
		if room.State.Users[i].UserID == session.UserID {
			room.State.Users[i].IsConnected = true
			break
		}
	}

	// The seat is still theirs, failover gave it to a deputy they now take back,
	// or the room has no host at all.
	var change *hostChange
	reclaim := room.State.HostID == session.UserID || room.OwnerID == session.UserID
	if canHostClient(c) && (reclaim || (room.Host == nil && room.State.HostID == "")) {
		change = assignHostLocked(room, c)
		if room.OwnerID == session.UserID {
			room.OwnerID = ""
		}
	}

	liveState := cloneLiveRoomState(room.State, serverNowMs())
	isHost := room.Host == c
	notices := room.PendingNotices[session.UserID]
	delete(room.PendingNotices, session.UserID)
	others := make([]*Client, 0, len(room.Clients))
	for _, client := range room.Clients {
		if client != nil && client != c {
			others = append(others, client)
		}
	}
	room.mu.Unlock()
	s.mu.Unlock()

	c.reply(s.logger, MsgTypeReconnected, ReconnectedPayload{
		RoomCode: room.Code,
		UserID:   c.clientID(),
		State:    liveState,
		IsHost:   isHost,
	})
	s.sendPolicyQueueSnapshot(c, liveState)
	for _, notice := range notices {
		c.sendMessage(s.logger, notice.MsgType, notice.Payload)
	}

	sendMessageToClients(s.logger, others, MsgTypeUserReconnected, UserReconnectedPayload{
		UserID:   c.clientID(),
		Username: c.userName(),
	})
	if change != nil {
		if change.announce != nil {
			sendMessageToClients(s.logger, others, MsgTypeHostChanged, *change.announce)
		}
		s.replayHostWork(c, change.joins, change.suggestions)
	}

	s.logger.Info("User reconnected",
		zap.String("username", c.userName()),
		zap.String("user_id", c.clientID()),
		zap.String("room_code", room.Code),
		zap.Bool("is_host", isHost))
}

func (s *Server) deleteRoomIfEmpty(room *Room) bool {
	s.mu.Lock()
	currentRoom, exists := s.rooms[room.Code]
	if !exists || currentRoom != room {
		s.mu.Unlock()
		return false
	}

	room.mu.Lock()
	if len(room.Clients) != 0 || len(room.DisconnectedUsers) != 0 {
		room.mu.Unlock()
		s.mu.Unlock()
		return false
	}

	for _, client := range room.PendingJoins {
		if client != nil {
			// Queue the rejection before releasing the affiliation so it cannot
			// arrive after the client's next join attempt.
			client.sendMessage(s.logger, MsgTypeJoinRejected, JoinRejectedPayload{Reason: "Room is no longer available"})
			client.clearPendingRoom(room)
		}
	}
	room.PendingJoins = make(map[string]*Client)
	delete(s.rooms, room.Code)
	room.mu.Unlock()
	s.mu.Unlock()
	return true
}
