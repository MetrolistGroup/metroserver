package server

import (
	"time"

	"go.uber.org/zap"
)

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
