package server

import (
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
)

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
