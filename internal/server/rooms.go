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
	mine := 0
	for _, pending := range room.PendingSuggestions {
		if pending == nil || pending.FromUserID != clientID {
			continue
		}
		if pending.Track != nil && pending.Track.ID == p.TrackInfo.ID {
			room.mu.Unlock()
			c.sendError(s.logger, "duplicate_suggestion", "You already suggested this track")
			return
		}
		mine++
	}
	if mine >= MaxSuggestionsPerUser {
		room.mu.Unlock()
		c.sendError(s.logger, "too_many_suggestions", "You have too many pending suggestions")
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
		Track:        cloneTrackInfo(p.TrackInfo),
	}

	host := room.Host
	hostConnected := host != nil && room.HostDisconnectedAt == nil
	notification := SuggestionReceivedPayload{
		SuggestionID: sugID,
		FromUserID:   clientID,
		FromUsername: username,
		TrackInfo:    cloneTrackInfo(p.TrackInfo),
	}
	room.mu.Unlock()

	if hostConnected {
		host.sendMessage(s.logger, MsgTypeSuggestionReceived, notification)
	}
	if c.hasFeature(FeatureSuggestionAck) {
		c.reply(s.logger, MsgTypeSuggestionSubmitted, SuggestionSubmittedPayload{
			SuggestionID: sugID,
			TrackInfo:    cloneTrackInfo(p.TrackInfo),
		})
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
	room.syncMu.Lock()
	defer room.syncMu.Unlock()
	room.mu.Lock()
	if room.Host == nil || room.Host != c || room.HostDisconnectedAt != nil {
		room.mu.Unlock()
		c.sendError(s.logger, "not_host", "Only the host can approve suggestions")
		return
	}
	suggestion, exists := room.PendingSuggestions[p.SuggestionID]
	if !exists || suggestion == nil {
		room.mu.Unlock()
		c.sendError(s.logger, "suggestion_not_found", "Suggestion not found")
		return
	}

	// Update room state queue: insert next (front of upcoming queue)
	if suggestion.Track != nil {
		if len(room.State.Queue) >= MaxQueueSize {
			room.mu.Unlock()
			c.sendError(s.logger, "queue_full", "Queue is full")
			return
		}
		if (room.State.CurrentTrack != nil && room.State.CurrentTrack.ID == suggestion.Track.ID) ||
			containsTrackID(room.State.Queue, suggestion.Track.ID) {
			room.mu.Unlock()
			c.sendError(s.logger, "duplicate_track", "Track is already playing or queued")
			return
		}
		suggestion.Track.SuggestedBy = suggestion.FromUsername
		queue := append([]TrackInfo{*suggestion.Track}, room.State.Queue...)
		if queueWireSize(queue) > MaxQueueBytes {
			room.mu.Unlock()
			c.sendError(s.logger, "queue_too_large", "Queue is too large")
			return
		}
		room.State.Queue = queue
	}

	// Remove from pending after all validation succeeds.
	delete(room.PendingSuggestions, p.SuggestionID)

	// Broadcast queue add (insert next) so clients apply immediately
	qa := PlaybackActionPayload{
		Action:     ActionQueueAdd,
		TrackInfo:  suggestion.Track,
		InsertNext: true,
		Queue:      append([]TrackInfo(nil), room.State.Queue...),
		ServerTime: serverNowMs(),
	}
	room.State.Revision++
	qa.Revision = room.State.Revision
	clients := roomClientsLocked(room)

	// Notify suggester of approval
	target := room.Clients[suggestion.FromUserID]
	approved := SuggestionApprovedPayload{
		SuggestionID: p.SuggestionID,
		TrackInfo:    cloneTrackInfo(suggestion.Track),
	}
	if target == nil {
		queueNoticeLocked(room, suggestion.FromUserID, MsgTypeSuggestionApproved, approved)
	}
	trackID := ""
	if suggestion.Track != nil {
		trackID = suggestion.Track.ID
	}
	roomCode := room.Code
	room.mu.Unlock()

	sendMessageToClients(s.logger, clients, MsgTypeSyncPlayback, qa)
	if target != nil {
		target.sendMessage(s.logger, MsgTypeSuggestionApproved, approved)
	}

	s.logger.Info("Suggestion approved",
		zap.String("room_code", roomCode),
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
	if room.Host == nil || room.Host != c || room.HostDisconnectedAt != nil {
		room.mu.Unlock()
		c.sendError(s.logger, "not_host", "Only the host can reject suggestions")
		return
	}
	suggestion, exists := room.PendingSuggestions[p.SuggestionID]
	if !exists || suggestion == nil {
		room.mu.Unlock()
		c.sendError(s.logger, "suggestion_not_found", "Suggestion not found")
		return
	}
	delete(room.PendingSuggestions, p.SuggestionID)

	// Notify suggester of rejection
	reason := sanitizeString(p.Reason, 200)
	target := room.Clients[suggestion.FromUserID]
	if target == nil {
		queueNoticeLocked(room, suggestion.FromUserID, MsgTypeSuggestionRejected, SuggestionRejectedPayload{
			SuggestionID: p.SuggestionID,
			Reason:       reason,
		})
	}
	trackID := ""
	if suggestion.Track != nil {
		trackID = suggestion.Track.ID
	}
	roomCode := room.Code
	room.mu.Unlock()

	if target != nil {
		target.sendMessage(s.logger, MsgTypeSuggestionRejected, SuggestionRejectedPayload{
			SuggestionID: p.SuggestionID,
			Reason:       reason,
		})
	}

	s.logger.Info("Suggestion rejected",
		zap.String("room_code", roomCode),
		zap.String("track_id", trackID))
}

func (s *Server) handleCreateRoom(c *Client, payload []byte) {
	if !canHostClient(c) {
		c.sendError(s.logger, "host_not_allowed", "This client is not allowed to host rooms")
		return
	}

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
	if c.currentPendingRoom() != nil {
		c.sendError(s.logger, "already_pending", "Cancel the pending join request before creating a room")
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
	if c.source != "" && !s.allowRoomCreate(c.source, time.Now()) {
		c.sendError(s.logger, "rate_limited", "Too many rooms created; try again in a minute")
		return
	}

	clientID := c.clientID()
	username := p.Username
	sessionToken := s.generateSessionToken()

	room := &Room{
		Code:              code,
		Host:              c,
		Clients:           make(map[string]*Client),
		PendingJoins:      make(map[string]*Client),
		DisconnectedUsers: make(map[string]*Session),
		State: &RoomState{
			RoomCode:   code,
			HostID:     clientID,
			Users:      []UserInfo{{UserID: clientID, Username: username, IsHost: true, IsConnected: true}},
			IsPlaying:  false,
			Position:   0,
			LastUpdate: serverNowMs(),
			Volume:     1.0,
			Queue:      []TrackInfo{},
		},
	}

	room.Clients[clientID] = c
	if !c.trySetRoom(room) {
		c.sendError(s.logger, "already_in_room", "You are already in a room")
		return
	}
	// Commit identity only after claiming the room, so a concurrent approval
	// elsewhere keeps the token it already sent.
	c.setUsername(username)
	c.setSessionToken(sessionToken)

	s.mu.Lock()
	if len(s.rooms) >= MaxRooms {
		s.mu.Unlock()
		c.clearRoom(room)
		c.sendError(s.logger, "room_limit_reached", "Server is at room capacity")
		return
	}
	if _, exists := s.rooms[code]; exists {
		s.mu.Unlock()
		c.clearRoom(room)
		c.sendError(s.logger, "server_error", "Failed to create room")
		return
	}
	s.rooms[code] = room
	s.mu.Unlock()

	s.logger.Info("About to send RoomCreated response",
		zap.String("room_code", code),
		zap.String("client_id", clientID),
		zap.Int("session_token_len", len(sessionToken)))

	c.reply(s.logger, MsgTypeRoomCreated, RoomCreatedPayload{
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
	if c.currentPendingRoom() != nil {
		c.sendError(s.logger, "already_pending", "You already have a pending join request")
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
	if !exists || room == nil {
		s.mu.RUnlock()
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
		s.mu.RUnlock()
		c.sendError(s.logger, "already_in_room", "You are already in this room")
		return
	}

	if _, exists := room.PendingJoins[clientID]; exists {
		room.mu.Unlock()
		s.mu.RUnlock()
		c.sendError(s.logger, "already_pending", "Your join request is already pending")
		return
	}
	if roomMembersLocked(room) >= MaxClientsPerRoom {
		room.mu.Unlock()
		s.mu.RUnlock()
		c.sendError(s.logger, "room_full", "Room is full")
		return
	}
	if len(room.PendingJoins) >= MaxPendingJoins {
		room.mu.Unlock()
		s.mu.RUnlock()
		c.sendError(s.logger, "too_many_pending", "Too many pending join requests")
		return
	}

	// Validate room isn't in an invalid state
	if room.State.HostID == "" {
		room.mu.Unlock()
		s.mu.RUnlock()
		c.sendError(s.logger, "room_invalid", "Room is no longer valid")
		return
	}

	// Add to pending joins
	if !c.trySetPendingRoom(room) {
		room.mu.Unlock()
		s.mu.RUnlock()
		c.sendError(s.logger, "already_pending", "You already have a pending join request")
		return
	}
	room.PendingJoins[clientID] = c
	host := room.Host
	hostConnected := host != nil && room.HostDisconnectedAt == nil
	room.mu.Unlock()
	s.mu.RUnlock()

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

	room.syncMu.Lock()
	defer room.syncMu.Unlock()
	room.mu.Lock()

	if room.Host == nil || room.Host != c || room.HostDisconnectedAt != nil {
		room.mu.Unlock()
		c.sendError(s.logger, "not_host", "Only the host can approve join requests")
		return
	}

	joiningClient, exists := room.PendingJoins[p.UserID]
	if !exists {
		room.mu.Unlock()
		c.sendError(s.logger, "join_request_not_found", "Join request not found")
		return
	}

	// Verify joining client is still valid
	if joiningClient == nil || joiningClient.isClosed() {
		delete(room.PendingJoins, p.UserID)
		if joiningClient != nil {
			joiningClient.clearPendingRoom(room)
		}
		room.mu.Unlock()
		c.sendError(s.logger, "user_disconnected", "User has disconnected")
		return
	}
	if roomMembersLocked(room) >= MaxClientsPerRoom {
		room.mu.Unlock()
		c.sendError(s.logger, "room_full", "Room is full")
		return
	}
	if !joiningClient.trySetRoom(room) {
		delete(room.PendingJoins, p.UserID)
		joiningClient.clearPendingRoom(room)
		room.mu.Unlock()
		c.sendError(s.logger, "user_unavailable", "User already joined another room")
		return
	}

	joiningID := joiningClient.clientID()
	joiningUsername := joiningClient.userName()
	joiningToken := s.generateSessionToken()

	// Remove from pending and add to room
	delete(room.PendingJoins, p.UserID)
	joiningClient.clearPendingRoom(room)
	room.Clients[joiningID] = joiningClient
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

	nowMs := serverNowMs()
	approval := JoinApprovedPayload{
		RoomCode:     room.Code,
		UserID:       joiningID,
		SessionToken: joiningToken,
		State:        cloneLiveRoomState(room.State, nowMs),
	}

	clientsToNotify := make([]*Client, 0, len(room.Clients)-1)
	for _, client := range room.Clients {
		if client != nil && client.clientID() != joiningID {
			clientsToNotify = append(clientsToNotify, client)
		}
	}
	room.mu.Unlock()

	joiningClient.sendMessage(s.logger, MsgTypeJoinApproved, approval)
	s.sendPolicyQueueSnapshot(joiningClient, approval.State)
	sendMessageToClients(s.logger, clientsToNotify, MsgTypeUserJoined, UserJoinedPayload{
		UserID: joiningID, Username: joiningUsername,
	})

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

	reason := sanitizeString(p.Reason, 200)
	if reason == "" {
		reason = "Join request rejected by host"
	}

	// Queue the rejection before releasing the affiliation so it cannot be
	// mistaken for a reply to the applicant's next join attempt.
	joiningClient.sendMessage(s.logger, MsgTypeJoinRejected, JoinRejectedPayload{
		Reason: reason,
	})
	delete(room.PendingJoins, p.UserID)
	joiningClient.clearPendingRoom(room)

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
	room.syncMu.Lock()
	defer room.syncMu.Unlock()
	s.mu.Lock()
	room.mu.Lock()

	if room.Host == nil || room.Host != c || room.HostDisconnectedAt != nil {
		room.mu.Unlock()
		s.mu.Unlock()
		c.sendError(s.logger, "not_host", "Only the host can kick users")
		return
	}

	if p.UserID == clientID {
		room.mu.Unlock()
		s.mu.Unlock()
		c.sendError(s.logger, "cannot_kick_self", "You cannot kick yourself")
		return
	}

	targetClient := room.Clients[p.UserID]
	disconnectedSession := room.DisconnectedUsers[p.UserID]
	if targetClient == nil && disconnectedSession == nil {
		room.mu.Unlock()
		s.mu.Unlock()
		c.sendError(s.logger, "user_not_found", "User not found in room")
		return
	}

	reason := sanitizeString(p.Reason, 200)
	if reason == "" {
		reason = "You have been kicked from the room"
	}

	kickedUsername := ""
	if targetClient != nil {
		kickedUsername = targetClient.userName()
		// Queue the notice before releasing the membership so it cannot follow
		// the kicked client into another room.
		targetClient.sendMessage(s.logger, MsgTypeKicked, KickedPayload{Reason: reason})
		targetClient.clearRoom(room)
		delete(room.Clients, p.UserID)
	} else {
		// Revoke an offline member too, or they could return without approval.
		kickedUsername = disconnectedSession.Username
		delete(room.DisconnectedUsers, p.UserID)
		for token, session := range s.sessions {
			if session != nil && session.RoomCode == room.Code && session.UserID == p.UserID {
				delete(s.sessions, token)
			}
		}
	}
	delete(room.PendingNotices, p.UserID)
	removeUserSuggestionsLocked(room, p.UserID)
	if room.OwnerID == p.UserID {
		room.OwnerID = ""
	}
	room.State.Users = withoutUser(room.State.Users, p.UserID)
	clientsToNotify := roomClientsLocked(room)
	room.mu.Unlock()
	s.mu.Unlock()

	sendMessageToClients(s.logger, clientsToNotify, MsgTypeUserLeft, UserLeftPayload{
		UserID:   p.UserID,
		Username: kickedUsername,
	})

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

	room.syncMu.Lock()
	defer room.syncMu.Unlock()
	room.mu.Lock()

	// Only current host can transfer ownership
	if room.Host == nil || room.Host != c || room.HostDisconnectedAt != nil {
		room.mu.Unlock()
		c.sendError(s.logger, "not_host", "Only the host can transfer ownership")
		return
	}

	// Cannot transfer to self
	if p.NewHostID == c.clientID() {
		room.mu.Unlock()
		c.sendError(s.logger, "cannot_transfer_to_self", "You are already the host")
		return
	}

	newHostClient, exists := room.Clients[p.NewHostID]
	if !exists || newHostClient == nil || newHostClient.isClosed() {
		room.mu.Unlock()
		c.sendError(s.logger, "user_not_found", "Target user not found in room")
		return
	}
	if !canHostClient(newHostClient) {
		room.mu.Unlock()
		c.sendError(s.logger, "host_not_allowed", "That user's client is not allowed to host rooms")
		return
	}

	oldHostName := c.userName()
	// An explicit hand-off replaces any owner waiting to reclaim after failover.
	room.OwnerID = ""
	change := assignHostLocked(room, newHostClient)
	clients := roomClientsLocked(room)
	room.mu.Unlock()

	s.deliverHostChange(clients, change)

	s.logger.Info("Host transferred",
		zap.String("room_code", room.Code),
		zap.String("old_host", oldHostName),
		zap.String("new_host", newHostClient.userName()))
}

func (s *Server) leaveRoom(c *Client) {
	room := c.currentRoom()
	if room == nil {
		s.removePendingJoin(c)
		if c.currentRoom() == nil {
			// A fresh ID makes a delayed approval of the cancelled request miss.
			c.setClientID(s.generateUserID())
			return
		}
		// Approval won the race with the cancellation; leave the room instead.
		room = c.currentRoom()
	}

	clientID := c.clientID()
	username := c.userName()
	sessionToken := c.session()
	room.syncMu.Lock()
	defer room.syncMu.Unlock()
	room.mu.Lock()
	if room.Clients[clientID] != c {
		room.mu.Unlock()
		c.clearRoom(room)
		return
	}

	delete(room.Clients, clientID)
	delete(room.PendingJoins, clientID)
	delete(room.DisconnectedUsers, clientID)
	delete(room.PendingNotices, clientID)
	removeUserSuggestionsLocked(room, clientID)
	if room.OwnerID == clientID {
		room.OwnerID = ""
	}

	wasHost := room.Host == c
	room.State.Users = withoutUser(room.State.Users, clientID)

	c.clearRoom(room)

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
		s.deleteRoomIfEmpty(room)
		s.logger.Info("Room became empty",
			zap.String("room_code", room.Code))
		return
	}

	var change *hostChange
	if wasHost {
		change = assignHostLocked(room, pickDeputy(room.Clients))
	}
	dead := hostlessDeadLocked(room)
	clientsToNotify := roomClientsLocked(room)
	room.mu.Unlock()
	if sessionToken != "" {
		s.mu.Lock()
		delete(s.sessions, sessionToken)
		s.mu.Unlock()
	}

	sendMessageToClients(s.logger, clientsToNotify, MsgTypeUserLeft, UserLeftPayload{
		UserID:   clientID,
		Username: username,
	})
	s.deliverHostChange(clientsToNotify, change)
	if dead {
		s.closeRoom(room, noHostLeftReason)
	}

	s.logger.Info("User left room",
		zap.String("username", username),
		zap.String("user_id", clientID),
		zap.String("room_code", room.Code),
		zap.Bool("was_host", wasHost))
}

// roomMembersLocked counts active members plus disconnected members whose
// seats are reserved for reconnection.
func roomMembersLocked(room *Room) int {
	return len(room.Clients) + len(room.DisconnectedUsers)
}

func removeUserSuggestionsLocked(room *Room, userID string) {
	for id, suggestion := range room.PendingSuggestions {
		if suggestion != nil && suggestion.FromUserID == userID {
			delete(room.PendingSuggestions, id)
		}
	}
}

// queueNoticeLocked keeps a message for a disconnected member until they reconnect.
func queueNoticeLocked(room *Room, userID, msgType string, payload interface{}) {
	if _, away := room.DisconnectedUsers[userID]; !away {
		return
	}
	if room.PendingNotices == nil {
		room.PendingNotices = make(map[string][]pendingNotice)
	}
	if len(room.PendingNotices[userID]) < MaxSuggestionsPerUser {
		room.PendingNotices[userID] = append(room.PendingNotices[userID], pendingNotice{MsgType: msgType, Payload: payload})
	}
}
