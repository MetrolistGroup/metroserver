package server

import (
	"time"

	"go.uber.org/zap"
)

// Lock order everywhere: room.syncMu, then s.mu, then room.mu.

// hostChange is everything that must be delivered after a host change, once
// the room lock is released (callers still hold room.syncMu).
type hostChange struct {
	host        *Client
	announce    *HostChangedPayload // nil when HostID did not change
	joins       []JoinRequestPayload
	suggestions []SuggestionReceivedPayload
}

// assignHostLocked makes host the room host and collects the pending work the
// new host has never seen. Caller holds room.mu.
func assignHostLocked(room *Room, host *Client) *hostChange {
	previousID := room.State.HostID
	room.Host = host
	room.HostDisconnectedAt = nil
	room.LastHostSequence = 0
	newID := ""
	if host != nil {
		newID = host.clientID()
	}
	room.State.HostID = newID
	for i := range room.State.Users {
		room.State.Users[i].IsHost = newID != "" && room.State.Users[i].UserID == newID
	}
	change := &hostChange{host: host}
	if newID != "" && newID != previousID {
		change.announce = &HostChangedPayload{NewHostID: newID, NewHostName: host.userName()}
	}
	if host != nil {
		change.joins, change.suggestions = pendingHostWorkLocked(room)
	}
	return change
}

func pendingHostWorkLocked(room *Room) ([]JoinRequestPayload, []SuggestionReceivedPayload) {
	joins := make([]JoinRequestPayload, 0, len(room.PendingJoins))
	for _, pendingClient := range room.PendingJoins {
		if pendingClient == nil || pendingClient.isClosed() {
			continue
		}
		joins = append(joins, JoinRequestPayload{UserID: pendingClient.clientID(), Username: pendingClient.userName()})
	}
	suggestions := make([]SuggestionReceivedPayload, 0, len(room.PendingSuggestions))
	for _, suggestion := range room.PendingSuggestions {
		if suggestion == nil {
			continue
		}
		suggestions = append(suggestions, SuggestionReceivedPayload{
			SuggestionID: suggestion.ID,
			FromUserID:   suggestion.FromUserID,
			FromUsername: suggestion.FromUsername,
			TrackInfo:    cloneTrackInfo(suggestion.Track),
		})
	}
	return joins, suggestions
}

// deliver announces the change to clients and replays pending work to the new
// host. Call without room.mu, while still holding room.syncMu.
func (s *Server) deliverHostChange(clients []*Client, change *hostChange) {
	if change == nil {
		return
	}
	if change.announce != nil {
		sendMessageToClients(s.logger, clients, MsgTypeHostChanged, *change.announce)
	}
	s.replayHostWork(change.host, change.joins, change.suggestions)
}

func (s *Server) replayHostWork(host *Client, joins []JoinRequestPayload, suggestions []SuggestionReceivedPayload) {
	if host == nil {
		return
	}
	for _, join := range joins {
		host.sendMessage(s.logger, MsgTypeJoinRequest, join)
	}
	for _, suggestion := range suggestions {
		host.sendMessage(s.logger, MsgTypeSuggestionReceived, suggestion)
	}
}

// pickDeputy chooses the eligible connected client with the lowest measured
// round trip, so control lands on the best-connected listener. Clients
// without a measurement rank last; ties break on user ID for determinism.
func pickDeputy(clients map[string]*Client) *Client {
	var best *Client
	var bestRTT int64
	var bestID string
	for id, client := range clients {
		if !canHostClient(client) || client.isClosed() {
			continue
		}
		rtt := client.roundTripMs()
		if rtt <= 0 {
			rtt = int64(time.Hour / time.Millisecond)
		}
		if best == nil || rtt < bestRTT || (rtt == bestRTT && id < bestID) {
			best, bestRTT, bestID = client, rtt, id
		}
	}
	return best
}

// hostlessDeadLocked reports whether a room without a host can never get one:
// nobody connected may host and nobody disconnected may come back.
func hostlessDeadLocked(room *Room) bool {
	return room.State.HostID == "" && len(room.Clients) > 0 && len(room.DisconnectedUsers) == 0
}

// closeRoom removes a room, telling members and applicants why. Caller holds
// room.syncMu and no other lock.
func (s *Server) closeRoom(room *Room, reason string) {
	s.mu.Lock()
	room.mu.Lock()
	if current := s.rooms[room.Code]; current == room {
		delete(s.rooms, room.Code)
	}
	for token, session := range s.sessions {
		if session != nil && session.RoomCode == room.Code {
			delete(s.sessions, token)
		}
	}
	members := roomClientsLocked(room)
	for _, client := range members {
		client.sendMessage(s.logger, MsgTypeKicked, KickedPayload{Reason: reason})
		client.clearRoom(room)
	}
	for _, client := range room.PendingJoins {
		if client != nil {
			client.sendMessage(s.logger, MsgTypeJoinRejected, JoinRejectedPayload{Reason: reason})
			client.clearPendingRoom(room)
		}
	}
	room.Clients = make(map[string]*Client)
	room.PendingJoins = make(map[string]*Client)
	room.DisconnectedUsers = make(map[string]*Session)
	room.Host = nil
	room.mu.Unlock()
	s.mu.Unlock()
	s.logger.Info("Closed room", zap.String("room_code", room.Code), zap.String("reason", reason))
}

const noHostLeftReason = "Room closed: nobody left can host it"

// failoverHostsOnce hands control to a deputy in rooms whose host has been
// disconnected longer than HostFailoverTimeout. The old host stays the owner
// and reclaims the room on reconnect.
func (s *Server) failoverHostsOnce(now time.Time) {
	s.mu.RLock()
	rooms := make([]*Room, 0, len(s.rooms))
	for _, room := range s.rooms {
		rooms = append(rooms, room)
	}
	s.mu.RUnlock()

	for _, room := range rooms {
		room.mu.RLock()
		due := room.HostDisconnectedAt != nil && room.State.HostID != "" && now.Sub(*room.HostDisconnectedAt) >= HostFailoverTimeout
		room.mu.RUnlock()
		if !due {
			continue
		}

		room.syncMu.Lock()
		room.mu.Lock()
		if room.HostDisconnectedAt == nil || room.State.HostID == "" || now.Sub(*room.HostDisconnectedAt) < HostFailoverTimeout {
			room.mu.Unlock()
			room.syncMu.Unlock()
			continue
		}
		deputy := pickDeputy(room.Clients)
		if deputy == nil {
			room.mu.Unlock()
			room.syncMu.Unlock()
			continue
		}
		if room.OwnerID == "" {
			room.OwnerID = room.State.HostID
		}
		change := assignHostLocked(room, deputy)
		clients := roomClientsLocked(room)
		room.mu.Unlock()
		s.deliverHostChange(clients, change)
		room.syncMu.Unlock()
		s.logger.Info("Host failover",
			zap.String("room_code", room.Code),
			zap.String("deputy_id", deputy.clientID()),
			zap.Int64("deputy_rtt_ms", deputy.roundTripMs()))
	}
}
