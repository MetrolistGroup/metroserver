package server

import (
	"testing"
	"time"

	pb "github.com/MetrolistGroup/metroserver/proto"
)

func lifecycleTestRoom(host, guest *Client) *Room {
	clients := make(map[string]*Client)
	users := make([]UserInfo, 0, 2)
	if host != nil {
		clients[host.clientID()] = host
		users = append(users, UserInfo{UserID: host.clientID(), Username: host.userName(), IsHost: true, IsConnected: true})
	}
	if guest != nil {
		clients[guest.clientID()] = guest
		users = append(users, UserInfo{UserID: guest.clientID(), Username: guest.userName(), IsConnected: true})
	}
	room := &Room{
		Code:               "ROOM1234",
		Host:               host,
		Clients:            clients,
		PendingJoins:       make(map[string]*Client),
		PendingSuggestions: make(map[string]*Suggestion),
		DisconnectedUsers:  make(map[string]*Session),
		BufferingUsers:     make(map[string]bool),
		State: &RoomState{
			RoomCode: "ROOM1234",
			Users:    users,
		},
	}
	if host != nil {
		room.State.HostID = host.clientID()
		host.setRoom(room)
	}
	if guest != nil {
		guest.setRoom(room)
	}
	return room
}

func receiveLifecycleError(t *testing.T, client *Client, wantCode string) {
	t.Helper()
	var response pb.ErrorPayload
	if msgType := receiveTestMessage(t, client, &response); msgType != MsgTypeError {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeError)
	}
	if response.Code != wantCode {
		t.Fatalf("error code = %q, want %q", response.Code, wantCode)
	}
}

func TestHandleClientDisconnectGuest(t *testing.T) {
	server := testServer()
	host := newClient("host", nil)
	host.setUsername("Host")
	guest := newClient("guest", nil)
	guest.setUsername("Guest")
	guest.setSessionToken("guest-token")
	room := lifecycleTestRoom(host, guest)
	room.BufferingUsers["guest"] = true
	server.rooms[room.Code] = room

	server.handleClientDisconnect(guest)

	if guest.currentRoom() != nil {
		t.Fatal("disconnected guest still references room")
	}
	if _, exists := room.Clients["guest"]; exists {
		t.Fatal("disconnected guest remains active")
	}
	if room.BufferingUsers["guest"] {
		t.Fatal("disconnected guest remains in buffering users")
	}
	session := room.DisconnectedUsers["guest"]
	if session == nil || session.UserID != "guest" || session.Username != "Guest" || session.RoomCode != room.Code || session.IsHost {
		t.Fatalf("unexpected disconnected session: %#v", session)
	}
	if server.sessions["guest-token"] != session {
		t.Fatal("session was not indexed by the existing token")
	}
	if room.State.Users[1].IsConnected {
		t.Fatal("guest is still marked connected")
	}
	if room.HostDisconnectedAt != nil {
		t.Fatal("guest disconnect marked host disconnected")
	}

	var notification pb.UserDisconnectedPayload
	if msgType := receiveTestMessage(t, host, &notification); msgType != MsgTypeUserDisconnected {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeUserDisconnected)
	}
	if notification.UserId != "guest" || notification.Username != "Guest" {
		t.Fatalf("unexpected disconnect notification: %#v", &notification)
	}
}

func TestHandleClientDisconnectHostCreatesToken(t *testing.T) {
	server := testServer()
	host := newClient("host", nil)
	host.setUsername("Host")
	guest := newClient("guest", nil)
	guest.setUsername("Guest")
	room := lifecycleTestRoom(host, guest)
	server.rooms[room.Code] = room

	server.handleClientDisconnect(host)

	if host.session() == "" {
		t.Fatal("host disconnect did not generate a session token")
	}
	if session := server.sessions[host.session()]; session == nil || !session.IsHost {
		t.Fatalf("unexpected host session: %#v", session)
	}
	if room.HostDisconnectedAt == nil {
		t.Fatal("host disconnect timestamp was not recorded")
	}
	if room.State.Users[0].IsConnected {
		t.Fatal("host is still marked connected")
	}
	var notification pb.UserDisconnectedPayload
	if msgType := receiveTestMessage(t, guest, &notification); msgType != MsgTypeUserDisconnected {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeUserDisconnected)
	}
	if notification.UserId != "host" || notification.Username != "Host" {
		t.Fatalf("unexpected disconnect notification: %#v", &notification)
	}
}

func TestHandleClientDisconnectWithoutRoomDoesNothing(t *testing.T) {
	server := testServer()
	client := newClient("guest", nil)
	server.handleClientDisconnect(client)
	if client.session() != "" || len(server.sessions) != 0 {
		t.Fatal("disconnect without a room created a session")
	}
}

func TestHandleReconnectGuest(t *testing.T) {
	server := testServer()
	host := newClient("host", nil)
	host.setUsername("Host")
	reconnecting := newClient("temporary", nil)
	session := &Session{
		UserID:       "guest",
		Username:     "Guest",
		RoomCode:     "ROOM1234",
		DisconnectAt: time.Now(),
	}
	room := lifecycleTestRoom(host, nil)
	room.State.Position = 321
	room.State.LastUpdate = 123
	room.State.Users = append(room.State.Users, UserInfo{UserID: "guest", Username: "Guest", IsConnected: false})
	room.DisconnectedUsers["guest"] = session
	server.rooms[room.Code] = room
	server.sessions["guest-token"] = session

	server.handleReconnect(reconnecting, encodeTestPayload(t, MsgTypeReconnect, &ReconnectPayload{SessionToken: "guest-token"}))

	if reconnecting.clientID() != "guest" || reconnecting.userName() != "Guest" || reconnecting.session() != "guest-token" {
		t.Fatalf("client identity was not restored: id=%q name=%q token=%q", reconnecting.clientID(), reconnecting.userName(), reconnecting.session())
	}
	if reconnecting.currentRoom() != room || room.Clients["guest"] != reconnecting {
		t.Fatal("client was not restored to the room")
	}
	if _, exists := room.DisconnectedUsers["guest"]; exists {
		t.Fatal("reconnected user remains disconnected")
	}
	if _, exists := server.sessions["guest-token"]; exists {
		t.Fatal("successful reconnect did not consume session")
	}
	if !room.State.Users[1].IsConnected {
		t.Fatal("guest is not marked connected")
	}

	var response pb.ReconnectedPayload
	if msgType := receiveTestMessage(t, reconnecting, &response); msgType != MsgTypeReconnected {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeReconnected)
	}
	if response.RoomCode != room.Code || response.UserId != "guest" || response.IsHost || response.State == nil || response.State.Position != 321 {
		t.Fatalf("unexpected reconnect response: %#v", &response)
	}
	if response.State.LastUpdate <= 123 {
		t.Fatalf("live state last update = %d, want refreshed value", response.State.LastUpdate)
	}

	var notification pb.UserReconnectedPayload
	if msgType := receiveTestMessage(t, host, &notification); msgType != MsgTypeUserReconnected {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeUserReconnected)
	}
	if notification.UserId != "guest" || notification.Username != "Guest" {
		t.Fatalf("unexpected reconnect notification: %#v", &notification)
	}
}

func TestHandleReconnectHostReplaysPendingWork(t *testing.T) {
	server := testServer()
	reconnecting := newClient("temporary", nil)
	guest := newClient("guest", nil)
	guest.setUsername("Guest")
	pending := newClient("pending", nil)
	pending.setUsername("Pending")
	closedPending := newClient("closed", nil)
	closedPending.closeSend()
	session := &Session{
		UserID:       "host",
		Username:     "Host",
		RoomCode:     "ROOM1234",
		IsHost:       true,
		DisconnectAt: time.Now(),
	}
	room := lifecycleTestRoom(nil, guest)
	room.State.HostID = "host"
	room.State.Users = append([]UserInfo{{UserID: "host", Username: "Host", IsHost: true, IsConnected: false}}, room.State.Users...)
	room.DisconnectedUsers["host"] = session
	now := time.Now()
	room.HostDisconnectedAt = &now
	room.PendingJoins["pending"] = pending
	room.PendingJoins["nil"] = nil
	room.PendingJoins["closed"] = closedPending
	room.PendingSuggestions["suggestion"] = &Suggestion{
		ID:           "suggestion",
		FromUserID:   "guest",
		FromUsername: "Guest",
		Track:        &TrackInfo{ID: "track", Title: "Track", Artist: "Artist", Duration: 1000},
	}
	room.PendingSuggestions["nil"] = nil
	server.rooms[room.Code] = room
	server.sessions["host-token"] = session

	server.handleReconnect(reconnecting, encodeTestPayload(t, MsgTypeReconnect, &ReconnectPayload{SessionToken: "host-token"}))

	if room.Host != reconnecting || room.State.HostID != "host" || room.HostDisconnectedAt != nil {
		t.Fatal("host status was not restored")
	}
	if !room.State.Users[0].IsHost || room.State.Users[1].IsHost || !room.State.Users[0].IsConnected {
		t.Fatalf("unexpected host flags after reconnect: %#v", room.State.Users)
	}
	var reconnected pb.ReconnectedPayload
	if msgType := receiveTestMessage(t, reconnecting, &reconnected); msgType != MsgTypeReconnected || !reconnected.IsHost {
		t.Fatalf("unexpected host reconnect response type=%q payload=%#v", msgType, &reconnected)
	}
	var joinRequest pb.JoinRequestPayload
	if msgType := receiveTestMessage(t, reconnecting, &joinRequest); msgType != MsgTypeJoinRequest {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeJoinRequest)
	}
	if joinRequest.UserId != "pending" || joinRequest.Username != "Pending" {
		t.Fatalf("unexpected replayed join request: %#v", &joinRequest)
	}
	var suggestion pb.SuggestionReceivedPayload
	if msgType := receiveTestMessage(t, reconnecting, &suggestion); msgType != MsgTypeSuggestionReceived {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeSuggestionReceived)
	}
	if suggestion.SuggestionId != "suggestion" || suggestion.FromUserId != "guest" || suggestion.TrackInfo == nil || suggestion.TrackInfo.Id != "track" {
		t.Fatalf("unexpected replayed suggestion: %#v", &suggestion)
	}
	var notification pb.UserReconnectedPayload
	if msgType := receiveTestMessage(t, guest, &notification); msgType != MsgTypeUserReconnected || notification.UserId != "host" {
		t.Fatalf("unexpected host reconnect notification type=%q payload=%#v", msgType, &notification)
	}
}

func TestHandleReconnectRejectsInvalidRequests(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*Server, *Client)
		payload  func(*testing.T) []byte
		wantCode string
	}{
		{
			name:     "invalid payload",
			payload:  func(*testing.T) []byte { return []byte{0xff} },
			wantCode: "invalid_payload",
		},
		{
			name:     "missing token",
			payload:  func(t *testing.T) []byte { return encodeTestPayload(t, MsgTypeReconnect, &ReconnectPayload{}) },
			wantCode: "missing_session_token",
		},
		{
			name: "already in room",
			setup: func(_ *Server, client *Client) {
				client.setRoom(&Room{})
			},
			payload: func(t *testing.T) []byte {
				return encodeTestPayload(t, MsgTypeReconnect, &ReconnectPayload{SessionToken: "token"})
			},
			wantCode: "already_in_room",
		},
		{
			name: "unknown session",
			payload: func(t *testing.T) []byte {
				return encodeTestPayload(t, MsgTypeReconnect, &ReconnectPayload{SessionToken: "unknown"})
			},
			wantCode: "session_not_found",
		},
		{
			name: "expired session",
			setup: func(server *Server, _ *Client) {
				server.sessions["expired"] = &Session{DisconnectAt: time.Now().Add(-ReconnectGracePeriod - time.Second)}
			},
			payload: func(t *testing.T) []byte {
				return encodeTestPayload(t, MsgTypeReconnect, &ReconnectPayload{SessionToken: "expired"})
			},
			wantCode: "session_expired",
		},
		{
			name: "missing room",
			setup: func(server *Server, _ *Client) {
				server.sessions["orphan"] = &Session{RoomCode: "MISSING", DisconnectAt: time.Now()}
			},
			payload: func(t *testing.T) []byte {
				return encodeTestPayload(t, MsgTypeReconnect, &ReconnectPayload{SessionToken: "orphan"})
			},
			wantCode: "room_not_found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := testServer()
			client := newClient("client", nil)
			if tt.setup != nil {
				tt.setup(server, client)
			}
			server.handleReconnect(client, tt.payload(t))
			receiveLifecycleError(t, client, tt.wantCode)
			if tt.wantCode == "session_expired" {
				if _, exists := server.sessions["expired"]; exists {
					t.Fatal("expired session was not deleted")
				}
			}
			if tt.wantCode == "room_not_found" {
				if _, exists := server.sessions["orphan"]; exists {
					t.Fatal("orphaned session was not deleted")
				}
			}
		})
	}
}

func TestRemoveClientPaths(t *testing.T) {
	t.Run("active client", func(t *testing.T) {
		server := testServer()
		host := newClient("host", nil)
		host.setUsername("Host")
		guest := newClient("guest", nil)
		guest.setUsername("Guest")
		room := lifecycleTestRoom(host, guest)
		server.rooms[room.Code] = room
		server.clients[guest] = true

		server.removeClient(guest)

		if server.clients[guest] || !guest.isClosed() {
			t.Fatal("active client was not removed and closed")
		}
		if room.DisconnectedUsers["guest"] == nil {
			t.Fatal("active client did not enter disconnected state")
		}
		var notification pb.UserDisconnectedPayload
		if msgType := receiveTestMessage(t, host, &notification); msgType != MsgTypeUserDisconnected || notification.UserId != "guest" {
			t.Fatalf("unexpected disconnect notification type=%q payload=%#v", msgType, &notification)
		}
	})

	t.Run("pending client", func(t *testing.T) {
		server := testServer()
		pending := newClient("pending", nil)
		other := newClient("pending", nil)
		room1 := lifecycleTestRoom(nil, nil)
		room2 := lifecycleTestRoom(nil, nil)
		room2.Code = "ROOM5678"
		room2.State.RoomCode = room2.Code
		room1.PendingJoins["pending"] = pending
		room2.PendingJoins["pending"] = other
		server.rooms[room1.Code] = room1
		server.rooms[room2.Code] = room2
		server.clients[pending] = true

		server.removeClient(pending)

		if _, exists := room1.PendingJoins["pending"]; exists {
			t.Fatal("matching pending join was not removed")
		}
		if room2.PendingJoins["pending"] != other {
			t.Fatal("a different pending client was removed")
		}
		if server.clients[pending] || !pending.isClosed() {
			t.Fatal("pending client was not removed and closed")
		}
	})

	t.Run("pending client without id", func(t *testing.T) {
		server := testServer()
		client := newClient("", nil)
		room := lifecycleTestRoom(nil, nil)
		room.PendingJoins[""] = client
		server.rooms[room.Code] = room
		server.clients[client] = true

		server.removeClient(client)

		if room.PendingJoins[""] != client {
			t.Fatal("empty-id pending join should be ignored")
		}
		if server.clients[client] || !client.isClosed() {
			t.Fatal("empty-id client was not removed and closed")
		}
	})
}

func TestDeleteRoomIfEmpty(t *testing.T) {
	t.Run("deletes registered empty room", func(t *testing.T) {
		server := testServer()
		room := lifecycleTestRoom(nil, nil)
		server.rooms[room.Code] = room
		if !server.deleteRoomIfEmpty(room) {
			t.Fatal("empty room was not deleted")
		}
		if _, exists := server.rooms[room.Code]; exists {
			t.Fatal("deleted room remains registered")
		}
	})

	t.Run("rejects missing or replaced room", func(t *testing.T) {
		server := testServer()
		room := lifecycleTestRoom(nil, nil)
		if server.deleteRoomIfEmpty(room) {
			t.Fatal("unregistered room was deleted")
		}
		server.rooms[room.Code] = lifecycleTestRoom(nil, nil)
		if server.deleteRoomIfEmpty(room) {
			t.Fatal("replaced room was deleted")
		}
	})

	t.Run("retains active and disconnected rooms", func(t *testing.T) {
		server := testServer()
		active := newClient("active", nil)
		room := lifecycleTestRoom(nil, active)
		server.rooms[room.Code] = room
		if server.deleteRoomIfEmpty(room) {
			t.Fatal("room with active client was deleted")
		}
		delete(room.Clients, "active")
		room.DisconnectedUsers["active"] = &Session{UserID: "active"}
		if server.deleteRoomIfEmpty(room) {
			t.Fatal("room with disconnected user was deleted")
		}
	})
}

func TestCleanupExpiredSessionsOnceRemovesGuestAndNotifies(t *testing.T) {
	server := testServer()
	host := newClient("host", nil)
	host.setUsername("Host")
	room := lifecycleTestRoom(host, nil)
	expired := &Session{UserID: "guest", Username: "Guest", RoomCode: room.Code, DisconnectAt: time.Unix(1, 0)}
	room.State.Users = append(room.State.Users, UserInfo{UserID: "guest", Username: "Guest", IsConnected: false})
	room.DisconnectedUsers["guest"] = expired
	server.rooms[room.Code] = room
	server.sessions["expired"] = expired
	server.sessions["current"] = &Session{UserID: "current", DisconnectAt: time.Unix(1000, 0)}
	now := time.Unix(1000, 0)

	server.cleanupExpiredSessionsOnce(now)

	if _, exists := server.sessions["expired"]; exists {
		t.Fatal("expired guest session remains indexed")
	}
	if _, exists := server.sessions["current"]; !exists {
		t.Fatal("unexpired session was removed")
	}
	if _, exists := room.DisconnectedUsers["guest"]; exists {
		t.Fatal("expired guest remains disconnected")
	}
	if len(room.State.Users) != 1 || room.State.Users[0].UserID != "host" {
		t.Fatalf("unexpected users after expiration: %#v", room.State.Users)
	}
	if room.Host != host || room.State.HostID != "host" {
		t.Fatal("guest expiration changed host")
	}
	var left pb.UserLeftPayload
	if msgType := receiveTestMessage(t, host, &left); msgType != MsgTypeUserLeft {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeUserLeft)
	}
	if left.UserId != "guest" || left.Username != "Guest" {
		t.Fatalf("unexpected user-left payload: %#v", &left)
	}
}

func TestCleanupExpiredSessionsOnceDeletesEmptyRoom(t *testing.T) {
	server := testServer()
	server.startTime = time.Unix(1, 0)
	expired := &Session{UserID: "guest", Username: "Guest", RoomCode: "ROOM1234", DisconnectAt: time.Unix(2, 0)}
	room := lifecycleTestRoom(nil, nil)
	room.DisconnectedUsers["guest"] = expired
	room.State.Users = []UserInfo{{UserID: "guest", Username: "Guest", IsConnected: false}}
	server.rooms[room.Code] = room
	server.sessions["expired"] = expired

	server.cleanupExpiredSessionsOnce(time.Unix(1000, 0))

	if _, exists := server.sessions["expired"]; exists {
		t.Fatal("expired session remains indexed")
	}
	if _, exists := server.rooms[room.Code]; exists {
		t.Fatal("empty room past retention was not deleted")
	}
}

func TestCleanupExpiredSessionsOnceRetainsRecentEmptyRoomAndHandlesOrphan(t *testing.T) {
	server := testServer()
	now := time.Unix(1000, 0)
	server.startTime = now
	hostSession := &Session{UserID: "host", Username: "Host", RoomCode: "ROOM1234", IsHost: true, DisconnectAt: time.Unix(1, 0)}
	orphan := &Session{UserID: "orphan", RoomCode: "MISSING", DisconnectAt: time.Unix(1, 0)}
	room := lifecycleTestRoom(nil, nil)
	room.State.HostID = "host"
	room.State.Users = []UserInfo{{UserID: "host", Username: "Host", IsHost: true, IsConnected: false}}
	room.DisconnectedUsers["host"] = hostSession
	stamp := time.Unix(900, 0)
	room.HostDisconnectedAt = &stamp
	server.rooms[room.Code] = room
	server.sessions["host"] = hostSession
	server.sessions["orphan"] = orphan

	server.cleanupExpiredSessionsOnce(now)

	if _, exists := server.rooms[room.Code]; !exists {
		t.Fatal("empty room inside restart retention window was deleted")
	}
	if room.Host != nil || room.State.HostID != "" || room.HostDisconnectedAt != nil || len(room.State.Users) != 0 {
		t.Fatalf("expired host was not fully cleared: host=%p state=%#v disconnectedAt=%v", room.Host, room.State, room.HostDisconnectedAt)
	}
	if len(server.sessions) != 0 {
		t.Fatalf("expired host and orphan sessions remain: %#v", server.sessions)
	}
}
