package server

import (
	"strings"
	"testing"

	pb "github.com/MetrolistGroup/metroserver/proto"
)

func roomTestFixture() (*Server, *Room, *Client, *Client) {
	server := testServer()
	host := newClient("host", nil)
	host.setUsername("Host")
	guest := newClient("guest", nil)
	guest.setUsername("Guest")
	room := &Room{
		Code:               "ROOM1234",
		Host:               host,
		Clients:            map[string]*Client{"host": host, "guest": guest},
		PendingJoins:       make(map[string]*Client),
		PendingSuggestions: make(map[string]*Suggestion),
		DisconnectedUsers:  make(map[string]*Session),
		BufferingUsers:     make(map[string]bool),
		State: &RoomState{
			RoomCode: "ROOM1234",
			HostID:   "host",
			Users: []UserInfo{
				{UserID: "host", Username: "Host", IsHost: true, IsConnected: true},
				{UserID: "guest", Username: "Guest", IsConnected: true},
			},
			Queue: []TrackInfo{},
		},
	}
	host.setRoom(room)
	guest.setRoom(room)
	server.rooms[room.Code] = room
	return server, room, host, guest
}

func receiveRoomError(t *testing.T, client *Client, want string) {
	t.Helper()
	var response pb.ErrorPayload
	if got := receiveTestMessage(t, client, &response); got != MsgTypeError {
		t.Fatalf("message type = %q, want %q", got, MsgTypeError)
	}
	if response.Code != want {
		t.Fatalf("error code = %q, want %q", response.Code, want)
	}
}

func TestSuggestionApproveAndRejectFlows(t *testing.T) {
	t.Run("approve", func(t *testing.T) {
		server, room, host, guest := roomTestFixture()
		track := &TrackInfo{ID: " track-1 ", Title: " Song ", Artist: "Artist", Duration: 1000}
		server.handleSuggestTrack(guest, encodeTestPayload(t, MsgTypeSuggestTrack, &SuggestTrackPayload{TrackInfo: track}))

		var received pb.SuggestionReceivedPayload
		if got := receiveTestMessage(t, host, &received); got != MsgTypeSuggestionReceived {
			t.Fatalf("message type = %q, want %q", got, MsgTypeSuggestionReceived)
		}
		if received.SuggestionId == "" || received.FromUserId != "guest" || received.FromUsername != "Guest" || received.TrackInfo.GetId() != "track-1" {
			t.Fatalf("unexpected suggestion: %#v", &received)
		}
		if len(room.PendingSuggestions) != 1 {
			t.Fatalf("pending suggestions = %d, want 1", len(room.PendingSuggestions))
		}

		server.handleApproveSuggestion(host, encodeTestPayload(t, MsgTypeApproveSuggestion, &ApproveSuggestionPayload{SuggestionID: received.SuggestionId}))
		if len(room.PendingSuggestions) != 0 || len(room.State.Queue) != 1 {
			t.Fatalf("unexpected suggestion/queue state: pending=%d queue=%#v", len(room.PendingSuggestions), room.State.Queue)
		}
		if room.State.Queue[0].ID != "track-1" || room.State.Queue[0].SuggestedBy != "Guest" {
			t.Fatalf("unexpected queued track: %#v", room.State.Queue[0])
		}

		var hostSync pb.PlaybackActionPayload
		if got := receiveTestMessage(t, host, &hostSync); got != MsgTypeSyncPlayback || hostSync.Action != ActionQueueAdd || !hostSync.InsertNext {
			t.Fatalf("unexpected host queue message: type=%q payload=%#v", got, &hostSync)
		}
		var guestSync pb.PlaybackActionPayload
		if got := receiveTestMessage(t, guest, &guestSync); got != MsgTypeSyncPlayback || guestSync.TrackInfo.GetId() != "track-1" {
			t.Fatalf("unexpected guest queue message: type=%q payload=%#v", got, &guestSync)
		}
		var approved pb.SuggestionApprovedPayload
		if got := receiveTestMessage(t, guest, &approved); got != MsgTypeSuggestionApproved || approved.SuggestionId != received.SuggestionId {
			t.Fatalf("unexpected approval: type=%q payload=%#v", got, &approved)
		}
	})

	t.Run("reject truncates reason", func(t *testing.T) {
		server, room, host, guest := roomTestFixture()
		room.PendingSuggestions["s1"] = &Suggestion{ID: "s1", FromUserID: "guest", FromUsername: "Guest", Track: &TrackInfo{ID: "track", Title: "Song"}}
		server.handleRejectSuggestion(host, encodeTestPayload(t, MsgTypeRejectSuggestion, &RejectSuggestionPayload{
			SuggestionID: "s1",
			Reason:       strings.Repeat("x", 220),
		}))
		if _, exists := room.PendingSuggestions["s1"]; exists {
			t.Fatal("rejected suggestion remained pending")
		}
		var rejected pb.SuggestionRejectedPayload
		if got := receiveTestMessage(t, guest, &rejected); got != MsgTypeSuggestionRejected || rejected.SuggestionId != "s1" || len(rejected.Reason) != 200 {
			t.Fatalf("unexpected rejection: type=%q payload=%#v", got, &rejected)
		}
	})
}

func TestSuggestionFailures(t *testing.T) {
	t.Run("suggest validation", func(t *testing.T) {
		tests := []struct {
			name string
			prep func(*Room, *Client)
			body []byte
			want string
		}{
			{name: "invalid payload", body: []byte{0xff}, want: "invalid_payload"},
			{name: "missing track", body: encodeTestPayload(t, MsgTypeSuggestTrack, &SuggestTrackPayload{}), want: "missing_track_info"},
			{name: "invalid track", body: encodeTestPayload(t, MsgTypeSuggestTrack, &SuggestTrackPayload{TrackInfo: &TrackInfo{Title: "title"}}), want: "invalid_track_info"},
			{name: "stale membership", prep: func(room *Room, guest *Client) { delete(room.Clients, guest.clientID()) }, body: encodeTestPayload(t, MsgTypeSuggestTrack, &SuggestTrackPayload{TrackInfo: &TrackInfo{ID: "id", Title: "title"}}), want: "not_in_room"},
			{name: "full", prep: func(room *Room, _ *Client) {
				for i := 0; i < MaxPendingSuggestions; i++ {
					room.PendingSuggestions[string(rune(i))] = &Suggestion{}
				}
			}, body: encodeTestPayload(t, MsgTypeSuggestTrack, &SuggestTrackPayload{TrackInfo: &TrackInfo{ID: "id", Title: "title"}}), want: "suggestions_full"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				server, room, _, guest := roomTestFixture()
				if tc.prep != nil {
					tc.prep(room, guest)
				}
				server.handleSuggestTrack(guest, tc.body)
				receiveRoomError(t, guest, tc.want)
			})
		}

		server := testServer()
		outside := newClient("outside", nil)
		server.handleSuggestTrack(outside, encodeTestPayload(t, MsgTypeSuggestTrack, &SuggestTrackPayload{TrackInfo: &TrackInfo{ID: "id", Title: "title"}}))
		receiveRoomError(t, outside, "not_in_room")
	})

	t.Run("host actions", func(t *testing.T) {
		tests := []struct {
			name    string
			handler func(*Server, *Client, []byte)
			msgType string
			body    any
			want    string
		}{
			{name: "approve invalid", handler: (*Server).handleApproveSuggestion, msgType: MsgTypeApproveSuggestion, body: nil, want: "invalid_payload"},
			{name: "approve missing id", handler: (*Server).handleApproveSuggestion, msgType: MsgTypeApproveSuggestion, body: &ApproveSuggestionPayload{}, want: "missing_suggestion_id"},
			{name: "approve non-host", handler: (*Server).handleApproveSuggestion, msgType: MsgTypeApproveSuggestion, body: &ApproveSuggestionPayload{SuggestionID: "s1"}, want: "not_host"},
			{name: "reject invalid", handler: (*Server).handleRejectSuggestion, msgType: MsgTypeRejectSuggestion, body: nil, want: "invalid_payload"},
			{name: "reject missing id", handler: (*Server).handleRejectSuggestion, msgType: MsgTypeRejectSuggestion, body: &RejectSuggestionPayload{}, want: "missing_suggestion_id"},
			{name: "reject non-host", handler: (*Server).handleRejectSuggestion, msgType: MsgTypeRejectSuggestion, body: &RejectSuggestionPayload{SuggestionID: "s1"}, want: "not_host"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				server, _, _, guest := roomTestFixture()
				payload := []byte{0xff}
				if tc.body != nil {
					payload = encodeTestPayload(t, tc.msgType, tc.body)
				}
				tc.handler(server, guest, payload)
				receiveRoomError(t, guest, tc.want)
			})
		}

		server, _, host, _ := roomTestFixture()
		server.handleApproveSuggestion(host, encodeTestPayload(t, MsgTypeApproveSuggestion, &ApproveSuggestionPayload{SuggestionID: "unknown"}))
		receiveRoomError(t, host, "suggestion_not_found")
		server.handleRejectSuggestion(host, encodeTestPayload(t, MsgTypeRejectSuggestion, &RejectSuggestionPayload{SuggestionID: "unknown"}))
		receiveRoomError(t, host, "suggestion_not_found")
	})
}

func TestCreateRoomHappyPathAndValidation(t *testing.T) {
	server := testServer()
	client := newClient("creator", nil)
	server.handleCreateRoom(client, encodeTestPayload(t, MsgTypeCreateRoom, &CreateRoomPayload{Username: "  Creator  "}))

	var created pb.RoomCreatedPayload
	if got := receiveTestMessage(t, client, &created); got != MsgTypeRoomCreated {
		t.Fatalf("message type = %q, want %q", got, MsgTypeRoomCreated)
	}
	room := client.currentRoom()
	if room == nil || created.RoomCode == "" || created.UserId != "creator" || created.SessionToken == "" {
		t.Fatalf("unexpected room-created result: room=%#v payload=%#v", room, &created)
	}
	if room.Host != client || room.State.HostID != "creator" || room.State.Users[0].Username != "Creator" || room.State.Volume != 1 {
		t.Fatalf("unexpected initial room state: %#v", room.State)
	}
	if server.rooms[created.RoomCode] != room || client.session() != created.SessionToken {
		t.Fatal("created room or session token was not installed")
	}

	tests := []struct {
		name    string
		payload []byte
		prep    func(*Client)
		want    string
	}{
		{name: "invalid payload", payload: []byte{0xff}, want: "invalid_payload"},
		{name: "missing username", payload: encodeTestPayload(t, MsgTypeCreateRoom, &CreateRoomPayload{}), want: "missing_username"},
		{name: "invalid username", payload: encodeTestPayload(t, MsgTypeCreateRoom, &CreateRoomPayload{Username: "\x01"}), want: "invalid_username"},
		{name: "already in room", payload: encodeTestPayload(t, MsgTypeCreateRoom, &CreateRoomPayload{Username: "Other"}), prep: func(c *Client) { c.setRoom(room) }, want: "already_in_room"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient("other", nil)
			if tc.prep != nil {
				tc.prep(c)
			}
			server.handleCreateRoom(c, tc.payload)
			receiveRoomError(t, c, tc.want)
		})
	}
}

func TestJoinApproveAndRejectFlows(t *testing.T) {
	t.Run("approve with playback sync", func(t *testing.T) {
		server, room, host, existing := roomTestFixture()
		delete(room.Clients, "guest")
		room.State.Users = room.State.Users[:1]
		existing.setRoom(nil)
		room.State.CurrentTrack = &TrackInfo{ID: "playing", Title: "Playing", Duration: 10000}
		room.State.IsPlaying = true
		room.State.Position = 500

		server.handleJoinRoom(existing, encodeTestPayload(t, MsgTypeJoinRoom, &JoinRoomPayload{RoomCode: "room1234", Username: " Guest "}))
		var request pb.JoinRequestPayload
		if got := receiveTestMessage(t, host, &request); got != MsgTypeJoinRequest || request.UserId != "guest" || request.Username != "Guest" {
			t.Fatalf("unexpected join request: type=%q payload=%#v", got, &request)
		}
		if room.PendingJoins["guest"] != existing || existing.currentRoom() != nil {
			t.Fatal("join request was not left pending")
		}

		server.handleApproveJoin(host, encodeTestPayload(t, MsgTypeApproveJoin, &ApproveJoinPayload{UserID: "guest"}))
		if room.Clients["guest"] != existing || existing.currentRoom() != room || existing.session() == "" || len(room.State.Users) != 2 {
			t.Fatalf("approved user not installed correctly: users=%#v", room.State.Users)
		}
		var approved pb.JoinApprovedPayload
		if got := receiveTestMessage(t, existing, &approved); got != MsgTypeJoinApproved || approved.RoomCode != room.Code || approved.State.GetHostId() != "host" {
			t.Fatalf("unexpected join approval: type=%q payload=%#v", got, &approved)
		}
		var buffered pb.BufferCompletePayload
		if got := receiveTestMessage(t, existing, &buffered); got != MsgTypeBufferComplete || buffered.TrackId != "playing" {
			t.Fatalf("unexpected buffer message: type=%q payload=%#v", got, &buffered)
		}
		for _, action := range []string{ActionSeek, ActionPlay} {
			var sync pb.PlaybackActionPayload
			if got := receiveTestMessage(t, existing, &sync); got != MsgTypeSyncPlayback || sync.Action != action || sync.TrackId != "playing" {
				t.Fatalf("unexpected playback sync: type=%q payload=%#v", got, &sync)
			}
		}
		var joined pb.UserJoinedPayload
		if got := receiveTestMessage(t, host, &joined); got != MsgTypeUserJoined || joined.UserId != "guest" {
			t.Fatalf("unexpected joined notification: type=%q payload=%#v", got, &joined)
		}
	})

	t.Run("reject uses default reason", func(t *testing.T) {
		server, room, host, guest := roomTestFixture()
		delete(room.Clients, "guest")
		guest.setRoom(nil)
		room.PendingJoins["guest"] = guest
		server.handleRejectJoin(host, encodeTestPayload(t, MsgTypeRejectJoin, &RejectJoinPayload{UserID: "guest"}))
		if _, exists := room.PendingJoins["guest"]; exists {
			t.Fatal("rejected join remained pending")
		}
		var rejected pb.JoinRejectedPayload
		if got := receiveTestMessage(t, guest, &rejected); got != MsgTypeJoinRejected || rejected.Reason != "Join request rejected by host" {
			t.Fatalf("unexpected join rejection: type=%q payload=%#v", got, &rejected)
		}
	})
}

func TestJoinValidationAndAuthorization(t *testing.T) {
	server, room, host, guest := roomTestFixture()
	delete(room.Clients, "guest")
	room.State.Users = room.State.Users[:1]
	guest.setRoom(nil)

	tests := []struct {
		name    string
		payload []byte
		prep    func(*Client)
		want    string
	}{
		{name: "invalid payload", payload: []byte{0xff}, want: "invalid_payload"},
		{name: "missing username", payload: encodeTestPayload(t, MsgTypeJoinRoom, &JoinRoomPayload{RoomCode: room.Code}), want: "missing_username"},
		{name: "invalid username", payload: encodeTestPayload(t, MsgTypeJoinRoom, &JoinRoomPayload{RoomCode: room.Code, Username: "\x01"}), want: "invalid_username"},
		{name: "missing room", payload: encodeTestPayload(t, MsgTypeJoinRoom, &JoinRoomPayload{Username: "Guest"}), want: "missing_room_code"},
		{name: "invalid room", payload: encodeTestPayload(t, MsgTypeJoinRoom, &JoinRoomPayload{RoomCode: "\x01", Username: "Guest"}), want: "invalid_room_code"},
		{name: "not found", payload: encodeTestPayload(t, MsgTypeJoinRoom, &JoinRoomPayload{RoomCode: "UNKNOWN", Username: "Guest"}), want: "room_not_found"},
		{name: "already in room", payload: encodeTestPayload(t, MsgTypeJoinRoom, &JoinRoomPayload{RoomCode: room.Code, Username: "Guest"}), prep: func(c *Client) { c.setRoom(room) }, want: "already_in_room"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient("candidate", nil)
			if tc.prep != nil {
				tc.prep(c)
			}
			server.handleJoinRoom(c, tc.payload)
			receiveRoomError(t, c, tc.want)
		})
	}

	t.Run("duplicate pending", func(t *testing.T) {
		room.PendingJoins["guest"] = guest
		server.handleJoinRoom(guest, encodeTestPayload(t, MsgTypeJoinRoom, &JoinRoomPayload{RoomCode: room.Code, Username: "Guest"}))
		receiveRoomError(t, guest, "already_pending")
		delete(room.PendingJoins, "guest")
	})

	actions := []struct {
		name    string
		handler func(*Server, *Client, []byte)
		msgType string
		body    any
		want    string
	}{
		{name: "approve invalid", handler: (*Server).handleApproveJoin, msgType: MsgTypeApproveJoin, want: "invalid_payload"},
		{name: "approve missing", handler: (*Server).handleApproveJoin, msgType: MsgTypeApproveJoin, body: &ApproveJoinPayload{}, want: "missing_user_id"},
		{name: "approve non-host", handler: (*Server).handleApproveJoin, msgType: MsgTypeApproveJoin, body: &ApproveJoinPayload{UserID: "guest"}, want: "not_host"},
		{name: "reject invalid", handler: (*Server).handleRejectJoin, msgType: MsgTypeRejectJoin, want: "invalid_payload"},
		{name: "reject missing", handler: (*Server).handleRejectJoin, msgType: MsgTypeRejectJoin, body: &RejectJoinPayload{}, want: "missing_user_id"},
		{name: "reject non-host", handler: (*Server).handleRejectJoin, msgType: MsgTypeRejectJoin, body: &RejectJoinPayload{UserID: "guest"}, want: "not_host"},
	}
	for _, tc := range actions {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, nonHost := roomTestFixture()
			payload := []byte{0xff}
			if tc.body != nil {
				payload = encodeTestPayload(t, tc.msgType, tc.body)
			}
			tc.handler(server, nonHost, payload)
			receiveRoomError(t, nonHost, tc.want)
		})
	}

	server.handleApproveJoin(host, encodeTestPayload(t, MsgTypeApproveJoin, &ApproveJoinPayload{UserID: "unknown"}))
	receiveRoomError(t, host, "join_request_not_found")
	server.handleRejectJoin(host, encodeTestPayload(t, MsgTypeRejectJoin, &RejectJoinPayload{UserID: "unknown"}))
	receiveRoomError(t, host, "join_request_not_found")
}

func TestKickAndTransferHost(t *testing.T) {
	t.Run("kick", func(t *testing.T) {
		server, room, host, guest := roomTestFixture()
		room.BufferingUsers["guest"] = true
		server.handleKickUser(host, encodeTestPayload(t, MsgTypeKickUser, &KickUserPayload{UserID: "guest", Reason: strings.Repeat("x", 220)}))
		if guest.currentRoom() != nil || room.Clients["guest"] != nil || room.BufferingUsers["guest"] || len(room.State.Users) != 1 {
			t.Fatalf("guest was not fully removed: clients=%#v users=%#v", room.Clients, room.State.Users)
		}
		var kicked pb.KickedPayload
		if got := receiveTestMessage(t, guest, &kicked); got != MsgTypeKicked || len(kicked.Reason) != 200 {
			t.Fatalf("unexpected kicked message: type=%q payload=%#v", got, &kicked)
		}
		var left pb.UserLeftPayload
		if got := receiveTestMessage(t, host, &left); got != MsgTypeUserLeft || left.UserId != "guest" || left.Username != "Guest" {
			t.Fatalf("unexpected user-left message: type=%q payload=%#v", got, &left)
		}
	})

	t.Run("transfer", func(t *testing.T) {
		server, room, host, guest := roomTestFixture()
		server.handleTransferHost(host, encodeTestPayload(t, MsgTypeTransferHost, &TransferHostPayload{NewHostID: "guest"}))
		if room.Host != guest || room.State.HostID != "guest" || room.State.Users[0].IsHost || !room.State.Users[1].IsHost {
			t.Fatalf("host role was not transferred: %#v", room.State.Users)
		}
		for _, client := range []*Client{host, guest} {
			var changed pb.HostChangedPayload
			if got := receiveTestMessage(t, client, &changed); got != MsgTypeHostChanged || changed.NewHostId != "guest" || changed.NewHostName != "Guest" {
				t.Fatalf("unexpected host change: type=%q payload=%#v", got, &changed)
			}
		}
	})
}

func TestKickAndTransferFailures(t *testing.T) {
	tests := []struct {
		name    string
		handler func(*Server, *Client, []byte)
		msgType string
		body    any
		actor   string
		want    string
	}{
		{name: "kick invalid", handler: (*Server).handleKickUser, msgType: MsgTypeKickUser, actor: "host", want: "invalid_payload"},
		{name: "kick missing", handler: (*Server).handleKickUser, msgType: MsgTypeKickUser, body: &KickUserPayload{}, actor: "host", want: "missing_user_id"},
		{name: "kick non-host", handler: (*Server).handleKickUser, msgType: MsgTypeKickUser, body: &KickUserPayload{UserID: "host"}, actor: "guest", want: "not_host"},
		{name: "kick self", handler: (*Server).handleKickUser, msgType: MsgTypeKickUser, body: &KickUserPayload{UserID: "host"}, actor: "host", want: "cannot_kick_self"},
		{name: "kick unknown", handler: (*Server).handleKickUser, msgType: MsgTypeKickUser, body: &KickUserPayload{UserID: "unknown"}, actor: "host", want: "user_not_found"},
		{name: "transfer invalid", handler: (*Server).handleTransferHost, msgType: MsgTypeTransferHost, actor: "host", want: "invalid_payload"},
		{name: "transfer missing", handler: (*Server).handleTransferHost, msgType: MsgTypeTransferHost, body: &TransferHostPayload{}, actor: "host", want: "missing_user_id"},
		{name: "transfer non-host", handler: (*Server).handleTransferHost, msgType: MsgTypeTransferHost, body: &TransferHostPayload{NewHostID: "host"}, actor: "guest", want: "not_host"},
		{name: "transfer self", handler: (*Server).handleTransferHost, msgType: MsgTypeTransferHost, body: &TransferHostPayload{NewHostID: "host"}, actor: "host", want: "cannot_transfer_to_self"},
		{name: "transfer unknown", handler: (*Server).handleTransferHost, msgType: MsgTypeTransferHost, body: &TransferHostPayload{NewHostID: "unknown"}, actor: "host", want: "user_not_found"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, _, host, guest := roomTestFixture()
			actor := host
			if tc.actor == "guest" {
				actor = guest
			}
			payload := []byte{0xff}
			if tc.body != nil {
				payload = encodeTestPayload(t, tc.msgType, tc.body)
			}
			tc.handler(server, actor, payload)
			receiveRoomError(t, actor, tc.want)
		})
	}
}

func TestLeaveRoomEmptyAndGuest(t *testing.T) {
	t.Run("sole member leaves empty room", func(t *testing.T) {
		server, room, host, guest := roomTestFixture()
		delete(room.Clients, "guest")
		room.State.Users = room.State.Users[:1]
		guest.setRoom(nil)
		host.setSessionToken("host-token")
		server.sessions["host-token"] = &Session{UserID: "host", RoomCode: room.Code}

		server.leaveRoom(host)
		if host.currentRoom() != nil || room.EmptySince == nil || len(room.Clients) != 0 || len(room.State.Users) != 0 {
			t.Fatalf("room was not marked empty: empty=%v clients=%d users=%#v", room.EmptySince, len(room.Clients), room.State.Users)
		}
		if _, exists := server.sessions["host-token"]; exists {
			t.Fatal("sole member session was not removed")
		}
	})

	t.Run("guest leaves active room", func(t *testing.T) {
		server, room, host, guest := roomTestFixture()
		guest.setSessionToken("guest-token")
		server.sessions["guest-token"] = &Session{UserID: "guest", RoomCode: room.Code}
		room.BufferingUsers["guest"] = true
		room.DisconnectedUsers["guest"] = &Session{UserID: "guest"}

		server.leaveRoom(guest)
		if guest.currentRoom() != nil || room.Host != host || room.EmptySince != nil || len(room.State.Users) != 1 || room.BufferingUsers["guest"] {
			t.Fatalf("unexpected room after guest left: %#v", room)
		}
		if _, exists := server.sessions["guest-token"]; exists {
			t.Fatal("guest session was not removed")
		}
		var left pb.UserLeftPayload
		if got := receiveTestMessage(t, host, &left); got != MsgTypeUserLeft || left.UserId != "guest" || left.Username != "Guest" {
			t.Fatalf("unexpected guest departure: type=%q payload=%#v", got, &left)
		}
	})

	server := testServer()
	server.leaveRoom(newClient("outside", nil))
}
