package server

import (
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"testing"
	"time"

	pb "github.com/MetrolistGroup/metroserver/proto"
)

func playbackTestRoom() (*Server, *Client, *Client, *Room) {
	server := testServer()
	host := newClient("host", nil)
	host.setUsername("Host")
	guest := newClient("guest", nil)
	guest.setUsername("Guest")
	room := &Room{
		Code:           "ROOM1234",
		Host:           host,
		Clients:        map[string]*Client{"host": host, "guest": guest},
		BufferingUsers: make(map[string]bool),
		State: &RoomState{
			RoomCode:     "ROOM1234",
			HostID:       "host",
			CurrentTrack: &TrackInfo{ID: "current", Title: "Current", Duration: 1000},
			Volume:       0.5,
		},
	}
	host.setRoom(room)
	guest.setRoom(room)
	return server, host, guest, room
}

func requireTestError(t *testing.T, client *Client, code string) {
	t.Helper()
	var response pb.ErrorPayload
	if msgType := receiveTestMessage(t, client, &response); msgType != MsgTypeError {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeError)
	}
	if response.Code != code {
		t.Fatalf("error code = %q, want %q", response.Code, code)
	}
}

func requirePlaybackMessage(t *testing.T, client *Client, action string) *pb.PlaybackActionPayload {
	t.Helper()
	var response pb.PlaybackActionPayload
	if msgType := receiveTestMessage(t, client, &response); msgType != MsgTypeSyncPlayback {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeSyncPlayback)
	}
	if response.Action != action {
		t.Fatalf("action = %q, want %q", response.Action, action)
	}
	return &response
}

func TestPlaybackActionRemainingPositionAndVolumeBranches(t *testing.T) {
	tests := []struct {
		name          string
		payload       PlaybackActionPayload
		playing       bool
		wantPosition  int64
		wantBroadcast int64
		wantVolume    float64
	}{
		{name: "pause clamps", payload: PlaybackActionPayload{Action: ActionPause, Position: 1200}, playing: true, wantPosition: 1000, wantBroadcast: 1000, wantVolume: 0.5},
		{name: "seek clamps and fills track ID", payload: PlaybackActionPayload{Action: ActionSeek, Position: 1200}, playing: true, wantPosition: 1000, wantBroadcast: 1000, wantVolume: 0.5},
		{name: "skip next", payload: PlaybackActionPayload{Action: ActionSkipNext, Position: 900}, playing: true, wantPosition: 0, wantBroadcast: 900, wantVolume: 0.5},
		{name: "skip previous", payload: PlaybackActionPayload{Action: ActionSkipPrev, Position: 900}, playing: true, wantPosition: 0, wantBroadcast: 900, wantVolume: 0.5},
		{name: "set volume", payload: PlaybackActionPayload{Action: ActionSetVolume, Volume: 0.75}, playing: false, wantPosition: 0, wantVolume: 0.75},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, host, guest, room := playbackTestRoom()
			room.State.IsPlaying = tt.playing
			before := time.Now().UnixMilli()
			server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &tt.payload))

			if room.State.Position != tt.wantPosition {
				t.Fatalf("position = %d, want %d", room.State.Position, tt.wantPosition)
			}
			if room.State.Volume != tt.wantVolume {
				t.Fatalf("volume = %v, want %v", room.State.Volume, tt.wantVolume)
			}
			if tt.payload.Action == ActionPause && room.State.IsPlaying {
				t.Fatal("pause left room playing")
			}
			if tt.payload.Action != ActionSetVolume && room.State.LastUpdate < before {
				t.Fatal("last update was not refreshed")
			}

			broadcast := requirePlaybackMessage(t, guest, tt.payload.Action)
			if (tt.payload.Action == ActionPause || tt.payload.Action == ActionSeek) && broadcast.TrackId != "current" {
				t.Fatalf("track ID = %q, want current", broadcast.TrackId)
			}
			if broadcast.Position != tt.wantBroadcast {
				t.Fatalf("broadcast position = %d, want %d", broadcast.Position, tt.wantBroadcast)
			}
		})
	}
}

func TestPlaybackActionChangeTrackSendsTransitionSequence(t *testing.T) {
	server, host, guest, room := playbackTestRoom()
	room.State.IsPlaying = true
	room.BufferingUsers = map[string]bool{"guest": true}
	track := &TrackInfo{ID: " next ", Title: " Next Track ", Duration: 2000}

	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{
		Action:    ActionChangeTrack,
		TrackInfo: track,
	}))

	if room.State.CurrentTrack.ID != "next" || room.State.CurrentTrack.Title != "Next Track" {
		t.Fatalf("track was not sanitized into state: %#v", room.State.CurrentTrack)
	}
	if room.State.Position != 0 || room.State.IsPlaying || room.HostStartPosition != 0 {
		t.Fatalf("unexpected transition state: %#v", room.State)
	}
	if room.BufferingUsers != nil {
		t.Fatal("track change did not disable buffering tracking")
	}

	changed := requirePlaybackMessage(t, guest, ActionChangeTrack)
	if changed.TrackInfo == nil || changed.TrackInfo.Id != "next" {
		t.Fatalf("unexpected track-change message: %#v", &changed)
	}
	paused := requirePlaybackMessage(t, guest, ActionPause)
	if paused.TrackId != "next" || paused.Position != 0 {
		t.Fatalf("unexpected pause message: %#v", &paused)
	}
	var complete pb.BufferCompletePayload
	if msgType := receiveTestMessage(t, guest, &complete); msgType != MsgTypeBufferComplete || complete.TrackId != "next" {
		t.Fatalf("unexpected buffer-complete message: type=%q payload=%#v", msgType, &complete)
	}
	seek := requirePlaybackMessage(t, guest, ActionSeek)
	if seek.TrackId != "next" || seek.Position != 0 {
		t.Fatalf("unexpected seek message: %#v", &seek)
	}
}

func TestPlaybackActionQueueMutationsAndSyncSanitization(t *testing.T) {
	server, host, guest, room := playbackTestRoom()
	room.State.Queue = []TrackInfo{{ID: "old", Title: "Old", Duration: 100}}

	actions := []PlaybackActionPayload{
		{Action: ActionQueueAdd, TrackInfo: &TrackInfo{ID: "tail", Title: "Tail", Duration: 200}},
		{Action: ActionQueueAdd, TrackInfo: &TrackInfo{ID: "next", Title: "Next", Duration: 300}, InsertNext: true},
		{Action: ActionQueueRemove, TrackID: "old"},
	}
	for _, action := range actions {
		server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &action))
		requirePlaybackMessage(t, guest, action.Action)
	}
	if got := []string{room.State.Queue[0].ID, room.State.Queue[1].ID}; !reflect.DeepEqual(got, []string{"next", "tail"}) {
		t.Fatalf("queue IDs = %v, want [next tail]", got)
	}

	syncQueue := []TrackInfo{
		{ID: " valid ", Title: " Valid ", Duration: 0},
		{ID: "", Title: "Invalid", Duration: 10},
	}
	for i := len(syncQueue); i < MaxQueueSize+1; i++ {
		syncQueue = append(syncQueue, TrackInfo{ID: "bulk", Title: "Bulk", Duration: 10})
	}
	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionSyncQueue, Queue: syncQueue}))
	broadcast := requirePlaybackMessage(t, guest, ActionSyncQueue)
	if len(room.State.Queue) != MaxQueueSize-1 || len(broadcast.Queue) != MaxQueueSize-1 {
		t.Fatalf("sanitized queue lengths = state %d, broadcast %d, want %d", len(room.State.Queue), len(broadcast.Queue), MaxQueueSize-1)
	}
	if room.State.Queue[0].ID != "valid" || room.State.Queue[0].Duration != 180000 {
		t.Fatalf("first queue item was not sanitized: %#v", room.State.Queue[0])
	}

	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionSyncQueue}))
	requirePlaybackMessage(t, guest, ActionSyncQueue)
	if len(room.State.Queue) != 0 {
		t.Fatalf("nil queue sync left %d items", len(room.State.Queue))
	}

	room.State.Queue = []TrackInfo{{ID: "again", Title: "Again"}}
	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionQueueClear}))
	requirePlaybackMessage(t, guest, ActionQueueClear)
	if room.State.Queue == nil || len(room.State.Queue) != 0 {
		t.Fatalf("queue clear produced %#v, want non-nil empty queue", room.State.Queue)
	}
}

func TestPlaybackActionValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		setup   func(*Client, *Room)
		code    string
	}{
		{name: "invalid payload", payload: []byte{0xff}, code: "invalid_payload"},
		{name: "missing action", code: "missing_action"},
		{name: "not in room", setup: func(host *Client, _ *Room) { host.setRoom(nil) }, code: "not_in_room"},
		{name: "disconnected host", setup: func(_ *Client, room *Room) { now := time.Now(); room.HostDisconnectedAt = &now }, code: "not_host"},
		{name: "play without track", setup: func(_ *Client, room *Room) { room.State.CurrentTrack = nil }, code: "no_track"},
		{name: "negative play", code: "invalid_position"},
		{name: "negative pause", code: "invalid_position"},
		{name: "negative seek", code: "invalid_position"},
		{name: "change track missing info", code: "missing_track_info"},
		{name: "change track invalid info", code: "invalid_track_info"},
		{name: "queue add missing info", code: "missing_track_info"},
		{name: "queue add invalid info", code: "invalid_track_info"},
		{name: "queue full", setup: func(_ *Client, room *Room) { room.State.Queue = make([]TrackInfo, MaxQueueSize) }, code: "queue_full"},
		{name: "queue remove missing ID", code: "missing_track_id"},
		{name: "volume below range", code: "invalid_volume"},
		{name: "volume above range", code: "invalid_volume"},
		{name: "unknown action", code: "unknown_action"},
	}

	payloads := map[string]*PlaybackActionPayload{
		"missing action":            {},
		"not in room":               {Action: ActionSeek},
		"disconnected host":         {Action: ActionSeek},
		"play without track":        {Action: ActionPlay},
		"negative play":             {Action: ActionPlay, Position: -1},
		"negative pause":            {Action: ActionPause, Position: -1},
		"negative seek":             {Action: ActionSeek, Position: -1},
		"change track missing info": {Action: ActionChangeTrack},
		"change track invalid info": {Action: ActionChangeTrack, TrackInfo: &TrackInfo{Title: "No ID"}},
		"queue add missing info":    {Action: ActionQueueAdd},
		"queue add invalid info":    {Action: ActionQueueAdd, TrackInfo: &TrackInfo{ID: "id"}},
		"queue full":                {Action: ActionQueueAdd, TrackInfo: &TrackInfo{ID: "id", Title: "Title"}},
		"queue remove missing ID":   {Action: ActionQueueRemove},
		"volume below range":        {Action: ActionSetVolume, Volume: -0.1},
		"volume above range":        {Action: ActionSetVolume, Volume: 1.1},
		"unknown action":            {Action: "rewind"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, host, _, room := playbackTestRoom()
			if tt.setup != nil {
				tt.setup(host, room)
			}
			payload := tt.payload
			if payload == nil {
				payload = encodeTestPayload(t, MsgTypePlaybackAction, payloads[tt.name])
			}
			server.handlePlaybackAction(host, payload)
			requireTestError(t, host, tt.code)
		})
	}
}

func TestBufferReadyDisabledSendsPerClientSync(t *testing.T) {
	tests := []struct {
		name    string
		playing bool
	}{
		{name: "paused"},
		{name: "playing", playing: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _, guest, room := playbackTestRoom()
			room.BufferingUsers = nil
			room.State.IsPlaying = tt.playing
			room.State.Position = 250
			room.State.LastUpdate = time.Now().Add(time.Hour).UnixMilli()

			server.handleBufferReady(guest, encodeTestPayload(t, MsgTypeBufferReady, &BufferReadyPayload{TrackID: "stale"}))

			var complete pb.BufferCompletePayload
			if msgType := receiveTestMessage(t, guest, &complete); msgType != MsgTypeBufferComplete || complete.TrackId != "current" {
				t.Fatalf("unexpected buffer completion: type=%q payload=%#v", msgType, &complete)
			}
			seek := requirePlaybackMessage(t, guest, ActionSeek)
			if seek.TrackId != "current" || seek.Position != 250 {
				t.Fatalf("unexpected seek: %#v", &seek)
			}
			if tt.playing {
				play := requirePlaybackMessage(t, guest, ActionPlay)
				if play.Position != 250 {
					t.Fatalf("play position = %d, want 250", play.Position)
				}
			} else {
				select {
				case message := <-guest.Send:
					t.Fatalf("unexpected extra message: %x", message)
				default:
				}
			}
		})
	}
}

func TestBufferReadyEnabledWaitsThenCompletes(t *testing.T) {
	t.Run("waits for remaining user", func(t *testing.T) {
		server, host, guest, room := playbackTestRoom()
		room.BufferingUsers = map[string]bool{"guest": true, "other": true}
		server.handleBufferReady(guest, encodeTestPayload(t, MsgTypeBufferReady, &BufferReadyPayload{TrackID: "current"}))

		for _, client := range []*Client{host, guest} {
			var wait pb.BufferWaitPayload
			if msgType := receiveTestMessage(t, client, &wait); msgType != MsgTypeBufferWait {
				t.Fatalf("message type = %q, want %q", msgType, MsgTypeBufferWait)
			}
			if wait.TrackId != "current" || !reflect.DeepEqual(wait.WaitingFor, []string{"other"}) {
				t.Fatalf("unexpected wait payload: %#v", &wait)
			}
		}
	})

	t.Run("last user completes and resumes", func(t *testing.T) {
		server, host, guest, room := playbackTestRoom()
		room.BufferingUsers = map[string]bool{"guest": true}
		room.State.IsPlaying = true
		room.State.Position = 600
		server.handleBufferReady(guest, encodeTestPayload(t, MsgTypeBufferReady, &BufferReadyPayload{TrackID: "stale"}))

		if room.State.Position != 0 || room.State.LastUpdate == 0 {
			t.Fatalf("unexpected completed buffer state: %#v", room.State)
		}
		for _, client := range []*Client{host, guest} {
			var complete pb.BufferCompletePayload
			if msgType := receiveTestMessage(t, client, &complete); msgType != MsgTypeBufferComplete || complete.TrackId != "current" {
				t.Fatalf("unexpected completion: type=%q payload=%#v", msgType, &complete)
			}
			if seek := requirePlaybackMessage(t, client, ActionSeek); seek.TrackId != "current" || seek.Position != 0 {
				t.Fatalf("unexpected seek: %#v", &seek)
			}
			if play := requirePlaybackMessage(t, client, ActionPlay); play.TrackId != "current" || play.Position != 0 {
				t.Fatalf("unexpected play: %#v", &play)
			}
		}
	})
}

func TestBufferReadyValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		noRoom  bool
		code    string
	}{
		{name: "invalid payload", payload: []byte{0xff}, code: "invalid_payload"},
		{name: "missing track", code: "missing_track_id"},
		{name: "not in room", noRoom: true, code: "not_in_room"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _, guest, _ := playbackTestRoom()
			if tt.noRoom {
				guest.setRoom(nil)
			}
			payload := tt.payload
			if payload == nil {
				trackID := ""
				if tt.noRoom {
					trackID = "track"
				}
				payload = encodeTestPayload(t, MsgTypeBufferReady, &BufferReadyPayload{TrackID: trackID})
			}
			server.handleBufferReady(guest, payload)
			requireTestError(t, guest, tt.code)
		})
	}
}

func TestRequestSyncReturnsSnapshotAndLivePosition(t *testing.T) {
	server, _, guest, room := playbackTestRoom()
	room.State.IsPlaying = true
	room.State.Position = 345
	room.State.LastUpdate = time.Now().Add(-time.Second).UnixMilli()
	room.State.Volume = 0.8
	room.State.Queue = []TrackInfo{{ID: "queued", Title: "Queued", Duration: 100}}

	server.handleRequestSync(guest)

	var sync pb.SyncStatePayload
	if msgType := receiveTestMessage(t, guest, &sync); msgType != MsgTypeSyncState {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeSyncState)
	}
	if sync.CurrentTrack == nil || sync.CurrentTrack.Id != "current" || !sync.IsPlaying || sync.Position != 1000 {
		t.Fatalf("unexpected sync state: %#v", &sync)
	}
	if sync.LastUpdate == 0 || sync.Volume != float32(0.8) || len(sync.Queue) != 1 || sync.Queue[0].Id != "queued" {
		t.Fatalf("incomplete sync state: %#v", &sync)
	}
}

func TestRequestSyncRejectsClientOutsideRoom(t *testing.T) {
	server := testServer()
	client := newClient("outside", nil)
	server.handleRequestSync(client)
	requireTestError(t, client, "not_in_room")
}

func TestServerIdentifierAndTokenGeneration(t *testing.T) {
	server := testServer()
	server.rng = rand.New(rand.NewSource(7))

	roomCode := server.generateRoomCode()
	if matched := regexp.MustCompile(`^[0-9A-Z]{8}$`).MatchString(roomCode); !matched {
		t.Fatalf("room code %q does not have the expected format", roomCode)
	}
	userID1 := server.generateUserID()
	userID2 := server.generateUserID()
	if matched := regexp.MustCompile(`^user_[0-9]+_[0-9]+$`).MatchString(userID1); !matched {
		t.Fatalf("user ID %q does not have the expected format", userID1)
	}
	if userID1 == userID2 {
		t.Fatalf("generated duplicate user IDs: %q", userID1)
	}
	token1 := server.generateSessionToken()
	token2 := server.generateSessionToken()
	if matched := regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(token1); !matched {
		t.Fatalf("session token %q does not have the expected format", token1)
	}
	if token1 == token2 {
		t.Fatal("generated duplicate session tokens")
	}
}

func TestHandleMessagePingUnknownInvalidAndDispatch(t *testing.T) {
	server, host, _, room := playbackTestRoom()
	codec := NewMessageCodec(false)

	t.Run("ping", func(t *testing.T) {
		message, err := codec.Encode(MsgTypePing, nil)
		if err != nil {
			t.Fatal(err)
		}
		server.handleMessage(host, message)
		if msgType := receiveTestMessage(t, host, nil); msgType != MsgTypePong {
			t.Fatalf("message type = %q, want %q", msgType, MsgTypePong)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		message, err := codec.Encode("not_a_message", nil)
		if err != nil {
			t.Fatal(err)
		}
		server.handleMessage(host, message)
		requireTestError(t, host, "unknown_message_type")
	})

	t.Run("invalid envelope", func(t *testing.T) {
		server.handleMessage(host, []byte{0xff})
		requireTestError(t, host, "invalid_message")
	})

	t.Run("missing type", func(t *testing.T) {
		message, err := codec.Encode("", nil)
		if err != nil {
			t.Fatal(err)
		}
		server.handleMessage(host, message)
		requireTestError(t, host, "invalid_message")
	})

	t.Run("playback dispatch", func(t *testing.T) {
		message, err := codec.Encode(MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionSetVolume, Volume: 0.25})
		if err != nil {
			t.Fatal(err)
		}
		server.handleMessage(host, message)
		if room.State.Volume != 0.25 {
			t.Fatalf("dispatched volume = %v, want 0.25", room.State.Volume)
		}
		requirePlaybackMessage(t, host, ActionSetVolume)
	})
}

func TestClientCapabilitiesResponses(t *testing.T) {
	t.Run("supported", func(t *testing.T) {
		server := testServer()
		client := newClient("client", nil)
		payload := encodeTestPayload(t, MsgTypeClientCapabilities, &ClientCapabilitiesPayload{SupportsProtobuf: true})
		server.handleClientCapabilities(client, payload)

		var response pb.ServerCapabilities
		if msgType := receiveTestMessage(t, client, &response); msgType != MsgTypeServerCapabilities {
			t.Fatalf("message type = %q, want %q", msgType, MsgTypeServerCapabilities)
		}
		if !response.SupportsProtobuf || !response.SupportsCompression || response.ServerVersion != "1" {
			t.Fatalf("unexpected server capabilities: %#v", &response)
		}
	})

	t.Run("protobuf required", func(t *testing.T) {
		server := testServer()
		client := newClient("client", nil)
		payload := encodeTestPayload(t, MsgTypeClientCapabilities, &ClientCapabilitiesPayload{})
		server.handleClientCapabilities(client, payload)
		requireTestError(t, client, "unsupported_client")
	})

	t.Run("invalid payload", func(t *testing.T) {
		server := testServer()
		client := newClient("client", nil)
		server.handleClientCapabilities(client, []byte{0xff})
		requireTestError(t, client, "invalid_payload")
	})
}

func TestCloseAllClientsSkipsNilClientsAndConnections(t *testing.T) {
	server := testServer()
	client := newClient("without-connection", nil)
	server.clients[nil] = true
	server.clients[client] = true

	server.closeAllClients()

	if client.isClosed() {
		t.Fatal("client without a connection should be skipped")
	}
}

func TestHandleWebSocketRejectsAtCapacityBeforeUpgrade(t *testing.T) {
	server := testServer()
	server.clients = make(map[*Client]bool, MaxClients)
	for i := 0; i < MaxClients; i++ {
		server.clients[new(Client)] = true
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/ws", nil)

	server.handleWebSocket(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if recorder.Body.String() != "server at connection capacity\n" {
		t.Fatalf("response body = %q", recorder.Body.String())
	}
}
