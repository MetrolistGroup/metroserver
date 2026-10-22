package server

import (
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/MetrolistGroup/metroserver/proto"
	"google.golang.org/protobuf/proto"
)

func playbackTestRoom() (*Server, *Client, *Client, *Room) {
	server := testServer()
	host := newClient("host", nil)
	host.setUsername("Host")
	guest := newClient("guest", nil)
	guest.setUsername("Guest")
	room := &Room{
		Code:    "ROOM1234",
		Host:    host,
		Clients: map[string]*Client{"host": host, "guest": guest},
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
		{name: "pause past reported duration", payload: PlaybackActionPayload{Action: ActionPause, Position: 1200}, playing: true, wantPosition: 1200, wantBroadcast: 1200, wantVolume: 0.5},
		{name: "seek fills track ID", payload: PlaybackActionPayload{Action: ActionSeek, Position: 1200}, playing: true, wantPosition: 1200, wantBroadcast: 1200, wantVolume: 0.5},
		{name: "seek clamps to maximum", payload: PlaybackActionPayload{Action: ActionSeek, Position: MaxTrackDuration + 1}, playing: false, wantPosition: MaxTrackDuration, wantBroadcast: MaxTrackDuration, wantVolume: 0.5},
		{name: "set volume", payload: PlaybackActionPayload{Action: ActionSetVolume, Volume: 0.75}, playing: false, wantPosition: 0, wantVolume: 0.75},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, host, guest, room := playbackTestRoom()
			room.State.IsPlaying = tt.playing
			before := serverNowMs()
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
	track := &TrackInfo{ID: "next", Title: " Next Track ", Duration: 2000}

	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{
		Action:    ActionChangeTrack,
		TrackInfo: track,
	}))

	if room.State.CurrentTrack.ID != "next" || room.State.CurrentTrack.Title != "Next Track" {
		t.Fatalf("track was not sanitized into state: %#v", room.State.CurrentTrack)
	}
	if room.State.Position != 0 || room.State.IsPlaying || room.TrackRevision != 1 {
		t.Fatalf("unexpected transition state: %#v", room.State)
	}

	changed := requirePlaybackMessage(t, guest, ActionChangeTrack)
	if changed.TrackInfo == nil || changed.TrackInfo.Id != "next" {
		t.Fatalf("unexpected track-change message: %#v", &changed)
	}
	if changed.Revision != 1 || changed.ServerTime == 0 {
		t.Fatalf("track change timing metadata = revision %d, server time %d", changed.Revision, changed.ServerTime)
	}
	select {
	case message := <-guest.Send:
		t.Fatalf("unexpected duplicate transition message: %x", message)
	default:
	}
}

func TestPlaybackSkipWaitsForCanonicalTrackChange(t *testing.T) {
	server, host, guest, room := playbackTestRoom()
	room.State.IsPlaying = true
	room.State.Position = 500
	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionSkipNext}))
	if room.State.Position != 500 || room.State.Revision != 0 {
		t.Fatalf("skip mutated authoritative state: %#v", room.State)
	}
	select {
	case message := <-guest.Send:
		t.Fatalf("skip was broadcast before canonical track change: %x", message)
	default:
	}
}

func TestPlaybackActionCompensatesTransitAndAdvancesRevision(t *testing.T) {
	server, host, guest, room := playbackTestRoom()
	room.State.Revision = 7
	capturedAt := time.Now().Add(-200 * time.Millisecond).UnixMilli()
	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{
		Action: ActionPlay, TrackID: "current", Position: 100, CapturedAtServerTime: capturedAt,
	}))

	if room.State.Position < 250 || room.State.Position > 450 {
		t.Fatalf("transit-compensated position = %d, want approximately 300", room.State.Position)
	}
	if room.State.Revision != 8 || room.State.LastUpdate == 0 {
		t.Fatalf("state timing metadata = revision %d, update %d", room.State.Revision, room.State.LastUpdate)
	}
	play := requirePlaybackMessage(t, guest, ActionPlay)
	if play.Revision != 8 || play.ServerTime != room.State.LastUpdate || play.Position != room.State.Position {
		t.Fatalf("broadcast does not match authoritative state: %#v", play)
	}
}

func TestPlaybackActionRejectsStaleTrack(t *testing.T) {
	server, host, guest, room := playbackTestRoom()
	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{
		Action: ActionSeek, TrackID: "old", Position: 500,
	}))
	requireTestError(t, host, "stale_track")
	if room.State.Position != 0 || room.State.Revision != 0 {
		t.Fatalf("stale action mutated state: %#v", room.State)
	}
	select {
	case message := <-guest.Send:
		t.Fatalf("stale action was broadcast: %x", message)
	default:
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
		{ID: "valid", Title: " Valid ", Duration: -5},
		{ID: "valid", Title: "Repeat", Duration: 10},
		{ID: "current", Title: "Current track", Duration: 10},
		{ID: "", Title: "Invalid", Duration: 10},
		{ID: "bad\x01", Title: "Invalid", Duration: 10},
	}
	for i := 0; i < MaxQueueSize; i++ {
		syncQueue = append(syncQueue, TrackInfo{ID: fmt.Sprintf("bulk-%d", i), Title: "Bulk", Duration: 10})
	}
	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionSyncQueue, Queue: syncQueue}))
	broadcast := requirePlaybackMessage(t, guest, ActionSyncQueue)
	if len(room.State.Queue) != MaxQueueSize || len(broadcast.Queue) != MaxQueueSize {
		t.Fatalf("sanitized queue lengths = state %d, broadcast %d, want %d", len(room.State.Queue), len(broadcast.Queue), MaxQueueSize)
	}
	if room.State.Queue[0].ID != "valid" || room.State.Queue[0].Title != "Valid" || room.State.Queue[0].Duration != 0 {
		t.Fatalf("first queue item was not sanitized: %#v", room.State.Queue[0])
	}
	// The host's queue is mirrored as sent: repeats and a later replay of the
	// current track are intentional.
	if got := []string{room.State.Queue[1].ID, room.State.Queue[2].ID}; !reflect.DeepEqual(got, []string{"valid", "current"}) {
		t.Fatalf("queue = %v, want repeats preserved", got)
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
		{name: "volume NaN", code: "invalid_volume"},
		{name: "volume infinity", code: "invalid_volume"},
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
		"queue add invalid info":    {Action: ActionQueueAdd, TrackInfo: &TrackInfo{ID: "id\x00"}},
		"queue full":                {Action: ActionQueueAdd, TrackInfo: &TrackInfo{ID: "id", Title: "Title"}},
		"queue remove missing ID":   {Action: ActionQueueRemove},
		"volume below range":        {Action: ActionSetVolume, Volume: -0.1},
		"volume above range":        {Action: ActionSetVolume, Volume: 1.1},
		"volume NaN":                {Action: ActionSetVolume, Volume: math.NaN()},
		"volume infinity":           {Action: ActionSetVolume, Volume: math.Inf(1)},
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
			room.State.IsPlaying = tt.playing
			room.State.Position = 250
			room.State.LastUpdate = time.Now().Add(time.Hour).UnixMilli()

			server.handleBufferReady(guest, encodeTestPayload(t, MsgTypeBufferReady, &BufferReadyPayload{TrackID: "current"}))

			seek := requirePlaybackMessage(t, guest, ActionSeek)
			if seek.TrackId != "current" || seek.Position != 250 {
				t.Fatalf("unexpected seek: %#v", &seek)
			}
			stateAction := ActionPause
			if tt.playing {
				stateAction = ActionPlay
			}
			state := requirePlaybackMessage(t, guest, stateAction)
			if state.Position != 250 || state.ServerTime == 0 {
				t.Fatalf("state position/time = %d/%d, want 250/nonzero", state.Position, state.ServerTime)
			}
			var complete pb.BufferCompletePayload
			if msgType := receiveTestMessage(t, guest, &complete); msgType != MsgTypeBufferComplete || complete.TrackId != "current" {
				t.Fatalf("unexpected buffer completion: type=%q payload=%#v", msgType, &complete)
			}
		})
	}
}

func TestBufferReadyFromHostOnlyCompletes(t *testing.T) {
	server, host, _, room := playbackTestRoom()
	room.State.IsPlaying = true
	room.State.Position = 600
	server.handleBufferReady(host, encodeTestPayload(t, MsgTypeBufferReady, &BufferReadyPayload{TrackID: "current"}))

	var complete pb.BufferCompletePayload
	if msgType := receiveTestMessage(t, host, &complete); msgType != MsgTypeBufferComplete || complete.TrackId != "current" {
		t.Fatalf("host got %q, want only buffer_complete", msgType)
	}
}

func TestBufferReadyRejectsStaleTrackRevision(t *testing.T) {
	server, _, guest, room := playbackTestRoom()
	room.TrackRevision = 5
	server.handleBufferReady(guest, encodeTestPayload(t, MsgTypeBufferReady, &BufferReadyPayload{TrackID: "current", TrackRevision: 4}))
	requireTestError(t, guest, "stale_track")
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
		{name: "stale track", code: "stale_track"},
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
				} else if tt.name == "stale track" {
					trackID = "old"
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
	room.State.Revision = 9

	server.handleRequestSync(guest)

	var sync pb.SyncStatePayload
	if msgType := receiveTestMessage(t, guest, &sync); msgType != MsgTypeSyncState {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeSyncState)
	}
	if sync.CurrentTrack == nil || sync.CurrentTrack.Id != "current" || !sync.IsPlaying || sync.Position < 1300 || sync.Position > 1500 {
		t.Fatalf("unexpected sync state: %#v", &sync)
	}
	if sync.LastUpdate == 0 || sync.Volume != float32(0.8) || len(sync.Queue) != 1 || sync.Queue[0].Id != "queued" || sync.Revision != 9 {
		t.Fatalf("incomplete sync state: %#v", &sync)
	}
}

func TestRequestSyncDropsRepeatedRequests(t *testing.T) {
	server, _, guest, _ := playbackTestRoom()
	server.handleRequestSync(guest)
	receiveTestMessage(t, guest, nil)

	server.handleRequestSync(guest)
	if got := len(guest.Send); got != 0 {
		t.Fatalf("repeated sync queued %d responses, want 0", got)
	}
}

func TestRequestSyncRejectsClientOutsideRoom(t *testing.T) {
	server := testServer()
	client := newClient("outside", nil)
	server.handleRequestSync(client)
	requireTestError(t, client, "not_in_room")
}

func TestRequestSyncRejectsStaleRoomReference(t *testing.T) {
	server, _, guest, room := playbackTestRoom()
	replacement := newClient(guest.clientID(), nil)
	room.Clients[guest.clientID()] = replacement

	server.handleRequestSync(guest)

	requireTestError(t, guest, "not_in_room")
}

func TestConcurrentPlaybackAndSnapshotsAreDeliveredInRevisionOrder(t *testing.T) {
	server, host, guest, room := playbackTestRoom()
	const count = 20
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		payload := encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{
			Action: ActionSetVolume, Volume: float64(i) / count,
		})
		wg.Add(2)
		go func(payload []byte) {
			defer wg.Done()
			server.handlePlaybackAction(host, payload)
		}(payload)
		go func() {
			defer wg.Done()
			server.handleRequestSync(guest)
		}()
	}
	wg.Wait()

	codec := NewMessageCodec(true)
	var lastRevision uint64
	for i := 0; i < count+1; i++ {
		select {
		case data := <-guest.Send:
			msgType, payload, err := codec.Decode(data)
			if err != nil {
				t.Fatal(err)
			}
			var revision uint64
			switch msgType {
			case MsgTypeSyncPlayback:
				var action pb.PlaybackActionPayload
				if err := proto.Unmarshal(payload, &action); err != nil {
					t.Fatal(err)
				}
				revision = action.Revision
			case MsgTypeSyncState:
				var state pb.SyncStatePayload
				if err := proto.Unmarshal(payload, &state); err != nil {
					t.Fatal(err)
				}
				revision = state.Revision
			default:
				t.Fatalf("unexpected message type %q", msgType)
			}
			if revision < lastRevision {
				t.Fatalf("revision moved backwards: %d after %d", revision, lastRevision)
			}
			lastRevision = revision
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for concurrent messages")
		}
	}
	if room.State.Revision != count {
		t.Fatalf("final revision = %d, want %d", room.State.Revision, count)
	}
}

func TestPlaybackActionAuthorizesBeforeDecodingQueue(t *testing.T) {
	server, _, guest, _ := playbackTestRoom()
	outside := newClient("outside", nil)

	server.handlePlaybackAction(outside, []byte{0xff})
	requireTestError(t, outside, "not_in_room")

	server.handlePlaybackAction(guest, []byte{0xff})
	requireTestError(t, guest, "not_host")
}

func TestPlaybackActionSanitizesQueueTitle(t *testing.T) {
	server, host, guest, _ := playbackTestRoom()
	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{
		Action:     ActionSyncQueue,
		QueueTitle: strings.Repeat("x", MaxTrackTitleLength+50) + "\x01",
	}))

	var action pb.PlaybackActionPayload
	if msgType := receiveTestMessage(t, guest, &action); msgType != MsgTypeSyncPlayback {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeSyncPlayback)
	}
	if action.QueueTitle != strings.Repeat("x", MaxTrackTitleLength) {
		t.Fatalf("queue title length = %d, want %d sanitized bytes", len(action.QueueTitle), MaxTrackTitleLength)
	}
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
		server.handleMessage(host, message, serverNowMs())
		if msgType := receiveTestMessage(t, host, nil); msgType != MsgTypePong {
			t.Fatalf("message type = %q, want %q", msgType, MsgTypePong)
		}
	})

	t.Run("timestamped ping", func(t *testing.T) {
		before := serverNowMs()
		message, err := codec.Encode(MsgTypePing, PingPayload{ClientTime: 1234, Sequence: 7})
		if err != nil {
			t.Fatal(err)
		}
		server.handleMessage(host, message, serverNowMs())
		var pong pb.PongPayload
		if msgType := receiveTestMessage(t, host, &pong); msgType != MsgTypePong {
			t.Fatalf("message type = %q, want %q", msgType, MsgTypePong)
		}
		if pong.ClientTime != 1234 || pong.Sequence != 7 || pong.ServerReceiveTime < before || pong.ServerSendTime < pong.ServerReceiveTime {
			t.Fatalf("invalid pong timing sample: %#v", &pong)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		message, err := codec.Encode("not_a_message", nil)
		if err != nil {
			t.Fatal(err)
		}
		server.handleMessage(host, message, serverNowMs())
		requireTestError(t, host, "unknown_message_type")
	})

	t.Run("invalid envelope", func(t *testing.T) {
		server.handleMessage(host, []byte{0xff}, serverNowMs())
		requireTestError(t, host, "invalid_message")
	})

	t.Run("missing type", func(t *testing.T) {
		message, err := codec.Encode("", nil)
		if err != nil {
			t.Fatal(err)
		}
		server.handleMessage(host, message, serverNowMs())
		requireTestError(t, host, "invalid_message")
	})

	t.Run("playback dispatch", func(t *testing.T) {
		message, err := codec.Encode(MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionSetVolume, Volume: 0.25})
		if err != nil {
			t.Fatal(err)
		}
		server.handleMessage(host, message, serverNowMs())
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
		if !response.SupportsProtobuf || !response.SupportsCompression || response.ServerVersion != "2" || len(response.Features) != 0 {
			t.Fatalf("unexpected server capabilities: %#v", &response)
		}
	})

	t.Run("client can disable compression", func(t *testing.T) {
		server := testServer()
		client := newClient("client", nil)
		payload := encodeTestPayload(t, MsgTypeClientCapabilities, &ClientCapabilitiesPayload{SupportsProtobuf: true})
		server.handleClientCapabilities(client, payload)
		receiveTestMessage(t, client, nil)

		encoded, err := client.codec.Encode(MsgTypeJoinRejected, JoinRejectedPayload{Reason: strings.Repeat("x", 1000)})
		if err != nil {
			t.Fatal(err)
		}
		var envelope pb.Envelope
		if err := proto.Unmarshal(encoded, &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Compressed {
			t.Fatal("server compressed a message after the client declined compression")
		}
	})

	t.Run("protobuf required", func(t *testing.T) {
		server := testServer()
		client := newClient("client", nil)
		payload := encodeTestPayload(t, MsgTypeClientCapabilities, &ClientCapabilitiesPayload{})
		server.handleClientCapabilities(client, payload)
		requireTestError(t, client, "unsupported_client")
	})

	t.Run("capabilities cannot be renegotiated", func(t *testing.T) {
		server := testServer()
		client := newClient("client", nil)
		payload := encodeTestPayload(t, MsgTypeClientCapabilities, &ClientCapabilitiesPayload{SupportsProtobuf: true, SupportsCompression: true})
		server.handleClientCapabilities(client, payload)
		receiveTestMessage(t, client, nil)
		server.handleClientCapabilities(client, payload)
		requireTestError(t, client, "capabilities_already_set")
	})

	t.Run("capabilities must precede room membership", func(t *testing.T) {
		server := testServer()
		client := newClient("client", nil)
		room := &Room{}
		client.setRoom(room)
		client.clearRoom(room)
		payload := encodeTestPayload(t, MsgTypeClientCapabilities, &ClientCapabilitiesPayload{SupportsProtobuf: true})
		server.handleClientCapabilities(client, payload)
		requireTestError(t, client, "capabilities_too_late")
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
