package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/MetrolistGroup/metroserver/proto"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func testUAPolicy(block, rickroll, advert, allow []string) *uaPolicy {
	return &uaPolicy{
		allow:       lowerPatterns(append(append([]string{}, defaultAllowUA...), allow...)),
		allowExact:  lowerPatterns(defaultAllowUAExact),
		block:       lowerPatterns(block),
		rickroll:    lowerPatterns(rickroll),
		advert:      lowerPatterns(advert),
		advertTitle: DefaultAdvertTitle,
	}
}

func markTier(client *Client, tier uaTier) *Client {
	client.policy = testUAPolicy(nil, nil, nil, nil)
	client.uaTier = tier
	return client
}

func TestUAPolicyResolvesTiers(t *testing.T) {
	policy := testUAPolicy([]string{"com.sporify.lite"}, []string{"com.nestmusic", "com.joymusic"}, nil, []string{"etherwave"})
	for _, test := range []struct {
		userAgent string
		want      uaTier
	}{
		{userAgent: "okhttp/4.12.0", want: uaAllow},
		{userAgent: "Ktor/client ktor-client/3", want: uaAllow},
		{userAgent: "com.metrolist.music", want: uaAllow},
		{userAgent: "com.nestmusic.music.debug", want: uaRickroll},
		{userAgent: "com.JoyMusic.App", want: uaRickroll},
		{userAgent: "com.sporify.lite", want: uaBlock},
		{userAgent: "Dart/3.12 (dart:io), EtherWave/1.8.1", want: uaAllow},
		{userAgent: "com.soundsphere.music", want: uaAdvert},
		{userAgent: "", want: uaAdvert},
	} {
		if got := policy.resolve(test.userAgent); got != test.want {
			t.Errorf("resolve(%q) = %v, want %v", test.userAgent, got, test.want)
		}
	}
	if uaTier(99).String() != "unknown" || uaRickroll.String() != "rickroll" {
		t.Fatal("tier naming broken")
	}
	if got := testUAPolicy([]string{"okhttp"}, nil, nil, nil).resolve("okhttp/4.12.0"); got != uaBlock {
		t.Fatalf("higher-precedence configured pattern resolved to %v, want block", got)
	}
}

func TestDefaultUAPolicyAllowlistUsesExactPackageNames(t *testing.T) {
	policy := defaultUAPolicy()
	for _, test := range []struct {
		userAgent string
		want      uaTier
	}{
		{userAgent: "okhttp/4.12.0", want: uaAllow},
		{userAgent: "ktor-client/2", want: uaAllow},
		{userAgent: "com.metrolist.music", want: uaAllow},
		{userAgent: "com.metrolist.music.debug", want: uaAllow},
		{userAgent: "com.nevar.nzik", want: uaAllow},
		{userAgent: "com.nevar.nzik.debug", want: uaAllow},
		{userAgent: "com.nevar.nzik.evil", want: uaAdvert},
		{userAgent: "com.joymusic.app", want: uaAdvert},
		{userAgent: "com.metrolist.musiiz", want: uaAdvert},
		{userAgent: "com.metrolist.music8", want: uaAdvert},
	} {
		if got := policy.resolve(test.userAgent); got != test.want {
			t.Errorf("resolve(%q) = %v, want %v", test.userAgent, got, test.want)
		}
	}
}

func TestNilUAPolicyAllowsEverything(t *testing.T) {
	var policy *uaPolicy
	if got := policy.resolve("com.joymusic.app"); got != uaAllow {
		t.Fatalf("nil policy resolved to %v, want allow", got)
	}
	// A client without a policy (unit tests, pre-upgrade) must pass through.
	client := newClient("c", nil)
	client.uaTier = uaRickroll
	original := realPlayback()
	if got := client.rewritePayload(original).(PlaybackActionPayload); got.QueueTitle != original.QueueTitle || got.TrackID != original.TrackID {
		t.Fatalf("policyless client payload was rewritten: %#v", got)
	}
}

func TestLoadUAPolicy(t *testing.T) {
	if got, err := loadUAPolicy(""); err != nil || got.advertTitle != DefaultAdvertTitle || len(got.block) != 0 {
		t.Fatalf("loadUAPolicy(\"\") = %+v, %v", got, err)
	}
	if _, err := loadUAPolicy(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("expected missing policy file to fail")
	}

	path := filepath.Join(t.TempDir(), "ua_policy.json")
	config := `{"block":["com.sporify.lite"],"rickroll":["com.nestmusic","com.joymusic"],` +
		`"advert":["org.cicada"],"allow":["com.metrolist.music"],"advert_title":"  Metrolist is better  "}`
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	policy, err := loadUAPolicy(path)
	if err != nil {
		t.Fatal(err)
	}
	if policy.advertTitle != "Metrolist is better" {
		t.Fatalf("advert title = %q", policy.advertTitle)
	}
	for _, test := range []struct {
		userAgent string
		want      uaTier
	}{
		{userAgent: "okhttp/4.12.0", want: uaAllow},
		{userAgent: "com.metrolist.music.debug", want: uaAllow},
		{userAgent: "com.nevar.nzik", want: uaAllow},
		{userAgent: "com.nevar.nzik.debug", want: uaAllow},
		{userAgent: "com.nevar.nzik.evil", want: uaAdvert},
		{userAgent: "com.nestmusic.music", want: uaRickroll},
		{userAgent: "org.cicada", want: uaAdvert},
		{userAgent: "com.sporify.lite", want: uaBlock},
		{userAgent: "com.recordlabs.music.debug", want: uaAdvert},
	} {
		if got := policy.resolve(test.userAgent); got != test.want {
			t.Errorf("resolve(%q) = %v, want %v", test.userAgent, got, test.want)
		}
	}
}

func TestLoadUAPolicyRejectsBadJSONAndKeepsDefaultTitle(t *testing.T) {
	broken := filepath.Join(t.TempDir(), "broken.json")
	if err := os.WriteFile(broken, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadUAPolicy(broken); err == nil {
		t.Fatal("expected malformed policy to fail")
	}

	for name, config := range map[string]string{
		"unknown field": `{"rickrol":["com.nestmusic"]}`,
		"second object": `{} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "invalid.json")
			if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadUAPolicy(path); err == nil {
				t.Fatalf("expected %s policy to fail", name)
			}
		})
	}

	minimal := filepath.Join(t.TempDir(), "minimal.json")
	if err := os.WriteFile(minimal, []byte(`{"rickroll":["com.nestmusic"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	policy, err := loadUAPolicy(minimal)
	if err != nil {
		t.Fatal(err)
	}
	if policy.advertTitle != DefaultAdvertTitle {
		t.Fatalf("advert title = %q, want default", policy.advertTitle)
	}
	if policy.resolve("okhttp/4.12.0") != uaAllow {
		t.Fatal("file allowlist replaced built-in first-party defaults")
	}
}

func realPlayback() PlaybackActionPayload {
	return PlaybackActionPayload{
		Action:     ActionChangeTrack,
		TrackID:    "real-id",
		QueueTitle: "Friday mix",
		TrackInfo:  &TrackInfo{ID: "real-id", Title: "Real Song", Artist: "Real Artist", Duration: 300000},
		Queue:      []TrackInfo{{ID: "next-id", Title: "Next Song", Duration: 200000}},
		Position:   42000,
	}
}

func TestAdvertTierOnlyRewritesQueueTitle(t *testing.T) {
	original := realPlayback()
	got := rewriteForUATier(uaAdvert, "Switch to Metrolist", original).(PlaybackActionPayload)

	if got.QueueTitle != "Switch to Metrolist" {
		t.Fatalf("queue title = %q", got.QueueTitle)
	}
	if got.TrackID != "real-id" || got.TrackInfo.Title != "Real Song" || got.Queue[0].Title != "Next Song" || got.Position != 42000 {
		t.Fatalf("advert tier touched tracks: %#v", got)
	}
	if original.QueueTitle != "Friday mix" {
		t.Fatalf("rewrite mutated the shared payload: %q", original.QueueTitle)
	}
}

func TestRickrollTierReplacesEveryTrack(t *testing.T) {
	original := realPlayback()
	original.Position = 400000
	got := rewriteForUATier(uaRickroll, DefaultAdvertTitle, original).(PlaybackActionPayload)

	if got.QueueTitle != DefaultAdvertTitle {
		t.Fatalf("queue title = %q", got.QueueTitle)
	}
	if got.TrackID != rickTrackID || got.TrackInfo.ID != rickTrackID || got.TrackInfo.Title != rickTrackTitle {
		t.Fatalf("current track not rickrolled: %#v", got.TrackInfo)
	}
	if len(got.Queue) != 1 || got.Queue[0].Title != rickTrackTitle {
		t.Fatalf("queue not rickrolled: %#v", got.Queue)
	}
	if got.Position != rickPosition(original.Position) {
		t.Fatalf("position = %d, want wrapped to %d", got.Position, rickPosition(original.Position))
	}
	if original.TrackID != "real-id" || original.Queue[0].Title != "Next Song" || original.Position != 400000 {
		t.Fatalf("rewrite mutated the shared payload: %#v", original)
	}
	if got.TrackInfo == original.TrackInfo {
		t.Fatal("rewrite returned the shared track pointer")
	}
}

func TestRickrollTierRewritesStateAndBufferMessages(t *testing.T) {
	roomState := &RoomState{
		CurrentTrack: &TrackInfo{ID: "real-id", Title: "Real Song"},
		Queue:        []TrackInfo{{ID: "next-id", Title: "Next Song"}},
		Position:     1000,
	}
	state := rewriteForUATier(uaRickroll, DefaultAdvertTitle, roomState).(*RoomState)
	if state.CurrentTrack.Title != rickTrackTitle || state.Queue[0].Title != rickTrackTitle {
		t.Fatalf("room state not rickrolled: %#v", state)
	}
	if roomState.CurrentTrack.Title != "Real Song" || roomState.Queue[0].Title != "Next Song" {
		t.Fatal("rewrite mutated the live room state")
	}
	if state == roomState {
		t.Fatal("rewrite returned the live room state pointer")
	}

	sync := rewriteForUATier(uaRickroll, DefaultAdvertTitle, SyncStatePayload{
		CurrentTrack: &TrackInfo{ID: "real-id", Title: "Real Song"},
	}).(SyncStatePayload)
	if sync.CurrentTrack.ID != rickTrackID {
		t.Fatalf("sync state not rickrolled: %#v", sync.CurrentTrack)
	}

	// Buffer messages must keep real track IDs for advert clients, or buffering
	// never completes; rickrolled clients see the placeholder consistently.
	if got := rewriteForUATier(uaAdvert, DefaultAdvertTitle, BufferWaitPayload{TrackID: "real-id"}).(BufferWaitPayload); got.TrackID != "real-id" {
		t.Fatalf("advert tier rewrote buffer track id: %#v", got)
	}
	if got := rewriteForUATier(uaRickroll, DefaultAdvertTitle, BufferWaitPayload{TrackID: "real-id"}).(BufferWaitPayload); got.TrackID != rickTrackID {
		t.Fatalf("buffer wait = %#v", got)
	}
	if got := rewriteForUATier(uaRickroll, DefaultAdvertTitle, BufferCompletePayload{TrackID: "real-id"}).(BufferCompletePayload); got.TrackID != rickTrackID {
		t.Fatalf("buffer complete = %#v", got)
	}

	for name, payload := range map[string]any{
		"received": SuggestionReceivedPayload{TrackInfo: &TrackInfo{ID: "real-id", Title: "Real Song"}},
		"approved": SuggestionApprovedPayload{TrackInfo: &TrackInfo{ID: "real-id", Title: "Real Song"}},
	} {
		t.Run(name+" suggestion", func(t *testing.T) {
			rewritten := rewriteForUATier(uaRickroll, DefaultAdvertTitle, payload)
			var track *TrackInfo
			switch value := rewritten.(type) {
			case SuggestionReceivedPayload:
				track = value.TrackInfo
			case SuggestionApprovedPayload:
				track = value.TrackInfo
			}
			if track == nil || track.ID != rickTrackID {
				t.Fatalf("suggestion was not rewritten: %#v", rewritten)
			}
		})
	}
}

func TestRickrollTierRewritesJoinAndReconnectState(t *testing.T) {
	newState := func() *RoomState {
		return &RoomState{
			CurrentTrack: &TrackInfo{ID: "real-id", Title: "Real Song", Duration: 300000},
			Queue:        []TrackInfo{{ID: "queued-id", Title: "Queued Song"}},
			Position:     900000,
		}
	}

	state := newState()
	join := rewriteForUATier(uaRickroll, DefaultAdvertTitle, JoinApprovedPayload{RoomCode: "ROOM1234", State: state}).(JoinApprovedPayload)
	if join.State.CurrentTrack.ID != rickTrackID || join.State.Queue[0].ID != rickTrackID || join.State.Position != rickPosition(state.Position) {
		t.Fatalf("join state not rickrolled: %#v", join.State)
	}
	if state.CurrentTrack.ID != "real-id" || state.Queue[0].ID != "queued-id" || state.Position != 900000 {
		t.Fatal("join rewrite mutated the live state")
	}

	reconnect := rewriteForUATier(uaRickroll, DefaultAdvertTitle, ReconnectedPayload{RoomCode: "ROOM1234", State: newState()}).(ReconnectedPayload)
	if reconnect.State.CurrentTrack.Title != rickTrackTitle {
		t.Fatalf("reconnect state not rickrolled: %#v", reconnect.State)
	}

	if got := rewriteForUATier(uaAdvert, DefaultAdvertTitle, JoinApprovedPayload{State: state}).(JoinApprovedPayload); got.State != state {
		t.Fatal("advert tier rewrote join state")
	}
	if got := rewriteForUATier(uaRickroll, DefaultAdvertTitle, ReconnectedPayload{}).(ReconnectedPayload); got.State != nil {
		t.Fatalf("nil state became %#v", got.State)
	}
}

func TestPassthroughTiersAndPayloads(t *testing.T) {
	pong := PongPayload{Sequence: 3}
	if got := rewriteForUATier(uaAllow, DefaultAdvertTitle, pong); !reflect.DeepEqual(got, pong) {
		t.Fatal("allow tier rewrote the payload")
	}
	if got := rewriteForUATier(uaBlock, DefaultAdvertTitle, pong); !reflect.DeepEqual(got, pong) {
		t.Fatal("block tier rewrote the payload")
	}
	if got := rewriteForUATier(uaRickroll, DefaultAdvertTitle, pong); !reflect.DeepEqual(got, pong) {
		t.Fatal("unrelated payload was rewritten")
	}
	if got := rewriteForUATier(uaAdvert, DefaultAdvertTitle, pong); !reflect.DeepEqual(got, pong) {
		t.Fatal("advert tier rewrote an unrelated payload")
	}
	var nilState *RoomState
	if got, ok := rewriteForUATier(uaRickroll, DefaultAdvertTitle, nilState).(*RoomState); !ok || got != nil {
		t.Fatalf("nil room state rewritten to %#v", got)
	}
	if got := rewriteForUATier(uaAllow, DefaultAdvertTitle, realPlayback()); !reflect.DeepEqual(got, realPlayback()) {
		t.Fatal("allow tier rewrote a playback payload")
	}
}

func TestRickrollQueueOfEmptyQueue(t *testing.T) {
	if got := rickQueue(nil); got != nil {
		t.Fatalf("rickQueue(nil) = %#v", got)
	}
	if got := rickPosition(rickTrackDuration); got != 0 {
		t.Fatalf("position at track boundary = %d, want 0", got)
	}
	if got := rickPosition(-1); got != 0 {
		t.Fatalf("negative position = %d, want 0", got)
	}
}

func TestRickMappedTrackID(t *testing.T) {
	state := &RoomState{CurrentTrack: &TrackInfo{ID: "real-id"}}
	if got := rickMappedTrackID(uaRickroll, rickTrackID, state); got != "real-id" {
		t.Fatalf("rickroll placeholder mapped to %q", got)
	}
	for _, test := range []struct {
		name string
		tier uaTier
		id   string
	}{
		{name: "allow tier", tier: uaAllow, id: rickTrackID},
		{name: "advert tier", tier: uaAdvert, id: rickTrackID},
		{name: "real track", tier: uaRickroll, id: "real-id"},
		{name: "empty track", tier: uaRickroll, id: ""},
	} {
		if got := rickMappedTrackID(test.tier, test.id, state); got != test.id {
			t.Errorf("%s: mapped %q to %q", test.name, test.id, got)
		}
	}
	if got := rickMappedTrackID(uaRickroll, rickTrackID, &RoomState{}); got != rickTrackID {
		t.Errorf("no current track: mapped to %q", got)
	}
	if got := rickMappedTrackID(uaRickroll, rickTrackID, nil); got != rickTrackID {
		t.Errorf("nil state: mapped to %q", got)
	}
}

func TestRickrollPlaceholderStaysInSync(t *testing.T) {
	server, _, guest, room := playbackTestRoom()
	guest.policy = testUAPolicy(nil, []string{"com.nestmusic"}, nil, nil)
	guest.uaTier = uaRickroll
	room.BufferingUsers = nil
	room.State.Position = 250

	// Our placeholder counts as the track the room is really on...
	if !validatePlaybackTrack(guest, server.logger, room.State, &PlaybackActionPayload{Action: ActionPlay, TrackID: rickTrackID}) {
		t.Fatal("rickroll placeholder rejected as stale")
	}
	// ...but a genuinely different track is still rejected.
	if validatePlaybackTrack(guest, server.logger, room.State, &PlaybackActionPayload{Action: ActionPlay, TrackID: "other-id"}) {
		t.Fatal("unrelated track id accepted")
	}
	requireTestError(t, guest, "stale_track")

	server.handleBufferReady(guest, encodeTestPayload(t, MsgTypeBufferReady, &BufferReadyPayload{TrackID: rickTrackID}))
	seek := requirePlaybackMessage(t, guest, ActionSeek)
	if seek.TrackId != rickTrackID {
		t.Fatalf("buffer-ready seek announced track %q, want the placeholder", seek.TrackId)
	}
	if seek.QueueTitle != DefaultAdvertTitle {
		t.Fatalf("queue title = %q, want the ad", seek.QueueTitle)
	}
}

func TestRickrollGuestRoomStateStaysReal(t *testing.T) {
	server, host, guest, room := playbackTestRoom()
	guest.policy = testUAPolicy(nil, []string{"com.nestmusic"}, nil, nil)
	guest.uaTier = uaRickroll
	host.policy = defaultUAPolicy()

	// The host's real track must survive: only what the rickrolled client is
	// told changes.
	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{
		Action:    ActionChangeTrack,
		TrackInfo: &TrackInfo{ID: "real-id", Title: "Real Song", Duration: 300000},
		Queue:     []TrackInfo{{ID: "queued-id", Title: "Queued Song", Duration: 100000}},
	}))

	requirePlaybackMessage(t, host, ActionChangeTrack)
	guestMessage := requirePlaybackMessage(t, guest, ActionChangeTrack)
	if guestMessage.TrackInfo.GetTitle() != rickTrackTitle {
		t.Fatalf("guest track = %q", guestMessage.TrackInfo.GetTitle())
	}
	if guestMessage.QueueTitle != DefaultAdvertTitle {
		t.Fatalf("guest queue title = %q", guestMessage.QueueTitle)
	}
	room.mu.RLock()
	defer room.mu.RUnlock()
	if room.State.CurrentTrack.ID != "real-id" || room.State.Queue[0].ID != "queued-id" {
		t.Fatalf("room state polluted by the rewrite: %#v", room.State)
	}
}

func TestBroadcastEncodesPerTier(t *testing.T) {
	policy := testUAPolicy(nil, []string{"com.nestmusic"}, nil, nil)
	allowClient := newClient("allow", nil)
	advertClient := markTier(newClient("advert", nil), uaAdvert)
	rickClient := markTier(newClient("rick", nil), uaRickroll)
	allowClient.policy = policy
	advertClient.policy = policy
	rickClient.policy = policy

	sendMessageToClients(zap.NewNop(), []*Client{allowClient, advertClient, rickClient}, MsgTypeSyncPlayback, realPlayback())

	for _, test := range []struct {
		name       string
		client     *Client
		wantTitle  string
		wantTrack  string
		wantQueued string
	}{
		{name: "allow", client: allowClient, wantTitle: "Friday mix", wantTrack: "Real Song", wantQueued: "Next Song"},
		{name: "advert", client: advertClient, wantTitle: DefaultAdvertTitle, wantTrack: "Real Song", wantQueued: "Next Song"},
		{name: "rickroll", client: rickClient, wantTitle: DefaultAdvertTitle, wantTrack: rickTrackTitle, wantQueued: rickTrackTitle},
	} {
		t.Run(test.name, func(t *testing.T) {
			var p pb.PlaybackActionPayload
			if msgType := receiveTestMessage(t, test.client, &p); msgType != MsgTypeSyncPlayback {
				t.Fatalf("message type = %q", msgType)
			}
			if p.GetQueueTitle() != test.wantTitle {
				t.Errorf("queue title = %q, want %q", p.GetQueueTitle(), test.wantTitle)
			}
			if got := p.GetTrackInfo().GetTitle(); got != test.wantTrack {
				t.Errorf("track title = %q, want %q", got, test.wantTrack)
			}
			if got := p.GetQueue()[0].GetTitle(); got != test.wantQueued {
				t.Errorf("queued title = %q, want %q", got, test.wantQueued)
			}
		})
	}
}

func TestDirectSendHonoursClientTier(t *testing.T) {
	server := testServer()
	client := markTier(newClient("rick", nil), uaRickroll)
	client.policy = testUAPolicy(nil, []string{"com.nestmusic"}, nil, nil)

	client.sendMessage(server.logger, MsgTypeSyncState, SyncStatePayload{
		CurrentTrack: &TrackInfo{ID: "real-id", Title: "Real Song", Duration: 300000},
		Position:     5000,
		Revision:     7,
	})

	var state pb.SyncStatePayload
	if msgType := receiveTestMessage(t, client, &state); msgType != MsgTypeSyncState {
		t.Fatalf("message type = %q", msgType)
	}
	if state.GetCurrentTrack().GetTitle() != rickTrackTitle {
		t.Fatalf("sync_state track = %q", state.GetCurrentTrack().GetTitle())
	}
	if state.GetRevision() != 7 || state.GetPosition() != 5000 {
		t.Fatalf("sync state fields lost: %#v", &state)
	}
}

func TestBlockedUserAgentIsDisconnectedWithoutRoomAccess(t *testing.T) {
	server := testServer()
	server.uaPolicy = testUAPolicy([]string{"go-http-client"}, nil, nil, nil)
	store, err := openDatabase(filepath.Join(t.TempDir(), "metroserver.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	server.database = store
	httpServer := httptest.NewServer(http.HandlerFunc(server.handleWebSocket))
	t.Cleanup(httpServer.Close)

	conn, err := dialTestWebSocket(httpServer.URL, "Go-HTTP-Client/1.1")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, message, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("blocked client received no error frame: %v", err)
	}
	msgType, payload, err := NewMessageCodec(true).Decode(message)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != MsgTypeError {
		t.Fatalf("message type = %q, want error", msgType)
	}
	var e pb.ErrorPayload
	if err := proto.Unmarshal(payload, &e); err != nil {
		t.Fatal(err)
	}
	if e.GetCode() != "blocked_client" {
		t.Fatalf("error code = %q", e.GetCode())
	}
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("blocked client connection stayed open")
	}
	counts, err := store.userAgentCounts()
	if err != nil {
		t.Fatal(err)
	}
	if counts["Go-HTTP-Client/1.1"] != 1 {
		t.Fatalf("blocked User-Agent was not recorded: %#v", counts)
	}

	deadline := time.Now().Add(time.Second)
	for {
		server.mu.RLock()
		clientCount := len(server.clients)
		server.mu.RUnlock()
		if clientCount == 0 && len(server.connectionSlots) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("blocked client leaked resources: %d clients, %d slots", clientCount, len(server.connectionSlots))
		}
		time.Sleep(time.Millisecond)
	}
}

func TestConnectionSlotsBoundAllWebSockets(t *testing.T) {
	server := testServer()
	server.connectionSlots = make(chan struct{}, 1)
	if !server.tryAcquireConnectionSlot() {
		t.Fatal("first connection slot was rejected")
	}
	if server.tryAcquireConnectionSlot() {
		t.Fatal("connection limit admitted an extra WebSocket")
	}
	server.releaseConnectionSlot()
	if !server.tryAcquireConnectionSlot() {
		t.Fatal("released connection slot was not reusable")
	}
	server.releaseConnectionSlot()
}

func TestAllowedUserAgentKeepsFullService(t *testing.T) {
	server := testServer()
	server.uaPolicy = testUAPolicy(nil, []string{"com.nestmusic"}, nil, nil)
	httpServer := httptest.NewServer(http.HandlerFunc(server.handleWebSocket))
	t.Cleanup(httpServer.Close)

	conn, err := dialTestWebSocket(httpServer.URL, "okhttp/4.12.0")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := onlyServerClient(t, server)
	if client.uaTier != uaAllow {
		t.Fatalf("okhttp client tier = %v, want allow", client.uaTier)
	}

	message, err := NewMessageCodec(false).Encode(MsgTypePing, PingPayload{ClientTime: 5, Sequence: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, message); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, reply, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	msgType, payload, err := NewMessageCodec(true).Decode(reply)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != MsgTypePong {
		t.Fatalf("message type = %q, want pong", msgType)
	}
	var pong pb.PongPayload
	if err := proto.Unmarshal(payload, &pong); err != nil {
		t.Fatal(err)
	}
	if pong.GetSequence() != 2 || pong.GetClientTime() != 5 {
		t.Fatalf("unexpected pong: %#v", &pong)
	}
}

func TestDialledUnknownUserAgentIsAdvertised(t *testing.T) {
	server := testServer()
	httpServer := httptest.NewServer(http.HandlerFunc(server.handleWebSocket))
	t.Cleanup(httpServer.Close)

	conn, err := dialTestWebSocket(httpServer.URL, "com.joymusic.app.debug")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	deadline := time.Now().Add(time.Second)
	var client *Client
	for client == nil {
		server.mu.RLock()
		for candidate := range server.clients {
			client = candidate
		}
		server.mu.RUnlock()
		if client != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server never registered the dialled client")
		}
		time.Sleep(time.Millisecond)
	}
	if got := client.uaTier; got != uaAdvert {
		t.Fatalf("tier = %v, want advert", got)
	}
	if client.policy.advertTitle != DefaultAdvertTitle {
		t.Fatalf("advert title = %q", client.policy.advertTitle)
	}
}

func dialTestWebSocket(url, userAgent string) (*websocket.Conn, error) {
	header := http.Header{}
	header.Set("User-Agent", userAgent)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(url, "http"), header)
	return conn, err
}

func onlyServerClient(t *testing.T, server *Server) *Client {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		server.mu.RLock()
		var found *Client
		count := 0
		for client := range server.clients {
			found = client
			count++
		}
		server.mu.RUnlock()
		if count == 1 {
			return found
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected exactly one registered client, got %d", count)
		}
		time.Sleep(time.Millisecond)
	}
}
