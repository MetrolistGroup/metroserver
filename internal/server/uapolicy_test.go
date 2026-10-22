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

func testUAPolicy(block, rickroll, advert, _ []string) *uaPolicy {
	return &uaPolicy{
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
	policy := testUAPolicy([]string{"com.sporify.lite"}, []string{"com.nestmusic", "com.joymusic"}, []string{"org.cicada"}, nil)
	for _, test := range []struct {
		userAgent string
		want      uaTier
	}{
		{userAgent: "okhttp/4.12.0", want: uaAllow},
		{userAgent: "com.metrolist.music", want: uaAllow},
		{userAgent: "com.nestmusic.music.debug", want: uaRickroll},
		{userAgent: "com.JoyMusic.App", want: uaRickroll},
		{userAgent: "org.cicada", want: uaAdvert},
		{userAgent: "com.sporify.lite", want: uaBlock},
		{userAgent: "com.soundsphere.music", want: uaAllow},
		{userAgent: "", want: uaAllow},
	} {
		if got := policy.resolve(test.userAgent); got != test.want {
			t.Errorf("resolve(%q) = %v, want %v", test.userAgent, got, test.want)
		}
	}
	if uaTier(99).String() != "unknown" || uaRickroll.String() != "rickroll" {
		t.Fatal("tier naming broken")
	}
	if got := testUAPolicy([]string{"okhttp"}, []string{"okhttp"}, nil, nil).resolve("okhttp/4.12.0"); got != uaBlock {
		t.Fatalf("block did not take precedence: %v", got)
	}
}

func TestDefaultUAPolicyServesEveryone(t *testing.T) {
	policy := defaultUAPolicy()
	for _, userAgent := range []string{"okhttp/4.12.0", "com.metrolist.music", "com.joymusic.app", "SimpMusic/2.1.0 (com.maxrave.simpmusic; Android 16)", ""} {
		if got := policy.resolve(userAgent); got != uaAllow {
			t.Errorf("resolve(%q) = %v, want allow", userAgent, got)
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
	if got := client.rewritePayload(original).(PlaybackActionPayload); got.QueueTitle != original.QueueTitle || len(got.Queue) != len(original.Queue) {
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
		{userAgent: "com.nevar.nzik.evil", want: uaAllow},
		{userAgent: "com.nestmusic.music", want: uaRickroll},
		{userAgent: "org.cicada", want: uaAdvert},
		{userAgent: "com.sporify.lite", want: uaBlock},
		{userAgent: "com.recordlabs.music.debug", want: uaAllow},
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
		"null":          `null`,
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

func queueIDs(queue []TrackInfo) []string {
	ids := make([]string, len(queue))
	for i := range queue {
		ids[i] = queue[i].ID
	}
	return ids
}

func TestAdvertTierOnlyRewritesQueueTitle(t *testing.T) {
	original := realPlayback()
	got := rewriteForUATier(uaAdvert, "Switch to Metrolist", original).(PlaybackActionPayload)

	if got.QueueTitle != "Switch to Metrolist" {
		t.Fatalf("queue title = %q", got.QueueTitle)
	}
	if got.TrackID != "real-id" || got.TrackInfo.Title != "Real Song" || len(got.Queue) != 1 || got.Position != 42000 {
		t.Fatalf("advert tier touched tracks: %#v", got)
	}
	if original.QueueTitle != "Friday mix" {
		t.Fatalf("rewrite mutated the shared payload: %q", original.QueueTitle)
	}
	state := rewriteForUATier(uaAdvert, "Ad", &RoomState{QueueTitle: "Mine"}).(*RoomState)
	if state.QueueTitle != "Ad" {
		t.Fatalf("advert snapshot title = %q", state.QueueTitle)
	}
}

func TestRickrollTierInsertsRickIntoQueue(t *testing.T) {
	original := realPlayback()
	original.Queue = []TrackInfo{{ID: "a", Title: "A"}, {ID: "b", Title: "B"}, {ID: "c", Title: "C"}}
	got := rewriteForUATier(uaRickroll, DefaultAdvertTitle, original).(PlaybackActionPayload)

	if got.QueueTitle != DefaultAdvertTitle {
		t.Fatalf("queue title = %q", got.QueueTitle)
	}
	if got.TrackID != "real-id" || got.TrackInfo.Title != "Real Song" || got.Position != original.Position {
		t.Fatalf("current track was touched: %#v", got)
	}
	ids := queueIDs(got.Queue)
	if len(ids) != 4 || strings.Count(strings.Join(ids, ","), rickTrackID) != 1 {
		t.Fatalf("queue = %v, want the real tracks plus one Rick", ids)
	}
	kept := make([]string, 0, 3)
	for _, id := range ids {
		if id != rickTrackID {
			kept = append(kept, id)
		}
	}
	if !reflect.DeepEqual(kept, []string{"a", "b", "c"}) {
		t.Fatalf("real queue order changed: %v", ids)
	}
	again := rewriteForUATier(uaRickroll, DefaultAdvertTitle, original).(PlaybackActionPayload)
	if !reflect.DeepEqual(queueIDs(again.Queue), ids) {
		t.Fatal("Rick moved between identical snapshots")
	}
	if len(original.Queue) != 3 {
		t.Fatal("rewrite mutated the shared queue")
	}
	if pause := rewriteForUATier(uaRickroll, DefaultAdvertTitle, PlaybackActionPayload{Action: ActionPause}).(PlaybackActionPayload); pause.Queue != nil {
		t.Fatalf("queue-less action gained a queue: %#v", pause.Queue)
	}
}

func TestRickrollTierRewritesSnapshots(t *testing.T) {
	newState := func() *RoomState {
		return &RoomState{
			CurrentTrack: &TrackInfo{ID: "real-id", Title: "Real Song", Duration: 300000},
			Queue:        []TrackInfo{{ID: "queued-id", Title: "Queued Song"}},
			Position:     900000,
		}
	}
	state := newState()
	join := rewriteForUATier(uaRickroll, DefaultAdvertTitle, JoinApprovedPayload{RoomCode: "ROOM1234", State: state}).(JoinApprovedPayload)
	if join.State.CurrentTrack.ID != "real-id" || len(join.State.Queue) != 2 || join.State.QueueTitle != DefaultAdvertTitle {
		t.Fatalf("join state = %#v", join.State)
	}
	if len(state.Queue) != 1 || state.QueueTitle != "" {
		t.Fatal("join rewrite mutated the live state")
	}
	reconnect := rewriteForUATier(uaRickroll, DefaultAdvertTitle, ReconnectedPayload{State: newState()}).(ReconnectedPayload)
	if len(reconnect.State.Queue) != 2 {
		t.Fatalf("reconnect state = %#v", reconnect.State)
	}
	if got := rewriteForUATier(uaRickroll, DefaultAdvertTitle, ReconnectedPayload{}).(ReconnectedPayload); got.State != nil {
		t.Fatalf("nil state became %#v", got.State)
	}
	sync := rewriteForUATier(uaRickroll, DefaultAdvertTitle, SyncStatePayload{}).(SyncStatePayload)
	if len(sync.Queue) != 1 || sync.Queue[0].ID != rickTrackID || sync.QueueTitle != DefaultAdvertTitle {
		t.Fatalf("sync state = %#v", sync)
	}
}

func TestPassthroughTiersAndPayloads(t *testing.T) {
	pong := PongPayload{Sequence: 3}
	for _, tier := range []uaTier{uaAllow, uaBlock, uaAdvert, uaRickroll} {
		if got := rewriteForUATier(tier, DefaultAdvertTitle, pong); !reflect.DeepEqual(got, pong) {
			t.Fatalf("%v tier rewrote an unrelated payload", tier)
		}
	}
	var nilState *RoomState
	if got, ok := rewriteForUATier(uaRickroll, DefaultAdvertTitle, nilState).(*RoomState); !ok || got != nil {
		t.Fatalf("nil room state rewritten to %#v", got)
	}
	if got := rewriteForUATier(uaAllow, DefaultAdvertTitle, realPlayback()); !reflect.DeepEqual(got, realPlayback()) {
		t.Fatal("allow tier rewrote a playback payload")
	}
}

func TestRickrollGuestRoomStateStaysReal(t *testing.T) {
	server, host, guest, room := playbackTestRoom()
	guest.policy = testUAPolicy(nil, []string{"com.nestmusic"}, nil, nil)
	guest.uaTier = uaRickroll
	host.policy = defaultUAPolicy()

	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{
		Action:    ActionChangeTrack,
		TrackInfo: &TrackInfo{ID: "real-id", Title: "Real Song", Duration: 300000},
		Queue:     []TrackInfo{{ID: "queued-id", Title: "Queued Song", Duration: 100000}},
	}))

	requirePlaybackMessage(t, host, ActionChangeTrack)
	guestMessage := requirePlaybackMessage(t, guest, ActionChangeTrack)
	if guestMessage.TrackInfo.GetTitle() != "Real Song" || len(guestMessage.Queue) != 2 {
		t.Fatalf("guest message = %#v", guestMessage)
	}
	if guestMessage.QueueTitle != DefaultAdvertTitle {
		t.Fatalf("guest queue title = %q", guestMessage.QueueTitle)
	}
	room.mu.RLock()
	defer room.mu.RUnlock()
	if len(room.State.Queue) != 1 || room.State.Queue[0].ID != "queued-id" {
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
		name      string
		client    *Client
		wantTitle string
		wantQueue int
	}{
		{name: "allow", client: allowClient, wantTitle: "Friday mix", wantQueue: 1},
		{name: "advert", client: advertClient, wantTitle: DefaultAdvertTitle, wantQueue: 1},
		{name: "rickroll", client: rickClient, wantTitle: DefaultAdvertTitle, wantQueue: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			var p pb.PlaybackActionPayload
			if msgType := receiveTestMessage(t, test.client, &p); msgType != MsgTypeSyncPlayback {
				t.Fatalf("message type = %q", msgType)
			}
			if p.GetQueueTitle() != test.wantTitle {
				t.Errorf("queue title = %q, want %q", p.GetQueueTitle(), test.wantTitle)
			}
			if got := p.GetTrackInfo().GetTitle(); got != "Real Song" {
				t.Errorf("track title = %q", got)
			}
			if got := len(p.GetQueue()); got != test.wantQueue {
				t.Errorf("queue length = %d, want %d", got, test.wantQueue)
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
	if state.GetCurrentTrack().GetTitle() != "Real Song" || len(state.GetQueue()) != 1 || state.GetQueue()[0].GetId() != rickTrackID {
		t.Fatalf("sync_state = %#v", &state)
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

func TestDialledUnknownUserAgentGetsFullService(t *testing.T) {
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
	if got := client.uaTier; got != uaAllow {
		t.Fatalf("tier = %v, want allow", got)
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
