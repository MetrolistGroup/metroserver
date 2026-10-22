package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/MetrolistGroup/metroserver/proto"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// drainTypes returns the types of every message already queued for client.
func drainTypes(t *testing.T, client *Client) []string {
	t.Helper()
	types := []string{}
	for len(client.Send) > 0 || len(client.pongs) > 0 {
		types = append(types, receiveTestMessage(t, client, nil))
	}
	return types
}

func hasType(types []string, want string) bool {
	for _, got := range types {
		if got == want {
			return true
		}
	}
	return false
}

func withFeatures(client *Client, features ...string) *Client {
	client.setFeatures(features)
	return client
}

func TestExpiredReconnectLeavesSessionForCleanup(t *testing.T) {
	server := testServer()
	guest := newClient("guest", nil)
	room := lifecycleTestRoom(nil, guest)
	room.State.HostID = "host"
	session := &Session{UserID: "host", Username: "Host", RoomCode: room.Code, IsHost: true, DisconnectAt: time.Now().Add(-ReconnectGracePeriod - time.Second)}
	room.DisconnectedUsers["host"] = session
	server.rooms[room.Code] = room
	server.sessions["host-token"] = session

	late := newClient("late", nil)
	server.handleReconnect(late, encodeTestPayload(t, MsgTypeReconnect, &ReconnectPayload{SessionToken: "host-token"}))
	receiveLifecycleError(t, late, "session_expired")

	server.cleanupExpiredSessionsOnce(time.Now())
	if room.State.HostID != "guest" || room.Host != guest {
		t.Fatalf("host = %q, want the guest promoted after expiry", room.State.HostID)
	}
	if _, stuck := room.DisconnectedUsers["host"]; stuck {
		t.Fatal("expired host stayed in the room")
	}
}

func TestHostFailoverPicksLowestRTTAndOwnerReclaims(t *testing.T) {
	server := testServer()
	host := newClient("host", nil)
	host.setUsername("Host")
	host.setSessionToken("host-token")
	slow := newClient("slow", nil)
	fast := newClient("fast", nil)
	fast.setUsername("Fast")
	slow.rttMs.Store(300)
	fast.rttMs.Store(20)
	room := lifecycleTestRoom(host, slow)
	room.Clients["fast"] = fast
	fast.setRoom(room)
	server.rooms[room.Code] = room
	server.clients[host] = true
	applicant := newClient("applicant", nil)
	applicant.setUsername("Applicant")
	room.PendingJoins["applicant"] = applicant
	applicant.trySetPendingRoom(room)

	server.handleClientDisconnect(host)
	drainTypes(t, fast)
	server.failoverHostsOnce(time.Now())
	if room.State.HostID != "host" {
		t.Fatal("failover happened before the timeout")
	}

	server.failoverHostsOnce(time.Now().Add(HostFailoverTimeout + time.Second))
	if room.Host != fast || room.State.HostID != "fast" || room.OwnerID != "host" {
		t.Fatalf("host = %q owner = %q, want fast deputy and host as owner", room.State.HostID, room.OwnerID)
	}
	types := drainTypes(t, fast)
	if !hasType(types, MsgTypeHostChanged) || !hasType(types, MsgTypeJoinRequest) {
		t.Fatalf("deputy got %v, want host_changed and the replayed join request", types)
	}

	returning := newClient("returning", nil)
	server.handleReconnect(returning, encodeTestPayload(t, MsgTypeReconnect, &ReconnectPayload{SessionToken: "host-token"}))
	if room.Host != returning || room.State.HostID != "host" || room.OwnerID != "" {
		t.Fatalf("owner did not reclaim the room: host %q owner %q", room.State.HostID, room.OwnerID)
	}
	if types := drainTypes(t, fast); !hasType(types, MsgTypeHostChanged) {
		t.Fatalf("deputy got %v, want host_changed back to the owner", types)
	}
}

func TestReconnectReplacesLiveConnection(t *testing.T) {
	server := testServer()
	host := newClient("host", nil)
	guest := newClient("guest", nil)
	guest.setSessionToken("guest-token")
	room := lifecycleTestRoom(host, guest)
	server.rooms[room.Code] = room
	server.clients[guest] = true

	replacement := newClient("replacement", nil)
	server.handleReconnect(replacement, encodeTestPayload(t, MsgTypeReconnect, &ReconnectPayload{SessionToken: "guest-token"}))
	if got := receiveTestMessage(t, replacement, nil); got != MsgTypeReconnected {
		t.Fatalf("replacement got %q, want reconnected", got)
	}
	if room.Clients["guest"] != replacement || !guest.isClosed() || guest.currentRoom() != nil {
		t.Fatal("old connection was not replaced")
	}
}

func TestKickRevokesDisconnectedMember(t *testing.T) {
	server, room, host, guest := roomTestFixture()
	guest.setSessionToken("guest-token")
	room.PendingSuggestions["s1"] = &Suggestion{ID: "s1", FromUserID: "guest", Track: &TrackInfo{ID: "t", Title: "T"}}
	server.handleClientDisconnect(guest)

	server.handleKickUser(host, encodeTestPayload(t, MsgTypeKickUser, &KickUserPayload{UserID: "guest"}))
	if _, exists := server.sessions["guest-token"]; exists {
		t.Fatal("kicked member kept a reconnect token")
	}
	if len(room.DisconnectedUsers) != 0 || len(room.PendingSuggestions) != 0 || len(room.State.Users) != 1 {
		t.Fatalf("kicked member left traces: %#v", room)
	}
}

func TestHostlessRoomClosesWhenNobodyCanHost(t *testing.T) {
	server, room, host, guest := roomTestFixture()
	guest.uaTier = uaAdvert
	applicant := newClient("applicant", nil)
	room.PendingJoins["applicant"] = applicant
	applicant.trySetPendingRoom(room)

	server.leaveRoom(host)
	if server.rooms[room.Code] != nil || guest.currentRoom() != nil {
		t.Fatal("room without a possible host stayed open")
	}
	if types := drainTypes(t, guest); !hasType(types, MsgTypeKicked) {
		t.Fatalf("guest got %v, want kicked", types)
	}
	if types := drainTypes(t, applicant); !hasType(types, MsgTypeJoinRejected) {
		t.Fatalf("applicant got %v, want join_rejected", types)
	}
}

func TestTransferReplaysPendingWork(t *testing.T) {
	server, room, host, guest := roomTestFixture()
	room.PendingSuggestions["s1"] = &Suggestion{ID: "s1", FromUserID: "guest", Track: &TrackInfo{ID: "t", Title: "T"}}
	server.handleTransferHost(host, encodeTestPayload(t, MsgTypeTransferHost, &TransferHostPayload{NewHostID: "guest"}))
	if types := drainTypes(t, guest); !hasType(types, MsgTypeSuggestionReceived) {
		t.Fatalf("new host got %v, want the pending suggestion", types)
	}
}

func TestChangeTrackQueueSemantics(t *testing.T) {
	server, host, guest, room := playbackTestRoom()
	room.State.Queue = []TrackInfo{{ID: "a", Title: "A"}, {ID: "b", Title: "B"}, {ID: "c", Title: "C"}}

	// Jumping to a queued track consumes everything up to it.
	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionChangeTrack, TrackInfo: &TrackInfo{ID: "b", Title: "B"}}))
	if ids := queueIDs(room.State.Queue); len(ids) != 1 || ids[0] != "c" {
		t.Fatalf("queue = %v, want [c]", ids)
	}
	changed := requirePlaybackMessage(t, guest, ActionChangeTrack)
	if changed.OriginUserId != "host" || changed.CapturedAtServerTime != 0 {
		t.Fatalf("broadcast origin/capture = %q/%d", changed.OriginUserId, changed.CapturedAtServerTime)
	}

	// An explicitly empty replacement queue clears it.
	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionChangeTrack, TrackInfo: &TrackInfo{ID: "x", Title: "X"}, ReplaceQueue: true}))
	if len(room.State.Queue) != 0 {
		t.Fatalf("queue = %v, want empty", queueIDs(room.State.Queue))
	}
	if room.TrackRevision != room.State.Revision {
		t.Fatal("track revision was not recorded")
	}
}

func TestQueueByteBudget(t *testing.T) {
	server, host, _, room := playbackTestRoom()
	huge := make([]TrackInfo, 0, MaxQueueSize)
	thumbnail := make([]byte, MaxTrackURLLength)
	for i := range thumbnail {
		thumbnail[i] = 'x'
	}
	for i := 0; i < 200; i++ {
		huge = append(huge, TrackInfo{ID: string(rune('a'+i%26)) + string(rune('a'+i/26)), Title: "T", Thumbnail: string(thumbnail)})
	}
	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionSyncQueue, Queue: huge}))
	requireTestError(t, host, "queue_too_large")
	if len(room.State.Queue) != 0 {
		t.Fatal("oversized queue was stored")
	}
}

func TestHostSequenceRejectsStaleObservations(t *testing.T) {
	server, host, _, room := playbackTestRoom()
	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionSeek, Position: 800, HostSequence: 5}))
	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionPlay, Position: 100, HostSequence: 4}))
	requirePlaybackMessage(t, host, ActionSeek)
	requireTestError(t, host, "stale_action")
	if room.State.Position != 800 || room.State.IsPlaying {
		t.Fatalf("stale observation was applied: %#v", room.State)
	}
}

func TestUpdateTrackKeepsPositionAndOnlyReachesCapableClients(t *testing.T) {
	server, host, guest, room := playbackTestRoom()
	capable := withFeatures(newClient("capable", nil), FeatureUpdateTrack)
	room.Clients["capable"] = capable
	capable.setRoom(room)
	room.State.Position = 700

	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionUpdateTrack, TrackInfo: &TrackInfo{ID: "current", Title: "Current", Duration: 400000}}))
	if room.State.CurrentTrack.Duration != 400000 || room.State.Position != 700 {
		t.Fatalf("update_track state = %#v", room.State)
	}
	requirePlaybackMessage(t, capable, ActionUpdateTrack)
	if len(guest.Send) != 0 {
		t.Fatal("client without update_track received it")
	}
	server.handlePlaybackAction(host, encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionUpdateTrack, TrackInfo: &TrackInfo{ID: "other", Title: "Other"}}))
	requireTestError(t, host, "stale_track")
}

func TestCompensateActionTransit(t *testing.T) {
	for _, test := range []struct {
		name                         string
		position, captured, now, rtt int64
		want                         int64
	}{
		{name: "captured", position: 100, captured: 1000, now: 9000, want: 8100},
		{name: "rtt fallback", position: 100, now: 9000, rtt: 80, want: 140},
		{name: "future capture", position: 100, captured: 9500, now: 9000, want: 100},
		{name: "ancient capture", position: 100, captured: 1, now: 60000, want: 100},
		{name: "saturates", position: 1<<63 - 1, captured: 8000, now: 9000, want: 1<<63 - 1},
	} {
		if got := compensateActionTransit(test.position, test.captured, test.now, test.rtt); got != test.want {
			t.Errorf("%s: got %d, want %d", test.name, got, test.want)
		}
	}
}

func TestPingDriftReportAndCanary(t *testing.T) {
	server, _, guest, room := playbackTestRoom()
	room.State.Queue = []TrackInfo{{ID: "q", Title: "Q"}}
	room.State.Position = 1000
	room.State.LastUpdate = serverNowMs()
	now := serverNowMs()

	server.handlePing(guest, encodeTestPayload(t, MsgTypePing, &PingPayload{
		Sequence: 1, HasPosition: true, TrackID: "current", Position: 1250, PositionCapturedAtServerTime: now,
		HasStateHash: true, StateHash: roomStateHash(room.State),
	}), now)
	var pong pb.PongPayload
	if got := receiveTestMessage(t, guest, &pong); got != MsgTypePong {
		t.Fatalf("got %q, want pong", got)
	}
	if !pong.DriftValid || pong.DriftMs != 250 || pong.StateMismatch {
		t.Fatalf("pong = %#v, want 250 ms ahead and matching state", &pong)
	}

	server.handlePing(guest, encodeTestPayload(t, MsgTypePing, &PingPayload{Sequence: 2, HasStateHash: true, StateHash: 1}), now)
	if got := receiveTestMessage(t, guest, &pong); got != MsgTypePong || !pong.StateMismatch {
		t.Fatalf("pong = %#v, want state mismatch", &pong)
	}
	if got := receiveTestMessage(t, guest, nil); got != MsgTypeSyncState {
		t.Fatalf("got %q, want sync_state after a canary mismatch", got)
	}
}

func TestRequestIDsAcksAndSyncThrottle(t *testing.T) {
	server, _, guest, _ := playbackTestRoom()
	withFeatures(guest, FeatureRequestID)
	codec := NewMessageCodec(false)

	encode := func(msgType string, payload interface{}, id uint64) []byte {
		data, err := codec.EncodeReply(msgType, payload, id)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	requestID := func() (string, uint64) {
		data := <-guest.Send
		msgType, _, id, err := guest.codec.DecodeRequest(data)
		if err != nil {
			t.Fatal(err)
		}
		return msgType, id
	}

	server.handleMessage(guest, encode(MsgTypeRequestSync, nil, 41), serverNowMs())
	if msgType, id := requestID(); msgType != MsgTypeSyncState || id != 41 {
		t.Fatalf("got %q #%d, want sync_state #41", msgType, id)
	}
	server.handleMessage(guest, encode(MsgTypeRequestSync, nil, 42), serverNowMs())
	if msgType, id := requestID(); msgType != MsgTypeError || id != 42 {
		t.Fatalf("got %q #%d, want sync_rate_limited #42", msgType, id)
	}
	server.handleMessage(guest, encode(MsgTypeLeaveRoom, nil, 43), serverNowMs())
	drainUntil := func(want string) uint64 {
		for {
			msgType, id := requestID()
			if msgType == want {
				return id
			}
		}
	}
	if id := drainUntil(MsgTypeAck); id != 43 {
		t.Fatalf("ack id = %d, want 43", id)
	}
}

func TestOverflowClosesSlowClient(t *testing.T) {
	client := newClient("slow", nil)
	big := make([]byte, MaxQueuedBytes/2+1)
	client.sendEncodedMessage(zap.NewNop(), "x", big)
	client.sendEncodedMessage(zap.NewNop(), "x", big)
	if !client.isClosed() {
		t.Fatal("client over its byte budget stayed open")
	}
}

func TestSuggestionLimitsAckAndOfflineNotice(t *testing.T) {
	server, room, host, guest := roomTestFixture()
	withFeatures(guest, FeatureSuggestionAck)
	suggest := func(id string) {
		server.handleSuggestTrack(guest, encodeTestPayload(t, MsgTypeSuggestTrack, &SuggestTrackPayload{TrackInfo: &TrackInfo{ID: id, Title: id}}))
	}
	suggest("one")
	if got := receiveTestMessage(t, guest, nil); got != MsgTypeSuggestionSubmitted {
		t.Fatalf("guest got %q, want suggestion_submitted", got)
	}
	suggest("one")
	receiveRoomError(t, guest, "duplicate_suggestion")
	for i := 1; i < MaxSuggestionsPerUser; i++ {
		suggest(string(rune('a' + i)))
		drainTypes(t, guest)
	}
	suggest("overflow")
	receiveRoomError(t, guest, "too_many_suggestions")
	drainTypes(t, host)

	guest.setSessionToken("guest-token")
	server.handleClientDisconnect(guest)
	var id string
	for suggestionID := range room.PendingSuggestions {
		id = suggestionID
		break
	}
	server.handleRejectSuggestion(host, encodeTestPayload(t, MsgTypeRejectSuggestion, &RejectSuggestionPayload{SuggestionID: id}))
	back := newClient("back", nil)
	server.handleReconnect(back, encodeTestPayload(t, MsgTypeReconnect, &ReconnectPayload{SessionToken: "guest-token"}))
	if types := drainTypes(t, back); !hasType(types, MsgTypeSuggestionRejected) {
		t.Fatalf("returning guest got %v, want the rejection made while away", types)
	}
}

func TestLeaveWhilePendingGetsFreshID(t *testing.T) {
	server, room, _, _ := roomTestFixture()
	applicant := newClient("applicant", nil)
	room.PendingJoins["applicant"] = applicant
	applicant.trySetPendingRoom(room)
	server.leaveRoom(applicant)
	if applicant.clientID() == "applicant" || len(room.PendingJoins) != 0 {
		t.Fatal("cancelled applicant kept its ID or pending entry")
	}
}

func TestReconnectCountsReservedSeats(t *testing.T) {
	server, room, host, _ := roomTestFixture()
	for i := 0; i < MaxClientsPerRoom; i++ {
		room.DisconnectedUsers[string(rune(1000+i))] = &Session{}
	}
	applicant := newClient("applicant", nil)
	applicant.setUsername("A")
	room.PendingJoins["applicant"] = applicant
	applicant.trySetPendingRoom(room)
	server.handleApproveJoin(host, encodeTestPayload(t, MsgTypeApproveJoin, &ApproveJoinPayload{UserID: "applicant"}))
	receiveRoomError(t, host, "room_full")
}

func TestRoomCreateRateLimitPerSource(t *testing.T) {
	server := testServer()
	now := time.Now()
	for i := 0; i < MaxRoomCreatesPerSource; i++ {
		if !server.allowRoomCreate("1.2.3.4", now) {
			t.Fatal("rate limit hit early")
		}
	}
	if server.allowRoomCreate("1.2.3.4", now) {
		t.Fatal("rate limit not enforced")
	}
	if !server.allowRoomCreate("5.6.7.8", now) || !server.allowRoomCreate("1.2.3.4", now.Add(RoomCreateWindow)) {
		t.Fatal("rate limit leaked across sources or windows")
	}
	request := httptest.NewRequest(http.MethodGet, "/ws", nil)
	request.Header.Set("CF-Connecting-IP", "9.9.9.9")
	if got := requestSource(request); got != "9.9.9.9" {
		t.Fatalf("source = %q", got)
	}
}

func TestWriteBehindPersistence(t *testing.T) {
	server, room, _, guest := roomTestFixture()
	guest.setSessionToken("guest-token")
	var err error
	server.database, err = openDatabase(filepath.Join(t.TempDir(), DefaultDatabaseFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.database.Close() })

	if err := server.FlushState(); err != nil {
		t.Fatal(err)
	}
	stored := string(server.persisted[room.Code])
	if err := server.FlushState(); err != nil || string(server.persisted[room.Code]) != stored {
		t.Fatal("unchanged room was rewritten")
	}

	restored := testServer()
	restored.database = server.database
	if err := restored.LoadState(); err != nil {
		t.Fatal(err)
	}
	if session := restored.sessions["guest-token"]; session == nil || session.DisconnectAt.IsZero() {
		t.Fatalf("connected member was not restored as reconnectable: %#v", session)
	}
}

func TestGracefulRestartOverWebSocket(t *testing.T) {
	server := testServer()
	httpServer := httptest.NewServer(http.HandlerFunc(server.handleWebSocket))
	t.Cleanup(httpServer.Close)
	conn, err := dialTestWebSocket(httpServer.URL, "com.metrolist.music")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	codec := NewMessageCodec(false)
	send := func(msgType string, payload interface{}) {
		data, err := codec.Encode(msgType, payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
			t.Fatal(err)
		}
	}
	read := func() (string, []byte) {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		msgType, payload, err := NewMessageCodec(true).Decode(data)
		if err != nil {
			t.Fatal(err)
		}
		return msgType, payload
	}

	send(MsgTypeClientCapabilities, &ClientCapabilitiesPayload{SupportsProtobuf: true, Features: []string{FeatureServerRestarting, "unknown"}})
	msgType, payload := read()
	var caps pb.ServerCapabilities
	if err := proto.Unmarshal(payload, &caps); err != nil || msgType != MsgTypeServerCapabilities || len(caps.Features) != 1 || !caps.CanHost {
		t.Fatalf("capabilities = %q %#v", msgType, &caps)
	}
	send(MsgTypePing, PingPayload{Sequence: 1})
	if msgType, _ := read(); msgType != MsgTypePong {
		t.Fatalf("got %q, want pong", msgType)
	}

	server.announceRestart()
	if msgType, _ := read(); msgType != MsgTypeServerRestarting {
		t.Fatalf("got %q, want server_restarting", msgType)
	}
	server.drainClients(time.Now().Add(2 * time.Second))
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = conn.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseServiceRestart) {
		t.Fatalf("close = %v, want 1012", err)
	}
	if _, err := dialTestWebSocket(httpServer.URL, "com.metrolist.music"); err == nil {
		t.Fatal("server accepted a connection while restarting")
	}
	server.Close()
}
