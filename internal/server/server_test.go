package server

import (
	"bytes"
	"compress/gzip"
	"os"
	"testing"
	"time"

	pb "github.com/MetrolistGroup/metroserver/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func testServer() *Server {
	return NewServer(zap.NewNop())
}

func encodeCompressedEnvelope(t *testing.T, msgType string, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	envelope, err := proto.Marshal(&pb.Envelope{Type: msgType, Payload: buf.Bytes(), Compressed: true})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func TestDecodeRejectsOversizedCompressedPayload(t *testing.T) {
	codec := NewMessageCodec(true)
	payload := bytes.Repeat([]byte("a"), MaxDecodedPayloadSize+1)
	if _, _, err := codec.Decode(encodeCompressedEnvelope(t, MsgTypePing, payload)); err == nil {
		t.Fatal("expected oversized compressed payload to be rejected")
	}
}

func TestRemoveClientCleansPendingJoin(t *testing.T) {
	server := testServer()
	host := newClient("host", nil)
	host.setUsername("host")
	guest := newClient("guest", nil)
	guest.setUsername("guest")
	room := &Room{
		Code:               "ROOM1234",
		Host:               host,
		Clients:            map[string]*Client{"host": host},
		PendingJoins:       map[string]*Client{"guest": guest},
		DisconnectedUsers:  make(map[string]*Session),
		PendingSuggestions: make(map[string]*Suggestion),
		BufferingUsers:     make(map[string]bool),
		State: &RoomState{
			RoomCode: "ROOM1234",
			HostID:   "host",
			Users:    []UserInfo{{UserID: "host", Username: "host", IsHost: true, IsConnected: true}},
		},
	}
	host.setRoom(room)
	server.rooms[room.Code] = room
	server.clients[guest] = true

	server.removeClient(guest)
	if _, exists := room.PendingJoins["guest"]; exists {
		t.Fatal("pending join was not removed for disconnected client")
	}
}

func TestApproveSuggestionQueueFullKeepsSuggestion(t *testing.T) {
	server := testServer()
	host := newClient("host", nil)
	host.setUsername("host")
	host.setRoom(nil)
	queue := make([]TrackInfo, MaxQueueSize)
	room := &Room{
		Code:               "ROOM1234",
		Host:               host,
		Clients:            map[string]*Client{"host": host},
		PendingJoins:       make(map[string]*Client),
		DisconnectedUsers:  make(map[string]*Session),
		PendingSuggestions: make(map[string]*Suggestion),
		BufferingUsers:     make(map[string]bool),
		State: &RoomState{
			RoomCode: "ROOM1234",
			HostID:   "host",
			Users:    []UserInfo{{UserID: "host", Username: "host", IsHost: true, IsConnected: true}},
			Queue:    queue,
		},
	}
	host.setRoom(room)
	room.PendingSuggestions["s1"] = &Suggestion{
		ID:           "s1",
		FromUserID:   "guest",
		FromUsername: "guest",
		Track:        &TrackInfo{ID: "track", Title: "title", Artist: "artist", Duration: 1},
	}
	payload, err := NewMessageCodec(false).Encode(MsgTypeApproveSuggestion, &ApproveSuggestionPayload{SuggestionID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	_, payloadBytes, err := NewMessageCodec(false).Decode(payload)
	if err != nil {
		t.Fatal(err)
	}

	server.handleApproveSuggestion(host, payloadBytes)
	if _, exists := room.PendingSuggestions["s1"]; !exists {
		t.Fatal("suggestion was removed even though queue was full")
	}
}

func TestSaveStateIncludesActiveSessionsAndLoadHasNoPlaceholderHost(t *testing.T) {
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldWD) })

	server := testServer()
	host := newClient("host", nil)
	host.setUsername("host")
	host.setSessionToken("token-host")
	room := &Room{
		Code:               "ROOM1234",
		Host:               host,
		Clients:            map[string]*Client{"host": host},
		PendingJoins:       make(map[string]*Client),
		DisconnectedUsers:  make(map[string]*Session),
		PendingSuggestions: make(map[string]*Suggestion),
		BufferingUsers:     make(map[string]bool),
		State: &RoomState{
			RoomCode: "ROOM1234",
			HostID:   "host",
			Users:    []UserInfo{{UserID: "host", Username: "host", IsHost: true, IsConnected: true}},
		},
	}
	host.setRoom(room)
	server.rooms[room.Code] = room
	server.clients[host] = true

	if err := server.SaveState(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(StateFile)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("state file permissions = %v, want 0600", info.Mode().Perm())
	}

	restored := testServer()
	if err := restored.LoadState(); err != nil {
		t.Fatal(err)
	}
	if _, exists := restored.sessions["token-host"]; !exists {
		t.Fatal("active host session was not persisted")
	}
	restoredRoom := restored.rooms["ROOM1234"]
	if restoredRoom == nil {
		t.Fatal("room was not restored")
	}
	if restoredRoom.Host != nil {
		t.Fatal("restored room should not contain a placeholder host client")
	}
	if restoredRoom.HostDisconnectedAt == nil || time.Since(*restoredRoom.HostDisconnectedAt) > time.Minute {
		t.Fatal("host disconnected timestamp was not restored")
	}
}

func TestExpiredHostSessionTransfersToActiveClient(t *testing.T) {
	server := testServer()
	guest := newClient("guest", nil)
	guest.setUsername("guest")
	expiredHost := &Session{
		UserID:       "host",
		Username:     "host",
		RoomCode:     "ROOM1234",
		IsHost:       true,
		DisconnectAt: time.Now().Add(-ReconnectGracePeriod - time.Second),
	}
	room := &Room{
		Code:               "ROOM1234",
		Host:               nil,
		Clients:            map[string]*Client{"guest": guest},
		PendingJoins:       make(map[string]*Client),
		DisconnectedUsers:  map[string]*Session{"host": expiredHost},
		PendingSuggestions: make(map[string]*Suggestion),
		BufferingUsers:     make(map[string]bool),
		State: &RoomState{
			RoomCode: "ROOM1234",
			HostID:   "host",
			Users: []UserInfo{
				{UserID: "host", Username: "host", IsHost: true, IsConnected: false},
				{UserID: "guest", Username: "guest", IsHost: false, IsConnected: true},
			},
		},
	}
	guest.setRoom(room)
	server.rooms[room.Code] = room
	server.sessions["token-host"] = expiredHost

	server.cleanupExpiredSessionsOnce(time.Now())
	if room.Host != guest {
		t.Fatal("active guest was not promoted after host session expired")
	}
	if room.State.HostID != "guest" {
		t.Fatalf("host id = %q, want guest", room.State.HostID)
	}
	if room.HostDisconnectedAt != nil {
		t.Fatal("host disconnected timestamp should be cleared after transfer")
	}
}
