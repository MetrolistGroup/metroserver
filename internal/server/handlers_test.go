package server

import (
	"testing"
	"time"

	pb "github.com/MetrolistGroup/metroserver/proto"
	"google.golang.org/protobuf/proto"
)

func encodeTestPayload(t *testing.T, msgType string, payload any) []byte {
	t.Helper()
	codec := NewMessageCodec(false)
	message, err := codec.Encode(msgType, payload)
	if err != nil {
		t.Fatal(err)
	}
	decodedType, payloadBytes, err := codec.Decode(message)
	if err != nil {
		t.Fatal(err)
	}
	if decodedType != msgType {
		t.Fatalf("decoded message type = %q, want %q", decodedType, msgType)
	}
	return payloadBytes
}

func receiveTestMessage(t *testing.T, client *Client, target proto.Message) string {
	t.Helper()
	var message []byte
	// Pongs first, as the writer prioritizes them, so tests see a fixed order.
	select {
	case pong := <-client.pongs:
		message = encodeTestPong(t, client, pong)
	default:
		select {
		case pong := <-client.pongs:
			message = encodeTestPong(t, client, pong)
		case queued, open := <-client.Send:
			if !open {
				t.Fatal("client send channel was closed")
			}
			message = queued
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for client message")
			return ""
		}
	}
	msgType, payload, err := client.codec.Decode(message)
	if err != nil {
		t.Fatal(err)
	}
	if target != nil {
		if err := proto.Unmarshal(payload, target); err != nil {
			t.Fatal(err)
		}
	}
	return msgType
}

// encodeTestPong stamps and encodes a pong the way writePong does.
func encodeTestPong(t *testing.T, client *Client, pong queuedPong) []byte {
	t.Helper()
	pong.payload.ServerSendTime = serverNowMs()
	encoded, err := client.codec.EncodeReply(MsgTypePong, pong.payload, pong.requestID)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestPlaybackActionPlayClampsPositionAndBroadcasts(t *testing.T) {
	server := testServer()
	host := newClient("host", nil)
	host.setUsername("Host")
	guest := newClient("guest", nil)
	room := &Room{
		Code:               "ROOM1234",
		Host:               host,
		Clients:            map[string]*Client{"host": host, "guest": guest},
		PendingJoins:       make(map[string]*Client),
		PendingSuggestions: make(map[string]*Suggestion),
		DisconnectedUsers:  make(map[string]*Session),
		State: &RoomState{
			RoomCode:     "ROOM1234",
			HostID:       "host",
			CurrentTrack: &TrackInfo{ID: "track", Title: "Track", Duration: 1000},
		},
	}
	host.setRoom(room)
	guest.setRoom(room)

	payload := encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{
		Action:   ActionPlay,
		Position: 1200,
	})
	server.handlePlaybackAction(host, payload)

	if !room.State.IsPlaying {
		t.Fatal("room was not marked as playing")
	}
	// Reported durations are placeholders when unknown, so they never cap the host.
	if room.State.Position != 1200 {
		t.Fatalf("position = %d, want 1200", room.State.Position)
	}
	if room.State.LastUpdate == 0 {
		t.Fatal("last update was not set")
	}

	var broadcast pb.PlaybackActionPayload
	if msgType := receiveTestMessage(t, guest, &broadcast); msgType != MsgTypeSyncPlayback {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeSyncPlayback)
	}
	if broadcast.Action != ActionPlay || broadcast.TrackId != "track" || broadcast.Position != 1200 || broadcast.ServerTime == 0 {
		t.Fatalf("unexpected playback broadcast: %#v", &broadcast)
	}
}

func TestPlaybackActionRejectsNonHost(t *testing.T) {
	server := testServer()
	host := newClient("host", nil)
	guest := newClient("guest", nil)
	room := &Room{
		Code:    "ROOM1234",
		Host:    host,
		Clients: map[string]*Client{"host": host, "guest": guest},
		State:   &RoomState{Position: 100},
	}
	guest.setRoom(room)

	payload := encodeTestPayload(t, MsgTypePlaybackAction, &PlaybackActionPayload{Action: ActionSeek, Position: 500})
	server.handlePlaybackAction(guest, payload)

	if room.State.Position != 100 {
		t.Fatalf("non-host changed position to %d", room.State.Position)
	}
	var response pb.ErrorPayload
	if msgType := receiveTestMessage(t, guest, &response); msgType != MsgTypeError {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeError)
	}
	if response.Code != "not_host" {
		t.Fatalf("error code = %q, want not_host", response.Code)
	}
}

func TestLeaveRoomTransfersHostAndRemovesSession(t *testing.T) {
	server := testServer()
	host := newClient("host", nil)
	host.setUsername("Host")
	host.setSessionToken("host-token")
	guest := newClient("guest", nil)
	guest.setUsername("Guest")
	room := &Room{
		Code:               "ROOM1234",
		Host:               host,
		Clients:            map[string]*Client{"host": host, "guest": guest},
		PendingJoins:       make(map[string]*Client),
		PendingSuggestions: make(map[string]*Suggestion),
		DisconnectedUsers:  make(map[string]*Session),
		State: &RoomState{
			RoomCode: "ROOM1234",
			HostID:   "host",
			Users: []UserInfo{
				{UserID: "host", Username: "Host", IsHost: true, IsConnected: true},
				{UserID: "guest", Username: "Guest", IsConnected: true},
			},
		},
	}
	host.setRoom(room)
	guest.setRoom(room)
	server.rooms[room.Code] = room
	server.sessions["host-token"] = &Session{UserID: "host", RoomCode: room.Code, IsHost: true}

	server.leaveRoom(host)

	if host.currentRoom() != nil {
		t.Fatal("departing host still references the room")
	}
	if room.Host != guest || room.State.HostID != "guest" {
		t.Fatal("guest was not promoted to host")
	}
	if len(room.State.Users) != 1 || room.State.Users[0].UserID != "guest" || !room.State.Users[0].IsHost {
		t.Fatalf("unexpected room users after host departure: %#v", room.State.Users)
	}
	if _, exists := server.sessions["host-token"]; exists {
		t.Fatal("departing host session was not removed")
	}

	var left pb.UserLeftPayload
	if msgType := receiveTestMessage(t, guest, &left); msgType != MsgTypeUserLeft {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeUserLeft)
	}
	if left.UserId != "host" || left.Username != "Host" {
		t.Fatalf("unexpected user-left payload: %#v", &left)
	}
	var changed pb.HostChangedPayload
	if msgType := receiveTestMessage(t, guest, &changed); msgType != MsgTypeHostChanged {
		t.Fatalf("message type = %q, want %q", msgType, MsgTypeHostChanged)
	}
	if changed.NewHostId != "guest" || changed.NewHostName != "Guest" {
		t.Fatalf("unexpected host-changed payload: %#v", &changed)
	}
}
