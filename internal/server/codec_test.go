package server

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	pb "github.com/MetrolistGroup/metroserver/proto"
	"google.golang.org/protobuf/proto"
)

func marshalCodecProto(t *testing.T, message proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	return data
}

func TestMessageCodecEncodeDecode(t *testing.T) {
	t.Run("compressed round trip", func(t *testing.T) {
		codec := NewMessageCodec(true)
		input := &CreateRoomPayload{Username: strings.Repeat("compressible", 100)}
		encoded, err := codec.Encode(MsgTypeCreateRoom, input)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		var envelope pb.Envelope
		if err := proto.Unmarshal(encoded, &envelope); err != nil {
			t.Fatalf("proto.Unmarshal(envelope) error = %v", err)
		}
		if !envelope.Compressed {
			t.Fatal("Encode() did not compress a large repetitive payload")
		}

		msgType, payload, err := codec.Decode(encoded)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		if msgType != MsgTypeCreateRoom {
			t.Fatalf("Decode() type = %q, want %q", msgType, MsgTypeCreateRoom)
		}
		var decoded pb.CreateRoomPayload
		if err := proto.Unmarshal(payload, &decoded); err != nil {
			t.Fatalf("proto.Unmarshal(payload) error = %v", err)
		}
		if decoded.Username != input.Username {
			t.Fatalf("decoded username length = %d, want %d", len(decoded.Username), len(input.Username))
		}
	})

	t.Run("nil payload", func(t *testing.T) {
		codec := NewMessageCodec(false)
		encoded, err := codec.Encode(MsgTypePing, nil)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
		msgType, payload, err := codec.Decode(encoded)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		if msgType != MsgTypePing || len(payload) != 0 {
			t.Fatalf("Decode() = (%q, %v), want (%q, empty)", msgType, payload, MsgTypePing)
		}
	})

	t.Run("unsupported payload", func(t *testing.T) {
		if _, err := NewMessageCodec(false).Encode("unknown", struct{}{}); err == nil || !strings.Contains(err.Error(), "unsupported payload type") {
			t.Fatalf("Encode() error = %v, want unsupported payload type", err)
		}
	})
}

func TestMessageCodecDecodeRejectsInvalidEnvelopes(t *testing.T) {
	oversizedCompressed, err := compressData(bytes.Repeat([]byte{'x'}, MaxDecodedPayloadSize+1))
	if err != nil {
		t.Fatalf("compressData() error = %v", err)
	}

	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{name: "empty", data: nil, wantErr: "empty data received"},
		{name: "malformed envelope", data: []byte{0xff}, wantErr: "unmarshal envelope"},
		{
			name:    "type too long",
			data:    marshalCodecProto(t, &pb.Envelope{Type: strings.Repeat("t", MaxEnvelopeTypeLength+1)}),
			wantErr: "message type too long",
		},
		{
			name:    "wire payload too large",
			data:    marshalCodecProto(t, &pb.Envelope{Type: "large", Payload: make([]byte, MaxReadMessageSize+1)}),
			wantErr: "payload too large",
		},
		{
			name:    "malformed compressed payload",
			data:    marshalCodecProto(t, &pb.Envelope{Type: "compressed", Payload: []byte("not gzip"), Compressed: true}),
			wantErr: "decompress payload",
		},
		{
			name:    "decoded payload too large",
			data:    marshalCodecProto(t, &pb.Envelope{Type: "compressed", Payload: oversizedCompressed, Compressed: true}),
			wantErr: "decompressed payload too large",
		},
	}

	codec := NewMessageCodec(true)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := codec.Decode(test.data)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Decode() error = %v, want error containing %q", err, test.wantErr)
			}
		})
	}
}

func TestCompressionHelpers(t *testing.T) {
	for _, input := range [][]byte{nil, []byte("small payload"), bytes.Repeat([]byte("abcdef"), 1000)} {
		compressed, err := compressData(input)
		if err != nil {
			t.Fatalf("compressData() error = %v", err)
		}
		decompressed, err := decompressData(compressed)
		if err != nil {
			t.Fatalf("decompressData() error = %v", err)
		}
		if !bytes.Equal(decompressed, input) {
			t.Fatalf("compression round trip = %q, want %q", decompressed, input)
		}
	}

	if _, err := decompressData([]byte("invalid gzip stream")); err == nil {
		t.Fatal("decompressData() accepted invalid gzip data")
	}
}

func TestToProtoMessageVariants(t *testing.T) {
	track := TrackInfo{ID: "track", Title: "title", Artist: "artist", Album: "album", Duration: 123, Thumbnail: "thumb", SuggestedBy: "user"}
	pbTrack := &pb.TrackInfo{Id: "track", Title: "title", Artist: "artist", Album: "album", Duration: 123, Thumbnail: "thumb", SuggestedBy: "user"}
	state := RoomState{RoomCode: "ROOM", HostID: "host", Users: []UserInfo{{UserID: "host", Username: "Host", IsHost: true, IsConnected: true}}, CurrentTrack: &track, IsPlaying: true, Position: 10, LastUpdate: 20, Volume: .75, Queue: []TrackInfo{track}}
	pbState := roomStateToProto(&state)
	playback := PlaybackActionPayload{Action: ActionChangeTrack, TrackID: "track", Position: 10, TrackInfo: &track, InsertNext: true, Queue: []TrackInfo{track}, QueueTitle: "queue", Volume: .75, ServerTime: 20}
	pbPlayback := &pb.PlaybackActionPayload{Action: ActionChangeTrack, TrackId: "track", Position: 10, TrackInfo: pbTrack, InsertNext: true, Queue: []*pb.TrackInfo{pbTrack}, QueueTitle: "queue", Volume: .75, ServerTime: 20}
	syncState := SyncStatePayload{CurrentTrack: &track, IsPlaying: true, Position: 10, LastUpdate: 20, Volume: .75, Queue: []TrackInfo{track}}
	pbSyncState := &pb.SyncStatePayload{CurrentTrack: pbTrack, IsPlaying: true, Position: 10, LastUpdate: 20, Volume: .75, Queue: []*pb.TrackInfo{pbTrack}}

	tests := []struct {
		name    string
		payload any
		want    proto.Message
	}{
		{"create room pointer", &CreateRoomPayload{Username: "name"}, &pb.CreateRoomPayload{Username: "name"}},
		{"join room pointer", &JoinRoomPayload{RoomCode: "ROOM", Username: "name"}, &pb.JoinRoomPayload{RoomCode: "ROOM", Username: "name"}},
		{"approve join pointer", &ApproveJoinPayload{UserID: "user"}, &pb.ApproveJoinPayload{UserId: "user"}},
		{"reject join pointer", &RejectJoinPayload{UserID: "user", Reason: "reason"}, &pb.RejectJoinPayload{UserId: "user", Reason: "reason"}},
		{"playback pointer", &playback, pbPlayback},
		{"buffer ready pointer", &BufferReadyPayload{TrackID: "track"}, &pb.BufferReadyPayload{TrackId: "track"}},
		{"kick user pointer", &KickUserPayload{UserID: "user", Reason: "reason"}, &pb.KickUserPayload{UserId: "user", Reason: "reason"}},
		{"transfer host pointer", &TransferHostPayload{NewHostID: "user"}, &pb.TransferHostPayload{NewHostId: "user"}},
		{"suggest track pointer", &SuggestTrackPayload{TrackInfo: &track}, &pb.SuggestTrackPayload{TrackInfo: pbTrack}},
		{"approve suggestion pointer", &ApproveSuggestionPayload{SuggestionID: "suggestion"}, &pb.ApproveSuggestionPayload{SuggestionId: "suggestion"}},
		{"reject suggestion pointer", &RejectSuggestionPayload{SuggestionID: "suggestion", Reason: "reason"}, &pb.RejectSuggestionPayload{SuggestionId: "suggestion", Reason: "reason"}},
		{"reconnect pointer", &ReconnectPayload{SessionToken: "token"}, &pb.ReconnectPayload{SessionToken: "token"}},
		{"client capabilities pointer", &ClientCapabilitiesPayload{SupportsProtobuf: true, SupportsCompression: true, ClientVersion: "1"}, &pb.ClientCapabilities{SupportsProtobuf: true, SupportsCompression: true, ClientVersion: "1"}},
		{"server capabilities pointer", &ServerCapabilitiesPayload{SupportsProtobuf: true, SupportsCompression: true, ServerVersion: "1"}, &pb.ServerCapabilities{SupportsProtobuf: true, SupportsCompression: true, ServerVersion: "1"}},
		{"room created pointer", &RoomCreatedPayload{RoomCode: "ROOM", UserID: "user", SessionToken: "token"}, &pb.RoomCreatedPayload{RoomCode: "ROOM", UserId: "user", SessionToken: "token"}},
		{"join request pointer", &JoinRequestPayload{UserID: "user", Username: "name"}, &pb.JoinRequestPayload{UserId: "user", Username: "name"}},
		{"join approved pointer", &JoinApprovedPayload{RoomCode: "ROOM", UserID: "user", SessionToken: "token", State: &state}, &pb.JoinApprovedPayload{RoomCode: "ROOM", UserId: "user", SessionToken: "token", State: pbState}},
		{"join rejected pointer", &JoinRejectedPayload{Reason: "reason"}, &pb.JoinRejectedPayload{Reason: "reason"}},
		{"user joined pointer", &UserJoinedPayload{UserID: "user", Username: "name"}, &pb.UserJoinedPayload{UserId: "user", Username: "name"}},
		{"user left pointer", &UserLeftPayload{UserID: "user", Username: "name"}, &pb.UserLeftPayload{UserId: "user", Username: "name"}},
		{"buffer wait pointer", &BufferWaitPayload{TrackID: "track", WaitingFor: []string{"user"}}, &pb.BufferWaitPayload{TrackId: "track", WaitingFor: []string{"user"}}},
		{"buffer complete pointer", &BufferCompletePayload{TrackID: "track"}, &pb.BufferCompletePayload{TrackId: "track"}},
		{"error pointer", &ErrorPayload{Code: "code", Message: "message"}, &pb.ErrorPayload{Code: "code", Message: "message"}},
		{"host changed pointer", &HostChangedPayload{NewHostID: "user", NewHostName: "name"}, &pb.HostChangedPayload{NewHostId: "user", NewHostName: "name"}},
		{"kicked pointer", &KickedPayload{Reason: "reason"}, &pb.KickedPayload{Reason: "reason"}},
		{"sync state pointer", &syncState, pbSyncState},
		{"reconnected pointer", &ReconnectedPayload{RoomCode: "ROOM", UserID: "user", State: &state, IsHost: true}, &pb.ReconnectedPayload{RoomCode: "ROOM", UserId: "user", State: pbState, IsHost: true}},
		{"user reconnected pointer", &UserReconnectedPayload{UserID: "user", Username: "name"}, &pb.UserReconnectedPayload{UserId: "user", Username: "name"}},
		{"user disconnected pointer", &UserDisconnectedPayload{UserID: "user", Username: "name"}, &pb.UserDisconnectedPayload{UserId: "user", Username: "name"}},
		{"suggestion received pointer", &SuggestionReceivedPayload{SuggestionID: "suggestion", FromUserID: "user", FromUsername: "name", TrackInfo: &track}, &pb.SuggestionReceivedPayload{SuggestionId: "suggestion", FromUserId: "user", FromUsername: "name", TrackInfo: pbTrack}},
		{"suggestion approved pointer", &SuggestionApprovedPayload{SuggestionID: "suggestion", TrackInfo: &track}, &pb.SuggestionApprovedPayload{SuggestionId: "suggestion", TrackInfo: pbTrack}},
		{"suggestion rejected pointer", &SuggestionRejectedPayload{SuggestionID: "suggestion", Reason: "reason"}, &pb.SuggestionRejectedPayload{SuggestionId: "suggestion", Reason: "reason"}},

		{"room created value", RoomCreatedPayload{RoomCode: "ROOM", UserID: "user", SessionToken: "token"}, &pb.RoomCreatedPayload{RoomCode: "ROOM", UserId: "user", SessionToken: "token"}},
		{"join request value", JoinRequestPayload{UserID: "user", Username: "name"}, &pb.JoinRequestPayload{UserId: "user", Username: "name"}},
		{"join approved value", JoinApprovedPayload{RoomCode: "ROOM", UserID: "user", SessionToken: "token", State: &state}, &pb.JoinApprovedPayload{RoomCode: "ROOM", UserId: "user", SessionToken: "token", State: pbState}},
		{"join rejected value", JoinRejectedPayload{Reason: "reason"}, &pb.JoinRejectedPayload{Reason: "reason"}},
		{"user joined value", UserJoinedPayload{UserID: "user", Username: "name"}, &pb.UserJoinedPayload{UserId: "user", Username: "name"}},
		{"user left value", UserLeftPayload{UserID: "user", Username: "name"}, &pb.UserLeftPayload{UserId: "user", Username: "name"}},
		{"buffer wait value", BufferWaitPayload{TrackID: "track", WaitingFor: []string{"user"}}, &pb.BufferWaitPayload{TrackId: "track", WaitingFor: []string{"user"}}},
		{"buffer complete value", BufferCompletePayload{TrackID: "track"}, &pb.BufferCompletePayload{TrackId: "track"}},
		{"error value", ErrorPayload{Code: "code", Message: "message"}, &pb.ErrorPayload{Code: "code", Message: "message"}},
		{"host changed value", HostChangedPayload{NewHostID: "user", NewHostName: "name"}, &pb.HostChangedPayload{NewHostId: "user", NewHostName: "name"}},
		{"kicked value", KickedPayload{Reason: "reason"}, &pb.KickedPayload{Reason: "reason"}},
		{"sync state value", syncState, pbSyncState},
		{"reconnected value", ReconnectedPayload{RoomCode: "ROOM", UserID: "user", State: &state, IsHost: true}, &pb.ReconnectedPayload{RoomCode: "ROOM", UserId: "user", State: pbState, IsHost: true}},
		{"user reconnected value", UserReconnectedPayload{UserID: "user", Username: "name"}, &pb.UserReconnectedPayload{UserId: "user", Username: "name"}},
		{"user disconnected value", UserDisconnectedPayload{UserID: "user", Username: "name"}, &pb.UserDisconnectedPayload{UserId: "user", Username: "name"}},
		{"suggestion received value", SuggestionReceivedPayload{SuggestionID: "suggestion", FromUserID: "user", FromUsername: "name", TrackInfo: &track}, &pb.SuggestionReceivedPayload{SuggestionId: "suggestion", FromUserId: "user", FromUsername: "name", TrackInfo: pbTrack}},
		{"suggestion approved value", SuggestionApprovedPayload{SuggestionID: "suggestion", TrackInfo: &track}, &pb.SuggestionApprovedPayload{SuggestionId: "suggestion", TrackInfo: pbTrack}},
		{"suggestion rejected value", SuggestionRejectedPayload{SuggestionID: "suggestion", Reason: "reason"}, &pb.SuggestionRejectedPayload{SuggestionId: "suggestion", Reason: "reason"}},
		{"playback value", playback, pbPlayback},
		{"server capabilities value", ServerCapabilitiesPayload{SupportsProtobuf: true, SupportsCompression: true, ServerVersion: "1"}, &pb.ServerCapabilities{SupportsProtobuf: true, SupportsCompression: true, ServerVersion: "1"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := toProtoMessage(test.payload)
			if err != nil {
				t.Fatalf("toProtoMessage() error = %v", err)
			}
			if !proto.Equal(got, test.want) {
				t.Fatalf("toProtoMessage() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestFromProtoMessageVariants(t *testing.T) {
	track := &pb.TrackInfo{Id: "track", Title: "title", Artist: "artist", Album: "album", Duration: 123, Thumbnail: "thumb", SuggestedBy: "user"}
	goTrack := TrackInfo{ID: "track", Title: "title", Artist: "artist", Album: "album", Duration: 123, Thumbnail: "thumb", SuggestedBy: "user"}

	tests := []struct {
		name    string
		msgType string
		input   proto.Message
		want    any
	}{
		{"create room", MsgTypeCreateRoom, &pb.CreateRoomPayload{Username: "name"}, &CreateRoomPayload{Username: "name"}},
		{"join room", MsgTypeJoinRoom, &pb.JoinRoomPayload{RoomCode: "ROOM", Username: "name"}, &JoinRoomPayload{RoomCode: "ROOM", Username: "name"}},
		{"approve join", MsgTypeApproveJoin, &pb.ApproveJoinPayload{UserId: "user"}, &ApproveJoinPayload{UserID: "user"}},
		{"reject join", MsgTypeRejectJoin, &pb.RejectJoinPayload{UserId: "user", Reason: "reason"}, &RejectJoinPayload{UserID: "user", Reason: "reason"}},
		{"playback action", MsgTypePlaybackAction, &pb.PlaybackActionPayload{Action: ActionPlay, TrackId: "track", Position: 10, TrackInfo: track, InsertNext: true, Queue: []*pb.TrackInfo{track}, QueueTitle: "queue", Volume: .75, ServerTime: 20}, &PlaybackActionPayload{Action: ActionPlay, TrackID: "track", Position: 10, TrackInfo: &goTrack, InsertNext: true, Queue: []TrackInfo{goTrack}, QueueTitle: "queue", Volume: float64(float32(.75)), ServerTime: 20}},
		{"buffer ready", MsgTypeBufferReady, &pb.BufferReadyPayload{TrackId: "track"}, &BufferReadyPayload{TrackID: "track"}},
		{"kick user", MsgTypeKickUser, &pb.KickUserPayload{UserId: "user", Reason: "reason"}, &KickUserPayload{UserID: "user", Reason: "reason"}},
		{"transfer host", MsgTypeTransferHost, &pb.TransferHostPayload{NewHostId: "user"}, &TransferHostPayload{NewHostID: "user"}},
		{"suggest track", MsgTypeSuggestTrack, &pb.SuggestTrackPayload{TrackInfo: track}, &SuggestTrackPayload{TrackInfo: &goTrack}},
		{"approve suggestion", MsgTypeApproveSuggestion, &pb.ApproveSuggestionPayload{SuggestionId: "suggestion"}, &ApproveSuggestionPayload{SuggestionID: "suggestion"}},
		{"reject suggestion", MsgTypeRejectSuggestion, &pb.RejectSuggestionPayload{SuggestionId: "suggestion", Reason: "reason"}, &RejectSuggestionPayload{SuggestionID: "suggestion", Reason: "reason"}},
		{"reconnect", MsgTypeReconnect, &pb.ReconnectPayload{SessionToken: "token"}, &ReconnectPayload{SessionToken: "token"}},
		{"client capabilities", MsgTypeClientCapabilities, &pb.ClientCapabilities{SupportsProtobuf: true, SupportsCompression: true, ClientVersion: "1"}, &ClientCapabilitiesPayload{SupportsProtobuf: true, SupportsCompression: true, ClientVersion: "1"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := fromProtoMessage(test.msgType, marshalCodecProto(t, test.input))
			if err != nil {
				t.Fatalf("fromProtoMessage() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("fromProtoMessage() = %#v, want %#v", got, test.want)
			}
		})
	}

	for _, test := range tests {
		t.Run("malformed "+test.name, func(t *testing.T) {
			if _, err := fromProtoMessage(test.msgType, []byte{0xff}); err == nil {
				t.Fatal("fromProtoMessage() accepted malformed protobuf")
			}
		})
	}

	if _, err := fromProtoMessage("not_supported", nil); err == nil || !strings.Contains(err.Error(), "unsupported message type") {
		t.Fatalf("fromProtoMessage() error = %v, want unsupported message type", err)
	}
}

func TestCodecConversionHelpers(t *testing.T) {
	track := &TrackInfo{ID: "id", Title: "title", Artist: "artist", Album: "album", Duration: 99, Thumbnail: "thumb", SuggestedBy: "user"}
	wantTrack := &pb.TrackInfo{Id: "id", Title: "title", Artist: "artist", Album: "album", Duration: 99, Thumbnail: "thumb", SuggestedBy: "user"}
	if got := trackInfoToProto(track); !proto.Equal(got, wantTrack) {
		t.Fatalf("trackInfoToProto() = %v, want %v", got, wantTrack)
	}
	if got := protoToTrackInfo(wantTrack); !reflect.DeepEqual(got, track) {
		t.Fatalf("protoToTrackInfo() = %#v, want %#v", got, track)
	}

	user := &UserInfo{UserID: "user", Username: "name", IsHost: true, IsConnected: true}
	wantUser := &pb.UserInfo{UserId: "user", Username: "name", IsHost: true, IsConnected: true}
	if got := userInfoToProto(user); !proto.Equal(got, wantUser) {
		t.Fatalf("userInfoToProto() = %v, want %v", got, wantUser)
	}

	state := &RoomState{RoomCode: "ROOM", HostID: "user", Users: []UserInfo{*user}, CurrentTrack: track, IsPlaying: true, Position: 10, LastUpdate: 20, Volume: .5, Queue: []TrackInfo{*track}}
	wantState := &pb.RoomState{RoomCode: "ROOM", HostId: "user", Users: []*pb.UserInfo{wantUser}, CurrentTrack: wantTrack, IsPlaying: true, Position: 10, LastUpdate: 20, Volume: .5, Queue: []*pb.TrackInfo{wantTrack}}
	if got := roomStateToProto(state); !proto.Equal(got, wantState) {
		t.Fatalf("roomStateToProto() = %v, want %v", got, wantState)
	}

	emptyState := roomStateToProto(&RoomState{})
	if emptyState.CurrentTrack != nil || emptyState.Users != nil || emptyState.Queue != nil {
		t.Fatalf("roomStateToProto(empty) populated optional fields: %v", emptyState)
	}
}

func TestDecodePayloadTargets(t *testing.T) {
	tests := []struct {
		name    string
		msgType string
		input   proto.Message
		target  any
		want    any
	}{
		{"create room", MsgTypeCreateRoom, &pb.CreateRoomPayload{Username: "name"}, &CreateRoomPayload{}, &CreateRoomPayload{Username: "name"}},
		{"join room", MsgTypeJoinRoom, &pb.JoinRoomPayload{RoomCode: "ROOM", Username: "name"}, &JoinRoomPayload{}, &JoinRoomPayload{RoomCode: "ROOM", Username: "name"}},
		{"approve join", MsgTypeApproveJoin, &pb.ApproveJoinPayload{UserId: "user"}, &ApproveJoinPayload{}, &ApproveJoinPayload{UserID: "user"}},
		{"reject join", MsgTypeRejectJoin, &pb.RejectJoinPayload{UserId: "user", Reason: "reason"}, &RejectJoinPayload{}, &RejectJoinPayload{UserID: "user", Reason: "reason"}},
		{"playback", MsgTypePlaybackAction, &pb.PlaybackActionPayload{Action: ActionPause}, &PlaybackActionPayload{}, &PlaybackActionPayload{Action: ActionPause}},
		{"buffer ready", MsgTypeBufferReady, &pb.BufferReadyPayload{TrackId: "track"}, &BufferReadyPayload{}, &BufferReadyPayload{TrackID: "track"}},
		{"kick user", MsgTypeKickUser, &pb.KickUserPayload{UserId: "user", Reason: "reason"}, &KickUserPayload{}, &KickUserPayload{UserID: "user", Reason: "reason"}},
		{"suggest track", MsgTypeSuggestTrack, &pb.SuggestTrackPayload{}, &SuggestTrackPayload{}, &SuggestTrackPayload{}},
		{"approve suggestion", MsgTypeApproveSuggestion, &pb.ApproveSuggestionPayload{SuggestionId: "suggestion"}, &ApproveSuggestionPayload{}, &ApproveSuggestionPayload{SuggestionID: "suggestion"}},
		{"reject suggestion", MsgTypeRejectSuggestion, &pb.RejectSuggestionPayload{SuggestionId: "suggestion", Reason: "reason"}, &RejectSuggestionPayload{}, &RejectSuggestionPayload{SuggestionID: "suggestion", Reason: "reason"}},
		{"reconnect", MsgTypeReconnect, &pb.ReconnectPayload{SessionToken: "token"}, &ReconnectPayload{}, &ReconnectPayload{SessionToken: "token"}},
		{"transfer host", MsgTypeTransferHost, &pb.TransferHostPayload{NewHostId: "user"}, &TransferHostPayload{}, &TransferHostPayload{NewHostID: "user"}},
		{"client capabilities", MsgTypeClientCapabilities, &pb.ClientCapabilities{SupportsProtobuf: true, ClientVersion: "1"}, &ClientCapabilitiesPayload{}, &ClientCapabilitiesPayload{SupportsProtobuf: true, ClientVersion: "1"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := decodePayload(marshalCodecProto(t, test.input), test.msgType, test.target); err != nil {
				t.Fatalf("decodePayload() error = %v", err)
			}
			if !reflect.DeepEqual(test.target, test.want) {
				t.Fatalf("decodePayload() target = %#v, want %#v", test.target, test.want)
			}
		})
	}
}

func TestDecodePayloadErrors(t *testing.T) {
	createRoom := marshalCodecProto(t, &pb.CreateRoomPayload{Username: "name"})
	tests := []struct {
		name    string
		payload []byte
		msgType string
		target  any
		wantErr string
	}{
		{"malformed payload", []byte{0xff}, MsgTypeCreateRoom, &CreateRoomPayload{}, "cannot parse invalid wire-format data"},
		{"unsupported message", nil, "not_supported", &CreateRoomPayload{}, "unsupported message type"},
		{"mismatched target", createRoom, MsgTypeCreateRoom, &JoinRoomPayload{}, "payload type mismatch"},
		{"unsupported target", createRoom, MsgTypeCreateRoom, &struct{}{}, "unsupported target type"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := decodePayload(test.payload, test.msgType, test.target)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("decodePayload() error = %v, want error containing %q", err, test.wantErr)
			}
		})
	}
}

func TestDecodePayloadMismatchedTargets(t *testing.T) {
	createRoom := marshalCodecProto(t, &pb.CreateRoomPayload{Username: "name"})
	joinRoom := marshalCodecProto(t, &pb.JoinRoomPayload{RoomCode: "ROOM", Username: "name"})
	tests := []struct {
		name    string
		payload []byte
		msgType string
		target  any
	}{
		{"create room", joinRoom, MsgTypeJoinRoom, &CreateRoomPayload{}},
		{"join room", createRoom, MsgTypeCreateRoom, &JoinRoomPayload{}},
		{"approve join", createRoom, MsgTypeCreateRoom, &ApproveJoinPayload{}},
		{"reject join", createRoom, MsgTypeCreateRoom, &RejectJoinPayload{}},
		{"playback", createRoom, MsgTypeCreateRoom, &PlaybackActionPayload{}},
		{"buffer ready", createRoom, MsgTypeCreateRoom, &BufferReadyPayload{}},
		{"kick user", createRoom, MsgTypeCreateRoom, &KickUserPayload{}},
		{"suggest track", createRoom, MsgTypeCreateRoom, &SuggestTrackPayload{}},
		{"approve suggestion", createRoom, MsgTypeCreateRoom, &ApproveSuggestionPayload{}},
		{"reject suggestion", createRoom, MsgTypeCreateRoom, &RejectSuggestionPayload{}},
		{"reconnect", createRoom, MsgTypeCreateRoom, &ReconnectPayload{}},
		{"transfer host", createRoom, MsgTypeCreateRoom, &TransferHostPayload{}},
		{"client capabilities", createRoom, MsgTypeCreateRoom, &ClientCapabilitiesPayload{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := decodePayload(test.payload, test.msgType, test.target)
			if err == nil || !strings.Contains(err.Error(), "payload type mismatch") {
				t.Fatalf("decodePayload() error = %v, want payload type mismatch", err)
			}
		})
	}
}
