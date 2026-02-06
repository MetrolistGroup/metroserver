package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"

	pb "github.com/nyxiereal/metroserver/proto"
	"google.golang.org/protobuf/proto"
)

// MessageFormat indicates the format of a message
type MessageFormat int

const (
	FormatJSON MessageFormat = iota
	FormatProtobuf
)

// MessageCodec handles encoding/decoding of messages in different formats
type MessageCodec struct {
	format             MessageFormat
	compressionEnabled bool
}

// NewMessageCodec creates a new codec with the specified format and compression settings
func NewMessageCodec(format MessageFormat, compression bool) *MessageCodec {
	return &MessageCodec{
		format:             format,
		compressionEnabled: compression,
	}
}

// detectMessageFormat detects if a message is JSON or Protobuf
// JSON messages start with '{', Protobuf messages start with a field tag
func detectMessageFormat(data []byte) MessageFormat {
	if len(data) == 0 {
		return FormatJSON // Default to JSON for empty messages
	}
	// JSON messages start with '{'
	if data[0] == '{' {
		return FormatJSON
	}
	// Protobuf messages have field tags (usually small numbers)
	return FormatProtobuf
}

// Encode encodes a message with the codec's format and compression settings
func (c *MessageCodec) Encode(msgType string, payload interface{}) ([]byte, error) {
	if c.format == FormatProtobuf {
		return c.encodeProtobuf(msgType, payload)
	}
	return c.encodeJSON(msgType, payload)
}

// Decode decodes a message, automatically detecting format
func (c *MessageCodec) Decode(data []byte) (string, []byte, error) {
	format := detectMessageFormat(data)

	if format == FormatProtobuf {
		return c.decodeProtobuf(data)
	}
	return c.decodeJSON(data)
}

// encodeJSON encodes a message as JSON (DEPRECATED - will be removed in future versions)
func (c *MessageCodec) encodeJSON(msgType string, payload interface{}) ([]byte, error) {
	msg := Message{
		Type: msgType,
	}

	if payload != nil {
		payloadJSON, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal payload: %w", err)
		}
		msg.Payload = payloadJSON
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}

	if c.compressionEnabled {
		return compressData(data)
	}

	return data, nil
}

// decodeJSON decodes a JSON message (DEPRECATED - will be removed in future versions)
func (c *MessageCodec) decodeJSON(data []byte) (string, []byte, error) {
	// Try to decompress if it looks compressed
	if c.compressionEnabled && len(data) > 2 && data[0] == 0x1f && data[1] == 0x8b {
		decompressed, err := decompressData(data)
		if err == nil {
			data = decompressed
		}
	}

	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return "", nil, fmt.Errorf("unmarshal message: %w", err)
	}

	return msg.Type, msg.Payload, nil
}

// encodeProtobuf encodes a message using Protocol Buffers
func (c *MessageCodec) encodeProtobuf(msgType string, payload interface{}) ([]byte, error) {
	var payloadBytes []byte

	if payload != nil {
		// Convert payload to protobuf message
		protoMsg, err := toProtoMessage(payload)
		if err != nil {
			return nil, fmt.Errorf("convert to proto: %w", err)
		}

		payloadBytes, err = proto.Marshal(protoMsg)
		if err != nil {
			return nil, fmt.Errorf("marshal proto payload: %w", err)
		}
	}

	// Compress payload if enabled
	compressed := false
	if c.compressionEnabled && len(payloadBytes) > 100 {
		compressedBytes, err := compressData(payloadBytes)
		if err == nil && len(compressedBytes) < len(payloadBytes) {
			payloadBytes = compressedBytes
			compressed = true
		}
	}

	envelope := &pb.Envelope{
		Type:       msgType,
		Payload:    payloadBytes,
		Compressed: compressed,
	}

	return proto.Marshal(envelope)
}

// decodeProtobuf decodes a protobuf message
func (c *MessageCodec) decodeProtobuf(data []byte) (string, []byte, error) {
	envelope := &pb.Envelope{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		return "", nil, fmt.Errorf("unmarshal envelope: %w", err)
	}

	payloadBytes := envelope.Payload
	if envelope.Compressed {
		decompressed, err := decompressData(payloadBytes)
		if err != nil {
			return "", nil, fmt.Errorf("decompress payload: %w", err)
		}
		payloadBytes = decompressed
	}

	return envelope.Type, payloadBytes, nil
}

// compressData compresses data using gzip
func compressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// decompressData decompresses gzip data
func decompressData(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

// toProtoMessage converts Go structs to protobuf messages
func toProtoMessage(payload interface{}) (proto.Message, error) {
	switch p := payload.(type) {
	case *CreateRoomPayload:
		return &pb.CreateRoomPayload{Username: p.Username}, nil
	case *JoinRoomPayload:
		return &pb.JoinRoomPayload{RoomCode: p.RoomCode, Username: p.Username}, nil
	case *ApproveJoinPayload:
		return &pb.ApproveJoinPayload{UserId: p.UserID}, nil
	case *RejectJoinPayload:
		return &pb.RejectJoinPayload{UserId: p.UserID, Reason: p.Reason}, nil
	case *PlaybackActionPayload:
		pbPayload := &pb.PlaybackActionPayload{
			Action:     p.Action,
			TrackId:    p.TrackID,
			Position:   p.Position,
			InsertNext: p.InsertNext,
			QueueTitle: p.QueueTitle,
			Volume:     float32(p.Volume),
			ServerTime: p.ServerTime,
		}
		if p.TrackInfo != nil {
			pbPayload.TrackInfo = trackInfoToProto(p.TrackInfo)
		}
		if p.Queue != nil {
			pbPayload.Queue = make([]*pb.TrackInfo, len(p.Queue))
			for i, track := range p.Queue {
				pbPayload.Queue[i] = trackInfoToProto(&track)
			}
		}
		return pbPayload, nil
	case *BufferReadyPayload:
		return &pb.BufferReadyPayload{TrackId: p.TrackID}, nil
	case *KickUserPayload:
		return &pb.KickUserPayload{UserId: p.UserID, Reason: p.Reason}, nil
	case *SuggestTrackPayload:
		pbPayload := &pb.SuggestTrackPayload{}
		if p.TrackInfo != nil {
			pbPayload.TrackInfo = trackInfoToProto(p.TrackInfo)
		}
		return pbPayload, nil
	case *ApproveSuggestionPayload:
		return &pb.ApproveSuggestionPayload{SuggestionId: p.SuggestionID}, nil
	case *RejectSuggestionPayload:
		return &pb.RejectSuggestionPayload{SuggestionId: p.SuggestionID, Reason: p.Reason}, nil
	case *ReconnectPayload:
		return &pb.ReconnectPayload{SessionToken: p.SessionToken}, nil
	case *RoomCreatedPayload:
		return &pb.RoomCreatedPayload{
			RoomCode:     p.RoomCode,
			UserId:       p.UserID,
			SessionToken: p.SessionToken,
		}, nil
	case *JoinRequestPayload:
		return &pb.JoinRequestPayload{UserId: p.UserID, Username: p.Username}, nil
	case *JoinApprovedPayload:
		pbPayload := &pb.JoinApprovedPayload{
			RoomCode:     p.RoomCode,
			UserId:       p.UserID,
			SessionToken: p.SessionToken,
		}
		if p.State != nil {
			pbPayload.State = roomStateToProto(p.State)
		}
		return pbPayload, nil
	case *JoinRejectedPayload:
		return &pb.JoinRejectedPayload{Reason: p.Reason}, nil
	case *UserJoinedPayload:
		return &pb.UserJoinedPayload{UserId: p.UserID, Username: p.Username}, nil
	case *UserLeftPayload:
		return &pb.UserLeftPayload{UserId: p.UserID, Username: p.Username}, nil
	case *BufferWaitPayload:
		return &pb.BufferWaitPayload{TrackId: p.TrackID, WaitingFor: p.WaitingFor}, nil
	case *BufferCompletePayload:
		return &pb.BufferCompletePayload{TrackId: p.TrackID}, nil
	case *ErrorPayload:
		return &pb.ErrorPayload{Code: p.Code, Message: p.Message}, nil
	case *HostChangedPayload:
		return &pb.HostChangedPayload{NewHostId: p.NewHostID, NewHostName: p.NewHostName}, nil
	case *KickedPayload:
		return &pb.KickedPayload{Reason: p.Reason}, nil
	case *SyncStatePayload:
		pbPayload := &pb.SyncStatePayload{
			IsPlaying:  p.IsPlaying,
			Position:   p.Position,
			LastUpdate: p.LastUpdate,
			Volume:     float32(p.Volume),
		}
		if p.CurrentTrack != nil {
			pbPayload.CurrentTrack = trackInfoToProto(p.CurrentTrack)
		}
		return pbPayload, nil
	case *ReconnectedPayload:
		pbPayload := &pb.ReconnectedPayload{
			RoomCode: p.RoomCode,
			UserId:   p.UserID,
			IsHost:   p.IsHost,
		}
		if p.State != nil {
			pbPayload.State = roomStateToProto(p.State)
		}
		return pbPayload, nil
	case *UserReconnectedPayload:
		return &pb.UserReconnectedPayload{UserId: p.UserID, Username: p.Username}, nil
	case *UserDisconnectedPayload:
		return &pb.UserDisconnectedPayload{UserId: p.UserID, Username: p.Username}, nil
	case *SuggestionReceivedPayload:
		pbPayload := &pb.SuggestionReceivedPayload{
			SuggestionId: p.SuggestionID,
			FromUserId:   p.FromUserID,
			FromUsername: p.FromUsername,
		}
		if p.TrackInfo != nil {
			pbPayload.TrackInfo = trackInfoToProto(p.TrackInfo)
		}
		return pbPayload, nil
	case *SuggestionApprovedPayload:
		pbPayload := &pb.SuggestionApprovedPayload{SuggestionId: p.SuggestionID}
		if p.TrackInfo != nil {
			pbPayload.TrackInfo = trackInfoToProto(p.TrackInfo)
		}
		return pbPayload, nil
	case *SuggestionRejectedPayload:
		return &pb.SuggestionRejectedPayload{SuggestionId: p.SuggestionID, Reason: p.Reason}, nil
	default:
		return nil, fmt.Errorf("unsupported payload type: %T", payload)
	}
}

// fromProtoMessage converts protobuf messages to Go structs
func fromProtoMessage(msgType string, data []byte) (interface{}, error) {
	switch msgType {
	case MsgTypeCreateRoom:
		var pbb pb.CreateRoomPayload
		if err := proto.Unmarshal(data, &pbb); err != nil {
			return nil, err
		}
		return &CreateRoomPayload{Username: pbb.Username}, nil
	case MsgTypeJoinRoom:
		var pbb pb.JoinRoomPayload
		if err := proto.Unmarshal(data, &pbb); err != nil {
			return nil, err
		}
		return &JoinRoomPayload{RoomCode: pbb.RoomCode, Username: pbb.Username}, nil
	case MsgTypeApproveJoin:
		var pbb pb.ApproveJoinPayload
		if err := proto.Unmarshal(data, &pbb); err != nil {
			return nil, err
		}
		return &ApproveJoinPayload{UserID: pbb.UserId}, nil
	case MsgTypeRejectJoin:
		var pbb pb.RejectJoinPayload
		if err := proto.Unmarshal(data, &pbb); err != nil {
			return nil, err
		}
		return &RejectJoinPayload{UserID: pbb.UserId, Reason: pbb.Reason}, nil
	case MsgTypePlaybackAction:
		var pbMsg pb.PlaybackActionPayload
		if err := proto.Unmarshal(data, &pbMsg); err != nil {
			return nil, err
		}
		payload := &PlaybackActionPayload{
			Action:     pbMsg.Action,
			TrackID:    pbMsg.TrackId,
			Position:   pbMsg.Position,
			InsertNext: pbMsg.InsertNext,
			QueueTitle: pbMsg.QueueTitle,
			Volume:     float64(pbMsg.Volume),
			ServerTime: pbMsg.ServerTime,
		}
		if pbMsg.TrackInfo != nil {
			payload.TrackInfo = protoToTrackInfo(pbMsg.TrackInfo)
		}
		if pbMsg.Queue != nil {
			payload.Queue = make([]TrackInfo, len(pbMsg.Queue))
			for i, track := range pbMsg.Queue {
				payload.Queue[i] = *protoToTrackInfo(track)
			}
		}
		return payload, nil
	case MsgTypeBufferReady:
		var pb pb.BufferReadyPayload
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return &BufferReadyPayload{TrackID: pb.TrackId}, nil
	case MsgTypeKickUser:
		var pb pb.KickUserPayload
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return &KickUserPayload{UserID: pb.UserId, Reason: pb.Reason}, nil
	case MsgTypeSuggestTrack:
		var pbMsg pb.SuggestTrackPayload
		if err := proto.Unmarshal(data, &pbMsg); err != nil {
			return nil, err
		}
		payload := &SuggestTrackPayload{}
		if pbMsg.TrackInfo != nil {
			payload.TrackInfo = protoToTrackInfo(pbMsg.TrackInfo)
		}
		return payload, nil
	case MsgTypeApproveSuggestion:
		var pb pb.ApproveSuggestionPayload
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return &ApproveSuggestionPayload{SuggestionID: pb.SuggestionId}, nil
	case MsgTypeRejectSuggestion:
		var pb pb.RejectSuggestionPayload
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return &RejectSuggestionPayload{SuggestionID: pb.SuggestionId, Reason: pb.Reason}, nil
	case MsgTypeReconnect:
		var pb pb.ReconnectPayload
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return &ReconnectPayload{SessionToken: pb.SessionToken}, nil
	default:
		return nil, fmt.Errorf("unsupported message type: %s", msgType)
	}
}

// Helper functions for converting between Go and Proto types

func trackInfoToProto(t *TrackInfo) *pb.TrackInfo {
	return &pb.TrackInfo{
		Id:          t.ID,
		Title:       t.Title,
		Artist:      t.Artist,
		Album:       t.Album,
		Duration:    t.Duration,
		Thumbnail:   t.Thumbnail,
		SuggestedBy: t.SuggestedBy,
	}
}

func protoToTrackInfo(p *pb.TrackInfo) *TrackInfo {
	return &TrackInfo{
		ID:          p.Id,
		Title:       p.Title,
		Artist:      p.Artist,
		Album:       p.Album,
		Duration:    p.Duration,
		Thumbnail:   p.Thumbnail,
		SuggestedBy: p.SuggestedBy,
	}
}

func userInfoToProto(u *UserInfo) *pb.UserInfo {
	return &pb.UserInfo{
		UserId:      u.UserID,
		Username:    u.Username,
		IsHost:      u.IsHost,
		IsConnected: u.IsConnected,
	}
}

func roomStateToProto(r *RoomState) *pb.RoomState {
	pbState := &pb.RoomState{
		RoomCode:   r.RoomCode,
		HostId:     r.HostID,
		IsPlaying:  r.IsPlaying,
		Position:   r.Position,
		LastUpdate: r.LastUpdate,
		Volume:     float32(r.Volume),
	}

	if r.CurrentTrack != nil {
		pbState.CurrentTrack = trackInfoToProto(r.CurrentTrack)
	}

	if r.Users != nil {
		pbState.Users = make([]*pb.UserInfo, len(r.Users))
		for i, user := range r.Users {
			pbState.Users[i] = userInfoToProto(&user)
		}
	}

	if r.Queue != nil {
		pbState.Queue = make([]*pb.TrackInfo, len(r.Queue))
		for i, track := range r.Queue {
			pbState.Queue[i] = trackInfoToProto(&track)
		}
	}

	return pbState
}

// Helper utility functions

func formatToString(f MessageFormat) string {
	if f == FormatProtobuf {
		return "protobuf"
	}
	return "json"
}

// decodePayload decodes a payload into the target interface based on format
func decodePayload(payloadBytes []byte, format MessageFormat, msgType string, target interface{}) error {
	if format == FormatProtobuf {
		// Use fromProtoMessage to convert protobuf to Go struct
		payload, err := fromProtoMessage(msgType, payloadBytes)
		if err != nil {
			return err
		}
		// Copy the decoded payload to target
		// This is a bit hacky but works for our use case
		targetVal := target
		switch t := targetVal.(type) {
		case *CreateRoomPayload:
			*t = *payload.(*CreateRoomPayload)
		case *JoinRoomPayload:
			*t = *payload.(*JoinRoomPayload)
		case *ApproveJoinPayload:
			*t = *payload.(*ApproveJoinPayload)
		case *RejectJoinPayload:
			*t = *payload.(*RejectJoinPayload)
		case *PlaybackActionPayload:
			*t = *payload.(*PlaybackActionPayload)
		case *BufferReadyPayload:
			*t = *payload.(*BufferReadyPayload)
		case *KickUserPayload:
			*t = *payload.(*KickUserPayload)
		case *SuggestTrackPayload:
			*t = *payload.(*SuggestTrackPayload)
		case *ApproveSuggestionPayload:
			*t = *payload.(*ApproveSuggestionPayload)
		case *RejectSuggestionPayload:
			*t = *payload.(*RejectSuggestionPayload)
		case *ReconnectPayload:
			*t = *payload.(*ReconnectPayload)
		default:
			return fmt.Errorf("unsupported target type: %T", target)
		}
		return nil
	}

	// JSON decode (DEPRECATED - will be removed in future versions)
	return json.Unmarshal(payloadBytes, target)
}
