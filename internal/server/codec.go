package server

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	pb "github.com/MetrolistGroup/metroserver/proto"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	MaxEnvelopeTypeLength = 64
	MaxDecodedPayloadSize = 2 * 1024 * 1024 // matches the KMP client's decoded limit
	MaxQueueInputSize     = MaxQueueSize * 2
	MaxClientFeatures     = 32
	MaxFeatureNameLength  = 64
)

// errMessageTooLarge marks size-limit failures so they are not reported as
// malformed input.
var errMessageTooLarge = errors.New("message too large")

// MessageCodec handles encoding/decoding of messages using Protocol Buffers
type MessageCodec struct {
	compressionEnabled atomic.Bool
}

// NewMessageCodec creates a new codec with compression settings
func NewMessageCodec(compression bool) *MessageCodec {
	codec := &MessageCodec{}
	codec.compressionEnabled.Store(compression)
	return codec
}

func (c *MessageCodec) setCompressionEnabled(enabled bool) {
	c.compressionEnabled.Store(enabled)
}

// Encode encodes a message using Protocol Buffers
func (c *MessageCodec) Encode(msgType string, payload interface{}) ([]byte, error) {
	return c.encodeProtobuf(msgType, payload, 0)
}

// EncodeReply encodes a message that answers the request with requestID.
func (c *MessageCodec) EncodeReply(msgType string, payload interface{}, requestID uint64) ([]byte, error) {
	return c.encodeProtobuf(msgType, payload, requestID)
}

// Decode decodes a protobuf message
func (c *MessageCodec) Decode(data []byte) (string, []byte, error) {
	msgType, payload, _, err := c.decodeProtobuf(data)
	return msgType, payload, err
}

// DecodeRequest decodes a protobuf message and its envelope request ID.
func (c *MessageCodec) DecodeRequest(data []byte) (string, []byte, uint64, error) {
	return c.decodeProtobuf(data)
}

// encodeProtobuf encodes a message using Protocol Buffers
func (c *MessageCodec) encodeProtobuf(msgType string, payload interface{}, requestID uint64) ([]byte, error) {
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
	if c.compressionEnabled.Load() && len(payloadBytes) > 100 {
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
		RequestId:  requestID,
	}

	envelopeBytes, err := proto.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope: %w", err)
	}

	return envelopeBytes, nil
}

// decodeProtobuf decodes a protobuf message
func (c *MessageCodec) decodeProtobuf(data []byte) (string, []byte, uint64, error) {
	if len(data) == 0 {
		return "", nil, 0, fmt.Errorf("empty data received")
	}

	envelope := &pb.Envelope{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		return "", nil, 0, fmt.Errorf("unmarshal envelope (received %d bytes): %w", len(data), err)
	}
	if len(envelope.Type) > MaxEnvelopeTypeLength {
		return "", nil, envelope.RequestId, fmt.Errorf("message type too long")
	}
	if len(envelope.Payload) > MaxReadMessageSize {
		return "", nil, envelope.RequestId, fmt.Errorf("payload: %w", errMessageTooLarge)
	}

	payloadBytes := envelope.Payload
	if envelope.Compressed {
		decompressed, err := decompressData(payloadBytes)
		if err != nil {
			return "", nil, envelope.RequestId, fmt.Errorf("decompress payload: %w", err)
		}
		payloadBytes = decompressed
	}
	if len(payloadBytes) > MaxDecodedPayloadSize {
		return "", nil, envelope.RequestId, fmt.Errorf("decoded payload: %w", errMessageTooLarge)
	}

	return envelope.Type, payloadBytes, envelope.RequestId, nil
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

	limited := io.LimitReader(reader, MaxDecodedPayloadSize+1)
	out, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(out) > MaxDecodedPayloadSize {
		return nil, fmt.Errorf("decompressed payload: %w", errMessageTooLarge)
	}
	return out, nil
}

// toProtoMessage converts Go structs to protobuf messages
func toProtoMessage(payload interface{}) (proto.Message, error) {
	switch p := payload.(type) {
	// Pointer types (from pending actions)
	case *CreateRoomPayload:
		return &pb.CreateRoomPayload{Username: p.Username}, nil
	case *JoinRoomPayload:
		return &pb.JoinRoomPayload{RoomCode: p.RoomCode, Username: p.Username}, nil
	case *ApproveJoinPayload:
		return &pb.ApproveJoinPayload{UserId: p.UserID}, nil
	case *RejectJoinPayload:
		return &pb.RejectJoinPayload{UserId: p.UserID, Reason: p.Reason}, nil
	case *PlaybackActionPayload:
		return toProtoMessage(*p)
	case *BufferReadyPayload:
		return &pb.BufferReadyPayload{TrackId: p.TrackID, TrackRevision: p.TrackRevision}, nil
	case *PingPayload:
		return toProtoMessage(*p)
	case *PongPayload:
		return toProtoMessage(*p)
	case *KickUserPayload:
		return &pb.KickUserPayload{UserId: p.UserID, Reason: p.Reason}, nil
	case *TransferHostPayload:
		return &pb.TransferHostPayload{NewHostId: p.NewHostID}, nil
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
	case *ClientCapabilitiesPayload:
		return &pb.ClientCapabilities{SupportsProtobuf: p.SupportsProtobuf, SupportsCompression: p.SupportsCompression, ClientVersion: p.ClientVersion, Features: p.Features}, nil
	case *ServerCapabilitiesPayload:
		return toProtoMessage(*p)
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
		return toProtoMessage(*p)
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

	// Value types (from sendMessage)
	case RoomCreatedPayload:
		return &pb.RoomCreatedPayload{
			RoomCode:     p.RoomCode,
			UserId:       p.UserID,
			SessionToken: p.SessionToken,
		}, nil
	case JoinRequestPayload:
		return &pb.JoinRequestPayload{UserId: p.UserID, Username: p.Username}, nil
	case JoinApprovedPayload:
		pbPayload := &pb.JoinApprovedPayload{
			RoomCode:     p.RoomCode,
			UserId:       p.UserID,
			SessionToken: p.SessionToken,
		}
		if p.State != nil {
			pbPayload.State = roomStateToProto(p.State)
		}
		return pbPayload, nil
	case JoinRejectedPayload:
		return &pb.JoinRejectedPayload{Reason: p.Reason}, nil
	case UserJoinedPayload:
		return &pb.UserJoinedPayload{UserId: p.UserID, Username: p.Username}, nil
	case UserLeftPayload:
		return &pb.UserLeftPayload{UserId: p.UserID, Username: p.Username}, nil
	case BufferWaitPayload:
		return &pb.BufferWaitPayload{TrackId: p.TrackID, WaitingFor: p.WaitingFor}, nil
	case BufferCompletePayload:
		return &pb.BufferCompletePayload{TrackId: p.TrackID}, nil
	case ErrorPayload:
		return &pb.ErrorPayload{Code: p.Code, Message: p.Message}, nil
	case HostChangedPayload:
		return &pb.HostChangedPayload{NewHostId: p.NewHostID, NewHostName: p.NewHostName}, nil
	case KickedPayload:
		return &pb.KickedPayload{Reason: p.Reason}, nil
	case SyncStatePayload:
		pbPayload := &pb.SyncStatePayload{
			IsPlaying:  p.IsPlaying,
			Position:   p.Position,
			LastUpdate: p.LastUpdate,
			Volume:     float32(p.Volume),
			Revision:   p.Revision,
			QueueTitle: p.QueueTitle,
		}
		if p.CurrentTrack != nil {
			pbPayload.CurrentTrack = trackInfoToProto(p.CurrentTrack)
		}
		if p.Queue != nil {
			pbPayload.Queue = make([]*pb.TrackInfo, len(p.Queue))
			for i, track := range p.Queue {
				pbPayload.Queue[i] = trackInfoToProto(&track)
			}
		}
		return pbPayload, nil
	case ReconnectedPayload:
		pbPayload := &pb.ReconnectedPayload{
			RoomCode: p.RoomCode,
			UserId:   p.UserID,
			IsHost:   p.IsHost,
		}
		if p.State != nil {
			pbPayload.State = roomStateToProto(p.State)
		}
		return pbPayload, nil
	case UserReconnectedPayload:
		return &pb.UserReconnectedPayload{UserId: p.UserID, Username: p.Username}, nil
	case UserDisconnectedPayload:
		return &pb.UserDisconnectedPayload{UserId: p.UserID, Username: p.Username}, nil
	case SuggestionReceivedPayload:
		pbPayload := &pb.SuggestionReceivedPayload{
			SuggestionId: p.SuggestionID,
			FromUserId:   p.FromUserID,
			FromUsername: p.FromUsername,
		}
		if p.TrackInfo != nil {
			pbPayload.TrackInfo = trackInfoToProto(p.TrackInfo)
		}
		return pbPayload, nil
	case SuggestionApprovedPayload:
		pbPayload := &pb.SuggestionApprovedPayload{SuggestionId: p.SuggestionID}
		if p.TrackInfo != nil {
			pbPayload.TrackInfo = trackInfoToProto(p.TrackInfo)
		}
		return pbPayload, nil
	case SuggestionRejectedPayload:
		return &pb.SuggestionRejectedPayload{SuggestionId: p.SuggestionID, Reason: p.Reason}, nil
	case PlaybackActionPayload:
		pbPayload := &pb.PlaybackActionPayload{
			Action:               p.Action,
			TrackId:              p.TrackID,
			Position:             p.Position,
			InsertNext:           p.InsertNext,
			QueueTitle:           p.QueueTitle,
			Volume:               float32(p.Volume),
			ServerTime:           p.ServerTime,
			Revision:             p.Revision,
			CapturedAtServerTime: p.CapturedAtServerTime,
			ReplaceQueue:         p.ReplaceQueue,
			OriginUserId:         p.OriginUserID,
			HostSequence:         p.HostSequence,
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
	case PingPayload:
		return &pb.PingPayload{
			ClientTime: p.ClientTime, Sequence: p.Sequence,
			HasPosition: p.HasPosition, TrackId: p.TrackID, Position: p.Position, PositionCapturedAtServerTime: p.PositionCapturedAtServerTime,
			HasStateHash: p.HasStateHash, StateHash: p.StateHash,
		}, nil
	case PongPayload:
		return &pb.PongPayload{
			ClientTime: p.ClientTime, ServerReceiveTime: p.ServerReceiveTime, ServerSendTime: p.ServerSendTime, Sequence: p.Sequence,
			DriftValid: p.DriftValid, DriftMs: p.DriftMs, StateMismatch: p.StateMismatch,
		}, nil
	case ServerCapabilitiesPayload:
		return &pb.ServerCapabilities{SupportsProtobuf: p.SupportsProtobuf, SupportsCompression: p.SupportsCompression, ServerVersion: p.ServerVersion, Features: p.Features, CanHost: p.CanHost}, nil
	case ServerRestartingPayload:
		return &pb.ServerRestartingPayload{EtaMs: p.EtaMs, Reason: p.Reason}, nil
	case SuggestionSubmittedPayload:
		pbPayload := &pb.SuggestionSubmittedPayload{SuggestionId: p.SuggestionID}
		if p.TrackInfo != nil {
			pbPayload.TrackInfo = trackInfoToProto(p.TrackInfo)
		}
		return pbPayload, nil

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
		if err := validatePlaybackActionCardinality(data); err != nil {
			return nil, err
		}
		var pbMsg pb.PlaybackActionPayload
		if err := proto.Unmarshal(data, &pbMsg); err != nil {
			return nil, err
		}
		payload := &PlaybackActionPayload{
			Action:               pbMsg.Action,
			TrackID:              pbMsg.TrackId,
			Position:             pbMsg.Position,
			InsertNext:           pbMsg.InsertNext,
			QueueTitle:           pbMsg.QueueTitle,
			Volume:               float64(pbMsg.Volume),
			ServerTime:           pbMsg.ServerTime,
			Revision:             pbMsg.Revision,
			CapturedAtServerTime: pbMsg.CapturedAtServerTime,
			ReplaceQueue:         pbMsg.ReplaceQueue,
			HostSequence:         pbMsg.HostSequence,
		}
		if pbMsg.TrackInfo != nil {
			payload.TrackInfo = protoToTrackInfo(pbMsg.TrackInfo)
		}
		if pbMsg.Queue != nil {
			if len(pbMsg.Queue) > MaxQueueInputSize {
				return nil, fmt.Errorf("queue has more than %d entries", MaxQueueInputSize)
			}
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
		return &BufferReadyPayload{TrackID: pb.TrackId, TrackRevision: pb.TrackRevision}, nil
	case MsgTypePing:
		var pbPayload pb.PingPayload
		if err := proto.Unmarshal(data, &pbPayload); err != nil {
			return nil, err
		}
		return &PingPayload{
			ClientTime: pbPayload.ClientTime, Sequence: pbPayload.Sequence,
			HasPosition: pbPayload.HasPosition, TrackID: pbPayload.TrackId, Position: pbPayload.Position,
			PositionCapturedAtServerTime: pbPayload.PositionCapturedAtServerTime,
			HasStateHash:                 pbPayload.HasStateHash, StateHash: pbPayload.StateHash,
		}, nil
	case MsgTypeKickUser:
		var pb pb.KickUserPayload
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return &KickUserPayload{UserID: pb.UserId, Reason: pb.Reason}, nil
	case MsgTypeTransferHost:
		var pb pb.TransferHostPayload
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return &TransferHostPayload{NewHostID: pb.NewHostId}, nil
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
	case MsgTypeClientCapabilities:
		var pb pb.ClientCapabilities
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		if len(pb.Features) > MaxClientFeatures {
			return nil, fmt.Errorf("more than %d client features", MaxClientFeatures)
		}
		for _, feature := range pb.Features {
			if len(feature) > MaxFeatureNameLength {
				return nil, fmt.Errorf("client feature name too long")
			}
		}
		return &ClientCapabilitiesPayload{SupportsProtobuf: pb.SupportsProtobuf, SupportsCompression: pb.SupportsCompression, ClientVersion: pb.ClientVersion, Features: pb.Features}, nil
	default:
		return nil, fmt.Errorf("unsupported message type: %s", msgType)
	}
}

func validatePlaybackActionCardinality(data []byte) error {
	const queueFieldNumber = 6
	queueEntries := 0
	for len(data) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(data)
		if tagLength < 0 {
			return protowire.ParseError(tagLength)
		}
		data = data[tagLength:]
		fieldLength := protowire.ConsumeFieldValue(number, wireType, data)
		if fieldLength < 0 {
			return protowire.ParseError(fieldLength)
		}
		if number == queueFieldNumber && wireType == protowire.BytesType {
			queueEntries++
			if queueEntries > MaxQueueInputSize {
				return fmt.Errorf("queue has more than %d entries", MaxQueueInputSize)
			}
		}
		data = data[fieldLength:]
	}
	return nil
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
		Revision:   r.Revision,
		QueueTitle: r.QueueTitle,
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

	if r.Queue != nil && len(r.Queue) > 0 {
		pbState.Queue = make([]*pb.TrackInfo, len(r.Queue))
		for i, track := range r.Queue {
			pbState.Queue[i] = trackInfoToProto(&track)
		}
	}

	return pbState
}

// decodePayload decodes a protobuf payload into the target interface
func decodePayload(payloadBytes []byte, msgType string, target interface{}) error {
	// Use fromProtoMessage to convert protobuf to Go struct
	payload, err := fromProtoMessage(msgType, payloadBytes)
	if err != nil {
		return err
	}
	// Copy the decoded payload to target using safe type assertion
	targetVal := target
	switch t := targetVal.(type) {
	case *CreateRoomPayload:
		p, ok := payload.(*CreateRoomPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected CreateRoomPayload, got %T", payload)
		}
		*t = *p
	case *JoinRoomPayload:
		p, ok := payload.(*JoinRoomPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected JoinRoomPayload, got %T", payload)
		}
		*t = *p
	case *ApproveJoinPayload:
		p, ok := payload.(*ApproveJoinPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected ApproveJoinPayload, got %T", payload)
		}
		*t = *p
	case *RejectJoinPayload:
		p, ok := payload.(*RejectJoinPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected RejectJoinPayload, got %T", payload)
		}
		*t = *p
	case *PlaybackActionPayload:
		p, ok := payload.(*PlaybackActionPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected PlaybackActionPayload, got %T", payload)
		}
		*t = *p
	case *BufferReadyPayload:
		p, ok := payload.(*BufferReadyPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected BufferReadyPayload, got %T", payload)
		}
		*t = *p
	case *PingPayload:
		p, ok := payload.(*PingPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected PingPayload, got %T", payload)
		}
		*t = *p
	case *KickUserPayload:
		p, ok := payload.(*KickUserPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected KickUserPayload, got %T", payload)
		}
		*t = *p
	case *SuggestTrackPayload:
		p, ok := payload.(*SuggestTrackPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected SuggestTrackPayload, got %T", payload)
		}
		*t = *p
	case *ApproveSuggestionPayload:
		p, ok := payload.(*ApproveSuggestionPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected ApproveSuggestionPayload, got %T", payload)
		}
		*t = *p
	case *RejectSuggestionPayload:
		p, ok := payload.(*RejectSuggestionPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected RejectSuggestionPayload, got %T", payload)
		}
		*t = *p
	case *ReconnectPayload:
		p, ok := payload.(*ReconnectPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected ReconnectPayload, got %T", payload)
		}
		*t = *p
	case *TransferHostPayload:
		p, ok := payload.(*TransferHostPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected TransferHostPayload, got %T", payload)
		}
		*t = *p
	case *ClientCapabilitiesPayload:
		p, ok := payload.(*ClientCapabilitiesPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected ClientCapabilitiesPayload, got %T", payload)
		}
		*t = *p
	default:
		return fmt.Errorf("unsupported target type: %T", target)
	}
	return nil
}
