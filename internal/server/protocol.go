package server

const (
	// Client -> Server
	MsgTypeCreateRoom         = "create_room"
	MsgTypeJoinRoom           = "join_room"
	MsgTypeLeaveRoom          = "leave_room"
	MsgTypeApproveJoin        = "approve_join"
	MsgTypeRejectJoin         = "reject_join"
	MsgTypePlaybackAction     = "playback_action"
	MsgTypeBufferReady        = "buffer_ready"
	MsgTypeKickUser           = "kick_user"
	MsgTypeTransferHost       = "transfer_host"
	MsgTypePing               = "ping"
	MsgTypeRequestSync        = "request_sync"
	MsgTypeReconnect          = "reconnect"
	MsgTypeSuggestTrack       = "suggest_track"
	MsgTypeApproveSuggestion  = "approve_suggestion"
	MsgTypeRejectSuggestion   = "reject_suggestion"
	MsgTypeClientCapabilities = "client_capabilities"

	// Server -> Client
	MsgTypeRoomCreated        = "room_created"
	MsgTypeJoinRequest        = "join_request"
	MsgTypeJoinApproved       = "join_approved"
	MsgTypeJoinRejected       = "join_rejected"
	MsgTypeUserJoined         = "user_joined"
	MsgTypeUserLeft           = "user_left"
	MsgTypeSyncPlayback       = "sync_playback"
	MsgTypeBufferWait         = "buffer_wait"
	MsgTypeBufferComplete     = "buffer_complete"
	MsgTypeError              = "error"
	MsgTypePong               = "pong"
	MsgTypeHostChanged        = "host_changed"
	MsgTypeKicked             = "kicked"
	MsgTypeSyncState          = "sync_state"
	MsgTypeReconnected        = "reconnected"
	MsgTypeUserReconnected    = "user_reconnected"
	MsgTypeUserDisconnected   = "user_disconnected"
	MsgTypeSuggestionReceived = "suggestion_received"
	MsgTypeSuggestionApproved = "suggestion_approved"
	MsgTypeSuggestionRejected = "suggestion_rejected"
	MsgTypeServerCapabilities = "server_capabilities"
)

// Playback actions
const (
	ActionPlay        = "play"
	ActionPause       = "pause"
	ActionSeek        = "seek"
	ActionSkipNext    = "skip_next"
	ActionSkipPrev    = "skip_prev"
	ActionChangeTrack = "change_track"
	ActionQueueAdd    = "queue_add"
	ActionQueueRemove = "queue_remove"
	ActionQueueClear  = "queue_clear"
	ActionSyncQueue   = "sync_queue"
	ActionSetVolume   = "set_volume"
)

// CreateRoomPayload is for creating a new room
type CreateRoomPayload struct {
	Username string `json:"username"`
}

// RoomCreatedPayload is the response for room creation
type RoomCreatedPayload struct {
	RoomCode     string `json:"room_code"`
	UserID       string `json:"user_id"`
	SessionToken string `json:"session_token"`
}

// JoinRoomPayload is for joining a room
type JoinRoomPayload struct {
	RoomCode string `json:"room_code"`
	Username string `json:"username"`
}

// JoinRequestPayload is sent to the host when someone wants to join
type JoinRequestPayload struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
}

// ApproveJoinPayload is for approving a join request
type ApproveJoinPayload struct {
	UserID string `json:"user_id"`
}

// RejectJoinPayload is for rejecting a join request
type RejectJoinPayload struct {
	UserID string `json:"user_id"`
	Reason string `json:"reason,omitempty"`
}

// JoinApprovedPayload is sent to the user when they are approved
type JoinApprovedPayload struct {
	RoomCode     string     `json:"room_code"`
	UserID       string     `json:"user_id"`
	SessionToken string     `json:"session_token"`
	State        *RoomState `json:"state"`
}

// JoinRejectedPayload is sent to the user when they are rejected
type JoinRejectedPayload struct {
	Reason string `json:"reason"`
}

// UserJoinedPayload is sent when a user joins the room
type UserJoinedPayload struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
}

// UserLeftPayload is sent when a user leaves the room
type UserLeftPayload struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
}

// PlaybackActionPayload is for playback control actions
type PlaybackActionPayload struct {
	Action     string      `json:"action"`
	TrackID    string      `json:"track_id,omitempty"`
	Position   int64       `json:"position,omitempty"` // milliseconds
	TrackInfo  *TrackInfo  `json:"track_info,omitempty"`
	InsertNext bool        `json:"insert_next,omitempty"`
	Queue      []TrackInfo `json:"queue,omitempty"`
	QueueTitle string      `json:"queue_title,omitempty"`
	Volume     float64     `json:"volume"`
	ServerTime int64       `json:"server_time,omitempty"`
}

// Suggestion payloads
type SuggestTrackPayload struct {
	TrackInfo *TrackInfo `json:"track_info"`
}

type SuggestionReceivedPayload struct {
	SuggestionID string     `json:"suggestion_id"`
	FromUserID   string     `json:"from_user_id"`
	FromUsername string     `json:"from_username"`
	TrackInfo    *TrackInfo `json:"track_info"`
}

type ApproveSuggestionPayload struct {
	SuggestionID string `json:"suggestion_id"`
}

type RejectSuggestionPayload struct {
	SuggestionID string `json:"suggestion_id"`
	Reason       string `json:"reason,omitempty"`
}

type SuggestionApprovedPayload struct {
	SuggestionID string     `json:"suggestion_id"`
	TrackInfo    *TrackInfo `json:"track_info"`
}

type SuggestionRejectedPayload struct {
	SuggestionID string `json:"suggestion_id"`
	Reason       string `json:"reason,omitempty"`
}

// TrackInfo contains information about a track
type TrackInfo struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Artist      string `json:"artist"`
	Album       string `json:"album,omitempty"`
	Duration    int64  `json:"duration"` // milliseconds
	Thumbnail   string `json:"thumbnail,omitempty"`
	SuggestedBy string `json:"suggested_by,omitempty"`
}

// BufferReadyPayload is sent when a user has finished buffering
type BufferReadyPayload struct {
	TrackID string `json:"track_id"`
}

// BufferWaitPayload is sent to tell users to wait for buffering
type BufferWaitPayload struct {
	TrackID    string   `json:"track_id"`
	WaitingFor []string `json:"waiting_for"` // user IDs still buffering
}

// BufferCompletePayload is sent when all users have buffered
type BufferCompletePayload struct {
	TrackID string `json:"track_id"`
}

// ErrorPayload is for error messages
type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// RoomState contains the current state of a room
type RoomState struct {
	RoomCode     string      `json:"room_code"`
	HostID       string      `json:"host_id"`
	Users        []UserInfo  `json:"users"`
	CurrentTrack *TrackInfo  `json:"current_track,omitempty"`
	IsPlaying    bool        `json:"is_playing"`
	Position     int64       `json:"position"`    // milliseconds
	LastUpdate   int64       `json:"last_update"` // unix timestamp ms
	Volume       float64     `json:"volume"`
	Queue        []TrackInfo `json:"queue,omitempty"`
}

// UserInfo contains information about a user
type UserInfo struct {
	UserID      string `json:"user_id"`
	Username    string `json:"username"`
	IsHost      bool   `json:"is_host"`
	IsConnected bool   `json:"is_connected"`
}

// KickUserPayload is for kicking a user from the room
type KickUserPayload struct {
	UserID string `json:"user_id"`
	Reason string `json:"reason,omitempty"`
}

// TransferHostPayload is for transferring host role to another user
type TransferHostPayload struct {
	NewHostID string `json:"new_host_id"`
}

// KickedPayload is sent to the user when they are kicked
type KickedPayload struct {
	Reason string `json:"reason"`
}

// HostChangedPayload is sent when the host changes
type HostChangedPayload struct {
	NewHostID   string `json:"new_host_id"`
	NewHostName string `json:"new_host_name"`
}

// SyncStatePayload is sent to a guest when they request current playback state
type SyncStatePayload struct {
	CurrentTrack *TrackInfo  `json:"current_track,omitempty"`
	IsPlaying    bool        `json:"is_playing"`
	Position     int64       `json:"position"`    // milliseconds
	LastUpdate   int64       `json:"last_update"` // unix timestamp ms
	Volume       float64     `json:"volume"`
	Queue        []TrackInfo `json:"queue,omitempty"`
}

// ReconnectPayload is for reconnecting to a room
type ReconnectPayload struct {
	SessionToken string `json:"session_token"`
}

type ClientCapabilitiesPayload struct {
	SupportsProtobuf    bool   `json:"supports_protobuf"`
	SupportsCompression bool   `json:"supports_compression"`
	ClientVersion       string `json:"client_version"`
}

type ServerCapabilitiesPayload struct {
	SupportsProtobuf    bool   `json:"supports_protobuf"`
	SupportsCompression bool   `json:"supports_compression"`
	ServerVersion       string `json:"server_version"`
}

// ReconnectedPayload is sent when successfully reconnected
type ReconnectedPayload struct {
	RoomCode string     `json:"room_code"`
	UserID   string     `json:"user_id"`
	State    *RoomState `json:"state"`
	IsHost   bool       `json:"is_host"`
}

// UserReconnectedPayload is sent to other users when someone reconnects
type UserReconnectedPayload struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
}

// UserDisconnectedPayload is sent when a user temporarily disconnects
type UserDisconnectedPayload struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
}
