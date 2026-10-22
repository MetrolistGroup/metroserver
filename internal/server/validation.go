package server

import (
	"strings"
	"time"
	"unicode/utf8"
)

// clockBase anchors serverNowMs. Deriving later readings from its monotonic
// component keeps playback extrapolation immune to wall-clock steps.
var clockBase = time.Now()

// serverNowMs is the server's protocol clock in Unix milliseconds. It is used
// for playback anchors and pong timestamps so clients estimate one clock.
func serverNowMs() int64 {
	return clockBase.UnixMilli() + time.Since(clockBase).Milliseconds()
}

// MaxTrackIDLength bounds opaque track IDs, which are never rewritten.
const MaxTrackIDLength = 200

func sanitizeString(s string, maxLen int) string {
	// Remove null bytes and other control characters
	s = strings.Map(func(r rune) rune {
		if r == 0 || (r < 32 && r != '\t' && r != '\n' && r != '\r') {
			return -1
		}
		return r
	}, s)

	// Trim whitespace
	s = strings.TrimSpace(s)

	// Validate UTF-8
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "")
	}

	// Limit length
	if len(s) > maxLen {
		// Ensure we don't cut in the middle of a multibyte character
		cut := maxLen
		for i := maxLen; i > 0 && i > maxLen-4; i-- {
			if utf8.ValidString(s[:i]) {
				cut = i
				break
			}
		}
		s = s[:cut]
	}

	// Copy so a short stored value cannot pin a huge decoded message in memory.
	return strings.Clone(s)
}

// validTrackID reports whether id can be used as an opaque track identifier.
// IDs are compared byte-for-byte everywhere, so they are validated, not cleaned.
func validTrackID(id string) bool {
	if id == "" || len(id) > MaxTrackIDLength || !utf8.ValidString(id) {
		return false
	}
	for _, r := range id {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}

func sanitizeTrackInfo(track *TrackInfo) bool {
	if track == nil {
		return false
	}

	if !validTrackID(track.ID) {
		return false
	}
	track.ID = strings.Clone(track.ID)
	track.Title = sanitizeString(track.Title, MaxTrackTitleLength)
	track.Artist = sanitizeString(track.Artist, MaxTrackArtistLength)
	track.Album = sanitizeString(track.Album, MaxTrackArtistLength)
	track.Thumbnail = sanitizeString(track.Thumbnail, MaxTrackURLLength)
	track.SuggestedBy = sanitizeString(track.SuggestedBy, MaxUsernameLength)

	if track.Title == "" {
		// A missing title must not freeze the room on the previous track.
		track.Title = sanitizeString(track.ID, MaxTrackTitleLength)
	}
	if track.Duration < 0 {
		track.Duration = 0 // unknown
	} else if track.Duration > MaxTrackDuration {
		track.Duration = MaxTrackDuration
	}

	return true
}

func cloneTrackInfo(track *TrackInfo) *TrackInfo {
	if track == nil {
		return nil
	}
	copyTrack := *track
	return &copyTrack
}

func cloneRoomState(state *RoomState) *RoomState {
	if state == nil {
		return nil
	}
	copyState := *state
	copyState.CurrentTrack = cloneTrackInfo(state.CurrentTrack)
	if state.Users != nil {
		copyState.Users = append([]UserInfo(nil), state.Users...)
	}
	if state.Queue != nil {
		copyState.Queue = append([]TrackInfo(nil), state.Queue...)
	}
	return &copyState
}

func livePlaybackPosition(state *RoomState, nowMs int64) int64 {
	if state == nil {
		return 0
	}

	position := state.Position
	if position < 0 {
		position = 0
	}

	if state.IsPlaying && state.LastUpdate > 0 {
		elapsed := nowMs - state.LastUpdate
		if elapsed > 0 {
			position += elapsed
		}
	}

	// Reported durations are metadata (clients send 0 or a 3-minute placeholder
	// when unknown), so they never cap the host's authoritative position.
	return min(position, MaxTrackDuration)
}

func cloneLiveRoomState(state *RoomState, nowMs int64) *RoomState {
	copyState := cloneRoomState(state)
	if copyState == nil {
		return nil
	}
	copyState.Position = livePlaybackPosition(state, nowMs)
	copyState.LastUpdate = nowMs
	return copyState
}
