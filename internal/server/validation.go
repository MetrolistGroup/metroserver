package server

import (
	"strings"
	"unicode/utf8"
)

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
		for i := maxLen; i > 0 && i > maxLen-4; i-- {
			if utf8.ValidString(s[:i]) {
				return s[:i]
			}
		}
		return s[:maxLen]
	}

	return s
}

func sanitizeTrackInfo(track *TrackInfo) bool {
	if track == nil {
		return false
	}

	track.ID = sanitizeString(track.ID, 200)
	track.Title = sanitizeString(track.Title, MaxTrackTitleLength)
	track.Artist = sanitizeString(track.Artist, MaxTrackArtistLength)
	track.Album = sanitizeString(track.Album, MaxTrackArtistLength)
	track.Thumbnail = sanitizeString(track.Thumbnail, MaxTrackURLLength)
	track.SuggestedBy = sanitizeString(track.SuggestedBy, MaxUsernameLength)

	if track.ID == "" || track.Title == "" {
		return false
	}
	if track.Duration <= 0 {
		track.Duration = 180000
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

	if state.CurrentTrack != nil && state.CurrentTrack.Duration > 0 && position > state.CurrentTrack.Duration {
		return state.CurrentTrack.Duration
	}

	return position
}
