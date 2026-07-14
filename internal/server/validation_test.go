package server

import (
	"reflect"
	"testing"
	"unicode/utf8"
)

func TestSanitizeString(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		maxLen int
		want   string
	}{
		{
			name:   "trims and removes control characters",
			input:  "  hello\x00\x01 world  ",
			maxLen: 50,
			want:   "hello world",
		},
		{
			name:   "preserves allowed whitespace",
			input:  "hello\tworld\nnext",
			maxLen: 50,
			want:   "hello\tworld\nnext",
		},
		{
			name:   "normalizes invalid UTF-8",
			input:  string([]byte{'a', 0xff, 'b'}),
			maxLen: 50,
			want:   "a�b",
		},
		{
			name:   "does not split a multibyte character",
			input:  "ééé",
			maxLen: 5,
			want:   "éé",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeString(tt.input, tt.maxLen)
			if got != tt.want {
				t.Fatalf("sanitizeString(%q, %d) = %q, want %q", tt.input, tt.maxLen, got, tt.want)
			}
			if !utf8.ValidString(got) {
				t.Fatalf("sanitizeString(%q, %d) returned invalid UTF-8", tt.input, tt.maxLen)
			}
		})
	}
}

func TestSanitizeTrackInfo(t *testing.T) {
	t.Run("requires ID and title", func(t *testing.T) {
		if sanitizeTrackInfo(nil) {
			t.Fatal("nil track was accepted")
		}
		if sanitizeTrackInfo(&TrackInfo{ID: "track"}) {
			t.Fatal("track without a title was accepted")
		}
	})

	t.Run("sanitizes fields and defaults duration", func(t *testing.T) {
		track := &TrackInfo{
			ID:          "  track-id  ",
			Title:       "  title\x00  ",
			Artist:      "  artist  ",
			Duration:    0,
			SuggestedBy: "  guest  ",
		}
		if !sanitizeTrackInfo(track) {
			t.Fatal("valid track was rejected")
		}
		if track.ID != "track-id" || track.Title != "title" || track.Artist != "artist" || track.SuggestedBy != "guest" {
			t.Fatalf("track fields were not sanitized: %#v", track)
		}
		if track.Duration != 180000 {
			t.Fatalf("duration = %d, want 180000", track.Duration)
		}
	})

	t.Run("clamps duration", func(t *testing.T) {
		track := &TrackInfo{ID: "track", Title: "title", Duration: MaxTrackDuration + 1}
		if !sanitizeTrackInfo(track) {
			t.Fatal("valid track was rejected")
		}
		if track.Duration != MaxTrackDuration {
			t.Fatalf("duration = %d, want %d", track.Duration, MaxTrackDuration)
		}
	})
}

func TestCloneRoomStateIsIndependent(t *testing.T) {
	original := &RoomState{
		RoomCode:     "ROOM1234",
		HostID:       "host",
		Users:        []UserInfo{{UserID: "host", Username: "Host"}},
		CurrentTrack: &TrackInfo{ID: "current", Title: "Current"},
		Queue:        []TrackInfo{{ID: "queued", Title: "Queued"}},
	}

	clone := cloneRoomState(original)
	if !reflect.DeepEqual(clone, original) {
		t.Fatalf("clone = %#v, want %#v", clone, original)
	}

	clone.Users[0].Username = "Changed"
	clone.CurrentTrack.Title = "Changed"
	clone.Queue[0].Title = "Changed"
	if original.Users[0].Username != "Host" || original.CurrentTrack.Title != "Current" || original.Queue[0].Title != "Queued" {
		t.Fatalf("mutating clone changed original: %#v", original)
	}
}

func TestLivePlaybackPosition(t *testing.T) {
	tests := []struct {
		name  string
		state *RoomState
		nowMs int64
		want  int64
	}{
		{name: "nil state", state: nil, nowMs: 1000, want: 0},
		{name: "negative position", state: &RoomState{Position: -1}, nowMs: 1000, want: 0},
		{name: "paused", state: &RoomState{Position: 250, LastUpdate: 1000}, nowMs: 1500, want: 250},
		{name: "playing", state: &RoomState{Position: 250, LastUpdate: 1000, IsPlaying: true}, nowMs: 1500, want: 750},
		{name: "clock before update", state: &RoomState{Position: 250, LastUpdate: 2000, IsPlaying: true}, nowMs: 1500, want: 250},
		{
			name:  "clamped to track duration",
			state: &RoomState{Position: 900, LastUpdate: 1000, IsPlaying: true, CurrentTrack: &TrackInfo{Duration: 1000}},
			nowMs: 1200,
			want:  1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := livePlaybackPosition(tt.state, tt.nowMs); got != tt.want {
				t.Fatalf("livePlaybackPosition() = %d, want %d", got, tt.want)
			}
		})
	}
}
