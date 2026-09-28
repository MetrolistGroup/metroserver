package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// uaTier is what we do with a client, decided by its User-Agent.
type uaTier int

const (
	uaAllow    uaTier = iota // normal service
	uaAdvert                 // queue title becomes a Metrolist ad
	uaRickroll               // uaAdvert, plus every track becomes Rick Astley
	uaBlock                  // handshake, one error frame, goodbye

	uaTierCount = 4
)

var uaTierNames = [uaTierCount]string{"allow", "advert", "rickroll", "block"}

func (t uaTier) String() string {
	if int(t) < 0 || int(t) >= uaTierCount {
		return "unknown"
	}
	return uaTierNames[t]
}

const (
	// DefaultAdvertTitle replaces the queue title for non-allowlisted clients.
	DefaultAdvertTitle = "⚠️ Unofficial client — get Metrolist: github.com/MetrolistGroup/Metrolist"
	// BlockedClientMessage is what a blocked client is told before we hang up.
	BlockedClientMessage = "This client is not permitted on this server. Use Metrolist: github.com/MetrolistGroup/Metrolist"
)

// Never Gonna Give You Up, the only correct answer to a freeloader.
const (
	rickTrackID        = "dQw4w9WgXcQ"
	rickTrackTitle     = "Never Gonna Give You Up"
	rickTrackArtist    = "Rick Astley"
	rickTrackAlbum     = "Whenever You Need Somebody"
	rickTrackThumbnail = "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg"
	rickTrackDuration  = 213000
)

// Transport User-Agents are substring matches. Package-name User-Agents are
// exact matches so repacks such as com.metrolist.music8 are not accidentally
// allowed to host.
var (
	defaultAllowUA      = []string{"okhttp", "ktor-client"}
	defaultAllowUAExact = []string{"com.metrolist.music", "com.metrolist.music.debug", "com.nevar.nzik"}
)

func rickTrack() TrackInfo {
	return TrackInfo{
		ID:        rickTrackID,
		Title:     rickTrackTitle,
		Artist:    rickTrackArtist,
		Album:     rickTrackAlbum,
		Duration:  rickTrackDuration,
		Thumbnail: rickTrackThumbnail,
	}
}

type uaPolicy struct {
	allow       []string // lowercased substrings
	allowExact  []string
	block       []string
	rickroll    []string
	advert      []string
	advertTitle string
}

func defaultUAPolicy() *uaPolicy {
	return &uaPolicy{allow: defaultAllowUA, allowExact: defaultAllowUAExact, advertTitle: DefaultAdvertTitle}
}

// uaPolicyFile is the on-disk shape. allow extends the built-in defaults, but
// higher-precedence block/rickroll/advert patterns may intentionally override them.
type uaPolicyFile struct {
	Allow       []string `json:"allow"`
	Block       []string `json:"block"`
	Rickroll    []string `json:"rickroll"`
	Advert      []string `json:"advert"`
	AdvertTitle string   `json:"advert_title"`
}

// loadUAPolicy reads the policy from path. An empty path keeps the defaults.
func loadUAPolicy(path string) (*uaPolicy, error) {
	policy := defaultUAPolicy()
	if path == "" {
		return policy, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var file uaPolicyFile
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("UA policy must contain exactly one JSON object")
	}

	policy.allow = lowerPatterns(append(append([]string{}, defaultAllowUA...), file.Allow...))
	policy.block = lowerPatterns(file.Block)
	policy.rickroll = lowerPatterns(file.Rickroll)
	policy.advert = lowerPatterns(file.Advert)
	if title := sanitizeString(strings.TrimSpace(file.AdvertTitle), MaxTrackTitleLength); title != "" {
		policy.advertTitle = title
	}
	return policy, nil
}

func lowerPatterns(patterns []string) []string {
	if len(patterns) == 0 {
		return nil
	}
	lowered := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		if pattern = strings.ToLower(strings.TrimSpace(pattern)); pattern != "" {
			lowered = append(lowered, pattern)
		}
	}
	return lowered
}

func uaMatches(userAgent string, patterns []string) bool {
	for _, pattern := range patterns {
		if strings.Contains(userAgent, pattern) {
			return true
		}
	}
	return false
}

func uaMatchesExact(userAgent string, patterns []string) bool {
	for _, pattern := range patterns {
		if userAgent == pattern {
			return true
		}
	}
	return false
}

// resolve maps a User-Agent to its tier. block beats rickroll beats advert;
// everything that is not explicitly allowlisted gets the ad treatment.
func (p *uaPolicy) resolve(userAgent string) uaTier {
	if p == nil {
		return uaAllow
	}
	ua := strings.ToLower(userAgent)
	switch {
	case uaMatches(ua, p.block):
		return uaBlock
	case uaMatches(ua, p.rickroll):
		return uaRickroll
	case uaMatches(ua, p.advert):
		return uaAdvert
	case uaMatchesExact(ua, p.allowExact), uaMatches(ua, p.allow):
		return uaAllow
	}
	return uaAdvert
}

// rewriteForUATier returns the payload as it should reach a client in this tier.
// It always builds fresh values: the incoming payload may be shared with other
// recipients and with live room state.
func rewriteForUATier(tier uaTier, advertTitle string, payload interface{}) interface{} {
	switch tier {
	case uaAdvert, uaRickroll:
	default:
		return payload
	}
	rick := rickTrack()

	switch v := payload.(type) {
	case PlaybackActionPayload:
		v.QueueTitle = advertTitle
		if tier == uaAdvert {
			return v
		}
		v.TrackID = rick.ID
		if v.TrackInfo != nil {
			track := rick
			v.TrackInfo = &track
		}
		v.Queue = rickQueue(v.Queue)
		v.Position = rickPosition(v.Position)
		return v

	case SyncStatePayload:
		if tier == uaAdvert {
			return payload
		}
		v.CurrentTrack = &rick
		v.Queue = rickQueue(v.Queue)
		v.Position = rickPosition(v.Position)
		return v

	case *RoomState:
		if tier == uaAdvert || v == nil {
			return payload
		}
		return rickState(v)

	case JoinApprovedPayload:
		if tier == uaAdvert {
			return payload
		}
		v.State = rickState(v.State)
		return v
	case ReconnectedPayload:
		if tier == uaAdvert {
			return payload
		}
		v.State = rickState(v.State)
		return v

	case BufferWaitPayload:
		if tier == uaAdvert {
			return payload
		}
		v.TrackID = rick.ID
		return v
	case BufferCompletePayload:
		if tier == uaAdvert {
			return payload
		}
		v.TrackID = rick.ID
		return v

	case SuggestionReceivedPayload:
		if tier == uaAdvert || v.TrackInfo == nil {
			return payload
		}
		track := rick
		v.TrackInfo = &track
		return v
	case SuggestionApprovedPayload:
		if tier == uaAdvert || v.TrackInfo == nil {
			return payload
		}
		track := rick
		v.TrackInfo = &track
		return v
	}
	return payload
}

// ponytail: a rickrolled queue collapses to one entry on clients that dedupe
// media IDs. Fine — they are not supposed to be here anyway.
func rickQueue(queue []TrackInfo) []TrackInfo {
	if len(queue) == 0 {
		return nil
	}
	rick := rickTrack()
	rewritten := make([]TrackInfo, len(queue))
	for i := range rewritten {
		rewritten[i] = rick
	}
	return rewritten
}

func rickPosition(position int64) int64 {
	if position <= 0 {
		return 0
	}
	return position % rickTrackDuration
}

// rickState returns a copy of state in which every track is Rick Astley. A nil
// state stays nil.
func rickState(state *RoomState) *RoomState {
	if state == nil {
		return nil
	}
	rick := rickTrack()
	rewritten := cloneRoomState(state)
	rewritten.CurrentTrack = &rick
	rewritten.Queue = rickQueue(rewritten.Queue)
	rewritten.Position = rickPosition(rewritten.Position)
	return rewritten
}

// rickMappedTrackID translates a placeholder reported by a rickrolled client
// back to the track the room is actually on, so buffer_ready and play/pause/seek
// from that client stay valid.
func rickMappedTrackID(tier uaTier, trackID string, state *RoomState) string {
	if tier != uaRickroll || trackID != rickTrackID || state == nil || state.CurrentTrack == nil {
		return trackID
	}
	return state.CurrentTrack.ID
}

func canHostClient(client *Client) bool {
	return client != nil && client.uaTier == uaAllow
}

func firstEligibleHost(clients map[string]*Client) *Client {
	for _, client := range clients {
		if canHostClient(client) {
			return client
		}
	}
	return nil
}
