package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"strings"
)

// uaTier is what we do with a client, decided by its User-Agent.
type uaTier int

const (
	uaAllow    uaTier = iota // normal service: the default for any client
	uaAdvert                 // queue title becomes a Metrolist ad
	uaRickroll               // uaAdvert, plus Rick Astley slipped into the queue
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
	block       []string // lowercased substrings
	rickroll    []string
	advert      []string
	advertTitle string
}

func defaultUAPolicy() *uaPolicy {
	return &uaPolicy{advertTitle: DefaultAdvertTitle}
}

// uaPolicyFile is the on-disk shape. Unmatched clients get normal service, so
// allow is accepted for compatibility with older policy files and ignored.
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
	var parsed *uaPolicyFile
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parsed); err != nil {
		return nil, err
	}
	if parsed == nil {
		return nil, fmt.Errorf("UA policy must be a JSON object, not null")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("UA policy must contain exactly one JSON object")
	}
	file := *parsed

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

// resolve maps a User-Agent to its tier. block beats rickroll beats advert;
// every client that no rule names gets normal service.
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
	}
	return uaAllow
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
	rickroll := tier == uaRickroll

	switch v := payload.(type) {
	case PlaybackActionPayload:
		v.QueueTitle = advertTitle
		if rickroll && v.Queue != nil {
			v.Queue = withRick(v.Queue)
		}
		return v
	case SyncStatePayload:
		v.QueueTitle = advertTitle
		if rickroll {
			v.Queue = withRick(v.Queue)
		}
		return v
	case *RoomState:
		return policyState(v, advertTitle, rickroll)
	case JoinApprovedPayload:
		v.State = policyState(v.State, advertTitle, rickroll)
		return v
	case ReconnectedPayload:
		v.State = policyState(v.State, advertTitle, rickroll)
		return v
	}
	return payload
}

// withRick returns a copy of queue with Rick Astley inserted at a position
// that looks random but stays put while the queue is unchanged, so repeated
// snapshots do not shuffle him around.
func withRick(queue []TrackInfo) []TrackInfo {
	hash := fnv.New32a()
	for i := range queue {
		hash.Write([]byte(queue[i].ID))
		hash.Write([]byte{0})
	}
	at := int(hash.Sum32() % uint32(len(queue)+1))
	rewritten := make([]TrackInfo, 0, len(queue)+1)
	rewritten = append(rewritten, queue[:at]...)
	rewritten = append(rewritten, rickTrack())
	return append(rewritten, queue[at:]...)
}

// policyState returns a rewritten copy of state. A nil state stays nil.
func policyState(state *RoomState, advertTitle string, rickroll bool) *RoomState {
	if state == nil {
		return nil
	}
	rewritten := cloneRoomState(state)
	rewritten.QueueTitle = advertTitle
	if rickroll {
		rewritten.Queue = withRick(rewritten.Queue)
	}
	return rewritten
}

func canHostClient(client *Client) bool {
	return client != nil && client.uaTier == uaAllow
}
