package server

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

func (s *Server) handlePlaybackAction(c *Client, payload []byte) {
	var p PlaybackActionPayload
	if err := decodePayload(payload, MsgTypePlaybackAction, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid playback action payload")
		return
	}

	if p.Action == "" {
		c.sendError(s.logger, "missing_action", "Action is required")
		return
	}

	room := c.currentRoom()
	if room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room.mu.Lock()
	defer room.mu.Unlock()

	if room.Host == nil || room.Host != c || room.HostDisconnectedAt != nil {
		c.sendError(s.logger, "not_host", "Only the host can control playback")
		return
	}

	switch p.Action {
	case ActionPlay:
		// Block play if no track is set
		if room.State.CurrentTrack == nil {
			s.logger.Debug("Play blocked - no current track", zap.String("room_code", room.Code))
			c.sendError(s.logger, "no_track", "Cannot play without a track")
			return
		}
		if p.Position < 0 {
			c.sendError(s.logger, "invalid_position", "Position cannot be negative")
			return
		}
		nowMs := time.Now().UnixMilli()
		if room.State.CurrentTrack != nil && room.State.CurrentTrack.Duration > 0 && p.Position > room.State.CurrentTrack.Duration {
			p.Position = room.State.CurrentTrack.Duration
		}
		room.State.IsPlaying = true
		room.State.Position = p.Position
		room.State.LastUpdate = nowMs
		p.ServerTime = nowMs
		if p.TrackID == "" && room.State.CurrentTrack != nil {
			p.TrackID = room.State.CurrentTrack.ID
		}

	case ActionPause:
		// Pause is always allowed
		if p.Position < 0 {
			c.sendError(s.logger, "invalid_position", "Position cannot be negative")
			return
		}
		nowMs := time.Now().UnixMilli()
		if room.State.CurrentTrack != nil && room.State.CurrentTrack.Duration > 0 && p.Position > room.State.CurrentTrack.Duration {
			p.Position = room.State.CurrentTrack.Duration
		}
		room.State.IsPlaying = false
		room.State.Position = p.Position
		room.State.LastUpdate = nowMs
		if p.TrackID == "" && room.State.CurrentTrack != nil {
			p.TrackID = room.State.CurrentTrack.ID
		}

	case ActionSeek:
		if p.Position < 0 {
			c.sendError(s.logger, "invalid_position", "Position cannot be negative")
			return
		}
		nowMs := time.Now().UnixMilli()
		if room.State.CurrentTrack != nil && room.State.CurrentTrack.Duration > 0 && p.Position > room.State.CurrentTrack.Duration {
			p.Position = room.State.CurrentTrack.Duration
		}
		room.State.Position = p.Position
		room.State.LastUpdate = nowMs
		if p.TrackID == "" && room.State.CurrentTrack != nil {
			p.TrackID = room.State.CurrentTrack.ID
		}

	case ActionChangeTrack:
		if p.TrackInfo == nil {
			c.sendError(s.logger, "missing_track_info", "Track info is required for track change")
			return
		}

		if !sanitizeTrackInfo(p.TrackInfo) {
			c.sendError(s.logger, "invalid_track_info", "Track must have ID and title")
			return
		}

		room.State.CurrentTrack = p.TrackInfo
		room.State.Position = 0
		room.State.IsPlaying = false
		room.State.LastUpdate = time.Now().UnixMilli()

		// For new tracks, always start at position 0
		room.HostStartPosition = 0
		s.logger.Debug("Track changed", zap.String("room_code", room.Code), zap.String("track_id", p.TrackInfo.ID))

		// We do not require guests to wait for everyone to buffer.
		// Immediately notify clients and sync them to position 0 so guests can proceed.
		room.BufferingUsers = nil // disable per-room buffering tracking

		// Broadcast track change and immediate sync
		for _, client := range room.Clients {
			if client != nil {
				// Send track change
				client.sendMessage(s.logger, MsgTypeSyncPlayback, p)

				// Ensure everyone is paused at position 0 during transition
				client.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
					Action:   ActionPause,
					TrackID:  p.TrackInfo.ID,
					Position: 0,
				})

				// Immediately notify buffer complete so clients that wait for it will apply seek/play
				client.sendMessage(s.logger, MsgTypeBufferComplete, BufferCompletePayload{
					TrackID: p.TrackInfo.ID,
				})

				// Seek everyone to the new start position (0)
				client.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
					Action:   ActionSeek,
					TrackID:  p.TrackInfo.ID,
					Position: 0,
				})

				// If the room was marked playing, start playback immediately
				if room.State.IsPlaying {
					client.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
						Action:   ActionPlay,
						TrackID:  p.TrackInfo.ID,
						Position: 0,
					})
				}
			}
		}
		return

	case ActionSkipNext, ActionSkipPrev:
		room.State.Position = 0
		room.State.LastUpdate = time.Now().UnixMilli()

	case ActionQueueAdd:
		if p.TrackInfo == nil {
			c.sendError(s.logger, "missing_track_info", "Track info is required for queue add")
			return
		}

		if !sanitizeTrackInfo(p.TrackInfo) {
			c.sendError(s.logger, "invalid_track_info", "Track must have ID and title")
			return
		}

		// Limit queue size to prevent memory issues
		if len(room.State.Queue) >= MaxQueueSize {
			c.sendError(s.logger, "queue_full", "Queue is full")
			return
		}

		if p.InsertNext {
			// Insert right after current track: at the front of upcoming queue
			room.State.Queue = append([]TrackInfo{*p.TrackInfo}, room.State.Queue...)
		} else {
			// Append to end of upcoming queue
			room.State.Queue = append(room.State.Queue, *p.TrackInfo)
		}

	case ActionQueueRemove:
		if p.TrackID == "" {
			c.sendError(s.logger, "missing_track_id", "Track ID is required for queue remove")
			return
		}

		// Remove track from queue by ID
		newQueue := make([]TrackInfo, 0, len(room.State.Queue))
		for _, t := range room.State.Queue {
			if t.ID != p.TrackID {
				newQueue = append(newQueue, t)
			}
		}
		room.State.Queue = newQueue

	case ActionQueueClear:
		room.State.Queue = []TrackInfo{}

	case ActionSyncQueue:
		if p.Queue == nil {
			// Allow empty queue sync (clearing) but log it
			room.State.Queue = []TrackInfo{}
		} else {
			// Limit queue size
			if len(p.Queue) > MaxQueueSize {
				p.Queue = p.Queue[:MaxQueueSize]
			}

			// Validate and sanitize each track in the queue
			sanitizedQueue := make([]TrackInfo, 0, len(p.Queue))
			for _, track := range p.Queue {
				if !sanitizeTrackInfo(&track) {
					continue
				}

				sanitizedQueue = append(sanitizedQueue, track)
			}
			room.State.Queue = sanitizedQueue
			// Pass sanitized queue back to payload for broadcast
			p.Queue = sanitizedQueue
		}

	case ActionSetVolume:
		if p.Volume < 0 || p.Volume > 1 {
			c.sendError(s.logger, "invalid_volume", "Volume must be between 0 and 1")
			return
		}
		room.State.Volume = p.Volume

	default:
		c.sendError(s.logger, "unknown_action", fmt.Sprintf("Unknown action: %s", p.Action))
		return
	}

	// Broadcast to all clients
	for _, client := range room.Clients {
		if client != nil {
			client.sendMessage(s.logger, MsgTypeSyncPlayback, p)
		}
	}

	s.logger.Debug("Playback action processed",
		zap.String("action", p.Action),
		zap.String("room_code", room.Code),
		zap.String("host_name", c.userName()))
}

func (s *Server) handleBufferReady(c *Client, payload []byte) {
	var p BufferReadyPayload
	if err := decodePayload(payload, MsgTypeBufferReady, &p); err != nil {
		c.sendError(s.logger, "invalid_payload", "Invalid buffer ready payload")
		return
	}

	if p.TrackID == "" {
		c.sendError(s.logger, "missing_track_id", "Track ID is required")
		return
	}

	room := c.currentRoom()
	if room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room.mu.Lock()
	defer room.mu.Unlock()
	clientID := c.clientID()
	username := c.userName()

	s.logger.Debug("Buffer ready received",
		zap.String("username", username),
		zap.String("user_id", clientID),
		zap.String("track_id", p.TrackID))

	// Mark user as ready
	delete(room.BufferingUsers, clientID)

	// If buffering is disabled for this room, respond per-client so late buffer_ready still receives SEEK/PLAY
	if room.BufferingUsers == nil {
		s.logger.Debug("Buffering disabled for room - per-client ACK", zap.String("room_code", room.Code), zap.String("user_id", clientID))
		syncPosition := livePlaybackPosition(room.State, time.Now().UnixMilli())
		syncTrackID := p.TrackID
		if room.State.CurrentTrack != nil && room.State.CurrentTrack.ID != "" {
			syncTrackID = room.State.CurrentTrack.ID
		}
		// Send buffer-complete and sync to this specific client so they will apply seek/play
		c.sendMessage(s.logger, MsgTypeBufferComplete, BufferCompletePayload{TrackID: syncTrackID})
		c.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
			Action:   ActionSeek,
			TrackID:  syncTrackID,
			Position: syncPosition,
		})
		if room.State.IsPlaying {
			c.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
				Action:   ActionPlay,
				TrackID:  syncTrackID,
				Position: syncPosition,
			})
		}
		return
	}

	// Check if all users are ready
	if len(room.BufferingUsers) == 0 {
		// All users ready - sync everyone to position 0 for new track
		syncPosition := int64(0)
		syncTrackID := p.TrackID
		if room.State.CurrentTrack != nil && room.State.CurrentTrack.ID != "" {
			syncTrackID = room.State.CurrentTrack.ID
		}
		room.State.Position = syncPosition
		room.State.LastUpdate = time.Now().UnixMilli()

		s.logger.Debug("All users buffered",
			zap.String("track_id", p.TrackID),
			zap.String("room_code", room.Code))

		for _, client := range room.Clients {
			if client != nil {
				// Step 1: Send buffer complete notification
				client.sendMessage(s.logger, MsgTypeBufferComplete, BufferCompletePayload{
					TrackID: syncTrackID,
				})

				// Step 2: SEEK everyone to exact position
				client.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
					Action:   ActionSeek,
					TrackID:  syncTrackID,
					Position: syncPosition,
				})

				// Step 3: Only PLAY if the host actually started playback
				if room.State.IsPlaying {
					client.sendMessage(s.logger, MsgTypeSyncPlayback, PlaybackActionPayload{
						Action:   ActionPlay,
						TrackID:  syncTrackID,
						Position: syncPosition,
					})
				}
			}
		}
	} else {
		// Notify all users of who is still buffering
		waitingFor := make([]string, 0, len(room.BufferingUsers))
		for id := range room.BufferingUsers {
			waitingFor = append(waitingFor, id)
		}

		for _, client := range room.Clients {
			if client != nil {
				client.sendMessage(s.logger, MsgTypeBufferWait, BufferWaitPayload{
					TrackID:    p.TrackID,
					WaitingFor: waitingFor,
				})
			}
		}
	}
}

func (s *Server) handleRequestSync(c *Client) {
	room := c.currentRoom()
	if room == nil {
		c.sendError(s.logger, "not_in_room", "You are not in a room")
		return
	}

	room.mu.RLock()

	// Calculate live position
	nowMs := time.Now().UnixMilli()
	currentPosition := livePlaybackPosition(room.State, nowMs)
	elapsed := int64(0)
	if room.State.IsPlaying && currentPosition > room.State.Position {
		elapsed = currentPosition - room.State.Position
	}

	responsePlaying := room.State.IsPlaying

	s.logger.Debug("Sync request received",
		zap.String("username", c.userName()),
		zap.String("user_id", c.clientID()),
		zap.Bool("has_track", room.State.CurrentTrack != nil),
		zap.Bool("server_playing", room.State.IsPlaying),
		zap.Bool("response_playing", responsePlaying),
		zap.Int64("position", currentPosition),
		zap.Int64("elapsed_ms", elapsed))

	response := SyncStatePayload{
		CurrentTrack: cloneTrackInfo(room.State.CurrentTrack),
		IsPlaying:    responsePlaying,
		Position:     currentPosition,
		LastUpdate:   nowMs,
		Queue:        append([]TrackInfo(nil), room.State.Queue...),
		Volume:       room.State.Volume,
	}
	room.mu.RUnlock()

	c.sendMessage(s.logger, MsgTypeSyncState, response)
}
