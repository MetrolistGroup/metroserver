package server

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

const (
	StateFile        = "server_state.json" // legacy migration from pre-bbolt deployments
	MaxStateFileSize = 50 * 1024 * 1024
	// flushHeartbeat refreshes the flush timestamp even when nothing changed,
	// so restarts measure downtime from close to the actual stop.
	flushHeartbeat = 30 * time.Second
)

var (
	serverStateBucket = []byte("server_state") // legacy single-snapshot store
	serverStateKey    = []byte("latest")
	roomsBucket       = []byte("rooms") // one roomRecord per room code
	metaBucket        = []byte("meta")  // flushedAtKey
	flushedAtKey      = []byte("flushed_at")
)

// PersistentState is the legacy whole-server snapshot, read only for migration.
type PersistentState struct {
	ServerShutdownTime time.Time           `json:"server_shutdown_time"`
	Rooms              []PersistentRoom    `json:"rooms"`
	Sessions           []PersistentSession `json:"sessions"`
}

// PersistentRoom is a serializable version of Room
type PersistentRoom struct {
	Code               string                 `json:"code"`
	HostID             string                 `json:"host_id"`
	OwnerID            string                 `json:"owner_id,omitempty"`
	TrackRevision      uint64                 `json:"track_revision,omitempty"`
	State              *RoomState             `json:"state"`
	DisconnectedUsers  map[string]*Session    `json:"disconnected_users"`
	PendingSuggestions []PersistentSuggestion `json:"pending_suggestions"`
	HostDisconnectedAt *time.Time             `json:"host_disconnected_at,omitempty"`
}

// PersistentSuggestion is a serializable version of Suggestion
type PersistentSuggestion struct {
	ID           string     `json:"id"`
	FromUserID   string     `json:"from_user_id"`
	FromUsername string     `json:"from_username"`
	Track        *TrackInfo `json:"track"`
}

// PersistentSession is a serializable version of Session with token. A zero
// DisconnectAt marks a member that was connected when the record was written.
type PersistentSession struct {
	Token        string    `json:"token"`
	UserID       string    `json:"user_id"`
	Username     string    `json:"username"`
	RoomCode     string    `json:"room_code"`
	IsHost       bool      `json:"is_host"`
	DisconnectAt time.Time `json:"disconnect_at"`
}

// roomRecord is one room plus every reconnect token that belongs to it.
type roomRecord struct {
	Room     PersistentRoom      `json:"room"`
	Sessions []PersistentSession `json:"sessions"`
}

// snapshotRooms encodes every room. Records are stable while nothing changes
// (connected members carry no timestamps), so the flusher can skip them.
func (s *Server) snapshotRooms() (map[string][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sessionsByRoom := make(map[string][]PersistentSession)
	for token, session := range s.sessions {
		if token == "" || session == nil {
			continue
		}
		sessionsByRoom[session.RoomCode] = append(sessionsByRoom[session.RoomCode], PersistentSession{
			Token:        token,
			UserID:       session.UserID,
			Username:     session.Username,
			RoomCode:     session.RoomCode,
			IsHost:       session.IsHost,
			DisconnectAt: session.DisconnectAt,
		})
	}

	records := make(map[string][]byte, len(s.rooms))
	for code, room := range s.rooms {
		room.mu.RLock()
		record, ok := persistRoomLocked(s, room, sessionsByRoom[code])
		room.mu.RUnlock()
		if !ok {
			continue
		}
		data, err := json.Marshal(record)
		if err != nil {
			return nil, fmt.Errorf("marshal room %s: %w", code, err)
		}
		records[code] = data
	}
	return records, nil
}

func persistRoomLocked(s *Server, room *Room, sessions []PersistentSession) (roomRecord, bool) {
	stateCopy := cloneRoomState(room.State)
	if stateCopy == nil {
		return roomRecord{}, false
	}
	for i := range stateCopy.Users {
		stateCopy.Users[i].IsConnected = false
	}

	disconnectedUsers := make(map[string]*Session, len(room.DisconnectedUsers)+len(room.Clients))
	for userID, session := range room.DisconnectedUsers {
		if session == nil {
			continue
		}
		copySession := *session
		disconnectedUsers[userID] = &copySession
	}
	for userID, client := range room.Clients {
		if client == nil {
			continue
		}
		token := client.session()
		if token == "" {
			token = s.generateSessionToken()
			client.setSessionToken(token)
		}
		isHost := room.State.HostID == userID
		disconnectedUsers[userID] = &Session{UserID: userID, Username: client.userName(), RoomCode: room.Code, IsHost: isHost}
		sessions = append(sessions, PersistentSession{
			Token:    token,
			UserID:   userID,
			Username: client.userName(),
			RoomCode: room.Code,
			IsHost:   isHost,
		})
	}
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].Token < sessions[j].Token })

	pendingSuggestions := make([]PersistentSuggestion, 0, len(room.PendingSuggestions))
	for _, suggestion := range room.PendingSuggestions {
		if suggestion == nil {
			continue
		}
		pendingSuggestions = append(pendingSuggestions, PersistentSuggestion{
			ID:           suggestion.ID,
			FromUserID:   suggestion.FromUserID,
			FromUsername: suggestion.FromUsername,
			Track:        cloneTrackInfo(suggestion.Track),
		})
	}
	sort.Slice(pendingSuggestions, func(i, j int) bool { return pendingSuggestions[i].ID < pendingSuggestions[j].ID })

	return roomRecord{
		Room: PersistentRoom{
			Code:               room.Code,
			HostID:             room.State.HostID,
			OwnerID:            room.OwnerID,
			TrackRevision:      room.TrackRevision,
			State:              stateCopy,
			DisconnectedUsers:  disconnectedUsers,
			PendingSuggestions: pendingSuggestions,
			HostDisconnectedAt: room.HostDisconnectedAt,
		},
		Sessions: sessions,
	}, true
}

// FlushState writes rooms that changed since the last flush and deletes rooms
// that are gone, in one transaction.
func (s *Server) FlushState() error {
	return s.flushState(false)
}

// SaveState is the final flush at shutdown.
func (s *Server) SaveState() error {
	return s.flushState(true)
}

func (s *Server) flushState(force bool) error {
	if s.database == nil || s.database.db == nil {
		return fmt.Errorf("database is not configured")
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	records, err := s.snapshotRooms()
	if err != nil {
		return err
	}
	changed := make(map[string][]byte)
	for code, data := range records {
		if !bytes.Equal(s.persisted[code], data) {
			changed[code] = data
		}
	}
	removed := make([]string, 0)
	for code := range s.persisted {
		if _, ok := records[code]; !ok {
			removed = append(removed, code)
		}
	}
	now := time.Now()
	if !force && len(changed) == 0 && len(removed) == 0 && now.Sub(s.lastFlush) < flushHeartbeat {
		return nil
	}

	err = s.database.db.Update(func(tx *bbolt.Tx) error {
		rooms, err := tx.CreateBucketIfNotExists(roomsBucket)
		if err != nil {
			return err
		}
		for code, data := range changed {
			if err := rooms.Put([]byte(code), data); err != nil {
				return err
			}
		}
		for _, code := range removed {
			if err := rooms.Delete([]byte(code)); err != nil {
				return err
			}
		}
		meta, err := tx.CreateBucketIfNotExists(metaBucket)
		if err != nil {
			return err
		}
		stamp := make([]byte, 8)
		binary.BigEndian.PutUint64(stamp, uint64(now.UnixMilli()))
		return meta.Put(flushedAtKey, stamp)
	})
	if err != nil {
		return fmt.Errorf("write server state: %w", err)
	}
	for code, data := range changed {
		s.persisted[code] = data
	}
	for _, code := range removed {
		delete(s.persisted, code)
	}
	s.lastFlush = now
	if force {
		s.logger.Info("Server state saved", zap.Int("rooms", len(records)))
	}
	return nil
}

// runStateFlusher persists changes every StateFlushInterval until Close, so
// a crash loses at most that much.
func (s *Server) runStateFlusher() {
	ticker := time.NewTicker(StateFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			if err := s.FlushState(); err != nil {
				s.logger.Error("Failed to flush state", zap.Error(err))
			}
		}
	}
}

func (d *database) saveServerState(data []byte) error {
	if d == nil || d.db == nil {
		return fmt.Errorf("database is not configured")
	}
	return d.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(serverStateBucket)
		if err != nil {
			return err
		}
		return bucket.Put(serverStateKey, data)
	})
}

func (d *database) loadServerState() ([]byte, error) {
	if d == nil || d.db == nil {
		return nil, fmt.Errorf("database is not configured")
	}
	var data []byte
	err := d.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(serverStateBucket)
		if bucket != nil {
			data = append(data, bucket.Get(serverStateKey)...)
		}
		return nil
	})
	return data, err
}

func (d *database) consumeServerState() error {
	if d == nil || d.db == nil {
		return fmt.Errorf("database is not configured")
	}
	return d.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(serverStateBucket)
		if bucket == nil {
			return nil
		}
		return bucket.Delete(serverStateKey)
	})
}

func (d *database) loadRoomRecords() (map[string][]byte, time.Time, error) {
	records := make(map[string][]byte)
	var flushedAt time.Time
	err := d.db.View(func(tx *bbolt.Tx) error {
		if meta := tx.Bucket(metaBucket); meta != nil {
			if stamp := meta.Get(flushedAtKey); len(stamp) == 8 {
				flushedAt = time.UnixMilli(int64(binary.BigEndian.Uint64(stamp)))
			}
		}
		rooms := tx.Bucket(roomsBucket)
		if rooms == nil {
			return nil
		}
		return rooms.ForEach(func(key, value []byte) error {
			records[string(key)] = append([]byte(nil), value...)
			return nil
		})
	})
	return records, flushedAt, err
}

// readLegacyState returns the pre-write-behind snapshot as room records.
func (s *Server) readLegacyState() ([]roomRecord, time.Time, bool, error) {
	data, err := s.database.loadServerState()
	if err != nil {
		return nil, time.Time{}, false, fmt.Errorf("read server state: %w", err)
	}
	if len(data) == 0 {
		info, statErr := os.Stat(StateFile)
		if os.IsNotExist(statErr) {
			return nil, time.Time{}, false, nil
		}
		if statErr != nil {
			return nil, time.Time{}, false, fmt.Errorf("stat legacy state file: %w", statErr)
		}
		if info.Size() > MaxStateFileSize {
			return nil, time.Time{}, false, fmt.Errorf("legacy state file exceeds %d bytes", MaxStateFileSize)
		}
		data, err = os.ReadFile(StateFile)
		if err != nil {
			return nil, time.Time{}, false, fmt.Errorf("read legacy state file: %w", err)
		}
	}
	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, time.Time{}, false, fmt.Errorf("unmarshal state: %w", err)
	}
	sessionsByRoom := make(map[string][]PersistentSession)
	for _, session := range state.Sessions {
		sessionsByRoom[session.RoomCode] = append(sessionsByRoom[session.RoomCode], session)
	}
	records := make([]roomRecord, 0, len(state.Rooms))
	for _, room := range state.Rooms {
		records = append(records, roomRecord{Room: room, Sessions: sessionsByRoom[room.Code]})
	}
	return records, state.ServerShutdownTime, true, nil
}

// LoadState restores rooms written by the state flusher. State stays in the
// database (the flusher keeps it current), so a failed startup loses nothing.
// The legacy single snapshot and JSON file are migrated once.
func (s *Server) LoadState() error {
	if s.database == nil || s.database.db == nil {
		return fmt.Errorf("database is not configured")
	}
	raw, flushedAt, err := s.database.loadRoomRecords()
	if err != nil {
		return fmt.Errorf("read room records: %w", err)
	}
	records := make([]roomRecord, 0, len(raw))
	for code, data := range raw {
		var record roomRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return fmt.Errorf("unmarshal room %s: %w", code, err)
		}
		records = append(records, record)
	}

	legacy, legacyShutdown, fromLegacy, err := s.readLegacyState()
	if err != nil {
		return err
	}
	if len(records) == 0 && fromLegacy {
		records, flushedAt = legacy, legacyShutdown
	}
	if len(records) == 0 {
		s.logger.Info("No previous server state found, starting fresh")
	}

	now := time.Now()
	offline := time.Duration(0)
	if !flushedAt.IsZero() && now.After(flushedAt) {
		offline = now.Sub(flushedAt)
	}
	s.logger.Info("Loading previous state",
		zap.Duration("offline_duration", offline),
		zap.Int("rooms", len(records)))

	s.mu.Lock()
	for _, record := range records {
		s.restoreRoomLocked(record, flushedAt, offline, now)
	}
	s.mu.Unlock()

	if fromLegacy {
		// Write the migrated rooms before dropping the legacy copies.
		if err := s.SaveState(); err != nil {
			return fmt.Errorf("migrate legacy state: %w", err)
		}
		if err := s.database.consumeServerState(); err != nil {
			return fmt.Errorf("consume legacy state: %w", err)
		}
		if err := os.Remove(StateFile); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove legacy state file: %w", err)
		}
	} else {
		// The flusher rewrites stored rooms whose content changed on restore.
		s.persistMu.Lock()
		for code, data := range raw {
			s.persisted[code] = data
		}
		s.persistMu.Unlock()
	}

	s.logger.Info("State restoration complete", zap.Int("rooms_restored", len(records)))
	return nil
}

func (s *Server) restoreRoomLocked(record roomRecord, flushedAt time.Time, offline time.Duration, now time.Time) {
	persistentRoom := record.Room
	if persistentRoom.State == nil {
		s.logger.Warn("Skipping restored room without state", zap.String("code", persistentRoom.Code))
		return
	}
	// Members connected at the last flush were dropped by the stop.
	stoppedAt := flushedAt
	if stoppedAt.IsZero() {
		stoppedAt = now
	}
	disconnectedAt := func(t time.Time) time.Time {
		if t.IsZero() {
			t = stoppedAt
		}
		return t.Add(offline)
	}

	state := persistentRoom.State
	for i := range state.Users {
		state.Users[i].IsConnected = false
	}
	state.Queue = sanitizeQueue(state.Queue)
	if state.IsPlaying && state.CurrentTrack != nil && state.CurrentTrack.Duration > 0 {
		// After a long outage the projection can run past the end; park it there
		// rather than claim the room is still playing nothing.
		if nowMs := serverNowMs(); livePlaybackPosition(state, nowMs) >= state.CurrentTrack.Duration {
			state.IsPlaying = false
			state.Position = state.CurrentTrack.Duration
			state.LastUpdate = nowMs
		}
	}

	room := &Room{
		Code:               persistentRoom.Code,
		Host:               nil, // The host pointer is only set when the real client reconnects.
		Clients:            make(map[string]*Client),
		PendingJoins:       make(map[string]*Client),
		PendingSuggestions: make(map[string]*Suggestion),
		DisconnectedUsers:  make(map[string]*Session),
		State:              state,
		OwnerID:            persistentRoom.OwnerID,
		TrackRevision:      persistentRoom.TrackRevision,
	}
	for _, ps := range persistentRoom.PendingSuggestions {
		room.PendingSuggestions[ps.ID] = &Suggestion{
			ID:           ps.ID,
			FromUserID:   ps.FromUserID,
			FromUsername: ps.FromUsername,
			Track:        ps.Track,
		}
	}
	reconnectable := make(map[string]bool, len(record.Sessions))
	for _, ps := range record.Sessions {
		if ps.Token != "" {
			reconnectable[ps.UserID] = true
		}
	}
	for userID, session := range persistentRoom.DisconnectedUsers {
		if session == nil {
			continue
		}
		if !reconnectable[userID] && len(record.Sessions) > 0 {
			// Captured mid-expiry: without a token nobody could ever resume or
			// expire this member, so drop them now.
			state.Users = withoutUser(state.Users, userID)
			continue
		}
		session.DisconnectAt = disconnectedAt(session.DisconnectAt)
		room.DisconnectedUsers[userID] = session
	}

	if persistentRoom.HostDisconnectedAt != nil {
		hostDisconnectedAt := persistentRoom.HostDisconnectedAt.Add(offline)
		room.HostDisconnectedAt = &hostDisconnectedAt
	} else if persistentRoom.HostID != "" {
		if hostSession, exists := room.DisconnectedUsers[persistentRoom.HostID]; exists {
			hostDisconnectedAt := hostSession.DisconnectAt
			room.HostDisconnectedAt = &hostDisconnectedAt
		}
	}

	s.rooms[room.Code] = room
	for _, ps := range record.Sessions {
		if ps.Token == "" {
			continue
		}
		s.sessions[ps.Token] = &Session{
			UserID:       ps.UserID,
			Username:     ps.Username,
			RoomCode:     ps.RoomCode,
			IsHost:       ps.IsHost,
			DisconnectAt: disconnectedAt(ps.DisconnectAt),
		}
	}
	s.logger.Info("Restored room",
		zap.String("code", room.Code),
		zap.String("host_id", persistentRoom.HostID),
		zap.Int("disconnected_users", len(room.DisconnectedUsers)))
}
