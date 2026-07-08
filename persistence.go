package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"go.uber.org/zap"
)

const (
	StateFile        = "server_state.json"
	MaxStateFileSize = 50 * 1024 * 1024
)

// PersistentState contains all data that needs to be saved across server restarts
type PersistentState struct {
	ServerShutdownTime time.Time           `json:"server_shutdown_time"`
	Rooms              []PersistentRoom    `json:"rooms"`
	Sessions           []PersistentSession `json:"sessions"`
}

// PersistentRoom is a serializable version of Room
type PersistentRoom struct {
	Code               string                 `json:"code"`
	HostID             string                 `json:"host_id"`
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

// PersistentSession is a serializable version of Session with token
type PersistentSession struct {
	Token        string    `json:"token"`
	UserID       string    `json:"user_id"`
	Username     string    `json:"username"`
	RoomCode     string    `json:"room_code"`
	IsHost       bool      `json:"is_host"`
	DisconnectAt time.Time `json:"disconnect_at"`
}

// SaveState saves the current server state to disk
func (s *Server) SaveState() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	state := PersistentState{
		ServerShutdownTime: now,
		Rooms:              make([]PersistentRoom, 0),
		Sessions:           make([]PersistentSession, 0),
	}
	sessions := make(map[string]PersistentSession)
	for token, session := range s.sessions {
		if token == "" || session == nil {
			continue
		}
		sessions[token] = PersistentSession{
			Token:        token,
			UserID:       session.UserID,
			Username:     session.Username,
			RoomCode:     session.RoomCode,
			IsHost:       session.IsHost,
			DisconnectAt: session.DisconnectAt,
		}
	}

	// Save all rooms
	for _, room := range s.rooms {
		room.mu.RLock()
		stateCopy := cloneRoomState(room.State)
		if stateCopy == nil {
			room.mu.RUnlock()
			continue
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
			session := &Session{
				UserID:       userID,
				Username:     client.userName(),
				RoomCode:     room.Code,
				IsHost:       isHost,
				DisconnectAt: now,
			}
			disconnectedUsers[userID] = session
			sessions[token] = PersistentSession{
				Token:        token,
				UserID:       session.UserID,
				Username:     session.Username,
				RoomCode:     session.RoomCode,
				IsHost:       session.IsHost,
				DisconnectAt: session.DisconnectAt,
			}
		}

		// Convert pending suggestions
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

		// Get host ID from room state (room.Host can be nil after disconnection)
		hostID := room.State.HostID

		hostDisconnectedAt := room.HostDisconnectedAt
		if room.State.HostID != "" && room.Clients[room.State.HostID] != nil {
			shutdownTime := now
			hostDisconnectedAt = &shutdownTime
		}

		persistentRoom := PersistentRoom{
			Code:               room.Code,
			HostID:             hostID,
			State:              stateCopy,
			DisconnectedUsers:  disconnectedUsers,
			PendingSuggestions: pendingSuggestions,
			HostDisconnectedAt: hostDisconnectedAt,
		}

		state.Rooms = append(state.Rooms, persistentRoom)
		room.mu.RUnlock()
	}

	tokens := make([]string, 0, len(sessions))
	for token := range sessions {
		tokens = append(tokens, token)
	}
	sort.Strings(tokens)
	for _, token := range tokens {
		state.Sessions = append(state.Sessions, sessions[token])
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	if err := writeStateFile(StateFile, data); err != nil {
		return fmt.Errorf("write state file: %w", err)
	}

	s.logger.Info("Server state saved",
		zap.Int("rooms", len(state.Rooms)),
		zap.Int("sessions", len(state.Sessions)))

	return nil
}

func writeStateFile(path string, data []byte) error {
	tmp, err := os.CreateTemp(".", path+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// LoadState loads the server state from disk
func (s *Server) LoadState() error {
	// Check if state file exists
	info, err := os.Stat(StateFile)
	if os.IsNotExist(err) {
		s.logger.Info("No previous state file found, starting fresh")
		return nil
	} else if err != nil {
		return fmt.Errorf("stat state file: %w", err)
	}
	if info.Size() > MaxStateFileSize {
		return fmt.Errorf("state file exceeds %d bytes", MaxStateFileSize)
	}

	// Read state file
	data, err := os.ReadFile(StateFile)
	if err != nil {
		return fmt.Errorf("read state file: %w", err)
	}

	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("unmarshal state: %w", err)
	}

	// Calculate time elapsed since shutdown
	shutdownDuration := time.Since(state.ServerShutdownTime)
	s.logger.Info("Loading previous state",
		zap.Duration("offline_duration", shutdownDuration),
		zap.Int("rooms", len(state.Rooms)),
		zap.Int("sessions", len(state.Sessions)))

	s.mu.Lock()
	defer s.mu.Unlock()

	// Restore rooms
	for _, persistentRoom := range state.Rooms {
		if persistentRoom.State == nil {
			s.logger.Warn("Skipping restored room without state", zap.String("code", persistentRoom.Code))
			continue
		}
		disconnectedUsers := persistentRoom.DisconnectedUsers
		if disconnectedUsers == nil {
			disconnectedUsers = make(map[string]*Session)
		}
		if persistentRoom.State != nil {
			for i := range persistentRoom.State.Users {
				persistentRoom.State.Users[i].IsConnected = false
			}
		}

		room := &Room{
			Code:               persistentRoom.Code,
			Host:               nil, // The host pointer is only set when the real client reconnects.
			Clients:            make(map[string]*Client),
			PendingJoins:       make(map[string]*Client),
			PendingSuggestions: make(map[string]*Suggestion),
			DisconnectedUsers:  disconnectedUsers,
			State:              persistentRoom.State,
			BufferingUsers:     make(map[string]bool),
			HostDisconnectedAt: persistentRoom.HostDisconnectedAt,
		}

		// Restore pending suggestions
		for _, ps := range persistentRoom.PendingSuggestions {
			room.PendingSuggestions[ps.ID] = &Suggestion{
				ID:           ps.ID,
				FromUserID:   ps.FromUserID,
				FromUsername: ps.FromUsername,
				Track:        ps.Track,
			}
		}

		// Update disconnect times for all users to account for shutdown duration
		for _, session := range room.DisconnectedUsers {
			session.DisconnectAt = session.DisconnectAt.Add(shutdownDuration)
		}

		// Update host disconnected time if applicable
		if room.HostDisconnectedAt != nil {
			newTime := room.HostDisconnectedAt.Add(shutdownDuration)
			room.HostDisconnectedAt = &newTime
		}

		if room.HostDisconnectedAt == nil && persistentRoom.HostID != "" {
			if hostSession, exists := room.DisconnectedUsers[persistentRoom.HostID]; exists && hostSession != nil {
				hostDisconnectedAt := hostSession.DisconnectAt
				room.HostDisconnectedAt = &hostDisconnectedAt
			}
		}

		s.rooms[room.Code] = room
		s.logger.Info("Restored room",
			zap.String("code", room.Code),
			zap.String("host_id", persistentRoom.HostID),
			zap.Int("disconnected_users", len(room.DisconnectedUsers)))
	}

	// Restore sessions
	for _, ps := range state.Sessions {
		// Adjust disconnect time to account for shutdown duration
		session := &Session{
			UserID:       ps.UserID,
			Username:     ps.Username,
			RoomCode:     ps.RoomCode,
			IsHost:       ps.IsHost,
			DisconnectAt: ps.DisconnectAt.Add(shutdownDuration),
		}
		s.sessions[ps.Token] = session
	}

	s.logger.Info("State restoration complete",
		zap.Int("rooms_restored", len(state.Rooms)),
		zap.Int("sessions_restored", len(state.Sessions)))

	return nil
}
