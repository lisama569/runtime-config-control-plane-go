package store

import (
	"context"
	"sync"
	"time"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
)

// Store owns the currently effective, immutable versioned snapshot and
// its history. All reads observe a complete snapshot; updates happen via
// compare-and-set so concurrent writers cannot silently overwrite.
type Store struct {
	mu      sync.RWMutex
	history []config.Snapshot // ascending by version, [0] is the seed
}

// New constructs an empty store seeded with version 0.
func New() *Store {
	return &Store{
		history: []config.Snapshot{{
			Version:   0,
			Config:    config.Empty(),
			Source:    "seed",
			Reason:    "initial empty configuration",
			CreatedAt: time.Now().Unix(),
		}},
	}
}

// Current returns the latest committed snapshot. The returned snapshot is
// a deep copy; callers may not mutate shared state.
func (s *Store) Current() config.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cloneLatest()
}

func (s *Store) cloneLatest() config.Snapshot {
	latest := s.history[len(s.history)-1]
	latest.Config = latest.Config.Clone()
	return latest
}

// Commit validates next and, when expectedVersion matches the current
// version, atomically publishes it under version+1. A stale
// expectedVersion is rejected with CatConflict; an invalid config with
// CatValidation/CatTypeMismatch. On rejection the store is unchanged.
func (s *Store) Commit(ctx context.Context, next config.Config, expectedVersion int64, actor, reason, src string) (config.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return config.Snapshot{}, err
	}
	if err := config.Validate(next); err != nil {
		return config.Snapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.history[len(s.history)-1]
	if expectedVersion != cur.Version {
		return config.Snapshot{}, config.NewError(
			config.CatConflict,
			"stale expected version %d, current version is %d", expectedVersion, cur.Version)
	}
	snap := config.Snapshot{
		Version:   cur.Version + 1,
		Config:    next.Clone(),
		Source:    src,
		Reason:    reason,
		CreatedAt: time.Now().Unix(),
	}
	s.history = append(s.history, snap)
	return s.cloneLatest(), nil
}

// Rollback reinstates a historical version as a new version. The
// reinstated config is a deep copy of the stored snapshot, so the result
// is complete and explainable (Source/Reason describe the rollback).
func (s *Store) Rollback(ctx context.Context, targetVersion int64, expectedVersion int64, actor, reason string) (config.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return config.Snapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.history[len(s.history)-1]
	if expectedVersion != cur.Version {
		return config.Snapshot{}, config.NewError(
			config.CatConflict,
			"stale expected version %d, current version is %d", expectedVersion, cur.Version)
	}
	var target *config.Snapshot
	for i := range s.history {
		if s.history[i].Version == targetVersion {
			target = &s.history[i]
			break
		}
	}
	if target == nil {
		return config.Snapshot{}, config.NewError(config.CatNotFound, "version %d not found in history", targetVersion)
	}
	snap := config.Snapshot{
		Version:   cur.Version + 1,
		Config:    target.Config.Clone(),
		Source:    "rollback",
		Reason:    reason,
		CreatedAt: time.Now().Unix(),
	}
	s.history = append(s.history, snap)
	return s.cloneLatest(), nil
}

// History returns committed metadata in version order, newest first.
// Config payloads are omitted to keep the listing cheap.
func (s *Store) History(limit int) []config.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := len(s.history)
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]config.Snapshot, 0, n)
	for i := len(s.history) - 1; i >= 0 && len(out) < n; i-- {
		meta := s.history[i]
		meta.Config = config.Empty()
		out = append(out, meta)
	}
	return out
}

// Get returns a historical snapshot by version.
func (s *Store) Get(version int64) (config.Snapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := range s.history {
		if s.history[i].Version == version {
			snap := s.history[i]
			snap.Config = snap.Config.Clone()
			return snap, true
		}
	}
	return config.Snapshot{}, false
}
