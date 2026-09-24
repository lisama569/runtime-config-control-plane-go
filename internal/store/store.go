// Package store holds the immutable effective snapshot behind an atomic
// pointer and records versioned history for audit and rollback.
package store

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

// Change describes who/what produced a new snapshot.
type Change struct {
	Reason   string // e.g. "hot-reload", "api-patch", "rollback"
	Actor    string
	Origin   map[string]string
	Features map[string]string
}

// Store provides atomic, wait-free reads and serialized updates.
type Store interface {
	Current() *config.Snapshot
	// CAS replaces the snapshot only when current version == expected.
	// A mismatched expected version yields a STALE_VERSION error.
	CAS(ctx context.Context, expected int64, doc *config.Document, ch Change) (*config.Snapshot, error)
	// Rollback restores the snapshot at targetVersion, producing a NEW version.
	Rollback(ctx context.Context, targetVersion int64, actor string) (*config.Snapshot, error)
	// AtVersion returns a historical snapshot.
	AtVersion(version int64) (*config.Snapshot, bool)
	// History returns all recorded versions, oldest first.
	History() []int64
}

// maxHistory bounds retained history; oldest entries are dropped beyond it.
const maxHistory = 100

type memoryStore struct {
	current atomic.Pointer[config.Snapshot]
	mu      sync.Mutex
	history []*config.Snapshot // including current, oldest first
}

// NewMemory wraps an already-built initial snapshot.
func NewMemory(initial *config.Snapshot) Store {
	s := &memoryStore{history: []*config.Snapshot{initial}}
	s.current.Store(initial)
	return s
}

// Current returns the live snapshot with no locks (atomic pointer read),
// so in-flight readers can never observe a half-updated configuration.
func (s *memoryStore) Current() *config.Snapshot { return s.current.Load() }

func (s *memoryStore) CAS(ctx context.Context, expected int64, doc *config.Document, ch Change) (*config.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, errs.New(errs.KindValidation, "store: cannot swap in a nil document")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.current.Load()
	if cur.Version != expected {
		return nil, errs.New(errs.KindStaleVersion,
			"store: expected version "+itoa(expected)+" but current is "+itoa(cur.Version))
	}
	snap := &config.Snapshot{
		Version:        expected + 1,
		Doc:            doc,
		PropertyOrigin: cloneOrigin(ch.Origin),
		FeatureOrigin:  cloneOrigin(ch.Features),
		SourceVersions: cloneVersions(cur.SourceVersions),
		CreatedAtUnix:  time.Now().Unix(),
	}
	s.commit(snap)
	return snap, nil
}

func (s *memoryStore) Rollback(ctx context.Context, targetVersion int64, actor string) (*config.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var target *config.Snapshot
	for _, snap := range s.history {
		if snap.Version == targetVersion {
			target = snap
			break
		}
	}
	if target == nil {
		return nil, errs.New(errs.KindRollbackFailed,
			"store: version "+itoa(targetVersion)+" is not available for rollback")
	}
	// Rollback restores the COMPLETE effective configuration at the target
	// version, but issues a fresh version number so the timeline stays linear
	// and auditable. Provenance is copied from the target for explainability.
	snap := &config.Snapshot{
		Version:        s.current.Load().Version + 1,
		Doc:            target.Doc,
		PropertyOrigin: cloneOrigin(target.PropertyOrigin),
		FeatureOrigin:  cloneOrigin(target.FeatureOrigin),
		SourceVersions: cloneVersions(target.SourceVersions),
		CreatedAtUnix:  time.Now().Unix(),
	}
	_ = actor // actor is recorded by the audit layer at the caller
	s.commit(snap)
	return snap, nil
}

func (s *memoryStore) commit(snap *config.Snapshot) {
	s.history = append(s.history, snap)
	if len(s.history) > maxHistory {
		s.history = s.history[len(s.history)-maxHistory:]
	}
	// Atomic pointer publish is the single point of visibility.
	s.current.Store(snap)
}

func (s *memoryStore) AtVersion(version int64) (*config.Snapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, snap := range s.history {
		if snap.Version == version {
			return snap, true
		}
	}
	return nil, false
}

func (s *memoryStore) History() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, len(s.history))
	for i, snap := range s.history {
		out[i] = snap.Version
	}
	return out
}

func cloneOrigin(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneVersions(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
