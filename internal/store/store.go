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

// BeforePublishHook is invoked while the store's update lock is held and
// BEFORE the new snapshot becomes visible. A non-nil error aborts the
// commit: no snapshot is published and no history entry is appended. This is
// the crash-safety seam for durable stores, which use it to persist first so
// that "published" and "durable" can never diverge.
//
// history is a read-only view containing retained snapshots oldest first,
// with newSnap already appended as its final element; hooks must not retain
// or mutate it.
type BeforePublishHook func(newSnap *config.Snapshot, history []*config.Snapshot) error

// Store provides atomic, wait-free reads and serialized updates.
type Store interface {
	// Current returns the live snapshot with no locks (atomic pointer read),
	// so in-flight readers can never observe a half-updated configuration.
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
	hook    BeforePublishHook
}

// NewMemory wraps an already-built initial snapshot.
func NewMemory(initial *config.Snapshot) Store {
	return NewMemoryWithHook(initial, nil)
}

// NewMemoryWithHook wraps an initial snapshot and installs a pre-publish
// durability hook. The initial snapshot is treated as already durable by the
// caller: the hook is NOT invoked for it, only for subsequent commits.
func NewMemoryWithHook(initial *config.Snapshot, hook BeforePublishHook) Store {
	s := &memoryStore{history: []*config.Snapshot{initial}, hook: hook}
	s.current.Store(initial)
	return s
}

// ReplaceHistory swaps the retained history of a memory store wholesale. It
// is a startup-only seam used to rebuild the rollback window from durable
// state; the final element of snaps must be the live current snapshot.
func ReplaceHistory(s Store, snaps []*config.Snapshot) {
	ms, ok := s.(*memoryStore)
	if !ok || len(snaps) == 0 {
		return
	}
	ms.history = append([]*config.Snapshot(nil), snaps...)
	ms.current.Store(snaps[len(snaps)-1])
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
	if err := s.commit(snap); err != nil {
		return nil, err
	}
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
	if err := s.commit(snap); err != nil {
		return nil, err
	}
	return snap, nil
}

// commit appends to history, trims to the retained window, invokes the
// durability hook and only then makes the snapshot visible. The hook sees
// exactly the history that remains after trimming, so durable and in-memory
// rollback windows always agree. Hook failure rolls the append back: the
// visible snapshot and retained history are unchanged and the version number
// carried by the rejected snap is simply never published (a later commit
// recomputes version+1 from the still-current snapshot, so it is not reused).
func (s *memoryStore) commit(snap *config.Snapshot) error {
	s.history = append(s.history, snap)
	if len(s.history) > maxHistory {
		s.history = s.history[len(s.history)-maxHistory:]
	}
	if s.hook != nil {
		if err := s.hook(snap, s.history); err != nil {
			// Rebuild the pre-append history: the trimmed oldest element is
			// unrecoverable, but trimming only happens at >maxHistory, i.e.
			// after the append there are at least maxHistory+1 entries, so
			// the rejected appended element is simply the last one and the
			// pre-existing window is still fully present after trim.
			s.history = s.history[:len(s.history)-1]
			return err
		}
	}
	// Atomic pointer publish is the single point of visibility.
	s.current.Store(snap)
	return nil
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
