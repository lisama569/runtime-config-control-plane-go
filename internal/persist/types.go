// Package persist provides crash-safe on-disk durability for the effective
// configuration timeline and the audit trail.
//
// Two on-disk artefacts live inside one data directory:
//
//	state.json      the latest durable control-plane state (version timeline,
//	                complete snapshots and API-override states), written via a
//	                CRC32-protected, fsynced, atomic temp-file+rename sequence;
//	state.json.bak  the previous complete state, kept as a fallback so a torn
//	                primary can never force the plane to start from scratch.
//	audit.log       append-only, framed, CRC32-protected audit journal.
//
// Every committed configuration is made durable (and checksummed) BEFORE it
// becomes visible in memory, so a process killed mid-write either keeps the
// last complete state on restart or rejects the damaged artefact explicitly.
package persist

import (
	"sync"

	"github.com/lisama569/runtime-config-control-plane-go/internal/audit"
	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
)

// Limits bounds on-disk growth. Every bound is explicit; once exceeded, old
// retained data is pruned or the commit is rejected rather than growing
// forever.
type Limits struct {
	// MaxStateBytes rejects a state write whose serialized payload is larger.
	MaxStateBytes int
	// RetainVersions bounds how many historical versions (including current)
	// stay in state.json for rollback.
	RetainVersions int
	// MaxAuditEntries bounds the number of durable audit entries; old entries
	// are pruned (journal compaction) beyond it.
	MaxAuditEntries int
	// MaxAuditBytes bounds the audit journal size; beyond it compaction runs.
	MaxAuditBytes int
}

// DefaultLimits are the on-disk capacity defaults (see docs/DESIGN.md §8).
func DefaultLimits() Limits {
	return Limits{
		MaxStateBytes:   16 << 20, // 16 MiB
		RetainVersions:  100,
		MaxAuditEntries: 10000,
		MaxAuditBytes:   16 << 20, // 16 MiB
	}
}

// Override is the durable form of the control plane's api-override layer for
// one effective version. Tombstones record keys explicitly deleted through
// the API so precedence over file sources is preserved across restarts.
type Override struct {
	Props     map[string]config.Property
	Feats     map[string]config.Feature
	DeadProps map[string]bool
	DeadFeats map[string]bool
}

// State is the complete durable control-plane state. It contains everything
// needed to rebuild the in-memory store and override timeline without any
// file sources. Snapshot Versions strictly increase and are never reused.
type State struct {
	// Current is the last committed effective snapshot.
	Current *config.Snapshot
	// History holds retained snapshots oldest-first, including Current.
	History []*config.Snapshot
	// Overrides maps an effective version -> api-override state at that
	// version; it must cover every version in History.
	Overrides map[int64]Override
}

// Problem reports one damaged durable artefact discovered during recovery.
// Kind is one of the stable errs.Kind* values so failures are classifiable.
type Problem struct {
	Kind    string
	Message string
}

func (p Problem) String() string { return p.Kind + ": " + p.Message }

// RecoveryReport describes what recovery found and did. It never exposes a
// damaged intermediate state as live: either a complete State is returned or
// Open fails.
type RecoveryReport struct {
	// Loaded is true when a complete state was loaded from state.json.
	Loaded bool
	// UsedBackup is true when the primary was damaged and state.json.bak
	// supplied the recovered state.
	UsedBackup bool
	// StateVersion is the recovered current version, or 0 on a fresh start.
	StateVersion int64
	// AuditEntries is how many audit frames were replayed.
	AuditEntries int
	// Problems lists damaged-but-handled artefacts (backup fallback, discarded
	// journal tail...). Fatal state corruption is returned as an error instead.
	Problems []Problem
}

// Store is the crash-safe durability backend.
type Store struct {
	dir    string
	limits Limits

	journalMu    sync.Mutex
	auditEntries []audit.Entry // replayed + appended during this process
}

// Dir reports the data directory.
func (s *Store) Dir() string { return s.dir }

// AuditEntries returns a defensive copy of the audit entries replayed from
// disk and appended during this process, in append order.
func (s *Store) AuditEntries() []audit.Entry {
	s.journalMu.Lock()
	defer s.journalMu.Unlock()
	out := make([]audit.Entry, len(s.auditEntries))
	copy(out, s.auditEntries)
	return out
}
