package store

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

// A failing durability hook must abort the commit atomically: no snapshot is
// published, history is unchanged, and the version number is not consumed
// (the next successful commit reuses it).
func TestBeforeCommitFailureAbortsAtomically(t *testing.T) {
	var fail atomic.Bool
	hook := func(_ context.Context, next *config.Snapshot, _ []*config.Snapshot) error {
		if fail.Load() {
			return errs.New(errs.KindPersistIO, "simulated disk failure")
		}
		return nil
	}
	s := NewMemoryWithHook(initialSnap(), hook)
	ctx := context.Background()

	fail.Store(true)
	if _, err := s.CAS(ctx, 1, docWith("k", 2), Change{}); err == nil {
		t.Fatal("expected hook failure to abort CAS")
	}
	if s.Current().Version != 1 {
		t.Fatalf("current = %d, must remain 1 after aborted commit", s.Current().Version)
	}
	if v := s.Current().Doc.Properties["k"].Value.Int; v != 1 {
		t.Fatalf("value = %d, aborted commit must not mutate state", v)
	}
	if len(s.History()) != 1 {
		t.Fatalf("history len = %d, aborted commit must not append", len(s.History()))
	}

	// The version is not consumed: the retry succeeds as version 2.
	fail.Store(false)
	snap, err := s.CAS(ctx, 1, docWith("k", 2), Change{})
	if err != nil {
		t.Fatal(err)
	}
	if snap.Version != 2 {
		t.Fatalf("retry version = %d, want 2 (aborted version not reused/skipped)", snap.Version)
	}

	// Rollback honors the same guarantee.
	fail.Store(true)
	if _, err := s.Rollback(ctx, 1, "admin"); err == nil {
		t.Fatal("expected hook failure to abort rollback")
	}
	if s.Current().Version != 2 {
		t.Fatalf("current = %d after aborted rollback", s.Current().Version)
	}
}

// The hook must observe the complete post-commit timeline (including trim),
// and every committed snapshot is exactly what becomes visible.
func TestBeforeCommitSeesTrimmedHistory(t *testing.T) {
	var seen atomic.Int64
	hook := func(_ context.Context, next *config.Snapshot, history []*config.Snapshot) error {
		seen.Add(1)
		if history[len(history)-1].Version != next.Version {
			t.Errorf("hook history tail = %d, next = %d", history[len(history)-1].Version, next.Version)
		}
		if len(history) > maxHistory {
			t.Errorf("hook saw %d history entries, cap %d", len(history), maxHistory)
		}
		return nil
	}
	s := NewMemoryWithHook(initialSnap(), hook)
	ctx := context.Background()
	for i := 0; i < maxHistory+10; i++ {
		cur := s.Current().Version
		if _, err := s.CAS(ctx, cur, docWith("k", cur+1), Change{}); err != nil {
			t.Fatal(err)
		}
	}
	if int(seen.Load()) != maxHistory+10 {
		t.Fatalf("hook ran %d times, want %d", seen.Load(), maxHistory+10)
	}
	if len(s.History()) != maxHistory {
		t.Fatalf("history = %d, want cap %d", len(s.History()), maxHistory)
	}
	// Restored-style constructor: newest becomes live, versions continue.
	rs := NewRestored(s.(*memoryStore).history, nil)
	if rs.Current().Version != int64(maxHistory+11) {
		t.Fatalf("restored current = %d", rs.Current().Version)
	}
	if _, err := rs.CAS(ctx, rs.Current().Version, docWith("k", 1), Change{}); err != nil {
		t.Fatal(err)
	}
	if rs.Current().Version != int64(maxHistory+12) {
		t.Fatalf("after restored CAS = %d", rs.Current().Version)
	}
}

var _ = errors.Is
