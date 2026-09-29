package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

func docWith(k string, n int64) *config.Document {
	return &config.Document{
		Properties: map[string]config.Property{
			k: {Key: k, Value: config.Value{Kind: config.KindInt, Int: n}},
		},
		Features: map[string]config.Feature{},
	}
}

func initialSnap() *config.Snapshot {
	return &config.Snapshot{
		Version: 1, Doc: docWith("k", 1),
		PropertyOrigin: map[string]string{"k": "base"},
		FeatureOrigin:  map[string]string{},
		SourceVersions: map[string]int64{"base": 1},
	}
}

func TestCASStaleVersionRejected(t *testing.T) {
	s := NewMemory(initialSnap())
	ctx := context.Background()

	win, err := s.CAS(ctx, 1, docWith("k", 2), Change{Reason: "t", Actor: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if win.Version != 2 {
		t.Fatalf("new version = %d, want 2", win.Version)
	}
	// A second writer still holding version 1 must be rejected explicitly.
	_, err = s.CAS(ctx, 1, docWith("k", 3), Change{Reason: "t", Actor: "b"})
	if err == nil || !errs.Of(err, errs.KindStaleVersion) {
		t.Fatalf("expected STALE_VERSION, got %v", err)
	}
	if s.Current().Doc.Properties["k"].Value.Int != 2 {
		t.Fatal("rejected stale write must not mutate state")
	}
	t.Logf("[version=%d] stale CAS on expected=1 classified as %s",
		s.Current().Version, errs.Classify(err))
}

func TestConcurrentCASNoSilentOverwrite(t *testing.T) {
	s := NewMemory(initialSnap())
	ctx := context.Background()
	var wg sync.WaitGroup
	var accepted, rejected int64
	const writers = 50
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				cur := s.Current()
				_, err := s.CAS(ctx, cur.Version,
					docWith("k", cur.Doc.Properties["k"].Value.Int+1),
					Change{Reason: "concurrent", Actor: fmt.Sprintf("w%d", id)})
				if err == nil {
					atomic.AddInt64(&accepted, 1)
					return
				}
				if !errs.Of(err, errs.KindStaleVersion) {
					t.Errorf("unexpected error: %v", err)
					return
				}
				atomic.AddInt64(&rejected, 1)
			}
		}(w)
	}
	wg.Wait()
	if accepted != writers {
		t.Fatalf("accepted=%d want %d", accepted, writers)
	}
	final := s.Current().Doc.Properties["k"].Value.Int
	if final != 1+writers {
		t.Fatalf("final value = %d, want %d (a write was silently lost)", final, 1+writers)
	}
	t.Logf("[version=%d] accepted=%d rejected(retried)=%d final k=%d",
		s.Current().Version, accepted, rejected, final)
}

func TestRollbackRestoresFullConfig(t *testing.T) {
	s := NewMemory(initialSnap())
	ctx := context.Background()
	_, _ = s.CAS(ctx, 1, docWith("k", 2), Change{
		Reason: "t", Actor: "a", Origin: map[string]string{"k": "base"}})
	v2 := s.Current()
	_, _ = s.CAS(ctx, 2, docWith("k", 3), Change{
		Reason: "t", Actor: "a", Origin: map[string]string{"k": "base"}})

	snap, err := s.Rollback(ctx, v2.Version, "admin")
	if err != nil {
		t.Fatal(err)
	}
	if snap.Version != 4 {
		t.Fatalf("rollback must issue a NEW version, got %d", snap.Version)
	}
	if snap.Doc.Properties["k"].Value.Int != 2 {
		t.Fatalf("rollback restored k=%d, want 2", snap.Doc.Properties["k"].Value.Int)
	}
	if snap.PropertyOrigin["k"] != "base" {
		t.Fatalf("provenance not restored: %v", snap.PropertyOrigin)
	}
	// History is linear and v2 remains available.
	hist := s.History()
	if len(hist) != 4 {
		t.Fatalf("history = %v", hist)
	}
	_, err = s.Rollback(ctx, 999, "admin")
	if !errs.Of(err, errs.KindRollbackFailed) {
		t.Fatalf("missing-version rollback must fail ROLLBACK_FAILED, got %v", err)
	}
	t.Logf("[version=%d] rollback to 2 restored k=2 with provenance; history=%v",
		snap.Version, hist)
}

func TestConcurrentReadersNeverSeeHalfState(t *testing.T) {
	s := NewMemory(initialSnap())
	ctx := context.Background()
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Reader invariant: k is always exactly one integer property, and the
	// version reported matches the value relationship (value increments with
	// version in this test), so a torn read would be detected.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				cur := s.Current()
				p, ok := cur.Doc.Properties["k"]
				if !ok || p.Value.Int != cur.Version {
					t.Errorf("torn/inconsistent read: version=%d value=%d exists=%t",
						cur.Version, p.Value.Int, ok)
					return
				}
			}
		}
	}()

	for v := int64(1); v < 200; v++ {
		for {
			cur := s.Current()
			d := docWith("k", cur.Version+1)
			if _, err := s.CAS(ctx, cur.Version, d, Change{}); err == nil {
				break
			}
		}
	}
	close(stop)
	wg.Wait()
	t.Logf("[version=%d] concurrent readers always observed whole snapshots", s.Current().Version)
}

func TestBeforePublishHookFailureAbortsCommit(t *testing.T) {
	initial := initialSnap()
	s := NewMemoryWithHook(initial, func(snap *config.Snapshot, history []*config.Snapshot) error {
		return errs.New(errs.KindPersistFailed, "simulated disk failure")
	})
	ctx := context.Background()
	before := s.Current()
	if _, err := s.CAS(ctx, before.Version, docWith("k", 2), Change{}); err == nil {
		t.Fatal("expected hook failure to abort the CAS")
	} else if !errs.Of(err, errs.KindPersistFailed) {
		t.Fatalf("want PERSIST_FAILED, got %v", errs.Classify(err))
	}
	if s.Current().Version != before.Version {
		t.Fatalf("current advanced despite hook failure: %d", s.Current().Version)
	}
	if got := s.History(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("history mutated despite hook failure: %v", got)
	}
	// A later successful commit recomputes version+1 from the still-current
	// snapshot: rejected version number is not reused as a duplicate commit.
	s2 := NewMemoryWithHook(initial, func(snap *config.Snapshot, history []*config.Snapshot) error {
		return nil
	})
	snap, err := s2.CAS(ctx, 1, docWith("k", 2), Change{})
	if err != nil {
		t.Fatal(err)
	}
	if snap.Version != 2 {
		t.Fatalf("next commit version = %d want 2", snap.Version)
	}
}

func TestHookSeesTrimmedHistory(t *testing.T) {
	initial := initialSnap()
	var seen int
	s := NewMemoryWithHook(initial, func(snap *config.Snapshot, history []*config.Snapshot) error {
		seen = len(history)
		return nil
	})
	ctx := context.Background()
	for i := 0; i < 150; i++ {
		cur := s.Current()
		if _, err := s.CAS(ctx, cur.Version, docWith("k", cur.Version+1), Change{}); err != nil {
			t.Fatal(err)
		}
	}
	if seen != maxHistory {
		t.Fatalf("hook saw %d history entries, want trimmed %d", seen, maxHistory)
	}
	if got := len(s.History()); got != maxHistory {
		t.Fatalf("history len = %d want %d", got, maxHistory)
	}
}
