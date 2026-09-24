package store

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
)

func baseConfig(flagName string, pct uint32) config.Config {
	c := config.Empty()
	c.Flags[flagName] = config.Flag{
		Name: flagName, Enabled: true, Rollout: config.Rollout{Percentage: pct},
	}
	c.Settings["greeting"] = config.Setting{Value: "hello", Type: config.TypeString}
	return c
}

func TestCommitAndConflict(t *testing.T) {
	s := New()
	snap, err := s.Commit(context.Background(), baseConfig("f1", 10), 0, "alice", "first", "test")
	if err != nil {
		t.Fatalf("initial commit failed: %v", err)
	}
	t.Logf("commit ok: version=%d reason=%s", snap.Version, snap.Reason)
	if snap.Version != 1 {
		t.Fatalf("expected version 1, got %d", snap.Version)
	}
	// stale expected version must be rejected and leave state untouched
	_, err = s.Commit(context.Background(), baseConfig("f2", 20), 0, "bob", "stale", "test")
	cat := config.CatOf(err)
	t.Logf("stale commit rejected: category=%s error=%q", cat, err)
	if cat != config.CatConflict {
		t.Fatalf("expected conflict, got category=%s err=%v", cat, err)
	}
	if s.Current().Version != 1 {
		t.Fatalf("rejected commit must not change version, got %d", s.Current().Version)
	}
	// invalid config must also be rejected
	bad := config.Empty()
	bad.Flags["bad"] = config.Flag{Name: "bad", Rollout: config.Rollout{Percentage: 500}}
	_, err = s.Commit(context.Background(), bad, 1, "bob", "invalid", "test")
	cat = config.CatOf(err)
	t.Logf("invalid commit rejected: category=%s error=%q", cat, err)
	if cat != config.CatValidation {
		t.Fatalf("expected validation error, got %s", cat)
	}
	if s.Current().Version != 1 {
		t.Fatalf("invalid commit must not change version, got %d", s.Current().Version)
	}
}

func TestRollbackRestoresFullConfig(t *testing.T) {
	s := New()
	v1, _ := s.Commit(context.Background(), baseConfig("f1", 10), 0, "alice", "v1", "test")
	v2, _ := s.Commit(context.Background(), baseConfig("f1", 90), v1.Version, "alice", "v2", "test")
	t.Logf("committed v%d pct=10 then v%d pct=90", v1.Version, v2.Version)

	restored, err := s.Rollback(context.Background(), v1.Version, v2.Version, "alice", "undo experiment")
	if err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	t.Logf("rollback ok: new version=%d restored-from=%d percentage=%d provenance=%s",
		restored.Version, v1.Version, restored.Config.Flags["f1"].Rollout.Percentage, restored.Source)
	if restored.Version != 3 {
		t.Fatalf("rollback must create new version 3, got %d", restored.Version)
	}
	if got := restored.Config.Flags["f1"].Rollout.Percentage; got != 10 {
		t.Fatalf("restored percentage must be 10, got %d", got)
	}
	if restored.Source != "rollback" {
		t.Fatalf("rollback snapshot must be explainable, source=%q", restored.Source)
	}
	if _, ok := restored.Config.Settings["greeting"]; !ok {
		t.Fatalf("rollback must restore complete config including settings")
	}
	// rolling back to unknown version fails
	_, err = s.Rollback(context.Background(), 99, 3, "alice", "bad target")
	if config.CatOf(err) != config.CatNotFound {
		t.Fatalf("expected not_found, got %v", err)
	}
}

func TestConcurrentCommitsNoSilentOverwrite(t *testing.T) {
	s := New()
	const writers = 16
	var wg sync.WaitGroup
	results := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.Commit(context.Background(), baseConfig(fmt.Sprintf("f-%d", i), 50), 0,
				fmt.Sprintf("writer-%d", i), "race", "test")
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	successes, conflicts := 0, 0
	for err := range results {
		switch config.CatOf(err) {
		case "":
			successes++
		case config.CatConflict:
			conflicts++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	t.Logf("concurrent commits: success=%d conflict=%d finalVersion=%d",
		successes, conflicts, s.Current().Version)
	if successes != 1 {
		t.Fatalf("exactly one writer must win against v0, got %d successes", successes)
	}
	if conflicts != writers-1 {
		t.Fatalf("remaining writers must see conflicts, got %d", conflicts)
	}
}

func TestConcurrentReadsSeeWholeSnapshots(t *testing.T) {
	s := New()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	// readers: every observed snapshot must validate
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				cur := s.Current()
				if err := config.Validate(cur.Config); err != nil {
					t.Errorf("reader observed invalid snapshot v%d: %v", cur.Version, err)
					return
				}
				// config must be internally consistent
				if cur.Version >= 1 && cur.Config.Flags["f1"].Rollout.Percentage > 100 {
					t.Errorf("observed impossible percentage")
					return
				}
			}
		}
	}()
	// writer: serial commits with fresh expected versions
	v := int64(0)
	for i := 0; i < 100; i++ {
		pct := uint32(i % 101)
		snap, err := s.Commit(context.Background(), baseConfig("f1", pct), v, "alice", "loop", "test")
		if err != nil {
			t.Fatalf("commit %d failed: %v", i, err)
		}
		v = snap.Version
	}
	close(stop)
	wg.Wait()
	t.Logf("100 commits interleaved with readers, finalVersion=%d", s.Current().Version)
}
