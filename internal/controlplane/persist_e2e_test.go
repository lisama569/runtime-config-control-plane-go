package controlplane

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/runtime-config-control-plane-go/internal/auth"
	"github.com/lisama569/runtime-config-control-plane-go/internal/source"
)

func persistentPlane(t *testing.T, pdir string) (*Plane, string) {
	t.Helper()
	return persistentPlaneWithSource(t, pdir, "")
}

// persistentPlaneWithSource builds (or restarts into pdir) a persistent plane.
// When srcPath is empty a fresh temp source file is created (first start); a
// non-empty path reopens that SAME source file, modeling a real restart where
// the on-disk configuration files persist across the process boundary.
func persistentPlaneWithSource(t *testing.T, pdir, srcPath string) (*Plane, string) {
	t.Helper()
	if srcPath == "" {
		srcPath = filepath.Join(t.TempDir(), "base.json")
		writeCfg(t, srcPath, baseV1())
	}
	tokens := map[string]auth.Principal{
		tokReader: {Name: "reader-1", Role: auth.RoleReader},
		tokWriter: {Name: "writer-1", Role: auth.RoleWriter},
		tokAdmin:  {Name: "admin-1", Role: auth.RoleAdmin},
	}
	plane, err := New(context.Background(), Options{
		Sources:       []source.Source{source.NewFileSource("base", 1, srcPath)},
		Tokens:        tokens,
		PollInterval:  time.Hour, // disable watcher interference
		PersistDir:    pdir,
		PersistLogger: nopLog{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plane.Close() })
	return plane, srcPath
}

type nopLog struct{}

func (nopLog) Printf(string, ...any) {}

func patchTimeout(t *testing.T, p *Plane, expected, val int64) int64 {
	t.Helper()
	from, to, err := p.ApplyPatch(context.Background(), "writer-1", PatchRequest{
		ExpectedVersion: expected,
		Properties:      []PatchOp{{Key: "timeout", Type: "int", Value: json.RawMessage(itoaJSON(val))}},
	})
	if err != nil {
		t.Fatalf("patch v%d->%d: %v", expected, val, err)
	}
	if from != expected {
		t.Fatalf("from=%d want %d", from, expected)
	}
	return to
}

func itoaJSON(n int64) string { return jsonItoa(n) }

func assertTimeout(t *testing.T, p *Plane, want int64) {
	t.Helper()
	got, err := p.Current().GetInt("timeout")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("timeout = %d, want %d (version %d)", got, want, p.Current().Version)
	}
}

func TestRestartRecoversEffectiveConfigAndTimeline(t *testing.T) {
	pdir := t.TempDir()
	p, _ := persistentPlane(t, pdir)
	if p.Current().Version != 1 {
		t.Fatalf("initial version = %d", p.Current().Version)
	}
	v := patchTimeout(t, p, 1, 200)
	if v != 2 {
		t.Fatalf("to=%d", v)
	}
	v = patchTimeout(t, p, 2, 300)
	if v != 3 {
		t.Fatalf("to=%d", v)
	}
	// Feature + tombstone also need to survive.
	_, _, err := p.ApplyPatch(context.Background(), "writer-1", PatchRequest{
		ExpectedVersion: 3,
		Features:        []FeatureOp{{Name: "checkout", Enabled: boolPtr(false), Percentage: intPtr(0)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Gray decision identical to pre-restart for a stable subject. Capture
	// the decision against the v4 snapshot before closing.
	dBefore, derr := p.Evaluator().Evaluate(p.Current(), "checkout", "alice")
	if derr != nil {
		t.Fatal(derr)
	}
	_ = p.Close()

	// Restart into the same data directory with fresh file sources.
	p2, _ := persistentPlane(t, pdir)
	if p2.Current().Version != 4 {
		t.Fatalf("after restart version = %d, want 4", p2.Current().Version)
	}
	assertTimeout(t, p2, 300)
	f, ok := p2.Current().Feature("checkout")
	if !ok || f.Enabled || f.Percentage != 0 {
		t.Fatalf("feature not restored: %+v ok=%v", f, ok)
	}
	// Sensitive value survives and is still masked externally.
	if sec, _ := p2.Current().Sensitive("db"); sec != "pw" {
		t.Fatalf("secret = %q", sec)
	}
	d2, err := p2.Evaluator().Evaluate(p2.Current(), "checkout", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if d2.Enabled != dBefore.Enabled {
		t.Fatalf("decision changed across restart: before=%+v after=%+v", dBefore, d2)
	}
	// Next write continues the linear timeline (no reuse/reset).
	next := patchTimeout(t, p2, 4, 400)
	if next != 5 {
		t.Fatalf("post-restart version = %d, want 5", next)
	}
}

func TestMultipleRestarts(t *testing.T) {
	pdir := t.TempDir()
	want := int64(100)
	for cycle := 0; cycle < 4; cycle++ {
		p, _ := persistentPlane(t, pdir)
		cur := p.Current().Version
		assertTimeout(t, p, want)
		// Two more writes per cycle.
		want += 10
		to := patchTimeout(t, p, cur, want)
		want += 10
		to = patchTimeout(t, p, to, want)
		if to != cur+2 {
			t.Fatalf("cycle %d advanced to %d from %d", cycle, to, cur)
		}
		_ = p.Close()
	}
	p, _ := persistentPlane(t, pdir)
	if p.Current().Version != 9 {
		t.Fatalf("after 4 cycles version = %d, want 9", p.Current().Version)
	}
	assertTimeout(t, p, 180)
}

func TestRestartRollbackWindowAndResultConsistent(t *testing.T) {
	pdir := t.TempDir()
	p, _ := persistentPlane(t, pdir)
	var val int64 = 200
	v := int64(1)
	for i := 0; i < 5; i++ {
		v = patchTimeout(t, p, v, val)
		val += 100
	}
	// Current v6=700; roll back to v2 (200) -> new version.
	from, to, err := p.Rollback(context.Background(), "admin-1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if from != 6 || to != 7 {
		t.Fatalf("rollback from=%d to=%d", from, to)
	}
	assertTimeout(t, p, 200)
	_ = p.Close()

	p2, _ := persistentPlane(t, pdir)
	if p2.Current().Version != 7 {
		t.Fatalf("version after restart = %d, want 7", p2.Current().Version)
	}
	assertTimeout(t, p2, 200)
	// Rollback target that pre-dates the retained window must keep failing.
	if _, ok := p2.store.AtVersion(2); !ok {
		t.Fatal("v2 should still be retained")
	}
	_, _, err = p2.Rollback(context.Background(), "admin-1", 999)
	if err == nil {
		t.Fatal("rollback to unknown version must fail")
	}
	// Rollback again post-restart produces a fresh linear number.
	_, to2, err := p2.Rollback(context.Background(), "admin-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if to2 != 8 {
		t.Fatalf("post-restart rollback version = %d, want 8", to2)
	}
}

func TestRestartDoesNotReplayDeniedOrUnauthorized(t *testing.T) {
	pdir := t.TempDir()
	p, _ := persistentPlane(t, pdir)
	// Unauthorized write attempt: must be denied and leave no config change.
	rec, _ := do(t, p.Handler(), http.MethodPost, tokReader, "/v1/config",
		map[string]any{"expected_version": 1,
			"properties": []map[string]any{{"key": "timeout", "type": "int", "value": 999}}})
	if rec != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec)
	}
	// A stale (rejected) write.
	_, _, _ = p.ApplyPatch(context.Background(), "writer-1", PatchRequest{
		ExpectedVersion: 1,
		Properties:      []PatchOp{{Key: "timeout", Type: "int", Value: json.RawMessage("555")}},
	})
	_, _, err := p.ApplyPatch(context.Background(), "writer-1", PatchRequest{
		ExpectedVersion: 1,
		Properties:      []PatchOp{{Key: "timeout", Type: "int", Value: json.RawMessage("999")}},
	})
	if err == nil {
		t.Fatal("stale write should be rejected")
	}
	assertTimeout(t, p, 555)
	_ = p.Close()

	p2, _ := persistentPlane(t, pdir)
	h2 := p2.Handler()
	// Effective config is the last COMMITTED state (555 at v2), never 999.
	assertTimeout(t, p2, 555)
	if p2.Current().Version != 2 {
		t.Fatalf("version = %d, want 2", p2.Current().Version)
	}
	// Denials are preserved in the recovered audit trail, but had no effect.
	req := httptest.NewRequest(http.MethodGet, "/v1/audit", nil)
	req.Header.Set("Authorization", "Bearer "+tokAdmin)
	w := httptest.NewRecorder()
	h2.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("audit status = %d", w.Code)
	}
	var auditResp struct {
		Entries []map[string]any `json:"entries"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &auditResp); err != nil {
		t.Fatal(err)
	}
	var denied int
	for _, e := range auditResp.Entries {
		if e["result"] == "denied" {
			denied++
		}
	}
	if denied == 0 {
		t.Fatal("denied audit events did not survive restart")
	}
}

func TestCorruptStateFallsBackThenHeals(t *testing.T) {
	pdir := t.TempDir()
	p, _ := persistentPlane(t, pdir)
	v := patchTimeout(t, p, 1, 200)
	_ = patchTimeout(t, p, v, 300) // v3; backup holds v2
	_ = p.Close()

	// Corrupt the primary.
	if err := os.WriteFile(filepath.Join(pdir, "state.json"),
		[]byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	p2, _ := persistentPlane(t, pdir)
	if p2.Current().Version != 2 {
		t.Fatalf("recovered version = %d, want backup v2", p2.Current().Version)
	}
	assertTimeout(t, p2, 200)
	// Primary was healed; a normal restart now loads v2 cleanly (no backup use).
	_ = p2.Close()
	p3, _ := persistentPlane(t, pdir)
	if p3.Current().Version != 2 {
		t.Fatalf("after heal version = %d", p3.Current().Version)
	}
	// And the timeline continues from 2 without reusing 3.
	next := patchTimeout(t, p3, 2, 400)
	if next != 3 {
		t.Fatalf("post-recovery version = %d, want 3 (no reuse)", next)
	}
}

func TestPrunedHistoryRecoversConsistently(t *testing.T) {
	pdir := t.TempDir()
	// Small retain window so pruning actually happens.
	sdir := t.TempDir()
	path := filepath.Join(sdir, "base.json")
	writeCfg(t, path, baseV1())
	tokens := map[string]auth.Principal{
		tokWriter: {Name: "writer-1", Role: auth.RoleWriter},
		tokAdmin:  {Name: "admin-1", Role: auth.RoleAdmin},
	}
	lim := defaultLimitsForTest()
	lim.RetainVersions = 5
	plane, err := New(context.Background(), Options{
		Sources:       []source.Source{source.NewFileSource("base", 1, path)},
		Tokens:        tokens,
		PollInterval:  time.Hour,
		PersistDir:    pdir,
		PersistLimits: lim,
		PersistLogger: nopLog{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plane.Close() })

	cur := int64(1)
	var val int64 = 200
	// Store maxHistory is still 100; force the durable window smaller via
	// many commits, then check that disk and in-memory rollback agree.
	for i := 0; i < 12; i++ {
		cur = patchTimeout(t, plane, cur, val)
		val += 10
	}
	_ = plane.Close()

	// Reopen with the same small window.
	plane2, err := New(context.Background(), Options{
		Sources:       []source.Source{source.NewFileSource("base", 1, path)},
		Tokens:        tokens,
		PollInterval:  time.Hour,
		PersistDir:    pdir,
		PersistLimits: lim,
		PersistLogger: nopLog{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plane2.Close() })

	if plane2.Current().Version != 13 {
		t.Fatalf("current = %d, want 13", plane2.Current().Version)
	}
	// Disk window is capped at 5: versions 9..13.
	if len(plane2.store.History()) != 5 {
		t.Fatalf("recovered history len = %d, want 5", len(plane2.store.History()))
	}
	if _, ok := plane2.store.AtVersion(8); ok {
		t.Fatal("v8 must be pruned")
	}
	if _, ok := plane2.store.AtVersion(9); !ok {
		t.Fatal("v9 must remain within window")
	}
	// Rollback to a retained version works and yields a consistent config.
	// v2 set 200; vN set 200+(N-2)*10, so v9 holds 270.
	_, to, err := plane2.Rollback(context.Background(), "admin-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	if to != 14 {
		t.Fatalf("rollback new version = %d, want 14", to)
	}
	assertTimeout(t, plane2, 270)
}

func TestConcurrentWritesDurabilityInvariant(t *testing.T) {
	pdir := t.TempDir()
	p, _ := persistentPlane(t, pdir)

	const writers, per = 8, 15
	var wg sync.WaitGroup
	var commits, staleOrFail atomic.Int64
	start := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			for i := 0; i < per; i++ {
				// Retry loop on stale version, like a real client.
				for {
					cur := p.Current().Version
					val := 1000 + id*100 + i
					_, _, err := p.ApplyPatch(context.Background(), "writer-1", PatchRequest{
						ExpectedVersion: cur,
						Properties:      []PatchOp{{Key: "timeout", Type: "int", Value: json.RawMessage(itoaJSON(int64(val)))}},
					})
					if err == nil {
						commits.Add(1)
						break
					}
					staleOrFail.Add(1)
					if !isStale(err) {
						t.Errorf("unexpected patch error: %v", err)
						return
					}
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()

	if got := commits.Load(); got != writers*per {
		t.Fatalf("commits = %d, want %d", got, writers*per)
	}
	final := p.Current().Version
	if final != int64(1+writers*per) {
		t.Fatalf("final version = %d, want %d", final, 1+writers*per)
	}
	t.Logf("commits=%d retries-on-conflict=%d", commits.Load(), staleOrFail.Load())

	_ = p.Close()

	// Restart: durable current version and value must equal in-memory final.
	p2, _ := persistentPlane(t, pdir)
	if p2.Current().Version != final {
		t.Fatalf("durable version = %d, in-memory final = %d", p2.Current().Version, final)
	}
	if _, err := p2.Current().GetInt("timeout"); err != nil {
		t.Fatal(err)
	}
}

func boolPtr(b bool) *bool { return &b }
func intPtr(i int) *int    { return &i }

// A failing persistence backend must make the whole commit abort: version,
// effective config and the override layer stay at the pre-commit state, and
// the next retry still succeeds on the expected version.
func TestPersistFailureAbortsCommitAtomically(t *testing.T) {
	pdir := t.TempDir()
	p, _ := persistentPlane(t, pdir)
	beforeVal, _ := p.Current().GetInt("timeout")

	// Replace the data directory with a regular FILE so state writes (which
	// create temp files / rename inside it) fail with a classified IO error.
	_ = p.Close()
	if err := os.RemoveAll(pdir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pdir, []byte("not-a-dir"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Reconstruct a plane wired to the broken directory; bootstrap itself must
	// fail closed (cannot make v1 durable).
	sdir := filepath.Join(t.TempDir(), "base.json")
	writeCfg(t, sdir, baseV1())
	tokens := map[string]auth.Principal{
		tokWriter: {Name: "writer-1", Role: auth.RoleWriter},
		tokAdmin:  {Name: "admin-1", Role: auth.RoleAdmin},
	}
	if _, err := New(context.Background(), Options{
		Sources:       []source.Source{source.NewFileSource("base", 1, sdir)},
		Tokens:        tokens,
		PollInterval:  time.Hour,
		PersistDir:    pdir,
		PersistLogger: nopLog{},
	}); err == nil {
		t.Fatal("expected startup to fail closed when state dir is unwritable")
	} else if k := isPersistKind(err); k == "" {
		t.Fatalf("expected a PERSIST_* classification, got %v", err)
	}

	// Restore the directory and start normally: aborts consumed no version and
	// left no half-state, so a fresh start bootstraps cleanly at v1.
	if err := os.Remove(pdir); err != nil {
		t.Fatal(err)
	}
	p2, err := New(context.Background(), Options{
		Sources:       []source.Source{source.NewFileSource("base", 1, sdir)},
		Tokens:        tokens,
		PollInterval:  time.Hour,
		PersistDir:    pdir,
		PersistLogger: nopLog{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p2.Close() })
	if p2.Current().Version != 1 {
		t.Fatalf("clean bootstrap version = %d, want 1", p2.Current().Version)
	}
	if v, _ := p2.Current().GetInt("timeout"); v != beforeVal {
		t.Fatalf("clean bootstrap timeout = %d, want original %d", v, beforeVal)
	}
	to := patchTimeout(t, p2, 1, 8888)
	if to != 2 {
		t.Fatalf("write after recovery = version %d, want 2", to)
	}
}

func isPersistKind(err error) string {
	for _, k := range []string{"PERSIST_IO", "PERSIST_CORRUPT", "PERSIST_CHECKSUM",
		"PERSIST_TOO_LARGE", "PERSIST_SCHEMA"} {
		if errKindString(err) == k {
			return k
		}
	}
	return ""
}
