package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/runtime-config-control-plane-go/internal/auth"
	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/merge"
	"github.com/lisama569/runtime-config-control-plane-go/internal/source"
)

// intPatch builds one optimistic write that upserts a single int property.
func intPatch(expected int64, key string, val int64) PatchRequest {
	return PatchRequest{
		ExpectedVersion: expected,
		Properties:      []PatchOp{{Key: key, Type: "int", Value: json.RawMessage(itoaJSON(val))}},
	}
}

// remergeWithOverride rebuilds the effective document from the current file
// layers and a GIVEN override state, using the real merger. It must be called
// with p.mu held.
func remergeWithOverride(p *Plane, ov overrideState) *config.Document {
	ordered := make([]merge.Layered, 0, len(p.sources)+1)
	overDoc := ov.emptyDoc()
	if len(overDoc.Properties) > 0 || len(overDoc.Features) > 0 {
		ordered = append(ordered, merge.Layered{Name: "api-override", Version: 0, Doc: overDoc})
	}
	for _, src := range p.sources {
		if sd, ok := p.layers[src.Name()]; ok {
			ordered = append(ordered, merge.Layered{Name: src.Name(), Version: sd.ver, Doc: sd.doc})
		}
	}
	res, err := merge.Layers(ordered)
	if err != nil {
		panic(err)
	}
	for k := range ov.deadProps {
		delete(res.Doc.Properties, k)
	}
	for k := range ov.deadFeats {
		delete(res.Doc.Features, k)
	}
	return res.Doc
}

// assertHistoryCovered verifies the committed override timeline covers exactly
// the store's retained versions (so every rollback target is reconstructable).
func assertHistoryCovered(t *testing.T, p *Plane) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, v := range p.store.History() {
		if _, ok := p.overHist[v]; !ok {
			t.Fatalf("committed version %d has no override snapshot", v)
		}
	}
	for v := range p.overHist {
		found := false
		for _, hv := range p.store.History() {
			if hv == v {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("override history has orphan version %d", v)
		}
	}
}

// assertCurrentRemerges verifies the live snapshot is reproducible from the
// live override layer + current sources: what is published equals what the
// committed override produces, so disk/restore (which keep the override) can
// never diverge from the served config.
func assertCurrentRemerges(t *testing.T, p *Plane) {
	t.Helper()
	// Everything that reads p.layers/p.over must happen under p.mu; otherwise
	// the checker itself races the plane's serialized transitions.
	p.mu.Lock()
	defer p.mu.Unlock()
	ov := p.over.clone()
	snap := p.store.Current()
	got := remergeWithOverride(p, ov)
	if msg, ok := docDiff(got, snap.Doc); !ok {
		t.Fatalf("live v%d does not remerge from its committed override: %s", snap.Version, msg)
	}
}

// docDiff returns a human-readable description of the first mismatch.
func docDiff(a, b *config.Document) (string, bool) {
	ka, kb := mapKeys(a.Properties), mapKeys(b.Properties)
	if miss := kb.Difference(ka); len(miss) > 0 {
		return "remerge is missing keys " + miss.String(), false
	}
	if extra := ka.Difference(kb); len(extra) > 0 {
		return "remerge has extra keys " + extra.String(), false
	}
	for k := range ka {
		va, vb := a.Properties[k], b.Properties[k]
		if va.Value.Kind != vb.Value.Kind {
			return fmt.Sprintf("key %s kind %s != %s", k, va.Value.Kind, vb.Value.Kind), false
		}
		if va.Value.Kind == config.KindInt && va.Value.Int != vb.Value.Int {
			return fmt.Sprintf("key %s value %d != %d", k, va.Value.Int, vb.Value.Int), false
		}
	}
	fa := stringSet{}
	for k := range a.Features {
		fa[k] = struct{}{}
	}
	fb := stringSet{}
	for k := range b.Features {
		fb[k] = struct{}{}
	}
	if miss := fb.Difference(fa); len(miss) > 0 {
		return "remerge missing features " + miss.String(), false
	}
	if extra := fa.Difference(fb); len(extra) > 0 {
		return "remerge extra features " + extra.String(), false
	}
	return "", true
}

type stringSet map[string]struct{}

func mapKeys[V any](m map[string]V) stringSet {
	s := stringSet{}
	for k := range m {
		s[k] = struct{}{}
	}
	return s
}

func (s stringSet) Difference(o stringSet) stringSet {
	out := stringSet{}
	for k := range s {
		if _, ok := o[k]; !ok {
			out[k] = struct{}{}
		}
	}
	return out
}

func (s stringSet) String() string {
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	sort.Strings(out)
	return fmt.Sprint(out)
}

// assertEveryVersionRemerges is the stronger check usable while file sources
// have not moved past the retained window: each retained snapshot must equal a
// fresh merge of its recorded override against the current sources.
func assertEveryVersionRemerges(t *testing.T, p *Plane) {
	t.Helper()
	assertHistoryCovered(t, p)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, v := range p.store.History() {
		snap, ok := p.store.AtVersion(v)
		if !ok {
			t.Fatalf("version %d not retained", v)
		}
		ov := p.overHist[v].clone()
		got := remergeWithOverride(p, ov)
		if msg, match := docDiff(got, snap.Doc); !match {
			t.Fatalf("version %d rollback config mismatches the snapshot that was live: %s", v, msg)
		}
	}
}

// historyPropKeys returns property keys that appear in ANY retained snapshot.
func historyPropKeys(t *testing.T, p *Plane) map[string]bool {
	t.Helper()
	keys := map[string]bool{}
	for _, v := range p.store.History() {
		snap, ok := p.store.AtVersion(v)
		if !ok {
			t.Fatalf("version %d missing", v)
		}
		for k := range snap.Doc.Properties {
			keys[k] = true
		}
	}
	return keys
}

func keysWithPrefix(keys map[string]bool, prefix string) []string {
	var out []string
	for k := range keys {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k)
		}
	}
	return out
}

// TestSameVersionConcurrentWritesExactlyOneWins reproduces the reported
// corruption: many writers submit the SAME expected version with DISTINCT
// keys. Exactly one commit may take effect; every other request must fail as
// STALE_VERSION and leave zero trace in live config, retained history, the
// durable file and the restarted plane.
func TestSameVersionConcurrentWritesExactlyOneWins(t *testing.T) {
	pdir := t.TempDir()
	p, path := persistentPlane(t, pdir)

	const racers = 6
	var ok, stale atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			key := fmt.Sprintf("racer-%d", id)
			_, _, err := p.ApplyPatch(context.Background(), "writer-1",
				intPatch(1, key, int64(1000+id)))
			if err == nil {
				ok.Add(1)
				return
			}
			if isStale(err) {
				stale.Add(1)
				return
			}
			t.Errorf("racer %d unexpected error: %v", id, err)
		}(i)
	}
	close(start)
	wg.Wait()

	if ok.Load() != 1 {
		t.Fatalf("successful commits = %d, want exactly 1", ok.Load())
	}
	if stale.Load() != racers-1 {
		t.Fatalf("stale rejections = %d, want %d", stale.Load(), racers-1)
	}
	if final := p.Current().Version; final != 2 {
		t.Fatalf("final version = %d, want 2", final)
	}
	assertEveryVersionRemerges(t, p)

	// Exactly one racer key may exist anywhere in the live config or history.
	racerKeys := keysWithPrefix(historyPropKeys(t, p), "racer-")
	if len(racerKeys) != 1 {
		t.Fatalf("racer keys left in timeline = %v, want exactly one", racerKeys)
	}
	liveVal, err := p.Current().GetInt(racerKeys[0])
	if err != nil {
		t.Fatal(err)
	}

	// Close, then recover: durable state must equal the in-memory final state.
	_ = p.Close()
	p2, _ := persistentPlaneWithSource(t, pdir, path)
	if p2.Current().Version != 2 {
		t.Fatalf("restart version = %d, want 2", p2.Current().Version)
	}
	racerKeys2 := keysWithPrefix(historyPropKeys(t, p2), "racer-")
	if len(racerKeys2) != 1 || racerKeys2[0] != racerKeys[0] {
		t.Fatalf("after restart racer keys = %v, want [%s]", racerKeys2, racerKeys[0])
	}
	if v, _ := p2.Current().GetInt(racerKeys2[0]); v != liveVal {
		t.Fatalf("after restart value = %d, want %d", v, liveVal)
	}
	assertEveryVersionRemerges(t, p2)
	// Continuing writes stay linear, no reuse/regression.
	if to := patchTimeout(t, p2, 2, 4242); to != 3 {
		t.Fatalf("post-restart write version = %d, want 3", to)
	}
}

// fireFileReload applies a hot-reload event for the named source through the
// same serialized entry point the watcher goroutine uses. The current file
// content is read inside the transition, so no (doc, version) pair is built
// outside the lock.
func fireFileReload(t *testing.T, p *Plane, name string) {
	t.Helper()
	p.mu.Lock()
	var fs source.Source
	for _, s := range p.sources {
		if s.Name() == name {
			fs = s
		}
	}
	p.mu.Unlock()
	if fs == nil {
		t.Fatalf("source %q not found", name)
	}
	p.applySourceEvent(context.Background(), fs)
}

// reloadJSON writes a valid source document changing only the file-owned key
// "fkey". It does NOT touch the file mtime: the test delivers reloads
// deterministically through fireFileReload, so the background watcher (a
// 1-hour poll interval here) cannot race the test with a second reload path.
func reloadJSON(t *testing.T, path string, v int64, val int64) {
	t.Helper()
	content := fmt.Sprintf(
		`{"version":%d,"properties":{"timeout":{"type":"int","value":100},"db":{"type":"sensitive","value":"pw"},"fkey":{"type":"int","value":%d}},"features":{"checkout":{"enabled":true,"percentage":50}}}`,
		v, val)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestConcurrentWritesAndHotReloadNeverInterleave hammers the plane with API
// writes while file reloads land concurrently. At every committed boundary:
//   - live config must remerge identically from its recorded override layer;
//   - versions are strictly linear (1..end, no gap/repeat);
//   - the durable file and a fresh restart equal the in-memory final state.
func TestConcurrentWritesAndHotReloadNeverInterleave(t *testing.T) {
	pdir := t.TempDir()
	p, path := persistentPlane(t, pdir)

	const rounds = 25
	var wg sync.WaitGroup
	start := make(chan struct{})
	var violations atomic.Int64

	// API writers: per round three writers race from the same observed base;
	// losers retry on stale like real clients until each lands one commit.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for r := 0; r < rounds; r++ {
			var iwg sync.WaitGroup
			for w := 0; w < 3; w++ {
				iwg.Add(1)
				go func(r, w int) {
					defer iwg.Done()
					for attempt := 0; ; attempt++ {
						cur := p.Current().Version
						key := fmt.Sprintf("api-%d-%d", r, w)
						_, _, err := p.ApplyPatch(context.Background(), "writer-1",
							intPatch(cur, key, int64(r*10+w)))
						if err == nil {
							return
						}
						if !isStale(err) {
							violations.Add(1)
							t.Errorf("writer r=%d w=%d: %v", r, w, err)
							return
						}
						if attempt > 5000 {
							violations.Add(1)
							return
						}
					}
				}(r, w)
			}
			iwg.Wait()
		}
	}()

	// Hot reloads: rewrite the file-owned key and apply the event directly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for r := 0; r < rounds; r++ {
			reloadJSON(t, path, int64(2+r), int64(5000+r))
			fireFileReload(t, p, "base")
		}
	}()

	close(start)
	wg.Wait()

	if violations.Load() != 0 {
		t.Fatalf("concurrent phase had %d violations", violations.Load())
	}
	// Every round produced exactly 3 API commits + 1 reload commit from v1.
	wantVer := int64(1 + 4*rounds)
	if got := p.Current().Version; got != wantVer {
		t.Fatalf("final version = %d, want %d", got, wantVer)
	}
	p.mu.Lock()
	if sd, ok := p.layers["base"]; !ok {
		t.Fatal("base layer missing after concurrent phase")
	} else if _, has := sd.doc.Properties["fkey"]; !has {
		t.Fatalf("base layer (source ver %d) lost fkey after concurrent phase", sd.ver)
	}
	p.mu.Unlock()
	hv := p.store.History()
	for i := 1; i < len(hv); i++ {
		if hv[i] != hv[i-1]+1 {
			t.Fatalf("history not linear around slot %d: %d -> %d (history=%v)",
				i, hv[i-1], hv[i], hv)
		}
	}
	if hv[len(hv)-1] != p.Current().Version {
		t.Fatalf("history newest %d != live %d", hv[len(hv)-1], p.Current().Version)
	}
	assertHistoryCovered(t, p)
	assertCurrentRemerges(t, p)

	lastFile, err := p.Current().GetInt("fkey")
	if err != nil {
		t.Fatal(err)
	}
	if lastFile != int64(5000+rounds-1) {
		t.Fatalf("fkey = %d, want %d", lastFile, 5000+rounds-1)
	}
	for r := 0; r < rounds; r++ {
		for w := 0; w < 3; w++ {
			key := fmt.Sprintf("api-%d-%d", r, w)
			if _, err := p.Current().GetInt(key); err != nil {
				t.Fatalf("api key %s missing after concurrent phase: %v", key, err)
			}
		}
	}

	memVersion := p.Current().Version
	_ = p.Close()
	// Restart into the SAME data directory AND the same source file, exactly
	// like a process restart on one host: the durable snapshot, the recovered
	// override timeline and the file content must all agree.
	p2, _ := persistentPlaneWithSource(t, pdir, path)
	if p2.Current().Version != memVersion {
		t.Fatalf("restart version = %d, memory = %d", p2.Current().Version, memVersion)
	}
	if v, _ := p2.Current().GetInt("fkey"); v != lastFile {
		t.Fatalf("restart fkey = %d, want %d", v, lastFile)
	}
	for r := 0; r < rounds; r++ {
		for w := 0; w < 3; w++ {
			key := fmt.Sprintf("api-%d-%d", r, w)
			if _, err := p2.Current().GetInt(key); err != nil {
				t.Fatalf("api key %s missing after restart: %v", key, err)
			}
		}
	}
	assertHistoryCovered(t, p2)
	assertCurrentRemerges(t, p2)
}

// TestFailedWriteNeverRollsBackCommittedState guarantees a later losing
// request cannot regress an already-successful commit, and rollback snapshots
// stay identical to what was once live.
func TestFailedWriteNeverRollsBackCommittedState(t *testing.T) {
	pdir := t.TempDir()
	p, path := persistentPlane(t, pdir)

	if to := patchTimeout(t, p, 1, 111); to != 2 {
		t.Fatalf("to=%d", to)
	}
	snapAt2, _ := p.store.AtVersion(2)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _, _ = p.ApplyPatch(context.Background(), "writer-1",
				intPatch(1, fmt.Sprintf("loser-%d", i), int64(i)))
		}(i)
	}
	close(start)
	wg.Wait()

	if p.Current().Version != 2 {
		t.Fatalf("version = %d, want 2 (losers must not commit)", p.Current().Version)
	}
	if v, _ := p.Current().GetInt("timeout"); v != 111 {
		t.Fatalf("committed value regressed to %d, want 111", v)
	}
	s2, ok := p.store.AtVersion(2)
	if !ok || s2 != snapAt2 {
		t.Fatal("v2 snapshot was mutated by losing requests")
	}
	assertEveryVersionRemerges(t, p)

	from, to, err := p.Rollback(context.Background(), "admin-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if from != 2 || to != 3 {
		t.Fatalf("rollback from=%d to=%d", from, to)
	}
	if v, _ := p.Current().GetInt("timeout"); v != 100 {
		t.Fatalf("post-rollback timeout = %d, want 100", v)
	}
	assertEveryVersionRemerges(t, p)

	_ = p.Close()
	p2, _ := persistentPlaneWithSource(t, pdir, path)
	if p2.Current().Version != 3 {
		t.Fatalf("restart version = %d, want 3", p2.Current().Version)
	}
	if v, _ := p2.Current().GetInt("timeout"); v != 100 {
		t.Fatalf("restart timeout = %d, want 100", v)
	}
	assertEveryVersionRemerges(t, p2)
}

// persistentPlanePoll builds a persistent plane with a short watcher poll so
// the real file-watching goroutine drives hot reloads (not a direct event).
func persistentPlanePoll(t *testing.T, pdir, srcPath string, poll time.Duration) *Plane {
	t.Helper()
	tokens := map[string]auth.Principal{
		tokWriter: {Name: "writer-1", Role: auth.RoleWriter},
		tokAdmin:  {Name: "admin-1", Role: auth.RoleAdmin},
	}
	plane, err := New(context.Background(), Options{
		Sources:       []source.Source{source.NewFileSource("base", 1, srcPath)},
		Tokens:        tokens,
		PollInterval:  poll,
		PersistDir:    pdir,
		PersistLogger: nopLog{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plane.Close() })
	return plane
}

// writeFileAndBumpMtime rewrites the source file with a new file-owned key and
// bumps its mtime so the real watcher notices it on the next poll.
func writeFileAndBumpMtime(t *testing.T, path string, v, val int64) {
	t.Helper()
	reloadJSON(t, path, v, val)
	future := time.Now().Add(time.Second)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
}

// TestRealWatcherConcurrentWithWrites exercises the production path: a
// short-interval file watcher picks up rewritten files while concurrent API
// writers commit. It only asserts the invariants (linear versions, coherent
// override history, live==durable after restart, race-clean); timing counts
// are not asserted because the watcher polls asynchronously.
func TestRealWatcherConcurrentWithWrites(t *testing.T) {
	pdir := t.TempDir()
	srcPath := filepath.Join(t.TempDir(), "base.json")
	writeCfg(t, srcPath, baseV1())
	p := persistentPlanePoll(t, pdir, srcPath, 10*time.Millisecond)

	const rounds = 12
	var wg sync.WaitGroup
	start := make(chan struct{})
	var violations atomic.Int64

	// API writers hammer distinct keys with stale-retry.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for r := 0; r < rounds; r++ {
			var iwg sync.WaitGroup
			for w := 0; w < 2; w++ {
				iwg.Add(1)
				go func(r, w int) {
					defer iwg.Done()
					for attempt := 0; ; attempt++ {
						cur := p.Current().Version
						key := fmt.Sprintf("w-%d-%d", r, w)
						_, _, err := p.ApplyPatch(context.Background(), "writer-1",
							intPatch(cur, key, int64(r*10+w)))
						if err == nil {
							return
						}
						if !isStale(err) {
							violations.Add(1)
							return
						}
						if attempt > 5000 {
							violations.Add(1)
							return
						}
					}
				}(r, w)
			}
			iwg.Wait()
		}
	}()

	// External editor rewrites the file-owned key repeatedly; the real watcher
	// must integrate each change without interleaving with the writers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for r := 0; r < rounds; r++ {
			writeFileAndBumpMtime(t, srcPath, int64(2+r), int64(7000+r))
			time.Sleep(15 * time.Millisecond)
		}
	}()

	close(start)
	wg.Wait()
	if violations.Load() != 0 {
		t.Fatalf("concurrent phase violations=%d", violations.Load())
	}

	// Versions are strictly linear across the retained window.
	hv := p.store.History()
	for i := 1; i < len(hv); i++ {
		if hv[i] != hv[i-1]+1 {
			t.Fatalf("non-linear history around %d: %v", i, hv)
		}
	}
	assertHistoryCovered(t, p)
	assertCurrentRemerges(t, p)

	// Every API key committed survives; the file key reflects some reload.
	for r := 0; r < rounds; r++ {
		for w := 0; w < 2; w++ {
			if _, err := p.Current().GetInt(fmt.Sprintf("w-%d-%d", r, w)); err != nil {
				t.Fatalf("writer key r=%d w=%d lost: %v", r, w, err)
			}
		}
	}
	if _, err := p.Current().GetInt("fkey"); err != nil {
		t.Fatalf("file key never integrated by watcher: %v", err)
	}

	// Wait for the last written file content to be observed, then restart and
	// require durable == live.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := p.Current().GetInt("fkey"); v == int64(7000+rounds-1) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	wantFile, _ := p.Current().GetInt("fkey")
	memVersion := p.Current().Version
	_ = p.Close()

	p2 := persistentPlanePoll(t, pdir, srcPath, time.Hour)
	if p2.Current().Version != memVersion {
		t.Fatalf("restart version=%d want %d", p2.Current().Version, memVersion)
	}
	if v, _ := p2.Current().GetInt("fkey"); v != wantFile {
		t.Fatalf("restart fkey=%d want %d", v, wantFile)
	}
	assertHistoryCovered(t, p2)
	assertCurrentRemerges(t, p2)
}

// TestHistoryWindowsStayAlignedAcrossPruning drives commits past the 100-entry
// retention boundary (mixing writes, reloads and a rollback) and verifies the
// retained snapshot versions and the retained override versions are the exact
// same set both in memory and after restart: no orphan override (e.g. a v1 kept
// forever) and no rollback target whose snapshot disagrees with what was live.
func TestHistoryWindowsStayAlignedAcrossPruning(t *testing.T) {
	pdir := t.TempDir()
	p, path := persistentPlane(t, pdir)

	// Well over 100 commits total, interleaving writes and hot reloads.
	for i := 0; i < 100; i++ {
		cur := p.Current().Version
		if _, _, err := p.ApplyPatch(context.Background(), "writer-1",
			intPatch(cur, fmt.Sprintf("k%02d", i%7), int64(i))); err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			reloadJSON(t, path, int64(2+i), int64(8000+i))
			fireFileReload(t, p, "base")
		}
	}
	// Rollback somewhere inside the retained window, then keep writing.
	hv := p.store.History()
	if len(hv) == 0 {
		t.Fatal("empty history")
	}
	target := hv[len(hv)/2]
	if _, _, err := p.Rollback(context.Background(), "admin-1", target); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		cur := p.Current().Version
		if _, _, err := p.ApplyPatch(context.Background(), "writer-1",
			intPatch(cur, "tail", int64(i))); err != nil {
			t.Fatal(err)
		}
	}

	snapVers := map[int64]bool{}
	for _, v := range p.store.History() {
		snapVers[v] = true
	}
	overVers := map[int64]bool{}
	p.mu.Lock()
	for v := range p.overHist {
		overVers[v] = true
	}
	p.mu.Unlock()
	if len(snapVers) != len(overVers) {
		t.Fatalf("window sizes differ: snapshots=%d overrides=%d", len(snapVers), len(overVers))
	}
	for v := range snapVers {
		if !overVers[v] {
			t.Fatalf("snapshot version %d missing override history", v)
		}
	}
	for v := range overVers {
		if !snapVers[v] {
			t.Fatalf("orphan override version %d with no retained snapshot", v)
		}
	}
	// The oldest retained versions must be > 1 (pruning really happened).
	if hv[0] <= 1 {
		t.Fatalf("expected pruning past v1, oldest retained=%d", hv[0])
	}
	assertHistoryCovered(t, p)
	assertCurrentRemerges(t, p)

	memVersion := p.Current().Version
	_ = p.Close()
	p2, _ := persistentPlaneWithSource(t, pdir, path)
	if p2.Current().Version != memVersion {
		t.Fatalf("restart version=%d want %d", p2.Current().Version, memVersion)
	}
	snapVers2 := map[int64]bool{}
	for _, v := range p2.store.History() {
		snapVers2[v] = true
	}
	if len(snapVers2) != len(snapVers) {
		t.Fatalf("restart window=%d want %d", len(snapVers2), len(snapVers))
	}
	for v := range snapVers {
		if !snapVers2[v] {
			t.Fatalf("restart dropped retained version %d", v)
		}
	}
	assertHistoryCovered(t, p2)
	assertCurrentRemerges(t, p2)
}
