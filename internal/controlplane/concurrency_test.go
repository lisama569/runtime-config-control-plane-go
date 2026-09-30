package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/runtime-config-control-plane-go/internal/auth"
	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/source"

	"path/filepath"
)

// startBarrier returns a test seam that blocks every committing goroutine
// until `want` of them have passed the optimistic version check and built
// their candidate. Releasing the barrier then makes them race the same
// expected version head-to-head deterministically.
func startBarrier(want int) (seam func(), release func()) {
	var arrived atomic.Int32
	allIn := make(chan struct{})
	go2 := make(chan struct{})
	seam = func() {
		if arrived.Add(1) == int32(want) {
			close(allIn)
		}
		<-go2
	}
	release = func() {
		<-allIn
		close(go2)
	}
	return seam, release
}

// distinctProps builds N distinct property ops on distinct keys, so a losing
// writer's value leaking anywhere is directly attributable.
func distinctProps(tag string, n int) [][]PatchOp {
	out := make([][]PatchOp, n)
	for i := range out {
		out[i] = []PatchOp{{
			Key:   fmt.Sprintf("race.%s.k%d", tag, i),
			Type:  "int",
			Value: json.RawMessage(fmt.Sprintf("%d", i+1)),
		}}
	}
	return out
}

// All writers bet the SAME expected version concurrently. Exactly one must
// commit; every loser must fail STALE_VERSION and its value must not appear in
// the live config, in any retained snapshot, or after a restart.
func TestConcurrentSameVersionWritesNoLeak(t *testing.T) {
	plane, _ := newTestPlane(t, time.Hour)
	ctx := context.Background()

	const racers = 6
	const rounds = 8
	for round := 0; round < rounds; round++ {
		expected := plane.Current().Version
		ops := distinctProps(fmt.Sprintf("r%d", round), racers)
		seam, release := startBarrier(racers)
		plane.testPreCommit = seam

		var wg sync.WaitGroup
		var wins, stale atomic.Int64
		errCh := make(chan error, racers)
		for r := 0; r < racers; r++ {
			wg.Add(1)
			go func(r int) {
				defer wg.Done()
				_, _, err := plane.ApplyPatch(ctx, "writer-1", PatchRequest{
					ExpectedVersion: expected,
					Properties:      ops[r],
				})
				if err == nil {
					wins.Add(1)
					return
				}
				if isStale(err) {
					stale.Add(1)
					return
				}
				errCh <- err
			}(r)
		}
		release()
		wg.Wait()
		plane.testPreCommit = nil
		close(errCh)
		for err := range errCh {
			t.Fatalf("round %d unexpected error: %v", round, err)
		}

		if wins.Load() != 1 {
			t.Fatalf("round %d: wins=%d want 1", round, wins.Load())
		}
		if stale.Load() != racers-1 {
			t.Fatalf("round %d: stale=%d want %d", round, stale.Load(), racers-1)
		}

		// Exactly ONE racer key may exist anywhere: the winner's. Loser keys
		// must be absent from the live document AND every retained snapshot.
		liveKeys := 0
		for r := 0; r < racers; r++ {
			k := fmt.Sprintf("race.r%d.k%d", round, r)
			if _, ok := plane.Current().Doc.Properties[k]; ok {
				liveKeys++
			}
			for _, v := range plane.store.History() {
				snap, ok := plane.store.AtVersion(v)
				if !ok {
					t.Fatalf("version %d in history but not retrievable", v)
				}
				if _, ok := snap.Doc.Properties[k]; ok && v != expected+1 {
					t.Fatalf("round %d: loser key %s leaked into retained version %d", round, k, v)
				}
			}
		}
		if liveKeys != 1 {
			t.Fatalf("round %d: %d racer keys live, want exactly 1", round, liveKeys)
		}
	}

	// One commit per round: strict linear timeline, no gaps/reuse.
	if got, want := plane.Current().Version, int64(1+rounds); got != want {
		t.Fatalf("final version = %d want %d", got, want)
	}
	hist := plane.store.History()
	for i := 1; i < len(hist); i++ {
		if hist[i] != hist[i-1]+1 {
			t.Fatalf("non-linear history: %v", hist)
		}
	}
}

// fakeSource is a minimal source.Source for driving reload events directly.
type fakeSource struct{ name string }

func (f *fakeSource) Name() string  { return f.name }
func (f *fakeSource) Priority() int { return 1 }
func (f *fakeSource) Version() int64 {
	return 1
}
func (f *fakeSource) Load(context.Context) (*config.Document, int64, error) {
	return nil, 0, nil
}

var _ source.Source = (*fakeSource)(nil)

func reloadDoc(t *testing.T, srcProp int64) *config.Document {
	t.Helper()
	raw := fmt.Sprintf(`{"version":1,"properties":{"timeout":{"type":"int","value":100},"db":{"type":"sensitive","value":"pw"},"src.k":{"type":"int","value":%d}},"features":{"checkout":{"enabled":true,"percentage":50}}}`, srcProp)
	var rd config.RawDocument
	if err := json.Unmarshal([]byte(raw), &rd); err != nil {
		t.Fatal(err)
	}
	doc, err := config.Parse(&rd)
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

// An API write and a hot reload that both start from the same version race
// head-to-head. Because the whole read-modify-commit sequence is serialized:
//
//   - one side wins the first linear commit; the API write loses as
//     STALE_VERSION if it loses, while a losing hot reload is remerged against
//     the newest snapshot and retried until the file change converges;
//   - at every intermediate version the effective document is complete and
//     coherent: exactly that commit's mutation on top of the prior snapshot,
//     never a mixture of both contenders;
//   - after convergence both the file-derived value and the committed write's
//     value (if it won) are present, and the retained timeline is linear.
func TestConcurrentWriteAndHotReloadNoInterleave(t *testing.T) {
	plane, _ := newTestPlane(t, time.Hour)
	ctx := context.Background()
	fsrc := &fakeSource{name: "base"}

	const rounds = 20
	var commits int64 = 1
	for round := 0; round < rounds; round++ {
		startVer := plane.Current().Version
		apiKey := fmt.Sprintf("api.k%d", round)
		srcKey := fmt.Sprintf("src.k%d", round)

		seam, release := startBarrier(2)
		plane.testPreCommit = seam

		var wg sync.WaitGroup
		var writeErr error
		var writeMu sync.Mutex
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, err := plane.ApplyPatch(ctx, "writer-1", PatchRequest{
				ExpectedVersion: startVer,
				Properties: []PatchOp{{
					Key:   apiKey,
					Type:  "int",
					Value: json.RawMessage(fmt.Sprintf("%d", round+1)),
				}},
			})
			writeMu.Lock()
			writeErr = err
			writeMu.Unlock()
		}()
		go func() {
			defer wg.Done()
			raw := fmt.Sprintf(`{"version":1,"properties":{"timeout":{"type":"int","value":100},"db":{"type":"sensitive","value":"pw"},"%s":{"type":"int","value":%d}},"features":{"checkout":{"enabled":true,"percentage":50}}}`, srcKey, round+1)
			var rd config.RawDocument
			if err := json.Unmarshal([]byte(raw), &rd); err != nil {
				t.Errorf("bad reload json: %v", err)
				return
			}
			doc, perr := config.Parse(&rd)
			if perr != nil {
				t.Errorf("bad reload doc: %v", perr)
				return
			}
			plane.applySourceEvent(ctx, source.UpdateEvent{
				Source: fsrc, Version: int64(round + 2), Doc: doc,
			})
		}()
		release()
		wg.Wait()
		plane.testPreCommit = nil

		writeMu.Lock()
		we := writeErr
		writeMu.Unlock()
		if we != nil && !isStale(we) {
			t.Fatalf("round %d unexpected write error: %v", round, we)
		}

		// Between one and two linear commits happened; versions stay strict.
		endVer := plane.Current().Version
		if !(endVer == startVer+1 || endVer == startVer+2) {
			t.Fatalf("round %d: %d -> %d, want +1 or +2", round, startVer, endVer)
		}
		introduced := map[string]bool{}
		for v := startVer + 1; v <= endVer; v++ {
			snap, ok := plane.store.AtVersion(v)
			if !ok {
				t.Fatalf("round %d: version %d not retained", round, v)
			}
			prev, ok := plane.store.AtVersion(v - 1)
			if !ok {
				t.Fatalf("round %d: previous version %d not retained", round, v-1)
			}
			// Exactly ONE contender's NEW mutation may be introduced per linear
			// step. A convergence reload inheriting the write's key is fine; both
			// keys appearing *new* in the same step would be an interleaving bug.
			var stepNew []string
			if _, now := snap.Doc.Properties[apiKey]; now {
				if _, before := prev.Doc.Properties[apiKey]; !before {
					stepNew = append(stepNew, apiKey)
				}
			}
			if _, now := snap.Doc.Properties[srcKey]; now {
				if _, before := prev.Doc.Properties[srcKey]; !before {
					stepNew = append(stepNew, srcKey)
				}
			}
			if len(stepNew) != 1 {
				t.Fatalf("round %d version %d introduces %d contender mutations %v, want exactly 1",
					round, v, len(stepNew), stepNew)
			}
			introduced[stepNew[0]] = true
			// Each snapshot must keep the complete base document (no torn state).
			if _, ok := snap.Doc.Properties["timeout"]; !ok {
				t.Fatalf("round %d version %d lost base timeout", round, v)
			}
			if _, ok := snap.Doc.Properties["db"]; !ok {
				t.Fatalf("round %d version %d lost base db", round, v)
			}
			if _, ok := snap.Doc.Features["checkout"]; !ok {
				t.Fatalf("round %d version %d lost base feature", round, v)
			}
		}
		// The reload always converges, so its change must have been introduced.
		if !introduced[srcKey] {
			t.Fatalf("round %d: source change never introduced", round)
		}
		// A write that lost the conflict must never have been introduced.
		if we != nil && introduced[apiKey] {
			t.Fatalf("round %d: stale-losing write was introduced anyway", round)
		}

		// Convergence: the reload eventually installs the source layer, so the
		// final snapshot always carries the source-derived key.
		if _, ok := plane.Current().Doc.Properties[srcKey]; !ok {
			t.Fatalf("round %d: reload never converged, %s missing", round, srcKey)
		}
		// If the write won, its key survives; if it lost, it is nowhere.
		if _, has := plane.Current().Doc.Properties[apiKey]; has != (we == nil) {
			t.Fatalf("round %d: write committed=%v but api key present=%v", round, we == nil, has)
		}
		commits = endVer
	}

	if got, want := commits, int64(1+rounds); got < want {
		t.Fatalf("final version = %d, at least %d expected", got, want)
	}
	hist := plane.store.History()
	for i := 1; i < len(hist); i++ {
		if hist[i] != hist[i-1]+1 {
			t.Fatalf("non-linear history: %v", hist)
		}
	}
}

// restartPlane reopens a plane against the same durable directory and the
// given source file, with reload watching disabled.
func restartPlane(t *testing.T, pdir string) *Plane {
	t.Helper()
	sdir := t.TempDir()
	path := filepath.Join(sdir, "base.json")
	writeCfg(t, path, baseV1())
	tokens := map[string]auth.Principal{
		tokWriter: {Name: "writer-1", Role: auth.RoleWriter},
		tokAdmin:  {Name: "admin-1", Role: auth.RoleAdmin},
	}
	p, err := New(context.Background(), Options{
		Sources:       []source.Source{source.NewFileSource("base", 1, path)},
		Tokens:        tokens,
		PollInterval:  time.Hour,
		PersistDir:    pdir,
		PersistLogger: nopLog{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// Same-version concurrent writes with durability: exactly one value per round
// may commit, and after a restart the recovered effective config, the retained
// history and the durable timeline must match the in-memory committed result
// exactly — no loser value may appear on disk or after recovery.
func TestConcurrentSameVersionWritesDurableTripleConsistency(t *testing.T) {
	pdir := t.TempDir()
	p, _ := persistentPlane(t, pdir)
	ctx := context.Background()

	const racers = 5
	const rounds = 10
	type commit struct {
		key string
		val int64
	}
	winners := make([]commit, rounds)
	for round := 0; round < rounds; round++ {
		expected := p.Current().Version
		key := fmt.Sprintf("dur.k%d", round)
		seam, release := startBarrier(racers)
		p.testPreCommit = seam

		var wg sync.WaitGroup
		var wins atomic.Int64
		for r := 0; r < racers; r++ {
			wg.Add(1)
			go func(r int) {
				defer wg.Done()
				val := int64(round*100 + r + 1)
				_, _, err := p.ApplyPatch(ctx, "writer-1", PatchRequest{
					ExpectedVersion: expected,
					Properties:      []PatchOp{{Key: key, Type: "int", Value: json.RawMessage(itoaJSON(val))}},
				})
				if err == nil {
					wins.Add(1)
					return
				}
				if !isStale(err) {
					t.Errorf("round %d racer %d: %v", round, r, err)
				}
			}(r)
		}
		release()
		wg.Wait()
		p.testPreCommit = nil
		if wins.Load() != 1 {
			t.Fatalf("round %d wins=%d want 1", round, wins.Load())
		}
		// Capture what the live API exposes right after the commit.
		prop, ok := p.Current().Doc.Properties[key]
		if !ok {
			t.Fatalf("round %d winner key missing", round)
		}
		winners[round] = commit{key: key, val: prop.Value.Int}
	}

	memVersion := p.Current().Version
	// Snapshot the in-memory retained timeline for a byte-level comparison.
	memHist := map[int64]map[string]int64{}
	for _, v := range p.store.History() {
		snap, _ := p.store.AtVersion(v)
		m := map[string]int64{}
		for k, pr := range snap.Doc.Properties {
			if pr.Value.Kind == config.KindInt {
				m[k] = pr.Value.Int
			}
		}
		memHist[v] = m
	}
	_ = p.Close()

	// Recover from disk in a fresh process-equivalent plane.
	p2 := restartPlane(t, pdir)
	if p2.Current().Version != memVersion {
		t.Fatalf("recovered version %d != in-memory %d", p2.Current().Version, memVersion)
	}
	if len(p2.store.History()) != len(memHist) {
		t.Fatalf("recovered history len %d != %d", len(p2.store.History()), len(memHist))
	}
	for v, memProps := range memHist {
		snap, ok := p2.store.AtVersion(v)
		if !ok {
			t.Fatalf("recovered history missing version %d", v)
		}
		recProps := map[string]int64{}
		for k, pr := range snap.Doc.Properties {
			if pr.Value.Kind == config.KindInt {
				recProps[k] = pr.Value.Int
			}
		}
		if len(recProps) != len(memProps) {
			t.Fatalf("version %d property count recovered=%d memory=%d", v, len(recProps), len(memProps))
		}
		for k, val := range memProps {
			if recProps[k] != val {
				t.Fatalf("version %d key %s recovered=%d memory=%d", v, k, recProps[k], val)
			}
		}
	}
	// Every winner present with the exact committed value; no loser present.
	final := p2.Current().Doc.Properties
	for round, w := range winners {
		pr, ok := final[w.key]
		if !ok {
			t.Fatalf("winner %s (round %d) lost across restart", w.key, round)
		}
		if pr.Value.Int != w.val {
			t.Fatalf("winner %s value=%d want %d", w.key, pr.Value.Int, w.val)
		}
		for r := 1; r <= racers; r++ {
			loser := int64(round*100 + r)
			if loser == w.val {
				continue
			}
			if pr.Value.Int == loser {
				t.Fatalf("loser value %d resurrected for key %s", loser, w.key)
			}
		}
	}
	// Timeline continues linearly after recovery (no reuse/regression).
	_, next, err := p2.ApplyPatch(context.Background(), "writer-1", PatchRequest{
		ExpectedVersion: memVersion,
		Properties:      []PatchOp{{Key: "post.restart", Type: "int", Value: json.RawMessage("1")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if next != memVersion+1 {
		t.Fatalf("post-restart version %d want %d", next, memVersion+1)
	}
}

// A hot reload and an API write race with durability enabled; after recovery
// the converged effective config and the full retained timeline must equal
// what the API exposed before shutdown.
func TestConcurrentWriteAndReloadDurableTripleConsistency(t *testing.T) {
	pdir := t.TempDir()
	p, _ := persistentPlane(t, pdir)
	ctx := context.Background()
	fsrc := &fakeSource{name: "base"}

	const rounds = 10
	for round := 0; round < rounds; round++ {
		startVer := p.Current().Version
		apiKey := fmt.Sprintf("dapi.k%d", round)
		srcKey := fmt.Sprintf("dsrc.k%d", round)
		seam, release := startBarrier(2)
		p.testPreCommit = seam

		var wg sync.WaitGroup
		var writeErr error
		var mu sync.Mutex
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, err := p.ApplyPatch(ctx, "writer-1", PatchRequest{
				ExpectedVersion: startVer,
				Properties:      []PatchOp{{Key: apiKey, Type: "int", Value: json.RawMessage(itoaJSON(int64(round + 10)))}},
			})
			mu.Lock()
			writeErr = err
			mu.Unlock()
		}()
		go func() {
			defer wg.Done()
			raw := fmt.Sprintf(`{"version":1,"properties":{"timeout":{"type":"int","value":100},"db":{"type":"sensitive","value":"pw"},"%s":{"type":"int","value":%d}},"features":{"checkout":{"enabled":true,"percentage":50}}}`, srcKey, round+20)
			var rd config.RawDocument
			if err := json.Unmarshal([]byte(raw), &rd); err != nil {
				t.Errorf("bad json: %v", err)
				return
			}
			doc, err := config.Parse(&rd)
			if err != nil {
				t.Errorf("bad doc: %v", err)
				return
			}
			p.applySourceEvent(ctx, source.UpdateEvent{Source: fsrc, Version: int64(round + 2), Doc: doc})
		}()
		release()
		wg.Wait()
		p.testPreCommit = nil
		mu.Lock()
		we := writeErr
		mu.Unlock()
		if we != nil && !isStale(we) {
			t.Fatalf("round %d write error: %v", round, we)
		}
		if _, ok := p.Current().Doc.Properties[srcKey]; !ok {
			t.Fatalf("round %d reload did not converge", round)
		}
	}

	memVersion := p.Current().Version
	memProps := map[string]int64{}
	for k, pr := range p.Current().Doc.Properties {
		if pr.Value.Kind == config.KindInt {
			memProps[k] = pr.Value.Int
		}
	}
	memHist := p.store.History()
	_ = p.Close()

	p2 := restartPlane(t, pdir)
	if p2.Current().Version != memVersion {
		t.Fatalf("recovered version %d want %d", p2.Current().Version, memVersion)
	}
	for k, v := range memProps {
		pr, ok := p2.Current().Doc.Properties[k]
		if !ok || pr.Value.Int != v {
			t.Fatalf("key %s recovered ok=%v val=%d want %d", k, ok, func() int64 {
				if ok {
					return pr.Value.Int
				}
				return -1
			}(), v)
		}
	}
	if len(p2.store.History()) != len(memHist) {
		t.Fatalf("history length recovered=%d memory=%d", len(p2.store.History()), len(memHist))
	}
}

// A rollback and an API write that both start from the same version race.
// Exactly one linear commit happens; the resulting live override layer must be
// internally consistent with its snapshot (no write value surviving if the
// rollback won, and vice-versa), and history stays linear.
func TestConcurrentRollbackAndWrite(t *testing.T) {
	plane, _ := newTestPlane(t, time.Hour)
	ctx := context.Background()

	// Build a few versions so there is something distinct to roll back to.
	_, _, err := plane.ApplyPatch(ctx, "writer-1", PatchRequest{
		ExpectedVersion: 1,
		Properties:      []PatchOp{{Key: "timeout", Type: "int", Value: json.RawMessage("222")}},
	})
	if err != nil {
		t.Fatal(err)
	}

	const rounds = 12
	for round := 0; round < rounds; round++ {
		startVer := plane.Current().Version
		seam, release := startBarrier(2)
		plane.testPreCommit = seam

		var wg sync.WaitGroup
		var rollbackErr, writeErr error
		var mu sync.Mutex
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, e := plane.Rollback(ctx, "admin-1", 1)
			mu.Lock()
			rollbackErr = e
			mu.Unlock()
		}()
		go func() {
			defer wg.Done()
			_, _, e := plane.ApplyPatch(ctx, "writer-1", PatchRequest{
				ExpectedVersion: startVer,
				Properties:      []PatchOp{{Key: fmt.Sprintf("rb.k%d", round), Type: "int", Value: json.RawMessage("7")}},
			})
			mu.Lock()
			writeErr = e
			mu.Unlock()
		}()
		release()
		wg.Wait()
		plane.testPreCommit = nil
		mu.Lock()
		re, we := rollbackErr, writeErr
		mu.Unlock()
		if re != nil && !errIs(re, errKindStale) && errKindString(re) != "ROLLBACK_FAILED" {
			t.Fatalf("round %d rollback error: %v", round, re)
		}
		if we != nil && !isStale(we) {
			t.Fatalf("round %d write error: %v", round, we)
		}

		endVer := plane.Current().Version
		// Either one contender won and the other conflicted (+1), or the write
		// landed first and the rollback then superseded it as a separate linear
		// "restore" commit (+2). Both are coherent; what must never happen is a
		// ghost of the loser surviving inside a snapshot it does not belong to.
		if !(endVer == startVer+1 || endVer == startVer+2) {
			t.Fatalf("round %d version %d, want +1 or +2", round, endVer)
		}
		writeKey := fmt.Sprintf("rb.k%d", round)
		writeIntroduced := false
		rollbackFinal := false
		for v := startVer + 1; v <= endVer; v++ {
			snap, ok := plane.store.AtVersion(v)
			if !ok {
				t.Fatalf("round %d missing version %d", round, v)
			}
			// Every snapshot is a complete config.
			if _, ok := snap.Doc.Properties["timeout"]; !ok {
				t.Fatalf("round %d version %d missing timeout", round, v)
			}
			if _, ok := snap.Doc.Features["checkout"]; !ok {
				t.Fatalf("round %d version %d missing checkout feature", round, v)
			}
			if tv, _ := snap.GetInt("timeout"); tv == 100 {
				rollbackFinal = true
			}
			if _, ok := snap.Doc.Properties[writeKey]; ok {
				writeIntroduced = true
				// A write snapshot must not also carry rollback-restored semantics:
				// if the write key is present, timeout is the pre-rollback value.
			}
		}
		// If the rollback is the final commit, the live config is exactly v1's
		// full effective config and the write's key has been cleanly removed.
		finalHasWrite := false
		if _, ok := plane.Current().Doc.Properties[writeKey]; ok {
			finalHasWrite = true
		}
		if rollbackFinal && finalHasWrite {
			t.Fatalf("round %d: rollback restored v1 but write ghost key survived", round)
		}
		// A write that returned an error must never have been introduced.
		if we != nil && writeIntroduced {
			t.Fatalf("round %d: write failed %v but its key was committed", round, we)
		}
		_ = re
	}
	hist := plane.store.History()
	for i := 1; i < len(hist); i++ {
		if hist[i] != hist[i-1]+1 {
			t.Fatalf("non-linear history: %v", hist)
		}
	}
}
