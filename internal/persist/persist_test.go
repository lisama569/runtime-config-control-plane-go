package persist

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lisama569/runtime-config-control-plane-go/internal/audit"
	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

func testState(ver int64, secret string) *State {
	props := map[string]config.Property{
		"svc.timeout": {Key: "svc.timeout", Version: ver,
			Value: config.Value{Kind: config.KindInt, Int: 1000 + ver}},
	}
	if secret != "" {
		props["db.password"] = config.Property{Key: "db.password", Version: ver,
			Value: config.Value{Kind: config.KindSensitive, RawSecret: secret}}
	}
	doc := &config.Document{
		Properties: props,
		Features: map[string]config.Feature{
			"new_checkout": {Name: "new_checkout", Enabled: true, Percentage: 50,
				AllowList: map[string]bool{"qa": true}},
		},
	}
	snap := &config.Snapshot{
		Version:        ver,
		Doc:            doc,
		PropertyOrigin: map[string]string{"svc.timeout": "api-override"},
		FeatureOrigin:  map[string]string{"new_checkout": "base"},
		SourceVersions: map[string]int64{"base": ver},
		CreatedAtUnix:  ver,
	}
	ov := Override{
		Props:     map[string]config.Property{"svc.timeout": props["svc.timeout"]},
		Feats:     map[string]config.Feature{},
		DeadProps: map[string]bool{},
		DeadFeats: map[string]bool{},
	}
	return &State{
		Current:   snap,
		History:   []*config.Snapshot{snap},
		Overrides: map[int64]Override{ver: ov},
	}
}

func TestSaveAndReopenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, rep, st, err := Open(ctx, dir, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Loaded {
		t.Fatal("fresh dir must not report loaded state")
	}
	if st != nil {
		t.Fatal("fresh dir must yield nil state")
	}

	orig := testState(1, "s3cr3t")
	if err := s.SaveState(ctx, orig); err != nil {
		t.Fatal(err)
	}
	s2, rep2, st2, err := Open(ctx, dir, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if !rep2.Loaded || rep2.StateVersion != 1 {
		t.Fatalf("recovery report = %+v, want loaded v1", rep2)
	}
	got := st2.Current
	if got.Version != 1 {
		t.Fatalf("version = %d", got.Version)
	}
	if got.Doc.Properties["svc.timeout"].Value.Int != 1001 {
		t.Fatalf("property not restored: %+v", got.Doc.Properties["svc.timeout"])
	}
	// Secrets must round-trip (file perms keep them owner-only).
	if sec, _ := got.Sensitive("db.password"); sec != "s3cr3t" {
		t.Fatalf("secret = %q", sec)
	}
	if got.PropertyOrigin["svc.timeout"] != "api-override" {
		t.Fatalf("origin lost: %v", got.PropertyOrigin)
	}
	if ov, ok := st2.Overrides[1]; !ok || ov.Props["svc.timeout"].Value.Int != 1001 {
		t.Fatalf("override state not restored: %+v", st2.Overrides)
	}
	_ = s2
}

func TestContinuousRestartMonotonicVersions(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	var st *State
	for v := int64(1); v <= 5; v++ {
		s, rep, loaded, err := Open(ctx, dir, DefaultLimits())
		if err != nil {
			t.Fatalf("open at v%d: %v", v, err)
		}
		if v > 1 {
			if !rep.Loaded || loaded == nil || loaded.Current.Version != v-1 {
				t.Fatalf("restart %d recovered %+v (st=%v)", v, rep, loaded)
			}
			st = growState(loaded, v)
		} else {
			st = testState(v, "")
		}
		if err := s.SaveState(ctx, st); err != nil {
			t.Fatalf("save v%d: %v", v, err)
		}
	}
	// Final restart: timeline keeps moving, no reset/reuse.
	_, rep, got, err := Open(ctx, dir, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if got.Current.Version != 5 {
		t.Fatalf("final version = %d, want 5", got.Current.Version)
	}
	for _, p := range rep.Problems {
		t.Errorf("unexpected recovery problem: %s", p)
	}
}

func growState(prev *State, ver int64) *State {
	next := testState(ver, "")
	hist := append(append([]*config.Snapshot(nil), prev.History...), next.Current)
	ovs := map[int64]Override{}
	for v, ov := range prev.Overrides {
		ovs[v] = ov
	}
	ovs[ver] = next.Overrides[ver]
	return &State{Current: next.Current, History: hist, Overrides: ovs}
}

func TestCorruptPrimaryFallsBackToBackup(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, _, _, err := Open(ctx, dir, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveState(ctx, testState(1, "")); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveState(ctx, testState(2, "")); err != nil {
		t.Fatal(err)
	}
	// state.json is now v2, backup is v1. Corrupt the primary.
	if err := os.WriteFile(statePath(dir), []byte("{garbage"), filePerm); err != nil {
		t.Fatal(err)
	}
	s2, rep, st, err := Open(ctx, dir, DefaultLimits())
	if err != nil {
		t.Fatalf("expected backup recovery, got %v", err)
	}
	if !rep.UsedBackup || st.Current.Version != 1 {
		t.Fatalf("report = %+v version = %d", rep, st.Current.Version)
	}
	if len(rep.Problems) != 1 || rep.Problems[0].Kind != string(errs.KindPersistCorrupt) {
		t.Fatalf("problems = %+v", rep.Problems)
	}
	// After recovery a clean canonical state is written.
	if _, rep2, st2, err := Open(ctx, dir, DefaultLimits()); err != nil ||
		rep2.UsedBackup || st2.Current.Version != 1 {
		t.Fatalf("second restart not clean: rep=%+v st=%v err=%v", rep2, st2, err)
	}
	_ = s2
}

func TestChecksumTamperingDetected(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, _, _, err := Open(ctx, dir, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveState(ctx, testState(1, "")); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveState(ctx, testState(2, "")); err != nil {
		t.Fatal(err)
	}
	// Damage the primary inside its data region; CRC must catch it and the
	// intact backup must be used (rollback to last complete state).
	p := statePath(dir)
	b, _ := os.ReadFile(p)
	flipDataBytes(b)
	if err := os.WriteFile(p, b, filePerm); err != nil {
		t.Fatal(err)
	}
	_, rep, st, err := Open(ctx, dir, DefaultLimits())
	if err != nil {
		t.Fatalf("backup should recover, got %v", err)
	}
	if !rep.UsedBackup || st.Current.Version != 1 {
		t.Fatalf("rep=%+v version=%d", rep, st.Current.Version)
	}

	// Fresh files: damage BOTH before any open -> must fail closed, never
	// serve damaged content.
	dir2 := t.TempDir()
	s2, _, _, _ := Open(ctx, dir2, DefaultLimits())
	_ = s2.SaveState(ctx, testState(1, ""))
	_ = s2.SaveState(ctx, testState(2, ""))
	for _, fp := range []string{statePath(dir2), backupPath(dir2)} {
		bb, _ := os.ReadFile(fp)
		flipDataBytes(bb)
		if err := os.WriteFile(fp, bb, filePerm); err != nil {
			t.Fatal(err)
		}
	}
	_, _, _, err = Open(ctx, dir2, DefaultLimits())
	if err == nil {
		t.Fatal("expected fail-closed when both files corrupt")
	}
	if errs.Classify(err) != errs.KindPersistChecksum &&
		errs.Classify(err) != errs.KindPersistCorrupt {
		t.Fatalf("error kind = %s (%v)", errs.Classify(err), err)
	}
}

// flipDataBytes flips a few bytes in the middle of a state file (the data
// payload region), mutating in place.
func flipDataBytes(b []byte) {
	for i := len(b) / 2; i < len(b)/2+40; i++ {
		b[i] ^= 0xFF
	}
}

func TestTornWriteLeavesLastCompleteState(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, _, _, _ := Open(ctx, dir, DefaultLimits())
	_ = s.SaveState(ctx, testState(3, ""))
	// Simulate a kill that left only a temp file and no new primary: the
	// primary remains the last complete state.
	if err := os.WriteFile(filepath.Join(dir, ".state-123.tmp"),
		[]byte("partial"), filePerm); err != nil {
		t.Fatal(err)
	}
	_, rep, st, err := Open(ctx, dir, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if st.Current.Version != 3 || rep.UsedBackup {
		t.Fatalf("version=%d rep=%+v", st.Current.Version, rep)
	}
}

func TestStateTooLargeRejected(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	lim := DefaultLimits()
	lim.MaxStateBytes = 200
	s, _, _, err := Open(ctx, dir, lim)
	if err != nil {
		t.Fatal(err)
	}
	err = s.SaveState(ctx, testState(1, "s3cr3t-with-some-length-to-exceed"))
	if !errs.Of(err, errs.KindPersistTooLarge) {
		t.Fatalf("want PERSIST_TOO_LARGE, got %v", err)
	}
	// Rejected write must not have replaced the on-disk state.
	if _, err := os.Stat(statePath(dir)); !os.IsNotExist(err) {
		t.Fatalf("state file must not exist after rejected first write, stat err=%v", err)
	}
}

func TestPruneStateToRetainWindow(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	lim := DefaultLimits()
	lim.RetainVersions = 3
	s, _, _, err := Open(ctx, dir, lim)
	if err != nil {
		t.Fatal(err)
	}
	var st *State
	for v := int64(1); v <= 6; v++ {
		if v == 1 {
			st = testState(v, "")
		} else {
			st = growState(st, v)
		}
	}
	if err := s.SaveState(ctx, st); err != nil {
		t.Fatal(err)
	}
	_, _, got, err := Open(ctx, dir, lim)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.History) != 3 {
		t.Fatalf("retained %d versions, want 3", len(got.History))
	}
	if got.History[0].Version != 4 || got.Current.Version != 6 {
		t.Fatalf("window = %v..%v, want 4..6", got.History[0].Version, got.Current.Version)
	}
	// Override data trimmed in lockstep.
	if len(got.Overrides) != 3 {
		t.Fatalf("overrides retained = %d, want 3", len(got.Overrides))
	}
	if _, ok := got.Overrides[3]; ok {
		t.Fatal("pruned version 3 must not keep override data")
	}
}

func TestAuditJournalReplayTornTailAndCompaction(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, _, _, err := Open(ctx, dir, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		if err := s.AppendAudit(ctx, audit.Entry{Actor: "alice", Action: "config:write", Seq: int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// Append a garbage torn tail.
	f, _ := os.OpenFile(auditPath(dir), os.O_APPEND|os.O_WRONLY, filePerm)
	_, _ = f.WriteString("\x00\x01\x02garbage-tail-bytes")
	_ = f.Close()

	entries, rep, err := s.LoadAudit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 10 {
		t.Fatalf("replayed %d entries, want 10", len(entries))
	}
	if len(rep.Problems) == 0 {
		t.Fatal("torn tail must be reported")
	}
	if !strings.Contains(rep.Problems[0].Message, "audit") {
		t.Fatalf("problem = %+v", rep.Problems[0])
	}

	// Compaction bound.
	lim := DefaultLimits()
	lim.MaxAuditEntries = 4
	s2, _, _, _ := Open(ctx, dir, lim)
	if err := s2.AppendAudit(ctx, audit.Entry{Actor: "bob", Seq: 11}); err != nil {
		t.Fatal(err)
	}
	got := s2.AuditEntries()
	if len(got) != 4 {
		t.Fatalf("after compaction entries = %d, want 4", len(got))
	}
	if got[0].Seq != 8 || got[3].Seq != 11 {
		t.Fatalf("compaction window seqs = %d..%d, want 8..11", got[0].Seq, got[3].Seq)
	}
	// Reopen reads exactly the compacted journal.
	s3, rep3, _, err := Open(ctx, dir, lim)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(s3.AuditEntries()); n != 4 {
		t.Fatalf("reopen entries = %d", n)
	}
	if len(rep3.Problems) != 0 {
		t.Fatalf("compacted journal must be clean: %+v", rep3.Problems)
	}
}
