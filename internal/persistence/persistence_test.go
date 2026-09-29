package persistence

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

func quietLogger() *log.Logger { return log.New(&bytes.Buffer{}, "", 0) }

func sampleState(t *testing.T, current int64) *State {
	t.Helper()
	snap := &config.Snapshot{
		Version: current,
		Doc: &config.Document{
			Properties: map[string]config.Property{
				"service.timeout_ms": {
					Key:     "service.timeout_ms",
					Value:   config.Value{Kind: config.KindInt, Int: 1500},
					Version: 1,
				},
				"db.password": {
					Key:     "db.password",
					Value:   config.Value{Kind: config.KindSensitive, RawSecret: "s3cr3t"},
					Version: 1,
				},
			},
			Features: map[string]config.Feature{
				"new_checkout": {
					Name: "new_checkout", Enabled: true, Percentage: 50,
					AllowList: map[string]bool{"qa": true},
				},
			},
		},
		PropertyOrigin: map[string]string{"service.timeout_ms": "base", "db.password": "base"},
		FeatureOrigin:  map[string]string{"new_checkout": "override"},
		SourceVersions: map[string]int64{"base": 1, "override": 1},
		CreatedAtUnix:  1700000000 + current,
	}
	return &State{
		SchemaVersion:  CurrentSchemaVersion,
		CurrentVersion: current,
		History:        []SnapshotDTO{SnapshotFrom(snap)},
		Overrides: []OverrideDTO{{
			Version: current,
			Props:   []PropertyDTO{PropertyFromModel(snap.Doc.Properties["db.password"])},
		}},
	}
}

func TestStateSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := NewStateStore(dir, quietLogger())
	if _, _, err := s.Load(); err != nil {
		t.Fatalf("empty dir load: %v", err)
	}
	if err := s.Save(sampleState(t, 3)); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, report, err := s.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if report == nil || len(report.CorruptDetails) != 0 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if got.CurrentVersion != 3 || len(got.History) != 1 {
		t.Fatalf("bad state: %+v", got)
	}
	snap := got.History[0].ToSnapshot()
	if v, err := snap.GetInt("service.timeout_ms"); err != nil || v != 1500 {
		t.Fatalf("int property not restored: %d %v", v, err)
	}
	if v, err := snap.Sensitive("db.password"); err != nil || v != "s3cr3t" {
		t.Fatalf("secret not restored: %q %v", v, err)
	}
	if f, ok := snap.Feature("new_checkout"); !ok || !f.AllowList["qa"] || f.Percentage != 50 {
		t.Fatalf("feature not restored: %+v", f)
	}
	if snap.PropertyOrigin["service.timeout_ms"] != "base" {
		t.Fatalf("origin lost: %v", snap.PropertyOrigin)
	}
}

func TestStateCorruptNewestFallsBack(t *testing.T) {
	dir := t.TempDir()
	s := NewStateStore(dir, quietLogger())
	if err := s.Save(sampleState(t, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(sampleState(t, 2)); err != nil {
		t.Fatal(err)
	}
	gens, err := listGenerations(dir)
	if err != nil || len(gens) != 2 {
		t.Fatalf("generations: %v %v", gens, err)
	}
	// Trash the newest file's bytes after the checksum: a torn-on-disk /
	// garbage-write scenario.
	newest := filepath.Join(dir, generationName(gens[1]))
	if err := os.WriteFile(newest, []byte(`{"schema_version":1,"broken`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, report, err := s.Load()
	if err != nil {
		t.Fatalf("expected fallback, got error %v", err)
	}
	if got.CurrentVersion != 1 {
		t.Fatalf("expected fallback to v1, got v%d", got.CurrentVersion)
	}
	if report.FallbackTo == "" || report.FallbackFrom == "" {
		t.Fatalf("expected fallback report, got %+v", report)
	}
}

func TestStateAllCorruptFailsClosed(t *testing.T) {
	dir := t.TempDir()
	s := NewStateStore(dir, quietLogger())
	if err := s.Save(sampleState(t, 1)); err != nil {
		t.Fatal(err)
	}
	// Corrupt the only complete generation directly.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if g, ok := parseGenerationName(e.Name()); ok && g == 1 {
			p := filepath.Join(dir, e.Name())
			data, _ := os.ReadFile(p)
			data[len(data)-5] ^= 0xFF
			if err := os.WriteFile(p, data, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	got, _, err := s.Load()
	if err == nil {
		t.Fatalf("expected corrupt error, got state v%d", got.CurrentVersion)
	}
	if !errs.Of(err, errs.KindStateCorrupt) {
		t.Fatalf("expected STATE_CORRUPT, got %v (%s)", err, errs.Classify(err))
	}
}

func TestStateChecksumTamperDetected(t *testing.T) {
	dir := t.TempDir()
	s := NewStateStore(dir, quietLogger())
	if err := s.Save(sampleState(t, 1)); err != nil {
		t.Fatal(err)
	}
	gens, _ := listGenerations(dir)
	p := filepath.Join(dir, generationName(gens[0]))
	data, _ := os.ReadFile(p)
	// Flip a byte in the version number region while keeping valid JSON shape
	// is hard; simply flipping any payload byte must fail checksum.
	for i := range data {
		if data[i] == '1' {
			data[i] = '2'
			break
		}
	}
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Load(); !errs.Of(err, errs.KindStateCorrupt) {
		t.Fatalf("expected STATE_CORRUPT, got %v", err)
	}
}

func TestStateTempFilesIgnored(t *testing.T) {
	dir := t.TempDir()
	s := NewStateStore(dir, quietLogger())
	if err := s.Save(sampleState(t, 1)); err != nil {
		t.Fatal(err)
	}
	// Leftover temp from a kill mid-write must never be read as a commit.
	if err := os.WriteFile(filepath.Join(dir, "state-000000002.tmp"),
		[]byte("garbage partial write"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, report, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentVersion != 1 {
		t.Fatalf("temp file affected recovery: v%d", got.CurrentVersion)
	}
	found := false
	for _, f := range report.IgnoredFiles {
		if strings.Contains(f, ".tmp") {
			found = true
		}
	}
	if !found {
		t.Fatalf("temp file not reported as ignored: %+v", report.IgnoredFiles)
	}
}

func TestStateGenerationRetainedTwice(t *testing.T) {
	dir := t.TempDir()
	s := NewStateStore(dir, quietLogger())
	for i := int64(1); i <= 5; i++ {
		if err := s.Save(sampleState(t, i)); err != nil {
			t.Fatal(err)
		}
	}
	gens, err := listGenerations(dir)
	if err != nil || len(gens) != retainGenerations {
		t.Fatalf("expected %d generations, got %v err=%v", retainGenerations, gens, err)
	}
	if gens[0] != 4 || gens[1] != 5 {
		t.Fatalf("unexpected retained generations: %v", gens)
	}
}

func TestStateCapacityBound(t *testing.T) {
	dir := t.TempDir()
	s := NewStateStore(dir, quietLogger())
	st := sampleState(t, 1)
	big := strings.Repeat("x", MaxStateBytes)
	st.Overrides[0].DeadProps = []string{big}
	err := s.Save(st)
	if !errs.Of(err, errs.KindPersistFailed) {
		t.Fatalf("expected PERSIST_FAILED on oversized state, got %v", err)
	}
	// No new generation must be visible after a rejected save.
	gens, _ := listGenerations(dir)
	if len(gens) != 0 {
		t.Fatalf("rejected save left generations: %v", gens)
	}
}
