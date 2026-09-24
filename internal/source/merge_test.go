package source

import (
	"context"
	"testing"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
)

func loadParsed(t *testing.T, paths ...string) []ParsedSource {
	t.Helper()
	ctx := context.Background()
	srcs := make([]Source, 0, len(paths))
	for i, p := range paths {
		fs, err := NewFileSource(p, p)
		if err != nil {
			t.Fatalf("source %d: %v", i, err)
		}
		srcs = append(srcs, fs)
	}
	parsed, err := LoadAll(ctx, srcs)
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	return parsed
}

func TestMergePriorityStable(t *testing.T) {
	// first listed = highest priority
	parsed := loadParsed(t, "../../testdata/override.json", "../../testdata/base.json")
	mr, err := Merge(parsed)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	f := mr.Config.Flags["checkout_v2"]
	t.Logf("checkout_v2 winner=%s enabled=%v percentage=%d (base says enabled/25%%, override says disabled/100%%)",
		mr.Provenance["flag:checkout_v2"], f.Enabled, f.Rollout.Percentage)
	if mr.Provenance["flag:checkout_v2"] != "../../testdata/override.json" {
		t.Fatalf("override source must win flag, got %s", mr.Provenance["flag:checkout_v2"])
	}
	if f.Enabled || f.Rollout.Percentage != 100 {
		t.Fatalf("higher priority definition must be used wholly")
	}
	g := mr.Config.Settings["greeting"]
	t.Logf("greeting winner=%s value=%v", mr.Provenance["setting:greeting"], g.Value)
	if g.Value != "hola" {
		t.Fatalf("override greeting must win, got %v", g.Value)
	}
	// lower-priority-only entries still appear
	if _, ok := mr.Config.Flags["beta_only"]; !ok {
		t.Fatalf("base-only flag must be present")
	}
	if pw, ok := mr.Config.Settings["db_password"]; !ok || pw.Value != "s3cr3t" || !pw.Secret {
		t.Fatalf("base secret must be present and flagged secret, got %+v", pw)
	}
	// reverse order -> base wins
	parsedRev := loadParsed(t, "../../testdata/base.json", "../../testdata/override.json")
	mrRev, err := Merge(parsedRev)
	if err != nil {
		t.Fatalf("reverse merge: %v", err)
	}
	t.Logf("reversed: checkout_v2 winner=%s", mrRev.Provenance["flag:checkout_v2"])
	if mrRev.Config.Flags["checkout_v2"].Rollout.Percentage != 25 {
		t.Fatalf("base must win when listed first")
	}
}

func TestMergeRejectsInvalidLowerSource(t *testing.T) {
	parsed := loadParsed(t, "../../testdata/broken.json")
	_, err := Merge(parsed)
	cat := config.CatOf(err)
	t.Logf("broken merge rejected: category=%s error=%v", cat, err)
	if cat != config.CatValidation {
		t.Fatalf("expected validation error, got %s (%v)", cat, err)
	}
}

func TestLoadMalformedFails(t *testing.T) {
	fs, _ := NewFileSource("malformed", "../../testdata/malformed.json")
	_, err := fs.Load(context.Background())
	t.Logf("malformed load error: %v", err)
	if err == nil {
		t.Fatalf("malformed JSON must fail to load")
	}
}
