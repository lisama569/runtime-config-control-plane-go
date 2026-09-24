package source

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

const validV1 = `{
  "version": 1,
  "properties": {"timeout": {"type": "int", "value": 100}},
  "features": {}
}`

const validV2 = `{
  "version": 2,
  "properties": {"timeout": {"type": "int", "value": 200}},
  "features": {}
}`

const invalidType = `{
  "version": 3,
  "properties": {"timeout": {"type": "int", "value": "not-a-number"}},
  "features": {}
}`

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	// Ensure a distinct mtime so mtime-based polling notices the write.
	mt := time.Now().Add(2 * time.Second)
	_ = os.Chtimes(path, mt, mt)
}

func TestFileSourceInvalidUpdateDoesNotBumpVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.json")
	writeFile(t, path, validV1)

	f := NewFileSource("f", 0, path)
	ctx := context.Background()
	doc1, v1, err := f.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if doc1.Properties["timeout"].Value.Int != 100 {
		t.Fatal("v1 not loaded")
	}

	// Invalid content: Load fails classified, source version unchanged.
	writeFile(t, path, invalidType)
	if _, _, err := f.Load(ctx); err == nil {
		t.Fatal("invalid content must be rejected")
	} else if errs.Classify(err) != errs.KindTypeMismatch {
		t.Fatalf("want TYPE_MISMATCH, got %s: %v", errs.Classify(err), err)
	}
	if f.Version() != v1 {
		t.Fatalf("version advanced after invalid load: %d -> %d", v1, f.Version())
	}
	t.Logf("invalid reload rejected kind=%s; last valid version=%d retained",
		errs.KindTypeMismatch, v1)

	// A subsequent valid change loads and bumps the version exactly once.
	writeFile(t, path, validV2)
	doc2, v2, err := f.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if doc2.Properties["timeout"].Value.Int != 200 {
		t.Fatal("v2 not loaded")
	}
	if v2 != v1+1 {
		t.Fatalf("version = %d, want %d", v2, v1+1)
	}
	if _, _, err := f.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if f.Version() != v2 {
		t.Fatal("unchanged content must not bump version")
	}
	t.Logf("valid reload accepted version=%d", v2)
}

func TestFileSourceMissingFileRejected(t *testing.T) {
	f := NewFileSource("f", 0, filepath.Join(t.TempDir(), "absent.json"))
	if _, _, err := f.Load(context.Background()); err == nil {
		t.Fatal("missing file must be rejected")
	} else {
		t.Logf("missing file rejected kind=%s: %v", errs.Classify(err), err)
	}
}

func TestWatcherDeliversOnlyValidReloads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.json")
	writeFile(t, path, validV1)
	f := NewFileSource("f", 0, path)
	if _, _, err := f.Load(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := NewWatcher([]Source{f}, 20*time.Millisecond)
	events := make(chan UpdateEvent, 4)
	go func() { _ = w.Run(ctx, events) }()
	// Let the watcher establish its initial baseline before mutating files.
	time.Sleep(100 * time.Millisecond)

	writeFile(t, path, invalidType)
	select {
	case le := <-w.Errors:
		t.Logf("watcher rejected invalid reload: kind=%s", errs.Classify(le.Err))
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected load error event")
	}
	select {
	case <-events:
		t.Fatal("invalid reload must not produce an update event")
	default:
	}

	writeFile(t, path, validV2)
	select {
	case ev := <-events:
		if ev.Doc.Properties["timeout"].Value.Int != 200 {
			t.Fatal("event carried wrong doc")
		}
		t.Logf("watcher delivered valid reload version=%d", ev.Version)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected valid reload event")
	}
}
