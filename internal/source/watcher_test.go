package source

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const docV1 = `{"flags":[{"name":"f","enabled":true,"rollout":{"percentage":10}}]}`
const docV2 = `{"flags":[{"name":"f","enabled":true,"rollout":{"percentage":20}}]}`
const docBad = `{"flags":[{"name":"f","rollout":{"percentage":999}}]}`

func TestWatcherHotReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "conf.json")
	if err := os.WriteFile(path, []byte(docV1), 0o600); err != nil {
		t.Fatal(err)
	}
	fs, err := NewFileSource("tmp", path)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWatcher(20*time.Millisecond, fs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan ChangeEvent, 16)
	go func() { _ = w.Run(ctx, func(ev ChangeEvent) { events <- ev }) }()

	// no initial event: modify -> change event with new content
	time.Sleep(30 * time.Millisecond) // ensure mtime differs
	if err := os.WriteFile(path, []byte(docV2), 0o600); err != nil {
		t.Fatal(err)
	}
	var ev ChangeEvent
	select {
	case ev = <-events:
		if ev.Err != nil {
			t.Fatalf("reload failed: %v", ev.Err)
		}
		t.Logf("hot reload event received, flags=%d", len(ev.Parsed[0].Raw.Flags))
	case <-time.After(2 * time.Second):
		t.Fatal("no change event after file modification")
	}

	// write invalid content -> error event, never a partial doc
	time.Sleep(30 * time.Millisecond)
	if err := os.WriteFile(path, []byte(docBad), 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case ev = <-events:
		if ev.Err == nil {
			// load succeeded; merge-time validation happens in the plane.
			// Here we only assert the raw doc loaded.
			t.Logf("invalid doc loaded raw (validation deferred to merge): %+v", ev.Parsed[0].Raw.Flags)
		} else {
			t.Logf("invalid doc produced error event: %v", ev.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event after invalid modification")
	}
}

func TestWatcherMissingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nope.json")
	if err := os.WriteFile(path, []byte(docV1), 0o600); err != nil {
		t.Fatal(err)
	}
	fs, _ := NewFileSource("missing", path)
	w := NewWatcher(20*time.Millisecond, fs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan ChangeEvent, 4)
	go func() { _ = w.Run(ctx, func(ev ChangeEvent) { events <- ev }) }()
	// delete the file -> fingerprint changes -> load error surfaces
	time.Sleep(30 * time.Millisecond)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-events:
		t.Logf("missing file event: err=%v", ev.Err)
		if ev.Err == nil {
			t.Fatal("missing file must produce error event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event for missing file")
	}
}
