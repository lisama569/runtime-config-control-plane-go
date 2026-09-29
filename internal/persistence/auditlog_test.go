package persistence

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

func TestAuditJournalAppendLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	a := NewAuditSink(path, quietLogger())
	if _, _, err := a.Load(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := a.Append([]byte(`{"seq":` + itoa64(int64(i+1)) + `}`)); err != nil {
			t.Fatal(err)
		}
	}
	b := NewAuditSink(path, quietLogger())
	got, truncated, err := b.Load()
	if err != nil || truncated {
		t.Fatalf("load: %v truncated=%v", err, truncated)
	}
	if len(got) != 5 {
		t.Fatalf("want 5 frames, got %d", len(got))
	}
}

func TestAuditJournalTornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")
	a := NewAuditSink(path, quietLogger())
	if _, _, err := a.Load(); err != nil {
		t.Fatal(err)
	}
	if err := a.Append([]byte(`{"seq":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := a.Append([]byte(`{"seq":2}`)); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a kill mid-append of a third frame: append partial frame bytes
	// (correct magic + length claiming more bytes than present).
	torn := append([]byte(data), []byte(auditMagic)...)
	torn = append(torn, 0, 0, 1, 0) // claims 256 bytes payload
	torn = append(torn, 'x', 'y')   // ... but only 2 arrive
	if err := os.WriteFile(path, torn, 0o600); err != nil {
		t.Fatal(err)
	}
	b := NewAuditSink(path, quietLogger())
	got, truncated, err := b.Load()
	if err != nil {
		t.Fatalf("torn tail should truncate, got %v", err)
	}
	if !truncated || len(got) != 2 {
		t.Fatalf("want 2 intact frames + truncation, got %d truncated=%v", len(got), truncated)
	}
	// Journal must have been physically repaired; subsequent appends work.
	if err := b.Append([]byte(`{"seq":3}`)); err != nil {
		t.Fatal(err)
	}
	c := NewAuditSink(path, quietLogger())
	got2, tr2, err := c.Load()
	if err != nil || tr2 || len(got2) != 3 {
		t.Fatalf("post-repair load: frames=%d truncated=%v err=%v", len(got2), tr2, err)
	}
}

func TestAuditJournalMidCorruptionRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")
	a := NewAuditSink(path, quietLogger())
	if _, _, err := a.Load(); err != nil {
		t.Fatal(err)
	}
	if err := a.Append([]byte(`{"seq":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := a.Append([]byte(`{"seq":2}`)); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	// Corrupt a payload byte of the FIRST frame: damage is not at the tail.
	data[auditHdrLen+2] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	b := NewAuditSink(path, quietLogger())
	_, _, err := b.Load()
	if !errs.Of(err, errs.KindStateCorrupt) {
		t.Fatalf("expected STATE_CORRUPT for mid-journal damage, got %v", err)
	}
}

func TestAuditJournalCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	a := NewAuditSink(path, quietLogger())
	if _, _, err := a.Load(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := a.Append([]byte(`{"seq":` + itoa64(int64(i+1)) + `}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Compact(4); err != nil {
		t.Fatal(err)
	}
	b := NewAuditSink(path, quietLogger())
	got, _, err := b.Load()
	if err != nil || len(got) != 4 {
		t.Fatalf("want 4 retained frames, got %d err=%v", len(got), err)
	}
}
