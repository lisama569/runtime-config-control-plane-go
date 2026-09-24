package audit

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestRecordAndQuery(t *testing.T) {
	l := New(4, nil)
	l.Record("alice", "config.write", OutcomeSuccess, WithVersions(1, 2), WithReason("deploy"))
	l.Record("bob", "config.write", OutcomeDenied, WithCategory("forbidden"))
	l.Record("alice", "config.rollback", OutcomeSuccess, WithVersions(2, 3))

	entries := l.Query(0, "")
	t.Logf("total entries=%d newest=%s/%s", len(entries), entries[0].Actor, entries[0].Action)
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	if entries[0].Action != "config.rollback" {
		t.Fatalf("newest first ordering violated")
	}
	alice := l.Query(0, "alice")
	if len(alice) != 2 {
		t.Fatalf("actor filter broken, got %d", len(alice))
	}
	if entries[0].Seq <= entries[1].Seq {
		t.Fatalf("sequence numbers must increase")
	}
}

func TestRingEvictsOldest(t *testing.T) {
	l := New(3, nil)
	for i := 0; i < 10; i++ {
		l.Record("a", "op", OutcomeSuccess)
	}
	entries := l.Query(0, "")
	t.Logf("ring size=%d oldest seq=%d", len(entries), entries[len(entries)-1].Seq)
	if len(entries) != 3 {
		t.Fatalf("ring must cap at 3, got %d", len(entries))
	}
}

func TestConcurrentRecord(t *testing.T) {
	l := New(1000, nil)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				l.Record("a", "op", OutcomeSuccess)
			}
		}()
	}
	wg.Wait()
	entries := l.Query(0, "")
	t.Logf("concurrent records=%d", len(entries))
	if len(entries) != 800 {
		t.Fatalf("expected 800 entries, got %d", len(entries))
	}
	seen := map[int64]bool{}
	for _, e := range entries {
		if seen[e.Seq] {
			t.Fatalf("duplicate seq %d", e.Seq)
		}
		seen[e.Seq] = true
	}
}

func TestFileSink(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	sink, err := NewFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	l := New(10, sink)
	l.Record("alice", "config.write", OutcomeSuccess, WithVersions(0, 1))
	sink.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("sink file content: %s", strings.TrimSpace(string(data)))
	if !strings.Contains(string(data), `"actor":"alice"`) {
		t.Fatalf("sink file missing entry")
	}
}
