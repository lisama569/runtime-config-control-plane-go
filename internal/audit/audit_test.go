package audit

import (
	"context"
	"testing"
)

func TestAppendOnlyOrdering(t *testing.T) {
	l := NewLogger()
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		seq, err := l.Record(ctx, Entry{Actor: "a", Action: "x", Result: ResultAllowed})
		if err != nil {
			t.Fatal(err)
		}
		if seq != int64(i+1) {
			t.Fatalf("seq = %d, want %d", seq, i+1)
		}
	}
	list := l.List()
	if len(list) != 100 {
		t.Fatalf("len = %d", len(list))
	}
	if list[0].Seq != 1 || list[99].Seq != 100 {
		t.Fatal("audit order broken")
	}
	// Defensive copy: mutating returned slice must not corrupt the log.
	list[0].Actor = "forged"
	if l.List()[0].Actor != "a" {
		t.Fatal("List must return a defensive copy")
	}
	t.Log("audit log append-only, sequential seq, copies are isolated")
}

type fakeSink struct{ payloads [][]byte }

func (f *fakeSink) Append(p []byte) error {
	f.payloads = append(f.payloads, append([]byte(nil), p...))
	return nil
}

func TestRestoreContinuesSeqAndSinkReceivesOnlyNewRecords(t *testing.T) {
	l := NewLogger()
	ctx := context.Background()
	// Simulate entries recovered from disk.
	l.Restore([]Entry{
		{Seq: 1, Actor: "a", Result: ResultAllowed},
		{Seq: 4, Actor: "b", Result: ResultDenied},
	})
	sink := &fakeSink{}
	l.SetSink(sink, nil)
	seq, err := l.Record(ctx, Entry{Actor: "c", Result: ResultAllowed})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 5 {
		t.Fatalf("seq after restore = %d, want 5 (never reuse)", seq)
	}
	if len(sink.payloads) != 1 {
		t.Fatalf("sink should only receive new records, got %d", len(sink.payloads))
	}
	list := l.List()
	if len(list) != 3 || list[2].Seq != 5 {
		t.Fatalf("restored+new trail wrong: %+v", list)
	}
	// Defensive copy: mutating the input slice must not affect the logger.
	restored := []Entry{{Seq: 99, Actor: "z"}}
	l2 := NewLogger()
	l2.Restore(restored)
	restored[0].Actor = "mutated"
	if got := l2.List(); got[0].Actor != "z" {
		t.Fatalf("restore did not copy entries: %+v", got)
	}
}
