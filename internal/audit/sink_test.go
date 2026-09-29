package audit

import (
	"context"
	"errors"
	"testing"
)

type memSink struct {
	entries []Entry
	err     error
}

func (m *memSink) Append(_ context.Context, e Entry) error {
	if m.err != nil {
		return m.err
	}
	m.entries = append(m.entries, e)
	return nil
}

func TestSeedContinuesSeqAndSinkReceivesNewEntries(t *testing.T) {
	seed := []Entry{{Seq: 5, Actor: "old"}, {Seq: 6, Actor: "old"}}
	l := NewLoggerWithSeed(seed)
	if got := len(l.List()); got != 2 {
		t.Fatalf("seeded len = %d", got)
	}
	sink := &memSink{}
	var sinkErr error
	l.AttachSink(sink, func(err error) { sinkErr = err })
	seq, err := l.Record(context.Background(), Entry{Actor: "new"})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 7 {
		t.Fatalf("seq = %d, want 7 (must continue past replayed 6)", seq)
	}
	if len(sink.entries) != 1 || sink.entries[0].Actor != "new" {
		t.Fatalf("sink did not receive new entry: %+v", sink.entries)
	}
	if len(l.List()) != 3 {
		t.Fatal("List must include seeded + new in order")
	}

	// A sink failure is reported but must not fail the recorded operation.
	sink.err = errors.New("disk full")
	seq2, err := l.Record(context.Background(), Entry{Actor: "new2"})
	if err != nil || seq2 != 8 {
		t.Fatalf("record after sink failure: seq=%d err=%v", seq2, err)
	}
	if sinkErr == nil {
		t.Fatal("expected sink error callback")
	}
}
