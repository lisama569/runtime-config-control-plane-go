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
