// Package audit records an append-only trail of privileged operations.
package audit

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Entry records one privileged operation and its outcome. Denials are
// recorded too, so unauthorized attempts are visible even though they never
// mutate runtime state.
type Entry struct {
	Seq      int64  `json:"seq"`
	TimeUnix int64  `json:"time_unix"`
	Actor    string `json:"actor"`
	Role     string `json:"role"`
	Action   string `json:"action"`
	Target   string `json:"target"`
	Result   string `json:"result"` // allowed | denied | error
	Reason   string `json:"reason"`
	FromVer  int64  `json:"from_version"`
	ToVer    int64  `json:"to_version"`
	ErrKind  string `json:"err_kind,omitempty"`
}

// Result values.
const (
	ResultAllowed = "allowed"
	ResultDenied  = "denied"
	ResultError   = "error"
)

// Logger is the append-only audit sink.
type Logger struct {
	mu      sync.Mutex
	seq     atomic.Int64
	entries []Entry
}

// NewLogger constructs an in-memory audit logger.
func NewLogger() *Logger { return &Logger{} }

// Record appends an entry and returns its sequence number. Seq and timestamp
// are assigned here so callers cannot forge ordering.
func (l *Logger) Record(ctx context.Context, e Entry) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	e.Seq = l.seq.Add(1)
	e.TimeUnix = time.Now().Unix()
	l.mu.Lock()
	l.entries = append(l.entries, e)
	l.mu.Unlock()
	return e.Seq, nil
}

// List returns a defensive copy of all entries in append order.
func (l *Logger) List() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Entry, len(l.entries))
	copy(out, l.entries)
	return out
}
