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

// Sink receives every recorded entry so it can be made durable. It is
// deliberately tiny (implemented by internal/persist) to keep audit free of
// storage dependencies.
type Sink interface {
	Append(ctx context.Context, e Entry) error
}

// Logger is the append-only audit sink.
type Logger struct {
	mu      sync.Mutex
	seq     atomic.Int64
	entries []Entry
	sink    Sink
	// sinkErr, if set, receives durability failures. A sink failure never
	// fails the audited operation: configuration commits are durable on their
	// own path, so only the audit write is lost.
	sinkErr func(error)
}

// NewLogger constructs an empty in-memory audit logger.
func NewLogger() *Logger { return &Logger{} }

// NewLoggerWithSeed constructs a logger preloaded with replayed entries (the
// durable suffix). Seeding happens before any new recording, so sequence
// numbers continue past the replayed maximum and List() stays append-ordered
// across restarts.
func NewLoggerWithSeed(seed []Entry) *Logger {
	l := &Logger{entries: make([]Entry, 0, len(seed))}
	var maxSeq int64
	for _, e := range seed {
		l.entries = append(l.entries, e)
		if e.Seq > maxSeq {
			maxSeq = e.Seq
		}
	}
	l.seq.Store(maxSeq)
	return l
}

// AttachSink installs a durability sink and an error reporter (may be nil).
// Only entries recorded AFTER attachment are forwarded.
func (l *Logger) AttachSink(sink Sink, onError func(error)) {
	l.mu.Lock()
	l.sink = sink
	l.sinkErr = onError
	l.mu.Unlock()
}

// Record appends an entry and returns its sequence number. Seq and timestamp
// are assigned here so callers cannot forge ordering. The entry is appended
// in memory first (it always survives the process), then forwarded to the
// durable sink; a sink error is reported but does not fail the call.
func (l *Logger) Record(ctx context.Context, e Entry) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	e.Seq = l.seq.Add(1)
	e.TimeUnix = time.Now().Unix()
	l.mu.Lock()
	l.entries = append(l.entries, e)
	sink := l.sink
	onErr := l.sinkErr
	l.mu.Unlock()
	if sink != nil {
		if err := sink.Append(ctx, e); err != nil {
			if onErr != nil {
				onErr(err)
			}
		}
	}
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
