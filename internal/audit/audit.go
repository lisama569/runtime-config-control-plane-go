// Package audit records an append-only trail of privileged operations.
package audit

import (
	"context"
	"encoding/json"
	"log"
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

// DurableSink is implemented by persistence backends that can durably store
// one already-JSON-encoded audit entry.
type DurableSink interface {
	Append(payload []byte) error
}

// Logger is the append-only audit sink.
type Logger struct {
	mu      sync.Mutex
	seq     atomic.Int64
	entries []Entry
	sink    DurableSink
	logger  *log.Logger
}

// NewLogger constructs an in-memory audit logger.
func NewLogger() *Logger { return &Logger{} }

// SetSink installs a durable sink. Restored entries already exist in that
// sink; only subsequent Record calls are appended. A nil sink disables
// durability.
func (l *Logger) SetSink(sink DurableSink, logger *log.Logger) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sink = sink
	l.logger = logger
}

// Restore replaces the in-memory trail with previously persisted entries
// (already decoded into Entry values). It is a startup-only operation: the
// monotonic sequence counter continues from the highest restored Seq, so
// sequence numbers are never reused across restarts. The provided entries
// are defensively copied.
func (l *Logger) Restore(entries []Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = make([]Entry, len(entries))
	copy(l.entries, entries)
	var max int64
	for _, e := range l.entries {
		if e.Seq > max {
			max = e.Seq
		}
	}
	l.seq.Store(max)
}

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
	sink := l.sink
	logger := l.logger
	l.mu.Unlock()
	// Durable append happens outside the in-memory lock. A failure does not
	// un-accept the operation: audit durability is best-effort relative to
	// the operation, but the attempt is logged clearly.
	if sink != nil {
		payload, err := json.Marshal(e)
		if err != nil {
			if logger != nil {
				logger.Printf("audit: cannot encode entry seq=%d: %v", e.Seq, err)
			}
		} else if err := sink.Append(payload); err != nil {
			if logger != nil {
				logger.Printf("audit: durable append failed for seq=%d: %v", e.Seq, err)
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
