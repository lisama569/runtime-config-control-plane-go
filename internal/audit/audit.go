package audit

import (
	"sync"
	"time"
)

// Outcome enumerates the result of an audited operation.
type Outcome string

const (
	OutcomeSuccess Outcome = "success"
	OutcomeDenied  Outcome = "denied"
	OutcomeReject  Outcome = "rejected"
)

// Entry is one immutable audit record.
type Entry struct {
	Seq      int64     `json:"seq"`
	Time     time.Time `json:"time"`
	Actor    string    `json:"actor"`
	Action   string    `json:"action"`
	Category string    `json:"category,omitempty"`
	Resource string    `json:"resource,omitempty"`
	FromVer  int64     `json:"from_version,omitempty"`
	ToVer    int64     `json:"to_version,omitempty"`
	Outcome  Outcome   `json:"outcome"`
	Reason   string    `json:"reason,omitempty"`
	Detail   string    `json:"detail,omitempty"`
}

// Logger records auditable operations in memory and optionally mirrors
// to a JSONL sink. It is safe for concurrent use.
type Logger struct {
	mu       sync.Mutex
	entries  []Entry
	ringSize int
	seq      int64
	sink     Sink
}

// Sink receives entries outside the in-memory ring.
type Sink interface {
	Write(Entry) error
}

// New constructs an audit logger with the given ring capacity.
func New(ringSize int, sink Sink) *Logger {
	if ringSize <= 0 {
		ringSize = 1024
	}
	return &Logger{ringSize: ringSize, sink: sink}
}

// Record appends an entry and returns the stored copy.
func (l *Logger) Record(actor, action string, outcome Outcome, opts ...Option) Entry {
	e := Entry{
		Time:    time.Now().UTC(),
		Actor:   actor,
		Action:  action,
		Outcome: outcome,
	}
	for _, o := range opts {
		o(&e)
	}
	l.mu.Lock()
	l.seq++
	e.Seq = l.seq
	l.entries = append(l.entries, e)
	if len(l.entries) > l.ringSize {
		l.entries = append([]Entry(nil), l.entries[len(l.entries)-l.ringSize:]...)
	}
	sink := l.sink
	l.mu.Unlock()
	if sink != nil {
		_ = sink.Write(e) // sink errors must not break the operation
	}
	return e
}

// Query returns recent entries, newest first, optionally filtered by actor.
func (l *Logger) Query(limit int, actor string) []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Entry, 0, len(l.entries))
	for i := len(l.entries) - 1; i >= 0; i-- {
		e := l.entries[i]
		if actor != "" && e.Actor != actor {
			continue
		}
		out = append(out, e)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// Option mutates an entry before storage.
type Option func(*Entry)

// WithCategory sets the error category.
func WithCategory(cat string) Option {
	return func(e *Entry) { e.Category = cat }
}

// WithResource sets the resource name.
func WithResource(r string) Option {
	return func(e *Entry) { e.Resource = r }
}

// WithVersions records from/to config versions.
func WithVersions(from, to int64) Option {
	return func(e *Entry) { e.FromVer, e.ToVer = from, to }
}

// WithReason sets a human readable reason.
func WithReason(r string) Option {
	return func(e *Entry) { e.Reason = r }
}

// WithDetail sets extra detail.
func WithDetail(d string) Option {
	return func(e *Entry) { e.Detail = d }
}
