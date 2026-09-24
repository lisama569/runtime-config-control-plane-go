// Package source provides local configuration sources with stable identity,
// versioning and hot-reload watching.
package source

import (
	"context"
	"time"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
)

// Source is one local configuration origin.
type Source interface {
	Name() string
	// Priority reports the explicit precedence (lower = higher priority).
	Priority() int
	Load(ctx context.Context) (*config.Document, int64, error)
	// Version returns the last observed source content version.
	Version() int64
}

// UpdateEvent is emitted when a watched source changes and reloads validly.
type UpdateEvent struct {
	Source  Source
	Version int64
	Doc     *config.Document
}

// LoadError reports a rejected reload attempt. The previous valid snapshot is
// retained by the control plane; this event exists for audit/logging only.
type LoadError struct {
	SourceName string
	Err        error
}

// Watcher polls watched files and emits valid reload events; invalid reload
// candidates are rejected (reported on Errors) and never published.
type Watcher struct {
	sources  []Source
	interval time.Duration
	Errors   chan LoadError
}

// NewWatcher constructs a Watcher for the given sources and poll interval.
// A non-positive interval defaults to one second.
func NewWatcher(sources []Source, interval time.Duration) *Watcher {
	if interval <= 0 {
		interval = time.Second
	}
	return &Watcher{
		sources:  sources,
		interval: interval,
		Errors:   make(chan LoadError, 16),
	}
}

// Run polls every source's mtime until ctx is canceled. A changed file is
// reloaded through Source.Load, which itself rejects invalid content; on
// success the new document is delivered to events.
func (w *Watcher) Run(ctx context.Context, events chan<- UpdateEvent) error {
	fsrcs := make([]*FileSource, 0, len(w.sources))
	for _, src := range w.sources {
		if fs, ok := src.(*FileSource); ok {
			fsrcs = append(fsrcs, fs)
		}
	}
	mtimes := map[string]time.Time{}
	sigs := map[string]string{}
	for _, fs := range fsrcs {
		if mt, err := fs.statMtime(); err == nil {
			mtimes[fs.Name()] = mt
		}
		if b, err := readSig(fs.path); err == nil {
			sigs[fs.Name()] = b
		}
	}
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			for _, src := range w.sources {
				fs, ok := src.(*FileSource)
				if !ok {
					continue
				}
				mt, err := fs.statMtime()
				if err != nil {
					continue // file disappeared: keep last valid state
				}
				sig, sigErr := readSig(fs.path)
				if sigErr != nil {
					continue
				}
				if mt.Equal(mtimes[fs.Name()]) && sig == sigs[fs.Name()] {
					continue
				}
				mtimes[fs.Name()] = mt
				sigs[fs.Name()] = sig
				doc, ver, lerr := src.Load(ctx)
				if lerr != nil {
					// Reject: do not update mtime baseline? Keep baseline so we
					// do not re-spam; an additional save later changes mtime.
					select {
					case w.Errors <- LoadError{SourceName: src.Name(), Err: lerr}:
					default:
					}
					continue
				}
				select {
				case events <- UpdateEvent{Source: src, Version: ver, Doc: doc}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
}
