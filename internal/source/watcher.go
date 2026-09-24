package source

import (
	"context"
	"time"
)

// ChangeEvent is emitted when a reload produced a new merged document.
// Err is set when the changed files failed to load; Parsed is nil then.
type ChangeEvent struct {
	Parsed []ParsedSource
	Err    error
}

// Watcher periodically polls file sources and reports changed documents.
// Sources that fail to load never silently replace the last good state:
// the error is surfaced through ChangeEvent.Err.
type Watcher struct {
	sources  []Source
	interval time.Duration
}

// NewWatcher constructs a polling watcher.
func NewWatcher(interval time.Duration, sources ...Source) *Watcher {
	return &Watcher{sources: sources, interval: interval}
}

// Run blocks until ctx is cancelled, invoking onChange after every
// detected change. The initial state is not emitted; the caller is
// expected to perform the initial load itself.
func (w *Watcher) Run(ctx context.Context, onChange func(ChangeEvent)) error {
	if w.interval <= 0 {
		w.interval = time.Second
	}
	last := w.fingerprint()
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			fp := w.fingerprint()
			if fp == last {
				continue
			}
			last = fp
			parsed, err := LoadAll(ctx, w.sources)
			if err != nil {
				onChange(ChangeEvent{Err: err})
				continue
			}
			onChange(ChangeEvent{Parsed: parsed})
		}
	}
}

// fingerprint hashes file modification times and sizes of file sources.
func (w *Watcher) fingerprint() string {
	var fp string
	for _, s := range w.sources {
		fs, ok := s.(*FileSource)
		if !ok {
			continue
		}
		info, err := fs.Stat()
		if err != nil {
			fp += fs.Path() + ":missing;"
			continue
		}
		fp += fs.Path() + ":" + info.ModTime().String() + ":" + itoa(info.Size()) + ";"
	}
	return fp
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
