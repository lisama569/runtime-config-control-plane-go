package source

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

// FileSource is a JSON file on local disk with explicit name and priority.
type FileSource struct {
	name     string
	priority int
	path     string

	mu        sync.Mutex
	version   int64 // monotonically bumped on each accepted change
	signature string
	loaded    bool
}

// NewFileSource constructs a file source.
func NewFileSource(name string, priority int, path string) *FileSource {
	return &FileSource{name: name, priority: priority, path: path}
}

func (f *FileSource) Name() string   { return f.name }
func (f *FileSource) Priority() int  { return f.priority }
func (f *FileSource) Path() string   { return f.path }
func (f *FileSource) Version() int64 { f.mu.Lock(); defer f.mu.Unlock(); return f.version }

// Load reads and parses the file. The source's own content version is bumped
// ONLY when the parse succeeds AND content changed since the last accepted
// load: illegal, missing or type-incompatible files are rejected and the
// last valid state remains intact (caller retains the old snapshot anyway,
// since a failed Load is not published anywhere).
func (f *FileSource) Load(ctx context.Context) (*config.Document, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	data, err := os.ReadFile(f.path)
	if err != nil {
		return nil, 0, errs.Wrap(errs.KindValidation,
			fmt.Sprintf("source %q: cannot read %s", f.name, f.path), err)
	}
	var raw config.RawDocument
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, 0, errs.Wrap(errs.KindValidation,
			fmt.Sprintf("source %q: invalid JSON in %s", f.name, f.path), err)
	}
	doc, err := config.Parse(&raw)
	if err != nil {
		return nil, 0, errs.Wrap(errs.Classify(err),
			fmt.Sprintf("source %q: %s", f.name, err.Error()), err)
	}
	sig := string(data)
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.loaded {
		f.loaded = true
		f.signature = sig
		f.version = 1
	} else if sig != f.signature {
		f.signature = sig
		f.version++
	}
	return doc, f.version, nil
}

// statMtime returns the file's modification time, or zero time.
func (f *FileSource) statMtime() (time.Time, error) {
	fi, err := os.Stat(f.path)
	if err != nil {
		return time.Time{}, err
	}
	return fi.ModTime(), nil
}

// readSig returns the file content as a change signature.
func readSig(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
