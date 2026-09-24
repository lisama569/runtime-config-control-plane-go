package audit

import (
	"encoding/json"
	"os"
	"sync"
)

// FileSink appends entries as JSON lines to a file.
type FileSink struct {
	mu sync.Mutex
	f  *os.File
}

// NewFileSink opens (or creates) an append-only JSONL sink at path.
func NewFileSink(path string) (*FileSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &FileSink{f: f}, nil
}

// Write implements Sink.
func (s *FileSink) Write(e Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = s.f.Write(append(b, '\n'))
	return err
}

// Close releases the underlying file.
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}
