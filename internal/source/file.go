package source

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

// FileSource loads a JSON document from disk. The document uses the
// on-disk wire format (flag/setting arrays); structural decoding errors
// are returned to the caller so they cannot overwrite good config.
type FileSource struct {
	name string
	path string
}

// fileDocument is the on-disk JSON shape.
type fileDocument struct {
	Flags    []map[string]any `json:"flags"`
	Settings []fileSetting    `json:"settings"`
}

type fileSetting struct {
	Key     string         `json:"key"`
	Secret  bool           `json:"secret"`
	Setting map[string]any `json:"setting"`
}

// NewFileSource creates a file backed source.
func NewFileSource(name, path string) (*FileSource, error) {
	if name == "" {
		return nil, fmt.Errorf("source name must not be empty")
	}
	if path == "" {
		return nil, fmt.Errorf("source %q path must not be empty", name)
	}
	return &FileSource{name: name, path: path}, nil
}

func (f *FileSource) Name() string { return f.name }

// Path returns the configured file path.
func (f *FileSource) Path() string { return f.path }

func (f *FileSource) Load(ctx context.Context) (RawConfig, error) {
	if err := ctx.Err(); err != nil {
		return RawConfig{}, err
	}
	data, err := os.ReadFile(f.path)
	if err != nil {
		return RawConfig{}, fmt.Errorf("source %q: read %s: %w", f.name, f.path, err)
	}
	var doc fileDocument
	dec := json.NewDecoder(newBytesReader(data))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return RawConfig{}, fmt.Errorf("source %q: parse %s: %w", f.name, f.path, err)
	}
	raw := RawConfig{
		Flags:    make([]RawFlag, 0, len(doc.Flags)),
		Settings: make([]RawSetting, 0, len(doc.Settings)),
	}
	for _, fg := range doc.Flags {
		raw.Flags = append(raw.Flags, RawFlag{Source: f.name, Flag: fg})
	}
	for _, st := range doc.Settings {
		if st.Key == "" {
			return RawConfig{}, fmt.Errorf("source %q: setting without key", f.name)
		}
		setting := st.Setting
		if setting == nil {
			setting = map[string]any{}
		}
		if st.Secret {
			setting["secret"] = true
		}
		raw.Settings = append(raw.Settings, RawSetting{Source: f.name, Key: st.Key, Setting: setting})
	}
	return raw, nil
}

// Stat returns the file modification time and size.
func (f *FileSource) Stat() (os.FileInfo, error) {
	return os.Stat(f.path)
}
