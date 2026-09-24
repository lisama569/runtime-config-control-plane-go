package source

import "context"

// RawConfig is the document as loaded from one origin, before merging.
type RawConfig struct {
	Flags    []RawFlag    `json:"flags"`
	Settings []RawSetting `json:"settings"`
}

// RawFlag is a flag entry together with its owning source name.
type RawFlag struct {
	Source string
	Flag   map[string]any
}

// RawSetting is a setting entry together with its owning source name.
type RawSetting struct {
	Source  string
	Key     string
	Setting map[string]any
}

// Source represents one configuration origin (file, env, memory...).
type Source interface {
	// Name returns the stable, unique source identifier.
	Name() string
	// Load reads the current document of this source.
	Load(ctx context.Context) (RawConfig, error)
}

// ParsedSource groups a loaded document with its source identity.
type ParsedSource struct {
	Name    string
	Raw     RawConfig
	Version int64 // source-local revision, 0 when unsupported
}
