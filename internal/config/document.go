package config

import (
	"fmt"

	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

// Snapshot is an immutable point-in-time effective configuration.
// Every read in the process observes either the previous snapshot or the
// new snapshot in full, never an intermediate state.
type Snapshot struct {
	Version int64
	Doc     *Document
	// Origin explains which source supplied each effective entry.
	PropertyOrigin map[string]string
	FeatureOrigin  map[string]string
	// SourceVersions records the per-source content version at merge time,
	// making precedence auditable and explainable.
	SourceVersions map[string]int64
	CreatedAtUnix  int64
}

func (s *Snapshot) typed(key string, want ValueKind) (Property, error) {
	prop, ok := s.Doc.Properties[key]
	if !ok {
		return Property{}, errs.New(errs.KindNotFound,
			fmt.Sprintf("config: property %q not found at version %d", key, s.Version))
	}
	if prop.Value.Kind != want {
		return Property{}, errs.New(errs.KindTypeMismatch,
			fmt.Sprintf("config: property %q is %s, not %s (version %d)",
				key, prop.Value.Kind, want, s.Version))
	}
	return prop, nil
}

// GetString returns a string property or a classified missing/type error.
func (s *Snapshot) GetString(key string) (string, error) {
	p, err := s.typed(key, KindString)
	if err != nil {
		return "", err
	}
	return p.Value.Str, nil
}

// GetInt returns an int property or a classified missing/type error.
func (s *Snapshot) GetInt(key string) (int64, error) {
	p, err := s.typed(key, KindInt)
	if err != nil {
		return 0, err
	}
	return p.Value.Int, nil
}

// GetFloat returns a float property or a classified missing/type error.
func (s *Snapshot) GetFloat(key string) (float64, error) {
	p, err := s.typed(key, KindFloat)
	if err != nil {
		return 0, err
	}
	return p.Value.Flt, nil
}

// GetBool returns a bool property or a classified missing/type error.
func (s *Snapshot) GetBool(key string) (bool, error) {
	p, err := s.typed(key, KindBool)
	if err != nil {
		return false, err
	}
	return p.Value.Bool, nil
}

// Sensitive returns a sensitive property's raw secret. Callers must be
// authorized; the value is never rendered by Display/views.
func (s *Snapshot) Sensitive(key string) (string, error) {
	p, err := s.typed(key, KindSensitive)
	if err != nil {
		return "", err
	}
	return p.Value.RawSecret, nil
}

// Feature returns the named feature definition.
func (s *Snapshot) Feature(name string) (Feature, bool) {
	f, ok := s.Doc.Features[name]
	return f, ok
}
