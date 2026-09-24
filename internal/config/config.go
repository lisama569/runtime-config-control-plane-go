package config

// Config is the effective configuration document: feature flags plus
// arbitrary key/value settings.
type Config struct {
	Flags    map[string]Flag    `json:"flags"`
	Settings map[string]Setting `json:"settings"`
}

// Flag describes one feature toggle and its rollout policy.
type Flag struct {
	Name        string            `json:"name"`
	Enabled     bool              `json:"enabled"`
	Description string            `json:"description,omitempty"`
	Rollout     Rollout           `json:"rollout"`
	Variants    map[string]uint32 `json:"variants,omitempty"`
}

// Rollout defines a percentage based gradual rollout and an allow/deny
// list of subject identifiers.
type Rollout struct {
	Percentage uint32   `json:"percentage"` // 0..100, inclusive boundary
	AllowList  []string `json:"allow_list,omitempty"`
	DenyList   []string `json:"deny_list,omitempty"`
}

// Setting is one configuration value entry. Secret values are masked when
// read by callers without read-secret permission.
type Setting struct {
	Value   any  `json:"value"`
	Type    Type `json:"type"`
	Secret  bool `json:"secret,omitempty"`
	Version int  `json:"version,omitempty"`
}

// Type enumerates the value types a setting may carry.
type Type string

const (
	TypeString Type = "string"
	TypeInt    Type = "int"
	TypeFloat  Type = "float"
	TypeBool   Type = "bool"
)

// Snapshot is one immutable versioned view of a Config.
type Snapshot struct {
	Version   int64  `json:"version"`
	Config    Config `json:"config"`
	Source    string `json:"source,omitempty"` // human readable merge provenance
	Reason    string `json:"reason,omitempty"`
	CreatedAt int64  `json:"created_at"` // unix seconds
}

// Empty returns a zero-value, structurally non-nil Config.
func Empty() Config {
	return Config{
		Flags:    map[string]Flag{},
		Settings: map[string]Setting{},
	}
}

// Clone returns a deep copy of the Config. The Value field of settings is
// copied by value (it only carries JSON scalars/[]any/map[string]any in
// this project; maps/slices inside are cloned via cloneValue).
func (c Config) Clone() Config {
	out := Config{
		Flags:    make(map[string]Flag, len(c.Flags)),
		Settings: make(map[string]Setting, len(c.Settings)),
	}
	for k, f := range c.Flags {
		cp := f
		if f.Rollout.AllowList != nil {
			cp.Rollout.AllowList = append([]string(nil), f.Rollout.AllowList...)
		}
		if f.Rollout.DenyList != nil {
			cp.Rollout.DenyList = append([]string(nil), f.Rollout.DenyList...)
		}
		if f.Variants != nil {
			cp.Variants = make(map[string]uint32, len(f.Variants))
			for vk, vv := range f.Variants {
				cp.Variants[vk] = vv
			}
		}
		out.Flags[k] = cp
	}
	for k, s := range c.Settings {
		s.Value = cloneValue(s.Value)
		out.Settings[k] = s
	}
	return out
}

func cloneValue(v any) any {
	switch t := v.(type) {
	case []any:
		cp := make([]any, len(t))
		for i := range t {
			cp[i] = cloneValue(t[i])
		}
		return cp
	case map[string]any:
		cp := make(map[string]any, len(t))
		for k, vv := range t {
			cp[k] = cloneValue(vv)
		}
		return cp
	default:
		return v // scalars are immutable by convention
	}
}
