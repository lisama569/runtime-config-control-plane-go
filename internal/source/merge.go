package source

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
)

// MergeResult is the output of merging multiple sources.
type MergeResult struct {
	Config     config.Config
	Provenance map[string]string // key/flag name -> winning source
}

// Summary returns a compact, stable description of the merge provenance.
func (m MergeResult) Summary() string {
	keys := make([]string, 0, len(m.Provenance))
	for k := range m.Provenance {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s;", k, m.Provenance[k])
	}
	return b.String()
}

// Merge combines documents; the first source in sources has the highest
// priority. Lower priority entries never shadow higher ones. Any
// structural or semantic error fails the whole merge so callers never
// observe a half-updated document.
func Merge(sources []ParsedSource) (MergeResult, error) {
	merged := config.Empty()
	provenance := map[string]string{}

	for _, ps := range sources {
		for _, rf := range ps.Raw.Flags {
			fl, err := decodeFlag(rf.Flag)
			if err != nil {
				return MergeResult{}, fmt.Errorf("source %q: %w", ps.Name, err)
			}
			if _, exists := merged.Flags[fl.Name]; exists {
				continue // higher priority source already defined it
			}
			merged.Flags[fl.Name] = fl
			provenance["flag:"+fl.Name] = ps.Name
		}
		for _, rs := range ps.Raw.Settings {
			if _, exists := merged.Settings[rs.Key]; exists {
				continue
			}
			st, err := decodeSetting(rs.Setting)
			if err != nil {
				return MergeResult{}, fmt.Errorf("source %q setting %q: %w", ps.Name, rs.Key, err)
			}
			merged.Settings[rs.Key] = st
			provenance["setting:"+rs.Key] = ps.Name
		}
	}
	if err := config.Validate(merged); err != nil {
		return MergeResult{}, fmt.Errorf("merged config invalid: %w", err)
	}
	return MergeResult{Config: merged, Provenance: provenance}, nil
}

// LoadAll loads every source in priority order.
func LoadAll(ctx context.Context, sources []Source) ([]ParsedSource, error) {
	out := make([]ParsedSource, 0, len(sources))
	for _, s := range sources {
		raw, err := s.Load(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, ParsedSource{Name: s.Name(), Raw: raw})
	}
	return out, nil
}

func decodeFlag(m map[string]any) (config.Flag, error) {
	name, _ := m["name"].(string)
	if name == "" {
		return config.Flag{}, fmt.Errorf("flag missing name")
	}
	fl := config.Flag{Name: name}
	if v, ok := m["enabled"].(bool); ok {
		fl.Enabled = v
	}
	if v, ok := m["description"].(string); ok {
		fl.Description = v
	}
	if rm, ok := m["rollout"].(map[string]any); ok {
		p, err := toUint32(rm["percentage"])
		if err != nil {
			return config.Flag{}, fmt.Errorf("flag %q: rollout percentage: %w", name, err)
		}
		fl.Rollout.Percentage = p
		if al, ok := rm["allow_list"].([]any); ok {
			for _, s := range al {
				str, ok := s.(string)
				if !ok {
					return config.Flag{}, fmt.Errorf("flag %q: allow_list entries must be strings", name)
				}
				fl.Rollout.AllowList = append(fl.Rollout.AllowList, str)
			}
		}
		if dl, ok := rm["deny_list"].([]any); ok {
			for _, s := range dl {
				str, ok := s.(string)
				if !ok {
					return config.Flag{}, fmt.Errorf("flag %q: deny_list entries must be strings", name)
				}
				fl.Rollout.DenyList = append(fl.Rollout.DenyList, str)
			}
		}
	}
	if vm, ok := m["variants"].(map[string]any); ok {
		fl.Variants = make(map[string]uint32, len(vm))
		for k, v := range vm {
			w, err := toUint32(v)
			if err != nil {
				return config.Flag{}, fmt.Errorf("flag %q: variant %q: %w", name, k, err)
			}
			fl.Variants[k] = w
		}
	}
	return fl, nil
}

func decodeSetting(m map[string]any) (config.Setting, error) {
	var st config.Setting
	t, _ := m["type"].(string)
	st.Type = config.Type(t)
	if v, ok := m["secret"].(bool); ok {
		st.Secret = v
	}
	if v, ok := m["version"]; ok {
		iv, err := toInt(v)
		if err != nil {
			return config.Setting{}, fmt.Errorf("version: %w", err)
		}
		st.Version = int(iv)
	}
	val, ok := m["value"]
	if !ok {
		return config.Setting{}, fmt.Errorf("value is missing")
	}
	cv, err := convertValue(val, st.Type)
	if err != nil {
		return config.Setting{}, err
	}
	st.Value = cv
	return st, nil
}

// convertValue normalizes a decoded JSON value to the Go representation
// expected by config.Setting (float64 for numbers, string, bool).
func convertValue(v any, t config.Type) (any, error) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid number %q: %w", n.String(), err)
		}
		return f, nil
	case string:
		if t == config.TypeInt || t == config.TypeFloat {
			return nil, fmt.Errorf("expected number, got string %q", n)
		}
		return n, nil
	case bool:
		return n, nil
	default:
		return nil, fmt.Errorf("unsupported value type %T", v)
	}
}

func toUint32(v any) (uint32, error) {
	i, err := toInt(v)
	if err != nil {
		return 0, err
	}
	if i < 0 || i > math_MaxUint32 {
		return 0, fmt.Errorf("value %d out of uint32 range", i)
	}
	return uint32(i), nil
}

const math_MaxUint32 = 1<<32 - 1

func toInt(v any) (int64, error) {
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		if err == nil {
			return i, nil
		}
		f, ferr := n.Float64()
		if ferr != nil {
			return 0, fmt.Errorf("invalid number %q", n.String())
		}
		if f != float64(int64(f)) {
			return 0, fmt.Errorf("value %v is not an integer", f)
		}
		return int64(f), nil
	case float64:
		if n != float64(int64(n)) {
			return 0, fmt.Errorf("value %v is not an integer", n)
		}
		return int64(n), nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("expected number, got %T", v)
	}
}
