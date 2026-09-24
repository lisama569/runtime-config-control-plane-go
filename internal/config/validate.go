package config

import (
	"fmt"
	"math"
)

// Validate checks a Config for semantic errors: unknown types, out of
// range rollout percentages, non-positive variant weights, allow/deny
// overlap and setting value/type compatibility.
func Validate(c Config) error {
	for name, f := range c.Flags {
		if name == "" {
			return NewError(CatValidation, "flag name must not be empty")
		}
		if f.Name != "" && f.Name != name {
			return NewError(CatValidation, "flag %q: embedded name %q does not match key", name, f.Name)
		}
		r := f.Rollout
		if r.Percentage > 100 {
			return NewError(CatValidation, "flag %q: rollout percentage %d out of range 0..100", name, r.Percentage)
		}
		deny := make(map[string]struct{}, len(r.DenyList))
		for _, s := range r.DenyList {
			if s == "" {
				return NewError(CatValidation, "flag %q: deny list contains empty subject", name)
			}
			deny[s] = struct{}{}
		}
		for _, s := range r.AllowList {
			if s == "" {
				return NewError(CatValidation, "flag %q: allow list contains empty subject", name)
			}
			if _, ok := deny[s]; ok {
				return NewError(CatValidation, "flag %q: subject %q present on both allow and deny lists", name, s)
			}
		}
		for variant, w := range f.Variants {
			if variant == "" {
				return NewError(CatValidation, "flag %q: variant name must not be empty", name)
			}
			if w == 0 {
				return NewError(CatValidation, "flag %q: variant %q weight must be > 0", name, variant)
			}
		}
	}
	for key, s := range c.Settings {
		if key == "" {
			return NewError(CatValidation, "setting key must not be empty")
		}
		if err := validateValue(key, s); err != nil {
			return err
		}
	}
	return nil
}

func validateValue(key string, s Setting) error {
	if s.Value == nil {
		return NewError(CatValidation, "setting %q: value is missing/null", key)
	}
	switch s.Type {
	case TypeString:
		if _, ok := s.Value.(string); !ok {
			return NewError(CatTypeMismatch, "setting %q: expected string, got %T", key, s.Value)
		}
	case TypeBool:
		if _, ok := s.Value.(bool); !ok {
			return NewError(CatTypeMismatch, "setting %q: expected bool, got %T", key, s.Value)
		}
	case TypeInt:
		f, ok := s.Value.(float64) // encoding/json decodes numbers as float64
		if !ok {
			return NewError(CatTypeMismatch, "setting %q: expected int, got %T", key, s.Value)
		}
		if math.Trunc(f) != f || f > math.MaxInt64 || f < math.MinInt64 {
			return NewError(CatTypeMismatch, "setting %q: value %v is not an integer", key, s.Value)
		}
	case TypeFloat:
		if _, ok := s.Value.(float64); !ok {
			return NewError(CatTypeMismatch, "setting %q: expected number, got %T", key, s.Value)
		}
	case "":
		return NewError(CatValidation, "setting %q: type is missing", key)
	default:
		return NewError(CatValidation, "setting %q: unknown type %q", key, s.Type)
	}
	if s.Version < 0 {
		return NewError(CatValidation, "setting %q: version must be >= 0", key)
	}
	return nil
}

var _ = fmt.Sprintf
