// Package config defines the normalized configuration model shared by
// sources, merging, the atomic store and the decision layer.
package config

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

// ValueKind enumerates the supported scalar value types.
type ValueKind string

const (
	KindString    ValueKind = "string"
	KindInt       ValueKind = "int"
	KindFloat     ValueKind = "float"
	KindBool      ValueKind = "bool"
	KindSensitive ValueKind = "sensitive"
)

// Value is a typed scalar property value.
type Value struct {
	Kind ValueKind
	Str  string
	Int  int64
	Flt  float64
	Bool bool
	// RawSecret is non-empty only for sensitive values; it is never serialized
	// by default and masked by view builders.
	RawSecret string
}

// Display renders the value for explainable responses. Sensitive values are
// masked at this layer too, so they can never leak by accident.
func (v Value) Display() string {
	switch v.Kind {
	case KindString:
		return v.Str
	case KindInt:
		return strconv.FormatInt(v.Int, 10)
	case KindFloat:
		return strconv.FormatFloat(v.Flt, 'g', -1, 64)
	case KindBool:
		return strconv.FormatBool(v.Bool)
	case KindSensitive:
		return "********"
	default:
		return ""
	}
}

// Property is one configuration entry.
type Property struct {
	Key     string
	Value   Value
	Version int64 // per-property version (bumped when the property changes)
}

// Feature describes a feature flag / gradual rollout rule.
type Feature struct {
	Name       string
	Enabled    bool
	Percentage int // [0,100]; deterministic stable-subject hashing
	AllowList  map[string]bool
}

// Document is a fully parsed normalized configuration.
type Document struct {
	Properties map[string]Property
	Features   map[string]Feature
}

// RawDocument is the on-disk / on-wire JSON representation.
type RawDocument struct {
	Version    int64                      `json:"version"`
	Properties map[string]json.RawMessage `json:"properties,omitempty"`
	Features   map[string]json.RawMessage `json:"features,omitempty"`
}

// RawProperty is a single raw property entry.
type RawProperty struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

// RawFeature is a single raw feature entry.
type RawFeature struct {
	Enabled    bool     `json:"enabled"`
	Percentage int      `json:"percentage,omitempty"`
	AllowList  []string `json:"allow_list,omitempty"`
}

// Parse converts a RawDocument into a validated Document.
// Invalid entries, missing values or type mismatches produce classified
// errors and never a partially usable document: parsing is all-or-nothing.
func Parse(raw *RawDocument) (*Document, error) {
	if raw == nil {
		return nil, errs.New(errs.KindValidation, "config: raw document is nil")
	}
	doc := &Document{
		Properties: make(map[string]Property, len(raw.Properties)),
		Features:   make(map[string]Feature, len(raw.Features)),
	}
	for key, rawMsg := range raw.Properties {
		if key == "" {
			return nil, errs.New(errs.KindValidation, "config: property key must not be empty")
		}
		prop, err := parseProperty(key, rawMsg)
		if err != nil {
			return nil, err
		}
		doc.Properties[key] = prop
	}
	for name, rawMsg := range raw.Features {
		if name == "" {
			return nil, errs.New(errs.KindValidation, "config: feature name must not be empty")
		}
		feat, err := parseFeature(name, rawMsg)
		if err != nil {
			return nil, err
		}
		doc.Features[name] = feat
	}
	return doc, nil
}

func parseProperty(key string, raw json.RawMessage) (Property, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return Property{}, errs.New(errs.KindMissingValue,
			fmt.Sprintf("config: property %q has no value", key))
	}
	var rp RawProperty
	if err := json.Unmarshal(raw, &rp); err != nil {
		return Property{}, errs.Wrap(errs.KindValidation,
			fmt.Sprintf("config: property %q is not a valid object", key), err)
	}
	if rp.Type == "" {
		return Property{}, errs.New(errs.KindValidation,
			fmt.Sprintf("config: property %q is missing type", key))
	}
	if len(rp.Value) == 0 || string(rp.Value) == "null" {
		return Property{}, errs.New(errs.KindMissingValue,
			fmt.Sprintf("config: property %q has null/missing value", key))
	}
	v := Value{Kind: ValueKind(rp.Type)}
	switch v.Kind {
	case KindString, KindSensitive:
		var s string
		if err := json.Unmarshal(rp.Value, &s); err != nil {
			return Property{}, errs.Wrap(errs.KindTypeMismatch,
				fmt.Sprintf("config: property %q expects %s but value is not a JSON string", key, rp.Type), err)
		}
		if v.Kind == KindString {
			v.Str = s
		} else {
			v.RawSecret = s
		}
	case KindInt:
		n, err := parseIntegral(rp.Value)
		if err != nil {
			return Property{}, errs.Wrap(errs.KindTypeMismatch,
				fmt.Sprintf("config: property %q expects int but value is not an integral JSON number", key), err)
		}
		v.Int = n
	case KindFloat:
		var f float64
		if err := json.Unmarshal(rp.Value, &f); err != nil {
			return Property{}, errs.Wrap(errs.KindTypeMismatch,
				fmt.Sprintf("config: property %q expects float but value is not a JSON number", key), err)
		}
		v.Flt = f
	case KindBool:
		var b bool
		if err := json.Unmarshal(rp.Value, &b); err != nil {
			return Property{}, errs.Wrap(errs.KindTypeMismatch,
				fmt.Sprintf("config: property %q expects bool but value is not a JSON boolean", key), err)
		}
		v.Bool = b
	default:
		return Property{}, errs.New(errs.KindValidation,
			fmt.Sprintf("config: property %q has unsupported type %q", key, rp.Type))
	}
	return Property{Key: key, Value: v, Version: 1}, nil
}

// parseIntegral accepts only JSON numbers with no fractional component.
func parseIntegral(raw json.RawMessage) (int64, error) {
	var n json.Number
	dec := json.NewDecoder(newBytesReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&n); err != nil {
		return 0, err
	}
	i, err := strconv.ParseInt(n.String(), 10, 64)
	if err != nil {
		// Distinguish genuine non-numbers from floats-with-fractions: both are
		// type mismatches for an int property.
		if f, ferr := strconv.ParseFloat(n.String(), 64); ferr == nil && f == float64(int64(f)) {
			return int64(f), nil
		}
		return 0, err
	}
	return i, nil
}

func parseFeature(name string, raw json.RawMessage) (Feature, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return Feature{}, errs.New(errs.KindMissingValue,
			fmt.Sprintf("config: feature %q has no definition", name))
	}
	var rf RawFeature
	if err := json.Unmarshal(raw, &rf); err != nil {
		return Feature{}, errs.Wrap(errs.KindValidation,
			fmt.Sprintf("config: feature %q is not a valid object", name), err)
	}
	if rf.Percentage < 0 || rf.Percentage > 100 {
		return Feature{}, errs.New(errs.KindValidation,
			fmt.Sprintf("config: feature %q percentage %d out of range [0,100]", name, rf.Percentage))
	}
	f := Feature{
		Name:       name,
		Enabled:    rf.Enabled,
		Percentage: rf.Percentage,
		AllowList:  make(map[string]bool, len(rf.AllowList)),
	}
	for _, subj := range rf.AllowList {
		if subj == "" {
			return Feature{}, errs.New(errs.KindValidation,
				fmt.Sprintf("config: feature %q allow_list contains empty subject", name))
		}
		f.AllowList[subj] = true
	}
	return f, nil
}

// Encode serializes a Document back into a RawDocument at the given version.
// Sensitive secrets are omitted from output; callers that need round-tripping
// use EncodeWithSecrets.
func Encode(doc *Document, version int64) *RawDocument {
	return encode(doc, version, false)
}

// EncodeWithSecrets serializes including sensitive raw secret strings.
func EncodeWithSecrets(doc *Document, version int64) *RawDocument {
	return encode(doc, version, true)
}

func encode(doc *Document, version int64, withSecrets bool) *RawDocument {
	raw := &RawDocument{
		Version:    version,
		Properties: make(map[string]json.RawMessage, len(doc.Properties)),
		Features:   make(map[string]json.RawMessage, len(doc.Features)),
	}
	for key, prop := range doc.Properties {
		rp := RawProperty{Type: string(prop.Value.Kind)}
		switch prop.Value.Kind {
		case KindString:
			rp.Value, _ = json.Marshal(prop.Value.Str)
		case KindSensitive:
			if withSecrets {
				rp.Value, _ = json.Marshal(prop.Value.RawSecret)
			} else {
				rp.Value, _ = json.Marshal("")
			}
		case KindInt:
			rp.Value, _ = json.Marshal(prop.Value.Int)
		case KindFloat:
			rp.Value, _ = json.Marshal(prop.Value.Flt)
		case KindBool:
			rp.Value, _ = json.Marshal(prop.Value.Bool)
		}
		raw.Properties[key], _ = json.Marshal(rp)
	}
	for name, feat := range doc.Features {
		rf := RawFeature{Enabled: feat.Enabled, Percentage: feat.Percentage}
		for subj := range feat.AllowList {
			rf.AllowList = append(rf.AllowList, subj)
		}
		sortStrings(rf.AllowList)
		raw.Features[name], _ = json.Marshal(rf)
	}
	return raw
}
