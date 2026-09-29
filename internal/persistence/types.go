// Package persistence provides crash-safe on-disk durability for the
// control plane's committed configuration state and audit trail.
//
// State is written as complete, checksummed records via
// temp-file + fsync + atomic rename, so a process killed mid-write can never
// leave a half-written commit that later looks valid; two generations are
// retained so a corrupted newest file can fall back to the last complete one.
// Audit entries live in a checksummed append-only framed journal whose torn
// tail is truncated on recovery.
package persistence

import (
	"sort"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
)

// ValueDTO is the durable form of a typed scalar property value.
type ValueDTO struct {
	Kind      string  `json:"kind"`
	Str       string  `json:"str,omitempty"`
	Int       int64   `json:"int,omitempty"`
	Flt       float64 `json:"flt,omitempty"`
	Bool      bool    `json:"bool,omitempty"`
	RawSecret string  `json:"raw_secret,omitempty"`
}

// PropertyDTO is the durable form of one property entry.
type PropertyDTO struct {
	Key     string   `json:"key"`
	Value   ValueDTO `json:"value"`
	Version int64    `json:"version"`
}

// FeatureDTO is the durable form of one feature definition.
type FeatureDTO struct {
	Name       string   `json:"name"`
	Enabled    bool     `json:"enabled"`
	Percentage int      `json:"percentage"`
	AllowList  []string `json:"allow_list,omitempty"`
}

// SnapshotDTO is the durable form of one committed effective snapshot,
// including provenance needed to keep reads explainable after recovery.
type SnapshotDTO struct {
	Version        int64             `json:"version"`
	Properties     []PropertyDTO     `json:"properties"`
	Features       []FeatureDTO      `json:"features"`
	PropertyOrigin map[string]string `json:"property_origin"`
	FeatureOrigin  map[string]string `json:"feature_origin"`
	SourceVersions map[string]int64  `json:"source_versions"`
	CreatedAtUnix  int64             `json:"created_at_unix"`
}

// OverrideDTO is the durable form of the control plane's API override layer
// for one effective version. Tombstones express deletions.
type OverrideDTO struct {
	Version   int64         `json:"version"`
	Props     []PropertyDTO `json:"props,omitempty"`
	Feats     []FeatureDTO  `json:"feats,omitempty"`
	DeadProps []string      `json:"dead_props,omitempty"`
	DeadFeats []string      `json:"dead_feats,omitempty"`
}

// State is the complete durable state at one point in time. Every save
// rewrites the whole state atomically.
type State struct {
	// SchemaVersion allows future format evolution.
	SchemaVersion int `json:"schema_version"`
	// CurrentVersion is the version of the last committed snapshot.
	CurrentVersion int64 `json:"current_version"`
	// History holds retained committed snapshots, oldest first.
	History []SnapshotDTO `json:"history"`
	// Overrides holds the override layer per retained effective version.
	Overrides []OverrideDTO `json:"overrides"`
	// Checksum is set by the writer (SHA-256 over the JSON without this
	// field) and verified on load.
	Checksum string `json:"checksum"`
}

// CurrentSchemaVersion is the only format version produced/accepted now.
const CurrentSchemaVersion = 1

// MaxStateBytes bounds a single state file. Persisting beyond it fails with
// PERSIST_FAILED instead of letting disk usage grow without bound.
const MaxStateBytes = 16 << 20 // 16 MiB

// PropertyFromModel converts a runtime property into a durable DTO.
func PropertyFromModel(prop config.Property) PropertyDTO {
	return PropertyDTO{
		Key: prop.Key,
		Value: ValueDTO{
			Kind:      string(prop.Value.Kind),
			Str:       prop.Value.Str,
			Int:       prop.Value.Int,
			Flt:       prop.Value.Flt,
			Bool:      prop.Value.Bool,
			RawSecret: prop.Value.RawSecret,
		},
		Version: prop.Version,
	}
}

// ToModel converts a durable property DTO into the runtime model.
func (d PropertyDTO) ToModel() config.Property {
	return config.Property{
		Key: d.Key,
		Value: config.Value{
			Kind:      config.ValueKind(d.Value.Kind),
			Str:       d.Value.Str,
			Int:       d.Value.Int,
			Flt:       d.Value.Flt,
			Bool:      d.Value.Bool,
			RawSecret: d.Value.RawSecret,
		},
		Version: d.Version,
	}
}

// FeatureFromModel converts a runtime feature into a durable DTO.
func FeatureFromModel(feat config.Feature) FeatureDTO {
	al := make([]string, 0, len(feat.AllowList))
	for s := range feat.AllowList {
		al = append(al, s)
	}
	sort.Strings(al)
	return FeatureDTO{
		Name:       feat.Name,
		Enabled:    feat.Enabled,
		Percentage: feat.Percentage,
		AllowList:  al,
	}
}

// ToModel converts a durable feature DTO into the runtime model.
func (d FeatureDTO) ToModel() config.Feature {
	al := make(map[string]bool, len(d.AllowList))
	for _, s := range d.AllowList {
		al[s] = true
	}
	return config.Feature{
		Name:       d.Name,
		Enabled:    d.Enabled,
		Percentage: d.Percentage,
		AllowList:  al,
	}
}

// SnapshotFrom converts a runtime snapshot into a durable DTO. Keys are
// sorted so the serialized form is deterministic (also for checksums).
func SnapshotFrom(snap *config.Snapshot) SnapshotDTO {
	dto := SnapshotDTO{
		Version:        snap.Version,
		Properties:     []PropertyDTO{},
		Features:       []FeatureDTO{},
		PropertyOrigin: cloneStringMap(snap.PropertyOrigin),
		FeatureOrigin:  cloneStringMap(snap.FeatureOrigin),
		SourceVersions: cloneInt64Map(snap.SourceVersions),
		CreatedAtUnix:  snap.CreatedAtUnix,
	}
	keys := make([]string, 0, len(snap.Doc.Properties))
	for k := range snap.Doc.Properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		p := snap.Doc.Properties[k]
		if p.Key == "" {
			p.Key = k
		}
		dto.Properties = append(dto.Properties, PropertyFromModel(p))
	}
	names := make([]string, 0, len(snap.Doc.Features))
	for k := range snap.Doc.Features {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, n := range names {
		f := snap.Doc.Features[n]
		if f.Name == "" {
			f.Name = n
		}
		dto.Features = append(dto.Features, FeatureFromModel(f))
	}
	return dto
}

// ToSnapshot converts a durable snapshot back into the runtime model.
func (d SnapshotDTO) ToSnapshot() *config.Snapshot {
	doc := &config.Document{
		Properties: make(map[string]config.Property, len(d.Properties)),
		Features:   make(map[string]config.Feature, len(d.Features)),
	}
	for _, pd := range d.Properties {
		p := pd.ToModel()
		if p.Key == "" {
			p.Key = pd.Key
		}
		doc.Properties[pd.Key] = p
	}
	for _, fd := range d.Features {
		f := fd.ToModel()
		if f.Name == "" {
			f.Name = fd.Name
		}
		doc.Features[fd.Name] = f
	}
	return &config.Snapshot{
		Version:        d.Version,
		Doc:            doc,
		PropertyOrigin: cloneStringMap(d.PropertyOrigin),
		FeatureOrigin:  cloneStringMap(d.FeatureOrigin),
		SourceVersions: cloneInt64Map(d.SourceVersions),
		CreatedAtUnix:  d.CreatedAtUnix,
	}
}

func cloneStringMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneInt64Map(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
