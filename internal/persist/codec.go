package persist

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

// on-disk schema versions
const (
	// envFormat is the outer envelope format ({format,crc32,data}).
	envFormat = 1
	// stateSchema is the version of the state payload inside the envelope.
	stateSchema = 1
)

type envelope struct {
	Format int             `json:"format"`
	CRC32  string          `json:"crc32"`
	Data   json.RawMessage `json:"data"`
}

// sealEnvelope wraps payload with its CRC32 (IEEE, lowercase hex). The
// checksum is computed over the exact data bytes stored in the file, so a
// torn/altered write is detected on the next start. The outer envelope is
// compact (NOT indented): re-indenting a nested RawMessage would change the
// stored data bytes and invalidate the checksum.
func sealEnvelope(data []byte) ([]byte, error) {
	env := envelope{Format: envFormat, CRC32: crcHex(data), Data: json.RawMessage(data)}
	out, err := json.Marshal(&env)
	if err != nil {
		return nil, errs.Wrap(errs.KindPersistIO, "persist: cannot encode envelope", err)
	}
	return out, nil
}

// openEnvelope parses, verifies and returns the inner payload bytes.
func openEnvelope(raw []byte) ([]byte, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, errs.New(errs.KindPersistCorrupt, "persist: state file is empty")
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, errs.Wrap(errs.KindPersistCorrupt, "persist: state file is not valid JSON", err)
	}
	if env.Format != envFormat {
		return nil, errs.New(errs.KindPersistSchema,
			fmt.Sprintf("persist: unsupported state envelope format %d (want %d)", env.Format, envFormat))
	}
	if len(env.Data) == 0 {
		return nil, errs.New(errs.KindPersistCorrupt, "persist: state envelope has no data")
	}
	if env.CRC32 != crcHex(env.Data) {
		return nil, errs.New(errs.KindPersistChecksum,
			"persist: state checksum mismatch (torn or altered write)")
	}
	return env.Data, nil
}

func crcHex(b []byte) string {
	return strconv.FormatUint(uint64(crc32.ChecksumIEEE(b)), 16)
}

// ---- state payload DTOs -------------------------------------------------

type stateDTO struct {
	Schema         int                    `json:"schema"`
	CurrentVersion int64                  `json:"current_version"`
	History        []snapshotDTO          `json:"history"`
	Overrides      map[string]overrideDTO `json:"overrides"`
}

type snapshotDTO struct {
	Version        int64             `json:"version"`
	CreatedAtUnix  int64             `json:"created_at_unix"`
	PropertyOrigin map[string]string `json:"property_origin,omitempty"`
	FeatureOrigin  map[string]string `json:"feature_origin,omitempty"`
	SourceVersions map[string]int64  `json:"source_versions,omitempty"`
	Doc            documentDTO       `json:"doc"`
}

type documentDTO struct {
	Properties map[string]propertyDTO `json:"properties"`
	Features   map[string]featureDTO  `json:"features"`
}

type propertyDTO struct {
	Type    string          `json:"type"`
	Version int64           `json:"version"`
	Value   json.RawMessage `json:"value"`
}

type featureDTO struct {
	Enabled    bool     `json:"enabled"`
	Percentage int      `json:"percentage"`
	AllowList  []string `json:"allow_list,omitempty"`
}

type overrideDTO struct {
	Props     map[string]propertyDTO `json:"props,omitempty"`
	Feats     map[string]featureDTO  `json:"feats,omitempty"`
	DeadProps []string               `json:"dead_props,omitempty"`
	DeadFeats []string               `json:"dead_feats,omitempty"`
}

// ---- encoding -----------------------------------------------------------

func encodeState(st *State) ([]byte, error) {
	if st == nil || st.Current == nil {
		return nil, errs.New(errs.KindValidation, "persist: refusing to encode a nil state")
	}
	dto := stateDTO{
		Schema:         stateSchema,
		CurrentVersion: st.Current.Version,
		History:        make([]snapshotDTO, 0, len(st.History)),
		Overrides:      map[string]overrideDTO{},
	}
	seen := map[int64]bool{}
	var last int64
	for _, snap := range st.History {
		if snap == nil || snap.Doc == nil {
			return nil, errs.New(errs.KindValidation, "persist: history contains a nil snapshot")
		}
		if seen[snap.Version] {
			return nil, errs.New(errs.KindValidation,
				"persist: duplicate version "+strconv.FormatInt(snap.Version, 10)+" in state")
		}
		seen[snap.Version] = true
		if snap.Version <= last && last != 0 {
			return nil, errs.New(errs.KindValidation, "persist: history is not version-ordered")
		}
		last = snap.Version
		dto.History = append(dto.History, encodeSnapshot(snap))
	}
	if last != st.Current.Version {
		return nil, errs.New(errs.KindValidation,
			"persist: current version is not the newest history entry")
	}
	for v, ov := range st.Overrides {
		if !seen[v] {
			return nil, errs.New(errs.KindValidation,
				"persist: override state for unknown version "+strconv.FormatInt(v, 10))
		}
		dto.Overrides[strconv.FormatInt(v, 10)] = encodeOverride(ov)
	}
	// Every retained version must have override state; an empty override layer
	// is persisted as an explicit empty object.
	for _, snap := range st.History {
		if _, ok := st.Overrides[snap.Version]; !ok {
			return nil, errs.New(errs.KindValidation,
				"persist: missing override state for version "+strconv.FormatInt(snap.Version, 10))
		}
	}
	inner, err := json.Marshal(&dto)
	if err != nil {
		return nil, errs.Wrap(errs.KindPersistIO, "persist: cannot encode state", err)
	}
	return sealEnvelope(inner)
}

func encodeSnapshot(snap *config.Snapshot) snapshotDTO {
	dto := snapshotDTO{
		Version:        snap.Version,
		CreatedAtUnix:  snap.CreatedAtUnix,
		PropertyOrigin: cloneStringMap(snap.PropertyOrigin),
		FeatureOrigin:  cloneStringMap(snap.FeatureOrigin),
		SourceVersions: cloneInt64Map(snap.SourceVersions),
		Doc:            encodeDocument(snap.Doc),
	}
	return dto
}

func encodeDocument(doc *config.Document) documentDTO {
	dto := documentDTO{
		Properties: map[string]propertyDTO{},
		Features:   map[string]featureDTO{},
	}
	for k, p := range doc.Properties {
		dto.Properties[k] = encodeProperty(p)
	}
	for n, f := range doc.Features {
		dto.Features[n] = encodeFeature(f)
	}
	return dto
}

func encodeProperty(p config.Property) propertyDTO {
	dto := propertyDTO{Type: string(p.Value.Kind), Version: p.Version}
	var v any
	switch p.Value.Kind {
	case config.KindString:
		v = p.Value.Str
	case config.KindSensitive:
		// Secrets are persisted intentionally: the data dir is 0700 and files
		// 0600, and full recovery of effective config requires them.
		v = p.Value.RawSecret
	case config.KindInt:
		v = p.Value.Int
	case config.KindFloat:
		v = p.Value.Flt
	case config.KindBool:
		v = p.Value.Bool
	default:
		v = nil
	}
	b, _ := json.Marshal(v)
	dto.Value = b
	return dto
}

func encodeFeature(f config.Feature) featureDTO {
	dto := featureDTO{Enabled: f.Enabled, Percentage: f.Percentage}
	for s := range f.AllowList {
		dto.AllowList = append(dto.AllowList, s)
	}
	sort.Strings(dto.AllowList)
	return dto
}

func encodeOverride(ov Override) overrideDTO {
	dto := overrideDTO{
		Props:     map[string]propertyDTO{},
		Feats:     map[string]featureDTO{},
		DeadProps: make([]string, 0, len(ov.DeadProps)),
		DeadFeats: make([]string, 0, len(ov.DeadFeats)),
	}
	for k, p := range ov.Props {
		dto.Props[k] = encodeProperty(p)
	}
	for n, f := range ov.Feats {
		dto.Feats[n] = encodeFeature(f)
	}
	for k, dead := range ov.DeadProps {
		if dead {
			dto.DeadProps = append(dto.DeadProps, k)
		}
	}
	for n, dead := range ov.DeadFeats {
		if dead {
			dto.DeadFeats = append(dto.DeadFeats, n)
		}
	}
	sort.Strings(dto.DeadProps)
	sort.Strings(dto.DeadFeats)
	return dto
}

// ---- decoding -----------------------------------------------------------

func decodeState(raw []byte) (*State, error) {
	inner, err := openEnvelope(raw)
	if err != nil {
		return nil, err
	}
	var dto stateDTO
	if err := json.Unmarshal(inner, &dto); err != nil {
		return nil, errs.Wrap(errs.KindPersistCorrupt, "persist: state payload is not valid JSON", err)
	}
	if dto.Schema != stateSchema {
		return nil, errs.New(errs.KindPersistSchema,
			fmt.Sprintf("persist: unsupported state schema %d (want %d)", dto.Schema, stateSchema))
	}
	if len(dto.History) == 0 {
		return nil, errs.New(errs.KindPersistCorrupt, "persist: state history is empty")
	}
	st := &State{Overrides: map[int64]Override{}}
	var last int64
	for _, sdto := range sdtoList(dto.History) {
		if sdto.Version <= 0 {
			return nil, errs.New(errs.KindPersistCorrupt, "persist: non-positive version in history")
		}
		if sdto.Version <= last {
			return nil, errs.New(errs.KindPersistCorrupt,
				"persist: history versions are not strictly increasing")
		}
		snap, derr := decodeSnapshot(sdto)
		if derr != nil {
			return nil, derr
		}
		st.History = append(st.History, snap)
		last = sdto.Version
	}
	if last != dto.CurrentVersion {
		return nil, errs.New(errs.KindPersistCorrupt,
			"persist: current_version does not match newest history entry")
	}
	st.Current = st.History[len(st.History)-1]
	for vk, odto := range dto.Overrides {
		v, perr := strconv.ParseInt(vk, 10, 64)
		if perr != nil {
			return nil, errs.Wrap(errs.KindPersistCorrupt,
				"persist: override key is not a version: "+vk, perr)
		}
		ov, oerr := decodeOverride(odto)
		if oerr != nil {
			return nil, oerr
		}
		st.Overrides[v] = ov
	}
	for _, snap := range st.History {
		if _, ok := st.Overrides[snap.Version]; !ok {
			return nil, errs.New(errs.KindPersistCorrupt,
				"persist: durable state is missing override data for version "+
					strconv.FormatInt(snap.Version, 10))
		}
	}
	return st, nil
}

func sdtoList(h []snapshotDTO) []snapshotDTO { return h }

func decodeSnapshot(dto snapshotDTO) (*config.Snapshot, error) {
	doc, err := decodeDocument(dto.Doc)
	if err != nil {
		return nil, err
	}
	return &config.Snapshot{
		Version:        dto.Version,
		CreatedAtUnix:  dto.CreatedAtUnix,
		Doc:            doc,
		PropertyOrigin: cloneStringMap(dto.PropertyOrigin),
		FeatureOrigin:  cloneStringMap(dto.FeatureOrigin),
		SourceVersions: cloneInt64Map(dto.SourceVersions),
	}, nil
}

func decodeDocument(dto documentDTO) (*config.Document, error) {
	doc := &config.Document{
		Properties: map[string]config.Property{},
		Features:   map[string]config.Feature{},
	}
	for k, pdto := range dto.Properties {
		p, err := config.ParseProperty(k, pdto.Type, pdto.Value)
		if err != nil {
			return nil, errs.Wrap(errs.KindPersistCorrupt,
				"persist: invalid durable property "+k, err)
		}
		p.Version = pdto.Version
		doc.Properties[k] = p
	}
	for n, fdto := range dto.Features {
		f, err := config.ParseFeatureDef(n, config.RawFeature{
			Enabled: fdto.Enabled, Percentage: fdto.Percentage, AllowList: fdto.AllowList,
		})
		if err != nil {
			return nil, errs.Wrap(errs.KindPersistCorrupt,
				"persist: invalid durable feature "+n, err)
		}
		doc.Features[n] = f
	}
	return doc, nil
}

func decodeOverride(dto overrideDTO) (Override, error) {
	ov := Override{
		Props:     map[string]config.Property{},
		Feats:     map[string]config.Feature{},
		DeadProps: map[string]bool{},
		DeadFeats: map[string]bool{},
	}
	for k, pdto := range dto.Props {
		p, err := config.ParseProperty(k, pdto.Type, pdto.Value)
		if err != nil {
			return Override{}, errs.Wrap(errs.KindPersistCorrupt,
				"persist: invalid durable override property "+k, err)
		}
		p.Version = pdto.Version
		ov.Props[k] = p
	}
	for n, fdto := range dto.Feats {
		f, err := config.ParseFeatureDef(n, config.RawFeature{
			Enabled: fdto.Enabled, Percentage: fdto.Percentage, AllowList: fdto.AllowList,
		})
		if err != nil {
			return Override{}, errs.Wrap(errs.KindPersistCorrupt,
				"persist: invalid durable override feature "+n, err)
		}
		ov.Feats[n] = f
	}
	for _, k := range dto.DeadProps {
		ov.DeadProps[k] = true
	}
	for _, n := range dto.DeadFeats {
		ov.DeadFeats[n] = true
	}
	return ov, nil
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
