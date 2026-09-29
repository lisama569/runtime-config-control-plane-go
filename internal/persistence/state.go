package persistence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

// stateFilePrefix names complete generations: state-000000001.json etc.
const (
	stateFilePrefix = "state-"
	stateFileSuffix = ".json"
	stateTempSuffix = ".tmp"
	// retainGenerations keeps the newest N complete files so the store can
	// fall back when the newest one is corrupt.
	retainGenerations = 2
)

// StateStore durably saves and recovers the complete control-plane state in
// a local directory. Writes are crash-safe: content is fully written to a
// temp file, fsynced and atomically renamed, and the directory is fsynced.
// The newest two generations are retained; if the newest file is corrupt or
// unreadable the store falls back to the older one (after logging it).
type StateStore struct {
	dir    string
	logger *log.Logger
}

// NewStateStore opens (without loading) a state store rooted at dir. The
// directory is created on the first save.
func NewStateStore(dir string, logger *log.Logger) *StateStore {
	if logger == nil {
		logger = log.Default()
	}
	return &StateStore{dir: dir, logger: logger}
}

// Dir returns the store's data directory.
func (s *StateStore) Dir() string { return s.dir }

// Save atomically persists state. It returns a classified error on I/O
// failure or when state exceeds the capacity bound; on such failure no new
// generation becomes visible, so callers can reject the commit.
func (s *StateStore) Save(state *State) error {
	if state == nil {
		return errs.New(errs.KindPersistFailed, "persistence: cannot save nil state")
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot create data directory "+s.dir, err)
	}
	state.SchemaVersion = CurrentSchemaVersion
	payload, err := encodeState(state)
	if err != nil {
		return err
	}
	if len(payload) > MaxStateBytes {
		return errs.New(errs.KindPersistFailed, fmt.Sprintf(
			"persistence: state size %d exceeds capacity %d bytes", len(payload), MaxStateBytes))
	}

	gens, err := listGenerations(s.dir)
	if err != nil {
		return err
	}
	gen := int64(1)
	if len(gens) > 0 {
		gen = gens[len(gens)-1] + 1
	}
	final := filepath.Join(s.dir, generationName(gen))
	tmp := filepath.Join(s.dir,
		fmt.Sprintf("%s%d%s", stateFilePrefix, gen, stateTempSuffix))

	// Write-then-fsync the temp file with restrictive permissions: state may
	// contain local secrets (sensitive values are needed for recovery).
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot open temp state file", err)
	}
	if _, werr := f.Write(payload); werr != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot write state file", werr)
	}
	if werr := f.Sync(); werr != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot fsync state file", werr)
	}
	if werr := f.Close(); werr != nil {
		_ = os.Remove(tmp)
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot close state file", werr)
	}
	if werr := os.Rename(tmp, final); werr != nil {
		_ = os.Remove(tmp)
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot atomically publish state file", werr)
	}
	if werr := syncDir(s.dir); werr != nil {
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot fsync state directory", werr)
	}
	s.prune(gens, gen)
	return nil
}

// prune removes complete generations older than the retained window. It also
// sweeps leftover temp files. Errors are logged only: pruning never blocks a
// committed save and never invalidates the just-published generation.
func (s *StateStore) prune(prevGens []int64, current int64) {
	keep := map[int64]bool{current: true}
	for i := len(prevGens) - 1; i >= 0 && len(keep) < retainGenerations; i-- {
		keep[prevGens[i]] = true
	}
	for _, g := range prevGens {
		if keep[g] {
			continue
		}
		if err := os.Remove(filepath.Join(s.dir, generationName(g))); err != nil {
			s.logger.Printf("persistence: cannot prune old state generation %d: %v", g, err)
		}
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), stateFilePrefix) && strings.HasSuffix(e.Name(), stateTempSuffix) {
			if err := os.Remove(filepath.Join(s.dir, e.Name())); err != nil {
				s.logger.Printf("persistence: cannot remove stale temp file %s: %v", e.Name(), err)
			}
		}
	}
}

// Load reads the newest complete, checksum-valid state. When the newest
// generation is corrupt it is reported through the returned diagnostics and
// the previous generation is used. A nil state with a nil report means no
// state has ever been persisted (fresh start).
func (s *StateStore) Load() (*State, *LoadReport, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot read data directory "+s.dir, err)
	}
	report := &LoadReport{}
	var gens []int64
	genFile := map[int64]string{}
	for _, e := range entries {
		name := e.Name()
		g, ok := parseGenerationName(name)
		if !ok {
			// Temp leftovers and unrelated files are never treated as state.
			report.IgnoredFiles = append(report.IgnoredFiles, name)
			continue
		}
		gens = append(gens, g)
		genFile[g] = name
	}
	if len(gens) == 0 {
		return nil, report, nil
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i] > gens[j] })
	var details []string
	for idx, g := range gens {
		name := genFile[g]
		path := filepath.Join(s.dir, name)
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			details = append(details, fmt.Sprintf("%s: unreadable: %v", name, rerr))
			continue
		}
		state, verr := decodeState(data)
		if verr != nil {
			details = append(details, fmt.Sprintf("%s: %v", name, verr))
			continue
		}
		if idx > 0 {
			report.FallbackFrom = genFile[gens[0]]
			report.FallbackTo = name
			s.logger.Printf("persistence: newest state %s unusable (%d problem(s)); recovered from %s",
				genFile[gens[0]], len(details), name)
		}
		report.CorruptDetails = details
		return state, report, nil
	}
	report.CorruptDetails = details
	for _, d := range details {
		s.logger.Printf("persistence: state file rejected: %s", d)
	}
	return nil, report, errs.New(errs.KindStateCorrupt,
		"persistence: no usable state generation found; refusing to start from corrupt data")
}

// LoadReport describes non-fatal recovery events (a corrupt newest
// generation that was skipped, leftover temp files, ...).
type LoadReport struct {
	FallbackFrom   string
	FallbackTo     string
	CorruptDetails []string
	IgnoredFiles   []string
}

func generationName(gen int64) string {
	return fmt.Sprintf("%s%09d%s", stateFilePrefix, gen, stateFileSuffix)
}

func parseGenerationName(name string) (int64, bool) {
	if !strings.HasPrefix(name, stateFilePrefix) || !strings.HasSuffix(name, stateFileSuffix) {
		return 0, false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(name, stateFilePrefix), stateFileSuffix)
	var g int64
	for _, c := range body {
		if c < '0' || c > '9' {
			return 0, false
		}
		g = g*10 + int64(c-'0')
	}
	if body == "" {
		return 0, false
	}
	return g, true
}

func listGenerations(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot list data directory "+dir, err)
	}
	var gens []int64
	for _, e := range entries {
		if g, ok := parseGenerationName(e.Name()); ok {
			gens = append(gens, g)
		}
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i] < gens[j] })
	return gens, nil
}

func encodeState(state *State) ([]byte, error) {
	// Checksum covers the canonical JSON with the checksum field cleared.
	state.Checksum = ""
	body, err := json.Marshal(state)
	if err != nil {
		return nil, errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot encode state", err)
	}
	sum := sha256.Sum256(body)
	state.Checksum = hex.EncodeToString(sum[:])
	payload, err := json.Marshal(state)
	if err != nil {
		return nil, errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot encode state", err)
	}
	return payload, nil
}

// decodeState parses and verifies one state file's bytes. JSON syntax
// errors, checksum mismatch and unknown schema versions are all reported as
// STATE_CORRUPT so callers never mistake damaged bytes for a valid commit.
func decodeState(data []byte) (*State, error) {
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, errs.Wrap(errs.KindStateCorrupt,
			"persistence: state file is not valid JSON", err)
	}
	want := state.Checksum
	state.Checksum = ""
	body, err := json.Marshal(&state)
	if err != nil {
		return nil, errs.Wrap(errs.KindStateCorrupt,
			"persistence: state file cannot be re-encoded", err)
	}
	sum := sha256.Sum256(body)
	if got := hex.EncodeToString(sum[:]); got != want || want == "" {
		return nil, errs.New(errs.KindStateCorrupt,
			"persistence: state checksum mismatch (file may be torn or tampered with)")
	}
	if state.SchemaVersion != CurrentSchemaVersion {
		return nil, errs.New(errs.KindStateInconsistent, fmt.Sprintf(
			"persistence: unsupported state schema version %d (want %d)",
			state.SchemaVersion, CurrentSchemaVersion))
	}
	return &state, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
