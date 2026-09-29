package persist

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

const (
	stateName = "state.json"
	backupSfx = ".bak"
	tmpSuffix = ".tmp"
	auditName = "audit.log"

	dirPerm  = 0o700 // data directory: owner only (durable state holds secrets)
	filePerm = 0o600 // state/audit files: owner only
)

func statePath(dir string) string  { return filepath.Join(dir, stateName) }
func backupPath(dir string) string { return statePath(dir) + backupSfx }
func auditPath(dir string) string  { return filepath.Join(dir, auditName) }

// Open recovers durable state from dir (creating it on first use).
//
// Recovery order:
//  1. state.json present and intact            -> load it.
//  2. state.json damaged but state.json.bak ok -> use the backup, record a
//     classified Problem; the in-flight commit that tore is simply absent
//     and the version counter continues past it (no version is reused).
//  3. both missing on an otherwise empty dir   -> fresh start.
//  4. both present but damaged                 -> fail closed: classified
//     error; no intermediate state is ever served.
func Open(ctx context.Context, dir string, lim Limits) (*Store, *RecoveryReport, *State, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	if dir == "" {
		return nil, nil, nil, errs.New(errs.KindValidation, "persist: data directory is empty")
	}
	lim = normalizeLimits(lim)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, nil, nil, errs.Wrap(errs.KindPersistIO,
			"persist: cannot create data directory "+dir, err)
	}
	s := &Store{dir: dir, limits: lim}

	rep := &RecoveryReport{}
	st, err := loadStateFile(statePath(dir))
	if err != nil {
		if errors.Is(err, errFileAbsent) {
			// No primary. A backup without a primary happens when the rename
			// to primary never happened: restore the backup (last complete).
			bst, berr := loadStateFile(backupPath(dir))
			if berr == nil {
				st = bst
				rep.UsedBackup = true
				rep.Problems = append(rep.Problems, Problem{
					Kind: string(errs.KindPersistIO),
					Message: "state.json missing after interrupted write; " +
						"recovered last complete state from state.json.bak",
				})
			} else if errors.Is(berr, errFileAbsent) {
				st = nil // genuinely fresh start
			} else {
				return nil, nil, nil, berr
			}
		} else {
			// Primary is damaged: try the backup.
			bst, berr := loadStateFile(backupPath(dir))
			if berr == nil {
				st = bst
				rep.UsedBackup = true
				rep.Problems = append(rep.Problems, Problem{
					Kind:    string(errs.Classify(err)),
					Message: "state.json unusable (" + err.Error() + "); recovered state.json.bak",
				})
			} else if errors.Is(berr, errFileAbsent) {
				return nil, nil, nil, err // no safety net: fail closed
			} else {
				return nil, nil, nil, errs.Wrap(errs.Classify(berr),
					"persist: state.json and state.json.bak are both unusable", berr)
			}
		}
	}

	entries, arep, aerr := s.LoadAudit(ctx)
	if aerr != nil {
		return nil, nil, nil, aerr
	}
	if arep != nil {
		rep.Problems = append(rep.Problems, arep.Problems...)
	}
	rep.AuditEntries = len(entries)

	if st != nil {
		rep.Loaded = true
		rep.StateVersion = st.Current.Version
		// Rewrite a clean canonical state when recovery had to use a backup, so
		// the damaged primary is replaced before serving traffic.
		if rep.UsedBackup {
			if serr := s.SaveState(ctx, st); serr != nil {
				return nil, nil, nil, serr
			}
		}
	}
	s.auditEntries = entries
	return s, rep, st, nil
}

var errFileAbsent = errors.New("persist: file absent")

// loadStateFile reads, checksums and decodes one state file.
func loadStateFile(path string) (*State, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errFileAbsent
		}
		return nil, errs.Wrap(errs.KindPersistIO, "persist: cannot read "+path, err)
	}
	st, derr := decodeState(raw)
	if derr != nil {
		return nil, derr
	}
	return st, nil
}

// SaveState atomically durably replaces the complete state. Callers must NOT
// publish the new snapshot to readers until this returns nil: durability
// precedes visibility. The previous good state is preserved as the backup
// before the new primary is installed.
func (s *Store) SaveState(ctx context.Context, st *State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if st == nil || st.Current == nil {
		return errs.New(errs.KindValidation, "persist: cannot save a nil state")
	}
	// Defensive pruning: history stays inside RetainVersions even if the
	// caller hands us a longer slice. The newest entries survive. This mutates
	// st in place so the returned/caller-held state matches what is on disk
	// (important when recovery self-heals with a smaller configured window).
	pruneStateInPlace(st, s.limits.RetainVersions)

	data, err := encodeState(st)
	if err != nil {
		return err
	}
	if s.limits.MaxStateBytes > 0 && len(data) > s.limits.MaxStateBytes {
		return errs.New(errs.KindPersistTooLarge,
			fmt.Sprintf("persist: state payload %d bytes exceeds cap %d",
				len(data), s.limits.MaxStateBytes))
	}

	if err := atomicWrite(statePath(s.dir), data); err != nil {
		return err
	}
	return nil
}

// pruneStateInPlace trims st's History to the newest retain entries (current
// always retained) and drops override data outside the window in lockstep.
func pruneStateInPlace(st *State, retain int) {
	if retain <= 0 || len(st.History) <= retain {
		return
	}
	drop := st.History[:len(st.History)-retain]
	st.History = st.History[len(st.History)-retain:]
	for _, snap := range drop {
		delete(st.Overrides, snap.Version)
	}
}

// atomicWrite writes data to path crash-safely:
//  1. write+fsync a temp file in the same directory,
//  2. preserve the current file as path.bak (fsync the rename),
//  3. rename the temp file over path and fsync the directory.
//
// A kill at any point leaves either the previous complete file or the new
// complete file; a half-written temp is simply ignored.
func atomicWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".state-*"+tmpSuffix)
	if err != nil {
		return errs.Wrap(errs.KindPersistIO, "persist: cannot create temp file", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return errs.Wrap(errs.KindPersistIO, "persist: cannot write temp file", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return errs.Wrap(errs.KindPersistIO, "persist: fsync temp failed", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return errs.Wrap(errs.KindPersistIO, "persist: close temp failed", err)
	}
	if err := os.Chmod(tmpName, filePerm); err != nil {
		cleanup()
		return errs.Wrap(errs.KindPersistIO, "persist: chmod temp failed", err)
	}

	// Preserve the previous complete state as backup. A missing previous file
	// (first save) is fine.
	if _, statErr := os.Stat(path); statErr == nil {
		if err := os.Rename(path, path+backupSfx); err != nil {
			cleanup()
			return errs.Wrap(errs.KindPersistIO, "persist: cannot rotate backup", err)
		}
		if err := syncDir(dir); err != nil {
			return err
		}
	}

	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return errs.Wrap(errs.KindPersistIO, "persist: cannot install new state", err)
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return errs.Wrap(errs.KindPersistIO, "persist: cannot open dir for fsync", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return errs.Wrap(errs.KindPersistIO, "persist: fsync dir failed", err)
	}
	return nil
}

func normalizeLimits(lim Limits) Limits {
	d := DefaultLimits()
	if lim.MaxStateBytes <= 0 {
		lim.MaxStateBytes = d.MaxStateBytes
	}
	if lim.RetainVersions <= 0 {
		lim.RetainVersions = d.RetainVersions
	}
	if lim.MaxAuditEntries <= 0 {
		lim.MaxAuditEntries = d.MaxAuditEntries
	}
	if lim.MaxAuditBytes <= 0 {
		lim.MaxAuditBytes = d.MaxAuditBytes
	}
	return lim
}
