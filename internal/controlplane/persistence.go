package controlplane

import (
	"encoding/json"
	"log"
	"path/filepath"
	"sort"

	"github.com/lisama569/runtime-config-control-plane-go/internal/audit"
	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
	"github.com/lisama569/runtime-config-control-plane-go/internal/persistence"
	"github.com/lisama569/runtime-config-control-plane-go/internal/store"
)

// durable wires crash-safe persistence into a Plane. A nil durable means the
// plane runs purely in memory (the historical behavior).
type durable struct {
	state *persistence.StateStore
	audit *persistence.AuditSink
}

func newDurable(dataDir string, logger *log.Logger) *durable {
	if logger == nil {
		logger = log.Default()
	}
	return &durable{
		state: persistence.NewStateStore(dataDir, logger),
		audit: persistence.NewAuditSink(filepath.Join(dataDir, "audit.log"), logger),
	}
}

// buildState serializes the committed history plus each version's override
// layer into one complete durable record.
func (p *Plane) buildState(current int64, history []*config.Snapshot) *persistence.State {
	st := &persistence.State{
		SchemaVersion:  persistence.CurrentSchemaVersion,
		CurrentVersion: current,
		History:        make([]persistence.SnapshotDTO, 0, len(history)),
		Overrides:      []persistence.OverrideDTO{},
	}
	for _, snap := range history {
		st.History = append(st.History, persistence.SnapshotFrom(snap))
	}
	versions := make([]int64, 0, len(p.overHist))
	for v := range p.overHist {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	for _, v := range versions {
		ov := p.overHist[v]
		dto := persistence.OverrideDTO{Version: v}
		for k, pr := range ov.props {
			pr.Key = k
			dto.Props = append(dto.Props, persistence.PropertyFromModel(pr))
		}
		sort.Slice(dto.Props, func(i, j int) bool { return dto.Props[i].Key < dto.Props[j].Key })
		for k, f := range ov.feats {
			f.Name = k
			dto.Feats = append(dto.Feats, persistence.FeatureFromModel(f))
		}
		sort.Slice(dto.Feats, func(i, j int) bool { return dto.Feats[i].Name < dto.Feats[j].Name })
		for k := range ov.deadProps {
			dto.DeadProps = append(dto.DeadProps, k)
		}
		for k := range ov.deadFeats {
			dto.DeadFeats = append(dto.DeadFeats, k)
		}
		sort.Strings(dto.DeadProps)
		sort.Strings(dto.DeadFeats)
		st.Overrides = append(st.Overrides, dto)
	}
	return st
}

// persistHook is invoked by the store before a new snapshot becomes visible.
// Persisting first guarantees that anything durable is also effective and
// that a failed commit neither advances the live version nor the durable
// timeline. The hook runs while the store's update lock is held; it takes the
// plane lock for a consistent read of the override timeline.
func (p *Plane) persistHook(snap *config.Snapshot, history []*config.Snapshot) error {
	if p.dur == nil {
		return nil
	}
	p.mu.Lock()
	st := p.buildState(snap.Version, history)
	p.mu.Unlock()
	return p.dur.state.Save(st)
}

// restoreFromDisk rebuilds the store history, override layers and audit
// trail from the last durable state. It fails closed on corrupt or
// internally inconsistent data and never exposes an intermediate state.
func (p *Plane) restoreFromDisk() error {
	st, report, err := p.dur.state.Load()
	if err != nil {
		return err
	}
	if report != nil {
		for _, d := range report.CorruptDetails {
			p.logf("persistence: recovery note: %s", d)
		}
		if report.FallbackTo != "" {
			p.logf("persistence: fell back from %s to %s", report.FallbackFrom, report.FallbackTo)
		}
	}
	if st == nil {
		// No prior state: this data directory has never held a commit.
		return nil
	}
	if err := validateState(st); err != nil {
		return err
	}

	snaps := make([]*config.Snapshot, 0, len(st.History))
	for _, dto := range st.History {
		snaps = append(snaps, dto.ToSnapshot())
	}
	// Rebuild the override timeline, including the live layer of the
	// current version.
	overHist := make(map[int64]overrideState, len(st.Overrides))
	var live overrideState
	for _, ov := range st.Overrides {
		ovs := newOverrideState()
		for _, pd := range ov.Props {
			pr := pd.ToModel()
			if pr.Key == "" {
				pr.Key = pd.Key
			}
			ovs.props[pd.Key] = pr
		}
		for _, fd := range ov.Feats {
			fe := fd.ToModel()
			if fe.Name == "" {
				fe.Name = fd.Name
			}
			ovs.feats[fd.Name] = fe
		}
		for _, k := range ov.DeadProps {
			ovs.deadProps[k] = true
		}
		for _, k := range ov.DeadFeats {
			ovs.deadFeats[k] = true
		}
		overHist[ov.Version] = ovs
		if ov.Version == st.CurrentVersion {
			live = ovs
		}
	}
	// Every retained snapshot must have a matching override layer; otherwise
	// rollback could not reconstruct the target's full effective config.
	for _, snap := range snaps {
		if _, ok := overHist[snap.Version]; !ok {
			return errs.New(errs.KindStateInconsistent,
				"persistence: missing override state for recovered version "+itoa(snap.Version))
		}
	}
	if live.props == nil {
		return errs.New(errs.KindStateInconsistent,
			"persistence: missing override layer for current version "+itoa(st.CurrentVersion))
	}

	// Audit journal recovery: torn tails are truncated by Load; decoded
	// entries continue the monotonic sequence.
	if err := p.loadAuditJournal(); err != nil {
		return err
	}

	p.over = live
	p.overHist = overHist
	current := snaps[len(snaps)-1]
	p.store = store.NewMemoryWithHook(current, p.persistHook)
	// Replace the store's retained history wholesale with the recovered one
	// so rollback can reach every durable retained version.
	store.ReplaceHistory(p.store, snaps)
	p.recovered = true
	p.logf("persistence: recovered %d committed version(s); current version %d",
		len(snaps), current.Version)
	return nil
}

// loadAuditJournal reads the framed audit journal (truncating a torn tail),
// restores decoded entries into the in-memory logger (continuing the monotonic
// sequence) and attaches the sink for subsequent appends.
func (p *Plane) loadAuditJournal() error {
	payloads, truncated, err := p.dur.audit.Load()
	if err != nil {
		return err
	}
	if truncated {
		p.logf("persistence: audit journal tail was torn and truncated")
	}
	entries := make([]audit.Entry, 0, len(payloads))
	for _, raw := range payloads {
		var e audit.Entry
		if uerr := json.Unmarshal(raw, &e); uerr != nil {
			return errs.Wrap(errs.KindStateCorrupt,
				"persistence: cannot decode persisted audit entry", uerr)
		}
		entries = append(entries, e)
	}
	p.aud.Restore(entries)
	p.aud.SetSink(p.dur.audit, p.optionsLogger())
	return nil
}

// persistInitial durably records the freshly bootstrapped version 1 state.
func (p *Plane) persistInitial(snap *config.Snapshot) error {
	st := p.buildState(snap.Version, []*config.Snapshot{snap})
	return p.dur.state.Save(st)
}

// validateState enforces the timeline invariants of a recovered record so a
// crafted or half-merged file can never resurrect or reuse versions.
func validateState(st *persistence.State) error {
	if len(st.History) == 0 {
		return errs.New(errs.KindStateInconsistent, "persistence: state has no history")
	}
	seen := map[int64]bool{}
	var prev int64
	for i, dto := range st.History {
		if dto.Version < 1 {
			return errs.New(errs.KindStateInconsistent,
				"persistence: history contains invalid version "+itoa(dto.Version))
		}
		if seen[dto.Version] {
			return errs.New(errs.KindStateInconsistent,
				"persistence: history contains duplicate version "+itoa(dto.Version))
		}
		seen[dto.Version] = true
		if i > 0 && dto.Version <= prev {
			return errs.New(errs.KindStateInconsistent,
				"persistence: history versions are not strictly increasing")
		}
		prev = dto.Version
	}
	last := st.History[len(st.History)-1].Version
	if st.CurrentVersion != last {
		return errs.New(errs.KindStateInconsistent,
			"persistence: current version "+itoa(st.CurrentVersion)+
				" does not match newest history version "+itoa(last))
	}
	overSeen := map[int64]bool{}
	for _, ov := range st.Overrides {
		if overSeen[ov.Version] {
			return errs.New(errs.KindStateInconsistent,
				"persistence: duplicate override layer for version "+itoa(ov.Version))
		}
		overSeen[ov.Version] = true
	}
	return nil
}

func (p *Plane) logf(format string, args ...any) {
	if p.logger != nil {
		p.logger.Printf(format, args...)
	}
}

func (p *Plane) optionsLogger() *log.Logger {
	if p.logger == nil {
		return log.Default()
	}
	return p.logger
}

// Recovered reports whether the plane started from durable state.
func (p *Plane) Recovered() bool { return p.recovered }
