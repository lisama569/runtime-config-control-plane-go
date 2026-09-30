// Package controlplane wires sources, merge, store, flag, auth and audit into
// one locally runnable configuration control plane.
package controlplane

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/lisama569/runtime-config-control-plane-go/internal/audit"
	"github.com/lisama569/runtime-config-control-plane-go/internal/auth"
	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/flag"
	"github.com/lisama569/runtime-config-control-plane-go/internal/merge"
	"github.com/lisama569/runtime-config-control-plane-go/internal/persist"
	"github.com/lisama569/runtime-config-control-plane-go/internal/source"
	"github.com/lisama569/runtime-config-control-plane-go/internal/store"
)

// Options configures a Plane.
type Options struct {
	Sources      []source.Source
	Tokens       map[string]auth.Principal
	PollInterval time.Duration
	// PersistDir enables crash-safe durability when non-empty: the effective
	// timeline and audit trail are kept in this directory and recovered on
	// startup. Empty keeps the original in-memory-only behavior.
	PersistDir string
	// PersistLimits overrides the default on-disk capacity bounds. The zero
	// value means defaults.
	PersistLimits persist.Limits
	// PersistLogger receives recovery information and durability errors. When
	// nil the standard library default logger is used.
	PersistLogger persistLogger
}

// persistLogger is the tiny logging surface used for durability events.
type persistLogger interface {
	Printf(format string, args ...any)
}

// Plane is the assembled control plane.
type Plane struct {
	store   store.Store
	eval    *flag.Evaluator
	authz   *auth.Authorizer
	aud     *audit.Logger
	sources []source.Source

	persist    *persist.Store // nil when persistence is disabled
	persistLog persistLogger

	cancel context.CancelFunc
	wg     sync.WaitGroup

	// mu is the SINGLE serialization point for every state transition (API
	// writes, rollbacks and hot reloads). A transition reads the live version,
	// builds its candidate override and document, makes the new state durable
	// and publishes the snapshot all in one critical section, so:
	//
	//   - two writes carrying the same expected version can never interleave:
	//     the loser is rejected as STALE_VERSION before it mutates anything;
	//   - file reloads never interleave with API writes, and the override layer
	//     cannot be observed half-applied;
	//   - a rejected/failed request leaves no staged state behind and can never
	//     roll an already-committed configuration backwards.
	//
	// The store's own mutex is only ever taken while mu is held (lock order is
	// always mu -> store.mu); store readers need neither lock because the live
	// snapshot is an atomic, immutable pointer.
	mu sync.Mutex

	layers      map[string]sourceDoc // latest accepted doc per source
	over        overrideState
	overHist    map[int64]overrideState // override state per effective version
	pendingOver map[int64]overrideState // staged for the in-flight durable commit
}

type sourceDoc struct {
	src source.Source
	doc *config.Document
	ver int64
}

// overrideState is the highest-priority in-memory layer produced by
// authenticated API writes. Only explicitly touched keys live here, so file
// sources continue to drive every untouched key and precedence stays
// explainable. tombstones remove keys provided by lower-priority sources.
type overrideState struct {
	props     map[string]config.Property
	feats     map[string]config.Feature
	deadProps map[string]bool
	deadFeats map[string]bool
}

func newOverrideState() overrideState {
	return overrideState{
		props:     map[string]config.Property{},
		feats:     map[string]config.Feature{},
		deadProps: map[string]bool{},
		deadFeats: map[string]bool{},
	}
}

func (o overrideState) clone() overrideState {
	c := newOverrideState()
	for k, v := range o.props {
		c.props[k] = v
	}
	for k, v := range o.feats {
		c.feats[k] = v
	}
	for k := range o.deadProps {
		c.deadProps[k] = true
	}
	for k := range o.deadFeats {
		c.deadFeats[k] = true
	}
	return c
}

func (o overrideState) emptyDoc() *config.Document {
	doc := &config.Document{
		Properties: map[string]config.Property{},
		Features:   map[string]config.Feature{},
	}
	for k, p := range o.props {
		if !o.deadProps[k] {
			doc.Properties[k] = p
		}
	}
	for k, f := range o.feats {
		if !o.deadFeats[k] {
			doc.Features[k] = f
		}
	}
	return doc
}

// New loads all sources, merges them and constructs the plane.
// It fails closed: any source that cannot be loaded validly at startup
// aborts construction instead of starting with partial state.
//
// When Options.PersistDir is set, the last durable timeline is recovered
// BEFORE the plane can serve anything. Requests rejected or unauthorized in a
// previous process are never replayed (only complete committed states are
// durable), so a restart cannot resurrect a write that had not taken effect.
func New(ctx context.Context, opts Options) (*Plane, error) {
	if len(opts.Sources) == 0 {
		return nil, errValidation("controlplane: at least one source is required")
	}
	sorted := append([]source.Source(nil), opts.Sources...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Priority() != sorted[j].Priority() {
			return sorted[i].Priority() < sorted[j].Priority()
		}
		return sorted[i].Name() < sorted[j].Name()
	})

	p := &Plane{
		eval:        flag.NewEvaluator(),
		authz:       auth.NewAuthorizer(opts.Tokens),
		aud:         audit.NewLogger(),
		sources:     sorted,
		layers:      map[string]sourceDoc{},
		over:        newOverrideState(),
		overHist:    map[int64]overrideState{},
		pendingOver: map[int64]overrideState{},
		persistLog:  opts.PersistLogger,
	}

	// Sources still load at startup: failure to read any source fails closed,
	// exactly as without persistence. Their current content is also captured
	// in layers so a later hot reload can merge against the restored config.
	for _, src := range sorted {
		doc, ver, err := src.Load(ctx)
		if err != nil {
			return nil, err
		}
		p.layers[src.Name()] = sourceDoc{src: src, doc: doc, ver: ver}
	}

	var restored *persist.State
	var seedAudit []audit.Entry
	if opts.PersistDir != "" {
		ps, rep, st, err := persist.Open(ctx, opts.PersistDir, opts.PersistLimits)
		if err != nil {
			return nil, err // corrupted with no usable backup: fail closed
		}
		p.persist = ps
		p.logRecovery(rep)
		if rep.Loaded {
			restored = st
		}
		// persist.Open already replayed the journal (and truncated any torn
		// tail); grab the in-memory replay once and seed the audit logger so
		// sequence numbers continue past it and the trail survives restart.
		seedAudit = ps.AuditEntries()
		p.aud = audit.NewLoggerWithSeed(seedAudit)
	}

	if restored != nil {
		if err := p.applyRestored(restored); err != nil {
			return nil, err
		}
	} else {
		p.mu.Lock()
		snap, err := p.bootstrapFromSourcesLocked()
		if err != nil {
			p.mu.Unlock()
			return nil, err
		}
		p.overHist[1] = p.over.clone()
		var hook store.BeforeCommit
		if p.persist != nil {
			hook = p.persistHook()
		}
		p.store = store.NewMemoryWithHook(snap, hook)
		if p.persist != nil {
			// Make the initial bootstrap durable BEFORE serving. The durability
			// hook runs with p.mu held (same contract as every later commit);
			// failure here aborts startup (fail closed) rather than serving a
			// non-durable v1, and nothing staged is left behind.
			p.pendingOver[snap.Version] = p.overHist[1]
			if err := p.saveDurable(ctx, snap, []*config.Snapshot{snap}); err != nil {
				delete(p.pendingOver, snap.Version)
				p.mu.Unlock()
				return nil, err
			}
		}
		p.mu.Unlock()
	}

	if p.persist != nil {
		p.aud.AttachSink(auditSinkAdapter{ps: p.persist}, func(err error) {
			p.plog().Printf("persist: audit durability failure (request unaffected): %v", err)
		})
	}

	rctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	w := source.NewWatcher(sorted, opts.PollInterval)
	p.wg.Add(2)
	go p.reloadLoop(rctx, w, &p.wg)
	go func() { defer p.wg.Done(); _ = w.Run(rctx, p.eventCh(rctx)) }()
	return p, nil
}

// saveDurable assembles the persist.State for snap + retained history and
// writes it through the durability backend. It MUST run inside a serialized
// state transition holding p.mu (the store invokes it from the pre-commit
// hook, after mu was already acquired): the candidate override is staged in
// pendingOver under the same lock and committed into overHist atomically with
// the durability write, so a later commit can never observe a missing entry,
// and a failed write leaves the in-memory timeline untouched.
func (p *Plane) saveDurable(ctx context.Context, snap *config.Snapshot, history []*config.Snapshot) error {
	if p.persist == nil {
		return nil
	}
	if _, ok := p.pendingOver[snap.Version]; !ok {
		// Programmer error, not a user failure: refuse to publish a commit we
		// cannot make durable.
		return persistStateErr(snap.Version)
	}
	st := &persist.State{
		Current:   snap,
		History:   history,
		Overrides: map[int64]persist.Override{},
	}
	for _, h := range history {
		hov, known := p.overHist[h.Version]
		if !known {
			// The new version is staged but its history entry is committed only
			// after the durability write succeeds below; build its durable form
			// straight from the staging slot so it is covered either way.
			if h.Version == snap.Version {
				st.Overrides[h.Version] = toPersistOverride(p.pendingOver[h.Version])
				continue
			}
			return persistStateErr(h.Version)
		}
		st.Overrides[h.Version] = toPersistOverride(hov)
	}
	if err := p.persist.SaveState(ctx, st); err != nil {
		return err
	}
	// Durability precedes visibility: only now (still inside the serialized
	// section, before the store publishes snap) commit the override timeline.
	p.overHist[snap.Version] = p.pendingOver[snap.Version].clone()
	delete(p.pendingOver, snap.Version)
	p.trimOverHist()
	return nil
}

// applyRestored rebuilds in-memory store and override timeline from a durable
// state. Recovery is complete before New returns, so no request can observe a
// partially reconstructed plane. The audit logger was already seeded from the
// replayed journal before this runs.
func (p *Plane) applyRestored(st *persist.State) error {
	overHist := map[int64]overrideState{}
	for v, ov := range st.Overrides {
		overHist[v] = fromPersistOverride(ov)
	}
	curOv, ok := overHist[st.Current.Version]
	if !ok {
		return persistStateErr(st.Current.Version)
	}
	p.over = curOv.clone()
	p.overHist = overHist
	p.store = store.NewRestored(st.History, p.persistHook())
	return nil
}

// bootstrapFromSourcesLocked merges current sources with an empty override.
// Callers must hold p.mu.
func (p *Plane) bootstrapFromSourcesLocked() (*config.Snapshot, error) {
	res, err := p.remergeLocked()
	if err != nil {
		return nil, err
	}
	return &config.Snapshot{
		Version:        1,
		Doc:            res.Doc,
		PropertyOrigin: res.PropertyOrigin,
		FeatureOrigin:  res.FeatureOrigin,
		SourceVersions: res.SourceVersions,
		CreatedAtUnix:  nowFn(),
	}, nil
}

func (p *Plane) eventCh(ctx context.Context) chan source.UpdateEvent {
	ch := make(chan source.UpdateEvent, 4)
	go p.consumeEvents(ctx, ch)
	return ch
}

// reloadLoop exists so future synchronous triggers can share the loop; the
// actual event consumption is in consumeEvents, serialized per event.
func (p *Plane) reloadLoop(ctx context.Context, w *source.Watcher, wg *sync.WaitGroup) {
	defer wg.Done()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case le := <-w.Errors:
				// Invalid hot update: reject, keep last valid config, audit.
				_, _ = p.aud.Record(ctx, audit.Entry{
					Actor:   "system",
					Action:  "source:reload",
					Target:  le.SourceName,
					Result:  audit.ResultError,
					Reason:  le.Err.Error(),
					ErrKind: errKindString(le.Err),
				})
			}
		}
	}()
	<-ctx.Done()
}

func (p *Plane) consumeEvents(ctx context.Context, ch <-chan source.UpdateEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-ch:
			// Only the source identity crosses into the transition; the
			// document and source version are re-read INSIDE the serialized
			// section (see applySourceEvent), so a (doc, version) pair can
			// never be assembled from two different file contents.
			p.applySourceEvent(ctx, ev.Source)
		}
	}
}

// applySourceEvent integrates the CURRENT content of one source. The whole
// transition (read the source, install the new layer, remerge, make durable,
// publish) runs in a single p.mu critical section, so a hot reload can never
// interleave with an API write and readers always see a complete effective
// configuration whose (source version, document) come from one file read.
// A reload that fails to load or merge is rejected audited, leaving the last
// committed configuration exactly in place.
func (p *Plane) applySourceEvent(ctx context.Context, src source.Source) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Re-read the source under the lock. The watcher already validated the
	// file once to decide an event was warranted; loading again here is
	// idempotent for unchanged content and, crucially, pairs the document
	// with the source version atomically relative to other transitions.
	doc, ver, lerr := src.Load(ctx)
	if lerr != nil {
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: "system", Action: "source:reload", Target: src.Name(),
			Result: audit.ResultError, Reason: lerr.Error(),
			ErrKind: errKindString(lerr),
		})
		return
	}

	cur := p.store.Current()
	// Remember the previously accepted layer so a rejected commit can restore
	// it: a reload that does not produce a new version must never leave its
	// candidate document behind (that would leak an uncommitted file value
	// into every later merge).
	prev, hadPrev := p.layers[src.Name()]
	p.layers[src.Name()] = sourceDoc{src: src, doc: doc, ver: ver}
	res, err := p.remergeLocked()
	if err != nil {
		if hadPrev {
			p.layers[src.Name()] = prev
		} else {
			delete(p.layers, src.Name())
		}
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: "system", Action: "source:reload", Target: src.Name(),
			Result: audit.ResultError, Reason: err.Error(),
			ErrKind: errKindString(err), FromVer: cur.Version,
		})
		return
	}

	// The reload carries no API changes: the override layer is simply carried
	// forward and recorded under the new version. Stage it for the durability
	// hook (read below under the same held lock).
	newVersion := cur.Version + 1
	p.pendingOver[newVersion] = p.over.clone()
	snap, cerr := p.store.CAS(ctx, cur.Version, res.Doc, store.Change{
		Reason:   "hot-reload:" + src.Name(),
		Actor:    "system",
		Origin:   res.PropertyOrigin,
		Features: res.FeatureOrigin,
	})
	delete(p.pendingOver, newVersion)
	if cerr != nil {
		// While serialized the expected version cannot have moved, so this is
		// a durability abort: nothing was published. Restore the previous layer
		// as well, keeping in-memory state identical to the last committed
		// snapshot. The dropped reload is simply absent, like an invalid file;
		// the next file change re-emits an event.
		if hadPrev {
			p.layers[src.Name()] = prev
		} else {
			delete(p.layers, src.Name())
		}
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: "system", Action: "source:reload", Target: src.Name(),
			Result: audit.ResultError, Reason: cerr.Error(),
			ErrKind: errKindString(cerr), FromVer: cur.Version,
		})
		return
	}
	_, _ = p.aud.Record(ctx, audit.Entry{
		Actor: "system", Action: "source:reload", Target: src.Name(),
		Result:  audit.ResultAllowed,
		Reason:  "merged at source version " + itoa(ver),
		FromVer: cur.Version, ToVer: snap.Version,
	})
}

// remergeLocked rebuilds the effective document from current sources and the
// LIVE override layer. Callers must hold p.mu.
func (p *Plane) remergeLocked() (*merge.Result, error) {
	return p.remergeOverrideLocked(p.over)
}

// remergeOverrideLocked rebuilds the effective document from current sources
// and an ARBITRARY candidate override state, without installing it. This lets
// a transition construct and validate its candidate while keeping the live
// override untouched until the commit is won. Callers must hold p.mu.
func (p *Plane) remergeOverrideLocked(ov overrideState) (*merge.Result, error) {
	ordered := make([]merge.Layered, 0, len(p.sources)+1)
	overDoc := ov.emptyDoc()
	if len(overDoc.Properties) > 0 || len(overDoc.Features) > 0 {
		ordered = append(ordered, merge.Layered{Name: "api-override", Version: 0, Doc: overDoc})
	}
	for _, src := range p.sources {
		sd, ok := p.layers[src.Name()]
		if !ok {
			continue
		}
		ordered = append(ordered, merge.Layered{Name: src.Name(), Version: sd.ver, Doc: sd.doc})
	}
	res, err := merge.Layers(ordered)
	if err != nil {
		return nil, err
	}
	for k := range ov.deadProps {
		if _, ok := res.Doc.Properties[k]; ok {
			delete(res.Doc.Properties, k)
			res.PropertyOrigin[k] = "api-override(deleted)"
		}
	}
	for k := range ov.deadFeats {
		if _, ok := res.Doc.Features[k]; ok {
			delete(res.Doc.Features, k)
			res.FeatureOrigin[k] = "api-override(deleted)"
		}
	}
	return res, nil
}

// Current returns the current effective snapshot (wait-free atomic read).
func (p *Plane) Current() *config.Snapshot { return p.store.Current() }

// Evaluator exposes the flag evaluator.
func (p *Plane) Evaluator() *flag.Evaluator { return p.eval }

// Audit exposes the audit log (handlers enforce role checks).
func (p *Plane) Audit() *audit.Logger { return p.aud }

// Close stops background reload watching.
func (p *Plane) Close() error {
	p.cancel()
	p.wg.Wait()
	return nil
}

const maxOverHist = 100

// trimOverHist keeps the override timeline in lockstep with the store's
// retained snapshot window (newest maxOverHist entries). It never retains a
// version the store has already dropped: an orphan override would make the two
// histories disagree about which configurations can be rolled back.
func (p *Plane) trimOverHist() {
	if len(p.overHist) <= maxOverHist {
		return
	}
	vers := make([]int64, 0, len(p.overHist))
	for v := range p.overHist {
		vers = append(vers, v)
	}
	sort.Slice(vers, func(i, j int) bool { return vers[i] < vers[j] })
	drop := len(vers) - maxOverHist
	for _, v := range vers[:drop] {
		delete(p.overHist, v)
	}
}
