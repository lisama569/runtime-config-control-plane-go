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

	mu          sync.Mutex
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
		snap, err := p.bootstrapFromSourcesLocked()
		if err != nil {
			return nil, err
		}
		p.overHist[1] = p.over.clone()
		var hook store.BeforeCommit
		if p.persist != nil {
			hook = p.persistHook()
		}
		p.store = store.NewMemoryWithHook(snap, hook)
		if p.persist != nil {
			// Make the initial bootstrap durable BEFORE serving. Failure here
			// aborts startup (fail closed) rather than serving non-durable v1.
			p.stagePending(snap.Version, p.overHist[1])
			if err := p.saveDurable(ctx, snap, []*config.Snapshot{snap}); err != nil {
				return nil, err
			}
		}
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

// stagePending records the override state for the version a commit is about
// to publish, so the durability hook can persist it inside the store section.
func (p *Plane) stagePending(v int64, ov overrideState) {
	p.mu.Lock()
	p.pendingOver[v] = ov.clone()
	p.mu.Unlock()
}

// clearPending drops the staged entry after a finished/failed commit.
func (p *Plane) clearPending(v int64) {
	p.mu.Lock()
	delete(p.pendingOver, v)
	p.mu.Unlock()
}

// saveDurable assembles the persist.State for snap + retained history and
// writes it through the durability backend. It runs inside the store's
// serialized commit (holding p.mu), so committing the new override-history
// entry here makes it visible to every later commit's durability write even
// when two clients race: a later hook can never observe a missing entry.
func (p *Plane) saveDurable(ctx context.Context, snap *config.Snapshot, history []*config.Snapshot) error {
	if p.persist == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if ov, ok := p.pendingOver[snap.Version]; ok {
		// Commit the staged override state into the timeline atomically with
		// durability, then release the staging slot.
		p.overHist[snap.Version] = ov
		p.trimOverHist()
		delete(p.pendingOver, snap.Version)
	}
	st := &persist.State{
		Current:   snap,
		History:   history,
		Overrides: map[int64]persist.Override{},
	}
	for _, h := range history {
		hov, known := p.overHist[h.Version]
		if !known {
			return persistStateErr(h.Version)
		}
		st.Overrides[h.Version] = toPersistOverride(hov)
	}
	return p.persist.SaveState(ctx, st)
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
func (p *Plane) bootstrapFromSourcesLocked() (*config.Snapshot, error) {
	res, err := p.remerge()
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
			p.applySourceEvent(ctx, ev)
		}
	}
}

func (p *Plane) applySourceEvent(ctx context.Context, ev source.UpdateEvent) {
	p.mu.Lock()
	p.layers[ev.Source.Name()] = sourceDoc{src: ev.Source, doc: ev.Doc, ver: ev.Version}
	p.mu.Unlock()

	// Serialized retry: rebuild + CAS until committed or rejected. The store
	// guarantees concurrent writers never silently overwrite each other.
	for attempt := 0; ; attempt++ {
		cur := p.store.Current()
		p.mu.Lock()
		res, err := p.remergeLocked()
		p.mu.Unlock()
		if err != nil {
			_, _ = p.aud.Record(ctx, audit.Entry{
				Actor: "system", Action: "source:reload", Target: ev.Source.Name(),
				Result: audit.ResultError, Reason: err.Error(),
				ErrKind: errKindString(err), FromVer: cur.Version,
			})
			return
		}
		p.stagePending(cur.Version+1, p.over)
		snap, cerr := p.store.CAS(ctx, cur.Version, res.Doc, store.Change{
			Reason:   "hot-reload:" + ev.Source.Name(),
			Actor:    "system",
			Origin:   res.PropertyOrigin,
			Features: res.FeatureOrigin,
		})
		p.clearPending(cur.Version + 1)
		if cerr == nil {
			// Override history committed inside the durability hook.
			_, _ = p.aud.Record(ctx, audit.Entry{
				Actor: "system", Action: "source:reload", Target: ev.Source.Name(),
				Result:  audit.ResultAllowed,
				Reason:  "merged at source version " + itoa(ev.Version),
				FromVer: cur.Version, ToVer: snap.Version,
			})
			return
		}
		if !errIs(cerr, errKindStale) {
			_, _ = p.aud.Record(ctx, audit.Entry{
				Actor: "system", Action: "source:reload", Target: ev.Source.Name(),
				Result: audit.ResultError, Reason: cerr.Error(),
				ErrKind: errKindString(cerr), FromVer: cur.Version,
			})
			return
		}
		if attempt > 8 {
			return
		}
	}
}

// remerge rebuilds the effective document from sources + override layer.
func (p *Plane) remerge() (*merge.Result, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.remergeLocked()
}

func (p *Plane) remergeLocked() (*merge.Result, error) {
	ordered := make([]merge.Layered, 0, len(p.sources)+1)
	overDoc := p.over.emptyDoc()
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
	for k := range p.over.deadProps {
		if _, ok := res.Doc.Properties[k]; ok {
			delete(res.Doc.Properties, k)
			res.PropertyOrigin[k] = "api-override(deleted)"
		}
	}
	for k := range p.over.deadFeats {
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

func (p *Plane) trimOverHist() {
	if len(p.overHist) <= maxOverHist {
		return
	}
	// Collect versions, drop oldest beyond bound (version 1 retained).
	vers := make([]int64, 0, len(p.overHist))
	for v := range p.overHist {
		vers = append(vers, v)
	}
	sort.Slice(vers, func(i, j int) bool { return vers[i] < vers[j] })
	drop := len(vers) - maxOverHist
	for i := 0; i < drop; i++ {
		if vers[i] == 1 {
			continue
		}
		delete(p.overHist, vers[i])
	}
}
