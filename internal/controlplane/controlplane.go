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
	// writes, rollbacks and hot reloads). All read-modify-commit work happens
	// while holding it, including the durability hook invoked by the store
	// (lock order is therefore always mu -> store.mu, never the reverse), so
	// two transitions can never interleave and a losing/aborted transaction
	// can never mutate the shared layer state.
	mu       sync.Mutex
	layers   map[string]sourceDoc    // latest accepted doc per source
	over     overrideState           // live api-override layer
	overHist map[int64]overrideState // override state per effective version
	txOver   *overrideState          // non-nil only inside an in-flight commit: the override that goes with the candidate snapshot
	txHist   map[int64]overrideState // tentative history staged for the in-flight commit

	// testPreCommit is a TEST-ONLY seam: when non-nil it is invoked inside the
	// serialized commit section just before durability/visibility, letting
	// tests force deterministic goroutine interleavings. Production code never
	// sets it.
	testPreCommit func()
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
		eval:       flag.NewEvaluator(),
		authz:      auth.NewAuthorizer(opts.Tokens),
		aud:        audit.NewLogger(),
		sources:    sorted,
		layers:     map[string]sourceDoc{},
		over:       newOverrideState(),
		overHist:   map[int64]overrideState{},
		persistLog: opts.PersistLogger,
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
		p.store = store.NewMemoryWithHook(snap, p.persistHook())
		if p.persist != nil {
			// Make the initial bootstrap durable BEFORE serving. Failure here
			// aborts startup (fail closed) rather than serving non-durable v1.
			if err := p.writeStateLocked(context.Background(), snap,
				[]*config.Snapshot{snap}, p.overHist[1]); err != nil {
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

// beginTxLocked stages a candidate override state for the in-flight commit.
// The caller MUST hold mu and MUST call either commitTxLocked (after a
// successful durable commit/visibility) or abortTxLocked (on any failure).
// Nothing staged here is observable by another transition until commit.
func (p *Plane) beginTxLocked(ov overrideState) {
	ov = ov.clone()
	p.txOver = &ov
	p.txHist = cloneOverHist(p.overHist)
}

// commitTxLocked publishes the staged override layer/timeline. It runs after
// the store has made the new snapshot durable and visible, so the in-memory
// override layer can never describe a different config than the live snapshot.
func (p *Plane) commitTxLocked(newVersion int64) {
	if p.txOver == nil {
		panic("controlplane: commitTxLocked without beginTxLocked")
	}
	p.over = p.txOver.clone()
	p.overHist[newVersion] = p.txOver.clone()
	p.trimOverHist()
	p.txOver = nil
	p.txHist = nil
}

// abortTxLocked discards the staged transaction, leaving the live override
// layer and timeline exactly as they were. A rejected/stale/failed request
// therefore leaves no trace whatsoever.
func (p *Plane) abortTxLocked() {
	p.txOver = nil
	p.txHist = nil
}

// writeStateLocked serializes snap together with the override state that
// describes it. It is called either for the initial bootstrap or from the
// store's pre-commit hook; in both cases mu is held and the override passed in
// is exactly the one the published snapshot was built from, so what clients
// see, what lands on disk and what a later restart recovers are one and the
// same configuration.
func (p *Plane) writeStateLocked(ctx context.Context, snap *config.Snapshot, history []*config.Snapshot, ov overrideState) error {
	if p.persist == nil {
		return nil
	}
	// Prefer the staged tentative timeline (covers the in-flight version); for
	// the bootstrap call use the live timeline plus the explicit override.
	ovm := p.txHist
	if ovm == nil {
		ovm = map[int64]overrideState{}
		for v, o := range p.overHist {
			ovm[v] = o
		}
	}
	if _, known := ovm[snap.Version]; !known {
		ovm[snap.Version] = ov
	}
	st := &persist.State{
		Current:   snap,
		History:   history,
		Overrides: map[int64]persist.Override{},
	}
	for _, h := range history {
		hov, known := ovm[h.Version]
		if !known {
			return persistStateErr(h.Version)
		}
		st.Overrides[h.Version] = toPersistOverride(hov)
	}
	return p.persist.SaveState(ctx, st)
}

func cloneOverHist(m map[int64]overrideState) map[int64]overrideState {
	out := make(map[int64]overrideState, len(m)+1)
	for v, o := range m {
		out[v] = o.clone()
	}
	return out
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
// The caller holds mu.
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
			p.applySourceEvent(ctx, ev)
		}
	}
}

// applySourceEvent merges a freshly accepted source document into the
// effective config. The layer swap plus the read-modify-commit sequence
// (including the durability hook) is serialized by mu, so a hot reload can
// never interleave with an API write: a reload racing a committed write loses
// the CAS and is retried from the newest snapshot, while a reload that wins is
// the only transition of its version. The new layer is installed inside the
// same section as the commit, and reverted on failure, so a losing/aborted
// reload cannot leak its document.
func (p *Plane) applySourceEvent(ctx context.Context, ev source.UpdateEvent) {
	// Test seam (outside the lock): lets concurrency tests line up a reload and
	// writes at the same starting version before any of them enters the section.
	if p.testPreCommit != nil {
		p.testPreCommit()
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	name := ev.Source.Name()
	prev := p.layers[name]
	p.layers[name] = sourceDoc{src: ev.Source, doc: ev.Doc, ver: ev.Version}

	for attempt := 0; ; attempt++ {
		cur := p.store.Current()
		res, err := p.remergeLocked()
		if err != nil {
			// Invalid merge candidate: keep the source's last accepted layer
			// so the last valid effective config is retained, audit and stop.
			p.layers[name] = prev
			_, _ = p.aud.Record(ctx, audit.Entry{
				Actor: "system", Action: "source:reload", Target: name,
				Result: audit.ResultError, Reason: err.Error(),
				ErrKind: errKindString(err), FromVer: cur.Version,
			})
			return
		}
		p.beginTxLocked(p.over)
		snap, cerr := p.store.CAS(ctx, cur.Version, res.Doc, store.Change{
			Reason:   "hot-reload:" + ev.Source.Name(),
			Actor:    "system",
			Origin:   res.PropertyOrigin,
			Features: res.FeatureOrigin,
		})
		if cerr == nil {
			p.commitTxLocked(snap.Version)
			_, _ = p.aud.Record(ctx, audit.Entry{
				Actor: "system", Action: "source:reload", Target: ev.Source.Name(),
				Result:  audit.ResultAllowed,
				Reason:  "merged at source version " + itoa(ev.Version),
				FromVer: cur.Version, ToVer: snap.Version,
			})
			return
		}
		p.abortTxLocked()
		if !errIs(cerr, errKindStale) {
			_, _ = p.aud.Record(ctx, audit.Entry{
				Actor: "system", Action: "source:reload", Target: ev.Source.Name(),
				Result: audit.ResultError, Reason: cerr.Error(),
				ErrKind: errKindString(cerr), FromVer: cur.Version,
			})
			return
		}
		// Another serialized transition landed first: remerge from the newest
		// snapshot and retry with the same source layer. Bounded so a persist
		// storm cannot wedge the reload goroutine forever.
		if attempt > 16 {
			return
		}
	}
}

// remergeLocked rebuilds the effective document from the current source
// layers plus the live override layer. Callers must hold mu.
func (p *Plane) remergeLocked() (*merge.Result, error) {
	return p.remergeWithLocked(p.over)
}

// remergeWithLocked is remergeLocked against an explicit override state, used
// by in-flight API transactions without touching the live layer.
func (p *Plane) remergeWithLocked(over overrideState) (*merge.Result, error) {
	ordered := make([]merge.Layered, 0, len(p.sources)+1)
	overDoc := over.emptyDoc()
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
	for k := range over.deadProps {
		if _, ok := res.Doc.Properties[k]; ok {
			delete(res.Doc.Properties, k)
			res.PropertyOrigin[k] = "api-override(deleted)"
		}
	}
	for k := range over.deadFeats {
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
