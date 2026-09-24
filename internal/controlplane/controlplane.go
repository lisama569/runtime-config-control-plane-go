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
	"github.com/lisama569/runtime-config-control-plane-go/internal/source"
	"github.com/lisama569/runtime-config-control-plane-go/internal/store"
)

// Options configures a Plane.
type Options struct {
	Sources      []source.Source
	Tokens       map[string]auth.Principal
	PollInterval time.Duration
}

// Plane is the assembled control plane.
type Plane struct {
	store   store.Store
	eval    *flag.Evaluator
	authz   *auth.Authorizer
	aud     *audit.Logger
	sources []source.Source

	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	layers   map[string]sourceDoc // latest accepted doc per source
	over     overrideState
	overHist map[int64]overrideState // override state per effective version
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
		eval:     flag.NewEvaluator(),
		authz:    auth.NewAuthorizer(opts.Tokens),
		aud:      audit.NewLogger(),
		sources:  sorted,
		layers:   map[string]sourceDoc{},
		over:     newOverrideState(),
		overHist: map[int64]overrideState{},
	}
	for _, src := range sorted {
		doc, ver, err := src.Load(ctx)
		if err != nil {
			return nil, err
		}
		p.layers[src.Name()] = sourceDoc{src: src, doc: doc, ver: ver}
	}
	res, err := p.remerge()
	if err != nil {
		return nil, err
	}
	snap := &config.Snapshot{
		Version:        1,
		Doc:            res.Doc,
		PropertyOrigin: res.PropertyOrigin,
		FeatureOrigin:  res.FeatureOrigin,
		SourceVersions: res.SourceVersions,
		CreatedAtUnix:  time.Now().Unix(),
	}
	p.store = store.NewMemory(snap)
	p.overHist[1] = p.over.clone()

	rctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	w := source.NewWatcher(sorted, opts.PollInterval)
	p.wg.Add(2)
	go p.reloadLoop(rctx, w, &p.wg)
	go func() { defer p.wg.Done(); _ = w.Run(rctx, p.eventCh(rctx)) }()
	return p, nil
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
		snap, cerr := p.store.CAS(ctx, cur.Version, res.Doc, store.Change{
			Reason:   "hot-reload:" + ev.Source.Name(),
			Actor:    "system",
			Origin:   res.PropertyOrigin,
			Features: res.FeatureOrigin,
		})
		if cerr == nil {
			p.mu.Lock()
			p.overHist[snap.Version] = p.over.clone()
			p.trimOverHist()
			p.mu.Unlock()
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
