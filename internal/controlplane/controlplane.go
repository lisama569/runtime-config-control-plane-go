package controlplane

import (
	"context"
	"fmt"
	"reflect"

	"github.com/lisama569/runtime-config-control-plane-go/internal/audit"
	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/flag"
	"github.com/lisama569/runtime-config-control-plane-go/internal/security"
	"github.com/lisama569/runtime-config-control-plane-go/internal/source"
	"github.com/lisama569/runtime-config-control-plane-go/internal/store"
)

// Plane wires sources, store, evaluator, RBAC and audit together.
type Plane struct {
	sources []source.Source
	store   *store.Store
	eval    *flag.Evaluator
	authz   *security.Authorizer
	audit   *audit.Logger
}

// Options constructs a Plane.
type Options struct {
	Sources []source.Source
	Audit   *audit.Logger
}

// New wires a control plane and performs a best-effort initial load as
// actor "system". Failures are audited and leave the seed version 0 in
// place.
func New(opts Options) *Plane {
	p := &Plane{
		sources: opts.Sources,
		store:   store.New(),
		eval:    flag.NewEvaluator(),
		authz:   security.NewAuthorizer(),
		audit:   opts.Audit,
	}
	if p.audit == nil {
		p.audit = audit.New(1024, nil)
	}
	if len(p.sources) > 0 {
		_, _ = p.ReloadLatest(context.Background(), "system", "initial load")
	}
	return p
}

// Reload loads all sources, merges and commits them under strict CAS.
// Invalid merges are rejected and leave the last good snapshot untouched.
func (p *Plane) Reload(ctx context.Context, actor, reason string, expectedVersion int64) (config.Snapshot, error) {
	parsed, err := source.LoadAll(ctx, p.sources)
	if err != nil {
		p.audit.Record(actor, "config.reload", audit.OutcomeReject,
			audit.WithCategory(string(config.CatValidation)),
			audit.WithReason("load failed: "+err.Error()))
		return config.Snapshot{}, config.NewError(config.CatValidation, "load sources: %v", err)
	}
	return p.commitMerged(ctx, actor, reason, parsed, expectedVersion, true)
}

// ReloadLatest merges sources and commits against whatever the current
// version is, retrying on conflict. Used by the file watcher.
func (p *Plane) ReloadLatest(ctx context.Context, actor, reason string) (config.Snapshot, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		parsed, err := source.LoadAll(ctx, p.sources)
		if err != nil {
			p.audit.Record(actor, "config.reload", audit.OutcomeReject,
				audit.WithCategory(string(config.CatValidation)),
				audit.WithReason("load failed: "+err.Error()))
			return config.Snapshot{}, config.NewError(config.CatValidation, "load sources: %v", err)
		}
		snap, err := p.commitMerged(ctx, actor, reason, parsed, p.store.Current().Version, false)
		if err == nil {
			return snap, nil
		}
		if config.CatOf(err) != config.CatConflict {
			return snap, err
		}
		lastErr = err
	}
	return config.Snapshot{}, lastErr
}

func (p *Plane) commitMerged(ctx context.Context, actor, reason string, parsed []source.ParsedSource, expectedVersion int64, strict bool) (config.Snapshot, error) {
	merged, err := source.Merge(parsed)
	if err != nil {
		p.audit.Record(actor, "config.reload", audit.OutcomeReject,
			audit.WithCategory(string(config.CatOf(err))),
			audit.WithReason("merge rejected: "+err.Error()))
		return config.Snapshot{}, config.NewError(config.CatValidation, "%v", err)
	}
	from := p.store.Current().Version
	// Idempotent skip: identical content never creates a new version.
	if cur := p.store.Current(); reflect.DeepEqual(cur.Config, merged.Config) {
		p.audit.Record(actor, "config.reload", audit.OutcomeSuccess,
			audit.WithVersions(from, from),
			audit.WithReason(reason),
			audit.WithDetail("no-op: merged content identical to current"))
		return cur, nil
	}
	snap, err := p.store.Commit(ctx, merged.Config, expectedVersion, actor, reason, merged.Summary())
	if err != nil {
		p.audit.Record(actor, "config.reload", audit.OutcomeReject,
			audit.WithCategory(string(config.CatOf(err))),
			audit.WithVersions(from, from),
			audit.WithReason(err.Error()))
		return config.Snapshot{}, err
	}
	p.audit.Record(actor, "config.reload", audit.OutcomeSuccess,
		audit.WithVersions(from, snap.Version),
		audit.WithReason(reason),
		audit.WithDetail("provenance: "+merged.Summary()))
	return snap, nil
}

// ReadConfig returns the effective snapshot, masked for the principal.
func (p *Plane) ReadConfig(pri security.Principal) (config.Snapshot, error) {
	if err := p.authz.Authorize(pri, security.ActionReadConfig); err != nil {
		p.audit.Record(pri.Actor(), "config.read", audit.OutcomeDenied,
			audit.WithCategory(string(config.CatOf(err))))
		return config.Snapshot{}, err
	}
	snap := p.store.Current()
	snap.Config = security.MaskConfig(snap.Config, p.authz.Can(pri, security.ActionReadSecret))
	return snap, nil
}

// Write validates then conditionally commits an externally edited config.
func (p *Plane) Write(ctx context.Context, pri security.Principal, next config.Config, expectedVersion int64, reason string) (config.Snapshot, error) {
	if err := p.authz.Authorize(pri, security.ActionWriteConfig); err != nil {
		p.audit.Record(pri.Actor(), "config.write", audit.OutcomeDenied,
			audit.WithCategory(string(config.CatOf(err))))
		return config.Snapshot{}, err
	}
	from := p.store.Current().Version
	snap, err := p.store.Commit(ctx, next, expectedVersion, pri.Actor(), reason, "api-write")
	if err != nil {
		p.audit.Record(pri.Actor(), "config.write", audit.OutcomeReject,
			audit.WithCategory(string(config.CatOf(err))),
			audit.WithVersions(from, from),
			audit.WithReason(err.Error()))
		return config.Snapshot{}, err
	}
	p.audit.Record(pri.Actor(), "config.write", audit.OutcomeSuccess,
		audit.WithVersions(from, snap.Version),
		audit.WithReason(reason))
	return snap, nil
}

// Rollback reinstates a historical version.
func (p *Plane) Rollback(ctx context.Context, pri security.Principal, targetVersion, expectedVersion int64, reason string) (config.Snapshot, error) {
	if err := p.authz.Authorize(pri, security.ActionRollback); err != nil {
		p.audit.Record(pri.Actor(), "config.rollback", audit.OutcomeDenied,
			audit.WithCategory(string(config.CatOf(err))))
		return config.Snapshot{}, err
	}
	from := p.store.Current().Version
	snap, err := p.store.Rollback(ctx, targetVersion, expectedVersion, pri.Actor(), reason)
	if err != nil {
		p.audit.Record(pri.Actor(), "config.rollback", audit.OutcomeReject,
			audit.WithCategory(string(config.CatOf(err))),
			audit.WithVersions(from, from),
			audit.WithReason(err.Error()))
		return config.Snapshot{}, err
	}
	p.audit.Record(pri.Actor(), "config.rollback", audit.OutcomeSuccess,
		audit.WithVersions(from, snap.Version),
		audit.WithReason(fmt.Sprintf("rollback to version %d: %s", targetVersion, reason)))
	return snap, nil
}

// Evaluate answers a feature flag question against the current snapshot.
func (p *Plane) Evaluate(pri security.Principal, flagName, subject string) (flag.Decision, config.Snapshot, error) {
	if err := p.authz.Authorize(pri, security.ActionEvaluateFlag); err != nil {
		p.audit.Record(pri.Actor(), "flag.evaluate", audit.OutcomeDenied,
			audit.WithCategory(string(config.CatOf(err))),
			audit.WithResource(flagName))
		return flag.Decision{}, config.Snapshot{}, err
	}
	snap := p.store.Current()
	return p.eval.Evaluate(snap, flagName, subject), snap, nil
}

// History lists version metadata.
func (p *Plane) History(pri security.Principal, limit int) ([]config.Snapshot, error) {
	if err := p.authz.Authorize(pri, security.ActionReadConfig); err != nil {
		p.audit.Record(pri.Actor(), "config.history", audit.OutcomeDenied,
			audit.WithCategory(string(config.CatOf(err))))
		return nil, err
	}
	return p.store.History(limit), nil
}

// AuditLog returns audit entries (requires audit:read).
func (p *Plane) AuditLog(pri security.Principal, limit int) ([]audit.Entry, error) {
	if err := p.authz.Authorize(pri, security.ActionReadAudit); err != nil {
		p.audit.Record(pri.Actor(), "audit.read", audit.OutcomeDenied,
			audit.WithCategory(string(config.CatOf(err))))
		return nil, err
	}
	return p.audit.Query(limit, ""), nil
}

// CurrentVersion returns the current version number.
func (p *Plane) CurrentVersion() int64 { return p.store.Current().Version }

// Store exposes the underlying store for tests and the watcher.
func (p *Plane) Store() *store.Store { return p.store }
