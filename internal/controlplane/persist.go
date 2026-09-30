package controlplane

import (
	"context"
	"log"

	"github.com/lisama569/runtime-config-control-plane-go/internal/audit"
	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
	"github.com/lisama569/runtime-config-control-plane-go/internal/persist"
	"github.com/lisama569/runtime-config-control-plane-go/internal/store"
)

// auditSinkAdapter bridges audit.Sink (Append) to persist.Store (AppendAudit).
type auditSinkAdapter struct{ ps *persist.Store }

func (a auditSinkAdapter) Append(ctx context.Context, e audit.Entry) error {
	return a.ps.AppendAudit(ctx, e)
}

// toPersistOverride converts the in-memory override layer to its durable form.
func toPersistOverride(o overrideState) persist.Override {
	return persist.Override{
		Props:     o.props,
		Feats:     o.feats,
		DeadProps: o.deadProps,
		DeadFeats: o.deadFeats,
	}
}

func fromPersistOverride(o persist.Override) overrideState {
	ov := newOverrideState()
	for k, v := range o.Props {
		ov.props[k] = v
	}
	for k, v := range o.Feats {
		ov.feats[k] = v
	}
	for k, dead := range o.DeadProps {
		if dead {
			ov.deadProps[k] = true
		}
	}
	for k, dead := range o.DeadFeats {
		if dead {
			ov.deadFeats[k] = true
		}
	}
	return ov
}

// persistHook builds the store.BeforeCommit durability hook. The store invokes
// the hook inside CAS/Rollback while holding its own mutex; every control-plane
// transition calls those with p.mu already held, so the single, consistent
// lock order is p.mu -> store.mu and the hook must NOT re-acquire p.mu.
func (p *Plane) persistHook() store.BeforeCommit {
	return func(ctx context.Context, snap *config.Snapshot, history []*config.Snapshot) error {
		// saveDurable verifies the staging slot, writes state, and commits the
		// override-history entry atomically (all under the held p.mu).
		return p.saveDurable(ctx, snap, history)
	}
}

func persistStateErr(v int64) error {
	return errs.Wrap(errs.KindPersistIO,
		"controlplane: cannot stage override state for version "+itoa(v), nil)
}

// logRecovery reports recovery decisions through the configured logger.
func (p *Plane) logRecovery(rep *persist.RecoveryReport) {
	if rep == nil {
		return
	}
	if !rep.Loaded {
		p.plog().Printf("persist: fresh data directory, bootstrapping from sources at version 1")
		return
	}
	via := "state.json"
	if rep.UsedBackup {
		via = "state.json.bak (primary was damaged)"
	}
	p.plog().Printf("persist: recovered effective version %d from %s; replayed %d audit entries",
		rep.StateVersion, via, rep.AuditEntries)
	for _, prob := range rep.Problems {
		p.plog().Printf("persist recovery [%s]: %s", prob.Kind, prob.Message)
	}
}

func (p *Plane) plog() persistLogger {
	if p.persistLog != nil {
		return p.persistLog
	}
	return log.Default()
}
