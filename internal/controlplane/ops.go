package controlplane

import (
	"context"

	"github.com/lisama569/runtime-config-control-plane-go/internal/audit"
	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/store"
)

// PatchOp is one property upsert/delete in a write request.
type PatchOp struct {
	Key    string  `json:"key"`
	Type   string  `json:"type,omitempty"` // string|int|float|bool|sensitive
	Value  jsonRaw `json:"value,omitempty"`
	Delete bool    `json:"delete,omitempty"`
}

// FeatureOp is one feature upsert/delete in a write request.
type FeatureOp struct {
	Name       string   `json:"name"`
	Enabled    *bool    `json:"enabled,omitempty"`
	Percentage *int     `json:"percentage,omitempty"`
	AllowList  []string `json:"allow_list,omitempty"`
	Delete     bool     `json:"delete,omitempty"`
}

// PatchRequest is an optimistic-concurrency write.
type PatchRequest struct {
	ExpectedVersion int64       `json:"expected_version"`
	Properties      []PatchOp   `json:"properties,omitempty"`
	Features        []FeatureOp `json:"features,omitempty"`
}

// jsonRaw re-exports json.RawMessage for request decoding.
type jsonRaw = rawMessage

// ApplyPatch validates and commits an authorized write.
// Concurrency rule: an advisory version precheck happens lock-free (the live
// snapshot is an atomic pointer), then the whole read-modify-commit sequence
// is serialized by p.mu — including the authoritative version check and the
// durability hook. A request that loses the race therefore fails with
// STALE_VERSION inside the serialized section BEFORE any shared state can be
// touched, and changes nothing: not the live override layer, not retained
// history, not durable state. Validation failures are all-or-nothing.
func (p *Plane) ApplyPatch(ctx context.Context, actor string, req PatchRequest) (from, to int64, err error) {
	// Advisory precheck against the wait-free live snapshot. It only rejects
	// obviously stale requests early; the authoritative check is repeated
	// under the lock, where losing racers are stopped before mutating anything.
	cur0 := p.store.Current()
	if req.ExpectedVersion != cur0.Version {
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result:  audit.ResultDenied,
			Reason:  "expected version " + itoa(req.ExpectedVersion) + " but current is " + itoa(cur0.Version),
			ErrKind: "STALE_VERSION", FromVer: cur0.Version,
		})
		return cur0.Version, 0, staleErr(cur0.Version, req.ExpectedVersion)
	}
	if p.testPreCommit != nil {
		p.testPreCommit()
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	cur := p.store.Current()
	from = cur.Version
	if req.ExpectedVersion != cur.Version {
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result:  audit.ResultDenied,
			Reason:  "expected version " + itoa(req.ExpectedVersion) + " but current is " + itoa(cur.Version),
			ErrKind: "STALE_VERSION", FromVer: cur.Version,
		})
		return from, 0, staleErr(cur.Version, req.ExpectedVersion)
	}

	// Build the candidate entirely from LOCAL state. The live override layer
	// is not touched until the commit has succeeded, so a losing/aborted
	// transaction cannot leak any value into the shared state.
	next := p.over.clone()
	cand := cloneDoc(cur.Doc)
	if verr := applyPropertyOps(cand, next, req.Properties); verr != nil {
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result: audit.ResultError, Reason: verr.Error(),
			ErrKind: errKindString(verr), FromVer: cur.Version,
		})
		return from, 0, verr
	}
	if verr := applyFeatureOps(cand, next, req.Features); verr != nil {
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result: audit.ResultError, Reason: verr.Error(),
			ErrKind: errKindString(verr), FromVer: cur.Version,
		})
		return from, 0, verr
	}

	// Re-verify against a freshly remerged document so source-derived entries
	// and tombstones are represented exactly — using the tentative override,
	// never the shared live layer.
	res, rerr := p.remergeWithLocked(next)
	if rerr != nil {
		return from, 0, rerr
	}

	newVersion := cur.Version + 1
	p.beginTxLocked(next)
	// Seed the tentative durable timeline so the hook can persist the full
	// window atomically; it is discarded on any failure.
	p.txHist[newVersion] = next.clone()
	snap, cerr := p.store.CAS(ctx, cur.Version, res.Doc, store.Change{
		Reason: "api-patch", Actor: actor,
		Origin: res.PropertyOrigin, Features: res.FeatureOrigin,
	})
	if cerr != nil {
		// Lost the race or durability failed: no snapshot was published and no
		// durable state changed. Discard the tentative layer entirely; a failed
		// request can never roll back or corrupt a previously successful commit.
		p.abortTxLocked()
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result: audit.ResultDenied, Reason: cerr.Error(),
			ErrKind: errKindString(cerr), FromVer: cur.Version,
		})
		return from, 0, cerr
	}
	p.commitTxLocked(snap.Version)
	_, _ = p.aud.Record(ctx, audit.Entry{
		Actor: actor, Action: string(actWrite), Target: "config",
		Result:  audit.ResultAllowed,
		Reason:  "optimistic write committed",
		FromVer: from, ToVer: snap.Version,
	})
	return from, snap.Version, nil
}

// Rollback restores the COMPLETE effective configuration recorded at
// targetVersion and publishes it under a brand-new version. The API override
// state associated with that version is restored as well, so the resulting
// effective config is identical to the target's and remains explainable.
func (p *Plane) Rollback(ctx context.Context, actor string, targetVersion int64) (from, to int64, err error) {
	if p.testPreCommit != nil {
		p.testPreCommit()
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	cur := p.store.Current()
	from = cur.Version
	if _, ok := p.store.AtVersion(targetVersion); !ok {
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actRollback), Target: itoa(targetVersion),
			Result:  audit.ResultError,
			Reason:  "target version not retained",
			ErrKind: "ROLLBACK_FAILED", FromVer: cur.Version,
		})
		return from, 0, rollbackErr(targetVersion)
	}
	ov, known := p.overHist[targetVersion]
	if !known {
		// Should not happen while history bounds agree; fail closed if it does.
		return from, 0, rollbackStateErr(targetVersion)
	}

	newVersion := cur.Version + 1
	p.beginTxLocked(ov)
	p.txHist[newVersion] = ov.clone()
	snap, cerr := p.store.Rollback(ctx, targetVersion, actor)
	if cerr != nil {
		p.abortTxLocked()
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actRollback), Target: itoa(targetVersion),
			Result: audit.ResultError, Reason: cerr.Error(),
			ErrKind: errKindString(cerr), FromVer: cur.Version,
		})
		return from, 0, cerr
	}
	p.commitTxLocked(snap.Version)
	_, _ = p.aud.Record(ctx, audit.Entry{
		Actor: actor, Action: string(actRollback), Target: itoa(targetVersion),
		Result:  audit.ResultAllowed,
		Reason:  "full effective configuration restored from version " + itoa(targetVersion),
		FromVer: from, ToVer: snap.Version,
	})
	return from, snap.Version, nil
}

func cloneDoc(d *config.Document) *config.Document {
	c := &config.Document{
		Properties: make(map[string]config.Property, len(d.Properties)),
		Features:   make(map[string]config.Feature, len(d.Features)),
	}
	for k, v := range d.Properties {
		c.Properties[k] = v
	}
	for k, v := range d.Features {
		v.AllowList = cloneBoolSet(v.AllowList)
		c.Features[k] = v
	}
	return c
}

func cloneBoolSet(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}
