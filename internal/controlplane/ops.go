package controlplane

import (
	"context"

	"github.com/lisama569/runtime-config-control-plane-go/internal/audit"
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
// Concurrency rule: the commit succeeds only if the live version still equals
// ExpectedVersion; otherwise a STALE_VERSION error is returned explicitly and
// NOTHING changes. Validation failures are all-or-nothing: no key from the
// request is applied if any key is invalid.
func (p *Plane) ApplyPatch(ctx context.Context, actor string, req PatchRequest) (from, to int64, err error) {
	p.commitMu.Lock()
	defer p.commitMu.Unlock()
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

	p.mu.Lock()
	next := p.over.clone()
	if verr := applyPropertyOps(cur.Doc, next, req.Properties); verr != nil {
		p.mu.Unlock()
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result: audit.ResultError, Reason: verr.Error(),
			ErrKind: errKindString(verr), FromVer: cur.Version,
		})
		return from, 0, verr
	}
	if verr := applyFeatureOps(cur.Doc, next, req.Features); verr != nil {
		p.mu.Unlock()
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result: audit.ResultError, Reason: verr.Error(),
			ErrKind: errKindString(verr), FromVer: cur.Version,
		})
		return from, 0, verr
	}
	// Build the exact effective document (sources + override + tombstones).
	res, rerr := p.remergeWithLocked(next)
	if rerr != nil {
		p.mu.Unlock()
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result: audit.ResultError, Reason: rerr.Error(),
			ErrKind: errKindString(rerr), FromVer: cur.Version,
		})
		return from, 0, rerr
	}
	newVersion := cur.Version + 1
	// Publish the candidate layer and pre-register it so the store's
	// pre-publish hook serializes exactly this state. p.over is reverted on
	// any failure below.
	p.over = next
	p.overHist[newVersion] = next
	p.mu.Unlock()

	snap, cerr := p.store.CAS(ctx, cur.Version, res.Doc, store.Change{
		Reason: "api-patch", Actor: actor,
		Origin: res.PropertyOrigin, Features: res.FeatureOrigin,
	})
	if cerr != nil {
		p.mu.Lock()
		delete(p.overHist, newVersion)
		if prev, ok := p.overHist[cur.Version]; ok {
			p.over = prev
		}
		p.mu.Unlock()
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result:  audit.ResultDenied,
			Reason:  cerr.Error(),
			ErrKind: errKindString(cerr), FromVer: cur.Version,
		})
		return from, 0, cerr
	}
	p.mu.Lock()
	p.trimOverHist()
	p.mu.Unlock()
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
	p.commitMu.Lock()
	defer p.commitMu.Unlock()
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
	p.mu.Lock()
	ov, known := p.overHist[targetVersion]
	if !known {
		// Should not happen while history bounds agree; fail closed if it does.
		p.mu.Unlock()
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actRollback), Target: itoa(targetVersion),
			Result: audit.ResultError, Reason: rollbackStateErr(targetVersion).Error(),
			ErrKind: "ROLLBACK_FAILED", FromVer: cur.Version,
		})
		return from, 0, rollbackStateErr(targetVersion)
	}
	p.over = ov.clone()
	newVersion := cur.Version + 1
	// Pre-register so the pre-publish hook serializes exactly this state.
	p.overHist[newVersion] = ov.clone()
	p.mu.Unlock()

	snap, cerr := p.store.Rollback(ctx, targetVersion, actor)
	if cerr != nil {
		p.mu.Lock()
		delete(p.overHist, newVersion)
		// Restore the live override layer of the still-current version.
		if prev, ok := p.overHist[cur.Version]; ok {
			p.over = prev
		}
		p.mu.Unlock()
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actRollback), Target: itoa(targetVersion),
			Result: audit.ResultError, Reason: cerr.Error(),
			ErrKind: errKindString(cerr), FromVer: cur.Version,
		})
		return from, 0, cerr
	}
	p.mu.Lock()
	p.trimOverHist()
	p.mu.Unlock()
	_, _ = p.aud.Record(ctx, audit.Entry{
		Actor: actor, Action: string(actRollback), Target: itoa(targetVersion),
		Result:  audit.ResultAllowed,
		Reason:  "full effective configuration restored from version " + itoa(targetVersion),
		FromVer: from, ToVer: snap.Version,
	})
	return from, snap.Version, nil
}
