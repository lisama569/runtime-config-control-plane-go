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
// Concurrency rule: the commit succeeds only if the live version still equals
// ExpectedVersion; otherwise a STALE_VERSION error is returned explicitly and
// NOTHING changes. Validation failures are all-or-nothing: no key from the
// request is applied if any key is invalid.
func (p *Plane) ApplyPatch(ctx context.Context, actor string, req PatchRequest) (from, to int64, err error) {
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
	// Build candidate effective document from current snapshot, then apply ops.
	cand := cloneDoc(cur.Doc)
	if err := applyPropertyOps(cand, next, req.Properties); err != nil {
		p.mu.Unlock()
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result: audit.ResultError, Reason: err.Error(),
			ErrKind: errKindString(err), FromVer: cur.Version,
		})
		return from, 0, err
	}
	if err := applyFeatureOps(cand, next, req.Features); err != nil {
		p.mu.Unlock()
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result: audit.ResultError, Reason: err.Error(),
			ErrKind: errKindString(err), FromVer: cur.Version,
		})
		return from, 0, err
	}
	p.over = next
	p.mu.Unlock()

	// Verify against a freshly remerged document so source-derived entries and
	// tombstones are represented exactly.
	res, err := p.remerge()
	if err != nil {
		return from, 0, err
	}
	// Stage the override state for the version we are about to publish so the
	// durability hook (run inside the store's commit) can persist it before
	// the new snapshot becomes visible.
	newVersion := cur.Version + 1
	p.stagePending(newVersion, next)
	snap, cerr := p.store.CAS(ctx, cur.Version, res.Doc, store.Change{
		Reason: "api-patch", Actor: actor,
		Origin: res.PropertyOrigin, Features: res.FeatureOrigin,
	})
	p.clearPending(newVersion)
	if cerr != nil {
		// Lost the race (or durability failed): revert in-memory override and
		// demand client retry. Nothing was published or made durable.
		p.mu.Lock()
		if prev, ok := p.overHist[cur.Version]; ok {
			p.over = prev
		}
		p.mu.Unlock()
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result: audit.ResultDenied, Reason: cerr.Error(),
			ErrKind: errKindString(cerr), FromVer: cur.Version,
		})
		return from, 0, cerr
	}
	// Override history for snap.Version was already committed inside the
	// store's durability hook (atomically with persistence).
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
	// Remember the current override so a failed/durability-aborted rollback
	// restores in-memory state to match the unchanged live snapshot.
	curOv, curKnown := p.overHist[cur.Version]
	if known {
		p.over = ov.clone()
	} else {
		// Should not happen while history bounds agree; fail closed if it does.
		p.mu.Unlock()
		return from, 0, rollbackStateErr(targetVersion)
	}
	p.mu.Unlock()

	newVersion := cur.Version + 1
	p.stagePending(newVersion, ov)
	snap, cerr := p.store.Rollback(ctx, targetVersion, actor)
	p.clearPending(newVersion)
	if cerr != nil {
		if curKnown {
			p.mu.Lock()
			p.over = curOv.clone()
			p.mu.Unlock()
		}
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actRollback), Target: itoa(targetVersion),
			Result: audit.ResultError, Reason: cerr.Error(),
			ErrKind: errKindString(cerr), FromVer: cur.Version,
		})
		return from, 0, cerr
	}
	// Override history for the new version was committed in the hook.
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
