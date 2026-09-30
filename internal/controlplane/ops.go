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
//
// The entire read-modify-write is one critical section serialized by p.mu:
// the live version is read, the candidate override built as a LOCAL value,
// validated, made durable and published without ever unlocking. Consequently:
//
//   - the commit succeeds only if the live version still equals
//     ExpectedVersion. Two requests carrying the same version are ordered: the
//     first wins, the second observes the advanced version and is rejected as
//     STALE_VERSION before touching any state, so the loser's keys can never
//     leak into the winner's snapshot, the durable file or a restart;
//   - validation is all-or-nothing: no key is applied if any key is invalid;
//   - a durability failure aborts before visibility: version, live config,
//     override layer and history all stay at the pre-commit state and the
//     client may safely retry the same expected version.
func (p *Plane) ApplyPatch(ctx context.Context, actor string, req PatchRequest) (from, to int64, err error) {
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

	// Build the candidate purely as local state. The live override p.over is
	// not assigned until the commit below succeeds, so a failed attempt cannot
	// leave partial state behind.
	next := p.over.clone()
	cand := cloneDoc(cur.Doc)
	if vErr := applyPropertyOps(cand, next, req.Properties); vErr != nil {
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result: audit.ResultError, Reason: vErr.Error(),
			ErrKind: errKindString(vErr), FromVer: cur.Version,
		})
		return from, 0, vErr
	}
	if vErr := applyFeatureOps(cand, next, req.Features); vErr != nil {
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result: audit.ResultError, Reason: vErr.Error(),
			ErrKind: errKindString(vErr), FromVer: cur.Version,
		})
		return from, 0, vErr
	}

	// Recompute the effective document from the candidate override and current
	// sources so file-derived entries and tombstones are represented exactly;
	// cand was only used as the validation workspace.
	res, mErr := p.remergeOverrideLocked(next)
	if mErr != nil {
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result: audit.ResultError, Reason: mErr.Error(),
			ErrKind: errKindString(mErr), FromVer: cur.Version,
		})
		return from, 0, mErr
	}

	newVersion := cur.Version + 1
	p.pendingOver[newVersion] = next
	// Advance the live override before the store commits, still holding p.mu:
	// the durability hook (invoked inside CAS) then persists exactly the
	// override backing the snapshot about to be published, and no reader can
	// observe either until the atomic pointer store. If CAS aborts (a
	// durability error; version mismatch is impossible while serialized), the
	// override is restored to the one backing the unchanged current snapshot.
	p.over = next
	snap, cerr := p.store.CAS(ctx, cur.Version, res.Doc, store.Change{
		Reason: "api-patch", Actor: actor,
		Origin: res.PropertyOrigin, Features: res.FeatureOrigin,
	})
	delete(p.pendingOver, newVersion)
	if cerr != nil {
		if prev, ok := p.overHist[cur.Version]; ok {
			p.over = prev
		}
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actWrite), Target: "config",
			Result: audit.ResultDenied, Reason: cerr.Error(),
			ErrKind: errKindString(cerr), FromVer: cur.Version,
		})
		return from, 0, cerr
	}

	// The hook committed next into overHist atomically with the durability
	// write, and p.over already equals it: served config and durable state are
	// identical.
	_, _ = p.aud.Record(ctx, audit.Entry{
		Actor: actor, Action: string(actWrite), Target: "config",
		Result:  audit.ResultAllowed,
		Reason:  "optimistic write committed",
		FromVer: from, ToVer: snap.Version,
	})
	return from, snap.Version, nil
}

// Rollback restores the COMPLETE effective configuration recorded at
// targetVersion and publishes it under a brand-new linear version. The API
// override state associated with the target is restored as well, so the new
// effective config is identical to the target's and stays explainable. The
// whole transition runs serialized under p.mu like ApplyPatch; a failure
// leaves the live snapshot and override layer untouched.
func (p *Plane) Rollback(ctx context.Context, actor string, targetVersion int64) (from, to int64, err error) {
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
		// Fail closed if the override window disagrees with the store window.
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actRollback), Target: itoa(targetVersion),
			Result:  audit.ResultError,
			Reason:  "target override state not retained",
			ErrKind: "ROLLBACK_FAILED", FromVer: cur.Version,
		})
		return from, 0, rollbackStateErr(targetVersion)
	}

	newVersion := cur.Version + 1
	candidate := ov.clone()
	p.pendingOver[newVersion] = candidate
	p.over = candidate
	snap, cerr := p.store.Rollback(ctx, targetVersion, actor)
	delete(p.pendingOver, newVersion)
	if cerr != nil {
		// Durability abort: nothing was published; restore the override backing
		// the unchanged current snapshot, all under the lock.
		if curOv, ok := p.overHist[cur.Version]; ok {
			p.over = curOv
		}
		_, _ = p.aud.Record(ctx, audit.Entry{
			Actor: actor, Action: string(actRollback), Target: itoa(targetVersion),
			Result: audit.ResultError, Reason: cerr.Error(),
			ErrKind: errKindString(cerr), FromVer: cur.Version,
		})
		return from, 0, cerr
	}
	// The hook committed the candidate into overHist under the same lock and
	// p.over already equals it.
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
