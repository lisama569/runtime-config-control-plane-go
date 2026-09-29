package controlplane

import (
	"encoding/json"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

type rawMessage = json.RawMessage

const (
	actWrite    = "config:write"
	actRollback = "config:rollback"
)

func staleErr(cur, expected int64) error {
	return errs.New(errs.KindStaleVersion,
		"write rejected as stale: expected version "+itoa(expected)+", current version "+itoa(cur))
}

func rollbackErr(v int64) error {
	return errs.New(errs.KindRollbackFailed, "version "+itoa(v)+" is not available for rollback")
}

func rollbackStateErr(v int64) error {
	return errs.Wrap(errs.KindRollbackFailed,
		"cannot reconstruct override state for version "+itoa(v), nil)
}

func applyPropertyOps(cur *config.Document, over overrideState, ops []PatchOp) error {
	for _, op := range ops {
		if op.Key == "" {
			return errs.New(errs.KindValidation, "patch: property key must not be empty")
		}
		if op.Delete {
			if existing, ok := cur.Properties[op.Key]; ok {
				over.props[op.Key] = existing
			} else {
				over.props[op.Key] = config.Property{Key: op.Key}
			}
			over.deadProps[op.Key] = true
			continue
		}
		prop, err := config.ParseProperty(op.Key, op.Type, json.RawMessage(op.Value))
		if err != nil {
			return err
		}
		// Version baseline prefers the live override entry (it may carry a
		// tombstone or an API-only value), then the effective document.
		if existing, ok := over.props[op.Key]; ok && !over.deadProps[op.Key] {
			prop.Version = existing.Version + 1
		} else if existing, ok := cur.Properties[op.Key]; ok {
			prop.Version = existing.Version + 1
		}
		over.props[op.Key] = prop
		delete(over.deadProps, op.Key)
	}
	return nil
}

func applyFeatureOps(cur *config.Document, over overrideState, ops []FeatureOp) error {
	for _, op := range ops {
		if op.Name == "" {
			return errs.New(errs.KindValidation, "patch: feature name must not be empty")
		}
		if op.Delete {
			if existing, ok := cur.Features[op.Name]; ok {
				over.feats[op.Name] = existing
			} else {
				over.feats[op.Name] = config.Feature{Name: op.Name}
			}
			over.deadFeats[op.Name] = true
			continue
		}
		rf := config.RawFeature{AllowList: op.AllowList}
		// Baseline prefers the live override feature, then the effective one.
		var base *config.Feature
		if f, ok := over.feats[op.Name]; ok && !over.deadFeats[op.Name] {
			base = &f
		} else if f, ok := cur.Features[op.Name]; ok {
			base = &f
		}
		if base != nil {
			rf.Enabled = base.Enabled
			rf.Percentage = base.Percentage
			for s := range base.AllowList {
				rf.AllowList = append(rf.AllowList, s)
			}
		}
		if op.Enabled != nil {
			rf.Enabled = *op.Enabled
		}
		if op.Percentage != nil {
			rf.Percentage = *op.Percentage
		}
		feat, err := config.ParseFeatureDef(op.Name, rf)
		if err != nil {
			return err
		}
		over.feats[op.Name] = feat
		delete(over.deadFeats, op.Name)
	}
	return nil
}
