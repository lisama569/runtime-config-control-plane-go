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

func applyPropertyOps(doc *config.Document, over overrideState, ops []PatchOp) error {
	for _, op := range ops {
		if op.Key == "" {
			return errs.New(errs.KindValidation, "patch: property key must not be empty")
		}
		if op.Delete {
			delete(doc.Properties, op.Key)
			over.props[op.Key] = config.Property{Key: op.Key}
			over.deadProps[op.Key] = true
			continue
		}
		prop, err := config.ParseProperty(op.Key, op.Type, json.RawMessage(op.Value))
		if err != nil {
			return err
		}
		if existing, ok := doc.Properties[op.Key]; ok {
			prop.Version = existing.Version + 1
		}
		doc.Properties[op.Key] = prop
		over.props[op.Key] = prop
		delete(over.deadProps, op.Key)
	}
	return nil
}

func applyFeatureOps(doc *config.Document, over overrideState, ops []FeatureOp) error {
	for _, op := range ops {
		if op.Name == "" {
			return errs.New(errs.KindValidation, "patch: feature name must not be empty")
		}
		if op.Delete {
			delete(doc.Features, op.Name)
			over.feats[op.Name] = config.Feature{Name: op.Name}
			over.deadFeats[op.Name] = true
			continue
		}
		rf := config.RawFeature{AllowList: op.AllowList}
		if cur, ok := doc.Features[op.Name]; ok {
			rf.Enabled = cur.Enabled
			rf.Percentage = cur.Percentage
			for s := range cur.AllowList {
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
		doc.Features[op.Name] = feat
		over.feats[op.Name] = feat
		delete(over.deadFeats, op.Name)
	}
	return nil
}
