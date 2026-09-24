// Package flag evaluates feature flags against stable subject identities and
// an immutable config snapshot.
package flag

import (
	"fmt"
	"hash/fnv"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

// Decision is the result of a flag evaluation.
type Decision struct {
	Subject    string `json:"subject"`
	Feature    string `json:"feature"`
	Enabled    bool   `json:"enabled"`
	Reason     string `json:"reason"` // allow_list | rollout_hit | rollout_miss | disabled | unknown_feature
	Bucket     int    `json:"bucket"`
	Percentage int    `json:"percentage"`
	Version    int64  `json:"version"`
}

// Evaluator evaluates flags against a snapshot.
type Evaluator struct{}

// NewEvaluator constructs an Evaluator.
func NewEvaluator() *Evaluator { return &Evaluator{} }

// Evaluate decides whether subject sees feature using snapshot.
// The decision is a pure function of (subject, feature, snapshot): identical
// inputs always produce identical outputs, and with an unchanged snapshot the
// subject's result is stable.
//
// Precedence within a feature:
//  1. unknown feature      -> disabled, reason unknown_feature
//  2. feature disabled     -> disabled, reason disabled
//  3. explicit allow list  -> enabled,  reason allow_list
//  4. percentage rollout   -> enabled iff bucket < percentage
//
// Boundaries: percentage 0 never hits; 100 always hits; the bucket is in
// [0,100), so hit probability equals exactly percentage/100 with no edge bias.
func (e *Evaluator) Evaluate(snap *config.Snapshot, feature, subject string) (Decision, error) {
	if snap == nil {
		return Decision{}, errs.New(errs.KindNotFound, "flag: no configuration loaded")
	}
	if subject == "" {
		return Decision{}, errs.New(errs.KindValidation, "flag: subject must not be empty")
	}
	d := Decision{
		Subject: subject,
		Feature: feature,
		Version: snap.Version,
		Bucket:  Bucket(feature, subject),
	}
	feat, ok := snap.Feature(feature)
	if !ok {
		d.Reason = "unknown_feature"
		return d, nil
	}
	d.Percentage = feat.Percentage
	if !feat.Enabled {
		d.Reason = "disabled"
		return d, nil
	}
	if feat.AllowList[subject] {
		d.Enabled = true
		d.Reason = "allow_list"
		return d, nil
	}
	if d.Bucket < feat.Percentage {
		d.Enabled = true
		d.Reason = "rollout_hit"
	} else {
		d.Reason = "rollout_miss"
	}
	return d, nil
}

// Bucket maps (feature, subject) to a uniform bucket in [0,100).
// It uses FNV-1a 64-bit over "feature\x00subject"; modulo over a 64-bit hash
// has no measurable boundary bias at modulus 100. Mixing the feature name in
// means different features spread the same subjects independently.
func Bucket(feature, subject string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(feature))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(subject))
	return int(h.Sum64() % 100)
}

// Explain renders a human-readable justification, used in tests/audit logs.
func (d Decision) Explain() string {
	return fmt.Sprintf("feature=%s subject=%s version=%d bucket=%d/%d enabled=%t reason=%s",
		d.Feature, d.Subject, d.Version, d.Bucket, d.Percentage, d.Enabled, d.Reason)
}
