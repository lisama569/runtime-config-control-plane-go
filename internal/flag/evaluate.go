package flag

import (
	"hash/fnv"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
)

// Reason explains why a decision was made; tests log it.
type Reason string

const (
	ReasonFlagOff     Reason = "flag_disabled"
	ReasonDenyList    Reason = "subject_on_deny_list"
	ReasonAllowList   Reason = "subject_on_allow_list"
	ReasonPercentage  Reason = "percentage_bucket"
	ReasonNoFlag      Reason = "flag_not_found"
	ReasonRolloutFull Reason = "rollout_100_percent"
	ReasonRolloutZero Reason = "rollout_0_percent"
)

// Decision is the result of one flag evaluation.
type Decision struct {
	Flag       string
	Subject    string
	On         bool
	Bucket     uint32 // deterministic 0..9999 bucket
	Threshold  uint32 // percentage * 100
	Percentage uint32
	Reason     Reason
	Version    int64 // config version the decision is bound to
}

// Evaluator answers flag questions against a versioned snapshot.
type Evaluator struct{}

// NewEvaluator constructs an evaluator.
func NewEvaluator() *Evaluator { return &Evaluator{} }

// Evaluate decides on/off for subject in the given snapshot. The decision
// is deterministic: the same (flag, subject) pair yields the same bucket
// regardless of evaluation time or config version, so results are stable
// while the flag definition is unchanged.
//
// Precedence: flag disabled > deny list > allow list > percentage.
func (e *Evaluator) Evaluate(snap config.Snapshot, flagName, subject string) Decision {
	d := Decision{
		Flag:    flagName,
		Subject: subject,
		Version: snap.Version,
	}
	fl, ok := snap.Config.Flags[flagName]
	if !ok {
		d.Reason = ReasonNoFlag
		return d
	}
	d.Percentage = fl.Rollout.Percentage
	d.Threshold = fl.Rollout.Percentage * 100
	if !fl.Enabled {
		d.Reason = ReasonFlagOff
		return d
	}
	for _, s := range fl.Rollout.DenyList {
		if s == subject {
			d.Reason = ReasonDenyList
			return d
		}
	}
	for _, s := range fl.Rollout.AllowList {
		if s == subject {
			d.On = true
			d.Reason = ReasonAllowList
			return d
		}
	}
	switch fl.Rollout.Percentage {
	case 0:
		d.Reason = ReasonRolloutZero
		return d
	case 100:
		d.On = true
		d.Reason = ReasonRolloutFull
		return d
	}
	d.Bucket = Bucket(flagName, subject)
	d.On = d.Bucket < d.Threshold
	d.Reason = ReasonPercentage
	return d
}

// Bucket derives the deterministic 0..9999 bucket for subject+flag using
// FNV-1a. The bucket never depends on wall clock or config version.
func Bucket(flagName, subject string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(flagName))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(subject))
	return h.Sum32() % 10000
}
