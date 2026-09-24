package flag

import (
	"fmt"
	"testing"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
)

// helper builds snapshots without exposing internal helpers
func mkSnap(t *testing.T, version int64, flags map[string]config.Flag) config.Snapshot {
	t.Helper()
	return config.Snapshot{Version: version, Config: config.Config{Flags: flags, Settings: map[string]config.Setting{}}}
}

func TestBoundaryPercentages(t *testing.T) {
	e := NewEvaluator()
	cases := []struct {
		name       string
		percentage uint32
		enabled    bool
		wantReason Reason
		wantOn     bool
	}{
		{"zero", 0, true, ReasonRolloutZero, false},
		{"full", 100, true, ReasonRolloutFull, true},
		{"disabled", 50, false, ReasonFlagOff, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mkSnap(t, 1, map[string]config.Flag{
				"f": {Name: "f", Enabled: tc.enabled, Rollout: config.Rollout{Percentage: tc.percentage}},
			})
			d := e.Evaluate(s, "f", "user-1")
			t.Logf("version=%d flag=f subject=user-1 reason=%s bucket=%d threshold=%d on=%v",
				d.Version, d.Reason, d.Bucket, d.Threshold, d.On)
			if d.Reason != tc.wantReason {
				t.Fatalf("reason mismatch: got=%s want=%s", d.Reason, tc.wantReason)
			}
			if d.On != tc.wantOn {
				t.Fatalf("on mismatch: got=%v want=%v", d.On, tc.wantOn)
			}
			if d.Bucket > 9999 {
				t.Fatalf("bucket out of range: %d", d.Bucket)
			}
		})
	}
}

func TestAllowDenyPrecedence(t *testing.T) {
	e := NewEvaluator()
	s := mkSnap(t, 3, map[string]config.Flag{
		"f": {Name: "f", Enabled: true, Rollout: config.Rollout{
			Percentage: 0, AllowList: []string{"vip-1"}, DenyList: []string{"blocked-1"},
		}},
	})
	dAllow := e.Evaluate(s, "f", "vip-1")
	t.Logf("allow-list decision: version=%d reason=%s on=%v", dAllow.Version, dAllow.Reason, dAllow.On)
	if dAllow.Reason != ReasonAllowList || !dAllow.On {
		t.Fatalf("allow list must force-on, got reason=%s on=%v", dAllow.Reason, dAllow.On)
	}
	dDeny := e.Evaluate(s, "f", "blocked-1")
	t.Logf("deny-list decision: version=%d reason=%s on=%v", dDeny.Version, dDeny.Reason, dDeny.On)
	if dDeny.Reason != ReasonDenyList || dDeny.On {
		t.Fatalf("deny list must force-off even at percentage 0, got reason=%s on=%v", dDeny.Reason, dDeny.On)
	}
}

func TestStableAcrossVersions(t *testing.T) {
	e := NewEvaluator()
	s1 := mkSnap(t, 1, map[string]config.Flag{"f": {Name: "f", Enabled: true, Rollout: config.Rollout{Percentage: 42}}})
	s2 := mkSnap(t, 2, map[string]config.Flag{"f": {Name: "f", Enabled: true, Rollout: config.Rollout{Percentage: 42}}})
	first := e.Evaluate(s1, "f", "stable-user")
	second := e.Evaluate(s2, "f", "stable-user")
	t.Logf("stability: v%d=%s/%v v%d=%s/%v bucket=%d",
		first.Version, first.Reason, first.On, second.Version, second.Reason, second.On, first.Bucket)
	if first.On != second.On || first.Bucket != second.Bucket {
		t.Fatalf("same subject must be stable when flag unchanged: %+v vs %+v", first, second)
	}
	if first.Bucket != Bucket("f", "stable-user") {
		t.Fatalf("bucket function must be deterministic")
	}
}

func TestPercentageBoundaryExact(t *testing.T) {
	e := NewEvaluator()
	s := mkSnap(t, 1, map[string]config.Flag{
		"f": {Name: "f", Enabled: true, Rollout: config.Rollout{Percentage: 50}},
	})
	d := e.Evaluate(s, "f", "some-user")
	t.Logf("50%% boundary decision: reason=%s bucket=%d threshold=%d on=%v", d.Reason, d.Bucket, d.Threshold, d.On)
	if d.Reason != ReasonPercentage {
		t.Fatalf("expected percentage reason, got %s", d.Reason)
	}
	if d.Threshold != 5000 {
		t.Fatalf("threshold must be percentage*100, got %d", d.Threshold)
	}
	if d.On != (d.Bucket < 5000) {
		t.Fatalf("on must equal bucket<threshold")
	}
}

func TestBucketDistributionNotSkewed(t *testing.T) {
	e := NewEvaluator()
	s := mkSnap(t, 1, map[string]config.Flag{
		"f": {Name: "f", Enabled: true, Rollout: config.Rollout{Percentage: 10}},
	})
	const n = 20000
	on := 0
	for i := 0; i < n; i++ {
		d := e.Evaluate(s, "f", fmt.Sprintf("user-%d", i))
		if d.On {
			on++
		}
	}
	ratio := float64(on) / n
	t.Logf("10%% rollout observed ratio=%.4f over %d subjects", ratio, n)
	if ratio < 0.08 || ratio > 0.12 {
		t.Fatalf("rollout ratio %.4f is biased beyond +/-2%%", ratio)
	}
}

func TestFlagNotFound(t *testing.T) {
	e := NewEvaluator()
	s := mkSnap(t, 1, map[string]config.Flag{})
	d := e.Evaluate(s, "missing", "user-1")
	t.Logf("missing flag: reason=%s on=%v version=%d", d.Reason, d.On, d.Version)
	if d.Reason != ReasonNoFlag || d.On {
		t.Fatalf("missing flag must evaluate off with not-found reason")
	}
}
