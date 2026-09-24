package flag

import (
	"testing"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
)

func snap(ver int64, feats map[string]config.Feature) *config.Snapshot {
	return &config.Snapshot{Version: ver, Doc: &config.Document{
		Properties: map[string]config.Property{}, Features: feats,
	}}
}

func TestDecisionStableAcrossCalls(t *testing.T) {
	s := snap(7, map[string]config.Feature{
		"f": {Name: "f", Enabled: true, Percentage: 37,
			AllowList: map[string]bool{}},
	})
	e := NewEvaluator()
	first, err := e.Evaluate(s, "f", "subject-42")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		d, err := e.Evaluate(s, "f", "subject-42")
		if err != nil {
			t.Fatal(err)
		}
		if d != first {
			t.Fatalf("decision drifted: %+v != %+v", d, first)
		}
	}
	t.Logf("[version=%d] stable decision: %s", s.Version, first.Explain())
}

func TestRolloutBoundaries(t *testing.T) {
	e := NewEvaluator()

	// percentage 0 => nobody enabled; 100 => everybody enabled, including
	// every bucket at the exact edges.
	zero := snap(1, map[string]config.Feature{"f": {Name: "f", Enabled: true, Percentage: 0}})
	full := snap(1, map[string]config.Feature{"f": {Name: "f", Enabled: true, Percentage: 100}})
	for i := 0; i < 2000; i++ {
		subj := "user-" + itoaS(i)
		d0, err := e.Evaluate(zero, "f", subj)
		if err != nil {
			t.Fatal(err)
		}
		if d0.Enabled || d0.Reason != "rollout_miss" {
			t.Fatalf("0%% rollout enabled %s: %s", subj, d0.Explain())
		}
		d100, err := e.Evaluate(full, "f", subj)
		if err != nil {
			t.Fatal(err)
		}
		if !d100.Enabled || d100.Reason != "rollout_hit" {
			t.Fatalf("100%% rollout rejected %s: %s", subj, d100.Explain())
		}
		if d0.Bucket < 0 || d0.Bucket >= 100 {
			t.Fatalf("bucket out of [0,100): %d", d0.Bucket)
		}
	}
	t.Log("[version=1] boundary: 0% never hits, 100% always hits, buckets in [0,100)")
}

func TestAllowListPrecedence(t *testing.T) {
	s := snap(3, map[string]config.Feature{
		"f": {Name: "f", Enabled: true, Percentage: 0,
			AllowList: map[string]bool{"vip": true}},
	})
	d, err := NewEvaluator().Evaluate(s, "f", "vip")
	if err != nil {
		t.Fatal(err)
	}
	if !d.Enabled || d.Reason != "allow_list" {
		t.Fatalf("allowlist subject rejected: %s", d.Explain())
	}
	t.Logf("[version=%d] allow_list wins over 0%% rollout: %s", s.Version, d.Explain())

	off := snap(3, map[string]config.Feature{
		"f": {Name: "f", Enabled: false, Percentage: 100,
			AllowList: map[string]bool{"vip": true}},
	})
	d2, _ := NewEvaluator().Evaluate(off, "f", "vip")
	if d2.Enabled || d2.Reason != "disabled" {
		t.Fatalf("disabled flag must win over allow list: %s", d2.Explain())
	}
	t.Logf("[version=%d] disabled wins over allow_list: %s", s.Version, d2.Explain())
}

func TestUnknownFeatureAndEmptySubject(t *testing.T) {
	s := snap(1, map[string]config.Feature{})
	d, err := NewEvaluator().Evaluate(s, "ghost", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if d.Enabled || d.Reason != "unknown_feature" {
		t.Fatalf("unknown feature should be disabled/unknown: %s", d.Explain())
	}
	t.Logf("[version=%d] %s", s.Version, d.Explain())

	if _, err := NewEvaluator().Evaluate(s, "ghost", ""); err == nil {
		t.Fatal("empty subject must be rejected")
	}
}

func TestDistributionHasNoGrossBias(t *testing.T) {
	const n = 20000
	counts := make([]int, 100)
	for i := 0; i < n; i++ {
		counts[Bucket("checkout", "user-"+itoaS(i))]++
	}
	expected := n / 100
	// Expected stddev per bucket ~14 for n=20000; ±25% (~3.6 sigma) never
	// trips on noise but would catch gross systematic bias.
	for b, c := range counts {
		if c < expected*75/100 || c > expected*125/100 {
			t.Fatalf("bucket %d count %d far from expected %d (biased?)", b, c, expected)
		}
	}
	t.Logf("[n=%d] per-bucket expected=%d, observed range=%d..%d",
		n, expected, minCount(counts), maxCount(counts))
}

func TestFeaturesSpreadIndependently(t *testing.T) {
	if Bucket("f1", "alice") == Bucket("f2", "alice") &&
		Bucket("f1", "bob") == Bucket("f2", "bob") {
		// probability of matching twice is 1/10000; fail only on more evidence
		same := 0
		for i := 0; i < 50; i++ {
			u := "u" + itoaS(i)
			if Bucket("f1", u) == Bucket("f2", u) {
				same++
			}
		}
		if same > 10 {
			t.Fatalf("features do not spread independently: %d/50 collisions", same)
		}
	}
}

func itoaS(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func minCount(c []int) int {
	m := c[0]
	for _, v := range c {
		if v < m {
			m = v
		}
	}
	return m
}

func maxCount(c []int) int {
	m := c[0]
	for _, v := range c {
		if v > m {
			m = v
		}
	}
	return m
}
