package merge

import (
	"strconv"
	"testing"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
)

func prop(k, t string, n int64) config.Property {
	return config.Property{Key: k, Version: 1, Value: config.Value{Kind: config.ValueKind(t), Int: n, Str: strconv.FormatInt(n, 10)}}
}

func TestPrecedenceFirstLayerWins(t *testing.T) {
	high := &config.Document{
		Properties: map[string]config.Property{"timeout": prop("timeout", "int", 100)},
		Features:   map[string]config.Feature{"f": {Name: "f", Enabled: true, Percentage: 10}},
	}
	low := &config.Document{
		Properties: map[string]config.Property{"timeout": prop("timeout", "int", 999)},
		Features:   map[string]config.Feature{"f": {Name: "f", Enabled: false, Percentage: 0}},
	}
	res, err := Layers([]Layered{
		{Name: "override", Version: 2, Doc: high},
		{Name: "base", Version: 1, Doc: low},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Doc.Properties["timeout"].Value.Int; got != 100 {
		t.Fatalf("high-priority value lost: %d", got)
	}
	if res.PropertyOrigin["timeout"] != "override" {
		t.Fatalf("origin = %q, want override", res.PropertyOrigin["timeout"])
	}
	if res.Doc.Features["f"].Percentage != 10 {
		t.Fatal("feature merge did not honor precedence")
	}
	if res.SourceVersions["base"] != 1 || res.SourceVersions["override"] != 2 {
		t.Fatalf("source versions not captured: %v", res.SourceVersions)
	}
	t.Logf("precedence: timeout=100 origin=override, sources=%v", res.SourceVersions)
}

func TestPrecedenceStableWhenSameKeyInThreeLayers(t *testing.T) {
	mk := func(n int64) *config.Document {
		return &config.Document{
			Properties: map[string]config.Property{"k": prop("k", "int", n)},
			Features:   map[string]config.Feature{},
		}
	}
	for i := 0; i < 50; i++ {
		res, err := Layers([]Layered{
			{Name: "a", Version: 1, Doc: mk(1)},
			{Name: "b", Version: 1, Doc: mk(2)},
			{Name: "c", Version: 1, Doc: mk(3)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if res.Doc.Properties["k"].Value.Int != 1 || res.PropertyOrigin["k"] != "a" {
			t.Fatalf("precedence unstable on iteration %d: %+v", i, res.PropertyOrigin)
		}
	}
	t.Log("precedence stable across 50 merges despite map-based internals")
}

func TestNilLayerDocumentRejected(t *testing.T) {
	_, err := Layers([]Layered{{Name: "x", Version: 1, Doc: nil}})
	if err == nil {
		t.Fatal("nil layer document must be rejected")
	}
	t.Logf("nil layer rejected: %v", err)
}
