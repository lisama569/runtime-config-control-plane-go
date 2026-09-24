// Package merge combines documents from multiple ordered sources.
package merge

import (
	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

// Layered is one source's contribution at merge time.
type Layered struct {
	Name    string // source name
	Version int64  // source content version
	Doc     *config.Document
}

// Result is the merged effective document plus provenance.
type Result struct {
	Doc            *config.Document
	PropertyOrigin map[string]string
	FeatureOrigin  map[string]string
	SourceVersions map[string]int64
}

// Layers merges layers in explicit precedence order: the FIRST layer has the
// HIGHEST priority. For a key present in several layers, the earliest layer
// carrying it wins entirely (no partial merge inside one entry). Ordering is
// given by the input slice — never map iteration or scheduling — so precedence
// is stable and explainable.
func Layers(ordered []Layered) (*Result, error) {
	if ordered == nil {
		return nil, errs.New(errs.KindValidation, "merge: no layers provided")
	}
	r := &Result{
		Doc: &config.Document{
			Properties: map[string]config.Property{},
			Features:   map[string]config.Feature{},
		},
		PropertyOrigin: map[string]string{},
		FeatureOrigin:  map[string]string{},
		SourceVersions: map[string]int64{},
	}
	// Walk low -> high priority so higher-priority entries overwrite lower.
	for i := len(ordered) - 1; i >= 0; i-- {
		layer := ordered[i]
		if layer.Doc == nil {
			return nil, errs.New(errs.KindValidation,
				"merge: layer "+layer.Name+" has no document")
		}
		r.SourceVersions[layer.Name] = layer.Version
		for key, prop := range layer.Doc.Properties {
			prop.Key = key
			r.Doc.Properties[key] = prop
			r.PropertyOrigin[key] = layer.Name
		}
		for name, feat := range layer.Doc.Features {
			feat.Name = name
			r.Doc.Features[name] = feat
			r.FeatureOrigin[name] = layer.Name
		}
	}
	return r, nil
}
