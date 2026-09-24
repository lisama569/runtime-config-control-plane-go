package controlplane

import (
	"sort"

	"github.com/lisama569/runtime-config-control-plane-go/internal/audit"
	"github.com/lisama569/runtime-config-control-plane-go/internal/auth"
	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

func sortedKeys(m map[string]config.Property) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedFeatureNames(m map[string]config.Feature) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortStrings2(s []string) { sort.Strings(s) }

func auditEntryDenied(p auth.Principal, action auth.Action, err error) audit.Entry {
	return audit.Entry{
		Actor: p.Name, Role: string(p.Role), Action: string(action),
		Target: "api", Result: audit.ResultDenied,
		Reason: err.Error(), ErrKind: string(errs.Classify(err)),
	}
}
