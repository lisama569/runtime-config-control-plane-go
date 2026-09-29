package controlplane

import (
	"strconv"

	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
	"github.com/lisama569/runtime-config-control-plane-go/internal/persist"
)

func jsonItoa(n int64) string { return strconv.FormatInt(n, 10) }

func isStale(err error) bool { return errs.Of(err, errs.KindStaleVersion) }

func defaultLimitsForTest() persist.Limits { return persist.DefaultLimits() }
