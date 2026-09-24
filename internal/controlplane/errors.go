package controlplane

import (
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

func errValidation(msg string) error { return errs.New(errs.KindValidation, msg) }

func errIs(err error, kind errs.Kind) bool { return errs.Of(err, kind) }

func errKindString(err error) string { return string(errs.Classify(err)) }

// itoa small local helper to avoid importing strconv everywhere.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

const errKindStale = errs.KindStaleVersion
