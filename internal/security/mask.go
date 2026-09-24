package security

import "github.com/lisama569/runtime-config-control-plane-go/internal/config"

// Masked is the redacted representation of a value.
const Masked = "********"

// MaskConfig returns a deep copy of c with secret values replaced by
// Masked when the principal lacks ActionReadSecret. The input is never
// mutated, so concurrent readers keep seeing the real snapshot.
func MaskConfig(c config.Config, canReadSecret bool) config.Config {
	out := c.Clone()
	if canReadSecret {
		return out
	}
	for k, s := range out.Settings {
		if s.Secret {
			s.Value = Masked
			out.Settings[k] = s
		}
	}
	return out
}
