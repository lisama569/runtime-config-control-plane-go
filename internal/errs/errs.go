// Package errs defines classified errors shared across the control plane.
package errs

import "errors"

// Kind classifies an error so callers can distinguish failure reasons.
type Kind string

const (
	KindUnknown        Kind = "UNKNOWN"
	KindValidation     Kind = "VALIDATION"
	KindNotFound       Kind = "NOT_FOUND"
	KindUnauthorized   Kind = "UNAUTHORIZED"
	KindForbidden      Kind = "FORBIDDEN"
	KindConflict       Kind = "CONFLICT"
	KindStaleVersion   Kind = "STALE_VERSION"
	KindTypeMismatch   Kind = "TYPE_MISMATCH"
	KindMissingValue   Kind = "MISSING_VALUE"
	KindRollbackFailed Kind = "ROLLBACK_FAILED"
	// Persistence failures. These kinds are deliberately distinct so operators
	// can tell corruption, truncated writes and capacity problems apart.
	KindPersistCorrupt  Kind = "PERSIST_CORRUPT"   // persisted bytes unparseable / structural mismatch
	KindPersistChecksum Kind = "PERSIST_CHECKSUM"  // CRC32 mismatch: torn or altered write
	KindPersistIO       Kind = "PERSIST_IO"        // underlying filesystem failure
	KindPersistTooLarge Kind = "PERSIST_TOO_LARGE" // payload exceeds the configured capacity cap
	KindPersistSchema   Kind = "PERSIST_SCHEMA"    // unsupported/unknown on-disk format version
)

// Error is a classified error carrying a stable Kind.
type Error struct {
	Kind    Kind
	Message string
	Cause   error
}

func (e *Error) Error() string { return e.Message }
func (e *Error) Unwrap() error { return e.Cause }

// New builds an Error of the given kind.
func New(kind Kind, msg string) *Error {
	return &Error{Kind: kind, Message: msg}
}

// Wrap builds an Error wrapping an underlying cause.
func Wrap(kind Kind, msg string, cause error) *Error {
	return &Error{Kind: kind, Message: msg, Cause: cause}
}

// Classify returns the Kind of err, or KindUnknown.
func Classify(err error) Kind {
	var ce *Error
	if errors.As(err, &ce) {
		return ce.Kind
	}
	return KindUnknown
}

// Of reports whether err has the given kind.
func Of(err error, kind Kind) bool {
	return Classify(err) == kind
}
