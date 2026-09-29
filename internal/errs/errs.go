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
	// KindPersistFailed is an I/O / capacity failure while durably storing
	// state. The in-memory commit is aborted, so live and durable state never
	// diverge silently.
	KindPersistFailed Kind = "PERSIST_FAILED"
	// KindStateCorrupt means persisted bytes were found unreadable or failed
	// their checksum; the content must not be treated as a valid commit.
	KindStateCorrupt Kind = "STATE_CORRUPT"
	// KindStateInconsistent means persisted content parsed but violates the
	// timeline invariants (non-monotonic or missing versions).
	KindStateInconsistent Kind = "STATE_INCONSISTENT"
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
