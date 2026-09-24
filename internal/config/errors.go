package config

import (
	"errors"
	"fmt"
)

// Category classifies an error so callers and tests can distinguish
// failure reasons without parsing messages.
type Category string

const (
	CatValidation   Category = "validation_error"
	CatConflict     Category = "conflict"
	CatUnauthorized Category = "unauthorized"
	CatForbidden    Category = "forbidden"
	CatNotFound     Category = "not_found"
	CatTypeMismatch Category = "type_mismatch"
	CatInternal     Category = "internal_error"
)

// Error wraps a message with a machine readable category.
type Error struct {
	Category Category
	Message  string
}

func (e *Error) Error() string {
	return string(e.Category) + ": " + e.Message
}

// NewError constructs a categorized error.
func NewError(cat Category, format string, args ...any) error {
	return &Error{Category: cat, Message: fmt.Sprintf(format, args...)}
}

// CatOf returns the category of err, or CatInternal.
func CatOf(err error) Category {
	if err == nil {
		return ""
	}
	var ce *Error
	if errors.As(err, &ce) {
		return ce.Category
	}
	return CatInternal
}
