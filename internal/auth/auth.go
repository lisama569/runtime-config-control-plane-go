// Package auth provides read/write permission separation for the local API.
package auth

import (
	"sync"

	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

// Role enumerates principal roles.
type Role string

const (
	RoleReader Role = "reader"
	RoleWriter Role = "writer"
	RoleAdmin  Role = "admin"
)

// Principal is an authenticated caller.
type Principal struct {
	Name string
	Role Role
}

// Action enumerates protected operations.
type Action string

const (
	ActRead      Action = "config:read"
	ActWrite     Action = "config:write"
	ActRollback  Action = "config:rollback"
	ActAuditRead Action = "audit:read"
)

// permissions is the single source of truth for the permission boundary.
var permissions = map[Role]map[Action]bool{
	RoleReader: {ActRead: true},
	RoleWriter: {ActRead: true, ActWrite: true},
	RoleAdmin:  {ActRead: true, ActWrite: true, ActRollback: true, ActAuditRead: true},
}

// Authorizer enforces the permission boundary.
type Authorizer struct {
	mu     sync.RWMutex
	tokens map[string]Principal
}

// NewAuthorizer constructs an Authorizer from token->principal mapping.
func NewAuthorizer(tokens map[string]Principal) *Authorizer {
	cp := make(map[string]Principal, len(tokens))
	for k, v := range tokens {
		cp[k] = v
	}
	return &Authorizer{tokens: cp}
}

// Authenticate resolves a bearer token to a principal.
func (a *Authorizer) Authenticate(token string) (Principal, bool) {
	if token == "" {
		return Principal{}, false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	p, ok := a.tokens[token]
	return p, ok
}

// Authorize reports whether the principal may perform action.
// Readers can never mutate; writers cannot roll back or read the audit trail;
// only admins may roll back and inspect audits.
func (a *Authorizer) Authorize(p Principal, action Action) bool {
	return permissions[p.Role][action]
}

// Check combines authentication and authorization into classified errors so
// callers cannot confuse "who are you" with "you may not do this".
func (a *Authorizer) Check(token string, action Action) (Principal, error) {
	p, ok := a.Authenticate(token)
	if !ok {
		return Principal{}, errs.New(errs.KindUnauthorized, "auth: missing or invalid token")
	}
	if !a.Authorize(p, action) {
		return p, errs.New(errs.KindForbidden,
			"auth: role "+string(p.Role)+" may not perform "+string(action))
	}
	return p, nil
}
