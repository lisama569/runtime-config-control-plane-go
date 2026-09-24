package security

import "github.com/lisama569/runtime-config-control-plane-go/internal/config"

// Role identifies a principal role.
type Role string

const (
	RoleAdmin     Role = "admin"
	RoleOperator  Role = "operator"
	RoleReader    Role = "reader"
	RoleAnonymous Role = "anonymous"
)

// Action enumerates protected operations.
type Action string

const (
	ActionReadConfig   Action = "config:read"
	ActionReadSecret   Action = "config:read_secret"
	ActionWriteConfig  Action = "config:write"
	ActionRollback     Action = "config:rollback"
	ActionEvaluateFlag Action = "flag:evaluate"
	ActionReadAudit    Action = "audit:read"
)

// Principal is an authenticated caller identified by a stable id.
type Principal struct {
	ID    string
	Name  string
	Roles []Role
}

// Actor returns a display name for audit logs.
func (p Principal) Actor() string {
	if p.Name != "" {
		return p.Name
	}
	if p.ID != "" {
		return p.ID
	}
	return "anonymous"
}

// rolePermissions is the static RBAC matrix.
var rolePermissions = map[Role]map[Action]bool{
	RoleAdmin: {
		ActionReadConfig: true, ActionReadSecret: true, ActionWriteConfig: true,
		ActionRollback: true, ActionEvaluateFlag: true, ActionReadAudit: true,
	},
	RoleOperator: {
		ActionReadConfig: true, ActionWriteConfig: true,
		ActionRollback: true, ActionEvaluateFlag: true, ActionReadAudit: true,
	},
	RoleReader: {
		ActionReadConfig: true, ActionEvaluateFlag: true,
	},
}

// Authorizer makes allow/deny decisions.
type Authorizer struct{}

// NewAuthorizer constructs an RBAC authorizer.
func NewAuthorizer() *Authorizer { return &Authorizer{} }

// Authorize returns nil when principal may perform action. Anonymous
// principals get CatUnauthorized; authenticated but insufficient
// principals get CatForbidden.
func (a *Authorizer) Authorize(p Principal, action Action) error {
	if len(p.Roles) == 0 {
		return config.NewError(config.CatUnauthorized,
			"principal %q is not authenticated", p.Actor())
	}
	if a.Can(p, action) {
		return nil
	}
	return config.NewError(config.CatForbidden,
		"principal %q lacks permission for %s", p.Actor(), action)
}

// Can is the non-error form of Authorize.
func (a *Authorizer) Can(p Principal, action Action) bool {
	for _, r := range p.Roles {
		if perms, ok := rolePermissions[r]; ok && perms[action] {
			return true
		}
	}
	return false
}
