package auth

import (
	"testing"

	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

func testAuthz() *Authorizer {
	return NewAuthorizer(map[string]Principal{
		"r": {Name: "reader", Role: RoleReader},
		"w": {Name: "writer", Role: RoleWriter},
		"a": {Name: "admin", Role: RoleAdmin},
	})
}

func TestPermissionMatrix(t *testing.T) {
	a := testAuthz()
	matrix := []struct {
		token  string
		action Action
		allow  bool
	}{
		{"r", ActRead, true},
		{"r", ActWrite, false},
		{"r", ActRollback, false},
		{"r", ActAuditRead, false},
		{"w", ActRead, true},
		{"w", ActWrite, true},
		{"w", ActRollback, false},
		{"w", ActAuditRead, false},
		{"a", ActRead, true},
		{"a", ActWrite, true},
		{"a", ActRollback, true},
		{"a", ActAuditRead, true},
	}
	for _, c := range matrix {
		p, err := a.Check(c.token, c.action)
		if c.allow {
			if err != nil {
				t.Fatalf("%s should be allowed %s: %v", c.token, c.action, err)
			}
			t.Logf("%s(%s) ALLOW %s", p.Name, p.Role, c.action)
		} else {
			if err == nil {
				t.Fatalf("%s should be denied %s", c.token, c.action)
			}
			if errs.Classify(err) != errs.KindForbidden {
				t.Fatalf("want FORBIDDEN, got %s", errs.Classify(err))
			}
			t.Logf("%s DENY %s kind=%s", c.token, c.action, errs.Classify(err))
		}
	}
}

func TestUnknownTokenIsUnauthorized(t *testing.T) {
	a := testAuthz()
	_, err := a.Check("nope", ActRead)
	if errs.Classify(err) != errs.KindUnauthorized {
		t.Fatalf("want UNAUTHORIZED, got %v", err)
	}
	_, err = a.Check("", ActRead)
	if errs.Classify(err) != errs.KindUnauthorized {
		t.Fatalf("empty token must be UNAUTHORIZED, got %v", err)
	}
	t.Log("missing/unknown token -> UNAUTHORIZED (distinct from FORBIDDEN)")
}
