package security

import (
	"testing"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
)

func TestRBACMatrix(t *testing.T) {
	a := NewAuthorizer()
	admin := Principal{ID: "u1", Roles: []Role{RoleAdmin}}
	operator := Principal{ID: "u2", Roles: []Role{RoleOperator}}
	reader := Principal{ID: "u3", Roles: []Role{RoleReader}}
	anon := Principal{}

	cases := []struct {
		name string
		p    Principal
		act  Action
		want config.Category // "" means allowed
	}{
		{"admin write", admin, ActionWriteConfig, ""},
		{"admin read secret", admin, ActionReadSecret, ""},
		{"admin rollback", admin, ActionRollback, ""},
		{"operator write", operator, ActionWriteConfig, ""},
		{"operator no secret", operator, ActionReadSecret, config.CatForbidden},
		{"operator audit", operator, ActionReadAudit, ""},
		{"reader read", reader, ActionReadConfig, ""},
		{"reader evaluate", reader, ActionEvaluateFlag, ""},
		{"reader write denied", reader, ActionWriteConfig, config.CatForbidden},
		{"reader rollback denied", reader, ActionRollback, config.CatForbidden},
		{"reader audit denied", reader, ActionReadAudit, config.CatForbidden},
		{"anonymous read", anon, ActionReadConfig, config.CatUnauthorized},
		{"anonymous evaluate", anon, ActionEvaluateFlag, config.CatUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := a.Authorize(tc.p, tc.act)
			got := config.CatOf(err)
			t.Logf("actor=%s action=%s category=%q", tc.p.Actor(), tc.act, got)
			if got != tc.want {
				t.Fatalf("category mismatch: got=%q want=%q", got, tc.want)
			}
		})
	}
}

func TestMaskConfig(t *testing.T) {
	c := config.Empty()
	c.Settings["db_password"] = config.Setting{Value: "s3cr3t", Type: config.TypeString, Secret: true}
	c.Settings["greeting"] = config.Setting{Value: "hello", Type: config.TypeString}

	masked := MaskConfig(c, false)
	t.Logf("masked secret=%v plain=%v", masked.Settings["db_password"].Value, masked.Settings["greeting"].Value)
	if masked.Settings["db_password"].Value != Masked {
		t.Fatalf("secret must be masked, got %v", masked.Settings["db_password"].Value)
	}
	if masked.Settings["greeting"].Value != "hello" {
		t.Fatalf("non-secret must stay visible")
	}
	if c.Settings["db_password"].Value != "s3cr3t" {
		t.Fatalf("masking must not mutate the source config")
	}
	unmasked := MaskConfig(c, true)
	if unmasked.Settings["db_password"].Value != "s3cr3t" {
		t.Fatalf("read-secret principal must see the real value")
	}
}
