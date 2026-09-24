package config

import "testing"

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		conf func() Config
		want Category
	}{
		{"valid", func() Config {
			c := Empty()
			c.Flags["f"] = Flag{Name: "f", Enabled: true, Rollout: Rollout{Percentage: 50, AllowList: []string{"a"}}}
			c.Settings["s"] = Setting{Value: float64(1), Type: TypeInt}
			return c
		}, ""},
		{"percentage over 100", func() Config {
			c := Empty()
			c.Flags["f"] = Flag{Name: "f", Rollout: Rollout{Percentage: 101}}
			return c
		}, CatValidation},
		{"allow deny overlap", func() Config {
			c := Empty()
			c.Flags["f"] = Flag{Name: "f", Rollout: Rollout{AllowList: []string{"a"}, DenyList: []string{"a"}}}
			return c
		}, CatValidation},
		{"zero variant weight", func() Config {
			c := Empty()
			c.Flags["f"] = Flag{Name: "f", Variants: map[string]uint32{"x": 0}}
			return c
		}, CatValidation},
		{"missing value", func() Config {
			c := Empty()
			c.Settings["s"] = Setting{Type: TypeString}
			return c
		}, CatValidation},
		{"type mismatch string", func() Config {
			c := Empty()
			c.Settings["s"] = Setting{Value: true, Type: TypeString}
			return c
		}, CatTypeMismatch},
		{"type mismatch int fraction", func() Config {
			c := Empty()
			c.Settings["s"] = Setting{Value: 1.5, Type: TypeInt}
			return c
		}, CatTypeMismatch},
		{"unknown type", func() Config {
			c := Empty()
			c.Settings["s"] = Setting{Value: "x", Type: Type("weird")}
			return c
		}, CatValidation},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.conf())
			got := CatOf(err)
			t.Logf("case=%s category=%q error=%v", tc.name, got, err)
			if got != tc.want {
				t.Fatalf("category mismatch: got=%q want=%q", got, tc.want)
			}
		})
	}
}

func TestCloneIsDeep(t *testing.T) {
	c := Empty()
	c.Settings["s"] = Setting{Value: "secret", Type: TypeString, Secret: true}
	c.Flags["f"] = Flag{Name: "f", Rollout: Rollout{Percentage: 10, AllowList: []string{"a"}}}
	clone := c.Clone()
	clone.Flags["f"].Rollout.AllowList[0] = "MUTATED"
	clone.Settings["s2"] = Setting{Value: "new", Type: TypeString}
	if c.Flags["f"].Rollout.AllowList[0] != "a" {
		t.Fatalf("clone leaked slice mutation into original")
	}
	if _, ok := c.Settings["s2"]; ok {
		t.Fatalf("clone leaked map mutation into original")
	}
}

func TestCatOfUnknownError(t *testing.T) {
	if got := CatOf(NewError(CatForbidden, "x")); got != CatForbidden {
		t.Fatalf("got %s", got)
	}
	if got := CatOf(nil); got != "" {
		t.Fatalf("nil error should give empty category, got %s", got)
	}
}
