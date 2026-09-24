package config

import (
	"encoding/json"
	"testing"

	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

func rawMsg(t *testing.T, v string) json.RawMessage {
	t.Helper()
	return json.RawMessage(v)
}

func TestParseRejectsBadEntriesWithDistinctKinds(t *testing.T) {
	cases := []struct {
		name string
		raw  *RawDocument
		want errs.Kind
	}{
		{
			"missing property value",
			&RawDocument{Properties: map[string]json.RawMessage{
				"a": rawMsg(t, `{"type":"string"}`),
			}},
			errs.KindMissingValue,
		},
		{
			"null property value",
			&RawDocument{Properties: map[string]json.RawMessage{
				"a": rawMsg(t, `{"type":"string","value":null}`),
			}},
			errs.KindMissingValue,
		},
		{
			"type mismatch string vs number",
			&RawDocument{Properties: map[string]json.RawMessage{
				"a": rawMsg(t, `{"type":"string","value":123}`),
			}},
			errs.KindTypeMismatch,
		},
		{
			"int rejecting fractional number",
			&RawDocument{Properties: map[string]json.RawMessage{
				"a": rawMsg(t, `{"type":"int","value":1.5}`),
			}},
			errs.KindTypeMismatch,
		},
		{
			"bool rejecting string",
			&RawDocument{Properties: map[string]json.RawMessage{
				"a": rawMsg(t, `{"type":"bool","value":"true"}`),
			}},
			errs.KindTypeMismatch,
		},
		{
			"unsupported type",
			&RawDocument{Properties: map[string]json.RawMessage{
				"a": rawMsg(t, `{"type":"bytes","value":"xx"}`),
			}},
			errs.KindValidation,
		},
		{
			"feature percentage over 100",
			&RawDocument{Features: map[string]json.RawMessage{
				"f": rawMsg(t, `{"enabled":true,"percentage":101}`),
			}},
			errs.KindValidation,
		},
		{
			"feature percentage negative",
			&RawDocument{Features: map[string]json.RawMessage{
				"f": rawMsg(t, `{"enabled":true,"percentage":-1}`),
			}},
			errs.KindValidation,
		},
		{
			"malformed json object",
			&RawDocument{Properties: map[string]json.RawMessage{
				"a": rawMsg(t, `"just-a-string"`),
			}},
			errs.KindValidation,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.raw)
			if err == nil {
				t.Fatalf("expected %s error, got nil", tc.want)
			}
			got := errs.Classify(err)
			if got != tc.want {
				t.Fatalf("error kind = %s, want %s (msg=%q)", got, tc.want, err.Error())
			}
			t.Logf("classified %q as %s: %s", tc.name, got, err.Error())
		})
	}
}

func TestParseValidRoundTrip(t *testing.T) {
	raw := &RawDocument{
		Version: 3,
		Properties: map[string]json.RawMessage{
			"s":      rawMsg(t, `{"type":"string","value":"hello"}`),
			"i":      rawMsg(t, `{"type":"int","value":-42}`),
			"f":      rawMsg(t, `{"type":"float","value":1.25}`),
			"b":      rawMsg(t, `{"type":"bool","value":true}`),
			"secret": rawMsg(t, `{"type":"sensitive","value":"topsecret"}`),
		},
		Features: map[string]json.RawMessage{
			"x": rawMsg(t, `{"enabled":true,"percentage":25,"allow_list":["a","b"]}`),
		},
	}
	doc, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Properties["secret"].Value.RawSecret != "topsecret" {
		t.Fatal("secret not retained internally")
	}
	if got := doc.Properties["secret"].Value.Display(); got != "********" {
		t.Fatalf("sensitive display = %q, want masked", got)
	}
	enc := Encode(doc, 4)
	if string(enc.Properties["secret"]) == "" || contains(string(enc.Properties["secret"]), "topsecret") {
		t.Fatalf("Encode must omit secret, got %s", enc.Properties["secret"])
	}
	encSecret := EncodeWithSecrets(doc, 4)
	if !contains(string(encSecret.Properties["secret"]), "topsecret") {
		t.Fatalf("EncodeWithSecrets should keep secret, got %s", encSecret.Properties["secret"])
	}
	t.Logf("[version=4] round trip ok, sensitive masked in public encoding: %s", enc.Properties["secret"])
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
