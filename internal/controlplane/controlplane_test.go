package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/runtime-config-control-plane-go/internal/auth"
	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/source"
)

const (
	tokReader = "reader-token"
	tokWriter = "writer-token"
	tokAdmin  = "admin-token"
)

func writeCfg(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Hour)
	_ = os.Chtimes(path, future, future)
}

func baseV1() string {
	return `{"version":1,
	"properties":{"timeout":{"type":"int","value":100},"db":{"type":"sensitive","value":"pw"}},
	"features":{"checkout":{"enabled":true,"percentage":50}}}`
}

func newTestPlane(t *testing.T, poll time.Duration) (*Plane, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "base.json")
	writeCfg(t, path, baseV1())
	tokens := map[string]auth.Principal{
		tokReader: {Name: "reader-1", Role: auth.RoleReader},
		tokWriter: {Name: "writer-1", Role: auth.RoleWriter},
		tokAdmin:  {Name: "admin-1", Role: auth.RoleAdmin},
	}
	plane, err := New(context.Background(), Options{
		Sources:      []source.Source{source.NewFileSource("base", 1, path)},
		Tokens:       tokens,
		PollInterval: poll,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plane.Close() })
	return plane, path
}

func do(t *testing.T, h http.Handler, method, token, target string, body any) (int, map[string]any) {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, target, rdr)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	out := map[string]any{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec.Code, out
}

func TestEndToEndHotReloadInvalidFallback(t *testing.T) {
	plane, path := newTestPlane(t, 20*time.Millisecond)
	h := plane.Handler()

	if v := plane.Current().Version; v != 1 {
		t.Fatalf("start version = %d", v)
	}

	// Invalid hot update must not change the effective config.
	writeCfg(t, path, `{"version":9,"properties":{"timeout":{"type":"int","value":"NaN"}}}`)
	time.Sleep(200 * time.Millisecond)
	snap := plane.Current()
	if snap.Version != 1 {
		t.Fatalf("invalid reload changed version to %d", snap.Version)
	}
	if snap.Doc.Properties["timeout"].Value.Int != 100 {
		t.Fatal("last valid config not retained")
	}
	t.Logf("[version=%d] invalid hot update rejected, last valid retained", snap.Version)

	// Valid hot update publishes a new whole snapshot.
	writeCfg(t, path, `{"version":2,"properties":{"timeout":{"type":"int","value":250}},
		"features":{}}`)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && plane.Current().Version != 2 {
		time.Sleep(20 * time.Millisecond)
	}
	if plane.Current().Version != 2 {
		t.Fatalf("valid reload not published, version=%d", plane.Current().Version)
	}
	code, resp := do(t, h, "GET", tokReader, "/v1/config", nil)
	if code != 200 {
		t.Fatalf("GET config code=%d", code)
	}
	t.Logf("[version=%.0f] GET /v1/config after reload ok", resp["version"])
}

func TestEndToEndStaleConflict(t *testing.T) {
	plane, _ := newTestPlane(t, time.Hour)
	h := plane.Handler()

	patch := map[string]any{
		"expected_version": 1,
		"properties": []map[string]any{
			{"key": "timeout", "type": "int", "value": 120},
		},
	}
	code, resp := do(t, h, "POST", tokWriter, "/v1/config", patch)
	if code != 200 {
		t.Fatalf("first write code=%d resp=%v", code, resp)
	}
	t.Logf("first write committed: %v", resp)

	// Replaying the same expected_version is an explicit stale rejection.
	code, resp = do(t, h, "POST", tokWriter, "/v1/config", patch)
	if code != http.StatusConflict {
		t.Fatalf("stale write code=%d, want 409", code)
	}
	if resp["kind"] != "STALE_VERSION" {
		t.Fatalf("kind=%v want STALE_VERSION", resp["kind"])
	}
	t.Logf("stale write rejected kind=%v version=%v: %v",
		resp["kind"], resp["version"], resp["error"])

	// Invalid patch is all-or-nothing: nothing applied.
	bad := map[string]any{
		"expected_version": 2,
		"properties": []map[string]any{
			{"key": "other", "type": "int", "value": 5},
			{"key": "broken", "type": "bool", "value": "yes"},
		},
	}
	code, resp = do(t, h, "POST", tokWriter, "/v1/config", bad)
	if code != 400 {
		t.Fatalf("invalid patch code=%d", code)
	}
	if plane.Current().Doc.Properties["other"].Value.Int == 5 {
		t.Fatal("partial invalid patch must not apply the valid half")
	}
	t.Logf("invalid patch rejected kind=%v: %v", resp["kind"], resp["error"])
}

func TestEndToEndUnauthorizedNeverMutates(t *testing.T) {
	plane, _ := newTestPlane(t, time.Hour)
	h := plane.Handler()
	before := plane.Current().Version

	code, resp := do(t, h, "POST", "", "/v1/config",
		map[string]any{"expected_version": before})
	if code != http.StatusUnauthorized || resp["kind"] != "UNAUTHORIZED" {
		t.Fatalf("anon write code=%d kind=%v", code, resp["kind"])
	}
	code, resp = do(t, h, "POST", tokReader, "/v1/config",
		map[string]any{"expected_version": before})
	if code != http.StatusForbidden || resp["kind"] != "FORBIDDEN" {
		t.Fatalf("reader write code=%d kind=%v", code, resp["kind"])
	}
	code, resp = do(t, h, "POST", tokWriter, "/v1/rollback", map[string]any{"version": 1})
	if code != http.StatusForbidden {
		t.Fatalf("writer rollback code=%d, want 403", code)
	}
	if plane.Current().Version != before {
		t.Fatal("unauthorized attempt changed runtime state")
	}
	entries := plane.Audit().List()
	denied := 0
	for _, e := range entries {
		if e.Result == "denied" {
			denied++
		}
	}
	if denied < 3 {
		t.Fatalf("denied attempts not audited: %+v", entries)
	}
	t.Logf("[version=%d] %d denied attempts audited, state unchanged", before, denied)
}

func TestEndToEndRollbackRestoresFullConfig(t *testing.T) {
	plane, _ := newTestPlane(t, time.Hour)
	h := plane.Handler()

	patch := func(v int64, n int) map[string]any {
		return map[string]any{
			"expected_version": v,
			"properties": []map[string]any{
				{"key": "timeout", "type": "int", "value": n},
			},
		}
	}
	if code, _ := do(t, h, "POST", tokWriter, "/v1/config", patch(1, 200)); code != 200 {
		t.Fatal("patch v2 failed")
	}
	if code, resp := do(t, h, "POST", tokWriter, "/v1/config", patch(2, 300)); code != 200 {
		t.Fatalf("patch v3 failed: %v", resp)
	}
	if got := plane.Current().Doc.Properties["timeout"].Value.Int; got != 300 {
		t.Fatalf("timeout=%d want 300", got)
	}

	code, resp := do(t, h, "POST", tokAdmin, "/v1/rollback", map[string]any{"version": 1})
	if code != 200 {
		t.Fatalf("rollback code=%d resp=%v", code, resp)
	}
	snap := plane.Current()
	if snap.Doc.Properties["timeout"].Value.Int != 100 {
		t.Fatalf("rollback timeout=%d want 100", snap.Doc.Properties["timeout"].Value.Int)
	}
	if snap.PropertyOrigin["timeout"] != "base" {
		t.Fatalf("rollback provenance lost: %v", snap.PropertyOrigin)
	}
	if !snap.Doc.Features["checkout"].Enabled {
		t.Fatal("rollback did not restore the full feature set")
	}
	if snap.Version != 4 {
		t.Fatalf("rollback must create a new linear version, got %d", snap.Version)
	}
	t.Logf("[version=%d] rollback restored full v1 config (timeout=100, checkout enabled, origin=base)",
		snap.Version)

	// Rolling back to an unknown version is a classified failure.
	code, resp = do(t, h, "POST", tokAdmin, "/v1/rollback", map[string]any{"version": 99})
	if code != http.StatusNotFound || resp["kind"] != "ROLLBACK_FAILED" {
		t.Fatalf("unknown rollback code=%d kind=%v", code, resp["kind"])
	}
	t.Logf("unknown rollback rejected kind=%v", resp["kind"])
}

func TestEndToEndConcurrentReadsDuringWrites(t *testing.T) {
	plane, _ := newTestPlane(t, time.Hour)

	var violations int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					snap := plane.Current()
					p, ok := snap.Doc.Properties["timeout"]
					// Sensitive property must always be present and masked in views;
					// timeout must always be a whole int property.
					if !ok || p.Value.Kind != config.KindInt {
						atomic.AddInt64(&violations, 1)
						return
					}
					if _, ok := snap.Doc.Properties["db"]; !ok {
						atomic.AddInt64(&violations, 1)
						return
					}
				}
			}
		}()
	}

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		for {
			cur := plane.Current()
			from, _, err := plane.ApplyPatch(ctx, "writer-1", PatchRequest{
				ExpectedVersion: cur.Version,
				Properties: []PatchOp{{
					Key: "timeout", Type: "int",
					Value: json.RawMessage(fmt.Sprintf("%d", 100+i+1)),
				}},
			})
			if err == nil {
				_ = from
				break
			}
		}
	}
	close(stop)
	wg.Wait()
	if atomic.LoadInt64(&violations) != 0 {
		t.Fatalf("readers observed torn snapshots %d times", violations)
	}
	t.Logf("[version=%d] 8 readers x 100 writes: no torn reads", plane.Current().Version)
}

func TestEndToEndEvaluateAndMasking(t *testing.T) {
	plane, _ := newTestPlane(t, time.Hour)
	h := plane.Handler()

	// Sensitive value must be masked in API output.
	code, resp := do(t, h, "GET", tokReader, "/v1/properties/db", nil)
	if code != 200 {
		t.Fatalf("get prop code=%d", code)
	}
	if resp["value"] != "********" || resp["masked"] != true {
		t.Fatalf("sensitive value leaked: %v", resp)
	}
	t.Logf("[version=1] sensitive property masked: %v", resp)

	// Boundary percentage semantics via live API: set 0% then 100%.
	setPct := func(pct int) {
		code, resp := do(t, h, "POST", tokWriter, "/v1/config", map[string]any{
			"expected_version": plane.Current().Version,
			"features": []map[string]any{
				{"name": "checkout", "enabled": true, "percentage": pct},
			},
		})
		if code != 200 {
			t.Fatalf("set pct %d failed code=%d resp=%v", pct, code, resp)
		}
	}
	setPct(0)
	allMiss := true
	for i := 0; i < 200; i++ {
		code, resp = do(t, h, "GET", tokReader,
			fmt.Sprintf("/v1/features/checkout/evaluate?subject=user-%d", i), nil)
		if code != 200 {
			t.Fatalf("evaluate code=%d", code)
		}
		if resp["enabled"] == true {
			allMiss = false
		}
	}
	if !allMiss {
		t.Fatal("0% rollout enabled a subject")
	}
	t.Logf("[version=%v] 0%% rollout: 200/200 misses", plane.Current().Version)

	setPct(100)
	for i := 0; i < 200; i++ {
		code, resp = do(t, h, "GET", tokReader,
			fmt.Sprintf("/v1/features/checkout/evaluate?subject=user-%d", i), nil)
		if code != 200 || resp["enabled"] != true {
			t.Fatalf("100%% rollout rejected user-%d: code=%d resp=%v", i, code, resp)
		}
	}
	t.Logf("[version=%v] 100%% rollout: 200/200 hits", plane.Current().Version)
}
