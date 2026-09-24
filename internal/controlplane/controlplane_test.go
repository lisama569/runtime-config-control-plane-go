package controlplane

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lisama569/runtime-config-control-plane-go/internal/audit"
	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/security"
	"github.com/lisama569/runtime-config-control-plane-go/internal/source"
)

var (
	admin    = security.Principal{ID: "a1", Name: "admin", Roles: []security.Role{security.RoleAdmin}}
	operator = security.Principal{ID: "o1", Name: "op", Roles: []security.Role{security.RoleOperator}}
	reader   = security.Principal{ID: "r1", Name: "reader", Roles: []security.Role{security.RoleReader}}
	anon     = security.Principal{}
)

func newPlane(t *testing.T, paths ...string) (*Plane, *audit.Logger) {
	t.Helper()
	var srcs []source.Source
	for _, p := range paths {
		fs, err := source.NewFileSource(p, p)
		if err != nil {
			t.Fatal(err)
		}
		srcs = append(srcs, fs)
	}
	logger := audit.New(256, nil)
	return New(Options{Sources: srcs, Audit: logger}), logger
}

func TestInitialLoadAndMasking(t *testing.T) {
	p, _ := newPlane(t, "../../testdata/base.json")
	if v := p.CurrentVersion(); v != 1 {
		t.Fatalf("initial load must commit version 1, got %d", v)
	}
	snap, err := p.ReadConfig(reader)
	if err != nil {
		t.Fatalf("reader read: %v", err)
	}
	t.Logf("reader view: version=%d greeting=%v db_password=%v",
		snap.Version, snap.Config.Settings["greeting"].Value, snap.Config.Settings["db_password"].Value)
	if snap.Config.Settings["db_password"].Value != security.Masked {
		t.Fatalf("reader must see masked secret")
	}
	snapAdmin, err := p.ReadConfig(admin)
	if err != nil {
		t.Fatalf("admin read: %v", err)
	}
	t.Logf("admin view: db_password=%v", snapAdmin.Config.Settings["db_password"].Value)
	if snapAdmin.Config.Settings["db_password"].Value != "s3cr3t" {
		t.Fatalf("admin must see real secret")
	}
}

func TestUnauthorizedOperationsDoNotChangeState(t *testing.T) {
	p, logger := newPlane(t, "../../testdata/base.json")
	before := p.CurrentVersion()

	next := config.Empty()
	next.Settings["greeting"] = config.Setting{Value: "hacked", Type: config.TypeString}
	_, err := p.Write(context.Background(), reader, next, before, "reader tries to write")
	cat := config.CatOf(err)
	t.Logf("reader write: category=%s error=%v", cat, err)
	if cat != config.CatForbidden {
		t.Fatalf("expected forbidden, got %s", cat)
	}

	_, err = p.Rollback(context.Background(), reader, 0, before, "reader tries rollback")
	cat = config.CatOf(err)
	t.Logf("reader rollback: category=%s", cat)
	if cat != config.CatForbidden {
		t.Fatalf("expected forbidden, got %s", cat)
	}

	_, err = p.ReadConfig(anon)
	cat = config.CatOf(err)
	t.Logf("anonymous read: category=%s", cat)
	if cat != config.CatUnauthorized {
		t.Fatalf("expected unauthorized, got %s", cat)
	}

	if got := p.CurrentVersion(); got != before {
		t.Fatalf("unauthorized ops changed version: %d -> %d", before, got)
	}
	snap, _ := p.ReadConfig(admin)
	if snap.Config.Settings["greeting"].Value != "hello" {
		t.Fatalf("unauthorized write leaked into state")
	}
	// denied attempts are auditable
	entries := logger.Query(0, "")
	denied := 0
	for _, e := range entries {
		if e.Outcome == audit.OutcomeDenied {
			denied++
			t.Logf("audit denied: actor=%s action=%s category=%s", e.Actor, e.Action, e.Category)
		}
	}
	if denied < 3 {
		t.Fatalf("expected >=3 denied audit entries, got %d", denied)
	}
}

func TestStaleAndInvalidWritesRejected(t *testing.T) {
	p, _ := newPlane(t, "../../testdata/base.json")
	v := p.CurrentVersion()

	valid := config.Empty()
	valid.Settings["greeting"] = config.Setting{Value: "bonjour", Type: config.TypeString}
	snap, err := p.Write(context.Background(), operator, valid, v, "update greeting")
	if err != nil {
		t.Fatalf("valid write: %v", err)
	}
	t.Logf("write committed: version=%d", snap.Version)

	// stale expected version -> conflict
	_, err = p.Write(context.Background(), operator, valid, v, "stale write")
	cat := config.CatOf(err)
	t.Logf("stale write: category=%s error=%v", cat, err)
	if cat != config.CatConflict {
		t.Fatalf("expected conflict, got %s", cat)
	}

	// invalid config -> validation, state untouched
	bad := config.Empty()
	bad.Flags["oops"] = config.Flag{Name: "oops", Rollout: config.Rollout{Percentage: 700}}
	_, err = p.Write(context.Background(), operator, bad, snap.Version, "invalid write")
	cat = config.CatOf(err)
	t.Logf("invalid write: category=%s error=%v", cat, err)
	if cat != config.CatValidation {
		t.Fatalf("expected validation, got %s", cat)
	}
	if p.CurrentVersion() != snap.Version {
		t.Fatalf("rejected writes must not advance version")
	}
	cur, _ := p.ReadConfig(admin)
	if cur.Config.Settings["greeting"].Value != "bonjour" {
		t.Fatalf("last good config must remain effective")
	}
}

func TestRollbackRestoresEffectiveConfig(t *testing.T) {
	p, _ := newPlane(t, "../../testdata/base.json")
	v1 := p.CurrentVersion()

	next := config.Empty()
	next.Settings["greeting"] = config.Setting{Value: "broken-value", Type: config.TypeString}
	v2snap, err := p.Write(context.Background(), operator, next, v1, "bad deploy")
	if err != nil {
		t.Fatal(err)
	}
	restored, err := p.Rollback(context.Background(), admin, v1, v2snap.Version, "revert bad deploy")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	t.Logf("rollback: from=%d to=%d greeting=%v", v2snap.Version, restored.Version,
		restored.Config.Settings["greeting"].Value)
	if restored.Config.Settings["greeting"].Value != "hello" {
		t.Fatalf("rollback must restore previous value")
	}
	if restored.Config.Settings["db_password"].Value != "s3cr3t" {
		t.Fatalf("rollback must restore complete config")
	}
}

func TestConcurrentReadWriteConsistency(t *testing.T) {
	p, _ := newPlane(t, "../../testdata/base.json")
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// readers: every snapshot must validate and be complete
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					snap, err := p.ReadConfig(admin)
					if err != nil {
						t.Errorf("reader %d: %v", id, err)
						return
					}
					if err := config.Validate(snap.Config); err != nil {
						t.Errorf("reader %d observed invalid snapshot v%d: %v", id, snap.Version, err)
						return
					}
					if _, ok := snap.Config.Settings["greeting"]; !ok {
						t.Errorf("reader %d observed half-updated snapshot v%d (missing greeting)", id, snap.Version)
						return
					}
				}
			}
		}(i)
	}
	// concurrent evaluators
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					d, _, err := p.Evaluate(reader, "checkout_v2", fmt.Sprintf("user-%d", id))
					if err != nil {
						t.Errorf("evaluator %d: %v", id, err)
						return
					}
					_ = d
				}
			}
		}(i)
	}
	// single writer doing serial CAS writes
	v := p.CurrentVersion()
	for i := 0; i < 50; i++ {
		next := config.Empty()
		next.Settings["greeting"] = config.Setting{Value: fmt.Sprintf("hello-%d", i), Type: config.TypeString}
		snap, err := p.Write(context.Background(), operator, next, v, fmt.Sprintf("iter %d", i))
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		v = snap.Version
	}
	close(stop)
	wg.Wait()
	t.Logf("final version=%d after 50 serialized writes with concurrent readers", p.CurrentVersion())
}

func TestConcurrentWritersSingleWinner(t *testing.T) {
	p, _ := newPlane(t, "../../testdata/base.json")
	v := p.CurrentVersion()
	const writers = 8
	var wg sync.WaitGroup
	results := make(chan config.Category, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			next := config.Empty()
			next.Settings["greeting"] = config.Setting{Value: fmt.Sprintf("w%d", i), Type: config.TypeString}
			_, err := p.Write(context.Background(), operator, next, v, "race")
			results <- config.CatOf(err)
		}(i)
	}
	wg.Wait()
	close(results)
	var success, conflict int
	for cat := range results {
		switch cat {
		case "":
			success++
		case config.CatConflict:
			conflict++
		default:
			t.Fatalf("unexpected category %s", cat)
		}
	}
	t.Logf("writers=%d success=%d conflict=%d version=%d", writers, success, conflict, p.CurrentVersion())
	if success != 1 || conflict != writers-1 {
		t.Fatalf("exactly one writer must win: success=%d conflict=%d", success, conflict)
	}
}

func TestHotReloadKeepsLastGoodOnInvalid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "conf.json")
	good := `{"flags":[{"name":"f","enabled":true,"rollout":{"percentage":10}}],
	          "settings":[{"key":"greeting","setting":{"value":"hello","type":"string"}}]}`
	if err := os.WriteFile(path, []byte(good), 0o600); err != nil {
		t.Fatal(err)
	}
	p, logger := newPlane(t, path)
	v1 := p.CurrentVersion()
	t.Logf("initial version=%d", v1)

	// write invalid content -> reload rejected, last good retained
	bad := `{"flags":[{"name":"f","rollout":{"percentage":999}}]}`
	if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := p.ReloadLatest(context.Background(), "watcher", "file change")
	cat := config.CatOf(err)
	t.Logf("invalid hot reload: category=%s error=%v", cat, err)
	if cat != config.CatValidation {
		t.Fatalf("expected validation, got %s", cat)
	}
	if p.CurrentVersion() != v1 {
		t.Fatalf("invalid reload must not advance version")
	}
	snap, _ := p.ReadConfig(admin)
	if snap.Config.Settings["greeting"].Value != "hello" {
		t.Fatalf("last good config must stay effective")
	}

	// fix the file -> reload succeeds
	good2 := `{"flags":[{"name":"f","enabled":true,"rollout":{"percentage":30}}],
	           "settings":[{"key":"greeting","setting":{"value":"hola","type":"string"}}]}`
	if err := os.WriteFile(path, []byte(good2), 0o600); err != nil {
		t.Fatal(err)
	}
	snap2, err := p.ReloadLatest(context.Background(), "watcher", "file change")
	if err != nil {
		t.Fatalf("valid reload: %v", err)
	}
	t.Logf("recovered reload: version=%d greeting=%v", snap2.Version, snap2.Config.Settings["greeting"].Value)
	if snap2.Config.Settings["greeting"].Value != "hola" {
		t.Fatalf("recovered reload must apply new content")
	}

	// audit trail shows the rejection
	found := false
	for _, e := range logger.Query(0, "") {
		if e.Outcome == audit.OutcomeReject && e.Category == string(config.CatValidation) {
			found = true
			t.Logf("audit rejection: actor=%s action=%s category=%s reason=%s", e.Actor, e.Action, e.Category, e.Reason)
		}
	}
	if !found {
		t.Fatalf("rejected reload must be audited")
	}
}

func TestEvaluateAndAuditAccess(t *testing.T) {
	p, _ := newPlane(t, "../../testdata/base.json")
	d, snap, err := p.Evaluate(reader, "checkout_v2", "user-42")
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	t.Logf("evaluate: version=%d flag=checkout_v2 subject=user-42 on=%v reason=%s bucket=%d threshold=%d",
		snap.Version, d.On, d.Reason, d.Bucket, d.Threshold)
	if d.Version != snap.Version {
		t.Fatalf("decision must be bound to snapshot version")
	}

	_, _, err = p.Evaluate(anon, "checkout_v2", "user-42")
	if config.CatOf(err) != config.CatUnauthorized {
		t.Fatalf("anonymous evaluate must be unauthorized, got %v", err)
	}

	if _, err := p.AuditLog(reader, 10); config.CatOf(err) != config.CatForbidden {
		t.Fatalf("reader audit access must be forbidden, got %v", err)
	}
	entries, err := p.AuditLog(admin, 10)
	if err != nil {
		t.Fatalf("admin audit read: %v", err)
	}
	t.Logf("audit entries visible to admin: %d", len(entries))
	if len(entries) == 0 {
		t.Fatalf("audit log must not be empty")
	}
}

func TestReloadNoOpDoesNotAdvanceVersion(t *testing.T) {
	p, logger := newPlane(t, "../../testdata/base.json")
	v := p.CurrentVersion()
	snap, err := p.ReloadLatest(context.Background(), "watcher", "same content")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("no-op reload: version=%d (was %d)", snap.Version, v)
	if snap.Version != v {
		t.Fatalf("identical reload must not advance version: %d -> %d", v, snap.Version)
	}
	found := false
	for _, e := range logger.Query(0, "") {
		if e.Detail == "no-op: merged content identical to current" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no-op reload must be audited")
	}
}
