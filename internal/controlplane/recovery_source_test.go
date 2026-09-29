package controlplane

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/lisama569/runtime-config-control-plane-go/internal/source"
)

func TestRecoveryIgnoresStaleSourceChangesUntilReload(t *testing.T) {
	root := t.TempDir()
	srcPath := filepath.Join(root, "base.json")
	writeCfg(t, srcPath, baseV1())
	dataDir := filepath.Join(root, "data")
	plane := newDurablePlane(t, dataDir, srcPath)
	_, _, err := plane.ApplyPatch(context.Background(), "writer-1", PatchRequest{
		ExpectedVersion: 1,
		Properties:      []PatchOp{{Key: "timeout", Type: "int", Value: json.RawMessage("999")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = plane.Close()

	// Source file changes while the process is down. Recovery must still
	// present the last EFFECTIVE config (API override wins), not silently
	// merge the new file content into the recovered snapshot.
	writeCfg(t, srcPath, `{"version":3,"properties":{"timeout":{"type":"int","value":55}}}`)
	p2, err := New(context.Background(), Options{
		Sources:      []source.Source{source.NewFileSource("base", 1, srcPath)},
		Tokens:       testTokens(),
		PollInterval: time.Hour, DataDir: dataDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p2.Close() }()
	if p2.Current().Version != 2 || p2.Current().Doc.Properties["timeout"].Value.Int != 999 {
		t.Fatalf("recovery must honor last effective config, got v=%d timeout=%v",
			p2.Current().Version, p2.Current().Doc.Properties["timeout"].Value)
	}
}
