package archive

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/vlog-tools/vlog-tools/internal/api"
	"go.uber.org/zap"
)

func TestCleanupSnapshotFallsBackToLocalDelete(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "snapshot")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	client := api.NewClient("http://127.0.0.1:1", zap.NewNop())
	if err := cleanupSnapshot(context.Background(), client, "/remote/snapshot", dir, ""); err != nil {
		t.Fatalf("cleanupSnapshot() error = %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("snapshot dir still exists, err=%v", err)
	}
}
