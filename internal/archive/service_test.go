package archive

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveSnapshotPathUsesReturnedPathWhenVisible(t *testing.T) {
	dir := t.TempDir()
	got, err := resolveSnapshotPath(dir, "")
	if err != nil {
		t.Fatalf("resolveSnapshotPath() error = %v", err)
	}
	if got != dir {
		t.Fatalf("path = %q, want %q", got, dir)
	}
}

func TestResolveSnapshotPathMapsSnapshotToLocalDataPath(t *testing.T) {
	localDataPath := t.TempDir()
	mappedPath := filepath.Join(localDataPath, "snapshots", "abc", "20260621")
	if err := os.MkdirAll(mappedPath, 0o755); err != nil {
		t.Fatal(err)
	}

	got, err := resolveSnapshotPath("/var/lib/victorialogs/snapshots/abc/20260621", localDataPath)
	if err != nil {
		t.Fatalf("resolveSnapshotPath() error = %v", err)
	}
	if got != mappedPath {
		t.Fatalf("path = %q, want %q", got, mappedPath)
	}
}
