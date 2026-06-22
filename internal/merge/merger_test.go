package merge

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/vlog-tools/vlog-tools/internal/config"
	"go.uber.org/zap"
)

func TestMergeLocalDirsPreservesCollidingPartsAndRemovesPartsJSON(t *testing.T) {
	srcDir := t.TempDir()
	writePart(t, srcDir, "node-a", "18BAEF8F11A364C9", "from-a")
	writePart(t, srcDir, "node-b", "18BAEF8F11A364C9", "from-b")
	writePart(t, srcDir, "node-b", "91C2", "from-b-2")

	m := NewMerger(&config.Config{}, zap.NewNop())
	mergedDir, err := m.MergeLocalDirs(context.Background(), srcDir, "20260621")
	if err != nil {
		t.Fatalf("MergeLocalDirs() error = %v", err)
	}
	defer os.RemoveAll(mergedDir)

	datadbDir := filepath.Join(mergedDir, "datadb")
	if _, err := os.Stat(filepath.Join(datadbDir, "parts.json")); !os.IsNotExist(err) {
		t.Fatalf("parts.json should be absent after merge, err=%v", err)
	}

	renamedPart := hashedPartID("node-b", "18BAEF8F11A364C9", 0)
	assertFileContent(t, filepath.Join(datadbDir, "18BAEF8F11A364C9", "data.txt"), "from-a")
	assertFileContent(t, filepath.Join(datadbDir, renamedPart, "data.txt"), "from-b")
	assertFileContent(t, filepath.Join(datadbDir, "91C2", "data.txt"), "from-b-2")
}

func writePart(t *testing.T, root, node, part, content string) {
	t.Helper()

	datadbDir := filepath.Join(root, node, "datadb")
	partDir := filepath.Join(datadbDir, part)
	if err := os.MkdirAll(partDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partDir, "data.txt"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(datadbDir, "parts.json"), []byte(`{"Parts":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(data) != want {
		t.Fatalf("%s = %q, want %q", path, string(data), want)
	}
}
