package pull

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
)

func TestVerifyPartitionAllowsMissingPartsJSON(t *testing.T) {
	partitionDir := t.TempDir()
	partDir := filepath.Join(partitionDir, "datadb", "18BAEF8F11A364C9")
	if err := os.MkdirAll(partDir, 0o755); err != nil {
		t.Fatal(err)
	}

	v := NewVerifier(zap.NewNop())
	if err := v.VerifyPartition(context.Background(), partitionDir); err != nil {
		t.Fatalf("VerifyPartition() error = %v", err)
	}
}

func TestVerifyPartitionRejectsEmptyDatadb(t *testing.T) {
	partitionDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(partitionDir, "datadb"), 0o755); err != nil {
		t.Fatal(err)
	}

	v := NewVerifier(zap.NewNop())
	if err := v.VerifyPartition(context.Background(), partitionDir); err == nil {
		t.Fatal("VerifyPartition() error = nil, want error")
	}
}
