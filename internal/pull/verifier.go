package pull

import (
	"context"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

type Verifier struct {
	logger *zap.Logger
}

func NewVerifier(logger *zap.Logger) *Verifier {
	return &Verifier{logger: logger}
}

func (v *Verifier) VerifyPartition(ctx context.Context, path string) error {
	v.logger.Debug("Verifying partition structure", zap.String("path", path))

	// 基本结构检查：确保包含 datadb 目录和至少一个 part 目录。
	// parts.json 在多节点合并后故意删除，由 VictoriaLogs 冷启动或 attach 时重建。
	datadbPath := filepath.Join(path, "datadb")
	if _, err := os.Stat(datadbPath); os.IsNotExist(err) {
		return err
	}

	entries, err := os.ReadDir(datadbPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return nil
		}
	}

	return os.ErrNotExist
}
