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

	// 基本结构检查：确保包含 datadb 目录和 parts.json
	datadbPath := filepath.Join(path, "datadb")
	if _, err := os.Stat(datadbPath); os.IsNotExist(err) {
		return err
	}

	partsPath := filepath.Join(datadbPath, "parts.json")
	if _, err := os.Stat(partsPath); os.IsNotExist(err) {
		return err
	}

	return nil
}
