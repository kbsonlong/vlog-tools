package merge

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/vlog-tools/vlog-tools/internal/config"
	"go.uber.org/zap"
)

type Merger struct {
	config *config.Config
	logger *zap.Logger
}

func NewMerger(cfg *config.Config, logger *zap.Logger) *Merger {
	return &Merger{
		config: cfg,
		logger: logger,
	}
}

// MergeLocalDirs 合并多个本地目录（用于 S3 拉取场景）
func (m *Merger) MergeLocalDirs(ctx context.Context, srcDir string, partition string) (string, error) {
	start := time.Now()
	m.logger.Info("Starting local merge",
		zap.String("src_dir", srcDir),
		zap.String("partition", partition))

	mergedDir := filepath.Join(os.TempDir(), fmt.Sprintf("vlog-merged-%s-%d", partition, time.Now().Unix()))
	datadbDir := filepath.Join(mergedDir, "datadb")

	if err := os.MkdirAll(datadbDir, 0755); err != nil {
		return "", err
	}

	nodeDirs, err := m.findNodeDirectories(srcDir)
	if err != nil {
		os.RemoveAll(mergedDir)
		return "", fmt.Errorf("failed to find node directories: %w", err)
	}

	if len(nodeDirs) == 0 {
		os.RemoveAll(mergedDir)
		return "", fmt.Errorf("no node directories found in %s", srcDir)
	}

	m.logger.Info("Found node directories",
		zap.Int("count", len(nodeDirs)),
		zap.Strings("nodes", nodeDirs))

	totalParts := 0

	for _, nodeDir := range nodeDirs {
		srcDatadb := filepath.Join(srcDir, nodeDir, "datadb")
		if _, err := os.Stat(srcDatadb); os.IsNotExist(err) {
			// 有些节点可能没有数据，忽略或记录
			m.logger.Warn("datadb not found", zap.String("dir", nodeDir))
			continue
		}

		copiedParts, err := m.copyPartData(srcDatadb, datadbDir, nodeDir)
		if err != nil {
			os.RemoveAll(mergedDir)
			return "", fmt.Errorf("failed to copy from %s: %w", nodeDir, err)
		}
		totalParts += copiedParts
	}

	if totalParts == 0 {
		os.RemoveAll(mergedDir)
		return "", fmt.Errorf("no data parts found in %s", srcDir)
	}

	if err := os.Remove(filepath.Join(datadbDir, "parts.json")); err != nil && !os.IsNotExist(err) {
		os.RemoveAll(mergedDir)
		return "", fmt.Errorf("failed to remove generated parts.json: %w", err)
	}

	duration := time.Since(start)
	m.logger.Info("Local merge completed",
		zap.String("merged_dir", mergedDir),
		zap.Int("parts", totalParts),
		zap.Duration("duration", duration))

	return mergedDir, nil
}

func (m *Merger) findNodeDirectories(srcDir string) ([]string, error) {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return nil, err
	}

	var nodeDirs []string
	for _, entry := range entries {
		if entry.IsDir() {
			nodeDirs = append(nodeDirs, entry.Name())
		}
	}
	return nodeDirs, nil
}

func (m *Merger) copyPartData(srcDatadb, dstDatadb, nodeName string) (int, error) {
	entries, err := os.ReadDir(srcDatadb)
	if err != nil {
		return 0, err
	}

	copiedParts := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		partID := entry.Name()
		srcPartPath := filepath.Join(srcDatadb, partID)
		dstPartPath := m.nextPartPath(dstDatadb, partID, nodeName)

		if filepath.Base(dstPartPath) != partID {
			m.logger.Warn("Part name collision, preserving both copies",
				zap.String("node", nodeName),
				zap.String("part", partID),
				zap.String("renamed_to", filepath.Base(dstPartPath)))
		}

		if err := m.copyDirectory(srcPartPath, dstPartPath); err != nil {
			return copiedParts, err
		}
		copiedParts++
	}
	return copiedParts, nil
}

func (m *Merger) nextPartPath(dstDatadb, partID, nodeName string) string {
	dst := filepath.Join(dstDatadb, partID)
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		return dst
	}

	for i := 0; ; i++ {
		dst = filepath.Join(dstDatadb, hashedPartID(nodeName, partID, i))
		if _, err := os.Stat(dst); os.IsNotExist(err) {
			return dst
		}
	}
}

func hashedPartID(nodeName, partID string, salt int) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(nodeName))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(partID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(fmt.Sprintf("%d", salt)))
	return fmt.Sprintf("%016X", h.Sum64())
}

func (m *Merger) copyDirectory(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		return m.copyFile(path, dstPath, info.Mode())
	})
}

func (m *Merger) copyFile(src, dst string, mode os.FileMode) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
