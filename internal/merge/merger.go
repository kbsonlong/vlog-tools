package merge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
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

	var wg sync.WaitGroup
	errChan := make(chan error, len(nodeDirs))

	for _, nodeDir := range nodeDirs {
		wg.Add(1)
		go func(dir string) {
			defer wg.Done()
			srcDatadb := filepath.Join(srcDir, dir, "datadb")
			if _, err := os.Stat(srcDatadb); os.IsNotExist(err) {
				// 有些节点可能没有数据，忽略或记录
				m.logger.Warn("datadb not found", zap.String("dir", dir))
				return
			}

			if err := m.copyStreamData(srcDatadb, datadbDir); err != nil {
				errChan <- fmt.Errorf("failed to copy from %s: %w", dir, err)
				return
			}
		}(nodeDir)
	}

	wg.Wait()
	close(errChan)

	var firstErr error
	for err := range errChan {
		if firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		os.RemoveAll(mergedDir)
		return "", firstErr
	}

	if err := m.mergePartsJSON(datadbDir, srcDir, nodeDirs); err != nil {
		os.RemoveAll(mergedDir)
		return "", fmt.Errorf("failed to merge parts.json: %w", err)
	}

	duration := time.Since(start)
	m.logger.Info("Local merge completed",
		zap.String("merged_dir", mergedDir),
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

func (m *Merger) copyStreamData(srcDatadb, dstDatadb string) error {
	entries, err := os.ReadDir(srcDatadb)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		streamID := entry.Name()
		srcStreamPath := filepath.Join(srcDatadb, streamID)
		dstStreamPath := filepath.Join(dstDatadb, streamID)

		if _, err := os.Stat(dstStreamPath); err == nil {
			m.logger.Debug("Stream already exists, skipping", zap.String("stream_id", streamID))
			continue
		}

		if err := m.copyDirectory(srcStreamPath, dstStreamPath); err != nil {
			return err
		}
	}
	return nil
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

func (m *Merger) mergePartsJSON(dstDatadbDir, srcDir string, nodeDirs []string) error {
	allParts := make(map[string]interface{})
	sourceNodes := make(map[string]bool)

	for _, dir := range nodeDirs {
		partsFile := filepath.Join(srcDir, dir, "datadb", "parts.json")
		if _, err := os.Stat(partsFile); os.IsNotExist(err) {
			continue
		}

		data, err := os.ReadFile(partsFile)
		if err != nil {
			return err
		}

		var partsData map[string]interface{}
		if err := json.Unmarshal(data, &partsData); err != nil {
			return err
		}

		if parts, ok := partsData["Parts"].([]interface{}); ok {
			for _, part := range parts {
				if partMap, ok := part.(map[string]interface{}); ok {
					partID := partMap["PartID"]
					if partID != nil {
						allParts[fmt.Sprintf("%v", partID)] = part
					}
				}
			}
		}

		sourceNodes[dir] = true
	}

	merged := map[string]interface{}{
		"Parts": make([]interface{}, 0, len(allParts)),
		"Metadata": map[string]interface{}{
			"SourceNodes": len(sourceNodes),
			"TotalParts":  len(allParts),
			"MergedAt":    time.Now().UTC().Format(time.RFC3339),
			"MergedBy":    "vlog-tools",
		},
	}

	for _, part := range allParts {
		merged["Parts"] = append(merged["Parts"].([]interface{}), part)
	}

	data, err := json.MarshalIndent(merged, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(dstDatadbDir, "parts.json"), data, 0644)
}
