package archive

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/vlog-tools/vlog-tools/internal/api"
	"github.com/vlog-tools/vlog-tools/internal/config"
	"github.com/vlog-tools/vlog-tools/internal/metadata"
	"github.com/vlog-tools/vlog-tools/internal/storage"
	"go.uber.org/zap"
)

type Archiver struct {
	metadata *metadata.Manager
	storage  storage.Storage
	config   *config.Config
	logger   *zap.Logger
}

func NewArchiver(cfg *config.Config, store storage.Storage, meta *metadata.Manager, logger *zap.Logger) *Archiver {
	return &Archiver{
		metadata: meta,
		storage:  store,
		config:   cfg,
		logger:   logger,
	}
}

func (a *Archiver) ArchivePartition(ctx context.Context, partition string) (*ArchiveResult, error) {
	start := time.Now()

	if len(a.config.HotNodes) == 0 {
		return nil, fmt.Errorf("no hot nodes configured for archive; set hot_nodes or sidecar POD_NAME")
	}

	a.logger.Info("Starting partition archive",
		zap.String("partition", partition),
		zap.Int("nodes", len(a.config.HotNodes)))

	var wg sync.WaitGroup
	results := make(chan *NodeArchiveResult, len(a.config.HotNodes))

	for _, node := range a.config.HotNodes {
		wg.Add(1)
		go func(n config.NodeConfig) {
			defer wg.Done()

			result, err := a.archiveFromNode(ctx, n, partition)
			if err != nil {
				a.logger.Error("Failed to archive from node",
					zap.String("node", n.Name),
					zap.Error(err))
				results <- &NodeArchiveResult{
					NodeName: n.Name,
					Error:    err,
				}
				return
			}

			results <- result
		}(node)
	}

	wg.Wait()
	close(results)

	var successfulNodes []string
	var totalSize int64
	var firstErr error

	for result := range results {
		if result.Error != nil {
			if firstErr == nil {
				firstErr = result.Error
			}
			continue
		}

		successfulNodes = append(successfulNodes, result.NodeName)
		totalSize += result.SizeBytes
	}

	if len(successfulNodes) == 0 {
		return nil, fmt.Errorf("all nodes failed to archive: %w", firstErr)
	}

	if a.config.Archive.UpdateMetadata {
		if err := a.metadata.UpdatePartitionMap(ctx, partition, successfulNodes); err != nil {
			a.logger.Warn("Failed to update metadata", zap.Error(err))
		}
	}

	duration := time.Since(start)
	a.logger.Info("Archive completed",
		zap.String("partition", partition),
		zap.Int("successful_nodes", len(successfulNodes)),
		zap.Int64("total_bytes", totalSize),
		zap.Duration("duration", duration))

	return &ArchiveResult{
		Partition:       partition,
		TotalSizeBytes:  totalSize,
		Duration:        duration,
		SuccessfulNodes: successfulNodes,
	}, nil
}

func (a *Archiver) archiveFromNode(ctx context.Context, node config.NodeConfig, partition string) (*NodeArchiveResult, error) {
	start := time.Now()

	a.logger.Info("Archiving from node",
		zap.String("node", node.Name),
		zap.String("partition", partition))

	s3Path := fmt.Sprintf("nodes/%s/%s", node.Name, partition)
	successMarker := fmt.Sprintf("%s/_SUCCESS", s3Path)

	if _, err := a.storage.GetMetadata(ctx, successMarker); err == nil {
		a.logger.Info("Partition already archived, skipping",
			zap.String("node", node.Name),
			zap.String("partition", partition))
		return &NodeArchiveResult{
			NodeName:  node.Name,
			SizeBytes: 0,
			Duration:  time.Since(start),
		}, nil
	} else if !errors.Is(err, fs.ErrorObjectNotFound) {
		a.logger.Warn("Failed to check archive marker, continuing",
			zap.String("node", node.Name),
			zap.String("partition", partition),
			zap.Error(err))
	}

	if node.URL == "" {
		return nil, fmt.Errorf("node url is required for snapshot backup: node=%s", node.Name)
	}

	vl := api.NewClient(node.URL, a.logger)
	snapshotPaths, err := vl.CreatePartitionSnapshot(ctx, partition, a.config.Archive.PartitionAuthKey)
	if err != nil {
		return nil, err
	}

	var uploadResult *storage.SyncResult
	var uploadErr error
	for _, snapshotPath := range snapshotPaths {
		localSnapshotPath, err := resolveSnapshotPath(snapshotPath, node.LocalDataPath)
		if err != nil {
			uploadErr = err
		} else {
			uploadResult, uploadErr = a.storage.CopyToS3(ctx, localSnapshotPath, s3Path)
		}

		if err := vl.DeletePartitionSnapshot(ctx, snapshotPath, a.config.Archive.PartitionAuthKey); err != nil {
			a.logger.Warn("Failed to delete partition snapshot",
				zap.String("node", node.Name),
				zap.String("partition", partition),
				zap.String("snapshot_path", snapshotPath),
				zap.Error(err))
		}
		if uploadErr != nil {
			return nil, uploadErr
		}
	}

	if uploadResult == nil {
		return nil, fmt.Errorf("no snapshot uploaded for partition %s on node %s", partition, node.Name)
	}

	if err := a.storage.PutMetadata(ctx, successMarker, []byte(time.Now().UTC().Format(time.RFC3339))); err != nil {
		return nil, fmt.Errorf("put archive marker: %w", err)
	}

	duration := time.Since(start)
	a.logger.Info("Node archive completed",
		zap.String("node", node.Name),
		zap.Int64("bytes", uploadResult.SizeBytes),
		zap.Duration("duration", duration))

	return &NodeArchiveResult{
		NodeName:  node.Name,
		SizeBytes: uploadResult.SizeBytes,
		Duration:  duration,
	}, nil
}

func resolveSnapshotPath(snapshotPath string, localDataPath string) (string, error) {
	if _, err := os.Stat(snapshotPath); err == nil {
		return snapshotPath, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}

	if localDataPath == "" {
		return "", fmt.Errorf("snapshot path not found: %s", snapshotPath)
	}

	needle := string(filepath.Separator) + "snapshots" + string(filepath.Separator)
	idx := strings.Index(snapshotPath, needle)
	if idx < 0 {
		return "", fmt.Errorf("snapshot path not found and cannot map to source path: %s", snapshotPath)
	}

	mappedPath := filepath.Join(localDataPath, snapshotPath[idx+1:])
	if _, err := os.Stat(mappedPath); err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("snapshot path not found: %s or mapped path %s", snapshotPath, mappedPath)
		}
		return "", err
	}
	return mappedPath, nil
}

type NodeArchiveResult struct {
	NodeName  string
	SizeBytes int64
	Duration  time.Duration
	Error     error
}

type ArchiveResult struct {
	Partition       string
	TotalSizeBytes  int64
	Duration        time.Duration
	SuccessfulNodes []string
}
