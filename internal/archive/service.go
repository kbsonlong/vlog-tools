package archive

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

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

	sourcePath := fmt.Sprintf("%s/partitions/%s", node.LocalDataPath, partition)

	if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("partition not found: %s", sourcePath)
	}

	s3Path := fmt.Sprintf("nodes/%s/%s", node.Name, partition)

	uploadResult, err := a.storage.CopyToS3(ctx, sourcePath, s3Path)
	if err != nil {
		return nil, err
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
