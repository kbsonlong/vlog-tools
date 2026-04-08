package pull

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/vlog-tools/vlog-tools/internal/api"
	"github.com/vlog-tools/vlog-tools/internal/cache"
	"github.com/vlog-tools/vlog-tools/internal/config"
	"github.com/vlog-tools/vlog-tools/internal/merge"
	"github.com/vlog-tools/vlog-tools/internal/metadata"
	"github.com/vlog-tools/vlog-tools/internal/storage"
	"go.uber.org/zap"
)

type Puller struct {
	storage  storage.Storage
	cache    *cache.Manager
	merger   *merge.Merger
	vlClient *api.Client
	config   *config.Config
	metadata *metadata.Manager
	verifier *Verifier
	logger   *zap.Logger
}

func NewPuller(cfg *config.Config, store storage.Storage, c *cache.Manager, merger *merge.Merger, vlClient *api.Client, meta *metadata.Manager, logger *zap.Logger) *Puller {
	return &Puller{
		storage:  store,
		cache:    c,
		merger:   merger,
		vlClient: vlClient,
		config:   cfg,
		metadata: meta,
		verifier: NewVerifier(logger),
		logger:   logger,
	}
}

type PullResult struct {
	Partition   string
	SizeBytes   int64
	Duration    time.Duration
	Cached      bool
	SourceNodes []string
}

func (p *Puller) PullPartition(ctx context.Context, partition string) (*PullResult, error) {
	start := time.Now()

	if p.cache.Has(partition) {
		p.logger.Info("Partition cached", zap.String("partition", partition))
		return &PullResult{Partition: partition, Cached: true}, nil
	}

	partitionMap, err := p.metadata.LoadPartitionMap(ctx)
	var sourceNodes []string
	if err == nil {
		for _, pInfo := range partitionMap.Partitions {
			if pInfo.Name == partition {
				sourceNodes = pInfo.Nodes
				break
			}
		}
	} else {
		p.logger.Warn("Failed to load partition map, falling back to hot_nodes list", zap.Error(err))
		for _, n := range p.config.HotNodes {
			sourceNodes = append(sourceNodes, n.Name)
		}
	}

	if len(sourceNodes) == 0 {
		return nil, fmt.Errorf("no nodes found with partition: %s", partition)
	}

	p.logger.Info("Found partition in nodes", zap.Strings("nodes", sourceNodes))

	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("vlog-pull-%s-%d", partition, time.Now().Unix()))
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, err
	}
	defer os.RemoveAll(tempDir)

	var wg sync.WaitGroup
	errChan := make(chan error, len(sourceNodes))

	for _, nodeName := range sourceNodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()

			s3Path := fmt.Sprintf("nodes/%s/%s", node, partition)
			localPath := filepath.Join(tempDir, node)

			downloadResult, err := p.storage.DownloadPartition(ctx, s3Path, localPath)
			if err != nil {
				p.logger.Error("Failed to download from node", zap.String("node", node), zap.Error(err))
				errChan <- err
				return
			}

			p.logger.Info("Download completed", zap.String("node", node), zap.Int64("bytes", downloadResult.SizeBytes))
		}(nodeName)
	}

	wg.Wait()
	close(errChan)

	var downloadErrors []error
	for err := range errChan {
		downloadErrors = append(downloadErrors, err)
	}

	if len(downloadErrors) == len(sourceNodes) {
		return nil, fmt.Errorf("all nodes failed to download: %v", downloadErrors)
	} else if len(downloadErrors) > 0 {
		p.logger.Warn("Some nodes failed to download, proceeding with available data", zap.Int("failed", len(downloadErrors)))
	}

	mergedDir, err := p.merger.MergeLocalDirs(ctx, tempDir, partition)
	if err != nil {
		return nil, fmt.Errorf("merge failed: %w", err)
	}
	defer os.RemoveAll(mergedDir)

	if err := p.verifier.VerifyPartition(ctx, mergedDir); err != nil {
		return nil, fmt.Errorf("verification failed: %w", err)
	}

	finalPath := filepath.Join(p.config.Pull.LocalDataPath, "partitions", partition)

	if err := os.Rename(mergedDir, finalPath); err != nil {
		if err := p.copyDir(mergedDir, finalPath); err != nil {
			return nil, fmt.Errorf("failed to move merged data: %w", err)
		}
	}

	sizeBytes, _ := p.dirSize(finalPath)
	p.cache.Add(partition, sizeBytes)

	if err := p.vlClient.ReloadPartition(ctx, partition); err != nil {
		p.logger.Warn("Failed to reload partition in VictoriaLogs", zap.Error(err))
	}

	duration := time.Since(start)
	return &PullResult{
		Partition:   partition,
		SizeBytes:   sizeBytes,
		Duration:    duration,
		Cached:      false,
		SourceNodes: sourceNodes,
	}, nil
}

func (p *Puller) copyDir(src, dst string) error {
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

		return p.copyFile(path, dstPath, info.Mode())
	})
}

func (p *Puller) copyFile(src, dst string, mode os.FileMode) error {
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

func (p *Puller) dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func (p *Puller) PullByTimeRange(ctx context.Context, start, end time.Time) ([]*PullResult, error) {
	partitions := p.calculatePartitions(start, end)

	var results []*PullResult
	for _, partition := range partitions {
		result, err := p.PullPartition(ctx, partition)
		if err != nil {
			p.logger.Error("Failed to pull partition", zap.String("partition", partition), zap.Error(err))
			continue
		}
		results = append(results, result)
	}

	return results, nil
}

func (p *Puller) calculatePartitions(start, end time.Time) []string {
	var partitions []string
	current := start

	for current.Before(end) || current.Equal(end) {
		partitions = append(partitions, current.Format("20060102"))
		current = current.AddDate(0, 0, 1)
	}

	return unique(partitions)
}

func unique(strs []string) []string {
	keys := make(map[string]bool)
	var list []string
	for _, entry := range strs {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}
