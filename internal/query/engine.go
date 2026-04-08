package query

import (
	"context"
	"fmt"
	"time"

	"github.com/vlog-tools/vlog-tools/internal/api"
	"github.com/vlog-tools/vlog-tools/internal/cache"
	"github.com/vlog-tools/vlog-tools/internal/config"
	"github.com/vlog-tools/vlog-tools/internal/pull"
	"go.uber.org/zap"
)

type Engine struct {
	puller   *pull.Puller
	cache    *cache.Manager
	vlClient *api.Client
	config   *config.Config
	logger   *zap.Logger
}

func NewEngine(cfg *config.Config, puller *pull.Puller, c *cache.Manager, vlClient *api.Client, logger *zap.Logger) *Engine {
	return &Engine{
		puller:   puller,
		cache:    c,
		vlClient: vlClient,
		config:   cfg,
		logger:   logger,
	}
}

func (e *Engine) Query(ctx context.Context, req api.QueryRequest) (*api.QueryResult, error) {
	start := time.Now()

	requiredPartitions := e.identifyPartitions(req.TimeRange)
	e.logger.Info("Query requires partitions", zap.Strings("partitions", requiredPartitions))

	for _, partition := range requiredPartitions {
		if !e.cache.Has(partition) {
			e.logger.Info("Pulling missing partition", zap.String("partition", partition))

			if _, err := e.puller.PullPartition(ctx, partition); err != nil {
				return nil, fmt.Errorf("failed to pull partition %s: %w", partition, err)
			}
		}
	}

	result, err := e.vlClient.Query(ctx, req)
	if err != nil {
		return nil, err
	}

	duration := time.Since(start)
	e.logger.Info("Query completed",
		zap.Duration("duration", duration),
		zap.Int("records", len(result.Records)))

	return result, nil
}

func (e *Engine) identifyPartitions(timeRange api.TimeRange) []string {
	var partitions []string
	current := timeRange.Start

	for current.Before(timeRange.End) || current.Equal(timeRange.End) {
		partition := current.Format("20060102")
		partitions = append(partitions, partition)
		current = current.AddDate(0, 0, 1) // 按天分区
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
