package archive

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type RunOptions struct {
	Every               time.Duration
	PartitionOffsetDays int
	Once                bool
}

func Run(ctx context.Context, logger *zap.Logger, archiver *Archiver, opts RunOptions) error {
	if opts.PartitionOffsetDays == 0 {
		opts.PartitionOffsetDays = 1
	}
	if opts.Every == 0 {
		opts.Every = 24 * time.Hour
	}

	runOnce := func() error {
		partition := time.Now().AddDate(0, 0, -opts.PartitionOffsetDays).Format("20060102")
		logger.Info("archive tick", zap.String("partition", partition))
		_, err := archiver.ArchivePartition(ctx, partition)
		if err != nil {
			return fmt.Errorf("archive partition %s: %w", partition, err)
		}
		return nil
	}

	if err := runOnce(); err != nil {
		logger.Error("archive run failed", zap.Error(err))
		if opts.Once {
			return err
		}
	}
	if opts.Once {
		return nil
	}

	ticker := time.NewTicker(opts.Every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := runOnce(); err != nil {
				logger.Error("archive run failed", zap.Error(err))
			}
		}
	}
}
