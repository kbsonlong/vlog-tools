package archive

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

type RunOptions struct {
	Every               time.Duration
	Cron                string
	PartitionOffsetDays int
	PartitionTimezone   string
	Once                bool
}

func Run(ctx context.Context, logger *zap.Logger, archiver *Archiver, opts RunOptions) error {
	if opts.Every == 0 {
		opts.Every = 24 * time.Hour
	}

	location, err := partitionLocation(opts.PartitionTimezone)
	if err != nil {
		return err
	}

	runOnce := func() error {
		partition := partitionForTime(time.Now(), opts.PartitionOffsetDays, location)
		logger.Info("archive tick",
			zap.String("partition", partition),
			zap.String("partition_timezone", location.String()))
		_, err := archiver.ArchivePartition(ctx, partition)
		if err != nil {
			if strings.Contains(err.Error(), "partition not found:") {
				logger.Warn("partition dir missing, skipping", zap.String("partition", partition), zap.Error(err))
				return nil
			}
			return fmt.Errorf("archive partition %s: %w", partition, err)
		}
		return nil
	}

	if opts.Once {
		if err := runOnce(); err != nil {
			logger.Error("archive run failed", zap.Error(err))
			return err
		}
		return nil
	}

	if opts.Cron != "" {
		c := cron.New(cron.WithLocation(time.Local))
		_, err := c.AddFunc(opts.Cron, func() {
			if err := runOnce(); err != nil {
				logger.Error("archive run failed", zap.Error(err))
			}
		})
		if err != nil {
			return fmt.Errorf("invalid cron: %w", err)
		}
		c.Start()
		defer c.Stop()

		<-ctx.Done()
		return ctx.Err()
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

func partitionLocation(name string) (*time.Location, error) {
	if name == "" || name == "Local" {
		return time.Local, nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("invalid partition timezone %q: %w", name, err)
	}
	return loc, nil
}

func partitionForTime(now time.Time, offsetDays int, loc *time.Location) string {
	return now.In(loc).AddDate(0, 0, -offsetDays).Format("20060102")
}
