package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/vlog-tools/vlog-tools/internal/api"
	"github.com/vlog-tools/vlog-tools/internal/archive"
	"github.com/vlog-tools/vlog-tools/internal/cache"
	"github.com/vlog-tools/vlog-tools/internal/config"
	"github.com/vlog-tools/vlog-tools/internal/merge"
	"github.com/vlog-tools/vlog-tools/internal/metadata"
	"github.com/vlog-tools/vlog-tools/internal/pull"
	"github.com/vlog-tools/vlog-tools/internal/storage"
)

var (
	Version    = "dev"
	Commit     = "unknown"
	BuildTime  = "unknown"
	configFile string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "vlog-tools",
		Short: "VictoriaLogs data management tools",
	}
	rootCmd.PersistentFlags().StringVar(&configFile, "config", "configs/config.yaml", "config file path")

	archiveCmd := &cobra.Command{
		Use:   "archive",
		Short: "Archive operations",
	}
	archivePartitionCmd := &cobra.Command{
		Use:   "partition [partition]",
		Short: "Archive a specific partition to S3",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, logger, store, meta, err := initDeps()
			if err != nil {
				return err
			}
			defer logger.Sync()

			archiver := archive.NewArchiver(cfg, store, meta, logger)
			res, err := archiver.ArchivePartition(context.Background(), args[0])
			if err != nil {
				return err
			}
			fmt.Printf("Archive success: %+v\n", res)
			return nil
		},
	}
	archiveRangeCmd := &cobra.Command{
		Use:   "range",
		Short: "Archive a range of partitions to S3",
		RunE: func(cmd *cobra.Command, args []string) error {
			startRaw, _ := cmd.Flags().GetString("start")
			endRaw, _ := cmd.Flags().GetString("end")
			continueOnError, _ := cmd.Flags().GetBool("continue-on-error")

			if startRaw == "" || endRaw == "" {
				return fmt.Errorf("both --start and --end are required")
			}

			start, err := parsePartitionDate(startRaw)
			if err != nil {
				return fmt.Errorf("invalid --start: %w", err)
			}
			end, err := parsePartitionDate(endRaw)
			if err != nil {
				return fmt.Errorf("invalid --end: %w", err)
			}
			if end.Before(start) {
				return fmt.Errorf("--end must be greater than or equal to --start")
			}

			cfg, logger, store, meta, err := initDeps()
			if err != nil {
				return err
			}
			defer logger.Sync()

			archiver := archive.NewArchiver(cfg, store, meta, logger)

			partitions := partitionsBetween(start, end)
			var failed []string
			for _, partition := range partitions {
				fmt.Printf("Archiving partition %s\n", partition)
				if _, err := archiver.ArchivePartition(context.Background(), partition); err != nil {
					if !continueOnError {
						return fmt.Errorf("archive partition %s: %w", partition, err)
					}
					logger.Error("archive partition failed", zap.String("partition", partition), zap.Error(err))
					failed = append(failed, partition)
				}
			}

			if len(failed) > 0 {
				return fmt.Errorf("archive range completed with failures: %s", strings.Join(failed, ","))
			}

			fmt.Printf("Archive range success: start=%s end=%s count=%d\n",
				start.Format("20060102"), end.Format("20060102"), len(partitions))
			return nil
		},
	}
	archiveRangeCmd.Flags().String("start", "", "start partition date in YYYYMMDD or YYYY-MM-DD")
	archiveRangeCmd.Flags().String("end", "", "end partition date in YYYYMMDD or YYYY-MM-DD")
	archiveRangeCmd.Flags().Bool("continue-on-error", false, "continue archiving remaining partitions when one partition fails")
	archiveCmd.AddCommand(archivePartitionCmd, archiveRangeCmd)

	pullCmd := &cobra.Command{
		Use:   "pull",
		Short: "Pull operations",
	}
	pullPartitionCmd := &cobra.Command{
		Use:   "partition [partition]",
		Short: "Pull a specific partition from S3",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, logger, store, meta, err := initDeps()
			if err != nil {
				return err
			}
			defer logger.Sync()

			c := cache.NewManager(cfg.Pull.LocalDataPath, cfg.Cache.MaxSizeGB, logger)
			m := merge.NewMerger(cfg, logger)
			vl := api.NewClient(cfg.ColdNode.URL, logger)

			puller := pull.NewPuller(cfg, store, c, m, vl, meta, logger)
			res, err := puller.PullPartition(context.Background(), args[0])
			if err != nil {
				return err
			}
			fmt.Printf("Pull success: %+v\n", res)
			return nil
		},
	}
	pullCmd.AddCommand(pullPartitionCmd)

	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start long-running services",
	}
	serveArchiveCmd := &cobra.Command{
		Use:   "archive",
		Short: "Run archive loop (for sidecar/CronJob)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, logger, store, meta, err := initDeps()
			if err != nil {
				return err
			}
			defer logger.Sync()

			everyStr, _ := cmd.Flags().GetString("every")
			cronSpec, _ := cmd.Flags().GetString("cron")
			offsetDays, _ := cmd.Flags().GetInt("offset-days")
			partitionTimezone, _ := cmd.Flags().GetString("partition-timezone")
			once, _ := cmd.Flags().GetBool("once")
			nodeName, _ := cmd.Flags().GetString("node-name")
			nodeURL, _ := cmd.Flags().GetString("node-url")
			sourcePath, _ := cmd.Flags().GetString("source-path")

			var every time.Duration
			if everyStr != "" {
				d, err := time.ParseDuration(everyStr)
				if err != nil {
					return err
				}
				every = d
			}

			if cronSpec == "" {
				cronSpec = cfg.Archive.Cron
			}
			if partitionTimezone == "" {
				partitionTimezone = cfg.Archive.PartitionTimezone
			}

			if nodeName == "" {
				nodeName = cfg.Archive.NodeName
			}
			if nodeURL == "" {
				nodeURL = cfg.Archive.NodeURL
			}
			if nodeName == "" {
				nodeName = cfg.ColdNode.Name
			}
			if sourcePath == "" {
				sourcePath = cfg.Archive.SourceDataPath
			}
			if sourcePath == "" {
				sourcePath = cfg.ColdNode.LocalDataPath
			}
			if nodeName != "" && sourcePath != "" {
				cfg.HotNodes = []config.NodeConfig{{
					Name:          nodeName,
					URL:           nodeURL,
					LocalDataPath: sourcePath,
				}}
			}

			archiver := archive.NewArchiver(cfg, store, meta, logger)
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			return archive.Run(ctx, logger, archiver, archive.RunOptions{
				Every:               every,
				Cron:                cronSpec,
				PartitionOffsetDays: offsetDays,
				PartitionTimezone:   partitionTimezone,
				Once:                once,
			})
		},
	}
	serveArchiveCmd.Flags().String("every", "", "archive interval, e.g. 24h")
	serveArchiveCmd.Flags().String("cron", "", "cron expression, e.g. \"0 2 * * *\"")
	serveArchiveCmd.Flags().Int("offset-days", 1, "partition offset days, e.g. 1 for yesterday")
	serveArchiveCmd.Flags().String("partition-timezone", "", "timezone used to calculate partition dates, e.g. UTC or Asia/Shanghai")
	serveArchiveCmd.Flags().Bool("once", false, "run once and exit")
	serveArchiveCmd.Flags().String("node-name", "", "sidecar node name")
	serveArchiveCmd.Flags().String("node-url", "", "sidecar VictoriaLogs URL, e.g. http://127.0.0.1:9428")
	serveArchiveCmd.Flags().String("source-path", "", "sidecar source data path (mount path)")
	serveCmd.AddCommand(serveArchiveCmd)

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Show version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("vlog-tools %s (%s) built at %s\n", Version, Commit, BuildTime)
		},
	}

	rootCmd.AddCommand(archiveCmd, pullCmd, serveCmd, versionCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parsePartitionDate(raw string) (time.Time, error) {
	for _, layout := range []string{"20060102", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, raw, time.UTC); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported date format %q", raw)
}

func partitionsBetween(start, end time.Time) []string {
	var partitions []string
	for current := start; !current.After(end); current = current.AddDate(0, 0, 1) {
		partitions = append(partitions, current.Format("20060102"))
	}
	return partitions
}

func initDeps() (*config.Config, *zap.Logger, storage.Storage, *metadata.Manager, error) {
	cfg, err := config.Load(configFile)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("load config: %w", err)
	}

	logger, err := zap.NewProduction()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	store, err := storage.NewS3Storage(&cfg.S3, logger)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("init s3: %w", err)
	}

	meta := metadata.NewManager(store, logger)
	return cfg, logger, store, meta, nil
}
