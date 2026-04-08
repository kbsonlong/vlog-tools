package rclone

import (
	"context"
	"time"

	_ "github.com/rclone/rclone/backend/local" // 注册 local backend
	_ "github.com/rclone/rclone/backend/s3"    // 注册 s3 backend
	"github.com/rclone/rclone/fs"
	rcloneSync "github.com/rclone/rclone/fs/sync"
	"go.uber.org/zap"
)

type Syncer struct {
	logger *zap.Logger
}

func NewSyncer(logger *zap.Logger) *Syncer {
	return &Syncer{logger: logger}
}

func (s *Syncer) Sync(ctx context.Context, dstFs, srcFs fs.Fs) (*SyncResult, error) {
	// Sync srcFs to dstFs
	// operations.Sync(ctx context.Context, fdst fs.Fs, fsrc fs.Fs, createEmptySrcDirs bool) error
	start := time.Now()
	err := rcloneSync.Sync(ctx, dstFs, srcFs, true)
	if err != nil {
		return nil, err
	}

	// 统计逻辑可以使用 rclone core stats，这里简化为只返回 duration
	return &SyncResult{
		Duration: time.Since(start),
	}, nil
}

func (s *Syncer) Copy(ctx context.Context, dstFs, srcFs fs.Fs) (*SyncResult, error) {
	start := time.Now()
	err := rcloneSync.CopyDir(ctx, dstFs, srcFs, true)
	if err != nil {
		return nil, err
	}
	return &SyncResult{
		Duration: time.Since(start),
	}, nil
}

type SyncResult struct {
	Files    int64
	Bytes    int64
	Duration time.Duration
}
