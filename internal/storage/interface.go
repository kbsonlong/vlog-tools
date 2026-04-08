package storage

import (
	"context"
	"time"
)

// Storage 统一存储接口
type Storage interface {
	// SyncToS3 同步本地目录到 S3
	SyncToS3(ctx context.Context, srcPath string, dstPath string) (*SyncResult, error)
	// CopyToS3 复制本地目录到 S3（不删除 S3 上本地不存在的数据）
	CopyToS3(ctx context.Context, srcPath string, dstPath string) (*SyncResult, error)
	// DownloadPartition 下载分区到本地
	DownloadPartition(ctx context.Context, s3Path string, localPath string) (*DownloadResult, error)
	// GetMetadata 获取元数据文件内容
	GetMetadata(ctx context.Context, path string) ([]byte, error)
	// PutMetadata 保存元数据文件内容
	PutMetadata(ctx context.Context, path string, data []byte) error
}

type SyncResult struct {
	SizeBytes  int64
	FilesCount int64
	Duration   time.Duration
}

type DownloadResult struct {
	SizeBytes  int64
	FilesCount int64
	Duration   time.Duration
}

type PartitionInfo struct {
	Name       string
	SizeBytes  int64
	ModifiedAt time.Time
}
