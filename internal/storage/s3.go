package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"go.uber.org/zap"

	"github.com/vlog-tools/vlog-tools/internal/config"
	"github.com/vlog-tools/vlog-tools/pkg/rclone"
)

type S3Storage struct {
	bucket string
	prefix string
	logger *zap.Logger
	syncer *rclone.Syncer
	cfg    *config.S3Config
}

func NewS3Storage(cfg *config.S3Config, logger *zap.Logger) (*S3Storage, error) {
	return &S3Storage{
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
		logger: logger,
		syncer: rclone.NewSyncer(logger),
		cfg:    cfg,
	}, nil
}

func (s *S3Storage) getS3Fs(ctx context.Context, path string) (fs.Fs, error) {
	s3Path := fmt.Sprintf(":s3:%s/%s", s.bucket, path)
	if s.prefix != "" {
		s3Path = fmt.Sprintf(":s3:%s/%s/%s", s.bucket, s.prefix, path)
	}

	// 在 Rclone 中，可以通过设置环境变量或直接配置参数来连接
	// 这里通过连接字符串的参数来初始化 S3 fs
	s3Path += fmt.Sprintf(",env_auth=false,access_key_id=%s,secret_access_key=%s,endpoint=%s,region=%s:",
		s.cfg.AccessKey, s.cfg.SecretKey, s.cfg.Endpoint, s.cfg.Region)

	f, err := fs.NewFs(ctx, s3Path)
	if err != nil {
		return nil, fmt.Errorf("failed to create s3 fs: %w", err)
	}
	return f, nil
}

func (s *S3Storage) getLocalFs(ctx context.Context, path string) (fs.Fs, error) {
	f, err := fs.NewFs(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to create local fs: %w", err)
	}
	return f, nil
}

func (s *S3Storage) SyncToS3(ctx context.Context, srcPath string, dstPath string) (*SyncResult, error) {
	s.logger.Info("syncing to s3", zap.String("src", srcPath), zap.String("dst", dstPath))

	srcFs, err := s.getLocalFs(ctx, srcPath)
	if err != nil {
		return nil, err
	}

	dstFs, err := s.getS3Fs(ctx, dstPath)
	if err != nil {
		return nil, err
	}

	res, err := s.syncer.Sync(ctx, dstFs, srcFs)
	if err != nil {
		return nil, err
	}

	return &SyncResult{
		Duration: res.Duration,
	}, nil
}

func (s *S3Storage) CopyToS3(ctx context.Context, srcPath string, dstPath string) (*SyncResult, error) {
	s.logger.Info("copying to s3", zap.String("src", srcPath), zap.String("dst", dstPath))

	srcFs, err := s.getLocalFs(ctx, srcPath)
	if err != nil {
		return nil, err
	}

	dstFs, err := s.getS3Fs(ctx, dstPath)
	if err != nil {
		return nil, err
	}

	res, err := s.syncer.Copy(ctx, dstFs, srcFs)
	if err != nil {
		return nil, err
	}

	return &SyncResult{
		Duration: res.Duration,
	}, nil
}

func (s *S3Storage) DownloadPartition(ctx context.Context, s3Path string, localPath string) (*DownloadResult, error) {
	s.logger.Info("downloading from s3", zap.String("src", s3Path), zap.String("dst", localPath))

	srcFs, err := s.getS3Fs(ctx, s3Path)
	if err != nil {
		return nil, err
	}

	dstFs, err := s.getLocalFs(ctx, localPath)
	if err != nil {
		return nil, err
	}

	res, err := s.syncer.Sync(ctx, dstFs, srcFs)
	if err != nil {
		return nil, err
	}

	return &DownloadResult{
		Duration:  res.Duration,
		SizeBytes: res.Bytes,
	}, nil
}

func (s *S3Storage) GetMetadata(ctx context.Context, path string) ([]byte, error) {
	f, err := s.getS3Fs(ctx, "")
	if err != nil {
		return nil, err
	}

	obj, err := f.NewObject(ctx, path)
	if err != nil {
		return nil, err
	}

	rc, err := obj.Open(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	return io.ReadAll(rc)
}

func (s *S3Storage) PutMetadata(ctx context.Context, path string, data []byte) error {
	f, err := s.getS3Fs(ctx, "")
	if err != nil {
		return err
	}

	src := io.NopCloser(bytes.NewReader(data))
	_, err = operations.Rcat(ctx, f, path, src, time.Now(), nil)
	return err
}
