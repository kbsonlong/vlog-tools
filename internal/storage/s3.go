package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
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

	var opts []string
	opts = append(opts, fmt.Sprintf("env_auth=%t", s.cfg.EnvAuth))
	if !s.cfg.EnvAuth {
		opts = append(opts,
			fmt.Sprintf("access_key_id=%s", s.cfg.AccessKey),
			fmt.Sprintf("secret_access_key=%s", s.cfg.SecretKey),
		)
	}
	if s.cfg.Endpoint != "" {
		opts = append(opts, fmt.Sprintf("endpoint=%s", s.cfg.Endpoint))
	}
	if s.cfg.Region != "" {
		opts = append(opts, fmt.Sprintf("region=%s", s.cfg.Region))
	}
	if s.cfg.Provider != "" {
		opts = append(opts, fmt.Sprintf("provider=%s", s.cfg.Provider))
	}
	if s.cfg.ForcePathStyle {
		opts = append(opts, "force_path_style=true")
	}
	s3Path += "," + strings.Join(opts, ",") + ":"

	f, err := fs.NewFs(ctx, s3Path)
	if err != nil {
		return nil, fmt.Errorf("failed to create s3 fs (bucket=%s prefix=%s path=%s endpoint=%s region=%s env_auth=%t provider=%s force_path_style=%t): %w",
			s.bucket, s.prefix, path, s.cfg.Endpoint, s.cfg.Region, s.cfg.EnvAuth, s.cfg.Provider, s.cfg.ForcePathStyle, err)
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
