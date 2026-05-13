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

func quoteConnValue(v string) string {
	if v == "" {
		return v
	}
	if strings.ContainsAny(v, ":, \"'") {
		return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
	}
	return v
}

func joinParts(parts ...string) string {
	var out []string
	for _, p := range parts {
		p = strings.Trim(p, "/")
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return strings.Join(out, "/")
}

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
	useSSL := true
	if s.cfg.UseSSL != nil {
		useSSL = *s.cfg.UseSSL
	}
	endpoint := strings.TrimSpace(s.cfg.Endpoint)
	if endpoint != "" && !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		if useSSL {
			endpoint = "https://" + endpoint
		} else {
			endpoint = "http://" + endpoint
		}
	}

	var opts []string
	opts = append(opts, fmt.Sprintf("env_auth=%t", s.cfg.EnvAuth))
	if !s.cfg.EnvAuth {
		opts = append(opts,
			fmt.Sprintf("access_key_id=%s", quoteConnValue(s.cfg.AccessKey)),
			fmt.Sprintf("secret_access_key=%s", quoteConnValue(s.cfg.SecretKey)),
		)
	}
	if endpoint != "" {
		opts = append(opts, fmt.Sprintf("endpoint=%s", quoteConnValue(endpoint)))
	}
	if s.cfg.Region != "" {
		opts = append(opts, fmt.Sprintf("region=%s", quoteConnValue(s.cfg.Region)))
	}
	provider := s.cfg.Provider
	if provider == "" {
		provider = "AWS"
	}
	opts = append(opts, fmt.Sprintf("provider=%s", quoteConnValue(provider)))
	if s.cfg.ForcePathStyle {
		opts = append(opts, "force_path_style=true")
	}
	if s.cfg.UseUnsignedPayload != nil {
		opts = append(opts, fmt.Sprintf("use_unsigned_payload=%t", *s.cfg.UseUnsignedPayload))
	}
	if s.cfg.HTTPProxy != "" {
		opts = append(opts, fmt.Sprintf("override.http_proxy=%s", quoteConnValue(s.cfg.HTTPProxy)))
	}
	if s.cfg.RcloneLogLevel != "" {
		opts = append(opts, fmt.Sprintf("global.log_level=%s", quoteConnValue(s.cfg.RcloneLogLevel)))
	}
	if s.cfg.RcloneDump != "" {
		opts = append(opts, fmt.Sprintf("global.dump=%s", quoteConnValue(s.cfg.RcloneDump)))
	}

	fsPath := joinParts(s.bucket, s.prefix, path)
	s3Path := fmt.Sprintf(":s3,%s:%s", strings.Join(opts, ","), fsPath)

	f, err := fs.NewFs(ctx, s3Path)
	if err != nil {
		return nil, fmt.Errorf("failed to create s3 fs (bucket=%s prefix=%s path=%s endpoint=%s region=%s env_auth=%t use_ssl=%t provider=%s force_path_style=%t): %w",
			s.bucket, s.prefix, path, endpoint, s.cfg.Region, s.cfg.EnvAuth, useSSL, provider, s.cfg.ForcePathStyle, err)
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

	rc, err := obj.Open(ctx)
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
