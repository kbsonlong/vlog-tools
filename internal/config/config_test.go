package config

import (
	"testing"
)

func TestConfigValidation(t *testing.T) {
	cfg := &Config{}
	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for empty S3 bucket, got nil")
	}

	cfg.S3.Bucket = "test-bucket"
	cfg.S3.EnvAuth = true
	err = cfg.Validate()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestExpandEnvVars(t *testing.T) {
	t.Setenv("POD_NAME", "vlstorage-0")
	t.Setenv("S3_BUCKET", "audit-log")

	cfg := &Config{
		Global: GlobalConfig{Environment: "${POD_NAME}"},
		S3: S3Config{
			Bucket: "${S3_BUCKET}",
		},
		Archive: ArchiveConfig{
			NodeName:       "${POD_NAME}",
			NodeURL:        "http://127.0.0.1:9428",
			SourceDataPath: "/var/lib/victorialogs",
		},
	}

	if err := cfg.expandEnvVars(); err != nil {
		t.Fatalf("expandEnvVars() error = %v", err)
	}
	if cfg.Global.Environment != "vlstorage-0" {
		t.Fatalf("environment = %q", cfg.Global.Environment)
	}
	if cfg.S3.Bucket != "audit-log" {
		t.Fatalf("bucket = %q", cfg.S3.Bucket)
	}
	if cfg.Archive.NodeName != "vlstorage-0" {
		t.Fatalf("node name = %q", cfg.Archive.NodeName)
	}
}

func TestApplySidecarDefaultsUsesPodName(t *testing.T) {
	t.Setenv("POD_NAME", "vlstorage-1")

	cfg := &Config{
		Archive: ArchiveConfig{
			NodeURL:        "http://127.0.0.1:9428",
			SourceDataPath: "/var/lib/victorialogs",
		},
	}

	cfg.applySidecarDefaults()
	if cfg.Archive.NodeName != "vlstorage-1" {
		t.Fatalf("archive node name = %q", cfg.Archive.NodeName)
	}
	if len(cfg.HotNodes) != 1 {
		t.Fatalf("hot_nodes len = %d", len(cfg.HotNodes))
	}
	if cfg.HotNodes[0].Name != "vlstorage-1" {
		t.Fatalf("hot_nodes[0].name = %q", cfg.HotNodes[0].Name)
	}
}
