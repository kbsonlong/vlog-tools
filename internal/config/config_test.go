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
	err = cfg.Validate()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
