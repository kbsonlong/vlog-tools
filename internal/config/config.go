package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Global   GlobalConfig  `yaml:"global"`
	HotNodes []NodeConfig  `yaml:"hot_nodes"`
	ColdNode NodeConfig    `yaml:"cold_node"`
	S3       S3Config      `yaml:"s3"`
	Archive  ArchiveConfig `yaml:"archive"`
	Pull     PullConfig    `yaml:"pull"`
	Cache    CacheConfig   `yaml:"cache"`
	Logging  LoggingConfig `yaml:"logging"`
	Metrics  MetricsConfig `yaml:"metrics"`
}

type GlobalConfig struct {
	Environment string `yaml:"environment"`
}

type NodeConfig struct {
	Name          string `yaml:"name"`
	URL           string `yaml:"url"`
	LocalDataPath string `yaml:"local_data_path"`
}

type S3Config struct {
	Endpoint           string `yaml:"endpoint"`
	Bucket             string `yaml:"bucket"`
	Region             string `yaml:"region"`
	Prefix             string `yaml:"prefix"`
	UseSSL             *bool  `yaml:"use_ssl"`
	EnvAuth            bool   `yaml:"env_auth"`
	Provider           string `yaml:"provider"`
	ForcePathStyle     bool   `yaml:"force_path_style"`
	UseUnsignedPayload *bool  `yaml:"use_unsigned_payload"`
	HTTPProxy          string `yaml:"http_proxy"`
	RcloneLogLevel     string `yaml:"rclone_log_level"`
	RcloneDump         string `yaml:"rclone_dump"`
	AccessKey          string `yaml:"access_key"`
	SecretKey          string `yaml:"secret_key"`
}

type ArchiveConfig struct {
	Concurrency         int    `yaml:"concurrency"`
	Every               string `yaml:"every"`
	Cron                string `yaml:"cron"`
	PartitionOffsetDays int    `yaml:"partition_offset_days"`
	UpdateMetadata      bool   `yaml:"update_metadata"`
	NodeName            string `yaml:"node_name"`
	SourceDataPath      string `yaml:"source_data_path"`
}

type PullConfig struct {
	LocalDataPath string `yaml:"local_data_path"`
}

type CacheConfig struct {
	MaxSizeGB     int64 `yaml:"max_size_gb"`
	RetentionDays int   `yaml:"retention_days"`
}

type LoggingConfig struct {
	Level string `yaml:"level"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Port    string `yaml:"port"`
}

// Load 加载配置文件
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// 环境变量替换 (简单示例，可后续扩展)
	if err := cfg.expandEnvVars(); err != nil {
		return nil, err
	}

	// 验证
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) expandEnvVars() error {
	// TODO: 实现 ${VAR} 替换
	return nil
}

func (c *Config) Validate() error {
	if c.S3.Bucket == "" {
		return fmt.Errorf("S3 bucket is required")
	}
	if !c.S3.EnvAuth && (c.S3.AccessKey == "" || c.S3.SecretKey == "") {
		return fmt.Errorf("S3 access_key/secret_key are required when env_auth is false")
	}
	return nil
}
