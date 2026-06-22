package config

import (
	"fmt"
	"os"
	"strings"

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
	DisableChecksum    *bool  `yaml:"disable_checksum"`
	UseMultipartEtag   *bool  `yaml:"use_multipart_etag"`
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
	PartitionTimezone   string `yaml:"partition_timezone"`
	PartitionAuthKey    string `yaml:"partition_auth_key"`
	UpdateMetadata      bool   `yaml:"update_metadata"`
	NodeName            string `yaml:"node_name"`
	NodeURL             string `yaml:"node_url"`
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
	cfg.applySidecarDefaults()

	// 验证
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) expandEnvVars() error {
	expand := func(v string) string {
		return os.Expand(v, func(key string) string {
			return os.Getenv(key)
		})
	}

	c.Global.Environment = expand(c.Global.Environment)
	for i := range c.HotNodes {
		c.HotNodes[i].Name = expand(c.HotNodes[i].Name)
		c.HotNodes[i].URL = expand(c.HotNodes[i].URL)
		c.HotNodes[i].LocalDataPath = expand(c.HotNodes[i].LocalDataPath)
	}
	c.ColdNode.Name = expand(c.ColdNode.Name)
	c.ColdNode.URL = expand(c.ColdNode.URL)
	c.ColdNode.LocalDataPath = expand(c.ColdNode.LocalDataPath)

	c.S3.Endpoint = expand(c.S3.Endpoint)
	c.S3.Bucket = expand(c.S3.Bucket)
	c.S3.Region = expand(c.S3.Region)
	c.S3.Prefix = expand(c.S3.Prefix)
	c.S3.Provider = expand(c.S3.Provider)
	c.S3.HTTPProxy = expand(c.S3.HTTPProxy)
	c.S3.RcloneLogLevel = expand(c.S3.RcloneLogLevel)
	c.S3.RcloneDump = expand(c.S3.RcloneDump)
	c.S3.AccessKey = expand(c.S3.AccessKey)
	c.S3.SecretKey = expand(c.S3.SecretKey)

	c.Archive.Every = expand(c.Archive.Every)
	c.Archive.Cron = expand(c.Archive.Cron)
	c.Archive.PartitionTimezone = expand(c.Archive.PartitionTimezone)
	c.Archive.PartitionAuthKey = expand(c.Archive.PartitionAuthKey)
	c.Archive.NodeName = expand(c.Archive.NodeName)
	c.Archive.NodeURL = expand(c.Archive.NodeURL)
	c.Archive.SourceDataPath = expand(c.Archive.SourceDataPath)

	c.Pull.LocalDataPath = expand(c.Pull.LocalDataPath)
	c.Logging.Level = expand(c.Logging.Level)
	c.Metrics.Port = expand(c.Metrics.Port)
	return nil
}

func (c *Config) applySidecarDefaults() {
	podName := strings.TrimSpace(os.Getenv("POD_NAME"))
	if c.Archive.NodeName == "" && podName != "" {
		c.Archive.NodeName = podName
	}
	if c.Archive.NodeURL == "" {
		c.Archive.NodeURL = "http://127.0.0.1:9428"
	}
	if c.Archive.SourceDataPath == "" {
		c.Archive.SourceDataPath = "/var/lib/victorialogs"
	}

	if len(c.HotNodes) == 0 && c.Archive.NodeName != "" && c.Archive.NodeURL != "" && c.Archive.SourceDataPath != "" {
		c.HotNodes = []NodeConfig{{
			Name:          c.Archive.NodeName,
			URL:           c.Archive.NodeURL,
			LocalDataPath: c.Archive.SourceDataPath,
		}}
	}
}

func (c *Config) Validate() error {
	if c.S3.Bucket == "" {
		return fmt.Errorf("S3 bucket is required")
	}
	if !c.S3.EnvAuth && (c.S3.AccessKey == "" || c.S3.SecretKey == "") {
		return fmt.Errorf("S3 access_key/secret_key are required when env_auth is false")
	}
	if len(c.HotNodes) == 0 && c.Archive.NodeName == "" {
		return fmt.Errorf("no hot nodes configured; set hot_nodes or provide POD_NAME for sidecar mode")
	}
	return nil
}
