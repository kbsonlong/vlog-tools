# VictoriaLogs 数据管理工具 - 技术设计方案

## 📋 项目概述

### 项目名称
**vlog-tools** - VictoriaLogs 数据管理工具套件

### 项目目标
构建一个企业级的 Go 语言工具，统一管理 VictoriaLogs 的数据生命周期：

1. **归档** (Archive) - 热节点 → S3 长期存储
2. **迁移** (Migrate) - 热节点 → 冷节点（直连）
3. **拉取** (Pull) - S3 → 冷节点（按需）
4. **查询** (Query) - 智能查询优化
5. **缓存** (Cache) - 本地缓存管理

### 核心价值

- ✅ **成本优化**: S3 存储节省 93% 成本
- ✅ **数据可靠性**: 多节点物理合并，完整性校验
- ✅ **按需加载**: 只拉取查询需要的数据
- ✅ **智能缓存**: LRU 策略，提升查询性能
- ✅ **生产级**: 完整监控、日志、错误处理
- ✅ **零依赖**: 无需 k8s SDK，仅输出 YAML 部署文件

### 技术栈

**核心依赖**：
- **Go 1.21+**: 主要开发语言
- **Rclone SDK/Sync**: S3 数据同步（支持所有 S3 兼容存储）
- **Zap**: 结构化日志
- **Prometheus Client**: 指标收集

**无依赖**：
- ❌ **不使用 k8s SDK**: 无需 client-go、controller-runtime 等
- ❌ **不使用 kubectl**: 不通过命令行操作 k8s
- ✅ **仅输出 YAML**: 部署时通过 kubectl apply -f 应用

**部署方式**：
- Kubernetes: CronJob（归档）+ Deployment（拉取服务）
- Docker/Bare Metal: 直接运行二进制文件

---

## 🏗️ 系统架构

### 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                         vlog-tools (Go)                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   CLI Interface                          │    │
│  │  archive | migrate | pull | query | cache | serve       │    │
│  └─────────────────────────────────────────────────────────┘    │
│                          │                                       │
│                          ▼                                       │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                  Core Services Layer                     │    │
│  ├──────────────┬──────────────┬──────────────┬────────────┤    │
│  │   Archiver   │   Migrator   │   Puller     │   Querier   │    │
│  │   Service    │   Service    │   Service    │   Service   │    │
│  └──────────────┴──────────────┴──────────────┴────────────┘    │
│           │              │              │              │        │
│  ┌────────┴────────┬─────┴──────┬───────┴──────────┬───────┐   │
│  ▼                 ▼            ▼                  ▼       ▼   │
│  ┌────────────┐ ┌──────────┐ ┌─────────┐ ┌──────────────┐   │
│  │  Metadata  │ │  Merge   │ │  Cache  │ │   Metrics    │   │
│  │  Manager   │ │  Engine  │ │ Manager │ │  & Logging   │   │
│  └────────────┘ └──────────┘ └─────────┘ └──────────────┘   │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
           │                │                │
           ▼                ▼                ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│VictoriaLogs  │  │    S3        │  │  Local       │
│  Hot Nodes   │  │  Storage     │  │  Storage     │
│  (3 nodes)   │  │              │  │              │
└──────────────┘  └──────────────┘  └──────────────┘
```

### 数据流向

```
【场景 1: 归档流程（热节点 → S3）】
Hot Node-1 ──► S3/nodes/node-1/20260407/ ──┐
Hot Node-2 ──► S3/nodes/node-2/20260407/ ──┼──► [按节点分离保存]
Hot Node-3 ──► S3/nodes/node-3/20260407/ ──┘

特点：
  ✅ 并行上传，速度快
  ✅ 无合并开销，资源消耗少
  ✅ 独立存储，灵活访问
  ✅ 更新元数据（partition-map.json）

【场景 2: 迁移流程（热节点 → 冷节点，直连）】
Hot Node ──► [直接迁移] ──► Cold Node

【场景 3: 拉取流程（S3 → 冷节点，按需合并）】
User Query 需要 20260407 数据
    │
    ▼
[检查本地缓存] ──► 缓存未命中
    │
    ▼
[从 S3 拉取]
    │
    ├─► S3/nodes/node-1/20260407/ ──┐
    ├─► S3/nodes/node-2/20260407/ ──┼──► [物理合并] ──► Cold Node/20260407/
    └─► S3/nodes/node-3/20260407/ ──┘
    │
    ▼
[添加到缓存] ──► [通知 VL 重新加载] ──► [执行查询]

特点：
  ✅ 按需合并，只合并需要的分区
  ✅ 一次性拉取，节省带宽
  ✅ 本地合并，速度快
  ✅ 合并后永久缓存

【场景 4: 查询流程】
User Query ──► [分析] ──► [检查缓存]
    │                    │
    │                    ├─► 命中 ──► [直接查询]
    │                    │
    │                    └─► 未命中 ──► [触发拉取+合并]
    │
    ▼
[返回结果]
```

---

## 📂 项目结构

```
vlog-tools/
├── cmd/
│   ├── vlog-tools/                    # 主 CLI 工具
│   │   └── main.go
│   ├── archive/                       # 归档服务（独立部署）
│   │   └── main.go
│   ├── puller/                        # 拉取服务（独立部署）
│   │   └── main.go
│   └── query-proxy/                   # 查询代理（可选）
│       └── main.go
│
├── internal/
│   ├── config/                        # 配置管理
│   │   ├── config.go
│   │   └── validator.go
│   │
│   ├── api/                           # VictoriaLogs API 客户端
│   │   ├── client.go
│   │   ├── partition.go
│   │   └── snapshot.go
│   │
│   ├── archive/                       # 归档服务
│   │   ├── service.go
│   │   ├── merger.go                  # 多节点数据合并
│   │   ├── uploader.go                # S3 上传
│   │   └── metadata.go                # 元数据管理
│   │
│   ├── migrate/                       # 迁移服务
│   │   ├── service.go
│   │   ├── orchestrator.go            # 迁移编排
│   │   ├── snapshot.go
│   │   └── rollback.go                # 失败回滚
│   │
│   ├── pull/                          # 拉取服务
│   │   ├── service.go
│   │   ├── downloader.go              # S3 下载
│   │   └── verifier.go                # 完整性校验
│   │
│   ├── query/                         # 查询优化
│   │   ├── engine.go
│   │   ├── analyzer.go                # 查询分析
│   │   └── planner.go                 # 查询计划
│   │
│   ├── cache/                         # 缓存管理
│   │   ├── manager.go
│   │   ├── lru.go                     # LRU 淘汰
│   │   └── cleanup.go                 # 定期清理
│   │
│   ├── storage/                       # 存储抽象
│   │   ├── interface.go               # 存储接口
│   │   ├── s3.go                      # S3 实现
│   │   ├── local.go                   # 本地实现
│   │   └── merge.go                   # 数据合并
│   │
│   └── metadata/                      # 元数据管理
│       ├── partition_map.go           # 分区映射表
│       ├── inventory.go               # 数据清单
│       └── version.go                 # 版本控制
│
├── pkg/
│   ├── rclone/                        # Rclone SDK 封装
│   │   ├── fs.go
│   │   ├── sync.go
│   │   └── operations.go
│   │
│   ├── retry/                         # 重试机制
│   │   └── retry.go
│   │
│   ├── logger/                        # 结构化日志
│   │   └── logger.go
│   │
│   └── metrics/                       # Prometheus 指标
│       ├── metrics.go
│       └── collectors.go
│
├── configs/
│   ├── config.yaml                    # 配置文件示例
│   ├── config-archive.yaml            # 归档配置
│   └── config-puller.yaml             # 拉取配置
│
├── deployments/
│   ├── kubernetes/
│   │   ├── cronjob-archive.yaml       # 归档 CronJob
│   │   ├── deployment-puller.yaml     # 拉取服务
│   │   └── service-cold-node.yaml     # 冷节点服务
│   │
│   └── docker/
│       ├── Dockerfile.tools           # CLI 工具
│       ├── Dockerfile.archive         # 归档服务
│       └── Dockerfile.puller          # 拉取服务
│
├── scripts/
│   ├── build.sh                       # 构建脚本
│   ├── test.sh                        # 测试脚本
│   └── release.sh                     # 发布脚本
│
├── go.mod
├── go.sum
├── Makefile
├── README.md
└── DESIGN.md                          # 本文档
```

---

## 🧩 核心组件设计

### 1. 配置管理 (internal/config)

```go
type Config struct {
    // 全局配置
    Global    GlobalConfig    `yaml:"global"`
    // 热节点配置
    HotNodes  []NodeConfig    `yaml:"hot_nodes"`
    // 冷节点配置
    ColdNode  NodeConfig      `yaml:"cold_node"`
    // S3 配置
    S3        S3Config        `yaml:"s3"`
    // 归档配置
    Archive   ArchiveConfig   `yaml:"archive"`
    // 迁移配置
    Migrate   MigrateConfig   `yaml:"migrate"`
    // 拉取配置
    Pull      PullConfig      `yaml:"pull"`
    // 缓存配置
    Cache     CacheConfig     `yaml:"cache"`
    // 日志配置
    Logging   LoggingConfig   `yaml:"logging"`
    // 指标配置
    Metrics   MetricsConfig   `yaml:"metrics"`
}

type NodeConfig struct {
    Name         string `yaml:"name"`
    URL          string `yaml:"url"`           // VictoriaLogs HTTP API endpoint（可选，用于健康检查）
    LocalDataPath string `yaml:"local_data_path"` // 本地数据路径（PVC 挂载或本地路径）
}

type S3Config struct {
    Endpoint     string `yaml:"endpoint"`
    Bucket       string `yaml:"bucket"`
    Region       string `yaml:"region"`
    Prefix       string `yaml:"prefix"`
    AccessKey    string `yaml:"access_key"`
    SecretKey    string `yaml:"secret_key"`
    SessionToken string `yaml:"session_token"`
}
```

### 2. 存储抽象 (internal/storage)

```go
// Storage 统一存储接口
type Storage interface {
    // 上传分区
    UploadPartition(ctx context.Context, partition string, srcPath string) (*UploadResult, error)
    // 下载分区
    DownloadPartition(ctx context.Context, partition string, dstPath string) (*DownloadResult, error)
    // 列出分区
    ListPartitions(ctx context.Context) ([]PartitionInfo, error)
    // 删除分区
    DeletePartition(ctx context.Context, partition string) error
    // 检查分区是否存在
    PartitionExists(ctx context.Context, partition string) (bool, error)
    // 获取分区元数据
    GetPartitionMetadata(ctx context.Context, partition string) (*PartitionMetadata, error)
}

// S3Storage S3 存储
type S3Storage struct {
    fs       fs.Fs
    bucket   string
    prefix   string
    metrics  *metrics.Registry
    logger   *zap.Logger
}

// LocalStorage 本地存储
type LocalStorage struct {
    basePath string
    logger   *zap.Logger
}
```

### 3. 归档服务 (internal/archive)

```go
// Archiver 归档服务（热节点 → S3）
type Archiver struct {
    metadata *metadata.Manager
    storage  storage.Storage
    config   *config.ArchiveConfig
    logger   *zap.Logger
    metrics  *metrics.Registry
}

// ArchivePartition 归档指定分区（按节点分离上传）
func (a *Archiver) ArchivePartition(ctx context.Context, partition string) (*ArchiveResult, error) {
    start := time.Now()

    a.logger.Info("Starting partition archive",
        zap.String("partition", partition),
        zap.Int("nodes", len(a.config.HotNodes)))

    var wg sync.WaitGroup
    results := make(chan *NodeArchiveResult, len(a.config.HotNodes))

    // 并发从每个热节点上传到 S3（节点独立目录）
    for _, node := range a.config.HotNodes {
        wg.Add(1)
        go func(n config.NodeConfig) {
            defer wg.Done()

            result, err := a.archiveFromNode(ctx, n, partition)
            if err != nil {
                a.logger.Error("Failed to archive from node",
                    zap.String("node", n.Name),
                    zap.Error(err))
                results <- &NodeArchiveResult{
                    NodeName: n.Name,
                    Error:    err,
                }
                return
            }

            results <- result
        }(node)
    }

    wg.Wait()
    close(results)

    // 收集结果
    var successfulNodes []string
    var totalSize int64
    var firstErr error

    for result := range results {
        if result.Error != nil {
            if firstErr == nil {
                firstErr = result.Error
            }
            continue
        }

        successfulNodes = append(successfulNodes, result.NodeName)
        totalSize += result.SizeBytes
    }

    if len(successfulNodes) == 0 {
        return nil, fmt.Errorf("all nodes failed to archive: %w", firstErr)
    }

    // 更新元数据（记录哪些节点有这个分区）
    if err := a.metadata.UpdatePartitionMap(ctx, partition, successfulNodes); err != nil {
        a.logger.Warn("Failed to update metadata", zap.Error(err))
    }

    duration := time.Since(start)
    a.logger.Info("Archive completed",
        zap.String("partition", partition),
        zap.Int("successful_nodes", len(successfulNodes)),
        zap.Int64("total_bytes", totalSize),
        zap.Duration("duration", duration))

    return &ArchiveResult{
        Partition:      partition,
        TotalSizeBytes: totalSize,
        Duration:       duration,
        SuccessfulNodes: successfulNodes,
    }, nil
}

// archiveFromNode 从单个节点归档到 S3（通过本地文件系统或共享 PVC）
func (a *Archiver) archiveFromNode(ctx context.Context, node config.NodeConfig, partition string) (*NodeArchiveResult, error) {
    start := time.Now()

    a.logger.Info("Archiving from node",
        zap.String("node", node.Name),
        zap.String("partition", partition))

    // 本地数据路径（通过 PVC 或本地挂载）
    sourcePath := fmt.Sprintf("%s/partitions/%s", node.LocalDataPath, partition)

    // 检查数据是否存在
    if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
        return nil, fmt.Errorf("partition not found: %s", sourcePath)
    }

    // S3 路径: s3://vlog-archive/nodes/{node-name}/{partition}/
    s3Path := fmt.Sprintf("nodes/%s/%s", node.Name, partition)

    // 使用 rclone sync 上传到 S3
    uploadResult, err := a.storage.SyncToS3(ctx, sourcePath, s3Path)
    if err != nil {
        return nil, err
    }

    duration := time.Since(start)
    a.logger.Info("Node archive completed",
        zap.String("node", node.Name),
        zap.Int64("bytes", uploadResult.SizeBytes),
        zap.Duration("duration", duration))

    return &NodeArchiveResult{
        NodeName:  node.Name,
        SizeBytes: uploadResult.SizeBytes,
        Duration:  duration,
    }, nil
}

type NodeArchiveResult struct {
    NodeName  string
    SizeBytes int64
    Duration  time.Duration
    Error     error
}

type ArchiveResult struct {
    Partition       string
    TotalSizeBytes  int64
    Duration        time.Duration
    SuccessfulNodes []string
}

```

### 4. 数据合并引擎 (internal/merge)

> 当前实现采用“按 part 目录合并 + 删除 parts.json”的恢复策略：从每个节点下载的 `datadb/<partID>` 目录会完整复制到目标冷节点分区；如果不同节点出现同名 part 目录，目标目录会生成新的 16 位十六进制 part 目录名避免覆盖；合并后不生成 `parts.json`，而是删除目标 `datadb/parts.json`，让 VictoriaLogs 在冷启动或 attach 时根据现有 part 目录重建。不要手写合并后的 `parts.json`，否则容易因内部格式变化或目录冲突导致恢复失败。

```go
// Merger 数据合并器（支持本地目录合并）
type Merger struct {
    config *config.Config
    logger *zap.Logger
}

// MergeLocalDirs 合并多个本地目录（用于 S3 拉取场景）
func (m *Merger) MergeLocalDirs(ctx context.Context, srcDir string, partition string) (string, error) {
    start := time.Now()
    m.logger.Info("Starting local merge",
        zap.String("src_dir", srcDir),
        zap.String("partition", partition))

    // 创建合并后的目标目录
    mergedDir := filepath.Join(os.TempDir(), fmt.Sprintf("vlog-merged-%s-%d", partition, time.Now().Unix()))
    datadbDir := filepath.Join(mergedDir, "datadb")

    if err := os.MkdirAll(datadbDir, 0755); err != nil {
        return "", err
    }

    // 1. 查找所有节点目录
    nodeDirs, err := m.findNodeDirectories(srcDir)
    if err != nil {
        os.RemoveAll(mergedDir)
        return "", fmt.Errorf("failed to find node directories: %w", err)
    }

    if len(nodeDirs) == 0 {
        os.RemoveAll(mergedDir)
        return "", fmt.Errorf("no node directories found in %s", srcDir)
    }

    m.logger.Info("Found node directories",
        zap.Int("count", len(nodeDirs)),
        zap.Strings("nodes", nodeDirs))

    // 2. 并发复制所有 Stream 数据
    var wg sync.WaitGroup
    errChan := make(chan error, len(nodeDirs))

    for _, nodeDir := range nodeDirs {
        wg.Add(1)
        go func(dir string) {
            defer wg.Done()

            // 复制节点的 datadb 到目标
            srcDatadb := filepath.Join(srcDir, dir, "datadb")
            if _, err := os.Stat(srcDatadb); os.IsNotExist(err) {
                errChan <- fmt.Errorf("datadb not found in %s", dir)
                return
            }

            if err := m.copyStreamData(srcDatadb, datadbDir); err != nil {
                errChan <- fmt.Errorf("failed to copy from %s: %w", dir, err)
                return
            }

            m.logger.Info("Copied stream data",
                zap.String("node", dir),
                zap.String("src", srcDatadb),
                zap.String("dst", datadbDir))
        }(nodeDir)
    }

    wg.Wait()
    close(errChan)

    // 检查错误
    var firstErr error
    for err := range errChan {
        if firstErr == nil {
            firstErr = err
        }
    }

    if firstErr != nil {
        os.RemoveAll(mergedDir)
        return "", firstErr
    }

    // 3. 合并 parts.json
    if err := m.mergePartsJSON(datadbDir); err != nil {
        os.RemoveAll(mergedDir)
        return "", fmt.Errorf("failed to merge parts.json: %w", err)
    }

    duration := time.Since(start)
    m.logger.Info("Local merge completed",
        zap.String("merged_dir", mergedDir),
        zap.Duration("duration", duration))

    return mergedDir, nil
}

// findNodeDirectories 查找所有节点目录
func (m *Merger) findNodeDirectories(srcDir string) ([]string, error) {
    entries, err := os.ReadDir(srcDir)
    if err != nil {
        return nil, err
    }

    var nodeDirs []string
    for _, entry := range entries {
        if entry.IsDir() {
            nodeDirs = append(nodeDirs, entry.Name())
        }
    }

    return nodeDirs, nil
}

// copyStreamData 复制 Stream 数据
func (m *Merger) copyStreamData(srcDatadb, dstDatadb string) error {
    // 遍历源目录中的所有 Stream 目录
    entries, err := os.ReadDir(srcDatadb)
    if err != nil {
        return err
    }

    for _, entry := range entries {
        if !entry.IsDir() {
            continue
        }

        streamID := entry.Name()
        srcStreamPath := filepath.Join(srcDatadb, streamID)
        dstStreamPath := filepath.Join(dstDatadb, streamID)

        // 如果目标已存在同名 Stream，直接跳过（Stream ID 唯一）
        if _, err := os.Stat(dstStreamPath); err == nil {
            m.logger.Debug("Stream already exists, skipping",
                zap.String("stream_id", streamID))
            continue
        }

        // 复制整个 Stream 目录
        if err := os.Rename(srcStreamPath, dstStreamPath); err != nil {
            // 如果 rename 失败（可能是跨设备），使用 copy
            if err := m.copyDirectory(srcStreamPath, dstStreamPath); err != nil {
                return err
            }
        }
    }

    return nil
}

// copyDirectory 递归复制目录（使用 Go 原生 SDK）
func (m *Merger) copyDirectory(src, dst string) error {
    return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }

        // 计算目标路径
        relPath, err := filepath.Rel(src, path)
        if err != nil {
            return err
        }
        dstPath := filepath.Join(dst, relPath)

        // 如果是目录，创建目标目录
        if info.IsDir() {
            return os.MkdirAll(dstPath, info.Mode())
        }

        // 如果是文件，复制文件
        return m.copyFile(path, dstPath, info.Mode())
    })
}

// copyFile 复制单个文件
func (m *Merger) copyFile(src, dst string, mode os.FileMode) error {
    // 打开源文件
    srcFile, err := os.Open(src)
    if err != nil {
        return err
    }
    defer srcFile.Close()

    // 创建目标文件
    dstFile, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
    if err != nil {
        return err
    }
    defer dstFile.Close()

    // 复制内容
    _, err = io.Copy(dstFile, srcFile)
    return err
}

// mergePartsJSON 合并多个 parts.json
func (m *Merger) mergePartsJSON(baseDir string) error {
    datadbDir := filepath.Join(baseDir, "datadb")

    // 查找所有 parts.json
    var partsFiles []string
    err := filepath.Walk(datadbDir, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        if info.Name() == "parts.json" {
            partsFiles = append(partsFiles, path)
        }
        return nil
    })

    if err != nil {
        return err
    }

    if len(partsFiles) == 0 {
        return fmt.Errorf("no parts.json found")
    }

    // 合并
    allParts := make(map[string]interface{})
    sourceNodes := make(map[string]bool)

    for _, partsFile := range partsFiles {
        data, err := os.ReadFile(partsFile)
        if err != nil {
            return err
        }

        var partsData map[string]interface{}
        if err := json.Unmarshal(data, &partsData); err != nil {
            return err
        }

        // 合并 Parts
        if parts, ok := partsData["Parts"].([]interface{}); ok {
            for _, part := range parts {
                if partMap, ok := part.(map[string]interface{}); ok {
                    partID := partMap["PartID"]
                    if partID != nil {
                        allParts[fmt.Sprintf("%v", partID)] = part
                    }
                }
            }
        }

        // 记录来源节点
        sourceNodes[filepath.Dir(partsFile)] = true
    }

    // 生成合并后的 parts.json
    merged := map[string]interface{}{
        "Parts": make([]interface{}, 0, len(allParts)),
        "Metadata": map[string]interface{}{
            "SourceNodes":      len(sourceNodes),
            "TotalParts":       len(allParts),
            "MergedAt":         time.Now().UTC().Format(time.RFC3339),
            "MergedBy":         "vlog-tools",
        },
    }

    for _, part := range allParts {
        merged["Parts"] = append(merged["Parts"].([]interface{}), part)
    }

    data, err := json.MarshalIndent(merged, "", "  ")
    if err != nil {
        return err
    }

    return os.WriteFile(filepath.Join(datadbDir, "parts.json"), data, 0644)
}
```

### 5. 拉取服务 (internal/pull)

```go
// Puller 拉取服务（支持按需合并）
type Puller struct {
    storage  storage.Storage
    cache    *cache.Manager
    merger   *merge.Merger
    vlClient *api.Client
    config   *config.PullConfig
    logger   *zap.Logger
}

// PullPartition 拉取并合并分区（按需合并）
func (p *Puller) PullPartition(ctx context.Context, partition string) (*PullResult, error) {
    start := time.Now()

    // 1. 检查缓存
    if p.cache.Has(partition) {
        p.logger.Info("Partition cached",
            zap.String("partition", partition))
        return &PullResult{
            Partition: partition,
            Cached:    true,
        }, nil
    }

    p.logger.Info("Pulling partition",
        zap.String("partition", partition),
        zap.String("mode", "merge-on-demand"))

    // 2. 查询元数据，找到哪些节点有这个分区
    partitionMap, err := p.storage.LoadPartitionMap(ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to load partition map: %w", err)
    }

    var sourceNodes []string
    for _, pInfo := range partitionMap.Partitions {
        if pInfo.Name == partition {
            sourceNodes = pInfo.Nodes
            break
        }
    }

    if len(sourceNodes) == 0 {
        return nil, fmt.Errorf("no nodes found with partition: %s", partition)
    }

    p.logger.Info("Found partition in nodes",
        zap.String("partition", partition),
        zap.Strings("nodes", sourceNodes))

    // 3. 从 S3 并发下载（节点独立目录）
    tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("vlog-pull-%s-%d", partition, time.Now().Unix()))
    if err := os.MkdirAll(tempDir, 0755); err != nil {
        return nil, err
    }
    defer os.RemoveAll(tempDir)

    var wg sync.WaitGroup
    errChan := make(chan error, len(sourceNodes))

    for _, nodeName := range sourceNodes {
        wg.Add(1)
        go func(node string) {
            defer wg.Done()

            // S3 路径: s3://vlog-archive/nodes/{node-name}/{partition}/
            s3Path := fmt.Sprintf("nodes/%s/%s", node, partition)
            localPath := filepath.Join(tempDir, node)

            p.logger.Info("Downloading from S3",
                zap.String("node", node),
                zap.String("s3_path", s3Path))

            downloadResult, err := p.storage.DownloadPartition(ctx, s3Path, localPath)
            if err != nil {
                p.logger.Error("Failed to download from node",
                    zap.String("node", node),
                    zap.Error(err))
                errChan <- err
                return
            }

            p.logger.Info("Download completed",
                zap.String("node", node),
                zap.Int64("bytes", downloadResult.SizeBytes))
        }(nodeName)
    }

    wg.Wait()
    close(errChan)

    // 检查下载错误
    var downloadErrors []error
    for err := range errChan {
        downloadErrors = append(downloadErrors, err)
    }

    if len(downloadErrors) > 0 {
        // 至少一个节点成功即可继续（容错）
        if len(downloadErrors) == len(sourceNodes) {
            return nil, fmt.Errorf("all nodes failed to download: %v", downloadErrors)
        }
        p.logger.Warn("Some nodes failed, continuing with available data",
            zap.Int("failed", len(downloadErrors)),
            zap.Int("total", len(sourceNodes)))
    }

    // 4. 物理合并多个节点的数据
    p.logger.Info("Merging data from multiple nodes",
        zap.String("partition", partition),
        zap.Int("sources", len(sourceNodes)))

    mergedDir, err := p.merger.MergeLocalDirs(ctx, tempDir, partition)
    if err != nil {
        return nil, fmt.Errorf("merge failed: %w", err)
    }

    // 5. 验证合并后的数据
    if err := p.verifyPartition(ctx, mergedDir); err != nil {
        os.RemoveAll(mergedDir)
        return nil, fmt.Errorf("verification failed: %w", err)
    }

    // 6. 移动到最终位置
    finalPath := filepath.Join(p.config.LocalDataPath, "partitions", partition)
    if err := os.Rename(mergedDir, finalPath); err != nil {
        // 如果 rename 失败，尝试 copy
        if err := p.copyDir(mergedDir, finalPath); err != nil {
            return nil, fmt.Errorf("failed to move merged data: %w", err)
        }
    }

    // 7. 计算合并后的大小
    sizeBytes, err := p.dirSize(finalPath)
    if err != nil {
        p.logger.Warn("Failed to calculate size", zap.Error(err))
        sizeBytes = 0
    }

    // 8. 添加到缓存
    p.cache.Add(partition, sizeBytes)

    // 9. 通知 VictoriaLogs 重新加载
    if err := p.vlClient.ReloadPartition(ctx, partition); err != nil {
        p.logger.Warn("Failed to reload partition",
            zap.String("partition", partition),
            zap.Error(err))
    }

    duration := time.Since(start)
    p.logger.Info("Pull and merge completed",
        zap.String("partition", partition),
        zap.Duration("duration", duration),
        zap.Int64("bytes", sizeBytes),
        zap.Int("source_nodes", len(sourceNodes)))

    return &PullResult{
        Partition:  partition,
        SizeBytes:  sizeBytes,
        Duration:   duration,
        Cached:     false,
        SourceNodes: sourceNodes,
    }, nil
}

// PullByTimeRange 按时间范围拉取分区
func (p *Puller) PullByTimeRange(ctx context.Context, start, end time.Time) ([]*PullResult, error) {
    partitions := p.calculatePartitions(start, end)

    var results []*PullResult
    for _, partition := range partitions {
        result, err := p.PullPartition(ctx, partition)
        if err != nil {
            p.logger.Error("Failed to pull partition",
                zap.String("partition", partition),
                zap.Error(err))
            continue
        }
        results = append(results, result)
    }

    return results, nil
}

func (p *Puller) copyDir(src, dst string) error {
    return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }

        // 计算目标路径
        relPath, err := filepath.Rel(src, path)
        if err != nil {
            return err
        }
        dstPath := filepath.Join(dst, relPath)

        // 如果是目录，创建目标目录
        if info.IsDir() {
            return os.MkdirAll(dstPath, info.Mode())
        }

        // 如果是文件，复制文件
        return p.copyFile(path, dstPath, info.Mode())
    })
}

func (p *Puller) copyFile(src, dst string, mode os.FileMode) error {
    srcFile, err := os.Open(src)
    if err != nil {
        return err
    }
    defer srcFile.Close()

    dstFile, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
    if err != nil {
        return err
    }
    defer dstFile.Close()

    _, err = io.Copy(dstFile, srcFile)
    return err
}

func (p *Puller) dirSize(path string) (int64, error) {
    var size int64
    err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        if !info.IsDir() {
            size += info.Size()
        }
        return nil
    })
    return size, err
}
```

### 6. 缓存管理 (internal/cache)

```go
// Manager 缓存管理器
type Manager struct {
    dataPath   string
    maxSizeGB  int64
    metrics    *metrics.Registry
    logger     *zap.Logger
    mu         sync.RWMutex
}

type CacheEntry struct {
    Partition  string
    SizeBytes  int64
    LastAccess time.Time
    CreatedAt  time.Time
    Checksum   string
}

// Add 添加到缓存
func (m *Manager) Add(partition string, sizeBytes int64) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    // 检查缓存大小
    currentSize := m.getCurrentSize()
    maxSize := m.maxSizeGB * 1024 * 1024 * 1024

    if currentSize + sizeBytes > maxSize {
        // LRU 淘汰
        if err := m.evictLRU(sizeBytes); err != nil {
            return err
        }
    }

    entry := &CacheEntry{
        Partition:  partition,
        SizeBytes:  sizeBytes,
        LastAccess: time.Now(),
        CreatedAt:  time.Now(),
    }

    return m.saveEntry(entry)
}

// Has 检查分区是否已缓存
func (m *Manager) Has(partition string) bool {
    m.mu.RLock()
    defer m.mu.RUnlock()

    entry, err := m.getEntry(partition)
    if err != nil {
        return false
    }

    // 验证文件完整性
    if !m.verifyEntry(entry) {
        return false
    }

    // 更新访问时间
    entry.LastAccess = time.Now()
    m.saveEntry(entry)

    return true
}

// evictLRU LRU 淘汰
func (m *Manager) evictLRU(requiredBytes int64) error {
    entries := m.listEntries()

    // 按访问时间排序
    sort.Slice(entries, func(i, j int) bool {
        return entries[i].LastAccess.Before(entries[j].LastAccess)
    })

    freedBytes := int64(0)
    for _, entry := range entries {
        if freedBytes >= requiredBytes {
            break
        }

        m.logger.Info("Evicting partition",
            zap.String("partition", entry.Partition),
            zap.Int64("bytes", entry.SizeBytes))

        partitionPath := filepath.Join(m.dataPath, "partitions", entry.Partition)
        if err := os.RemoveAll(partitionPath); err != nil {
            return err
        }

        m.deleteEntry(entry.Partition)
        freedBytes += entry.SizeBytes
    }

    return nil
}

// Cleanup 清理旧分区
func (m *Manager) Cleanup(retentionDays int) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    entries := m.listEntries()
    cutoff := time.Now().AddDate(0, 0, -retentionDays)

    for _, entry := range entries {
        if entry.LastAccess.Before(cutoff) {
            partitionPath := filepath.Join(m.dataPath, "partitions", entry.Partition)

            m.logger.Info("Cleaning up old partition",
                zap.String("partition", entry.Partition),
                zap.Time("last_access", entry.LastAccess))

            if err := os.RemoveAll(partitionPath); err != nil {
                m.logger.Error("Failed to cleanup",
                    zap.String("partition", entry.Partition),
                    zap.Error(err))
                continue
            }

            m.deleteEntry(entry.Partition)
        }
    }

    return nil
}
```

### 7. 查询引擎 (internal/query)

```go
// Engine 查询引擎
type Engine struct {
    puller    *pull.Puller
    cache     *cache.Manager
    vlClient  *api.Client
    config    *config.QueryConfig
    logger    *zap.Logger
}

// Query 执行查询
func (e *Engine) Query(ctx context.Context, query QueryRequest) (*QueryResult, error) {
    start := time.Now()

    // 1. 解析查询，识别需要的分区
    requiredPartitions := e.identifyPartitions(query.TimeRange)

    e.logger.Info("Query requires partitions",
        zap.Strings("partitions", requiredPartitions))

    // 2. 确保所有分区都已缓存
    for _, partition := range requiredPartitions {
        if !e.cache.Has(partition) {
            e.logger.Info("Pulling missing partition",
                zap.String("partition", partition))

            if _, err := e.puller.PullPartition(ctx, partition); err != nil {
                return nil, fmt.Errorf("failed to pull partition %s: %w", partition, err)
            }
        }
    }

    // 3. 执行查询
    result, err := e.vlClient.Query(ctx, query)
    if err != nil {
        return nil, err
    }

    duration := time.Since(start)
    e.logger.Info("Query completed",
        zap.Duration("duration", duration),
        zap.Int("records", len(result.Records)))

    return result, nil
}

// identifyPartitions 识别查询需要的分区
func (e *Engine) identifyPartitions(timeRange TimeRange) []string {
    var partitions []string
    current := timeRange.Start

    for current.Before(timeRange.End) {
        partition := current.Format("20060102")
        partitions = append(partitions, partition)
        current = current.AddDate(0, 0, 1)  // 按天分区
    }

    return unique(partitions)
}
```

### 8. 元数据管理 (internal/metadata)

```go
// Manager 元数据管理器
type Manager struct {
    storage storage.Storage
    cache   *cache.Manager
    logger  *zap.Logger
}

// PartitionMap 分区映射表
type PartitionMap struct {
    Version    int              `json:"version"`
    Partitions []PartitionInfo  `json:"partitions"`
    GeneratedAt time.Time       `json:"generated_at"`
}

// PartitionInfo 分区信息
type PartitionInfo struct {
    Name        string    `json:"name"`
    Nodes       []string  `json:"nodes"`
    SizeBytes   int64     `json:"size_bytes"`
    FileCount   int       `json:"file_count"`
    ModifiedAt  time.Time `json:"modified_at"`
    Checksum    string    `json:"checksum"`
}

// UpdatePartitionMap 更新分区映射表
func (m *Manager) UpdatePartitionMap(ctx context.Context, partition string, nodes []string) error {
    // 加载现有的分区映射表
    partitionMap, err := m.LoadPartitionMap(ctx)
    if err != nil {
        partitionMap = &PartitionMap{
            Version: 1,
            Partitions: []PartitionInfo{},
        }
    }

    // 查找并更新分区信息
    found := false
    for i, p := range partitionMap.Partitions {
        if p.Name == partition {
            partitionMap.Partitions[i].Nodes = nodes
            partitionMap.Partitions[i].ModifiedAt = time.Now().UTC()
            found = true
            break
        }
    }

    if !found {
        // 添加新分区
        partitionMap.Partitions = append(partitionMap.Partitions, PartitionInfo{
            Name:       partition,
            Nodes:      nodes,
            ModifiedAt: time.Now().UTC(),
        })
    }

    partitionMap.GeneratedAt = time.Now().UTC()

    // 保存到 S3
    return m.savePartitionMap(ctx, partitionMap)
}

// LoadPartitionMap 加载分区映射表
func (m *Manager) LoadPartitionMap(ctx context.Context) (*PartitionMap, error) {
    data, err := m.storage.GetMetadata(ctx, "partition-map.json")
    if err != nil {
        return nil, err
    }

    var partitionMap PartitionMap
    if err := json.Unmarshal(data, &partitionMap); err != nil {
        return nil, err
    }

    return &partitionMap, nil
}
```

---

## 🔧 CLI 工具设计

### 命令结构

```bash
vlog-tools <command> [subcommand] [flags]

Commands:
  archive    归档分区到 S3
    partition    归档指定分区
    list          列出已归档的分区
    status        查看归档状态

  migrate    迁移分区（热节点 → 冷节点）
    partition     迁移指定分区
    list          列出可迁移的分区
    validate      验证迁移配置

  pull       从 S3 拉取分区
    partition     拉取指定分区
    range         按时间范围拉取
    prefetch      预取常用分区

  query      查询优化
    execute       执行查询
    explain       查询计划
    analyze       分析查询

  cache      缓存管理
    list          列出已缓存分区
    cleanup       清理旧分区
    stats         缓存统计

  serve      启动服务
    archive       归档服务
    puller        拉取服务
    query-proxy   查询代理

  version    查看版本信息
```

### 使用示例

```bash
# 归档今天的分区
vlog-tools archive partition 20260408 \
  --config config.yaml

# 拉取指定分区
vlog-tools pull partition 20260407 \
  --config config.yaml

# 按时间范围拉取
vlog-tools pull range \
  --start 2026-04-01 \
  --end 2026-04-30 \
  --config config.yaml

# 查看缓存状态
vlog-tools cache stats \
  --config config.yaml

# 清理旧分区
vlog-tools cache cleanup \
  --retention-days 180 \
  --config config.yaml

# 启动归档服务
vlog-tools serve archive \
  --config config-archive.yaml

# 启动拉取服务
vlog-tools serve puller \
  --config config-puller.yaml
```

---

## 📊 监控与可观测性

### Prometheus 指标

```go
var (
    // 归档指标
    archiveDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "vlog_archive_duration_seconds",
            Help: "Archive duration in seconds",
        },
        []string{"partition", "status"},
    )

    archiveBytes = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "vlog_archive_bytes_total",
            Help: "Total bytes archived",
        },
        []string{"partition"},
    )

    // 拉取指标
    pullDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "vlog_pull_duration_seconds",
            Help: "Pull duration in seconds",
        },
        []string{"partition", "cached"},
    )

    // 缓存指标
    cacheSizeBytes = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Name: "vlog_cache_size_bytes",
            Help: "Current cache size in bytes",
        },
    )

    cacheHitRate = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Name: "vlog_cache_hit_rate",
            Help: "Cache hit rate",
        },
    )

    cacheEvictions = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "vlog_cache_evictions_total",
            Help: "Total cache evictions",
        },
        []string{"reason"},
    )

    // 查询指标
    queryDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "vlog_query_duration_seconds",
            Help: "Query duration in seconds",
        },
        []string{"cached"},
    )
)
```

### 日志格式

```json
{
  "level": "info",
  "ts": "2026-04-08T10:30:00.123Z",
  "caller": "archive/service.go:45",
  "msg": "Archive completed",
  "partition": "20260408",
  "duration": "5m23s",
  "bytes": 8589934592,
  "source_nodes": ["node-1", "node-2", "node-3"]
}
```

---

## 🚀 部署方案

### 方案概述

本工具只需输出 Kubernetes YAML 文件，无需使用 k8s SDK。通过以下方式访问热节点数据：

**方案一：共享 PVC（推荐）**
- 归档服务挂载热节点的 PVC（只读）
- 适合：热节点和归档服务在同一 k8s 集群

**方案二：NFS/对象存储**
- 热节点数据通过 NFS 或对象存储共享
- 归档服务直接访问共享存储

**方案三：本地文件系统**
- 适合：非 k8s 环境，或本地开发测试

### Kubernetes CronJob (归档服务)

**挂载热节点 PVC 的方式：**

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: vlog-archive
  namespace: monitoring
spec:
  schedule: "0 2 * * *"  # 每天凌晨 2 点执行
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: vlog-tools  # 无需特殊权限
          containers:
          - name: archiver
            image: your-registry/vlog-tools:latest
            command: ["vlog-tools", "archive", "partition"]
            args:
            - "$(PARTITION)"
            env:
            - name: PARTITION
              value: "$(date +%Y%m%d)"
            volumeMounts:
            # 挂载热节点数据（PVC 共享）
            - name: hot-node-0-data
              mountPath: /data/vlstorage-node-0  # 挂载到子路径
              readOnly: true
            - name: hot-node-1-data
              mountPath: /data/vlstorage-node-1
              readOnly: true
            - name: hot-node-2-data
              mountPath: /data/vlstorage-node-2
              readOnly: true
            # 临时目录（用于 rclone 缓存）
            - name: tmp
              mountPath: /tmp
          volumes:
          # 方式一：使用热节点的 PVC（只读）
          - name: hot-node-0-data
            persistentVolumeClaim:
              claimName: vlstorage-data-vlstorage-0  # 热节点 PVC 名称
              readOnly: true
          - name: hot-node-1-data
            persistentVolumeClaim:
              claimName: vlstorage-data-vlstorage-1
              readOnly: true
          - name: hot-node-2-data
            persistentVolumeClaim:
              claimName: vlstorage-data-vlstorage-2
              readOnly: true
          # 临时存储
          - name: tmp
            emptyDir:
              sizeLimit: 5Gi
          restartPolicy: OnFailure
```

**对应的配置文件 (config.yaml)：**

```yaml
hot_nodes:
  - name: node-0
    local_data_path: /data/vlstorage-node-0  # 对应 volumeMounts 的挂载点
  - name: node-1
    local_data_path: /data/vlstorage-node-1
  - name: node-2
    local_data_path: /data/vlstorage-node-2

s3:
  endpoint: s3.amazonaws.com
  bucket: vlog-archive
  region: us-east-1
```

### Deployment (拉取服务)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vlog-puller
  namespace: monitoring
spec:
  replicas: 1
  selector:
    matchLabels:
      app: vlog-puller
  template:
    spec:
      containers:
      - name: puller
        image: your-registry/vlog-tools:latest
        command: ["vlog-tools", "serve", "puller"]
        ports:
        - containerPort: 9090  # Metrics endpoint
        env:
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: aws-credentials
              key: access-key
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: aws-credentials
              key: secret-key
        volumeMounts:
        # 配置文件
        - name: config
          mountPath: /etc/vlog-tools
          readOnly: true
        # 冷数据存储（VictoriaLogs 数据目录）
        - name: data
          mountPath: /var/lib/victorialogs
        # 临时缓存（下载和合并使用）
        - name: cache
          mountPath: /var/cache/vlog-tools
      volumes:
      - name: config
        configMap:
          name: vlog-tools-config
      # 冷数据 PVC（VictoriaLogs 持久化数据）
      - name: data
        persistentVolumeClaim:
          claimName: vlog-cold-data
      # 临时缓存
      - name: cache
        emptyDir:
          sizeLimit: 50Gi  # 根据需要调整
```

### RBAC（无需特殊权限）

由于不需要使用 k8s SDK，只需基本的 ServiceAccount：

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: vlog-tools
  namespace: monitoring
```

**注意**：无需创建 ClusterRole、RoleBinding 等 RBAC 资源。

---

## 🔄 关键架构决策：按需合并 vs 归档时合并

### ❌ 旧架构（已废弃）：归档时合并

```
Hot Node-1 ──┐
Hot Node-2 ──┼──► [物理合并] ──► S3 ──► [长期保存]
Hot Node-3 ──┘
```

**问题**：
- ❌ 每次归档都需要合并，消耗大量 CPU 和内存
- ❌ 合并时间随数据量增长而增加
- ❌ 需要维护临时存储空间
- ❌ S3 存储合并后的数据，无法追溯节点来源
- ❌ 单点故障：如果合并失败，整个归档任务失败

### ✅ 新架构（推荐）：按需合并

```
【归档流程 - 快速上传】
Hot Node-1 ──► S3/nodes/node-1/20260407/ ──┐
Hot Node-2 ──► S3/nodes/node-2/20260407/ ──┼──► [按节点分离保存]
Hot Node-3 ──► S3/nodes/node-3/20260407/ ──┘

【拉取流程 - 按需合并】
User Query 需要 20260407 数据
    ↓
从 S3 拉取:
  ├─ S3/nodes/node-1/20260407/ ──┐
  ├─ S3/nodes/node-2/20260407/ ──┼──► [物理合并] ──► Cold Node
  └─ S3/nodes/node-3/20260407/ ──┘
```

**优势**：
- ✅ 归档快速：每个节点独立上传，并发完成
- ✅ 资源节省：无需在归档时消耗 CPU/内存进行合并
- ✅ 灵活性强：保留原始节点数据，便于调试和去重
- ✅ 容错性好：单个节点失败不影响其他节点
- ✅ 按需加载：只合并和缓存查询需要的数据
- ✅ 成本优化：冷节点可配置本地缓存大小，LRU 自动淘汰

### 📊 性能对比

| 指标 | 旧架构（归档时合并） | 新架构（按需合并） |
|------|---------------------|-------------------|
| **归档时间** | ~30-60 分钟（合并3个节点） | ~10-15 分钟（并发上传） |
| **归档 CPU** | 高（合并计算） | 低（仅网络 I/O） |
| **归档内存** | 高（临时目录） | 低（流式上传） |
| **首次查询** | < 1 秒（已合并） | 30秒-5分钟（需要合并） |
| **后续查询** | < 1 秒 | < 1 秒（本地缓存） |
| **存储成本** | 相同 | 相同 |
| **网络流量** | 低（上传一次） | 中（下载多次，但可缓存） |

### 🎯 使用场景

**新架构（按需合并）适合**：
- ✅ 查询模式不固定（无法预测哪些分区需要）
- ✅ 冷节点存储有限（需要 LRU 缓存管理）
- ✅ 归档频率高（每天执行）
- ✅ 成本敏感（S3 + 本地缓存 vs 全量本地存储）
- ✅ 可接受首次查询延迟（30秒-5分钟）

**旧架构（归档时合并）适合**：
- ✅ 查询模式固定（大多数历史数据都需要频繁查询）
- ✅ 冷节点存储充足（可以缓存所有历史数据）
- ✅ 对首次查询延迟敏感（必须 < 1秒）
- ✅ 归档频率低（每周或每月执行）

### 💡 实现建议

根据当前设计，**新架构（按需合并）是默认推荐方案**，因为：
1. 保留了原始节点数据，便于调试和追溯
2. 归档流程简单快速，资源消耗低
3. 支持灵活的缓存策略（LRU、预取、定期清理）
4. 成本最优：S3 长期存储 + 本地按需缓存

**下一步**：查看[执行计划](./execution-plan-v2.md)了解如何实施。

---

## ✅ 总结

这个设计方案整合了所有需求：

1. **统一工具套件** - 归档、迁移、拉取、查询、缓存
2. **模块化设计** - 清晰的职责划分
3. **生产级质量** - 完整的监控、日志、错误处理
4. **灵活部署** - CLI 工具 + Kubernetes 服务
5. **高可用** - 并发处理、重试机制、数据校验

**下一步**：查看[执行计划](./execution-plan-v2.md)了解如何实施。
