# vlog-tools

VictoriaLogs 数据生命周期管理工具，用于高效归档、拉取、缓存、查询 VictoriaLogs 数据。

## 核心特性
- **独立归档**：各个热节点并行独立上传分区到 S3，归档过程极快且节省 CPU/内存资源。
- **按需拉取**：查询时仅从 S3 拉取所需分区并进行本地物理合并。
- **本地缓存**：内置 LRU 缓存管理，确保热数据高效读取，节约冷节点存储空间。
- **无状态部署**：全原生 Go 编写，无需依赖 Kubernetes SDK。

## 构建
```bash
make build
```

## 测试环境启动
```bash
docker-compose up -d
```

## CLI 示例

```bash
# 归档
./bin/vlog-tools archive partition 20260408 --config configs/config.yaml

# 拉取
./bin/vlog-tools pull partition 20260408 --config configs/config.yaml
```

## Sidecar 模式

把 `vlog-tools` 作为 sidecar 与 VLStorage 容器运行在同一个 Pod 内，`vlog-tools` 只归档本节点数据（无需配置 hot_nodes 列表），并按照策略定时从挂载的数据目录同步到 S3。

- 示例片段: [statefulset-sidecar-archive-snippet.yaml](file:///Users/zengshenglong/Code/GoWorkSpace/vlog-backup/vlog-tools/deployments/kubernetes/statefulset-sidecar-archive-snippet.yaml)
- CronJob 场景: [cronjob-archive.yaml](file:///Users/zengshenglong/Code/GoWorkSpace/vlog-backup/vlog-tools/deployments/kubernetes/cronjob-archive.yaml)
