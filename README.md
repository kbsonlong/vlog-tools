# vlog-tools

VictoriaLogs 数据生命周期管理工具，用于高效归档、拉取、缓存、查询 VictoriaLogs 数据。

## 核心特性
- **独立归档**：各个热节点并行独立上传分区到 S3，归档过程极快且节省 CPU/内存资源。
- **官方快照备份**：归档前调用 VictoriaLogs partition snapshot API，只从稳定快照目录上传，避免直接复制活跃分区导致文件消失。
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

### 单次归档

有两种方式执行单次归档：

**1. 指定分区归档**（适合临时/手动场景）：

```bash
./bin/vlog-tools archive partition 20260408 --config configs/config.yaml
```

**1.1 批量补历史分区**（适合历史回填）：

```bash
./bin/vlog-tools archive range \
  --start 2026-05-01 \
  --end 2026-06-15 \
  --config configs/config.yaml
```

接受 `YYYYMMDD` 和 `YYYY-MM-DD` 两种格式。范围按 UTC 日期逐天包含式执行；如果希望遇错继续补跑剩余分区，可以追加 `--continue-on-error`。

**2. sidecar/CronJob 模式单次执行**（自动计算分区日期，适合调试或一次性补跑）：

```bash
./bin/vlog-tools serve archive \
  --once \
  --offset-days=1 \
  --partition-timezone=UTC \
  --node-url=http://127.0.0.1:9428 \
  --source-path=/var/lib/victorialogs \
  --config configs/config.yaml
```

`--once` 会让归档循环只执行一次即退出，分区日期根据 `--offset-days` 和 `--partition-timezone` 自动计算（默认为 1，即指定时区的昨天）。VictoriaLogs 分区通常按 UTC 日期命名；如果集群运行在 Asia/Shanghai，凌晨 01:00 仍处于前一个 UTC 日期，不能归档这个仍在写入的分区。

归档流程遵循 VictoriaLogs 官方建议：

1. 调用 `http://<vlstorage>:9428/internal/partition/snapshot/create?partition_prefix=YYYYMMDD` 创建分区快照。
2. 从接口返回的 snapshot 目录上传到 S3。
3. 调用 `/internal/partition/snapshot/delete?path=<snapshot-path>` 删除快照。

因此归档容器必须能访问 vlstorage HTTP 管理接口，并且能读取 vlstorage 的 `-storageDataPath` 卷。sidecar 是推荐部署方式；如果启用了 `-partitionManageAuthKey`，在配置中设置 `archive.partition_auth_key`。

### 拉取

```bash
./bin/vlog-tools pull partition 20260408 --config configs/config.yaml
```

拉取恢复会从 S3 的 `nodes/{node}/{partition}` 下载所有节点副本，按 part 目录合并到冷节点的 `<storageDataPath>/partitions/YYYYMMDD/datadb/`。如果不同节点出现同名 part 目录，工具会生成新的 16 位十六进制 part 目录名后保留两份数据；目标 `datadb/parts.json` 会被删除，由 VictoriaLogs 冷启动或 `/internal/partition/attach?name=YYYYMMDD` 时按磁盘上的 part 目录重建。

## Sidecar 模式

把 `vlog-tools` 作为 sidecar 与 VLStorage 容器运行在同一个 Pod 内，`vlog-tools` 只归档本节点数据（无需配置 hot_nodes 列表），并按照策略定时从挂载的数据目录同步到 S3。

- 示例片段: [statefulset-sidecar-archive-snippet.yaml](file:///Users/zengshenglong/Code/GoWorkSpace/vlog-backup/vlog-tools/deployments/kubernetes/statefulset-sidecar-archive-snippet.yaml)
- CronJob 场景: [cronjob-archive.yaml](file:///Users/zengshenglong/Code/GoWorkSpace/vlog-backup/vlog-tools/deployments/kubernetes/cronjob-archive.yaml)
- sidecar 历史补备配置模板: [config-sidecar-history.yaml](file:///Users/zengshenglong/Code/GoWorkSpace/vlog-backup/vlog-tools/configs/config-sidecar-history.yaml)

`archive.node_name/node_url/source_data_path` 在 sidecar 模式下指向“当前 Pod 的这个 vlstorage 节点”。如果设置了 `POD_NAME` 环境变量，程序会自动把它作为节点名；`hot_nodes` 也可以留空，程序会自动用 `POD_NAME + archive.node_url + archive.source_data_path` 推导当前节点。如果是 `vlstorage-0`、`vlstorage-1`、`vlstorage-2` 三个副本，就分别在三个 sidecar 里各自执行补历史；不要试图用一个 sidecar 替整个 StatefulSet 回填所有历史数据。

历史补备推荐直接执行：

```bash
./bin/vlog-tools archive range \
  --start 20260501 \
  --end 20260615 \
  --config /config/rclone.conf
```

如果希望跳过失败分区继续跑：

```bash
./bin/vlog-tools archive range \
  --start 20260501 \
  --end 20260615 \
  --continue-on-error \
  --config /config/rclone.conf
```

## 用 cron 管理 sidecar 归档

更直观的方式是使用 Go 的 cron 库，通过 cron 表达式控制归档时间点（避免 `--every=24h` 因重启导致的重复归档抖动）。同时每个分区归档成功后会在 S3 写入 `_SUCCESS` 标记，重复触发会自动跳过。

示例参数（Asia/Shanghai 每天 09:00 归档昨天 UTC 分区）：

```bash
vlog-tools serve archive \
  --cron="0 9 * * *" \
  --offset-days=1 \
  --partition-timezone=UTC \
  --node-url=http://127.0.0.1:9428 \
  --source-path=/var/lib/victorialogs \
  --config=/etc/vlog-tools/config.yaml
```
