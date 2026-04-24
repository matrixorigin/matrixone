# FileService

## 概述

FileService 是 MO 的对象存储抽象层，屏蔽底层存储差异（本地磁盘 / S3 / MinIO）。

## 核心接口（`pkg/fileservice/`）

| 接口 | 职责 |
|------|------|
| `FileService` | 基础读写操作 |
| `MutableFileService` | 追加/删除操作 |
| `ReaderWriterFileService` | 流式 I/O |
| `CacheDataAllocator` | 内存管理 |
| `FileCache` | 本地缓存层 |
| `ETLFileService` | ETL 专用操作 |

## 存储后端

- **本地文件系统** — 开发/测试环境
- **S3 / MinIO** — 生产环境对象存储
- **HTTP** — 远程文件访问

## 配置

```toml
# etc/launch/tn.toml
[fileservice.s3]
bucket = "my-bucket"
key-prefix = "mo-data"
endpoint = "http://minio:9000"
```

本地存储模式：数据存储在 `mo-data/` 目录下。

## 与测试的关联

| 变更范围 | 影响的测试 |
|---------|----------|
| 读写逻辑 | BVT: load_data, stage |
| 缓存层 | 稳定性: tpch（大量读取）|
| S3 对接 | 大数据量测试（云端 load）|
| ETL 功能 | BVT: load_data |
