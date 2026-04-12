# TAE Checkpoint 模块深度解析

## 概述

Checkpoint（检查点）是 MatrixOne TAE（Transactional Analytical Engine）存储引擎中的核心机制，用于将内存中的数据持久化到对象存储，并支持系统崩溃后的快速恢复。

## 什么是 Checkpoint？

Checkpoint 是数据库系统中的一种持久化机制，它定期将内存中的脏数据（dirty data）刷新到持久存储中。在 TAE 中，Checkpoint 主要完成以下任务：

1. **数据持久化**：将内存中的修改持久化到对象存储
2. **WAL 截断**：允许截断已持久化的 WAL（Write-Ahead Log）日志
3. **快速恢复**：系统重启时从 Checkpoint 恢复，而非重放所有 WAL
4. **存储空间回收**：配合 GC 机制回收过期数据

## Checkpoint 的类型

TAE 支持四种类型的 Checkpoint：

```go
const (
    ET_Global      EntryType = iota  // 全局检查点
    ET_Incremental                    // 增量检查点
    ET_Backup                         // 备份检查点
    ET_Compacted                      // 压缩检查点
)
```

### 1. 增量检查点（Incremental Checkpoint）

- 记录自上次检查点以来的所有变更
- 执行频率高，开销小
- 时间范围：`[start_ts, end_ts]`

### 2. 全局检查点（Global Checkpoint）

- 包含系统在某个时间点的完整快照
- 执行频率低，开销大
- 时间范围：`[0, end_ts]`
- 用于加速恢复和支持 GC

### 3. 备份检查点（Backup Checkpoint）

- 用于数据备份场景
- 特殊处理 LSN 信息

### 4. 压缩检查点（Compacted Checkpoint）

- 多个检查点合并后的结果
- 用于减少元数据文件数量

## 核心组件

```
┌─────────────────────────────────────────────────────────────┐
│                     Checkpoint Runner                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Flusher   │  │  Executor   │  │   RunnerStore       │  │
│  │  (刷盘器)    │  │  (执行器)    │  │   (存储管理)         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│         │                │                    │              │
│         ▼                ▼                    ▼              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  Collector  │  │  ICKP/GCKP  │  │  CheckpointEntry    │  │
│  │  (收集器)    │  │   Exec      │  │   (检查点条目)       │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
              ┌─────────────────────────┐
              │    Object Storage       │
              │    (对象存储)            │
              └─────────────────────────┘
```

## 文档目录

本文档系列包含以下章节：

1. [架构设计](./01_architecture.md) - Checkpoint 整体架构
2. [核心数据结构](./02_data_structures.md) - Entry、Store 等核心结构
3. [Runner 详解](./03_runner.md) - Checkpoint Runner 工作机制
4. [Executor 执行器](./04_executor.md) - 检查点执行逻辑
5. [增量检查点](./05_incremental_checkpoint.md) - ICKP 实现细节
6. [全局检查点](./06_global_checkpoint.md) - GCKP 实现细节
7. [Flusher 刷盘机制](./07_flusher.md) - 脏数据刷盘
8. [Collector 收集器](./08_collector.md) - 脏数据收集
9. [数据格式与版本](./09_data_format.md) - Checkpoint 数据格式
10. [恢复流程](./10_replay.md) - 系统恢复机制
11. [快照与备份](./11_snapshot.md) - 快照和备份支持
12. [配置与调优](./12_configuration.md) - 参数配置指南

## 关键代码位置

- **主要代码**：`pkg/vm/engine/tae/db/checkpoint/`
- **Logtail 相关**：`pkg/vm/engine/tae/logtail/`
- **数据工具**：`pkg/vm/engine/ckputil/`
