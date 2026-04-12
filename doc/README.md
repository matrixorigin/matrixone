# TAE Checkpoint 模块文档

欢迎阅读 MatrixOne TAE Checkpoint 模块的深度解析文档。本文档系列详细介绍了 Checkpoint 的设计原理、实现细节和使用方法。

## 文档目录

| 章节 | 标题 | 描述 |
|------|------|------|
| 00 | [概述](./00_overview.md) | Checkpoint 模块简介和整体概念 |
| 01 | [架构设计](./01_architecture.md) | 整体架构、组件交互和状态机 |
| 02 | [核心数据结构](./02_data_structures.md) | Entry、Store、Config 等核心结构 |
| 03 | [Runner 详解](./03_runner.md) | Checkpoint Runner 工作机制 |
| 04 | [Executor 执行器](./04_executor.md) | 检查点执行逻辑和 Job 管理 |
| 05 | [增量检查点](./05_incremental_checkpoint.md) | ICKP 触发条件、执行流程和数据收集 |
| 06 | [全局检查点](./06_global_checkpoint.md) | GCKP 触发条件、执行流程和数据收集 |
| 07 | [Flusher 刷盘机制](./07_flusher.md) | 脏数据刷盘策略和实现 |
| 08 | [Collector 收集器](./08_collector.md) | 脏数据收集和管理 |
| 09 | [数据格式与版本](./09_data_format.md) | Checkpoint 数据格式和版本演进 |
| 10 | [恢复流程](./10_replay.md) | 系统恢复机制和 Replayer |
| 11 | [快照与备份](./11_snapshot.md) | 快照和备份功能支持 |
| 12 | [配置与调优](./12_configuration.md) | 参数配置和性能调优指南 |

## 快速导航

### 入门阅读

如果你是第一次了解 Checkpoint 模块，建议按以下顺序阅读：

1. [概述](./00_overview.md) - 了解基本概念
2. [架构设计](./01_architecture.md) - 理解整体架构
3. [核心数据结构](./02_data_structures.md) - 熟悉关键数据结构

### 深入理解

如果你想深入理解实现细节：

1. [Runner 详解](./03_runner.md) - 核心组件
2. [Executor 执行器](./04_executor.md) - 执行逻辑
3. [增量检查点](./05_incremental_checkpoint.md) - ICKP 实现
4. [全局检查点](./06_global_checkpoint.md) - GCKP 实现

### 运维相关

如果你关注运维和调优：

1. [配置与调优](./12_configuration.md) - 配置参数
2. [恢复流程](./10_replay.md) - 故障恢复
3. [快照与备份](./11_snapshot.md) - 数据保护

## 代码位置

- **主要代码**：`pkg/vm/engine/tae/db/checkpoint/`
- **Logtail 相关**：`pkg/vm/engine/tae/logtail/`
- **数据工具**：`pkg/vm/engine/ckputil/`

## 核心概念速查

### Checkpoint 类型

| 类型 | 代码 | 说明 |
|------|------|------|
| 全局检查点 | `ET_Global` | 完整快照，时间范围 [0, end] |
| 增量检查点 | `ET_Incremental` | 增量变更，时间范围 [start, end] |
| 备份检查点 | `ET_Backup` | 用于备份 |
| 压缩检查点 | `ET_Compacted` | 合并后的检查点 |

### 状态

| 状态 | 代码 | 说明 |
|------|------|------|
| 等待中 | `ST_Pending` | 等待执行 |
| 运行中 | `ST_Running` | 正在执行 |
| 已完成 | `ST_Finished` | 执行完成 |

### 默认配置

| 参数 | 默认值 |
|------|--------|
| 增量检查点间隔 | 1 分钟 |
| 最小事务数 | 10000 |
| 全局检查点阈值 | 40 个增量检查点 |
| 刷盘间隔 | 1 分钟 |
| 定时任务周期 | 5 秒 |

## 版本信息

- 当前 Checkpoint 版本：`CheckpointVersion13`
- 协议版本：`CKPProtocolVersion_V2`

## 问题分析与优化

如果你想了解当前 Checkpoint 模块存在的问题和优化方向，请参阅 [分析文档](./analysis/README.md)：

| 文档 | 描述 |
|------|------|
| [代码质量问题](./analysis/01_code_issues.md) | 代码复杂度、错误处理、魔法数字等 |
| [架构设计问题](./analysis/02_architecture_issues.md) | 模块耦合、接口设计、状态管理等 |
| [并发与性能问题](./analysis/03_concurrency_performance.md) | 锁竞争、CAS 循环、性能瓶颈等 |
| [可靠性问题](./analysis/04_reliability_issues.md) | 崩溃恢复、数据一致性、容错等 |
| [优化建议](./analysis/05_optimization_suggestions.md) | 综合优化方案和实施路线图 |

## 贡献

如果你发现文档中的错误或有改进建议，欢迎提交 Issue 或 PR。
