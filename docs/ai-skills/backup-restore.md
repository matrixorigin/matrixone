# 备份恢复（PITR / Snapshot）

## 概述

MO 支持两种备份恢复机制：
- **PITR (Point-In-Time Recovery)** — 恢复到任意时间点
- **Snapshot** — 恢复到指定快照

## PITR

### 原理
- 基于 Logtail 的增量日志回放
- 记录事务提交时间戳，恢复时回放到指定时间点
- 依赖 TN 的 checkpoint + WAL

### 关键路径
- `pkg/backup/` — 备份配置与元数据
- `pkg/frontend/` — 前端 PITR SQL 命令处理
- `pkg/vm/engine/tae/` — 存储层 checkpoint 管理
- `pkg/logservice/` — WAL 日志持久化

### SQL 语法
```sql
CREATE PITR pitr_name FOR ACCOUNT account_name RANGE value unit;
ALTER PITR pitr_name ...;
DROP PITR pitr_name;
RESTORE ACCOUNT account_name FROM PITR pitr_name TIMESTAMP '2024-01-01 00:00:00';
```

## Snapshot

### 原理
- 创建数据库/表的一致性快照
- 基于 MVCC 时间戳实现
- 快照元数据存储在系统表中

### SQL 语法
```sql
CREATE SNAPSHOT snapshot_name FOR ACCOUNT account_name;
RESTORE ACCOUNT account_name FROM SNAPSHOT snapshot_name;
DROP SNAPSHOT snapshot_name;
```

## Backup 包（`pkg/backup/`）

- `BackupType` — 备份类型
- `BackupTs` — 备份时间戳
- `BackupObject` (objectio) — 备份对象元数据

## 与测试的关联

| 变更范围 | 影响的测试 |
|---------|----------|
| PITR 核心逻辑 | PITR 测试; BVT: pitr |
| Snapshot 核心逻辑 | Snapshot 测试; BVT: snapshot |
| Logtail/Checkpoint | PITR + Snapshot 都受影响 |
| 备份元数据 | BVT: pitr, snapshot |
| 多租户备份 | BVT: tenant; PITR/Snapshot 多租户场景 |
