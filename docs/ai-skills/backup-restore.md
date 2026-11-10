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

## 部分恢复的主体身份边界

数据库/表的 Snapshot 和 PITR 部分恢复保留历史创建者与所有者的身份。角色改名不会改变身份；删除后重建用户或角色、重用旧名称，不会继承历史对象的所有权。所需主体已不存在时，恢复在删除当前对象之前失败。表级恢复保留已有数据库的当前所有权。

全量账户恢复会重建主体目录，并可能回拨普通主体的自增 ID。因此，从目录重建之前的快照或 PITR 时间点部分恢复普通主体拥有的对象会被拒绝，即使全量恢复正确重建了该主体，或当前主体的 ID、名称与历史值相同。错误为 `principal catalog was rebuilt; identity continuity cannot be established`，当前对象及数据保留。可使用当前目录代际内的新快照/时间点，或按需要进行全量恢复。

固定身份受保护的内置角色和初始管理员有经过校验的跨代际例外；其他管理员不享有该例外。部分恢复保留当前对象授权，不复活已撤销的历史授权；全量/跨账户恢复继续使用其主体目录重建流程。

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
