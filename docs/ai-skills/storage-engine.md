# 存储引擎

## 引擎分层

```
CN 侧: DisttaE (分布式事务引擎)
         ↕
TN 侧: TAE (本地存储引擎)
         ↕
底层:   FileService (对象存储抽象)
```

## DisttaE（分布式 TAE）

- **路径：** `pkg/vm/engine/disttae/`
- **角色：** CN 侧使用的分布式引擎，负责跨节点事务读写
- **核心类型：**
  - `Engine` — 分布式引擎实例
  - `txnDatabase` — 事务级数据库视图
  - `txnTable` / `txnTableDelegate` — 事务级表操作
- **特性：** MVCC 快照隔离，支持乐观/悲观事务

## TAE（Transaction Aware Engine）

- **路径：** `pkg/vm/engine/tae/`
- **角色：** TN 侧的本地存储引擎
- **特性：**
  - 块状列存格式
  - Checkpoint / Flush 管理
  - Logtail 增量变更捕获

## ObjectIO（对象存储模型）

- **路径：** `pkg/objectio/`
- **核心类型：**
  - `BlockInfo` — 存储块元数据
  - `ObjectLocation` — 对象存储位置引用
  - `BackupObject` — 备份对象元数据
- **说明：** 通过 FileService 执行实际的对象 I/O

## Engine 接口

- **路径：** `pkg/vm/engine/types.go`
- **抽象：** Engine → Database → Relation(Table) → Reader
- **可插拔：** 支持 DisttaE、MemoryEngine (测试用) 等实现

## 与测试的关联

| 变更范围 | 影响的测试 |
|---------|----------|
| DisttaE 事务逻辑 | BVT: disttae, pessimistic_transaction, optimistic |
| TAE flush/checkpoint | 稳定性: tpcc/sysbench 长时间写入 |
| ObjectIO 读写 | BVT: load_data; 大数据量测试 |
| Logtail 变更捕获 | PITR, Snapshot, CDC |
