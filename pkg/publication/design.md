cross-cluster physical subscription

**mo_ccpr_log**
```
CREATE TABLE mo_catalog.mo_ccpr_log (
    -- 任务标识
    task_id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    subscription_name    VARCHAR(5000) NOT NULL,
    
    -- 复制级别和范围
    sync_level           VARCHAR(16) NOT NULL,           -- 'database', 'table'
    db_name              VARCHAR(5000),                   -- database/table级别必填
    table_name           VARCHAR(5000),                   -- table级别必填
    
    -- 上游连接配置
    upstream_conn         VARCHAR(5000) NOT NULL,          -- MySQL连接字符串
    
    -- 复制配置（JSON格式）
    sync_config          JSON NOT NULL,                  -- {sync_interval}
    
    -- 任务控制
    state                TINYINT,  -- 'running', 'stopped'
    
    -- 执行状态
    iteration_state      TINYINT NOT NULL DEFAULT 0,  -- 'pending', 'running', 'complete', 'error', 'cancel'
    iteration_lsn        BIGINT DEFAULT 0,               -- Job序列号
    context              JSON,                           -- iteration上下文，如snapshot名称等
    cn_uuid              VARCHAR(64),                    -- 执行任务的CN标识
    
    -- 错误信息
    error_message        VARCHAR(5000),                  -- 错误信息
    
    -- 时间戳
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
```

**兼容性说明**
- 系统表 `mo_ccpr_log` 已添加到系统初始化流程中
- 在 `catalog/types.go` 中定义了常量 `MO_CCPR_LOG = "mo_ccpr_log"`
- 在 `predefined.go` 中定义了 DDL: `MoCatalogMoCcprLogDDL`
- 在 `authenticate.go` 中注册到 `createSqls`、`sysWantedTables` 和 `predefinedTables`
- 新部署的集群会自动创建该表
- 对于已存在的集群，系统会在下次初始化时自动创建该表（如果不存在）

**sql**
* snapshot diff 
 objectlist [database d] [table t] snapshot sp2 [against snapshot sp1]
返回值:db name, table name, object list
for table,
get table,
get snapshot ts(from,to)
scan partition state

* create database/table from cluster(show subscribe的结果,检查上游是否发布)

* get snapshot ts

* drop database/table(删除mo_sync_configs)

* get object

**subscribe**
update mo_sync_configs

**sql builder**
create snapshot
query mo databases
query mo tables
query mo columns

**iteration**
* 0. 初始化阶段
  - 检查iteration状态
  - new txn(engine, client)txn: 创建本地事务，用于操作本地表
  - lock table(本地的表): 锁定本地目标表，防止并发修改冲突
  - upstream executor开启事务: 在上游集群开启事务，用于执行查询操作

* 1. 获取上游元数据和DDL
  - 1.1 请求上游snapshot
    - 通过upstream executor向上游发送: CREATE SNAPSHOT sp1 FOR TABLE db1 t1 (或 FOR DATABASE db1)
    - 传入参数: table info/db info (用于确定snapshot范围)
    - 返回: snapshot名称

    1.1.2 请求上游的snapshot ts，用新建的snapshot来取ts
  
  - 1.2 查询上游三表获取DDL
    - 通过upstream executor查询上游: mo_catalog.mo_databases, mo_catalog.mo_tables, mo_catalog.mo_columns
    - 检查：1. 是否行数内容不对 2. id是否不对
    - 生成DDL变更语句(create table/alter table等)

* 2. 计算snapshot diff获取object list
  - 在上游执行: OBJECTLIST DATABASE db1 TABLE t1 SNAPSHOT sp2 AGAINST SNAPSHOT sp1
  - 返回: db name, table name, object list (包含stats, create_at, delete_at, is_tombstone)
  - 下游snapshot diff: 如果下游也有snapshot，计算下游的object list用于对比和去重
  - 结果按CN分片: 每个CN处理自己负责的partition的object list

* 3. 获取object数据
  - 遍历object list中的每个object
  - 通过upstream executor执行: GETOBJECT object_name
  - 从上游fileservice复制object文件到本地fileservice
  - 验证object完整性(checksum等)

* 4. 写入和过滤object
  - write(filter) object: 将object写入本地fileservice
  - aobj排序: 按object的create_at时间戳排序，保证应用顺序
  - 删除ts abort: 过滤掉abort事务产生的object(is_tombstone=true且delete_at在abort范围内)
  - truncate: 处理truncate操作(可能需要清空表或特殊标记)

* 5. TN apply object
  - 在TN节点应用object(需要覆盖旧值，即upsert语义)
  - 更新partition state中的object列表
  - 更新table metadata

* 6. 清理阶段
  - drop snapshot: 在上游执行 DROP SNAPSHOT sp1, DROP SNAPSHOT sp2 (清理临时snapshot)
  - unlock table(本地的表): 释放表锁
  - upstream executor结束事务: 提交或回滚上游事务

* 7. 更新系统表
  - 在ccpr表里更新iteration上下文，更新iteration_state，iteration_lsn, context, error_message

**iteration 函数签名和参数**

```go
func ExecuteIteration(
    ctx context.Context,
    cnUUID string,
    cnEngine engine.Engine,
    cnTxnClient client.TxnClient,
    taskID uint64,
    localFS fileservice.FileService,  // 本地文件服务，用于存储object
) error
```

**IterationContext 结构体**
```go
type IterationContext struct {
    // 任务标识
    taskID           uint64           // 对应 mo_ccpr_log.task_id
    subscriptionName string           // 订阅名称
    syncLevel        string           // 'database' 或 'table'
    dbName           string           // 数据库名（database/table级别必填）
    tableName        string           // 表名（table级别必填）
    accountID        uint32           // 账户ID
    tableID          uint64           // 表ID
    
    // 上游连接配置
    upstreamExecutor     UpstreamExecutor           // 上游连接字符串
    
    // 复制配置
    syncConfig       map[string]any  // 同步配置，如 sync_interval 等
    
    // 执行状态
    iterationLSN     uint64           // 当前iteration的LSN
    cnUUID           string           // 执行任务的CN标识
    
    // 上下文信息
    prevSnapshotName string
    prevSnapshotTS types.TS
    currentSnapshotName string
    currentSnapshotTS types.TS
    activeAObj         []ObjectStats   // 之前的object信息，用于去重和对比
    tableIDs map[string]uint64
}
```

**Sinker 接口**
```go
type UpstreamExecutor interface {
    StartTxn(ctx context.Context) error
    ExecSQL(ctx context.Context, sql string) (*Result, error)
    EndTxn(ctx context.Context, commit bool) error
}
```

**sinker**
start txn
send sql
end txn

**init executor**

**executor**
* apply system table
  
* check state and gen iteration

**snapshot meta diff** ?
collect change scan object

**get object**
复制文件

**优化alter不删表？**

**检查权限**

**检查get object list的snapshot是否全部刷盘了**

**object 分包**

**索引表**

**兼容性**