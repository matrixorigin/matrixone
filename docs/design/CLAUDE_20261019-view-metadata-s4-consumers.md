# S4：View 元数据消费者边界与迁移说明

本文记录 S1/S2 已批准契约在 S4 中的接线，不构成默认上线批准。设计依据为 [S1](CLAUDE_20260922-view-metadata-on-demand.md)、[S1 批准记录](CLAUDE_20260928-view-metadata-s1-review.md)及 [S2 v2.1 修订](CLAUDE_20261005-view-s2-scope-revision.md)。

## 激活及所有权

默认 SQL 不创建新请求，不新增 session variable、持久协议或全局开关。已有兼容实现本身就支持按需元数据；S4 增加的是统一 S2 请求、预算及生命周期边界，而非声称旧实现全部读取陈旧列。

显式接线的语句调用者使用 `TxnCompilerContext.NewViewMetadataRequest(ctx, authorize)`，将 `request.Context()` 传递到规划和 origin-CN 执行。调用者必须提供对应元数据入口的授权策略，且保持相同事务、快照、执行代和角色域。上下文标记不是权限凭据，不能跳过每个根的授权，也不向远端序列化。

- 语句调用者拥有 request；消费者清理后调用 `Close`，不把请求存入 session、计划缓存或全局对象。
- S2 拥有隔离 binder、memo、累计工作量及首次描述开始的共同截止时间。并行分支串行进入 binder，但各自持有独立结果。
- `ViewSchemaResult` 由消费者 `Release`；TVF 在完成、错误、提前终止、Reset、Free 时释放当前列描述。
- request 只借用外层事务，不负责关闭父事务。取消等待的分支不取得正在工作的 binder 的所有权。
- 派生 TVF 节点不可缓存；SHOW 沿用现有强制重绑定标记，避免没有函数版本证据的 VALUES 描述跨执行复用。

## 消费者清单

| 消费者 | 分类及处理 |
| --- | --- |
| DESC、SHOW COLUMNS/FULL | 逻辑当前列：opt-in 时从共同请求取得完整描述，沿用原展示规则 |
| SHOW COLUMN_NUMBER、SHOW TABLE_VALUES 的列选择 | 逻辑当前列：复用同一描述入口；TABLE_VALUES 的显式数据查询语义不变 |
| 当前/受支持历史 `information_schema.columns`，本地与订阅 View | 逻辑当前列：可信系统模板中的私有 TVF 接入共同请求；表、系统 View 固定列路径不变 |
| `frontend.buildTableDefFromMoColumns` 的 LIMIT 0 快径 | 现有实现已经只返回 View kind，不使用持久列作为当前结果；普通查询展开属于 S3，不在此重复迁移 |
| 普通 SELECT、CTAS、PREPARE 的 View 查询展开 | S3 所有；本次不代替其绑定和执行代接线 |
| `frontend.resolveTableVisibleColumns` / ANALYZE | 物理统计消费者；`bindAnalyzeTables` 明确拒绝 View 和非自有物理表，不扩展支持范围 |
| raw catalog、恢复/升级所需持久目录 | 物理兼容语义，保留 MVCC 快照，不写回推导结果 |
| COM_FIELD_LIST、DUMP、SHOW CREATE | 不扩功能；DUMP 的 View 拒绝以及无效 View 的 SHOW CREATE 能力保持 |

## 关系及安全语义

仅适配可识别的系统 COLUMNS 模板，不猜测改写用户 View。私有 TVF 继续要求可信系统 View lineage；三参数签名为调用者可见的库名、对象名和候选 relation ID。执行时复核 S2 捕获的根身份，订阅授权使用 subscriber 名称，定义绑定仍保留 publisher 身份及快照。

候选筛选、行列可见性、JOIN、聚合、ORDER BY、残余列谓词和 LIMIT 仍由已有关系计划执行。不使用持久 View 列作为行骨架，也不为节省推导提前截断候选。

单对象描述失败即报错。批量扫描仅在 binder 阶段确证缺库、表或列时跳过整张 View，并沿用既有 warning。授权、provider 初始化、根查找、取消、协议、预算及内部错误保持 fatal；相同的 missing-object 错误码不能单独证明可跳过。S2 的原有公开 `Describe` 仍返回原错误身份和错误码。

## 有界输出

请求按候选逐个准入，不另建大候选数组（受控候选数为 1，低于设计上限 32）。普通查询上游批次继续受现有执行器约束。

opt-in TVF 每页最多 1024 行，目标有效载荷 1 MiB；较大但合法的单行独占一页，超过 16 MiB 的行拒绝。APPLY 保留这种源批次边界，不能仅按行数把多页大行重新聚合成超大输出。默认旧签名保留原单批行为。这里约束的是受控描述/输出结构，依照 S2 v2.1，不承诺整条 parser/binder/执行器调用链的精确 RSS 上限。

## raw catalog 与逻辑元数据的区别

例如 `v2 -> v1 -> t`，只修改 `t` 的 VARCHAR 宽度：两个 View 自身版本可以不变。需要当前可见列的客户端应使用逻辑入口：

```sql
SHOW COLUMNS FROM db.v2;
SELECT column_name, column_type, column_default, is_nullable
FROM information_schema.columns
WHERE table_schema = 'db' AND table_name = 'v2'
ORDER BY ordinal_position;
```

以下查询仍是创建/修改时持久列的物理快照，不承诺反映依赖 DDL 后的逻辑描述：

```sql
SELECT attname, mo_show_visible_bin(atttyp, 3)
FROM mo_catalog.mo_columns
WHERE account_id = current_account_id()
  AND att_database = 'db' AND att_relname = 'v2'
ORDER BY attnum;
```

合法历史查询使用其历史定义及依赖快照，不借当前列伪造历史。描述读取不持久刷新 `mo_columns`，不排队修复，也不执行 View 数据查询。
