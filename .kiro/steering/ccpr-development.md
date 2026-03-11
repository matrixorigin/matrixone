---
inclusion: manual
---

# CCPR (Cross-Cluster Publication Replication) 开发笔记

## 测试文件位置

CCPR 相关的单元测试分布在以下位置：
- `pkg/frontend/publication_subscription_test.go` - 前端订阅管理测试
- `pkg/sql/parsers/tree/drop_test.go` - SQL 解析器测试
- `pkg/vm/engine/disttae/ccpr_txn_cache_test.go` - 事务缓存测试
- `pkg/vm/engine/test/publication_test.go` - 核心发布/迭代测试
- `pkg/vm/engine/test/apply_objects_test.go` - 对象应用测试

## 运行 CCPR 测试的命令

```bash
LD_LIBRARY_PATH=$(pwd)/lib:$(pwd)/thirdparties/install/lib \
CGO_CFLAGS="-I$(pwd)/thirdparties/install/include" \
CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib" \
go test -v -run "Ccpr|CCPR" ./pkg/...
```

## 接口实现注意事项

`publication.SQLExecutor` 接口包含以下方法，mock 实现时需要全部实现：
- `Close() error`
- `Connect() error`
- `EndTxn(ctx context.Context, commit bool) error`
- `ExecSQL(ctx, ar, accountID, query, useTxn, needRetry, timeout) (*Result, CancelFunc, error)`
- `ExecSQLInDatabase(ctx, ar, accountID, query, database, useTxn, needRetry, timeout) (*Result, CancelFunc, error)`

## 未实现的功能

`ExecuteIteration` 函数中的以下参数目前未使用，预留给未来的同步保护功能：
- `syncProtectionWorker`
- `syncProtectionRetryOpt`

相关代码位置：`pkg/publication/iteration.go` 第 1097-1118 行

`TestCCPRSyncProtectionRetry` 测试已被 skip，等待功能实现后启用。
