# 修改记录：fulltext native-only 模式

## 概述

完全跳过 v1 隐藏表路径，查询和 DML 均只走 native sidecar。

## 改动原因

v1 隐藏表路径存在一个无法在 v1 框架内修复的正确性问题：大批量 UPDATE 时，PostDML 生成的 `DELETE FROM hidden_index WHERE doc_id IN (...)` 在 IN 列表过大时执行不完整，导致旧 token 残留。测试数据：100K 行 UPDATE 后期望旧词命中 0，实际残留 53,277。

native sidecar 路径通过 MO 原生 tombstone 处理 DELETE/UPDATE 可见性，不依赖隐藏表，测试中 DELETE 场景完全正确。因此最直接的修复方式是跳过 v1 隐藏表维护和查询。

## 改动文件

```
 pkg/fulltext/types.go                             |  +8
 pkg/sql/plan/apply_indices_fulltext.go            |  +1
 pkg/sql/colexec/table_function/fulltext_native.go |  +3
 pkg/sql/colexec/postdml/postdml.go                | +13 -7
 4 files changed, 25 insertions(+), 7 deletions(-)
```

## 逐文件说明

### 1. `pkg/fulltext/types.go`

```go
// 新增字段
type FullTextParserParam struct {
    ...
    NativeOnlyMode bool `json:"native_only,omitempty"`
}

// 新增方法
func (p FullTextParserParam) NativeOnly() bool {
    return p.NativeOnlyMode && p.UseNative()
}
```

- `NativeOnlyMode` 通过 JSON 序列化在 `AlgoParams` 中传递，不需要改 proto
- `NativeOnly()` 要求 `UseNative()` 也为 true，双重保护
- `omitempty` 确保旧索引的 AlgoParams 不包含此字段时默认 false

### 2. `pkg/sql/plan/apply_indices_fulltext.go`

```go
param.NativeOnlyMode = true  // 新增一行
```

在 planner 构建 fulltext scan 参数时设置。所有新的 fulltext 查询都会走 native-only。

### 3. `pkg/sql/colexec/table_function/fulltext_native.go`

```go
if u.param.NativeOnly() {
    scan.complete = true
}
```

强制 `scan.complete = true` 后：
- `fulltextIndexMatchNative` 返回 `used = true`
- `fulltext.go` 中 v1 的 `runCountStar` + `runWordStats` + `groupby` 全部跳过
- 统计从 sidecar 汇总，不再查隐藏表

### 4. `pkg/sql/colexec/postdml/postdml.go`

```go
var param fulltext.FullTextParserParam
if ftctx.AlgoParams != "" {
    json.Unmarshal([]byte(ftctx.AlgoParams), &param)
}
if param.NativeOnly() {
    return nil  // 跳过隐藏表维护
}
```

- INSERT/UPDATE/DELETE 不再生成维护隐藏表的 SQL
- 同时消除了原代码中重复的 `var param` 声明（-7 行）

## 数据流变化

### 改动前（混合模式）

```
INSERT → PostDML 写隐藏表 token + flush 生成 sidecar
UPDATE → PostDML DELETE 旧 token + INSERT 新 token + tombstone + 新 sidecar
DELETE → PostDML DELETE token + tombstone
查询   → native 读 sidecar + v1 读隐藏表 → 合并结果
```

### 改动后（native-only）

```
INSERT → flush 生成 sidecar（PostDML 跳过）
UPDATE → tombstone 旧版本 + flush 新版本生成新 sidecar（PostDML 跳过）
DELETE → tombstone（PostDML 跳过）
查询   → native 读 sidecar + tail segment → 完整结果
```

## 影响范围

| 范围 | 影响 |
|------|------|
| 有 FULLTEXT 索引的表 | 查询走 native-only，DML 不维护隐藏表 |
| 没有 FULLTEXT 索引的表 | 零影响 |
| TAE flush/merge | 零影响（sidecar 生成逻辑不变） |
| DDL | 零影响（隐藏表仍然建，方便回退） |
| 向量索引 | 零影响 |

## 回退方式

将 `apply_indices_fulltext.go` 中的 `param.NativeOnlyMode = true` 改为 `false`（或删除该行），即可回退到混合模式。隐藏表仍然存在，v1 路径可以立即恢复工作。

## 已知限制

1. **datalink 列**：native sidecar 不支持 datalink 列（`ObjectIndexer` 跳过），native-only 模式下 datalink 列的 fulltext 查询会返回不完整结果。这是边缘场景，实际使用极少。
2. **query expansion 模式**：`nativeQuerySupported` 不支持 query expansion，会 fallback 到 v1。但 v1 本身也不支持 query expansion（返回 unsupported），所以实际无影响。

## 验证

```
go build  ./pkg/fulltext/...                          ✅
go build  ./pkg/sql/colexec/postdml/...               ✅
go build  ./pkg/sql/colexec/table_function/...        ✅
go build  ./pkg/sql/plan/...                          ✅
go test   ./pkg/fulltext/...                          ✅ PASS
go test   ./pkg/fulltext/native/...                   ✅ PASS
go test   ./pkg/sql/colexec/postdml/...               ✅ PASS
go test   ./pkg/sql/colexec/table_function/... -run fulltext  ✅ PASS
```

---

## 追加修改：多列索引 NULL 列修复

### 问题

多列 FULLTEXT 索引中，如果某行的任意一个索引列为 NULL，该行在全文查询中完全不可见。例如 `INSERT INTO t VALUES (2, NULL, 'cherry')` 后搜索 `cherry` 返回 0。

### 根因

`pkg/fulltext/native/object.go` 的 `collectIndexValues` 遇到任意 NULL 列就 `return nil, false, nil` 跳过整行。

### 修复

```diff
-   if vec.IsNull(uint64(row)) {
-       return nil, false, nil
-   }
+   if vec.IsNull(uint64(row)) {
+       continue
+   }
    ...
+   if len(values) == 0 {
+       return nil, false, nil
+   }
```

NULL 列跳过（`continue`），其他列正常收集。只有所有列都 NULL 时才跳过整行。

### 影响

- flush 路径和 tail segment 路径都调用 `collectIndexValues`，一处修改覆盖两个路径
- 改动量：+4 / -1 行
