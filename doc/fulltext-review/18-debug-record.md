# Native FTS Debug 记录

## 1. 问题现象

所有 fulltext 查询都走 v1 fallback，native sidecar 路径从未被命中。表现为：
- UPDATE 100K 行后旧词残留（v1 PostDML 的已知问题）
- NULL 列修复不生效
- native-only 开关不生效

## 2. Debug 过程

### 2.1 第一轮日志：发现 mode=0 未被识别

加日志位置：`fulltext_native.go` 的 `fulltextIndexMatchNative` 入口

```
[FTS-DEBUG] nativeQuerySupported=false, mode=0
[FTS-DEBUG] native path NOT used, falling back to v1
```

**根因**：`nativeQuerySupported` 的 switch 只处理了 `FULLTEXT_NL`（=1）和 `FULLTEXT_BOOLEAN`（=3），没有处理 `FULLTEXT_DEFAULT`（=0）。用户执行 `AGAINST('xxx')` 不带模式时 mode=0。

**修复**：在 switch 中加入 `FULLTEXT_DEFAULT`：
```go
case int64(tree.FULLTEXT_DEFAULT), int64(tree.FULLTEXT_NL):
```

同时修复 `fulltextIndexMatchNative` 中的 mode 判断：
```go
if s.Mode == int64(tree.FULLTEXT_DEFAULT) || s.Mode == int64(tree.FULLTEXT_NL) {
```

### 2.2 第二轮日志：发现 prepareNativeScan 返回 nil

修复 mode=0 后重新部署，日志显示：

```
[FTS-DEBUG] prepareNativeScan returned nil or err, UseNative=true, NativeOnly=true
[FTS-DEBUG] native path NOT used, falling back to v1
```

`UseNative=true, NativeOnly=true` 说明开关生效了，但 `prepareNativeScan` 返回了 nil。

### 2.3 第三轮日志：定位到 objects=0

在 `prepareNativeScan` 内部多个 return nil 点加日志后：

```
[FTS-DEBUG] prepareNativeScan: objects=0, stats=1, visible_remaining=0, incomplete=true, tailSeg=false
```

**分析**：
- `stats=1`：有 1 个 non-appendable object（数据已 flush）
- `visible_remaining=0`：object 在 visible 列表中（被 `delete(visible, nameStr)` 消费了）
- `objects=0`：但没有任何 object 加入 objects 列表
- `incomplete=true`：被标记为不完整
- `tailSeg=false`：没有 tail segment

**推导**：object 在 visible 中找到了，但 `ReadSidecar` 返回 `exists=false`（sidecar 文件不存在），所以 object 没加入列表，`incomplete=true`。

### 2.4 根因：sidecar 从未生成

sidecar 在 flush 时由 `flushobj.go` 生成，关键路径：

```go
indexer, idxErr := ftnative.NewObjectIndexer(task.meta.GetSchema())
if !indexer.Empty() {
    // 生成 sidecar
}
```

`NewObjectIndexer` 调用 `ExtractIndexDefinitions`，后者从 `schema.Constraint` 中提取 FULLTEXT 索引定义：

```go
func ExtractIndexDefinitions(schema *catalog.Schema) ([]IndexDefinition, error) {
    if len(schema.Constraint) == 0 {
        return nil, nil  // ← 这里直接返回了
    }
    // ...
}
```

**根因**：TAE flush 时 `schema.Constraint` 为空（或不包含 FULLTEXT 索引信息），导致 `ExtractIndexDefinitions` 返回空，`indexer.Empty()` 为 true，sidecar 生成被跳过。

**这意味着 native sidecar 在当前部署上从未真正生成过。** 之前所有测试中"正确"的结果都是 v1 fallback 路径返回的。

## 3. 当前加的日志位置

| 文件 | 位置 | 日志内容 |
|------|------|---------|
| `fulltext_native.go:83` | `nativeQuerySupported` 返回 false | mode 值 |
| `fulltext_native.go:89` | `prepareNativeScan` 返回 nil | UseNative, NativeOnly |
| `fulltext_native.go:91` | `prepareNativeScan` 正常返回 | objects 数量, complete, totalDocs |
| `fulltext_native.go:152` | `prepareNativeScan` Parts 为空 | — |
| `fulltext_native.go:172` | `prepareNativeScan` hasDatalinkPart | — |
| `fulltext_native.go:245` | `prepareNativeScan` objects=0 | stats, visible, incomplete, tailSeg |
| `fulltext.go:648` | native 路径被使用 | — |
| `fulltext.go:650` | native 路径未使用，fallback v1 | — |
| `fulltext.go:652` | UseNative()=false | Implementation 值 |
| `postdml.go:126` | NativeOnly=true 跳过 | — |
| `postdml.go:129` | NativeOnly=false 走 v1 | Implementation, NativeOnlyMode |

## 4. 待解决

**核心问题**：TAE 的 `catalog.Schema.Constraint` 在 flush 时没有包含 FULLTEXT 索引信息，导致 sidecar 从未生成。

需要排查：
1. `CREATE FULLTEXT INDEX` 后，FULLTEXT 索引元数据写入了 catalog 的哪个位置？
2. TAE flush 时 `task.meta.GetSchema()` 返回的 schema 是否包含了最新的 constraint 信息？
3. 是否存在 schema 版本不一致的问题（DDL 更新了 catalog 但 flush 时读到的是旧版本）？

## 5. 已完成的代码修改汇总

| 文件 | 改动 | 状态 |
|------|------|------|
| `pkg/fulltext/types.go` | NativeOnlyMode 字段 + NativeOnly() | ✅ 已提交 |
| `pkg/sql/plan/apply_indices_fulltext.go` | NativeOnlyMode = true | ✅ 已提交 |
| `pkg/sql/colexec/table_function/fulltext_native.go` | native-only 强制 complete + FULLTEXT_DEFAULT 修复 + debug 日志 | ✅ 代码在本地 |
| `pkg/sql/colexec/postdml/postdml.go` | NativeOnly 跳过 v1 维护 + debug 日志 | ✅ 已提交 |
| `pkg/sql/colexec/table_function/fulltext.go` | debug 日志 | ✅ 代码在本地 |
| `pkg/fulltext/native/object.go` | NULL 列 continue 而非 return | ✅ 已提交 |
