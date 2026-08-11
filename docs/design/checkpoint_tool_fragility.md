# Checkpoint Tool 脆弱性分析：上游依赖与变更风险

本文档梳理 `mo-tool ckp` 离线 checkpoint 工具链中对 MatrixOne 内核私有 API / 数据格式的各项依赖，分析它们在不相关代码变更下被意外打破的风险。

---

## 1. 系统表 Schema 的硬编码副本

**位置**：`pkg/tools/checkpointtool/table_dump.go:165-244` (`builtinColumnsForLayout`)

**问题**：`mo_tables`（20 列）、`mo_columns`（27 列）、`mo_database`（10 列）三张系统表的完整列名、类型、Position 被逐列手写了一份，作为 checkpoint 数据中 catalog 元数据不可用时的 fallback。

```go
case catalog.MO_TABLES_ID:
    cols := []builtinColumnDef{
        {Name: "rel_id", SQLType: "BIGINT", Position: 0},
        {Name: "relname", SQLType: "VARCHAR(5000)", Position: 1},
        // ... 共 20+ 列
    }
```

**触发条件**：
- `catalog` 包中 `MoTablesSchema` / `MoColumnsSchema` 增删或重命名任一列（例如新增 `rel_foo`、删除 `extra_info`）
- 该变更不会导致编译错误——因为 `builtinColumnsForLayout` 是手写的独立结构体，不引用 `catalog` 包的 schema 变量

**后果**：checkpoint 数据中如果 header 是泛型名（`col_0`, `col_1`），按位置取值时会拿到错误的列值；`builtinTableSchemaForLayout` 返回的 Column 列表与实际物理数据不对齐。

---

## 2. Catalog Layout 检测依赖列数

**位置**：`pkg/tools/checkpointtool/table_dump.go:357-370` (`inferCatalogLayout`)

**问题**：纯靠**列数**判断一份 checkpoint 是当前 layout 还是 3.0-dev 旧 layout：

```go
switch dataWidth {
case len(schema):      // 列数完全匹配
    return layout, 0
case len(schema) + 1:  // 多一列也接受，带 offset=1
    return layout, 1
}
return currentCatalogLayout, 0  // 都不匹配 → 假设当前版本
```

已知的两个 layout 差异：
| | `mo_tables` 列数 | `mo_columns` 列数 |
|---|---|---|
| current | 20 | 27 |
| 3.0-dev | 19（少 `rel_logical_id`） | 25（少 `attr_has_generated`, `attr_generated`） |

**触发条件**：
- 未来的 layout 变体恰好与已知 layout 的列数相同（比如新版本加一列又删一列，列数不变但顺序变了）
- 列数对不上时静默 fallback 到 `currentCatalogLayout`，不报错

**后果**：`fallbackCatalogColIndex` 用 layout 的固定列顺序定位，列映射全错。对 `mo_tables` 的影响是取不到正确的 `rel_createsql`、`relname`；对 `mo_columns` 的影响是 `att_seqnum` / `att_is_hidden` 位置错误，导致 CSV 列全部错位或隐藏列未能过滤。

---

## 3. `logicalViewMetaCols = 3` 硬编码

**位置**：`pkg/tools/checkpointtool/table_dump.go:34`

**问题**：`LogicalTableView` 固定有 3 个 meta 列（`object`, `block`, `row`），所有数据列的偏移计算都依赖这个常量：

```go
const logicalViewMetaCols = 3

// 在 table_dump.go 中大量使用：
dataRow := fullRow[logicalViewMetaCols:]
dataWidth := len(view.Headers) - logicalViewMetaCols
dataIdx := logicalViewMetaCols + pos
```

`LogicalTableView` 的 headers 在 `logical_table.go:43` 构造：
```go
view := &LogicalTableView{
    Headers: []string{"object", "block", "row"},
    // ...
}
```

**触发条件**：`BuildLogicalTableView` 的 header 构造逻辑变更（增减 meta 列，如新增 `segment` 列）

**后果**：所有 `table_dump.go` 中的列偏移计算全部偏移，`buildSchemaFromMoTablesRow` 和 `buildColumnsFromMoColumnsRows` 拿到错误的数据列值。

---

## 4. 内核私有 API 直接依赖

### 4.1 `checkpoint.ReadEntriesFromMeta`

**位置**：`pkg/vm/engine/tae/db/checkpoint/meta.go:45`

工具调用方式：
```go
checkpoint.ReadEntriesFromMeta(
    ctx, "", ioutil.GetCheckpointDir(), name, 0, nil, r.mp, r.fs,
)
```

**风险**：
- 7 参数函数，`sid=""`, `verbose=0`, `onEachEntry=nil` 对工具无意义
- 内部绑定 meta 文件的二进制格式（注释中有明确的 attr/types 表格）
- meta 文件格式变更 → 函数行为改变，无需 deprecation

### 4.2 `logtail.NewCKPReaderWithTableID_V2`

**位置**：`pkg/vm/engine/tae/logtail/ckp_reader.go:237`

**风险**：
- `_V2` 后缀说明已发生过一次破坏性 API 变更
- 配套的 `ReadMeta()` 有隐式调用顺序要求（注释："must called after ReadMeta"）
- `ConsumeCheckpointWithTableID` 的回调签名如果改变，编译失败

### 4.3 `ioutil.GetCheckpointDir()`

**位置**：`pkg/objectio/ioutil/`

**风险**：
- 固定返回 `"ckp/"` 字符串
- 如果 MO 调整 checkpoint 目录结构 → 工具找不到任何 checkpoint 文件

### 4.4 `ioutil.ListTSRangeFiles` / `ckputil.ListCKPMetaNames`

**风险**：
- 依赖 checkpoint 目录下的文件命名约定
- 如果文件命名规则变化（扩展名、前缀等），过滤逻辑失效

### 4.5 `ckputil.TableObjectsAttrs`

**位置**：`pkg/vm/engine/ckputil/types.go:66`

```go
var TableObjectsAttrs = []string{
    "account_id", "db_id", "table_id", "object_type",
    "id", "create_ts", "delete_ts", ...
}
```

工具在 `checkpoint_reader.go:471` 直接引用：
```go
columns := ckputil.TableObjectsAttrs
```

**风险**：
- 包级 `var`（非 const），无编译期保护
- 属性名或顺序变化 → 工具输出的列名不变但值对不上

### 4.6 `objectioutil.FindTombstonesOfObject` / `GetTombstonesByBlockId`

**位置**：`pkg/objectio/ioutil/`

工具在 `logical_table.go` 中使用：
```go
selection, err := objectioutil.FindTombstonesOfObject(ctx, objectID, tombstoneStats, r.fs)
// ...
err := objectioutil.GetTombstonesByBlockId(ctx, snapshotTS, &blockID, getTombstone, &mask, r.fs)
```

**风险**：
- 深度依赖 objectio 的 bitmap、ObjectId、ObjectStats 等内部类型
- 如果 objectio 重构 bitmap 表示方式 → tombstone 过滤逻辑完全失效
- `getTombstone` 回调闭包跨层传递 `tombstoneStats` 切片，控制流不直观

---

## 5. 字符串列名匹配的静默 Fallback

**位置**：`pkg/tools/checkpointtool/table_dump.go:373-384` (`fallbackCatalogColIndex`)

**问题**：列位置解析有两步 fallback，失败时静默退化为按位置猜测：

```
① view.Headers 中按字符串匹配列名（如 "relname", "attnum"）
   ↓ 失败
② inferCatalogLayout 判断 layout → 按 layout 固定顺序定位
   ↓ 仍找不到
③ 返回 -1 → 上层以 0 作为默认值，拿到错误的列
```

**触发条件**：
- `catalog` 包改了列名（如 `rel_createsql` → `rel_create_sql`）→ 步骤① 失败
- 新版 checkpoint 的 header 变成了 `col_N` 泛型名（无字符串可匹配）→ 直接走步骤②
- 步骤② 的 layout 检测也失败（见第 2 节）→ 走步骤③

**后果**：不是报错而是**静默输出错误的 CSV**。用户在不知情的情况下拿到列值错位的导出结果。

---

## 6. `knownCatalogLayouts` 只覆盖两个版本

**位置**：`pkg/tools/checkpointtool/table_dump.go:150-152`

```go
func knownCatalogLayouts() []catalogLayout {
    return []catalogLayout{currentCatalogLayout, legacy3CatalogLayout}
}
```

**问题**：只认识 "current" 和 "3.0-dev" 两种 layout。如果 MO 4.0 又改了一次 catalog schema，就需要手动加第三种。否则 `inferCatalogLayout` fallback 到 `currentCatalogLayout`，列位置全错。

---

## 7. Catalog Layout 假设差异列只在末尾

**位置**：`pkg/tools/checkpointtool/table_dump.go:53-57`

```go
legacy3CatalogLayout = catalogLayout{
    moTablesSchema:  catalog.MoTablesSchema[:len(catalog.MoTablesSchema)-1],   // 切掉末尾 1 列
    moColumnsSchema: catalog.MoColumnsSchema[:len(catalog.MoColumnsSchema)-2], // 切掉末尾 2 列
}
```

**前提假设**：新版本只在 `MoColumnsSchema` / `MoTablesSchema` **末尾**追加列。如果未来在**中间**插入列或调整列顺序，用"切末尾"方式构造的旧 layout schema 就会产生错位（前半段对齐、后半段全错）。

---

## 8. `ObjectEntryInfo` 的可见性判断依赖 `objectio.ObjectEntry.Visible(ts)`

**位置**：`pkg/tools/checkpointtool/logical_table.go:218`

```go
if obj.Visible(snapshotTS) {
    visible = append(visible, entry)
}
```

**问题**：`objectio.ObjectEntry.Visible()` 的语义（基于 CreateTime / DeleteTime）是 MO 内部约定。如果可见性判断逻辑被调整（例如引入新的时间维度），工具的 tombstone 过滤和逻辑表输出就会不一致。

---

## 影响矩阵

| 上游变更 | 影响范围 | 是否静默 |
|---|---|---|
| `catalog` 包增删列 | schema 解析 → CSV 列全错 | ✅ 静默（编译通过） |
| 新增 catalog layout | 列位置偏移 | ✅ 静默（fallback 到 current） |
| `logical_table.go` 改 header | 所有数据列偏移 | ✅ 静默（`logicalViewMetaCols` 仍是 3） |
| `checkpoint` 包 meta 格式 | 无法加载 entry | ❌ 报错 |
| `logtail.NewCKPReader*` 改名 | 编译失败 | ❌ 编译错误 |
| `ioutil.GetCheckpointDir()` 改路径 | 找不到 checkpoint | ❌ 报错 |
| `ckputil.TableObjectsAttrs` 变动 | `ReadRangeData` 列名错误 | ✅ 静默 |
| `objectioutil` tombstone API 变更 | tombstone 过滤失效 | ❌ 可能编译或运行时错误 |
| `catalog` 列名修改 | 字符串匹配失败 → fallback 到手写位置 | ✅ 静默 |
| MO 列顺序调整（中间插入） | 旧 layout schema 错位 | ✅ 静默 |

---

## 缓解建议

1. **消除 schema 硬编码**：`builtinColumnsForLayout` 应改为从 `catalog.MoTablesSchema` / `catalog.MoColumnsSchema` 动态生成，而非手写一份副本。

2. **在 checkpoint 数据中嵌入 layout 版本号**：让 `mo_tables` 数据中携带 catalog schema 版本标记，替代列数推断。

3. **将 `logicalViewMetaCols` 改为由 `LogicalTableView` 自身提供**：`LogicalTableView.MetaWidth()` 方法，而非在 `table_dump.go` 中硬编码常量 `3`。

4. **对所有内核 API 调用加一层抽象接口**：定义 `CheckpointSource` / `ObjectReader` 等接口，将 `checkpoint.ReadEntriesFromMeta`、`logtail.NewCKPReader*` 等调用隔离在 adapter 层内。

5. **在 `inferCatalogLayout` 无法匹配时发出 warning**：而非静默 fallback 到 `currentCatalogLayout`。

6. **对列名匹配失败增加告警**：`fallbackCatalogColIndex` 返回 `-1` 时记录日志，便于排查。
