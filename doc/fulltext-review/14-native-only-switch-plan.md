# 跳过 v1 隐藏表方案：`fulltext_native_only` 开关

## 目标

加一个开关，开启后：
1. **查询**：完全走 native sidecar 路径，不 fallback 到 v1 隐藏表
2. **DML**：跳过 PostDML 的隐藏表维护（不再生成 DELETE/INSERT token SQL）
3. **DDL**：暂时保留建隐藏表（方便回退），但隐藏表不再被维护和查询

## 改动清单

### 改动 1：查询路径 — 强制 native complete

文件：`pkg/sql/colexec/table_function/fulltext_native.go`

```go
func fulltextIndexMatchNative(
    u *fulltextState,
    proc *process.Process,
    s *fulltext.SearchAccum,
    srctbl, tblname string,
) (bool, error) {
    if !nativeQuerySupported(s) {
        return false, nil
    }

    scan, err := prepareNativeScan(proc, srctbl, tblname, u.param)
    if err != nil || scan == nil {
        return false, err
    }

    // 新增：native-only 模式下强制 complete，跳过 v1 fallback
    if u.param.NativeOnly() {
        scan.complete = true
    }

    if scan.complete {
        applyNativeSegmentStats(u, s, scan)
    }
    // ... 后续不变
}
```

效果：`scan.complete = true` 后，返回 `used = true`，v1 的 `runCountStar` + `runWordStats` + `groupby` 全部跳过。

### 改动 2：DML 路径 — 跳过 PostDML 隐藏表维护

文件：`pkg/sql/colexec/postdml/postdml.go`

```go
func (postdml *PostDml) runPostDml(proc *process.Process, result vm.CallResult) error {
    // ... 现有代码 ...

    if postdml.PostDmlCtx.FullText != nil {
        // 新增：native-only 模式下跳过隐藏表维护
        if postdml.PostDmlCtx.FullText.NativeOnly {
            return nil
        }
        // ... 现有 v1 隐藏表维护代码 ...
    }
    return nil
}
```

效果：INSERT/UPDATE/DELETE 不再生成维护隐藏表的 SQL。UPDATE 的旧词残留问题彻底消失。

### 改动 3：开关定义

文件：`pkg/fulltext/types.go`

```go
type FullTextParserParam struct {
    Parser         string   `json:"parser"`
    Implementation string   `json:"implementation,omitempty"`
    Parts          []string `json:"parts,omitempty"`
    NativeOnlyMode bool     `json:"native_only,omitempty"`
}

func (p FullTextParserParam) NativeOnly() bool {
    return p.NativeOnlyMode
}
```

### 改动 4：开关传递

文件：`pkg/sql/plan/apply_indices_fulltext.go`

```go
func (builder *QueryBuilder) buildFullTextScanParams(...) (string, error) {
    // ... 现有代码 ...
    param.Implementation = fulltext.FullTextImplementationNative
    param.NativeOnlyMode = true  // 新增：默认开启 native-only
    // ...
}
```

文件：`pkg/sql/plan/build_dml_util.go` — 在构建 PostDML context 时传递 NativeOnly 标志。

## 风险分析

| 风险 | 评估 |
|------|------|
| native 路径 bug 导致查询结果不正确 | 低 — tail segment 和混合查询已实现，tombstone 过滤已验证 |
| 隐藏表不再维护导致数据不一致 | 无影响 — 开启 native-only 后隐藏表不再被查询 |
| 回退困难 | 低 — 隐藏表仍然存在（DDL 不改），关闭开关即可回退到 v1 |

## 立即解决的问题

1. **大批量 UPDATE 旧词残留** — 根因是 v1 PostDML DELETE 大 IN 列表不完整。跳过 PostDML 后，UPDATE 完全依赖 native tombstone，问题消失。
2. **DML 性能** — 不再生成额外的 PostDML SQL，UPDATE/DELETE 路径更短。
3. **查询一致性** — 不再有 native + v1 混合结果的不一致风险。

## 保留的安全网

- DDL 仍然建隐藏表（方便回退）
- 如果 `NativeOnlyMode = false`，行为和当前完全一样
- 后续稳定后可以把 DDL 也改掉，不再建隐藏表
