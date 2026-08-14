# PR #26907 base vector metadata fixes

## 目标

1. 修复 Vector 缩短后越界 stale NULL 位参与 `AllNull` 判定，导致有效行 binary-string provenance 被错误清除或保留。
2. 修复 allocation-accounted TEXT Vector 仅携带 `PrepareParamKinds` 时，`InplaceSort` / `InplaceSortAndCompact` 扩展 NULL/group bitmap 未先申请容量而 panic。

## 设计与步骤

1. 将 `AllNull` 的非 const 判定限定在逻辑区间 `[0, length)`，不修改或扫描逻辑长度以外的 bitmap extent。
2. 在 sort metadata 初始化处统一通过 Vector 的 allocation owner 为所有需要扩展的 bitmap 申请容量；NULL、group、binary-string sidecar 使用同一所有权契约，prepare-only 路径也必须覆盖。
3. 增加两个 stale NULL witness：单点 `SetNull` 不得清掉仍存活行的 binary marker；批量 `SetNulls` 在所有逻辑行均 NULL 时必须清掉 scalar marker。
4. 增加 accounted prepare-only sort/compact 回归，覆盖 128 行交替 kind、无 binary sidecar，并验证排序后 metadata 与值保持行对齐。
5. 运行聚焦测试、`pkg/container/vector` 完整测试、静态检查和推送前自审；再次合并最新 `mo/main` 后提交并正常推送。

## 不变量与成本

- NULL/metadata 判定只允许观察 Vector 的逻辑长度，不受复用缓冲区的历史 extent 影响。
- allocation-accounted bitmap 扩容必须先获得 owner 容量，失败路径不得留下部分扩展或未记账内存。
- `AllNull` 仍为 bitmap range count，不新增分配；sort 仅在已有 metadata 慢路径申请所需容量，不改变无 metadata 的热路径。
