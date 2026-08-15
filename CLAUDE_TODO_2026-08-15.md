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

---

# PR #26907 fifth review closure

## 目标与实现分组

1. Flow control：分别实现 COALESCE 的 selected-arm provenance 与 IF/CASE/IFNULL 的 binary common-domain provenance，覆盖真实 binder cast、聚合与行执行路径。
2. CONVERT/CHAR：删除 non-string 固定 VARCHAR(4) 中间类型，按源类型最大显示宽度推导；默认 CHAR 返回 binary string，只有 `USING utf8mb4` 返回文本，并同步 CTAS。
3. REGEXP：subject 与 pattern 独立决定执行编码；静态 BLOB/BINARY、裸 binary user variable、显式 CAST、SQL PREPARE 与 COM_STMT 分别按 MySQL item/protocol 类别处理。
4. Provenance transport：修复 BIT/JSON 对新增来源类别的处理，并补齐 batch/group codec 对全部合法 kind 的编码、校验和 round trip。
5. 扩张函数：CONCAT、REPLACE、LPAD/RPAD 等超过 VARBINARY 上限时统一提升 BLOB，保证表达式 metadata、执行值与 CTAS 声明一致。

## 验证矩阵

- 每个 P1 先加入独立失败回归及相邻控制组，避免一个 marker 同时“修好”相反规则。
- 运行 vector/batch/group、colexec、function、plan、frontend 的聚焦与完整 CGo 测试。
- 执行 build、vet、golangci-lint、diff check 和推送前全差异自审；最后 merge 最新 `mo/main` 并重跑受冲突影响的验证。
