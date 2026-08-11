# PR #26923 review / CI 修复计划（2026-08-11）

## 背景与目标

本轮修复三个 P1，并先通过普通 merge 同步最新 `mo/main`：

1. CHECK 约束只能检查真实命中的 update target；未命中的 LEFT JOIN target 必须是 no-op，`UPDATE IGNORE` 也只能忽略该 target，不能过滤共享输入并漏写 sibling target。
2. Stage 1 尚未支持同一物理表的多个 writable alias；这类语句必须在 modern planner 中确定性拒绝，不能回退到可能 panic 的 legacy planner。
3. 新增的跨 CN target selector / target-aware dedup 字段必须有新的 MORPC 最低版本门禁；旧 CN 不得将未知 protobuf 字段按默认值继续执行。

## 设计不变量

- **Target eligibility 先于 target-local constraints**：Rowid/active selector 未命中的 target 不参与 CHECK、UNIQUE、PRE_INSERT 或物理写入。
- **IGNORE 隔离性**：一个 target 的约束失败只移除该 target branch 的候选写入；同一 join row 上其他 target 的合法写入保持可达。
- **Stage 1 安全边界**：检测到同一物理表的多个 writable alias 时返回稳定的 `NotSupported`，事务不执行且进程不 panic。
- **Wire fail-closed**：跨 CN pipeline 包含 target-aware dedup 或 PRE_INSERT selector 时，双方协议版本必须达到新版本；低版本在发送/执行前返回兼容性错误，不得依赖 protobuf 默认值。
- **成本边界**：约束分流沿用已有 per-target branch，不增加按行的全局状态、后台任务或无界缓存；版本检测只在远程 pipeline 构造/恢复边界执行。

## 实施步骤

1. 核对 PR 精确 head、工作区和中断 merge 状态；`git fetch mo main && git merge mo/main`，按 merge 方式解决冲突，生成文件由生成器重建。
2. 调整 UPDATE planner：把 CHECK 构造移动到 target eligibility 之后的 per-target branch；补 unmatched LEFT JOIN、matched violation、`UPDATE IGNORE` sibling 保留的 typed UT/BVT。
3. 将 repeated writable physical target 的路由从 legacy fallback 改为确定性 rejected；更新 route UT 和已有 FK/BVT golden，补实际命中行不 panic 且数据不变的回归。
4. 调研当前 MORPC 协议协商与 feature gate 机制，分配下一协议版本，并对 plan/pipeline 两侧 target-aware 字段做统一最低版本检测；补 current/current 成功与 current/old 双向 fail-closed 测试。
5. 重新生成 protobuf，并运行非空 focused tests、owning/dependent package test/build/vet、覆盖率、相关 BVT、SCA/static check 与 `make build`。
6. 对完整分支相对 verified PR base 执行 `mo-self-review`：逐层检查 target eligibility、约束隔离、route 分类、wire serialize/restore/clone/deepcopy，以及 Q1 资源、Q2 wait、Q3 增长边界。
7. push 前再次 fetch/merge 最新 `mo/main`；若有新 merge，重跑受影响验证。确认 diff/冲突标记/生成文件干净后 commit 并正常 push `origin/issue-26340-core-main`，不 force push、不评论 PR。

## 回归矩阵

| 维度 | Case | Oracle |
|---|---|---|
| CHECK / eligibility | LEFT JOIN target 未命中且 CHECK 恒假 | UPDATE 成功、target 无写入 |
| CHECK / ordinary | target 命中且 CHECK 失败 | 返回约束错误、无部分提交 |
| CHECK / IGNORE | 未命中或违规 target + 合法 sibling | 仅忽略失败 target，sibling 数据与 affected rows 正确 |
| route | 同一物理表两个 writable alias 实际命中 | 稳定 NotSupported，无 panic、数据不变 |
| route control | 两个不同物理表 target | modern planner 正常规划/执行 |
| wire | 新版本 sender/receiver | target selector 和 target-aware dedup 完整 round-trip |
| wire | 任一端低于最低版本 | 在远程执行前确定性拒绝，不消费 AUTO_INCREMENT、不写错 target |

## Review 修复：selected target 之前禁止赋值与 CHECK 副作用

### 新增不变量

- **求值隔离**：可能报错的赋值、DEFAULT、类型转换、on-update 和 generated 表达式，只能在对应 target 的最终 selected candidate（active 且 row_number=1）上求值；inactive/loser candidate 不得影响 sibling target。
- **CHECK 选择一致性**：CHECK、FK、index、PRE_INSERT 与物理 writer 必须复用同一个 `active AND row_number=1` 语义，不能各自重建不完整 eligibility。
- **单 target outer join**：未命中的 target 用 Rowid eligibility 做惰性求值保护，同时保留后续物理过滤；没有候选行时与空表 UPDATE 一样是 no-op。

### 实施与回归

1. 将 multi-target row-number 选择下推到 assignment projection 之前，并用惰性 `if(selected, new, old)` 包住 target-local 新值；保留 writer 的最终物理过滤。
2. 对 single-target nullable join 用 `isnotnull(Rowid)` 保护赋值求值；对 generated/on-update/default/cast 做同一 selected guard。
3. CHECK 直接复用 `buildTargetSelectedExpr` 的完整条件。
4. 增加 inactive 非法 cast、dedup loser 非法 cast、dedup loser CHECK violation 以及合法 sibling target 的白盒/BVT 对照。
5. 运行 planner/colexec/compile owning tests、相关 BVT、build/vet/SCA、完整 diff 自审；再次 merge 最新 `mo/main` 后正常 push。
