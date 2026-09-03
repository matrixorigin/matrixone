# Issue #27836：历史孤儿对象权限升级清理设计

- 状态：**Draft，Revision 1 已 REQUEST_CHANGES；Revision 3 等待 admission prerequisite 完成**
- 设计版本：Revision 3（2026-09-03）
- Revision 1 review：`XuPeng-SH` 针对 `fb82b87a5723ccd8ac753b22461ccd36c9670e72` 提出 durable admission 与 protocol allocation blockers
- Owning issue：<https://github.com/matrixorigin/matrixone/issues/27836>
- Implementation PR：<https://github.com/matrixorigin/matrixone/pull/27944>
- 适用版本：v4.0.6 same-version offset upgrade
- 评审边界：本文描述完整迁移协议；独立评审必须引用包含本文的精确 Git revision。本文状态在获得可追踪 PASS 前不得改为 Approved。

## 1. 设计触发条件

该改动虽然源自数据修复，但同时改变 catalog DDL、持久 tenant-upgrade cursor、跨事务 retry、混合版本准入、restore 行为和 authorization metadata，因此命中 mandatory design-first gate：

1. 改变持久状态和 upgrade/restart/retry state machine；
2. 跨 bootstrap、tenant upgrade、frontend restore、compile DROP privilege cleanup 等 ownership boundary；
3. 涉及 mixed-version MORPC protocol；
4. 删除 authorization metadata，影响 tenant security boundary；
5. cleanup page 独立提交，改变 caller transaction contract。

## 2. 问题、证据与成功标准

### 2.1 问题

历史版本 DROP database/table/view/sequence 后，`mo_catalog.mo_role_privs` 可能遗留引用已删除对象的权限行。遗留行造成 catalog 不一致，并可能影响后续授权检查。普通 DROP 的在线清理不能修复升级前已经存在的数据。

### 2.2 核心不变量

对于每个存活 tenant，在 migration 发布完成后：

1. 被本设计明确识别的 database-scope grant，其非零 `obj_id` 必须引用该 tenant 的存活 `mo_database.dat_id`；
2. 被明确识别的 relation-scope grant，其非零 `obj_id` 必须引用该 tenant 的存活 `mo_tables.rel_logical_id`；
3. migration 不得删除 global grant、`obj_id = 0`、存活对象 grant、存活 hidden child grant、sequence grant（按 table privilege encoding 验证）以及无法确认语义的 legacy encoding；
4. tenant A 的事务、谓词和 cursor 不得读取或修改 tenant B 的 privilege catalog；
5. tenant version/task ready 只能在该 tenant 所有 cleanup page 已提交后发布；
6. durable range cursor 只能越过已完成或已删除的最低未处理 account ID，绝不跳过仍存活的较小 account ID。

不变量的反例包括：根据 `rel_id` 而非 `rel_logical_id` 判断、删除 `obj_id=0`、未完成 page 就发布版本、从未定义行序的 `tenants[0]` 推进 cursor、旧协议 CN 在新 page protocol 已开始后继续写入。

### 2.3 Goals

- 修复现有 v4.0.6 tenant，包括 snapshot/PITR/account/cluster restore 后仍为同版本的 tenant；
- rolling upgrade 下仅在所有 CN 支持新协议后开始 tenant snapshot 与 destructive cleanup；
- 每事务最多删除 1000 行，支持 crash/retry/resume；
- background、manual `UpgradeOneTenant` 和 on-demand `MaybeUpgradeTenant` 共享相同 page semantics；
- 合法并发 `DROP ACCOUNT` 不得导致 CN Fatal；
- 保留普通 DROP 对 hidden-index logical ID privilege 的在线清理。

### 2.4 Non-goals

- 不推断或重写未知 legacy privilege encoding；
- 不修复非目标 authorization table；
- 不提供迁移已提交后的反向数据恢复；被删行是经 preservation boundary 判定的历史垃圾；
- 不允许在 caller-owned transaction 内隐式提交多个独立 page；
- 不引入新 progress table、后台队列或无限 retry framework。

### 2.5 成功标准

- 目标 orphan 全部清除且 preservation controls 全部保留；
- 1001+ 行证明跨事务分页和 final publication；
- rollback、conflict retry、restart/resume、stale worker、tenant deletion、restore、SI barrier race 均有确定性证据；
- page workspace、lock/tombstone 和 retry cost 均受 1000 行上限约束；
- 精确设计 revision 获得独立 PASS，implementation 与该 revision 无 material deviation。

## 3. 数据分类与 authorization preservation boundary

### 3.1 可删除 database scope

仅以下编码进入 database orphan predicate：

- `obj_type = 'database' AND privilege_level = 'd'`；
- `obj_type IN ('table','view') AND privilege_level IN ('d.*','*')`。

并且必须同时满足：`obj_id != 0`，且 tenant-local `mo_database` 中不存在 `dat_id = obj_id`。

### 3.2 可删除 relation scope

仅以下编码进入 relation orphan predicate：

- `obj_type IN ('table','view') AND privilege_level IN ('d.t','t')`。

并且必须同时满足：`obj_id != 0`，且 tenant-local `mo_tables` 中不存在 `rel_logical_id = obj_id`。Sequence 使用 table privilege encoding，因此由相同 relation predicate 保护和清理。

### 3.3 必须保留

- `obj_id = 0` 的 global/wildcard grant；
- live database/table/view/sequence grant；
- hidden index child 在 relation 存活时对应的 logical-ID grant；
- 不属于上述已知组合的 encoding。

拒绝“删除所有找不到 table row 的非零 obj_id”方案，因为 database scope、global grant 和未知 encoding 会被误删。

### 3.4 Tenant isolation

Cleanup 事务使用目标 tenant account context 并访问该 tenant 的 `mo_catalog`。不存在跨 tenant 聚合 DELETE；task metadata 位于 system catalog，但只保存 account ID range/progress，不保存 privilege 内容。

## 4. 前置 index 与协议准入

### 4.1 Index ownership/order

v4.0.6 tenant upgrade entry 创建 `idx_mo_role_privs_obj_id(obj_id)`。Entry 以显式 identity 匹配，不依赖 upgrade list 的偶然位置。Index creation 必须在 orphan DELETE entry 前成功；失败则当前事务回滚，不进入 cleanup page。

该 index 同时支持 bounded historical DELETE 和普通 DROP 的持续 cleanup。重复执行通过 upgrade check 判定已有 index，保持幂等。

### 4.2 唯一 cumulative capability allocation

本 migration 不预先占用具体数字，而定义：

```text
V_migration = MORPCLatestVersion(prerequisites merged into main) + 1
```

截至 Revision 2 更新时，live allocation 已发生 drift：main 的 v43 owner 是 scalar runtime-filter；#27553 最新 head 在该基础上提出 v44；#27756 仍需 rebase 后重新分配自己的 next version。因此文档中的数字示例不能成为 allocation，只有 prerequisite merge 后 main 的实际常量链才是 source of truth。

Prerequisites 是 #27553 的 payload capability 与 #27756 的 authoritative DDL visibility/admission fence 均合入 `main`。#27756 曾关闭且未合入，使 Revision 2 的 prerequisite 一度失效；现已重新开放，由其继续作为 durable admission owner，不能用 main 中仅面向 view metadata 的 #27430 lifecycle admission 代替。#27756 必须先关闭其 unknown-commit/post-commit visibility blocker 并合入。本 migration 随后分配 `V_migration`。若任一 prerequisite 的版本、API 或 merge 顺序改变，本设计必须产生新 revision，并始终使用 `max(merged cumulative version)+1`。在 prerequisites 未合并前，本 PR 不得进入 implementation approval/merge。

`MORPCLatestVersion` 表示 binary 的 compiled maximum capability。任何 `SetProtocolVersion(V)` receiver 必须在 `V > MORPCLatestVersion` 时 fail closed；因此仅包含 prerequisites、缺少本 migration semantics 的 binary 不能谎报 `V_migration`。

### 4.3 Authoritative durable admission fence

Point-in-time `GetProtocolVersion` 仅用于诊断，**不再充当 migration activation fence**。`V_migration` 复用 #27756 在 HAKeeper RSM 中持久化的 authoritative deployed-protocol/admission generation，并扩展其 cumulative protocol contract：

1. Activation owner 从 HAKeeper authoritative CN membership 构造 exact `(ServiceID, generation, QueryAddress)` target set；
2. 每个 target 在接受 `V_migration` 前验证 `MORPCLatestVersion >= V_migration`，停止新 SQL/task worker admission，drain 已接纳 work，并以 generation-bound heartbeat 发布 Prepared；
3. HAKeeper 在一个 replicated transition 中确认完整 membership/Prepared set 并持久化 minimum deployed protocol `V_migration`；该 transition 是 migration activation linearization point；
4. 每个 target 观察 committed `V_migration` 后发布 Fenced/Admitted，才重新开放 SQL ingress 和 tenant worker；
5. 新启动、scale-out、replacement CN 必须先读取 authoritative deployed protocol。若 compiled latest < deployed minimum，startup fail closed：不得开放 SQL ingress、QueryService work、pipeline work或 TaskRunner；若 compiled latest >= minimum，则加入 exact generation admission、完成 fence 后才能 serving/worker；
6. UUID takeover/restart 由 authoritative generation revocation 同步关闭旧 process 的 ingress 和 TaskRunner，旧 generation 不得继续工作；
7. Cluster upgrade 只有在读取到 authoritative deployed protocol >=45 后，才可在**新的事务**中创建 SI tenant snapshot 和 tenant tasks。

这个 fence 持续到 deployment lifetime，而不是 migration 完成后解除；它保证 migration publication 后也不会有旧 DROP producer 重新制造 orphan。单纯每 page 重查所有 CN 不能替代该协议。

对 #27756 admission owner 的必要 cumulative hardening 是本设计的一部分：

- receiver 拒绝高于 compiled latest 的 protocol；
- markerless/restarted binary 发现 durable deployed minimum 高于 compiled latest 时保持 non-serving/non-worker，而不是把 runtime version 直接提升到自己不具备的版本；
- tenant task runner 的 start/admit 与 SQL ingress 使用同一 authoritative generation gate；
- migration precondition 读取 durable deployed minimum，不依据某次 service snapshot 中恰好出现的 CN 集合。

### 4.4 两个确定性 admission 反例

1. **Activation 前缺席、task 前加入**：仅支持 prerequisite latest 的旧 CN 不在 initial target observation 中；`V_migration` commit 后它 heartbeat/start。HAKeeper 返回 minimum `V_migration`，CN 因 compiled latest `< V_migration` 保持 non-serving/non-worker，不能运行 legacy range worker。
2. **Final page check 后、publication 前加入**：在新 worker 完成 final empty-page check 后暂停 transaction；旧 CN heartbeat。该 CN 同样被 authoritative minimum 拒绝，不能执行 old DROP；随后 tenant version/task publication 才允许提交。

测试必须使用 replicated admission transition/generation hook 构造 happens-before，不使用 sleep，也不能用重复 `GetProtocolVersion` 轮询冒充拒绝证据。

### 4.5 Mixed-version、downgrade 与 rollback

- `V_migration` activation 前：不执行新 tenant cleanup，仅支持 prerequisite latest 的 CN 可继续运行；
- activation 失败/超时：minimum deployed protocol 不提交，不创建 tenant SI snapshot，不删除权限；已经本地 Prepared 的 CN保持 fail-closed 或按 v44 activation recovery 重试，不能单方面开放旧 producer；
- authoritative `V_migration` commit 后：compiled latest `< V_migration` 的 CN 永久 non-serving/non-worker；
- page 事务失败：仅该 page 回滚，之前已提交 page 保持有效并由 cursor/谓词恢复；
- committed `V_migration` 后不支持 binary downgrade 到低于 `V_migration`。安全 fallback 是停止 upgrade、保留已完成 orphan 删除和 task cursor，并恢复/升级至 `V_migration`-capable binaries 后继续；
- catalog index 对旧查询透明，但不能把“index 兼容”解释为允许旧 worker 修改新 cursor；
- 已提交的 orphan 删除不做自动数据反向恢复，因为 preservation predicate 将其定义为无合法 owner 的 metadata。若 predicate 被证明错误，必须停止 rollout 并从 migration 前备份恢复。

## 5. 状态与 owner

### 5.1 持久状态

- `mo_account.create_version`：tenant 已发布版本；owner 为 tenant upgrade transaction；
- `mo_upgrade_tenant.from_account_id`：range 内最低候选 account cursor；owner 为 background task claim transaction；
- `mo_upgrade_tenant.to_account_id`：range 固定终点；创建后不变；
- `mo_upgrade_tenant.ready`：该 range 已无待处理 tenant；
- cluster upgrade ready counters：只在 task cursor/final ready 原子 claim 成功时推进。

`from_account_id` 的复用避免新增永久 progress table；代价是所有 worker 必须服从本设计的单调 cursor predicate，且 authoritative `V_migration` admission commit 后禁止旧 worker。

### 5.2 临时状态

- 每个 cleanup transaction 的 affected rows/completed bit；生命周期到事务提交/回滚；
- direct driver 的 handler route；每个 page 都根据 `SELECT create_version ... FOR UPDATE` 得到的持久版本重建，不缓存 callback version；
- tenant checked cache：仅缓存 independently committed no-upgrade/complete 观察；caller-owned transaction 中的观察不得缓存。

## 6. Background range state machine

### 6.1 选择 task 与 tenant

1. 选择 `ready=0` 的 task range；
2. 查询 `[from_account_id,to_account_id]` 内账户，SQL 必须使用 `ORDER BY account_id FOR UPDATE`；
3. 实现对 account/version pairs 再做 paired ascending sort，防御 executor/storage 未遵守返回顺序；
4. 只处理 `tenants[0]`，它必须是最低存活未处理 account；
5. fast-fail lock conflict 时不改变该 range cursor，可扫描其他 range；最终由 retry 重新选择。

### 6.2 Page transition

对所选 tenant：

```text
PENDING(version V, cursor C)
  -- lock tenant/version; run <=1000 DELETE; affected==1000 --> PAGE_COMMITTED(V,C)
PAGE_COMMITTED(V,C)
  -- retry/restart --> PENDING(version V,cursor C)
PENDING
  -- affected<1000 and final checks pass --> TENANT_COMPLETE
TENANT_COMPLETE
  -- atomic cursor claim --> cursor=tenantID+1 OR range ready=1
```

每次 `HandleTenantUpgradeStep` 只执行一个 page。`affected == 1000` 保守视为可能仍有数据；下一事务继续。`affected < 1000` 才允许该 entry 完成并继续后续 entry/version publication。

### 6.3 Cursor ownership invariant

`AdvanceUpgradeTenantTask` 使用 compare-and-claim predicates：

- 非末尾：仅当 `ready=0 AND from<=completedID AND to>completedID` 时设置 `from=completedID+1`；
- 末尾：仅当 `ready=0 AND from<=completedID AND to<=completedID` 时设置 `ready=1`；
- `AffectedRows=0` 表示 stale worker 未取得 ownership，不增加 ready count；
- `AffectedRows>1` 是 invalid state，返回错误。

由于 selected tenant 是最低存活 account，`completedID+1` 只越过：已完成 account、range 中不存在的 ID，以及刚完成 account；不会越过存活未处理 tenant。

### 6.4 删除 tenant

- range 内无账户：reconciliation 原子标记 ready；
- page 提交后并发 DROP ACCOUNT：下一次 direct/background lookup 零行，视为合法 terminal；
- deletion reconciliation 只在 range 内不存在任何账户时标记 ready；不得因一个账户删除跳过其他存活账户。

## 7. Direct/manual/on-demand state machine

`UpgradeOneTenant` 与 `MaybeUpgradeTenant` 调用共同 direct driver：

1. 初始 `GetTenantVersionIfExists`；不存在即正常完成且不运行 handler；
2. `MaybeUpgradeTenant` 若需要 migration 且传入 caller-owned `txnOp`，返回固定 retry error：`tenant upgrade requires retry without a caller-owned transaction`，并且在创建独立 page transaction 前返回；
3. 每个独立事务执行 `GetTenantCreateVersionForUpdateIfExists ... FOR UPDATE`；不存在即返回 `exists=false`；
4. 根据锁定的 persisted version 重建 handler route；callback version 仅是 hint，不参与 route；
5. 一个 page 未完成则提交并开启下一事务；
6. 全部 entry 完成后在同一锁定事务发布 tenant version；
7. 下一 final done-check 发现已无 route 时返回 complete。

并发 version advance 后，下一 page 从新版本重建 route，不得写回旧版本。并发 DROP ACCOUNT 在 page commit 后合法；不得 Fatal，也不得执行 version update。

## 8. Restore 与普通 DROP

- 普通 DROP 继续删除主对象及 hidden-index logical ID 对应的 privilege；
- 仅 `SessionInfo.IsRestore` 抑制 hidden-child cleanup，因为 restore catalog replay 时 `mo_role_privs` 可能尚未创建；
- account、cluster、snapshot、PITR restore 最终进入相同 same-version offset detection：缺少 index 或仍存在目标 orphan 时需要 migration；
- same-version tenant 不能仅根据版本字符串跳过，需要 `TenantUpgradeRequired` 检查 migration 后置条件。

## 9. Crash、retry、取消与并发矩阵

| 事件 | Commit point 前 | Commit point 后 |
|---|---|---|
| DELETE page error/cancel | 当前 page 全回滚 | 不适用 |
| CN crash | 当前 txn 回滚 | 已提交 page 保留，谓词重扫/继续 |
| lock/write conflict | txn 返回错误，由既有 scheduler/direct caller retry | 已提交旧 page 不重复计数 |
| stale duplicate worker | account lock/cursor predicate 串行化 | cursor claim 0 rows，不重复 ready count |
| tenant version 并发推进 | 下一 page 锁定后重建 route | 不覆盖更高 persisted version |
| DROP ACCOUNT | lookup 零行为 terminal | 下一 page/final check 零行为 terminal |
| caller txn rollback | required upgrade 在独立 page 前拒绝 | no-upgrade 观察不缓存 |
| authoritative `V_migration` activation failure | 不提交 deployed minimum、不创建 tenant SI snapshot、不 cleanup | commit 后持续执行 `V_migration` admission contract |

Retry 没有内部无限紧循环：每个 scheduler invocation/direct page 都受上层 context 控制；SQL 错误向上返回。DELETE predicate 幂等，因此已删行不会产生副作用。

## 10. Bounds、性能与 failure containment

- DELETE：每 transaction 每 entry 最多 1000 rows；workspace、tombstone、锁与 rollback cost 为 O(1000)；
- index：`obj_id` secondary index 将在线 DROP 和 migration lookup 从无辅助索引扫描变为按 object ID 定位；index installation 是一次性 tenant DDL；
- task range account query：结果数受 task range 大小约束，SQL 排序由 account primary/index key 支持；defensive in-memory paired sort 为 O(n log n)，无额外 row copy；
- direct driver：每 page 一个 transaction，不持有跨事务锁或 goroutine；
- 无新增 goroutine、queue、unbounded cache、metrics cardinality 或永久表；
- 单 tenant SQL/predicate 失败只阻止该 tenant/task publication，不得使其他 tenant 数据被删除；missing tenant 不得终止 CN。

## 11. Observability 与运维

现有持久字段提供诊断面：

- `mo_upgrade.from_version/to_version/state`：cluster upgrade 阶段；
- `mo_upgrade_tenant.from_account_id/to_account_id/ready/update_at`：range progress 与停滞位置；
- `mo_account.create_version`：tenant publication；
- SQL/error logs：page、lock conflict、protocol barrier 和 invalid affected-row failures。

不增加每 row 日志或高基数 metrics。运维发现停滞时比较 task `update_at`、cursor 和 tenant version；先修复失败原因，再由正常 retry 恢复。不得手工将 ready 置 1 绕过 preservation/postcondition。

## 12. Alternatives

### A. 单事务全量 DELETE（拒绝）

优点是实现简单；缺点是 workspace/tombstone/锁和 rollback cost 无界，1000+ 历史数据及大 tenant 会放大 OOM、冲突和失败重做风险，也无法安全 resume。

### B. 新建永久 per-tenant progress table（拒绝）

状态表达更直接，但引入 catalog schema、bootstrap/restore/cleanup 生命周期和兼容面。现有 task range cursor 在明确单调 ownership predicate 和 authoritative `V_migration` admission 下足够，新增表总复杂度更高。

### C. 仅修复未来 DROP，不迁移历史数据（拒绝）

无法满足 issue 中现有 tenant 的历史 orphan 修复，也不能覆盖 restore 的旧 snapshot。

### D. 每 tenant 扫描并一次删除全部 orphan（拒绝）

避免 range cursor 复杂度，但仍有单 tenant 无界事务问题；不能满足 page resource budget。

### E. 选定方案

显式 preservation predicates + obj_id index + authoritative `V_migration` admission + bounded idempotent page + existing task cursor atomic claim。它增加了 protocol 约束，但在不新增永久 migration schema 的前提下同时闭合历史修复、资源上限、restart 和 all-entry-point semantics。

## 13. Validation matrix

| Contract | 最便宜的确定性证据 |
|---|---|
| predicate 删除目标且保留 controls | v4.0.6 upgrade UT + embedded catalog assertions |
| database/relation/sequence logical ID | entry SQL UT 与 embedded live/orphan fixtures |
| index before cleanup、order independence | upgrade list/entry identity UT |
| 每 page <=1000、完成后才 publication | 1001-row incremental UT/embedded test |
| rollback/retry/resume | injected txn failure 与 retry UT |
| cursor monotonic/stale ownership | affected-row predicate UT、duplicate worker UT |
| undefined row order 不跳 tenant | adversarial reverse account/version rows，断言 paired ascending result 与 ORDER BY SQL |
| SI snapshot 在 durable activation 后 | deterministic blocked activation/tenant-create interleaving embedded test |
| prerequisite-only CN 在 task 前加入 | HAKeeper committed-minimum + startup hook，断言 SQL/worker admission 均关闭 |
| prerequisite-only CN 在 final check 后加入 | transaction publication hook + heartbeat，断言 old DROP/legacy worker 不可达 |
| unique cumulative allocation | receiver rejects requested > compiled latest；prerequisite-only fixture 不能声明 `V_migration` |
| callback stale/newer 不改变 route | persisted-version routing UT |
| concurrent version advance 不 downgrade | page interleaving UT |
| page 后 DROP ACCOUNT | deterministic next-transaction missing-row UT |
| caller-owned txn | rejection-before-page UT + real row-lock embedded regression |
| ordinary DROP hidden child | frontend/compile DROP regression |
| restore suppress only restore path | account/cluster/snapshot/PITR restore regressions |
| owning packages compile/static checks | CGo package tests、`go vet`、`git diff --check` |

测试不得依赖 sleep；并发场景使用 channel/hook/transaction lock 形成确定性 happens-before。GitHub exact-head terminal CI 可复用，但 pending/旧 HEAD 不能作为 PASS。

## 14. Rollout 与 removal

1. #27553、#27756 完成 rebase 并进入 main，本 migration 按 main 实际 latest+1 分配 `V_migration`；
2. 部署所有 `V_migration`-capable CN并通过 authoritative activation 持久化 minimum deployed protocol `V_migration`；
3. 确认 durable `V_migration` admission 后创建新 tenant snapshot；
4. 安装 tenant index，再按 page 执行 cleanup；
5. 观察 task cursor/version publication；失败 tenant 保持未 ready，可 retry；
6. migration 完成后保留 index 和 `V_migration` minimum admission，继续服务普通 DROP cleanup；
7. 本迁移 entry 属于版本历史，不在未证明所有升级/restore 来源都不再需要前删除。

Blast radius 以 tenant/page transaction 隔离。若发现 predicate 设计错误，停止 rollout，不继续发布 tenant version；使用 migration 前备份恢复受影响 authorization metadata，并提交新的设计 revision。

## 15. 决策记录与待审批项

### 已决策

- 使用 `rel_logical_id` 而非 physical `rel_id`；
- `obj_id=0`、global、unknown encoding 保留；
- page size 固定 1000；
- cursor 必须 SQL ordering + defensive paired ordering；
- callback version 不参与 routing；
- caller-owned transaction 中 required migration 明确拒绝；
- DROP ACCOUNT 是合法 terminal；
- authoritative `V_migration` commit 后的 binary downgrade 不支持；
- `V_migration` receiver 必须拒绝 `requested > compiled latest`，旧 binary join 必须保持 non-serving/non-worker。

### Revision 2 implementation status

当前 implementation 仍使用 v43 point-in-time checks，**明确不符合 Revision 3，不能进入 implementation approval**。#27756 已重新开放并继续作为独立 durable admission owner；实现必须等待 #27553、#27756 合并到 main，随后基于 merged authoritative admission API 分配 `V_migration` 并完成上述 hardening/tests。若 prerequisite API 或版本链变化，先更新本文并重新评审，不在本 PR 复制 #27756 的 8000+ 行 admission implementation。

### Blocking approval

Revision 2 需要独立设计评审；implementation 还受 prerequisite merge 与 alignment 阻塞。评审记录必须采用以下格式并引用精确 revision：

```text
Change scope: Issue #27836 complete migration protocol
Trigger: persistent upgrade/retry state, mixed-version protocol, catalog and authorization boundary
Design: docs/design/CLAUDE_ISSUE_27836_ORPHAN_ROLE_PRIVILEGE_CLEANUP.md, revision <commit>
Blocking findings: <none or list>
Decision log: <accepted tradeoffs/resolved questions>
Decision: PASS | REQUEST_CHANGES
Implementation deviations: <none or sections requiring re-review>
```

在得到 PASS 前，PR 维持 Draft/REQUEST_CHANGES 状态，不能宣称 mandatory design gate 已关闭。
