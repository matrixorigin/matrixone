# #28238 / #30107：接受感知的 AUTO_INCREMENT 与约束仲裁设计

## 0. 修订与审查边界

修订 **r3，2026-10-18**，原实现基于 main `adbdd5c718baca6e4c054194f888b48a4f84a0a2`；2026-10-19 合并 main `def799e1260dd547cae2e0e54323ea20ec1b7685`，仅顺延本功能协议门槛至 120 并保留上游 119 dependent-program 元数据，不改变 r3 查找/锁/接受算法。r3 的实现前设计经 `gpt-6.1-sol / xhigh` 独立审查 PASS（session `01a14e30-2309-7380-b4f0-2a2ab293c768`）；LOCAL policy 澄清在同一模型/effort 下补充设计 PASS，保留原 PR 的 LOCAL CHECK 行跳过契约；实现与验证证据按当前版本单独记录在 PR 中。此记录不修改 GitHub reviewer 的决定。

问题 [#28238](https://github.com/matrixorigin/matrixone/issues/28238)，实现 [#30107](https://github.com/matrixorigin/matrixone/pull/30107)。保留原始 13 个提交及 6 次合并的累计功能修改；不恢复已被 main 吸收、累计增量为零的 EXPLAIN prerequisite。

r2 实测发现逐候选扫描和无条件共享父表锁的实际退化，其 N/P/F 限制仅是文档提案。**r3 撤回该限制提案并替换对应算法**：安全的 PK/UNIQUE 点查，其余情况每次执行按需构建一次有预算的 resident JoinMap；锁定实际匹配的物理父键。没有 SQL 形状限制、禁用配置、旧错误路径回退或动态 spill 承诺。

历史 r2 文档和数据保存在 [原修订](https://github.com/ck89119/matrixone/blob/27812a3c3816c0db490618183177403119c5df63/docs/design/CLAUDE_INSERT_IGNORE_ACCEPTANCE_28238.md)及[原测量附件](CLAUDE_INSERT_IGNORE_FK_BENCH_30107.md)。其中的 coarse-lock/O(N·F·P) 与小批上线提案均属于旧实现，不是当前执行契约。

## 1. 问题、依据与成功条件

旧链路先在 PRE_INSERT 发号，再过滤 CHECK/FK，最后执行 PK/UK 仲裁。被提前过滤的行丢失候选来源；被拒绝的正显式大 ID 已推进共享分配器，甚至使后续有效行提前溢出。下游不能恢复这些信息，更不能回退全局高水位。

问题讨论给出的四行 CHECK witness 为 `(NULL,1,-1),(NULL,2,1),(NULL,2,1),(NULL,3,1)`：先前 MO 接受 `(id,uk)=(2,2),(3,3)`，MySQL 8.4.10 接受 `(1,2),(2,3)`。另一个 TINYINT witness 中，因重复 UK 拒绝的显式 127 不应使后续有效生成行尝试 128。两者不是要求全局无间隙。

参考 MySQL 8.4 的 [AUTO_INCREMENT](https://dev.mysql.com/doc/refman/8.4/en/innodb-auto-increment-handling.html)、[INSERT](https://dev.mysql.com/doc/refman/8.4/en/insert.html)、[LOAD DATA](https://dev.mysql.com/doc/refman/8.4/en/load-data.html) 和 [外键约束](https://dev.mysql.com/doc/refman/8.4/en/create-table-foreign-keys.html)。MySQL 行映射是兼容性参照；MO 的分布式独占号段、事务快照和锁协议仍是实现约束，不复制 InnoDB 自增锁模式。错误文本和 warning 完整兼容不在本次目标内。

### 必须保持的不变量

1. 已消费的共享号段永不归还、倒退或与其他语句/CN 重叠；业务 rollback 不回收共享租约。
2. 同一串行仲裁器内，被拒绝的生成候选可以重用；只有完整通过约束的行推进候选游标和显式正 ID 下界。负数、显式零及 `NO_AUTO_VALUE_ON_ZERO` 继续遵循既有规则。
3. CHECK 等可提前判断的表达式只物化一次，不为重排重新执行 volatile 表达式。自动 ID 依赖的约束不能拿临时值当最终值。
4. 拒绝行不拥有任何 PK/UK/self-parent 键；当前行 self-FK 匹配不是向 accepted-parent 集合投机发布。
5. 输出、最终 PK 锁、普通及同步维护索引读取同一接受行映像；不得只改主表 ID。最终输出批次未完成时不发布本批生成 ID 的语句结果。
6. 真正 allocator/reader/表达式/容量/取消错误使语句失败，不能伪装成 FK 不存在或普通 IGNORE 跳行。
7. 新执行元数据必须同时通过发送方、接收方与实际目的 CN 的能力门禁；protobuf 可忽略未知字段不等于语义兼容。

成功标准：问题 witness、首/中/末/全部拒绝、接受/拒绝显式高 ID、整数终端、会话序列、跨批次及 prepared reuse 都符合这些不变量；base/index 一致，取消/失败后可清理；不增加逐行 allocator 事务，不宣称跨 CN 全局连续。

## 2. 边界与替代方案

自动路径仅用于纯 IGNORE、一个真实单列整数 AUTO_INCREMENT PK，并存在其它有效 UNIQUE 或 CHECK/FK。排除 ODKU、fake/composite PK、cluster-by/不可靠 remapping、可跳过 dedup 的内部路径，以及会依赖重写 ID 的生成表达式/非冗余唯一表达式。直接包含该 PK 的冗余 UNIQUE 不得把未生成的值当最终键。独立 self-FK 元数据还覆盖无 AUTO_INCREMENT 的 IGNORE。

普通 FK 保留向量化 join；只有 ID-dependent FK 和 self-FK 转入有序路径。普通非 IGNORE 自引用 INSERT/LOAD 保留语句检测 SQL；IGNORE 必须逐行拒绝，不能用检测 SQL 把整个有效语句回滚。`foreign_key_checks=0` 沿用现有禁用语义；prepared/catalog dependency 变化必须触发既有失效/重绑。

不新增 catalog 属性、allocator 持久表字段、后台 worker 或通用 retry 框架；不实现 gapless sequence、全局行顺序、新 FK 动作或新的 charset/collation、分区、UDF、存储过程、warning 兼容。

| 方案 | 正确性、成本及兼容性 | 决策 |
|---|---|---|
| 保持先发号后过滤 | 无新协议，但丢失来源，拒绝显式 ID 仍推进/溢出 | 不能满足问题契约 |
| 全部约束预过滤 | 普通 CHECK 可用且便宜；ID-dependent FK/生成键必须先知道最终 ID，循环依赖仍未解决 | 只用作可独立判断部分 |
| 回退 allocator / 按接受行重新发号 | 回退破坏跨 CN 租约独占；逐行 allocator 事务增加 I/O/争用，取消恢复复杂 | 拒绝 |
| 批量投机 ID 加向量 FK、失败再重算 | 有机会降低 FK I/O，但拒绝改变后继 ID，需迭代重算/撤销所有键、结果和 volatile 求值 | 暂不选择，不能无界重试 |
| 语句私有租约游标 + 完整接受仲裁 | 保留共享 allocator，准确延后显式推进；需要明确所有权/wire 和接受后才能决定的 FK probe | 选择；配套下述 point/resident lookup、物理 witness 锁与预算 |

选择不是吞吐保证。未来向量化必须保持接受顺序、最终键、self visibility 和单次求值，不可作为静默语义 fallback。

## 3. 数据与控制流

```text
SQL/session snapshot + typed table/epoch
  → binder：物化 CHECK eligibility；拆分 ordinary / ordered FK
  → PRE_INSERT：CHECK 选行、生成来源标志，延迟自动 ID/显式推进
  → ordinary FK join：保留资格位，不丢候选来源
  → 串行 PRE_INSERT_UNIQUE：候选 → eligibility → PK/UK → ordered FK
      → 接受：键集合、生成游标/显式下界、self-parent、最终输出
      → 拒绝：不发布、不消耗当前生成候选
  → lease TS 主键历史检查 → 最终 PK LOCK_OP
  → 接受行 SINK / MULTI_UPDATE / index consumers → 业务事务 commit
```

### CHECK、来源与 LOAD 策略

`retainInsertIgnoreCandidates` 把过滤条件转为 PROJECT 中一次求值的 bool，logical/physical pruning、deepcopy 和 codec 均保留位置。PRE_INSERT 的 CHECK selector 仅复制选中行；选中输出不可 shrink 借用的 child vector。全部 CHECK 失败且尚未进入生成阶段时，不启动 reservation。VALUES 已知输入数仍作为后来启动 reservation 的需求预算；SELECT/LOAD 未知长度按块请求，不能承诺与 VALUES 相同的未用号段形状。

生成来源标志在把零转 NULL 的既有 sql_mode 规则后捕获。deferred 路径对 AUTO_INCREMENT 的暂时 NULL 不提前做最终 NOT NULL 判断，其它列保持既有检查。FK eligibility 在 arbiter 处消费；资格为 NULL/false 的行不发布键。

LOCAL（非 REPLACE）的行拒绝与显式 IGNORE 的 decoder 放宽是两种策略。CHECK 通过原有 loadCheckIgnore 和 statement profile 选择 FILTER；LOCAL（非 REPLACE）和显式 IGNORE 都跳过失败行。main 的旧 LOCAL ASSERT 测试以前依赖 LOCAL 不启用 IGNORE 的假设，随本 PR 的 shared profile 扩展同步更新；非 LOCAL/非 IGNORE 与 REPLACE 仍 ASSERT。decoder 的显式放宽保持独立。`tree.IsIgnoreStatement` 供行/表达式 profile 使用；两个 LOAD binder 从语法重算 `ExternParam.LoadExplicitIgnore`，通过 Createsql JSON 传递。compiler 对 LOCAL 只因显式 IGNORE 放宽严格输入解析；非严格模式仍非严格。TIME 的原始文本/assignment cast 是上游独立契约，不可把 LOCAL row-profile=true 当作所有输入列都应宽松解析。复用 AST 重绑必须重置该标志。

## 4. 租约、游标与线性化点

`AutoIncrementReservation` 是内部 context request，不是 SQL API：`ColumnIndex, Type, Count, Runs`。调用 `InsertValues` 时必须为空向量、rows=0、estimate=0、Count>0、Runs 初始为空，目标列有效；表/epoch 继续由原 service admission 校验。`AutoIncrementRun` 为 `(First, Count, Step, AllocatedAt)`，不用不可表示的 exclusive end 描述类型末端。

| 状态/事件 | 所有者与动作 | 失败/下一状态 |
|---|---|---|
| 已有 shared cache 段 | 原 table cache acquire/release 与 column mutex；等待原在途分配时释放 mutex | ctx 取消/allocator 错误直接终止 |
| 消费 lease → request.Runs | 持列锁消费/推进 cache cursor；此点号码已不属于 cache | 部分成功后错误也不回收号码 |
| request → candidate stream | arbiter 将段复制进 mpool-backed stream，保留最老 TS | append 失败终止；所有已消费 lease 保持消耗 |
| 候选未接受 | 私有 stream peek，PK/UK/FK 依次判断 | 拒绝仍用同一候选；不发明新号码 |
| 接受生成行 | pop 一个合法候选，记录最终 PK/UK 与输出 | 后续容量/锁错误使整个语句失败，不回滚租约 |
| 接受正显式 ID | 必要时用既有显式 InsertValues 推进 allocator；discardThrough 保留私有下界 | 拒绝的显式 ID 不做此操作 |
| 输出批次完成 | 按 lease 最老 TS 检查历史 PK，发布本批 first-generated statement result | 检查发现并发占用返回既有 txn retry |
| Reset/Free/abort | 清除私有 run、下界、类型、TS、键集合、tuple、executor | 新执行是新 generation，不复用失败游标 |

三个点不能混淆：**allocator lease 消费**保证号码所有权；**arbiter 接受**只是语句内部键发布；**业务事务 commit**才是存储可见性。键集合更新中间可能失败，此时不能返回半批输出，原 txn rollback 和 operator cleanup 负责撤销数据/释放私有状态。

已提交表的 allocator 高水位沿用其既有独立事务，不受业务 rollback 回退；未提交 CREATE/private table 仍用原 owning txn，整个创建失败的既有例外不改变。业务重试重新 bind/admit/申请当前租约，不重放旧 Runs；可能留洞，不能把 retry 承诺成同一 ID。

VALUES 的首次请求使用 `ValuesRows`；未知流按 1、2、4…扩张，块上限 **65535**。剩余预算按到达 arbiter 的行维护，candidate 消耗只随接受发生。连续段合并，exhaustion compaction 不清除显式下界；只有 Reset/Free 清除。实际 allocator 仍按 CACHE/会话增量与 offset 选值，checked 加乘及 int 转换，unsigned all-ones 不作生成 ID；接近界限允许 reservation 缩短/饱和，但真实请求无合法下一值必须报错。

无进展分配至多沿用 **8** 次 guard，不新增无限重试。retire/epoch 更新返回 definition-change retry；旧 callback 不复活 retired cache。Wait-for 链仍为 caller → 原在途 allocator 完成或 ctx cancellation，不能把取消改成普通约束拒绝。

## 5. FK 查找、可见性与锁

只有 ID 依赖或自引用 FK 进入现有全局有序 arbiter；其它 FK 保留普通向量化路径。每个候选仍按原顺序经过 eligibility、PK/UK 和全部 FK；最终接受之前不发布键、不消耗当前候选号。

每个 FK 的存储查找只选择一条路径：

| 路径 | 资格与 owner | 匹配及保护 |
|---|---|---|
| PK point | 复用现有 Unicode/浮点/类型 guard，完整物理 PK 可安全裁剪 | 实际父行完整 SQL 等式复核，锁 base PK |
| UNIQUE point | 现有 index 定义、prefix 与 KeyFormat owner；真实隐藏表再回表 | 用实际父行计算 hidden UNIQUE key，锁该 namespace |
| resident | 其余既有 FK 形状，包括非唯一前缀和不能安全点查的比较域 | 固定视图扫描一次最小父 projection，经 HashmapBuilder 构建标准 JoinMap；取 SQL 等值组的一条实际父行，锁其 base PK |

LookupFilter 是物理裁剪条件，Predicate 是完整 SQL 真值，两者不可混淆。point filter 共用 `ConstantFold → BuildFoldedFilterExprs`，storage Fold 是裁剪字节，不能交给普通表达式执行器。复合 PK 的 serial 只用于物理访问，已从完整 SQL 等式中删除。

resident build/probe、current-self 和 earlier-accepted-self 都从原始 SQL 等式派生同一类型及 collation 域，不能以 raw vector bytes 替代。accepted-self 集合使用标准 SetRejectNaN 模式，NaN 不能因为 hash 命中而绕过 SQL 的非自反等式；有符号零仍按 SQL 等值匹配。任一 child 分量为 NULL 按 MATCH SIMPLE 通过。自引用依次查当前完整行、此前**完全接受**的行、固定存储视图；不读未来或已拒绝的候选。重复 tuple 使用标准 GroupSels 的一个 witness；标准 builder 省略 GroupSels 的全唯一表示直接使用 group ordinal。

编译时捕获 `Compile.TxnOffset`，本地、远端和 retry 更新均传递该值。lookup 不重新读取 workspace 的当前 write offset；当前语句的自引用可见性只有既有 accepted overlay 一份 owner。prepared/retry reuse 清除整个旧 attempt。

全部 FK 通过后，最多收集 F 个 witness，按现有 `SortForeignKeyLockTargets` namespace 次序调用保留 refresh/retry 的 `LockRows`。成功后才发布 accepted keys 和输出。parent DELETE/key UPDATE 使用对应 base/hidden exclusive namespace；非唯一引用锁住一个仍存在的 witness 即可维持存在性。**删除无条件共享父表锁**；仍遵循既有锁升级、deadlock 和 retry 机制，不承诺升级之后无关写入永不等待。

## 6. Wire 与资源生命周期

最低能力 **MORPC 120**；published 114–119 的含义保持 main 原样，其中 119 已由上游用于 coordinator-owned parameterized subquery programs，不能与本功能共用。发送端、接收端及实际目标 CN 均执行能力拒绝，不能丢弃新字段后走旧 scalar 语义。

| message / 字段 | 用途 |
|---|---|
| plan.PreInsertCtx 15 / 16 | check_eligibility / defer_auto_increment |
| pipeline.PreInsert 19 / 20 | 上述 operator wire |
| plan.PreInsertUkCtx 19 / 20 / 21 | row_eligibility / ordered_auto_increment / ordered_foreign_keys |
| OrderedForeignKey 7 | parent_access：kind、真实 lookup ref/table、filter、base-PK selector、lock target、witness expression |
| pipeline.PreInsertUnique 2 | 固定 txn_offset |

OrderedAutoIncrement 既有 1–5 字段和 OrderedForeignKey 1–6 字段保留。新 access kind 只能是 PK_POINT、UNIQUE_POINT、RESIDENT；unspecified/unknown、缺失元数据、错误 namespace/列界、负 offset 在共享校验边界拒绝。local Prepare、remote sender/receiver 共用 `ValidateOrderedInsertIgnore`；输入 batch 的实际列界仍由执行器检查。旧 eager AutoIncrementReorder wire 保留，新 ordered spec 同时带该 flag 是有效形状；无 spec 的 eager+eligibility 混合形状不由 planner 产生，应拒绝。

没有新的 WAL/catalog 格式，也不持久化 cursor、JoinMap 或 capability。混版要先升级参与执行的 CN；降级先 drain 在途语句和 prepared contexts，allocator 高水位不回退。

| 资源 | 分配和终态 |
|---|---|
| 租约流、accepted keys、自引用 overlay | 原 arbiter owner；ordered 路径激活既有 statement allocation account，实际容量准入；失败不回收已消费租约 |
| resident parent | HashmapBuilder 共用同一 account；append 完成后再推进 retained row count；finish 成功才转移 JoinMap、row count 和一个引用 |
| reader / build 临时状态 | 同步调用拥有；BuildReaders 部分成功亦关闭全部 reader；读取、关闭、表达式、取消和预算错误均向上传递，不伪装成 FK miss |
| published JoinMap | 通过 IteratorClearOwner 释放已记账 scratch 并解除 iterator，再 Free map；ready-empty 避免空表反复构建；Reset/Free 后旧 map 不跨 attempt |
| tuple、executor、witness | F 组有界 probe scratch；partial preparation 失败立即清理；先清除全部 operator 资源，再 ClearAllocationAccount |

正常 attempt 的父读取是 `Σ(需要 resident 的父行) + qualified native point work`，probe 随到达的 N·F 增长。resident 内存随最小父 projection 增长，由现有 statement/CN 预算准入；不足时 statement 失败并回滚。它不是无界隐式缓存，也没有自建 queue、worker、第二份 accepted 状态或 spill 状态机。普通 DQL/DML 和普通非有序 FK 不构建该状态；共享 PK guard 和 Fold preparation 是移动现有 owner 的实现。

## 7. 本次验证与可复核边界

当前 normal/native receipts 使用 Go 1.27.1、darwin/arm64、GOMAXPROCS=4；复用经过 provenance 核对的 native artifacts，无全仓构建或重复 race stress。

- 首轮 8 个完整 normal owning packages：512 个 root tests 通过，覆盖 defines、两个 protobuf 包、incrservice、preinsert、preinsertunique、tree、readutil。最终修正后 plan/compile/preinsertunique 三个完整包另以 5378 个 root tests 通过；未改动包的证据复用。
- `TestOrderedForeignKeyResidentWorkAndCleanup`：P=64 时 N=1/8/32 均只读 64 行；P=1024/N=32 单 FK 读 1024 行、双 FK 读 2048 行。还覆盖空表、重复与全唯一 tuple、8195 行跨标准 chunk、partial reader/close/预算失败；关闭和 account release 均检查。
- 同一 authenticated one-CN fixture 通过 PK/UNIQUE/非唯一前缀、自引用复合与 Unicode、prepared reuse、FK-setting 控制、LOAD IGNORE、实际行/ID/affected rows/rollback 验证。
- 最终 QA 红绿验证：LOCAL 原 ASSERT/profile 组合实际提交了 2 个 CHECK-invalid 行，NaN accepted-self 实际多接受了 1 行；修复后实际 LOCAL 行/ID/每行 CHECK warning、普通 LOAD 与 LOCAL REPLACE 失败无写入、NaN/有符号零通过。额外的 committed Unicode parent 查找暴露并修复了 consumer iterator 的记账 scratch 清理；不使用仅 nil 掉引用替代 owner release。
- 九个真实并发场景：精确 base/hidden namespace 的 `mo_locks()` waiter 作 barrier；无关 parent UPDATE 在 child holder 释放前完成；双向取消后无子行提交、无遗留 waiter；删除 parent 后重试必须弃用旧 witness。重试允许新 lease/ID，不要求重用已消费号码，最终逐行验证无 orphan。
- 12 个小型 public SQL workload：P=64/1024 × N=1/8/32 × ordinary/ordered。fixture/DDL/oracle 不计时；本轮 SQL execution 为 7.875–11.391ms。它们是 warm in-memory 单次诊断，不是历史旧二进制对比、冷盘、多 CN、吞吐或 p99 资格。owner work counters 也不冒充持久块或物理 I/O counters。

增量 normal consumer closure、一次 focused race、configured incremental SCA 和最终独立实现审查由 PR 的当前验证记录收口；历史 r2 的 540 SQL/72 contention 样本仅用于说明本次消除的旧退化。没有运行旧 CN binary 或新的多 CN 部署，不将单 CN wire tests 报成真实混版测试。新 GitHub CI 仍以当前 head 的 checks 为准。

## 8. 决策与维护成本

保留原 lease-transfer/allocator/cache retirement、final-ID cursor、CHECK materialization 和原唯一接受 owner。查找描述只保存 planner 作出的物理访问决定；TxnOffset 保护 statement view。同步 build 消费现有预算/hash/refcount；witness 消费现有 row lock/retry。这些增量分别消除重复全扫、过宽锁和旧视图复用风险，不创建通用在线 spill 字典或另一套事务协议。

后续真实 workload 若要求超预算磁盘查找，应另行设计可寻址的磁盘 lookup owner，不能把普通异步 JOIN spill 的单消费者 move-only payload 当随机访问缓存。本次没有隐藏 legacy fallback，也没有把旧 r2 小批 envelope 作为未执行的上线约束。

### Unicode 前缀索引的准入

Unicode UNIQUE 的字符前缀不能证明完整 SQL 等值关系：`ßa` 和 `ssa` 在 `utf8mb4_unicode_ci` 下相等，但各自第一个字符不同。这类索引复用每次执行只构建一次的 resident 路径，以 SQL 等值键匹配并锁定实际父行的物理主键。完整的版本化 Unicode 索引仍可点查；原生回归测试以 SQL 等值查询为参照，同时覆盖完整索引和前缀索引。
