# #23371：AUTO_ID_CACHE 发布边界补充设计（A 已批准，r1）

日期：2026-09-09。基线：`cd04bb4c1af5bc595e2147dc645dfa754f4c395b`。用户在方案 A/B 评审后以 “go ahead” 批准方案 A，实施前已记录于当日 TODO。归属 #23371，实施 PR 为 `ck89119:issue-23371-main` → `matrixorigin/matrixone:main` 的 Draft PR；其正文链接本设计的确切提交修订。

本文件讨论 r4 的版本门禁，不改变已批准的 CACHE 数值、元数据所有者和发号语义。不借此扩展其它 AUTO_INCREMENT issue。

## 1. 已证实的事实

- `pkg/common/runtime/runtime.go:SetupServiceBasedRuntime` 缺省设置本机 `MORPCLatestVersion`。它不是所有参与节点的最低版本。
- 最新 main 的 V57 属于 Arrow LOAD，V58 已由 binary-string/runtime-domain 协议使用。合并 `d3e8aced87` 时保留上游 V58，将本 PR 尚未合入的 CACHE 门槛从初版 V58 顺延为 **V59**；再合并 `401b967dc1` 时，上游 V59 已用于 FORMAT，CACHE 顺延 **V60**；既有 opcode/字段编号不变。
- 旧 CN 不认识 SchemaExtra.auto_id_cache，会使用默认 allocator 策略；旧 TN 的手写 schema clone 可能丢掉该字段。这是能力缺失，不是号段算法的错误。
- `pkg/hakeeper/view_metadata_admission.go` 不是通用能力注册表：它绑定 view catalog epoch、CN/proxy generation、准备阶段/barrier/恢复状态及 store timeout。
- `pkg/cnservice/server_view_metadata_admission.go` 绑定 catalog fence、bootstrap、SQL/query/pipeline ingress、generation 撤销和任务停止；直接把 CACHE 塞进它会混淆 view 的所有者。
- 只检查一次 heartbeat 或 CREATE 时的节点列表，不能排除随后旧进程重新加入、UUID 复用、分区中的旧进程继续服务。
- 既有 exact-TN AUTO_INCREMENT epoch fence 解决 allocator 重置与写入代际，不证明 writer 认识新的表级缓存策略，不能借用它宣称 CACHE 兼容。

## 2. 已确认的产品边界

r4 的“旧/未知节点不得静默忽略”与“本任务不新增跨服务状态所有者”在任意混合版本/旧节点重入条件下不能同时由一个局部版本号保证。必须明确首版的支持范围，不能在实现时偷偷降低保证。

### 方案 A：首版采用受控全量升级 + 显式开关（已批准）

**支持契约：首次启用非零 AUTO_ID_CACHE 前完成全量升级；不支持新策略在混合版本期间启用/运行。** 这不是自动混合版本 admission 的实现。

新增 `incrservice.Config.EnableAutoIDCache`，由已有 CN 自增配置入口暴露，默认 false：

- 关闭时，0/省略继续完全兼容；非零 CREATE 在发布 metadata 之前拒绝。
- 已有非零表在禁用节点构造 allocator cache/发号时明确失败，不把策略降为 0；SHOW metadata 仍保留真实选项。开关不是禁止该表所有维护操作的写锁：既有 `ALTER AUTO_INCREMENT` 直接调整 offset/epoch、不申请号段，继续可用并保留 CACHE 属性，不为本功能重写上游 ALTER 契约。
- 开启后沿用 r4 的持久策略；重启必须保持配置一致。
- V60 本机检查与追加的 `PreInsertAutoIDCache` wire opcode 保护携带策略的 PRE_INSERT 传输：旧 decoder 走 unknown operator 拒绝；新 receiver 在构造 scope/operator 之前检查开关和版本。该标记不覆盖所有 CN/TN metadata 通道，因此不能替代全角色升级，更不是集群 admission 证明。
- 无新 goroutine、后台扫描、HAKeeper 持久状态、per-row 能力 RPC；只在 DDL/冷 cache/远端计划边界作有限检查。

运维步骤：阻止新业务请求并排空事务 → 停止所有旧 CN/TN → 升级并确认全角色版本及节点清单 → 在全部 CN 配置显式开启 → 恢复业务 → 才允许建非零 CACHE 表。节点替换必须使用同一支持版本/配置。配置开关不能替代运维对旧进程已停止的证明。

降级：存在非零表后不支持原地回退旧二进制。停写后恢复升级前备份，或执行明确的逻辑迁移方案；不能静默删除策略或倒退 allocator 高水位。关闭开关是阻止新策略使用，不是数据回滚。

验证：默认关闭/显式开启/0 控制；DDL 无半发布；旧 local/wire version fail closed；已有表冷加载禁止降为默认；全新集群、全量重启和启用后的备份/恢复。局部旧版本/关闭节点的测试预期是拒绝；不存在能自动禁止管理员在混合版本集群误配开启的协议，不声称自动驱逐旧节点。

**优点**：不把 #23371 扩成平台升级协议；默认行为不变；运行成本和实现范围小。
**代价**：明确不支持滚动升级过程中启用该功能；需要运维维护全量升级/替换边界。用户已在实现前接受这一契约变化。

### 方案 B：保留任意混合版本下的自动安全承诺

需要独立的平台能力激活/admission 协议作为本功能的前置依赖。至少闭合：

- CN、TN、HAKeeper 解码与持久状态能力证明，及旧 HAKeeper 复制节点的 barrier。
- Durable `disabled → preparing → active`、能力 generation、超时/撤销、重启恢复。
- 业务路由、直接连接、已建立连接、远端 pipeline、allocator SQL 和 TN 最终接受点；仅隐藏旧节点的发现条目不足以停止旧直连写入。
- CACHE 元数据出现前完成激活；激活后旧/未知进程重新加入也不能成功进行会忽略策略的操作。
- 在参与者不知道新协议时，明确哪个已有、可强制执行的最终边界能够 fence 它；缺少该证明不能宣称成功。

这不是简单复制 view admission。需要单独设计及兼容测试矩阵；本 issue 只接入通过评审的平台能力，不趁机重构 view 功能。

**优点**：可以保留更强的滚动升级承诺。
**代价**：跨服务持久状态、最终接受点与运维协议，明显超过当前 allocator 表选项实现，开发/验证成本高。未获授权不直接实现新状态机。

### 被否决的方案 C：只增加局部版本号或只查询一次所有节点

实现短，但会把本机/过期快照误当成集群安全证据，无法满足 r4 对旧节点静默忽略的禁止。不是可交付方案。

## 3. 当前功能证据（与门禁证明分离）

- 双 CN SQL：冷加载、CurrentValue 无预留、CREATE 起点、LIKE、ALTER 起点、COPY、TRUNCATE、临时 DDL 外层 rollback 已通过。
- mo-tester：`auto_id_cache.sql` 67 条语句，生成 `.result` 后逐项审阅，正常比较在同一任务实例连续两次 67/67；每次之间独立查询确认数据库已删除。
- 全集群关闭/重启：SHOW 保留 CACHE=1，持久下一值为 2，下一次插入确为 2；任务实例已关闭。
- schema clone/二进制恢复和 CN catalog 重建 UT 已通过。
- 这些仅证明**同版本功能行为**，不证明任意混合版本 admission 已完成。

## 4. Review 记录

- 触发：跨服务/持久 metadata/兼容契约；必须先设计评审。
- 已批准部分：r4 allocator/SQL/metadata 行为；本轮继续验证与修补。
- 当前决定：**方案 A 已批准并实现；不实现 B，也不重启被否决的 C**。
- 实现：默认关闭配置；DDL/构建 PRE_INSERT/直接 service.Create/冷 cache 分层拒绝；V60 与 wire-only opcode；既有 schema 属性仍是唯一持久所有者。
- 运维配置：
  ```toml
  [cn.auto-increment]
  enable-auto-id-cache = true
  ```
  仅在全角色升级完成后开启。仓库 `etc/launch`、multi-CN、compose BVT 样例与 shared feature-test fixture 显式开启，用于同版本新测试集群；`Config{}` 生产缺省仍关闭。
- 已验证：关闭节点的普通/LIKE CREATE 不发布残表；已有表 INSERT 拒绝且 SHOW 保真；全服务重启关闭、再次重启开启后下一 ID 仍为 2；公开 dump/load 保留策略与下一 ID。
- 最终基线 owning UT 与 incrservice/compile owning race 通过；五个精确 race 用例分别自适应重复100次通过；BVT 同实例两次67/67；变更行覆盖率250/271=92.25%（排除生成/测试）。默认一行 benchmark 均0 B/op、0 allocs/op，未见明显热路径退化。
- 实现自审 PASS。公开双 CN 含 rename/dump/load，永久重启测试 27.45s；语法/两份 protobuf 用原工具重生后的 SHA256 一致。两个批准设计文件随实现纳入 Draft PR 交付；TODO/本地验证产物不提交。交付前同步到 `3d22c694a8a3224c03a88f94327439304de23607` 并重跑 plan/compile owning 和公开 lifecycle，通过；未改变 CACHE 协议或其它已验证调用路径。
