# #29004 Review 修订：仲裁、证据与恢复合同

状态：Draft，待审查。回应 review 5232882733（基于 d6ac371788）。本文优先于上一版补充设计的候选方案；不代表实现已完成或设计已获批。

## A. 成员仲裁：能力检查不能代替升级栅栏

### A.1 两个不同的安全问题

1. 升级前已经投递的旧命令：不携带 reservation，新 RSM 无法证明其已失效。
2. 全部 executor 升级后的新命令：可以强制使用 replicated reservation。

不能用第二项的解决方案宣称解决了第一项。当前没有足够证据证明现有系统支持无停机切换到新仲裁。

**本次选择的保守合同**：允许滚动部署新 binary，但首次启用新 entry/reservation 协议需要一次受控维护切换。混合版本自动激活不支持。这个约束改变了原计划的上线能力，必须由设计 reviewer 明确接受；若 #29004 必须提供无停机首次激活，则此方案不能通过，不能以文档修改冒充完成。

维护切换步骤：

1. 外部部署 owner 关闭 scheduler/人工 membership mutation，停止所有可能执行 HAKeeper add/start 的旧进程，禁止旧 binary 重启。仅失去心跳不算停止证明。
2. 停止所有 HAKeeper replica进程，随后只用新 binary 恢复现有 membership；不加入新成员。旧进程内存中的 response/command 不得恢复为可执行 work。
3. 等待 Raft 完成恢复并取得线性一致 membership/config-change-index。崩溃前成功提交的 config entry会反映在恢复结果中；未提交的操作仍由 Raft日志处理，不能通过缓存 checker state推测结果。
4. 所有重启后的 executor默认拒绝无 reservation 的 HAKeeper add/start，包括旧 durable schedule队列恢复出的命令。调度 owner基于恢复后的 authoritative membership重新生成命令，而不是重新授权旧 epoch命令。
5. 在外部旧 binary启动禁令仍生效时，检查全部 voting/non-voting replica的当前 incarnation capability，启用新的仲裁模式。此后才能提交首次新 barrier entry。任一步失败保持维护状态，不回退到无reservation执行模式。

这里的进程停止证明及启动禁令属于部署控制面的信任前提，不是 SQL/RSM boolean。当前仓库尚无该维护流程实现，因此不能在 #29004 中悄悄添加自动 Begin 开关。

### A.2 升级后共同线性化点

HAKeeper RSM持有唯一 `MembershipArbitration` owner。状态至少包括 `Mode`、`NextOperationID`、单个 `Reservation` 和 `BarrierSubmissionFence`。**Reservation只用于Raft membership mutation（add/remove voting或non-voting），不用于本地StartReplica。** reservation绑定 operation ID、当前 config-change-index、完整目标 membership摘要、目标UUID/replica ID/incarnation、action。本地start使用A.4的独立permit。

- `ReserveAdmission` 与 `FenceBarrierSubmission` 是同一 RSM上的互斥转移。后者只在无 reservation时成功；前者在 fence存在时拒绝。这两个 entry 的 apply顺序是共同线性化点。
- fence提交后重新做线性一致 membership读取和 incarnation能力检查。检查通过才提交 Begin；失败保留decoder floor，显式释放submission fence，但不撤销已经持久化的protocol floor。
- executor调用Dragonboat add/remove前必须获得匹配reservation；本地start前必须获得A.4的permit。无对应token的调用拒绝；普通数据shard不受此新合同影响。
- reservation存在期间不得推进首次Begin，也不得授权另一个membership mutation，包括remove操作，避免结果归属歧义。
- token仅允许一个指定的mutation及expected config-change-index。重试不分配新token，不刷新expected index；旧token不能授权新目标。
- capability只证明指定incarnation；replacement需要独立reservation。外部进程启动门禁必须继续禁止低于durable decoder floor的binary使用既有replica身份启动。

### A.3 Membership mutation的未知结果与恢复

本节仅适用于改变membership的add/remove，不适用于StartReplica。RPC success、timeout或executor ack都不直接删除reservation。reconciler从线性一致Raft membership取得结果：

| 观察 | 行为 |
|---|---|
| index已推进且目标membership符合预期 | 标记APPLIED；旧index命令不能再次改变membership；释放reservation |
| index已推进但membership不符 | 标记CONFLICT，封闭调度，要求重新核对；不得自动改写token |
| index未推进 | 结果可能尚未提交；保留reservation，允许相同token重试 |
| 无法读取/leader变化 | 保留reservation，重试只做读取或相同token操作 |
| executor崩溃 | 新executor接管同一durable token；不能因旧进程失联清理 |
| 请求取消/超时 | 只结束调用者等待，不撤销Raft操作、不释放reservation |

不提供未经证明的自动abort。无法确认结果时允许阻塞membership/首次activation，普通SQL不受影响。活性依赖quorum恢复及目标操作最终得到确定结果；运维不能靠清空map解除阻塞。确需取消未决操作时，必须用维护切换停掉所有executor后核对Raft结果，不能实现TTL清理。

### A.4 Local start：独立permit，不等待membership index推进

StartReplica只启动已经admitted的本地replica，不是membership mutation。RSM核对authoritative membership确实包含该UUID/replica ID后签发 `StartPermit(PermitID, UUID, ReplicaID, StoreIncarnation, DecoderFloor)`。Bootstrap初始建群不通过这个已admitted成员入口；首次协议启用必须在bootstrap完成后。

- permit每个当前member最多一个pending项，签发时验证对应incarnation capability符合durable floor。pending start期间禁止首次submission fence及该member的remove/replacement；不占用全局mutation reservation，但两者的冲突检查由同一RSM执行。
- executor的本地supervisor必须持久保存permit high-water及STARTING/STARTED/REVOKED状态。启动和撤销在该supervisor中串行执行；只有`StartReplica`返回并核实本地实际shard/replica身份后才持久记录STARTED。
- supervisor将绑定PermitID/incarnation的STARTED结果重发给RSM；RSM校验pending permit精确匹配后，将其转成当前member的有界start证明并清除pending。**不要求membership/config-change-index改变。** 正常start完成后允许新的admission。
- RPC timeout不等于启动失败。executor或RSM leader重启后，使用同一permit查询supervisor的durable状态及实际本地replica；已经启动则补发STARTED，尚未启动则重试同一permit，不新分配token。
- 取消需supervisor持久记录REVOKED，并确认没有该permit的启动正在执行；若已启动，先StopReplica成功，再记录REVOKED。RSM收到精确token的撤销证明才清pending。迟到start被supervisor的high-water/tombstone拒绝；不能仅靠调用前远程读取permit来避免TOCTOU。
- supervisor永久丢失时不能清pending。需A.1的部署停止/旧incarnation禁止重启证明才能退休旧permit。新incarnation不能替旧incarnation发送成功ack。
- high-water及当前状态每个当前/未退休member只存一项，旧permit号不可复用。member退休后可清本地记录，但必须先禁止其旧incarnation重新执行；不能凭清本地文件重新取得旧身份。

该supervisor持久执行合同是新增实现要求，不声称当前StartReplica wrapper已经提供。不存在该owner时生产start-permit路径保持不可达。

### A.5 BarrierSubmissionFence：完整终止与接管合同

RSM保存单个 `Fence(Token, OwnerIncarnation, ExpectedPhase, E, R)` 以及全局单调`NextOperationID`。token与reservation/permit同源分配，永不复用；溢出拒绝新操作。fence token不是leader term，leader变化不会隐式释放它。

1. **Acquire**：CAS无reservation、无pending start且无fence，记录expected phase/E/R。已识别的新协议entry依C.2持久提高decoder floor，不能等到Begin成功才保护后续admission。
2. **Begin/consume**：请求必须携带完整fence token、owner及expected tuple。RSM再次检查成员/能力条件；成功推进PREPARING和消费fence在同一次apply中完成。消费后迟到同token请求只能返回ALREADY_CONSUMED或STALE，不能再次递增E/R。
3. **业务拒绝**：保留fence及floor，由owner修正证据后重试或显式Release；不因一次checker read失败开放membership。
4. **Release**：仅接受当前token/owner的CAS，在一次apply中清除fence。随后延迟Begin因token不存在/不匹配拒绝；先后顺序由Raft apply确定。若Begin已先成功，则Release不能撤销phase。release不降低floor。
5. **Takeover**：经认证的控制面新owner以当前token CAS接管，分配更大的token并保留冻结状态；不依赖旧owner存活或主动释放。旧owner的Begin/Release从此STALE。新owner必须重新取得线性一致membership和当前incarnation能力证据，不能继承旧checker read。
6. **崩溃恢复**：fence、next ID、最后一个consumed token及结果进入同一snapshot。proposer死在Acquire后，由successor执行Takeover再Begin或Release；死在Begin提交后通过readback确认结果，不重新Begin。

仅保留最后一次consumed token用于精确幂等响应；更老token返回STALE并要求readback，不保存无限历史。请求处理遇到旧token必须先拒绝，不能因为phase恰好又回到PREPARING就重放旧操作。Release后不要求保留每个tombstone，单调计数器+当前fence精确匹配即可拒绝旧token。

## B. Catalog证据：提交后outbox与claim身份

### B.1 Owner及可信边界

HAKeeper拥有E/R与transition，catalog transaction owner拥有marker/claim/completion。RSM不读取数据库、不接受普通CN heartbeat的completion声明。

新增内部proposal入口只接受经认证的catalog recovery coordinator；普通SQL用户及普通heartbeat不能调用。该coordinator属于可信集群控制面，不提供拜占庭容错。未经身份验证的任意RPC携带transaction ID不构成证据。

每个catalog marker记录 `(E, R, ClaimID, Kind, Sequence)`，证据outbox与对应业务修改在同一事务提交；dispatcher只能读已提交outbox。不能先发proposal再提交catalog事务。

### B.2 精确流转

1. SEALED以后，coordinator获得当前E/R，事务锁定catalog barrier row，校验其generation不会倒退，写required marker和对应outbox。提交成功后发送 `CatalogRequired(E,R,Sequence)`。
2. RSM验证当前phase=SEALED、E/R精确匹配、发送者角色合法、sequence未处理；推进CATALOG_REQUIRED。超时重发同一evidence identity。
3. RSM分配单调、不可复用的ClaimID（绑定当前E/R）。catalog owner在事务中CAS claim row，写claim及outbox；提交后发送 `RecoveryStarted(E,R,ClaimID,Sequence)`。旧claim不能覆盖较新的claim。
4. worker每次catalog写入都在同一事务中校验claim row仍匹配；final transaction在锁定该row时验证所有恢复条件、写completion marker和outbox。提交后才能发送 `Complete(E,R,ClaimID,Sequence)`。
5. RSM只接受当前E/R/ClaimID且phase=RECOVERING的completion，再检查member/seal gates；成功才设置C=R和ACTIVATED。

supersession先在RSM推进E/R，立即禁止旧completion授权。新catalog worker只有在安装新claim后才能写；旧worker若在安装前提交，仍属于旧generation，不能开启新generation；安装后被事务CAS拒绝。public authority必须依据当前RSM E/R与catalog generation共同验证，不能仅凭某条旧completion row开放。

同一evidence重试幂等；相同identity但不同内容为错误；future E/R、旧claim、未知kind、未认证sender全部拒绝。catalog commit成功后publisher崩溃，由durable outbox重发。RSM已接受但响应丢失，重发读到相同结果。没有outbox/owner实现时该生产transition不可达，不通过test-only fixture偷渡成产品入口。

这些wire/认证/outbox/claim owner属于#29005接口合同；#29004仅实现consumer时不能宣称跨服务集成已验证。

### B.3 Evidence清理、去重与容量

证据不是永久事件日志。每个协调范围只允许一个当前E/R/ClaimID；RSM保存三个阶段的固定receipt槽（required/started/complete）、当前claim及一个退休watermark，不保存逐generation历史。Sequence是该阶段的固定序号；同一identity不同内容必须拒绝，不生成新Sequence规避错误。

清理握手：

1. catalog事务提交outbox，publisher按同一identity发送。
2. RSM apply后返回 `ACCEPTED(identity,digest,result)`；该结果必须来自已提交状态，不能在propose之前或仅进入queue时返回。
3. publisher收到ACCEPTED，或通过线性一致readback得到相同receipt，才能事务删除对应outbox。删除失败重试；删除前崩溃重发仍幂等。
4. supersession使旧E/R/claim的证据永久不可授权后，RSM返回 `RETIRED(identity,currentWatermark)`。publisher验证watermark严格越过该证据后事务删除outbox；不能因timeout/普通STALE字符串删除。
5. 当前tuple下身份冲突或future证据不能删除为“已处理”；保留一个有界失败槽，停止该scope生产并告警，等待修复，不持续生成相同失败事件。

retirement watermark由单调E/R和不可复用ClaimID构成，随snapshot保存。claim replacement必须先在RSM提升claim ID并使旧claim永久无效，再由catalog CAS安装；旧claim outbox于是可获得RETIRED。恢复不得回退watermark后直接开放producer，必须完成committed log replay。已接受但receipt槽被新generation替换的重试，用watermark返回RETIRED，不需要保留旧payload。

容量是提交前的硬门禁：每个scope最多3条未确认outbox、每条编码后最多4 KiB；控制面部署固定全局上限1024条/4 MiB（两者分别计数），最多一个活动claim。额度计数和outbox插入在同一catalog事务中完成；相同identity重试不重复扣额度。超限回滚该控制事务并返回明确backpressure，不提交业务marker后丢掉outbox，不影响普通SQL事务。

supersession若旧outbox占满scope额度，先通过RETIRED握手回收；否则新marker事务等待/拒绝，不绕过额度另建scope。同一scope身份必须稳定，不能每generation新建quota桶。RSM receipt为固定3槽，publisher每scope最多一个in-flight RPC；全局dispatcher最多16个in-flight，失败采用有上限退避并受context取消，绝不把durable backlog全部读入内存。

required/completion marker不是outbox：每scope覆盖保存当前generation的固定row，历史generation不无限追加。旧claim/recovery工作数据只有在worker事务CAS已被新claim fencing后才允许分批清理；每批有固定条数上限，尚未清理的数据计入catalog recovery预算。#29005必须实现并验证这项预算，不能通过提前删marker释放额度。

retirement record每UUID仅保留当前/尚未退休owner；老record清理后依单调身份allocator及部署旧incarnation禁令拒绝复用，不保留每代tombstone。证据服务不可用时积压最多达到上述上限，随后暂停新的metadata lifecycle工作；不设置丢弃安全证据的TTL。

### B.4 Authority退休及背压

不把heartbeat timeout/store deletion当作退休证明。选择显式drain：issuer先在durable issuance owner中停止为旧E签发新的authority，旧进程关闭该E的新metadata response/COMMIT授权入口并排空已经进入的授权操作，之后才发送绑定generation/E/R的seal ack。终止点定义为authorization linearization point，不承诺网络中已经发送的字节被收回。

generation替换不能替旧owner ack。每个UUID最多一个未退休owner；replacement可以运行普通SQL，但在旧owner退休前不得获得新的metadata authority。无权签发者不加入旧-owner target集合。由此不累积无界replacement历史。

故障进程没有ack时，选安全优先：保持SEALED并对该UUID的metadata authority背压。人工恢复需要受信部署owner确认旧进程已终止且不能重启，再由issuer发布durable retirement record；不能由heartbeat失联推导。普通SQL继续运行。若要求自动故障恢复，需要另行设计带明确时钟假设的bounded lease；本文不臆造现有系统没有的lease到期证明。

上述issuer/drain/retirement实现属于#29006；#29004不签发authority，MetadataReadsEnabled固定false。

## C. Snapshot及entry接受矩阵

### C.1 定义

- F：新增durable decoder floor，当前支持0/1；大于1拒绝恢复。
- P：phase，仍遵守既有phase/E/R/C/protocol校验。
- V：runtime evidence schema version，0为旧格式，1为本文格式。
- I：evidence initialized；I=false时targets必须为空，所有runtime推进拒绝。
- B0：expression floor非零；B1：P非DISABLED；B2：F=1。
- V=1可以是未初始化的封闭状态，不以空targets自动推导ready。

required features必须精确等于B0|B1|B2，无多余已知bit，也不能缺少任何bit。未知bit先于RSM payload decode拒绝；envelope version仍为1。

| F | P | V/I/targets | 接受/运行行为 |
|---|---|---|---|
| 0 | DISABLED | V=0,I=false,无targets | 旧raw/MOH2正常；按原规则允许精确features的MOH3 |
| 0 | 非DISABLED | V=0,I=false,无targets | 兼容#29003 MOH3；仅恢复，不允许runtime推进/签发authority |
| 0 | 任意 | V=1或任何新evidence | 拒绝：缺少decoder floor |
| 1 | DISABLED | V=1,I=false,无targets | 合法；MOH3+B2；覆盖业务拒绝的Begin |
| 1 | DISABLED | I=true或有targets | 拒绝：未开始却有active evidence |
| 1 | 非DISABLED | V=1,I=false,无targets | 合法封闭状态；不得推进，适用于旧state仅提高decoder floor |
| 1 | 非DISABLED | V=1,I=true,合法targets | 合法；I=true的空集必须来自原子捕获，不能由decoder补齐 |
| 1 | 任意 | V=0或未知V | 拒绝 |
| >1 | 任意 | 任意 | 拒绝：不支持的decoder floor |

targets含重复身份、零generation、未知service type、不匹配E/R的ack、future phase ack均拒绝。完整新runtime evidence还包含claim/outbox消费identity与仲裁state；不得只序列化targets却丢失这些恢复条件。

F=1强制MOH3，即使P=DISABLED。raw/MOH2携带F=1拒绝。旧decoder不支持B2，必须fail closed。F=0与非DISABLED的旧MOH3只允许只读恢复/受控升级，不能默认为member gates已成立。

### C.2 Entry语义及拒绝的Begin

新tag是decoder义务：**只要识别到新tag，就持久记录F=1，再解析payload**。这是对前稿“只有合法entry才提高floor”的修正；否则malformed entry已经进入日志仍可能被降级binary回放。

- 长度不足以读取tag：沿用现有外层entry framing错误处理，不解释为barrier entry。
- 可识别的新tag但payload损坏、未知request version/action、业务条件失败：F提高到1，初始化V=1/I=false（若之前无runtime evidence）；P/E/R/C及既有targets/claim保持不变，返回确定性的拒绝结果。
- request声明未知floor/version不能把F任意提高：当前tag的decoder最低义务固定为1；未知版本返回unsupported，不接受其desired state。
- duplicate valid request不得再次增加E/R；同一结果由expected tuple与保存的accepted operation identity判断。
- 未知tag仍沿用unknown-tag处理，不猜测其协议floor。

因此“拒绝原子性”是业务状态不变，不是整个RSM字节不变。测试分别比较index、F/V以及独立复制的P/E/R/C/targets/claim。SaveSnapshot在拒绝后也必须输出MOH3+B2；恢复后不能回到旧decoder可加入的状态。

F=0旧非DISABLEDstate首次遇到新tag时可以升级成F=1/V=1/I=false，但不生成ready。重新捕获必须经显式维护/owner退休证明，不能用decode过程自动重启barrier。

## D. 对应验证与交付状态

- 成员调度：用阻塞点复现投递后未执行、Begin争用reservation；executor崩溃/timeout/leader change后reservation不得消失；旧token在index推进后拒绝。
- Local start：成功start且config index不变时清pending，随后新admission成功；STARTED丢ack/restart补发，cancel与start交错，旧permit迟到、incarnation替换不得错误清pending。
- Submission fence：Acquire后死亡、Begin提交但响应丢失、Takeover与旧Begin/Release交错、Release后延迟Begin、snapshot恢复接管、token overflow与幂等receipt有界性。
- Evidence容量：提交后/ACCEPTED后/删除前各崩溃点；superseded outbox获RETIRED后回收；当前identity冲突不得删除；连续generation运行中receipt恒定3槽、outbox条数/字节不越界，满额时marker事务原子回滚。
- 升级：维护切换前无自动Begin；新executor拒绝旧无token命令；部署owner未提供停止/启动禁令证明时不启用新协议。
- Catalog：outbox提交前不发送、提交后崩溃可重放、重复幂等、旧E/R/claim拒绝、完成响应丢失重试；事务CAS阻止旧worker写入。
- Authority：g2不替g1退休，delete/timeout不完成seal；backpressure不影响普通SQL；无issuer时没有生产authority。
- Snapshot：上述矩阵全组合、malformed/unknown-version新tag、rejected Begin立即snapshot/recover、旧MOH3缺targets封闭恢复；所有拒绝使用独立oracle。

当前只有文档修订，没有runtime实现/测试结果。C节补齐了P2所需的明确接受矩阵。A/B节给出可审查的保守决策，但带来维护切换及显式退休的活性取舍；P1是否可关闭取决于reviewer是否接受这些需求约束，不能宣称已证明无停机升级或自动故障恢复。
