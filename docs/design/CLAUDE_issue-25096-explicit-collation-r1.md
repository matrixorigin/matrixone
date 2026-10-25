# 显式 collation 版本化执行与分阶段激活：r1

## 版本、归属与审批来源

- 设计修订：**r1（2026-10-17）**；公开文档版本：**r1-public.2（2026-10-25）**（r1-public.1于2026-10-22公开）。本文件是该本地设计的公开转录与实现期记录，不把发布日期冒充设计先行日期，也不把本地r1授权冒充对公开转录的维护者审批。
- 所属：[问题 #25096](https://github.com/matrixorigin/matrixone/issues/25096)、[实现 PR #30684](https://github.com/matrixorigin/matrixone/pull/30684)。
- **本地用户范围授权**：2026-10-17，用户在本地开发会话对r1方案回复“可以”，接受A→B→C分阶段实现、首批native索引拒绝、不切换默认的取舍。该决定在当日工作记录及本地设计中先于实施记录。此处为授权来源的转录，不是新授予的审批。
- **公开审批状态**：没有可链接的GitHub maintainer设计批准记录，不能从本地用户授权、代理自审、测试成功或本文件推导maintainer已批准。没有可公开核验的原始会话链接或设计批准commit，本文件也不能替代它们。初次公开时实现PR为Draft；当前保留远端Ready状态，但它不构成设计批准。P1仍待维护者明确确认r1及当前native132集成记录，不自行标记已关闭。
- 本地授权不授权全局默认切换、native物理索引、在线迁移或整个#25096完成。下文技术范围以r1为准；实现期协议号N由未占用的append-only能力决定，不预占号码。

## 1. 问题与目标

旧实现接受显式`utf8mb4_general_ci`/`utf8mb4_bin`，却在revision0下继续执行字节语义。只改`=`或binder会让IN、hash、排序、剪枝及持久重绑定产生不同身份；直接改变revision0又会重解释旧对象和物理key。

固定域的MySQL对照而非不同session默认是oracle：general-ci大小写/重音不敏感、PAD SPACE；传统文本bin大小写敏感、PAD SPACE；binary原始字节不忽略尾空格。0900默认为NO PAD，不能据此声称所有MySQL默认尾空格相等。

目标是新的**显式**表达式、列、表默认能请求revision1，并覆盖其可达执行与恢复消费者；旧revision0、未写COLLATE的默认、旧索引和旧SQL重绑定保持历史语义。

非目标：默认/schema/session/handshake整体切换、完整LIKE/pattern（#27216）、严格utf8mb3三字节repertoire、新权重表、native物理索引/FK、native持久计算、在线native relabel、无来源的独立native SQL导出。现有general-ci/bin的utf8/utf8mb3→utf8mb4兼容拼写保持；已有UCA `utf8_unicode_ci`的严格repertoire与物理格式不变，并非将所有utf8 collation无条件映射为utf8mb4。未版本化routine body保留历史绑定，不扩展routine功能。

## 2. 不变量

1. 执行域由完整Type的物理字符串类型、collation identity和semantic revision确定，不能只看charset或从权重bytes猜测。
2. 同域的`Compare(a,b)==0`与规范化hash-key相等一致，排序/peer/shuffle也使用同域；NULL维持外层三值逻辑。
3. 原字符串bytes不替换成权重。不同revision的同OID转换必须是真实cast，不重标共享列/表达式节点。
4. 显式强域可以转换弱legacy操作数；同级不兼容域明确拒绝，不用binary-wins或“较新revision胜出”掩盖冲突。
5. codec合法性、可执行性、持久shape、reader、writer和physical-key准入分离。允许SELECT不意味着允许索引或持久计算。
6. 不支持的边界在资源/副作用/发布前拒绝，或使用已证明正确的保守扫描；绝不退回错误字节语义。
7. pre-fold来源义务不因结果变成bool、literal、旧形状Type或经View/CTAS展开而消失。

## 3. 备选方案与选择

| 方案 | 正确性与成本 | 决策 |
|---|---|---|
| 改revision0或把所有general-ci加入UCA-only判定 | 重解释历史数据/key，错误扩大物理能力 | 拒绝 |
| 只放开COLLATE或只修`=` | IN/hash/order/cast/pruning不闭合 | 拒绝 |
| 一次实现所有索引、迁移及默认切换 | 可成为后续目标，但超出已授权产品决策与恢复范围 | 不纳入本轮 |
| 完整Type共同执行owner，加独立的分阶段admission | 复用冻结权重与现有生命周期；旧域不变；不支持的native物理路径可明确拒绝 | 采用 |

不新增平行权重registry、全局cache、后台worker或第二套hash框架。

## 4. 执行闭包与所有权

`COLLATE/DDL来源 → 完整Type/coercibility → cast/派生/fold → compare/IN/hash/order/agg/shuffle → transport/恢复`。

- `CollationDomainForType`是域解析owner。binary/opaque保留raw；revision0不升级；revision1 native general-ci/bin复用冻结backend；UCA400身份/物理格式保持独立。
- scalar/vector/list/参数/cache必须携带同一完整域；同OID也不能丢revision。CASE未选分支不提前执行，参数执行不改共享声明。
- keycodec估长和编码一致；比较、分组、DISTINCT、join、MIN/MAX和窗口peer不依赖不同的charset-only规则。
- raw index/zone-map/Bloom/runtime-filter不能证明native谓词时禁用该shortcut，保留扫描正确性。新域表达式不能匹配旧byte索引的函数树。
- physical helper在空/NULL/输出/packer分配shortcut之前校验全部输入Type；Prepare/Call、错误、Reset/Free及下一次legacy调用保留原owner与mpool归零。
- 一个完整owner遍历同时识别Type与独立表达式能力，覆盖plan/pipeline/aggregate声明、嵌套诊断容器及生成schema字段；不只检查特定overload或最终结果OID。

## 5. 分阶段激活与协议

| 阶段 | 允许范围 | 启用门 |
|---|---|---|
| A | 完整Type共同执行基础，生产新域仍关闭 | 原值、权重、consumer、清理和legacy/UCA回归 |
| B | 新显式执行表达式 | 独立能力N、source placement/实际destination/receiver检查及prepared/两CN证据 |
| C | 新显式列/表默认和已闭合的View/CTAS/typed恢复 | pre-fold来源、reader/writer与catalog fence、真实恢复/重启、公开SQL证据；未闭合shape仍拒绝 |

普通显式请求默认revision1；明确的持久来源0/1优先，未知marker拒绝。这个默认只属于显式入口，不是session或database默认切换。

能力N与上游独立功能取max，不覆盖其他号。CTAS字段19是执行义务，20是绑定代际，21是native来源/作者义务；外层generation0可以因native来源仍要求N，临时时间函数不能变成native作者门。

## 6. 持久来源、恢复与回滚

- ViewData缺失/0 marker一律历史重绑定；新native来源显式保存revision1和最低protocol。嵌套View独立作用域恢复account、SQLMode、默认库与来源；reader许可不能借给writer。
- bool/literal折叠前捕获依赖；PREPARE/缓存、深拷贝、wire往返和实际DDL执行都保留。DEFAULT/generated/CHECK/函数索引等未实现持久native契约的入口继续拒绝，即使输出为numeric/legacy。
- 新无索引列保存原bytes与版本化metadata，不新增物理key格式。native PK/UNIQUE/二级/函数索引及FK、在线revision转换在产生目标副作用前拒绝；int/binary/legacy索引和已有UCA能力保持可用。
- ALTER/COPY/DROP/RENAME/TRUNCATE/clone在resolved source/target、等待后和发布前的既有owner处重查，不改变事务/锁/取消顺序，不承诺新的原子floor切换协议。
- CONVERT的合法性归一化不能冒充新作者请求：显式目标保留validated原拼写，charset-only仍是legacy默认，preflight与MODIFY replay使用同一目标域；不以全局generation0或重写源表达式来规避native relabel门。
- snapshot/clone/PITR保留完整typed表元数据；View排序与最终CREATE都保留来源。私有schema inspection只提供依赖信息，不授予权限或writer能力。
- 独立SQL无法表达native来源时拒绝导出。MO的legacy兼容拼写、外部MySQL固定canonical spelling及manifest hash是不同契约；不擅自把后两者改成MO alias。
- HAKeeper已有admission/read/authoring floor和catalog fence负责排除旧CN。提交不兼容floor后不能只关开关直接降级；需要受支持的移除/转换及集群兼容流程。本设计不保证未发布草稿能力号之间的实验目录升级。

## 7. 生命周期、成本与安全

新增状态限于plan局部捕获、不可变context标记和遍历局部seen集合；没有新锁/channel/队列/worker/后台状态。原DDL/RPC/batch/aggregate的取消、释放、事务abort与Reset/Free owner继续负责成功和不快乐路径。

- raw/legacy比较与key快路径目标0新增heap分配/行，不新增I/O、锁或逐行日志。
- general-ci/bin key payload沿用`4*inputBytes+1`边界，执行envelope另计1字节；checked arithmetic先于分配。UCA已有上界不被错误替换。
- scratch局部复用、不与源bytes重叠；retained key由现有算子预算计费，不增加无界cache或按声明宽度填充。
- 同模式legacy key/compare中位退化预算5%；remote transport另记录实际成本，不承诺全链零回归。非法UTF-8、OOM、部分批次失败不泄漏临时vector/packer。
- 沿用租户/权限/事务边界；来源context不跨租户共享，不授予额外SQL执行能力。

## 8. 验证与启用判据

| 契约 | 必须的证据 |
|---|---|
| 旧0/新1/unknown与同OID cast | 最小UT、原Type/bytes不变、native拒绝正负控制 |
| compare/hash/order/peer一致 | 固定域MySQL oracle、最小case/accent/PAD/NUL/NULL与非法UTF-8，完整owner消费者 |
| 远端与持久兼容 | 独立能力与混合max；source/destination/receiver/batch前拒绝；真实两CN、旧source与实际old-reader证据分别记录 |
| View/CTAS/DDL来源 | pre-fold bool、prepared、wire、reader/writer独立、未知来源拒绝、错误后source/catalog不变 |
| 恢复与公开行为 | clean实例的snapshot/clone/PITR/account/restart/default继承；正式mo-tester生成后独立审核，同实例两轮和catalog/owned清理 |
| 成本与资源 | 匹配模式benchmark、mpool/Reset/Free/重用、分配边界；不以sleep或大数据撞概率代替控制 |

启用需要对应阶段证据，不以设计授权、CI绿色或旧基础PR合并代替。历史证据保持原source/protocol身份，新的依赖变化按语义有效性重新验证；未完成门明确标注pending。

## 9. 与基础设计的关系及实现期记录

[权重key基础设计](issue-28164-weight-key-v1.md)、[collation选择基础设计](issue-28164-collation-selection.md)和[metadata设计](CLAUDE_issue-29480-collation-metadata.md)不能作为生产激活审批。本r1明确提出B/C激活及其兼容/恢复条件，是单独的产品范围决定，不回改基础设计的禁用声明。

2026-10-22实现记录（非2026-10-17批准内容的伪造快照）：上游已占用125=LOAD FK、126=View metadata、127=vector knobs、128=mixed JSON/numeric，因此当前N=129；字段19/20/21保持独立。旧125草稿验证仅为历史证据，未发布native125实验目录不作为129受支持升级路径。PR正文按source区分验证；同名sys/tenant snapshot identity的上游问题、自动View metadata refresh被上游关闭、完整pattern范围等限制继续公开。

### 可追溯决策记录

| 时间/修订 | 来源 | 决定及边界 |
|---|---|---|
| 2026-10-17 / r1 | 本地用户会话；原话“可以”；当日工作记录在实施前记录 | 允许A→B→C和首批native物理拒绝，不批准默认切换 |
| 2026-10-22 / r1公开转录 | PR #30684的P1评审要求 | 将设计和授权来源公开；不伪造maintainer或GitHub批准 |
| 待定 | GitHub维护者设计评审 | P1公开设计审批尚未获得，仍需显式确认r1及当前native132集成记录；Ready状态不关闭该门 |

## 10. 2026-10-23兼容集成记录（不改变r1范围）

上游`main@2b388dd1690f`已将129分配给JSON ELT/opaque text独立执行契约，本PR未发布的native能力因此迁到**130**，不能覆盖JSON129，也不改变125–128等其他能力。CTAS字段19/20/21仍独立，130使用两字节varint `0x82,0x01`。过去129及125候选的测试与目录保持原身份，不宣称未发布native129/125实验catalog到130的受支持升级。

合并保留JSON typed MIN/MAX及其extra状态所有权，native blob/text仍使用完整域；共享PK/cluster-key读取准入也必须看完整identity/revision，不能因只检查Unicode ID而重新启用native PAD的raw-key剪枝。没有启用新的native物理key或在线转换。

本轮开始时远端PR已为Ready，冲突修复保留远端状态；Ready状态本身不作为新的设计批准记录，不从状态变化推导维护者已批准r1。

## 11. 2026-10-25兼容集成记录（r1-public.2，不改变r1范围）

上游`main@9a7e12a93328`包含已占用的130=incomplete temporal strings和131=exact weighted percentile dispatch；本PR未发布native能力迁到独立的**132**，JSON129、temporal130、weighted131均保持自己的最低版本。CTAS字段21的132编码为`0x84,0x01`；字段19/20以及旧目录的身份不随之改写。125/129/130等未发布native实验catalog不作为132受支持升级来源，历史验证也不作为本轮验证。

自动合并的raw-string hash快路径必须按完整Type决定是否需要collation key，而非仅按Unicode charset分类；native general-ci/bin不能绕过PAD/case canonicalization。远程编码须将native132计入绑定stream的最低版本，与weighted131等独立能力取max，并保留调用者已给出的更高floor；placement probe不能代替实际stream的版本确认。两项均是既有完整域与独立准入契约的合并修复，不扩大SQL或物理存储范围。

本次CI夹具修复仅将新增DUMP/LOAD正例的两个generated目标列声明改为历史bin别名，保留JSON null、二进制HEX、NOT NULL、恢复后写入/更新及约束检查，不放开native持久计算或standalone SQL export。此前针对native130版本的设计评审请求不自动覆盖132；本版本继续明确等待可追溯的维护者决定，不据CI修复或协议重分配自行关闭P1。

参考规范：[MySQL coercibility](https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html)、[binary与_bin](https://dev.mysql.com/doc/refman/8.0/en/charset-binary-collations.html)。严格域兼容与历史MO别名是不同承诺，本设计不把alias宣传成完整MySQL字符集实现。
