# FIELD 类型历史设计：批准记录与实施合约

## 可追溯决定（2026-10-19）

- 完整范围：issue #25301 / PR #30096。D2/D3已在提交`f2b418d1bfe604a1825419b3abf6affe0f53d09b`发布。
- D2 SHA256：`b2df6187d3f952284c92374d38e78f30ab04ca958bb49e241bcc0d78f2c42c8c`。
- D3 SHA256：`9dd6d7ff2044cb2a9dfb9dc6da3dfe2a022717bd367433006458657a6508d33a`。
- 第一次用户`go ahead`授权继续D2验证；随后发现的错误结果和实测性能代价没有被当成默示接受。
- 本次向用户明确提出：“R1旧历史会使重试算错，建议依赖变化时返回重准备错误；R2普通32层REVERSE绑定耗时约增54%、分配增64KB，建议先优化再交付。”用户再次回复原文`go ahead`。
- 据此选择D3的**R1方案A（fail-closed）**及**R2先优化、不直接接受已测额外开销**。D2的不可变H、handle发布点、单槽cache、迁移拒绝、不增加history wire/独立quota等其余已记录取舍不变。
- 批准来源是本次交互用户，不冒充某个GitHub reviewer，也不倒签过去已有design-first批准。原D2/D3保持冻结；这份记录解释后续决定。GitHub实现review由相应reviewer独立完成，不自行dismiss CHANGES_REQUESTED。

## R1：重试代际验证

1. 新建或Reset缓存Compile时，retry capture携带当次history所依赖的不可变catalog引用事实（对象、版本、数据库/订阅、snapshot等）；不捕获可变handle来延迟取值。
2. 复用既有`checkModify`依赖验证契约；在retry重建执行计划前，以当前事务编译context解析原依赖。捕获的catalog对象/版本等代际事实变化时，保守返回既有`TxnNeedRetryWithDefChanged`错误；这也包括版本发生变化但列类型未变的DDL。解析失败直接返回错误。不存在H的路径不增加此检查。经既有`RewriteError`，客户端可见代码为20631、SQLSTATE为HY000，不是MySQL 1615，不能假定驱动自动reprepare。
3. 不把旧H用于新声明的rows，也不在retry建立第二个handle发布者。generation rebuilt先于重建回调的现有顺序保持；错误终端使frontend cache失效，下一EXECUTE按正常owner重建并发布H。
4. 无变化/历史snapshot/仅物理重试保持原历史；metadata冻结前后都不能绕过检查。代价是本次请求可能要求客户端重试，已明确选择而非宣称与MySQL自动重准备完全一致。
5. 新事实只属于当前/缓存Compile的retry capture，至多O(D)依赖数；容器/事实不与可变wrapper字段别名，不保留额外AST/Expr/值图。真正回pool清闭包后断开这些引用。不是新全局缓存或history链。

## R2：普通TEXT路径的无状态证明

- 优先优化`bindStringSourceDeclaration`的无关普通TEXT输入，避免每层都分配resolver/maps并遍历完整来源图。
- 可使用窄的、无分配、只读证明：明确TEXT声明的基础扫描列或文本字面量，经已有函数source/result contract声明为透传的函数链仍为TEXT。复用已有contract，不添加第二份函数白名单。
- 对无QueryBuilder（包括DEFAULT）、PREPARE/source binding context、参数/变量、来源witness、条件/NULLIF、隐式转换不明、窗口/聚合、投影/标量子查询/物化/关系回边等不确定情况，继续走原完整解析；不以当前payload或物理TEXT单独证明声明。TABLE_SCAN已有投影时也只接受同tag/position、无witness、TEXT的直接自引用，不能跳过投影所有者。
- 证明只沿binder构造的表达式参数树，不追ColRef投影图，不加常驻memo，不在每行执行时做图遍历。若无状态证明不能恢复性能，先给实际原因，不擅自扩展为跨绑定缓存。
- 在相同toolchain/native/SQL下比较原B、D3前H、最终实现。普通深嵌套样本要实质消除D3指出的额外分配/遍历，不以共享机器时间噪声掩盖；完整语义矩阵先于性能结论。

## R3/R4及验收

- `Compile.clear`是真正pool归还owner：清`buildPlanFunc`。prepared缓存的Release仍保留重试闭包；不依赖GC/pool Get指针巧合证明清理。
- 将确定性cleanup及DEFAULT roundtrip探针落入适当永久测试；覆盖复制/序列化后的完整声明与实际表达式结果。
- R1覆盖原错误反例、版本/对象变化、解析失败、未变化控制、metadata冻结前后、终端失效和下一次正常重建。实际公共SQL/客户端路径使用最小schema，不能把helper测试冒充完整服务竞态。
- R2覆盖基础TEXT、显式二进制、隐藏声明、prepared历史、NULLIF、关系边界与别名回边；配对benchmark/结构计数用同一逻辑选择，不降低断言。
- 所有正式变更完成后在merge后的最终源码运行focused/owning/static、生命周期race和相关公共SQL；有新增执行行时覆盖率至少75%。先复用有效证据，再补新增/失效部分。
- 没有跨版本FIELD执行器行为一致性的保证；升级须避免让受影响查询同时使用旧、新执行语义。协议字段兼容测试不能替代该部署前提。Proxy/MULTICN/旧版本重启未执行时明确不宣称通过。
