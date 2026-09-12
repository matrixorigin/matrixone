# REGEXP r1 设计决定与实现对照

## 独立设计审批

- 日期：2026-09-12。
- 设计：[r1 固定版本](https://github.com/ck89119/matrixone/blob/931f646eb986221e34285e14f35ce85dc515e2a0/docs/design/CLAUDE_REGEXP_DOMAINS_27217_r1.md)。
- 文档内容 SHA-256：`bdb3dc057ebf620b17b899dd39b5eeff51e9303b9e9d097ba194b231ba3d1ba2`。
- 审批来源：项目用户在开发会话中明确回复「确认 r1 设计」，随后回复「go ahead」授权交付。
- 决定：r1 设计批准。接受文档所述局部四域分离、现有资源限制、同版本发布约束及物化来源/大文本元数据排除项。
- 本记录由开发助手转录用户决定，不是助手自行审批，也不代表 GitHub reviewer XuPeng-SH 已撤销 CHANGES_REQUESTED。
- 原文保留历史的“待审批”状态；本记录是后续独立设计决定，不倒签成实现之前的审批。

## 实现对应关系

对照范围：`4f9d170a22deaabc2eee6b239a51a8cb40a39f9f...e073823f44`，16个实现/测试文件；本文及r1为新增文档。

| 设计契约 | 实现对应 |
|---|---|
| 静态来源与兼容性分离 | base_binder.go 的 regexpStaticOperandCheckMode；function.go 的 StringDomainCheck 模式；type_check.go 的 REGEXP 兼容检查 |
| marker 元数据稳定、局部来源恢复 | function.go 仅重算返回类型；visit_plan_rule.go 深拷贝 REGEXP 参数后恢复 SQL/COM_STMT 来源，不改写共享参数 |
| 独立解码与返回编码 | regexp_operands.go 的 regexpStringParameter、regexpTextPrefix、regexpResultUsesBinary、regexpEncodeResult |
| 非法文本3854 | error.go 的错误映射；regexpTextPrefix 的输入宽度/完整非法序列区分与六字节格式化 |
| NULL 与位置顺序 | func_builtin_regexp.go 的 SUBSTR NULL 分支及核心路径；REPLACE 独立顺序；INSTR suffix 路径 |
| SUBSTR 正常行不重复预验证 | arity3/4 只在存在NULL时调用 validateRegexpSubstrRow；全非NULL直接进入核心执行 |
| 可观察行为 | frontend computation_wrapper、三个 REGEXP function 测试文件及 func_builtin_regexp_test、真实协议 regexp_sources_test |
| 公共SQL与会话恢复 | func_regexp_sources.sql/result、func_regular_instr.test/result |

这些对应关系未发现需要改变r1契约的实现偏差；这是设计对照，不是完整运行时正确性或性能重新验收。未修改生产代码、测试或生成文件。

## 证据边界与剩余验收

- 相比已验证的826a355564，核心 func_builtin_regexp.go、regexp_operands.go、真实协议 regexp_sources_test.go 内容未变；历史测试和benchmark仍可说明当时实现，不直接证明新合并版本。
- 上游合并修改了binder/frontend等依赖，不能将历史 package/COM_STMT/BVT通过或91.14%覆盖率冒充当前head结果。
- 本次设计文档交付只做内容哈希核验、版本链接及diff检查，不重跑无关native suite。当前head运行验收需依据有效CI/定向验证另行确认。
- 无新增wire、catalog或存储格式；混合CN语义一致性未获证明，r1明确要求同版本发布隔离。
- 当前关闭的是“没有版本化设计及独立用户审批记录”这一材料缺口。外部review是否接受该设计及记录仍由reviewer决定，不代其Resolve/Approve，不宣称PR可合并。
