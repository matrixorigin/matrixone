# MatrixOne Copilot Instructions

## 项目概述

MatrixOne (MO) 是一个超融合数据库，采用存算分离架构（CN/TN/LogService/Proxy）。

## AI Skill 文档

`docs/ai-skills/` 目录下包含 MO 底层实现的知识文档，用于帮助 AI 理解代码变更的影响：

- `architecture.md` — 整体架构
- `sql-engine.md` — SQL 引擎
- `storage-engine.md` — 存储引擎
- `transaction.md` — 事务模型
- `backup-restore.md` — 备份恢复
- `cdc.md` — CDC
- `fulltext-vector.md` — 全文检索 + 向量索引
- `multi-cn.md` — 多 CN 架构
- `fileservice.md` — FileService
- `module-test-mapping.md` — 模块→测试关联表
- `testing-guide.md` — 测试操作指南
- `test-env-setup.md` — 测试环境配置

## 测试体系

MO 有 6 类测试：BVT、稳定性、Chaos、大数据、PITR、Snapshot。

BVT 测试位于 `test/distributed/cases/` 下，使用 `.test` / `.sql` 格式，通过 mo-tester 运行。

## 编码规范

- Go 代码遵循 `.golangci.yml` 中的 lint 规则
- BVT case 编写规范见 `docs/ai-skills/testing-guide.md`

---

<agent>
<name>MO Test Coverage Analyzer</name>
<description>分析 PR diff，参考 skill 文档判断 6 类测试覆盖情况，自动生成缺失的 BVT case</description>
</agent>
