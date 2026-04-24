# MO Test Coverage Analyzer — 内部使用手册

本文档介绍 MatrixOne 内部如何调用 PR #24180 引入的 **MO Test Coverage Analyzer** 自动分析 PR 测试覆盖情况。

## 它是什么

仓库内的 GitHub Copilot 自定义 Agent，按需触发。读取 PR diff，结合 [`docs/ai-skills/`](./) 下的知识库，输出 6 类测试覆盖矩阵（BVT / 稳定性 / Chaos / 大数据 / PITR / Snapshot）以及缺失 BVT case 的建议。

不会自动跑、不会自动评论、不会消耗资源 —— 只在你显式调用时执行。

## 前置条件

- matrixorigin 组织已订阅 GitHub Copilot Business/Enterprise
- 你已被分配 Copilot 席位（找组织管理员申请）
- matrixone 仓库已启用 Copilot Coding Agent（仓库设置 → Copilot）

## 触发方式

### 1. 在 PR 网页评论触发

在任意 matrixone PR 下评论：

```
@copilot 用 Test Coverage Analyzer 分析此 PR
```

Copilot 后台会读取 [`.github/copilot-instructions.md`](../../.github/copilot-instructions.md) 与 [`.github/agents/test-analyzer.agent.md`](../../.github/agents/test-analyzer.agent.md)，按 agent 流程产出报告并直接回复评论。

### 2. 在 VS Code Copilot Chat 触发

打开 matrixone 仓库，Copilot Chat 输入：

```
分析当前 PR 的测试覆盖
```

或指定 PR 号：

```
分析 PR #24178 的测试覆盖
```

### 3. 仅生成 BVT case（不要分析报告）

```
@copilot 为 PR #xxxxx 在 test/distributed/cases/ 下补充缺失的 BVT case
```

## 输出范例

Agent 会按以下结构输出（节选自 PR #24178 真实运行结果）：

```markdown
## PR #24178 测试覆盖分析

### 变更摘要
推迟 CN task runner 启动直到 frontend ready，修复 CDC daemon task 启动竞争。

### 6 类测试覆盖情况
| 测试类型 | 状态 | 说明 |
|---|---|---|
| Unit | ⚠️ 需补充 | server_task_start_test.go 缺少 race-fix 直接断言 |
| BVT | ⚠️ 需补充 | test/distributed/cases/cdc/ 下无 daemon-task lifecycle |
| Chaos | ⚠️ 强相关 | mo-nightly-regression 需补 pod-kill CN 场景 |
| Stability | ➖ 不相关 | 启动期 race，稳态无影响 |
| 大数据 / PITR / Snapshot | ➖ 不相关 | 与改动无业务关联 |

### 建议补充的 BVT Case
| 文件 | 场景 |
|---|---|
| test/distributed/cases/cdc/cdc_task_runner_lifecycle.test | 创建/查看/删除 CDC task 的跨 session 生命周期 |
```

## 知识库覆盖范围

Agent 在分析时会按需读取 [`docs/ai-skills/`](./) 下的文档：

| 文档 | 何时被读 |
|---|---|
| [module-test-mapping.md](./module-test-mapping.md) | **每次必读** —— 模块到测试类型的映射 |
| [testing-guide.md](./testing-guide.md) | 生成 BVT case 时读，遵循格式规范 |
| [architecture.md](./architecture.md) | 跨模块或集群级改动 |
| [sql-engine.md](./sql-engine.md) | `pkg/sql/` 改动 |
| [storage-engine.md](./storage-engine.md) | `pkg/vm/engine/` 改动 |
| [transaction.md](./transaction.md) | `pkg/txn/`, `pkg/lockservice/` 改动 |
| [cdc.md](./cdc.md) | `pkg/cdc/` 改动 |
| [multi-cn.md](./multi-cn.md) | `pkg/cnservice/`, `pkg/queryservice/`, `pkg/proxy/` 改动 |
| [fileservice.md](./fileservice.md) | `pkg/fileservice/`, `pkg/objectio/` 改动 |
| [fulltext-vector.md](./fulltext-vector.md) | `pkg/fulltext/`, `pkg/vectorindex/` 改动 |
| [backup-restore.md](./backup-restore.md) | `pkg/backup/`, PITR/Snapshot 改动 |

## 维护与升级

- **改 agent 行为**：编辑 [`.github/agents/test-analyzer.agent.md`](../../.github/agents/test-analyzer.agent.md)，下个 PR 生效。
- **补/改 知识库**：在 [`docs/ai-skills/`](./) 增删 `.md` 文件并更新 [`module-test-mapping.md`](./module-test-mapping.md) 的索引。
- **不要改 [`.github/copilot-instructions.md`](../../.github/copilot-instructions.md) 的 `<agent>` 块名称**，否则触发关键词失效。

## 局限

| 局限 | 解释 | 何时考虑升级 |
|---|---|---|
| 只能在 matrixone 仓内调用 | 配置文件随仓走 | 若要跨仓使用，可走 GitHub App 方案 |
| 受 Copilot 后台行为约束 | 工具调用顺序由 Copilot 决定 | 需要严格可控时改用自建 workflow + LLM API |
| 不会自动跑 | 必须显式 mention | 若要 PR 创建即跑，可加 `.github/workflows/` |
| 不分析非代码改动 | 仅文档/CI 改动会回 "无需补充测试" | 按设计行为 |

## 反馈

使用中遇到问题或想增强 agent 能力，请直接在 matrixone 仓提 issue，并 cc agent 维护者（即本目录贡献者）。
