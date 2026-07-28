# CDC（Change Data Capture）

## 概述

MO 的 CDC 模块基于 Logtail 捕获数据变更，将变更同步到下游系统。

## 核心组件（`pkg/cdc/`）

| 组件 | 职责 |
|------|------|
| `CDCStateManager` | 每个 publication 的状态跟踪 |
| `CDCStatementBuilder` | 为下游生成复制 SQL |
| `WatermarkUpdater` | 同步进度（水位线）管理 |
| Table Scanner | 扫描表变更 |

## 工作流程

```
表数据变更 → Logtail 捕获 → CDC 模块消费 → 生成下游 SQL → 同步到下游
                                ↓
                         WatermarkUpdater 更新水位线
```

## 后台任务类型

- `JT_CDC_GetOrAddCommittedWM` — 获取/添加已提交水位线
- `JT_CDC_CommittingWM` — 提交中的水位线
- `JT_CDC_UpdateWMErrMsg` — 更新水位线错误信息
- `JT_CDC_RemoveCachedWM` — 清除缓存水位线

## SQL 语法

```sql
CREATE CDC cdc_name ...;
ALTER CDC cdc_name ...;
DROP CDC cdc_name;
SHOW CDC;
```

## 与测试的关联

| 变更范围 | 影响的测试 |
|---------|----------|
| CDC 状态管理 | BVT: cdc |
| 水位线逻辑 | BVT: cdc; 稳定性: 长时间 CDC 同步 |
| Logtail 消费 | CDC + PITR + Snapshot |
| 杀节点后 CDC 恢复 | Chaos: 杀 CN/TN 后 CDC 恢复 |
