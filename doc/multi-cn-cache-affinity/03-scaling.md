# 动态扩缩容场景深入分析

线上 MO 的 CN 会根据负载动态扩缩容，这是云原生架构的核心优势，但也给 Cache 亲和性带来了根本性挑战：每次扩缩容都意味着 Hash Ring 变化、路由映射迁移、Cache 失效。如果处理不好，扩缩容反而会导致短时间内 Cache 命中率断崖式下跌，查询延迟飙升。

## 1 扩缩容带来的核心问题

**扩容（Scale Out）：**

```
扩容前：3 CN，每个负责 1/3 的表
扩容后：5 CN，Hash Ring 重新分配

CN₁: [A,B,C,D] → CN₁: [A,B]      ← C,D 被迁走，但 Cache 还在 CN₁
CN₂: [E,F,G,H] → CN₂: [E,F]      ← G,H 被迁走
CN₃: [I,J,K,L] → CN₃: [I,J]      ← K,L 被迁走
                  CN₄: [C,D,G]     ← 新节点，Cache 全空，冷启动
                  CN₅: [H,K,L]     ← 新节点，Cache 全空，冷启动
```

问题：
- 新 CN 的 Cache 完全为空，所有请求都要回源对象存储
- 被迁走的表在原 CN 上还有热 Cache，但不再被路由到，浪费
- 扩容瞬间可能出现大量 Cache Miss 风暴，对象存储压力骤增

**缩容（Scale In）：**

```
缩容前：5 CN
缩容后：3 CN，CN₄ 和 CN₅ 被移除

CN₄ 上的 Cache 数据 [C,D,G] → 全部丢失
CN₅ 上的 Cache 数据 [H,K,L] → 全部丢失
这些表的请求重新分配到 CN₁~CN₃ → 全部 Cache Miss
```

问题：
- 被移除 CN 上的 Cache 数据直接丢失
- 接管这些表的 CN 需要重新从对象存储加载，延迟飙升
- 如果缩容发生在业务高峰期，影响更大

**频繁扩缩容（弹性伸缩）：**

- 如果扩缩容频率较高（比如每小时根据负载调整），Hash Ring 频繁变化
- Cache 还没预热完就又被迁移，始终处于低命中率状态
- 极端情况下比不做亲和性路由还差（因为亲和路由 + 频繁迁移 = 持续冷启动）

## 2 各方案在动态扩缩容下的表现

| 方案 | 扩容影响 | 缩容影响 | 频繁弹性伸缩适应性 |
|------|---------|---------|-------------------|
| 一：Consistent Hash | 仅 1/N 数据迁移，但新节点冷启动 | 被移除节点 Cache 丢失 | 中，需要配合预热 |
| 二：CN 分组 | 组内扩缩影响小，跨组调整成本高 | 组内缩容影响可控 | 差，静态绑定不适合弹性 |
| 三：独立 Cache 池 | CN 扩缩不影响 Cache | CN 扩缩不影响 Cache | 最优，Cache 与 CN 完全解耦 |
| 四：Cache-Aware 调度 | 自适应，新 CN 逐步积累 Cache | 自适应，其他 CN 接管 | 好，天然适应变化 |
| 五：协同淘汰 | 不涉及路由，无直接影响 | 需要感知节点下线 | 好，但效果有限 |
| 六：分层 + MESI | Slot 迁移可控 | 需要 Slot 交接 | 中，依赖迁移效率 |

## 3 动态扩缩容专项设计

### 1 Graceful Scale Out（优雅扩容）

**阶段一：Shadow 模式（预热期）**

新 CN 加入后不立即承接流量，而是进入 Shadow 模式：

```
时间线：
T0: CN₄ 加入集群
T0~T1: Shadow 期 — Proxy 仍将请求路由到原 CN，但同时将查询计划异步发送给 CN₄
       CN₄ 在后台预加载即将接管的表数据到 Cache
T1: CN₄ Cache 预热完成（命中率达到阈值，如 80%）
T1+: Proxy 将 CN₄ 加入 Hash Ring，开始正式承接流量
```

借鉴：Kafka Consumer Group Rebalance 的 Cooperative Sticky 策略 — 不一次性切换，而是渐进式迁移。

**阶段二：渐进式流量切换**

不是一次性把所有应该迁移的表切到新 CN，而是分批切换：

```
T1: 先迁移 20% 的表到 CN₄
T2: 观察 Cache 命中率和延迟，确认稳定
T3: 再迁移 30%
T4: 迁移剩余 50%
```

每批迁移后等待 Cache 预热窗口，避免 Miss 风暴。

**阶段三：Peer Fetch 加速预热**

新 CN 在预热期间，Cache Miss 时优先从原 Owner CN 通过 RPC 获取数据（而非回对象存储）：

```
CN₄ Cache Miss(Table C) → 查 Hash Ring 历史，找到原 Owner CN₁
                        → RPC Fetch from CN₁（内网 ~ms）
                        → 写入 CN₄ 本地 Cache
                        → 比回对象存储（~100ms）快 100 倍
```

### 2 Graceful Scale In（优雅缩容）

**阶段一：Cache 数据迁移**

CN 被移除前，主动将 Cache 中的热数据推送到接管节点：

```
CN₅ 即将下线：
1. 通知 Proxy 停止向 CN₅ 分发新请求
2. CN₅ 等待存量请求处理完毕（Drain）
3. CN₅ 将 Cache 中的热数据（按访问频率排序 Top K）推送到接管 CN
4. 接管 CN 收到数据后写入本地 Cache
5. CN₅ 正式下线
```

借鉴：Kubernetes Pod Graceful Shutdown + Redis Cluster 的 Slot 迁移（MIGRATE 命令）。

**阶段二：双写过渡期**

在 CN₅ Drain 期间，Proxy 将该 CN 负责的表的请求同时发送到 CN₅ 和接管 CN：

- CN₅ 正常处理并返回结果
- 接管 CN 执行查询但不返回结果，仅用于预热 Cache
- 过渡期结束后切换到接管 CN

### 3 弹性伸缩稳定性机制

**冷却期（Cooldown）：**

- 扩缩容操作之间设置最小间隔（如 10 分钟）
- 避免频繁扩缩导致 Cache 持续震荡
- 借鉴：Kubernetes HPA 的 stabilizationWindowSeconds

**Cache 命中率感知的伸缩决策：**

- 将 Cache 命中率纳入扩缩容决策指标
- 如果当前 Cache 命中率低于阈值（说明还在预热），延迟下一次扩缩容
- 避免 Cache 还没稳定就又触发变更

```
扩缩容决策 = f(CPU利用率, 内存利用率, QPS, Cache命中率)

if cache_hit_rate < 70%:
    suppress_scale_action()  # 抑制扩缩容，等待 Cache 稳定
```

**虚拟节点权重动态调整：**

- 新加入的 CN 初始权重低（承接少量流量），随着 Cache 预热逐步提升权重
- 即将下线的 CN 权重逐步降低，流量平滑迁移

```
CN₄ 加入：
T0: weight = 0.2（承接 20% 正常流量）
T1: weight = 0.5（Cache 命中率 > 60%）
T2: weight = 0.8（Cache 命中率 > 80%）
T3: weight = 1.0（完全就绪）
```

### 4 扩缩容场景下的方案选择建议

| 扩缩容特征 | 推荐方案 | 原因 |
|-----------|---------|------|
| 低频扩缩容（每天 1~2 次） | 方案一 + Graceful 机制 | Consistent Hash 够用，配合预热即可 |
| 中频扩缩容（每小时级别） | 方案四（Cache-Aware 调度） | 天然适应节点变化，无需迁移 |
| 高频弹性伸缩（分钟级别） | 方案三（独立 Cache 池） | CN 扩缩完全不影响 Cache |
| 混合场景 | 方案一 + 方案四 + Graceful | 路由亲和为主，调度为辅，优雅迁移兜底 |

## 4 扩缩容全流程时序

**扩容流程：**

```
                    Proxy          CN_new         CN_old(owner)    对象存储
                      │               │               │               │
  1. CN_new 注册      │◄──────────────│               │               │
  2. 进入 Shadow      │──shadow queries──►│            │               │
  3. 预热 Peer Fetch  │               │──RPC Fetch───►│               │
                      │               │◄──Cache Data──│               │
  4. 预热完成通知     │◄──ready───────│               │               │
  5. 调整 Hash Ring   │               │               │               │
  6. 渐进切流量       │──real queries─►│               │               │
  7. 权重逐步提升     │──more queries─►│               │               │
  8. 完全就绪         │──full traffic─►│               │               │
```

**缩容流程：**

```
                    Proxy          CN_leaving      CN_takeover      对象存储
                      │               │               │               │
  1. 标记下线         │──stop new req─►│               │               │
  2. Drain 存量       │               │──processing──►│               │
  3. 双写预热         │──dual write───►│               │               │
                      │──dual write──────────────────►│               │
  4. Cache 推送       │               │──push hot data►│              │
  5. 切换完成         │──all traffic─────────────────►│               │
  6. CN 下线          │               │ (shutdown)     │               │
```

