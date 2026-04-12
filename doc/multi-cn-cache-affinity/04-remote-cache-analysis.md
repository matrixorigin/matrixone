# Remote Cache 深度分析

## 架构概览

MO 已经实现了一套完整的 CN 间远程缓存机制，由三个组件组成：

```
┌─────────────────────────────────────────────────────────────────┐
│                        Remote Cache 架构                         │
│                                                                  │
│  CN₁                          CN₂                               │
│  ┌──────────────┐             ┌──────────────┐                  │
│  │ MemCache     │             │ MemCache     │                  │
│  │  PostSet ────┼──┐     ┌───┼── PostSet     │                  │
│  │  PostEvict ──┼──┤     │   ├── PostEvict   │                  │
│  └──────────────┘  │     │   └──────────────┘                  │
│                    ▼     ▼                                      │
│  ┌──────────────────────────────────────────┐                  │
│  │         Gossip (memberlist)               │                  │
│  │  CN₁ 广播: Set(key1, CN₁_addr)           │                  │
│  │  CN₂ 广播: Set(key2, CN₂_addr)           │                  │
│  │                                           │                  │
│  │  每个 CN 维护: keyTarget map[CacheKey]string                 │
│  │  key1 → CN₁_addr                         │                  │
│  │  key2 → CN₂_addr                         │                  │
│  └──────────────────────────────────────────┘                  │
│                                                                  │
│  读取路径 (S3FS.Read):                                          │
│  vector.Caches → memCache → diskCache → remoteCache → S3       │
│                                                                  │
│  RemoteCache.Read():                                            │
│  1. 遍历未命中的 entry                                          │
│  2. 构造 CacheKey{Path, Offset, Size}                           │
│  3. keyRouter.Target(key) → 找到持有该 key 的 CN 地址            │
│  4. 发 RPC (QueryService) 到目标 CN                             │
│  5. 目标 CN 的 handleGetCacheData → ReadCache → 查本地 memCache  │
│  6. 返回数据                                                    │
└─────────────────────────────────────────────────────────────────┘
```

## 2 当前使用状态：默认关闭

**Remote Cache 在生产环境中默认是关闭的。**

证据：
- `CacheConfig.RemoteCacheEnabled` 是 `bool` 类型，Go 零值为 `false`
- 所有 `etc/` 下的配置文件（launch-with-proxy、launch-multi-cn、launch-dynamic-cn 等）的 `[fileservice.cache]` 段都没有设置 `remote-cache-enabled = true`
- 只有测试代码（`remote_cache_test.go`、`query_service_test.go`）中显式设置了 `RemoteCacheEnabled: true`
- 代码中虽然在 `main.go` 里为 CN 设置了 `KeyRouterFactory` 和 `QueryClient`，但如果 `RemoteCacheEnabled` 为 false，`initCaches()` 不会创建 `remoteCache` 实例

**结论：Remote Cache 的代码完整存在，但从未在生产环境启用过。**

## 3 设计缺陷分析

#### 缺陷一：Gossip 广播的可扩展性问题（严重）

**问题：** 每个 CacheKey（Path + Offset + Size）的 Set/Delete 操作都要通过 Gossip 广播给所有 CN。

```go
// cache.go - 每次 memCache 写入都触发广播
c.CacheCallbacks.PostSet = append(c.CacheCallbacks.PostSet,
    func(key fscache.CacheKey, data fscache.Data) {
        c.KeyRouter.AddItem(gossip.CommonItem{
            Operation: gossip.Operation_Set,
            Key: &gossip.CommonItem_CacheKey{CacheKey: &key},
        })
    },
)
```

假设场景：10 个 CN，每个 CN 的 MemCache 有 100 万个 CacheKey，每秒有 1 万次 Set/Evict 操作。

- 每个 CN 每秒要广播 1 万条消息
- 10 个 CN 总共每秒 10 万条 Gossip 消息在集群中传播
- `keyTarget` map 在每个 CN 上存储 900 万条记录（其他 9 个 CN 的 key）
- 内存开销：每个 CacheKey 约 80 字节（Path string + Offset int64 + Size int64 + map overhead），900 万条 ≈ 720MB

**这就是 Remote Cache 没有在生产环境启用的根本原因 — Gossip 广播量和内存开销在大规模场景下不可接受。**

#### 缺陷二：broadcastData 函数有 Bug

```go
// delegate.go
func (d *delegate) broadcastData(limit int) []gossip.GossipData {
    items, left := d.dataCacheKey.Data(left)
    data := make([]gossip.GossipData, len(items))  // ← 创建了 len(items) 个零值元素
    for _, item := range items {
        data = append(data, ...)  // ← append 在零值元素之后追加
    }
}
```

`make([]gossip.GossipData, len(items))` 创建了 `len(items)` 个零值元素，然后 `append` 又追加了 `len(items)` 个有效元素。结果 `data` 的前半部分全是零值的 `GossipData`，会被序列化为空消息广播出去，浪费带宽。

应该是 `make([]gossip.GossipData, 0, len(items))`。

#### 缺陷三：statsInfoState 使用了错误的锁

```go
// delegate.go
func (d *delegate) statsInfoState() map[string]*statsinfo.StatsInfoKeys {
    d.dataCacheKey.mu.Lock()         // ← 锁的是 dataCacheKey
    defer d.dataCacheKey.mu.Unlock()
    for key, target := range d.statsInfoKey.mu.keyTarget {  // ← 访问的是 statsInfoKey
        ...
    }
}
```

锁了 `dataCacheKey.mu` 但访问的是 `statsInfoKey.mu.keyTarget`，没有对 `statsInfoKey` 加锁，存在数据竞争。

#### 缺陷四：NotifyLeave 只清理了 dataCacheKey，没清理 statsInfoKey

```go
// delegate.go
func (d *delegate) NotifyLeave(node *memberlist.Node) {
    d.dataCacheKey.mu.Lock()
    defer d.dataCacheKey.mu.Unlock()
    for key, target := range d.dataCacheKey.mu.keyTarget {
        if cacheServiceAddr == target {
            delete(d.dataCacheKey.mu.keyTarget, key)
        }
    }
    // ← 缺少对 d.statsInfoKey 的清理
}
```

节点离开时，`statsInfoKey` 中该节点的条目不会被清理，导致后续查询可能路由到已下线的节点。

#### 缺陷五：Remote Cache 在读取路径中的位置不合理

```go
// s3_fs.go Read() 方法
// 当前顺序：
vector.Caches → memCache → diskCache → remoteCache → S3
```

Remote Cache 排在 Disk Cache 之后。但 Remote Cache 走内网 RPC（~1ms），而 Disk Cache 走本地磁盘 IO（~1-10ms，取决于磁盘类型和负载）。在高频访问场景下，Remote Cache 应该排在 Disk Cache 之前。

#### 缺陷六：CacheKey 粒度过细

```go
// CacheKey 定义
type CacheKey struct {
    Path   string  // 文件路径
    Offset int64   // 偏移量
    Sz     int64   // 大小
}
```

每个 (Path, Offset, Size) 三元组是一个独立的 CacheKey。同一个文件的不同 offset 是不同的 key。这意味着：

- 一个表的数据可能分散在数百个 CacheKey 中
- Gossip 需要广播每一个 CacheKey 的变更
- `keyTarget` map 的条目数 = 所有 CN 的 MemCache 中的数据块总数

如果改为文件级（Path 级）的粒度，广播量和内存开销可以降低 1~2 个数量级。

#### 缺陷七：广播队列有上限但无降级策略

```go
// base_item.go
const maxItemCount = 8192 * 2000  // 16,384,000

func (s *BaseStore[K]) AddItem(item gossip.CommonItem) {
    if len(s.queueMu.itemQueue) >= maxItemCount {
        return  // ← 静默丢弃，没有日志，没有降级
    }
    s.queueMu.itemQueue = append(s.queueMu.itemQueue, item)
}
```

当广播队列满时，新的 Set/Delete 事件被静默丢弃。这意味着其他 CN 的 `keyTarget` 会变得不准确：
- 丢弃 Set → 其他 CN 不知道本节点有某个 key，Remote Cache 查不到
- 丢弃 Delete → 其他 CN 以为本节点还有某个 key，Remote Cache 查到后发现 miss

没有任何日志或指标来监控这种情况。

#### 缺陷八：HandleRemoteRead 只查 memCache，不查 diskCache

```go
// remote_cache.go - 服务端处理
func HandleRemoteRead(...) error {
    // 调用 fs.ReadCache()
    if err := fs.ReadCache(ctx, ioVec); err != nil { ... }
}

// s3_fs.go - ReadCache 只查 memCache
func (s *S3FS) ReadCache(ctx context.Context, vector *IOVector) (err error) {
    // 查 vector.Caches
    // 查 s.memCache
    // ← 没有查 s.diskCache
    // ← 没有查 s.remoteCache（避免递归）
    return nil
}
```

当 CN₁ 向 CN₂ 发起 Remote Cache 请求时，CN₂ 只查自己的 memCache。如果数据在 CN₂ 的 diskCache 中但不在 memCache 中，会返回 miss。这大大降低了 Remote Cache 的命中率。

## 4 总结

| 维度 | 评估 |
|------|------|
| 代码完整性 | 完整，读写路径、Gossip 广播、RPC 处理都已实现 |
| 生产启用状态 | **未启用**，所有配置文件默认 `RemoteCacheEnabled = false` |
| 可扩展性 | **差**，逐 Key 广播在大规模场景下不可行 |
| 正确性 | 有 Bug（broadcastData 零值、statsInfoState 错误锁、NotifyLeave 不完整） |
| 缓存层级设计 | 不合理（Remote Cache 排在 Disk Cache 之后，服务端只查 memCache） |
| 监控可观测性 | 有基本指标（hit-remote counter），但广播丢弃无监控 |

**如果要启用 Remote Cache，需要先修复上述缺陷，特别是 Gossip 可扩展性问题。建议改为 Bloom Filter 摘要广播或文件级粒度路由，而非逐 Key 广播。**

