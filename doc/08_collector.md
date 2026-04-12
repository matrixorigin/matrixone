# Collector 脏数据收集器

## 概述

Collector（收集器）负责收集和管理系统中的脏数据（dirty data）。它跟踪哪些数据块在特定时间范围内被修改，为 Flusher 和 Checkpoint 提供数据来源。

## 接口定义

```go
type Collector interface {
    String() string
    Run(lag time.Duration)
    ScanInRange(from, to types.TS) (*DirtyTreeEntry, int)
    ScanInRangePruned(from, to types.TS) *DirtyTreeEntry
    GetAndRefreshMerged() *DirtyTreeEntry
    Merge() *DirtyTreeEntry
    GetMaxLSN(from, to types.TS) uint64
    Init(maxts types.TS)
}
```

## 核心结构

### DirtyTreeEntry

```go
type DirtyTreeEntry struct {
    sync.RWMutex
    start, end types.TS   // 时间范围
    tree       *model.Tree  // 脏数据树
}

func NewEmptyDirtyTreeEntry() *DirtyTreeEntry {
    return &DirtyTreeEntry{
        tree: model.NewTree(),
    }
}

func NewDirtyTreeEntry(start, end types.TS, tree *model.Tree) *DirtyTreeEntry {
    entry := NewEmptyDirtyTreeEntry()
    entry.start = start
    entry.end = end
    entry.tree = tree
    return entry
}
```

### 脏数据树结构

```
DirtyTreeEntry [t1, t5]
        /           \
     [TB1]         [TB2]
       |             |
      [1]        [2,3,4]    <- 叶子节点是脏块 ID
```

### dirtyCollector

```go
type dirtyCollector struct {
    // 数据源
    sourcer *Manager

    // 上下文
    catalog     *catalog.Catalog
    clock       *types.TsAlloctor
    interceptor DirtyEntryInterceptor

    // 存储
    storage struct {
        sync.RWMutex
        entries *btree.BTreeG[*DirtyTreeEntry]
        maxTs   types.TS
    }
    merged atomic.Pointer[DirtyTreeEntry]
}
```

## 初始化

```go
func NewDirtyCollector(
    sourcer *Manager,
    clock clock.Clock,
    catalog *catalog.Catalog,
    interceptor DirtyEntryInterceptor,
) *dirtyCollector {
    collector := &dirtyCollector{
        sourcer:     sourcer,
        catalog:     catalog,
        interceptor: interceptor,
        clock:       types.NewTsAlloctor(clock),
    }
    
    // 初始化 B-Tree 存储
    collector.storage.entries = btree.NewBTreeGOptions(
        func(a, b *DirtyTreeEntry) bool {
            return a.start.LT(&b.start) && a.end.LT(&b.end)
        }, btree.Options{
            NoLocks: true,
        })

    collector.merged.Store(NewEmptyDirtyTreeEntry())
    return collector
}
```

## 运行流程

### Run 方法

```go
func (d *dirtyCollector) Run(lag time.Duration) {
    // 1. 确定扫描范围
    from, to := d.findRange(lag)

    // 如果范围过期，跳过本次运行
    if to.IsEmpty() {
        return
    }

    // 2. 范围扫描并更新
    d.rangeScanAndUpdate(from, to)
    
    // 3. 清理存储
    d.cleanupStorage()
    
    // 4. 刷新合并结果
    d.GetAndRefreshMerged()
}
```

### 确定扫描范围

```go
func (d *dirtyCollector) findRange(lagDuration time.Duration) (from, to types.TS) {
    now := d.clock.Alloc()
    // 故意滞后，避免与最新的 ablock 竞争
    lag := types.BuildTS(now.Physical()-int64(lagDuration), now.Logical())
    
    d.storage.RLock()
    defer d.storage.RUnlock()
    
    if lag.LE(&d.storage.maxTs) {
        return
    }
    from, to = d.storage.maxTs.Next(), lag
    return
}
```

### 范围扫描

```go
func (d *dirtyCollector) ScanInRange(from, to types.TS) (
    entry *DirtyTreeEntry, count int,
) {
    reader := d.sourcer.GetReader(from, to)
    tree, count := reader.GetDirty()

    entry = &DirtyTreeEntry{
        start: from,
        end:   to,
        tree:  tree,
    }
    return
}

func (d *dirtyCollector) ScanInRangePruned(from, to types.TS) (
    tree *DirtyTreeEntry,
) {
    tree, _ = d.ScanInRange(from, to)
    if err := d.tryCompactTree(context.Background(), tree); err != nil {
        panic(err)
    }
    return
}
```

### 范围扫描并更新

```go
func (d *dirtyCollector) rangeScanAndUpdate(from, to types.TS) (updated bool) {
    entry, _ := d.ScanInRange(from, to)
    updated = d.tryStoreEntry(entry)
    return
}

func (d *dirtyCollector) tryStoreEntry(entry *DirtyTreeEntry) (ok bool) {
    ok = true
    d.storage.Lock()
    defer d.storage.Unlock()

    // 检查是否是连续的
    maxTS := d.storage.maxTs.Next()
    if !entry.start.Equal(&maxTS) {
        ok = false
        return
    }

    // 更新最大时间戳
    d.storage.maxTs = entry.end

    // 不存储空条目
    if entry.tree.IsEmpty() {
        return
    }

    d.storage.entries.Set(entry)
    return
}
```

## 合并操作

### GetAndRefreshMerged

```go
func (d *dirtyCollector) GetAndRefreshMerged() (merged *DirtyTreeEntry) {
    merged = d.merged.Load()
    
    d.storage.RLock()
    maxTs := d.storage.maxTs
    d.storage.RUnlock()
    
    if maxTs.LE(&merged.end) {
        return
    }
    
    merged = d.Merge()
    d.tryUpdateMerged(merged)
    return
}
```

### Merge

```go
func (d *dirtyCollector) Merge() *DirtyTreeEntry {
    // 获取存储快照
    snapshot, maxTs := d.getStorageSnapshot()

    merged := NewEmptyDirtyTreeEntry()
    merged.end = maxTs

    // 扫描并合并所有条目
    snapshot.Scan(func(entry *DirtyTreeEntry) bool {
        entry.RLock()
        defer entry.RUnlock()
        merged.tree.Merge(entry.tree)
        return true
    })

    return merged
}
```

### 更新合并结果

```go
func (d *dirtyCollector) tryUpdateMerged(merged *DirtyTreeEntry) (updated bool) {
    var old *DirtyTreeEntry
    for {
        old = d.merged.Load()
        if old.end.GE(&merged.end) {
            break
        }
        if d.merged.CompareAndSwap(old, merged) {
            updated = true
            break
        }
    }
    return
}
```

## 存储清理

### cleanupStorage

```go
func (d *dirtyCollector) cleanupStorage() {
    toDeletes := make([]*DirtyTreeEntry, 0)

    // 获取条目快照
    entries, _ := d.getStorageSnapshot()

    // 扫描所有条目，尝试压缩脏数据树
    entries.Scan(func(entry *DirtyTreeEntry) bool {
        entry.Lock()
        defer entry.Unlock()
        
        // 如果树为空，标记删除
        if entry.tree.IsEmpty() {
            toDeletes = append(toDeletes, entry)
            return true
        }
        
        // 尝试压缩树
        if err := d.tryCompactTree(context.Background(), entry); err != nil {
            logutil.Warnf("error: interceptor on dirty tree: %v", err)
        }
        
        if entry.tree.IsEmpty() {
            toDeletes = append(toDeletes, entry)
        }
        return true
    })

    if len(toDeletes) == 0 {
        return
    }

    // 从存储中删除空条目
    d.storage.Lock()
    defer d.storage.Unlock()
    for _, tree := range toDeletes {
        d.storage.entries.Delete(tree)
    }
}
```

### tryCompactTree

压缩脏数据树，移除已刷盘或不存在的条目：

```go
func (d *dirtyCollector) tryCompactTree(
    ctx context.Context,
    entry *DirtyTreeEntry,
) (err error) {
    var (
        db   *catalog.DBEntry
        tbl  *catalog.TableEntry
        tree = entry.tree
    )
    
    for id, dirtyTable := range tree.Tables {
        // 移除不存在的数据库
        if db, err = d.catalog.GetDatabaseByID(dirtyTable.DbID); err != nil {
            if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
                tree.Shrink(id)
                err = nil
                continue
            }
            break
        }
        
        // 移除不存在的表
        if tbl, err = db.GetTableEntryByID(dirtyTable.ID); err != nil {
            if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
                tree.Shrink(id)
                err = nil
                continue
            }
            break
        }

        // 临时过滤器检查
        if x := ctx.Value(TempFKey{}); x != nil && TempF.Check(tbl.ID) {
            tree.Shrink(id)
            continue
        }

        // 检查表尾是否已刷盘
        if flushed, _ := tbl.IsTableTailFlushed(entry.start, entry.end); flushed {
            tree.Shrink(id)
            continue
        }
    }
    return
}
```

## DirtyTreeEntry 方法

### 合并

```go
func (entry *DirtyTreeEntry) Merge(o *DirtyTreeEntry) {
    if entry.start.GT(&o.start) {
        entry.start = o.start
    }
    if entry.end.LT(&o.end) {
        entry.end = o.end
    }
    entry.tree.Merge(o.tree)
}
```

### 判空

```go
func (entry *DirtyTreeEntry) IsEmpty() bool {
    return entry.tree.IsEmpty()
}
```

### 获取时间范围

```go
func (entry *DirtyTreeEntry) GetTimeRange() (from, to types.TS) {
    return entry.start, entry.end
}
```

### 获取树

```go
func (entry *DirtyTreeEntry) GetTree() (tree *model.Tree) {
    return entry.tree
}
```

### 字符串表示

```go
func (entry *DirtyTreeEntry) String() string {
    var buf bytes.Buffer
    _, _ = buf.WriteString(
        fmt.Sprintf("DirtyTreeEntry[%s=>%s]\n",
            entry.start.ToString(),
            entry.end.ToString()))
    _, _ = buf.WriteString(entry.tree.String())
    return buf.String()
}
```

## 辅助方法

### 获取脏数据计数

```go
func (d *dirtyCollector) DirtyCount() int {
    merged := d.GetAndRefreshMerged()
    return merged.tree.TableCount()
}
```

### 获取最大 LSN

```go
func (d *dirtyCollector) GetMaxLSN(from, to types.TS) uint64 {
    reader := d.sourcer.GetReader(from, to)
    return reader.GetMaxLSN()
}
```

### 初始化

```go
func (d *dirtyCollector) Init(maxts types.TS) {
    d.storage.maxTs = maxts
}
```

## 临时过滤器

用于测试和调试的临时过滤器：

```go
type TempFilter struct {
    sync.RWMutex
    m map[uint64]bool
}

type TempFKey struct{}

func (f *TempFilter) Add(id uint64) {
    f.Lock()
    defer f.Unlock()
    f.m[id] = true
}

func (f *TempFilter) Check(id uint64) (skip bool) {
    f.Lock()
    defer f.Unlock()
    if _, ok := f.m[id]; ok {
        delete(f.m, id)
        return true
    }
    return false
}

var TempF *TempFilter

func init() {
    TempF = &TempFilter{
        m: make(map[uint64]bool),
    }
}
```

## 数据流图

```
┌─────────────────────────────────────────────────────────────────┐
│                    Collector 数据流                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐                                                │
│  │   Manager   │ ◀── Logtail 数据源                             │
│  └─────────────┘                                                │
│         │                                                       │
│         │ GetReader(from, to)                                   │
│         ▼                                                       │
│  ┌─────────────┐                                                │
│  │   Reader    │                                                │
│  └─────────────┘                                                │
│         │                                                       │
│         │ GetDirty()                                            │
│         ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    DirtyTreeEntry                        │   │
│  │  ┌─────────────────────────────────────────────────────┐│   │
│  │  │                    model.Tree                        ││   │
│  │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐             ││   │
│  │  │  │ Table1  │  │ Table2  │  │ Table3  │  ...        ││   │
│  │  │  └─────────┘  └─────────┘  └─────────┘             ││   │
│  │  └─────────────────────────────────────────────────────┘│   │
│  └─────────────────────────────────────────────────────────┘   │
│         │                                                       │
│         │ tryStoreEntry()                                       │
│         ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    storage.entries                       │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐                  │   │
│  │  │Entry[t1]│  │Entry[t2]│  │Entry[t3]│  ...             │   │
│  │  └─────────┘  └─────────┘  └─────────┘                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│         │                                                       │
│         │ Merge()                                               │
│         ▼                                                       │
│  ┌─────────────┐                                                │
│  │   merged    │ ◀── 合并后的脏数据树                           │
│  └─────────────┘                                                │
│         │                                                       │
│         │ 提供给 Flusher 和 Checkpoint                          │
│         ▼                                                       │
│  ┌─────────────┐  ┌─────────────┐                              │
│  │   Flusher   │  │ Checkpoint  │                              │
│  └─────────────┘  └─────────────┘                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```
