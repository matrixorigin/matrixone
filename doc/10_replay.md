# Checkpoint 恢复流程

## 概述

系统重启时，需要从 Checkpoint 恢复数据。恢复流程包括读取元数据文件、加载检查点数据、重放到 Catalog 等步骤。

## CkpReplayer

`CkpReplayer` 是检查点恢复的核心组件。

### 结构定义

```go
type CkpReplayer struct {
    dir        string
    r          *runner
    ckpEntries []*CheckpointEntry
    ckpReader  *CheckpointReader

    readDuration, applyDuration       time.Duration
    readCount, applyCount, totalCount int

    objectReplayWorker []sm.Queue
    wg                 sync.WaitGroup
    objectCountMap     map[uint64]int
}
```

### 创建 Replayer

```go
func (r *runner) BuildReplayer(dir string) *CkpReplayer {
    replayer := &CkpReplayer{
        r:              r,
        dir:            dir,
        objectCountMap: make(map[uint64]int),
    }
    
    // 创建对象重放工作线程
    objectWorker := make([]sm.Queue, DefaultObjectReplayWorkerCount)
    for i := 0; i < DefaultObjectReplayWorkerCount; i++ {
        objectWorker[i] = sm.NewSafeQueue(10000, 100, func(items ...any) {
            for _, item := range items {
                fn := item.(func())
                fn()
                replayer.wg.Done()
            }
        })
        objectWorker[i].Start()
    }
    replayer.objectReplayWorker = objectWorker
    return replayer
}
```

## 恢复流程

### 1. 读取检查点条目

```go
func (c *CkpReplayer) readCheckpointEntries() (
    allEntries []*CheckpointEntry, maxGlobalTS types.TS, err error,
) {
    var files []ioutil.TSRangeFile

    // 列出所有元数据文件
    if files, err = ioutil.ListTSRangeFiles(
        c.r.ctx, c.dir, c.r.rt.Fs,
    ); err != nil {
        return
    }

    if len(files) == 0 {
        return
    }

    // 按结束时间戳排序
    sort.Slice(files, func(i, j int) bool {
        return files[i].GetEnd().LT(files[j].GetEnd())
    })

    var metaFiles, compactedEntries []ioutil.TSRangeFile

    // 分类文件
    for _, file := range files {
        c.r.store.AddMetaFile(file.GetName())
        if file.IsCompactExt() {
            compactedEntries = append(compactedEntries, file)
        } else if file.IsMetadataFile() {
            metaFiles = append(metaFiles, file)
        }
    }

    // 重放压缩条目
    if len(compactedEntries) > 0 {
        maxEntry := compactedEntries[len(compactedEntries)-1]
        var entries []*CheckpointEntry
        if entries, err = ReadEntriesFromMeta(
            c.r.ctx, c.r.rt.SID(), c.dir, maxEntry.GetName(),
            0, nil, common.CheckpointAllocator, c.r.rt.Fs,
        ); err != nil {
            return
        }
        if err = c.r.ReplayCKPEntry(entries[0]); err != nil {
            return
        }
    }

    // 从最新的元数据文件读取
    if len(metaFiles) > 0 {
        var maxGlobalFile ioutil.TSRangeFile
        for i := len(metaFiles) - 1; i >= 0; i-- {
            start := metaFiles[i].GetStart()
            if start.IsEmpty() {
                maxGlobalFile = metaFiles[i]
                break
            }
        }

        maxFile := metaFiles[len(metaFiles)-1]
        toReadFiles := []ioutil.TSRangeFile{maxFile}
        if maxGlobalFile.GetName() != maxFile.GetName() {
            toReadFiles = append(toReadFiles, maxGlobalFile)
        }

        updateGlobal := func(entry *CheckpointEntry) {
            if entry.GetType() == ET_Global {
                thisEnd := entry.GetEnd()
                if thisEnd.GT(&maxGlobalTS) {
                    maxGlobalTS = thisEnd
                }
            }
        }

        for _, oneFile := range toReadFiles {
            var loopEntries []*CheckpointEntry
            if loopEntries, err = ReadEntriesFromMeta(
                c.r.ctx, c.r.rt.SID(), c.dir, oneFile.GetName(),
                0, updateGlobal, common.CheckpointAllocator, c.r.rt.Fs,
            ); err != nil {
                return
            }
            allEntries = append(allEntries, loopEntries...)
        }

        // 排序去重
        allEntries = compute.SortAndDedup(
            allEntries,
            func(lh, rh **CheckpointEntry) bool {
                lhEnd, rhEnd := (*lh).GetEnd(), (*rh).GetEnd()
                return lhEnd.LT(&rhEnd)
            },
            func(lh, rh **CheckpointEntry) bool {
                lhStart, rhStart := (*lh).GetStart(), (*rh).GetStart()
                if !lhStart.EQ(&rhStart) {
                    return false
                }
                lhEnd, rhEnd := (*lh).GetEnd(), (*rh).GetEnd()
                return lhEnd.EQ(&rhEnd)
            },
        )
    }
    return
}
```

### 2. 读取检查点文件

```go
func (c *CkpReplayer) ReadCkpFiles() (err error) {
    var maxGlobalEnd types.TS

    // 读取检查点条目
    if c.ckpEntries, maxGlobalEnd, err = c.readCheckpointEntries(); err != nil {
        return
    }
    c.totalCount = len(c.ckpEntries)

    // 创建检查点读取器
    c.ckpReader = NewCheckpointReader(
        c.r.ctx, c.ckpEntries, maxGlobalEnd, 
        common.CheckpointAllocator, c.r.rt.Fs,
    )

    // 准备读取
    if err = c.ckpReader.Prepare(c.r.ctx); err != nil {
        return
    }

    // 添加条目到 runner
    for i, entry := range c.ckpEntries {
        if entry == nil {
            continue
        }
        if entry.IsGlobal() {
            c.ckpReader.globalIdx = i
        }
        if err = c.r.ReplayCKPEntry(entry); err != nil {
            return
        }
    }

    // 等待所有条目处理完成
    if len(c.ckpEntries) > 0 {
        select {
        case <-c.r.ctx.Done():
            err = context.Cause(c.r.ctx)
            return
        case <-c.ckpEntries[len(c.ckpEntries)-1].Wait():
        }
    }

    return
}
```

### 3. 重放三表对象列表

```go
func (c *CkpReplayer) ReplayThreeTablesObjectlist(phase string) (
    maxTs types.TS, maxLSN uint64, err error,
) {
    t0 := time.Now()
    defer func() {
        c.applyDuration += time.Since(t0)
    }()

    if len(c.ckpEntries) == 0 {
        return
    }

    ctx := c.r.ctx

    // 重放系统表对象列表
    maxTs, maxLSN, err = c.replayObjectList(ctx, true)
    
    c.wg.Wait()
    return
}
```

### 4. 重放 Catalog

```go
func (c *CkpReplayer) ReplayCatalog(
    readTxn txnif.AsyncTxn,
    phase string,
) (err error) {
    if len(c.ckpEntries) == 0 {
        return
    }

    sortFunc := func(cols []containers.Vector, pkidx int) (err2 error) {
        _, err2 = mergesort.SortBlockColumns(cols, pkidx, c.r.rt.VectorPool.Transient)
        return
    }
    
    closeFn := c.r.catalog.RelayFromSysTableObjects(
        c.r.ctx,
        readTxn,
        tables.ReadSysTableBatch,
        sortFunc,
        c,
    )
    
    c.wg.Wait()
    for _, fn := range closeFn {
        fn()
    }
    c.resetObjectCountMap()
    return
}
```

### 5. 重放对象列表

```go
func (c *CkpReplayer) ReplayObjectlist(ctx context.Context, phase string) (err error) {
    if len(c.ckpEntries) == 0 {
        return
    }
    
    t0 := time.Now()
    r := c.r

    var maxTS types.TS
    if maxTS, _, err = c.replayObjectList(ctx, false); err != nil {
        return
    }

    c.wg.Wait()
    c.applyDuration += time.Since(t0)
    
    // 初始化数据源
    r.source.Init(maxTS)
    
    return
}
```

### 6. 对象列表重放实现

```go
func (c *CkpReplayer) replayObjectList(
    ctx context.Context,
    forSys bool,
) (maxTS types.TS, maxLsn uint64, err error) {
    tmpBatch := ckputil.MakeDataScanTableIDBatch()
    defer tmpBatch.Clean(common.CheckpointAllocator)
    defer c.ckpReader.Reset(ctx)
    
    for {
        tmpBatch.CleanOnlyData()
        var end bool
        if end, err = c.ckpReader.Read(ctx, tmpBatch); err != nil {
            return
        }
        if end {
            maxTS = c.ckpReader.maxTS
            maxLsn = c.ckpReader.maxLSN
            return
        }
        
        // 解析批次数据
        dbids := vector.MustFixedColNoTypeCheck[uint64](tmpBatch.Vecs[1])
        tableIds := vector.MustFixedColNoTypeCheck[uint64](tmpBatch.Vecs[2])
        objectTypes := vector.MustFixedColNoTypeCheck[int8](tmpBatch.Vecs[3])
        objectStatsVec := tmpBatch.Vecs[4]
        createTSs := vector.MustFixedColNoTypeCheck[types.TS](tmpBatch.Vecs[5])
        deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](tmpBatch.Vecs[6])
        
        // 重放每一行
        for i, rows := 0, tmpBatch.RowCount(); i < rows; i++ {
            if forSys == pkgcatalog.IsSystemTable(tableIds[i]) {
                c.r.catalog.OnReplayObjectBatch_V2(
                    dbids[i],
                    tableIds[i],
                    objectTypes[i],
                    objectio.ObjectStats(objectStatsVec.GetBytesAt(i)),
                    createTSs[i],
                    deleteTSs[i],
                )
            }
        }
    }
}
```

## CheckpointReader

### 结构定义

```go
type CheckpointReader struct {
    checkpointEntries []*CheckpointEntry
    maxGlobalEnd      types.TS

    readers    []*logtail.CKPReader
    currentIdx int
    globalIdx  int
    globalDone bool

    mp *mpool.MPool
    fs fileservice.FileService

    maxTS  types.TS
    maxLSN uint64
}
```

### 准备读取

```go
func (reader *CheckpointReader) Prepare(ctx context.Context) (err error) {
    // 预取数据
    for i, entry := range reader.checkpointEntries {
        if entry.end.LT(&reader.maxGlobalEnd) {
            continue
        }
        ioutil.Prefetch(entry.sid, reader.fs, entry.GetLocation())
        reader.readers[i] = logtail.NewCKPReader(
            entry.version,
            entry.GetLocation(),
            reader.mp,
            reader.fs,
        )
    }

    // 读取元数据
    for i, entry := range reader.checkpointEntries {
        if entry.end.LT(&reader.maxGlobalEnd) {
            continue
        }
        if err = reader.readers[i].ReadMeta(ctx); err != nil {
            return
        }
        reader.readers[i].PrefetchData(entry.sid)
    }
    return
}
```

### 读取数据

```go
func (reader *CheckpointReader) Read(
    ctx context.Context,
    bat *batch.Batch,
) (end bool, err error) {
    // 先读取全局检查点
    if !reader.globalDone && reader.globalIdx != -1 {
        ckpEntry := reader.checkpointEntries[reader.globalIdx]
        if end, err = reader.readers[reader.globalIdx].Read(
            ctx, bat, reader.mp,
        ); err != nil {
            return
        }
        if !end {
            return
        }
        reader.globalDone = true
        if reader.maxTS.LT(&ckpEntry.end) {
            reader.maxTS = ckpEntry.end
        }
        if ckpEntry.ckpLSN > 0 && ckpEntry.ckpLSN < reader.maxLSN {
            reader.maxLSN = ckpEntry.ckpLSN
        }
        reader.maxLSN = ckpEntry.ckpLSN
    }
    
    // 读取增量检查点
    for {
        if reader.currentIdx >= len(reader.readers) {
            return true, nil
        }
        if reader.checkpointEntries[reader.currentIdx] == nil {
            reader.currentIdx++
            continue
        }
        if reader.checkpointEntries[reader.currentIdx].end.LT(&reader.maxGlobalEnd) {
            reader.currentIdx++
            continue
        }
        
        ckpEntry := reader.checkpointEntries[reader.currentIdx]
        if end, err = reader.readers[reader.currentIdx].Read(
            ctx, bat, reader.mp,
        ); err != nil {
            return
        }
        if !end {
            return
        }
        
        if reader.maxTS.LT(&ckpEntry.end) {
            reader.maxTS = ckpEntry.end
        }
        if ckpEntry.ckpLSN != 0 {
            if ckpEntry.ckpLSN < reader.maxLSN {
                panic(fmt.Sprintf("logic error, current lsn %d, incoming lsn %d", 
                    reader.maxLSN, ckpEntry.ckpLSN))
            }
            reader.maxLSN = ckpEntry.ckpLSN
        }
        reader.currentIdx++
    }
}
```

## 并行重放

### 提交任务

```go
func (c *CkpReplayer) Submit(tid uint64, replayFn func()) {
    c.wg.Add(1)
    workerOffset := tid % uint64(len(c.objectReplayWorker))
    c.objectCountMap[tid] = c.objectCountMap[tid] + 1
    c.objectReplayWorker[workerOffset].Enqueue(replayFn)
}
```

### 关闭 Replayer

```go
func (c *CkpReplayer) Close() {
    for _, worker := range c.objectReplayWorker {
        worker.Stop()
    }
}
```

## 恢复流程图

```
┌─────────────────────────────────────────────────────────────────┐
│                    Checkpoint 恢复流程                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. BuildReplayer()                                             │
│     │                                                           │
│     ▼                                                           │
│  2. ReadCkpFiles()                                              │
│     ├── readCheckpointEntries() - 读取元数据文件                 │
│     ├── NewCheckpointReader() - 创建读取器                       │
│     ├── Prepare() - 预取数据                                    │
│     └── ReplayCKPEntry() - 添加条目到 store                     │
│     │                                                           │
│     ▼                                                           │
│  3. ReplayThreeTablesObjectlist()                               │
│     └── replayObjectList(forSys=true) - 重放系统表               │
│     │                                                           │
│     ▼                                                           │
│  4. ReplayCatalog()                                             │
│     └── RelayFromSysTableObjects() - 重放 Catalog               │
│     │                                                           │
│     ▼                                                           │
│  5. ReplayObjectlist()                                          │
│     └── replayObjectList(forSys=false) - 重放用户表              │
│     │                                                           │
│     ▼                                                           │
│  6. Close()                                                     │
│     └── 停止工作线程                                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 读取顺序

```
┌─────────────────────────────────────────────────────────────────┐
│                    检查点读取顺序                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  时间线:                                                        │
│  ─────────────────────────────────────────────────────────────▶│
│      │         │         │         │         │                 │
│    GCKP      ICKP1     ICKP2     ICKP3     ICKP4               │
│   [0,t1]   [t1+1,t2] [t2+1,t3] [t3+1,t4] [t4+1,t5]             │
│                                                                 │
│  读取顺序:                                                      │
│  1. 先读取 GCKP [0,t1] - 全局检查点                             │
│  2. 跳过 ICKP1, ICKP2 (end < maxGlobalEnd)                     │
│  3. 读取 ICKP3 [t3+1,t4]                                       │
│  4. 读取 ICKP4 [t4+1,t5]                                       │
│                                                                 │
│  注意: 只读取 end >= maxGlobalEnd 的增量检查点                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```
