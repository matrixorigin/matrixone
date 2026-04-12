# 快照与备份

## 概述

Checkpoint 模块支持快照（Snapshot）和备份（Backup）功能，允许用户获取特定时间点的数据视图或创建数据备份。

## 快照机制

### 快照检查点列表

根据快照时间戳获取相关的检查点列表：

```go
func ListSnapshotCheckpoint(
    ctx context.Context,
    sid string,
    fs fileservice.FileService,
    snapshot types.TS,
    allFiles map[string]struct{},
) ([]*CheckpointEntry, error) {
    if len(allFiles) == 0 {
        return nil, nil
    }
    
    metaFiles := make([]ioutil.TSRangeFile, 0)
    compactedFiles := make([]ioutil.TSRangeFile, 0)
    
    // 分类文件
    for name := range allFiles {
        meta := ioutil.DecodeTSRangeFile(name)
        if meta.IsCompactExt() {
            compactedFiles = append(compactedFiles, meta)
        } else {
            metaFiles = append(metaFiles, meta)
        }
    }
    
    return loadCheckpointMeta(
        ctx, sid, 
        getSnapshotMetaFiles(metaFiles, compactedFiles, &snapshot), 
        fs, &snapshot,
    )
}
```

### 获取快照元数据文件

```go
func getSnapshotMetaFiles(
    metaFiles, compactedFiles []ioutil.TSRangeFile,
    snapshot *types.TS,
) []ioutil.TSRangeFile {
    // 按结束时间戳排序
    sort.Slice(compactedFiles, func(i, j int) bool {
        return compactedFiles[i].GetEnd().LT(compactedFiles[j].GetEnd())
    })
    sort.Slice(metaFiles, func(i, j int) bool {
        return metaFiles[i].GetEnd().LT(metaFiles[j].GetEnd())
    })

    retFiles := make([]ioutil.TSRangeFile, 0)
    
    if len(compactedFiles) > 0 {
        file := compactedFiles[len(compactedFiles)-1]
        retFiles = append(retFiles, file)
        
        if snapshot.LE(compactedFiles[len(compactedFiles)-1].GetEnd()) {
            // 压缩检查点已包含快照范围
            // 但为避免数据丢失，还需要一个额外的检查点
            for _, f := range metaFiles {
                if !f.GetStart().IsEmpty() && f.GetStart().GE(file.GetEnd()) {
                    retFiles = append(retFiles, f)
                    break
                }
            }
            return retFiles
        }
    }

    // 过滤元数据文件
    iCkpFiles := FilterSortedMetaFilesByTimestamp(snapshot, metaFiles)
    retFiles = append(retFiles, iCkpFiles...)

    return retFiles
}
```

### 按时间戳过滤元数据文件

```go
func FilterSortedMetaFilesByTimestamp(
    ts *types.TS,
    files []ioutil.TSRangeFile,
) []ioutil.TSRangeFile {
    if len(files) == 0 {
        return nil
    }

    prevFile := files[0]

    // 如果第一个文件是全局检查点且包含时间戳
    if prevFile.GetStart().IsEmpty() && ts.LE(prevFile.GetEnd()) {
        return files[:1]
    }

    for i := 1; i < len(files); i++ {
        curr := files[i]
        // 找到包含时间戳的全局检查点
        if curr.GetStart().IsEmpty() && ts.LE(curr.GetEnd()) {
            if ts.Equal(curr.GetEnd()) {
                return files[:i+1]
            }
            return files[:i]
        }
    }

    return files
}
```

### 过滤快照条目

```go
func filterSnapshotEntries(entries []*CheckpointEntry, snapshot *types.TS) []*CheckpointEntry {
    if len(entries) == 0 {
        return entries
    }

    // 找到最大全局结束时间戳
    var maxGlobalEnd types.TS
    for _, entry := range entries {
        if entry != nil && entry.entryType == ET_Global {
            if entry.end.GT(&maxGlobalEnd) {
                maxGlobalEnd = entry.end
            }
        }
    }

    // 按结束时间戳排序
    sort.Slice(entries, func(i, j int) bool {
        if entries[i] == nil || entries[j] == nil {
            return false
        }
        return entries[i].end.LT(&entries[j].end)
    })

    // 如果快照时间戳等于最大全局结束时间戳
    if snapshot != nil && snapshot.Equal(&maxGlobalEnd) {
        for i := range entries {
            if entries[i].end.Equal(&maxGlobalEnd) &&
                entries[i].entryType == ET_Global {
                return []*CheckpointEntry{entries[i]}
            }
        }
    }
    
    // 找到合适的截断点
    for i := range entries {
        if entries[i] == nil {
            continue
        }
        p := maxGlobalEnd.Prev()
        if entries[i].end.Equal(&p) || (entries[i].end.Equal(&maxGlobalEnd) &&
            entries[i].entryType == ET_Global) {
            return entries[i:]
        }
    }

    return entries
}
```

## 备份机制

### 备份检查点条目

```go
func (s *runnerStore) AddBackupCKPEntry(entry *CheckpointEntry) (success bool) {
    entry.entryType = ET_Incremental
    success = s.AddICKPFinishedEntry(entry)
    if !success {
        return
    }
    
    s.Lock()
    defer s.Unlock()
    
    // 备份后重置 LSN
    it := s.incrementals.Iter()
    for it.Next() {
        e := it.Item()
        e.ckpLSN = 0
        e.truncateLSN = 0
    }
    return
}
```

### 获取备份检查点

```go
func (s *runnerStore) GetAllCheckpointsForBackup(compact *CheckpointEntry) []*CheckpointEntry {
    ckps := make([]*CheckpointEntry, 0)
    var ts types.TS
    
    if compact != nil {
        ts = compact.GetEnd()
        ckps = append(ckps, compact)
    }
    
    s.Lock()
    g := s.MaxFinishedGlobalCheckpointLocked()
    tree := s.incrementals.Copy()
    s.Unlock()
    
    if g != nil {
        if ts.IsEmpty() {
            ts = g.GetEnd()
        }
        ckps = append(ckps, g)
    }
    
    pivot := NewCheckpointEntry(s.sid, ts.Next(), ts.Next(), ET_Incremental)
    iter := tree.Iter()
    defer iter.Release()
    
    if ok := iter.Seek(pivot); ok {
        for {
            e := iter.Item()
            if !e.IsFinished() {
                break
            }
            ckps = append(ckps, e)
            if !iter.Next() {
                break
            }
        }
    }
    return ckps
}
```

### 备份数据收集器

```go
func NewBackupCollector_V2(start, end types.TS, fs fileservice.FileService) *BaseCollector_V2 {
    collector := &BaseCollector_V2{
        LoopProcessor: &catalog.LoopProcessor{},
        data:          NewCheckpointData_V2(common.CheckpointAllocator, 0, fs),
        start:         start,
        end:           end,
        packer:        types.NewPacker(),
    }
    collector.ObjectFn = collector.visitObjectForBackup
    collector.TombstoneFn = collector.visitObjectForBackup
    return collector
}

func (collector *BaseCollector_V2) visitObjectForBackup(entry *catalog.ObjectEntry) error {
    createTS := entry.GetCreatedAt()
    // 只收集在 start 之前创建的对象
    if createTS.GT(&collector.start) {
        return nil
    }
    return collector.visitObject(entry)
}
```

### 合并检查点元数据

```go
func MergeCkpMeta(
    ctx context.Context,
    sid string,
    fs fileservice.FileService,
    cnLocation, tnLocation objectio.Location,
    startTs, ts types.TS,
) (string, error) {
    var metaFiles []ioutil.TSRangeFile
    var err error
    
    if metaFiles, err = ckputil.ListCKPMetaFiles(ctx, fs); err != nil {
        return "", err
    }

    if len(metaFiles) == 0 {
        return "", nil
    }

    // 排序并获取最新文件
    sort.Slice(metaFiles, func(i, j int) bool {
        return metaFiles[i].GetEnd().LT(metaFiles[j].GetEnd())
    })
    maxFile := metaFiles[len(metaFiles)-1]

    // 读取现有元数据
    reader, err := ioutil.NewFileReader(fs, maxFile.GetCKPFullName())
    if err != nil {
        return "", err
    }
    bats, closeCB, err := reader.LoadAllColumns(ctx, nil, common.CheckpointAllocator)
    if err != nil {
        return "", err
    }
    defer func() {
        for i := range bats {
            for j := range bats[i].Vecs {
                bats[i].Vecs[j].Free(common.CheckpointAllocator)
            }
        }
        if closeCB != nil {
            closeCB()
        }
    }()
    
    // 创建新批次
    bat := containers.NewBatch()
    defer bat.Close()
    // ... 复制现有数据 ...

    // 添加备份条目
    last := bat.Vecs[0].Length() - 1
    var cptLocation objectio.LocationSlice
    bat.GetVectorByName(CheckpointAttr_StartTS).Append(startTs, false)
    bat.GetVectorByName(CheckpointAttr_EndTS).Append(ts, false)
    bat.GetVectorByName(CheckpointAttr_MetaLocation).Append([]byte(cnLocation), false)
    bat.GetVectorByName(CheckpointAttr_EntryType).Append(true, false)
    bat.GetVectorByName(CheckpointAttr_Version).Append(
        bat.GetVectorByName(CheckpointAttr_Version).Get(last), false)
    bat.GetVectorByName(CheckpointAttr_AllLocations).Append([]byte(tnLocation), false)
    bat.GetVectorByName(CheckpointAttr_CheckpointLSN).Append(
        bat.GetVectorByName(CheckpointAttr_CheckpointLSN).Get(last), false)
    bat.GetVectorByName(CheckpointAttr_TruncateLSN).Append(
        bat.GetVectorByName(CheckpointAttr_TruncateLSN).Get(last), false)
    bat.GetVectorByName(CheckpointAttr_Type).Append(int8(ET_Backup), false)
    bat.GetVectorByName(CheckpointAttr_TableIDLocation).Append([]byte(cptLocation), false)
    
    // 写入新文件
    name := ioutil.EncodeCKPMetadataFullName(startTs, ts)
    writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, name, fs)
    if err != nil {
        return "", err
    }
    if _, err = writer.Write(containers.ToCNBatch(bat)); err != nil {
        return "", err
    }
    _, err = writer.WriteEnd(ctx)
    return name, err
}
```

## 消费检查点条目

### ConsumeCheckpointEntries

```go
func ConsumeCheckpointEntries(
    ctx context.Context,
    sid string,
    metaLoc string,
    tableID uint64,
    tableName string,
    dbID uint64,
    dbName string,
    forEachObject func(ctx context.Context, fs fileservice.FileService, 
        obj objectio.ObjectEntry, isTombstone bool) error,
    mp *mpool.MPool,
    fs fileservice.FileService,
) (err error) {
    if metaLoc == "" {
        return nil
    }
    
    v2.LogtailLoadCheckpointCounter.Inc()
    now := time.Now()
    defer func() {
        v2.LogTailLoadCheckpointDurationHistogram.Observe(time.Since(now).Seconds())
    }()
    
    // 解析位置和版本
    locationsAndVersions := strings.Split(metaLoc, ";")
    if len(locationsAndVersions)%2 == 1 {
        locationsAndVersions = locationsAndVersions[1:]
    }

    readers := make([]*CKPReader, 0)
    for i := 0; i < len(locationsAndVersions); i += 2 {
        key := locationsAndVersions[i]
        version, err := strconv.ParseUint(locationsAndVersions[i+1], 10, 32)
        if err != nil {
            return err
        }
        location, err := objectio.StringToLocation(key)
        if err != nil {
            return err
        }
        reader := NewCKPReaderWithTableID_V2(uint32(version), location, tableID, mp, fs)
        readers = append(readers, reader)
    }

    // 预取数据
    for _, reader := range readers {
        ioutil.Prefetch(sid, fs, reader.location)
    }

    // 读取元数据
    for _, reader := range readers {
        if err := reader.ReadMeta(ctx); err != nil {
            return err
        }
        reader.PrefetchData(sid)
    }

    // 消费数据
    for _, reader := range readers {
        if err := reader.ConsumeCheckpointWithTableID(ctx, forEachObject); err != nil {
            return err
        }
    }
    return
}
```

## 压缩检查点

### 压缩条目类型

```go
const (
    ET_Compacted EntryType = 3  // 压缩检查点
)
```

### 更新压缩检查点

```go
func (s *runnerStore) UpdateCompacted(entry *CheckpointEntry) (updated bool) {
    for {
        old := s.compacted.Load()
        if old != nil {
            newEnd := entry.GetEnd()
            oldEnd := old.GetEnd()
            if newEnd.LE(&oldEnd) {
                return
            }
        }
        if s.compacted.CompareAndSwap(old, entry) {
            updated = true
            return
        }
    }
}

func (s *runnerStore) GetCompacted() *CheckpointEntry {
    return s.compacted.Load()
}
```

## 示例：快照过滤

```
文件列表:
  [0,100]    - 全局检查点
  [100,200]  - 增量检查点
  [200,300]  - 增量检查点
  [0,300]    - 全局检查点
  [300,400]  - 增量检查点
  [400,500]  - 增量检查点

快照时间戳: 250

过滤结果:
  [0,100]    - 包含
  [100,200]  - 包含
  [200,300]  - 包含

原因: 快照时间戳 250 在 [200,300] 范围内，
      需要 [0,100] 作为基础，加上 [100,200] 和 [200,300] 的增量
```

## 监控指标

```go
v2.LogtailLoadCheckpointCounter.Inc()
v2.LogTailLoadCheckpointDurationHistogram.Observe(time.Since(now).Seconds())
```
