# Checkpoint 数据格式与版本

## 概述

Checkpoint 数据存储在对象存储中，包含元数据文件和数据文件。本章详细介绍数据格式和版本演进。

## 版本历史

```go
const (
    CheckpointVersion12 uint32 = 12  // 旧版本
    CheckpointVersion13 uint32 = 13  // 当前版本

    CheckpointCurrentVersion = CheckpointVersion13
)
```

## 文件组织

### 目录结构

```
ckp/
├── meta_<start_ts>_<end_ts>.ckp      # 元数据文件
├── meta_<start_ts>_<end_ts>.ckp.compact  # 压缩元数据文件
├── <object_name_1>                    # 数据对象文件
├── <object_name_2>
└── ...
```

### 文件命名

```go
// 元数据文件名编码
func EncodeCKPMetadataName(start, end types.TS) string {
    return fmt.Sprintf("meta_%s_%s", start.ToString(), end.ToString())
}

// 完整路径
func EncodeCKPMetadataFullName(start, end types.TS) string {
    return fmt.Sprintf("ckp/meta_%s_%s.ckp", start.ToString(), end.ToString())
}
```

## 元数据 Schema

### Checkpoint Schema

```go
var CheckpointSchemaAttr = []string{
    CheckpointAttr_StartTS,         // 开始时间戳
    CheckpointAttr_EndTS,           // 结束时间戳
    CheckpointAttr_MetaLocation,    // 元数据位置
    CheckpointAttr_EntryType,       // 条目类型（bool: true=增量）
    CheckpointAttr_Version,         // 版本号
    CheckpointAttr_AllLocations,    // 所有位置
    CheckpointAttr_CheckpointLSN,   // 检查点 LSN
    CheckpointAttr_TruncateLSN,     // 截断 LSN
    CheckpointAttr_Type,            // 类型（int8）
    CheckpointAttr_TableIDLocation, // TableID 位置
}

var CheckpointSchemaTypes = []types.Type{
    types.New(types.T_TS, 0, 0),                    // StartTS
    types.New(types.T_TS, 0, 0),                    // EndTS
    types.New(types.T_varchar, types.MaxVarcharLen, 0),  // MetaLocation
    types.New(types.T_bool, 0, 0),                  // EntryType
    types.New(types.T_uint32, 0, 0),                // Version
    types.New(types.T_varchar, types.MaxVarcharLen, 0),  // AllLocations
    types.New(types.T_uint64, 0, 0),                // CheckpointLSN
    types.New(types.T_uint64, 0, 0),                // TruncateLSN
    types.New(types.T_int8, 0, 0),                  // Type
    types.New(types.T_varchar, types.MaxVarcharLen, 0),  // TableIDLocation
}
```

### Schema 版本

```go
const (
    CheckpointSchemaColumnCountV1 = 5   // start, end, loc, type, ver
    CheckpointSchemaColumnCountV2 = 9
    CheckpointSchemaColumnCountV3 = 10  // 添加 TableIDLocation
)
```

## V12 数据格式

### Meta Schema

```go
var MetaSchemaAttr = []string{
    SnapshotAttr_TID,                              // 表 ID
    SnapshotMetaAttr_BlockInsertBatchLocation,     // 块插入位置
    SnapshotMetaAttr_BlockDeleteBatchLocation,     // 块删除位置
    SnapshotMetaAttr_DataObjectBatchLocation,      // 数据对象位置
    SnapshotMetaAttr_TombstoneObjectBatchLocation, // 墓碑对象位置
    CheckpointMetaAttr_StorageUsageInsLocation,    // 存储使用插入位置
    CheckpointMetaAttr_StorageUsageDelLocation,    // 存储使用删除位置
}
```

### Object Info Schema

```go
var ObjectInfoAttr = []string{
    ObjectAttr_ObjectStats,        // 对象统计信息
    SnapshotAttr_DBID,            // 数据库 ID
    SnapshotAttr_TID,             // 表 ID
    EntryNode_CreateAt,           // 创建时间
    EntryNode_DeleteAt,           // 删除时间
    txnbase.SnapshotAttr_StartTS,  // 事务开始时间
    txnbase.SnapshotAttr_PrepareTS, // 事务准备时间
    txnbase.SnapshotAttr_CommitTS,  // 事务提交时间
}

var ObjectInfoTypes = []types.Type{
    types.New(types.T_varchar, types.MaxVarcharLen, 0),
    types.New(types.T_uint64, 0, 0),
    types.New(types.T_uint64, 0, 0),
    types.New(types.T_TS, 0, 0),
    types.New(types.T_TS, 0, 0),
    types.New(types.T_TS, 0, 0),
    types.New(types.T_TS, 0, 0),
    types.New(types.T_TS, 0, 0),
}
```

### BlockLocation

```go
const (
    LocationOffset      = 0
    LocationLength      = objectio.LocationLen
    StartOffsetOffset   = LocationOffset + LocationLength
    StartOffsetLength   = 8
    EndOffsetOffset     = StartOffsetOffset + StartOffsetLength
    EndOffsetLength     = 8
    BlockLocationLength = EndOffsetOffset + EndOffsetLength
)

type BlockLocation []byte

// 布局: Location(objectio.Location) | StartOffset(uint64) | EndOffset(uint64)
```

## V13 数据格式

### 数据批次 Schema

```go
// ckputil 包中定义
var TableObjectsAttrs = []string{
    TableObjectsAttr_Accout,       // 账户 ID
    TableObjectsAttr_DB,           // 数据库 ID
    TableObjectsAttr_Table,        // 表 ID
    TableObjectsAttr_ObjectType,   // 对象类型
    TableObjectsAttr_ID,           // 对象 ID (ObjectStats)
    TableObjectsAttr_CreateTS,     // 创建时间戳
    TableObjectsAttr_DeleteTS,     // 删除时间戳
    TableObjectsAttr_PhysicalAddr, // 物理地址
    TableObjectsAttr_Cluster,      // 聚簇键
}
```

### 对象类型

```go
const (
    ObjectType_Data      int8 = 0  // 数据对象
    ObjectType_Tombstone int8 = 1  // 墓碑对象
)
```

## 数据收集

### collectObjectBatch

```go
func collectObjectBatch(
    data *batch.Batch,
    srcObjectEntry *catalog.ObjectEntry,
    isObjectTombstone bool,
    encoder *types.Packer,
    mp *mpool.MPool,
) (err error) {
    // 账户 ID
    vector.AppendFixed(
        data.Vecs[ckputil.TableObjectsAttr_Accout_Idx], 
        srcObjectEntry.GetTable().GetDB().GetTenantID(), false, mp,
    )
    
    // 数据库 ID
    vector.AppendFixed(
        data.Vecs[ckputil.TableObjectsAttr_DB_Idx], 
        srcObjectEntry.GetTable().GetDB().ID, false, mp,
    )
    
    // 表 ID
    vector.AppendFixed(
        data.Vecs[ckputil.TableObjectsAttr_Table_Idx], 
        srcObjectEntry.GetTable().ID, false, mp,
    )
    
    // 对象统计信息
    vector.AppendBytes(
        data.Vecs[ckputil.TableObjectsAttr_ID_Idx], 
        srcObjectEntry.ObjectStats[:], false, mp,
    )
    
    // 对象类型
    objType := ckputil.ObjectType_Data
    if srcObjectEntry.IsTombstone {
        objType = ckputil.ObjectType_Tombstone
    }
    vector.AppendFixed(
        data.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx], 
        objType, false, mp,
    )
    
    // 聚簇键
    encoder.Reset()
    ckputil.EncodeCluser(encoder, srcObjectEntry.GetTable().ID, objType, 
        srcObjectEntry.ID(), isObjectTombstone)
    vector.AppendBytes(
        data.Vecs[ckputil.TableObjectsAttr_Cluster_Idx], 
        encoder.Bytes(), false, mp,
    )
    
    // 创建时间戳
    vector.AppendFixed(
        data.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], 
        srcObjectEntry.CreatedAt, false, mp,
    )
    
    // 删除时间戳
    if !isObjectTombstone {
        vector.AppendFixed(
            data.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], 
            types.TS{}, false, mp,
        )
    } else {
        vector.AppendFixed(
            data.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], 
            srcObjectEntry.DeletedAt, false, mp,
        )
    }
    
    data.SetRowCount(data.Vecs[0].Length())
    return
}
```

## 数据同步

### CheckpointData_V2.Sync

```go
func (data *CheckpointData_V2) Sync(
    ctx context.Context,
    fs fileservice.FileService,
) (location objectio.Location, ckpfiles []string, err error) {
    // 写入剩余数据
    if data.batch != nil && data.batch.RowCount() != 0 {
        err = data.sinker.Write(ctx, data.batch)
        if err != nil {
            return
        }
    }
    
    // 同步 sinker
    if err = data.sinker.Sync(ctx); err != nil {
        return
    }
    
    // 获取结果文件
    files, inMems := data.sinker.GetResult()
    for _, file := range files {
        data.rows += int(file.Rows())
        data.size += int(file.Size())
    }
    
    // 收集表范围
    ranges := ckputil.MakeTableRangeBatch()
    defer ranges.Clean(data.allocator)
    if err = ckputil.CollectTableRanges(
        ctx, files, ranges, data.allocator, fs,
    ); err != nil {
        return
    }
    
    // 写入元数据
    segmentid := objectio.NewSegmentid()
    fileNum := uint16(0)
    name := objectio.BuildObjectName(segmentid, fileNum)
    writer, err := ioutil.NewBlockWriterNew(fs, name, 0, nil, false)
    if err != nil {
        return
    }
    if _, err = writer.WriteBatch(ranges); err != nil {
        return
    }
    
    var blks []objectio.BlockObject
    if blks, _, err = writer.Sync(ctx); err != nil {
        return
    }
    
    location = objectio.BuildLocation(name, blks[0].GetExtent(), 0, blks[0].GetID())
    
    // 收集文件名
    ckpfiles = make([]string, 0)
    for _, obj := range files {
        ckpfiles = append(ckpfiles, obj.ObjectName().String())
    }
    return
}
```

## TableID 批次

### Schema

```go
var TableIDAttrs = []string{
    TableIDAttr_Account,     // 账户 ID
    TableIDAttr_DBID,        // 数据库 ID
    TableIDAttr_TableID,     // 表 ID
    TableIDAttr_ObjectStart, // 对象开始时间
    TableIDAttr_ObjectEnd,   // 对象结束时间
}

var TableIDTypes = []types.Type{
    types.T_uint32.ToType(),
    types.T_uint64.ToType(),
    types.T_uint64.ToType(),
    types.T_TS.ToType(),
    types.T_TS.ToType(),
}
```

### 同步 TableID

```go
func SyncTableIDBatch(
    ctx context.Context,
    start, end types.TS,
    ttl time.Duration,
    sinkerThreshold int,
    ckpLocation objectio.Location,
    ckpVersion uint32,
    prevTableIDLocation objectio.LocationSlice,
    mp *mpool.MPool,
    fs fileservice.FileService,
) (locations objectio.LocationSlice, err error) {
    // 创建 sinker
    dataFactory := ioutil.NewFSinkerImplFactory(
        TableIDSeqnums, -1, false, false, 0,
    )
    sinker := ioutil.NewSinker(
        -1, TableIDAttrs, TableIDTypes, dataFactory, mp, fs,
        ioutil.WithMemorySizeThreshold(sinkerThreshold),
    )
    defer sinker.Close()
    
    // 读取之前的 TableID 批次
    reader, err := NewSyncTableIDReader(prevTableIDLocation, mp, fs)
    if err != nil {
        return
    }
    for {
        release, bat, isEnd, err := reader.Read(ctx)
        if err != nil {
            return nil, err
        }
        if isEnd {
            break
        }
        defer release()
        consumeFn(bat, release)
    }
    
    // 从当前检查点收集 TableID
    if !ckpLocation.IsEmpty() {
        reader := NewCKPReader(ckpVersion, ckpLocation, common.CheckpointAllocator, fs)
        if err = reader.ReadMeta(ctx); err != nil {
            return
        }
        // 收集表信息...
    }
    
    // 写入特殊行（记录时间范围）
    vector.AppendFixed(bat.Vecs[0], uint32(0), false, mp)
    vector.AppendFixed(bat.Vecs[1], uint64(0), false, mp)
    vector.AppendFixed(bat.Vecs[2], uint64(0), false, mp)  // 特殊 TableID = 0
    vector.AppendFixed(bat.Vecs[3], tableBatchStart, false, mp)
    vector.AppendFixed(bat.Vecs[4], tableBatchEnd, false, mp)
    
    // 同步
    if err = sinker.Write(ctx, bat); err != nil {
        return
    }
    if err = sinker.Sync(ctx); err != nil {
        return
    }
    
    files, _ := sinker.GetResult()
    for _, file := range files {
        location := file.ObjectLocation()
        location.SetID(uint16(file.BlkCnt()))
        locations.Append(location)
    }
    return
}
```

## 版本兼容性

### V12 读取

```go
func readMetaForV12(
    ctx context.Context,
    location objectio.Location,
    mp *mpool.MPool,
    fs fileservice.FileService,
) (data, tombstone map[string]objectio.Location, err error) {
    var reader *ioutil.BlockReader
    if reader, err = ioutil.NewObjectReader(fs, location); err != nil {
        return
    }
    
    attrs := append(BaseAttr, MetaSchemaAttr...)
    typs := append(BaseTypes, MetaShcemaTypes...)
    
    var bats []*containers.Batch
    if bats, err = LoadBlkColumnsByMeta(
        CheckpointVersion12, ctx, typs, attrs, MetaIDX, reader, mp,
    ); err != nil {
        return
    }
    
    metaBatch := bats[0]
    defer metaBatch.Close()
    
    data = make(map[string]objectio.Location)
    tombstone = make(map[string]objectio.Location)
    
    // 解析 BlockLocations
    dataLocationsVec := metaBatch.Vecs[MetaSchema_DataObject_Idx+2]
    tombstoneLocationsVec := metaBatch.Vecs[MetaSchema_TombstoneObject_Idx+2]
    
    for i := 0; i < dataLocationsVec.Length(); i++ {
        dataLocations := BlockLocations(dataLocationsVec.GetDownstreamVector().GetBytesAt(i))
        it := dataLocations.MakeIterator()
        for it.HasNext() {
            loc := it.Next().GetLocation()
            if !loc.IsEmpty() {
                str := loc.Name().String()
                data[str] = loc
            }
        }
        // 类似处理 tombstone...
    }
    return
}
```

### V13 读取

```go
func readMeta(
    ctx context.Context,
    location objectio.Location,
    mp *mpool.MPool,
    fs fileservice.FileService,
) (objects []objectio.ObjectStats, err error) {
    var metaBatch *batch.Batch
    var release func()
    if metaBatch, release, err = readMetaBatch(ctx, location, mp, fs); err != nil {
        return
    }
    defer release()
    return ckputil.ScanObjectStats(metaBatch), nil
}
```

## 常量定义

```go
const DefaultCheckpointBlockRows = 10000
const DefaultCheckpointSize = 512 * 1024 * 1024  // 512MB
```
