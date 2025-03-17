// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"
)

type BaseCollector_V2 struct {
	*catalog.LoopProcessor
	data       *CheckpointData_V2
	objectSize int
	start, end types.TS
	packer     *types.Packer
}

func NewBaseCollector_V2(start, end types.TS, size int, fs fileservice.FileService) *BaseCollector_V2 {
	collector := &BaseCollector_V2{
		LoopProcessor: &catalog.LoopProcessor{},
		data:          NewCheckpointData_V2(common.CheckpointAllocator, size, fs),
		start:         start,
		end:           end,
		objectSize:    size,
		packer:        types.NewPacker(),
	}
	if collector.objectSize == 0 {
		collector.objectSize = DefaultCheckpointSize
	}
	collector.ObjectFn = collector.visitObject
	collector.TombstoneFn = collector.visitObject
	return collector
}
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

func (collector *BaseCollector_V2) Close() {
	collector.packer.Close()
}
func (collector *BaseCollector_V2) OrphanData() *CheckpointData_V2 {
	data := collector.data
	collector.data = nil
	return data
}
func (collector *BaseCollector_V2) Collect(c *catalog.Catalog) (err error) {
	err = c.RecurLoop(collector)
	return
}

func (collector *BaseCollector_V2) visitObject(entry *catalog.ObjectEntry) error {
	mvccNodes := entry.GetMVCCNodeInRange(collector.start, collector.end)

	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		isObjectTombstone := node.End.Equal(&entry.CreatedAt)
		if err := collectObjectBatch(
			collector.data.batch, entry, isObjectTombstone, collector.packer, collector.data.allocator,
		); err != nil {
			return err
		}
		if collector.data.batch.Size() >= collector.objectSize || collector.data.batch.RowCount() >= DefaultCheckpointBlockRows {
			collector.data.sinker.Write(context.Background(), collector.data.batch)
			collector.data.batch.CleanOnlyData()
		}
	}
	return nil
}

func (collector *BaseCollector_V2) visitObjectForBackup(entry *catalog.ObjectEntry) error {
	createTS := entry.GetCreatedAt()
	if createTS.GT(&collector.start) {
		return nil
	}
	return collector.visitObject(entry)
}

func NewGlobalCollector_V2(
	fs fileservice.FileService,
	end types.TS,
	versionInterval time.Duration,
) *GlobalCollector_V2 {
	versionThresholdTS := types.BuildTS(end.Physical()-versionInterval.Nanoseconds(), end.Logical())
	collector := &GlobalCollector_V2{
		BaseCollector_V2: *NewBaseCollector_V2(types.TS{}, end, 0, fs),
		versionThershold: versionThresholdTS,
	}
	collector.ObjectFn = collector.visitObjectForGlobal
	collector.TombstoneFn = collector.visitObjectForGlobal

	return collector
}

type GlobalCollector_V2 struct {
	BaseCollector_V2
	// not collect objects deleted before versionThershold
	versionThershold types.TS
}

func (collector *GlobalCollector_V2) isEntryDeletedBeforeThreshold(entry catalog.BaseEntry) bool {
	entry.RLock()
	defer entry.RUnlock()
	return entry.DeleteBeforeLocked(collector.versionThershold)
}

func (collector *GlobalCollector_V2) visitObjectForGlobal(entry *catalog.ObjectEntry) error {
	if entry.DeleteBefore(collector.versionThershold) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetTable().BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetTable().GetDB().BaseEntryImpl) {
		return nil
	}
	return collector.BaseCollector_V2.visitObject(entry)
}

func collectObjectBatch(
	data *batch.Batch,
	srcObjectEntry *catalog.ObjectEntry,
	isObjectTombstone bool,
	encoder *types.Packer,
	mp *mpool.MPool,
) (err error) {
	if err = vector.AppendFixed(
		data.Vecs[ckputil.TableObjectsAttr_Accout_Idx], srcObjectEntry.GetTable().GetDB().GetTenantID(), false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed(
		data.Vecs[ckputil.TableObjectsAttr_DB_Idx], srcObjectEntry.GetTable().GetDB().ID, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed(
		data.Vecs[ckputil.TableObjectsAttr_Table_Idx], srcObjectEntry.GetTable().ID, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendBytes(
		data.Vecs[ckputil.TableObjectsAttr_ID_Idx], srcObjectEntry.ObjectStats[:], false, mp,
	); err != nil {
		return
	}
	encoder.Reset()
	objType := ckputil.ObjectType_Data
	if srcObjectEntry.IsTombstone {
		objType = ckputil.ObjectType_Tombstone
	}
	if err = vector.AppendFixed(
		data.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx], objType, false, mp,
	); err != nil {
		return
	}
	ckputil.EncodeCluser(encoder, srcObjectEntry.GetTable().ID, objType, srcObjectEntry.ID())
	if err = vector.AppendBytes(
		data.Vecs[ckputil.TableObjectsAttr_Cluster_Idx], encoder.Bytes(), false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed(
		data.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], srcObjectEntry.CreatedAt, false, mp,
	); err != nil {
		return
	}
	if isObjectTombstone {
		if err = vector.AppendFixed(
			data.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], types.TS{}, false, mp,
		); err != nil {
			return
		}
	} else {
		if err = vector.AppendFixed(
			data.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], srcObjectEntry.DeletedAt, false, mp,
		); err != nil {
			return
		}
	}
	data.SetRowCount(data.Vecs[0].Length())
	return
}

type CheckpointData_V2 struct {
	batch     *batch.Batch
	sinker    *ioutil.Sinker
	allocator *mpool.MPool
}

func NewCheckpointData_V2(
	allocator *mpool.MPool,
	objSize int,
	fs fileservice.FileService,
) *CheckpointData_V2 {
	return &CheckpointData_V2{
		batch: ckputil.NewObjectListBatch(),
		sinker: ckputil.NewDataSinker(
			allocator, fs, ioutil.WithMemorySizeThreshold(objSize),
		),
		allocator: allocator,
	}
}

func NewCheckpointDataWithSinker(sinker *ioutil.Sinker, allocator *mpool.MPool) *CheckpointData_V2 {
	return &CheckpointData_V2{
		sinker:    sinker,
		allocator: allocator,
	}
}

func (data *CheckpointData_V2) WriteTo(
	ctx context.Context,
	fs fileservice.FileService,
) (CNLocation, TNLocation objectio.Location, ckpfiles []string, err error) {
	if data.batch != nil && data.batch.RowCount() != 0 {
		err = data.sinker.Write(ctx, data.batch)
		if err != nil {
			return
		}
	}
	if err = data.sinker.Sync(ctx); err != nil {
		return
	}
	files, inMems := data.sinker.GetResult()
	if len(inMems) != 0 {
		panic("logic error")
	}
	ranges := ckputil.MakeTableRangeBatch()
	defer ranges.Clean(data.allocator)
	if err = ckputil.CollectTableRanges(
		ctx, files, ranges, data.allocator, fs,
	); err != nil {
		return
	}
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
	CNLocation = objectio.BuildLocation(name, blks[0].GetExtent(), 0, blks[0].GetID())
	TNLocation = CNLocation
	ckpfiles = make([]string, 0)
	for _, obj := range files {
		ckpfiles = append(ckpfiles, obj.ObjectName().String())
	}
	return
}
func (data *CheckpointData_V2) Close() {
	data.batch.FreeColumns(data.allocator)
	data.sinker.Close()
}
func (data *CheckpointData_V2) ExportStats(prefix string) []zap.Field {
	fields := make([]zap.Field, 0)
	size := data.batch.Allocated()
	rows := data.batch.RowCount()
	persisted, _ := data.sinker.GetResult()
	for _, obj := range persisted {
		size += int(obj.Size())
		rows += int(obj.Rows())
	}
	fields = append(fields, zap.Int(fmt.Sprintf("%stotalSize", prefix), size))
	fields = append(fields, zap.Int(fmt.Sprintf("%stotalRow", prefix), rows))
	return fields
}

func MockCheckpointV12(
	ctx context.Context,
	c *catalog.Catalog,
	start, end types.TS,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (location objectio.Location, err error) {

	meta := make(map[uint64]*CheckpointMeta)
	dataBatch := makeRespBatchFromSchema(ObjectInfoSchema, mp)
	tombstoneBatch := makeRespBatchFromSchema(ObjectInfoSchema, mp)

	visitObjFn := func(objectEntry *catalog.ObjectEntry, dstBatch *containers.Batch, metaIdx int) {
		mvccNodes := objectEntry.GetMVCCNodeInRange(start, end)
		if len(mvccNodes) == 0 {
			return
		}

		dataStart := dstBatch.GetVectorByName(catalog.ObjectAttr_ObjectStats).Length()
		for _, node := range mvccNodes {
			if node.IsAborted() {
				continue
			}
			create := node.End.Equal(&objectEntry.CreatedAt)
			visitObject(dstBatch, objectEntry, node, create, false, types.TS{})

		}
		dataEnd := dstBatch.GetVectorByName(catalog.ObjectAttr_ObjectStats).Length()
		if dataEnd <= dataStart {
			return
		}
		tid := objectEntry.GetTable().ID
		tableMeta, ok := meta[tid]
		if !ok {
			tableMeta = NewCheckpointMeta()
			meta[tid] = tableMeta
		}

		if dataEnd > dataStart {
			if tableMeta.tables[metaIdx] == nil {
				tableMeta.tables[metaIdx] = NewTableMeta()
				tableMeta.tables[metaIdx].Start = uint64(dataStart)
				tableMeta.tables[metaIdx].End = uint64(dataEnd)
			} else {
				if !tableMeta.tables[metaIdx].TryMerge(common.ClosedInterval{Start: uint64(dataStart), End: uint64(dataEnd)}) {
					panic(fmt.Sprintf("logic error interval %v, start %d, end %d", tableMeta.tables[metaIdx].ClosedInterval, start, end))
				}
			}
		}
	}

	p := &catalog.LoopProcessor{}
	p.ObjectFn = func(oe *catalog.ObjectEntry) error {
		visitObjFn(oe, dataBatch, DataObject)
		return nil
	}
	p.TombstoneFn = func(oe *catalog.ObjectEntry) error {
		visitObjFn(oe, tombstoneBatch, TombstoneObject)
		return nil
	}

	if err = c.RecurLoop(p); err != nil {
		return
	}

	if location, err = mockCheckpointV12_writeTo(
		ctx,
		dataBatch,
		tombstoneBatch,
		meta,
		common.Const1MBytes,
		20,
		mp,
		fs,
	); err != nil {
		return
	}

	return
}

func mockCheckpointV12_writeTo(
	ctx context.Context,
	dataBatch, tombstoneBatch *containers.Batch,
	meta map[uint64]*CheckpointMeta,
	checkpointSize int,
	blockRows int,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (location objectio.Location, err error) {
	checkpointNames := make([]objectio.ObjectName, 1)
	segmentid := objectio.NewSegmentid()
	fileNum := uint16(0)
	name := objectio.BuildObjectName(segmentid, fileNum)
	writer, err := ioutil.NewBlockWriterNew(fs, name, 0, nil, false)
	if err != nil {
		return
	}
	checkpointNames[0] = name
	objectBlocks := make([][]objectio.BlockObject, 0)
	schemas := make([][]uint16, 0)
	schemaTypes := make([]uint16, 0)
	dataIndexes := make([]blockIndexes, 0)
	tombstoneIndexes := make([]blockIndexes, 0)
	var objectSize int
	batFn := func(
		srcBatch *containers.Batch,
		dataType uint16, //5 for data, 7 for tombstone
		indexes *[]blockIndexes,
	) {
		offset := 0
		formatBatch(srcBatch)
		var block objectio.BlockObject
		var bat *containers.Batch
		var size int
		var blks []objectio.BlockObject
		if objectSize > checkpointSize {
			fileNum++
			blks, _, err = writer.Sync(ctx)
			if err != nil {
				return
			}
			name = objectio.BuildObjectName(segmentid, fileNum)
			writer, err = ioutil.NewBlockWriterNew(fs, name, 0, nil, false)
			if err != nil {
				return
			}
			checkpointNames = append(checkpointNames, name)
			objectBlocks = append(objectBlocks, blks)
			schemas = append(schemas, schemaTypes)
			schemaTypes = make([]uint16, 0)
			objectSize = 0
		}
		if srcBatch.Length() == 0 {
			if block, size, err = writer.WriteSubBatch(
				containers.ToCNBatch(srcBatch),
				objectio.DataMetaType(dataType),
			); err != nil {
				return
			}
			blockLoc := BuildBlockLoaction(block.GetID(), uint64(offset), uint64(0))
			*indexes = append(*indexes, blockIndexes{
				fileNum: fileNum,
				indexes: &blockLoc,
			})
			schemaTypes = append(schemaTypes, uint16(dataType-2))
			objectSize += size
		} else {
			split := containers.NewBatchSplitter(srcBatch, blockRows)
			for {
				bat, err = split.Next()
				if err != nil {
					break
				}
				defer bat.Close()
				if block, size, err = writer.WriteSubBatch(containers.ToCNBatch(bat), objectio.DataMetaType(dataType)); err != nil {
					return
				}
				Endoffset := offset + bat.Length()
				blockLoc := BuildBlockLoaction(block.GetID(), uint64(offset), uint64(Endoffset))
				*indexes = append(*indexes, blockIndexes{
					fileNum: fileNum,
					indexes: &blockLoc,
				})
				schemaTypes = append(schemaTypes, dataType-2)
				offset += bat.Length()
				objectSize += size
			}
		}
	}
	batFn(dataBatch, 5, &dataIndexes)
	batFn(tombstoneBatch, 7, &tombstoneIndexes)
	blks, _, err := writer.Sync(ctx)
	if err != nil {
		return
	}
	schemas = append(schemas, schemaTypes)
	objectBlocks = append(objectBlocks, blks)

	tnMetaBatch := prepareTNMeta(checkpointNames, objectBlocks, schemas, mp)

	for _, tableMeta := range meta {
		for i, table := range tableMeta.tables {
			if table == nil || table.ClosedInterval.Start == table.ClosedInterval.End {
				continue
			}

			var indexes []blockIndexes
			switch i {
			case 2:
				indexes = dataIndexes
			case 3:
				indexes = tombstoneIndexes
			default:
				panic("invalid table")
			}
			for _, blockIdx := range indexes {
				block := blockIdx.indexes
				name = checkpointNames[blockIdx.fileNum]
				if table.End <= block.GetStartOffset() {
					break
				}
				if table.Start >= block.GetEndOffset() {
					continue
				}
				blks = objectBlocks[blockIdx.fileNum]
				//blockLoc1 := objectio.BuildLocation(name, blks[block.GetID()].GetExtent(), 0, block.GetID())
				//logutil.Infof("write block %v to %d-%d, table is %d-%d", blockLoc1.String(), block.GetStartOffset(), block.GetEndOffset(), table.Start, table.End)
				if table.Uint64Contains(block.GetStartOffset(), block.GetEndOffset()) {
					blockLoc := BuildBlockLoactionWithLocation(
						name, blks[block.GetID()].GetExtent(), 0, block.GetID(),
						0, block.GetEndOffset()-block.GetStartOffset())
					table.locations.Append(blockLoc)
				} else if block.Contains(table.ClosedInterval) {
					blockLoc := BuildBlockLoactionWithLocation(
						name, blks[block.GetID()].GetExtent(), 0, block.GetID(),
						table.Start-block.GetStartOffset(), table.End-block.GetStartOffset())
					table.locations.Append(blockLoc)
				} else if table.Start <= block.GetEndOffset() && table.Start >= block.GetStartOffset() {
					blockLoc := BuildBlockLoactionWithLocation(
						name, blks[block.GetID()].GetExtent(), 0, block.GetID(),
						table.Start-block.GetStartOffset(), block.GetEndOffset()-block.GetStartOffset())
					table.locations.Append(blockLoc)
				} else if table.End <= block.GetEndOffset() && table.End >= block.GetStartOffset() {
					blockLoc := BuildBlockLoactionWithLocation(
						name, blks[block.GetID()].GetExtent(), 0, block.GetID(),
						0, table.End-block.GetStartOffset())
					table.locations.Append(blockLoc)
				}
			}
		}
	}

	meta[0] = NewCheckpointMeta()
	meta[0].tables[0] = NewTableMeta()
	for num, fileName := range checkpointNames {
		loc := objectBlocks[num][0]
		blockLoc := BuildBlockLoactionWithLocation(
			fileName, loc.GetExtent(), 0, loc.GetID(),
			0, 0)
		meta[0].tables[0].locations.Append(blockLoc)
	}
	cnMetaBatch := prepareCNMeta(meta, mp)
	if err != nil {
		return
	}

	segmentid2 := objectio.NewSegmentid()
	name2 := objectio.BuildObjectName(segmentid2, 0)
	writer2, err := ioutil.NewBlockWriterNew(fs, name2, 0, nil, false)
	if err != nil {
		return
	}
	if _, _, err = writer2.WriteSubBatch(
		containers.ToCNBatch(cnMetaBatch),
		objectio.ConvertToSchemaType(uint16(MetaIDX)),
	); err != nil {
		return
	}
	if _, _, err = writer2.WriteSubBatch(
		containers.ToCNBatch(tnMetaBatch),
		objectio.ConvertToSchemaType(uint16(TNMetaIDX)),
	); err != nil {
		return
	}

	var blks2 []objectio.BlockObject
	if blks2, _, err = writer2.Sync(ctx); err != nil {
		return
	}
	location = objectio.BuildLocation(name2, blks2[0].GetExtent(), 0, blks2[0].GetID())
	// TNLocation = objectio.BuildLocation(name2, blks2[1].GetExtent(), 0, blks2[1].GetID())
	return
}

func prepareCNMeta(meta map[uint64]*CheckpointMeta, mp *mpool.MPool) (bat *containers.Batch) {

	bat = makeRespBatchFromSchema(MetaSchema, mp)
	blkInsLoc := bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchLocation).GetDownstreamVector()
	blkDelLoc := bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchLocation).GetDownstreamVector()
	dataObjectLoc := bat.GetVectorByName(SnapshotMetaAttr_DataObjectBatchLocation).GetDownstreamVector()
	tombstoneObjectLoc := bat.GetVectorByName(SnapshotMetaAttr_TombstoneObjectBatchLocation).GetDownstreamVector()
	tidVec := bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	usageInsLoc := bat.GetVectorByName(CheckpointMetaAttr_StorageUsageInsLocation).GetDownstreamVector()
	usageDelLoc := bat.GetVectorByName(CheckpointMetaAttr_StorageUsageDelLocation).GetDownstreamVector()

	sortMeta := make([]int, 0)
	for tid := range meta {
		sortMeta = append(sortMeta, int(tid))
	}
	sort.Ints(sortMeta)
	for _, tid := range sortMeta {
		vector.AppendFixed[uint64](tidVec, uint64(tid), false, mp)
		if meta[uint64(tid)].tables[BlockInsert] == nil {
			vector.AppendBytes(blkInsLoc, nil, true, mp)
		} else {
			vector.AppendBytes(blkInsLoc, []byte(meta[uint64(tid)].tables[BlockInsert].locations), false, mp)
		}
		if meta[uint64(tid)].tables[BlockDelete] == nil {
			vector.AppendBytes(blkDelLoc, nil, true, mp)
		} else {
			vector.AppendBytes(blkDelLoc, []byte(meta[uint64(tid)].tables[BlockDelete].locations), false, mp)
		}
		if meta[uint64(tid)].tables[DataObject] == nil {
			vector.AppendBytes(dataObjectLoc, nil, true, mp)
		} else {
			vector.AppendBytes(dataObjectLoc, []byte(meta[uint64(tid)].tables[DataObject].locations), false, mp)
		}
		if meta[uint64(tid)].tables[TombstoneObject] == nil {
			vector.AppendBytes(tombstoneObjectLoc, nil, true, mp)
		} else {
			vector.AppendBytes(tombstoneObjectLoc, []byte(meta[uint64(tid)].tables[TombstoneObject].locations), false, mp)
		}

		if meta[uint64(tid)].tables[StorageUsageIns] == nil {
			vector.AppendBytes(usageInsLoc, nil, true, mp)
		} else {
			vector.AppendBytes(usageInsLoc, meta[uint64(tid)].tables[StorageUsageIns].locations, false, mp)
		}

		if meta[uint64(tid)].tables[StorageUsageDel] == nil {
			vector.AppendBytes(usageDelLoc, nil, true, mp)
		} else {
			vector.AppendBytes(usageDelLoc, meta[uint64(tid)].tables[StorageUsageDel].locations, false, mp)
		}
	}
	formatBatch(bat)
	return
}

func prepareTNMeta(
	checkpointNames []objectio.ObjectName,
	objectBlocks [][]objectio.BlockObject,
	schemaTypes [][]uint16,
	mp *mpool.MPool,
) (dstBatch *containers.Batch) {
	dstBatch = makeRespBatchFromSchema(TNMetaSchema, mp)
	for i, blks := range objectBlocks {
		for y, blk := range blks {
			location := objectio.BuildLocation(checkpointNames[i], blk.GetExtent(), 0, blk.GetID())
			dstBatch.GetVectorByName(CheckpointMetaAttr_BlockLocation).Append([]byte(location), false)
			dstBatch.GetVectorByName(CheckpointMetaAttr_SchemaType).Append(schemaTypes[i][y], false)
		}
	}
	return
}
