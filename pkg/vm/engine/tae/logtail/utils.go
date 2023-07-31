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
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
)

const (
	CheckpointVersion1 uint32 = 1
	CheckpointVersion2 uint32 = 2
	CheckpointVersion3 uint32 = 3

	CheckpointCurrentVersion = CheckpointVersion3
)

const (
	MetaIDX uint16 = iota

	DBInsertIDX
	DBInsertTxnIDX
	DBDeleteIDX
	DBDeleteTxnIDX

	TBLInsertIDX
	TBLInsertTxnIDX
	TBLDeleteIDX
	TBLDeleteTxnIDX
	TBLColInsertIDX
	TBLColDeleteIDX

	SEGInsertIDX
	SEGInsertTxnIDX
	SEGDeleteIDX
	SEGDeleteTxnIDX

	BLKMetaInsertIDX
	BLKMetaInsertTxnIDX
	BLKMetaDeleteIDX
	BLKMetaDeleteTxnIDX

	BLKDNMetaInsertIDX
	BLKDNMetaInsertTxnIDX
	BLKDNMetaDeleteIDX
	BLKDNMetaDeleteTxnIDX

	BLKCNMetaInsertIDX
)

const MaxIDX = BLKCNMetaInsertIDX + 1

const (
	Checkpoint_Meta_TID_IDX                = 2
	Checkpoint_Meta_Insert_Block_Start_IDX = 3
	Checkpoint_Meta_Insert_Block_End_IDX   = 4
	Checkpoint_Meta_Insert_Block_LOC       = 5
	Checkpoint_Meta_Delete_Block_Start_IDX = 6
	Checkpoint_Meta_Delete_Block_End_IDX   = 7
	Checkpoint_Meta_Delete_Block_LOC       = 8
	Checkpoint_Meta_Segment_Start_IDX      = 9
	Checkpoint_Meta_Segment_End_IDX        = 10
	Checkpoint_Meta_Segment_LOC            = 11
)

type checkpointDataItem struct {
	schema *catalog.Schema
	types  []types.Type
	attrs  []string
}

var checkpointDataSchemas_V1 [MaxIDX]*catalog.Schema
var checkpointDataSchemas_V2 [MaxIDX]*catalog.Schema
var checkpointDataSchemas_V3 [MaxIDX]*catalog.Schema
var checkpointDataSchemas_Curr [MaxIDX]*catalog.Schema

var checkpointDataRefer_V1 [MaxIDX]*checkpointDataItem
var checkpointDataRefer_V2 [MaxIDX]*checkpointDataItem
var checkpointDataRefer_V3 [MaxIDX]*checkpointDataItem
var checkpointDataReferVersions map[uint32][MaxIDX]*checkpointDataItem

func init() {
	checkpointDataSchemas_V1 = [MaxIDX]*catalog.Schema{
		MetaSchema_V1,
		catalog.SystemDBSchema,
		TxnNodeSchema,
		DelSchema, // 3
		DBDNSchema,
		catalog.SystemTableSchema_V1,
		TblDNSchema,
		DelSchema, // 7
		TblDNSchema,
		catalog.SystemColumnSchema,
		DelSchema,
		SegSchema, // 11
		SegDNSchema,
		DelSchema,
		SegDNSchema,
		BlkMetaSchema, // 15
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 19
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 23
	}
	checkpointDataSchemas_V2 = [MaxIDX]*catalog.Schema{
		MetaSchema_V1,
		catalog.SystemDBSchema,
		TxnNodeSchema,
		DelSchema, // 3
		DBDNSchema,
		catalog.SystemTableSchema,
		TblDNSchema,
		DelSchema, // 7
		TblDNSchema,
		catalog.SystemColumnSchema,
		DelSchema,
		SegSchema, // 11
		SegDNSchema,
		DelSchema,
		SegDNSchema,
		BlkMetaSchema, // 15
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 19
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 23
	}
	checkpointDataSchemas_V3 = [MaxIDX]*catalog.Schema{
		MetaSchema,
		catalog.SystemDBSchema,
		TxnNodeSchema,
		DBDelSchema, // 3
		DBDNSchema,
		catalog.SystemTableSchema,
		TblDNSchema,
		TblDelSchema, // 7
		TblDNSchema,
		catalog.SystemColumnSchema,
		ColumnDelSchema,
		SegSchema, // 11
		SegDNSchema,
		DelSchema,
		SegDNSchema,
		BlkMetaSchema, // 15
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 19
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 23
	}

	checkpointDataSchemas_Curr = checkpointDataSchemas_V3
	checkpointDataReferVersions = make(map[uint32][24]*checkpointDataItem)

	for idx, schema := range checkpointDataSchemas_V1 {
		checkpointDataRefer_V1[idx] = &checkpointDataItem{
			schema,
			append(BaseTypes, schema.Types()...),
			append(BaseAttr, schema.AllNames()...),
		}
	}
	checkpointDataReferVersions[CheckpointVersion1] = checkpointDataRefer_V1
	for idx, schema := range checkpointDataSchemas_V2 {
		checkpointDataRefer_V2[idx] = &checkpointDataItem{
			schema,
			append(BaseTypes, schema.Types()...),
			append(BaseAttr, schema.AllNames()...),
		}
	}
	checkpointDataReferVersions[CheckpointVersion2] = checkpointDataRefer_V2
	for idx, schema := range checkpointDataSchemas_V3 {
		checkpointDataRefer_V3[idx] = &checkpointDataItem{
			schema,
			append(BaseTypes, schema.Types()...),
			append(BaseAttr, schema.AllNames()...),
		}
	}
	checkpointDataReferVersions[CheckpointVersion3] = checkpointDataRefer_V3
}

func IncrementalCheckpointDataFactory(start, end types.TS) func(c *catalog.Catalog) (*CheckpointData, error) {
	return func(c *catalog.Catalog) (data *CheckpointData, err error) {
		collector := NewIncrementalCollector(start, end)
		defer collector.Close()
		err = c.RecurLoop(collector)
		if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			err = nil
		}
		data = collector.OrphanData()
		return
	}
}

func GlobalCheckpointDataFactory(end types.TS, versionInterval time.Duration) func(c *catalog.Catalog) (*CheckpointData, error) {
	return func(c *catalog.Catalog) (data *CheckpointData, err error) {
		collector := NewGlobalCollector(end, versionInterval)
		defer collector.Close()
		err = c.RecurLoop(collector)
		if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			err = nil
		}
		data = collector.OrphanData()
		return
	}
}

type CheckpointMeta struct {
	blkInsertOffset   *common.ClosedInterval
	blkDeleteOffset   *common.ClosedInterval
	segDeleteOffset   *common.ClosedInterval
	blkInsertLocation objectio.Location
	blkDeleteLocation objectio.Location
	segDeleteLocation objectio.Location
}

func NewCheckpointMeta() *CheckpointMeta {
	return &CheckpointMeta{}
}

type CheckpointData struct {
	meta      map[uint64]*CheckpointMeta
	locations map[string]objectio.Location
	bats      [MaxIDX]*containers.Batch
}

func NewCheckpointData() *CheckpointData {
	data := &CheckpointData{
		meta: make(map[uint64]*CheckpointMeta),
	}
	for idx, schema := range checkpointDataSchemas_Curr {
		data.bats[idx] = makeRespBatchFromSchema(schema)
	}
	return data
}

type BaseCollector struct {
	*catalog.LoopProcessor
	start, end types.TS

	data *CheckpointData
}

type IncrementalCollector struct {
	*BaseCollector
}

func NewIncrementalCollector(start, end types.TS) *IncrementalCollector {
	collector := &IncrementalCollector{
		BaseCollector: &BaseCollector{
			LoopProcessor: new(catalog.LoopProcessor),
			data:          NewCheckpointData(),
			start:         start,
			end:           end,
		},
	}
	collector.DatabaseFn = collector.VisitDB
	collector.TableFn = collector.VisitTable
	collector.SegmentFn = collector.VisitSeg
	collector.BlockFn = collector.VisitBlk
	return collector
}

type GlobalCollector struct {
	*BaseCollector
	versionThershold types.TS
}

func NewGlobalCollector(end types.TS, versionInterval time.Duration) *GlobalCollector {
	versionThresholdTS := types.BuildTS(end.Physical()-versionInterval.Nanoseconds(), end.Logical())
	collector := &GlobalCollector{
		BaseCollector: &BaseCollector{
			LoopProcessor: new(catalog.LoopProcessor),
			data:          NewCheckpointData(),
			end:           end,
		},
		versionThershold: versionThresholdTS,
	}
	collector.DatabaseFn = collector.VisitDB
	collector.TableFn = collector.VisitTable
	collector.SegmentFn = collector.VisitSeg
	collector.BlockFn = collector.VisitBlk
	return collector
}

func (data *CheckpointData) ApplyReplayTo(
	c *catalog.Catalog,
	dataFactory catalog.DataFactory,
) (err error) {
	c.OnReplayDatabaseBatch(data.GetDBBatchs())
	ins, colins, dnins, del, dndel := data.GetTblBatchs()
	c.OnReplayTableBatch(ins, colins, dnins, del, dndel, dataFactory)
	ins, dnins, del, dndel = data.GetSegBatchs()
	c.OnReplaySegmentBatch(ins, dnins, del, dndel, dataFactory)
	ins, dnins, del, dndel = data.GetDNBlkBatchs()
	c.OnReplayBlockBatch(ins, dnins, del, dndel, dataFactory)
	ins, dnins, del, dndel = data.GetBlkBatchs()
	c.OnReplayBlockBatch(ins, dnins, del, dndel, dataFactory)
	return
}

type CNCheckpointData struct {
	meta map[uint64]*CheckpointMeta
	bats [MaxIDX]*batch.Batch
}

func NewCNCheckpointData() *CNCheckpointData {
	return &CNCheckpointData{
		meta: make(map[uint64]*CheckpointMeta),
	}
}

func (data *CNCheckpointData) PrefetchFrom(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location) (err error) {
	return prefetchCheckpointData(ctx, version, service, key)
}

func (data *CNCheckpointData) GetTableMeta(tableID uint64) (meta *CheckpointMeta) {
	if len(data.meta) != 0 {
		meta = data.meta[tableID]
		return
	}
	for i := 0; i < data.bats[MetaIDX].Vecs[Checkpoint_Meta_TID_IDX].Length(); i++ {
		tid := vector.MustFixedCol[uint64](data.bats[MetaIDX].Vecs[Checkpoint_Meta_TID_IDX])[i]
		insStart := vector.MustFixedCol[int32](data.bats[MetaIDX].Vecs[Checkpoint_Meta_Insert_Block_Start_IDX])[i]
		insEnd := vector.MustFixedCol[int32](data.bats[MetaIDX].Vecs[Checkpoint_Meta_Insert_Block_End_IDX])[i]
		delStart := vector.MustFixedCol[int32](data.bats[MetaIDX].Vecs[Checkpoint_Meta_Delete_Block_Start_IDX])[i]
		delEnd := vector.MustFixedCol[int32](data.bats[MetaIDX].Vecs[Checkpoint_Meta_Delete_Block_End_IDX])[i]
		segDelStart := vector.MustFixedCol[int32](data.bats[MetaIDX].Vecs[Checkpoint_Meta_Segment_Start_IDX])[i]
		segDelEnd := vector.MustFixedCol[int32](data.bats[MetaIDX].Vecs[Checkpoint_Meta_Segment_End_IDX])[i]
		meta := new(CheckpointMeta)
		if insStart != -1 {
			meta.blkInsertOffset = &common.ClosedInterval{
				Start: uint64(insStart),
				End:   uint64(insEnd),
			}
		}
		if delStart != -1 {
			meta.blkDeleteOffset = &common.ClosedInterval{
				Start: uint64(delStart),
				End:   uint64(delEnd),
			}
		}
		if segDelStart != -1 {
			meta.segDeleteOffset = &common.ClosedInterval{
				Start: uint64(segDelStart),
				End:   uint64(segDelEnd),
			}
		}
		data.meta[tid] = meta
		// logutil.Infof("GetTableMeta TID=%d, INTERVAL=%s", tid, meta.blkInsertOffset.String())
	}
	meta = data.meta[tableID]
	return
}

func (data *CNCheckpointData) ReadFrom(
	ctx context.Context,
	reader *blockio.BlockReader,
	version uint32,
	m *mpool.MPool,
) (err error) {

	var bat *batch.Batch
	metaIdx := checkpointDataReferVersions[version][MetaIDX]
	bat, err = LoadCNSubBlkColumnsByMeta(ctx, metaIdx.types, metaIdx.attrs, MetaIDX, reader, m)
	if err != nil {
		return
	}
	data.bats[MetaIDX] = bat
	key := objectio.Location(bat.Vecs[5].GetBytesAt(0))
	for idx, item := range checkpointDataReferVersions[version] {
		if uint16(idx) == MetaIDX {
			continue
		}
		reader, err = blockio.NewObjectReader(reader.GetObjectReader().GetObject().GetFs(), key)
		var bat *batch.Batch
		bat, err = LoadCNSubBlkColumnsByMeta(ctx, item.types, item.attrs, uint16(idx), reader, m)
		if err != nil {
			return
		}
		data.bats[idx] = bat
	}
	if version == CheckpointVersion1 {
		bat := data.bats[TBLInsertIDX]
		if bat == nil {
			return
		}
		versionVec := vector.MustFixedCol[uint32](bat.Vecs[pkgcatalog.MO_TABLES_VERSION_IDX+2]) // 2 for rowid and committs
		length := len(versionVec)
		vec := vector.NewVec(types.T_uint32.ToType())
		for i := 0; i < length; i++ {
			err := vector.AppendFixed(vec, pkgcatalog.CatalogVersion_V1, false, m)
			if err != nil {
				return err
			}
		}
		bat.Attrs = append(bat.Attrs, pkgcatalog.SystemRelAttr_CatalogVersion)
		bat.Vecs = append(bat.Vecs, vec)
	}
	if version <= CheckpointVersion2 {
		bat := data.bats[DBDeleteIDX]
		if bat == nil {
			return
		}
		rowIDVec := vector.MustFixedCol[types.Rowid](bat.Vecs[0])
		length := len(rowIDVec)
		pkVec := vector.NewVec(types.T_uint64.ToType())
		for i := 0; i < length; i++ {
			err := vector.AppendFixed(pkVec, rowIDToU64(rowIDVec[i]), false, m)
			if err != nil {
				return err
			}
		}
		bat.Attrs = append(bat.Attrs, pkgcatalog.SystemDBAttr_ID)
		bat.Vecs = append(bat.Vecs, pkVec)

		bat = data.bats[TBLDeleteIDX]
		if bat == nil {
			return
		}
		rowIDVec = vector.MustFixedCol[types.Rowid](bat.Vecs[0])
		length = len(rowIDVec)
		pkVec2 := vector.NewVec(types.T_uint64.ToType())
		for i := 0; i < length; i++ {
			err := vector.AppendFixed(pkVec2, rowIDToU64(rowIDVec[i]), false, m)
			if err != nil {
				return err
			}
		}
		bat.Attrs = append(bat.Attrs, pkgcatalog.SystemRelAttr_ID)
		bat.Vecs = append(bat.Vecs, pkVec2)
	}
	return
}

func (data *CNCheckpointData) GetCloseCB(version uint32, m *mpool.MPool) func() {
	return func() {
		if version == CheckpointVersion1 {
			data.closeVector(TBLInsertIDX, pkgcatalog.MO_TABLES_CATALOG_VERSION_IDX+2, m) // 2 for rowid and committs
		}
		if version <= CheckpointVersion2 {
			data.closeVector(DBDeleteIDX, 2, m)
			data.closeVector(TBLDeleteIDX, 2, m)
		}
	}
}

func (data *CNCheckpointData) closeVector(batIdx uint16, colIdx int, m *mpool.MPool) {
	bat := data.bats[batIdx]
	if bat == nil {
		return
	}
	if len(bat.Vecs) <= colIdx {
		return
	}
	vec := data.bats[TBLInsertIDX].Vecs[colIdx]
	vec.Free(m)

}

func (data *CNCheckpointData) GetTableData(tid uint64) (ins, del, cnIns, segDel *api.Batch, err error) {
	var insTaeBat, delTaeBat, cnInsTaeBat, segDelTaeBat *batch.Batch
	switch tid {
	case pkgcatalog.MO_DATABASE_ID:
		insTaeBat = data.bats[DBInsertIDX]
		delTaeBat = data.bats[DBDeleteIDX]
		if insTaeBat != nil {
			ins, err = batch.BatchToProtoBatch(insTaeBat)
			if err != nil {
				return
			}
		}
		if delTaeBat != nil {
			del, err = batch.BatchToProtoBatch(delTaeBat)
			if err != nil {
				return
			}
		}
		return
	case pkgcatalog.MO_TABLES_ID:
		insTaeBat = data.bats[TBLInsertIDX]
		delTaeBat = data.bats[TBLDeleteIDX]
		if insTaeBat != nil {
			ins, err = batch.BatchToProtoBatch(insTaeBat)
			if err != nil {
				return
			}
		}
		if delTaeBat != nil {
			del, err = batch.BatchToProtoBatch(delTaeBat)
			if err != nil {
				return
			}
		}
		return
	case pkgcatalog.MO_COLUMNS_ID:
		insTaeBat = data.bats[TBLColInsertIDX]
		delTaeBat = data.bats[TBLColDeleteIDX]
		if insTaeBat != nil {
			ins, err = batch.BatchToProtoBatch(insTaeBat)
			if err != nil {
				return
			}
		}
		if delTaeBat != nil {
			del, err = batch.BatchToProtoBatch(delTaeBat)
			if err != nil {
				return
			}
		}
		return
	}

	// For Debug
	// if insTaeBat != nil {
	// 	logutil.Infof("GetTableData: TID=%d %s", tid, BatchToString("INS-DATA", insTaeBat, true))
	// }
	// if delTaeBat != nil {
	// 	logutil.Infof("GetTableData: TID=%d %s", tid, BatchToString("DEL-DATA", delTaeBat, true))
	// }

	meta := data.GetTableMeta(tid)
	if meta == nil {
		return nil, nil, nil, nil, nil
	}

	insInterval := meta.blkInsertOffset
	if insInterval != nil && insInterval.End-insInterval.Start > 0 {
		insTaeBat = data.bats[BLKMetaInsertIDX]
		windowCNBatch(insTaeBat, insInterval.Start, insInterval.End)
		ins, err = batch.BatchToProtoBatch(insTaeBat)
		if err != nil {
			return
		}
	}

	delInterval := meta.blkDeleteOffset
	if delInterval != nil && delInterval.End-delInterval.Start > 0 {
		delTaeBat = data.bats[BLKMetaDeleteIDX]
		cnInsTaeBat = data.bats[BLKCNMetaInsertIDX]
		windowCNBatch(delTaeBat, delInterval.Start, delInterval.End)
		windowCNBatch(cnInsTaeBat, delInterval.Start, delInterval.End)
		del, err = batch.BatchToProtoBatch(delTaeBat)
		if err != nil {
			return
		}
		cnIns, err = batch.BatchToProtoBatch(cnInsTaeBat)
		if err != nil {
			return
		}
	}

	segDelInterval := meta.segDeleteOffset
	if segDelInterval != nil && segDelInterval.End-segDelInterval.Start > 0 {
		segDelTaeBat = data.bats[SEGDeleteIDX]
		windowCNBatch(segDelTaeBat, segDelInterval.Start, segDelInterval.End)
		segDel, err = batch.BatchToProtoBatch(segDelTaeBat)
		if err != nil {
			return
		}
	}

	// For debug
	// if insTaeBat != nil {
	// 	logutil.Infof("GetTableData: TID=%d %s", tid, BatchToString("INS-BLK-DATA", insTaeBat, true))
	// }
	// if delTaeBat != nil {
	// 	logutil.Infof("GetTableData: TID=%d %s", tid, BatchToString("DEL-BLK-DATA", delTaeBat, true))
	// }
	// if cnInsTaeBat != nil {
	// 	logutil.Infof("GetTableData: TID=%d %s", tid, BatchToString("CN-INS-DATA", cnInsTaeBat, true))
	// }
	return
}

func windowCNBatch(bat *batch.Batch, start, end uint64) {
	var err error
	for i, vec := range bat.Vecs {
		bat.Vecs[i], err = vec.Window(int(start), int(end))
		if err != nil {
			panic(err)
		}
	}
}

func (data *CheckpointData) prepareMeta(key []byte) {
	bat := data.bats[MetaIDX]
	blkInsStart := bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).GetDownstreamVector()
	blkInsEnd := bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchEnd).GetDownstreamVector()
	blkInsLoc := bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchLocation).GetDownstreamVector()

	blkDelStart := bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchStart).GetDownstreamVector()
	blkDelEnd := bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchEnd).GetDownstreamVector()
	blkDelLoc := bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchLocation).GetDownstreamVector()

	segDelStart := bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchStart).GetDownstreamVector()
	segDelEnd := bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchEnd).GetDownstreamVector()
	segDelLoc := bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchLocation).GetDownstreamVector()

	tidVec := bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()

	for tid, meta := range data.meta {
		vector.AppendFixed(tidVec, tid, false, common.DefaultAllocator)
		if meta.blkInsertOffset == nil {
			vector.AppendFixed(blkInsStart, int32(-1), false, common.DefaultAllocator)
			vector.AppendFixed(blkInsEnd, int32(-1), false, common.DefaultAllocator)
		} else {
			vector.AppendFixed(blkInsStart, int32(meta.blkInsertOffset.Start), false, common.DefaultAllocator)
			vector.AppendFixed(blkInsEnd, int32(meta.blkInsertOffset.End), false, common.DefaultAllocator)
		}
		vector.AppendBytes(blkInsLoc, key, false, common.DefaultAllocator)
		if meta.blkDeleteOffset == nil {
			vector.AppendFixed(blkDelStart, int32(-1), false, common.DefaultAllocator)
			vector.AppendFixed(blkDelEnd, int32(-1), false, common.DefaultAllocator)
		} else {
			vector.AppendFixed(blkDelStart, int32(meta.blkDeleteOffset.Start), false, common.DefaultAllocator)
			vector.AppendFixed(blkDelEnd, int32(meta.blkDeleteOffset.End), false, common.DefaultAllocator)
		}
		vector.AppendBytes(blkDelLoc, key, false, common.DefaultAllocator)
		if meta.segDeleteOffset == nil {
			vector.AppendFixed(segDelStart, int32(-1), false, common.DefaultAllocator)
			vector.AppendFixed(segDelEnd, int32(-1), false, common.DefaultAllocator)
		} else {
			vector.AppendFixed(segDelStart, int32(meta.segDeleteOffset.Start), false, common.DefaultAllocator)
			vector.AppendFixed(segDelEnd, int32(meta.segDeleteOffset.End), false, common.DefaultAllocator)
		}
		vector.AppendBytes(segDelLoc, key, false, common.DefaultAllocator)
	}
}

func (data *CheckpointData) UpdateBlkMeta(tid uint64, insStart, insEnd, delStart, delEnd int32) {
	if delEnd < delStart && insEnd < insStart {
		return
	}
	meta, ok := data.meta[tid]
	if !ok {
		meta = NewCheckpointMeta()
		data.meta[tid] = meta
	}
	if delEnd >= delStart {
		if meta.blkDeleteOffset == nil {
			meta.blkDeleteOffset = &common.ClosedInterval{Start: uint64(delStart), End: uint64(delEnd)}
		} else {
			if !meta.blkDeleteOffset.TryMerge(common.ClosedInterval{Start: uint64(delStart), End: uint64(delEnd)}) {
				panic(fmt.Sprintf("logic error interval %v, start %d, end %d", meta.blkDeleteOffset, delStart, delEnd))
			}
		}
	}
	if insEnd >= insStart {
		if meta.blkInsertOffset == nil {
			meta.blkInsertOffset = &common.ClosedInterval{Start: uint64(insStart), End: uint64(insEnd)}
		} else {
			if !meta.blkInsertOffset.TryMerge(common.ClosedInterval{Start: uint64(insStart), End: uint64(insEnd)}) {
				panic(fmt.Sprintf("logic error interval %v, start %d, end %d", meta.blkInsertOffset, insStart, insEnd))
			}
		}
	}
}

func (data *CheckpointData) updateBlkInsertLocation(tid uint64, location objectio.Location) {
	data.meta[tid].blkInsertLocation = location
}

func (data *CheckpointData) updateBlkDeleteLocation(tid uint64, location objectio.Location) {
	data.meta[tid].blkDeleteLocation = location
}

func (data *CheckpointData) UpdateSegMeta(tid uint64, delStart, delEnd int32) {
	if delEnd < delStart {
		return
	}
	meta, ok := data.meta[tid]
	if !ok {
		meta = NewCheckpointMeta()
		data.meta[tid] = meta
	}
	if delEnd >= delStart {
		if meta.segDeleteOffset == nil {
			meta.segDeleteOffset = &common.ClosedInterval{Start: uint64(delStart), End: uint64(delEnd)}
		} else {
			if !meta.segDeleteOffset.TryMerge(common.ClosedInterval{Start: uint64(delStart), End: uint64(delEnd)}) {
				panic(fmt.Sprintf("logic error interval %v, start %d, end %d", meta.segDeleteOffset, delStart, delEnd))
			}
		}
	}
}

func (data *CheckpointData) updateSegmentLocation(tid uint64, location objectio.Location) {
	data.meta[tid].segDeleteLocation = location
}

func (data *CheckpointData) PrintData() {
	logutil.Info(BatchToString("BLK-META-DEL-BAT", data.bats[BLKMetaDeleteIDX], true))
	logutil.Info(BatchToString("BLK-META-INS-BAT", data.bats[BLKMetaInsertIDX], true))
}

func (data *CheckpointData) WriteTo(
	fs fileservice.FileService,
) (location objectio.Location, err error) {
	segmentid := objectio.NewSegmentid()
	name := objectio.BuildObjectName(segmentid, 0)
	writer, err := blockio.NewBlockWriterNew(fs, name, 0, nil)
	if err != nil {
		return
	}
	for i := range checkpointDataSchemas_Curr {
		if i == int(MetaIDX) {
			continue
		}
		if _, err = writer.WriteSubBatch(containers.ToCNBatch(data.bats[i]), objectio.ConvertToSchemaType(uint16(i))); err != nil {
			return
		}
	}
	blks, _, err := writer.Sync(context.Background())

	location = objectio.BuildLocation(name, blks[0].GetExtent(), 0, blks[0].GetID())
	for tid := range data.meta {
		data.updateBlkDeleteLocation(tid, location)
		data.updateBlkInsertLocation(tid, location)
		data.updateSegmentLocation(tid, location)
	}
	//mocatalog
	for tid := uint64(1); tid < 4; tid++ {
		data.meta[tid] = NewCheckpointMeta()
		data.updateBlkDeleteLocation(tid, location)
		data.updateBlkInsertLocation(tid, location)
		data.updateSegmentLocation(tid, location)
	}
	//data.prepareMeta()
	data.prepareMeta(location)
	if err != nil {
		return
	}

	segmentid2 := objectio.NewSegmentid()
	name2 := objectio.BuildObjectName(segmentid2, 0)
	writer2, err := blockio.NewBlockWriterNew(fs, name2, 0, nil)
	if err != nil {
		return
	}
	if _, err = writer2.WriteSubBatch(
		containers.ToCNBatch(data.bats[MetaIDX]),
		objectio.ConvertToSchemaType(uint16(MetaIDX))); err != nil {
		return
	}
	if err != nil {
		return
	}
	blks2, _, err := writer2.Sync(context.Background())
	location = objectio.BuildLocation(name2, blks2[0].GetExtent(), 0, blks2[0].GetID())
	return
}

func LoadBlkColumnsByMeta(
	cxt context.Context,
	colTypes []types.Type,
	colNames []string,
	id uint16,
	reader *blockio.BlockReader,
) (*containers.Batch, error) {
	bat := containers.NewBatch()
	idxs := make([]uint16, len(colNames))
	for i := range colNames {
		idxs[i] = uint16(i)
	}
	ioResult, err := reader.LoadSubColumns(cxt, idxs, nil, id, nil)
	if err != nil {
		return nil, err
	}

	for i, idx := range idxs {
		pkgVec := ioResult.Vecs[i]
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(colTypes[i])
		} else {
			vec = containers.ToDNVector(pkgVec)
		}
		bat.AddVector(colNames[idx], vec)
		bat.Vecs[i] = vec

	}
	return bat, nil
}

func LoadCNSubBlkColumnsByMeta(
	cxt context.Context,
	colTypes []types.Type,
	colNames []string,
	id uint16,
	reader *blockio.BlockReader,
	m *mpool.MPool,
) (*batch.Batch, error) {
	idxs := make([]uint16, len(colNames))
	for i := range colNames {
		idxs[i] = uint16(i)
	}
	ioResult, err := reader.LoadSubColumns(cxt, idxs, nil, id, nil)
	if err != nil {
		return nil, err
	}
	ioResult.Attrs = make([]string, len(colNames))
	copy(ioResult.Attrs, colNames)
	maxLength := 0
	for _, vec := range ioResult.Vecs {
		length := vec.Length()
		if maxLength < length {
			maxLength = length
		}
	}
	ioResult.SetRowCount(maxLength)
	return ioResult, nil
}

func (data *CheckpointData) PrefetchFrom(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location) (err error) {
	return prefetchCheckpointData(ctx, version, service, key)
}

func prefetchCheckpointData(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location,
) (err error) {
	// FIXME
	return nil
	pref, err := blockio.BuildSubPrefetchParams(service, key)
	if err != nil {
		return
	}
	for idx, item := range checkpointDataReferVersions[version] {
		idxes := make([]uint16, len(item.attrs))
		for i := range item.attrs {
			idxes[i] = uint16(i)
		}
		pref.AddBlock(idxes, []uint16{uint16(idx)})
	}
	return blockio.PrefetchWithMerged(pref)
}

func prefetchMetaBatch(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location) (err error) {

	pref, err := blockio.BuildSubPrefetchParams(service, key)
	if err != nil {
		return
	}
	item := checkpointDataReferVersions[version][MetaIDX]
	idxes := make([]uint16, len(item.attrs))
	for i := range item.attrs {
		idxes[i] = uint16(i)
	}
	pref.AddBlock(idxes, []uint16{uint16(MetaIDX)})
	return blockio.PrefetchWithMerged(pref)
}

func prefetchBatches(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	schemaIdxes []uint16,
	key objectio.Location,
) (err error) {
	pref, err := blockio.BuildSubPrefetchParams(service, key)
	if err != nil {
		return
	}
	for _, idx := range schemaIdxes {
		item := checkpointDataReferVersions[version][idx]
		idxes := make([]uint16, len(item.attrs))
		for i := range item.attrs {
			idxes[i] = uint16(i)
		}
		pref.AddBlock(idxes, []uint16{uint16(idx)})
	}
	return blockio.PrefetchWithMerged(pref)
}

// TODO:
// There need a global io pool
func (data *CheckpointData) ReadFrom(
	ctx context.Context,
	version uint32,
	reader *blockio.BlockReader,
	fs fileservice.FileService,
	m *mpool.MPool,
) (err error) {
	err = data.readMetaBatch(ctx, version, reader, m)
	if err != nil {
		return
	}
	err = data.readAll(ctx, version, fs)
	if err != nil {
		return
	}

	if version == CheckpointVersion1 {
		bat := data.bats[TBLInsertIDX]
		if bat == nil {
			return
		}
		length := bat.GetVectorByName(pkgcatalog.SystemRelAttr_Version).Length()
		vec := containers.MakeVector(types.T_uint32.ToType())
		for i := 0; i < length; i++ {
			vec.Append(pkgcatalog.CatalogVersion_V1, false)
		}
		//Fixme: add vector to batch
		//bat.AddVector(pkgcatalog.SystemRelAttr_CatalogVersion, vec)
	}

	return
}

func (data *CheckpointData) readMetaBatch(
	ctx context.Context,
	version uint32,
	reader *blockio.BlockReader,
	m *mpool.MPool,
) (err error) {
	var bat *containers.Batch
	item := checkpointDataReferVersions[version][MetaIDX]
	bat, err = LoadBlkColumnsByMeta(ctx, item.types, item.attrs, uint16(0), reader)
	if err != nil {
		return
	}
	data.setMetaBatch(bat)
	return
}
func (data *CheckpointData) setMetaBatch(bat *containers.Batch) {
	data.bats[MetaIDX].Append(bat)
}
func (data *CheckpointData) getMetaBatch() (bat *containers.Batch) {
	return data.bats[MetaIDX]
}

func (data *CheckpointData) replayMetaBatch() {
	bat := data.getMetaBatch()
	data.locations = make(map[string]objectio.Location)
	insertVec := bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchLocation).GetDownstreamVector()
	blkDelVec := bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchLocation).GetDownstreamVector()
	segDelVec := bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchLocation).GetDownstreamVector()
	blkInsertStartVec := bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).GetDownstreamVector()
	blkInsertEndVec := bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchEnd).GetDownstreamVector()
	blkDelStartVec := bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchStart).GetDownstreamVector()
	blkDelEndVec := bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchEnd).GetDownstreamVector()
	segDelStartVec := bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchStart).GetDownstreamVector()
	segDelEndVec := bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchEnd).GetDownstreamVector()
	tidVec := bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()

	for i := 0; i < bat.Vecs[2].Length(); i++ {
		insLocation := insertVec.GetBytesAt(i)
		data.locations[objectio.Location(insLocation).String()] = insLocation
		delLocation := blkDelVec.GetBytesAt(i)
		data.locations[objectio.Location(delLocation).String()] = delLocation
		segLocation := segDelVec.GetBytesAt(i)
		data.locations[objectio.Location(segLocation).String()] = segLocation

		meta := &CheckpointMeta{
			blkInsertOffset: &common.ClosedInterval{
				Start: uint64(vector.GetFixedAt[int32](blkInsertStartVec, i)),
				End:   uint64(vector.GetFixedAt[int32](blkInsertEndVec, i)),
			},
			blkInsertLocation: insLocation,
			blkDeleteOffset: &common.ClosedInterval{
				Start: uint64(vector.GetFixedAt[int32](blkDelStartVec, i)),
				End:   uint64(vector.GetFixedAt[int32](blkDelEndVec, i)),
			},
			blkDeleteLocation: delLocation,
			segDeleteOffset: &common.ClosedInterval{
				Start: uint64(vector.GetFixedAt[int32](segDelStartVec, i)),
				End:   uint64(vector.GetFixedAt[int32](segDelEndVec, i)),
			},
			segDeleteLocation: segLocation,
		}
		tid := vector.GetFixedAt[uint64](tidVec, i)
		data.meta[tid] = meta
	}

}
func (data *CheckpointData) prefetchMetaBatch(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location,
) (err error) {
	err = prefetchMetaBatch(ctx, version, service, key)
	return
}

func (data *CheckpointData) readAll(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
) (err error) {
	data.replayMetaBatch()
	for _, val := range data.locations {
		var reader *blockio.BlockReader
		reader, err = blockio.NewObjectReader(service, val)
		if err != nil {
			return
		}
		var bat *containers.Batch
		for idx := range checkpointDataReferVersions[version] {
			if uint16(idx) == MetaIDX {
				continue
			}
			item := checkpointDataReferVersions[version][idx]
			bat, err = LoadBlkColumnsByMeta(ctx, item.types, item.attrs, uint16(idx), reader)
			if err != nil {
				return
			}
			data.bats[idx].Append(bat)
		}
	}
	return
}

func (data *CheckpointData) readBatch(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
) (err error) {
	return
}

func (data *CheckpointData) prefetchAll(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
) (err error) {
	return
}

func (data *CheckpointData) Close() {
	for idx := range data.bats {
		if data.bats[idx] != nil {
			data.bats[idx].Close()
			data.bats[idx] = nil
		}
	}
}

func (data *CheckpointData) CloseWhenLoadFromCache(version uint32) {
	switch version {
	case CheckpointVersion1:
		bat := data.bats[TBLInsertIDX]
		if bat == nil {
			return
		}
		vec := data.bats[TBLInsertIDX].GetVectorByName(pkgcatalog.SystemRelAttr_CatalogVersion)
		vec.Close()
	}
}

func (data *CheckpointData) GetDBBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return data.bats[DBInsertIDX],
		data.bats[DBInsertTxnIDX],
		data.bats[DBDeleteIDX],
		data.bats[DBDeleteTxnIDX]
}
func (data *CheckpointData) GetTblBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return data.bats[TBLInsertIDX],
		data.bats[TBLInsertTxnIDX],
		data.bats[TBLColInsertIDX],
		data.bats[TBLDeleteIDX],
		data.bats[TBLDeleteTxnIDX]
}
func (data *CheckpointData) GetSegBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return data.bats[SEGInsertIDX],
		data.bats[SEGInsertTxnIDX],
		data.bats[SEGDeleteIDX],
		data.bats[SEGDeleteTxnIDX]
}
func (data *CheckpointData) GetBlkBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return data.bats[BLKMetaInsertIDX],
		data.bats[BLKMetaInsertTxnIDX],
		data.bats[BLKMetaDeleteIDX],
		data.bats[BLKMetaDeleteTxnIDX]
}
func (data *CheckpointData) GetDNBlkBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return data.bats[BLKDNMetaInsertIDX],
		data.bats[BLKDNMetaInsertTxnIDX],
		data.bats[BLKDNMetaDeleteIDX],
		data.bats[BLKDNMetaDeleteTxnIDX]
}

func (collector *BaseCollector) VisitDB(entry *catalog.DBEntry) error {
	if shouldIgnoreDBInLogtail(entry.ID) {
		return nil
	}
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		dbNode := node
		var created, dropped bool
		if dbNode.HasDropCommitted() {
			dropped = true
			if dbNode.CreatedAt.Equal(dbNode.DeletedAt) {
				created = true
			}
		} else {
			created = true
		}
		if dropped {
			// delScehma is empty, it will just fill rowid / commit ts
			catalogEntry2Batch(
				collector.data.bats[DBDeleteIDX],
				entry,
				node,
				DBDelSchema,
				txnimpl.FillDBRow,
				u64ToRowID(entry.GetID()),
				dbNode.GetEnd())
			dbNode.TxnMVCCNode.AppendTuple(collector.data.bats[DBDeleteTxnIDX])
			collector.data.bats[DBDeleteTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetID(), false)
		}
		if created {
			catalogEntry2Batch(collector.data.bats[DBInsertIDX],
				entry,
				node,
				catalog.SystemDBSchema,
				txnimpl.FillDBRow,
				u64ToRowID(entry.GetID()),
				dbNode.GetEnd())
			dbNode.TxnMVCCNode.AppendTuple(collector.data.bats[DBInsertTxnIDX])
		}
	}
	return nil
}
func (collector *GlobalCollector) isEntryDeletedBeforeThreshold(entry catalog.BaseEntry) bool {
	entry.RLock()
	defer entry.RUnlock()
	return entry.DeleteBefore(collector.versionThershold)
}
func (collector *GlobalCollector) VisitDB(entry *catalog.DBEntry) error {
	if collector.isEntryDeletedBeforeThreshold(entry.BaseEntryImpl) {
		return nil
	}
	return collector.BaseCollector.VisitDB(entry)
}

func (collector *BaseCollector) VisitTable(entry *catalog.TableEntry) (err error) {
	if shouldIgnoreTblInLogtail(entry.ID) {
		return nil
	}
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	entry.RUnlock()
	tableColDelBat := collector.data.bats[TBLColDeleteIDX]
	tableDelTxnBat := collector.data.bats[TBLDeleteTxnIDX]
	tableDelBat := collector.data.bats[TBLDeleteIDX]
	tableColInsBat := collector.data.bats[TBLColInsertIDX]
	tableInsBat := collector.data.bats[TBLInsertIDX]
	tableColInsTxnBat := collector.data.bats[TBLInsertTxnIDX]
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		tblNode := node
		var created, dropped bool
		if tblNode.HasDropCommitted() {
			dropped = true
			if tblNode.CreatedAt.Equal(tblNode.DeletedAt) {
				created = true
			}
		} else {
			created = true
		}
		if created {
			for _, syscol := range catalog.SystemColumnSchema.ColDefs {
				txnimpl.FillColumnRow(
					entry,
					tblNode,
					syscol.Name,
					tableColInsBat.GetVectorByName(syscol.Name),
				)
			}
			// send dropped column del
			for _, name := range tblNode.BaseNode.Schema.Extra.DroppedAttrs {
				tableColDelBat.GetVectorByName(catalog.AttrRowID).Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", entry.GetID(), name))), false)
				tableColDelBat.GetVectorByName(catalog.AttrCommitTs).Append(tblNode.GetEnd(), false)
			}
			rowidVec := tableColInsBat.GetVectorByName(catalog.AttrRowID)
			commitVec := tableColInsBat.GetVectorByName(catalog.AttrCommitTs)
			for _, usercol := range tblNode.BaseNode.Schema.ColDefs {
				rowidVec.Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name))), false)
				commitVec.Append(tblNode.GetEnd(), false)
			}

			tableColInsTxnBat.GetVectorByName(
				SnapshotAttr_BlockMaxRow).Append(entry.GetLastestSchema().BlockMaxRows, false)
			tableColInsTxnBat.GetVectorByName(
				SnapshotAttr_SegmentMaxBlock).Append(entry.GetLastestSchema().SegmentMaxBlocks, false)
			tableColInsTxnBat.GetVectorByName(
				SnapshotAttr_SchemaExtra).Append(tblNode.BaseNode.Schema.MustGetExtraBytes(), false)

			catalogEntry2Batch(
				tableInsBat,
				entry,
				tblNode,
				catalog.SystemTableSchema,
				txnimpl.FillTableRow,
				u64ToRowID(entry.GetID()),
				tblNode.GetEnd(),
			)

			tblNode.TxnMVCCNode.AppendTuple(tableColInsTxnBat)
		}
		if dropped {
			tableDelTxnBat.GetVectorByName(
				SnapshotAttr_DBID).Append(entry.GetDB().GetID(), false)
			tableDelTxnBat.GetVectorByName(
				SnapshotAttr_TID).Append(entry.GetID(), false)

			rowidVec := tableColDelBat.GetVectorByName(catalog.AttrRowID)
			commitVec := tableColDelBat.GetVectorByName(catalog.AttrCommitTs)
			pkVec := tableColDelBat.GetVectorByName(pkgcatalog.SystemColAttr_UniqName)
			for _, usercol := range tblNode.BaseNode.Schema.ColDefs {
				rowidVec.Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name))), false)
				commitVec.Append(tblNode.GetEnd(), false)
				pkVec.Append([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name)), false)
			}

			catalogEntry2Batch(
				tableDelBat,
				entry,
				tblNode,
				TblDelSchema,
				txnimpl.FillTableRow,
				u64ToRowID(entry.GetID()),
				tblNode.GetEnd(),
			)
			tblNode.TxnMVCCNode.AppendTuple(tableDelTxnBat)
		}
	}
	return nil
}

func (collector *GlobalCollector) VisitTable(entry *catalog.TableEntry) error {
	if collector.isEntryDeletedBeforeThreshold(entry.BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetDB().BaseEntryImpl) {
		return nil
	}
	return collector.BaseCollector.VisitTable(entry)
}

func (collector *BaseCollector) VisitSeg(entry *catalog.SegmentEntry) (err error) {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	entry.RUnlock()
	if len(mvccNodes) == 0 {
		return nil
	}
	delStart := collector.data.bats[SEGDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	segDelBat := collector.data.bats[SEGDeleteIDX]
	segDelTxn := collector.data.bats[SEGDeleteTxnIDX]
	segInsBat := collector.data.bats[SEGInsertIDX]
	segInsTxn := collector.data.bats[SEGInsertTxnIDX]

	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		segNode := node
		if segNode.HasDropCommitted() {
			vector.AppendFixed(
				segDelBat.GetVectorByName(catalog.AttrRowID).GetDownstreamVector(),
				segid2rowid(&entry.ID),
				false,
				common.DefaultAllocator,
			)
			vector.AppendFixed(
				segDelBat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector(),
				segNode.GetEnd(),
				false,
				common.DefaultAllocator,
			)
			vector.AppendFixed(
				segDelTxn.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector(),
				entry.GetTable().GetDB().GetID(),
				false,
				common.DefaultAllocator,
			)
			vector.AppendFixed(
				segDelTxn.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
				entry.GetTable().GetID(),
				false,
				common.DefaultAllocator,
			)
			segNode.TxnMVCCNode.AppendTuple(segDelTxn)
		} else {
			vector.AppendFixed(
				segInsBat.GetVectorByName(SegmentAttr_ID).GetDownstreamVector(),
				entry.ID,
				false,
				common.DefaultAllocator,
			)
			vector.AppendFixed(
				segInsBat.GetVectorByName(SegmentAttr_CreateAt).GetDownstreamVector(),
				segNode.GetEnd(),
				false,
				common.DefaultAllocator,
			)
			buf := &bytes.Buffer{}
			if _, err := entry.SegmentNode.WriteTo(buf); err != nil {
				return err
			}
			vector.AppendBytes(
				segInsBat.GetVectorByName(SegmentAttr_SegNode).GetDownstreamVector(),
				buf.Bytes(),
				false,
				common.DefaultAllocator,
			)
			vector.AppendFixed(
				segInsTxn.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector(),
				entry.GetTable().GetDB().GetID(),
				false,
				common.DefaultAllocator,
			)
			vector.AppendFixed(
				segInsTxn.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
				entry.GetTable().GetID(),
				false,
				common.DefaultAllocator,
			)
			segNode.TxnMVCCNode.AppendTuple(segInsTxn)
		}
	}
	delEnd := segDelBat.GetVectorByName(catalog.AttrRowID).Length()
	collector.data.UpdateSegMeta(entry.GetTable().ID, int32(delStart), int32(delEnd))
	return nil
}

func (collector *GlobalCollector) VisitSeg(entry *catalog.SegmentEntry) error {
	if collector.isEntryDeletedBeforeThreshold(entry.BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetTable().BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetTable().GetDB().BaseEntryImpl) {
		return nil
	}
	return collector.BaseCollector.VisitSeg(entry)
}

func (collector *BaseCollector) VisitBlk(entry *catalog.BlockEntry) (err error) {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	entry.RUnlock()
	if len(mvccNodes) == 0 {
		return nil
	}
	insStart := collector.data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.AttrRowID).Length()
	delStart := collector.data.bats[BLKMetaDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	blkDNMetaDelBat := collector.data.bats[BLKDNMetaDeleteIDX]
	blkDNMetaDelTxnBat := collector.data.bats[BLKDNMetaDeleteTxnIDX]
	blkDNMetaInsBat := collector.data.bats[BLKDNMetaInsertIDX]
	blkDNMetaInsTxnBat := collector.data.bats[BLKDNMetaInsertTxnIDX]
	blkMetaDelBat := collector.data.bats[BLKMetaDeleteIDX]
	blkMetaDelTxnBat := collector.data.bats[BLKMetaDeleteTxnIDX]
	blkCNMetaInsBat := collector.data.bats[BLKCNMetaInsertIDX]
	blkMetaInsBat := collector.data.bats[BLKMetaInsertIDX]
	blkMetaInsTxnBat := collector.data.bats[BLKMetaInsertTxnIDX]

	blkDNMetaDelRowIDVec := blkDNMetaDelBat.GetVectorByName(catalog.AttrRowID).GetDownstreamVector()
	blkDNMetaDelCommitTsVec := blkDNMetaDelBat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector()
	blkDNMetaDelTxnDBIDVec := blkDNMetaDelTxnBat.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector()
	blkDNMetaDelTxnTIDVec := blkDNMetaDelTxnBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	blkDNMetaDelTxnMetaLocVec := blkDNMetaDelTxnBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	blkDNMetaDelTxnDeltaLocVec := blkDNMetaDelTxnBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()

	blkDNMetaInsRowIDVec := blkDNMetaInsBat.GetVectorByName(catalog.AttrRowID).GetDownstreamVector()
	blkDNMetaInsCommitTimeVec := blkDNMetaInsBat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector()
	blkDNMetaInsIDVec := blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_ID).GetDownstreamVector()
	blkDNMetaInsStateVec := blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_EntryState).GetDownstreamVector()
	blkDNMetaInsMetaLocVec := blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	blkDNMetaInsDelLocVec := blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()
	blkDNMetaInsSortedVec := blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_Sorted).GetDownstreamVector()
	blkDNMetaInsSegIDVec := blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).GetDownstreamVector()
	blkDNMetaInsCommitTsVec := blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).GetDownstreamVector()

	blkDNMetaInsTxnDBIDVec := blkDNMetaInsTxnBat.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector()
	blkDNMetaInsTxnTIDVec := blkDNMetaInsTxnBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	blkDNMetaInsTxnMetaLocVec := blkDNMetaInsTxnBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	blkDNMetaInsTxnDeltaLocVec := blkDNMetaInsTxnBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()

	blkMetaDelRowIDVec := blkMetaDelBat.GetVectorByName(catalog.AttrRowID).GetDownstreamVector()
	blkMetaDelCommitTsVec := blkMetaDelBat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector()

	blkMetaDelTxnDBIDVec := blkMetaDelTxnBat.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector()
	blkMetaDelTxnTIDVec := blkMetaDelTxnBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	blkMetaDelTxnMetaLocVec := blkMetaDelTxnBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	blkMetaDelTxnDeltaLocVec := blkMetaDelTxnBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()

	blkCNMetaInsRowIDVec := blkCNMetaInsBat.GetVectorByName(catalog.AttrRowID).GetDownstreamVector()
	blkCNMetaInsCommitTimeVec := blkCNMetaInsBat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector()
	blkCNMetaInsIDVec := blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_ID).GetDownstreamVector()
	blkCNMetaInsStateVec := blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_EntryState).GetDownstreamVector()
	blkCNMetaInsMetaLocVec := blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	blkCNMetaInsDelLocVec := blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()
	blkCNMetaInsSortedVec := blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_Sorted).GetDownstreamVector()
	blkCNMetaInsSegIDVec := blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).GetDownstreamVector()
	blkCNMetaInsCommitTsVec := blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).GetDownstreamVector()

	blkMetaInsRowIDVec := blkMetaInsBat.GetVectorByName(catalog.AttrRowID).GetDownstreamVector()
	blkMetaInsCommitTimeVec := blkMetaInsBat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector()
	blkMetaInsIDVec := blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_ID).GetDownstreamVector()
	blkMetaInsStateVec := blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_EntryState).GetDownstreamVector()
	blkMetaInsMetaLocVec := blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	blkMetaInsDelLocVec := blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()
	blkMetaInsSortedVec := blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_Sorted).GetDownstreamVector()
	blkMetaInsSegIDVec := blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).GetDownstreamVector()
	blkMetaInsCommitTsVec := blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).GetDownstreamVector()

	blkMetaInsTxnDBIDVec := blkMetaInsTxnBat.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector()
	blkMetaInsTxnTIDVec := blkMetaInsTxnBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	blkMetaInsTxnMetaLocVec := blkMetaInsTxnBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	blkMetaInsTxnDeltaLocVec := blkMetaInsTxnBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()

	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		metaNode := node
		if metaNode.BaseNode.MetaLoc.IsEmpty() || metaNode.Aborted {
			if metaNode.HasDropCommitted() {
				vector.AppendFixed(
					blkDNMetaDelRowIDVec,
					blockid2rowid(&entry.ID),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaDelCommitTsVec,
					metaNode.GetEnd(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaDelTxnDBIDVec,
					entry.GetSegment().GetTable().GetDB().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaDelTxnTIDVec,
					entry.GetSegment().GetTable().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkDNMetaDelTxnMetaLocVec,
					[]byte(metaNode.BaseNode.MetaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkDNMetaDelTxnDeltaLocVec,
					[]byte(metaNode.BaseNode.DeltaLoc),
					false,
					common.DefaultAllocator,
				)
				metaNode.TxnMVCCNode.AppendTuple(blkDNMetaDelTxnBat)
			} else {
				vector.AppendFixed(
					blkDNMetaInsIDVec,
					entry.ID,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaInsStateVec,
					entry.IsAppendable(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaInsCommitTsVec,
					metaNode.GetEnd(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkDNMetaInsMetaLocVec,
					[]byte(metaNode.BaseNode.MetaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkDNMetaInsDelLocVec,
					[]byte(metaNode.BaseNode.DeltaLoc),
					false,
					common.DefaultAllocator,
				)
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				vector.AppendFixed(
					blkDNMetaInsSortedVec,
					is_sorted,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaInsSegIDVec,
					entry.GetSegment().ID,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaInsCommitTimeVec,
					metaNode.CreatedAt,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaInsRowIDVec,
					blockid2rowid(&entry.ID),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaInsTxnDBIDVec,
					entry.GetSegment().GetTable().GetDB().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaInsTxnTIDVec,
					entry.GetSegment().GetTable().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkDNMetaInsTxnMetaLocVec,
					[]byte(metaNode.BaseNode.MetaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkDNMetaInsTxnDeltaLocVec,
					[]byte(metaNode.BaseNode.DeltaLoc),
					false,
					common.DefaultAllocator,
				)
				metaNode.TxnMVCCNode.AppendTuple(blkDNMetaInsTxnBat)
			}
		} else {
			if metaNode.HasDropCommitted() {
				vector.AppendFixed(
					blkMetaDelRowIDVec,
					blockid2rowid(&entry.ID),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaDelCommitTsVec,
					metaNode.GetEnd(),
					false,
					common.DefaultAllocator,
				)

				vector.AppendFixed(
					blkMetaDelTxnDBIDVec,
					entry.GetSegment().GetTable().GetDB().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaDelTxnTIDVec,
					entry.GetSegment().GetTable().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkMetaDelTxnMetaLocVec,
					[]byte(metaNode.BaseNode.MetaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkMetaDelTxnDeltaLocVec,
					[]byte(metaNode.BaseNode.DeltaLoc),
					false,
					common.DefaultAllocator,
				)
				metaNode.TxnMVCCNode.AppendTuple(blkMetaDelTxnBat)

				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				vector.AppendFixed(
					blkCNMetaInsIDVec,
					entry.ID,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkCNMetaInsStateVec,
					entry.IsAppendable(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkCNMetaInsMetaLocVec,
					[]byte(metaNode.BaseNode.MetaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkCNMetaInsDelLocVec,
					[]byte(metaNode.BaseNode.DeltaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkCNMetaInsSortedVec,
					is_sorted,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkCNMetaInsSegIDVec,
					entry.GetSegment().ID,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkCNMetaInsCommitTsVec,
					metaNode.GetEnd(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkCNMetaInsRowIDVec,
					blockid2rowid(&entry.ID),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkCNMetaInsCommitTimeVec,
					metaNode.CreatedAt,
					false,
					common.DefaultAllocator,
				)
			} else {
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				vector.AppendFixed(
					blkMetaInsIDVec,
					entry.ID,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaInsStateVec,
					entry.IsAppendable(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkMetaInsMetaLocVec,
					[]byte(metaNode.BaseNode.MetaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkMetaInsDelLocVec,
					[]byte(metaNode.BaseNode.DeltaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaInsCommitTsVec,
					metaNode.GetEnd(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaInsSortedVec,
					is_sorted,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaInsSegIDVec,
					entry.GetSegment().ID,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaInsCommitTimeVec,
					metaNode.CreatedAt,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaInsRowIDVec,
					blockid2rowid(&entry.ID),
					false,
					common.DefaultAllocator,
				)

				vector.AppendFixed(
					blkMetaInsTxnDBIDVec,
					entry.GetSegment().GetTable().GetDB().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaInsTxnTIDVec,
					entry.GetSegment().GetTable().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkMetaInsTxnMetaLocVec,
					[]byte(metaNode.BaseNode.MetaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkMetaInsTxnDeltaLocVec,
					[]byte(metaNode.BaseNode.DeltaLoc),
					false,
					common.DefaultAllocator,
				)

				metaNode.TxnMVCCNode.AppendTuple(blkMetaInsTxnBat)
			}
		}
	}
	insEnd := collector.data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.AttrRowID).Length()
	delEnd := collector.data.bats[BLKMetaDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	collector.data.UpdateBlkMeta(entry.GetSegment().GetTable().ID, int32(insStart), int32(insEnd), int32(delStart), int32(delEnd))
	return nil
}

func (collector *GlobalCollector) VisitBlk(entry *catalog.BlockEntry) error {
	if collector.isEntryDeletedBeforeThreshold(entry.BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetSegment().BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetSegment().GetTable().BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetSegment().GetTable().GetDB().BaseEntryImpl) {
		return nil
	}
	return collector.BaseCollector.VisitBlk(entry)
}

func (collector *BaseCollector) OrphanData() *CheckpointData {
	data := collector.data
	collector.data = nil
	return data
}

func (collector *BaseCollector) Close() {
	if collector.data != nil {
		collector.data.Close()
	}
}
