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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
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

	BLKInsertIDX
	BLKInsertTxnIDX
	BLKDeleteIDX
	BLKDeleteTxnIDX
)

const MaxIDX = BLKCNMetaInsertIDX + 1

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

type MemoryTable = model.AOT[*model.BatchBlock, *containers.Batch]

type CheckpointData struct {
	meta      map[uint64]*CheckpointMeta
	locations map[string]struct{}
	bats      [MaxIDX]*MemoryTable
}

func GetRows(mt *MemoryTable) int {
	rows := 0
	fn := func(block *model.BatchBlock) bool {
		rows += block.Length()
		return true
	}
	mt.Scan(fn)
	return rows
}

func GetBatch(mt *MemoryTable, schema *catalog.Schema) *containers.Batch {
	bat := makeRespBatchFromSchema(schema)
	fn := func(block *model.BatchBlock) bool {
		bat.Append(block.Batch)
		return true
	}
	mt.Scan(fn)
	return bat
}

func AppendBatch(mt *MemoryTable, bat *containers.Batch) {
	if bat.Length() == 0 {
		rows := 0
		for i := range bat.Vecs {
			if bat.Vecs[i].Length() > 0 {
				rows = bat.Vecs[i].Length()
				break
			}
		}
		if rows > 0 {
			for i := range bat.Vecs {
				if bat.Vecs[i].Length() == 0 {
					for z := 0; z < rows; z++ {
						bat.Vecs[i].Append(nil, true)
					}
				}
			}
		}
	}
	mt.Append(bat)
}

func NewCheckpointData() *CheckpointData {
	data := &CheckpointData{
		meta: make(map[uint64]*CheckpointMeta),
	}
	for idx, schema := range checkpointDataSchemas_Curr {
		factory := func(bat *containers.Batch, s any) *model.BatchBlock {
			bat = makeRespBatchFromSchema(s.(*catalog.Schema))
			id := common.NextGlobalSeqNum()
			return &model.BatchBlock{
				Batch: bat,
				ID:    id,
			}
		}
		data.bats[idx] = model.NewAOTWithSchema(schema, 40000, factory,
			func(a, b *model.BatchBlock) bool {
				return a.ID < b.ID
			})
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
	dataFactory catalog.DataFactory) (err error) {
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

const (
	Checkpoint_Meta_TID_IDX                = 2
	Checkpoint_Meta_Insert_Block_Start_IDX = 3
	Checkpoint_Meta_Insert_Block_End_IDX   = 4
	Checkpoint_Meta_Delete_Block_Start_IDX = 5
	Checkpoint_Meta_Delete_Block_End_IDX   = 6
	Checkpoint_Meta_Segment_Start_IDX      = 7
	Checkpoint_Meta_Segment_End_IDX        = 8
)

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
	m *mpool.MPool) (err error) {

	for idx, item := range checkpointDataReferVersions[version] {
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

func (data *CheckpointData) prepareMeta() {
	bat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[MetaIDX])
	for tid, meta := range data.meta {
		bat.GetVectorByName(SnapshotAttr_TID).Append(tid, false)
		if meta.blkInsertOffset == nil {
			bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).Append(int32(-1), false)
			bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchEnd).Append(int32(-1), false)
		} else {
			bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).Append(int32(meta.blkInsertOffset.Start), false)
			bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchEnd).Append(int32(meta.blkInsertOffset.End), false)
		}
		if meta.blkDeleteOffset == nil {
			bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchStart).Append(int32(-1), false)
			bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchEnd).Append(int32(-1), false)
		} else {
			bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchStart).Append(int32(meta.blkDeleteOffset.Start), false)
			bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchEnd).Append(int32(meta.blkDeleteOffset.End), false)
		}
		if meta.segDeleteOffset == nil {
			bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchStart).Append(int32(-1), false)
			bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchEnd).Append(int32(-1), false)
		} else {
			bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchStart).Append(int32(meta.segDeleteOffset.Start), false)
			bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchEnd).Append(int32(meta.segDeleteOffset.End), false)
		}
	}
	AppendBatch(data.bats[MetaIDX], bat)
	//data.bats[MetaIDX].Append(bat)
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
	/*logutil.Info(BatchToString("BLK-META-DEL-BAT", data.bats[BLKMetaDeleteIDX], true))
	logutil.Info(BatchToString("BLK-META-INS-BAT", data.bats[BLKMetaInsertIDX], true))*/
	logutil.Info(data.bats[BLKMetaDeleteIDX].String())
	logutil.Info(data.bats[BLKMetaInsertIDX].String())
}

func (data *CheckpointData) WriteTo(
	fs fileservice.FileService) (location objectio.Location, err error) {
	segmentid := objectio.NewSegmentid()
	name := objectio.BuildObjectName(segmentid, 0)
	writer, err := blockio.NewBlockWriterNew(fs, name, 0, nil)
	if err != nil {
		return
	}
	for i, schema := range checkpointDataSchemas_Curr {
		if i == int(MetaIDX) {
			continue
		}
		if _, err = writer.WriteSubBatch(containers.ToCNBatch(GetBatch(data.bats[i], schema)), objectio.ConvertToSchemaType(uint16(i))); err != nil {
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
	data.prepareMeta()
	if err != nil {
		return
	}

	segmentid2 := objectio.NewSegmentid()
	name2 := objectio.BuildObjectName(segmentid2, 0)
	writer2, err := blockio.NewBlockWriterNew(fs, name2, 0, nil)
	if err != nil {
		return
	}
	writeFun := func(block *model.BatchBlock) bool {
		if _, err = writer2.WriteSubBatch(
			containers.ToCNBatch(GetBatch(data.bats[MetaIDX], checkpointDataSchemas_Curr[MetaIDX])),
			objectio.ConvertToSchemaType(uint16(MetaIDX))); err != nil {
			return false
		}
		return true
	}
	data.bats[MetaIDX].Scan(writeFun)
	if err != nil {
		return
	}
	blks2, _, err := writer.Sync(context.Background())

	location = objectio.BuildLocation(name, blks2[0].GetExtent(), 0, blks2[0].GetID())
	return
}

func LoadBlkColumnsByMeta(cxt context.Context, colTypes []types.Type, colNames []string, id uint16, reader *blockio.BlockReader) (*containers.Batch, error) {
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

func LoadCNSubBlkColumnsByMeta(cxt context.Context, colTypes []types.Type, colNames []string, id uint16, reader *blockio.BlockReader, m *mpool.MPool) (*batch.Batch, error) {
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
	key objectio.Location) (err error) {

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
	key objectio.Location) (err error) {

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
	m *mpool.MPool) (err error) {

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
		length := GetRows(bat)
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
	return GetBatch(data.bats[MetaIDX], MetaSchema)
}

func (data *CheckpointData) replayMetaBatch() {
	bat := data.getMetaBatch()
	for i := 0; i < bat.Length(); i++ {
		insLocation := bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchLocation).Get(i).([]byte)
		data.locations[string(insLocation)] = struct{}{}
		delLocation := bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchLocation).Get(i).([]byte)
		data.locations[string(delLocation)] = struct{}{}
		segLocation := bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchLocation).Get(i).([]byte)
		data.locations[string(insLocation)] = struct{}{}
		meta := &CheckpointMeta{
			blkInsertOffset: &common.ClosedInterval{
				Start: uint64(bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).Get(i).(int32)),
				End:   uint64(bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchEnd).Get(i).(int32)),
			},
			blkInsertLocation: insLocation,
			blkDeleteOffset: &common.ClosedInterval{
				Start: uint64(bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchStart).Get(i).(int32)),
				End:   uint64(bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchEnd).Get(i).(int32)),
			},
			blkDeleteLocation: delLocation,
			segDeleteOffset: &common.ClosedInterval{
				Start: uint64(bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchStart).Get(i).(int32)),
				End:   uint64(bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchEnd).Get(i).(int32)),
			},
			segDeleteLocation: segLocation,
		}
		tid := bat.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		data.meta[tid] = meta
	}

}
func (data *CheckpointData) prefetchMetaBatch(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location) (err error) {
	err = prefetchMetaBatch(ctx, version, service, key)
	return
}

func (data *CheckpointData) readAll(
	ctx context.Context,
	version uint32,
	service fileservice.FileService) (err error) {
	data.replayMetaBatch()
	for key := range data.locations {
		var reader *blockio.BlockReader
		reader, err = blockio.NewObjectReader(service, []byte(key))
		if err != nil {
			return
		}
		var bat *containers.Batch
		for idx := range checkpointDataReferVersions[version] {
			item := checkpointDataReferVersions[version][idx]
			bat, err = LoadBlkColumnsByMeta(ctx, item.types, item.attrs, uint16(0), reader)
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
	service fileservice.FileService) (err error) {
	return
}

func (data *CheckpointData) prefetchAll(
	ctx context.Context,
	version uint32,
	service fileservice.FileService) (err error) {

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
		colseFun := func(block *model.BatchBlock) bool {
			vec := block.GetVectorByName(pkgcatalog.SystemRelAttr_CatalogVersion)
			vec.Close()
			return true
		}
		data.bats[TBLInsertIDX].Scan(colseFun)
	}
}

func (data *CheckpointData) GetDBBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return GetBatch(data.bats[DBInsertIDX], checkpointDataSchemas_Curr[DBInsertIDX]),
		GetBatch(data.bats[DBInsertTxnIDX], checkpointDataSchemas_Curr[DBInsertTxnIDX]),
		GetBatch(data.bats[DBDeleteIDX], checkpointDataSchemas_Curr[DBDeleteIDX]),
		GetBatch(data.bats[DBDeleteTxnIDX], checkpointDataSchemas_Curr[DBDeleteTxnIDX])

}
func (data *CheckpointData) GetTblBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return GetBatch(data.bats[TBLInsertIDX], checkpointDataSchemas_Curr[TBLInsertIDX]),
		GetBatch(data.bats[TBLInsertTxnIDX], checkpointDataSchemas_Curr[TBLInsertTxnIDX]),
		GetBatch(data.bats[TBLColInsertIDX], checkpointDataSchemas_Curr[TBLColInsertIDX]),
		GetBatch(data.bats[TBLDeleteIDX], checkpointDataSchemas_Curr[TBLDeleteIDX]),
		GetBatch(data.bats[TBLDeleteTxnIDX], checkpointDataSchemas_Curr[TBLDeleteTxnIDX])
}
func (data *CheckpointData) GetSegBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return GetBatch(data.bats[SEGInsertIDX], checkpointDataSchemas_Curr[SEGInsertIDX]),
		GetBatch(data.bats[SEGInsertTxnIDX], checkpointDataSchemas_Curr[SEGInsertTxnIDX]),
		GetBatch(data.bats[SEGDeleteIDX], checkpointDataSchemas_Curr[SEGDeleteIDX]),
		GetBatch(data.bats[SEGDeleteTxnIDX], checkpointDataSchemas_Curr[SEGDeleteTxnIDX])
}
func (data *CheckpointData) GetBlkBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return GetBatch(data.bats[BLKMetaInsertIDX], checkpointDataSchemas_Curr[BLKMetaInsertIDX]),
		GetBatch(data.bats[BLKMetaInsertTxnIDX], checkpointDataSchemas_Curr[BLKMetaInsertTxnIDX]),
		GetBatch(data.bats[BLKMetaDeleteIDX], checkpointDataSchemas_Curr[BLKMetaDeleteIDX]),
		GetBatch(data.bats[BLKMetaDeleteTxnIDX], checkpointDataSchemas_Curr[BLKMetaDeleteTxnIDX])
}
func (data *CheckpointData) GetDNBlkBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return GetBatch(data.bats[BLKDNMetaInsertIDX], checkpointDataSchemas_Curr[BLKDNMetaInsertIDX]),
		GetBatch(data.bats[BLKDNMetaInsertTxnIDX], checkpointDataSchemas_Curr[BLKDNMetaInsertTxnIDX]),
		GetBatch(data.bats[BLKDNMetaDeleteIDX], checkpointDataSchemas_Curr[BLKDNMetaDeleteIDX]),
		GetBatch(data.bats[BLKDNMetaDeleteTxnIDX], checkpointDataSchemas_Curr[BLKDNMetaDeleteTxnIDX])
}

func (collector *BaseCollector) VisitDB(entry *catalog.DBEntry) error {
	if shouldIgnoreDBInLogtail(entry.ID) {
		return nil
	}
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	entry.RUnlock()
	dbDelBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[DBDeleteIDX])
	dbDelTxnBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[DBDeleteTxnIDX])
	dbInsBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[DBInsertIDX])
	dbInsTxnBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[DBInsertTxnIDX])
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
				dbDelBat,
				entry,
				node,
				DBDelSchema,
				txnimpl.FillDBRow,
				u64ToRowID(entry.GetID()),
				dbNode.GetEnd())
			dbNode.TxnMVCCNode.AppendTuple(dbDelTxnBat)
			dbDelTxnBat.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetID(), false)
		}
		if created {
			catalogEntry2Batch(dbInsBat,
				entry,
				node,
				catalog.SystemDBSchema,
				txnimpl.FillDBRow,
				u64ToRowID(entry.GetID()),
				dbNode.GetEnd())
			dbNode.TxnMVCCNode.AppendTuple(dbInsTxnBat)
		}
	}
	AppendBatch(collector.data.bats[DBDeleteIDX], dbDelBat)
	AppendBatch(collector.data.bats[DBDeleteTxnIDX], dbDelTxnBat)
	AppendBatch(collector.data.bats[DBInsertIDX], dbInsBat)
	AppendBatch(collector.data.bats[DBInsertTxnIDX], dbInsTxnBat)
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
	tableColDelBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[TBLColDeleteIDX])
	tableDelTxnBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[TBLDeleteTxnIDX])
	tableDelBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[TBLDeleteIDX])
	tableColInsBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[TBLColInsertIDX])
	tableInsBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[TBLInsertIDX])
	tableColInsTxnBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[TBLInsertTxnIDX])
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
	AppendBatch(collector.data.bats[TBLColDeleteIDX], tableColDelBat)
	AppendBatch(collector.data.bats[TBLDeleteTxnIDX], tableDelTxnBat)
	AppendBatch(collector.data.bats[TBLDeleteIDX], tableDelBat)
	AppendBatch(collector.data.bats[TBLColInsertIDX], tableColInsBat)
	AppendBatch(collector.data.bats[TBLInsertTxnIDX], tableColInsTxnBat)
	AppendBatch(collector.data.bats[TBLInsertIDX], tableInsBat)
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
	delStart := GetRows(collector.data.bats[SEGDeleteIDX])
	segDelBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[SEGDeleteIDX])
	segDelTxn := makeRespBatchFromSchema(checkpointDataSchemas_Curr[SEGDeleteTxnIDX])
	segInsBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[SEGInsertIDX])
	segInsTxn := makeRespBatchFromSchema(checkpointDataSchemas_Curr[SEGInsertTxnIDX])
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		segNode := node
		if segNode.HasDropCommitted() {
			segDelBat.GetVectorByName(catalog.AttrRowID).Append(segid2rowid(&entry.ID), false)
			segDelBat.GetVectorByName(catalog.AttrCommitTs).Append(segNode.GetEnd(), false)
			segDelTxn.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetTable().GetDB().GetID(), false)
			segDelTxn.GetVectorByName(SnapshotAttr_TID).Append(entry.GetTable().GetID(), false)
			segNode.TxnMVCCNode.AppendTuple(segDelTxn)
		} else {
			segInsBat.GetVectorByName(SegmentAttr_ID).Append(entry.ID, false)
			segInsBat.GetVectorByName(SegmentAttr_CreateAt).Append(segNode.GetEnd(), false)
			buf := &bytes.Buffer{}
			if _, err := entry.SegmentNode.WriteTo(buf); err != nil {
				return err
			}
			segInsBat.GetVectorByName(SegmentAttr_SegNode).Append(buf.Bytes(), false)
			segInsTxn.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetTable().GetDB().GetID(), false)
			segInsTxn.GetVectorByName(SnapshotAttr_TID).Append(entry.GetTable().GetID(), false)
			segNode.TxnMVCCNode.AppendTuple(segInsTxn)
		}
	}
	delEnd := segDelBat.GetVectorByName(catalog.AttrRowID).Length()
	collector.data.UpdateSegMeta(entry.GetTable().ID, int32(delStart), int32(delEnd))
	AppendBatch(collector.data.bats[SEGDeleteIDX], segDelBat)
	AppendBatch(collector.data.bats[SEGDeleteTxnIDX], segDelTxn)
	AppendBatch(collector.data.bats[SEGInsertIDX], segInsBat)
	AppendBatch(collector.data.bats[SEGInsertTxnIDX], segInsTxn)
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
	insStart := GetRows(collector.data.bats[BLKMetaInsertIDX])
	delStart := GetRows(collector.data.bats[BLKMetaDeleteIDX])
	blkDNMetaDelBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKDNMetaDeleteIDX])
	blkDNMetaDelTxnBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKDNMetaDeleteTxnIDX])
	blkDNMetaInsBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKDNMetaInsertIDX])
	blkDNMetaInsTxnBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKDNMetaInsertTxnIDX])
	blkMetaDelBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKMetaDeleteIDX])
	blkMetaDelTxnBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKMetaDeleteTxnIDX])
	blkCNMetaInsBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKCNMetaInsertIDX])
	blkMetaInsBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKMetaInsertIDX])
	blkMetaInsTxnBat := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKMetaInsertTxnIDX])

	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		metaNode := node
		if metaNode.BaseNode.MetaLoc.IsEmpty() || metaNode.Aborted {
			if metaNode.HasDropCommitted() {
				blkDNMetaDelBat.GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID), false)
				blkDNMetaDelBat.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.GetEnd(), false)
				blkDNMetaDelTxnBat.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID(), false)
				blkDNMetaDelTxnBat.GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID(), false)
				metaNode.TxnMVCCNode.AppendTuple(blkDNMetaDelTxnBat)
				blkDNMetaDelTxnBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc), false)
				blkDNMetaDelTxnBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc), false)
			} else {
				blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_ID).Append(entry.ID, false)
				blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(entry.IsAppendable(), false)
				blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc), false)
				blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc), false)
				blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd(), false)
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted, false)
				blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(entry.GetSegment().ID, false)
				blkDNMetaInsBat.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.CreatedAt, false)
				blkDNMetaInsBat.GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID), false)
				blkDNMetaInsTxnBat.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID(), false)
				blkDNMetaInsTxnBat.GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID(), false)
				metaNode.TxnMVCCNode.AppendTuple(blkDNMetaInsTxnBat)
				blkDNMetaInsTxnBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc), false)
				blkDNMetaInsTxnBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc), false)
			}
		} else {
			if metaNode.HasDropCommitted() {
				blkMetaDelBat.GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID), false)
				blkMetaDelBat.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.GetEnd(), false)
				blkMetaDelTxnBat.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID(), false)
				blkMetaDelTxnBat.GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID(), false)
				metaNode.TxnMVCCNode.AppendTuple(blkMetaDelTxnBat)
				blkMetaDelTxnBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc), false)
				blkMetaDelTxnBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc), false)

				blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_ID).Append(entry.ID, false)
				blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(entry.IsAppendable(), false)
				blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc), false)
				blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc), false)
				blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd(), false)
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted, false)
				blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(entry.GetSegment().ID, false)
				blkCNMetaInsBat.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.CreatedAt, false)
				blkCNMetaInsBat.GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID), false)
			} else {
				blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_ID).Append(entry.ID, false)
				blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(entry.IsAppendable(), false)
				blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc), false)
				blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc), false)
				blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd(), false)
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted, false)
				blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(entry.GetSegment().ID, false)
				blkMetaInsBat.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.CreatedAt, false)
				blkMetaInsBat.GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID), false)
				blkMetaInsTxnBat.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID(), false)
				blkMetaInsTxnBat.GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID(), false)
				metaNode.TxnMVCCNode.AppendTuple(blkMetaInsTxnBat)
				blkMetaInsTxnBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc), false)
				blkMetaInsTxnBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc), false)
			}
		}
	}
	AppendBatch(collector.data.bats[BLKDNMetaDeleteIDX], blkDNMetaDelBat)
	AppendBatch(collector.data.bats[BLKDNMetaDeleteTxnIDX], blkDNMetaDelTxnBat)
	AppendBatch(collector.data.bats[BLKDNMetaInsertIDX], blkDNMetaInsBat)
	AppendBatch(collector.data.bats[BLKDNMetaInsertTxnIDX], blkDNMetaInsTxnBat)
	AppendBatch(collector.data.bats[BLKMetaDeleteIDX], blkMetaDelBat)
	AppendBatch(collector.data.bats[BLKMetaDeleteTxnIDX], blkMetaDelTxnBat)
	AppendBatch(collector.data.bats[BLKCNMetaInsertIDX], blkCNMetaInsBat)
	AppendBatch(collector.data.bats[BLKMetaInsertIDX], blkMetaInsBat)
	AppendBatch(collector.data.bats[BLKMetaInsertTxnIDX], blkMetaInsTxnBat)
	insEnd := GetRows(collector.data.bats[BLKMetaInsertIDX])
	delEnd := GetRows(collector.data.bats[BLKMetaDeleteIDX])
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
