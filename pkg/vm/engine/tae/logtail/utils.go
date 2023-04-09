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
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
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

var checkpointDataSchemas [MaxIDX]*catalog.Schema

var checkpointDataRefer [MaxIDX]*checkpointDataItem

func init() {
	checkpointDataSchemas = [MaxIDX]*catalog.Schema{
		MetaSchema,
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
	for idx, schema := range checkpointDataSchemas {
		checkpointDataRefer[idx] = &checkpointDataItem{
			schema,
			append(BaseTypes, schema.Types()...),
			append(BaseAttr, schema.AllNames()...),
		}
	}
}

func IncrementalCheckpointDataFactory(start, end types.TS) func(c *catalog.Catalog) (*CheckpointData, error) {
	return func(c *catalog.Catalog) (data *CheckpointData, err error) {
		collector := NewIncrementalCollector(start, end)
		defer collector.Close()
		err = c.RecurLoop(collector)
		if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			err = nil
		}
		collector.data.prepareMeta()
		if err != nil {
			return
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
		collector.data.prepareMeta()
		if err != nil {
			return
		}
		data = collector.OrphanData()
		return
	}
}

type CheckpointMeta struct {
	blkInsertOffset *common.ClosedInterval
	blkDeleteOffset *common.ClosedInterval
}

func NewCheckpointMeta() *CheckpointMeta {
	return &CheckpointMeta{}
}

type CheckpointData struct {
	meta map[uint64]*CheckpointMeta
	bats [MaxIDX]*containers.Batch
}

func NewCheckpointData() *CheckpointData {
	data := &CheckpointData{
		meta: make(map[uint64]*CheckpointMeta),
	}
	for idx, schema := range checkpointDataSchemas {
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
func (data *CheckpointData) GetTableMeta(tableID uint64) (meta *CheckpointMeta) {
	if len(data.meta) != 0 {
		meta = data.meta[tableID]
		return
	}
	for i := 0; i < data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_Tid).Length(); i++ {
		tid := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_Tid).Get(i).(uint64)
		insStart := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).Get(i).(int32)
		insEnd := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockInsertBatchEnd).Get(i).(int32)
		delStart := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchStart).Get(i).(int32)
		delEnd := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchEnd).Get(i).(int32)
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
		data.meta[tid] = meta
		// logutil.Infof("GetTableMeta TID=%d, INTERVAL=%s", tid, meta.blkInsertOffset.String())
	}
	meta = data.meta[tableID]
	return
}
func (data *CheckpointData) GetTableData(tid uint64) (ins, del, cnIns *api.Batch, err error) {
	var insTaeBat, delTaeBat, cnInsTaeBat *containers.Batch
	switch tid {
	case pkgcatalog.MO_DATABASE_ID:
		insTaeBat = data.bats[DBInsertIDX]
		delTaeBat = data.bats[DBDeleteIDX]
		if insTaeBat != nil {
			ins, err = containersBatchToProtoBatch(insTaeBat)
			if err != nil {
				return
			}
		}
		if delTaeBat != nil {
			del, err = containersBatchToProtoBatch(delTaeBat)
			if err != nil {
				return
			}
		}
		return
	case pkgcatalog.MO_TABLES_ID:
		insTaeBat = data.bats[TBLInsertIDX]
		delTaeBat = data.bats[TBLDeleteIDX]
		if insTaeBat != nil {
			ins, err = containersBatchToProtoBatch(insTaeBat)
			if err != nil {
				return
			}
		}
		if delTaeBat != nil {
			del, err = containersBatchToProtoBatch(delTaeBat)
			if err != nil {
				return
			}
		}
		return
	case pkgcatalog.MO_COLUMNS_ID:
		insTaeBat = data.bats[TBLColInsertIDX]
		delTaeBat = data.bats[TBLColDeleteIDX]
		if insTaeBat != nil {
			ins, err = containersBatchToProtoBatch(insTaeBat)
			if err != nil {
				return
			}
		}
		if delTaeBat != nil {
			del, err = containersBatchToProtoBatch(delTaeBat)
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
		return nil, nil, nil, nil
	}

	insInterval := meta.blkInsertOffset
	if insInterval != nil {
		insOffset := insInterval.Start
		insLength := insInterval.End - insInterval.Start
		insTaeBat = data.bats[BLKMetaInsertIDX].Window(int(insOffset), int(insLength))
	}

	delInterval := meta.blkDeleteOffset
	if delInterval != nil {
		delOffset := delInterval.Start
		delLength := delInterval.End - delInterval.Start
		delTaeBat = data.bats[BLKMetaDeleteIDX].Window(int(delOffset), int(delLength))
		cnInsTaeBat = data.bats[BLKCNMetaInsertIDX].Window(int(delOffset), int(delLength))
	}

	if insTaeBat != nil {
		ins, err = containersBatchToProtoBatch(insTaeBat)
		if err != nil {
			return
		}
	}
	if delTaeBat != nil {
		del, err = containersBatchToProtoBatch(delTaeBat)
		if err != nil {
			return
		}
		cnIns, err = containersBatchToProtoBatch(cnInsTaeBat)
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

func (data *CheckpointData) prepareMeta() {
	bat := data.bats[MetaIDX]
	for tid, meta := range data.meta {
		bat.GetVectorByName(SnapshotMetaAttr_Tid).Append(tid)
		if meta.blkInsertOffset == nil {
			bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).Append(int32(-1))
			bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchEnd).Append(int32(-1))
		} else {
			bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).Append(int32(meta.blkInsertOffset.Start))
			bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchEnd).Append(int32(meta.blkInsertOffset.End))
		}
		if meta.blkDeleteOffset == nil {
			bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchStart).Append(int32(-1))
			bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchEnd).Append(int32(-1))
		} else {
			bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchStart).Append(int32(meta.blkDeleteOffset.Start))
			bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchEnd).Append(int32(meta.blkDeleteOffset.End))
		}
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

func (data *CheckpointData) PrintData() {
	logutil.Info(BatchToString("BLK-META-DEL-BAT", data.bats[BLKMetaDeleteIDX], true))
	logutil.Info(BatchToString("BLK-META-INS-BAT", data.bats[BLKMetaInsertIDX], true))
}

func (data *CheckpointData) WriteTo(
	writer *blockio.BlockWriter) (blks []objectio.BlockObject, err error) {
	for _, bat := range data.bats {
		mobat := batch.New(true, bat.Attrs)
		mobat.Vecs = containers.UnmarshalToMoVecs(bat.Vecs)
		if _, err = writer.WriteBatchWithOutIndex(mobat); err != nil {
			return
		}
	}
	blks, _, err = writer.Sync(context.Background())
	return
}

func LoadBlkColumnsByMeta(cxt context.Context, colTypes []types.Type, colNames []string, id uint32, reader dataio.Reader) (*containers.Batch, error) {
	bat := containers.NewBatch()
	idxs := make([]uint16, len(colNames))
	for i := range colNames {
		idxs[i] = uint16(i)
	}
	ioResult, err := reader.LoadColumns(cxt, idxs, []uint32{id}, nil)
	if err != nil {
		return nil, err
	}

	for i, idx := range idxs {
		pkgVec := ioResult[0].Vecs[i]
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(colTypes[i])
		} else {
			vec = containers.NewVectorWithSharedMemory(pkgVec)
		}
		bat.AddVector(colNames[idx], vec)
		bat.Vecs[i] = vec

	}
	return bat, nil
}

func (data *CheckpointData) PrefetchFrom(
	ctx context.Context,
	service fileservice.FileService,
	key string) (err error) {
	reader, err := blockio.NewCheckPointReader(service, key)
	if err != nil {
		return
	}
	metas, err := reader.LoadBlocksMeta(ctx, common.DefaultAllocator)
	if err != nil {
		return
	}

	pref, err := blockio.BuildCkpPrefetch(service, key)
	if err != nil {
		return
	}
	for idx, item := range checkpointDataRefer {
		idxes := make([]uint16, len(item.attrs))
		for i := range item.attrs {
			idxes[i] = uint16(i)
		}
		pref.AddBlock(idxes, []uint32{metas[idx].GetID()})
	}
	return blockio.PrefetchWithMerged(pref)
}

// TODO:
// There need a global io pool
func (data *CheckpointData) ReadFrom(
	ctx context.Context,
	reader dataio.Reader,
	m *mpool.MPool) (err error) {
	metas, err := reader.LoadBlocksMeta(ctx, m)
	if err != nil {
		return
	}

	for idx, item := range checkpointDataRefer {
		var bat *containers.Batch
		bat, err = LoadBlkColumnsByMeta(ctx, item.types, item.attrs, metas[idx].GetID(), reader)
		if err != nil {
			return
		}
		data.bats[idx] = bat
	}

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
		if dbNode.HasDropCommitted() {
			// delScehma is empty, it will just fill rowid / commit ts
			catalogEntry2Batch(
				collector.data.bats[DBDeleteIDX],
				entry,
				node,
				DelSchema,
				txnimpl.FillDBRow,
				u64ToRowID(entry.GetID()),
				dbNode.GetEnd(),
				dbNode.GetEnd())
			dbNode.TxnMVCCNode.AppendTuple(collector.data.bats[DBDeleteTxnIDX])
			collector.data.bats[DBDeleteTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetID())
		} else {
			catalogEntry2Batch(collector.data.bats[DBInsertIDX],
				entry,
				node,
				catalog.SystemDBSchema,
				txnimpl.FillDBRow,
				u64ToRowID(entry.GetID()),
				dbNode.GetEnd(),
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
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		tblNode := node
		if !tblNode.HasDropCommitted() {
			for _, syscol := range catalog.SystemColumnSchema.ColDefs {
				txnimpl.FillColumnRow(
					entry,
					syscol.Name,
					collector.data.bats[TBLColInsertIDX].GetVectorByName(syscol.Name),
				)
			}
			rowidVec := collector.data.bats[TBLColInsertIDX].GetVectorByName(catalog.AttrRowID)
			commitVec := collector.data.bats[TBLColInsertIDX].GetVectorByName(catalog.AttrCommitTs)
			for _, usercol := range entry.GetSchema().ColDefs {
				rowidVec.Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name))))
				commitVec.Append(tblNode.GetEnd())
			}

			collector.data.bats[TBLInsertTxnIDX].GetVectorByName(
				SnapshotAttr_BlockMaxRow).Append(entry.GetSchema().BlockMaxRows)
			collector.data.bats[TBLInsertTxnIDX].GetVectorByName(
				SnapshotAttr_SegmentMaxBlock).Append(entry.GetSchema().SegmentMaxBlocks)

			catalogEntry2Batch(
				collector.data.bats[TBLInsertIDX],
				entry,
				node,
				catalog.SystemTableSchema,
				txnimpl.FillTableRow,
				u64ToRowID(entry.GetID()),
				tblNode.GetEnd(),
				tblNode.GetEnd(),
			)

			tblNode.TxnMVCCNode.AppendTuple(collector.data.bats[TBLInsertTxnIDX])
		} else {
			collector.data.bats[TBLDeleteTxnIDX].GetVectorByName(
				SnapshotAttr_DBID).Append(entry.GetDB().GetID())
			collector.data.bats[TBLDeleteTxnIDX].GetVectorByName(
				SnapshotAttr_TID).Append(entry.GetID())

			rowidVec := collector.data.bats[TBLColDeleteIDX].GetVectorByName(catalog.AttrRowID)
			commitVec := collector.data.bats[TBLColDeleteIDX].GetVectorByName(catalog.AttrCommitTs)
			for _, usercol := range entry.GetSchema().ColDefs {
				rowidVec.Append(
					bytesToRowID([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name))),
				)
				commitVec.Append(tblNode.GetEnd())
			}

			catalogEntry2Batch(
				collector.data.bats[TBLDeleteIDX],
				entry,
				node,
				DelSchema,
				txnimpl.FillTableRow,
				u64ToRowID(entry.GetID()),
				tblNode.GetEnd(),
				tblNode.GetEnd(),
			)
			tblNode.TxnMVCCNode.AppendTuple(collector.data.bats[TBLDeleteTxnIDX])
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
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		segNode := node
		if segNode.HasDropCommitted() {
			collector.data.bats[SEGDeleteIDX].GetVectorByName(catalog.AttrRowID).Append(segid2rowid(&entry.ID))
			collector.data.bats[SEGDeleteIDX].GetVectorByName(catalog.AttrCommitTs).Append(segNode.GetEnd())
			collector.data.bats[SEGDeleteTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetTable().GetDB().GetID())
			collector.data.bats[SEGDeleteTxnIDX].GetVectorByName(SnapshotAttr_TID).Append(entry.GetTable().GetID())
			segNode.TxnMVCCNode.AppendTuple(collector.data.bats[SEGDeleteTxnIDX])
		} else {
			collector.data.bats[SEGInsertIDX].GetVectorByName(SegmentAttr_ID).Append(entry.ID)
			collector.data.bats[SEGInsertIDX].GetVectorByName(SegmentAttr_CreateAt).Append(segNode.GetEnd())
			collector.data.bats[SEGInsertIDX].GetVectorByName(SegmentAttr_State).Append(entry.IsAppendable())
			collector.data.bats[SEGInsertIDX].GetVectorByName(SegmentAttr_Sorted).Append(entry.IsSorted())
			collector.data.bats[SEGInsertTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetTable().GetDB().GetID())
			collector.data.bats[SEGInsertTxnIDX].GetVectorByName(SnapshotAttr_TID).Append(entry.GetTable().GetID())
			segNode.TxnMVCCNode.AppendTuple(collector.data.bats[SEGInsertTxnIDX])
		}
	}
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
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		metaNode := node
		if metaNode.BaseNode.MetaLoc == "" || metaNode.Aborted {
			if metaNode.HasDropCommitted() {
				collector.data.bats[BLKDNMetaDeleteIDX].GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID))
				collector.data.bats[BLKDNMetaDeleteIDX].GetVectorByName(catalog.AttrCommitTs).Append(metaNode.GetEnd())
				collector.data.bats[BLKDNMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID())
				collector.data.bats[BLKDNMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID())
				collector.data.bats[BLKDNMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_SegID).Append(entry.GetSegment().ID)
				metaNode.TxnMVCCNode.AppendTuple(collector.data.bats[BLKDNMetaDeleteTxnIDX])
				collector.data.bats[BLKDNMetaDeleteTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc))
				collector.data.bats[BLKDNMetaDeleteTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc))
			} else {
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_ID).Append(entry.ID)
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(entry.IsAppendable())
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc))
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc))
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd())
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted)
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(entry.GetSegment().ID)
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(catalog.AttrCommitTs).Append(metaNode.CreatedAt)
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID))
				collector.data.bats[BLKDNMetaInsertTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID())
				collector.data.bats[BLKDNMetaInsertTxnIDX].GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID())
				collector.data.bats[BLKDNMetaInsertTxnIDX].GetVectorByName(SnapshotAttr_SegID).Append(entry.GetSegment().ID)
				metaNode.TxnMVCCNode.AppendTuple(collector.data.bats[BLKDNMetaInsertTxnIDX])
				collector.data.bats[BLKDNMetaInsertTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc))
				collector.data.bats[BLKDNMetaInsertTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc))
			}
		} else {
			if metaNode.HasDropCommitted() {
				collector.data.bats[BLKMetaDeleteIDX].GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID))
				collector.data.bats[BLKMetaDeleteIDX].GetVectorByName(catalog.AttrCommitTs).Append(metaNode.GetEnd())
				collector.data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID())
				collector.data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID())
				collector.data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_SegID).Append(entry.GetSegment().ID)
				metaNode.TxnMVCCNode.AppendTuple(collector.data.bats[BLKMetaDeleteTxnIDX])
				collector.data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc))
				collector.data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc))

				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_ID).Append(entry.ID)
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(entry.IsAppendable())
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc))
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc))
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd())
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted)
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(entry.GetSegment().ID)
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.AttrCommitTs).Append(metaNode.CreatedAt)
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID))
			} else {
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_ID).Append(entry.ID)
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(entry.IsAppendable())
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc))
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc))
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd())
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted)
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(entry.GetSegment().ID)
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.AttrCommitTs).Append(metaNode.CreatedAt)
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID))
				collector.data.bats[BLKMetaInsertTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID())
				collector.data.bats[BLKMetaInsertTxnIDX].GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID())
				collector.data.bats[BLKMetaInsertTxnIDX].GetVectorByName(SnapshotAttr_SegID).Append(entry.GetSegment().ID)
				metaNode.TxnMVCCNode.AppendTuple(collector.data.bats[BLKMetaInsertTxnIDX])
				collector.data.bats[BLKMetaInsertTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc))
				collector.data.bats[BLKMetaInsertTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc))
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
