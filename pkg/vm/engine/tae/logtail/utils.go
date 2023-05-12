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
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	segDeleteOffset *common.ClosedInterval
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
	for i := 0; i < data.bats[MetaIDX].GetVectorByName(SnapshotAttr_TID).Length(); i++ {
		tid := data.bats[MetaIDX].GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		insStart := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).Get(i).(int32)
		insEnd := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockInsertBatchEnd).Get(i).(int32)
		delStart := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchStart).Get(i).(int32)
		delEnd := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchEnd).Get(i).(int32)
		segDelStart := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_SegDeleteBatchStart).Get(i).(int32)
		segDelEnd := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_SegDeleteBatchEnd).Get(i).(int32)
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
func (data *CheckpointData) GetTableData(tid uint64) (ins, del, cnIns, segDel *api.Batch, err error) {
	var insTaeBat, delTaeBat, cnInsTaeBat, segDelTaeBat *containers.Batch
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
		return nil, nil, nil, nil, nil
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

	segDelInterval := meta.segDeleteOffset
	if segDelInterval != nil {
		segDelOffset := segDelInterval.Start
		segDelLength := segDelInterval.End - segDelInterval.Start
		segDelTaeBat = data.bats[SEGDeleteIDX].Window(int(segDelOffset), int(segDelLength))
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
	if segDelTaeBat != nil {
		segDel, err = containersBatchToProtoBatch(segDelTaeBat)
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

func (data *CheckpointData) PrintData() {
	logutil.Info(BatchToString("BLK-META-DEL-BAT", data.bats[BLKMetaDeleteIDX], true))
	logutil.Info(BatchToString("BLK-META-INS-BAT", data.bats[BLKMetaInsertIDX], true))
}

func (data *CheckpointData) WriteTo(
	writer *blockio.BlockWriter) (blks []objectio.BlockObject, err error) {
	for _, bat := range data.bats {
		if _, err = writer.WriteBatchWithOutIndex(containers.ToCNBatch(bat)); err != nil {
			return
		}
	}
	blks, _, err = writer.Sync(context.Background())
	return
}

func LoadBlkColumnsByMeta(cxt context.Context, colTypes []types.Type, colNames []string, id uint16, reader *blockio.BlockReader) (*containers.Batch, error) {
	bat := containers.NewBatch()
	idxs := make([]uint16, len(colNames))
	for i := range colNames {
		idxs[i] = uint16(i)
	}
	ioResult, err := reader.LoadColumns(cxt, idxs, nil, id, nil)
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

func (data *CheckpointData) PrefetchFrom(
	ctx context.Context,
	service fileservice.FileService,
	key objectio.Location) (err error) {

	pref, err := blockio.BuildPrefetchParams(service, key)
	if err != nil {
		return
	}
	for idx, item := range checkpointDataRefer {
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
	reader *blockio.BlockReader,
	m *mpool.MPool) (err error) {

	for idx, item := range checkpointDataRefer {
		var bat *containers.Batch
		bat, err = LoadBlkColumnsByMeta(ctx, item.types, item.attrs, uint16(idx), reader)
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
				DelSchema,
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
					collector.data.bats[TBLColInsertIDX].GetVectorByName(syscol.Name),
				)
			}
			// send dropped column del
			for _, name := range tblNode.BaseNode.Schema.Extra.DroppedAttrs {
				collector.data.bats[TBLColDeleteIDX].GetVectorByName(catalog.AttrRowID).Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", entry.GetID(), name))), false)
				collector.data.bats[TBLColDeleteIDX].GetVectorByName(catalog.AttrCommitTs).Append(tblNode.GetEnd(), false)
			}
			rowidVec := collector.data.bats[TBLColInsertIDX].GetVectorByName(catalog.AttrRowID)
			commitVec := collector.data.bats[TBLColInsertIDX].GetVectorByName(catalog.AttrCommitTs)
			for _, usercol := range tblNode.BaseNode.Schema.ColDefs {
				rowidVec.Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name))), false)
				commitVec.Append(tblNode.GetEnd(), false)
			}

			collector.data.bats[TBLInsertTxnIDX].GetVectorByName(
				SnapshotAttr_BlockMaxRow).Append(entry.GetLastestSchema().BlockMaxRows, false)
			collector.data.bats[TBLInsertTxnIDX].GetVectorByName(
				SnapshotAttr_SegmentMaxBlock).Append(entry.GetLastestSchema().SegmentMaxBlocks, false)
			collector.data.bats[TBLInsertTxnIDX].GetVectorByName(
				SnapshotAttr_SchemaExtra).Append(tblNode.BaseNode.Schema.MustGetExtraBytes(), false)

			catalogEntry2Batch(
				collector.data.bats[TBLInsertIDX],
				entry,
				tblNode,
				catalog.SystemTableSchema,
				txnimpl.FillTableRow,
				u64ToRowID(entry.GetID()),
				tblNode.GetEnd(),
			)

			tblNode.TxnMVCCNode.AppendTuple(collector.data.bats[TBLInsertTxnIDX])
		}
		if dropped {
			collector.data.bats[TBLDeleteTxnIDX].GetVectorByName(
				SnapshotAttr_DBID).Append(entry.GetDB().GetID(), false)
			collector.data.bats[TBLDeleteTxnIDX].GetVectorByName(
				SnapshotAttr_TID).Append(entry.GetID(), false)

			rowidVec := collector.data.bats[TBLColDeleteIDX].GetVectorByName(catalog.AttrRowID)
			commitVec := collector.data.bats[TBLColDeleteIDX].GetVectorByName(catalog.AttrCommitTs)
			for _, usercol := range tblNode.BaseNode.Schema.ColDefs {
				rowidVec.Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name))), false)
				commitVec.Append(tblNode.GetEnd(), false)
			}

			catalogEntry2Batch(
				collector.data.bats[TBLDeleteIDX],
				entry,
				tblNode,
				DelSchema,
				txnimpl.FillTableRow,
				u64ToRowID(entry.GetID()),
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
	delStart := collector.data.bats[SEGDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		segNode := node
		if segNode.HasDropCommitted() {
			collector.data.bats[SEGDeleteIDX].GetVectorByName(catalog.AttrRowID).Append(segid2rowid(&entry.ID), false)
			collector.data.bats[SEGDeleteIDX].GetVectorByName(catalog.AttrCommitTs).Append(segNode.GetEnd(), false)
			collector.data.bats[SEGDeleteTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetTable().GetDB().GetID(), false)
			collector.data.bats[SEGDeleteTxnIDX].GetVectorByName(SnapshotAttr_TID).Append(entry.GetTable().GetID(), false)
			segNode.TxnMVCCNode.AppendTuple(collector.data.bats[SEGDeleteTxnIDX])
		} else {
			collector.data.bats[SEGInsertIDX].GetVectorByName(SegmentAttr_ID).Append(entry.ID, false)
			collector.data.bats[SEGInsertIDX].GetVectorByName(SegmentAttr_CreateAt).Append(segNode.GetEnd(), false)
			collector.data.bats[SEGInsertIDX].GetVectorByName(SegmentAttr_State).Append(entry.IsAppendable(), false)
			collector.data.bats[SEGInsertIDX].GetVectorByName(SegmentAttr_Sorted).Append(entry.IsSorted(), false)
			collector.data.bats[SEGInsertTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetTable().GetDB().GetID(), false)
			collector.data.bats[SEGInsertTxnIDX].GetVectorByName(SnapshotAttr_TID).Append(entry.GetTable().GetID(), false)
			segNode.TxnMVCCNode.AppendTuple(collector.data.bats[SEGInsertTxnIDX])
		}
	}
	delEnd := collector.data.bats[SEGDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
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
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		metaNode := node
		if metaNode.BaseNode.MetaLoc.IsEmpty() || metaNode.Aborted {
			if metaNode.HasDropCommitted() {
				collector.data.bats[BLKDNMetaDeleteIDX].GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID), false)
				collector.data.bats[BLKDNMetaDeleteIDX].GetVectorByName(catalog.AttrCommitTs).Append(metaNode.GetEnd(), false)
				collector.data.bats[BLKDNMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID(), false)
				collector.data.bats[BLKDNMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID(), false)
				metaNode.TxnMVCCNode.AppendTuple(collector.data.bats[BLKDNMetaDeleteTxnIDX])
				collector.data.bats[BLKDNMetaDeleteTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc), false)
				collector.data.bats[BLKDNMetaDeleteTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc), false)
			} else {
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_ID).Append(entry.ID, false)
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(entry.IsAppendable(), false)
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc), false)
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc), false)
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd(), false)
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted, false)
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(entry.GetSegment().ID, false)
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(catalog.AttrCommitTs).Append(metaNode.CreatedAt, false)
				collector.data.bats[BLKDNMetaInsertIDX].GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID), false)
				collector.data.bats[BLKDNMetaInsertTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID(), false)
				collector.data.bats[BLKDNMetaInsertTxnIDX].GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID(), false)
				metaNode.TxnMVCCNode.AppendTuple(collector.data.bats[BLKDNMetaInsertTxnIDX])
				collector.data.bats[BLKDNMetaInsertTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc), false)
				collector.data.bats[BLKDNMetaInsertTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc), false)
			}
		} else {
			if metaNode.HasDropCommitted() {
				collector.data.bats[BLKMetaDeleteIDX].GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID), false)
				collector.data.bats[BLKMetaDeleteIDX].GetVectorByName(catalog.AttrCommitTs).Append(metaNode.GetEnd(), false)
				collector.data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID(), false)
				collector.data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID(), false)
				metaNode.TxnMVCCNode.AppendTuple(collector.data.bats[BLKMetaDeleteTxnIDX])
				collector.data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc), false)
				collector.data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc), false)

				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_ID).Append(entry.ID, false)
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(entry.IsAppendable(), false)
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc), false)
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc), false)
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd(), false)
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted, false)
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(entry.GetSegment().ID, false)
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.AttrCommitTs).Append(metaNode.CreatedAt, false)
				collector.data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID), false)
			} else {
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_ID).Append(entry.ID, false)
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(entry.IsAppendable(), false)
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc), false)
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc), false)
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd(), false)
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted, false)
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(entry.GetSegment().ID, false)
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.AttrCommitTs).Append(metaNode.CreatedAt, false)
				collector.data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.AttrRowID).Append(blockid2rowid(&entry.ID), false)
				collector.data.bats[BLKMetaInsertTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID(), false)
				collector.data.bats[BLKMetaInsertTxnIDX].GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID(), false)
				metaNode.TxnMVCCNode.AppendTuple(collector.data.bats[BLKMetaInsertTxnIDX])
				collector.data.bats[BLKMetaInsertTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.BaseNode.MetaLoc), false)
				collector.data.bats[BLKMetaInsertTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.BaseNode.DeltaLoc), false)
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
