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
	"fmt"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
)

func IncrementalCheckpointDataFactory(start, end types.TS) func(c *catalog.Catalog) (*CheckpointData, error) {
	return func(c *catalog.Catalog) (data *CheckpointData, err error) {
		collector := NewIncrementalCollector(start, end)
		defer collector.Close()
		err = c.RecurLoop(collector)
		if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			err = nil
		}
		if err != nil {
			return
		}
		data = collector.OrphanData()
		return
	}
}

type CheckpointData struct {
	dbInsBatch         *containers.Batch
	dbInsTxnBatch      *containers.Batch
	dbDelBatch         *containers.Batch
	dbDelTxnBatch      *containers.Batch
	tblInsBatch        *containers.Batch
	tblInsTxnBatch     *containers.Batch
	tblDelBatch        *containers.Batch
	tblDelTxnBatch     *containers.Batch
	tblColInsBatch     *containers.Batch
	tblColDelBatch     *containers.Batch
	segInsBatch        *containers.Batch
	segInsTxnBatch     *containers.Batch
	segDelBatch        *containers.Batch
	segDelTxnBatch     *containers.Batch
	blkMetaInsBatch    *containers.Batch
	blkMetaInsTxnBatch *containers.Batch
	blkMetaDelBatch    *containers.Batch
	blkMetaDelTxnBatch *containers.Batch
}

func NewCheckpointData() *CheckpointData {
	return &CheckpointData{
		dbInsBatch:         makeRespBatchFromSchema(catalog.SystemDBSchema),
		dbInsTxnBatch:      makeRespBatchFromSchema(TxnNodeSchema),
		dbDelBatch:         makeRespBatchFromSchema(DelSchema),
		dbDelTxnBatch:      makeRespBatchFromSchema(DBDNSchema),
		tblInsBatch:        makeRespBatchFromSchema(catalog.SystemTableSchema),
		tblInsTxnBatch:     makeRespBatchFromSchema(TblDNSchema),
		tblDelBatch:        makeRespBatchFromSchema(DelSchema),
		tblDelTxnBatch:     makeRespBatchFromSchema(TblDNSchema),
		tblColInsBatch:     makeRespBatchFromSchema(catalog.SystemColumnSchema),
		tblColDelBatch:     makeRespBatchFromSchema(DelSchema),
		segInsBatch:        makeRespBatchFromSchema(SegSchema),
		segInsTxnBatch:     makeRespBatchFromSchema(SegDNSchema),
		segDelBatch:        makeRespBatchFromSchema(DelSchema),
		segDelTxnBatch:     makeRespBatchFromSchema(SegDNSchema),
		blkMetaInsBatch:    makeRespBatchFromSchema(BlkMetaSchema),
		blkMetaInsTxnBatch: makeRespBatchFromSchema(BlkDNSchema),
		blkMetaDelBatch:    makeRespBatchFromSchema(DelSchema),
		blkMetaDelTxnBatch: makeRespBatchFromSchema(BlkDNSchema),
	}
}

type IncrementalCollector struct {
	*catalog.LoopProcessor
	start, end types.TS

	data *CheckpointData
}

func NewIncrementalCollector(start, end types.TS) *IncrementalCollector {
	collector := &IncrementalCollector{
		LoopProcessor: new(catalog.LoopProcessor),
		start:         start,
		end:           end,
		data:          NewCheckpointData(),
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
	ins, dnins, del, dndel = data.GetBlkBatchs()
	c.OnReplayBlockBatch(ins, dnins, del, dndel, dataFactory)
	return
}

func (data *CheckpointData) WriteTo(
	writer *blockio.Writer) (blks []objectio.BlockObject, err error) {
	if _, err = writer.WriteBlock(data.dbInsBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.dbInsTxnBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.dbDelBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.dbDelTxnBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.tblInsBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.tblInsTxnBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.tblDelBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.tblDelTxnBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.tblColInsBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.tblColDelBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.segInsBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.segInsTxnBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.segDelBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.segDelTxnBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.blkMetaInsBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.blkMetaInsTxnBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.blkMetaDelBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.blkMetaDelTxnBatch); err != nil {
		return
	}
	blks, err = writer.Sync()
	return
}

func (data *CheckpointData) ReadFrom(
	reader *blockio.Reader,
	m *mpool.MPool) (err error) {
	metas, err := reader.ReadMetas(m)
	if err != nil {
		return
	}
	if data.dbInsBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, catalog.SystemDBSchema.Types()...),
		append(BaseAttr, catalog.SystemDBSchema.AllNames()...),
		append([]bool{false, false}, catalog.SystemDBSchema.AllNullables()...),
		metas[0]); err != nil {
		return
	}
	if data.dbInsTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, TxnNodeSchema.Types()...),
		append(BaseAttr, TxnNodeSchema.AllNames()...),
		append([]bool{false, false}, TxnNodeSchema.AllNullables()...),
		metas[1]); err != nil {
		return
	}
	if data.dbDelBatch, err = reader.LoadBlkColumnsByMeta(
		BaseTypes,
		BaseAttr,
		[]bool{false, false},
		metas[2]); err != nil {
		return
	}
	if data.dbDelTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, DBDNSchema.Types()...),
		append(BaseAttr, DBDNSchema.AllNames()...),
		append([]bool{false, false}, DBDNSchema.AllNullables()...),
		metas[3]); err != nil {
		return
	}
	if data.tblInsBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, catalog.SystemTableSchema.Types()...),
		append(BaseAttr, catalog.SystemTableSchema.AllNames()...),
		append([]bool{false, false}, catalog.SystemTableSchema.AllNullables()...),
		metas[4]); err != nil {
		return
	}
	if data.tblInsTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, TblDNSchema.Types()...),
		append(BaseAttr, TblDNSchema.AllNames()...),
		append([]bool{false, false}, TblDNSchema.AllNullables()...),
		metas[5]); err != nil {
		return
	}
	if data.tblDelBatch, err = reader.LoadBlkColumnsByMeta(
		BaseTypes,
		BaseAttr,
		[]bool{false, false},
		metas[6]); err != nil {
		return
	}
	if data.tblDelTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, TblDNSchema.Types()...),
		append(BaseAttr, TblDNSchema.AllNames()...),
		append([]bool{false, false}, TblDNSchema.AllNullables()...),
		metas[7]); err != nil {
		return
	}
	if data.tblColInsBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, catalog.SystemColumnSchema.Types()...),
		append(BaseAttr, catalog.SystemColumnSchema.AllNames()...),
		append([]bool{false, false}, catalog.SystemColumnSchema.AllNullables()...),
		metas[8]); err != nil {
		return
	}
	if data.tblColDelBatch, err = reader.LoadBlkColumnsByMeta(
		BaseTypes,
		BaseAttr,
		[]bool{false, false},
		metas[9]); err != nil {
		return
	}
	if data.segInsBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, SegSchema.Types()...),
		append(BaseAttr, SegSchema.AllNames()...),
		append([]bool{false, false}, SegSchema.AllNullables()...),
		metas[10]); err != nil {
		return
	}
	if data.segInsTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, SegDNSchema.Types()...),
		append(BaseAttr, SegDNSchema.AllNames()...),
		append([]bool{false, false}, SegDNSchema.AllNullables()...),
		metas[11]); err != nil {
		return
	}
	if data.segDelBatch, err = reader.LoadBlkColumnsByMeta(
		BaseTypes,
		BaseAttr,
		[]bool{false, false},
		metas[12]); err != nil {
		return
	}
	if data.segDelTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, SegDNSchema.Types()...),
		append(BaseAttr, SegDNSchema.AllNames()...),
		append([]bool{false, false}, SegDNSchema.AllNullables()...),
		metas[13]); err != nil {
		return
	}
	if data.blkMetaInsBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, BlkMetaSchema.Types()...),
		append(BaseAttr, BlkMetaSchema.AllNames()...),
		append([]bool{false, false}, BlkMetaSchema.AllNullables()...),
		metas[14]); err != nil {
		return
	}
	if data.blkMetaInsTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, BlkDNSchema.Types()...),
		append(BaseAttr, BlkDNSchema.AllNames()...),
		append([]bool{false, false}, BlkDNSchema.AllNullables()...),
		metas[15]); err != nil {
		return
	}
	if data.blkMetaDelBatch, err = reader.LoadBlkColumnsByMeta(
		BaseTypes,
		BaseAttr,
		[]bool{false, false},
		metas[16]); err != nil {
		return
	}
	if data.blkMetaDelTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, BlkDNSchema.Types()...),
		append(BaseAttr, BlkDNSchema.AllNames()...),
		append([]bool{false, false}, BlkDNSchema.AllNullables()...),
		metas[17]); err != nil {
		return
	}
	return
}

func (data *CheckpointData) Close() {
	if data.dbInsBatch != nil {
		data.dbInsBatch.Close()
		data.dbInsBatch = nil
	}
	if data.dbInsTxnBatch != nil {
		data.dbInsTxnBatch.Close()
		data.dbInsTxnBatch = nil
	}
	if data.dbDelBatch != nil {
		data.dbDelBatch.Close()
		data.dbDelBatch = nil
	}
	if data.dbDelTxnBatch != nil {
		data.dbDelTxnBatch.Close()
		data.dbDelTxnBatch = nil
	}
	if data.tblInsBatch != nil {
		data.tblInsBatch.Close()
		data.tblInsBatch = nil
	}
	if data.tblInsTxnBatch != nil {
		data.tblInsTxnBatch.Close()
		data.tblInsTxnBatch = nil
	}
	if data.tblDelBatch != nil {
		data.tblDelBatch.Close()
		data.tblDelBatch = nil
	}
	if data.tblDelTxnBatch != nil {
		data.tblDelTxnBatch.Close()
		data.tblDelTxnBatch = nil
	}
	if data.tblColInsBatch != nil {
		data.tblColInsBatch.Close()
		data.tblColInsBatch = nil
	}
	if data.tblColDelBatch != nil {
		data.tblColDelBatch.Close()
		data.tblColDelBatch = nil
	}
	if data.segInsBatch != nil {
		data.segInsBatch.Close()
		data.segInsBatch = nil
	}
	if data.segInsTxnBatch != nil {
		data.segInsTxnBatch.Close()
		data.segInsTxnBatch = nil
	}
	if data.segDelBatch != nil {
		data.segDelBatch.Close()
		data.segDelBatch = nil
	}
	if data.segDelTxnBatch != nil {
		data.segDelTxnBatch.Close()
		data.segDelTxnBatch = nil
	}
	if data.blkMetaInsBatch != nil {
		data.blkMetaInsBatch.Close()
		data.blkMetaInsBatch = nil
	}
	if data.blkMetaInsTxnBatch != nil {
		data.blkMetaInsTxnBatch.Close()
		data.blkMetaInsTxnBatch = nil
	}
	if data.blkMetaDelBatch != nil {
		data.blkMetaDelBatch.Close()
		data.blkMetaDelBatch = nil
	}
	if data.blkMetaDelTxnBatch != nil {
		data.blkMetaDelTxnBatch.Close()
		data.blkMetaDelTxnBatch = nil
	}
}
func (data *CheckpointData) GetDBBatchs() (*containers.Batch, *containers.Batch, *containers.Batch, *containers.Batch) {
	return data.dbInsBatch, data.dbInsTxnBatch, data.dbDelBatch, data.dbDelTxnBatch
}
func (data *CheckpointData) GetTblBatchs() (*containers.Batch, *containers.Batch, *containers.Batch, *containers.Batch, *containers.Batch) {
	return data.tblInsBatch, data.tblInsTxnBatch, data.tblColInsBatch, data.tblDelBatch, data.tblDelTxnBatch
}
func (data *CheckpointData) GetSegBatchs() (*containers.Batch, *containers.Batch, *containers.Batch, *containers.Batch) {
	return data.segInsBatch, data.segInsTxnBatch, data.segDelBatch, data.segDelTxnBatch
}
func (data *CheckpointData) GetBlkBatchs() (*containers.Batch, *containers.Batch, *containers.Batch, *containers.Batch) {
	return data.blkMetaInsBatch, data.blkMetaInsTxnBatch, data.blkMetaDelBatch, data.blkMetaDelTxnBatch
}

func (collector *IncrementalCollector) VisitDB(entry *catalog.DBEntry) error {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		dbNode := node.(*catalog.DBMVCCNode)
		if dbNode.HasDropCommitted() {
			// delScehma is empty, it will just fill rowid / commit ts
			catalogEntry2Batch(collector.data.dbDelBatch,
				entry, DelSchema,
				txnimpl.FillDBRow,
				u64ToRowID(entry.GetID()),
				dbNode.GetEnd())
			dbNode.TxnMVCCNode.AppendTuple(collector.data.dbDelTxnBatch)
			collector.data.dbDelTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetID())
		} else {
			catalogEntry2Batch(collector.data.dbInsBatch,
				entry,
				catalog.SystemDBSchema,
				txnimpl.FillDBRow,
				u64ToRowID(entry.GetID()),
				dbNode.GetEnd())
			dbNode.TxnMVCCNode.AppendTuple(collector.data.dbInsTxnBatch)
		}
	}
	return nil
}

func (collector *IncrementalCollector) VisitTable(entry *catalog.TableEntry) (err error) {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		tblNode := node.(*catalog.TableMVCCNode)
		if !tblNode.HasDropCommitted() {
			for _, syscol := range catalog.SystemColumnSchema.ColDefs {
				txnimpl.FillColumnRow(entry, syscol.Name, collector.data.tblColInsBatch.GetVectorByName(syscol.Name))
				rowidVec := collector.data.tblColInsBatch.GetVectorByName(catalog.AttrRowID)
				commitVec := collector.data.tblColInsBatch.GetVectorByName(catalog.AttrCommitTs)
				for _, usercol := range entry.GetSchema().ColDefs {
					rowidVec.Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name))))
					commitVec.Append(tblNode.GetEnd())
				}
			}
			collector.data.tblInsTxnBatch.GetVectorByName(SnapshotAttr_BlockMaxRow).Append(entry.GetSchema().BlockMaxRows)
			collector.data.tblInsTxnBatch.GetVectorByName(SnapshotAttr_SegmentMaxBlock).Append(entry.GetSchema().SegmentMaxBlocks)
			catalogEntry2Batch(collector.data.tblInsBatch,
				entry,
				catalog.SystemTableSchema,
				txnimpl.FillTableRow,
				u64ToRowID(entry.GetID()),
				tblNode.GetEnd())
			tblNode.TxnMVCCNode.AppendTuple(collector.data.tblInsTxnBatch)
		} else {
			collector.data.tblDelTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetDB().GetID())
			collector.data.tblDelTxnBatch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetID())
			rowidVec := collector.data.tblColDelBatch.GetVectorByName(catalog.AttrRowID)
			commitVec := collector.data.tblColDelBatch.GetVectorByName(catalog.AttrCommitTs)
			for _, usercol := range entry.GetSchema().ColDefs {
				rowidVec.Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name))))
				commitVec.Append(tblNode.GetEnd())
			}
			catalogEntry2Batch(collector.data.tblDelBatch,
				entry, DelSchema,
				txnimpl.FillTableRow,
				u64ToRowID(entry.GetID()),
				tblNode.GetEnd())
			tblNode.TxnMVCCNode.AppendTuple(collector.data.tblDelTxnBatch)
		}
	}
	return nil
}

func (collector *IncrementalCollector) VisitSeg(entry *catalog.SegmentEntry) (err error) {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		segNode := node.(*catalog.MetadataMVCCNode)
		if segNode.HasDropCommitted() {
			collector.data.segDelBatch.GetVectorByName(catalog.AttrRowID).Append(u64ToRowID(entry.ID))
			collector.data.segDelBatch.GetVectorByName(catalog.AttrCommitTs).Append(segNode.GetEnd())
			collector.data.segDelTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetTable().GetDB().GetID())
			collector.data.segDelTxnBatch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetTable().GetID())
			segNode.TxnMVCCNode.AppendTuple(collector.data.segDelTxnBatch)
		} else {
			collector.data.segInsBatch.GetVectorByName(SegmentAttr_ID).Append(entry.GetID())
			collector.data.segInsBatch.GetVectorByName(SegmentAttr_CreateAt).Append(segNode.GetEnd())
			collector.data.segInsBatch.GetVectorByName(SegmentAttr_State).Append(entry.IsAppendable())
			collector.data.segInsTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetTable().GetDB().GetID())
			collector.data.segInsTxnBatch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetTable().GetID())
			segNode.TxnMVCCNode.AppendTuple(collector.data.segInsTxnBatch)
		}
	}
	return nil
}
func (collector *IncrementalCollector) VisitBlk(entry *catalog.BlockEntry) (err error) {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		metaNode := node.(*catalog.MetadataMVCCNode)
		if metaNode.HasDropCommitted() {
			collector.data.blkMetaDelBatch.GetVectorByName(catalog.AttrRowID).Append(u64ToRowID(entry.ID))
			collector.data.blkMetaDelBatch.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.GetEnd())
			collector.data.blkMetaDelTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID())
			collector.data.blkMetaDelTxnBatch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID())
			collector.data.blkMetaDelTxnBatch.GetVectorByName(SnapshotAttr_SegID).Append(entry.GetSegment().GetID())
			metaNode.TxnMVCCNode.AppendTuple(collector.data.blkMetaDelTxnBatch)
			collector.data.blkMetaDelTxnBatch.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.MetaLoc))
			collector.data.blkMetaDelTxnBatch.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.DeltaLoc))
		} else {
			collector.data.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_ID).Append(entry.ID)
			collector.data.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(entry.IsAppendable())
			collector.data.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.MetaLoc))
			collector.data.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.DeltaLoc))
			collector.data.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd())
			collector.data.blkMetaInsBatch.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.CreatedAt)
			collector.data.blkMetaInsBatch.GetVectorByName(catalog.AttrRowID).Append(u64ToRowID(entry.ID))
			collector.data.blkMetaInsTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID())
			collector.data.blkMetaInsTxnBatch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID())
			collector.data.blkMetaInsTxnBatch.GetVectorByName(SnapshotAttr_SegID).Append(entry.GetSegment().GetID())
			metaNode.TxnMVCCNode.AppendTuple(collector.data.blkMetaInsTxnBatch)
			collector.data.blkMetaInsTxnBatch.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.MetaLoc))
			collector.data.blkMetaInsTxnBatch.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.DeltaLoc))
		}
	}
	return nil
}

func (collector *IncrementalCollector) OrphanData() *CheckpointData {
	data := collector.data
	collector.data = nil
	return data
}

func (collector *IncrementalCollector) Close() {
	if collector.data != nil {
		collector.data.Close()
	}
}
