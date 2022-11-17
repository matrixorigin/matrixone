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
	"sync"

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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/panjf2000/ants/v2"
)

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

type CheckpointMeta struct {
	blkInsertOffset *common.ClosedInterval
	blkDeleteOffset *common.ClosedInterval
}

func NewCheckpointMeta() *CheckpointMeta {
	return &CheckpointMeta{}
}

type CheckpointData struct {
	meta                 map[uint64]*CheckpointMeta
	metaBatch            *containers.Batch
	dbInsBatch           *containers.Batch
	dbInsTxnBatch        *containers.Batch
	dbDelBatch           *containers.Batch
	dbDelTxnBatch        *containers.Batch
	tblInsBatch          *containers.Batch
	tblInsTxnBatch       *containers.Batch
	tblDelBatch          *containers.Batch
	tblDelTxnBatch       *containers.Batch
	tblColInsBatch       *containers.Batch
	tblColDelBatch       *containers.Batch
	segInsBatch          *containers.Batch
	segInsTxnBatch       *containers.Batch
	segDelBatch          *containers.Batch
	segDelTxnBatch       *containers.Batch
	blkMetaInsBatch      *containers.Batch
	blkMetaInsTxnBatch   *containers.Batch
	blkMetaDelBatch      *containers.Batch
	blkMetaDelTxnBatch   *containers.Batch
	blkDNMetaInsBatch    *containers.Batch
	blkDNMetaInsTxnBatch *containers.Batch
	blkDNMetaDelBatch    *containers.Batch
	blkDNMetaDelTxnBatch *containers.Batch
	blkCNMetaInsBatch    *containers.Batch
}

func NewCheckpointData() *CheckpointData {
	return &CheckpointData{
		meta:                 make(map[uint64]*CheckpointMeta),
		metaBatch:            makeRespBatchFromSchema(MetaSchema),
		dbInsBatch:           makeRespBatchFromSchema(catalog.SystemDBSchema),
		dbInsTxnBatch:        makeRespBatchFromSchema(TxnNodeSchema),
		dbDelBatch:           makeRespBatchFromSchema(DelSchema),
		dbDelTxnBatch:        makeRespBatchFromSchema(DBDNSchema),
		tblInsBatch:          makeRespBatchFromSchema(catalog.SystemTableSchema),
		tblInsTxnBatch:       makeRespBatchFromSchema(TblDNSchema),
		tblDelBatch:          makeRespBatchFromSchema(DelSchema),
		tblDelTxnBatch:       makeRespBatchFromSchema(TblDNSchema),
		tblColInsBatch:       makeRespBatchFromSchema(catalog.SystemColumnSchema),
		tblColDelBatch:       makeRespBatchFromSchema(DelSchema),
		segInsBatch:          makeRespBatchFromSchema(SegSchema),
		segInsTxnBatch:       makeRespBatchFromSchema(SegDNSchema),
		segDelBatch:          makeRespBatchFromSchema(DelSchema),
		segDelTxnBatch:       makeRespBatchFromSchema(SegDNSchema),
		blkMetaInsBatch:      makeRespBatchFromSchema(BlkMetaSchema),
		blkMetaInsTxnBatch:   makeRespBatchFromSchema(BlkDNSchema),
		blkMetaDelBatch:      makeRespBatchFromSchema(DelSchema),
		blkMetaDelTxnBatch:   makeRespBatchFromSchema(BlkDNSchema),
		blkDNMetaInsBatch:    makeRespBatchFromSchema(BlkMetaSchema),
		blkDNMetaInsTxnBatch: makeRespBatchFromSchema(BlkDNSchema),
		blkDNMetaDelBatch:    makeRespBatchFromSchema(DelSchema),
		blkDNMetaDelTxnBatch: makeRespBatchFromSchema(BlkDNSchema),
		blkCNMetaInsBatch:    makeRespBatchFromSchema(BlkMetaSchema),
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
	for i := 0; i < data.metaBatch.GetVectorByName(SnapshotMetaAttr_Tid).Length(); i++ {
		tid := data.metaBatch.GetVectorByName(SnapshotMetaAttr_Tid).Get(i).(uint64)
		insStart := data.metaBatch.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).Get(i).(int32)
		insEnd := data.metaBatch.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchEnd).Get(i).(int32)
		delStart := data.metaBatch.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchStart).Get(i).(int32)
		delEnd := data.metaBatch.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchEnd).Get(i).(int32)
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
		insTaeBat = data.dbInsBatch
		delTaeBat = data.dbDelBatch
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
		insTaeBat = data.tblInsBatch
		delTaeBat = data.tblDelBatch
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
		insTaeBat = data.tblColInsBatch
		delTaeBat = data.tblColDelBatch
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
		insTaeBat = data.blkMetaInsBatch.Window(int(insOffset), int(insLength))
	}

	delInterval := meta.blkDeleteOffset
	if delInterval != nil {
		delOffset := delInterval.Start
		delLength := delInterval.End - delInterval.Start
		delTaeBat = data.blkMetaDelBatch.Window(int(delOffset), int(delLength))
		cnInsTaeBat = data.blkCNMetaInsBatch.Window(int(delOffset), int(delLength))
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
	for tid, meta := range data.meta {
		data.metaBatch.GetVectorByName(SnapshotMetaAttr_Tid).Append(tid)
		if meta.blkInsertOffset == nil {
			data.metaBatch.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).Append(int32(-1))
			data.metaBatch.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchEnd).Append(int32(-1))
		} else {
			data.metaBatch.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).Append(int32(meta.blkInsertOffset.Start))
			data.metaBatch.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchEnd).Append(int32(meta.blkInsertOffset.End))
		}
		if meta.blkDeleteOffset == nil {
			data.metaBatch.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchStart).Append(int32(-1))
			data.metaBatch.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchEnd).Append(int32(-1))
		} else {
			data.metaBatch.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchStart).Append(int32(meta.blkDeleteOffset.Start))
			data.metaBatch.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchEnd).Append(int32(meta.blkDeleteOffset.End))
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
	logutil.Info(BatchToString("BLK-META-DEL-BAT", data.blkMetaDelBatch, true))
	logutil.Info(BatchToString("BLK-META-INS-BAT", data.blkMetaInsBatch, true))
}

func (data *CheckpointData) WriteTo(
	writer *blockio.Writer) (blks []objectio.BlockObject, err error) {
	// data.PrintData()
	if _, err = writer.WriteBlock(data.metaBatch); err != nil {
		return
	}
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
	if _, err = writer.WriteBlock(data.blkDNMetaInsBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.blkDNMetaInsTxnBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.blkDNMetaDelBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.blkDNMetaDelTxnBatch); err != nil {
		return
	}
	if _, err = writer.WriteBlock(data.blkCNMetaInsBatch); err != nil {
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

	wg := new(sync.WaitGroup)

	errchan := make(chan error, 30)
	pool, err := ants.NewPool(200)
	if err != nil {
		return
	}
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.metaBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, MetaSchema.Types()...),
			append(BaseAttr, MetaSchema.AllNames()...),
			append([]bool{false, false}, catalog.SystemDBSchema.AllNullables()...),
			metas[0]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.dbInsBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, catalog.SystemDBSchema.Types()...),
			append(BaseAttr, catalog.SystemDBSchema.AllNames()...),
			append([]bool{false, false}, catalog.SystemDBSchema.AllNullables()...),
			metas[1]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.dbInsTxnBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, TxnNodeSchema.Types()...),
			append(BaseAttr, TxnNodeSchema.AllNames()...),
			append([]bool{false, false}, TxnNodeSchema.AllNullables()...),
			metas[2]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.dbDelBatch, err = reader.LoadBlkColumnsByMeta(
			BaseTypes,
			BaseAttr,
			[]bool{false, false},
			metas[3]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.dbDelTxnBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, DBDNSchema.Types()...),
			append(BaseAttr, DBDNSchema.AllNames()...),
			append([]bool{false, false}, DBDNSchema.AllNullables()...),
			metas[4]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.tblInsBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, catalog.SystemTableSchema.Types()...),
			append(BaseAttr, catalog.SystemTableSchema.AllNames()...),
			append([]bool{false, false}, catalog.SystemTableSchema.AllNullables()...),
			metas[5]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.tblInsTxnBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, TblDNSchema.Types()...),
			append(BaseAttr, TblDNSchema.AllNames()...),
			append([]bool{false, false}, TblDNSchema.AllNullables()...),
			metas[6]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.tblDelBatch, err = reader.LoadBlkColumnsByMeta(
			BaseTypes,
			BaseAttr,
			[]bool{false, false},
			metas[7]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.tblDelTxnBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, TblDNSchema.Types()...),
			append(BaseAttr, TblDNSchema.AllNames()...),
			append([]bool{false, false}, TblDNSchema.AllNullables()...),
			metas[8]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.tblColInsBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, catalog.SystemColumnSchema.Types()...),
			append(BaseAttr, catalog.SystemColumnSchema.AllNames()...),
			append([]bool{false, false}, catalog.SystemColumnSchema.AllNullables()...),
			metas[9]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.tblColDelBatch, err = reader.LoadBlkColumnsByMeta(
			BaseTypes,
			BaseAttr,
			[]bool{false, false},
			metas[10]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.segInsBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, SegSchema.Types()...),
			append(BaseAttr, SegSchema.AllNames()...),
			append([]bool{false, false}, SegSchema.AllNullables()...),
			metas[11]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.segInsTxnBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, SegDNSchema.Types()...),
			append(BaseAttr, SegDNSchema.AllNames()...),
			append([]bool{false, false}, SegDNSchema.AllNullables()...),
			metas[12]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.segDelBatch, err = reader.LoadBlkColumnsByMeta(
			BaseTypes,
			BaseAttr,
			[]bool{false, false},
			metas[13]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.segDelTxnBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, SegDNSchema.Types()...),
			append(BaseAttr, SegDNSchema.AllNames()...),
			append([]bool{false, false}, SegDNSchema.AllNullables()...),
			metas[14]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.blkMetaInsBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, BlkMetaSchema.Types()...),
			append(BaseAttr, BlkMetaSchema.AllNames()...),
			append([]bool{false, false}, BlkMetaSchema.AllNullables()...),
			metas[15]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.blkMetaInsTxnBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, BlkDNSchema.Types()...),
			append(BaseAttr, BlkDNSchema.AllNames()...),
			append([]bool{false, false}, BlkDNSchema.AllNullables()...),
			metas[16]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.blkMetaDelBatch, err = reader.LoadBlkColumnsByMeta(
			BaseTypes,
			BaseAttr,
			[]bool{false, false},
			metas[17]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.blkMetaDelTxnBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, BlkDNSchema.Types()...),
			append(BaseAttr, BlkDNSchema.AllNames()...),
			append([]bool{false, false}, BlkDNSchema.AllNullables()...),
			metas[18]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.blkDNMetaInsBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, BlkMetaSchema.Types()...),
			append(BaseAttr, BlkMetaSchema.AllNames()...),
			append([]bool{false, false}, BlkMetaSchema.AllNullables()...),
			metas[19]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.blkDNMetaInsTxnBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, BlkDNSchema.Types()...),
			append(BaseAttr, BlkDNSchema.AllNames()...),
			append([]bool{false, false}, BlkDNSchema.AllNullables()...),
			metas[20]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.blkDNMetaDelBatch, err = reader.LoadBlkColumnsByMeta(
			BaseTypes,
			BaseAttr,
			[]bool{false, false},
			metas[21]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.blkDNMetaDelTxnBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, BlkDNSchema.Types()...),
			append(BaseAttr, BlkDNSchema.AllNames()...),
			append([]bool{false, false}, BlkDNSchema.AllNullables()...),
			metas[22]); err != nil {
			errchan <- err
		}
	})
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		var err error
		if data.blkCNMetaInsBatch, err = reader.LoadBlkColumnsByMeta(
			append(BaseTypes, BlkMetaSchema.Types()...),
			append(BaseAttr, BlkMetaSchema.AllNames()...),
			append([]bool{false, false}, BlkMetaSchema.AllNullables()...),
			metas[23]); err != nil {
			errchan <- err
		}
	})

	go func() {
		wg.Wait()
		errchan <- nil
	}()

	err = <-errchan
	if err != nil {
		data.Close()
	}
	pool.Release()
	return
}

func (data *CheckpointData) Close() {
	if data.metaBatch != nil {
		data.metaBatch.Close()
		data.metaBatch = nil
	}
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
	if data.blkDNMetaInsBatch != nil {
		data.blkDNMetaInsBatch.Close()
		data.blkDNMetaInsBatch = nil
	}
	if data.blkDNMetaInsTxnBatch != nil {
		data.blkDNMetaInsTxnBatch.Close()
		data.blkDNMetaInsTxnBatch = nil
	}
	if data.blkDNMetaDelBatch != nil {
		data.blkDNMetaDelBatch.Close()
		data.blkDNMetaDelBatch = nil
	}
	if data.blkDNMetaDelTxnBatch != nil {
		data.blkDNMetaDelTxnBatch.Close()
		data.blkDNMetaDelTxnBatch = nil
	}
	if data.blkCNMetaInsBatch != nil {
		data.blkCNMetaInsBatch.Close()
		data.blkCNMetaInsBatch = nil
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
func (data *CheckpointData) GetDNBlkBatchs() (*containers.Batch, *containers.Batch, *containers.Batch, *containers.Batch) {
	return data.blkDNMetaInsBatch, data.blkDNMetaInsTxnBatch, data.blkDNMetaDelBatch, data.blkDNMetaDelTxnBatch
}

func (collector *IncrementalCollector) VisitDB(entry *catalog.DBEntry) error {
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
		tblNode := node.(*catalog.TableMVCCNode)
		if !tblNode.HasDropCommitted() {
			for _, syscol := range catalog.SystemColumnSchema.ColDefs {
				txnimpl.FillColumnRow(entry, syscol.Name, collector.data.tblColInsBatch.GetVectorByName(syscol.Name))
			}
			rowidVec := collector.data.tblColInsBatch.GetVectorByName(catalog.AttrRowID)
			commitVec := collector.data.tblColInsBatch.GetVectorByName(catalog.AttrCommitTs)
			for _, usercol := range entry.GetSchema().ColDefs {
				rowidVec.Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name))))
				commitVec.Append(tblNode.GetEnd())
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
	if len(mvccNodes) == 0 {
		return nil
	}
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
	if len(mvccNodes) == 0 {
		return nil
	}
	insStart := collector.data.blkMetaInsBatch.GetVectorByName(catalog.AttrRowID).Length()
	delStart := collector.data.blkMetaDelBatch.GetVectorByName(catalog.AttrRowID).Length()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		metaNode := node.(*catalog.MetadataMVCCNode)
		if metaNode.MetaLoc == "" || metaNode.Aborted {
			if metaNode.HasDropCommitted() {
				collector.data.blkDNMetaDelBatch.GetVectorByName(catalog.AttrRowID).Append(u64ToRowID(entry.ID))
				collector.data.blkDNMetaDelBatch.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.GetEnd())
				collector.data.blkDNMetaDelTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID())
				collector.data.blkDNMetaDelTxnBatch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID())
				collector.data.blkDNMetaDelTxnBatch.GetVectorByName(SnapshotAttr_SegID).Append(entry.GetSegment().GetID())
				metaNode.TxnMVCCNode.AppendTuple(collector.data.blkDNMetaDelTxnBatch)
				collector.data.blkDNMetaDelTxnBatch.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.MetaLoc))
				collector.data.blkDNMetaDelTxnBatch.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.DeltaLoc))
			} else {
				collector.data.blkDNMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_ID).Append(entry.ID)
				collector.data.blkDNMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(entry.IsAppendable())
				collector.data.blkDNMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.MetaLoc))
				collector.data.blkDNMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.DeltaLoc))
				collector.data.blkDNMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd())
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasPK() {
					is_sorted = true
				}
				collector.data.blkDNMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted)
				collector.data.blkDNMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(entry.GetSegment().ID)
				collector.data.blkDNMetaInsBatch.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.CreatedAt)
				collector.data.blkDNMetaInsBatch.GetVectorByName(catalog.AttrRowID).Append(u64ToRowID(entry.ID))
				collector.data.blkDNMetaInsTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID())
				collector.data.blkDNMetaInsTxnBatch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID())
				collector.data.blkDNMetaInsTxnBatch.GetVectorByName(SnapshotAttr_SegID).Append(entry.GetSegment().GetID())
				metaNode.TxnMVCCNode.AppendTuple(collector.data.blkDNMetaInsTxnBatch)
				collector.data.blkDNMetaInsTxnBatch.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.MetaLoc))
				collector.data.blkDNMetaInsTxnBatch.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.DeltaLoc))
			}
		} else {
			if metaNode.HasDropCommitted() {
				collector.data.blkMetaDelBatch.GetVectorByName(catalog.AttrRowID).Append(u64ToRowID(entry.ID))
				collector.data.blkMetaDelBatch.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.GetEnd())
				collector.data.blkMetaDelTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID())
				collector.data.blkMetaDelTxnBatch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID())
				collector.data.blkMetaDelTxnBatch.GetVectorByName(SnapshotAttr_SegID).Append(entry.GetSegment().GetID())
				metaNode.TxnMVCCNode.AppendTuple(collector.data.blkMetaDelTxnBatch)
				collector.data.blkMetaDelTxnBatch.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.MetaLoc))
				collector.data.blkMetaDelTxnBatch.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.DeltaLoc))

				collector.data.blkCNMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_ID).Append(entry.ID)
				collector.data.blkCNMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(entry.IsAppendable())
				collector.data.blkCNMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.MetaLoc))
				collector.data.blkCNMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.DeltaLoc))
				collector.data.blkCNMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd())
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasPK() {
					is_sorted = true
				}
				collector.data.blkCNMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted)
				collector.data.blkCNMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(entry.GetSegment().ID)
				collector.data.blkCNMetaInsBatch.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.CreatedAt)
				collector.data.blkCNMetaInsBatch.GetVectorByName(catalog.AttrRowID).Append(u64ToRowID(entry.ID))
			} else {
				collector.data.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_ID).Append(entry.ID)
				collector.data.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(entry.IsAppendable())
				collector.data.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.MetaLoc))
				collector.data.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.DeltaLoc))
				collector.data.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd())
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasPK() {
					is_sorted = true
				}
				collector.data.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted)
				collector.data.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(entry.GetSegment().ID)
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
	}
	insEnd := collector.data.blkMetaInsBatch.GetVectorByName(catalog.AttrRowID).Length()
	delEnd := collector.data.blkMetaDelBatch.GetVectorByName(catalog.AttrRowID).Length()
	collector.data.UpdateBlkMeta(entry.GetSegment().GetTable().ID, int32(insStart), int32(insEnd), int32(delStart), int32(delEnd))
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
