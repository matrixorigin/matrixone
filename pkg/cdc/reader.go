// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cdc

import (
	"context"
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ Reader = new(tableReader)

type tableReader struct {
	cnTxnClient  client.TxnClient
	cnEngine     engine.Engine
	mp           *mpool.MPool
	packerPool   *fileservice.Pool[*types.Packer]
	info         *DbTableInfo
	sinker       Sinker
	wMarkUpdater *WatermarkUpdater
	tick         *time.Ticker
	restartFunc  func(*DbTableInfo) error

	tableDef                           *plan.TableDef
	insTsColIdx, insCompositedPkColIdx int
	delTsColIdx, delCompositedPkColIdx int
}

func NewTableReader(
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
	mp *mpool.MPool,
	packerPool *fileservice.Pool[*types.Packer],
	info *DbTableInfo,
	sinker Sinker,
	wMarkUpdater *WatermarkUpdater,
	tableDef *plan.TableDef,
	restartFunc func(*DbTableInfo) error,
) Reader {
	reader := &tableReader{
		cnTxnClient:  cnTxnClient,
		cnEngine:     cnEngine,
		mp:           mp,
		packerPool:   packerPool,
		info:         info,
		sinker:       sinker,
		wMarkUpdater: wMarkUpdater,
		tick:         time.NewTicker(200 * time.Millisecond),
		restartFunc:  restartFunc,
		tableDef:     tableDef,
	}

	// batch columns layout:
	// 1. data: user defined cols | cpk (if needed) | commit-ts
	// 2. tombstone: pk/cpk | commit-ts
	reader.insTsColIdx, reader.insCompositedPkColIdx = len(tableDef.Cols)-1, len(tableDef.Cols)-2
	reader.delTsColIdx, reader.delCompositedPkColIdx = 1, 0
	// if single col pk, there's no additional cpk col
	if len(tableDef.Pkey.Names) == 1 {
		reader.insCompositedPkColIdx = int(tableDef.Name2ColIndex[tableDef.Pkey.Names[0]])
	}
	return reader
}

func (reader *tableReader) Close() {
	reader.sinker.Close()
}

func (reader *tableReader) Run(
	ctx context.Context,
	ar *ActiveRoutine) {
	logutil.Infof("cdc tableReader(%v).Run: start", reader.info)
	defer func() {
		reader.Close()
		logutil.Infof("cdc tableReader(%v).Run: end", reader.info)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ar.Pause:
			return
		case <-ar.Cancel:
			return
		case <-reader.tick.C:
		}

		if err := reader.readTable(ctx, ar); err != nil {
			logutil.Errorf("cdc tableReader(%v) failed, err: %v\n", reader.info, err)

			// if stale read, try to restart reader
			var moErr *moerr.Error
			if errors.As(err, &moErr) && moErr.ErrorCode() == moerr.ErrStaleRead {
				if err = reader.restartFunc(reader.info); err != nil {
					logutil.Errorf("cdc tableReader(%v) restart failed, err: %v\n", reader.info, err)
					return
				}
				continue
			}

			return
		}
	}
}

func (reader *tableReader) readTable(
	ctx context.Context,
	ar *ActiveRoutine) (err error) {

	var txnOp client.TxnOperator
	//step1 : create an txnOp
	txnOp, err = GetTxnOp(ctx, reader.cnEngine, reader.cnTxnClient, "readMultipleTables")
	if err != nil {
		return err
	}
	defer func() {
		FinishTxnOp(ctx, err, txnOp, reader.cnEngine)
	}()
	err = GetTxn(ctx, reader.cnEngine, txnOp)
	if err != nil {
		return err
	}

	var packer *types.Packer
	put := reader.packerPool.Get(&packer)
	defer put.Put()

	//step2 : read table
	err = reader.readTableWithTxn(
		ctx,
		txnOp,
		packer,
		ar)

	return
}

func (reader *tableReader) readTableWithTxn(
	ctx context.Context,
	txnOp client.TxnOperator,
	packer *types.Packer,
	ar *ActiveRoutine) (err error) {
	var rel engine.Relation
	var changes engine.ChangesHandle
	//step1 : get relation
	_, _, rel, err = GetRelationById(ctx, reader.cnEngine, txnOp, reader.info.SourceTblId)
	if err != nil {
		return
	}

	//step2 : define time range
	//	from = last wmark
	//  to = txn operator snapshot ts
	fromTs := reader.wMarkUpdater.GetFromMem(reader.info.SourceTblIdStr)
	toTs := types.TimestampToTS(GetSnapshotTS(txnOp))
	start := time.Now()
	changes, err = CollectChanges(ctx, rel, fromTs, toTs, reader.mp)
	v2.CdcReadDurationHistogram.Observe(time.Since(start).Seconds())
	if err != nil {
		return
	}
	defer changes.Close()

	//step3: pull data
	var insertData, deleteData *batch.Batch
	var insertAtmBatch, deleteAtmBatch *AtomicBatch

	defer func() {
		if insertData != nil {
			insertData.Clean(reader.mp)
		}
		if deleteData != nil {
			deleteData.Clean(reader.mp)
		}
		if insertAtmBatch != nil {
			insertAtmBatch.Close()
		}
		if deleteAtmBatch != nil {
			deleteAtmBatch.Close()
		}
	}()

	allocateAtomicBatchIfNeed := func(atmBatch *AtomicBatch) *AtomicBatch {
		if atmBatch == nil {
			atmBatch = NewAtomicBatch(reader.mp)
		}
		return atmBatch
	}

	addStartMetrics := func() {
		count := float64(batchRowCount(insertData) + batchRowCount(deleteData))
		allocated := float64(insertData.Allocated() + deleteData.Allocated())
		v2.CdcTotalProcessingRecordCountGauge.Add(count)
		v2.CdcTotalAllocatedBatchBytesGauge.Add(allocated)
		v2.CdcReadRecordCounter.Add(count)
	}

	addSnapshotEndMetrics := func() {
		count := float64(batchRowCount(insertData))
		allocated := float64(insertData.Allocated())
		v2.CdcTotalProcessingRecordCountGauge.Sub(count)
		v2.CdcTotalAllocatedBatchBytesGauge.Sub(allocated)
		v2.CdcSinkRecordCounter.Add(count)
	}

	addTailEndMetrics := func(bat *AtomicBatch) {
		count := float64(bat.RowCount())
		allocated := float64(bat.Allocated())
		v2.CdcTotalProcessingRecordCountGauge.Sub(count)
		v2.CdcTotalAllocatedBatchBytesGauge.Sub(allocated)
		v2.CdcSinkRecordCounter.Add(count)
	}

	var curHint engine.ChangesHandle_Hint
	for {
		select {
		case <-ctx.Done():
			return
		case <-ar.Pause:
			return
		case <-ar.Cancel:
			return
		default:
		}

		start = time.Now()
		insertData, deleteData, curHint, err = changes.Next(ctx, reader.mp)
		v2.CdcReadDurationHistogram.Observe(time.Since(start).Seconds())
		if err != nil {
			return
		}

		// both nil denote no more data
		if insertData == nil && deleteData == nil {
			// heartbeat
			err = reader.sinker.Sink(ctx, &DecoderOutput{
				noMoreData: true,
				fromTs:     fromTs,
				toTs:       toTs,
			})
			return
		}

		addStartMetrics()

		switch curHint {
		case engine.ChangesHandle_Snapshot:
			// transform into insert instantly
			err = reader.sinker.Sink(ctx, &DecoderOutput{
				outputTyp:     OutputTypeSnapshot,
				checkpointBat: insertData,
				fromTs:        fromTs,
				toTs:          toTs,
			})
			addSnapshotEndMetrics()
			insertData.Clean(reader.mp)
			if err != nil {
				return
			}
		case engine.ChangesHandle_Tail_wip:
			insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
			deleteAtmBatch = allocateAtomicBatchIfNeed(deleteAtmBatch)
			insertAtmBatch.Append(packer, insertData, reader.insTsColIdx, reader.insCompositedPkColIdx)
			deleteAtmBatch.Append(packer, deleteData, reader.delTsColIdx, reader.delCompositedPkColIdx)
		case engine.ChangesHandle_Tail_done:
			insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
			deleteAtmBatch = allocateAtomicBatchIfNeed(deleteAtmBatch)
			insertAtmBatch.Append(packer, insertData, reader.insTsColIdx, reader.insCompositedPkColIdx)
			deleteAtmBatch.Append(packer, deleteData, reader.delTsColIdx, reader.delCompositedPkColIdx)

			//if !strings.Contains(reader.info.SourceTblName, "order") {
			//	logutil.Errorf("tableReader(%s)[%s, %s], insertAtmBatch: %s, deleteAtmBatch: %s",
			//		reader.info.SourceTblName, fromTs.ToString(), toTs.ToString(),
			//		insertAtmBatch.DebugString(reader.tableDef, false),
			//		deleteAtmBatch.DebugString(reader.tableDef, true))
			//}

			err = reader.sinker.Sink(ctx, &DecoderOutput{
				outputTyp:      OutputTypeTail,
				insertAtmBatch: insertAtmBatch,
				deleteAtmBatch: deleteAtmBatch,
				fromTs:         fromTs,
				toTs:           toTs,
			})
			addTailEndMetrics(insertAtmBatch)
			addTailEndMetrics(deleteAtmBatch)
			insertAtmBatch.Close()
			deleteAtmBatch.Close()
			if err != nil {
				return
			}

			// reset, allocate new when next wip/done
			insertAtmBatch = nil
			deleteAtmBatch = nil
		}
	}
}
