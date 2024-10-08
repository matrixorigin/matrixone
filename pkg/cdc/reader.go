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

func (reader *tableReader) Close() {}

func (reader *tableReader) Run(
	ctx context.Context,
	ar *ActiveRoutine) {
	logutil.Infof("^^^^^ tableReader(%s).Run: start", reader.info.SourceTblName)
	defer func() {
		logutil.Infof("^^^^^ tableReader(%s).Run: end", reader.info.SourceTblName)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ar.Cancel:
			return
		case <-reader.tick.C:
		}

		if err := reader.readTable(ctx, ar); err != nil {
			logutil.Errorf("reader %v failed err:%v", reader.info, err)

			// if stale read, restart reader
			var moErr *moerr.Error
			if errors.As(err, &moErr) && moErr.ErrorCode() == moerr.ErrStaleRead {
				if err = reader.restartFunc(reader.info); err != nil {
					logutil.Errorf("reader %v restart failed, err:%v", reader.info, err)
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
	fromTs := reader.wMarkUpdater.GetFromMem(reader.info.SourceTblId)
	toTs := types.TimestampToTS(GetSnapshotTS(txnOp))
	changes, err = CollectChanges(ctx, rel, fromTs, toTs, reader.mp)
	if err != nil {
		return
	}
	defer changes.Close()

	//step3: pull data
	var insertData, deleteData *batch.Batch
	var insertAtmBatch, deleteAtmBatch *AtomicBatch

	allocateAtomicBatchIfNeed := func(atmBatch *AtomicBatch) *AtomicBatch {
		if atmBatch == nil {
			atmBatch = NewAtomicBatch(
				reader.mp,
				fromTs, toTs,
			)
		}
		return atmBatch
	}

	recordCount := func(bat *batch.Batch) int {
		if bat == nil || len(bat.Vecs) == 0 {
			return 0
		}
		return bat.Vecs[0].Length()
	}

	var curHint engine.ChangesHandle_Hint
	for {
		select {
		case <-ctx.Done():
			return
		case <-ar.Cancel:
			return
		default:
		}

		if insertData, deleteData, curHint, err = changes.Next(ctx, reader.mp); err != nil {
			return
		}

		//both nil denote no more data
		if insertData == nil && deleteData == nil {
			// heartbeat
			err = reader.sinker.Sink(ctx, &DecoderOutput{
				noMoreData: true,
				fromTs:     fromTs,
				toTs:       toTs,
			})
			return
		}

		count := float64(recordCount(insertData) + recordCount(deleteData))
		allocated := float64(insertData.Allocated() + deleteData.Allocated())
		v2.CdcTotalProcessingRecordCountGauge.Add(count)
		v2.CdcTotalAllocatedBatchBytesGauge.Add(allocated)
		v2.CdcReadRecordCounter.Add(count)

		switch curHint {
		case engine.ChangesHandle_Snapshot:
			// transform into insert instantly
			err = reader.sinker.Sink(ctx, &DecoderOutput{
				outputTyp:     OutputTypeCheckpoint,
				checkpointBat: insertData,
				fromTs:        fromTs,
				toTs:          toTs,
			})
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
			err = reader.sinker.Sink(ctx, &DecoderOutput{
				outputTyp:      OutputTypeTailDone,
				insertAtmBatch: insertAtmBatch,
				deleteAtmBatch: deleteAtmBatch,
				fromTs:         fromTs,
				toTs:           toTs,
			})
			if err != nil {
				return
			}

			// reset, allocate new when next wip/done
			insertAtmBatch = nil
			deleteAtmBatch = nil
		}
	}
}
