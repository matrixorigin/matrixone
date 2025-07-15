// Copyright 2021 Matrix Origin
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

package idxcdc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

func CollectChanges_2(
	ctx context.Context,
	iter *Iteration,
	rel engine.Relation,
	fromTs types.TS,
	toTs types.TS,
	consumers []*SinkerEntry,
	initSnapshotSplitTxn bool,
	packer *types.Packer,
	mp *mpool.MPool,
) (errs []error) {
	errs = make([]error, len(consumers))
	changes, err := CollectChanges(ctx, rel, fromTs, toTs, mp)
	if msg, injected := objectio.CDCExecutorInjected(); injected && msg == "collectChanges" {
		err = errors.New(msg)
	}
	if err != nil {
		for i := range consumers {
			errs[i] = err
		}
		return
	}
	defer changes.Close()
	//step3: pull data
	var insertData, deleteData *batch.Batch
	var insertAtmBatch, deleteAtmBatch *AtomicBatch
	var preinsertAtmBatch, predeleteAtmBatch *AtomicBatch

	tableDef := rel.CopyTableDef(ctx)
	insTSColIdx := len(tableDef.Cols) - 1
	insCompositedPkColIdx := len(tableDef.Cols) - 2
	delTSColIdx := 1
	delCompositedPkColIdx := 0
	if len(tableDef.Pkey.Names) == 1 {
		insCompositedPkColIdx = int(tableDef.Name2ColIndex[tableDef.Pkey.Names[0]])
	}

	defer func() {
		if insertData != nil {
			insertData.Clean(mp)
		}
		if deleteData != nil {
			deleteData.Clean(mp)
		}
		if insertAtmBatch != nil {
			insertAtmBatch.Close()
		}
		if deleteAtmBatch != nil {
			deleteAtmBatch.Close()
		}
	}()

	allocateAtomicBatchIfNeed := func(atomicBatch *AtomicBatch) *AtomicBatch {
		if atomicBatch == nil {
			atomicBatch = NewAtomicBatch(mp)
		}
		return atomicBatch
	}

	dataRetrievers := make([]DataRetriever, len(consumers))
	insertDataChs := make([]chan *CDCData, len(consumers))
	ackChs := make([]chan struct{}, len(consumers))
	typ := CDCDataType_Tail
	if fromTs.IsEmpty() {
		typ = CDCDataType_Snapshot
	}
	txns := make([]client.TxnOperator, len(consumers))
	waitGroups := make([]sync.WaitGroup, len(consumers))
	for i, consumer := range consumers {
		insertDataChs[i] = make(chan *CDCData, 1)
		ackChs[i] = make(chan struct{}, 1)
		txns[i], err = iter.table.exec.txnFactory()
		if msg, injected := objectio.CDCExecutorInjected(); injected && msg == "collectChangesCreateTxn" {
			err = errors.New(msg)
		}
		if msg, injected := objectio.CDCExecutorInjected(); injected && msg == "collectChangesCreateTxn, firstTxn" && i == 0 {
			err = errors.New(msg)
		}
		if err != nil {
			close(insertDataChs[i])
			close(ackChs[i])
			errs[i] = err
			continue
		}
		dataRetrievers[i] = NewDataRetriever(consumer, iter, txns[i], insertDataChs[i], ackChs[i], typ)
	}

	go func() {
		for {
			var data *CDCData
			insertData, deleteData, currentHint, err := changes.Next(ctx, mp)
			if msg, injected := objectio.CDCExecutorInjected(); injected && msg == "changesNext" {
				err = errors.New(msg)
			}
			if err != nil {
				indexNames := ""
				for _, sinker := range iter.sinkers {
					indexNames = fmt.Sprintf("%s%s, ", indexNames, sinker.indexName)
				}
				logutil.Error(
					"Async-Index-CDC-Task sink iteration failed",
					zap.Uint32("tenantID", iter.table.accountID),
					zap.Uint64("tableID", iter.table.tableID),
					zap.String("indexName", indexNames),
					zap.Error(err),
					zap.String("from", iter.from.ToString()),
					zap.String("to", iter.to.ToString()),
				)
				data = &CDCData{
					noMoreData:  true,
					insertBatch: nil,
					deleteBatch: nil,
					err:         err,
				}
			} else {
				// both nil denote no more data (end of this tail)
				if insertData == nil && deleteData == nil {
					data = &CDCData{
						noMoreData:  true,
						insertBatch: nil,
						deleteBatch: nil,
					}
				} else {
					switch currentHint {
					case engine.ChangesHandle_Snapshot:
						if typ != CDCDataType_Snapshot {
							panic("logic error")
						}
						insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
						insertAtmBatch.Append(packer, insertData, insTSColIdx, insCompositedPkColIdx)
						data = &CDCData{
							noMoreData:  false,
							insertBatch: insertAtmBatch,
							deleteBatch: nil,
						}
					case engine.ChangesHandle_Tail_wip:
						panic("logic error")
					case engine.ChangesHandle_Tail_done:
						if typ != CDCDataType_Tail {
							panic("logic error")
						}
						insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
						deleteAtmBatch = allocateAtomicBatchIfNeed(deleteAtmBatch)
						insertAtmBatch.Append(packer, insertData, insTSColIdx, insCompositedPkColIdx)
						deleteAtmBatch.Append(packer, deleteData, delTSColIdx, delCompositedPkColIdx)
						data = &CDCData{
							noMoreData:  false,
							insertBatch: insertAtmBatch,
							deleteBatch: deleteAtmBatch,
						}

						addTailEndMetrics(insertAtmBatch)
						addTailEndMetrics(deleteAtmBatch)
					}
				}
			}

			for i := range consumers {
				if dataRetrievers[i] == nil {
					continue
				}
				insertDataChs[i] <- data
			}
			for i := range consumers {
				if dataRetrievers[i] == nil {
					continue
				}
				<-ackChs[i]
			}
			if preinsertAtmBatch != nil {
				preinsertAtmBatch.Close()
				preinsertAtmBatch = nil
			}
			if predeleteAtmBatch != nil {
				predeleteAtmBatch.Close()
				predeleteAtmBatch = nil
			}
			if insertAtmBatch != nil {
				preinsertAtmBatch = insertAtmBatch
				insertAtmBatch = nil
			}
			if deleteAtmBatch != nil {
				predeleteAtmBatch = deleteAtmBatch
				deleteAtmBatch = nil
			}

			if data.noMoreData {
				return
			}
		}
	}()

	for i, consumerEntry := range consumers {
		waitGroups[i].Add(1)
		go func(i int) {
			defer waitGroups[i].Done()
			err := consumerEntry.consumer.Consume(ctx, dataRetrievers[i])
			if msg, injected := objectio.CDCExecutorInjected(); injected && msg == "consume" {
				err = errors.New(msg)
			}
			if msg, injected := objectio.CDCExecutorInjected(); injected && msg == "consume, firstTxn" && i == 0 {
				err = errors.New(msg)
			}
			if err != nil {
				logutil.Error(
					"Async-Index-CDC-Task sink consume failed",
					zap.Uint32("tenantID", iter.table.accountID),
					zap.Uint64("tableID", iter.table.tableID),
					zap.String("indexName", iter.sinkers[i].indexName),
					zap.Error(err),
					zap.String("from", iter.from.ToString()),
					zap.String("to", iter.to.ToString()),
				)
				if txns[i] != nil {
					txns[i].Rollback(ctx)
					txns[i] = nil
				}
				close(insertDataChs[i])
				close(ackChs[i])
				ackChs[i] = nil
				insertDataChs[i] = nil
				dataRetrievers[i] = nil
				errs[i] = err
			}
		}(i)
	}
	for i := range waitGroups {
		waitGroups[i].Wait()
	}

	for i, txn := range txns {
		if txn != nil {
			close(insertDataChs[i])
			close(ackChs[i])
			err := txn.Commit(ctx)
			if err != nil {
				errs[i] = err
			}
		}
	}

	return
}
