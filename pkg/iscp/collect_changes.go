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

package iscp

import (
	"context"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

type DataRetrieverConsumer interface {
	DataRetriever
	SetNextBatch(*ISCPData)
	SetError(error)
	Close()
}

func CollectChangesForIteration(
	ctx context.Context,
	iter *Iteration,
	rel engine.Relation,
	fromTs types.TS,
	toTs types.TS,
	consumers []*JobEntry,
	initSnapshotSplitTxn bool,
	packer *types.Packer,
	mp *mpool.MPool,
) (errs []error) {
	errs = make([]error, len(consumers))
	changes, err := CollectChanges(ctx, rel, fromTs, toTs, mp)
	if msg, injected := objectio.ISCPExecutorInjected(); injected && msg == "collectChanges" {
		err = moerr.NewInternalErrorNoCtx(msg)
	}
	if err != nil {
		for i := range consumers {
			errs[i] = err
		}
		return
	}
	defer changes.Close()

	tableDef := rel.CopyTableDef(ctx)
	insTSColIdx := len(tableDef.Cols) - 1
	insCompositedPkColIdx := len(tableDef.Cols) - 2
	delTSColIdx := 1
	delCompositedPkColIdx := 0
	if len(tableDef.Pkey.Names) == 1 {
		insCompositedPkColIdx = int(tableDef.Name2ColIndex[tableDef.Pkey.Names[0]])
	}

	allocateAtomicBatchIfNeed := func(atomicBatch *AtomicBatch) *AtomicBatch {
		if atomicBatch == nil {
			atomicBatch = NewAtomicBatch(mp)
		}
		return atomicBatch
	}

	dataRetrievers := make([]DataRetrieverConsumer, len(consumers))
	typ := ISCPDataType_Tail
	if fromTs.IsEmpty() {
		typ = ISCPDataType_Snapshot
	}
	waitGroups := make([]sync.WaitGroup, len(consumers))
	for i, consumer := range consumers {
		dataRetrievers[i] = NewDataRetriever(consumer, iter, typ)
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)
	changeHandelWg := sync.WaitGroup{}
	go func() {
		defer cancel()
		defer changeHandelWg.Done()
		changeHandelWg.Add(1)
		for {
			select {
			case <-ctxWithCancel.Done():
				return
			default:
			}
			var data *ISCPData
			insertData, deleteData, currentHint, err := changes.Next(ctxWithCancel, mp)
			if msg, injected := objectio.ISCPExecutorInjected(); injected && msg == "changesNext" {
				err = moerr.NewInternalErrorNoCtx(msg)
			}
			if err != nil {
				jobNames := ""
				for _, sinker := range iter.jobs {
					jobNames = fmt.Sprintf("%s%s, ", jobNames, sinker.jobName)
				}
				logutil.Error(
					"ISCP-Task sink iteration failed",
					zap.Uint32("tenantID", iter.table.accountID),
					zap.Uint64("tableID", iter.table.tableID),
					zap.String("jobName", jobNames),
					zap.Error(err),
					zap.String("from", iter.from.ToString()),
					zap.String("to", iter.to.ToString()),
				)
				data = NewISCPData(true, nil, nil, err)
			} else {
				// both nil denote no more data (end of this tail)
				if insertData == nil && deleteData == nil {
					data = NewISCPData(true, nil, nil, err)
				} else {
					var insertAtmBatch *AtomicBatch
					var deleteAtmBatch *AtomicBatch
					switch currentHint {
					case engine.ChangesHandle_Snapshot:
						if typ != ISCPDataType_Snapshot {
							panic("logic error")
						}
						insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
						insertAtmBatch.Append(packer, insertData, insTSColIdx, insCompositedPkColIdx)
						data = NewISCPData(false, insertAtmBatch, nil, nil)
					case engine.ChangesHandle_Tail_wip:
						panic("logic error")
					case engine.ChangesHandle_Tail_done:
						if typ != ISCPDataType_Tail {
							panic("logic error")
						}
						insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
						deleteAtmBatch = allocateAtomicBatchIfNeed(deleteAtmBatch)
						insertAtmBatch.Append(packer, insertData, insTSColIdx, insCompositedPkColIdx)
						deleteAtmBatch.Append(packer, deleteData, delTSColIdx, delCompositedPkColIdx)
						data = NewISCPData(false, insertAtmBatch, deleteAtmBatch, nil)

					}
				}
			}

			noMoreData := data.noMoreData
			data.Set(len(consumers))
			for i := range consumers {
				dataRetrievers[i].SetNextBatch(data)
			}

			if noMoreData {
				return
			}
		}
	}()

	for i, consumerEntry := range consumers {
		if dataRetrievers[i] == nil {
			continue
		}
		waitGroups[i].Add(1)
		go func(i int) {
			defer waitGroups[i].Done()
			err := consumerEntry.consumer.Consume(context.Background(), dataRetrievers[i])
			if err != nil {
				logutil.Error(
					"ISCP-Task sink consume failed",
					zap.Uint32("tenantID", iter.table.accountID),
					zap.Uint64("tableID", iter.table.tableID),
					zap.String("jobName", iter.jobs[i].jobName),
					zap.Error(err),
					zap.String("from", iter.from.ToString()),
					zap.String("to", iter.to.ToString()),
				)
				dataRetrievers[i].SetError(err)
				errs[i] = err
			}
		}(i)
	}
	for i := range waitGroups {
		waitGroups[i].Wait()
	}

	cancel()
	changeHandelWg.Wait()

	for _, dataRetriever := range dataRetrievers {
		dataRetriever.Close()
	}

	return
}
