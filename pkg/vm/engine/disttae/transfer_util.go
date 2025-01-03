// Copyright 2021-2024 Matrix Origin
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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"
)

type UT_ForceTransCheck struct{}

type TransferOption func(*TransferFlow)

func WithTrasnferBuffer(buffer *containers.OneSchemaBatchBuffer) TransferOption {
	return func(flow *TransferFlow) {
		flow.buffer = buffer
	}
}

func ConstructCNTombstoneObjectsTransferFlow(
	ctx context.Context,
	start, end types.TS,
	table *txnTable,
	txn *Transaction,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (*TransferFlow, []zap.Field, error) {

	//ctx := table.proc.Load().Ctx
	state, err := table.getPartitionState(ctx)
	if err != nil {
		return nil, nil, err
	}

	isObjectDeletedFn := func(objId *objectio.ObjectId) bool {
		return state.CheckIfObjectDeletedBeforeTS(end, false, objId)
	}

	var logs []zap.Field

	newDataObjects, deletedObjects := state.CollectObjectsBetween(start, end)

	logs = append(logs,
		zap.Int("newDataObjects", len(newDataObjects)),
		zap.Int("deletedObjects", len(deletedObjects)))

	if len(newDataObjects) == 0 || len(deletedObjects) == 0 {
		return nil, logs, nil
	}

	deletedObjectsIter := func() *types.Objectid {
		if len(deletedObjects) == 0 {
			return nil
		}

		id := deletedObjects[0].ObjectName().ObjectId()
		deletedObjects = deletedObjects[1:]
		return id
	}

	tombstoneObjects := make([]objectio.ObjectStats, 0)

	for _, e := range txn.writes {
		if e.tableId != table.tableId || e.databaseId != table.db.databaseId {
			continue
		}

		if e.fileName != "" && e.typ == DELETE {
			for i := range e.bat.Vecs[0].Length() {
				stats := objectio.ObjectStats(e.bat.Vecs[0].GetBytesAt(i))
				tombstoneObjects = append(tombstoneObjects, stats)
			}
		}
	}

	logs = append(logs, zap.Int("origin-tombstoneObjects", len(tombstoneObjects)))

	if tombstoneObjects, err = ioutil.CoarseFilterTombstoneObject(
		ctx, deletedObjectsIter, tombstoneObjects, fs); err != nil {
		return nil, logs, err
	} else if len(tombstoneObjects) == 0 {
		return nil, logs, nil
	}

	logs = append(logs, zap.Int("coarse-tombstoneObjects", len(tombstoneObjects)))

	pkColIdx := table.tableDef.Name2ColIndex[table.tableDef.Pkey.PkeyColName]
	pkCol := table.tableDef.Cols[pkColIdx]
	r := engine_util.SimpleMultiObjectsReader(
		ctx, fs,
		tombstoneObjects,
		end.ToTimestamp(),
		engine_util.WithColumns(
			[]uint16{0, 1},
			[]types.Type{types.T_Rowid.ToType(), plan2.ExprType2Type(&pkCol.Typ)},
		),
	)

	return ConstructTransferFlow(
		table,
		objectio.HiddenColumnSelection_None,
		r,
		isObjectDeletedFn,
		newDataObjects,
		mp,
		fs), logs, nil
}

func ConstructTransferFlow(
	table *txnTable,
	hiddenSelection objectio.HiddenColumnSelection,
	sourcer engine.Reader,
	isObjectDeletedFn func(*objectio.ObjectId) bool,
	newDataObjects []objectio.ObjectStats,
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...TransferOption,
) *TransferFlow {
	flow := &TransferFlow{
		table:             table,
		hiddenSelection:   hiddenSelection,
		sourcer:           sourcer,
		isObjectDeletedFn: isObjectDeletedFn,
		newDataObjects:    newDataObjects,
		mp:                mp,
		fs:                fs,
	}
	for _, opt := range opts {
		opt(flow)
	}
	flow.fillDefaults()
	return flow
}

type TransferFlow struct {
	table             *txnTable
	hiddenSelection   objectio.HiddenColumnSelection
	sourcer           engine.Reader
	isObjectDeletedFn func(*objectio.ObjectId) bool
	newDataObjects    []objectio.ObjectStats
	buffer            *containers.OneSchemaBatchBuffer
	staged            *batch.Batch
	sinker            *ioutil.Sinker
	mp                *mpool.MPool
	fs                fileservice.FileService

	transferred struct {
		rowCnt     int
		objDetails map[string]int
	}
}

func (flow *TransferFlow) fillDefaults() {
	pkType := plan2.ExprType2Type(&flow.table.tableDef.Cols[flow.table.primaryIdx].Typ)
	if flow.buffer == nil {
		attrs, attrTypes := objectio.GetTombstoneSchema(
			pkType, flow.hiddenSelection,
		)
		flow.buffer = containers.NewOneSchemaBatchBuffer(
			mpool.MB*8,
			attrs,
			attrTypes,
		)
	}
	if flow.sinker == nil {
		flow.sinker = ioutil.NewTombstoneSinker(
			flow.hiddenSelection,
			pkType,
			flow.mp,
			flow.fs,
			ioutil.WithBuffer(flow.buffer, false),
			ioutil.WithMemorySizeThreshold(mpool.MB*16),
			ioutil.WithTailSizeCap(0),
			//engine_util.WithAllMergeSorted(),
		)
	}

	flow.transferred.objDetails = make(map[string]int)
}

func (flow *TransferFlow) getBuffer() *batch.Batch {
	return flow.buffer.Fetch()
}

func (flow *TransferFlow) putBuffer(bat *batch.Batch) {
	flow.buffer.Putback(bat, flow.mp)
}

func (flow *TransferFlow) Process(ctx context.Context) error {
	for {
		buffer := flow.getBuffer()
		defer flow.putBuffer(buffer)
		done, err := flow.sourcer.Read(ctx, buffer.Attrs, nil, flow.mp, buffer)
		if err != nil {
			return err
		}
		if done {
			break
		}
		if err := flow.processOneBatch(ctx, buffer); err != nil {
			return err
		}
	}

	if err := flow.transferStaged(ctx); err != nil {
		return err
	}

	return flow.sinker.Sync(ctx)
}

func (flow *TransferFlow) getStaged() *batch.Batch {
	if flow.staged == nil {
		flow.staged = flow.getBuffer()
	}
	return flow.staged
}

func (flow *TransferFlow) orphanStaged() {
	flow.putBuffer(flow.staged)
	flow.staged = nil
}

func (flow *TransferFlow) processOneBatch(ctx context.Context, buffer *batch.Batch) error {
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](buffer.Vecs[0])
	var (
		last    *objectio.ObjectId
		deleted bool
	)
	staged := flow.getStaged()
	for i, rowid := range rowids {
		objectid := rowid.BorrowObjectID()
		if last == nil || !objectid.EQ(last) {
			deleted = flow.isObjectDeletedFn(objectid)
			last = objectid
		}
		if !deleted {
			continue
		}
		if err := staged.UnionOne(buffer, int64(i), flow.mp); err != nil {
			return err
		}

		flow.transferred.rowCnt++
		flow.transferred.objDetails[objectid.ShortStringEx()]++

		if staged.Vecs[0].Length() >= objectio.BlockMaxRows {
			if err := flow.transferStaged(ctx); err != nil {
				return err
			}
			staged = flow.getStaged()
		}
	}
	return nil
}

func (flow *TransferFlow) transferStaged(ctx context.Context) error {
	staged := flow.getStaged()
	if staged.RowCount() == 0 {
		return nil
	}

	// sort staged batch by primary key
	// TODO: do not sort if fake pk
	if err := mergeutil.SortColumnsByIndex(
		staged.Vecs,
		1,
		flow.mp,
	); err != nil {
		return err
	}
	result := flow.getBuffer()
	defer flow.putBuffer(result)

	if err := doTransferRowids(
		ctx,
		flow.table,
		flow.newDataObjects,
		staged.Vecs[0], // rowid intents
		result.Vecs[0], // rowid results
		staged.Vecs[1], // pk intents
		result.Vecs[1], // pks results
		flow.mp,
		flow.fs,
	); err != nil {
		return err
	}

	flow.orphanStaged()

	result.SetRowCount(result.Vecs[0].Length())
	return flow.sinker.Write(ctx, result)
}

func (flow *TransferFlow) GetResult() ([]objectio.ObjectStats, []*batch.Batch) {
	return flow.sinker.GetResult()
}

func (flow *TransferFlow) Close() error {
	if flow.sourcer != nil {
		flow.sourcer.Close()
		flow.sourcer = nil
	}
	if flow.sinker != nil {
		flow.sinker.Close()
		flow.sinker = nil
	}
	if flow.buffer != nil {
		flow.buffer.Close(flow.mp)
		flow.buffer = nil
	}
	if flow.staged != nil {
		flow.staged.Clean(flow.mp)
		flow.staged = nil
	}
	flow.mp = nil
	flow.table = nil
	flow.transferred.objDetails = nil
	return nil
}
