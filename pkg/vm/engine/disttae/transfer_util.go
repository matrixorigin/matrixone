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
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
)

type TransferOption func(*TransferFlow)

func WithTrasnferBuffer(buffer *containers.OneSchemaBatchBuffer) TransferOption {
	return func(flow *TransferFlow) {
		flow.buffer = buffer
	}
}

func ConstructTransferFlow(
	table *txnTable,
	withHidden bool,
	sourcer engine.Reader,
	isObjectDeletedFn func(*objectio.ObjectId) bool,
	newDataObjects []objectio.ObjectStats,
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...TransferOption,
) *TransferFlow {
	flow := &TransferFlow{
		table:             table,
		withHidden:        withHidden,
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
	withHidden        bool
	sourcer           engine.Reader
	isObjectDeletedFn func(*objectio.ObjectId) bool
	newDataObjects    []objectio.ObjectStats
	buffer            *containers.OneSchemaBatchBuffer
	staged            *batch.Batch
	sinker            *engine_util.Sinker
	mp                *mpool.MPool
	fs                fileservice.FileService
}

func (flow *TransferFlow) fillDefaults() {
	pkType := plan2.ExprType2Type(&flow.table.tableDef.Cols[flow.table.primaryIdx].Typ)
	if flow.buffer == nil {
		attrs, attrTypes := objectio.GetTombstoneSchema(
			pkType, flow.withHidden,
		)
		flow.buffer = containers.NewOneSchemaBatchBuffer(
			mpool.MB*8,
			attrs,
			attrTypes,
		)
	}
	if flow.sinker == nil {
		flow.sinker = engine_util.NewTombstoneSinker(
			flow.withHidden,
			pkType,
			flow.mp,
			flow.fs,
			engine_util.WithBuffer(flow.buffer, false),
			engine_util.WithMemorySizeThreshold(mpool.MB*16),
			// engine_util.WithAllMergeSorted(),
		)
	}
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
	return flow.transferStaged(ctx)
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
		last    objectio.ObjectId
		deleted bool
	)
	staged := flow.getStaged()
	for i, rowid := range rowids {
		objectid := rowid.BorrowObjectID()
		if !objectid.Eq(last) {
			deleted = flow.isObjectDeletedFn(objectid)
			last = *objectid
		}
		if deleted {
			continue
		}
		if err := staged.UnionOne(buffer, int64(i), flow.mp); err != nil {
			return err
		}
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
	if err := mergesort.SortColumnsByIndex(
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
	return nil
}
