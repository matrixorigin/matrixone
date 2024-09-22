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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
)

type TransferFlow struct {
	table             *txnTable
	sourcer           engine.Reader
	isObjectDeletedFn func(*objectio.ObjectId) bool
	targetObjects     []objectio.ObjectStats
	mp                *mpool.MPool
	fs                fileservice.FileService
	attrs             []string
	types             []types.Type
	batchBuffer       *containers.OneSchemaBatchBuffer
	staged            *batch.Batch
	sinker            *engine_util.Sinker
}

func (flow *TransferFlow) getBuffer() *batch.Batch {
	return flow.batchBuffer.Fetch()
}

func (flow *TransferFlow) putBuffer(bat *batch.Batch) {
	flow.batchBuffer.Putback(bat, flow.mp)
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
		flow.targetObjects,
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

func (flow *TransferFlow) Close() error {
	if flow.sourcer != nil {
		flow.sourcer.Close()
		flow.sourcer = nil
	}
	if flow.sinker != nil {
		flow.sinker.Close()
		flow.sinker = nil
	}
	if flow.batchBuffer != nil {
		flow.batchBuffer.Close(flow.mp)
		flow.batchBuffer = nil
	}
	if flow.staged != nil {
		flow.staged.Clean(flow.mp)
		flow.staged = nil
	}
	flow.mp = nil
	flow.table = nil
	return nil
}
