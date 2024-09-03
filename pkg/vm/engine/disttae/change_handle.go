// Copyright 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func (tbl *txnTable) CollectChanges(ctx context.Context, from, to types.TS, mp *mpool.MPool) (engine.ChangesHandle, error) {
	if from.IsEmpty() {
		return NewCheckpointChangesHandle(to, tbl, mp, ctx)
	}
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}
	return logtailreplay.NewChangesHandler(state, from, to, mp, 8192, tbl.getTxn().engine.fs, ctx), nil
}

type ChangesHandle interface {
	Next(mp *mpool.MPool, ctx context.Context) (data *batch.Batch, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error)
	Close() error
}
type CheckpointChangesHandle struct {
	end    types.TS
	table  *txnTable
	fs     fileservice.FileService
	reader engine.Reader
	attrs  []string
	isEnd  bool
}

func NewCheckpointChangesHandle(end types.TS, table *txnTable, mp *mpool.MPool, ctx context.Context) (*CheckpointChangesHandle, error) {
	handle := &CheckpointChangesHandle{
		end:   end,
		table: table,
		fs:    table.getTxn().engine.fs,
	}
	err := handle.initReader(ctx)
	return handle, err
}

func (h *CheckpointChangesHandle) Next(ctx context.Context, mp *mpool.MPool) (data *batch.Batch, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	select {
	case <-ctx.Done():
		return
	default:
	}
	hint = engine.ChangesHandle_Snapshot
	if h.isEnd {
		return nil, nil, hint, nil
	}
	tblDef := h.table.GetTableDef(ctx)

	buildBatch := func() *batch.Batch {
		bat := batch.NewWithSize(len(tblDef.Cols))
		for i, col := range tblDef.Cols {
			bat.Attrs = append(bat.Attrs, col.Name)
			typ := types.New(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale)
			bat.Vecs[i] = vector.NewVec(typ)
		}
		return bat
	}
	data = buildBatch()
	h.isEnd, err = h.reader.Read(
		ctx,
		h.attrs,
		nil,
		mp,
		nil,
		data,
	)
	if h.isEnd {
		return nil, nil, hint, nil
	}
	if err != nil {
		return
	}

	committs, err := vector.NewConstFixed(types.T_TS.ToType(), h.end, data.Vecs[0].Length(), mp)
	if err != nil {
		data.Clean(mp)
		return
	}
	rowidVec := data.Vecs[len(data.Vecs)-1]
	rowidVec.Free(mp)
	data.Vecs[len(data.Vecs)-1] = committs
	data.Attrs[len(data.Attrs)-1] = catalog.AttrCommitTs
	return
}
func (h *CheckpointChangesHandle) Close() error {
	h.reader.Close()
	return nil
}
func (h *CheckpointChangesHandle) initReader(ctx context.Context) (err error) {
	tblDef := h.table.GetTableDef(ctx)
	h.attrs = make([]string, 0)
	for _, col := range tblDef.Cols {
		h.attrs = append(h.attrs, col.Name)
	}

	var part *logtailreplay.PartitionState
	if part, err = h.table.getPartitionState(ctx); err != nil {
		return
	}

	var blockList objectio.BlockInfoSlice
	if _, err = TryFastFilterBlocks(
		ctx,
		h.table,
		h.end.ToTimestamp(),
		tblDef,
		nil,
		part,
		nil,
		nil,
		&blockList,
		h.fs,
		h.table.proc.Load(),
	); err != nil {
		return
	}
	relData := NewEmptyBlockListRelationData()
	relData.AppendBlockInfo(objectio.EmptyBlockInfo) // read partition insert
	for i, end := 0, blockList.Len(); i < end; i++ {
		relData.AppendBlockInfo(*blockList.Get(i))
	}

	readers, err := h.table.BuildReaders(
		ctx,
		h.table.proc.Load(),
		nil,
		relData,
		1,
		0,
		false,
		engine.Policy_CheckCommittedOnly,
	)
	if err != nil {
		return
	}
	h.reader = readers[0]

	return
}
