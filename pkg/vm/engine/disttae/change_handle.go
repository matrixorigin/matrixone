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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

func (tbl *txnTable) CollectChanges(from, to types.TS) (ChangesHandle, error)

type ChangesHandle interface {
	//两个batch都为空，结束。然后close
	//batch每列的字段
	//    data 用户定义列，ts
	//    tombstone 主键，ts
	Next() (data *batch.Batch, tombstone *batch.Batch, hint logtailreplay.Hint, err error)
	Close() error
}
type CheckpointChangesHandle struct {
	end   types.TS
	table *txnTable
	fs fileservice.FileService
	mp *mpool.MPool
}

func (h *CheckpointChangesHandle) Next() (data *batch.Batch, tombstone *batch.Batch, hint logtailreplay.Hint, err error) {
	hint = logtailreplay.Checkpoint
	return
}
func (h *CheckpointChangesHandle) Close() error {
	return nil
}
func (h *CheckpointChangesHandle) readData(ctx context.Context, table *txnTable) (err error) {
	tblDef:=table.GetTableDef(ctx)
	pkColumName := table.GetTableDef(ctx).Pkey.PkeyColName

	var blockList objectio.BlockInfoSlice
	if _, err = TryFastFilterBlocks(
		ctx,
		table,
		table.db.op.SnapshotTS(),
		table.GetTableDef(ctx),
		nil,
		nil,
		objectList,
		nil,
		&blockList,
		h.fs,
		table.proc.Load(),
	); err != nil {
		return
	}
	relData := NewEmptyBlockListRelationData()
	relData.AppendBlockInfo(objectio.EmptyBlockInfo) // read partition insert
	for i, end := 0, blockList.Len(); i < end; i++ {
		relData.AppendBlockInfo(*blockList.Get(i))
	}

	readers, err := table.BuildReaders(
		ctx,
		table.proc.Load(),
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
	defer func() {
		readers[0].Close()
	}()

	attrs := []string{
		pkColumName,
		catalog.Row_ID,
	}
	buildBatch := func() *batch.Batch {
		bat := batch.NewWithSize(2)
		bat.Attrs = append(bat.Attrs, attrs...)

		bat.Vecs[0] = vector.NewVec(*readPKColumn.GetType())
		bat.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
		return bat
	}
	bat := buildBatch()
	defer func() {
		bat.Clean(h.mp)
	}()
	var isEnd bool
	for {
		bat.CleanOnlyData()
		isEnd, err = readers[0].Read(
			ctx,
			attrs,
			nil,
			h.mp,
			nil,
			bat,
		)
		if err != nil {
			return
		}
		if isEnd {
			break
		}
		if err = vector.GetUnionAllFunction(
			*readPKColumn.GetType(), h.mp,
		)(
			readPKColumn, bat.GetVector(0),
		); err != nil {
			return
		}

		if err = vector.GetUnionAllFunction(
			*targetRowids.GetType(), h.mp,
		)(
			targetRowids, bat.GetVector(1),
		); err != nil {
			return
		}
	}

	return
}

