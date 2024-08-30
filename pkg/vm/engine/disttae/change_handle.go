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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func (tbl *txnTable) CollectChanges(from, to types.TS) (ChangesHandle, error) {
	if from.IsEmpty() {
		return NewCheckpointChangesHandle(to, tbl)
	}
	state, err := tbl.getPartitionState(context.TODO())
	if err != nil {
		return nil, err
	}
	return logtailreplay.NewChangesHandler(state, from, to, tbl.getTxn().engine.mp, 8192, tbl.getTxn().engine.fs), nil
}

type ChangesHandle interface {
	//两个batch都为空，结束。然后close
	//batch每列的字段
	//    data 用户定义列，ts
	//    tombstone 主键，ts
	Next() (data *batch.Batch, tombstone *batch.Batch, hint logtailreplay.Hint, err error)
	Close() error
}
type CheckpointChangesHandle struct {
	end    types.TS
	table  *txnTable
	fs     fileservice.FileService
	mp     *mpool.MPool
	reader engine.Reader
	attrs  []string
}

func NewCheckpointChangesHandle(end types.TS, table *txnTable) (*CheckpointChangesHandle, error) {
	handle := &CheckpointChangesHandle{
		end:   end,
		table: table,
		fs:    table.getTxn().engine.fs,
		mp:    table.getTxn().engine.mp,
	}
	err := handle.initReader()
	return handle, err
}

func (h *CheckpointChangesHandle) Next() (data *batch.Batch, tombstone *batch.Batch, hint logtailreplay.Hint, err error) {
	hint = logtailreplay.Checkpoint

	isEnd, err := h.reader.Read(
		context.TODO(),
		h.attrs,
		nil,
		h.mp,
		nil,
		data,
	)
	if err != nil {
		return
	}
	if isEnd {
		err = moerr.GetOkExpectedEOF()
	}
	return
}
func (h *CheckpointChangesHandle) Close() error {
	h.reader.Close()
	return nil
}
func (h *CheckpointChangesHandle) initReader() (err error) {
	tblDef := h.table.GetTableDef(context.TODO())
	h.attrs = []string{
		catalog.Row_ID,
		catalog2.AttrCommitTs,
	}
	for _, col := range tblDef.Cols {
		h.attrs = append(h.attrs, col.Name)
	}

	var blockList objectio.BlockInfoSlice
	if _, err = TryFastFilterBlocks(
		context.TODO(),
		h.table,
		h.end.ToTimestamp(),
		tblDef,
		nil,
		nil,
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
		context.TODO(),
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
