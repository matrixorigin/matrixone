// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

type coverageCheckpointReader struct {
	closed bool
}

func (r *coverageCheckpointReader) Close() error {
	r.closed = true
	return nil
}

func (r *coverageCheckpointReader) Read(
	_ context.Context,
	_ []string,
	_ *plan.Expr,
	mp *mpool.MPool,
	bat *batch.Batch,
) (bool, error) {
	var blockID types.Blockid
	for _, vec := range bat.Vecs {
		switch vec.GetType().Oid {
		case types.T_Rowid:
			if err := vector.AppendFixed(vec, types.NewRowid(&blockID, 0), false, mp); err != nil {
				return false, err
			}
		case types.T_int64:
			if err := vector.AppendFixed(vec, int64(7), false, mp); err != nil {
				return false, err
			}
		}
	}
	bat.SetRowCount(1)
	return false, nil
}

func (r *coverageCheckpointReader) SetOrderBy([]*plan.OrderBySpec)       {}
func (r *coverageCheckpointReader) GetOrderBy() []*plan.OrderBySpec      { return nil }
func (r *coverageCheckpointReader) SetIndexParam(*plan.IndexReaderParam) {}
func (r *coverageCheckpointReader) SetFilterZM(objectio.ZoneMap)         {}

func TestCollectChangesSchemaValidationMatrix(t *testing.T) {
	require.Error(t, func() error { _, err := collectChangesSchema(nil); return err }())
	_, err := collectChangesSchema(&plan.TableDef{Cols: []*plan.ColDef{nil}})
	require.ErrorContains(t, err, "nil column")
	_, err = collectChangesSchema(&plan.TableDef{Cols: []*plan.ColDef{{Name: "", Seqnum: 1}}})
	require.ErrorContains(t, err, "invalid data column")
	_, err = collectChangesSchema(&plan.TableDef{Cols: []*plan.ColDef{
		{Name: "a", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: "b", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int64)}},
	}})
	require.ErrorContains(t, err, "duplicate sequence")
	schema, err := collectChangesSchema(&plan.TableDef{Cols: []*plan.ColDef{
		{Name: catalog.Row_ID, Seqnum: uint32(objectio.SEQNUM_ROWID), Typ: plan.Type{Id: int32(types.T_Rowid)}},
		{Name: "value", Seqnum: 7, Typ: plan.Type{Id: int32(types.T_int64)}},
	}})
	require.NoError(t, err)
	require.Equal(t, []string{"value"}, schema.Attrs)
	require.Equal(t, []uint16{7}, schema.Seqnums)
}

func TestCheckpointChangesHandleNextLayouts(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	tableDef := &plan.TableDef{Cols: []*plan.ColDef{
		{Name: catalog.Row_ID, Typ: plan.Type{Id: int32(types.T_Rowid)}},
		{Name: "value", Typ: plan.Type{Id: int32(types.T_int64)}},
	}}
	newHandle := func(retain bool) (*CheckpointChangesHandle, *coverageCheckpointReader) {
		reader := &coverageCheckpointReader{}
		return &CheckpointChangesHandle{
			end: types.BuildTS(10, 0), table: &txnTable{tableDef: tableDef},
			reader: reader, attrs: []string{catalog.Row_ID, "value"},
			retainRowID: retain, lastPrintTime: time.Now(),
		}, reader
	}

	t.Run("retain rowid", func(t *testing.T) {
		handle, reader := newHandle(true)
		data, tombstone, hint, err := handle.Next(context.Background(), mp)
		require.NoError(t, err)
		require.Nil(t, tombstone)
		require.Equal(t, engine.ChangesHandle_Snapshot, hint)
		require.Equal(t, []string{catalog.Row_ID, "value", objectio.DefaultCommitTS_Attr}, data.Attrs)
		require.Equal(t, 1, data.RowCount())
		data.Clean(mp)
		require.NoError(t, handle.Close())
		require.True(t, reader.closed)
	})

	t.Run("drop rowid", func(t *testing.T) {
		handle, _ := newHandle(false)
		data, _, _, err := handle.Next(context.Background(), mp)
		require.NoError(t, err)
		require.Equal(t, []string{"value", objectio.DefaultCommitTS_Attr}, data.Attrs)
		require.Equal(t, 1, data.RowCount())
		data.Clean(mp)
	})

	t.Run("already drained and canceled", func(t *testing.T) {
		handle, _ := newHandle(false)
		handle.isEnd = true
		data, tombstone, hint, err := handle.Next(context.Background(), mp)
		require.NoError(t, err)
		require.Nil(t, data)
		require.Nil(t, tombstone)
		require.Equal(t, engine.ChangesHandle_Snapshot, hint)
		handle.prefetch()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		data, tombstone, _, err = handle.Next(ctx, mp)
		require.NoError(t, err)
		require.Nil(t, data)
		require.Nil(t, tombstone)
	})
}

func TestPartitionChangesCloseOwnership(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	newBatch := func() *batch.Batch {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
		bat.SetRowCount(1)
		return bat
	}
	handle := &PartitionChangesHandle{
		mp:                  mp,
		bufferedBatches:     []queuedChangeBatch{{data: newBatch(), tombstone: newBatch(), mp: mp}},
		currentChangeHandle: &logtailreplay.ChangeHandler{},
	}
	require.NoError(t, handle.swapCurrentHandleToSnapshotStateRange(context.Background()))
	require.NoError(t, handle.Close())
	require.Empty(t, handle.bufferedBatches)
	require.Nil(t, handle.currentChangeHandle)
	require.NoError(t, handle.Close())
	var nilHandle *PartitionChangesHandle
	require.NoError(t, nilHandle.Close())
	require.NoError(t, nilHandle.closeCurrentChangeHandle())
}
