// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type scriptedReader struct {
	batches []*batch.Batch
	idx     int
}

func (r *scriptedReader) Close() error { return nil }

func (r *scriptedReader) Read(_ context.Context, _ []string, _ *pbplan.Expr, mp *mpool.MPool, dst *batch.Batch) (bool, error) {
	if r.idx >= len(r.batches) {
		return true, nil
	}
	src := r.batches[r.idx]
	r.idx++
	for row := 0; row < src.RowCount(); row++ {
		if err := dst.UnionOne(src, int64(row), mp); err != nil {
			return false, err
		}
	}
	dst.SetRowCount(src.RowCount())
	return false, nil
}

func (r *scriptedReader) SetOrderBy([]*plan.OrderBySpec) {}

func (r *scriptedReader) GetOrderBy() []*plan.OrderBySpec { return nil }

func (r *scriptedReader) SetIndexParam(*pbplan.IndexReaderParam) {}

func (r *scriptedReader) SetFilterZM(objectio.ZoneMap) {}

func TestVisibleStateChangesHandleNext(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	h := &VisibleStateChangesHandle{
		end:           types.BuildTS(10, 0),
		coarseMaxRow:  16,
		mp:            mp,
		scanAttrs:     []string{"a", "b"},
		scanTypes:     []types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		dataScanIdxes: []int{0, 1},
		dataAttrs:     []string{"a", "b"},
		dataTypes:     []types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		pkScanIdx:     0,
		pkType:        types.T_int32.ToType(),
		beforeRows:    make(map[string]visibleStateRow),
	}

	before := makeInt32Batch(t, mp, [][2]int32{
		{1, 10},
		{2, 20},
	})
	defer before.Clean(mp)
	for row := 0; row < before.RowCount(); row++ {
		pkBytes, rowBytes := h.encodeSnapshotRow(before, row)
		h.beforeRows[string(pkBytes)] = visibleStateRow{pk: pkBytes, row: rowBytes}
	}

	after := makeInt32Batch(t, mp, [][2]int32{
		{1, 11},
		{3, 30},
	})
	defer after.Clean(mp)
	h.afterReaders = []engine.Reader{&scriptedReader{batches: []*batch.Batch{after}}}

	data, tombstone, hint, err := h.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Equal(t, engine.ChangesHandle_Tail_done, hint)
	require.NotNil(t, data)
	require.NotNil(t, tombstone)
	defer data.Clean(mp)
	defer tombstone.Clean(mp)

	require.Equal(t, []int32{1, 3}, vector.MustFixedColWithTypeCheck[int32](data.Vecs[0]))
	require.Equal(t, []int32{11, 30}, vector.MustFixedColWithTypeCheck[int32](data.Vecs[1]))
	require.Equal(t, []int32{1, 2}, vector.MustFixedColWithTypeCheck[int32](tombstone.Vecs[0]))
	require.Equal(t, []types.TS{h.end, h.end}, vector.MustFixedColWithTypeCheck[types.TS](data.Vecs[2]))
	require.Equal(t, []types.TS{h.end, h.end}, vector.MustFixedColWithTypeCheck[types.TS](tombstone.Vecs[1]))

	data, tombstone, hint, err = h.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Equal(t, engine.ChangesHandle_Tail_done, hint)
	require.Nil(t, data)
	require.Nil(t, tombstone)
}

func TestVisibleStateChangesHandleIgnoresCompactionRowIDRewrite(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	h := &VisibleStateChangesHandle{
		end:           types.BuildTS(20, 0),
		coarseMaxRow:  16,
		mp:            mp,
		scanAttrs:     []string{"__mo_rowid", "id", "v"},
		scanTypes:     []types.Type{types.T_Rowid.ToType(), types.T_int32.ToType(), types.T_int32.ToType()},
		dataScanIdxes: []int{1, 2},
		dataAttrs:     []string{"id", "v"},
		dataTypes:     []types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		pkScanIdx:     1,
		pkType:        types.T_int32.ToType(),
		rowIDScanIdx:  0,
		rowIDType:     types.T_Rowid.ToType(),
		retainRowID:   true,
		beforeRows:    make(map[string]visibleStateRow),
	}

	beforeRowIDs := []types.Rowid{
		types.BuildTestRowid(1, 1),
		types.BuildTestRowid(1, 2),
		types.BuildTestRowid(1, 4),
	}
	before := makeVisibleStateBatch(t, mp, beforeRowIDs, [][2]int32{
		{1, 10},
		{2, 20},
		{4, 40},
	})
	defer before.Clean(mp)
	for row := 0; row < before.RowCount(); row++ {
		pkBytes, rowBytes := h.encodeSnapshotRow(before, row)
		h.beforeRows[string(pkBytes)] = visibleStateRow{
			pk:    pkBytes,
			rowID: h.encodeValue(before.Vecs[h.rowIDScanIdx], row),
			row:   rowBytes,
		}
	}

	afterRowIDs := []types.Rowid{
		types.BuildTestRowid(2, 1),
		types.BuildTestRowid(2, 2),
		types.BuildTestRowid(2, 3),
	}
	after := makeVisibleStateBatch(t, mp, afterRowIDs, [][2]int32{
		{1, 10}, // unchanged payload, rewritten physical rowid
		{2, 21}, // update
		{3, 30}, // insert
	})
	defer after.Clean(mp)
	h.afterReaders = []engine.Reader{&scriptedReader{batches: []*batch.Batch{after}}}

	data, tombstone, hint, err := h.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Equal(t, engine.ChangesHandle_Tail_done, hint)
	require.NotNil(t, data)
	require.NotNil(t, tombstone)
	defer data.Clean(mp)
	defer tombstone.Clean(mp)

	require.Equal(t, []string{"__mo_rowid", "id", "v", objectio.DefaultCommitTS_Attr}, data.Attrs)
	require.Equal(t, afterRowIDs[1:], vector.MustFixedColWithTypeCheck[types.Rowid](data.Vecs[0]))
	require.Equal(t, []int32{2, 3}, vector.MustFixedColWithTypeCheck[int32](data.Vecs[1]))
	require.Equal(t, []int32{21, 30}, vector.MustFixedColWithTypeCheck[int32](data.Vecs[2]))
	require.Equal(t, []types.TS{h.end, h.end}, vector.MustFixedColWithTypeCheck[types.TS](data.Vecs[3]))

	require.Equal(t, []string{"__mo_rowid", objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}, tombstone.Attrs)
	require.Equal(t, []types.Rowid{beforeRowIDs[1], beforeRowIDs[2]}, vector.MustFixedColWithTypeCheck[types.Rowid](tombstone.Vecs[0]))
	require.Equal(t, []int32{2, 4}, vector.MustFixedColWithTypeCheck[int32](tombstone.Vecs[1]))
	require.Equal(t, []types.TS{h.end, h.end}, vector.MustFixedColWithTypeCheck[types.TS](tombstone.Vecs[2]))
}

func TestVisibleStateChangesHandleCloseNilReceiver(t *testing.T) {
	var h *VisibleStateChangesHandle
	require.NotPanics(t, func() {
		_ = h.Close()
	})
}

func TestVisibleStateChangesHandleCloseNilMPoolAndTypedNilReader(t *testing.T) {
	var nilReader *scriptedReader
	h := &VisibleStateChangesHandle{
		mp:             nil,
		beforeRows:     map[string]visibleStateRow{"before": {}},
		pendingDeletes: []visibleStateRow{{pk: []byte("pending")}},
		currentAfter: func() *batch.Batch {
			bat := batch.NewWithSize(1)
			bat.SetAttributes([]string{"a"})
			bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			bat.SetRowCount(0)
			return bat
		}(),
		afterReaders: []engine.Reader{
			nilReader,
		},
	}
	require.NotPanics(t, func() {
		_ = h.Close()
	})
	require.Nil(t, h.currentAfter)
	require.Nil(t, h.beforeRows)
	require.Nil(t, h.pendingDeletes)
}

func makeInt32Batch(t *testing.T, mp *mpool.MPool, rows [][2]int32) *batch.Batch {
	t.Helper()

	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{"a", "b"})
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for _, row := range rows {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], row[0], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], row[1], false, mp))
	}
	bat.SetRowCount(len(rows))
	return bat
}

func makeVisibleStateBatch(
	t *testing.T,
	mp *mpool.MPool,
	rowIDs []types.Rowid,
	rows [][2]int32,
) *batch.Batch {
	t.Helper()
	require.Len(t, rowIDs, len(rows))

	bat := batch.NewWithSize(3)
	bat.SetAttributes([]string{"__mo_rowid", "id", "v"})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())
	for i, row := range rows {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], rowIDs[i], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], row[0], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], row[1], false, mp))
	}
	bat.SetRowCount(len(rows))
	return bat
}
