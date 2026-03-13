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
