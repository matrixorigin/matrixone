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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestShrinkBatchByRowList(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	newBatch := func() *batch.Batch {
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
		for i := range 6 {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i*10), i == 4, mp))
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte{byte('a' + i)}, false, mp))
		}
		bat.SetRowCount(6)
		return bat
	}
	rowsOf := func(bat *batch.Batch) ([]int64, []string, []bool) {
		ints := make([]int64, 0, bat.RowCount())
		strs := make([]string, 0, bat.RowCount())
		nulls := make([]bool, 0, bat.RowCount())
		for row := range bat.RowCount() {
			ints = append(ints, vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], row))
			strs = append(strs, bat.Vecs[1].GetStringAt(row))
			nulls = append(nulls, bat.Vecs[0].IsNull(uint64(row)))
		}
		return ints, strs, nulls
	}

	// Unordered, repeated rows to keep: the probe query's row order is not relied on.
	bat := newBatch()
	require.NoError(t, shrinkBatchByRowList(bat, []int64{4, 1, 4, 3}, false, mp))
	ints, strs, nulls := rowsOf(bat)
	require.Equal(t, []int64{10, 30}, ints[:2])
	require.Equal(t, []string{"b", "d", "e"}, strs)
	require.Equal(t, []bool{false, false, true}, nulls)
	bat.Clean(mp)

	// Rows to remove, as the conflict checks produce them.
	bat = newBatch()
	require.NoError(t, shrinkBatchByRowList(bat, []int64{5, 0, 0}, true, mp))
	_, strs, _ = rowsOf(bat)
	require.Equal(t, []string{"b", "c", "d", "e"}, strs)
	bat.Clean(mp)

	bat = newBatch()
	require.Error(t, shrinkBatchByRowList(bat, []int64{-1}, true, mp))
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}
