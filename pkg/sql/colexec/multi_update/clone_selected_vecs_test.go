// Copyright 2024 Matrix Origin
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

package multi_update

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestCloneSelectedVecsFromCompactBatches_NullMask(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	defer bat.Clean(mp)

	for i := 0; i < 4; i++ {
		isNull := i == 1 || i == 3
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i), isNull, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(100+i), false, mp))
	}
	bat.SetRowCount(4)

	sourceBats := batch.NewCompactBatchs(10)
	require.NoError(t, sourceBats.Push(mp, bat))

	cloned, err := cloneSelectedVecsFromCompactBatches(
		sourceBats,
		[]int{0, 1},
		[]string{"a", "b"},
		0,
		mp,
	)
	require.NoError(t, err)
	require.Len(t, cloned, 1)
	defer func() {
		for _, cb := range cloned {
			if cb != nil {
				cb.Clean(mp)
			}
		}
	}()

	got := cloned[0]
	require.Equal(t, 2, got.RowCount())
	require.Equal(t, got.RowCount(), got.Vecs[0].Length())
	require.Equal(t, got.RowCount(), got.Vecs[1].Length())

	col0 := vector.MustFixedColWithTypeCheck[int64](got.Vecs[0])
	col1 := vector.MustFixedColWithTypeCheck[int64](got.Vecs[1])
	require.Equal(t, []int64{0, 2}, col0)
	require.Equal(t, []int64{100, 102}, col1)
}
