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

package partition

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestPartitionAllNullAny(t *testing.T) {
	mp := mpool.MustNewZero()
	func() {
		v := vector.NewVec(types.T_any.ToType(), mp)
		defer v.Free(mp)
		for range 4 {
			require.NoError(t, vector.AppendNull(v, mp))
		}

		diffs := []bool{true, false, true, false}
		var partitions []int64
		require.NotPanics(t, func() {
			partitions = Partition([]int64{3, 1, 2, 0}, diffs, []int64{99}, v)
		})
		require.Equal(t, []int64{0, 2}, partitions)
		require.Equal(t, []bool{true, false, true, false}, diffs)
	}()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPartitionEmptyAny(t *testing.T) {
	mp := mpool.MustNewZero()
	func() {
		v := vector.NewVec(types.T_any.ToType(), mp)
		defer v.Free(mp)
		require.NotPanics(t, func() {
			require.Empty(t, Partition(nil, nil, nil, v))
		})
	}()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPartitionAllNullAnyAfterSetLength(t *testing.T) {
	mp := mpool.MustNewZero()
	func() {
		v := vector.NewVec(types.T_any.ToType(), mp)
		defer v.Free(mp)
		for range 4 {
			require.NoError(t, vector.AppendNull(v, mp))
		}
		v.SetLength(2)
		require.True(t, v.AllNull())

		diffs := make([]bool, 2)
		var partitions []int64
		require.NotPanics(t, func() {
			partitions = Partition([]int64{0, 1}, diffs, nil, v)
		})
		require.Equal(t, []int64{0}, partitions)
		require.Equal(t, []bool{true, false}, diffs)
	}()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPartitionRejectsNonNullAny(t *testing.T) {
	mp := mpool.MustNewZero()
	func() {
		v := vector.NewVec(types.T_any.ToType(), mp)
		defer v.Free(mp)
		require.NoError(t, vector.AppendNull(v, mp))
		v.UnsetNull(0)
		require.Panics(t, func() {
			Partition([]int64{0}, make([]bool, 1), nil, v)
		})
	}()
	require.Equal(t, int64(0), mp.CurrNB())
}
