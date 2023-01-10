// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
)

func TestPartition(t *testing.T) {
	mp := mpool.MustNewZero()
	v0 := vector.New(vector.FLAT, types.T_int8.ToType())
	vector.AppendList(v0, []int8{3, 4, 5, 6, 7, 8}, nil, mp)
	partitions := make([]int64, 2)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v0)
	require.Equal(t, []int64{0, 1}, partitions)
	nulls.Add(v0.GetNulls(), 1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v0)
	require.Equal(t, []int64{0, 1}, partitions)

	v1 := vector.New(vector.FLAT, types.T_int16.ToType())
	vector.AppendList(v1, []int16{3, 4, 5, 6, 7, 8}, nil, mp)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v1)
	require.Equal(t, []int64{0, 1}, partitions)
	nulls.Add(v0.GetNulls(), 1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v1)
	require.Equal(t, []int64{0, 1}, partitions)

	v2 := vector.New(vector.FLAT, types.T_int32.ToType())
	vector.AppendList(v2, []int32{3, 4, 5, 6, 7, 8}, nil, mp)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v2)
	require.Equal(t, []int64{0, 1}, partitions)
	nulls.Add(v2.GetNulls(), 1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v2)
	require.Equal(t, []int64{0, 1}, partitions)

	v3 := vector.New(vector.FLAT, types.T_int64.ToType())
	vector.AppendList(v3, []int64{3, 4, 5, 6, 7, 8}, nil, mp)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v3)
	require.Equal(t, []int64{0, 1}, partitions)
	nulls.Add(v3.GetNulls(), 1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v3)
	require.Equal(t, []int64{0, 1}, partitions)

	v4 := vector.New(vector.FLAT, types.T_uint8.ToType())
	vector.AppendList(v4, []uint8{3, 4, 5, 6, 7, 8}, nil, mp)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v4)
	require.Equal(t, []int64{0, 1}, partitions)
	nulls.Add(v4.GetNulls(), 1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v4)
	require.Equal(t, []int64{0, 1}, partitions)

	v5 := vector.New(vector.FLAT, types.T_uint16.ToType())
	vector.AppendList(v5, []uint16{3, 4, 5, 6, 7, 8}, nil, mp)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v5)
	require.Equal(t, []int64{0, 1}, partitions)
	nulls.Add(v5.GetNulls(), 1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v5)
	require.Equal(t, []int64{0, 1}, partitions)

	v6 := vector.New(vector.FLAT, types.T_uint32.ToType())
	vector.AppendList(v6, []uint32{3, 4, 5, 6, 7, 8}, nil, mp)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v6)
	require.Equal(t, []int64{0, 1}, partitions)
	nulls.Add(v6.GetNulls(), 1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v6)
	require.Equal(t, []int64{0, 1}, partitions)

	v7 := vector.New(vector.FLAT, types.T_uint64.ToType())
	vector.AppendList(v7, []uint64{3, 4, 5, 6, 7, 8}, nil, mp)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v7)
	require.Equal(t, []int64{0, 1}, partitions)
	nulls.Add(v7.GetNulls(), 1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v7)
	require.Equal(t, []int64{0, 1}, partitions)

	v8 := vector.New(vector.FLAT, types.T_date.ToType())
	vector.AppendList(v8, []types.Date{3, 4, 5, 6, 7, 8}, nil, mp)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v8)
	require.Equal(t, []int64{0, 1}, partitions)
	nulls.Add(v8.GetNulls(), 1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v8)
	require.Equal(t, []int64{0, 1}, partitions)

	v9 := vector.New(vector.FLAT, types.T_float32.ToType())
	vector.AppendList(v9, []float32{3, 4, 5, 6, 7, 8}, nil, mp)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v9)
	require.Equal(t, []int64{0, 1}, partitions)
	nulls.Add(v9.GetNulls(), 1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v9)
	require.Equal(t, []int64{0, 1}, partitions)

	v10 := vector.New(vector.FLAT, types.T_float64.ToType())
	vector.AppendList(v10, []float64{3, 4, 5, 6, 7, 8}, nil, mp)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v10)
	require.Equal(t, []int64{0, 1}, partitions)
	nulls.Add(v10.GetNulls(), 1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v10)
	require.Equal(t, []int64{0, 1}, partitions)

	v11 := vector.New(vector.FLAT, types.T_char.ToType())
	vector.AppendStringList(v11, []string{"hello", "Gut", "konichiwa", "nihao", "nihao", "nihao", "nihao"}, nil, mp)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v10)
	require.Equal(t, []int64{0, 1}, partitions)
	nulls.Add(v11.GetNulls(), 1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v11)
	require.Equal(t, []int64{0, 1}, partitions)
}
