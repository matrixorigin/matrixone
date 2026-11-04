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

package runtimefilter

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func checkExactDataSorter[T types.FixedSizeT](t *testing.T, typ types.Type, values []T) {
	t.Helper()
	mp := mpool.MustNewZero()
	v := vector.NewVec(typ)
	defer v.Free(mp)
	require.NoError(t, vector.AppendFixedList(v, values, nil, mp))
	data := append([]byte(nil), v.GetData()[:v.Length()*typ.TypeSize()]...)
	sorter := ExactFilterDataSorter(typ)
	require.NotNil(t, sorter)
	sorter(data)
	require.NoError(t, v.InplaceSort(mp))
	require.Equal(t, v.GetData()[:v.Length()*typ.TypeSize()], data)
}

func TestExactDataSortersMatchVectorOrdering(t *testing.T) {
	checkExactDataSorter(t, types.T_bool.ToType(), []bool{true, false, true})
	checkExactDataSorter(t, types.T_int8.ToType(), []int8{3, -7, 0})
	checkExactDataSorter(t, types.T_int16.ToType(), []int16{257, -7, 0})
	checkExactDataSorter(t, types.T_int32.ToType(), []int32{65537, -7, 0})
	checkExactDataSorter(t, types.T_int64.ToType(), []int64{1 << 40, -7, 0})
	checkExactDataSorter(t, types.T_uint8.ToType(), []uint8{255, 0, 128})
	checkExactDataSorter(t, types.T_uint16.ToType(), []uint16{257, 1, 256})
	checkExactDataSorter(t, types.T_uint32.ToType(), []uint32{1 << 31, 0, 65537})
	checkExactDataSorter(t, types.T_uint64.ToType(), []uint64{1 << 63, 0, 1 << 40})
	checkExactDataSorter(t, types.T_bit.ToType(), []uint64{1 << 63, 0, 1 << 40})
	checkExactDataSorter(t, types.T_float32.ToType(), []float32{3, -7, 0, float32(math.NaN())})
	checkExactDataSorter(t, types.T_float64.ToType(), []float64{3, -7, 0, math.NaN()})
	checkExactDataSorter(t, types.T_date.ToType(), []types.Date{3, -7, 0})
	checkExactDataSorter(t, types.T_time.ToType(), []types.Time{3, -7, 0})
	checkExactDataSorter(t, types.T_datetime.ToType(), []types.Datetime{3, -7, 0})
	checkExactDataSorter(t, types.T_timestamp.ToType(), []types.Timestamp{3, -7, 0})
	checkExactDataSorter(t, types.T_year.ToType(), []types.MoYear{2026, 0, 1990})
	checkExactDataSorter(t, types.T_enum.ToType(), []types.Enum{3, 1, 2})
	checkExactDataSorter(t, types.T_decimal64.ToType(), []types.Decimal64{7, 0, types.Decimal64(^uint64(0))})
	checkExactDataSorter(t, types.T_decimal128.ToType(), []types.Decimal128{{B0_63: 7}, {B0_63: 1}, {}})
	checkExactDataSorter(t, types.T_decimal256.ToType(), []types.Decimal256{{B0_63: 7}, {B0_63: 1}, {}})
	checkExactDataSorter(t, types.T_uuid.ToType(), []types.Uuid{{7}, {1}, {0}})
	require.Nil(t, ExactFilterDataSorter(types.T_varchar.ToType()))
}

func TestMarshalExactFilterRuns(t *testing.T) {
	for _, variable := range []bool{false, true} {
		t.Run(map[bool]string{false: "fixed", true: "variable"}[variable], func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
			require.NoError(t, err)
			account, err := registry.Open(1 << 30)
			require.NoError(t, err)
			typ := types.T_int64.ToType()
			if variable {
				typ = types.T_varchar.ToType()
			}
			x, y, expected := vector.NewVec(typ), vector.NewVec(typ), vector.NewVec(typ)
			defer func() {
				x.Free(mp)
				y.Free(mp)
				expected.Free(mp)
				require.Zero(t, mp.CurrNB())
				require.Zero(t, account.Snapshot().Used)
			}()
			for i := 0; i < 31; i++ {
				v := x
				if i%2 != 0 {
					v = y
				}
				if variable {
					value := append([]byte{byte('a' + i%26)}, make([]byte, i+1)...)
					require.NoError(t, vector.AppendBytes(v, value, false, mp))
					require.NoError(t, vector.AppendBytes(expected, value, false, mp))
				} else {
					require.NoError(t, vector.AppendFixed(v, int64(31-i), false, mp))
					require.NoError(t, vector.AppendFixed(expected, int64(31-i), false, mp))
				}
			}
			for _, v := range []*vector.Vector{x, y, expected} {
				if variable || v == expected {
					require.NoError(t, v.InplaceSort(mp))
				}
			}
			data, release, err := MarshalExactFilterRuns(context.Background(), []*vector.Vector{x, y}, mp, account, mpool.AllocationOwnerHashBuild, mpool.AllocationSiteMin)
			require.NoError(t, err)
			defer release()
			var actual vector.Vector
			require.NoError(t, actual.UnmarshalBinary(data))
			defer actual.Free(nil)
			require.Equal(t, expected.Length(), actual.Length())
			for row := 0; row < actual.Length(); row++ {
				if variable {
					require.Equal(t, expected.GetRawBytesAt(row), actual.GetRawBytesAt(row))
				} else {
					require.Equal(t, vector.MustFixedColNoTypeCheck[int64](expected)[row], vector.MustFixedColNoTypeCheck[int64](&actual)[row])
				}
			}
			require.True(t, actual.GetSorted())
		})
	}
}

func TestMarshalExactFilterRunsSharedArea(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	runs := make([]*vector.Vector, 2)
	t.Cleanup(func() {
		for _, v := range runs {
			if v != nil {
				v.Free(mp)
			}
		}
		require.Zero(t, mp.CurrNB())
		require.Zero(t, account.Snapshot().Used)
	})
	for i := range runs {
		func() {
			value := make([]byte, 64)
			for j := range value {
				value[j] = byte('a' + i)
			}
			source, err := vector.NewConstBytes(types.T_varchar.ToType(), value, 3, mp)
			require.NoError(t, err)
			defer source.Free(mp)
			runs[i] = vector.NewVec(types.T_varchar.ToType())
			require.NoError(t, runs[i].UnionBatch(source, 0, 3, nil, mp))
			require.Len(t, runs[i].GetArea(), 64, "three descriptors share one value")
		}()
	}
	data, release, err := MarshalExactFilterRuns(context.Background(), runs, mp, account, mpool.AllocationOwnerHashBuild, mpool.AllocationSiteMin)
	require.NoError(t, err)
	defer release()
	var actual vector.Vector
	require.NoError(t, actual.UnmarshalBinary(data))
	defer actual.Free(nil)
	require.Equal(t, 6, actual.Length())
	require.Len(t, actual.GetArea(), 128, "keep the shared area instead of copying it for every row")
	for row := 0; row < actual.Length(); row++ {
		require.Equal(t, runs[row/3].GetRawBytesAt(0), actual.GetRawBytesAt(row))
	}
	require.True(t, actual.GetSorted())
}
