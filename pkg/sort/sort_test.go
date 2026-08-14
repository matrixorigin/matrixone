// Copyright 2021 Matrix Origin
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

package sort

import (
	"math"
	"slices"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 15
	BenchmarkRows = 100000
)

type testCase struct {
	desc bool
	vec  *vector.Vector
	proc *process.Process
}

func makeTestCases(t *testing.T) []testCase {
	mp := mpool.MustNewZero()
	return []testCase{
		newTestCase(t, true, mp, types.New(types.T_bool, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_bool, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_bit, 64, 0)),
		newTestCase(t, false, mp, types.New(types.T_bit, 64, 0)),

		newTestCase(t, true, mp, types.New(types.T_int8, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_int8, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_int16, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_int16, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_int32, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_int32, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_int64, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_int64, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_uint8, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_uint8, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_uint16, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_uint16, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_uint32, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_uint32, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_uint64, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_uint64, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_float32, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_float32, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_float64, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_float64, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_date, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_date, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_datetime, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_datetime, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_timestamp, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_timestamp, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_decimal64, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_decimal64, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_decimal128, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_decimal128, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_varchar, types.MaxVarcharLen, 0)),
		newTestCase(t, false, mp, types.New(types.T_varchar, types.MaxVarcharLen, 0)),

		newTestCase(t, true, mp, types.New(types.T_array_float32, types.MaxArrayDimension, 0)),
		newTestCase(t, false, mp, types.New(types.T_array_float32, types.MaxArrayDimension, 0)),

		newTestCase(t, true, mp, types.New(types.T_array_float64, types.MaxArrayDimension, 0)),
		newTestCase(t, false, mp, types.New(types.T_array_float64, types.MaxArrayDimension, 0)),

		newTestCase(t, true, mp, types.T_Blockid.ToType()),
		newTestCase(t, false, mp, types.T_Blockid.ToType()),

		newTestCase(t, true, mp, types.T_Rowid.ToType()),
		newTestCase(t, false, mp, types.T_Rowid.ToType()),
	}
}

func TestSort(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		os := make([]int64, tc.vec.Length())
		for i := range os {
			os[i] = int64(i)
		}
		nb0 := tc.proc.Mp().CurrNB()
		Sort(tc.desc, false, false, os, tc.vec)
		checkResult(t, tc.desc, tc.vec, os)
		nb1 := tc.proc.Mp().CurrNB()
		require.Equal(t, nb0, nb1)
		tc.vec.Free(tc.proc.Mp())
	}
}

func TestSortDecimal256(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.New(types.T_decimal256, 65, 2))
	defer vec.Free(mp)

	v1, err := types.ParseDecimal256("3.45", 65, 2)
	require.NoError(t, err)
	v2, err := types.ParseDecimal256("1.23", 65, 2)
	require.NoError(t, err)
	v3, err := types.ParseDecimal256("2.34", 65, 2)
	require.NoError(t, err)

	err = vector.AppendFixedList(vec, []types.Decimal256{v1, v2, v3}, nil, mp)
	require.NoError(t, err)

	os := []int64{0, 1, 2}
	Sort(false, false, false, os, vec)
	require.Equal(t, []int64{1, 2, 0}, os)

	os = []int64{0, 1, 2}
	Sort(true, false, false, os, vec)
	require.Equal(t, []int64{0, 2, 1}, os)
}

func TestSortByVectors(t *testing.T) {
	mp := mpool.MustNewZero()
	first := vector.NewVec(types.T_int64.ToType())
	second := vector.NewVec(types.T_int64.ToType())
	defer first.Free(mp)
	defer second.Free(mp)

	require.NoError(t, vector.AppendFixedList(
		first,
		[]int64{1, 1, 1, 2, 2, 0},
		[]bool{false, false, false, false, false, true},
		mp,
	))
	require.NoError(t, vector.AppendFixedList(second, []int64{2, 1, 3, 2, 1, 0}, nil, mp))

	selectors := []int64{0, 1, 2, 3, 4, 5}
	SortByVectors(
		selectors,
		[]*vector.Vector{first, second},
		[]bool{false, true},
		[]bool{true, false},
	)
	require.Equal(t, []int64{2, 0, 1, 3, 4, 5}, selectors)
}

func TestSortByVectorsFloatOrderPeersUseSecondaryKey(t *testing.T) {
	mp := mpool.MustNewZero()
	first := vector.NewVec(types.T_float64.ToType())
	second := vector.NewVec(types.T_int64.ToType())
	defer first.Free(mp)
	defer second.Free(mp)

	require.NoError(t, vector.AppendFixedList(first, []float64{
		math.Float64frombits(0x7ff8000000000002), math.Inf(1), math.Copysign(0, -1), 1,
		math.Inf(-1), math.Float64frombits(0x7ff8000000000001), 0, -1,
	}, nil, mp))
	require.NoError(t, vector.AppendFixedList(second, []int64{2, 30, 20, 40, 50, 1, 10, 60}, nil, mp))

	for _, tc := range []struct {
		name string
		desc bool
		want []int64
	}{
		{name: "ascending", want: []int64{4, 7, 6, 2, 3, 1, 5, 0}},
		{name: "descending", desc: true, want: []int64{1, 3, 6, 2, 7, 4, 5, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selectors := []int64{0, 1, 2, 3, 4, 5, 6, 7}
			SortByVectors(selectors, []*vector.Vector{first, second}, []bool{tc.desc, false}, []bool{false, false})
			require.Equal(t, tc.want, selectors)
		})
	}
}

func TestSortForSQLOrderFloat32NaNLast(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_float32.ToType())
	defer vec.Free(mp)
	require.NoError(t, vector.AppendFixedList(vec, []float32{
		math.Float32frombits(0x7fc00002), 1, float32(math.Inf(-1)),
		math.Float32frombits(0x7fc00001), float32(math.Inf(1)), -1,
	}, nil, mp))

	for _, tc := range []struct {
		desc bool
		want []int64
	}{
		{want: []int64{2, 5, 1, 4, 0, 3}},
		{desc: true, want: []int64{4, 1, 5, 2, 0, 3}},
	} {
		selectors := []int64{0, 1, 2, 3, 4, 5}
		SortForSQLOrder(tc.desc, false, false, selectors, vec)
		require.Equal(t, tc.want[:4], selectors[:4])
		for _, sel := range selectors[4:] {
			require.True(t, math.IsNaN(float64(vector.MustFixedColWithTypeCheck[float32](vec)[sel])))
		}
	}
}

func BenchmarkSortInt(b *testing.B) {
	vs := make([]int, BenchmarkRows)
	for i := range vs {
		vs[i] = i
	}
	for i := 0; i < b.N; i++ {
		sort.Ints(vs)
	}
}

func BenchmarkSortIntVector(b *testing.B) {
	m := mpool.MustNewZero()
	vec := testutil.NewInt32Vector(BenchmarkRows, types.T_int32.ToType(), m, true, nil, nil)
	os := make([]int64, vec.Length())
	for i := range os {
		os[i] = int64(i)
	}
	for i := 0; i < b.N; i++ {
		Sort(false, false, false, os, vec)
	}
}

func checkResult(t *testing.T, desc bool, vec *vector.Vector, os []int64) {
	switch vec.GetType().Oid {
	case types.T_bit:
		vs := make([]int, len(os))
		col := vector.MustFixedColWithTypeCheck[uint64](vec)
		for i := range vs {
			vs[i] = int(col[i])
		}
		sort.Ints(vs)
		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, int(col[os[j]]))
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, int(col[os[i]]))
			}
		}
	case types.T_int32:
		vs := make([]int, len(os))
		col := vector.MustFixedColWithTypeCheck[int32](vec)
		for i := range vs {
			vs[i] = int(col[i])
		}
		sort.Ints(vs)
		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, int(col[os[j]]))
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, int(col[os[i]]))
			}
		}
	case types.T_int64:
		vs := make([]int, len(os))
		col := vector.MustFixedColWithTypeCheck[int64](vec)
		for i := range vs {
			vs[i] = int(col[i])
		}
		sort.Ints(vs)
		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, int(col[os[j]]))
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, int(col[os[i]]))
			}
		}
	case types.T_float32:
		vs := make([]float64, len(os))
		col := vector.MustFixedColWithTypeCheck[float32](vec)
		for i := range vs {
			vs[i] = float64(col[i])
		}
		sort.Float64s(vs)
		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, float64(col[os[j]]))
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, float64(col[os[i]]))
			}
		}
	case types.T_float64:
		vs := make([]float64, len(os))
		col := vector.MustFixedColWithTypeCheck[float64](vec)
		for i := range vs {
			vs[i] = float64(col[i])
		}
		sort.Float64s(vs)
		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, float64(col[os[j]]))
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, float64(col[os[i]]))
			}
		}
	case types.T_Blockid:
		col := vector.MustFixedColWithTypeCheck[types.Blockid](vec)
		vs := make([]types.Blockid, len(os))

		for i := range vs {
			vs[i] = col[i]
		}

		slices.SortFunc(vs, func(a, b types.Blockid) int {
			return a.Compare(&b)
		})

		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, col[os[j]])
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, col[os[i]])
			}
		}

	case types.T_Rowid:
		col := vector.MustFixedColWithTypeCheck[types.Rowid](vec)
		vs := make([]types.Rowid, len(os))

		for i := range vs {
			vs[i] = col[i]
		}

		slices.SortFunc(vs, func(a, b types.Rowid) int {
			return a.Compare(&b)
		})

		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, col[os[j]])
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, col[os[i]])
			}
		}
	}
}

func newTestCase(t *testing.T, desc bool, m *mpool.MPool, typ types.Type) testCase {
	return testCase{
		desc: desc,
		proc: testutil.NewProcessWithMPool(t, "", m),
		vec:  testutil.NewVector(Rows, typ, m, true, nil),
	}
}
