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

package util

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestBuildIndexTableName(t *testing.T) {
	tests := []struct {
		indexNames     string
		uniques        bool
		indexTableName string
	}{
		{
			indexNames:     "a",
			uniques:        true,
			indexTableName: catalog.PrefixIndexTableName + "unique_",
		},
		{
			indexNames:     "b",
			uniques:        false,
			indexTableName: catalog.PrefixIndexTableName + "secondary_",
		},
	}
	for _, test := range tests {
		unique := test.uniques
		ctx := context.TODO()
		indexTableName, err := BuildIndexTableName(ctx, unique)
		require.Equal(t, indexTableName[:len(test.indexTableName)], test.indexTableName)
		require.Equal(t, err, nil)
	}
}

func TestBuildUniqueKeyBatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	tests := []struct {
		vecs  []*vector.Vector
		attrs []string
		parts []string
		proc  *process.Process
	}{
		{
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
			},
			attrs: []string{"a", "b", "c"},
			parts: []string{"a", "b", "c"},
			proc:  proc,
		},
		{
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
			},
			attrs: []string{"a", "b", "c"},
			parts: []string{"a"},
			proc:  proc,
		},
		{
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}),
				testutil.NewVector(3, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}),
				testutil.NewVector(3, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}),
			},
			attrs: []string{"a", "b", "c"},
			parts: []string{"a", "b", "c"},
			proc:  proc,
		},
		{
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}),
				testutil.NewVector(3, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}),
				testutil.NewVector(3, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}),
			},
			attrs: []string{"a", "b", "c"},
			parts: []string{"a"},
			proc:  proc,
		},
	}
	for _, test := range tests {
		packers := PackerList{}
		if len(test.parts) >= 2 {
			vec, _ := function.RunFunctionDirectly(proc, function.SerialFunctionEncodeID, test.vecs, test.vecs[0].Length())
			b, _, err := BuildUniqueKeyBatch(test.vecs, test.attrs, test.parts, "", test.proc, &packers)
			require.NoError(t, err)
			require.Equal(t, vec.UnsafeGetRawData(), b.Vecs[0].UnsafeGetRawData())
		} else {
			b, _, err := BuildUniqueKeyBatch(test.vecs, test.attrs, test.parts, "", test.proc, &packers)
			require.NoError(t, err)
			require.Equal(t, test.vecs[0].UnsafeGetRawData(), b.Vecs[0].UnsafeGetRawData())
		}
		for _, p := range packers.ps {
			p.Close()
		}
	}
}

func TestCompactUniqueKeyBatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	tests := []struct {
		vecs  []*vector.Vector
		attrs []string
		parts []string
		proc  *process.Process
	}{
		{
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
			},
			attrs: []string{"a", "b", "c"},
			parts: []string{"a", "b", "c"},
			proc:  proc,
		},
		{
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
			},
			attrs: []string{"a", "b", "c"},
			parts: []string{"b"},
			proc:  proc,
		},
		{
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}),
				testutil.NewVector(3, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}),
				testutil.NewVector(3, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}),
			},
			attrs: []string{"a", "b", "c"},
			parts: []string{"a", "b", "c"},
			proc:  proc,
		},
		{
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}),
				testutil.NewVector(3, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}),
				testutil.NewVector(3, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}),
			},
			attrs: []string{"a", "b", "c"},
			parts: []string{"b"},
			proc:  proc,
		},
	}
	for _, test := range tests {
		nulls.Add(test.vecs[1].GetNulls(), 1)
		//if JudgeIsCompositeIndexColumn(test.f) {
		packers := PackerList{}
		if len(test.parts) >= 2 {
			//b, _ := BuildUniqueKeyBatch(test.vecs, test.attrs, test.f.Parts, "", test.proc)
			b, _, err := BuildUniqueKeyBatch(test.vecs, test.attrs, test.parts, "", test.proc, &packers)
			require.NoError(t, err)
			require.Equal(t, 2, b.Vecs[0].Length())
		} else {
			//b, _ := BuildUniqueKeyBatch(test.vecs, test.attrs, test.f.Parts, "", test.proc)
			b, _, err := BuildUniqueKeyBatch(test.vecs, test.attrs, test.parts, "", test.proc, &packers)
			require.NoError(t, err)
			require.Equal(t, 2, b.Vecs[0].Length())
		}
		for _, p := range packers.ps {
			p.Close()
		}
	}
}

func TestSerialWithCompactedDecimal256(t *testing.T) {
	values := []types.Decimal256{
		(types.Decimal256{B0_63: 1}).Minus(),
		{B128_191: 1}, {}, {B192_255: 1}, {B64_127: 1},
	}
	for _, nullable := range []bool{false, true} {
		name := "without_null"
		if nullable {
			name = "mixed_null"
		}
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			decimal := vector.NewVec(types.New(types.T_decimal256, 65, 2))
			defer decimal.Free(proc.Mp())
			require.NoError(t, vector.AppendFixedList(decimal, values, nil, proc.Mp()))
			other := vector.NewVec(types.T_int64.ToType())
			defer other.Free(proc.Mp())
			require.NoError(t, vector.AppendFixedList(other, []int64{7, 7, 7, 7, 7}, nil, proc.Mp()))
			kept := []int{0, 1, 2, 3, 4}
			if nullable {
				decimal.GetNulls().Add(2)
				other.GetNulls().Add(4)
				kept = []int{0, 1, 3}
			}
			out := vector.NewVec(types.T_varchar.ToType())
			defer out.Free(proc.Mp())
			var packers PackerList
			defer packers.Free()
			removed, err := serialWithCompacted([]*vector.Vector{decimal, other}, out, proc, &packers, DefaultPackerSize)
			require.NoError(t, err)
			require.Equal(t, len(kept), out.Length())
			for i := range values {
				require.Equal(t, nullable && (i == 2 || i == 4), removed.Contains(uint64(i)))
			}
			for i, sourceRow := range kept {
				tuple, schema, err := types.UnpackWithSchema(out.GetBytesAt(i))
				require.NoError(t, err)
				require.Equal(t, []types.T{types.T_decimal256, types.T_int64}, schema)
				require.Equal(t, types.Tuple{values[sourceRow], int64(7)}, tuple)
				for j := 0; j < i; j++ {
					require.NotEqual(t, out.GetBytesAt(j), out.GetBytesAt(i), "distinct decimal components must not collapse to the same unique key")
				}
			}
		})
	}
}

func TestCompactSingleIndexColDecimal256(t *testing.T) {
	values := []types.Decimal256{
		(types.Decimal256{B0_63: 1}).Minus(), {B128_191: 1}, {}, {B192_255: 1},
	}
	for _, nullable := range []bool{false, true} {
		name := "without_null"
		if nullable {
			name = "mixed_null"
		}
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			input := vector.NewVec(types.New(types.T_decimal256, 65, 2))
			defer input.Free(proc.Mp())
			require.NoError(t, vector.AppendFixedList(input, values, nil, proc.Mp()))
			want := values
			if nullable {
				input.GetNulls().Add(0, 2)
				want = []types.Decimal256{values[1], values[3]}
			}
			out := vector.NewVec(*input.GetType())
			defer out.Free(proc.Mp())
			removed, err := compactSingleIndexCol(input, out, proc)
			require.NoError(t, err)
			require.Equal(t, len(want), out.Length())
			require.Equal(t, want, vector.MustFixedColNoTypeCheck[types.Decimal256](out))
			require.Equal(t, *input.GetType(), *out.GetType())
			require.False(t, out.HasNull())
			for i := range values {
				require.Equal(t, nullable && (i == 0 || i == 2), removed.Contains(uint64(i)))
			}
		})
	}
}

func TestCompactPrimaryColDecimal256(t *testing.T) {
	values := []types.Decimal256{{B128_191: 1}, {B128_191: 2}, {B128_191: 3}, {B128_191: 4}, {B128_191: 5}}
	for _, compact := range []bool{false, true} {
		name := "empty_bitmap"
		if compact {
			name = "index_null_bitmap"
		}
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			input := vector.NewVec(types.New(types.T_decimal256, 65, 0))
			defer input.Free(proc.Mp())
			require.NoError(t, vector.AppendFixedList(input, values, nil, proc.Mp()))
			removed := new(nulls.Nulls)
			want := values
			if compact {
				removed.Add(2, 4)
				want = []types.Decimal256{values[0], values[1], values[3]}
			}
			out := vector.NewVec(*input.GetType())
			defer out.Free(proc.Mp())
			require.NoError(t, compactPrimaryCol(input, out, removed, proc))
			require.Equal(t, len(want), out.Length())
			require.Equal(t, want, vector.MustFixedColNoTypeCheck[types.Decimal256](out), "primary keys must retain the source positions selected by the unique index bitmap")
			require.Equal(t, *input.GetType(), *out.GetType())
			require.False(t, out.HasNull())
		})
	}
}

func TestIsIndexTableName(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		expected  bool
	}{
		{
			name:      "test01",
			tableName: "__mo_index_unique_c1d278ec-bfd6-11ed-9e9d-000c29203f30",
			expected:  true,
		},
		{
			name:      "test02",
			tableName: "something_random",
			expected:  false,
		},
		{
			name:      "test03",
			tableName: "",
			expected:  false,
		},
		{
			name:      "test04",
			tableName: "normal_table_001",
			expected:  false,
		},
		{
			name:      "test05",
			tableName: "__mo_index_unique_c1d278ec-bfd6",
			expected:  false,
		},
		{
			name:      "test06",
			tableName: "secondary_idx_5678",
			expected:  false,
		},
		{
			name:      "test07",
			tableName: "__mo_index_secondary_c1d278ec-bfd6-11ed-9e9d-000c29203f30",
			expected:  true,
		},
		{
			name:      "test08",
			tableName: "__mo_index_secondary_c1d278ec-bfd6-11ed-9e9d",
			expected:  false,
		},
		{
			name:      "test09",
			tableName: "__mo_index_unique_",
			expected:  false,
		},
		{
			name:      "test10",
			tableName: "__mo_index_secondary_",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsIndexTableName(tt.tableName)
			if result != tt.expected {
				t.Errorf("IsIndexTableName(%s) = %v, expected %v", tt.tableName, result, tt.expected)
			}
		})
	}
}

func TestXXHashVectors(t *testing.T) {
	proc := testutil.NewProcess(t)

	vecs1 := []*vector.Vector{
		testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
		testutil.NewVector(3, types.T_varchar.ToType(), proc.Mp(), false, []string{"1", "2", "3"}),
		testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
	}

	vecs2 := []*vector.Vector{
		testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{4, 5, 6}),
		testutil.NewVector(3, types.T_varchar.ToType(), proc.Mp(), false, []string{"4", "5", "6"}),
		testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{4, 5, 6}),
	}

	vecs3 := []*vector.Vector{
		testutil.NewVector(6, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3, 4, 5, 6}),
		testutil.NewVector(6, types.T_varchar.ToType(), proc.Mp(), false, []string{"1", "2", "3", "4", "5", "6"}),
		testutil.NewVector(6, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3, 4, 5, 6}),
	}

	var packers PackerList

	hashCode1, rowCount1, err := XXHashVectors(vecs1, proc, &packers, nil)
	require.NoError(t, err)
	require.Equal(t, 3, rowCount1)
	require.Equal(t, 3, len(hashCode1))

	hashCode2, rowCount2, err := XXHashVectors(vecs2, proc, &packers, nil)
	require.NoError(t, err)
	require.Equal(t, 3, rowCount2)
	require.Equal(t, 3, len(hashCode2))

	hashCode3, rowCount3, err := XXHashVectors(vecs3, proc, &packers, nil)
	require.NoError(t, err)
	require.Equal(t, 6, rowCount3)
	require.Equal(t, 6, len(hashCode3))

	require.Equal(t, hashCode1, hashCode3[:3])
	require.Equal(t, hashCode2, hashCode3[3:])
}
