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
	proc := testutil.NewProcess()
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
	proc := testutil.NewProcess()
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
