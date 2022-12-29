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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/multi"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"testing"

	"github.com/stretchr/testify/require"
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
			indexTableName: catalog.PrefixIndexTableName + "unique_a_",
		},
		{
			indexNames:     "b",
			uniques:        false,
			indexTableName: catalog.PrefixIndexTableName + "secondary_b_",
		},
	}
	for _, test := range tests {
		indexName := test.indexNames
		unique := test.uniques
		ctx := context.TODO()
		indexTableName, err := BuildIndexTableName(ctx, unique, indexName)
		require.Equal(t, indexTableName[:len(test.indexTableName)], test.indexTableName)
		require.Equal(t, err, nil)
	}
}

func TestBuildUniqueKeyBatch(t *testing.T) {
	proc := testutil.NewProcess()
	tests := []struct {
		vecs  []*vector.Vector
		attrs []string
		f     *plan.Field
		proc  *process.Process
	}{
		{
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
			},
			attrs: []string{"a", "b", "c"},
			f: &plan.Field{
				Parts: []string{
					"a", "b", "c",
				},
				Cols: []*plan.ColDef{
					{
						Name: catalog.IndexTableIndexColName,
						Typ:  &plan.Type{Id: int32(types.T_varchar), Size: types.VarlenaSize},
					},
				},
			},
			proc: proc,
		},
		{
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
			},
			attrs: []string{"a", "b", "c"},
			f: &plan.Field{
				Parts: []string{
					"a",
				},
				Cols: []*plan.ColDef{
					{
						Name: catalog.IndexTableIndexColName,
						Typ:  &plan.Type{Id: int32(types.T_int64)},
					},
				},
			},
			proc: proc,
		},
	}
	for _, test := range tests {
		if JudgeIsCompositeIndexColumn(test.f) {
			vec, _ := multi.Serial(test.vecs, proc)
			b, _ := BuildUniqueKeyBatch(test.vecs, test.attrs, test.f.Parts, "", test.proc)
			require.Equal(t, b.Attrs[0], test.f.Cols[0].Name)
			require.Equal(t, vec.Col, b.Vecs[0].Col)
		} else {
			b, _ := BuildUniqueKeyBatch(test.vecs, test.attrs, test.f.Parts, "", test.proc)
			require.Equal(t, b.Attrs[0], test.f.Cols[0].Name)
			require.Equal(t, test.vecs[0].Col, b.Vecs[0].Col)
		}
	}
}

func TestCompactUniqueKeyBatch(t *testing.T) {
	proc := testutil.NewProcess()
	tests := []struct {
		vecs  []*vector.Vector
		attrs []string
		f     *plan.Field
		proc  *process.Process
	}{
		{
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
			},
			attrs: []string{"a", "b", "c"},
			f: &plan.Field{
				Parts: []string{
					"a", "b", "c",
				},
				Cols: []*plan.ColDef{
					{
						Name: catalog.IndexTableIndexColName,
						Typ:  &plan.Type{Id: int32(types.T_varchar), Size: types.VarlenaSize},
					},
				},
			},
			proc: proc,
		},
		{
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
			},
			attrs: []string{"a", "b", "c"},
			f: &plan.Field{
				Parts: []string{
					"b",
				},
				Cols: []*plan.ColDef{
					{
						Name: catalog.IndexTableIndexColName,
						Typ:  &plan.Type{Id: int32(types.T_int64)},
					},
				},
			},
			proc: proc,
		},
	}
	for _, test := range tests {
		nulls.Add(test.vecs[1].Nsp, 1)
		if JudgeIsCompositeIndexColumn(test.f) {
			b, _ := BuildUniqueKeyBatch(test.vecs, test.attrs, test.f.Parts, "", test.proc)
			require.Equal(t, 2, b.Vecs[0].Length())
		} else {
			b, _ := BuildUniqueKeyBatch(test.vecs, test.attrs, test.f.Parts, "", test.proc)
			require.Equal(t, 2, b.Vecs[0].Length())
		}
	}
}
