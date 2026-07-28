// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJSONLineObjectProjectionUsesPhysicalFieldIndex(t *testing.T) {
	const jsonLine = `{"a":"one","b":"two","c":"three","d":"four","e":"five","f":"six"}`
	columns := []string{"a", "b", "c", "d", "e", "f"}
	values := []string{"one", "two", "three", "four", "five", "six"}

	for fieldIdx, column := range columns {
		t.Run(column, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()

			attrs := []plan.ExternAttr{{
				ColName:       column,
				ColIndex:      0,
				ColFieldIndex: int32(fieldIdx),
			}}
			cols := []*plan.ColDef{{
				Name: column,
				Typ:  plan.Type{Id: int32(types.T_varchar)},
			}}
			reader := &CsvReader{}
			line, err := reader.transJsonObject2Lines(proc.Ctx, jsonLine, attrs, cols)
			require.NoError(t, err)
			require.Len(t, line, fieldIdx+1)

			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			defer bat.Clean(proc.Mp())

			param := &ExternalParam{
				ExParamConst: ExParamConst{
					ColumnListLen: int32(len(columns)),
					StrictSqlMode: true,
					Attrs:         attrs,
					Cols:          cols,
					Extern: &tree.ExternParam{
						ExParamConst: tree.ExParamConst{Format: tree.JSONLINE},
						ExParam:      tree.ExParam{ExternType: int32(plan.ExternType_EXTERNAL_TB)},
					},
				},
				ExParam: ExParam{Fileparam: &ExFileparam{}},
			}

			require.NoError(t, getOneRowData(proc, bat, line, 0, param))
			require.Equal(t, values[fieldIdx], string(bat.Vecs[0].GetBytesAt(0)))
		})
	}
}

func TestJSONLineObjectProjectionSkipsExternalFilepath(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	attrs := []plan.ExternAttr{
		{ColName: "c", ColIndex: 0, ColFieldIndex: 2},
		{ColName: catalog.ExternalFilePath, ColIndex: 1},
	}
	cols := []*plan.ColDef{
		{Name: "c", Typ: plan.Type{Id: int32(types.T_varchar)}},
		{Name: catalog.ExternalFilePath, Typ: plan.Type{Id: int32(types.T_varchar)}},
	}
	reader := &CsvReader{}
	line, err := reader.transJsonObject2Lines(proc.Ctx, `{"a":"one","b":"two","c":"three"}`, attrs, cols)
	require.NoError(t, err)
	require.Len(t, line, 3)

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	defer bat.Clean(proc.Mp())

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			ColumnListLen: 3,
			StrictSqlMode: true,
			Attrs:         attrs,
			Cols:          cols,
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{Format: tree.JSONLINE},
				ExParam:      tree.ExParam{ExternType: int32(plan.ExternType_EXTERNAL_TB)},
			},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{Filepath: "/tmp/rows.jsonl"}},
	}

	require.NoError(t, getOneRowData(proc, bat, line, 0, param))
	require.Equal(t, "three", string(bat.Vecs[0].GetBytesAt(0)))
	require.Equal(t, "/tmp/rows.jsonl", string(bat.Vecs[1].GetBytesAt(0)))
}

func TestJSONLineObjectProjectionMissingKey(t *testing.T) {
	attrs := []plan.ExternAttr{{ColName: "c", ColIndex: 0, ColFieldIndex: 2}}
	cols := []*plan.ColDef{{Name: "c", Typ: plan.Type{Id: int32(types.T_varchar)}}}

	_, err := (&CsvReader{}).transJsonObject2Lines(
		context.Background(), `{"a":"one","b":"two"}`, attrs, cols)
	require.ErrorContains(t, err, "the attr c is not in json")
}

func TestJSONLineObjectProjectionRejectsInvalidPhysicalFieldIndex(t *testing.T) {
	cols := []*plan.ColDef{{Name: "c", Typ: plan.Type{Id: int32(types.T_varchar)}}}
	for _, fieldIdx := range []int32{-1, plan2.TableColumnCountLimit} {
		attrs := []plan.ExternAttr{{ColName: "c", ColIndex: 0, ColFieldIndex: fieldIdx}}
		_, err := (&CsvReader{}).transJsonObject2Lines(
			context.Background(), `{"c":"three"}`, attrs, cols)
		require.ErrorContains(t, err, "invalid external field index")
	}
}

func TestJSONLineObjectProjectionRejectsMissingColumnDefinition(t *testing.T) {
	attrs := []plan.ExternAttr{{ColName: "c", ColIndex: 0, ColFieldIndex: 2}}
	_, err := (&CsvReader{}).transJsonObject2Lines(
		context.Background(), `{"c":"three"}`, attrs, nil)
	require.ErrorContains(t, err, "missing external column definition")
}
