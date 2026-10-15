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
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
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

func TestJSONLineNullRemainsNullWithParallelLoad(t *testing.T) {
	for _, tc := range []struct {
		name     string
		jsonData string
		line     string
	}{
		{name: "object", jsonData: tree.OBJECT, line: `{"d":null}`},
		{name: "array", jsonData: tree.ARRAY, line: `[null]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()

			attrs := []plan.ExternAttr{{ColName: "d", ColIndex: 0, ColFieldIndex: 0}}
			cols := []*plan.ColDef{{Name: "d", Typ: plan.Type{Id: int32(types.T_int32)}}}
			reader := &CsvReader{}
			line, err := reader.transJson2Lines(proc.Ctx, tc.line, attrs, cols, tc.jsonData)
			require.NoError(t, err)
			require.Equal(t, []csvparser.Field{{IsNull: true}}, line)

			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			defer bat.Clean(proc.Mp())

			param := &ExternalParam{
				ExParamConst: ExParamConst{
					ParallelLoad:  true,
					ColumnListLen: 1,
					Attrs:         attrs,
					Cols:          cols,
					Extern: &tree.ExternParam{
						ExParamConst: tree.ExParamConst{Format: tree.JSONLINE},
						ExParam:      tree.ExParam{ExternType: int32(plan.ExternType_LOAD), JsonData: tc.jsonData},
					},
				},
				ExParam: ExParam{Fileparam: &ExFileparam{}},
			}

			require.NoError(t, getOneRowData(proc, bat, line, 0, param))
			require.True(t, bat.Vecs[0].GetNulls().Contains(0))
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

func TestJSONLineVectorArrayValues(t *testing.T) {
	for _, shape := range []string{tree.OBJECT, tree.ARRAY} {
		for _, tc := range []struct {
			typ  types.T
			want []byte
		}{
			{types.T_array_float32, types.ArrayToBytes([]float32{4, 5, 6})},
			{types.T_array_float64, types.ArrayToBytes([]float64{4, 5, 6})},
			{types.T_array_bf16, types.ArrayToBytes([]types.BF16{types.BF16FromFloat32(4), types.BF16FromFloat32(5), types.BF16FromFloat32(6)})},
			{types.T_array_float16, types.ArrayToBytes([]types.Float16{types.Float16FromFloat32(4), types.Float16FromFloat32(5), types.Float16FromFloat32(6)})},
			{types.T_array_int8, types.ArrayToBytes([]int8{4, 5, 6})},
			{types.T_array_uint8, types.ArrayToBytes([]uint8{4, 5, 6})},
		} {
			t.Run(shape+"/"+tc.typ.String(), func(t *testing.T) {
				for _, value := range []string{`[4,5,6]`, `"[4,5,6]"`, `null`} {
					t.Run(value, func(t *testing.T) {
						proc := testutil.NewProcess(t)
						defer proc.Free()
						fieldIdx := int32(0)
						input := "[" + value + "]"
						if shape == tree.OBJECT {
							// The target is projected from the third physical field.
							fieldIdx = 2
							input = `{"unused":0,"other":"text","v":` + value + "}"
						}
						attrs := []plan.ExternAttr{{ColName: "v", ColIndex: 0, ColFieldIndex: fieldIdx}}
						cols := []*plan.ColDef{{Name: "v", Typ: plan.Type{Id: int32(tc.typ), Width: 3}}}
						line, err := (&CsvReader{}).transJson2Lines(proc.Ctx, input, attrs, cols, shape)
						require.NoError(t, err)
						bat := batch.NewWithSize(1)
						bat.Vecs[0] = vector.NewVec(types.New(tc.typ, 3, 0))
						defer bat.Clean(proc.Mp())
						param := &ExternalParam{
							ExParamConst: ExParamConst{
								Ctx: proc.Ctx, Attrs: attrs, Cols: cols, ColumnListLen: fieldIdx + 1, StrictSqlMode: true,
								Extern: &tree.ExternParam{
									ExParamConst: tree.ExParamConst{Format: tree.JSONLINE},
									ExParam:      tree.ExParam{ExternType: int32(plan.ExternType_EXTERNAL_TB), JsonData: shape},
								},
							},
							ExParam: ExParam{Fileparam: &ExFileparam{}},
						}
						require.NoError(t, getOneRowData(proc, bat, line, 0, param))
						require.Equal(t, 1, bat.Vecs[0].Length())
						if value == "null" {
							require.True(t, bat.Vecs[0].GetNulls().Contains(0))
						} else {
							require.False(t, bat.Vecs[0].GetNulls().Contains(0))
							require.Equal(t, tc.want, bat.Vecs[0].GetBytesAt(0))
						}
					})
				}
			})
		}
	}
}

func TestJSONLineVectorArrayLexemesAndInvalidElements(t *testing.T) {
	for _, shape := range []string{tree.OBJECT, tree.ARRAY} {
		t.Run(shape, func(t *testing.T) {
			attrs := []plan.ExternAttr{{ColName: "v", ColIndex: 0, ColFieldIndex: 0}}
			cols := []*plan.ColDef{{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float64)}}}
			reader := &CsvReader{}
			convert := func(value string) ([]csvparser.Field, error) {
				input := "[" + value + "]"
				if shape == tree.OBJECT {
					input = `{"v":` + value + "}"
				}
				return reader.transJson2Lines(context.Background(), input, attrs, cols, shape)
			}
			// Numeric text must not pass through a float64 intermediate.
			for _, value := range []string{`[-1.5,0,2.5e+2]`, `[1.0000000596046447753906250000000001]`, `[1e400]`, `[]`} {
				line, err := convert(value)
				require.NoError(t, err)
				require.Equal(t, value, line[0].Val)
			}
			for _, value := range []string{`["4"]`, `["NaN"]`, `[true]`, `[null]`, `[[4,5]]`, `[{"x":4}]`, `[4,"5",6]`} {
				t.Run(value, func(t *testing.T) {
					_, err := convert(value)
					require.Error(t, err)
					require.Empty(t, reader.prevStr)
					line, err := convert(`[4,5,6]`)
					require.NoError(t, err)
					require.Equal(t, "[4,5,6]", line[0].Val)
				})
			}
		})
	}
}

func TestJSONLineVectorArrayConsumerErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   types.T
		value string
		width int32
	}{
		{"empty", types.T_array_float32, `[]`, 3},
		{"dimension", types.T_array_float64, `[4,5]`, 3},
		{"float_overflow", types.T_array_float32, `[1e400]`, 1},
		{"int8_overflow", types.T_array_int8, `[128]`, 1},
		{"uint8_negative", types.T_array_uint8, `[-1]`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			attrs := []plan.ExternAttr{{ColName: "v", ColIndex: 0, ColFieldIndex: 0}}
			cols := []*plan.ColDef{{Name: "v", Typ: plan.Type{Id: int32(tc.typ), Width: tc.width}}}
			line, err := (&CsvReader{}).transJsonObject2Lines(proc.Ctx, `{"v":`+tc.value+`}`, attrs, cols)
			require.NoError(t, err) // Type/range/dimension validation belongs to the consumer.
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vector.NewVec(types.New(tc.typ, tc.width, 0))
			defer bat.Clean(proc.Mp())
			param := &ExternalParam{
				ExParamConst: ExParamConst{
					Ctx: proc.Ctx, Attrs: attrs, Cols: cols, ColumnListLen: 1, StrictSqlMode: true,
					Extern: &tree.ExternParam{
						ExParamConst: tree.ExParamConst{Format: tree.JSONLINE},
						ExParam:      tree.ExParam{ExternType: int32(plan.ExternType_EXTERNAL_TB)},
					},
				},
				ExParam: ExParam{Fileparam: &ExFileparam{}},
			}
			require.Error(t, getOneRowData(proc, bat, line, 0, param))
			require.Zero(t, bat.Vecs[0].Length())
		})
	}
}

func TestJSONLineVectorArrayErrorModeRecovery(t *testing.T) {
	for _, shape := range []string{tree.OBJECT, tree.ARRAY} {
		t.Run(shape, func(t *testing.T) {
			param, proc, bat := errorModeParam(t, tree.JSONLINE, shape, numTestCols)
			param.Cols[1].Typ = plan.Type{Id: int32(types.T_array_float32), Width: 3}
			bat.Vecs[1].Free(proc.Mp())
			bat.Vecs[1] = vector.NewVec(types.New(types.T_array_float32, 3, 0))
			invalid := `{"a":1,"s":[4,"5",6]}`
			wrongDim := `{"a":2,"s":[4,5]}`
			valid := `{"a":3,"s":[4,5,6]}`
			if shape == tree.ARRAY {
				invalid, wrongDim, valid = `[1,[4,"5",6]]`, `[2,[4,5]]`, `[3,[4,5,6]]`
			}
			require.NoError(t, readAllText(t, param, proc, bat, invalid+"\n"+wrongDim+"\n"+valid+"\n"))
			for _, vec := range bat.Vecs {
				require.Equal(t, 3, vec.Length())
			}
			for row, raw := range []string{invalid, wrongDim} {
				require.True(t, bat.Vecs[0].GetNulls().Contains(uint64(row)))
				require.True(t, bat.Vecs[1].GetNulls().Contains(uint64(row)))
				require.NotEmpty(t, bat.Vecs[3].GetBytesAt(row))
				require.Equal(t, raw, string(bat.Vecs[4].GetBytesAt(row)))
			}
			require.Equal(t, int32(3), vector.GetFixedAtNoTypeCheck[int32](bat.Vecs[0], 2))
			require.Equal(t, []float32{4, 5, 6}, types.BytesToArray[float32](bat.Vecs[1].GetBytesAt(2)))
			require.True(t, bat.Vecs[3].GetNulls().Contains(2))
			require.True(t, bat.Vecs[4].GetNulls().Contains(2))
		})
	}
}
