// Copyright 2021 - 2024 Matrix Origin
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

package external

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

var vecBlockTestOids = []types.T{types.T_array_float8, types.T_array_float4}

func TestParquetStringToVecBlock(t *testing.T) {
	proc := testutil.NewProc(t)
	for _, oid := range vecBlockTestOids {
		t.Run(oid.String(), func(t *testing.T) {
			f, page := writeColumnAndGetPage(t, parquet.Optional(parquet.String()), []parquet.Row{
				{parquet.ByteArrayValue([]byte("[1, -3, 0, 6]")).Level(0, 1, 0)},
				{parquet.NullValue().Level(0, 0, 0)},
				{parquet.ByteArrayValue([]byte(" [0.5,0.5,0.5,0.5] ")).Level(0, 1, 0)},
			})
			vec := vector.NewVec(types.New(oid, 4, 0))
			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(oid), Width: 4})
			require.NotNil(t, mp)
			require.NoError(t, mp.mapping(page, proc, vec))
			require.Equal(t, 3, vec.Length())
			require.Equal(t, "[1, -3, 0, 6]", vec.RowToString(0))
			require.Equal(t, "null", vec.RowToString(1))
			require.Equal(t, "[0.5, 0.5, 0.5, 0.5]", vec.RowToString(2))

			// declared dimension mismatch
			mp = h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(oid), Width: 3})
			require.NotNil(t, mp)
			require.Error(t, mp.mapping(page, proc, vector.NewVec(types.New(oid, 3, 0))))
		})
	}
}

func TestParquetListToVecBlock(t *testing.T) {
	proc := testutil.NewProc(t)
	for _, leaf := range []struct {
		name string
		typ  parquet.Type
		val  func(float32) parquet.Value
	}{
		{"float", parquet.FloatType, func(x float32) parquet.Value { return parquet.FloatValue(x) }},
		{"double", parquet.DoubleType, func(x float32) parquet.Value { return parquet.DoubleValue(float64(x)) }},
	} {
		for _, oid := range vecBlockTestOids {
			t.Run(leaf.name+"_"+oid.String(), func(t *testing.T) {
				listRow := func(xs ...float32) parquet.Row {
					row := make(parquet.Row, len(xs))
					for i, x := range xs {
						rep := 1
						if i == 0 {
							rep = 0
						}
						row[i] = leaf.val(x).Level(rep, 1, 0)
					}
					return row
				}
				f, page := writeListAndGetPage(t, parquet.Leaf(leaf.typ), []parquet.Row{
					listRow(1, -3, 0, 6),
					listRow(0.5, 0.5, 0.5, 0.5),
				})
				var h ParquetHandler
				_, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(oid), Width: 4})
				require.NotNil(t, mp)
				vec := vector.NewVec(types.New(oid, 4, 0))
				require.NoError(t, mp.mapper(mp, page, proc, vec))
				require.Equal(t, 2, vec.Length())
				require.Equal(t, "[1, -3, 0, 6]", vec.RowToString(0))
				require.Equal(t, "[0.5, 0.5, 0.5, 0.5]", vec.RowToString(1))
				c, err := types.ParseBlockScaledCell(vec.GetBytesAt(0))
				require.NoError(t, err)
				wantFormat, _ := oid.BlockScaledFormat()
				require.Equal(t, wantFormat, c.Format)

				// declared dimension mismatch rolls back
				_, mp = h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(oid), Width: 3})
				require.NotNil(t, mp)
				bad := vector.NewVec(types.New(oid, 3, 0))
				require.Error(t, mp.mapper(mp, page, proc, bad))
				require.Equal(t, 0, bad.Length())
			})
		}
	}
}

func TestParquetListToVecBlockRejectsIntLeaf(t *testing.T) {
	f, _ := writeListAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Row{
		{parquet.Int32Value(1).Level(0, 1, 0)},
	})
	var h ParquetHandler
	for _, oid := range vecBlockTestOids {
		_, mp := h.getNestedListMapper(f.Root().Column("c"), plan.Type{Id: int32(oid), Width: 1})
		require.Nil(t, mp, oid.String())
	}
}

func TestIsLegalLineVecBlock(t *testing.T) {
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV}}
	for _, oid := range vecBlockTestOids {
		cols := []*plan.ColDef{{Name: "v", Typ: plan.Type{Id: int32(oid), Width: 3}}}
		require.True(t, isLegalLine(param, cols, []csvparser.Field{{Val: "[1,2,3]"}}))
		require.False(t, isLegalLine(param, cols, []csvparser.Field{{Val: "[1,2"}}))
		require.False(t, isLegalLine(param, cols, []csvparser.Field{{Val: "[1,2,nan]"}}))
	}
}

func TestJSONLineVecBlockExactText(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	for _, oid := range vecBlockTestOids {
		t.Run(oid.String(), func(t *testing.T) {
			f, _ := oid.BlockScaledFormat()
			cell, err := types.StringToBlockScaled(f, "[8.7649145,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5.7432985,-1e-3]")
			require.NoError(t, err)
			text, err := types.BlockScaledToJSON(cell)
			require.NoError(t, err)

			attrs := []plan.ExternAttr{{ColName: "v", ColIndex: 0, ColFieldIndex: 0}}
			cols := []*plan.ColDef{{Name: "v", Typ: plan.Type{Id: int32(oid), Width: 18}}}
			r := &CsvReader{}
			obj, err := r.transJson2Lines(proc.Ctx, `{"v":`+text+`}`, attrs, cols, tree.OBJECT)
			require.NoError(t, err)
			arr, err := r.transJsonArray2Lines(proc.Ctx, `[`+text+`]`, attrs, cols)
			require.NoError(t, err)
			for _, line := range [][]csvparser.Field{obj, arr} {
				got, err := types.StringToBlockScaled(f, line[0].Val)
				require.NoError(t, err)
				require.Equal(t, cell, got)
			}
		})
	}
}
