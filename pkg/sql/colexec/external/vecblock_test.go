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
	"bytes"
	"flag"
	"os"
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

func TestParquetStringExactTextToVecBlock(t *testing.T) {
	proc := testutil.NewProc(t)
	for _, oid := range vecBlockTestOids {
		t.Run(oid.String(), func(t *testing.T) {
			bf, _ := oid.BlockScaledFormat()
			cell, err := types.StringToBlockScaled(bf, "[0.44547153, 1.7, -3.1, 0.02]")
			require.NoError(t, err)
			text, err := types.BlockScaledToJSON(cell)
			require.NoError(t, err)
			f, page := writeColumnAndGetPage(t, parquet.Optional(parquet.String()), []parquet.Row{
				{parquet.ByteArrayValue([]byte(text)).Level(0, 1, 0)},
				{parquet.NullValue().Level(0, 0, 0)},
			})
			vec := vector.NewVec(types.New(oid, 4, 0))
			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(oid), Width: 4})
			require.NotNil(t, mp)
			require.NoError(t, mp.mapping(page, proc, vec))
			require.Equal(t, 2, vec.Length())
			require.Equal(t, cell, vec.GetBytesAt(0))
			require.True(t, vec.IsNull(1))

			// declared dimension mismatch
			mp = h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(oid), Width: 3})
			require.NotNil(t, mp)
			require.Error(t, mp.mapping(page, proc, vector.NewVec(types.New(oid, 3, 0))))
		})
	}
}

// TestParquetBinaryToVecBlock checks a column without a logical type: each value is binary,
// the stored cell as is or little-endian float32 elements quantized, as a BLOB casts.
func TestParquetBinaryToVecBlock(t *testing.T) {
	proc := testutil.NewProc(t)
	values := []float32{0.44547153, 1.7, -3.1, 0.02}
	for _, oid := range vecBlockTestOids {
		t.Run(oid.String(), func(t *testing.T) {
			bf, _ := oid.BlockScaledFormat()
			cell, err := types.AppendBlockScaled(nil, bf, values)
			require.NoError(t, err)
			for _, node := range []parquet.Node{
				parquet.Optional(parquet.Leaf(parquet.ByteArrayType)),
				parquet.Optional(parquet.Leaf(parquet.FixedLenByteArrayType(len(cell)))),
			} {
				rows := []parquet.Row{
					{parquet.ByteArrayValue(cell).Level(0, 1, 0)},
					{parquet.NullValue().Level(0, 0, 0)},
				}
				if node.Type().Kind() == parquet.ByteArray {
					rows = append(rows, parquet.Row{parquet.ByteArrayValue(types.ArrayToBytes(values)).Level(0, 1, 0)})
				}
				f, page := writeColumnAndGetPage(t, node, rows)
				vec := vector.NewVec(types.New(oid, 4, 0))
				var h ParquetHandler
				mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(oid), Width: 4})
				require.NotNil(t, mp)
				require.NoError(t, mp.mapping(page, proc, vec))
				require.Equal(t, len(rows), vec.Length())
				require.Equal(t, cell, vec.GetBytesAt(0))
				require.True(t, vec.IsNull(1))
				if len(rows) > 2 {
					require.Equal(t, cell, vec.GetBytesAt(2))
				}
				// text is not read from a binary column
				mp = h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(oid), Width: 3})
				require.NotNil(t, mp)
				require.Error(t, mp.mapping(page, proc, vector.NewVec(types.New(oid, 3, 0))))
			}
		})
	}
}

var updateVecBlockParquet = flag.Bool("update-vecblock-parquet", false, "rewrite the vecf8/vecf4 Parquet BVT resource")

const vecBlockParquetResource = "../../../../test/distributed/resources/parquet/vecblock.parquet"

type vecBlockParquetRow struct {
	ID     int32     `parquet:"id"`
	V4Bin  *[]byte   `parquet:"v4_bin,optional"`
	V8Bin  *[]byte   `parquet:"v8_bin,optional"`
	V4F32  *[]byte   `parquet:"v4_f32,optional"`
	V4JSON *string   `parquet:"v4_json,optional"`
	V8Text *string   `parquet:"v8_text,optional"`
	V4List []float32 `parquet:"v4_list,optional,list"`
	V8List []float64 `parquet:"v8_list,optional,list"`
}

// vecBlockParquetValues are the source rows of the resource: vecf4(17) and vecf8(33)
// values, and a row of NULLs. The list columns repeat the first row in place of the NULL
// row: the writer stores a nil list as an empty list.
func vecBlockParquetValues() (v4, v8 [][]float32) {
	v4 = [][]float32{
		{8.7649145, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5.7432985},
		{0.5, -1, 1.5, -2, 3, -4, 6, 0.25, 0.1, -0.3, 2.5, 7, -9, 12, 0, 1e-3, 100},
		nil,
	}
	v8 = make([][]float32, 3)
	for r := 0; r < 2; r++ {
		v8[r] = make([]float32, 33)
		for i := range v8[r] {
			v8[r][i] = float32(i-16) * float32(r+1) * 0.37
		}
	}
	return v4, v8
}

// TestVecBlockParquetResource checks the BVT resource vecblock.parquet: each column loads
// into vecf4(17) / vecf8(33) as the cell its source values quantize to. -update-vecblock-parquet
// rewrites it.
func TestVecBlockParquetResource(t *testing.T) {
	v4, v8 := vecBlockParquetValues()
	cell := func(f types.BlockScaledFormat, v []float32) []byte {
		if v == nil {
			return nil
		}
		c, err := types.AppendBlockScaled(nil, f, v)
		require.NoError(t, err)
		return c
	}
	if *updateVecBlockParquet {
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[vecBlockParquetRow](&buf)
		for r := range v4 {
			row := vecBlockParquetRow{ID: int32(r + 1)}
			row.V4List, row.V8List = v4[0], make([]float64, len(v8[0]))
			for i, x := range v8[0] {
				row.V8List[i] = float64(x)
			}
			if v4[r] != nil {
				c4, c8 := cell(types.BlockScaledNVFP4, v4[r]), cell(types.BlockScaledMXFP8, v8[r])
				j, err := types.BlockScaledToJSON(c4)
				require.NoError(t, err)
				text := types.ArrayToString(v8[r])
				f32 := types.ArrayToBytes(v4[r])
				row.V4Bin, row.V8Bin, row.V4F32 = &c4, &c8, &f32
				row.V4JSON, row.V8Text, row.V4List = &j, &text, v4[r]
				for i, x := range v8[r] {
					row.V8List[i] = float64(x)
				}
			}
			_, err := w.Write([]vecBlockParquetRow{row})
			require.NoError(t, err)
		}
		require.NoError(t, w.Close())
		require.NoError(t, os.WriteFile(vecBlockParquetResource, buf.Bytes(), 0o644))
	}

	data, err := os.ReadFile(vecBlockParquetResource)
	require.NoError(t, err)
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	proc := testutil.NewProc(t)
	list4 := [][]float32{v4[0], v4[1], v4[0]}
	list8 := [][]float32{v8[0], v8[1], v8[0]}
	for _, c := range []struct {
		name   string
		oid    types.T
		dim    int32
		values [][]float32
	}{
		{"v4_bin", types.T_array_float4, 17, v4},
		{"v8_bin", types.T_array_float8, 33, v8},
		{"v4_f32", types.T_array_float4, 17, v4},
		{"v4_json", types.T_array_float4, 17, v4},
		{"v8_text", types.T_array_float8, 33, v8},
		{"v4_list", types.T_array_float4, 17, list4},
		{"v8_list", types.T_array_float8, 33, list8},
	} {
		col := f.Root().Column(c.name)
		require.NotNil(t, col, c.name)
		var h ParquetHandler
		typ := plan.Type{Id: int32(c.oid), Width: c.dim}
		vec := vector.NewVec(types.New(c.oid, c.dim, 0))
		if col.Leaf() {
			mp := h.getMapper(col, typ)
			require.NotNil(t, mp, c.name)
			pages := col.Pages()
			page, err := pages.ReadPage()
			require.NoError(t, err, c.name)
			require.NoError(t, mp.mapping(page, proc, vec), c.name)
			require.NoError(t, pages.Close())
		} else {
			_, mp := h.getNestedListMapper(col, typ)
			require.NotNil(t, mp, c.name)
			leaf, ok := parquetListElementLeaf(col)
			require.True(t, ok, c.name)
			pages := leaf.Pages()
			page, err := pages.ReadPage()
			require.NoError(t, err, c.name)
			require.NoError(t, mp.mapper(mp, page, proc, vec), c.name)
			require.NoError(t, pages.Close())
		}
		require.Equal(t, len(c.values), vec.Length(), c.name)
		bf, _ := c.oid.BlockScaledFormat()
		for r, v := range c.values {
			if v == nil {
				require.True(t, vec.IsNull(uint64(r)), "%s row %d", c.name, r)
				continue
			}
			require.Equal(t, cell(bf, v), vec.GetBytesAt(r), "%s row %d", c.name, r)
		}
		vec.Free(proc.Mp())
	}
}
