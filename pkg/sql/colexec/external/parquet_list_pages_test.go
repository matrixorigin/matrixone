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
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestParquetListVectorAcrossV1Pages(t *testing.T) {
	savedBatchCount := maxParquetBatchCnt
	maxParquetBatchCnt = 3
	t.Cleanup(func() { maxParquetBatchCnt = savedBatchCount })

	// Generated with PyArrow 12.0.0 and NumPy 1.26.4: 8 rows of 64
	// float64 values, data_page_version=1.0, write_batch_size=16,
	// data_page_size=128, compression=NONE, and dictionary encoding disabled.
	// The fixture is base64 encoded so it remains reviewable as repository text.
	encoded, err := os.ReadFile("testdata/list_double_cross_page_v1.parquet.base64")
	require.NoError(t, err)
	data, err := io.ReadAll(base64.NewDecoder(base64.StdEncoding, bytes.NewReader(encoded)))
	require.NoError(t, err)
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	pages := file.RowGroups()[0].ColumnChunks()[1].Pages()
	firstPage, err := pages.ReadPage()
	require.NoError(t, err)
	require.Equal(t, int64(1), firstPage.NumRows())
	require.Equal(t, int64(16), firstPage.NumValues())
	continuationPage, err := pages.ReadPage()
	require.NoError(t, err)
	require.Zero(t, continuationPage.NumRows())
	require.NotEmpty(t, continuationPage.RepetitionLevels())
	require.Equal(t, byte(1), continuationPage.RepetitionLevels()[0])
	require.NoError(t, pages.Close())

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:          context.Background(),
			maxBatchSize: 300,
			Attrs: []plan.ExternAttr{
				{ColName: "id", ColIndex: 0},
				{ColName: "emb", ColIndex: 1},
			},
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}, NotNull: true},
				{
					Name:    "emb",
					Typ:     plan.Type{Id: int32(types.T_array_float32), Width: 64},
					NotNull: true,
				},
			},
			Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{
				ScanType: tree.INLINE,
				Format:   tree.PARQUET,
				Data:     string(data),
			}},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	proc := testutil.NewProc(t)
	reader := NewParquetReader(param, proc)
	empty, err := reader.Open(param, proc)
	require.NoError(t, err)
	require.False(t, empty)
	defer reader.Close()

	gotIDs := make([]int64, 0, 8)
	gotVectors := make([][]float32, 0, 8)
	batches := 0
	for {
		bat := vectorBatch([]types.Type{types.T_int64.ToType(), types.New(types.T_array_float32, 64, 0)})
		finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
		require.NoError(t, err)
		batches++
		gotIDs = append(gotIDs, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])[:bat.RowCount()]...)
		for _, values := range vector.MustArrayCol[float32](bat.Vecs[1])[:bat.RowCount()] {
			gotVectors = append(gotVectors, append([]float32(nil), values...))
		}
		bat.Clean(proc.Mp())
		if finished {
			break
		}
	}

	require.Greater(t, batches, 3)
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7}, gotIDs)
	require.Len(t, gotVectors, 8)
	for row, values := range gotVectors {
		require.Len(t, values, 64)
		for column, value := range values {
			require.Equal(t, float32(row*1000+column), value)
		}
	}
}

func TestParquetListVectorRowModePreservesSiblingMappers(t *testing.T) {
	schema := parquet.NewSchema("mixed", parquet.Group{
		"amount":      parquet.Decimal(2, 12, parquet.Int64Type),
		"emb":         parquet.List(parquet.Leaf(parquet.DoubleType)),
		"event_date":  parquet.Optional(parquet.Date()),
		"flag_double": parquet.Leaf(parquet.BooleanType),
		"flag_float":  parquet.Leaf(parquet.BooleanType),
		"number":      parquet.String(),
		"payload":     parquet.JSON(),
		"text_vec":    parquet.String(),
	})
	makeRow := func(row int) parquet.Row {
		values := make(parquet.Row, 0, 9)
		for column, path := range schema.Columns() {
			switch path[0] {
			case "amount":
				values = append(values, parquet.Int64Value(int64(1234+row)).Level(0, 0, column))
			case "emb":
				values = append(values,
					parquet.DoubleValue(float64(row*10+1)).Level(0, 1, column),
					parquet.DoubleValue(float64(row*10+2)).Level(1, 1, column),
				)
			case "event_date":
				if row == 0 {
					values = append(values, parquet.Int32Value(1).Level(0, 1, column))
				} else {
					values = append(values, parquet.NullValue().Level(0, 0, column))
				}
			case "flag_double", "flag_float":
				values = append(values, parquet.BooleanValue(row == 0).Level(0, 0, column))
			case "number":
				values = append(values, parquet.ByteArrayValue([]byte(fmt.Sprint(41+row))).Level(0, 0, column))
			case "payload":
				values = append(values, parquet.ByteArrayValue([]byte(`{"row":`+fmt.Sprint(row)+`}`)).Level(0, 0, column))
			case "text_vec":
				values = append(values, parquet.ByteArrayValue([]byte(fmt.Sprintf("[%d,%d]", row+3, row+4))).Level(0, 0, column))
			}
		}
		return values
	}

	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf, schema)
	_, err := writer.WriteRows([]parquet.Row{makeRow(0), makeRow(1)})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: context.Background(),
			Attrs: []plan.ExternAttr{
				{ColName: "amount", ColIndex: 0},
				{ColName: "emb", ColIndex: 1},
				{ColName: "event_date", ColIndex: 2},
				{ColName: "flag_double", ColIndex: 3},
				{ColName: "flag_float", ColIndex: 4},
				{ColName: "number", ColIndex: 5},
				{ColName: "payload", ColIndex: 6},
				{ColName: "text_vec", ColIndex: 7},
			},
			Cols: []*plan.ColDef{
				{Name: "amount", Typ: plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 2, NotNullable: true}, NotNull: true},
				{Name: "emb", Typ: plan.Type{Id: int32(types.T_array_float32), Width: 2, NotNullable: true}, NotNull: true},
				{Name: "event_date", Typ: plan.Type{Id: int32(types.T_date)}},
				{Name: "flag_double", Typ: plan.Type{Id: int32(types.T_float64), NotNullable: true}, NotNull: true},
				{Name: "flag_float", Typ: plan.Type{Id: int32(types.T_float32), NotNullable: true}, NotNull: true},
				{Name: "number", Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}, NotNull: true},
				{Name: "payload", Typ: plan.Type{Id: int32(types.T_json), NotNullable: true}, NotNull: true},
				{Name: "text_vec", Typ: plan.Type{Id: int32(types.T_array_float32), Width: 2, NotNullable: true}, NotNull: true},
			},
			Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{
				ScanType: tree.INLINE,
				Format:   tree.PARQUET,
				Data:     buf.String(),
			}},
			FileSize: []int64{int64(buf.Len())},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	proc := testutil.NewProc(t)
	reader := NewParquetReader(param, proc)
	empty, err := reader.Open(param, proc)
	require.NoError(t, err)
	require.False(t, empty)
	defer reader.Close()

	bat := vectorBatch([]types.Type{
		types.New(types.T_decimal64, 12, 2),
		types.New(types.T_array_float32, 2, 0),
		types.T_date.ToType(),
		types.T_float64.ToType(),
		types.T_float32.ToType(),
		types.T_int32.ToType(),
		types.T_json.ToType(),
		types.New(types.T_array_float32, 2, 0),
	})
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, []types.Decimal64{1234, 1235}, vector.MustFixedColWithTypeCheck[types.Decimal64](bat.Vecs[0]))
	require.Equal(t, [][]float32{{1, 2}, {11, 12}}, vector.MustArrayCol[float32](bat.Vecs[1]))
	require.Equal(t, types.DaysFromUnixEpochToDate(1), vector.MustFixedColWithTypeCheck[types.Date](bat.Vecs[2])[0])
	require.True(t, bat.Vecs[2].GetNulls().Contains(1))
	require.Equal(t, []float64{1, 0}, vector.MustFixedColWithTypeCheck[float64](bat.Vecs[3]))
	require.Equal(t, []float32{1, 0}, vector.MustFixedColWithTypeCheck[float32](bat.Vecs[4]))
	require.Equal(t, []int32{41, 42}, vector.MustFixedColWithTypeCheck[int32](bat.Vecs[5]))
	require.Equal(t, `{"row": 0}`, types.DecodeJson(bat.Vecs[6].GetBytesAt(0)).String())
	require.Equal(t, `{"row": 1}`, types.DecodeJson(bat.Vecs[6].GetBytesAt(1)).String())
	require.Equal(t, [][]float32{{3, 4}, {4, 5}}, vector.MustArrayCol[float32](bat.Vecs[7]))
}

func TestParquetListVectorRowModeRollsBackWholeRow(t *testing.T) {
	schema := parquet.NewSchema("rollback", parquet.Group{
		"emb":     parquet.List(parquet.Leaf(parquet.DoubleType)),
		"payload": parquet.String(),
	})
	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf, schema)
	_, err := writer.WriteRows([]parquet.Row{{
		parquet.DoubleValue(1).Level(0, 1, 0),
		parquet.DoubleValue(2).Level(1, 1, 0),
		parquet.ByteArrayValue([]byte("not-json")).Level(0, 0, 1),
	}})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	param := inlineListVectorParam(buf.Bytes(), []plan.ExternAttr{
		{ColName: "emb", ColIndex: 0},
		{ColName: "payload", ColIndex: 1},
	}, []*plan.ColDef{
		{Name: "emb", Typ: plan.Type{Id: int32(types.T_array_float32), Width: 2, NotNullable: true}, NotNull: true},
		{Name: "payload", Typ: plan.Type{Id: int32(types.T_json), NotNullable: true}, NotNull: true},
	})
	proc := testutil.NewProc(t)
	reader := NewParquetReader(param, proc)
	_, err = reader.Open(param, proc)
	require.NoError(t, err)
	defer reader.Close()

	bat := vectorBatch([]types.Type{types.New(types.T_array_float32, 2, 0), types.T_json.ToType()})
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	_, err = reader.ReadBatch(context.Background(), bat, proc, nil)
	require.ErrorContains(t, err, "json text not-json")
	require.Zero(t, bat.Vecs[0].Length())
	require.Zero(t, bat.Vecs[1].Length())
}

func TestParquetListVectorRowModeEnforcesColDefNotNull(t *testing.T) {
	schema := parquet.NewSchema("not-null", parquet.Group{
		"emb": parquet.Optional(parquet.List(parquet.Leaf(parquet.DoubleType))),
	})
	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf, schema)
	_, err := writer.WriteRows([]parquet.Row{{parquet.NullValue().Level(0, 0, 0)}})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	param := inlineListVectorParam(buf.Bytes(),
		[]plan.ExternAttr{{ColName: "emb", ColIndex: 0}},
		[]*plan.ColDef{{
			Name:    "emb",
			Typ:     plan.Type{Id: int32(types.T_array_float32), Width: 2},
			NotNull: true,
		}},
	)
	proc := testutil.NewProc(t)
	reader := NewParquetReader(param, proc)
	_, err = reader.Open(param, proc)
	require.NoError(t, err)
	defer reader.Close()

	bat := vectorBatch([]types.Type{types.New(types.T_array_float32, 2, 0)})
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	_, err = reader.ReadBatch(context.Background(), bat, proc, nil)
	require.ErrorContains(t, err, "NOT NULL")
	require.Zero(t, bat.Vecs[0].Length())
}

func TestParquetListVectorRowModeRejectsMalformedNullRows(t *testing.T) {
	file, _ := writeListNodeAndGetPage(t,
		parquet.Optional(parquet.List(parquet.Leaf(parquet.DoubleType))),
		[]parquet.Row{{parquet.NullValue().Level(0, 0, 0)}},
	)
	col := file.Root().Column("c")
	handler := new(ParquetHandler)
	_, mapper := handler.getNestedListMapper(col, plan.Type{Id: int32(types.T_array_float32)})
	require.NotNil(t, mapper)
	proc := testutil.NewProc(t)

	tests := []struct {
		name    string
		row     parquet.Row
		wantErr string
	}{
		{
			name: "null marker followed by repeated value",
			row: parquet.Row{
				parquet.NullValue().Level(0, 0, 0),
				parquet.DoubleValue(1).Level(1, 2, 0),
			},
			wantErr: "NULL row has repeated values",
		},
		{
			name:    "missing list value",
			row:     parquet.Row{},
			wantErr: "1 rows but no values",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vec := vector.NewVec(types.T_array_float32.ToType())
			t.Cleanup(func() { vec.Free(proc.Mp()) })
			err := handler.processNestedValue(test.row, col, mapper, vec, &plan.ColDef{
				Name: "emb",
				Typ:  plan.Type{Id: int32(types.T_array_float32)},
			}, proc)
			require.ErrorContains(t, err, test.wantErr)
			require.Zero(t, vec.Length())
		})
	}
}

func TestParquetListVectorRowMapperSupportsAllElementFamilies(t *testing.T) {
	proc := testutil.NewProc(t)
	tests := []struct {
		name   string
		elem   parquet.Node
		values []parquet.Value
		target types.T
	}{
		{name: "float32", elem: parquet.Leaf(parquet.FloatType), target: types.T_array_float32,
			values: []parquet.Value{parquet.FloatValue(1), parquet.FloatValue(2)}},
		{name: "float64", elem: parquet.Leaf(parquet.DoubleType), target: types.T_array_float64,
			values: []parquet.Value{parquet.DoubleValue(1), parquet.DoubleValue(2)}},
		{name: "bf16", elem: parquet.Leaf(parquet.FloatType), target: types.T_array_bf16,
			values: []parquet.Value{parquet.FloatValue(1), parquet.FloatValue(2)}},
		{name: "float16", elem: parquet.Leaf(parquet.FloatType), target: types.T_array_float16,
			values: []parquet.Value{parquet.FloatValue(1), parquet.FloatValue(2)}},
		{name: "int8", elem: parquet.Leaf(parquet.Int32Type), target: types.T_array_int8,
			values: []parquet.Value{parquet.Int32Value(-1), parquet.Int32Value(2)}},
		{name: "uint8", elem: parquet.Leaf(parquet.Int32Type), target: types.T_array_uint8,
			values: []parquet.Value{parquet.Int32Value(1), parquet.Int32Value(2)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row := parquet.Row{
				test.values[0].Level(0, 1, 0),
				test.values[1].Level(1, 1, 0),
			}
			file, page := writeListAndGetPage(t, test.elem, []parquet.Row{row})
			_, mapper := new(ParquetHandler).getNestedListMapper(file.Root().Column("c"), plan.Type{
				Id:    int32(test.target),
				Width: 2,
			})
			require.NotNil(t, mapper)
			require.NotNil(t, mapper.listValuesMapper)
			values, err := readParquetPageAllValues(proc.Ctx, page)
			require.NoError(t, err)
			vec := vector.NewVec(types.New(test.target, 2, 0))
			t.Cleanup(func() { vec.Free(proc.Mp()) })
			require.NoError(t, mapper.listValuesMapper(mapper, values, 1, proc, vec))
			require.Equal(t, 1, vec.Length())
		})
	}
}

func TestParquetListVectorRowModeHonorsCancellation(t *testing.T) {
	schema := parquet.NewSchema("cancel", parquet.Group{
		"emb": parquet.List(parquet.Leaf(parquet.DoubleType)),
	})
	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf, schema)
	_, err := writer.WriteRows([]parquet.Row{{
		parquet.DoubleValue(1).Level(0, 1, 0),
		parquet.DoubleValue(2).Level(1, 1, 0),
	}})
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	param := inlineListVectorParam(buf.Bytes(),
		[]plan.ExternAttr{{ColName: "emb", ColIndex: 0}},
		[]*plan.ColDef{{
			Name:    "emb",
			Typ:     plan.Type{Id: int32(types.T_array_float32), Width: 2, NotNullable: true},
			NotNull: true,
		}},
	)
	proc := testutil.NewProc(t)
	canceledCtx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = canceledCtx
	reader := NewParquetReader(param, proc)
	_, err = reader.Open(param, proc)
	require.NoError(t, err)
	defer reader.Close()
	cancel()

	bat := vectorBatch([]types.Type{types.New(types.T_array_float32, 2, 0)})
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	_, err = reader.ReadBatch(proc.Ctx, bat, proc, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, bat.Vecs[0].Length())
}

func inlineListVectorParam(data []byte, attrs []plan.ExternAttr, cols []*plan.ColDef) *ExternalParam {
	return &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:   context.Background(),
			Attrs: attrs,
			Cols:  cols,
			Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{
				ScanType: tree.INLINE,
				Format:   tree.PARQUET,
				Data:     string(data),
			}},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
}
