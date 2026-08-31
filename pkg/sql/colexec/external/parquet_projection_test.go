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

package external

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestParquetNestedProjectionPrunesUnprojectedColumns(t *testing.T) {
	data := writeNestedProjectionParquet(t, 2, 64)
	param := nestedProjectionParam(data)
	proc := testutil.NewProc(t)
	r := NewParquetReader(param, proc)

	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	defer r.Close()

	require.Equal(t, [][]string{{"z_nested", "v"}}, r.h.rowReader.Schema().Columns())

	bat := vectorBatch([]types.Type{types.T_text.ToType()})
	defer bat.Clean(proc.Mp())
	finished, err := r.ReadBatch(context.Background(), bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 2, bat.RowCount())
	require.JSONEq(t, `{"v":0}`, bat.Vecs[0].GetStringAt(0))
	require.JSONEq(t, `{"v":1}`, bat.Vecs[0].GetStringAt(1))
}

func TestParquetNestedAndScalarProjectionPrunesOtherColumns(t *testing.T) {
	data := writeNestedProjectionParquet(t, 2, 64)
	param := nestedAndScalarProjectionParam(data)
	proc := testutil.NewProc(t)
	r := NewParquetReader(param, proc)

	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)
	defer r.Close()

	require.Equal(t, [][]string{{"a_unused_000"}, {"z_nested", "v"}}, r.h.rowReader.Schema().Columns())
	for _, pages := range r.h.pages {
		require.Nil(t, pages)
	}

	bat := vectorBatch([]types.Type{types.T_int32.ToType(), types.T_text.ToType()})
	defer bat.Clean(proc.Mp())
	finished, err := r.ReadBatch(context.Background(), bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, []int32{0, 0}, vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0]))
	require.JSONEq(t, `{"v":0}`, bat.Vecs[1].GetStringAt(0))
	require.JSONEq(t, `{"v":1}`, bat.Vecs[1].GetStringAt(1))
}

func BenchmarkParquetProjectedNestedColumn(b *testing.B) {
	for _, unprojectedColumns := range []int{0, 8, 32, 64} {
		b.Run(fmt.Sprintf("unprojected=%d", unprojectedColumns), func(b *testing.B) {
			data := writeNestedProjectionParquet(b, 2048, unprojectedColumns)
			param := nestedProjectionParam(data)
			proc := testutil.NewProc(b)

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r := NewParquetReader(param, proc)
				fileEmpty, err := r.Open(param, proc)
				if err != nil {
					b.Fatal(err)
				}
				if fileEmpty {
					b.Fatal("projected parquet benchmark file is empty")
				}

				bat := vectorBatch([]types.Type{types.T_text.ToType()})
				finished, err := r.ReadBatch(context.Background(), bat, proc, nil)
				if err != nil {
					b.Fatal(err)
				}
				if !finished || bat.RowCount() != 2048 {
					b.Fatalf("unexpected scan result: finished=%v rows=%d", finished, bat.RowCount())
				}
				if err := r.Close(); err != nil {
					b.Fatal(err)
				}
				bat.Clean(proc.Mp())
			}
		})
	}
}

func writeNestedProjectionParquet(tb testing.TB, rowCount, unprojectedColumns int) []byte {
	tb.Helper()

	group := make(parquet.Group, unprojectedColumns+1)
	for i := 0; i < unprojectedColumns; i++ {
		group[fmt.Sprintf("a_unused_%03d", i)] = parquet.Leaf(parquet.Int32Type)
	}
	group["z_nested"] = parquet.Group{"v": parquet.Leaf(parquet.Int32Type)}
	schema := parquet.NewSchema("projection", group)

	rows := make([]parquet.Row, rowCount)
	for rowIndex := range rows {
		row := make(parquet.Row, 0, unprojectedColumns+1)
		for columnIndex := 0; columnIndex < unprojectedColumns; columnIndex++ {
			row = append(row, parquet.Int32Value(int32(columnIndex)).Level(0, 0, columnIndex))
		}
		row = append(row, parquet.Int32Value(int32(rowIndex)).Level(0, 0, unprojectedColumns))
		rows[rowIndex] = row
	}

	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	_, err := w.WriteRows(rows)
	require.NoError(tb, err)
	require.NoError(tb, w.Close())
	return buf.Bytes()
}

func nestedProjectionParam(data []byte) *ExternalParam {
	return &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Attrs:    []plan.ExternAttr{{ColName: "z_nested", ColIndex: 0}},
			Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_text)}}},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE, Format: tree.PARQUET, Data: string(data)}},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
}

func nestedAndScalarProjectionParam(data []byte) *ExternalParam {
	return &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: context.Background(),
			Attrs: []plan.ExternAttr{
				{ColName: "a_unused_000", ColIndex: 0},
				{ColName: "z_nested", ColIndex: 1},
			},
			Cols: []*plan.ColDef{
				{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}},
				{Typ: plan.Type{Id: int32(types.T_text)}},
			},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE, Format: tree.PARQUET, Data: string(data)}},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
}
