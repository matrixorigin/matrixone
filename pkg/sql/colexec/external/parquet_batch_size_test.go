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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestParquetReaderBatchByteBudget(t *testing.T) {
	t.Run("variable width permits only one oversize row", func(t *testing.T) {
		values := make([]string, 8)
		for i := range values {
			values[i] = strings.Repeat(string(rune('a'+i)), 4<<10)
		}
		data := writeBatchBudgetStringParquet(t, values)
		reader, param, proc := newBatchBudgetParquetReader(t, data, types.T_text, 1<<10)
		defer reader.Close()

		var got []string
		var rowsPerBatch []int
		for !reader.h.isFinished() {
			bat := batchBudgetVectorBatch(types.T_text)
			finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
			require.NoError(t, err)
			rowsPerBatch = append(rowsPerBatch, bat.RowCount())
			require.Equal(t, 1, bat.RowCount())
			require.Greater(t, uint64(bat.Size()), param.maxBatchSize)
			got = append(got, bat.Vecs[0].GetStringAt(0))
			bat.Clean(proc.Mp())
			if finished {
				break
			}
		}

		require.Equal(t, values, got)
		require.Equal(t, []int{1, 1, 1, 1, 1, 1, 1, 1}, rowsPerBatch)
	})

	t.Run("fixed width uses the byte boundary rather than one row", func(t *testing.T) {
		values := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		data := writeBatchBudgetInt32Parquet(t, values)
		reader, _, proc := newBatchBudgetParquetReader(t, data, types.T_int32, 10)
		defer reader.Close()

		var got []int32
		var rowsPerBatch []int
		for {
			bat := batchBudgetVectorBatch(types.T_int32)
			finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
			require.NoError(t, err)
			rowsPerBatch = append(rowsPerBatch, bat.RowCount())
			got = append(got, vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])[:bat.RowCount()]...)
			bat.Clean(proc.Mp())
			if finished {
				break
			}
		}

		require.Equal(t, values, got)
		require.Equal(t, []int{2, 2, 2, 2, 2}, rowsPerBatch)
	})

	t.Run("nested row mode observes the same boundary", func(t *testing.T) {
		values := []string{
			strings.Repeat("x", 4<<10),
			strings.Repeat("y", 4<<10),
			strings.Repeat("z", 4<<10),
		}
		data := writeBatchBudgetNestedParquet(t, values)
		reader, param, proc := newBatchBudgetParquetReader(t, data, types.T_text, 1<<10)
		defer reader.Close()

		var total int
		for {
			bat := batchBudgetVectorBatch(types.T_text)
			finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
			require.NoError(t, err)
			require.Equal(t, 1, bat.RowCount())
			require.Greater(t, uint64(bat.Size()), param.maxBatchSize)
			total += bat.RowCount()
			bat.Clean(proc.Mp())
			if finished {
				break
			}
		}
		require.Equal(t, len(values), total)
	})

	t.Run("nested row mode does not publish a second budget-crossing row", func(t *testing.T) {
		values := []string{"a", "b", "c"}
		data := writeBatchBudgetNestedParquet(t, values)
		reader, param, proc := newBatchBudgetParquetReader(t, data, types.T_text, 47)
		defer reader.Close()

		var rowsPerBatch []int
		for {
			bat := batchBudgetVectorBatch(types.T_text)
			finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
			require.NoError(t, err)
			rowsPerBatch = append(rowsPerBatch, bat.RowCount())
			require.Equal(t, 1, bat.RowCount())
			require.LessOrEqual(t, uint64(bat.Size()), param.maxBatchSize)
			bat.Clean(proc.Mp())
			if finished {
				break
			}
		}
		require.Equal(t, []int{1, 1, 1}, rowsPerBatch)
	})

	t.Run("nested row mode seeks back after byte-boundary prefetch", func(t *testing.T) {
		values := []string{
			"",
			strings.Repeat("x", 4<<10),
			"tail-one",
			"tail-two",
		}
		data := writeBatchBudgetNestedParquet(t, values)
		reader, _, proc := newBatchBudgetParquetReader(t, data, types.T_text, 1<<10)
		defer reader.Close()

		var got []string
		for {
			bat := batchBudgetVectorBatch(types.T_text)
			finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
			require.NoError(t, err)
			for row := 0; row < bat.RowCount(); row++ {
				got = append(got, bat.Vecs[0].GetStringAt(row))
			}
			bat.Clean(proc.Mp())
			if finished {
				break
			}
		}

		require.Len(t, got, len(values))
		for i, value := range values {
			require.JSONEq(t, `{"v":"`+value+`"}`, got[i])
		}
	})

	t.Run("dictionary sharing still makes forward progress", func(t *testing.T) {
		values := make([]string, 100)
		for i := range values {
			values[i] = strings.Repeat("d", 100)
		}
		data := writeBatchBudgetDictionaryStringParquet(t, values)
		reader, param, proc := newBatchBudgetParquetReader(t, data, types.T_text, 1<<10)
		defer reader.Close()

		var total int
		for attempts := 0; attempts < len(values); attempts++ {
			bat := batchBudgetVectorBatch(types.T_text)
			finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
			require.NoError(t, err)
			require.Positive(t, bat.RowCount())
			require.LessOrEqual(t, uint64(bat.Size()), param.maxBatchSize)
			total += bat.RowCount()
			bat.Clean(proc.Mp())
			if finished {
				break
			}
		}
		require.Equal(t, len(values), total)
	})

	t.Run("columns with different page boundaries stay aligned", func(t *testing.T) {
		stringsIn := make([]string, 10)
		intsIn := make([]int32, 10)
		for i := range stringsIn {
			stringsIn[i] = strings.Repeat(string(rune('a'+i)), 300)
			intsIn[i] = int32(i)
		}
		data := writeBatchBudgetTwoColumnParquet(t, stringsIn, intsIn)
		reader, proc := newBatchBudgetTwoColumnReader(t, data, 700)
		defer reader.Close()

		var stringsOut []string
		var intsOut []int32
		for {
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
			finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
			require.NoError(t, err)
			for row := 0; row < bat.RowCount(); row++ {
				stringsOut = append(stringsOut, bat.Vecs[0].GetStringAt(row))
			}
			intsOut = append(intsOut,
				vector.MustFixedColWithTypeCheck[int32](bat.Vecs[1])[:bat.RowCount()]...)
			bat.Clean(proc.Mp())
			if finished {
				break
			}
		}

		require.Equal(t, stringsIn, stringsOut)
		require.Equal(t, intsIn, intsOut)
	})

	t.Run("virtual-only row count respects the generated vector boundary", func(t *testing.T) {
		data := writeBatchBudgetInt32Parquet(t, []int32{1, 2, 3})
		reader, param, proc := newBatchBudgetProjectedParquetReader(t, data, 1,
			[]plan.ExternAttr{{ColName: catalog.ExternalFilePath, ColIndex: 0}},
			[]*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_text)}}},
			"file.parquet")
		defer reader.Close()

		var rowsPerBatch []int
		for {
			bat := batch.NewWithSchema(false, []string{"path"}, []types.Type{types.T_text.ToType()})
			finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
			require.NoError(t, err)
			rowsPerBatch = append(rowsPerBatch, bat.RowCount())
			require.Equal(t, 1, bat.RowCount())
			require.Greater(t, uint64(bat.Size()), param.maxBatchSize)
			bat.Clean(proc.Mp())
			if finished {
				break
			}
		}
		require.Equal(t, []int{1, 1, 1}, rowsPerBatch)
	})

	t.Run("row count-only without generated columns keeps the full batch", func(t *testing.T) {
		rows := int(maxParquetBatchCnt) + 1
		values := make([]int32, rows)
		data := writeBatchBudgetInt32Parquet(t, values)
		reader, _, proc := newBatchBudgetProjectedParquetReader(t, data, 1,
			nil, nil, "")
		defer reader.Close()

		first := batch.NewWithSize(0)
		finished, err := reader.ReadBatch(context.Background(), first, proc, nil)
		require.NoError(t, err)
		require.False(t, finished)
		require.Equal(t, int(maxParquetBatchCnt), first.RowCount())
		first.Clean(proc.Mp())

		second := batch.NewWithSize(0)
		finished, err = reader.ReadBatch(context.Background(), second, proc, nil)
		require.NoError(t, err)
		require.True(t, finished)
		require.Equal(t, 1, second.RowCount())
		second.Clean(proc.Mp())
	})

	t.Run("mixed physical and virtual columns reserve generated bytes", func(t *testing.T) {
		data := writeBatchBudgetInt32Parquet(t, []int32{1, 2, 3})
		reader, _, proc := newBatchBudgetProjectedParquetReader(t, data, 50,
			[]plan.ExternAttr{{ColName: "c", ColIndex: 0}, {ColName: catalog.ExternalFilePath, ColIndex: 1}},
			[]*plan.ColDef{
				{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}, NotNull: true},
				{Typ: plan.Type{Id: int32(types.T_text)}},
			},
			"p")
		defer reader.Close()

		var values []int32
		var rowsPerBatch []int
		for {
			bat := batch.NewWithSchema(false, []string{"c", "path"}, []types.Type{types.T_int32.ToType(), types.T_text.ToType()})
			finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
			require.NoError(t, err)
			rowsPerBatch = append(rowsPerBatch, bat.RowCount())
			values = append(values, vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])[:bat.RowCount()]...)
			bat.Clean(proc.Mp())
			if finished {
				break
			}
		}
		require.Equal(t, []int32{1, 2, 3}, values)
		require.Equal(t, []int{1, 1, 1}, rowsPerBatch)
	})

	t.Run("reused batch ignores stale Hive default partition state", func(t *testing.T) {
		values := make([]int32, 100)
		for i := range values {
			values[i] = int32(i)
		}
		data := writeBatchBudgetInt32Parquet(t, values)
		reader, _, proc := newBatchBudgetHiveDefaultPartitionReader(t, data, 1<<10)
		defer reader.Close()

		bat := batch.NewWithSchema(false, []string{"c", "p"},
			[]types.Type{types.T_int32.ToType(), types.T_text.ToType()})
		finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
		require.NoError(t, err)
		require.False(t, finished)
		firstRows := bat.RowCount()
		require.Positive(t, firstRows)
		require.True(t, bat.Vecs[1].IsConstNull())

		bat.CleanOnlyData()
		finished, err = reader.ReadBatch(context.Background(), bat, proc, nil)
		require.NoError(t, err)
		require.False(t, finished)
		require.Equal(t, firstRows, bat.RowCount())
		require.Equal(t, int32(firstRows), vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])[0])
		require.True(t, bat.Vecs[1].IsConstNull())
		bat.Clean(proc.Mp())
	})

	t.Run("Iceberg generated metadata reserves bytes and advances", func(t *testing.T) {
		data := writeBatchBudgetInt32Parquet(t, []int32{1, 2, 3})
		path := strings.Repeat("f", 32)
		reader, param, proc := newBatchBudgetProjectedParquetReader(t, data, 64,
			[]plan.ExternAttr{
				{ColName: "c", ColIndex: 0},
				{ColName: IcebergDMLDataFilePathAttr, ColIndex: 1},
				{ColName: IcebergDMLRowOrdinalAttr, ColIndex: 2},
			},
			[]*plan.ColDef{
				{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}, NotNull: true},
				{Typ: plan.Type{Id: int32(types.T_varchar)}},
				{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}, NotNull: true},
			},
			path)
		defer reader.Close()

		var ordinals []int64
		for {
			bat := batch.NewWithSchema(false,
				[]string{"c", IcebergDMLDataFilePathAttr, IcebergDMLRowOrdinalAttr},
				[]types.Type{types.T_int32.ToType(), types.T_varchar.ToType(), types.T_int64.ToType()})
			finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
			require.NoError(t, err)
			require.Equal(t, 1, bat.RowCount())
			require.Greater(t, uint64(bat.Size()), param.maxBatchSize)
			require.Equal(t, path, bat.Vecs[1].GetStringAt(0))
			ordinals = append(ordinals, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[2])[:bat.RowCount()]...)
			bat.Clean(proc.Mp())
			if finished {
				break
			}
		}
		require.Equal(t, []int64{0, 1, 2}, ordinals)
	})

	t.Run("Iceberg missing optional column reserves null vector bytes", func(t *testing.T) {
		data := writeBatchBudgetFieldIDInt32Parquet(t, []int32{1, 2, 3})
		reader, proc := newBatchBudgetIcebergSchemaEvolutionReader(t, data, 35)
		defer reader.Close()

		var values []int32
		var rowsPerBatch []int
		for {
			bat := batch.NewWithSchema(false, []string{"c", "added"},
				[]types.Type{types.T_int32.ToType(), types.T_text.ToType()})
			finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
			require.NoError(t, err)
			rowsPerBatch = append(rowsPerBatch, bat.RowCount())
			require.Equal(t, 1, bat.RowCount())
			require.Less(t, uint64(bat.Size()), uint64(35))
			require.True(t, bat.Vecs[1].GetNulls().Contains(0))
			values = append(values, vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])[:bat.RowCount()]...)
			bat.Clean(proc.Mp())
			if finished {
				break
			}
		}
		require.Equal(t, []int32{1, 2, 3}, values)
		require.Equal(t, []int{1, 1, 1}, rowsPerBatch)
	})
}

func BenchmarkParquetReaderBatchByteBudget(b *testing.B) {
	values := make([]string, 2048)
	for i := range values {
		values[i] = strings.Repeat("x", 4<<10)
	}
	data := writeBatchBudgetStringParquet(b, values)
	const budget = uint64(64 << 10)
	var maxBatchBytes int

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _, proc := newBatchBudgetParquetReader(b, data, types.T_text, budget)
		for {
			bat := batchBudgetVectorBatch(types.T_text)
			finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
			require.NoError(b, err)
			maxBatchBytes = max(maxBatchBytes, bat.Size())
			bat.Clean(proc.Mp())
			if finished {
				break
			}
		}
		require.NoError(b, reader.Close())
	}
	b.ReportMetric(float64(maxBatchBytes), "max-batch-bytes")
}

func BenchmarkParquetReaderRowCountOnly(b *testing.B) {
	values := make([]int32, maxParquetBatchCnt)
	data := writeBatchBudgetInt32Parquet(b, values)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _, proc := newBatchBudgetProjectedParquetReader(b, data, 1, nil, nil, "")
		bat := batch.NewWithSize(0)
		finished, err := reader.ReadBatch(context.Background(), bat, proc, nil)
		require.NoError(b, err)
		require.True(b, finished)
		require.Equal(b, int(maxParquetBatchCnt), bat.RowCount())
		bat.Clean(proc.Mp())
		require.NoError(b, reader.Close())
	}
}

func newBatchBudgetParquetReader(
	t testing.TB,
	data []byte,
	targetType types.T,
	budget uint64,
) (*ParquetReader, *ExternalParam, *process.Process) {
	t.Helper()
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:          context.Background(),
			maxBatchSize: budget,
			Attrs:        []plan.ExternAttr{{ColName: "c", ColIndex: 0}},
			Cols: []*plan.ColDef{{
				Typ:     plan.Type{Id: int32(targetType), NotNullable: true},
				NotNull: true,
			}},
			Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{
				ScanType: tree.INLINE,
				Format:   tree.PARQUET,
				Data:     string(data),
			}},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{
			Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1},
		},
	}
	proc := testutil.NewProc(t)
	reader := NewParquetReader(param, proc)
	empty, err := reader.Open(param, proc)
	require.NoError(t, err)
	require.False(t, empty)
	return reader, param, proc
}

func newBatchBudgetProjectedParquetReader(
	t testing.TB,
	data []byte,
	budget uint64,
	attrs []plan.ExternAttr,
	cols []*plan.ColDef,
	filePath string,
) (*ParquetReader, *ExternalParam, *process.Process) {
	t.Helper()
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:          context.Background(),
			maxBatchSize: budget,
			Attrs:        attrs,
			Cols:         cols,
			Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{
				ScanType: tree.INLINE,
				Format:   tree.PARQUET,
				Data:     string(data),
			}},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, Filepath: filePath, FileCnt: 1}},
	}
	proc := testutil.NewProc(t)
	reader := NewParquetReader(param, proc)
	empty, err := reader.Open(param, proc)
	require.NoError(t, err)
	require.False(t, empty)
	return reader, param, proc
}

func newBatchBudgetHiveDefaultPartitionReader(
	t testing.TB,
	data []byte,
	budget uint64,
) (*ParquetReader, *ExternalParam, *process.Process) {
	t.Helper()
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:          context.Background(),
			maxBatchSize: budget,
			Attrs: []plan.ExternAttr{
				{ColName: "c", ColIndex: 0},
				{ColName: "p", ColIndex: 1},
			},
			Cols: []*plan.ColDef{
				{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}, NotNull: true},
				{Name: "p", Typ: plan.Type{Id: int32(types.T_text)}, Default: &plan.Default{NullAbility: true}},
			},
			Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{
				ScanType:          tree.INLINE,
				Format:            tree.PARQUET,
				Data:              string(data),
				Filepath:          "/fixture",
				HivePartitioning:  true,
				HivePartitionCols: []string{"p"},
			}},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{
			Fileparam:         &ExFileparam{FileIndex: 1, Filepath: "/fixture/p=__HIVE_DEFAULT_PARTITION__/data.parquet", FileCnt: 1},
			currentPartValues: map[string]string{"p": HiveDefaultPartition},
		},
	}
	proc := testutil.NewProc(t)
	reader := NewParquetReader(param, proc)
	empty, err := reader.Open(param, proc)
	require.NoError(t, err)
	require.False(t, empty)
	return reader, param, proc
}

func newBatchBudgetIcebergSchemaEvolutionReader(
	t testing.TB,
	data []byte,
	budget uint64,
) (*ParquetReader, *process.Process) {
	t.Helper()
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:          context.Background(),
			maxBatchSize: budget,
			Attrs: []plan.ExternAttr{
				{ColName: "c", ColIndex: 0},
				{ColName: "added", ColIndex: 1},
			},
			Cols: []*plan.ColDef{
				{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}, NotNull: true},
				{Typ: plan.Type{Id: int32(types.T_text)}},
			},
			IcebergColumns: []*pipeline.IcebergColumnMapping{
				{MoColIndex: 0, IcebergFieldId: 1, SnapshotFieldName: "c", CurrentFieldName: "c"},
				{MoColIndex: 1, IcebergFieldId: 2, SnapshotFieldName: "added", CurrentFieldName: "added", DefaultNullFill: true},
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
	return reader, proc
}

func batchBudgetVectorBatch(targetType types.T) *batch.Batch {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.New(targetType, 0, 0))
	return bat
}

func writeBatchBudgetStringParquet(t testing.TB, values []string) []byte {
	t.Helper()
	return writeBatchBudgetParquet(t, parquet.NewSchema("x", parquet.Group{
		"c": parquet.String(),
	}), values, func(value string) parquet.Row {
		return parquet.Row{parquet.ByteArrayValue([]byte(value)).Level(0, 0, 0)}
	})
}

func writeBatchBudgetDictionaryStringParquet(t testing.TB, values []string) []byte {
	t.Helper()
	return writeBatchBudgetParquet(t, parquet.NewSchema("x", parquet.Group{
		"c": parquet.Encoded(parquet.String(), &parquet.RLEDictionary),
	}), values, func(value string) parquet.Row {
		return parquet.Row{parquet.ByteArrayValue([]byte(value)).Level(0, 0, 0)}
	})
}

func writeBatchBudgetNestedParquet(t testing.TB, values []string) []byte {
	t.Helper()
	return writeBatchBudgetParquet(t, parquet.NewSchema("x", parquet.Group{
		"c": parquet.Group{"v": parquet.String()},
	}), values, func(value string) parquet.Row {
		return parquet.Row{parquet.ByteArrayValue([]byte(value)).Level(0, 0, 0)}
	})
}

func writeBatchBudgetInt32Parquet(t testing.TB, values []int32) []byte {
	t.Helper()
	return writeBatchBudgetParquet(t, parquet.NewSchema("x", parquet.Group{
		"c": parquet.Leaf(parquet.Int32Type),
	}), values, func(value int32) parquet.Row {
		return parquet.Row{parquet.Int32Value(value).Level(0, 0, 0)}
	})
}

func writeBatchBudgetFieldIDInt32Parquet(t testing.TB, values []int32) []byte {
	t.Helper()
	return writeBatchBudgetParquet(t, parquet.NewSchema("x", parquet.Group{
		"c": parquet.FieldID(parquet.Leaf(parquet.Int32Type), 1),
	}), values, func(value int32) parquet.Row {
		return parquet.Row{parquet.Int32Value(value).Level(0, 0, 0)}
	})
}

func writeBatchBudgetTwoColumnParquet(t testing.TB, stringsIn []string, intsIn []int32) []byte {
	t.Helper()
	require.Len(t, intsIn, len(stringsIn))
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, parquet.NewSchema("x", parquet.Group{
		"c1": parquet.String(),
		"c2": parquet.Leaf(parquet.Int32Type),
	}), parquet.PageBufferSize(512))
	rows := make([]parquet.Row, len(stringsIn))
	for i := range stringsIn {
		rows[i] = parquet.Row{
			parquet.ByteArrayValue([]byte(stringsIn[i])).Level(0, 0, 0),
			parquet.Int32Value(intsIn[i]).Level(0, 0, 1),
		}
	}
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func newBatchBudgetTwoColumnReader(
	t testing.TB,
	data []byte,
	budget uint64,
) (*ParquetReader, *process.Process) {
	t.Helper()
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:          context.Background(),
			maxBatchSize: budget,
			Attrs: []plan.ExternAttr{
				{ColName: "c1", ColIndex: 0},
				{ColName: "c2", ColIndex: 1},
			},
			Cols: []*plan.ColDef{
				{Typ: plan.Type{Id: int32(types.T_text), NotNullable: true}, NotNull: true},
				{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}, NotNull: true},
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
	return reader, proc
}

func writeBatchBudgetParquet[T any](
	t testing.TB,
	schema *parquet.Schema,
	values []T,
	row func(T) parquet.Row,
) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	rows := make([]parquet.Row, len(values))
	for i := range values {
		rows[i] = row(values[i])
	}
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}
