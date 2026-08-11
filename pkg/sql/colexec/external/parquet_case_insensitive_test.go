// Copyright 2025 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// TestFindColumnIgnoreCase tests the case-insensitive column lookup function.
func TestFindColumnIgnoreCase(t *testing.T) {
	ctx := context.Background()

	t.Run("exact_match", func(t *testing.T) {
		f := createParquetFileWithColumns(t, []string{"col1", "col2"})
		h := &ParquetHandler{file: f}

		col, err := h.findColumnIgnoreCase(ctx, "col1")
		require.NoError(t, err)
		require.NotNil(t, col)
		require.Equal(t, "col1", col.Name())
	})

	t.Run("lowercase_query_uppercase_parquet", func(t *testing.T) {
		f := createParquetFileWithColumns(t, []string{"FL_DATE", "DEP_DELAY"})
		h := &ParquetHandler{file: f}

		col, err := h.findColumnIgnoreCase(ctx, "fl_date")
		require.NoError(t, err)
		require.NotNil(t, col)
		require.Equal(t, "FL_DATE", col.Name())

		col, err = h.findColumnIgnoreCase(ctx, "dep_delay")
		require.NoError(t, err)
		require.NotNil(t, col)
		require.Equal(t, "DEP_DELAY", col.Name())
	})

	t.Run("uppercase_query_lowercase_parquet", func(t *testing.T) {
		f := createParquetFileWithColumns(t, []string{"passengerid", "survived"})
		h := &ParquetHandler{file: f}

		col, err := h.findColumnIgnoreCase(ctx, "PassengerId")
		require.NoError(t, err)
		require.NotNil(t, col)
		require.Equal(t, "passengerid", col.Name())
	})

	t.Run("mixed_case", func(t *testing.T) {
		f := createParquetFileWithColumns(t, []string{"FlightDate"})
		h := &ParquetHandler{file: f}

		testCases := []string{"flightdate", "FLIGHTDATE", "FlightDate", "fLiGhTdAtE"}
		for _, query := range testCases {
			col, err := h.findColumnIgnoreCase(ctx, query)
			require.NoError(t, err, "query: %s", query)
			require.NotNil(t, col, "query: %s", query)
			require.Equal(t, "FlightDate", col.Name())
		}
	})

	t.Run("column_not_found", func(t *testing.T) {
		f := createParquetFileWithColumns(t, []string{"existing_col"})
		h := &ParquetHandler{file: f}

		col, err := h.findColumnIgnoreCase(ctx, "nonexistent")
		require.NoError(t, err)
		require.Nil(t, col)
	})

	t.Run("ambiguous_columns", func(t *testing.T) {
		// Create parquet file with both "col" and "COL"
		f := createAmbiguousParquetFile(t)
		h := &ParquetHandler{file: f}

		// Query "Col" should match both "col" and "COL" case-insensitively
		col, err := h.findColumnIgnoreCase(ctx, "Col")
		require.Error(t, err)
		require.Nil(t, col)
		require.Contains(t, err.Error(), "ambiguous")
	})

	t.Run("ambiguous_with_exact_match", func(t *testing.T) {
		// Create parquet file with both "col" and "COL"
		// Even if query "col" matches exactly, it should still be ambiguous
		f := createAmbiguousParquetFile(t)
		h := &ParquetHandler{file: f}

		// Query "col" exactly matches "col", but "COL" also matches case-insensitively
		// This should return an error, not silently return "col"
		col, err := h.findColumnIgnoreCase(ctx, "col")
		require.Error(t, err)
		require.Nil(t, col)
		require.Contains(t, err.Error(), "ambiguous")

		// Same for "COL"
		col, err = h.findColumnIgnoreCase(ctx, "COL")
		require.Error(t, err)
		require.Nil(t, col)
		require.Contains(t, err.Error(), "ambiguous")
	})
}

// TestParquetPrepare_CaseInsensitive tests prepare() with case-insensitive column matching.
func TestParquetPrepare_CaseInsensitive(t *testing.T) {
	t.Run("lowercase_query_uppercase_parquet", func(t *testing.T) {
		f := createParquetFileWithColumns(t, []string{"FL_DATE", "DEP_DELAY"})
		h := &ParquetHandler{file: f}

		param := &ExternalParam{
			ExParamConst: ExParamConst{
				Ctx: context.Background(),
				Attrs: []plan.ExternAttr{
					{ColName: "fl_date", ColIndex: 0},
					{ColName: "dep_delay", ColIndex: 1},
				},
				Cols: []*plan.ColDef{
					{Typ: plan.Type{Id: int32(types.T_int32)}},
					{Typ: plan.Type{Id: int32(types.T_int32)}},
				},
			},
		}

		err := h.prepare(param)
		require.NoError(t, err)
		require.NotNil(t, h.cols[0])
		require.NotNil(t, h.cols[1])
		require.Equal(t, "FL_DATE", h.cols[0].Name())
		require.Equal(t, "DEP_DELAY", h.cols[1].Name())
	})

	t.Run("column_not_found_still_errors", func(t *testing.T) {
		f := createParquetFileWithColumns(t, []string{"existing_col"})
		h := &ParquetHandler{file: f}

		param := &ExternalParam{
			ExParamConst: ExParamConst{
				Ctx: context.Background(),
				Attrs: []plan.ExternAttr{
					{ColName: "nonexistent", ColIndex: 0},
				},
				Cols: []*plan.ColDef{
					{Typ: plan.Type{Id: int32(types.T_int32)}},
				},
			},
		}

		err := h.prepare(param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("ambiguous_columns_error", func(t *testing.T) {
		f := createAmbiguousParquetFile(t)
		h := &ParquetHandler{file: f}

		param := &ExternalParam{
			ExParamConst: ExParamConst{
				Ctx: context.Background(),
				Attrs: []plan.ExternAttr{
					{ColName: "Col", ColIndex: 0}, // ambiguous
				},
				Cols: []*plan.ColDef{
					{Typ: plan.Type{Id: int32(types.T_int32)}},
				},
			},
		}

		err := h.prepare(param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "ambiguous")
	})
}

func TestParquetPrepare_IcebergFieldIDMapping(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("parquet_field_id_round_trip_and_missing_id_zero", func(t *testing.T) {
		f := createParquetFileWithFieldIDData(t, []parquetFieldIDColumn{
			{name: "with_id", fieldID: 7, value: 42},
			{name: "without_id", value: 11},
		})

		require.Equal(t, 7, f.Root().Column("with_id").ID())
		require.Equal(t, 0, f.Root().Column("without_id").ID())
	})

	t.Run("field_id_wins_after_rename", func(t *testing.T) {
		f := createParquetFileWithFieldIDData(t, []parquetFieldIDColumn{
			{name: "old_order_id", fieldID: 7, value: 42},
		})
		h := &ParquetHandler{file: f, batchCnt: 10}
		param := icebergParquetParam(
			[]plan.ExternAttr{{ColName: "current_order_id", ColIndex: 0}},
			[]*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32)}}},
			[]*pipeline.IcebergColumnMapping{{
				MoColIndex:        0,
				IcebergFieldId:    7,
				SnapshotFieldName: "current_order_id",
				CurrentFieldName:  "current_order_id",
			}},
		)

		require.NoError(t, h.prepare(param))
		require.Equal(t, "old_order_id", h.cols[0].Name())

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_int32, 0, 0))
		require.NoError(t, h.getData(bat, param, proc))
		require.Equal(t, 1, bat.RowCount())
		values := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
		require.Equal(t, int32(42), values[0])
	})

	t.Run("missing_field_id_uses_explicit_safe_path_hint", func(t *testing.T) {
		f := createParquetFileWithFieldIDData(t, []parquetFieldIDColumn{
			{name: "stable_name", value: 11},
		})
		h := &ParquetHandler{file: f, batchCnt: 10}
		param := icebergParquetParam(
			[]plan.ExternAttr{{ColName: "stable_name", ColIndex: 0}},
			[]*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32)}}},
			[]*pipeline.IcebergColumnMapping{{
				MoColIndex:       0,
				IcebergFieldId:   8,
				CurrentFieldName: "stable_name",
				ParquetPathHint:  "stable_name",
			}},
		)

		require.NoError(t, h.prepare(param))
		require.Equal(t, "stable_name", h.cols[0].Name())
	})

	t.Run("path_hint_with_mismatched_field_id_fails", func(t *testing.T) {
		f := createParquetFileWithFieldIDData(t, []parquetFieldIDColumn{
			{name: "stable_name", fieldID: 88, value: 11},
		})
		h := &ParquetHandler{file: f, batchCnt: 10}
		param := icebergParquetParam(
			[]plan.ExternAttr{{ColName: "stable_name", ColIndex: 0}},
			[]*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32)}}},
			[]*pipeline.IcebergColumnMapping{{
				MoColIndex:       0,
				IcebergFieldId:   8,
				CurrentFieldName: "stable_name",
				ParquetPathHint:  "stable_name",
			}},
		)

		err := h.prepare(param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "field id mismatch")
	})

	t.Run("optional_added_column_missing_in_old_file_fills_null", func(t *testing.T) {
		f := createParquetFileWithFieldIDData(t, []parquetFieldIDColumn{
			{name: "existing_id", fieldID: 1, value: 1},
		})
		h := &ParquetHandler{file: f, batchCnt: 10}
		param := icebergParquetParam(
			[]plan.ExternAttr{{ColName: "new_optional_col", ColIndex: 0}},
			[]*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32)}}},
			[]*pipeline.IcebergColumnMapping{{
				MoColIndex:        0,
				IcebergFieldId:    2,
				SnapshotFieldName: "new_optional_col",
				CurrentFieldName:  "new_optional_col",
			}},
		)

		require.NoError(t, h.prepare(param))
		require.True(t, h.rowCountOnly)

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_int32, 0, 0))
		require.NoError(t, h.getData(bat, param, proc))
		require.Equal(t, 1, bat.RowCount())
		require.Equal(t, 1, bat.Vecs[0].Length())
		require.True(t, bat.Vecs[0].GetNulls().Contains(0))
	})

	t.Run("missing_field_id_without_safe_fallback_fails", func(t *testing.T) {
		f := createParquetFileWithFieldIDData(t, []parquetFieldIDColumn{
			{name: "old_name", value: 1},
		})
		h := &ParquetHandler{file: f, batchCnt: 10}
		param := icebergParquetParam(
			[]plan.ExternAttr{{ColName: "new_name", ColIndex: 0}},
			[]*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32)}}},
			[]*pipeline.IcebergColumnMapping{{
				MoColIndex:        0,
				IcebergFieldId:    9,
				SnapshotFieldName: "new_name",
				CurrentFieldName:  "new_name",
			}},
		)

		err := h.prepare(param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "field_id=9")
	})

	t.Run("duplicate_field_id_fails_fast", func(t *testing.T) {
		f := createParquetFileWithFieldIDData(t, []parquetFieldIDColumn{
			{name: "a", fieldID: 10, value: 1},
			{name: "b", fieldID: 10, value: 2},
		})
		h := &ParquetHandler{file: f, batchCnt: 10}
		param := icebergParquetParam(
			[]plan.ExternAttr{{ColName: "a", ColIndex: 0}},
			[]*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32)}}},
			[]*pipeline.IcebergColumnMapping{{
				MoColIndex:        0,
				IcebergFieldId:    10,
				SnapshotFieldName: "a",
			}},
		)

		err := h.prepare(param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "ambiguous parquet field id 10")
	})
}

func TestParquetPrepare_IcebergSchemaEvolutionRead(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("rename_reorder_drop_add_and_type_promotion", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("test", parquet.Group{
			"legacy_amount": parquet.FieldID(parquet.Leaf(parquet.Int32Type), 2),
			"legacy_id":     parquet.FieldID(parquet.Leaf(parquet.Int32Type), 1),
			"legacy_rating": parquet.FieldID(parquet.Leaf(parquet.FloatType), 3),
			"dropped_col":   parquet.FieldID(parquet.Leaf(parquet.Int32Type), 4),
		})
		w := parquet.NewWriter(&buf, schema)
		row := make([]parquet.Value, len(schema.Fields()))
		for i, field := range schema.Fields() {
			switch field.Name() {
			case "legacy_amount":
				row[i] = parquet.Int32Value(123).Level(0, 0, i)
			case "legacy_id":
				row[i] = parquet.Int32Value(7).Level(0, 0, i)
			case "legacy_rating":
				row[i] = parquet.FloatValue(1.5).Level(0, 0, i)
			case "dropped_col":
				row[i] = parquet.Int32Value(999).Level(0, 0, i)
			}
		}
		_, err := w.WriteRows([]parquet.Row{row})
		require.NoError(t, err)
		require.NoError(t, w.Close())
		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		h := &ParquetHandler{file: f, batchCnt: 10}
		param := icebergParquetParam(
			[]plan.ExternAttr{
				{ColName: "rating", ColIndex: 0},
				{ColName: "id", ColIndex: 1},
				{ColName: "amount", ColIndex: 2},
				{ColName: "new_optional", ColIndex: 3},
			},
			[]*plan.ColDef{
				{Name: "rating", Typ: plan.Type{Id: int32(types.T_float64)}},
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "amount", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "new_optional", Typ: plan.Type{Id: int32(types.T_int32)}},
			},
			[]*pipeline.IcebergColumnMapping{
				{MoColIndex: 0, IcebergFieldId: 3, SnapshotFieldName: "rating", CurrentFieldName: "rating"},
				{MoColIndex: 1, IcebergFieldId: 1, SnapshotFieldName: "id", CurrentFieldName: "id"},
				{MoColIndex: 2, IcebergFieldId: 2, SnapshotFieldName: "amount", CurrentFieldName: "amount"},
				{MoColIndex: 3, IcebergFieldId: 5, SnapshotFieldName: "new_optional", CurrentFieldName: "new_optional", DefaultNullFill: true},
			},
		)

		require.NoError(t, h.prepare(param))
		require.Equal(t, "legacy_rating", h.cols[0].Name())
		require.Equal(t, "legacy_id", h.cols[1].Name())
		require.Equal(t, "legacy_amount", h.cols[2].Name())
		require.Nil(t, h.cols[3])

		bat := batch.NewWithSize(4)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_float64, 0, 0))
		bat.Vecs[1] = vector.NewVec(types.New(types.T_int64, 0, 0))
		bat.Vecs[2] = vector.NewVec(types.New(types.T_int64, 0, 0))
		bat.Vecs[3] = vector.NewVec(types.New(types.T_int32, 0, 0))
		require.NoError(t, h.getData(bat, param, proc))
		require.Equal(t, 1, bat.RowCount())
		require.InDelta(t, 1.5, vector.MustFixedColWithTypeCheck[float64](bat.Vecs[0])[0], 0.0001)
		require.Equal(t, int64(7), vector.MustFixedColWithTypeCheck[int64](bat.Vecs[1])[0])
		require.Equal(t, int64(123), vector.MustFixedColWithTypeCheck[int64](bat.Vecs[2])[0])
		require.True(t, bat.Vecs[3].GetNulls().Contains(0))
	})

	t.Run("decimal_precision_increase", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("test", parquet.Group{
			"legacy_price": parquet.FieldID(parquet.Decimal(2, 5, parquet.FixedLenByteArrayType(3)), 6),
		})
		w := parquet.NewWriter(&buf, schema)
		rows := []parquet.Row{{
			parquet.FixedLenByteArrayValue(encodeDecimalToBytes(12345, 3)).Level(0, 0, 0),
		}}
		_, err := w.WriteRows(rows)
		require.NoError(t, err)
		require.NoError(t, w.Close())
		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		h := &ParquetHandler{file: f, batchCnt: 10}
		param := icebergParquetParam(
			[]plan.ExternAttr{{ColName: "price", ColIndex: 0}},
			[]*plan.ColDef{{Name: "price", Typ: plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}}},
			[]*pipeline.IcebergColumnMapping{{
				MoColIndex:        0,
				IcebergFieldId:    6,
				SnapshotFieldName: "price",
				CurrentFieldName:  "price",
			}},
		)

		require.NoError(t, h.prepare(param))
		require.Equal(t, "legacy_price", h.cols[0].Name())

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_decimal64, 10, 2))
		require.NoError(t, h.getData(bat, param, proc))
		require.Equal(t, 1, bat.RowCount())
		got := vector.MustFixedColWithTypeCheck[types.Decimal64](bat.Vecs[0])
		require.Equal(t, types.Decimal64(12345), got[0])
	})
}

// createParquetFileWithColumns creates a parquet file with the given column names.
func createParquetFileWithColumns(t *testing.T, colNames []string) *parquet.File {
	t.Helper()

	group := make(parquet.Group)
	for _, name := range colNames {
		group[name] = parquet.Leaf(parquet.Int32Type)
	}

	schema := parquet.NewSchema("test", group)

	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)

	// Build row according to schema field order
	row := make([]parquet.Value, len(schema.Fields()))
	for i := range schema.Fields() {
		row[i] = parquet.Int32Value(int32(i+1)).Level(0, 0, i)
	}

	_, err := w.WriteRows([]parquet.Row{row})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	return f
}

type parquetFieldIDColumn struct {
	name    string
	fieldID int
	value   int32
}

func createParquetFileWithFieldIDData(t *testing.T, cols []parquetFieldIDColumn) *parquet.File {
	t.Helper()

	group := make(parquet.Group)
	nameToValue := make(map[string]int32, len(cols))
	for _, col := range cols {
		node := parquet.Leaf(parquet.Int32Type)
		if col.fieldID != 0 {
			node = parquet.FieldID(node, col.fieldID)
		}
		group[col.name] = node
		nameToValue[col.name] = col.value
	}

	schema := parquet.NewSchema("test", group)
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	row := make([]parquet.Value, len(schema.Fields()))
	for i, field := range schema.Fields() {
		row[i] = parquet.Int32Value(nameToValue[field.Name()]).Level(0, 0, i)
	}

	_, err := w.WriteRows([]parquet.Row{row})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	return f
}

func icebergParquetParam(
	attrs []plan.ExternAttr,
	cols []*plan.ColDef,
	mappings []*pipeline.IcebergColumnMapping,
) *ExternalParam {
	return &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:            context.Background(),
			Attrs:          attrs,
			Cols:           cols,
			IcebergColumns: mappings,
			IcebergSnapshot: &pipeline.IcebergSnapshotRuntime{
				SnapshotId: 123,
			},
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					Format: tree.PARQUET,
				},
				ExParam: tree.ExParam{
					ExternType: int32(plan.ExternType_ICEBERG_TB),
				},
			},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{
			FileIndex: 1,
			FileCnt:   1,
			Filepath:  "s3://bucket/path/data.parquet",
		}},
	}
}

// createAmbiguousParquetFile creates a parquet file with "col" and "COL" columns.
func createAmbiguousParquetFile(t *testing.T) *parquet.File {
	t.Helper()

	// parquet.Group is a map, which allows "col" and "COL" as different keys
	schema := parquet.NewSchema("test", parquet.Group{
		"col": parquet.Leaf(parquet.Int32Type),
		"COL": parquet.Leaf(parquet.Int32Type),
	})

	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)

	// Build row according to schema field order
	row := make([]parquet.Value, len(schema.Fields()))
	for i := range schema.Fields() {
		row[i] = parquet.Int32Value(int32(i+1)).Level(0, 0, i)
	}

	_, err := w.WriteRows([]parquet.Row{row})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	return f
}

// TestParquetScan_CaseInsensitive tests the full scan flow (prepare + getData) with case-insensitive matching.
func TestParquetScan_CaseInsensitive(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("scan_with_lowercase_query_uppercase_parquet", func(t *testing.T) {
		// Create parquet file with uppercase columns: FL_DATE=100, DEP_DELAY=200
		f := createParquetFileWithData(t, []string{"FL_DATE", "DEP_DELAY"}, []int32{100, 200})

		h := &ParquetHandler{file: f, batchCnt: 10}

		// Query with lowercase column names (simulating MO table definition behavior)
		param := &ExternalParam{
			ExParamConst: ExParamConst{
				Ctx: context.Background(),
				Attrs: []plan.ExternAttr{
					{ColName: "fl_date", ColIndex: 0},
					{ColName: "dep_delay", ColIndex: 1},
				},
				Cols: []*plan.ColDef{
					{Typ: plan.Type{Id: int32(types.T_int32)}},
					{Typ: plan.Type{Id: int32(types.T_int32)}},
				},
			},
			ExParam: ExParam{
				Fileparam: &ExFileparam{
					FileCnt:   1,
					FileIndex: 1,
				},
			},
		}

		// Prepare should succeed with case-insensitive matching
		err := h.prepare(param)
		require.NoError(t, err)
		require.Equal(t, "FL_DATE", h.cols[0].Name())
		require.Equal(t, "DEP_DELAY", h.cols[1].Name())

		// Create batch and read data
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_int32, 0, 0))
		bat.Vecs[1] = vector.NewVec(types.New(types.T_int32, 0, 0))

		err = h.getData(bat, param, proc)
		require.NoError(t, err)
		require.Equal(t, 1, bat.RowCount())

		// Verify the actual data values
		col0 := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
		col1 := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[1])
		require.Equal(t, int32(100), col0[0])
		require.Equal(t, int32(200), col1[0])
	})

	t.Run("scan_with_mixed_case", func(t *testing.T) {
		// Create parquet file with mixed case columns
		f := createParquetFileWithData(t, []string{"FlightDate", "DepDelay"}, []int32{300, 400})

		h := &ParquetHandler{file: f, batchCnt: 10}

		// Query with different case
		param := &ExternalParam{
			ExParamConst: ExParamConst{
				Ctx: context.Background(),
				Attrs: []plan.ExternAttr{
					{ColName: "flightdate", ColIndex: 0},
					{ColName: "DEPDELAY", ColIndex: 1},
				},
				Cols: []*plan.ColDef{
					{Typ: plan.Type{Id: int32(types.T_int32)}},
					{Typ: plan.Type{Id: int32(types.T_int32)}},
				},
			},
			ExParam: ExParam{
				Fileparam: &ExFileparam{
					FileCnt:   1,
					FileIndex: 1,
				},
			},
		}

		err := h.prepare(param)
		require.NoError(t, err)

		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_int32, 0, 0))
		bat.Vecs[1] = vector.NewVec(types.New(types.T_int32, 0, 0))

		err = h.getData(bat, param, proc)
		require.NoError(t, err)
		require.Equal(t, 1, bat.RowCount())

		col0 := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
		col1 := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[1])
		require.Equal(t, int32(300), col0[0])
		require.Equal(t, int32(400), col1[0])
	})
}

// createParquetFileWithData creates a parquet file with given column names and values.
func createParquetFileWithData(t *testing.T, colNames []string, values []int32) *parquet.File {
	t.Helper()
	require.Equal(t, len(colNames), len(values), "colNames and values must have same length")

	group := make(parquet.Group)
	for _, name := range colNames {
		group[name] = parquet.Leaf(parquet.Int32Type)
	}

	schema := parquet.NewSchema("test", group)

	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)

	// Build row: need to map values to schema field order
	nameToValue := make(map[string]int32)
	for i, name := range colNames {
		nameToValue[name] = values[i]
	}

	row := make([]parquet.Value, len(schema.Fields()))
	for i, field := range schema.Fields() {
		val := nameToValue[field.Name()]
		row[i] = parquet.Int32Value(val).Level(0, 0, i)
	}

	_, err := w.WriteRows([]parquet.Row{row})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	return f
}
