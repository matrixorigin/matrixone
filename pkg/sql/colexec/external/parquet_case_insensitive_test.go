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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
