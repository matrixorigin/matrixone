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

package checkpointtool

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWriteCSV_empty tests writing an empty logical view to CSV.
func TestWriteCSV_empty(t *testing.T) {
	schema := &TableSchema{
		TableName:    "test_table",
		DatabaseName: "test_db",
		Columns: []TableColumn{
			{Name: "id", SQLType: "BIGINT", Position: 1, PhysicalPosition: 0},
			{Name: "name", SQLType: "VARCHAR(100)", Position: 2, PhysicalPosition: 1},
		},
	}
	view := &LogicalTableView{
		Headers: []string{"object", "block", "row", "col_0", "col_1"},
		Rows:    [][]string{},
	}

	var buf bytes.Buffer
	err := WriteCSV(&buf, schema, view)
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	// Skip comment lines to find the CSV header
	csvStart := 0
	for i, line := range lines {
		if !strings.HasPrefix(line, "--") {
			csvStart = i
			break
		}
	}
	require.GreaterOrEqual(t, len(lines), csvStart+1, "expected at least a header line")
	assert.Equal(t, "id,name", lines[csvStart])
}

// TestWriteCSV_withData tests writing data rows to CSV.
func TestWriteCSV_withData(t *testing.T) {
	schema := &TableSchema{
		TableName:    "users",
		DatabaseName: "mydb",
		CreateSQL:    "CREATE TABLE users (id BIGINT, name VARCHAR(100), age INT)",
		Columns: []TableColumn{
			{Name: "id", SQLType: "BIGINT", Position: 1, PhysicalPosition: 0},
			{Name: "name", SQLType: "VARCHAR(100)", Position: 2, PhysicalPosition: 1},
			{Name: "age", SQLType: "INT", Position: 3, PhysicalPosition: 2},
		},
	}
	view := &LogicalTableView{
		Headers: []string{"object", "block", "row", "col_0", "col_1", "col_2"},
		Rows: [][]string{
			{"obj1", "0", "0", "1", "Alice", "30"},
			{"obj1", "0", "1", "2", "Bob", "25"},
			{"obj2", "1", "0", "3", "Charlie, Jr.", "35"},
			{"obj2", "1", "1", "4", "NULL", "NULL"},
		},
		VisibleRows: 4,
	}

	var buf bytes.Buffer
	err := WriteCSV(&buf, schema, view)
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	// Skip comment lines
	var dataLines []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "--") {
			dataLines = append(dataLines, line)
		}
	}

	assert.Equal(t, 5, len(dataLines)) // header + 4 rows
	assert.Equal(t, "id,name,age", dataLines[0])
	assert.Equal(t, "1,Alice,30", dataLines[1])
	assert.Equal(t, "2,Bob,25", dataLines[2])
	assert.Equal(t, `3,"Charlie, Jr.",35`, dataLines[3])
	assert.Equal(t, "4,NULL,NULL", dataLines[4])
}

// TestWriteCSV_withCreateSQLHeader tests the header comment from CreateSQL.
func TestWriteCSV_withCreateSQLHeader(t *testing.T) {
	schema := &TableSchema{
		TableName:    "t1",
		DatabaseName: "db1",
		CreateSQL:    "CREATE TABLE t1 (id BIGINT)",
		Columns: []TableColumn{
			{Name: "id", SQLType: "BIGINT", Position: 1, PhysicalPosition: 0},
		},
	}
	view := &LogicalTableView{
		Headers: []string{"object", "block", "row", "col_0"},
		Rows: [][]string{
			{"obj1", "0", "0", "42"},
		},
	}

	var buf bytes.Buffer
	err := WriteCSV(&buf, schema, view)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "-- CREATE TABLE t1 (id BIGINT)")
	assert.Contains(t, output, "-- Database: db1")
	assert.Contains(t, output, "-- Table: t1")
}

// TestWriteCSV_mismatchedColumns verifies that position-based column mapping
// only includes columns matching the schema positions.
func TestWriteCSV_mismatchedColumns(t *testing.T) {
	schema := &TableSchema{
		TableName: "t1",
		Columns: []TableColumn{
			{Name: "a", SQLType: "INT", Position: 1, PhysicalPosition: 0},
		},
	}
	view := &LogicalTableView{
		Headers: []string{"object", "block", "row", "col_0", "col_1"},
		Rows: [][]string{
			{"obj1", "0", "0", "1", "extra"},
		},
	}

	var buf bytes.Buffer
	err := WriteCSV(&buf, schema, view)
	require.NoError(t, err)

	output := buf.String()
	// Skip comment lines
	var dataLines []string
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if !strings.HasPrefix(line, "--") {
			dataLines = append(dataLines, line)
		}
	}
	// Position-based: only column at position 0 (a) is included, extra columns dropped
	assert.Equal(t, "a", dataLines[0])
	assert.Equal(t, "1", dataLines[1])
}

// TestBuildSchemaFromMoTablesRow tests extracting a TableSchema from mo_tables row data.
func TestBuildSchemaFromMoTablesRow(t *testing.T) {
	// mo_tables physical layout: rel_id, relname, reldatabase, ...
	// After meta cols stripped, data cols = [rel_id, relname, reldatabase, reldatabase_id, ..., rel_createsql]
	view := &LogicalTableView{
		Headers: []string{
			"object", "block", "row",
			"rel_id", "relname", "reldatabase", "reldatabase_id",
			"col_4", "col_5", "col_6", "rel_createsql",
		},
	}

	// Full row with meta + data: rel_id="12345", relname="my_table", reldatabase="my_db", rel_createsql="CREATE TABLE my_table (x INT)"
	fullRow := []string{
		"obj1", "0", "0", // meta cols
		"12345", "my_table", "my_db", "100", "x", "x", "x", "CREATE TABLE my_table (x INT)",
	}

	schema := buildSchemaFromMoTablesRow(view, fullRow)
	assert.Equal(t, "my_table", schema.TableName)
	assert.Equal(t, "my_db", schema.DatabaseName)
	assert.Equal(t, "CREATE TABLE my_table (x INT)", schema.CreateSQL)
}

// TestBuildColumnsFromMoColumnsRows tests building column list from mo_columns rows.
func TestBuildColumnsFromMoColumnsRows(t *testing.T) {
	// mo_columns physical layout: ..., att_relname_id, att_relname, attname, atttyp, attnum, ..., att_is_hidden
	view := &LogicalTableView{
		Headers: []string{
			"object", "block", "row",
			"col_0", "col_1", "col_2", "col_3",
			"att_relname_id", "att_relname", "attname", "atttyp", "attnum",
			"col_9", "col_10", "col_11", "col_12", "col_13", "col_14", "col_15", "col_16", "col_17",
			"att_is_hidden",
			"col_19", "col_20", "col_21", "att_seqnum",
		},
		Rows: [][]string{
			{"obj1", "0", "0", "", "", "", "", "12345", "my_table", "id", "BIGINT", "1", "", "", "", "", "", "", "", "", "", "0", "", "", "", "0"},
			{"obj1", "0", "1", "", "", "", "", "12345", "my_table", "name", "VARCHAR(100)", "2", "", "", "", "", "", "", "", "", "", "0", "", "", "", "1"},
			{"obj1", "0", "2", "", "", "", "", "12345", "my_table", "_hidden_col", "INT", "3", "", "", "", "", "", "", "", "", "", "1", "", "", "", "2"},
		},
	}

	cols := buildColumnsFromMoColumnsRows(view, 12345)
	require.Len(t, cols, 2) // hidden column filtered

	assert.Equal(t, "id", cols[0].Name)
	assert.Equal(t, "BIGINT", cols[0].SQLType)
	assert.Equal(t, 1, cols[0].Position)
	assert.Equal(t, 0, cols[0].PhysicalPosition)

	assert.Equal(t, "name", cols[1].Name)
	assert.Equal(t, "VARCHAR(100)", cols[1].SQLType)
	assert.Equal(t, 2, cols[1].Position)
	assert.Equal(t, 1, cols[1].PhysicalPosition)
}

func TestBuildColumnsFromMoColumnsRows_FallbackToAttnumWhenSeqnumMissing(t *testing.T) {
	view := &LogicalTableView{
		Headers: []string{
			"object", "block", "row",
			"col_0", "col_1", "col_2", "col_3",
			"att_relname_id", "att_relname", "attname", "atttyp", "attnum",
			"col_9", "col_10", "col_11", "col_12", "col_13", "col_14", "col_15", "col_16", "col_17",
			"att_is_hidden",
		},
		Rows: [][]string{
			{"obj1", "0", "0", "", "", "", "", "12345", "my_table", "id", "BIGINT", "1", "", "", "", "", "", "", "", "", "", "0"},
			{"obj1", "0", "1", "", "", "", "", "12345", "my_table", "name", "VARCHAR(100)", "2", "", "", "", "", "", "", "", "", "", "0"},
		},
	}

	cols := buildColumnsFromMoColumnsRows(view, 12345)
	require.Len(t, cols, 2)
	assert.Equal(t, 0, cols[0].PhysicalPosition)
	assert.Equal(t, 1, cols[1].PhysicalPosition)
}

// TestLogicalTableViewWithSchema_mergeHeaders merges schema column names using position-based mapping.
func TestLogicalTableViewWithSchema_mergeHeaders(t *testing.T) {
	schema := &TableSchema{
		TableName: "t1",
		Columns: []TableColumn{
			{Name: "id", SQLType: "BIGINT", Position: 1, PhysicalPosition: 0},
			{Name: "val", SQLType: "INT", Position: 2, PhysicalPosition: 1},
		},
	}
	view := &LogicalTableView{
		Headers: []string{"object", "block", "row", "col_0", "col_1"},
		Rows: [][]string{
			{"obj1", "0", "0", "42", "100"},
		},
	}

	merged := MergeLogicalViewWithSchema(view, schema)
	assert.Equal(t, []string{"id", "val"}, merged.Headers)
	// Data rows only include columns matching schema positions
	require.Len(t, merged.Rows, 1)
	assert.Equal(t, []string{"42", "100"}, merged.Rows[0])
}

func TestMergeLogicalViewWithSchema_requiresResolvedSchema(t *testing.T) {
	view := &LogicalTableView{
		Headers: []string{"object", "block", "row", "col_0", "col_1"},
		Rows: [][]string{
			{"obj1", "0", "0", "42", "100"},
		},
		VisibleRows:  1,
		DeletedRows:  0,
		PhysicalRows: 1,
	}

	merged := MergeLogicalViewWithSchema(view, &TableSchema{})
	assert.Empty(t, merged.Headers)
	assert.Empty(t, merged.Rows)
	assert.Equal(t, 1, merged.VisibleRows)
	assert.Equal(t, 1, merged.PhysicalRows)
}

func TestMergeLogicalViewWithSchema_UsesPhysicalPosition(t *testing.T) {
	schema := &TableSchema{
		TableName: "shifted",
		Columns: []TableColumn{
			{Name: "id", SQLType: "VARCHAR(36)", Position: 1, PhysicalPosition: 4},
			{Name: "task_id", SQLType: "VARCHAR(36)", Position: 2, PhysicalPosition: 5},
		},
	}
	view := &LogicalTableView{
		Headers: []string{"object", "block", "row", "col_0", "col_1", "col_2", "col_3", "col_4", "col_5"},
		Rows: [][]string{
			{"obj1", "0", "0", "ignored0", "ignored1", "ignored2", "ignored3", "case-001", "task-001"},
		},
	}

	merged := MergeLogicalViewWithSchema(view, schema)
	require.Len(t, merged.Rows, 1)
	assert.Equal(t, []string{"case-001", "task-001"}, merged.Rows[0])
}

// TestMergeLogicalViewWithSchema_hiddenColumns verifies that hidden columns (e.g. __mo_fake_pk_col,
// __mo_cpkey_col) are excluded from the merged view using position-based column mapping.
func TestMergeLogicalViewWithSchema_hiddenColumns(t *testing.T) {
	// Simulate: physical columns = [__mo_fake_pk_col(hidden,pos=0), id(pos=1), name(pos=2), __mo_cpkey_col(hidden,pos=3)]
	// Schema only has visible columns: id(pos=1), name(pos=2)
	schema := &TableSchema{
		TableName: "users",
		Columns: []TableColumn{
			{Name: "id", SQLType: "BIGINT", Position: 1, PhysicalPosition: 1},
			{Name: "name", SQLType: "VARCHAR(100)", Position: 2, PhysicalPosition: 2},
		},
	}

	// The LogicalTableView has all 4 physical columns + 3 meta columns = 7 total
	view := &LogicalTableView{
		Headers: []string{"object", "block", "row", "col_0", "col_1", "col_2", "col_3"},
		Rows: [][]string{
			{"obj1", "0", "0", "fake_pk_123", "1", "Alice", "cpkey_456"},
			{"obj1", "0", "1", "fake_pk_789", "2", "Bob", "cpkey_999"},
		},
	}

	merged := MergeLogicalViewWithSchema(view, schema)

	// Headers should only contain visible columns
	assert.Equal(t, []string{"id", "name"}, merged.Headers)

	// Data rows: only columns at positions 1 and 2 (skip pos 0 and pos 3)
	require.Len(t, merged.Rows, 2)
	assert.Equal(t, []string{"1", "Alice"}, merged.Rows[0])
	assert.Equal(t, []string{"2", "Bob"}, merged.Rows[1])

	// Full CSV round-trip
	var buf bytes.Buffer
	err := WriteCSV(&buf, schema, view)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "id,name")
	assert.Contains(t, output, "1,Alice")
	assert.Contains(t, output, "2,Bob")
	assert.NotContains(t, output, "fake_pk") // hidden columns excluded
	assert.NotContains(t, output, "cpkey")   // hidden columns excluded
}

func TestMergeLogicalViewWithSchema_LargeSparsePhysicalPositions(t *testing.T) {
	const physicalCols = 128
	headers := []string{"object", "block", "row"}
	row := []string{"obj1", "0", "0"}
	for i := 0; i < physicalCols; i++ {
		headers = append(headers, fmt.Sprintf("col_%d", i))
		row = append(row, fmt.Sprintf("v%d", i))
	}

	schema := &TableSchema{
		TableName: "wide_table",
		Columns: []TableColumn{
			{Name: "first", Position: 1, PhysicalPosition: 0},
			{Name: "middle", Position: 2, PhysicalPosition: 63},
			{Name: "last", Position: 3, PhysicalPosition: 127},
		},
	}
	view := &LogicalTableView{
		Headers: headers,
		Rows:    [][]string{row},
	}

	merged := MergeLogicalViewWithSchema(view, schema)
	require.Len(t, merged.Rows, 1)
	assert.Equal(t, []string{"v0", "v63", "v127"}, merged.Rows[0])
}

// makeColumnRow creates a mock mo_columns row with specific values at given indexes.
func makeColumnRow(indexValuePairs ...any) []string {
	// 27 columns in mo_columns
	row := make([]string, 27)
	for i := range row {
		row[i] = ""
	}
	for i := 0; i < len(indexValuePairs); i += 2 {
		idx := indexValuePairs[i].(int)
		val := indexValuePairs[i+1].(string)
		row[idx] = val
	}
	// set a default att_relname_id if not provided
	if row[4] == "" {
		row[4] = "12345"
	}
	return row
}

// TestColumnSchemaRoundTrip tests the full pipeline: rows → columns → schema → CSV header.
func TestColumnSchemaRoundTrip(t *testing.T) {
	moColumnsView := &LogicalTableView{
		Headers: []string{
			"object", "block", "row",
			"col_0", "col_1", "col_2", "col_3",
			"att_relname_id", "att_relname", "attname", "atttyp", "attnum",
			"col_9", "col_10", "col_11", "col_12", "col_13", "col_14", "col_15", "col_16", "col_17",
			"att_is_hidden",
			"col_19", "col_20", "col_21", "att_seqnum",
		},
		Rows: [][]string{
			{"obj1", "0", "0", "", "", "", "", "12345", "my_table", "a", "INT", "1", "", "", "", "", "", "", "", "", "", "0", "", "", "", "0"},
			{"obj1", "0", "1", "", "", "", "", "12345", "my_table", "b", "TEXT", "2", "", "", "", "", "", "", "", "", "", "0", "", "", "", "1"},
			{"obj1", "0", "2", "", "", "", "", "12345", "my_table", "c", "FLOAT", "3", "", "", "", "", "", "", "", "", "", "0", "", "", "", "2"},
		},
	}

	cols := buildColumnsFromMoColumnsRows(moColumnsView, 12345)
	require.Len(t, cols, 3)

	schema := &TableSchema{
		TableName:    "roundtrip",
		DatabaseName: "test",
		Columns:      cols,
		CreateSQL:    "CREATE TABLE roundtrip (a INT, b TEXT, c FLOAT)",
	}

	dataView := &LogicalTableView{
		Headers: []string{"object", "block", "row", "col_0", "col_1", "col_2"},
		Rows: [][]string{
			{"obj1", "0", "0", "1", "hello", "1.500000"},
			{"obj1", "0", "1", "2", "world", "2.000000"},
		},
	}

	var buf bytes.Buffer
	err := WriteCSV(&buf, schema, dataView)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "a,b,c")
	assert.Contains(t, output, "1,hello,1.500000")

	// Verify comments are present
	assert.Contains(t, output, "-- CREATE TABLE roundtrip")
	assert.Contains(t, output, "-- Database: test")
	fmt.Println(output)
}

func TestBuildCreateTableFromMoColumns(t *testing.T) {
	// Simulate mo_columns data for a user table (id=100) with 3 columns
	view := &LogicalTableView{
		Headers: []string{
			"object", "block", "row",
			"att_relname_id", "attname", "atttyp", "attnum", "att_is_hidden",
		},
		Rows: [][]string{
			// id BIGINT, pos=0
			{"", "0", "0", "100", "id", "BIGINT", "1", "0"},
			// name VARCHAR(100), pos=1
			{"", "0", "1", "100", "name", "VARCHAR(100)", "2", "0"},
			// __mo_fake_pk_col, pos=2, hidden -> skipped
			{"", "0", "2", "100", "__mo_fake_pk_col", "VARCHAR(65535)", "3", "1"},
		},
	}

	ddl := buildCreateTableFromMoColumns(view, 100)
	assert.NotEmpty(t, ddl)
	assert.Contains(t, ddl, "BIGINT")
	assert.Contains(t, ddl, "VARCHAR(100)")
	assert.NotContains(t, ddl, "__mo_fake_pk_col")
	assert.Contains(t, ddl, "CREATE TABLE")
}

func TestBuildCreateTableFromMoColumns_empty(t *testing.T) {
	view := &LogicalTableView{
		Headers: []string{"object", "block", "row", "att_relname_id", "attname", "atttyp", "attnum", "att_is_hidden"},
		Rows: [][]string{
			{"", "0", "0", "200", "x", "INT", "1", "0"},
		},
	}
	ddl := buildCreateTableFromMoColumns(view, 999) // different table id
	assert.Empty(t, ddl)
}

func TestHardcodedCreateTable(t *testing.T) {
	tests := []struct {
		tableID  uint64
		wantName string
	}{
		{catalog.MO_TABLES_ID, "mo_tables"},
		{catalog.MO_COLUMNS_ID, "mo_columns"},
		{catalog.MO_DATABASE_ID, "mo_database"},
		{999999, ""}, // unknown table should return empty
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("table-%d", tt.tableID), func(t *testing.T) {
			ddl := hardcodedCreateTable(tt.tableID)
			if tt.wantName == "" {
				assert.Empty(t, ddl)
			} else {
				assert.Contains(t, ddl, tt.wantName)
				assert.Contains(t, ddl, "CREATE TABLE")
			}
		})
	}
}
