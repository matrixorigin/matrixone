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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func encodedSQLType(t *testing.T, typ types.Type) string {
	t.Helper()
	data, err := types.Encode(&typ)
	require.NoError(t, err)
	return string(data)
}

func encodedDefault(t *testing.T, origin string, nullable bool) string {
	t.Helper()
	data, err := types.Encode(&plan.Default{OriginString: origin, NullAbility: nullable})
	require.NoError(t, err)
	return string(data)
}

func encodedConstraint(t *testing.T, constraints ...engine.Constraint) string {
	t.Helper()
	def := &engine.ConstraintDef{
		Cts: constraints,
	}
	data, err := def.MarshalBinary()
	require.NoError(t, err)
	return string(data)
}

func encodedIndexConstraint(t *testing.T, indexes ...*plan.IndexDef) string {
	t.Helper()
	return encodedConstraint(t, &engine.IndexDef{Indexes: indexes})
}

func encodedPrimaryKeyConstraint(t *testing.T, names ...string) engine.Constraint {
	t.Helper()
	pkeyColName := ""
	if len(names) == 1 {
		pkeyColName = names[0]
	} else if len(names) > 1 {
		pkeyColName = catalog.CPrimaryKeyColName
	}
	return &engine.PrimaryKeyDef{
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: pkeyColName,
			Names:       names,
		},
	}
}

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

func TestDumpPreparedTableCSV_EmptyTableWritesHeader(t *testing.T) {
	reader := &CheckpointReader{}
	data := &TableDumpData{
		TableID: 334019,
		Schema: &TableSchema{
			TableName:    "t_empty",
			DatabaseName: "ckp_tables",
			Columns: []TableColumn{
				{Name: "id", SQLType: "INT", Position: 1, PhysicalPosition: 0},
				{Name: "name", SQLType: "VARCHAR(64)", Position: 2, PhysicalPosition: 1},
			},
		},
	}

	var buf bytes.Buffer
	err := reader.DumpPreparedTableCSV(
		context.Background(),
		&buf,
		data,
		types.TS{},
		WithCSVMetaComments(false),
		WithCSVHeader(true),
	)
	require.NoError(t, err)
	assert.Equal(t, "id,name\n", buf.String())
}

func TestIsTableDataUnavailable(t *testing.T) {
	assert.False(t, isTableDataUnavailable(assert.AnError))
	assert.False(t, isTableDataUnavailable(nil))
	assert.True(t, isTableDataUnavailable(fmt.Errorf("internal error: table 334019 not found in checkpoint at ts 1-0")))
	assert.True(t, isTableDataUnavailable(fmt.Errorf("internal error: no data entries for table 334019")))
	assert.False(t, isTableDataUnavailable(fmt.Errorf("compose checkpoint failed")))
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
	assert.Equal(t, `1,"Alice",30`, dataLines[1])
	assert.Equal(t, `2,"Bob",25`, dataLines[2])
	assert.Equal(t, `3,"Charlie, Jr.",35`, dataLines[3])
	assert.Equal(t, `4,"NULL",NULL`, dataLines[4])
}

func TestWriteProjectedCSVRowFromVecsFastPath(t *testing.T) {
	mp := mpool.MustNewZero()
	vecInt := vector.NewVec(types.T_int64.ToType())
	vecDecimal := vector.NewVec(types.T_decimal64.ToTypeWithScale(2))
	vecDecimal128 := vector.NewVec(types.T_decimal128.ToTypeWithScale(5))
	vecDate := vector.NewVec(types.T_date.ToType())
	vecString := vector.NewVec(types.T_varchar.ToType())
	vecNull := vector.NewVec(types.T_int32.ToType())
	defer vecInt.Free(mp)
	defer vecDecimal.Free(mp)
	defer vecDecimal128.Free(mp)
	defer vecDate.Free(mp)
	defer vecString.Free(mp)
	defer vecNull.Free(mp)

	require.NoError(t, vector.AppendFixed(vecInt, int64(-42), false, mp))
	decimal, err := types.ParseDecimal64("-123.40", 18, 2)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(vecDecimal, decimal, false, mp))
	decimal128, err := types.ParseDecimal128("123456789012345.67890", 30, 5)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(vecDecimal128, decimal128, false, mp))
	require.NoError(t, vector.AppendFixed(vecDate, types.DateFromCalendar(2024, 6, 1), false, mp))
	require.NoError(t, vector.AppendBytes(vecString, []byte(`a"b\c`), false, mp))
	require.NoError(t, vector.AppendFixed(vecNull, int32(0), true, mp))

	var buf bytes.Buffer
	err = writeProjectedCSVRowFromVecs(
		&buf,
		[]types.Type{
			types.T_int64.ToType(),
			types.T_decimal64.ToTypeWithScale(2),
			types.T_decimal128.ToTypeWithScale(5),
			types.T_date.ToType(),
			types.T_varchar.ToType(),
			types.T_int32.ToType(),
		},
		[]*vector.Vector{vecInt, vecDecimal, vecDecimal128, vecDate, vecString, vecNull},
		[]int{0, 1, 2, 3, 4, 5},
		0,
	)
	require.NoError(t, err)
	assert.Equal(t, "-42,-123.40,123456789012345.67890,2024-06-01,\"a\"\"b\\\\c\",\\N\n", buf.String())
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

func TestWriteCSV_WithoutMetadataComments(t *testing.T) {
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
	err := WriteCSV(&buf, schema, view, WithCSVMetaComments(false))
	require.NoError(t, err)

	output := strings.TrimSpace(buf.String())
	assert.Equal(t, "id\n42", output)
	assert.NotContains(t, output, "-- CREATE TABLE")
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
			"col_19", "col_20", "col_21", "attr_seqnum",
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

func TestBuildColumnsFromMoColumnsRows_GenericPreCPKWithPhysicalPrefix(t *testing.T) {
	const (
		tableID = uint64(12345)
		offset  = 1
	)

	headers := []string{"object", "block", "row"}
	for i := 0; i < len(preCPKLayout.moColumnsSchema)+offset; i++ {
		headers = append(headers, fmt.Sprintf("col_%d", i))
	}

	row := func(metaRow, name, typ, attnum, hidden, seqnum string) []string {
		data := make([]string, len(preCPKLayout.moColumnsSchema)+offset)
		setCatalogValue := func(colName, value string) {
			idx := catalogColIndexForLayout(preCPKLayout, moColumnsID, colName, offset)
			require.GreaterOrEqual(t, idx, 0)
			data[idx] = value
		}
		data[0] = "physical-prefix"
		setCatalogValue("att_relname_id", fmt.Sprintf("%d", tableID))
		setCatalogValue("attname", name)
		setCatalogValue("atttyp", typ)
		setCatalogValue("attnum", attnum)
		setCatalogValue("att_is_hidden", hidden)
		setCatalogValue("attr_seqnum", seqnum)
		return append([]string{"obj1", "0", metaRow}, data...)
	}

	view := &LogicalTableView{
		Headers: headers,
		Rows: [][]string{
			row("0", "id", "INT", "1", "0", "0"),
			row("1", "name", "VARCHAR(100)", "2", "0", "1"),
			row("2", "__mo_rowid", "ROWID", "0", "1", "2"),
		},
	}

	cols := buildColumnsFromMoColumnsRows(view, tableID)
	require.Len(t, cols, 2)
	assert.Equal(t, "id", cols[0].Name)
	assert.Equal(t, 0, cols[0].PhysicalPosition)
	assert.Equal(t, "name", cols[1].Name)
	assert.Equal(t, 1, cols[1].PhysicalPosition)

	ddl := buildCreateTableFromMoColumns(view, tableID)
	assert.Contains(t, ddl, "`id` INT")
	assert.Contains(t, ddl, "`name` VARCHAR(100)")
	assert.NotContains(t, ddl, "__mo_rowid")
}

func TestCreateTableDDLFromCatalogViews_PrefersMoColumnsOverStaleCreateSQL(t *testing.T) {
	moTablesView := &LogicalTableView{
		Headers: []string{
			"object", "block", "row",
			"rel_id", "relname", "reldatabase", "reldatabase_id",
			"col_4", "col_5", "col_6", "rel_createsql",
		},
		Rows: [][]string{
			{
				"obj1", "0", "0",
				"12345", "t", "db", "100",
				"", "", "", "CREATE TABLE t (a INT, b VARCHAR(100))",
			},
		},
	}
	moColumnsView := &LogicalTableView{
		Headers: []string{"object", "block", "row", "att_relname_id", "attname", "atttyp", "attnum", "att_is_hidden"},
		Rows: [][]string{
			{"obj1", "0", "0", "12345", "a", encodedSQLType(t, types.T_int32.ToType()), "1", "0"},
			{"obj1", "0", "1", "12345", "c", encodedSQLType(t, types.T_int64.ToType()), "2", "0"},
		},
	}

	ddl := createTableDDLFromCatalogViews(12345, moTablesView, moColumnsView)
	assert.Contains(t, ddl, "CREATE TABLE `t`")
	assert.Contains(t, ddl, "`a` INT")
	assert.Contains(t, ddl, "`c` BIGINT")
	assert.NotContains(t, ddl, "`b`")
}

func TestCreateTableDDLFromCatalogViews_DoesNotFallbackToRelCreateSQL(t *testing.T) {
	moTablesView := &LogicalTableView{
		Headers: []string{
			"object", "block", "row",
			"rel_id", "relname", "reldatabase", "reldatabase_id",
			"col_4", "col_5", "col_6", "rel_createsql",
		},
		Rows: [][]string{
			{
				"obj1", "0", "0",
				"12345", "employees", "db", "100",
				"", "", "", "CREATE TABLE employees (id INT)",
			},
		},
	}

	assert.Empty(t, createTableDDLFromCatalogViews(12345, moTablesView, nil))
}

func TestCreateTableDDLFromCatalogViews_DecodesMoColumnTypes(t *testing.T) {
	moTablesView := &LogicalTableView{
		Headers: []string{
			"object", "block", "row",
			"rel_id", "relname", "reldatabase", "reldatabase_id",
			"col_4", "col_5", "col_6", "rel_createsql",
		},
		Rows: [][]string{
			{
				"obj1", "0", "0",
				"333999", "parent", "ckp_constraints", "333997",
				"", "", "", "CREATE TABLE `ckp_constraints`.`parent` (`old` INT)",
			},
		},
	}
	moColumnsView := &LogicalTableView{
		Headers: []string{"object", "block", "row", "att_relname_id", "attname", "atttyp", "attnum", "att_is_hidden"},
		Rows: [][]string{
			{"obj1", "0", "0", "333999", "id", encodedSQLType(t, types.T_int32.ToType()), "1", "0"},
			{"obj1", "0", "1", "333999", "code", encodedSQLType(t, types.New(types.T_varchar, 20, 0)), "2", "0"},
		},
	}

	ddl := createTableDDLFromCatalogViews(333999, moTablesView, moColumnsView)
	assert.Contains(t, ddl, "CREATE TABLE `parent`")
	assert.Contains(t, ddl, "`id` INT")
	assert.Contains(t, ddl, "`code` VARCHAR(20)")
	assert.NotContains(t, ddl, "`old`")
}

func TestCreateTableDDLFromCatalogViews_IncludesColumnAndTableAttributes(t *testing.T) {
	moTablesView := &LogicalTableView{
		Headers: []string{
			"object", "block", "row",
			"rel_id", "relname", "reldatabase", "reldatabase_id",
			"relpersistence", "relkind", "rel_comment", "rel_createsql", "constraint",
		},
		Rows: [][]string{
			{
				"obj1", "0", "0",
				"333999", "parent", "ckp_constraints", "333997",
				"", "r", "parent table comment", "CREATE TABLE `ckp_constraints`.`parent` (`old` INT)", encodedConstraint(
					t,
					encodedPrimaryKeyConstraint(t, "id"),
					&engine.IndexDef{Indexes: []*plan.IndexDef{
						{IndexName: "code", Parts: []string{"code"}, Unique: true},
						{IndexName: "idx_parent_note", Parts: []string{"note"}},
						{IndexName: "ivf_500", Parts: []string{"embedding"}, IndexAlgo: "ivfflat", IndexAlgoParams: `{"lists":"500","op_type":"vector_l2_ops"}`},
						{IndexName: "ivf_2000", Parts: []string{"embedding"}, IndexAlgo: "ivfflat", IndexAlgoParams: `{"lists":"2000","op_type":"vector_l2_ops"}`},
					}},
				),
			},
		},
	}

	headers := append([]string{"object", "block", "row"}, catalog.MoColumnsSchema...)
	row := func(name, typ, attnum, notNull, hasDefault, defaultExpr, constraintType, comment, seqnum string) []string {
		data := make([]string, len(catalog.MoColumnsSchema))
		set := func(colName, value string) {
			for i, header := range catalog.MoColumnsSchema {
				if header == colName {
					data[i] = value
					return
				}
			}
			t.Fatalf("missing mo_columns header %s", colName)
		}
		set(catalog.SystemColAttr_RelID, "333999")
		set(catalog.SystemColAttr_RelName, "parent")
		set(catalog.SystemColAttr_Name, name)
		set(catalog.SystemColAttr_Type, typ)
		set(catalog.SystemColAttr_Num, attnum)
		set(catalog.SystemColAttr_NullAbility, notNull)
		set(catalog.SystemColAttr_HasExpr, hasDefault)
		set(catalog.SystemColAttr_DefaultExpr, defaultExpr)
		set(catalog.SystemColAttr_ConstraintType, constraintType)
		set(catalog.SystemColAttr_Comment, comment)
		set(catalog.SystemColAttr_IsHidden, "0")
		set(catalog.SystemColAttr_Seqnum, seqnum)
		return append([]string{"obj1", "0", attnum}, data...)
	}
	moColumnsView := &LogicalTableView{
		Headers: headers,
		Rows: [][]string{
			row("id", encodedSQLType(t, types.T_int32.ToType()), "1", "1", "0", "", "p", "", "0"),
			row("code", encodedSQLType(t, types.New(types.T_varchar, 20, 0)), "2", "1", "0", "", "", "", "1"),
			row("note", encodedSQLType(t, types.New(types.T_varchar, 100, 0)), "3", "0", "1", encodedDefault(t, "'parent-default'", true), "", "parent note", "2"),
			row("embedding", encodedSQLType(t, types.New(types.T_array_float32, 128, 0)), "4", "0", "0", "", "", "", "3"),
		},
	}
	partitionMetadataView := &LogicalTableView{
		Headers: []string{
			"object", "block", "row",
			"table_id", "table_name", "database_name", "partition_method", "partition_description", "partition_count",
		},
		Rows: [][]string{
			{
				"obj1", "0", "0",
				"333999", "parent", "ckp_constraints", "Key", "key algorithm = 2 (`id`, `tenant_id`)", "4",
			},
		},
	}

	ddl := createTableDDLFromCatalogViews(333999, moTablesView, moColumnsView, partitionMetadataView)
	assert.Contains(t, ddl, "CREATE TABLE `parent`")
	assert.Contains(t, ddl, "`id` INT NOT NULL")
	assert.Contains(t, ddl, "`code` VARCHAR(20) NOT NULL")
	assert.Contains(t, ddl, "`note` VARCHAR(100) DEFAULT 'parent-default' COMMENT 'parent note'")
	assert.Contains(t, ddl, "PRIMARY KEY (`id`)")
	assert.Contains(t, ddl, "UNIQUE KEY `code`(`code`)")
	assert.Contains(t, ddl, "KEY `idx_parent_note`(`note`)")
	assert.Contains(t, ddl, "KEY `ivf_500` USING ivfflat(`embedding`) lists = 500  op_type 'vector_l2_ops'")
	assert.Contains(t, ddl, "KEY `ivf_2000` USING ivfflat(`embedding`) lists = 2000  op_type 'vector_l2_ops'")
	assert.Contains(t, ddl, "COMMENT='parent table comment'")
	assert.Contains(t, ddl, "partition by key algorithm = 2 (`id`, `tenant_id`) partitions 4")
	assert.NotContains(t, ddl, "`old`")
}

func TestBuildPartitionClauseFromMetadata_UsesSeqNumsWithHiddenColumn(t *testing.T) {
	view := &LogicalTableView{
		Headers: append([]string{"object", "block", "row"}, "col_0", "col_1", "col_2", "col_3", "col_4", "col_5", "col_6"),
		ColSeqNums: []uint16{
			6, // hidden/cpkey column before visible catalog columns
			0, 1, 2, 3, 4, 5,
		},
		Rows: [][]string{
			{
				"obj1", "0", "0",
				"hidden-key",
				"334023", "t_hash_partition", "ckp_tables", "Hash", "hash (`id`)", "4",
			},
		},
	}
	applyCatalogHeadersBySeqNums(view, moPartitionMetadataHeaders)

	assert.Equal(t, "partition by hash (`id`) partitions 4", buildPartitionClauseFromMetadata(view, 334023))
}

func TestBuildPartitionClauseFromMetadata_FallsBackToRowShape(t *testing.T) {
	view := &LogicalTableView{
		Headers: []string{"object", "block", "row", "col_0", "col_1", "col_2", "col_3", "col_4", "col_5"},
		Rows: [][]string{
			{
				"obj1", "0", "0",
				"272577", "t_hash_partition", "ckp_tables", "Hash", "hash (`id`)", "4",
			},
		},
	}

	assert.Equal(t, "partition by hash (`id`) partitions 4", buildPartitionClauseFromMetadata(view, 272577))
}

func TestBuildPartitionTableIDMap(t *testing.T) {
	view := &LogicalTableView{
		Headers: append([]string{"object", "block", "row"}, moPartitionTablesHeaders...),
		Rows: [][]string{
			{"obj1", "0", "0", "272578", "%!%p0%!%t_hash_partition", "272577", "p0", "0", "", ""},
			{"obj1", "0", "1", "272579", "%!%p1%!%t_hash_partition", "272577", "p1", "1", "", ""},
			{"obj1", "0", "2", "272580", "%!%p0%!%other", "999999", "p0", "0", "", ""},
		},
	}

	got := buildPartitionTableIDMap(view, map[uint64]struct{}{272577: {}})
	assert.Equal(t, []uint64{272578, 272579}, got[272577])
}

func TestBuildPartitionTableIDMap_FallsBackToRowShape(t *testing.T) {
	view := &LogicalTableView{
		Headers: []string{"object", "block", "row", "col_0", "col_1", "col_2", "col_3", "col_4", "col_5"},
		Rows: [][]string{
			{"obj1", "0", "1", "334040", "%!%p1%!%t_hash_partition", "334039", "p1", "1", "", ""},
			{"obj1", "0", "0", "334041", "%!%p0%!%t_hash_partition", "334039", "p0", "0", "", ""},
		},
	}

	got := buildPartitionTableIDMap(view, map[uint64]struct{}{334039: {}})
	assert.Equal(t, []uint64{334041, 334040}, got[334039])
}

func TestCreateTableDDLFromCatalogViews_IgnoresMoColumnsPrimaryWithoutConstraint(t *testing.T) {
	moTablesView := &LogicalTableView{
		Headers: []string{
			"object", "block", "row",
			"rel_id", "relname", "reldatabase", "reldatabase_id",
			"relpersistence", "relkind", "rel_comment", "rel_createsql", "constraint",
		},
		Rows: [][]string{
			{
				"obj1", "0", "0",
				"334100", "t_int_signed", "ckp_types", "334099",
				"", "r", "", "", "",
			},
		},
	}

	headers := append([]string{"object", "block", "row"}, catalog.MoColumnsSchema...)
	row := func(name, typ, attnum, notNull, constraintType, seqnum string) []string {
		data := make([]string, len(catalog.MoColumnsSchema))
		set := func(colName, value string) {
			for i, header := range catalog.MoColumnsSchema {
				if header == colName {
					data[i] = value
					return
				}
			}
			t.Fatalf("missing mo_columns header %s", colName)
		}
		set(catalog.SystemColAttr_RelID, "334100")
		set(catalog.SystemColAttr_RelName, "t_int_signed")
		set(catalog.SystemColAttr_Name, name)
		set(catalog.SystemColAttr_Type, typ)
		set(catalog.SystemColAttr_Num, attnum)
		set(catalog.SystemColAttr_NullAbility, notNull)
		set(catalog.SystemColAttr_ConstraintType, constraintType)
		set(catalog.SystemColAttr_IsHidden, "0")
		set(catalog.SystemColAttr_Seqnum, seqnum)
		return append([]string{"obj1", "0", attnum}, data...)
	}
	moColumnsView := &LogicalTableView{
		Headers: headers,
		Rows: [][]string{
			row("id", encodedSQLType(t, types.T_int32.ToType()), "1", "1", "p", "0"),
			row("c_tiny", encodedSQLType(t, types.T_int8.ToType()), "2", "0", "", "1"),
			row("c_small", encodedSQLType(t, types.T_int16.ToType()), "3", "0", "", "2"),
			row("c_int", encodedSQLType(t, types.T_int32.ToType()), "4", "0", "", "3"),
			row("c_big", encodedSQLType(t, types.T_int64.ToType()), "5", "0", "", "4"),
		},
	}

	ddl := createTableDDLFromCatalogViews(334100, moTablesView, moColumnsView)
	assert.Contains(t, ddl, "`id` INT NOT NULL")
	assert.NotContains(t, ddl, "PRIMARY KEY")
}

func TestRenderCreateTableDDLFromSchema_PrefersColumnsOverCreateSQL(t *testing.T) {
	ddl := RenderCreateTableDDLFromSchema(&TableSchema{
		TableName: "t",
		CreateSQL: "CREATE TABLE t (a INT, b VARCHAR(100))",
		Columns: []TableColumn{
			{Name: "a", SQLType: "INT", Position: 1},
			{Name: "c", SQLType: "BIGINT", Position: 2},
		},
	})

	assert.Contains(t, ddl, "`a` INT")
	assert.Contains(t, ddl, "`c` BIGINT")
	assert.NotContains(t, ddl, "`b`")
}

func TestRenderCreateTableDDLFromSchema_DoesNotFallbackToCreateSQLWhenColumnTypesAreBinary(t *testing.T) {
	ddl := RenderCreateTableDDLFromSchema(&TableSchema{
		TableName: "t",
		CreateSQL: "CREATE TABLE t (a INT)",
		Columns: []TableColumn{
			{Name: "a", SQLType: string([]byte{'A', 0, 0xff})},
		},
	})

	assert.Empty(t, ddl)
}

func TestIsPrintableCreateTableSQLAcceptsExternalTable(t *testing.T) {
	assert.True(t, isPrintableCreateTableSQL("CREATE EXTERNAL TABLE ext_csv (id INT) INFILE {'filepath'='/tmp/ext.csv','format'='csv'}"))
}

func TestIsPrintableExternalParamJSON(t *testing.T) {
	assert.True(t, isPrintableExternalParamJSON(`{"ExParamConst":{"Option":["filepath","/tmp/ext.csv","format","csv"]}}`))
	assert.False(t, isPrintableExternalParamJSON(`CREATE EXTERNAL TABLE ext_csv (id INT)`))
}

func TestFindTableSchemaFromMoTablesUsesCatalogLayoutFallback(t *testing.T) {
	schema := schemaForLayout(currentCatalogLayout, moTablesID)
	headers := append([]string{}, logicalTableViewMetaHeaders...)
	for range schema {
		headers = append(headers, "")
	}
	row := make([]string, len(headers))
	row[0], row[1], row[2] = "obj", "0", "0"
	data := row[logicalViewMetaCols:]
	for i, name := range schema {
		switch name {
		case "rel_id":
			data[i] = "272746"
		case "relname":
			data[i] = "ext_csv_local"
		case "reldatabase":
			data[i] = "ckp25010_external"
		case "rel_createsql":
			data[i] = `{"ExParamConst":{"Option":["filepath","/tmp/ext.csv","format","csv"]}}`
		}
	}

	got := findTableSchemaFromMoTables(&LogicalTableView{Headers: headers, Rows: [][]string{row}}, 272746)
	require.NotNil(t, got)
	assert.Equal(t, "ext_csv_local", got.TableName)
	assert.Equal(t, "ckp25010_external", got.DatabaseName)
	assert.Contains(t, got.CreateSQL, "ExParamConst")
	assert.Contains(t, got.CreateSQL, "filepath")
}

func TestRenderCreateTableDDLFromSchema_EnumSetValues(t *testing.T) {
	ddl := RenderCreateTableDDLFromSchema(&TableSchema{
		TableName: "t_enum_set",
		Columns: []TableColumn{
			{Name: "c_enum", SQLType: "ENUM", EnumValues: "'red','blue'", Position: 1},
			{Name: "c_set", SQLType: "SET", EnumValues: "('x','y')", Position: 2},
		},
	})

	assert.Contains(t, ddl, "`c_enum` ENUM('red','blue')")
	assert.Contains(t, ddl, "`c_set` SET('x','y')")
}

func TestDecodeMoColumnEncodedSQLType_TemporalScale(t *testing.T) {
	sqlType, ok := decodeMoColumnEncodedSQLType(encodedSQLType(t, types.New(types.T_time, 0, 6)))
	require.True(t, ok)
	assert.Equal(t, "TIME(6)", sqlType)

	sqlType, ok = decodeMoColumnEncodedSQLType(encodedSQLType(t, types.New(types.T_datetime, 0, 3)))
	require.True(t, ok)
	assert.Equal(t, "DATETIME(3)", sqlType)

	sqlType, ok = decodeMoColumnEncodedSQLType(encodedSQLType(t, types.New(types.T_timestamp, 0, 6)))
	require.True(t, ok)
	assert.Equal(t, "TIMESTAMP(6)", sqlType)
}

func TestBuildCatalogTablesFromMoTablesRows_GenericWithTrailingColumns(t *testing.T) {
	headers := []string{"object", "block", "row"}
	for i := 0; i < len(preCPKLayout.moTablesSchema)+2; i++ {
		headers = append(headers, fmt.Sprintf("col_%d", i))
	}
	row := func(relID, relName, dbName, dbID, relKind, accountID string) []string {
		data := make([]string, len(preCPKLayout.moTablesSchema)+2)
		data[0] = relID
		data[1] = relName
		data[2] = dbName
		data[3] = dbID
		data[5] = relKind
		data[11] = accountID
		return append([]string{"obj1", "0", relID}, data...)
	}
	view := &LogicalTableView{
		Headers: headers,
		Rows: [][]string{
			row("1001", "orders", "tpch_10g", "9001", "r", "7"),
			row("1002", "revenue0", "tpch_10g", "9001", "v", "7"),
		},
	}

	tables := buildCatalogTablesFromMoTablesRows(view)
	require.Len(t, tables, 2)
	assert.Equal(t, uint64(1001), tables[0].TableID)
	assert.Equal(t, uint32(7), tables[0].AccountID)
	assert.Equal(t, uint64(9001), tables[0].DatabaseID)
	assert.Equal(t, "tpch_10g", tables[0].DatabaseName)
	assert.Equal(t, "orders", tables[0].TableName)
	assert.Equal(t, "r", tables[0].RelKind)
	assert.Equal(t, "revenue0", tables[1].TableName)
	assert.Equal(t, "v", tables[1].RelKind)
}

func TestBuildCatalogTablesFromMoTablesRows_UsesTemporaryDisplayName(t *testing.T) {
	headers := []string{"object", "block", "row"}
	for i := 0; i < len(preCPKLayout.moTablesSchema); i++ {
		headers = append(headers, fmt.Sprintf("col_%d", i))
	}
	data := make([]string, len(preCPKLayout.moTablesSchema))
	data[0] = "1003"
	data[1] = "__mo_tmp_123e4567e89b12d3a456426614174000_ckp_temp_t_session_only"
	data[2] = "ckp_temp"
	data[3] = "9002"
	data[5] = "r"
	data[11] = "7"
	view := &LogicalTableView{
		Headers: headers,
		Rows:    [][]string{append([]string{"obj1", "0", "0"}, data...)},
	}

	tables := buildCatalogTablesFromMoTablesRows(view)
	require.Len(t, tables, 1)
	assert.Equal(t, "t_session_only", tables[0].TableName)
}

func TestDisplayTableName(t *testing.T) {
	assert.Equal(t, "orders", displayTableName("db1", "orders"))
	assert.Equal(t, "t1", displayTableName("db1", "__mo_tmp_123e4567e89b12d3a456426614174000_db1_t1"))
	assert.Equal(t, "__mo_tmp_123e4567e89b12d3a456426614174000_other_t1", displayTableName("db1", "__mo_tmp_123e4567e89b12d3a456426614174000_other_t1"))
	assert.Equal(t, "__mo_tmp_bad", displayTableName("db1", "__mo_tmp_bad"))
}

func TestListCatalogTablesDoesNotFilterRelKinds(t *testing.T) {
	tables := []TableCatalogEntry{
		{TableID: 1, TableName: "orders", DatabaseName: "db1", RelKind: "r"},
		{TableID: 2, TableName: "ext_orders", DatabaseName: "db1", RelKind: "e"},
		{TableID: 3, TableName: "orders_v", DatabaseName: "db1", RelKind: "v"},
		{TableID: 4, TableName: "cluster_orders", DatabaseName: "db1", RelKind: "cluster"},
	}
	filtered := filterCatalogTablesForList(tables, TableListOptions{})

	require.Len(t, filtered, 4)
	names := make([]string, 0, len(filtered))
	for _, table := range filtered {
		names = append(names, table.TableName)
	}
	assert.ElementsMatch(t, []string{"orders", "ext_orders", "orders_v", "cluster_orders"}, names)
}

func TestIsMissingCheckpointTableError(t *testing.T) {
	err := moerr.NewInternalErrorf(context.Background(), "table %d not found in checkpoint at ts %s", uint64(272419), "1-0")
	assert.True(t, isMissingCheckpointTableError(err, 272419))
	assert.False(t, isMissingCheckpointTableError(err, 272420))
	assert.False(t, isMissingCheckpointTableError(nil, 272419))
}

func TestInferCatalogLayout(t *testing.T) {
	tests := []struct {
		name      string
		dataWidth int
		tableID   uint64
		layout    string
		offset    int
		ok        bool
	}{
		{name: "current_tables", dataWidth: len(catalog.MoTablesSchema), tableID: moTablesID, layout: currentCatalogLayout.name, offset: 0, ok: true},
		{name: "current_tables_fakepk", dataWidth: len(catalog.MoTablesSchema) + 1, tableID: moTablesID, layout: currentCatalogLayout.name, offset: 1, ok: true},
		{name: "current_columns_fakepk", dataWidth: len(catalog.MoColumnsSchema) + 1, tableID: moColumnsID, layout: currentCatalogLayout.name, offset: 1, ok: true},
		{name: "pre_cpk_tables", dataWidth: len(preCPKLayout.moTablesSchema), tableID: moTablesID, layout: preCPKLayout.name, offset: 0, ok: true},
		{name: "pre_cpk_columns", dataWidth: len(preCPKLayout.moColumnsSchema), tableID: moColumnsID, layout: preCPKLayout.name, offset: 0, ok: true},
		{name: "legacy3_tables", dataWidth: len(legacy3CatalogLayout.moTablesSchema), tableID: moTablesID, layout: legacy3CatalogLayout.name, offset: 0, ok: true},
		{name: "legacy3_columns", dataWidth: len(legacy3CatalogLayout.moColumnsSchema), tableID: moColumnsID, layout: legacy3CatalogLayout.name, offset: 0, ok: true},
		{name: "unknown_tables", dataWidth: 7, tableID: moTablesID, ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layout, offset, ok := inferCatalogLayout(tt.dataWidth, tt.tableID)
			assert.Equal(t, tt.ok, ok)
			if !tt.ok {
				return
			}
			assert.Equal(t, tt.layout, layout.name)
			assert.Equal(t, tt.offset, offset)
		})
	}
}

func TestFallbackCatalogColIndex_UnknownLayoutDoesNotGuessCurrent(t *testing.T) {
	view := &LogicalTableView{
		Headers: []string{"object", "block", "row", "col_0", "col_1", "col_2", "col_3", "col_4", "col_5", "col_6"},
	}
	assert.Equal(t, -1, fallbackCatalogColIndex(view, moTablesID, "rel_createsql"))
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
	assert.Contains(t, output, `1,"Alice"`)
	assert.Contains(t, output, `2,"Bob"`)
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

func TestBuildCreateIndexStatementsFromMoIndexes_IVFFlat(t *testing.T) {
	view := &LogicalTableView{
		Headers: []string{
			"table_id", "name", "type", "algo", "algo_params", "comment",
			"column_name", "ordinal_position", "hidden",
		},
		Rows: [][]string{
			{"272535", "PRIMARY", "PRIMARY", "", "", "", "id", "1", "0"},
			{"272535", "ivf_2000", "MULTIPLE", "ivfflat", `{"lists":"2000","op_type":"vector_l2_ops"}`, "", "embedding", "1", "0"},
			{"272535", "ivf_2000", "MULTIPLE", "ivfflat", `{"lists":"2000","op_type":"vector_l2_ops"}`, "", "embedding", "1", "0"},
			{"272535", "ivf_2000", "MULTIPLE", "ivfflat", `{"lists":"2000","op_type":"vector_l2_ops"}`, "", "__mo_alias_embedding", "1", "0"},
			{"999999", "other_idx", "MULTIPLE", "", "", "", "v", "1", "0"},
		},
	}

	stmts, err := buildCreateIndexStatementsFromMoIndexes(view, 272535, "items_gist")
	require.NoError(t, err)
	require.Equal(t, []string{
		"ALTER TABLE `items_gist` ADD KEY `ivf_2000` USING ivfflat(`embedding`) lists = 2000  op_type 'vector_l2_ops' ;",
	}, stmts)
}

func TestBuildCreateIndexStatementsFromMoIndexes_FullText(t *testing.T) {
	view := &LogicalTableView{
		Headers: []string{
			"table_id", "name", "type", "algo", "algo_params", "comment",
			"column_name", "ordinal_position", "hidden",
		},
		Rows: [][]string{
			{"272535", "idx_doc", "MULTIPLE", "fulltext", "", "", "doc", "1", "0"},
		},
	}

	stmts, err := buildCreateIndexStatementsFromMoIndexes(view, 272535, "t_fulltext")
	require.NoError(t, err)
	require.Equal(t, []string{
		"ALTER TABLE `t_fulltext` ADD FULLTEXT INDEX `idx_doc`(`doc`);",
	}, stmts)
}

func TestWriteCSV_LexicalRowOrder(t *testing.T) {
	schema := &TableSchema{
		TableName: "sorted",
		Columns: []TableColumn{
			{Name: "id", Position: 1, PhysicalPosition: 0},
			{Name: "name", Position: 2, PhysicalPosition: 1},
		},
	}
	view := &LogicalTableView{
		Headers: []string{"object", "block", "row", "col_0", "col_1"},
		Rows: [][]string{
			{"obj2", "0", "1", "2", "zeta"},
			{"obj1", "0", "0", "1", "beta"},
			{"obj3", "0", "0", "1", "alpha"},
		},
	}

	var buf bytes.Buffer
	err := WriteCSV(&buf, schema, view, WithCSVMetaComments(false), WithCSVRowOrder(CSVRowOrderLexical))
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Equal(t, []string{
		"id,name",
		"1,alpha",
		"1,beta",
		"2,zeta",
	}, lines)
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
			"col_19", "col_20", "col_21", "attr_seqnum",
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
	assert.Contains(t, output, `1,"hello",1.500000`)

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

func TestHardcodedCreateTableForLegacy3Dev(t *testing.T) {
	ddl := hardcodedCreateTableForLayout(catalog.MO_TABLES_ID, legacy3CatalogLayout)
	assert.Contains(t, ddl, "CREATE TABLE `mo_tables`")
	assert.NotContains(t, ddl, "rel_logical_id")
	assert.NotContains(t, ddl, "extra_info")
	assert.NotContains(t, ddl, catalog.SystemRelAttr_CPKey)

	ddl = hardcodedCreateTableForLayout(catalog.MO_COLUMNS_ID, legacy3CatalogLayout)
	assert.Contains(t, ddl, "CREATE TABLE `mo_columns`")
	assert.NotContains(t, ddl, "attr_generated")
	assert.NotContains(t, ddl, "attr_has_generated")
	assert.NotContains(t, ddl, catalog.SystemColAttr_CPKey)
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
			ddl := hardcodedCreateTableForLayout(tt.tableID, currentCatalogLayout)
			if tt.wantName == "" {
				assert.Empty(t, ddl)
			} else {
				assert.Contains(t, ddl, tt.wantName)
				assert.Contains(t, ddl, "CREATE TABLE")
			}
		})
	}
}

func TestBuiltinTableSchemaForLayout_CurrentVisibleColumnsOnly(t *testing.T) {
	schema := builtinTableSchemaForLayout(currentCatalogLayout, catalog.MO_TABLES_ID)
	require.NotNil(t, schema)
	assert.Equal(t, "mo_tables", schema.TableName)
	assert.Equal(t, "mo_catalog", schema.DatabaseName)
	assert.NotEmpty(t, schema.CreateSQL)

	var names []string
	for _, col := range schema.Columns {
		names = append(names, col.Name)
		assert.Equal(t, col.Position, col.PhysicalPosition)
	}
	assert.Contains(t, names, "rel_logical_id")
	assert.NotContains(t, names, "extra_info")
	assert.NotContains(t, names, catalog.SystemRelAttr_CPKey)
}

func TestBuiltinTableSchemaForLayout_Legacy3Compatibility(t *testing.T) {
	tablesSchema := builtinTableSchemaForLayout(legacy3CatalogLayout, catalog.MO_TABLES_ID)
	require.NotNil(t, tablesSchema)
	var tableNames []string
	for _, col := range tablesSchema.Columns {
		tableNames = append(tableNames, col.Name)
	}
	assert.NotContains(t, tableNames, "rel_logical_id")
	assert.NotContains(t, tableNames, "extra_info")
	assert.NotContains(t, tableNames, catalog.SystemRelAttr_CPKey)

	columnsSchema := builtinTableSchemaForLayout(legacy3CatalogLayout, catalog.MO_COLUMNS_ID)
	require.NotNil(t, columnsSchema)
	var columnNames []string
	for _, col := range columnsSchema.Columns {
		columnNames = append(columnNames, col.Name)
	}
	assert.NotContains(t, columnNames, "attr_has_generated")
	assert.NotContains(t, columnNames, "attr_generated")
	assert.NotContains(t, columnNames, catalog.SystemColAttr_CPKey)
}

func TestReadTableSchema_BuiltinFallbackForSystemTable(t *testing.T) {
	reader := &CheckpointReader{}
	schema := reader.ReadTableSchema(context.Background(), catalog.MO_TABLES_ID, types.TS{}, nil)

	require.NotNil(t, schema)
	assert.Equal(t, "mo_tables", schema.TableName)
	assert.Equal(t, "mo_catalog", schema.DatabaseName)
	require.NotEmpty(t, schema.Columns)

	var names []string
	for _, col := range schema.Columns {
		names = append(names, col.Name)
	}
	assert.Contains(t, names, "relname")
	assert.NotContains(t, names, "extra_info")
	assert.NotContains(t, names, catalog.SystemRelAttr_CPKey)
}

func TestWriteCSVMetadata(t *testing.T) {
	var buf bytes.Buffer
	err := writeCSVMetadata(&buf, &TableSchema{
		TableName:    "t1",
		DatabaseName: "db1",
		CreateSQL:    "CREATE TABLE t1 (id INT)",
	}, logicalTableStats{
		VisibleRows:  10,
		DeletedRows:  3,
		PhysicalRows: 13,
	})
	require.NoError(t, err)
	out := buf.String()
	assert.Contains(t, out, "-- CREATE TABLE t1 (id INT)")
	assert.Contains(t, out, "-- Database: db1")
	assert.Contains(t, out, "-- Table: t1")
	assert.Contains(t, out, "-- Visible rows: 10 (deleted: 3, physical: 13)")
}

func TestWriteSQLLoadCSVRow_NullAndEscaping(t *testing.T) {
	var buf bytes.Buffer
	types := []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_json.ToType()}
	err := writeSQLLoadCSVRow(&buf, types, []string{`a"b\c`, "42", `{"x":"y"}`}, []bool{false, true, false})
	require.NoError(t, err)
	assert.Equal(t, "\"a\"\"b\\\\c\",\\N,\"{\"\"x\"\":\"\"y\"\"}\"\n", buf.String())
}

func TestWriteSQLLoadCSVRow_RoundTripsThroughCSVParser(t *testing.T) {
	var buf bytes.Buffer
	colTypes := []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_json.ToType()}
	err := writeSQLLoadCSVRow(&buf, colTypes, []string{`a"b\c`, "42", `{"x":"y"}`}, []bool{false, true, false})
	require.NoError(t, err)

	parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
		FieldsTerminatedBy: ",",
		FieldsEnclosedBy:   `"`,
		FieldsEscapedBy:    `\`,
		LinesTerminatedBy:  "\n",
		Null:               []string{`\N`},
		UnescapedQuote:     true,
	}, strings.NewReader(buf.String()), csvparser.ReadBlockSize, false)
	require.NoError(t, err)

	row, err := parser.Read(nil)
	require.NoError(t, err)
	require.Len(t, row, 3)
	assert.Equal(t, `a"b\c`, row[0].Val)
	assert.False(t, row[0].IsNull)
	assert.True(t, row[0].HasStringQuote)
	assert.True(t, row[1].IsNull)
	assert.Equal(t, `{"x":"y"}`, row[2].Val)
	assert.False(t, row[2].IsNull)
	assert.True(t, row[2].HasStringQuote)
}

func TestWriteSQLLoadCSVFieldFromVec_BitUsesPackedBytes(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.New(types.T_bit, 1, 0))
	require.NoError(t, vector.AppendFixed(vec, uint64(1), false, mp))

	var buf bytes.Buffer
	require.NoError(t, writeSQLLoadCSVFieldFromVec(&buf, *vec.GetType(), []*vector.Vector{vec}, 0, 0))
	assert.Equal(t, "\"\x01\"", buf.String())
}

func TestWriteSQLLoadCSVFieldFromVec_JSONUsesVisibleText(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_json.ToType())
	bj, err := types.ParseStringToByteJson(`{"name":"mo","n":1}`)
	require.NoError(t, err)
	stored, err := types.EncodeJson(bj)
	require.NoError(t, err)
	require.NoError(t, vector.AppendBytes(vec, stored, false, mp))

	var buf bytes.Buffer
	require.NoError(t, writeSQLLoadCSVFieldFromVec(&buf, *vec.GetType(), []*vector.Vector{vec}, 0, 0))
	assert.Equal(t, `"{""n"": 1, ""name"": ""mo""}"`, buf.String())
}

func TestWriteSQLLoadCSVFieldFromVec_TemporalKeepsScale(t *testing.T) {
	mp := mpool.MustNewZero()
	timeVec := vector.NewVec(types.New(types.T_time, 0, 6))
	datetimeVec := vector.NewVec(types.New(types.T_datetime, 0, 3))
	timestampVec := vector.NewVec(types.New(types.T_timestamp, 0, 6))
	require.NoError(t, vector.AppendFixed(timeVec, types.TimeFromClock(false, 11, 22, 33, 123456), false, mp))
	require.NoError(t, vector.AppendFixed(datetimeVec, types.DatetimeFromClock(2024, 1, 2, 3, 4, 5, 123000), false, mp))
	require.NoError(t, vector.AppendFixed(timestampVec, types.FromClockZone(time.Local, 2024, 1, 2, 3, 4, 5, 123456), false, mp))

	var buf bytes.Buffer
	require.NoError(t, writeSQLLoadCSVFieldFromVec(&buf, *timeVec.GetType(), []*vector.Vector{timeVec}, 0, 0))
	assert.Equal(t, "11:22:33.123456", buf.String())
	buf.Reset()
	require.NoError(t, writeSQLLoadCSVFieldFromVec(&buf, *datetimeVec.GetType(), []*vector.Vector{datetimeVec}, 0, 0))
	assert.Equal(t, "2024-01-02 03:04:05.123", buf.String())
	buf.Reset()
	require.NoError(t, writeSQLLoadCSVFieldFromVec(&buf, *timestampVec.GetType(), []*vector.Vector{timestampVec}, 0, 0))
	assert.Equal(t, "2024-01-02 03:04:05.123456", buf.String())
}

func TestWriteSQLLoadCSVFieldFromVec_BinaryEscapesControlBytes(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_blob.ToType())
	require.NoError(t, vector.AppendBytes(vec, []byte{'a', '\n', 0, '\r', '\t', 0x1a, '"', '\\'}, false, mp))

	var buf bytes.Buffer
	require.NoError(t, writeSQLLoadCSVFieldFromVec(&buf, *vec.GetType(), []*vector.Vector{vec}, 0, 0))
	assert.Equal(t, `"a\n\0\r\t\Z""\\"`, buf.String())
}

func TestParseCSVRowOrder(t *testing.T) {
	order, err := ParseCSVRowOrder("storage")
	require.NoError(t, err)
	assert.Equal(t, CSVRowOrderStorage, order)

	order, err = ParseCSVRowOrder("lexical")
	require.NoError(t, err)
	assert.Equal(t, CSVRowOrderLexical, order)

	_, err = ParseCSVRowOrder("unknown")
	require.Error(t, err)
}
