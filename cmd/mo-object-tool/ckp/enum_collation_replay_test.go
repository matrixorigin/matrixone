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

package ckp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/stretchr/testify/require"
)

func TestRestoreScriptPreservesCheckpointEnumIdentity(t *testing.T) {
	ctx := context.Background()
	table := checkpointtool.TableCatalogEntry{TableID: 101, DatabaseName: "app", TableName: "colors", RelKind: "r"}
	tableRow := ckpMoTablesRow(t, "101", "colors", "app", "10", "r", "0")
	extra, err := (&api.SchemaExtra{DefaultCharset: types.EnumCollationGeneralCI}).Marshal()
	require.NoError(t, err)
	setCell := func(headers []string, row []string, name, value string) {
		t.Helper()
		for i, header := range headers {
			if header == name {
				row[i+3] = value
				return
			}
		}
		t.Fatalf("missing catalog column %s", name)
	}
	setCell(catalog.MoTablesSchema, tableRow, catalog.SystemRelAttr_ExtraInfo, string(extra))
	typ := types.T_enum.ToType()
	typ.SetEnumCollation(types.EnumCollationUTF8Bin)
	rawType, err := types.Encode(&typ)
	require.NoError(t, err)
	rawDefault, err := types.Encode(&plan.Default{NullAbility: true})
	require.NoError(t, err)
	colRow := make([]string, len(catalog.MoColumnsSchema)+3)
	colRow[0], colRow[1], colRow[2] = "obj", "0", "1"
	for name, value := range map[string]string{
		catalog.SystemColAttr_RelID: "101", catalog.SystemColAttr_RelName: "colors",
		catalog.SystemColAttr_Name: "choice", catalog.SystemColAttr_Type: string(rawType),
		catalog.SystemColAttr_Num: "1", catalog.SystemColAttr_NullAbility: "0",
		catalog.SystemColAttr_IsHidden: "0", catalog.SystemColAttr_Seqnum: "0",
		catalog.SystemColAttr_EnumValues: "A,a", catalog.SystemColAttr_HasExpr: "1",
		catalog.SystemColAttr_DefaultExpr: string(rawDefault),
	} {
		setCell(catalog.MoColumnsSchema, colRow, name, value)
	}
	reader := &checkpointtool.CheckpointReader{}
	reader.SetGetLogicalViewForTest(func(_ *checkpointtool.CheckpointReader, tableID uint64) (*checkpointtool.LogicalTableView, error) {
		switch tableID {
		case catalog.MO_TABLES_ID:
			return &checkpointtool.LogicalTableView{Headers: append([]string{"object", "block", "row"}, catalog.MoTablesSchema...), Rows: [][]string{tableRow}, NullsKnown: true, RowNulls: [][]uint64{make([]uint64, (len(catalog.MoTablesSchema)+63)/64)}}, nil
		case catalog.MO_COLUMNS_ID:
			return &checkpointtool.LogicalTableView{Headers: append([]string{"object", "block", "row"}, catalog.MoColumnsSchema...), Rows: [][]string{colRow}, NullsKnown: true, RowNulls: [][]uint64{make([]uint64, (len(catalog.MoColumnsSchema)+63)/64)}}, nil
		default:
			return nil, fmt.Errorf("no catalog fixture for table %d", tableID)
		}
	})
	schema := reader.ReadTableSchema(ctx, 101, types.BuildTS(1, 0), nil)
	require.Len(t, schema.Columns, 1)
	data := map[uint64]*checkpointtool.TableDumpData{101: {TableID: 101, Schema: schema, IndexesPrepared: true}}
	for _, includeLoad := range []bool{false, true} {
		output := t.TempDir()
		path, writeErr := writeRestoreScript(ctx, reader, &dumpOutput{}, []checkpointtool.TableCatalogEntry{table}, data, types.BuildTS(1, 0), output, output, loadDataPathResolver{}, includeLoad, false)
		require.NoError(t, writeErr)
		script, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.Contains(t, string(script), "ENUM('A','a') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
		require.Contains(t, string(script), " COLLATE=utf8mb4_general_ci")
		require.Contains(t, string(script), "SET @__mo_ckp_saved_sql_mode = @@SESSION.sql_mode;")
		require.Contains(t, string(script), "',NO_BACKSLASH_ESCAPES,', ','")
		require.Contains(t, string(script), "SET SESSION sql_mode = @__mo_ckp_saved_sql_mode;")
		if includeLoad {
			require.Contains(t, string(script), "LOAD DATA")
		}
	}
}

func TestRestoreScriptRejectsUnreplayableEnumBeforePublish(t *testing.T) {
	ctx := context.Background()
	output := t.TempDir()
	table := checkpointtool.TableCatalogEntry{TableID: 101, DatabaseName: "app", TableName: "colors", RelKind: "r"}
	data := map[uint64]*checkpointtool.TableDumpData{101: {TableID: 101, IndexesPrepared: true, Schema: &checkpointtool.TableSchema{
		TableName: "colors", DefaultCharset: types.EnumCollationUTF8Bin,
		Columns: []checkpointtool.TableColumn{{Name: "choice", SQLType: "ENUM", EnumValues: "A,a"}},
	}}}
	_, err := writeRestoreScript(ctx, nil, &dumpOutput{}, []checkpointtool.TableCatalogEntry{table}, data, types.BuildTS(1, 0), output, output, loadDataPathResolver{}, true, false)
	require.ErrorContains(t, err, "cannot replay legacy ENUM")
	_, err = os.Stat(filepath.Join(output, "restore.sql"))
	require.True(t, os.IsNotExist(err))
	_, err = renderExternalCreateTableDDLFromParamJSON(table, &checkpointtool.TableSchema{DefaultCharset: types.EnumCollationUTF8Bin})
	require.ErrorContains(t, err, "cannot replay a table ENUM collation default")
}

func TestRestoreScriptRejectsAmbiguousEnumDataButAllowsDDL(t *testing.T) {
	ctx := context.Background()
	table := checkpointtool.TableCatalogEntry{TableID: 101, DatabaseName: "app", TableName: "colors", RelKind: "r"}
	schema := &checkpointtool.TableSchema{TableName: "colors", Columns: []checkpointtool.TableColumn{{
		Name: "choice", SQLType: "ENUM", EnumValues: "2,x", EnumCollation: types.EnumCollationUTF8Bin, EnumValuesAreRaw: true,
	}}}
	data := map[uint64]*checkpointtool.TableDumpData{101: {TableID: 101, Schema: schema, IndexesPrepared: true}}
	for _, includeLoad := range []bool{false, true} {
		output := t.TempDir()
		path, err := writeRestoreScript(ctx, nil, &dumpOutput{}, []checkpointtool.TableCatalogEntry{table}, data, types.BuildTS(1, 0), output, output, loadDataPathResolver{}, includeLoad, false)
		if includeLoad {
			require.ErrorContains(t, err, "decimal ordinal 2 matches member 1")
			_, statErr := os.Stat(filepath.Join(output, "restore.sql"))
			require.True(t, os.IsNotExist(statErr))
		} else {
			require.NoError(t, err)
			script, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			require.Contains(t, string(script), "ENUM('2','x') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
			require.NotContains(t, string(script), "LOAD DATA")
		}
	}
}
