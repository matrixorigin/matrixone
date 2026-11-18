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
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/stretchr/testify/require"
)

func TestRestorePreflightChecksEntireSelectedSet(t *testing.T) {
	good := checkpointtool.TableCatalogEntry{TableID: 1, DatabaseName: "app", TableName: "good", RelKind: "r"}
	bad := checkpointtool.TableCatalogEntry{TableID: 2, DatabaseName: "app", TableName: "bad", RelKind: "r"}
	goodSchema := &checkpointtool.TableSchema{TableName: "good", Columns: []checkpointtool.TableColumn{{Name: "id", SQLType: "INT"}}}
	badSchema := &checkpointtool.TableSchema{TableName: "bad", Columns: []checkpointtool.TableColumn{{Name: "e", SQLType: "ENUM", EnumValues: "2,x", EnumCollation: types.EnumCollationUTF8Bin, EnumValuesAreRaw: true}}}
	data := map[uint64]*checkpointtool.TableDumpData{1: {Schema: goodSchema, IndexesPrepared: true}, 2: {Schema: badSchema, IndexesPrepared: true}}
	for _, tables := range [][]checkpointtool.TableCatalogEntry{{bad}, {good, bad}, {bad, good}} {
		err := preflightRestoreTables(context.Background(), nil, tables, data, types.TS{}, true)
		require.ErrorContains(t, err, "decimal ordinal 2 matches member 1")
		require.NoError(t, preflightRestoreTables(context.Background(), nil, tables, data, types.TS{}, false), "DDL-only mode has no numeric LOAD ambiguity")
	}
}

func TestRestorePreflightChecksSchemaExternalAndView(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{TableID: 1, DatabaseName: "app", TableName: "t", RelKind: "r"}
	data := map[uint64]*checkpointtool.TableDumpData{1: {Schema: &checkpointtool.TableSchema{TableName: "t", DefaultCharset: types.EnumCollationUTF8Bin, Columns: []checkpointtool.TableColumn{{Name: "e", SQLType: "ENUM", EnumValues: "A,a"}}}, IndexesPrepared: true}}
	for _, load := range []bool{false, true} {
		require.ErrorContains(t, preflightRestoreTables(context.Background(), nil, []checkpointtool.TableCatalogEntry{table}, data, types.TS{}, load), "cannot replay legacy ENUM")
	}
	table.RelKind = "external"
	data[1].Schema.Columns = []checkpointtool.TableColumn{{Name: "id", SQLType: "INT"}}
	data[1].Schema.CreateSQL = `{}`
	require.ErrorContains(t, preflightRestoreTables(context.Background(), nil, []checkpointtool.TableCatalogEntry{table}, data, types.TS{}, true), "cannot replay a table ENUM collation default")
	table.RelKind = "v"
	data[1].Schema = &checkpointtool.TableSchema{}
	require.ErrorContains(t, preflightRestoreTables(context.Background(), nil, []checkpointtool.TableCatalogEntry{table}, data, types.TS{}, true), "missing verified view definition")
	data[1] = checkpointViewTestData(t, "CREATE VIEW t AS SELECT 1", "", "app", "DEFINER")
	require.NoError(t, preflightRestoreTables(context.Background(), nil, []checkpointtool.TableCatalogEntry{table}, data, types.TS{}, true))
}

func TestRestorePreflightUsesAccountRestoreFilter(t *testing.T) {
	user := checkpointtool.TableCatalogEntry{TableID: 1, DatabaseName: "app", TableName: "t", RelKind: "r"}
	system := checkpointtool.TableCatalogEntry{TableID: 2, DatabaseName: "mo_catalog", TableName: "t", RelKind: "r"}
	data := map[uint64]*checkpointtool.TableDumpData{
		1: {Schema: &checkpointtool.TableSchema{TableName: "t", Columns: []checkpointtool.TableColumn{{Name: "id", SQLType: "INT"}}}},
		2: {Schema: &checkpointtool.TableSchema{TableName: "t", Columns: []checkpointtool.TableColumn{{Name: "e", SQLType: "ENUM", EnumValues: "2,x", EnumCollation: types.EnumCollationUTF8Bin, EnumValuesAreRaw: true}}}},
	}
	selected, skipped := filterAccountRestoreScriptTables([]checkpointtool.TableCatalogEntry{user, system}, true)
	require.Equal(t, 1, skipped)
	require.NoError(t, preflightRestoreTables(context.Background(), nil, selected, data, types.TS{}, true))
	selected, skipped = filterAccountRestoreScriptTables([]checkpointtool.TableCatalogEntry{user, system}, false)
	require.Zero(t, skipped)
	require.ErrorContains(t, preflightRestoreTables(context.Background(), nil, selected, data, types.TS{}, true), "decimal ordinal 2 matches member 1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, preflightRestoreTables(ctx, nil, []checkpointtool.TableCatalogEntry{user}, data, types.TS{}, true), context.Canceled)
}

func TestRestoreScriptCanceledBeforeOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dir := filepath.Join(t.TempDir(), "not-created")
	_, err := writeRestoreScript(ctx, nil, &dumpOutput{}, nil, nil, types.TS{}, dir, dir, loadDataPathResolver{}, false, false)
	require.ErrorIs(t, err, context.Canceled)
	_, err = os.Stat(dir)
	require.True(t, os.IsNotExist(err))
}
