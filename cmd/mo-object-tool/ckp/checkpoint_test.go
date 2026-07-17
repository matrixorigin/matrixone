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

package ckp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/tools/toolfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreferRealErrorKeepsNonCanceledRootCause(t *testing.T) {
	root := errors.New("s3 write failed")

	assert.ErrorIs(t, preferRealError(context.Canceled, root), root)
	assert.ErrorIs(t, preferRealError(root, context.Canceled), root)
	assert.ErrorIs(t, preferRealError(context.Canceled, context.Canceled), context.Canceled)
	assert.NoError(t, preferRealError(nil, nil))
}

func TestDumpTableErrorDedupesPipeWriteAndCloseError(t *testing.T) {
	err := errors.New("s3 write failed")

	assert.Equal(t, err, dumpTableError(err, errors.New("s3 write failed")))
}

func TestIsContextCanceledOnly(t *testing.T) {
	root := errors.New("s3 write failed")

	assert.True(t, isContextCanceledOnly(context.Canceled))
	assert.True(t, isContextCanceledOnly(fmt.Errorf("dump table 1: %w", context.Canceled)))
	assert.True(t, isContextCanceledOnly(errors.Join(context.Canceled, fmt.Errorf("worker: %w", context.Canceled))))
	assert.False(t, isContextCanceledOnly(root))
	assert.False(t, isContextCanceledOnly(context.DeadlineExceeded))
	assert.False(t, isContextCanceledOnly(errors.Join(context.Canceled, root)))
}

func TestNormalizeCreateTableDDLName(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		DatabaseName: "compat_ckp",
		TableName:    "employees",
	}

	tests := []struct {
		name string
		ddl  string
		want string
	}{
		{
			name: "plain table name",
			ddl:  "CREATE TABLE employees_copy_123 (id INT)",
			want: "CREATE TABLE `compat_ckp`.`employees` (id INT)",
		},
		{
			name: "qualified table name",
			ddl:  "CREATE TABLE `compat_ckp`.`employees_copy_123` (id INT)",
			want: "CREATE TABLE `compat_ckp`.`employees` (id INT)",
		},
		{
			name: "if not exists",
			ddl:  "CREATE TABLE IF NOT EXISTS old_name (id INT)",
			want: "CREATE TABLE IF NOT EXISTS `compat_ckp`.`employees` (id INT)",
		},
		{
			name: "external table",
			ddl:  "CREATE EXTERNAL TABLE old_name (id INT) INFILE {'filepath'='/tmp/data.csv','format'='csv'}",
			want: "CREATE EXTERNAL TABLE `compat_ckp`.`employees` (id INT) INFILE {'filepath'='/tmp/data.csv','format'='csv'}",
		},
		{
			name: "view",
			ddl:  "CREATE VIEW old_view AS SELECT id FROM src",
			want: "CREATE VIEW `compat_ckp`.`employees` AS SELECT id FROM src",
		},
		{
			name: "or replace view",
			ddl:  "CREATE OR REPLACE VIEW `old_db`.`old_view` AS SELECT id FROM src",
			want: "CREATE OR REPLACE VIEW `compat_ckp`.`employees` AS SELECT id FROM src",
		},
		{
			name: "escaped target name",
			ddl:  "CREATE TABLE old_name (id INT)",
			want: "CREATE TABLE `compat``ckp`.`employees` (id INT)",
		},
	}

	tests[len(tests)-1].want = "CREATE TABLE `compat``ckp`.`employees` (id INT)"
	tests[len(tests)-1].ddl = "CREATE TABLE old_name (id INT)"
	tests[len(tests)-1].name = "escaped database name"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "escaped database name" {
				table.DatabaseName = "compat`ckp"
			} else {
				table.DatabaseName = "compat_ckp"
			}
			assert.Equal(t, tt.want, normalizeCreateTableDDLName(tt.ddl, table))
		})
	}
}

func TestRestoreCreateTableDDLUsesExternalCreateSQL(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		DatabaseName: "ckp_external",
		TableName:    "ext_csv_local",
		RelKind:      "e",
	}
	paramJSON, err := json.Marshal(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Option: []string{"filepath", "/tmp/ext.csv", "format", "csv", "compression", "none"},
			Tail: &tree.TailParameter{
				Fields: &tree.Fields{
					Terminated: &tree.Terminated{Value: ","},
					EnclosedBy: &tree.EnclosedBy{Value: '"'},
				},
				Lines: &tree.Lines{
					TerminatedBy: &tree.Terminated{Value: "\n"},
				},
				IgnoredLines: 1,
			},
		},
	})
	require.NoError(t, err)
	dumpData := &checkpointtool.TableDumpData{
		Schema: &checkpointtool.TableSchema{
			TableName: "stale_name",
			CreateSQL: string(paramJSON),
			Columns:   []checkpointtool.TableColumn{{Name: "id", SQLType: "INT", Position: 1}},
		},
	}

	ddl, err := restoreCreateTableDDL(context.Background(), nil, table, dumpData, types.TS{})
	require.NoError(t, err)
	assert.Equal(t, "CREATE EXTERNAL TABLE `ckp_external`.`ext_csv_local` (\n  `id` INT\n) INFILE {'filepath'='/tmp/ext.csv', 'compression'='none', 'format'='csv'} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\\\n' IGNORE 1 LINES", ddl)
}

func TestRestoreCreateTableDDLExternalRequiresCreateSQL(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		DatabaseName: "ckp_external",
		TableName:    "ext_csv_local",
		RelKind:      "e",
	}
	dumpData := &checkpointtool.TableDumpData{
		Schema: &checkpointtool.TableSchema{
			TableName: "ext_csv_local",
			Columns:   []checkpointtool.TableColumn{{Name: "id", SQLType: "INT", Position: 1}},
		},
	}

	_, err := restoreCreateTableDDL(context.Background(), nil, table, dumpData, types.TS{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing external table parameter JSON")
}

func TestRenderExternalInfileClauseWithOptionalSettings(t *testing.T) {
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath:         "/tmp/data.jsonl",
			CompressType:     "none",
			Format:           "jsonline",
			HivePartitioning: true,
			HivePartitionCols: []string{
				"ds",
				"region",
			},
			Tail: &tree.TailParameter{
				Fields: &tree.Fields{
					EscapedBy: &tree.EscapedBy{Value: '\\'},
				},
				Lines: &tree.Lines{
					StartingBy: "\t",
				},
			},
		},
		ExParam: tree.ExParam{JsonData: "object"},
	}

	got := renderExternalInfileClause(param)
	require.Contains(t, got, "'jsondata'='object'")
	require.Contains(t, got, "'hive_partitioning'='true'")
	require.Contains(t, got, "'hive_partition_columns'='ds,region'")
	require.Contains(t, got, "ESCAPED BY '\\\\'")
	require.Contains(t, got, "STARTING BY '\\\\t'")
	require.Equal(t, []string{"a", "b", "c"}, appendExternalOptionIfSet([]string{"a"}, "b", "c"))
	require.Equal(t, []string{"a"}, appendExternalOptionIfSet([]string{"a"}, "b", ""))
	require.Equal(t, "", byteSQLString(0))
	require.Equal(t, ",", byteSQLString(','))
}

func TestRestoreCreateTableDDLUsesViewCreateSQL(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		DatabaseName: "ckp_views",
		TableName:    "v_normal",
		RelKind:      "v",
	}
	dumpData := &checkpointtool.TableDumpData{
		Schema: &checkpointtool.TableSchema{
			TableName: "stale_name",
			CreateSQL: "CREATE VIEW `old_db`.`old_view` AS SELECT id, name FROM `ckp_tables`.`t_normal`",
			Columns:   []checkpointtool.TableColumn{{Name: "id", SQLType: "INT", Position: 1}},
		},
	}

	ddl, err := restoreCreateTableDDL(context.Background(), nil, table, dumpData, types.TS{})
	require.NoError(t, err)
	assert.Equal(t, "CREATE VIEW `ckp_views`.`v_normal` AS SELECT id, name FROM `ckp_tables`.`t_normal`", ddl)
}

func TestRestoreCreateTableDDLViewRequiresCreateViewSQL(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		DatabaseName: "ckp_views",
		TableName:    "v_normal",
		RelKind:      "v",
	}
	dumpData := &checkpointtool.TableDumpData{
		Schema: &checkpointtool.TableSchema{
			TableName: "v_normal",
			CreateSQL: "CREATE TABLE `v_normal` (`id` INT)",
			Columns:   []checkpointtool.TableColumn{{Name: "id", SQLType: "INT", Position: 1}},
		},
	}

	_, err := restoreCreateTableDDL(context.Background(), nil, table, dumpData, types.TS{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rel_createsql is not CREATE VIEW")
}

func TestResolveTableByIDAndRestoreFallback(t *testing.T) {
	ctx := context.Background()
	reader := &checkpointtool.CheckpointReader{}
	reader.SetGetLogicalViewForTest(func(_ *checkpointtool.CheckpointReader, tableID uint64) (*checkpointtool.LogicalTableView, error) {
		if tableID != 2 {
			return nil, fmt.Errorf("unexpected table %d", tableID)
		}
		return &checkpointtool.LogicalTableView{
			Headers: append([]string{"object", "block", "row"}, catalog.MoTablesSchema...),
			Rows: [][]string{
				ckpMoTablesRow(t, "2", catalog.MO_TABLES, catalog.MO_CATALOG, "1", "r", "0"),
				ckpMoTablesRow(t, "42", "target", "db", "7", "r", "1"),
			},
		}, nil
	})

	table, err := resolveTableByID(ctx, reader, types.BuildTS(1, 0), 42)
	require.NoError(t, err)
	require.Equal(t, "target", table.TableName)
	_, err = resolveTableByID(ctx, reader, types.BuildTS(1, 0), 999)
	require.ErrorContains(t, err, "table 999 not found")

	ddl, err := restoreCreateTableDDL(ctx, reader, checkpointtool.TableCatalogEntry{
		TableID:      2,
		DatabaseName: catalog.MO_CATALOG,
		TableName:    catalog.MO_TABLES,
		RelKind:      "r",
	}, nil, types.BuildTS(1, 0))
	require.NoError(t, err)
	require.Contains(t, ddl, "CREATE TABLE `mo_catalog`.`mo_tables`")
}

func TestShouldWriteLoadDataSkipsExternalAndViewRelations(t *testing.T) {
	assert.False(t, shouldWriteLoadData(true, checkpointtool.TableCatalogEntry{RelKind: "e"}))
	assert.False(t, shouldWriteLoadData(true, checkpointtool.TableCatalogEntry{RelKind: "external"}))
	assert.False(t, shouldWriteLoadData(true, checkpointtool.TableCatalogEntry{RelKind: "v"}))
	assert.False(t, shouldWriteLoadData(true, checkpointtool.TableCatalogEntry{RelKind: "view"}))
	assert.True(t, shouldWriteLoadData(true, checkpointtool.TableCatalogEntry{RelKind: "r"}))
	assert.False(t, shouldWriteLoadData(false, checkpointtool.TableCatalogEntry{RelKind: "r"}))
}

func TestFilterAccountRestoreScriptTablesSkipsSystemDatabases(t *testing.T) {
	tables := []checkpointtool.TableCatalogEntry{
		{DatabaseName: "mo_catalog", TableName: "mo_user"},
		{DatabaseName: "mysql", TableName: "user"},
		{DatabaseName: "system", TableName: "statement_info"},
		{DatabaseName: "system_metrics", TableName: "metric"},
		{DatabaseName: "coverage", TableName: "t1"},
		{DatabaseName: "Coverage_Extra", TableName: "t2"},
	}

	filtered, skipped := filterAccountRestoreScriptTables(tables, true)

	require.Len(t, filtered, 2)
	assert.Equal(t, 4, skipped)
	assert.Equal(t, "coverage", filtered[0].DatabaseName)
	assert.Equal(t, "t1", filtered[0].TableName)
	assert.Equal(t, "Coverage_Extra", filtered[1].DatabaseName)
	assert.Equal(t, "t2", filtered[1].TableName)
}

func TestFilterAccountRestoreScriptTablesLeavesNonAccountDumpUnchanged(t *testing.T) {
	tables := []checkpointtool.TableCatalogEntry{
		{DatabaseName: "mo_catalog", TableName: "mo_user"},
		{DatabaseName: "coverage", TableName: "t1"},
	}

	filtered, skipped := filterAccountRestoreScriptTables(tables, false)

	assert.Equal(t, tables, filtered)
	assert.Zero(t, skipped)
}

func TestCommandValidationBranches(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "cpu profile create error",
			args: []string{"--cpuprofile", t.TempDir()},
			want: "create cpuprofile",
		},
		{
			name: "missing selector",
			args: nil,
			want: "--table-id, or at least one of --database-id/--account-id is required",
		},
		{
			name: "no load without load script",
			args: []string{"--table-id=1", "--no-load"},
			want: "--no-load requires --load-script",
		},
		{
			name: "table id conflicts with output dir",
			args: []string{"--table-id=1", "--output-dir=out"},
			want: "--table-id cannot be combined",
		},
		{
			name: "batch output needs output dir",
			args: []string{"--database-id=1", "-o", "out.csv"},
			want: "--output cannot be used",
		},
		{
			name: "load script needs output",
			args: []string{"--database-id=1", "--load-script"},
			want: "--output/-o directory is required",
		},
		{
			name: "batch needs output dir",
			args: []string{"--database-id=1"},
			want: "--output-dir is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := dumpCommand(&toolfs.StorageOptions{})
			cmd.SetArgs(tc.args)
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			err := cmd.Execute()
			require.ErrorContains(t, err, tc.want)
		})
	}

	showCmd := showCreateTableCommand(&toolfs.StorageOptions{})
	showCmd.SetArgs(nil)
	showCmd.SetOut(&bytes.Buffer{})
	showCmd.SetErr(&bytes.Buffer{})
	require.ErrorContains(t, showCmd.Execute(), "--table-id is required")

	showCmd = showCreateTableCommand(&toolfs.StorageOptions{})
	showCmd.SetArgs([]string{"--table-id=1", "--local", "--local2", t.TempDir()})
	showCmd.SetOut(&bytes.Buffer{})
	showCmd.SetErr(&bytes.Buffer{})
	require.Error(t, showCmd.Execute())
}

func TestDumpCommandRunsUntilResolveSnapshotValidation(t *testing.T) {
	cmd := dumpCommand(&toolfs.StorageOptions{})
	cmd.SetArgs([]string{"--table-id=1", "--row-order=bad", t.TempDir()})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	err := cmd.Execute()
	require.ErrorContains(t, err, "resolve --ts")
}

func TestListAndShowCreateCommandsReachSnapshotValidation(t *testing.T) {
	dir := t.TempDir()

	listCmd := listCommand(&toolfs.StorageOptions{})
	listCmd.SetArgs([]string{"--type=databases", "--account-id=1", dir})
	listCmd.SetOut(&bytes.Buffer{})
	listCmd.SetErr(&bytes.Buffer{})
	require.ErrorContains(t, listCmd.Execute(), "resolve --ts")

	showCmd := showCreateTableCommand(&toolfs.StorageOptions{})
	showCmd.SetArgs([]string{"--table-id=1", dir})
	showCmd.SetOut(&bytes.Buffer{})
	showCmd.SetErr(&bytes.Buffer{})
	require.ErrorContains(t, showCmd.Execute(), "resolve --ts")
}

func TestDumpOutputLocalCreateMkdirAndClose(t *testing.T) {
	ctx := context.Background()
	out, err := openDumpOutput(ctx, toolfs.StorageOptions{})
	require.NoError(t, err)
	require.False(t, out.remote)
	require.NoError(t, out.MkdirAll(filepath.Join(t.TempDir(), "nested")))
	out.Close(ctx)

	filePath := filepath.Join(t.TempDir(), "dump.csv")
	w, err := out.Create(ctx, filePath)
	require.NoError(t, err)
	_, err = w.Write([]byte("a,b\n"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, "a,b\n", string(data))

	var nilOut *dumpOutput
	dir := filepath.Join(t.TempDir(), "nil-output")
	require.NoError(t, nilOut.MkdirAll(dir))
	require.DirExists(t, dir)
}

func TestOpenReaderResolveSnapshotAndFileServiceWriteCloser(t *testing.T) {
	ctx := context.Background()

	reader, err := openReader(ctx, t.TempDir(), toolfs.StorageOptions{}, objectio.OfflineKindLocal)
	require.NoError(t, err)
	require.Equal(t, objectio.OfflineKindLocal, reader.Kind())
	require.NoError(t, reader.Close())

	_, err = resolveSnapshotTS(ctx, reader, "")
	require.ErrorContains(t, err, "no checkpoint timestamp")
	_, err = resolveSnapshotTS(ctx, reader, "bad-ts")
	require.ErrorContains(t, err, "timestamp must be")
	_, err = resolveSnapshotTS(ctx, reader, "1:0")
	require.ErrorContains(t, err, "not usable")

	fs, err := fileservice.NewLocalFS(ctx, "local", t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	w, err := newFileServiceWriteCloser(ctx, fs, "/remote/out.csv")
	require.NoError(t, err)
	n, err := w.Write([]byte("a,b\n"))
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close())

	vec := &fileservice.IOVector{
		FilePath: "remote/out.csv",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   4,
			ToCacheData: func(ctx context.Context, _ io.Reader, data []byte, allocator fileservice.CacheDataAllocator) (fscache.Data, error) {
				return allocator.CopyToCacheData(ctx, data), nil
			},
		}},
	}
	require.NoError(t, fs.Read(ctx, vec))
	require.Equal(t, []byte("a,b\n"), vec.Entries[0].CachedData.Bytes())
	vec.Release()
	(&dumpOutput{fs: fs}).Close(ctx)

	_, err = openReader(ctx, ".", toolfs.StorageOptions{Backend: "GCS"}, objectio.OfflineKindLocal)
	require.ErrorContains(t, err, "unsupported backend")
	_, err = openReader(ctx, ".", toolfs.StorageOptions{Backend: "S3"}, objectio.OfflineKindLocal)
	require.ErrorContains(t, err, "missing --s3 arguments")
	_, err = openReader(ctx, ".", toolfs.StorageOptions{FSConfig: filepath.Join(t.TempDir(), "missing.toml")}, objectio.OfflineKindLocal)
	require.Error(t, err)

	_, err = openDumpOutput(ctx, toolfs.StorageOptions{Backend: "GCS"})
	require.ErrorContains(t, err, "unsupported backend")

	_, err = openDumpOutput(ctx, toolfs.StorageOptions{Backend: "S3"})
	require.ErrorContains(t, err, "missing --s3 arguments")
	_, err = openDumpOutput(ctx, toolfs.StorageOptions{FSConfig: filepath.Join(t.TempDir(), "missing.toml")})
	require.Error(t, err)
}

func TestRunViewerReportsOpenReaderError(t *testing.T) {
	err := runViewer(".", toolfs.StorageOptions{Backend: "GCS"}, objectio.OfflineKindLocal)
	require.ErrorContains(t, err, "open checkpoint dir")
}

func TestSetupLogFileReportsMkdirError(t *testing.T) {
	t.Setenv("HOME", filepath.Join(t.TempDir(), "not-a-dir"))
	require.NoError(t, os.WriteFile(os.Getenv("HOME"), []byte("x"), 0o644))

	_, err := setupLogFile()
	require.Error(t, err)
}

func TestDumpDataByTableIDAndOrderTablesForRestore(t *testing.T) {
	parent := checkpointtool.TableCatalogEntry{DatabaseName: "db", TableName: "parent", TableID: 1}
	child := checkpointtool.TableCatalogEntry{DatabaseName: "db", TableName: "child", TableID: 2}
	other := checkpointtool.TableCatalogEntry{DatabaseName: "db", TableName: "other", TableID: 3}
	base := checkpointtool.TableCatalogEntry{DatabaseName: "db", TableName: "z_table", TableID: 4, RelKind: "r"}
	view := checkpointtool.TableCatalogEntry{DatabaseName: "db", TableName: "a_view", TableID: 5, RelKind: "v"}
	view2 := checkpointtool.TableCatalogEntry{DatabaseName: "db", TableName: "b_view", TableID: 6, RelKind: "v"}
	plans := []tableDumpPlan{
		{table: child, data: &checkpointtool.TableDumpData{
			TableID: child.TableID,
			Schema: &checkpointtool.TableSchema{ForeignKeys: []checkpointtool.TableForeignKey{{
				ReferDatabase: parent.DatabaseName,
				ReferTable:    parent.TableName,
			}}},
		}},
		{table: parent, data: &checkpointtool.TableDumpData{TableID: parent.TableID, Schema: &checkpointtool.TableSchema{}}},
		{table: other, data: &checkpointtool.TableDumpData{TableID: other.TableID, Schema: &checkpointtool.TableSchema{}}},
		{table: base, data: &checkpointtool.TableDumpData{TableID: base.TableID, Schema: &checkpointtool.TableSchema{}}},
		{table: view, data: &checkpointtool.TableDumpData{TableID: view.TableID, Schema: &checkpointtool.TableSchema{
			CreateSQL: "CREATE VIEW `db`.`a_view` AS SELECT * FROM `db`.`z_table`",
		}}},
		{table: view2, data: &checkpointtool.TableDumpData{TableID: view2.TableID, Schema: &checkpointtool.TableSchema{
			CreateSQL: "CREATE VIEW `db`.`b_view` AS SELECT * FROM `db`.`a_view`",
		}}},
	}

	byID := dumpDataByTableID(plans)
	require.Len(t, byID, 6)
	require.Equal(t, uint64(2), byID[2].TableID)
	require.Nil(t, dumpDataByTableID(nil))

	ordered := orderTablesForRestore([]checkpointtool.TableCatalogEntry{child, other, parent}, byID)
	require.Equal(t, []uint64{1, 2, 3}, []uint64{ordered[0].TableID, ordered[1].TableID, ordered[2].TableID})
	require.Equal(t, []checkpointtool.TableCatalogEntry{parent}, orderTablesForRestore([]checkpointtool.TableCatalogEntry{parent}, byID))

	byID[1].Schema.ForeignKeys = []checkpointtool.TableForeignKey{{ReferDatabase: child.DatabaseName, ReferTable: child.TableName}}
	ordered = orderTablesForRestore([]checkpointtool.TableCatalogEntry{child, parent}, byID)
	require.Len(t, ordered, 2)
	restoreOrder := buildRestoreTableOrder([]checkpointtool.TableCatalogEntry{child, parent}, byID)
	require.True(t, restoreOrder.cyclicFKTables[parent.TableID])
	require.True(t, restoreOrder.cyclicFKTables[child.TableID])

	ordered = orderTablesForRestore([]checkpointtool.TableCatalogEntry{view, view2, base}, byID)
	require.Equal(t, []uint64{4, 5, 6}, []uint64{ordered[0].TableID, ordered[1].TableID, ordered[2].TableID})
}

func TestWriteRestoreScriptDefersCyclicForeignKeys(t *testing.T) {
	ctx := context.Background()
	dumpOut := &dumpOutput{}
	scriptDir := filepath.Join(t.TempDir(), "scripts")
	parent := checkpointtool.TableCatalogEntry{DatabaseName: "db", TableName: "parent", TableID: 1}
	child := checkpointtool.TableCatalogEntry{DatabaseName: "db", TableName: "child", TableID: 2}
	columns := []checkpointtool.TableColumn{
		{Name: "id", SQLType: "BIGINT", Position: 1},
		{Name: "other_id", SQLType: "BIGINT", Position: 2},
	}
	dumpDataByTable := map[uint64]*checkpointtool.TableDumpData{
		parent.TableID: {
			TableID: parent.TableID,
			Schema: &checkpointtool.TableSchema{
				TableName: "parent",
				Columns:   columns,
				ForeignKeys: []checkpointtool.TableForeignKey{{
					Name:          "fk_parent_child",
					Columns:       []string{"other_id"},
					ReferDatabase: "db",
					ReferTable:    "child",
					ReferColumns:  []string{"id"},
				}},
			},
		},
		child.TableID: {
			TableID: child.TableID,
			Schema: &checkpointtool.TableSchema{
				TableName: "child",
				Columns:   columns,
				ForeignKeys: []checkpointtool.TableForeignKey{{
					Name:          "fk_child_parent",
					Columns:       []string{"other_id"},
					ReferDatabase: "db",
					ReferTable:    "parent",
					ReferColumns:  []string{"id"},
				}},
			},
		},
	}
	reader := &checkpointtool.CheckpointReader{}
	reader.SetGetLogicalViewForTest(func(_ *checkpointtool.CheckpointReader, _ uint64) (*checkpointtool.LogicalTableView, error) {
		return &checkpointtool.LogicalTableView{}, nil
	})

	path, err := writeRestoreScript(
		ctx,
		reader,
		dumpOut,
		[]checkpointtool.TableCatalogEntry{parent, child},
		dumpDataByTable,
		types.BuildTS(1, 0),
		scriptDir,
		"csv",
		loadDataPathResolver{},
		true,
		true,
	)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	sql := string(data)
	parentCreate := strings.Index(sql, "CREATE TABLE `db`.`parent`")
	childCreate := strings.Index(sql, "CREATE TABLE `db`.`child`")
	parentLoad := strings.Index(sql, "INTO TABLE `parent`")
	childLoad := strings.Index(sql, "INTO TABLE `child`")
	parentAlter := strings.Index(sql, "ALTER TABLE `parent` ADD CONSTRAINT `fk_parent_child`")
	childAlter := strings.Index(sql, "ALTER TABLE `child` ADD CONSTRAINT `fk_child_parent`")
	require.NotEqual(t, -1, parentCreate)
	require.NotEqual(t, -1, childCreate)
	require.NotEqual(t, -1, parentLoad)
	require.NotEqual(t, -1, childLoad)
	require.NotEqual(t, -1, parentAlter)
	require.NotEqual(t, -1, childAlter)
	require.Greater(t, parentAlter, parentLoad)
	require.Greater(t, parentAlter, childLoad)
	require.Greater(t, childAlter, parentLoad)
	require.Greater(t, childAlter, childLoad)
	require.NotContains(t, sql[parentCreate:parentAlter], "CONSTRAINT `fk_parent_child`")
	require.NotContains(t, sql[childCreate:childAlter], "CONSTRAINT `fk_child_parent`")
}

func TestWriteRestoreScriptForViewAndErrors(t *testing.T) {
	ctx := context.Background()
	reader := &checkpointtool.CheckpointReader{}
	dumpOut := &dumpOutput{}
	scriptDir := filepath.Join(t.TempDir(), "scripts")
	table := checkpointtool.TableCatalogEntry{
		AccountID:    1,
		DatabaseName: "db",
		TableName:    "v1",
		TableID:      10,
		RelKind:      "v",
	}
	dataByTable := map[uint64]*checkpointtool.TableDumpData{
		10: {
			TableID: 10,
			Schema: &checkpointtool.TableSchema{
				TableName: "v1",
				CreateSQL: "CREATE VIEW old_v AS SELECT 1 AS c",
			},
		},
	}
	resolver := loadDataPathResolver{}

	path, err := writeRestoreScript(
		ctx,
		reader,
		dumpOut,
		[]checkpointtool.TableCatalogEntry{table},
		dataByTable,
		types.BuildTS(1, 0),
		scriptDir,
		"csv",
		resolver,
		true,
		true,
	)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(scriptDir, "restore.sql"), path)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	sql := string(data)
	require.Contains(t, sql, "CREATE DATABASE IF NOT EXISTS `db`;")
	require.Contains(t, sql, "USE `db`;")
	require.Contains(t, sql, "CREATE VIEW `db`.`v1` AS SELECT 1 AS c")
	require.NotContains(t, sql, "LOAD DATA")

	_, err = writeRestoreScript(
		ctx,
		reader,
		dumpOut,
		[]checkpointtool.TableCatalogEntry{{TableID: 11, TableName: "bad"}},
		map[uint64]*checkpointtool.TableDumpData{},
		types.BuildTS(1, 0),
		filepath.Join(t.TempDir(), "bad"),
		"csv",
		resolver,
		false,
		false,
	)
	require.ErrorContains(t, err, "empty database name")
}

func TestWriteRestoreScriptForExternalTablePackagesSource(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	sourcePath := filepath.Join(tmpDir, "source.csv")
	require.NoError(t, os.WriteFile(sourcePath, []byte("id\n1\n"), 0o644))

	paramJSON, err := json.Marshal(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Option: []string{"filepath", sourcePath, "format", "csv", "compression", "none"},
		},
	})
	require.NoError(t, err)

	table := checkpointtool.TableCatalogEntry{
		AccountID:    1,
		DatabaseID:   2,
		TableID:      3,
		DatabaseName: "db",
		TableName:    "ext_t",
		RelKind:      "e",
	}
	dumpDataByTable := map[uint64]*checkpointtool.TableDumpData{
		table.TableID: {
			TableID: table.TableID,
			Schema: &checkpointtool.TableSchema{
				TableName: "old_ext",
				CreateSQL: string(paramJSON),
				Columns: []checkpointtool.TableColumn{
					{Name: "id", SQLType: "INT", Position: 1},
				},
			},
		},
	}

	scriptPath, err := writeRestoreScript(
		ctx,
		&checkpointtool.CheckpointReader{},
		&dumpOutput{},
		[]checkpointtool.TableCatalogEntry{table},
		dumpDataByTable,
		types.BuildTS(1, 0),
		filepath.Join(tmpDir, "scripts"),
		filepath.Join(tmpDir, "csv"),
		loadDataPathResolver{},
		true,
		true,
	)
	require.ErrorContains(t, err, "show create indexes for table 3")
	require.Empty(t, scriptPath)
}

func TestWriteRestoreScriptForOrdinaryTableWithLoadData(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	table := checkpointtool.TableCatalogEntry{
		AccountID:    1,
		DatabaseID:   2,
		TableID:      3,
		DatabaseName: "db",
		TableName:    "t1",
		RelKind:      "r",
	}
	reader := &checkpointtool.CheckpointReader{}
	reader.SetGetLogicalViewForTest(func(_ *checkpointtool.CheckpointReader, tableID uint64) (*checkpointtool.LogicalTableView, error) {
		if tableID != 2 {
			return nil, fmt.Errorf("unexpected table %d", tableID)
		}
		return &checkpointtool.LogicalTableView{
			Headers: append([]string{"object", "block", "row"}, catalog.MoTablesSchema...),
			Rows: [][]string{
				ckpMoTablesRow(t, "3", "t1", "db", "2", "r", "1"),
			},
		}, nil
	})

	path, err := writeRestoreScript(
		ctx,
		reader,
		&dumpOutput{},
		[]checkpointtool.TableCatalogEntry{table},
		map[uint64]*checkpointtool.TableDumpData{
			table.TableID: {
				TableID: table.TableID,
				Schema: &checkpointtool.TableSchema{
					TableName:    "old_t",
					DatabaseName: "old_db",
					Columns: []checkpointtool.TableColumn{
						{Name: "id", SQLType: "INT", Position: 1},
					},
				},
			},
		},
		types.BuildTS(1, 0),
		filepath.Join(tmpDir, "scripts"),
		filepath.Join(tmpDir, "csv"),
		loadDataPathResolver{},
		true,
		true,
	)
	require.NoError(t, err)
	script, err := os.ReadFile(path)
	require.NoError(t, err)
	got := string(script)
	require.Contains(t, got, "CREATE TABLE `db`.`t1`")
	require.Contains(t, got, "LOAD DATA INFILE")
	require.Contains(t, got, "IGNORE 1 LINES")
	require.Contains(t, got, "parallel 'true'")
}

func TestDumpTablesConcurrentlySkipsViews(t *testing.T) {
	var out bytes.Buffer
	err := dumpTablesConcurrently(
		context.Background(),
		&checkpointtool.CheckpointReader{},
		&dumpOutput{},
		[]tableDumpPlan{{
			table: checkpointtool.TableCatalogEntry{
				DatabaseName: "db",
				TableName:    "v1",
				TableID:      10,
				RelKind:      "view",
			},
		}},
		types.BuildTS(1, 0),
		t.TempDir(),
		1,
		checkpointtool.CSVRowOrderStorage,
		false,
		true,
		&out,
	)
	require.NoError(t, err)
	require.Contains(t, out.String(), "View 10 db.v1 skipped CSV dump")
}

func TestNewLoadDataPathResolverErrors(t *testing.T) {
	_, err := newLoadDataPathResolver(toolfs.StorageOptions{S3: "bucket=b", Backend: "GCS"})
	require.NoError(t, err)
	// Unknown backend falls back to local load-data path because it is not S3 output.
	resolver, err := newLoadDataPathResolver(toolfs.StorageOptions{})
	require.NoError(t, err)
	require.Contains(t, resolver.loadDataSource("out", checkpointtool.TableCatalogEntry{AccountID: 1, DatabaseID: 2, TableID: 3, TableName: "t"}), "LOAD DATA INFILE")

	_, err = newLoadDataPathResolver(toolfs.StorageOptions{S3: "endpoint=http://minio", Backend: "S3"})
	require.ErrorContains(t, err, "missing bucket")
}

func TestDumpOneTableAndConcurrentDumpUsePreparedData(t *testing.T) {
	ctx := context.Background()
	outDir := t.TempDir()
	dumpOut, err := openDumpOutput(ctx, toolfs.StorageOptions{})
	require.NoError(t, err)
	reader := &checkpointtool.CheckpointReader{}
	plan := tableDumpPlan{
		table: checkpointtool.TableCatalogEntry{
			AccountID:    1,
			DatabaseID:   2,
			TableID:      3,
			DatabaseName: "db",
			TableName:    "t",
			RelKind:      "r",
		},
		data: &checkpointtool.TableDumpData{
			TableID: 3,
			Schema: &checkpointtool.TableSchema{
				TableName:    "t",
				DatabaseName: "db",
				Columns: []checkpointtool.TableColumn{
					{Name: "id", SQLType: "INT", Position: 1, PhysicalPosition: 0},
				},
			},
		},
	}

	var status bytes.Buffer
	var statusMu sync.Mutex
	require.NoError(t, dumpOneTable(ctx, reader, dumpOut, plan, types.BuildTS(1, 0), outDir, checkpointtool.CSVRowOrderStorage, false, true, &status, &statusMu))
	require.Contains(t, status.String(), "Table 3 db.t dumped")
	csvPath := tableCSVPath(outDir, plan.table)
	data, err := os.ReadFile(csvPath)
	require.NoError(t, err)
	require.Equal(t, "id\n", string(data))

	status.Reset()
	require.NoError(t, dumpTablesConcurrently(ctx, reader, dumpOut, []tableDumpPlan{plan}, types.BuildTS(1, 0), outDir, 4, checkpointtool.CSVRowOrderStorage, false, true, &status))
	require.Contains(t, status.String(), "Table 3 db.t dumped")
}

func TestDumpOneTableSkipsViewsAndReportsErrors(t *testing.T) {
	ctx := context.Background()
	dumpOut, err := openDumpOutput(ctx, toolfs.StorageOptions{})
	require.NoError(t, err)
	reader := &checkpointtool.CheckpointReader{}

	var status bytes.Buffer
	var statusMu sync.Mutex
	viewPlan := tableDumpPlan{table: checkpointtool.TableCatalogEntry{TableID: 4, DatabaseName: "db", TableName: "v", RelKind: "v"}}
	require.NoError(t, dumpOneTable(ctx, reader, dumpOut, viewPlan, types.BuildTS(1, 0), t.TempDir(), checkpointtool.CSVRowOrderStorage, false, true, &status, &statusMu))
	require.Contains(t, status.String(), "View 4 db.v skipped CSV dump")

	badPlan := tableDumpPlan{
		table: checkpointtool.TableCatalogEntry{TableID: 5, DatabaseName: "db", TableName: "bad", RelKind: "r"},
		data:  &checkpointtool.TableDumpData{TableID: 5},
	}
	err = dumpOneTable(ctx, reader, dumpOut, badPlan, types.BuildTS(1, 0), t.TempDir(), checkpointtool.CSVRowOrderStorage, false, true, &status, &statusMu)
	require.ErrorContains(t, err, "dump table 5")
	require.ErrorContains(t, err, "cannot resolve visible columns")
}

func TestPrepareTableDumpPlansEmptySelection(t *testing.T) {
	plans, err := prepareTableDumpPlans(context.Background(), &checkpointtool.CheckpointReader{}, nil, types.BuildTS(1, 0))
	require.NoError(t, err)
	require.Empty(t, plans)
}

func TestPrepareTableDumpPlansReportsPrepareFailure(t *testing.T) {
	reader := &checkpointtool.CheckpointReader{}
	table := checkpointtool.TableCatalogEntry{TableID: 99, DatabaseName: "db", TableName: "missing"}
	_, err := prepareTableDumpPlans(context.Background(), reader, []checkpointtool.TableCatalogEntry{table}, types.BuildTS(1, 0))
	require.Error(t, err)
}

func TestWriteRestoreScriptForView(t *testing.T) {
	ctx := context.Background()
	dumpOut, err := openDumpOutput(ctx, toolfs.StorageOptions{})
	require.NoError(t, err)
	outputDir := t.TempDir()
	table := checkpointtool.TableCatalogEntry{
		AccountID:    1,
		DatabaseID:   2,
		TableID:      3,
		DatabaseName: "db",
		TableName:    "v",
		RelKind:      "v",
	}
	dumpData := map[uint64]*checkpointtool.TableDumpData{
		table.TableID: {
			TableID: table.TableID,
			Schema: &checkpointtool.TableSchema{
				TableName: "old_v",
				CreateSQL: "CREATE VIEW old_v AS SELECT 1 AS id",
				Columns:   []checkpointtool.TableColumn{{Name: "id", SQLType: "INT", Position: 1}},
			},
		},
	}

	scriptPath, err := writeRestoreScript(ctx, nil, dumpOut, []checkpointtool.TableCatalogEntry{table}, dumpData, types.BuildTS(1, 0), outputDir, outputDir, loadDataPathResolver{}, true, true)
	require.NoError(t, err)
	require.Equal(t, outputPathJoin(outputDir, "restore.sql"), scriptPath)
	script, err := os.ReadFile(scriptPath)
	require.NoError(t, err)
	require.Contains(t, string(script), "CREATE DATABASE IF NOT EXISTS `db`;")
	require.Contains(t, string(script), "CREATE VIEW `db`.`v` AS SELECT 1 AS id")
	require.NotContains(t, string(script), "LOAD DATA")
}

func TestWriteRestoreScriptRejectsMissingDatabase(t *testing.T) {
	ctx := context.Background()
	dumpOut, err := openDumpOutput(ctx, toolfs.StorageOptions{})
	require.NoError(t, err)
	_, err = writeRestoreScript(ctx, nil, dumpOut, []checkpointtool.TableCatalogEntry{{TableID: 7, TableName: "t"}}, nil, types.BuildTS(1, 0), t.TempDir(), t.TempDir(), loadDataPathResolver{}, true, true)
	require.ErrorContains(t, err, "empty database name")
}

func TestPackageExternalTableSourceCopiesAndRewritesFilepath(t *testing.T) {
	tmpDir := t.TempDir()
	sourcePath := filepath.Join(tmpDir, "local_ext_people.csv")
	require.NoError(t, os.WriteFile(sourcePath, []byte("id,name\n1,a\n"), 0o644))

	table := checkpointtool.TableCatalogEntry{
		AccountID:    0,
		DatabaseID:   272731,
		TableID:      272732,
		DatabaseName: "ckp_external",
		TableName:    "ext_csv_local",
		RelKind:      "e",
	}
	ddl := "CREATE EXTERNAL TABLE ext_csv_local (id INT, name VARCHAR(50)) INFILE {'filepath'='" + sourcePath + "', 'format'='csv'}"

	got, err := packageExternalTableSource(context.Background(), &dumpOutput{}, filepath.Join(tmpDir, "dump"), table, ddl)
	require.NoError(t, err)
	assert.NotContains(t, got, sourcePath)

	copiedPath, _, _, ok := externalTableFilepathValueRange(got)
	require.True(t, ok)
	assert.True(t, filepath.IsAbs(copiedPath))
	assert.Contains(t, copiedPath, filepath.Join("external_sources", "account_0", "db_272731"))
	data, err := os.ReadFile(copiedPath)
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,a\n", string(data))
}

func TestPackageExternalTableSourceErrorBranches(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		AccountID:    1,
		DatabaseID:   2,
		TableID:      3,
		DatabaseName: "db",
		TableName:    "ext",
		RelKind:      "e",
	}
	_, err := packageExternalTableSource(context.Background(), &dumpOutput{}, t.TempDir(), table, "CREATE EXTERNAL TABLE ext (id INT)")
	require.ErrorContains(t, err, "does not contain INFILE filepath")

	ddl := "CREATE EXTERNAL TABLE ext (id INT) INFILE {'filepath'='/tmp/missing.csv'}"
	_, err = packageExternalTableSource(context.Background(), &dumpOutput{remote: true}, t.TempDir(), table, ddl)
	require.ErrorContains(t, err, "remote dump output")
}

func TestCopyLocalFileToDumpOutputErrors(t *testing.T) {
	ctx := context.Background()
	err := copyLocalFileToDumpOutput(ctx, &dumpOutput{}, filepath.Join(t.TempDir(), "missing.csv"), filepath.Join(t.TempDir(), "out.csv"))
	require.Error(t, err)

	source := filepath.Join(t.TempDir(), "source.csv")
	require.NoError(t, os.WriteFile(source, []byte("id\n"), 0o644))
	err = copyLocalFileToDumpOutput(ctx, &dumpOutput{}, source, filepath.Join(t.TempDir(), "missing-dir", "out.csv"))
	require.Error(t, err)
}

func TestExternalTableFilepathValueRange(t *testing.T) {
	ddl := "CREATE EXTERNAL TABLE t (id INT) INFILE {'format'='csv', 'filepath'='/tmp/a.csv'}"
	value, start, end, ok := externalTableFilepathValueRange(ddl)
	require.True(t, ok)
	assert.Equal(t, "/tmp/a.csv", value)
	assert.Equal(t, "CREATE EXTERNAL TABLE t (id INT) INFILE {'format'='csv', 'filepath'='/new.csv'}", ddl[:start]+quoteSQLString("/new.csv")+ddl[end:])
}

func ckpMoTablesRow(t *testing.T, relID, relName, dbName, dbID, relKind, accountID string) []string {
	t.Helper()
	data := make([]string, len(catalog.MoTablesSchema))
	set := func(name, value string) {
		for i, header := range catalog.MoTablesSchema {
			if header == name {
				data[i] = value
				return
			}
		}
		t.Fatalf("missing mo_tables header %s", name)
	}
	set(catalog.SystemRelAttr_ID, relID)
	set(catalog.SystemRelAttr_Name, relName)
	set(catalog.SystemRelAttr_DBName, dbName)
	set(catalog.SystemRelAttr_DBID, dbID)
	set(catalog.SystemRelAttr_Kind, relKind)
	set(catalog.SystemRelAttr_AccID, accountID)
	return append([]string{"obj", "0", relID}, data...)
}

func TestExternalParamOptionsAndS3Rendering(t *testing.T) {
	param := tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.S3,
			Option: []string{
				"filepath", "data/part.csv",
				"format", "JSONLINE",
				"compression", "gzip",
				"jsondata", "OBJECT",
				"hive_partitioning", "true",
				"hive_partition_columns", "dt,region",
				"endpoint", "https://s3.example",
				"region", "us-west-2",
				"access_key_id", "ak",
				"secret_access_key", "sk",
				"bucket", "bucket-a",
				"provider", "minio",
				"role_arn", "role",
				"external_id", "external",
			},
			Tail: &tree.TailParameter{
				Fields: &tree.Fields{
					Terminated: &tree.Terminated{Value: "|"},
					EscapedBy:  &tree.EscapedBy{Value: '\t'},
				},
				Lines: &tree.Lines{
					StartingBy:   "#",
					TerminatedBy: &tree.Terminated{Value: "\r\n"},
				},
				IgnoredLines: 2,
			},
		},
	}

	applyExternalParamOptions(&param)

	require.NotNil(t, param.S3Param)
	assert.Equal(t, "jsonline", param.Format)
	assert.Equal(t, "object", param.JsonData)
	assert.True(t, param.HivePartitioning)
	assert.Equal(t, []string{"dt", "region"}, param.HivePartitionCols)

	clause := renderExternalParamClause(&param)
	for _, want := range []string{
		"URL s3option",
		"'endpoint'='https://s3.example'",
		"'bucket'='bucket-a'",
		"'provider'='minio'",
		"'role_arn'='role'",
		"'external_id'='external'",
		"'jsondata'='object'",
		`FIELDS TERMINATED BY '|' ESCAPED BY '\\t'`,
		`LINES STARTING BY '#' TERMINATED BY '\\r\\n'`,
		"IGNORE 2 LINES",
	} {
		assert.Contains(t, clause, want)
	}
}

func TestExternalParamDefaultsAndDecodeErrors(t *testing.T) {
	param := tree.ExternParam{}
	applyExternalParamOptions(&param)
	assert.Equal(t, "auto", param.CompressType)
	assert.Equal(t, "csv", param.Format)
	assert.Contains(t, renderExternalParamClause(&param), "INFILE")

	_, err := renderExternalCreateTableDDLFromParamJSON(
		checkpointtool.TableCatalogEntry{TableID: 7, DatabaseName: "db", TableName: "ext"},
		&checkpointtool.TableSchema{CreateSQL: "{bad json"},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode external table parameter JSON")
}

func TestSQLValueAndIdentifierHelpers(t *testing.T) {
	require.Equal(t, strings.Repeat("x", 240)+"...", summarizeSQLForError(strings.Repeat("x", 300)))

	value, end, ok := readSQLStringOrBareValue(`'a''b\\c' tail`, 0)
	require.True(t, ok)
	assert.Equal(t, `a'b\c`, value)
	assert.Equal(t, len(`'a''b\\c'`), end)

	value, end, ok = readSQLStringOrBareValue(`"quoted value", next`, 0)
	require.True(t, ok)
	assert.Equal(t, "quoted value", value)
	assert.Equal(t, len(`"quoted value"`), end)

	value, end, ok = readSQLStringOrBareValue("bare_value, next", 0)
	require.True(t, ok)
	assert.Equal(t, "bare_value", value)
	assert.Equal(t, len("bare_value"), end)

	_, _, ok = readSQLStringOrBareValue("'unterminated", 0)
	assert.False(t, ok)
	_, _, ok = readSQLStringOrBareValue("", 0)
	assert.False(t, ok)

	value, _, _, ok = externalTableFilepathValueRange("INFILE {'notfilepathx'='bad', filepath=data.csv}")
	require.True(t, ok)
	assert.Equal(t, "data.csv", value)
	_, _, _, ok = externalTableFilepathValueRange("INFILE {'file_path'='bad'}")
	assert.False(t, ok)

	assert.Equal(t, "orders`2024", ddlTableName("CREATE TABLE `db`.`orders``2024` (id int)", 42))
	assert.Equal(t, "42", ddlTableName("SELECT 1", 42))
	unquoted, ok := unquoteSQLIdent(" `a``b` ")
	require.True(t, ok)
	assert.Equal(t, "a`b", unquoted)
	unquoted, ok = unquoteSQLIdent("plain")
	assert.False(t, ok)
	assert.Equal(t, "plain", unquoted)
}

func TestAlterTableAddClauseVariants(t *testing.T) {
	tests := []struct {
		sql  string
		want string
		ok   bool
	}{
		{sql: "ALTER TABLE `db`.`t` ADD UNIQUE KEY `uk` (`c`);", want: "UNIQUE KEY `uk` (`c`)", ok: true},
		{sql: "alter table t add index idx_c(c)", want: "index idx_c(c)", ok: true},
		{sql: "ALTER TABLE t ADD FULLTEXT KEY ft(doc);", want: "FULLTEXT KEY ft(doc)", ok: true},
		{sql: "ALTER TABLE t DROP KEY idx_c", ok: false},
		{sql: "ALTER TABLE ADD KEY bad", ok: false},
		{sql: "CREATE INDEX idx ON t(c)", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			got, ok := alterTableAddClause(tt.sql)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPathAndCommandValidationHelpers(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		AccountID:    9,
		DatabaseID:   10,
		TableID:      11,
		DatabaseName: "db",
		TableName:    "bad/name table",
	}

	assert.Equal(t, "_", safePathPart("   "))
	assert.Equal(t, "bad_name_table", safePathPart(table.TableName))
	assert.Equal(t, "prefix/dir/file.csv", outputS3ObjectKey("/prefix/", "/dir/file.csv"))
	assert.Equal(t, "dir/file.csv", outputS3ObjectKey("", "/dir/file.csv"))
	assert.Equal(t, "out/account_9/db_10/bad_name_table_11.csv", tableCSVPath("out", table))
	assert.Equal(t, "out/external_sources/account_9/db_10/bad_name_table_11_bad_name_table.data", externalSourcePath("out", table, "/"))

	showCmd := showCreateTableCommand(&toolfs.StorageOptions{})
	showCmd.SilenceUsage = true
	showCmd.SilenceErrors = true
	showCmd.SetOut(&bytes.Buffer{})
	showCmd.SetErr(&bytes.Buffer{})
	require.ErrorContains(t, showCmd.Execute(), "--table-id is required")

	rootCmd := PrepareCommand()
	rootCmd.SilenceUsage = true
	rootCmd.SilenceErrors = true
	rootCmd.SetOut(&bytes.Buffer{})
	rootCmd.SetErr(&bytes.Buffer{})
	rootCmd.SetArgs([]string{"--local", "--local2"})
	require.Error(t, rootCmd.Execute())
}

func TestFilterExistingIndexDDLs(t *testing.T) {
	createDDL := "CREATE TABLE `items_gist` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `embedding` vecf32(960) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `ivf_2000` USING ivfflat(`embedding`) lists=2000 op_type 'vector_l2_ops'\n" +
		")"
	indexDDLs := []string{
		"ALTER TABLE `items_gist` ADD KEY `ivf_2000` USING ivfflat(`embedding`) lists = 2000  op_type 'vector_l2_ops' ;",
		"ALTER TABLE `items_gist` ADD KEY `new_idx`(`id`);",
	}

	assert.Equal(t, []string{"ALTER TABLE `items_gist` ADD KEY `new_idx`(`id`);"}, filterExistingIndexDDLs(createDDL, indexDDLs))
}

func TestMergeCreateTableIndexDDLs(t *testing.T) {
	createDDL := "CREATE TABLE `ann`.`items_gist` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `embedding` vecf32(960) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		")"
	indexDDLs := []string{
		"ALTER TABLE `items_gist` ADD KEY `ivf_2000` USING ivfflat(`embedding`) lists = 2000  op_type 'vector_l2_ops' ;",
	}
	want := "CREATE TABLE `ann`.`items_gist` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `embedding` vecf32(960) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `ivf_2000` USING ivfflat(`embedding`) lists = 2000  op_type 'vector_l2_ops'\n" +
		")"

	got, err := mergeCreateTableIndexDDLs(createDDL, indexDDLs)
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestMergeCreateTableIndexDDLsFullText(t *testing.T) {
	createDDL := "CREATE TABLE `ckp_constraints`.`t_fulltext` (\n" +
		"  `id` BIGINT NOT NULL,\n" +
		"  `doc` TEXT DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		")"
	indexDDLs := []string{
		"ALTER TABLE `t_fulltext` ADD FULLTEXT KEY `idx_doc`(`doc`);",
	}
	want := "CREATE TABLE `ckp_constraints`.`t_fulltext` (\n" +
		"  `id` BIGINT NOT NULL,\n" +
		"  `doc` TEXT DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  FULLTEXT KEY `idx_doc`(`doc`)\n" +
		")"

	got, err := mergeCreateTableIndexDDLs(createDDL, indexDDLs)
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestFilterExistingIndexDDLsFullText(t *testing.T) {
	createDDL := "CREATE TABLE `ckp_constraints`.`t_fulltext` (\n" +
		"  `id` BIGINT NOT NULL,\n" +
		"  `doc` TEXT DEFAULT NULL,\n" +
		"  FULLTEXT KEY `idx_doc`(`doc`)\n" +
		")"
	indexDDLs := []string{
		"ALTER TABLE `t_fulltext` ADD FULLTEXT KEY `idx_doc`(`doc`);",
	}

	assert.Empty(t, filterExistingIndexDDLs(createDDL, indexDDLs))
}

func TestFilterExistingIndexDDLsFullTextWithoutKeyKeyword(t *testing.T) {
	createDDL := "CREATE TABLE `ckp_constraints`.`t_fulltext` (\n" +
		"  `id` BIGINT NOT NULL,\n" +
		"  `doc` TEXT DEFAULT NULL,\n" +
		"  FULLTEXT `idx_doc`(`doc`) WITH PARSER ngram\n" +
		")"
	indexDDLs := []string{
		"ALTER TABLE `t_fulltext` ADD FULLTEXT `idx_doc`(`doc`) WITH PARSER ngram;",
	}

	assert.Empty(t, filterExistingIndexDDLs(createDDL, indexDDLs))
}

func TestMergeCreateTableIndexDDLsSingleLine(t *testing.T) {
	createDDL := "CREATE TABLE `ann`.`items_sift` (id int primary key, embedding vecf32(128))"
	indexDDLs := []string{
		"ALTER TABLE `items_sift` ADD KEY `ivf_500` USING ivfflat(`embedding`) lists = 500  op_type 'vector_l2_ops' ;",
	}
	want := "CREATE TABLE `ann`.`items_sift` (id int primary key, embedding vecf32(128), KEY `ivf_500` USING ivfflat(`embedding`) lists = 500  op_type 'vector_l2_ops')"

	got, err := mergeCreateTableIndexDDLs(createDDL, indexDDLs)
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestMergeCreateTableIndexDDLsSkipsExistingIndex(t *testing.T) {
	createDDL := "CREATE TABLE `items_gist` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `embedding` vecf32(960) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `ivf_2000` USING ivfflat(`embedding`) lists=2000 op_type 'vector_l2_ops'\n" +
		")"
	indexDDLs := []string{
		"ALTER TABLE `items_gist` ADD KEY `ivf_2000` USING ivfflat(`embedding`) lists = 2000  op_type 'vector_l2_ops' ;",
	}

	got, err := mergeCreateTableIndexDDLs(createDDL, indexDDLs)
	assert.NoError(t, err)
	assert.Equal(t, createDDL, got)
}

func TestMergeCreateTableIndexDDLsWithConstraintsAndComments(t *testing.T) {
	createDDL := "CREATE TABLE `ckp_constraints`.`parent` (\n" +
		"  `id` INT NOT NULL PRIMARY KEY,\n" +
		"  `code` VARCHAR(20) NOT NULL UNIQUE,\n" +
		"  `note` VARCHAR(100) DEFAULT 'parent-default' COMMENT 'parent note'\n" +
		") COMMENT='parent table comment'"
	indexDDLs := []string{
		"ALTER TABLE `parent` ADD KEY `idx_parent_note`(`note`);",
	}

	got, err := mergeCreateTableIndexDDLs(createDDL, indexDDLs)
	require.NoError(t, err)
	assert.Contains(t, got, "KEY `idx_parent_note`(`note`)")
	assert.Contains(t, got, ") COMMENT='parent table comment'")
}

func TestMergeCreateTableIndexDDLsWithAutoIncrementAndClusterTable(t *testing.T) {
	createDDL := "CREATE CLUSTER TABLE `ckp_constraints`.`t_auto_inc` (\n" +
		"  `id` BIGINT NOT NULL AUTO_INCREMENT,\n" +
		"  `note` VARCHAR(64) COMMENT 'note (with parens)',\n" +
		"  PRIMARY KEY (`id`)\n" +
		") COMMENT='auto increment table'"
	indexDDLs := []string{
		"ALTER TABLE `t_auto_inc` ADD KEY `idx_auto_inc_note`(`note`);",
	}

	got, err := mergeCreateTableIndexDDLs(createDDL, indexDDLs)
	require.NoError(t, err)
	assert.Contains(t, got, "KEY `idx_auto_inc_note`(`note`)")
	assert.Contains(t, got, "COMMENT='auto increment table'")
}

func TestMergeCreateTableIndexDDLsFallsBackToSeparateAlter(t *testing.T) {
	createDDL := "CREATE TABLE `parent` LIKE `parent_template`"
	indexDDLs := []string{
		"ALTER TABLE `parent` ADD KEY `idx_parent_note`(`note`);",
	}

	got, err := mergeCreateTableIndexDDLs(createDDL, indexDDLs)
	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `parent` LIKE `parent_template`;\nALTER TABLE `parent` ADD KEY `idx_parent_note`(`note`)", got)
}

func TestNormalizeCreateTableDDLNameWithCreateTableModifiers(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		DatabaseName: "ckp_constraints",
		TableName:    "parent",
	}

	got := normalizeCreateTableDDLName("CREATE CLUSTER TABLE old_parent (id INT)", table)
	assert.Equal(t, "CREATE CLUSTER TABLE `ckp_constraints`.`parent` (id INT)", got)
}

func TestSQLIdentifierParserEdges(t *testing.T) {
	name, ok := readLeadingSQLIdent(" `a``b` rest")
	require.True(t, ok)
	require.Equal(t, "a`b", name)
	_, ok = readLeadingSQLIdent("`unterminated")
	require.False(t, ok)
	_, ok = readLeadingSQLIdent(" !bad")
	require.False(t, ok)
	name, ok = readLeadingSQLIdent("abc$123 next")
	require.True(t, ok)
	require.Equal(t, "abc$123", name)

	start, end, ok := createTableNameRange("CREATE OR REPLACE TEMPORARY EXTERNAL TABLE IF NOT EXISTS `db`.`t``x` (id INT)")
	require.True(t, ok)
	require.Equal(t, "`db`.`t``x`", "CREATE OR REPLACE TEMPORARY EXTERNAL TABLE IF NOT EXISTS `db`.`t``x` (id INT)"[start:end])
	_, _, ok = createTableNameRange("CREATE TABLE ")
	require.False(t, ok)
	require.Equal(t, "\t", inferCreateTableClauseIndent("(\n\t`id` INT\n)"))
	require.Equal(t, "  ", inferCreateTableClauseIndent("( )"))
}

func TestCleanObjectPath(t *testing.T) {
	assert.Equal(t, "dump/account_1/t.csv", cleanObjectPath("dump/account_1/t.csv"))
	assert.Equal(t, "tmp/dump/t.csv", cleanObjectPath("/tmp/dump/t.csv"))
	assert.Equal(t, "dump/t.csv", cleanObjectPath("dump//nested/../t.csv"))
}

func TestPrintCatalogList(t *testing.T) {
	tables := []checkpointtool.TableCatalogEntry{
		{AccountID: 2, DatabaseID: 20, DatabaseName: "db2", TableID: 201, TableName: "t2", RelKind: "r"},
		{AccountID: 1, DatabaseID: 10, DatabaseName: "db1", TableID: 101, TableName: "t1", RelKind: "v"},
		{AccountID: 1, DatabaseID: 10, DatabaseName: "db1", TableID: 102, TableName: "t1_b", RelKind: "r"},
	}

	t.Run("tables", func(t *testing.T) {
		var buf bytes.Buffer
		require.NoError(t, printCatalogList(&buf, tables, "tables"))
		got := buf.String()
		assert.Contains(t, got, "ACCOUNT_ID")
		assert.Contains(t, got, "db1")
		assert.Contains(t, got, "t2")
		assert.Contains(t, got, "REL_KIND")
	})

	t.Run("databases_dedupes_and_sorts", func(t *testing.T) {
		var buf bytes.Buffer
		require.NoError(t, printCatalogList(&buf, tables, "databases"))
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		require.Len(t, lines, 3)
		assert.Contains(t, lines[1], "1")
		assert.Contains(t, lines[1], "db1")
		assert.Contains(t, lines[2], "db2")
	})

	t.Run("accounts_dedupes_and_sorts", func(t *testing.T) {
		var buf bytes.Buffer
		require.NoError(t, printCatalogList(&buf, tables, "accounts"))
		assert.Equal(t, "ACCOUNT_ID\n1\n2\n", buf.String())
	})

	t.Run("unknown_type", func(t *testing.T) {
		var buf bytes.Buffer
		require.Error(t, printCatalogList(&buf, tables, "indexes"))
	})
}

func TestDumpOutputLocalAndRemoteHelpers(t *testing.T) {
	ctx := context.Background()

	t.Run("nil_or_local", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "out")
		var out *dumpOutput
		require.NoError(t, out.MkdirAll(dir))
		_, err := os.Stat(dir)
		require.NoError(t, err)

		filePath := filepath.Join(dir, "rows.csv")
		w, err := out.Create(ctx, filePath)
		require.NoError(t, err)
		_, err = w.Write([]byte("a,b\n"))
		require.NoError(t, err)
		require.NoError(t, w.Close())
		data, err := os.ReadFile(filePath)
		require.NoError(t, err)
		assert.Equal(t, "a,b\n", string(data))
		out.Close(ctx)
	})

	t.Run("remote_noop_mkdir", func(t *testing.T) {
		out := &dumpOutput{remote: true}
		require.NoError(t, out.MkdirAll(filepath.Join(t.TempDir(), "not-created")))
		out.Close(ctx)
	})
}

func TestParseTSAndAllDigits(t *testing.T) {
	ts, err := parseTS("123:4")
	require.NoError(t, err)
	assert.Equal(t, types.BuildTS(123, 4), ts)

	ts, err = parseTS("123-5")
	require.NoError(t, err)
	assert.Equal(t, types.BuildTS(123, 5), ts)

	ts, err = parseTS("2024-01-02T03:04:05Z")
	require.NoError(t, err)
	assert.False(t, ts.IsEmpty())

	ts, err = parseTS("2024-01-02 03:04:05")
	require.NoError(t, err)
	assert.False(t, ts.IsEmpty())

	_, err = parseTS("123:not-a-logical")
	require.Error(t, err)
	_, err = parseTS("bad timestamp")
	require.Error(t, err)
	_, err = parseTS("123:42949672960")
	require.Error(t, err)

	assert.True(t, allDigits("0123456789"))
	assert.False(t, allDigits(""))
	assert.False(t, allDigits("12a"))
}

func TestDumpCommandValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing selector",
			args: nil,
			want: "--table-id",
		},
		{
			name: "no-load without load-script",
			args: []string{"--table-id=1", "--no-load"},
			want: "--no-load requires --load-script",
		},
		{
			name: "table id with database id",
			args: []string{"--table-id=1", "--database-id=2"},
			want: "--table-id cannot be combined",
		},
		{
			name: "batch output without load script",
			args: []string{"--database-id=2", "--output=out.csv"},
			want: "--output cannot be used",
		},
		{
			name: "load script requires output",
			args: []string{"--table-id=1", "--load-script"},
			want: "--output/-o directory is required",
		},
		{
			name: "batch dump requires output dir",
			args: []string{"--database-id=2"},
			want: "--output-dir is required",
		},
		{
			name: "cpuprofile create error",
			args: []string{"--table-id=1", "--cpuprofile", filepath.Join(t.TempDir(), "missing", "cpu.pprof")},
			want: "create cpuprofile",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var storage toolfs.StorageOptions
			cmd := dumpCommand(&storage)
			cmd.SilenceUsage = true
			cmd.SilenceErrors = true
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			cmd.SetArgs(tt.args)
			err := cmd.Execute()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestShowCreateTableCommandRequiresTableID(t *testing.T) {
	var storage toolfs.StorageOptions
	cmd := showCreateTableCommand(&storage)
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs(nil)
	err := cmd.Execute()
	require.ErrorContains(t, err, "--table-id is required")
}

func TestCheckpointCommandsRejectConflictingLocalFlags(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	tests := []struct {
		name string
		args []string
	}{
		{name: "view", args: []string{"--local", "--local2", "view", t.TempDir()}},
		{name: "list", args: []string{"--local", "--local2", "list", t.TempDir()}},
		{name: "dump", args: []string{"--local", "--local2", "dump", "--table-id=1", t.TempDir()}},
		{name: "show-create", args: []string{"--local", "--local2", "show-create-table", "--table-id=1", t.TempDir()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := PrepareCommand()
			cmd.SilenceUsage = true
			cmd.SilenceErrors = true
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			cmd.SetArgs(tt.args)
			require.Error(t, cmd.Execute())
		})
	}
}

func TestLoadDataPathResolverLocal(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		AccountID:  0,
		DatabaseID: 272577,
		TableID:    272578,
		TableName:  "bmsql_config",
	}

	resolver, err := newLoadDataPathResolver(toolfs.StorageOptions{})
	require.NoError(t, err)
	loadSource := resolver.loadDataSource("bmsql_config", table)
	assert.True(t, strings.HasPrefix(loadSource, "LOAD DATA INFILE '"))
	assert.Contains(t, loadSource, "/bmsql_config/account_0/db_272577/bmsql_config_272578.csv'")
	assert.True(t, filepath.IsAbs(strings.TrimSuffix(strings.TrimPrefix(loadSource, "LOAD DATA INFILE '"), "'")))
}

func TestLoadDataPathResolverS3(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		AccountID:  0,
		DatabaseID: 272577,
		TableID:    272578,
		TableName:  "bmsql_config",
	}

	resolver, err := newLoadDataPathResolver(toolfs.StorageOptions{
		S3: "bucket=mo-nightly-gz-1308875761,endpoint=https://cos.ap-guangzhou.myqcloud.com,region=ap-guangzhou," +
			"key-prefix=/ckp-dump-test/tpcc_100_20260612_174916/bmsql_config/,key-id=xxx,key-secret=yyy",
		Backend: "S3",
	})
	require.NoError(t, err)
	assert.Equal(t,
		"LOAD DATA URL s3option{'bucket'='mo-nightly-gz-1308875761', 'filepath'='ckp-dump-test/tpcc_100_20260612_174916/bmsql_config/bmsql_config/account_0/db_272577/bmsql_config_272578.csv', 'endpoint'='https://cos.ap-guangzhou.myqcloud.com', 'region'='ap-guangzhou', 'access_key_id'='xxx', 'secret_access_key'='yyy'}",
		resolver.loadDataSource("bmsql_config", table),
	)
}

// TestCkpOfflineKindFlags checks the ckp command exposes the local format flags.
// The branch keeps --s3 as remote storage arguments, so local DISK remains the
// default and DISK-V2 is selected explicitly with --local2.
func TestCkpOfflineKindFlags(t *testing.T) {
	cmd := PrepareCommand()
	for _, name := range []string{"local", "local2"} {
		require.NotNilf(t, cmd.PersistentFlags().Lookup(name), "ckp --%s", name)
	}

	kind, err := kindFromFlags(cmd)
	require.NoError(t, err)
	require.Equal(t, objectio.OfflineKindLocal, kind)

	c2 := PrepareCommand()
	c2.SetArgs([]string{"--local2"})
	require.NoError(t, c2.ParseFlags([]string{"--local2"}))
	kind, err = kindFromFlags(c2)
	require.NoError(t, err)
	require.Equal(t, objectio.OfflineKindLocal2, kind)

	c3 := PrepareCommand()
	require.NoError(t, c3.ParseFlags([]string{"--local", "--local2"}))
	_, err = kindFromFlags(c3)
	require.Error(t, err)
}

// TestCkpInfoEmptyDir runs `info` against an empty dir: the offline fs opens,
// finds zero checkpoint metas, prints the summary and returns nil. It exercises
// infoCommand/setupLogFile/checkpointtool.Open without launching the TUI.
func TestCkpInfoEmptyDir(t *testing.T) {
	// setupLogFile writes $HOME/.mo-tool/ckp.log; point HOME at a writable temp
	// dir so the test does not depend on (or pollute) the real home directory
	// and works in a read-only-home sandbox.
	t.Setenv("HOME", t.TempDir())
	cmd := PrepareCommand()
	cmd.SetArgs([]string{"info", "--local", t.TempDir()})
	require.NoError(t, cmd.Execute())
}
