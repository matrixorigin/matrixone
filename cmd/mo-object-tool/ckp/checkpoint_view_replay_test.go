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
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/stretchr/testify/require"
)

func checkpointViewTestData(t *testing.T, stmt, mode, defaultDB, security string) *checkpointtool.TableDumpData {
	t.Helper()
	data, err := json.Marshal(sqlplan.ViewData{Stmt: stmt, DefaultDatabase: defaultDB, SQLMode: &mode, SecurityType: security})
	require.NoError(t, err)
	return &checkpointtool.TableDumpData{Schema: &checkpointtool.TableSchema{ViewDefinitionKnown: true, ViewDefinition: string(data)}}
}

func TestCheckpointViewRestoreCreationContext(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{TableID: 1, DatabaseName: "view_db", TableName: "actual", RelKind: "v"}
	for _, tc := range []struct{ name, stmt, mode string }{
		{"no backslash escapes", `CREATE VIEW old AS SELECT 'a\b' AS value`, "NO_BACKSLASH_ESCAPES"},
		{"ansi quotes", `CREATE VIEW "old" AS SELECT "col" FROM source`, "ANSI_QUOTES"},
		{"pipes", `CREATE VIEW old AS SELECT 'a'||'b' AS value`, "PIPES_AS_CONCAT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := checkpointViewTestData(t, tc.stmt, tc.mode, "source_db", "INVOKER")
			view, err := prepareCheckpointViewRestore(context.Background(), table, data)
			require.NoError(t, err)
			require.Equal(t, tc.mode, view.sqlMode)
			require.Equal(t, "source_db", view.defaultDB)
			require.Equal(t, "INVOKER", view.security)
			require.Contains(t, view.ddl, "VIEW `view_db`.`actual` AS")
		})
	}
}

func TestCheckpointViewRestoreRejectsUnverifiedMetadata(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{TableID: 1, DatabaseName: "db", TableName: "v", RelKind: "v"}
	for _, data := range []*checkpointtool.TableDumpData{
		nil,
		{Schema: &checkpointtool.TableSchema{}},
		{Schema: &checkpointtool.TableSchema{ViewDefinitionKnown: true}},
		{Schema: &checkpointtool.TableSchema{ViewDefinitionKnown: true, ViewDefinition: "{"}},
		checkpointViewTestData(t, "CREATE VIEW v AS SELECT 1; DROP TABLE t", "", "db", "DEFINER"),
		checkpointViewTestData(t, "CREATE VIEW v AS SELECT 1", "NO_BACKSLASH_ESCAPES'; DROP TABLE t", "db", "DEFINER"),
		checkpointViewTestData(t, "CREATE VIEW v AS SELECT 1", "", "", "DEFINER"),
		checkpointViewTestData(t, "CREATE VIEW v AS SELECT 1", "", "db", "unknown"),
	} {
		_, err := prepareCheckpointViewRestore(context.Background(), table, data)
		require.Error(t, err)
	}
}

func TestCheckpointViewWithoutDefaultDatabase(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{TableID: 1, DatabaseName: "db", TableName: "v", RelKind: "v"}
	view, err := prepareCheckpointViewRestore(context.Background(), table,
		checkpointViewTestData(t, "CREATE VIEW db.old AS SELECT 1 AS n", "", "", "DEFINER"))
	require.NoError(t, err)
	require.Equal(t, "db", view.defaultDB)
	require.Equal(t, "CREATE VIEW `db`.`v` AS SELECT 1 AS n", view.ddl)
	_, err = prepareCheckpointViewRestore(context.Background(), table,
		checkpointViewTestData(t, "CREATE VIEW db.old AS SELECT * FROM source", "", "", "DEFINER"))
	require.ErrorContains(t, err, "not a self-contained literal SELECT")
}

func TestCheckpointViewScriptRestoresModeAndDatabase(t *testing.T) {
	view := checkpointtool.TableCatalogEntry{TableID: 2, DatabaseName: "view_db", TableName: "v", RelKind: "v"}
	data := checkpointViewTestData(t, `CREATE VIEW old AS SELECT 'a\b' AS value`, "NO_BACKSLASH_ESCAPES", "source_db", "INVOKER")
	for _, includeLoad := range []bool{false, true} {
		dir := t.TempDir()
		path, err := writeRestoreScript(context.Background(), nil, &dumpOutput{}, []checkpointtool.TableCatalogEntry{view}, map[uint64]*checkpointtool.TableDumpData{2: data}, types.TS{}, dir, dir, loadDataPathResolver{}, includeLoad, false)
		require.NoError(t, err)
		contents, err := os.ReadFile(path)
		require.NoError(t, err)
		script := string(contents)
		want := []string{
			"SET SESSION sql_mode = 'NO_BACKSLASH_ESCAPES';",
			"SET SESSION view_security_type = 'INVOKER';",
			"USE `source_db`;",
			"CREATE VIEW `view_db`.`v` AS SELECT 'a\\b' AS value",
			"SET SESSION sql_mode = @__mo_ckp_normal_sql_mode;",
			"SET SESSION view_security_type = @__mo_ckp_saved_view_security_type;",
			"USE `view_db`;",
			"SET SESSION sql_mode = @__mo_ckp_saved_sql_mode;",
		}
		last := 0
		for _, part := range want {
			index := strings.Index(script[last:], part)
			require.GreaterOrEqual(t, index, 0, "missing %q", part)
			last += index + len(part)
		}
	}
}

func TestCheckpointViewCrossDatabaseOrderAndContext(t *testing.T) {
	consumer := checkpointtool.TableCatalogEntry{TableID: 10, DatabaseName: "view_db", TableName: "consumer", RelKind: "v"}
	dependency := checkpointtool.TableCatalogEntry{TableID: 11, DatabaseName: "source_db", TableName: "base_view", RelKind: "v"}
	data := map[uint64]*checkpointtool.TableDumpData{
		10: checkpointViewTestData(t, "CREATE VIEW consumer AS SELECT * FROM base_view", "", "source_db", "DEFINER"),
		11: checkpointViewTestData(t, "CREATE VIEW base_view AS SELECT 1 AS id", "PIPES_AS_CONCAT", "source_db", "INVOKER"),
	}
	order := buildRestoreTableOrder(context.Background(), []checkpointtool.TableCatalogEntry{consumer, dependency}, data)
	require.Equal(t, []checkpointtool.TableCatalogEntry{dependency, consumer}, order.tables)

	// A selected default database containing only unrelated views may otherwise
	// appear later in restore order; create it before USE, then switch back.
	data[10] = checkpointViewTestData(t, "CREATE VIEW consumer AS SELECT 1 AS id", "", "source_db", "DEFINER")
	dir := t.TempDir()
	path, err := writeRestoreScript(context.Background(), nil, &dumpOutput{}, []checkpointtool.TableCatalogEntry{consumer, dependency}, data, types.TS{}, dir, dir, loadDataPathResolver{}, false, false)
	require.NoError(t, err)
	bytes, err := os.ReadFile(path)
	require.NoError(t, err)
	script := string(bytes)
	created := strings.Index(script, "CREATE DATABASE IF NOT EXISTS `source_db`;")
	used := strings.Index(script, "USE `source_db`;")
	require.GreaterOrEqual(t, created, 0)
	require.Greater(t, used, created)
}

func TestCheckpointViewCommentsBeforeTarget(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{TableID: 1, DatabaseName: "db", TableName: "v", RelKind: "v"}
	for _, stmt := range []string{
		"CREATE VIEW /* explanatory */ old AS SELECT 1",
		"CREATE VIEW -- explanatory\nold AS SELECT 1",
		"CREATE VIEW # explanatory\nold AS SELECT 1",
		"CREATE VIEW source_db -- explanatory\n . /* marker */ old AS SELECT 1",
	} {
		data := checkpointViewTestData(t, stmt, "", "db", "DEFINER")
		view, err := prepareCheckpointViewRestore(context.Background(), table, data)
		require.NoError(t, err)
		require.Contains(t, view.ddl, "`db`.`v` AS SELECT 1")
	}
}

func TestCheckpointViewCrossDatabaseANSIQuotedDependency(t *testing.T) {
	consumer := checkpointtool.TableCatalogEntry{TableID: 10, DatabaseName: "first_db", TableName: "consumer", RelKind: "v"}
	dependency := checkpointtool.TableCatalogEntry{TableID: 11, DatabaseName: "other_db", TableName: "dep_view", RelKind: "v"}
	data := map[uint64]*checkpointtool.TableDumpData{
		10: checkpointViewTestData(t, `CREATE VIEW consumer AS SELECT * FROM "other_db"."dep_view"`, "ANSI_QUOTES", "first_db", "DEFINER"),
		11: checkpointViewTestData(t, "CREATE VIEW dep_view AS SELECT 1 AS id", "", "other_db", "DEFINER"),
	}
	order := buildRestoreTableOrder(context.Background(), []checkpointtool.TableCatalogEntry{consumer, dependency}, data)
	require.Equal(t, []checkpointtool.TableCatalogEntry{dependency, consumer}, order.tables)
}

func TestCheckpointReplaySQLModeServerValues(t *testing.T) {
	for _, tc := range []struct{ saved, want string }{
		{"", ""},
		{"ANS", ""}, // historical ANSI value had no recognized parser flags
		{"ANSI", "ANSI"},
		{"ANSI_QUOTES,NO_BACKSLASH_ESCAPES", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"},
		{"ansi, only_full_group_by", "ANSI,ONLY_FULL_GROUP_BY"},
	} {
		t.Run(tc.saved, func(t *testing.T) {
			got, err := checkpointReplaySQLMode(tc.saved)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, mode := range []string{"ANSIONLY_FULL_GROUP_BY", "UNKNOWN", "ANSI'; DROP TABLE t"} {
		_, err := checkpointReplaySQLMode(mode)
		require.Error(t, err)
	}
}

func TestCheckpointViewWithoutDefaultDatabaseDependencies(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{TableID: 1, DatabaseName: "db", TableName: "v", RelKind: "v"}
	for _, tc := range []struct {
		name, sql, database, relation string
	}{
		{"expression", "CREATE VIEW db.v AS SELECT (1 + 2) * -3 AS n", "", ""},
		{"qualified", "CREATE VIEW db.v AS SELECT n FROM other.t", "other", "t"},
		{"qualified wildcard", "CREATE VIEW db.v AS SELECT * FROM other.t", "other", "t"},
		{"qualified trivia", "CREATE VIEW db.v AS SELECT n FROM `other` /* comment */ . `t`", "other", "t"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view, err := prepareCheckpointViewRestore(context.Background(), table, checkpointViewTestData(t, tc.sql, "", "", "DEFINER"))
			require.NoError(t, err)
			require.Equal(t, tc.database, view.sourceDatabase)
			require.Equal(t, tc.relation, view.sourceTable)
		})
	}
	for _, sql := range []string{
		"CREATE VIEW db.v AS SELECT n FROM t",
		"CREATE VIEW db.v AS SELECT f(1)",
		"CREATE VIEW db.v AS SELECT (SELECT 1)",
		"CREATE VIEW db.v AS SELECT n FROM other.t WHERE n = 1",
		"CREATE VIEW db.v AS SELECT n FROM other.t JOIN other.u ON t.n = u.n",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := prepareCheckpointViewRestore(context.Background(), table, checkpointViewTestData(t, sql, "", "", "DEFINER"))
			require.Error(t, err)
		})
	}
	dependency := checkpointtool.TableCatalogEntry{TableID: 2, DatabaseName: "other", TableName: "t", RelKind: "v"}
	data := map[uint64]*checkpointtool.TableDumpData{
		1: checkpointViewTestData(t, "CREATE VIEW db.v AS SELECT n FROM `other` /* comment */ . `t`", "", "", "DEFINER"),
		2: checkpointViewTestData(t, "CREATE VIEW other.t AS SELECT 1 AS n", "", "", "DEFINER"),
	}
	order := buildRestoreTableOrder(context.Background(), []checkpointtool.TableCatalogEntry{table, dependency}, data)
	require.Equal(t, []checkpointtool.TableCatalogEntry{dependency, table}, order.tables)
}

func TestCheckpointViewQualifiedDependencyWithDifferentDefault(t *testing.T) {
	consumer := checkpointtool.TableCatalogEntry{TableID: 10, DatabaseName: "app", TableName: "a_consumer", RelKind: "v"}
	dependency := checkpointtool.TableCatalogEntry{TableID: 11, DatabaseName: "app", TableName: "z_dep", RelKind: "v"}
	data := map[uint64]*checkpointtool.TableDumpData{
		10: checkpointViewTestData(t, "CREATE VIEW app.a_consumer AS SELECT * FROM app /* source */ . z_dep", "", "other", "DEFINER"),
		11: checkpointViewTestData(t, "CREATE VIEW app.z_dep AS SELECT 1 AS n", "", "app", "DEFINER"),
	}
	order := buildRestoreTableOrder(context.Background(), []checkpointtool.TableCatalogEntry{consumer, dependency}, data)
	require.Equal(t, []checkpointtool.TableCatalogEntry{dependency, consumer}, order.tables)
}

func TestCheckpointViewOrderingIgnoresLiteralDependencies(t *testing.T) {
	first := checkpointtool.TableCatalogEntry{TableID: 20, DatabaseName: "app", TableName: "a", RelKind: "v"}
	second := checkpointtool.TableCatalogEntry{TableID: 21, DatabaseName: "app", TableName: "z_dep", RelKind: "v"}
	for _, tc := range []struct{ name, mode, literal string }{
		{"escaped", "", `'app.\z_dep'`},
		{"no backslash escapes", "NO_BACKSLASH_ESCAPES", `'app.z_dep'`},
		{"ansi quotes", "ANSI_QUOTES", `'app.z_dep'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := map[uint64]*checkpointtool.TableDumpData{
				20: checkpointViewTestData(t, "CREATE VIEW app.a AS SELECT "+tc.literal+" AS txt", tc.mode, "app", "DEFINER"),
				21: checkpointViewTestData(t, "CREATE VIEW app.z_dep AS SELECT * FROM app.a", "", "app", "DEFINER"),
			}
			order := buildRestoreTableOrder(context.Background(), []checkpointtool.TableCatalogEntry{first, second}, data)
			require.Equal(t, []checkpointtool.TableCatalogEntry{first, second}, order.tables)
		})
	}
}
