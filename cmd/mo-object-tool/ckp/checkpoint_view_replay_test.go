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
		{"legacy ANSI alias", `CREATE VIEW old AS SELECT 1`, "ANS"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := checkpointViewTestData(t, tc.stmt, tc.mode, "source_db", "INVOKER")
			view, err := prepareCheckpointViewRestore(context.Background(), table, data)
			require.NoError(t, err)
			wantMode := tc.mode
			if wantMode == "ANS" {
				wantMode = "ANSI"
			}
			require.Equal(t, wantMode, view.sqlMode)
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
	for _, tc := range []struct {
		stmt, want, mode string
	}{
		{"CREATE VIEW db.expression_v AS SELECT 1 + 1 AS n", "CREATE VIEW `db`.`v` AS SELECT 1 + 1 AS n", "ANS"},
		{"CREATE VIEW db.qualified_v AS SELECT n FROM db.t", "CREATE VIEW `db`.`v` AS SELECT n FROM db.t", "ANS"},
		{"CREATE VIEW db.cte_v AS WITH q AS (SELECT n FROM db.t) SELECT n FROM q", "CREATE VIEW `db`.`v` AS WITH q AS (SELECT n FROM db.t) SELECT n FROM q", "ANS"},
	} {
		view, err := prepareCheckpointViewRestore(context.Background(), table,
			checkpointViewTestData(t, tc.stmt, tc.mode, "", "DEFINER"))
		require.NoError(t, err)
		require.Equal(t, "db", view.defaultDB)
		require.Equal(t, "ANSI", view.sqlMode)
		require.Equal(t, tc.want, view.ddl)
	}
	for _, stmt := range []string{
		"CREATE VIEW db.old AS SELECT * FROM source",
		"CREATE VIEW db.old AS SELECT (SELECT n FROM source) AS n",
		"CREATE VIEW db.old AS SELECT DATABASE() AS db_name",
		"CREATE VIEW db.old AS WITH q AS (SELECT n FROM source) SELECT n FROM q",
	} {
		_, err := prepareCheckpointViewRestore(context.Background(), table,
			checkpointViewTestData(t, stmt, "", "", "DEFINER"))
		require.ErrorContains(t, err, "unqualified or unsupported relation")
	}
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
	order := buildRestoreTableOrder([]checkpointtool.TableCatalogEntry{consumer, dependency}, data)
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
	order := buildRestoreTableOrder([]checkpointtool.TableCatalogEntry{consumer, dependency}, data)
	require.Equal(t, []checkpointtool.TableCatalogEntry{dependency, consumer}, order.tables)
}
