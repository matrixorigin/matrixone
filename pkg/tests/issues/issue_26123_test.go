// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue26123DatabaseCopiesKeepLikeMetacharacterFKTables(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		execSQLRequire(t, ctx, db, "set role moadmin")
		execSQLRequire(t, ctx, db, "select mo_feature_registry_upsert('branch', 'Branch feature', '{\"allowed_scope\":[]}', true)")

		copyModes := []struct {
			name string
			key  string
			copy func(sourceDB, targetDB string) string
		}{
			{
				name: "data branch",
				key:  "branch",
				copy: func(sourceDB, targetDB string) string {
					return "data branch create database `" + targetDB + "` from `" + sourceDB + "`"
				},
			},
			{
				name: "clone",
				key:  "clone",
				copy: func(sourceDB, targetDB string) string {
					return "create database `" + targetDB + "` clone `" + sourceDB + "`"
				},
			},
		}
		tableCases := []struct {
			name           string
			tableName      string
			tableNameSQL   string
			collisionTable string
		}{
			{name: "underscore", tableName: "a_b", tableNameSQL: "`a_b`", collisionTable: "`a0b`"},
			{name: "percent", tableName: "a%b", tableNameSQL: "`a%b`", collisionTable: "`a0b`"},
			{name: "backslash", tableName: `child\fk`, tableNameSQL: "`child\\fk`"},
		}

		for _, mode := range copyModes {
			for _, tc := range tableCases {
				t.Run(mode.name+"/"+tc.name, func(t *testing.T) {
					sourceDB := "issue_26123_" + mode.key + "_" + tc.name + "_src"
					targetDB := "issue_26123_" + mode.key + "_" + tc.name + "_dst"
					defer func() {
						cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cleanupCancel()
						execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+targetDB+"`")
						execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+sourceDB+"`")
					}()

					execSQLRequire(t, ctx, db, "drop database if exists `"+targetDB+"`")
					execSQLRequire(t, ctx, db, "drop database if exists `"+sourceDB+"`")
					execSQLRequire(t, ctx, db, "create database `"+sourceDB+"`")
					execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`.`parent_t` (id int primary key)")
					if tc.collisionTable != "" {
						execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`."+tc.collisionTable+" (id int primary key)")
						execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`."+tc.collisionTable+" values (22)")
					}
					execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`."+tc.tableNameSQL+" (id int primary key, parent_id int, foreign key (parent_id) references `"+sourceDB+"`.`parent_t`(id))")
					execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`.`parent_t` values (1)")
					execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`."+tc.tableNameSQL+" values (11, 1)")

					execSQLRequire(t, ctx, db, mode.copy(sourceDB, targetDB))
					assertIssue26123CopiedFKTable(t, ctx, db, sourceDB, targetDB, tc.tableName, tc.tableNameSQL, tc.collisionTable)
				})
			}
		}
	})
}

func assertIssue26123CopiedFKTable(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	sourceDatabaseName string,
	databaseName string,
	tableName string,
	tableNameSQL string,
	collisionTableSQL string,
) {
	t.Helper()

	var id, parentID int
	require.NoError(t, db.QueryRowContext(ctx,
		"select id, parent_id from `"+databaseName+"`."+tableNameSQL).Scan(&id, &parentID))
	require.Equal(t, 11, id)
	require.Equal(t, 1, parentID)
	if collisionTableSQL != "" {
		require.NoError(t, db.QueryRowContext(ctx,
			"select id from `"+databaseName+"`."+collisionTableSQL).Scan(&id))
		require.Equal(t, 22, id)
	}

	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_tables where account_id = 0 and reldatabase = ? and relname = ?",
		databaseName, tableName).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_foreign_keys where db_name = ? and refer_db_name = ? and refer_table_name = 'parent_t'",
		databaseName, databaseName).Scan(&count))
	require.Equal(t, 1, count)
	var sourceFKTableName, targetFKTableName string
	require.NoError(t, db.QueryRowContext(ctx,
		"select table_name from mo_catalog.mo_foreign_keys where db_name = ? and refer_db_name = ? and refer_table_name = 'parent_t'",
		sourceDatabaseName, sourceDatabaseName).Scan(&sourceFKTableName))
	require.NoError(t, db.QueryRowContext(ctx,
		"select table_name from mo_catalog.mo_foreign_keys where db_name = ? and refer_db_name = ? and refer_table_name = 'parent_t'",
		databaseName, databaseName).Scan(&targetFKTableName))
	require.Equal(t, sourceFKTableName, targetFKTableName)

	_, err := db.ExecContext(ctx,
		"insert into `"+databaseName+"`."+tableNameSQL+" values (?, ?)", 111, 999)
	require.Error(t, err)
}
