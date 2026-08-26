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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue26121DatabaseOperationsKeepOrdinaryInternalLookingTables(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		execSQLRequire(t, ctx, db, "set role moadmin")

		const (
			sourceDB     = "issue_26121_source"
			branchDB     = "issue_26121_branch"
			cloneDB      = "issue_26121_clone"
			deleteDB     = "issue_26121_delete"
			restoreDB    = "issue_26121_after_snapshot"
			snapshotName = "issue_26121_account_snapshot"
		)
		deleteCases := []string{"__mo_tmp_user_keep", "__mo_account_lock", "mo_increment_columns"}
		cleanupDBs := []string{branchDB, cloneDB, sourceDB, deleteDB, restoreDB}
		execSQLMaybe(t, ctx, db, "drop snapshot if exists "+snapshotName)
		for _, name := range cleanupDBs {
			execSQLMaybe(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", name))
		}
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			for _, name := range cleanupDBs {
				execSQLMaybe(t, cleanupCtx, db, fmt.Sprintf("drop database if exists `%s`", name))
			}
			execSQLMaybe(t, cleanupCtx, db, "drop snapshot if exists "+snapshotName)
		}()

		t.Run("account restore skips bootstrap index tables", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create snapshot `"+snapshotName+"` for account sys")
			execSQLRequire(t, ctx, db, "create database `"+restoreDB+"`")
			execSQLRequire(t, ctx, db, "restore account sys {snapshot=\""+snapshotName+"\"}")

			var count int
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_database where datname = ?", restoreDB).Scan(&count))
			require.Zero(t, count)
			execSQLRequire(t, ctx, db, "drop snapshot `"+snapshotName+"`")
		})

		t.Run("database copies", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create database `"+sourceDB+"`")
			tableNames := []string{"__mo_tmp_user_data", "__mo_account_lock", "mo_increment_columns"}
			for i, tableName := range tableNames {
				execSQLRequire(t, ctx, db, fmt.Sprintf(
					"create table `%s`.`%s` (id int primary key, note varchar(32))",
					sourceDB, tableName))
				execSQLRequire(t, ctx, db, fmt.Sprintf(
					"insert into `%s`.`%s` values (%d, 'ordinary-user-row')",
					sourceDB, tableName, i+1))
			}
			execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`.`docs` (id bigint primary key, body text)")
			execSQLRequire(t, ctx, db, "create fulltext index `ft_body` on `"+sourceDB+"`.`docs` (`body`)")
			execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`.`docs` values (1, 'one document')")
			var sourceHidden int
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_tables where reldatabase = ? and relname like '__mo_index_%'",
				sourceDB).Scan(&sourceHidden))
			require.Equal(t, 1, sourceHidden)

			execSQLRequire(t, ctx, db, "data branch create database `"+branchDB+"` from `"+sourceDB+"`")
			execSQLRequire(t, ctx, db, "create database `"+cloneDB+"` clone `"+sourceDB+"`")

			t.Run("ordinary internal-looking tables", func(t *testing.T) {
				for _, copiedDB := range []string{branchDB, cloneDB} {
					for _, tableName := range tableNames {
						var count int
						err = db.QueryRowContext(ctx, fmt.Sprintf(
							"select count(*) from `%s`.`%s`", copiedDB, tableName)).Scan(&count)
						require.NoErrorf(t, err, "%s.%s must be copied", copiedDB, tableName)
						require.Equal(t, 1, count, "%s.%s must retain its row", copiedDB, tableName)
					}
				}
			})

			t.Run("fulltext storage", func(t *testing.T) {
				for _, copiedDB := range []string{branchDB, cloneDB} {
					var targetHidden int
					require.NoError(t, db.QueryRowContext(ctx,
						"select count(*) from mo_catalog.mo_tables where reldatabase = ? and relname like '__mo_index_%'",
						copiedDB).Scan(&targetHidden))
					require.Equal(t, sourceHidden, targetHidden)
					var rows int
					require.NoError(t, db.QueryRowContext(ctx,
						"select count(*) from `"+copiedDB+"`.`docs`").Scan(&rows))
					require.Equal(t, 1, rows)
				}
			})
		})

		t.Run("delete validation sees ordinary tables", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create database `"+deleteDB+"`")
			execSQLRequire(t, ctx, db, "create table `"+deleteDB+"`.`base` (id int primary key)")
			execSQLRequire(t, ctx, db, "insert into `"+deleteDB+"`.`base` values (1)")
			execSQLRequire(t, ctx, db, "data branch create table `"+deleteDB+"`.`branch_t` from `"+deleteDB+"`.`base`")
			execSQLRequire(t, ctx, db, "drop table `"+deleteDB+"`.`base`")

			for _, tableName := range deleteCases {
				t.Run(tableName, func(t *testing.T) {
					execSQLRequire(t, ctx, db, "create table `"+deleteDB+"`.`"+tableName+"` (id int primary key)")
					execSQLRequire(t, ctx, db, "insert into `"+deleteDB+"`.`"+tableName+"` values (1)")

					_, err = db.ExecContext(ctx, "data branch delete database `"+deleteDB+"`")
					require.Error(t, err)
					require.Contains(t, err.Error(), "is not an active branch table")

					var count int
					require.NoError(t, db.QueryRowContext(ctx,
						"select count(*) from `"+deleteDB+"`.`"+tableName+"`").Scan(&count))
					require.Equal(t, 1, count)
					execSQLRequire(t, ctx, db, "drop table `"+deleteDB+"`.`"+tableName+"`")
				})
			}
		})
	})
}
