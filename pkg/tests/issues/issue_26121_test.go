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
	embed.RunBaseClusterTests(func(c embed.Cluster) {
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
			sourceDB = "issue_26121_source"
			branchDB = "issue_26121_branch"
			cloneDB  = "issue_26121_clone"
		)
		deleteCases := []struct {
			dbName    string
			tableName string
		}{
			{dbName: "issue_26121_delete_tmp", tableName: "__mo_tmp_user_keep"},
			{dbName: "issue_26121_delete_lock", tableName: "__mo_account_lock"},
			{dbName: "issue_26121_delete_auto", tableName: "mo_increment_columns"},
		}
		cleanupDBs := []string{branchDB, cloneDB, sourceDB}
		for _, tc := range deleteCases {
			cleanupDBs = append(cleanupDBs, tc.dbName)
		}
		for _, name := range cleanupDBs {
			execSQLMaybe(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", name))
		}
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			for _, name := range cleanupDBs {
				execSQLMaybe(t, cleanupCtx, db, fmt.Sprintf("drop database if exists `%s`", name))
			}
		}()

		t.Run("database copies preserve ordinary tables", func(t *testing.T) {
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

			execSQLRequire(t, ctx, db, "data branch create database `"+branchDB+"` from `"+sourceDB+"`")
			execSQLRequire(t, ctx, db, "create database `"+cloneDB+"` clone `"+sourceDB+"`")

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

		t.Run("delete validation sees ordinary tables", func(t *testing.T) {
			for _, tc := range deleteCases {
				t.Run(tc.tableName, func(t *testing.T) {
					execSQLRequire(t, ctx, db, "create database `"+tc.dbName+"`")
					execSQLRequire(t, ctx, db, "create table `"+tc.dbName+"`.`base` (id int primary key)")
					execSQLRequire(t, ctx, db, "insert into `"+tc.dbName+"`.`base` values (1)")
					execSQLRequire(t, ctx, db, "data branch create table `"+tc.dbName+"`.`branch_t` from `"+tc.dbName+"`.`base`")
					execSQLRequire(t, ctx, db, "drop table `"+tc.dbName+"`.`base`")
					execSQLRequire(t, ctx, db, "create table `"+tc.dbName+"`.`"+tc.tableName+"` (id int primary key)")
					execSQLRequire(t, ctx, db, "insert into `"+tc.dbName+"`.`"+tc.tableName+"` values (1)")

					_, err = db.ExecContext(ctx, "data branch delete database `"+tc.dbName+"`")
					require.Error(t, err)
					require.Contains(t, err.Error(), "is not an active branch table")

					var count int
					require.NoError(t, db.QueryRowContext(ctx,
						"select count(*) from `"+tc.dbName+"`.`"+tc.tableName+"`").Scan(&count))
					require.Equal(t, 1, count)
				})
			}
		})
	})
}
