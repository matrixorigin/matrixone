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
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssues28318And28321CreateTableValidation(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		sourceDB := testutils.GetDatabaseName(t)
		targetDB := sourceDB + "_other"
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		execSQLMaybe(t, cleanupCtx, db, fmt.Sprintf("drop database if exists `%s`", sourceDB))
		execSQLMaybe(t, cleanupCtx, db, fmt.Sprintf("drop database if exists `%s`", targetDB))
		cleanupCancel()
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "rollback")
			execSQLMaybe(t, cleanupCtx, db, fmt.Sprintf("drop database if exists `%s`", sourceDB))
			execSQLMaybe(t, cleanupCtx, db, fmt.Sprintf("drop database if exists `%s`", targetDB))
		}()

		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", sourceDB))
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", targetDB))
		mustExec(t, ctx, conn, fmt.Sprintf("create table `%s`.`parent_a` (id int primary key)", sourceDB))
		mustExec(t, ctx, conn, fmt.Sprintf(
			"create table `%s`.`child` (parent_id int, constraint fk_parent foreign key (parent_id) references `%s`.`parent_a` (id))",
			sourceDB, sourceDB))

		// #28318: the current database is unrelated to the fully-qualified LIKE source.
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", targetDB))
		mustExec(t, ctx, conn, fmt.Sprintf(
			"create table `%s`.`child_copy` like `%s`.`child`", sourceDB, sourceDB))

		var copiedTableName, copiedCreateSQL string
		require.NoError(t, conn.QueryRowContext(ctx,
			"show create table `"+sourceDB+"`.`child_copy`").Scan(&copiedTableName, &copiedCreateSQL))
		require.Equal(t, "child_copy", copiedTableName)
		require.Contains(t, strings.ToUpper(copiedCreateSQL), "FOREIGN KEY")

		var referencedDB, referencedTable string
		require.NoError(t, conn.QueryRowContext(ctx, `
select refer_db_name, refer_table_name
from mo_catalog.mo_foreign_keys
where db_name = ? and table_name = ? and constraint_name = 'fk_parent'`,
			strings.ToLower(sourceDB), "child_copy").Scan(&referencedDB, &referencedTable))
		require.Equal(t, strings.ToLower(sourceDB), referencedDB)
		require.Equal(t, "parent_a", referencedTable)

		// #28321: case variants are duplicate column names and must fail before publish.
		for _, tc := range []struct {
			name          string
			inTransaction bool
		}{
			{name: "autocommit"},
			{name: "transaction", inTransaction: true},
		} {
			tableName := "duplicate_column_case_" + tc.name
			if tc.inTransaction {
				mustExec(t, ctx, conn, "begin")
			}
			_, err = conn.ExecContext(ctx, fmt.Sprintf(
				"create table `%s`.`%s` (`Id` int, `id` int)", targetDB, tableName))
			require.Error(t, err)
			var mysqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &mysqlErr)
			require.Equal(t, uint16(1060), mysqlErr.Number)
			if tc.inTransaction {
				mustExec(t, ctx, conn, "rollback")
			}

			var tableCount int
			require.NoError(t, conn.QueryRowContext(ctx, `
select count(*)
from information_schema.tables
where table_schema = ? and table_name = ?`, targetDB, tableName).Scan(&tableCount))
			require.Zero(t, tableCount)
		}
	})
}
