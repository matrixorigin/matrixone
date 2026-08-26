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

	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue27591BinaryPreparedDatalinkValidation(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
		mustExec(t, ctx, conn, "create table dl (id int primary key, value datalink)")

		_, textErr := conn.ExecContext(ctx, "insert into dl values (1, 'not-a-datalink')")
		require.Error(t, textErr)
		var textMySQLErr *mysql.MySQLError
		require.ErrorAs(t, textErr, &textMySQLErr)

		stmt, err := conn.PrepareContext(ctx, "insert into dl values (?, ?)")
		require.NoError(t, err)
		defer stmt.Close()

		_, binaryErr := stmt.ExecContext(ctx, int64(2), "not-a-datalink")
		require.Error(t, binaryErr)
		var binaryMySQLErr *mysql.MySQLError
		require.ErrorAs(t, binaryErr, &binaryMySQLErr)
		require.Equal(t, textMySQLErr.Number, binaryMySQLErr.Number)

		var rows int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from dl").Scan(&rows))
		require.Zero(t, rows)

		const validDatalink = "file:///tmp/issue-27591.txt"
		_, err = stmt.ExecContext(ctx, int64(3), validDatalink)
		require.NoError(t, err)

		var id int
		var value string
		require.NoError(t, conn.QueryRowContext(ctx, "select id, value from dl").Scan(&id, &value))
		require.Equal(t, 3, id)
		require.Equal(t, validDatalink, value)
	})
}
