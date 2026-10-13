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

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue26874BinaryPreparedStringAssignmentRecoversAfterNull(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port)
		db, err := sql.Open("mysql", dsn)
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
		mustExec(t, ctx, conn, `create table nullable_strings(
			id int primary key,
			c char(20),
			v varchar(20),
			t tinytext,
			plain text)`)

		insertStmt, err := conn.PrepareContext(ctx,
			"insert into nullable_strings values (?, ?, ?, ?, ?)")
		require.NoError(t, err)
		defer insertStmt.Close()
		for _, args := range [][]any{
			{1, "before-c", "before-v", "before-t", "before-text"},
			{2, nil, nil, nil, nil},
			{3, "after-c", "after-v", "after-t", "after-text"},
			{4, int64(7), float64(8), true, "control-text"},
		} {
			_, err = insertStmt.ExecContext(ctx, args...)
			require.NoError(t, err, "prepared insert args %v", args)
		}
		requireIssue26874Strings(t, ctx, conn, 3,
			"after-c", "after-v", "after-t", "after-text")
		requireIssue26874Strings(t, ctx, conn, 4,
			"7", "8", "1", "control-text")

		mustExec(t, ctx, conn, "insert into nullable_strings values (10, 'seed', 'seed', 'seed', 'seed')")
		updateStmt, err := conn.PrepareContext(ctx,
			"update nullable_strings set c=?, v=?, t=?, plain=? where id=10")
		require.NoError(t, err)
		defer updateStmt.Close()
		for _, args := range [][]any{
			{"first-c", "first-v", "first-t", "first-text"},
			{nil, nil, nil, nil},
			{"last-c", "last-v", "last-t", "last-text"},
		} {
			_, err = updateStmt.ExecContext(ctx, args...)
			require.NoError(t, err, "prepared update args %v", args)
		}
		requireIssue26874Strings(t, ctx, conn, 10,
			"last-c", "last-v", "last-t", "last-text")

		mustExec(t, ctx, conn, "create table required_string(id int primary key, v varchar(4) not null)")
		mustExec(t, ctx, conn, "insert into required_string values (1, 'seed')")
		requiredStmt, err := conn.PrepareContext(ctx, "update required_string set v=? where id=1")
		require.NoError(t, err)
		defer requiredStmt.Close()
		_, err = requiredStmt.ExecContext(ctx, nil)
		requireIssue26874MySQLError(t, err, 3819)
		_, err = requiredStmt.ExecContext(ctx, "okay")
		require.NoError(t, err)
		var required string
		require.NoError(t, conn.QueryRowContext(ctx, "select v from required_string where id=1").Scan(&required))
		require.Equal(t, "okay", required)

		_, err = requiredStmt.ExecContext(ctx, "toolong")
		requireIssue26874MySQLError(t, err, 1406)
		_, err = requiredStmt.ExecContext(ctx, "done")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, "select v from required_string where id=1").Scan(&required))
		require.Equal(t, "done", required)
	})
}

func requireIssue26874Strings(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	id int,
	wantChar, wantVarchar, wantTinyText, wantText string,
) {
	t.Helper()
	var gotChar, gotVarchar, gotTinyText, gotText string
	err := conn.QueryRowContext(ctx,
		"select c, v, t, plain from nullable_strings where id=?", id).
		Scan(&gotChar, &gotVarchar, &gotTinyText, &gotText)
	require.NoError(t, err)
	require.Equal(t, []string{wantChar, wantVarchar, wantTinyText, wantText},
		[]string{gotChar, gotVarchar, gotTinyText, gotText})
}

func requireIssue26874MySQLError(t *testing.T, err error, number uint16) {
	t.Helper()
	require.Error(t, err)
	var mysqlErr *mysqlDriver.MySQLError
	require.ErrorAs(t, err, &mysqlErr)
	require.Equal(t, number, mysqlErr.Number)
}
