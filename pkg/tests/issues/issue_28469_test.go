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
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

// TestIssue28469BinaryPreparedIntegerAssignment verifies that FLOAT parameters
// retain ties-to-even semantics beneath numeric expressions and that negative
// FLOAT values reach the unsigned assignment boundary without wrapping.
func TestIssue28469BinaryPreparedIntegerAssignment(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
			cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()

		mustExec(t, ctx, conn, "create table t_abs (value bigint)")
		mustExec(t, ctx, conn, "create table t_add (value bigint)")
		for _, tc := range []struct {
			query string
			value int64
		}{
			{query: "insert into t_abs values (abs(?))", value: 2},
			{query: "insert into t_add values (? + 0)", value: -2},
		} {
			stmt, prepareErr := conn.PrepareContext(ctx, tc.query)
			require.NoError(t, prepareErr)
			defer stmt.Close()
			_, execErr := stmt.ExecContext(ctx, float64(-2.5))
			require.NoError(t, execErr)
		}
		var value int64
		require.NoError(t, conn.QueryRowContext(ctx, "select value from t_abs").Scan(&value))
		require.Equal(t, int64(2), value)
		require.NoError(t, conn.QueryRowContext(ctx, "select value from t_add").Scan(&value))
		require.Equal(t, int64(-2), value)

		mustExec(t, ctx, conn, "set sql_mode='STRICT_TRANS_TABLES'")
		mustExec(t, ctx, conn, "create table u (value bigint unsigned)")
		strictStmt, err := conn.PrepareContext(ctx, "insert into u values (?)")
		require.NoError(t, err)
		defer strictStmt.Close()
		_, err = strictStmt.ExecContext(ctx, float64(-1))
		require.Error(t, err)
		var count int64
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from u").Scan(&count))
		require.Zero(t, count)
	})
}
