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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
)

func TestIssue26678MaxExecutionTime(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()

		timedConn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer timedConn.Close()
		defaultConn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer defaultConn.Close()

		_, err = timedConn.ExecContext(ctx, "set @@session.max_execution_time=100")
		require.NoError(t, err)
		var timeout int64
		require.NoError(t, timedConn.QueryRowContext(
			ctx,
			"select @@session.max_execution_time",
		).Scan(&timeout))
		require.Equal(t, int64(100), timeout)
		require.NoError(t, defaultConn.QueryRowContext(
			ctx,
			"select @@session.max_execution_time",
		).Scan(&timeout))
		require.Zero(t, timeout, "max_execution_time must remain session scoped")

		started := time.Now()
		var slept int
		err = timedConn.QueryRowContext(ctx, "select sleep(2)").Scan(&slept)
		requireQueryTimeout(t, err)
		require.Less(t, time.Since(started), time.Second,
			"max_execution_time did not stop SELECT near its deadline")

		// A statement timeout must not cancel the session or its explicit
		// transaction. MatrixOne rolls back only the failed statement here.
		require.NoError(t, timedConn.QueryRowContext(ctx, "select 1").Scan(&slept))
		require.Equal(t, 1, slept)
		_, err = timedConn.ExecContext(ctx, "begin")
		require.NoError(t, err)
		err = timedConn.QueryRowContext(ctx, "select sleep(2)").Scan(&slept)
		requireQueryTimeout(t, err)
		require.NoError(t, timedConn.QueryRowContext(ctx, "select 1").Scan(&slept))
		require.Equal(t, 1, slept)
		_, err = timedConn.ExecContext(ctx, "rollback")
		require.NoError(t, err)

		// COM_STMT_EXECUTE follows the prepared SELECT, not the outer EXECUTE
		// command, when deciding whether the timeout applies.
		prepared, err := timedConn.PrepareContext(ctx, "select sleep(?)")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, prepared.Close())
		}()
		err = prepared.QueryRowContext(ctx, 2).Scan(&slept)
		requireQueryTimeout(t, err)

		_, err = timedConn.ExecContext(ctx, "set @@session.max_execution_time=0")
		require.NoError(t, err)
		require.NoError(t, timedConn.QueryRowContext(ctx, "select sleep(0.05)").Scan(&slept))
		require.Zero(t, slept)
	})
}

func requireQueryTimeout(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var mysqlErr *mysql.MySQLError
	require.True(t, errors.As(err, &mysqlErr), "expected MySQL error, got %T: %v", err, err)
	require.Equal(t, uint16(moerr.ER_QUERY_TIMEOUT), mysqlErr.Number)
	require.Equal(t, [5]byte{'H', 'Y', '0', '0', '0'}, mysqlErr.SQLState)
}
