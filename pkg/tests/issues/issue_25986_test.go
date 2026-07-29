// Copyright 2021 - 2026 Matrix Origin
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

// A binary COM_STMT_EXECUTE wrapper borrows the AST retained by PrepareStmt.
// A schema-refresh error must not return that shared AST to the parser pool,
// because later executions of the same prepared handle still need it.
func TestIssue25986BinaryExecuteReplanErrorKeepsPreparedAST(t *testing.T) {
	embed.RunBaseClusterTests(
		t,
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
			defer cancel()

			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			port := cn.GetServiceConfig().CN.Frontend.Port
			dsn := fmt.Sprintf(
				"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
				port,
			)
			db, err := sql.Open("mysql", dsn)
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

			mustExec(t, ctx, conn, "create table t (id int)")
			stmt, err := conn.PrepareContext(ctx, "alter table t add column note int")
			require.NoError(t, err)
			defer stmt.Close()

			_, err = stmt.ExecContext(ctx)
			require.NoError(t, err)

			for i := 0; i < 2; i++ {
				_, err = stmt.ExecContext(ctx)
				require.ErrorContains(t, err, "Duplicate column name 'note'")
				require.NotContains(t, err.Error(), "panic")
			}
		},
	)
}
