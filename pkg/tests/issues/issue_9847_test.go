// Copyright 2021 - 2024 Matrix Origin
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

// Issue 9847: currval should reflect nextval used in non-SELECT statements.
func TestIssue9847CurrvalAfterInsert(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			port := cn1.GetServiceConfig().CN.Frontend.Port
			dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port)
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
				mustExec(t, ctx, conn, "use mo_catalog")
				mustExec(t, ctx, conn, fmt.Sprintf("drop database if exists `%s`", dbName))
			}()

			mustExec(t, ctx, conn, "create table if not exists seq_table_01(col1 int)")
			mustExec(t, ctx, conn, "drop sequence if exists seq_14")
			mustExec(t, ctx, conn, "create sequence seq_14 increment 50 start with 126 no cycle")

			mustExec(t, ctx, conn, "select nextval('seq_14')")

			var nextv, currv string
			row := conn.QueryRowContext(ctx, "select nextval('seq_14'), currval('seq_14')")
			require.NoError(t, row.Scan(&nextv, &currv))
			require.Equal(t, "176", currv)

			mustExec(t, ctx, conn, "insert into seq_table_01 select nextval('seq_14')")

			row = conn.QueryRowContext(ctx, "select currval('seq_14')")
			require.NoError(t, row.Scan(&currv))
			require.Equal(t, "226", currv)

			mustExec(t, ctx, conn, "insert into seq_table_01 values(nextval('seq_14'))")
			mustExec(t, ctx, conn, "insert into seq_table_01 values(nextval('seq_14'))")
			mustExec(t, ctx, conn, "insert into seq_table_01 values(nextval('seq_14'))")
			mustExec(t, ctx, conn, "insert into seq_table_01 values(nextval('seq_14'))")

			row = conn.QueryRowContext(ctx, "select currval('seq_14')")
			require.NoError(t, row.Scan(&currv))
			require.Equal(t, "426", currv)

			mustExec(t, ctx, conn, "drop sequence seq_14")
			mustExec(t, ctx, conn, "drop table seq_table_01")
		},
	)
}

func mustExec(t *testing.T, ctx context.Context, conn *sql.Conn, stmt string) {
	t.Helper()
	_, err := conn.ExecContext(ctx, stmt)
	require.NoError(t, err, stmt)
}
