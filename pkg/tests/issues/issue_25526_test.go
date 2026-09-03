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

// Issues 25526 and 27935 exercise prepared UPDATE ... JOIN through the binary
// protocol (COM_STMT_PREPARE / COM_STMT_EXECUTE). The first regression hung on
// the second execution because cached operator state was stale. The second
// wrapped a positional TIME join result in assignment casts and failed during
// COM_STMT_PREPARE. interpolateParams=false keeps both cases on one real
// server-side prepared handle per statement.
func TestIssue25526And27935PreparedUpdateJoinBinaryProtocol(t *testing.T) {
	embed.RunBaseClusterTests(t,
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*180)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			port := cn1.GetServiceConfig().CN.Frontend.Port
			dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port)
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
				// Best effort: after a hang the session may be wedged, so use a
				// fresh short context and tolerate failure.
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second*10)
				defer cleanupCancel()
				_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
			}()

			mustExec(t, ctx, conn, "create table parent(pid int primary key, code varchar(16) unique)")
			mustExec(t, ctx, conn, `create table acct(
				tenant int not null,
				acct_id int not null,
				status varchar(16),
				amount decimal(12,2),
				parent_id int,
				primary key(tenant, acct_id),
				key idx_status_amount(status, amount),
				key idx_parent_status(parent_id, status),
				constraint fk_acct_parent foreign key(parent_id) references parent(pid))`)
			mustExec(t, ctx, conn, "insert into parent values (1,'p1'),(2,'p2')")
			mustExec(t, ctx, conn, "insert into acct values (1,101,'open',10.50,1),(2,201,'open',30.75,2)")

			stmt, err := conn.PrepareContext(ctx,
				"update acct a join parent p on a.parent_id=p.pid set a.amount=a.amount+?, a.status=? where p.code=? and a.status=?")
			require.NoError(t, err)
			defer stmt.Close()

			for _, args := range [][]any{
				{"3.33", "closed", "p1", "open"},
				{"4.44", "closed", "p2", "open"},
			} {
				execCtx, execCancel := context.WithTimeout(ctx, time.Second*30)
				res, err := stmt.ExecContext(execCtx, args...)
				execCancel()
				require.NoError(t, err, "prepared execute with args %v must not hang", args)
				affected, err := res.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, int64(1), affected)
			}

			rows, err := conn.QueryContext(ctx, "select status, amount from acct order by tenant")
			require.NoError(t, err)
			defer rows.Close()
			var got []string
			for rows.Next() {
				var status, amount string
				require.NoError(t, rows.Scan(&status, &amount))
				got = append(got, status+":"+amount)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []string{"closed:13.83", "closed:35.19"}, got)

			mustExec(t, ctx, conn, "create table temporal_dst(id int primary key, tm time(6))")
			mustExec(t, ctx, conn, "create table temporal_src(k int primary key)")
			mustExec(t, ctx, conn, "insert into temporal_dst values(1, '00:00:01')")
			mustExec(t, ctx, conn, "insert into temporal_src values(10)")
			mustExec(t, ctx, conn, "set session sql_mode='STRICT_TRANS_TABLES'")

			temporalStmt, err := conn.PrepareContext(ctx,
				"update temporal_dst d join temporal_src s on s.k=? set d.tm=? where d.id=?")
			require.NoError(t, err)
			defer temporalStmt.Close()

			executeTemporalUpdate := func(value string) {
				t.Helper()
				res, execErr := temporalStmt.ExecContext(ctx, int64(10), value, int64(1))
				require.NoError(t, execErr)
				affected, affectedErr := res.RowsAffected()
				require.NoError(t, affectedErr)
				require.Equal(t, int64(1), affected)
			}
			readTemporalValue := func() string {
				t.Helper()
				var value string
				require.NoError(t, conn.QueryRowContext(ctx,
					"select cast(tm as char) from temporal_dst where id=1").Scan(&value))
				return value
			}

			executeTemporalUpdate("02:03:04.000005")
			_, err = temporalStmt.ExecContext(ctx, int64(10), "838:59:59.000001", int64(1))
			require.ErrorContains(t, err, "data out of range")
			require.Equal(t, "02:03:04.000005", readTemporalValue(), "failed assignment must leave the row unchanged")

			executeTemporalUpdate("03:04:05.000006")
			require.Equal(t, "03:04:05.000006", readTemporalValue(),
				"the prepared handle must remain reusable after an assignment error")
		},
	)
}
