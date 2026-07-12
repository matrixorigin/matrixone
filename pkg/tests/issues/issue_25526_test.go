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

// Issue 25526: a prepared UPDATE ... JOIN executed through the binary protocol
// (COM_STMT_PREPARE / COM_STMT_EXECUTE) hung on the second execution: the
// cached compile retained stale operator state (hashbuild ctr, dispatch
// channels), leaving scan receivers blocked in waitForRuntimeFilters.
// interpolateParams=false forces the driver onto protocol-level prepared
// statements, and both executions run on the same prepared handle.
func TestIssue25526PreparedUpdateJoinSecondExecute(t *testing.T) {
	embed.RunBaseClusterTests(
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
		},
	)
}
