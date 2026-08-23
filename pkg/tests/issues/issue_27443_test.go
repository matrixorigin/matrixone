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

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

// TestIssue27443BinaryPreparedDMLAndAggregate exercises the same COM_STMT_PREPARE
// and COM_STMT_EXECUTE path as database/sql clients.  Runtime specialization
// must preserve DML execution metadata and must not feed an integer literal to
// an aggregate executor that was bound to a floating-point parameter domain.
func TestIssue27443BinaryPreparedDMLAndAggregate(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		dbName := testutils.GetDatabaseName(t)
		execSQLRequire(t, ctx, db, "create database `"+dbName+"`")
		defer execSQLMaybe(t, ctx, db, "drop database if exists `"+dbName+"`")

		execSQLRequire(t, ctx, db, "create table `"+dbName+"`.src (id int primary key, tenant int, payload varchar(32), amount decimal(12,2))")
		execSQLRequire(t, ctx, db, "create table `"+dbName+"`.dst (id int primary key, tenant int, payload varchar(32), amount decimal(12,2))")
		execSQLRequire(t, ctx, db, "insert into `"+dbName+"`.src values (1, 7, 'new', 12.34), (2, 8, 'other', 56.78)")
		execSQLRequire(t, ctx, db, "insert into `"+dbName+"`.dst values (1, 7, 'old', 1.00)")

		replaceStmt, err := db.PrepareContext(ctx, "replace into `"+dbName+"`.dst (id, tenant, payload, amount) select id, tenant, payload, amount from `"+dbName+"`.src where tenant = ?")
		require.NoError(t, err)
		_, err = replaceStmt.ExecContext(ctx, int64(7))
		require.NoError(t, err)
		require.NoError(t, replaceStmt.Close())

		var payload string
		var amount string
		require.NoError(t, db.QueryRowContext(ctx, "select payload, amount from `"+dbName+"`.dst where id = 1").Scan(&payload, &amount))
		require.Equal(t, "new", payload)
		require.Equal(t, "12.34", amount)

		updateStmt, err := db.PrepareContext(ctx, "update `"+dbName+"`.dst set payload = ?, amount = ? where tenant = ? and id = ?")
		require.NoError(t, err)
		_, err = updateStmt.ExecContext(ctx, "updated", "23.45", int64(7), int64(1))
		require.NoError(t, err)
		require.NoError(t, updateStmt.Close())
		require.NoError(t, db.QueryRowContext(ctx, "select payload, amount from `"+dbName+"`.dst where id = 1").Scan(&payload, &amount))
		require.Equal(t, "updated", payload)
		require.Equal(t, "23.45", amount)

		execSQLRequire(t, ctx, db, "create table `"+dbName+"`.pp_dst (tenant int, id int, status int, amount decimal(12,2), dt datetime, tm time, bin varbinary(16), u bigint unsigned, primary key (tenant, id))")
		execSQLRequire(t, ctx, db, "insert into `"+dbName+"`.pp_dst values (1, 1, 999, 1.00, '2026-08-21 01:02:03', '01:02:03', x'6131', 7)")
		ppStmt, err := db.PrepareContext(ctx, "update `"+dbName+"`.pp_dst set status=?, amount=?, dt=?, tm=?, bin=?, u=? where tenant=? and id=?")
		require.NoError(t, err)
		_, err = ppStmt.ExecContext(ctx, int64(200), "2.50", "2026-08-22 04:05:06", "04:05:06", []byte("b2"), uint64(42), int64(1), int64(1))
		require.NoError(t, err)
		require.NoError(t, ppStmt.Close())
		var status int64
		var dt, tm string
		var bin []byte
		var unsigned uint64
		require.NoError(t, db.QueryRowContext(ctx, "select status, amount, dt, tm, bin, u from `"+dbName+"`.pp_dst where tenant=1 and id=1").Scan(&status, &amount, &dt, &tm, &bin, &unsigned))
		require.Equal(t, int64(200), status)
		require.Equal(t, "2.50", amount)
		require.Equal(t, "2026-08-22 04:05:06", dt)
		require.Equal(t, "04:05:06", tm)
		require.Equal(t, []byte("b2"), bin)
		require.Equal(t, uint64(42), unsigned)

		sumStmt, err := db.PrepareContext(ctx, "select sum(?)")
		require.NoError(t, err)
		var sum int64
		require.NoError(t, sumStmt.QueryRowContext(ctx, int64(7)).Scan(&sum))
		require.Equal(t, int64(7), sum)
		require.NoError(t, sumStmt.Close())
	})
}
