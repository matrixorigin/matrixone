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

func TestIssue26875ReplaceMaintainsIndexedForeignKeyChildren(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		cn2, err := c.GetCNService(1)
		require.NoError(t, err)
		db2, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", cn2.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db2.Close()
		conn2, err := db2.Conn(ctx)
		require.NoError(t, err)
		defer conn2.Close()

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
		mustExec(t, ctx, conn2, fmt.Sprintf("use `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			mustExec(t, cleanupCtx, conn, "use mo_catalog")
			mustExec(t, cleanupCtx, conn, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()

		mustExec(t, ctx, conn, "create table cascade_p(id int primary key, note varchar(20))")
		mustExec(t, ctx, conn, `create table cascade_c(
			id int primary key, pid int, index idx_pid(pid),
			foreign key(pid) references cascade_p(id) on delete cascade)`)
		mustExec(t, ctx, conn, "replace into cascade_p values(1, 'first'),(2, 'unaffected')")
		mustExec(t, ctx, conn, "insert into cascade_c values(10, 1),(20, 2)")
		stmt, err := conn.PrepareContext(ctx, "replace into cascade_p values(?, ?)")
		require.NoError(t, err)
		defer stmt.Close()
		_, err = stmt.ExecContext(ctx, 1, "replaced")
		require.NoError(t, err)
		var count int
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from cascade_c force index(idx_pid) where pid=1").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from cascade_c force index(idx_pid) where pid=2 and id=20").Scan(&count))
		require.Equal(t, 1, count)

		mustExec(t, ctx, conn, "create table setnull_p(id int primary key, note varchar(20))")
		mustExec(t, ctx, conn, `create table setnull_c(
			id int primary key, pid int, index idx_pid(pid),
			foreign key(pid) references setnull_p(id) on delete set null)`)
		mustExec(t, ctx, conn, "replace into setnull_p values(1, 'first'),(2, 'unaffected')")
		mustExec(t, ctx, conn, "insert into setnull_c values(10, 1),(20, 2)")
		setNullStmt, err := conn.PrepareContext(ctx, "replace into setnull_p values(?, ?)")
		require.NoError(t, err)
		defer setNullStmt.Close()
		_, err = setNullStmt.ExecContext(ctx, 1, "replaced")
		require.NoError(t, err)
		var pid sql.NullInt64
		require.NoError(t, conn.QueryRowContext(ctx, "select pid from setnull_c where id=10").Scan(&pid))
		require.False(t, pid.Valid)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from setnull_c force index(idx_pid) where pid=1").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from setnull_c force index(idx_pid) where pid=2 and id=20").Scan(&count))
		require.Equal(t, 1, count)
		mustExec(t, ctx, conn, "insert into setnull_p values(5, 'select target'),(6, 'select unaffected')")
		mustExec(t, ctx, conn, "insert into setnull_c values(50, 5),(60, 6)")
		mustExec(t, ctx, conn, "replace into setnull_p select 5, 'selected replacement'")
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from setnull_c force index(idx_pid) where pid=6 and id=60").Scan(&count))
		require.Equal(t, 1, count)

		mustExec(t, ctx, conn, "create table unique_cascade_p(id int primary key, note varchar(20))")
		mustExec(t, ctx, conn, `create table unique_cascade_c(
			id int primary key, pid int, unique key uk_pid(pid),
			foreign key(pid) references unique_cascade_p(id) on delete cascade)`)
		mustExec(t, ctx, conn, "insert into unique_cascade_p values(1, 'first'),(2, 'empty'),(3, 'unaffected')")
		mustExec(t, ctx, conn, "insert into unique_cascade_c values(10, 1),(11, null),(13, 3)")
		uniqueCascadeStmt, err := conn2.PrepareContext(ctx, "replace into unique_cascade_p values(?, ?)")
		require.NoError(t, err)
		defer uniqueCascadeStmt.Close()
		_, err = uniqueCascadeStmt.ExecContext(ctx, 1, "replaced")
		require.NoError(t, err)
		_, err = uniqueCascadeStmt.ExecContext(ctx, 2, "still empty")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from unique_cascade_c where id=10").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from unique_cascade_c where id=11 and pid is null").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from unique_cascade_c force index(uk_pid) where pid=3 and id=13").Scan(&count))
		require.Equal(t, 1, count)
		_, err = conn.ExecContext(ctx, "insert into unique_cascade_c values(14, 3)")
		require.Error(t, err)

		mustExec(t, ctx, conn, "create table unique_setnull_p(id int primary key, note varchar(20))")
		mustExec(t, ctx, conn, `create table unique_setnull_c(
			id int primary key, pid int, marker int,
			unique key uk_pid_marker(pid, marker),
			foreign key(pid) references unique_setnull_p(id) on delete set null)`)
		mustExec(t, ctx, conn, "insert into unique_setnull_p values(1, 'first'),(2, 'delete'),(3, 'empty'),(4, 'unaffected')")
		mustExec(t, ctx, conn, "insert into unique_setnull_c values(10, 1, 7),(11, 2, 8),(12, null, 9),(14, 4, 10)")
		uniqueSetNullStmt, err := conn2.PrepareContext(ctx, "replace into unique_setnull_p values(?, ?)")
		require.NoError(t, err)
		defer uniqueSetNullStmt.Close()
		_, err = uniqueSetNullStmt.ExecContext(ctx, 1, "replaced")
		require.NoError(t, err)
		_, err = uniqueSetNullStmt.ExecContext(ctx, 3, "still empty")
		require.NoError(t, err)
		mustExec(t, ctx, conn, "delete from unique_setnull_p where id=2")
		for _, childID := range []int{10, 11, 12} {
			require.NoError(t, conn.QueryRowContext(ctx,
				"select count(*) from unique_setnull_c where id=? and pid is null", childID).Scan(&count))
			require.Equal(t, 1, count)
		}
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from unique_setnull_c force index(uk_pid_marker) where pid in (1,2)").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from unique_setnull_c force index(uk_pid_marker) where pid=4 and marker=10").Scan(&count))
		require.Equal(t, 1, count)
		_, err = conn.ExecContext(ctx, "insert into unique_setnull_c values(15, 4, 10)")
		require.Error(t, err)

		for _, tc := range []struct {
			tableName string
			indexDDL  string
		}{
			{tableName: "self_setnull_sk", indexDDL: "index idx_pid(pid)"},
			{tableName: "self_setnull_uk", indexDDL: "unique key uk_pid(pid)"},
		} {
			mustExec(t, ctx, conn, fmt.Sprintf(`create table %s(
				id int primary key, pid int, %s,
				foreign key(pid) references %s(id) on delete set null)`, tc.tableName, tc.indexDDL, tc.tableName))
			mustExec(t, ctx, conn, fmt.Sprintf("insert into %s values(1,null),(2,1)", tc.tableName))
			selfStmt, prepareErr := conn2.PrepareContext(ctx, fmt.Sprintf("replace into %s values(?,?)", tc.tableName))
			require.NoError(t, prepareErr)
			defer selfStmt.Close()
			_, execErr := selfStmt.ExecContext(ctx, 1, nil)
			require.NoError(t, execErr)
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select count(*) from %s where id=2 and pid is null", tc.tableName)).Scan(&count))
			require.Equal(t, 1, count)
		}
	})
}
