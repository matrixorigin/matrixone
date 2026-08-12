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

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
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
		mustExec(t, ctx, conn, "replace into cascade_p values(1, 'first')")
		mustExec(t, ctx, conn, "insert into cascade_c values(10, 1)")
		stmt, err := conn.PrepareContext(ctx, "replace into cascade_p values(?, ?)")
		require.NoError(t, err)
		defer stmt.Close()
		_, err = stmt.ExecContext(ctx, 1, "replaced")
		require.NoError(t, err)
		var count int
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from cascade_c force index(idx_pid) where pid=1").Scan(&count))
		require.Zero(t, count)

		mustExec(t, ctx, conn, "create table setnull_p(id int primary key, note varchar(20))")
		mustExec(t, ctx, conn, `create table setnull_c(
			id int primary key, pid int, index idx_pid(pid),
			foreign key(pid) references setnull_p(id) on delete set null)`)
		mustExec(t, ctx, conn, "replace into setnull_p values(1, 'first')")
		mustExec(t, ctx, conn, "insert into setnull_c values(10, 1)")
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
	})
}
