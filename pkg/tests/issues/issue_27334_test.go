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

func TestIssue27334UpdateAffectedRowsHonorsClientFoundRows(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		dbName := testutils.GetDatabaseName(t)

		openConn := func(clientFoundRows bool) (*sql.DB, *sql.Conn) {
			t.Helper()
			dsn := fmt.Sprintf(
				"dump:111@tcp(127.0.0.1:%d)/?clientFoundRows=%t&interpolateParams=false",
				port, clientFoundRows)
			db, err := sql.Open("mysql", dsn)
			require.NoError(t, err)
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			return db, conn
		}

		changedDB, changedConn := openConn(false)
		defer changedDB.Close()
		defer changedConn.Close()
		mustExec(t, ctx, changedConn, fmt.Sprintf("create database `%s`", dbName))
		mustExec(t, ctx, changedConn, fmt.Sprintf("use `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = changedDB.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()
		mustExec(t, ctx, changedConn, "create table t (id int primary key, v int, nullable int)")
		mustExec(t, ctx, changedConn, "insert into t values (1,10,null),(2,10,null)")

		foundDB, foundConn := openConn(true)
		defer foundDB.Close()
		defer foundConn.Close()
		mustExec(t, ctx, foundConn, fmt.Sprintf("use `%s`", dbName))

		assertAffected := func(conn *sql.Conn, query string, expected int64, args ...any) {
			t.Helper()
			result, err := conn.ExecContext(ctx, query, args...)
			require.NoError(t, err)
			actual, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		}

		assertAffected(changedConn, "update t set v = 10 where id = 1", 0)
		assertAffected(foundConn, "update t set v = 10 where id = 1", 1)
		assertAffected(changedConn, "update t set v = 11 where id in (1, 2)", 2)
		assertAffected(changedConn, "update t set v = 11 where id in (1, 2)", 0)
		assertAffected(changedConn, "update t set v = if(id = 1, 12, 11) where id in (1, 2)", 1)
		assertAffected(changedConn, "update t set nullable = null where id = 1", 0)
		assertAffected(changedConn, "update t set nullable = 1 where id = 1", 1)
		mustExec(t, ctx, changedConn, "create table geo_t (id int primary key, g geometry)")
		mustExec(t, ctx, changedConn, "insert into geo_t values (1, st_geomfromtext('point(1 2)'))")
		assertAffected(changedConn, "update geo_t set g = g where id = 1", 0)

		mustExec(t, ctx, changedConn, "create table string_t (id int primary key, c char(4), v varchar(4))")
		mustExec(t, ctx, changedConn, "insert into string_t values (1, 'a', 'a')")
		assertAffected(changedConn, "update string_t set c = 'a   ' where id = 1", 0)
		assertAffected(foundConn, "update string_t set c = 'a   ' where id = 1", 1)
		assertAffected(changedConn, "update string_t set v = 'a ' where id = 1", 1)
		assertAffected(foundConn, "update string_t set v = 'a ' where id = 1", 1)

		mustExec(t, ctx, changedConn, "create table target_a (id int primary key, k int, v int)")
		mustExec(t, ctx, changedConn, "create table target_b (id int primary key, k int, v int)")
		mustExec(t, ctx, changedConn, "insert into target_a values (1,1,10)")
		mustExec(t, ctx, changedConn, "insert into target_b values (1,1,20),(2,1,20)")
		assertAffected(changedConn,
			"update target_a a, target_b b set a.v = 11, b.v = b.v where a.k = b.k", 1)

		stmt, err := changedConn.PrepareContext(ctx, "update t set v = ? where id = ?")
		require.NoError(t, err)
		defer stmt.Close()
		result, err := stmt.ExecContext(ctx, "12", 1)
		require.NoError(t, err)
		affected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Zero(t, affected)
		result, err = stmt.ExecContext(ctx, "13", 1)
		require.NoError(t, err)
		affected, err = result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), affected)
	})
}
