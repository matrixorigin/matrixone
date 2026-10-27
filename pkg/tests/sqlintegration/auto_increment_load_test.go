// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestLoadAutoIncrementObservedDemand(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists auto_increment_load")
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(q string) sql.Result {
			t.Helper()
			r, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
			return r
		}
		number := func(q string) uint64 {
			t.Helper()
			var n uint64
			require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&n), q)
			return n
		}
		file := func(name, data string) string {
			t.Helper()
			p := filepath.Join(t.TempDir(), name)
			require.NoError(t, os.WriteFile(p, []byte(data), 0600))
			return p
		}
		load := func(path, table, columns string) sql.Result {
			return exec(fmt.Sprintf("load data infile '%s' into table %s fields terminated by ',' %s", strings.ReplaceAll(path, "'", "''"), table, columns))
		}
		exec("create database auto_increment_load")
		exec("use auto_increment_load")
		exec("create table generated(id bigint unsigned auto_increment primary key,v int)")
		// Four pipeline batches cross the default cache size and exercise learned
		// demand through the real LOAD consumer without a throughput assertion.
		const n = 32768
		load(file("bulk.csv", strings.Repeat("7\n", n)), "generated", "(v)")
		require.Equal(t, uint64(n), number("select count(*) from generated"))
		require.Equal(t, uint64(1), number("select min(id) from generated"))
		require.Equal(t, uint64(n), number("select max(id) from generated"))
		// LOAD preserves the session baseline (frontend status_stmt.go); INSERT
		// below exercises the first-generated-ID protocol result.
		require.Zero(t, number("select last_insert_id()"))
		require.Equal(t, uint64(n+1), number("select auto_increment from information_schema.tables where table_schema=database() and table_name='generated'"))
		result := exec("insert into generated(v) values(8),(9)")
		first, err := result.LastInsertId()
		require.NoError(t, err)
		require.Equal(t, int64(n+1), first)
		result = exec("insert into generated values(NULL,10),(1000000,11),(NULL,12)")
		first, err = result.LastInsertId()
		require.NoError(t, err)
		require.Equal(t, int64(n+3), first)
		require.Equal(t, uint64(1000001), number("select id from generated where v=12"))
		require.Equal(t, uint64(1000002), number("select auto_increment from information_schema.tables where table_schema=database() and table_name='generated'"))
		small := file("small.csv", "77\n77\n")
		exec("begin")
		load(small, "generated", "(v)")
		require.Equal(t, uint64(2), number("select count(*) from generated where v=77"))
		exec("rollback")
		require.Zero(t, number("select count(*) from generated where v=77"))
		exec("insert into generated(v) values(88)")
		require.Greater(t, number("select id from generated where v=88"), uint64(1000001))
		require.Equal(t, uint64(n+6), number("select count(*) from generated"))

		exec("create table explicit_ids(id bigint auto_increment primary key,v int)")
		previous := number("select last_insert_id()")
		load(file("explicit.csv", "42,1\n43,2\n"), "explicit_ids", "")
		require.Equal(t, uint64(2), number("select count(*) from explicit_ids where id in(42,43)"))
		require.Equal(t, previous, number("select last_insert_id()"), "explicit values do not publish a generated ID")

		exec("set auto_increment_increment=4")
		exec("set auto_increment_offset=3")
		exec("create table series(id bigint auto_increment primary key,v int)")
		load(file("series.csv", "1\n2\n3\n"), "series", "(v)")
		require.Equal(t, uint64(3), number("select count(*) from series where id in(3,7,11)"))
		require.Equal(t, previous, number("select last_insert_id()"))
		exec("set auto_increment_increment=1")
		exec("set auto_increment_offset=1")

		// A private CREATE uses its actual txnOp, not a nil-transaction cache flag.
		for _, action := range []string{"commit", "rollback"} {
			table := "private_" + action
			exec("begin")
			exec("create table " + table + "(id bigint auto_increment primary key,v int)")
			load(small, table, "(v)")
			require.Equal(t, uint64(2), number("select count(*) from "+table+" where id in(1,2)"))
			exec(action)
			if action == "rollback" {
				require.Zero(t, number("select count(*) from information_schema.tables where table_schema=database() and table_name='"+table+"'"))
				exec("create table " + table + "(id bigint auto_increment primary key,v int)")
				load(small, table, "(v)")
			}
			require.Equal(t, uint64(2), number("select count(*) from "+table+" where id in(1,2)"))
		}
	})
}
