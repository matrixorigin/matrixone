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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Two real CNs distinguish durable table policy from CREATE-local state. Reuse
// the package's base cluster; all tables contain at most four rows.
func TestAutoIDCachePublicLifecycle(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		var conns []*sql.Conn
		for i := range 2 {
			cn, err := c.GetCNService(i)
			require.NoError(t, err)
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer db.Close()
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()
			conns = append(conns, conn)
		}
		exec := func(conn *sql.Conn, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(t, err, "SQL: %s", statement)
		}
		number := func(conn *sql.Conn, statement string) int64 {
			t.Helper()
			var v int64
			require.NoErrorf(t, conn.QueryRowContext(ctx, statement).Scan(&v), "SQL: %s", statement)
			return v
		}
		showCache := func(conn *sql.Conn, table string) {
			t.Helper()
			var name, ddl string
			require.NoError(t, conn.QueryRowContext(ctx, "show create table "+table).Scan(&name, &ddl))
			require.Contains(t, ddl, "AUTO_ID_CACHE=1")
		}
		dbName := testutils.GetDatabaseName(t)
		exec(conns[0], "create database `"+dbName+"`")
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			_, err := conns[0].ExecContext(cleanup, "drop database `"+dbName+"`")
			require.NoError(t, err)
			_, err = conns[0].ExecContext(cleanup, "drop stage if exists ai_cache_dump_stage")
			require.NoError(t, err)
		}()
		for _, conn := range conns {
			exec(conn, "use `"+dbName+"`")
		}
		exec(conns[0], "create table ai_cache(id bigint auto_increment primary key, v int) auto_increment=10 auto_id_cache=1")
		showCache(conns[1], "ai_cache")
		current := "select internal_auto_increment(database(),'ai_cache')"
		require.Equal(t, int64(10), number(conns[1], current))
		require.Equal(t, int64(10), number(conns[1], current), "observation must not reserve an ID")
		exec(conns[1], "insert into ai_cache(v) values (1),(2)")
		require.Equal(t, int64(10), number(conns[1], "select last_insert_id()"))
		require.Equal(t, int64(12), number(conns[0], current))
		exec(conns[0], "insert into ai_cache(v) values (3)")
		require.Equal(t, int64(12), number(conns[0], "select last_insert_id()"))

		exec(conns[0], "create table ai_like like ai_cache")
		showCache(conns[1], "ai_like")
		exec(conns[1], "insert into ai_like(v) values (1)")
		require.Equal(t, int64(1), number(conns[1], "select id from ai_like"))

		exec(conns[0], "alter table ai_cache auto_increment=100")
		showCache(conns[1], "ai_cache")
		exec(conns[1], "insert into ai_cache(v) values (4)")
		require.Equal(t, int64(100), number(conns[1], "select id from ai_cache where v=4"))
		exec(conns[0], "alter table ai_cache add column extra int, algorithm=copy")
		showCache(conns[1], "ai_cache")
		exec(conns[0], "alter table ai_cache rename to ai_renamed")
		showCache(conns[1], "ai_renamed")
		exec(conns[0], "alter table ai_renamed rename to ai_cache")
		showCache(conns[1], "ai_cache")
		exec(conns[0], "truncate table ai_cache")
		showCache(conns[1], "ai_cache")
		exec(conns[1], "insert into ai_cache(v) values (1)")
		require.Equal(t, int64(1), number(conns[1], "select id from ai_cache"))

		// Dump/load goes through the public stage/object path and restores the
		// sequence independently of the CREATE-local allocator cache.
		exec(conns[0], "create stage ai_cache_dump_stage url='file://"+filepath.ToSlash(t.TempDir())+"/'")
		exec(conns[0], "select mo_ctl('dn','flush','"+strings.ToLower(dbName)+".ai_cache')")
		exec(conns[0], "dump table ai_cache to 'stage://ai_cache_dump_stage/full'")
		exec(conns[0], "create table ai_loaded like ai_cache")
		exec(conns[0], "load table ai_loaded from 'stage://ai_cache_dump_stage/full'")
		showCache(conns[1], "ai_loaded")
		exec(conns[1], "insert into ai_loaded(v) values (2)")
		require.Equal(t, int64(2), number(conns[1], "select id from ai_loaded where v=2"))
		require.Equal(t, int64(2), number(conns[1], "select count(*) from ai_loaded"))

		exec(conns[0], "begin")
		exec(conns[0], "create temporary table ai_temp(id bigint auto_increment primary key) auto_id_cache=1")
		exec(conns[0], "rollback")
		exec(conns[0], "insert into ai_temp values (NULL)")
		showCache(conns[0], "ai_temp")
		require.Equal(t, int64(1), number(conns[0], "select id from ai_temp"))
		exec(conns[0], "drop temporary table ai_temp")
	})
}
