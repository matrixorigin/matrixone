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
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// TestIssue28349AutoIncrementPublicPaths exercises the three public paths added
// by the auto-increment session/provenance work: one multi-statement COM_QUERY,
// ordered INSERT IGNORE candidate reuse, and REPLACE LAST_INSERT_ID reporting.
// The name refers to the PR; the covered issues are #28237, #28238 and #28239.
func TestIssue28349AutoIncrementPublicPaths(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dbName := testutils.GetDatabaseName(t)
		dsn := fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?multiStatements=true",
			cn.GetServiceConfig().CN.Frontend.Port,
		)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(t *testing.T, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(t, err, "exec failed: %s", statement)
		}
		queryInt64Rows := func(t *testing.T, statement string, columns int) [][]int64 {
			t.Helper()
			rows, err := conn.QueryContext(ctx, statement)
			require.NoErrorf(t, err, "query failed: %s", statement)
			defer rows.Close()
			var result [][]int64
			for rows.Next() {
				values := make([]int64, columns)
				dest := make([]any, columns)
				for i := range values {
					dest[i] = &values[i]
				}
				require.NoError(t, rows.Scan(dest...))
				result = append(result, values)
			}
			require.NoError(t, rows.Err())
			return result
		}
		queryInt64 := func(t *testing.T, statement string) int64 {
			t.Helper()
			var value int64
			require.NoError(t, conn.QueryRowContext(ctx, statement).Scan(&value))
			return value
		}

		exec(t, fmt.Sprintf("create database `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()
		exec(t, fmt.Sprintf("use `%s`", dbName))
		exec(t, "set auto_increment_increment = 1")
		exec(t, "set auto_increment_offset = 1")

		exec(t, "create table ai_multi(id bigint auto_increment primary key, v int)")
		// The SET statements and INSERT must share one COM_QUERY. Separate
		// Exec calls would not exercise frontend statement snapshot refresh.
		exec(t, "set auto_increment_increment = 3; set auto_increment_offset = 2; insert into ai_multi(v) values (1), (2), (3)")
		require.Equal(t, [][]int64{{2, 1}, {5, 2}, {8, 3}},
			queryInt64Rows(t, "select id, v from ai_multi order by id", 2))
		exec(t, "drop table ai_multi")

		exec(t, "set auto_increment_increment = 1")
		exec(t, "set auto_increment_offset = 1")
		exec(t, "create table ai_ignore(id bigint auto_increment primary key, uk int unique, v int)")
		exec(t, "insert into ai_ignore(uk, v) values (10, 0)")
		exec(t, "insert ignore into ai_ignore(uk, v) values (20, 1), (10, 2), (30, 3)")
		require.Equal(t, [][]int64{{1, 10}, {2, 20}, {3, 30}},
			queryInt64Rows(t, "select id, uk from ai_ignore order by id", 2))
		require.Equal(t, int64(2), queryInt64(t, "select last_insert_id()"),
			"INSERT IGNORE must publish the first accepted generated candidate")
		exec(t, "drop table ai_ignore")

		for _, tc := range []struct {
			name, definition, rows string
		}{
			{"composite", "a varchar(10), b int, unique key uk(a,b)", "('ab1',1),('ab1',1),('ab2',2)"},
			{"prefix", "a varchar(10), b int, unique key uk(a(2))", "('ab1',1),('ab2',1),('cd1',2)"},
			{"check", "a varchar(10), b int check(b > 0), unique key uk(a,b)", "('ab1',1),('ab1',1),('ab2',2)"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec(t, "create table ai_aux(id bigint auto_increment primary key, "+tc.definition+")")
				defer exec(t, "drop table ai_aux")
				exec(t, "insert ignore into ai_aux(a,b) values "+tc.rows)
				require.Equal(t, [][]int64{{1, 1}, {2, 2}}, queryInt64Rows(t, "select id,b from ai_aux order by id", 2))
				require.Equal(t, int64(1), queryInt64(t, "select last_insert_id()"))
				// Lookup by the UK, not only a base-table scan, must see final PKs.
				require.Equal(t, [][]int64{{1, 1}}, queryInt64Rows(t, "select id,b from ai_aux where a='ab1' and b=1", 2))
				exec(t, "update ai_aux set b=3 where id=2")
				require.Equal(t, [][]int64{{1, 1}, {2, 3}}, queryInt64Rows(t, "select id,b from ai_aux order by id", 2))
				stmt, err := conn.PrepareContext(ctx, "insert ignore into ai_aux(a,b) values (?,?)")
				require.NoError(t, err)
				defer stmt.Close()
				_, err = stmt.ExecContext(ctx, "xy1", 4)
				require.NoError(t, err)
				_, err = stmt.ExecContext(ctx, "xy1", 4)
				require.NoError(t, err)
				require.Equal(t, int64(3), queryInt64(t, "select count(*) from ai_aux"))
			})
		}

		t.Run("irregular_indexes_final_image", func(t *testing.T) {
			exec(t, "create table ai_irregular(id bigint auto_increment primary key, uk int, g int check(g>=0), body varchar(100), unique key uq(uk,g), index mi using master(body), fulltext fi(body))")
			defer exec(t, "drop table ai_irregular")
			exec(t, "insert ignore into ai_irregular(uk,g,body) values (1,0,'alpha'),(1,0,'beta'),(2,0,'gamma')")
			require.Equal(t, [][]int64{{1, 1}, {2, 2}}, queryInt64Rows(t, "select id,uk from ai_irregular order by id", 2))
			require.Equal(t, int64(1), queryInt64(t, "select last_insert_id()"))
			assertIndexed := func(token string, ids [][]int64) {
				t.Helper()
				require.Equal(t, ids, queryInt64Rows(t, "select id from ai_irregular where match(body) against('"+token+"' in boolean mode) order by id", 1))
				require.Equal(t, ids, queryInt64Rows(t, "select id from ai_irregular force index(mi) where body='"+token+"' order by id", 1))
			}
			assertIndexed("gamma", [][]int64{{2}})
			assertIndexed("beta", nil)

			// Inspect postings too: an orphan can be invisible to the join today
			// and become a false positive when that PK is explicitly inserted later.
			var tableID uint64
			require.NoError(t, conn.QueryRowContext(ctx,
				"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='ai_irregular'", strings.ToLower(dbName)).Scan(&tableID))
			var postings string
			require.NoError(t, conn.QueryRowContext(ctx,
				"select index_table_name from mo_catalog.mo_indexes where table_id=? and name='fi' limit 1", tableID).Scan(&postings))
			require.Equal(t, [][]int64{{1}, {2}}, queryInt64Rows(t, "select distinct doc_id from `"+postings+"` order by doc_id", 1))
			assertNoRejectedPostings := func() {
				t.Helper()
				require.Zero(t, queryInt64(t, "select count(*) from `"+postings+"` where word in ('beta','rejected','rollback','failed')"))
			}
			assertNoRejectedPostings()
			exec(t, "insert ignore into ai_irregular(uk,g,body) values (1,0,'rejected'),(4,-1,'rejected')")
			exec(t, "insert ignore into ai_irregular(uk,g,body) select uk,g,body from ai_irregular where false")
			require.Equal(t, int64(2), queryInt64(t, "select count(*) from ai_irregular"))
			assertNoRejectedPostings()

			stmt, err := conn.PrepareContext(ctx, "insert ignore into ai_irregular(uk,g,body) values (?,0,?)")
			require.NoError(t, err)
			defer stmt.Close()
			_, err = stmt.ExecContext(ctx, 3, "delta")
			require.NoError(t, err)
			_, err = stmt.ExecContext(ctx, 3, "rejected")
			require.NoError(t, err)
			assertIndexed("delta", queryInt64Rows(t, "select id from ai_irregular where uk=3", 1))
			assertNoRejectedPostings()

			exec(t, "begin")
			defer exec(t, "rollback")
			exec(t, "insert ignore into ai_irregular(uk,g,body) values (4,0,'rollback')")
			assertIndexed("rollback", queryInt64Rows(t, "select id from ai_irregular where uk=4", 1))
			exec(t, "rollback")
			assertIndexed("rollback", nil)
			_, err = conn.ExecContext(ctx, "insert into ai_irregular(uk,g,body) values (1,0,'failed')")
			require.Error(t, err)
			assertNoRejectedPostings()
			exec(t, "delete from ai_irregular where uk=2")
			assertIndexed("gamma", nil)
			exec(t, "update ai_irregular set body='updated' where uk=1")
			assertIndexed("alpha", nil)
			assertIndexed("updated", [][]int64{{1}})
		})

		t.Run("irregular_ivf_final_image", func(t *testing.T) {
			exec(t, "set experimental_ivf_index=1")
			exec(t, "create table ai_irregular_ivf(id bigint auto_increment primary key, uk int unique, v vecf32(3))")
			defer exec(t, "drop table ai_irregular_ivf")
			exec(t, "create index vi using ivfflat on ai_irregular_ivf(v) lists=1 op_type 'vector_l2_ops'")
			exec(t, "insert ignore into ai_irregular_ivf(uk,v) values (1,'[1,0,0]'),(1,'[0,0,1]'),(2,'[0,1,0]')")
			require.Equal(t, [][]int64{{1, 1}, {2, 2}}, queryInt64Rows(t, "select id,uk from ai_irregular_ivf order by id", 2))
			var entryTable string
			require.NoError(t, conn.QueryRowContext(ctx,
				"select index_table_name from mo_catalog.mo_indexes where name='vi' and algo_table_type='entries' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='ai_irregular_ivf')", strings.ToLower(dbName)).Scan(&entryTable))
			// Compare hidden entries with base rows, not just a top-K query that
			// could fall back to a table scan on an untrained, initially empty index.
			require.Equal(t, [][]int64{{1}, {2}}, queryInt64Rows(t, "select __mo_index_pri_col from `"+entryTable+"` order by __mo_index_pri_col", 1))
			require.Equal(t, int64(2), queryInt64(t, "select count(*) from `"+entryTable+"` e join ai_irregular_ivf b on e.__mo_index_pri_col=b.id where l2_distance(e.__mo_index_centroid_fk_entry,b.v)=0"))
		})

		t.Run("irregular_multi_batch", func(t *testing.T) {
			exec(t, "create table ai_irregular_batch(id bigint auto_increment primary key, uk bigint unique, body varchar(16), index mi using master(body), fulltext fi(body))")
			defer exec(t, "drop table ai_irregular_batch")
			// More than two accepted execution batches exercise shared-SINK
			// fanout/backpressure as well as candidate retention across batches.
			exec(t, "insert ignore into ai_irregular_batch(uk,body) select (result+1) div 2, if(result%2=1,'kept','rejected') from generate_series(1,40000) g order by result")
			require.Equal(t, [][]int64{{20000, 1, 20000}}, queryInt64Rows(t, "select count(*),min(id),max(id) from ai_irregular_batch", 3))
			require.Zero(t, queryInt64(t, "select count(*) from ai_irregular_batch where id<>uk or body<>'kept'"))
			require.Equal(t, int64(20000), queryInt64(t, "select count(*) from ai_irregular_batch where match(body) against('kept' in boolean mode)"))
			require.Equal(t, int64(20000), queryInt64(t, "select count(*) from ai_irregular_batch force index(mi) where body='kept'"))
			require.Zero(t, queryInt64(t, "select count(*) from ai_irregular_batch where match(body) against('rejected' in boolean mode)"))
			var postings string
			require.NoError(t, conn.QueryRowContext(ctx,
				"select index_table_name from mo_catalog.mo_indexes where name='fi' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='ai_irregular_batch') limit 1", strings.ToLower(dbName)).Scan(&postings))
			require.Equal(t, int64(20000), queryInt64(t, "select count(distinct doc_id) from `"+postings+"`"))
			require.Zero(t, queryInt64(t, "select count(*) from `"+postings+"` p left join ai_irregular_batch b on p.doc_id=b.id where b.id is null or p.word='rejected'"))
		})

		// Non-reordering controls exercise single-key DEDUP with both real and
		// hidden primary keys. Its accepted row is not the join's first child.
		for _, tc := range []struct{ keyDef, indexDef, hint, match string }{
			{"primary key", "fulltext fi(body)", "", "match(body) against('%s' in boolean mode)"},
			{"unique", "index mi using master(body)", "force index(mi)", "body='%s'"},
		} {
			t.Run("irregular_"+tc.keyDef, func(t *testing.T) {
				exec(t, "create table ai_irregular_control(k bigint "+tc.keyDef+", body varchar(100), "+tc.indexDef+")")
				defer exec(t, "drop table ai_irregular_control")
				exec(t, "insert ignore into ai_irregular_control values (1,'alpha'),(1,'beta'),(2,'gamma')")
				require.Equal(t, [][]int64{{1}, {2}}, queryInt64Rows(t, "select k from ai_irregular_control order by k", 1))
				require.Equal(t, [][]int64{{2}}, queryInt64Rows(t, "select k from ai_irregular_control "+tc.hint+" where "+fmt.Sprintf(tc.match, "gamma"), 1))
				require.Empty(t, queryInt64Rows(t, "select k from ai_irregular_control "+tc.hint+" where "+fmt.Sprintf(tc.match, "beta"), 1))
			})
		}

		exec(t, "create table ai_replace(id bigint auto_increment primary key, uk int unique, v int)")
		exec(t, "insert into ai_replace(uk, v) values (10, 1)")
		exec(t, "replace into ai_replace(uk, v) values (10, 2)")
		require.Equal(t, [][]int64{{2, 10, 2}},
			queryInt64Rows(t, "select id, uk, v from ai_replace order by id", 3))
		require.Equal(t, int64(2), queryInt64(t, "select last_insert_id()"),
			"REPLACE must publish the generated replacement key")
		exec(t, "drop table ai_replace")

		// Two explicit CN endpoints: another writer owns a different range but
		// must honor its own series, independent of the creating session.
		exec(t, "set auto_increment_increment=3; set auto_increment_offset=2")
		exec(t, "create table ai_remote(id bigint auto_increment primary key, v int)")
		defer exec(t, "drop table ai_remote")
		exec(t, "set auto_increment_increment=1; set auto_increment_offset=1")
		exec(t, "insert into ai_remote(v) values (0)")
		require.Equal(t, int64(1), queryInt64(t, "select id from ai_remote"))
		cn1, err := c.GetCNService(1)
		require.NoError(t, err)
		other, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true",
			cn1.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer other.Close()
		other.SetMaxOpenConns(1)
		_, err = other.ExecContext(ctx, fmt.Sprintf("use `%s`", dbName))
		require.NoError(t, err)
		_, err = other.ExecContext(ctx,
			"set auto_increment_increment=4; set auto_increment_offset=3; insert into ai_remote(v) values (1),(2),(3)")
		require.NoError(t, err)
		require.Equal(t, [][]int64{{3, 3, 8}}, queryInt64Rows(t,
			"select count(*), min(id)%4, max(id)-min(id) from ai_remote where v>0", 3))
		require.Equal(t, int64(4), queryInt64(t, "select count(distinct id) from ai_remote"))
	})
}
