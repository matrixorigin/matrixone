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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

func TestIssue30345LoadIgnoreCheckConstraints(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		open := func(index int) (*sql.DB, client.TxnTimestampAware) {
			cn, err := c.GetCNService(index)
			require.NoError(t, err)
			service, ok := cn.RawService().(cnservice.Service)
			require.True(t, ok)
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			return db, service.GetTxnClient()
		}
		db, writer := open(0)
		defer db.Close()
		peer, reader := open(1)
		defer peer.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(t *testing.T, query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		const database = "issue30345_load_check"
		exec(t, "create database "+database)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			_, err := conn.ExecContext(cleanup, "drop database "+database)
			require.NoError(t, err)
		}()
		csv := filepath.Join(t.TempDir(), "checks.csv")
		require.NoError(t, os.WriteFile(csv, []byte("1,k_bad,1,-1\n2,k_ok,1,5\n3,k_tail,1,8\n4,k_null,1,\\N\n"), 0o600))
		mysql.RegisterLocalFile(csv)
		defer mysql.DeregisterLocalFile(csv)
		sharedKeyCSV := filepath.Join(filepath.Dir(csv), "shared_key.csv")
		require.NoError(t, os.WriteFile(sharedKeyCSV, []byte("1,k_bad,1,-1\n1,k_bad,1,6\n"), 0o600))
		mysql.RegisterLocalFile(sharedKeyCSV)
		defer mysql.DeregisterLocalFile(sharedKeyCSV)
		read := func(t *testing.T, table, generated string) [][3]int {
			t.Helper()
			// Order the peer's next autocommit snapshot after the writer's
			// commit before checking durable rows on another CN.
			frontier := writer.GetLatestCommitTS()
			require.False(t, frontier.IsEmpty(), "writer must publish a commit before the peer reads")
			waitCtx, stop := context.WithTimeout(ctx, 10*time.Second)
			snapshotTS, err := reader.WaitLogTailAppliedAt(waitCtx, frontier)
			stop()
			require.NoError(t, err, "peer must apply logtail through the writer commit")
			require.True(t, frontier.Less(snapshotTS), "peer snapshot must advance past the writer commit")
			rows, err := peer.QueryContext(ctx, "select id,coalesce(v,999),coalesce("+generated+",999) from "+table+" order by id")
			require.NoError(t, err)
			defer rows.Close()
			var result [][3]int
			for rows.Next() {
				var row [3]int
				require.NoError(t, rows.Scan(&row[0], &row[1], &row[2]))
				result = append(result, row)
			}
			require.NoError(t, rows.Err())
			return result
		}
		for _, local := range []bool{false, true} {
			for index, tc := range []struct {
				name, definition, generated string
				warnings                    int
				want                        [][3]int
			}{
				{"single", "unique(k), constraint %[1]s_chk_pos check(v>0)", "v*2", 1, [][3]int{{2, 5, 10}, {3, 8, 16}, {4, 999, 999}}},
				{"multiple", "unique(k), constraint %[1]s_chk_pos check(v>0), constraint %[1]s_chk_upper check(v<7)", "v*2", 2, [][3]int{{2, 5, 10}, {4, 999, 999}}},
				{"generated_composite", "g int generated always as (v*2) stored, unique(k,tag), constraint %[1]s_chk_gen check(g>0)", "g", 1, [][3]int{{2, 5, 10}, {3, 8, 16}, {4, 999, 999}}},
			} {
				t.Run(fmt.Sprintf("local_%t/%s", local, tc.name), func(t *testing.T) {
					// CHECK names are schema-scoped, and earlier case tables remain
					// alive. Qualify fixture identities without weakening LOAD checks.
					name := fmt.Sprintf("t_%t_%d", local, index)
					table := database + "." + name
					exec(t, "create table "+table+"(id int primary key,k varchar(20),tag int,v int,"+fmt.Sprintf(tc.definition, name)+")")
					prefix := "load data "
					if local {
						prefix += "local "
					}
					prefix += "infile '" + csv + "' "
					suffix := "into table " + table + " fields terminated by ',' (id,k,tag,v)"
					exec(t, prefix+"ignore "+suffix)
					requireWarningCodes(t, ctx, conn, map[uint16]int{3819: tc.warnings})
					require.Equal(t, tc.want, read(t, table, tc.generated), "second CN must see only CHECK-valid committed rows")
					// A rejected row must not reserve its primary or unique key.
					exec(t, "insert into "+table+"(id,k,tag,v) values(1,'k_bad',1,6)")
					want := append([][3]int{{1, 6, 12}}, tc.want...)
					require.Equal(t, want, read(t, table, tc.generated))
					exec(t, "truncate table "+table)
					_, err := conn.ExecContext(ctx, prefix+suffix)
					var checkError *mysql.MySQLError
					require.ErrorAs(t, err, &checkError)
					require.Equal(t, uint16(3819), checkError.Number)
					require.Empty(t, read(t, table, tc.generated), "ordinary LOAD must roll back all rows")
					exec(t, "insert ignore into "+table+"(id,k,tag,v) values(1,'k_bad',1,-1),(2,'k_ok',1,5)")
					requireWarningCodes(t, ctx, conn, map[uint16]int{3819: 1})
					require.Equal(t, [][3]int{{2, 5, 10}}, read(t, table, tc.generated))
					exec(t, "truncate table "+table)
					// The invalid candidate must be filtered before dedup sees a
					// later valid row with the same primary and unique keys.
					keyPrefix := "load data "
					if local {
						keyPrefix += "local "
					}
					keyPrefix += "infile '" + sharedKeyCSV + "' ignore "
					exec(t, keyPrefix+suffix)
					requireWarningCodes(t, ctx, conn, map[uint16]int{3819: 1})
					require.Equal(t, [][3]int{{1, 6, 12}}, read(t, table, tc.generated))
				})
			}
		}
		t.Run("insert ignore keeps duplicate arbitration", func(t *testing.T) {
			exec(t, "create table "+database+".dup(id int primary key,k varchar(20),tag int,v int,check(v>0))")
			exec(t, "insert into "+database+".dup values(2,'seed',1,5)")
			// LOAD's dedup bypass is checked by TestLoadCheckConstraintPolicy.
			// Do not require the duplicate-row corruption tracked by #25470.
			exec(t, "insert ignore into "+database+".dup values(2,'k_input',1,6),(3,'k_tail',1,8)")
			requireWarningCodes(t, ctx, conn, map[uint16]int{1062: 1})
			require.Equal(t, [][3]int{{2, 5, 10}, {3, 8, 16}}, read(t, database+".dup", "v*2"))
		})
		t.Run("load ignore keeps foreign key rejection", func(t *testing.T) {
			validCSV := filepath.Join(filepath.Dir(csv), "valid_checks.csv")
			require.NoError(t, os.WriteFile(validCSV, []byte("2,k_input,1,6\n3,k_tail,1,8\n"), 0o600))
			exec(t, "create table "+database+".parent(id int primary key)")
			exec(t, "create table "+database+".child(id int primary key,k varchar(20),tag int,v int,check(v>0),foreign key(tag) references "+database+".parent(id))")
			_, err := conn.ExecContext(ctx, "load data infile '"+validCSV+"' ignore into table "+database+".child fields terminated by ','")
			require.ErrorContains(t, err, "foreign key constraint fails")
			require.Empty(t, read(t, database+".child", "v*2"))
		})
		exec(t, "set experimental_fulltext_index=1")
		files := make(map[string]string)
		for name, data := range map[string]string{
			"valid":    "1,one,1,5,accepted\n",
			"mixed":    "2,two,1,-1,rejected\n3,three,1,8,surviving\n",
			"rejected": "4,four,1,-1,discarded\n",
		} {
			path := filepath.Join(filepath.Dir(csv), "fulltext_"+name+".csv")
			require.NoError(t, os.WriteFile(path, []byte(data), 0o600))
			mysql.RegisterLocalFile(path)
			defer mysql.DeregisterLocalFile(path)
			files[name] = path
		}
		queryIDs := func(t *testing.T, query string) []int {
			t.Helper()
			rows, err := peer.QueryContext(ctx, query)
			require.NoError(t, err)
			defer rows.Close()
			var ids []int
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id))
				ids = append(ids, id)
			}
			require.NoError(t, rows.Err())
			return ids
		}
		for _, local := range []bool{false, true} {
			for index, tc := range []struct {
				name, definition, generated string
				ignore, checks              bool
			}{
				{"basic", "check(v>0)", "v*2", true, true},
				{"single_unique", "unique(k),check(v>0)", "v*2", true, true},
				{"composite_unique", "unique(k,tag),check(v>0)", "v*2", true, true},
				{"generated", "g int generated always as (v*2) stored,unique(k,tag),check(g>0)", "g", true, true},
				{"no_check", "", "v*2", true, false},
				{"ordinary_load", "check(v>0)", "v*2", false, true},
			} {
				t.Run(fmt.Sprintf("fulltext/local_%t/%s", local, tc.name), func(t *testing.T) {
					name := fmt.Sprintf("ft_%t_%d", local, index)
					table := database + "." + name
					definition := "id int primary key,k varchar(20),tag int,v int,body text,fulltext fi(body)"
					if tc.definition != "" {
						definition += "," + tc.definition
					}
					exec(t, "create table "+table+"("+definition+")")
					load := func(file string) {
						t.Helper()
						prefix := "load data "
						if local {
							prefix += "local "
						}
						prefix += "infile '" + files[file] + "' "
						if tc.ignore {
							prefix += "ignore "
						}
						exec(t, prefix+"into table "+table+" fields terminated by ',' (id,k,tag,v,body)")
					}
					load("valid")
					requireWarningCodes(t, ctx, conn, map[uint16]int{})
					var postings string
					require.NoError(t, conn.QueryRowContext(ctx,
						"select index_table_name from mo_catalog.mo_indexes where name='fi' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?) limit 1",
						database, name).Scan(&postings))
					postings = database + ".`" + postings + "`"
					assertImage := func(want [][3]int, ids []int) {
						t.Helper()
						require.Equal(t, want, read(t, table, tc.generated))
						require.Equal(t, ids, queryIDs(t, "select distinct doc_id from "+postings+" order by doc_id"))
						var orphans int
						require.NoError(t, peer.QueryRowContext(ctx, "select count(*) from "+postings+" p left join "+table+" b on p.doc_id=b.id where b.id is null or p.word in ('rejected','discarded')").Scan(&orphans))
						require.Zero(t, orphans, "rejected rows must never produce postings, even after their keys are reused")
					}
					assertImage([][3]int{{1, 5, 10}}, []int{1})
					require.Equal(t, []int{1}, queryIDs(t, "select id from "+table+" where match(body) against('accepted' in boolean mode) order by id"))
					if !tc.checks || !tc.ignore {
						return
					}
					load("mixed")
					requireWarningCodes(t, ctx, conn, map[uint16]int{3819: 1})
					assertImage([][3]int{{1, 5, 10}, {3, 8, 16}}, []int{1, 3})
					load("rejected")
					requireWarningCodes(t, ctx, conn, map[uint16]int{3819: 1})
					assertImage([][3]int{{1, 5, 10}, {3, 8, 16}}, []int{1, 3})
					exec(t, "insert into "+table+"(id,k,tag,v,body) values(2,'two',1,6,'reused'),(4,'four',1,7,'recovered')")
					assertImage([][3]int{{1, 5, 10}, {2, 6, 12}, {3, 8, 16}, {4, 7, 14}}, []int{1, 2, 3, 4})
					require.Equal(t, []int{3}, queryIDs(t, "select id from "+table+" where match(body) against('surviving' in boolean mode) order by id"))
					require.Empty(t, queryIDs(t, "select id from "+table+" where match(body) against('rejected discarded' in boolean mode) order by id"))
				})
			}
		}
	})
}
