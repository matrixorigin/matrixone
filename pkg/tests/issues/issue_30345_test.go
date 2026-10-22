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
	"strings"
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
		t.Run("load ignore foreign key admission", func(t *testing.T) {
			testLoadIgnoreForeignKeyAdmission(t, ctx, conn, peer, writer, reader, database)
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

// Reuse the CHECK test's authenticated cluster and its commit frontier protocol.
func testLoadIgnoreForeignKeyAdmission(t *testing.T, ctx context.Context, conn *sql.Conn, peer *sql.DB, writer, reader client.TxnTimestampAware, database string) {
	exec := func(t *testing.T, query string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	syncPeer := func(t *testing.T) {
		t.Helper()
		frontier := writer.GetLatestCommitTS()
		require.False(t, frontier.IsEmpty())
		waitCtx, stop := context.WithTimeout(ctx, 10*time.Second)
		defer stop()
		snapshot, err := reader.WaitLogTailAppliedAt(waitCtx, frontier)
		require.NoError(t, err)
		require.True(t, frontier.Less(snapshot))
	}
	type queryer interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	}
	read := func(t *testing.T, query string) [][2]int {
		t.Helper()
		fetch := func(source queryer) [][2]int {
			t.Helper()
			rows, err := source.QueryContext(ctx, query)
			require.NoError(t, err)
			defer rows.Close()
			var got [][2]int
			for rows.Next() {
				var row [2]int
				require.NoError(t, rows.Scan(&row[0], &row[1]))
				got = append(got, row)
			}
			require.NoError(t, rows.Err())
			return got
		}
		local := fetch(conn)
		syncPeer(t)
		remote := fetch(peer)
		require.Equal(t, local, remote, "writer and peer must see the same committed image")
		return remote
	}
	inline := func(table, data, columns, suffix string, ignore bool) string {
		data = strings.ReplaceAll(strings.ReplaceAll(data, "\\", "\\\\"), "'", "''")
		policy := ""
		if ignore {
			policy = "ignore "
		}
		return "load data inline format='csv', data='" + data + "' " + policy + "into table " + database + "." + table + " fields terminated by ',' " + columns + " " + suffix
	}
	warn := func(t *testing.T, codes map[uint16]int) { t.Helper(); requireWarningCodes(t, ctx, conn, codes) }
	parent := database + ".fk_parent"
	exec(t, "create table "+parent+"(id int primary key)")
	exec(t, "insert into "+parent+" values(1),(2)")
	for _, mode := range []string{"inline", "server", "local"} {
		t.Run(mode, func(t *testing.T) {
			name := "fk_" + mode
			table := database + "." + name
			exec(t, "create table "+table+"(id int primary key,pid int,v int,check(v>0),foreign key(pid) references "+parent+"(id))")
			const data = "1,1,5\n2,99,5\n3,2,8\n"
			file := filepath.Join(t.TempDir(), "fk.csv")
			require.NoError(t, os.WriteFile(file, []byte(data), 0o600))
			mysql.RegisterLocalFile(file)
			defer mysql.DeregisterLocalFile(file)
			load := func(ignore bool) string {
				if mode == "inline" {
					return inline(name, data, "", "", ignore)
				}
				prefix := "load data "
				if mode == "local" {
					prefix += "local "
				}
				prefix += "infile '" + file + "' "
				if ignore {
					prefix += "ignore "
				}
				return prefix + "into table " + table + " fields terminated by ','"
			}
			exec(t, load(true))
			warn(t, map[uint16]int{1452: 1})
			require.Equal(t, [][2]int{{1, 1}, {3, 2}}, read(t, "select id,pid from "+table+" order by id"))
			exec(t, "truncate table "+table)
			_, err := conn.ExecContext(ctx, load(false))
			var fkError *mysql.MySQLError
			require.ErrorAs(t, err, &fkError)
			require.Equal(t, uint16(1452), fkError.Number)
			require.Empty(t, read(t, "select id,pid from "+table))
			if mode == "inline" {
				_, err = conn.ExecContext(ctx, inline(name, "1,1,-1\n3,2,8\n", "", "", false))
				var checkError *mysql.MySQLError
				require.ErrorAs(t, err, &checkError)
				require.Equal(t, uint16(3819), checkError.Number)
				require.Empty(t, read(t, "select id,pid from "+table))
			}
			// E4: valid parent keys do not let CHECK-invalid rows bypass admission.
			exec(t, inline(name, "1,1,-1\n3,2,8\n", "", "", true))
			warn(t, map[uint16]int{3819: 1})
			require.Equal(t, [][2]int{{3, 2}}, read(t, "select id,pid from "+table+" order by id"))
			exec(t, "truncate table "+table)
			exec(t, inline(name, "1,1,-1\n2,99,5\n3,2,8\n4,\\N,9\n", "", "", true))
			warn(t, map[uint16]int{3819: 1, 1452: 1})
			require.Equal(t, [][2]int{{3, 2}, {4, -999}}, read(t, "select id,coalesce(pid,-999) from "+table+" order by id"))
		})
	}
	t.Run("composite and multiple foreign keys", func(t *testing.T) {
		exec(t, "create table "+database+".fk_pair_parent(a int,b int,primary key(a,b))")
		exec(t, "insert into "+database+".fk_pair_parent values(1,1),(2,2)")
		exec(t, "create table "+database+".fk_pair(id int primary key,a int,b int,pid int,foreign key(a,b) references "+database+".fk_pair_parent(a,b),foreign key(pid) references "+parent+"(id))")
		// Both FKs fail on id=2, yet it produces only one warning. Partial NULL
		// bypasses only its own composite FK; id=4 must still fail the scalar FK.
		exec(t, inline("fk_pair", "1,1,1,1\n2,99,99,99\n3,\\N,99,1\n4,\\N,99,99\n5,2,2,2\n", "", "", true))
		warn(t, map[uint16]int{1452: 2})
		require.Equal(t, [][2]int{{1, 1}, {3, 1}, {5, 2}}, read(t, "select id,pid from "+database+".fk_pair order by id"))
		exec(t, inline("fk_pair", "6,99,99,99\n7,88,88,88\n", "", "", true))
		warn(t, map[uint16]int{1452: 2})
		require.Equal(t, [][2]int{{1, 1}, {3, 1}, {5, 2}}, read(t, "select id,pid from "+database+".fk_pair order by id"))
		exec(t, inline("fk_pair", "", "", "", true))
		warn(t, map[uint16]int{})
	})
	t.Run("final generated and assigned foreign keys", func(t *testing.T) {
		exec(t, "create table "+database+".fk_generated(id int primary key,v int,g int generated always as (v+1) stored,foreign key(g) references "+parent+"(id))")
		exec(t, inline("fk_generated", "1,0\n2,98\n3,1\n", "(id,v)", "", true))
		warn(t, map[uint16]int{1452: 1})
		require.Equal(t, [][2]int{{1, 1}, {3, 2}}, read(t, "select id,g from "+database+".fk_generated order by id"))
		exec(t, "create table "+database+".fk_assigned(id int primary key,pid int,foreign key(pid) references "+parent+"(id))")
		exec(t, inline("fk_assigned", "1,1\n2,2\n3,99\n4,88\n", "(id,@raw)", "set pid=nullif(@raw,99)", true))
		warn(t, map[uint16]int{1452: 1})
		require.Equal(t, [][2]int{{1, 1}, {2, 2}, {3, -999}}, read(t, "select id,coalesce(pid,-999) from "+database+".fk_assigned order by id"))
	})
	t.Run("nonunique parent does not multiply rows", func(t *testing.T) {
		exec(t, "create table "+database+".fk_prefix_parent(a int,b int,primary key(a,b))")
		exec(t, "insert into "+database+".fk_prefix_parent values(1,1),(1,2)")
		exec(t, "create table "+database+".fk_prefix(id int primary key,pid int,foreign key(pid) references "+database+".fk_prefix_parent(a))")
		exec(t, inline("fk_prefix", "1,1\n2,99\n", "", "", true))
		warn(t, map[uint16]int{1452: 1})
		require.Equal(t, [][2]int{{1, 1}}, read(t, "select id,pid from "+database+".fk_prefix"))
	})
	t.Run("fulltext and composite unique keys", func(t *testing.T) {
		exec(t, "set experimental_fulltext_index=1")
		exec(t, "create table "+database+".fk_indexed(id int primary key,k varchar(20),tag int,pid int,body text,unique(k,tag),fulltext fi(body),foreign key(pid) references "+parent+"(id))")
		exec(t, inline("fk_indexed", "1,reused,1,99,rejected\n1,reused,1,1,accepted\n2,tail,1,2,surviving\n3,recover,1,99,discarded\n", "", "", true))
		warn(t, map[uint16]int{1452: 2})
		require.Equal(t, [][2]int{{1, 1}, {2, 2}}, read(t, "select id,pid from "+database+".fk_indexed order by id"))
		exec(t, "insert into "+database+".fk_indexed values(3,'recover',1,2,'recovered')")
		require.Equal(t, [][2]int{{1, 1}, {2, 2}, {3, 2}}, read(t, "select id,pid from "+database+".fk_indexed order by id"))
		var postings string
		require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where name='fi' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='fk_indexed') limit 1", database).Scan(&postings))
		require.Equal(t, [][2]int{{1, 1}, {2, 2}, {3, 3}}, read(t, "select distinct doc_id,doc_id from "+database+".`"+postings+"` order by doc_id"))
		require.Empty(t, read(t, "select doc_id,doc_id from "+database+".`"+postings+"` where word in ('rejected','discarded')"))
		require.Equal(t, [][2]int{{1, 1}}, read(t, "select id,id from "+database+".fk_indexed where match(body) against('accepted' in boolean mode)"))
	})
	t.Run("Unicode prefix foreign key diagnostics", func(t *testing.T) {
		table := database + ".fk_unicode"
		exec(t, "create table "+table+"(id int primary key,k varchar(20) collate utf8mb4_unicode_ci,pid int,unique key uk(k(3)),foreign key(pid) references "+parent+"(id))")
		// The rejected row must not occupy the case-insensitive prefix key.
		exec(t, inline("fk_unicode", "1,Alpha,99\n1,alpha,1\n2,Tail,2\n", "", "", true))
		warn(t, map[uint16]int{1452: 1})
		require.Equal(t, [][2]int{{1, 1}, {2, 2}}, read(t, "select id,pid from "+table+" order by id"))
		// A duplicate INSERT checks the Unicode index populated by LOAD and
		// retains the logical prefix in its diagnostic.
		_, err := conn.ExecContext(ctx, "insert into "+table+" values(3,'ALPine',1)")
		var duplicate *mysql.MySQLError
		require.ErrorAs(t, err, &duplicate)
		require.Equal(t, uint16(1062), duplicate.Number)
		require.Equal(t, "23000", string(duplicate.SQLState[:]))
		require.Equal(t, "Duplicate entry 'ALP' for key 'uk'", duplicate.Message)
		require.Equal(t, [][2]int{{1, 1}, {2, 2}}, read(t, "select id,pid from "+table+" order by id"))
	})
	t.Run("foreign key checks toggles and parent targets", func(t *testing.T) {
		exec(t, "set foreign_key_checks=0")
		defer exec(t, "set foreign_key_checks=1")
		exec(t, inline("fk_inline", "10,99,5\n13,99,-1\n", "", "", true))
		warn(t, map[uint16]int{3819: 1})
		exec(t, "set foreign_key_checks=1")
		exec(t, inline("fk_inline", "11,99,5\n12,1,5\n", "", "", true))
		warn(t, map[uint16]int{1452: 1})
		require.Equal(t, [][2]int{{3, 2}, {4, -999}, {10, 99}, {12, 1}}, read(t, "select id,coalesce(pid,-999) from "+database+".fk_inline order by id"))
		exec(t, inline("fk_parent", "3\n", "", "", true))
		warn(t, map[uint16]int{})
		require.Equal(t, [][2]int{{1, 1}, {2, 2}, {3, 3}}, read(t, "select id,id from "+parent+" order by id"))
	})
	t.Run("prepared load observes foreign key checks", func(t *testing.T) {
		exec(t, "create table "+database+".fk_prepared(id int primary key,pid int,foreign key(pid) references "+parent+"(id))")
		exec(t, "set foreign_key_checks=0")
		defer exec(t, "set foreign_key_checks=1")
		query := inline("fk_prepared", "1,99\n2,1\n", "", "", true)
		exec(t, "prepare load_fk_probe from '"+strings.ReplaceAll(strings.ReplaceAll(query, "\\", "\\\\"), "'", "''")+"'")
		defer exec(t, "deallocate prepare load_fk_probe")
		exec(t, "execute load_fk_probe")
		warn(t, map[uint16]int{})
		require.Equal(t, [][2]int{{1, 99}, {2, 1}}, read(t, "select id,pid from "+database+".fk_prepared order by id"))
		exec(t, "truncate table "+database+".fk_prepared")
		exec(t, "set foreign_key_checks=1")
		exec(t, "execute load_fk_probe")
		warn(t, map[uint16]int{1452: 1})
		require.Equal(t, [][2]int{{2, 1}}, read(t, "select id,pid from "+database+".fk_prepared order by id"))
	})

	t.Run("self reference fallback keeps CHECK admission", func(t *testing.T) {
		exec(t, "create table "+database+".fk_self(id int primary key,pid int,v int,check(v>0),foreign key(pid) references "+database+".fk_self(id))")
		exec(t, inline("fk_self", "1,\\N,5\n", "(id,pid,v)", "", true))
		warn(t, map[uint16]int{})
		exec(t, inline("fk_self", "2,1,-1\n3,1,5\n", "(id,pid,v)", "", true))
		warn(t, map[uint16]int{3819: 1})
		require.Equal(t, [][2]int{{1, -999}, {3, 1}}, read(t, "select id,coalesce(pid,-999) from "+database+".fk_self order by id"))
		_, err := conn.ExecContext(ctx, inline("fk_self", "4,99,5\n", "(id,pid,v)", "", true))
		require.ErrorContains(t, err, "foreign key constraint fails")
		require.Equal(t, [][2]int{{1, -999}, {3, 1}}, read(t, "select id,coalesce(pid,-999) from "+database+".fk_self order by id"))
		exec(t, "set foreign_key_checks=0")
		defer exec(t, "set foreign_key_checks=1")
		exec(t, inline("fk_self", "5,99,5\n", "(id,pid,v)", "", true))
		warn(t, map[uint16]int{})
		require.Equal(t, [][2]int{{1, -999}, {3, 1}, {5, 99}}, read(t, "select id,coalesce(pid,-999) from "+database+".fk_self order by id"))
	})
}
