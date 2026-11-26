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

package dml

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	txnclient "github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

// Use the MySQL frontend and real transaction/index writers. Unit tests cover
// the internal ordering/accounting invariants independently.
func TestODKUSystematicSQL(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		t.Cleanup(cancel)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		exec := func(t *testing.T, q string) { t.Helper(); _, e := conn.ExecContext(ctx, q); require.NoError(t, e, q) }
		rowsFrom := func(t *testing.T, reader *sql.Conn, q string) []string {
			t.Helper()
			r, e := reader.QueryContext(ctx, q)
			require.NoError(t, e, q)
			defer r.Close()
			cols, e := r.Columns()
			require.NoError(t, e)
			var out []string
			for r.Next() {
				values := make([]sql.RawBytes, len(cols))
				args := make([]any, len(cols))
				for i := range args {
					args[i] = &values[i]
				}
				require.NoError(t, r.Scan(args...))
				s := make([]string, len(cols))
				for i, b := range values {
					if b == nil {
						s[i] = "NULL"
					} else {
						s[i] = string(b)
					}
				}
				out = append(out, strings.Join(s, "|"))
			}
			require.NoError(t, r.Err())
			return out
		}
		rows := func(t *testing.T, q string) []string { t.Helper(); return rowsFrom(t, conn, q) }
		constraintError := func(t *testing.T, err error, code uint16, message string) {
			t.Helper()
			var serverError *mysqlDriver.MySQLError
			require.ErrorAs(t, err, &serverError)
			require.Equal(t, code, serverError.Number)
			require.Contains(t, serverError.Message, message)
		}

		exec(t, "create database odku_systematic")
		t.Cleanup(func() { cleanupODKUSQL(t, conn, "drop database if exists odku_systematic") })
		exec(t, "use odku_systematic")
		exec(t, "create table parent (id int primary key)")
		exec(t, "insert into parent values (1)")
		affected := func(t *testing.T, want int64, q string, args ...any) {
			t.Helper()
			result, err := conn.ExecContext(ctx, q, args...)
			require.NoError(t, err, q)
			count, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, want, count, q)
		}
		t.Run("physical floating key identities", func(t *testing.T) {
			for _, primary := range []bool{false, true} {
				for _, negativeFirst := range []bool{false, true} {
					name := fmt.Sprintf("physical_zero_%t_%t", primary, negativeFirst)
					keys := "primary key(id),unique key uk(k)"
					if primary {
						keys = "primary key(k),unique key ui(id)"
					}
					exec(t, "create table "+name+"(id int,k double,v int,"+keys+")")
					first, second := float64(0), math.Copysign(0, -1)
					if negativeFirst {
						first, second = second, first
					}
					// Binary parameters preserve the two IEEE zero representations.
					affected(t, 2, "insert into "+name+" values(?,?,?),(?,?,?) on duplicate key update v=values(v)", 1, first, 11, 2, second, 22)
					require.Equal(t, []string{"1|11", "2|22"}, rows(t, "select id,v from "+name+" order by id"))
					affected(t, 2, "insert into "+name+" values(?,?,?) on duplicate key update v=values(v)", 3, second, 99)
					require.Equal(t, []string{"1|11", "2|99"}, rows(t, "select id,v from "+name+" order by id"))
					if !primary {
						require.Equal(t, []string{"1|11", "2|99"}, rows(t, "select id,v from "+name+" force index(uk) where k between -1 and 1 order by id"))
					}
				}
			}
			t.Run("captured unique workspace keeps both zero owners", func(t *testing.T) {
				exec(t, "create table physical_zero_workspace(id int primary key,a double,g double generated always as(a) stored,v int,unique key ug(g))")
				exec(t, "begin")
				t.Cleanup(func() { cleanupODKUSQL(t, conn, "rollback") })
				affected(t, 1, "insert into physical_zero_workspace(id,a,v) values(?,?,?)", 1, float64(0), 0)
				affected(t, 1, "insert into physical_zero_workspace(id,a,v) values(?,?,?)", 2, math.Copysign(0, -1), 0)
				affected(t, 4, "insert into physical_zero_workspace(id,a,v) values(?,?,?),(?,?,?) on duplicate key update a=values(a),v=v+values(v)", 9, float64(0), 1, 10, math.Copysign(0, -1), 1)
				require.Equal(t, []string{"1|1", "2|1"}, rows(t, "select id,v from physical_zero_workspace order by id"))
				exec(t, "rollback")
				require.Empty(t, rows(t, "select id from physical_zero_workspace"))
			})
		})
		t.Run("effective action precedes automatic generated keys", func(t *testing.T) {
			year := rows(t, "select year(now())")[0]
			for _, primary := range []bool{false, true} {
				name := fmt.Sprintf("automatic_key_%t", primary)
				keys := "primary key(id),unique key ug(g)"
				if primary {
					keys = "primary key(g),unique key ui(id)"
				}
				exec(t, "create table "+name+"(id int,v int,ts timestamp not null default current_timestamp on update current_timestamp,g int generated always as(year(ts)) stored,"+keys+")")
				exec(t, "insert into "+name+"(id,v,ts) values(1,0,'2000-01-01'),(2,0,now())")
				statement := "insert into " + name + "(id,v,ts) values(1,?,'2000-01-01') on duplicate key update v=values(v)"
				affected(t, 0, statement, 0)
				want := []string{"1|0|2000|2000", "2|0|" + year + "|" + year}
				require.Equal(t, want, rows(t, "select id,v,year(ts),g from "+name+" order by id"))
				_, err := conn.ExecContext(ctx, statement, 1)
				constraintError(t, err, 1062, "Duplicate entry")
				require.Equal(t, want, rows(t, "select id,v,year(ts),g from "+name+" order by id"), "the real collision rolls back the whole action")
			}
		})
		t.Run("logical action counts and final automatic image", func(t *testing.T) {
			exec(t, "create table action_counts(id int primary key,v int)")
			affected(t, 1, "insert into action_counts values(1,0)")
			affected(t, 4, "insert into action_counts values(1,1),(1,2),(1,2) on duplicate key update v=values(v)")
			affected(t, 0, "insert into action_counts values(1,2) on duplicate key update v=values(v)")
			require.Equal(t, []string{"1|2"}, rows(t, "select * from action_counts"))
			affected(t, 0, "insert into action_counts select id,v from action_counts where false on duplicate key update v=values(v)")
			exec(t, "truncate table action_counts")
			affected(t, 3, "insert into action_counts values(1,1),(1,2) on duplicate key update v=values(v)")
			require.Equal(t, []string{"1|2"}, rows(t, "select * from action_counts"))
			statement, err := conn.PrepareContext(ctx, "insert into action_counts values(1,?) on duplicate key update v=values(v)")
			require.NoError(t, err)
			defer statement.Close()
			for _, action := range []struct{ value, count int64 }{{3, 2}, {3, 0}, {2, 2}} {
				result, err := statement.ExecContext(ctx, action.value)
				require.NoError(t, err)
				count, err := result.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, action.count, count)
			}
			exec(t, "create table action_revert(id int primary key,v int,ts timestamp not null default current_timestamp on update current_timestamp)")
			exec(t, "insert into action_revert values(1,0,'2000-01-01')")
			affected(t, 4, "insert into action_revert(id,v) values(1,1),(1,0) on duplicate key update v=values(v)")
			require.Equal(t, []string{"1|0|" + rows(t, "select year(now())")[0]}, rows(t, "select id,v,year(ts) from action_revert"))
		})
		t.Run("no-op retains JSON representation for the next action", func(t *testing.T) {
			exec(t, "create table action_json(id int primary key,j json,seen varchar(32))")
			exec(t, `insert into action_json values(1,'{"n":1.0}','none')`)
			require.Equal(t, []string{"1"}, rows(t, `select cast('{"n":1}' as json) <=> cast('{"n":1.0}' as json)`))
			affected(t, 2, `insert into action_json values(1,'{"n":1}','keep'),(1,'{"n":1}','read') on duplicate key update j=if(values(seen)='keep',values(j),j),seen=if(values(seen)='read',json_type(json_extract(j,'$.n')),seen)`)
			require.Equal(t, []string{"DOUBLE|DOUBLE"}, rows(t, "select json_type(json_extract(j,'$.n')),seen from action_json"))
			// A real change may store a SQL-equal JSON representation. It must
			// remain visible; restoring every equal column would lose this value.
			affected(t, 4, `insert into action_json values(1,'{"n":1}','change'),(1,'{"n":1}','read') on duplicate key update j=if(values(seen)='change',values(j),j),seen=if(values(seen)='read',json_type(json_extract(j,'$.n')),values(seen))`)
			require.Equal(t, []string{"INTEGER|INTEGER"}, rows(t, "select json_type(json_extract(j,'$.n')),seen from action_json"))
		})
		t.Run("typed no-op comparison preserves SQL equality", func(t *testing.T) {
			exec(t, "create table action_types(id int primary key,c char(8),e enum('red','blue'),f float(8,2),n int)")
			exec(t, "insert into action_types values(1,'A','red',1.23,null)")
			affected(t, 0, "insert into action_types values(1,'A','red',1.234,null) on duplicate key update c=values(c),e=values(e),f=values(f),n=values(n)")
			affected(t, 2, "insert into action_types values(1,'B','blue',1.25,7) on duplicate key update c=values(c),e=values(e),f=values(f),n=values(n)")
			require.Equal(t, []string{"B|blue|1.25|7"}, rows(t, "select c,e,f,n from action_types"))
		})
		t.Run("insert claims and snapshot keys retain constraint priority", func(t *testing.T) {
			exec(t, "create table static_priority(id int primary key,u int unique,v int)")
			exec(t, "insert into static_priority values(1,10,100)")
			statement := "insert into static_priority values(2,20,1),(2,10,2),(1,20,3),(99,null,5),(99,10,7) on duplicate key update v=v+values(v)"
			check := func() {
				require.Equal(t, []string{"1|10|103", "2|20|3", "99|NULL|12"}, rows(t, "select * from static_priority order by id"))
				require.Equal(t, []string{"1|103"}, rows(t, "select id,v from static_priority force index(u) where u=10"))
				require.Equal(t, []string{"2|3"}, rows(t, "select id,v from static_priority force index(u) where u=20"))
			}
			exec(t, "begin")
			t.Cleanup(func() { _, _ = conn.ExecContext(ctx, "rollback") })
			exec(t, statement)
			check()
			exec(t, "rollback")
			require.Equal(t, []string{"1|10|100"}, rows(t, "select * from static_priority"))
			exec(t, statement)
			check()
		})
		t.Run("irregular indexes delete original primary keys", func(t *testing.T) {
			oldFulltext := rows(t, "select @@experimental_fulltext_index")[0]
			exec(t, "set experimental_fulltext_index=1")
			t.Cleanup(func() { exec(t, "set experimental_fulltext_index="+oldFulltext) })
			exec(t, "create table index_rhs(suffix varchar(16))")
			exec(t, "insert into index_rhs values(' words')")
			for _, scalar := range []bool{false, true} {
				t.Run(fmt.Sprintf("scalar_helper=%t", scalar), func(t *testing.T) {
					name := fmt.Sprintf("texts_%t", scalar)
					exec(t, "create table "+name+"(a int,g int generated always as(a+1) stored primary key,u int unique,unused varchar(32),body text,tail int,key ia(a),key it(tail),key iu(u),fulltext ft(body))")
					exec(t, "insert into "+name+"(a,u,unused,body,tail) values(1,10,'unused','original terms',7),(10,77,'other','neighbortoken',41)")
					hidden := rows(t, "select index_table_name from mo_catalog.mo_indexes where name='ft' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='odku_systematic' and relname='"+name+"')")
					require.Len(t, hidden, 1)
					ids := func() []string { return rows(t, "select distinct doc_id from `"+hidden[0]+"` order by doc_id") }
					suffix := "' words'"
					if scalar {
						suffix = "(select max(suffix) from index_rhs)"
					}
					update := "insert into " + name + "(a,u,unused,body,tail) values(1,10,'input','updated',8) on duplicate key update a=3,body=concat(values(body)," + suffix + "),tail=8"
					check := func() {
						require.Equal(t, []string{"3|4|10|updated words|8", "10|11|77|neighbortoken|41"}, rows(t, "select a,g,u,body,tail from "+name+" order by g"))
						require.Equal(t, []string{"4|updated words"}, rows(t, "select g,body from "+name+" force index(u) where u=10"))
						require.Equal(t, []string{"4", "11"}, ids())
						require.Equal(t, []string{"4"}, rows(t, "select g from "+name+" where match(body) against('updated') order by g"))
						require.Empty(t, rows(t, "select g from "+name+" where match(body) against('original')"))
						require.Equal(t, []string{"11"}, rows(t, "select g from "+name+" where match(body) against('neighbortoken')"))
					}
					exec(t, "begin")
					t.Cleanup(func() { _, _ = conn.ExecContext(ctx, "rollback") })
					exec(t, update)
					check()
					exec(t, "rollback")
					require.Equal(t, []string{"2", "11"}, ids())
					require.Equal(t, []string{"2|original terms"}, rows(t, "select g,body from "+name+" force index(u) where u=10"))
					exec(t, update)
					check()
					if scalar {
						// The same key projection serves REPLACE, UPDATE and DELETE
						// RETURNING. Different same-typed columns must not select a
						// neighbor's entries or the final primary key.
						exec(t, "replace into "+name+"(a,u,body) values(5,10,'replaceunique')")
						require.Equal(t, []string{"6", "11"}, ids())
						exec(t, "replace into "+name+"(a,u,body) values(5,55,'replaceprimary')")
						require.Equal(t, []string{"6", "11"}, ids())
						require.Empty(t, rows(t, "select g from "+name+" where match(body) against('replaceunique')"))
						exec(t, "update "+name+" set a=7,body='updatedirect' where g=6")
						require.Equal(t, []string{"8", "11"}, ids())
						require.Equal(t, []string{"8|updatedirect"}, rows(t, "delete from "+name+" where g=8 returning g,body"))
						require.Equal(t, []string{"11"}, ids())
						// A new row carries no old PK, so it cannot delete any entry.
						exec(t, "insert into "+name+"(a,u,body) values(6,66,'freshterm') on duplicate key update body=values(body)")
						require.Equal(t, []string{"7", "11"}, ids())
					}
				})
			}
			t.Run("IVF original key and rollback", func(t *testing.T) {
				oldIVF := rows(t, "select @@experimental_ivf_index")[0]
				exec(t, "set experimental_ivf_index=1")
				t.Cleanup(func() { exec(t, "set experimental_ivf_index="+oldIVF) })
				exec(t, "create table vectors_move(a int,g int generated always as(a+1) stored primary key,u int unique,unused varchar(32),embedding vecf32(3),tail int,key ia(a),key it(tail))")
				exec(t, "insert into vectors_move(a,u,unused,embedding,tail) values(1,10,'unused','[1,1,1]',7),(10,77,'neighbor','[9,9,9]',41)")
				exec(t, "create index iv using ivfflat on vectors_move(embedding) lists=1 op_type 'vector_l2_ops'")
				hidden := rows(t, "select index_table_name from mo_catalog.mo_indexes where name='iv' and algo_table_type='entries' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='odku_systematic' and relname='vectors_move')")
				require.Len(t, hidden, 1)
				ids := func() []string {
					return rows(t, "select distinct __mo_index_pri_col from `"+hidden[0]+"` order by __mo_index_pri_col")
				}
				update := "insert into vectors_move(a,u,embedding,tail) values(1,10,'[100,100,100]',8) on duplicate key update a=3,embedding=values(embedding),tail=(select length(max(suffix))+2 from index_rhs)"
				check := func() {
					require.Equal(t, []string{"4", "11"}, ids())
					require.Equal(t, []string{"4|8"}, rows(t, "select g,tail from vectors_move force index(u) where u=10"))
					require.Equal(t, []string{"4"}, rows(t, "select g from vectors_move order by l2_distance(embedding,'[100,100,100]') limit 1"))
				}
				exec(t, "begin")
				t.Cleanup(func() { _, _ = conn.ExecContext(ctx, "rollback") })
				exec(t, update)
				check()
				exec(t, "rollback")
				require.Equal(t, []string{"2", "11"}, ids())
				exec(t, update)
				check()
			})
		})
		t.Run("float primary identities remain distinct through unique conflicts", func(t *testing.T) {
			exec(t, "create table float_original(id double primary key,u int unique,v int)")
			exec(t, "insert into float_original values(cast('-0' as double),10,100),(cast('0' as double),20,200)")
			require.Equal(t, []string{"10|100", "20|200"}, rows(t, "select u,v from float_original order by u"))
			// Probe both entries separately to verify each stored index payload
			// and the base row it addresses, before and after the updates.
			require.Equal(t, []string{"10|100"}, rows(t, "select u,v from float_original force index(u) where u=10"))
			require.Equal(t, []string{"20|200"}, rows(t, "select u,v from float_original force index(u) where u=20"))
			exec(t, "insert into float_original values(2,20,1),(3,10,1) on duplicate key update v=v+values(v)")
			require.Equal(t, []string{"10|101", "20|201"}, rows(t, "select u,v from float_original order by u"))
			require.Equal(t, []string{"10|101"}, rows(t, "select u,v from float_original force index(u) where u=10"))
			require.Equal(t, []string{"20|201"}, rows(t, "select u,v from float_original force index(u) where u=20"))
		})
		t.Run("recursive source retains every action", func(t *testing.T) {
			exec(t, "create table recursive_actions(id int primary key,v int)")
			exec(t, "insert into recursive_actions values(1,10)")
			require.Equal(t, []string{"6"}, rows(t, "with recursive r(n) as(select 1 union all select n+1 from r where n<3) select sum(n) from r"))
			exec(t, "insert into recursive_actions with recursive r(n) as(select 1 union all select n+1 from r where n<3) select 1,n from r on duplicate key update v=v+values(v)")
			require.Equal(t, []string{"1|16"}, rows(t, "select * from recursive_actions"))
		})
		t.Run("omitted nullable columns retain declared types", func(t *testing.T) {
			exec(t, "create table nullable_defaults (id int primary key, s varchar(128), d decimal(10,2), n int)")
			exec(t, "insert into nullable_defaults values (1,'old',12.34,0)")
			exec(t, "insert into nullable_defaults (id,n) values (1,1),(1,2),(2,3) on duplicate key update n=n+values(n), d=d+1")
			require.Equal(t, []string{"1|old|14.34|3", "2|NULL|NULL|3"}, rows(t, "select id,s,d,n from nullable_defaults order by id"))
			exec(t, "insert into nullable_defaults (id,n) values (1,4) on duplicate key update s=values(s), d=values(d)")
			require.Equal(t, []string{"1|NULL|NULL|3", "2|NULL|NULL|3"}, rows(t, "select id,s,d,n from nullable_defaults order by id"))
		})
		t.Run("fake primary key all unique constraints", func(t *testing.T) {
			exec(t, "create table fake (u1 int not null,u2 int not null,payload int not null,unique key uk_first(u1),unique key uk_second(u2))")
			exec(t, "insert into fake values (1,10,0),(2,20,0)")
			exec(t, "insert into fake values (1,99,7) on duplicate key update payload=values(payload)")
			exec(t, "insert into fake values (99,10,5) on duplicate key update payload=values(payload)")
			require.Equal(t, []string{"1|10|5", "2|20|0"}, rows(t, "select * from fake order by u1"))
		})
		t.Run("enum physical key", func(t *testing.T) {
			exec(t, "create table colors (id int primary key,color enum('red','blue') not null,payload int not null,unique key uk_color(color))")
			exec(t, "insert into colors values (1,'red',100),(2,'blue',200)")
			exec(t, "insert into colors values (99,'red',5) on duplicate key update payload=values(payload)")
			require.Equal(t, []string{"1|red|5", "2|blue|200"}, rows(t, "select * from colors order by id"))
		})
		schema := "(id int primary key,u1 int not null,u2 int not null,payload int not null,unique key uk_u1(u1),unique key uk_u2(u2))"
		t.Run("existing target order across lookups", func(t *testing.T) {
			for _, extra := range []bool{false, true} {
				exec(t, "drop table if exists target")
				exec(t, "create table target "+schema)
				exec(t, "insert into target values (1,10,100,0)")
				if extra {
					exec(t, "insert into target values (4,40,400,0),(5,50,500,0)")
				}
				exec(t, "insert into target values (2,10,200,1),(3,30,100,2) on duplicate key update payload=values(payload)")
				require.Equal(t, []string{"1|10|100|2"}, rows(t, "select * from target where id=1"))
			}
		})
		t.Run("earlier insert visible through either unique", func(t *testing.T) {
			for _, third := range []string{"(3,20,300,3)", "(3,30,200,3)"} {
				exec(t, "drop table if exists prior")
				exec(t, "create table prior "+schema)
				exec(t, "insert into prior values (1,10,100,1),(2,20,200,2),"+third+" on duplicate key update payload=values(payload)")
				require.Equal(t, []string{"1|10|100|1", "2|20|200|3"}, rows(t, "select * from prior order by id"))
			}
		})
		t.Run("constraint priority interleaves local and snapshot", func(t *testing.T) {
			exec(t, "create table priority "+schema)
			exec(t, "insert into priority values (9,90,900,0)")
			exec(t, "insert into priority values (1,10,100,1),(2,10,900,2) on duplicate key update payload=values(payload)")
			require.Equal(t, []string{"1|10|100|2", "9|90|900|0"}, rows(t, "select * from priority order by id"))
		})
		t.Run("update does not publish unused incoming keys", func(t *testing.T) {
			exec(t, "create table unused "+schema)
			exec(t, "insert into unused values (1,10,100,0)")
			exec(t, "insert into unused values (2,20,100,1),(3,20,300,2) on duplicate key update payload=values(payload)")
			require.Equal(t, []string{"1|10|100|1", "3|20|300|2"}, rows(t, "select * from unused order by id"))
		})
		t.Run("ordered assignments and column mapping", func(t *testing.T) {
			exec(t, "create table assignments (id int primary key,u int unique,a int,b int)")
			exec(t, "insert into assignments values (1,10,10,0)")
			exec(t, "insert into assignments(b,a,u,id) values (0,1,10,2),(0,2,10,3) on duplicate key update a=a+values(a),b=a,a=b*2")
			require.Equal(t, []string{"1|10|48|24"}, rows(t, "select * from assignments"))
		})
		t.Run("scalar RHS columns survive source arbitration", func(t *testing.T) {
			exec(t, "create table scalar_target(id int primary key,u int unique,v int)")
			exec(t, "create table scalar_input(n int)")
			exec(t, "insert into scalar_target values(1,10,0)")
			exec(t, "insert into scalar_input values(7)")
			exec(t, "insert into scalar_target values(2,10,1),(3,10,2) on duplicate key update v=v+values(v)+(select max(n) from scalar_input)")
			require.Equal(t, []string{"1|10|17"}, rows(t, "select * from scalar_target"))
			stmt, e := conn.PrepareContext(ctx, "insert into scalar_target values(?,10,?) on duplicate key update v=v+values(v)+(select ?+? from scalar_input)")
			require.NoError(t, e)
			defer stmt.Close()
			_, e = stmt.ExecContext(ctx, 4, 3, 4, 5)
			require.NoError(t, e)
			require.Equal(t, []string{"1|10|29"}, rows(t, "select * from scalar_target"))
		})

		t.Run("not null and check validate every action atomically", func(t *testing.T) {
			exec(t, "create table checked (id int primary key,u int unique,v int not null,constraint positive check(v>0))")
			exec(t, "insert into checked values (1,10,1)")
			for _, value := range []string{"-1", "null"} {
				_, e := conn.ExecContext(ctx, "insert into checked values (2,10,"+value+"),(3,10,2) on duplicate key update v=values(v)")
				message := "Check constraint 'positive' is violated"
				if value == "null" {
					message = "Column 'v' cannot be null"
				}
				constraintError(t, e, moerr.ER_CHECK_CONSTRAINT_VIOLATED, message)
				require.Equal(t, []string{"1|10|1"}, rows(t, "select * from checked"))
			}
			exec(t, "delete from checked")
			_, e := conn.ExecContext(ctx, "insert into checked values (1,10,-1),(2,10,2) on duplicate key update v=values(v)")
			constraintError(t, e, moerr.ER_CHECK_CONSTRAINT_VIOLATED, "Check constraint 'positive' is violated")
			require.Empty(t, rows(t, "select * from checked"))
			exec(t, "insert into checked values (10,100,1)")
			exec(t, "insert into checked values (11,100,2) on duplicate key update v=values(v)")
			require.Equal(t, []string{"10|100|2"}, rows(t, "select * from checked"))
			exec(t, "create table check_unknown (id int primary key,u int unique,v int,constraint positive check(v>0))")
			exec(t, "insert into check_unknown values(1,10,null),(2,10,2) on duplicate key update v=values(v)")
			require.Equal(t, []string{"1|10|2"}, rows(t, "select * from check_unknown"))
		})
		t.Run("incoming constraints before conflict update", func(t *testing.T) {
			for _, tc := range []struct {
				name, values, update, message string
				wantError                     bool
			}{
				{"primary check", "(1,90,1,-1)", "v=2", "positive_incoming", true},
				{"unique check", "(9,10,1,-1)", "v=2", "positive_incoming", true},
				{"unique not null repaired", "(9,10,1,NULL)", "v=2", "", false},
				{"unique foreign key repaired", "(9,10,999,1)", "pid=1,v=2", "", false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					exec(t, "drop table if exists incoming_checked")
					exec(t, "create table incoming_checked(id int primary key,u int,pid int,v int not null,unique key uk_u(u),constraint positive_incoming check(v>0),foreign key(pid) references parent(id))")
					exec(t, "insert into incoming_checked values(1,10,1,1),(4,40,1,1)")
					_, err := conn.ExecContext(ctx, "insert into incoming_checked values"+tc.values+" on duplicate key update "+tc.update)
					want := []string{"1|10|1|1", "4|40|1|1"}
					if tc.wantError {
						constraintError(t, err, moerr.ER_CHECK_CONSTRAINT_VIOLATED, tc.message)
					} else {
						require.NoError(t, err)
						want[0] = "1|10|1|2"
					}
					require.Equal(t, want, rows(t, "select id,u,pid,v from incoming_checked order by u"))
					require.Equal(t, want, rows(t, "select id,u,pid,v from incoming_checked force index(uk_u) where u in (10,40) order by u"))
				})
			}
		})
		t.Run("declarative not null and explicit check have distinct input rules", func(t *testing.T) {
			exec(t, "create table declared_nn (id int primary key,u int unique,v int not null)")
			exec(t, "insert into declared_nn values(1,10,1)")
			exec(t, "insert into declared_nn values(9,10,null) on duplicate key update v=2")
			require.Equal(t, []string{"1|10|2"}, rows(t, "select * from declared_nn force index(u) where u=10"))
			_, err := conn.ExecContext(ctx, "insert into declared_nn values(9,10,1) on duplicate key update v=null")
			constraintError(t, err, moerr.ER_CHECK_CONSTRAINT_VIOLATED, "Column 'v' cannot be null")
			_, err = conn.ExecContext(ctx, "insert into declared_nn values(2,20,null) on duplicate key update v=2")
			constraintError(t, err, moerr.ER_CHECK_CONSTRAINT_VIOLATED, "Column 'v' cannot be null")
			require.Equal(t, []string{"1|10|2"}, rows(t, "select * from declared_nn"))

			exec(t, "create table explicit_nn (id int primary key,v int,constraint require_v check(v is not null))")
			exec(t, "insert into explicit_nn values(1,1)")
			_, err = conn.ExecContext(ctx, "insert into explicit_nn values(1,null) on duplicate key update v=2")
			constraintError(t, err, moerr.ER_CHECK_CONSTRAINT_VIOLATED, "Check constraint 'require_v' is violated")
			require.Equal(t, []string{"1|1"}, rows(t, "select * from explicit_nn"))
		})
		t.Run("incoming not null across retained batches", func(t *testing.T) {
			exec(t, "create table incoming_batches(id bigint primary key,u int unique,v int not null)")
			exec(t, "insert into incoming_batches values(1,10,1)")
			statement := "insert into incoming_batches select result+100,10,if(result=8193,null,2) from generate_series(1,8193) g order by result on duplicate key update v=values(v)"
			_, err := conn.ExecContext(ctx, statement)
			constraintError(t, err, moerr.ER_CHECK_CONSTRAINT_VIOLATED, "Column 'v' cannot be null")
			require.Equal(t, []string{"1|10|1"}, rows(t, "select id,u,v from incoming_batches"))
			require.Equal(t, []string{"1|10|1"}, rows(t, "select id,u,v from incoming_batches force index(u) where u=10"))
			exec(t, "insert into incoming_batches select result+100,10,2 from generate_series(1,8193) g order by result on duplicate key update v=values(v)")
			require.Equal(t, []string{"1|10|2"}, rows(t, "select id,u,v from incoming_batches"))
		})
		t.Run("incoming generated check before conflict update", func(t *testing.T) {
			exec(t, "create table incoming_generated(id int primary key,u int unique,a int,g int generated always as(a*2) stored,constraint positive_generated_input check(g>0))")
			for _, tc := range []struct {
				name, values string
				wantError    bool
			}{
				{"primary invalid repaired", "(1,90,-1)", true},
				{"unique invalid repaired", "(9,10,-1)", true},
				{"unique null repaired", "(9,10,null)", false},
				{"unique valid", "(9,10,3)", false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					exec(t, "delete from incoming_generated")
					exec(t, "insert into incoming_generated(id,u,a) values(1,10,1),(4,40,1)")
					_, err := conn.ExecContext(ctx, "insert into incoming_generated(id,u,a) values"+tc.values+" on duplicate key update a=2")
					want := []string{"1|10|1|2", "4|40|1|2"}
					if tc.wantError {
						constraintError(t, err, moerr.ER_CHECK_CONSTRAINT_VIOLATED, "positive_generated_input")
					} else {
						require.NoError(t, err)
						want[0] = "1|10|2|4"
					}
					require.Equal(t, want, rows(t, "select id,u,a,g from incoming_generated order by u"))
					require.Equal(t, want, rows(t, "select id,u,a,g from incoming_generated force index(u) where u in (10,40) order by u"))
				})
			}
		})
		t.Run("constraint failure across source batches leaves no rows", func(t *testing.T) {
			exec(t, "create table late_check (id bigint primary key,u bigint not null,v bigint not null,unique key uk_u(u),constraint positive check(v>0))")
			// The invalid last input crosses the source batch boundary. Final
			// mutation records must remain unpublished when that action fails.
			_, e := conn.ExecContext(ctx, "insert into late_check select result+100,result+100,if(result=8193,-1,1) from generate_series(1,8193) g order by result on duplicate key update v=values(v)")
			constraintError(t, e, moerr.ER_CHECK_CONSTRAINT_VIOLATED, "Check constraint 'positive' is violated")
			require.Equal(t, []string{"0"}, rows(t, "select count(*) from late_check"))
			require.Equal(t, []string{"0"}, rows(t, "select count(*) from late_check force index(uk_u) where u>0"))
		})

		t.Run("nullable composite and prefix keys", func(t *testing.T) {
			exec(t, "create table nullable_keys (id int primary key,a int,b int,v int,unique key uk_ab(a,b))")
			exec(t, "insert into nullable_keys values (1,null,1,1),(2,null,1,2),(3,10,20,3),(4,10,20,4) on duplicate key update v=values(v)")
			require.Equal(t, []string{"1|NULL|1|1", "2|NULL|1|2", "3|10|20|4"}, rows(t, "select * from nullable_keys order by id"))
			exec(t, "create table prefix_keys (id int primary key,k varchar(32),v int,unique key uk_prefix(k(4)))")
			exec(t, "insert into prefix_keys values (1,'abcd-one',1),(2,'abcd-two',2) on duplicate key update v=values(v)")
			require.Equal(t, []string{"1|abcd-one|2"}, rows(t, "select * from prefix_keys"))
		})
		t.Run("prepared reuse and rollback", func(t *testing.T) {
			exec(t, "create table prepared_rows (id int primary key,u int unique,v int,constraint prepared_positive check(v>=0))")
			stmt, e := conn.PrepareContext(ctx, "insert into prepared_rows(id,u,v) values (?,?,?),(?,?,?) on duplicate key update v=v+values(v)")
			require.NoError(t, e)
			defer stmt.Close()
			tx, e := conn.BeginTx(ctx, nil)
			require.NoError(t, e)
			t.Cleanup(func() { _ = tx.Rollback() })
			txStmt := tx.StmtContext(ctx, stmt)
			defer txStmt.Close()
			_, e = txStmt.ExecContext(ctx, 11, 10, 1, 12, 10, 2)
			require.NoError(t, e)
			require.NoError(t, tx.Rollback())
			require.Empty(t, rows(t, "select * from prepared_rows"))
			_, e = stmt.ExecContext(ctx, 21, 10, 3, 22, 10, 4)
			require.NoError(t, e)
			require.Equal(t, []string{"21|10|7"}, rows(t, "select * from prepared_rows"))
			_, e = stmt.ExecContext(ctx, 31, 10, 5, 32, 10, 6)
			require.NoError(t, e)
			require.Equal(t, []string{"21|10|18"}, rows(t, "select * from prepared_rows"))
			incoming, e := conn.PrepareContext(ctx, "insert into prepared_rows(id,u,v) values (?,?,?),(?,?,?) on duplicate key update v=values(v)")
			require.NoError(t, e)
			defer incoming.Close()
			_, e = incoming.ExecContext(ctx, 41, 10, -1, 42, 10, 4)
			constraintError(t, e, moerr.ER_CHECK_CONSTRAINT_VIOLATED, "Check constraint 'prepared_positive' is violated")
			require.Equal(t, []string{"21|10|18"}, rows(t, "select * from prepared_rows"))
			for _, value := range []int{4, 6} {
				_, e = incoming.ExecContext(ctx, 41, 10, value-1, 42, 10, value)
				require.NoError(t, e)
				require.Equal(t, []string{fmt.Sprintf("21|10|%d", value)}, rows(t, "select * from prepared_rows"))
			}
		})
		t.Run("insert select preserves explicit source order", func(t *testing.T) {
			exec(t, "create table ordered_select (id int primary key,u int unique,v int,payload varchar(64),mirror varchar(64))")
			for _, existing := range []bool{true, false} {
				exec(t, "truncate table ordered_select")
				firstID := 11
				if existing {
					exec(t, "insert into ordered_select values (1,10,0,'seed','seed')")
					firstID = 1
				}
				// Shared incoming vectors, NULLs and distinct varlen values must
				// remain correct across the build-batch boundary and group reuse.
				exec(t, "insert into ordered_select select result+10,10,result,if(result%2=0,null,concat('row-',result)),'unused' from generate_series(1,8193) g order by result on duplicate key update v=values(v),payload=values(payload),mirror=values(payload)")
				require.Equal(t, []string{fmt.Sprintf("%d|10|8193|row-8193|row-8193", firstID)}, rows(t, "select * from ordered_select"))
			}
		})
		t.Run("generated images use materialized assignments", func(t *testing.T) {
			exec(t, "create table generated_values (id int primary key,u int unique,a bigint,g bigint generated always as (a*2) stored)")
			exec(t, "insert into generated_values(id,u,a) values (1,10,0)")
			exec(t, "insert into generated_values(id,u,a) values (2,10,99) on duplicate key update a=a+1")
			require.Equal(t, []string{"1|2"}, rows(t, "select a,g from generated_values"))
			exec(t, "create table generated_volatile (id int primary key,a varchar(64),g varchar(128) generated always as (concat(a,'/g')) stored)")
			exec(t, "insert into generated_volatile(id,a) values (1,'seed')")
			exec(t, "insert into generated_volatile(id,a) values (1,'unused') on duplicate key update a=uuid()")
			require.Equal(t, []string{"1|1"}, rows(t, "select a <> 'seed',g=concat(a,'/g') from generated_volatile"))
			exec(t, "create table generated_checked (id int primary key,a int,g int generated always as(a*2) stored,constraint small_g check(g<10))")
			exec(t, "insert into generated_checked(id,a) values(1,1)")
			exec(t, "insert into generated_checked(id,a) values(1,2) on duplicate key update a=values(a)")
			require.Equal(t, []string{"1|2|4"}, rows(t, "select * from generated_checked"))
			_, generatedError := conn.ExecContext(ctx, "insert into generated_checked(id,a) values(1,8),(1,2) on duplicate key update a=values(a)")
			constraintError(t, generatedError, moerr.ER_CHECK_CONSTRAINT_VIOLATED, "Check constraint 'small_g' is violated")
			require.Equal(t, []string{"1|2|4"}, rows(t, "select * from generated_checked"))
		})
		t.Run("generated unique key actions follow current keys", func(t *testing.T) {
			exec(t, "create table generated_identity (id int primary key,a int,b int,g int generated always as (a+1) stored,unique key ug(g))")
			exec(t, "insert into generated_identity(id,a,b) values (1,1,0),(9,5,9)")
			exec(t, "insert into generated_identity(id,a,b) values (2,1,7) on duplicate key update b=values(b)")
			exec(t, "insert into generated_identity(id,a,b) values (3,1,0),(4,1,8) on duplicate key update a=a+1")
			base := []string{"1|2|7|3", "4|1|8|2", "9|5|9|6"}
			indexed := []string{"4|1|8|2", "1|2|7|3", "9|5|9|6"}
			baseQuery := "select id,a,b,g from generated_identity order by id"
			indexQuery := "select id,a,b,g from generated_identity force index(ug) where g in (2,3,6) order by g"
			require.Equal(t, base, rows(t, baseQuery))
			require.Equal(t, indexed, rows(t, indexQuery))
			_, e := conn.ExecContext(ctx, "insert into generated_identity(id,a,b) values (5,2,0) on duplicate key update a=5")
			constraintError(t, e, moerr.ER_DUP_ENTRY, "Duplicate entry")
			require.Equal(t, base, rows(t, baseQuery))
			require.Equal(t, indexed, rows(t, indexQuery))
		})
		t.Run("generated keys retain prior transaction writes and deletes", func(t *testing.T) {
			exec(t, "set @odku_prior_saved_cte=@@session.cte_max_memory_bytes")
			exec(t, "set session cte_max_memory_bytes=1")
			defer func() { _, _ = conn.ExecContext(ctx, "set session cte_max_memory_bytes=@odku_prior_saved_cte") }()
			for _, flush := range []bool{false, true} {
				t.Run(fmt.Sprintf("persisted=%t", flush), func(t *testing.T) {
					name := fmt.Sprintf("generated_prior_%t", flush)
					exec(t, "create table "+name+"(A int,B int generated always as(A*2) stored,U int,Payload varchar(8) default 'keep',primary key(B),unique key uk_u(U))")
					if flush {
						if fault.Enable() {
							defer fault.Disable()
						}
						remove, e := objectio.SimpleInject(objectio.FJ_CNWorkspaceForceFlush)
						require.NoError(t, e)
						defer remove()
					}
					exec(t, "begin")
					defer func() { _, _ = conn.ExecContext(ctx, "rollback") }()
					exec(t, "insert into "+name+"(a,u) values(1,11),(4,44)")
					exec(t, "delete from "+name+" where u=44")
					exec(t, "insert into "+name+"(a,u) values(6,66)")
					exec(t, "update "+name+" set Payload='prior' where U=11")
					exec(t, "insert into "+name+"(a,u) values(1,101),(1,22),(2,103),(4,104) on duplicate key update a=values(u)-99")
					base := "select a,b,u from " + name + " order by a"
					indexed := "select a,b,u from " + name + " force index(uk_u) where u in(11,22,66) order by u"
					want := []string{"1|2|22", "5|10|11", "6|12|66"}
					require.Equal(t, want, rows(t, base))
					require.Equal(t, []string{"5|10|11", "1|2|22", "6|12|66"}, rows(t, indexed))
					_, e := conn.ExecContext(ctx, "insert into "+name+"(a,u) values(5,105) on duplicate key update a=values(u)-99")
					constraintError(t, e, moerr.ER_DUP_ENTRY, "Duplicate entry")
					require.Equal(t, want, rows(t, base), "failed action preserves prior statements")
					statement, e := conn.PrepareContext(ctx, "insert into "+name+"(a,u) values(?,?) on duplicate key update a=values(u)-99")
					require.NoError(t, e)
					defer statement.Close()
					_, e = statement.ExecContext(ctx, 5, 106)
					require.NoError(t, e)
					_, e = statement.ExecContext(ctx, 7, 107)
					require.NoError(t, e)
					exec(t, "commit")
					require.Equal(t, []string{"1|2|22", "6|12|66", "8|16|11"}, rows(t, base))
					require.Equal(t, []string{"8|16|11", "1|2|22", "6|12|66"}, rows(t, indexed))
					require.Empty(t, rows(t, "select a,b,u from "+name+" where b=10"))
					require.Equal(t, []string{"11|prior", "22|keep", "66|keep"}, rows(t, "select U,Payload from "+name+" order by U"))
				})
			}
		})
		t.Run("float probes preserve generated-key moves and rollback", func(t *testing.T) {
			for _, persisted := range []bool{false, true} {
				t.Run(fmt.Sprintf("persisted=%t", persisted), func(t *testing.T) {
					name := fmt.Sprintf("float_prior_%t", persisted)
					exec(t, "create table "+name+"(id double primary key,u int unique,a int,g int generated always as(a+1) stored,unique key ug(g))")
					exec(t, "insert into "+name+"(id,u,a) values(cast('-0' as double),10,10),(cast('0' as double),20,20),(cast('NaN' as double),30,30),(cast('+Inf' as double),40,40),(cast('-Inf' as double),50,50)")
					if persisted {
						exec(t, "select mo_ctl('dn','flush','odku_systematic."+name+".0')")
					}
					query := "select u,a,g from " + name + " order by u"
					seed := []string{"10|10|11", "20|20|21", "30|30|31", "40|40|41", "50|50|51"}
					want := []string{"10|11|12", "20|21|22", "30|31|32", "40|41|42", "50|51|52"}
					_, e := conn.ExecContext(ctx, "insert into "+name+"(id,u,a) values(100,10,10) on duplicate key update a=20")
					constraintError(t, e, moerr.ER_DUP_ENTRY, "Duplicate entry")
					require.Equal(t, seed, rows(t, query), "failed generated-key collision preserves both zeros and exceptional originals")
					statement := "insert into " + name + "(id,u,a) values(100,10,10),(101,20,20),(102,30,30),(103,40,40),(104,50,50) on duplicate key update a=a+1"
					exec(t, "begin")
					defer func() { _, _ = conn.ExecContext(ctx, "rollback") }()
					exec(t, statement)
					require.Equal(t, want, rows(t, query))
					exec(t, "rollback")
					require.Equal(t, seed, rows(t, query))
					exec(t, statement)
					require.Equal(t, want, rows(t, query))
					require.Equal(t, want, rows(t, "select u,a,g from "+name+" force index(u) where u in(10,20,30,40,50) order by u"))
				})
			}
			exec(t, "create table float_scaled(id int primary key,a int,g float(10,2) generated always as(a/1000) stored,unique key ug(g))")
			exec(t, "insert into float_scaled(id,a) values(1,1231),(9,2001)")
			exec(t, "select mo_ctl('dn','flush','odku_systematic.float_scaled.0')")
			seed := rows(t, "select id,a from float_scaled order by id")
			statement := "insert into float_scaled(id,a) values(2,1234),(3,2234) on duplicate key update a=a+1000"
			exec(t, "begin")
			defer func() { _, _ = conn.ExecContext(ctx, "rollback") }()
			exec(t, statement)
			require.Equal(t, []string{"1|3231", "9|2001"}, rows(t, "select id,a from float_scaled order by id"), "distinct raw floats in one normalized key class find the same owner")
			exec(t, "rollback")
			require.Equal(t, seed, rows(t, "select id,a from float_scaled order by id"))
			_, e := conn.ExecContext(ctx, "insert into float_scaled(id,a) values(2,1234) on duplicate key update a=2004")
			constraintError(t, e, moerr.ER_DUP_ENTRY, "Duplicate entry")
			require.Equal(t, seed, rows(t, "select id,a from float_scaled order by id"))
		})
		t.Run("generated unique keys can enter and leave NULL", func(t *testing.T) {
			exec(t, "create table generated_null_key(id int primary key,a int,g int generated always as(a+1) stored,unique key ug(g))")
			exec(t, "insert into generated_null_key(id,a) values(1,1),(2,null)")
			exec(t, "insert into generated_null_key(id,a) values(1,null),(3,1),(2,null) on duplicate key update a=values(a)")
			want := []string{"1|NULL|NULL", "2|NULL|NULL", "3|1|2"}
			require.Equal(t, want, rows(t, "select id,a,g from generated_null_key order by id"))
			require.Equal(t, []string{"3|1|2"}, rows(t, "select id,a,g from generated_null_key force index(ug) where g=2"))
			_, e := conn.ExecContext(ctx, "insert into generated_null_key(id,a) values(2,1) on duplicate key update a=values(a)")
			constraintError(t, e, moerr.ER_DUP_ENTRY, "Duplicate entry")
			require.Equal(t, want, rows(t, "select id,a,g from generated_null_key order by id"))
		})

		t.Run("generated final keys and originals survive optimistic competing commits", func(t *testing.T) {
			rt := moruntime.ServiceRuntime(cn.ServiceID())
			oldMode, hadMode := rt.GetGlobalVariables(moruntime.TxnMode)
			oldIsolation, hadIsolation := rt.GetGlobalVariables(moruntime.TxnIsolation)
			defer func() {
				if hadMode {
					rt.SetGlobalVariables(moruntime.TxnMode, oldMode)
				} else {
					rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Pessimistic)
				}
				if hadIsolation {
					rt.SetGlobalVariables(moruntime.TxnIsolation, oldIsolation)
				} else {
					rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_RC)
				}
			}()
			for _, persisted := range []bool{false, true} {
				for _, conflict := range []string{"original row", "final primary", "final unique"} {
					t.Run(fmt.Sprintf("%s/persisted=%t", conflict, persisted), func(t *testing.T) {
						competeCtx, stop := context.WithTimeout(ctx, 20*time.Second)
						defer stop()
						// Schema setup uses the shared cluster's normal mode. Only
						// the competing DML transactions use optimistic SI.
						rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Pessimistic)
						rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_RC)
						name := fmt.Sprintf("generated_optimistic_%s_%t", strings.ReplaceAll(conflict, " ", "_"), persisted)
						key := "primary key(g),unique key uk_u(u)"
						if conflict == "final unique" {
							key = "primary key(u),unique key ug(g)"
						}
						exec(t, "create table "+name+"(a int,g int generated always as(a+1) stored,u int,v int,"+key+")")
						exec(t, "insert into "+name+"(a,u,v) values(1,10,0)")
						rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Optimistic)
						rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_SI)
						defer func() {
							rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Pessimistic)
							rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_RC)
						}()
						if persisted {
							if fault.Enable() {
								defer fault.Disable()
							}
							remove, e := objectio.SimpleInject(objectio.FJ_CNWorkspaceForceFlush)
							require.NoError(t, e)
							defer remove()
						}
						competing, e := db.Conn(competeCtx)
						require.NoError(t, e)
						defer competing.Close()
						_, e = competing.ExecContext(competeCtx, "use odku_systematic")
						require.NoError(t, e)
						first, e := conn.BeginTx(competeCtx, nil)
						require.NoError(t, e)
						defer first.Rollback()
						defer func() { _, _ = conn.ExecContext(ctx, "rollback") }()
						// Stage the entire generated-key move before the competitor
						// commits. An accidental pessimistic run cannot pass this order.
						_, e = first.ExecContext(competeCtx, "insert into "+name+"(a,u,v) values(1,10,7) on duplicate key update a=3,v=values(v)")
						require.NoError(t, e)
						var currentKey int
						require.NoError(t, first.QueryRowContext(competeCtx, "select g from "+name+" where u=10").Scan(&currentKey))
						require.Equal(t, 4, currentKey, "the first transaction stages its generated-key move")
						second, e := competing.BeginTx(competeCtx, nil)
						require.NoError(t, e)
						defer second.Rollback()
						require.NoError(t, second.QueryRowContext(competeCtx, "select g from "+name+" where u=10").Scan(&currentKey))
						require.Equal(t, 2, currentKey, "the competitor sees the committed original")
						optimisticUsers := 0
						cn.RawService().(cnservice.Service).GetTxnClient().IterTxns(func(v txnclient.TxnOverview) bool {
							if v.UserTxn && v.Meta.Mode == pbtxn.TxnMode_Optimistic {
								optimisticUsers++
							}
							return true
						})
						require.GreaterOrEqual(t, optimisticUsers, 2, "the real CN metadata must contain both optimistic transactions")
						var requestedPolicy int
						require.NoError(t, first.QueryRowContext(competeCtx, "select @@mo_pk_check_by_dn").Scan(&requestedPolicy))
						require.Zero(t, requestedPolicy, "exercise default selection rather than forcing a stricter session policy")
						statement := "insert into " + name + "(a,u,v) values(3,20,9)"
						want := []string{"1|2|10|0", "3|4|20|9"}
						if conflict == "original row" {
							statement = "update " + name + " set v=9 where u=10"
							want = []string{"1|2|10|9"}
						}
						result, e := second.ExecContext(competeCtx, statement)
						require.NoError(t, e)
						affected, e := result.RowsAffected()
						require.NoError(t, e)
						require.Positive(t, affected, "the competitor must perform a real write")
						require.NoError(t, second.Commit())
						commitErr := first.Commit()
						_, e = conn.ExecContext(ctx, "rollback")
						require.NoError(t, e)
						// SacrificingFreshness permits another session to open an older
						// SI snapshot. Read on the winner's session: its commit timestamp
						// is the causal visibility floor, with no sleeps or polling.
						actual := rowsFrom(t, competing, "select a,g,u,v from "+name+" order by u")
						t.Logf("controlled optimistic commit: error=%v, committed rows=%v", commitErr, actual)
						require.Error(t, commitErr, "TN must reject the stale RowID or competing final key")
						require.Equal(t, want, actual)
						index := "uk_u"
						column := "u"
						if conflict == "final unique" {
							index = "ug"
							column = "g"
						}
						require.Equal(t, want, rowsFrom(t, competing, "select a,g,u,v from "+name+" force index("+index+") where "+column+" in(2,4,10,20) order by u"))
					})
				}
			}
			// The TN policy normalization is shared by all optimistic writes.
			// Cover ordinary DML through real commits independently of ODKU.
			for i, tc := range []struct {
				name       string
				first      []string
				second     string
				want       []string
				conflict   bool
				persisted  bool
				fullPolicy bool
			}{
				{"insert conflict memory", []string{"insert into $t values(3,30,10)"}, "insert into $t values(3,30,20)", []string{"1|1", "2|2", "3|20"}, true, false, false},
				{"insert conflict persisted", []string{"insert into $t values(3,30,10)"}, "insert into $t values(3,30,20)", []string{"1|1", "2|2", "3|20"}, true, true, false},
				{"unique conflict persisted", []string{"insert into $t values(3,30,10)"}, "insert into $t values(4,30,20)", []string{"1|1", "2|2", "4|20"}, true, true, false},
				{"insert conflict explicit full", []string{"insert into $t values(3,30,10)"}, "insert into $t values(3,30,20)", []string{"1|1", "2|2", "3|20"}, true, true, true},
				{"disjoint inserts persisted", []string{"insert into $t values(3,30,10)"}, "insert into $t values(4,40,20)", []string{"1|1", "2|2", "3|10", "4|20"}, false, true, false},
				{"update conflict", []string{"update $t set v=11 where id=1"}, "update $t set v=21 where id=1", []string{"1|21", "2|2"}, true, false, false},
				{"delete conflict", []string{"delete from $t where id=1"}, "update $t set v=21 where id=1", []string{"1|21", "2|2"}, true, true, false},
				{"disjoint update delete", []string{"update $t set v=11 where id=1"}, "delete from $t where id=2", []string{"1|11"}, false, true, false},
				{"workspace replace same key", []string{"update $t set v=11 where id=1", "delete from $t where id=1", "insert into $t values(1,10,99)"}, "update $t set v=22 where id=2", []string{"1|99", "2|22"}, false, true, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Pessimistic)
					rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_RC)
					name := fmt.Sprintf("ordinary_optimistic_%d", i)
					exec(t, "create table "+name+"(id int primary key,u int unique,v int)")
					exec(t, "insert into "+name+" values(1,10,1),(2,20,2)")
					rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Optimistic)
					rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_SI)
					defer func() {
						rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Pessimistic)
						rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_RC)
					}()
					if tc.persisted {
						if fault.Enable() {
							defer fault.Disable()
						}
						remove, err := objectio.SimpleInject(objectio.FJ_CNWorkspaceForceFlush)
						require.NoError(t, err)
						defer remove()
					}
					competeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
					defer cancel()
					competing, err := db.Conn(competeCtx)
					require.NoError(t, err)
					defer competing.Close()
					_, err = competing.ExecContext(competeCtx, "use odku_systematic")
					require.NoError(t, err)
					if tc.fullPolicy {
						exec(t, "set mo_pk_check_by_dn=1")
						defer cleanupODKUSQL(t, conn, "set mo_pk_check_by_dn=0")
					}
					first, err := conn.BeginTx(competeCtx, nil)
					require.NoError(t, err)
					defer first.Rollback()
					var policy int
					require.NoError(t, first.QueryRowContext(competeCtx, "select @@mo_pk_check_by_dn").Scan(&policy))
					require.Equal(t, tc.fullPolicy, policy == 1)
					for _, statement := range tc.first {
						_, err = first.ExecContext(competeCtx, strings.ReplaceAll(statement, "$t", name))
						require.NoError(t, err)
					}
					second, err := competing.BeginTx(competeCtx, nil)
					require.NoError(t, err)
					defer second.Rollback()
					var original int
					require.NoError(t, second.QueryRowContext(competeCtx, "select v from "+name+" where id=1").Scan(&original))
					require.Equal(t, 1, original)
					_, err = second.ExecContext(competeCtx, strings.ReplaceAll(tc.second, "$t", name))
					require.NoError(t, err)
					require.NoError(t, second.Commit())
					err = first.Commit()
					if tc.conflict {
						var serverError *mysqlDriver.MySQLError
						require.ErrorAs(t, err, &serverError)
						require.True(t, serverError.Number == 1062 || strings.Contains(strings.ToLower(serverError.Message), "conflict"), "%v", err)
					} else {
						require.NoError(t, err)
					}
					_, err = conn.ExecContext(competeCtx, "rollback")
					require.NoError(t, err)
					// Each reader has observed its own commit; use the transaction
					// that committed last as the causal visibility floor.
					reader := conn
					if tc.conflict {
						reader = competing
					}
					require.Equal(t, tc.want, rowsFrom(t, reader, "select id,v from "+name+" order by id"))
					require.Equal(t, tc.want, rowsFrom(t, reader, "select id,v from "+name+" force index(u) where u between 0 and 100 order by id"))
				})
			}
		})
		t.Run("explicit key assignments remain unsupported", func(t *testing.T) {
			exec(t, "create table direct_key (id int primary key,u int unique,v int)")
			exec(t, "insert into direct_key values (1,10,1)")
			_, e := conn.ExecContext(ctx, "insert into direct_key values (1,10,2) on duplicate key update id=2")
			require.ErrorContains(t, e, "update primary key on duplicate")
			_, e = conn.ExecContext(ctx, "insert into direct_key values (1,10,2) on duplicate key update u=20")
			require.ErrorContains(t, e, "update unique key on duplicate")
			require.Equal(t, []string{"1|10|1"}, rows(t, "select * from direct_key"))
			require.Equal(t, []string{"1|10|1"}, rows(t, "select * from direct_key force index(u) where u=10"))
			exec(t, "set @odku_direct_saved_hints=@@session.optimizer_hints")
			defer func() { _, _ = conn.ExecContext(ctx, "set session optimizer_hints=@odku_direct_saved_hints") }()
			for i, parts := range []string{"g,u", "u,g"} {
				name := fmt.Sprintf("direct_generated_part_%d", i)
				exec(t, "create table "+name+"(id int primary key,a int,u int,g int generated always as(a+1) stored,unique key ug("+parts+"))")
				exec(t, "insert into "+name+"(id,a,u) values(1,1,7)")
				for _, hint := range []string{"", "skipDedup=1"} {
					exec(t, "set session optimizer_hints='"+hint+"'")
					_, e := conn.ExecContext(ctx, "insert into "+name+"(id,a,u) values(1,2,8) on duplicate key update a=values(a),u=values(u)")
					require.ErrorContains(t, e, "update unique key on duplicate")
					require.Equal(t, []string{"1|1|7|2"}, rows(t, "select * from "+name))
					require.Equal(t, []string{"1|1|7|2"}, rows(t, "select * from "+name+" force index(ug) where g=2 and u=7"))
				}
			}

		})
		t.Run("generated key collision rejects untouched row", func(t *testing.T) {
			cases := []struct {
				name, create, seed, update, base, index string
				want                                    []string
			}{
				{
					name:   "primary",
					create: "create table generated_pk_single_collision(a int,g int generated always as(a*2) stored,u int,primary key(g),unique key uk_u(u))",
					seed:   "insert into generated_pk_single_collision(a,u) values(1,11),(5,55)",
					update: "insert into generated_pk_single_collision(a,u) values(1,11) on duplicate key update a=5",
					base:   "select a,g,u from generated_pk_single_collision order by u",
					index:  "select a,g,u from generated_pk_single_collision force index(uk_u) where u in (11,55) order by u",
					want:   []string{"1|2|11", "5|10|55"},
				},
				{
					name:   "unique",
					create: "create table generated_uk_single_collision(id int primary key,a int,g int generated always as(a+1) stored,unique key ug(g))",
					seed:   "insert into generated_uk_single_collision(id,a) values(1,1),(2,5)",
					update: "insert into generated_uk_single_collision(id,a) values(1,1) on duplicate key update a=5",
					base:   "select id,a,g from generated_uk_single_collision order by id",
					index:  "select id,a,g from generated_uk_single_collision force index(ug) where g in (2,6) order by id",
					want:   []string{"1|1|2", "2|5|6"},
				},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					exec(t, tc.create)
					exec(t, tc.seed)
					_, e := conn.ExecContext(ctx, tc.update)
					require.Equal(t, tc.want, rows(t, tc.base))
					require.Equal(t, tc.want, rows(t, tc.index))
					constraintError(t, e, moerr.ER_DUP_ENTRY, "Duplicate entry")
				})
			}
		})
		t.Run("generated primary key actions follow current keys", func(t *testing.T) {
			const schema = "(a int,b int generated always as(a*2) stored,u int,primary key(b),unique key uk_u(u),check(a>0))"
			t.Run("composite key and indexes follow the final image", func(t *testing.T) {
				exec(t, "create table generated_composite_key(a int,g int generated always as(a*2) stored,k int,u int,primary key(g,k),unique key uk_u(u),key ix_k(k))")
				exec(t, "insert into generated_composite_key(a,k,u) values(1,9,11)")
				exec(t, "insert into generated_composite_key(a,k,u) values(1,9,11) on duplicate key update a=5")
				want := []string{"5|10|9|11"}
				require.Equal(t, want, rows(t, "select a,g,k,u from generated_composite_key"))
				require.Equal(t, want, rows(t, "select a,g,k,u from generated_composite_key where g=10 and k=9"))
				require.Empty(t, rows(t, "select a,g,k,u from generated_composite_key where g=2 and k=9"))
				require.Equal(t, want, rows(t, "select a,g,k,u from generated_composite_key force index(uk_u) where u=11"))
				require.Equal(t, want, rows(t, "select a,g,k,u from generated_composite_key force index(ix_k) where k=9"))
			})
			t.Run("move reuse and update", func(t *testing.T) {
				exec(t, "create table generated_key_move "+schema)
				exec(t, "insert into generated_key_move(a,u) values(1,11),(4,44)")
				exec(t, "insert into generated_key_move(a,u) values(1,101),(1,22),(2,102) on duplicate key update a=values(u)-99")
				require.Equal(t, []string{"1|2|22", "3|6|11", "4|8|44"}, rows(t, "select a,b,u from generated_key_move order by a"))
				require.Equal(t, []string{"3|6|11", "1|2|22", "4|8|44"}, rows(t, "select a,b,u from generated_key_move force index(uk_u) where u in (11,22,44) order by u"))
			})
			t.Run("collision rolls back only the statement", func(t *testing.T) {
				exec(t, "create table generated_key_collision "+schema)
				exec(t, "insert into generated_key_collision(a,u) values(1,11),(4,44)")
				exec(t, "create table generated_key_prior(id int primary key,v int)")
				exec(t, "insert into generated_key_prior values(1,0)")
				exec(t, "begin")
				defer func() { _, _ = conn.ExecContext(ctx, "rollback") }()
				exec(t, "update generated_key_prior set v=7 where id=1")
				_, e := conn.ExecContext(ctx, "insert into generated_key_collision(a,u) values(1,101),(1,22),(2,102),(3,103) on duplicate key update a=values(u)-99")
				var serverError *mysqlDriver.MySQLError
				require.ErrorAs(t, e, &serverError)
				require.Equal(t, uint16(1062), serverError.Number)
				exec(t, "commit")
				require.Equal(t, []string{"1|2|11", "4|8|44"}, rows(t, "select a,b,u from generated_key_collision order by a"))
				require.Equal(t, []string{"1|2|11", "4|8|44"}, rows(t, "select a,b,u from generated_key_collision force index(uk_u) where u in (11,44) order by u"))
				require.Equal(t, []string{"7"}, rows(t, "select v from generated_key_prior where id=1"))
			})
		})
		t.Run("skip dedup target constraints", func(t *testing.T) {
			exec(t, "set session optimizer_hints = 'skipDedup=1'")
			t.Cleanup(func() { cleanupODKUSQL(t, conn, "set session optimizer_hints = ''") })
			exec(t, "create table hint_unique (u1 int unique,u2 int unique,v int)")
			exec(t, "insert into hint_unique values(10,100,1)")
			exec(t, "insert into hint_unique values(10,999,5) on duplicate key update v=values(v)")
			_, e := conn.ExecContext(ctx, "insert into hint_unique values(99,100,8) on duplicate key update v=values(v)")
			constraintError(t, e, moerr.ER_DUP_ENTRY, "Duplicate entry")
			require.Equal(t, []string{"10|100|5"}, rows(t, "select u1,u2,v from hint_unique force index(u2) where u2=100"))

			exec(t, "create table hint_nullable (u1 int unique,u2 int unique,v int)")
			exec(t, "insert into hint_nullable(u2,v) values(7,1)")
			exec(t, "insert into hint_nullable(u2,v) values(7,8) on duplicate key update v=values(v)")
			require.Equal(t, []string{"NULL|7|8"}, rows(t, "select u1,u2,v from hint_nullable force index(u2) where u2=7"))
		})
		t.Run("generated assignment modes", func(t *testing.T) {
			rt := moruntime.ServiceRuntime(cn.ServiceID())
			oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
			exec(t, "set @odku_saved_hints = @@session.optimizer_hints")
			t.Cleanup(func() {
				cleanupODKUSQL(t, conn, "set session optimizer_hints = @odku_saved_hints")
				if hadHints {
					rt.SetGlobalVariables("optimizer_hints", oldHints)
				} else if current, ok := rt.GetGlobalVariables("optimizer_hints"); ok {
					rt.CompareAndDeleteGlobalVariables("optimizer_hints", current)
				}
			})
			for _, mode := range []struct{ name, hint string }{{"ordered", ""}, {"skip_dedup", "skipDedup=1"}} {
				t.Run(mode.name, func(t *testing.T) {
					exec(t, "set session optimizer_hints = '"+mode.hint+"'")
					table := "generated_mode_" + mode.name
					exec(t, "create table "+table+" (id int primary key,a int,b int,g int generated always as(a*10+b) stored,h int generated always as(g+1) stored)")
					exec(t, "insert into "+table+"(id,a,b) values(1,1,0)")
					exec(t, "insert into "+table+"(id,a,b) values(1,8,8) on duplicate key update a=a+1,b=a")
					require.Equal(t, []string{"2|2|22|23"}, rows(t, "select a,b,g,h from "+table))
					exec(t, "insert into "+table+"(id,a,b) values(1,8,8),(1,8,8) on duplicate key update a=a+1,b=a")
					require.Equal(t, []string{"4|4|44|45"}, rows(t, "select a,b,g,h from "+table))
					exec(t, "insert into "+table+"(id,a,b) values(1,8,8) on duplicate key update a=a+1,a=a+1,b=a")
					require.Equal(t, []string{"6|6|66|67"}, rows(t, "select a,b,g,h from "+table))
					exec(t, "insert into "+table+"(id,a,b) values(1,9,0) on duplicate key update a=values(a),b=a")
					require.Equal(t, []string{"9|9|99|100"}, rows(t, "select a,b,g,h from "+table))
					stmt, e := conn.PrepareContext(ctx, "insert into "+table+"(id,a,b) values (?,?,?) on duplicate key update a=?,b=a")
					require.NoError(t, e)
					t.Cleanup(func() { require.NoError(t, stmt.Close()) })
					_, e = stmt.ExecContext(ctx, 1, 0, 0, 7)
					require.NoError(t, e)
					require.Equal(t, []string{"7|7|77|78"}, rows(t, "select a,b,g,h from "+table))
					exec(t, "create table "+table+"_source (v int)")
					exec(t, "insert into "+table+"_source values (11)")
					exec(t, "insert into "+table+"(id,a,b) values(1,0,0) on duplicate key update a=(select max(v) from "+table+"_source),b=a")
					require.Equal(t, []string{"11|11|121|122"}, rows(t, "select a,b,g,h from "+table))

					fresh := table + "_fresh"
					exec(t, "create table "+fresh+" (id int primary key,a int,b int,g int generated always as(a*10+b) stored,h int generated always as(g+1) stored)")
					exec(t, "insert into "+fresh+"(id,a,b) values(2,1,0),(2,0,0),(2,0,0) on duplicate key update a=a+1,b=a")
					require.Equal(t, []string{"3|3|33|34"}, rows(t, "select a,b,g,h from "+fresh))

					volatile := table + "_volatile"
					exec(t, "create table "+volatile+" (id int primary key,a varchar(64),g varchar(100) generated always as(concat(a,'/g')) stored,h varchar(120) generated always as(concat(g,'/h')) stored)")
					exec(t, "insert into "+volatile+"(id,a) values(1,'seed')")
					exec(t, "insert into "+volatile+"(id,a) values(1,'unused') on duplicate key update a=uuid()")
					require.Equal(t, []string{"1|1|1"}, rows(t, "select a<>'seed',g=concat(a,'/g'),h=concat(g,'/h') from "+volatile))

					defaulted := table + "_defaulted"
					exec(t, "create table "+defaulted+" (id int primary key,a varchar(64) default (uuid()),b int,g varchar(100) generated always as(concat(a,'/g')) stored,h varchar(120) generated always as(concat(g,'/h')) stored,constraint generated_consistent check(g=concat(a,'/g') and h=concat(g,'/h')))")
					exec(t, "insert into "+defaulted+"(id,b) values (1,0),(2,0) on duplicate key update b=b+1")
					exec(t, "insert ignore into "+defaulted+"(id,b) values (3,0),(4,0)")
					exec(t, "insert into "+defaulted+"(id,b) select result,0 from generate_series(5,6) g on duplicate key update b=b+1")
					require.Equal(t, []string{"6|6|6"}, rows(t, "select count(*),count(distinct a),sum(if(g=concat(a,'/g') and h=concat(g,'/h'),1,0)) from "+defaulted))
					defaultStmt, e := conn.PrepareContext(ctx, "insert into "+defaulted+"(id,b) values (?,?) on duplicate key update a=values(a),b=values(b)")
					require.NoError(t, e)
					t.Cleanup(func() { require.NoError(t, defaultStmt.Close()) })
					_, e = defaultStmt.ExecContext(ctx, 1, 7)
					require.NoError(t, e)
					require.Equal(t, []string{"7|1|1"}, rows(t, "select b,g=concat(a,'/g'),h=concat(g,'/h') from "+defaulted+" where id=1"))
					exec(t, "insert into "+defaulted+"(id,a,b) values (7,null,0) on duplicate key update a=values(a)")
					require.Equal(t, []string{"1|1|1"}, rows(t, "select a is null,g is null,h is null from "+defaulted+" where id=7"))
					if mode.name == "ordered" {
						path := filepath.Join(t.TempDir(), "generated-defaults.csv")
						require.NoError(t, os.WriteFile(path, []byte("8,0\n9,0\n"), 0600))
						exec(t, "load data infile '"+strings.ReplaceAll(path, "'", "''")+"' into table "+defaulted+" fields terminated by ',' lines terminated by '\\n' (id,b)")
						require.Equal(t, []string{"2|2"}, rows(t, "select count(*),sum(if(g=concat(a,'/g') and h=concat(g,'/h'),1,0)) from "+defaulted+" where id in (8,9)"))
					}

					layout := table + "_layout"
					exec(t, "create table "+layout+" (id bigint auto_increment primary key,shade enum('red','blue'),a varchar(64) default (uuid()),g varchar(100) generated always as(concat(a,'/g')) stored)")
					exec(t, "insert into "+layout+"(shade) values ('red'),('blue') on duplicate key update shade=values(shade)")
					require.Equal(t, []string{"2|2|2|2"}, rows(t, "select count(*),count(distinct id),count(distinct shade),sum(if(g=concat(a,'/g'),1,0)) from "+layout))
					composite := table + "_composite"
					exec(t, "create table "+composite+" (p int,q int,a varchar(64) default (uuid()),g varchar(100) generated always as(concat(a,'/g')) stored,primary key(p,q))")
					exec(t, "insert into "+composite+"(p,q) values (1,1),(1,2) on duplicate key update a=values(a)")
					require.Equal(t, []string{"2|2"}, rows(t, "select count(*),sum(if(g=concat(a,'/g'),1,0)) from "+composite))

					failing := table + "_error"
					exec(t, "create table "+failing+" (id int primary key,a bigint,b bigint,g bigint generated always as(a*2) stored)")
					exec(t, "insert into "+failing+"(id,a,b) values (1,1,0)")
					_, e = conn.ExecContext(ctx, "insert into "+failing+"(id,a,b) values (1,0,1),(1,0,0) on duplicate key update a=if(values(b)=1,a+9223372036854775807,1)")
					constraintError(t, e, moerr.ER_DATA_OUT_OF_RANGE, "out of range")
					require.Equal(t, []string{"1|1|0|2"}, rows(t, "select * from "+failing))
					insertErrors := table + "_insert_errors"
					exec(t, "create table "+insertErrors+" (id int primary key,a varchar(64) default (uuid()),g varchar(1) generated always as(if(b=1,concat(a,'/g'),'x')) stored,b tinyint)")
					_, e = conn.ExecContext(ctx, "insert into "+insertErrors+"(id,b) select result,1 from generate_series(1,1) g on duplicate key update b=values(b)")
					constraintError(t, e, moerr.ER_DATA_TOO_LONG, "larger than Dest length")
					require.Empty(t, rows(t, "select * from "+insertErrors))
					_, e = conn.ExecContext(ctx, "insert into "+insertErrors+"(id,b) select result,result*1000 from generate_series(1,1) g on duplicate key update b=values(b)")
					constraintError(t, e, moerr.ER_DATA_OUT_OF_RANGE, "out of range")
					require.Empty(t, rows(t, "select * from "+insertErrors))

					nullable := table + "_nullable"
					exec(t, "create table "+nullable+" (u int unique,a int,g int generated always as(a*2) stored)")
					exec(t, "insert into "+nullable+"(u,a) values(null,1),(null,2),(7,3) on duplicate key update a=a+1")
					require.Equal(t, []string{"1|2", "2|4", "3|6"}, rows(t, "select a,g from "+nullable+" order by a"))
					if mode.name == "skip_dedup" {
						for _, n := range []int{8192, 8193} {
							exec(t, "truncate table "+nullable)
							exec(t, fmt.Sprintf("insert into %s(u,a) select if(result<=%d,null,7),result from generate_series(1,%d) g on duplicate key update a=a+1", nullable, n, n+1))
							require.Equal(t, []string{fmt.Sprintf("%d|%d|%d", n+1, n+1, n)}, rows(t, "select count(*),sum(if(g=2*a,1,0)),sum(if(u is null,1,0)) from "+nullable))
						}
						conflict := table + "_secondary"
						exec(t, "create table "+conflict+" (id int primary key,u int unique,a int,g int generated always as(a*2) stored)")
						exec(t, "insert into "+conflict+"(id,u,a) values (1,10,1)")
						_, e = conn.ExecContext(ctx, "insert into "+conflict+"(id,u,a) values (2,10,2) on duplicate key update a=a+1")
						constraintError(t, e, moerr.ER_DUP_ENTRY, "Duplicate entry")
						require.Equal(t, []string{"1|10|1|2"}, rows(t, "select * from "+conflict))
					}
				})
			}
		})
		t.Run("varlen assignment aliases remain stable", func(t *testing.T) {
			exec(t, "create table aliases (id int primary key,u int unique,a varchar(2048),b varchar(2048),suffix varchar(32))")
			exec(t, "insert into aliases values (1,10,'A','B','seed')")
			exec(t, "insert into aliases values (2,10,'x','y','1'),(3,10,'x','y','2') on duplicate key update a=b,b=concat(a,values(suffix)),a=b")
			require.Equal(t, []string{"B12|B12|seed"}, rows(t, "select a,b,suffix from aliases"))
			// Long borrowed RHS values, key movement and repeated hits cross the
			// source batch boundary through the real planner and execution owner.
			exec(t, "drop table aliases")
			exec(t, "create table aliases (id int primary key,u int unique,a varchar(2048),b varchar(2048),suffix varchar(32),g int generated always as(length(b)) stored,unique key ug(g))")
			exec(t, "insert into aliases(id,u,a,b,suffix) values (1,10,repeat('A',128),repeat('B',128),'seed')")
			exec(t, "insert into aliases(id,u,a,b,suffix) select result+100000,10,repeat('x',128),repeat('y',128),cast(result as varchar) from generate_series(1,8193) g order by result on duplicate key update a=b,b=concat(values(b),values(suffix)),a=b")
			expected := strings.Repeat("y", 128) + "8193"
			require.Equal(t, []string{"1|10|" + expected + "|" + expected + "|seed|132"}, rows(t, "select * from aliases"))
		})
		t.Run("fake primary key enum later unique", func(t *testing.T) {
			exec(t, "create table fake_enum (u int unique,color enum('red','blue') unique,v int)")
			exec(t, "insert into fake_enum values (1,'red',0),(2,'blue',0)")
			exec(t, "insert into fake_enum values (99,'red',7) on duplicate key update v=values(v)")
			require.Equal(t, []string{"1|red|7", "2|blue|0"}, rows(t, "select * from fake_enum order by u"))
		})

		t.Run("foreign key intermediate actions and no-op rollback", func(t *testing.T) {
			exec(t, "create table child (id int primary key,u int unique,pid int,payload int,foreign key(pid) references parent(id))")
			exec(t, "insert into child values (1,10,1,0)")
			_, e := conn.ExecContext(ctx, "insert into child values (2,10,999,1),(3,10,1,0) on duplicate key update pid=values(pid),payload=values(payload)")
			constraintError(t, e, moerr.ER_NO_REFERENCED_ROW_2, "foreign key constraint fails")
			require.Equal(t, []string{"1|10|1|0"}, rows(t, "select * from child"))
			exec(t, "set foreign_key_checks=0")
			exec(t, "update child set pid=999")
			exec(t, "set foreign_key_checks=1")
			exec(t, "insert into child values (2,10,1,7) on duplicate key update payload=values(payload)")
			require.Equal(t, []string{"1|10|999|7"}, rows(t, "select * from child"))
			_, e = conn.ExecContext(ctx, "insert into child values (2,10,1,7),(3,10,999,7) on duplicate key update pid=values(pid)")
			constraintError(t, e, moerr.ER_NO_REFERENCED_ROW_2, "foreign key constraint fails")
			require.Equal(t, []string{"1|10|999|7"}, rows(t, "select * from child"))
			exec(t, "create table fk_batches (id int primary key,u int unique,pid int,v bigint,foreign key(pid) references parent(id))")
			exec(t, "insert into fk_batches values (1,10,1,0)")
			var q strings.Builder
			q.WriteString("insert into fk_batches values ")
			for i := 1; i <= 8193; i++ {
				if i > 1 {
					q.WriteByte(',')
				}
				fmt.Fprintf(&q, "(%d,10,1,1)", i+10)
			}
			q.WriteString(" on duplicate key update pid=values(pid),v=v+values(v)")
			exec(t, q.String())
			require.Equal(t, []string{"1|10|1|8193"}, rows(t, "select * from fk_batches"))
		})
		t.Run("forced remote source restores order", func(t *testing.T) {
			exec(t, "create table remote_source (n int)")
			exec(t, "insert into remote_source select result from generate_series(1,24576) g")
			exec(t, "select mo_ctl('dn','flush','odku_systematic.remote_source.0')")
			exec(t, "create table remote_destination (id int primary key,u int unique,v int)")
			exec(t, "insert into remote_destination values (1,10,0)")
			coordinator, e := cluster.GetCNService(0)
			require.NoError(t, e)
			peer, e := cluster.GetCNService(1)
			require.NoError(t, e)
			var peerAddress string
			require.Eventually(t, func() bool {
				service := coordinator.RawService().(cnservice.Service)
				clusterservice.GetMOCluster(service.ID()).ForceRefresh(true)
				nodes, err := service.GetEngine().Nodes(false, "sys", "dump", nil)
				for _, node := range nodes {
					if node.Id == peer.RawService().(cnservice.Service).ID() {
						peerAddress = node.Addr
					}
				}
				return err == nil && len(nodes) >= 2 && peerAddress != ""
			}, 15*time.Second, 100*time.Millisecond)
			// Broadcast DEDUP normally pins scans locally. Exercise its supported
			// remote codec path with the existing optimizer test hint.
			rt := moruntime.ServiceRuntime(coordinator.ServiceID())
			oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
			rt.SetGlobalVariables("optimizer_hints", "forceOneCN=1")
			defer func() {
				if hadHints {
					rt.SetGlobalVariables("optimizer_hints", oldHints)
				} else {
					rt.CompareAndDeleteGlobalVariables("optimizer_hints", "forceOneCN=1")
				}
			}()
			oldForce := plan.GetForceScanOnMultiCN()
			plan.SetForceScanOnMultiCN(true)
			defer plan.SetForceScanOnMultiCN(oldForce)
			var receipts atomic.Int64
			previous := logutil.GetReportZapFunc()
			logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(enc zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
				if strings.HasPrefix(entry.Message, "inject_send_pipeline: client2server") {
					receipts.Add(1)
				}
				return previous(enc, entry, fields)
			}})
			defer logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: previous})
			if fault.Enable() {
				defer fault.Disable()
			}
			require.NoError(t, fault.AddFaultPoint(ctx, "inject_send_pipeline", ":::", "return", 0, "insert into remote_destination", false))
			defer func() {
				_, err := fault.RemoveFaultPoint(context.Background(), "inject_send_pipeline")
				require.NoError(t, err)
			}()
			physical := rows(t, "explain phyplan analyze insert into remote_destination select n+10,10,n from remote_source order by n on duplicate key update v=values(v)")
			require.Contains(t, strings.Join(physical, "\n"), "addr:"+peerAddress)
			require.Positive(t, receipts.Load(), "must send a real RPC pipeline")
			require.Equal(t, []string{"1|10|24576"}, rows(t, "select * from remote_destination"))
		})
		t.Run("foreign key eligibility is per constraint", func(t *testing.T) {
			exec(t, "create table parent_second (id int primary key)")
			exec(t, "insert into parent_second values (2)")
			for _, reverse := range []bool{false, true} {
				exec(t, "drop table if exists child_two")
				fks := "foreign key(p1) references parent(id),foreign key(p2) references parent_second(id)"
				if reverse {
					fks = "foreign key(p2) references parent_second(id),foreign key(p1) references parent(id)"
				}
				exec(t, "create table child_two(id int primary key,u int unique,p1 int,p2 int,v int,"+fks+")")
				exec(t, "set foreign_key_checks=0")
				exec(t, "insert into child_two values(1,10,999,2,0)")
				exec(t, "set foreign_key_checks=1")
				exec(t, "insert into child_two values(2,10,1,2,7) on duplicate key update p2=values(p2),v=values(v)")
				require.Equal(t, []string{"1|10|999|2|7"}, rows(t, "select * from child_two"))
				_, e := conn.ExecContext(ctx, "insert into child_two values(2,10,1,888,8),(3,10,1,2,9) on duplicate key update p2=values(p2),v=values(v)")
				constraintError(t, e, moerr.ER_NO_REFERENCED_ROW_2, "foreign key constraint fails")
				require.Equal(t, []string{"1|10|999|2|7"}, rows(t, "select * from child_two"))
			}
		})

		t.Run("final FK validation retries after parent commit", func(t *testing.T) {
			for _, deleting := range []bool{false, true} {
				t.Run(fmt.Sprint(deleting), func(t *testing.T) {
					parentCtx, stop := context.WithTimeout(ctx, 30*time.Second)
					defer stop()
					exec(t, "drop table if exists child_pending")
					exec(t, "create table child_pending(id int primary key,u int unique,pid int,v int,foreign key(pid) references parent(id))")
					exec(t, "insert into child_pending values(1,10,1,0)")
					if deleting {
						exec(t, "insert into parent values(42)")
					}
					parentConn, e := db.Conn(parentCtx)
					require.NoError(t, e)
					defer parentConn.Close()
					_, e = parentConn.ExecContext(parentCtx, "use odku_systematic")
					require.NoError(t, e)
					tx, e := parentConn.BeginTx(parentCtx, nil)
					require.NoError(t, e)
					defer tx.Rollback()
					parentSQL := "insert into parent values(42)"
					if deleting {
						parentSQL = "delete from parent where id=42"
					}
					_, e = tx.ExecContext(parentCtx, parentSQL)
					require.NoError(t, e)
					var parentID uint64
					require.NoError(t, conn.QueryRowContext(parentCtx, "select rel_id from mo_catalog.mo_tables where reldatabase='odku_systematic' and relname='parent'").Scan(&parentID))
					cn, e := cluster.GetCNService(0)
					require.NoError(t, e)
					tc := moruntime.MustGetTestingContext(cn.ServiceID())
					before := tc.GetBeforeLockFunc()
					defer tc.SetBeforeLockFunc(before)
					arrived, release := make(chan struct{}), make(chan struct{})
					var entered, released sync.Once
					unblock := func() { released.Do(func() { close(release) }) }
					defer unblock()
					tc.SetBeforeLockFunc(func(txn []byte, id uint64) {
						before(txn, id)
						if id != parentID {
							return
						}
						entered.Do(func() {
							close(arrived)
							select {
							case <-release:
							case <-parentCtx.Done():
							}
						})
					})
					done := make(chan error, 1)
					go func() {
						_, err := conn.ExecContext(parentCtx, "insert into child_pending values(2,10,42,7) on duplicate key update pid=values(pid),v=values(v)")
						done <- err
					}()
					select {
					case <-arrived:
					case err := <-done:
						t.Fatalf("must reach final parent lock before rejecting: %v", err)
					case <-parentCtx.Done():
						t.Fatal(parentCtx.Err())
					}
					require.NoError(t, tx.Commit())
					unblock()
					select {
					case err := <-done:
						if deleting {
							constraintError(t, err, moerr.ER_NO_REFERENCED_ROW_2, "foreign key constraint fails")
						} else {
							require.NoError(t, err)
						}
					case <-parentCtx.Done():
						t.Fatal(parentCtx.Err())
					}
					tc.SetBeforeLockFunc(before)
					expected := "1|10|42|7"
					if deleting {
						expected = "1|10|1|0"
					}
					require.Equal(t, []string{expected}, rows(t, "select * from child_pending"))
					exec(t, "drop table child_pending")
					if !deleting {
						exec(t, "delete from parent where id=42")
					}
				})
			}
		})

		t.Run("resolved identity survives competing commit and rollback", func(t *testing.T) {
			for _, nonUnique := range []bool{false, true} {
				for _, rollback := range []bool{false, true} {
					t.Run(fmt.Sprintf("secondary=%t/rollback=%t", nonUnique, rollback), func(t *testing.T) {
						pairCtx, stop := context.WithTimeout(ctx, 40*time.Second)
						defer stop()
						exec(t, "drop table if exists competitors")
						exec(t, "create table competitors (id bigint primary key,a bigint not null,b bigint not null,v bigint,d int,unique key ua(a),unique key ub(b),key secondary_v(d))")
						exec(t, "insert into competitors values (1,1,10,0,7)")
						var tableID uint64
						require.NoError(t, conn.QueryRowContext(pairCtx, "select rel_id from mo_catalog.mo_tables where reldatabase='odku_systematic' and relname='competitors'").Scan(&tableID))
						var conns [2]*sql.Conn
						for i := range conns {
							cn, e := cluster.GetCNService(i)
							require.NoError(t, e)
							db, e := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/odku_systematic", cn.GetServiceConfig().CN.Frontend.Port))
							require.NoError(t, e)
							defer db.Close()
							conns[i], e = db.Conn(pairCtx)
							require.NoError(t, e)
							defer conns[i].Close()
						}
						gates := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
						var once [2]sync.Once
						release := func(i int) { once[i].Do(func() { close(gates[i]) }) }
						defer func() { release(0); release(1) }()
						arrived := make(chan int, 2)
						var mu sync.Mutex
						seen := make(map[string]bool)
						count := 0
						for i := 0; i < 3; i++ {
							cn, e := cluster.GetCNService(i)
							require.NoError(t, e)
							tc := moruntime.MustGetTestingContext(cn.ServiceID())
							before := tc.GetBeforeLockFunc()
							defer tc.SetBeforeLockFunc(before)
							tc.SetBeforeLockFunc(func(txn []byte, id uint64) {
								before(txn, id)
								if id != tableID {
									return
								}
								mu.Lock()
								key := string(txn)
								slot := -1
								if !seen[key] && count < 2 {
									seen[key] = true
									slot = count
									count++
								}
								mu.Unlock()
								if slot < 0 {
									return
								}
								arrived <- slot
								select {
								case <-gates[slot]:
								case <-pairCtx.Done():
								}
							})
						}
						var done [2]chan error
						for i := range done {
							done[i] = make(chan error, 1)
						}
						start := func(i int) {
							go func() {
								if i == 0 && rollback {
									if _, e := conns[i].ExecContext(pairCtx, "begin"); e != nil {
										done[i] <- e
										return
									}
								}
								candidate := "(1000,1,200000,1,7)"
								if i == 1 {
									candidate = "(1001,100000,10,2,7)"
								}
								// Same non-unique index value forces replacement of its physical row-id entry.
								assignment := "v=values(v)"
								if nonUnique {
									assignment = "d=values(d),v=values(v)"
								}
								_, e := conns[i].ExecContext(pairCtx, "insert into competitors values "+candidate+" on duplicate key update "+assignment)
								if i == 0 && rollback {
									_, re := conns[i].ExecContext(pairCtx, "rollback")
									if e == nil {
										e = re
									}
								}
								done[i] <- e
							}()
						}
						await := func(ch <-chan error) {
							t.Helper()
							select {
							case e := <-ch:
								require.NoError(t, e)
							case <-pairCtx.Done():
								t.Fatal(pairCtx.Err())
							}
						}
						for i := range 2 {
							start(i)
							select {
							case slot := <-arrived:
								require.Equal(t, i, slot)
							case <-pairCtx.Done():
								t.Fatal(pairCtx.Err())
							}
						}
						release(0)
						await(done[0])
						exec(t, "select mo_ctl('dn','flush','odku_systematic.competitors.0')")
						release(1)
						await(done[1])
						exec(t, "select mo_ctl('dn','flush','odku_systematic.competitors.0')")
						expected := "1|1|10|2"

						for i := range 3 {
							cn, e := cluster.GetCNService(i)
							require.NoError(t, e)
							db, e := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/odku_systematic", cn.GetServiceConfig().CN.Frontend.Port))
							require.NoError(t, e)
							for _, path := range []string{"", " force index(ua) where a=1", " force index(ub) where b=10", fmt.Sprintf(" force index(secondary_v) where d=%d", 7)} {
								got := func() []string {
									r, e := db.QueryContext(pairCtx, "select id,a,b,v from competitors"+path)
									require.NoError(t, e)
									defer r.Close()
									var got []string
									for r.Next() {
										var id, a, b, v int
										require.NoError(t, r.Scan(&id, &a, &b, &v))
										got = append(got, fmt.Sprintf("%d|%d|%d|%d", id, a, b, v))
									}
									require.NoError(t, r.Err())
									return got
								}()
								require.Equal(t, []string{expected}, got, "CN=%d path=%s", i, path)
							}
							require.NoError(t, db.Close())
						}
					})
				}
			}
		})

	})
}

// Cleanup owns a bounded budget after a failed or cancelled SQL scenario.
// The combined fixture drops many base and hidden tables in one DDL; allow
// that work to complete under race instrumentation and a busy shared host.
func cleanupODKUSQL(t *testing.T, conn *sql.Conn, statement string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
	defer cancel()
	if _, err := conn.ExecContext(ctx, statement); err != nil {
		t.Errorf("SQL cleanup %q: %v", statement, err)
	}
}

// A fresh one-CN instance installs the real query cap before any services run;
// mutating the shared cluster's live configuration would race other sessions.
func TestODKUWriterFenceBudgetRollback(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(s embed.ServiceOperator) {
		if s.ServiceType() == metadata.ServiceType_CN {
			s.Adjust(func(cfg *embed.ServiceConfig) { cfg.CN.Frontend.ProcessLimitationSize = 7 << 20 })
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	exec := func(q string) { t.Helper(); _, e := conn.ExecContext(ctx, q); require.NoError(t, e, q) }
	exec("create database odku_writer_budget")
	t.Cleanup(func() { cleanupODKUSQL(t, conn, "drop database if exists odku_writer_budget") })
	exec("use odku_writer_budget")
	exec("set experimental_fulltext_index=1")
	exec("create table docs(a int,g int generated always as(a+1) stored primary key,body text,fulltext ft(body))")
	exec("insert into docs(a,body) values(1,'seedword')")
	var hidden string
	require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where name='ft' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='odku_writer_budget' and relname='docs')").Scan(&hidden))
	checkIndex := func(expected int) {
		t.Helper()
		var count, lo, hi int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(distinct doc_id),min(doc_id),max(doc_id) from `"+hidden+"`").Scan(&count, &lo, &hi))
		require.Equal(t, 1, count)
		require.Equal(t, expected, lo)
		require.Equal(t, expected, hi)
	}
	// One document expands into multiple tokenizer batches, with less than 1 MiB
	// of SQL input. The real shared cap rejects retained writer output.
	var body strings.Builder
	for i := 0; i < 100000; i++ {
		fmt.Fprintf(&body, "word%05d ", i)
	}
	_, err = conn.ExecContext(ctx, "insert into docs(a,body) values(1,'"+body.String()+"') on duplicate key update a=2,body=values(body)")
	var resourceError *mysqlDriver.MySQLError
	require.ErrorAs(t, err, &resourceError)
	require.True(t, strings.Contains(resourceError.Message, "budget") || strings.Contains(resourceError.Message, "capacity"), resourceError.Message)
	var a, g int
	var payload string
	require.NoError(t, conn.QueryRowContext(ctx, "select a,g,body from docs").Scan(&a, &g, &payload))
	require.Equal(t, 1, a)
	require.Equal(t, 2, g)
	require.Equal(t, "seedword", payload)
	checkIndex(2)
	var count int
	require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from docs where match(body) against('seedword')").Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from docs where match(body) against('word99999')").Scan(&count))
	require.Zero(t, count)
	exec("insert into docs(a,body) values(1,'newword') on duplicate key update a=2,body=values(body)")
	require.NoError(t, conn.QueryRowContext(ctx, "select a,g,body from docs").Scan(&a, &g, &payload))
	require.Equal(t, 2, a)
	require.Equal(t, 3, g)
	require.Equal(t, "newword", payload)
	checkIndex(3)
	require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from docs where match(body) against('word99999')").Scan(&count))
	require.Zero(t, count, "a rejected writer must not leave tokens that become visible when its key is reused")
	require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from docs where match(body) against('seedword')").Scan(&count))
	require.Zero(t, count)
	require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from docs where match(body) against('newword')").Scan(&count))
	require.Equal(t, 1, count)
}
