// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestFunctionalIndexCascadeAndRename(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists functional_index_actions")
		_, err = conn.ExecContext(ctx, "create database functional_index_actions")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "use functional_index_actions")
		require.NoError(t, err)
		t.Run("composite_primary_controls", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "create table composite_parent(id int primary key)")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "insert into composite_parent values(4)")
			require.NoError(t, err)
			for _, tc := range []struct{ name, extra string }{
				{"ordinary_cp", "index ix(pid,id),foreign key(pid) references composite_parent(id) on update cascade"},
				{"functional_cp", "index ix((pid+1),id)"},
				{"functional_fk_cp", "index ix((pid+1),id),foreign key(pid) references composite_parent(id) on update cascade"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					_, err := conn.ExecContext(ctx, "create table "+tc.name+"(id int,pid int,primary key(id,pid),"+tc.extra+")")
					require.NoError(t, err)
					_, err = conn.ExecContext(ctx, "insert into "+tc.name+" values(40,4)")
					require.NoError(t, err)
				})
			}
			for _, name := range []string{"ordinary_cluster", "functional_cluster"} {
				t.Run(name, func(t *testing.T) {
					index := "index ix(pid,id)"
					if name == "functional_cluster" {
						index = "index ix((pid+1),id)"
					}
					_, err := conn.ExecContext(ctx, "create table "+name+"(id int,pid int,"+index+") cluster by(id,pid)")
					require.NoError(t, err)
					_, err = conn.ExecContext(ctx, "insert into "+name+" values(40,4)")
					require.NoError(t, err)
					var n int
					require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+name+" where id=40 and pid=4").Scan(&n))
					require.Equal(t, 1, n)
				})
			}
			t.Run("fake_and_auto_primary", func(t *testing.T) {
				exec := func(s string) { t.Helper(); _, e := conn.ExecContext(ctx, s); require.NoErrorf(t, e, "%s", s) }
				exec("create table fake_fi(v int,index ix((v+1)))")
				exec("insert into fake_fi values(1),(2)")
				var n int
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from fake_fi force index(ix) where v+1=2").Scan(&n))
				require.Equal(t, 1, n)
				exec("create table auto_fi(id bigint auto_increment primary key,u int unique,name varchar(40),index ix((lower(name))))")
				exec("insert ignore into auto_fi(u,name) values(1,'ABC'),(1,'SKIPPED'),(2,'DEF')")
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from auto_fi").Scan(&n))
				require.Equal(t, 2, n)
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from auto_fi force index(ix) where lower(name)='abc'").Scan(&n))
				require.Equal(t, 1, n)
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from auto_fi force index(ix) where lower(name)='skipped'").Scan(&n))
				require.Zero(t, n)
			})
		})
		t.Run("cascade", func(t *testing.T) {
			exec := func(s string) { t.Helper(); _, e := conn.ExecContext(ctx, s); require.NoErrorf(t, e, "%s", s) }
			exec("create table p(id int primary key)")
			exec("create table c(id int primary key,pid int,foreign key(pid) references p(id) on update cascade)")
			exec("insert into p values(1)")
			exec("insert into c values(10,1)")
			exec("update p set id=2 where id=1")
			exec("create index ix on c((id+1))")
			exec("update p set id=3 where id=2")
			var pid int
			require.NoError(t, conn.QueryRowContext(ctx, "select pid from c force index(ix) where id+1=11").Scan(&pid))
			require.Equal(t, 3, pid)
			exec("drop index ix on c")
			exec("update p set id=4 where id=3")
			require.NoError(t, conn.QueryRowContext(ctx, "select pid from c").Scan(&pid))
			require.Equal(t, 4, pid)
		})
		t.Run("dependent_actions", func(t *testing.T) {
			exec := func(s string) { t.Helper(); _, e := conn.ExecContext(ctx, s); require.NoErrorf(t, e, "%s", s) }
			count := func(s string, want int) {
				t.Helper()
				var n int
				require.NoError(t, conn.QueryRowContext(ctx, s).Scan(&n))
				require.Equal(t, want, n, s)
			}
			exec("create table p2(id int primary key)")
			exec("insert into p2 values(1)")
			// Index before FK, expression second, and an unaffected outgoing FK.
			exec("create table other_p(id int primary key)")
			exec("insert into other_p values(7)")
			exec("create table c2(id int primary key,pid int,other_id int,index ix(other_id,(pid+1)))")
			exec("alter table c2 add constraint f2 foreign key(pid) references p2(id) on update cascade")
			exec("alter table c2 add constraint fo foreign key(other_id) references other_p(id)")
			exec("insert into c2 values(10,1,7),(11,NULL,7)")
			exec("begin")
			exec("update p2 set id=2 where id=1")
			count("select count(*) from c2 force index(ix) where other_id=7 and pid+1=3", 1)
			count("select count(*) from c2 force index(ix) where other_id=7 and pid+1=2", 0)
			exec("rollback")
			count("select count(*) from c2 force index(ix) where other_id=7 and pid+1=2", 1)
			exec("update p2 set id=2 where id=1")
			count("select count(*) from c2 force index(ix) where other_id=7 and pid+1=3", 1)
			count("select count(*) from c2 ignore index(ix) where other_id=7 and pid+1=3", 1)
			func() {
				rows, err := conn.QueryContext(ctx, "explain select id from c2 force index(ix) where other_id=7 and pid+1=3")
				require.NoError(t, err)
				defer rows.Close()
				var lines []string
				for rows.Next() {
					var line string
					require.NoError(t, rows.Scan(&line))
					lines = append(lines, line)
				}
				require.NoError(t, rows.Err())
				require.Contains(t, strings.Join(lines, "\n"), "Index Table Scan on c2.ix")
			}()
			count("select count(*) from c2 where pid is null", 1)
			// SET NULL must remove the old functional key and keep the child row.
			exec("create table cn(id int primary key,pid int,index ix((pid+1)),foreign key(pid) references p2(id) on update set null)")
			exec("insert into cn values(20,2)")
			exec("update p2 set id=3 where id=2")
			count("select count(*) from cn where pid is null", 1)
			count("select count(*) from cn force index(ix) where pid+1=3", 0)
			exec("update cn set pid=3 where id=20")
			count("select count(*) from cn force index(ix) where pid+1=4", 1)
			// Recursive cascade with an indexed generated value at each child.
			exec("create table middle_fi(id int primary key,index ix((id+1)),foreign key(id) references p2(id) on update cascade)")
			exec("create table leaf(id int primary key,pid int,index ix((pid+1)),foreign key(pid) references middle_fi(id) on update cascade)")
			exec("insert into middle_fi values(3)")
			exec("insert into leaf values(30,3)")
			exec("update p2 set id=4 where id=3")
			count("select count(*) from middle_fi force index(ix) where id+1=5", 1)
			count("select count(*) from middle_fi force index(ix) where id+1=4", 0)
			count("select count(*) from leaf force index(ix) where pid+1=5", 1)
			count("select count(*) from leaf force index(ix) where pid+1=4", 0)
			// Cascading a component of a composite primary key rebuilds aliases too.
			exec("create table cp(id int,pid int,primary key(id,pid),index ix((pid+1),id),foreign key(pid) references p2(id) on update cascade)")
			exec("insert into cp values(40,4)")
			exec("update p2 set id=5 where id=4")
			count("select count(*) from cp force index(ix) where pid+1=6 and id=40", 1)
			count("select count(*) from cp force index(ix) where pid+1=5 and id=40", 0)
			// Functional indexes do not relax FK referenced-key DDL admission.
			exec("create table np(id int primary key,k int,index ik(k))")
			_, err := conn.ExecContext(ctx, "create table nc(id int primary key,k int,index ix((k+1)),foreign key(k) references np(k) on update cascade)")
			require.ErrorContains(t, err, "failed to add the foreign key constraint")
			// An expression failure must roll back parent, child and index together.
			exec("create table bp(id bigint primary key)")
			exec("create table bc(id int primary key,pid bigint,index ix((pid+1)),foreign key(pid) references bp(id) on update cascade)")
			exec("insert into bp values(1)")
			exec("insert into bc values(1,1)")
			_, err = conn.ExecContext(ctx, "update bp set id=9223372036854775807 where id=1")
			require.Error(t, err)
			count("select count(*) from bp where id=1", 1)
			count("select count(*) from bc force index(ix) where pid+1=2", 1)
			exec("update bp set id=2 where id=1")
			count("select count(*) from bc force index(ix) where pid+1=3", 1)
		})
		t.Run("self_cascade", func(t *testing.T) {
			exec := func(s string) { t.Helper(); _, e := conn.ExecContext(ctx, s); require.NoErrorf(t, e, "%s", s) }
			count := func(s string, want int) {
				t.Helper()
				var n int
				require.NoError(t, conn.QueryRowContext(ctx, s).Scan(&n))
				require.Equal(t, want, n, s)
			}
			exec("create table self_fi(id int primary key,pid int,v int,index ix((pid+v)),foreign key(pid) references self_fi(id) on update cascade)")
			exec("insert into self_fi values(1,NULL,10),(2,1,20)")
			exec("update self_fi set id=id+10,v=v+1")
			count("select count(*) from self_fi force index(ix) where pid+v=32", 1)
			count("select count(*) from self_fi ignore index(ix) where pid+v=32", 1)
			count("select count(*) from self_fi force index(ix) where pid+v=21", 0)
		})
		t.Run("rename", func(t *testing.T) {
			exec := func(s string) { t.Helper(); _, e := conn.ExecContext(ctx, s); require.NoErrorf(t, e, "%s", s) }
			exec("create table r(id int primary key,name varchar(40),index ix((lower(name))),index im(id,(upper(name)),(lower(name))))")
			exec("insert into r values(1,'ABC')")
			exec("alter table r rename column name to renamed")
			var id int
			require.NoError(t, conn.QueryRowContext(ctx, "select id from r force index(ix) where lower(renamed)='abc'").Scan(&id))
			require.Equal(t, 1, id)
			var tableName, ddl string
			require.NoError(t, conn.QueryRowContext(ctx, "show create table r").Scan(&tableName, &ddl))
			require.Contains(t, ddl, "lower(`renamed`)")
			require.NotContains(t, ddl, "lower(`name`)")
			exec("alter table r change column renamed `New Name` varchar(80) first")
			exec("update r set `New Name`='DEF' where id=1")
			for _, hint := range []string{"force index(im)", "ignore index(ix,im)"} {
				require.NoError(t, conn.QueryRowContext(ctx, "select id from r "+hint+" where id=1 and upper(`New Name`)='DEF' and lower(`New Name`)='def'").Scan(&id))
				require.Equal(t, 1, id)
			}
			require.NoError(t, conn.QueryRowContext(ctx, "show create table r").Scan(&tableName, &ddl))
			require.Contains(t, ddl, "lower(`New Name`)")
			require.NotContains(t, ddl, "renamed")
			var n int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from information_schema.statistics where table_schema='functional_index_actions' and table_name='r' and expression like '%New Name%'").Scan(&n))
			require.Equal(t, 3, n)
			exec("create table r_like like r")
			exec("insert into r_like(`New Name`,id) values('XYZ',2)")
			require.NoError(t, conn.QueryRowContext(ctx, "select id from r_like force index(ix) where lower(`New Name`)='xyz'").Scan(&id))
			require.Equal(t, 2, id)
			// The ordinary generated-column restriction has not been relaxed.
			exec("create table ordinary_g(id int,name varchar(40),g varchar(40) as(lower(name)))")
			_, err := conn.ExecContext(ctx, "alter table ordinary_g rename column name to renamed")
			require.ErrorContains(t, err, "depends on it")
		})
	})
}

func TestFunctionalCompositeIndexLifecycle(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists functional_index_multi")
		exec := func(s string) { t.Helper(); _, e := conn.ExecContext(ctx, s); require.NoErrorf(t, e, "%s", s) }
		count := func(s string, want int) {
			t.Helper()
			var got int
			require.NoError(t, conn.QueryRowContext(ctx, s).Scan(&got))
			require.Equal(t, want, got, s)
		}
		exec("create database functional_index_multi")
		exec("use functional_index_multi")
		exec("create table t(id int primary key, tenant int, name varchar(40), index ie ((lower(name)), (id+1)), index il (tenant,(lower(name))), index ir ((lower(name)),tenant), index idup ((lower(name)),(lower(name))))")
		exec("insert into t values(1,7,'ABC'),(2,7,'abc'),(3,8,'ABC'),(4,7,NULL)")
		checks := []struct {
			index, predicate string
			want             int
		}{
			{"ie", "lower(name)='abc' and id+1=2", 1},
			{"il", "tenant=7 and lower(name)='abc'", 2},
			{"ir", "lower(name)='abc' and tenant=8", 1},
			{"idup", "lower(name)='abc'", 3},
			{"il", "tenant=7", 3},
			{"ir", "lower(name) is null and tenant=7", 1},
		}
		for _, check := range checks {
			count("select count(*) from t force index("+check.index+") where "+check.predicate, check.want)
			count("select count(*) from t ignore index(ie,il,ir,idup) where "+check.predicate, check.want)
		}
		for _, check := range checks[:3] {
			func() {
				rows, e := conn.QueryContext(ctx, "explain select id from t force index("+check.index+") where "+check.predicate)
				require.NoError(t, e)
				defer rows.Close()
				var lines []string
				for rows.Next() {
					var line string
					require.NoError(t, rows.Scan(&line))
					lines = append(lines, line)
				}
				require.NoError(t, rows.Err())
				require.Contains(t, strings.Join(lines, "\n"), "Index Table Scan on t."+check.index)
			}()
		}
		count("select count(*) from information_schema.statistics where table_schema='functional_index_multi' and table_name='t' and column_name is null and expression is not null", 6)
		exec("begin")
		exec("update t set name='different',tenant=9 where id=1")
		exec("rollback")
		count("select count(*) from t force index(il) where tenant=7 and lower(name)='abc'", 2)
		exec("update t set name='XYZ',tenant=9 where id=2")
		count("select count(*) from t force index(ir) where lower(name)='xyz' and tenant=9", 1)
		exec("delete from t where id=3")
		count("select count(*) from t force index(idup) where lower(name)='abc'", 1)
		exec("create index iback on t (tenant,(upper(name)))")
		count("select count(*) from t force index(iback) where tenant=9 and upper(name)='XYZ'", 1)
		exec("drop index iback on t")
		exec("alter table t add column extra int first, modify name varchar(60)")
		count("select count(*) from t force index(ie) where lower(name)='abc' and id+1=2", 1)
		exec("create table cloned like t")
		exec("insert into cloned(id,tenant,name) values(11,7,'Clone')")
		count("select count(*) from cloned force index(il) where tenant=7 and lower(name)='clone'", 1)
		exec("alter table t drop index idup, add column tail int")
		count("select count(*) from mo_catalog.mo_columns where att_database='functional_index_multi' and att_relname='t' and att_is_hidden=1 and attr_has_generated=1", 4)
		count("select count(*) from t force index(il) where tenant=7 and lower(name)='abc'", 1)
		_, err = conn.ExecContext(ctx, "alter table t add index bad ((lower(name)),(rand()))")
		require.Error(t, err)
		count("select count(*) from mo_catalog.mo_columns where att_database='functional_index_multi' and att_relname='t' and att_is_hidden=1 and attr_has_generated=1", 4)
	})
}

func TestFunctionalIndexLifecycle(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists functional_index_lifecycle")
		exec := func(s string) { t.Helper(); _, err := conn.ExecContext(ctx, s); require.NoErrorf(t, err, "%s", s) }
		count := func(s string, want int) {
			t.Helper()
			var got int
			require.NoError(t, conn.QueryRowContext(ctx, s).Scan(&got))
			require.Equal(t, want, got, s)
		}
		exec("create database functional_index_lifecycle")
		exec("use functional_index_lifecycle")
		exec("create table t (id int primary key, name varchar(40), index il ((lower(name))))")
		exec("insert into t values (1,'ABC'),(2,'def'),(3,NULL)")
		count("select count(*) from t force index(il) where lower(name)='abc'", 1)
		exec("update t set name='AbC' where id=2")
		count("select count(*) from t force index(il) where lower(name)='abc'", 2)
		exec("begin")
		exec("delete from t where id=1")
		exec("rollback")
		count("select count(*) from t force index(il) where lower(name)='abc'", 2)
		func() {
			stmt, err := conn.PrepareContext(ctx, "insert into t values (?,?)")
			require.NoError(t, err)
			defer func() { require.NoError(t, stmt.Close()) }()
			_, err = stmt.ExecContext(ctx, 4, "ABC")
			require.NoError(t, err)
		}()
		exec("create index ip on t ((id+1))")
		count("select count(*) from t force index(ip) where id+1=5", 1)
		exec("alter table t add column extra int first")
		count("select count(*) from t force index(il) where lower(name)='abc'", 3)
		exec("alter table t modify name varchar(60)")
		exec("set sql_mode='';")
		exec("insert into t(id,name) values(5,'ABC')")
		exec("set sql_mode='STRICT_TRANS_TABLES'")
		count("select count(*) from t force index(il) where lower(name)='abc'", 4)
		var lines []string
		func() {
			rows, err := conn.QueryContext(ctx, "explain select id from t force index(il) where lower(name)='abc'")
			require.NoError(t, err)
			defer func() { require.NoError(t, rows.Close()) }()
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				lines = append(lines, line)
			}
			require.NoError(t, rows.Err())
		}()
		require.Contains(t, strings.Join(lines, "\n"), "Index Table Scan on t.il")
		require.Contains(t, strings.Join(lines, "\n"), "lower(t.name)")
		count("select count(*) from information_schema.statistics where table_schema='functional_index_lifecycle' and table_name='t' and index_name='il' and column_name is null and expression is not null", 1)
		var backing string
		require.NoError(t, conn.QueryRowContext(ctx, "select attname from mo_catalog.mo_columns where att_database='functional_index_lifecycle' and att_relname='t' and attr_has_generated=1 order by attname limit 1").Scan(&backing))
		_, err = conn.ExecContext(ctx, "insert into t(id,"+backing+") values(10,default)")
		require.Error(t, err)
		_, err = conn.ExecContext(ctx, "update t set "+backing+"=default")
		require.Error(t, err)
		exec("create table cloned like t")
		exec("insert into cloned(id,name) values(20,'ABC')")
		count("select count(*) from cloned force index(il) where lower(name)='abc'", 1)
		exec("drop index il on t")
		count("select count(*) from information_schema.statistics where table_schema='functional_index_lifecycle' and table_name='t' and index_name='il'", 0)
		exec("drop index ip on t")
		count("select count(*) from mo_catalog.mo_columns where att_database='functional_index_lifecycle' and att_relname='t' and attr_has_generated=1", 0)
		count("select count(*) from t", 5)
	})
}
