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
