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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Exercise inherited index bindings through the frontend, COPY INSERT and
// catalog publication, including rollback. Two rows distinguish the collision
// and composite-NULL contracts; no distributed topology is needed.
func TestAlterReplacementPreservesIndexes(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(t *testing.T, q string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		query := func(t *testing.T, q string) [][]string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, q)
			require.NoError(t, err, q)
			defer rows.Close()
			columns, err := rows.Columns()
			require.NoError(t, err)
			result := make([][]string, 0)
			for rows.Next() {
				values := make([]sql.NullString, len(columns))
				dest := make([]any, len(columns))
				for i := range values {
					dest[i] = &values[i]
				}
				require.NoError(t, rows.Scan(dest...))
				row := make([]string, len(values))
				for i, v := range values {
					row[i] = "NULL"
					if v.Valid {
						row[i] = v.String
					}
				}
				result = append(result, row)
			}
			require.NoError(t, rows.Err())
			return result
		}
		reject := func(t *testing.T, q string, code uint16) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q)
			var me *mysql.MySQLError
			require.ErrorAs(t, err, &me, q)
			if code != 0 {
				require.Equal(t, code, me.Number, q)
			}
		}
		exec(t, "create database alter_replacement_indexes")
		defer cleanupSQLIntegration(t, cn, "drop database if exists alter_replacement_indexes")
		exec(t, "use alter_replacement_indexes")
		indexParts := func(t *testing.T, table, index string) [][]string {
			return query(t, "select column_name from information_schema.statistics where table_schema=database() and table_name='"+table+"' and index_name='"+index+"' order by seq_in_index")
		}
		t.Run("unique failure is atomic", func(t *testing.T) {
			exec(t, "create table collision(id int primary key,v int,unique key uq(v))")
			exec(t, "insert into collision values(1,10),(2,20)")
			before := query(t, "select count(*) from mo_catalog.mo_tables where reldatabase=database()")
			reject(t, "alter table collision drop column v,add column v int default 7", 1062)
			require.Equal(t, [][]string{{"1", "10"}, {"2", "20"}}, query(t, "select id,v from collision order by id"))
			require.Equal(t, [][]string{{"v"}}, indexParts(t, "collision", "uq"))
			require.Equal(t, before, query(t, "select count(*) from mo_catalog.mo_tables where reldatabase=database()"), "failed COPY must leave no temporary relations")
			reject(t, "insert into collision values(3,10)", 1062)
			exec(t, "alter table collision drop column v,add column v int default null")
			require.Equal(t, [][]string{{"1", "NULL"}, {"2", "NULL"}}, query(t, "select id,v from collision order by id"))
			require.Equal(t, [][]string{{"v"}}, indexParts(t, "collision", "uq"))
			exec(t, "insert into collision values(3,7)")
			reject(t, "insert into collision values(4,7)", 1062)
		})
		t.Run("unique nonconflicting replacement", func(t *testing.T) {
			exec(t, "create table singleton(id int primary key,v int,unique key uq(v))")
			exec(t, "insert into singleton values(1,10)")
			exec(t, "alter table singleton drop column v,add column v int default 7")
			require.Equal(t, [][]string{{"1", "7"}}, query(t, "select id,v from singleton force index(uq) where v=7"))
			require.Empty(t, query(t, "select id from singleton force index(uq) where v=10"))
			reject(t, "insert into singleton values(2,7)", 1062)
		})
		t.Run("composite null does not shorten unique key", func(t *testing.T) {
			exec(t, "create table composite_key(id int primary key,a int,b int,unique key uq(a,b))")
			exec(t, "insert into composite_key values(1,1,10),(2,1,20)")
			exec(t, "alter table composite_key drop column b,add column b int default null")
			require.Equal(t, [][]string{{"a"}, {"b"}}, indexParts(t, "composite_key", "uq"))
			require.Equal(t, [][]string{{"1", "1", "NULL"}, {"2", "1", "NULL"}}, query(t, "select id,a,b from composite_key order by id"))
			exec(t, "insert into composite_key values(3,1,99),(4,1,100)")
			reject(t, "insert into composite_key values(5,1,99)", 1062)
			require.Equal(t, query(t, "select id from composite_key ignore index(uq) where a=1 and b=99"), query(t, "select id from composite_key force index(uq) where a=1 and b=99"))
		})
		t.Run("ordinary index and view use replacement values", func(t *testing.T) {
			exec(t, "create table ordinary(id int primary key,v int,key ix(v))")
			exec(t, "insert into ordinary values(1,10),(2,20)")
			exec(t, "create view replacement_view as select id,v from ordinary")
			exec(t, "alter table ordinary drop column v,add column v int default 7")
			require.Equal(t, [][]string{{"v"}}, indexParts(t, "ordinary", "ix"))
			want := [][]string{{"1", "7"}, {"2", "7"}}
			require.Equal(t, want, query(t, "select id,v from ordinary force index(ix) where v=7 order by id"))
			require.Equal(t, want, query(t, "select id,v from ordinary ignore index(ix) where v=7 order by id"))
			require.Equal(t, want, query(t, "select id,v from replacement_view order by id"))
			require.Empty(t, query(t, "select id from ordinary force index(ix) where v=10"))
			exec(t, "update ordinary set v=9 where id=2")
			require.Equal(t, [][]string{{"2"}}, query(t, "select id from ordinary force index(ix) where v=9"))
		})
		t.Run("fulltext retains definition and rebuilds tokens", func(t *testing.T) {
			exec(t, "create table text_key(id int primary key,body text,fulltext key ft(body))")
			exec(t, "insert into text_key values(1,'oldtoken')")
			exec(t, "alter table text_key drop column body,add column body text")
			require.Equal(t, [][]string{{"body"}}, indexParts(t, "text_key", "ft"))
			require.Equal(t, [][]string{{"1", "NULL"}}, query(t, "select id,body from text_key"))
			require.Empty(t, query(t, "select id from text_key where match(body) against('oldtoken' in boolean mode)"))
			exec(t, "insert into text_key values(2,'newtoken')")
			require.Equal(t, [][]string{{"2"}}, query(t, "select id from text_key where match(body) against('newtoken' in boolean mode)"))
		})
		t.Run("fresh rename binds replacement index", func(t *testing.T) {
			exec(t, "create table renamed_key(id int primary key,v int,unique key uq(v))")
			exec(t, "insert into renamed_key values(1,10)")
			exec(t, "alter table renamed_key drop column v,add column tmp int default 7,rename column tmp to v")
			require.Equal(t, [][]string{{"1", "7"}}, query(t, "select id,v from renamed_key force index(uq) where v=7"))
			reject(t, "insert into renamed_key values(2,7)", 1062)
			exec(t, "alter table renamed_key drop column v,add column v int default 8,rename column v to x")
			require.Equal(t, [][]string{{"x"}}, indexParts(t, "renamed_key", "uq"))
			require.Equal(t, [][]string{{"1", "8"}}, query(t, "select id,x from renamed_key force index(uq) where x=8"))
			exec(t, "alter table renamed_key drop column x,add column x int,drop column x")
			require.Empty(t, indexParts(t, "renamed_key", "uq"))
		})
		t.Run("merged replacement releases constraint names", func(t *testing.T) {
			exec(t, "create table merged_key(id int primary key,a int,b int,w int,unique key u(a),unique key j(b),constraint u check(w>0))")
			exec(t, "alter table merged_key drop column a,add column a int,drop column b,rename column a to b,drop column b,drop constraint u")
			require.Empty(t, indexParts(t, "merged_key", "u"))
			require.Empty(t, indexParts(t, "merged_key", "j"))
			exec(t, "insert into merged_key values(1,-1)")
			require.Equal(t, [][]string{{"1", "-1"}}, query(t, "select id,w from merged_key"))
		})
		t.Run("replacement type still validates inherited key", func(t *testing.T) {
			exec(t, "create table prefix_key(id int primary key,v varchar(20),key ix(v(5)))")
			exec(t, "insert into prefix_key values(1,'original')")
			reject(t, "alter table prefix_key drop column v,add column v json", 0)
			require.Equal(t, [][]string{{"original"}}, query(t, "select v from prefix_key where id=1"))
			require.Equal(t, [][]string{{"v"}}, indexParts(t, "prefix_key", "ix"))
			exec(t, "alter table prefix_key drop column v,add column v varchar(20) default 'changed'")
			require.Contains(t, query(t, "show create table prefix_key")[0][1], "`v`(5)")
			require.Equal(t, [][]string{{"1"}}, query(t, "select id from prefix_key force index(ix) where v='changed'"))
		})
	})
}
