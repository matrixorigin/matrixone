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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIntegerIndexAccess(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists integer_index_access")
		exec := func(q string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		query := func(q string) [][]string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, q)
			require.NoError(t, err, q)
			defer rows.Close()
			cols, err := rows.Columns()
			require.NoError(t, err)
			result := make([][]string, 0)
			for rows.Next() {
				values := make([]sql.NullString, len(cols))
				dest := make([]any, len(cols))
				for i := range values {
					dest[i] = &values[i]
				}
				require.NoError(t, rows.Scan(dest...))
				row := make([]string, len(cols))
				for i, v := range values {
					row[i] = "NULL"
					if v.Valid {
						row[i] = v.String
					}
				}
				result = append(result, row)
			}
			require.NoError(t, rows.Err(), q)
			return result
		}
		exec("create database integer_index_access")
		exec("use integer_index_access")
		exec("create table t(id int primary key,k int,v int,unique key uq(k))")
		exec("insert into t values(1,NULL,11),(2,NULL,22),(3,1,33)")
		for _, suffix := range []string{"order by id", "where k is null order by id", "where k <=> null order by id", "where k=1 order by id"} {
			require.Equal(t, query("select id,k,v from t ignore index(uq) "+suffix), query("select id,k,v from t force index(uq) "+suffix), suffix)
			require.Equal(t, query("select id,k from t ignore index(uq) "+suffix), query("select id,k from t force index(uq) "+suffix), suffix)
		}
		require.Equal(t, [][]string{{"NULL", "2"}, {"1", "1"}}, query("select k,count(*) from t force index(uq) group by k order by k"))
		require.Equal(t, [][]string{{"NULL", "2"}, {"1", "1"}}, query("select k,count(*) from t force index for group by(uq) group by k order by k"))
		exec("create table copied as select id,k,v from t force index(uq) order by id")
		require.Equal(t, query("select * from t order by id"), query("select * from copied order by id"))
		exec("prepare sparse from 'select id,k,v from t force index(uq) where k is null order by id'")
		require.Equal(t, [][]string{{"1", "NULL", "11"}, {"2", "NULL", "22"}}, query("execute sparse"))
		exec("deallocate prepare sparse")
		func() {
			exec("begin")
			defer func() {
				rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer rollbackCancel()
				_, err := conn.ExecContext(rollbackCtx, "rollback")
				require.NoError(t, err)
			}()
			exec("update t set k=2 where id=1")
			require.Equal(t, query("select id,k from t ignore index(uq) order by id"), query("select id,k from t force index(uq) order by id"))
		}()
		require.Equal(t, [][]string{{"1", "NULL"}, {"2", "NULL"}, {"3", "1"}}, query("select id,k from t force index(uq) order by id"))
		exec("create table u(id int primary key,a int,b int,v int,unique key uq(a,b))")
		exec("insert into u values(1,NULL,NULL,11),(2,NULL,2,22),(3,1,NULL,33),(4,1,2,44),(5,3,4,55)")
		for _, predicate := range []string{"true", "a=1", "a is not null", "a=1 or b=2", "a is not null and b is not null", "(a=1 and b=2) or (a=3 and b=4)", "a is null or b is null"} {
			for _, cols := range []string{"id,a,b", "id,a,b,v"} {
				require.Equal(t, query("select "+cols+" from u ignore index(uq) where "+predicate+" order by id"), query("select "+cols+" from u force index(uq) where "+predicate+" order by id"), predicate)
			}
		}
		exec("create table ints(id bigint primary key)")
		exec("insert into ints values(-9223372036854775808),(0),(3),(9007199254740993),(9223372036854775807)")
		for _, tc := range []struct {
			predicate string
			expected  [][]string
		}{
			{"id=3.0", [][]string{{"3"}}}, {"3.0=id", [][]string{{"3"}}},
			{"id=9007199254740993.0", [][]string{{"9007199254740993"}}},
			{"id=-9223372036854775808.0", [][]string{{"-9223372036854775808"}}},
			{"id=9223372036854775807.0", [][]string{{"9223372036854775807"}}},
			{"id=9223372036854775808.0", [][]string{}}, {"id=3.1", [][]string{}},
			{"id=3.0000000000000000000000000000000000000000", [][]string{{"3"}}},
			{"id=0.00000000000000000000000000000000000001", [][]string{}},
		} {
			require.Equal(t, tc.expected, query("select id from ints where "+tc.predicate), tc.predicate)
		}
		exec("create table unsigned_ints(id bigint unsigned primary key)")
		exec("insert into unsigned_ints values(0),(18446744073709551615)")
		require.Equal(t, [][]string{{"18446744073709551615"}}, query("select id from unsigned_ints where id=18446744073709551615.0"))
		require.Empty(t, query("select id from unsigned_ints where id=-1.0"))
		require.Empty(t, query("select id from unsigned_ints where id=18446744073709551616.0"))
		for _, predicate := range []string{"id=3.0", "3.0<id", "id in (1.0,3.0)", "id between 1.0 and 3.0"} {
			text := ""
			for _, row := range query("explain select id from ints where " + predicate) {
				text += strings.Join(row, " ")
			}
			require.Contains(t, text, "Block Filter Cond", predicate)
			require.NotContains(t, text, "cast(ints.id", predicate)
		}
		exec("prepare exact from 'select id from ints where id=?'")
		for _, value := range []string{"3.0", "3.1", "9223372036854775808.0", "3.0"} {
			exec("set @value=" + value)
			want := [][]string{}
			if value == "3.0" {
				want = [][]string{{"3"}}
			}
			require.Equal(t, want, query("execute exact using @value"), value)
		}
		exec("deallocate prepare exact")
	})
}
