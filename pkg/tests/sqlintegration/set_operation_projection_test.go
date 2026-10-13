// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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

func TestSetOperationExternalColumnPruning(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists set_external_pruning")
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		for _, q := range []string{"create database set_external_pruning", "use set_external_pruning"} {
			_, err = conn.ExecContext(ctx, q)
			require.NoError(t, err)
		}
		dir := t.TempDir()
		for _, tc := range []struct{ name, data string }{
			{"bad_tail", "1,notanint\n2,notanint\n"},
			{"bad_head", "notanint,7\n"},
			{"nullable", "\\N,notanint\n"},
			{"empty", ""},
		} {
			path, err := filepath.Abs(filepath.Join(dir, tc.name+".csv"))
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, []byte(tc.data), 0600))
			_, err = conn.ExecContext(ctx, fmt.Sprintf("create external table %s(a int,b int) infile{'filepath'='%s'} fields terminated by ',' lines terminated by '\\n'", tc.name, strings.ReplaceAll(path, "'", "''")))
			require.NoError(t, err)
		}
		for _, tc := range []struct {
			name, query string
			want        []string
		}{
			{"direct-control", "select a from bad_tail order by a", []string{"1", "2"}},
			{"unused-invalid-field", "select a from (select a,b from bad_tail union all select a,b from bad_tail) u order by a", []string{"1", "1", "2", "2"}},
			{"physical-second-field", "select b from (select a,b from bad_head union all select a,b from bad_head) u", []string{"7", "7"}},
			{"null", "select a from (select a,b from nullable union all select a,b from nullable) u", []string{"NULL", "NULL"}},
			{"empty", "select a from (select a,b from empty union all select a,b from empty) u", nil},
			{"error-record-text", "select e from (select __mo_error_text e from bad_tail union all select __mo_error_text from bad_tail) u order by e", []string{"1,notanint", "1,notanint", "2,notanint", "2,notanint"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rows, err := conn.QueryContext(ctx, tc.query)
				require.NoError(t, err)
				require.Equal(t, tc.want, readWindowNullRows(t, rows))
			})
		}
		t.Run("error-record-messages", func(t *testing.T) {
			rows, err := conn.QueryContext(ctx, "select e from (select __mo_error_message e from bad_tail union all select __mo_error_message from bad_tail) u")
			require.NoError(t, err)
			defer rows.Close()
			var count int
			for rows.Next() {
				var message sql.NullString
				require.NoError(t, rows.Scan(&message))
				require.True(t, message.Valid)
				require.NotEmpty(t, message.String)
				count++
			}
			require.NoError(t, rows.Err())
			require.Equal(t, 4, count)
		})
		// Expected conversion failures run last on separate connections. Protocol
		// recovery after an internal error is outside this pruning regression.
		for _, tc := range []struct{ name, query string }{
			{"direct-invalid", "select b from set_external_pruning.bad_tail"},
			{"union-invalid", "select b from (select a,b from set_external_pruning.bad_tail union all select a,b from set_external_pruning.bad_tail) u"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				check, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
				require.NoError(t, err)
				defer check.Close()
				rows, queryErr := check.QueryContext(ctx, tc.query)
				if queryErr == nil {
					defer rows.Close()
					for rows.Next() {
					}
					queryErr = rows.Err()
				}
				require.ErrorContains(t, queryErr, "notanint", tc.query)
			})
		}
	})
}

func TestSharedCTEUnionProjection(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists shared_cte_projection")
		for _, q := range []string{
			"create database shared_cte_projection", "use shared_cte_projection",
			"create table source_values(k int,n bigint,p varchar(44))",
			"insert into source_values select result%3+1,result,case when result%3=1 then NULL else '7' end from generate_series(1,1000) g",
		} {
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err)
		}
		numeric := "with c as (select k,max(n) payload from source_values group by k) "
		variable := "with c as (select k,max(p) payload from source_values group by k) "
		numericQuery := numeric + "select k from (select k,payload from c union all select k,payload from c) u order by k"
		variableQuery := variable + "select k from (select a.k,a.payload from c a join c b on a.k=b.k union all select 0,cast(NULL as varchar(44))) u order by k"
		for _, query := range []string{numericQuery, variableQuery} {
			rows, err := conn.QueryContext(ctx, "explain "+query)
			require.NoError(t, err)
			require.Equal(t, 2, strings.Count(strings.Join(readWindowNullRows(t, rows), "\n"), "Sink Scan"),
				"the public fixture must execute two readers of an admitted shared producer")
		}
		for _, tc := range []struct {
			name, query string
			want        []string
		}{
			{"numeric-duplicates", numericQuery, []string{"1", "1", "2", "2", "3", "3"}},
			{"variable-width", variableQuery, []string{"0", "1", "2", "3"}},
			{"independent-producer-control", "select k from (select a.k,a.payload from (select k,max(p) payload from source_values group by k) a join (select k,max(p) payload from source_values group by k) b on a.k=b.k union all select 0,cast(NULL as varchar(44))) u order by k", []string{"0", "1", "2", "3"}},
			{"retained-null-payload", variable + "select k,payload from (select k,payload from c union all select k,payload from c) u order by k", []string{"1:7", "1:7", "2:NULL", "2:NULL", "3:7", "3:7"}},
			{"having-demand", "with c as (select k,max(p) payload from source_values group by k having max(p)='7') select k from (select a.k,a.payload from c a join c b on a.k=b.k union all select 0,cast(NULL as varchar(44))) u order by k", []string{"0", "1", "3"}},
			{"count-carrier", variable + "select count(*) from c a join c b on a.k=b.k", []string{"3"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rows, err := conn.QueryContext(ctx, tc.query)
				require.NoError(t, err)
				require.Equal(t, tc.want, readWindowNullRows(t, rows))
			})
		}
		_, err = conn.ExecContext(ctx, "update source_values set p='invalid' where k=1")
		require.NoError(t, err)
		err = func() error {
			rows, err := conn.QueryContext(ctx, "with c as (select k,cast(max(p) as bigint) payload from source_values group by k) select k from (select k,payload from c union all select k,payload from c) u")
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
			}
			return rows.Err()
		}()
		require.Error(t, err, "unused fallible shared outputs must retain their error contract")
		_, err = conn.ExecContext(ctx, "delete from source_values")
		require.NoError(t, err)
		for _, query := range []string{numericQuery, variable + "select k from (select a.k,a.payload from c a join c b on a.k=b.k union all select 0,cast(NULL as varchar(44))) u where k>0"} {
			rows, err := conn.QueryContext(ctx, query)
			require.NoError(t, err)
			require.Empty(t, readWindowNullRows(t, rows))
		}
		rows, err := conn.QueryContext(ctx, "with c as (select 1 k,max(p) payload from source_values) select k from (select a.k,a.payload from c a join c b on a.k=b.k union all select 0,cast(NULL as varchar(44))) u order by k")
		require.NoError(t, err)
		require.Equal(t, []string{"0", "1"}, readWindowNullRows(t, rows), "scalar aggregate preserves its row on empty input")
	})
}

func TestFilteredSetOperationProjection(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		for _, tc := range []struct {
			name, query string
			want        []string
		}{
			{"projection", "select point,eid from (select 1 point,10 eid union all select 2,20) u where point>0 order by point", []string{"1:10", "2:20"}},
			{"aggregate", "select sum(eid) from (select 1 point,10 eid union all select 2,20) u where point>0", []string{"30"}},
			{"string", "select v from (select 1 k,'ten' v union all select 2,'twenty') u where k>0 order by k", []string{"ten", "twenty"}},
			{"unfiltered-control", "select sum(eid) from (select 1 point,10 eid union all select 2,20) u", []string{"30"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rows, err := conn.QueryContext(ctx, tc.query)
				require.NoError(t, err)
				require.Equal(t, tc.want, readWindowNullRows(t, rows))
			})
		}

		for _, tc := range []struct {
			name, query string
			want        []string
		}{
			{"unpruned-union-tuple", "select k from (select 1 k,10 v union select 1,20) u where k>0 order by k", []string{"1", "1"}},
			{"unpruned-intersect-tuple", "select k from (select 1 k,10 v intersect select 1,20) u where k>0", nil},
			{"unpruned-except-tuple", "select k from (select 1 k,10 v except select 1,20) u where k>0", []string{"1"}},
			{"nested-membership", "select sum(v) from ((select 1 k,10 v union select 1,20) union all select 2,30) u where k>0", []string{"60"}},
			{"count-carrier", "select count(*) from (select 1 k,10 v union all select 2,20) u where k>0", []string{"2"}},
			{"cte", "with u as (select 1 k,10 v union all select 2,20) select sum(v) from u where k>0", []string{"30"}},
			{"window", "select k,sum(v) over(order by k) from (select 1 k,10 v union all select 2,20) u where k>0 order by k", []string{"1:10", "2:30"}},
			{"null", "select v from (select 1 k,cast(NULL as signed) v union all select 2,20) u where k>0 order by k", []string{"NULL", "20"}},
			{"string-pruned", "select v from (select 1 k,'ten' v union all select 2,'twenty') u where k>0 order by v", []string{"ten", "twenty"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rows, err := conn.QueryContext(ctx, tc.query)
				require.NoError(t, err)
				require.Equal(t, tc.want, readWindowNullRows(t, rows))
			})
		}
		t.Run("prepared-reuse", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx, "select point,eid from (select 1 point,1 eid,cast(1 as double) value union all select 2,1,2 union all select 3,2,3) f where point>? order by point")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct {
				min  int
				want []string
			}{
				{0, []string{"1:1", "2:1", "3:2"}}, {1, []string{"2:1", "3:2"}}, {3, nil}, {0, []string{"1:1", "2:1", "3:2"}},
			} {
				rows, err := stmt.QueryContext(ctx, tc.min)
				require.NoError(t, err)
				require.Equal(t, tc.want, readWindowNullRows(t, rows))
			}
		})
		t.Run("function-columns", func(t *testing.T) {
			for _, tc := range []struct {
				name, query string
				want        []string
			}{
				{"path", `select path from (select n.path,n.value from unnest('[1,2]') n union all select n.path,n.value from unnest('[3,4]') n) u order by path`, []string{"$[0]", "$[0]", "$[1]", "$[1]"}},
				{"value", `select value from (select n.path,n.value from unnest('[1,2]') n union all select n.path,n.value from unnest('[3,4]') n) u order by value`, []string{"1", "2", "3", "4"}},
				{"filter-demand", `select path from (select n.path,n.value from unnest('[1,2]') n where cast(n.value as varchar)='1' union all select n.path,n.value from unnest('[3,4]') n where cast(n.value as varchar)='3') u order by path`, []string{"$[0]", "$[0]"}},
				{"membership-tuple", `select path from (select n.path,n.value from unnest('[1]') n union select n.path,n.value from unnest('[2]') n) u`, []string{"$[0]", "$[0]"}},
				{"count-carrier", `select count(*) from (select n.path,n.value from unnest('[1,2]') n union all select n.path,n.value from unnest('[3,4]') n) u`, []string{"4"}},
				{"empty", `select path from (select n.path,n.value from unnest('[]') n union all select n.path,n.value from unnest('[]') n) u`, nil},
				{"null-input", `select path from (select n.path,n.value from unnest(cast(NULL as json)) n union all select n.path,n.value from unnest(cast(NULL as json)) n) u`, nil},
				{"outer-empty-count", `select count(*) from (select n.path,n.value from unnest('[]','$',true) n union all select n.path,n.value from unnest('[]','$',true) n) u`, []string{"2"}},
				{"lateral-input", `select path from (select n.path,n.value from (select '[1,2]' j) t,unnest(t.j) n union all select n.path,n.value from (select '[3,4]' j) t,unnest(t.j) n) u order by path`, []string{"$[0]", "$[0]", "$[1]", "$[1]"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					rows, err := conn.QueryContext(ctx, tc.query)
					require.NoError(t, err)
					require.Equal(t, tc.want, readWindowNullRows(t, rows))
				})
			}
			stmt, err := conn.PrepareContext(ctx, `select path from (select n.path,n.value from unnest(?) n union all select n.path,n.value from unnest(?) n) u order by path`)
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct {
				arg  any
				want []string
			}{
				{"[1,2]", []string{"$[0]", "$[0]", "$[1]", "$[1]"}}, {"[]", nil}, {nil, nil}, {"[3]", []string{"$[0]", "$[0]"}},
			} {
				rows, err := stmt.QueryContext(ctx, tc.arg, tc.arg)
				require.NoError(t, err)
				require.Equal(t, tc.want, readWindowNullRows(t, rows))
			}
		})
		t.Run("persisted-values", func(t *testing.T) {
			defer cleanupSQLIntegration(t, cn, "drop database if exists set_projection_regression")
			for _, q := range []string{
				"create database set_projection_regression", "use set_projection_regression",
				"create table saved(v bigint)",
				"insert into saved select sum(v) from (select 1 k,10 v union all select 2,20) u where k>0",
				"create table copied as select v from (select 1 k,10 v union all select 2,20) u where k>0",
				"create table sample_values(a bigint not null,b bigint not null)",
				"insert into sample_values values (1,10),(2,20),(3,30)",
				"create table nullable_sample(a bigint,b bigint)",
				"insert into nullable_sample values (1,NULL),(NULL,20),(3,30)",
				"create table time_values(ts datetime,a bigint,b bigint)",
				"insert into time_values values ('2026-01-01 00:00:00',10,100),('2026-01-01 00:00:10',20,200),('2026-01-01 00:01:00',40,400)",
			} {
				_, err := conn.ExecContext(ctx, q)
				require.NoError(t, err, q)
			}
			_, err := conn.ExecContext(ctx, "create sequence effect_seq start with 1")
			require.NoError(t, err)
			rows, err := conn.QueryContext(ctx, "select count(*) from (select k,v+1 v from (select 1 k,nextval('effect_seq') v) a union all select 2,20) u where k>0")
			require.NoError(t, err)
			require.Equal(t, []string{"2"}, readWindowNullRows(t, rows))
			var effects string
			require.NoError(t, conn.QueryRowContext(ctx, "select currval('effect_seq')").Scan(&effects))
			require.Equal(t, "1", effects, "hidden branch expression must evaluate exactly once")
			_, err = conn.ExecContext(ctx, "create sequence function_effect_seq start with 1")
			require.NoError(t, err)
			rows, err = conn.QueryContext(ctx, `select count(*) from (select n.path,n.value from unnest(cast(concat('[',cast(nextval('function_effect_seq') as varchar),']') as json)) n union all select n.path,n.value from unnest(cast(concat('[',cast(nextval('function_effect_seq') as varchar),']') as json)) n) u`)
			require.NoError(t, err)
			require.Equal(t, []string{"2"}, readWindowNullRows(t, rows))
			require.NoError(t, conn.QueryRowContext(ctx, "select currval('function_effect_seq')").Scan(&effects))
			require.Equal(t, "2", effects, "pruned function outputs must retain exactly-once argument effects")

			for _, tc := range []struct {
				query string
				want  []string
			}{
				{"select c from (select count(*) c,sum(v) s from saved union all select count(*),sum(v) from copied) u order by c", []string{"1", "2"}},
				{"select s from (select count(*) c,sum(v) s from saved union all select count(*),sum(v) from copied) u order by s", []string{"30", "30"}},
				{"select c,s from (select count(*) c,sum(v) s from saved union all select count(*),sum(v) from copied) u order by c", []string{"1:30", "2:30"}},
				{"select c from (select count(*) c,sum(v) s from saved where v<0 union all select count(*),sum(v) from copied where v<0) u", []string{"0", "0"}},
				{"select s from (select count(*) c,sum(v) s from saved where v<0 union all select count(*),sum(v) from copied where v<0) u", []string{"NULL", "NULL"}},
				{"select 1 from (select count(*) c,sum(v) s from saved where v<0 union all select count(*),sum(v) from copied where v<0) u", []string{"1", "1"}},
				{"select c from (select count(*) c,avg(v) a,sum(v) s from saved group by v having sum(v)>15 union all select count(*),avg(v),sum(v) from copied group by v having sum(v)>15) u order by c", []string{"1", "1"}},
				{"select c from (select count(*) c,sum(v) s from saved union select count(*),sum(v) from copied where v=10) u order by c", []string{"1", "1"}},
				{"select v from (select v,sum(v) over() w from saved union all select v,sum(v) over() from copied) u order by v", []string{"10", "20", "30"}},
				{"select v,w from (select v,sum(v) over() w from saved union all select v,sum(v) over() from copied) u order by v", []string{"10:30", "20:30", "30:30"}},
				{"select v from (select v,sum(v) over(partition by v order by v) w from saved union all select v,sum(v) over(partition by v order by v) from copied) u order by v", []string{"10", "20", "30"}},
				{"select v from (select v,sum(v) over() w from saved union all select v,sum(v) over() from copied) u where w>15 order by v", []string{"10", "20", "30"}},
				{"select count(*) from (select sample(a,b,2 rows) from sample_values union all select sample(a,b,2 rows) from sample_values) u", []string{"4"}},
				{"select count(*) from (select sample(a,b,10 rows) from nullable_sample union all select sample(a,b,10 rows) from nullable_sample) u", []string{"6"}},
				{"select count(*) from (select sample(a,b,10 rows) from nullable_sample union all select sample(a,b,10 rows) from nullable_sample) u where a is null", []string{"2"}},
				{"select c from (select c,s from (select sum(a) c,sum(b) s from time_values interval(ts,1,minute)) t union all select c,s from (select sum(a) c,sum(b) s from time_values interval(ts,1,minute)) t) u order by c", []string{"30", "30", "40", "40"}},
				{"select s from (select c,s from (select sum(a) c,sum(b) s from time_values interval(ts,1,minute)) t union all select c,s from (select sum(a) c,sum(b) s from time_values interval(ts,1,minute)) t) u order by s", []string{"300", "300", "400", "400"}},
				{"select c,s from (select c,s from (select sum(a) c,sum(b) s from time_values interval(ts,1,minute)) t union all select c,s from (select sum(a) c,sum(b) s from time_values interval(ts,1,minute)) t) u order by c", []string{"30:300", "30:300", "40:400", "40:400"}},
				{"select c from (select c,s from (select sum(a) c,sum(b) s from time_values interval(ts,1,minute) fill(null)) t union all select c,s from (select sum(a) c,sum(b) s from time_values interval(ts,1,minute) fill(null)) t) u order by c", []string{"30", "30", "40", "40"}},
				{"select count(*) from (select c,s from (select sum(a) c,sum(b) s from time_values where a<0 interval(ts,1,minute)) t union all select c,s from (select sum(a) c,sum(b) s from time_values where a<0 interval(ts,1,minute)) t) u", []string{"0"}},
				{"select c from (select c,s from (select sum(a) c,sum(b) s from time_values having sum(b)>300 interval(ts,1,minute)) t union all select c,s from (select sum(a) c,sum(b) s from time_values having sum(b)>300 interval(ts,1,minute)) t) u", []string{"40", "40"}},
				{"select c from (select c,s,w from (select sum(a) c,sum(b) s,_wstart w from time_values interval(ts,1,minute) fill(prev)) t union all select c,s,w from (select sum(a) c,sum(b) s,_wstart w from time_values interval(ts,1,minute) fill(prev)) t) u order by c", []string{"30", "30", "40", "40"}},
				{"select v from saved", []string{"30"}},
				{"select v from copied order by v", []string{"10", "20"}},
				{"select sum(v) from (select 1 k,10 v union all select 2,v from copied where v=20) u where k>0", []string{"30"}},
			} {
				rows, err := conn.QueryContext(ctx, tc.query)
				require.NoError(t, err)
				require.Equal(t, tc.want, readWindowNullRows(t, rows))
			}
			_, err = conn.ExecContext(ctx, "create sequence aggregate_effect_seq start with 1")
			require.NoError(t, err)
			rows, err = conn.QueryContext(ctx, "select c from (select count(*) c,sum(cast(nextval('aggregate_effect_seq') as bigint)) s from saved union all select count(*),sum(cast(nextval('aggregate_effect_seq') as bigint)) from copied) u order by c")
			require.NoError(t, err)
			require.Equal(t, []string{"1", "2"}, readWindowNullRows(t, rows))
			require.NoError(t, conn.QueryRowContext(ctx, "select currval('aggregate_effect_seq')").Scan(&effects))
			require.Equal(t, "3", effects, "unused aggregate arguments must evaluate once per input row")
			_, err = conn.ExecContext(ctx, "create sequence window_effect_seq start with 1")
			require.NoError(t, err)
			rows, err = conn.QueryContext(ctx, "select v from (select v,sum(cast(nextval('window_effect_seq') as bigint)) over() w from saved union all select v,sum(cast(nextval('window_effect_seq') as bigint)) over() from copied) u order by v")
			require.NoError(t, err)
			require.Equal(t, []string{"10", "20", "30"}, readWindowNullRows(t, rows))
			require.NoError(t, conn.QueryRowContext(ctx, "select currval('window_effect_seq')").Scan(&effects))
			require.Equal(t, "3", effects, "unused window arguments must evaluate once per input row")
			_, err = conn.ExecContext(ctx, "create sequence time_effect_seq start with 1")
			require.NoError(t, err)
			rows, err = conn.QueryContext(ctx, "select c from (select c,s from (select sum(a) c,sum(cast(nextval('time_effect_seq') as bigint)) s from time_values interval(ts,1,minute)) t union all select c,s from (select sum(a) c,sum(cast(nextval('time_effect_seq') as bigint)) s from time_values interval(ts,1,minute)) t) u order by c")
			require.NoError(t, err)
			require.Equal(t, []string{"30", "30", "40", "40"}, readWindowNullRows(t, rows))
			require.NoError(t, conn.QueryRowContext(ctx, "select currval('time_effect_seq')").Scan(&effects))
			require.Equal(t, "6", effects, "unused time-window aggregate arguments must evaluate once per input row")
			_, err = conn.ExecContext(ctx, "create sequence sample_effect_seq start with 1")
			require.NoError(t, err)
			rows, err = conn.QueryContext(ctx, "select count(*) from (select sample(a,cast(nextval('sample_effect_seq') as bigint),10 rows) from sample_values union all select sample(a,cast(nextval('sample_effect_seq') as bigint),10 rows) from sample_values) u")
			require.NoError(t, err)
			require.Equal(t, []string{"6"}, readWindowNullRows(t, rows))
			require.NoError(t, conn.QueryRowContext(ctx, "select currval('sample_effect_seq')").Scan(&effects))
			require.Equal(t, "6", effects, "unused sample arguments must evaluate once per input row")
		})
		var one int
		require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&one))
		require.Equal(t, 1, one)
	})
}
