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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

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
