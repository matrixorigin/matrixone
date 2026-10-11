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

func TestWindowScalarSubqueryInputs(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn, "drop database if exists window_subquery_inputs")
		exec := func(s string) { t.Helper(); execSQLRequire(t, ctx, db, s) }
		exec("drop database if exists window_subquery_inputs")
		exec("create database window_subquery_inputs")
		exec("use window_subquery_inputs")
		exec("create table src(id int,v int)")
		exec("insert into src values(1,10),(1,20),(2,30)")
		query := func(t *testing.T, s string) []string {
			t.Helper()
			rows, e := db.QueryContext(ctx, s)
			require.NoError(t, e, s)
			defer rows.Close()
			cols, e := rows.Columns()
			require.NoError(t, e)
			var got []string
			for rows.Next() {
				values := make([]sql.NullString, len(cols))
				args := make([]any, len(cols))
				for i := range values {
					args[i] = &values[i]
				}
				require.NoError(t, rows.Scan(args...))
				fields := make([]string, len(cols))
				for i, v := range values {
					if v.Valid {
						fields[i] = v.String
					} else {
						fields[i] = "NULL"
					}
				}
				got = append(got, strings.Join(fields, ":"))
			}
			require.NoError(t, rows.Err(), s)
			return got
		}
		// Compare partition membership and complete row-number sets, not arbitrary tie winners.
		for _, key := range []string{"(select null)", "(select 1)", "coalesce((select 1),0)", "(select max(v) from src)", "null", "cast(null as int)", "1", "id"} {
			t.Run("constant_peers/"+key, func(t *testing.T) {
				s := "select id,rn from (select id,row_number() over(partition by id order by " + key + ") rn from src) w order by id,rn"
				require.Equal(t, []string{"1:1", "1:2", "2:1"}, query(t, s))
			})
		}
		for _, tc := range []struct {
			name, sql string
			want      []string
		}{
			{"partition correlation", "select id,v,row_number() over(partition by (select src.id) order by v) from src order by id,v", []string{"1:10:1", "1:20:2", "2:30:1"}},
			{"order correlation", "select id,v,row_number() over(partition by id order by (select -src.v)) from src order by id,v", []string{"1:10:2", "1:20:1", "2:30:1"}},
			{"table correlation", "select id,v,row_number() over(partition by id order by (select max(s.v) from src s where s.v=src.v) desc) from src order by id,v", []string{"1:10:2", "1:20:1", "2:30:1"}},
			{"function argument", "select id,v,sum((select src.v)) over(partition by id) from src order by id,v", []string{"1:10:30", "1:20:30", "2:30:30"}},
			{"grouped input", "select id,sum(v),sum(sum(v)) over(order by (select max(v) from src)) from src group by id order by id", []string{"1:30:60", "2:30:60"}},
			{"grouped correlation", "select id,sum(v),row_number() over(order by (select count(*) from src s where s.id=src.id)) from src group by id order by id", []string{"1:30:2", "2:30:1"}},
			{"having before window", "select id,sum(v),row_number() over(order by (select max(s.v) from src s where s.id=src.id)) from src group by id having count(*)>1", []string{"1:30:1"}},
			{"count empty input", "select id,v,sum((select count(*) from src s where s.id=99)) over(partition by id) from src order by id,v", []string{"1:10:0", "1:20:0", "2:30:0"}},
			{"empty scalar", "select id,v,rank() over(partition by id order by (select v from src where id=99)) from src order by id,v", []string{"1:10:1", "1:20:1", "2:30:1"}},
			{"multiple windows and range peers", "select id,v,rank() over(partition by id order by (select 1)),dense_rank() over(partition by id order by null),sum(v) over(partition by id order by (select null)) from src order by id,v", []string{"1:10:1:1:30", "1:20:1:1:30", "2:30:1:1:30"}},
			{"rows frame", "select id,v,sum(v) over(partition by id order by (select src.v) rows between 1 preceding and current row) from src order by id,v", []string{"1:10:10", "1:20:30", "2:30:30"}},
			{"secondary null preserves keys", "select id,v,rank() over(order by id,null) from src order by id,v", []string{"1:10:1", "1:20:1", "2:30:3"}},
			{"null partition", "select id,v,row_number() over(partition by null order by v) from src order by v", []string{"1:10:1", "1:20:2", "2:30:3"}},
			{"conditional scalar", "select id,v,row_number() over(partition by id order by case when v=10 then (select 1) else (select 2) end) from src order by id,v", []string{"1:10:1", "1:20:2", "2:30:1"}},
			{"empty input", "select row_number() over(order by (select 1)) from src where id<0", nil},
		} {
			t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, query(t, tc.sql)) })
		}
		for _, s := range []string{
			"select row_number() over(order by (select v from src)) from src",
			"select row_number() over(order by (select 1,2)) from src",
			"select row_number() over(partition by (select 1,2)) from src",
		} {
			t.Run("invalid_scalar/"+s, func(t *testing.T) {
				rows, e := db.QueryContext(ctx, s)
				if e == nil {
					defer rows.Close()
					for rows.Next() {
					}
					e = rows.Err()
				}
				require.Error(t, e, s)
				if strings.Contains(s, "select v from src") {
					require.Contains(t, strings.ToLower(e.Error()), "subquery returns more than 1 row", s)
				} else {
					require.Contains(t, e.Error(), "subquery should return 1 column", s)
				}
			})
		}
		// Prepared reuse keeps the producer's values per execution, not per plan.
		stmt, err := db.PrepareContext(ctx, "select count(*) from (select row_number() over(order by (select 1)) rn from src where id>?) w")
		require.NoError(t, err)
		defer stmt.Close()
		for _, tc := range []struct{ min, want int }{{0, 3}, {1, 1}, {2, 0}, {0, 3}} {
			var n int
			require.NoError(t, stmt.QueryRowContext(ctx, tc.min).Scan(&n))
			require.Equal(t, tc.want, n)
		}
		// One row beyond the standard input batch boundary; no volume workload.
		exec("create table batches(id int)")
		exec("insert into batches select result from generate_series(1,8200) g")
		require.Equal(t, []string{"8200:1:8200:33624100"}, query(t, "select count(*),min(rn),max(rn),sum(rn) from (select row_number() over(order by (select 1)) rn from batches) w"))
		require.Equal(t, []string{"8200:1:8200:33624100"}, query(t, "select count(*),min(rn),max(rn),sum(rn) from (select row_number() over(partition by null order by (select null)) rn from batches) w"))
	})
}
