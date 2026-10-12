// Copyright 2021 - 2022 Matrix Origin
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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Public regression for concrete value-window types, RANGE NULL boundaries,
// and prepared default reuse across partition/cardinality transitions.
func TestWindowNullDomainAndRange(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn, "drop database if exists roi_window_reproduction")
		for _, q := range []string{"create database roi_window_reproduction", "use roi_window_reproduction", "create table r(id int primary key,k int,v int)", "insert into r values(1,NULL,100),(2,0,10),(3,1,20)", "create table defaults_input(point int,eid int,value double)", "insert into defaults_input values(1,1,1),(2,1,2),(3,2,3)"} {
			_, err := db.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		base := " from (select 1 point,1 eid,cast(1 as double) value union all select 2,1,2 union all select 3,2,3) f order by point"
		cases := []struct {
			name, q string
			want    []string
		}{
			{"29972-any-value", "select id,first_value(NULL) over(order by id) from r order by id", []string{"1:NULL", "2:NULL", "3:NULL"}},
			{"29972-typed-control", "select id,first_value(cast(NULL as signed)) over(order by id) from r order by id", []string{"1:NULL", "2:NULL", "3:NULL"}},
			{"29973-desc-following", "select id,count(*) over(order by k desc range between 1 following and 2 following),sum(v) over(order by k desc range between 1 following and 2 following) from r order by id", []string{"1:1:100", "2:0:NULL", "3:1:10"}},
			{"29925-lag-null", "select point,eid,lag(value,1,NULL) over(partition by eid order by point)" + base, []string{"1:1:NULL", "2:1:1", "3:2:NULL"}},
			{"29925-lag-typed-null", "select point,eid,lag(value,1,cast(NULL as double)) over(partition by eid order by point)" + base, []string{"1:1:NULL", "2:1:1", "3:2:NULL"}},
			{"29925-lag-omitted", "select point,eid,lag(value) over(partition by eid order by point)" + base, []string{"1:1:NULL", "2:1:1", "3:2:NULL"}},
			{"29925-lead-null", "select point,eid,lead(value,1,NULL) over(partition by eid order by point)" + base, []string{"1:1:2", "2:1:NULL", "3:2:NULL"}},
			{"29925-lead-typed-null", "select point,eid,lead(value,1,cast(NULL as double)) over(partition by eid order by point)" + base, []string{"1:1:2", "2:1:NULL", "3:2:NULL"}},
			{"29925-lead-omitted", "select point,eid,lead(value) over(partition by eid order by point)" + base, []string{"1:1:2", "2:1:NULL", "3:2:NULL"}},
		}
		for _, fn := range []string{"last_value(NULL)", "nth_value(NULL,2)", "lag(NULL)", "lead(NULL)", "lag(NULL,1,7)", "lead(NULL,1,7)"} {
			want := []string{"1:NULL", "2:NULL", "3:NULL"}
			if fn == "lag(NULL,1,7)" {
				want[0] = "1:7"
			}
			if fn == "lead(NULL,1,7)" {
				want[2] = "3:7"
			}
			cases = append(cases, struct {
				name, q string
				want    []string
			}{fn, "select id," + fn + " over(order by id) from r order by id", want})
		}
		for _, key := range []string{"cast(k as decimal(10,2))", "cast(k as double)"} {
			cases = append(cases, struct {
				name, q string
				want    []string
			}{"range/" + key, "select id,count(*) over(order by " + key + " desc range between 1 following and 2 following),sum(v) over(order by " + key + " desc range between 1 following and 2 following) from r order by id", []string{"1:1:100", "2:0:NULL", "3:1:10"}})
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				rows, err := db.QueryContext(ctx, tc.q)
				if err != nil {
					t.Errorf("query error: %v", err)
					return
				}
				defer rows.Close()
				cols, err := rows.Columns()
				require.NoError(t, err)
				var got []string
				for rows.Next() {
					values := make([]sql.NullFloat64, len(cols))
					args := make([]any, len(cols))
					for i := range values {
						args[i] = &values[i]
					}
					require.NoError(t, rows.Scan(args...))
					parts := make([]string, len(cols))
					for i, v := range values {
						parts[i] = "NULL"
						if v.Valid {
							parts[i] = fmt.Sprintf("%g", v.Float64)
						}
					}
					got = append(got, strings.Join(parts, ":"))
				}
				if err := rows.Err(); err != nil {
					t.Errorf("rows error: %v", err)
					return
				}
				t.Logf("got=%v want=%v", got, tc.want)
				require.Equal(t, tc.want, got)
			})
		}
		for _, fn := range []string{"lag", "lead"} {
			t.Run("29925-prepared-"+fn, func(t *testing.T) {
				stmt, err := db.PrepareContext(ctx, "select point,eid,"+fn+"(value,1,?) over(partition by eid order by point) from defaults_input where point>? order by point")
				require.NoError(t, err)
				defer stmt.Close()
				for _, tc := range []struct {
					def               any
					min               int
					wantLag, wantLead []string
				}{
					{nil, 0, []string{"1:1:NULL", "2:1:1", "3:2:NULL"}, []string{"1:1:2", "2:1:NULL", "3:2:NULL"}},
					{float64(7), 0, []string{"1:1:7", "2:1:1", "3:2:7"}, []string{"1:1:2", "2:1:7", "3:2:7"}},
					{nil, 1, []string{"2:1:NULL", "3:2:NULL"}, []string{"2:1:NULL", "3:2:NULL"}},
					{nil, 2, []string{"3:2:NULL"}, []string{"3:2:NULL"}},
					{nil, 3, nil, nil},
					{nil, 0, []string{"1:1:NULL", "2:1:1", "3:2:NULL"}, []string{"1:1:2", "2:1:NULL", "3:2:NULL"}},
				} {
					rows, err := stmt.QueryContext(ctx, tc.def, tc.min)
					if err != nil {
						t.Errorf("prepared def=%v min=%d: %v", tc.def, tc.min, err)
						continue
					}
					func() {
						defer rows.Close()
						var got []string
						for rows.Next() {
							var point, eid int
							var value sql.NullFloat64
							require.NoError(t, rows.Scan(&point, &eid, &value))
							v := "NULL"
							if value.Valid {
								v = fmt.Sprintf("%g", value.Float64)
							}
							got = append(got, fmt.Sprintf("%d:%d:%s", point, eid, v))
						}
						if err := rows.Err(); err != nil {
							t.Errorf("prepared rows: %v", err)
							return
						}
						want := tc.wantLag
						if fn == "lead" {
							want = tc.wantLead
						}
						t.Logf("prepared def=%v min=%d got=%v want=%v", tc.def, tc.min, got, want)
						require.Equal(t, want, got)
					}()
				}
			})
		}
		t.Run("29972-prepared-first-value", func(t *testing.T) {
			stmt, err := db.PrepareContext(ctx, "select id,first_value(?) over(order by id) from r order by id")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct {
				v    any
				want string
			}{{nil, "NULL"}, {int64(7), "7"}, {"text", "text"}, {[]byte("binary"), "binary"}, {nil, "NULL"}} {
				rows, err := stmt.QueryContext(ctx, tc.v)
				if err != nil {
					t.Errorf("prepared first_value(%v): %v", tc.v, err)
					continue
				}
				func() {
					defer rows.Close()
					count := 0
					for rows.Next() {
						var id int
						var value sql.NullString
						require.NoError(t, rows.Scan(&id, &value))
						got := "NULL"
						if value.Valid {
							got = value.String
						}
						if got != tc.want {
							t.Errorf("first_value(%v) id=%d got=%s want=%s", tc.v, id, got, tc.want)
						}
						count++
					}
					if err := rows.Err(); err != nil {
						t.Errorf("prepared first_value rows: %v", err)
						return
					}
					require.Equal(t, 3, count)
				}()
			}
		})

		for _, fn := range []string{"lag", "lead"} {
			t.Run("prepared-first-domain-"+fn, func(t *testing.T) {
				stmt, err := db.PrepareContext(ctx, "select id,"+fn+"(?,1,?) over(order by id) from r order by id")
				require.NoError(t, err)
				defer stmt.Close()
				for _, tc := range []struct {
					value, fallback any
					want, boundary  string
				}{
					{nil, int64(7), "NULL", "7"}, {int64(2), int64(3), "2", "3"},
					{"text", "fallback", "text", "fallback"}, {nil, nil, "NULL", "NULL"},
				} {
					rows, err := stmt.QueryContext(ctx, tc.value, tc.fallback)
					require.NoError(t, err)
					func() {
						defer rows.Close()
						count := 0
						for rows.Next() {
							var id int
							var value sql.NullString
							require.NoError(t, rows.Scan(&id, &value))
							want := tc.want
							if (fn == "lag" && id == 1) || (fn == "lead" && id == 3) {
								want = tc.boundary
							}
							got := "NULL"
							if value.Valid {
								got = value.String
							}
							require.Equal(t, want, got)
							count++
						}
						require.NoError(t, rows.Err())
						require.Equal(t, 3, count)
					}()
				}
			})
		}
		t.Run("ctas-flush-read", func(t *testing.T) {
			_, err := db.ExecContext(ctx, "create table materialized as select id,first_value(NULL) over(order by id) as fv,last_value(NULL) over(order by id) as lv,nth_value(NULL,2) over(order by id) as nv,lag(NULL) over(order by id) as lagv,lead(NULL) over(order by id) as leadv from r")
			require.NoError(t, err)
			rows, err := db.QueryContext(ctx, "select data_type from information_schema.columns where table_schema='roi_window_reproduction' and table_name='materialized' and column_name<>'id' order by column_name")
			require.NoError(t, err)
			func() {
				defer rows.Close()
				count := 0
				for rows.Next() {
					var typ string
					require.NoError(t, rows.Scan(&typ))
					require.Equal(t, "text", strings.ToLower(typ))
					count++
				}
				require.NoError(t, rows.Err())
				require.Equal(t, 5, count)
			}()
			var response string
			require.NoError(t, db.QueryRowContext(ctx, "select mo_ctl('dn','flush','roi_window_reproduction.materialized')").Scan(&response))
			var flush struct {
				Method string `json:"method"`
				Result []struct {
					ReturnStr string `json:"returnStr"`
				} `json:"result"`
			}
			require.NoError(t, json.Unmarshal([]byte(response), &flush))
			require.Equal(t, "Flush", flush.Method)
			require.NotEmpty(t, flush.Result, "at least one TN actually flushed the table")
			for _, result := range flush.Result {
				require.Equal(t, "OK", result.ReturnStr)
			}
			var total, nulls int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*),count(case when fv is null and lv is null and nv is null and lagv is null and leadv is null then 1 end) from materialized").Scan(&total, &nulls))
			require.Equal(t, 3, total)
			require.Equal(t, 3, nulls)
			var healthy int
			require.NoError(t, db.QueryRowContext(ctx, "select 1").Scan(&healthy))
			require.Equal(t, 1, healthy)
		})
	})
}
