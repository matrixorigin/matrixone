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

// Issues #30290 and #30292 share temporal frame normalization, including
// execution-time source types and the difference between NULL and zero offsets.
func TestWindowTemporalIntervals(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn, "drop database if exists window_interval_regression")
		for _, q := range []string{
			"create database window_interval_regression", "use window_interval_regression",
			"create table dt(id int primary key, at datetime(6), v int)",
			"insert into dt values (1,'2026-01-01 00:00:00',10),(2,'2026-01-01 00:00:00.000500',20),(3,'2026-01-01 00:00:01.499999',30),(4,'2026-01-01 00:00:01.500000',40),(5,'2026-01-01 00:00:01.500001',50),(6,'2026-01-01 00:00:02',60)",
		} {
			execSQLRequire(t, ctx, db, q)
		}
		query := func(t *testing.T, q string, args ...any) []string {
			t.Helper()
			rows, err := db.QueryContext(ctx, q, args...)
			require.NoError(t, err, q)
			return readWindowNullRows(t, rows)
		}
		frameQuery := func(bound string) string {
			return "select id,sum(v) over(order by at range between interval " + bound + " preceding and current row) from dt order by id"
		}
		zero := []string{"1:10", "2:20", "3:30", "4:40", "5:50", "6:60"}
		one := []string{"1:10", "2:30", "3:30", "4:70", "5:120", "6:180"}
		fraction := []string{"1:10", "2:30", "3:60", "4:100", "5:140", "6:180"}
		for _, bound := range []string{"'0 00:00:01.5' day_second", "'00:00:01.5' hour_second", "'00:01.5' minute_second"} {
			t.Run(bound, func(t *testing.T) {
				require.Equal(t, []string{"1:NULL", "2:NULL", "3:NULL", "4:NULL", "5:NULL", "6:NULL"}, query(t, frameQuery(bound)))
			})
		}
		t.Run("invalid string is zero", func(t *testing.T) {
			require.Equal(t, zero, query(t, frameQuery("'abc' day")))
		})
		t.Run("literal controls", func(t *testing.T) {
			require.Equal(t, fraction, query(t, frameQuery("1.5 second")))
			require.Equal(t, one, query(t, frameQuery("'0:01' minute_second")))
			require.Equal(t, one, query(t, frameQuery("true second")))
			require.Equal(t, zero, query(t, frameQuery("false second")))
		})
		t.Run("NULL interval consumers", func(t *testing.T) {
			for _, typ := range []string{"date", "time(6)", "datetime(6)", "timestamp(6)"} {
				q := "select id,count(*) over w,sum(v) over w,first_value(v) over w from dt window w as (order by cast(at as " + typ + ") desc range between current row and interval '00:01.5' minute_second following) order by id"
				require.Equal(t, []string{"1:0:NULL:NULL", "2:0:NULL:NULL", "3:0:NULL:NULL", "4:0:NULL:NULL", "5:0:NULL:NULL", "6:0:NULL:NULL"}, query(t, q))
			}
		})
		t.Run("SQL prepared reuse", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "prepare frame_param from '"+frameQuery("? second")+"'")
			defer execSQLRequire(t, ctx, db, "deallocate prepare frame_param")
			for _, tc := range []struct {
				value string
				want  []string
			}{{"0", zero}, {"true", one}, {"false", zero}, {"1", one}, {"1.5", fraction}, {"cast('0.000001' as decimal(38,6))", []string{"1:10", "2:20", "3:30", "4:70", "5:90", "6:60"}}, {"0.0000009", zero}, {"2", []string{"1:10", "2:30", "3:60", "4:100", "5:150", "6:210"}}, {"0", zero}} {
				execSQLRequire(t, ctx, db, "set @frame_offset="+tc.value)
				require.Equal(t, tc.want, query(t, "execute frame_param using @frame_offset"))
			}
		})
		t.Run("binary prepared reuse and failures", func(t *testing.T) {
			stmt, err := db.PrepareContext(ctx, frameQuery("? second"))
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct {
				value any
				want  []string
			}{{int64(0), zero}, {true, one}, {false, zero}, {int64(1), one}, {"1.5", fraction}, {float64(1.5), fraction}, {int64(0), zero}} {
				func() {
					rows, err := stmt.QueryContext(ctx, tc.value)
					require.NoError(t, err)
					require.Equal(t, tc.want, readWindowNullRows(t, rows))
				}()
			}
			for _, value := range []any{int64(-1), nil, "9223372036854775808"} {
				func() {
					rows, err := stmt.QueryContext(ctx, value)
					if err != nil {
						return
					}
					defer rows.Close()
					require.False(t, rows.Next(), "offset %v", value)
					require.Error(t, rows.Err(), "offset %v", value)
				}()
			}
			for _, tc := range []struct {
				value any
				want  []string
			}{{true, one}, {false, zero}, {int64(0), zero}} {
				rows, err := stmt.QueryContext(ctx, tc.value)
				require.NoError(t, err)
				require.Equal(t, tc.want, readWindowNullRows(t, rows))
			}
		})
		t.Run("empty input still validates parameters", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create table empty_dt like dt")
			q := strings.ReplaceAll(frameQuery("? second"), "from dt", "from empty_dt")
			execSQLRequire(t, ctx, db, "prepare empty_frame from '"+q+"'")
			defer execSQLRequire(t, ctx, db, "deallocate prepare empty_frame")
			for _, value := range []string{"-1", "NULL", "9223372036854.775808"} {
				execSQLRequire(t, ctx, db, "set @frame_offset="+value)
				_, err := db.ExecContext(ctx, "execute empty_frame using @frame_offset")
				require.Error(t, err, value)
			}
			execSQLRequire(t, ctx, db, "set @frame_offset=0")
			require.Empty(t, query(t, "execute empty_frame using @frame_offset"))
		})
		t.Run("LIMIT zero still validates parameters", func(t *testing.T) {
			for _, tc := range []struct {
				name, sql string
				dynamic   bool
			}{
				{"literal", frameQuery("? second") + " limit 0", false},
				{"outer", "select * from (" + frameQuery("? second") + ") q limit 0", false},
				{"CTE", "with q as (" + frameQuery("? second") + ") select * from q limit 0", false},
				{"parameter", frameQuery("? second") + " limit ?", true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					execSQLRequire(t, ctx, db, "prepare zero_frame from '"+tc.sql+"'")
					defer execSQLRequire(t, ctx, db, "deallocate prepare zero_frame")
					execSQLRequire(t, ctx, db, "set @frame_limit=0")
					execute := "execute zero_frame using @frame_offset"
					if tc.dynamic {
						execute += ",@frame_limit"
					}
					values := []string{"0", "-1", "NULL", "9223372036854.775808", "1.5", "-1", "0"}
					if tc.name == "literal" {
						values = append(values, "true", "false")
					}
					for _, value := range values {
						execSQLRequire(t, ctx, db, "set @frame_offset="+value)
						if value == "0" || value == "1.5" || value == "true" || value == "false" {
							require.Empty(t, query(t, execute))
						} else {
							_, err := db.ExecContext(ctx, execute)
							require.Error(t, err, value)
						}
					}
					stmt, err := db.PrepareContext(ctx, tc.sql)
					require.NoError(t, err)
					defer stmt.Close()
					argsToTest := []any{int64(0), int64(-1), nil, "9223372036854.775808", "1.5", int64(-1), int64(0)}
					if tc.name == "literal" {
						argsToTest = append(argsToTest, true, false)
					}
					for _, value := range argsToTest {
						func() {
							args := []any{value}
							if tc.dynamic {
								args = append(args, int64(0))
							}
							rows, err := stmt.QueryContext(ctx, args...)
							valid := value == int64(0) || value == "1.5" || value == true || value == false
							if valid {
								require.NoError(t, err)
								require.Empty(t, readWindowNullRows(t, rows))
							} else if err == nil {
								defer rows.Close()
								require.False(t, rows.Next())
								require.Error(t, rows.Err(), "offset %v", value)
							}
						}()
					}
					if tc.dynamic {
						execSQLRequire(t, ctx, db, "set @frame_limit=1")
						require.Equal(t, zero[:1], query(t, execute))
					}
				})
			}
		})
		t.Run("NULL interval keeps NULL order peers", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "insert into dt values (7,NULL,70),(8,NULL,80)")
			require.Equal(t, []string{"1:NULL", "2:NULL", "3:NULL", "4:NULL", "5:NULL", "6:NULL", "7:150", "8:150"}, query(t, frameQuery("'00:01.5' minute_second")))
			for _, direction := range []string{"asc", "desc"} {
				q := "select id,count(*) over w,cast(avg(v) over w as decimal(10,1)) from dt window w as (order by at " + direction + " range between interval '00:01.5' minute_second preceding and current row) order by id"
				require.Equal(t, []string{"1:0:NULL", "2:0:NULL", "3:0:NULL", "4:0:NULL", "5:0:NULL", "6:0:NULL", "7:2:75.0", "8:2:75.0"}, query(t, q))
			}
		})
	})
}
