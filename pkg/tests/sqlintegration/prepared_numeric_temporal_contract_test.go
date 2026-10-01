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

// One existing one-CN fixture exercises the frontend, both prepared protocols,
// persistence and metadata. Each closure uses only its distinguishing rows.
func TestPreparedNumericTemporalContracts(t *testing.T) {
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
		const schema = "prepared_numeric_temporal_contract"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(t *testing.T, q string, args ...any) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q, args...)
			require.NoError(t, err, q)
		}
		exec(t, "drop database if exists "+schema)
		exec(t, "create database "+schema)
		exec(t, "use "+schema)
		scalar := func(t *testing.T, q string, args ...any) string {
			t.Helper()
			var s string
			require.NoError(t, conn.QueryRowContext(ctx, q, args...).Scan(&s), q)
			return s
		}
		t.Run("decimal scientific values and persistence", func(t *testing.T) {
			exec(t, "create table source(v varchar(128))")
			exec(t, "insert into source values ('1E-2'),('-1E-2'),('0E2')")
			for _, typ := range []string{"decimal(5,2)", "decimal(20,8)", "decimal(38,18)", "decimal(65,10)"} {
				// Exact independent value oracle; equality with e also checks spelling invariance.
				require.Equal(t, "1", scalar(t, "select cast('1E-2' as "+typ+")=cast('0.01' as "+typ+")"))
				func() {
					p, err := conn.PrepareContext(ctx, "select cast(? as "+typ+")=cast('0.01' as "+typ+")")
					require.NoError(t, err)
					defer p.Close()
					var same int
					require.NoError(t, p.QueryRowContext(ctx, "1E-2").Scan(&same))
					require.Equal(t, 1, same)
				}()
			}
			var longFraction string
			err := conn.QueryRowContext(ctx, "select cast(? as decimal(5,2))", "0."+strings.Repeat("0", 60)+"123E61").Scan(&longFraction)
			t.Logf("long fractional scientific cast value=%s error=%v", longFraction, err)
			require.Error(t, err, "unsupported long nonintegral mantissa must not silently become zero")
			exec(t, "create table converted(v decimal(5,2))")
			exec(t, "insert into converted select cast(v as decimal(5,2)) from source")
			require.Equal(t, "-0.01,0.00,0.01", scalar(t, "select group_concat(v order by v) from converted"))
			exec(t, "prepare d from 'select cast(? as decimal(5,2))'")
			defer exec(t, "deallocate prepare d")
			for _, tc := range []struct{ input, want string }{{"1E-2", "0.01"}, {"0E2", "0.00"}, {"1E10", "999.99"}, {"-1E10", "-999.99"}} {
				exec(t, "set @d=?", tc.input)
				require.Equal(t, tc.want, scalar(t, "execute d using @d"))
			}
		})
		t.Run("signed prepared identity and DML", func(t *testing.T) {
			ids := func(rows *sql.Rows, err error) []int64 {
				t.Helper()
				require.NoError(t, err)
				defer rows.Close()
				out := []int64{}
				for rows.Next() {
					var v int64
					require.NoError(t, rows.Scan(&v))
					out = append(out, v)
				}
				require.NoError(t, rows.Err())
				return out
			}

			exec(t, "create table keys_t(id bigint primary key,n int)")
			exec(t, "insert into keys_t values(9007199254740992,0),(9007199254740993,0),(9007199254740994,0)")
			target := int64(9007199254740993)
			for _, tc := range []struct {
				op   string
				want []int64
			}{{"=", []int64{target}}, {"<>", []int64{target - 1, target + 1}}, {"<", []int64{target - 1}}, {"<=", []int64{target - 1, target}}, {">", []int64{target + 1}}, {">=", []int64{target, target + 1}}} {
				func() {
					p, err := conn.PrepareContext(ctx, "select id from keys_t where id"+tc.op+"? order by id")
					require.NoError(t, err)
					defer p.Close()
					require.Equal(t, tc.want, ids(p.QueryContext(ctx, "9007199254740993")))
				}()
			}
			p, err := conn.PrepareContext(ctx, "select id from keys_t where id=? order by id")
			require.NoError(t, err)
			defer p.Close()
			for _, v := range []any{"9007199254740993", target, nil, "9007199254740993E0", "9007199254740993" + strings.Repeat("0", 60) + "E-60", "0." + strings.Repeat("0", 60) + "9007199254740993E76", "9007199254740993"} {
				want := []int64{target}
				if v == nil {
					want = []int64{}
				}
				require.Equal(t, want, ids(p.QueryContext(ctx, v)))
			}
			for _, tc := range []struct {
				value string
				want  []int64
			}{
				{"9007199254740993.5", []int64{target + 1}},
				{"9223372036854775808", []int64{}},
				{"notnumber", []int64{}},
				{"9007199254740993tail", []int64{target - 1, target}},
				{"9007199254740993", []int64{target}},
			} {
				require.Equal(t, tc.want, ids(p.QueryContext(ctx, tc.value)), tc.value)
			}
			// Plain string literal comparison retains its existing approximate domain.
			require.Equal(t, []int64{target}, ids(conn.QueryContext(ctx, "select id from keys_t where id='9007199254740993' order by id")))
			require.Equal(t, []int64{target}, ids(conn.QueryContext(ctx, "select id from keys_t where id in (?) order by id", "9007199254740993")))
			require.Equal(t, []int64{target}, ids(conn.QueryContext(ctx, "select id from keys_t where ?=id order by id", "9007199254740993")))
			exec(t, "prepare s from 'select id from keys_t where id=? order by id'")
			defer exec(t, "deallocate prepare s")
			exec(t, "set @v='9007199254740993'")
			require.Equal(t, []int64{target}, ids(conn.QueryContext(ctx, "execute s using @v")))
			exec(t, "prepare u from 'update keys_t set n=n+1 where id=?'")
			defer exec(t, "deallocate prepare u")
			exec(t, "execute u using @v")
			require.Equal(t, "9007199254740993", scalar(t, "select group_concat(id order by id) from keys_t where n=1"))
			exec(t, "select mo_ctl('dn','flush','"+schema+".keys_t')")
			require.Equal(t, []int64{target}, ids(p.QueryContext(ctx, "9007199254740993")))
			d, err := conn.PrepareContext(ctx, "delete from keys_t where id=?")
			require.NoError(t, err)
			defer d.Close()
			_, err = d.ExecContext(ctx, "9007199254740993")
			require.NoError(t, err)
			require.Equal(t, []int64{target - 1, target + 1}, ids(conn.QueryContext(ctx, "select id from keys_t order by id")))
		})
		t.Run("last day DATE metadata and schema", func(t *testing.T) {
			check := func(rows *sql.Rows, err error) {
				t.Helper()
				require.NoError(t, err)
				defer rows.Close()
				cols, err := rows.ColumnTypes()
				require.NoError(t, err)
				for _, col := range cols {
					require.Equal(t, "DATE", col.DatabaseTypeName())
				}
				require.True(t, rows.Next())
				vals := make([]sql.NullString, len(cols))
				dest := make([]any, len(cols))
				for i := range vals {
					dest[i] = &vals[i]
				}
				require.NoError(t, rows.Scan(dest...))
				for _, v := range vals {
					require.True(t, v.Valid)
					require.Equal(t, "2024-02-29", v.String)
				}
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			}
			check(conn.QueryContext(ctx, "select last_day('2024-02-10'),last_day(cast('2024-02-10' as date)),last_day(cast('2024-02-10 13:00:00' as datetime))"))
			p, err := conn.PrepareContext(ctx, "select last_day(?)")
			require.NoError(t, err)
			defer p.Close()
			check(p.QueryContext(ctx, "2024-02-10"))
			exec(t, "prepare l from 'select last_day(?)'")
			defer exec(t, "deallocate prepare l")
			exec(t, "set @l='2024-02-10'")
			check(conn.QueryContext(ctx, "execute l using @l"))
			exec(t, "create table calendar as select last_day(v) as d from source")
			exec(t, "create view calendar_view as select last_day('2024-02-10') as d")
			for _, name := range []string{"calendar", "calendar_view"} {
				require.Equal(t, "date", scalar(t, "select data_type from information_schema.columns where table_schema='"+schema+"' and table_name='"+name+"' and column_name='d'"))
			}
			require.Equal(t, "3", scalar(t, "select count(*) from calendar where d is null"))
			exec(t, "create table valid_calendar as select last_day('2024-02-10') as d")
			require.Equal(t, "2024-02-29", scalar(t, "select d from valid_calendar"))
		})
		t.Run("EXPLAIN preserves underlying SELECT binding", func(t *testing.T) {
			exec(t, "create table explain_t(id int primary key)")
			exec(t, "insert into explain_t values(1),(2)")
			explainText := func(rows *sql.Rows, err error) string {
				t.Helper()
				require.NoError(t, err)
				defer rows.Close()
				var lines []string
				for rows.Next() {
					var line string
					require.NoError(t, rows.Scan(&line))
					lines = append(lines, line)
				}
				require.NoError(t, rows.Err())
				require.NotEmpty(t, lines)
				text := strings.Join(lines, "\n")
				require.NotContains(t, strings.ToLower(text), "cast(explain_t.id as bigint)")
				require.NotContains(t, text, "Cast expression may prevent index usage")
				return text
			}
			for _, wrapper := range []string{"explain ", "explain analyze ", "explain phyplan "} {
				func() {
					exec(t, "prepare prof from '"+wrapper+"select sum(id) from explain_t where id=?'")
					defer exec(t, "deallocate prepare prof")
					for _, v := range []any{int64(1), "1", int64(2)} {
						exec(t, "set @p=?", v)
						text := explainText(conn.QueryContext(ctx, "execute prof using @p"))
						if wrapper != "explain phyplan " {
							require.Contains(t, text, "Filter Cond:")
						}
					}
					p, err := conn.PrepareContext(ctx, wrapper+"select sum(id) from explain_t where id=?")
					require.NoError(t, err)
					defer p.Close()
					explainText(p.QueryContext(ctx, int64(1)))
				}()
			}

		})
	})
}
