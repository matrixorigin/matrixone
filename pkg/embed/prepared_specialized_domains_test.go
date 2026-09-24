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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPreparedSpecializedDomains(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(t *testing.T, s string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, s)
			require.NoError(t, err, s)
		}
		query := func(t *testing.T, s string) [][]string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, s)
			require.NoError(t, err, s)
			defer rows.Close()
			cols, err := rows.ColumnTypes()
			require.NoError(t, err)
			var out [][]string
			for rows.Next() {
				vals := make([]sql.NullString, len(cols))
				args := make([]any, len(cols))
				for i := range vals {
					args[i] = &vals[i]
				}
				require.NoError(t, rows.Scan(args...))
				row := make([]string, len(cols))
				for i, v := range vals {
					row[i] = "NULL"
					if v.Valid {
						row[i] = v.String
					}
				}
				out = append(out, row)
			}
			require.NoError(t, rows.Err())
			return out
		}
		exec(t, "create database review29349")
		exec(t, "use review29349")
		defer conn.ExecContext(context.Background(), "drop database review29349")
		t.Run("series_reuse", func(t *testing.T) {
			exec(t, "prepare gs_reuse from 'select count(*),min(result),max(result) from generate_series(?,?,?) g'")
			defer conn.ExecContext(ctx, "deallocate prepare gs_reuse")
			exec(t, "set @a=1,@b=9,@s=2")
			require.Equal(t, [][]string{{"5", "1", "9"}}, query(t, "execute gs_reuse using @a,@b,@s"))
			exec(t, "set @a='2020-01-01',@b='2020-01-03',@s='1 day'")
			got := query(t, "execute gs_reuse using @a,@b,@s")
			require.Equal(t, "3", got[0][0])
			exec(t, "set @a=1,@b=9,@s=2")
			require.Equal(t, [][]string{{"5", "1", "9"}}, query(t, "execute gs_reuse using @a,@b,@s"))
		})
		t.Run("series_sum", func(t *testing.T) {
			want := query(t, "select sum(result),avg(result) from generate_series(1,9,2) g")
			exec(t, "prepare gs_sum from 'select sum(result),avg(result) from generate_series(?,?,?) g'")
			defer conn.ExecContext(ctx, "deallocate prepare gs_sum")
			exec(t, "set @a=1,@b=9,@s=2")
			require.Equal(t, want, query(t, "execute gs_sum using @a,@b,@s"))
		})
		t.Run("series_ctas", func(t *testing.T) {
			exec(t, "create table gs_ctas_direct as select result from generate_series(1,9,2) g")
			exec(t, "prepare gs_ctas from 'create table gs_ctas_result as select result from generate_series(?,?,?) g'")
			defer conn.ExecContext(ctx, "deallocate prepare gs_ctas")
			exec(t, "set @a=1,@b=9,@s=2")
			exec(t, "execute gs_ctas using @a,@b,@s")
			require.Equal(t, [][]string{{"5"}}, query(t, "select count(*) from gs_ctas_result"))
			schema := func(table string) [][]string {
				return query(t, "select data_type from information_schema.columns where table_schema='review29349' and table_name='"+table+"' and column_name='result'")
			}
			require.Equal(t, schema("gs_ctas_direct"), schema("gs_ctas_result"))
		})
		t.Run("series_ctas_temporal", func(t *testing.T) {
			exec(t, "create table gs_date_direct as select result from generate_series('2020-01-01','2020-01-03','1 day') g")
			exec(t, "prepare gs_date_ctas from 'create table gs_date_prepared as select result from generate_series(?,?,?) g'")
			defer conn.ExecContext(ctx, "deallocate prepare gs_date_ctas")
			exec(t, "set @a='2020-01-01',@b='2020-01-03',@s='1 day'")
			exec(t, "execute gs_date_ctas using @a,@b,@s")
			columnType := func(table string) [][]string {
				return query(t, "select data_type from information_schema.columns where table_schema='review29349' and table_name='"+table+"' and column_name='result'")
			}
			require.Equal(t, columnType("gs_date_direct"), columnType("gs_date_prepared"))
			require.Equal(t, query(t, "select result from gs_date_direct order by result"),
				query(t, "select result from gs_date_prepared order by result"))
		})
		t.Run("series_ctas_explicit_column", func(t *testing.T) {
			exec(t, "prepare gs_explicit from 'create table gs_explicit_result (result varchar(30)) as select result from generate_series(?,?,?) g'")
			defer conn.ExecContext(ctx, "deallocate prepare gs_explicit")
			exec(t, "set @a=1,@b=9,@s=2")
			exec(t, "execute gs_explicit using @a,@b,@s")
			require.Equal(t, [][]string{{"varchar"}}, query(t,
				"select data_type from information_schema.columns where table_schema='review29349' and table_name='gs_explicit_result' and column_name='result'"))
			require.Equal(t, [][]string{{"5"}}, query(t, "select count(*) from gs_explicit_result"))
		})
		t.Run("series_ctas_binary_protocol", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx, "create table gs_binary_result as select result from generate_series(?,?,?) g")
			require.NoError(t, err)
			defer stmt.Close()
			_, err = stmt.ExecContext(ctx, int64(1), int64(9), int64(2))
			require.NoError(t, err)
			require.Equal(t, [][]string{{"5"}}, query(t, "select count(*) from gs_binary_result"))
		})
		t.Run("series_union_order", func(t *testing.T) {
			want := query(t, "select result from generate_series(1,11,2) g union all select 4 order by result")
			exec(t, "prepare gs_union from 'select result from generate_series(?,?,?) g union all select 4 order by result'")
			defer conn.ExecContext(ctx, "deallocate prepare gs_union")
			exec(t, "set @a=1,@b=11,@s=2")
			got := query(t, "execute gs_union using @a,@b,@s")
			if !reflect.DeepEqual(want, got) {
				t.Errorf("ordinary=%v prepared=%v", want, got)
			}
		})
		t.Run("series_union_temporal", func(t *testing.T) {
			want := query(t, "select result from generate_series('2020-01-01','2020-01-03','1 day') g union all select '2020-01-04 00:00:00' order by result")
			exec(t, "prepare gs_union_date from 'select result from generate_series(?,?,?) g union all select ''2020-01-04 00:00:00'' order by result'")
			defer conn.ExecContext(ctx, "deallocate prepare gs_union_date")
			exec(t, "set @a='2020-01-01',@b='2020-01-03',@s='1 day'")
			require.Equal(t, want, query(t, "execute gs_union_date using @a,@b,@s"))
		})
		t.Run("series_temporal_string_consumer", func(t *testing.T) {
			want := query(t, "select result from generate_series('2020-01-01','2020-01-03','1 day') g where result like '2020-01-0%' order by result")
			exec(t, "prepare gs_like from 'select result from generate_series(?,?,?) g where result like ''2020-01-0%'' order by result'")
			defer conn.ExecContext(ctx, "deallocate prepare gs_like")
			exec(t, "set @a='2020-01-01',@b='2020-01-03',@s='1 day'")
			require.Equal(t, want, query(t, "execute gs_like using @a,@b,@s"))
		})
		t.Run("series_fixed_start_temporal_scale", func(t *testing.T) {
			want := query(t, "select result from generate_series('2020-01-01','2020-01-03','1 day') g order by result")
			exec(t, "prepare gs_fixed_start from 'select result from generate_series(''2020-01-01'',?,''1 day'') g order by result'")
			defer conn.ExecContext(ctx, "deallocate prepare gs_fixed_start")
			exec(t, "set @end='2020-01-03'")
			require.Equal(t, want, query(t, "execute gs_fixed_start using @end"))
			exec(t, "prepare gs_fixed_ctas from 'create table gs_fixed_date as select result from generate_series(''2020-01-01'',?,''1 day'') g'")
			defer conn.ExecContext(ctx, "deallocate prepare gs_fixed_ctas")
			exec(t, "execute gs_fixed_ctas using @end")
			require.Equal(t, want, query(t, "select result from gs_fixed_date order by result"))
			exec(t, "prepare gs_fixed_step from 'select result from generate_series(''2020-01-01'',''2020-01-03'',?) g order by result'")
			defer conn.ExecContext(ctx, "deallocate prepare gs_fixed_step")
			exec(t, "set @step='1 day'")
			require.Equal(t, want, query(t, "execute gs_fixed_step using @step"))
		})
		t.Run("unnest_large_json", func(t *testing.T) {
			require.Equal(t, [][]string{{"1"}}, query(t, `select count(*) from unnest(cast(concat('["',repeat('x',70000),'"]') as json)) u`))
			exec(t, "prepare u_large from 'select count(*) from unnest(?) u'")
			defer conn.ExecContext(ctx, "deallocate prepare u_large")
			exec(t, `set @j=concat('["',repeat('x',70000),'"]')`)
			rows, err := conn.QueryContext(ctx, "execute u_large using @j")
			if err != nil {
				msg := err.Error()
				if len(msg) > 200 {
					msg = msg[:200]
				}
				t.Fatalf("large prepared JSON: %s", msg)
			}
			defer rows.Close()
			require.True(t, rows.Next())
			var n int
			require.NoError(t, rows.Scan(&n))
			require.Equal(t, 1, n)
		})
		t.Run("precision_binary_protocol_masks_recovery", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx, "select case when result=2 then ceil(123.456,?) else 0 end from generate_series(1,3) g order by result")
			require.NoError(t, err)
			defer stmt.Close()
			for _, v := range []any{2.5, "2.5tail", nil, 2.0} {
				rows, err := stmt.QueryContext(ctx, v)
				if v == nil {
					require.ErrorContains(t, err, "not const")
					continue
				}
				require.NoError(t, err)
				var got []string
				for rows.Next() {
					var s sql.NullString
					require.NoError(t, rows.Scan(&s))
					if s.Valid {
						got = append(got, s.String)
					} else {
						got = append(got, "NULL")
					}
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.Equal(t, []string{"0.000", "123.460", "0.000"}, got)
			}
		})
		t.Run("binary_state_protocol_recovery", func(t *testing.T) {
			var sketch []byte
			require.NoError(t, conn.QueryRowContext(ctx, "select hll_add_agg(result) from generate_series(1,3) g").Scan(&sketch))
			require.True(t, strings.ContainsRune(string(sketch), 0))
			stmt, err := conn.PrepareContext(ctx, "select hll_cardinality(?)")
			require.NoError(t, err)
			defer stmt.Close()
			for _, v := range []any{sketch, []byte{0}, nil, sketch} {
				var n sql.NullInt64
				err := stmt.QueryRowContext(ctx, v).Scan(&n)
				if b, ok := v.([]byte); ok && len(b) == 1 {
					require.Error(t, err)
					continue
				}
				require.NoError(t, err)
				if v == nil {
					require.False(t, n.Valid)
				} else {
					require.Equal(t, int64(3), n.Int64)
				}
			}
		})
		t.Run("enum_set_aggregate", func(t *testing.T) {
			exec(t, "create table special(e enum('20','3','z'),s set('x','y'))")
			exec(t, "insert into special values('20','x'),('3','y'),('z','x,y'),(null,null)")
			want := query(t, "select sum(e),avg(e),sum(s),avg(s) from special")
			exec(t, "prepare special_p from 'select sum(e),avg(e),sum(s),avg(s) from special'")
			defer conn.ExecContext(ctx, "deallocate prepare special_p")
			require.Equal(t, want, query(t, "execute special_p"))
		})
	})
}
