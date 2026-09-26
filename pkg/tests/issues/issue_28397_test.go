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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue28397FieldKeepsExactNumericComparison(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		_, err = conn.ExecContext(ctx, fmt.Sprintf("create database `%s`", dbName))
		require.NoError(t, err)
		defer func() {
			_, cleanupErr := conn.ExecContext(ctx, fmt.Sprintf("drop database if exists `%s`", dbName))
			require.NoError(t, cleanupErr)
		}()
		_, err = conn.ExecContext(ctx, fmt.Sprintf("use `%s`", dbName))
		require.NoError(t, err)

		cases := []struct {
			name string
			sql  string
			want int64
		}{
			{
				name: "integer boundary",
				sql:  "select field(cast(9007199254740993 as decimal(20,0)), cast(9007199254740992 as decimal(20,0)), cast(9007199254740993 as decimal(20,0)))",
				want: 2,
			},
			{
				name: "decimal128 boundary",
				sql:  "select field(cast('99999999999999999999999999999999999999' as decimal(38,0)), cast('99999999999999999999999999999999999998' as decimal(38,0)), cast('99999999999999999999999999999999999999' as decimal(38,0)))",
				want: 2,
			},
			{
				name: "scale boundary",
				sql:  "select field(cast(1.0000000000000001 as decimal(20,16)), cast(1.0000000000000000 as decimal(20,16)), cast(1.0000000000000001 as decimal(20,16)))",
				want: 2,
			},
			{
				name: "mixed integer",
				sql:  "select field(18446744073709551615, -1, 18446744073709551615)",
				want: 1,
			},
			{
				name: "equal different scales",
				sql:  "select field(cast(1.20 as decimal(4,2)), cast(1.2 as decimal(3,1)), cast(1.20 as decimal(4,2)))",
				want: 1,
			},
			{
				name: "null mixed integer candidate",
				sql:  "select field(cast(1 as unsigned), null, cast(1 as signed))",
				want: 2,
			},
			{
				name: "null decimal search",
				sql:  "select field(null, cast(1.2 as decimal(4,2)), cast(1.20 as decimal(4,2)))",
				want: 0,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var got int64
				require.NoError(t, conn.QueryRowContext(ctx, tc.sql).Scan(&got))
				require.Equal(t, tc.want, got)
			})
		}

		_, err = conn.ExecContext(ctx, "create table field_decimal(search decimal(38,0), candidate1 decimal(38,0), candidate2 decimal(38,0))")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into field_decimal values (99999999999999999999999999999999999999, 99999999999999999999999999999999999998, 99999999999999999999999999999999999999), (9007199254740993,9007199254740992,9007199254740993), (123,122,123)")
		require.NoError(t, err)
		t.Run("issue 29378 prepared bool aggregate keeps numeric adapter", func(t *testing.T) {
			_, err := conn.ExecContext(ctx,
				"prepare projected_bool_sum from 'select sum(char_length(?)), sum(? like ''____'') from field_decimal'")
			require.NoError(t, err)
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare projected_bool_sum") }()
			_, err = conn.ExecContext(ctx, "prepare projected_bool_avg from 'select avg(? like ''____'') from field_decimal'")
			require.NoError(t, err)
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare projected_bool_avg") }()
			for _, tc := range []struct {
				source string
				want   []sql.NullInt64
			}{
				{"X'61626364'", []sql.NullInt64{{Int64: 12, Valid: true}, {Int64: 3, Valid: true}}},
				{"if(false, X'ff', '你好')", []sql.NullInt64{{Int64: 6, Valid: true}, {Int64: 0, Valid: true}}},
				{"null", []sql.NullInt64{{}, {}}},
				{"X'61626364'", []sql.NullInt64{{Int64: 12, Valid: true}, {Int64: 3, Valid: true}}},
			} {
				_, err = conn.ExecContext(ctx, "set @aggregate_source="+tc.source)
				require.NoError(t, err)
				read := func(query string) []sql.NullInt64 {
					var length, matches sql.NullInt64
					require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&length, &matches), query)
					return []sql.NullInt64{length, matches}
				}
				direct := read("select sum(char_length(@aggregate_source)), sum(@aggregate_source like '____') from field_decimal")
				prepared := read("execute projected_bool_sum using @aggregate_source,@aggregate_source")
				require.Equal(t, tc.want, direct, tc.source)
				require.Equal(t, direct, prepared, tc.source)
				var directAvg, preparedAvg sql.NullString
				require.NoError(t, conn.QueryRowContext(ctx,
					"select avg(@aggregate_source like '____') from field_decimal").Scan(&directAvg))
				require.NoError(t, conn.QueryRowContext(ctx,
					"execute projected_bool_avg using @aggregate_source").Scan(&preparedAvg))
				require.Equal(t, directAvg, preparedAvg, tc.source)
			}
		})
		t.Run("issue 29378 projected BETWEEN domain", func(t *testing.T) {
			_, err := conn.ExecContext(ctx,
				"prepare projected_between from 'select x between ''1'' and ''2'', x not between ''1'' and ''2'' from (select ? as x limit 1) d'")
			require.NoError(t, err)
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare projected_between") }()
			for _, tc := range []struct {
				source string
				want   []int64
			}{
				{"cast(10 as decimal(20,0))", []int64{0, 1}},
				{"'10'", []int64{1, 0}},
				{"cast(10 as decimal(20,0))", []int64{0, 1}},
			} {
				_, err = conn.ExecContext(ctx, "set @between_source="+tc.source)
				require.NoError(t, err)
				read := func(query string) []int64 {
					var between, notBetween int64
					require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&between, &notBetween), query)
					return []int64{between, notBetween}
				}
				direct := read("select x between '1' and '2', x not between '1' and '2' from (select @between_source as x limit 1) d")
				prepared := read("execute projected_between using @between_source")
				require.Equal(t, tc.want, direct, tc.source)
				require.Equal(t, direct, prepared, tc.source)
			}
			_, err = conn.ExecContext(ctx,
				"prepare projected_between_bounds from 'select x between lo and hi, x not between lo and hi from (select ? as x, ? as lo, ? as hi limit 1) d'")
			require.NoError(t, err)
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare projected_between_bounds") }()
			for _, tc := range []struct {
				value, low, high string
				want             []sql.NullInt64
			}{
				{"cast(2 as decimal(20,0))", "cast(1 as decimal(20,0))", "cast(10 as decimal(20,0))", []sql.NullInt64{{Int64: 1, Valid: true}, {Int64: 0, Valid: true}}},
				{"'2'", "'1'", "'10'", []sql.NullInt64{{Int64: 0, Valid: true}, {Int64: 1, Valid: true}}},
				{"cast(2 as decimal(20,0))", "cast(1 as decimal(20,0))", "null", []sql.NullInt64{{}, {}}},
				{"cast(2 as decimal(20,0))", "cast(1 as decimal(20,0))", "cast(10 as decimal(20,0))", []sql.NullInt64{{Int64: 1, Valid: true}, {Int64: 0, Valid: true}}},
			} {
				_, err = conn.ExecContext(ctx, "set @between_value="+tc.value+", @between_low="+tc.low+", @between_high="+tc.high)
				require.NoError(t, err)
				read := func(query string) []sql.NullInt64 {
					var between, notBetween sql.NullInt64
					require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&between, &notBetween), query)
					return []sql.NullInt64{between, notBetween}
				}
				direct := read("select x between lo and hi, x not between lo and hi from (select @between_value as x, @between_low as lo, @between_high as hi limit 1) d")
				prepared := read("execute projected_between_bounds using @between_value,@between_low,@between_high")
				require.Equal(t, tc.want, direct, tc)
				require.Equal(t, direct, prepared, tc)
			}
		})
		rows, err := conn.QueryContext(ctx, "select field(search,candidate1,candidate2) from field_decimal order by search")
		require.NoError(t, err)
		defer rows.Close()
		var got []int64
		for rows.Next() {
			var value int64
			require.NoError(t, rows.Scan(&value))
			got = append(got, value)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int64{2, 2, 2}, got)

		t.Run("issue 29378 prepared fixed decimal peers", func(t *testing.T) {
			const first = "cast(9007199254740992 as decimal(20,0))"
			const second = "cast(9007199254740993 as decimal(20,0))"
			_, err := conn.ExecContext(ctx, "prepare field_fixed from 'select field(?, "+first+", "+second+")'")
			require.NoError(t, err)
			defer func() {
				_, _ = conn.ExecContext(context.Background(), "deallocate prepare field_fixed")
			}()
			for _, tc := range []struct {
				name, source string
				want         int64
			}{
				{"distinct exact decimal", second, 2},
				{"matching exact decimal", first, 1},
				{"string comparison", "'9007199254740993'", 1},
				{"explicit real comparison", "cast(9007199254740993 as double)", 1},
				{"null search", "null", 0},
				{"exact decimal after reuse", second, 2},
			} {
				t.Run(tc.name, func(t *testing.T) {
					_, err := conn.ExecContext(ctx, "set @field_search = "+tc.source)
					require.NoError(t, err)
					var direct, prepared int64
					require.NoError(t, conn.QueryRowContext(ctx,
						"select field("+tc.source+", "+first+", "+second+")").Scan(&direct))
					require.NoError(t, conn.QueryRowContext(ctx,
						"execute field_fixed using @field_search").Scan(&prepared))
					require.Equal(t, tc.want, direct)
					require.Equal(t, direct, prepared)
				})
			}

			_, err = conn.ExecContext(ctx, "prepare field_candidate from 'select field("+second+", ?)'")
			require.NoError(t, err)
			defer func() {
				_, _ = conn.ExecContext(context.Background(), "deallocate prepare field_candidate")
			}()
			for _, tc := range []struct {
				source string
				want   int64
			}{{first, 0}, {second, 1}} {
				_, err = conn.ExecContext(ctx, "set @field_candidate = "+tc.source)
				require.NoError(t, err)
				var prepared int64
				require.NoError(t, conn.QueryRowContext(ctx,
					"execute field_candidate using @field_candidate").Scan(&prepared))
				require.Equal(t, tc.want, prepared)
			}

			for _, tc := range []struct {
				name, source, peers string
				want                int64
			}{
				{"integer literal peer", second, "9007199254740992", 0},
				{"scaled decimal peer", "cast(1.0000000000000001 as decimal(20,16))",
					"cast(1.0000000000000000 as decimal(20,16))", 0},
				{"mixed string peer", second, first + `, "x"`, 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					_, err := conn.ExecContext(ctx, "prepare field_boundary from 'select field(?, "+tc.peers+")'")
					require.NoError(t, err)
					defer func() {
						_, _ = conn.ExecContext(context.Background(), "deallocate prepare field_boundary")
					}()
					_, err = conn.ExecContext(ctx, "set @field_search = "+tc.source)
					require.NoError(t, err)
					var direct, prepared int64
					require.NoError(t, conn.QueryRowContext(ctx,
						"select field("+tc.source+", "+tc.peers+")").Scan(&direct))
					require.NoError(t, conn.QueryRowContext(ctx,
						"execute field_boundary using @field_search").Scan(&prepared))
					require.Equal(t, tc.want, direct)
					require.Equal(t, direct, prepared)
				})
			}

			_, err = conn.ExecContext(ctx, "prepare field_columns from 'select field(?, candidate1, candidate2) from field_decimal where search = 9007199254740993'")
			require.NoError(t, err)
			defer func() {
				_, _ = conn.ExecContext(context.Background(), "deallocate prepare field_columns")
			}()
			_, err = conn.ExecContext(ctx, "set @field_search = "+second)
			require.NoError(t, err)
			var fromColumns int64
			require.NoError(t, conn.QueryRowContext(ctx,
				"execute field_columns using @field_search").Scan(&fromColumns))
			require.Equal(t, int64(2), fromColumns)
		})

		t.Run("issue 29378 nested and binary prepared peers", func(t *testing.T) {
			const first = "cast(9007199254740992 as decimal(20,0))"
			const second = "cast(9007199254740993 as decimal(20,0))"
			type fieldSource struct {
				source string
				want   int64
			}
			for _, tc := range []struct {
				name, expr string
				values     []fieldSource
			}{
				{
					name: "nested abs and source reuse", expr: "field(abs(?), " + first + ")",
					values: []fieldSource{{second, 0}, {"cast(9007199254740993 as double)", 1}, {second, 0}},
				},
				{
					name: "nested coalesce", expr: "field(coalesce(?, cast(0 as decimal(20,0))), " + first + ")",
					values: []fieldSource{{second, 0}},
				},
				{
					name: "nested if", expr: "field(if(true, ?, cast(0 as decimal(20,0))), " + first + ")",
					values: []fieldSource{{second, 0}},
				},
				{
					name: "nested candidate", expr: "field(" + second + ", abs(?))",
					values: []fieldSource{{first, 0}},
				},
				{
					name: "folded fixed abs candidate", expr: "field(?, abs(" + first + "))",
					values: []fieldSource{{second, 0}, {"cast(9007199254740993 as double)", 1}, {second, 0}},
				},
				{
					name:   "folded fixed coalesce candidate",
					expr:   "field(?, coalesce(" + first + ", cast(0 as decimal(20,0))))",
					values: []fieldSource{{second, 0}},
				},
				{
					name:   "folded fixed coalesce null branch",
					expr:   "field(?, coalesce(cast(null as decimal(20,0)), " + first + "))",
					values: []fieldSource{{second, 0}},
				},
				{
					name: "folded fixed abs needle", expr: "field(abs(" + second + "), ?)",
					values: []fieldSource{{first, 0}},
				},
				{
					name:   "folded fixed fractional candidate",
					expr:   "field(?, abs(cast(1.0000000000000000 as decimal(20,16))))",
					values: []fieldSource{{"cast(1.0000000000000001 as decimal(20,16))", 0}},
				},
				{
					name:   "greatest nested exact peer",
					expr:   "field(greatest(?, cast(0 as decimal(20,0))), " + first + ")",
					values: []fieldSource{{second, 0}},
				},
				{
					name:   "least nested exact peer",
					expr:   "field(least(?, cast(9007199254740994 as decimal(20,0))), " + first + ")",
					values: []fieldSource{{second, 0}},
				},
				{
					name: "explicit real marker", expr: "field(abs(cast(? as double)), " + first + ")",
					values: []fieldSource{{second, 1}},
				},
				{
					name: "explicit real peer", expr: "field(abs(?), cast(9007199254740992 as double))",
					values: []fieldSource{{second, 1}},
				},
				{
					name: "fixed real expression boundary", expr: "field(?, abs(cast(9007199254740992 as double)))",
					values: []fieldSource{{second, 1}},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					_, err := conn.ExecContext(ctx, "prepare field_nested from 'select "+tc.expr+"'")
					require.NoError(t, err)
					defer func() {
						cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						_, cleanupErr := conn.ExecContext(cleanupCtx, "deallocate prepare field_nested")
						require.NoError(t, cleanupErr)
					}()
					for _, value := range tc.values {
						_, err := conn.ExecContext(ctx, "set @field_nested_value = "+value.source)
						require.NoError(t, err)
						var direct, prepared int64
						require.NoError(t, conn.QueryRowContext(ctx,
							"select "+strings.Replace(tc.expr, "?", value.source, 1)).Scan(&direct))
						require.NoError(t, conn.QueryRowContext(ctx,
							"execute field_nested using @field_nested_value").Scan(&prepared))
						require.Equal(t, value.want, direct)
						require.Equal(t, direct, prepared)
					}
				})
			}

			stmt, err := conn.PrepareContext(ctx, "select field(?, "+first+")")
			require.NoError(t, err)
			defer func() { require.NoError(t, stmt.Close()) }()
			for _, value := range []struct {
				name  string
				param any
				want  int64
			}{
				{"distinct unsigned", uint64(9007199254740993), 0},
				{"matching unsigned", uint64(9007199254740992), 1},
				{"numeric text keeps approximate domain", "9007199254740993", 1},
				{"null search", nil, 0},
				{"unsigned after reuse", uint64(9007199254740993), 0},
			} {
				t.Run(value.name, func(t *testing.T) {
					var got int64
					require.NoError(t, stmt.QueryRowContext(ctx, value.param).Scan(&got))
					require.Equal(t, value.want, got)
				})
			}

			nested, err := conn.PrepareContext(ctx, "select field(abs(?), "+first+")")
			require.NoError(t, err)
			defer func() { require.NoError(t, nested.Close()) }()
			for _, value := range []struct {
				param uint64
				want  int64
			}{{9007199254740993, 0}, {9007199254740992, 1}, {9007199254740993, 0}} {
				var got int64
				require.NoError(t, nested.QueryRowContext(ctx, value.param).Scan(&got))
				require.Equal(t, value.want, got, "param=%d", value.param)
			}

			// COM_STMT sends nil without the TEXT source type that SQL user
			// variables carry. The null candidate and null result must not
			// turn the fixed DECIMAL peer into a rounded DOUBLE.
			for _, tc := range []struct {
				name, expr string
				args       []any
				want       int64
			}{
				{"coalesce", "field(coalesce(?, abs(?)), abs(" + first + "))", []any{nil, uint64(9007199254740993)}, 0},
				{"ifnull", "field(ifnull(?, abs(?)), abs(" + first + "))", []any{nil, uint64(9007199254740993)}, 0},
				{"reverse coalesce", "field(coalesce(abs(?), ?), abs(" + first + "))", []any{nil, uint64(9007199254740993)}, 0},
				{"extra null candidate", "field(?, ?, abs(" + first + "), abs(" + second + "))", []any{uint64(9007199254740993), nil}, 3},
				{"unflattened numeric", "field(x, abs(" + first + ")) from (select ? as x limit 1) d", []any{uint64(9007199254740993)}, 0},
				{"unflattened null candidate", "field(" + second + ", x, abs(" + first + "), abs(" + second + ")) from (select ? as x limit 1) d", []any{nil}, 3},
				{"unflattened null common result", "field(coalesce(x, abs(" + second + ")), abs(" + first + ")) from (select ? as x limit 1) d", []any{nil}, 0},
				{"nested projected null common result", "field(coalesce(x, abs(" + second + ")), abs(" + first + ")) from (select coalesce(?, " + second + ") as x) d", []any{nil}, 0},
				{"unflattened null", "field(x, abs(" + first + ")) from (select ? as x limit 1) d", []any{nil}, 0},
				{"literal null control", "field(?, null, abs(" + first + "), abs(" + second + "))", []any{uint64(9007199254740993)}, 3},
			} {
				t.Run(tc.name, func(t *testing.T) {
					stmt, err := conn.PrepareContext(ctx, "select "+tc.expr)
					require.NoError(t, err)
					defer func() { require.NoError(t, stmt.Close()) }()
					var got int64
					require.NoError(t, stmt.QueryRowContext(ctx, tc.args...).Scan(&got))
					require.Equal(t, tc.want, got)
				})
			}
			t.Run("grouped domainless null common value", func(t *testing.T) {
				const source = "field(coalesce(x,y), abs(" + first + ")) from "
				var direct int64
				require.NoError(t, conn.QueryRowContext(ctx,
					"select "+source+"(select null as x, cast(9007199254740993 as unsigned) as y from field_decimal group by x,y) d").Scan(&direct))
				stmt, err := conn.PrepareContext(ctx,
					"select "+source+"(select ? as x, ? as y from field_decimal group by x,y) d")
				require.NoError(t, err)
				defer func() { require.NoError(t, stmt.Close()) }()
				var prepared int64
				require.NoError(t, stmt.QueryRowContext(ctx, nil, uint64(9007199254740993)).Scan(&prepared))
				require.Equal(t, direct, prepared)
			})
			t.Run("mixed set keeps every physical row", func(t *testing.T) {
				stmt, err := conn.PrepareContext(ctx,
					"select x, field(x, cast(0 as decimal(20,0))) from (select ? as x union all select ?) d order by 2")
				require.NoError(t, err)
				defer func() { require.NoError(t, stmt.Close()) }()
				for execution, args := range [][]any{
					{nil, "abc"}, {"abc", nil}, {nil, nil},
					{uint64(9007199254740993), uint64(9007199254740992)}, {nil, "abc"},
				} {
					t.Run(fmt.Sprintf("execution %d", execution), func(t *testing.T) {
						rows, err := stmt.QueryContext(ctx, args...)
						require.NoError(t, err)
						defer func() { require.NoError(t, rows.Close()) }()
						var got []struct {
							source sql.NullString
							field  int64
						}
						for rows.Next() {
							var row struct {
								source sql.NullString
								field  int64
							}
							require.NoError(t, rows.Scan(&row.source, &row.field))
							got = append(got, row)
						}
						require.NoError(t, rows.Err())
						require.Len(t, got, 2)
						if _, numeric := args[0].(uint64); numeric {
							require.Equal(t, []int64{0, 0}, []int64{got[0].field, got[1].field})
							require.ElementsMatch(t, []sql.NullString{
								{String: "9007199254740993", Valid: true},
								{String: "9007199254740992", Valid: true},
							}, []sql.NullString{got[0].source, got[1].source})
						} else if args[0] == nil && args[1] == nil {
							require.Equal(t, []int64{0, 0}, []int64{got[0].field, got[1].field})
							require.False(t, got[0].source.Valid)
							require.False(t, got[1].source.Valid)
						} else {
							require.False(t, got[0].source.Valid)
							require.Equal(t, int64(0), got[0].field)
							require.Equal(t, sql.NullString{String: "abc", Valid: true}, got[1].source)
							require.Equal(t, int64(1), got[1].field)
						}
					})
				}
			})
			for _, tc := range []struct {
				name, branch string
				want         []int64
			}{
				{"domainless null", "?", []int64{0, 0}},
				{"typed text null", "cast(? as varchar)", []int64{1, 1}},
				{"typed decimal null", "cast(? as decimal(20,0))", []int64{0, 0}},
				{"typed double null", "cast(? as double)", []int64{1, 1}},
			} {
				t.Run("conditional "+tc.name, func(t *testing.T) {
					stmt, err := conn.PrepareContext(ctx,
						"select field(coalesce(x, abs("+second+")), abs("+first+")) "+
							"from (select if(id=1, "+tc.branch+", null) as x from "+
							"(select 1 as id union all select 2) r) d order by 1")
					require.NoError(t, err)
					defer func() { require.NoError(t, stmt.Close()) }()
					rows, err := stmt.QueryContext(ctx, nil)
					require.NoError(t, err)
					defer rows.Close()
					var got []int64
					for rows.Next() {
						var value int64
						require.NoError(t, rows.Scan(&value))
						got = append(got, value)
					}
					require.NoError(t, rows.Err())
					require.Equal(t, tc.want, got)
				})
			}
			for _, tc := range []struct {
				name, expr, wantType, wantValue string
			}{
				{"untyped null", "coalesce(abs(?), ?)", "DECIMAL", "9007199254740993"},
				{"explicit double null", "coalesce(abs(cast(? as double)), ?)", "DOUBLE", ""},
			} {
				t.Run(tc.name+" result metadata", func(t *testing.T) {
					stmt, err := conn.PrepareContext(ctx, "select "+tc.expr)
					require.NoError(t, err)
					defer func() { require.NoError(t, stmt.Close()) }()
					rows, err := stmt.QueryContext(ctx, nil, uint64(9007199254740993))
					require.NoError(t, err)
					defer rows.Close()
					columns, err := rows.ColumnTypes()
					require.NoError(t, err)
					require.Equal(t, tc.wantType, columns[0].DatabaseTypeName())
					require.True(t, rows.Next())
					var value string
					require.NoError(t, rows.Scan(&value))
					if tc.wantValue != "" {
						require.Equal(t, tc.wantValue, value)
					} else {
						require.NotEmpty(t, value)
					}
					require.NoError(t, rows.Err())
				})
			}
		})

		t.Run("issue 29378 result metadata and failed execution reuse", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "set @md = cast(null as decimal(20,0)), @ms = 'abc', @mn = cast(9007199254740993 as decimal(20,0))")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "prepare field_metadata from 'select coalesce(?, ?, abs(?)), x, field(x, abs(cast(9007199254740992 as decimal(20,0)))) from (select ? as x limit 1) d'")
			require.NoError(t, err)
			defer func() { _, err := conn.ExecContext(ctx, "deallocate prepare field_metadata"); require.NoError(t, err) }()
			rows, err := conn.QueryContext(ctx, "execute field_metadata using @md, @ms, @mn, @mn")
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Equal(t, []string{"VARCHAR", "TEXT", "UNSIGNED BIGINT"}, []string{columns[0].DatabaseTypeName(), columns[1].DatabaseTypeName(), columns[2].DatabaseTypeName()})
			require.True(t, rows.Next())
			var selected, visible string
			var field int64
			require.NoError(t, rows.Scan(&selected, &visible, &field))
			require.Equal(t, "abc", selected)
			require.Equal(t, "9007199254740993", visible)
			require.Zero(t, field)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())

			stmt, err := conn.PrepareContext(ctx, "select field(cast(? as decimal(20,0)), abs(cast(9007199254740992 as decimal(20,0))))")
			require.NoError(t, err)
			defer func() { require.NoError(t, stmt.Close()) }()
			require.Error(t, stmt.QueryRowContext(ctx, "invalid").Scan(&field))
			require.NoError(t, stmt.QueryRowContext(ctx, uint64(9007199254740993)).Scan(&field))
			require.Zero(t, field)
		})

		t.Run("issue 29378 projected common result metadata and reuse", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "prepare common_projected from 'select x, greatest(x,y), least(x,y) from (select ? as x, ? as y limit 1) d'")
			require.NoError(t, err)
			defer func() {
				_, err := conn.ExecContext(ctx, "deallocate prepare common_projected")
				require.NoError(t, err)
			}()
			for _, run := range []struct {
				name, left, right, visible, greatest, least string
			}{
				{"decimal", "cast(2 as decimal(20,0))", "cast(10 as decimal(20,0))", "2", "10", "2"},
				{"varchar", "'2'", "'10'", "2", "2", "10"},
				{"decimal again", "cast(2 as decimal(20,0))", "cast(10 as decimal(20,0))", "2", "10", "2"},
			} {
				t.Run(run.name, func(t *testing.T) {
					_, err := conn.ExecContext(ctx, "set @common_left="+run.left+", @common_right="+run.right)
					require.NoError(t, err)
					read := func(query string) ([]string, []string) {
						rows, err := conn.QueryContext(ctx, query)
						require.NoError(t, err)
						defer func() { require.NoError(t, rows.Close()) }()
						columns, err := rows.ColumnTypes()
						require.NoError(t, err)
						require.Len(t, columns, 3)
						types := make([]string, len(columns))
						for i, column := range columns {
							types[i] = column.DatabaseTypeName()
							if precision, scale, ok := column.DecimalSize(); ok {
								types[i] = fmt.Sprintf("%s(%d,%d)", types[i], precision, scale)
							}
						}
						require.True(t, rows.Next())
						var visible, high, low string
						require.NoError(t, rows.Scan(&visible, &high, &low))
						require.False(t, rows.Next())
						require.NoError(t, rows.Err())
						return []string{visible, high, low}, types
					}
					directValues, directTypes := read("select x, greatest(x,y), least(x,y) from (select @common_left as x, @common_right as y limit 1) d")
					preparedValues, preparedTypes := read("execute common_projected using @common_left, @common_right")
					require.Equal(t, []string{run.visible, run.greatest, run.least}, directValues, "direct oracle")
					require.Equal(t, directValues, preparedValues)
					require.Equal(t, directTypes[1:], preparedTypes[1:], "computed result types")
					require.Equal(t, "TEXT", preparedTypes[0], "bare projected marker stays visible TEXT")
				})
			}
		})

		t.Run("issue 29378 projected condition follows direct SQL", func(t *testing.T) {
			const projection = "greatest(x,y), if(x>y,x,y), case when x>y then x else y end from (select ? as x, ? as y limit 1) d"
			_, err := conn.ExecContext(ctx, "prepare projected_selection from 'select "+projection+"'")
			require.NoError(t, err)
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare projected_selection") }()
			for _, tc := range []struct {
				left, right, want string
			}{
				{"cast(2 as decimal(20,0))", "cast(10 as decimal(20,0))", "10"},
				{"'2'", "'10'", "2"},
				{"cast(2 as decimal(20,0))", "cast(10 as decimal(20,0))", "10"},
				{"cast(2 as decimal(20,0))", "cast(10 as char)", ""},
				{"cast(2 as double)", "cast(10 as decimal(20,0))", ""},
			} {
				_, err = conn.ExecContext(ctx, "set @select_left="+tc.left+", @select_right="+tc.right)
				require.NoError(t, err)
				read := func(query string) []string {
					var greatest, conditional, searchedCase string
					require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&greatest, &conditional, &searchedCase))
					return []string{greatest, conditional, searchedCase}
				}
				direct := read("select greatest(x,y), if(x>y,x,y), case when x>y then x else y end from (select @select_left as x, @select_right as y limit 1) d")
				prepared := read("execute projected_selection using @select_left,@select_right")
				if tc.want != "" {
					require.Equal(t, tc.want, direct[0], "direct common-value oracle")
				}
				require.Equal(t, direct, prepared)
			}
		})

		t.Run("issue 29378 projected null-safe comparison", func(t *testing.T) {
			_, err := conn.ExecContext(ctx,
				"prepare projected_null_compare from 'select x<=>y, ifnull(x>y,0) from (select ? as x, ? as y limit 1) d'")
			require.NoError(t, err)
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare projected_null_compare") }()
			for _, tc := range []struct {
				left, right string
				want        []int64
			}{
				{"null", "null", []int64{1, 0}},
				{"null", "cast(10 as decimal(20,0))", []int64{0, 0}},
				{"cast(2 as decimal(20,0))", "cast(10 as decimal(20,0))", []int64{0, 0}},
				{"'2'", "'10'", []int64{0, 1}},
			} {
				_, err = conn.ExecContext(ctx, "set @cmp_left="+tc.left+", @cmp_right="+tc.right)
				require.NoError(t, err)
				read := func(query string) []int64 {
					var equal, greater int64
					require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&equal, &greater))
					return []int64{equal, greater}
				}
				direct := read("select x<=>y, ifnull(x>y,0) from (select @cmp_left as x, @cmp_right as y limit 1) d")
				prepared := read("execute projected_null_compare using @cmp_left,@cmp_right")
				require.Equal(t, tc.want, direct)
				require.Equal(t, direct, prepared)
			}
		})

		t.Run("issue 29378 projected IN domain", func(t *testing.T) {
			_, err := conn.ExecContext(ctx,
				"prepare projected_in from 'select x in (''1'',''2''), x not in (''1'',''2'') from (select ? as x limit 1) d'")
			require.NoError(t, err)
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare projected_in") }()
			for _, tc := range []struct {
				source string
				want   []int64
			}{
				{"cast(1 as decimal(10,1))", []int64{1, 0}},
				{"'1.0'", []int64{0, 1}},
				{"cast(1 as decimal(10,1))", []int64{1, 0}},
			} {
				_, err = conn.ExecContext(ctx, "set @in_source="+tc.source)
				require.NoError(t, err)
				read := func(query string) []int64 {
					var in, notIn int64
					require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&in, &notIn))
					return []int64{in, notIn}
				}
				direct := read("select x in ('1','2'), x not in ('1','2') from (select @in_source as x limit 1) d")
				prepared := read("execute projected_in using @in_source")
				require.Equal(t, tc.want, direct)
				require.Equal(t, direct, prepared)
			}
			_, err = conn.ExecContext(ctx,
				"prepare projected_in_null from 'select x in (''1'',null), x not in (''1'',null) from (select ? as x limit 1) d'")
			require.NoError(t, err)
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare projected_in_null") }()
			_, err = conn.ExecContext(ctx, "set @in_source=cast(3 as decimal(10,1))")
			require.NoError(t, err)
			var directIn, directNotIn, preparedIn, preparedNotIn sql.NullInt64
			require.NoError(t, conn.QueryRowContext(ctx,
				"select x in ('1',null), x not in ('1',null) from (select @in_source as x limit 1) d").Scan(&directIn, &directNotIn))
			require.NoError(t, conn.QueryRowContext(ctx,
				"execute projected_in_null using @in_source").Scan(&preparedIn, &preparedNotIn))
			require.Equal(t, sql.NullInt64{}, directIn)
			require.Equal(t, sql.NullInt64{}, directNotIn)
			require.Equal(t, []sql.NullInt64{directIn, directNotIn}, []sql.NullInt64{preparedIn, preparedNotIn})
		})

		t.Run("issue 29378 explicit string and nested null boundaries", func(t *testing.T) {
			const first = "cast(9007199254740992 as decimal(20,0))"
			const second = "cast(9007199254740993 as decimal(20,0))"
			type fieldBoundaryRun struct {
				name   string
				values []string
				want   []int64
			}
			type fieldBoundaryCase struct {
				name, expr string
				runs       []fieldBoundaryRun
			}
			cases := []fieldBoundaryCase{
				{
					name: "explicit char peer", expr: "field(?, cast(9007199254740993 as char))",
					runs: []fieldBoundaryRun{
						{"distinct decimal", []string{first}, []int64{1}},
						{"matching decimal", []string{second}, []int64{1}},
						{"decimal after reuse", []string{first}, []int64{1}},
					},
				},
				{
					name: "folded explicit char peer", expr: "field(?, cast(abs(" + second + ") as char))",
					runs: []fieldBoundaryRun{{"distinct decimal", []string{first}, []int64{1}}},
				},
				{
					name: "null then nested abs", expr: "field(coalesce(?, abs(?)), abs(" + first + "))",
					runs: []fieldBoundaryRun{
						{"text null and distinct decimal", []string{"null", second}, []int64{1}},
						{"null and matching decimal", []string{"null", first}, []int64{1}},
						{"char null source", []string{"cast(null as char)", second}, []int64{1}},
						{"double null source", []string{"cast(null as double)", second}, []int64{1}},
						{"string boundary", []string{"'9007199254740993'", second}, []int64{1}},
						{"null after string", []string{"null", second}, []int64{1}},
						{"real boundary", []string{"null", "cast(9007199254740993 as double)"}, []int64{1}},
						{"null after real", []string{"null", second}, []int64{1}},
					},
				},
				{
					name: "null in numeric result", expr: "field(coalesce(abs(?), ?), abs(" + first + "))",
					runs: []fieldBoundaryRun{
						{"text null", []string{"null", second}, []int64{0}},
						{"double null", []string{"cast(null as double)", second}, []int64{1}},
						{"decimal null", []string{"cast(null as decimal(20,0))", second}, []int64{0}},
					},
				},
				{
					name: "projected marker", expr: "field(x, abs(" + first + ")) from (select ? as x) d",
					runs: []fieldBoundaryRun{{"exact decimal", []string{second}, []int64{0}}},
				},
				{
					name: "nested projected marker",
					expr: "field(y, abs(" + first + ")) from (select x as y from (select ? as x) d1) d2",
					runs: []fieldBoundaryRun{{"exact decimal", []string{second}, []int64{0}}},
				},
				{
					name: "projected common operands",
					expr: "field(greatest(x,y), abs(" + first + ")) from (select ? as x, ? as y limit 1) d",
					runs: []fieldBoundaryRun{
						{"two decimals", []string{second, first}, []int64{0}},
						{"two strings", []string{"'9007199254740993'", "'9007199254740992'"}, []int64{1}},
						{"decimals after strings", []string{second, first}, []int64{0}},
					},
				},
				{
					name: "projected field operands",
					expr: "field(x,y) from (select ? as x, ? as y limit 1) d",
					runs: []fieldBoundaryRun{
						{"numeric scales", []string{"cast(1 as decimal(10,1))", "cast(1 as decimal(10,2))"}, []int64{1}},
						{"distinct strings", []string{"'1.0'", "'1.00'"}, []int64{0}},
					},
				},
				{
					name: "grouped common operands",
					expr: "field(coalesce(x,y), abs(" + first + ")) from (select ? as x, ? as y from field_decimal group by x,y) d",
					runs: []fieldBoundaryRun{
						{"decimal null", []string{"cast(null as decimal(20,0))", second}, []int64{0}},
						{"char null", []string{"cast(null as char)", second}, []int64{1}},
					},
				},
				{
					name: "unrelated projected marker",
					expr: "field(y, abs(" + first + ")) from (select ? as x, cast(" + second + " as double) as y) d where x is not null",
					runs: []fieldBoundaryRun{{"explicit real peer stays real", []string{second}, []int64{1}}},
				},
				{
					name: "both window ordinals", expr: "field(x, abs(" + first + "))+field(y, 0) from (select max(?) over () as x, max(0) over () as y) d",
					runs: []fieldBoundaryRun{{"exact source", []string{second}, []int64{1}}},
				},
				{
					name: "second window ordinal", expr: "field(x, abs(" + first + "))+field(y, 0) from (select max(0) over () as y, max(?) over () as x) d",
					runs: []fieldBoundaryRun{{"exact source", []string{second}, []int64{1}}},
				},

				{
					name: "three operand source boundary", expr: "field(coalesce(?, ?, abs(?)), abs(" + first + "))",
					runs: []fieldBoundaryRun{
						{"typed null", []string{"cast(null as decimal(20,0))", "cast(null as char)", second}, []int64{1}},
						{"reversed types", []string{"cast(null as char)", "cast(null as decimal(20,0))", second}, []int64{1}},
						{"selected text", []string{"cast(null as decimal(20,0))", "'9007199254740993'", second}, []int64{1}},
						{"all exact", []string{"cast(null as decimal(20,0))", "cast(null as decimal(20,0))", second}, []int64{0}},
						{"real null", []string{"cast(null as decimal(20,0))", "cast(null as double)", second}, []int64{1}},
					},
				},
				{
					name: "unflattened common result", expr: "field(coalesce(x, abs(" + second + ")), abs(" + first + ")) from (select ? as x limit 1) d",
					runs: []fieldBoundaryRun{{"exact", []string{second}, []int64{0}}, {"typed text null", []string{"null"}, []int64{1}}},
				},
				{
					name: "nested common result", expr: "field(coalesce(x, abs(" + second + ")), abs(" + first + ")) from (select coalesce(?, " + second + ") as x) d",
					runs: []fieldBoundaryRun{{"typed text null stays a string", []string{"null"}, []int64{1}}},
				},
				{
					name: "table backed scalar output", expr: "field((select ? from field_decimal where search=123 limit 1), abs(" + first + "))",
					runs: []fieldBoundaryRun{{"selected numeric source", []string{second}, []int64{0}}},
				},

				{
					name: "unflattened reuse", expr: "field(x, abs(" + first + ")) from (select ? as x limit 1) d",
					runs: []fieldBoundaryRun{
						{"decimal", []string{second}, []int64{0}},
						{"text", []string{"'9007199254740993'"}, []int64{1}},
						{"real", []string{"cast(9007199254740993 as double)"}, []int64{1}},
						{"null", []string{"null"}, []int64{0}},
						{"decimal again", []string{second}, []int64{0}},
					},
				},
				{
					name: "union both markers", expr: "field(x, abs(" + first + ")) from (select ? as x union all select ?) d order by 1",
					runs: []fieldBoundaryRun{{"both exact", []string{second, first}, []int64{0, 1}}},
				},
				{
					name: "union numeric and text source", expr: "field(x, abs(" + first + ")) from (select ? as x union all select ?) d order by 1",
					runs: []fieldBoundaryRun{
						{"decimal then text", []string{second, "'9007199254740993'"}, []int64{1, 1}},
						{"text then decimal", []string{"'9007199254740993'", second}, []int64{1, 1}},
					},
				},
				{
					name: "union typed text null", expr: "field(x, abs(" + first + ")) from (select ? as x union all select ?) d order by 1",
					runs: []fieldBoundaryRun{{"null and exact", []string{"cast(null as char)", first}, []int64{0, 1}}},
				},
				{
					name: "union left marker", expr: "field(x, abs(" + first + ")) from (select ? as x union all select " + second + ") d",
					runs: []fieldBoundaryRun{{"exact", []string{second}, []int64{0, 0}}},
				},
				{
					name: "union right marker", expr: "field(x, abs(" + first + ")) from (select " + second + " as x union all select ?) d",
					runs: []fieldBoundaryRun{{"exact", []string{second}, []int64{0, 0}}},
				},
				{
					name: "union distinct", expr: "field(x, abs(" + first + ")) from (select " + second + " as x union select ?) d",
					runs: []fieldBoundaryRun{{"exact", []string{second}, []int64{0}}, {"real", []string{"cast(9007199254740993 as double)"}, []int64{1}}},
				},
				{
					name: "where cardinality", expr: "1 from (select ? as x) d where field(x, abs(" + first + "))=0",
					runs: []fieldBoundaryRun{{"retained", []string{second}, []int64{1}}, {"filtered", []string{first}, nil}},
				},
				{
					name: "having cardinality", expr: "1 from (select ? as x) d group by x having field(x, abs(" + first + "))=0",
					runs: []fieldBoundaryRun{{"retained", []string{second}, []int64{1}}, {"filtered", []string{first}, nil}},
				},
				{
					name: "empty projection", expr: "field(x, abs(" + first + ")) from (select ? as x limit 0) d",
					runs: []fieldBoundaryRun{{"no rows", []string{second}, nil}},
				},
				{
					name: "outer join null extension", expr: "field(x, abs(" + second + ")) from (select 1 as id) a left join (select max(?) as x) d on a.id=2",
					runs: []fieldBoundaryRun{{"null rather than parameter", []string{second}, []int64{0}}},
				},
				{
					name: "projected explicit char", expr: "field(cast(x as char), abs(" + first + ")) from (select ? as x limit 1) d",
					runs: []fieldBoundaryRun{{"string remains boundary", []string{second}, []int64{1}}},
				},

				{
					name: "extra text null candidate", expr: "field(?, ?, abs(" + first + "), abs(" + second + "))",
					runs: []fieldBoundaryRun{{"exact search", []string{second, "null"}, []int64{2}}},
				},
			}
			// One fixture, with one witness for each relational ownership boundary.
			for _, relation := range []string{
				"(select ? as x limit 1) d",
				"(select x from (select ? as x limit 1) a limit 1) d",
				"(select max(?) as x) d", "(select sum(?) as x) d", "(select avg(?) as x) d",
				"(select ? as x group by x) d", "(select distinct ? as x) d",
			} {
				cases = append(cases, fieldBoundaryCase{
					name: relation, expr: "field(x, abs(" + first + ")) from " + relation,
					runs: []fieldBoundaryRun{{"exact source", []string{second}, []int64{0}}},
				})
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					_, err := conn.ExecContext(ctx, "prepare field_review from 'select "+tc.expr+"'")
					require.NoError(t, err)
					defer func() {
						cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						_, cleanupErr := conn.ExecContext(cleanupCtx, "deallocate prepare field_review")
						require.NoError(t, cleanupErr)
					}()
					for _, run := range tc.runs {
						t.Run(run.name, func(t *testing.T) {
							directExpr := tc.expr
							variables := make([]string, len(run.values))
							for i, source := range run.values {
								variables[i] = fmt.Sprintf("@field_review_%d", i)
								directExpr = strings.Replace(directExpr, "?", variables[i], 1)
								_, err := conn.ExecContext(ctx, "set "+variables[i]+" = "+source)
								require.NoError(t, err)
							}
							query := func(sql string) []int64 {
								rows, err := conn.QueryContext(ctx, sql)
								require.NoError(t, err)
								defer rows.Close()
								var result []int64
								for rows.Next() {
									var value int64
									require.NoError(t, rows.Scan(&value))
									result = append(result, value)
								}
								require.NoError(t, rows.Err())
								return result
							}
							require.Equal(t, run.want, query("select "+directExpr), "direct oracle")
							require.Equal(t, run.want, query("execute field_review using "+strings.Join(variables, ", ")))
						})
					}
				})
			}
		})
	})
}
