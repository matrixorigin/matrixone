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
		})

		t.Run("issue 29378 explicit string and nested null boundaries", func(t *testing.T) {
			const first = "cast(9007199254740992 as decimal(20,0))"
			const second = "cast(9007199254740993 as decimal(20,0))"
			type fieldBoundaryRun struct {
				name   string
				values []string
				want   int64
			}
			for _, tc := range []struct {
				name, expr string
				runs       []fieldBoundaryRun
			}{
				{
					name: "explicit char peer", expr: "field(?, cast(9007199254740993 as char))",
					runs: []fieldBoundaryRun{
						{"distinct decimal", []string{first}, 1},
						{"matching decimal", []string{second}, 1},
						{"decimal after reuse", []string{first}, 1},
					},
				},
				{
					name: "folded explicit char peer", expr: "field(?, cast(abs(" + second + ") as char))",
					runs: []fieldBoundaryRun{{"distinct decimal", []string{first}, 1}},
				},
				{
					name: "null then nested abs", expr: "field(coalesce(?, abs(?)), abs(" + first + "))",
					runs: []fieldBoundaryRun{
						{"null and distinct decimal", []string{"null", second}, 0},
						{"null and matching decimal", []string{"null", first}, 1},
						{"string boundary", []string{"'9007199254740993'", second}, 1},
						{"null after string", []string{"null", second}, 0},
						{"real boundary", []string{"null", "cast(9007199254740993 as double)"}, 1},
						{"null after real", []string{"null", second}, 0},
					},
				},
			} {
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
								directExpr = strings.Replace(directExpr, "?", source, 1)
								variables[i] = fmt.Sprintf("@field_review_%d", i)
								_, err := conn.ExecContext(ctx, "set "+variables[i]+" = "+source)
								require.NoError(t, err)
							}
							var direct, prepared int64
							require.NoError(t, conn.QueryRowContext(ctx, "select "+directExpr).Scan(&direct))
							require.Equal(t, run.want, direct)
							require.NoError(t, conn.QueryRowContext(ctx,
								"execute field_review using "+strings.Join(variables, ", ")).Scan(&prepared))
							require.Equal(t, run.want, prepared)
						})
					}
				})
			}
		})
	})
}
