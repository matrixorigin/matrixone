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
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestSetOperationSignedZero(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists set_signed_zero")
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		for _, q := range []string{
			"create database set_signed_zero", "use set_signed_zero",
			"create table zeros(id int primary key, f float, d double, n decimal(4,2))",
			"insert into zeros values (1,-0e0,-0e0,-0.00),(2,0e0,0e0,0.00),(3,-0e0,-0e0,-0.00),(4,0e0,0e0,0.00),(5,NULL,NULL,NULL),(6,1.25,1.25,1.25),(7,1.25,1.25,1.25)",
		} {
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		for _, col := range []string{"f", "d"} {
			for _, tc := range []struct {
				name, set string
				want      int
			}{
				{"union", "select %[1]s from zeros where id=1 union select %[1]s from zeros where id=2", 2},
				{"intersect", "select %[1]s from zeros where id=1 intersect select %[1]s from zeros where id=2", 0},
				{"except", "select %[1]s from zeros where id=1 except select %[1]s from zeros where id=2", 1},
				{"same-sign", "select %[1]s from zeros where id=1 union select %[1]s from zeros where id=3", 1},
				{"union-all", "select %[1]s from zeros where id=1 union all select %[1]s from zeros where id=2", 2},
				{"intersect-all-opposite", "select %[1]s from zeros where id in(1,3) intersect all select %[1]s from zeros where id in(2,4)", 0},
				{"except-all-opposite", "select %[1]s from zeros where id in(1,3) except all select %[1]s from zeros where id=2", 2},
				{"intersect-all-same", "select %[1]s from zeros where id in(1,3) intersect all select %[1]s from zeros where id=1", 1},
				{"except-all-same", "select %[1]s from zeros where id in(1,3) except all select %[1]s from zeros where id=1", 1},
				{"null-and-finite", "select %[1]s from zeros union select %[1]s from zeros", 4},
				{"parenthesized-self-union", "(select %[1]s from zeros) union (select %[1]s from zeros)", 4},
				{"empty", "select %[1]s from zeros where id<0 union select %[1]s from zeros where id<0", 0},
				{"reverse-except", "select %[1]s from zeros where id=2 except select %[1]s from zeros where id=1", 1},
				{"nested-chain", "select %[1]s from zeros where id=1 union select %[1]s from zeros where id=2 union select %[1]s from zeros where id=3", 2},
			} {
				t.Run(col+"/"+tc.name, func(t *testing.T) {
					q := "select count(*) from (" + fmt.Sprintf(tc.set, col) + ") s"
					if tc.name == "null-and-finite" {
						rows, err := conn.QueryContext(ctx, fmt.Sprintf("select %[1]s is null,cast(%[1]s as varchar) from (%[2]s) s order by %[1]s is null desc,serial(%[1]s)", col, fmt.Sprintf(tc.set, col)))
						require.NoError(t, err)
						require.Equal(t, []string{"1:NULL", "0:-0", "0:0", "0:1.25"}, readWindowNullRows(t, rows))
					}
					var count int
					require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&count), q)
					require.Equal(t, tc.want, count, q)
				})
			}
			t.Run(col+"/representatives", func(t *testing.T) {
				q := fmt.Sprintf("select cast(x as varchar) from (select %[1]s x from zeros where id=1 union select %[1]s from zeros where id=2) s order by serial(x)", col)
				rows, err := conn.QueryContext(ctx, q)
				require.NoError(t, err)
				require.Equal(t, []string{"-0", "0"}, readWindowNullRows(t, rows))
			})
		}
		for _, tc := range []struct {
			name, q string
			want    int
		}{
			{"decimal", "select count(*) from (select n from zeros where id=1 union select n from zeros where id=2) s", 1},
			{"scalar", "select -0e0=0e0", 1},
			{"distinct", "select count(distinct d) from zeros where id<=4", 1},
			{"select-distinct", "select count(*) from (select distinct d from zeros where id<=4) s", 1},
			{"group", "select count(*) from (select d from zeros where id<=4 group by d) s", 1},
			{"literal-union", "select count(*) from (select -0e0 union select 0e0) s", 2},
			{"literal-intersect", "select count(*) from (select -0e0 intersect select 0e0) s", 0},
			{"literal-except", "select count(*) from (select -0e0 except select 0e0) s", 1},
			{"pruned-payload", "select count(*) from (select 1 k,d from zeros where id=1 union select 1,d from zeros where id=2) s", 2},
			{"pad-space-float", "select count(*) from (select cast('a' as char(4)) s,d from zeros where id=1 union select cast('a ' as varchar(4)),d from zeros where id=2) s", 2},
			{"pad-space-same-sign", "select count(*) from (select cast('a' as char(4)) s,d from zeros where id=1 union select cast('a ' as varchar(4)),d from zeros where id=3) s", 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var count int
				require.NoError(t, conn.QueryRowContext(ctx, tc.q).Scan(&count), tc.q)
				require.Equal(t, tc.want, count, tc.q)
			})
		}
		t.Run("stored-signs", func(t *testing.T) {
			rows, err := conn.QueryContext(ctx, "select hex(serial(d)) from zeros where id<=2 order by id")
			require.NoError(t, err)
			require.Equal(t, []string{"217FFFFFFFFFFFFFFF", "218000000000000000"}, readWindowNullRows(t, rows))
			rows, err = conn.QueryContext(ctx, "select cast(x as varchar) from (select d x from zeros where id=1 except select d from zeros where id=2) s")
			require.NoError(t, err)
			require.Equal(t, []string{"-0"}, readWindowNullRows(t, rows))
		})
		t.Run("aggregate-controls", func(t *testing.T) {
			rows, err := conn.QueryContext(ctx, "select count(*),count(distinct d),approx_count_distinct(d),hll_cardinality(hll_add_agg(d)) from zeros where id<=4")
			require.NoError(t, err)
			require.Equal(t, []string{"4:1:1:1"}, readWindowNullRows(t, rows))
		})
		t.Run("correlated-union", func(t *testing.T) {
			for _, q := range []string{
				"create table ci_inner(oref int,grp int,ie int)",
				"insert into ci_inner values(1,1,1),(1,1,1),(1,2,NULL),(2,1,3),(3,1,4),(3,2,NULL)",
				"create table ci_outer(id int,oref int,a int)",
				"insert into ci_outer values(1,1,1),(2,2,2),(3,3,3),(4,4,NULL),(5,2,NULL)",
			} {
				_, err := conn.ExecContext(ctx, q)
				require.NoError(t, err, q)
			}
			for _, tc := range []struct {
				name, query string
				want        []string
			}{
				{"exists", "select id from ci_outer o where exists ((select 1 from ci_inner i where i.oref=o.oref) union (select 1 from ci_inner i where i.oref=o.oref)) order by id", []string{"1", "2", "3", "5"}},
				{"multiple-identities", "select id from ci_outer o where exists (select 1 from ci_inner i where i.oref=o.oref and i.ie=o.a union select 1 from ci_inner i where i.oref=o.oref and i.ie=o.a) order by id", []string{"1"}},
				{"non-equality", "select id from ci_outer o where exists (select 1 from ci_inner i where i.oref>o.oref union select 1 from ci_inner i where i.oref>o.oref) order by id", []string{"1", "2", "5"}},
				{"membership", "select id,a in (select ie from ci_inner i where i.oref=o.oref union select ie from ci_inner i where i.oref=o.oref) from ci_outer o order by id", []string{"1:1", "2:0", "3:NULL", "4:0", "5:NULL"}},
				{"aggregate", "select id,a in (select max(ie) from ci_inner i where i.oref=o.oref group by grp union select max(ie) from ci_inner i where i.oref=o.oref group by grp) from ci_outer o order by id", []string{"1:1", "2:0", "3:NULL", "4:0", "5:NULL"}},
				{"scalar-identities", "select id,(select 1 from ci_inner i where i.oref=o.oref union select 1 from ci_inner i where i.oref=o.oref) from ci_outer o order by id", []string{"1:1", "2:1", "3:1", "4:NULL", "5:1"}},
				{"float-identities", "select id,(select 1 from zeros i where i.d=o.d union select 1 from zeros i where i.d=o.d) from zeros o where id<=4 order by id", []string{"1:1", "2:1", "3:1", "4:1"}},
				{"float-members", "select id,cast((select d from zeros i where i.id=o.id union select d from zeros i where i.id=o.id) as varchar) from zeros o where id<=4 order by id", []string{"1:-0", "2:0", "3:-0", "4:0"}},
				{"float-aggregate-boundary", "select id,cast((select max(d) from zeros i where i.id=o.id and i.id<=3 union select max(d) from zeros i where i.id=o.id and i.id<=3) as varchar) from ci_outer o order by id", []string{"1:-0", "2:0", "3:-0", "4:NULL", "5:NULL"}},
				{"integer-limit-two-boundary", "select id,((select ie from ci_inner i where i.oref=o.oref and ie is not null limit 2) union (select ie from ci_inner i where i.oref=o.oref and ie is not null limit 2)) from ci_outer o order by id", []string{"1:1", "2:3", "3:4", "4:NULL", "5:3"}},
				{"integer-grouped-boundary", "select id,(select max(ie) from ci_inner i where i.oref=o.oref and ie is not null group by grp union select max(ie) from ci_inner i where i.oref=o.oref and ie is not null group by grp) from ci_outer o order by id", []string{"1:1", "2:3", "3:4", "4:NULL", "5:3"}},
				{"float-grouped-boundary", "select id,cast((select max(d) from zeros i where i.id=o.id and i.id<=3 group by i.id union select max(d) from zeros i where i.id=o.id and i.id<=3 group by i.id) as varchar) from ci_outer o order by id", []string{"1:-0", "2:0", "3:-0", "4:NULL", "5:NULL"}},
				{"float-limit-two-boundary", "select id,cast(((select d from zeros i where i.id=o.id and i.id<=3 limit 2) union (select d from zeros i where i.id=o.id and i.id<=3 limit 2)) as varchar) from ci_outer o order by id", []string{"1:-0", "2:0", "3:-0", "4:NULL", "5:NULL"}},
				{"float-pagination-boundary", "select id,cast(((select d from zeros i where i.id=o.id and i.id<=3 limit 1) union (select d from zeros i where i.id=o.id and i.id<=3 limit 1)) as varchar) from ci_outer o order by id", []string{"1:-0", "2:0", "3:-0", "4:NULL", "5:NULL"}},
				{"float-ordered-pagination", "select id,cast(((select d from zeros i where i.id=o.id and i.id<=3 order by id limit 1) union (select d from zeros i where i.id=o.id and i.id<=3 order by id limit 1)) as varchar) from ci_outer o order by id", []string{"1:-0", "2:0", "3:-0", "4:NULL", "5:NULL"}},
				{"float-offset-pagination", "select id,cast(((select cast(ie as double) from ci_inner i where i.oref=o.oref and ie is not null order by ie limit 1 offset 1) union (select cast(ie as double) from ci_inner i where i.oref=o.oref and ie is not null order by ie limit 1 offset 1)) as varchar) from ci_outer o order by id", []string{"1:1", "2:NULL", "3:NULL", "4:NULL", "5:NULL"}},
				{"float-zero-limit", "select id,cast(((select d from zeros i where i.id=o.id limit 0) union (select d from zeros i where i.id=o.id limit 0)) as varchar) from ci_outer o order by id", []string{"1:NULL", "2:NULL", "3:NULL", "4:NULL", "5:NULL"}},
				{"float-real-null", "select id,cast((select max(d) from zeros i where i.id=o.id union select max(d) from zeros i where i.id=o.id) as varchar) from ci_outer o order by id", []string{"1:-0", "2:0", "3:-0", "4:0", "5:NULL"}},
				{"float-count-empty", "select id,cast((select cast(count(*) as double) from zeros i where i.id=o.id and i.id<=3 union select cast(count(*) as double) from zeros i where i.id=o.id and i.id<=3) as varchar) from ci_outer o order by id", []string{"1:1", "2:1", "3:1", "4:0", "5:0"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					rows, err := conn.QueryContext(ctx, tc.query)
					require.NoError(t, err, tc.query)
					require.Equal(t, tc.want, readWindowNullRows(t, rows), tc.query)
				})
			}

			t.Run("multirow-boundary-membership", func(t *testing.T) {
				for _, q := range []string{
					"create table boundary_inner(k int,g int,d double)",
					"insert into boundary_inner values(1,10,-0e0),(1,20,-0e0),(1,30,-0e0),(2,10,-0e0),(2,20,-0e0),(3,10,NULL),(3,20,NULL),(5,10,-0e0),(5,20,0e0),(6,10,1e0),(6,20,2e0)",
				} {
					_, err := conn.ExecContext(ctx, q)
					require.NoError(t, err, q)
				}
				for _, tc := range []struct{ name, arm string }{
					{"limit-two", "select d from boundary_inner i where i.k=o.id order by g limit 2"},
					{"grouped", "select max(d) from boundary_inner i where i.k=o.id group by g"},
					{"offset", "select d from boundary_inner i where i.k=o.id order by g limit 2 offset 1"},
					{"float-identity", "select d from boundary_inner i where cast(i.k as double)=cast(o.id as double) order by g limit 2"},
				} {
					t.Run(tc.name, func(t *testing.T) {
						q := "select id,cast(((" + tc.arm + ") union (" + tc.arm + ")) as varchar) from ci_outer o where id<=4 order by id"
						rows, err := conn.QueryContext(ctx, q)
						require.NoError(t, err, q)
						require.Equal(t, []string{"1:-0", "2:-0", "3:NULL", "4:NULL"}, readWindowNullRows(t, rows), "duplicate members collapse within, but never across, correlation partitions")
					})
				}
				for _, arm := range []string{
					"select d from boundary_inner i where i.k=o.id order by g limit 2",
					"select max(d) from boundary_inner i where i.k=o.id group by g",
				} {
					for _, key := range []int{5, 6} {
						q := fmt.Sprintf("select ((%s) union (%s)) from (select %d id) o", arm, arm, key)
						var value sql.NullFloat64
						err := conn.QueryRowContext(ctx, q).Scan(&value)
						require.EqualError(t, err, "Error 1242 (21000): Subquery returns more than 1 row", q)
					}
				}
				// OFFSET belongs to the arm, before UNION membership. Moving or
				// dropping it changes the scalar result or its cardinality.
				for _, tc := range []struct {
					key, offset int
					want        []string
				}{
					{5, 1, []string{"0"}}, {5, 2, []string{"NULL"}}, {6, 1, []string{"2"}},
				} {
					arm := fmt.Sprintf("select d from boundary_inner i where i.k=o.id order by g limit 2 offset %d", tc.offset)
					q := fmt.Sprintf("select cast(((%s) union (%s)) as varchar) from (select %d id) o", arm, arm, tc.key)
					rows, err := conn.QueryContext(ctx, q)
					require.NoError(t, err, q)
					require.Equal(t, tc.want, readWindowNullRows(t, rows), q)
				}
			})

			t.Run("retained-correlation-shapes", func(t *testing.T) {
				for _, tc := range []struct {
					name, predicate string
					want            []string
				}{
					{"multiple", "i.oref=o.oref and i.ie=o.a", []string{"1"}},
					{"range", "i.oref>o.oref", []string{"1", "2", "5"}},
				} {
					t.Run(tc.name, func(t *testing.T) {
						q := "select id from ci_outer o where exists (select cast(ie as double) from ci_inner i where " + tc.predicate + " union select cast(ie as double) from ci_inner i where " + tc.predicate + ") order by id"
						rows, err := conn.QueryContext(ctx, q)
						require.NoError(t, err, q)
						require.Equal(t, tc.want, readWindowNullRows(t, rows))
						q = "select id from ci_outer o where exists (select 1 from ci_inner i where " + tc.predicate + " and ? union select 1 from ci_inner i where " + tc.predicate + " and ?) order by id"
						stmt, err := conn.PrepareContext(ctx, q)
						require.NoError(t, err)
						defer stmt.Close()
						for _, args := range [][2]int{{0, 1}, {1, 0}, {0, 0}, {1, 1}, {0, 1}} {
							rows, err := stmt.QueryContext(ctx, args[0], args[1])
							require.NoError(t, err, q)
							want := tc.want
							if args == [2]int{0, 0} {
								want = nil
							}
							require.Equal(t, want, readWindowNullRows(t, rows))
						}
					})
				}

				rows, err := conn.QueryContext(ctx, "select id,cast((select cast(ie as double) from ci_inner i where i.oref=o.oref and i.ie=o.a union select cast(ie as double) from ci_inner i where i.oref=o.oref and i.ie=o.a) as varchar) from ci_outer o order by id")
				require.NoError(t, err)
				require.Equal(t, []string{"1:1", "2:NULL", "3:NULL", "4:NULL", "5:NULL"}, readWindowNullRows(t, rows))
				rows, err = conn.QueryContext(ctx, "select id,a in (select cast(ie as double) from ci_inner i where i.oref=o.oref and i.ie=o.a union select cast(ie as double) from ci_inner i where i.oref=o.oref and i.ie=o.a) from ci_outer o order by id")
				require.NoError(t, err)
				require.Equal(t, []string{"1:1", "2:0", "3:0", "4:0", "5:0"}, readWindowNullRows(t, rows))
				var rejected float64
				err = conn.QueryRowContext(ctx, "select (select cast(ie as double) from ci_inner i where i.oref>o.oref union select cast(ie as double) from ci_inner i where i.oref>o.oref) from ci_outer o").Scan(&rejected)
				require.EqualError(t, err, "Error 1242 (21000): Subquery returns more than 1 row")
				q := "select id from ci_outer o where not exists (select cast(ie as double) from ci_inner i where i.oref>o.oref union select cast(ie as double) from ci_inner i where i.oref>o.oref) order by id"
				rows, err = conn.QueryContext(ctx, q)
				require.NoError(t, err, q)
				require.Equal(t, []string{"3", "4"}, readWindowNullRows(t, rows))
			})

			t.Run("missing-arm-identity-diagnostic", func(t *testing.T) {
				q := "select o.id from ci_outer o where exists (with q(n) as (select o.oref) select n from q union all select 7) and o.id=2"
				_, err := conn.ExecContext(ctx, q)
				require.EqualError(t, err, "Error 20102 (HY000): correlated local CTE: UNION ALL without matching row identities is not yet implemented")
			})

			t.Run("existential-predicate-templates", func(t *testing.T) {
				for _, q := range []string{
					"create table predicate_inner(a int,b int,f double)",
					"create table predicate_outer(id int,a int,b int,f double)",
					"insert into predicate_inner values(1,10,-0e0),(1,10,-0e0),(2,20,0e0),(3,30,1e0),(NULL,40,NULL)",
					"insert into predicate_outer values(1,1,10,-0e0),(2,2,20,0e0),(3,3,30,1e0),(4,NULL,40,NULL),(5,9,99,2e0)",
				} {
					_, err := conn.ExecContext(ctx, q)
					require.NoError(t, err, q)
				}
				qAggregate := "select id from predicate_outer o where exists (select count(*) from (select a,f from predicate_inner union select a,f from predicate_inner) u where u.a=o.a having count(*)=1) order by id"
				aggregateRows, aggregateErr := conn.QueryContext(ctx, qAggregate)
				require.NoError(t, aggregateErr, qAggregate)
				require.Equal(t, []string{"1", "2", "3"}, readWindowNullRows(t, aggregateRows))
				for _, tc := range []struct {
					name, predicate string
					want            []string
				}{
					{"or", "i.a=o.a or i.b=o.b", []string{"1", "2", "3", "4"}},
					{"outer-conjunct", "i.a=o.a and o.b>10", []string{"2", "3"}},
					{"outer-only", "o.b>10", []string{"2", "3", "4", "5"}},
					{"null-safe", "i.a<=>o.a", []string{"1", "2", "3", "4"}},
					{"float-range", "i.f>o.f", []string{"1", "2"}},
				} {
					t.Run(tc.name, func(t *testing.T) {
						t.Run("membership", func(t *testing.T) {
							wants := map[string][]string{
								"or":             {"1:1", "2:1", "3:1", "4:NULL", "5:0"},
								"outer-conjunct": {"1:0", "2:1", "3:1", "4:0", "5:0"},
								"outer-only":     {"1:0", "2:1", "3:1", "4:NULL", "5:NULL"},
								"null-safe":      {"1:1", "2:1", "3:1", "4:NULL", "5:0"},
								"float-range":    {"1:0", "2:0", "3:0", "4:0", "5:0"},
							}
							arm := "select f from predicate_inner i where (" + tc.predicate + ")"
							q := "select id,f in (" + arm + " union " + arm + ") from predicate_outer o order by id"
							rows, err := conn.QueryContext(ctx, q)
							require.NoError(t, err, q)
							require.Equal(t, wants[tc.name], readWindowNullRows(t, rows))
							q = "select id,f not in (" + arm + " union " + arm + ") from predicate_outer o order by id"
							rows, err = conn.QueryContext(ctx, q)
							require.NoError(t, err, q)
							notWant := append([]string(nil), wants[tc.name]...)
							for i, want := range notWant {
								if want[len(want)-1] == '0' {
									notWant[i] = want[:len(want)-1] + "1"
								}
								if want[len(want)-1] == '1' {
									notWant[i] = want[:len(want)-1] + "0"
								}
							}
							require.Equal(t, notWant, readWindowNullRows(t, rows))

						})
						t.Run("scalar", func(t *testing.T) {
							if tc.name == "outer-only" {
								var value sql.NullFloat64
								err := conn.QueryRowContext(ctx, "select (select f from predicate_inner i where o.b>10 union select f from predicate_inner i where o.b>10) from predicate_outer o where id=2").Scan(&value)
								require.EqualError(t, err, "Error 1242 (21000): Subquery returns more than 1 row")
								return
							}
							wants := map[string][]string{
								"or":             {"1:-0", "2:0", "3:1", "4:NULL", "5:NULL"},
								"outer-conjunct": {"1:NULL", "2:0", "3:1", "4:NULL", "5:NULL"},
								"null-safe":      {"1:-0", "2:0", "3:1", "4:NULL", "5:NULL"},
								"float-range":    {"1:1", "2:1", "3:NULL", "4:NULL", "5:NULL"},
							}
							arm := "select f from predicate_inner i where (" + tc.predicate + ")"
							q := "select id,cast((" + arm + " union " + arm + ") as varchar) from predicate_outer o order by id"
							rows, err := conn.QueryContext(ctx, q)
							require.NoError(t, err, q)
							require.Equal(t, wants[tc.name], readWindowNullRows(t, rows))
						})
						arm := "select f from predicate_inner i where (" + tc.predicate + ")"
						q := "select id from predicate_outer o where exists (" + arm + " union " + arm + ") order by id"
						rows, err := conn.QueryContext(ctx, q)
						require.NoError(t, err, q)
						require.Equal(t, tc.want, readWindowNullRows(t, rows))
						q = "select id from predicate_outer o where exists ((" + arm + " union " + arm + ") union " + arm + ") order by id"
						rows, err = conn.QueryContext(ctx, q)
						require.NoError(t, err, q)
						require.Equal(t, tc.want, readWindowNullRows(t, rows))
						excluded := []string{}
						for _, id := range []string{"1", "2", "3", "4", "5"} {
							included := false
							for _, match := range tc.want {
								included = included || id == match
							}
							if !included {
								excluded = append(excluded, id)
							}
						}
						q = "select id from predicate_outer o where not exists (" + arm + " union " + arm + ") order by id"
						rows, err = conn.QueryContext(ctx, q)
						require.NoError(t, err, q)
						require.Equal(t, excluded, readWindowNullRows(t, rows))
						arm = "select 1 from predicate_inner i where (" + tc.predicate + ") and ?"
						q = "select id from predicate_outer o where exists (" + arm + " union " + arm + ") order by id"
						stmt, err := conn.PrepareContext(ctx, q)
						require.NoError(t, err)
						defer stmt.Close()
						for _, args := range [][2]int{{0, 1}, {1, 0}, {0, 0}, {1, 1}, {0, 1}} {
							rows, err := stmt.QueryContext(ctx, args[0], args[1])
							require.NoError(t, err, q)
							want := tc.want
							if args == [2]int{0, 0} {
								want = nil
							}
							require.Equal(t, want, readWindowNullRows(t, rows))
						}
					})
				}
				t.Run("scalar-outer-shapes", func(t *testing.T) {
					for _, shape := range []string{
						"predicate_outer o",
						"(select id,a,b,f from predicate_outer) o",
						"(select id,a+0 as a,b,f from predicate_outer) o",
						"predicate_outer o inner join predicate_outer p on p.id=o.id",
						"predicate_outer o left join predicate_outer p on p.id=o.id and p.id<3",
						"(select id,a,b,f from predicate_outer union all select id,a,b,f from predicate_outer) o",
						"(select distinct id,a,b,f from predicate_outer) o",
					} {
						t.Run(shape, func(t *testing.T) {
							arm := "select f from predicate_inner i where i.a=o.a or i.b=o.b"
							q := "select o.id,cast((" + arm + " union " + arm + ") as varchar) from " + shape + " order by o.id"
							rows, err := conn.QueryContext(ctx, q)
							require.NoError(t, err, q)
							want := []string{"1:-0", "2:0", "3:1", "4:NULL", "5:NULL"}
							if shape == "(select id,a,b,f from predicate_outer union all select id,a,b,f from predicate_outer) o" {
								want = []string{"1:-0", "1:-0", "2:0", "2:0", "3:1", "3:1", "4:NULL", "4:NULL", "5:NULL", "5:NULL"}
							}
							require.Equal(t, want, readWindowNullRows(t, rows))
						})
					}
				})
				t.Run("scalar-composition", func(t *testing.T) {
					first := "select f from predicate_inner i where i.a=o.a or i.b=o.b"
					first = "(" + first + " union " + first + ")"
					second := "select f from predicate_inner j where j.a=o.a and o.b>10"
					second = "(" + second + " union " + second + ")"
					for _, tc := range []struct {
						name, q string
						want    []string
					}{
						{"two-select", "select id,cast(" + first + " as varchar),cast(" + second + " as varchar) from predicate_outer o order by id", []string{"1:-0:NULL", "2:0:0", "3:1:1", "4:NULL:NULL", "5:NULL:NULL"}},
						{"where-then-select", "select id,cast(" + second + " as varchar) from predicate_outer o where " + first + " is not null order by id", []string{"1:NULL", "2:0", "3:1"}},
					} {
						t.Run(tc.name, func(t *testing.T) {
							rows, err := conn.QueryContext(ctx, tc.q)
							require.NoError(t, err, tc.q)
							require.Equal(t, tc.want, readWindowNullRows(t, rows))
						})
					}
				})
				t.Run("scalar-stream-identity", func(t *testing.T) {
					arm := "select f from predicate_inner i where i.a=o.a or i.b=o.b"
					scalar := "cast((" + arm + " union " + arm + ") as varchar)"
					for _, tc := range []struct {
						name, q string
						want    []string
					}{
						{"one-to-many", "select o.id," + scalar + " from predicate_outer o join predicate_inner p on p.a=o.a order by o.id", []string{"1:-0", "1:-0", "2:0", "3:1"}},
						{"left-null", "select o.id,p.id," + scalar + " from predicate_outer o left join predicate_outer p on p.id=o.id and p.id<3 order by o.id", []string{"1:1:-0", "2:2:0", "3:NULL:1", "4:NULL:NULL", "5:NULL:NULL"}},
						{"aliases", "select o.id,o.a,o.other_a," + scalar + " from (select id,a,a as other_a,b,f from predicate_outer) o order by o.id,o.other_a", []string{"1:1:1:-0", "2:2:2:0", "3:3:3:1", "4:NULL:NULL:NULL", "5:9:9:NULL"}},
						{"existing-window", "select o.id,row_number() over(order by o.id)," + scalar + " from predicate_outer o order by o.id", []string{"1:1:-0", "2:2:0", "3:3:1", "4:4:NULL", "5:5:NULL"}},
						{"derived-window", "select o.id,o.rn," + scalar + " from (select id,a,b,row_number() over(order by id) rn from predicate_outer) o order by o.id", []string{"1:1:-0", "2:2:0", "3:3:1", "4:4:NULL", "5:5:NULL"}},
						{"empty-outer", "select o.id," + scalar + " from (select id,a,b,f from predicate_outer where id<0) o", nil},
					} {
						t.Run(tc.name, func(t *testing.T) {
							rows, err := conn.QueryContext(ctx, tc.q)
							require.NoError(t, err, tc.q)
							require.Equal(t, tc.want, readWindowNullRows(t, rows))
						})
					}
					first := "select f from predicate_inner i where i.a=o.a or i.b=o.b"
					first = "(" + first + " union " + first + ")"
					second := "select f from predicate_inner j where j.a=o.a and o.b>10"
					second = "(" + second + " union " + second + ")"
					rows, err := conn.QueryContext(ctx, "select id,cast("+second+" as varchar),cast("+first+" as varchar) from predicate_outer o order by id")
					require.NoError(t, err)
					require.Equal(t, []string{"1:NULL:-0", "2:0:0", "3:1:1", "4:NULL:NULL", "5:NULL:NULL"}, readWindowNullRows(t, rows))
					rows, err = conn.QueryContext(ctx, "select id,cast("+first+" as varchar) from predicate_outer o where "+second+" is not null order by id")
					require.NoError(t, err)
					require.Equal(t, []string{"2:0", "3:1"}, readWindowNullRows(t, rows))
				})
				t.Run("scalar-identity-across-batches", func(t *testing.T) {
					// One row beyond the standard batch boundary; identity must not
					// restart when the global WINDOW receives another input batch.
					_, err := conn.ExecContext(ctx, "create table scalar_outer_batches(id int,a int,b int)")
					require.NoError(t, err)
					_, err = conn.ExecContext(ctx, "insert into scalar_outer_batches select result,3,30 from generate_series(1,8193) g")
					require.NoError(t, err)
					q := "select count(*),count(v),sum(v) from (select (select f from predicate_inner i where i.a=o.a or i.b=o.b union select f from predicate_inner i where i.a=o.a or i.b=o.b) v from scalar_outer_batches o) u"
					var rows, values int
					var sum float64
					require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&rows, &values, &sum))
					require.Equal(t, 8193, rows)
					require.Equal(t, 8193, values)
					require.Equal(t, float64(8193), sum)
				})
				t.Run("scalar-set-boundaries", func(t *testing.T) {
					_, err := conn.ExecContext(ctx, "insert into predicate_inner values(4,50,-0e0)")
					require.NoError(t, err)
					q := "select cast((select f from predicate_inner i where i.a=1 or i.a=o.a+3 union select f from predicate_inner i where i.a=1 or i.a=o.a+3) as varchar) from predicate_outer o where id=1"
					var value string
					require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&value))
					require.Equal(t, "-0", value, "different hidden identities must collapse to one visible member")
					var scalar sql.NullInt64
					err = conn.QueryRowContext(ctx, "select (select 1 from predicate_inner i where i.a>o.a union select 2 from predicate_inner i where i.a>o.a) from predicate_outer o where id=1").Scan(&scalar)
					require.EqualError(t, err, "Error 1242 (21000): Subquery returns more than 1 row", "different constant arms still require scalar cardinality checking")
					for _, predicate := range []string{"i.a<3 and o.a=1", "i.a is null or i.a=o.a"} {
						q = "select (select f from predicate_inner i where " + predicate + " union select f from predicate_inner i where " + predicate + ") from predicate_outer o where id=1"
						var result sql.NullFloat64
						err := conn.QueryRowContext(ctx, q).Scan(&result)
						require.EqualError(t, err, "Error 1242 (21000): Subquery returns more than 1 row", q)
					}
					_, err = conn.ExecContext(ctx, "insert into predicate_outer values(1,1,10,-0e0)")
					require.NoError(t, err)
					q = "select id,cast((select f from predicate_inner i where i.a=o.a or i.b=o.b union select f from predicate_inner i where i.a=o.a or i.b=o.b) as varchar) from predicate_outer o where id=1"
					rows, err := conn.QueryContext(ctx, q)
					require.NoError(t, err)
					require.Equal(t, []string{"1:-0", "1:-0"}, readWindowNullRows(t, rows))
				})
				t.Run("prepared-scalar-set-domains", func(t *testing.T) {
					q := "select (select ? from predicate_inner i where i.a=o.a or i.b=o.b union select ? from predicate_inner i where i.a=o.a or i.b=o.b) from predicate_outer o where id=1"
					stmt, err := conn.PrepareContext(ctx, q)
					require.NoError(t, err)
					defer stmt.Close()
					var value sql.NullFloat64
					require.NoError(t, stmt.QueryRowContext(ctx, int64(0), int64(0)).Scan(&value))
					require.Equal(t, sql.NullFloat64{Float64: 0, Valid: true}, value)
					err = stmt.QueryRowContext(ctx, math.Copysign(0, -1), float64(0)).Scan(&value)
					require.EqualError(t, err, "Error 1242 (21000): Subquery returns more than 1 row")
					require.NoError(t, stmt.QueryRowContext(ctx, int64(0), int64(0)).Scan(&value))
					require.Equal(t, sql.NullFloat64{Float64: 0, Valid: true}, value)
				})
				q := "select id from predicate_outer o where not exists (select f from predicate_inner i where i.f>o.f union select f from predicate_inner i where i.f>o.f) order by id"
				rows, err := conn.QueryContext(ctx, q)
				require.NoError(t, err, q)
				require.Equal(t, []string{"3", "4", "5"}, readWindowNullRows(t, rows))
				q = "select id from predicate_outer o where exists (select f from predicate_inner i where o.b>0 and i.a<0 union select f from predicate_inner i where o.b>0 and i.a<0)"
				rows, err = conn.QueryContext(ctx, q)
				require.NoError(t, err, q)
				require.Empty(t, readWindowNullRows(t, rows), "outer-only true must not create an inner row")
			})
			t.Run("opposite-float-members", func(t *testing.T) {
				var value float64
				err := conn.QueryRowContext(ctx, "select (select d from zeros i where i.id=o.id union select -d from zeros i where i.id=o.id) from zeros o where id=1").Scan(&value)
				require.ErrorContains(t, err, "Subquery returns more than 1 row")
			})
			t.Run("view", func(t *testing.T) {
				_, err := conn.ExecContext(ctx, "create view ci_view as select id from ci_outer o where exists ((select 1 from ci_inner i where i.oref=o.oref) union (select 1 from ci_inner i where i.oref=o.oref))")
				require.NoError(t, err)
				rows, err := conn.QueryContext(ctx, "select id from ci_view order by id")
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3", "5"}, readWindowNullRows(t, rows))
			})
			t.Run("prepared-float-identities", func(t *testing.T) {
				for _, predicate := range []string{"i.d=o.d", "i.id=o.id and i.d=o.d"} {
					func() {
						q := "select id,(select ? from zeros i where " + predicate + " union select ? from zeros i where " + predicate + ") from zeros o where id<=4 order by id"
						stmt, err := conn.PrepareContext(ctx, q)
						require.NoError(t, err)
						defer stmt.Close()
						for _, value := range []any{int64(1), float64(1), int64(1)} {
							rows, err := stmt.QueryContext(ctx, value, value)
							require.NoError(t, err)
							require.Equal(t, []string{"1:1", "2:1", "3:1", "4:1"}, readWindowNullRows(t, rows))
						}
					}()
				}
			})
		})

		t.Run("keyed-filter-results", func(t *testing.T) {
			for _, op := range []string{"union", "intersect", "except"} {
				q := "select id,cast(d as varchar) from (select id,d from zeros " + op + " select id,d from zeros) u where id=1"
				rows, err := conn.QueryContext(ctx, q)
				require.NoError(t, err)
				want := []string{"1:-0"}
				if op == "except" {
					want = nil
				}
				require.Equal(t, want, readWindowNullRows(t, rows))
			}
			rows, err := conn.QueryContext(ctx, "select id,cast(d as varchar) from (select id,d from zeros union select id,d from zeros) u where id=6 and d>0")
			require.NoError(t, err)
			require.Equal(t, []string{"6:1.25"}, readWindowNullRows(t, rows))
			rows, err = conn.QueryContext(ctx, "select s from (select cast('a' as char(4)) s,1 k except select cast('a ' as varchar(4)),1) u where k=1 and hex(s)='61'")
			require.NoError(t, err)
			require.Empty(t, readWindowNullRows(t, rows), "equivalent members must cancel before the byte-sensitive predicate")
		})
		t.Run("prepared-reuse", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx, "select count(*) from (select d from zeros where id=? union select d from zeros where id=?) s")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct{ left, right, count int }{{1, 2, 2}, {1, 3, 1}, {5, 5, 1}, {1, 2, 2}} {
				var count int
				require.NoError(t, stmt.QueryRowContext(ctx, tc.left, tc.right).Scan(&count))
				require.Equal(t, tc.count, count)
			}
		})
		t.Run("prepared-domains", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx, "select count(*) from (select ? x union select ?) s")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct {
				left, right any
				count       int
			}{{math.Copysign(0, -1), float64(0), 2}, {int64(0), int64(0), 1}, {"0", "0", 1}, {nil, nil, 1}, {math.Copysign(0, -1), float64(0), 2}} {
				var count int
				require.NoError(t, stmt.QueryRowContext(ctx, tc.left, tc.right).Scan(&count))
				require.Equal(t, tc.count, count)
			}
		})
		t.Run("prepared-branch-predicates", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx, "select count(*) from (select 1 where ? union select 1 where ?) s")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct{ left, right, count int }{{0, 1, 1}, {1, 0, 1}, {0, 0, 0}, {1, 1, 1}, {0, 1, 1}} {
				var count int
				require.NoError(t, stmt.QueryRowContext(ctx, tc.left, tc.right).Scan(&count))
				require.Equal(t, tc.count, count)
			}
		})
	})
}
