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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

// TestIssue28469BinaryPreparedIntegerAssignment verifies that FLOAT parameters
// retain ties-to-even semantics beneath numeric expressions and that negative
// FLOAT values reach the unsigned assignment boundary without wrapping.
func TestIssue28469BinaryPreparedIntegerAssignment(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
			cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()

		ordinaryPrepared, err := conn.PrepareContext(ctx, "select ?/2 as q")
		require.NoError(t, err)
		defer ordinaryPrepared.Close()
		assertFloatRows := func(t *testing.T, query func() (*sql.Rows, error), want float64) {
			t.Helper()
			rows, err := query()
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, columns, 1)
			require.Equal(t, "DOUBLE", columns[0].DatabaseTypeName())
			require.True(t, rows.Next())
			var got float64
			require.NoError(t, rows.Scan(&got))
			require.Equal(t, want, got)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}
		checkOrdinarySelect := func(t *testing.T) {
			assertFloatRows(t, func() (*sql.Rows, error) {
				return conn.QueryContext(ctx, "select 5/2 as q")
			}, 2.5)
			assertFloatRows(t, func() (*sql.Rows, error) {
				return ordinaryPrepared.QueryContext(ctx, int64(5))
			}, 2.5)
			mustExec(t, ctx, conn, "create table select_contract as select 5/2 as q")
			defer func() { _, _ = conn.ExecContext(ctx, "drop table select_contract") }()
			assertFloatRows(t, func() (*sql.Rows, error) {
				return conn.QueryContext(ctx, "select q from select_contract")
			}, 2.5)
		}
		t.Run("ordinary_select_before_integer_dml", checkOrdinarySelect)

		mustExec(t, ctx, conn, "create table src (x bigint)")
		mustExec(t, ctx, conn, "insert into src values (5)")
		mustExec(t, ctx, conn, "create table dst (v bigint)")
		for _, tc := range []struct {
			name, query string
			want        int64
		}{
			{"constant_division", "insert into dst values (5 / 2)", 3},
			{"projection", "insert into dst select x / 2 from src", 3},
			{"abs_wrapper", "insert into dst select abs(x / 2) from src", 3},
			{"negation_wrapper", "insert into dst select -(x / 2) from src", -3},
			{"addition_wrapper", "insert into dst select x / 2 + 0 from src", 3},
			{"left_mixed_wrapper", "insert into dst select 1 + floor(x / 2) from src", 3},
			{"right_mixed_wrapper", "insert into dst select floor(x / 2) + 1 from src", 3},
			{"integer_division_wrapper", "insert into dst select 10 div (x / 2) from src", 4},
			{"integer_division_floor_wrapper", "insert into dst select 10 div floor(x / 2) from src", 5},
			{"update_wrapper", "update dst set v = abs(5 / 2)", 3},
			{"approximate_control", "insert into dst select x / 2E0 from src", 2},
			{"folded_approximate_control", "insert into dst values (abs(5E0 / 2) + 0)", 2},
			{"explicit_float_boundary", "insert into dst select cast(x / 2 as double) from src", 2},
			{"derived_projection", "insert into dst select abs(q) from (select x / 2 as q from src) s", 3},
			{"negative_constant", "insert into dst values (-(5 / 2))", -3},
			{"coalesce_wrapper", "insert into dst select coalesce(x / 2, 0) from src", 3},
			{"if_wrapper", "insert into dst select if(true, x / 2, 0) from src", 3},
			{"ifnull_wrapper", "insert into dst select ifnull(x / 2, 0) from src", 3},
			{"nullif_wrapper", "insert into dst select nullif(x / 2, 0) from src", 3},
			{"case_wrapper", "insert into dst select case when true then x / 2 else 0 end from src", 3},
			{"round_wrapper", "insert into dst select round(x / 2, 1) from src", 3},
			{"truncate_wrapper", "insert into dst select truncate(x / 2, 1) from src", 3},
			{"greatest_wrapper", "insert into dst select greatest(x / 2, 0) from src", 3},
			{"least_wrapper", "insert into dst select least(x / 2, 3) from src", 3},
			{"aggregate_approximate_control", "insert into dst select sum(x / 2E0) from src", 2},
			{"aggregate_explicit_float_boundary", "insert into dst select cast(sum(x / 2) as double) from src", 2},
		} {
			t.Run(tc.name, func(t *testing.T) {
				mustExec(t, ctx, conn, "delete from dst")
				if tc.name == "update_wrapper" {
					mustExec(t, ctx, conn, "insert into dst values (0)")
				}
				mustExec(t, ctx, conn, tc.query)
				var got int64
				require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
				require.Equal(t, tc.want, got)
			})
		}

		t.Run("integer_division_preserves_unsigned_domain", func(t *testing.T) {
			mustExec(t, ctx, conn, "delete from dst")
			_, err := conn.ExecContext(ctx,
				"insert into dst select cast(10 as unsigned) div cast(-2 as decimal(65,0)) from src")
			require.Error(t, err)
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from dst").Scan(&count))
			require.Zero(t, count)
			mustExec(t, ctx, conn,
				"insert into dst select cast(10 as unsigned) div cast(2 as decimal(65,0)) from src")
			var got int64
			require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
			require.Equal(t, int64(5), got)
			mustExec(t, ctx, conn, "delete from dst")
		})

		t.Run("decimal_division_sibling_does_not_inherit_integer_scale", func(t *testing.T) {
			const coefficient = "123456789012345678901234567890123456789012345678901234567890"
			mustExec(t, ctx, conn, "create table mixed_assignment_dst(i int, d decimal(65,0))")
			mustExec(t, ctx, conn, "insert into mixed_assignment_dst select 1, cast('"+coefficient+
				"' as decimal(65,0))/1 from src")
			var got string
			require.NoError(t, conn.QueryRowContext(ctx,
				"select cast(d as char) from mixed_assignment_dst").Scan(&got))
			require.Equal(t, coefficient, got)
			mustExec(t, ctx, conn, "update mixed_assignment_dst set i=2,d=cast('"+coefficient+
				"' as decimal(65,0))/1 where i=1")
			require.NoError(t, conn.QueryRowContext(ctx,
				"select cast(d as char) from mixed_assignment_dst where i=2").Scan(&got))
			require.Equal(t, coefficient, got)

			mustExec(t, ctx, conn, "create table mixed_float_dst(i int, d double)")
			const floatExpr = "(1000000000000000000/1)*1000000000000000000*1000000000000000000"
			mustExec(t, ctx, conn, "insert into mixed_float_dst select 1,"+floatExpr)
			var approximate float64
			require.NoError(t, conn.QueryRowContext(ctx, "select d from mixed_float_dst").Scan(&approximate))
			require.Equal(t, 1e54, approximate)
			mustExec(t, ctx, conn, "update mixed_float_dst set i=2,d="+floatExpr+" where i=1")
			require.NoError(t, conn.QueryRowContext(ctx, "select d from mixed_float_dst where i=2").Scan(&approximate))
			require.Equal(t, 1e54, approximate)
		})

		t.Run("integer_target_does_not_change_predicate_domain", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table predicate_domain_dst(i int)")
			const predicate = "(1000000000000000000/1)*1000000000000000000*1000000000000000000>0"
			mustExec(t, ctx, conn, "insert into predicate_domain_dst select 1 where "+predicate)
			mustExec(t, ctx, conn, "update predicate_domain_dst set i=2 where "+predicate)
			mustExec(t, ctx, conn, "insert into predicate_domain_dst select 3 having "+predicate)
			mustExec(t, ctx, conn, "insert into predicate_domain_dst select (select 7/2 where "+predicate+")")
			mustExec(t, ctx, conn, "create table predicate_aggregate_src(x bigint)")
			mustExec(t, ctx, conn, "insert into predicate_aggregate_src values (5)")
			mustExec(t, ctx, conn, "insert into predicate_domain_dst select sum(x/2) from predicate_aggregate_src having sum(x/2)>0")
			rows, err := conn.QueryContext(ctx, "select i from predicate_domain_dst order by i")
			require.NoError(t, err)
			defer rows.Close()
			var got []int
			for rows.Next() {
				var value int
				require.NoError(t, rows.Scan(&value))
				got = append(got, value)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []int{2, 3, 3, 4}, got)
		})

		t.Run("integer_target_only_changes_value_dependencies", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table control_domain_src(x bigint)")
			mustExec(t, ctx, conn, "insert into control_domain_src values (1000000000000000000)")
			mustExec(t, ctx, conn, "create table control_domain_dst(i int)")
			const largeExpr = "(x/1)*x*x"
			mustExec(t, ctx, conn, "insert into control_domain_dst select 1 from control_domain_src having sum("+largeExpr+")>0")
			mustExec(t, ctx, conn, "insert into control_domain_dst select 1 from control_domain_src group by "+largeExpr)
			mustExec(t, ctx, conn, "insert into control_domain_dst select row_number() over(order by "+largeExpr+") from control_domain_src")
			mustExec(t, ctx, conn, "insert into control_domain_dst select case when "+largeExpr+">0 then 1 else 0 end from control_domain_src")
			var count, sum int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*), sum(i) from control_domain_dst").Scan(&count, &sum))
			require.Equal(t, 4, count)
			require.Equal(t, 4, sum)
		})

		t.Run("group_value_dependency_is_wrapper_and_order_independent", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table group_dependency_src(x bigint)")
			mustExec(t, ctx, conn, "insert into group_dependency_src values (5)")
			mustExec(t, ctx, conn, "create table group_dependency_a(i bigint, d double)")
			mustExec(t, ctx, conn, "insert into group_dependency_a select abs(x/2), x/2+0 from group_dependency_src group by x/2")
			mustExec(t, ctx, conn, "create table group_dependency_b(d double, i bigint)")
			mustExec(t, ctx, conn, "insert into group_dependency_b select x/2+0, abs(x/2) from group_dependency_src group by x/2")
			mustExec(t, ctx, conn, "create table group_dependency_add(i bigint)")
			mustExec(t, ctx, conn, "insert into group_dependency_add select x/2+0 from group_dependency_src group by x/2")
			for _, table := range []string{"group_dependency_a", "group_dependency_b"} {
				var integerValue int64
				var approximateValue float64
				query := "select i,d from " + table
				require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&integerValue, &approximateValue))
				require.Equal(t, int64(3), integerValue)
				require.Equal(t, 2.5, approximateValue)
			}
			var wrappedAdd int64
			require.NoError(t, conn.QueryRowContext(ctx, "select i from group_dependency_add").Scan(&wrappedAdd))
			require.Equal(t, int64(3), wrappedAdd)
		})

		t.Run("on_duplicate_key_update_keeps_case_condition_ordinary", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table ondup_control_dst(id int primary key, i int)")
			mustExec(t, ctx, conn, "insert into ondup_control_dst values (1,0)")
			const condition = "(1000000000000000000/1)*1000000000000000000*1000000000000000000>0"
			mustExec(t, ctx, conn, "insert into ondup_control_dst values (1,0) on duplicate key update i=case when "+condition+" then 1 else 0 end")
			var got int
			require.NoError(t, conn.QueryRowContext(ctx, "select i from ondup_control_dst where id=1").Scan(&got))
			require.Equal(t, 1, got)

			mustExec(t, ctx, conn, "create table ondup_float_dst(id int primary key, x bigint, i bigint)")
			mustExec(t, ctx, conn, "insert into ondup_float_dst values (1,1000000000000000000,0),(2,1000000000000000000,0)")
			const floatAssignment = "((x/1)*x*x+0E0)/1E54"
			mustExec(t, ctx, conn, "update ondup_float_dst set i="+floatAssignment+" where id=1")
			mustExec(t, ctx, conn, "insert into ondup_float_dst values (2,0,0) on duplicate key update i="+floatAssignment)
			var count, sum int64
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*),sum(i) from ondup_float_dst").Scan(&count, &sum))
			require.Equal(t, int64(2), count)
			require.Equal(t, int64(2), sum)
		})

		t.Run("group_ordinals_aliases_and_float_boundaries", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table group_scope_src(x bigint)")
			mustExec(t, ctx, conn, "insert into group_scope_src values (5)")
			mustExec(t, ctx, conn, "create table group_constant_dst(i bigint)")
			mustExec(t, ctx, conn, "insert into group_constant_dst select 5/2 from group_scope_src group by 1/2")
			var constantGroup int64
			require.NoError(t, conn.QueryRowContext(ctx, "select i from group_constant_dst").Scan(&constantGroup))
			require.Equal(t, int64(3), constantGroup)

			mustExec(t, ctx, conn, "create table group_alias_a(d double, i bigint)")
			mustExec(t, ctx, conn, "insert into group_alias_a select x/2 as q, abs(x/2) from group_scope_src group by q")
			mustExec(t, ctx, conn, "create table group_alias_b(i bigint, d double)")
			mustExec(t, ctx, conn, "insert into group_alias_b select abs(x/2), x/2 as q from group_scope_src group by q")
			for _, table := range []string{"group_alias_a", "group_alias_b"} {
				var integerValue int64
				var approximateValue float64
				require.NoError(t, conn.QueryRowContext(ctx, "select i,d from "+table).Scan(&integerValue, &approximateValue))
				require.Equal(t, int64(3), integerValue)
				require.Equal(t, 2.5, approximateValue)
			}

			mustExec(t, ctx, conn, "create table float_boundary_dst(i bigint)")
			mustExec(t, ctx, conn, "insert into float_boundary_dst select ((1000000000000000000/1e0)*1000000000000000000*1000000000000000000)/1e54")
			mustExec(t, ctx, conn, "create table float_boundary_src(f double)")
			mustExec(t, ctx, conn, "insert into float_boundary_src values (1000000000000000000)")
			mustExec(t, ctx, conn, "insert into float_boundary_dst select ((f/1)*f*f)/1e54 from float_boundary_src")
			var count, sum int64
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*),sum(i) from float_boundary_dst").Scan(&count, &sum))
			require.Equal(t, int64(2), count)
			require.Equal(t, int64(2), sum)
		})

		t.Run("prepared_strict_division_by_zero", func(t *testing.T) {
			mustExec(t, ctx, conn, "set sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO'")
			defer func() { _, _ = conn.ExecContext(ctx, "set sql_mode='STRICT_TRANS_TABLES'") }()
			for _, tc := range []struct{ name, divisor string }{
				{"literal", "0"}, {"nested", "(0/2)"}, {"floor", "floor(1/2)"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					mustExec(t, ctx, conn, "delete from dst")
					mustExec(t, ctx, conn, "prepare strict_zero from 'insert into dst values (10/"+tc.divisor+"), (5/2)'")
					_, err := conn.ExecContext(ctx, "execute strict_zero")
					require.Error(t, err)
					mustExec(t, ctx, conn, "deallocate prepare strict_zero")
					var count int
					require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from dst").Scan(&count))
					require.Zero(t, count)
				})
			}
		})

		for _, query := range []string{
			"select sum(x/2) from src",
			"select min(x/2) from src",
			"select max(x/2) from src",
			"select avg(x/2) from src",
			"select sum(x/2) over () from src",
			"select q from (select x/2 as q from src group by x/2) s",
		} {
			t.Run("exact_relational/"+query, func(t *testing.T) {
				mustExec(t, ctx, conn, "delete from dst")
				mustExec(t, ctx, conn, "insert into dst "+query)
				var got int64
				require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
				require.Equal(t, int64(3), got)
			})
		}

		t.Run("shared_projection", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table shared_dst (i bigint, f double)")
			mustExec(t, ctx, conn, "insert into shared_dst select q, q+0E0 from (select x/2 as q from src) s")
			var i int64
			var f float64
			require.NoError(t, conn.QueryRowContext(ctx, "select i,f from shared_dst").Scan(&i, &f))
			require.Equal(t, int64(3), i)
			require.Equal(t, 2.5, f)
		})
		t.Run("shared_prepared_projection", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table shared_prepared_dst (i bigint, f double)")
			stmt, err := conn.PrepareContext(ctx,
				"insert into shared_prepared_dst select q,q+0E0 from (select ?/2 as q) s")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct {
				value any
				want  int64
			}{{int64(5), 3}, {float64(5), 2}, {nil, 0}, {int64(5), 3}} {
				mustExec(t, ctx, conn, "delete from shared_prepared_dst")
				_, err = stmt.ExecContext(ctx, tc.value)
				require.NoError(t, err)
				var i sql.NullInt64
				var f sql.NullFloat64
				require.NoError(t, conn.QueryRowContext(ctx, "select i,f from shared_prepared_dst").Scan(&i, &f))
				require.Equal(t, tc.value != nil, i.Valid)
				require.Equal(t, tc.value != nil, f.Valid)
				if tc.value != nil {
					require.Equal(t, tc.want, i.Int64)
					require.Equal(t, 2.5, f.Float64)
				}
			}
		})
		t.Run("prepared_relational_domains", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table relational_dst(i bigint, f double)")
			for _, shape := range []struct{ name, source string }{
				{"projection", "(select ?/2 q) s"},
				{"predicate", "(select ?/2 q) s where q>2.4 and q<2.6"},
				{"aggregate", "(select sum(?/2) q) s"},
				{"empty_aggregate", "(select sum(?/2) q from src where false) s"},
				{"window", "(select sum(?/2) over () q) s"},
				{"window_lag", "(select lag(?/2,0) over () q) s"},
				{"window_first", "(select first_value(?/2) over () q) s"},
				{"group", "(select q from (select ?/2 q) s group by q) g"},
			} {
				for _, protocol := range []string{"sql", "binary"} {
					t.Run(shape.name+"/"+protocol, func(t *testing.T) {
						query := "insert into relational_dst select q,q+0E0 from " + shape.source
						var stmt *sql.Stmt
						if protocol == "binary" {
							stmt, err = conn.PrepareContext(ctx, query)
							require.NoError(t, err)
							defer stmt.Close()
						} else {
							mustExec(t, ctx, conn, "prepare relational_p from '"+query+"'")
							defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare relational_p") }()
						}
						for _, tc := range []struct {
							approximate bool
							want        int64
						}{{false, 3}, {true, 2}, {false, 3}} {
							mustExec(t, ctx, conn, "delete from relational_dst")
							if protocol == "binary" {
								var value any = int64(5)
								if tc.approximate {
									value = float64(5)
								}
								_, err = stmt.ExecContext(ctx, value)
								require.NoError(t, err)
							} else {
								value := "5"
								if tc.approximate {
									value = "5E0"
								}
								mustExec(t, ctx, conn, "set @relational_v="+value)
								mustExec(t, ctx, conn, "execute relational_p using @relational_v")
							}
							var i sql.NullInt64
							var f sql.NullFloat64
							require.NoError(t, conn.QueryRowContext(ctx, "select i,f from relational_dst").Scan(&i, &f))
							require.Equal(t, shape.name != "empty_aggregate", i.Valid)
							require.Equal(t, shape.name != "empty_aggregate", f.Valid)
							if shape.name != "empty_aggregate" {
								require.Equal(t, tc.want, i.Int64)
								require.Equal(t, 2.5, f.Float64)
							}
						}
					})
				}
			}
		})
		t.Run("prepared_source_preserves_no_unsigned_subtraction", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table no_unsigned_dst(i bigint, f double)")
			mustExec(t, ctx, conn, "set sql_mode='STRICT_TRANS_TABLES,NO_UNSIGNED_SUBTRACTION'")
			defer func() { _, _ = conn.ExecContext(ctx, "set sql_mode='STRICT_TRANS_TABLES'") }()
			query := "insert into no_unsigned_dst select q,q+0E0 from (select ?/2 q) s where ?-cast(2 as unsigned)<0"
			for _, protocol := range []string{"sql", "binary"} {
				t.Run(protocol, func(t *testing.T) {
					mustExec(t, ctx, conn, "delete from no_unsigned_dst")
					if protocol == "binary" {
						stmt, err := conn.PrepareContext(ctx, query)
						require.NoError(t, err)
						defer stmt.Close()
						_, err = stmt.ExecContext(ctx, int64(5), int64(1))
						require.NoError(t, err)
					} else {
						mustExec(t, ctx, conn, "prepare unsigned_subtraction_p from '"+query+"'")
						defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare unsigned_subtraction_p") }()
						mustExec(t, ctx, conn, "set @unsigned_x=5,@unsigned_y=1")
						mustExec(t, ctx, conn, "execute unsigned_subtraction_p using @unsigned_x,@unsigned_y")
					}
					var i int64
					var f float64
					require.NoError(t, conn.QueryRowContext(ctx, "select i,f from no_unsigned_dst").Scan(&i, &f))
					require.Equal(t, int64(3), i)
					require.Equal(t, 2.5, f)
				})
			}
		})
		t.Run("prepared_integer_division_rebinds_complete_root", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table prepared_div_dst(v bigint)")
			for _, wrapper := range []struct{ name, expression string }{
				{"direct", "10 div (?/2)"}, {"abs", "abs(10 div (?/2))"},
			} {
				for _, protocol := range []string{"sql", "binary"} {
					t.Run(wrapper.name+"/"+protocol, func(t *testing.T) {
						query := "insert into prepared_div_dst values (" + wrapper.expression + ")"
						if protocol == "binary" {
							stmt, err := conn.PrepareContext(ctx, query)
							require.NoError(t, err)
							defer stmt.Close()
							for _, value := range []any{float64(5), "5", float64(5)} {
								mustExec(t, ctx, conn, "delete from prepared_div_dst")
								_, err = stmt.ExecContext(ctx, value)
								require.NoError(t, err)
								var got int64
								require.NoError(t, conn.QueryRowContext(ctx, "select v from prepared_div_dst").Scan(&got))
								require.Equal(t, int64(4), got)
							}
						} else {
							mustExec(t, ctx, conn, "prepare div_root_p from '"+query+"'")
							defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare div_root_p") }()
							for _, value := range []string{"5E0", "'5'", "5E0"} {
								mustExec(t, ctx, conn, "delete from prepared_div_dst")
								mustExec(t, ctx, conn, "set @div_root_value="+value)
								mustExec(t, ctx, conn, "execute div_root_p using @div_root_value")
								var got int64
								require.NoError(t, conn.QueryRowContext(ctx, "select v from prepared_div_dst").Scan(&got))
								require.Equal(t, int64(4), got)
							}
						}
					})
				}
			}
		})

		t.Run("prepared_source_preserves_other_assignments", func(t *testing.T) {
			rt := moruntime.ServiceRuntime(cn.GetServiceConfig().CN.UUID)
			oldVersion, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			require.True(t, exists)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
			defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
			mustExec(t, ctx, conn, "create table mixed_policy(i bigint, u bigint unsigned)")
			stmt, err := conn.PrepareContext(ctx, "insert into mixed_policy select q,? from (select ?/2 q) s limit ?")
			require.NoError(t, err)
			defer stmt.Close()
			mustExec(t, ctx, conn, "set sql_mode=''")
			defer func() { _, _ = conn.ExecContext(ctx, "set sql_mode='STRICT_TRANS_TABLES'") }()
			_, err = stmt.ExecContext(ctx, float64(-1), float64(5), uint64(1))
			require.NoError(t, err)
			var i int64
			var u uint64
			require.NoError(t, conn.QueryRowContext(ctx, "select i,u from mixed_policy").Scan(&i, &u))
			require.Equal(t, int64(2), i)
			require.Zero(t, u)
			mustExec(t, ctx, conn, "delete from mixed_policy")
			mustExec(t, ctx, conn, "set sql_mode='STRICT_TRANS_TABLES'")
			_, err = stmt.ExecContext(ctx, float64(-1), int64(5), uint64(1))
			require.Error(t, err)
			var count int64
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mixed_policy").Scan(&count))
			require.Zero(t, count)
		})
		t.Run("shared_predicate", func(t *testing.T) {
			mustExec(t, ctx, conn, "delete from dst")
			mustExec(t, ctx, conn, "insert into dst select q from (select x/2 as q from src) s where q>2.6")
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from dst").Scan(&count))
			require.Zero(t, count)
		})
		for _, protocol := range []string{"sql_prepare", "com_stmt"} {
			t.Run(protocol+"_integer_division", func(t *testing.T) {
				mustExec(t, ctx, conn, "delete from dst")
				if protocol == "sql_prepare" {
					mustExec(t, ctx, conn, "prepare exact_p from 'insert into dst values (?/2)'")
					defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare exact_p") }()
					for _, tc := range []struct {
						value string
						want  int64
					}{{"5", 3}, {"cast(5 as double)", 2}, {"7", 4}, {"5", 3}} {
						mustExec(t, ctx, conn, "delete from dst")
						mustExec(t, ctx, conn, "set @exact_v="+tc.value)
						mustExec(t, ctx, conn, "execute exact_p using @exact_v")
						var got int64
						require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
						require.Equal(t, tc.want, got)
					}
				} else {
					stmt, err := conn.PrepareContext(ctx, "insert into dst values (?/2)")
					require.NoError(t, err)
					defer stmt.Close()
					for _, tc := range []struct {
						value any
						want  int64
					}{{int64(5), 3}, {float64(5), 2}, {int64(7), 4}, {int64(5), 3}} {
						mustExec(t, ctx, conn, "delete from dst")
						_, err = stmt.ExecContext(ctx, tc.value)
						require.NoError(t, err)
						var got int64
						require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
						require.Equal(t, tc.want, got)
					}
				}
				var got int64
				require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
				require.Equal(t, int64(3), got)
			})
		}

		t.Run("float_target_preserves_fraction", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table float_dst (v double)")
			mustExec(t, ctx, conn, "insert into float_dst select x / 2 from src")
			var got float64
			require.NoError(t, conn.QueryRowContext(ctx, "select v from float_dst").Scan(&got))
			require.Equal(t, 2.5, got)
		})

		t.Run("large_exact_division", func(t *testing.T) {
			mustExec(t, ctx, conn, "delete from dst")
			mustExec(t, ctx, conn, "insert into dst values (9007199254740993 / 2)")
			var got int64
			require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
			require.Equal(t, int64(4503599627370497), got)
			mustExec(t, ctx, conn, "delete from dst")
			mustExec(t, ctx, conn, "insert into dst values (9223372036854775807 / 1)")
			require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
			require.Equal(t, int64(9223372036854775807), got)

			mustExec(t, ctx, conn, "create table large_src (x bigint)")
			mustExec(t, ctx, conn, "insert into large_src values (9007199254740993)")
			mustExec(t, ctx, conn, "delete from dst")
			mustExec(t, ctx, conn, "insert into dst select x / 2 from large_src")
			require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
			require.Equal(t, int64(4503599627370497), got)

			mustExec(t, ctx, conn, "delete from large_src")
			mustExec(t, ctx, conn, "insert into large_src values (9223372036854775807)")
			mustExec(t, ctx, conn, "delete from dst")
			mustExec(t, ctx, conn, "insert into dst select x / 1 from large_src")
			require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
			require.Equal(t, int64(9223372036854775807), got)
			mustExec(t, ctx, conn, "delete from dst")
			mustExec(t, ctx, conn, "insert into dst select coalesce(x / 1, 0) from large_src")
			require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
			require.Equal(t, int64(9223372036854775807), got)
			for _, expression := range []string{"sum(x/1)", "min(x/1)", "max(x/1)", "avg(x/1)", "sum(x/1) over ()", "x/cast(1 as decimal(38,37))"} {
				mustExec(t, ctx, conn, "delete from dst")
				mustExec(t, ctx, conn, "insert into dst select "+expression+" from large_src")
				require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
				require.Equal(t, int64(9223372036854775807), got, expression)
			}
		})

		t.Run("division_unique_key", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table quotient_source (result bigint)")
			mustExec(t, ctx, conn, "insert into quotient_source values (150), (250)")
			mustExec(t, ctx, conn, "create table quotient_target (a bigint, b bigint, primary key(a,b))")
			mustExec(t, ctx, conn, "insert into quotient_target select result/100, result%100 from quotient_source")
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from quotient_target where (a=2 or a=3) and b=50").Scan(&count))
			require.Equal(t, 2, count)
		})

		t.Run("prepared_approximate_division", func(t *testing.T) {
			mustExec(t, ctx, conn, "delete from dst")
			stmt, err := conn.PrepareContext(ctx, "insert into dst values (? / 2)")
			require.NoError(t, err)
			defer stmt.Close()
			_, err = stmt.ExecContext(ctx, float64(5))
			require.NoError(t, err)
			var got int64
			require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
			require.Equal(t, int64(2), got)
		})

		mustExec(t, ctx, conn, "create table ignore_unsigned (id int primary key, v bigint unsigned)")
		for _, protocol := range []string{"sql_prepare", "com_stmt"} {
			t.Run(protocol+"_ignore_latest", func(t *testing.T) {
				rt := moruntime.ServiceRuntime(cn.GetServiceConfig().CN.UUID)
				old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
				require.True(t, exists)
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
				defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
				mustExec(t, ctx, conn, "delete from ignore_unsigned")
				if protocol == "sql_prepare" {
					mustExec(t, ctx, conn, "prepare ignore_p from 'insert ignore into ignore_unsigned values (?, ?)'")
					defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare ignore_p") }()
					mustExec(t, ctx, conn, "set @ignore_id=1, @ignore_v=cast(-1 as double)")
					mustExec(t, ctx, conn, "execute ignore_p using @ignore_id, @ignore_v")
					mustExec(t, ctx, conn, "set @ignore_id=2, @ignore_v=cast(3.5 as double)")
					mustExec(t, ctx, conn, "execute ignore_p using @ignore_id, @ignore_v")
				} else {
					stmt, err := conn.PrepareContext(ctx, "insert ignore into ignore_unsigned values (?, ?)")
					require.NoError(t, err)
					defer stmt.Close()
					_, err = stmt.ExecContext(ctx, 1, float64(-1))
					require.NoError(t, err)
					_, err = stmt.ExecContext(ctx, 2, float64(3.5))
					require.NoError(t, err)
				}
				var got uint64
				require.NoError(t, conn.QueryRowContext(ctx, "select v from ignore_unsigned where id=1").Scan(&got))
				require.Zero(t, got)
				require.NoError(t, conn.QueryRowContext(ctx, "select v from ignore_unsigned where id=2").Scan(&got))
				require.Equal(t, uint64(4), got)
			})
		}

		mustExec(t, ctx, conn, "create table t_abs (value bigint)")
		mustExec(t, ctx, conn, "create table t_add (value bigint)")
		for _, tc := range []struct {
			query string
			value int64
		}{
			{query: "insert into t_abs values (abs(?))", value: 2},
			{query: "insert into t_add values (? + 0)", value: -2},
		} {
			stmt, prepareErr := conn.PrepareContext(ctx, tc.query)
			require.NoError(t, prepareErr)
			defer stmt.Close()
			_, execErr := stmt.ExecContext(ctx, float64(-2.5))
			require.NoError(t, execErr)
		}
		var value int64
		require.NoError(t, conn.QueryRowContext(ctx, "select value from t_abs").Scan(&value))
		require.Equal(t, int64(2), value)
		require.NoError(t, conn.QueryRowContext(ctx, "select value from t_add").Scan(&value))
		require.Equal(t, int64(-2), value)
		mustExec(t, ctx, conn, "delete from t_add")
		mustExec(t, ctx, conn, "prepare nested_add_sql from 'insert into t_add values (? + 0)'")
		mustExec(t, ctx, conn, "set @nested_add_value=cast(-2.5 as double)")
		mustExec(t, ctx, conn, "execute nested_add_sql using @nested_add_value")
		mustExec(t, ctx, conn, "deallocate prepare nested_add_sql")
		require.NoError(t, conn.QueryRowContext(ctx, "select value from t_add").Scan(&value))
		require.Equal(t, int64(-2), value)

		rt := moruntime.ServiceRuntime(cn.GetServiceConfig().CN.UUID)
		oldVersion, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
		require.True(t, exists)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)

		mustExec(t, ctx, conn, "create table u (value bigint unsigned)")
		mustExec(t, ctx, conn, "set sql_mode=''")
		mustExec(t, ctx, conn, "insert into u values (-1E0)")
		var unsignedValue uint64
		require.NoError(t, conn.QueryRowContext(ctx, "select value from u").Scan(&unsignedValue))
		require.Zero(t, unsignedValue)
		mustExec(t, ctx, conn, "delete from u")
		mustExec(t, ctx, conn, "insert into u values (-5/2)")
		require.NoError(t, conn.QueryRowContext(ctx, "select value from u").Scan(&unsignedValue))
		require.Zero(t, unsignedValue)
		mustExec(t, ctx, conn, "delete from u")

		assignmentStmt, err := conn.PrepareContext(ctx, "insert into u values (?)")
		require.NoError(t, err)
		defer assignmentStmt.Close()
		_, err = assignmentStmt.ExecContext(ctx, float64(-1))
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, "select value from u").Scan(&unsignedValue))
		require.Zero(t, unsignedValue)
		mustExec(t, ctx, conn, "delete from u")

		mustExec(t, ctx, conn, "set sql_mode='STRICT_TRANS_TABLES'")
		_, err = assignmentStmt.ExecContext(ctx, float64(-1))
		require.Error(t, err)
		var count int64
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from u").Scan(&count))
		require.Zero(t, count)
		t.Run("ordinary_select_after_integer_dml", checkOrdinarySelect)
	})
}
