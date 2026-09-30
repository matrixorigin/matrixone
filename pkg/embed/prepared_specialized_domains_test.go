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

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
		t.Run("unassigned_variable_alias", func(t *testing.T) {
			exec(t, "set @alias_of_unassigned = @never_assigned")
			require.Equal(t, [][]string{{"NULL"}}, query(t, "select @alias_of_unassigned"))
		})
		t.Run("numeric_aggregate_source_transitions", func(t *testing.T) {
			exec(t, "prepare numeric_sum from 'select cast(sum(?) as signed)'")
			defer conn.ExecContext(ctx, "deallocate prepare numeric_sum")
			for _, tc := range []struct{ assignment, want string }{
				{"2", "2"}, {"'2'", "2"}, {"null", "NULL"}, {"'abc'", "0"}, {"3", "3"},
			} {
				exec(t, "set @numeric_source = "+tc.assignment)
				require.Equal(t, [][]string{{tc.want}}, query(t, "execute numeric_sum using @numeric_source"), tc.assignment)
			}
		})
		t.Run("prepared_round_truncate_value_domains", func(t *testing.T) {
			for _, tc := range []struct {
				name string
				fn   string
			}{
				{name: "round", fn: "round"},
				{name: "truncate", fn: "truncate"},
			} {
				fractionalResult := "1.4"
				if tc.fn == "round" {
					fractionalResult = "1.5"
				}
				t.Run(tc.name+"/sql_execute", func(t *testing.T) {
					stmtName := "numeric_" + tc.name
					exec(t, "prepare "+stmtName+" from 'select "+tc.fn+"(?,?)'")
					defer conn.ExecContext(ctx, "deallocate prepare "+stmtName)
					for _, value := range []struct {
						assignment string
						precision  string
						want       string
					}{
						{"'1.46'", "1", fractionalResult + "0"},
						{"cast(1.46 as decimal(10,2))", "1", fractionalResult + "0"},
						{"2", "0", "2"},
						{"null", "1", "NULL"},
					} {
						exec(t, "set @numeric_value="+value.assignment+", @numeric_precision="+value.precision)
						require.Equal(t, [][]string{{value.want}}, query(t,
							"execute "+stmtName+" using @numeric_value,@numeric_precision"), value)
					}
					exec(t, "set @numeric_value='not-a-number', @numeric_precision=1")
					err := func() error {
						rows, err := conn.QueryContext(ctx,
							"execute "+stmtName+" using @numeric_value,@numeric_precision")
						if rows == nil {
							return err
						}
						defer rows.Close()
						for rows.Next() {
						}
						return rows.Err()
					}()
					require.Error(t, err, "invalid text should fail as on the GOOD baseline")
					exec(t, "set @numeric_value='1.46'")
					require.Equal(t, [][]string{{fractionalResult + "0"}},
						query(t, "execute "+stmtName+" using @numeric_value,@numeric_precision"),
						"a failed execution must not poison the next binding")
				})

				t.Run(tc.name+"/binary_protocol", func(t *testing.T) {
					stmt, err := conn.PrepareContext(ctx, "select "+tc.fn+"(?,?)")
					require.NoError(t, err)
					defer stmt.Close()
					for _, value := range []struct {
						input any
						want  string
					}{
						{"1.46", fractionalResult + "0"},
						{int64(2), "2"},
						{float64(1.46), fractionalResult},
						{[]byte("1.46"), fractionalResult + "0"},
						{nil, "NULL"},
					} {
						var got sql.NullString
						require.NoError(t, stmt.QueryRowContext(ctx, value.input, 1).Scan(&got), value)
						gotString := "NULL"
						if got.Valid {
							gotString = got.String
						}
						require.Equal(t, value.want, gotString, value)
					}
				})
				for _, shape := range []struct {
					name  string
					value string
				}{
					{"scalar", "(select ?)"},
					{"derived", "x"},
				} {
					t.Run(tc.name+"/"+shape.name, func(t *testing.T) {
						statement := "select cast(" + tc.fn + "(" + shape.value + ",1) as double)"
						if shape.name == "derived" {
							statement += " from (select ? x limit 1) d"
						}
						stmt, err := conn.PrepareContext(ctx, statement)
						require.NoError(t, err)
						defer stmt.Close()
						var got sql.NullString
						require.NoError(t, stmt.QueryRowContext(ctx, "1.46").Scan(&got))
						require.Equal(t, fractionalResult, got.String)
						tieStatement := "select cast(" + tc.fn + "(" + shape.value + ",0) as double)"
						if shape.name == "derived" {
							tieStatement += " from (select ? x limit 1) d"
						}
						tie, tieErr := conn.PrepareContext(ctx, tieStatement)
						require.NoError(t, tieErr)
						defer tie.Close()
						require.NoError(t, tie.QueryRowContext(ctx, "2.5").Scan(&got))
						if tc.name == "round" {
							require.Equal(t, "3", got.String)
						} else {
							require.Equal(t, "2", got.String)
						}
						if shape.name == "derived" {
							require.Error(t, stmt.QueryRowContext(ctx, "not-a-number").Scan(&got))
						}
						require.NoError(t, stmt.QueryRowContext(ctx, "1.46").Scan(&got))
						require.Equal(t, fractionalResult, got.String)
					})
				}
				t.Run(tc.name+"/set_operation_domain", func(t *testing.T) {
					stmt, err := conn.PrepareContext(ctx,
						"select cast("+tc.fn+"(x,1) as double) from "+
							"(select ? x union all select cast(1.46 as decimal(10,2))) d")
					require.NoError(t, err)
					defer stmt.Close()
					rows, err := stmt.QueryContext(ctx, int64(1))
					require.NoError(t, err)
					defer rows.Close()
					var got []string
					for rows.Next() {
						var value string
						require.NoError(t, rows.Scan(&value))
						got = append(got, value)
					}
					require.NoError(t, rows.Err())
					fraction := "1.5"
					if tc.name == "truncate" {
						fraction = "1.4"
					}
					require.ElementsMatch(t, []string{"1", fraction}, got)
				})
			}

			exec(t, "prepare explicit_decimal_round from 'select round(cast(? as decimal(10,2)),1)'")
			defer conn.ExecContext(ctx, "deallocate prepare explicit_decimal_round")
			exec(t, "set @explicit_decimal='1.46'")
			require.Equal(t, [][]string{{"1.5"}}, query(t,
				"execute explicit_decimal_round using @explicit_decimal"))
			explicitDerived, err := conn.PrepareContext(ctx,
				"select cast(round(x,0) as double) from (select cast(? as decimal(10,2)) x) d")
			require.NoError(t, err)
			defer explicitDerived.Close()
			var explicitGot sql.NullString
			require.NoError(t, explicitDerived.QueryRowContext(ctx, "2.5").Scan(&explicitGot))
			require.Equal(t, "3", explicitGot.String)
		})
		t.Run("prepared_round_filter_domains", func(t *testing.T) {
			exec(t, "create table rounding_filters(id bigint)")
			defer conn.ExecContext(ctx, "drop table rounding_filters")
			exec(t, "insert into rounding_filters values (null),(-54322),(-54321),(0),(54320),(54321),(54322),"+
				"(9007199254740991),(9007199254740992),(9007199254740993)")
			for _, predicate := range []string{
				"id=round(?)", "id=round((select ?))",
				"id<round((select ?))", "id<=round((select ?),0)",
				"id>round((select ?),0)", "id>=round((select ?),0)",
				"round((select ?))>id", "id<>round((select ?))", "id<=>round((select ?))",
				"id=truncate(?)", "id<truncate((select ?))", "id>=truncate((select ?),0)",
			} {
				t.Run(predicate, func(t *testing.T) {
					exec(t, "prepare rounding_filter from 'select id from rounding_filters where "+predicate+" order by id'")
					defer conn.ExecContext(ctx, "deallocate prepare rounding_filter")
					// Keep the pre-rewrite exact DECIMAL column domain executable as
					// an independent oracle, including BIGINTs beyond 2^53.
					control := strings.ReplaceAll(predicate, "id", "cast(id as decimal(38,0))")
					exec(t, "prepare rounding_control from 'select id from rounding_filters where "+control+" order by id'")
					defer conn.ExecContext(ctx, "deallocate prepare rounding_control")
					for _, value := range []string{"'54321.0'", "'54321.5'", "'9007199254740992'", "null", "'-54321.0'", "'54321.0'"} {
						exec(t, "set @rounding_filter_source="+value)
						require.Equal(t, query(t, "execute rounding_control using @rounding_filter_source"),
							query(t, "execute rounding_filter using @rounding_filter_source"), value)
					}
				})
			}
		})
		t.Run("prepared_round_reversed_primary_key_ranges", func(t *testing.T) {
			exec(t, "create table rounding_keys(id bigint primary key)")
			defer conn.ExecContext(ctx, "drop table rounding_keys")
			exec(t, "insert into rounding_keys values (54320),(54321),(54322)")
			exec(t, "set @rounding_key_source='54321.0'")
			for _, fn := range []string{"round", "truncate"} {
				for _, source := range []string{"?", "(select ?)"} {
					for _, tc := range []struct {
						op   string
						want [][]string
					}{{"<", [][]string{{"54322"}}}, {"<=", [][]string{{"54321"}, {"54322"}}},
						{">", [][]string{{"54320"}}}, {">=", [][]string{{"54320"}, {"54321"}}}} {
						exec(t, "prepare rounding_key from 'select id from rounding_keys where "+fn+"("+source+")"+tc.op+"id order by id'")
						require.Equal(t, tc.want, query(t, "execute rounding_key using @rounding_key_source"), fn, source, tc.op)
						exec(t, "deallocate prepare rounding_key")
					}
				}
			}
		})
		t.Run("ntile_null_runtime_error", func(t *testing.T) {
			exec(t, "create table ntile_source(id int)")
			exec(t, "insert into ntile_source values (1),(2)")
			exec(t, "prepare ntile_buckets from 'select ntile(?) over (order by id) from ntile_source'")
			defer conn.ExecContext(ctx, "deallocate prepare ntile_buckets")
			exec(t, "set @buckets = 2")
			require.Equal(t, [][]string{{"1"}, {"2"}}, query(t, "execute ntile_buckets using @buckets"))
			exec(t, "set @buckets = null")
			rows, err := conn.QueryContext(ctx, "execute ntile_buckets using @buckets")
			if rows != nil {
				defer rows.Close()
				for rows.Next() {
				}
				err = rows.Err()
			}
			require.ErrorContains(t, err, "ntile bucket count cannot be NULL")
		})
		t.Run("regexp_scalar_mixed_domain_reuse", func(t *testing.T) {
			exec(t, "create table regexp_source(id int)")
			exec(t, "insert into regexp_source values (1)")
			exec(t, `prepare regexp_scalar from 'select regexp_instr(
				(select ? from regexp_source limit 1),
				(select ? from regexp_source limit 1), 2)'`)
			defer conn.ExecContext(ctx, "deallocate prepare regexp_scalar")
			exec(t, "set @subject=cast(_binary'中中' as varbinary(6)), @pattern=cast(_binary'中' as varbinary(3))")
			require.Equal(t, [][]string{{"4"}}, query(t, "execute regexp_scalar using @subject,@pattern"))
			exec(t, "set @subject='中中', @pattern='中'")
			require.Equal(t, [][]string{{"2"}}, query(t, "execute regexp_scalar using @subject,@pattern"))
			exec(t, "set @subject=cast(_binary'中中' as varbinary(6)), @pattern='中'")
			require.Equal(t, [][]string{{"4"}}, query(t, "execute regexp_scalar using @subject,@pattern"))
			exec(t, `prepare regexp_derived from 'select regexp_instr(
				(select d.subject from (select ? as subject) d),
				(select d.pattern from (select ? as pattern) d), 2)'`)
			defer conn.ExecContext(ctx, "deallocate prepare regexp_derived")
			exec(t, "set @subject=cast(_binary'中中' as varbinary(6)), @pattern=cast(_binary'中' as varbinary(3))")
			require.Equal(t, [][]string{{"4"}}, query(t, "execute regexp_derived using @subject,@pattern"))
			exec(t, "set @subject='中中', @pattern='中'")
			require.Equal(t, [][]string{{"2"}}, query(t, "execute regexp_derived using @subject,@pattern"))
		})
		t.Run("date_interval_scale_reuse", func(t *testing.T) {
			exec(t, "prepare date_interval from 'select date_add(''2026-01-01'', interval ? second)'")
			defer conn.ExecContext(ctx, "deallocate prepare date_interval")
			for _, tc := range []struct{ assignment, literal, want string }{
				{"3", "3", ""},
				{"'4'", "", "2026-01-01 00:00:04.000000"},
				{"null", "", "NULL"},
				{"5", "5", ""},
			} {
				exec(t, "set @interval_value = "+tc.assignment)
				want := [][]string{{tc.want}}
				if tc.literal != "" {
					want = query(t, "select date_add('2026-01-01', interval "+tc.literal+" second)")
				}
				require.Equal(t, want, query(t, "execute date_interval using @interval_value"), tc.assignment)
			}
		})
		t.Run("nested_decimal_common_value", func(t *testing.T) {
			exec(t, "create table decimal_peer(id int primary key, d decimal(38,10))")
			exec(t, `insert into decimal_peer values
				(1,9007199254740992.0000000001),
				(2,9007199254740992.0000000002),
				(3,9007199254740992.0000000003)`)
			exec(t, "prepare decimal_nested from 'select id from decimal_peer where greatest(?,coalesce(?,d))=d order by id'")
			defer conn.ExecContext(ctx, "deallocate prepare decimal_nested")
			for _, tc := range []struct {
				assignment string
				want       [][]string
			}{
				{"'9007199254740992.0000000002'", [][]string{{"2"}}},
				{"null", nil},
				{"'9007199254740992.0000000003'", [][]string{{"3"}}},
			} {
				exec(t, "set @decimal_source = "+tc.assignment)
				require.Equal(t, tc.want, query(t, "execute decimal_nested using @decimal_source,@decimal_source"), tc.assignment)
			}
			exec(t, "set @decimal_source = '9007199254740992.0000000002'")
			for _, expr := range []string{
				"greatest(?,abs(coalesce(?,d)))",
				"coalesce(?,abs(coalesce(?,d)))",
				"greatest(?,coalesce(?,d)+0)",
				"greatest(?,round(coalesce(?,d),10))",
			} {
				exec(t, "prepare decimal_wrapped from 'select id from decimal_peer where "+expr+"=d order by id'")
				require.Equal(t, [][]string{{"2"}}, query(t, "execute decimal_wrapped using @decimal_source,@decimal_source"), expr)
				exec(t, "deallocate prepare decimal_wrapped")
			}
		})
		t.Run("text_assignment_bool_equivalence", func(t *testing.T) {
			exec(t, "create table bool_text(id int, v text)")
			exec(t, "insert into bool_text values(1,true)")
			exec(t, "prepare insert_bool_text from 'insert into bool_text values(2,?)'")
			defer conn.ExecContext(ctx, "deallocate prepare insert_bool_text")
			exec(t, "set @bool_text_source = true")
			exec(t, "execute insert_bool_text using @bool_text_source")
			got := query(t, "select v from bool_text order by id")
			require.Len(t, got, 2)
			require.Equal(t, got[0], got[1])
		})
		t.Run("float_literal_membership", func(t *testing.T) {
			exec(t, "create table float_source(a float(3))")
			exec(t, "insert into float_source values(1),(0.00),(0.8)")
			require.Equal(t, [][]string{{"0.8"}}, query(t,
				"select a from float_source where a in (0.8,0.9)"))
			exec(t, "create table float_range(id float, b int)")
			exec(t, "insert into float_range values(4.574,1),(5.3111,2),(177.171,3)")
			require.Equal(t, [][]string{{"2"}}, query(t,
				"select count(*) from float_range where id>=5.3111 and id<=177.171"))
			for _, rhs := range []string{"9.0", "cast(9.0 as decimal)", "abs(-9)"} {
				plan := query(t, "explain select * from float_source where a = "+rhs)
				require.NotContains(t, fmt.Sprint(plan), "cast(float_source.a AS DOUBLE)", rhs)
			}
		})
		t.Run("ordinary_string_integer_comparison", func(t *testing.T) {
			exec(t, "create table comparison_source(v char(1), w varchar(20))")
			exec(t, "insert into comparison_source values('a','abc')")
			for _, statement := range []string{
				"select case v when 1 then 'yes' else 'no' end from comparison_source",
				"select (v,w)<(v,0) from comparison_source",
			} {
				err := func() error {
					rows, err := conn.QueryContext(ctx, statement)
					if rows == nil {
						return err
					}
					defer rows.Close()
					for rows.Next() {
					}
					return rows.Err()
				}()
				require.ErrorContains(t, err, "invalid argument cast to int", statement)
			}
		})
		t.Run("prepared_numeric_text_range", func(t *testing.T) {
			exec(t, "create table range_source(v varchar(20))")
			exec(t, "insert into range_source values('02'),('2'),('invalid')")
			exec(t, "set @low=2,@high=3")
			for _, tc := range []struct{ name, predicate, want string }{
				{"between_numeric_bounds", "v between ? and ?", "2"},
				{"between_text_lower", "? between v and ?", "3"},
				{"not_between", "v not between ? and ?", "1"},
			} {
				exec(t, "prepare "+tc.name+" from 'select count(*) from range_source where "+tc.predicate+"'")
				require.Equal(t, [][]string{{tc.want}}, query(t, "execute "+tc.name+" using @low,@high"), tc.name)
				exec(t, "deallocate prepare "+tc.name)
			}
		})
		t.Run("projected_text_numeric_comparison", func(t *testing.T) {
			exec(t, "create table projected_comparison(k bigint primary key)")
			exec(t, "insert into projected_comparison values(1),(2)")
			for _, shape := range []struct{ name, predicate string }{
				{"direct", "k=?"},
				{"scalar", "k=(select ?)"},
			} {
				statement := "select count(*) from projected_comparison where " + shape.predicate
				for _, binary := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/binary=%t", shape.name, binary), func(t *testing.T) {
						var stmt *sql.Stmt
						if binary {
							stmt, err = conn.PrepareContext(ctx, statement)
							require.NoError(t, err)
							defer stmt.Close()
						} else {
							exec(t, "prepare projected_comparison_stmt from '"+statement+"'")
							defer conn.ExecContext(ctx, "deallocate prepare projected_comparison_stmt")
						}
						for _, value := range []struct {
							assignment string
							binary     any
							want       int
						}{
							{"'1.5'", "1.5", 0},
							{"'1'", "1", 1},
							{"null", nil, 0},
							{"'1.5'", "1.5", 0},
							{"'2'", "2", 1},
						} {
							var got int
							if binary {
								require.NoError(t, stmt.QueryRowContext(ctx, value.binary).Scan(&got), value)
							} else {
								exec(t, "set @projected_comparison_value="+value.assignment)
								require.Equal(t, [][]string{{fmt.Sprint(value.want)}},
									query(t, "execute projected_comparison_stmt using @projected_comparison_value"), value)
								continue
							}
							require.Equal(t, value.want, got, value)
						}
					})
				}
			}
			exec(t, "create table projected_large_comparison(k bigint primary key)")
			exec(t, "insert into projected_large_comparison values(9007199254740992),(9007199254740993)")
			require.Equal(t, [][]string{{"1"}}, query(t,
				"select count(*) from projected_large_comparison where k=(select '9007199254740993')"))
			exec(t, "prepare projected_large_stmt from 'select count(*) from projected_large_comparison where k=(select ?)' ")
			defer conn.ExecContext(ctx, "deallocate prepare projected_large_stmt")
			exec(t, "set @projected_large_value='9007199254740993'")
			require.Equal(t, [][]string{{"1"}}, query(t, "execute projected_large_stmt using @projected_large_value"))
			stmt, err := conn.PrepareContext(ctx,
				"select count(*) from projected_large_comparison where k=(select ?)")
			require.NoError(t, err)
			defer stmt.Close()
			var got int
			require.NoError(t, stmt.QueryRowContext(ctx, "9007199254740993").Scan(&got))
			require.Equal(t, 1, got)
		})
		t.Run("between_decimal_precision", func(t *testing.T) {
			exec(t, `prepare decimal_between from
				'select cast(''9007199254740992.0000000002'' as decimal(38,10))
				between ? and ''9007199254740992.0000000001'''`)
			defer conn.ExecContext(ctx, "deallocate prepare decimal_between")
			exec(t, "set @decimal_lower=0")
			require.Equal(t, [][]string{{"0"}}, query(t, "execute decimal_between using @decimal_lower"))
			exec(t, `prepare decimal_text_left from
				'select ''9007199254740992.0000000002'' between ? and
				cast(''9007199254740992.0000000001'' as decimal(38,10))'`)
			defer conn.ExecContext(ctx, "deallocate prepare decimal_text_left")
			exec(t, "set @decimal_lower=20")
			require.Equal(t, [][]string{{"0"}}, query(t, "execute decimal_text_left using @decimal_lower"))
			exec(t, "prepare lexical_between from 'select ''10'' between ''2'' and ?'")
			defer conn.ExecContext(ctx, "deallocate prepare lexical_between")
			exec(t, "set @text_upper='20'")
			require.Equal(t, [][]string{{"0"}}, query(t, "execute lexical_between using @text_upper"))
			for _, peer := range []string{"signed", "unsigned"} {
				exec(t, "prepare integer_bound from 'select ''9007199254740993'' between ? and cast(9007199254740992 as "+peer+")'")
				require.Equal(t, [][]string{{"0"}}, query(t, "execute integer_bound using @decimal_lower"), peer)
				exec(t, "deallocate prepare integer_bound")
			}
		})
		t.Run("between_exact_unsigned_marker", func(t *testing.T) {
			exec(t, "prepare unsigned_between from 'select ? between ? and ?'")
			defer conn.ExecContext(ctx, "deallocate prepare unsigned_between")
			exec(t, "set @text_value='9007199254740993', @lower_bound=20, @upper_bound=cast(9007199254740992 as unsigned)")
			require.Equal(t, [][]string{{"0"}}, query(t,
				"execute unsigned_between using @text_value,@lower_bound,@upper_bound"))
			exec(t, "set @upper_bound=cast(9007199254740992 as bit(64))")
			require.Equal(t, [][]string{{"0"}}, query(t,
				"execute unsigned_between using @text_value,@lower_bound,@upper_bound"))
		})
		t.Run("between_volatile_text_left", func(t *testing.T) {
			exec(t, "prepare volatile_between from 'select concat(''2'',rand()) between ? and ?'")
			defer conn.ExecContext(ctx, "deallocate prepare volatile_between")
			exec(t, "set @lower=0,@upper=100")
			require.Equal(t, [][]string{{"1"}}, query(t, "execute volatile_between using @lower,@upper"))
		})
		t.Run("between_text_warning_once", func(t *testing.T) {
			exec(t, "prepare warning_between from 'select ''invalid'' between ? and ?'")
			defer conn.ExecContext(ctx, "deallocate prepare warning_between")
			exec(t, "set @lower=-1,@upper=1")
			require.Equal(t, [][]string{{"1"}}, query(t, "execute warning_between using @lower,@upper"))
			require.Equal(t, [][]string{{"1"}}, query(t, "show count(*) warnings"))
		})
		t.Run("series_reuse", func(t *testing.T) {
			exec(t, "prepare gs_reuse from 'select count(*),min(result),max(result) from generate_series(?,?,?) g'")
			defer conn.ExecContext(ctx, "deallocate prepare gs_reuse")
			exec(t, "set @a=1,@b=9,@s=2")
			require.Equal(t, [][]string{{"5", "1", "9"}}, query(t, "execute gs_reuse using @a,@b,@s"))
			exec(t, "set @a=cast(1 as unsigned),@b=cast(9 as unsigned),@s=cast(2 as unsigned)")
			require.Equal(t, [][]string{{"5", "1", "9"}}, query(t, "execute gs_reuse using @a,@b,@s"))
			exec(t, "set @a=cast(18446744073709551615 as unsigned)")
			overflowErr := func() error {
				rows, err := conn.QueryContext(ctx, "execute gs_reuse using @a,@b,@s")
				if rows == nil {
					return err
				}
				defer rows.Close()
				for rows.Next() {
				}
				return rows.Err()
			}()
			require.Error(t, overflowErr, "an unsigned endpoint outside signed BIGINT must not wrap")
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
				return query(t, "select data_type,is_nullable from information_schema.columns where table_schema='review29349' and table_name='"+table+"' and column_name='result'")
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
				return query(t, "select data_type,is_nullable from information_schema.columns where table_schema='review29349' and table_name='"+table+"' and column_name='result'")
			}
			require.Equal(t, columnType("gs_date_direct"), columnType("gs_date_prepared"))
			require.Equal(t, query(t, "select result from gs_date_direct order by result"),
				query(t, "select result from gs_date_prepared order by result"))
		})
		t.Run("series_ctas_target_only_default", func(t *testing.T) {
			exec(t, "prepare gs_prefix from 'create table gs_prefix_result (extra int default 1) as select result from generate_series(?,?,?) g'")
			defer conn.ExecContext(ctx, "deallocate prepare gs_prefix")
			exec(t, "set @a=1,@b=3,@s=1")
			exec(t, "execute gs_prefix using @a,@b,@s")
			require.Equal(t, [][]string{{"1", "1"}, {"1", "2"}, {"1", "3"}},
				query(t, "select extra,result from gs_prefix_result order by result"))
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
		t.Run("series_binary_domain_and_scale_reuse", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx,
				"select result from generate_series(?,?,?) g order by result")
			require.NoError(t, err)
			defer stmt.Close()
			run := func(args ...any) (string, [][]string) {
				rows, err := stmt.QueryContext(ctx, args...)
				require.NoError(t, err)
				defer rows.Close()
				columns, err := rows.ColumnTypes()
				require.NoError(t, err)
				require.Len(t, columns, 1)
				var values [][]string
				for rows.Next() {
					var value string
					require.NoError(t, rows.Scan(&value))
					values = append(values, []string{value})
				}
				require.NoError(t, rows.Err())
				return columns[0].DatabaseTypeName(), values
			}
			wholeType, whole := run("2020-01-01 00:00:00", "2020-01-01 00:00:01", "1 second")
			require.Equal(t, query(t,
				"select result from generate_series('2020-01-01 00:00:00','2020-01-01 00:00:01','1 second') g order by result"), whole)
			fractionalType, fractional := run("2020-01-01 00:00:00.123", "2020-01-01 00:00:01.123", "1 second")
			require.Equal(t, wholeType, fractionalType)
			require.Equal(t, query(t,
				"select result from generate_series('2020-01-01 00:00:00.123','2020-01-01 00:00:01.123','1 second') g order by result"), fractional)
			_, microseconds := run("2020-01-01 00:00:00", "2020-01-01 00:00:00.000002", "1 microsecond")
			require.Equal(t, query(t,
				"select result from generate_series('2020-01-01 00:00:00','2020-01-01 00:00:00.000002','1 microsecond') g order by result"), microseconds)
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
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			exec(t, "set @j='not json'")
			badRows, err := conn.QueryContext(ctx, "execute u_large using @j")
			if err == nil {
				defer badRows.Close()
				for badRows.Next() {
				}
				err = badRows.Err()
			}
			require.Error(t, err)
			exec(t, `set @j=concat('["',repeat('x',70000),'"]')`)
			require.Equal(t, [][]string{{"1"}}, query(t, "execute u_large using @j"))
		})
		t.Run("precision_binary_protocol_masks_recovery", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx, "select case when result=2 then ceil(123.456,?) else 0 end from generate_series(1,3) g order by result")
			require.NoError(t, err)
			defer stmt.Close()
			runCase := func(v any) {
				rows, err := stmt.QueryContext(ctx, v)
				if v == nil {
					require.ErrorContains(t, err, "not const")
					return
				}
				require.NoError(t, err)
				defer rows.Close()
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
				require.Equal(t, []string{"0.000", "123.460", "0.000"}, got)
			}
			for _, v := range []any{2.5, "2.5tail", nil, 2.0} {
				runCase(v)
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
		t.Run("large_bitmap_state_protocol", func(t *testing.T) {
			state := roaring.New()
			for i := uint32(0); i < 40001; i++ {
				state.Add(i * 65537)
			}
			bitmap, err := state.ToBytes()
			require.NoError(t, err)
			require.Greater(t, len(bitmap), types.MaxVarBinaryLen)
			stmt, err := conn.PrepareContext(ctx,
				"select bitmap_count(bitmap_or_agg(?)) from (select 1) x")
			require.NoError(t, err)
			defer stmt.Close()
			for _, value := range []any{bitmap, []byte{0}, nil, bitmap} {
				var count sql.NullInt64
				err := stmt.QueryRowContext(ctx, value).Scan(&count)
				if invalid, ok := value.([]byte); ok && len(invalid) == 1 {
					require.Error(t, err)
					continue
				}
				require.NoError(t, err)
				if value == nil {
					require.False(t, count.Valid)
				} else {
					require.True(t, count.Valid)
					require.Equal(t, int64(40001), count.Int64)
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
