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

package dml

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestBuiltinConversionWarnings(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		db := openRetestSQLDB(t, c)
		defer db.Close()
		for _, tc := range []struct {
			name, expression, want, diagnostic string
			count                              int
		}{
			{"repeat", "hex(repeat('x','3x'))", "787878", "INTEGER", 1},
			{"lpad", "hex(lpad('x','3x','0'))", "303078", "INTEGER", 1},
			{"rpad", "hex(rpad('x','3x','0'))", "783030", "INTEGER", 1},
			{"space", "hex(space('3x'))", "202020", "INTEGER", 1},
			{"substring_length", "substring('abcdef',1,'2x')", "ab", "INTEGER", 1},
			{"conv_target_base", "conv('10',10,'2x')", "1010", "INTEGER", 1},
			{"substring", "substring('abcdef','2x')", "bcdef", "INTEGER", 1},
			{"left", "left('abcdef','2x')", "ab", "INTEGER", 1},
			{"right", "right('abcdef','2x')", "ef", "INTEGER", 1},
			{"insert", "insert('abcdef','2x',1,'X')", "aXcdef", "INTEGER", 1},
			{"elt", "elt('2x','a','b','c')", "b", "INTEGER", 1},
			{"substring_index", "substring_index('a,b,c',',','2x')", "a,b", "INTEGER", 1},
			{"char", "hex(char('65x'))", "41", "INTEGER", 1},
			{"conv", "conv('10','2x','10')", "2", "INTEGER", 1},
			{"make_set", "make_set('2x','a','b','c')", "b", "INTEGER", 1},
			{"export_set", "export_set('2x','Y','N',',',3)", "N,Y,N", "INTEGER", 1},
			{"bit_count", "bit_count('3x')", "2", "INTEGER", 1},
			{"round", "round(12.345,'2x')", "12.350", "INTEGER", 1},
			{"truncate", "truncate(12.345,'2x')", "12.340", "INTEGER", 1},
			{"field", "field('x',0,'x')", "1", "DOUBLE", 2},
			{"valid", "repeat('x','3 ')", "xxx", "", 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				require.Equal(t, [][]string{{tc.want}}, queryStringRows(t, ctx, db, "select "+tc.expression))
				warnings := queryStringRows(t, ctx, db, "show warnings")
				require.Len(t, warnings, tc.count)
				for _, w := range warnings {
					require.Equal(t, "Warning", w[0])
					require.Equal(t, "1292", w[1])
					require.Contains(t, w[2], "Truncated incorrect "+tc.diagnostic+" value")
				}
			})
		}
		t.Run("constant_consumers", func(t *testing.T) {
			t.Run("malformed", func(t *testing.T) {
				for _, tc := range []struct{ name, query, want string }{
					{"ceil", "select ceil(12.345,bit_count('3x')) = 12.35", "1"},
					{"ceil_decimal", "select ceil(12.345,cast(bit_count('3x') as decimal(18,0))) = 12.35", "1"},
					{"extractvalue", "select extractvalue('<a>x</a>',concat('/a[',bit_count('1x'),']'))", "x"},
					{"updatexml", "select updatexml('<a>x</a>',concat('/a[',bit_count('1x'),']'),'<b/>')", "<b/>"},
				} {
					t.Run(tc.name, func(t *testing.T) {
						require.Equal(t, [][]string{{tc.want}}, queryStringRows(t, ctx, db, tc.query))
						warnings := queryStringRows(t, ctx, db, "show warnings")
						require.Len(t, warnings, 1)
						require.Equal(t, "1292", warnings[0][1])
					})
				}
			})
			for _, tc := range []struct{ name, query, want string }{
				{"ceil_text", "select ceil(12.345,bit_count('3')) = 12.35", "1"},
				{"ceil_numeric", "select ceil(12.345,bit_count(3)) = 12.35", "1"},
				{"ceil_nested_text", "select ceil(12.345,bit_count(concat('', '3'))) = 12.35", "1"},
				{"extractvalue_text", "select extractvalue('<a><b>x</b></a>',concat('/a/b[',bit_count('1'),']'))", "x"},
				{"extractvalue_numeric", "select extractvalue('<a><b>x</b></a>',concat('/a/b[',bit_count(1),']'))", "x"},
				{"extractvalue_nested_text", "select extractvalue('<a><b>x</b></a>',concat('/a/b[',bit_count(concat('', '1')),']'))", "x"},
				{"updatexml_text", "select updatexml('<a><b>x</b></a>',concat('/a/b[',bit_count('1'),']'),'<c/>')", "<a><c/></a>"},
				{"updatexml_numeric", "select updatexml('<a><b>x</b></a>',concat('/a/b[',bit_count(1),']'),'<c/>')", "<a><c/></a>"},
				{"percentile_text", "select percentile_disc(bit_count('1')) within group (order by v) from (select 10 as v union all select 20) s", "20"},
				{"percentile_numeric", "select percentile_disc(bit_count(1)) within group (order by v) from (select 10 as v union all select 20) s", "20"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					require.Equal(t, [][]string{{tc.want}}, queryStringRows(t, ctx, db, tc.query))
					require.Empty(t, queryStringRows(t, ctx, db, "show warnings"))
				})
			}
			t.Run("prepared_xml_reuse", func(t *testing.T) {
				query := "select extractvalue('<a><b>x</b><b>y</b></a>',concat('/a/b[',bit_count(cast(? as char)),']'))"
				stmt, err := db.PrepareContext(ctx, query)
				require.NoError(t, err)
				defer stmt.Close()
				execSQLDB(t, ctx, db, "prepare warning_xpath from '"+strings.ReplaceAll(query, "'", "''")+"'")
				defer execSQLDB(t, ctx, db, "deallocate prepare warning_xpath")
				for _, tc := range []struct{ input, want string }{{"1", "x"}, {"3x", "y"}, {"1x", "x"}, {"3", "y"}, {"1x", "x"}} {
					var got string
					require.NoError(t, stmt.QueryRowContext(ctx, tc.input).Scan(&got))
					require.Equal(t, tc.want, got)
					warnings := queryStringRows(t, ctx, db, "show warnings")
					if strings.HasSuffix(tc.input, "x") {
						require.Equal(t, [][]string{{"Warning", "1292", "Truncated incorrect INTEGER value: '" + tc.input + "'"}}, warnings)
					} else {
						require.Empty(t, warnings)
					}
					execSQLDB(t, ctx, db, "set @warning_xpath_arg='"+tc.input+"'")
					require.Equal(t, [][]string{{tc.want}}, queryStringRows(t, ctx, db, "execute warning_xpath using @warning_xpath_arg"))
					require.Equal(t, warnings, queryStringRows(t, ctx, db, "show warnings"))
				}
			})
		})
		t.Run("columns_and_statement_constants", func(t *testing.T) {
			execSQLDB(t, ctx, db, "create database builtin_conversion_warnings")
			defer cleanupTestDatabases(t, db, "builtin_conversion_warnings")
			execSQLDB(t, ctx, db, "use builtin_conversion_warnings")
			execSQLDB(t, ctx, db, "create table inputs(id int, n varchar(16))")
			execSQLDB(t, ctx, db, "insert into inputs values(1,'3x'),(2,'2'),(3,null)")
			require.Equal(t, [][]string{{"xxx"}, {"xx"}, {"NULL"}}, queryStringRows(t, ctx, db, "select repeat('x',n) from inputs order by id"))
			require.Equal(t, [][]string{{"Warning", "1292", "Truncated incorrect INTEGER value: '3x'"}}, queryStringRows(t, ctx, db, "show warnings"))
			require.Equal(t, [][]string{{"xxx"}, {"xxx"}, {"xxx"}}, queryStringRows(t, ctx, db, "select repeat('x','3x') from inputs order by id"))
			// Projection casts preserve one diagnostic per evaluated row.
			require.Len(t, queryStringRows(t, ctx, db, "show warnings"), 3)
			require.Equal(t, [][]string{{"safe"}, {"xx"}, {"NULL"}}, queryStringRows(t, ctx, db, "select if(id=1,'safe',repeat('x',n)) from inputs order by id"))
			require.Empty(t, queryStringRows(t, ctx, db, "show warnings"), "unselected malformed value must not diagnose")
			for _, tc := range []struct {
				name, expression, predicate string
				want                        [][]string
				warnings                    int
			}{
				{"bit_count_constant", "bit_count('3x')", "", [][]string{{"2"}, {"2"}, {"2"}}, 3},
				{"bit_count_nested", "bit_count('3x')+1", "", [][]string{{"3"}, {"3"}, {"3"}}, 3},
				{"ceil_constant", "cast(ceil(12.345,bit_count('3x')) = 12.35 as char)", "", [][]string{{"1"}, {"1"}, {"1"}}, 3},
				{"ceil_partial", "if(id=1,'safe',cast(ceil(12.345,bit_count('3x')) = 12.35 as char))", "", [][]string{{"safe"}, {"1"}, {"1"}}, 2},
				{"xml_partial", "if(id=1,'safe',extractvalue('<a>x</a>',concat('/a[',bit_count('3x')-1,']')))", "", [][]string{{"safe"}, {"x"}, {"x"}}, 2},
				{"xml_inactive", "if(id>0,'safe',updatexml('<a>x</a>',concat('/a[',bit_count('3x')-1,']'),'<b/>'))", "", [][]string{{"safe"}, {"safe"}, {"safe"}}, 0},
				{"ceil_empty", "cast(ceil(12.345,bit_count('3x')) as char)", " where id<0", nil, 0},
				{"bit_count_partial_if", "if(id=1,0,bit_count('3x'))", "", [][]string{{"0"}, {"2"}, {"2"}}, 2},
				{"bit_count_partial_case", "case when id=1 then 0 else bit_count('3x') end", "", [][]string{{"0"}, {"2"}, {"2"}}, 2},
				{"bit_count_partial_coalesce", "coalesce(if(id=1,0,null),bit_count('3x'))", "", [][]string{{"0"}, {"2"}, {"2"}}, 2},
				{"bit_count_column", "bit_count(n)", "", [][]string{{"2"}, {"1"}, {"NULL"}}, 1},
				{"bit_count_inactive", "if(id>0,0,bit_count('3x'))", "", [][]string{{"0"}, {"0"}, {"0"}}, 0},
				{"bit_count_empty", "bit_count('3x')", " where id<0", nil, 0},
				{"bit_count_valid_text", "bit_count('3 ')", "", [][]string{{"2"}, {"2"}, {"2"}}, 0},
				{"bit_count_numeric", "bit_count(3)", "", [][]string{{"2"}, {"2"}, {"2"}}, 0},
				{"bit_count_binary", "bit_count(cast('3x' as binary))", "", [][]string{{"8"}, {"8"}, {"8"}}, 0},
			} {
				t.Run(tc.name, func(t *testing.T) {
					require.Equal(t, tc.want, queryStringRows(t, ctx, db, "select "+tc.expression+" from inputs"+tc.predicate+" order by id"))
					warnings := queryStringRows(t, ctx, db, "show warnings")
					require.Len(t, warnings, tc.warnings)
					for _, warning := range warnings {
						require.Equal(t, []string{"Warning", "1292", "Truncated incorrect INTEGER value: '3x'"}, warning)
					}
				})
			}
			t.Run("generated_columns", func(t *testing.T) {
				for _, tc := range []struct {
					name, sqlType, values, updated string
					want                           [][]string
					warnings                       int
				}{
					{"text", "varchar(16)", "(1,'3x'),(2,'2'),(3,null)", "3", [][]string{{"2"}, {"1"}, {"NULL"}}, 1},
					{"numeric", "int", "(1,3),(2,2),(3,null)", "3", [][]string{{"2"}, {"1"}, {"NULL"}}, 0},
					{"binary", "varbinary(16)", "(1,'3x'),(2,'2'),(3,null)", "5", [][]string{{"8"}, {"3"}, {"NULL"}}, 0},
				} {
					t.Run(tc.name, func(t *testing.T) {
						table := "generated_bit_count_" + tc.name
						execSQLDB(t, ctx, db, "create table "+table+"(id int primary key, v "+tc.sqlType+", bits bigint unsigned generated always as (bit_count(v)) stored)")
						execSQLDB(t, ctx, db, "insert into "+table+"(id,v) values"+tc.values)
						warnings := queryStringRows(t, ctx, db, "show warnings")
						require.Len(t, warnings, tc.warnings)
						if tc.warnings != 0 {
							require.Equal(t, []string{"Warning", "1292", "Truncated incorrect INTEGER value: '3x'"}, warnings[0])
						}
						require.Equal(t, tc.want, queryStringRows(t, ctx, db, "select bits from "+table+" order by id"))
						execSQLDB(t, ctx, db, "update "+table+" set v='7' where id=1")
						require.Empty(t, queryStringRows(t, ctx, db, "show warnings"))
						require.Equal(t, [][]string{{tc.updated}}, queryStringRows(t, ctx, db, "select bits from "+table+" where id=1"))
					})
				}
			})
			t.Run("returning", func(t *testing.T) {
				for _, tc := range []struct {
					name, expression, want string
					warnings               int
				}{
					{"numeric", "bit_count(id)", "1", 0},
					{"binary", "bit_count(cast(n as binary))", "5", 0},
					{"text", "bit_count(n)", "3", 0},
					{"malformed_literal", "bit_count('3x')", "2", 1},
				} {
					t.Run(tc.name, func(t *testing.T) {
						// Each case must perform the same real mutation rather than
						// inheriting the previous case's already-updated value.
						execSQLDB(t, ctx, db, "update inputs set n='3' where id=1")
						require.Equal(t, [][]string{{tc.want}}, queryStringRows(t, ctx, db, "update inputs set n='7' where id=1 returning "+tc.expression))
						warnings := queryStringRows(t, ctx, db, "show warnings")
						require.Len(t, warnings, tc.warnings)
						if tc.warnings != 0 {
							require.Equal(t, []string{"Warning", "1292", "Truncated incorrect INTEGER value: '3x'"}, warnings[0])
						}
					})
				}
			})
		})
		t.Run("prepared_reuse", func(t *testing.T) {
			for _, prepared := range []struct{ expression, want string }{
				{"hex(lpad('x',?,'0'))", "303078"}, {"hex(rpad('x',?,'0'))", "783030"},
				{"hex(repeat('x',?))", "787878"}, {"hex(space(?))", "202020"},
				{"bit_count(cast(? as char))", "2"},
			} {
				expression := prepared.expression
				func() {
					stmt, err := db.PrepareContext(ctx, "select "+expression)
					require.NoError(t, err)
					defer stmt.Close()
					execSQLDB(t, ctx, db, "prepare warning_input from 'select "+strings.ReplaceAll(expression, "'", "''")+"'")
					defer execSQLDB(t, ctx, db, "deallocate prepare warning_input")
					for _, arg := range []any{"3x", "3", int64(3), nil, "3x"} {
						var got sql.NullString
						require.NoError(t, stmt.QueryRowContext(ctx, arg).Scan(&got))
						warnings := queryStringRows(t, ctx, db, "show warnings")
						wantCount := 0
						if arg == "3x" {
							wantCount = 1
						}
						require.Len(t, warnings, wantCount, "binary prepared %s arg=%v", expression, arg)
						require.Equal(t, arg != nil, got.Valid)
						if got.Valid {
							require.Equal(t, prepared.want, got.String)
						}
						if wantCount != 0 {
							require.Equal(t, [][]string{{"Warning", "1292", "Truncated incorrect INTEGER value: '3x'"}}, warnings)
						}
						if arg == nil {
							execSQLDB(t, ctx, db, "set @warning_arg=null")
						} else if arg == "3x" {
							execSQLDB(t, ctx, db, "set @warning_arg='3x'")
						} else if arg == int64(3) {
							execSQLDB(t, ctx, db, "set @warning_arg=3")
						} else {
							execSQLDB(t, ctx, db, "set @warning_arg='3'")
						}
						rows := queryStringRows(t, ctx, db, "execute warning_input using @warning_arg")
						if got.Valid {
							require.Equal(t, [][]string{{got.String}}, rows)
						} else {
							require.Equal(t, [][]string{{"NULL"}}, rows)
						}
						sqlWarnings := queryStringRows(t, ctx, db, "show warnings")
						require.Equal(t, warnings, sqlWarnings, "SQL prepared %s arg=%v", expression, arg)
					}
				}()
			}
		})
	})
}
