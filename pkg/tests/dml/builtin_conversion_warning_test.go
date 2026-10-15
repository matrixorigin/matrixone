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
		})
		t.Run("prepared_reuse", func(t *testing.T) {
			for _, prepared := range []struct{ expression, want string }{
				{"hex(lpad('x',?,'0'))", "303078"}, {"hex(rpad('x',?,'0'))", "783030"},
				{"hex(repeat('x',?))", "787878"}, {"hex(space(?))", "202020"},
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
