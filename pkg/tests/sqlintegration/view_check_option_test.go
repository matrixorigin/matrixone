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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestViewCheckOptionCanonicalization(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()

		const schema = "view_check_option_canonicalization"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		execSQLRequire(t, ctx, db, "drop database if exists "+schema)
		for round := 0; round < 2; round++ {
			t.Run(fmt.Sprintf("round_%d", round+1), func(t *testing.T) {
				execSQLRequire(t, ctx, db, "create database "+schema)
				execSQLRequire(t, ctx, db, "create view "+schema+".current_v as select 1 with cAsCaDeD check option")
				var current string
				require.NoError(t, db.QueryRowContext(ctx,
					"select mo_view_check_option(viewdef) from mo_catalog.mo_tables where reldatabase=? and relname='current_v' and account_id=0", schema).Scan(&current))
				require.Equal(t, "CASCADED", current)

				// Exercise the public scalar used by VIEWS without rewriting
				// catalog rows, changing admission floors, or waiting for the
				// startup predecessor VIEWS definition to be reconciled.
				for _, tc := range []struct {
					persisted string
					want      string
				}{
					{`{"Stmt":"create view old as select 1 with local check option"}`, "LOCAL"},
					{`{"Stmt":"create view old as select 1 with cascaded check option"}`, current},
					{`{"Stmt":"create view old as select 1 with CaScAdEd check option"}`, current},
					{`{"Stmt":"CREATE VIEW old AS SELECT 1 WITH CASCADED CHECK OPTION"}`, current},
					{`{"Stmt":"create view old as select 1"}`, "NONE"},
				} {
					var option, definition string
					require.NoError(t, db.QueryRowContext(ctx,
						"select mo_view_check_option(?), mo_view_definition(?)", tc.persisted, tc.persisted).Scan(&option, &definition))
					require.Equal(t, tc.want, option, tc.persisted)
					require.Equal(t, "select 1", definition)
				}
				// Replaying a legacy definition must preserve explicit COLLATE.
				// Binary and case-insensitive comparisons distinguish lost syntax.
				for _, tc := range []struct {
					collation string
					want      int
				}{{"utf8mb4_bin", 0}, {"utf8mb4_unicode_ci", 1}} {
					persisted := fmt.Sprintf(`{"Stmt":"create view old as select 'A' collate %s = 'a' as eq"}`, tc.collation)
					var definition string
					require.NoError(t, db.QueryRowContext(ctx, "select mo_view_definition(?)", persisted).Scan(&definition))
					require.Contains(t, definition, "collate "+tc.collation)
					var equal int
					require.NoError(t, db.QueryRowContext(ctx, definition).Scan(&equal))
					require.Equal(t, tc.want, equal, definition)
				}
				// A metadata round trip retains NAME_CONST's public column name,
				// bytes, column type, character set and collation for new and Stmt-only rows.
				replayCases := make([]struct{ selectSQL, want string }, 0, 25)
				replayCases = append(replayCases, []struct{ selectSQL, want string }{
					{`select 'ab' as c`, "ab"},
					{`select 'a\\b' as c`, "a\\b"},
					{`select 'é\\界' as c`, "é\\界"},
					{`select src.ab as c from (select _binary 'ab') src`, "ab"},
					{`select src.ab as c from (select _binary 'ab' as ab) src`, "ab"},
					{`select src.` + "`n + 1`" + ` as c from (select n + 1 from (select 1 as n) t) src`, "2"},
					{`select src.` + "`a\\b`" + ` as c from (select name_const('a\\b', 1)) src`, "1"},
					{`select src.ab as c from (select _binary 'ab') src(ab)`, "ab"},
					{`with src as (select _binary 'ab') select ab as c from src`, "ab"},
					{`select src.ab as c from ((select _binary 'ab') union all (select _binary 'cd' where false)) src`, "ab"},
					{`select (select src.ab from (select _binary 'ab') src) as c`, "ab"},
					{`select name_const('c', 1)`, "1"},
					{`select name_const('c', 'plain') as c`, "plain"},
					{`select name_const('c', _binary 'a\\b') as c`, "a\\b"},
					{`select length(name_const('c', 'a\\b')) as c`, "3"},
					{`select name_const('c', 'a\\b') as c`, "a\\b"},
					{`select name_const('c', ('a\\b'))`, "a\\b"},
					{`select name_const('c', 'a\\b' collate utf8mb4_bin)`, "a\\b"},
					{`select c from (select name_const('c', 'a\\b')) src`, "a\\b"},
					{`select name_const('c', 'a\\b$''é') as c`, "a\\b$'é"},
				}...)
				// The accepted literal domain crosses the declared VARCHAR limit.
				// Include both sides of that boundary and a multi-byte value whose
				// byte count crosses it while its character count does not.
				for _, width := range []int{65534, 65535, 65536} {
					value := strings.Repeat("a", width-1) + `\`
					replayCases = append(replayCases, struct{ selectSQL, want string }{
						"select '" + strings.ReplaceAll(value, `\`, `\\`) + "' as c", value})
				}
				for _, value := range []string{strings.Repeat("界", 32768) + `\`, strings.Repeat("界", 65535) + `\`} {
					replayCases = append(replayCases, struct{ selectSQL, want string }{
						"select name_const('c', '" + strings.ReplaceAll(value, `\`, `\\`) + "') as c", value})
				}
				for index, tc := range replayCases {
					source := fmt.Sprintf("%s.name_const_source_%d", schema, index)
					execSQLRequire(t, ctx, db, "create view "+source+" as "+tc.selectSQL)
					read := func(view string) [4]string {
						var values [4]string
						require.NoError(t, db.QueryRowContext(ctx,
							"select c, charset(c), collation(c) from "+view).Scan(&values[0], &values[1], &values[2]))
						require.NoError(t, db.QueryRowContext(ctx,
							"select column_type from information_schema.columns where table_schema=? and concat(table_schema, '.', table_name)=? and column_name='c'",
							schema, view).Scan(&values[3]))
						rows, err := db.QueryContext(ctx, "select * from "+view+" limit 0")
						require.NoError(t, err)
						defer rows.Close()
						names, err := rows.Columns()
						require.NoError(t, err)
						require.Equal(t, []string{"c"}, names)
						require.False(t, rows.Next())
						require.NoError(t, rows.Err())
						return values
					}
					want := read(source)
					require.Equal(t, tc.want, want[0])
					var currentDefinition string
					require.NoError(t, db.QueryRowContext(ctx,
						"select mo_view_definition(viewdef) from mo_catalog.mo_tables where account_id=0 and reldatabase=? and relname=?",
						schema, fmt.Sprintf("name_const_source_%d", index)).Scan(&currentDefinition))
					persisted, err := json.Marshal(map[string]string{"Stmt": "create view old as " + tc.selectSQL})
					require.NoError(t, err)
					var legacyDefinition string
					// A SQL literal keeps this input's actual width; an untyped
					// prepared parameter can acquire a default VARCHAR bound.
					legacyInput := strings.ReplaceAll(strings.ReplaceAll(string(persisted), `\`, `\\`), "'", "''")
					require.NoError(t, db.QueryRowContext(ctx, "select mo_view_definition('"+legacyInput+"')").Scan(&legacyDefinition))
					for generation, definition := range []string{currentDefinition, legacyDefinition} {
						replayed := fmt.Sprintf("%s.name_const_replayed_%d_%d", schema, index, generation)
						execSQLRequire(t, ctx, db, "create view "+replayed+" as "+definition)
						require.Equal(t, want, read(replayed), definition)
					}
				}
				execSQLRequire(t, ctx, db, "drop database "+schema)
			})
			if t.Failed() {
				return
			}
			var remaining int
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_tables where reldatabase=? and account_id=0", schema).Scan(&remaining))
			require.Zero(t, remaining, "verify teardown before reusing the same CN")
		}
	})
}
