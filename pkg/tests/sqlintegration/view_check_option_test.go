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
