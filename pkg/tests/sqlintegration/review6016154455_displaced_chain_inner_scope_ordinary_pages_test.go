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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// The exact review-witnesses on the frozen head, executed through the real
// embedded MySQL protocol: a displaced CTE declaration chain must be followed
// until an actually visible declaration; UNION recursion classification must
// use the declarations resolved in the innermost parenthesized WITH scope; an
// ordinary CTE with a union body and enclosing pagination stages must apply
// every stage in order.
func TestReviewFourWitnessScopeChainClassificationAndOrdinaryPages(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		const probeDB = "patrol331_review_witnesses"
		_, e := conn.ExecContext(ctx, "drop database if exists "+probeDB)
		require.NoError(t, e)
		_, e = conn.ExecContext(ctx, "create database "+probeDB)
		require.NoError(t, e)
		_, e = conn.ExecContext(ctx, "use "+probeDB)
		require.NoError(t, e)
		defer func() { _, _ = conn.ExecContext(ctx, "drop database if exists "+probeDB) }()

		rows := func(sqlText string) ([]string, error) {
			rs, qErr := conn.QueryContext(ctx, sqlText)
			if qErr != nil {
				return nil, qErr
			}
			defer rs.Close()
			var got []string
			for rs.Next() {
				var v sql.NullString
				if sErr := rs.Scan(&v); sErr != nil {
					return nil, sErr
				}
				got = append(got, v.String)
			}
			return got, rs.Err()
		}
		expect := func(t *testing.T, sqlText string, want []string) {
			t.Helper()
			got, err := rows(sqlText)
			require.NoErrorf(t, err, "witness failed: %s", sqlText)
			// Unordered result sets compare order-independently: a reviewer
			// witness without an ORDER BY must not depend on incidental order.
			sort.Strings(got)
			require.Equalf(t, want, got, "witness: %s", sqlText)
		}

		for _, tc := range []struct {
			name string
			sql  string
			want []string
		}{
			{"displaced_chain_no_physical_returns_outer_7",
				"WITH scope_x AS (SELECT 7 AS a) (WITH owner_cte AS (WITH reader_cte AS (SELECT a FROM scope_x), scope_x AS (SELECT 9 AS a) (SELECT a FROM reader_cte)), scope_x AS (SELECT 8 AS a) SELECT a FROM owner_cte)",
				[]string{"7"}},
			{"union_classifies_qx_1_7",
				"WITH q AS (WITH x AS (SELECT 1 AS a) (WITH q AS (SELECT 7 AS a) SELECT a FROM x UNION ALL SELECT a FROM q)) SELECT a FROM q ORDER BY a",
				[]string{"1", "7"}},
			{"union_classifies_qz_unordered_1_2",
				"WITH q AS (WITH z AS (SELECT 2 AS a) (WITH q AS (SELECT a FROM z) (SELECT 1 AS a UNION ALL SELECT a FROM q))) SELECT a FROM q",
				[]string{"1", "2"}},
			{"ordinary_cte_two_pages_returns_2",
				"WITH q AS ((SELECT 1 AS a UNION ALL SELECT 2 AS a ORDER BY a LIMIT 2) LIMIT 1 OFFSET 1) SELECT a FROM q",
				[]string{"2"}},
			{"ordinary_cte_single_page_control",
				"WITH q AS ((SELECT 1 AS a UNION ALL SELECT 2 AS a ORDER BY a LIMIT 2)) SELECT a FROM q ORDER BY a",
				[]string{"1", "2"}},
			{"plain_derived_table_two_page_comparator",
				"SELECT * FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a ORDER BY a LIMIT 2) t LIMIT 1 OFFSET 1",
				[]string{"2"}},
			{"parenthesized_with_two_page_form",
				"WITH q AS (WITH x AS (SELECT 1 AS a UNION ALL SELECT 2 AS a ORDER BY a LIMIT 2) (SELECT a FROM x) ORDER BY a LIMIT 1 OFFSET 1) SELECT a FROM q",
				[]string{"2"}},
			{"reverse_stage_order_discriminator",
				"WITH q AS ((SELECT 1 AS a UNION ALL SELECT 2 AS a ORDER BY a LIMIT 3 OFFSET 1) ORDER BY a LIMIT 1) SELECT a FROM q",
				[]string{"2"}},
			{"inner_wrapper_with_two_stages",
				"WITH q AS ((WITH x AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) (SELECT a FROM x ORDER BY a LIMIT 2)) LIMIT 1 OFFSET 1) SELECT a FROM q",
				[]string{"2"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				expect(t, tc.sql, tc.want)
			})
		}

		t.Run("prepared_outer_page_and_empty_result_controls", func(t *testing.T) {
			// The enclosing page binds through the prepared protocol: the same
			// statement re-executed with different parameter slots must page the
			// wrapped stages independently, and a page landing past the finite
			// content yields an empty result rather than an error.
			q := "WITH q AS ((SELECT 1 AS a UNION ALL SELECT 2 AS a ORDER BY a LIMIT 2) ORDER BY a LIMIT ? OFFSET ?) SELECT a FROM q"
			st, pErr := conn.PrepareContext(ctx, q)
			require.NoError(t, pErr)
			defer st.Close()
			query := func(args ...any) []string {
				rs, e := st.QueryContext(ctx, args...)
				require.NoError(t, e)
				defer rs.Close()
				var got []string
				for rs.Next() {
					var v sql.NullString
					require.NoError(t, rs.Scan(&v))
					got = append(got, v.String)
				}
				require.NoError(t, rs.Err())
				return got
			}
			require.Equal(t, []string{"2"}, query(1, 1), "prepared LIMIT 1 OFFSET 1 pages the wrapped stage")
			require.Equal(t, []string{"1", "2"}, query(2, 0), "re-execution with new slots does not leak the previous page")
			require.Empty(t, query(2, 5), "a page past the finite content is empty, not an error")
		})

		t.Run("displaced_chain_with_physical_99_still_returns_outer_7", func(t *testing.T) {
			_, e := conn.ExecContext(ctx, "drop table if exists scope_x")
			require.NoError(t, e)
			_, e = conn.ExecContext(ctx, "create table scope_x(a int)")
			require.NoError(t, e)
			_, e = conn.ExecContext(ctx, "insert into scope_x values (99)")
			require.NoError(t, e)
			defer func() { _, _ = conn.ExecContext(ctx, "drop table if exists scope_x") }()
			got, err := rows("WITH scope_x AS (SELECT 7 AS a) (WITH owner_cte AS (WITH reader_cte AS (SELECT a FROM scope_x), scope_x AS (SELECT 9 AS a) (SELECT a FROM reader_cte)), scope_x AS (SELECT 8 AS a) SELECT a FROM owner_cte)")
			require.NoError(t, err)
			require.Len(t, got, 1, "the full row collector proves the single-row cardinality alongside the value")
			require.Equal(t, []string{"7"}, got,
				"the physical 99 must not capture the lookup while the outer 7 is visible")
		})
	})
}
