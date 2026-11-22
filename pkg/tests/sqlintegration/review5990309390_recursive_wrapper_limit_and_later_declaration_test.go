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
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// A recursive CTE whose body is a WITH-bearing, parenthesized query expression
// must behave exactly like its unwrapped twin: the wrapper's own effective
// LIMIT bounds the recursion, every wrapper's WITH names its body scope, and a
// later same-WITH declaration stays invisible to earlier bodies (falling back
// to a physical table, or a typed missing-table error when none exists).
// Exercised end to end through the real embedded single-CN MySQL protocol.
func TestRecursiveWrapperLimitAndLaterDeclarationScopeSQL(t *testing.T) {
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

		const testDB = "review5990309390_wrapper_scope"
		var preexisting int
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", testDB).Scan(&preexisting))
		require.Zero(t, preexisting, "owned database name must not preexist: %s", testDB)
		_, err = conn.ExecContext(ctx, "CREATE DATABASE "+testDB+"; USE "+testDB)
		require.NoError(t, err, "multi-statement create-database request")
		defer cleanupSQLIntegration(t, cn, "DROP DATABASE IF EXISTS "+testDB)

		queryInts := func(t *testing.T, query string) []int {
			t.Helper()
			rows, qerr := conn.QueryContext(ctx, query)
			require.NoError(t, qerr, "query must plan and execute: %s", query)
			defer rows.Close()
			var got []int
			for rows.Next() {
				var v int
				require.NoError(t, rows.Scan(&v))
				got = append(got, v)
			}
			require.NoError(t, rows.Err())
			sort.Ints(got)
			return got
		}

		requireInts := func(t *testing.T, query string, want ...int) {
			t.Helper()
			got := queryInts(t, query)
			require.Equal(t, want, got, "query %s returned wrong rows", query)
		}

		const doublePending = "WITH owner_a AS (WITH owner_b AS (SELECT a FROM pending_x), pending_x AS (SELECT 8 AS a) (SELECT a FROM owner_b)), pending_x AS (SELECT 9 AS a) SELECT a FROM owner_a"
		const wrappedRecursive = "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) (SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3 LIMIT 2)) SELECT n FROM q"
		const unwrappedTwin = "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3 LIMIT 2) SELECT n FROM q"
		const nestedWrapperRecursive = "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) (WITH y AS (SELECT a FROM x) (SELECT a FROM y UNION ALL SELECT n + 1 FROM q WHERE n < 3 LIMIT 2))) SELECT n FROM q"

		t.Run("later_declaration_lookup_fails_without_physical_table", func(t *testing.T) {
			var probe int
			qerr := conn.QueryRowContext(ctx, doublePending).Scan(&probe)
			require.Error(t, qerr, "both pending_x declarations occur after their bodies: no physical table means the lookup must fail")
			var myErr *gomysql.MySQLError
			require.True(t, errors.As(qerr, &myErr), "expected a typed mysql error for the missing table, got: %v", qerr)
			require.Equal(t, uint16(1146), myErr.Number, "typed missing-table error required, got: %v", qerr)
		})
		t.Run("physical_table_serves_fallback_after_later_declaration_withheld", func(t *testing.T) {
			_, eerr := conn.ExecContext(ctx, "CREATE TABLE pending_x(a INT); INSERT INTO pending_x VALUES (99)")
			require.NoError(t, eerr)
			requireInts(t, doublePending, 99)
		})
		t.Run("enclosing_visible_cte_serves_body", func(t *testing.T) {
			requireInts(t, "WITH enclosing_src AS (SELECT 7 AS a) (WITH inner_reader AS (SELECT a FROM enclosing_src) SELECT a FROM inner_reader)", 7)
		})
		// Wrappers that sit outside the first WITH-bearing select paginate the
		// body like any other owner: the review witness composes the union's own
		// emission cover with the enclosing pages, so every owning clause applies
		// in order and none may be dropped by the scope-preserving peel.
		const outerWrappedRecursive = "WITH RECURSIVE q(n) AS (((WITH x AS (SELECT 1 AS a) (SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 5 LIMIT 4)) LIMIT 2) LIMIT 1) SELECT n FROM q"
		const outerWrappedRecursiveOffset = "WITH RECURSIVE q(n) AS (((WITH x AS (SELECT 1 AS a) (SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 5 LIMIT 4)) LIMIT 2 OFFSET 1) LIMIT 1) SELECT n FROM q"
		const outerWrappedOrdinary = "WITH c AS ((WITH x AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) (SELECT a FROM x)) LIMIT 1) SELECT a FROM c"
		t.Run("wrappers_outside_with_keep_recursive_pagination", func(t *testing.T) {
			requireInts(t, outerWrappedRecursive, 1)
		})
		t.Run("wrappers_outside_with_compose_offset", func(t *testing.T) {
			requireInts(t, outerWrappedRecursiveOffset, 2)
		})
		t.Run("wrappers_outside_with_keep_ordinary_cte_pagination", func(t *testing.T) {
			requireInts(t, outerWrappedOrdinary, 1)
		})
		t.Run("outer_only_clause_without_inner_limit_bounds_the_body", func(t *testing.T) {
			requireInts(t, "WITH RECURSIVE q(n) AS ((WITH x AS (SELECT 1 AS a) (SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3)) LIMIT 2) SELECT n FROM q", 1, 2)
		})
		t.Run("ordered_recursive_wrapper_keeps_existing_typed_contract", func(t *testing.T) {
			var probe int
			qerr := conn.QueryRowContext(ctx,
				"WITH RECURSIVE q(n) AS ((WITH x AS (SELECT 1 AS a) (SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3 LIMIT 3)) ORDER BY n DESC LIMIT 2) SELECT n FROM q").Scan(&probe)
			if qerr != nil {
				var myErr *gomysql.MySQLError
				require.True(t, errors.As(qerr, &myErr), "an ordered recursive wrapper must keep a typed existing contract, got: %v", qerr)
				require.Equal(t, uint16(1064), myErr.Number, "the ordered recursive wrapper keeps the ER_PARSE_ERROR contract")
				require.True(t, strings.Contains(strings.ToLower(myErr.Message), "order"),
					"the parse error must point at the rejected ORDER BY position, got: %q", myErr.Message)
				return
			}
			// If the current contract orders the consumer, the descending
			// wrapper window is exactly the largest two values.
			require.Equal(t, 3, probe, "an accepted ordered wrapper must order its own page window")
		})
		t.Run("wrapped_recursive_body_keeps_effective_limit", func(t *testing.T) {
			requireInts(t, wrappedRecursive, 1, 2)
		})
		t.Run("unwrapped_recursive_body_limit_twin", func(t *testing.T) {
			requireInts(t, unwrappedTwin, 1, 2)
		})
		t.Run("nested_wrapper_with_resolves_inner_scope", func(t *testing.T) {
			requireInts(t, nestedWrapperRecursive, 1, 2)
		})
		t.Run("outer_wrapper_limit_cannot_erase_inner_limit", func(t *testing.T) {
			requireInts(t, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) ((SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 4 LIMIT 2) LIMIT 3)) SELECT n FROM q", 1, 2)
		})
		t.Run("outer_wrapper_offset_composes_over_inner_limit", func(t *testing.T) {
			requireInts(t, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) ((SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 4 LIMIT 2) LIMIT 3 OFFSET 1)) SELECT n FROM q", 2)
		})
		t.Run("inner_offset_survives_enclosing_pagination", func(t *testing.T) {
			requireInts(t, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) ((SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 5 LIMIT 2 OFFSET 1) LIMIT 3)) SELECT n FROM q", 2, 3)
		})
		t.Run("nested_offsets_compose_page_by_page", func(t *testing.T) {
			requireInts(t, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) ((SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 5 LIMIT 2 OFFSET 1) LIMIT 3 OFFSET 1)) SELECT n FROM q", 3)
		})
		t.Run("middle_layer_pagination_is_not_dropped", func(t *testing.T) {
			requireInts(t, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) (((SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 5 LIMIT 4) LIMIT 2 OFFSET 1) LIMIT 4)) SELECT n FROM q", 2, 3)
		})
		t.Run("derived_pagination_oracle_inner_offset", func(t *testing.T) {
			_, eerr := conn.ExecContext(ctx, "CREATE TABLE pagination_oracle(a INT); INSERT INTO pagination_oracle VALUES (1),(2),(3),(4),(5)")
			require.NoError(t, eerr)
			requireInts(t, "SELECT a FROM (SELECT a FROM (SELECT a FROM pagination_oracle LIMIT 2 OFFSET 1) t LIMIT 3) u", 2, 3)
		})
		t.Run("derived_pagination_oracle_two_offsets", func(t *testing.T) {
			requireInts(t, "SELECT a FROM (SELECT a FROM (SELECT a FROM pagination_oracle LIMIT 2 OFFSET 1) t LIMIT 3 OFFSET 1) u", 3)
		})
		t.Run("derived_pagination_oracle_middle_layer", func(t *testing.T) {
			requireInts(t, "SELECT a FROM (SELECT a FROM (SELECT a FROM (SELECT a FROM pagination_oracle LIMIT 4) t LIMIT 2 OFFSET 1) u LIMIT 4) v", 2, 3)
		})
		t.Run("single_page_uint64_limit_plus_offset", func(t *testing.T) {
			requireInts(t, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) SELECT a FROM x UNION ALL SELECT n+1 FROM q WHERE n<3 LIMIT 18446744073709551615 OFFSET 1) SELECT n FROM q", 2, 3)
		})
		t.Run("outer_uint64_max_limit_is_noop_on_finite_inner_page", func(t *testing.T) {
			requireInts(t, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) ((SELECT a FROM x UNION ALL SELECT n+1 FROM q WHERE n<3 LIMIT 2 OFFSET 1) LIMIT 18446744073709551615)) SELECT n FROM q", 2, 3)
		})
		t.Run("zero_inner_page_with_outer_skip_is_empty", func(t *testing.T) {
			requireInts(t, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) ((SELECT a FROM x UNION ALL SELECT n+1 FROM q WHERE n<3 LIMIT 0) LIMIT 3 OFFSET 1)) SELECT n FROM q")
		})
		t.Run("outer_skip_exhausts_finite_inner_page", func(t *testing.T) {
			requireInts(t, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) ((SELECT a FROM x UNION ALL SELECT n+1 FROM q WHERE n<3 LIMIT 2) LIMIT 3 OFFSET 4)) SELECT n FROM q")
		})
		t.Run("nested_offset_exhausts_uint64_page_without_overflow", func(t *testing.T) {
			requireInts(t, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) ((SELECT a FROM x UNION ALL SELECT n+1 FROM q WHERE n<3 LIMIT 2 OFFSET 18446744073709551615) LIMIT 1 OFFSET 1)) SELECT n FROM q")
		})
		t.Run("inner_offset_only_count_stays_unbounded", func(t *testing.T) {
			requireInts(t, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) ((SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3 OFFSET 1) LIMIT 2)) SELECT n FROM q", 2, 3)
		})
		t.Run("single_stage_offset_only_unwrapped_control", func(t *testing.T) {
			requireInts(t, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3 OFFSET 1) SELECT n FROM q", 2, 3)
		})
		t.Run("prepared_offset_param_zero_null_one", func(t *testing.T) {
			stmt, perr := conn.PrepareContext(ctx, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) ((SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3 LIMIT 3) LIMIT 2 OFFSET ?)) SELECT n FROM q")
			require.NoError(t, perr, "prepare must accept the same composed pagination SQL")
			defer stmt.Close()
			collect := func(stmt *sql.Stmt, args ...any) []int {
				t.Helper()
				rows, qerr := stmt.QueryContext(ctx, args...)
				require.NoError(t, qerr, "prepared execution must run: %v", args)
				defer rows.Close()
				var got []int
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				require.NoError(t, rows.Err())
				sort.Ints(got)
				return got
			}
			// nearest single-stage unwrapped control isolates any composed-
			// expression reachability from the established prepared-offset
			// contract of the plain recursive body
			control, cerr := conn.PrepareContext(ctx, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) (SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3 LIMIT 3 OFFSET ?)) SELECT n FROM q")
			require.NoError(t, cerr, "control prepare must accept the single-stage prepared offset")
			defer control.Close()
			require.Equal(t, []int{1, 2, 3}, collect(control, 0), "the single-stage control must skip nothing for param 0")
			require.Equal(t, []int{2, 3}, collect(control, 1), "the single-stage control must skip one row for param 1")
			require.Equal(t, []int{1, 2}, collect(stmt, 0), "param 0 must skip nothing")
			require.Equal(t, []int{1, 2}, collect(stmt, nil), "a NULL prepared offset must normalize to skip-nothing, never an empty result")
			require.Equal(t, []int{2, 3}, collect(stmt, 1), "param 1 must skip one row")
			require.Equal(t, []int{1, 2}, collect(stmt, 0), "reused statement execution must not carry a stale binding")
			// A NULL prepared COUNT is empty by contract; this proves the
			// composed pagination distinguishes NULL COUNT (no rows) from an
			// absent COUNT (unbounded), the plain offset-only case above.
			nullCount, ncerr := conn.PrepareContext(ctx, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) ((SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3 LIMIT 3) LIMIT ? OFFSET 1)) SELECT n FROM q")
			require.NoError(t, ncerr, "prepare must accept a prepared NULL COUNT")
			defer nullCount.Close()
			require.Len(t, collect(nullCount, nil), 0, "a NULL prepared COUNT must yield an empty result, never unbounded rows")
		})
		t.Run("prepared_distinct_slots_across_stages_swap_null_and_reuse", func(t *testing.T) {
			// One prepared handle carries DISTINCT LIMIT/OFFSET slots per
			// enclosing stage in statement order: A.LIMIT, A.OFFSET, B.LIMIT,
			// B.OFFSET. Swapped values return observably different rows, a
			// NULL offset normalizes to skip-nothing, a NULL count yields
			// empty, an exhausted page stays empty, and the reused statement
			// returns the original rows with no stale binding.
			stmt, perr := conn.PrepareContext(ctx, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) (((SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3 LIMIT 3) LIMIT ? OFFSET ?) LIMIT ? OFFSET ?)) SELECT n FROM q")
			require.NoError(t, perr, "prepare must accept the multi-slot composed pagination")
			defer stmt.Close()
			collect := func(t *testing.T, args ...any) []int {
				t.Helper()
				rows, qerr := stmt.QueryContext(ctx, args...)
				require.NoError(t, qerr, "prepared execution must run: %v", args)
				defer rows.Close()
				var got []int
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				require.NoError(t, rows.Err())
				sort.Ints(got)
				return got
			}
			// statement-order slots: A.limit=3 A.offset=0 B.limit=2 B.offset=1
			require.Equal(t, []int{2, 3}, collect(t, 3, 0, 2, 1), "run 1 the three-row stream paged by skip 1 then limit 2")
			// swapped/different values must return observably different rows
			require.Equal(t, []int{3}, collect(t, 2, 1, 3, 1), "run 2 skip 1 take 2, then skip 1 leaves only 3")
			// asymmetry at the same total skip: the OFFSET swap is observable.
			// A(1,1) pages the base stream to [2], B(2,0) keeps it.
			require.Equal(t, []int{2}, collect(t, 1, 1, 2, 0),
				"A.limit=1 A.offset=1 B.limit=2 B.offset=0 must return exactly [2]")
			// the same slots repaged: A(1,0) keeps only [1], B(2,1) skips past it.
			require.Len(t, collect(t, 1, 0, 2, 1), 0,
				"A.limit=1 A.offset=0 B.limit=2 B.offset=1 must empty the result: swapping the offsets changes the rows")
			// reuse after the discriminating pairs: no stale binding lingers.
			require.Equal(t, []int{2}, collect(t, 1, 1, 2, 0),
				"the reused statement returns the discriminating pair's rows again")
			// a NULL offset normalizes to skip-nothing on its own stage
			require.Equal(t, []int{2, 3}, collect(t, 3, nil, 2, 1), "a NULL prepared offset skips nothing")
			// a NULL count yields an empty result, never unbounded rows
			require.Len(t, collect(t, nil, 0, 2, 1), 0, "a NULL prepared count must stay empty")
			// an offset beyond the finite stream is an exhausted empty page
			require.Len(t, collect(t, 3, 0, 2, 4), 0, "an exhausted page stays empty")
			// reuse returns the original rows: no stale execution binding
			require.Equal(t, []int{2, 3}, collect(t, 3, 0, 2, 1), "the reused statement returns the original rows")
		})
		t.Run("prepared_innermost_offset_param_zero_null_one_zero", func(t *testing.T) {
			stmt, perr := conn.PrepareContext(ctx, "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) ((SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3 LIMIT 3 OFFSET ?) LIMIT 2)) SELECT n FROM q")
			require.NoError(t, perr, "prepare must accept the innermost prepared offset")
			defer stmt.Close()
			collect := func(args ...any) []int {
				t.Helper()
				rows, qerr := stmt.QueryContext(ctx, args...)
				require.NoError(t, qerr, "prepared execution must run: %v", args)
				defer rows.Close()
				var got []int
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				require.NoError(t, rows.Err())
				sort.Ints(got)
				return got
			}
			require.Equal(t, []int{1, 2}, collect(0), "param 0 must skip nothing")
			require.Equal(t, []int{1, 2}, collect(nil), "a NULL innermost prepared offset must normalize to skip-nothing, never an empty result")
			require.Equal(t, []int{2, 3}, collect(1), "param 1 must skip one row")
			require.Equal(t, []int{1, 2}, collect(0), "reused statement execution must not carry a stale binding")
		})
	})
}

// The full DDL lifecycle of a column declared with the unquoted keyword END:
// creation, insertion, defaults, positional adds, renames, type changes and
// the drop all reference it by the same unambiguous spelling through the real
// embedded single-CN MySQL protocol, on par with its quoted twin.
func TestEndColumnAlterLifecycleSQL(t *testing.T) {
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

		const testDB = "review5990578563_end_alter_lifecycle"
		var preexisting int
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", testDB).Scan(&preexisting))
		require.Zero(t, preexisting, "owned database name must not preexist: %s", testDB)
		_, err = conn.ExecContext(ctx, "CREATE DATABASE "+testDB+"; USE "+testDB)
		require.NoError(t, err, "multi-statement create-database request")
		defer cleanupSQLIntegration(t, cn, "DROP DATABASE IF EXISTS "+testDB)

		for _, step := range []struct {
			name string
			sql  string
		}{
			{"create_and_seed", "CREATE TABLE end_cycle(id INT PRIMARY KEY, end INT); INSERT INTO end_cycle VALUES (1, 7), (2, 8)"},
			{"bare_end_set_default", "ALTER TABLE end_cycle ALTER COLUMN end SET DEFAULT 7"},
			{"default_reaches_new_rows", "INSERT INTO end_cycle (id) VALUES (3); "},
			{"bare_end_add_after", "ALTER TABLE end_cycle ADD COLUMN tail INT AFTER end"},
		} {
			_, err := conn.ExecContext(ctx, step.sql)
			require.NoError(t, err, "lifecycle step %s must execute: %s", step.name, step.sql)
		}
		var v int
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT end FROM end_cycle WHERE id = 3").Scan(&v))
		require.Equal(t, 7, v, "the default declared through the bare reference must reach the row")
		_, uerr1 := conn.ExecContext(ctx, "INSERT INTO end_cycle(id,end) VALUES (1,11) ON DUPLICATE KEY UPDATE end = VALUES(end)")
		require.NoError(t, uerr1, "the bare END upsert accessor must parse and apply its conflict value")
		var upsertBare int
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT end FROM end_cycle WHERE id = 1").Scan(&upsertBare))
		require.Equal(t, 11, upsertBare, "the bare END conflict accessor must apply its value before the quoted control runs")
		_, uerr2 := conn.ExecContext(ctx, "INSERT INTO end_cycle(id,`end`) VALUES (1,12) ON DUPLICATE KEY UPDATE `end` = VALUES(`end`)")
		require.NoError(t, uerr2, "the quoted END upsert control must keep accepting the same lifecycle")
		var upsertQuoted int
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT end FROM end_cycle WHERE id = 1").Scan(&upsertQuoted))
		require.Equal(t, 12, upsertQuoted, "the quoted END conflict accessor must apply its value")
		_, rerr := conn.ExecContext(ctx, "UPDATE end_cycle SET end = 7 WHERE id = 1")
		require.NoError(t, rerr, "restore the seeded value so the later rename and change-back assertions keep their exact rows")
		var cols int
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = ? AND table_name = 'end_cycle' AND column_name = 'tail'", testDB).Scan(&cols))
		require.Equal(t, 1, cols, "the column added AFTER the bare reference must exist")
		var tailOrdinal int
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT ORDINAL_POSITION FROM information_schema.columns WHERE table_schema = ? AND table_name = 'end_cycle' AND column_name = 'tail'", testDB).Scan(&tailOrdinal))
		require.Equal(t, 3, tailOrdinal, "AFTER end must place the new column behind the bare END reference")
		for _, step := range []struct {
			name string
			sql  string
		}{
			{"bare_end_rename", "ALTER TABLE end_cycle RENAME COLUMN end TO finish"},
		} {
			_, err := conn.ExecContext(ctx, step.sql)
			require.NoError(t, err, "lifecycle step %s must execute: %s", step.name, step.sql)
		}
		renamedRows, rerr := conn.QueryContext(ctx, "SELECT finish FROM end_cycle ORDER BY id")
		require.NoError(t, rerr, "the renamed column must be readable by its new name")
		defer renamedRows.Close()
		for _, want := range []int{7, 8, 7} {
			require.True(t, renamedRows.Next(), "the renamed column must keep its rows")
			var got int
			require.NoError(t, renamedRows.Scan(&got))
			require.Equal(t, want, got, "renamed column values stay in id order")
		}
		require.False(t, renamedRows.Next(), "the renamed column holds exactly the seeded rows")
		require.NoError(t, renamedRows.Err())
		_, cerr2 := conn.ExecContext(ctx, "ALTER TABLE end_cycle CHANGE COLUMN finish end BIGINT")
		require.NoError(t, cerr2, "lifecycle step bare_finish_change_back must execute")
		var endType string
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT DATA_TYPE FROM information_schema.columns WHERE table_schema = ? AND table_name = 'end_cycle' AND column_name = 'end'", testDB).Scan(&endType))
		require.True(t, strings.EqualFold(endType, "bigint"), "the change-back must bind the BIGINT catalog type, got %q", endType)
		changedRows, eerr2 := conn.QueryContext(ctx, "SELECT end FROM end_cycle ORDER BY id")
		require.NoError(t, eerr2, "the changed-back column must be readable by its bare name")
		defer changedRows.Close()
		for _, want := range []int{7, 8, 7} {
			require.True(t, changedRows.Next(), "the changed-back column must keep its rows")
			var got int
			require.NoError(t, changedRows.Scan(&got))
			require.Equal(t, want, got, "changed-back column values stay in id order")
		}
		require.False(t, changedRows.Next(), "the changed-back column holds exactly the seeded rows")
		require.NoError(t, changedRows.Err())
		_, derr2 := conn.ExecContext(ctx, "ALTER TABLE end_cycle DROP COLUMN end")
		require.NoError(t, derr2, "lifecycle step bare_end_drop_the_primary_gap must execute")
		var dropped int
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = ? AND table_name = 'end_cycle' AND column_name = 'end'", testDB).Scan(&dropped))
		require.Zero(t, dropped, "the bare DROP COLUMN end must have removed the column")
		survRows, serr2 := conn.QueryContext(ctx, "SELECT id, tail FROM end_cycle ORDER BY id")
		require.NoError(t, serr2, "surviving rows must read the exact ordered values")
		defer survRows.Close()
		for _, wantID := range []int{1, 2, 3} {
			require.True(t, survRows.Next(), "each surviving row must be present in id order")
			var gotID int
			var tail sql.NullInt64
			require.NoError(t, survRows.Scan(&gotID, &tail))
			require.Equal(t, wantID, gotID, "surviving row order is by id")
			require.False(t, tail.Valid, "the surviving tail values are typed NULLs, not defaults")
		}
		require.False(t, survRows.Next(), "all surviving rows must be enumerated")
		require.NoError(t, survRows.Err())
	})
}

// A parenthesized WITH-bearing source reaches INSERT, CTAS and view
// consumers with its outer lexical scope: the CTE defined by the source
// shadows any same-name physical table, and its absence must resolve to the
// declared CTE instead of a missing-relation error. The unwrapped forms and
// the truly-absent relation keep their existing contracts.
func tryExec(conn *sql.Conn, ctx context.Context, stmt string) error {
	_, err := conn.ExecContext(ctx, stmt)
	return err
}

func TestParenthesizedWithSourceConsumerScopeSQL(t *testing.T) {
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

		const testDB = "review6002629771_with_source_scope"
		var preexisting int
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", testDB).Scan(&preexisting))
		require.Zero(t, preexisting, "owned database name must not preexist: %s", testDB)
		_, err = conn.ExecContext(ctx, "CREATE DATABASE "+testDB+"; USE "+testDB)
		require.NoError(t, err, "multi-statement create-database request")
		defer cleanupSQLIntegration(t, cn, "DROP DATABASE IF EXISTS "+testDB)

		// Each consumer runs against its own CTE identifier, and every
		// absence scenario proves the identifier is truly absent through the
		// catalog immediately before the consumer runs; every homonym
		// scenario proves its physical table exists with a contrasting row
		// and a distinguishable BIGINT type so metadata cannot silently be
		// inferred from the physical source.
		requireTableCount := func(t *testing.T, name string, want int, why string) {
			t.Helper()
			var got int
			require.NoError(t, conn.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", testDB, name).Scan(&got),
				"the catalog lookup for %s must itself work", name)
			require.Equal(t, want, got, "%s: expected the physical table %q catalogue count %d", why, name, want)
		}
		queryFirstInt := func(t *testing.T, query string) int {
			t.Helper()
			rows, qerr := conn.QueryContext(ctx, query)
			require.NoError(t, qerr, "query must plan and execute: %s", query)
			defer rows.Close()
			require.True(t, rows.Next(), "query %s must return a row", query)
			var v int
			require.NoError(t, rows.Scan(&v))
			require.False(t, rows.Next(), "query %s must return exactly one row", query)
			require.NoError(t, rows.Err())
			return v
		}
		columnType := func(t *testing.T, table, column string) string {
			t.Helper()
			var typ string
			require.NoError(t, conn.QueryRowContext(ctx,
				"SELECT DATA_TYPE FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?",
				testDB, table, column).Scan(&typ))
			return typ
		}
		source := func(cte string) string {
			return fmt.Sprintf("WITH %s AS (SELECT 7 AS a) (SELECT a FROM %s)", cte, cte)
		}
		plainSource := func(cte string) string {
			return fmt.Sprintf("WITH %s AS (SELECT 7 AS a) SELECT a FROM %s", cte, cte)
		}

		t.Run("insert_source_keeps_declared_cte_without_physical_table", func(t *testing.T) {
			requireTableCount(t, "cte1007_insert_absent", 0, "the insert absence cell")
			_, cerr := conn.ExecContext(ctx, "CREATE TABLE insert_no_homonym(a INT)")
			require.NoError(t, cerr)
			_, ierr := conn.ExecContext(ctx, "INSERT INTO insert_no_homonym "+source("cte1007_insert_absent"))
			require.NoError(t, ierr, "the parenthesized WITH source must bind through INSERT")
			require.Equal(t, 7, queryFirstInt(t, "SELECT a FROM insert_no_homonym"), "the declared CTE value reaches the INSERT target")
		})
		t.Run("insert_source_cte_shadows_physical_homonym", func(t *testing.T) {
			_, cerr := conn.ExecContext(ctx, "CREATE TABLE cte1007_insert_homonym(a BIGINT); INSERT INTO cte1007_insert_homonym VALUES (999)")
			require.NoError(t, cerr)
			requireTableCount(t, "cte1007_insert_homonym", 1, "the insert homonym cell")
			require.Equal(t, 999, queryFirstInt(t, "SELECT a FROM cte1007_insert_homonym"), "the physical homonym must exist with its contrasting value first")
			_, cerr2 := conn.ExecContext(ctx, "CREATE TABLE insert_homonym(a INT)")
			require.NoError(t, cerr2)
			_, ierr := conn.ExecContext(ctx, "INSERT INTO insert_homonym "+source("cte1007_insert_homonym"))
			require.NoError(t, ierr, "the parenthesized WITH source must bind through INSERT with the homonym present")
			require.Equal(t, 7, queryFirstInt(t, "SELECT a FROM insert_homonym"), "the declared CTE shadows the physical same-name table")
		})
		t.Run("insert_unwrapped_with_source_control", func(t *testing.T) {
			requireTableCount(t, "cte1007_insert_plain", 0, "the unwrapped insert control")
			_, cerr := conn.ExecContext(ctx, "CREATE TABLE insert_unwrapped(a INT)")
			require.NoError(t, cerr)
			_, ierr := conn.ExecContext(ctx, "INSERT INTO insert_unwrapped "+plainSource("cte1007_insert_plain"))
			require.NoError(t, ierr, "the unwrapped WITH source keeps working")
			require.Equal(t, 7, queryFirstInt(t, "SELECT a FROM insert_unwrapped"))
		})
		t.Run("ctas_source_keeps_declared_cte_without_physical_table", func(t *testing.T) {
			requireTableCount(t, "cte1007_ctas_absent", 0, "the CTAS absence cell")
			_, cerr := conn.ExecContext(ctx, "CREATE TABLE ctas_no_homonym AS "+source("cte1007_ctas_absent"))
			require.NoError(t, cerr, "the parenthesized WITH source must bind through CTAS inference")
			require.True(t, strings.EqualFold(columnType(t, "ctas_no_homonym", "a"), "bigint"),
				"CTAS must infer the CTE's own integer column type, got %q", columnType(t, "ctas_no_homonym", "a"))
			require.Equal(t, 7, queryFirstInt(t, "SELECT a FROM ctas_no_homonym"), "the declared CTE value reaches the created table")
		})
		t.Run("ctas_source_cte_shadows_physical_homonym", func(t *testing.T) {
			_, cerr := conn.ExecContext(ctx, "CREATE TABLE cte1007_ctas_homonym(a VARCHAR(8)); INSERT INTO cte1007_ctas_homonym VALUES ('999')")
			require.NoError(t, cerr)
			requireTableCount(t, "cte1007_ctas_homonym", 1, "the CTAS homonym cell")
			var physicalContrast string
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT a FROM cte1007_ctas_homonym").Scan(&physicalContrast))
			require.Equal(t, "999", physicalContrast, "the physical homonym's contrasting value is proven present first")
			require.True(t, strings.EqualFold(columnType(t, "cte1007_ctas_homonym", "a"), "varchar"),
				"the physical homonym keeps its distinguishable VARCHAR type")
			_, cerr2 := conn.ExecContext(ctx, "CREATE TABLE ctas_homonym AS "+source("cte1007_ctas_homonym"))
			require.NoError(t, cerr2, "the homonym stays irrelevant to the declared CTE")
			require.True(t, strings.EqualFold(columnType(t, "ctas_homonym", "a"), "bigint"),
				"the created table's inferred type comes from the CTE, not the VARCHAR physical homonym; got %q", columnType(t, "ctas_homonym", "a"))
			require.Equal(t, 7, queryFirstInt(t, "SELECT a FROM ctas_homonym"))
		})
		t.Run("view_source_keeps_declared_cte_and_reads_back", func(t *testing.T) {
			requireTableCount(t, "cte1007_view_absent", 0, "the view absence cell")
			_, cerr := conn.ExecContext(ctx, "CREATE VIEW view_no_homonym AS "+source("cte1007_view_absent"))
			require.NoError(t, cerr, "the parenthesized WITH source must bind through the view definition")
			require.True(t, strings.EqualFold(columnType(t, "view_no_homonym", "a"), "bigint"),
				"the persisted view's column type comes from the CTE's own integer expression; got %q", columnType(t, "view_no_homonym", "a"))
			var defCount int
			require.NoError(t, conn.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM information_schema.views WHERE table_schema = ? AND table_name = 'view_no_homonym'", testDB).Scan(&defCount))
			require.Equal(t, 1, defCount, "the view definition persists in the catalog")
			require.Equal(t, 7, queryFirstInt(t, "SELECT a FROM view_no_homonym"), "the persisted view reads the declared CTE value")
		})
		t.Run("view_source_cte_shadows_physical_homonym", func(t *testing.T) {
			_, cerr := conn.ExecContext(ctx, "CREATE TABLE cte1007_view_homonym(a VARCHAR(8)); INSERT INTO cte1007_view_homonym VALUES ('999')")
			require.NoError(t, cerr)
			requireTableCount(t, "cte1007_view_homonym", 1, "the view homonym cell")
			_, cerr2 := conn.ExecContext(ctx, "CREATE VIEW view_homonym AS "+source("cte1007_view_homonym"))
			require.NoError(t, cerr2)
			require.True(t, strings.EqualFold(columnType(t, "view_homonym", "a"), "bigint"),
				"the view's inferred type comes from the CTE, not the VARCHAR physical homonym; got %q", columnType(t, "view_homonym", "a"))
			require.Equal(t, 7, queryFirstInt(t, "SELECT a FROM view_homonym"))
		})
		t.Run("alter_view_alias_binds_the_complete_source_too", func(t *testing.T) {
			requireTableCount(t, "cte1007_alter_absent", 0, "the ALTER VIEW alias cell")
			_, cerr := conn.ExecContext(ctx, "CREATE TABLE alter_target_tbl(a INT); INSERT INTO alter_target_tbl VALUES (1)")
			require.NoError(t, cerr)
			_, verr := conn.ExecContext(ctx, "CREATE VIEW view_altered AS SELECT a FROM alter_target_tbl")
			require.NoError(t, verr)
			_, aerr := conn.ExecContext(ctx, "ALTER VIEW view_altered AS "+source("cte1007_alter_absent"))
			require.NoError(t, aerr, "the ALTER VIEW alias must bind the complete WITH-bearing source like CREATE VIEW")
			require.Equal(t, 7, queryFirstInt(t, "SELECT a FROM view_altered"), "the redefined view reads the declared CTE value")
		})
		t.Run("replace_source_collision_shares_the_complete_source_route", func(t *testing.T) {
			// REPLACE reaches the INSERT/REPLACE source through the same
			// shared producer, so the parenthesized WITH-bearing source must
			// keep its lexical scope there too: the declared CTE shadows the
			// same-name physical table and the real replacement row is the
			// CTE's value, not the physical one.
			_, herr := conn.ExecContext(ctx, "CREATE TABLE cte1007_replace_homonym(a INT); INSERT INTO cte1007_replace_homonym VALUES (999)")
			require.NoError(t, herr)
			requireTableCount(t, "cte1007_replace_homonym", 1, "the REPLACE homonym cell")
			_, cerr := conn.ExecContext(ctx, "CREATE TABLE replace_target(id INT PRIMARY KEY, a INT)")
			require.NoError(t, cerr)
			_, serr := conn.ExecContext(ctx, "INSERT INTO replace_target VALUES (1, 888)")
			require.NoError(t, serr)
			_, rerr := conn.ExecContext(ctx,
				"REPLACE INTO replace_target WITH cte1007_replace_homonym AS (SELECT 7 AS x) (SELECT 1, x FROM cte1007_replace_homonym)")
			require.NoError(t, rerr, "the parenthesized WITH source must bind through REPLACE's shared source route")
			require.Equal(t, 7, queryFirstInt(t, "SELECT a FROM replace_target WHERE id = 1"),
				"the real replacement row carries the declared CTE value, not the physical homonym's 999")
			var kept int
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM replace_target").Scan(&kept))
			require.Equal(t, 1, kept, "the REPLACE replaced exactly the conflicting key")
		})
		t.Run("recursive_seed_pending_enclosing_declarations_missing_without_physical", func(t *testing.T) {
			// The reviewed witness: inside the recursive body reader_cte precedes
			// the inner pending_x (still pending there), while the outer pending_x
			// follows q (still pending in the enclosing scope). Without a physical
			// pending_x the seed must see neither later declaration: typed 1146.
			const witness = "WITH RECURSIVE q(n) AS (WITH reader_cte AS (SELECT a FROM pending_x), pending_x AS (SELECT 8 AS a) (SELECT a FROM reader_cte UNION ALL SELECT n+1 FROM q WHERE n < 10)), pending_x AS (SELECT 9 AS a) SELECT n FROM q"
			var probe int
			aerr := conn.QueryRowContext(ctx, witness).Scan(&probe)
			require.Error(t, aerr, "the recursive seed must not see either still-pending pending_x declaration")
			var myErr *gomysql.MySQLError
			require.True(t, errors.As(aerr, &myErr), "expected a typed mysql error, got: %v", aerr)
			require.Equal(t, uint16(1146), myErr.Number, "a still-pending enclosing declaration stays invisible to the seed")
			var stored int
			require.NoError(t, conn.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM mo_catalog.mo_tables WHERE reldatabase = ? AND relname = 'pending_x'", testDB).Scan(&stored))
			require.Zero(t, stored, "the failed plan must not leave any persisted side effect")
		})
		t.Run("recursive_seed_pending_falls_back_to_real_physical_with_full_rows", func(t *testing.T) {
			// A genuinely present physical pending_x legally serves the seed; its
			// value (1) is distinguishable from both later declarations (8 inner,
			// 9 outer) and the full ordered recursive row bag must come back.
			_, herr := conn.ExecContext(ctx, "CREATE TABLE pending_x(a INT); INSERT INTO pending_x VALUES (1)")
			require.NoError(t, herr)
			var got []int
			func() {
				rows, rerr := conn.QueryContext(ctx,
					"WITH RECURSIVE q(n) AS (WITH reader_cte AS (SELECT a FROM pending_x), pending_x AS (SELECT 8 AS a) (SELECT a FROM reader_cte UNION ALL SELECT n+1 FROM q WHERE n < 10)), pending_x AS (SELECT 9 AS a) SELECT n FROM q ORDER BY n")
				require.NoError(t, rerr, "the physical fallback serves the recursive seed")
				defer rows.Close()
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				require.NoError(t, rows.Err())
			}()
			require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, got,
				"the full recursive bag rows come from the physical seed 1, not the inner 8 or outer 9")
			_, derr := conn.ExecContext(ctx, "DROP TABLE pending_x")
			require.NoError(t, derr)
		})
		t.Run("recursive_seed_earlier_sibling_stays_visible_on_recursive_path", func(t *testing.T) {
			// The earlier same-scope declaration stays visible to the seed on the
			// real recursive path, with the full ordered bag.
			var got []int
			func() {
				rows, rerr := conn.QueryContext(ctx,
					"WITH RECURSIVE q(n) AS (WITH earlier_cte AS (SELECT 1 AS a), reader_cte AS (SELECT a FROM earlier_cte) (SELECT a FROM reader_cte UNION ALL SELECT n+1 FROM q WHERE n < 3)) SELECT n FROM q ORDER BY n")
				require.NoError(t, rerr, "an earlier declaration is visible to the recursive seed")
				defer rows.Close()
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				require.NoError(t, rows.Err())
			}()
			require.Equal(t, []int{1, 2, 3}, got)
		})
		t.Run("inner_same_spelling_with_q_does_not_make_outer_recursive", func(t *testing.T) {
			rows, rerr := conn.QueryContext(ctx,
				"WITH q AS (WITH q AS (SELECT 7 AS a) (SELECT 1 AS a UNION ALL SELECT a FROM q)) SELECT a FROM q ORDER BY a")
			require.NoError(t, rerr, "the union reference resolves to the inner declaration, so the outer q is an ordinary CTE")
			defer rows.Close()
			var got []int
			for rows.Next() {
				var v int
				require.NoError(t, rows.Scan(&v))
				got = append(got, v)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []int{1, 7}, got, "the resolved identity drives classification: exactly 1 and 7 with multiplicity")
		})
		t.Run("shadowed_inner_q_with_joined_union_member_stays_ordinary", func(t *testing.T) {
			_, jerr := conn.ExecContext(ctx, "CREATE TABLE join_side(b INT); INSERT INTO join_side VALUES (70)")
			require.NoError(t, jerr)
			rows, rerr := conn.QueryContext(ctx,
				"WITH q AS (WITH q AS (SELECT 7 AS a) (SELECT 1 AS a UNION ALL SELECT q.a FROM q LEFT JOIN join_side ON q.a = join_side.b)) SELECT a FROM q ORDER BY a")
			require.NoError(t, rerr, "a joined member reading the shadowed inner q must not trip any recursive-only guard")
			defer rows.Close()
			var got []int
			for rows.Next() {
				var v int
				require.NoError(t, rows.Scan(&v))
				got = append(got, v)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []int{1, 7}, got, "the joined shadowed member binds as an ordinary reference")
		})
		t.Run("true_recursive_join_member_prohibition_stays_typed", func(t *testing.T) {
			_, jerr := conn.ExecContext(ctx, "CREATE TABLE join_side2(b INT); INSERT INTO join_side2 VALUES (70)")
			require.NoError(t, jerr)
			var probe int
			aerr := conn.QueryRowContext(ctx,
				"WITH RECURSIVE q(n) AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM q LEFT JOIN join_side2 ON q.n = join_side2.b) SELECT n FROM q").Scan(&probe)
			require.Error(t, aerr, "a genuine recursive member with an outer join keeps its prohibition")
			var myErr *gomysql.MySQLError
			require.True(t, errors.As(aerr, &myErr), "expected a typed mysql error, got: %v", aerr)
			require.Equal(t, uint16(1064), myErr.Number, "the recursive-member outer-join prohibition keeps its typed parse-error code")
			require.Contains(t, strings.ToLower(myErr.Message), "outer join", "the diagnostic names the outer-join contract: %s", myErr.Message)
		})
		t.Run("recursive_seed_prefers_visible_oldest_after_two_pending_scopes", func(t *testing.T) {
			// The outermost pending_x=4 is declared before everything, so it is
			// visible to the seed even though both nearer same-name
			// declarations (the inner 8 after reader_cte, the wrapper's 9 after
			// q) are still pending there. No physical table is involved.
			var got []int
			func() {
				rows, rerr := conn.QueryContext(ctx,
					"WITH pending_x AS (SELECT 4 AS a), wrapper AS (WITH RECURSIVE q(n) AS (WITH reader_cte AS (SELECT a FROM pending_x), pending_x AS (SELECT 8 AS a) (SELECT a FROM reader_cte UNION ALL SELECT n+1 FROM q WHERE n < 6)), pending_x AS (SELECT 9 AS a) SELECT n FROM q) SELECT n FROM wrapper ORDER BY n")
				require.NoError(t, rerr, "the visible oldest definition must serve the seed through both pending scopes")
				defer rows.Close()
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				require.NoError(t, rows.Err())
			}()
			require.Equal(t, []int{4, 5, 6}, got,
				"the seed reads the visible oldest pending_x=4, never the still-pending 8 or 9")
		})
		t.Run("true_nonrecursive_self_reference_still_rejected", func(t *testing.T) {
			var probe int
			aerr := conn.QueryRowContext(ctx,
				"WITH q AS (SELECT 1 AS a UNION ALL SELECT a+1 FROM q) SELECT a FROM q").Scan(&probe)
			require.Error(t, aerr, "an unresolved genuine self-reference without RECURSIVE stays rejected")
			var myErr *gomysql.MySQLError
			require.True(t, errors.As(aerr, &myErr), "expected a typed mysql error, got: %v", aerr)
			require.Equal(t, uint16(1064), myErr.Number, "the not-recursive declaration error keeps its typed parse-error code")
			require.Contains(t, myErr.Message, "not declare RECURSIVE", "the observed diagnostic names the missing RECURSIVE declaration: %s", myErr.Message)
		})
		t.Run("true_recursive_q_with_both_union_shapes_still_works", func(t *testing.T) {
			for _, shape := range []string{
				"WITH RECURSIVE q(n) AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM q WHERE n < 3) SELECT n FROM q ORDER BY n",
				"WITH RECURSIVE q(n) AS (SELECT 1 AS n UNION DISTINCT SELECT n+1 FROM q WHERE n < 3) SELECT n FROM q ORDER BY n",
			} {
				var bag []int
				func() {
					rows, rerr := conn.QueryContext(ctx, shape)
					require.NoError(t, rerr, "the genuine recursive control keeps working: %s", shape)
					defer rows.Close()
					for rows.Next() {
						var v int
						require.NoError(t, rows.Scan(&v))
						bag = append(bag, v)
					}
					require.NoError(t, rows.Err())
				}()
				require.Equal(t, []int{1, 2, 3}, bag, "the full ordered recursive bag with multiplicity: %s", shape)
			}
		})
		assertTableAbsent := func(t *testing.T, name string, why string) {
			t.Helper()
			var stored int
			require.NoError(t, conn.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM mo_catalog.mo_tables WHERE reldatabase = ? AND relname = ?", testDB, name).Scan(&stored),
				"the catalog lookup for %s must itself work", name)
			require.Zero(t, stored, "%s: no persisted DDL row may exist before any cleanup", why)
		}
		typedCreateFailure := func(t *testing.T, sql, name, why string) *gomysql.MySQLError {
			t.Helper()
			_, cerr := conn.ExecContext(ctx, sql)
			require.Error(t, cerr, "%s", why)
			var myErr *gomysql.MySQLError
			require.True(t, errors.As(cerr, &myErr), "expected a typed mysql error for %s, got: %v", name, cerr)
			assertTableAbsent(t, name, why)
			return myErr
		}
		t.Run("invalid_charset_rejected_before_persistence_single_statement", func(t *testing.T) {
			myErr := typedCreateFailure(t,
				"CREATE TABLE ctas_bad_charset CHARSET 'test charset' AS WITH seed_cte AS (SELECT 7 AS a) (SELECT a FROM seed_cte)",
				"ctas_bad_charset", "an unsupported quoted charset must fail the create-time validation")
			require.Equal(t, uint16(20301), myErr.Number)
			require.Contains(t, myErr.Message, "unsupported character set 'test charset'",
				"the typed diagnostic names the exact unsupported charset value: %s", myErr.Message)
		})
		t.Run("invalid_charset_rejected_before_persistence_multi_statement", func(t *testing.T) {
			myErr := typedCreateFailure(t,
				"CREATE TABLE ctas_bad_charset_ms CHARSET 'test charset' AS SELECT 1 AS a; SELECT 1 AS a",
				"ctas_bad_charset_ms", "the multi-statement canonical route rejects the unsupported charset before persisting too")
			require.Equal(t, uint16(20301), myErr.Number)
			require.Contains(t, myErr.Message, "unsupported character set 'test charset'",
				"the multi-statement route carries the same exact value diagnostic: %s", myErr.Message)
		})
		t.Run("invalid_collate_with_valid_charset_observed_exactly", func(t *testing.T) {
			// With a valid charset, an invalid quoted collation is exercised on
			// its own: observe the exact typed behavior (or persistence) rather
			// than letting charset validation answer first.
			myErr := typedCreateFailure(t,
				"CREATE TABLE ctas_bad_collate CHARSET 'utf8mb4' COLLATE 'test collation' AS WITH seed_cte AS (SELECT 7 AS a) (SELECT a FROM seed_cte)",
				"ctas_bad_collate", "an unsupported quoted collation must not silently persist")
			require.Equal(t, uint16(20301), myErr.Number, "the collation validation keeps its typed number")
			require.Contains(t, myErr.Message, "unsupported collation 'test collation'",
				"the typed diagnostic names the exact unsupported collation value: %s", myErr.Message)
		})
		t.Run("invalid_collate_rejected_before_persistence_multi_statement", func(t *testing.T) {
			myErr := typedCreateFailure(t,
				"CREATE TABLE ctas_bad_collate_ms CHARSET 'utf8mb4' COLLATE 'test collation' AS SELECT 1 AS a; SELECT 1 AS a",
				"ctas_bad_collate_ms", "the multi-statement route must not persist the unsupported collation either")
			require.Equal(t, uint16(20301), myErr.Number, "the multi-statement collation rejection keeps the same typed number")
			require.Contains(t, myErr.Message, "unsupported collation 'test collation'",
				"the multi-statement route carries the exact collation value diagnostic: %s", myErr.Message)
		})
		t.Run("quoted_valid_charset_persists_and_replays_through_catalog", func(t *testing.T) {
			_, cerr := conn.ExecContext(ctx, "CREATE TABLE ctas_charset_ok CHARSET 'utf8mb4' AS WITH seed_cte AS (SELECT 7 AS a) (SELECT a FROM seed_cte); SELECT 42 AS sentinel")
			require.NoError(t, cerr, "a quoted but supported charset value persists on CTAS")
			var createsql string
			require.NoError(t, conn.QueryRowContext(ctx,
				"SELECT rel_createsql FROM mo_catalog.mo_tables WHERE reldatabase = ? AND relname = 'ctas_charset_ok'", testDB).Scan(&createsql),
				"the canonical catalog DDL must be readable")
			t.Logf("stored DDL: %s", createsql)
			require.Contains(t, strings.ToLower(createsql), "charset", "the stored canonical DDL records the charset option")
			require.Contains(t, strings.ToLower(createsql), "utf8mb4", "the stored canonical DDL records the decoded utf8mb4 charset value")
			s1, perr := parsers.ParseOne(t.Context(), dialect.MYSQL, createsql, 1)
			require.NoError(t, perr, "the canonical stored DDL must parse back")
			ct1, isCT := s1.(*tree.CreateTable)
			require.True(t, isCT, "the canonical stored DDL is a CREATE TABLE")
			sawCharset := ""
			for _, opt := range ct1.Options {
				if cs, isC := opt.(*tree.TableOptionCharset); isC && !cs.Convert {
					sawCharset = cs.Charset
				}
			}
			require.Equal(t, "utf8mb4", sawCharset, "the canonical producer parses back to the exact typed utf8mb4 charset value")
			require.NoError(t, tryExec(conn, ctx, "DROP TABLE ctas_charset_ok"), "release the original so the stored DDL replays under its own name")
			_, rerr := conn.ExecContext(ctx, "USE "+testDB+"; "+createsql)
			require.NoError(t, rerr, "the stored canonical DDL must reparse and replay through the real definition consumer")
			var v int
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT a FROM ctas_charset_ok").Scan(&v))
			require.Equal(t, 7, v, "the replayed table carries the source rows")
			var readd string
			require.NoError(t, conn.QueryRowContext(ctx,
				"SELECT rel_createsql FROM mo_catalog.mo_tables WHERE reldatabase = ? AND relname = 'ctas_charset_ok'", testDB).Scan(&readd),
				"the second catalog read after the replay must succeed")
			require.Contains(t, strings.ToLower(readd), "utf8mb4", "the replayed table's own stored DDL keeps the typed charset value")
		})
		t.Run("quoted_valid_charset_with_compatible_collation_persists_and_replays", func(t *testing.T) {
			_, cerr := conn.ExecContext(ctx,
				"CREATE TABLE ctas_collate_ok CHARSET 'utf8mb4' COLLATE 'utf8mb4_general_ci' AS WITH seed_cte AS (SELECT 7 AS a) (SELECT a FROM seed_cte); SELECT 42 AS sentinel")
			require.NoError(t, cerr, "a quoted supported charset with a compatible quoted collation persists on CTAS")
			var createsql string
			require.NoError(t, conn.QueryRowContext(ctx,
				"SELECT rel_createsql FROM mo_catalog.mo_tables WHERE reldatabase = ? AND relname = 'ctas_collate_ok'", testDB).Scan(&createsql))
			t.Logf("stored DDL: %s", createsql)
			require.Contains(t, strings.ToLower(createsql), "collate", "the stored canonical DDL records the collation option")
			require.Contains(t, strings.ToLower(createsql), "utf8mb4_general_ci", "the stored canonical DDL records the decoded collation value")
			s2, perr2 := parsers.ParseOne(t.Context(), dialect.MYSQL, createsql, 1)
			require.NoError(t, perr2, "the canonical stored DDL must parse back")
			ct2, isCT2 := s2.(*tree.CreateTable)
			require.True(t, isCT2, "the canonical stored DDL is a CREATE TABLE")
			sawCharset2, sawCollate2 := "", ""
			for _, opt := range ct2.Options {
				if cs, isC := opt.(*tree.TableOptionCharset); isC && !cs.Convert {
					sawCharset2 = cs.Charset
				}
				if co, isL := opt.(*tree.TableOptionCollate); isL {
					sawCollate2 = co.Collate
				}
			}
			require.Equal(t, "utf8mb4", sawCharset2, "the canonical producer parses back to the exact typed utf8mb4 charset value")
			require.Equal(t, "utf8mb4_general_ci", sawCollate2, "the canonical producer parses back to the exact typed collation value")
			require.NoError(t, tryExec(conn, ctx, "DROP TABLE ctas_collate_ok"))
			_, rerr := conn.ExecContext(ctx, "USE "+testDB+"; "+createsql)
			require.NoError(t, rerr, "the stored canonical DDL with charset and collation must reparse and replay")
			var v int
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT a FROM ctas_collate_ok").Scan(&v))
			require.Equal(t, 7, v)
			var readd string
			require.NoError(t, conn.QueryRowContext(ctx,
				"SELECT rel_createsql FROM mo_catalog.mo_tables WHERE reldatabase = ? AND relname = 'ctas_collate_ok'", testDB).Scan(&readd),
				"the second catalog read after the replay must succeed")
			require.Contains(t, strings.ToLower(readd), "utf8mb4_general_ci", "the replayed table's own stored DDL keeps the collation value")
		})
		t.Run("conventional_valid_charset_collate_control", func(t *testing.T) {
			_, cerr := conn.ExecContext(ctx, "CREATE TABLE plain_charset_ok (a INT) CHARSET 'utf8mb4'")
			require.NoError(t, cerr, "a conventional valid charset keeps working on CTAS/CREATE")
			_, derr := conn.ExecContext(ctx, "DROP TABLE plain_charset_ok")
			require.NoError(t, derr)
		})
		t.Run("truly_absent_relation_keeps_typical_missing_table_error", func(t *testing.T) {
			var probe int
			aerr := conn.QueryRowContext(ctx, "SELECT a FROM never_created_table_6002629771").Scan(&probe)
			require.Error(t, aerr, "a genuinely absent relation must still fail")
			var myErr *gomysql.MySQLError
			require.True(t, errors.As(aerr, &myErr), "expected a typed mysql error for the missing table, got: %v", aerr)
			require.Equal(t, uint16(1146), myErr.Number, "typed missing-table error remains the contract")
		})
	})
}
