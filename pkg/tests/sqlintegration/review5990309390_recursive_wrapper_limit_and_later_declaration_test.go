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
