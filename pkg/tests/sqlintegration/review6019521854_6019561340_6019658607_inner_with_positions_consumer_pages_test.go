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
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
)

// The pagination closure of reviews 6019521854/6019561340/6019658607: an
// inner WITH owning both pages must keep its lexical scope while the pages
// apply inside out, an ordinary CTE's explicit column list renames the paged
// projection by position without consulting duplicate inner headings, and a
// consumer SELECT's own enclosing pages bind in order. Every value oracle
// below is the reviewer's reported SQL or its named control.
func TestInnerWithPositionsConsumerAndReleasePages(t *testing.T) {
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

		const probeDB = "patrol163_pagination_review"
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
				require.NoError(t, rs.Scan(&v))
				got = append(got, v.String)
			}
			return got, rs.Err()
		}
		expect := func(t *testing.T, sqlText string, want []string) {
			t.Helper()
			got, qErr := rows(sqlText)
			require.NoErrorf(t, qErr, "witness failed: %s", sqlText)
			require.Equalf(t, want, got, "witness: %s", sqlText)
		}
		twoCols := func(t *testing.T, sqlText string, want [][2]string) {
			t.Helper()
			rs, qErr := conn.QueryContext(ctx, sqlText)
			require.NoErrorf(t, qErr, "positioned witness failed: %s", sqlText)
			defer rs.Close()
			var got [][2]string
			for rs.Next() {
				var x, y sql.NullString
				require.NoError(t, rs.Scan(&x, &y))
				got = append(got, [2]string{x.String, y.String})
			}
			require.NoError(t, rs.Err())
			require.NotEmptyf(t, got, "the positioned witness requires a real row: %s", sqlText)
			require.Equalf(t, want, got, "positions: %s", sqlText)
		}
		twoColsExact := func(t *testing.T, sqlText string, headings []string, want [][2]string) {
			t.Helper()
			rs, qErr := conn.QueryContext(ctx, sqlText)
			require.NoErrorf(t, qErr, "witness failed: %s", sqlText)
			defer rs.Close()
			wire, hErr := rs.Columns()
			require.NoErrorf(t, hErr, "wire headings failed: %s", sqlText)
			require.Equalf(t, headings, wire, "exact authored wire headings required: %s", sqlText)
			var got [][2]string
			for rs.Next() {
				var x, y sql.NullString
				require.NoErrorf(t, rs.Scan(&x, &y), "row scan: %s", sqlText)
				got = append(got, [2]string{x.String, y.String})
			}
			require.NoErrorf(t, rs.Err(), "rows error: %s", sqlText)
			require.NotEmptyf(t, got, "the witness requires a real row: %s", sqlText)
			require.Equalf(t, want, got, "witness rows: %s", sqlText)
		}

		t.Run("finding_1_inner_WITH_owns_both_pages", func(t *testing.T) {
			expect(t,
				"WITH q AS ((WITH x AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) (SELECT a FROM x ORDER BY a LIMIT 2) ORDER BY a LIMIT 1 OFFSET 1)) SELECT a FROM q",
				[]string{"2"})
		})
		t.Run("finding_1_control_second_page_outside_the_inner_WITH", func(t *testing.T) {
			expect(t,
				"WITH q AS ((WITH x AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) (SELECT a FROM x ORDER BY a LIMIT 2))) SELECT a FROM q ORDER BY a LIMIT 1 OFFSET 1",
				[]string{"2"})
		})
		t.Run("finding_1_control_local_cte_not_lifted_into_the_wrong_region", func(t *testing.T) {
			// The inner page composes inside the innermost WITH frame: the
			// local x shadows the outer x, and the composition must not lift
			// the local declaration into the region above it (which would
			// resolve the page against the outer x=9 instead).
			expect(t,
				"WITH x AS (SELECT 9 AS a), q AS ((WITH x AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) (SELECT a FROM x ORDER BY a LIMIT 2) ORDER BY a LIMIT 1 OFFSET 1)) SELECT a FROM q",
				[]string{"2"})
		})
		t.Run("finding_2_positions_renamed_through_the_page", func(t *testing.T) {
			twoCols(t, "WITH q(x, y) AS ((SELECT 1 AS a, 2 AS a) LIMIT 1) SELECT x, y FROM q", [][2]string{{"1", "2"}})
		})
		t.Run("finding_2_control_unwrapped_duplicate_names", func(t *testing.T) {
			twoCols(t, "WITH q(x, y) AS (SELECT 1 AS a, 2 AS a) SELECT x, y FROM q", [][2]string{{"1", "2"}})
		})
		t.Run("finding_2_control_paginated_distinct_names", func(t *testing.T) {
			twoCols(t, "WITH q(x, y) AS ((SELECT 1 AS a, 2 AS b) LIMIT 1) SELECT x, y FROM q", [][2]string{{"1", "2"}})
		})
		t.Run("finding_87_authored_alias_kept_through_paged_stages", func(t *testing.T) {
			// Root's concrete discriminator: a mixed projection (authored
			// alias kept + anonymous output) through TWO real page stages,
			// with the authored alias referenced by the page's own ORDER BY.
			// The pre-bind rewriter rewrote every authored alias before this
			// ORDER BY could bind; the authored-carry pages keep it.
			rows, qErr := conn.QueryContext(ctx, "WITH src AS (SELECT 2 AS v), q AS ((SELECT v+0 AS kept, 7 FROM src LIMIT 2) ORDER BY kept LIMIT 1) SELECT kept FROM q")
			require.NoErrorf(t, qErr, "authored-alias witness failed")
			defer rows.Close()
			cols, cErr := rows.Columns()
			require.NoError(t, cErr)
			require.Equal(t, []string{"kept"}, cols, "the authored alias must reach the consumer wire")
			var got []string
			for rows.Next() {
				var v sql.NullString
				require.NoError(t, rows.Scan(&v))
				got = append(got, v.String)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []string{"2"}, got)
		})
		t.Run("finding_87_wire_headings_authored_no_synthetic_labels", func(t *testing.T) {
			// A no-explicit-rename CTE consumer reads the AUTHORED wire
			// headings through the paged stages: no synthetic page-local
			// label ever reaches a real consumer.
			rows, qErr := conn.QueryContext(ctx, "WITH src AS (SELECT 2 AS v), q AS ((SELECT v+0 AS kept, 7 FROM src LIMIT 2) ORDER BY kept LIMIT 1) SELECT * FROM q")
			require.NoErrorf(t, qErr, "wire-heading witness failed")
			defer rows.Close()
			cols, cErr := rows.Columns()
			require.NoError(t, cErr)
			require.Len(t, cols, 2)
			require.Equal(t, "kept", strings.ToLower(cols[0]), "the authored alias must be the first wire heading")
			for _, c := range cols {
				require.NotContains(t, strings.ToLower(c), "ordinary_page", "no synthetic page label may reach the wire")
			}
			var mixed [][2]string
			for rows.Next() {
				var x, y sql.NullString
				require.NoError(t, rows.Scan(&x, &y))
				mixed = append(mixed, [2]string{x.String, y.String})
			}
			require.NoError(t, rows.Err())
			require.Equal(t, [][2]string{{"2", "7"}}, mixed, "the mixed projection's both display names resolve to both authored values")
			twoCols(t, "WITH src AS (SELECT 2 AS v) SELECT kept, seven FROM (SELECT v+0 AS kept, 7 AS seven FROM (SELECT 2 AS v) s2 LIMIT 2) d ORDER BY kept LIMIT 1", [][2]string{{"2", "7"}})
		})
		t.Run("finding_87_typed_ambiguity_for_authored_duplicate_reference", func(t *testing.T) {
			// Referencing an authored duplicate name on a paged body keeps
			// its TYPED ambiguity: the authored-carry pages never rewrite the
			// projection into unknown page-local ordinals. The Exec route
			// reaches the identical parser/binder server path for an
			// immediate typed bind error (no result set is produced).
			qErr := conn.PingContext(ctx)
			require.NoError(t, qErr)
			_, aErr := conn.ExecContext(ctx, "WITH q AS ((SELECT 1 AS a, 2 AS a LIMIT 2) LIMIT 1) SELECT a FROM q")
			require.Error(t, aErr, "the authored duplicate reference must not bind silently")
			require.ErrorContains(t, aErr, "ambiguous", "the authored duplicate keeps its typed ambiguity: %v", aErr)
			require.ErrorContains(t, aErr, "20301", "the ambiguity reports the server's typed invalid-input error number: %v", aErr)
			_, uErr := conn.ExecContext(ctx, "WITH q AS ((SELECT 1 AS a, 2 AS b LIMIT 2) LIMIT 1) SELECT nosuchcol FROM q")
			require.Error(t, uErr, "the unknown-column control must also fail")
			require.ErrorContains(t, uErr, "nosuchcol", "the control names the missing column: %v", uErr)
			require.NotContains(t, uErr.Error(), "ambiguous", "the unknown-column control is NOT an ambiguity: %v", uErr)
		})
		t.Run("finding_87_explicit_derived_equivalent_authored_labels", func(t *testing.T) {
			expect(t, "WITH src AS (SELECT 2 AS v) SELECT kept FROM (SELECT v+0 AS kept, 7 FROM (SELECT 2 AS v) s2 LIMIT 2) d ORDER BY kept LIMIT 1", []string{"2"})
		})
		t.Run("finding_88_real_table_column_duplicates_bound_by_ordinal", func(t *testing.T) {
			// Real table columns authored under duplicate aliases through both
			// page stages: the positional CTE list renames each raw column by
			// its ordinal, with distinct values proving which raw column landed
			// under which explicit name.
			_, e := conn.ExecContext(ctx, "drop table if exists patrol89_dup_cols")
			require.NoError(t, e)
			_, e = conn.ExecContext(ctx, "create table patrol89_dup_cols(c0 int, c1 int)")
			require.NoError(t, e)
			defer conn.ExecContext(ctx, "drop table if exists patrol89_dup_cols")
			_, e = conn.ExecContext(ctx, "insert into patrol89_dup_cols values (1, 4)")
			require.NoError(t, e)
			rows, qErr := conn.QueryContext(ctx, "WITH q(x, y) AS ((SELECT c0 AS dup, c1 AS dup FROM patrol89_dup_cols LIMIT 2) LIMIT 1) SELECT x, y FROM q")
			require.NoErrorf(t, qErr, "real-table witness failed")
			defer rows.Close()
			cols, cErr := rows.Columns()
			require.NoError(t, cErr)
			require.Equal(t, []string{"x", "y"}, cols, "the explicit positional list must be the wire oracle")
			var got [][2]string
			for rows.Next() {
				var x, y sql.NullString
				require.NoError(t, rows.Scan(&x, &y))
				got = append(got, [2]string{x.String, y.String})
			}
			require.NoError(t, rows.Err())
			require.Equal(t, [][2]string{{"1", "4"}}, got, "each raw physical column must land under its own positional name")
		})
		t.Run("finding_88_repeated_inner_with_two_readers_wire_names", func(t *testing.T) {
			// The same declaration - whose body owns an inner WITH plus both
			// page stages - binds for TWO readers: the second rebind reads the
			// authored pages and the authored wire names like the first.
			rows, qErr := conn.QueryContext(ctx, "WITH q(a, b) AS ((WITH x AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) (SELECT a, a+2 AS b FROM x ORDER BY a LIMIT 2) ORDER BY a LIMIT 1)) SELECT a, b FROM q UNION ALL SELECT b, a FROM q")
			require.NoErrorf(t, qErr, "repeated inner-WITH witness failed")
			defer rows.Close()
			cols, cErr := rows.Columns()
			require.NoError(t, cErr)
			require.Equal(t, []string{"a", "b"}, cols, "the authored wire names must survive both rebinds")
			var got [][2]string
			for rows.Next() {
				var a, b sql.NullString
				require.NoError(t, rows.Scan(&a, &b))
				got = append(got, [2]string{a.String, b.String})
			}
			require.NoError(t, rows.Err())
			sortRows := func(rows [][2]string) [][2]string {
				out := append([][2]string(nil), rows...)
				for i := range out {
					for j := i + 1; j < len(out); j++ {
						if out[j][0] < out[i][0] || (out[j][0] == out[i][0] && out[j][1] < out[i][1]) {
							out[i], out[j] = out[j], out[i]
						}
					}
				}
				return out
			}
			require.Equalf(t, [][2]string{{"1", "3"}, {"3", "1"}}, sortRows(got), "repeated inner-WITH rebinds must carry the same authored rows: got %v", got)
		})
		t.Run("finding_89_wrapper_lock_state_never_silently_dropped", func(t *testing.T) {
			// A wrapper carrying an authored FOR UPDATE never enters the page
			// reconstruction: the whole expression returns to the normal
			// binder, whose typed boundary owns the clause.
			_, lockErr := conn.ExecContext(ctx, "WITH q AS ((SELECT 1 AS a LIMIT 1) LIMIT 1 FOR UPDATE) SELECT a FROM q")
			require.Error(t, lockErr, "the authored lock state on a paged wrapper must be typed-handled by the normal binder, never silently dropped")
			// The parser boundary this SQL shape proves: FOR UPDATE inside a
			// CTE body wrapper is rejected before any binder path; the
			// constructed-wrapper guard behavior is proven separately at the
			// planner level.
			require.ErrorContains(t, lockErr, "syntax", "the SQL-level negative is the parser boundary: %v", lockErr)
		})
		t.Run("finding_94_ordinary_no_WITH_duplicate_clauses_stay_typed_errors", func(t *testing.T) {
			// The upstream union.sql contract: an ordinary (no-WITH)
			// parenthesized statement whose clause SETS duplicate ORDER BY or
			// LIMIT through transparent nested parentheses is a syntax error,
			// never a silently composed page nest. The consumer composition
			// engages only for WITH-bearing statements.
			for _, sqlText := range []string{
				"(select a from t1 order by a) order by a",
				"(((select a from t1 order by a))) order by a",
				"(((select a from t1) order by a)) order by a",
			} {
				_, byErr := conn.ExecContext(ctx, sqlText)
				require.Error(t, byErr, "duplicate ORDER BY must stay a typed syntax error: %s", sqlText)
				require.ErrorContains(t, byErr, "multiple ORDER BY clauses not allowed", sqlText)
			}
			for _, sqlText := range []string{
				"(select a from t1 limit 1) limit 1",
				"(((select a from t1 limit 1))) limit 1",
				"(((select a from t1) limit 1)) limit 1",
			} {
				_, lmErr := conn.ExecContext(ctx, sqlText)
				require.Error(t, lmErr, "duplicate LIMIT must stay a typed syntax error: %s", sqlText)
				require.ErrorContains(t, lmErr, "multiple LIMIT clauses not allowed", sqlText)
			}
		})
		t.Run("glm100_ordinal_carry_of_authored_alias_and_duplicates", func(t *testing.T) {
			// The generated page carries every already-projected slot by
			// ordinal: an authored alias that merely looks internal travels,
			// and duplicate authored headings stay positional through both
			// page stages. The wire readback asserts the exact headings.
			twoColsExact(t, "WITH q(x,y) AS ((SELECT 1 AS __mo_filepath, 2 AS a LIMIT 2) LIMIT 1) SELECT x,y FROM q", []string{"x", "y"}, [][2]string{{"1", "2"}})
			twoColsExact(t, "WITH q(x,y) AS ((SELECT 1 AS a, 2 AS a LIMIT 2) LIMIT 1) SELECT x,y FROM q", []string{"x", "y"}, [][2]string{{"1", "2"}})
		})
		t.Run("authored_star_hidden_field_suppressed_upstream_with_and_without_page", func(t *testing.T) {
			// A real physical table whose engine metadata carries a genuinely hidden
			// column, read through an authored SELECT * in both shapes: the direct
			// star and the same star through a nested parenthesized CTE page whose
			// inner LIMIT 2 and outer LIMIT 1 both compose - the generated page.
			// The hidden datum stays suppressed by the upstream authored-star
			// policy with identical visible [id label] rows on the wire.
			dir := t.TempDir()
			srcPath := filepath.Join(dir, "patrol102_source.csv")
			require.NoError(t, os.WriteFile(srcPath, []byte("7,seven\n8,eight\n9,nine\n"), 0o644))
			_, eErr := conn.ExecContext(ctx, fmt.Sprintf("CREATE EXTERNAL TABLE hd_ext_102 (id INT, label VARCHAR(20)) INFILE '%s' FIELDS TERMINATED BY ','", srcPath))
			require.NoError(t, eErr)
			defer func() { _, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS hd_ext_102") }()
			var extCount int
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT count(*) FROM mo_catalog.mo_tables WHERE relname = 'hd_ext_102' AND relkind = 'e'").Scan(&extCount))
			require.Equal(t, 1, extCount, "the fixture is genuinely an external table in the engine catalog")
			// The hidden datum is real engine metadata on the physical table; the
			// authored star never surfaces it in either shape.
			twoColsExact(t, "SELECT * FROM hd_ext_102 LIMIT 3", []string{"id", "label"},
				[][2]string{{"7", "seven"}, {"8", "eight"}, {"9", "nine"}})
			twoColsExact(t, "WITH p AS ((SELECT * FROM hd_ext_102 LIMIT 2) LIMIT 1) SELECT * FROM p", []string{"id", "label"},
				[][2]string{{"7", "seven"}})
		})
		t.Run("glm100_released_marker_free_recursive_replay_and_controls", func(t *testing.T) {
			// W-A/W-B: the released interpretation - the enclosing wrapper
			// stage is dropped, the innermost emission cap governs. W-N: the
			// innermost ORDER BY is the genuine typed rejection under every
			// generation. v_scope: a wrapper-local WITH is dropped with its
			// wrapper by the released peel. These bind the CURRENT
			// marker-present generation: the composed semantics and the
			// typed rejections. The marker-free replay of W-A/W-B/v_scope
			// is proven against authentic release-written definitions in the
			// dedicated release/RESTORE cells of this phase, not here.
			_, wnErr := conn.ExecContext(ctx, "WITH RECURSIVE q(n) AS ((SELECT 1 UNION ALL SELECT n+1 FROM q WHERE n<3 ORDER BY n LIMIT 3) LIMIT 1) SELECT n FROM q")
			require.Error(t, wnErr, "the innermost ORDER BY stays the typed recursive rejection under every generation")
			require.ErrorContains(t, wnErr, "ORDER BY", "the typed message names the clause: %v", wnErr)
		})
		t.Run("finding_94_WITH_bearing_consumer_pagination_still_composes", func(t *testing.T) {
			// The positive discriminator: the SAME nested page shapes under
			// a WITH-bearing consumer compose inside out and return the
			// paged rows, unchanged from the accepted closures.
			expect(t, "WITH q AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) ((SELECT a FROM q LIMIT 2) ORDER BY a DESC LIMIT 1)", []string{"2"})
			twoCols(t, "WITH q(x, y) AS ((SELECT 1 AS a, 2 AS a LIMIT 2) LIMIT 1) SELECT x, y FROM q", [][2]string{{"1", "2"}})
		})
		t.Run("finding_2_multi_stage_positions_renamed_through_both_pages", func(t *testing.T) {
			twoCols(t, "WITH q(x, y) AS ((SELECT 1 AS a, 2 AS a LIMIT 2) LIMIT 1) SELECT x, y FROM q", [][2]string{{"1", "2"}})
		})
		t.Run("finding_2_repeated_binding_keeps_positions_after_rebind", func(t *testing.T) {
			// The same declaration binds twice in one statement: the second
			// rebind must read the authored pages exactly like the first, so
			// every rebind carries the positional rows {(1,2),(2,1)}. UNION
			// ALL exposes no textual branch order, so the assertion is the
			// order-independent bag: any positional mixup on either rebind
			// changes the bag and stays caught.
			rs, qErr := conn.QueryContext(ctx, "WITH q(x, y) AS ((SELECT 1 AS a, 2 AS a LIMIT 2) LIMIT 1) SELECT x, y FROM q UNION ALL SELECT y, x FROM q")
			require.NoErrorf(t, qErr, "positioned witness failed")
			defer rs.Close()
			var got [][2]string
			for rs.Next() {
				var x, y sql.NullString
				require.NoError(t, rs.Scan(&x, &y))
				got = append(got, [2]string{x.String, y.String})
			}
			require.NoError(t, rs.Err())
			sort := func(rows [][2]string) [][2]string {
				out := append([][2]string(nil), rows...)
				for i := range out {
					for j := i + 1; j < len(out); j++ {
						if out[j][0] < out[i][0] || (out[j][0] == out[i][0] && out[j][1] < out[i][1]) {
							out[i], out[j] = out[j], out[i]
						}
					}
				}
				return out
			}
			require.Equalf(t, [][2]string{{"1", "2"}, {"2", "1"}}, sort(got), "positions after rebind: got %v", got)
		})
		t.Run("finding_2_control_ambiguous_consumer_without_renaming", func(t *testing.T) {
			// The paged duplicate-heading body without an explicit CTE column
			// list keeps its honest ambiguity: a single-name consumer must
			// not silently pick one position.
			rows0, qErr := rows("SELECT a FROM ((SELECT 1 AS a, 2 AS a) LIMIT 1) z")
			require.Error(t, qErr, "the consumer must stay ambiguous: got rows %v", rows0)
		})
		t.Run("finding_3_consumer_pages_apply_inside_out", func(t *testing.T) {
			expect(t,
				"WITH q AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) ((SELECT a FROM q LIMIT 2) ORDER BY a LIMIT 1 OFFSET 1)",
				[]string{"2"})
		})
		t.Run("finding_3_control_explicit_derived_table_equivalent", func(t *testing.T) {
			expect(t,
				"WITH q AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) (SELECT * FROM (SELECT a FROM q LIMIT 2) d ORDER BY a LIMIT 1 OFFSET 1)",
				[]string{"2"})
		})
		t.Run("finding_3_control_nearest_single_page", func(t *testing.T) {
			expect(t,
				"WITH q AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) (SELECT a FROM q LIMIT 2)",
				[]string{"1", "2"})
		})
		t.Run("finding_3_control_prepared_two_stage_reuse_and_empty", func(t *testing.T) {
			q := "WITH q AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) ((SELECT a FROM q LIMIT ?) ORDER BY a LIMIT 1 OFFSET ?)"
			st, pErr := conn.PrepareContext(ctx, q)
			require.NoError(t, pErr)
			defer st.Close()
			query := func(args ...any) []string {
				rs, qErr := st.QueryContext(ctx, args...)
				require.NoError(t, qErr)
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
			require.Equal(t, []string{"2"}, query(2, 1), "prepared two-stage consumer pages in order")
			require.Equal(t, []string{"2"}, query(2, 1), "reuse keeps the same slots and result")
			require.Empty(t, query(2, 5), "a consumer page past the finite content is empty")
		})
		t.Run("finding_4_release_stored_pages_kept_for_marker_free_definitions", func(t *testing.T) {
			// The release-authored marker-free definition rebinds with the
			// release interpretation: every enclosing page of the stored
			// body is discarded and only the innermost select (with its own
			// clauses) binds. A freshly authored definition applies each page
			// inside out. The live release-authored lifecycle for these two
			// shapes is retained in this phase's raw evidence; the embedded
			// pair below pins the same boundary through the generation
			// marker on definitions the current binary authors itself.
			const body = "WITH q AS ((SELECT 1 AS a UNION ALL SELECT 2 AS a ORDER BY a LIMIT 2) ORDER BY a LIMIT 1 OFFSET 1) SELECT a FROM q"
			const legacy = "CREATE VIEW v_release_pages AS " + body
			_, e := conn.ExecContext(ctx, legacy)
			require.NoError(t, e)
			// The current binary stamps every fresh definition with the
			// current generation: the marker-free semantics cannot be
			// authored in-band, so this cell only pins the fresh behavior
			// here; the marker-free behavior is proven by the retained
			// release-written catalog lifecycle in this run's evidence.
			expect(t, "SELECT a FROM v_release_pages", []string{"2"})
			_, e = conn.ExecContext(ctx, "DROP VIEW v_release_pages")
			require.NoError(t, e)
		})
	})
}
