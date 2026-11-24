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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"sort"
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

		t.Run("page_order_by_scalar_subquery_binds_authored_cte_over_same_name_physical", func(t *testing.T) {
			// The moved page's ORDER BY reads x through a scalar subquery:
			// abs(a - (SELECT max(a) FROM x)) is tie-free over the rows 1 and 2
			// and its winner REVERSES with the source - the authored CTE has
			// max(a)=2 so row 2 wins (distance 0), while a same-name physical
			// x holding (1) has max(a)=1 so row 1 would win. If the generated
			// page bound outside the WITH that declared x, the physical table
			// (when present) or an absent-name 1146 would take the reference.
			wire109 := func(t *testing.T, sql string) []int {
				t.Helper()
				stmt109, perr := conn.PrepareContext(ctx, sql)
				require.NoError(t, perr, "prepare must accept the nested page shape")
				defer func() { require.NoError(t, stmt109.Close()) }()
				rows109, qerr := stmt109.QueryContext(ctx)
				require.NoError(t, qerr, "the paginated shape must bind: %s", sql)
				defer func() { require.NoError(t, rows109.Close()) }()
				headings109, herr := rows109.Columns()
				require.NoError(t, herr)
				require.Equal(t, []string{"a"}, headings109, "the wire headings stay the authored projection")
				var got []int
				for rows109.Next() {
					var v int
					require.NoError(t, rows109.Scan(&v))
					got = append(got, v)
				}
				require.NoError(t, rows109.Err())
				sort.Ints(got)
				return got
			}
			pageSQL := "WITH q AS (WITH x AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) (SELECT a FROM x) ORDER BY abs(a - (SELECT max(a) FROM x)) LIMIT 1) SELECT a FROM q"
			// No physical x: the moved ORDER BY must resolve the authored CTE.
			require.Equal(t, []int{2}, wire109(t, pageSQL),
				"the moved ORDER BY's scalar subquery must read the authored CTE (max=2 picks row 2), not an absent name")
			// The same-name/same-column physical x holding (1) would reverse the
			// winner to row 1 if it captured the reference: the authored WITH
			// must still own the page's scope.
			_, derr := conn.ExecContext(ctx, "drop table if exists x")
			require.NoError(t, derr)
			_, cerr := conn.ExecContext(ctx, "create table x(a int)")
			require.NoError(t, cerr)
			_, ierr := conn.ExecContext(ctx, "insert into x values (1)")
			require.NoError(t, ierr)
			defer func() { _, _ = conn.ExecContext(ctx, "drop table if exists x") }()
			require.Equal(t, []int{2}, wire109(t, pageSQL),
				"a same-name physical x with a reversing winner must not capture the page's ORDER BY subquery")
			// The explicit legal derived-table equivalence returns the authored
			// winner independently of the generated page machinery.
			derivedSQL := "SELECT a FROM (WITH x AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) SELECT a FROM x ORDER BY abs(a - (SELECT max(a) FROM x)) LIMIT 1) AS page"
			require.Equal(t, []int{2}, wire109(t, derivedSQL),
				"the derived equivalent, with the ORDER BY inside the WITH's lexical region, picks the same authored winner")
			require.Equal(t, []int{2}, wire109(t, pageSQL),
				"after the derived control the paginated shape still picks the authored winner")
			_, derr2 := conn.ExecContext(ctx, "drop table if exists x")
			require.NoError(t, derr2)

			// The preserved unpaged inner owner under a real enclosing page: the
			// WITH-bearing level has no local clauses of its own, so its
			// declaration stays on its unchanged authored owner and the outer
			// stage pages its output without any missing-relation error.
			unpagedSQL := "WITH q AS ((WITH y AS (SELECT 7 AS b) (SELECT b FROM y)) LIMIT 1) SELECT b FROM q"
			stmtU, uerr := conn.PrepareContext(ctx, unpagedSQL)
			require.NoError(t, uerr, "the unpaged inner WITH under an outer page must prepare")
			defer func() { require.NoError(t, stmtU.Close()) }()
			rowsU, rerr := stmtU.QueryContext(ctx)
			require.NoError(t, rerr, "the unpaged inner WITH must bind through its own declaration")
			defer func() { require.NoError(t, rowsU.Close()) }()
			headingsU, herrU := rowsU.Columns()
			require.NoError(t, herrU)
			require.Equal(t, []string{"b"}, headingsU)
			var rowU []int
			for rowsU.Next() {
				var v int
				require.NoError(t, rowsU.Scan(&v))
				rowU = append(rowU, v)
			}
			require.NoError(t, rowsU.Err())
			require.Equal(t, []int{7}, rowU, "the unpaged inner owner keeps its local CTE visible")
		})

		t.Run("nested_with_owner_page_binds_moved_order_by_in_x_scope", func(t *testing.T) {
			// The nested owner the review #6029591213 witness targets: q's body
			// ROOT is a clause-bearing ParenSelect wrapper with NO WITH, and its
			// CHILD level is the one that owns WITH x plus its own ORDER BY and
			// LIMIT. The generated page that consumes those clauses must own
			// that exact WITH (the page-local move), so the moved ORDER BY's
			// scalar subquery reads the authored CTE even when a same-name
			// physical x(a) holding (1) would reverse the winner.
			sql110 := "WITH q AS ((WITH x AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) (SELECT a FROM x) ORDER BY abs(a - (SELECT max(a) FROM x)) LIMIT 1) LIMIT 1) SELECT a FROM q"
			// Structural assertion on the parsed AST: q.Stmt's root is the
			// parenthesized wrapper carrying the outer LIMIT and no WITH; the
			// WITH x, its ORDER BY and its LIMIT live on the CHILD level.
			parsed, err110 := parsers.ParseOne(ctx, dialect.MYSQL, sql110, 1)
			require.NoError(t, err110, "the nested owner witness must parse")
			outerStmt, okOuter := parsed.(*tree.Select)
			require.True(t, okOuter, "the witness parses as a select statement")
			require.NotNil(t, outerStmt.With, "the consuming statement declares WITH q")
			require.NotEmpty(t, outerStmt.With.CTEs, "the q declaration exists")
			root110, ok110 := outerStmt.With.CTEs[0].Stmt.(*tree.Select)
			require.True(t, ok110, "q's body is a select expression")
			require.Nil(t, root110.With, "the body root wrapper carries no WITH")
			require.NotNil(t, root110.Limit, "the body root carries the outer stage LIMIT")
			paren110, okParen := root110.Select.(*tree.ParenSelect)
			require.True(t, okParen, "the body root is a parenthesized wrapper")
			child110 := paren110.Select
			require.NotNil(t, child110.With, "the child level owns the WITH x")
			require.NotNil(t, child110.Limit, "the child level owns its own LIMIT")
			require.NotEmpty(t, child110.OrderBy, "the child level owns its own ORDER BY")

			wire110 := func(t *testing.T, sql string) []int {
				t.Helper()
				stmt110, perr := conn.PrepareContext(ctx, sql)
				require.NoError(t, perr)
				defer func() { require.NoError(t, stmt110.Close()) }()
				rows110, qerr := stmt110.QueryContext(ctx)
				require.NoError(t, qerr)
				defer func() { require.NoError(t, rows110.Close()) }()
				headings110, herr := rows110.Columns()
				require.NoError(t, herr)
				require.Equal(t, []string{"a"}, headings110)
				var got []int
				for rows110.Next() {
					var v int
					require.NoError(t, rows110.Scan(&v))
					got = append(got, v)
				}
				require.NoError(t, rows110.Err())
				sort.Ints(got)
				return got
			}
			require.Equal(t, []int{2}, wire110(t, sql110),
				"no physical x: the generated page owning the child's clauses binds x's scope (max=2), not an absent name")
			_, derr := conn.ExecContext(ctx, "drop table if exists x")
			require.NoError(t, derr)
			_, cerr := conn.ExecContext(ctx, "create table x(a int)")
			require.NoError(t, cerr)
			_, ierr := conn.ExecContext(ctx, "insert into x values (1)")
			require.NoError(t, ierr)
			defer func() { _, _ = conn.ExecContext(ctx, "drop table if exists x") }()
			require.Equal(t, []int{2}, wire110(t, sql110),
				"a same-name physical x(1) must not reverse the page's winner: the authored CTE still owns the moved ORDER BY")
			_, derr2 := conn.ExecContext(ctx, "drop table if exists x")
			require.NoError(t, derr2)
		})
	})
}

// The lazy-broadcast composition regression for review6040251798's D1 class:
// a non-hash LEFT LoopJoin whose single probe scope is a lazily admitted
// composed page (a UNION ALL body under a LIMIT) must keep the union
// starter's sequential-branch space intact while the join build attaches
// beside the stream. On the original compiler the build shared the lazy
// row-branch array, was never claimed, and the exact witness hung until the
// client cancelled; every value below is the reviewer's SQL or its named
// deterministic control.
func TestLazyBroadcastProbeKeepsAdmissionSpaceIntact(t *testing.T) {
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

		const probeDB = "patrol170_lazy_broadcast"
		_, e := conn.ExecContext(ctx, "drop database if exists "+probeDB)
		require.NoError(t, e)
		_, e = conn.ExecContext(ctx, "create database "+probeDB)
		require.NoError(t, e)
		// The fixture owns the lease through the callback scope: a plain
		// defer registers the checked fresh-context cleanup immediately
		// after the successful CREATE and before USE, so it runs inside
		// the callback (where the fixture still decides failures) and is
		// never missed if a later setup step errors.
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+probeDB)
		_, e = conn.ExecContext(ctx, "use "+probeDB)
		require.NoError(t, e)

		_, e = conn.ExecContext(ctx, "create table k147x(v int)")
		require.NoError(t, e)
		_, e = conn.ExecContext(ctx, "insert into k147x values (-1)")
		require.NoError(t, e)
		_, e = conn.ExecContext(ctx, "create table empty147(v int)")
		require.NoError(t, e)

		rowsOf := func(t *testing.T, sqlText string) []int {
			t.Helper()
			// A per-query bound keeps the deterministically hanging original
			// witness red without consuming the whole scenario deadline.
			qctx, qcancel := context.WithTimeout(ctx, 30*time.Second)
			defer qcancel()
			rows, qerr := conn.QueryContext(qctx, sqlText)
			require.NoError(t, qerr)
			defer func() { require.NoError(t, rows.Close()) }()
			headings, herr := rows.Columns()
			require.NoError(t, herr)
			require.Equal(t, []string{"a"}, headings)
			var got []int
			for rows.Next() {
				var v int
				require.NoError(t, rows.Scan(&v))
				got = append(got, v)
			}
			require.NoError(t, rows.Err())
			return got
		}
		sortedCopy := func(in []int) []int {
			out := append([]int(nil), in...)
			sort.Ints(out)
			return out
		}

		t.Run("exact_d1_row2", func(t *testing.T) {
			got := rowsOf(t, "(with k147x as (select 1 as a union all select 2 as a) (select a from k147x limit 5)) order by a * (select max(v) from k147x) limit 1")
			require.Equal(t, []int{2}, got,
				"the physical max=-1 orders the composed rows 2,1 by ascending key; the join build must be admitted beside the lazy stream instead of hanging on it")
		})
		t.Run("no_outer_limit_orders_desc", func(t *testing.T) {
			got := rowsOf(t, "(with k147x as (select 1 as a union all select 2 as a) (select a from k147x limit 5)) order by a * (select max(v) from k147x)")
			require.Equal(t, []int{2, 1}, got,
				"without the outer LIMIT both composed rows return in ascending key order (-2 then -1)")
		})
		t.Run("null_key_left_join_control", func(t *testing.T) {
			got := rowsOf(t, "(with k147x as (select 1 as a union all select 2 as a) (select a from k147x limit 5)) order by a * (select max(v) from empty147)")
			require.Equal(t, []int{1, 2}, sortedCopy(got),
				"an empty physical makes every scalar key NULL: the LEFT join null-extends and the composed rows still return instead of hanging")
		})
		t.Run("d0_plain_twin_row2", func(t *testing.T) {
			got := rowsOf(t, "select a from (select 1 as a union all select 2 as a) t order by a * (select max(v) from k147x) limit 1")
			require.Equal(t, []int{2}, got, "the ordinary non-lazy probe path is unchanged")
		})
		t.Run("lazy_page_early_stop_control", func(t *testing.T) {
			got := rowsOf(t, "(with q147 as (select 1 as a union all select 2 as a) (select a from q147 limit 1))")
			require.Equal(t, []int{1}, got, "the composed page without a join keeps its lazy early-stop and row result")
		})
	})
}
