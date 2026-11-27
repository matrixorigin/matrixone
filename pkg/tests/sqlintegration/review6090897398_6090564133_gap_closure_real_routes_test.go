// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Reviews 6090897398 (inline 4721617684) and 6090564133 gap closure on the
// real native routes: the semantically redundant outer literal count composes
// through the actual PREPARE/EXECUTE route and persists the child's row; the
// authored narrower count keeps its genuine duplicate-owner rejection with
// no destination; the routed statement root - including the outer-
// parenthesized WITH equivalent - keeps its authored assignment; public
// ordinary EXPLAIN composes the valid two-page WITH export into plan rows and
// leaves the target absent while the executed same SQL performs it; and the
// real catalog rel_createsql of a quoted-charset multi-statement CTAS
// roundtrips and replays with its exact quoted origin.
func TestReview6090GapClosureRealRoutesRoundtrip(t *testing.T) {
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
		_, err = conn.ExecContext(ctx, "create database if not exists r6090_gap_db")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "use r6090_gap_db")
		require.NoError(t, err)
		defer func() { _, _ = conn.ExecContext(ctx, "drop database if exists r6090_gap_db") }()

		// The exact review SQL at the real native prepared route: the
		// redundant outer literal count prepares, executes and persists
		// the child's authored row.
		_, err = conn.ExecContext(ctx, "PREPARE redundant_p FROM 'CREATE TABLE ctas_r2 AS (SELECT 7 AS a LIMIT 1) LIMIT 2'")
		require.NoErrorf(t, err, "the redundant outer literal count prepares through the shared child-only boundary")
		_, err = conn.ExecContext(ctx, "EXECUTE redundant_p")
		require.NoErrorf(t, err, "the prepared redundant count executes and persists the child's rows")
		rows, rerr := conn.QueryContext(ctx, "SELECT a FROM ctas_r2")
		require.NoError(t, rerr)
		func() {
			defer rows.Close()
			var got int
			require.Truef(t, rows.Next(), "the created table holds the child's row")
			require.NoError(t, rows.Scan(&got))
			require.Equalf(t, 7, got, "the persisted value is the child's authored literal row")
			require.False(t, rows.Next(), "the child's own LIMIT caps the destination at its row")
			require.NoError(t, rows.Err())
		}()
		_, err = conn.ExecContext(ctx, "DEALLOCATE PREPARE redundant_p")
		require.NoError(t, err)

		// Authoritative narrower control at the same route: the genuine
		// duplicate-owner rejection stays and no destination appears.
		_, err = conn.ExecContext(ctx, "PREPARE narrow_p FROM 'CREATE TABLE ctas_narrow AS (SELECT 7 AS a LIMIT 5) LIMIT 3'")
		require.ErrorContainsf(t, err, "multiple LIMIT",
			"an authored narrower count keeps the duplicate-owner rejection at the prepared route")
		nrows, nerr := conn.QueryContext(ctx, "SELECT count(*) FROM mo_catalog.mo_tables WHERE relname = 'ctas_narrow'")
		require.NoError(t, nerr)
		func() {
			defer nrows.Close()
			var ncnt int
			require.True(t, nrows.Next())
			require.NoError(t, nrows.Scan(&ncnt))
			require.Zerof(t, ncnt, "the rejected prepare leaves no destination table")
			require.NoError(t, nrows.Err())
		}()

		// The routed statement root on the outer-parenthesized WITH
		// equivalent: the authored assignment rides the outer group.
		_, err = conn.ExecContext(ctx, "(WITH q AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) (SELECT a FROM q LIMIT 2)) ORDER BY a LIMIT 1 OFFSET 1 INTO @pgy")
		require.NoErrorf(t, err, "the outer-parenthesized WITH equivalent composes with the routed root's assignment")
		var assigned int
		vrows, verr := conn.QueryContext(ctx, "SELECT @pgy")
		require.NoError(t, verr)
		func() {
			defer vrows.Close()
			require.True(t, vrows.Next())
			require.NoError(t, vrows.Scan(&assigned))
			require.Equalf(t, 2, assigned, "the routed root assignment composes the pages and reads back the second row")
			require.False(t, vrows.Next())
			require.NoError(t, vrows.Err())
		}()

		// Public ordinary EXPLAIN composes the valid two-page WITH export
		// and returns its plan rows while leaving the target absent; the
		// executable same SQL performs the export through its proper owner.
		tmp := t.TempDir()
		exportPath := filepath.Join(tmp, "r6090-owner-guard.csv")
		exportSQL := fmt.Sprintf("WITH q AS (SELECT 1 AS a UNION ALL SELECT 2 AS a) (SELECT a FROM q LIMIT 2) ORDER BY a LIMIT 1 OFFSET 1 INTO OUTFILE '%s'", exportPath)
		erows, eerr := conn.QueryContext(ctx, "EXPLAIN "+exportSQL)
		require.NoErrorf(t, eerr, "public ordinary EXPLAIN composes the valid two-page WITH export")
		var explainRows int
		func() {
			defer erows.Close()
			for erows.Next() {
				explainRows++
			}
			require.NoError(t, erows.Err())
		}()
		require.Positivef(t, explainRows, "the EXPLAIN result carries its plan rows")
		_, statErr := os.Stat(exportPath)
		require.Truef(t, os.IsNotExist(statErr), "explain-only composition leaves the export target absent")
		_, err = conn.ExecContext(ctx, exportSQL)
		require.NoErrorf(t, err, "the executed same SQL performs the export through its proper execution owner")
		written, wberr := os.ReadFile(exportPath)
		require.NoError(t, wberr, "the executed export wrote its target file")
		require.Equalf(t, "a\n2\n", string(written),
			"the exported file carries exactly the header plus the composed page's single row, no extra rows")

		// The real catalog consumer of review 6090897398 across BOTH of
		// createTableSQLForCatalog's branches. The engine validates charset
		// names at DDL (unsupported values are rejected outright, per the
		// frozen probe receipts), so the catalog route carries quoted
		// spellings of real charsets while the digit/keyword/spaced-value
		// old-red/new-green discrimination lives at the canonical-formatter
		// level in the parser roundtrip test.
		_, err = conn.ExecContext(ctx, "CREATE TABLE ctas_charset_quote charset 'utf8MB4' collate 'utf8MB4_bin' AS (SELECT 8 AS b LIMIT 1) LIMIT 2")
		require.NoErrorf(t, err, "the quoted-charset CTAS creates through the real COM_QUERY route")
		crows, cerr := conn.QueryContext(ctx, "SELECT rel_createsql FROM mo_catalog.mo_tables WHERE reldatabase = 'r6090_gap_db' AND relname = 'ctas_charset_quote'")
		require.NoError(t, cerr)
		var rawCatalogSQL string
		func() {
			defer crows.Close()
			require.True(t, crows.Next(), "the scoped raw readback returns its single row")
			require.NoError(t, crows.Scan(&rawCatalogSQL))
			require.False(t, crows.Next(), "the scoped raw readback is a single row")
			require.NoError(t, crows.Err())
		}()
		require.NotEmptyf(t, rawCatalogSQL, "the catalog stores the CTAS create SQL")
		require.Containsf(t, rawCatalogSQL, "charset 'utf8MB4'",
			"the single-statement request takes the raw-echo branch and keeps the authored quoted spelling exactly")
		require.NotContainsf(t, rawCatalogSQL, "charset = 'utf8MB4'",
			"the raw-echo branch does not run the canonical formatter")

		// The genuine multi-statement COM_QUERY: the CTAS plus a harmless
		// durable sibling in ONE ExecContext call. Only that shape leaves the
		// raw-echo branch and persists the canonical AST render, so the stored
		// text is the fixed formatter's output: the canonical charset = form
		// with the authored mixed-case quoted value, distinguishable from the
		// submitted raw CREATE and excluding the sibling statement.
		batchSQL := "CREATE TABLE ctas_charset_canon charset 'utf8MB4' collate 'utf8MB4_bin' AS (SELECT 8 AS b LIMIT 1) LIMIT 2;CREATE TABLE r6090_sibling_marker (id int)"
		_, err = conn.ExecContext(ctx, batchSQL)
		require.NoErrorf(t, err, "the multi-statement COM_QUERY runs the CTAS and its sibling together")
		sibrows, serr := conn.QueryContext(ctx, "SELECT count(*) FROM mo_catalog.mo_tables WHERE reldatabase = 'r6090_gap_db' AND relname = 'r6090_sibling_marker'")
		require.NoError(t, serr)
		var siblingCount int
		func() {
			defer sibrows.Close()
			require.True(t, sibrows.Next(), "the sibling readback returns its row")
			require.NoError(t, sibrows.Scan(&siblingCount))
			require.Equalf(t, 1, siblingCount, "the sibling statement genuinely ran in the same COM_QUERY")
			require.NoError(t, sibrows.Err())
		}()
		krows, kerr := conn.QueryContext(ctx, "SELECT rel_createsql FROM mo_catalog.mo_tables WHERE reldatabase = 'r6090_gap_db' AND relname = 'ctas_charset_canon'")
		require.NoError(t, kerr)
		var catalogSQL string
		func() {
			defer krows.Close()
			require.True(t, krows.Next(), "the scoped canonical readback returns its single row")
			require.NoError(t, krows.Scan(&catalogSQL))
			require.False(t, krows.Next(), "the scoped canonical readback is a single row")
			require.NoError(t, krows.Err())
		}()
		require.Containsf(t, catalogSQL, "charset = 'utf8MB4'",
			"the multi-statement branch persists the canonical formatter text with the authored mixed-case quoted value")
		require.NotContainsf(t, catalogSQL, "charset 'utf8MB4'",
			"the canonical text is distinguished from the raw submitted CREATE spelling")
		require.NotContainsf(t, catalogSQL, "r6090_sibling_marker",
			"the canonical row excludes the sibling statement that shared the COM_QUERY")
		_, err = conn.ExecContext(ctx, "create database if not exists r6090_replay_db")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "use r6090_replay_db")
		require.NoError(t, err)
		defer func() { _, _ = conn.ExecContext(ctx, "drop database if exists r6090_replay_db") }()
		replay := strings.ReplaceAll(catalogSQL, "r6090_gap_db", "r6090_replay_db")
		replay = strings.ReplaceAll(replay, "ctas_charset_canon", "ctas_charset_replay")
		_, err = conn.ExecContext(ctx, replay)
		require.NoErrorf(t, err, "the actual canonical catalog text reparses and replays as a fresh CREATE in an isolated schema")
		rprows, rperr := conn.QueryContext(ctx, "SELECT rel_createsql FROM mo_catalog.mo_tables WHERE reldatabase = 'r6090_replay_db' AND relname = 'ctas_charset_replay'")
		require.NoError(t, rperr)
		var replayCatalogSQL string
		func() {
			defer rprows.Close()
			require.True(t, rprows.Next(), "the replayed row readback returns its single row")
			require.NoError(t, rprows.Scan(&replayCatalogSQL))
			require.False(t, rprows.Next(), "the replayed row readback is a single row")
			require.NoError(t, rprows.Err())
		}()
		require.Containsf(t, replayCatalogSQL, "charset = 'utf8MB4'",
			"the replayed catalog row preserves the exact canonical quoted mixed-case value through the real reparse")
		brows, berr := conn.QueryContext(ctx, "SELECT b FROM ctas_charset_replay")
		require.NoError(t, berr)
		var replayVal int
		func() {
			defer brows.Close()
			require.True(t, brows.Next())
			require.NoError(t, brows.Scan(&replayVal))
			require.Equalf(t, 8, replayVal, "the replayed CTAS restores the persisted row")
			require.NoError(t, brows.Err())
		}()
	})
}
