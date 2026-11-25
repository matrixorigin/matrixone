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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type genSinkRow struct {
	id, a, g, gv, h int64
	b, gs           string
}

// TestCDCGeneratedColumnMiddleSinkSnapshotAndIncremental runs a real MO-to-MO
// CDC task on an embedded cluster against a source table whose generated
// column sits between replicated base columns. Before the sink fix the task
// rejects every generated-column assignment with moerr 20301 and the
// watermark stays at 0-0; after the fix base columns replicate unchanged and
// the target recomputes the generated values itself, for both the initial
// snapshot and incremental tail changes, with the persisted checkpoint
// strictly advancing past the snapshot one. The fixture registers each
// acquired resource for teardown immediately after creating it, so a setup
// failure mid-fixture still cleans everything already built. The unchanged
// generated-free SQL contract is asserted separately at the builder level.
func TestCDCGeneratedColumnMiddleSinkSnapshotAndIncremental(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := int(cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		// Registered first so every per-acquisition cleanup below still runs
		// against a live connection; the defers run LIFO after it.
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
		defer cancel()

		// Every teardown step reports unexpected failures instead of
		// discarding them, so a broken cleanup surfaces as a checkable
		// postcondition even when the test body already passed.
		cleanupExec := func(step, query string) func() {
			return func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cleanupCancel()
				if _, err := db.ExecContext(cleanupCtx, query); err != nil {
					t.Errorf("cleanup %s failed: %v", step, err)
				}
			}
		}

		// Each acquisition registers its cleanup immediately. Defers run in
		// LIFO order, so teardown drops the CDC task first, then the PITR,
		// then the databases, each on its own bounded context.
		require.NoError(t, execSQL(ctx, db, "CREATE DATABASE cdc_gen_src"))
		defer cleanupExec("drop source database", "DROP DATABASE IF EXISTS cdc_gen_src")()
		require.NoError(t, execSQL(ctx, db, "CREATE DATABASE cdc_gen_sink"))
		defer cleanupExec("drop sink database", "DROP DATABASE IF EXISTS cdc_gen_sink")()
		// g is a generated INT column in a MIDDLE position between replicated
		// base columns, gs is a STORED generated geometry column and gv is a
		// VIRTUAL generated column in front of the trailing generated h, so a
		// wrong column projection shifts b values and makes every misalignment
		// observable through distinguishable base values. The geometry slot is
		// also of a type the generic extractor cannot decode, so the real
		// routed CDC batches prove generated slots are skipped without being
		// decoded, stored or virtual alike. The generated DDL must stay
		// identical on the CDC-created target.
		require.NoError(t, execSQL(ctx, db,
			"CREATE TABLE cdc_gen_src.t (id INT PRIMARY KEY, a INT, g INT GENERATED ALWAYS AS (a + 1) STORED, b VARCHAR(32), gs GEOMETRY GENERATED ALWAYS AS (ST_GeomFromText('POINT(1 2)')) STORED, gv INT GENERATED ALWAYS AS (a + 2) VIRTUAL, h INT GENERATED ALWAYS AS (a * 10) STORED)"))

		// Snapshot payload exists before the task starts.
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_gen_src.t (id, a, b) VALUES (1, 10, 'alpha'), (2, 20, 'beta'), (3, 30, 'gamma')"))
		// The geometry generated expression is constant, so the exact WKT the
		// source materializes is also what the target must recompute; read it
		// once instead of hardcoding MO's WKT spacing.
		var wantGSText string
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT ST_AsText(gs) FROM cdc_gen_src.t WHERE id = 1").Scan(&wantGSText))
		require.Contains(t, strings.ToUpper(wantGSText), "POINT",
			"stored geometry generated column must materialize a readable point")

		uri := fmt.Sprintf("mysql://dump:111@127.0.0.1:%d", port)
		// The CDC task framework requires a PITR of at least 2 hours covering
		// the routed source tables (same contract as the CDC identity BVT).
		require.NoError(t, execSQL(ctx, db, "CREATE PITR cdc_gen_pitr FOR DATABASE cdc_gen_src RANGE 2 'h'"))
		defer cleanupExec("drop pitr", "DROP PITR IF EXISTS cdc_gen_pitr")()
		require.NoError(t, execSQL(ctx, db, fmt.Sprintf(
			"CREATE CDC cdc_gen_task '%s' 'matrixone' '%s' 'cdc_gen_src.t:cdc_gen_sink.t' {'Level'='table'}",
			uri, uri)))
		defer cleanupExec("drop cdc task", "DROP CDC TASK cdc_gen_task")()

		readGen := func() []genSinkRow {
			rows, err := db.QueryContext(ctx, "SELECT id, a, g, b, ST_AsText(gs), gv, h FROM cdc_gen_sink.t ORDER BY id")
			if err != nil {
				return nil
			}
			defer rows.Close()
			var out []genSinkRow
			for rows.Next() {
				var r genSinkRow
				if rows.Scan(&r.id, &r.a, &r.g, &r.b, &r.gs, &r.gv, &r.h) != nil {
					return nil
				}
				out = append(out, r)
			}
			if rows.Err() != nil {
				return nil
			}
			return out
		}
		watermarkFor := func(table string) (string, bool) {
			var wm string
			err := db.QueryRowContext(ctx,
				"SELECT w.watermark FROM mo_catalog.mo_cdc_watermark AS w JOIN mo_catalog.mo_cdc_task AS t "+
					"ON t.account_id = w.account_id AND t.task_id = w.task_id "+
					"WHERE t.task_name = 'cdc_gen_task' AND w.db_name = 'cdc_gen_src' AND w.table_name = '"+table+"'").Scan(&wm)
			return wm, err == nil
		}
		// parseCheckpoint mirrors the persisted "physical-logical" watermark
		// format from pkg/cdc (parseWatermarkTS) without panicking on
		// transient values observed while polling.
		parseCheckpoint := func(wm string) (types.TS, bool) {
			physical, logical, ok := strings.Cut(wm, "-")
			if !ok || physical == "" || logical == "" || strings.Contains(logical, "-") {
				return types.TS{}, false
			}
			p, err := strconv.ParseInt(physical, 10, 64)
			if err != nil || p < 0 {
				return types.TS{}, false
			}
			l, err := strconv.ParseUint(logical, 10, 32)
			if err != nil {
				return types.TS{}, false
			}
			return types.BuildTS(p, uint32(l)), true
		}
		taskState := func() string {
			var state string
			if err := db.QueryRowContext(ctx,
				"SELECT state FROM mo_catalog.mo_cdc_task WHERE task_name = 'cdc_gen_task'").Scan(&state); err != nil {
				return ""
			}
			return state
		}
		// sinkDiagnostics surfaces the persisted CDC task state and sink error
		// so a failing sync step fails with the real consumer error (before the
		// fix the target rejects the generated column with moerr 20301).
		sinkDiagnostics := func() string {
			var msgs []string
			rows, qerr := db.QueryContext(ctx,
				"SELECT w.table_name, IFNULL(w.err_msg, '') FROM mo_catalog.mo_cdc_watermark AS w "+
					"JOIN mo_catalog.mo_cdc_task AS t ON t.account_id = w.account_id AND t.task_id = w.task_id "+
					"WHERE t.task_name = 'cdc_gen_task'")
			if qerr == nil {
				defer rows.Close()
				for rows.Next() {
					var table, msg string
					if rows.Scan(&table, &msg) == nil && msg != "" {
						msgs = append(msgs, table+": "+msg)
					}
				}
				if err := rows.Err(); err != nil {
					msgs = append(msgs, "watermark-query-error: "+err.Error())
				}
			}
			return fmt.Sprintf("task state=%q, watermark err_msgs=%v", taskState(), msgs)
		}

		// Snapshot: base values replicate; the target computes g and h itself.
		wantGenSnapshot := []genSinkRow{
			{id: 1, a: 10, g: 11, gs: wantGSText, gv: 12, h: 100, b: "alpha"},
			{id: 2, a: 20, g: 21, gs: wantGSText, gv: 22, h: 200, b: "beta"},
			{id: 3, a: 30, g: 31, gs: wantGSText, gv: 32, h: 300, b: "gamma"},
		}
		if !assert.Eventually(t, func() bool {
			return reflect.DeepEqual(readGen(), wantGenSnapshot)
		}, 90*time.Second, 500*time.Millisecond,
			"snapshot must sync base columns and let the target recompute generated columns") {
			require.FailNow(t, "generated-column snapshot sync failed", sinkDiagnostics())
		}

		// The persisted snapshot checkpoint must be a real timestamp, not the
		// initial 0-0 from before any sink transaction committed.
		var snapshotCheckpoint string
		require.Eventually(t, func() bool {
			wm, ok := watermarkFor("t")
			if !ok {
				return false
			}
			checkpoint, parsed := parseCheckpoint(wm)
			snapshotCheckpoint = wm
			return parsed && !checkpoint.IsEmpty()
		}, 60*time.Second, 500*time.Millisecond,
			"persisted snapshot checkpoint must advance beyond the initial 0-0")

		// The CDC-created target preserves the generated DDL.
		var sinkTable, sinkDDL string
		require.NoError(t, db.QueryRowContext(ctx, "SHOW CREATE TABLE cdc_gen_sink.t").
			Scan(&sinkTable, &sinkDDL))
		require.Contains(t, sinkDDL, "GENERATED ALWAYS AS")

		// Incremental tail: insert, update, and delete must replay through the
		// same INSERT ... ON DUPLICATE KEY UPDATE sinker with aligned values.
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_gen_src.t (id, a, b) VALUES (4, 40, 'delta')"))
		require.NoError(t, execSQL(ctx, db, "UPDATE cdc_gen_src.t SET a = 25, b = 'beta2' WHERE id = 2"))
		require.NoError(t, execSQL(ctx, db, "DELETE FROM cdc_gen_src.t WHERE id = 1"))

		wantGenTail := []genSinkRow{
			{id: 2, a: 25, g: 26, gs: wantGSText, gv: 27, h: 250, b: "beta2"},
			{id: 3, a: 30, g: 31, gs: wantGSText, gv: 32, h: 300, b: "gamma"},
			{id: 4, a: 40, g: 41, gs: wantGSText, gv: 42, h: 400, b: "delta"},
		}
		if !assert.Eventually(t, func() bool {
			return reflect.DeepEqual(readGen(), wantGenTail)
		}, 90*time.Second, 500*time.Millisecond,
			"incremental changes must sync with aligned base values and target-computed generated values") {
			require.FailNow(t, "generated-column incremental sync failed", sinkDiagnostics())
		}

		// The persisted checkpoint must strictly advance past the snapshot
		// checkpoint once the incremental changes have been sunk.
		var tailCheckpoint string
		require.Eventually(t, func() bool {
			wm, ok := watermarkFor("t")
			if !ok {
				return false
			}
			tailCheckpoint = wm
			tailTS, parsed := parseCheckpoint(tailCheckpoint)
			if !parsed {
				return false
			}
			snapTS, snapParsed := parseCheckpoint(snapshotCheckpoint)
			return snapParsed && !snapTS.IsEmpty() && tailTS.GT(&snapTS)
		}, 60*time.Second, 500*time.Millisecond,
			"persisted checkpoint must strictly advance past the snapshot checkpoint")
		snapTS, snapParsed := parseCheckpoint(snapshotCheckpoint)
		tailTS, tailParsed := parseCheckpoint(tailCheckpoint)
		require.True(t, snapParsed && tailParsed && tailTS.GT(&snapTS),
			"persisted checkpoints must strictly advance: snapshot=%q tail=%q",
			snapshotCheckpoint, tailCheckpoint)
		require.Equal(t, cdc.CDCState_Running, taskState(),
			"task must remain running after generated-column sink sync")
		// Log the actual persisted checkpoints so the raw verbose evidence
		// carries the strict advance values for independent review.
		t.Logf("persisted checkpoints: snapshot=%q tail=%q (strict parsed advance verified)",
			snapshotCheckpoint, tailCheckpoint)
	})
}
