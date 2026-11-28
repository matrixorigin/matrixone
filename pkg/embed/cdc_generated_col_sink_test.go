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
	"github.com/matrixorigin/matrixone/pkg/cnservice"
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

// TestCDCGeneratedColumnSameIDRenameFence proves the per-DML projection
// fence against a genuine same-ID in-place target drift: after a target
// column rename that provably keeps the table identity, an attempted
// replication batch is rejected before any target DML or acknowledgement,
// the drifted row never lands, and the persisted checkpoint stays at its
// pre-drift value. The fence message names the drift explicitly so it stays
// distinguishable from table replacement.
func TestCDCGeneratedColumnSameIDRenameFence(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := int(cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		cleanupExec := func(step, query string) func() {
			return func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cleanupCancel()
				if _, err := db.ExecContext(cleanupCtx, query); err != nil {
					t.Errorf("cleanup %s failed: %v", step, err)
				}
			}
		}
		require.NoError(t, execSQL(ctx, db, "CREATE DATABASE cdc_gen_src2"))
		defer cleanupExec("drop source database", "DROP DATABASE IF EXISTS cdc_gen_src2")()
		require.NoError(t, execSQL(ctx, db, "CREATE DATABASE cdc_gen_sink2"))
		defer cleanupExec("drop sink database", "DROP DATABASE IF EXISTS cdc_gen_sink2")()
		require.NoError(t, execSQL(ctx, db,
			"CREATE TABLE cdc_gen_src2.t (id INT PRIMARY KEY, a INT, g INT GENERATED ALWAYS AS (a + 1) STORED, b VARCHAR(32))"))
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_gen_src2.t (id, a, b) VALUES (1, 10, 'alpha'), (2, 20, 'beta')"))
		uri := fmt.Sprintf("mysql://dump:111@127.0.0.1:%d", port)
		require.NoError(t, execSQL(ctx, db, "CREATE PITR cdc_gen_pitr2 FOR DATABASE cdc_gen_src2 RANGE 2 'h'"))
		defer cleanupExec("drop pitr", "DROP PITR IF EXISTS cdc_gen_pitr2")()
		require.NoError(t, execSQL(ctx, db, fmt.Sprintf(
			"CREATE CDC cdc_gen_fence_task '%s' 'matrixone' '%s' 'cdc_gen_src2.t:cdc_gen_sink2.t' {'Level'='table'}",
			uri, uri)))
		defer cleanupExec("drop cdc task", "DROP CDC TASK cdc_gen_fence_task")()

		readSink := func() (int, string) {
			var b string
			rows, qerr := db.QueryContext(ctx, "SELECT id, b FROM cdc_gen_sink2.t ORDER BY id")
			if qerr != nil {
				return -1, qerr.Error()
			}
			defer rows.Close()
			var ids []int
			for rows.Next() {
				var id int
				var v string
				if rows.Scan(&id, &v) != nil {
					return -1, "scan"
				}
				ids = append(ids, id)
				b = v
			}
			_ = b
			if err := rows.Err(); err != nil {
				return -1, err.Error()
			}
			return len(ids), fmt.Sprint(ids)
		}
		watermarkFor := func() string {
			var wm string
			if err := db.QueryRowContext(ctx,
				"SELECT w.watermark FROM mo_catalog.mo_cdc_watermark AS w JOIN mo_catalog.mo_cdc_task AS t "+
					"ON t.account_id = w.account_id AND t.task_id = w.task_id "+
					"WHERE t.task_name = 'cdc_gen_fence_task' AND w.db_name = 'cdc_gen_src2' AND w.table_name = 't'").Scan(&wm); err != nil {
				return ""
			}
			return wm
		}
		taskState := func() string {
			var state string
			if err := db.QueryRowContext(ctx,
				"SELECT state FROM mo_catalog.mo_cdc_task WHERE task_name = 'cdc_gen_fence_task'").Scan(&state); err != nil {
				return ""
			}
			return state
		}
		sinkErr := func() string {
			var msg string
			_ = db.QueryRowContext(ctx,
				"SELECT IFNULL(w.err_msg, '') FROM mo_catalog.mo_cdc_watermark AS w JOIN mo_catalog.mo_cdc_task AS t "+
					"ON t.account_id = w.account_id AND t.task_id = w.task_id "+
					"WHERE t.task_name = 'cdc_gen_fence_task'").Scan(&msg)
			return msg
		}
		targetID := func() uint64 {
			tx, txErr := db.BeginTx(ctx, nil)
			if txErr != nil {
				require.FailNow(t, "identity call begin failed", txErr.Error())
			}
			defer func() { _ = tx.Rollback() }()
			var id uint64
			require.NoError(t, tx.QueryRowContext(ctx, "CALL mo_cdc_target_identity('cdc_gen_sink2', 't')").Scan(&id))
			return id
		}

		// Snapshot syncs first.
		var snapshotCheckpoint string
		require.Eventually(t, func() bool {
			count, ids := readSink()
			if count != 2 || !strings.Contains(ids, "1") || !strings.Contains(ids, "2") {
				return false
			}
			snapshotCheckpoint = watermarkFor()
			return snapshotCheckpoint != "" && snapshotCheckpoint != "0-0"
		}, 90*time.Second, 500*time.Millisecond, "snapshot must sync before the drift phase")

		// A marker row that must fully sync before the drift arms proves the
		// checkpoint boundary: everything at or below the marker is
		// acknowledged, so the only unacknowledged range after the fence is
		// the drifted row itself.
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_gen_src2.t (id, a, b) VALUES (10, 100, 'marker')"))
		require.Eventually(t, func() bool {
			count, _ := readSink()
			return count == 3
		}, 90*time.Second, 500*time.Millisecond, "the marker row must sync before the drift")
		// One incremental batch so a fence failure cannot be attributed to
		// snapshot-only state.
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_gen_src2.t (id, a, b) VALUES (3, 30, 'gamma')"))
		var preDriftCheckpoint string
		require.Eventually(t, func() bool {
			count, _ := readSink()
			if count != 4 {
				return false
			}
			preDriftCheckpoint = watermarkFor()
			return preDriftCheckpoint != "" && preDriftCheckpoint != snapshotCheckpoint
		}, 90*time.Second, 500*time.Millisecond,
			"marker plus tail batch must sync and advance the checkpoint before the drift")

		preDriftCheckpoint = watermarkFor()
		require.NotEmpty(t, preDriftCheckpoint, "the post-marker checkpoint must be readable")

		idBefore := targetID()

		// Same-ID in-place drift: rename an ordinary writable target column.
		// The table identity is preserved; only the guard's projection fence
		// can observe the change.
		require.NoError(t, execSQL(ctx, db, "ALTER TABLE cdc_gen_sink2.t RENAME COLUMN b TO b_renamed"))
		idAfter := targetID()
		require.Equal(t, idBefore, idAfter,
			"an INPLACE rename keeps the table id: the drift is invisible to the identity guard by design")

		// Attempted replication of a further row must be fenced before any
		// target DML. The sinker duty cycle advances the persisted checkpoint
		// with no-op polls, so the fenced invariant is that the checkpoint
		// FREEZES the moment the fence fires instead of progressing over the
		// drifted data - not bitwise equality with an earlier sample.
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_gen_src2.t (id, a, b) VALUES (9, 90, 'drift')"))
		if !assert.Eventually(t, func() bool {
			return strings.Contains(sinkErr(), "changed schema without being replaced")
		}, 90*time.Second, 500*time.Millisecond,
			"the drifted replication must reach the drift fence") {
			require.FailNow(t, "same-ID drift fence failed",
				"fence_err=%q actual_watermark=%q baseline=%q state=%q",
				sinkErr(), watermarkFor(), preDriftCheckpoint, taskState())
		}
		fenceCheckpoint := watermarkFor()
		require.NotEmpty(t, fenceCheckpoint, "the checkpoint at the failure moment must be readable")
		// The pipeline persists the fence error first and the frontend flips
		// the task state asynchronously a moment later.
		require.Eventually(t, func() bool {
			return taskState() != cdc.CDCState_Running
		}, 30*time.Second, 500*time.Millisecond,
			"the task must fail closed instead of silently omitting or corrupting the projection")

		// Distinct replaced-table and generated-free controls. The sink
		// target is task-created: the upstream first admission refuses a
		// pre-created target for a fresh full-snapshot task, so the control
		// exercises the generated-free creation path itself.
		require.NoError(t, execSQL(ctx, db,
			"CREATE TABLE cdc_gen_src2.gfree (id INT PRIMARY KEY, w INT)"))
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_gen_src2.gfree VALUES (1, 10)"))
		require.NoError(t, execSQL(ctx, db, fmt.Sprintf(
			"CREATE CDC cdc_gen_free_task '%s' 'matrixone' '%s' 'cdc_gen_src2.gfree:cdc_gen_sink2.gfree' {'Level'='table'}",
			uri, uri)))
		defer cleanupExec("drop free cdc task", "DROP CDC TASK cdc_gen_free_task")()
		require.Eventually(t, func() bool {
			var w int
			if err := db.QueryRowContext(ctx,
				"SELECT w FROM cdc_gen_sink2.gfree WHERE id = 1").Scan(&w); err != nil {
				return false
			}
			return w == 10
		}, 90*time.Second, 500*time.Millisecond,
			"the generated-free control must keep the ordinary replication")
		targetFreeID := func() uint64 {
			tx, txErr := db.BeginTx(ctx, nil)
			if txErr != nil {
				require.FailNow(t, "identity call begin failed", txErr.Error())
			}
			defer func() { _ = tx.Rollback() }()
			var id uint64
			require.NoError(t, tx.QueryRowContext(ctx, "CALL mo_cdc_target_identity('cdc_gen_sink2', 'gfree')").Scan(&id))
			return id
		}
		freeIDBefore := targetFreeID()
		require.NoError(t, execSQL(ctx, db, "DROP TABLE cdc_gen_sink2.gfree"))
		require.NoError(t, execSQL(ctx, db,
			"CREATE TABLE cdc_gen_sink2.gfree (id INT PRIMARY KEY, w INT)"))
		require.NotEqual(t, freeIDBefore, targetFreeID(),
			"the recreated control target must carry a distinct identity before the replacement fence")
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_gen_src2.gfree VALUES (2, 20)"))
		freeErr := func() string {
			var msg string
			_ = db.QueryRowContext(ctx,
				"SELECT IFNULL(w.err_msg, '') FROM mo_catalog.mo_cdc_watermark AS w JOIN mo_catalog.mo_cdc_task AS t "+
					"ON t.account_id = w.account_id AND t.task_id = w.task_id "+
					"WHERE t.task_name = 'cdc_gen_free_task'").Scan(&msg)
			return msg
		}
		require.Eventually(t, func() bool {
			return strings.Contains(freeErr(), "was replaced")
		}, 90*time.Second, 500*time.Millisecond,
			"the replaced-table control must fire the replacement fence")
		require.NotContains(t, freeErr(), "changed schema",
			"replacement and drift fences must stay distinguishable")
		t.Logf("replaced-table control: fence=%q", freeErr())
		var drifted int
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM cdc_gen_sink2.t WHERE id = 9 OR id > 10").Scan(&drifted))
		require.Zero(t, drifted, "no target DML for the drifted or post-marker range may land")
		finalCheckpoint := watermarkFor()
		require.Equal(t, fenceCheckpoint, finalCheckpoint,
			"the persisted checkpoint must stay at the failure sample: acknowledging the rejected range would advance it")
		t.Logf("same-ID drift fence: task_id=%d fence=%q frozen_checkpoint==failure_sample=%q pre_drift_checkpoint=%q",
			idBefore, sinkErr(), finalCheckpoint, preDriftCheckpoint)
	})
}

// TestCDCLowPrecisionGeneratedCopyAdmission proves the four low-precision
// generated payload types CopyDecodeSupported newly admits actually flow
// through the REAL guarded writable-target mapping: one source table with an
// ordinary INT PK plus STORED generated BF16/FLOAT16/FLOAT8/FLOAT4 columns,
// one pre-created ordinary writable MO target and one NoFull attached task.
// The source must have stored the four expected values, the target must copy
// the source-materialized values exactly, and the persisted watermark must
// advance strictly past the write's source commit fence.
func TestCDCLowPrecisionGeneratedCopyAdmission(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := int(cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
		defer cancel()
		cleanupExec := func(step, query string) func() {
			return func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cleanupCancel()
				if _, err := db.ExecContext(cleanupCtx, query); err != nil {
					t.Errorf("cleanup %s failed: %v", step, err)
				}
			}
		}
		require.NoError(t, execSQL(ctx, db, "CREATE DATABASE cdc_lpsrc"))
		defer cleanupExec("drop lp source database", "DROP DATABASE IF EXISTS cdc_lpsrc")()
		require.NoError(t, execSQL(ctx, db, "CREATE DATABASE cdc_lpsink"))
		defer cleanupExec("drop lp sink database", "DROP DATABASE IF EXISTS cdc_lpsink")()
		require.NoError(t, execSQL(ctx, db,
			"CREATE TABLE cdc_lpsrc.lp (id INT PRIMARY KEY, a INT, "+
				"gb BF16 GENERATED ALWAYS AS (CAST(a AS BF16)) STORED, "+
				"gh FLOAT16 GENERATED ALWAYS AS (CAST(a + 1 AS FLOAT16)) STORED, "+
				"g8 FLOAT8 GENERATED ALWAYS AS (CAST(a + 2 AS FLOAT8)) STORED, "+
				"g4 FLOAT4 GENERATED ALWAYS AS (CAST(a + 3 AS FLOAT4)) STORED)"))
		require.NoError(t, execSQL(ctx, db,
			"CREATE TABLE cdc_lpsink.lp (id INT PRIMARY KEY, a INT, gb BF16, gh FLOAT16, g8 FLOAT8, g4 FLOAT4)"))
		uri := fmt.Sprintf("mysql://dump:111@127.0.0.1:%d", port)
		require.NoError(t, execSQL(ctx, db, "CREATE PITR cdc_lp_pitr FOR DATABASE cdc_lpsrc RANGE 2 'h'"))
		defer cleanupExec("drop lp pitr", "DROP PITR IF EXISTS cdc_lp_pitr")()
		require.NoError(t, execSQL(ctx, db, fmt.Sprintf(
			"CREATE CDC cdc_lp_task '%s' 'matrixone' '%s' 'cdc_lpsrc.lp:cdc_lpsink.lp' {'Level'='table', 'NoFull'='true'}",
			uri, uri)))
		defer cleanupExec("drop lp cdc task", "DROP CDC TASK cdc_lp_task")()
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_lpsrc.lp (id, a) VALUES (7, 1)"))
		lpFence := func() (types.TS, string) {
			ts := cn.RawService().(cnservice.Service).GetTxnClient().GetLatestCommitTS()
			require.False(t, ts.IsEmpty(), "the source-CN commit fence must be nonempty")
			return types.BuildTS(ts.PhysicalTime, uint32(ts.LogicalTime)), fmt.Sprintf("%d-%d", ts.PhysicalTime, ts.LogicalTime)
		}
		insertFenceTS, insertFenceText := lpFence()
		lpReadback := func(dbName string) (float64, float64, float64, float64, error) {
			var gb, gh, g8, g4 float64
			err := db.QueryRowContext(ctx,
				"SELECT CAST(gb AS DOUBLE), CAST(gh AS DOUBLE), CAST(g8 AS DOUBLE), CAST(g4 AS DOUBLE) FROM "+dbName+".lp WHERE id = 7").
				Scan(&gb, &gh, &g8, &g4)
			return gb, gh, g8, g4, err
		}
		srcB, srcH, src8, src4, err := lpReadback("cdc_lpsrc")
		require.NoError(t, err, "the source must expose its stored low-precision values")
		require.Equal(t, []float64{1, 2, 3, 4}, []float64{srcB, srcH, src8, src4},
			"the source must actually have stored the four expected materialized values")
		t.Logf("source-stored low-precision payloads: bf16=%v float16=%v float8=%v float4=%v", srcB, srcH, src8, src4)
		require.Eventually(t, func() bool {
			gb, gh, g8, g4, err := lpReadback("cdc_lpsink")
			if err != nil {
				return false
			}
			return gb == srcB && gh == srcH && g8 == src8 && g4 == src4
		}, 90*time.Second, 500*time.Millisecond,
			"the ordinary writable target must copy the four source-materialized low-precision values exactly")
		var sinkRows int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cdc_lpsink.lp").Scan(&sinkRows))
		require.Positive(t, sinkRows, "a nonzero target row set must prove the admission branch")
		parseLp := func(s string) (types.TS, bool) {
			physical, logical, ok := strings.Cut(s, "-")
			if !ok || physical == "" || logical == "" {
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
		lpWatermark := func() string {
			var wm string
			if err := db.QueryRowContext(ctx,
				"SELECT w.watermark FROM mo_catalog.mo_cdc_watermark AS w JOIN mo_catalog.mo_cdc_task AS t "+
					"ON t.account_id = w.account_id AND t.task_id = w.task_id "+
					"WHERE t.task_name = 'cdc_lp_task' AND w.db_name = 'cdc_lpsrc' AND w.table_name = 'lp'").Scan(&wm); err != nil {
				return ""
			}
			return wm
		}
		var observed string
		require.Eventually(t, func() bool {
			observed = lpWatermark()
			if observed == "" {
				return false
			}
			wmTS, okW := parseLp(observed)
			return okW && wmTS.GT(&insertFenceTS)
		}, 90*time.Second, 500*time.Millisecond,
			"the persisted catalog watermark must be strictly greater than the write's source commit fence (half-open stream; the persisted watermark is the next range's start)")
		t.Logf("stage insert: source commit fence=%s persisted watermark=%s target_rows=%d", insertFenceText, observed, sinkRows)
		var copiedB, copiedH, copied8, copied4 float64
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT CAST(gb AS DOUBLE), CAST(gh AS DOUBLE), CAST(g8 AS DOUBLE), CAST(g4 AS DOUBLE) FROM cdc_lpsink.lp WHERE id = 7").
			Scan(&copiedB, &copiedH, &copied8, &copied4))
		require.Equal(t, []float64{srcB, srcH, src8, src4}, []float64{copiedB, copiedH, copied8, copied4},
			"the final target row keeps the exact copied values")
	})
}

// TestCDCMixedCaseOrdinaryNoFullAttachment proves the SHOW-routed ordinary
// admission compares the DECLARED spelling of a mixed-case column exactly: a
// NoFull task attaches to a pre-created ordinary target whose mixed-case
// names match the source's declared spelling, exact tail rows replicate, the
// untouched sentinel survives, and the persisted checkpoint strictly
// advances across the witnessed commits.
func TestCDCMixedCaseOrdinaryNoFullAttachment(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := int(cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
		defer cancel()
		cleanupExec := func(step, query string) func() {
			return func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cleanupCancel()
				if _, err := db.ExecContext(cleanupCtx, query); err != nil {
					t.Errorf("cleanup %s failed: %v", step, err)
				}
			}
		}
		require.NoError(t, execSQL(ctx, db, "CREATE DATABASE cdc_mx_src"))
		defer cleanupExec("drop mx source database", "DROP DATABASE IF EXISTS cdc_mx_src")()
		require.NoError(t, execSQL(ctx, db, "CREATE DATABASE cdc_mx_sink"))
		defer cleanupExec("drop mx sink database", "DROP DATABASE IF EXISTS cdc_mx_sink")()
		require.NoError(t, execSQL(ctx, db,
			"CREATE TABLE cdc_mx_src.mx (MyId INT PRIMARY KEY, MyValue VARCHAR(32))"))
		require.NoError(t, execSQL(ctx, db,
			"CREATE TABLE cdc_mx_sink.mx (MyId INT PRIMARY KEY, MyValue VARCHAR(32))"))
		// The NoFull attach keeps the pre-created target: the seeded sentinel
		// never appears on the source and must survive untouched while the
		// tail rows replicate through the mixed-case ordinary mapping.
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_mx_sink.mx VALUES (901, 'sentinel')"))
		uri := fmt.Sprintf("mysql://dump:111@127.0.0.1:%d", port)
		require.NoError(t, execSQL(ctx, db, "CREATE PITR cdc_mx_pitr FOR DATABASE cdc_mx_src RANGE 2 'h'"))
		defer cleanupExec("drop mx pitr", "DROP PITR IF EXISTS cdc_mx_pitr")()
		require.NoError(t, execSQL(ctx, db, fmt.Sprintf(
			"CREATE CDC cdc_mx_task '%s' 'matrixone' '%s' 'cdc_mx_src.mx:cdc_mx_sink.mx' {'Level'='table', 'NoFull'='true'}",
			uri, uri)))
		defer cleanupExec("drop mx cdc task", "DROP CDC TASK cdc_mx_task")()
		parseMx := func(s string) (types.TS, bool) {
			physical, logical, ok := strings.Cut(s, "-")
			if !ok || physical == "" || logical == "" {
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
		mxWatermark := func() string {
			var wm string
			if err := db.QueryRowContext(ctx,
				"SELECT w.watermark FROM mo_catalog.mo_cdc_watermark AS w JOIN mo_catalog.mo_cdc_task AS t "+
					"ON t.account_id = w.account_id AND t.task_id = w.task_id "+
					"WHERE t.task_name = 'cdc_mx_task' AND w.db_name = 'cdc_mx_src' AND w.table_name = 'mx'").Scan(&wm); err != nil {
				return ""
			}
			return wm
		}
		// The persisted watermark flushes asynchronously (watermark_updater
		// runs on its own cadence), so any bare sample could belong to an
		// earlier committed range and a zero TS still parses. Following the
		// established same-CN transaction-client pattern, a NONEMPTY source
		// commit fence is captured immediately after each source write with
		// no intervening read; the persisted catalog watermark must then be
		// STRICTLY greater than that fence - the CDC stream interval is
		// half-open and the persisted watermark is the next range's start -
		// while the exact sink value is asserted first.
		mxCommitFence := func() (types.TS, string) {
			ts := cn.RawService().(cnservice.Service).GetTxnClient().GetLatestCommitTS()
			require.False(t, ts.IsEmpty(), "the source-CN commit fence must be nonempty")
			return types.BuildTS(ts.PhysicalTime, uint32(ts.LogicalTime)), fmt.Sprintf("%d-%d", ts.PhysicalTime, ts.LogicalTime)
		}
		mxPersistedPast := func(stage string, fence types.TS, fenceText string) {
			var observed string
			require.Eventually(t, func() bool {
				observed = mxWatermark()
				if observed == "" {
					return false
				}
				wmTS, okW := parseMx(observed)
				return okW && wmTS.GT(&fence)
			}, 90*time.Second, 500*time.Millisecond,
				"stage %s: the persisted catalog watermark must be strictly greater than the write's commit fence (half-open stream; the persisted watermark is the next range's start)", stage)
			t.Logf("stage %s: source commit fence=%s persisted watermark=%s", stage, fenceText, observed)
		}
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_mx_src.mx VALUES (7, 'mixed-case-a')"))
		insertFenceTS, insertFenceText := mxCommitFence()
		require.Eventually(t, func() bool {
			var v string
			if err := db.QueryRowContext(ctx, "SELECT MyValue FROM cdc_mx_sink.mx WHERE MyId = 7").Scan(&v); err != nil {
				return false
			}
			return v == "mixed-case-a"
		}, 90*time.Second, 500*time.Millisecond,
			"the mixed-case ordinary target must admit through the SHOW route and copy the exact row")
		mxPersistedPast("insert", insertFenceTS, insertFenceText)
		require.NoError(t, execSQL(ctx, db, "UPDATE cdc_mx_src.mx SET MyValue = 'mixed-case-b' WHERE MyId = 7"))
		updateFenceTS, updateFenceText := mxCommitFence()
		require.Eventually(t, func() bool {
			var v string
			if err := db.QueryRowContext(ctx, "SELECT MyValue FROM cdc_mx_sink.mx WHERE MyId = 7").Scan(&v); err != nil {
				return false
			}
			return v == "mixed-case-b"
		}, 90*time.Second, 500*time.Millisecond,
			"the mixed-case update must replicate exactly on the attached ordinary target")
		mxPersistedPast("update", updateFenceTS, updateFenceText)
		var sentinel string
		require.NoError(t, db.QueryRowContext(ctx, "SELECT MyValue FROM cdc_mx_sink.mx WHERE MyId = 901").Scan(&sentinel))
		require.Equal(t, "sentinel", sentinel, "the untouched seeded row must survive")
	})
}

// TestCDCSessionValueCopyAndNoFullSeedOracles closes the original review's
// value-application oracles. Two source writer sessions in different time
// zones materialize independently different stored generated values for the
// same input, and an ordinary writable target must copy exactly those
// values. On a pre-created ordinary target, an overlapping seeded row is
// upserted with exact new values, an untouched sentinel row survives intact,
// and the persisted checkpoint advances across insert, update and delete.
func TestCDCSessionValueCopyAndNoFullSeedOracles(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := int(cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
		defer cancel()
		cleanupExec := func(step, query string) func() {
			return func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cleanupCancel()
				if _, err := db.ExecContext(cleanupCtx, query); err != nil {
					t.Errorf("cleanup %s failed: %v", step, err)
				}
			}
		}
		require.NoError(t, execSQL(ctx, db, "CREATE DATABASE cdc_val_src"))
		defer cleanupExec("drop source database", "DROP DATABASE IF EXISTS cdc_val_src")()
		require.NoError(t, execSQL(ctx, db, "CREATE DATABASE cdc_val_sink"))
		defer cleanupExec("drop sink database", "DROP DATABASE IF EXISTS cdc_val_sink")()
		uri := fmt.Sprintf("mysql://dump:111@127.0.0.1:%d", port)
		require.NoError(t, execSQL(ctx, db, "CREATE PITR cdc_val_pitr FOR DATABASE cdc_val_src RANGE 2 'h'"))
		defer cleanupExec("drop pitr", "DROP PITR IF EXISTS cdc_val_pitr")()

		// ---- Session value-copy oracle ----
		require.NoError(t, execSQL(ctx, db,
			"CREATE TABLE cdc_val_src.tz (id INT PRIMARY KEY, a INT, d DATETIME GENERATED ALWAYS AS (FROM_UNIXTIME(a)) STORED)"))
		// The target column stays ordinary and writable so the source value
		// must be copied, never recomputed in the sink session.
		require.NoError(t, execSQL(ctx, db,
			"CREATE TABLE cdc_val_sink.tz (id INT PRIMARY KEY, a INT, d DATETIME)"))
		// A NoFull task starts from its creation snapshot: the writer
		// sessions insert after that point so each materialized value
		// replicates through the incremental ordinary-copy path.
		require.NoError(t, execSQL(ctx, db, fmt.Sprintf(
			"CREATE CDC cdc_val_task '%s' 'matrixone' '%s' 'cdc_val_src.tz:cdc_val_sink.tz' {'Level'='table', 'NoFull'='true'}",
			uri, uri)))
		defer cleanupExec("drop tz cdc task", "DROP CDC TASK cdc_val_task")()
		s1, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() { _ = s1.Close() }()
		_, tzErr := s1.ExecContext(ctx, "SET time_zone = '+00:00'")
		require.NoError(t, tzErr)
		_, insErr := s1.ExecContext(ctx, "INSERT INTO cdc_val_src.tz (id, a) VALUES (1, 1709236800)")
		require.NoError(t, insErr)
		s2, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() { _ = s2.Close() }()
		_, tz2Err := s2.ExecContext(ctx, "SET time_zone = '+08:00'")
		require.NoError(t, tz2Err)
		_, ins2Err := s2.ExecContext(ctx, "INSERT INTO cdc_val_src.tz (id, a) VALUES (2, 1709236800)")
		require.NoError(t, ins2Err)
		var utcStored, plus8Stored string
		require.NoError(t, db.QueryRowContext(ctx, "SELECT d FROM cdc_val_src.tz WHERE id = 1").Scan(&utcStored))
		require.NoError(t, db.QueryRowContext(ctx, "SELECT d FROM cdc_val_src.tz WHERE id = 2").Scan(&plus8Stored))
		require.NotEqual(t, utcStored, plus8Stored,
			"different writer session time zones must materialize different stored generated values")
		t.Logf("session materialized values: utc=%q+08=%q", utcStored, plus8Stored)
		require.Eventually(t, func() bool {
			var c1, c2 string
			if err := db.QueryRowContext(ctx, "SELECT d FROM cdc_val_sink.tz WHERE id = 1").Scan(&c1); err != nil {
				return false
			}
			if err := db.QueryRowContext(ctx, "SELECT d FROM cdc_val_sink.tz WHERE id = 2").Scan(&c2); err != nil {
				return false
			}
			return c1 == utcStored && c2 == plus8Stored
		}, 90*time.Second, 500*time.Millisecond,
			"the writable target must copy the two session-materialized values exactly")
		_, updErr := s1.ExecContext(ctx, "UPDATE cdc_val_src.tz SET a = 1709236900 WHERE id = 1")
		require.NoError(t, updErr)
		require.Eventually(t, func() bool {
			var shifted, srcShifted string
			if err := db.QueryRowContext(ctx, "SELECT d FROM cdc_val_src.tz WHERE id = 1").Scan(&srcShifted); err != nil {
				return false
			}
			if srcShifted == utcStored {
				return false
			}
			if err := db.QueryRowContext(ctx, "SELECT d FROM cdc_val_sink.tz WHERE id = 1").Scan(&shifted); err != nil {
				return false
			}
			return shifted == srcShifted
		}, 90*time.Second, 500*time.Millisecond,
			"an incremental change must copy the new session-materialized value")

		// ---- Seeded NoFull oracle ----
		require.NoError(t, execSQL(ctx, db,
			"CREATE TABLE cdc_val_src.nf (id INT PRIMARY KEY, a INT, g INT GENERATED ALWAYS AS (a + 1) STORED)"))
		require.NoError(t, execSQL(ctx, db,
			"CREATE TABLE cdc_val_sink.nf (id INT PRIMARY KEY, a INT, g INT)"))
		// A NoFull task starts from its creation snapshot and never runs the
		// initial full phase, so the pre-created target with its seeds is
		// legal. The seeded row overlaps the upcoming source insert; the
		// sentinel never appears in the source and must survive untouched.
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_val_sink.nf VALUES (900, 7, 999)"))
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_val_sink.nf VALUES (901, 8, 1234)"))
		require.NoError(t, execSQL(ctx, db, fmt.Sprintf(
			"CREATE CDC cdc_val_nf_task '%s' 'matrixone' '%s' 'cdc_val_src.nf:cdc_val_sink.nf' {'Level'='table', 'NoFull'='true'}",
			uri, uri)))
		defer cleanupExec("drop nf cdc task", "DROP CDC TASK cdc_val_nf_task")()
		parseNf := func(s string) (types.TS, bool) {
			physical, logical, ok := strings.Cut(s, "-")
			if !ok || physical == "" || logical == "" {
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
		nfWatermark := func() string {
			var wm string
			if err := db.QueryRowContext(ctx,
				"SELECT w.watermark FROM mo_catalog.mo_cdc_watermark AS w JOIN mo_catalog.mo_cdc_task AS t "+
					"ON t.account_id = w.account_id AND t.task_id = w.task_id "+
					"WHERE t.task_name = 'cdc_val_nf_task' AND w.db_name = 'cdc_val_src' AND w.table_name = 'nf'").Scan(&wm); err != nil {
				return ""
			}
			return wm
		}
		// The persisted watermark flushes asynchronously, so bare samples can
		// reflect earlier committed ranges and zero parses; every stage below
		// captures a NONEMPTY source-CN commit fence immediately after its
		// source write (no intervening read), asserts the exact sink value,
		// and then requires the PERSISTED catalog watermark to be strictly
		// greater than that write's fence - half-open [from,to) semantics:
		// the persisted watermark is the next range's start.
		nfCommitFence := func() (types.TS, string) {
			ts := cn.RawService().(cnservice.Service).GetTxnClient().GetLatestCommitTS()
			require.False(t, ts.IsEmpty(), "the source-CN commit fence must be nonempty")
			return types.BuildTS(ts.PhysicalTime, uint32(ts.LogicalTime)), fmt.Sprintf("%d-%d", ts.PhysicalTime, ts.LogicalTime)
		}
		nfPersistedPast := func(stage string, fence types.TS, fenceText string) {
			var observed string
			require.Eventually(t, func() bool {
				observed = nfWatermark()
				if observed == "" {
					return false
				}
				wmTS, okW := parseNf(observed)
				return okW && wmTS.GT(&fence)
			}, 90*time.Second, 500*time.Millisecond,
				"stage %s: the persisted catalog watermark must be strictly greater than the write's commit fence (half-open stream; the persisted watermark is the next range's start)", stage)
			t.Logf("stage %s: source commit fence=%s persisted watermark=%s", stage, fenceText, observed)
		}
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_val_src.nf (id, a) VALUES (900, 9)"))
		insertFenceTS, insertFenceText := nfCommitFence()
		var srcGen int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT g FROM cdc_val_src.nf WHERE id = 900").Scan(&srcGen))
		require.Equal(t, 10, srcGen, "the source must materialize the stored generated value")
		require.Eventually(t, func() bool {
			var a, g int
			if err := db.QueryRowContext(ctx, "SELECT a, g FROM cdc_val_sink.nf WHERE id = 900").Scan(&a, &g); err != nil {
				return false
			}
			return a == 9 && g == 10
		}, 90*time.Second, 500*time.Millisecond,
			"the insert must upsert the seeded row copying the generated value 10, replacing the stale seeded g")
		nfPersistedPast("insert", insertFenceTS, insertFenceText)
		require.NoError(t, execSQL(ctx, db, "UPDATE cdc_val_src.nf SET a = 10 WHERE id = 900"))
		updateFenceTS, updateFenceText := nfCommitFence()
		var srcGenUpd int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT g FROM cdc_val_src.nf WHERE id = 900").Scan(&srcGenUpd))
		require.Equal(t, 11, srcGenUpd, "the source must recompute the stored generated value")
		require.Eventually(t, func() bool {
			var a, g int
			if err := db.QueryRowContext(ctx, "SELECT a, g FROM cdc_val_sink.nf WHERE id = 900").Scan(&a, &g); err != nil {
				return false
			}
			return a == 10 && g == 11
		}, 90*time.Second, 500*time.Millisecond, "the update must copy the recomputed generated value exactly")
		nfPersistedPast("update", updateFenceTS, updateFenceText)
		require.NoError(t, execSQL(ctx, db, "DELETE FROM cdc_val_src.nf WHERE id = 900"))
		deleteFenceTS, deleteFenceText := nfCommitFence()
		var srcGone int
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM cdc_val_src.nf WHERE id = 900").Scan(&srcGone))
		require.Zero(t, srcGone, "the source delete must actually remove the row before the sink is asserted")
		require.Eventually(t, func() bool {
			var n int
			if err := db.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM cdc_val_sink.nf WHERE id = 900").Scan(&n); err != nil {
				return false
			}
			return n == 0
		}, 90*time.Second, 500*time.Millisecond, "the sink row must disappear after the source delete")
		nfPersistedPast("delete", deleteFenceTS, deleteFenceText)
		var sentinelA, sentinelG int
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT a, g FROM cdc_val_sink.nf WHERE id = 901").Scan(&sentinelA, &sentinelG))
		require.Equal(t, 8, sentinelA, "the untouched sentinel row must keep its seeded a")
		require.Equal(t, 1234, sentinelG, "the untouched sentinel row must keep its seeded g")
		// The per-stage persisted watermarks are already logged by
		// nfPersistedPast alongside their exact source commit fences.

		// Native negative: an ordinary source-shaped mapping attaching to a
		// same-shape GENERATED target must fail closed at admission, before
		// any write or acknowledgement.
		require.NoError(t, execSQL(ctx, db,
			"CREATE TABLE cdc_val_src.gneg (id INT PRIMARY KEY, a INT, g INT)"))
		require.NoError(t, execSQL(ctx, db,
			"CREATE TABLE cdc_val_sink.gneg (id INT PRIMARY KEY, a INT, g INT GENERATED ALWAYS AS (a + 1) STORED)"))
		require.NoError(t, execSQL(ctx, db, fmt.Sprintf(
			"CREATE CDC cdc_val_neg_task '%s' 'matrixone' '%s' 'cdc_val_src.gneg:cdc_val_sink.gneg' {'Level'='table', 'NoFull'='true'}",
			uri, uri)))
		defer cleanupExec("drop negative cdc task", "DROP CDC TASK cdc_val_neg_task")()
		require.NoError(t, execSQL(ctx, db, "INSERT INTO cdc_val_src.gneg VALUES (1, 5, 6)"))
		gnegErr := func() string {
			var msg string
			_ = db.QueryRowContext(ctx,
				"SELECT IFNULL(w.err_msg, '') FROM mo_catalog.mo_cdc_watermark AS w JOIN mo_catalog.mo_cdc_task AS t "+
					"ON t.account_id = w.account_id AND t.task_id = w.task_id "+
					"WHERE t.task_name = 'cdc_val_neg_task'").Scan(&msg)
			return msg
		}
		require.Eventually(t, func() bool {
			msg := gnegErr()
			return msg != "" && (strings.Contains(msg, "not supported") || strings.Contains(msg, "ordinary writable column"))
		}, 90*time.Second, 500*time.Millisecond,
			"a same-shape generated target must fail closed at admission with the generated alternatives stated")
		var negRows int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cdc_val_sink.gneg").Scan(&negRows))
		require.Zero(t, negRows, "no row may land on a target fenced at admission")
		t.Logf("generated-target negative fence: %q", gnegErr())
	})
}
