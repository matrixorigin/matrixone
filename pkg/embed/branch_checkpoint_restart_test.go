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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

// dataBranchRow is one DATA BRANCH DIFF output row reduced to the fields the
// branch lifecycle scenarios assert on: the owning table, the change flag,
// and the row's (id, v) payload.
type dataBranchRow struct {
	scope string
	flag  string
	id    int
	v     int
}

// branchRestartScenario encodes one issue #27465 lifecycle: the exact
// DDL/DML of the reproduction, the flush/checkpoint/GC actions that shift
// the service onto checkpoint-backed history, and the assertions that must
// hold before and after the scenario's own full-cluster restarts. Every
// scenario owns a dedicated embedded cluster, so its knobs, catalog, and
// storage never leak into the other scenarios and each one can be selected
// alone with -run TestDataBranchDiffAcrossCheckpointGCAndRestart/<name>.
type branchRestartScenario struct {
	name      string
	database  string
	setup     []string
	lifecycle []string

	// reclaimProbeTable, when non-empty, asks the harness to prove that TN
	// disk GC actually ran before the restart: the probe table's objects are
	// captured after its flush, the table is dropped, and the test waits,
	// bounded, for the files to be deleted from the TN's shared storage
	// root. The probe table shares the scenario's database and time window
	// but has no branch protection, so its reclamation is observable
	// evidence that a real GC cycle completed — the mo_ctl diskcleaner
	// force_gc RPC alone is not, because it silently enqueues no work when
	// every CN reports an empty MinTimestamp, which is the steady state of
	// a quiet test cluster.
	//
	// Setting this field also enables this scenario's accelerated TN GC
	// knobs (2s scans, 1ms TTLs, 2s incremental checkpoints): the shipped
	// defaults (1h gc-ttl, 30m scan-gc-interval) make checkpoints younger
	// than an hour ineligible for scanning, which is the right policy for
	// production but leaves a seconds-old test cluster unreachable for the
	// probe. The knob pattern matches the repo's storage UTs
	// (pkg/vm/engine/tae/testutils/config).
	reclaimProbeTable string

	// restarts counts the full-cluster stop/start cycles the scenario must
	// survive with every assertion passing after each one. The GC scenario
	// runs two cycles so cumulative branch survival across repeated
	// shutdowns stays covered on the reproducer itself; the controls run
	// exactly one, matching their single-restart role in the issue's
	// published matrix.
	restarts int

	wantDiffRows []dataBranchRow
	rootTable    string
	leafTable    string
	wantRootRows int
	wantLeafRows int
}

// TestDataBranchDiffAcrossCheckpointGCAndRestart is the executable
// regression for issue #27465: GC, checkpointing, and a full service
// restart must not invalidate the history a live data branch needs for
// DIFF.
//
// Scenario gc_checkpoint_restart replays the issue body's one-edge
// reproduction exactly (two root mutations, two leaf mutations, flush,
// global checkpoint, forced disk GC), proves through the reclaim probe
// that the TN's disk-GC cycle completed before the restart, and then
// survives two full restarts — the cumulative branch-survival half of the
// original matrix, kept on the reproducer rather than on the controls.
// Scenario checkpoint_restart is the minimized checkpoint-only trigger
// recorded on the issue on 2026-10-09. Scenario flush_only_restart is the
// published control: without a global checkpoint the same restart never
// lost the historical catalog read.
//
// Only the GC scenario's private cluster gets the accelerated TN GC
// knobs. The controls keep production TTLs, whose one-hour gate keeps a
// minutes-old test cluster ineligible for the disk cleaner — that is what
// makes them genuinely GC-free controls, distinguishable from the
// GC/restart coverage they are compared against.
func TestDataBranchDiffAcrossCheckpointGCAndRestart(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())

	scenarios := []branchRestartScenario{
		{
			name:     "gc_checkpoint_restart",
			database: "db_gc_restart",
			setup: []string{
				"create database db_gc_restart",
				"create table db_gc_restart.root(id int primary key, v int)",
				"insert into db_gc_restart.root values (1,1),(2,2),(3,3),(4,4)",
				"data branch create table db_gc_restart.leaf from db_gc_restart.root",
				"update db_gc_restart.root set v=101 where id=1",
				"delete from db_gc_restart.root where id=2",
				"update db_gc_restart.leaf set v=303 where id=3",
				"insert into db_gc_restart.leaf values(5,505)",
			},
			lifecycle: []string{
				"select mo_ctl('dn','flush','db_gc_restart.root')",
				"select mo_ctl('dn','flush','db_gc_restart.leaf')",
				"select mo_ctl('dn','globalcheckpoint','')",
				"select mo_ctl('dn','diskcleaner','force_gc')",
				"select mo_ctl('dn','globalcheckpoint','')",
				"select mo_ctl('dn','diskcleaner','force_gc')",
			},
			reclaimProbeTable: "gc_probe",
			restarts:          2,
			wantDiffRows: []dataBranchRow{
				{scope: "root", flag: "UPDATE", id: 1, v: 101},
				{scope: "root", flag: "DELETE", id: 2, v: 2},
				{scope: "leaf", flag: "UPDATE", id: 3, v: 303},
				{scope: "leaf", flag: "INSERT", id: 5, v: 505},
			},
			rootTable:    "root",
			leafTable:    "leaf",
			wantRootRows: 3,
			wantLeafRows: 5,
		},
		{
			name:     "checkpoint_restart",
			database: "db_checkpoint_restart",
			setup: []string{
				"create database db_checkpoint_restart",
				"create table db_checkpoint_restart.base(id int primary key, v int)",
				"insert into db_checkpoint_restart.base values (1,10),(2,20)",
				"data branch create table db_checkpoint_restart.br from db_checkpoint_restart.base",
				"update db_checkpoint_restart.br set v=11 where id=1",
			},
			lifecycle: []string{
				"select mo_ctl('dn','flush','db_checkpoint_restart.base')",
				"select mo_ctl('dn','flush','db_checkpoint_restart.br')",
				"select mo_ctl('dn','globalcheckpoint','')",
			},
			restarts: 1,
			wantDiffRows: []dataBranchRow{
				{scope: "br", flag: "UPDATE", id: 1, v: 11},
			},
			rootTable:    "base",
			leafTable:    "br",
			wantRootRows: 2,
			wantLeafRows: 2,
		},
		{
			name:     "flush_only_restart",
			database: "db_flush_restart",
			setup: []string{
				"create database db_flush_restart",
				"create table db_flush_restart.base(id int primary key, v int)",
				"insert into db_flush_restart.base values (1,10),(2,20)",
				"data branch create table db_flush_restart.br from db_flush_restart.base",
				"update db_flush_restart.br set v=11 where id=1",
			},
			lifecycle: []string{
				"select mo_ctl('dn','flush','db_flush_restart.base')",
				"select mo_ctl('dn','flush','db_flush_restart.br')",
			},
			restarts: 1,
			wantDiffRows: []dataBranchRow{
				{scope: "br", flag: "UPDATE", id: 1, v: 11},
			},
			rootTable:    "base",
			leafTable:    "br",
			wantRootRows: 2,
			wantLeafRows: 2,
		},
	}

	for i := range scenarios {
		sc := scenarios[i]
		t.Run(sc.name, func(t *testing.T) {
			runBranchRestartScenario(t, sc)
		})
	}
}

// runBranchRestartScenario executes one scenario end to end inside its own
// subtest: build a private cluster with the scenario's knobs, replay setup
// and lifecycle, prove reclamation when the scenario asks for it, assert
// the pre-restart branch contract, then close and restart the cluster the
// scenario's restart count times, asserting the same contract after every
// restart. A failure anywhere fails only this scenario's subtest and no
// later step depends on any other scenario, so each one also runs
// standalone.
func runBranchRestartScenario(t *testing.T, sc branchRestartScenario) {
	setupCtx := t.Context()
	if deadline, ok := t.Deadline(); ok {
		var cancel context.CancelFunc
		setupCtx, cancel = context.WithDeadline(setupCtx, deadline)
		defer cancel()
	}

	c, err := NewCluster(
		WithTesting(),
		WithTestAdmissionContext(setupCtx),
		WithCNCount(1),
		WithPreStart(func(svc ServiceOperator) {
			adjustBasicClusterService(svc)
			if sc.reclaimProbeTable == "" ||
				svc.ServiceType() != metadata.ServiceType_TN {
				return
			}
			svc.Adjust(func(cfg *ServiceConfig) {
				tn := cfg.TN_please_use_getTNServiceConfig
				if tn == nil {
					tn = cfg.TNCompatible
				}
				if tn != nil {
					tn.GCCfg.ScanGCInterval.Duration = 2 * time.Second
					tn.GCCfg.GCTTL.Duration = time.Millisecond
					tn.GCCfg.GCInMemoryTTL.Duration = time.Millisecond
					tn.Ckp.IncrementalInterval.Duration = 2 * time.Second
				}
			})
		}),
	)
	if c != nil {
		t.Cleanup(func() {
			require.NoError(t, c.Close())
			require.NoError(t, os.RemoveAll(c.(*cluster).options.dataPath))
		})
	}
	require.NoError(t, err)

	// The reclaim wait rides on this harness's slow background cadences
	// (forced global checkpoints complete on a roughly per-minute boundary
	// in a quiet embedded cluster, and reclamation has needed up to four of
	// those cycles after a drop), so the default five-minute workload
	// budget could expire inside the first generation. Fifteen minutes
	// leaves wide margin while keeping the budget finite.
	start, finish := newRecoveryClusterStarterWithBudget(
		setupCtx, c.Start, time.Now, 15*time.Minute,
	)
	defer finish()
	// The starter's generation context carries the workload budget and
	// excludes startup/admission time; every SQL call must run on the
	// context of the generation that serves it, refreshed after restart.
	ctx, err := start()
	require.NoError(t, err)

	// Every SQL client is closed by its own cleanup, so a mid-test require
	// failure cannot leak the client's opener goroutine; explicit closes
	// before each restart are retained to release connections
	// deterministically.
	openConn := func() *sql.DB {
		cn, cnErr := c.GetCNService(0)
		require.NoError(t, cnErr)
		client := recoverySQLClient(t, cn)
		t.Cleanup(func() { _ = client.Close() })
		return client
	}

	db := openConn()
	for _, statement := range append(append([]string{}, sc.setup...), sc.lifecycle...) {
		_, execErr := db.ExecContext(ctx, statement)
		require.NoError(t, execErr, statement)
	}
	if sc.reclaimProbeTable != "" {
		assertDiskGCReclaimsProbe(t, ctx, db, c, sc)
	}
	assertBranchScenario(t, ctx, db, sc)

	for restart := 1; restart <= sc.restarts; restart++ {
		require.NoError(t, db.Close())
		require.NoError(t, c.Close())
		ctx, err = start()
		require.NoError(t, err)
		db = openConn()
		assertBranchScenario(t, ctx, db, sc)
	}
	require.NoError(t, db.Close())
}

// assertDiskGCReclaimsProbe proves that the scenario's GC leg really
// reached physical reclamation before the restart: flush the probe table,
// record its live data objects through metadata_scan, drop it, take a
// global checkpoint over the drop, then wait — bounded — for the disk
// cleaner to delete those objects from the TN's shared storage root while
// asserting the branch-owned scenario tables stay protected.
//
// The wait keeps issuing a global checkpoint each round: the cleaner only
// GCs against a global checkpoint once a strictly newer one exists
// (its GC watermark is pinned at the latest scanned global checkpoint), so
// a bounded alternation of "new global checkpoint, re-check files" is the
// deterministic completion condition. Everything fails loudly through
// require if the deadline passes with objects left, which is exactly the
// red state expected when the disk cleaner silently does nothing.
//
// The alternation must tolerate this harness's slow checkpoint cadence: on
// a quiet embedded cluster every forced global checkpoint completes on a
// roughly per-minute boundary, so DB.ForceGlobalCheckpoint spends ~55s in
// ForceICKP no-progress retries (ICKP-Schedule-Force-End logs
// no-progress-retries=54-59) and each round below can cost a minute. The
// commit heartbeat keeps WAL traffic flowing so the checkpoint machinery
// has fresh boundaries to work against; the deadline is sized for the
// slowest observed cadence, not for the fast path.
func assertDiskGCReclaimsProbe(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	c Cluster,
	sc branchRestartScenario,
) {
	t.Helper()

	probeTable := fmt.Sprintf("%s.%s", sc.database, sc.reclaimProbeTable)
	for _, statement := range []string{
		fmt.Sprintf("create table %s(id int primary key, v int)", probeTable),
		fmt.Sprintf("insert into %s values (1,1),(2,2),(3,3)", probeTable),
		fmt.Sprintf("select mo_ctl('dn','flush','%s')", probeTable),
	} {
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}

	objectNames := func() []string {
		nameRows, err := db.QueryContext(ctx,
			fmt.Sprintf("select object_name from metadata_scan('%s', 'id') m", probeTable))
		require.NoError(t, err)
		defer func() { require.NoError(t, nameRows.Close()) }()
		var names []string
		for nameRows.Next() {
			var name string
			require.NoError(t, nameRows.Scan(&name))
			names = append(names, name)
		}
		require.NoError(t, nameRows.Err())
		return names
	}()
	require.NotEmpty(t, objectNames, "probe table produced no flushed objects to track")

	sharedRoot := filepath.Join(c.(*cluster).options.dataPath, "s3")
	for _, name := range objectNames {
		_, statErr := os.Stat(filepath.Join(sharedRoot, name))
		require.NoError(t, statErr,
			"probe object %s must exist in the TN shared storage root before the drop", name)
	}

	_, err := db.ExecContext(ctx, fmt.Sprintf("drop table %s", probeTable))
	require.NoError(t, err)

	stopHeartbeat, heartbeatDone := startCommitHeartbeat(ctx, db)
	defer func() {
		stopHeartbeat()
		require.NoError(t, <-heartbeatDone)
	}()

	_, err = db.ExecContext(ctx, "select mo_ctl('dn','globalcheckpoint','')")
	require.NoError(t, err)

	waitStart := time.Now()
	// The deadline must absorb this harness's slow checkpoint cadence: in
	// the quiet embedded cluster every forced global checkpoint completes
	// on a roughly per-minute boundary (observed Force-GCKP-End costs of
	// 54-58s with 54-59 ForceICKP no-progress retries, in this harness
	// across runs), so each round below can cost a minute; the slowest
	// observed reclamation needed three rounds after the drop checkpoint.
	// Reclamation itself has always completed — the deadline only needs to
	// be generous enough to never cut a working cadence short.
	deadline := waitStart.Add(6 * time.Minute)
	for round := 1; ; round++ {
		require.NoError(t, ctx.Err())
		allReclaimed := true
		for _, name := range objectNames {
			if _, statErr := os.Stat(filepath.Join(sharedRoot, name)); statErr == nil {
				allReclaimed = false
				break
			}
		}
		if allReclaimed {
			t.Logf("probe objects %v reclaimed after %d checkpoint rounds in %s",
				objectNames, round-1, time.Since(waitStart).Round(time.Millisecond))
			return
		}
		require.True(t, time.Now().Before(deadline),
			"disk GC did not reclaim probe objects %v in the scenario database before the restart", objectNames)
		_, err = db.ExecContext(ctx, "select mo_ctl('dn','globalcheckpoint','')")
		require.NoError(t, err)
		time.Sleep(2 * time.Second)
	}
}

// startCommitHeartbeat commits a tiny insert on a short interval until the
// returned stop function is called. A quiet embedded cluster has no WAL
// traffic besides what the probe itself commits, and the checkpoint
// machinery in that state schedules work on its own ~60s cadence
// (Force-GCKP-End shows 54-58s stalls bounded by ForceICKP no-progress
// retries). The heartbeat's steady commit stream keeps fresh WAL
// boundaries available so each forced checkpoint has new content to
// advance through when the cadence fires.
//
// The heartbeat runs on its own context derived from the caller's: stop
// closes stopCh and cancels that context, so a statement blocked inside
// ExecContext is released immediately and the caller's join on done cannot
// outlive the scenario context. A statement that fails because stop was
// called — during setup DDL or during inserts — is a clean exit, not a
// heartbeat failure. The returned channel is closed when the goroutine
// exits and yields the first genuine failure, or nil if the heartbeat was
// stopped or the parent context ended it.
func startCommitHeartbeat(ctx context.Context, db *sql.DB) (stop func(), done <-chan error) {
	hbCtx, cancel := context.WithCancel(ctx)
	stopCh := make(chan struct{})
	doneCh := make(chan error, 1)
	go func() {
		defer close(doneCh)
		defer cancel()
		for _, statement := range []string{
			"create database if not exists gc_progress_heartbeat",
			"create table if not exists gc_progress_heartbeat.tick(id int primary key)",
		} {
			if _, err := db.ExecContext(hbCtx, statement); err != nil {
				select {
				case <-stopCh:
					// Stopped while the statement was in flight.
					return
				default:
					doneCh <- err
					return
				}
			}
		}
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for n := 1; ; n++ {
			select {
			case <-stopCh:
				return
			default:
			}
			if _, err := db.ExecContext(hbCtx,
				fmt.Sprintf("insert into gc_progress_heartbeat.tick values (%d)", n)); err != nil {
				select {
				case <-stopCh:
					// Stopped while the statement was in flight.
					return
				default:
					doneCh <- err
					return
				}
			}
			select {
			case <-stopCh:
				return
			case <-hbCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() {
		close(stopCh)
		cancel()
	}, doneCh
}

// assertBranchScenario checks the full post-lifecycle contract of one
// scenario: exact DIFF rows and count, current table contents, and the
// live branch protection record.
func assertBranchScenario(t *testing.T, ctx context.Context, db *sql.DB, sc branchRestartScenario) {
	t.Helper()

	diffQuery := fmt.Sprintf(
		"data branch diff %s.%s against %s.%s",
		sc.database, sc.leafTable, sc.database, sc.rootTable,
	)
	got := func() []dataBranchRow {
		rows, err := db.QueryContext(ctx, diffQuery)
		require.NoError(t, err, diffQuery)
		defer func() { require.NoError(t, rows.Close()) }()
		var out []dataBranchRow
		for rows.Next() {
			var row dataBranchRow
			require.NoError(t, rows.Scan(&row.scope, &row.flag, &row.id, &row.v))
			out = append(out, row)
		}
		require.NoError(t, rows.Err())
		return out
	}()
	require.ElementsMatch(t, sc.wantDiffRows, got, diffQuery)

	var count int
	require.NoError(t, db.QueryRowContext(ctx, diffQuery+" output count").Scan(&count))
	require.Equal(t, len(sc.wantDiffRows), count, diffQuery+" output count")

	for _, table := range []struct {
		name string
		want int
	}{
		{sc.rootTable, sc.wantRootRows},
		{sc.leafTable, sc.wantLeafRows},
	} {
		var tableRows int
		require.NoError(t, db.QueryRowContext(ctx,
			fmt.Sprintf("select count(*) from %s.%s", sc.database, table.name)).Scan(&tableRows))
		require.Equal(t, table.want, tableRows, sc.database+"."+table.name)
	}

	var protection int
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
		"select count(*) from mo_catalog.mo_snapshots where kind = 'branch' "+
			"and database_name = '%s' and table_name = '%s'",
		sc.database, sc.rootTable,
	)).Scan(&protection))
	require.Equal(t, 1, protection, "branch protection record for "+sc.database+"."+sc.rootTable)
}
