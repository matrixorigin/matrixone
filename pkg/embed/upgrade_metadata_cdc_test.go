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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_4"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_5"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_6"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// columns46ShapeChain truncates the production chain at v4_0_6 so that a
// fresh embed generation finalizes READY at 4.0.6/5 — the 4.2-era catalog
// shape a stopped pre-upgrade deployment presents to the new binary.
func columns46ShapeChain() []bootstrap.VersionHandle {
	return []bootstrap.VersionHandle{v4_0_4.Handler, v4_0_5.Handler, v4_0_6.Handler}
}

// stageReady406Predecessor boots one generation on the 4.2-shaped chain,
// installs the mask probes and the undecorated persisted COLUMNS definition
// in the sys tenant, and stops holding READY-4.0.6/5 content.
func stageReady406Predecessor(t *testing.T, budget time.Duration) (Cluster, func() (context.Context, error)) {
	t.Helper()
	c := newColumnsUpgradeCluster(t, t.Context(), columns46ShapeChain())
	start, cleanup := newRecoveryClusterStarterWithBudget(t.Context(), c.Start, time.Now, budget)
	t.Cleanup(cleanup)
	ctx, err := start()
	require.NoError(t, err)
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	db := recoverySQLClient(t, cn)
	defer db.Close()
	for _, statement := range []string{
		"create database columns_upgrade",
		"create table columns_upgrade.zf (x decimal(10,2) unsigned zerofill)",
		"create table columns_upgrade.u (x decimal(10,2) unsigned)",
		"create table columns_upgrade.plain (x decimal(10,2))",
		"insert into columns_upgrade.u values (1.5)",
		"insert into columns_upgrade.zf values (1.5)",
		"insert into columns_upgrade.plain values (1.5)",
	} {
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	// The pre-change binary persisted the undecorated template; install it via
	// the internal executor (the wire user holds no privilege to replace an
	// information_schema view) so every later boot starts from the predecessor
	// content. The periodic COLUMNS/TABLES maintenance walker was removed
	// upstream (#31662), so nothing recreates the staged predecessor text
	// outside the versioned entries. Read back the exact predecessor state
	// before crossing the next version boundary.
	execInternalStatement(t, ctx, cn, "DROP VIEW IF EXISTS information_schema.COLUMNS;")
	execInternalStatement(t, ctx, cn, preChangeCurrentColumnsDDL())
	requireAccountColumnsDefinitionsPreChange(t, ctx, cn, 0)
	require.NoError(t, c.Stop())
	return c, start
}

// execInternalStatement runs one catalog fixture statement through the CN
// internal executor for account 0. test fixtures touch restricted catalog
// objects (information_schema.COLUMNS, mo_catalog.mo_cdc_task) whose wire
// privileges deny even the moadmin role; the upgrade bootstrapper itself
// writes the same objects through the internal executor.
func execInternalStatement(t *testing.T, ctx context.Context, cn ServiceOperator, statement string) {
	t.Helper()
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	res, err := exec.Exec(ctx, statement,
		executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithAccountID(0).WithWaitCommittedLogApplied())
	require.NoError(t, err, statement)
	res.Close()
}

// readCDCTaskNames returns every mo_cdc_task task name visible to account 0
// through the internal executor (wire reads of the restricted table are
// denied).
func readCDCTaskNames(t *testing.T, ctx context.Context, cn ServiceOperator) []string {
	t.Helper()
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	res, err := exec.Exec(ctx, "select task_name from mo_catalog.mo_cdc_task order by task_name",
		executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithAccountID(0).WithWaitCommittedLogApplied())
	require.NoError(t, err)
	defer res.Close()
	var names []string
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		names = append(names, executor.GetStringRows(cols[0])...)
		return true
	})
	return names
}

// seedLegacyCDCTask plants one mo_cdc_task row, creating the table with the
// production DDL when the staged chain does not own it.
func seedLegacyCDCTask(t *testing.T, ctx context.Context, cn ServiceOperator, name, state string) {
	t.Helper()
	db := recoverySQLClient(t, cn)
	defer db.Close()
	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		"select count(*) from information_schema.tables where table_schema='mo_catalog' and table_name='mo_cdc_task'").Scan(&count))
	if count == 0 {
		execInternalStatement(t, ctx, cn, frontend.MoCatalogMoCdcTaskDDL)
	}
	execInternalStatement(t, ctx, cn, fmt.Sprintf(
		"insert into mo_catalog.mo_cdc_task (account_id, task_id, task_name, source_uri, sink_uri, tables, state) "+
			"values (0, '6ba7b810-9dad-11d1-80b4-00c04fd430c8', '%s', 'mysql://src', 'mysql://sink', 'd.t', '%s')",
		name, state))
}

// requireNoRouteAdvance pins the pre-mutation abort: no 4.0.7 or 4.0.7
// version marker, and no mo_upgrade rows at all. Reads go through the
// internal executor like the upgrade framework's own catalog probes; the
// wire user holds no mo_catalog read privilege.
func requireNoRouteAdvance(t *testing.T, ctx context.Context, cn ServiceOperator) {
	t.Helper()
	require.Zero(t, countInternalRows(t, ctx, cn,
		"select count(*) from mo_catalog.mo_version where version = '4.0.7'"),
		"the aborted boot must not persist any new version marker")
	require.Zero(t, countInternalRows(t, ctx, cn,
		"select count(*) from mo_catalog.mo_upgrade"),
		"the aborted boot must not persist any route step")
}

// countInternalRows runs one single-row count through the CN internal
// executor for account 0 and returns the count.
func countInternalRows(t *testing.T, ctx context.Context, cn ServiceOperator, statement string) int64 {
	t.Helper()
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	res, err := exec.Exec(ctx, statement,
		executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithAccountID(0).WithWaitCommittedLogApplied())
	require.NoError(t, err, statement)
	defer res.Close()
	var counts []int64
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		counts = append(counts, executor.GetFixedRows[int64](cols[0])...)
		return true
	})
	require.Len(t, counts, 1, statement)
	return counts[0]
}

// requireColumnsCrossingComplete pins a completed 4.0.6 -> 4.0.7 crossing
// route: both steps Ready with converged tenant counters for the staged
// account count.
func requireColumnsCrossingComplete(t *testing.T, ctx context.Context, cn ServiceOperator, tenants int32) {
	t.Helper()
	final, steps := readRecoveryCatalog(t, ctx, cn, v4_0_7.Handler.Metadata())
	require.Equal(t, versions.StateReady, final.State)
	require.Len(t, steps, 1)
	require.Equal(t, "4.0.6", steps[0].FromVersion)
	require.Equal(t, "4.0.7", steps[0].ToVersion)
	require.Equal(t, versions.StateReady, steps[0].State)
	require.Equal(t, tenants, steps[0].TotalTenant)
	require.Equal(t, steps[0].TotalTenant, steps[0].ReadyTenant)

}

// TestColumnsUpgradeLegacyCDCAbortsBootBeforeCrossing is validation item
// (vi)'s legacy-CDC abort leg: a 4.2-shaped catalog holding one pre-upgrade
// mo_cdc_task row aborts the automatic boot with the terminal task
// attribution before the 4.0.7 step commits — no route advance, the named
// task row preserved — and the operator remedy (remove the task on the
// source version) completes the crossing to READY 4.0.7.
func TestColumnsUpgradeLegacyCDCAbortsBootBeforeCrossing(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c, start := stageReady406Predecessor(t, 10*time.Minute)
	var ctx context.Context
	var err error

	func() {
		ctx, err = start()
		require.NoError(t, err)
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		seedLegacyCDCTask(t, ctx, cn, "legacy-task", "paused")
		require.NoError(t, c.Stop())
	}()

	setRecoveryCNHandles(c, append(columns46ShapeChain(), v4_0_7.Handler), true)
	_, err = start()
	require.Error(t, err, "a catalog with a legacy CDC task must abort the automatic boot")
	require.Contains(t, err.Error(), "legacy CDC tasks must be exported and removed")
	require.Contains(t, err.Error(), "legacy-task")
	require.NoError(t, c.Stop())

	// The crossing artifacts are untouched: the source version still boots, no
	// route marker or step exists, the task row the preflight named is still
	// present for the operator to remediate, and with maintenance suppressed
	// the staged undecorated COLUMNS definition persists — the abort leaves
	// the predecessor content exactly as staged.
	setRecoveryCNHandles(c, columns46ShapeChain(), true)
	ctx, err = start()
	require.NoError(t, err)
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	requireNoRouteAdvance(t, ctx, cn)
	require.Equal(t, []string{"legacy-task"}, readCDCTaskNames(t, ctx, cn),
		"the aborted boot must leave the named legacy task row for the operator")
	requireAccountColumnsDefinitionsPreChange(t, ctx, cn, 0)
	// Operator remedy: remove the legacy task on the source version.
	execInternalStatement(t, ctx, cn, "delete from mo_catalog.mo_cdc_task")
	require.NoError(t, c.Stop())

	setRecoveryCNHandles(c, append(columns46ShapeChain(), v4_0_7.Handler), true)
	ctx, err = start()
	require.NoError(t, err)
	cn, err = c.GetCNService(0)
	require.NoError(t, err)
	requireColumnsCrossingComplete(t, ctx, cn, 1)
	requireSysColumnsDecorated(t, ctx, cn)
	require.NoError(t, c.Stop())
}

func requireSysColumnsDecorated(t *testing.T, ctx context.Context, cn ServiceOperator) {
	t.Helper()
	db := recoverySQLClient(t, cn)
	defer db.Close()
	requireColumnsDecorated(t, ctx, db, map[string]string{
		"plain": "decimal(10,2)",
		"u":     "decimal(10,2) unsigned",
		"zf":    "decimal(10,2) unsigned zerofill",
	})
}

// TestColumnsUpgradeEmptyCDCControlCompletesCrossing is the empty-seed
// control: the same 4.2-shaped catalog without any mo_cdc_task row drives
// the full 4.0.6 -> 4.0.7 crossing to READY 4.0.7.
func TestColumnsUpgradeEmptyCDCControlCompletesCrossing(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c, start := stageReady406Predecessor(t, 10*time.Minute)
	var ctx context.Context
	var err error

	setRecoveryCNHandles(c, append(columns46ShapeChain(), v4_0_7.Handler), true)
	ctx, err = start()
	require.NoError(t, err)
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	requireColumnsCrossingComplete(t, ctx, cn, 1)
	requireSysColumnsDecorated(t, ctx, cn)
	require.NoError(t, c.Stop())
}
