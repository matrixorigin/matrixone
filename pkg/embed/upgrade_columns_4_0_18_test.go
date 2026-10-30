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
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_18"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_4"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_5"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_6"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

const columnsUpgradeTenant = "columns_upgrade_tenant"

// preChangeCurrentColumnsDDL reproduces the undecorated current template a
// pre-change binary persists as information_schema.COLUMNS: the V109 lineage
// with the MEDIUMINT DATA_TYPE arm. The sysview package pins this exact
// construction byte-identical to its frozen T0 pre-change template identity
// (pkg/util/sysview/view_columns_history_test.go), and the #31461-era second
// variant differs from it only in the binary-family charset selectors.
func preChangeCurrentColumnsDDL() string {
	const v109DataType = "upper(mo_show_visible_bin(mc.atttyp,2)) = 'BOOL' then 'TINYINT' "
	const v110DataType = v109DataType + "when upper(mo_show_visible_bin(mc.atttyp,3)) like 'MEDIUMINT%' then 'MEDIUMINT' "
	return strings.ReplaceAll(sysview.InformationSchemaColumnsV109DDL(), v109DataType, v110DataType)
}

// newColumnsUpgradeCluster starts a generation with the CN handle list
// replaced by handles: truncating the list at v4_0_7 makes bootstrap finalize
// the cluster READY at the frozen consolidated 4.0.7/41 terminal, exactly
// like a pre-change deployment.
func newColumnsUpgradeCluster(
	t *testing.T,
	handles []bootstrap.VersionHandle,
) Cluster {
	t.Helper()
	c, err := NewCluster(WithTesting(), WithTestAdmissionContext(t.Context()), WithCNCount(1), WithPreStart(func(svc ServiceOperator) {
		adjustBasicClusterService(svc)
		if svc.ServiceType() == metadata.ServiceType_CN {
			svc.Adjust(func(cfg *ServiceConfig) { cfg.CN.AutomaticUpgrade = true })
			// Suppress the background COLUMNS/TABLES repair walker (design
			// validation item (iv) requires maintenance-disabled proof): with
			// the walker off, the staged predecessor definition survives until
			// the crossing entry recreates it, so restored metadata is
			// attributable to the entry rather than to background maintenance.
			svc.(*operator).testingCNOptions = recoveryCNOptions(handles)
		}
	}))
	if c != nil {
		t.Cleanup(func() {
			require.NoError(t, c.Close())
			require.NoError(t, os.RemoveAll(c.(*cluster).options.dataPath))
		})
	}
	require.NoError(t, err)
	return c
}

// startColumnsUpgradePhase starts the SQL/recovery work budget only after
// cluster admission and startup finish. The runner-wide queue is bounded by
// t.Context instead, so another test process cannot consume this phase budget.
func startColumnsUpgradePhase(t *testing.T, c Cluster) (context.Context, context.CancelFunc) {
	t.Helper()
	require.NoError(t, c.Start())
	return context.WithTimeout(t.Context(), 5*time.Minute)
}

// columnsPredecessorChain is the consolidated chain up to the frozen terminal.
func columnsPredecessorChain() []bootstrap.VersionHandle {
	return []bootstrap.VersionHandle{v4_0_4.Handler, v4_0_5.Handler, v4_0_6.Handler, v4_0_7.Handler}
}

// Note on background maintenance: the periodic COLUMNS/TABLES repair walker
// was removed upstream (#31662); the versioned 4.0.7 entry and CREATE ACCOUNT
// initialization now own convergence. These scenario clusters therefore prove
// entry attribution with no suppression option at all: nothing besides the
// v4_0_18 crossing entry recreates a staged predecessor definition.

// stageReady407Predecessor runs generation one on the truncated chain, then
// installs the probe fixtures and the exact pre-change persisted COLUMNS
// definition in the sys tenant and in columns_upgrade_tenant. The returned
// stopped cluster holds the READY-4.0.7/41 predecessor content the v4_0_18
// entry must recreate.
func stageReady407Predecessor(t *testing.T) Cluster {
	t.Helper()
	c := newColumnsUpgradeCluster(t, columnsPredecessorChain())
	ctx, cancel := startColumnsUpgradePhase(t, c)
	defer cancel()
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	createColumnsFixtures(t, ctx, cn)
	downgradeColumnsDefinitions(t, ctx, cn)
	requireColumnsDefinitionsPreChange(t, ctx, cn)
	// Settle across several tenant-upgrade passes, then re-assert. The final
	// version of the staged chain is already completed at this point, so a
	// repair walker would have converged the definition to the code template
	// by now; the re-read both pins the predecessor state the next generation
	// starts from and verifies the scenario's maintenance suppression.
	time.Sleep(2 * time.Second)
	requireColumnsDefinitionsPreChange(t, ctx, cn)
	require.NoError(t, c.Stop())
	return c
}

// requireColumnsDefinitionsPreChange asserts through the internal executor
// that both the sys tenant and columns_upgrade_tenant still persist the exact
// pre-change COLUMNS definition. The walker is suppressed in these scenarios,
// so this readback is deterministic: it proves the staged predecessor state
// the crossing is expected to repair, and it fails if the suppression is
// misconfigured (the walker would have recreated the canonical definition).
func requireColumnsDefinitionsPreChange(t *testing.T, ctx context.Context, cn ServiceOperator) {
	t.Helper()
	db := recoverySQLClient(t, cn)
	defer db.Close()
	var tenantID uint32
	require.NoError(t, db.QueryRowContext(ctx,
		fmt.Sprintf("select account_id from mo_catalog.mo_account where account_name = '%s'", columnsUpgradeTenant)).
		Scan(&tenantID))
	requireAccountColumnsDefinitionsPreChange(t, ctx, cn, 0, tenantID)
}

// requireAccountColumnsDefinitionsPreChange asserts through the internal
// executor that each given account still persists the exact pre-change
// undecorated COLUMNS definition.
func requireAccountColumnsDefinitionsPreChange(t *testing.T, ctx context.Context, cn ServiceOperator, accountIDs ...uint32) {
	t.Helper()
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	for _, accountID := range accountIDs {
		err := exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			exists, definition, err := versions.CheckInformationSchemaViewDefinition(txn, accountID, "COLUMNS")
			if err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("account %d has no persisted information_schema.COLUMNS definition", accountID)
			}
			if !sysview.InformationSchemaViewDefinitionMatches(definition, preChangeCurrentColumnsDDL()) {
				return fmt.Errorf("account %d must persist the pre-change undecorated COLUMNS definition before the crossing", accountID)
			}
			return nil
		}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithAccountID(accountID).WithWaitCommittedLogApplied())
		require.NoError(t, err)
	}
}

// createColumnsFixtures installs the attributed-DECIMAL probe tables in the
// sys tenant and in columns_upgrade_tenant. Mask coverage: plain carries mask
// 0, u mask 1, and zf mask 3.
func createColumnsFixtures(t *testing.T, ctx context.Context, cn ServiceOperator) {
	t.Helper()
	func() {
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
			fmt.Sprintf("create account %s ADMIN_NAME 'root' IDENTIFIED BY '111'", columnsUpgradeTenant),
		} {
			_, err := db.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
	}()
	func() {
		db := columnsTenantSQLClient(t, cn)
		defer db.Close()
		for _, statement := range []string{
			"create database columns_upgrade",
			"create table columns_upgrade.zf (x decimal(10,2) unsigned zerofill)",
			"create table columns_upgrade.u (x decimal(10,2) unsigned)",
			"insert into columns_upgrade.u values (1.5)",
		} {
			_, err := db.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
	}()
}

// columnsTenantSQLClient connects as the tenant administrator root.
func columnsTenantSQLClient(t *testing.T, cn ServiceOperator) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("%s#root:111@tcp(127.0.0.1:%d)/",
		columnsUpgradeTenant, cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	return db
}

// downgradeColumnsDefinitions restores the exact pre-change persisted COLUMNS
// definition in the sys tenant and in columns_upgrade_tenant: the cluster
// then holds the predecessor content (READY 4.0.7/41, undecorated template)
// that a carried-forward catalog presents to the v4_0_18 entry.
func downgradeColumnsDefinitions(t *testing.T, ctx context.Context, cn ServiceOperator) {
	t.Helper()
	db := recoverySQLClient(t, cn)
	defer db.Close()
	var tenantID uint32
	require.NoError(t, db.QueryRowContext(ctx,
		fmt.Sprintf("select account_id from mo_catalog.mo_account where account_name = '%s'", columnsUpgradeTenant)).
		Scan(&tenantID))
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	for _, accountID := range []uint32{0, tenantID} {
		for _, statement := range []string{
			"DROP VIEW IF EXISTS information_schema.COLUMNS;",
			preChangeCurrentColumnsDDL(),
		} {
			res, err := exec.Exec(ctx, statement,
				executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithAccountID(accountID).WithWaitCommittedLogApplied())
			require.NoError(t, err)
			res.Close()
		}
	}
	t.Logf("downgraded information_schema.COLUMNS to the pre-change definition for accounts 0 and %d", tenantID)
}

// requireColumnsDecorated asserts the decorated COLUMN_TYPE rendering for the
// mask probes in database columns_upgrade of the connected tenant.
func requireColumnsDecorated(t *testing.T, ctx context.Context, db *sql.DB, tables map[string]string) {
	t.Helper()
	for table, want := range tables {
		var got string
		require.NoError(t, db.QueryRowContext(ctx,
			"select column_type from information_schema.columns where table_schema='columns_upgrade' and table_name=? and column_name='x'",
			table).Scan(&got))
		require.Equal(t, want, got, "table %s", table)
	}
}

// requireDecoratedColumns asserts the decorated rendering in both tenants.
func requireDecoratedColumns(t *testing.T, ctx context.Context, cn ServiceOperator) {
	t.Helper()
	func() {
		db := recoverySQLClient(t, cn)
		defer db.Close()
		requireColumnsDecorated(t, ctx, db, map[string]string{
			"plain": "decimal(10,2)",
			"u":     "decimal(10,2) unsigned",
			"zf":    "decimal(10,2) unsigned zerofill",
		})
	}()
	func() {
		db := columnsTenantSQLClient(t, cn)
		defer db.Close()
		requireColumnsDecorated(t, ctx, db, map[string]string{
			"u":  "decimal(10,2) unsigned",
			"zf": "decimal(10,2) unsigned zerofill",
		})
	}()
}

// requireColumnsEntryNoop pins the content-idempotent completion marker: the
// entry's CheckFunc accepts every account's definition, so a replay issues
// zero writes.
func requireColumnsEntryNoop(t *testing.T, ctx context.Context, cn ServiceOperator) {
	t.Helper()
	db := recoverySQLClient(t, cn)
	defer db.Close()
	var tenantID uint32
	require.NoError(t, db.QueryRowContext(ctx,
		fmt.Sprintf("select account_id from mo_catalog.mo_account where account_name = '%s'", columnsUpgradeTenant)).
		Scan(&tenantID))
	entry := versions.InformationSchemaColumnsUpgradeEntry()
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	for _, accountID := range []uint32{0, tenantID} {
		err := exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			ok, err := entry.CheckFunc(txn, accountID)
			if err != nil {
				return err
			}
			require.True(t, ok, "account %d must hold the canonical decorated definition", accountID)
			return nil
		}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithAccountID(accountID).WithWaitCommittedLogApplied())
		require.NoError(t, err)
	}
}

// requireColumnsUpgradeComplete pins the completed forward route: the final
// version marker is Ready at 4.0.18/1, and the single 4.0.7 -> 4.0.18 step is
// Ready with converged tenant counters.
func requireColumnsUpgradeComplete(t *testing.T, ctx context.Context, cn ServiceOperator) []uint64 {
	t.Helper()
	final, steps := readRecoveryCatalog(t, ctx, cn, v4_0_18.Handler.Metadata())
	require.Equal(t, versions.StateReady, final.State)
	require.Len(t, steps, 1)
	require.Equal(t, "4.0.7", steps[0].FromVersion)
	require.Equal(t, "4.0.18", steps[0].ToVersion)
	require.Equal(t, versions.StateReady, steps[0].State)
	require.Equal(t, int32(2), steps[0].TotalTenant)
	require.Equal(t, steps[0].TotalTenant, steps[0].ReadyTenant)
	return []uint64{steps[0].ID}
}

// TestColumnsUpgradeAutomaticScenarioCompletion is validation item (iv)'s
// startup-owned completion leg on the integrated source: from the
// READY-4.0.7/41 predecessor with the undecorated persisted template, a
// binary carrying the v4_0_18 forward handle drives the route to Ready before
// SQL admission, recreates every account's COLUMNS with the decorated
// definition, and a restart replays the route as a no-op.
func TestColumnsUpgradeAutomaticScenarioCompletion(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c := stageReady407Predecessor(t)

	setRecoveryCNHandles(c, append(columnsPredecessorChain(), v4_0_18.Handler), true)
	ctx, cancel := startColumnsUpgradePhase(t, c)
	defer cancel()
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	stepIDs := requireColumnsUpgradeComplete(t, ctx, cn)
	requireDecoratedColumns(t, ctx, cn)
	requireColumnsEntryNoop(t, ctx, cn)
	require.NoError(t, c.Stop())

	// Restart: the published route and the decorated definitions stay; no new
	// route is created.
	cancel()
	ctx, cancel = startColumnsUpgradePhase(t, c)
	defer cancel()
	cn, err = c.GetCNService(0)
	require.NoError(t, err)
	final, steps := readRecoveryCatalog(t, ctx, cn, v4_0_18.Handler.Metadata())
	require.Equal(t, versions.StateReady, final.State)
	require.Len(t, steps, len(stepIDs))
	for i, step := range steps {
		require.Equal(t, stepIDs[i], step.ID, "published route must not be replaced")
	}
	requireDecoratedColumns(t, ctx, cn)
	require.NoError(t, c.Stop())
}

// TestColumnsUpgradeManualFailFastThenComplete is validation item (iv)'s
// fail-fast leg on the integrated source: with the automatic worker off, a
// v4_0_18 binary refuses to start against a catalog that is not READY at its
// final version, and the operator remedy — enabling the worker and
// restarting — drives the same completion.
func TestColumnsUpgradeManualFailFastThenComplete(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c := stageReady407Predecessor(t)

	setRecoveryCNHandles(c, append(columnsPredecessorChain(), v4_0_18.Handler), false)
	err := c.Start()
	require.Error(t, err, "manual mode must fail startup against a not-ready final catalog")
	require.Contains(t, err.Error(), "catalog version 4.0.7/41 is not ready for 4.0.18/1; enable auto-upgrade for the stopped upgrade and restart")

	err = c.Stop()
	require.NoError(t, err)

	setRecoveryCNHandles(c, append(columnsPredecessorChain(), v4_0_18.Handler), true)
	ctx, cancel := startColumnsUpgradePhase(t, c)
	defer cancel()
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	requireColumnsUpgradeComplete(t, ctx, cn)
	requireDecoratedColumns(t, ctx, cn)
	requireColumnsEntryNoop(t, ctx, cn)
	require.NoError(t, c.Stop())
}

// TestColumnsUpgradeWithoutEntryKeepsPredecessorDefinitions is the ablation
// control for the restoration claim: with the background walker suppressed,
// a generation that carries no v4_0_18 entry leaves every account's staged
// predecessor definition untouched — the decorated presentation the
// completion legs assert is absent — and only the generation that again
// carries the entry performs the repair. This pins the restored metadata to
// the v4_0_18 entry itself, for the sys tenant and columns_upgrade_tenant.
func TestColumnsUpgradeWithoutEntryKeepsPredecessorDefinitions(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c := stageReady407Predecessor(t)

	// Ablation generation: the frozen 4.0.7/41 chain with no v4_0_18 handle.
	setRecoveryCNHandles(c, columnsPredecessorChain(), true)
	ctx, cancel := startColumnsUpgradePhase(t, c)
	defer cancel()
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	final, steps := readRecoveryCatalog(t, ctx, cn, v4_0_7.Handler.Metadata())
	require.Equal(t, versions.StateReady, final.State)
	require.Len(t, steps, 0, "no new route may be created without the v4_0_18 handle")
	// Restoration assertion fails as designed: both tenants still present the
	// bare undecorated rendering, and the persisted definition is the staged
	// pre-change text, stable across maintenance passes.
	requireColumnsDefinitionsPreChange(t, ctx, cn)
	func() {
		db := recoverySQLClient(t, cn)
		defer db.Close()
		requireColumnsDecorated(t, ctx, db, map[string]string{
			"plain": "decimal(10,2)",
			"u":     "decimal(10,2)",
			"zf":    "decimal(10,2)",
		})
	}()
	func() {
		db := columnsTenantSQLClient(t, cn)
		defer db.Close()
		requireColumnsDecorated(t, ctx, db, map[string]string{
			"u":  "decimal(10,2)",
			"zf": "decimal(10,2)",
		})
	}()
	time.Sleep(2 * time.Second)
	requireColumnsDefinitionsPreChange(t, ctx, cn)
	require.NoError(t, c.Stop())

	// Attribution leg: the same data directory, the entry carried again. Only
	// now does the repair execute, proving the completion legs' decorated
	// state comes from the v4_0_18 entry and not from background machinery.
	setRecoveryCNHandles(c, append(columnsPredecessorChain(), v4_0_18.Handler), true)
	cancel()
	ctx, cancel = startColumnsUpgradePhase(t, c)
	defer cancel()
	cn, err = c.GetCNService(0)
	require.NoError(t, err)
	requireColumnsUpgradeComplete(t, ctx, cn)
	requireDecoratedColumns(t, ctx, cn)
	requireColumnsEntryNoop(t, ctx, cn)
	require.NoError(t, c.Stop())
}

// errCrashAfterReplacement is the injected tenant-upgrade crash: it fires
// after the real CREATE OR REPLACE executed inside the tenant upgrade
// transaction and before that transaction (view text plus tenant version
// marker) commits.
var errCrashAfterReplacement = errors.New("test crash after real replacement, before tenant-version commit")

// rollbackObservation records what a tenant's retry observes about the
// aborted attempt's aftermath, read inside the retry's fresh transaction,
// i.e. from committed state only. Tenant upgrade progress is the tenant's
// mo_account.create_version row: versions.UpgradeTenantVersion updates it in
// the same route transaction, after the handle returns (see
// service_upgrade_tenant.go's execute-upgrade-tenant loop), so an aborted
// attempt must leave it at the predecessor version.
type rollbackObservation struct {
	persistsPreChangeDefinition bool
	createVersionRead           bool
	tenantCreateVersion         string
}

// postReplacementCrashHandler wraps the real v4_0_18 tenant handle: the first
// attempt for each tenant runs the complete entry (the CREATE OR REPLACE
// executes) and then returns errCrashAfterReplacement so the framework rolls
// the tenant-version transaction back. The retry observes and records the
// committed aftermath — predecessor view text, no 4.0.18 version marker —
// before completing the same entry to success.
type postReplacementCrashHandler struct {
	bootstrap.VersionHandle
	mu       sync.Mutex
	crashed  map[int32]bool
	observed map[int32]rollbackObservation
}

func (h *postReplacementCrashHandler) HandleTenantUpgrade(ctx context.Context, tenantID int32, txn executor.TxnExecutor) error {
	h.mu.Lock()
	crashed := h.crashed[tenantID]
	h.mu.Unlock()
	if crashed {
		h.observeRollback(tenantID, txn)
		return h.VersionHandle.HandleTenantUpgrade(ctx, tenantID, txn)
	}
	if err := h.VersionHandle.HandleTenantUpgrade(ctx, tenantID, txn); err != nil {
		return err
	}
	h.mu.Lock()
	h.crashed[tenantID] = true
	h.mu.Unlock()
	return errCrashAfterReplacement
}

// observeRollback reads the committed state a retry sees after the aborted
// attempt, from inside the retry's fresh transaction: the persisted
// definition must be back at the predecessor text (the replacement rolled
// back) and the tenant's create_version must still be the predecessor 4.0.7
// (the post-handler progress update rolled back in the same transaction). It
// records only the first post-crash invocation per tenant: after the retry
// commits, a later pass legitimately observes the tenant's own advanced
// create_version, but that is no longer rollback evidence. It only records —
// assertions belong to the test goroutine after Start returns; require inside
// a bootstrap worker callback would not fail reliably.
func (h *postReplacementCrashHandler) observeRollback(tenantID int32, txn executor.TxnExecutor) {
	h.mu.Lock()
	if _, recorded := h.observed[tenantID]; recorded {
		h.mu.Unlock()
		return
	}
	h.mu.Unlock()
	obs := rollbackObservation{}
	if exists, definition, err := versions.CheckInformationSchemaViewDefinition(txn, uint32(tenantID), "COLUMNS"); err == nil && exists {
		obs.persistsPreChangeDefinition = sysview.InformationSchemaViewDefinitionMatches(definition, preChangeCurrentColumnsDDL())
	}
	res, err := txn.Exec(
		fmt.Sprintf("select create_version from mo_account where account_id = %d", uint32(tenantID)),
		executor.StatementOption{})
	if err == nil {
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			values := executor.GetStringRows(cols[0])
			if len(values) == 1 {
				obs.tenantCreateVersion = values[0]
				obs.createVersionRead = true
			}
			return true
		})
		res.Close()
	}
	h.mu.Lock()
	if _, recorded := h.observed[tenantID]; !recorded {
		h.observed[tenantID] = obs
	}
	h.mu.Unlock()
}

// rollbackEvidence returns the observations keyed by tenant id.
func (h *postReplacementCrashHandler) rollbackEvidence() map[int32]rollbackObservation {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[int32]rollbackObservation, len(h.observed))
	for id, obs := range h.observed {
		out[id] = obs
	}
	return out
}

// TestColumnsUpgradeInterruptedRouteResumes is validation item (iii)'s crash
// replay leg on the integrated source, against the real replacement: every
// tenant's first attempt executes the actual CREATE OR REPLACE and then
// crashes before the tenant-version transaction commits, so that transaction
// (view text and tenant progress together) rolls back. The retry reads the
// committed aftermath — predecessor definition, create_version still at the
// 4.0.7 predecessor — and replays the complete entry, advancing each tenant
// to Ready exactly once. A full process restart afterwards is a no-op replay.
func TestColumnsUpgradeInterruptedRouteResumes(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c := stageReady407Predecessor(t)

	crash := &postReplacementCrashHandler{
		VersionHandle: v4_0_18.Handler,
		crashed:       map[int32]bool{},
		observed:      map[int32]rollbackObservation{},
	}
	setRecoveryCNHandles(c, append(columnsPredecessorChain(), crash), true)
	ctx, cancel := startColumnsUpgradePhase(t, c)
	defer cancel()
	cn, err := c.GetCNService(0)
	require.NoError(t, err)

	// Both tenants ran the real replacement once, crashed, observed the
	// rollback, and completed on the retry. The recorded evidence is the
	// atomicity proof: aborting after the CREATE OR REPLACE reverts both the
	// view content and the tenant progress.
	evidence := crash.rollbackEvidence()
	require.Len(t, evidence, 2, "sys tenant and columns_upgrade_tenant must each crash and replay once")
	for id, obs := range evidence {
		require.True(t, obs.persistsPreChangeDefinition,
			"tenant %d: the aborted attempt must leave the predecessor COLUMNS definition committed", id)
		require.True(t, obs.createVersionRead,
			"tenant %d: the retry must be able to read the tenant's own account row", id)
		require.Equal(t, "4.0.7", obs.tenantCreateVersion,
			"tenant %d: the aborted attempt must leave create_version at the predecessor", id)
	}
	stepIDs := requireColumnsUpgradeComplete(t, ctx, cn)
	requireDecoratedColumns(t, ctx, cn)
	requireColumnsEntryNoop(t, ctx, cn)
	require.NoError(t, c.Stop())

	// Restart: the completed route and decorated definitions persist; replay
	// is a no-op and no new route is created.
	cancel()
	ctx, cancel = startColumnsUpgradePhase(t, c)
	defer cancel()
	cn, err = c.GetCNService(0)
	require.NoError(t, err)
	final, steps := readRecoveryCatalog(t, ctx, cn, v4_0_18.Handler.Metadata())
	require.Equal(t, versions.StateReady, final.State)
	require.Len(t, steps, len(stepIDs))
	for i, step := range steps {
		require.Equal(t, stepIDs[i], step.ID, "published route must not be replaced")
	}
	requireDecoratedColumns(t, ctx, cn)
	requireColumnsEntryNoop(t, ctx, cn)
	require.NoError(t, c.Stop())
}
