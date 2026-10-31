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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_6"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
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
// replaced by handles: the source fixture uses a registry ending at the released 4.0.6/5 identity.
// This stages a catalog on the current engine; it is not an old-binary test.
func newColumnsUpgradeCluster(
	t *testing.T,
	setupCtx context.Context,
	handles []bootstrap.VersionHandle,
) Cluster {
	t.Helper()
	c, err := NewCluster(WithTesting(), WithTestAdmissionContext(setupCtx), WithCNCount(1), WithPreStart(func(svc ServiceOperator) {
		adjustBasicClusterService(svc)
		if svc.ServiceType() == metadata.ServiceType_CN {
			svc.Adjust(func(cfg *ServiceConfig) { cfg.CN.AutomaticUpgrade = true })
			// Versioned handles own catalog convergence; keep each staged chain explicit.
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

// Note on background maintenance: the periodic COLUMNS/TABLES repair walker
// was removed upstream (#31662); the versioned 4.0.7 entry and CREATE ACCOUNT
// initialization now own convergence. These scenario clusters therefore prove
// entry attribution with no suppression option at all: nothing besides the
// v4_0_7 crossing entry recreates a staged predecessor definition.

// stageReleasedMetadataPredecessor runs generation one on the truncated chain, then
// installs the probe fixtures and the exact pre-change persisted COLUMNS
// definition in the sys tenant and in columns_upgrade_tenant. The returned
// stopped cluster holds the READY-4.0.6/5 predecessor content the v4_0_7
// entry must recreate.
func stageReleasedMetadataPredecessor(t *testing.T, budget time.Duration) (Cluster, func() (context.Context, error)) {
	t.Helper()
	c := newColumnsUpgradeCluster(t, t.Context(), columns46ShapeChain())
	start, cleanup := newRecoveryClusterStarterWithBudget(t.Context(), c.Start, time.Now, budget)
	t.Cleanup(cleanup)
	ctx, err := start()
	require.NoError(t, err)
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	createColumnsFixtures(t, ctx, cn)
	downgradeColumnsDefinitions(t, ctx, cn)
	requireColumnsDefinitionsPreChange(t, ctx, cn)
	require.NoError(t, c.Stop())
	return c, start
}

// requireColumnsDefinitionsPreChange checks both tenant definitions before
// the versioned entry runs; there is no periodic repair walker.
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
			"create table columns_upgrade.prefixes (id int primary key, v varchar(20), key prefix_key(v(4)), key whole_key(v))",
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
			"create table columns_upgrade.prefixes (id int primary key, v varchar(20), key prefix_key(v(4)), key whole_key(v))",
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
// then holds the predecessor content (READY 4.0.6/5, undecorated template)
// that a carried-forward catalog presents to the v4_0_7 entry.
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
			"DROP VIEW IF EXISTS information_schema.STATISTICS;",
			sysview.InformationSchemaStatisticsDDL,
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
func requireIndexPrefixMetadata(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	for _, tc := range []struct {
		name   string
		valid  bool
		length int64
	}{
		{"prefix_key", true, 4}, {"whole_key", false, 0},
	} {
		var length sql.NullInt64
		require.NoError(t, db.QueryRowContext(ctx, "select sub_part from information_schema.statistics where table_schema='columns_upgrade' and table_name='prefixes' and index_name=?", tc.name).Scan(&length))
		require.Equal(t, tc.valid, length.Valid, tc.name)
		if tc.valid {
			require.Equal(t, tc.length, length.Int64)
		}
	}
}

func requireDecoratedColumns(t *testing.T, ctx context.Context, cn ServiceOperator) {
	t.Helper()
	func() {
		db := recoverySQLClient(t, cn)
		defer db.Close()
		requireIndexPrefixMetadata(t, ctx, db)
		requireColumnsDecorated(t, ctx, db, map[string]string{
			"plain": "decimal(10,2)",
			"u":     "decimal(10,2) unsigned",
			"zf":    "decimal(10,2) unsigned zerofill",
		})
	}()
	func() {
		db := columnsTenantSQLClient(t, cn)
		defer db.Close()
		requireIndexPrefixMetadata(t, ctx, db)
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
// version marker is Ready at the registered 4.0.7 offset, and the single 4.0.6 -> 4.0.7 step is
// Ready with converged tenant counters.
func requireColumnsUpgradeComplete(t *testing.T, ctx context.Context, cn ServiceOperator) []uint64 {
	t.Helper()
	final, steps := readRecoveryCatalog(t, ctx, cn, v4_0_7.Handler.Metadata())
	require.Equal(t, versions.StateReady, final.State)
	require.Len(t, steps, 1)
	require.Equal(t, "4.0.6", steps[0].FromVersion)
	require.Equal(t, "4.0.7", steps[0].ToVersion)
	require.Equal(t, versions.StateReady, steps[0].State)
	require.Equal(t, int32(2), steps[0].TotalTenant)
	require.Equal(t, steps[0].TotalTenant, steps[0].ReadyTenant)
	return []uint64{steps[0].ID}
}

// TestColumnsUpgradeAutomaticScenarioCompletion is validation item (iv)'s
// startup-owned completion leg on the integrated source: from the
// READY-4.0.6/5 predecessor with the undecorated persisted template, a
// binary carrying the v4_0_7 forward handle drives the route to Ready before
// SQL admission, recreates every account's COLUMNS with the decorated
// definition, and a restart replays the route as a no-op.
func TestColumnsUpgradeAutomaticScenarioCompletion(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c, start := stageReleasedMetadataPredecessor(t, 5*time.Minute)
	var ctx context.Context
	var err error

	setRecoveryCNHandles(c, append(columns46ShapeChain(), v4_0_7.Handler), true)
	ctx, err = start()
	require.NoError(t, err)
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	stepIDs := requireColumnsUpgradeComplete(t, ctx, cn)
	func() {
		db := recoverySQLClient(t, cn)
		defer db.Close()
		_, err := db.ExecContext(ctx, "create account metadata_fresh admin_name 'root' identified by '111'")
		require.NoError(t, err)
		fresh, err := sql.Open("mysql", fmt.Sprintf("metadata_fresh#root:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer fresh.Close()
		for _, statement := range []string{
			"create database columns_upgrade",
			"create table columns_upgrade.zf (x decimal(10,2) unsigned zerofill)",
			"create table columns_upgrade.plain (x decimal(10,2))",
			"create table columns_upgrade.prefixes (id int primary key, v varchar(20), key prefix_key(v(4)), key whole_key(v))",
		} {
			_, err := fresh.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		requireColumnsDecorated(t, ctx, fresh, map[string]string{"zf": "decimal(10,2) unsigned zerofill", "plain": "decimal(10,2)"})
		requireIndexPrefixMetadata(t, ctx, fresh)
		_, err = db.ExecContext(ctx, "drop account metadata_fresh")
		require.NoError(t, err)
	}()
	requireDecoratedColumns(t, ctx, cn)
	requireColumnsEntryNoop(t, ctx, cn)
	seedLegacyCDCTask(t, ctx, cn, "current-task", "paused")
	require.NoError(t, c.Stop())

	// Restart: the published route and the decorated definitions stay; no new
	// route is created.
	ctx, err = start()
	require.NoError(t, err)
	cn, err = c.GetCNService(0)
	require.NoError(t, err)
	final, steps := readRecoveryCatalog(t, ctx, cn, v4_0_7.Handler.Metadata())
	require.Equal(t, versions.StateReady, final.State)
	require.Len(t, steps, len(stepIDs))
	for i, step := range steps {
		require.Equal(t, stepIDs[i], step.ID, "published route must not be replaced")
	}
	requireDecoratedColumns(t, ctx, cn)
	require.Equal(t, []string{"current-task"}, readCDCTaskNames(t, ctx, cn))
	require.NoError(t, c.Stop())
}

// TestColumnsUpgradeManualFailFastThenComplete is validation item (iv)'s
// fail-fast leg on the integrated source: with the automatic worker off, a
// v4_0_7 binary refuses to start against a catalog that is not READY at its
// final version, and the operator remedy — enabling the worker and
// restarting — drives the same completion.
func TestColumnsUpgradeManualFailFastThenComplete(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c, start := stageReleasedMetadataPredecessor(t, 5*time.Minute)
	var ctx context.Context
	var err error

	setRecoveryCNHandles(c, append(columns46ShapeChain(), v4_0_7.Handler), false)
	_, err = start()
	require.Error(t, err, "manual mode must fail startup against a not-ready final catalog")
	target := v4_0_7.Handler.Metadata()
	require.Contains(t, err.Error(), fmt.Sprintf("catalog version 4.0.6/5 is not ready for %s/%d; enable auto-upgrade for the stopped upgrade and restart", target.Version, target.VersionOffset))

	err = c.Stop()
	require.NoError(t, err)

	setRecoveryCNHandles(c, append(columns46ShapeChain(), v4_0_7.Handler), true)
	ctx, err = start()
	require.NoError(t, err)
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	requireColumnsUpgradeComplete(t, ctx, cn)
	requireDecoratedColumns(t, ctx, cn)
	requireColumnsEntryNoop(t, ctx, cn)
	require.NoError(t, c.Stop())
}

// TestColumnsUpgradeWithoutEntryKeepsPredecessorDefinitions is the ablation
// control for the restoration claim: with the background walker suppressed,
// a generation that carries no v4_0_7 entry leaves every account's staged
// predecessor definition untouched — the decorated presentation the
// completion legs assert is absent — and only the generation that again
// carries the entry performs the repair. This pins the restored metadata to
// the v4_0_7 entry itself, for the sys tenant and columns_upgrade_tenant.
func TestColumnsUpgradeWithoutEntryKeepsPredecessorDefinitions(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c, start := stageReleasedMetadataPredecessor(t, 5*time.Minute)
	var ctx context.Context
	var err error

	// Ablation generation: the released-predecessor chain with no v4_0_7 handle.
	setRecoveryCNHandles(c, columns46ShapeChain(), true)
	ctx, err = start()
	require.NoError(t, err)
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	final, steps := readRecoveryCatalog(t, ctx, cn, v4_0_6.Handler.Metadata())
	require.Equal(t, versions.StateReady, final.State)
	require.Len(t, steps, 0, "no new route may be created without the v4_0_7 handle")
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
	requireColumnsDefinitionsPreChange(t, ctx, cn)
	require.NoError(t, c.Stop())

	// Attribution leg: the same data directory, the entry carried again. Only
	// now does the repair execute, proving the completion legs' decorated
	// state comes from the v4_0_7 entry and not from background machinery.
	setRecoveryCNHandles(c, append(columns46ShapeChain(), v4_0_7.Handler), true)
	ctx, err = start()
	require.NoError(t, err)
	cn, err = c.GetCNService(0)
	require.NoError(t, err)
	requireColumnsUpgradeComplete(t, ctx, cn)
	requireDecoratedColumns(t, ctx, cn)
	requireColumnsEntryNoop(t, ctx, cn)
	require.NoError(t, c.Stop())
}
