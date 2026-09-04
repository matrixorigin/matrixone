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

package upgrade

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_6"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestV406MaintenanceCleansHistoricalOrphanObjectPrivileges(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		sqlExecutor := cn.RawService().(cnservice.Service).GetSQLExecutor()

		const (
			databaseName = "orphan_privilege_upgrade_27836"
			roleName     = "orphan_privilege_upgrade_role_27836"
		)
		_, _ = conn.ExecContext(ctx, "drop database if exists "+databaseName)
		_, _ = conn.ExecContext(ctx, "drop role if exists "+roleName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "drop database if exists "+databaseName)
			_, _ = conn.ExecContext(cleanupCtx, "drop role if exists "+roleName)
		}()

		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn, "set role moadmin")
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn, "create role "+roleName)
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"grant create database on account * to "+roleName)
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn, "create database "+databaseName)
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"create table "+databaseName+".live_table(id int)")
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"create table "+databaseName+".drop_during_maintenance(id int)")
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"create index legacy_hidden_idx on "+databaseName+".live_table(id)")
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"create view "+databaseName+".live_view as select 1 as id")
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"create sequence "+databaseName+".live_sequence")
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"grant create table on database "+databaseName+" to "+roleName)
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"grant select on table "+databaseName+".* to "+roleName)
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"grant select on table "+databaseName+".live_table to "+roleName)
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"grant select on table "+databaseName+".drop_during_maintenance to "+roleName)
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"grant select on view "+databaseName+".live_view to "+roleName)
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"grant select on table "+databaseName+".live_sequence to "+roleName)

		databaseID := queryOrphanPrivilegeUpgradeID(t, ctx, conn,
			"select dat_id from mo_catalog.mo_database where datname = '"+databaseName+"'")
		tableID := queryOrphanPrivilegeUpgradeID(t, ctx, conn,
			"select rel_logical_id from mo_catalog.mo_tables where reldatabase = '"+databaseName+"' and relname = 'live_table'")
		dropTableID := queryOrphanPrivilegeUpgradeID(t, ctx, conn,
			"select rel_logical_id from mo_catalog.mo_tables where reldatabase = '"+databaseName+"' "+
				"and relname = 'drop_during_maintenance'")
		viewID := queryOrphanPrivilegeUpgradeID(t, ctx, conn,
			"select rel_logical_id from mo_catalog.mo_tables where reldatabase = '"+databaseName+"' and relname = 'live_view'")
		sequenceID := queryOrphanPrivilegeUpgradeID(t, ctx, conn,
			"select rel_logical_id from mo_catalog.mo_tables where reldatabase = '"+databaseName+"' and relname = 'live_sequence'")
		hiddenIndexID := queryOrphanPrivilegeUpgradeID(t, ctx, conn,
			"select rel_logical_id from mo_catalog.mo_tables where reldatabase = '"+databaseName+"' "+
				"and relname = (select distinct index_table_name from mo_catalog.mo_indexes where name = 'legacy_hidden_idx' "+
				"and table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = '"+databaseName+"' "+
				"and relname = 'live_table') limit 1)")

		var maxObjectID uint64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select max(object_id) from (select dat_id as object_id from mo_catalog.mo_database "+
				"union all select rel_logical_id from mo_catalog.mo_tables) objects",
		).Scan(&maxObjectID))
		orphanIDs := []uint64{
			maxObjectID + 1000001,
			maxObjectID + 1000002,
			maxObjectID + 1000003,
			maxObjectID + 1000004,
			maxObjectID + 1000005,
		}
		malformedControlID := maxObjectID + 1000006
		const bulkDatabaseOrphanCount = uint64(10036)
		bulkDatabaseOrphanStart := maxObjectID + 2000000

		copyRolePrivilegeRangeForUpgradeTest(
			t, ctx, conn, roleName, databaseID, bulkDatabaseOrphanStart,
			bulkDatabaseOrphanCount, "database", "d",
		)
		copyRolePrivilegeForUpgradeTest(
			t, ctx, conn, roleName, databaseID, orphanIDs[0], "database", "d", "d",
		)
		copyRolePrivilegeForUpgradeTest(
			t, ctx, conn, roleName, databaseID, orphanIDs[1], "table", "d.*", "d.*",
		)
		copyRolePrivilegeForUpgradeTest(
			t, ctx, conn, roleName, tableID, orphanIDs[2], "table", "d.t", "d.t",
		)
		copyRolePrivilegeForUpgradeTest(
			t, ctx, conn, roleName, viewID, orphanIDs[3], "view", "d.t", "d.t",
		)
		copyRolePrivilegeForUpgradeTest(
			t, ctx, conn, roleName, sequenceID, orphanIDs[4], "table", "d.t", "d.t",
		)
		copyRolePrivilegeForUpgradeTest(
			t, ctx, conn, roleName, tableID, malformedControlID, "table", "d.t", "legacy.unknown",
		)
		copyRolePrivilegeForUpgradeTest(
			t, ctx, conn, roleName, tableID, hiddenIndexID, "table", "d.t", "d.t",
		)

		require.Equal(t, 5, countRolePrivilegesByObjectIDs(
			t, ctx, conn, roleName, orphanIDs,
		))
		require.Equal(t, int(bulkDatabaseOrphanCount), countRolePrivilegesByObjectIDRange(
			t, ctx, conn, roleName, bulkDatabaseOrphanStart,
			bulkDatabaseOrphanStart+bulkDatabaseOrphanCount-1,
		))
		liveObjectIDs := []uint64{databaseID, tableID, viewID, sequenceID}
		liveGrantCount := countRolePrivilegesByObjectIDs(t, ctx, conn, roleName, liveObjectIDs)
		globalGrantCount := countRolePrivilegesByObjectIDs(t, ctx, conn, roleName, []uint64{0})
		require.Positive(t, liveGrantCount)
		require.Positive(t, globalGrantCount)
		require.Equal(t, 1, countRolePrivilegesByObjectIDs(
			t, ctx, conn, roleName, []uint64{malformedControlID},
		))

		var highWaterPhysicalKey string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select hex(__mo_cpkey_col) from mo_catalog.mo_role_privs "+
				"order by __mo_cpkey_col desc limit 1",
		).Scan(&highWaterPhysicalKey))
		highWaterPlan := queryOrphanPrivilegeExplainAnalyze(t, ctx, conn,
			"select role_id,obj_type,obj_id,privilege_id,privilege_level,hex(__mo_cpkey_col) "+
				"from mo_catalog.mo_role_privs order by __mo_cpkey_col desc limit 1")
		physicalPlan := func(limit int) string {
			return queryOrphanPrivilegeExplainAnalyze(t, ctx, conn, fmt.Sprintf(
				"select role_id,obj_type,obj_id,privilege_id,privilege_level,hex(__mo_cpkey_col) "+
					"from mo_catalog.mo_role_privs where __mo_cpkey_col <= unhex('%s') "+
					"order by __mo_cpkey_col limit %d", highWaterPhysicalKey, limit))
		}
		oneRowPlan := physicalPlan(1)
		pagePlan := physicalPlan(1000)
		oneRowScanInput := orphanPrivilegeTableScanInputRows(t, oneRowPlan)
		pageScanInput := orphanPrivilegeTableScanInputRows(t, pagePlan)
		oneRowScanBlocks := orphanPrivilegeTableScanBlocks(t, oneRowPlan)
		pageScanBlocks := orphanPrivilegeTableScanBlocks(t, pagePlan)
		require.Contains(t, highWaterPlan, "Index Reader Param:")
		require.Contains(t, highWaterPlan, "Limit: 1")
		require.LessOrEqual(t, orphanPrivilegeTableScanInputRows(t, highWaterPlan), oneRowScanInput,
			"descending high-water discovery must retain one-row physical pushdown; plan:\n%s", highWaterPlan)
		require.LessOrEqual(t, orphanPrivilegeTableScanBlocks(t, highWaterPlan), oneRowScanBlocks,
			"descending high-water discovery must retain bounded read-block behavior; plan:\n%s", highWaterPlan)
		require.Contains(t, oneRowPlan, "Index Reader Param:")
		require.Contains(t, oneRowPlan, "Limit: 1")
		require.Contains(t, pagePlan, "Index Reader Param:")
		require.Contains(t, pagePlan, "Limit: 1000")
		require.LessOrEqual(t, pageScanInput, oneRowScanInput+1000,
			"the 1,000-row physical limit may add at most one bounded reader page over the live-row baseline; plan:\n%s",
			pagePlan)
		require.Less(t, pageScanInput, int(bulkDatabaseOrphanCount),
			"the candidate scan must not read the complete 10,036-row object; plan:\n%s", pagePlan)
		require.LessOrEqual(t, pageScanBlocks, oneRowScanBlocks+1,
			"the 1,000-row physical limit must keep the read-block count near the one-row baseline; plan:\n%s",
			pagePlan)

		var cursorPhysicalKey string
		require.NoError(t, conn.QueryRowContext(ctx,
			fmt.Sprintf("select hex(__mo_cpkey_col) from mo_catalog.mo_role_privs "+
				"where obj_id = %d limit 1", bulkDatabaseOrphanStart+500),
		).Scan(&cursorPhysicalKey))
		cursorPlan := queryOrphanPrivilegeExplainAnalyze(t, ctx, conn, fmt.Sprintf(
			"select role_id,obj_type,obj_id,privilege_id,privilege_level,hex(__mo_cpkey_col) "+
				"from mo_catalog.mo_role_privs where __mo_cpkey_col > unhex('%s') "+
				"and __mo_cpkey_col <= unhex('%s') order by __mo_cpkey_col limit 1000",
			cursorPhysicalKey, highWaterPhysicalKey))
		require.Contains(t, cursorPlan, "Index Reader Param:")
		require.Contains(t, cursorPlan, "Limit: 1000")
		require.LessOrEqual(t, orphanPrivilegeTableScanInputRows(t, cursorPlan), oneRowScanInput+1000,
			"physical cursor bounds must retain ordered-limit pushdown; plan:\n%s", cursorPlan)
		require.LessOrEqual(t, orphanPrivilegeTableScanBlocks(t, cursorPlan), oneRowScanBlocks+1,
			"physical cursor bounds must retain bounded read-block behavior; plan:\n%s", cursorPlan)

		countOrphans := func() int {
			return countRolePrivilegesByObjectIDs(t, ctx, conn, roleName, orphanIDs) +
				countRolePrivilegesByObjectIDRange(
					t, ctx, conn, roleName, bulkDatabaseOrphanStart,
					bulkDatabaseOrphanStart+bulkDatabaseOrphanCount-1,
				)
		}
		var scan v4_0_6.OrphanPrivilegeScan
		runMaintenanceStep := func(rollback bool) (bool, error) {
			completed := false
			nextScan := scan
			err := sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
				txn.Use(catalog.MO_CATALOG)
				var err error
				nextScan, completed, err = v4_0_6.MaintainOrphanObjectPrivilegesPage(
					txn, catalog.System_Account, scan,
				)
				if err != nil {
					return err
				}
				if rollback {
					return errRollbackOrphanPrivilegeMaintenance
				}
				return nil
			}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied())
			if err == nil {
				scan = nextScan
			}
			return completed, err
		}

		initialOrphans := countOrphans()
		completed, err := runMaintenanceStep(true)
		require.False(t, completed)
		require.ErrorIs(t, err, errRollbackOrphanPrivilegeMaintenance)
		require.Equal(t, initialOrphans, countOrphans(),
			"a failed page transaction must not publish data or cursor progress")
		require.Zero(t, scan)

		previousOrphans := initialOrphans
		committedSteps := 0
		ordinaryDropDone := false
		for !completed && committedSteps < 30 {
			completed, err = runMaintenanceStep(false)
			require.NoError(t, err)
			committedSteps++
			remainingOrphans := countOrphans()
			removed := previousOrphans - remainingOrphans
			require.LessOrEqual(t, removed, int(1000),
				"one committed transaction must delete only from its bounded candidate page")
			if committedSteps == 2 {
				mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
					"drop table "+databaseName+".drop_during_maintenance")
				require.Zero(t, countRolePrivilegesByObjectIDs(
					t, ctx, conn, roleName, []uint64{dropTableID},
				), "ordinary DROP must remain safe while bounded maintenance is in progress")
				ordinaryDropDone = true
			}
			previousOrphans = remainingOrphans
		}
		require.True(t, completed)
		require.GreaterOrEqual(t, committedSteps, 2)
		require.Zero(t, countOrphans())
		require.Equal(t, liveGrantCount, countRolePrivilegesByObjectIDs(
			t, ctx, conn, roleName, liveObjectIDs,
		))
		require.Equal(t, globalGrantCount, countRolePrivilegesByObjectIDs(
			t, ctx, conn, roleName, []uint64{0},
		))
		require.Equal(t, 1, countRolePrivilegesByObjectIDs(
			t, ctx, conn, roleName, []uint64{malformedControlID},
		))
		require.True(t, ordinaryDropDone)
		require.Equal(t, 1, countRolePrivilegesByObjectIDs(
			t, ctx, conn, roleName, []uint64{hiddenIndexID},
		), "maintenance must preserve a legacy grant while its hidden relation exists")
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"drop index legacy_hidden_idx on "+databaseName+".live_table")
		require.Zero(t, countRolePrivilegesByObjectIDs(
			t, ctx, conn, roleName, []uint64{hiddenIndexID},
		), "ordinary DROP INDEX must clean the preserved hidden-child legacy grant")

		// A completed cleanup remains idempotent when the maintenance pass is
		// retried after a process restart or a later periodic tick.
		completed, err = runMaintenanceStep(false)
		require.NoError(t, err)
		require.True(t, completed)
		require.Zero(t, countOrphans())
	})
}

func TestV406MaintenanceIsTenantLocalAndCleansLateCreatedTenant(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer sysDB.Close()
		sysConn, err := sysDB.Conn(ctx)
		require.NoError(t, err)
		defer sysConn.Close()

		const (
			accountName  = "orphan_privilege_maintenance_27836"
			snapshotName = "orphan_privilege_maintenance_snapshot_27836"
		)
		_, _ = sysConn.ExecContext(ctx, "drop snapshot if exists "+snapshotName)
		_, _ = sysConn.ExecContext(ctx, "drop account if exists "+accountName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = sysConn.ExecContext(cleanupCtx, "drop snapshot if exists "+snapshotName)
			_, _ = sysConn.ExecContext(cleanupCtx, "drop account if exists "+accountName)
		}()

		sqlExecutor := cn.RawService().(cnservice.Service).GetSQLExecutor()
		var systemScan v4_0_6.OrphanPrivilegeScan
		clean, err := runOrphanPrivilegeMaintenancePage(
			ctx, sqlExecutor, catalog.System_Account, &systemScan)
		require.NoError(t, err)
		require.True(t, clean, "the initial account scan must finish before the late tenant is created")

		openTenant := func() (*sql.DB, *sql.Conn) {
			tenantDB, err := sql.Open("mysql", fmt.Sprintf(
				"%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, port,
			))
			require.NoError(t, err)
			tenantConn, err := tenantDB.Conn(ctx)
			require.NoError(t, err)
			return tenantDB, tenantConn
		}
		createTenant := func() (uint32, *sql.DB, *sql.Conn) {
			mustExecOrphanPrivilegeUpgradeSQL(t, ctx, sysConn,
				"create account "+accountName+" ADMIN_NAME 'root' IDENTIFIED BY '111'")
			accountID := uint32(queryOrphanPrivilegeUpgradeID(t, ctx, sysConn,
				"select account_id from mo_catalog.mo_account where account_name = '"+accountName+"'"))
			tenantDB, tenantConn := openTenant()
			return accountID, tenantDB, tenantConn
		}

		accountID, tenantDB, tenantConn := createTenant()

		// The catalog must remain restorable without a maintenance-only hidden
		// index table. This is the same-account restore shape exercised by BVT.
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, sysConn,
			"create snapshot "+snapshotName+" for account "+accountName)
		require.NoError(t, tenantConn.Close())
		require.NoError(t, tenantDB.Close())
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, sysConn,
			"restore account "+accountName+"{snapshot='"+snapshotName+"'}")
		tenantDB, tenantConn = openTenant()
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, sysConn, "drop snapshot "+snapshotName)

		const (
			orphanStart = uint64(8000000000)
			orphanCount = uint64(1001)
		)
		seedSQL := fmt.Sprintf(`insert into mo_catalog.mo_role_privs
			select p.role_id, p.role_name, 'database', %d + g.result,
				p.privilege_id, p.privilege_name, 'd', p.operation_user_id,
				p.granted_time, p.with_grant_option
			from (select * from mo_catalog.mo_role_privs limit 1) p
			cross join generate_series(0, %d) g`, orphanStart, orphanCount-1)
		seeded, err := execOrphanPrivilegeUpgradeInternalSQLForAccountAffected(
			ctx, sqlExecutor, accountID, seedSQL,
		)
		require.NoError(t, err)
		require.Equal(t, orphanCount, seeded)

		// A pass for another account must not see or delete this tenant's rows.
		_, err = runOrphanPrivilegeMaintenancePage(
			ctx, sqlExecutor, catalog.System_Account, &systemScan)
		require.NoError(t, err)
		require.Equal(t, int(orphanCount), countAllRolePrivilegesByObjectIDRange(
			t, ctx, tenantConn, orphanStart, orphanStart+orphanCount-1,
		))

		previous := int(orphanCount)
		var tenantScan v4_0_6.OrphanPrivilegeScan
		for pass := 0; pass < 10; pass++ {
			clean, err = runOrphanPrivilegeMaintenancePage(ctx, sqlExecutor, accountID, &tenantScan)
			require.NoError(t, err)
			remaining := countAllRolePrivilegesByObjectIDRange(
				t, ctx, tenantConn, orphanStart, orphanStart+orphanCount-1,
			)
			require.LessOrEqual(t, previous-remaining, 1000)
			previous = remaining
			if clean {
				break
			}
		}
		require.True(t, clean)
		require.Zero(t, previous)

		require.NoError(t, tenantConn.Close())
		require.NoError(t, tenantDB.Close())
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, sysConn, "drop account "+accountName)

		// Recreating a tenant after a complete scan models a restored/late tenant:
		// no persistent completion marker may prevent a later pass from repairing it.
		accountID, tenantDB, tenantConn = createTenant()
		defer tenantDB.Close()
		defer tenantConn.Close()
		const lateOrphanID = uint64(9000000000)
		lateSeedSQL := fmt.Sprintf(`insert into mo_catalog.mo_role_privs
			select role_id, role_name, 'database', %d, privilege_id, privilege_name,
				'd', operation_user_id, granted_time, with_grant_option
			from mo_catalog.mo_role_privs limit 1`, lateOrphanID)
		seeded, err = execOrphanPrivilegeUpgradeInternalSQLForAccountAffected(
			ctx, sqlExecutor, accountID, lateSeedSQL,
		)
		require.NoError(t, err)
		require.Equal(t, uint64(1), seeded)

		clean = false
		tenantScan = v4_0_6.OrphanPrivilegeScan{}
		for pass := 0; pass < 3 && !clean; pass++ {
			clean, err = runOrphanPrivilegeMaintenancePage(ctx, sqlExecutor, accountID, &tenantScan)
			require.NoError(t, err)
		}
		require.True(t, clean)
		require.Zero(t, countAllRolePrivilegesByObjectIDRange(
			t, ctx, tenantConn, lateOrphanID, lateOrphanID,
		))
	})
}

var errRollbackOrphanPrivilegeMaintenance = errors.New("rollback orphan privilege maintenance")

func runOrphanPrivilegeMaintenancePage(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	accountID uint32,
	scan *v4_0_6.OrphanPrivilegeScan,
) (bool, error) {
	clean := false
	nextScan := *scan
	err := sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		txn.Use(catalog.MO_CATALOG)
		var err error
		nextScan, clean, err = v4_0_6.MaintainOrphanObjectPrivilegesPage(txn, accountID, *scan)
		return err
	}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied())
	if err == nil {
		*scan = nextScan
	}
	return clean, err
}

func orphanPrivilegeTableScanInputRows(t *testing.T, plan string) int {
	t.Helper()
	matches := regexp.MustCompile(
		`(?s)Table Scan on mo_catalog\.mo_role_privs\s+Analyze:.*?inputRows=([0-9]+)`,
	).FindStringSubmatch(plan)
	require.Len(t, matches, 2, "missing mo_role_privs Table Scan metrics in plan:\n%s", plan)
	rows, err := strconv.Atoi(matches[1])
	require.NoError(t, err)
	return rows
}

func orphanPrivilegeTableScanBlocks(t *testing.T, plan string) int {
	t.Helper()
	matches := regexp.MustCompile(
		`(?s)Table Scan on mo_catalog\.mo_role_privs\s+Analyze:.*?inputBlocks=([0-9]+)`,
	).FindStringSubmatch(plan)
	require.Len(t, matches, 2, "missing mo_role_privs Table Scan block metrics in plan:\n%s", plan)
	blocks, err := strconv.Atoi(matches[1])
	require.NoError(t, err)
	return blocks
}

func queryOrphanPrivilegeExplainAnalyze(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	statement string,
) string {
	t.Helper()
	rows, err := conn.QueryContext(ctx, "explain analyze "+statement)
	require.NoError(t, err)
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(t, rows.Err())
	return strings.Join(lines, "\n")
}

func mustExecOrphanPrivilegeUpgradeSQL(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	statement string,
) {
	t.Helper()
	_, err := conn.ExecContext(ctx, statement)
	require.NoError(t, err, statement)
}

func queryOrphanPrivilegeUpgradeID(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	query string,
) uint64 {
	t.Helper()
	var id uint64
	require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&id), query)
	return id
}

func copyRolePrivilegeForUpgradeTest(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	roleName string,
	sourceObjectID uint64,
	targetObjectID uint64,
	objectType string,
	sourcePrivilegeLevel string,
	targetPrivilegeLevel string,
) {
	t.Helper()
	statement := fmt.Sprintf(
		"insert into mo_catalog.mo_role_privs "+
			"select role_id, role_name, obj_type, %d, privilege_id, privilege_name, %s, "+
			"operation_user_id, granted_time, with_grant_option "+
			"from mo_catalog.mo_role_privs where role_name = %s and obj_id = %d "+
			"and obj_type = %s and privilege_level = %s",
		targetObjectID,
		sqlquote.String(targetPrivilegeLevel),
		sqlquote.String(roleName),
		sourceObjectID,
		sqlquote.String(objectType),
		sqlquote.String(sourcePrivilegeLevel),
	)
	mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn, statement)
}

func copyRolePrivilegeRangeForUpgradeTest(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	roleName string,
	sourceObjectID uint64,
	targetObjectIDStart uint64,
	count uint64,
	objectType string,
	privilegeLevel string,
) {
	t.Helper()
	require.Positive(t, count)
	statement := fmt.Sprintf(
		"insert into mo_catalog.mo_role_privs "+
			"select role_id, role_name, obj_type, %d + result, privilege_id, privilege_name, privilege_level, "+
			"operation_user_id, granted_time, with_grant_option "+
			"from mo_catalog.mo_role_privs cross join generate_series(0, %d) g "+
			"where role_name = %s and obj_id = %d and obj_type = %s and privilege_level = %s",
		targetObjectIDStart,
		count-1,
		sqlquote.String(roleName),
		sourceObjectID,
		sqlquote.String(objectType),
		sqlquote.String(privilegeLevel),
	)
	mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn, statement)
}

func execOrphanPrivilegeUpgradeInternalSQLForAccountAffected(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	accountID uint32,
	statement string,
) (uint64, error) {
	var affectedRows uint64
	err := sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		txn.Use(catalog.MO_CATALOG)
		res, err := txn.Exec(
			statement,
			versions.UpgradeStatementOption(accountID),
		)
		if err != nil {
			return err
		}
		affectedRows = res.AffectedRows
		res.Close()
		return nil
	}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied())
	return affectedRows, err
}

func countAllRolePrivilegesByObjectIDRange(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	fromObjectID uint64,
	toObjectID uint64,
) int {
	t.Helper()
	query := fmt.Sprintf(
		"select count(*) from mo_catalog.mo_role_privs where obj_id between %d and %d",
		fromObjectID,
		toObjectID,
	)
	var count int
	require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&count), query)
	return count
}

func countRolePrivilegesByObjectIDRange(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	roleName string,
	fromObjectID uint64,
	toObjectID uint64,
) int {
	t.Helper()
	query := fmt.Sprintf(
		"select count(*) from mo_catalog.mo_role_privs "+
			"where role_name = %s and obj_id between %d and %d",
		sqlquote.String(roleName),
		fromObjectID,
		toObjectID,
	)
	var count int
	require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&count), query)
	return count
}

func countRolePrivilegesByObjectIDs(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	roleName string,
	objectIDs []uint64,
) int {
	t.Helper()
	idList := ""
	for i, id := range objectIDs {
		if i > 0 {
			idList += ","
		}
		idList += fmt.Sprintf("%d", id)
	}
	query := fmt.Sprintf(
		"select count(*) from mo_catalog.mo_role_privs where role_name = %s and obj_id in (%s)",
		sqlquote.String(roleName),
		idList,
	)
	var count int
	require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&count), query)
	return count
}
