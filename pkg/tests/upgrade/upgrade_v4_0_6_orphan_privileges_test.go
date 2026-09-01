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

func TestV406UpgradeCleansHistoricalOrphanObjectPrivileges(t *testing.T) {
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
		require.NoError(t, execOrphanPrivilegeUpgradeInternalSQL(
			ctx, sqlExecutor, "drop index idx_mo_role_privs_obj_id on mo_catalog.mo_role_privs",
		))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			if err := ensureMoRolePrivsObjectIDIndex(cleanupCtx, sqlExecutor); err != nil {
				t.Errorf("restore mo_role_privs object ID index: %v", err)
			}
		}()
		require.Zero(t, countMoRolePrivsObjectIDIndexes(t, ctx, conn))
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn, "create role "+roleName)
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"grant create database on account * to "+roleName)
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn, "create database "+databaseName)
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"create table "+databaseName+".live_table(id int)")
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
			"grant select on view "+databaseName+".live_view to "+roleName)
		mustExecOrphanPrivilegeUpgradeSQL(t, ctx, conn,
			"grant select on table "+databaseName+".live_sequence to "+roleName)

		databaseID := queryOrphanPrivilegeUpgradeID(t, ctx, conn,
			"select dat_id from mo_catalog.mo_database where datname = '"+databaseName+"'")
		tableID := queryOrphanPrivilegeUpgradeID(t, ctx, conn,
			"select rel_logical_id from mo_catalog.mo_tables where reldatabase = '"+databaseName+"' and relname = 'live_table'")
		viewID := queryOrphanPrivilegeUpgradeID(t, ctx, conn,
			"select rel_logical_id from mo_catalog.mo_tables where reldatabase = '"+databaseName+"' and relname = 'live_view'")
		sequenceID := queryOrphanPrivilegeUpgradeID(t, ctx, conn,
			"select rel_logical_id from mo_catalog.mo_tables where reldatabase = '"+databaseName+"' and relname = 'live_sequence'")

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
		const bulkDatabaseOrphanCount = uint64(1001)
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

		countOrphans := func() int {
			return countRolePrivilegesByObjectIDs(t, ctx, conn, roleName, orphanIDs) +
				countRolePrivilegesByObjectIDRange(
					t, ctx, conn, roleName, bulkDatabaseOrphanStart,
					bulkDatabaseOrphanStart+bulkDatabaseOrphanCount-1,
				)
		}
		runUpgradeStep := func(rollback bool) (bool, error) {
			completed := false
			err := sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
				txn.Use(catalog.MO_CATALOG)
				var err error
				completed, err = v4_0_6.Handler.HandleTenantUpgradeStep(
					ctx, int32(catalog.System_Account), txn,
				)
				if err != nil {
					return err
				}
				if rollback {
					return errRollbackOrphanPrivilegeUpgrade
				}
				return nil
			}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied())
			return completed, err
		}

		initialOrphans := countOrphans()
		completed, err := runUpgradeStep(true)
		require.False(t, completed)
		require.ErrorIs(t, err, errRollbackOrphanPrivilegeUpgrade)
		require.Equal(t, initialOrphans, countOrphans(),
			"a failed page transaction must not publish partial cleanup")
		require.Zero(t, countMoRolePrivsObjectIDIndexes(t, ctx, conn),
			"a failed page transaction must also roll back the catalog index")

		previousOrphans := initialOrphans
		committedSteps := 0
		for {
			completed, err = runUpgradeStep(false)
			require.NoError(t, err)
			committedSteps++
			remainingOrphans := countOrphans()
			removed := previousOrphans - remainingOrphans
			require.LessOrEqual(t, removed, int(1000),
				"one committed transaction must contain at most one cleanup page")
			if completed {
				require.Zero(t, removed)
				break
			}
			require.Positive(t, removed)
			previousOrphans = remainingOrphans
		}
		require.Equal(t, 4, committedSteps)
		require.Equal(t, 1, countMoRolePrivsObjectIDIndexes(t, ctx, conn))
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

		// A completed cleanup remains idempotent when the upgrade generation is
		// retried after restart or offset reconciliation.
		completed, err = runUpgradeStep(false)
		require.NoError(t, err)
		require.True(t, completed)
		require.Zero(t, countOrphans())
		require.Equal(t, 1, countMoRolePrivsObjectIDIndexes(t, ctx, conn))
	})
}

var errRollbackOrphanPrivilegeUpgrade = errors.New("rollback orphan privilege upgrade")

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

func execOrphanPrivilegeUpgradeInternalSQL(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	statement string,
) error {
	return sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		txn.Use(catalog.MO_CATALOG)
		res, err := txn.Exec(
			statement,
			versions.UpgradeStatementOption(catalog.System_Account),
		)
		if err != nil {
			return err
		}
		res.Close()
		return nil
	}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied())
}

func ensureMoRolePrivsObjectIDIndex(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
) error {
	return sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		txn.Use(catalog.MO_CATALOG)
		exists, err := versions.CheckIndexDefinition(
			txn,
			catalog.System_Account,
			catalog.MO_CATALOG,
			"mo_role_privs",
			"idx_mo_role_privs_obj_id",
		)
		if err != nil || exists {
			return err
		}
		res, err := txn.Exec(
			"create index idx_mo_role_privs_obj_id on mo_catalog.mo_role_privs(obj_id)",
			versions.UpgradeStatementOption(catalog.System_Account),
		)
		if err != nil {
			return err
		}
		res.Close()
		return nil
	}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied())
}

func countMoRolePrivsObjectIDIndexes(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
) int {
	t.Helper()
	query := fmt.Sprintf(
		"select count(distinct idx.name) from mo_catalog.mo_indexes idx "+
			"join mo_catalog.mo_tables tbl on idx.table_id = tbl.rel_id "+
			"where tbl.account_id = %d and tbl.reldatabase = %s and tbl.relname = %s "+
			"and idx.name = %s",
		catalog.System_Account,
		sqlquote.String(catalog.MO_CATALOG),
		sqlquote.String("mo_role_privs"),
		sqlquote.String("idx_mo_role_privs_obj_id"),
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
