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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

type issue32820BranchLineage struct {
	tableID  uint64
	parentID uint64
	cloneTS  int64
	creator  uint64
	level    string
}

func captureIssue32820BranchLineage(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	sourceDB, sourceTable, branchDB, branchTable, expectedLevel string,
) issue32820BranchLineage {
	t.Helper()
	lineage := issue32820BranchLineage{tableID: currentRelationID(t, ctx, db, branchDB, branchTable)}
	require.NoError(t, db.QueryRowContext(ctx,
		"select p_table_id, clone_ts, creator, level from mo_catalog.mo_branch_metadata where table_id=? and table_deleted=false",
		lineage.tableID,
	).Scan(&lineage.parentID, &lineage.cloneTS, &lineage.creator, &lineage.level))
	require.Equal(t, currentRelationID(t, ctx, db, sourceDB, sourceTable), lineage.parentID)
	require.Equal(t, expectedLevel, lineage.level)
	return lineage
}

func checkIssue32820RestoredLineage(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	sourceDB, sourceTable, branchDB, branchTable string,
	original issue32820BranchLineage,
) {
	t.Helper()
	var id int
	require.NoError(t, db.QueryRowContext(ctx,
		"select id from `"+branchDB+"`.`"+branchTable+"`").Scan(&id))
	require.Equal(t, 1, id)

	restoredID := currentRelationID(t, ctx, db, branchDB, branchTable)
	require.NotEqual(t, original.tableID, restoredID)
	var restoredParentID, restoredCreator uint64
	var restoredCloneTS int64
	var restoredLevel string
	require.NoError(t, db.QueryRowContext(ctx,
		"select p_table_id, clone_ts, creator, level from mo_catalog.mo_branch_metadata where table_id=? and table_deleted=false",
		restoredID,
	).Scan(&restoredParentID, &restoredCloneTS, &restoredCreator, &restoredLevel))
	require.Equal(t, original.parentID, restoredParentID)
	require.Equal(t, original.cloneTS, restoredCloneTS)
	require.Equal(t, original.creator, restoredCreator)
	require.Equal(t, original.level, restoredLevel)

	var protectedParentID uint64
	var protectedTS int64
	require.NoError(t, db.QueryRowContext(ctx,
		"select obj_id, ts from mo_catalog.mo_snapshots where kind='branch' and sname=?",
		fmt.Sprintf("__mo_branch_%d", restoredID),
	).Scan(&protectedParentID, &protectedTS))
	require.Equal(t, original.parentID, protectedParentID)
	require.Equal(t, original.cloneTS, protectedTS)

	execSQLRequire(t, ctx, db, "data branch diff `"+branchDB+"`.`"+branchTable+
		"` against `"+sourceDB+"`.`"+sourceTable+"`")
}

func checkIssue32820RestoredBranch(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	sourceDB, branchDB string,
	original issue32820BranchLineage,
) {
	t.Helper()
	t.Run("restored identity", func(t *testing.T) {
		var databaseType string
		require.NoError(t, db.QueryRowContext(ctx,
			"select dat_type from mo_catalog.mo_database where account_id=0 and datname=?", branchDB,
		).Scan(&databaseType))
		require.Equal(t, "data-branch", databaseType)
		checkIssue32820RestoredLineage(t, ctx, db, sourceDB, "t", branchDB, "t", original)
	})
	t.Run("delete restored branch", func(t *testing.T) {
		execSQLRequire(t, ctx, db, "data branch delete database `"+branchDB+"`")
	})
}

func TestIssue32820SnapshotRestoreRestitchesBranchLineage(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port,
		))
		require.NoError(t, err)
		defer db.Close()

		const (
			sourceDB = "issue_32820_snapshot_source"
			branchDB = "issue_32820_snapshot_branch"
			snapshot = "issue_32820_snapshot"
		)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+branchDB+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+sourceDB+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop snapshot if exists "+snapshot)
		}()

		execSQLRequire(t, ctx, db, "drop snapshot if exists "+snapshot)
		execSQLRequire(t, ctx, db, "drop database if exists `"+branchDB+"`")
		execSQLRequire(t, ctx, db, "drop database if exists `"+sourceDB+"`")
		execSQLRequire(t, ctx, db, "create database `"+sourceDB+"`")
		execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`.t(id int primary key)")
		execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`.t values(1)")
		execSQLRequire(t, ctx, db, "data branch create database `"+branchDB+"` from `"+sourceDB+"`")
		original := captureIssue32820BranchLineage(t, ctx, db, sourceDB, "t", branchDB, "t", "database")

		execSQLRequire(t, ctx, db, "create snapshot "+snapshot+" for database `"+branchDB+"`")
		execSQLRequire(t, ctx, db, "data branch delete database `"+branchDB+"`")
		execSQLRequire(t, ctx, db, "restore database `"+branchDB+"` {snapshot='"+snapshot+"'}")
		checkIssue32820RestoredBranch(t, ctx, db, sourceDB, branchDB, original)
	})
}

func TestIssue32820PITRRestoreRestitchesBranchLineage(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port,
		))
		require.NoError(t, err)
		defer db.Close()

		const (
			sourceDB = "issue_32820_pitr_source"
			branchDB = "issue_32820_pitr_branch"
			pitr     = "issue_32820_pitr"
		)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+branchDB+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+sourceDB+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop pitr if exists "+pitr)
		}()

		execSQLRequire(t, ctx, db, "drop pitr if exists "+pitr)
		execSQLRequire(t, ctx, db, "drop database if exists `"+branchDB+"`")
		execSQLRequire(t, ctx, db, "drop database if exists `"+sourceDB+"`")
		execSQLRequire(t, ctx, db, "create database `"+sourceDB+"`")
		execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`.t(id int primary key)")
		execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`.t values(1)")
		execSQLRequire(t, ctx, db, "data branch create database `"+branchDB+"` from `"+sourceDB+"`")
		original := captureIssue32820BranchLineage(t, ctx, db, sourceDB, "t", branchDB, "t", "database")

		execSQLRequire(t, ctx, db, "create pitr "+pitr+" for database `"+branchDB+"` range 1 'h'")
		var slept int
		require.NoError(t, db.QueryRowContext(ctx, "select sleep(1)").Scan(&slept))
		var restoreAt string
		require.NoError(t, db.QueryRowContext(ctx,
			"select date_format(current_timestamp(6), '%Y-%m-%d %H:%i:%s')").Scan(&restoreAt))
		execSQLRequire(t, ctx, db, "insert into `"+branchDB+"`.t values(2)")
		execSQLRequire(t, ctx, db,
			"restore database `"+branchDB+"` from pitr "+pitr+" '"+restoreAt+"'")
		checkIssue32820RestoredBranch(t, ctx, db, sourceDB, branchDB, original)
	})
}

func TestIssue32820TableSnapshotRestoreRestitchesBranchLineage(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port,
		))
		require.NoError(t, err)
		defer db.Close()

		const (
			database   = "issue_32820_table_source"
			branchTbl  = "branch_t"
			snapshot   = "issue_32820_table_snapshot"
			sourceTbl  = "t"
			levelTable = "table"
		)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+database+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop snapshot if exists "+snapshot)
		}()

		execSQLRequire(t, ctx, db, "drop snapshot if exists "+snapshot)
		execSQLRequire(t, ctx, db, "drop database if exists `"+database+"`")
		execSQLRequire(t, ctx, db, "create database `"+database+"`")
		execSQLRequire(t, ctx, db, "create table `"+database+"`.`"+sourceTbl+"`(id int primary key)")
		execSQLRequire(t, ctx, db, "insert into `"+database+"`.`"+sourceTbl+"` values(1)")
		execSQLRequire(t, ctx, db, "data branch create table `"+database+"`.`"+branchTbl+
			"` from `"+database+"`.`"+sourceTbl+"`")
		original := captureIssue32820BranchLineage(
			t, ctx, db, database, sourceTbl, database, branchTbl, levelTable,
		)

		execSQLRequire(t, ctx, db, "create snapshot "+snapshot+" for table `"+database+"` `"+branchTbl+"`")
		execSQLRequire(t, ctx, db, "data branch delete table `"+database+"`.`"+branchTbl+"`")
		execSQLRequire(t, ctx, db, "restore table `"+database+"`.`"+branchTbl+
			"` {snapshot='"+snapshot+"'}")
		checkIssue32820RestoredLineage(t, ctx, db, database, sourceTbl, database, branchTbl, original)
		execSQLRequire(t, ctx, db, "data branch delete table `"+database+"`.`"+branchTbl+"`")
	})
}

func requireIssue32820BranchProtection(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	childID, parentID uint64,
	cloneTS int64,
	wantPresent bool,
) {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_snapshots where kind='branch' and sname=?",
		fmt.Sprintf("__mo_branch_%d", childID),
	).Scan(&count))
	if !wantPresent {
		require.Zero(t, count)
		return
	}
	require.Equal(t, 1, count)
	var protectedParentID uint64
	var protectedTS int64
	require.NoError(t, db.QueryRowContext(ctx,
		"select obj_id, ts from mo_catalog.mo_snapshots where kind='branch' and sname=?",
		fmt.Sprintf("__mo_branch_%d", childID),
	).Scan(&protectedParentID, &protectedTS))
	require.Equal(t, parentID, protectedParentID)
	require.Equal(t, cloneTS, protectedTS)
}

func testIssue32820RestoreAncestorProtection(t *testing.T, usePITR bool) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port,
		))
		require.NoError(t, err)
		defer db.Close()

		mode := "snapshot"
		if usePITR {
			mode = "pitr"
		}
		sourceDB := "issue_32820_" + mode + "_chain_a"
		parentDB := "issue_32820_" + mode + "_chain_b"
		branchDB := "issue_32820_" + mode + "_chain_c"
		restoreObject := "issue_32820_" + mode + "_chain"
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+branchDB+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+parentDB+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+sourceDB+"`")
			if usePITR {
				execSQLMaybe(t, cleanupCtx, db, "drop pitr if exists "+restoreObject)
			} else {
				execSQLMaybe(t, cleanupCtx, db, "drop snapshot if exists "+restoreObject)
			}
		}()

		if usePITR {
			execSQLRequire(t, ctx, db, "drop pitr if exists "+restoreObject)
		} else {
			execSQLRequire(t, ctx, db, "drop snapshot if exists "+restoreObject)
		}
		for _, database := range []string{branchDB, parentDB, sourceDB} {
			execSQLRequire(t, ctx, db, "drop database if exists `"+database+"`")
		}
		execSQLRequire(t, ctx, db, "create database `"+sourceDB+"`")
		execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`.t(id int primary key, v int)")
		execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`.t values(1, 10)")
		execSQLRequire(t, ctx, db, "data branch create database `"+parentDB+"` from `"+sourceDB+"`")
		parentLineage := captureIssue32820BranchLineage(
			t, ctx, db, sourceDB, "t", parentDB, "t", "database",
		)
		// This post-fork root update needs the ancestor edge's historical LCA row.
		execSQLRequire(t, ctx, db, "update `"+sourceDB+"`.t set v=11 where id=1")
		execSQLRequire(t, ctx, db, "data branch create database `"+branchDB+"` from `"+parentDB+"`")
		branchLineage := captureIssue32820BranchLineage(
			t, ctx, db, parentDB, "t", branchDB, "t", "database",
		)
		requireIssue32820BranchProtection(
			t, ctx, db, parentLineage.tableID, parentLineage.parentID, parentLineage.cloneTS, true,
		)
		requireIssue32820BranchProtection(
			t, ctx, db, branchLineage.tableID, branchLineage.parentID, branchLineage.cloneTS, true,
		)

		var restoreAt string
		if usePITR {
			execSQLRequire(t, ctx, db, "create pitr "+restoreObject+" for database `"+branchDB+"` range 1 'h'")
			var slept int
			require.NoError(t, db.QueryRowContext(ctx, "select sleep(1)").Scan(&slept))
			require.NoError(t, db.QueryRowContext(ctx,
				"select date_format(current_timestamp(6), '%Y-%m-%d %H:%i:%s')").Scan(&restoreAt))
		} else {
			execSQLRequire(t, ctx, db, "create snapshot "+restoreObject+" for database `"+branchDB+"`")
		}

		execSQLRequire(t, ctx, db, "data branch delete database `"+parentDB+"`")
		requireIssue32820BranchProtection(
			t, ctx, db, parentLineage.tableID, parentLineage.parentID, parentLineage.cloneTS, true,
		)
		execSQLRequire(t, ctx, db, "data branch delete database `"+branchDB+"`")
		requireIssue32820BranchProtection(
			t, ctx, db, parentLineage.tableID, parentLineage.parentID, parentLineage.cloneTS, false,
		)
		requireIssue32820BranchProtection(
			t, ctx, db, branchLineage.tableID, branchLineage.parentID, branchLineage.cloneTS, false,
		)

		if usePITR {
			execSQLRequire(t, ctx, db,
				"restore database `"+branchDB+"` from pitr "+restoreObject+" '"+restoreAt+"'")
		} else {
			execSQLRequire(t, ctx, db,
				"restore database `"+branchDB+"` {snapshot='"+restoreObject+"'}")
		}
		restoredID := currentRelationID(t, ctx, db, branchDB, "t")
		require.NotEqual(t, branchLineage.tableID, restoredID)
		var restoredParentID, restoredCreator uint64
		var restoredCloneTS int64
		var restoredLevel string
		require.NoError(t, db.QueryRowContext(ctx,
			"select p_table_id, clone_ts, creator, level from mo_catalog.mo_branch_metadata where table_id=? and table_deleted=false",
			restoredID,
		).Scan(&restoredParentID, &restoredCloneTS, &restoredCreator, &restoredLevel))
		require.Equal(t, branchLineage.parentID, restoredParentID)
		require.Equal(t, branchLineage.cloneTS, restoredCloneTS)
		require.Equal(t, branchLineage.creator, restoredCreator)
		require.Equal(t, branchLineage.level, restoredLevel)
		requireIssue32820BranchProtection(
			t, ctx, db, restoredID, branchLineage.parentID, branchLineage.cloneTS, true,
		)
		requireIssue32820BranchProtection(
			t, ctx, db, parentLineage.tableID, parentLineage.parentID, parentLineage.cloneTS, true,
		)
		requireIssue32820BranchProtection(
			t, ctx, db, branchLineage.tableID, branchLineage.parentID, branchLineage.cloneTS, false,
		)
		var parentDeleted bool
		require.NoError(t, db.QueryRowContext(ctx,
			"select table_deleted from mo_catalog.mo_branch_metadata where table_id=?",
			parentLineage.tableID,
		).Scan(&parentDeleted))
		require.True(t, parentDeleted)

		rows, err := db.QueryContext(ctx,
			"data branch diff `"+branchDB+"`.t against `"+sourceDB+"`.t")
		require.NoError(t, err)
		defer rows.Close()
		var diffRows int
		for rows.Next() {
			var source, flag string
			var id, value int
			require.NoError(t, rows.Scan(&source, &flag, &id, &value))
			require.Equal(t, "UPDATE", flag)
			require.Equal(t, 1, id)
			require.Equal(t, 11, value)
			diffRows++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 1, diffRows)

		execSQLRequire(t, ctx, db, "data branch delete database `"+branchDB+"`")
		for _, childID := range []uint64{restoredID, parentLineage.tableID, branchLineage.tableID} {
			requireIssue32820BranchProtection(t, ctx, db, childID, 0, 0, false)
		}
	})
}

func TestIssue32820SnapshotRestoreRestoresAncestorProtection(t *testing.T) {
	testIssue32820RestoreAncestorProtection(t, false)
}

func TestIssue32820PITRRestoreRestoresAncestorProtection(t *testing.T) {
	testIssue32820RestoreAncestorProtection(t, true)
}

func captureIssue32820LineageForID(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	tableID, parentID uint64,
	expectedLevel string,
) issue32820BranchLineage {
	t.Helper()
	lineage := issue32820BranchLineage{tableID: tableID}
	require.NoError(t, db.QueryRowContext(ctx,
		"select p_table_id, clone_ts, creator, level from mo_catalog.mo_branch_metadata where table_id=? and table_deleted=false",
		tableID,
	).Scan(&lineage.parentID, &lineage.cloneTS, &lineage.creator, &lineage.level))
	require.Equal(t, parentID, lineage.parentID)
	require.Equal(t, expectedLevel, lineage.level)
	return lineage
}

func requireIssue32820Lineage(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	expected issue32820BranchLineage,
	wantDeleted bool,
) {
	t.Helper()
	var actual issue32820BranchLineage
	var deleted bool
	require.NoError(t, db.QueryRowContext(ctx,
		"select p_table_id, clone_ts, creator, level, table_deleted from mo_catalog.mo_branch_metadata where table_id=?",
		expected.tableID,
	).Scan(&actual.parentID, &actual.cloneTS, &actual.creator, &actual.level, &deleted))
	actual.tableID = expected.tableID
	require.Equal(t, expected, actual)
	require.Equal(t, wantDeleted, deleted)
}

func requireIssue32820LineageCount(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	tableID uint64,
	want int,
) {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_branch_metadata where table_id=?", tableID,
	).Scan(&count))
	require.Equal(t, want, count)
}

func requireIssue32820RootUpdateDiff(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	branchDB, sourceDB string,
) {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		"data branch diff `"+branchDB+"`.t against `"+sourceDB+"`.t columns (id, v)")
	require.NoError(t, err)
	defer rows.Close()
	var diffRows int
	for rows.Next() {
		var source, flag string
		var id, value int
		require.NoError(t, rows.Scan(&source, &flag, &id, &value))
		require.Equal(t, "UPDATE", flag)
		require.Equal(t, 1, id)
		require.Equal(t, 11, value)
		diffRows++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 1, diffRows)
}

func testIssue32820RestoreReconstructsAlterAncestor(t *testing.T, usePITR bool) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port,
		))
		require.NoError(t, err)
		defer db.Close()

		mode := "snapshot"
		if usePITR {
			mode = "pitr"
		}
		sourceDB := "issue_32820_" + mode + "_alter_a"
		parentDB := "issue_32820_" + mode + "_alter_b"
		branchDB := "issue_32820_" + mode + "_alter_c"
		restoreObject := "issue_32820_" + mode + "_alter"
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+branchDB+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+parentDB+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+sourceDB+"`")
			if usePITR {
				execSQLMaybe(t, cleanupCtx, db, "drop pitr if exists "+restoreObject)
			} else {
				execSQLMaybe(t, cleanupCtx, db, "drop snapshot if exists "+restoreObject)
			}
		}()

		if usePITR {
			execSQLRequire(t, ctx, db, "drop pitr if exists "+restoreObject)
		} else {
			execSQLRequire(t, ctx, db, "drop snapshot if exists "+restoreObject)
		}
		for _, database := range []string{branchDB, parentDB, sourceDB} {
			execSQLRequire(t, ctx, db, "drop database if exists `"+database+"`")
		}
		execSQLRequire(t, ctx, db, "create database `"+sourceDB+"`")
		execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`.t(id int primary key, v int)")
		execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`.t values(1, 10)")
		execSQLRequire(t, ctx, db, "data branch create database `"+parentDB+"` from `"+sourceDB+"`")
		parentLineage := captureIssue32820BranchLineage(
			t, ctx, db, sourceDB, "t", parentDB, "t", "database",
		)
		execSQLRequire(t, ctx, db, "update `"+sourceDB+"`.t set v=11 where id=1")

		execSQLRequire(t, ctx, db, "alter table `"+parentDB+"`.t add column note int")
		alteredID := currentRelationID(t, ctx, db, parentDB, "t")
		require.NotEqual(t, parentLineage.tableID, alteredID)
		alteredLineage := captureIssue32820LineageForID(
			t, ctx, db, alteredID, parentLineage.tableID, "alter:database",
		)
		requireIssue32820BranchProtection(
			t, ctx, db, alteredID, parentLineage.tableID, alteredLineage.cloneTS, true,
		)

		execSQLRequire(t, ctx, db, "data branch create database `"+branchDB+"` from `"+parentDB+"`")
		branchLineage := captureIssue32820BranchLineage(
			t, ctx, db, parentDB, "t", branchDB, "t", "database",
		)
		require.Equal(t, alteredID, branchLineage.parentID)

		var restoreAt string
		if usePITR {
			execSQLRequire(t, ctx, db, "create pitr "+restoreObject+" for database `"+branchDB+"` range 1 'h'")
			var slept int
			require.NoError(t, db.QueryRowContext(ctx, "select sleep(1)").Scan(&slept))
			require.NoError(t, db.QueryRowContext(ctx,
				"select date_format(current_timestamp(6), '%Y-%m-%d %H:%i:%s')").Scan(&restoreAt))
		} else {
			execSQLRequire(t, ctx, db, "create snapshot "+restoreObject+" for database `"+branchDB+"`")
		}

		execSQLRequire(t, ctx, db, "data branch delete database `"+parentDB+"`")
		execSQLRequire(t, ctx, db, "data branch delete database `"+branchDB+"`")
		requireIssue32820BranchProtection(
			t, ctx, db, parentLineage.tableID, parentLineage.parentID, parentLineage.cloneTS, false,
		)
		requireIssue32820LineageCount(t, ctx, db, alteredID, 0)
		requireIssue32820BranchProtection(t, ctx, db, alteredID, 0, 0, false)

		if usePITR {
			execSQLRequire(t, ctx, db,
				"restore database `"+branchDB+"` from pitr "+restoreObject+" '"+restoreAt+"'")
		} else {
			execSQLRequire(t, ctx, db,
				"restore database `"+branchDB+"` {snapshot='"+restoreObject+"'}")
		}
		restoredID := currentRelationID(t, ctx, db, branchDB, "t")
		require.NotEqual(t, branchLineage.tableID, restoredID)
		restoredLineage := branchLineage
		restoredLineage.tableID = restoredID
		requireIssue32820Lineage(t, ctx, db, restoredLineage, false)
		requireIssue32820Lineage(t, ctx, db, alteredLineage, true)
		requireIssue32820Lineage(t, ctx, db, parentLineage, true)
		requireIssue32820BranchProtection(
			t, ctx, db, restoredID, alteredID, branchLineage.cloneTS, true,
		)
		requireIssue32820BranchProtection(
			t, ctx, db, parentLineage.tableID, parentLineage.parentID, parentLineage.cloneTS, true,
		)
		requireIssue32820BranchProtection(t, ctx, db, alteredID, 0, 0, false)
		requireIssue32820RootUpdateDiff(t, ctx, db, branchDB, sourceDB)

		execSQLRequire(t, ctx, db, "data branch delete database `"+branchDB+"`")
		for _, childID := range []uint64{restoredID, parentLineage.tableID, alteredID, branchLineage.tableID} {
			requireIssue32820BranchProtection(t, ctx, db, childID, 0, 0, false)
		}
		requireIssue32820LineageCount(t, ctx, db, alteredID, 0)
	})
}

func TestIssue32820SnapshotRestoreReconstructsAlterAncestorLineage(t *testing.T) {
	testIssue32820RestoreReconstructsAlterAncestor(t, false)
}

func TestIssue32820PITRRestoreReconstructsAlterAncestorLineage(t *testing.T) {
	testIssue32820RestoreReconstructsAlterAncestor(t, true)
}

func requireIssue32820CatalogLockWaiter(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	tableID uint64,
	message string,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		var count int
		if err := db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_locks where table_id=? and lock_wait<>''",
			fmt.Sprint(tableID),
		).Scan(&count); err != nil {
			return false
		}
		return count > 0
	}, 30*time.Second, 20*time.Millisecond, message)
}

func TestIssue32820RestoreSerializesSiblingProtectionReclaim(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port,
		))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(8)

		const (
			sourceDB  = "issue_32820_race_a"
			parentDB  = "issue_32820_race_b"
			branchDB  = "issue_32820_race_c"
			siblingDB = "issue_32820_race_d"
			snapshot  = "issue_32820_race"
		)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			for _, database := range []string{branchDB, siblingDB, parentDB, sourceDB} {
				execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+database+"`")
			}
			execSQLMaybe(t, cleanupCtx, db, "drop snapshot if exists "+snapshot)
		}()

		execSQLRequire(t, ctx, db, "drop snapshot if exists "+snapshot)
		for _, database := range []string{branchDB, siblingDB, parentDB, sourceDB} {
			execSQLRequire(t, ctx, db, "drop database if exists `"+database+"`")
		}
		execSQLRequire(t, ctx, db, "create database `"+sourceDB+"`")
		execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`.t(id int primary key, v int)")
		execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`.t values(1, 10)")
		execSQLRequire(t, ctx, db, "data branch create database `"+parentDB+"` from `"+sourceDB+"`")
		parentLineage := captureIssue32820BranchLineage(
			t, ctx, db, sourceDB, "t", parentDB, "t", "database",
		)
		execSQLRequire(t, ctx, db, "update `"+sourceDB+"`.t set v=11 where id=1")
		execSQLRequire(t, ctx, db, "data branch create database `"+branchDB+"` from `"+parentDB+"`")
		branchLineage := captureIssue32820BranchLineage(
			t, ctx, db, parentDB, "t", branchDB, "t", "database",
		)
		execSQLRequire(t, ctx, db, "data branch create database `"+siblingDB+"` from `"+parentDB+"`")
		siblingLineage := captureIssue32820BranchLineage(
			t, ctx, db, parentDB, "t", siblingDB, "t", "database",
		)
		execSQLRequire(t, ctx, db, "create snapshot "+snapshot+" for database `"+branchDB+"`")
		execSQLRequire(t, ctx, db, "data branch delete database `"+parentDB+"`")
		execSQLRequire(t, ctx, db, "data branch delete database `"+branchDB+"`")
		requireIssue32820BranchProtection(
			t, ctx, db, parentLineage.tableID, parentLineage.parentID, parentLineage.cloneTS, true,
		)

		holder, err := db.Conn(ctx)
		require.NoError(t, err)
		defer holder.Close()
		deleter, err := db.Conn(ctx)
		require.NoError(t, err)
		defer deleter.Close()
		restorer, err := db.Conn(ctx)
		require.NoError(t, err)
		defer restorer.Close()

		_, err = holder.ExecContext(ctx, "begin")
		require.NoError(t, err)
		var lockedParentID uint64
		require.NoError(t, holder.QueryRowContext(ctx,
			"select obj_id from mo_catalog.mo_snapshots where kind='branch' and sname=? for update",
			fmt.Sprintf("__mo_branch_%d", parentLineage.tableID),
		).Scan(&lockedParentID))
		require.Equal(t, parentLineage.parentID, lockedParentID)
		holderOpen := true
		defer func() {
			if holderOpen {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				_, _ = holder.ExecContext(cleanupCtx, "rollback")
			}
		}()

		raceCtx, cancelRace := context.WithCancel(ctx)
		defer cancelRace()
		deleteDone := make(chan error, 1)
		restoreDone := make(chan error, 1)
		workersPending := false
		defer func() {
			if !workersPending {
				return
			}
			if holderOpen {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, _ = holder.ExecContext(cleanupCtx, "rollback")
				cleanupCancel()
				holderOpen = false
			}
			cancelRace()
			for _, done := range []chan error{deleteDone, restoreDone} {
				select {
				case <-done:
				case <-time.After(30 * time.Second):
					t.Error("racing statement did not stop during cleanup")
				}
			}
		}()

		workersPending = true
		go func() {
			_, deleteErr := deleter.ExecContext(raceCtx,
				"data branch delete database `"+siblingDB+"`")
			deleteDone <- deleteErr
		}()
		snapshotsID := currentRelationID(t, ctx, db, "mo_catalog", "mo_snapshots")
		requireIssue32820CatalogLockWaiter(t, ctx, db, snapshotsID,
			"sibling deletion must hold the lineage DAG lock while waiting to reclaim protection")
		select {
		case deleteErr := <-deleteDone:
			t.Fatalf("sibling deletion returned before protection lock release: %v", deleteErr)
		default:
		}

		go func() {
			_, restoreErr := restorer.ExecContext(raceCtx,
				"restore database `"+branchDB+"` {snapshot='"+snapshot+"'}")
			restoreDone <- restoreErr
		}()
		metadataID := currentRelationID(t, ctx, db, "mo_catalog", "mo_branch_metadata")
		requireIssue32820CatalogLockWaiter(t, ctx, db, metadataID,
			"restore must reach the lineage DAG coordination point")
		select {
		case restoreErr := <-restoreDone:
			t.Fatalf("restore returned before sibling reclamation completed: %v", restoreErr)
		default:
		}

		_, err = holder.ExecContext(ctx, "commit")
		require.NoError(t, err)
		holderOpen = false
		for _, result := range []struct {
			name string
			done chan error
		}{
			{name: "sibling deletion", done: deleteDone},
			{name: "descendant restore", done: restoreDone},
		} {
			select {
			case resultErr := <-result.done:
				require.NoError(t, resultErr, result.name)
			case <-time.After(30 * time.Second):
				t.Fatalf("%s did not finish after protection lock release", result.name)
			}
		}
		workersPending = false

		restoredID := currentRelationID(t, ctx, db, branchDB, "t")
		require.NotEqual(t, branchLineage.tableID, restoredID)
		restoredLineage := branchLineage
		restoredLineage.tableID = restoredID
		requireIssue32820Lineage(t, ctx, db, restoredLineage, false)
		requireIssue32820Lineage(t, ctx, db, siblingLineage, true)
		requireIssue32820BranchProtection(
			t, ctx, db, parentLineage.tableID, parentLineage.parentID, parentLineage.cloneTS, true,
		)
		requireIssue32820BranchProtection(t, ctx, db, siblingLineage.tableID, 0, 0, false)
		requireIssue32820RootUpdateDiff(t, ctx, db, branchDB, sourceDB)

		execSQLRequire(t, ctx, db, "data branch delete database `"+branchDB+"`")
		for _, childID := range []uint64{restoredID, parentLineage.tableID, branchLineage.tableID, siblingLineage.tableID} {
			requireIssue32820BranchProtection(t, ctx, db, childID, 0, 0, false)
		}
	})
}
