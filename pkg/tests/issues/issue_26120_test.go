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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIssue26120SnapshotBranchKeepsHistoricalParentIdentity(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		execSQLRequire(t, ctx, db, "set role moadmin")
		execSQLRequire(t, ctx, db, "select mo_feature_registry_upsert('branch', 'Branch feature', '{\"allowed_scope\":[]}', true)")
		execSQLRequire(t, ctx, db, "select mo_feature_registry_upsert('snapshot', 'Snapshot feature', '{\"allowed_scope\":[\"account\",\"database\",\"table\"]}', true)")

		const (
			tableDB       = "issue_26120_table"
			tableSnapshot = "issue_26120_table_sp"
			databaseSrc   = "issue_26120_db_src"
			databaseDst   = "issue_26120_db_dst"
			dbSnapshot    = "issue_26120_db_sp"
		)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+databaseDst+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+databaseSrc+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+tableDB+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop snapshot if exists "+dbSnapshot)
			execSQLMaybe(t, cleanupCtx, db, "drop snapshot if exists "+tableSnapshot)
		}()

		execSQLRequire(t, ctx, db, "create database `"+tableDB+"`")
		execSQLRequire(t, ctx, db, "create table `"+tableDB+"`.`parent_t` (id int primary key, val varchar(20))")
		execSQLRequire(t, ctx, db, "insert into `"+tableDB+"`.`parent_t` values (1, 'snapshot-row')")
		execSQLRequire(t, ctx, db, "create snapshot "+tableSnapshot+" for table `"+tableDB+"` `parent_t`")
		tableSnapshotParentID := relationIDAtSnapshot(t, ctx, db, tableDB, "parent_t", tableSnapshot)
		execSQLRequire(t, ctx, db, "drop table `"+tableDB+"`.`parent_t`")
		execSQLRequire(t, ctx, db, "create table `"+tableDB+"`.`parent_t` (id int primary key, val varchar(20))")
		execSQLRequire(t, ctx, db, "insert into `"+tableDB+"`.`parent_t` values (2, 'current-row')")
		require.NotEqual(t, tableSnapshotParentID, currentRelationID(t, ctx, db, tableDB, "parent_t"))
		execSQLRequire(t, ctx, db, "data branch create table `"+tableDB+"`.`child_t` from `"+tableDB+"`.`parent_t`{snapshot='"+tableSnapshot+"'}")
		assertIssue26120BranchMetadata(t, ctx, db, tableDB, "child_t", tableSnapshotParentID, 1)

		execSQLRequire(t, ctx, db, "create database `"+databaseSrc+"`")
		execSQLRequire(t, ctx, db, "create table `"+databaseSrc+"`.`parent_t` (id int primary key, val varchar(20))")
		execSQLRequire(t, ctx, db, "insert into `"+databaseSrc+"`.`parent_t` values (10, 'snapshot-row')")
		execSQLRequire(t, ctx, db, "create snapshot "+dbSnapshot+" for database `"+databaseSrc+"`")
		dbSnapshotParentID := relationIDAtSnapshot(t, ctx, db, databaseSrc, "parent_t", dbSnapshot)
		execSQLRequire(t, ctx, db, "drop table `"+databaseSrc+"`.`parent_t`")
		execSQLRequire(t, ctx, db, "create table `"+databaseSrc+"`.`parent_t` (id int primary key, val varchar(20))")
		execSQLRequire(t, ctx, db, "insert into `"+databaseSrc+"`.`parent_t` values (20, 'current-row')")
		require.NotEqual(t, dbSnapshotParentID, currentRelationID(t, ctx, db, databaseSrc, "parent_t"))
		execSQLRequire(t, ctx, db, "data branch create database `"+databaseDst+"` from `"+databaseSrc+"`{snapshot='"+dbSnapshot+"'}")
		assertIssue26120BranchMetadata(t, ctx, db, databaseDst, "parent_t", dbSnapshotParentID, 10)

		execSQLRequire(t, ctx, db, "data branch diff `"+tableDB+"`.`child_t` against `"+tableDB+"`.`parent_t`")
		execSQLRequire(t, ctx, db, "data branch diff `"+databaseDst+"`.`parent_t` against `"+databaseSrc+"`.`parent_t`")
	})
}

func relationIDAtSnapshot(t *testing.T, ctx context.Context, db *sql.DB, databaseName, tableName, snapshotName string) uint64 {
	t.Helper()
	var id uint64
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
		"select rel_id from mo_catalog.mo_tables {snapshot='%s'} where account_id = 0 and reldatabase = '%s' and relname = '%s'",
		snapshotName, databaseName, tableName)).Scan(&id))
	return id
}

func currentRelationID(t *testing.T, ctx context.Context, db *sql.DB, databaseName, tableName string) uint64 {
	t.Helper()
	var id uint64
	require.NoError(t, db.QueryRowContext(ctx,
		"select rel_id from mo_catalog.mo_tables where account_id = 0 and reldatabase = ? and relname = ?",
		databaseName, tableName).Scan(&id))
	return id
}

func assertIssue26120BranchMetadata(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	childDB string,
	childTable string,
	wantParentID uint64,
	wantID int,
) {
	t.Helper()

	childID := currentRelationID(t, ctx, db, childDB, childTable)

	var metadataParentID uint64
	require.NoError(t, db.QueryRowContext(ctx,
		"select p_table_id from mo_catalog.mo_branch_metadata where table_id = ?", childID).Scan(&metadataParentID))
	assert.Equal(t, wantParentID, metadataParentID)

	var snapshotParentID uint64
	require.NoError(t, db.QueryRowContext(ctx,
		"select obj_id from mo_catalog.mo_snapshots where kind = 'branch' and sname = ?",
		fmt.Sprintf("__mo_branch_%d", childID)).Scan(&snapshotParentID))
	assert.Equal(t, wantParentID, snapshotParentID)

	var id int
	var value string
	qualifiedChild := "`" + childDB + "`.`" + childTable + "`"
	require.NoError(t, db.QueryRowContext(ctx, "select id, val from "+qualifiedChild).Scan(&id, &value))
	require.Equal(t, wantID, id)
	require.Equal(t, "snapshot-row", value)
}
