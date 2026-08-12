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

func TestIssue26127CloneAndBranchEmbeddedBacktickTable(t *testing.T) {
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

		const (
			sourceDB         = "issue_26127_src"
			branchDB         = "issue_26127_branch"
			cloneDB          = "issue_26127_clone"
			sourceTableSQL   = "`src``table`"
			sourceViewSQL    = "`view``v`"
			viewOnlySourceDB = "issue_26127_view_src"
			viewOnlyBranchDB = "issue_26127_view_branch"
			viewOnlyCloneDB  = "issue_26127_view_clone"
			viewOnlyTableSQL = "`base`"
			viewOnlyViewSQL  = "`view``v`"
			roleName         = "issue_26127_view_role"
			userName         = "issue_26127_view_user"
			snapshotName     = "issue_26127_snapshot"
		)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop snapshot if exists "+snapshotName)
			execSQLMaybe(t, cleanupCtx, db, "drop user if exists "+userName)
			execSQLMaybe(t, cleanupCtx, db, "drop role if exists "+roleName)
			for _, name := range []string{viewOnlyBranchDB, viewOnlyCloneDB, viewOnlySourceDB, branchDB, cloneDB, sourceDB} {
				execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+name+"`")
			}
		}()

		execSQLRequire(t, ctx, db, "create database `"+sourceDB+"`")
		execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`."+sourceTableSQL+" (id int primary key, note varchar(32))")
		execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`."+sourceTableSQL+" values (1, 'source-row')")
		execSQLRequire(t, ctx, db, "create view `"+sourceDB+"`."+sourceViewSQL+" as select id, note from `"+sourceDB+"`."+sourceTableSQL)
		assertIssue26127Row(t, ctx, db, sourceDB, sourceTableSQL)
		assertIssue26127ViewCount(t, ctx, db, sourceDB, sourceViewSQL, 1)

		execSQLRequire(t, ctx, db, "create role "+roleName)
		execSQLRequire(t, ctx, db, "create user "+userName+" identified by '111' default role "+roleName)
		execSQLRequire(t, ctx, db, "grant "+roleName+" to "+userName)
		execSQLRequire(t, ctx, db, "grant connect on account * to "+roleName)
		execSQLRequire(t, ctx, db, "grant select on view `"+sourceDB+"`."+sourceViewSQL+" to "+roleName)

		t.Run("embedded view over ordinary table", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create database `"+viewOnlySourceDB+"`")
			execSQLRequire(t, ctx, db, "create table `"+viewOnlySourceDB+"`."+viewOnlyTableSQL+" (id int primary key, note varchar(32))")
			execSQLRequire(t, ctx, db, "insert into `"+viewOnlySourceDB+"`."+viewOnlyTableSQL+" values (1, 'source-row')")
			execSQLRequire(t, ctx, db, "create view `"+viewOnlySourceDB+"`."+viewOnlyViewSQL+" as select id, note from `"+viewOnlySourceDB+"`."+viewOnlyTableSQL)
			assertIssue26127ViewCount(t, ctx, db, viewOnlySourceDB, viewOnlyViewSQL, 1)

			execSQLRequire(t, ctx, db, "data branch create database `"+viewOnlyBranchDB+"` from `"+viewOnlySourceDB+"`")
			execSQLRequire(t, ctx, db, "create database `"+viewOnlyCloneDB+"` clone `"+viewOnlySourceDB+"`")
			assertIssue26127ViewCount(t, ctx, db, viewOnlyBranchDB, viewOnlyViewSQL, 1)
			assertIssue26127ViewCount(t, ctx, db, viewOnlyCloneDB, viewOnlyViewSQL, 1)
		})

		userDB, err := sql.Open("mysql", fmt.Sprintf(userName+":111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer userDB.Close()
		assertIssue26127ViewCount(t, ctx, userDB, sourceDB, sourceViewSQL, 1)
		execSQLRequire(t, ctx, db, "create snapshot "+snapshotName+" for database `"+sourceDB+"`")

		t.Run("ordinary table clone", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`.`ordinary_clone` clone `"+sourceDB+"`."+sourceTableSQL)
			assertIssue26127Row(t, ctx, db, sourceDB, "`ordinary_clone`")
		})
		t.Run("table branch", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "data branch create table `"+sourceDB+"`.`branch_clone` from `"+sourceDB+"`."+sourceTableSQL)
			assertIssue26127Row(t, ctx, db, sourceDB, "`branch_clone`")
		})
		t.Run("database branch", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "data branch create database `"+branchDB+"` from `"+sourceDB+"`")
			assertIssue26127Row(t, ctx, db, branchDB, sourceTableSQL)
			assertIssue26127ViewCount(t, ctx, db, branchDB, sourceViewSQL, 1)
		})
		t.Run("database clone", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create database `"+cloneDB+"` clone `"+sourceDB+"`")
			assertIssue26127Row(t, ctx, db, cloneDB, sourceTableSQL)
			assertIssue26127ViewCount(t, ctx, db, cloneDB, sourceViewSQL, 1)
		})
		t.Run("snapshot restore", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`."+sourceTableSQL+" values (2, 'after-snapshot')")
			assertIssue26127ViewCount(t, ctx, db, sourceDB, sourceViewSQL, 2)
			execSQLRequire(t, ctx, db, "restore database `"+sourceDB+"` {snapshot=\""+snapshotName+"\"}")
			assertIssue26127ViewCount(t, ctx, db, sourceDB, sourceViewSQL, 1)
		})
		t.Run("drop database", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "drop database `"+sourceDB+"`")
			var count int
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_database where datname = '"+sourceDB+"'").Scan(&count))
			require.Zero(t, count)
		})
	})
}

func assertIssue26127ViewCount(t *testing.T, ctx context.Context, db *sql.DB, databaseName, viewNameSQL string, expected int) {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		"select count(*) from `"+databaseName+"`."+viewNameSQL).Scan(&count))
	require.Equal(t, expected, count)
}

func assertIssue26127Row(t *testing.T, ctx context.Context, db *sql.DB, databaseName, tableNameSQL string) {
	t.Helper()
	var id int
	var note string
	require.NoError(t, db.QueryRowContext(ctx,
		"select id, note from `"+databaseName+"`."+tableNameSQL).Scan(&id, &note))
	require.Equal(t, 1, id)
	require.Equal(t, "source-row", note)
}
