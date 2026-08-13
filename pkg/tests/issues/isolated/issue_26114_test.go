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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func runIssue26114ClusterTest(t *testing.T, fn func(embed.Cluster)) {
	t.Helper()
	cluster, err := embed.StartTestCluster(
		embed.WithCNCount(1),
		embed.WithPreStart(func(service embed.ServiceOperator) {
			if service.ServiceType() != metadata.ServiceType_CN {
				return
			}
			service.Adjust(func(config *embed.ServiceConfig) {
				config.CN.LockService.MaxFixedSliceSize = 10001
				config.CN.LockService.MaxLockRowCount = 10000
				config.CN.Frontend.SkipCheckUser = false
			})
		}),
	)
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	fn(cluster)
}

func execIssue26114SQLRequire(t *testing.T, ctx context.Context, db *sql.DB, statement string) {
	t.Helper()
	_, err := db.ExecContext(ctx, statement)
	require.NoErrorf(t, err, "exec failed: %s", statement)
}

func execIssue26114SQLMaybe(ctx context.Context, db *sql.DB, statement string) {
	_, _ = db.ExecContext(ctx, statement)
}

func TestIssue26114CrossAccountBranchUsesTargetQuotaAndOwnership(t *testing.T) {
	runIssue26114ClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer sysDB.Close()
		sysDB.SetMaxOpenConns(4)
		execIssue26114SQLRequire(t, ctx, sysDB, "set role moadmin")

		const (
			accountName   = "issue_26114_target"
			targetDB      = "issue_26114_target_db"
			sourceDB      = "issue_26114_source"
			dbSource      = "issue_26114_db_source"
			dbDestination = "issue_26114_db_destination"
			tableSnapshot = "issue_26114_table_sp"
			dbSnapshot    = "issue_26114_db_sp"
		)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execIssue26114SQLMaybe(cleanupCtx, sysDB, "drop snapshot if exists "+tableSnapshot)
			execIssue26114SQLMaybe(cleanupCtx, sysDB, "drop snapshot if exists "+dbSnapshot)
			execIssue26114SQLMaybe(cleanupCtx, sysDB, "drop database if exists `"+sourceDB+"`")
			execIssue26114SQLMaybe(cleanupCtx, sysDB, "drop database if exists `"+dbSource+"`")
			execIssue26114SQLMaybe(cleanupCtx, sysDB, "drop account if exists "+accountName)
		}()

		execIssue26114SQLMaybe(ctx, sysDB, "drop account if exists "+accountName)
		accountID := testutils.CreateAccount(t, c, accountName, "111")
		execIssue26114SQLRequire(t, ctx, sysDB, "select mo_feature_registry_upsert('branch', 'Branch feature', '{\"allowed_scope\":[]}', true)")
		execIssue26114SQLRequire(t, ctx, sysDB, fmt.Sprintf(
			"select mo_feature_limit_upsert(%d, 'branch', '', 0)", accountID))

		tenantDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, port))
		require.NoError(t, err)
		defer tenantDB.Close()
		execIssue26114SQLRequire(t, ctx, tenantDB, "create database `"+targetDB+"`")

		execIssue26114SQLRequire(t, ctx, sysDB, "create database `"+sourceDB+"`")
		execIssue26114SQLRequire(t, ctx, sysDB, "create table `"+sourceDB+"`.`base` (id int primary key)")
		execIssue26114SQLRequire(t, ctx, sysDB, "insert into `"+sourceDB+"`.`base` values (1)")
		execIssue26114SQLRequire(t, ctx, sysDB, "create snapshot "+tableSnapshot+" for table `"+sourceDB+"` `base`")

		execIssue26114SQLRequire(t, ctx, sysDB, "create database `"+dbSource+"`")
		execIssue26114SQLRequire(t, ctx, sysDB, "create table `"+dbSource+"`.`t1` (id int primary key)")
		execIssue26114SQLRequire(t, ctx, sysDB, "create table `"+dbSource+"`.`t2` (id int primary key)")
		execIssue26114SQLRequire(t, ctx, sysDB, "insert into `"+dbSource+"`.`t1` values (1)")
		execIssue26114SQLRequire(t, ctx, sysDB, "insert into `"+dbSource+"`.`t2` values (2)")
		execIssue26114SQLRequire(t, ctx, sysDB, "create snapshot "+dbSnapshot+" for database `"+dbSource+"`")

		_, err = sysDB.ExecContext(ctx, "data branch create table `"+targetDB+"`.`blocked` from `"+
			sourceDB+"`.`base`{snapshot='"+tableSnapshot+"'} to account "+accountName)
		require.Error(t, err)
		require.Contains(t, err.Error(), "has disabled for account "+accountName)

		_, err = sysDB.ExecContext(ctx, "data branch create database `"+dbDestination+"` from `"+
			dbSource+"`{snapshot='"+dbSnapshot+"'} to account "+accountName)
		require.Error(t, err)
		require.Contains(t, err.Error(), "has disabled for account "+accountName)

		var count int
		require.NoError(t, tenantDB.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_tables where reldatabase = '"+targetDB+"' and relname = 'blocked'").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, tenantDB.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_database where datname = '"+dbDestination+"'").Scan(&count))
		require.Zero(t, count)

		execIssue26114SQLRequire(t, ctx, sysDB, fmt.Sprintf(
			"select mo_feature_limit_upsert(%d, 'branch', '', 1)", accountID))
		start := make(chan struct{})
		results := make(chan error, 2)
		for _, tableName := range []string{"race_one", "race_two"} {
			go func(name string) {
				<-start
				_, createErr := sysDB.ExecContext(ctx, "data branch create table `"+targetDB+"`.`"+name+"` from `"+
					sourceDB+"`.`base`{snapshot='"+tableSnapshot+"'} to account "+accountName)
				results <- createErr
			}(tableName)
		}
		close(start)
		succeeded := 0
		rejected := 0
		for range 2 {
			createErr := <-results
			if createErr == nil {
				succeeded++
				continue
			}
			require.True(t, strings.Contains(createErr.Error(), "has reached the limit of 1"), createErr)
			rejected++
		}
		require.Equal(t, 1, succeeded)
		require.Equal(t, 1, rejected)
		require.NoError(t, tenantDB.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_tables where reldatabase = '"+targetDB+"' and relname in ('race_one', 'race_two')").Scan(&count))
		require.Equal(t, 1, count)

		execIssue26114SQLRequire(t, ctx, sysDB, fmt.Sprintf(
			"select mo_feature_limit_upsert(%d, 'branch', '', 4)", accountID))
		execIssue26114SQLRequire(t, ctx, sysDB, "data branch create table `"+targetDB+"`.`allowed` from `"+
			sourceDB+"`.`base`{snapshot='"+tableSnapshot+"'} to account "+accountName)
		execIssue26114SQLRequire(t, ctx, sysDB, "data branch create database `"+dbDestination+"` from `"+
			dbSource+"`{snapshot='"+dbSnapshot+"'} to account "+accountName)

		require.NoError(t, sysDB.QueryRowContext(ctx, fmt.Sprintf(
			"select count(*) from mo_catalog.mo_branch_metadata b join mo_catalog.mo_tables t on b.table_id = t.rel_id "+
				"where t.account_id = %d and b.creator = %d and b.table_deleted = false", accountID, accountID)).Scan(&count))
		require.Equal(t, 4, count)
	})
}

func TestIssue26114LegacyCrossAccountMetadataCountsTowardTargetQuota(t *testing.T) {
	runIssue26114ClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer sysDB.Close()
		execIssue26114SQLRequire(t, ctx, sysDB, "set role moadmin")

		const (
			accountName = "issue_26114_legacy_target"
			targetDB    = "issue_26114_legacy_dst"
			sourceDB    = "issue_26114_legacy_src"
			snapshot    = "issue_26114_legacy_sp"
		)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execIssue26114SQLMaybe(cleanupCtx, sysDB, "drop snapshot if exists "+snapshot)
			execIssue26114SQLMaybe(cleanupCtx, sysDB, "drop database if exists `"+sourceDB+"`")
			execIssue26114SQLMaybe(cleanupCtx, sysDB, "drop account if exists "+accountName)
		}()

		execIssue26114SQLMaybe(ctx, sysDB, "drop account if exists "+accountName)
		accountID := testutils.CreateAccount(t, c, accountName, "111")
		execIssue26114SQLRequire(t, ctx, sysDB, "select mo_feature_registry_upsert('branch', 'Branch feature', '{\"allowed_scope\":[]}', true)")
		execIssue26114SQLRequire(t, ctx, sysDB, fmt.Sprintf(
			"select mo_feature_limit_upsert(%d, 'branch', '', -1)", accountID))

		tenantDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, port))
		require.NoError(t, err)
		defer tenantDB.Close()
		execIssue26114SQLRequire(t, ctx, tenantDB, "create database `"+targetDB+"`")

		execIssue26114SQLRequire(t, ctx, sysDB, "create database `"+sourceDB+"`")
		execIssue26114SQLRequire(t, ctx, sysDB, "create table `"+sourceDB+"`.`base` (id int primary key)")
		execIssue26114SQLRequire(t, ctx, sysDB, "insert into `"+sourceDB+"`.`base` values (1)")
		execIssue26114SQLRequire(t, ctx, sysDB, "create snapshot "+snapshot+" for table `"+sourceDB+"` `base`")

		execIssue26114SQLRequire(t, ctx, sysDB, "data branch create table `"+targetDB+"`.`legacy` from `"+
			sourceDB+"`.`base`{snapshot='"+snapshot+"'} to account "+accountName)

		var legacyTableID uint64
		require.NoError(t, tenantDB.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = '"+targetDB+"' and relname = 'legacy'").Scan(&legacyTableID))
		// Model the representation persisted by released binaries while retaining
		// the real target-owned table, branch metadata, and protect snapshot.
		internalExec := testutils.GetSQLExecutor(cn)
		result, err := internalExec.Exec(ctx, fmt.Sprintf(
			"update mo_branch_metadata set creator = 0 where table_id = %d", legacyTableID),
			executor.Options{}.WithDatabase("mo_catalog").WithAccountID(0))
		require.NoError(t, err)
		result.Close()
		execIssue26114SQLRequire(t, ctx, sysDB, fmt.Sprintf(
			"select mo_feature_limit_upsert(%d, 'branch', '', 1)", accountID))

		_, err = sysDB.ExecContext(ctx, "data branch create table `"+targetDB+"`.`should_reject` from `"+
			sourceDB+"`.`base`{snapshot='"+snapshot+"'} to account "+accountName)
		require.Error(t, err)
		require.Contains(t, err.Error(), "has reached the limit of 1")

		var activeTargetTables int
		require.NoError(t, tenantDB.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_tables where reldatabase = '"+targetDB+"' and relname in ('legacy', 'should_reject')").Scan(&activeTargetTables))
		require.Equal(t, 1, activeTargetTables)
	})
}
