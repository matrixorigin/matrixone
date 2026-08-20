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

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestIssue26111DataBranchDatabaseWithCyclicForeignKeys(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		defer db.Close()
		execSQLRequire(t, ctx, db, "set role moadmin")

		const (
			sourceDB       = "issue_26111_source"
			branchDB       = "issue_26111_branch"
			snapshotBranch = "issue_26111_snapshot_branch"
			accountBranch  = "issue_26111_account_branch"
			snapshotName   = "issue_26111_snapshot"
			existingTarget = "issue_26111_existing"
			targetAccount  = "i26111t"
		)
		execSQLRequire(t, ctx, db, "drop account if exists `"+targetAccount+"`")
		execSQLRequire(t, ctx, db, "drop snapshot if exists "+snapshotName)
		for _, name := range []string{branchDB, snapshotBranch, existingTarget, sourceDB} {
			execSQLRequire(t, ctx, db, "drop database if exists `"+name+"`")
		}
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop account if exists `"+targetAccount+"`")
			execSQLRequire(t, cleanupCtx, db, "drop snapshot if exists "+snapshotName)
			for _, name := range []string{branchDB, snapshotBranch, existingTarget, sourceDB} {
				execSQLRequire(t, cleanupCtx, db, "drop database if exists `"+name+"`")
			}
		}()

		execSQLRequire(t, ctx, db, "create database `"+sourceDB+"`")
		execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`.`a` (id int primary key, b_id int)")
		execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`.`b` (id int primary key, a_id int, constraint `fk_b_a` foreign key (a_id) references `"+sourceDB+"`.`a`(id))")
		execSQLRequire(t, ctx, db, "alter table `"+sourceDB+"`.`a` add constraint `fk_a_b` foreign key (b_id) references `"+sourceDB+"`.`b`(id)")
		execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`.`a` values (1, null)")
		execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`.`b` values (1, 1)")
		execSQLRequire(t, ctx, db, "update `"+sourceDB+"`.`a` set b_id = 1 where id = 1")
		execSQLRequire(t, ctx, db, "create database `"+existingTarget+"`")
		_, err = db.ExecContext(ctx, "data branch create database `"+existingTarget+"` from `"+sourceDB+"`")
		require.Error(t, err)
		var foreignKeyChecks int
		require.NoError(t, db.QueryRowContext(ctx, "select @@session.foreign_key_checks").Scan(&foreignKeyChecks))
		require.Equal(t, 1, foreignKeyChecks)

		execSQLRequire(t, ctx, db, "create snapshot "+snapshotName+" for database `"+sourceDB+"`")
		execSQLRequire(t, ctx, db, "data branch create database `"+branchDB+"` from `"+sourceDB+"`")
		execSQLRequire(t, ctx, db, "data branch create database `"+snapshotBranch+"` from `"+sourceDB+"` {snapshot='"+snapshotName+"'}")
		testutils.WaitDatabaseCreated(t, branchDB, cn)
		testutils.WaitTableCreated(t, branchDB, "a", cn)
		testutils.WaitTableCreated(t, branchDB, "b", cn)
		testutils.WaitDatabaseCreated(t, snapshotBranch, cn)
		testutils.WaitTableCreated(t, snapshotBranch, "a", cn)
		testutils.WaitTableCreated(t, snapshotBranch, "b", cn)
		targetAccountID := testutils.CreateAccount(t, c, targetAccount, "111")
		execSQLRequire(t, ctx, db, "data branch create database `"+accountBranch+"` from `"+sourceDB+"` {snapshot='"+snapshotName+"'} to account `"+targetAccount+"`")
		testutils.WaitDatabaseCreatedWithAccount(t, targetAccountID, accountBranch, cn)
		testutils.WaitTableCreatedWithAccount(t, targetAccountID, accountBranch, "a", cn)
		testutils.WaitTableCreatedWithAccount(t, targetAccountID, accountBranch, "b", cn)
		rootResult, err := testutils.GetSQLExecutor(cn).Exec(
			defines.AttachAccountId(ctx, 0),
			fmt.Sprintf("select count(*) from mo_catalog.mo_database where account_id = %d and datname = '%s'", targetAccountID, accountBranch),
			executor.Options{}.
				WithAccountID(0).
				WithDatabase("mo_catalog").
				WithWaitCommittedLogApplied(),
		)
		require.NoError(t, err)
		require.Equal(t, 1, testutils.ReadCount(rootResult))
		rootResult.Close()

		for _, destination := range []string{branchDB, snapshotBranch} {
			rootCtx := defines.AttachAccountId(ctx, 0)
			rootExec := testutils.GetSQLExecutor(cn)
			rootOpts := executor.Options{}.
				WithAccountID(0).
				WithDatabase(destination).
				WithWaitCommittedLogApplied()
			result, err := rootExec.Exec(rootCtx,
				"select count(*) from `a` a join `b` b on a.b_id = b.id and b.a_id = a.id",
				rootOpts)
			require.NoError(t, err)
			require.Equal(t, 1, testutils.ReadCount(result))
			result.Close()

			result, err = rootExec.Exec(rootCtx,
				"select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+destination+"' and refer_db_name = '"+destination+"' and ((table_name = 'a' and refer_table_name = 'b') or (table_name = 'b' and refer_table_name = 'a'))",
				rootOpts)
			require.NoError(t, err)
			require.Equal(t, 2, testutils.ReadCount(result))
			result.Close()

			result, err = rootExec.Exec(rootCtx, "insert into `a` values (2, 999)", rootOpts)
			result.Close()
			require.Error(t, err)
			result, err = rootExec.Exec(rootCtx, "insert into `b` values (2, 999)", rootOpts)
			result.Close()
			require.Error(t, err)
		}

		tenantCtx := defines.AttachAccountId(ctx, uint32(targetAccountID))
		tenantExec := cn.RawService().(cnservice.Service).GetSQLExecutor()
		tenantOpts := executor.Options{}.WithAccountID(uint32(targetAccountID)).WithDatabase(accountBranch)

		result, err := tenantExec.Exec(tenantCtx,
			"select count(*) from `a` a join `b` b on a.b_id = b.id and b.a_id = a.id", tenantOpts)
		require.NoError(t, err)
		require.Equal(t, 1, testutils.ReadCount(result))
		result.Close()

		result, err = tenantExec.Exec(tenantCtx,
			"select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+accountBranch+"' and refer_db_name = '"+accountBranch+"' and ((table_name = 'a' and refer_table_name = 'b') or (table_name = 'b' and refer_table_name = 'a'))", tenantOpts)
		require.NoError(t, err)
		require.Equal(t, 2, testutils.ReadCount(result))
		result.Close()

		result, err = tenantExec.Exec(tenantCtx, "insert into `a` values (2, 999)", tenantOpts)
		result.Close()
		require.Error(t, err)
		result, err = tenantExec.Exec(tenantCtx, "insert into `b` values (2, 999)", tenantOpts)
		result.Close()
		require.Error(t, err)

		_, err = db.ExecContext(ctx, "data branch create database `"+accountBranch+"` from `"+sourceDB+"` {snapshot='"+snapshotName+"'} to account `"+targetAccount+"`")
		require.Error(t, err)
		require.NoError(t, db.QueryRowContext(ctx, "select @@session.foreign_key_checks").Scan(&foreignKeyChecks))
		require.Equal(t, 1, foreignKeyChecks)
		result, err = tenantExec.Exec(tenantCtx,
			"select count(*) from mo_catalog.mo_tables where reldatabase = '"+accountBranch+"' and relkind = 'r'", tenantOpts)
		require.NoError(t, err)
		require.Equal(t, 2, testutils.ReadCount(result))
		result.Close()
		result, err = tenantExec.Exec(tenantCtx,
			"select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+accountBranch+"' and refer_db_name = '"+accountBranch+"'", tenantOpts)
		require.NoError(t, err)
		require.Equal(t, 2, testutils.ReadCount(result))
		result.Close()
	})
}
