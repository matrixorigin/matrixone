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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func execSQLRequire(t *testing.T, ctx context.Context, db *sql.DB, statement string) {
	t.Helper()
	_, err := db.ExecContext(ctx, statement)
	require.NoErrorf(t, err, "exec failed: %s", statement)
}

func execSQLMaybe(t *testing.T, ctx context.Context, db *sql.DB, statement string) {
	t.Helper()
	_, _ = db.ExecContext(ctx, statement)
}

func TestIssue26111DataBranchDatabaseWithCyclicForeignKeys(t *testing.T) {
	c, err := embed.StartTestCluster(embed.WithCNCount(1))
	if c != nil {
		t.Cleanup(func() { require.NoError(t, c.Close()) })
	}
	require.NoError(t, err)
	require.NotNil(t, c)

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
	require.NoError(t, waitSystemBootstrap(ctx, db))

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
	targetAccountID := testutils.CreateAccount(t, c, targetAccount, "111")
	execSQLRequire(t, ctx, db, "data branch create database `"+accountBranch+"` from `"+sourceDB+"` {snapshot='"+snapshotName+"'} to account `"+targetAccount+"`")
	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_database where account_id = ? and datname = ?", targetAccountID, accountBranch).Scan(&count))
	require.Equal(t, 1, count)

	for _, destination := range []string{branchDB, snapshotBranch} {
		var count int
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from `"+destination+"`.`a` a join `"+destination+"`.`b` b on a.b_id = b.id and b.a_id = a.id").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+destination+"' and refer_db_name = '"+destination+"' and ((table_name = 'a' and refer_table_name = 'b') or (table_name = 'b' and refer_table_name = 'a'))").Scan(&count))
		require.Equal(t, 2, count)
		_, err = db.ExecContext(ctx, "insert into `"+destination+"`.`a` values (2, 999)")
		require.Error(t, err)
		_, err = db.ExecContext(ctx, "insert into `"+destination+"`.`b` values (2, 999)")
		require.Error(t, err)
	}

	// The cross-account DDL returns only after commit. Verify visibility and FK
	// enforcement through the target account's public SQL path instead of three
	// open-ended catalog polling loops.
	targetDB, err := sql.Open("mysql", fmt.Sprintf(
		"%s#root#accountadmin:111@tcp(127.0.0.1:%d)/%s", targetAccount, port, accountBranch,
	))
	require.NoError(t, err)
	defer targetDB.Close()
	require.NoError(t, targetDB.PingContext(ctx))
	require.NoError(t, targetDB.QueryRowContext(ctx,
		"select count(*) from `a` a join `b` b on a.b_id = b.id and b.a_id = a.id").Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, targetDB.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+accountBranch+"' and refer_db_name = '"+accountBranch+"' and ((table_name = 'a' and refer_table_name = 'b') or (table_name = 'b' and refer_table_name = 'a'))").Scan(&count))
	require.Equal(t, 2, count)

	_, err = targetDB.ExecContext(ctx, "insert into `a` values (2, 999)")
	require.Error(t, err)
	_, err = targetDB.ExecContext(ctx, "insert into `b` values (2, 999)")
	require.Error(t, err)

	_, err = db.ExecContext(ctx, "data branch create database `"+accountBranch+"` from `"+sourceDB+"` {snapshot='"+snapshotName+"'} to account `"+targetAccount+"`")
	require.Error(t, err)
	require.NoError(t, db.QueryRowContext(ctx, "select @@session.foreign_key_checks").Scan(&foreignKeyChecks))
	require.Equal(t, 1, foreignKeyChecks)
	require.NoError(t, targetDB.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_tables where reldatabase = '"+accountBranch+"' and relkind = 'r'").Scan(&count))
	require.Equal(t, 2, count)
	require.NoError(t, targetDB.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+accountBranch+"' and refer_db_name = '"+accountBranch+"'").Scan(&count))
	require.Equal(t, 2, count)
}
