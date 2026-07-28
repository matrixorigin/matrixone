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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue26111DataBranchDatabaseWithCyclicForeignKeys(t *testing.T) {
	embed.RunBaseClusterTests(func(c embed.Cluster) {
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
			snapshotName   = "issue_26111_snapshot"
			existingTarget = "issue_26111_existing"
		)
		execSQLRequire(t, ctx, db, "drop snapshot if exists "+snapshotName)
		for _, name := range []string{branchDB, snapshotBranch, existingTarget, sourceDB} {
			execSQLRequire(t, ctx, db, "drop database if exists `"+name+"`")
		}
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
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
	})
}
