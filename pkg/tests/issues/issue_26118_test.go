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

func TestIssue26118DatabaseCopiesPreserveHashIdentifiers(t *testing.T) {
	embed.RunBaseClusterTests(func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		execSQLRequire(t, ctx, db, "set role moadmin")

		const (
			sourceDB = "issue#26118#source"
			branchDB = "issue_26118_branch"
			cloneDB  = "issue_26118_clone"
		)
		for _, name := range []string{branchDB, cloneDB, sourceDB} {
			execSQLMaybe(t, ctx, db, "drop database if exists `"+name+"`")
		}
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			for _, name := range []string{branchDB, cloneDB, sourceDB} {
				execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+name+"`")
			}
		}()

		execSQLRequire(t, ctx, db, "create database `"+sourceDB+"`")
		execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`.`parent#p` (id int primary key, note varchar(32))")
		execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`.`child#c` (id int primary key, parent_id int, constraint `fk#parent` foreign key (parent_id) references `"+sourceDB+"`.`parent#p`(id))")
		execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`.`parent#p` values (1, 'one'), (2, 'two')")
		execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`.`child#c` values (10, 1), (20, 2)")
		execSQLRequire(t, ctx, db, "create view `"+sourceDB+"`.`view#1` as select id, note from `"+sourceDB+"`.`parent#p`")
		execSQLRequire(t, ctx, db, "create view `"+sourceDB+"`.`view#2` as select id, note from `"+sourceDB+"`.`view#1`")

		execSQLRequire(t, ctx, db, "data branch create database `"+branchDB+"` from `"+sourceDB+"`")
		execSQLRequire(t, ctx, db, "create database `"+cloneDB+"` clone `"+sourceDB+"`")

		for _, destination := range []string{branchDB, cloneDB} {
			var count int
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from `"+destination+"`.`child#c` c join `"+destination+"`.`parent#p` p on c.parent_id = p.id").Scan(&count))
			require.Equal(t, 2, count)
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from `"+destination+"`.`view#2`").Scan(&count))
			require.Equal(t, 2, count)
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+destination+"' and table_name = 'child#c' and refer_table_name = 'parent#p'").Scan(&count))
			require.Equal(t, 1, count)
		}
	})
}
