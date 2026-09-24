// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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

// The PICK pipeline runs producer and consumer SQL against one background
// transaction. Repeating the deleted-on-both-sides case from pick_8.sql makes
// nested SQL overlap visible to the workspace statement boundary.
func TestDataBranchPickDeletedKeyWorkspaceStatementOwnership(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		defer func() { require.NoError(t, db.Close()) }()

		const database = "txn_workspace_pick_deleted_key"
		execSQLMaybe(t, ctx, db, "drop database if exists `"+database+"`")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+database+"`")
		}()

		execSQLRequire(t, ctx, db, "create database `"+database+"`")
		execSQLRequire(t, ctx, db,
			"create table `"+database+"`.`base` (id int primary key, value int)")
		execSQLRequire(t, ctx, db,
			"insert into `"+database+"`.`base` values (1, 10), (2, 20), (3, 30)")
		execSQLRequire(t, ctx, db,
			"data branch create table `"+database+"`.`dst` from `"+database+"`.`base`")
		execSQLRequire(t, ctx, db,
			"data branch create table `"+database+"`.`src` from `"+database+"`.`base`")
		execSQLRequire(t, ctx, db, "delete from `"+database+"`.`dst` where id = 2")
		execSQLRequire(t, ctx, db, "delete from `"+database+"`.`src` where id = 2")

		for range 20 {
			execSQLRequire(t, ctx, db,
				"data branch pick `"+database+"`.`src` into `"+database+"`.`dst` keys(2)")
		}
		var count int
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from `"+database+"`.`dst` where id = 2").Scan(&count))
		require.Zero(t, count)
	})
}
