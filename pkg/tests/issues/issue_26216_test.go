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

func TestIssue26216CopiedViewSupportsBareBooleanPredicate(t *testing.T) {
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
			sourceDB = "issue_26216_src"
			targetDB = "issue_26216_dst"
		)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+targetDB+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+sourceDB+"`")
		}()

		execSQLRequire(t, ctx, db, "create database `"+sourceDB+"`")
		execSQLRequire(t, ctx, db, "create table `"+sourceDB+"`.`t` (id int primary key, amount decimal(10,2), enabled bool not null)")
		execSQLRequire(t, ctx, db, "insert into `"+sourceDB+"`.`t` values (1, 10.00, true), (2, 20.00, false)")
		execSQLRequire(t, ctx, db, "create view `"+sourceDB+"`.`v` as select id, amount from `"+sourceDB+"`.`t` where enabled")
		assertIssue26216ViewRows(t, ctx, db, sourceDB)

		execSQLRequire(t, ctx, db, "data branch create database `"+targetDB+"` from `"+sourceDB+"`")
		assertIssue26216ViewRows(t, ctx, db, targetDB)
	})
}

func assertIssue26216ViewRows(t *testing.T, ctx context.Context, db *sql.DB, databaseName string) {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		"select id, cast(amount as char) from `"+databaseName+"`.`v` order by id")
	require.NoError(t, err)
	defer rows.Close()

	var ids []int
	var amounts []string
	for rows.Next() {
		var id int
		var amount string
		require.NoError(t, rows.Scan(&id, &amount))
		ids = append(ids, id)
		amounts = append(amounts, amount)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int{1}, ids)
	require.Equal(t, []string{"10.00"}, amounts)
}
