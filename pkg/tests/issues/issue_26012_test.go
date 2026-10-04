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

func TestIssue26012MultiKeyEnumOrder(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		dbConn, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer dbConn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		execSQLRequire(t, ctx, dbConn, "set role moadmin")

		const db = "issue_26012"
		for _, stmt := range []string{
			"drop database if exists " + db,
			"create database " + db,
			"create table " + db + ".t (id int primary key, e enum('low','medium','high'))",
			"insert into " + db + ".t values (1, 'low'), (2, 'high'), (3, null)",
		} {
			execSQLRequire(t, ctx, dbConn, stmt)
		}
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, dbConn, "drop database if exists "+db)
		}()

		for _, query := range []string{
			"select id from " + db + ".t order by e is null, e desc",
			"select id from (select id, e from " + db + ".t) d order by e is null, e desc",
			"with c as (select id, e from " + db + ".t) select id from c order by e is null, e desc",
		} {
			func(query string) {
				rows, err := dbConn.QueryContext(ctx, query)
				require.NoError(t, err, query)
				defer rows.Close()
				var got []int
				for rows.Next() {
					var id int
					require.NoError(t, rows.Scan(&id), query)
					got = append(got, id)
				}
				require.NoError(t, rows.Err(), query)
				require.Equal(t, []int{2, 1, 3}, got, query)
			}(query)
		}
	})
}
