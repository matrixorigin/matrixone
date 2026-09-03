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

func TestIssue27743FreshConnectionSeesCrossCNDDL(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		ddlCN, err := c.GetCNService(0)
		require.NoError(t, err)
		readCN, err := c.GetCNService(1)
		require.NoError(t, err)
		ddlPort := ddlCN.GetServiceConfig().CN.Frontend.Port
		readPort := readCN.GetServiceConfig().CN.Frontend.Port
		require.NotEqual(t, ddlPort, readPort,
			"DDL and the fresh read must use distinct CNs")

		adminDB, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", ddlPort))
		require.NoError(t, err)
		defer adminDB.Close()
		execSQLRequire(t, ctx, adminDB, "set role moadmin")

		const (
			database = "issue_27743_cross_cn_ddl"
			userName = "issue_27743_reader"
			roleName = "issue_27743_reader_role"
			password = "Issue27743Pass01"
		)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, adminDB, "drop user if exists "+userName)
			execSQLMaybe(t, cleanupCtx, adminDB, "drop role if exists "+roleName)
			execSQLMaybe(t, cleanupCtx, adminDB, "drop database if exists `"+database+"`")
		}()
		execSQLRequire(t, ctx, adminDB, "drop user if exists "+userName)
		execSQLRequire(t, ctx, adminDB, "drop role if exists "+roleName)
		execSQLRequire(t, ctx, adminDB, "drop database if exists `"+database+"`")
		execSQLRequire(t, ctx, adminDB, "create database `"+database+"`")
		execSQLRequire(t, ctx, adminDB, "create role "+roleName)
		execSQLRequire(t, ctx, adminDB,
			"create user "+userName+" identified by '"+password+"' default role "+roleName)
		execSQLRequire(t, ctx, adminDB, "grant connect on account * to "+roleName)
		execSQLRequire(t, ctx, adminDB,
			"grant select on table `"+database+"`.* to "+roleName)
		execSQLRequire(t, ctx, adminDB, "grant "+roleName+" to "+userName)

		// This is the ordering from #27743: CREATE commits on CN-A before a fresh
		// connection starts on CN-B. Do not add SYNCCOMMIT, sleeps, or retries.
		execSQLRequire(t, ctx, adminDB,
			"create table `"+database+"`.`created_on_cn_a` (id int primary key)")
		execSQLRequire(t, ctx, adminDB,
			"insert into `"+database+"`.`created_on_cn_a` values (1)")

		readerDB, err := sql.Open("mysql", fmt.Sprintf(
			"sys#%s#%s:%s@tcp(127.0.0.1:%d)/%s",
			userName, roleName, password, readPort, database))
		require.NoError(t, err)
		readerDB.SetMaxIdleConns(0)
		defer readerDB.Close()

		var count int
		require.NoError(t, readerDB.QueryRowContext(ctx,
			"select count(*) from created_on_cn_a").Scan(&count),
			"CN-B's first query must observe CN-A's committed CREATE")
		require.Equal(t, 1, count)
	})
}
