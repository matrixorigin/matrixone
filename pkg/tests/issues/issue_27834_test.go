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

func TestIssue27834CrossCNAuthenticationReadsLatestCatalog(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		mutationCN, err := c.GetCNService(0)
		require.NoError(t, err)
		loginCN, err := c.GetCNService(1)
		require.NoError(t, err)
		mutationPort := mutationCN.GetServiceConfig().CN.Frontend.Port
		loginPort := loginCN.GetServiceConfig().CN.Frontend.Port
		require.NotEqual(t, mutationPort, loginPort,
			"the acceptance test must route mutation and login to distinct CNs")

		adminDB, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", mutationPort))
		require.NoError(t, err)
		defer adminDB.Close()
		execSQLRequire(t, ctx, adminDB, "set role moadmin")

		const (
			userName = "issue_27834_user"
			roleName = "issue_27834_role"
		)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(
				context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, adminDB, "drop user if exists "+userName)
			execSQLMaybe(t, cleanupCtx, adminDB, "drop role if exists "+roleName)
		}()
		execSQLRequire(t, ctx, adminDB, "drop user if exists "+userName)
		execSQLRequire(t, ctx, adminDB, "drop role if exists "+roleName)
		execSQLRequire(t, ctx, adminDB, "create role "+roleName)
		execSQLRequire(t, ctx, adminDB,
			"create user "+userName+" identified by 'Issue27834Pass01' default role "+roleName)
		execSQLRequire(t, ctx, adminDB, "grant "+roleName+" to "+userName)
		execSQLRequire(t, ctx, adminDB, "grant connect on account * to "+roleName)

		assertLogin := func(password string, wantSuccess bool) {
			t.Helper()
			db, openErr := sql.Open("mysql", fmt.Sprintf(
				"sys#%s#%s:%s@tcp(127.0.0.1:%d)/",
				userName, roleName, password, loginPort))
			require.NoError(t, openErr)
			db.SetMaxIdleConns(0)
			defer db.Close()
			pingErr := db.PingContext(ctx)
			if !wantSuccess {
				require.Error(t, pingErr,
					"a fresh connection on CN-B accepted stale authentication state")
				return
			}
			require.NoError(t, pingErr,
				"a fresh connection on CN-B rejected committed authentication state")
			var one int
			require.NoError(t, db.QueryRowContext(ctx, "select 1").Scan(&one))
			require.Equal(t, 1, one)
		}

		passwords := []string{
			"Issue27834Pass01",
			"Issue27834Pass02",
			"Issue27834Pass03",
			"Issue27834Pass04",
		}
		assertLogin(passwords[0], true)
		for i := 1; i < len(passwords); i++ {
			execSQLRequire(t, ctx, adminDB, fmt.Sprintf(
				"alter user %s identified by '%s'", userName, passwords[i]))
			assertLogin(passwords[i-1], false)
			assertLogin(passwords[i], true)
		}

		execSQLRequire(t, ctx, adminDB, "revoke "+roleName+" from "+userName)
		assertLogin(passwords[len(passwords)-1], false)
		execSQLRequire(t, ctx, adminDB, "grant "+roleName+" to "+userName)
		assertLogin(passwords[len(passwords)-1], true)
	})
}
