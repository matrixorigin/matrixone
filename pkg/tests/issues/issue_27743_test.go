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

func TestIssue27743SpecialUserSeesCrossCNDDL(t *testing.T) {
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

		ddlDB, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", ddlPort))
		require.NoError(t, err)
		defer ddlDB.Close()
		execSQLRequire(t, ctx, ddlDB, "set role moadmin")

		const database = "issue_27743_cross_cn_ddl"
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, ddlDB, "drop database if exists `"+database+"`")
		}()
		execSQLRequire(t, ctx, ddlDB, "drop database if exists `"+database+"`")
		execSQLRequire(t, ctx, ddlDB, "create database `"+database+"`")

		// Reproduce #27743: the CREATE response returns on CN-A before a new
		// sys:dump connection starts on CN-B. Do not add SYNCCOMMIT, sleeps,
		// polling, or retries around the first read.
		execSQLRequire(t, ctx, ddlDB,
			"create table `"+database+"`.`created_on_cn_a` (id int primary key)")

		readDB, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/%s", readPort, database))
		require.NoError(t, err)
		readDB.SetMaxIdleConns(0)
		defer readDB.Close()

		var count int
		require.NoError(t, readDB.QueryRowContext(ctx,
			"select count(*) from created_on_cn_a").Scan(&count),
			"CN-B's first table access must observe CN-A's committed CREATE")
		require.Zero(t, count)
	})
}
