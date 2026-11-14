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
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// This is a synthetic shared-CTE/join witness, not the customer's schema or SQL.
const issue31064SharedCTESQL = `
WITH rep AS (
    SELECT a.zone, AVG(a.v) / SUM(b.denom) AS ratio
    FROM ra a JOIN rb b ON a.k = b.k JOIN rc c ON b.k = c.k
    GROUP BY a.zone
), line AS (
    SELECT x.amount AS response_hours,
           CASE WHEN y.flag = 1 THEN 2 ELSE -1 END AS multiplier
    FROM la x JOIN lb y ON x.k = y.k
    WHERE x.k IN (1, 2, 3)
)
SELECT
    (SELECT COUNT(*) FROM rep WHERE zone = 0),
    (SELECT SUM(ratio) FROM rep),
    (SELECT COUNT(*) FROM line WHERE multiplier > 0),
    (SELECT SUM(response_hours * multiplier) FROM line)`

// A shared-CTE query must release its merge intermediates without suspending
// account admission; cached execution must obey the same ownership contract.
// Reuse the package's serialized authenticated cluster: the real frontend and a
// genuinely new authentication are the public oracles, not an injected engine.
func TestIssue31064SharedCTEJoinAllocation(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		require.False(t, cn.GetServiceConfig().CN.Frontend.SkipCheckUser)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
			cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		// Repeat setup and verified teardown on the same cluster, not a restart
		// that could conceal leaked allocation/session state.
		for round := 0; round < 2; round++ {
			if !t.Run(fmt.Sprintf("round_%d", round+1), func(t *testing.T) {
				const database = "issue_31064_shared_cte"
				t.Cleanup(func() {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cleanupCancel()
					_, _ = conn.ExecContext(cleanupCtx, "use mo_catalog")
					_, cleanupErr := conn.ExecContext(cleanupCtx, "drop database if exists "+database)
					require.NoError(t, cleanupErr)
				})
				mustExec(t, ctx, conn, "create database "+database)
				mustExec(t, ctx, conn, "use "+database)
				// Keep the reduced witness's 1K/10K cardinalities so ordinary
				// ANALYZE statistics select materialized CTEs and joins without
				// fabricated statistics, optimizer hooks, or customer-scale data.
				// Only the three matching line keys affect the semantic result.
				for _, statement := range []string{
					"create table ra (k int, zone int, v double)",
					"create table rb (k int, denom double)",
					"create table rc (k int)",
					"create table la (k int, amount double)",
					"create table lb (k int, flag int)",
					"insert into ra select result, mod(result, 2), 10 from generate_series(1,1000) g",
					"insert into rb select result, 1 from generate_series(1,1000) g",
					"insert into rc select result from generate_series(1,1000) g",
					"insert into la select result, result from generate_series(1,10000) g",
					"insert into lb values (1,1),(2,0),(3,1)",
					"analyze table ra, rb, rc, la, lb",
				} {
					mustExec(t, ctx, conn, statement)
				}

				plan, err := testutils.QueryText(ctx, conn, "explain "+issue31064SharedCTESQL)
				require.NoError(t, err)
				sinkScans := strings.Count(plan, "Sink Scan")
				t.Logf("shared CTE Sink Scan count=%d", sinkScans)
				require.GreaterOrEqual(t, sinkScans, 4,
					"all four scalar consumers must reach shared CTEs:\n%s", plan)

				t.Log("direct query")
				rows, err := conn.QueryContext(ctx, issue31064SharedCTESQL)
				requireIssue31064Result(t, rows, err)

				stmt, err := conn.PrepareContext(ctx, issue31064SharedCTESQL)
				require.NoError(t, err)
				defer stmt.Close()
				for execution := 0; execution < 2; execution++ {
					t.Logf("binary prepared execution %d", execution+1)
					rows, err = stmt.QueryContext(ctx)
					requireIssue31064Result(t, rows, err)
				}
				require.NoError(t, stmt.Close())

				// SQL PREPARE/EXECUTE takes the text-protocol path. Both
				// protocols are repeated on the explicitly pinned *sql.Conn.
				quotedSQL := strings.ReplaceAll(strings.TrimSpace(issue31064SharedCTESQL), "'", "''")
				mustExec(t, ctx, conn, "prepare issue31064_stmt from '"+quotedSQL+"'")
				for execution := 0; execution < 2; execution++ {
					t.Logf("text prepared execution %d", execution+1)
					rows, err = conn.QueryContext(ctx, "execute issue31064_stmt")
					requireIssue31064Result(t, rows, err)
				}
				mustExec(t, ctx, conn, "deallocate prepare issue31064_stmt")

				// sql.DB alone does not pin a physical connection. An
				// independent pool, with no existing sessions, must authenticate
				// successfully after all five query executions.
				freshDB, err := sql.Open("mysql", dsn)
				require.NoError(t, err)
				defer freshDB.Close()
				require.NoError(t, freshDB.PingContext(ctx))
				var tableRows int
				require.NoError(t, freshDB.QueryRowContext(ctx,
					"select count(*) from "+database+".ra").Scan(&tableRows))
				require.Equal(t, 1000, tableRows)
				t.Logf("fresh independent authentication and table query passed: rows=%d", tableRows)

				mustExec(t, ctx, conn, "use mo_catalog")
				mustExec(t, ctx, conn, "drop database "+database)
				var databases int
				require.NoError(t, conn.QueryRowContext(ctx,
					"select count(*) from information_schema.schemata where schema_name = '"+database+"'").Scan(&databases))
				require.Zero(t, databases, "teardown must finish before the same-instance repetition")
			}) {
				return
			}
		}
	})
}

func requireIssue31064Result(t *testing.T, rows *sql.Rows, queryErr error) {
	t.Helper()
	if rows != nil {
		defer rows.Close()
	}
	require.NoError(t, queryErr)
	require.True(t, rows.Next(), "the four scalar consumers must produce one row")
	var evenGroups, positive int
	var ratioSum, weightedSum float64
	require.NoError(t, rows.Scan(&evenGroups, &ratioSum, &positive, &weightedSum))
	t.Logf("result=[%d %.12g %d %.12g]", evenGroups, ratioSum, positive, weightedSum)
	require.Equal(t, 1, evenGroups)
	// Each zone has 500 rows: 10/500 + 10/500 = 0.04.
	require.InDelta(t, 0.04, ratioSum, 1e-9)
	require.Equal(t, 2, positive)
	// The line contribution is 1*2 + 2*(-1) + 3*2 = 6.
	require.InDelta(t, 6, weightedSum, 1e-9)
	require.False(t, rows.Next(), "the scalar result must contain exactly one row")
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
}
