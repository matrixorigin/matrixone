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
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	joinSpillRegressionRowsEnv = "MO_JOIN_SPILL_REGRESSION_ROWS"
	defaultJoinSpillRows       = int64(4096)
	minJoinSpillRows           = int64(1024)
	maxJoinSpillRows           = int64(65_536)
)

var (
	positiveSpillRowsPattern = regexp.MustCompile(`SpillRows=([1-9][0-9]*)`)
	positiveSpillSizePattern = regexp.MustCompile(`SpillSize=([1-9][0-9]*(?:\.[0-9]+)?) (?:bytes|KiB|MiB|GiB|TiB)`)
)

// TestIssue26408JoinSpillSQLRegression exercises the optimizer, compiler, and
// execution engine through the SQL protocol. It deliberately uses patched
// statistics and a low row-count threshold instead of BVT-sized input.
//
// MO_JOIN_SPILL_REGRESSION_ROWS controls the physical scale without changing
// any correctness oracle. The default keeps presubmit cheap, and the upper
// bound prevents an accidental CI setting from exhausting the test host.
func TestIssue26408JoinSpillSQLRegression(t *testing.T) {
	rows := joinSpillRegressionRows(t)
	spillThreshold := rows / 128
	if spillThreshold < 1 {
		spillThreshold = 1
	}

	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		execJoinSpillSQL(t, ctx, conn, "set role moadmin")
		execJoinSpillSQL(t, ctx, conn, "drop database if exists `"+dbName+"`")
		execJoinSpillSQL(t, ctx, conn, "create database `"+dbName+"`")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "drop database if exists `"+dbName+"`")
		}()
		execJoinSpillSQL(t, ctx, conn, "use `"+dbName+"`")
		execJoinSpillSQL(t, ctx, conn, "set @@max_dop = 1")
		defer resetOptimizerHintsOnCN(t, port)
		execJoinSpillSQL(t, ctx, conn, `set session optimizer_hints = "forceOneCN=1"`)

		execJoinSpillSQL(t, ctx, conn, "create table probe_keys (k bigint not null, payload bigint not null) cluster by k")
		execJoinSpillSQL(t, ctx, conn, "create table build_keys (k bigint not null, payload bigint not null) cluster by k")
		execJoinSpillSQL(t, ctx, conn, fmt.Sprintf(
			"insert into build_keys select result, result from generate_series(1, %d) g",
			rows,
		))
		// Every probe batch has the same ordered key. This reaches the
		// allBatchInOneRange shortcut that used to index an empty boundary.
		execJoinSpillSQL(t, ctx, conn, fmt.Sprintf(
			"insert into probe_keys select 7, result from generate_series(1, %d) g",
			rows,
		))

		patchJoinSpillStats(t, ctx, conn, dbName, "probe_keys", 20_000_000)
		patchJoinSpillStats(t, ctx, conn, dbName, "build_keys", 10_000_000)

		residentQuery := joinSpillQuery("resident_probe", "resident_build")
		spillQuery := joinSpillQuery("spill_probe", "spill_build")
		analyzeQuery := joinSpillQuery("analyze_probe", "analyze_build")

		execJoinSpillSQL(t, ctx, conn, "set @@join_spill_mem = 0")
		residentPlan := queryJoinSpillText(t, ctx, conn, "explain "+residentQuery)
		require.Contains(t, residentPlan, "shuffle: range(")
		resident := queryJoinSpillResult(t, ctx, conn, residentQuery)
		require.Equal(t, rows, resident.count)

		spillBefore := promtestutil.ToFloat64(
			metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"),
		)
		respillBefore := promtestutil.ToFloat64(
			metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", "2"),
		)
		execJoinSpillSQL(t, ctx, conn, fmt.Sprintf("set @@join_spill_mem = %d", spillThreshold))
		var configuredThreshold int64
		require.NoError(t, conn.QueryRowContext(ctx, "select @@join_spill_mem").Scan(&configuredThreshold))
		require.Equal(t, spillThreshold, configuredThreshold)

		spilled := queryJoinSpillResult(t, ctx, conn, spillQuery)
		require.Equal(t, resident, spilled)
		analyzePlan := queryJoinSpillText(t, ctx, conn, "explain (analyze true) "+analyzeQuery)
		require.Contains(t, analyzePlan, "shuffle: range(")
		require.Regexpf(t, positiveSpillRowsPattern, analyzePlan, "plan did not report spill rows:\n%s", analyzePlan)
		require.Regexpf(t, positiveSpillSizePattern, analyzePlan, "plan did not report spill bytes:\n%s", analyzePlan)
		require.Greater(t, promtestutil.ToFloat64(
			metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"),
		), spillBefore)
		require.Greater(t, promtestutil.ToFloat64(
			metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", "2"),
		), respillBefore)
	})
}

type joinSpillSQLResult struct {
	count    int64
	probeSum string
	buildSum string
}

func joinSpillQuery(probeAlias, buildAlias string) string {
	return fmt.Sprintf(
		"select count(*), sum(%s.payload), sum(%s.payload) "+
			"from probe_keys %s join build_keys %s on %s.k = %s.k",
		probeAlias,
		buildAlias,
		probeAlias,
		buildAlias,
		probeAlias,
		buildAlias,
	)
}

func joinSpillRegressionRows(t *testing.T) int64 {
	t.Helper()
	value := os.Getenv(joinSpillRegressionRowsEnv)
	if value == "" {
		return defaultJoinSpillRows
	}
	rows, err := strconv.ParseInt(value, 10, 64)
	require.NoErrorf(t, err, "%s must be a positive integer", joinSpillRegressionRowsEnv)
	require.GreaterOrEqualf(t, rows, minJoinSpillRows,
		"%s must be at least %d so the test deterministically reaches re-spill",
		joinSpillRegressionRowsEnv, minJoinSpillRows)
	require.LessOrEqualf(t, rows, maxJoinSpillRows,
		"%s must not exceed %d; larger spill soak tests belong outside presubmit",
		joinSpillRegressionRowsEnv, maxJoinSpillRows)
	return rows
}

func queryJoinSpillResult(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	query string,
) joinSpillSQLResult {
	t.Helper()
	var result joinSpillSQLResult
	err := conn.QueryRowContext(ctx, query).Scan(&result.count, &result.probeSum, &result.buildSum)
	require.NoErrorf(t, err, "query failed: %s", query)
	return result
}
