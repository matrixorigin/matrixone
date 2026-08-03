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
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	hashBuildRecoveryQueryCap = int64(64 << 20)
	hashBuildRecoveryRows     = 1_000_000
)

// TestHashBuildSharedBudgetRecoverySQL is the SQL-protocol counterexample for
// late spill admission. join_spill_mem remains at its automatic threshold so a
// finite query budget makes the parallel shuffle HashBuild transition from
// retained data to spill. A one-shot fault fixes the first recovery window at
// 1024 rows, making the public fallback path observable without depending on
// worker scheduling.
//
// The physical input is intentionally narrow and bounded. Patched statistics
// select the same shuffle topology as a large analytical join, while the CN's
// query limit creates the relevant shared-budget pressure without TPCH-sized
// fixture data.
func TestHashBuildSharedBudgetRecoverySQL(t *testing.T) {
	cluster, err := embed.StartTestCluster(
		embed.WithCNCount(1),
		embed.WithPreStart(func(service embed.ServiceOperator) {
			if service.ServiceType() != metadata.ServiceType_CN {
				return
			}
			service.Adjust(func(config *embed.ServiceConfig) {
				config.CN.Frontend.ProcessLimitationSize = hashBuildRecoveryQueryCap
				config.CN.Frontend.ProcessLimitationSpillSize = 1 << 30
			})
		}),
	)
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
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
	execJoinSpillSQL(t, ctx, conn, "set @@max_dop = 8")
	defer resetOptimizerHintsOnCN(t, port)
	execJoinSpillSQL(t, ctx, conn,
		`set session optimizer_hints = "forceOneCN=1,joinOrdering=1"`)
	execJoinSpillSQL(t, ctx, conn, "set @@join_spill_mem = 0")

	execJoinSpillSQL(t, ctx, conn,
		"create table recovery_probe (k bigint not null, payload varchar(32) not null) cluster by k")
	execJoinSpillSQL(t, ctx, conn,
		"create table recovery_build (k bigint not null, payload varchar(32) not null, payload2 bigint not null) cluster by k")
	execJoinSpillSQL(t, ctx, conn,
		"insert into recovery_probe select result, 'x' from generate_series(1, 4096) g")
	execJoinSpillSQL(t, ctx, conn, fmt.Sprintf(
		"insert into recovery_build select result, case when result <= 900000 then 'x' else repeat('x', 32) end, -result "+
			"from generate_series(1, %d) g",
		hashBuildRecoveryRows))

	patchJoinSpillStats(t, ctx, conn, dbName, "recovery_probe", 20_000_000)
	patchJoinSpillStats(t, ctx, conn, dbName, "recovery_build", 10_000_000)
	query := `select count(*)
		from recovery_probe p join recovery_build b
			on serial_full(p.k, p.payload) = serial_full(b.k, b.payload)`
	plan := queryJoinSpillText(t, ctx, conn, "explain "+query)
	require.Contains(t, plan, "shuffle: hash(")
	require.Contains(t, plan, "serial_full(",
		"the public regression must exercise the expression-key recovery path")
	probeScan := strings.Index(plan, ".recovery_probe")
	buildScan := strings.Index(plan, ".recovery_build")
	require.NotEqualf(t, -1, probeScan, "probe scan missing from plan:\n%s", plan)
	require.NotEqualf(t, -1, buildScan, "build scan missing from plan:\n%s", plan)
	require.Lessf(t, probeScan, buildScan,
		"the million-row table must remain the right/hash-build input:\n%s", plan)

	spillBefore := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"))
	chunkFallbackBefore := promtestutil.ToFloat64(
		metricv2.HashBuildBudgetEventCounter.WithLabelValues(
			"spill_recovery", "chunk_fallback", "query"))
	execJoinSpillSQL(t, ctx, conn, "select enable_fault_injection()")
	execJoinSpillSQL(t, ctx, conn,
		"select add_fault_point('hashbuild-spill-recovery-chunk-fallback', '1:1:1:', 'echo', 1024, '')")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, _ = conn.ExecContext(cleanupCtx,
			"select remove_fault_point('hashbuild-spill-recovery-chunk-fallback')")
	})
	var count int64
	require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&count))
	require.Equal(t, int64(4096), count)
	require.Greater(t, promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1")), spillBefore,
		"the automatic-threshold query must spill because of the shared hard budget")
	require.Greater(t, promtestutil.ToFloat64(
		metricv2.HashBuildBudgetEventCounter.WithLabelValues(
			"spill_recovery", "chunk_fallback", "query")), chunkFallbackBefore,
		"the SQL counterexample must enter direct-spill chunk recovery")

	// A terminal recovery bug often leaves the service reachable but the query
	// dependency graph poisoned. Verify both the result and a fresh statement.
	var healthy int
	require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&healthy))
	require.Equal(t, 1, healthy)
}
