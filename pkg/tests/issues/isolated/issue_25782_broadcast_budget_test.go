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

package isolated

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	issue25782QueryBudget = int64(1 << 20)
	issue25782BuildRows   = int64(200_000)
)

// TestIssue25782BroadcastHashBuildFailsClosedUnderHardBudget proves the
// resident-only topology contract through the SQL protocol:
//
//   - a #25782-like grouped child feeds a parallel low-NDV LEFT JOIN;
//   - the low-NDV join selects broadcast rather than shuffle;
//   - an admitted control returns the correct joined-row count;
//   - a larger build is rejected before it can exceed the hard query budget;
//   - the rejected broadcast build never enters spill and leaves the CN usable.
//
// The grouped child preserves the incident's build-side Aggregate. The low-NDV
// key keeps the physical topology independent of SQL predicate ordering and
// avoids turning this into a shuffle-spill test.
func TestIssue25782BroadcastHashBuildFailsClosedUnderHardBudget(t *testing.T) {
	cluster, err := embed.StartTestCluster(
		embed.WithCNCount(1),
		embed.WithPreStart(func(service embed.ServiceOperator) {
			if service.ServiceType() != metadata.ServiceType_CN {
				return
			}
			service.Adjust(func(config *embed.ServiceConfig) {
				// Keep the frontend pool comfortably above the test input. The
				// Process limitation is the only pressure source under test.
				config.CN.Frontend.GuestMmuLimitation = 64 << 20
				config.CN.Frontend.ProcessLimitationSize = issue25782QueryBudget
				config.CN.Frontend.ProcessLimitationSpillSize = 16 << 20
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
	execJoinSpillSQL(t, ctx, conn, "set @@max_dop = 2")
	defer resetOptimizerHintsOnCN(t, port)
	execJoinSpillSQL(t, ctx, conn, `set session optimizer_hints = "forceOneCN=1,joinOrdering=1"`)
	// A minimal soft threshold must not turn a shared/broadcast JoinMap into a
	// spill topology. The hard budget, not this threshold, is the safety line.
	execJoinSpillSQL(t, ctx, conn, "set @@join_spill_mem = 1")

	execJoinSpillSQL(t, ctx, conn,
		"create table broadcast_probe (k bigint not null) cluster by k")
	execJoinSpillSQL(t, ctx, conn,
		"create table broadcast_build (k bigint not null, payload bigint not null) cluster by k")
	execJoinSpillSQL(t, ctx, conn, "insert into broadcast_probe values (1)")

	// The outer aggregate is intentionally blocking: the SQL protocol may send
	// SELECT metadata before execution, but it must not publish a result row
	// until every parallel join dependency has reached a terminal state.
	query := `select count(b.payload)
		from broadcast_probe p
		left join (
			select k, payload
			from broadcast_build
			group by k, payload
		) b on p.k = b.k`

	// The control changes only the build cardinality. It establishes the SQL
	// result oracle under the same broadcast topology and hard budget.
	execJoinSpillSQL(t, ctx, conn,
		"insert into broadcast_build select mod(result, 64), result from generate_series(1, 64) g")
	patchJoinSpillStats(t, ctx, conn, dbName, "broadcast_probe", 1)
	patchJoinSpillStatsWithNDV(t, ctx, conn, dbName, "broadcast_build", 64, 64)
	controlPlan := queryJoinSpillText(t, ctx, conn, "explain "+query)
	require.Contains(t, controlPlan, "Aggregate", "the public witness must retain the grouped build child:\n%s", controlPlan)
	require.NotContains(t, controlPlan, "shuffle:", "low-NDV join must remain broadcast:\n%s", controlPlan)
	controlCount, err := func() (_ int64, err error) {
		controlResult, err := conn.QueryContext(ctx, query)
		if err != nil {
			return 0, err
		}
		defer func() {
			err = errors.Join(err, controlResult.Close(), controlResult.Err())
		}()

		var count int64
		rows := 0
		for controlResult.Next() {
			if err = controlResult.Scan(&count); err != nil {
				return 0, err
			}
			rows++
		}
		if rows != 1 {
			return 0, fmt.Errorf("admitted broadcast join returned %d aggregate rows", rows)
		}
		return count, nil
	}()
	require.NoError(t, err)
	require.Equal(t, int64(1), controlCount)

	execJoinSpillSQL(t, ctx, conn, "truncate table broadcast_build")
	execJoinSpillSQL(t, ctx, conn, fmt.Sprintf(
		"insert into broadcast_build select mod(result, 64), result from generate_series(1, %d) g",
		issue25782BuildRows,
	))
	patchJoinSpillStatsWithNDV(t, ctx, conn, dbName, "broadcast_build", issue25782BuildRows, 64)
	plan := queryJoinSpillText(t, ctx, conn, "explain "+query)
	require.Contains(t, plan, "Aggregate", "the rejected plan must retain the grouped build child:\n%s", plan)
	require.NotContains(t, plan, "shuffle:", "low-NDV join must remain broadcast:\n%s", plan)
	// The parallel plan above is the public topology witness. Execute the
	// protocol error check with one consumer: independent parallel pipelines can
	// legitimately stream rows before a sibling pipeline fails, and MySQL has no
	// cross-pipeline result rollback. The HashJoin unit contract separately
	// proves that every consumer of one failed shared JoinMap stops before probe.
	execJoinSpillSQL(t, ctx, conn, "set @@max_dop = 1")

	spillBefore := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"))
	// SELECT metadata is staged before pipeline execution. Depending on packet
	// buffering, the driver can therefore surface the terminal build error from
	// QueryContext or from Rows.Next/Err. The blocking root makes the no-row
	// assertion independent of parallel pipeline scheduling.
	failedResult, err := conn.QueryContext(ctx, query)
	if failedResult != nil {
		func() {
			defer func() {
				err = errors.Join(err, failedResult.Close())
			}()
			require.False(t, failedResult.Next(),
				"failed broadcast build exposed a result row before its terminal error")
			err = errors.Join(err, failedResult.Err())
		}()
	}
	require.Error(t, err)
	var mysqlErr *mysqlDriver.MySQLError
	require.True(t, errors.As(err, &mysqlErr), "expected MySQL protocol error, got %T: %v", err, err)
	require.Equal(t, uint16(moerr.ER_ENGINE_OUT_OF_MEMORY), mysqlErr.Number)
	require.Equal(t, [5]byte{'H', 'Y', '0', '0', '0'}, mysqlErr.SQLState)
	require.ErrorContains(t, err, "resource exhausted: hash build memory budget exceeded")
	require.Equal(t, spillBefore, promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1")),
		"broadcast budget rejection must not create a spill payload")

	var healthy int
	require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&healthy))
	require.Equal(t, 1, healthy)
}
