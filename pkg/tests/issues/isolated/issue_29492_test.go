// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// Full-channel causality is proved in the component tests. This public-path
// control verifies three-CN cancellation and remote-stream quiescence. Its
// separate successful spill control does not claim that cancellation reaches
// spill: a SQL fault barrier makes the cancellation phase deterministic.
func TestIssue29492MultiCNCancellationCleanup(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(3))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	var dbs []*sql.DB
	for i := 0; i < 3; i++ {
		cn, err := cluster.GetCNService(i)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		require.NoError(t, waitSystemBootstrap(ctx, db))
		dbs = append(dbs, db)
	}
	runner, err := dbs[0].Conn(ctx)
	require.NoError(t, err)
	defer runner.Close()
	exec := func(query string) {
		t.Helper()
		_, err := runner.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	const dbName = "issue29492_cancel"
	exec("create database " + dbName)
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_, err := dbs[0].ExecContext(cleanupCtx, "drop database "+dbName)
		require.NoError(t, err)
	}()
	exec("use " + dbName)
	exec("create table t(v bigint)")
	// Three persisted blocks give remote scan placement actual work.
	exec("insert into t select result from generate_series(1,24576) g")
	exec("select mo_ctl('dn','flush','" + dbName + ".t')")
	oldForce := plan.GetForceScanOnMultiCN()
	plan.SetForceScanOnMultiCN(true)
	defer plan.SetForceScanOnMultiCN(oldForce)
	exec("set session agg_spill_mem=1024")
	const percentile = "select percentile_cont(0.5) within group (order by v), percentile_disc(0.5) within group (order by v) from " + dbName + ".t"
	var cont float64
	var disc int64
	require.NoError(t, runner.QueryRowContext(ctx, percentile).Scan(&cont, &disc))
	require.Equal(t, 12288.5, cont)
	require.Equal(t, int64(12288), disc)
	func() {
		rows, err := runner.QueryContext(ctx, "explain analyze "+percentile)
		require.NoError(t, err)
		defer rows.Close()
		var explain strings.Builder
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			explain.WriteString(line)
			explain.WriteByte('\n')
		}
		require.NoError(t, rows.Err())
		require.Regexp(t, `SpillRows=[1-9][0-9]*`, explain.String())
		t.Log(explain.String())
	}()
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(metricv2.PipelineMessageSenderGauge) == 0 &&
			promtestutil.ToFloat64(metricv2.PipelineStreamLifecycleGauge) == 0
	}, 15*time.Second, 10*time.Millisecond, "spill control must release remote streams")
	keys := []string{"connector_cleanup_send_terminal_signal", "dispatch_cleanup_send_terminal_signal", "remote_notify_cleanup_send_terminal_signal"}
	before := make([]float64, len(keys))
	for i, key := range keys {
		before[i] = promtestutil.ToFloat64(metricv2.PipelineCleanupEventCounter.WithLabelValues(key))
	}
	if fault.Enable() {
		defer fault.Disable()
	}
	const barrier = "issue29492_running"
	const waiters = "issue29492_waiters"
	require.NoError(t, fault.AddFaultPoint(ctx, barrier, ":::", "wait", 0, "", false))
	defer func() { _, _ = fault.RemoveFaultPoint(context.Background(), barrier) }()
	require.NoError(t, fault.AddFaultPoint(ctx, waiters, ":::", "getwaiters", 0, barrier, false))
	defer func() { _, _ = fault.RemoveFaultPoint(context.Background(), waiters) }()
	var connectionID uint64
	require.NoError(t, runner.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
	done := make(chan error, 1)
	joined := false
	defer func() {
		if joined {
			return
		}
		cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_, _ = dbs[0].ExecContext(cleanupCtx, fmt.Sprintf("kill query %d", connectionID))
		_, _ = fault.RemoveFaultPoint(cleanupCtx, barrier)
		select {
		case <-done:
		case <-cleanupCtx.Done():
			t.Error("canceled query did not join")
		}
	}()
	go func() {
		_, err := runner.ExecContext(ctx, "select percentile_cont(0.5) within group (order by v + trigger_fault_point('"+barrier+"')) from "+dbName+".t")
		done <- err
	}()
	require.Eventually(t, func() bool {
		count, _, ok := fault.TriggerFault(waiters)
		return ok && count > 0 && promtestutil.ToFloat64(metricv2.PipelineMessageSenderGauge) > 0
	}, 20*time.Second, 10*time.Millisecond, "query must enter execution with live remote senders")
	_, err = dbs[0].ExecContext(ctx, fmt.Sprintf("kill query %d", connectionID))
	require.NoError(t, err)
	// The SQL fault WAIT is not context-aware; release only after server KILL.
	_, err = fault.RemoveFaultPoint(ctx, barrier)
	require.NoError(t, err)
	select {
	case err = <-done:
		joined = true
	case <-ctx.Done():
		t.Fatal("KILL QUERY did not finish")
	}
	require.ErrorContains(t, err, "context canceled")
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(metricv2.PipelineMessageSenderGauge) == 0 &&
			promtestutil.ToFloat64(metricv2.PipelineStreamLifecycleGauge) == 0
	}, 15*time.Second, 10*time.Millisecond, "all CN remote stream owners must finish")
	for _, db := range dbs {
		require.NoError(t, db.QueryRowContext(ctx, percentile).Scan(&cont, &disc))
		require.Equal(t, 12288.5, cont)
		require.Equal(t, int64(12288), disc)
	}
	for i, key := range keys {
		require.Equal(t, before[i], promtestutil.ToFloat64(metricv2.PipelineCleanupEventCounter.WithLabelValues(key)), key)
	}
}
