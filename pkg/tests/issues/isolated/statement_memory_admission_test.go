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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Run the actual protocol, frontend generation/transaction boundaries, nested
// SET execution and prepared EXECUTE against the production admission owner.
// Synthetic pressure/queue ordering is covered by the owning-package tests.
func TestStatementMemoryAdmissionSQLLifecycle(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1))
	if cluster != nil {
		t.Cleanup(func() {
			require.NoError(t, cluster.Close())
			require.Eventually(t, func() bool {
				s := system.DefaultMemoryAdmissionBudget().Snapshot()
				return s.StatementActive == 0 && s.StatementQueued == 0
			}, time.Second, 10*time.Millisecond, "request/cluster teardown must not leak admissions")
		})
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	before := system.DefaultMemoryAdmissionBudget().Snapshot().StatementAdmitted
	_, err = conn.ExecContext(ctx, "create database statement_memory_lifecycle; use statement_memory_lifecycle; create table t(id int); insert into t values (1)")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "begin; insert into t values (2); rollback; set @n=(select count(*) from t)")
	require.NoError(t, err)
	var count int
	require.NoError(t, conn.QueryRowContext(ctx, "select @n").Scan(&count))
	require.Equal(t, 1, count)
	stmt, err := conn.PrepareContext(ctx, "select count(*) from t where id > ?")
	require.NoError(t, err)
	defer stmt.Close()
	require.NoError(t, stmt.QueryRowContext(ctx, 0).Scan(&count))
	require.Equal(t, 1, count)
	_, err = conn.ExecContext(ctx, "select missing_column from t")
	require.Error(t, err)
	_, err = conn.ExecContext(ctx, "begin; insert into t values (3); commit")
	require.NoError(t, err)
	require.NoError(t, stmt.QueryRowContext(ctx, 0).Scan(&count))
	require.Equal(t, 2, count)
	_, err = conn.ExecContext(ctx, "drop database statement_memory_lifecycle")
	require.NoError(t, err)
	require.Greater(t, system.DefaultMemoryAdmissionBudget().Snapshot().StatementAdmitted, before)
}

// Cancellation must not queue behind the query whose resources it releases.
// Hold a real authenticated SQL query at an execution barrier, saturate the
// pressure concurrency with accounted entries, then lower the test-owned
// domain's headroom. Never consume the process-wide ledger: compiler/coverage
// memory in a shared cgroup must not change this protocol regression's premise.
func TestStatementMemoryAdmissionKillQueryUnderPressure(t *testing.T) {
	const capacity, healthyUsed = uint64(16 << 30), uint64(1 << 30)
	var used atomic.Uint64
	used.Store(healthyUsed)
	b := system.NewMemoryAdmissionBudgetWithProvider(system.DefaultMemoryDomainReservePolicy(), func() (system.MemoryDomainSnapshot, error) {
		return system.MemoryDomainSnapshot{Used: used.Load(), Limit: capacity, Source: system.MemoryDomainSourceCgroupV2,
			Measured: true, ObservedAt: time.Now()}, nil
	})
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(svc embed.ServiceOperator) {
		if svc.ServiceType() == metadata.ServiceType_CN {
			cleanup, setupErr := process.SetServiceMemoryAdmissionBudgetForTesting(svc.ServiceID(), b)
			require.NoError(t, setupErr)
			t.Cleanup(cleanup) // registered before cluster.Close: removed after shutdown
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, waitSystemBootstrap(ctx, db))
	runner, err := db.Conn(ctx)
	require.NoError(t, err)
	defer runner.Close()
	killer, err := db.Conn(ctx)
	require.NoError(t, err)
	defer killer.Close()
	var connectionID uint64
	require.NoError(t, runner.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
	killSQL := fmt.Sprintf("kill query %d", connectionID)
	_, err = killer.ExecContext(ctx, killSQL)
	require.NoError(t, err, "prewarm the same identity/connection/cancellation path")
	if fault.Enable() {
		defer fault.Disable()
	}
	const barrier, waiters = "memory_admission_kill_running", "memory_admission_kill_waiters"
	require.NoError(t, fault.AddFaultPoint(ctx, barrier, ":::", "WAIT", 0, "", false))
	defer func() { _, _ = fault.RemoveFaultPoint(context.Background(), barrier) }()
	require.NoError(t, fault.AddFaultPoint(ctx, waiters, ":::", "GETWAITERS", 0, barrier, false))
	defer func() { _, _ = fault.RemoveFaultPoint(context.Background(), waiters) }()
	done := make(chan error, 1)
	joined := false
	defer func() {
		if !joined {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			_, _ = killer.ExecContext(cleanupCtx, killSQL)
			_, _ = fault.RemoveFaultPoint(cleanupCtx, barrier)
			select {
			case <-done:
			case <-cleanupCtx.Done():
				t.Error("canceled query did not join")
			}
		}
	}()
	go func() {
		_, queryErr := runner.ExecContext(ctx, "select trigger_fault_point('"+barrier+"')")
		done <- queryErr
	}()
	require.Eventually(t, func() bool {
		n, _, ok := fault.TriggerFault(waiters)
		return ok && n > 0
	}, 20*time.Second, 10*time.Millisecond, "worker must reach execution before pressure is injected")

	// Retain the default policy/concurrency and real frontend/allocator paths,
	// but control only this CN's domain observation, not unrelated test owners.
	for range max(1, system.GoMaxProcs()) {
		_, lease, err := b.EnterStatement(ctx, false)
		require.NoError(t, err)
		defer lease.Close()
	}
	s := b.Snapshot()
	used.Store(capacity - s.Reserve - (8 << 20))
	defer used.Store(healthyUsed)
	s = b.Snapshot()
	require.Less(t, s.Available, s.Reserve)
	require.Positive(t, s.StatementActive)
	t.Logf("pressure admission: available=%d reserve=%d active=%d", s.Available, s.Reserve, s.StatementActive)
	_, err = killer.ExecContext(ctx, "select 1")
	require.ErrorContains(t, err, "memory statement admission wait timed out", "ordinary SQL must still wait under pressure")
	require.ErrorContains(t, err, "control=false", "the MySQL error must preserve the rejecting decision's diagnostics")
	_, err = killer.ExecContext(ctx, killSQL)
	require.NoError(t, err, "KILL must use the bounded control path while the ordinary queue is saturated")
	_, err = fault.RemoveFaultPoint(ctx, barrier)
	require.NoError(t, err)
	select {
	case err = <-done:
		joined = true
	case <-ctx.Done():
		t.Fatal("KILL QUERY did not finish")
	}
	var sqlErr *mysql.MySQLError
	require.ErrorAs(t, err, &sqlErr)
	require.Equal(t, uint16(moerr.ER_QUERY_INTERRUPTED), sqlErr.Number)
	require.Equal(t, "70100", string(sqlErr.SQLState[:]))
	require.Eventually(t, func() bool {
		s := b.Snapshot()
		return s.ControlActive == 0 && s.ControlReserved == 0 && s.StatementQueued == 0
	}, time.Second, 10*time.Millisecond, "cancellation/failed entry must not leak a control slot or queue element")
}
