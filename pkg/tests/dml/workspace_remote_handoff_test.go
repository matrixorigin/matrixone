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

package dml

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test requires an actual remote object adoption receipt, not just an
// INSERT that happened to execute locally on a multi-CN cluster.
func TestWorkspaceRemoteHandoffSQLRollbackAndCommit(t *testing.T) {
	// macOS limits Unix-domain socket paths; t.TempDir includes the full test
	// name and can exceed that limit before the SQL service starts.
	root, err := os.MkdirTemp("/tmp", "ws42-handoff-")
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if closed {
			require.NoError(t, os.RemoveAll(root))
		}
	})
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2), embed.WithDataPath(root))
	if cluster != nil {
		t.Cleanup(func() {
			closeErr := cluster.Close()
			closed = closeErr == nil
			require.NoError(t, closeErr)
		})
	} else {
		// StartTestCluster returns nil only after successful rollback.
		closed = true
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	cn0, err := cluster.GetCNService(0)
	require.NoError(t, err)
	cn1, err := cluster.GetCNService(1)
	require.NoError(t, err)
	view, err := clusterservice.GetMOClusterWithContext(ctx, cn0.ServiceID())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		view.ForceRefresh(false)
		seen := make(map[string]bool)
		view.GetCNServiceWithoutWorkingState(clusterservice.NewSelector(), func(cn metadata.CNService) bool {
			seen[cn.ServiceID] = cn.PipelineServiceAddress != "" && cn.QueryAddress != ""
			return true
		})
		return seen[cn0.ServiceID()] && seen[cn1.ServiceID()]
	}, 30*time.Second, 250*time.Millisecond, "both CNs must be discoverable before the distributed test")
	var placementMu sync.Mutex
	var placements []compile.QueryScheduleObservation
	for _, cn := range []embed.ServiceOperator{cn0, cn1} {
		cleanup := compile.SetQueryScheduleObserverForTesting(cn.ServiceID(), func(observation compile.QueryScheduleObservation) {
			if strings.Contains(observation.SQL, "insert into dst select") {
				placementMu.Lock()
				placements = append(placements, observation)
				placementMu.Unlock()
			}
		})
		defer cleanup()
	}
	remote := cn1.RawService().(cnservice.Service).GetEngine().(*disttae.Engine)
	_, _, priorAdopted, _ := remote.InspectRemoteWorkspaceHandoffsForTest()
	port := cn0.GetServiceConfig().CN.Frontend.Port
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	observer, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
	require.NoError(t, err)
	defer observer.Close()
	exec := func(query string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, query)
		require.NoError(t, err, "query: %s", query)
	}
	exec("create database workspace_handoff_test")
	exec("use workspace_handoff_test")
	exec("create table src (id bigint, v bigint, pad varchar(512))")
	exec("create table dst (id bigint, v bigint, pad varchar(512))")
	exec("create table dst_fail (id bigint, v bigint, pad varchar(512))")
	// Force only this test table to flush small objects. The default writer
	// threshold otherwise keeps this bounded dataset entirely in memory.
	fault.Enable()
	defer fault.Disable()
	require.NoError(t, fault.AddFaultPoint(ctx, objectio.FJ_CNFlushSmallObjs, ":::", "echo", 0, "workspace_handoff_test", false))
	defer fault.RemoveFaultPoint(ctx, objectio.FJ_CNFlushSmallObjs)
	exec("insert into src select result, result * 2, repeat('x', 512) from generate_series(1, 131072) g")
	// Fix the optimizer's source estimate above the distributed INSERT
	// threshold; freshly populated tables may not yet have refreshed stats.
	var patched float64
	require.NoError(t, conn.QueryRowContext(ctx,
		"select table_cnt from table_stats(?, 'patch', ?) g",
		"workspace_handoff_test.src",
		`{"table_cnt":131072,"block_number":16,"accurate_object_number":4,"approx_object_number":4,"ndv_map":{"id":131072}}`,
	).Scan(&patched))
	require.EqualValues(t, 131072, patched)
	oldForce := plan.GetForceScanOnMultiCN()
	plan.SetForceScanOnMultiCN(true)
	defer plan.SetForceScanOnMultiCN(oldForce)
	planRows, err := conn.QueryContext(ctx, "explain insert into dst select id, v, pad from src")
	require.NoError(t, err)
	for planRows.Next() {
		var line string
		require.NoError(t, planRows.Scan(&line))
		t.Log(line)
	}
	require.NoError(t, planRows.Err())
	require.NoError(t, planRows.Close())

	exec("begin")
	exec("insert into dst select id, v, pad from src")
	var count int
	require.NoError(t, observer.QueryRowContext(ctx, "select count(*) from workspace_handoff_test.dst").Scan(&count))
	require.Zero(t, count, "another connection must not see uncommitted rows")
	exec("rollback")
	require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from dst").Scan(&count))
	require.Zero(t, count)
	exec("begin")
	exec("insert into dst select id, v, pad from src")
	exec("commit")
	var sum, padBytes int64
	require.NoError(t, observer.QueryRowContext(ctx, "select count(*), sum(v), sum(length(pad)) from workspace_handoff_test.dst").Scan(&count, &sum, &padBytes))
	require.Equal(t, 131072, count)
	require.EqualValues(t, int64(131072)*131073, sum)
	require.EqualValues(t, int64(131072)*512, padBytes)
	settled := func() bool {
		active, pending, adopted, _ := remote.InspectRemoteWorkspaceHandoffsForTest()
		return active == 0 && pending == 0 && adopted > priorAdopted
	}
	if !assert.Eventually(t, settled, 10*time.Second, 100*time.Millisecond, "remote CN must settle at least one real object handoff") {
		placementMu.Lock()
		for _, p := range placements {
			t.Logf("placement: kind=%v reason=%s selected=%d", p.ExecKind, p.Reason, len(p.SelectedWorkers))
		}
		placementMu.Unlock()
		active, pending, adopted, rejected := remote.InspectRemoteWorkspaceHandoffsForTest()
		t.Logf("remote handoffs: active=%d pending=%d adopted=%d rejected=%d before=%d", active, pending, adopted, rejected, priorAdopted)
		local := cn0.RawService().(cnservice.Service).GetEngine().(*disttae.Engine)
		active, pending, adopted, rejected = local.InspectRemoteWorkspaceHandoffsForTest()
		t.Logf("coordinator handoffs: active=%d pending=%d adopted=%d rejected=%d", active, pending, adopted, rejected)
		t.FailNow()
	}

	// Refuse only a remote object's workspace record after it has been
	// persisted. Local metadata and unrelated databases are unaffected.
	local := cn0.RawService().(cnservice.Service).GetEngine().(*disttae.Engine)
	_, _, _, priorRejected := remote.InspectRemoteWorkspaceHandoffsForTest()
	require.NoError(t, fault.AddFaultPoint(ctx, "fj/cn/workspace_remote_record_refusal", ":::", "echo", 0, "workspace_handoff_test", false))
	defer fault.RemoveFaultPoint(ctx, "fj/cn/workspace_remote_record_refusal")
	exec("begin")
	_, err = conn.ExecContext(ctx, "insert into dst_fail select id, v, pad from src")
	require.ErrorContains(t, err, "allocation account capacity exceeded")
	_, rejectedAtCoordinator := local.InspectRemoteWorkspaceCoordinatorForTest()
	require.Greater(t, rejectedAtCoordinator, 0, "the coordinator must fence a real remote object before rejecting it")
	rejectedIDs := local.RemoteWorkspaceCoordinatorIDsForTest(ioutil.RemoteWorkspaceHandoffRejected)
	if !assert.Eventually(t, func() bool {
		active, pending, _, rejected := remote.InspectRemoteWorkspaceHandoffsForTest()
		if active != pending || rejected <= priorRejected {
			return false
		}
		for _, id := range rejectedIDs {
			if !remote.RemoteWorkspaceRejectedObjectCleanedForTest(id) {
				return false
			}
		}
		return true
	}, 10*time.Second, 100*time.Millisecond, "each fenced rejection must settle, and all producers must stop") {
		active, pending, adopted, rejected := remote.InspectRemoteWorkspaceHandoffsForTest()
		t.Logf("after refusal: active=%d pending=%d adopted=%d rejected=%d before=%d coordinator-rejected=%d", active, pending, adopted, rejected, priorRejected, rejectedAtCoordinator)
		t.FailNow()
	}
	_, pendingUnknown, _, _ := remote.InspectRemoteWorkspaceHandoffsForTest()
	t.Logf("ambiguous object obligations retained after cancellation: %d", pendingUnknown)
	_, err = fault.RemoveFaultPoint(ctx, "fj/cn/workspace_remote_record_refusal")
	require.NoError(t, err)
	exec("insert into dst_fail values (1, 2, 'small')")
	exec("commit")
	require.NoError(t, observer.QueryRowContext(ctx, "select count(*), sum(v) from workspace_handoff_test.dst_fail").Scan(&count, &sum))
	require.Equal(t, 1, count)
	require.EqualValues(t, 2, sum)
}
