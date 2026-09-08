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
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	proxypb "github.com/matrixorigin/matrixone/pkg/pb/proxy"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

var issue27966DialSequence atomic.Uint64

func TestIssue27966FrontendIVFInternalSQLPreservesQueryPool(t *testing.T) {
	cluster, err := embed.StartTestCluster(
		embed.WithCNCount(4),
		embed.WithPreStart(func(service embed.ServiceOperator) {
			if service.ServiceType() != metadata.ServiceType_CN ||
				!strings.HasPrefix(service.ServiceID(), "2-cn-") {
				return
			}
			service.Adjust(func(config *embed.ServiceConfig) {
				// The production label selector is delivered by the proxy preface.
				// Keep the extra-info read disabled on non-ingress CNs so their
				// internal task clients retain the normal direct handshake.
				config.CN.Frontend.ProxyEnabled = true
			})
		}),
	)
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cnServices := make([]embed.ServiceOperator, 4)
	for i := range cnServices {
		cnServices[i], err = cluster.GetCNService(i)
		require.NoError(t, err)
	}
	tpIDs := map[string]struct{}{
		cnServices[0].ServiceID(): {},
		cnServices[1].ServiceID(): {},
		cnServices[2].ServiceID(): {},
	}
	ingressID := cnServices[2].ServiceID()
	apID := cnServices[3].ServiceID()
	tpLabels := map[string][]string{"account": {"tp"}, "role": {"tp"}}
	apLabels := map[string][]string{"account": {"ap"}, "role": {"ap"}}
	setLabels := func(labels map[string][]string) error {
		control := clusterservice.GetMOCluster(cnServices[0].ServiceID())
		for id := range tpIDs {
			if updateErr := control.DebugUpdateCNLabel(id, labels); updateErr != nil {
				return updateErr
			}
		}
		if updateErr := control.DebugUpdateCNLabel(apID, apLabels); updateErr != nil {
			return updateErr
		}
		for _, service := range cnServices {
			clusterservice.GetMOCluster(service.ServiceID()).ForceRefresh(true)
		}
		return nil
	}
	require.NoError(t, setLabels(tpLabels))
	require.Eventually(t, func() bool {
		for _, service := range cnServices {
			mc := clusterservice.GetMOCluster(service.ServiceID())
			mc.ForceRefresh(true)
			seen := 0
			matched := true
			mc.GetCNServiceWithoutWorkingState(clusterservice.NewSelector(), func(cn metadata.CNService) bool {
				seen++
				want := "ap"
				if _, ok := tpIDs[cn.ServiceID]; ok {
					want = "tp"
				}
				account, ok := cn.Labels["account"]
				matched = matched && ok && len(account.Labels) == 1 && account.Labels[0] == want
				return matched
			})
			if !matched || seen != len(cnServices) {
				return false
			}
		}
		return true
	}, 20*time.Second, 100*time.Millisecond)

	var observations struct {
		sync.Mutex
		values []compile.QueryScheduleObservation
	}
	var invalidateAfterOuter atomic.Bool
	var invalidated atomic.Bool
	var invalidationErr struct {
		sync.Mutex
		err error
	}
	observe := func(observation compile.QueryScheduleObservation) {
		observations.Lock()
		observations.values = append(observations.values, observation)
		observations.Unlock()

		if !invalidateAfterOuter.Load() ||
			!strings.Contains(observation.SQL, "issue27966_strict_boundary") ||
			!observation.Satisfied ||
			!invalidated.CompareAndSwap(false, true) {
			return
		}
		if updateErr := setLabels(apLabels); updateErr != nil {
			invalidationErr.Lock()
			invalidationErr.err = updateErr
			invalidationErr.Unlock()
		}
	}
	for _, service := range cnServices {
		cleanup := compile.SetQueryScheduleObserverForTesting(service.ServiceID(), observe)
		t.Cleanup(cleanup)
	}

	port := cnServices[2].GetServiceConfig().CN.Frontend.Port
	dialName := fmt.Sprintf("issue27966_%d", issue27966DialSequence.Add(1))
	mysqlDriver.RegisterDialContext(dialName, func(dialCtx context.Context, address string) (net.Conn, error) {
		conn, dialErr := (&net.Dialer{}).DialContext(dialCtx, "tcp", address)
		if dialErr != nil {
			return nil, dialErr
		}
		preface, encodeErr := (&proxypb.ExtraInfo{
			Salt:         []byte("issue27966-salt-1234"),
			ConnectionID: 27966,
			Label:        map[string]string{"account": "tp", "role": "tp"},
			ClientAddr:   "issue27966-test",
		}).Encode()
		if encodeErr != nil {
			_ = conn.Close()
			return nil, encodeErr
		}
		for len(preface) > 0 {
			written, writeErr := conn.Write(preface)
			if writeErr != nil {
				_ = conn.Close()
				return nil, writeErr
			}
			preface = preface[written:]
		}
		return conn, nil
	})
	t.Cleanup(func() { mysqlDriver.DeregisterDialContext(dialName) })
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@%s(127.0.0.1:%d)/", dialName, port))
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	connectCtx, connectCancel := context.WithTimeout(ctx, 30*time.Second)
	conn, err := db.Conn(connectCtx)
	connectCancel()
	require.NoError(t, err)
	defer conn.Close()

	dbName := strings.ToLower(testutils.GetDatabaseName(t))
	execIssue27966SQL(t, ctx, conn, "drop database if exists `"+dbName+"`")
	execIssue27966SQL(t, ctx, conn, "create database `"+dbName+"`")
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_ = setLabels(tpLabels)
		_, _ = conn.ExecContext(cleanupCtx, "set query_pool_strict = off")
		_, _ = conn.ExecContext(cleanupCtx, "drop database if exists `"+dbName+"`")
	}()
	execIssue27966SQL(t, ctx, conn, "use `"+dbName+"`")
	execIssue27966SQL(t, ctx, conn, "set ivf_preload_entries = 0")
	execIssue27966SQL(t, ctx, conn, "set probe_limit = 4")
	execIssue27966SQL(t, ctx, conn, "create table t (id bigint primary key, embedding vecf32(4))")
	execIssue27966SQL(t, ctx, conn, `insert into t values
		(1, '[0,0,0,0]'), (2, '[1,0,0,0]'), (3, '[2,0,0,0]'), (4, '[3,0,0,0]'),
		(5, '[4,0,0,0]'), (6, '[5,0,0,0]'), (7, '[6,0,0,0]'), (8, '[7,0,0,0]')`)
	execIssue27966SQL(t, ctx, conn,
		"create index idx_t_embedding using ivfflat on t(embedding) lists=4 op_type 'vector_l2_ops'")
	execIssue27966SQL(t, ctx, conn, "set query_pool_strict = on")
	execIssue27966SQL(t, ctx, conn, "set query_max_workers = 2")

	planText := queryIssue27966Text(t, ctx, conn,
		"explain select id from t order by l2_distance(embedding, '[0,0,0,0]') limit 4")
	require.Contains(t, strings.ToLower(planText), "table function on ivf_search")

	observations.Lock()
	observations.values = nil
	observations.Unlock()

	capQuery := `select /* issue27966_worker_cap */ id
		from t order by l2_distance(embedding, '[0,0,0,0]') limit 4`
	ids, err := queryIssue27966IDs(ctx, conn, capQuery)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4}, ids)

	observations.Lock()
	capObservations := append([]compile.QueryScheduleObservation(nil), observations.values...)
	observations.Unlock()
	outer := findIssue27966Observation(capObservations, "issue27966_worker_cap", true)
	require.NotNil(t, outer)
	assertIssue27966TPWorkerCap(t, *outer, tpIDs)
	require.Equal(t, ingressID, outer.ServiceID)

	var entriesObservations []compile.QueryScheduleObservation
	for _, observation := range capObservations {
		lowerSQL := strings.ToLower(observation.SQL)
		if strings.Contains(lowerSQL, "__mo_index_secondary_") && strings.Contains(lowerSQL, "vec_dist") {
			entriesObservations = append(entriesObservations, observation)
		}
	}
	require.NotEmpty(t, entriesObservations, "the public query must generate and compile IVF entries SQL")
	for _, observation := range entriesObservations {
		assertIssue27966TPWorkerCap(t, observation, tpIDs)
	}

	require.NoError(t, setLabels(tpLabels))
	observations.Lock()
	observations.values = nil
	observations.Unlock()
	invalidated.Store(false)
	invalidateAfterOuter.Store(true)
	strictQuery := `select /* issue27966_strict_boundary */ id
		from t order by l2_distance(embedding, '[0,0,0,0]') limit 4`
	strictErr := drainIssue27966Query(ctx, conn, strictQuery)
	invalidateAfterOuter.Store(false)
	require.True(t, invalidated.Load(), "the outer frontend placement must run before the pool is invalidated")
	invalidationErr.Lock()
	require.NoError(t, invalidationErr.err)
	invalidationErr.Unlock()
	require.Error(t, strictErr)
	require.Contains(t, strictErr.Error(), schedule.ReasonNoCandidateCN)

	observations.Lock()
	strictObservations := append([]compile.QueryScheduleObservation(nil), observations.values...)
	observations.Unlock()
	strictOuter := findIssue27966Observation(strictObservations, "issue27966_strict_boundary", true)
	require.NotNil(t, strictOuter)
	assertIssue27966TPWorkerCap(t, *strictOuter, tpIDs)

	var nestedFailure *compile.QueryScheduleObservation
	for i := range strictObservations {
		observation := &strictObservations[i]
		lowerSQL := strings.ToLower(observation.SQL)
		if _, ok := tpIDs[observation.ServiceID]; ok &&
			strings.Contains(lowerSQL, "__mo_index_secondary_") && strings.Contains(lowerSQL, "vec_dist") &&
			!observation.Satisfied && observation.Reason == schedule.ReasonNoCandidateCN {
			nestedFailure = observation
			break
		}
	}
	require.NotNil(t, nestedFailure,
		"the generated IVF background SQL must fail at real candidate resolution after the TP pool disappears")
	require.Equal(t, strictOuter.RequestedPool, nestedFailure.RequestedPool)
	require.Equal(t, strictOuter.ResolvedPool, nestedFailure.ResolvedPool)
	require.Equal(t, schedule.PoolFallbackStrict, nestedFailure.PoolFallbackPolicy)
	require.Equal(t, schedule.EmptyWorkerFail, nestedFailure.EmptyWorkerPolicy)
	require.Equal(t, schedule.WorkerSetMax, nestedFailure.WorkerSetMode)
	require.Equal(t, 2, nestedFailure.MaxWorkers)
	require.Empty(t, nestedFailure.SelectedWorkers)
}

func queryIssue27966IDs(ctx context.Context, conn *sql.Conn, statement string) (ids []int64, err error) {
	rows, err := conn.QueryContext(ctx, statement)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func drainIssue27966Query(ctx context.Context, conn *sql.Conn, statement string) (err error) {
	rows, err := conn.QueryContext(ctx, statement)
	if rows == nil {
		return err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	for rows.Next() {
	}
	return errors.Join(err, rows.Err())
}

func execIssue27966SQL(t *testing.T, ctx context.Context, conn *sql.Conn, statement string) {
	t.Helper()
	_, err := conn.ExecContext(ctx, statement)
	require.NoError(t, err, statement)
}

func queryIssue27966Text(t *testing.T, ctx context.Context, conn *sql.Conn, statement string) string {
	t.Helper()
	rows, err := conn.QueryContext(ctx, statement)
	require.NoError(t, err, statement)
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(t, rows.Err())
	return strings.Join(lines, "\n")
}

func findIssue27966Observation(
	observations []compile.QueryScheduleObservation,
	marker string,
	satisfied bool,
) *compile.QueryScheduleObservation {
	for i := range observations {
		if strings.Contains(observations[i].SQL, marker) && observations[i].Satisfied == satisfied {
			return &observations[i]
		}
	}
	return nil
}

func assertIssue27966TPWorkerCap(
	t *testing.T,
	observation compile.QueryScheduleObservation,
	tpIDs map[string]struct{},
) {
	t.Helper()
	require.True(t, observation.Satisfied)
	require.Equal(t, schedule.QueryExecAPMultiCN, observation.ExecKind)
	require.False(t, observation.PoolFallback)
	require.Equal(t, schedule.PoolFallbackStrict, observation.PoolFallbackPolicy)
	require.Equal(t, schedule.EmptyWorkerFail, observation.EmptyWorkerPolicy)
	require.Equal(t, schedule.WorkerSetMax, observation.WorkerSetMode)
	require.Equal(t, 2, observation.MaxWorkers)
	require.Len(t, observation.SelectedWorkers, 2)
	remoteSelected := false
	for _, worker := range observation.SelectedWorkers {
		_, ok := tpIDs[worker.ID]
		require.True(t, ok, "selected non-TP CN %q", worker.ID)
		remoteSelected = remoteSelected || worker.Route == schedule.WorkerRouteRemote
	}
	require.True(t, remoteSelected, "AP multi-CN placement must select a RemoteRun worker")
}
