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
	"slices"
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
	"github.com/stretchr/testify/assert"
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
	updater, ok := clusterservice.GetMOCluster(cnServices[0].ServiceID()).(clusterservice.CNLabelUpdaterWithContext)
	require.True(t, ok)
	views := make([]clusterservice.MOCluster, len(cnServices))
	for i, service := range cnServices {
		views[i] = clusterservice.GetMOCluster(service.ServiceID())
		_, ok := views[i].(clusterservice.AuthoritativeRefresher)
		require.True(t, ok)
	}
	targets := make([]issue27966LabelTarget, len(cnServices))
	for i, service := range cnServices {
		labels := tpLabels
		if service.ServiceID() == apID {
			labels = apLabels
		}
		targets[i] = issue27966LabelTarget{id: service.ServiceID(), labels: labels}
	}
	var invalidation struct {
		sync.Mutex
		armed, done bool
		err         error
	}
	var conn *sql.Conn
	var db *sql.DB
	var dbName string
	// Registered before any mutation: partial/ambiguous updates also need cleanup.
	defer func() {
		cancel()
		invalidation.Lock()
		invalidation.armed = false
		invalidation.Unlock() // joins a callback that is still mutating labels
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		assert.NoError(t, issue27966RestoreLabels(cleanupCtx, updater, views, targets), "restore labels")
		if conn != nil {
			_, cleanupErr := conn.ExecContext(cleanupCtx, "set query_pool_strict = off")
			assert.NoError(t, cleanupErr, "reset strict query pool")
			if dbName != "" {
				_, cleanupErr = conn.ExecContext(cleanupCtx, "drop database if exists `"+dbName+"`")
				assert.NoError(t, cleanupErr, "drop test database")
			}
			assert.NoError(t, conn.Close())
		}
		if db != nil {
			assert.NoError(t, db.Close())
		}
	}()
	phase := func(parent context.Context, changes, expected []issue27966LabelTarget) error {
		phaseCtx, stop := context.WithTimeout(parent, 30*time.Second)
		defer stop()
		if err := issue27966UpdateLabels(phaseCtx, updater, changes); err != nil {
			return err
		}
		return issue27966RefreshLabels(phaseCtx, views, expected)
	}
	require.NoError(t, phase(ctx, targets, targets), "initial labels")

	var observations struct {
		sync.Mutex
		values []compile.QueryScheduleObservation
	}
	observe := func(observation compile.QueryScheduleObservation) {
		observations.Lock()
		observations.values = append(observations.values, observation)
		observations.Unlock()

		if !strings.Contains(observation.SQL, "issue27966_strict_boundary") || !observation.Satisfied {
			return
		}
		invalidation.Lock()
		defer invalidation.Unlock()
		if !invalidation.armed || invalidation.done {
			return
		}
		invalidation.done = true
		invalidTargets := make([]issue27966LabelTarget, len(targets))
		for i, target := range targets {
			invalidTargets[i] = issue27966LabelTarget{id: target.id, labels: apLabels}
		}
		invalidation.err = phase(ctx, invalidTargets[:3], invalidTargets)
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
	db, err = sql.Open("mysql", fmt.Sprintf("dump:111@%s(127.0.0.1:%d)/", dialName, port))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	connectCtx, connectCancel := context.WithTimeout(ctx, 30*time.Second)
	conn, err = db.Conn(connectCtx)
	connectCancel()
	require.NoError(t, err)

	dbName = strings.ToLower(testutils.GetDatabaseName(t))
	execIssue27966SQL(t, ctx, conn, "drop database if exists `"+dbName+"`")
	execIssue27966SQL(t, ctx, conn, "create database `"+dbName+"`")

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

	observations.Lock()
	observations.values = nil
	observations.Unlock()
	invalidation.Lock()
	invalidation.armed = true
	invalidation.Unlock()
	strictQuery := `select /* issue27966_strict_boundary */ id
		from t order by l2_distance(embedding, '[0,0,0,0]') limit 4`
	strictErr := drainIssue27966Query(ctx, conn, strictQuery)
	invalidation.Lock()
	invalidation.armed = false
	invalidated, invalidationErr := invalidation.done, invalidation.err
	invalidation.Unlock()
	require.True(t, invalidated, "the outer frontend placement must run before the pool is invalidated")
	require.NoError(t, invalidationErr)
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

type issue27966LabelTarget struct {
	id     string
	labels map[string][]string
}

func issue27966UpdateLabels(ctx context.Context, updater clusterservice.CNLabelUpdaterWithContext, targets []issue27966LabelTarget) error {
	for _, target := range targets {
		if err := updater.DebugUpdateCNLabelWithContext(ctx, target.id, target.labels); err != nil {
			return fmt.Errorf("update labels for CN %s: %w", target.id, err)
		}
	}
	return nil
}

func issue27966RefreshLabels(ctx context.Context, views []clusterservice.MOCluster, expected []issue27966LabelTarget) error {
	for i, view := range views {
		refresher, ok := view.(clusterservice.AuthoritativeRefresher)
		if !ok {
			return fmt.Errorf("view %d lacks authoritative refresh", i)
		}
		if err := refresher.Refresh(ctx); err != nil {
			return fmt.Errorf("refresh view %d: %w", i, err)
		}
		seen := make(map[string]metadata.CNService)
		view.GetCNServiceWithoutWorkingState(clusterservice.NewSelector(), func(cn metadata.CNService) bool { seen[cn.ServiceID] = cn; return true })
		if len(seen) != len(expected) {
			return fmt.Errorf("view %d: expected %d CNs, got %d", i, len(expected), len(seen))
		}
		for _, target := range expected {
			cn, ok := seen[target.id]
			if !ok {
				return fmt.Errorf("view %d missing CN %s", i, target.id)
			}
			for key, want := range target.labels {
				got, ok := cn.Labels[key]
				if !ok || !slices.Equal(got.Labels, want) {
					return fmt.Errorf("view %d CN %s: unexpected %s labels %v", i, target.id, key, got.Labels)
				}
			}
		}
	}
	return nil
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

func issue27966RestoreLabels(ctx context.Context, updater clusterservice.CNLabelUpdaterWithContext, views []clusterservice.MOCluster, targets []issue27966LabelTarget) error {
	errs := make([]error, 0, len(targets)+1)
	for _, target := range targets {
		errs = append(errs, issue27966UpdateLabels(ctx, updater, []issue27966LabelTarget{target}))
	}
	return errors.Join(append(errs, issue27966RefreshLabels(ctx, views, targets))...)
}

func TestIssue27966LabelPhases(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	targets := []issue27966LabelTarget{
		{id: "tp", labels: map[string][]string{"account": {"tp"}, "role": {"tp"}}},
		{id: "ap", labels: map[string][]string{"account": {"ap"}, "role": {"ap"}}},
	}
	injected := errors.New("injected label RPC failure")
	fake := &issue27966LabelControl{failID: "tp", updateErr: injected}
	err := issue27966UpdateLabels(ctx, fake, targets)
	require.ErrorIs(t, err, injected)
	require.ErrorContains(t, err, "CN tp")
	require.Equal(t, []string{"tp"}, fake.updated, "a normal phase stops at its first error")
	require.Same(t, ctx, fake.ctx)

	// Restoration includes the failed target and continues through later targets and refresh.
	fake.updated = nil
	fake.refreshErr = errors.New("injected refresh failure")
	err = issue27966RestoreLabels(ctx, fake, []clusterservice.MOCluster{fake}, targets)
	require.ErrorIs(t, err, injected)
	require.ErrorIs(t, err, fake.refreshErr)
	require.Equal(t, []string{"tp", "ap"}, fake.updated)
	require.Equal(t, 1, fake.refreshed)
	require.NoError(t, fake.ctx.Err())

	fake.refreshErr = nil
	fake.services = []metadata.CNService{
		{ServiceID: "tp", Labels: map[string]metadata.LabelList{"account": {Labels: []string{"tp"}}, "role": {Labels: []string{"ap"}}}},
		{ServiceID: "ap", Labels: map[string]metadata.LabelList{"account": {Labels: []string{"ap"}}, "role": {Labels: []string{"ap"}}}},
	}
	require.ErrorContains(t, issue27966RefreshLabels(ctx, []clusterservice.MOCluster{fake}, targets), "role")
	fake.services[0].Labels["role"] = metadata.LabelList{Labels: []string{"tp"}}
	require.NoError(t, issue27966RefreshLabels(ctx, []clusterservice.MOCluster{fake}, targets))
	fake.services[0].ServiceID = "unexpected"
	require.ErrorContains(t, issue27966RefreshLabels(ctx, []clusterservice.MOCluster{fake}, targets), "missing CN tp")
}

type issue27966LabelControl struct {
	clusterservice.MOCluster
	failID                string
	updateErr, refreshErr error
	updated               []string
	refreshed             int
	ctx                   context.Context
	services              []metadata.CNService
}

func (c *issue27966LabelControl) DebugUpdateCNLabelWithContext(ctx context.Context, id string, _ map[string][]string) error {
	c.ctx = ctx
	c.updated = append(c.updated, id)
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == c.failID {
		return c.updateErr
	}
	return nil
}
func (c *issue27966LabelControl) Refresh(ctx context.Context) error {
	c.ctx = ctx
	c.refreshed++
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.refreshErr
}
func (c *issue27966LabelControl) GetCNServiceWithoutWorkingState(_ clusterservice.Selector, apply func(metadata.CNService) bool) {
	for _, cn := range c.services {
		if !apply(cn) {
			break
		}
	}
}
