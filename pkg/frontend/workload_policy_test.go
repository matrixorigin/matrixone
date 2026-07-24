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

package frontend

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	mysqlparser "github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const (
	testWorkloadPolicyOld        = `{"version":1,"policies":{"ap":{"pool":"old","labels":{"role":"ap"}}}}`
	testWorkloadPolicyNew        = `{"version":1,"policies":{"ap":{"pool":"new","labels":{"role":"ap"}}}}`
	testWorkloadPolicyVersionSQL = "select version, version_offset from mo_catalog.mo_version " +
		"where state = 2 order by create_at desc limit 1"
)

var benchmarkWorkloadPolicySnapshot schedule.WorkloadPolicySet

type workloadPolicyContextExec struct {
	*backgroundExecTest
	contexts []context.Context
	closes   int
}

type workloadPolicyInjectExec struct {
	*backgroundExecTest
	failPrefix string
	failErr    error
}

type workloadPolicySQLExecutor struct {
	exec func(
		context.Context,
		string,
		executor.Options,
	) (executor.Result, error)
}

type workloadPolicyQueryClient struct {
	serviceID string

	mu                sync.Mutex
	responses         map[string]*query.Response
	sendErrs          map[string]error
	methodSendErrs    map[query.CmdMethod]map[string]error
	protocolVersions  map[string]int64
	protocolResponses map[string]*query.Response
	requests          map[string]*query.Request
	releases          int
}

type workloadPolicyMOCluster struct {
	clusterservice.MOCluster
	services []metadata.CNService
}

func (c *workloadPolicyMOCluster) GetCNService(
	_ clusterservice.Selector,
	apply func(metadata.CNService) bool,
) {
	for _, service := range c.services {
		if !apply(service) {
			return
		}
	}
}

func (c *workloadPolicyMOCluster) GetCNServiceWithoutWorkingState(
	selector clusterservice.Selector,
	apply func(metadata.CNService) bool,
) {
	c.GetCNService(selector, apply)
}

func newWorkloadPolicyQueryClient(serviceID string) *workloadPolicyQueryClient {
	return &workloadPolicyQueryClient{
		serviceID:         serviceID,
		responses:         make(map[string]*query.Response),
		sendErrs:          make(map[string]error),
		methodSendErrs:    make(map[query.CmdMethod]map[string]error),
		protocolVersions:  make(map[string]int64),
		protocolResponses: make(map[string]*query.Response),
		requests:          make(map[string]*query.Request),
	}
}

func setupWorkloadPolicyQueryCluster(
	t *testing.T,
	service string,
	services []metadata.CNService,
) *workloadPolicyQueryClient {
	t.Helper()
	client := newWorkloadPolicyQueryClient(service)
	InitServerLevelVars(service)
	setPu(service, &config.ParameterUnit{QueryClient: client})
	cluster := &workloadPolicyMOCluster{services: services}
	rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
	moruntime.SetupServiceBasedRuntime(service, rt)
	rt.SetGlobalVariables(moruntime.ClusterService, cluster)
	return client
}

func (c *workloadPolicyQueryClient) ServiceID() string {
	return c.serviceID
}

func (c *workloadPolicyQueryClient) SendMessage(
	_ context.Context,
	address string,
	request *query.Request,
) (*query.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests[address] = request
	if err := c.sendErrs[address]; err != nil {
		return nil, err
	}
	if errs := c.methodSendErrs[request.CmdMethod]; errs != nil {
		if err := errs[address]; err != nil {
			return nil, err
		}
	}
	if request.CmdMethod == query.CmdMethod_GetProtocolVersion {
		if response, ok := c.protocolResponses[address]; ok {
			return response, nil
		}
		version, ok := c.protocolVersions[address]
		if !ok {
			version = defines.MORPCLatestVersion
		}
		return &query.Response{
			GetProtocolVersion: &query.GetProtocolVersionResponse{
				Version: version,
			},
		}, nil
	}
	return c.responses[address], nil
}

func (c *workloadPolicyQueryClient) NewRequest(
	method query.CmdMethod,
) *query.Request {
	return &query.Request{CmdMethod: method}
}

func (c *workloadPolicyQueryClient) Release(*query.Response) {
	c.mu.Lock()
	c.releases++
	c.mu.Unlock()
}

func (c *workloadPolicyQueryClient) Close() error {
	return nil
}

func (e *workloadPolicySQLExecutor) Exec(
	ctx context.Context,
	sql string,
	opts executor.Options,
) (executor.Result, error) {
	return e.exec(ctx, sql, opts)
}

func (e *workloadPolicySQLExecutor) ExecTxn(
	context.Context,
	func(executor.TxnExecutor) error,
	executor.Options,
) error {
	return moerr.NewInternalErrorNoCtx("unexpected transaction")
}

func (e *workloadPolicyContextExec) Exec(ctx context.Context, sql string) error {
	e.contexts = append(e.contexts, ctx)
	return e.backgroundExecTest.Exec(ctx, sql)
}

func (e *workloadPolicyContextExec) Close() {
	e.closes++
}

func (e *workloadPolicyInjectExec) Exec(
	ctx context.Context,
	sql string,
) error {
	if strings.HasPrefix(sql, e.failPrefix) {
		e.currentSql = sql
		e.executedSQLs = append(e.executedSQLs, sql)
		return e.failErr
	}
	return e.backgroundExecTest.Exec(ctx, sql)
}

func newWorkloadPolicyCatalogResult(rows [][]interface{}) *MysqlResultSet {
	result := &MysqlResultSet{}
	policyColumn := &MysqlColumn{}
	policyColumn.SetName("policy")
	policyColumn.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	result.AddColumn(policyColumn)
	revisionColumn := &MysqlColumn{}
	revisionColumn.SetName("revision")
	revisionColumn.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	result.AddColumn(revisionColumn)
	for _, row := range rows {
		result.AddRow(row)
	}
	return result
}

func newWorkloadPolicyVersionResult(
	version interface{},
	offset interface{},
) *MysqlResultSet {
	result := &MysqlResultSet{}
	versionColumn := &MysqlColumn{}
	versionColumn.SetName("version")
	versionColumn.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	result.AddColumn(versionColumn)
	offsetColumn := &MysqlColumn{}
	offsetColumn.SetName("version_offset")
	offsetColumn.SetColumnType(defines.MYSQL_TYPE_LONG)
	result.AddColumn(offsetColumn)
	result.AddRow([]interface{}{version, offset})
	return result
}

func TestWorkloadPolicyApplyIsRevisionOrderedAndAccountScoped(t *testing.T) {
	const accountID = uint32(42)
	manager := newWorkloadPolicyManager(time.Minute, nil)
	state := manager.acquire(accountID)

	applied, revision, err := manager.Apply(accountID, testWorkloadPolicyNew, 20)
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, uint64(20), revision)
	require.Equal(
		t,
		"new",
		manager.cached(state).Rules[schedule.WorkloadAP].PoolIdentity,
	)

	applied, revision, err = manager.Apply(accountID, testWorkloadPolicyOld, 10)
	require.NoError(t, err)
	require.False(t, applied)
	require.Equal(t, uint64(20), revision)

	applied, revision, err = manager.Apply(accountID, testWorkloadPolicyNew, 20)
	require.NoError(t, err)
	require.False(t, applied)
	require.Equal(t, uint64(20), revision)

	_, revision, err = manager.Apply(accountID, testWorkloadPolicyOld, 20)
	require.ErrorContains(t, err, "conflicting workload policies")
	require.Equal(t, uint64(20), revision)

	applied, revision, err = manager.Apply(accountID+1, testWorkloadPolicyOld, 1)
	require.NoError(t, err)
	require.False(t, applied)
	require.Zero(t, revision)
	_, loaded := manager.accounts.Load(accountID + 1)
	require.False(t, loaded)
}

func TestWorkloadPolicyCacheIsReleasedWithLastSessionReference(t *testing.T) {
	const accountID = uint32(51)
	manager := newWorkloadPolicyManager(time.Minute, nil)
	first := manager.acquire(accountID)
	second := manager.acquire(accountID)
	require.Same(t, first, second)

	manager.release(first)
	_, loaded := manager.accounts.Load(accountID)
	require.True(t, loaded)

	manager.release(second)
	_, loaded = manager.accounts.Load(accountID)
	require.False(t, loaded)

	reopened := manager.acquire(accountID)
	require.NotSame(t, first, reopened)
	manager.release(reopened)
}

func TestWorkloadPolicyRefreshDoesNotResurrectReleasedAccount(t *testing.T) {
	const accountID = uint32(53)
	started := make(chan struct{})
	releaseLoad := make(chan struct{})
	manager := newWorkloadPolicyManager(
		time.Minute,
		func(context.Context, FeSession, uint32) (string, uint64, error) {
			close(started)
			<-releaseLoad
			return testWorkloadPolicyNew, 1, nil
		},
	)
	state := manager.acquire(accountID)
	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- manager.Refresh(
			context.Background(),
			&Session{},
			accountID,
			state,
		)
	}()
	<-started

	manager.release(state)
	_, loaded := manager.accounts.Load(accountID)
	require.False(t, loaded)
	close(releaseLoad)
	require.NoError(t, <-refreshDone)
	require.Nil(t, state.snapshot.Load())

	reopened := manager.acquire(accountID)
	require.NotSame(t, state, reopened)
	manager.release(reopened)
}

func TestWorkloadPolicyRefreshRejectsMismatchedAccountState(t *testing.T) {
	const accountID = uint32(58)
	var loads atomic.Int32
	manager := newWorkloadPolicyManager(
		time.Minute,
		func(context.Context, FeSession, uint32) (string, uint64, error) {
			loads.Add(1)
			return testWorkloadPolicyNew, 1, nil
		},
	)
	state := manager.acquire(accountID)
	defer manager.release(state)

	err := manager.Refresh(
		context.Background(),
		&Session{},
		accountID+1,
		state,
	)
	require.ErrorContains(t, err, "belongs to account 58, not account 59")
	require.Zero(t, loads.Load())
	require.Nil(t, state.snapshot.Load())
	require.Nil(t, state.refreshing)
}

func TestWorkloadPolicyCatalogRefreshCannotOverwriteNewerRPC(t *testing.T) {
	const accountID = uint32(43)
	started := make(chan struct{})
	release := make(chan struct{})
	manager := newWorkloadPolicyManager(
		time.Minute,
		func(context.Context, FeSession, uint32) (string, uint64, error) {
			close(started)
			<-release
			return testWorkloadPolicyOld, 1, nil
		},
	)
	state := manager.acquire(accountID)
	applied, _, err := manager.Apply(accountID, testWorkloadPolicyOld, 1)
	require.NoError(t, err)
	require.True(t, applied)
	state.refreshAfter.Store(0)

	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- manager.Refresh(
			context.Background(),
			&Session{},
			accountID,
			state,
		)
	}()
	<-started

	applied, revision, err := manager.Apply(accountID, testWorkloadPolicyNew, 2)
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, uint64(2), revision)
	close(release)
	require.NoError(t, <-refreshDone)

	snapshot := state.snapshot.Load()
	require.Equal(t, uint64(2), snapshot.revision)
	require.Equal(t, testWorkloadPolicyNew, snapshot.raw)
}

func TestWorkloadPolicyConcurrentRPCSupersedesFailedRefresh(t *testing.T) {
	const accountID = uint32(56)
	started := make(chan struct{})
	release := make(chan struct{})
	manager := newWorkloadPolicyManager(
		time.Minute,
		func(context.Context, FeSession, uint32) (string, uint64, error) {
			close(started)
			<-release
			return "", 0, moerr.NewInternalErrorNoCtx("stale read failed")
		},
	)
	state := manager.acquire(accountID)
	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- manager.Refresh(
			context.Background(),
			&Session{},
			accountID,
			state,
		)
	}()
	<-started

	applied, revision, err := manager.Apply(
		accountID,
		testWorkloadPolicyNew,
		2,
	)
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, uint64(2), revision)
	rpcRefreshAfter := state.refreshAfter.Load()
	close(release)

	require.NoError(t, <-refreshDone)
	require.Zero(t, state.failures)
	require.Equal(t, rpcRefreshAfter, state.refreshAfter.Load())
	require.Equal(t, uint64(2), state.snapshot.Load().revision)
	require.Equal(t, testWorkloadPolicyNew, state.snapshot.Load().raw)
}

func TestWorkloadPolicyInitialRefreshIsSingleflightAndCancellable(t *testing.T) {
	const accountID = uint32(44)
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	manager := newWorkloadPolicyManager(
		time.Minute,
		func(context.Context, FeSession, uint32) (string, uint64, error) {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-release
			return testWorkloadPolicyNew, 3, nil
		},
	)
	state := manager.acquire(accountID)

	const goroutines = 16
	results := make(chan error, goroutines)
	var waitGroup sync.WaitGroup
	for range goroutines {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			results <- manager.Refresh(
				context.Background(),
				&Session{},
				accountID,
				state,
			)
		}()
	}
	<-started

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(
		t,
		manager.Refresh(cancelledCtx, &Session{}, accountID, state),
		context.Canceled,
	)

	close(release)
	waitGroup.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, uint64(3), state.snapshot.Load().revision)
}

func TestWorkloadPolicyStaleSnapshotDoesNotQueueBehindRefresh(t *testing.T) {
	const accountID = uint32(45)
	started := make(chan struct{})
	release := make(chan struct{})
	manager := newWorkloadPolicyManager(
		time.Minute,
		func(context.Context, FeSession, uint32) (string, uint64, error) {
			close(started)
			<-release
			return testWorkloadPolicyNew, 2, nil
		},
	)
	state := manager.acquire(accountID)
	applied, _, err := manager.Apply(accountID, testWorkloadPolicyOld, 1)
	require.NoError(t, err)
	require.True(t, applied)
	state.refreshAfter.Store(0)

	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- manager.Refresh(
			context.Background(),
			&Session{},
			accountID,
			state,
		)
	}()
	<-started

	start := time.Now()
	require.NoError(t, manager.Refresh(
		context.Background(),
		&Session{},
		accountID,
		state,
	))
	require.Less(t, time.Since(start), 100*time.Millisecond)
	require.Equal(
		t,
		"old",
		manager.cached(state).Rules[schedule.WorkloadAP].PoolIdentity,
	)

	close(release)
	require.NoError(t, <-refreshDone)
	require.Equal(
		t,
		"new",
		manager.cached(state).Rules[schedule.WorkloadAP].PoolIdentity,
	)
}

func TestWorkloadPolicyEstablishedRefreshIsDetachedAndSingleflight(t *testing.T) {
	const accountID = uint32(57)
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	manager := newWorkloadPolicyManager(time.Minute, nil)
	state := manager.acquire(accountID)
	applied, _, err := manager.Apply(accountID, testWorkloadPolicyOld, 1)
	require.NoError(t, err)
	require.True(t, applied)
	state.refreshAfter.Store(0)

	statementCtx, cancelStatement := context.WithCancel(context.Background())
	start := time.Now()
	require.NoError(t, manager.refreshWithMode(
		statementCtx,
		accountID,
		state,
		func(ctx context.Context) (string, uint64, error) {
			if calls.Add(1) == 1 {
				close(started)
			}
			select {
			case <-release:
				return testWorkloadPolicyNew, 2, nil
			case <-ctx.Done():
				return "", 0, context.Cause(ctx)
			}
		},
		true,
	))
	require.Less(t, time.Since(start), 100*time.Millisecond)
	cancelStatement()
	<-started

	require.NoError(t, manager.refreshWithMode(
		context.Background(),
		accountID,
		state,
		func(context.Context) (string, uint64, error) {
			calls.Add(1)
			return testWorkloadPolicyNew, 2, nil
		},
		true,
	))
	require.Equal(t, int32(1), calls.Load())

	close(release)
	require.Eventually(t, func() bool {
		snapshot := state.snapshot.Load()
		return snapshot != nil &&
			snapshot.revision == 2 &&
			snapshot.raw == testWorkloadPolicyNew
	}, time.Second, time.Millisecond)
	require.Zero(t, state.failures)
}

func TestWorkloadPolicyCatalogCorruptionFailsClosed(t *testing.T) {
	const accountID = uint32(46)
	manager := newWorkloadPolicyManager(
		time.Minute,
		func(context.Context, FeSession, uint32) (string, uint64, error) {
			return `{"version":1`, 4, nil
		},
	)
	state := manager.acquire(accountID)
	require.NoError(t, manager.Refresh(
		context.Background(),
		&Session{},
		accountID,
		state,
	))
	snapshot := manager.cached(state)
	require.NotEmpty(t, snapshot.InvalidReason)
	require.False(t, snapshot.Configured())
}

func TestWorkloadPolicyInitialReadFailureUsesBoundedRetry(t *testing.T) {
	const accountID = uint32(52)
	var calls atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	manager := newWorkloadPolicyManager(
		time.Minute,
		func(context.Context, FeSession, uint32) (string, uint64, error) {
			calls.Add(1)
			if fail.Load() {
				return "", 0, moerr.NewInternalErrorNoCtx(
					"catalog temporarily unavailable",
				)
			}
			return testWorkloadPolicyNew, 6, nil
		},
	)
	state := manager.acquire(accountID)

	require.ErrorContains(
		t,
		manager.Refresh(context.Background(), &Session{}, accountID, state),
		"catalog temporarily unavailable",
	)
	require.Contains(
		t,
		manager.cached(state).InvalidReason,
		"catalog temporarily unavailable",
	)
	require.NoError(
		t,
		manager.Refresh(context.Background(), &Session{}, accountID, state),
	)
	require.Equal(t, int32(1), calls.Load())

	fail.Store(false)
	state.refreshAfter.Store(0)
	require.NoError(
		t,
		manager.Refresh(context.Background(), &Session{}, accountID, state),
	)
	require.Equal(t, int32(2), calls.Load())
	require.Zero(t, state.failures)
	require.Empty(t, manager.cached(state).InvalidReason)
	require.Equal(t, uint64(6), state.snapshot.Load().revision)
}

func TestWorkloadPolicyRefreshBackoffIsExponentialAndCapped(t *testing.T) {
	require.Equal(t, time.Second, workloadPolicyRefreshBackoff(0))
	require.Equal(t, time.Second, workloadPolicyRefreshBackoff(1))
	require.Equal(t, 2*time.Second, workloadPolicyRefreshBackoff(2))
	require.Equal(t, 16*time.Second, workloadPolicyRefreshBackoff(5))
	require.Equal(t, 30*time.Second, workloadPolicyRefreshBackoff(6))
	require.Equal(t, 30*time.Second, workloadPolicyRefreshBackoff(^uint8(0)))
}

func TestWorkloadPolicyCachedReadDoesNotAllocateOrReload(t *testing.T) {
	const accountID = uint32(47)
	var loads atomic.Int32
	manager := newWorkloadPolicyManager(
		time.Minute,
		func(context.Context, FeSession, uint32) (string, uint64, error) {
			loads.Add(1)
			return testWorkloadPolicyNew, 5, nil
		},
	)
	state := manager.acquire(accountID)
	require.NoError(t, manager.Refresh(
		context.Background(),
		&Session{},
		accountID,
		state,
	))

	allocations := testing.AllocsPerRun(1000, func() {
		if manager.cached(state).Generation == "" {
			panic("cached workload policy disappeared")
		}
	})
	require.Zero(t, allocations)
	for range 1000 {
		require.NoError(t, manager.Refresh(
			context.Background(),
			&Session{},
			accountID,
			state,
		))
	}
	require.Equal(t, int32(1), loads.Load())
}

func TestWorkloadPolicyControlPlaneBypassesConfiguredPolicy(t *testing.T) {
	state := &accountWorkloadPolicy{}
	state.snapshot.Store(&workloadPolicySnapshot{
		raw:      testWorkloadPolicyNew,
		revision: 1,
		set: schedule.WorkloadPolicySet{
			Generation: "configured",
		},
	})
	ses := &backSession{}
	ses.workloadPolicy.Store(state)

	require.Equal(
		t,
		"configured",
		queryWorkloadPolicySnapshotAt(context.Background(), ses).Generation,
	)
	require.Empty(
		t,
		queryWorkloadPolicySnapshotAt(
			withWorkloadPolicyBypass(context.Background()),
			ses,
		).Generation,
	)
}

func TestLoadWorkloadPolicyCatalogMarksControlPlaneContext(t *testing.T) {
	const accountID = uint32(55)
	selectSQL := "select policy, revision from mo_catalog.mo_query_workload_policy where account_id = 55"
	result := newWorkloadPolicyCatalogResult(
		[][]interface{}{{testWorkloadPolicyNew, uint64(8)}},
	)

	base := &backgroundExecTest{}
	base.init()
	base.sql2result[selectSQL] = result
	executor := &workloadPolicyContextExec{backgroundExecTest: base}

	raw, revision, err := loadWorkloadPolicyFromCatalogWithExec(
		context.Background(),
		executor,
		accountID,
	)
	require.NoError(t, err)
	require.Equal(t, testWorkloadPolicyNew, raw)
	require.Equal(t, uint64(8), revision)
	require.Len(t, executor.contexts, 1)
	require.True(t, workloadPolicyBypassed(executor.contexts[0]))
}

func TestLoadWorkloadPolicyCatalogUsesTargetAccountAndClosesExecutor(t *testing.T) {
	const accountID = uint32(59)
	selectSQL := "select policy, revision from mo_catalog.mo_query_workload_policy where account_id = 59"
	base := &backgroundExecTest{}
	base.init()
	base.sql2result[selectSQL] = newWorkloadPolicyCatalogResult(
		[][]interface{}{{testWorkloadPolicyOld, uint64(11)}},
	)
	exec := &workloadPolicyContextExec{backgroundExecTest: base}
	stub := gostub.StubFunc(&NewBackgroundExec, exec)
	defer stub.Reset()

	raw, revision, err := loadWorkloadPolicyFromCatalog(
		context.Background(),
		&Session{},
		accountID,
	)
	require.NoError(t, err)
	require.Equal(t, testWorkloadPolicyOld, raw)
	require.Equal(t, uint64(11), revision)
	require.Equal(t, 1, exec.closes)
	require.Len(t, exec.contexts, 1)
	require.True(t, workloadPolicyBypassed(exec.contexts[0]))
	actualAccountID, err := defines.GetAccountId(exec.contexts[0])
	require.NoError(t, err)
	require.Equal(t, accountID, actualAccountID)

	_, _, err = loadWorkloadPolicyFromCatalog(
		context.Background(),
		nil,
		accountID,
	)
	require.ErrorContains(t, err, "session is nil")
}

func TestLoadWorkloadPolicyCatalogWithExecRejectsInvalidResults(t *testing.T) {
	const accountID = uint32(60)
	selectSQL := "select policy, revision from mo_catalog.mo_query_workload_policy where account_id = 60"

	_, _, err := loadWorkloadPolicyFromCatalogWithExec(
		context.Background(),
		nil,
		accountID,
	)
	require.ErrorContains(t, err, "background executor is nil")

	tests := []struct {
		name         string
		result       ExecResult
		execErr      error
		wantRaw      string
		wantRevision uint64
		wantErr      string
	}{
		{
			name:    "table not installed",
			execErr: moerr.NewNoSuchTableNoCtx("mo_catalog", "mo_query_workload_policy"),
		},
		{
			name:    "catalog unavailable",
			execErr: moerr.NewInternalErrorNoCtx("catalog unavailable"),
			wantErr: "catalog unavailable",
		},
		{
			name:   "no policy row",
			result: newWorkloadPolicyCatalogResult(nil),
		},
		{
			name: "multiple policy rows",
			result: newWorkloadPolicyCatalogResult([][]interface{}{
				{testWorkloadPolicyOld, uint64(1)},
				{testWorkloadPolicyNew, uint64(2)},
			}),
			wantErr: "expected one workload policy row",
		},
		{
			name: "invalid policy value",
			result: newWorkloadPolicyCatalogResult(
				[][]interface{}{{struct{}{}, uint64(1)}},
			),
			wantErr: "unsupported type",
		},
		{
			name: "invalid revision value",
			result: newWorkloadPolicyCatalogResult(
				[][]interface{}{{testWorkloadPolicyOld, "not-a-revision"}},
			),
			wantErr: "invalid syntax",
		},
		{
			name: "zero revision",
			result: newWorkloadPolicyCatalogResult(
				[][]interface{}{{testWorkloadPolicyOld, uint64(0)}},
			),
			wantErr: "revision is zero",
		},
		{
			name: "valid row",
			result: newWorkloadPolicyCatalogResult(
				[][]interface{}{{testWorkloadPolicyNew, uint64(12)}},
			),
			wantRaw:      testWorkloadPolicyNew,
			wantRevision: 12,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := &backgroundExecTest{}
			base.init()
			base.sql2err[selectSQL] = test.execErr
			if test.result != nil {
				base.sql2result[selectSQL] = test.result
			}
			raw, revision, err := loadWorkloadPolicyFromCatalogWithExec(
				context.Background(),
				base,
				accountID,
			)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.wantRaw, raw)
			require.Equal(t, test.wantRevision, revision)
		})
	}
}

func TestLoadWorkloadPolicyCatalogByService(t *testing.T) {
	const (
		service   = "workload-policy-loader-test"
		accountID = uint32(58)
	)
	mp := mpool.MustNewZero()
	memResult := executor.NewMemResult(
		[]types.Type{
			types.T_text.ToType(),
			types.T_uint64.ToType(),
		},
		mp,
	)
	memResult.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(
		memResult,
		0,
		[]string{testWorkloadPolicyNew},
	))
	require.NoError(t, executor.AppendFixedRows(
		memResult,
		1,
		[]uint64{9},
	))

	rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
	moruntime.SetupServiceBasedRuntime(service, rt)
	rt.SetGlobalVariables(
		moruntime.InternalSQLExecutor,
		&workloadPolicySQLExecutor{
			exec: func(
				ctx context.Context,
				sql string,
				opts executor.Options,
			) (executor.Result, error) {
				require.True(t, workloadPolicyBypassed(ctx))
				require.Contains(
					t,
					sql,
					"where account_id = 58",
				)
				require.True(t, opts.HasAccountID())
				require.Equal(t, accountID, opts.AccountID())
				require.Equal(t, "mo_catalog", opts.Database())
				require.True(t, opts.StatementOption().DisableLog())
				return memResult.GetResult(), nil
			},
		},
	)

	raw, revision, err := loadWorkloadPolicyFromCatalogByService(
		context.Background(),
		service,
		accountID,
	)
	require.NoError(t, err)
	require.Equal(t, testWorkloadPolicyNew, raw)
	require.Equal(t, uint64(9), revision)
	require.Zero(t, mp.CurrNB())
}

func TestQueryWorkloadPolicyRefreshUsesOwnedAccountIdentity(t *testing.T) {
	const (
		service        = "workload-policy-owned-account-test"
		ownedAccountID = uint32(58)
	)
	mp := mpool.MustNewZero()
	memResult := executor.NewMemResult(
		[]types.Type{
			types.T_text.ToType(),
			types.T_uint64.ToType(),
		},
		mp,
	)
	memResult.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(
		memResult,
		0,
		[]string{testWorkloadPolicyNew},
	))
	require.NoError(t, executor.AppendFixedRows(
		memResult,
		1,
		[]uint64{2},
	))

	loadedAccount := make(chan uint32, 1)
	rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
	moruntime.SetupServiceBasedRuntime(service, rt)
	rt.SetGlobalVariables(
		moruntime.InternalSQLExecutor,
		&workloadPolicySQLExecutor{
			exec: func(
				_ context.Context,
				_ string,
				opts executor.Options,
			) (executor.Result, error) {
				loadedAccount <- opts.AccountID()
				if opts.AccountID() != ownedAccountID {
					return executor.Result{}, moerr.NewInternalErrorNoCtx(
						"loaded the wrong account",
					)
				}
				return memResult.GetResult(), nil
			},
		},
	)

	previousManager := GWorkloadPolicyManager
	manager := newWorkloadPolicyManager(time.Minute, nil)
	GWorkloadPolicyManager = manager
	t.Cleanup(func() {
		GWorkloadPolicyManager = previousManager
	})
	state := manager.acquire(ownedAccountID)
	defer manager.release(state)
	applied, _, err := manager.Apply(
		ownedAccountID,
		testWorkloadPolicyOld,
		1,
	)
	require.NoError(t, err)
	require.True(t, applied)
	state.refreshAfter.Store(0)

	ses := &Session{feSessionImpl: feSessionImpl{service: service}}
	ses.SetAccountId(ownedAccountID + 1)
	ses.workloadPolicy.Store(state)

	snapshot := queryWorkloadPolicySnapshotAt(context.Background(), ses)
	require.Empty(t, snapshot.InvalidReason)
	require.Equal(
		t,
		"old",
		snapshot.Rules[schedule.WorkloadAP].PoolIdentity,
	)
	select {
	case accountID := <-loadedAccount:
		require.Equal(t, ownedAccountID, accountID)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for workload policy refresh")
	}
	require.Eventually(t, func() bool {
		current := state.snapshot.Load()
		return current != nil && current.revision == 2
	}, time.Second, time.Millisecond)
	require.Zero(t, mp.CurrNB())
}

func TestLoadWorkloadPolicyCatalogByServiceFailureBoundaries(t *testing.T) {
	t.Run("runtime not initialized", func(t *testing.T) {
		_, _, err := loadWorkloadPolicyFromCatalogByService(
			context.Background(),
			t.Name(),
			61,
		)
		require.ErrorContains(t, err, "runtime is not initialized")
	})

	t.Run("executor not initialized", func(t *testing.T) {
		service := t.Name()
		rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
		moruntime.SetupServiceBasedRuntime(service, rt)

		_, _, err := loadWorkloadPolicyFromCatalogByService(
			context.Background(),
			service,
			61,
		)
		require.ErrorContains(t, err, "executor is not initialized")
	})

	t.Run("executor has invalid type", func(t *testing.T) {
		service := t.Name()
		rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
		moruntime.SetupServiceBasedRuntime(service, rt)
		rt.SetGlobalVariables(moruntime.InternalSQLExecutor, "not an executor")

		_, _, err := loadWorkloadPolicyFromCatalogByService(
			context.Background(),
			service,
			61,
		)
		require.ErrorContains(t, err, "executor has invalid type")
	})

	for _, test := range []struct {
		name    string
		execErr error
		wantErr string
	}{
		{
			name:    "table not installed",
			execErr: moerr.NewNoSuchTableNoCtx("mo_catalog", "mo_query_workload_policy"),
		},
		{
			name:    "database not installed",
			execErr: moerr.NewBadDBNoCtx("mo_catalog"),
		},
		{
			name:    "query failure",
			execErr: moerr.NewInternalErrorNoCtx("injected catalog failure"),
			wantErr: "injected catalog failure",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := t.Name()
			rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
			moruntime.SetupServiceBasedRuntime(service, rt)
			rt.SetGlobalVariables(
				moruntime.InternalSQLExecutor,
				&workloadPolicySQLExecutor{
					exec: func(
						context.Context,
						string,
						executor.Options,
					) (executor.Result, error) {
						return executor.Result{}, test.execErr
					},
				},
			)

			raw, revision, err := loadWorkloadPolicyFromCatalogByService(
				context.Background(),
				service,
				61,
			)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Empty(t, raw)
			require.Zero(t, revision)
		})
	}

	for _, test := range []struct {
		name      string
		policies  []string
		revisions []uint64
		wantErr   string
	}{
		{
			name: "no policy row",
		},
		{
			name:      "multiple policy rows",
			policies:  []string{testWorkloadPolicyOld, testWorkloadPolicyNew},
			revisions: []uint64{1, 2},
			wantErr:   "expected one workload policy row",
		},
		{
			name:      "zero revision",
			policies:  []string{testWorkloadPolicyOld},
			revisions: []uint64{0},
			wantErr:   "revision is zero",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := t.Name()
			mp := mpool.MustNewZero()
			memResult := executor.NewMemResult(
				[]types.Type{
					types.T_text.ToType(),
					types.T_uint64.ToType(),
				},
				mp,
			)
			if len(test.policies) > 0 {
				memResult.NewBatchWithRowCount(len(test.policies))
				require.NoError(t, executor.AppendStringRows(
					memResult,
					0,
					test.policies,
				))
				require.NoError(t, executor.AppendFixedRows(
					memResult,
					1,
					test.revisions,
				))
			}

			rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
			moruntime.SetupServiceBasedRuntime(service, rt)
			rt.SetGlobalVariables(
				moruntime.InternalSQLExecutor,
				&workloadPolicySQLExecutor{
					exec: func(
						context.Context,
						string,
						executor.Options,
					) (executor.Result, error) {
						return memResult.GetResult(), nil
					},
				},
			)

			raw, revision, err := loadWorkloadPolicyFromCatalogByService(
				context.Background(),
				service,
				61,
			)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
				require.Empty(t, raw)
				require.Zero(t, revision)
			}
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestUpsertWorkloadPolicyIsAtomicAndSQLModeIndependent(t *testing.T) {
	const accountID = uint32(48)
	raw := `{"version":1,"policies":{"ap":{"pool":"O'Reilly\\pool","labels":{"role":"ap"}}}}`
	selectSQL := "select revision from mo_catalog.mo_query_workload_policy where account_id = 48"
	result := &MysqlResultSet{}
	column := &MysqlColumn{}
	column.SetName("revision")
	column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	result.AddColumn(column)
	result.AddRow([]interface{}{uint64(7)})

	executor := &backgroundExecTest{}
	executor.init()
	executor.sql2result[selectSQL] = result
	stub := gostub.StubFunc(&NewBackgroundExec, executor)
	defer stub.Reset()

	revision, err := upsertWorkloadPolicy(
		context.Background(),
		&Session{},
		accountID,
		raw,
		1,
		2,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(7), revision)
	require.Len(t, executor.executedSQLs, 4)
	require.Equal(t, "begin;", executor.executedSQLs[0])
	require.Equal(t, selectSQL, executor.executedSQLs[2])
	require.Equal(t, "commit;", executor.executedSQLs[3])

	upsertSQL := executor.executedSQLs[1]
	require.NotContains(t, upsertSQL, raw)
	require.Contains(t, upsertSQL, "revision = revision + 1")
	statements, err := mysqlparser.Parse(context.Background(), upsertSQL, 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	for _, statement := range statements {
		statement.Free()
	}
	statements, err = mysqlparser.ParseWithSQLMode(
		context.Background(),
		upsertSQL,
		1,
		"NO_BACKSLASH_ESCAPES",
	)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	for _, statement := range statements {
		statement.Free()
	}
}

func TestWorkloadPolicyTableDDLParses(t *testing.T) {
	statements, err := mysqlparser.Parse(
		context.Background(),
		MoCatalogMoQueryWorkloadPolicyDDL,
		1,
	)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	for _, statement := range statements {
		statement.Free()
	}
}

func TestWorkloadPolicySystemTableLifecycleRegistration(t *testing.T) {
	_, wantedBySystemBootstrap := sysWantedTables[catalog.MO_QUERY_WORKLOAD_POLICY]
	require.True(t, wantedBySystemBootstrap)
	_, predefinedForEveryTenant := predefinedTables[catalog.MO_QUERY_WORKLOAD_POLICY]
	require.True(t, predefinedForEveryTenant)
	require.Equal(
		t,
		int8(1),
		needSkipTablesInMocatalog[catalog.MO_QUERY_WORKLOAD_POLICY],
	)

	require.Contains(t, createSqls, MoCatalogMoQueryWorkloadPolicyDDL)
	var registeredDrop bool
	for _, sql := range dropSqls {
		if strings.Contains(sql, catalog.MO_QUERY_WORKLOAD_POLICY) {
			registeredDrop = true
			break
		}
	}
	require.True(t, registeredDrop)
}

func TestWorkloadPolicyActivationRequiresReadyClusterVersion(t *testing.T) {
	result := newWorkloadPolicyVersionResult("4.0.4", uint64(100))
	service := t.Name()
	setupWorkloadPolicyQueryCluster(t, service, []metadata.CNService{{
		ServiceID:    "cn-1",
		QueryAddress: "cn-1",
		WorkState:    metadata.WorkState_Working,
	}})

	executor := &backgroundExecTest{}
	executor.init()
	executor.sql2result[testWorkloadPolicyVersionSQL] = result
	stub := gostub.StubFunc(&NewBackgroundExec, executor)
	defer stub.Reset()

	session := &Session{feSessionImpl: feSessionImpl{service: service}}
	err := ensureWorkloadPolicyFeatureReady(context.Background(), session)
	require.ErrorContains(
		t,
		err,
		"latest ready version is 4.0.4 offset 100",
	)

	result.Data[0][0] = catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION
	result.Data[0][1] = uint64(
		catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION_OFFSET - 1,
	)
	err = ensureWorkloadPolicyFeatureReady(context.Background(), session)
	require.ErrorContains(t, err, "latest ready version is 4.0.5 offset 4")

	result.Data[0][1] = uint64(
		catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION_OFFSET,
	)
	require.NoError(
		t,
		ensureWorkloadPolicyFeatureReady(context.Background(), session),
	)

	result.Data[0][1] = uint64(
		catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION_OFFSET + 1,
	)
	require.NoError(
		t,
		ensureWorkloadPolicyFeatureReady(context.Background(), session),
	)

	result.Data[0][0] = "4.1.0"
	result.Data[0][1] = uint64(0)
	require.NoError(
		t,
		ensureWorkloadPolicyFeatureReady(context.Background(), session),
	)
}

func TestWorkloadPolicyActivationRequiresCompatibleRPCProtocol(t *testing.T) {
	tests := []struct {
		name        string
		service     string
		protocol    interface{}
		wantErr     string
		initRuntime bool
	}{
		{
			name:    "runtime unavailable",
			service: "workload-policy-protocol-no-runtime",
			wantErr: "runtime is not initialized",
		},
		{
			name:        "protocol value invalid",
			service:     "workload-policy-protocol-invalid",
			protocol:    "5",
			wantErr:     "protocol version is not initialized",
			initRuntime: true,
		},
		{
			name:        "protocol too old",
			service:     "workload-policy-protocol-old",
			protocol:    defines.MORPCVersion5 - 1,
			wantErr:     "requires protocol version 5 or later; current protocol version is 4",
			initRuntime: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.initRuntime {
				moruntime.RunTest(test.service, func(rt moruntime.Runtime) {
					rt.SetGlobalVariables(
						moruntime.MOProtocolVersion,
						test.protocol,
					)
				})
				t.Cleanup(func() {
					moruntime.ServiceRuntime(test.service).SetGlobalVariables(
						moruntime.MOProtocolVersion,
						defines.MORPCLatestVersion,
					)
				})
			}

			exec := &workloadPolicyContextExec{
				backgroundExecTest: &backgroundExecTest{},
			}
			exec.init()
			stub := gostub.StubFunc(&NewBackgroundExec, exec)
			defer stub.Reset()

			session := &Session{
				feSessionImpl: feSessionImpl{service: test.service},
			}
			err := ensureWorkloadPolicyFeatureReady(
				context.Background(),
				session,
			)
			require.ErrorContains(t, err, test.wantErr)
			require.Empty(t, exec.executedSQLs)
			require.Zero(t, exec.closes)
		})
	}
}

func TestWorkloadPolicyActivationRequiresEveryCNProtocol(t *testing.T) {
	result := newWorkloadPolicyVersionResult(
		catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION,
		uint64(catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION_OFFSET),
	)
	base := &backgroundExecTest{}
	base.init()
	base.sql2result[testWorkloadPolicyVersionSQL] = result
	stub := gostub.StubFunc(&NewBackgroundExec, base)
	defer stub.Reset()

	t.Run("mixed-version CN blocks activation", func(t *testing.T) {
		service := t.Name()
		client := setupWorkloadPolicyQueryCluster(
			t,
			service,
			[]metadata.CNService{
				{
					ServiceID:    "cn-new",
					QueryAddress: "new-address",
					WorkState:    metadata.WorkState_Working,
				},
				{
					ServiceID:    "cn-old",
					QueryAddress: "old-address",
					WorkState:    metadata.WorkState_Working,
				},
			},
		)
		client.protocolVersions["old-address"] = defines.MORPCVersion5 - 1

		err := ensureWorkloadPolicyFeatureReady(
			context.Background(),
			&Session{feSessionImpl: feSessionImpl{service: service}},
		)
		require.ErrorContains(t, err, "CN cn-old reports version 4")
		require.Equal(t, 2, client.releases)
	})

	t.Run("unreachable CN blocks activation", func(t *testing.T) {
		service := t.Name()
		client := setupWorkloadPolicyQueryCluster(
			t,
			service,
			[]metadata.CNService{{
				ServiceID:    "cn-down",
				QueryAddress: "down-address",
				WorkState:    metadata.WorkState_Working,
			}},
		)
		client.sendErrs["down-address"] = errors.New("injected network failure")

		err := ensureWorkloadPolicyFeatureReady(
			context.Background(),
			&Session{feSessionImpl: feSessionImpl{service: service}},
		)
		require.ErrorContains(t, err, "injected network failure")
		require.ErrorContains(
			t,
			err,
			"failed to verify workload policy protocol on CN cn-down",
		)
		require.Zero(t, client.releases)
	})

	t.Run("empty cluster snapshot blocks activation", func(t *testing.T) {
		service := t.Name()
		setupWorkloadPolicyQueryCluster(t, service, nil)

		err := ensureWorkloadPolicyFeatureReady(
			context.Background(),
			&Session{feSessionImpl: feSessionImpl{service: service}},
		)
		require.ErrorContains(t, err, "requires at least one active CN")
	})

	t.Run("CN without query address blocks activation", func(t *testing.T) {
		service := t.Name()
		setupWorkloadPolicyQueryCluster(
			t,
			service,
			[]metadata.CNService{{
				ServiceID: "cn-no-address",
				WorkState: metadata.WorkState_Working,
			}},
		)

		err := ensureWorkloadPolicyFeatureReady(
			context.Background(),
			&Session{feSessionImpl: feSessionImpl{service: service}},
		)
		require.ErrorContains(t, err, "CN cn-no-address has no query address")
	})

	t.Run("empty protocol response blocks activation", func(t *testing.T) {
		service := t.Name()
		client := setupWorkloadPolicyQueryCluster(
			t,
			service,
			[]metadata.CNService{{
				ServiceID:    "cn-empty",
				QueryAddress: "empty-address",
				WorkState:    metadata.WorkState_Working,
			}},
		)
		client.protocolResponses["empty-address"] = &query.Response{}

		err := ensureWorkloadPolicyFeatureReady(
			context.Background(),
			&Session{feSessionImpl: feSessionImpl{service: service}},
		)
		require.ErrorContains(
			t,
			err,
			"CN cn-empty returned an empty protocol version response",
		)
		require.Equal(t, 1, client.releases)
	})

	t.Run("query client is required", func(t *testing.T) {
		service := t.Name()
		InitServerLevelVars(service)
		rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
		moruntime.SetupServiceBasedRuntime(service, rt)

		err := ensureWorkloadPolicyFeatureReady(
			context.Background(),
			&Session{feSessionImpl: feSessionImpl{service: service}},
		)
		require.ErrorContains(t, err, "query client is not initialized")
	})
}

func TestWorkloadPolicyActivationFailsClosedOnCatalogErrors(t *testing.T) {
	tests := []struct {
		name    string
		result  ExecResult
		execErr error
		wantErr string
	}{
		{
			name:    "query fails",
			execErr: moerr.NewInternalErrorNoCtx("injected version query failure"),
			wantErr: "injected version query failure",
		},
		{
			name:    "no completed version",
			result:  &MysqlResultSet{},
			wantErr: "requires a completed cluster version upgrade",
		},
		{
			name: "invalid version value",
			result: newWorkloadPolicyVersionResult(
				struct{}{},
				uint64(catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION_OFFSET),
			),
			wantErr: "unsupported type",
		},
		{
			name: "invalid version offset",
			result: newWorkloadPolicyVersionResult(
				catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION,
				"not-an-offset",
			),
			wantErr: "invalid syntax",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := &backgroundExecTest{}
			base.init()
			base.sql2err[testWorkloadPolicyVersionSQL] = test.execErr
			if test.result != nil {
				base.sql2result[testWorkloadPolicyVersionSQL] = test.result
			}
			exec := &workloadPolicyContextExec{backgroundExecTest: base}
			stub := gostub.StubFunc(&NewBackgroundExec, exec)
			defer stub.Reset()

			err := ensureWorkloadPolicyFeatureReady(
				context.Background(),
				&Session{},
			)
			require.ErrorContains(t, err, test.wantErr)
			require.Equal(t, 1, exec.closes)
			require.Equal(
				t,
				[]string{testWorkloadPolicyVersionSQL},
				exec.executedSQLs,
			)
			require.Len(t, exec.contexts, 1)
			accountID, err := defines.GetAccountId(exec.contexts[0])
			require.NoError(t, err)
			require.Equal(t, uint32(sysAccountID), accountID)
		})
	}
}

func TestResolveWorkloadPolicyAccountEnforcesScopeAndValidatesCatalog(t *testing.T) {
	self := &Session{}
	self.SetTenantInfo(&TenantInfo{
		Tenant:      "tenant_a",
		TenantID:    62,
		DefaultRole: accountAdminRoleName,
	})
	id, err := resolveWorkloadPolicyAccount(
		context.Background(),
		self,
		"",
	)
	require.NoError(t, err)
	require.Equal(t, uint32(62), id)
	id, err = resolveWorkloadPolicyAccount(
		context.Background(),
		self,
		"TENANT_A",
	)
	require.NoError(t, err)
	require.Equal(t, uint32(62), id)

	_, err = resolveWorkloadPolicyAccount(
		context.Background(),
		self,
		"tenant_b",
	)
	require.ErrorContains(
		t,
		err,
		"only the system administrator can configure another account",
	)

	admin := &Session{}
	admin.SetTenantInfo(&TenantInfo{
		Tenant:      sysAccountName,
		DefaultRole: moAdminRoleName,
	})
	_, err = resolveWorkloadPolicyAccount(
		context.Background(),
		admin,
		"bad account name",
	)
	require.ErrorContains(t, err, "invalid input")

	targetSQL, err := getSqlForCheckTenant(context.Background(), "tenant_b")
	require.NoError(t, err)
	tests := []struct {
		name    string
		result  ExecResult
		execErr error
		wantID  uint32
		wantErr string
	}{
		{
			name: "account exists",
			result: newMrsForCheckTenant([][]interface{}{
				{uint64(63), "tenant_b", "open", uint64(1)},
			}),
			wantID: 63,
		},
		{
			name:    "account does not exist",
			result:  newMrsForCheckTenant(nil),
			wantErr: "account tenant_b does not exist",
		},
		{
			name: "account id out of range",
			result: newMrsForCheckTenant([][]interface{}{
				{uint64(^uint32(0)) + 1, "tenant_b", "open", uint64(1)},
			}),
			wantErr: "is out of range",
		},
		{
			name: "invalid account id",
			result: newMrsForCheckTenant([][]interface{}{
				{"not-an-id", "tenant_b", "open", uint64(1)},
			}),
			wantErr: "invalid syntax",
		},
		{
			name:    "catalog query fails",
			execErr: moerr.NewInternalErrorNoCtx("injected account lookup failure"),
			wantErr: "injected account lookup failure",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := &backgroundExecTest{}
			base.init()
			base.sql2err[targetSQL] = test.execErr
			if test.result != nil {
				base.sql2result[targetSQL] = test.result
			}
			exec := &workloadPolicyContextExec{backgroundExecTest: base}
			stub := gostub.StubFunc(&NewBackgroundExec, exec)
			defer stub.Reset()

			id, err := resolveWorkloadPolicyAccount(
				context.Background(),
				admin,
				" tenant_b ",
			)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.wantID, id)
			}
			require.Equal(t, 1, exec.closes)
			require.Equal(t, []string{targetSQL}, exec.executedSQLs)
		})
	}
}

func TestAlterQueryWorkloadPolicyCommitsAppliesAndPublishes(t *testing.T) {
	const (
		accountID = uint32(65)
		userID    = uint32(66)
	)
	service := t.Name()
	client := newWorkloadPolicyQueryClient(service)
	client.responses["cn-1"] = &query.Response{
		WorkloadPolicyUpdateResponse: &query.WorkloadPolicyUpdateResponse{
			Applied:  true,
			Revision: 12,
		},
	}
	InitServerLevelVars(service)
	setPu(service, &config.ParameterUnit{QueryClient: client})
	cluster := &workloadPolicyMOCluster{
		services: []metadata.CNService{{
			ServiceID:    "cn-1",
			QueryAddress: "cn-1",
			WorkState:    metadata.WorkState_Working,
		}},
	}
	rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
	moruntime.SetupServiceBasedRuntime(service, rt)
	rt.SetGlobalVariables(moruntime.ClusterService, cluster)

	session := &Session{feSessionImpl: feSessionImpl{service: service}}
	session.SetTenantInfo(&TenantInfo{
		Tenant:      "tenant_a",
		TenantID:    accountID,
		UserID:      userID,
		DefaultRole: accountAdminRoleName,
	})

	versionResult := newWorkloadPolicyVersionResult(
		catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION,
		uint64(catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION_OFFSET),
	)

	revisionSQL := "select revision from mo_catalog.mo_query_workload_policy " +
		"where account_id = 65"
	revisionResult := &MysqlResultSet{}
	revisionColumn := &MysqlColumn{}
	revisionColumn.SetName("revision")
	revisionColumn.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	revisionResult.AddColumn(revisionColumn)
	revisionResult.AddRow([]interface{}{uint64(12)})

	base := &backgroundExecTest{}
	base.init()
	base.sql2result[testWorkloadPolicyVersionSQL] = versionResult
	base.sql2result[revisionSQL] = revisionResult
	exec := &workloadPolicyContextExec{backgroundExecTest: base}
	stub := gostub.StubFunc(&NewBackgroundExec, exec)
	defer stub.Reset()

	previousManager := GWorkloadPolicyManager
	manager := newWorkloadPolicyManager(time.Minute, nil)
	GWorkloadPolicyManager = manager
	defer func() {
		GWorkloadPolicyManager = previousManager
	}()
	state := manager.acquire(accountID)
	defer manager.release(state)

	reset := tree.NewAlterAccountConfig(
		"",
		queryWorkloadPolicy,
		"this value must be ignored by RESET",
		true,
	)
	defer reset.Free()
	require.NoError(t, doAlterQueryWorkloadPolicy(
		context.Background(),
		session,
		reset,
	))

	require.Equal(t, 2, exec.closes)
	require.Len(t, exec.executedSQLs, 5)
	require.Equal(t, testWorkloadPolicyVersionSQL, exec.executedSQLs[0])
	require.Equal(t, "begin;", exec.executedSQLs[1])
	require.Equal(t, revisionSQL, exec.executedSQLs[3])
	require.Equal(t, "commit;", exec.executedSQLs[4])
	require.Contains(
		t,
		exec.executedSQLs[2],
		"(65, unhex(''), 1, 65, 66)",
	)
	require.Len(t, exec.contexts, 5)
	for _, execCtx := range exec.contexts {
		require.True(t, workloadPolicyBypassed(execCtx))
	}
	systemAccountID, err := defines.GetAccountId(exec.contexts[0])
	require.NoError(t, err)
	require.Equal(t, uint32(sysAccountID), systemAccountID)
	for _, execCtx := range exec.contexts[1:] {
		targetAccountID, err := defines.GetAccountId(execCtx)
		require.NoError(t, err)
		require.Equal(t, accountID, targetAccountID)
	}
	require.Equal(t, uint64(12), state.snapshot.Load().revision)
	require.Empty(t, state.snapshot.Load().raw)

	client.mu.Lock()
	request := client.requests["cn-1"]
	require.Equal(t, 2, client.releases)
	client.mu.Unlock()
	require.NotNil(t, request)
	require.Equal(t, accountID, request.WorkloadPolicyUpdateRequest.AccountID)
	require.Empty(t, request.WorkloadPolicyUpdateRequest.Policy)
	require.Equal(t, uint64(12), request.WorkloadPolicyUpdateRequest.Revision)

	// A notification failure happens after commit and must not turn a durable
	// successful ALTER into a client-visible failure. Catalog reconciliation
	// is the repair path for the missed CN.
	client.mu.Lock()
	client.methodSendErrs[query.CmdMethod_WorkloadPolicyUpdate] = map[string]error{
		"cn-1": errors.New("injected publish failure"),
	}
	client.mu.Unlock()
	revisionResult.Data[0][0] = uint64(13)
	set := tree.NewAlterAccountConfig(
		"",
		queryWorkloadPolicy,
		testWorkloadPolicyNew,
		false,
	)
	defer set.Free()
	require.NoError(t, doAlterQueryWorkloadPolicy(
		context.Background(),
		session,
		set,
	))
	require.Equal(t, uint64(13), state.snapshot.Load().revision)
	require.Equal(t, testWorkloadPolicyNew, state.snapshot.Load().raw)
	require.Equal(t, "commit;", exec.executedSQLs[len(exec.executedSQLs)-1])
	require.Equal(t, 4, exec.closes)
}

func TestAlterQueryWorkloadPolicyRejectsBeforeCatalogMutation(t *testing.T) {
	session := &Session{}
	session.SetTenantInfo(&TenantInfo{
		Tenant:      "tenant_a",
		TenantID:    67,
		DefaultRole: accountAdminRoleName,
	})

	unsupported := tree.NewAlterAccountConfig("", "unknown", "{}", false)
	defer unsupported.Free()
	err := doAlterQueryWorkloadPolicy(
		context.Background(),
		session,
		unsupported,
	)
	require.ErrorContains(t, err, "unsupported account configuration")

	session.GetTenantInfo().SetDefaultRole("public")
	valid := tree.NewAlterAccountConfig(
		"",
		queryWorkloadPolicy,
		testWorkloadPolicyNew,
		false,
	)
	defer valid.Free()
	err = doAlterQueryWorkloadPolicy(context.Background(), session, valid)
	require.ErrorContains(t, err, "do not have privilege")

	session.GetTenantInfo().SetDefaultRole(accountAdminRoleName)
	otherAccount := tree.NewAlterAccountConfig(
		"tenant_b",
		queryWorkloadPolicy,
		testWorkloadPolicyNew,
		false,
	)
	defer otherAccount.Free()
	err = doAlterQueryWorkloadPolicy(
		context.Background(),
		session,
		otherAccount,
	)
	require.ErrorContains(
		t,
		err,
		"only the system administrator can configure another account",
	)

	invalid := tree.NewAlterAccountConfig(
		"",
		queryWorkloadPolicy,
		`{"version":1`,
		false,
	)
	defer invalid.Free()
	err = doAlterQueryWorkloadPolicy(context.Background(), session, invalid)
	require.ErrorContains(t, err, "invalid query workload policy")
}

func TestHandleAlterQueryWorkloadPolicyDelegatesValidation(t *testing.T) {
	stmt := tree.NewAlterAccountConfig("", "unsupported", "{}", false)
	defer stmt.Free()
	err := handleAlterQueryWorkloadPolicy(
		&Session{},
		&ExecCtx{reqCtx: context.Background()},
		stmt,
	)
	require.ErrorContains(t, err, "unsupported account configuration")
}

func TestAlterQueryWorkloadPolicyDoesNotMutateBeforeFeatureReady(t *testing.T) {
	base := &backgroundExecTest{}
	base.init()
	base.sql2result[testWorkloadPolicyVersionSQL] = &MysqlResultSet{}
	exec := &workloadPolicyContextExec{backgroundExecTest: base}
	stub := gostub.StubFunc(&NewBackgroundExec, exec)
	defer stub.Reset()

	session := &Session{}
	session.SetTenantInfo(&TenantInfo{
		Tenant:      "tenant_a",
		TenantID:    69,
		DefaultRole: accountAdminRoleName,
	})
	stmt := tree.NewAlterAccountConfig(
		"",
		queryWorkloadPolicy,
		testWorkloadPolicyNew,
		false,
	)
	defer stmt.Free()

	err := doAlterQueryWorkloadPolicy(
		context.Background(),
		session,
		stmt,
	)
	require.ErrorContains(t, err, "requires a completed cluster version upgrade")
	require.Equal(
		t,
		[]string{testWorkloadPolicyVersionSQL},
		exec.executedSQLs,
	)
	require.Equal(t, 1, exec.closes)
}

func TestAlterQueryWorkloadPolicyPropagatesMutationFailure(t *testing.T) {
	service := t.Name()
	setupWorkloadPolicyQueryCluster(t, service, []metadata.CNService{{
		ServiceID:    "cn-1",
		QueryAddress: "cn-1",
		WorkState:    metadata.WorkState_Working,
	}})
	versionResult := newWorkloadPolicyVersionResult(
		catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION,
		uint64(catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION_OFFSET),
	)

	base := &backgroundExecTest{}
	base.init()
	base.sql2result[testWorkloadPolicyVersionSQL] = versionResult
	exec := &workloadPolicyInjectExec{
		backgroundExecTest: base,
		failPrefix:         "insert into mo_catalog.mo_query_workload_policy",
		failErr: moerr.NewInternalErrorNoCtx(
			"injected mutation failure",
		),
	}
	stub := gostub.StubFunc(&NewBackgroundExec, exec)
	defer stub.Reset()

	session := &Session{feSessionImpl: feSessionImpl{service: service}}
	session.SetTenantInfo(&TenantInfo{
		Tenant:      "tenant_a",
		TenantID:    70,
		DefaultRole: accountAdminRoleName,
	})
	stmt := tree.NewAlterAccountConfig(
		"",
		queryWorkloadPolicy,
		testWorkloadPolicyNew,
		false,
	)
	defer stmt.Free()

	err := doAlterQueryWorkloadPolicy(
		context.Background(),
		session,
		stmt,
	)
	require.ErrorContains(t, err, "injected mutation failure")
	require.Equal(t, "rollback;", base.executedSQLs[len(base.executedSQLs)-1])
	require.NotContains(t, base.executedSQLs, "commit;")
}

func TestPublishWorkloadPolicyFanoutAndErrorAggregation(t *testing.T) {
	newSession := func(
		service string,
		client *workloadPolicyQueryClient,
		nodes ...string,
	) *Session {
		InitServerLevelVars(service)
		setPu(service, &config.ParameterUnit{QueryClient: client})
		services := make([]metadata.CNService, 0, len(nodes))
		for index, node := range nodes {
			services = append(services, metadata.CNService{
				ServiceID:    string(rune('a' + index)),
				QueryAddress: node,
				WorkState:    metadata.WorkState_Working,
			})
		}
		cluster := &workloadPolicyMOCluster{services: services}
		rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
		moruntime.SetupServiceBasedRuntime(service, rt)
		rt.SetGlobalVariables(moruntime.ClusterService, cluster)
		return &Session{feSessionImpl: feSessionImpl{service: service}}
	}

	t.Run("query client is required", func(t *testing.T) {
		service := t.Name()
		InitServerLevelVars(service)
		setPu(service, &config.ParameterUnit{})
		err := publishWorkloadPolicy(
			context.Background(),
			&Session{feSessionImpl: feSessionImpl{service: service}},
			64,
			testWorkloadPolicyNew,
			8,
		)
		require.ErrorContains(t, err, "query client is not initialized")
	})

	t.Run("all CNs acknowledge", func(t *testing.T) {
		service := t.Name()
		client := newWorkloadPolicyQueryClient(service)
		client.responses["cn-1"] = &query.Response{
			WorkloadPolicyUpdateResponse: &query.WorkloadPolicyUpdateResponse{
				Applied:  true,
				Revision: 8,
			},
		}
		client.responses["cn-2"] = &query.Response{
			WorkloadPolicyUpdateResponse: &query.WorkloadPolicyUpdateResponse{
				Revision: 9,
			},
		}
		session := newSession(service, client, "cn-1", "cn-2")

		err := publishWorkloadPolicy(
			context.Background(),
			session,
			64,
			testWorkloadPolicyNew,
			8,
		)
		require.NoError(t, err)

		client.mu.Lock()
		defer client.mu.Unlock()
		require.Len(t, client.requests, 2)
		require.Equal(t, 2, client.releases)
		for _, request := range client.requests {
			require.Equal(t, query.CmdMethod_WorkloadPolicyUpdate, request.CmdMethod)
			require.Equal(t, uint32(64), request.WorkloadPolicyUpdateRequest.AccountID)
			require.Equal(t, testWorkloadPolicyNew, request.WorkloadPolicyUpdateRequest.Policy)
			require.Equal(t, uint64(8), request.WorkloadPolicyUpdateRequest.Revision)
		}
	})

	t.Run("transport and response errors are joined", func(t *testing.T) {
		service := t.Name()
		client := newWorkloadPolicyQueryClient(service)
		client.responses["cn-stale"] = &query.Response{
			WorkloadPolicyUpdateResponse: &query.WorkloadPolicyUpdateResponse{
				Applied:  true,
				Revision: 7,
			},
		}
		client.sendErrs["cn-down"] = errors.New("injected network failure")
		session := newSession(service, client, "cn-stale", "cn-down")

		err := publishWorkloadPolicy(
			context.Background(),
			session,
			64,
			testWorkloadPolicyNew,
			8,
		)
		require.ErrorContains(t, err, "injected network failure")
		require.ErrorContains(t, err, "failed to publish workload policy to CN cn-down")
		require.ErrorContains(
			t,
			err,
			"acknowledged workload policy revision 8 as 7",
		)

		client.mu.Lock()
		defer client.mu.Unlock()
		require.Len(t, client.requests, 2)
		require.Equal(t, 1, client.releases)
	})
}

func TestValidateWorkloadPolicyUpdateResponse(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, validateWorkloadPolicyUpdateResponse(
		ctx,
		"cn-1",
		8,
		&query.Response{WorkloadPolicyUpdateResponse: &query.WorkloadPolicyUpdateResponse{
			Applied:  true,
			Revision: 8,
		}},
	))
	require.NoError(t, validateWorkloadPolicyUpdateResponse(
		ctx,
		"cn-1",
		8,
		&query.Response{WorkloadPolicyUpdateResponse: &query.WorkloadPolicyUpdateResponse{
			Revision: 9,
		}},
	))
	require.NoError(t, validateWorkloadPolicyUpdateResponse(
		ctx,
		"cn-1",
		8,
		&query.Response{WorkloadPolicyUpdateResponse: &query.WorkloadPolicyUpdateResponse{}},
	))
	require.Error(t, validateWorkloadPolicyUpdateResponse(
		ctx,
		"cn-1",
		8,
		nil,
	))
	require.Error(t, validateWorkloadPolicyUpdateResponse(
		ctx,
		"cn-1",
		8,
		&query.Response{WorkloadPolicyUpdateResponse: &query.WorkloadPolicyUpdateResponse{
			Applied:  true,
			Revision: 7,
		}},
	))
	require.Error(t, validateWorkloadPolicyUpdateResponse(
		ctx,
		"cn-1",
		8,
		&query.Response{WorkloadPolicyUpdateResponse: &query.WorkloadPolicyUpdateResponse{
			Revision: 7,
		}},
	))
}

func TestUpsertWorkloadPolicyRollsBackOnFailure(t *testing.T) {
	const accountID = uint32(49)
	executor := &backgroundExecTest{}
	executor.init()
	selectSQL := "select revision from mo_catalog.mo_query_workload_policy where account_id = 49"
	result := &MysqlResultSet{}
	column := &MysqlColumn{}
	column.SetName("revision")
	column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	result.AddColumn(column)
	result.AddRow([]interface{}{uint64(2)})
	executor.sql2result[selectSQL] = result
	executor.sql2err["commit;"] = moerr.NewInternalErrorNoCtx("injected commit failure")
	stub := gostub.StubFunc(&NewBackgroundExec, executor)
	defer stub.Reset()

	_, err := upsertWorkloadPolicy(
		context.Background(),
		&Session{},
		accountID,
		testWorkloadPolicyNew,
		1,
		2,
	)
	require.ErrorContains(t, err, "injected commit failure")
	require.Equal(t, "rollback;", executor.executedSQLs[len(executor.executedSQLs)-1])
	require.True(t, strings.HasPrefix(
		executor.executedSQLs[1],
		"insert into mo_catalog.mo_query_workload_policy",
	))
}

func TestUpsertWorkloadPolicyFailureMatrix(t *testing.T) {
	const accountID = uint32(68)
	selectSQL := "select revision from mo_catalog.mo_query_workload_policy where account_id = 68"
	tests := []struct {
		name          string
		failPrefix    string
		execErrs      map[string]error
		result        ExecResult
		wantErr       string
		wantJoinedErr string
		wantRollback  bool
	}{
		{
			name:       "begin fails",
			failPrefix: "begin;",
			wantErr:    "injected statement failure",
		},
		{
			name:         "upsert fails",
			failPrefix:   "insert into mo_catalog.mo_query_workload_policy",
			wantErr:      "injected statement failure",
			wantRollback: true,
		},
		{
			name: "revision query fails",
			execErrs: map[string]error{
				selectSQL: moerr.NewInternalErrorNoCtx("injected revision query failure"),
			},
			wantErr:      "injected revision query failure",
			wantRollback: true,
		},
		{
			name:         "revision row missing",
			result:       &MysqlResultSet{},
			wantErr:      "did not return one row",
			wantRollback: true,
		},
		{
			name: "multiple revision rows",
			result: func() ExecResult {
				result := &MysqlResultSet{}
				column := &MysqlColumn{}
				column.SetName("revision")
				column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
				result.AddColumn(column)
				result.AddRow([]interface{}{uint64(1)})
				result.AddRow([]interface{}{uint64(2)})
				return result
			}(),
			wantErr:      "did not return one row",
			wantRollback: true,
		},
		{
			name: "invalid revision",
			result: func() ExecResult {
				result := &MysqlResultSet{}
				column := &MysqlColumn{}
				column.SetName("revision")
				column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
				result.AddColumn(column)
				result.AddRow([]interface{}{"not-a-revision"})
				return result
			}(),
			wantErr:      "invalid syntax",
			wantRollback: true,
		},
		{
			name: "zero revision",
			result: func() ExecResult {
				result := &MysqlResultSet{}
				column := &MysqlColumn{}
				column.SetName("revision")
				column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
				result.AddColumn(column)
				result.AddRow([]interface{}{uint64(0)})
				return result
			}(),
			wantErr:      "revision zero",
			wantRollback: true,
		},
		{
			name: "rollback failure is joined",
			execErrs: map[string]error{
				selectSQL:   moerr.NewInternalErrorNoCtx("injected revision query failure"),
				"rollback;": moerr.NewInternalErrorNoCtx("injected rollback failure"),
			},
			wantErr:       "injected revision query failure",
			wantJoinedErr: "injected rollback failure",
			wantRollback:  true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := &backgroundExecTest{}
			base.init()
			for sql, err := range test.execErrs {
				base.sql2err[sql] = err
			}
			if test.result != nil {
				base.sql2result[selectSQL] = test.result
			}

			var exec BackgroundExec = base
			if test.failPrefix != "" {
				exec = &workloadPolicyInjectExec{
					backgroundExecTest: base,
					failPrefix:         test.failPrefix,
					failErr: moerr.NewInternalErrorNoCtx(
						"injected statement failure",
					),
				}
			}
			stub := gostub.StubFunc(&NewBackgroundExec, exec)
			defer stub.Reset()

			_, err := upsertWorkloadPolicy(
				context.Background(),
				&Session{},
				accountID,
				testWorkloadPolicyNew,
				1,
				2,
			)
			require.ErrorContains(t, err, test.wantErr)
			if test.wantJoinedErr != "" {
				require.ErrorContains(t, err, test.wantJoinedErr)
			}
			require.NotEmpty(t, base.executedSQLs)
			if test.wantRollback {
				require.Equal(
					t,
					"rollback;",
					base.executedSQLs[len(base.executedSQLs)-1],
				)
			} else {
				require.Equal(t, []string{"begin;"}, base.executedSQLs)
			}
			require.NotContains(t, base.executedSQLs, "commit;")
		})
	}
}

func BenchmarkWorkloadPolicyCachedRead(b *testing.B) {
	manager := newWorkloadPolicyManager(time.Minute, nil)
	state := manager.acquire(50)
	state.snapshot.Store(&workloadPolicySnapshot{
		raw:      testWorkloadPolicyNew,
		revision: 1,
		set: schedule.WorkloadPolicySet{
			Generation: "benchmark",
		},
	})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkWorkloadPolicySnapshot = manager.cached(state)
	}
}

func BenchmarkWorkloadPolicyStatementFastPath(b *testing.B) {
	manager := newWorkloadPolicyManager(time.Minute, nil)
	state := manager.acquire(54)
	state.snapshot.Store(&workloadPolicySnapshot{
		raw:      testWorkloadPolicyNew,
		revision: 1,
		set: schedule.WorkloadPolicySet{
			Generation: "benchmark",
		},
	})
	state.refreshAfter.Store(time.Now().Add(time.Hour).UnixNano())
	ctx := context.Background()
	ses := &Session{}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := manager.RefreshAsync(ctx, ses.GetService(), 54, state); err != nil {
			b.Fatal(err)
		}
		benchmarkWorkloadPolicySnapshot = manager.cached(state)
	}
}
