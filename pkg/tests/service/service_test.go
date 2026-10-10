// Copyright 2021 - 2022 Matrix Origin
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

package service

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
)

const (
	supportMultiTN = false
)

type lifecycleCN struct {
	cnservice.Service
	closeErr  error
	complete  bool
	closes    int
	starts    int
	taskReads int
}

func (s *lifecycleCN) Close() error {
	s.closes++
	return s.closeErr
}

func (s *lifecycleCN) CloseComplete() bool { return s.complete }
func (s *lifecycleCN) Start() error        { s.starts++; return nil }
func (s *lifecycleCN) GetTaskService() (taskservice.TaskService, bool) {
	s.taskReads++
	return nil, false
}

func TestCNWrapperClosesAcquiredBackendBeforeStart(t *testing.T) {
	failure := moerr.NewInternalErrorNoCtx("CN close incomplete")
	for _, tc := range []struct {
		name         string
		closeErr     error
		complete     bool
		expectStatus ServiceStatus
	}{
		{name: "complete", expectStatus: ServiceClosed},
		{name: "incomplete", closeErr: failure, expectStatus: ServiceInitialized},
		{name: "diagnostic after local close", closeErr: failure, complete: true, expectStatus: ServiceClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &lifecycleCN{closeErr: tc.closeErr, complete: tc.complete}
			owner := &cnService{status: ServiceInitialized, svc: backend, cfg: &cnservice.Config{UUID: t.Name()}}
			require.Equal(t, tc.closeErr, owner.Close())
			require.Equal(t, tc.closeErr, owner.Close())
			require.Equal(t, 1, backend.closes)
			require.Equal(t, tc.expectStatus, owner.Status())
			require.Error(t, owner.Start())
			require.Empty(t, owner.SQLAddress())
			require.Nil(t, owner.GetTaskRunner())
			require.Nil(t, owner.GetSQLExecutor())
			require.Nil(t, owner.GetBootstrapService())
			task, ok := owner.GetTaskService()
			require.Nil(t, task)
			require.False(t, ok)
		})
	}
}

func TestClusterCloseDistinguishesCompletionFromDiagnostics(t *testing.T) {
	cnErr := moerr.NewInternalErrorNoCtx("completed CN diagnostic")
	tnErr := moerr.NewInternalErrorNoCtx("incomplete TN close")
	for _, tc := range []struct {
		name      string
		complete  bool
		tnFailure bool
	}{
		{name: "completed diagnostic", complete: true},
		{name: "incomplete CN"},
		{name: "completed CN then incomplete TN", complete: true, tnFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := &lifecycleCN{closeErr: cnErr, complete: tc.complete}
			next := &lifecycleCN{}
			fs := &trackedFileService{}
			c := &testCluster{logger: zap.NewNop(), stopper: stopper.NewStopper(t.Name()), fileservices: &fileServices{s3FS: fs}}
			c.opt.keepData = true
			c.mu.running = true
			c.cn.svcs = []CNService{&cnService{svc: first, status: ServiceStarted}, &cnService{svc: next, status: ServiceStarted}}
			if tc.tnFailure {
				c.tn.svcs = []TNService{&tnService{svc: &lifecycleTN{closeErr: tnErr}, status: ServiceStarted}}
			}
			require.NoError(t, c.acquireAdmissionLocked())
			t.Cleanup(func() {
				// No live service workers are created by this fixture.
				require.NoError(t, c.releaseAdmissionLocked())
				c.stopper.Stop()
			})
			for attempt := 0; attempt < 2; attempt++ {
				err := c.Close()
				require.ErrorIs(t, err, cnErr)
				require.Equal(t, 1, first.closes)
				if tc.complete {
					require.Equal(t, ServiceClosed, c.cn.svcs[0].Status())
					require.Equal(t, 1, next.closes)
				} else {
					require.Equal(t, ServiceStarted, c.cn.svcs[0].Status())
					require.Zero(t, next.closes)
				}
				if tc.complete && !tc.tnFailure {
					require.Equal(t, 1, fs.closes)
					require.False(t, c.mu.running)
					require.Nil(t, c.mu.admission)
				} else {
					require.Zero(t, fs.closes)
					require.True(t, c.mu.running)
					require.NotNil(t, c.mu.admission)
				}
				if tc.tnFailure {
					require.ErrorIs(t, err, tnErr)
				}
			}
		})
	}
}

type partialBatchHAKeeperClient struct{}

func (partialBatchHAKeeperClient) Close() error { return nil }
func (partialBatchHAKeeperClient) AllocateID(context.Context) (uint64, error) {
	return 1, nil
}
func (partialBatchHAKeeperClient) AllocateIDByKey(context.Context, string) (uint64, error) {
	return 1, nil
}
func (partialBatchHAKeeperClient) AllocateIDByKeyWithBatch(context.Context, string, uint64) (uint64, error) {
	return 1, nil
}
func (partialBatchHAKeeperClient) GetClusterDetails(context.Context) (logpb.ClusterDetails, error) {
	return logpb.ClusterDetails{}, nil
}
func (partialBatchHAKeeperClient) GetClusterState(context.Context) (logpb.CheckerState, error) {
	return logpb.CheckerState{}, nil
}
func (partialBatchHAKeeperClient) CheckLogServiceHealth(context.Context) error { return nil }
func (partialBatchHAKeeperClient) SendTNHeartbeat(context.Context, logpb.TNStoreHeartbeat) (logpb.CommandBatch, error) {
	return logpb.CommandBatch{}, nil
}

func TestClusterAdmissionCoversServiceClusterLifecycle(t *testing.T) {
	c := &testCluster{
		logger:  zap.NewNop(),
		stopper: stopper.NewStopper("cluster-admission-test"),
	}
	c.opt.keepData = true

	require.NoError(t, c.acquireAdmissionLocked())
	require.NotNil(t, c.mu.admission)
	c.mu.running = true
	require.NoError(t, c.Close())
	require.Nil(t, c.mu.admission)
}

func TestInitTNServicesRetainsPublishedOwnersOnPartialBatchFailure(t *testing.T) {
	ctx := context.Background()
	opt := DefaultOptions().WithTNServiceNum(2).WithRootDataDir(t.TempDir())
	opt.validate()
	// Build only the pieces used by initTNServices. NewCluster also builds CN
	// configs, whose process runtime is unrelated to this TN ownership test.
	opt.initial.cnServiceNum = 0
	first := &testCluster{
		t:       t,
		testID:  "partial-tn",
		opt:     opt,
		logger:  zap.NewNop(),
		stopper: stopper.NewStopper("partial-tn"),
	}
	first.clock = clock.NewUnixNanoHLCClockWithStopper(first.stopper, 0)
	first.network.addresses = first.buildServiceAddresses()
	first.tn.cfgs, first.tn.opts = first.buildTNConfigs()
	first.fileservices = first.buildFileServices(ctx)

	t.Cleanup(func() {
		for _, svc := range first.tn.svcs {
			_ = svc.Close()
		}
		for i, fs := range first.fileservices.tnLocalFSs {
			if i == 1 && fs == first.fileservices.s3FS {
				continue
			}
			if fs != nil {
				fs.Close(ctx)
			}
		}
		if first.fileservices.s3FS != nil {
			first.fileservices.s3FS.Close(ctx)
		}
		if first.fileservices.etlFS != nil {
			first.fileservices.etlFS.Close(ctx)
		}
		first.stopper.Stop()
	})
	first.tn.cfgs[0].InStandalone = true
	firstHAKeeper := partialBatchHAKeeperClient{}
	first.tn.opts[0] = append(first.tn.opts[0], tnservice.WithHAKeeperClientFactory(
		func() (logservice.TNHAKeeperClient, error) { return firstHAKeeper, nil },
	))
	for _, cfg := range first.tn.cfgs {
		rt := first.newRuntime(cfg.UUID)
		moruntime.SetupServiceBasedRuntime(cfg.UUID, rt)
	}

	// The second batch item fails while building its file-service graph. The
	// first TN has already published its wrapper, so the caller must retain it
	// for cleanup instead of losing it with a temporary local slice.
	first.fileservices.tnLocalFSs[1] = first.fileservices.s3FS
	err := first.initTNServices(first.fileservices)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrDupServiceName))
	require.Len(t, first.tn.svcs, 1)
	require.Equal(t, first.tn.cfgs[0].UUID, first.tn.svcs[0].ID())
	require.NoError(t, first.tn.svcs[0].Close())
	require.NoError(t, first.tn.svcs[0].Close())
}

func TestSetInitialClusterInfoUsesHAKeeperLeader(t *testing.T) {
	follower := &initialClusterInfoLogService{id: "follower"}
	leader := &initialClusterInfoLogService{id: "leader", leader: true}
	c := &testCluster{
		t:      t,
		logger: zap.NewNop(),
	}
	c.opt.initial.logServiceNum = 2
	c.opt.initial.logShardNum = 3
	c.opt.initial.tnShardNum = 4
	c.opt.initial.logReplicaNum = 5
	c.log.svcs = []LogService{follower, leader}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, c.setInitialClusterInfo(ctx))
	require.NoError(t, c.setInitialClusterInfo(ctx))

	require.Equal(t, 0, follower.initialClusterInfoCalls)
	require.Equal(t, 1, leader.initialClusterInfoCalls)
	require.Equal(t, [3]uint64{3, 4, 5}, leader.initialClusterInfoArgs)
}

func TestSetInitialClusterInfoCanBeCalledAgain(t *testing.T) {
	leader := &initialClusterInfoLogService{id: "leader", leader: true}
	c := &testCluster{
		t:      t,
		logger: zap.NewNop(),
	}
	c.opt.initial.logServiceNum = 1
	c.log.svcs = []LogService{leader}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, c.setInitialClusterInfo(ctx))
	require.NoError(t, c.setInitialClusterInfo(ctx))
	require.Equal(t, 1, leader.initialClusterInfoCalls)
}

type initialClusterInfoLogService struct {
	id                      string
	leader                  bool
	initialClusterInfoCalls int
	initialClusterInfoArgs  [3]uint64
}

func (s *initialClusterInfoLogService) Start() error { return nil }
func (s *initialClusterInfoLogService) Close() error { return nil }
func (s *initialClusterInfoLogService) Status() ServiceStatus {
	return ServiceStarted
}
func (s *initialClusterInfoLogService) ID() string { return s.id }
func (s *initialClusterInfoLogService) IsLeaderHakeeper() (bool, error) {
	return s.leader, nil
}
func (s *initialClusterInfoLogService) GetClusterState() (*logpb.CheckerState, error) {
	return nil, nil
}
func (s *initialClusterInfoLogService) SetInitialClusterInfo(
	numOfLogShards, numOfTNShards, numOfLogReplicas uint64,
) error {
	s.initialClusterInfoCalls++
	s.initialClusterInfoArgs = [3]uint64{numOfLogShards, numOfTNShards, numOfLogReplicas}
	return nil
}
func (s *initialClusterInfoLogService) StartHAKeeperReplica(
	replicaID uint64, initialReplicas map[uint64]dragonboat.Target, join bool,
) error {
	return nil
}
func (s *initialClusterInfoLogService) GetTaskService() (taskservice.TaskService, bool) {
	return nil, false
}

func TestClusterStart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	// initialize cluster
	c, err := NewCluster(ctx, t, DefaultOptions())
	require.NoError(t, err)
	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())
}

func TestAllocateID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	// initialize cluster
	c, err := NewCluster(ctx, t, DefaultOptions())
	require.NoError(t, err)

	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	c.WaitHAKeeperState(ctx, logpb.HAKeeperRunning)

	cfg := logservice.HAKeeperClientConfig{
		ServiceAddresses: []string{c.(*testCluster).network.addresses.logAddresses[0].listenAddr},
		AllocateIDBatch:  10,
	}
	hc, err := logservice.NewCNHAKeeperClient(ctx, "", cfg)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, hc.Close())
	}()

	last := uint64(0)
	for i := 0; i < int(cfg.AllocateIDBatch)-1; i++ {
		v, err := hc.AllocateID(ctx)
		require.NoError(t, err)
		assert.True(t, v > 0)
		if last != 0 {
			assert.Equal(t, v, last+1, i)
		}
		last = v
	}
}

func TestAllocateIDByKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	// initialize cluster
	c, err := NewCluster(ctx, t, DefaultOptions())
	require.NoError(t, err)

	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	c.WaitHAKeeperState(ctx, logpb.HAKeeperRunning)

	cfg := logservice.HAKeeperClientConfig{
		ServiceAddresses: []string{c.(*testCluster).network.addresses.logAddresses[0].listenAddr},
		AllocateIDBatch:  10,
	}
	hc, err := logservice.NewCNHAKeeperClient(ctx, "", cfg)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, hc.Close())
	}()

	last := uint64(0)
	for i := 0; i < int(cfg.AllocateIDBatch)-1; i++ {
		v, err := hc.AllocateIDByKey(ctx, "k1")
		require.NoError(t, err)
		assert.True(t, v > 0)
		if last != 0 {
			assert.Equal(t, v, last+1, i)
		}
		last = v
	}
	v2, err := hc.AllocateIDByKey(ctx, "k2")
	require.NoError(t, err)
	assert.Equal(t, v2, uint64(1))
	v3, err := hc.AllocateIDByKey(ctx, "k3")
	require.NoError(t, err)
	assert.Equal(t, v3, uint64(1))
}

func TestClusterAwareness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	if !supportMultiTN {
		t.Skip("skipping, multi db not support")
		return
	}

	tnSvcNum := 2
	logSvcNum := 3
	opt := DefaultOptions().
		WithTNServiceNum(tnSvcNum).
		WithLogServiceNum(logSvcNum)

	// initialize cluster
	c, err := NewCluster(ctx, t, opt)
	require.NoError(t, err)

	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())

	// -------------------------------------------
	// the following would test `ClusterAwareness`
	// -------------------------------------------
	dsuuids := c.ListTNServices()
	require.Equal(t, tnSvcNum, len(dsuuids))

	lsuuids := c.ListLogServices()
	require.Equal(t, logSvcNum, len(lsuuids))

	hksvcs := c.ListHAKeeperServices()
	require.NotZero(t, len(hksvcs))

	tn, err := c.GetTNService(dsuuids[0])
	require.NoError(t, err)
	require.Equal(t, ServiceStarted, tn.Status())

	log, err := c.GetLogService(lsuuids[0])
	require.NoError(t, err)
	require.Equal(t, ServiceStarted, log.Status())

	ctx1, cancel1 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel1()
	leader := c.WaitHAKeeperLeader(ctx1)
	require.NotNil(t, leader)

	// we must wait for hakeeper's running state, or hakeeper wouldn't receive hearbeat.
	ctx2, cancel2 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel2()
	c.WaitHAKeeperState(ctx2, logpb.HAKeeperRunning)

	ctx3, cancel3 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel3()
	state, err := c.GetClusterState(ctx3)
	require.NoError(t, err)
	require.Equal(t, tnSvcNum, len(state.TNState.Stores))
	require.Equal(t, logSvcNum, len(state.LogState.Stores))
}

func TestClusterOperation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	if !supportMultiTN {
		t.Skip("skipping, multi db not support")
		return
	}

	tnSvcNum := 3
	logSvcNum := 3
	opt := DefaultOptions().
		WithTNServiceNum(tnSvcNum).
		WithLogServiceNum(logSvcNum)

	// initialize cluster
	c, err := NewCluster(ctx, t, opt)
	require.NoError(t, err)

	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())

	// -------------------------------------------
	// the following would test `ClusterOperation`
	// -------------------------------------------

	// 1. start/close tn services via different ways
	dsuuids := c.ListTNServices()
	require.Equal(t, tnSvcNum, len(dsuuids))
	// 1.a start/close tn service by uuid
	{
		index := 0
		dsuuid := dsuuids[index]

		// get the instance of tn service
		ds, err := c.GetTNService(dsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// start it
		err = c.StartTNService(dsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// close it
		err = c.CloseTNService(dsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ds.Status())
	}

	// 1.b start/close tn service by index
	{
		index := 1

		// get the instance of tn service
		ds, err := c.GetTNServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// start it
		err = c.StartTNServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// close it
		err = c.CloseTNServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ds.Status())
	}

	// 1.c start/close tn service by instance
	{
		index := 2

		// get the instance of tn service
		ds, err := c.GetTNServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// start it
		err = ds.Start()
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// close it
		err = ds.Close()
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ds.Status())
	}

	// 2. start/close log services by different ways
	lsuuids := c.ListLogServices()
	require.Equal(t, logSvcNum, len(lsuuids))
	// 2.a start/close log service by uuid
	{
		index := 0
		lsuuid := lsuuids[index]

		// get the instance of log service
		ls, err := c.GetLogService(lsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// start it
		err = c.StartLogService(lsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// close it
		err = c.CloseLogService(lsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ls.Status())
	}

	// 2.b start/close log service by index
	{
		index := 1

		// get the instance of log service
		ls, err := c.GetLogServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// start it
		err = c.StartLogServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// close it
		err = c.CloseLogServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ls.Status())
	}

	// 2.c start/close log service by instance
	{
		index := 2

		// get the instance of log service
		ls, err := c.GetLogServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// start it
		err = ls.Start()
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// close it
		err = ls.Close()
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ls.Status())
	}
}

func TestClusterState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	if !supportMultiTN {
		t.Skip("skipping, multi db not support")
		return
	}

	tnSvcNum := 2
	logSvcNum := 3
	opt := DefaultOptions().
		WithTNServiceNum(tnSvcNum).
		WithLogServiceNum(logSvcNum)

	// initialize cluster
	c, err := NewCluster(ctx, t, opt)
	require.NoError(t, err)

	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())

	// ----------------------------------------
	// the following would test `ClusterState`.
	// ----------------------------------------
	ctx1, cancel1 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel1()
	leader := c.WaitHAKeeperLeader(ctx1)
	require.NotNil(t, leader)

	dsuuids := c.ListTNServices()
	require.Equal(t, tnSvcNum, len(dsuuids))

	lsuuids := c.ListLogServices()
	require.Equal(t, logSvcNum, len(lsuuids))

	// we must wait for hakeeper's running state, or hakeeper wouldn't receive hearbeat.
	ctx2, cancel2 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel2()
	c.WaitHAKeeperState(ctx2, logpb.HAKeeperRunning)

	hkstate := c.GetHAKeeperState()
	require.Equal(t, logpb.HAKeeperRunning, hkstate)

	// cluster should be healthy
	require.True(t, c.IsClusterHealthy())

	ctx3, cancel3 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel3()
	state, err := c.GetClusterState(ctx3)
	require.NoError(t, err)
	require.Equal(t, tnSvcNum, len(state.TNState.Stores))
	require.Equal(t, logSvcNum, len(state.LogState.Stores))

	// FIXME: validate the result list of tn shards
	ctx4, cancel4 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel4()
	_, err = c.ListTNShards(ctx4)
	require.NoError(t, err)

	// FIXME: validate the result list of log shards
	ctx5, cancel5 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel5()
	_, err = c.ListLogShards(ctx5)
	require.NoError(t, err)

	// test for:
	//   - GetDNStoreInfo
	//   - GetDNStoreInfoIndexed
	//   - DNStoreExpired
	//   - DNStoreExpiredIndexed
	{
		tnIndex := 0
		dsuuid := dsuuids[tnIndex]

		ctx6, cancel6 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel6()
		tnStoreInfo1, err := c.GetTNStoreInfo(ctx6, dsuuid)
		require.NoError(t, err)

		ctx7, cancel7 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel7()
		tnStoreInfo2, err := c.GetTNStoreInfoIndexed(ctx7, tnIndex)
		require.NoError(t, err)
		require.Equal(t, tnStoreInfo1.Shards, tnStoreInfo2.Shards)

		expired1, err := c.TNStoreExpired(dsuuid)
		require.NoError(t, err)
		require.False(t, expired1)

		expired2, err := c.TNStoreExpiredIndexed(tnIndex)
		require.NoError(t, err)
		require.False(t, expired2)
	}

	// test for:
	//   - GetLogStoreInfo
	//   - GetLogStoreInfoIndexed
	//   - LogStoreExpired
	//   - LogStoreExpiredIndexed
	{
		logIndex := 1
		lsuuid := lsuuids[logIndex]

		ctx8, cancel8 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel8()
		logStoreInfo1, err := c.GetLogStoreInfo(ctx8, lsuuid)
		require.NoError(t, err)

		ctx9, cancel9 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel9()
		logStoreInfo2, err := c.GetLogStoreInfoIndexed(ctx9, logIndex)
		require.NoError(t, err)
		require.Equal(t, len(logStoreInfo1.Replicas), len(logStoreInfo2.Replicas)) // TODO: sort and compare detail.

		expired1, err := c.LogStoreExpired(lsuuid)
		require.NoError(t, err)
		require.False(t, expired1)

		expired2, err := c.LogStoreExpiredIndexed(logIndex)
		require.NoError(t, err)
		require.False(t, expired2)
	}
}

func TestClusterWaitState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	if !supportMultiTN {
		t.Skip("skipping, multi db not support")
		return
	}

	tnSvcNum := 2
	logSvcNum := 3
	opt := DefaultOptions().
		WithTNServiceNum(tnSvcNum).
		WithLogServiceNum(logSvcNum)

	// initialize cluster
	c, err := NewCluster(ctx, t, opt)
	require.NoError(t, err)

	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())

	// we must wait for hakeeper's running state, or hakeeper wouldn't receive hearbeat.
	ctx1, cancel1 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel1()
	c.WaitHAKeeperState(ctx1, logpb.HAKeeperRunning)

	// --------------------------------------------
	// the following would test `ClusterWaitState`.
	// --------------------------------------------

	// test WaitDNShardsReported
	{
		ctx2, cancel2 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel2()
		c.WaitTNShardsReported(ctx2)
	}

	// test WaitLogShardsReported
	{
		ctx3, cancel3 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel3()
		c.WaitLogShardsReported(ctx3)
	}

	// test WaitDNReplicaReported
	{
		ctx4, cancel4 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel4()
		tnShards, err := c.ListTNShards(ctx4)
		require.NoError(t, err)
		require.NotZero(t, len(tnShards))

		tnShardID := tnShards[0].ShardID
		ctx5, cancel5 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel5()
		c.WaitTNReplicaReported(ctx5, tnShardID)
	}

	// test WaitLogReplicaReported
	{
		ctx6, cancel6 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel6()
		logShards, err := c.ListLogShards(ctx6)
		require.NotZero(t, len(logShards))
		require.NoError(t, err)

		logShardID := logShards[0].ShardID
		ctx7, cancel7 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel7()
		c.WaitLogReplicaReported(ctx7, logShardID)
	}
}

func TestNetworkPartition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	if !supportMultiTN {
		t.Skip("skipping, multi db not support")
		return
	}

	tnSvcNum := 2
	logSvcNum := 4
	opt := DefaultOptions().
		WithTNServiceNum(tnSvcNum).
		WithLogServiceNum(logSvcNum)

	// initialize cluster
	c, err := NewCluster(ctx, t, opt)
	require.NoError(t, err)

	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())

	// we must wait for hakeeper's running state, or hakeeper wouldn't receive hearbeat.
	ctx1, cancel1 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel1()
	c.WaitHAKeeperState(ctx1, logpb.HAKeeperRunning)

	// --------------------------------------------
	// the following would test network partition
	// --------------------------------------------

	// tn service index: 0, 1
	// log service index: 0, 1, 2, 3
	// separate tn service 1 from other services
	partition1 := c.NewNetworkPartition([]uint32{1}, nil, nil)
	require.Equal(t, []uint32{1}, partition1.ListTNServiceIndex())
	require.Nil(t, partition1.ListLogServiceIndex())

	partition2 := c.RemainingNetworkPartition(partition1)
	require.Equal(t, []uint32{0}, partition2.ListTNServiceIndex())
	require.Equal(t, []uint32{0, 1, 2, 3}, partition2.ListLogServiceIndex())

	// enable network partition
	c.StartNetworkPartition(partition1, partition2)
	ctx2, cancel2 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel2()
	c.WaitTNStoreTimeoutIndexed(ctx2, 1)

	// disable network partition
	c.CloseNetworkPartition()
	ctx3, cancel3 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel3()
	c.WaitTNStoreReportedIndexed(ctx3, 1)

	// tn service index: 0, 1
	// log service index: 0, 1, 2, 3
	// separate log service 3 from other services
	partition3 := c.NewNetworkPartition(nil, []uint32{3}, nil)
	require.Nil(t, partition3.ListTNServiceIndex())
	require.Equal(t, []uint32{3}, partition3.ListLogServiceIndex())

	partition4 := c.RemainingNetworkPartition(partition3)
	require.Equal(t, []uint32{0, 1}, partition4.ListTNServiceIndex())
	require.Equal(t, []uint32{0, 1, 2}, partition4.ListLogServiceIndex())

	// enable network partition
	c.StartNetworkPartition(partition3, partition4)
	ctx4, cancel4 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel4()
	c.WaitLogStoreTimeoutIndexed(ctx4, 3)

	// disable network partition
	c.CloseNetworkPartition()
	ctx5, cancel5 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel5()
	c.WaitLogStoreReportedIndexed(ctx5, 3)
}

// Pause the actual constructor before backend acquisition, without live workers.
type blockedMetadataFS struct {
	fileservice.ReplaceableFileService
	entered, release chan struct{}
}

func (f *blockedMetadataFS) Name() string {
	close(f.entered)
	<-f.release
	return "missing-local-service"
}
func TestServicePublicationWaitsForConstruction(t *testing.T) {
	for _, kind := range []string{"CN", "TN"} {
		t.Run(kind, func(t *testing.T) {
			id := "publication-" + kind
			moruntime.SetupServiceBasedRuntime(id, moruntime.DefaultRuntime())
			fs := &blockedMetadataFS{entered: make(chan struct{}), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(fs.release) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			constructed := make(chan error, 1)
			c := &testCluster{}
			go func() {
				var err error
				if kind == "CN" {
					_, err = newCNService(&cnservice.Config{UUID: id}, ctx, cancel, fs,
						func(owner CNService) { c.cn.Lock(); c.cn.svcs = append(c.cn.svcs, owner); c.cn.Unlock() }, nil)
				} else {
					_, err = newTNService(&tnservice.Config{UUID: id}, moruntime.DefaultRuntime(), fs,
						func(owner TNService) { c.tn.Lock(); c.tn.svcs = append(c.tn.svcs, owner); c.tn.Unlock() }, nil)
				}
				constructed <- err
			}()
			t.Cleanup(func() {
				release()
				select {
				case err := <-constructed:
					require.Error(t, err)
				case <-time.After(5 * time.Second):
					t.Error("constructor did not retire")
				}
			})
			select {
			case <-fs.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("constructor did not reach metadata boundary")
			}
			var owner interface {
				Status() ServiceStatus
				Close() error
			}
			var gate interface {
				TryLock() bool
				Unlock()
			}
			started := make(chan error, 1)
			closed := make(chan error, 1)
			available := make(chan bool, 1)
			if kind == "CN" {
				cn, err := c.GetCNServiceIndexed(0)
				require.NoError(t, err)
				owner, gate = cn, cn.(*cnService)
				go func() { started <- c.StartCNServiceIndexed(0) }()
				go func() { closed <- c.CloseCNServiceIndexed(0) }()
				go func() { available <- cn.SQLAddress() != "" }()
			} else {
				tn, err := c.GetTNServiceIndexed(0)
				require.NoError(t, err)
				owner, gate = tn, tn.(*tnService)
				go func() { started <- c.StartTNServiceIndexed(0) }()
				go func() { closed <- c.CloseTNServiceIndexed(0) }()
				go func() { _, ok := tn.GetTaskService(); available <- ok }()
			}
			// The real constructor holds the gate independently of scheduler timing.
			if gate.TryLock() {
				gate.Unlock()
				t.Fatal("operational gate released during construction")
			}
			select {
			case <-started:
				t.Fatal("Start escaped the construction gate")
			case <-closed:
				t.Fatal("Close escaped the construction gate")
			case <-available:
				t.Fatal("getter escaped the construction gate")
			case <-time.After(50 * time.Millisecond):
			}
			release()
			select {
			case err := <-started:
				require.Error(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Start did not retire")
			}
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not retire")
			}
			select {
			case got := <-available:
				require.False(t, got)
			case <-time.After(5 * time.Second):
				t.Fatal("getter did not retire")
			}
			require.Equal(t, ServiceClosed, owner.Status())
			if kind == "CN" {
				require.Error(t, ctx.Err(), "failure must cancel constructor context before handing off")
			}
			require.NoError(t, owner.Close())
		})
	}
}

func TestServiceLookupBeforePublication(t *testing.T) {
	c := &testCluster{}
	c.tn.cfgs = []*tnservice.Config{{UUID: "not-published"}}
	_, err := c.GetTNService("not-published")
	require.Error(t, err)
	require.Equal(t, []string{"not-published"}, c.ListTNServices())
	require.Error(t, c.StartCNServices(1))
}

func TestConstructorPublicationUnwind(t *testing.T) {
	for _, kind := range []string{"CN", "TN"} {
		for _, abnormal := range []string{"panic", "Goexit"} {
			t.Run(kind+"/"+abnormal, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan struct{})
				var owner interface {
					Start() error
					Close() error
					Status() ServiceStatus
				}
				fail := func() {
					if abnormal == "Goexit" {
						runtime.Goexit()
					}
					panic("publication failure")
				}
				go func() {
					defer close(done)
					defer func() { _ = recover() }()
					if kind == "CN" {
						_, _ = newCNService(&cnservice.Config{UUID: t.Name()}, ctx, cancel, nil,
							func(s CNService) { owner = s; fail() }, nil)
					} else {
						_, _ = newTNService(&tnservice.Config{UUID: t.Name()}, nil, nil,
							func(s TNService) { owner = s; fail() }, nil)
					}
				}()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("constructor unwind did not finish")
				}
				require.NotNil(t, owner)
				require.Equal(t, ServiceClosed, owner.Status())
				require.Error(t, owner.Start())
				require.NoError(t, owner.Close())
				if kind == "CN" {
					require.Error(t, ctx.Err())
				}
			})
		}
	}
}

func TestPublishedWrappersPreserveHealthyOperations(t *testing.T) {
	cn := &lifecycleCN{}
	tn := &lifecycleTN{}
	c := &testCluster{}
	c.cn.svcs = []CNService{&cnService{status: ServiceInitialized, svc: cn, cfg: &cnservice.Config{UUID: "cn"}}}
	c.tn.svcs = []TNService{&tnService{status: ServiceInitialized, svc: tn, uuid: "tn"}}
	t.Cleanup(func() { _ = c.CloseCNServiceIndexed(0); _ = c.CloseTNServiceIndexed(0) })
	for i := 0; i < 2; i++ {
		require.NoError(t, c.StartCNService("cn"))
		require.NoError(t, c.StartTNService("tn"))
	}
	require.Equal(t, 1, cn.starts)
	require.Equal(t, 1, tn.starts)
	require.Equal(t, []string{"cn"}, c.ListCnServices())
	owner, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:0", owner.SQLAddress())
	_, _ = owner.GetTaskService()
	require.Equal(t, 1, cn.taskReads)
	require.NoError(t, c.CloseCNService("cn"))
	_, ok := owner.GetTaskService()
	require.False(t, ok)
	require.Equal(t, 1, cn.taskReads, "retired wrapper must not consult backend")
}
