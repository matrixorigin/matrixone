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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/syshealth"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	defaultWaitInterval = 100 * time.Millisecond
	defaultTestTimeout  = 3 * time.Minute
)

// Cluster describes behavior of test framework.
type Cluster interface {
	// Start starts svcs sequentially, after start, system init is completed.
	Start() error
	// Close stops svcs sequentially
	Close() error
	// Options returns the adjusted options
	Options() Options
	// Clock get cluster clock
	Clock() clock.Clock

	ClusterOperation
	ClusterAwareness
	ClusterState
	ClusterWaitState
}

// ClusterOperation supports kinds of cluster operations.
type ClusterOperation interface {
	// CloseTNService closes tn service by uuid.
	CloseTNService(uuid string) error
	// StartTNService starts tn service by uuid.
	StartTNService(uuid string) error

	// CloseTNServiceIndexed closes tn service by its index.
	CloseTNServiceIndexed(index int) error
	// StartTNServiceIndexed starts tn service by its index.
	StartTNServiceIndexed(index int) error

	// CloseLogService closes log service by uuid.
	CloseLogService(uuid string) error
	// StartLogService starts log service by uuid.
	StartLogService(uuid string) error

	// CloseLogServiceIndexed closes log service by its index.
	CloseLogServiceIndexed(index int) error
	// StartLogServiceIndexed starts log service by its index.
	StartLogServiceIndexed(index int) error

	// CloseCNService closes cn service by uuid.
	CloseCNService(uuid string) error
	// StartCNService starts cn service by uuid.
	StartCNService(uuid string) error

	// CloseCNServiceIndexed closes cn service by its index.
	CloseCNServiceIndexed(index int) error
	// StartCNServiceIndexed starts cn service by its index.
	StartCNServiceIndexed(index int) error

	// StartCNServices start number of cn services.
	StartCNServices(n int) error

	// NewNetworkPartition constructs network partition from service index.
	NewNetworkPartition(tnIndexes, logIndexes, cnIndexes []uint32) NetworkPartition
	// RemainingNetworkPartition returns partition for the remaining services.
	RemainingNetworkPartition(partitions ...NetworkPartition) NetworkPartition
	// StartNetworkPartition enables network partition feature.
	StartNetworkPartition(partitions ...NetworkPartition)
	// CloseNetworkPartition disables network partition feature.
	CloseNetworkPartition()
}

// ClusterAwareness provides cluster awareness information.
type ClusterAwareness interface {
	// ListTNServices lists uuid of all tn services.
	ListTNServices() []string
	// ListLogServices lists uuid of all log services.
	ListLogServices() []string
	// ListCnServices lists uuid of all cn services.
	ListCnServices() []string
	// ListHAKeeperServices lists all hakeeper log services.
	ListHAKeeperServices() []LogService

	// GetTNService fetches tn service instance by uuid.
	GetTNService(uuid string) (TNService, error)
	// GetLogService fetches log service instance by index.
	GetLogService(uuid string) (LogService, error)
	// GetTNServiceIndexed fetches tn service instance by uuid.
	GetTNServiceIndexed(index int) (TNService, error)
	// GetLogServiceIndexed fetches log service instance by index.
	GetLogServiceIndexed(index int) (LogService, error)
	// GetCNService fetches cn service instance by index.
	GetCNService(uuid string) (CNService, error)
	// GetCNServiceIndexed fetches cn service instance by index.
	GetCNServiceIndexed(index int) (CNService, error)

	// GetClusterState fetches current cluster state
	GetClusterState(ctx context.Context) (*logpb.CheckerState, error)
}

// ClusterState provides cluster running state.
type ClusterState interface {
	// ListTNShards lists all tn shards within the cluster.
	ListTNShards(ctx context.Context) ([]metadata.TNShardRecord, error)
	// ListLogShards lists all log shards within the cluster.
	ListLogShards(ctx context.Context) ([]metadata.LogShardRecord, error)

	// GetTNStoreInfo gets tn store information by uuid.
	GetTNStoreInfo(ctx context.Context, uuid string) (logpb.TNStoreInfo, error)
	// GetTNStoreInfoIndexed gets tn store information by index.
	GetTNStoreInfoIndexed(ctx context.Context, index int) (logpb.TNStoreInfo, error)

	// GetLogStoreInfo gets log store information by uuid.
	GetLogStoreInfo(ctx context.Context, uuid string) (logpb.LogStoreInfo, error)
	// GetLogStoreInfoIndexed gets log store information by index.
	GetLogStoreInfoIndexed(ctx context.Context, index int) (logpb.LogStoreInfo, error)

	// GetCNStoreInfo gets cn store information by uuid.
	GetCNStoreInfo(ctx context.Context, uuid string) (logpb.CNStoreInfo, error)
	// GetCNStoreInfoIndexed gets cn store information by index.
	GetCNStoreInfoIndexed(ctx context.Context, index int) (logpb.CNStoreInfo, error)

	// GetHAKeeperState returns hakeeper state from running hakeeper.
	GetHAKeeperState() logpb.HAKeeperState
	// GetHAKeeperConfig returns hakeeper configuration.
	GetHAKeeperConfig() hakeeper.Config

	// TNStoreExpired checks tn store expired or not by uuid.
	TNStoreExpired(uuid string) (bool, error)
	// TNStoreExpiredIndexed checks tn store expired or not by index.
	TNStoreExpiredIndexed(index int) (bool, error)
	// LogStoreExpired checks log store expired or not by uuid.
	LogStoreExpired(uuid string) (bool, error)
	// LogStoreExpiredIndexed checks log store expired or not by index.
	LogStoreExpiredIndexed(index int) (bool, error)
	// CNStoreExpired checks cn store expired or not by uuid.
	CNStoreExpired(uuid string) (bool, error)
	// CNStoreExpiredIndexed checks cn store expired or not by index.
	CNStoreExpiredIndexed(index int) (bool, error)

	// IsClusterHealthy checks whether cluster is healthy or not.
	IsClusterHealthy() bool
}

// ClusterWaitState waits cluster state until timeout.
type ClusterWaitState interface {
	// WaitHAKeeperLeader waits hakeeper leader elected and return it.
	WaitHAKeeperLeader(ctx context.Context) LogService
	// WaitHAKeeperState waits the specific hakeeper state.
	WaitHAKeeperState(ctx context.Context, expected logpb.HAKeeperState)

	// WaitTNShardsReported waits the expected count of tn shards reported.
	WaitTNShardsReported(ctx context.Context)
	// WaitLogShardsReported waits the expected count of log shards reported.
	WaitLogShardsReported(ctx context.Context)
	// WaitTNReplicaReported waits tn replica reported.
	WaitTNReplicaReported(ctx context.Context, shardID uint64)
	// WaitLogReplicaReported waits log replicas reported.
	WaitLogReplicaReported(ctx context.Context, shardID uint64)

	// WaitTNStoreTimeout waits tn store timeout by uuid.
	WaitTNStoreTimeout(ctx context.Context, uuid string)
	// WaitTNStoreTimeoutIndexed waits tn store timeout by index.
	WaitTNStoreTimeoutIndexed(ctx context.Context, index int)
	// WaitTNStoreReported waits tn store reported by uuid.
	WaitTNStoreReported(ctx context.Context, uuid string)
	// WaitTNStoreReportedIndexed waits tn store reported by index.
	WaitTNStoreReportedIndexed(ctx context.Context, index int)
	// WaitTNStoreTaskServiceCreated waits tn store task service started by uuid.
	WaitTNStoreTaskServiceCreated(ctx context.Context, uuid string)
	// WaitTNStoreTaskServiceCreatedIndexed waits tn store task service started by index.
	WaitTNStoreTaskServiceCreatedIndexed(ctx context.Context, index int)
	// WaitCNStoreReported waits cn store reported by uuid.
	WaitCNStoreReported(ctx context.Context, uuid string)
	// WaitCNStoreReportedIndexed waits cn store reported by index.
	WaitCNStoreReportedIndexed(ctx context.Context, index int)
	// WaitCNStoreTaskServiceCreated waits cn store task service started by uuid.
	WaitCNStoreTaskServiceCreated(ctx context.Context, uuid string)
	// WaitCNStoreTaskServiceCreatedIndexed waits cn store task service started by index.
	WaitCNStoreTaskServiceCreatedIndexed(ctx context.Context, index int)
	// WaitLogStoreTaskServiceCreated waits log store task service started by uuid
	WaitLogStoreTaskServiceCreated(ctx context.Context, uuid string)
	// WaitLogStoreTaskServiceCreatedIndexed waits log store task service started by index
	WaitLogStoreTaskServiceCreatedIndexed(ctx context.Context, index int)

	// WaitLogStoreTimeout waits log store timeout by uuid.
	WaitLogStoreTimeout(ctx context.Context, uuid string)
	// WaitLogStoreTimeoutIndexed waits log store timeout by index.
	WaitLogStoreTimeoutIndexed(ctx context.Context, index int)
	// WaitLogStoreReported waits log store reported by uuid.
	WaitLogStoreReported(ctx context.Context, uuid string)
	// WaitLogStoreReportedIndexed waits log store reported by index.
	WaitLogStoreReportedIndexed(ctx context.Context, index int)
}

// ----------------------------------------------------
// The following are implements for interface `Cluster`.
// ----------------------------------------------------

// testCluster simulates a cluster with tn and log service.
type testCluster struct {
	t       *testing.T
	testID  string
	opt     Options
	logger  *zap.Logger
	stopper *stopper.Stopper
	clock   clock.Clock

	tn struct {
		sync.Mutex
		cfgs []*tnservice.Config
		opts []tnOptions
		svcs []TNService
	}

	log struct {
		once sync.Once

		sync.Mutex
		cfgs []logservice.Config
		opts []logOptions
		svcs []LogService
	}

	cn struct {
		sync.Mutex
		cfgs []*cnservice.Config
		opts []cnOptions
		svcs []CNService
	}

	network struct {
		addresses *serviceAddresses

		sync.RWMutex
		addressSets []addressSet
	}

	fileservices *fileServices

	mu struct {
		sync.Mutex
		running bool
	}
}

// NewCluster construct a cluster for integration test.
func NewCluster(ctx context.Context, t *testing.T, opt Options) (Cluster, error) {
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:  "fatal",
		Format: "console",
	})
	opt.validate()

	uid, _ := uuid.NewV7()
	c := &testCluster{
		t:       t,
		testID:  uid.String(),
		opt:     opt,
		stopper: stopper.NewStopper("test-cluster"),
	}
	c.logger = logutil.Adjust(opt.logger).With(zap.String("testcase", t.Name())).With(zap.String("test-id", c.testID))
	c.opt.rootDataDir = filepath.Join(c.opt.rootDataDir, c.testID, t.Name())
	if c.clock == nil {
		c.clock = clock.NewUnixNanoHLCClockWithStopper(c.stopper, 0)
	}

	// TODO: CN and LOG use process level runtime
	runtime.SetupProcessLevelRuntime(c.newRuntime())

	// build addresses for all services
	c.network.addresses = c.buildServiceAddresses()
	// build log service configurations
	c.log.cfgs, c.log.opts = c.buildLogConfigs()
	// build tn service configurations
	c.tn.cfgs, c.tn.opts = c.buildTNConfigs()

	// build FileService instances
	c.fileservices = c.buildFileServices(ctx)

	// build cn service configurations
	c.buildCNConfigs(c.opt.initial.cnServiceNum)
	return c, nil
}

func (c *testCluster) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.running {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	c.mu.running = true
	// start log services first
	if err := c.startLogServices(ctx); err != nil {
		return err
	}

	// start tn services
	if err := c.startTNServices(ctx); err != nil {
		return err
	}

	// start cn services
	if err := c.startCNServices(ctx); err != nil {
		return err
	}

	return nil
}

func (c *testCluster) Options() Options {
	return c.opt
}

func (c *testCluster) Clock() clock.Clock {
	return c.clock
}

func (c *testCluster) Close() error {
	defer logutil.LogClose(c.logger, "tests-framework")()
	c.logger.Info("closing testCluster")

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.mu.running {
		return nil
	}

	// close all cn services first
	if err := c.closeCNServices(); err != nil {
		return err
	}

	// close all tn services
	if err := c.closeTNServices(); err != nil {
		return err
	}

	// close all log services
	if err := c.closeLogServices(); err != nil {
		return err
	}

	c.mu.running = false
	c.stopper.Stop()

	if !c.opt.keepData {
		if err := os.RemoveAll(c.opt.rootDataDir); err != nil {
			return err
		}
	}
	return nil
}

// ----------------------------------------------------------
// The following are implements for interface `ClusterState`.
// ----------------------------------------------------------
func (c *testCluster) ListTNShards(
	ctx context.Context,
) ([]metadata.TNShardRecord, error) {
	state, err := c.GetClusterState(ctx)
	if err != nil {
		return nil, err
	}
	return state.ClusterInfo.TNShards, nil
}

func (c *testCluster) ListLogShards(
	ctx context.Context,
) ([]metadata.LogShardRecord, error) {
	state, err := c.GetClusterState(ctx)
	if err != nil {
		return nil, err
	}
	return state.ClusterInfo.LogShards, nil
}

func (c *testCluster) GetTNStoreInfo(
	ctx context.Context, uuid string,
) (logpb.TNStoreInfo, error) {
	state, err := c.GetClusterState(ctx)
	if err != nil {
		return logpb.TNStoreInfo{}, err
	}
	stores := state.TNState.Stores
	if storeInfo, ok := stores[uuid]; ok {
		return storeInfo, nil
	}
	return logpb.TNStoreInfo{}, moerr.NewNoService(ctx, uuid)
}

func (c *testCluster) GetTNStoreInfoIndexed(
	ctx context.Context, index int,
) (logpb.TNStoreInfo, error) {
	ds, err := c.GetTNServiceIndexed(index)
	if err != nil {
		return logpb.TNStoreInfo{}, err
	}
	return c.GetTNStoreInfo(ctx, ds.ID())
}

func (c *testCluster) GetLogStoreInfo(
	ctx context.Context, uuid string,
) (logpb.LogStoreInfo, error) {
	state, err := c.GetClusterState(ctx)
	if err != nil {
		return logpb.LogStoreInfo{}, err
	}
	stores := state.LogState.Stores
	if storeInfo, ok := stores[uuid]; ok {
		return storeInfo, nil
	}
	return logpb.LogStoreInfo{}, moerr.NewNoService(ctx, uuid)
}

func (c *testCluster) GetLogStoreInfoIndexed(
	ctx context.Context, index int,
) (logpb.LogStoreInfo, error) {
	ls, err := c.GetLogServiceIndexed(index)
	if err != nil {
		return logpb.LogStoreInfo{}, err
	}
	return c.GetLogStoreInfo(ctx, ls.ID())
}

func (c *testCluster) GetCNStoreInfo(ctx context.Context, uuid string) (logpb.CNStoreInfo, error) {
	state, err := c.GetClusterState(ctx)
	if err != nil {
		return logpb.CNStoreInfo{}, err
	}
	stores := state.CNState.Stores
	if storeInfo, ok := stores[uuid]; ok {
		return storeInfo, nil
	}
	return logpb.CNStoreInfo{}, moerr.NewNoService(ctx, uuid)
}

func (c *testCluster) GetCNStoreInfoIndexed(ctx context.Context, index int) (logpb.CNStoreInfo, error) {
	ls, err := c.GetCNServiceIndexed(index)
	if err != nil {
		return logpb.CNStoreInfo{}, err
	}
	return c.GetCNStoreInfo(ctx, ls.ID())
}

func (c *testCluster) GetHAKeeperState() logpb.HAKeeperState {
	state := c.getClusterState()
	require.NotNil(c.t, state)
	return state.State
}

func (c *testCluster) GetHAKeeperConfig() hakeeper.Config {
	return c.opt.BuildHAKeeperConfig()
}

func (c *testCluster) TNStoreExpired(uuid string) (bool, error) {
	state := c.getClusterState()
	require.NotNil(c.t, state)

	tnStore, ok := state.TNState.Stores[uuid]
	if !ok {
		return false, moerr.NewShardNotReportedNoCtx(uuid, 0xDEADBEEF)
	}

	hkcfg := c.GetHAKeeperConfig()
	expired := hkcfg.TNStoreExpired(tnStore.Tick, state.Tick)

	c.logger.Info(
		"check tn store expired or not",
		zap.Any("hakeeper config", hkcfg),
		zap.Uint64("dn store tick", tnStore.Tick),
		zap.Uint64("current tick", state.Tick),
		zap.Bool("expired", expired),
	)

	return expired, nil
}

func (c *testCluster) TNStoreExpiredIndexed(index int) (bool, error) {
	ds, err := c.GetTNServiceIndexed(index)
	if err != nil {
		return false, err
	}
	return c.TNStoreExpired(ds.ID())
}

func (c *testCluster) LogStoreExpired(uuid string) (bool, error) {
	state := c.getClusterState()
	require.NotNil(c.t, state)

	logStore, ok := state.LogState.Stores[uuid]
	if !ok {
		return false, moerr.NewShardNotReportedNoCtx(uuid, 0xDEADBEEF)
	}

	hkcfg := c.GetHAKeeperConfig()
	expired := hkcfg.LogStoreExpired(logStore.Tick, state.Tick)

	c.logger.Info(
		"check log store expired or not",
		zap.Any("hakeeper config", hkcfg),
		zap.Uint64("log store tick", logStore.Tick),
		zap.Uint64("current tick", state.Tick),
		zap.Bool("expired", expired),
	)

	return expired, nil
}

func (c *testCluster) LogStoreExpiredIndexed(index int) (bool, error) {
	ls, err := c.GetLogServiceIndexed(index)
	if err != nil {
		return false, err
	}
	return c.LogStoreExpired(ls.ID())
}

func (c *testCluster) CNStoreExpired(uuid string) (bool, error) {
	state := c.getClusterState()
	require.NotNil(c.t, state)

	cnStore, ok := state.CNState.Stores[uuid]
	if !ok {
		return false, moerr.NewShardNotReportedNoCtx(uuid, 0)
	}

	hkcfg := c.GetHAKeeperConfig()
	expired := hkcfg.CNStoreExpired(cnStore.Tick, state.Tick)

	c.logger.Info(
		"check cn store expired or not",
		zap.Any("hakeeper config", hkcfg),
		zap.Uint64("cn store tick", cnStore.Tick),
		zap.Uint64("current tick", state.Tick),
		zap.Bool("expired", expired),
	)

	return expired, nil
}

func (c *testCluster) CNStoreExpiredIndexed(index int) (bool, error) {
	cs, err := c.GetCNServiceIndexed(index)
	if err != nil {
		return false, err
	}
	return c.CNStoreExpired(cs.ID())
}

func (c *testCluster) IsClusterHealthy() bool {
	hkcfg := c.GetHAKeeperConfig()
	state := c.getClusterState()
	_, healthy := syshealth.Check(
		hkcfg,
		state.GetClusterInfo(),
		state.GetTNState(),
		state.GetLogState(),
		state.GetTick(),
	)
	return healthy
}

// --------------------------------------------------------------
// The following are implements for interface `ClusterWaitState`.
// --------------------------------------------------------------
func (c *testCluster) WaitHAKeeperLeader(ctx context.Context) LogService {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting for hakeeper leader",
				"error: %s", ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			leader := c.getHAKeeperLeader()
			if leader != nil {
				return leader
			}
		}
	}
}

func (c *testCluster) WaitHAKeeperState(
	ctx context.Context, expected logpb.HAKeeperState,
) {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting for hakeeper state",
				"error: %s", ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			state := c.getClusterState()
			if state == nil {
				continue
			}
			if state.State == expected {
				return
			}
		}
	}
}

func (c *testCluster) WaitTNShardsReported(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting for all tn shards reported",
				"error: %s", ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			state := c.getClusterState()
			if state == nil {
				continue
			}

			expected := ParseExpectedTNShardCount(state.ClusterInfo)
			reported := ParseReportedTNShardCount(
				state.TNState, c.GetHAKeeperConfig(), state.Tick,
			)

			// FIXME: what about reported larger than expected
			if reported >= expected {
				return
			}
		}
	}
}

func (c *testCluster) WaitLogShardsReported(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting for all log shards reported",
				"error: %s", ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			state := c.getClusterState()
			if state == nil {
				continue
			}

			expected := ParseExpectedLogShardCount(state.ClusterInfo)
			reported := ParseReportedLogShardCount(
				state.LogState, c.GetHAKeeperConfig(), state.Tick,
			)
			// FIXME: what about reported larger than expected
			if reported >= expected {
				return
			}
		}
	}
}

func (c *testCluster) WaitTNReplicaReported(ctx context.Context, shardID uint64) {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting replica of tn shard reported",
				"shard %d, error: %s", shardID, ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			state := c.getClusterState()
			if state == nil {
				continue
			}

			reported := ParseTNShardReportedSize(
				shardID, state.TNState, c.GetHAKeeperConfig(), state.Tick,
			)
			if reported >= TNShardExpectedSize {
				return
			}
		}
	}
}

func (c *testCluster) WaitLogReplicaReported(ctx context.Context, shardID uint64) {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting replica of log shard reported",
				"shard %d, error: %s", shardID, ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			state := c.getClusterState()
			if state == nil {
				continue
			}

			expected := ParseLogShardExpectedSize(shardID, state.ClusterInfo)
			reported := ParseLogShardReportedSize(
				shardID, state.LogState, c.GetHAKeeperConfig(), state.Tick,
			)
			if reported >= expected {
				return
			}
		}
	}
}

func (c *testCluster) WaitTNStoreTimeout(ctx context.Context, uuid string) {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting tn store timeout",
				"dn store %s, error: %s", uuid, ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			expired, err := c.TNStoreExpired(uuid)
			if err != nil {
				c.logger.Error("fail to check tn store expired or not",
					zap.Error(err),
					zap.String("uuid", uuid),
				)
				continue
			}

			if expired {
				return
			}
		}
	}
}

func (c *testCluster) WaitTNStoreTimeoutIndexed(ctx context.Context, index int) {
	ds, err := c.GetTNServiceIndexed(index)
	require.NoError(c.t, err)

	c.WaitTNStoreTimeout(ctx, ds.ID())
}

func (c *testCluster) WaitTNStoreReported(ctx context.Context, uuid string) {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting tn store reported",
				"dn store %s, error: %s", uuid, ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			expired, err := c.TNStoreExpired(uuid)
			if err != nil {
				c.logger.Error("fail to check tn store expired or not",
					zap.Error(err),
					zap.String("uuid", uuid),
				)
				continue
			}

			if !expired {
				return
			}
		}
	}
}

func (c *testCluster) WaitTNStoreReportedIndexed(ctx context.Context, index int) {
	ds, err := c.GetTNServiceIndexed(index)
	require.NoError(c.t, err)

	c.WaitTNStoreReported(ctx, ds.ID())
}

func (c *testCluster) WaitCNStoreReported(ctx context.Context, uuid string) {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting cn store reported",
				"cn store %s, error: %s", uuid, ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			expired, err := c.CNStoreExpired(uuid)
			if err != nil {
				c.logger.Error("fail to check cn store expired or not",
					zap.Error(err),
					zap.String("uuid", uuid),
				)
				continue
			}

			if !expired {
				return
			}
		}
	}
}

func (c *testCluster) WaitCNStoreReportedIndexed(ctx context.Context, index int) {
	ds, err := c.GetCNServiceIndexed(index)
	require.NoError(c.t, err)

	c.WaitCNStoreReported(ctx, ds.ID())
}

func (c *testCluster) WaitCNStoreTaskServiceCreated(ctx context.Context, uuid string) {
	ds, err := c.GetCNService(uuid)
	require.NoError(c.t, err)

	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting task service created on cn store",
				"cn store %s, error: %s", uuid, ctx.Err(),
			)
		default:
			_, ok := ds.GetTaskService()
			if ok {
				return
			}
			time.Sleep(defaultWaitInterval)
		}
	}
}

func (c *testCluster) WaitCNStoreTaskServiceCreatedIndexed(ctx context.Context, index int) {
	ds, err := c.GetCNServiceIndexed(index)
	require.NoError(c.t, err)
	c.WaitCNStoreTaskServiceCreated(ctx, ds.ID())
}

func (c *testCluster) WaitTNStoreTaskServiceCreated(ctx context.Context, uuid string) {
	ds, err := c.GetTNService(uuid)
	require.NoError(c.t, err)

	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting task service created on tn store",
				"dn store %s, error: %s", uuid, ctx.Err(),
			)
		default:
			_, ok := ds.GetTaskService()
			if ok {
				return
			}
			time.Sleep(defaultWaitInterval)
		}
	}
}

func (c *testCluster) WaitTNStoreTaskServiceCreatedIndexed(ctx context.Context, index int) {
	ds, err := c.GetTNServiceIndexed(index)
	require.NoError(c.t, err)
	c.WaitTNStoreTaskServiceCreated(ctx, ds.ID())
}

func (c *testCluster) WaitLogStoreTaskServiceCreated(ctx context.Context, uuid string) {
	ls, err := c.GetLogService(uuid)
	require.NoError(c.t, err)

	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting task service created on log store",
				"log store %s, error: %s", uuid, ctx.Err(),
			)
		default:
			_, ok := ls.GetTaskService()
			if ok {
				return
			}
			time.Sleep(defaultWaitInterval)
		}
	}
}

func (c *testCluster) WaitLogStoreTaskServiceCreatedIndexed(ctx context.Context, index int) {
	ds, err := c.GetLogServiceIndexed(index)
	require.NoError(c.t, err)
	c.WaitLogStoreTaskServiceCreated(ctx, ds.ID())
}

func (c *testCluster) WaitLogStoreTimeout(ctx context.Context, uuid string) {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting log store timeout",
				"log store %s, error: %s", uuid, ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			expired, err := c.LogStoreExpired(uuid)
			if err != nil {
				c.logger.Error("fail to check log store expired or not",
					zap.Error(err),
					zap.String("uuid", uuid),
				)
				continue
			}

			if expired {
				return
			}
		}
	}
}

func (c *testCluster) WaitLogStoreTimeoutIndexed(ctx context.Context, index int) {
	ls, err := c.GetLogServiceIndexed(index)
	require.NoError(c.t, err)

	c.WaitLogStoreTimeout(ctx, ls.ID())
}

func (c *testCluster) WaitLogStoreReported(ctx context.Context, uuid string) {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting log store reported",
				"log store %s, error: %s", uuid, ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			expired, err := c.LogStoreExpired(uuid)
			if err != nil {
				c.logger.Error("fail to check log store expired or not",
					zap.Error(err),
					zap.String("uuid", uuid),
				)
				continue
			}

			if !expired {
				return
			}
		}
	}
}

func (c *testCluster) WaitLogStoreReportedIndexed(ctx context.Context, index int) {
	ls, err := c.GetLogServiceIndexed(index)
	require.NoError(c.t, err)

	c.WaitLogStoreReported(ctx, ls.ID())
}

// --------------------------------------------------------------
// The following are implements for interface `ClusterAwareness`.
// --------------------------------------------------------------
func (c *testCluster) ListTNServices() []string {
	ids := make([]string, 0, len(c.tn.svcs))
	for _, cfg := range c.tn.cfgs {
		ids = append(ids, cfg.UUID)
	}
	return ids
}

func (c *testCluster) ListLogServices() []string {
	ids := make([]string, 0, len(c.log.svcs))
	for _, svc := range c.log.svcs {
		ids = append(ids, svc.ID())
	}
	return ids
}

func (c *testCluster) ListCnServices() []string {
	ids := make([]string, 0, len(c.cn.svcs))
	for _, svc := range c.cn.svcs {
		ids = append(ids, svc.ID())
	}
	return ids
}

func (c *testCluster) ListHAKeeperServices() []LogService {
	return c.selectHAkeeperServices()
}

func (c *testCluster) GetTNService(uuid string) (TNService, error) {
	c.tn.Lock()
	defer c.tn.Unlock()

	for i, cfg := range c.tn.cfgs {
		if cfg.UUID == uuid {
			return c.tn.svcs[i], nil
		}
	}
	return nil, moerr.NewNoServiceNoCtx(uuid)
}

func (c *testCluster) GetLogService(uuid string) (LogService, error) {
	c.log.Lock()
	defer c.log.Unlock()

	for _, svc := range c.log.svcs {
		if svc.ID() == uuid {
			return svc, nil
		}
	}
	return nil, moerr.NewNoServiceNoCtx(uuid)
}

func (c *testCluster) GetCNService(uuid string) (CNService, error) {
	c.log.Lock()
	defer c.log.Unlock()

	for _, svc := range c.cn.svcs {
		if svc.ID() == uuid {
			return svc, nil
		}
	}
	return nil, moerr.NewNoServiceNoCtx(uuid)
}

func (c *testCluster) GetTNServiceIndexed(index int) (TNService, error) {
	c.tn.Lock()
	defer c.tn.Unlock()

	if index >= len(c.tn.svcs) || index < 0 {
		return nil, moerr.NewInvalidServiceIndexNoCtx(index)
	}
	return c.tn.svcs[index], nil
}

func (c *testCluster) GetLogServiceIndexed(index int) (LogService, error) {
	c.log.Lock()
	defer c.log.Unlock()

	if index >= len(c.log.svcs) || index < 0 {
		return nil, moerr.NewInvalidServiceIndexNoCtx(index)
	}
	return c.log.svcs[index], nil
}

func (c *testCluster) GetCNServiceIndexed(index int) (CNService, error) {
	c.log.Lock()
	defer c.log.Unlock()

	if index >= len(c.cn.svcs) || index < 0 {
		return nil, moerr.NewInvalidServiceIndexNoCtx(index)
	}
	return c.cn.svcs[index], nil
}

// NB: we could also fetch cluster state from non-leader hakeeper.
func (c *testCluster) GetClusterState(
	ctx context.Context,
) (*logpb.CheckerState, error) {
	c.WaitHAKeeperState(ctx, logpb.HAKeeperRunning)
	leader := c.WaitHAKeeperLeader(ctx)
	return leader.GetClusterState()
}

// --------------------------------------------------------------
// The following are implements for interface `ClusterOperation`.
// --------------------------------------------------------------
func (c *testCluster) CloseTNService(uuid string) error {
	ds, err := c.GetTNService(uuid)
	if err != nil {
		return err
	}
	return ds.Close()
}

func (c *testCluster) StartTNService(uuid string) error {
	ds, err := c.GetTNService(uuid)
	if err != nil {
		return err
	}
	return ds.Start()
}

func (c *testCluster) CloseTNServiceIndexed(index int) error {
	ds, err := c.GetTNServiceIndexed(index)
	if err != nil {
		return err
	}
	return ds.Close()
}

func (c *testCluster) StartTNServiceIndexed(index int) error {
	ds, err := c.GetTNServiceIndexed(index)
	if err != nil {
		return err
	}
	return ds.Start()
}

func (c *testCluster) CloseLogService(uuid string) error {
	ls, err := c.GetLogService(uuid)
	if err != nil {
		return err
	}
	return ls.Close()
}

func (c *testCluster) StartLogService(uuid string) error {
	ls, err := c.GetLogService(uuid)
	if err != nil {
		return err
	}
	return ls.Start()
}

func (c *testCluster) CloseLogServiceIndexed(index int) error {
	ls, err := c.GetLogServiceIndexed(index)
	if err != nil {
		return err
	}
	return ls.Close()
}

func (c *testCluster) StartLogServiceIndexed(index int) error {
	ls, err := c.GetLogServiceIndexed(index)
	if err != nil {
		return err
	}
	return ls.Start()
}

func (c *testCluster) CloseCNService(uuid string) error {
	cs, err := c.GetCNService(uuid)
	if err != nil {
		return err
	}
	return cs.Close()
}

func (c *testCluster) StartCNService(uuid string) error {
	cs, err := c.GetCNService(uuid)
	if err != nil {
		return err
	}
	return cs.Start()
}

func (c *testCluster) CloseCNServiceIndexed(index int) error {
	cs, err := c.GetCNServiceIndexed(index)
	if err != nil {
		return err
	}
	return cs.Close()
}

func (c *testCluster) StartCNServiceIndexed(index int) error {
	cs, err := c.GetCNServiceIndexed(index)
	if err != nil {
		return err
	}
	return cs.Start()
}

func (c *testCluster) StartCNServices(n int) error {
	offset := len(c.cn.svcs)
	c.buildCNConfigs(n)
	c.initCNServices(c.fileservices, offset)

	for _, cs := range c.cn.svcs[offset:] {
		if err := cs.Start(); err != nil {
			return err
		}
	}
	return nil
}

func (c *testCluster) NewNetworkPartition(
	tnIndexes, logIndexes, cnIndexes []uint32,
) NetworkPartition {
	return newNetworkPartition(
		c.opt.initial.logServiceNum, logIndexes,
		c.opt.initial.tnServiceNum, tnIndexes,
		c.opt.initial.cnServiceNum, cnIndexes,
	)
}

func (c *testCluster) RemainingNetworkPartition(
	partitions ...NetworkPartition,
) NetworkPartition {
	return remainingNetworkPartition(c.opt.initial.logServiceNum, c.opt.initial.tnServiceNum, 0, partitions...)
}

func (c *testCluster) StartNetworkPartition(parts ...NetworkPartition) {
	c.network.Lock()
	defer c.network.Unlock()

	addressSets := c.network.addresses.buildPartitionAddressSets(parts...)
	c.network.addressSets = addressSets
}

func (c *testCluster) CloseNetworkPartition() {
	c.network.Lock()
	defer c.network.Unlock()

	c.network.addressSets = nil
}

// ------------------------------------------------------
// The following are private utilities for `testCluster`.
// ------------------------------------------------------

// buildServiceAddresses builds addresses for all services.
func (c *testCluster) buildServiceAddresses() *serviceAddresses {
	return newServiceAddresses(
		c.t,
		c.opt.initial.logServiceNum,
		c.opt.initial.tnServiceNum,
		c.opt.initial.cnServiceNum,
		c.opt.hostAddr)
}

// buildTNConfigs builds configurations for all tn services.
func (c *testCluster) buildTNConfigs() ([]*tnservice.Config, []tnOptions) {
	batch := c.opt.initial.tnServiceNum

	cfgs := make([]*tnservice.Config, 0, batch)
	opts := make([]tnOptions, 0, batch)
	for i := 0; i < batch; i++ {
		cfg := buildTNConfig(i, c.opt, c.network.addresses)
		cfgs = append(cfgs, cfg)

		localAddr := cfg.ListenAddress
		opt := buildTNOptions(cfg, c.backendFilterFactory(localAddr))
		opts = append(opts, opt)
	}
	return cfgs, opts
}

// buildLogConfigs builds configurations for all log services.
func (c *testCluster) buildLogConfigs() ([]logservice.Config, []logOptions) {
	batch := c.opt.initial.logServiceNum

	cfgs := make([]logservice.Config, 0, batch)
	opts := make([]logOptions, 0, batch)
	for i := 0; i < batch; i++ {
		cfg := buildLogConfig(i, c.opt, c.network.addresses)
		cfgs = append(cfgs, cfg)

		localAddr := cfg.LogServiceServiceAddr()
		opt := buildLogOptions(cfg, c.backendFilterFactory(localAddr))
		opts = append(opts, opt)
	}
	return cfgs, opts
}

func (c *testCluster) buildCNConfigs(n int) {
	offset := len(c.cn.opts)
	batch := n
	c.network.addresses.buildCNAddress(c.t, batch, c.opt.hostAddr)
	for i := 0; i < batch; i++ {
		cfg := buildCNConfig(i+offset, c.opt, c.network.addresses)
		c.cn.cfgs = append(c.cn.cfgs, cfg)
		var opt cnOptions
		if c.opt.cn.optionFunc != nil {
			opt = c.opt.cn.optionFunc(i + offset)
		}
		opt = append(opt, cnservice.WithLogger(c.logger))
		c.cn.opts = append(c.cn.opts, opt)

		c.fileservices.cnLocalFSs = append(c.fileservices.cnLocalFSs,
			c.createFS(context.Background(), filepath.Join(c.opt.rootDataDir, cfg.UUID), defines.LocalFileServiceName))
		c.fileservices.cnServiceNum++
	}
}

// initTNServices builds all tn services.
//
// Before initializing tn service, log service must be started already.
func (c *testCluster) initTNServices(fileservices *fileServices) []TNService {
	batch := c.opt.initial.tnServiceNum

	c.logger.Info("initialize tn services", zap.Int("batch", batch))

	svcs := make([]TNService, 0, batch)
	for i := 0; i < batch; i++ {
		cfg := c.tn.cfgs[i]
		opt := c.tn.opts[i]
		fs, err := fileservice.NewFileServices(
			"",
			fileservices.getTNLocalFileService(i),
			fileservices.getS3FileService(),
		)
		if err != nil {
			panic(err)
		}
		ds, err := newTNService(
			cfg,
			c.newRuntime(),
			fs,
			opt)
		require.NoError(c.t, err)

		c.logger.Info(
			"dn service initialized",
			zap.Int("index", i),
			zap.Any("config", cfg),
		)

		svcs = append(svcs, ds)
	}

	return svcs
}

// initLogServices builds all log services.
func (c *testCluster) initLogServices() []LogService {
	batch := c.opt.initial.logServiceNum

	c.logger.Info("initialize log services", zap.Int("batch", batch))

	svcs := make([]LogService, 0, batch)
	for i := 0; i < batch; i++ {
		cfg := c.log.cfgs[i]
		opt := c.log.opts[i]
		ls, err := newLogService(cfg, testutil.NewFS(), opt)
		require.NoError(c.t, err)

		c.logger.Info(
			"log service initialized",
			zap.Int("index", i),
			zap.Any("config", cfg),
		)

		svcs = append(svcs, ls)
	}
	return svcs
}

func (c *testCluster) initCNServices(
	fileservices *fileServices,
	offset int) {
	batch := len(c.cn.cfgs)

	c.logger.Info("initialize cn services", zap.Int("batch", batch))
	for i := offset; i < batch; i++ {
		cfg := c.cn.cfgs[i]
		opt := c.cn.opts[i]
		fs, err := fileservice.NewFileServices(
			"",
			fileservices.getCNLocalFileService(i),
			fileservices.getS3FileService(),
			fileservices.getETLFileService(),
		)
		if err != nil {
			panic(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cs, err := newCNService(cfg, ctx, fs, opt)
		if err != nil {
			panic(err)
		}
		cs.SetCancel(cancel)

		c.logger.Info(
			"cn service initialized",
			zap.Int("index", i),
			zap.Any("config", cfg),
		)

		c.cn.svcs = append(c.cn.svcs, cs)
	}
}

// startTNServices initializes and starts all tn services.
func (c *testCluster) startTNServices(ctx context.Context) error {
	// initialize all tn services
	c.tn.svcs = c.initTNServices(c.fileservices)

	// start tn services
	for _, ds := range c.tn.svcs {
		if err := ds.Start(); err != nil {
			return err
		}
	}

	c.WaitTNShardsReported(ctx)
	return nil
}

// startLogServices initializes and starts all log services.
func (c *testCluster) startLogServices(ctx context.Context) error {
	// initialize all log service
	c.log.svcs = c.initLogServices()

	// start log services
	for _, ls := range c.log.svcs {
		if err := ls.Start(); err != nil {
			return err
		}
	}

	// start hakeeper replicas
	if err := c.startHAKeeperReplica(); err != nil {
		return err
	}

	// initialize cluster information
	if err := c.setInitialClusterInfo(); err != nil {
		return err
	}

	c.WaitHAKeeperState(ctx, logpb.HAKeeperRunning)
	return nil
}

func (c *testCluster) startCNServices(ctx context.Context) error {
	c.initCNServices(c.fileservices, 0)

	for _, cs := range c.cn.svcs {
		if err := cs.Start(); err != nil {
			return err
		}
	}

	if err := c.waitSystemInitCompleted(ctx); err != nil {
		return err
	}
	return nil
}

// closeTNServices closes all tn services.
func (c *testCluster) closeTNServices() error {
	c.logger.Info("start to close tn services")

	for i, ds := range c.tn.svcs {
		c.logger.Info("close tn service", zap.Int("index", i))
		if err := ds.Close(); err != nil {
			return err
		}
		c.logger.Info("dn service closed", zap.Int("index", i))
	}

	return nil
}

// closeLogServices closes all log services.
func (c *testCluster) closeLogServices() error {
	defer logutil.LogClose(c.logger, "tests-framework/logservices")()

	for i, ls := range c.log.svcs {
		c.logger.Info("close log service", zap.Int("index", i))
		if err := ls.Close(); err != nil {
			return err
		}
		c.logger.Info("log service closed", zap.Int("index", i))
	}

	return nil
}

func (c *testCluster) closeCNServices() error {
	defer logutil.LogClose(c.logger, "tests-framework/cnservices")()

	for i, cs := range c.cn.svcs {
		c.logger.Info("close cn service", zap.Int("index", i))
		if err := cs.Close(); err != nil {
			return err
		}
		c.logger.Info("cn service closed", zap.Int("index", i))
	}

	return nil
}

// getClusterState fetches cluster state from arbitrary hakeeper.
//
// NB: it's possible that getClusterState returns nil value.
func (c *testCluster) getClusterState() *logpb.CheckerState {
	var state *logpb.CheckerState
	fn := func(index int, svc LogService) bool {
		s, err := svc.GetClusterState()
		if err != nil {
			c.logger.Error(
				"fail to get cluster state",
				zap.Error(err),
				zap.Int("index", index),
			)
			return false
		}
		state = s
		// XXX MPOOL
		// Too much logging can break CI.
		// c.logger.Info("current cluster state", zap.Any("state", s))
		return true
	}
	c.rangeHAKeeperService(fn)
	return state
}

// getHAKeeperLeader gets log service which is hakeeper leader.
func (c *testCluster) getHAKeeperLeader() LogService {
	var leader LogService
	fn := func(index int, svc LogService) bool {
		isLeader, err := svc.IsLeaderHakeeper()
		if err != nil {
			c.logger.Error(
				"fail to check hakeeper",
				zap.Error(err),
				zap.Int("index", index),
			)
			return false
		}
		c.logger.Info(
			"hakeeper state",
			zap.Bool("isLeader", isLeader),
			zap.Int("index", index),
		)

		if isLeader {
			leader = svc
			return true
		}

		return false
	}
	c.rangeHAKeeperService(fn)
	return leader
}

// rangeHAKeeperService iterates all hakeeper service until `fn` returns true.
func (c *testCluster) rangeHAKeeperService(
	fn func(index int, svc LogService) bool,
) {
	for i, svc := range c.selectHAkeeperServices() {
		index := i

		if svc.Status() != ServiceStarted {
			c.logger.Warn(
				"hakeeper service not started",
				zap.Int("index", index),
			)
			continue
		}

		if fn(index, svc) {
			break
		}
	}
}

func (c *testCluster) waitSystemInitCompleted(ctx context.Context) error {
	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	cn, err := c.GetCNServiceIndexed(0)
	if err != nil {
		return err
	}
	if err := cn.WaitSystemInitCompleted(ctx); err != nil {
		return err
	}
	return nil
}

func (c *testCluster) newRuntime() runtime.Runtime {
	return runtime.NewRuntime(metadata.ServiceType_CN, "", c.logger, runtime.WithClock(c.clock))
}

// FilterFunc returns true if traffic was allowed.
type FilterFunc func(morpc.Message, string) bool

// backendFilterFactory constructs a closure with the type of FilterFunc.
func (c *testCluster) backendFilterFactory(localAddr string) FilterFunc {
	return func(_ morpc.Message, backendAddr string) bool {
		// NB: it's possible that partition takes effect once more after disabled.
		c.network.RLock()
		addressSets := c.network.addressSets
		c.network.RUnlock()

		if len(addressSets) == 0 {
			return true
		}

		for _, addrSet := range addressSets {
			if addrSet.contains(localAddr) &&
				addrSet.contains(backendAddr) {
				return true
			}
		}

		c.logger.Info(
			"traffic not allowed",
			zap.String("local", localAddr),
			zap.String("backend", backendAddr),
		)

		return false
	}
}
