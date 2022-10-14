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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/dnservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/syshealth"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
)

var (
	defaultWaitInterval = 100 * time.Millisecond
)

// Cluster describes behavior of test framework.
type Cluster interface {
	// Start starts svcs sequentially
	Start() error
	// Close stops svcs sequentially
	Close() error

	ClusterOperation
	ClusterAwareness
	ClusterState
	ClusterWaitState
}

// ClusterOperation supports kinds of cluster operations.
type ClusterOperation interface {
	// CloseDNService closes dn service by uuid.
	CloseDNService(uuid string) error
	// StartDNService starts dn service by uuid.
	StartDNService(uuid string) error

	// CloseDNServiceIndexed closes dn service by its index.
	CloseDNServiceIndexed(index int) error
	// StartDNServiceIndexed starts dn service by its index.
	StartDNServiceIndexed(index int) error

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

	// NewNetworkPartition constructs network partition from service index.
	NewNetworkPartition(dnIndexes, logIndexes, cnIndexes []uint32) NetworkPartition
	// RemainingNetworkPartition returns partition for the remaining services.
	RemainingNetworkPartition(partitions ...NetworkPartition) NetworkPartition
	// StartNetworkPartition enables network partition feature.
	StartNetworkPartition(partitions ...NetworkPartition)
	// CloseNetworkPartition disables network partition feature.
	CloseNetworkPartition()
}

// ClusterAwareness provides cluster awareness information.
type ClusterAwareness interface {
	// ListDNServices lists uuid of all dn services.
	ListDNServices() []string
	// ListLogServices lists uuid of all log services.
	ListLogServices() []string
	// ListCnServices lists uuid of all cn services.
	ListCnServices() []string
	// ListHAKeeperServices lists all hakeeper log services.
	ListHAKeeperServices() []LogService

	// GetDNService fetches dn service instance by uuid.
	GetDNService(uuid string) (DNService, error)
	// GetLogService fetches log service instance by index.
	GetLogService(uuid string) (LogService, error)
	// GetDNServiceIndexed fetches dn service instance by uuid.
	GetDNServiceIndexed(index int) (DNService, error)
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
	// ListDNShards lists all dn shards within the cluster.
	ListDNShards(ctx context.Context) ([]metadata.DNShardRecord, error)
	// ListLogShards lists all log shards within the cluster.
	ListLogShards(ctx context.Context) ([]metadata.LogShardRecord, error)

	// GetDNStoreInfo gets dn store information by uuid.
	GetDNStoreInfo(ctx context.Context, uuid string) (logpb.DNStoreInfo, error)
	// GetDNStoreInfoIndexed gets dn store information by index.
	GetDNStoreInfoIndexed(ctx context.Context, index int) (logpb.DNStoreInfo, error)

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

	// DNStoreExpired checks dn store expired or not by uuid.
	DNStoreExpired(uuid string) (bool, error)
	// DNStoreExpiredIndexed checks dn store expired or not by index.
	DNStoreExpiredIndexed(index int) (bool, error)
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

	// WaitDNShardsReported waits the expected count of dn shards reported.
	WaitDNShardsReported(ctx context.Context)
	// WaitLogShardsReported waits the expected count of log shards reported.
	WaitLogShardsReported(ctx context.Context)
	// WaitDNReplicaReported waits dn replica reported.
	WaitDNReplicaReported(ctx context.Context, shardID uint64)
	// WaitLogReplicaReported waits log replicas reported.
	WaitLogReplicaReported(ctx context.Context, shardID uint64)

	// WaitDNStoreTimeout waits dn store timeout by uuid.
	WaitDNStoreTimeout(ctx context.Context, uuid string)
	// WaitDNStoreTimeoutIndexed waits dn store timeout by index.
	WaitDNStoreTimeoutIndexed(ctx context.Context, index int)
	// WaitDNStoreReported waits dn store reported by uuid.
	WaitDNStoreReported(ctx context.Context, uuid string)
	// WaitDNStoreReportedIndexed waits dn store reported by index.
	WaitDNStoreReportedIndexed(ctx context.Context, index int)

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

// testCluster simulates a cluster with dn and log service.
type testCluster struct {
	t       *testing.T
	opt     Options
	logger  *zap.Logger
	stopper *stopper.Stopper
	clock   clock.Clock

	dn struct {
		sync.Mutex
		cfgs []*dnservice.Config
		opts []dnOptions
		svcs []DNService
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
		addresses serviceAddresses

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
func NewCluster(t *testing.T, opt Options) (Cluster, error) {
	opt.validate()

	c := &testCluster{
		t:       t,
		opt:     opt,
		stopper: stopper.NewStopper("test-cluster"),
	}
	c.logger = logutil.Adjust(c.logger).With(
		zap.String("tests", "service"),
	)

	if c.clock == nil {
		c.clock = clock.NewUnixNanoHLCClockWithStopper(c.stopper, 0)
	}
	clock.SetupDefaultClock(c.clock)

	// build addresses for all services
	c.network.addresses = c.buildServiceAddresses()

	// build FileService instances
	c.fileservices = c.buildFileServices()

	// build log service configurations
	c.log.cfgs, c.log.opts = c.buildLogConfigs(c.network.addresses)

	// build dn service configurations
	c.dn.cfgs, c.dn.opts = c.buildDnConfigs(c.network.addresses)

	c.cn.cfgs, c.cn.opts = c.buildCNConfigs(c.network.addresses)

	return c, nil
}

func (c *testCluster) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.running {
		return nil
	}

	// start log services first
	if err := c.startLogServices(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c.WaitHAKeeperState(ctx, logpb.HAKeeperRunning)

	// start dn services
	if err := c.startDNServices(); err != nil {
		return err
	}
	ctx1, cancel1 := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel1()
	c.WaitDNShardsReported(ctx1)

	if c.opt.initial.cnServiceNum != 0 {
		if err := c.startCNServices(); err != nil {
			return err
		}
	}

	c.mu.running = true
	return nil
}

func (c *testCluster) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.mu.running {
		return nil
	}

	// close all dn services first
	if err := c.closeDNServices(); err != nil {
		return err
	}

	// close all log services
	if err := c.closeLogServices(); err != nil {
		return err
	}

	if err := c.closeCNServices(); err != nil {
		return err
	}

	c.mu.running = false
	c.stopper.Stop()
	return nil
}

// ----------------------------------------------------------
// The following are implements for interface `ClusterState`.
// ----------------------------------------------------------
func (c *testCluster) ListDNShards(
	ctx context.Context,
) ([]metadata.DNShardRecord, error) {
	state, err := c.GetClusterState(ctx)
	if err != nil {
		return nil, err
	}
	return state.ClusterInfo.DNShards, nil
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

func (c *testCluster) GetDNStoreInfo(
	ctx context.Context, uuid string,
) (logpb.DNStoreInfo, error) {
	state, err := c.GetClusterState(ctx)
	if err != nil {
		return logpb.DNStoreInfo{}, err
	}
	stores := state.DNState.Stores
	if storeInfo, ok := stores[uuid]; ok {
		return storeInfo, nil
	}
	return logpb.DNStoreInfo{}, moerr.NewNoService(uuid)
}

func (c *testCluster) GetDNStoreInfoIndexed(
	ctx context.Context, index int,
) (logpb.DNStoreInfo, error) {
	ds, err := c.GetDNServiceIndexed(index)
	if err != nil {
		return logpb.DNStoreInfo{}, err
	}
	return c.GetDNStoreInfo(ctx, ds.ID())
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
	return logpb.LogStoreInfo{}, moerr.NewNoService(uuid)
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
	return logpb.CNStoreInfo{}, moerr.NewNoService(uuid)
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

func (c *testCluster) DNStoreExpired(uuid string) (bool, error) {
	state := c.getClusterState()
	require.NotNil(c.t, state)

	dnStore, ok := state.DNState.Stores[uuid]
	if !ok {
		return false, moerr.NewShardNotReported(uuid, 0xDEADBEEF)
	}

	hkcfg := c.GetHAKeeperConfig()
	expired := hkcfg.DNStoreExpired(dnStore.Tick, state.Tick)

	c.logger.Info(
		"check dn store expired or not",
		zap.Any("hakeeper config", hkcfg),
		zap.Uint64("dn store tick", dnStore.Tick),
		zap.Uint64("current tick", state.Tick),
		zap.Bool("expired", expired),
	)

	return expired, nil
}

func (c *testCluster) DNStoreExpiredIndexed(index int) (bool, error) {
	ds, err := c.GetDNServiceIndexed(index)
	if err != nil {
		return false, err
	}
	return c.DNStoreExpired(ds.ID())
}

func (c *testCluster) LogStoreExpired(uuid string) (bool, error) {
	state := c.getClusterState()
	require.NotNil(c.t, state)

	logStore, ok := state.LogState.Stores[uuid]
	if !ok {
		return false, moerr.NewShardNotReported(uuid, 0xDEADBEEF)
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
		return false, moerr.NewShardNotReported(uuid, 0)
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
		state.GetDNState(),
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

func (c *testCluster) WaitDNShardsReported(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting for all dn shards reported",
				"error: %s", ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			state := c.getClusterState()
			if state == nil {
				continue
			}

			expected := ParseExpectedDNShardCount(state.ClusterInfo)
			reported := ParseReportedDNShardCount(
				state.DNState, c.GetHAKeeperConfig(), state.Tick,
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

func (c *testCluster) WaitDNReplicaReported(ctx context.Context, shardID uint64) {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting replica of dn shard reported",
				"shard %d, error: %s", shardID, ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			state := c.getClusterState()
			if state == nil {
				continue
			}

			reported := ParseDNShardReportedSize(
				shardID, state.DNState, c.GetHAKeeperConfig(), state.Tick,
			)
			if reported >= DNShardExpectedSize {
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

func (c *testCluster) WaitDNStoreTimeout(ctx context.Context, uuid string) {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting dn store timeout",
				"dn store %s, error: %s", uuid, ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			expired, err := c.DNStoreExpired(uuid)
			if err != nil {
				c.logger.Error("fail to check dn store expired or not",
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

func (c *testCluster) WaitDNStoreTimeoutIndexed(ctx context.Context, index int) {
	ds, err := c.GetDNServiceIndexed(index)
	require.NoError(c.t, err)

	c.WaitDNStoreTimeout(ctx, ds.ID())
}

func (c *testCluster) WaitDNStoreReported(ctx context.Context, uuid string) {
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(
				c.t,
				"terminated when waiting dn store reported",
				"dn store %s, error: %s", uuid, ctx.Err(),
			)
		default:
			time.Sleep(defaultWaitInterval)

			expired, err := c.DNStoreExpired(uuid)
			if err != nil {
				c.logger.Error("fail to check dn store expired or not",
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

func (c *testCluster) WaitDNStoreReportedIndexed(ctx context.Context, index int) {
	ds, err := c.GetDNServiceIndexed(index)
	require.NoError(c.t, err)

	c.WaitDNStoreReported(ctx, ds.ID())
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
func (c *testCluster) ListDNServices() []string {
	ids := make([]string, 0, len(c.dn.svcs))
	for _, cfg := range c.dn.cfgs {
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

func (c *testCluster) GetDNService(uuid string) (DNService, error) {
	c.dn.Lock()
	defer c.dn.Unlock()

	for i, cfg := range c.dn.cfgs {
		if cfg.UUID == uuid {
			return c.dn.svcs[i], nil
		}
	}
	return nil, moerr.NewNoService(uuid)
}

func (c *testCluster) GetLogService(uuid string) (LogService, error) {
	c.log.Lock()
	defer c.log.Unlock()

	for _, svc := range c.log.svcs {
		if svc.ID() == uuid {
			return svc, nil
		}
	}
	return nil, moerr.NewNoService(uuid)
}

func (c *testCluster) GetCNService(uuid string) (CNService, error) {
	c.log.Lock()
	defer c.log.Unlock()

	for _, svc := range c.cn.svcs {
		if svc.ID() == uuid {
			return svc, nil
		}
	}
	return nil, moerr.NewNoService(uuid)
}

func (c *testCluster) GetDNServiceIndexed(index int) (DNService, error) {
	c.dn.Lock()
	defer c.dn.Unlock()

	if index >= len(c.dn.svcs) || index < 0 {
		return nil, moerr.NewInvalidServiceIndex(index)
	}
	return c.dn.svcs[index], nil
}

func (c *testCluster) GetLogServiceIndexed(index int) (LogService, error) {
	c.log.Lock()
	defer c.log.Unlock()

	if index >= len(c.log.svcs) || index < 0 {
		return nil, moerr.NewInvalidServiceIndex(index)
	}
	return c.log.svcs[index], nil
}

func (c *testCluster) GetCNServiceIndexed(index int) (CNService, error) {
	c.log.Lock()
	defer c.log.Unlock()

	if index >= len(c.cn.svcs) || index < 0 {
		return nil, moerr.NewInvalidServiceIndex(index)
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
func (c *testCluster) CloseDNService(uuid string) error {
	ds, err := c.GetDNService(uuid)
	if err != nil {
		return err
	}
	return ds.Close()
}

func (c *testCluster) StartDNService(uuid string) error {
	ds, err := c.GetDNService(uuid)
	if err != nil {
		return err
	}
	return ds.Start()
}

func (c *testCluster) CloseDNServiceIndexed(index int) error {
	ds, err := c.GetDNServiceIndexed(index)
	if err != nil {
		return err
	}
	return ds.Close()
}

func (c *testCluster) StartDNServiceIndexed(index int) error {
	ds, err := c.GetDNServiceIndexed(index)
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

func (c *testCluster) NewNetworkPartition(
	dnIndexes, logIndexes, cnIndexes []uint32,
) NetworkPartition {
	return newNetworkPartition(
		c.opt.initial.logServiceNum, logIndexes,
		c.opt.initial.dnServiceNum, dnIndexes,
		c.opt.initial.cnServiceNum, cnIndexes,
	)
}

func (c *testCluster) RemainingNetworkPartition(
	partitions ...NetworkPartition,
) NetworkPartition {
	return remainingNetworkPartition(c.opt.initial.logServiceNum, c.opt.initial.dnServiceNum, 0, partitions...)
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
func (c *testCluster) buildServiceAddresses() serviceAddresses {
	return newServiceAddresses(c.t, c.opt.initial.logServiceNum,
		c.opt.initial.dnServiceNum, c.opt.initial.cnServiceNum, c.opt.hostAddr)
}

// buildFileServices builds all file services.
func (c *testCluster) buildFileServices() *fileServices {
	return newFileServices(c.t, c.opt.initial.dnServiceNum, c.opt.initial.cnServiceNum)
}

// buildDnConfigs builds configurations for all dn services.
func (c *testCluster) buildDnConfigs(
	address serviceAddresses,
) ([]*dnservice.Config, []dnOptions) {
	batch := c.opt.initial.dnServiceNum

	cfgs := make([]*dnservice.Config, 0, batch)
	opts := make([]dnOptions, 0, batch)
	for i := 0; i < batch; i++ {
		cfg := buildDnConfig(i, c.opt, address)
		cfgs = append(cfgs, cfg)

		localAddr := cfg.ListenAddress
		opt := buildDnOptions(cfg, c.backendFilterFactory(localAddr))
		opts = append(opts, opt)
	}
	return cfgs, opts
}

// buildLogConfigs builds configurations for all log services.
func (c *testCluster) buildLogConfigs(
	address serviceAddresses,
) ([]logservice.Config, []logOptions) {
	batch := c.opt.initial.logServiceNum

	cfgs := make([]logservice.Config, 0, batch)
	opts := make([]logOptions, 0, batch)
	for i := 0; i < batch; i++ {
		cfg := buildLogConfig(i, c.opt, address)
		cfgs = append(cfgs, cfg)

		localAddr := cfg.ServiceAddress
		opt := buildLogOptions(cfg, c.backendFilterFactory(localAddr))
		opts = append(opts, opt)
	}
	return cfgs, opts
}

func (c *testCluster) buildCNConfigs(
	address serviceAddresses,
) ([]*cnservice.Config, []cnOptions) {
	batch := c.opt.initial.cnServiceNum

	cfgs := make([]*cnservice.Config, 0, batch)
	opts := make([]cnOptions, 0, batch)
	for i := 0; i < batch; i++ {
		cfg := buildCnConfig(i, c.opt, address)
		cfgs = append(cfgs, cfg)

		opt := buildCnOptions()
		opts = append(opts, opt)
	}
	return cfgs, opts
}

// initDNServices builds all dn services.
//
// Before initializing dn service, log service must be started already.
func (c *testCluster) initDNServices(fileservices *fileServices) []DNService {
	batch := c.opt.initial.dnServiceNum

	c.logger.Info("initialize dn services", zap.Int("batch", batch))

	svcs := make([]DNService, 0, batch)
	for i := 0; i < batch; i++ {
		cfg := c.dn.cfgs[i]
		opt := c.dn.opts[i]
		fs, err := fileservice.NewFileServices(
			"LOCAL",
			fileservices.getDNLocalFileService(i),
			fileservices.getS3FileService(),
		)
		if err != nil {
			panic(err)
		}

		ds, err := newDNService(cfg, fs, opt)
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
		ls, err := newLogService(cfg, testutil.NewFS(), taskservice.NewTaskService(c.opt.task.taskStorage, nil), opt)
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

func (c *testCluster) initCNServices(fileservices *fileServices) []CNService {
	batch := c.opt.initial.cnServiceNum

	c.logger.Info("initialize cn services", zap.Int("batch", batch))

	svcs := make([]CNService, 0, batch)
	for i := 0; i < batch; i++ {
		cfg := c.cn.cfgs[i]
		opt := c.cn.opts[i]
		fs, err := fileservice.NewFileServices(
			"LOCAL",
			fileservices.getCNLocalFileService(i),
			fileservices.getS3FileService(),
		)
		if err != nil {
			panic(err)
		}

		cs, err := newCNService(cfg, context.TODO(), fs, c.opt.task.taskStorage, opt)
		if err != nil {
			panic(err)
		}

		c.logger.Info(
			"cn service initialized",
			zap.Int("index", i),
			zap.Any("config", cfg),
		)

		svcs = append(svcs, cs)
	}
	return svcs
}

// startDNServices initializes and starts all dn services.
func (c *testCluster) startDNServices() error {
	// initialize all dn services
	c.dn.svcs = c.initDNServices(c.fileservices)

	// start dn services
	for _, ds := range c.dn.svcs {
		if err := ds.Start(); err != nil {
			return err
		}
	}

	return nil
}

// startLogServices initializes and starts all log services.
func (c *testCluster) startLogServices() error {
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

	return nil
}

func (c *testCluster) startCNServices() error {
	c.cn.svcs = c.initCNServices(c.fileservices)

	for _, cs := range c.cn.svcs {
		if err := cs.Start(); err != nil {
			return err
		}
	}
	return nil
}

// closeDNServices closes all dn services.
func (c *testCluster) closeDNServices() error {
	c.logger.Info("start to close dn services")

	for i, ds := range c.dn.svcs {
		c.logger.Info("close dn service", zap.Int("index", i))
		if err := ds.Close(); err != nil {
			return err
		}
		c.logger.Info("dn service closed", zap.Int("index", i))
	}

	return nil
}

// closeLogServices closes all log services.
func (c *testCluster) closeLogServices() error {
	c.logger.Info("start to close log services")

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
	c.logger.Info("start to close cn services")

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
