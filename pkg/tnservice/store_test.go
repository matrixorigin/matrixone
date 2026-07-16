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

package tnservice

import (
	"context"
	"errors"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testTNStoreAddr      = "unix:///tmp/test-dnstore.sock"
	testTNLogtailAddress = "127.0.0.1:22001"
)

const storeTestTimeout = 30 * time.Second

type storeQueryClient struct {
	client.QueryClient
	closeCalls int
}

type storeQueryService struct {
	queryservice.QueryService
	closeCalls int
}

type storeTxnServer struct {
	rpc.TxnServer
	beforeClose func()
}

func (s *storeTxnServer) Close() error {
	s.beforeClose()
	return s.TxnServer.Close()
}

func (s *storeQueryService) Close() error {
	s.closeCalls++
	return s.QueryService.Close()
}

func (c *storeQueryClient) Close() error {
	c.closeCalls++
	return nil
}

func TestNewAndStartAndCloseService(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		thc := s.hakeeperClient.(*testHAKeeperClient)
		for {
			if v := thc.getCount(); v > 0 {
				return
			}
		}
	})
}

func TestAddReplica(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		addTestReplica(t, s, 1, 2, 3)
	})
}

func TestHandleShutdown(t *testing.T) {
	fn := func(s *store) {
		cmd := logservicepb.ScheduleCommand{
			UUID: s.cfg.UUID,
			ShutdownStore: &logservicepb.ShutdownStore{
				StoreID: s.cfg.UUID,
			},
			ServiceType: logservicepb.TNService,
		}

		shutdownC := make(chan struct{})
		exit := atomic.Bool{}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer func() {
				cancel()
				exit.Store(true)
			}()

			select {
			case <-ctx.Done():
				panic("deadline reached")
			case <-shutdownC:
				runtime.DefaultRuntime().Logger().Info("received shutdown command")
			}
		}()

		s.shutdownC = shutdownC

		for !exit.Load() {
			s.handleCommands([]logservicepb.ScheduleCommand{cmd})
			time.Sleep(time.Millisecond)
		}

	}
	runTNStoreTest(t, fn)
}

func TestStartWithReplicas(t *testing.T) {
	localFS, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
	assert.NoError(t, err)

	factory := func(name string) (*fileservice.FileServices, error) {
		s3fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
		if err != nil {
			return nil, err
		}
		return fileservice.NewFileServices(
			"",
			s3fs,
			localFS,
		)
	}

	runTNStoreTestWithFileServiceFactory(t, func(s *store) {
		addTestReplica(t, s, 1, 2, 3)
	}, factory)

	runTNStoreTestWithFileServiceFactory(t, func(s *store) {

	}, factory)
}

func TestStartReplica(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		assert.NoError(t, s.StartTNReplica(newTestTNShard(1, 2, 3)))
		r := s.getReplica(1)
		assert.NoError(t, r.waitStarted(context.Background()))
		assert.Equal(t, newTestTNShard(1, 2, 3), r.shard)
	})
}

func TestRemoveReplica(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		assert.NoError(t, s.StartTNReplica(newTestTNShard(1, 2, 3)))
		r := s.getReplica(1)
		assert.NoError(t, r.waitStarted(context.Background()))

		thc := s.hakeeperClient.(*testHAKeeperClient)
		thc.setCommandBatch(logservicepb.CommandBatch{
			Commands: []logservicepb.ScheduleCommand{
				{
					ServiceType: logservicepb.TNService,
					ConfigChange: &logservicepb.ConfigChange{
						ChangeType: logservicepb.RemoveReplica,
						Replica: logservicepb.Replica{
							LogShardID: 3,
							ReplicaID:  2,
							ShardID:    1,
						},
					},
				},
			},
		})

		for {
			r := s.getReplica(1)
			if r == nil {
				return
			}
			time.Sleep(time.Millisecond * 10)
		}
	})
}

func TestCloseReplica(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		shard := newTestTNShard(1, 2, 3)
		assert.NoError(t, s.StartTNReplica(shard))
		r := s.getReplica(1)
		assert.NoError(t, r.waitStarted(context.Background()))
		assert.Equal(t, shard, r.shard)

		assert.NoError(t, s.CloseTNReplica(shard))
		assert.Nil(t, s.getReplica(1))
	})
}

func TestRemoveReplicaCancelsStorageCreation(t *testing.T) {
	oldInterval := retryCreateStorageInterval
	retryCreateStorageInterval = 10 * time.Millisecond
	t.Cleanup(func() { retryCreateStorageInterval = oldInterval })

	var allowCreate atomic.Bool
	createAttempted := make(chan struct{}, 1)
	runTNStoreTest(t, func(s *store) {
		s.options.logServiceClientFactory = func(metadata.TNShard) (logservice.Client, error) {
			if !allowCreate.Load() {
				select {
				case createAttempted <- struct{}{}:
				default:
				}
				return nil, errors.New("injected log client creation failure")
			}
			return mem.NewMemLog(), nil
		}

		shard := newTestTNShard(101, 201, 301)
		require.NoError(t, s.StartTNReplica(shard))
		removed := s.getReplica(shard.ShardID)
		require.NotNil(t, removed)
		select {
		case <-createAttempted:
		case <-time.After(time.Second):
			t.Fatal("storage creation was not attempted")
		}

		require.NoError(t, s.CloseTNReplica(shard))
		require.Nil(t, s.getReplica(shard.ShardID))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.ErrorIs(t, removed.waitStarted(ctx), context.Canceled)
		removed.mu.RLock()
		require.Nil(t, removed.service)
		removed.mu.RUnlock()

		require.NoError(t, s.StartTNReplica(shard))
		replacement := s.getReplica(shard.ShardID)
		require.NotNil(t, replacement)
		require.NotSame(t, removed, replacement)
		allowCreate.Store(true)
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, replacement.waitStarted(ctx))

		time.Sleep(5 * retryCreateStorageInterval)
		removed.mu.RLock()
		require.Nil(t, removed.service)
		removed.mu.RUnlock()
	})
}

func TestStoreCloseClosesSharedQueryClient(t *testing.T) {
	sharedClient := &storeQueryClient{}
	runTNStoreTest(t, func(s *store) {
		realClient := s.queryClient
		s.queryClient = sharedClient
		require.NoError(t, realClient.Close())
		t.Cleanup(func() {
			require.Equal(t, 1, sharedClient.closeCalls)
		})
	})
}

func TestStoreCloseClosesQueryService(t *testing.T) {
	var tracked *storeQueryService
	runTNStoreTest(t, func(s *store) {
		tracked = &storeQueryService{QueryService: s.queryService}
		s.queryService = tracked
		t.Cleanup(func() {
			require.Equal(t, 1, tracked.closeCalls)
		})
	})
}

func TestStoreCloseCancelsReplicasBeforeDrainingRPCServer(t *testing.T) {
	var canceledBeforeServerClose atomic.Bool
	runTNStoreTest(t, func(s *store) {
		r := newReplica(newTestTNShard(1, 2, 3), s.rt)
		s.replicas.Store(r.shard.ShardID, r)
		s.server = &storeTxnServer{
			TxnServer: s.server,
			beforeClose: func() {
				r.mu.RLock()
				defer r.mu.RUnlock()
				canceledBeforeServerClose.Store(r.mu.cancelled)
			},
		}
	})
	require.True(t, canceledBeforeServerClose.Load())
}

func TestHeartbeatOnlyReportsStartedReplicas(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		r := newReplica(newTestTNShard(1, 2, 3), s.rt)
		s.replicas.Store(r.shard.ShardID, r)
		require.Empty(t, s.getTNShardInfo())

		sender := service.NewTestSender()
		t.Cleanup(func() { require.NoError(t, sender.Close()) })
		txnService := service.NewTestTxnService(t, 1, sender, service.NewTestClock(1))
		require.NoError(t, r.start(txnService))
		require.Equal(t, []logservicepb.TNShardInfo{{
			ShardID: 1, ReplicaID: 2,
		}}, s.getTNShardInfo())
	})
}

func TestReplicaCreateRetryStopsOnStoreStop(t *testing.T) {
	oldInterval := retryCreateStorageInterval
	retryCreateStorageInterval = time.Hour
	t.Cleanup(func() { retryCreateStorageInterval = oldInterval })

	createAttempted := make(chan struct{}, 1)
	var createAttempts atomic.Int32
	runTNStoreTest(t, func(s *store) {
		s.options.logServiceClientFactory = func(metadata.TNShard) (logservice.Client, error) {
			createAttempts.Add(1)
			select {
			case createAttempted <- struct{}{}:
			default:
			}
			return nil, errors.New("injected log client creation failure")
		}

		require.NoError(t, s.StartTNReplica(newTestTNShard(102, 202, 302)))
		select {
		case <-createAttempted:
		case <-time.After(storeTestTimeout):
			t.Fatal("storage creation was not attempted")
		}

		stopped := make(chan struct{})
		go func() {
			s.stopper.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(storeTestTimeout):
			t.Fatal("store stop did not cancel replica creation retry")
		}
		require.Equal(t, int32(1), createAttempts.Load())
	})
}

func TestConcurrentStartTNReplicaPersistsAllShards(t *testing.T) {
	const replicaCount = 24
	runTNStoreTest(t, func(s *store) {
		var wg sync.WaitGroup
		errs := make(chan error, replicaCount)
		for i := 0; i < replicaCount; i++ {
			shardID := uint64(1000 + i)
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- s.StartTNReplica(newTestTNShard(shardID, shardID+1000, shardID+2000))
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}

		s.mu.RLock()
		shards := append([]metadata.TNShard(nil), s.mu.metadata.Shards...)
		s.mu.RUnlock()
		require.Len(t, shards, replicaCount)
		seen := make(map[uint64]struct{}, replicaCount)
		for _, shard := range shards {
			seen[shard.ShardID] = struct{}{}
		}
		require.Len(t, seen, replicaCount)
	})
}

func runTNStoreTest(
	t *testing.T,
	testFn func(*store),
	opts ...Option) {
	runTNStoreTestWithFileServiceFactory(t, testFn, func(name string) (*fileservice.FileServices, error) {
		local, err := fileservice.NewMemoryFS(
			defines.LocalFileServiceName,
			fileservice.DisabledCacheConfig, nil,
		)
		if err != nil {
			return nil, err
		}
		s3, err := fileservice.NewMemoryFS(
			defines.SharedFileServiceName,
			fileservice.DisabledCacheConfig, nil,
		)
		if err != nil {
			return nil, err
		}
		etl, err := fileservice.NewMemoryFS(
			defines.ETLFileServiceName,
			fileservice.DisabledCacheConfig, nil,
		)
		if err != nil {
			return nil, err
		}
		return fileservice.NewFileServices(name, local, s3, etl)
	}, opts...)
}

func runTNStoreTestWithFileServiceFactory(
	t *testing.T,
	testFn func(*store),
	fsFactory fileservice.NewFileServicesFunc,
	opts ...Option) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	thc := newTestHAKeeperClient()
	opts = append(opts,
		WithHAKeeperClientFactory(func() (logservice.TNHAKeeperClient, error) {
			return thc, nil
		}),
		WithLogServiceClientFactory(func(d metadata.TNShard) (logservice.Client, error) {
			return mem.NewMemLog(), nil
		}),
		WithConfigAdjust(func(c *Config) {
			c.HAKeeper.HeatbeatInterval.Duration = time.Millisecond * 10
			c.Txn.Storage.Backend = StorageMEMKV
		}))

	if fsFactory == nil {
		fsFactory = func(name string) (*fileservice.FileServices, error) {
			fs, err := fileservice.NewMemoryFS(name, fileservice.DisabledCacheConfig, nil)
			if err != nil {
				return nil, err
			}
			return fileservice.NewFileServices(name, fs)
		}
	}
	runtime.SetupServiceBasedRuntime("u1", runtime.ServiceRuntime(""))
	s := newTestStore(t, "u1", fsFactory, opts...)
	defer func() {
		assert.NoError(t, s.Close())
	}()
	assert.NoError(t, s.Start())
	testFn(s)
}

func addTestReplica(t *testing.T, s *store, shardID, replicaID, logShardID uint64) {
	thc := s.hakeeperClient.(*testHAKeeperClient)
	thc.setCommandBatch(logservicepb.CommandBatch{
		Commands: []logservicepb.ScheduleCommand{
			{
				ServiceType: logservicepb.TNService,
				ConfigChange: &logservicepb.ConfigChange{
					ChangeType: logservicepb.AddReplica,
					Replica: logservicepb.Replica{
						LogShardID: logShardID,
						ReplicaID:  replicaID,
						ShardID:    shardID,
					},
				},
			},
		},
	})

	for {
		r := s.getReplica(1)
		if r != nil {
			assert.NoError(t, r.waitStarted(context.Background()))
			assert.Equal(t, newTestTNShard(shardID, replicaID, logShardID), r.shard)
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func newTestStore(
	t *testing.T,
	uuid string,
	fsFactory fileservice.NewFileServicesFunc,
	options ...Option) *store {
	assert.NoError(t, os.RemoveAll(testTNStoreAddr[7:]))
	c := &Config{
		UUID:           uuid,
		ListenAddress:  testTNStoreAddr,
		ServiceAddress: testTNStoreAddr,
	}
	c.LogtailServer.ListenAddress = testTNLogtailAddress
	fs, err := fsFactory(defines.LocalFileServiceName)
	assert.Nil(t, err)

	rt := runtime.NewRuntime(
		metadata.ServiceType_TN,
		uuid,
		logutil.Adjust(nil),
		runtime.WithClock(
			clock.NewHLCClock(
				func() int64 { return time.Now().UTC().UnixNano() },
				time.Duration(math.MaxInt64))))
	s, err := NewService(
		c,
		rt,
		fs,
		nil,
		options...)
	assert.NoError(t, err)
	return s.(*store)
}

func newTestTNShard(shardID, replicaID, logShardID uint64) metadata.TNShard {
	tnShard := service.NewTestTNShard(shardID)
	tnShard.ReplicaID = replicaID
	tnShard.LogShardID = logShardID
	tnShard.Address = testTNStoreAddr
	return tnShard
}

type testHAKeeperClient struct {
	mu struct {
		sync.RWMutex
		commandBatch logservicepb.CommandBatch
	}

	atomic struct {
		count uint64
	}
}

func newTestHAKeeperClient() *testHAKeeperClient {
	return &testHAKeeperClient{}
}

func (thc *testHAKeeperClient) setCommandBatch(commandBatch logservicepb.CommandBatch) {
	thc.mu.Lock()
	defer thc.mu.Unlock()
	thc.mu.commandBatch = commandBatch
}

func (thc *testHAKeeperClient) getCount() uint64 {
	return atomic.LoadUint64(&thc.atomic.count)
}

func (thc *testHAKeeperClient) Close() error {
	return nil
}

func (thc *testHAKeeperClient) SendTNHeartbeat(ctx context.Context, hb logservicepb.TNStoreHeartbeat) (logservicepb.CommandBatch, error) {
	atomic.AddUint64(&thc.atomic.count, 1)
	thc.mu.RLock()
	defer thc.mu.RUnlock()
	return thc.mu.commandBatch, nil
}

var nextID uint64

func (thc *testHAKeeperClient) AllocateID(ctx context.Context) (uint64, error) {
	return atomic.AddUint64(&nextID, 1), nil
}

func (thc *testHAKeeperClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return atomic.AddUint64(&nextID, 1), nil
}

func (thc *testHAKeeperClient) AllocateIDByKeyWithBatch(ctx context.Context, key string, batch uint64) (uint64, error) {
	return atomic.AddUint64(&nextID, 1), nil
}

func (thc *testHAKeeperClient) GetClusterDetails(ctx context.Context) (logservicepb.ClusterDetails, error) {
	return logservicepb.ClusterDetails{}, nil
}
func (thc *testHAKeeperClient) GetClusterState(ctx context.Context) (logservicepb.CheckerState, error) {
	return logservicepb.CheckerState{}, nil
}

func (c *testHAKeeperClient) CheckLogServiceHealth(_ context.Context) error {
	return nil
}
