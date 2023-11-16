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
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/stretchr/testify/assert"
)

var (
	testTNStoreAddr      = "unix:///tmp/test-dnstore.sock"
	testTNLogtailAddress = "127.0.0.1:22001"
)

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
		r.waitStarted()
		assert.Equal(t, newTestTNShard(1, 2, 3), r.shard)
	})
}

func TestRemoveReplica(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		assert.NoError(t, s.StartTNReplica(newTestTNShard(1, 2, 3)))
		r := s.getReplica(1)
		r.waitStarted()

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
		r.waitStarted()
		assert.Equal(t, shard, r.shard)

		assert.NoError(t, s.CloseTNReplica(shard))
		assert.Nil(t, s.getReplica(1))
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
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
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
			r.waitStarted()
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
		"",
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
