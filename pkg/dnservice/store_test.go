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

package dnservice

import (
	"context"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/stretchr/testify/assert"
)

var (
	testDNStoreAddr = "unix:///tmp/test-dnstore.sock"
	testDataDir     = "/tmp/mo/dn"
)

func TestNewAndStartAndCloseService(t *testing.T) {
	runDNStoreTest(t, func(s *store) {
		thc := s.hakeeperClient.(*testHAKeeperClient)
		for {
			if v := thc.getCount(); v > 0 {
				return
			}
		}
	})
}

func TestAddReplica(t *testing.T) {
	runDNStoreTest(t, func(s *store) {
		thc := s.hakeeperClient.(*testHAKeeperClient)
		thc.setCommandBatch(logservicepb.CommandBatch{
			Commands: []logservicepb.ScheduleCommand{
				{
					ServiceType: logservicepb.DnService,
					ConfigChange: &logservicepb.ConfigChange{
						ChangeType: logservicepb.AddReplica,
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
			if r != nil {
				r.waitStarted()
				assert.Equal(t, newTestDNShard(1, 2, 3), r.shard)
				return
			}
			time.Sleep(time.Millisecond * 10)
		}
	})
}

func TestStartReplica(t *testing.T) {
	runDNStoreTest(t, func(s *store) {
		assert.NoError(t, s.StartDNReplica(newTestDNShard(1, 2, 3)))
		r := s.getReplica(1)
		r.waitStarted()
		assert.Equal(t, newTestDNShard(1, 2, 3), r.shard)
	})
}

func TestRemoveReplica(t *testing.T) {
	runDNStoreTest(t, func(s *store) {
		assert.NoError(t, s.StartDNReplica(newTestDNShard(1, 2, 3)))
		r := s.getReplica(1)
		r.waitStarted()

		thc := s.hakeeperClient.(*testHAKeeperClient)
		thc.setCommandBatch(logservicepb.CommandBatch{
			Commands: []logservicepb.ScheduleCommand{
				{
					ServiceType: logservicepb.DnService,
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
	runDNStoreTest(t, func(s *store) {
		shard := newTestDNShard(1, 2, 3)
		assert.NoError(t, s.StartDNReplica(shard))
		r := s.getReplica(1)
		r.waitStarted()
		assert.Equal(t, shard, r.shard)

		assert.NoError(t, s.CloseDNReplica(shard))
		assert.Nil(t, s.getReplica(1))
	})
}

func runDNStoreTest(t *testing.T, testFn func(*store), opts ...Option) {
	thc := newTestHAKeeperClient()

	opts = append(opts, WithHAKeeperClientFactory(func() (logservice.DNHAKeeperClient, error) {
		return thc, nil
	}),
		WithLogServiceClientFactory(func(d metadata.DNShard) (logservice.Client, error) {
			return mem.NewMemLog(), nil
		}),
		WithConfigAdjust(func(c *Config) {
			c.HAKeeper.HeatbeatDuration.Duration = time.Millisecond * 10
			c.Txn.Storage.Backend = memStorageBackend
		}))

	s := newTestStore(t, "u1", opts...)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	assert.NoError(t, s.Start())
	testFn(s)
}

func newTestStore(t *testing.T, uuid string, options ...Option) *store {
	assert.NoError(t, os.RemoveAll(testDataDir))
	assert.NoError(t, os.RemoveAll(testDNStoreAddr[7:]))
	assert.NoError(t, os.MkdirAll(testDataDir, 0755))
	c := &Config{
		UUID:          uuid,
		DataDir:       testDataDir,
		ListenAddress: testDNStoreAddr,
	}
	c.Txn.Clock.MaxClockOffset.Duration = time.Duration(math.MaxInt64)
	s, err := NewService(c, options...)
	assert.NoError(t, err)
	return s.(*store)
}

func newTestDNShard(shardID, replicaID, logShardID uint64) metadata.DNShard {
	dnShard := service.NewTestDNShard(shardID)
	dnShard.ReplicaID = replicaID
	dnShard.LogShardID = logShardID
	dnShard.Address = testDNStoreAddr
	return dnShard
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

func (thc *testHAKeeperClient) SendDNHeartbeat(ctx context.Context, hb logservicepb.DNStoreHeartbeat) (logservicepb.CommandBatch, error) {
	atomic.AddUint64(&thc.atomic.count, 1)
	thc.mu.RLock()
	defer thc.mu.RUnlock()
	return thc.mu.commandBatch, nil
}
