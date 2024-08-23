// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

func TestCreateShards(t *testing.T) {
	runServicesTest(
		t,
		"cn1",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			table := uint64(1)
			shards := uint32(1)
			mustAddTestShards(t, ctx, services[0], table, shards, 1)
			waitReplicaCount(table, services[0], 1)
		},
		nil,
	)
}

func TestCreateShardsWithSkip(t *testing.T) {
	runServicesTest(
		t,
		"cn1",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			table := uint64(1)
			shards := uint32(1)

			txnOp, close := client.NewTestTxnOperator(ctx)
			defer close()

			s1 := services[0]
			addTestUncommittedTable(s1, table, shards, pb.Policy_None, 1)

			require.NoError(t, s1.Create(context.TODO(), table, txnOp))
			require.NoError(t, txnOp.Commit(ctx))
			require.Equal(t, uint64(1), s1.atomic.skip.Load())

		},
		nil,
	)
}

func TestCreateShardsWithTxnAborted(t *testing.T) {
	runServicesTest(
		t,
		"cn1",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			table := uint64(1)
			shards := uint32(1)

			txnOp, close := client.NewTestTxnOperator(ctx)
			defer close()

			s1 := services[0]
			addTestUncommittedTable(s1, table, shards, pb.Policy_Hash, 1)

			require.NoError(t, s1.Create(context.TODO(), table, txnOp))
			require.NoError(t, txnOp.Rollback(ctx))
			require.Equal(t, uint64(1), s1.atomic.abort.Load())
		},
		nil,
	)
}

func TestDeleteShards(t *testing.T) {
	runServicesTest(
		t,
		"cn1",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			table := uint64(1)
			shards := uint32(1)
			mustAddTestShards(t, ctx, s1, table, shards, 1)
			waitReplicaCount(table, s1, 1)

			txnOp, close := client.NewTestTxnOperator(ctx)
			defer close()

			require.NoError(t, s1.Delete(context.TODO(), table, txnOp))
			require.NoError(t, txnOp.Commit(ctx))

			waitReplicaCount(table, s1, 0)
		},
		nil,
	)
}

func TestAsyncDeleteShards(t *testing.T) {
	runServicesTest(
		t,
		"cn1",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			table := uint64(1)
			shards := uint32(1)
			mustAddTestShards(t, ctx, s1, table, shards, 1)
			waitReplicaCount(table, s1, 1)

			txnOp, close := client.NewTestTxnOperator(ctx)
			defer close()

			require.NoError(t, s1.Delete(context.TODO(), table, txnOp))
			require.NoError(t, txnOp.Commit(ctx))

			waitReplicaCount(table, s1, 0)
		},
		func(c *Config) []Option {
			return []Option{withDisableAppendDeleteCallback()}
		},
	)
}

func TestAsyncUpdateShards(t *testing.T) {
	runServicesTest(
		t,
		"cn1",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			table := uint64(1)
			shards := uint32(1)
			mustAddTestShards(t, ctx, s1, table, shards, 1)
			waitReplicaCount(table, s1, 1)

			store := s1.storage.(*MemShardStorage)
			store.Lock()
			v := store.committed[table]
			v.Version++
			v.ShardsCount = 2
			v.ShardIDs = []uint64{1, 2}
			store.committed[table] = v
			store.Unlock()
			waitReplicaCount(table, s1, 2)
			require.Equal(t, uint64(3), s1.atomic.added.Load())
			require.Equal(t, uint64(1), s1.atomic.removed.Load())
		},
		nil,
	)
}

func TestDeleteShardsWithTxnAborted(t *testing.T) {
	runServicesTest(
		t,
		"cn1",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			table := uint64(1)
			shards := uint32(1)
			mustAddTestShards(t, ctx, s1, table, shards, 1)
			waitReplicaCount(table, s1, 1)

			txnOp, close := client.NewTestTxnOperator(ctx)
			defer close()

			require.NoError(t, s1.Delete(context.TODO(), table, txnOp))
			require.NoError(t, txnOp.Rollback(ctx))
			require.Equal(t, uint64(1), s1.atomic.abort.Load())
		},
		nil,
	)
}

func TestDeleteShardsWithSkip(t *testing.T) {
	runServicesTest(
		t,
		"cn1",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			table := uint64(1)
			shards := uint32(1)

			txnOp, close := client.NewTestTxnOperator(ctx)
			defer close()

			s1 := services[0]
			addTestCommittedTable(s1, table, shards, pb.Policy_None)

			require.NoError(t, s1.Delete(context.TODO(), table, txnOp))
			require.NoError(t, txnOp.Commit(ctx))
			require.Equal(t, uint64(1), s1.atomic.skip.Load())
		},
		nil,
	)
}

func TestGetShardsWithAllocated(t *testing.T) {
	runServicesTest(
		t,
		"cn1",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			table := uint64(1)
			shards := uint32(1)
			mustAddTestShards(t, ctx, s1, table, shards, 1)
			waitReplicaCount(table, s1, 1)

			cache, err := s1.getShards(table)
			require.NoError(t, err)
			require.True(t, cache.hasTableCache(table))
		},
		nil,
	)
}

func TestGetShardsWithoutCreate(t *testing.T) {
	runServicesTest(
		t,
		"cn1",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			table := uint64(1)
			shards := uint32(1)

			addTestCommittedTable(s1, table, shards, pb.Policy_Hash)

			cache, err := s1.getShards(table)
			require.NoError(t, err)
			require.True(t, cache.hasTableCache(table))
			require.Equal(t, 1, len(s1.getAllocatedShards()))
		},
		nil,
	)
}

func TestShardCanBeAllocated(t *testing.T) {
	runServicesTest(
		t,
		"cn1",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			table := uint64(1)
			shards := uint32(1)
			mustAddTestShards(t, ctx, s1, table, shards, 1)
			waitReplicaCount(table, s1, 1)
		},
		nil,
	)
}

func TestShardCanBeAllocatedToMultiCN(t *testing.T) {
	runServicesTest(
		t,
		"cn1,cn2",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			s2 := services[1]
			table := uint64(1)
			shards := uint32(2)
			mustAddTestShards(t, ctx, s1, table, shards, 1, s2)

			waitReplicaCount(table, s1, 1)
			waitReplicaCount(table, s2, 1)
		},
		nil,
	)
}

func TestShardCanBeAllocatedWithLabel(t *testing.T) {
	runServicesTest(
		t,
		"cn1:account:100,cn2:account:1",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s2 := services[1]
			table := uint64(1)
			shards := uint32(2)

			txnOp, close := client.NewTestTxnOperator(ctx)
			defer close()

			addTestUncommittedTable(s2, table, shards, pb.Policy_Hash, 1)
			require.NoError(t, s2.Create(context.TODO(), table, txnOp))
			require.NoError(t, txnOp.Commit(ctx))

			waitReplicaCount(table, s2, 2)
		},
		func(c *Config) []Option {
			c.SelectCNLabel = "account"
			return nil
		},
	)
}

func TestBalanceWithSingleTable(t *testing.T) {
	runServicesTest(
		t,
		"cn1,cn2,cn3",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			s2 := services[1]
			s3 := services[2]
			table := uint64(1)
			shards := uint32(3)
			mustAddTestShards(t, ctx, s1, table, shards, 1)
			waitReplicaCount(table, s1, 3)

			s2.options.disableHeartbeat.Store(false)
			s3.options.disableHeartbeat.Store(false)
			waitReplicaCount(table, s1, 1)
			waitReplicaCount(table, s2, 1)
			waitReplicaCount(table, s3, 1)
		},
		func(c *Config) []Option {
			c.FreezeCNTimeout.Duration = time.Millisecond * 10

			if c.ServiceID != "cn1" {
				return []Option{withDisableHeartbeat()}
			}
			return nil
		},
	)
}

func TestBalanceWithMultiTable(t *testing.T) {
	runServicesTest(
		t,
		"cn1,cn2,cn3",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			n := 10
			tables := make([]uint64, 0, n)
			for i := 0; i < n; i++ {
				tables = append(tables, uint64(i+1))
			}

			s1 := services[0]
			s2 := services[1]
			s3 := services[2]
			shards := uint32(3)

			for _, table := range tables {
				mustAddTestShards(t, ctx, s1, table, shards, 1, s2, s3)
				waitReplicaCount(table, s1, 3)
			}

			s2.options.disableHeartbeat.Store(false)
			s3.options.disableHeartbeat.Store(false)
			for _, table := range tables {
				waitReplicaCount(table, s1, 1)
				waitReplicaCount(table, s2, 1)
				waitReplicaCount(table, s3, 1)
			}
		},
		func(c *Config) []Option {
			c.FreezeCNTimeout.Duration = time.Millisecond * 10

			if c.ServiceID != "cn1" {
				return []Option{withDisableHeartbeat()}
			}
			return nil
		},
	)
}

func TestBalanceWithMultiTableAndMultiShards(t *testing.T) {
	runServicesTest(
		t,
		"cn1,cn2,cn3",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			n := 10
			tables := make([]uint64, 0, n)
			for i := 0; i < n; i++ {
				tables = append(tables, uint64(i+1))
			}

			s1 := services[0]
			s2 := services[1]
			s3 := services[2]
			shards := uint32(3)
			replicas := uint32(3)

			for _, table := range tables {
				mustAddTestShards(t, ctx, s1, table, shards, replicas, s2, s3)
				waitReplicaCount(table, s1, int(shards*replicas))
			}

			s2.options.disableHeartbeat.Store(false)
			s3.options.disableHeartbeat.Store(false)
			for _, table := range tables {
				waitReplicaCount(table, s1, int(shards*replicas)/3)
				waitReplicaCount(table, s2, int(shards*replicas)/3)
				waitReplicaCount(table, s3, int(shards*replicas)/3)
			}
		},
		func(c *Config) []Option {
			c.FreezeCNTimeout.Duration = time.Millisecond * 10

			if c.ServiceID != "cn1" {
				return []Option{withDisableHeartbeat()}
			}
			return nil
		},
	)
}

func TestForceUnsubscribe(t *testing.T) {
	runServicesTest(
		t,
		"cn1,cn2",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			table := uint64(1)
			shards := uint32(4)
			mustAddTestShards(t, ctx, s1, table, shards, 1, services[1])
			waitReplicaCount(table, s1, 4)

			s2 := services[1]
			s2.options.disableHeartbeat.Store(false)
			waitReplicaCount(table, s1, 2)
			waitReplicaCount(table, s2, 2)

			require.Equal(t, 1, s2.storage.(*MemShardStorage).UnsubscribeCount(table))
		},
		func(c *Config) []Option {
			c.FreezeCNTimeout.Duration = time.Millisecond * 10
			if c.ServiceID != "cn1" {
				return []Option{withDisableHeartbeat()}
			}
			return nil
		},
	)
}

func TestMoveReplicaFromPauseCN(t *testing.T) {
	runServicesTest(
		t,
		"cn1,cn2,cn3",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			s2 := services[1]
			s3 := services[2]
			table := uint64(1)
			shards := uint32(6)
			mustAddTestShards(t, ctx, s1, table, shards, 1, s2, s3)
			waitReplicaCount(table, s1, 6)

			s2.options.disableHeartbeat.Store(false)
			s3.options.disableHeartbeat.Store(false)
			waitReplicaCount(table, s1, 2)
			waitReplicaCount(table, s2, 2)
			waitReplicaCount(table, s3, 2)

			// make cn1 pause
			server.env.UpdateState("cn1", metadata.WorkState_Draining)

			waitReplicaCount(table, s1, 0)
			waitReplicaCount(table, s2, 3)
			waitReplicaCount(table, s3, 3)

			// make cn1 work
			server.env.UpdateState("cn1", metadata.WorkState_Working)
			waitReplicaCount(table, s1, 2)
			waitReplicaCount(table, s2, 2)
			waitReplicaCount(table, s3, 2)
		},
		func(c *Config) []Option {
			c.FreezeCNTimeout.Duration = time.Millisecond * 10

			if c.ServiceID != "cn1" {
				return []Option{withDisableHeartbeat()}
			}
			return nil
		},
	)
}

func addTestUncommittedTable(
	s *service,
	tableID uint64,
	shardCount uint32,
	policy pb.Policy,
	replicas uint32,
) {
	shardIDs := make([]uint64, 0, shardCount)
	for i := uint64(1); i <= uint64(shardCount); i++ {
		shardIDs = append(shardIDs, i)
	}

	store := s.storage.(*MemShardStorage)
	store.UncommittedAdd(
		tableID,
		pb.ShardsMetadata{
			AccountID:       1,
			ShardsCount:     shardCount,
			Policy:          policy,
			Version:         1,
			MaxReplicaCount: replicas,
			ShardIDs:        shardIDs,
		},
	)
}

func addTestCommittedTable(
	s *service,
	tableID uint64,
	shardCount uint32,
	policy pb.Policy,
) {
	shardIDs := make([]uint64, 0, shardCount)
	for i := uint64(1); i <= uint64(shardCount); i++ {
		shardIDs = append(shardIDs, i)
	}

	store := s.storage.(*MemShardStorage)
	store.AddCommitted(
		tableID,
		pb.ShardsMetadata{
			AccountID:   1,
			ShardsCount: shardCount,
			Policy:      policy,
			Version:     1,
			ShardIDs:    shardIDs,
		})
}

func mustAddTestShards(
	t *testing.T,
	ctx context.Context,
	s *service,
	table uint64,
	shards uint32,
	replicas uint32,
	others ...*service,
) {
	txnOp, close := client.NewTestTxnOperator(ctx)
	defer close()

	addTestUncommittedTable(s, table, shards, pb.Policy_Hash, replicas)
	require.NoError(t, s.Create(context.TODO(), table, txnOp))
	require.NoError(t, txnOp.Commit(ctx))

	store := s.storage.(*MemShardStorage)
	store.RLock()
	for _, o := range others {
		store2 := o.storage.(*MemShardStorage)
		store2.Lock()
		for k, m := range store.committed {
			store2.committed[k] = m
		}
		store2.Unlock()
	}
	store.RUnlock()
}

func mustAddTestPartitionShards(
	t *testing.T,
	ctx context.Context,
	s *service,
	table uint64,
	shards uint32,
	replicas uint32,
	others ...*service,
) {
	txnOp, close := client.NewTestTxnOperator(ctx)
	defer close()

	addTestUncommittedTable(
		s,
		table,
		shards,
		pb.Policy_Partition,
		replicas,
	)
	require.NoError(t, s.Create(context.TODO(), table, txnOp))
	require.NoError(t, txnOp.Commit(ctx))

	store := s.storage.(*MemShardStorage)
	store.RLock()
	for _, o := range others {
		store2 := o.storage.(*MemShardStorage)
		store2.Lock()
		for k, m := range store.committed {
			store2.committed[k] = m
		}
		store2.Unlock()
	}
	store.RUnlock()
}

func waitReplicaCount(
	table uint64,
	s *service,
	count int,
) {
	for {
		shards := s.getAllocatedShards()
		n := 0
		for _, s := range shards {
			if s.TableID == table {
				n += len(s.Replicas)
			}
		}
		if n == count {
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func runServicesTest(
	t *testing.T,
	cluster string,
	fn func(context.Context, *server, []*service),
	adjustConfigFunc func(*Config) []Option,
) {
	leaktest.AfterTest(t)()
	cns, tn := initTestCluster(cluster)

	cfg := Config{
		Enable:        true,
		ServiceID:     tn.ServiceID,
		ListenAddress: tn.ShardServiceAddress,
	}
	cfg.ScheduleDuration.Duration = time.Millisecond * 10
	if adjustConfigFunc != nil {
		adjustConfigFunc(&cfg)
	}
	server := NewShardServer(cfg, runtime.ServiceRuntime(sid).Logger()).(*server)

	services := make([]*service, 0, len(cns))
	for _, cn := range cns {
		if err := os.RemoveAll(cn.ShardServiceAddress[7:]); err != nil {
			panic(err)
		}

		cfg := Config{
			Enable:        true,
			ServiceID:     cn.ServiceID,
			ListenAddress: cn.ShardServiceAddress,
		}
		cfg.HeartbeatDuration.Duration = time.Millisecond * 10
		cfg.CheckChangedDuration.Duration = time.Millisecond * 10

		var opts []Option
		if adjustConfigFunc != nil {
			opts = adjustConfigFunc(&cfg)
		}

		s := NewService(cfg, NewMemShardStorage(runtime.ServiceRuntime(sid).Logger()), opts...)
		services = append(services, s.(*service))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	fn(ctx, server, services)

	for _, s := range services {
		require.NoError(t, s.Close())
	}
	require.NoError(t, server.Close())
}
