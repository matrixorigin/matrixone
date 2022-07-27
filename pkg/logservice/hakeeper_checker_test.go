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

package logservice

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func TestIDAllocatorDefaultState(t *testing.T) {
	alloc := newIDAllocator()
	assert.Equal(t, uint64(0), alloc.Capacity())
	v, ok := alloc.Next()
	assert.False(t, ok)
	assert.Equal(t, uint64(0), v)
}

func TestIDAllocatorCapacity(t *testing.T) {
	tests := []struct {
		next     uint64
		last     uint64
		capacity uint64
	}{
		{1, 1, 1},
		{2, 1, 0},
		{1, 2, 2},
		{100, 200, 101},
	}

	for _, tt := range tests {
		alloc := idAllocator{nextID: tt.next, lastID: tt.last}
		assert.Equal(t, tt.capacity, alloc.Capacity())
	}
}

func TestIDAllocatorSet(t *testing.T) {
	alloc := idAllocator{nextID: 100, lastID: 200}
	alloc.Set(hakeeper.K8SIDRangeEnd, hakeeper.K8SIDRangeEnd+100)
	expected := idAllocator{
		nextID: hakeeper.K8SIDRangeEnd,
		lastID: hakeeper.K8SIDRangeEnd + 100,
	}
	assert.Equal(t, expected, alloc)
}

func TestIDAllocatorRejectInvalidSetInput(t *testing.T) {
	alloc := idAllocator{nextID: 100, lastID: 200}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to trigger panic")
		}
	}()
	alloc.Set(300, 400)
}

func TestIDAllocatorNext(t *testing.T) {
	tests := []struct {
		next     uint64
		last     uint64
		capacity uint64
	}{
		{1, 1, 1},
		{2, 1, 0},
		{1, 2, 2},
		{100, 200, 101},
	}

	for _, tt := range tests {
		expected := tt.next
		alloc := idAllocator{nextID: tt.next, lastID: tt.last}
		for {
			hasID := alloc.Capacity() != 0
			v, ok := alloc.Next()
			assert.Equal(t, hasID, ok)
			if hasID {
				assert.Equal(t, expected, v)
				expected++
			} else {
				assert.Equal(t, uint64(0), v)
				break
			}
		}
	}
}

func TestHandleBootstrapFailure(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to trigger panic")
		}
	}()
	s := store{}
	s.handleBootstrapFailure()
}

func runHAKeeperStoreTest(t *testing.T, startLogReplica bool, fn func(*testing.T, *store)) {
	defer leaktest.AfterTest(t)()
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	store, err := getTestStore(cfg, startLogReplica)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, store.close())
	}()
	peers := make(map[uint64]dragonboat.Target)
	peers[1] = store.id()
	assert.NoError(t, store.startHAKeeperReplica(1, peers, false))
	fn(t, store)
}

func runHAKeeperClusterTest(t *testing.T, fn func(*testing.T, []*Service)) {
	defer leaktest.AfterTest(t)()
	cfg1 := Config{
		UUID:                uuid.New().String(),
		FS:                  vfs.NewStrictMem(),
		DeploymentID:        1,
		RTTMillisecond:      5,
		DataDir:             "data-1",
		ServiceAddress:      "127.0.0.1:9002",
		RaftAddress:         "127.0.0.1:9000",
		GossipAddress:       "127.0.0.1:9001",
		GossipSeedAddresses: "127.0.0.1:9011;127.0.0.1:9021;127.0.0.1:9031",
		DisableWorkers:      true,
	}
	cfg1.HAKeeperConfig.TickPerSecond = 10
	cfg1.HAKeeperConfig.LogStoreTimeout.Duration = 5 * time.Second
	cfg1.HAKeeperConfig.DnStoreTimeout.Duration = 10 * time.Second
	cfg2 := Config{
		UUID:                uuid.New().String(),
		FS:                  vfs.NewStrictMem(),
		DeploymentID:        1,
		RTTMillisecond:      5,
		DataDir:             "data-2",
		ServiceAddress:      "127.0.0.1:9012",
		RaftAddress:         "127.0.0.1:9010",
		GossipAddress:       "127.0.0.1:9011",
		GossipSeedAddresses: "127.0.0.1:9001;127.0.0.1:9021;127.0.0.1:9031",
		DisableWorkers:      true,
	}
	cfg2.HAKeeperConfig.TickPerSecond = 10
	cfg2.HAKeeperConfig.LogStoreTimeout.Duration = 5 * time.Second
	cfg2.HAKeeperConfig.DnStoreTimeout.Duration = 10 * time.Second
	cfg3 := Config{
		UUID:                uuid.New().String(),
		FS:                  vfs.NewStrictMem(),
		DeploymentID:        1,
		RTTMillisecond:      5,
		DataDir:             "data-3",
		ServiceAddress:      "127.0.0.1:9022",
		RaftAddress:         "127.0.0.1:9020",
		GossipAddress:       "127.0.0.1:9021",
		GossipSeedAddresses: "127.0.0.1:9001;127.0.0.1:9011;127.0.0.1:9031",
		DisableWorkers:      true,
	}
	cfg3.HAKeeperConfig.TickPerSecond = 10
	cfg3.HAKeeperConfig.LogStoreTimeout.Duration = 5 * time.Second
	cfg3.HAKeeperConfig.DnStoreTimeout.Duration = 10 * time.Second
	cfg4 := Config{
		UUID:                uuid.New().String(),
		FS:                  vfs.NewStrictMem(),
		DeploymentID:        1,
		RTTMillisecond:      5,
		DataDir:             "data-4",
		ServiceAddress:      "127.0.0.1:9032",
		RaftAddress:         "127.0.0.1:9030",
		GossipAddress:       "127.0.0.1:9031",
		GossipSeedAddresses: "127.0.0.1:9001;127.0.0.1:9011;127.0.0.1:9021",
		DisableWorkers:      true,
	}
	cfg4.HAKeeperConfig.TickPerSecond = 10
	cfg4.HAKeeperConfig.LogStoreTimeout.Duration = 5 * time.Second
	cfg4.HAKeeperConfig.DnStoreTimeout.Duration = 10 * time.Second
	cfg1.Fill()
	service1, err := NewService(cfg1)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service1.Close())
	}()
	cfg2.Fill()
	service2, err := NewService(cfg2)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service2.Close())
	}()
	cfg3.Fill()
	service3, err := NewService(cfg3)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service3.Close())
	}()
	cfg4.Fill()
	service4, err := NewService(cfg4)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service4.Close())
	}()

	peers := make(map[uint64]dragonboat.Target)
	peers[1] = service1.ID()
	peers[2] = service2.ID()
	peers[3] = service3.ID()
	assert.NoError(t, service1.store.startHAKeeperReplica(1, peers, false))
	assert.NoError(t, service2.store.startHAKeeperReplica(2, peers, false))
	assert.NoError(t, service3.store.startHAKeeperReplica(3, peers, false))
	fn(t, []*Service{service1, service2, service3, service4})
}

func TestHAKeeperCanBootstrapAndRepairShards(t *testing.T) {
	fn := func(t *testing.T, services []*Service) {
		// bootstrap the cluster, 1 DN 1 Log shard, Log and HAKeeper have
		// 3 replicas
		store1 := services[0].store
		state, err := store1.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperCreated, state.State)
		require.NoError(t, store1.setInitialClusterInfo(1, 1, 3))
		state, err = store1.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperBootstrapping, state.State)

		sendHeartbeat := func(ss []*Service) {
			for _, s := range ss {
				m := s.store.getHeartbeatMessage()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				assert.NoError(t, s.store.addLogStoreHeartbeat(ctx, m))
			}
		}
		sendHeartbeat(services[:3])

		// fake a DN store
		dnMsg := pb.DNStoreHeartbeat{
			UUID:   uuid.New().String(),
			Shards: make([]pb.DNShardInfo, 0),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		require.NoError(t, services[0].store.addDNStoreHeartbeat(ctx, dnMsg))

		// find out the leader HAKeeper store as we need the term value
		var term uint64
		var leaderStore *store
		for _, s := range services[:3] {
			isLeader, curTerm, err := s.store.isLeaderHAKeeper()
			require.NoError(t, err)
			if isLeader {
				term = curTerm
				leaderStore = s.store
				break
			}
		}
		require.NotNil(t, leaderStore)
		require.True(t, term > 0)

		// bootstrap the cluster
		state, err = leaderStore.getCheckerState()
		require.NoError(t, err)
		leaderStore.bootstrap(term, state)

		state, err = leaderStore.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperBootstrapCommandsReceived, state.State)
		assert.Equal(t, uint64(checkBootstrapInterval), leaderStore.bootstrapCheckInterval)
		require.NotNil(t, leaderStore.bootstrapMgr)
		assert.False(t, leaderStore.bootstrapMgr.CheckBootstrap(state.LogState))

		// get and apply all bootstrap schedule commands
		for _, s := range services[:3] {
			cb, err := s.store.getCommandBatch(ctx, s.store.id())
			require.NoError(t, err)
			if len(cb.Commands) > 0 {
				s.handleStartReplica(cb.Commands[0])
			}
		}

		// check bootstrap can be completed
		for i := 0; i < 100; i++ {
			sendHeartbeat(services[:3])
			state, err = leaderStore.getCheckerState()
			require.NoError(t, err)
			leaderStore.checkBootstrap(state)

			state, err = leaderStore.getCheckerState()
			require.NoError(t, err)
			if state.State != pb.HAKeeperRunning {
				// FIXME: why wait here?
				time.Sleep(50 * time.Millisecond)
			} else {
				break
			}
			if i == 99 {
				t.Fatalf("failed to complete bootstrap")
			}
		}

		// get the DN bootstrap command, it contains DN shard and replica ID
		cb, err := leaderStore.getCommandBatch(ctx, dnMsg.UUID)
		require.NoError(t, err)
		require.Equal(t, 1, len(cb.Commands))
		cmd := cb.Commands[0]
		assert.True(t, cmd.Bootstrapping)
		assert.Equal(t, pb.DnService, cmd.ServiceType)
		dnShardInfo := pb.DNShardInfo{
			ShardID:   cmd.ConfigChange.Replica.ShardID,
			ReplicaID: cmd.ConfigChange.Replica.ReplicaID,
		}
		dnMsg.Shards = append(dnMsg.Shards, dnShardInfo)
		// as if DN is running
		require.NoError(t, services[0].store.addDNStoreHeartbeat(ctx, dnMsg))
		// fake a free DN store
		dnMsg2 := pb.DNStoreHeartbeat{
			UUID:   uuid.New().String(),
			Shards: make([]pb.DNShardInfo, 0),
		}
		require.NoError(t, services[0].store.addDNStoreHeartbeat(ctx, dnMsg2))

		// stop store 1
		require.NoError(t, services[0].Close())
		// no service.Close can be repeatedly called
		services[0].store = nil
		services = services[1:]

		// wait for HAKeeper to repair the Log & HAKeeper shards
		dnRepaired := false
		for i := 0; i < 5000; i++ {
			tn := func() (bool, error) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				m := services[0].store.getHeartbeatMessage()
				if err := services[0].store.addLogStoreHeartbeat(ctx, m); err != nil {
					return false, err
				}
				m = services[1].store.getHeartbeatMessage()
				if err := services[1].store.addLogStoreHeartbeat(ctx, m); err != nil {
					return false, err
				}
				m = services[2].store.getHeartbeatMessage()
				if err := services[0].store.addLogStoreHeartbeat(ctx, m); err != nil {
					return false, err
				}
				if err := services[0].store.addDNStoreHeartbeat(ctx, dnMsg2); err != nil {
					return false, err
				}

				for _, s := range services {
					if hasShard(s.store, 0) {
						s.store.hakeeperTick()
						s.store.hakeeperCheck()
					}

					cb, err = services[0].store.getCommandBatch(ctx, dnMsg2.UUID)
					if err != nil {
						return false, err
					}
					if len(cb.Commands) > 0 {
						cmd := cb.Commands[0]
						if cmd.ServiceType == pb.DnService {
							if cmd.ConfigChange.Replica.ShardID == dnShardInfo.ShardID &&
								cmd.ConfigChange.Replica.ReplicaID > dnShardInfo.ReplicaID {
								dnRepaired = true
							}
						}
					}

					cb, err = services[0].store.getCommandBatch(ctx, s.store.id())
					if err != nil {
						return false, err
					}
					for _, cmd := range cb.Commands {
						plog.Infof("store returned schedule command: %s", cmd.LogString())
					}
					s.handleCommands(cb.Commands)
				}

				logRepaired := true
				for _, s := range services {
					if !hasShard(s.store, 0) || !hasShard(s.store, 1) {
						logRepaired = false
						break
					}
				}
				plog.Infof("dnRepairted %t, logRepaired %t", dnRepaired, logRepaired)
				if !logRepaired || !dnRepaired {
					return false, nil
				} else {
					plog.Infof("repair completed, i: %d", i)
					return true, nil
				}
			}
			completed, err := tn()
			if err != nil && err != dragonboat.ErrTimeout {
				t.Fatalf("unexpected error %v", err)
			}
			if completed {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("failed to repair shards")
	}
	runHAKeeperClusterTest(t, fn)
}

func TestGetCheckerState(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		state, err := store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperCreated, state.State)
	}
	runHAKeeperStoreTest(t, false, fn)
}

func TestSetInitialClusterInfo(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		state, err := store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperCreated, state.State)
		require.NoError(t, store.setInitialClusterInfo(1, 1, 1))
		state, err = store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperBootstrapping, state.State)
	}
	runHAKeeperStoreTest(t, false, fn)
}

func TestFailedBootstrap(t *testing.T) {
	testBootstrap(t, true)
}

func TestBootstrap(t *testing.T) {
	testBootstrap(t, false)
}

func testBootstrap(t *testing.T, fail bool) {
	fn := func(t *testing.T, store *store) {
		state, err := store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperCreated, state.State)
		require.NoError(t, store.setInitialClusterInfo(1, 1, 1))
		state, err = store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperBootstrapping, state.State)
		m := store.getHeartbeatMessage()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		assert.NoError(t, store.addLogStoreHeartbeat(ctx, m))

		dnMsg := pb.DNStoreHeartbeat{
			UUID:   uuid.New().String(),
			Shards: make([]pb.DNShardInfo, 0),
		}
		assert.NoError(t, store.addDNStoreHeartbeat(ctx, dnMsg))

		_, term, err := store.isLeaderHAKeeper()
		require.NoError(t, err)

		state, err = store.getCheckerState()
		require.NoError(t, err)
		store.bootstrap(term, state)

		state, err = store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperBootstrapCommandsReceived, state.State)
		assert.Equal(t, uint64(checkBootstrapInterval), store.bootstrapCheckInterval)
		require.NotNil(t, store.bootstrapMgr)
		assert.False(t, store.bootstrapMgr.CheckBootstrap(state.LogState))

		if fail {
			// keep checking, bootstrap will eventually be set as failed
			for i := 0; i <= checkBootstrapInterval; i++ {
				store.checkBootstrap(state)
			}

			state, err = store.getCheckerState()
			require.NoError(t, err)
			assert.Equal(t, pb.HAKeeperBootstrapFailed, state.State)
		} else {
			cb, err := store.getCommandBatch(ctx, dnMsg.UUID)
			require.NoError(t, err)
			require.Equal(t, 1, len(cb.Commands))
			assert.True(t, cb.Commands[0].Bootstrapping)
			assert.Equal(t, pb.DnService, cb.Commands[0].ServiceType)
			assert.True(t, cb.Commands[0].ConfigChange.Replica.ReplicaID > 0)

			cb, err = store.getCommandBatch(ctx, store.id())
			require.NoError(t, err)
			require.Equal(t, 1, len(cb.Commands))
			assert.True(t, cb.Commands[0].Bootstrapping)
			service := &Service{store: store}
			service.handleStartReplica(cb.Commands[0])

			for i := 0; i < 100; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				m := store.getHeartbeatMessage()
				assert.NoError(t, store.addLogStoreHeartbeat(ctx, m))

				state, err = store.getCheckerState()
				require.NoError(t, err)
				store.checkBootstrap(state)

				state, err = store.getCheckerState()
				require.NoError(t, err)
				if state.State != pb.HAKeeperRunning {
					time.Sleep(50 * time.Millisecond)
				} else {
					return
				}
				if i == 2999 {
					t.Fatalf("failed to complete bootstrap")
				}
			}
		}
	}
	runHAKeeperStoreTest(t, false, fn)
}
