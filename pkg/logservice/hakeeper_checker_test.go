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
	"os"
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
		FS:                    vfs.NewStrictMem(),
		DeploymentID:          1,
		RTTMillisecond:        5,
		DataDir:               "data-1",
		ServiceAddress:        "127.0.0.1:9002",
		RaftAddress:           "127.0.0.1:9000",
		GossipAddress:         "127.0.0.1:9001",
		GossipSeedAddresses:   []string{"127.0.0.1:9011", "127.0.0.1:9021", "127.0.0.1:9031"},
		DisableHAKeeperTicker: true,
	}
	cfg2 := Config{
		FS:                    vfs.NewStrictMem(),
		DeploymentID:          1,
		RTTMillisecond:        5,
		DataDir:               "data-2",
		ServiceAddress:        "127.0.0.1:9012",
		RaftAddress:           "127.0.0.1:9010",
		GossipAddress:         "127.0.0.1:9011",
		GossipSeedAddresses:   []string{"127.0.0.1:9001", "127.0.0.1:9021", "127.0.0.1:9031"},
		DisableHAKeeperTicker: true,
	}
	cfg3 := Config{
		FS:                    vfs.NewStrictMem(),
		DeploymentID:          1,
		RTTMillisecond:        5,
		DataDir:               "data-3",
		ServiceAddress:        "127.0.0.1:9022",
		RaftAddress:           "127.0.0.1:9020",
		GossipAddress:         "127.0.0.1:9021",
		GossipSeedAddresses:   []string{"127.0.0.1:9001", "127.0.0.1:9011", "127.0.0.1:9031"},
		DisableHAKeeperTicker: true,
	}
	cfg4 := Config{
		FS:                    vfs.NewStrictMem(),
		DeploymentID:          1,
		RTTMillisecond:        5,
		DataDir:               "data-4",
		ServiceAddress:        "127.0.0.1:9032",
		RaftAddress:           "127.0.0.1:9030",
		GossipAddress:         "127.0.0.1:9031",
		GossipSeedAddresses:   []string{"127.0.0.1:9001", "127.0.0.1:9011", "127.0.0.1:9021"},
		DisableHAKeeperTicker: true,
	}
	service1, err := NewService(cfg1)
	require.NoError(t, err)
	defer func() {
		//assert.NoError(t, service1.Close())
	}()
	service2, err := NewService(cfg2)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service2.Close())
	}()
	service3, err := NewService(cfg3)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service3.Close())
	}()
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

func TestHAKeeperClusterCanBootstrapAndRepairLogShard(t *testing.T) {
	if os.Getenv("LONG_TEST") == "" {
		// this test will fail on go1.18 when -race is enabled, as it always
		// timeout. go1.19 has a much faster -race implementation and it works fine
		t.Skip("Skipping long test")
	}

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
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
		defer cancel()
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

		// stop store 1
		require.NoError(t, services[0].Close())
		// no service.Close can be repeatedly called
		services[0].store = nil
		services = services[1:]

		// wait for HAKeeper to repair the cluster
		for i := 0; i < 5000; i++ {
			m := services[0].store.getHeartbeatMessage()
			assert.NoError(t, services[0].store.addLogStoreHeartbeat(ctx, m))
			m = services[1].store.getHeartbeatMessage()
			assert.NoError(t, services[1].store.addLogStoreHeartbeat(ctx, m))
			m = services[2].store.getHeartbeatMessage()
			assert.NoError(t, services[0].store.addLogStoreHeartbeat(ctx, m))

			for _, s := range services {
				if hasShard(s.store, 0) {
					s.store.hakeeperTick()
					s.store.hakeeperCheck()
				}

				cb, err := services[0].store.getCommandBatch(ctx, s.store.id())
				for _, cmd := range cb.Commands {
					plog.Infof("store returned schedule command: %s", cmd.LogString())
				}
				require.NoError(t, err)
				s.handleCommands(cb.Commands)
			}

			notReady := false
			for _, s := range services {
				if !hasShard(s.store, 0) || !hasShard(s.store, 1) {
					notReady = true
					break
				}
			}
			if notReady {
				time.Sleep(time.Millisecond)
			} else {
				plog.Infof("repair completed, i: %d", i)
				return
			}
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
		dnMsg.Shards = append(dnMsg.Shards, pb.DNShardInfo{ShardID: 2, ReplicaID: 3})
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
			cb, err := store.getCommandBatch(ctx, store.id())
			require.NoError(t, err)
			require.Equal(t, 1, len(cb.Commands))
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
