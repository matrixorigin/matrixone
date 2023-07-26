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
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
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
	store, err := getTestStore(cfg, startLogReplica, nil)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, store.close())
	}()
	peers := make(map[uint64]dragonboat.Target)
	peers[1] = store.id()
	assert.NoError(t, store.startHAKeeperReplica(1, peers, false))
	fn(t, store)
}

func runHakeeperTaskServiceTest(t *testing.T, fn func(*testing.T, *store, taskservice.TaskService)) {
	defer leaktest.AfterTest(t)()
	cfg := getStoreTestConfig()
	cfg.HAKeeperConfig.CNStoreTimeout.Duration = 5 * time.Second
	defer vfs.ReportLeakedFD(cfg.FS, t)

	taskService := taskservice.NewTaskService(runtime.DefaultRuntime(), taskservice.NewMemTaskStorage())
	defer taskService.StopScheduleCronTask()

	store, err := getTestStore(cfg, false, taskService)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, store.close())
	}()
	peers := make(map[uint64]dragonboat.Target)
	peers[1] = store.id()
	assert.NoError(t, store.startHAKeeperReplica(1, peers, false))
	fn(t, store, taskService)
}

func runHAKeeperClusterTest(t *testing.T, fn func(*testing.T, []*Service)) {
	defer leaktest.AfterTest(t)()
	cfg1 := DefaultConfig()
	cfg1.UUID = uuid.New().String()
	cfg1.FS = vfs.NewStrictMem()
	cfg1.DeploymentID = 1
	cfg1.RTTMillisecond = 5
	cfg1.DataDir = "data-1"
	cfg1.ServiceAddress = "127.0.0.1:9002"
	cfg1.RaftAddress = "127.0.0.1:9000"
	cfg1.GossipAddress = "127.0.0.1:9001"
	cfg1.GossipSeedAddresses = []string{"127.0.0.1:9011", "127.0.0.1:9021", "127.0.0.1:9031"}
	cfg1.DisableWorkers = true
	cfg1.HAKeeperConfig.TickPerSecond = 10
	cfg1.HAKeeperConfig.LogStoreTimeout.Duration = 5 * time.Second
	cfg1.HAKeeperConfig.DNStoreTimeout.Duration = 10 * time.Second
	cfg1.HAKeeperConfig.CNStoreTimeout.Duration = 5 * time.Second
	cfg1.Fill()
	cfg2 := DefaultConfig()
	cfg2.UUID = uuid.New().String()
	cfg2.FS = vfs.NewStrictMem()
	cfg2.DeploymentID = 1
	cfg2.RTTMillisecond = 5
	cfg2.DataDir = "data-2"
	cfg2.ServiceAddress = "127.0.0.1:9012"
	cfg2.RaftAddress = "127.0.0.1:9010"
	cfg2.GossipAddress = "127.0.0.1:9011"
	cfg2.GossipSeedAddresses = []string{"127.0.0.1:9001", "127.0.0.1:9021", "127.0.0.1:9031"}
	cfg2.DisableWorkers = true
	cfg2.HAKeeperConfig.TickPerSecond = 10
	cfg2.HAKeeperConfig.LogStoreTimeout.Duration = 5 * time.Second
	cfg2.HAKeeperConfig.DNStoreTimeout.Duration = 10 * time.Second
	cfg2.HAKeeperConfig.CNStoreTimeout.Duration = 5 * time.Second
	cfg2.Fill()
	cfg3 := DefaultConfig()
	cfg3.UUID = uuid.New().String()
	cfg3.FS = vfs.NewStrictMem()
	cfg3.DeploymentID = 1
	cfg3.RTTMillisecond = 5
	cfg3.DataDir = "data-3"
	cfg3.ServiceAddress = "127.0.0.1:9022"
	cfg3.RaftAddress = "127.0.0.1:9020"
	cfg3.GossipAddress = "127.0.0.1:9021"
	cfg3.GossipSeedAddresses = []string{"127.0.0.1:9001", "127.0.0.1:9011", "127.0.0.1:9031"}
	cfg3.DisableWorkers = true
	cfg3.HAKeeperConfig.TickPerSecond = 10
	cfg3.HAKeeperConfig.LogStoreTimeout.Duration = 5 * time.Second
	cfg3.HAKeeperConfig.DNStoreTimeout.Duration = 10 * time.Second
	cfg3.HAKeeperConfig.CNStoreTimeout.Duration = 5 * time.Second
	cfg3.Fill()
	cfg4 := DefaultConfig()
	cfg4.UUID = uuid.New().String()
	cfg4.FS = vfs.NewStrictMem()
	cfg4.DeploymentID = 1
	cfg4.RTTMillisecond = 5
	cfg4.DataDir = "data-4"
	cfg4.ServiceAddress = "127.0.0.1:9032"
	cfg4.RaftAddress = "127.0.0.1:9030"
	cfg4.GossipAddress = "127.0.0.1:9031"
	cfg4.GossipSeedAddresses = []string{"127.0.0.1:9001", "127.0.0.1:9011", "127.0.0.1:9021"}
	cfg4.DisableWorkers = true
	cfg4.HAKeeperConfig.TickPerSecond = 10
	cfg4.HAKeeperConfig.LogStoreTimeout.Duration = 5 * time.Second
	cfg4.HAKeeperConfig.DNStoreTimeout.Duration = 10 * time.Second
	cfg4.HAKeeperConfig.CNStoreTimeout.Duration = 5 * time.Second
	cfg4.Fill()
	service1, err := NewService(cfg1,
		newFS(),
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service1.Close())
	}()
	service2, err := NewService(cfg2,
		newFS(),
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service2.Close())
	}()
	service3, err := NewService(cfg3,
		newFS(),
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service3.Close())
	}()
	service4, err := NewService(cfg4,
		newFS(),
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
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
				done := false
				for i := 0; i < 10; i++ {
					m := s.store.getHeartbeatMessage()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					_, err := s.store.addLogStoreHeartbeat(ctx, m)
					if err == dragonboat.ErrTimeout {
						time.Sleep(100 * time.Millisecond)
					} else {
						if err == nil {
							done = true
							break
						} else {
							t.Fatalf("failed to add heartbeat %v", err)
						}
					}
				}
				if !done {
					t.Fatalf("failed to add heartbeat after 10 retries")
				}
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
		_, err = services[0].store.addDNStoreHeartbeat(ctx, dnMsg)
		require.NoError(t, err)

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
		assert.Equal(t, uint64(checkBootstrapCycles), leaderStore.bootstrapCheckCycles)
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
		assert.Equal(t, pb.DNService, cmd.ServiceType)
		dnShardInfo := pb.DNShardInfo{
			ShardID:   cmd.ConfigChange.Replica.ShardID,
			ReplicaID: cmd.ConfigChange.Replica.ReplicaID,
		}
		dnMsg.Shards = append(dnMsg.Shards, dnShardInfo)
		// as if DN is running
		_, err = services[0].store.addDNStoreHeartbeat(ctx, dnMsg)
		require.NoError(t, err)
		// fake a free DN store
		dnMsg2 := pb.DNStoreHeartbeat{
			UUID:   uuid.New().String(),
			Shards: make([]pb.DNShardInfo, 0),
		}
		_, err = services[0].store.addDNStoreHeartbeat(ctx, dnMsg2)
		require.NoError(t, err)

		// stop store 1
		require.NoError(t, services[0].Close())
		// no service.Close can be repeatedly called
		services[0].store = nil
		services = services[1:]

		// wait for HAKeeper to repair the Log & HAKeeper shards
		dnRepaired := false
		for i := 0; i < 5000; i++ {
			testLogger.Debug(fmt.Sprintf("iteration %d", i))
			tn := func() (bool, error) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				m := services[0].store.getHeartbeatMessage()
				if cb, err := services[0].store.addLogStoreHeartbeat(ctx, m); err != nil {
					return false, err
				} else {
					services[0].handleCommands(cb.Commands)
				}
				m = services[1].store.getHeartbeatMessage()
				if cb, err := services[1].store.addLogStoreHeartbeat(ctx, m); err != nil {
					return false, err
				} else {
					services[1].handleCommands(cb.Commands)
				}
				m = services[2].store.getHeartbeatMessage()
				if cb, err := services[0].store.addLogStoreHeartbeat(ctx, m); err != nil {
					return false, err
				} else {
					services[2].handleCommands(cb.Commands)
				}
				if _, err := services[0].store.addDNStoreHeartbeat(ctx, dnMsg2); err != nil {
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
						if cmd.ServiceType == pb.DNService {
							if cmd.ConfigChange != nil && cmd.ConfigChange.Replica.ShardID == dnShardInfo.ShardID &&
								cmd.ConfigChange.Replica.ReplicaID > dnShardInfo.ReplicaID {
								dnRepaired = true
							}
						}
					}
				}

				logRepaired := true
				for _, s := range services {
					if !hasShard(s.store, 0) || !hasShard(s.store, 1) {
						logRepaired = false
						break
					}
				}
				testLogger.Debug(fmt.Sprintf("dnRepaired %t, logRepaired %t", dnRepaired, logRepaired))
				if !logRepaired || !dnRepaired {
					return false, nil
				} else {
					testLogger.Debug(fmt.Sprintf("repair completed, i: %d", i))
					return true, nil
				}
			}
			completed, err := tn()
			if err != nil && err != dragonboat.ErrTimeout &&
				err != dragonboat.ErrInvalidDeadline && err != dragonboat.ErrTimeoutTooSmall {
				t.Fatalf("unexpected error %v", err)
			}
			if completed {
				for _, s := range services[:3] {
					_ = s.task.holder.Close()
				}
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("failed to repair shards")
	}
	runHAKeeperClusterTest(t, fn)
}

func TestGetCheckerStateFromLeader(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*10))
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				t.Error("test deadline reached")
				return

			default:
				isLeader, termA, err := store.isLeaderHAKeeper()
				state, termB := store.getCheckerStateFromLeader()
				require.NoError(t, err)
				assert.Equal(t, termB, termA)

				if !isLeader {
					assert.Equal(t, (*pb.CheckerState)(nil), state)
				} else {
					assert.NotEqual(t, (*pb.CheckerState)(nil), state)
					return
				}
				time.Sleep(time.Second)
			}
		}
	}

	runHAKeeperStoreTest(t, false, fn)
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
		_, err = store.addLogStoreHeartbeat(ctx, m)
		assert.NoError(t, err)

		dnMsg := pb.DNStoreHeartbeat{
			UUID:   uuid.New().String(),
			Shards: make([]pb.DNShardInfo, 0),
		}
		_, err = store.addDNStoreHeartbeat(ctx, dnMsg)
		assert.NoError(t, err)

		_, term, err := store.isLeaderHAKeeper()
		require.NoError(t, err)

		state, err = store.getCheckerState()
		require.NoError(t, err)
		store.bootstrap(term, state)

		state, err = store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperBootstrapCommandsReceived, state.State)
		assert.Equal(t, uint64(checkBootstrapCycles), store.bootstrapCheckCycles)
		require.NotNil(t, store.bootstrapMgr)
		assert.False(t, store.bootstrapMgr.CheckBootstrap(state.LogState))

		if fail {
			// keep checking, bootstrap will eventually be set as failed
			for i := 0; i <= checkBootstrapCycles; i++ {
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
			assert.Equal(t, pb.DNService, cb.Commands[0].ServiceType)
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
				_, err = store.addLogStoreHeartbeat(ctx, m)
				assert.NoError(t, err)

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

func TestTaskSchedulerCanScheduleTasksToCNs(t *testing.T) {
	fn := func(t *testing.T, store *store, taskService taskservice.TaskService) {
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
		_, err = store.addLogStoreHeartbeat(ctx, m)
		assert.NoError(t, err)

		_, term, err := store.isLeaderHAKeeper()
		require.NoError(t, err)

		state, err = store.getCheckerState()
		require.NoError(t, err)
		store.bootstrap(term, state)

		state, err = store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperBootstrapCommandsReceived, state.State)
		assert.Equal(t, uint64(checkBootstrapCycles), store.bootstrapCheckCycles)
		require.NotNil(t, store.bootstrapMgr)
		assert.False(t, store.bootstrapMgr.CheckBootstrap(state.LogState))

		cb, err := store.getCommandBatch(ctx, store.id())
		require.NoError(t, err)
		require.Equal(t, 1, len(cb.Commands))
		assert.True(t, cb.Commands[0].Bootstrapping)
		service := &Service{store: store}
		service.handleStartReplica(cb.Commands[0])

		for i := 0; i < 100; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			m := store.getHeartbeatMessage()
			_, err = store.addLogStoreHeartbeat(ctx, m)
			assert.NoError(t, err)

			state, err = store.getCheckerState()
			require.NoError(t, err)
			store.checkBootstrap(state)

			state, err = store.getCheckerState()
			require.NoError(t, err)
			if state.State != pb.HAKeeperRunning {
				time.Sleep(50 * time.Millisecond)
			} else {
				break
			}
			if i == 2999 {
				t.Fatalf("failed to complete bootstrap")
			}
		}

		cnUUID1 := uuid.New().String()
		cnMsg1 := pb.CNStoreHeartbeat{UUID: cnUUID1}
		_, err = store.addCNStoreHeartbeat(ctx, cnMsg1)
		assert.NoError(t, err)
		err = taskService.Create(ctx, task.TaskMetadata{ID: "a"})
		assert.NoError(t, err)
		state, err = store.getCheckerState()
		require.NoError(t, err)
		tasks, err := taskService.QueryTask(ctx, taskservice.WithTaskRunnerCond(taskservice.EQ, cnUUID1))
		assert.NoError(t, err)
		assert.Equal(t, 0, len(tasks))
		store.taskSchedule(state)
		// update state
		state, err = store.getCheckerState()
		require.NoError(t, err)
		store.taskSchedule(state)
		tasks, err = taskService.QueryTask(ctx, taskservice.WithTaskRunnerCond(taskservice.EQ, cnUUID1))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(tasks))

		cnUUID2 := uuid.New().String()
		cnMsg2 := pb.CNStoreHeartbeat{UUID: cnUUID2}
		_, err = store.addCNStoreHeartbeat(ctx, cnMsg2)
		assert.NoError(t, err)
		err = taskService.Create(ctx, task.TaskMetadata{ID: "b"})
		assert.NoError(t, err)
		state, err = store.getCheckerState()
		require.NoError(t, err)
		tasks, err = taskService.QueryTask(ctx, taskservice.WithTaskRunnerCond(taskservice.EQ, cnUUID2))
		assert.NoError(t, err)
		assert.Equal(t, 0, len(tasks))
		store.taskSchedule(state)
		tasks, err = taskService.QueryTask(ctx, taskservice.WithTaskRunnerCond(taskservice.EQ, cnUUID2))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(tasks))
	}
	runHakeeperTaskServiceTest(t, fn)
}

func TestTaskSchedulerCanReScheduleExpiredTasks(t *testing.T) {
	fn := func(t *testing.T, store *store, taskService taskservice.TaskService) {
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
		_, err = store.addLogStoreHeartbeat(ctx, m)
		assert.NoError(t, err)

		_, term, err := store.isLeaderHAKeeper()
		require.NoError(t, err)

		state, err = store.getCheckerState()
		require.NoError(t, err)
		store.bootstrap(term, state)

		state, err = store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperBootstrapCommandsReceived, state.State)
		assert.Equal(t, uint64(checkBootstrapCycles), store.bootstrapCheckCycles)
		require.NotNil(t, store.bootstrapMgr)
		assert.False(t, store.bootstrapMgr.CheckBootstrap(state.LogState))

		cb, err := store.getCommandBatch(ctx, store.id())
		require.NoError(t, err)
		require.Equal(t, 1, len(cb.Commands))
		assert.True(t, cb.Commands[0].Bootstrapping)
		service := &Service{store: store}
		service.handleStartReplica(cb.Commands[0])

		for i := 0; i < 100; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			m := store.getHeartbeatMessage()
			_, err = store.addLogStoreHeartbeat(ctx, m)
			assert.NoError(t, err)

			state, err = store.getCheckerState()
			require.NoError(t, err)
			store.checkBootstrap(state)

			state, err = store.getCheckerState()
			require.NoError(t, err)
			if state.State != pb.HAKeeperRunning {
				time.Sleep(50 * time.Millisecond)
			} else {
				break
			}
			if i == 2999 {
				t.Fatalf("failed to complete bootstrap")
			}
		}

		cnUUID1 := uuid.New().String()
		cnMsg1 := pb.CNStoreHeartbeat{UUID: cnUUID1}
		_, err = store.addCNStoreHeartbeat(ctx, cnMsg1)
		assert.NoError(t, err)
		err = taskService.Create(ctx, task.TaskMetadata{ID: "a"})
		assert.NoError(t, err)
		state, err = store.getCheckerState()
		require.NoError(t, err)
		tasks, err := taskService.QueryTask(ctx, taskservice.WithTaskRunnerCond(taskservice.EQ, cnUUID1))
		assert.NoError(t, err)
		assert.Equal(t, 0, len(tasks))
		store.taskSchedule(state)
		// update state
		state, err = store.getCheckerState()
		require.NoError(t, err)
		store.taskSchedule(state)
		tasks, err = taskService.QueryTask(ctx, taskservice.WithTaskRunnerCond(taskservice.EQ, cnUUID1))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(tasks))

		cnUUID2 := uuid.New().String()
		for i := 0; i < 1000; i++ {
			testLogger.Debug(fmt.Sprintf("iteration %d", i))
			tn := func() bool {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				cnMsg2 := pb.CNStoreHeartbeat{UUID: cnUUID2}
				_, err = store.addCNStoreHeartbeat(ctx, cnMsg2)
				assert.NoError(t, err)
				state, err = store.getCheckerState()
				require.NoError(t, err)
				store.taskSchedule(state)
				tasks, err = taskService.QueryTask(ctx, taskservice.WithTaskRunnerCond(taskservice.EQ, cnUUID2))
				assert.NoError(t, err)
				if len(tasks) == 0 {
					testLogger.Info("no task found")
					time.Sleep(50 * time.Millisecond)
				} else {
					tasks, err = taskService.QueryTask(ctx, taskservice.WithTaskRunnerCond(taskservice.EQ, cnUUID1))
					assert.Equal(t, 0, len(tasks))
					return true
				}
				return false
			}
			completed := tn()
			if completed {
				store.taskScheduler.StopScheduleCronTask()
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("failed to reschedule expired tasks")
	}
	runHakeeperTaskServiceTest(t, fn)
}

func TestGetTaskTableUserFromEnv(t *testing.T) {
	t.Setenv(moAdminUser, "root")
	user, ok := getTaskTableUserFromEnv()
	require.False(t, ok)
	require.Equal(t, pb.TaskTableUser{}, user)

	t.Setenv(moAdminPassword, "")
	user, ok = getTaskTableUserFromEnv()
	require.False(t, ok)
	require.Equal(t, pb.TaskTableUser{}, user)

	t.Setenv(moAdminPassword, "root")
	user, ok = getTaskTableUserFromEnv()
	require.True(t, ok)
	require.Equal(t, pb.TaskTableUser{Username: "root", Password: "root"}, user)
}
