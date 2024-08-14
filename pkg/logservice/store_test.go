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
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:  "debug",
		Format: "console",
	})

	runtime.SetupServiceBasedRuntime("", runtime.NewRuntime(metadata.ServiceType_LOG, "test", logutil.GetGlobalLogger()))
	m.Run()
}

var (
	testIOTimeout = 5 * time.Second
)

func TestNodeHostConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DeploymentID = 1234
	cfg.DataDir = "lalala"
	nhConfig := getNodeHostConfig(cfg)
	assert.Equal(t, cfg.DeploymentID, nhConfig.DeploymentID)
	assert.Equal(t, cfg.DataDir, nhConfig.NodeHostDir)
	assert.True(t, nhConfig.AddressByNodeHostID)
}

func TestRaftConfig(t *testing.T) {
	cfg := getRaftConfig(1, 1)
	assert.True(t, cfg.CheckQuorum)
	assert.True(t, cfg.OrderedConfigChange)
}

func getStoreTestConfig() Config {
	cfg := DefaultConfig()
	cfg.UUID = uuid.New().String()
	cfg.RTTMillisecond = 10
	cfg.GossipPort = testGossipPort
	cfg.GossipSeedAddresses = []string{testGossipAddress, dummyGossipSeedAddress}
	cfg.DeploymentID = 1
	cfg.FS = vfs.NewStrictMem()
	cfg.UseTeeLogDB = true

	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			runtime.SetupServiceBasedRuntime(cfg.UUID, rt)
		},
	)
	return cfg
}

func TestStoreCanBeCreatedAndClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	store, err := newLogStore(cfg, nil, nil, runtime.DefaultRuntime())
	assert.NoError(t, err)
	runtime.DefaultRuntime().Logger().Info("1")
	defer func() {
		assert.NoError(t, store.close())
	}()
	runtime.DefaultRuntime().Logger().Info("2")
}

func getTestStore(cfg Config, startLogReplica bool, taskService taskservice.TaskService) (*store, error) {
	store, err := newLogStore(
		cfg,
		func() taskservice.TaskService { return taskService },
		nil,
		runtime.DefaultRuntime(),
	)
	if err != nil {
		return nil, err
	}
	if startLogReplica {
		peers := make(map[uint64]dragonboat.Target)
		peers[2] = store.nh.ID()
		if err := store.startReplica(1, 2, peers, false); err != nil {
			store.close()
			return nil, err
		}
	}
	return store, nil
}

func TestHAKeeperCanBeStarted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	store, err := newLogStore(cfg, nil, nil, runtime.DefaultRuntime())
	assert.NoError(t, err)
	peers := make(map[uint64]dragonboat.Target)
	peers[2] = store.nh.ID()
	assert.NoError(t, store.startHAKeeperReplica(2, peers, false))
	defer func() {
		assert.NoError(t, store.close())
	}()
	mustHaveReplica(t, store, hakeeper.DefaultHAKeeperShardID, 2)
}

func TestStateMachineCanBeStarted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	store, err := getTestStore(cfg, true, nil)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, store.close())
	}()
	mustHaveReplica(t, store, 1, 2)
}

func TestReplicaCanBeStopped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	store, err := getTestStore(cfg, true, nil)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, store.close())
	}()
	mustHaveReplica(t, store, 1, 2)
	require.NoError(t, store.stopReplica(1, 2))
	assert.False(t, hasReplica(store, 1, 2))
}

func runStoreTest(t *testing.T, fn func(*testing.T, *store)) {
	defer leaktest.AfterTest(t)()
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	store, err := getTestStore(cfg, true, nil)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, store.close())
	}()
	fn(t, store)
}

func getTestUserEntry() []byte {
	cmd := make([]byte, headerSize+8+8)
	binaryEnc.PutUint32(cmd, uint32(pb.UserEntryUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], 100)
	binaryEnc.PutUint64(cmd[headerSize+8:], 1234567890)
	return cmd
}

func TestGetOrExtendLease(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		assert.NoError(t, store.getOrExtendTNLease(ctx, 1, 100))
	}
	runStoreTest(t, fn)
}

func TestAppendLog(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		assert.NoError(t, store.getOrExtendTNLease(ctx, 1, 100))
		cmd := getTestUserEntry()
		lsn, err := store.append(ctx, 1, cmd)
		assert.NoError(t, err)
		assert.Equal(t, uint64(4), lsn)
	}
	runStoreTest(t, fn)
}

func TestAppendLogIsRejectedForMismatchedLeaseHolderID(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		assert.NoError(t, store.getOrExtendTNLease(ctx, 1, 100))
		cmd := make([]byte, headerSize+8+8)
		binaryEnc.PutUint32(cmd, uint32(pb.UserEntryUpdate))
		binaryEnc.PutUint64(cmd[headerSize:], 101)
		binaryEnc.PutUint64(cmd[headerSize+8:], 1234567890)
		_, err := store.append(ctx, 1, cmd)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotLeaseHolder))
	}
	runStoreTest(t, fn)
}

func TestStoreTsoUpdate(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		v1, err := store.tsoUpdate(ctx, 100)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), v1)
		v2, err := store.tsoUpdate(ctx, 1000)
		require.NoError(t, err)
		assert.Equal(t, uint64(101), v2)
	}
	runStoreTest(t, fn)
}

func TestTruncateLog(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		assert.NoError(t, store.getOrExtendTNLease(ctx, 1, 100))
		cmd := getTestUserEntry()
		_, err := store.append(ctx, 1, cmd)
		assert.NoError(t, err)
		assert.NoError(t, store.truncateLog(ctx, 1, 4))
		err = store.truncateLog(ctx, 1, 3)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTruncateLsn))
	}
	runStoreTest(t, fn)
}

func TestGetTruncatedIndex(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		index, err := store.getTruncatedLsn(ctx, 1)
		assert.Equal(t, uint64(0), index)
		assert.NoError(t, err)
		assert.NoError(t, store.getOrExtendTNLease(ctx, 1, 100))
		cmd := getTestUserEntry()
		_, err = store.append(ctx, 1, cmd)
		assert.NoError(t, err)
		assert.NoError(t, store.truncateLog(ctx, 1, 4))
		index, err = store.getTruncatedLsn(ctx, 1)
		assert.Equal(t, uint64(4), index)
		assert.NoError(t, err)
	}
	runStoreTest(t, fn)
}

func TestQueryLog(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		assert.NoError(t, store.getOrExtendTNLease(ctx, 1, 100))
		cmd := getTestUserEntry()
		_, err := store.append(ctx, 1, cmd)
		assert.NoError(t, err)
		entries, lsn, err := store.queryLog(ctx, 1, 4, math.MaxUint64)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(entries))
		assert.Equal(t, uint64(4), lsn)
		assert.Equal(t, entries[0].Data, cmd)
		// leaseholder ID update cmd at entry index 3
		entries, lsn, err = store.queryLog(ctx, 1, 3, math.MaxUint64)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(entries))
		assert.Equal(t, uint64(3), lsn)
		assert.Equal(t, cmd, entries[1].Data)
		assert.Equal(t, pb.LeaseUpdate, entries[0].Type)
		assert.Equal(t, pb.UserRecord, entries[1].Type)

		// size limited
		_, err = store.append(ctx, 1, cmd)
		assert.NoError(t, err)
		entries, lsn, err = store.queryLog(ctx, 1, 4, 1)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(entries))
		assert.Equal(t, uint64(5), lsn)
		assert.Equal(t, entries[0].Data, cmd)
		// more log available
		entries, lsn, err = store.queryLog(ctx, 1, 5, 1)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(entries))
		assert.Equal(t, uint64(5), lsn)
		assert.Equal(t, entries[0].Data, cmd)
	}
	runStoreTest(t, fn)
}

func proceedHAKeeperToRunning(t *testing.T, store *store) {
	state, err := store.getCheckerState()
	assert.NoError(t, err)
	assert.Equal(t, pb.HAKeeperCreated, state.State)

	nextIDByKey := map[string]uint64{"a": 1, "b": 2}
	err = store.setInitialClusterInfo(
		1,
		1,
		1,
		hakeeper.K8SIDRangeEnd+10,
		nextIDByKey,
		nil,
	)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	hb := store.getHeartbeatMessage()
	_, err = store.addLogStoreHeartbeat(ctx, hb)
	assert.NoError(t, err)

	state, err = store.getCheckerState()
	assert.NoError(t, err)
	assert.Equal(t, pb.HAKeeperBootstrapping, state.State)
	assert.Equal(t, hakeeper.K8SIDRangeEnd+10, state.NextId)
	assert.Equal(t, nextIDByKey, state.NextIDByKey)

	_, term, err := store.isLeaderHAKeeper()
	assert.NoError(t, err)

	store.bootstrap(term, state)
	state, err = store.getCheckerState()

	assert.NoError(t, err)
	assert.Equal(t, pb.HAKeeperBootstrapCommandsReceived, state.State)

	cmd, err := store.getCommandBatch(ctx, store.id())
	require.NoError(t, err)
	require.Equal(t, 1, len(cmd.Commands))
	assert.True(t, cmd.Commands[0].Bootstrapping)

	// handle startReplica to make sure logHeartbeat msg contain shards info,
	// which used in store.checkBootstrap to determine if all log shards ready
	service := &Service{store: store}
	service.handleStartReplica(cmd.Commands[0])

	for state.State != pb.HAKeeperRunning && store.bootstrapCheckCycles > 0 {
		func() {
			ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			_, err = store.addLogStoreHeartbeat(ctx, store.getHeartbeatMessage())
			assert.NoError(t, err)

			store.checkBootstrap(state)
			state, err = store.getCheckerState()
			assert.NoError(t, err)

			time.Sleep(time.Millisecond * 100)
		}()
	}

	assert.Equal(t, pb.HAKeeperRunning, state.State)
}

// test if the tickerForTaskSchedule can push forward these routine
func TestTickerForTaskSchedule(t *testing.T) {
	fn := func(t *testing.T, store *store, taskService taskservice.TaskService) {

		tickerCxt, tickerCancel := context.WithCancel(context.Background())
		defer tickerCancel()

		//do task schedule background
		go store.tickerForTaskSchedule(tickerCxt, time.Millisecond*10)

		// making hakeeper state proceeds to running before test task schedule
		proceedHAKeeperToRunning(t, store)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := taskService.CreateAsyncTask(ctx, task.TaskMetadata{ID: "1234"})
		assert.NoError(t, err)

		cnUUID := uuid.New().String()
		cmd := pb.CNStoreHeartbeat{UUID: cnUUID}
		_, err = store.addCNStoreHeartbeat(ctx, cmd)
		assert.NoError(t, err)

		ticker := time.NewTicker(time.Millisecond * 100)
		defer ticker.Stop()
		timeout := time.NewTimer(time.Second * 10)
		defer timeout.Stop()

		for {
			select {
			case <-ticker.C:
				tasks, err := taskService.QueryAsyncTask(ctx, taskservice.WithTaskRunnerCond(taskservice.EQ, cnUUID))
				assert.NoError(t, err)
				if len(tasks) == 1 {
					return
				}

			case <-timeout.C:
				panic("task schedule timeout")
			}
		}

	}

	runHakeeperTaskServiceTest(t, fn)
}

func TestHAKeeperTick(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.id()
		assert.NoError(t, store.startHAKeeperReplica(1, peers, false))
		store.hakeeperTick()
	}
	runStoreTest(t, fn)
}

func TestAddScheduleCommands(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.id()
		assert.NoError(t, store.startHAKeeperReplica(1, peers, false))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sc1 := pb.ScheduleCommand{
			UUID: "uuid1",
			ConfigChange: &pb.ConfigChange{
				Replica: pb.Replica{
					ShardID: 1,
				},
			},
		}
		sc2 := pb.ScheduleCommand{
			UUID: "uuid2",
			ConfigChange: &pb.ConfigChange{
				Replica: pb.Replica{
					ShardID: 2,
				},
			},
		}
		sc3 := pb.ScheduleCommand{
			UUID: "uuid1",
			ConfigChange: &pb.ConfigChange{
				Replica: pb.Replica{
					ShardID: 3,
				},
			},
		}
		require.NoError(t,
			store.addScheduleCommands(ctx, 1, []pb.ScheduleCommand{sc1, sc2, sc3}))
		cb, err := store.getCommandBatch(ctx, "uuid1")
		require.NoError(t, err)
		assert.Equal(t, []pb.ScheduleCommand{sc1, sc3}, cb.Commands)
		cb, err = store.getCommandBatch(ctx, "uuid2")
		require.NoError(t, err)
		assert.Equal(t, []pb.ScheduleCommand{sc2}, cb.Commands)
	}
	runStoreTest(t, fn)
}

func TestGetHeartbeatMessage(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.id()
		assert.NoError(t, store.startReplica(10, 1, peers, false))
		assert.NoError(t, store.startHAKeeperReplica(1, peers, false))

		for i := 0; i < 5000; i++ {
			m := store.getHeartbeatMessage()
			if len(m.Replicas) != 3 {
				time.Sleep(time.Millisecond)
			} else {
				return
			}
		}
		t.Fatalf("failed to get all replicas details from heartbeat message")
	}
	runStoreTest(t, fn)
}

func TestAddHeartbeat(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.id()
		assert.NoError(t, store.startHAKeeperReplica(1, peers, false))

		m := store.getHeartbeatMessage()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := store.addLogStoreHeartbeat(ctx, m)
		assert.NoError(t, err)

		cnMsg := pb.CNStoreHeartbeat{
			UUID: store.id(),
		}
		_, err = store.addCNStoreHeartbeat(ctx, cnMsg)
		assert.NoError(t, err)

		tnMsg := pb.TNStoreHeartbeat{
			UUID:   store.id(),
			Shards: make([]pb.TNShardInfo, 0),
		}
		tnMsg.Shards = append(tnMsg.Shards, pb.TNShardInfo{ShardID: 2, ReplicaID: 3})
		_, err = store.addTNStoreHeartbeat(ctx, tnMsg)
		assert.NoError(t, err)

		proxyMsg := pb.ProxyHeartbeat{
			UUID: store.id(),
		}
		_, err = store.addProxyHeartbeat(ctx, proxyMsg)
		assert.NoError(t, err)
	}
	runStoreTest(t, fn)
}

func TestAddReplicaRejectedForInvalidCCI(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		err := store.addReplica(1, 100, uuid.New().String(), 0)
		assert.Equal(t, dragonboat.ErrRejected, err)
	}
	runStoreTest(t, fn)
}

func TestAddNonVotingReplicaRejectedForInvalidCCI(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		err := store.addNonVotingReplica(1, 100, uuid.New().String(), 0)
		assert.Equal(t, dragonboat.ErrRejected, err)
	}
	runStoreTest(t, fn)
}

func TestAddReplica(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		for {
			_, _, ok, err := store.nh.GetLeaderID(1)
			require.NoError(t, err)
			if ok {
				break
			}
			time.Sleep(time.Millisecond)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m, err := store.nh.SyncGetShardMembership(ctx, 1)
		require.NoError(t, err)
		err = store.addReplica(1, 100, uuid.New().String(), m.ConfigChangeID)
		assert.NoError(t, err)
		hb := store.getHeartbeatMessage()
		assert.Equal(t, 2, len(hb.Replicas[0].Replicas))
	}
	runStoreTest(t, fn)
}

func TestAddNonVotingReplica(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		for {
			_, _, ok, err := store.nh.GetLeaderID(1)
			require.NoError(t, err)
			if ok {
				break
			}
			time.Sleep(time.Millisecond)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m, err := store.nh.SyncGetShardMembership(ctx, 1)
		require.NoError(t, err)
		err = store.addNonVotingReplica(1, 100, uuid.New().String(), m.ConfigChangeID)
		assert.NoError(t, err)
		hb := store.getHeartbeatMessage()
		assert.Equal(t, 1, len(hb.Replicas[0].Replicas))
		assert.Equal(t, 1, len(hb.Replicas[0].NonVotingReplicas))
	}
	runStoreTest(t, fn)
}

func getTestStores() (*store, *store, error) {
	cfg1 := DefaultConfig()
	cfg1.UUID = uuid.NewString()
	cfg1.FS = vfs.NewStrictMem()
	cfg1.DeploymentID = 1
	cfg1.RTTMillisecond = 5
	cfg1.DataDir = "data-1"
	cfg1.LogServicePort = 9001
	cfg1.RaftPort = 9002
	cfg1.GossipPort = 9011
	cfg1.GossipSeedAddresses = []string{"127.0.0.1:9011", "127.0.0.1:9012"}
	store1, err := newLogStore(cfg1, nil, nil, runtime.DefaultRuntime())
	if err != nil {
		return nil, nil, err
	}
	cfg2 := DefaultConfig()
	cfg2.UUID = uuid.NewString()
	cfg2.FS = vfs.NewStrictMem()
	cfg2.DeploymentID = 1
	cfg2.RTTMillisecond = 5
	cfg2.DataDir = "data-1"
	cfg2.LogServicePort = 9006
	cfg2.RaftPort = 9007
	cfg2.GossipPort = 9012
	cfg2.GossipSeedAddresses = []string{"127.0.0.1:9011", "127.0.0.1:9012"}
	store2, err := newLogStore(cfg2, nil, nil, runtime.DefaultRuntime())
	if err != nil {
		return nil, nil, err
	}

	peers1 := make(map[uint64]dragonboat.Target)
	peers1[1] = store1.nh.ID()
	peers1[2] = store2.nh.ID()
	if err := store1.startReplica(1, 1, peers1, false); err != nil {
		return nil, nil, err
	}
	peers2 := make(map[uint64]dragonboat.Target)
	peers2[1] = store1.nh.ID()
	peers2[2] = store2.nh.ID()
	if err := store2.startReplica(1, 2, peers2, false); err != nil {
		return nil, nil, err
	}

	for i := 0; i <= 30000; i++ {
		leaderID, _, ok, err := store1.nh.GetLeaderID(1)
		if err != nil {
			return nil, nil, err
		}
		if ok && leaderID == 1 {
			break
		}
		if ok && leaderID != 1 {
			if err := store1.requestLeaderTransfer(1, 1); err != nil {
				runtime.DefaultRuntime().Logger().Error("failed to transfer leader")
			}
		}
		time.Sleep(time.Millisecond)
		if i == 30000 {
			panic("failed to have leader elected in 30 seconds")
		}
	}
	return store1, store2, nil
}

func TestRemoveReplica(t *testing.T) {
	store1, store2, err := getTestStores()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store1.close())
		require.NoError(t, store2.close())
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		m, err := store1.nh.SyncGetShardMembership(ctx, 1)
		if err == dragonboat.ErrShardNotReady {
			time.Sleep(time.Millisecond)
			continue
		}
		require.NoError(t, err)
		require.NoError(t, store1.removeReplica(1, 2, m.ConfigChangeID))
		return
	}
}

func TestStopReplicaCanResetHAKeeperReplicaID(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.id()
		assert.NoError(t, store.startHAKeeperReplica(1, peers, false))
		assert.Equal(t, uint64(1), atomic.LoadUint64(&store.haKeeperReplicaID))
		assert.NoError(t, store.stopReplica(hakeeper.DefaultHAKeeperShardID, 1))
		assert.Equal(t, uint64(0), atomic.LoadUint64(&store.haKeeperReplicaID))
	}
	runStoreTest(t, fn)
}

func hasShard(s *store, shardID uint64) bool {
	hb := s.getHeartbeatMessage()
	for _, info := range hb.Replicas {
		if info.ShardID == shardID {
			return true
		}
	}
	return false
}

func hasReplica(s *store, shardID uint64, replicaID uint64) bool {
	hb := s.getHeartbeatMessage()
	for _, info := range hb.Replicas {
		if info.ShardID == shardID {
			for r := range info.Replicas {
				if r == replicaID {
					return true
				}
			}
		}
	}
	return false
}

func hasNonVotingReplica(s *store, shardID uint64, replicaID uint64) bool {
	hb := s.getHeartbeatMessage()
	for _, info := range hb.Replicas {
		if info.ShardID == shardID {
			for r := range info.NonVotingReplicas {
				if r == replicaID {
					return true
				}
			}
		}
	}
	return false
}

func mustHaveReplica(t *testing.T,
	s *store, shardID uint64, replicaID uint64) {
	for i := 0; i < 100; i++ {
		if hasReplica(s, shardID, replicaID) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("failed to locate the replica")
}

func mustHaveNonVotingReplica(t *testing.T,
	s *store, shardID uint64, replicaID uint64) {
	for i := 0; i < 100; i++ {
		if hasNonVotingReplica(s, shardID, replicaID) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("failed to locate the replica")
}

func TestUpdateCNLabel(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.id()
		assert.NoError(t, store.startHAKeeperReplica(1, peers, false))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		uuid := "uuid1"
		label := pb.CNStoreLabel{
			UUID: uuid,
			Labels: map[string]metadata.LabelList{
				"account": {Labels: []string{"a", "b"}},
				"role":    {Labels: []string{"1", "2"}},
			},
		}
		err := store.updateCNLabel(ctx, label)
		assert.EqualError(t, err, fmt.Sprintf("internal error: CN [%s] does not exist", uuid))

		// begin heartbeat to add CN store.
		hb := pb.CNStoreHeartbeat{
			UUID: uuid,
		}
		_, err = store.addCNStoreHeartbeat(ctx, hb)
		assert.NoError(t, err)

		label = pb.CNStoreLabel{
			UUID: uuid,
			Labels: map[string]metadata.LabelList{
				"account": {Labels: []string{"a", "b"}},
				"role":    {Labels: []string{"1", "2"}},
			},
		}
		err = store.updateCNLabel(ctx, label)
		assert.NoError(t, err)

		state, err := store.getCheckerState()
		assert.NoError(t, err)
		assert.NotEmpty(t, state)
		info, ok1 := state.CNState.Stores[uuid]
		assert.True(t, ok1)
		labels1, ok2 := info.Labels["account"]
		assert.True(t, ok2)
		assert.Equal(t, labels1.Labels, []string{"a", "b"})
		labels2, ok3 := info.Labels["role"]
		assert.True(t, ok3)
		assert.Equal(t, labels2.Labels, []string{"1", "2"})

		label = pb.CNStoreLabel{
			UUID: uuid,
			Labels: map[string]metadata.LabelList{
				"account": {Labels: []string{"a", "b"}},
			},
		}
		err = store.updateCNLabel(ctx, label)
		assert.NoError(t, err)

		state, err = store.getCheckerState()
		assert.NoError(t, err)
		assert.NotEmpty(t, state)
		info, ok1 = state.CNState.Stores[uuid]
		assert.True(t, ok1)
		labels1, ok2 = info.Labels["account"]
		assert.True(t, ok2)
		assert.Equal(t, labels1.Labels, []string{"a", "b"})
		_, ok3 = info.Labels["role"]
		assert.False(t, ok3)
	}
	runStoreTest(t, fn)
}

func TestUpdateCNWorkState(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.id()
		assert.NoError(t, store.startHAKeeperReplica(1, peers, false))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		uuid := "uuid1"
		workState := pb.CNWorkState{
			UUID:  uuid,
			State: metadata.WorkState_Working,
		}
		err := store.updateCNWorkState(ctx, workState)
		assert.EqualError(t, err, fmt.Sprintf("internal error: CN [%s] does not exist", uuid))

		// begin heartbeat to add CN store.
		hb := pb.CNStoreHeartbeat{
			UUID: uuid,
		}
		_, err = store.addCNStoreHeartbeat(ctx, hb)
		assert.NoError(t, err)

		err = store.updateCNWorkState(ctx, workState)
		assert.NoError(t, err)

		state, err := store.getCheckerState()
		assert.NoError(t, err)
		assert.NotEmpty(t, state)
		info, ok1 := state.CNState.Stores[uuid]
		assert.True(t, ok1)
		assert.Equal(t, metadata.WorkState_Working, info.WorkState)

		workState = pb.CNWorkState{
			UUID:  uuid,
			State: metadata.WorkState_Draining,
		}
		err = store.updateCNWorkState(ctx, workState)
		assert.NoError(t, err)

		state, err = store.getCheckerState()
		assert.NoError(t, err)
		assert.NotEmpty(t, state)
		info, ok1 = state.CNState.Stores[uuid]
		assert.True(t, ok1)
		assert.Equal(t, metadata.WorkState_Draining, info.WorkState)

		workState = pb.CNWorkState{
			UUID:  uuid,
			State: metadata.WorkState_Working,
		}
		err = store.updateCNWorkState(ctx, workState)
		assert.NoError(t, err)

		state, err = store.getCheckerState()
		assert.NoError(t, err)
		assert.NotEmpty(t, state)
		info, ok1 = state.CNState.Stores[uuid]
		assert.True(t, ok1)
		assert.Equal(t, metadata.WorkState_Working, info.WorkState)
	}
	runStoreTest(t, fn)
}

func TestPatchCNStore(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.id()
		assert.NoError(t, store.startHAKeeperReplica(1, peers, false))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		uuid := "uuid1"
		stateLabel := pb.CNStateLabel{
			UUID:  uuid,
			State: metadata.WorkState_Working,
			Labels: map[string]metadata.LabelList{
				"account": {Labels: []string{"a", "b"}},
				"role":    {Labels: []string{"1", "2"}},
			},
		}
		err := store.patchCNStore(ctx, stateLabel)
		assert.EqualError(t, err, fmt.Sprintf("internal error: CN [%s] does not exist", uuid))

		// begin heartbeat to add CN store.
		hb := pb.CNStoreHeartbeat{
			UUID: uuid,
		}
		_, err = store.addCNStoreHeartbeat(ctx, hb)
		assert.NoError(t, err)

		err = store.patchCNStore(ctx, stateLabel)
		assert.NoError(t, err)

		state, err := store.getCheckerState()
		assert.NoError(t, err)
		assert.NotEmpty(t, state)
		info, ok1 := state.CNState.Stores[uuid]
		assert.True(t, ok1)
		assert.Equal(t, metadata.WorkState_Working, info.WorkState)
		labels1, ok2 := info.Labels["account"]
		assert.True(t, ok2)
		assert.Equal(t, labels1.Labels, []string{"a", "b"})
		labels2, ok3 := info.Labels["role"]
		assert.True(t, ok3)
		assert.Equal(t, labels2.Labels, []string{"1", "2"})

		stateLabel = pb.CNStateLabel{
			UUID:  uuid,
			State: metadata.WorkState_Draining,
		}
		err = store.patchCNStore(ctx, stateLabel)
		assert.NoError(t, err)

		state, err = store.getCheckerState()
		assert.NoError(t, err)
		assert.NotEmpty(t, state)
		info, ok1 = state.CNState.Stores[uuid]
		assert.True(t, ok1)
		assert.Equal(t, metadata.WorkState_Draining, info.WorkState)
		labels1, ok2 = info.Labels["account"]
		assert.True(t, ok2)
		assert.Equal(t, labels1.Labels, []string{"a", "b"})
		labels2, ok3 = info.Labels["role"]
		assert.True(t, ok3)
		assert.Equal(t, labels2.Labels, []string{"1", "2"})

		stateLabel = pb.CNStateLabel{
			UUID: uuid,
			Labels: map[string]metadata.LabelList{
				"account": {Labels: []string{"a", "b"}},
			},
		}
		err = store.patchCNStore(ctx, stateLabel)
		assert.NoError(t, err)

		state, err = store.getCheckerState()
		assert.NoError(t, err)
		assert.NotEmpty(t, state)
		info, ok1 = state.CNState.Stores[uuid]
		assert.True(t, ok1)
		assert.Equal(t, metadata.WorkState_Working, info.WorkState)
		labels1, ok2 = info.Labels["account"]
		assert.True(t, ok2)
		assert.Equal(t, labels1.Labels, []string{"a", "b"})
		_, ok3 = info.Labels["role"]
		assert.False(t, ok3)
	}
	runStoreTest(t, fn)
}

func TestDeleteCNStore(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.id()
		assert.NoError(t, store.startHAKeeperReplica(1, peers, false))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		uuid := "uuid1"
		hb := pb.CNStoreHeartbeat{
			UUID: uuid,
		}
		_, err := store.addCNStoreHeartbeat(ctx, hb)
		assert.NoError(t, err)
		state, err := store.getCheckerState()
		assert.NoError(t, err)
		assert.NotEmpty(t, state)
		assert.Equal(t, 1, len(state.CNState.Stores))
		_, ok := state.CNState.Stores[uuid]
		assert.Equal(t, true, ok)

		cnStore := pb.DeleteCNStore{
			StoreID: uuid,
		}
		err = store.deleteCNStore(ctx, cnStore)
		assert.NoError(t, err)

		state, err = store.getCheckerState()
		assert.NoError(t, err)
		assert.NotEmpty(t, state)
		assert.NoError(t, err)
		assert.NotEmpty(t, state)
		assert.Equal(t, 0, len(state.CNState.Stores))
	}
	runStoreTest(t, fn)
}

func TestManagedHAKeeperClient_UpdateNonVotingReplicaNum(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.id()
		assert.NoError(t, store.startHAKeeperReplica(1, peers, false))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		var num uint64 = 5
		err := store.updateNonVotingReplicaNum(ctx, num)
		assert.NoError(t, err)

		state, err := store.getCheckerState()
		assert.NoError(t, err)
		assert.Equal(t, num, state.NonVotingReplicaNum)
	}
	runStoreTest(t, fn)
}

func TestManagedHAKeeperClient_UpdateNonVotingLocality(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.id()
		assert.NoError(t, store.startHAKeeperReplica(1, peers, false))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		locality := pb.Locality{
			Value: map[string]string{
				"k1": "v1",
				"k2": "v2",
			},
		}
		err := store.updateNonVotingLocality(ctx, locality)
		assert.NoError(t, err)

		state, err := store.getCheckerState()
		assert.NoError(t, err)
		assert.Equal(t, locality, state.NonVotingLocality)
	}
	runStoreTest(t, fn)
}

func TestGetLatestLsn(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		index, err := store.getLatestLsn(ctx, 1)
		assert.Equal(t, uint64(0), index)
		assert.NoError(t, err)
		assert.NoError(t, store.getOrExtendTNLease(ctx, 1, 100))
		cmd := getTestUserEntry()
		_, err = store.append(ctx, 1, cmd)
		assert.NoError(t, err)
		index, err = store.getLatestLsn(ctx, 1)
		assert.Equal(t, uint64(4), index)
		assert.NoError(t, err)
	}
	runStoreTest(t, fn)
}

func TestRequiredLsn(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		assert.NoError(t, store.getOrExtendTNLease(ctx, 1, 100))
		cmd := getTestUserEntry()
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout*5)
			defer cancel()
			for i := 0; i < 10; i++ {
				_, err := store.append(ctx, 1, cmd)
				assert.NoError(t, err)
			}
		}()
		assert.NoError(t, store.setRequiredLsn(ctx, 1, 8))
		lsn, err := store.getRequiredLsn(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(8), lsn)

		assert.NoError(t, store.setRequiredLsn(ctx, 1, 800))
		lsn, err = store.getRequiredLsn(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(15), lsn)
	}
	runStoreTest(t, fn)
}

func TestGetLeaderID(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		assert.NoError(t, store.getOrExtendTNLease(ctx, 1, 100))
		leaderID, err := store.leaderID(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), leaderID)
	}
	runStoreTest(t, fn)
}

func TestAddLogShard(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.id()
		assert.NoError(t, store.startHAKeeperReplica(1, peers, false))

		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()

		assert.NoError(t, store.addLogShard(ctx, pb.AddLogShard{
			ShardID: 1,
		}))
		state, err := store.getCheckerState()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(state.LogState.Shards))
	}
	runStoreTest(t, fn)
}
