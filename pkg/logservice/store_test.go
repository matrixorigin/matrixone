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
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/taskservice"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	testIOTimeout = 5 * time.Second
)

func TestNodeHostConfig(t *testing.T) {
	cfg := Config{
		DeploymentID: 1234,
		DataDir:      "lalala",
	}
	cfg.Fill()
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
	cfg := Config{
		UUID:                uuid.New().String(),
		RTTMillisecond:      10,
		GossipAddress:       testGossipAddress,
		GossipSeedAddresses: []string{testGossipAddress, dummyGossipSeedAddress},
		DeploymentID:        1,
		FS:                  vfs.NewStrictMem(),
		UseTeeLogDB:         true,
	}
	cfg.Fill()
	return cfg
}

func TestStoreCanBeCreatedAndClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	store, err := newLogStore(cfg, nil)
	assert.NoError(t, err)
	logger.Info("1")
	defer func() {
		assert.NoError(t, store.close())
	}()
	logger.Info("2")
}

func getTestStore(cfg Config, startLogReplica bool, taskService taskservice.TaskService) (*store, error) {
	store, err := newLogStore(cfg, taskService)
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
	store, err := newLogStore(cfg, nil)
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
		assert.NoError(t, store.getOrExtendDNLease(ctx, 1, 100))
	}
	runStoreTest(t, fn)
}

func TestAppendLog(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		assert.NoError(t, store.getOrExtendDNLease(ctx, 1, 100))
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
		assert.NoError(t, store.getOrExtendDNLease(ctx, 1, 100))
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
		assert.NoError(t, store.getOrExtendDNLease(ctx, 1, 100))
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
		assert.NoError(t, store.getOrExtendDNLease(ctx, 1, 100))
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
		assert.NoError(t, store.getOrExtendDNLease(ctx, 1, 100))
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
		assert.NoError(t, store.addCNStoreHeartbeat(ctx, cnMsg))

		dnMsg := pb.DNStoreHeartbeat{
			UUID:   store.id(),
			Shards: make([]pb.DNShardInfo, 0),
		}
		dnMsg.Shards = append(dnMsg.Shards, pb.DNShardInfo{ShardID: 2, ReplicaID: 3})
		_, err = store.addDNStoreHeartbeat(ctx, dnMsg)
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

func getTestStores() (*store, *store, error) {
	cfg1 := Config{
		FS:                  vfs.NewStrictMem(),
		DeploymentID:        1,
		RTTMillisecond:      5,
		DataDir:             "data-1",
		ServiceAddress:      "127.0.0.1:9001",
		RaftAddress:         "127.0.0.1:9002",
		GossipAddress:       "127.0.0.1:9011",
		GossipSeedAddresses: []string{"127.0.0.1:9011", "127.0.0.1:9012"},
	}
	cfg1.Fill()
	store1, err := newLogStore(cfg1, nil)
	if err != nil {
		return nil, nil, err
	}
	cfg2 := Config{
		FS:                  vfs.NewStrictMem(),
		DeploymentID:        1,
		RTTMillisecond:      5,
		DataDir:             "data-1",
		ServiceAddress:      "127.0.0.1:9006",
		RaftAddress:         "127.0.0.1:9007",
		GossipAddress:       "127.0.0.1:9012",
		GossipSeedAddresses: []string{"127.0.0.1:9011", "127.0.0.1:9012"},
	}
	cfg2.Fill()
	store2, err := newLogStore(cfg2, nil)
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
				logger.Error("failed to transfer leader")
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
