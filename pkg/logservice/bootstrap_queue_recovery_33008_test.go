// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"bytes"
	"context"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestDynamicBootstrapRecoversLostRepliesWithTwoRaftVoters(t *testing.T) {
	const shard = 77
	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Second)
	defer cancel()
	var configs [3]Config
	var services [3]*Service
	seeds := make([]string, 0, len(configs))
	for i := range configs {
		configs[i] = getServiceTestConfig()
		configs[i].DataDir = t.TempDir()
		configs[i].SnapshotExportDir = filepath.Join(configs[i].DataDir, "export")
		configs[i].FS = vfs.Default
		configs[i].LogDBDisablePrealloc = true
		configs[i].RPC.MaxMessageSize = DefaultConfig().RPC.MaxMessageSize
		seeds = append(seeds, getTestGossipAddress(configs[i].GossipPort))
	}
	for i := range configs {
		configs[i].GossipSeedAddresses = seeds
		s, err := NewService(configs[i], newFS(), nil)
		require.NoError(t, err)
		services[i] = s
		t.Cleanup(func() {
			if services[i] != nil {
				require.NoError(t, services[i].Close())
			}
		})
	}
	waitForRecoveryPeers(t, ctx, services[:])
	rsm := hakeeper.NewStateMachine(0, 1)
	var index uint64
	apply := func(command []byte) sm.Result {
		t.Helper()
		index++
		result, err := rsm.Update(sm.Entry{Index: index, Cmd: command})
		require.NoError(t, err)
		return result
	}
	logHeartbeat := func(hb pb.LogStoreHeartbeat) pb.CommandBatch {
		t.Helper()
		hb.TaskServiceCreated = true
		data, err := hb.Marshal()
		require.NoError(t, err)
		result := apply(hakeeper.GetGuardedLogStoreHeartbeatCmd(data))
		var batch pb.CommandBatch
		require.NoError(t, batch.Unmarshal(result.Data))
		return batch
	}
	queue := func(uuid string) pb.CommandBatch {
		t.Helper()
		value, err := rsm.Lookup(&hakeeper.ScheduleCommandQuery{UUID: uuid})
		require.NoError(t, err)
		return *value.(*pb.CommandBatch)
	}

	cfg := hakeeper.Config{TickPerSecond: 1, LogStoreTimeout: time.Second}
	apply(hakeeper.GetInitialClusterRequestCmd(1, 1, 3, 0, nil, nil))
	apply(hakeeper.GetSetStateCmd(pb.HAKeeperBootstrapCommandsReceived))
	apply(hakeeper.GetSetStateCmd(pb.HAKeeperRunning))
	apply(hakeeper.GetTickCmdWithLogStorePolicy(1, cfg.LogStoreTimeoutTicks()))
	apply(hakeeper.GetAddLogShardCmd(pb.AddLogShard{ShardID: shard}))
	for _, service := range services {
		logHeartbeat(service.store.getHeartbeatMessage())
	}
	value, err := rsm.Lookup(&hakeeper.StateQuery{Scheduling: true})
	require.NoError(t, err)
	coordinator := checkers.NewCoordinator("", cfg)
	initial := coordinator.Check(util.NewTestIDAllocator(100), *value.(*pb.CheckerState), false)
	require.Len(t, initial, len(services))
	replicaIDs := make(map[string]uint64, len(initial))
	var members map[uint64]string
	for _, command := range initial {
		require.NotNil(t, command.BootstrapShard)
		require.EqualValues(t, shard, command.BootstrapShard.ShardID)
		if members == nil {
			members = command.BootstrapShard.InitialMembers
		}
		require.Equal(t, members, command.BootstrapShard.InitialMembers)
		replicaIDs[command.UUID] = command.BootstrapShard.ReplicaID
	}
	require.Len(t, members, len(services))
	apply(hakeeper.GetUpdateCommandsCmd(1, initial))

	c := services[2]
	require.NoError(t, c.Close())
	services[2] = nil
	apply(hakeeper.GetTickCmd())
	apply(hakeeper.GetTickCmd())
	require.True(t, cfg.LogStoreExpired(1, 3))
	// Both guarded heartbeat proposals commit after C has expired, but the
	// two responses are dropped before either store handles its command.
	var lost [2]pb.CommandBatch
	for i, service := range services[:2] {
		lost[i] = logHeartbeat(service.store.getHeartbeatMessage())
		require.Len(t, lost[i].Commands, 1)
		require.False(t, service.store.nh.HasNodeInfo(shard, replicaIDs[service.ID()]))
	}
	var snapshot bytes.Buffer
	require.NoError(t, rsm.SaveSnapshot(&snapshot, nil, nil))
	rsm = hakeeper.NewStateMachine(0, 2)
	require.NoError(t, rsm.RecoverFromSnapshot(&snapshot, nil, nil))

	// The unrelated legacy TN batch must not replace the admitted cohort.
	tnTask := pb.ScheduleCommand{UUID: "tn", ServiceType: pb.TNService, CreateTaskService: &pb.CreateTaskService{TaskDatabase: "task"}}
	apply(hakeeper.GetUpdateCommandsCmd(1, []pb.ScheduleCommand{tnTask}))
	require.Equal(t, []pb.ScheduleCommand{tnTask}, queue("tn").Commands)
	for i, service := range services[:2] {
		batch := logHeartbeat(service.store.getHeartbeatMessage())
		require.Equal(t, lost[i], batch, "retry exactly the original cohort and local ID")
		service.handleCommands(batch.Commands)
		require.True(t, service.store.nh.HasNodeInfo(shard, replicaIDs[service.ID()]))
	}
	for i, writer := range services[:2] {
		reader := services[1-i]
		entry := getTestUserEntry()
		entry[len(entry)-1] = byte(i + 1)
		var lsn uint64
		waitForRecovery(t, ctx, "write and peer read through the surviving cohort", 9*time.Second, time.Second, func(attempt context.Context) bool {
			if lsn == 0 {
				var err error
				lsn, err = writer.store.append(attempt, shard, entry)
				if err != nil {
					t.Logf("append at %s: %v", writer.ID(), err)
					return false
				}
			}
			records, _, err := reader.store.queryLog(attempt, shard, lsn, math.MaxUint64)
			if err != nil {
				t.Logf("peer read at %s: %v", reader.ID(), err)
				return false
			}
			require.NotEmpty(t, records)
			require.Equal(t, lsn, records[0].Lsn)
			require.Equal(t, pb.UserRecord, records[0].Type)
			require.Equal(t, entry, records[0].Data)
			return true
		})
	}
	for _, service := range services[:2] {
		hb := recoveryHeartbeat(t, ctx, service, shard, replicaIDs[service.ID()], members)
		require.Empty(t, logHeartbeat(hb).Commands)
		require.Empty(t, queue(service.ID()).Commands)
	}
	require.Len(t, queue(c.ID()).Commands, 1, "neither surviving store can acknowledge C")
}

func TestLateBootstrapCannotRecreateGenesisInNewerRaftConfiguration(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var configs [2]Config
	for i := range configs {
		configs[i] = getServiceTestConfig()
		configs[i].DataDir = t.TempDir()
		configs[i].RPC.MaxMessageSize = DefaultConfig().RPC.MaxMessageSize
	}
	seeds := []string{getTestGossipAddress(configs[0].GossipPort), getTestGossipAddress(configs[1].GossipPort)}
	var services [2]*Service
	for i := range configs {
		configs[i].GossipSeedAddresses = seeds
		s, err := NewService(configs[i], newFS(), nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		services[i] = s
	}
	a, later := services[0], services[1]
	waitForRecoveryPeers(t, ctx, services[:])
	const shard = 77
	require.NoError(t, a.store.startReplica(shard, 1, map[uint64]string{1: a.ID()}, false))
	var membership *dragonboat.Membership
	waitForRecovery(t, ctx, "initial Raft membership", 6*time.Second, 300*time.Millisecond, func(attempt context.Context) bool {
		var err error
		membership, err = a.store.nh.SyncGetShardMembership(attempt, shard)
		return err == nil
	})
	// Pass through a non-voting change before configuring the later voter.
	// Its current roster matches the delayed command, but its Raft index
	// proves that this is not the original two-voter genesis.
	require.NoError(t, a.store.addNonVotingReplica(shard, 23, uuid.NewString(), membership.ConfigChangeID))
	membership, err := a.store.nh.SyncGetShardMembership(ctx, shard)
	require.NoError(t, err)
	require.NoError(t, a.store.removeReplica(shard, 23, membership.ConfigChangeID))
	membership, err = a.store.nh.SyncGetShardMembership(ctx, shard)
	require.NoError(t, err)
	require.NoError(t, a.store.addReplica(shard, 2, later.ID(), membership.ConfigChangeID))
	var epoch uint64
	waitForRecovery(t, ctx, "later store learns the new configuration through Raft gossip", 8*time.Second, 100*time.Millisecond, func(context.Context) bool {
		registry, ok := later.store.nh.GetNodeHostRegistry()
		if !ok {
			return false
		}
		info, ok := registry.GetShardInfo(shard)
		if !ok || len(info.Nodes) != 2 || info.Nodes[1] != a.ID() || info.Nodes[2] != later.ID() ||
			len(info.NonVotingNodes) != 0 || info.ConfigChangeIndex <= 2 {
			return false
		}
		epoch = info.ConfigChangeIndex
		return true
	})
	require.False(t, later.store.nh.HasNodeInfo(shard, 2))
	lateGenesis := pb.ScheduleCommand{UUID: later.ID(), ServiceType: pb.LogService,
		BootstrapShard: &pb.BootstrapShard{ShardID: shard, ReplicaID: 2,
			InitialMembers: map[uint64]string{1: a.ID(), 2: later.ID()}}}
	later.handleCommands([]pb.ScheduleCommand{lateGenesis})
	require.False(t, later.store.nh.HasNodeInfo(shard, 2), "a stale bootstrap response must not create a genesis")
	current := pb.ScheduleCommand{UUID: later.ID(), ServiceType: pb.LogService,
		ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica,
			Replica: pb.Replica{UUID: later.ID(), ShardID: shard, ReplicaID: 2, Epoch: epoch}}}
	later.handleCommands([]pb.ScheduleCommand{current})
	require.True(t, later.store.nh.HasNodeInfo(shard, 2), "current authorization still allows joining")
}
