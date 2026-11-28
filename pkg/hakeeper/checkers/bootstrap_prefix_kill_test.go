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

package checkers

import (
	"bytes"
	"maps"
	"slices"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestInitialPrefixCannotRetireFutureVoter(t *testing.T) {
	t.Run("initial_cluster", func(t *testing.T) { testInitialPrefixCannotRetireFutureVoter(t, false) })
	t.Run("dynamic", func(t *testing.T) { testInitialPrefixCannotRetireFutureVoter(t, true) })
}

func testInitialPrefixCannotRetireFutureVoter(t *testing.T, dynamic bool) {
	t.Helper()
	shardID := uint64(1)
	if dynamic {
		shardID = 77
	}
	cfg := hakeeper.Config{}
	cfg.Fill()
	s := hakeeper.NewStateMachine(0, 1)
	var index uint64
	apply := func(cmd []byte) sm.Result {
		t.Helper()
		index++
		res, err := s.Update(sm.Entry{Index: index, Cmd: cmd})
		require.NoError(t, err)
		return res
	}
	heartbeat := func(uuid string, replicas ...pb.LogReplicaInfo) pb.CommandBatch {
		t.Helper()
		hb := pb.LogStoreHeartbeat{UUID: uuid, TaskServiceCreated: true, StoreIncarnation: "disk-" + uuid, Replicas: replicas}
		data, err := hb.Marshal()
		require.NoError(t, err)
		result := apply(hakeeper.GetGuardedLogStoreHeartbeatCmd(data))
		var batch pb.CommandBatch
		require.NoError(t, batch.Unmarshal(result.Data))
		return batch
	}
	state := func() *pb.CheckerState {
		t.Helper()
		value, err := s.Lookup(&hakeeper.StateQuery{Scheduling: true})
		require.NoError(t, err)
		return value.(*pb.CheckerState)
	}
	check := func() []pb.ScheduleCommand {
		return NewCoordinator("", cfg).Check(util.NewTestIDAllocator(100), *state(), false)
	}
	apply(hakeeper.GetInitialClusterRequestCmd(1, 1, 3, 0, nil, nil))
	apply(hakeeper.GetTickCmdWithLogStorePolicy(1, cfg.LogStoreTimeoutTicks()))
	ha := pb.LogShardInfo{ShardID: 0, Epoch: 3, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
	for id, uuid := range ha.Replicas {
		heartbeat(uuid, pb.LogReplicaInfo{LogShardInfo: ha, ReplicaID: id})
	}
	var approved []pb.ScheduleCommand
	var members map[uint64]string
	if dynamic {
		apply(hakeeper.GetSetStateCmd(pb.HAKeeperBootstrapCommandsReceived))
		apply(hakeeper.GetSetStateCmd(pb.HAKeeperRunning))
		apply(hakeeper.GetAddLogShardCmd(pb.AddLogShard{ShardID: shardID}))
		approved = check()
		require.Len(t, approved, 3)
		require.NotNil(t, approved[0].BootstrapShard)
		members = approved[0].BootstrapShard.InitialMembers
	} else {
		current := state()
		var err error
		approved, err = bootstrap.NewBootstrapManager(current.ClusterInfo).
			Bootstrap("", util.NewTestIDAllocator(100), current.TNState, current.LogState)
		require.NoError(t, err)
		require.Len(t, approved, 3)
		require.True(t, approved[0].Bootstrapping)
		members = approved[0].ConfigChange.InitialMembers
	}
	ids := slices.Sorted(maps.Keys(members))
	a, b, c := members[ids[0]], members[ids[1]], members[ids[2]]
	apply(hakeeper.GetUpdateCommandsCmd(1, approved))

	first := pb.LogShardInfo{ShardID: shardID, Epoch: 1, Replicas: map[uint64]string{ids[0]: a}}
	majority := pb.LogShardInfo{ShardID: shardID, Epoch: 2, Replicas: map[uint64]string{ids[0]: a, ids[1]: b}}
	localBeforeOwnAdd := pb.LogReplicaInfo{LogShardInfo: first, ReplicaID: ids[2]}
	heartbeat(a, pb.LogReplicaInfo{LogShardInfo: majority, ReplicaID: ids[0]})
	heartbeat(b, pb.LogReplicaInfo{LogShardInfo: majority, ReplicaID: ids[1]})
	heartbeat(c, localBeforeOwnAdd)
	if !dynamic {
		current := state()
		require.True(t, bootstrap.NewBootstrapManager(current.ClusterInfo).CheckBootstrap(current.LogState),
			"initial cluster can enter Running on the first majority")
		apply(hakeeper.GetSetStateCmd(pb.HAKeeperRunning))
	}
	var kill pb.ScheduleCommand
	for _, cmd := range check() {
		if cmd.UUID == c && cmd.ConfigChange != nil && cmd.ConfigChange.ChangeType == pb.KillZombie {
			kill = cmd
		}
	}
	require.NotNil(t, kill.ConfigChange, "the real checker can classify a report that precedes its own initial AddNode as a zombie")
	require.Equal(t, ids[2], kill.ConfigChange.Replica.ReplicaID)
	apply(hakeeper.GetUpdateCommandsCmd(2, []pb.ScheduleCommand{kill}))

	var saved bytes.Buffer
	require.NoError(t, s.SaveSnapshot(&saved, nil, nil))
	prefixIndex := index
	for _, tc := range []struct {
		name        string
		epoch       uint64
		voters      map[uint64]string
		conflicting bool
		caughtUp    bool
		kill        bool
	}{
		{name: "not_yet_applied", epoch: 2, voters: majority.Replicas},
		{name: "caught_up_without_own_add", epoch: 2, voters: majority.Replicas, caughtUp: true},
		{name: "already_configured", epoch: 3, voters: members},
		{name: "still_configured_at_later_index", epoch: 7, voters: members},
		{name: "removed_by_later_configuration", epoch: 7, voters: majority.Replicas, kill: true},
		{name: "different_genesis_not_protected", epoch: 2, voters: majority.Replicas, conflicting: true, kill: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s = hakeeper.NewStateMachine(0, 2)
			index = prefixIndex
			require.NoError(t, s.RecoverFromSnapshot(bytes.NewReader(saved.Bytes()), nil, nil))
			for _, cmd := range heartbeat(a, pb.LogReplicaInfo{LogShardInfo: pb.LogShardInfo{
				ShardID: shardID, Epoch: tc.epoch, Replicas: tc.voters}, ReplicaID: ids[0]}).Commands {
				require.NotEqual(t, c, cmd.UUID)
			}
			report := localBeforeOwnAdd
			if tc.caughtUp {
				report.LogShardInfo = majority
			}
			if tc.conflicting {
				report.Replicas = map[uint64]string{ids[0]: "different-native-configuration"}
			}
			reply := heartbeat(c, report)
			if tc.kill {
				require.Equal(t, []pb.ScheduleCommand{kill}, reply.Commands, "a genuine removal must still be delivered")
				require.Empty(t, heartbeat(c, report).Commands)
			} else if dynamic && tc.epoch <= uint64(len(members)) {
				require.Len(t, reply.Commands, 1)
				require.NotNil(t, reply.Commands[0].BootstrapShard, "an initial partial report must not cause terminal retirement")
				require.Equal(t, members, reply.Commands[0].BootstrapShard.InitialMembers)
				require.Equal(t, ids[2], reply.Commands[0].BootstrapShard.ReplicaID)
			} else {
				require.Empty(t, reply.Commands, "a partial report is not terminal retirement, even after initial command consumption")
			}
		})
	}
}
