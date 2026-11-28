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
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestUnregisteredShardPreservesReplicasUntilExplicitRegistration(t *testing.T) {
	machine := hakeeper.NewStateMachine(0, 1)
	var index uint64
	commit := func(cmd []byte) sm.Result {
		t.Helper()
		index++
		result, err := machine.Update(sm.Entry{Index: index, Cmd: cmd})
		require.NoError(t, err)
		return result
	}
	state := func() *pb.CheckerState {
		t.Helper()
		result, err := machine.Lookup(&hakeeper.StateQuery{Scheduling: true})
		require.NoError(t, err)
		return result.(*pb.CheckerState)
	}
	cfg := hakeeper.Config{}
	cfg.Fill()
	commit(hakeeper.GetInitialClusterRequestCmd(1, 1, 3, 0, nil, nil))
	commit(hakeeper.GetSetStateCmd(pb.HAKeeperBootstrapCommandsReceived))
	commit(hakeeper.GetSetStateCmd(pb.HAKeeperRunning))
	commit(hakeeper.GetTickCmd())
	shard := pb.LogShardInfo{ShardID: 77, Epoch: 7, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
	heartbeat := func(uuid string, reports ...pb.LogReplicaInfo) pb.CommandBatch {
		t.Helper()
		hb := pb.LogStoreHeartbeat{UUID: uuid, TaskServiceCreated: true, Replicas: reports}
		data, err := hb.Marshal()
		require.NoError(t, err)
		result := commit(hakeeper.GetLogStoreHeartbeatCmd(data))
		var batch pb.CommandBatch
		require.NoError(t, batch.Unmarshal(result.Data))
		return batch
	}
	a := pb.LogReplicaInfo{LogShardInfo: shard, ReplicaID: 1}
	heartbeat("a", a)
	heartbeat("b")
	heartbeat("c", pb.LogReplicaInfo{LogShardInfo: shard, ReplicaID: 3})
	before := state()
	for _, record := range before.ClusterInfo.LogShards {
		require.NotEqual(t, uint64(77), record.ShardID)
	}
	commands := NewCoordinator("", cfg).Check(util.NewTestIDAllocator(100), *before, false)
	var start bool
	for _, cmd := range commands {
		if cmd.ConfigChange == nil || cmd.ConfigChange.Replica.ShardID != 77 {
			continue
		}
		require.NotEqual(t, pb.RemoveReplica, cmd.ConfigChange.ChangeType)
		require.NotEqual(t, pb.RemoveNonVotingReplica, cmd.ConfigChange.ChangeType)
		start = start || cmd.ConfigChange.ChangeType == pb.StartReplica && cmd.ConfigChange.Replica.UUID == "b"
	}
	require.True(t, start, "an existing configured identity can still be restarted")
}
