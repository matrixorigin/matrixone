// Copyright 2021 - 2026 Matrix Origin
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

package checkers

import (
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestTNStartRetriesPreserveAcknowledgedDelivery(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "preparing"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			c, state, alloc := tnStartScenario(t)
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 1, LeaderID: 1, Term: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
			state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}
			state.LogState = pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{
				"a": {Tick: 1, TaskServiceCreated: true, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
				"b": {Tick: 1, TaskServiceCreated: true, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}},
				"c": {Tick: 1, TaskServiceCreated: true},
			}}
			r := hakeeper.NewStateMachine(0, 1)
			var index uint64
			apply := func(cmd []byte) sm.Result {
				t.Helper()
				index++
				result, err := r.Update(sm.Entry{Index: index, Cmd: cmd})
				require.NoError(t, err)
				return result
			}
			heartbeat := func(ack uint64) pb.CommandBatch {
				t.Helper()
				data, err := (&pb.TNStoreHeartbeat{UUID: "tn-a", CommandDeliveryAckSupported: true, AckedCommandBatchID: ack}).Marshal()
				require.NoError(t, err)
				var b pb.CommandBatch
				require.NoError(t, b.Unmarshal(apply(hakeeper.GetTNStoreHeartbeatCmd(data)).Data))
				return b
			}
			logHB, err := (&pb.LogStoreHeartbeat{UUID: "log", CommandDeliverySupported: true, Replicas: []pb.LogReplicaInfo{{LogShardInfo: pb.LogShardInfo{ShardID: hakeeper.DefaultHAKeeperShardID, Epoch: 1, Replicas: map[uint64]string{1: "log"}}}}}).Marshal()
			require.NoError(t, err)
			apply(hakeeper.GetLogStoreHeartbeatCmd(logHB))
			require.Equal(t, uint64(2), apply(hakeeper.GetEnableCommandDeliveryCmd()).Value)
			if enabled {
				apply(hakeeper.GetLogStoreHeartbeatCmd(logHB))
				heartbeat(0)
				require.Equal(t, uint64(1), apply(hakeeper.GetEnableCommandDeliveryCmd()).Value)
			}
			first := c.Check(alloc, state, false)
			require.Len(t, first, 2)
			tnFirst := first[1]
			apply(hakeeper.GetUpdateCommandsCmd(1, first))
			logBatch := func() *pb.CommandBatch {
				value, err := r.Lookup(&hakeeper.ScheduleCommandQuery{UUID: "c"})
				require.NoError(t, err)
				return value.(*pb.CommandBatch)
			}
			originalLog := logBatch()
			require.Len(t, originalLog.Commands, 1)
			original := heartbeat(0)
			require.Len(t, original.Commands, 1)
			require.NotZero(t, original.BatchID)
			for i := 0; i < 3; i++ {
				apply(hakeeper.GetUpdateCommandsCmd(1, c.Check(alloc, state, false)))
				require.Equal(t, original, heartbeat(0), "pending retry must not grow or renumber the batch")
				require.Equal(t, originalLog, logBatch(), "mixed Log retry must not multiply pending equivalent commands")
			}
			store := state.LogState.Stores["c"]
			store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}
			state.LogState.Stores["c"] = store
			// A reconstructed coordinator can allocate another ID for the same intent.
			c = NewCoordinator("", hakeeper.Config{})
			state.Tick = 10000
			for uuid, store := range state.LogState.Stores {
				store.Tick = state.Tick
				state.LogState.Stores[uuid] = store
			}
			info := state.TNState.Stores["tn-a"]
			info.Tick = state.Tick
			state.TNState.Stores["tn-a"] = info
			replanned := c.Check(alloc, state, false)
			require.Len(t, replanned, 1)
			require.NotEqual(t, tnFirst.ConfigChange.Replica.ReplicaID, replanned[0].ConfigChange.Replica.ReplicaID)
			apply(hakeeper.GetUpdateCommandsCmd(1, replanned))
			require.Equal(t, original, heartbeat(0), "existing ACK coalescing also bounds allocator churn")
			require.Empty(t, heartbeat(original.BatchID).Commands, "exact transport ACK still retires ordinary TN work")
			apply(hakeeper.GetUpdateCommandsCmd(1, c.Check(alloc, state, false)))
			next := heartbeat(original.BatchID)
			require.Len(t, next.Commands, 1)
			require.NotEqual(t, original.BatchID, next.BatchID)
			require.NotEqual(t, original.CommandIDs, next.CommandIDs)
			require.Equal(t, next, heartbeat(original.BatchID), "stale ACK cannot consume the new attempt")
			require.Empty(t, heartbeat(next.BatchID).Commands)
			info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: tnFirst.ConfigChange.Replica.ReplicaID}}
			state.TNState.Stores["tn-a"] = info
			require.Empty(t, c.Check(alloc, state, false))
			require.Empty(t, c.OperatorController.GetOperators(2))
		})
	}
}
