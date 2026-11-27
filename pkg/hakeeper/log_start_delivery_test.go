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

package hakeeper

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	sm "github.com/lni/dragonboat/v4/statemachine"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestLogHeartbeatAuthorizesStartsWithoutChangingDeliveryState(t *testing.T) {
	const store = "store-a"
	start := func(kind pb.ConfigChangeType, shard, replica uint64, target string) pb.ScheduleCommand {
		return pb.ScheduleCommand{UUID: store, ServiceType: pb.LogService,
			ConfigChange: &pb.ConfigChange{ChangeType: kind, Replica: pb.Replica{UUID: target, ShardID: shard, ReplicaID: replica}}}
	}
	valid := start(pb.StartReplica, 1, 11, store)
	removed := start(pb.StartReplica, 1, 12, store)
	wrongRole := start(pb.StartNonVotingReplica, 1, 11, store)
	wrongTarget := start(pb.StartReplica, 1, 11, "store-b")
	wrongDisk := start(pb.StartReplica, 2, 21, store)
	nonvoting := start(pb.StartNonVotingReplica, 3, 31, store)
	bootstrap := start(pb.StartReplica, 4, 41, store)
	bootstrap.Bootstrapping = true
	bootstrap.ConfigChange.InitialMembers = map[uint64]string{41: store}
	catalog := start(pb.StartReplica, 0, 51, store)
	catalog.CatalogMetadataStart = &pb.CatalogMetadataStartPermit{Token: 7}
	shutdown := pb.ScheduleCommand{UUID: store, ServiceType: pb.LogService, ShutdownStore: &pb.ShutdownStore{StoreID: store}}
	cmds := []pb.ScheduleCommand{valid, removed, wrongRole, wrongTarget, wrongDisk, nonvoting, bootstrap, catalog, shutdown}
	ids := make([]pb.ScheduleCommandID, len(cmds))
	for i := range ids {
		ids[i] = pb.ScheduleCommandID{OriginBatchID: 7, CommandIndex: uint64(i)}
	}

	machine := NewStateMachine(0, 1).(*stateMachine)
	machine.state.State = pb.HAKeeperBootstrapCommandsReceived
	machine.state.LogState.Stores[store] = pb.LogStoreInfo{StoreIncarnation: "disk-new"}
	machine.state.LogState.Shards[1] = pb.LogShardInfo{ShardID: 1, Epoch: 5, Replicas: map[uint64]string{11: store}, ReplicaStoreIncarnations: map[uint64]string{11: "disk-new"}}
	machine.state.LogState.Shards[2] = pb.LogShardInfo{ShardID: 2, Epoch: 5, Replicas: map[uint64]string{21: store}, ReplicaStoreIncarnations: map[uint64]string{21: "disk-old"}}
	machine.state.LogState.Shards[3] = pb.LogShardInfo{ShardID: 3, Epoch: 5, NonVotingReplicas: map[uint64]string{31: store}}
	machine.state.ScheduleCommands[store] = pb.CommandBatch{BatchID: 7, Commands: cmds, CommandIDs: ids}
	before := proto.Clone(&machine.state).(*pb.HAKeeperRSMState)
	hb := pb.LogStoreHeartbeat{UUID: store, StoreIncarnation: "disk-new"}
	data, err := hb.Marshal()
	require.NoError(t, err)
	result, err := machine.Update(sm.Entry{Index: 1, Cmd: GetLogStoreHeartbeatCmd(data)})
	require.NoError(t, err)
	var got pb.CommandBatch
	require.NoError(t, got.Unmarshal(result.Data))
	expectedVoter, expectedLearner := *valid.ConfigChange, *nonvoting.ConfigChange
	expectedVoter.Replica.Epoch, expectedLearner.Replica.Epoch = 5, 5
	valid.ConfigChange, nonvoting.ConfigChange = &expectedVoter, &expectedLearner
	require.Equal(t, []pb.ScheduleCommand{valid, nonvoting, bootstrap, catalog, shutdown}, got.Commands)
	require.Equal(t, []pb.ScheduleCommandID{ids[0], ids[5], ids[6], ids[7], ids[8]}, got.CommandIDs)

	// Reproduce the original state transitions through the existing owner.
	// Response filtering must not alter queues, bootstrap retry, or permits.
	reference := NewStateMachine(0, 1).(*stateMachine)
	reference.state = *before
	reference.state.Index = 1
	reference.state.LogState.Update(hb, reference.state.Tick)
	reference.observeCatalogStartResult(hb)
	reference.attachPendingCatalogStart(reference.getCommandBatchFiltered(store, nil), hb)
	require.True(t, proto.Equal(&reference.state, &machine.state), "response filtering must not alter replicated state")
	pending := machine.state.ScheduleCommands[store]
	require.Equal(t, []pb.ScheduleCommand{bootstrap}, pending.Commands)
	require.Equal(t, []pb.ScheduleCommandID{ids[6]}, pending.CommandIDs)
}
