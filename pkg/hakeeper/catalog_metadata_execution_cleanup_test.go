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

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func catalogExecutionHeartbeat(s *stateMachine, uuid string, local ...pb.LogReplicaInfo) pb.LogStoreHeartbeat {
	return pb.LogStoreHeartbeat{UUID: uuid, StoreIncarnation: "inc", Replicas: local,
		CatalogMetadataCapabilities: &pb.CatalogMetadataCapabilities{HAKeeperBarrierProtocol: 1}}
}

func TestCatalogRemovedIdentityReclaimsTokenlessExecutionCommands(t *testing.T) {
	s := catalogRuntimeFixture(t)
	shard := s.state.LogState.Shards[DefaultHAKeeperShardID]
	shard.Replicas[2], shard.ReplicaStoreIncarnations[2] = "new-log", "inc"
	s.state.LogState.Shards[DefaultHAKeeperShardID] = shard
	s.state.CatalogMetadataBarrier.Arbitration.Members[2] = pb.CatalogMetadataReplicaIdentity{UUID: "new-log", StoreIncarnation: "inc"}

	// KillZombie is not valid for a currently admitted identity.
	admittedKill := unhappyCommand(pb.KillZombie, 2, "new-log")
	admittedKill.UUID, admittedKill.ConfigChange.Replica.Epoch = "new-log", 0
	unhappyQueue(t, s, admittedKill)
	require.NotContains(t, s.state.ScheduleCommands, "new-log")
	// An admitted start with no token can wait for its target heartbeat. Once
	// membership removal commits, it cannot become a start permit and must not
	// remain an admission blocker.
	start := unhappyCommand(pb.StartReplica, 2, "new-log")
	start.UUID, start.ConfigChange.Replica.Epoch = "new-log", 0
	unhappyQueue(t, s, start)
	require.True(t, s.hasPendingHAKeeperAdmission())
	reserve := runtimeRequest(s, pb.CATALOG_ACTION_RESERVE_MEMBERSHIP)
	reserve.Membership = &pb.CatalogMetadataMembershipOperation{ConfigChangeIndex: 7, ReplicaID: 2, UUID: "new-log", StoreIncarnation: "inc", ChangeType: pb.RemoveReplica}
	token := runtimeApply(t, s, reserve, CatalogMetadataApplied)
	complete := runtimeRequest(s, pb.CATALOG_ACTION_COMPLETE_MEMBERSHIP)
	complete.Token, complete.ObservedConfigChangeIndex = token, 8
	complete.ObservedVoting = map[uint64]string{1: "log"}
	runtimeApply(t, s, complete, CatalogMetadataApplied)
	require.NotContains(t, s.state.ScheduleCommands, "new-log")
	require.False(t, s.hasPendingHAKeeperAdmission())

	// The removal-created retirement permit still makes a tokenless stop useful.
	stop := unhappyCommand(pb.StopReplica, 2, "new-log")
	stop.UUID, stop.ConfigChange.Replica.Epoch = "new-log", 0
	unhappyQueue(t, s, stop)
	require.Contains(t, s.state.ScheduleCommands, "new-log")
	batch := unhappyHeartbeat(t, s, catalogExecutionHeartbeat(s, "new-log"))
	require.Len(t, batch.Commands, 1)
	require.NotNil(t, batch.Commands[0].CatalogMetadataStart)
	require.True(t, batch.Commands[0].CatalogMetadataStart.Revoked)
	retire := *batch.Commands[0].CatalogMetadataStart
	done := runtimeRequest(s, pb.CATALOG_ACTION_COMPLETE_START)
	done.Token, retire.Revoked, retire.RevocationPending = retire.Token, true, false
	done.StartPermit = &retire
	runtimeApply(t, s, done, CatalogMetadataApplied)
	require.Empty(t, s.state.CatalogMetadataBarrier.Arbitration.StartPermits)

	stale := []pb.ScheduleCommand{
		unhappyCommand(pb.StartReplica, 2, "new-log"),
		unhappyCommand(pb.StartNonVotingReplica, 2, "new-log"),
		unhappyCommand(pb.StopReplica, 2, "new-log"),
		unhappyCommand(pb.StopNonVotingReplica, 2, "new-log"),
		unhappyCommand(pb.KillZombie, 2, "new-log"),
	}
	for i := range stale {
		stale[i].UUID, stale[i].ConfigChange.Replica.Epoch = "new-log", 0
	}
	// Real Update admission rejects every retired identity immediately. A target
	// heartbeat has nothing to deliver and no other catalog action is required.
	unhappyQueue(t, s, stale...)
	require.NotContains(t, s.state.ScheduleCommands, "new-log")
	require.False(t, s.hasPendingHAKeeperAdmission())
	require.Empty(t, unhappyHeartbeat(t, s, catalogExecutionHeartbeat(s, "new-log")).Commands)

	// A snapshot from the buggy version can contain the same residue. Recovery
	// publishes the state only after applying the identical cleanup predicate.
	s.state.ScheduleCommands["new-log"] = pb.CommandBatch{Commands: stale}
	require.True(t, s.hasPendingHAKeeperAdmission())
	s = runtimeRestore(t, s)
	require.NotContains(t, s.state.ScheduleCommands, "new-log")
	require.False(t, s.hasPendingHAKeeperAdmission())
	require.Empty(t, unhappyHeartbeat(t, s, catalogExecutionHeartbeat(s, "new-log")).Commands)
}

func TestCatalogObservedPreCutoverZombieStopIsNotReclaimed(t *testing.T) {
	s := catalogRuntimeFixture(t)
	shard := s.state.LogState.Shards[DefaultHAKeeperShardID]
	local := pb.LogReplicaInfo{ReplicaID: 99, LogShardInfo: shard}
	hb := catalogExecutionHeartbeat(s, "new-log", local)
	unhappyHeartbeat(t, s, hb)
	kill := unhappyCommand(pb.KillZombie, 99, "new-log")
	kill.UUID, kill.ConfigChange.Replica.Epoch = "new-log", 0
	unhappyQueue(t, s, kill)
	require.Contains(t, s.state.ScheduleCommands, "new-log")
	batch := unhappyHeartbeat(t, s, hb)
	require.Len(t, batch.Commands, 1)
	require.Equal(t, pb.KillZombie, batch.Commands[0].ConfigChange.ChangeType)
	require.NotNil(t, batch.Commands[0].CatalogMetadataStart)
	require.True(t, batch.Commands[0].CatalogMetadataStart.Revoked)
}
