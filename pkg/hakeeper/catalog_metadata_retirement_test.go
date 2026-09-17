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

func TestCatalogUnfinishedGenerationCanBeSupersededWithoutLosingOwners(t *testing.T) {
	for _, phase := range []pb.CatalogMetadataBarrierPhase{pb.CATALOG_METADATA_BARRIER_PREPARING, pb.CATALOG_METADATA_BARRIER_SEALED} {
		t.Run(phase.String(), func(t *testing.T) {
			s := catalogRuntimeFixture(t)
			s.state.CNState.Stores["cn"] = pb.CNStoreInfo{ViewMetadataAdmissionGeneration: 1,
				CatalogMetadataCapabilities: &pb.CatalogMetadataCapabilities{BarrierParticipantProtocol: 1, ViewDependencyProtocol: 1, RecoveryProtocol: 1}}
			unhappyBegin(t, s)
			if phase == pb.CATALOG_METADATA_BARRIER_SEALED {
				ack := runtimeRequest(s, pb.CATALOG_ACTION_ACK_TARGET)
				ack.Target = &pb.CatalogMetadataBarrierTarget{ServiceType: pb.CNService, UUID: "cn", Generation: 1, ObservedPreparing: true}
				runtimeApply(t, s, ack, CatalogMetadataApplied)
				runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_SEAL), CatalogMetadataApplied)
			}
			old := s.state.CatalogMetadataBarrier.Targets[0]
			token := runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataApplied)
			r := runtimeRequest(s, pb.CATALOG_ACTION_SUPERSEDE)
			r.Token, r.RequiredViewDependencyProtocol, r.RequiredRecoveryProtocol = token, 1, 1
			// A same-UUID replacement cannot erase the captured old owner.
			info := s.state.CNState.Stores["cn"]
			info.ViewMetadataAdmissionGeneration = 2
			s.state.CNState.Stores["cn"] = info
			unhappyReject(t, s, r, CatalogMetadataRejected)
			info.ViewMetadataAdmissionGeneration = 1
			s.state.CNState.Stores["cn"] = info
			runtimeApply(t, s, r, CatalogMetadataApplied)
			b := runtimeRestore(t, s).state.CatalogMetadataBarrier
			require.Equal(t, uint64(2), b.RequiredGeneration)
			require.Equal(t, uint64(2), b.MembershipEpoch)
			require.Equal(t, old.Generation, b.Targets[0].Generation)
			require.Equal(t, old.CapturedTick, b.Targets[0].CapturedTick)
			require.False(t, b.Targets[0].ObservedPreparing)
			require.False(t, b.Targets[0].SealComplete)
		})
	}
}

func TestCatalogLegacyZombieRetirementAndReadmission(t *testing.T) {
	s := catalogRuntimeFixture(t)
	command := unhappyCommand(pb.KillZombie, 99, "new-log")
	command.UUID = "new-log"
	unhappyQueue(t, s, command)
	hb := pb.LogStoreHeartbeat{UUID: "new-log", StoreIncarnation: "inc",
		CatalogMetadataCapabilities: &pb.CatalogMetadataCapabilities{HAKeeperBarrierProtocol: 1},
		Replicas:                    []pb.LogReplicaInfo{{LogShardInfo: s.state.LogState.Shards[DefaultHAKeeperShardID], ReplicaID: 99}},
	}
	batch := unhappyHeartbeat(t, s, hb)
	require.Len(t, batch.Commands, 1)
	permit := *batch.Commands[0].CatalogMetadataStart
	require.True(t, permit.Revoked)
	require.NotZero(t, permit.Token)
	require.NotContains(t, s.state.ScheduleCommands, "new-log")
	s = runtimeRestore(t, s)
	// Lost delivery must replay cancellation, never start the retired ID.
	retry := unhappyHeartbeat(t, s, hb)
	require.Len(t, retry.Commands, 1)
	require.Equal(t, pb.StopReplica, retry.Commands[0].ConfigChange.ChangeType)
	require.Equal(t, permit.Token, retry.Commands[0].CatalogMetadataStart.Token)
	hb.CatalogMetadataStartResult = &permit
	unhappyHeartbeat(t, s, hb)
	require.True(t, s.state.CatalogMetadataBarrier.Arbitration.StartPermits[0].RevocationPending)
	hb.Replicas = nil
	unhappyHeartbeat(t, s, hb)
	require.Empty(t, s.state.CatalogMetadataBarrier.Arbitration.StartPermits)
	r := runtimeRequest(s, pb.CATALOG_ACTION_RESERVE_MEMBERSHIP)
	r.Membership = &pb.CatalogMetadataMembershipOperation{ConfigChangeIndex: 7, ReplicaID: 2, UUID: "new-log", StoreIncarnation: "inc", ChangeType: pb.AddReplica}
	runtimeApply(t, s, r, CatalogMetadataApplied)
}
