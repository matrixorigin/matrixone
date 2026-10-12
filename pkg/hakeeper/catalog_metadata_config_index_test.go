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
	"bytes"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestCatalogTokenlessStaleCommandIsReclaimedAtCompletion(t *testing.T) {
	s := catalogRuntimeFixture(t)
	s.state.LogState.Stores["third"] = s.state.LogState.Stores["new-log"]
	first := unhappyCommand(pb.AddReplica, 2, "new-log")
	stale := unhappyCommand(pb.AddNonVotingReplica, 3, "third")
	unhappyQueue(t, s, first, stale)
	batch := unhappyHeartbeat(t, s, unhappyLogHeartbeat(s, "inc"))
	require.Len(t, batch.Commands, 1)
	require.NotNil(t, batch.Commands[0].CatalogMetadataMembership)
	require.Len(t, s.state.ScheduleCommands["log"].Commands, 1)
	require.Nil(t, s.state.ScheduleCommands["log"].Commands[0].CatalogMetadataMembership)
	require.True(t, s.hasPendingHAKeeperAdmission())
	complete := runtimeRequest(s, pb.CATALOG_ACTION_COMPLETE_MEMBERSHIP)
	complete.Token = batch.Commands[0].CatalogMetadataMembership.Token
	complete.ObservedConfigChangeIndex = 8
	complete.ObservedVoting = map[uint64]string{1: "log", 2: "new-log"}
	runtimeApply(t, s, complete, CatalogMetadataApplied)
	require.NotContains(t, s.state.ScheduleCommands, "log")
	require.False(t, s.hasPendingHAKeeperAdmission())
	// A command generated late from the stale checker view is rejected by its
	// real UpdateCommands admission boundary. No heartbeat, catalog operation,
	// or restart is needed to keep admission unblocked.
	unhappyQueue(t, s, stale)
	require.NotContains(t, s.state.ScheduleCommands, "log")
	require.False(t, s.hasPendingHAKeeperAdmission())
	// Simulate a snapshot written by the buggy version; do not use the fixed
	// admission path to manufacture an impossible current-version state.
	s.state.ScheduleCommands["log"] = pb.CommandBatch{Commands: []pb.ScheduleCommand{stale}}
	require.True(t, s.hasPendingHAKeeperAdmission())
	s = runtimeRestore(t, s)
	require.NotContains(t, s.state.ScheduleCommands, "log", "snapshot recovery must reclaim inherited stale history")
	require.False(t, s.hasPendingHAKeeperAdmission())
	// Commands for the confirmed configuration and a future configuration
	// remain pending; only a strictly older epoch is terminally obsolete.
	equal, future := unhappyCommand(pb.AddNonVotingReplica, 3, "third"), unhappyCommand(pb.AddNonVotingReplica, 4, "future")
	equal.ConfigChange.Replica.Epoch = 8
	future.ConfigChange.Replica.Epoch = 9
	unhappyQueue(t, s, equal, future)
	require.Len(t, s.state.ScheduleCommands["log"].Commands, 2)
}

func TestCatalogConfirmedConfigurationFencesStaleReservation(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		name := "continuous"
		if recovery {
			name = "snapshot-recovery"
		}
		t.Run(name, func(t *testing.T) {
			s := catalogRuntimeFixture(t)
			s.state.LogState.Stores["third"] = s.state.LogState.Stores["new-log"]
			oldHeartbeat := unhappyLogHeartbeat(s, "inc")
			reserve := runtimeRequest(s, pb.CATALOG_ACTION_RESERVE_MEMBERSHIP)
			reserve.Membership = &pb.CatalogMetadataMembershipOperation{ConfigChangeIndex: 7, ReplicaID: 2, UUID: "new-log", StoreIncarnation: "inc", ChangeType: pb.AddReplica}
			token := runtimeApply(t, s, reserve, CatalogMetadataApplied)
			complete := runtimeRequest(s, pb.CATALOG_ACTION_COMPLETE_MEMBERSHIP)
			complete.Token, complete.ObservedConfigChangeIndex = token, 8
			complete.ObservedVoting = map[uint64]string{1: "log", 2: "new-log"}
			runtimeApply(t, s, complete, CatalogMetadataApplied)
			if recovery {
				s = runtimeRestore(t, s)
			}
			require.Equal(t, uint64(8), s.state.CatalogMetadataBarrier.Arbitration.ConfirmedConfigChangeIndex)
			require.False(t, s.catalogMetadataReplicasReady())
			require.Equal(t, uint64(7), s.state.LogState.Shards[DefaultHAKeeperShardID].Epoch, "completion must not manufacture a heartbeat")
			next := runtimeRequest(s, pb.CATALOG_ACTION_RESERVE_MEMBERSHIP)
			next.Membership = &pb.CatalogMetadataMembershipOperation{ConfigChangeIndex: 7, ReplicaID: 3, UUID: "third", StoreIncarnation: "inc", ChangeType: pb.AddReplica}
			unhappyReject(t, s, next, CatalogMetadataRejected)
			unhappyHeartbeat(t, s, oldHeartbeat)
			unhappyReject(t, s, next, CatalogMetadataRejected)
			require.Nil(t, s.state.CatalogMetadataBarrier.Arbitration.Reservation)
			// The actual configuration catches up; the next expected set must
			// retain the member added by the preceding completed operation.
			heartbeat := oldHeartbeat
			heartbeat.Replicas = []pb.LogReplicaInfo{{ReplicaID: 1, LogShardInfo: pb.LogShardInfo{
				ShardID: DefaultHAKeeperShardID, Epoch: 8, Replicas: complete.ObservedVoting,
			}}}
			unhappyHeartbeat(t, s, heartbeat)
			next.Membership.ConfigChangeIndex = 8
			token = runtimeApply(t, s, next, CatalogMetadataApplied)
			require.Equal(t, "new-log", s.state.CatalogMetadataBarrier.Arbitration.Reservation.ExpectedVoting[2])
			bad := proto.Clone(&s.state).(*pb.HAKeeperRSMState)
			bad.CatalogMetadataBarrier.Arbitration.Reservation.ConfigChangeIndex = 7
			payload, err := bad.Marshal()
			require.NoError(t, err)
			encoded := marshalSnapshotEnvelopeFixture(t, 1, snapshotRequiredFeatures(bad), payload)
			live := catalogRuntimeFixture(t)
			before := proto.Clone(&live.state)
			err = live.RecoverFromSnapshot(bytes.NewReader(encoded), nil, nil)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
			require.True(t, proto.Equal(before, &live.state))
			complete = runtimeRequest(s, pb.CATALOG_ACTION_COMPLETE_MEMBERSHIP)
			complete.Token, complete.ObservedConfigChangeIndex = token, 9
			complete.ObservedVoting = map[uint64]string{1: "log", 2: "new-log", 3: "third"}
			runtimeApply(t, s, complete, CatalogMetadataApplied)
			heartbeat.Replicas[0].Epoch, heartbeat.Replicas[0].Replicas = 9, complete.ObservedVoting
			for _, member := range []struct {
				id   uint64
				uuid string
			}{{1, "log"}, {2, "new-log"}, {3, "third"}} {
				heartbeat.UUID, heartbeat.Replicas[0].ReplicaID = member.uuid, member.id
				unhappyHeartbeat(t, s, heartbeat)
			}
			s = runtimeRestore(t, s)
			require.Equal(t, uint64(9), s.state.CatalogMetadataBarrier.Arbitration.ConfirmedConfigChangeIndex)
			runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataApplied)
		})
	}
}
