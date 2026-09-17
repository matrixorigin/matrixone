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
	"maps"
	"testing"

	"github.com/gogo/protobuf/proto"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

// 拒绝请求可以推进 Raft index，但不能改变已提交的业务状态。
func unhappyReject(t *testing.T, s *stateMachine, r pb.CatalogMetadataRequest, status uint64) {
	t.Helper()
	before := proto.Clone(s.state.CatalogMetadataBarrier)
	runtimeApply(t, s, r, status)
	require.True(t, proto.Equal(before, s.state.CatalogMetadataBarrier), "拒绝请求改变了 durable barrier")
}

func unhappyHeartbeat(t *testing.T, s *stateMachine, hb pb.LogStoreHeartbeat) pb.CommandBatch {
	t.Helper()
	data, err := hb.Marshal()
	require.NoError(t, err)
	result, err := s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: GetLogStoreHeartbeatCmd(data)})
	require.NoError(t, err)
	var batch pb.CommandBatch
	require.NoError(t, batch.Unmarshal(result.Data))
	return batch
}

func unhappyLogHeartbeat(s *stateMachine, incarnation string) pb.LogStoreHeartbeat {
	shard := s.state.LogState.Shards[DefaultHAKeeperShardID]
	return pb.LogStoreHeartbeat{
		UUID: "log", StoreIncarnation: incarnation,
		CatalogMetadataCapabilities: &pb.CatalogMetadataCapabilities{HAKeeperBarrierProtocol: 1},
		Replicas:                    []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}},
	}
}

func unhappyQueue(t *testing.T, s *stateMachine, commands ...pb.ScheduleCommand) {
	t.Helper()
	_, err := s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: GetUpdateCommandsCmd(s.state.Term+1, commands)})
	require.NoError(t, err)
}

func unhappyCommand(change pb.ConfigChangeType, id uint64, uuid string) pb.ScheduleCommand {
	return pb.ScheduleCommand{UUID: "log", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
		ChangeType: change,
		Replica:    pb.Replica{ShardID: DefaultHAKeeperShardID, ReplicaID: id, UUID: uuid, Epoch: 7},
	}}
}

func unhappyBegin(t *testing.T, s *stateMachine) {
	t.Helper()
	token := runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataApplied)
	r := runtimeRequest(s, pb.CATALOG_ACTION_BEGIN)
	r.Token, r.RequiredViewDependencyProtocol, r.RequiredRecoveryProtocol = token, 1, 1
	runtimeApply(t, s, r, CatalogMetadataApplied)
}

func unhappyReceipt(s *stateMachine, action pb.CatalogMetadataAction, claim uint64) pb.CatalogMetadataRequest {
	r := runtimeRequest(s, action)
	r.Receipt = &pb.CatalogMetadataReceipt{Action: action, MembershipEpoch: r.MembershipEpoch,
		RequiredGeneration: r.RequiredGeneration, ClaimID: claim, Digest: bytes.Repeat([]byte{byte(action)}, 32)}
	return r
}

func TestCatalogUnhappyMembershipRequiresWholeConfiguration(t *testing.T) {
	for _, change := range []pb.ConfigChangeType{pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica} {
		t.Run(change.String(), func(t *testing.T) {
			s := catalogRuntimeFixture(t)
			// 独立的 voting/non-voting 成员使目标正确但旁系成员错误可被区分。
			shard := s.state.LogState.Shards[DefaultHAKeeperShardID]
			shard.Replicas[3] = "voter"
			shard.NonVotingReplicas = map[uint64]string{4: "observer"}
			for id, uuid := range map[uint64]string{3: "voter", 4: "observer"} {
				s.state.CatalogMetadataBarrier.Arbitration.Members[id] = pb.CatalogMetadataReplicaIdentity{UUID: uuid, StoreIncarnation: "inc", NonVoting: id == 4}
				shard.ReplicaStoreIncarnations[id] = "inc"
			}
			if change == pb.RemoveReplica || change == pb.RemoveNonVotingReplica {
				s.state.CatalogMetadataBarrier.Arbitration.Members[2] = pb.CatalogMetadataReplicaIdentity{UUID: "new-log", StoreIncarnation: "inc", NonVoting: change == pb.RemoveNonVotingReplica}
				shard.ReplicaStoreIncarnations[2] = "inc"
				if change == pb.RemoveReplica {
					shard.Replicas[2] = "new-log"
				} else {
					shard.NonVotingReplicas[2] = "new-log"
				}
			}
			s.state.LogState.Shards[DefaultHAKeeperShardID] = shard
			r := runtimeRequest(s, pb.CATALOG_ACTION_RESERVE_MEMBERSHIP)
			r.Membership = &pb.CatalogMetadataMembershipOperation{ConfigChangeIndex: 7, ReplicaID: 2, UUID: "new-log", StoreIncarnation: "inc", ChangeType: change}
			token := runtimeApply(t, s, r, CatalogMetadataApplied)
			s = runtimeRestore(t, s)
			voting, nonVoting := maps.Clone(shard.Replicas), maps.Clone(shard.NonVotingReplicas)
			switch change {
			case pb.AddReplica:
				voting[2] = "new-log"
			case pb.AddNonVotingReplica:
				nonVoting[2] = "new-log"
			case pb.RemoveReplica:
				delete(voting, 2)
			case pb.RemoveNonVotingReplica:
				delete(nonVoting, 2)
			}
			for _, tc := range []struct {
				name   string
				mutate func(map[uint64]string, map[uint64]string)
			}{
				{"missing-voter", func(v, n map[uint64]string) { delete(v, 3) }},
				{"changed-voter", func(v, n map[uint64]string) { v[3] = "replacement" }},
				{"extra-voter", func(v, n map[uint64]string) { v[9] = "extra" }},
				{"missing-observer", func(v, n map[uint64]string) { delete(n, 4) }},
				{"changed-observer", func(v, n map[uint64]string) { n[4] = "replacement" }},
				{"extra-observer", func(v, n map[uint64]string) { n[9] = "extra" }},
				{"observer-became-voter", func(v, n map[uint64]string) { delete(n, 4); v[4] = "observer" }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					r := runtimeRequest(s, pb.CATALOG_ACTION_COMPLETE_MEMBERSHIP)
					r.Token, r.ObservedConfigChangeIndex = token, 8
					r.ObservedVoting, r.ObservedNonVoting = maps.Clone(voting), maps.Clone(nonVoting)
					tc.mutate(r.ObservedVoting, r.ObservedNonVoting)
					unhappyReject(t, s, r, CatalogMetadataConflict)
				})
			}
			r = runtimeRequest(s, pb.CATALOG_ACTION_COMPLETE_MEMBERSHIP)
			r.Token, r.ObservedConfigChangeIndex, r.ObservedVoting, r.ObservedNonVoting = token, 8, voting, nonVoting
			runtimeApply(t, s, r, CatalogMetadataApplied)
			require.Nil(t, runtimeRestore(t, s).state.CatalogMetadataBarrier.Arbitration.Reservation)
		})
	}
}

func TestCatalogUnhappyIncarnationRevocationIsSticky(t *testing.T) {
	s := catalogRuntimeFixture(t)
	unhappyHeartbeat(t, s, unhappyLogHeartbeat(s, "replacement"))
	require.True(t, s.state.CatalogMetadataBarrier.Arbitration.Members[1].Revoked)
	s = runtimeRestore(t, s)
	unhappyHeartbeat(t, s, unhappyLogHeartbeat(s, "inc"))
	require.Equal(t, "inc", s.state.LogState.Stores["log"].StoreIncarnation)
	require.True(t, s.state.CatalogMetadataBarrier.Arbitration.Members[1].Revoked)
	r := runtimeRequest(s, pb.CATALOG_ACTION_GRANT_START)
	r.StartPermit = &pb.CatalogMetadataStartPermit{ReplicaID: 1, UUID: "log", StoreIncarnation: "inc"}
	unhappyReject(t, s, r, CatalogMetadataRejected)
	token := runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataApplied)
	r = runtimeRequest(s, pb.CATALOG_ACTION_BEGIN)
	r.Token, r.RequiredViewDependencyProtocol, r.RequiredRecoveryProtocol = token, 1, 1
	unhappyReject(t, s, r, CatalogMetadataRejected)
	require.True(t, runtimeRestore(t, s).state.CatalogMetadataBarrier.Arbitration.Members[1].Revoked)
}

func TestCatalogUnhappyFenceAllocatorAndOwnerCAS(t *testing.T) {
	s := catalogRuntimeFixture(t)
	acquire := runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE)
	token := runtimeApply(t, s, acquire, CatalogMetadataApplied)
	for _, action := range []pb.CatalogMetadataAction{pb.CATALOG_ACTION_BEGIN, pb.CATALOG_ACTION_RELEASE_FENCE, pb.CATALOG_ACTION_TAKEOVER_FENCE} {
		r := runtimeRequest(s, action)
		r.Token, r.Owner, r.NewOwner = token, "other-owner", "successor"
		r.RequiredViewDependencyProtocol, r.RequiredRecoveryProtocol = 1, 1
		unhappyReject(t, s, r, CatalogMetadataStale)
	}
	r := runtimeRequest(s, pb.CATALOG_ACTION_RELEASE_FENCE)
	r.Token = token
	runtimeApply(t, s, r, CatalogMetadataApplied)
	s = runtimeRestore(t, s)
	unhappyReject(t, s, acquire, CatalogMetadataStale)
	require.Nil(t, s.state.CatalogMetadataBarrier.Arbitration.Fence)
	fresh := runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE)
	fresh.Owner = "successor"
	next := runtimeApply(t, s, fresh, CatalogMetadataApplied)
	require.Greater(t, next, token)
	unhappyReject(t, s, acquire, CatalogMetadataRejected)
	unhappyReject(t, s, r, CatalogMetadataStale)
	require.Equal(t, next, runtimeApply(t, s, fresh, CatalogMetadataApplied))
}

func TestCatalogUnhappyScheduleReservationSurvivesDelivery(t *testing.T) {
	s := catalogRuntimeFixture(t)
	command := unhappyCommand(pb.AddReplica, 2, "new-log")
	unhappyQueue(t, s, command)
	require.Nil(t, s.state.CatalogMetadataBarrier.Arbitration.Reservation)
	batch := unhappyHeartbeat(t, s, unhappyLogHeartbeat(s, "inc"))
	require.Len(t, batch.Commands, 1)
	reservation := batch.Commands[0].CatalogMetadataMembership
	require.NotNil(t, reservation)
	require.NotZero(t, reservation.Token)
	require.Equal(t, map[uint64]string{1: "log", 2: "new-log"}, reservation.ExpectedVoting)
	require.NotContains(t, s.state.ScheduleCommands, "log")
	s = runtimeRestore(t, s)
	require.Equal(t, reservation, s.state.CatalogMetadataBarrier.Arbitration.Reservation)
	unhappyReject(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataRejected)
	// 响应丢失后的重投必须沿用 token，不能重新分配或放过另一个 mutation。
	unhappyQueue(t, s, command, unhappyCommand(pb.AddNonVotingReplica, 3, "other-log"))
	batch = unhappyHeartbeat(t, s, unhappyLogHeartbeat(s, "inc"))
	require.Len(t, batch.Commands, 1)
	require.Equal(t, reservation, batch.Commands[0].CatalogMetadataMembership)
	require.Len(t, s.state.ScheduleCommands["log"].Commands, 1)
	require.Equal(t, reservation.Token, s.state.CatalogMetadataBarrier.Arbitration.LastOperationID)
}

func TestCatalogUnhappyStartCancellationRejectsLateHeartbeat(t *testing.T) {
	s := catalogRuntimeFixture(t)
	unhappyQueue(t, s, unhappyCommand(pb.StartReplica, 1, "log"))
	batch := unhappyHeartbeat(t, s, unhappyLogHeartbeat(s, "inc"))
	require.Len(t, batch.Commands, 1)
	permit := batch.Commands[0].CatalogMetadataStart
	require.NotNil(t, permit)
	hb := unhappyLogHeartbeat(s, "inc")
	ack := *permit
	ack.Completed = true
	hb.CatalogMetadataStartResult = &ack
	// 没有运行中 replica 的 STARTED 不能清掉 pending。
	hb.Replicas = nil
	unhappyHeartbeat(t, s, hb)
	require.False(t, s.state.CatalogMetadataBarrier.Arbitration.StartPermits[0].Completed)
	hb.Replicas = unhappyLogHeartbeat(s, "inc").Replicas
	unhappyHeartbeat(t, s, hb)
	require.True(t, s.state.CatalogMetadataBarrier.Arbitration.StartPermits[0].Completed)
	require.Equal(t, uint64(7), s.state.LogState.Shards[DefaultHAKeeperShardID].Epoch)
	unhappyQueue(t, s, unhappyCommand(pb.StopReplica, 1, "log"))
	batch = unhappyHeartbeat(t, s, unhappyLogHeartbeat(s, "inc"))
	require.Len(t, batch.Commands, 1)
	require.True(t, batch.Commands[0].CatalogMetadataStart.Revoked)
	require.Equal(t, permit.Token, batch.Commands[0].CatalogMetadataStart.Token)
	s = runtimeRestore(t, s)
	pending := s.state.CatalogMetadataBarrier.Arbitration.StartPermits[0]
	require.True(t, pending.RevocationPending)
	require.False(t, pending.Completed)
	unhappyHeartbeat(t, s, hb) // Stop 尚未确认时，迟到的 STARTED 不能撤销 cancel。
	require.Equal(t, pending, s.state.CatalogMetadataBarrier.Arbitration.StartPermits[0])
	unhappyReject(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataRejected)
	ack.Completed, ack.Revoked = false, true
	hb.CatalogMetadataStartResult = &ack
	unhappyHeartbeat(t, s, hb) // REVOKED 与同一 heartbeat 中的运行中 replica 矛盾。
	require.Equal(t, pending, s.state.CatalogMetadataBarrier.Arbitration.StartPermits[0])
	hb.Replicas = nil
	unhappyHeartbeat(t, s, hb)
	require.True(t, s.state.CatalogMetadataBarrier.Arbitration.StartPermits[0].Revoked)
	require.False(t, s.state.CatalogMetadataBarrier.Arbitration.StartPermits[0].RevocationPending)
	runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataApplied)
	ack.Completed, ack.Revoked = true, false
	hb.Replicas = unhappyLogHeartbeat(s, "inc").Replicas
	unhappyHeartbeat(t, s, hb)
	require.True(t, runtimeRestore(t, s).state.CatalogMetadataBarrier.Arbitration.StartPermits[0].Revoked)
}

func TestCatalogUnhappyClaimsAndFixedReceiptSlots(t *testing.T) {
	s := catalogRuntimeFixture(t)
	unhappyBegin(t, s)
	runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_SEAL), CatalogMetadataApplied)
	required := unhappyReceipt(s, pb.CATALOG_ACTION_CATALOG_REQUIRED, 0)
	runtimeApply(t, s, required, CatalogMetadataApplied)
	claim := runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_CLAIM), CatalogMetadataApplied)
	r := runtimeRequest(s, pb.CATALOG_ACTION_CLAIM)
	r.Token = claim
	replacement := runtimeApply(t, s, r, CatalogMetadataApplied)
	require.Greater(t, replacement, claim)
	unhappyReject(t, s, unhappyReceipt(s, pb.CATALOG_ACTION_RECOVERY_STARTED, claim), CatalogMetadataStale)
	started := unhappyReceipt(s, pb.CATALOG_ACTION_RECOVERY_STARTED, replacement)
	runtimeApply(t, s, started, CatalogMetadataApplied)
	unhappyReject(t, s, unhappyReceipt(s, pb.CATALOG_ACTION_COMPLETE, claim), CatalogMetadataStale)
	complete := unhappyReceipt(s, pb.CATALOG_ACTION_COMPLETE, replacement)
	runtimeApply(t, s, complete, CatalogMetadataApplied)
	s = runtimeRestore(t, s)
	for _, receipt := range []pb.CatalogMetadataRequest{required, started, complete} {
		before := proto.Clone(s.state.CatalogMetadataBarrier)
		runtimeApply(t, s, receipt, CatalogMetadataApplied)
		require.True(t, proto.Equal(before, s.state.CatalogMetadataBarrier))
		conflict := proto.Clone(&receipt).(*pb.CatalogMetadataRequest)
		conflict.Receipt.Digest[0]++
		unhappyReject(t, s, *conflict, CatalogMetadataConflict)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*pb.CatalogMetadataRequest)
		status uint64
	}{
		{"unknown-action", func(r *pb.CatalogMetadataRequest) { r.Action = 99 }, CatalogMetadataRejected},
		{"future-generation", func(r *pb.CatalogMetadataRequest) { r.RequiredGeneration++ }, CatalogMetadataStale},
		{"old-epoch", func(r *pb.CatalogMetadataRequest) { r.MembershipEpoch-- }, CatalogMetadataStale},
		{"receipt-generation", func(r *pb.CatalogMetadataRequest) { r.Receipt.RequiredGeneration++ }, CatalogMetadataRejected},
		{"receipt-kind", func(r *pb.CatalogMetadataRequest) { r.Receipt.Action = pb.CATALOG_ACTION_CATALOG_REQUIRED }, CatalogMetadataRejected},
		{"receipt-digest", func(r *pb.CatalogMetadataRequest) { r.Receipt.Digest = []byte{1} }, CatalogMetadataRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := unhappyReceipt(s, pb.CATALOG_ACTION_COMPLETE, replacement)
			tc.mutate(&r)
			unhappyReject(t, s, r, tc.status)
		})
	}
	a := s.state.CatalogMetadataBarrier.Arbitration
	require.NotNil(t, a.RequiredReceipt)
	require.NotNil(t, a.StartedReceipt)
	require.NotNil(t, a.CompletedReceipt)
	require.Equal(t, replacement, a.LastOperationID)
	require.Equal(t, uint64(1), s.state.CatalogMetadataBarrier.CompletedGeneration)
}

func TestCatalogUnhappySnapshotArbitrationCorruptionIsAtomic(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*pb.CatalogMetadataBarrierState)
	}{
		{"missing-members", func(b *pb.CatalogMetadataBarrierState) { b.Arbitration.Members = nil }},
		{"zero-member-id", func(b *pb.CatalogMetadataBarrierState) { b.Arbitration.Members[0] = b.Arbitration.Members[1] }},
		{"member-incarnation", func(b *pb.CatalogMetadataBarrierState) {
			b.Arbitration.Members[1] = pb.CatalogMetadataReplicaIdentity{UUID: "log"}
		}},
		{"disabled-owner", func(b *pb.CatalogMetadataBarrierState) { b.Arbitration.MaintenanceEnabled = false }},
		{"future-fence", func(b *pb.CatalogMetadataBarrierState) { b.Arbitration.Fence.Token++ }},
		{"empty-owner", func(b *pb.CatalogMetadataBarrierState) { b.Arbitration.Fence.Owner = "" }},
		{"wrong-fence-phase", func(b *pb.CatalogMetadataBarrierState) {
			b.Arbitration.Fence.ExpectedPhase = pb.CATALOG_METADATA_BARRIER_SEALED
		}},
		{"wrong-fence-generation", func(b *pb.CatalogMetadataBarrierState) { b.Arbitration.Fence.RequiredGeneration++ }},
		{"consumed-fence", func(b *pb.CatalogMetadataBarrierState) { b.Arbitration.LastConsumedFence = b.Arbitration.Fence.Token }},
		{"future-claim", func(b *pb.CatalogMetadataBarrierState) { b.Arbitration.ClaimID = b.Arbitration.LastOperationID + 1 }},
		{"pending-start-and-fence", func(b *pb.CatalogMetadataBarrierState) {
			b.Arbitration.StartPermits = []pb.CatalogMetadataStartPermit{{Token: 1, ReplicaID: 1, UUID: "log", StoreIncarnation: "inc"}}
		}},
		{"conflicting-start-result", func(b *pb.CatalogMetadataBarrierState) {
			b.Arbitration.Fence = nil
			b.Arbitration.StartPermits = []pb.CatalogMetadataStartPermit{{Token: 1, ReplicaID: 1, UUID: "log", StoreIncarnation: "inc", Completed: true, Revoked: true}}
		}},
		{"cancelpending-completed", func(b *pb.CatalogMetadataBarrierState) {
			b.Arbitration.Fence = nil
			b.Arbitration.StartPermits = []pb.CatalogMetadataStartPermit{{Token: 1, ReplicaID: 1, UUID: "log", StoreIncarnation: "inc", Completed: true, RevocationPending: true}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := catalogRuntimeFixture(t)
			runtimeApply(t, corrupt, runtimeRequest(corrupt, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataApplied)
			tc.mutate(corrupt.state.CatalogMetadataBarrier)
			payload, err := corrupt.state.Marshal()
			require.NoError(t, err)
			encoded := marshalSnapshotEnvelopeFixture(t, 1, snapshotRequiredFeatures(&corrupt.state), payload)
			live := catalogRuntimeFixture(t)
			live.state.NextIDByKey["independent"] = 17
			before := proto.Clone(&live.state)
			err = live.RecoverFromSnapshot(bytes.NewReader(encoded), nil, nil)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "恢复错误=%v", err)
			require.True(t, proto.Equal(before, &live.state), "损坏 snapshot 修改了 live state")
		})
	}
}

func TestCatalogUnhappyParticipantHeartbeatCannotSeal(t *testing.T) {
	for _, kind := range []pb.ServiceType{pb.CNService, pb.ProxyService} {
		t.Run(kind.String(), func(t *testing.T) {
			s := catalogRuntimeFixture(t)
			capability := &pb.CatalogMetadataCapabilities{ViewDependencyProtocol: 1, RecoveryProtocol: 1, BarrierParticipantProtocol: 1}
			if kind == pb.CNService {
				s.state.CNState.Stores["participant"] = pb.CNStoreInfo{ViewMetadataAdmissionGeneration: 11, CatalogMetadataCapabilities: capability}
			} else {
				s.state.ProxyState.Stores["participant"] = pb.ProxyStore{ViewMetadataAdmissionGeneration: 11, CatalogMetadataCapabilities: capability}
			}
			send := func(generation uint64, ack *pb.CatalogMetadataAck) pb.CommandBatch {
				t.Helper()
				var cmd []byte
				if kind == pb.CNService {
					hb := pb.CNStoreHeartbeat{UUID: "participant", ViewMetadataAdmissionGeneration: generation, CatalogMetadataCapabilities: capability, CatalogMetadataAck: ack}
					data, err := hb.Marshal()
					require.NoError(t, err)
					cmd = GetCNStoreHeartbeatCmd(data)
				} else {
					hb := pb.ProxyHeartbeat{UUID: "participant", ViewMetadataAdmissionGeneration: generation, CatalogMetadataCapabilities: capability, CatalogMetadataAck: ack}
					data, err := hb.Marshal()
					require.NoError(t, err)
					cmd = GetProxyHeartbeatCmd(data)
				}
				result, err := s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: cmd})
				require.NoError(t, err)
				var batch pb.CommandBatch
				require.NoError(t, batch.Unmarshal(result.Data))
				return batch
			}
			require.Nil(t, send(11, nil).CatalogMetadataBarrier)
			unhappyBegin(t, s)
			for _, ack := range []pb.CatalogMetadataAck{
				{Generation: 10, MembershipEpoch: 1, RequiredGeneration: 1, ObservedPhase: pb.CATALOG_METADATA_BARRIER_PREPARING},
				{Generation: 11, MembershipEpoch: 2, RequiredGeneration: 1, ObservedPhase: pb.CATALOG_METADATA_BARRIER_PREPARING},
				{Generation: 11, MembershipEpoch: 1, RequiredGeneration: 2, ObservedPhase: pb.CATALOG_METADATA_BARRIER_PREPARING},
				{Generation: 11, MembershipEpoch: 1, RequiredGeneration: 1, ObservedPhase: pb.CATALOG_METADATA_BARRIER_SEALED},
			} {
				batch := send(11, &ack)
				require.NotNil(t, batch.CatalogMetadataBarrier)
				require.False(t, batch.CatalogMetadataBarrier.MetadataReadsEnabled)
				require.False(t, s.state.CatalogMetadataBarrier.Targets[0].ObservedPreparing)
			}
			ack := &pb.CatalogMetadataAck{Generation: 11, MembershipEpoch: 1, RequiredGeneration: 1, ObservedPhase: pb.CATALOG_METADATA_BARRIER_PREPARING, SealComplete: true}
			send(11, ack)
			require.True(t, s.state.CatalogMetadataBarrier.Targets[0].ObservedPreparing)
			require.False(t, s.state.CatalogMetadataBarrier.Targets[0].SealComplete)
			runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_SEAL), CatalogMetadataApplied)
			ack.ObservedPhase = pb.CATALOG_METADATA_BARRIER_SEALED
			send(11, ack)
			require.False(t, s.state.CatalogMetadataBarrier.Targets[0].SealComplete)
			unhappyReject(t, s, unhappyReceipt(s, pb.CATALOG_ACTION_CATALOG_REQUIRED, 0), CatalogMetadataRejected)
			r := runtimeRequest(s, pb.CATALOG_ACTION_ACK_TARGET)
			r.Target = &pb.CatalogMetadataBarrierTarget{ServiceType: kind, UUID: "participant", Generation: 11, SealComplete: true}
			runtimeApply(t, s, r, CatalogMetadataApplied)
			runtimeApply(t, s, unhappyReceipt(s, pb.CATALOG_ACTION_CATALOG_REQUIRED, 0), CatalogMetadataApplied)
			claim := runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_CLAIM), CatalogMetadataApplied)
			runtimeApply(t, s, unhappyReceipt(s, pb.CATALOG_ACTION_RECOVERY_STARTED, claim), CatalogMetadataApplied)
			runtimeApply(t, s, unhappyReceipt(s, pb.CATALOG_ACTION_COMPLETE, claim), CatalogMetadataApplied)
			s = runtimeRestore(t, s)
			batch := send(11, ack)
			require.True(t, batch.CatalogMetadataBarrier.Admitted)
			require.False(t, batch.CatalogMetadataBarrier.MetadataReadsEnabled)
			batch = send(12, ack)
			require.Equal(t, uint64(12), batch.CatalogMetadataBarrier.RecipientGeneration)
			require.False(t, batch.CatalogMetadataBarrier.Admitted)
			require.False(t, batch.CatalogMetadataBarrier.MetadataReadsEnabled)
		})
	}
}

func TestCatalogUnhappyStartIdentityAndTerminalResults(t *testing.T) {
	s := catalogRuntimeFixture(t)
	r := runtimeRequest(s, pb.CATALOG_ACTION_GRANT_START)
	r.StartPermit = &pb.CatalogMetadataStartPermit{ReplicaID: 1, UUID: "log", StoreIncarnation: "inc"}
	token := runtimeApply(t, s, r, CatalogMetadataApplied)
	for _, tc := range []struct {
		name   string
		mutate func(*pb.CatalogMetadataRequest)
		status uint64
	}{
		{"nil-permit", func(r *pb.CatalogMetadataRequest) { r.StartPermit = nil }, CatalogMetadataRejected},
		{"wrong-token", func(r *pb.CatalogMetadataRequest) { r.Token++ }, CatalogMetadataStale},
		{"wrong-replica", func(r *pb.CatalogMetadataRequest) { r.StartPermit.ReplicaID++ }, CatalogMetadataStale},
		{"wrong-uuid", func(r *pb.CatalogMetadataRequest) { r.StartPermit.UUID = "other" }, CatalogMetadataStale},
		{"wrong-incarnation", func(r *pb.CatalogMetadataRequest) { r.StartPermit.StoreIncarnation = "replacement" }, CatalogMetadataStale},
		{"wrong-role", func(r *pb.CatalogMetadataRequest) { r.StartPermit.NonVoting = true }, CatalogMetadataStale},
		{"no-terminal-result", func(r *pb.CatalogMetadataRequest) { r.StartPermit.Completed = false }, CatalogMetadataRejected},
		{"both-terminal-results", func(r *pb.CatalogMetadataRequest) { r.StartPermit.Revoked = true }, CatalogMetadataRejected},
		{"unsolicited-revoke", func(r *pb.CatalogMetadataRequest) { r.StartPermit.Completed, r.StartPermit.Revoked = false, true }, CatalogMetadataRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := runtimeRequest(s, pb.CATALOG_ACTION_COMPLETE_START)
			r.Token = token
			r.StartPermit = &pb.CatalogMetadataStartPermit{ReplicaID: 1, UUID: "log", StoreIncarnation: "inc", Completed: true}
			tc.mutate(&r)
			unhappyReject(t, s, r, tc.status)
		})
	}
	// 终态 permit 被新 token 替换，固定每 member 一个槽；旧 completion 永久失效。
	r.Action, r.Token, r.StartPermit.Completed = pb.CATALOG_ACTION_COMPLETE_START, token, true
	runtimeApply(t, s, r, CatalogMetadataApplied)
	r.Action = pb.CATALOG_ACTION_GRANT_START
	next := runtimeApply(t, s, r, CatalogMetadataApplied)
	require.Greater(t, next, token)
	require.Len(t, s.state.CatalogMetadataBarrier.Arbitration.StartPermits, 1)
	r.Action = pb.CATALOG_ACTION_COMPLETE_START
	unhappyReject(t, s, r, CatalogMetadataStale)
	s = runtimeRestore(t, s)
	require.False(t, s.state.CatalogMetadataBarrier.Arbitration.StartPermits[0].Completed)
}

func TestCatalogUnhappyReservationSnapshotCorruption(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*pb.CatalogMetadataMembershipOperation)
	}{
		{"missing-voting", func(m *pb.CatalogMetadataMembershipOperation) { m.ExpectedVoting = nil }},
		{"invalid-voting-id", func(m *pb.CatalogMetadataMembershipOperation) { m.ExpectedVoting[0] = "invalid" }},
		{"invalid-observer-uuid", func(m *pb.CatalogMetadataMembershipOperation) { m.ExpectedNonVoting = map[uint64]string{3: ""} }},
		{"overlapping-roles", func(m *pb.CatalogMetadataMembershipOperation) { m.ExpectedNonVoting = map[uint64]string{1: "log"} }},
		{"future-token", func(m *pb.CatalogMetadataMembershipOperation) { m.Token++ }},
		{"zero-config-index", func(m *pb.CatalogMetadataMembershipOperation) { m.ConfigChangeIndex = 0 }},
		{"unknown-action", func(m *pb.CatalogMetadataMembershipOperation) { m.ChangeType = 99 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := catalogRuntimeFixture(t)
			r := runtimeRequest(s, pb.CATALOG_ACTION_RESERVE_MEMBERSHIP)
			r.Membership = &pb.CatalogMetadataMembershipOperation{ConfigChangeIndex: 7, ReplicaID: 2, UUID: "new-log", StoreIncarnation: "inc", ChangeType: pb.AddReplica}
			runtimeApply(t, s, r, CatalogMetadataApplied)
			tc.mutate(s.state.CatalogMetadataBarrier.Arbitration.Reservation)
			payload, err := s.state.Marshal()
			require.NoError(t, err)
			encoded := marshalSnapshotEnvelopeFixture(t, 1, snapshotRequiredFeatures(&s.state), payload)
			live := catalogRuntimeFixture(t)
			before := proto.Clone(&live.state)
			err = live.RecoverFromSnapshot(bytes.NewReader(encoded), nil, nil)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "恢复错误=%v", err)
			require.True(t, proto.Equal(before, &live.state))
		})
	}
}

// 每个持久 start proof 必须仍绑定仲裁 owner 中的同一 member。
func TestCatalogUnhappySnapshotRejectsOrphanStartProof(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*pb.CatalogMetadataStartPermit)
	}{
		{"unadmitted-replica", func(p *pb.CatalogMetadataStartPermit) { p.ReplicaID = 99 }},
		{"wrong-member-uuid", func(p *pb.CatalogMetadataStartPermit) { p.UUID = "other" }},
		{"wrong-member-incarnation", func(p *pb.CatalogMetadataStartPermit) { p.StoreIncarnation = "replacement" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := catalogRuntimeFixture(t)
			r := runtimeRequest(s, pb.CATALOG_ACTION_GRANT_START)
			r.StartPermit = &pb.CatalogMetadataStartPermit{ReplicaID: 1, UUID: "log", StoreIncarnation: "inc"}
			r.Token = runtimeApply(t, s, r, CatalogMetadataApplied)
			r.Action, r.StartPermit.Completed = pb.CATALOG_ACTION_COMPLETE_START, true
			runtimeApply(t, s, r, CatalogMetadataApplied)
			tc.mutate(&s.state.CatalogMetadataBarrier.Arbitration.StartPermits[0])
			payload, err := s.state.Marshal()
			require.NoError(t, err)
			encoded := marshalSnapshotEnvelopeFixture(t, 1, snapshotRequiredFeatures(&s.state), payload)
			live := catalogRuntimeFixture(t)
			before := proto.Clone(&live.state)
			err = live.RecoverFromSnapshot(bytes.NewReader(encoded), nil, nil)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "不应恢复不属于当前 member 的 start proof，恢复错误=%v", err)
			require.True(t, proto.Equal(before, &live.state))
		})
	}
}

func TestCatalogUnhappyMaintenanceDiscardsOnlyLegacyHAKeeperWork(t *testing.T) {
	s := NewStateMachine(0, 1).(*stateMachine)
	s.state.LogState = catalogRuntimeFixture(t).state.LogState
	legacy := unhappyCommand(pb.AddReplica, 2, "new-log")
	dataShard := unhappyCommand(pb.AddReplica, 3, "data-log")
	dataShard.ConfigChange.Replica.ShardID = 99
	other := legacy
	other.UUID = "other-executor"
	unhappyQueue(t, s, legacy, dataShard, other)
	batch := s.state.ScheduleCommands["log"]
	batch.CommandIDs = []pb.ScheduleCommandID{{OriginBatchID: 8, CommandIndex: 0}, {OriginBatchID: 8, CommandIndex: 1}}
	s.state.ScheduleCommands["log"] = batch
	runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ENABLE_MAINTENANCE), CatalogMetadataApplied)
	require.NotContains(t, s.state.ScheduleCommands, "other-executor")
	require.Equal(t, []pb.ScheduleCommand{dataShard}, s.state.ScheduleCommands["log"].Commands)
	require.Equal(t, batch.CommandIDs[1:], s.state.ScheduleCommands["log"].CommandIDs)
	// 第一次 cutover 才清旧队列；重复 enable 不能删掉新协议准入后的工作。
	unhappyQueue(t, s, dataShard, legacy)
	runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ENABLE_MAINTENANCE), CatalogMetadataApplied)
	require.Contains(t, s.state.ScheduleCommands["log"].Commands, legacy)
	s = runtimeRestore(t, s)
	batch = unhappyHeartbeat(t, s, unhappyLogHeartbeat(s, "inc"))
	require.Len(t, batch.Commands, 2)
	require.Nil(t, batch.Commands[0].CatalogMetadataMembership)
	require.NotNil(t, batch.Commands[1].CatalogMetadataMembership)
}

func TestCatalogUnhappyLostStartResponseRetriesDurablePermit(t *testing.T) {
	for _, nonVoting := range []bool{false, true} {
		name := "voting"
		if nonVoting {
			name = "non-voting"
		}
		t.Run(name, func(t *testing.T) {
			s := catalogRuntimeFixture(t)
			start, stop := pb.StartReplica, pb.StopReplica
			if nonVoting {
				shard := s.state.LogState.Shards[DefaultHAKeeperShardID]
				shard.Replicas = map[uint64]string{2: "new-log"}
				shard.NonVotingReplicas = map[uint64]string{1: "log"}
				shard.ReplicaStoreIncarnations[2] = "inc"
				s.state.LogState.Shards[DefaultHAKeeperShardID] = shard
				s.state.CatalogMetadataBarrier.Arbitration.Members[2] = pb.CatalogMetadataReplicaIdentity{UUID: "new-log", StoreIncarnation: "inc"}
				s.state.CatalogMetadataBarrier.Arbitration.Members[1] = pb.CatalogMetadataReplicaIdentity{UUID: "log", StoreIncarnation: "inc", NonVoting: true}
				start, stop = pb.StartNonVotingReplica, pb.StopNonVotingReplica
			}
			hb := unhappyLogHeartbeat(s, "inc")
			hb.Replicas[0].IsNonVoting = nonVoting
			unhappyQueue(t, s, unhappyCommand(start, 1, "log"))
			first := unhappyHeartbeat(t, s, hb)
			require.Len(t, first.Commands, 1)
			permit := first.Commands[0].CatalogMetadataStart
			require.NotNil(t, permit)
			require.NotContains(t, s.state.ScheduleCommands, "log")
			s = runtimeRestore(t, s)
			retried := unhappyHeartbeat(t, s, hb)
			require.Len(t, retried.Commands, 1)
			require.Equal(t, *permit, *retried.Commands[0].CatalogMetadataStart)
			require.Equal(t, start, retried.Commands[0].ConfigChange.ChangeType)
			unhappyQueue(t, s, unhappyCommand(stop, 1, "log"))
			firstStop := unhappyHeartbeat(t, s, hb)
			require.Len(t, firstStop.Commands, 1)
			require.True(t, firstStop.Commands[0].CatalogMetadataStart.Revoked)
			s = runtimeRestore(t, s)
			retried = unhappyHeartbeat(t, s, hb)
			require.Len(t, retried.Commands, 1)
			require.Equal(t, stop, retried.Commands[0].ConfigChange.ChangeType)
			require.Equal(t, permit.Token, retried.Commands[0].CatalogMetadataStart.Token)
			require.True(t, retried.Commands[0].CatalogMetadataStart.Revoked)
			require.Equal(t, permit.Token, s.state.CatalogMetadataBarrier.Arbitration.LastOperationID)
		})
	}
}

func TestCatalogUnhappyStopWithoutExistingPermit(t *testing.T) {
	s := catalogRuntimeFixture(t)
	unhappyQueue(t, s, unhappyCommand(pb.StopReplica, 1, "log"))
	batch := unhappyHeartbeat(t, s, unhappyLogHeartbeat(s, "inc"))
	require.Len(t, batch.Commands, 1)
	permit := batch.Commands[0].CatalogMetadataStart
	require.NotNil(t, permit)
	require.True(t, permit.Revoked)
	require.True(t, s.state.CatalogMetadataBarrier.Arbitration.StartPermits[0].RevocationPending)
	s = runtimeRestore(t, s)
	unhappyReject(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataRejected)
	hb := unhappyLogHeartbeat(s, "inc")
	hb.CatalogMetadataStartResult = permit
	require.Len(t, unhappyHeartbeat(t, s, hb).Commands, 1, "运行中 replica 不能确认已退休")
	hb.Replicas = nil
	require.Empty(t, unhappyHeartbeat(t, s, hb).Commands)
	require.True(t, s.state.CatalogMetadataBarrier.Arbitration.StartPermits[0].Revoked)
}

func TestCatalogUnhappyMembershipAdmissionRejectsInvalidIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*pb.CatalogMetadataMembershipOperation)
	}{
		{"zero-replica", func(m *pb.CatalogMetadataMembershipOperation) { m.ReplicaID = 0 }},
		{"wrong-index", func(m *pb.CatalogMetadataMembershipOperation) { m.ConfigChangeIndex++ }},
		{"existing-replica", func(m *pb.CatalogMetadataMembershipOperation) { m.ReplicaID = 1 }},
		{"existing-uuid", func(m *pb.CatalogMetadataMembershipOperation) { m.UUID = "log" }},
		{"unknown-store", func(m *pb.CatalogMetadataMembershipOperation) { m.UUID = "missing" }},
		{"wrong-incarnation", func(m *pb.CatalogMetadataMembershipOperation) { m.StoreIncarnation = "replacement" }},
		{"remove-last-voter", func(m *pb.CatalogMetadataMembershipOperation) {
			m.ChangeType, m.ReplicaID, m.UUID = pb.RemoveReplica, 1, "log"
		}},
		{"remove-wrong-role", func(m *pb.CatalogMetadataMembershipOperation) {
			m.ChangeType, m.ReplicaID, m.UUID = pb.RemoveNonVotingReplica, 1, "log"
		}},
		{"unknown-action", func(m *pb.CatalogMetadataMembershipOperation) { m.ChangeType = 99 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := catalogRuntimeFixture(t)
			r := runtimeRequest(s, pb.CATALOG_ACTION_RESERVE_MEMBERSHIP)
			r.Membership = &pb.CatalogMetadataMembershipOperation{ConfigChangeIndex: 7, ReplicaID: 2, UUID: "new-log", StoreIncarnation: "inc", ChangeType: pb.AddReplica}
			tc.mutate(r.Membership)
			unhappyReject(t, s, r, CatalogMetadataRejected)
		})
	}
}

func TestCatalogUnhappyScheduleDropsConsumedTokens(t *testing.T) {
	t.Run("membership", func(t *testing.T) {
		s := catalogRuntimeFixture(t)
		unhappyQueue(t, s, unhappyCommand(pb.AddReplica, 2, "new-log"))
		batch := unhappyHeartbeat(t, s, unhappyLogHeartbeat(s, "inc"))
		require.Len(t, batch.Commands, 1)
		delivered := batch.Commands[0]
		r := runtimeRequest(s, pb.CATALOG_ACTION_COMPLETE_MEMBERSHIP)
		r.Token = delivered.CatalogMetadataMembership.Token
		r.ObservedConfigChangeIndex, r.ObservedVoting = 8, map[uint64]string{1: "log", 2: "new-log"}
		runtimeApply(t, s, r, CatalogMetadataApplied)
		unhappyQueue(t, s, delivered)
		s = runtimeRestore(t, s)
		require.Empty(t, unhappyHeartbeat(t, s, unhappyLogHeartbeat(s, "inc")).Commands)
		require.NotContains(t, s.state.ScheduleCommands, "log")
		require.Nil(t, s.state.CatalogMetadataBarrier.Arbitration.Reservation)
	})
	t.Run("start", func(t *testing.T) {
		s := catalogRuntimeFixture(t)
		unhappyQueue(t, s, unhappyCommand(pb.StartReplica, 1, "log"))
		batch := unhappyHeartbeat(t, s, unhappyLogHeartbeat(s, "inc"))
		require.Len(t, batch.Commands, 1)
		delivered := batch.Commands[0]
		unhappyQueue(t, s, delivered)
		retried := unhappyHeartbeat(t, s, unhappyLogHeartbeat(s, "inc"))
		require.Len(t, retried.Commands, 1)
		require.Equal(t, delivered, retried.Commands[0])
		r := runtimeRequest(s, pb.CATALOG_ACTION_COMPLETE_START)
		ack := *delivered.CatalogMetadataStart
		ack.Completed = true
		r.Token, r.StartPermit = ack.Token, &ack
		runtimeApply(t, s, r, CatalogMetadataApplied)
		r.Action = pb.CATALOG_ACTION_GRANT_START
		next := runtimeApply(t, s, r, CatalogMetadataApplied)
		unhappyQueue(t, s, delivered)
		s = runtimeRestore(t, s)
		retried = unhappyHeartbeat(t, s, unhappyLogHeartbeat(s, "inc"))
		require.Len(t, retried.Commands, 1)
		require.Equal(t, next, retried.Commands[0].CatalogMetadataStart.Token)
		require.NotContains(t, s.state.ScheduleCommands, "log")
	})
}
