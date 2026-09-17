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
	"math"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func catalogRuntimeFixture(t *testing.T) *stateMachine {
	t.Helper()
	s := NewStateMachine(0, 1).(*stateMachine)
	s.state.LogState.Shards[DefaultHAKeeperShardID] = pb.LogShardInfo{
		ShardID: DefaultHAKeeperShardID, Epoch: 7,
		Replicas:                 map[uint64]string{1: "log"},
		ReplicaStoreIncarnations: map[uint64]string{1: "inc"},
	}
	s.state.LogState.Stores["log"] = pb.LogStoreInfo{StoreIncarnation: "inc", CatalogMetadataCapabilities: &pb.CatalogMetadataCapabilities{HAKeeperBarrierProtocol: 1}}
	s.state.LogState.Stores["new-log"] = pb.LogStoreInfo{StoreIncarnation: "inc", CatalogMetadataCapabilities: &pb.CatalogMetadataCapabilities{HAKeeperBarrierProtocol: 1}}
	runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ENABLE_MAINTENANCE), CatalogMetadataApplied)
	return s
}

func runtimeRequest(s *stateMachine, action pb.CatalogMetadataAction) pb.CatalogMetadataRequest {
	r := pb.CatalogMetadataRequest{Version: 1, Action: action, Owner: "owner/inc"}
	if b := s.state.CatalogMetadataBarrier; b != nil {
		r.ExpectedPhase, r.MembershipEpoch, r.RequiredGeneration = b.Phase, b.MembershipEpoch, b.RequiredGeneration
		if action == pb.CATALOG_ACTION_ACQUIRE_FENCE && b.Arbitration != nil {
			r.Token = b.Arbitration.LastOperationID
		}
	}
	return r
}

func runtimeApply(t *testing.T, s *stateMachine, r pb.CatalogMetadataRequest, expected uint64) uint64 {
	t.Helper()
	cmd, err := GetCatalogMetadataRequestCmd(r)
	require.NoError(t, err)
	result, err := s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: cmd})
	require.NoError(t, err)
	require.Equal(t, expected, result.Value, "action=%v", r.Action)
	return binaryEnc.Uint64(result.Data)
}

func runtimeRestore(t *testing.T, s *stateMachine) *stateMachine {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, s.SaveSnapshot(&buf, nil, nil))
	next := NewStateMachine(0, 1).(*stateMachine)
	require.NoError(t, next.RecoverFromSnapshot(&buf, nil, nil))
	return next
}

func TestCatalogRuntimeRejectedEntryRetainsFloor(t *testing.T) {
	for _, payload := range [][]byte{nil, {0x80}, {0x08, 0x02}, {0x08, 0x01, 0x10, 0x7f}} {
		s := NewStateMachine(0, 1).(*stateMachine)
		s.state.PersistedExpressionRequiredProtocolVersion = 3
		s.state.PersistedExpressionProtocolActivationPending = true
		cmd := make([]byte, headerSize+len(payload))
		binaryEnc.PutUint32(cmd, uint32(pb.CatalogMetadataBarrierUpdate))
		copy(cmd[headerSize:], payload)
		result, err := s.Update(sm.Entry{Index: 1, Cmd: cmd})
		require.NoError(t, err)
		require.Equal(t, CatalogMetadataRejected, result.Value)
		s = runtimeRestore(t, s)
		require.Equal(t, uint64(1), s.state.CatalogMetadataBarrierRequiredProtocolVersion)
		require.Equal(t, uint32(1), s.state.CatalogMetadataBarrier.RuntimeEvidenceVersion)
		require.Equal(t, pb.CATALOG_METADATA_BARRIER_DISABLED, s.state.CatalogMetadataBarrier.Phase)
		require.False(t, s.state.CatalogMetadataBarrier.EvidenceInitialized)
		require.Equal(t, uint64(3), s.state.PersistedExpressionRequiredProtocolVersion)
		require.True(t, s.state.PersistedExpressionProtocolActivationPending)
	}
}

func TestCatalogRuntimeFenceRecoveryAndDelayedOwner(t *testing.T) {
	s := catalogRuntimeFixture(t)
	r := runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE)
	token := runtimeApply(t, s, r, CatalogMetadataApplied)
	require.Equal(t, token, runtimeApply(t, s, r, CatalogMetadataApplied))
	s = runtimeRestore(t, s) // proposer died after committed acquire
	r = runtimeRequest(s, pb.CATALOG_ACTION_TAKEOVER_FENCE)
	r.Token, r.NewOwner = token, "successor/inc"
	nextToken := runtimeApply(t, s, r, CatalogMetadataApplied)
	require.Greater(t, nextToken, token)
	runtimeApply(t, s, r, CatalogMetadataStale)
	r = runtimeRequest(s, pb.CATALOG_ACTION_RELEASE_FENCE)
	r.Token = token
	runtimeApply(t, s, r, CatalogMetadataStale)
	r.Token, r.Owner = nextToken, "successor/inc"
	runtimeApply(t, s, r, CatalogMetadataApplied)
	r.Action = pb.CATALOG_ACTION_BEGIN
	r.RequiredViewDependencyProtocol, r.RequiredRecoveryProtocol = 1, 1
	runtimeApply(t, s, r, CatalogMetadataStale)
	require.Nil(t, s.state.CatalogMetadataBarrier.Arbitration.Fence)
	// A lookup cannot let a caller mutate durable arbitration state.
	value, err := s.Lookup(&CatalogMetadataStateQuery{})
	require.NoError(t, err)
	value.(*pb.CatalogMetadataBarrierState).Arbitration.LastOperationID = math.MaxUint64
	require.Equal(t, nextToken, s.state.CatalogMetadataBarrier.Arbitration.LastOperationID)
	s.state.CatalogMetadataBarrier.Arbitration.LastOperationID = math.MaxUint64
	runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataRejected)
}

func TestCatalogRuntimeMembershipUnknownResultAndReconciliation(t *testing.T) {
	for _, change := range []pb.ConfigChangeType{pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica} {
		t.Run(change.String(), func(t *testing.T) {
			s := catalogRuntimeFixture(t)
			if change == pb.RemoveReplica || change == pb.RemoveNonVotingReplica {
				s.state.CatalogMetadataBarrier.Arbitration.Members[2] = pb.CatalogMetadataReplicaIdentity{UUID: "new-log", StoreIncarnation: "inc"}
				shard := s.state.LogState.Shards[DefaultHAKeeperShardID]
				shard.ReplicaStoreIncarnations[2] = "inc"
				if change == pb.RemoveReplica {
					shard.Replicas[2] = "new-log"
				} else {
					shard.NonVotingReplicas = map[uint64]string{2: "new-log"}
				}
				s.state.LogState.Shards[DefaultHAKeeperShardID] = shard
			}
			r := runtimeRequest(s, pb.CATALOG_ACTION_RESERVE_MEMBERSHIP)
			r.Membership = &pb.CatalogMetadataMembershipOperation{ConfigChangeIndex: 7, ReplicaID: 2, UUID: "new-log", StoreIncarnation: "inc", ChangeType: change}
			token := runtimeApply(t, s, r, CatalogMetadataApplied)
			runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataRejected)
			s = runtimeRestore(t, s)
			r = runtimeRequest(s, pb.CATALOG_ACTION_COMPLETE_MEMBERSHIP)
			r.Token = token
			runtimeApply(t, s, r, CatalogMetadataRejected) // unknown result, no TTL cleanup
			r.ObservedConfigChangeIndex = 7
			runtimeApply(t, s, r, CatalogMetadataRejected)
			r.ObservedConfigChangeIndex = 8
			r.ObservedVoting = map[uint64]string{2: "wrong"}
			runtimeApply(t, s, r, CatalogMetadataConflict)
			r.ObservedVoting = map[uint64]string{1: "log"}
			if change == pb.AddReplica {
				r.ObservedVoting[2] = "new-log"
			} else if change == pb.AddNonVotingReplica {
				r.ObservedNonVoting = map[uint64]string{2: "new-log"}
			}
			runtimeApply(t, s, r, CatalogMetadataApplied)
			runtimeApply(t, s, r, CatalogMetadataStale)
			if change == pb.RemoveReplica || change == pb.RemoveNonVotingReplica {
				runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataRejected)
				retire := s.state.CatalogMetadataBarrier.Arbitration.StartPermits[0]
				retire.Revoked, retire.RevocationPending = true, false
				done := runtimeRequest(s, pb.CATALOG_ACTION_COMPLETE_START)
				done.Token, done.StartPermit = retire.Token, &retire
				runtimeApply(t, s, done, CatalogMetadataApplied)
				require.Empty(t, s.state.CatalogMetadataBarrier.Arbitration.StartPermits)
			}
			runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataApplied)
		})
	}
}

func TestCatalogRuntimeStartCompletesWithoutMembershipIndexChange(t *testing.T) {
	for _, revoked := range []bool{false, true} {
		s := catalogRuntimeFixture(t)
		r := runtimeRequest(s, pb.CATALOG_ACTION_GRANT_START)
		r.StartPermit = &pb.CatalogMetadataStartPermit{ReplicaID: 1, UUID: "log", StoreIncarnation: "inc"}
		token := runtimeApply(t, s, r, CatalogMetadataApplied)
		runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataRejected)
		s = runtimeRestore(t, s)
		if revoked {
			r.Action, r.Token = pb.CATALOG_ACTION_REVOKE_START, token
			runtimeApply(t, s, r, CatalogMetadataApplied)
		}
		r.Action, r.Token = pb.CATALOG_ACTION_COMPLETE_START, token
		r.StartPermit.Completed, r.StartPermit.Revoked = !revoked, revoked
		runtimeApply(t, s, r, CatalogMetadataApplied)
		runtimeApply(t, s, r, CatalogMetadataApplied) // lost response
		require.Equal(t, uint64(7), s.state.LogState.Shards[DefaultHAKeeperShardID].Epoch)
		r.Action = pb.CATALOG_ACTION_RESERVE_MEMBERSHIP
		r.Membership = &pb.CatalogMetadataMembershipOperation{ConfigChangeIndex: 7, ReplicaID: 2, UUID: "new-log", StoreIncarnation: "inc", ChangeType: pb.AddReplica}
		runtimeApply(t, s, r, CatalogMetadataApplied)
	}
}

func TestCatalogRuntimeLifecycleAndGenerationFencing(t *testing.T) {
	s := catalogRuntimeFixture(t)
	s.state.PersistedExpressionRequiredProtocolVersion = 3
	s.state.PersistedExpressionProtocolActivationPending = true
	s.state.CNState.Stores["cn"] = pb.CNStoreInfo{ViewMetadataAdmissionGeneration: 11, CatalogMetadataCapabilities: &pb.CatalogMetadataCapabilities{ViewDependencyProtocol: 1, RecoveryProtocol: 1, BarrierParticipantProtocol: 1}}
	token := runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataApplied)
	r := runtimeRequest(s, pb.CATALOG_ACTION_BEGIN)
	r.Token, r.RequiredViewDependencyProtocol, r.RequiredRecoveryProtocol = token, 1, 1
	runtimeApply(t, s, r, CatalogMetadataApplied)
	runtimeApply(t, s, r, CatalogMetadataStale)
	s = runtimeRestore(t, s)
	runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_SEAL), CatalogMetadataRejected)
	r = runtimeRequest(s, pb.CATALOG_ACTION_ACK_TARGET)
	r.Target = &pb.CatalogMetadataBarrierTarget{ServiceType: pb.CNService, UUID: "cn", Generation: 10, ObservedPreparing: true}
	runtimeApply(t, s, r, CatalogMetadataStale)
	r.Target.Generation = 11
	runtimeApply(t, s, r, CatalogMetadataApplied)
	r.Target.SealComplete = true
	runtimeApply(t, s, r, CatalogMetadataRejected)
	runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_SEAL), CatalogMetadataApplied)
	r = runtimeRequest(s, pb.CATALOG_ACTION_ACK_TARGET)
	r.Target = &pb.CatalogMetadataBarrierTarget{ServiceType: pb.CNService, UUID: "cn", Generation: 11, ObservedPreparing: true, SealComplete: true}
	runtimeApply(t, s, r, CatalogMetadataApplied)
	var claim uint64
	for _, action := range []pb.CatalogMetadataAction{pb.CATALOG_ACTION_CATALOG_REQUIRED, pb.CATALOG_ACTION_RECOVERY_STARTED, pb.CATALOG_ACTION_COMPLETE} {
		if action == pb.CATALOG_ACTION_RECOVERY_STARTED {
			claimReq := runtimeRequest(s, pb.CATALOG_ACTION_CLAIM)
			claim = runtimeApply(t, s, claimReq, CatalogMetadataApplied)
			runtimeApply(t, s, claimReq, CatalogMetadataStale)
		}
		r = runtimeRequest(s, action)
		r.Receipt = &pb.CatalogMetadataReceipt{MembershipEpoch: r.MembershipEpoch, RequiredGeneration: r.RequiredGeneration, ClaimID: claim, Action: action, Digest: bytes.Repeat([]byte{byte(action)}, 32)}
		runtimeApply(t, s, r, CatalogMetadataApplied)
		runtimeApply(t, s, r, CatalogMetadataApplied)
		r.Receipt.Digest[0]++
		runtimeApply(t, s, r, CatalogMetadataConflict)
		s = runtimeRestore(t, s)
	}
	require.Equal(t, pb.CATALOG_METADATA_BARRIER_ACTIVATED, s.state.CatalogMetadataBarrier.Phase)
	require.Equal(t, uint64(1), s.state.CatalogMetadataBarrier.CompletedGeneration)
	retired := runtimeRestore(t, s)
	retire := runtimeRequest(retired, pb.CATALOG_ACTION_RETIRE_TARGET)
	retire.Target = &pb.CatalogMetadataBarrierTarget{ServiceType: pb.CNService, UUID: "cn", Generation: 11, AuthorityRetirementDigest: bytes.Repeat([]byte{7}, 32)}
	runtimeApply(t, retired, retire, CatalogMetadataApplied)
	var response pb.CommandBatch
	require.NoError(t, response.Unmarshal(retired.attachCatalogMetadataBarrier(sm.Result{}, "cn", false).Data))
	require.False(t, response.CatalogMetadataBarrier.Admitted, "a retired issuer cannot reacquire authority in ACTIVATED")
	retireToken := runtimeApply(t, retired, runtimeRequest(retired, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataApplied)
	retireNext := runtimeRequest(retired, pb.CATALOG_ACTION_SUPERSEDE)
	retireNext.Token, retireNext.RequiredViewDependencyProtocol, retireNext.RequiredRecoveryProtocol = retireToken, 1, 1
	runtimeApply(t, retired, retireNext, CatalogMetadataApplied)
	require.Equal(t, retire.Target.AuthorityRetirementDigest, runtimeRestore(t, retired).state.CatalogMetadataBarrier.Targets[0].AuthorityRetirementDigest)
	// The pre-activation seal does not retire fresh authority issued after
	// ACTIVATE. Deleting its heartbeat record must not discard that owner.
	delete(s.state.CNState.Stores, "cn")
	token = runtimeApply(t, s, runtimeRequest(s, pb.CATALOG_ACTION_ACQUIRE_FENCE), CatalogMetadataApplied)
	supersede := runtimeRequest(s, pb.CATALOG_ACTION_SUPERSEDE)
	supersede.Token, supersede.RequiredViewDependencyProtocol, supersede.RequiredRecoveryProtocol = token, 1, 1
	runtimeApply(t, s, supersede, CatalogMetadataApplied)
	runtimeApply(t, s, r, CatalogMetadataStale)
	require.Equal(t, uint64(2), s.state.CatalogMetadataBarrier.RequiredGeneration)
	require.Equal(t, uint64(1), s.state.CatalogMetadataBarrier.CompletedGeneration)
	require.Nil(t, s.state.CatalogMetadataBarrier.Arbitration.CompletedReceipt)
	require.Len(t, s.state.CatalogMetadataBarrier.Targets, 1)
	require.Equal(t, uint64(11), s.state.CatalogMetadataBarrier.Targets[0].Generation)
	require.False(t, s.state.CatalogMetadataBarrier.Targets[0].SealComplete)
	require.Equal(t, uint64(3), s.state.PersistedExpressionRequiredProtocolVersion)
	require.True(t, s.state.PersistedExpressionProtocolActivationPending)
}
