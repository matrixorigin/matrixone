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
	"math"
	"sort"

	sm "github.com/lni/dragonboat/v4/statemachine"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// These results distinguish a durable business rejection from a Raft failure.
// The decoder floor is retained even when the business request is rejected.
const (
	CatalogMetadataRejected uint64 = iota
	CatalogMetadataApplied
	CatalogMetadataStale
	CatalogMetadataConflict
)

// CatalogMetadataStateQuery is a linearizable control-plane read. The returned
// protobuf is an independent copy; callers cannot mutate the replicated owner.
type CatalogMetadataStateQuery struct{}

// GetCatalogMetadataRequestCmd is an internal control-plane encoder, not a SQL
// activation API. A proposer must establish the maintenance/membership gate
// BEFORE submitting the first entry, including entries that may be rejected.
func GetCatalogMetadataRequestCmd(req pb.CatalogMetadataRequest) ([]byte, error) {
	payload, err := req.Marshal()
	if err != nil {
		return nil, err
	}
	cmd := make([]byte, headerSize+len(payload))
	binaryEnc.PutUint32(cmd, uint32(pb.CatalogMetadataBarrierUpdate))
	copy(cmd[headerSize:], payload)
	return cmd, nil
}

func catalogMetadataResult(status, token uint64) sm.Result {
	data := make([]byte, 8)
	binaryEnc.PutUint64(data, token)
	return sm.Result{Value: status, Data: data}
}

func (s *stateMachine) handleCatalogMetadataRequest(cmd []byte) sm.Result {
	// Recognition of the tag, not successful payload parsing, creates the
	// replay obligation. Never copy an untrusted requested floor into state.
	if s.state.CatalogMetadataBarrierRequiredProtocolVersion > catalogMetadataBarrierProtocolVersion {
		return catalogMetadataResult(CatalogMetadataRejected, 0)
	}
	s.state.CatalogMetadataBarrierRequiredProtocolVersion = catalogMetadataBarrierProtocolVersion
	if s.state.CatalogMetadataBarrier == nil {
		s.state.CatalogMetadataBarrier = &pb.CatalogMetadataBarrierState{}
	}
	current := s.state.CatalogMetadataBarrier
	if current.RuntimeEvidenceVersion == 0 {
		current.RuntimeEvidenceVersion = catalogMetadataEvidenceVersion
	}
	var req pb.CatalogMetadataRequest
	if len(cmd) <= headerSize || req.Unmarshal(cmd[headerSize:]) != nil || req.Version != 1 {
		return catalogMetadataResult(CatalogMetadataRejected, 0)
	}
	// Stage mutations on a deep copy; rejected requests preserve business
	// state, including maps/slices, while retaining the floor above.
	payload, err := current.Marshal()
	if err != nil {
		panic(err)
	}
	var next pb.CatalogMetadataBarrierState
	if err := next.Unmarshal(payload); err != nil {
		panic(err)
	}
	if next.Arbitration == nil {
		next.Arbitration = &pb.CatalogMetadataArbitration{}
	}
	status, token := s.applyCatalogMetadataRequest(&next, &req)
	if status != CatalogMetadataApplied {
		return catalogMetadataResult(status, token)
	}
	probe := s.state
	probe.CatalogMetadataBarrier = &next
	if validateCatalogMetadataBarrier(&next) != nil || validateCatalogMetadataEvidence(&probe) != nil {
		return catalogMetadataResult(CatalogMetadataRejected, 0)
	}
	cutover := req.Action == pb.CATALOG_ACTION_ENABLE_MAINTENANCE && (current.Arbitration == nil || !current.Arbitration.MaintenanceEnabled)
	s.state.CatalogMetadataBarrier = &next
	if cutover {
		s.discardLegacyCatalogSchedule()
	}
	return catalogMetadataResult(status, token)
}

func catalogMetadataTuple(b *pb.CatalogMetadataBarrierState, r *pb.CatalogMetadataRequest) bool {
	return b.Phase == r.ExpectedPhase && b.MembershipEpoch == r.MembershipEpoch && b.RequiredGeneration == r.RequiredGeneration
}

func catalogMetadataNextID(a *pb.CatalogMetadataArbitration) uint64 {
	if a.LastOperationID == math.MaxUint64 {
		return 0
	}
	a.LastOperationID++
	return a.LastOperationID
}

func catalogMetadataPendingStart(a *pb.CatalogMetadataArbitration) bool {
	for _, p := range a.StartPermits {
		if !p.Completed && !p.Revoked {
			return true
		}
	}
	return false
}

func (s *stateMachine) catalogMetadataReplicasReady() bool {
	shard, ok := s.state.LogState.Shards[DefaultHAKeeperShardID]
	if !ok || len(shard.Replicas) == 0 {
		return false
	}
	var admitted map[uint64]pb.CatalogMetadataReplicaIdentity
	if b := s.state.CatalogMetadataBarrier; b != nil && b.Arbitration != nil && b.Arbitration.MaintenanceEnabled {
		if shard.Epoch < b.Arbitration.ConfirmedConfigChangeIndex {
			return false
		}
		admitted = b.Arbitration.Members
		if len(admitted) != len(shard.Replicas)+len(shard.NonVotingReplicas) {
			return false
		}
	}
	for _, members := range []map[uint64]string{shard.Replicas, shard.NonVotingReplicas} {
		for id, uuid := range members {
			store, ok := s.state.LogState.Stores[uuid]
			if admitted != nil {
				identity, exists := admitted[id]
				if !exists || identity.Revoked || identity.UUID != uuid || identity.StoreIncarnation != store.StoreIncarnation {
					return false
				}
			}
			if !ok || store.StoreIncarnation == "" || shard.ReplicaStoreIncarnations[id] != store.StoreIncarnation ||
				store.CatalogMetadataCapabilities == nil || store.CatalogMetadataCapabilities.HAKeeperBarrierProtocol < 1 {
				return false
			}
		}
	}
	return true
}

func (s *stateMachine) applyCatalogMetadataRequest(b *pb.CatalogMetadataBarrierState, r *pb.CatalogMetadataRequest) (uint64, uint64) {
	a := b.Arbitration
	if r.Action == pb.CATALOG_ACTION_ENABLE_MAINTENANCE {
		if !catalogMetadataTuple(b, r) || !s.catalogMetadataReplicasReady() {
			return CatalogMetadataRejected, 0
		}
		// This entry can only be proposed by the trusted maintenance owner after
		// legacy executors have stopped. It is intentionally not auto-proposed.
		if !a.MaintenanceEnabled {
			a.Members = make(map[uint64]pb.CatalogMetadataReplicaIdentity)
			shard := s.state.LogState.Shards[DefaultHAKeeperShardID]
			for _, members := range []map[uint64]string{shard.Replicas, shard.NonVotingReplicas} {
				for id, uuid := range members {
					a.Members[id] = pb.CatalogMetadataReplicaIdentity{UUID: uuid, StoreIncarnation: s.state.LogState.Stores[uuid].StoreIncarnation, NonVoting: shard.NonVotingReplicas[id] == uuid}
				}
			}
		}
		a.MaintenanceEnabled = true
		return CatalogMetadataApplied, 0
	}
	if !a.MaintenanceEnabled {
		return CatalogMetadataRejected, 0
	}
	switch r.Action {
	case pb.CATALOG_ACTION_ACQUIRE_FENCE:
		if !catalogMetadataTuple(b, r) || r.Owner == "" || a.Reservation != nil || catalogMetadataPendingStart(a) {
			return CatalogMetadataRejected, 0
		}
		if a.Fence != nil {
			if r.Token != math.MaxUint64 && a.Fence.Token == r.Token+1 && a.Fence.Owner == r.Owner {
				return CatalogMetadataApplied, a.Fence.Token
			}
			return CatalogMetadataRejected, 0
		}
		// Acquire is a CAS on the allocator watermark. A delayed acquire after
		// release must not resurrect a fence for an old owner.
		if r.Token != a.LastOperationID {
			return CatalogMetadataStale, 0
		}
		token := catalogMetadataNextID(a)
		if token == 0 {
			return CatalogMetadataRejected, 0
		}
		a.Fence = &pb.CatalogMetadataFence{Token: token, Owner: r.Owner, ExpectedPhase: b.Phase, MembershipEpoch: b.MembershipEpoch, RequiredGeneration: b.RequiredGeneration}
		return CatalogMetadataApplied, token
	case pb.CATALOG_ACTION_TAKEOVER_FENCE, pb.CATALOG_ACTION_RELEASE_FENCE:
		if a.Fence == nil || a.Fence.Token != r.Token || a.Fence.Owner != r.Owner {
			return CatalogMetadataStale, 0
		}
		if r.Action == pb.CATALOG_ACTION_RELEASE_FENCE {
			a.Fence = nil
			return CatalogMetadataApplied, r.Token
		}
		if r.NewOwner == "" || r.NewOwner == r.Owner {
			return CatalogMetadataRejected, 0
		}
		token := catalogMetadataNextID(a)
		if token == 0 {
			return CatalogMetadataRejected, 0
		}
		a.Fence.Token, a.Fence.Owner = token, r.NewOwner
		return CatalogMetadataApplied, token
	case pb.CATALOG_ACTION_RESERVE_MEMBERSHIP:
		return s.reserveCatalogMembership(a, r)
	case pb.CATALOG_ACTION_COMPLETE_MEMBERSHIP:
		return completeCatalogMembership(a, r)
	case pb.CATALOG_ACTION_GRANT_START, pb.CATALOG_ACTION_COMPLETE_START, pb.CATALOG_ACTION_REVOKE_START:
		return s.applyCatalogStart(a, r)
	case pb.CATALOG_ACTION_BEGIN, pb.CATALOG_ACTION_SUPERSEDE:
		if !catalogMetadataTuple(b, r) || a.Fence == nil || a.Fence.Token != r.Token || a.Fence.Owner != r.Owner {
			return CatalogMetadataStale, 0
		}
		if (r.Action == pb.CATALOG_ACTION_SUPERSEDE && !b.EvidenceInitialized) ||
			(r.Action == pb.CATALOG_ACTION_BEGIN) != (b.Phase == pb.CATALOG_METADATA_BARRIER_DISABLED) ||
			r.RequiredViewDependencyProtocol == 0 || r.RequiredRecoveryProtocol == 0 ||
			r.RequiredViewDependencyProtocol < b.RequiredViewDependencyProtocol || r.RequiredRecoveryProtocol < b.RequiredRecoveryProtocol ||
			b.MembershipEpoch == math.MaxUint64 || b.RequiredGeneration == math.MaxUint64 || !s.catalogMetadataReplicasReady() {
			return CatalogMetadataRejected, 0
		}
		targets, ok := s.captureCatalogTargets(r.RequiredViewDependencyProtocol, r.RequiredRecoveryProtocol)
		if !ok {
			return CatalogMetadataRejected, 0
		}
		// Capture new participants without dropping unretired owners. A
		// same-UUID replacement waits behind its old owner, not a growing
		// history of generations. ACTIVATE may have issued fresh authority:
		// the previous pre-activation seal is not its retirement proof.
		targets = recaptureCatalogTargets(b, targets)
		b.MembershipEpoch++
		b.RequiredGeneration++
		b.Phase = pb.CATALOG_METADATA_BARRIER_PREPARING
		b.RequiredViewDependencyProtocol, b.RequiredRecoveryProtocol = r.RequiredViewDependencyProtocol, r.RequiredRecoveryProtocol
		b.EvidenceInitialized, b.Targets = true, targets
		a.LastConsumedFence, a.Fence = r.Token, nil
		a.ClaimID, a.RequiredReceipt, a.StartedReceipt, a.CompletedReceipt = 0, nil, nil, nil
		return CatalogMetadataApplied, r.Token
	}
	if !b.EvidenceInitialized || b.MembershipEpoch != r.MembershipEpoch || b.RequiredGeneration != r.RequiredGeneration {
		return CatalogMetadataStale, 0
	}
	if r.Action == pb.CATALOG_ACTION_CATALOG_REQUIRED || r.Action == pb.CATALOG_ACTION_RECOVERY_STARTED || r.Action == pb.CATALOG_ACTION_COMPLETE {
		return s.applyCatalogReceipt(b, r)
	}
	if !catalogMetadataTuple(b, r) {
		return CatalogMetadataStale, 0
	}
	switch r.Action {
	case pb.CATALOG_ACTION_RETIRE_TARGET:
		if r.Target == nil || len(r.Target.AuthorityRetirementDigest) != 32 {
			return CatalogMetadataRejected, 0
		}
		for i := range b.Targets {
			t := &b.Targets[i]
			if t.ServiceType == r.Target.ServiceType && t.UUID == r.Target.UUID && t.Generation == r.Target.Generation {
				if len(t.AuthorityRetirementDigest) != 0 && !bytes.Equal(t.AuthorityRetirementDigest, r.Target.AuthorityRetirementDigest) {
					return CatalogMetadataConflict, 0
				}
				// Only the trusted authority owner / deployment retirement
				// coordinator submits this proof, never a generic heartbeat.
				t.AuthorityRetirementDigest = append([]byte(nil), r.Target.AuthorityRetirementDigest...)
				t.ObservedPreparing, t.SealComplete = true, true
				return CatalogMetadataApplied, 0
			}
		}
		return CatalogMetadataStale, 0
	case pb.CATALOG_ACTION_ACK_TARGET:
		if r.Target == nil {
			return CatalogMetadataRejected, 0
		}
		for i := range b.Targets {
			t := &b.Targets[i]
			if t.ServiceType == r.Target.ServiceType && t.UUID == r.Target.UUID && t.Generation == r.Target.Generation {
				if r.Target.SealComplete && (b.Phase != pb.CATALOG_METADATA_BARRIER_SEALED || !t.ObservedPreparing) {
					return CatalogMetadataRejected, 0
				}
				t.ObservedPreparing = t.ObservedPreparing || r.Target.ObservedPreparing
				t.SealComplete = t.SealComplete || r.Target.SealComplete
				return CatalogMetadataApplied, 0
			}
		}
		return CatalogMetadataStale, 0
	case pb.CATALOG_ACTION_SEAL:
		if b.Phase != pb.CATALOG_METADATA_BARRIER_PREPARING || !s.catalogMetadataReplicasReady() || !s.catalogMetadataTargetsCurrent(b) {
			return CatalogMetadataRejected, 0
		}
		for _, target := range b.Targets {
			if !target.ObservedPreparing {
				return CatalogMetadataRejected, 0
			}
		}
		b.Phase = pb.CATALOG_METADATA_BARRIER_SEALED
		return CatalogMetadataApplied, 0
	case pb.CATALOG_ACTION_CLAIM:
		if b.Phase != pb.CATALOG_METADATA_BARRIER_CATALOG_REQUIRED {
			return CatalogMetadataRejected, 0
		}
		if r.Token != a.ClaimID {
			return CatalogMetadataStale, a.ClaimID
		}
		token := catalogMetadataNextID(a)
		if token == 0 {
			return CatalogMetadataRejected, 0
		}
		a.ClaimID = token
		return CatalogMetadataApplied, token
	default:
		return CatalogMetadataRejected, 0
	}
}

func (s *stateMachine) reserveCatalogMembership(a *pb.CatalogMetadataArbitration, r *pb.CatalogMetadataRequest) (uint64, uint64) {
	m := r.Membership
	if a.Fence != nil || a.Reservation != nil || catalogMetadataPendingStart(a) || m == nil || m.ReplicaID == 0 || m.UUID == "" || m.StoreIncarnation == "" || m.ConfigChangeIndex == 0 {
		return CatalogMetadataRejected, 0
	}
	shard, exists := s.state.LogState.Shards[DefaultHAKeeperShardID]
	if !exists || shard.Epoch != m.ConfigChangeIndex || m.ConfigChangeIndex < a.ConfirmedConfigChangeIndex || len(shard.Replicas) == 0 {
		return CatalogMetadataRejected, 0
	}
	switch m.ChangeType {
	case pb.AddReplica, pb.AddNonVotingReplica:
		if _, exists := a.Members[m.ReplicaID]; exists {
			return CatalogMetadataRejected, 0
		}
		for _, member := range a.Members {
			if member.UUID == m.UUID {
				return CatalogMetadataRejected, 0
			}
		}
		info, ok := s.state.LogState.Stores[m.UUID]
		for _, replica := range info.Replicas {
			if replica.ShardID == DefaultHAKeeperShardID {
				return CatalogMetadataRejected, 0
			}
		}
		if !ok || info.StoreIncarnation != m.StoreIncarnation || info.CatalogMetadataCapabilities == nil || info.CatalogMetadataCapabilities.HAKeeperBarrierProtocol < 1 {
			return CatalogMetadataRejected, 0
		}
	case pb.RemoveReplica, pb.RemoveNonVotingReplica:
		shard := s.state.LogState.Shards[DefaultHAKeeperShardID]
		if m.ChangeType == pb.RemoveReplica && (len(shard.Replicas) <= 1 || shard.Replicas[m.ReplicaID] != m.UUID) {
			return CatalogMetadataRejected, 0
		}
		if m.ChangeType == pb.RemoveNonVotingReplica && shard.NonVotingReplicas[m.ReplicaID] != m.UUID {
			return CatalogMetadataRejected, 0
		}
		member, exists := a.Members[m.ReplicaID]
		if !exists || member.UUID != m.UUID || member.StoreIncarnation != m.StoreIncarnation {
			return CatalogMetadataRejected, 0
		}
	default:
		return CatalogMetadataRejected, 0
	}
	token := catalogMetadataNextID(a)
	if token == 0 {
		return CatalogMetadataRejected, 0
	}
	copy := *m
	copy.Token = token
	copy.ExpectedVoting = maps.Clone(shard.Replicas)
	copy.ExpectedNonVoting = maps.Clone(shard.NonVotingReplicas)
	if copy.ExpectedNonVoting == nil {
		copy.ExpectedNonVoting = make(map[uint64]string)
	}
	switch m.ChangeType {
	case pb.AddReplica:
		copy.ExpectedVoting[m.ReplicaID] = m.UUID
	case pb.AddNonVotingReplica:
		copy.ExpectedNonVoting[m.ReplicaID] = m.UUID
	case pb.RemoveReplica:
		delete(copy.ExpectedVoting, m.ReplicaID)
	case pb.RemoveNonVotingReplica:
		delete(copy.ExpectedNonVoting, m.ReplicaID)
	}
	a.Reservation = &copy
	return CatalogMetadataApplied, token
}

func completeCatalogMembership(a *pb.CatalogMetadataArbitration, r *pb.CatalogMetadataRequest) (uint64, uint64) {
	m := a.Reservation
	if m == nil || m.Token != r.Token {
		return CatalogMetadataStale, 0
	}
	if r.ObservedConfigChangeIndex <= m.ConfigChangeIndex {
		return CatalogMetadataRejected, 0
	}
	v, voting := r.ObservedVoting[m.ReplicaID]
	n, nonVoting := r.ObservedNonVoting[m.ReplicaID]
	matches := false
	switch m.ChangeType {
	case pb.AddReplica:
		matches = voting && v == m.UUID && !nonVoting
	case pb.AddNonVotingReplica:
		matches = nonVoting && n == m.UUID && !voting
	case pb.RemoveReplica, pb.RemoveNonVotingReplica:
		matches = !voting && !nonVoting
	}
	if !matches || !maps.Equal(m.ExpectedVoting, r.ObservedVoting) || !maps.Equal(m.ExpectedNonVoting, r.ObservedNonVoting) {
		return CatalogMetadataConflict, 0
	}
	if m.ChangeType == pb.AddReplica || m.ChangeType == pb.AddNonVotingReplica {
		if a.Members == nil {
			a.Members = make(map[uint64]pb.CatalogMetadataReplicaIdentity)
		}
		a.Members[m.ReplicaID] = pb.CatalogMetadataReplicaIdentity{UUID: m.UUID, StoreIncarnation: m.StoreIncarnation, NonVoting: m.ChangeType == pb.AddNonVotingReplica}
	} else {
		delete(a.Members, m.ReplicaID)
	}
	if m.ChangeType == pb.RemoveReplica || m.ChangeType == pb.RemoveNonVotingReplica {
		// Membership removal fences Raft, but does not stop the local executor.
		// Transfer the consumed reservation token into a bounded retirement
		// permit; do not lose cancellation delivery when the queue is empty.
		retire := pb.CatalogMetadataStartPermit{Token: m.Token, ReplicaID: m.ReplicaID, UUID: m.UUID,
			StoreIncarnation: m.StoreIncarnation, NonVoting: m.ChangeType == pb.RemoveNonVotingReplica, RevocationPending: true}
		replaced := false
		for i := range a.StartPermits {
			if a.StartPermits[i].UUID == m.UUID {
				a.StartPermits[i] = retire
				replaced = true
				break
			}
		}
		if !replaced {
			a.StartPermits = append(a.StartPermits, retire)
		}
		sort.Slice(a.StartPermits, func(i, j int) bool { return a.StartPermits[i].UUID < a.StartPermits[j].UUID })
	}
	// Completion and its configuration high-water must commit together.
	// Heartbeats can lag this proof; they cannot authorize the next mutation
	// against the older configuration, even after snapshot recovery.
	a.ConfirmedConfigChangeIndex = r.ObservedConfigChangeIndex
	a.Reservation = nil
	return CatalogMetadataApplied, r.Token
}

func (s *stateMachine) applyCatalogStart(a *pb.CatalogMetadataArbitration, r *pb.CatalogMetadataRequest) (uint64, uint64) {
	p := r.StartPermit
	if p == nil {
		return CatalogMetadataRejected, 0
	}
	if r.Action == pb.CATALOG_ACTION_REVOKE_START {
		if a.Fence != nil || a.Reservation != nil {
			return CatalogMetadataRejected, 0
		}
		if r.Token == 0 {
			// A pre-cutover zombie has no start permit. Authorize stop only
			// after both admitted and observed memberships exclude its ID.
			shard := s.state.LogState.Shards[DefaultHAKeeperShardID]
			_, admitted := a.Members[p.ReplicaID]
			if admitted || p.ReplicaID == 0 || p.UUID == "" || p.StoreIncarnation == "" ||
				shard.Replicas[p.ReplicaID] != "" || shard.NonVotingReplicas[p.ReplicaID] != "" ||
				s.state.LogState.Stores[p.UUID].StoreIncarnation != p.StoreIncarnation || catalogMetadataPendingStart(a) {
				return CatalogMetadataRejected, 0
			}
			for _, old := range a.StartPermits {
				if old.UUID == p.UUID {
					return CatalogMetadataRejected, 0
				}
			}
			token := catalogMetadataNextID(a)
			if token == 0 {
				return CatalogMetadataRejected, 0
			}
			retire := *p
			retire.Token, retire.Completed, retire.Revoked, retire.RevocationPending = token, false, false, true
			a.StartPermits = append(a.StartPermits, retire)
			sort.Slice(a.StartPermits, func(i, j int) bool { return a.StartPermits[i].UUID < a.StartPermits[j].UUID })
			return CatalogMetadataApplied, token
		}
		for i := range a.StartPermits {
			old := &a.StartPermits[i]
			if old.Token == r.Token && old.UUID == p.UUID && old.ReplicaID == p.ReplicaID && old.StoreIncarnation == p.StoreIncarnation && old.NonVoting == p.NonVoting {
				if !old.Revoked {
					old.Completed, old.RevocationPending = false, true
				}
				return CatalogMetadataApplied, r.Token
			}
		}
		return CatalogMetadataStale, 0
	}
	if r.Action == pb.CATALOG_ACTION_COMPLETE_START {
		for i := range a.StartPermits {
			old := &a.StartPermits[i]
			if old.Token == r.Token && old.UUID == p.UUID && old.StoreIncarnation == p.StoreIncarnation && old.ReplicaID == p.ReplicaID && old.NonVoting == p.NonVoting {
				if p.Completed == p.Revoked || (old.Revoked && !p.Revoked) || (old.RevocationPending && p.Completed) ||
					(p.Revoked && !old.RevocationPending && !old.Revoked) {
					return CatalogMetadataRejected, 0
				}
				old.Completed, old.Revoked, old.RevocationPending = p.Completed, p.Revoked, false
				if p.Revoked {
					if _, member := a.Members[p.ReplicaID]; !member {
						a.StartPermits = append(a.StartPermits[:i], a.StartPermits[i+1:]...)
					}
				}
				return CatalogMetadataApplied, r.Token
			}
		}
		return CatalogMetadataStale, 0
	}
	if a.Fence != nil || a.Reservation != nil || p.UUID == "" || p.ReplicaID == 0 || p.StoreIncarnation == "" {
		return CatalogMetadataRejected, 0
	}
	shard := s.state.LogState.Shards[DefaultHAKeeperShardID]
	members := shard.Replicas
	if p.NonVoting {
		members = shard.NonVotingReplicas
	}
	info := s.state.LogState.Stores[p.UUID]
	identity, admitted := a.Members[p.ReplicaID]
	if !admitted || identity.Revoked || identity.UUID != p.UUID || identity.StoreIncarnation != p.StoreIncarnation || identity.NonVoting != p.NonVoting {
		return CatalogMetadataRejected, 0
	}
	if members[p.ReplicaID] != p.UUID || info.StoreIncarnation != p.StoreIncarnation || info.CatalogMetadataCapabilities == nil || info.CatalogMetadataCapabilities.HAKeeperBarrierProtocol < 1 {
		return CatalogMetadataRejected, 0
	}
	for _, old := range a.StartPermits {
		if old.UUID == p.UUID {
			// A replacement may not retire a prior incarnation's execution.
			if old.StoreIncarnation != p.StoreIncarnation || (!old.Completed && !old.Revoked) {
				return CatalogMetadataRejected, 0
			}
		}
	}
	token := catalogMetadataNextID(a)
	if token == 0 {
		return CatalogMetadataRejected, 0
	}
	next := *p
	next.Token, next.Completed, next.Revoked, next.RevocationPending = token, false, false, false
	for i := range a.StartPermits {
		if a.StartPermits[i].UUID == p.UUID {
			a.StartPermits[i] = next
			return CatalogMetadataApplied, token
		}
	}
	a.StartPermits = append(a.StartPermits, next)
	sort.Slice(a.StartPermits, func(i, j int) bool { return a.StartPermits[i].UUID < a.StartPermits[j].UUID })
	return CatalogMetadataApplied, token
}

func (s *stateMachine) captureCatalogTargets(view, recovery uint64) ([]pb.CatalogMetadataBarrierTarget, bool) {
	capacity := len(s.state.CNState.Stores) + len(s.state.ProxyState.Stores)
	if capacity == 0 {
		return nil, true
	}
	targets := make([]pb.CatalogMetadataBarrierTarget, 0, capacity)
	for uuid, info := range s.state.CNState.Stores {
		cap := info.CatalogMetadataCapabilities
		if info.ViewMetadataAdmissionGeneration == 0 || cap == nil || cap.BarrierParticipantProtocol < 1 || cap.ViewDependencyProtocol < view || cap.RecoveryProtocol < recovery ||
			(cap.PersistedExpressionProtocol != 0 && info.PersistedExpressionProtocolVersion != 0 && cap.PersistedExpressionProtocol != info.PersistedExpressionProtocolVersion) {
			return nil, false
		}
		targets = append(targets, pb.CatalogMetadataBarrierTarget{ServiceType: pb.CNService, UUID: uuid, Generation: info.ViewMetadataAdmissionGeneration, CapturedTick: s.state.Tick})
	}
	for uuid, info := range s.state.ProxyState.Stores {
		if info.ViewMetadataAdmissionGeneration == 0 || info.CatalogMetadataCapabilities == nil || info.CatalogMetadataCapabilities.BarrierParticipantProtocol < 1 {
			return nil, false
		}
		targets = append(targets, pb.CatalogMetadataBarrierTarget{ServiceType: pb.ProxyService, UUID: uuid, Generation: info.ViewMetadataAdmissionGeneration, CapturedTick: s.state.Tick})
	}
	sort.Slice(targets, func(i, j int) bool { return catalogMetadataTargetLess(&targets[i], &targets[j]) })
	return targets, true
}

func (s *stateMachine) applyCatalogReceipt(b *pb.CatalogMetadataBarrierState, r *pb.CatalogMetadataRequest) (uint64, uint64) {
	a, receipt := b.Arbitration, r.Receipt
	if receipt == nil || len(receipt.Digest) != 32 || receipt.Action != r.Action || receipt.MembershipEpoch != b.MembershipEpoch || receipt.RequiredGeneration != b.RequiredGeneration {
		return CatalogMetadataRejected, 0
	}
	var slot **pb.CatalogMetadataReceipt
	var phase, next pb.CatalogMetadataBarrierPhase
	switch r.Action {
	case pb.CATALOG_ACTION_CATALOG_REQUIRED:
		slot, phase, next = &a.RequiredReceipt, pb.CATALOG_METADATA_BARRIER_SEALED, pb.CATALOG_METADATA_BARRIER_CATALOG_REQUIRED
		if receipt.ClaimID != 0 {
			return CatalogMetadataRejected, 0
		}
	case pb.CATALOG_ACTION_RECOVERY_STARTED:
		slot, phase, next = &a.StartedReceipt, pb.CATALOG_METADATA_BARRIER_CATALOG_REQUIRED, pb.CATALOG_METADATA_BARRIER_RECOVERING
	case pb.CATALOG_ACTION_COMPLETE:
		slot, phase, next = &a.CompletedReceipt, pb.CATALOG_METADATA_BARRIER_RECOVERING, pb.CATALOG_METADATA_BARRIER_ACTIVATED
	}
	if r.Action != pb.CATALOG_ACTION_CATALOG_REQUIRED && (a.ClaimID == 0 || receipt.ClaimID != a.ClaimID) {
		return CatalogMetadataStale, 0
	}
	if *slot != nil {
		if (*slot).ClaimID == receipt.ClaimID && bytes.Equal((*slot).Digest, receipt.Digest) {
			return CatalogMetadataApplied, receipt.ClaimID
		}
		return CatalogMetadataConflict, 0
	}
	if b.Phase != phase || r.ExpectedPhase != phase || !s.catalogMetadataReplicasReady() || !s.catalogMetadataTargetsCurrent(b) || a.Reservation != nil || a.Fence != nil || catalogMetadataPendingStart(a) {
		return CatalogMetadataRejected, 0
	}
	for _, target := range b.Targets {
		if !target.SealComplete {
			return CatalogMetadataRejected, 0
		}
	}
	copy := *receipt
	copy.Digest = append([]byte(nil), receipt.Digest...)
	*slot = &copy
	b.Phase = next
	if next == pb.CATALOG_METADATA_BARRIER_ACTIVATED {
		b.CompletedGeneration = b.RequiredGeneration
	}
	return CatalogMetadataApplied, receipt.ClaimID
}
