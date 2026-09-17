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
	sm "github.com/lni/dragonboat/v4/statemachine"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func (s *stateMachine) discardLegacyCatalogSchedule() {
	for uuid, batch := range s.state.ScheduleCommands {
		kept := make([]pb.ScheduleCommand, 0, len(batch.Commands))
		var ids []pb.ScheduleCommandID
		aligned := len(batch.CommandIDs) == len(batch.Commands)
		for i, cmd := range batch.Commands {
			if cmd.ServiceType == pb.LogService && cmd.ConfigChange != nil && cmd.ConfigChange.Replica.ShardID == DefaultHAKeeperShardID {
				continue
			}
			kept = append(kept, cmd)
			if aligned {
				ids = append(ids, batch.CommandIDs[i])
			}
		}
		if len(kept) == 0 {
			delete(s.state.ScheduleCommands, uuid)
			continue
		}
		batch.Commands, batch.CommandIDs = kept, ids
		s.state.ScheduleCommands[uuid] = batch
	}
}

func (s *stateMachine) catalogScheduleObsolete(cmd pb.ScheduleCommand) bool {
	b := s.state.CatalogMetadataBarrier
	if b == nil || b.Arbitration == nil {
		return false
	}
	a := b.Arbitration
	if m := cmd.CatalogMetadataMembership; m != nil {
		return a.Reservation == nil || m.Token != a.Reservation.Token
	}
	if p := cmd.CatalogMetadataStart; p != nil {
		for _, current := range a.StartPermits {
			if current.Token == p.Token && sameCatalogStartIdentity(current, *p) {
				return false
			}
		}
		return true
	}
	return false
}

func sameCatalogStartIdentity(a, b pb.CatalogMetadataStartPermit) bool {
	return a.UUID == b.UUID && a.ReplicaID == b.ReplicaID && a.StoreIncarnation == b.StoreIncarnation && a.NonVoting == b.NonVoting
}

// Pending execution is independent of the destructively delivered queue. A
// lost heartbeat response must not strand a permit the executor never received.
func (s *stateMachine) attachPendingCatalogStart(result sm.Result, hb pb.LogStoreHeartbeat) sm.Result {
	b := s.state.CatalogMetadataBarrier
	if b == nil || b.Arbitration == nil || hb.CatalogMetadataCapabilities == nil || hb.CatalogMetadataCapabilities.HAKeeperBarrierProtocol < 1 {
		return result
	}
	for _, p := range b.Arbitration.StartPermits {
		if p.UUID != hb.UUID || p.StoreIncarnation != hb.StoreIncarnation || p.Completed || p.Revoked {
			continue
		}
		var batch pb.CommandBatch
		if len(result.Data) > 0 {
			if err := batch.Unmarshal(result.Data); err != nil {
				panic(err)
			}
		}
		for _, cmd := range batch.Commands {
			if cmd.CatalogMetadataStart != nil && cmd.CatalogMetadataStart.Token == p.Token {
				return result
			}
		}
		change := pb.StartReplica
		if p.NonVoting {
			change = pb.StartNonVotingReplica
		}
		if p.RevocationPending {
			change = pb.StopReplica
			if p.NonVoting {
				change = pb.StopNonVotingReplica
			}
			p.Revoked = true
		}
		batch.Commands = append(batch.Commands, pb.ScheduleCommand{UUID: p.UUID, ServiceType: pb.LogService, CatalogMetadataStart: &p,
			ConfigChange: &pb.ConfigChange{ChangeType: change, Replica: pb.Replica{ShardID: DefaultHAKeeperShardID, ReplicaID: p.ReplicaID, UUID: p.UUID}}})
		data, err := batch.Marshal()
		if err != nil {
			panic(err)
		}
		result.Data = data
		return result
	}
	return result
}

// prepareCatalogSchedule runs inside the replicated heartbeat/poll entry,
// before destructive delivery. The operation remains in arbitration state even
// after the transport queue no longer contains the delivered command.
func (s *stateMachine) prepareCatalogSchedule(cmd *pb.ScheduleCommand) bool {
	if s.state.CatalogMetadataBarrierRequiredProtocolVersion == 0 || cmd.ServiceType != pb.LogService || cmd.ConfigChange == nil || cmd.ConfigChange.Replica.ShardID != DefaultHAKeeperShardID {
		return true
	}
	b := s.state.CatalogMetadataBarrier
	if b == nil || b.Arbitration == nil || !b.Arbitration.MaintenanceEnabled {
		return false
	}
	a, cfg := b.Arbitration, cmd.ConfigChange
	if p := cmd.CatalogMetadataStart; p != nil {
		for _, current := range a.StartPermits {
			if current.Token == p.Token && sameCatalogStartIdentity(current, *p) {
				if current.RevocationPending && (cfg.ChangeType == pb.StartReplica || cfg.ChangeType == pb.StartNonVotingReplica) {
					return false
				}
				return true
			}
		}
		return false
	}
	replica := cfg.Replica
	incarnation := s.state.LogState.Stores[replica.UUID].StoreIncarnation
	if cfg.ChangeType == pb.RemoveReplica || cfg.ChangeType == pb.RemoveNonVotingReplica {
		incarnation = a.Members[replica.ReplicaID].StoreIncarnation
	}
	r := pb.CatalogMetadataRequest{Version: 1}
	switch cfg.ChangeType {
	case pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica:
		m := a.Reservation
		if m != nil {
			if m.ConfigChangeIndex != replica.Epoch || m.ReplicaID != replica.ReplicaID || m.UUID != replica.UUID || m.ChangeType != cfg.ChangeType || m.StoreIncarnation != incarnation {
				return false
			}
			copy := *m
			cmd.CatalogMetadataMembership = &copy
			return true
		}
		r.Action = pb.CATALOG_ACTION_RESERVE_MEMBERSHIP
		r.Membership = &pb.CatalogMetadataMembershipOperation{ConfigChangeIndex: replica.Epoch, ReplicaID: replica.ReplicaID, UUID: replica.UUID, StoreIncarnation: incarnation, ChangeType: cfg.ChangeType}
	case pb.StopReplica, pb.StopNonVotingReplica, pb.KillZombie:
		return s.prepareCatalogStop(cmd, incarnation)
	case pb.StartReplica, pb.StartNonVotingReplica:
		for _, permit := range a.StartPermits {
			if permit.UUID == replica.UUID && permit.ReplicaID == replica.ReplicaID && permit.StoreIncarnation == incarnation && !permit.Completed && !permit.Revoked && !permit.RevocationPending {
				if permit.NonVoting != (cfg.ChangeType == pb.StartNonVotingReplica) {
					return false
				}
				copy := permit
				cmd.CatalogMetadataStart = &copy
				return true
			}
		}
		r.Action = pb.CATALOG_ACTION_GRANT_START
		r.StartPermit = &pb.CatalogMetadataStartPermit{ReplicaID: replica.ReplicaID, UUID: replica.UUID, StoreIncarnation: incarnation, NonVoting: cfg.ChangeType == pb.StartNonVotingReplica}
	default:
		return true
	}
	entry, err := GetCatalogMetadataRequestCmd(r)
	if err != nil {
		panic(err)
	}
	result := s.handleCatalogMetadataRequest(entry)
	if result.Value != CatalogMetadataApplied {
		return false
	}
	a = s.state.CatalogMetadataBarrier.Arbitration
	if r.Action == pb.CATALOG_ACTION_RESERVE_MEMBERSHIP {
		copy := *a.Reservation
		cmd.CatalogMetadataMembership = &copy
	} else {
		token := binaryEnc.Uint64(result.Data)
		for _, p := range a.StartPermits {
			if p.Token == token {
				copy := p
				cmd.CatalogMetadataStart = &copy
				break
			}
		}
	}
	return true
}

func (s *stateMachine) prepareCatalogStop(cmd *pb.ScheduleCommand, incarnation string) bool {
	cfg := cmd.ConfigChange
	a := s.state.CatalogMetadataBarrier.Arbitration
	var permit *pb.CatalogMetadataStartPermit
	for _, p := range a.StartPermits {
		if p.UUID == cfg.Replica.UUID && p.ReplicaID == cfg.Replica.ReplicaID && p.StoreIncarnation == incarnation {
			copy := p
			permit = &copy
			break
		}
	}
	if permit == nil {
		p := &pb.CatalogMetadataStartPermit{UUID: cfg.Replica.UUID, ReplicaID: cfg.Replica.ReplicaID, StoreIncarnation: incarnation, NonVoting: cfg.ChangeType == pb.StopNonVotingReplica}
		action := pb.CATALOG_ACTION_GRANT_START
		if _, admitted := a.Members[p.ReplicaID]; !admitted {
			action = pb.CATALOG_ACTION_REVOKE_START
			for _, replica := range s.state.LogState.Stores[p.UUID].Replicas {
				if replica.ShardID == DefaultHAKeeperShardID && replica.ReplicaID == p.ReplicaID {
					p.NonVoting = replica.IsNonVoting
				}
			}
		}
		r := pb.CatalogMetadataRequest{Version: 1, Action: action, StartPermit: p}
		entry, err := GetCatalogMetadataRequestCmd(r)
		if err != nil {
			panic(err)
		}
		result := s.handleCatalogMetadataRequest(entry)
		if result.Value != CatalogMetadataApplied {
			return false
		}
		p.Token = binaryEnc.Uint64(result.Data)
		permit = p
	}
	r := pb.CatalogMetadataRequest{Version: 1, Action: pb.CATALOG_ACTION_REVOKE_START, Token: permit.Token, StartPermit: permit}
	entry, err := GetCatalogMetadataRequestCmd(r)
	if err != nil {
		panic(err)
	}
	if s.handleCatalogMetadataRequest(entry).Value != CatalogMetadataApplied {
		return false
	}
	permit.Completed, permit.Revoked = false, true
	cmd.CatalogMetadataStart = permit
	return true
}

func (s *stateMachine) observeCatalogStartResult(hb pb.LogStoreHeartbeat) {
	if b := s.state.CatalogMetadataBarrier; b != nil && b.Arbitration != nil && b.Arbitration.MaintenanceEnabled {
		for id, identity := range b.Arbitration.Members {
			if identity.UUID == hb.UUID && identity.StoreIncarnation != hb.StoreIncarnation {
				identity.Revoked = true
				b.Arbitration.Members[id] = identity
			}
		}
	}
	p := hb.CatalogMetadataStartResult
	if p == nil || hb.StoreIncarnation == "" || p.UUID != hb.UUID || p.StoreIncarnation != hb.StoreIncarnation ||
		s.state.CatalogMetadataBarrierRequiredProtocolVersion == 0 {
		return
	}
	store := s.state.LogState.Stores[hb.UUID]
	if store.StoreIncarnation != hb.StoreIncarnation {
		return
	}
	if p.Completed == p.Revoked {
		return
	}
	// A transport success does not substitute for local execution evidence.
	// Likewise a revoked result cannot contradict a still-running replica in
	// the same heartbeat, including a mismatched voting role.
	running, matchingRole := false, false
	for _, replica := range hb.Replicas {
		if replica.ShardID == DefaultHAKeeperShardID && replica.ReplicaID == p.ReplicaID {
			running = true
			matchingRole = replica.IsNonVoting == p.NonVoting
		}
	}
	if (p.Completed && (!running || !matchingRole)) || (p.Revoked && running) {
		return
	}
	r := pb.CatalogMetadataRequest{Version: 1, Action: pb.CATALOG_ACTION_COMPLETE_START, Token: p.Token, StartPermit: p}
	entry, err := GetCatalogMetadataRequestCmd(r)
	if err != nil {
		panic(err)
	}
	s.handleCatalogMetadataRequest(entry)
}
