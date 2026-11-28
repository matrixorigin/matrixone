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
	"slices"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// LogMembershipTargets returns reporting, retained voters that can submit the
// change with a reporting majority in both configurations.
func LogMembershipTargets(cfg Config, state pb.LogState, tick uint64, change pb.ConfigChangeType, replica pb.Replica) []string {
	targets, _ := logMembershipTargets(cfg.LogStoreTimeoutTicks(), state, tick, change, replica, "")
	return targets
}

// An empty target collects sorted executors; a non-empty target checks one
// executor without allocating a list.
func logMembershipTargets(timeoutTicks uint64, state pb.LogState, tick uint64, change pb.ConfigChangeType, replica pb.Replica, target string) ([]string, bool) {
	shard, ok := state.Shards[replica.ShardID]
	n := len(shard.Replicas)
	if !ok || shard.Epoch != replica.Epoch || n == 0 {
		return nil, false
	}
	voting := change == pb.AddReplica || change == pb.RemoveReplica
	removing := change == pb.RemoveReplica || change == pb.RemoveNonVotingReplica
	switch change {
	case pb.RemoveReplica:
		if shard.Replicas[replica.ReplicaID] != replica.UUID {
			return nil, false
		}
	case pb.RemoveNonVotingReplica:
		if shard.NonVotingReplicas[replica.ReplicaID] != replica.UUID {
			return nil, false
		}
	case pb.AddReplica, pb.AddNonVotingReplica:
		store, exists := state.Stores[replica.UUID]
		if replica.UUID == "" || !exists || commandDeliveryStoreExpired(store.Tick, tick, timeoutTicks) {
			return nil, false
		}
		if _, exists := shard.Replicas[replica.ReplicaID]; exists {
			return nil, false
		}
		if _, exists := shard.NonVotingReplicas[replica.ReplicaID]; exists {
			return nil, false
		}
		for _, uuid := range shard.Replicas {
			if uuid == replica.UUID {
				return nil, false
			}
		}
		for _, uuid := range shard.NonVotingReplicas {
			if uuid == replica.UUID {
				return nil, false
			}
		}
	default:
		return nil, false
	}

	resulting := n
	if voting {
		if removing {
			resulting--
		} else {
			resulting++
		}
	}
	if resulting == 0 {
		return nil, false
	}

	var targets []string
	matched := false
	active := 0
	retained := 0
	for id, uuid := range shard.Replicas {
		store, exists := state.Stores[uuid]
		if !exists || commandDeliveryStoreExpired(store.Tick, tick, timeoutTicks) {
			continue
		}
		if LogReplicaIncarnationChanged(id, uuid, shard.ReplicaStoreIncarnations, state.Stores) ||
			!LogReplicaReported(replica.ShardID, id, false, store.Replicas) {
			continue
		}
		active++
		if change != pb.RemoveReplica || id != replica.ReplicaID {
			retained++
			if target == "" {
				targets = append(targets, uuid)
			} else if uuid == target {
				matched = true
			}
		}
	}
	if active < n/2+1 || retained < resulting/2+1 {
		return nil, false
	}
	if target == "" {
		slices.Sort(targets)
	}
	return targets, matched
}

func isLogMembershipChange(cmd pb.ScheduleCommand) bool {
	if cmd.ServiceType != pb.LogService || cmd.Bootstrapping || cmd.ConfigChange == nil {
		return false
	}
	switch cmd.ConfigChange.ChangeType {
	case pb.AddReplica, pb.RemoveReplica, pb.AddNonVotingReplica, pb.RemoveNonVotingReplica:
		return true
	default:
		return false
	}
}

// logMembershipDeliverable reuses the checker quorum and repair decisions, now
// using the committed policy and reports at the heartbeat's Raft index.
func (s *stateMachine) logMembershipDeliverable(uuid string, cmd pb.ScheduleCommand) bool {
	change := cmd.ConfigChange
	_, retained := logMembershipTargets(s.state.LogStoreTimeoutPolicyTicks, s.state.LogState,
		s.state.Tick, change.ChangeType, change.Replica, uuid)
	if !retained {
		return false
	}
	record, registered := LogShardRecord(change.Replica.ShardID, s.state.ClusterInfo.LogShards)
	if !registered || record.NumberOfReplicas == 0 {
		return false
	}
	shard := s.state.LogState.Shards[record.ShardID]
	if s.bootstrapInFlight(shard, record.NumberOfReplicas) {
		return false // Let the approved initial AddNode entries finish.
	}
	var expired []string
	for _, members := range []map[uint64]string{shard.Replicas, shard.NonVotingReplicas} {
		for _, member := range members {
			if store, ok := s.state.LogState.Stores[member]; ok &&
				commandDeliveryStoreExpired(store.Tick, s.state.Tick, s.state.LogStoreTimeoutPolicyTicks) {
				expired = append(expired, member)
			}
		}
	}
	voters, nonVoters := PlanLogRepair(record, s.state.NonVotingReplicaNum, shard, expired, s.state.LogState.Stores)
	switch change.ChangeType {
	case pb.RemoveReplica:
		_, remains := voters.Replicas[change.Replica.ReplicaID]
		return !remains
	case pb.RemoveNonVotingReplica:
		_, remains := nonVoters.Replicas[change.Replica.ReplicaID]
		return !remains
	case pb.AddReplica, pb.AddNonVotingReplica:
		if uint64(voters.ToAdd) == record.NumberOfReplicas {
			return false // A fresh voting cohort must bootstrap, not join.
		}
		wanted, locality := voters.ToAdd, pb.Locality{}
		if change.ChangeType == pb.AddNonVotingReplica {
			wanted, locality = nonVoters.ToAdd, s.state.NonVotingLocality
		}
		return wanted != 0 && LogStoreEligibleLocality(s.state.LogState.Stores[change.Replica.UUID].Locality, locality)
	default:
		return false
	}
}
