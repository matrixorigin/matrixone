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
	"maps"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// bootstrapCommandStatus validates the queued initial configuration against
// current authority. A report from another store can establish membership but
// cannot acknowledge an individual target's durable start.
func (s *stateMachine) bootstrapCommandStatus(cmd pb.ScheduleCommand) (keep, ready bool) {
	start := cmd.BootstrapShard
	if cmd.ServiceType != pb.LogService || cmd.Bootstrapping || start == nil ||
		start.ShardID == DefaultHAKeeperShardID || start.Join || start.ReplicaID == 0 ||
		cmd.UUID == "" || start.InitialMembers[start.ReplicaID] != cmd.UUID {
		return false, false
	}
	record, known := LogShardRecord(start.ShardID, s.state.ClusterInfo.LogShards)
	if !known || uint64(len(start.InitialMembers)) != record.NumberOfReplicas {
		return false, false
	}
	shard := s.state.LogState.Shards[start.ShardID]
	if len(shard.NonVotingReplicas) != 0 {
		return false, false
	}
	if len(shard.Replicas) != 0 &&
		!pb.IsInitialMembership(start.InitialMembers, shard.Replicas, shard.NonVotingReplicas, shard.Epoch) {
		return false, false
	}
	if LogReplicaIncarnationChanged(start.ReplicaID, cmd.UUID, shard.ReplicaStoreIncarnations, s.state.LogState.Stores) {
		return false, false
	}
	for _, report := range s.state.LogState.Stores[cmd.UUID].Replicas {
		if report.ShardID == start.ShardID && report.ReplicaID == start.ReplicaID &&
			!report.IsNonVoting && report.Replicas[start.ReplicaID] == cmd.UUID {
			return false, false
		}
	}
	// An empty known configuration at a positive index is incomplete evidence.
	return true, shard.Epoch == 0 || len(shard.Replicas) != 0
}

// hasConflictingBootstrap detects conflicting historical claims if neither has
// been disambiguated by the voter map. New admissions cannot create them.
func (s *stateMachine) hasConflictingBootstrap(start *pb.BootstrapShard) bool {
	for _, batch := range s.state.ScheduleCommands {
		for _, cmd := range batch.Commands {
			if other := cmd.BootstrapShard; other != nil && other.ShardID == start.ShardID &&
				!maps.Equal(other.InitialMembers, start.InitialMembers) {
				if keep, _ := s.bootstrapCommandStatus(cmd); keep {
					return true
				}
			}
		}
	}
	return false
}

func (s *stateMachine) admitDynamicBootstrap(cmd pb.ScheduleCommand) bool {
	keep, _ := s.bootstrapCommandStatus(cmd)
	if !keep {
		return false
	}
	start := cmd.BootstrapShard
	approved := false
	for _, batch := range s.state.ScheduleCommands {
		for _, queued := range batch.Commands {
			if other := queued.BootstrapShard; other != nil && other.ShardID == start.ShardID {
				if pending, _ := s.bootstrapCommandStatus(queued); pending {
					if !maps.Equal(start.InitialMembers, other.InitialMembers) || start.ReplicaID == other.ReplicaID {
						return false
					}
					approved = true
				}
			}
		}
	}
	if approved {
		return true // Only a missing target of this exact approved cohort.
	}
	shard := s.state.LogState.Shards[start.ShardID]
	if shard.Epoch != 0 || len(shard.Replicas) != 0 {
		return false // An observed map alone does not authorize a new bootstrap.
	}
	seen := make(map[string]bool, len(start.InitialMembers))
	for id, uuid := range start.InitialMembers {
		if id == 0 || uuid == "" || seen[uuid] {
			return false
		}
		seen[uuid] = true
	}
	return true
}

// pendingDynamicBootstraps retains only the durable bootstrap work across
// legacy global queue replacement. Other service lifecycles stay unchanged.
func (s *stateMachine) pendingDynamicBootstraps() map[string]pb.CommandBatch {
	pending := make(map[string]pb.CommandBatch)
	for uuid, batch := range s.state.ScheduleCommands {
		kept := batch
		kept.Commands = nil
		kept.CommandIDs = nil
		hasIDs := len(batch.CommandIDs) == len(batch.Commands)
		for i, cmd := range batch.Commands {
			if cmd.BootstrapShard != nil {
				if keep, _ := s.bootstrapCommandStatus(cmd); keep {
					kept.Commands = append(kept.Commands, cmd)
					if hasIDs {
						kept.CommandIDs = append(kept.CommandIDs, batch.CommandIDs[i])
					}
				}
			}
		}
		if len(kept.Commands) != 0 {
			pending[uuid] = kept
		}
	}
	return pending
}

// An admitted initial command belongs to the store's current disk, including
// before its first report has bound the replica incarnation in LogShardInfo.
func (s *stateMachine) discardReplacedStoreBootstraps(uuid string) {
	batch, exists := s.state.ScheduleCommands[uuid]
	if !exists {
		return
	}
	hasIDs := len(batch.CommandIDs) == len(batch.Commands)
	var n int
	for i, cmd := range batch.Commands {
		if cmd.ServiceType == pb.LogService && cmd.BootstrapShard != nil {
			continue
		}
		batch.Commands[n] = cmd
		if hasIDs {
			batch.CommandIDs[n] = batch.CommandIDs[i]
		}
		n++
	}
	if n == len(batch.Commands) {
		return
	}
	clear(batch.Commands[n:])
	batch.Commands = batch.Commands[:n]
	if hasIDs {
		batch.CommandIDs = batch.CommandIDs[:n]
	} else {
		batch.CommandIDs = nil
	}
	if n == 0 {
		delete(s.state.ScheduleCommands, uuid)
	} else {
		s.state.ScheduleCommands[uuid] = batch
	}
}

func (s *stateMachine) startHasPendingBootstrap(cmd pb.ScheduleCommand, batch pb.CommandBatch) bool {
	change := cmd.ConfigChange
	if cmd.ServiceType != pb.LogService || cmd.Bootstrapping || change == nil ||
		change.ChangeType != pb.StartReplica || change.Replica.UUID != cmd.UUID {
		return false
	}
	for _, queued := range batch.Commands {
		if start := queued.BootstrapShard; start != nil &&
			start.ShardID == change.Replica.ShardID && start.ReplicaID == change.Replica.ReplicaID {
			if keep, _ := s.bootstrapCommandStatus(queued); keep {
				return true
			}
		}
	}
	return false
}

// logKillObsolete protects a configured identity and an incomplete report from
// the same initial configuration. Dragonboat publishes its local ID before
// applying that ID's initial AddNode, so a longer prefix does not prove removal.
func (s *stateMachine) logKillObsolete(cmd pb.ScheduleCommand) bool {
	change := cmd.ConfigChange
	if cmd.ServiceType != pb.LogService || cmd.UUID == "" || change == nil || change.ChangeType != pb.KillZombie ||
		change.Replica.ShardID == DefaultHAKeeperShardID || change.Replica.UUID != cmd.UUID {
		return false
	}
	replica := change.Replica
	shard := s.state.LogState.Shards[replica.ShardID]
	if (shard.Replicas[replica.ReplicaID] == cmd.UUID || shard.NonVotingReplicas[replica.ReplicaID] == cmd.UUID) &&
		!LogReplicaIncarnationChanged(replica.ReplicaID, cmd.UUID, shard.ReplicaStoreIncarnations, s.state.LogState.Stores) {
		return true
	}
	record, registered := LogShardRecord(replica.ShardID, s.state.ClusterInfo.LogShards)
	if !registered || shard.Epoch == 0 || shard.Epoch >= record.NumberOfReplicas ||
		shard.Epoch != uint64(len(shard.Replicas)) || len(shard.NonVotingReplicas) != 0 {
		return false
	}
	// Each existing voter consumes a distinct initial AddNode index. Here
	// there has been no spare index for a removal. Confirm the reported
	// victim is on this same genesis, not a conflicting Raft configuration.
	for _, report := range s.state.LogState.Stores[cmd.UUID].Replicas {
		if report.ShardID == replica.ShardID && report.ReplicaID == replica.ReplicaID {
			return !report.IsNonVoting &&
				pb.IsInitialMembership(shard.Replicas, report.Replicas, report.NonVotingReplicas, report.Epoch)
		}
	}
	return true // No current report justifies irrevocable cleanup during genesis.
}

// A stop holds, and an authorized kill supersedes, the same local bootstrap.
func (s *stateMachine) bootstrapFencedByBatch(cmd pb.ScheduleCommand, batch pb.CommandBatch) (held, retired bool) {
	start := cmd.BootstrapShard
	for _, sibling := range batch.Commands {
		change := sibling.ConfigChange
		if sibling.ServiceType != pb.LogService || sibling.UUID != cmd.UUID || change == nil ||
			change.Replica.ShardID != start.ShardID || change.Replica.ReplicaID != start.ReplicaID {
			continue
		}
		switch change.ChangeType {
		case pb.StopReplica:
			held = true
		case pb.KillZombie:
			if !s.logKillObsolete(sibling) {
				return true, true
			}
		}
	}
	return held, false
}

func (s *stateMachine) bootstrapInFlight(shard pb.LogShardInfo, desired uint64) bool {
	if uint64(len(shard.Replicas)) >= desired || shard.Epoch >= desired {
		return false
	}
	for _, batch := range s.state.ScheduleCommands {
		for _, cmd := range batch.Commands {
			if cmd.BootstrapShard != nil && cmd.BootstrapShard.ShardID == shard.ShardID {
				if keep, _ := s.bootstrapCommandStatus(cmd); keep {
					return true
				}
			}
		}
	}
	return false
}
