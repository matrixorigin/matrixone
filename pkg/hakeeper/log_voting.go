// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hakeeper

import pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"

// ReportsLogVoter checks that the store still hosts the configured voter in
// its current storage generation. Older reports may omit the incarnation.
func ReportsLogVoter(shard pb.LogShardInfo, replicaID uint64, store pb.LogStoreInfo) bool {
	expected := shard.ReplicaStoreIncarnations[replicaID]
	if expected != "" && store.StoreIncarnation != "" && expected != store.StoreIncarnation {
		return false
	}
	for _, report := range store.Replicas {
		if report.ShardID == shard.ShardID && report.ReplicaID == replicaID && !report.IsNonVoting {
			return true
		}
	}
	return false
}

func HasQuorumAfterVotingRemoval(voters, reporters int, removesReporter bool) bool {
	if voters < 2 || reporters <= voters/2 {
		return false
	}
	if removesReporter {
		reporters--
	}
	return reporters > (voters-1)/2
}

// canDeliverVotingRemoval works entirely from the replicated command and log
// reports. Inherited commands without a guard are discarded and replanned.
func (s *stateMachine) canDeliverVotingRemoval(cmd pb.ScheduleCommand) bool {
	if cmd.ServiceType != pb.LogService || cmd.ConfigChange == nil || cmd.ConfigChange.ChangeType != pb.RemoveReplica {
		return true
	}
	if cmd.VotingRemoval == nil {
		return false
	}
	target := cmd.ConfigChange.Replica
	shard, ok := s.state.LogState.Shards[target.ShardID]
	if !ok || shard.Epoch != target.Epoch || target.UUID == "" ||
		shard.Replicas[target.ReplicaID] != target.UUID || cmd.UUID == target.UUID {
		return false
	}

	reporters := 0
	executorReports, targetReports := false, false
	for replicaID, uuid := range shard.Replicas {
		store, ok := s.state.LogState.Stores[uuid]
		if !ok || (s.state.Tick > store.Tick && s.state.Tick-store.Tick > cmd.VotingRemoval.LogStoreTimeoutTicks) ||
			!ReportsLogVoter(shard, replicaID, store) {
			continue
		}
		reporters++
		executorReports = executorReports || uuid == cmd.UUID
		targetReports = targetReports || replicaID == target.ReplicaID
	}
	return executorReports && HasQuorumAfterVotingRemoval(len(shard.Replicas), reporters, targetReports)
}
