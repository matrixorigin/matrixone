// Copyright 2021 - 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// LogRepairPlan contains the retained replicas and remaining additions for one role.
type LogRepairPlan struct {
	Replicas map[uint64]string
	ToAdd    uint32
}

func newPlan(replicas map[uint64]string) *LogRepairPlan {
	plan := &LogRepairPlan{Replicas: make(map[uint64]string, len(replicas))}
	for id, store := range replicas {
		plan.Replicas[id] = store
	}
	return plan
}

func fixedLogShardInfoNormal(
	record metadata.LogShardRecord,
	info pb.LogShardInfo,
	expiredStores []string,
	stores map[string]pb.LogStoreInfo,
) *LogRepairPlan {
	// info.Replicas only contains the normal replicas.
	// info.NonVotingReplicas only contains the non-voting replicas.
	fixing := newPlan(info.Replicas)
	diff := len(fixing.Replicas) - int(record.NumberOfReplicas)
	// The number of replicas is less than expected.
	// Record how many replicas should be added.
	if diff < 0 {
		fixing.ToAdd = uint32(-diff)
	}

	// remove the expired replicas.
	for replicaID, uuid := range info.Replicas {
		if slices.Contains(expiredStores, uuid) {
			delete(fixing.Replicas, replicaID)
			// do not remove replicas more than expected.
			if diff > 0 {
				diff--
			}
		}
	}

	// The number of replicas is more than expected. Remove some of them.
	if diff > 0 {
		idSlice := sortedReplicaID(fixing.Replicas, info.LeaderID)
		// Prefer members that are not reporting their configured voter. Removing
		// a live voter first could lose the projected majority even when a safe
		// surplus candidate exists. Preserve the existing leader/ID ordering
		// among equally available candidates.
		reported := make(map[uint64]bool, len(idSlice))
		for _, id := range idSlice {
			uuid := fixing.Replicas[id]
			reported[id] = !LogReplicaIncarnationChanged(id, uuid, info.ReplicaStoreIncarnations, stores) &&
				LogReplicaReported(info.ShardID, id, false, stores[uuid].Replicas)
		}
		slices.SortStableFunc(idSlice, func(a, b uint64) int {
			if reported[a] == reported[b] {
				return 0
			}
			if reported[a] {
				return 1
			}
			return -1
		})
		for i := 0; i < diff; i++ {
			delete(fixing.Replicas, idSlice[i])
		}
	}

	return fixing
}

func fixedLogShardInfoNonVoting(
	nonVotingReplicaNum uint64,
	info pb.LogShardInfo,
	expiredStores []string,
) *LogRepairPlan {
	// info.Replicas only contains the normal replicas.
	// info.NonVotingReplicas only contains the non-voting replicas.
	fixing := newPlan(info.NonVotingReplicas)
	// nonVotingReplicaNum is the number of expected non-voting replicas.
	// calculate the expected non-voting replicas number.
	diff := len(fixing.Replicas) - int(nonVotingReplicaNum)
	if diff < 0 {
		fixing.ToAdd = uint32(-diff)
	}

	// remove the expired replicas.
	for replicaID, uuid := range info.NonVotingReplicas {
		if slices.Contains(expiredStores, uuid) {
			delete(fixing.Replicas, replicaID)
			if diff > 0 {
				diff--
			}
		}
	}

	// The number of replicas is more than expected. Remove some of them.
	if diff > 0 {
		idSlice := sortedReplicaID(fixing.Replicas, info.LeaderID)
		for i := 0; i < diff; i++ {
			delete(fixing.Replicas, idSlice[i])
		}
	}

	return fixing
}

// PlanLogRepair selects the voting and non-voting replicas required by the checker.
func PlanLogRepair(
	record metadata.LogShardRecord,
	nonVotingReplicaNum uint64,
	info pb.LogShardInfo,
	expiredStores []string,
	stores map[string]pb.LogStoreInfo,
) (*LogRepairPlan, *LogRepairPlan) {
	normal := fixedLogShardInfoNormal(record, info, expiredStores, stores)
	nonVoting := fixedLogShardInfoNonVoting(nonVotingReplicaNum, info, expiredStores)
	removeReplicasOnChangedStoreIncarnation(normal, info, stores)
	removeReplicasOnChangedStoreIncarnation(nonVoting, info, stores)
	return normal, nonVoting
}

// LogShardRecord returns the checker's desired configuration for the shard.
func LogShardRecord(shardID uint64, shards []metadata.LogShardRecord) (metadata.LogShardRecord, bool) {
	for _, record := range shards {
		if record.ShardID == shardID {
			return record, true
		}
	}
	return metadata.LogShardRecord{}, false
}

// LogReplicaIncarnationChanged reports whether a replica belongs to an old store generation.
func LogReplicaIncarnationChanged(
	replicaID uint64,
	storeID string,
	replicaStoreIncarnations map[uint64]string,
	stores map[string]pb.LogStoreInfo,
) bool {
	replicaIncarnation := replicaStoreIncarnations[replicaID]
	storeIncarnation := stores[storeID].StoreIncarnation
	return replicaIncarnation != "" &&
		storeIncarnation != "" &&
		replicaIncarnation != storeIncarnation
}

func removeReplicasOnChangedStoreIncarnation(
	fixed *LogRepairPlan,
	shardInfo pb.LogShardInfo,
	stores map[string]pb.LogStoreInfo,
) {
	for replicaID, storeID := range fixed.Replicas {
		if LogReplicaIncarnationChanged(
			replicaID,
			storeID,
			shardInfo.ReplicaStoreIncarnations,
			stores,
		) {
			delete(fixed.Replicas, replicaID)
		}
	}
}

// sortedReplicaID returns a sorted replica id slice with leader at last.
// The first <expected-current> replicas will be removed as current replica
// num is larger than expected. So we put the leader replica at last to make
// sure that no leader election happened if replicas are removed.
func sortedReplicaID(replicas map[uint64]string, leaderID uint64) []uint64 {
	var exist bool
	idSlice := make([]uint64, 0, len(replicas))
	for id := range replicas {
		if id != leaderID {
			idSlice = append(idSlice, id)
		} else {
			exist = true
		}
	}
	slices.Sort(idSlice)
	if exist {
		idSlice = append(idSlice, leaderID)
	}
	return idSlice
}

// LogReplicaReported reports whether the exact replica and role are present.
func LogReplicaReported(shardID, replicaID uint64, nonVoting bool, replicas []pb.LogReplicaInfo) bool {
	for _, r := range replicas {
		if r.ShardID == shardID && r.ReplicaID == replicaID && r.IsNonVoting == nonVoting {
			return true
		}
	}
	return false
}

// LogStoreEligibleLocality applies the same hard placement constraint to new replicas
// at scheduling and delivery. Empty requirements select only unlabelled stores.
func LogStoreEligibleLocality(store, required pb.Locality) bool {
	if len(required.Value) == 0 {
		return len(store.Value) == 0
	}
	for key, value := range required.Value {
		if got, ok := store.Value[key]; !ok || got != value {
			return false
		}
	}
	return true
}
