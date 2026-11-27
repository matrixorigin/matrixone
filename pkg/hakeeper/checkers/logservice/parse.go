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

package logservice

import (
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func getReplicasToRemove(shardID uint64, current, left map[uint64]string) []replica {
	toRemove := make([]replica, 0, len(current)-len(left))
	for id, uuid := range current {
		if _, ok := left[id]; ok {
			continue
		}
		rep := replica{
			uuid:      uuid,
			shardID:   shardID,
			replicaID: id,
		}
		toRemove = append(toRemove, rep)
	}
	return toRemove
}

func getReplicasToStart(
	shardID uint64,
	replicas map[uint64]string,
	replicaStoreIncarnations map[uint64]string,
	stores map[string]pb.LogStoreInfo,
) []replica {
	toStart := make([]replica, 0)
	for id, uuid := range replicas {
		if storeIncarnationChanged(id, uuid, replicaStoreIncarnations, stores) {
			// This replica belongs to an older persistent storage generation.
			// Starting it with the same replica ID would make Raft peers reuse
			// progress acknowledged by the lost generation.
			continue
		}
		store := stores[uuid]
		// A NodeHost can host only one local instance per shard. A different
		// reported ID must be stopped before scheduling this one.
		if !shardReported(shardID, store.Replicas) {
			rep := replica{
				uuid:      uuid,
				shardID:   shardID,
				replicaID: id,
			}
			toStart = append(toStart, rep)
		}
	}
	return toStart
}

// parseLogShards collects stats for further use.
// It returns two stats, the first is the stats for normal replicas
// and the second one is for the non-voting replicas.
func parseLogShards(
	cluster pb.ClusterInfo, infos pb.LogState, expired []string, nonVotingReplicaNum uint64,
) (*stats, *stats) {
	normalStats, nonVotingStats := newStats(false), newStats(true)
	for _, shardInfo := range infos.Shards {
		shardID := shardInfo.ShardID
		// Existing replica starts are independent of membership reconfiguration.
		// A report alone is not authority to add or remove shard members.
		toStartNormal := getReplicasToStart(
			shardID,
			shardInfo.Replicas,
			shardInfo.ReplicaStoreIncarnations,
			infos.Stores,
		)
		toStartNonVoting := getReplicasToStart(
			shardID,
			shardInfo.NonVotingReplicas,
			shardInfo.ReplicaStoreIncarnations,
			infos.Stores,
		)

		normalStats.toStart = append(normalStats.toStart, toStartNormal...)
		nonVotingStats.toStart = append(nonVotingStats.toStart, toStartNonVoting...)
		record, known := recordForShard(shardID, cluster.LogShards)
		if !known || record.NumberOfReplicas == 0 {
			continue
		}
		fixingNormal, fixingNonVoting := planRepair(record, nonVotingReplicaNum, shardInfo, expired, infos.Stores)
		toRemoveNormal := getReplicasToRemove(shardID, shardInfo.Replicas, fixingNormal.Replicas)
		toRemoveNonVoting := getReplicasToRemove(shardID, shardInfo.NonVotingReplicas, fixingNonVoting.Replicas)

		if fixingNormal.ToAdd > 0 {
			normalStats.toAdd[shardID] = fixingNormal.ToAdd
		}
		if fixingNonVoting.ToAdd > 0 {
			nonVotingStats.toAdd[shardID] = fixingNonVoting.ToAdd
		}
		if len(toRemoveNormal) > 0 {
			normalStats.toRemove[shardID] = toRemoveNormal
		}
		if len(toRemoveNonVoting) > 0 {
			nonVotingStats.toRemove[shardID] = toRemoveNonVoting
		}
	}
	// Check zombies
	for uuid, storeInfo := range infos.Stores {
		if contains(expired, uuid) {
			continue
		}
		zombie := make([]replica, 0)
		for _, replicaInfo := range storeInfo.Replicas {
			_, ok := infos.Shards[replicaInfo.ShardID].Replicas[replicaInfo.ReplicaID]
			if ok || replicaInfo.Epoch >= infos.Shards[replicaInfo.ShardID].Epoch {
				continue
			}
			_, ok = infos.Shards[replicaInfo.ShardID].NonVotingReplicas[replicaInfo.ReplicaID]
			if ok {
				continue
			}
			zombie = append(zombie, replica{uuid: uuid, shardID: replicaInfo.ShardID,
				replicaID: replicaInfo.ReplicaID})
		}
		normalStats.zombies = append(normalStats.zombies, zombie...)
	}
	return normalStats, nonVotingStats
}

// parseLogStores returns all expired stores' ids.
// The first return value is working stores, it is a map, key is uuid, value is locality.
// The second return value is expired stores.
func parseLogStores(cfg hakeeper.Config, infos pb.LogState, currentTick uint64) (map[string]pb.Locality, []string) {
	working := make(map[string]pb.Locality, 0)
	expired := make([]string, 0)
	for uuid, storeInfo := range infos.Stores {
		if cfg.LogStoreExpired(storeInfo.Tick, currentTick) {
			expired = append(expired, uuid)
		} else {
			working[uuid] = storeInfo.Locality
		}
	}
	return working, expired
}

func shardReported(shardID uint64, replicas []pb.LogReplicaInfo) bool {
	for _, r := range replicas {
		if r.ShardID == shardID {
			return true
		}
	}
	return false
}
