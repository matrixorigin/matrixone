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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// fixingShard is the replicas of the shardID that required.
type fixingShard struct {
	// shardID is the shard ID.
	shardID uint64
	// They are the replicas after calculated. Other replicas need to be removed.
	// The key of the map is replica ID and the value is store UUID.
	replicas map[uint64]string
	// toAdd is the normal replica number needs to be added.
	toAdd uint32
}

func newFixingShardNormal(origin pb.LogShardInfo) *fixingShard {
	shard := &fixingShard{
		shardID:  origin.ShardID,
		replicas: make(map[uint64]string),
		toAdd:    0,
	}
	for replicaID, uuid := range origin.Replicas {
		shard.replicas[replicaID] = uuid
	}
	return shard
}

func newFixingShardNonVoting(origin pb.LogShardInfo) *fixingShard {
	shard := &fixingShard{
		shardID:  origin.ShardID,
		replicas: make(map[uint64]string),
		toAdd:    0,
	}
	for replicaID, uuid := range origin.NonVotingReplicas {
		shard.replicas[replicaID] = uuid
	}
	return shard
}

func fixedLogShardInfoNormal(
	record metadata.LogShardRecord,
	info pb.LogShardInfo,
	expiredStores []string,
) *fixingShard {
	// info.Replicas only contains the normal replicas.
	// info.NonVotingReplicas only contains the non-voting replicas.
	fixing := newFixingShardNormal(info)
	diff := len(fixing.replicas) - int(record.NumberOfReplicas)
	// The number of replicas is less than expected.
	// Record how many replicas should be added.
	if diff < 0 {
		fixing.toAdd = uint32(-diff)
	}

	// remove the expired replicas.
	for replicaID, uuid := range info.Replicas {
		if contains(expiredStores, uuid) {
			delete(fixing.replicas, replicaID)
			// do not remove replicas more than expected.
			if diff > 0 {
				diff--
			}
		}
	}

	// The number of replicas is more than expected. Remove some of them.
	if diff > 0 {
		idSlice := sortedReplicaID(fixing.replicas, info.LeaderID)
		for i := 0; i < diff; i++ {
			delete(fixing.replicas, idSlice[i])
		}
	}

	return fixing
}

func fixedLogShardInfoNonVoting(
	nonVotingReplicaNum uint64,
	info pb.LogShardInfo,
	expiredStores []string,
) *fixingShard {
	// info.Replicas only contains the normal replicas.
	// info.NonVotingReplicas only contains the non-voting replicas.
	fixing := newFixingShardNonVoting(info)
	// nonVotingReplicaNum is the number of expected non-voting replicas.
	// calculate the expected non-voting replicas number.
	diff := len(fixing.replicas) - int(nonVotingReplicaNum)
	if diff < 0 {
		fixing.toAdd = uint32(-diff)
	}

	// remove the expired replicas.
	for replicaID, uuid := range info.NonVotingReplicas {
		if contains(expiredStores, uuid) {
			delete(fixing.replicas, replicaID)
			if diff > 0 {
				diff--
			}
		}
	}

	// The number of replicas is more than expected. Remove some of them.
	if diff > 0 {
		idSlice := sortedReplicaID(fixing.replicas, info.LeaderID)
		for i := 0; i < diff; i++ {
			delete(fixing.replicas, idSlice[i])
		}
	}

	return fixing
}

// fixedLogShardInfo returns the replicas information after fixing for the shard.
// the first return value is the normal replicas, and the second return value
// is the non-voting replicas.
func fixedLogShardInfo(
	record metadata.LogShardRecord,
	nonVotingReplicaNum uint64,
	info pb.LogShardInfo,
	expiredStores []string,
) (*fixingShard, *fixingShard) {
	return fixedLogShardInfoNormal(record, info, expiredStores),
		fixedLogShardInfoNonVoting(nonVotingReplicaNum, info, expiredStores)
}

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
	shardID uint64, replicas map[uint64]string, stores map[string]pb.LogStoreInfo,
) []replica {
	toStart := make([]replica, 0)
	for id, uuid := range replicas {
		store := stores[uuid]
		if !replicaStarted(shardID, store.Replicas) {
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
		record := getRecord(shardID, cluster.LogShards)
		fixingNormal, fixingNonVoting := fixedLogShardInfo(record, nonVotingReplicaNum, shardInfo, expired)

		// cal the toRemove field.
		toRemoveNormal := getReplicasToRemove(shardID, shardInfo.Replicas, fixingNormal.replicas)
		toRemoveNonVoting := getReplicasToRemove(shardID, shardInfo.NonVotingReplicas, fixingNonVoting.replicas)

		// cal the toStart field.
		toStartNormal := getReplicasToStart(shardID, shardInfo.Replicas, infos.Stores)
		toStartNonVoting := getReplicasToStart(shardID, shardInfo.NonVotingReplicas, infos.Stores)

		if fixingNormal.toAdd > 0 {
			normalStats.toAdd[shardID] = fixingNormal.toAdd
		}
		if fixingNonVoting.toAdd > 0 {
			nonVotingStats.toAdd[shardID] = fixingNonVoting.toAdd
		}
		if len(toRemoveNormal) > 0 {
			normalStats.toRemove[shardID] = toRemoveNormal
		}
		if len(toRemoveNonVoting) > 0 {
			nonVotingStats.toRemove[shardID] = toRemoveNonVoting
		}
		normalStats.toStart = append(normalStats.toStart, toStartNormal...)
		nonVotingStats.toStart = append(nonVotingStats.toStart, toStartNonVoting...)
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

// getRecord returns the LogShardRecord with the given shardID.
func getRecord(shardID uint64, LogShards []metadata.LogShardRecord) metadata.LogShardRecord {
	for _, record := range LogShards {
		if record.ShardID == shardID {
			return record
		}
	}
	return metadata.LogShardRecord{}
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
	sort.Slice(idSlice, func(i, j int) bool { return idSlice[i] < idSlice[j] })
	if exist {
		idSlice = append(idSlice, leaderID)
	}
	return idSlice
}

// replicaStarted checks if a replica is started in LogReplicaInfo.
func replicaStarted(shardID uint64, replicas []pb.LogReplicaInfo) bool {
	for _, r := range replicas {
		if r.ShardID == shardID {
			return true
		}
	}
	return false
}
