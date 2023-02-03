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

type fixingShard struct {
	shardID  uint64
	replicas map[uint64]string
	toAdd    uint32
}

func newFixingShard(origin pb.LogShardInfo) *fixingShard {
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

func fixedLogShardInfo(record metadata.LogShardRecord, info pb.LogShardInfo,
	expiredStores []string) *fixingShard {
	fixing := newFixingShard(info)
	diff := len(fixing.replicas) - int(record.NumberOfReplicas)

	// The number of replicas is less than expected.
	// Record how many replicas should be added.
	if diff < 0 {
		fixing.toAdd = uint32(-diff)
	}

	for replicaID, uuid := range info.Replicas {
		if contains(expiredStores, uuid) {
			delete(fixing.replicas, replicaID)
			// do not remove replicas more than expected.
			if diff > 0 {
				diff--
			}
		}
	}

	// The number of replicas is more than expected.
	// Remove some of them.
	if diff > 0 {
		idSlice := sortedReplicaID(fixing.replicas, info.LeaderID)

		for i := 0; i < diff; i++ {
			delete(fixing.replicas, idSlice[i])
		}
	}

	return fixing
}

// parseLogShards collects stats for further use.
func parseLogShards(cluster pb.ClusterInfo, infos pb.LogState, expired []string) *stats {
	collect := newStats()

	for _, shardInfo := range infos.Shards {
		shardID := shardInfo.ShardID
		record := getRecord(shardID, cluster.LogShards)
		fixing := fixedLogShardInfo(record, shardInfo, expired)

		toRemove := make([]replica, 0, len(shardInfo.Replicas)-len(fixing.replicas))
		for id, uuid := range shardInfo.Replicas {
			if _, ok := fixing.replicas[id]; ok {
				continue
			}
			rep := replica{
				uuid:      uuid,
				shardID:   shardID,
				replicaID: id,
			}
			toRemove = append(toRemove, rep)
		}

		toStart := make([]replica, 0)
		for id, uuid := range fixing.replicas {
			store := infos.Stores[uuid]
			// Check dangling
			if !replicaStarted(shardID, store.Replicas) {
				rep := replica{
					uuid:      uuid,
					shardID:   shardID,
					replicaID: id,
				}
				toStart = append(toStart, rep)
			}
		}
		if fixing.toAdd > 0 {
			collect.toAdd[shardID] = fixing.toAdd
		}
		if len(toRemove) > 0 {
			collect.toRemove[shardID] = toRemove
		}
		collect.toStart = append(collect.toStart, toStart...)
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
			zombie = append(zombie, replica{uuid: uuid, shardID: replicaInfo.ShardID,
				replicaID: replicaInfo.ReplicaID})
		}
		collect.zombies = append(collect.zombies, zombie...)
	}

	return collect
}

// parseLogStores returns all expired stores' ids.
func parseLogStores(cfg hakeeper.Config, infos pb.LogState, currentTick uint64) ([]string, []string) {
	working := make([]string, 0)
	expired := make([]string, 0)
	for uuid, storeInfo := range infos.Stores {
		if cfg.LogStoreExpired(storeInfo.Tick, currentTick) {
			expired = append(expired, uuid)
		} else {
			working = append(working, uuid)
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
