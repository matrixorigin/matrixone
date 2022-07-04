package logservice

import (
	"sort"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

const LogStoreCapacity int = 32

func parseLogShards(cluster pb.ClusterInfo, infos pb.LogState, currentTick uint64) *stats {
	collect := newStats()

	for _, shardInfo := range infos.Shards {
		var record metadata.LogShardRecord
		for _, logShardRecord := range cluster.LogShards {
			if logShardRecord.ShardID == shardInfo.ShardID {
				record = logShardRecord
			}
		}

		// Check lacks
		if record.ShardID != 0 && len(shardInfo.Replicas) < int(record.NumberOfReplicas) {
			collect.toAdd[shardInfo.ShardID] += int(record.NumberOfReplicas) - len(shardInfo.Replicas)
		}

		// Check redundant
		if record.ShardID != 0 && uint64(len(shardInfo.Replicas)) > record.NumberOfReplicas {
			replicaSlice := make([]uint64, 0, len(shardInfo.Replicas))
			for replicaID := range shardInfo.Replicas {
				replicaSlice = append(replicaSlice, replicaID)
			}
			sort.Slice(replicaSlice, func(i, j int) bool { return replicaSlice[i] < replicaSlice[j] })

			for i := 0; i < len(shardInfo.Replicas)-int(record.NumberOfReplicas); i++ {
				collect.toRemove[shardInfo.ShardID] = append(collect.toRemove[shardInfo.ShardID],
					replica{uuid: util.StoreID(shardInfo.Replicas[replicaSlice[i]]),
						shardID: shardInfo.ShardID, replicaID: replicaSlice[i]})
			}
		}

		for replicaID, uuid := range shardInfo.Replicas {
			// Check dangling
			started := false
			for _, replica := range infos.Stores[uuid].Replicas {
				if replica.ShardID == shardInfo.ShardID {
					started = true
				}
			}
			if !started {
				collect.toStart = append(collect.toStart, replica{uuid: util.StoreID(uuid),
					shardID: shardInfo.ShardID, replicaID: replicaID})
			}

			// Check expired
			if hakeeper.ExpiredTick(infos.Stores[uuid].Tick, hakeeper.LogStoreTimeout) < currentTick {
				collect.toRemove[shardInfo.ShardID] = append(collect.toRemove[shardInfo.ShardID],
					replica{uuid: util.StoreID(uuid), shardID: shardInfo.ShardID,
						epoch: shardInfo.Epoch, replicaID: replicaID})
			}
		}
	}

	// Check zombies
	for uuid, storeInfo := range infos.Stores {
		for _, replicaInfo := range storeInfo.Replicas {
			if replicaInfo.Epoch < infos.Shards[replicaInfo.ShardID].Epoch {
				collect.toStop = append(collect.toStop, replica{uuid: util.StoreID(uuid),
					shardID: replicaInfo.ShardID})
			}
		}
	}

	return collect
}

func parseLogStores(infos pb.LogState, currentTick uint64) *util.ClusterStores {
	stores := util.NewClusterStores()
	for uuid, storeInfo := range infos.Stores {
		expired := false
		store := util.NewStore(uuid, len(storeInfo.Replicas), LogStoreCapacity)
		if currentTick > hakeeper.ExpiredTick(storeInfo.Tick, hakeeper.LogStoreTimeout) {
			expired = true
		}
		if expired {
			stores.RegisterExpired(store)
		} else {
			stores.RegisterWorking(store)
		}
	}

	return stores
}
