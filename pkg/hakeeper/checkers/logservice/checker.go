package logservice

import (
	"sort"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/utils"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

type replica struct {
	uuid    string
	shardID uint64
	epoch   uint64

	replicaID uint64
}

type stats struct {
	toStop   []replica
	toStart  []replica
	toRemove map[uint64][]replica
	toAdd    map[uint64]int
}

func collectStats(cluster hakeeper.ClusterInfo, infos hakeeper.LogState, tick uint64) stats {
	s := stats{
		toRemove: make(map[uint64][]replica),
		toAdd:    make(map[uint64]int),
	}

	for _, shardInfo := range infos.Shards {
		var record metadata.LogShardRecord
		for _, logShardRecord := range cluster.LogShards {
			if logShardRecord.ShardID == shardInfo.ShardID {
				record = logShardRecord
			}
		}

		// Check lacks
		if record.ShardID != 0 && len(shardInfo.Replicas) < int(record.NumberOfReplicas) {
			s.toAdd[shardInfo.ShardID] += int(record.NumberOfReplicas) - len(shardInfo.Replicas)
		}

		// Check redundant
		if record.ShardID != 0 && uint64(len(shardInfo.Replicas)) > record.NumberOfReplicas {
			replicaSlice := make([]uint64, 0, len(shardInfo.Replicas))
			for replicaID := range shardInfo.Replicas {
				replicaSlice = append(replicaSlice, replicaID)
			}
			sort.Slice(replicaSlice, func(i, j int) bool { return replicaSlice[i] < replicaSlice[j] })

			for i := 0; i < len(shardInfo.Replicas)-int(record.NumberOfReplicas); i++ {
				s.toRemove[shardInfo.ShardID] = append(s.toRemove[shardInfo.ShardID],
					replica{uuid: shardInfo.Replicas[replicaSlice[i]],
						shardID: shardInfo.ShardID, replicaID: replicaSlice[i]})
			}
		}

		for replicaID, uuid := range shardInfo.Replicas {
			// Check dangling
			started := false
			for _, shard := range infos.Stores[uuid].Shards {
				if shard.ShardID == shardInfo.ShardID {
					started = true
				}
			}
			if !started {
				s.toStart = append(s.toStart, replica{uuid: uuid,
					shardID: shardInfo.ShardID, replicaID: replicaID})
			}

			// Check expired
			if utils.ExpiredTick(infos.Stores[uuid].Tick, utils.StoreTimeout) < tick {
				s.toRemove[shardInfo.ShardID] = append(s.toRemove[shardInfo.ShardID],
					replica{uuid: uuid, shardID: shardInfo.ShardID,
						epoch: shardInfo.Epoch, replicaID: replicaID})
			}
		}
	}

	// Check zombies
	for uuid, storeInfo := range infos.Stores {
		for _, shardInfo := range storeInfo.Shards {
			if shardInfo.Epoch < infos.Shards[shardInfo.ShardID].Epoch {
				s.toStop = append(s.toStop, replica{uuid: uuid, shardID: shardInfo.ShardID})
			}
		}
	}

	return s
}

func Check(cluster hakeeper.ClusterInfo, infos hakeeper.LogState, removing map[uint64][]uint64, adding map[uint64][]uint64, tick uint64) (operators []*operator.Operator, err error) {
	stats := collectStats(cluster, infos, tick)

	for shardID, toAdd := range stats.toAdd {
		for toAdd-len(adding[shardID]) > 0 {
			var bestStore string
			var newReplicaID uint64
			//bestStore := selectStore()
			//newReplicaID, ok := GenerateNewReplicaID()
			op, err := operator.NewBuilder("", infos.Shards[shardID]).AddPeer(bestStore, newReplicaID).Build()
			if err != nil {
				return nil, err
			}
			operators = append(operators, op)
			toAdd--
		}
	}

	for shardID, toRemove := range stats.toRemove {
		skip := false

		for _, toRemoveReplica := range toRemove {
			for _, removingID := range removing[shardID] {
				if removingID == toRemoveReplica.replicaID {
					skip = true
					break
				}
			}
			if skip {
				continue
			}
			op, err := operator.NewBuilder("", infos.Shards[toRemoveReplica.shardID]).
				RemovePeer(toRemoveReplica.uuid).Build()
			if err != nil {
				return nil, err
			}
			operators = append(operators, op)
		}
	}

	for _, toStart := range stats.toStart {
		operators = append(operators, operator.NewOperator("", toStart.shardID, toStart.epoch,
			operator.StartLogService{UUID: toStart.uuid, ShardID: toStart.shardID,
				ReplicaID: toStart.replicaID}))
	}

	for _, toStop := range stats.toStop {
		operators = append(operators, operator.NewOperator("", toStop.shardID, toStop.epoch,
			operator.StopLogService{UUID: toStop.uuid, ShardID: toStop.shardID}))
	}

	return operators, nil
}
