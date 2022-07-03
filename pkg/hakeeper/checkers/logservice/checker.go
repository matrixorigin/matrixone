// Copyright 2022 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"sort"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

const LogStoreCapacity int = 32

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

func collectStats(cluster pb.ClusterInfo, infos pb.LogState, currentTick uint64) (*util.ClusterStores, stats) {
	collect := stats{
		toRemove: make(map[uint64][]replica),
		toAdd:    make(map[uint64]int),
	}

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
					replica{uuid: shardInfo.Replicas[replicaSlice[i]],
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
				collect.toStart = append(collect.toStart, replica{uuid: uuid,
					shardID: shardInfo.ShardID, replicaID: replicaID})
			}

			// Check expired
			if hakeeper.ExpiredTick(infos.Stores[uuid].Tick, hakeeper.LogStoreTimeout) < currentTick {
				collect.toRemove[shardInfo.ShardID] = append(collect.toRemove[shardInfo.ShardID],
					replica{uuid: uuid, shardID: shardInfo.ShardID,
						epoch: shardInfo.Epoch, replicaID: replicaID})
			}
		}
	}

	// Check zombies
	for uuid, storeInfo := range infos.Stores {
		for _, replicaInfo := range storeInfo.Replicas {
			if replicaInfo.Epoch < infos.Shards[replicaInfo.ShardID].Epoch {
				collect.toStop = append(collect.toStop, replica{uuid: uuid, shardID: replicaInfo.ShardID})
			}
		}
	}

	return stores, collect
}

func Check(alloc util.IDAllocator, cluster pb.ClusterInfo, infos pb.LogState,
	removing map[uint64][]uint64, adding map[uint64][]uint64, currentTick uint64) (operators []*operator.Operator) {
	stores, stats := collectStats(cluster, infos, currentTick)

	for shardID, toAdd := range stats.toAdd {
		for toAdd-len(adding[shardID]) > 0 {
			bestStore := selector(infos.Shards[shardID], stores)
			newReplicaID, ok := alloc.Next()
			if !ok {
				return nil
			}
			op, err := operator.NewBuilder("", infos.Shards[shardID]).AddPeer(string(bestStore), newReplicaID).Build()
			if err != nil {
				return operators
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
				return operators
			}
			operators = append(operators, op)
		}
	}

	for _, toStart := range stats.toStart {
		operators = append(operators, operator.NewOperator("", toStart.shardID, toStart.epoch,
			operator.StartLogService{StoreID: toStart.uuid, ShardID: toStart.shardID,
				ReplicaID: toStart.replicaID}))
	}

	for _, toStop := range stats.toStop {
		operators = append(operators, operator.NewOperator("", toStop.shardID, toStop.epoch,
			operator.StopLogService{StoreID: toStop.uuid, ShardID: toStop.shardID}))
	}

	return operators
}
