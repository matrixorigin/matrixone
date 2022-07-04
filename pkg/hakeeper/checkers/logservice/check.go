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
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func Check(alloc util.IDAllocator, cluster pb.ClusterInfo, infos pb.LogState,
	removing map[uint64][]uint64, adding map[uint64][]uint64, currentTick uint64) (operators []*operator.Operator) {
	stats := parseLogShards(cluster, infos, currentTick)
	stores := parseLogStores(infos, currentTick)

	for shardID, toAdd := range stats.toAdd {
		for toAdd > len(adding[shardID]) {
			bestStore := selectStore(infos.Shards[shardID], stores)
			newReplicaID, ok := alloc.Next()
			if !ok {
				return nil
			}
			if op, err := operator.CreateAddReplica(bestStore, infos.Shards[shardID], newReplicaID); err != nil {
				return nil
			} else {
				operators = append(operators, op)
				toAdd--
			}
		}
	}

	for shardID, toRemove := range stats.toRemove {
		for _, toRemoveReplica := range toRemove {
			if isRemoving(toRemoveReplica, removing[shardID]) {
				continue
			}
			if op, err := operator.CreateRemoveReplica(toRemoveReplica.uuid,
				infos.Shards[toRemoveReplica.shardID]); err != nil {
				return nil
			} else {
				operators = append(operators, op)
			}
		}
	}

	for _, toStart := range stats.toStart {
		operators = append(operators, operator.CreateStartReplica("",
			toStart.uuid, toStart.shardID, toStart.replicaID))
	}

	for _, toStop := range stats.toStop {
		operators = append(operators, operator.CreateStopReplica("",
			toStop.uuid, toStop.shardID))
	}

	return operators
}

func isRemoving(replica replica, removingReplicas []uint64) bool {
	if removingReplicas == nil {
		return false
	}
	for _, removingID := range removingReplicas {
		if replica.replicaID == removingID {
			return true
		}
	}
	return false
}
