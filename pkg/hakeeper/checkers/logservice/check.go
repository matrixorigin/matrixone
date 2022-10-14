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
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

func Check(alloc util.IDAllocator, cfg hakeeper.Config, cluster pb.ClusterInfo, infos pb.LogState,
	executing operator.ExecutingReplicas, currentTick uint64) (operators []*operator.Operator) {
	working, expired := parseLogStores(cfg, infos, currentTick)
	for _, node := range expired {
		logutil.Info("node is expired", zap.String("uuid", node))
	}
	stats := parseLogShards(cluster, infos, expired)

	removing := executing.Removing
	adding := executing.Adding
	starting := executing.Starting

	for shardID, toAdd := range stats.toAdd {
		for toAdd > uint32(len(adding[shardID])) {
			bestStore := selectStore(infos.Shards[shardID], working)
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
			if contains(removing[shardID], toRemoveReplica.replicaID) {
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
		if contains(starting[toStart.shardID], toStart.replicaID) {
			continue
		}

		operators = append(operators, operator.CreateStartReplica("",
			toStart.uuid, toStart.shardID, toStart.replicaID))
	}

	for _, zombie := range stats.zombies {
		operators = append(operators, operator.CreateKillZombie("",
			zombie.uuid, zombie.shardID, zombie.replicaID))
	}

	return operators
}

func contains[T comparable](slice []T, v T) bool {
	for i := range slice {
		if slice[i] == v {
			return true
		}
	}
	return false
}
