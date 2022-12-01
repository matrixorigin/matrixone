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

package dnservice

import (
	"sort"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	// waitingShards makes check logic stateful.
	waitingShards *initialShards

	// bootstrapping indicates the dn is bootstrapping.
	// When dn checker finds a new dn shard should be added,
	// it waits for a while if bootstrapping is false to avoid thrashing.
	// If bootstrapping is true, dn checker will construct create dn shard command immediately.
	// This flag helps to accelarate cluster bootstrapping.
	bootstrapping bool
)

func init() {
	waitingShards = newInitialShards()
	bootstrapping = true
}

// Check checks dn state and generate operator for expired dn store.
// The less shard ID, the higher priority.
// NB: the returned order should be deterministic.
func Check(
	idAlloc util.IDAllocator,
	cfg hakeeper.Config,
	cluster pb.ClusterInfo,
	dnState pb.DNState,
	user pb.TaskTableUser,
	currTick uint64,
	logger *zap.Logger,
) []*operator.Operator {
	stores, reportedShards := parseDnState(cfg, dnState, currTick)
	logger.Debug("reported dn shards in cluster",
		zap.Any("dn shard IDs", reportedShards.shardIDs),
		zap.Any("dn shards", reportedShards.shards),
	)
	for _, node := range stores.ExpiredStores() {
		logutil.Info("node is expired", zap.String("uuid", node.ID))
	}
	if len(stores.WorkingStores()) < 1 {
		logger.Warn("no working dn stores")
		return nil
	}

	mapper := parseClusterInfo(cluster)

	var operators []*operator.Operator

	// 1. check reported dn state
	operators = append(operators,
		checkReportedState(reportedShards, mapper, stores.WorkingStores(), idAlloc, logger)...,
	)

	// 2. check expected dn state
	operators = append(operators,
		checkInitiatingShards(reportedShards, mapper, stores.WorkingStores(), idAlloc, cluster, cfg, currTick, logger)...,
	)

	if user.Username != "" {
		for _, store := range stores.WorkingStores() {
			if !dnState.Stores[store.ID].TaskServiceCreated {
				operators = append(operators, operator.CreateTaskServiceOp("",
					store.ID, pb.DNService, user))
			}
		}
	}

	return operators
}

// schedule generator operator as much as possible
// NB: the returned order should be deterministic.
func checkShard(
	shard *dnShard, mapper ShardMapper, workingStores []*util.Store, idAlloc util.IDAllocator, logger *zap.Logger,
) []operator.OpStep {
	switch len(shard.workingReplicas()) {
	case 0: // need add replica
		newReplicaID, ok := idAlloc.Next()
		if !ok {
			logger.Warn("fail to allocate replica ID")
			return nil
		}

		target, err := consumeLeastSpareStore(workingStores)
		if err != nil {
			logger.Warn("no working dn stores")
			return nil
		}

		logShardID, err := mapper.getLogShardID(shard.shardID)
		if err != nil {
			logger.Warn("shard not registered", zap.Uint64("ShardID", shard.shardID))
			return nil
		}

		s := newAddStep(
			target, shard.shardID, newReplicaID, logShardID,
		)
		logger.Info(s.String())
		return []operator.OpStep{s}

	case 1: // ignore expired replicas
		return nil

	default: // remove extra working replicas
		replicas := extraWorkingReplicas(shard)
		steps := make([]operator.OpStep, 0, len(replicas))

		logShardID, err := mapper.getLogShardID(shard.shardID)
		if err != nil {
			logger.Warn("shard not registered", zap.Uint64("ShardID", shard.shardID))
			return nil
		}

		for _, r := range replicas {
			s := newRemoveStep(
				r.storeID, r.shardID, r.replicaID, logShardID,
			)
			logger.Info(s.String())
			steps = append(steps, s)
		}
		return steps
	}
}

// newAddStep constructs operator to launch a dn shard replica
func newAddStep(target string, shardID, replicaID, logShardID uint64) operator.OpStep {
	return operator.AddDnReplica{
		StoreID:    target,
		ShardID:    shardID,
		ReplicaID:  replicaID,
		LogShardID: logShardID,
	}
}

// newRemoveStep constructs operator to remove a dn shard replica
func newRemoveStep(target string, shardID, replicaID, logShardID uint64) operator.OpStep {
	return operator.RemoveDnReplica{
		StoreID:    target,
		ShardID:    shardID,
		ReplicaID:  replicaID,
		LogShardID: logShardID,
	}
}

// expiredReplicas return all expired replicas.
// NB: the returned order should be deterministic.
func expiredReplicas(shard *dnShard) []*dnReplica {
	expired := shard.expiredReplicas()
	// less replica first
	sort.Slice(expired, func(i, j int) bool {
		return expired[i].replicaID < expired[j].replicaID
	})
	return expired
}

// extraWorkingReplicas return all working replicas except the largest.
// NB: the returned order should be deterministic.
func extraWorkingReplicas(shard *dnShard) []*dnReplica {
	working := shard.workingReplicas()
	if len(working) == 0 {
		return working
	}

	// less replica first
	sort.Slice(working, func(i, j int) bool {
		return working[i].replicaID < working[j].replicaID
	})

	return working[0 : len(working)-1]
}

// consumeLeastSpareStore consume a slot from the least spare dn store.
// If there are multiple dn store with the same least slots,
// the store with less ID would be chosen.
// NB: the returned result should be deterministic.
func consumeLeastSpareStore(working []*util.Store) (string, error) {
	if len(working) == 0 {
		return "", moerr.NewNoWorkingStoreNoCtx()
	}

	// the least shards, the higher priority
	sort.Slice(working, func(i, j int) bool {
		return working[i].Length < working[j].Length
	})

	// stores with the same Length
	var leastStores []*util.Store

	least := working[0].Length
	for i := 0; i < len(working); i++ {
		store := working[i]
		if least != store.Length {
			break
		}
		leastStores = append(leastStores, store)
	}
	sort.Slice(leastStores, func(i, j int) bool {
		return leastStores[i].ID < leastStores[j].ID
	})

	// consume a slot from this dn store
	leastStores[0].Length += 1

	return leastStores[0].ID, nil
}
