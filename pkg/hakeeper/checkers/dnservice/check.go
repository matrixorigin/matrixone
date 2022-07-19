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
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// Check checks dn state and generate operator for expired dn store.
// The less shard ID, the higher priority.
// NB: the returned order should be deterministic.
func Check(
	idAlloc util.IDAllocator, cfg hakeeper.Config, dnState pb.DNState, currTick uint64,
) []*operator.Operator {
	stores, shards := parseDnState(cfg, dnState, currTick)
	if len(stores.WorkingStores()) < 1 {
		// warning with no working store
		return nil
	}

	// keep order of all shards deterministic
	shardIDs := shards.listShards()
	sort.Slice(shardIDs, func(i, j int) bool {
		return shardIDs[i] < shardIDs[j]
	})

	var operators []*operator.Operator
	for _, shardID := range shardIDs {
		shard, err := shards.getShard(shardID)
		if err != nil {
			// error should be always nil
			panic(fmt.Sprintf("shard `%d` not register", shardID))
		}

		steps := checkShard(shard, stores.WorkingStores(), idAlloc)
		if len(steps) > 0 {
			operators = append(operators,
				operator.NewOperator("dnservice", shardID, operator.NoopEpoch, steps...),
			)
		}

	}
	return operators
}

// schedule generator operator as much as possible
// NB: the returned order should be deterministic.
func checkShard(
	shard *dnShard, workingStores []*util.Store, idAlloc util.IDAllocator,
) []operator.OpStep {
	switch len(shard.workingReplicas()) {
	case 0: // need add replica
		newReplicaID, ok := idAlloc.Next()
		if !ok {
			// temproray failure when allocate replica ID
			return nil
		}

		target, err := consumeLeastSpareStore(workingStores)
		if err != nil {
			// no working dn stores
			return nil
		}

		return []operator.OpStep{
			newAddStep(target, shard.shardID, newReplicaID),
		}
	case 1: // ignore expired replicas
		return nil
	default: // remove extra working replicas
		replicas := extraWorkingReplicas(shard)
		steps := make([]operator.OpStep, 0, len(replicas))
		for _, r := range replicas {
			steps = append(steps,
				newRemoveStep(r.storeID, r.shardID, r.replicaID),
			)
		}
		return steps
	}
}

// newAddStep constructs operator to launch a dn shard replica
func newAddStep(target util.StoreID, shardID, replicaID uint64) operator.OpStep {
	return operator.AddDnReplica{
		StoreID:   string(target),
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
}

// newRemoveStep constructs operator to remove a dn shard replica
func newRemoveStep(target util.StoreID, shardID, replicaID uint64) operator.OpStep {
	return operator.RemoveDnReplica{
		StoreID:   string(target),
		ShardID:   shardID,
		ReplicaID: replicaID,
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
func consumeLeastSpareStore(working []*util.Store) (util.StoreID, error) {
	if len(working) == 0 {
		return util.NullStoreID, errNoWorkingStore
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
