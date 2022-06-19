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
)

// FIXME: placeholder
type idGenerator func() (uint64, bool)

// CheckService check dn state and generate operator for expired dn store.
// The less shard id, the higher priority.
// NB: the returned order should be deterministic.
func Check(
	cluster hakeeper.ClusterInfo,
	dnState hakeeper.DNState, currTick uint64, idGen idGenerator,
) []Operator {
	stores, shards := parseDnState(dnState, currTick)
	if len(stores.workingStores()) < 1 {
		// warning with no working store
		return nil
	}

	// keep order of all shards deterministic
	shardIDs := shards.listShards()
	sort.Slice(shardIDs, func(i, j int) bool {
		return shardIDs[i] < shardIDs[j]
	})

	var ops []Operator
	for _, shardID := range shardIDs {
		shard, err := shards.getShard(shardID)
		if err != nil {
			// error should be always nil
			panic(fmt.Sprintf("shard `%d` not register", shardID))
		}

		steps := checkShard(shard, stores.workingStores(), idGen)
		for _, step := range steps {
			ops = append(ops, Operator{Step: step})
		}
	}
	return ops
}

// schedule generator operator as much as possible
// NB: the returned order should be deterministic.
func checkShard(
	shard *dnShard, workingStores []*dnStore, idGen idGenerator,
) []Step {
	switch len(shard.workingReplicas()) {
	case 0: // need add replica
		newReplicaID, ok := idGen()
		if !ok {
			// temproray failure when allocate replica ID
			return nil
		}

		target, err := consumeLeastSpareStore(workingStores)
		if err != nil {
			// no working dn stores
			return nil
		}

		return []Step{
			newLaunchStep(shard.shardID, newReplicaID, target),
		}
	case 1: // ignore expired replicas
		return nil
	default: // remove extra working replicas
		replicas := extraWorkingReplicas(shard)
		steps := make([]Step, 0, len(replicas))
		for _, replica := range replicas {
			steps = append(steps, newStopStep(
				replica.shardID,
				replica.replicaID,
				replica.storeID,
			))
		}
		return steps
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
// the store with less id would be chosen.
// NB: the returned result should be deterministic.
func consumeLeastSpareStore(working []*dnStore) (StoreID, error) {
	if len(working) == 0 {
		return NullStoreID, errNoWrokingStore
	}

	// the least shards, the higher priority
	sort.Slice(working, func(i, j int) bool {
		return working[i].length < working[j].length
	})

	// stores with the same length
	var leastStores []*dnStore

	least := working[0].length
	for i := 0; i < len(working); i++ {
		store := working[i]
		if least != store.length {
			break
		}
		leastStores = append(leastStores, store)
	}
	sort.Slice(leastStores, func(i, j int) bool {
		return leastStores[i].id < leastStores[j].id
	})

	// consume a slot from this dn store
	leastStores[0].length += 1

	return leastStores[0].id, nil
}
