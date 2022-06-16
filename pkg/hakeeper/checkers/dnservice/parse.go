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
	"time"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
)

const (
	dnStoreTimeout  = 5 * time.Second
	DnStoreCapacity = 32
)

// parseDnState parses cluster dn state.
func parseDnState(
	dnState hakeeper.DNState, currTick uint64,
) (*clusterStores, *clusterShards) {
	stores := newClusterStores()
	shards := newClusterShards()

	for storeID, storeInfo := range dnState.Stores {
		expired := false
		if hakeeper.ExpiredTick(storeInfo.Tick, dnStoreTimeout) < currTick {
			expired = true
		}

		store := newDnStore(storeID, len(storeInfo.Shards), DnStoreCapacity)
		if expired {
			stores.registerExpired(store)
		} else {
			stores.registerWorking(store)
		}

		for _, shard := range storeInfo.Shards {
			replica := newReplica(shard.ReplicaID, shard.ShardID, storeID)
			shards.registerReplica(replica, expired)
		}
	}

	return stores, shards
}

type StoreID string

const (
	NullStoreID = StoreID("")
)

// clusterStores collects dn stores by their status.
type clusterStores struct {
	working []*dnStore
	expired []*dnStore
}

func newClusterStores() *clusterStores {
	return &clusterStores{}
}

// registerWorking collects working dn store.
func (cs *clusterStores) registerWorking(store *dnStore) {
	cs.working = append(cs.working, store)
}

// registerExpired collects expired dn store.
func (cs *clusterStores) registerExpired(store *dnStore) {
	cs.expired = append(cs.expired, store)
}

// workingStores returns all recorded working dn stores.
// NB: the returned order isn't deterministic.
func (cs *clusterStores) workingStores() []*dnStore {
	return cs.working
}

// expiredStores returns all recorded expired dn stores.
// NB: the returned order isn't deterministic.
func (cs *clusterStores) expiredStores() []*dnStore {
	return cs.expired
}

// clusterShards collects all dn shards.
type clusterShards struct {
	shards   map[uint64]*dnShard
	shardIDs []uint64
}

func newClusterShards() *clusterShards {
	return &clusterShards{
		shards: make(map[uint64]*dnShard),
	}
}

// registerReplica collects dn shard replicas by their status.
func (cs *clusterShards) registerReplica(replica *dnReplica, expired bool) {
	shardID := replica.shardID
	if _, ok := cs.shards[shardID]; !ok {
		cs.shardIDs = append(cs.shardIDs, shardID)
		cs.shards[shardID] = newDnShard(shardID)
	}
	cs.shards[shardID].register(replica, expired)
}

// listShards lists all the shard IDs.
// NB: the returned order isn't deterministic.
func (cs *clusterShards) listShards() []uint64 {
	return cs.shardIDs
}

// getShard returns dn shard by shard ID.
func (cs *clusterShards) getShard(shardID uint64) (*dnShard, error) {
	if shard, ok := cs.shards[shardID]; ok {
		return shard, nil
	}
	return nil, errShardNotExist
}

// dnStore records metadata for dn store.
type dnStore struct {
	id       StoreID
	length   int
	capacity int
}

func newDnStore(storeID string, length int, capacity int) *dnStore {
	return &dnStore{
		id:       StoreID(storeID),
		length:   length,
		capacity: capacity,
	}
}

// dnShard records metadata for dn shard.
type dnShard struct {
	shardID uint64
	expired []*dnReplica
	working []*dnReplica
}

func newDnShard(shardID uint64) *dnShard {
	return &dnShard{
		shardID: shardID,
	}
}

// register collects dn shard replica.
func (s *dnShard) register(replica *dnReplica, expired bool) {
	if expired {
		s.expired = append(s.expired, replica)
	} else {
		s.working = append(s.working, replica)
	}
}

// workingReplicas returns all working replicas.
// NB: the returned order isn't deterministic.
func (s *dnShard) workingReplicas() []*dnReplica {
	return s.working
}

// workingReplicas returns all expired replicas.
// NB: the returned order isn't deterministic.
func (s *dnShard) expiredReplicas() []*dnReplica {
	return s.expired
}

// dnReplica records metadata for dn shard replica
type dnReplica struct {
	replicaID uint64
	shardID   uint64
	storeID   StoreID
}

func newReplica(
	replicaID, shardID uint64, storeID string,
) *dnReplica {
	return &dnReplica{
		replicaID: replicaID,
		shardID:   shardID,
		storeID:   StoreID(storeID),
	}
}
