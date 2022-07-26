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
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

const (
	DnStoreCapacity = 32
)

// parseDnState parses cluster dn state.
func parseDnState(cfg hakeeper.Config,
	dnState pb.DNState, currTick uint64,
) (*util.ClusterStores, *clusterShards) {
	stores := util.NewClusterStores()
	shards := newClusterShards()

	for storeID, storeInfo := range dnState.Stores {
		expired := false
		if cfg.DnStoreExpired(storeInfo.Tick, currTick) {
			expired = true
		}

		store := util.NewStore(storeID, len(storeInfo.Shards), DnStoreCapacity)
		if expired {
			stores.RegisterExpired(store)
		} else {
			stores.RegisterWorking(store)
		}

		for _, shard := range storeInfo.Shards {
			replica := newReplica(shard.ReplicaID, shard.ShardID, storeID)
			shards.registerReplica(replica, expired)
		}
	}

	return stores, shards
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
	storeID   string
}

func newReplica(
	replicaID, shardID uint64, storeID string,
) *dnReplica {
	return &dnReplica{
		replicaID: replicaID,
		shardID:   shardID,
		storeID:   storeID,
	}
}
