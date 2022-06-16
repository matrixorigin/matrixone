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

package hakeeper

import (
	"reflect"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	metapb "github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// FIXME: dragonboat should have a public value indicating what is NoLeader node id
const (
	// NoLeader is the replica ID of the leader node.
	NoLeader uint64 = 0
)

type HAKeeperState struct {
	Tick        uint64
	ClusterInfo ClusterInfo
	DNState     DNState
	LogState    LogState
}

// ClusterInfo provides a global view of all shards in the cluster. It
// describes the logical sharding of the system, rather than physical
// distribution of all replicas that belong to those shards.
type ClusterInfo struct {
	DNShards  []metapb.DNShardRecord
	LogShards []metapb.LogShardRecord
}

// DNStoreInfo contins information on a list of shards.
type DNStoreInfo struct {
	Tick   uint64
	Shards []pb.DNShardInfo
}

// DNState contains all DN details known to the HAKeeper.
type DNState struct {
	// Stores is keyed by DN store UUID, it contains details found on each DN
	// store. Each DNStoreInfo reflects what was last reported by each DN store.
	Stores map[string]DNStoreInfo
}

// NewDNState creates a new DNState.
func NewDNState() DNState {
	return DNState{
		Stores: make(map[string]DNStoreInfo),
	}
}

// Update applies the incoming DNStoreHeartbeat into HAKeeper. Tick is the
// current tick of the HAKeeper which can be used as the timestamp of the
// heartbeat.
func (s *DNState) Update(hb pb.DNStoreHeartbeat, tick uint64) {
	storeInfo, ok := s.Stores[hb.UUID]
	if !ok {
		storeInfo = DNStoreInfo{}
	}
	storeInfo.Tick = tick
	storeInfo.Shards = hb.Shards
	s.Stores[hb.UUID] = storeInfo
}

// LogStoreInfo contains information of all replicas found on a Log store.
type LogStoreInfo struct {
	Tick           uint64
	RaftAddress    string
	ServiceAddress string
	GossipAddress  string
	Shards         []pb.LogShardInfo
}

type LogState struct {
	// Shards is keyed by ShardID, it contains details aggregated from all Log
	// stores. Each pb.LogShardInfo here contains data aggregated from
	// different replicas and thus reflect a more accurate description on each
	// shard.
	Shards map[uint64]pb.LogShardInfo
	// Stores is keyed by log store UUID, it contains details found on each store.
	// Each LogStoreInfo here reflects what was last reported by each Log store.
	Stores map[string]LogStoreInfo
}

// NewLogState creates a new LogState.
func NewLogState() LogState {
	return LogState{
		Shards: make(map[uint64]pb.LogShardInfo),
		Stores: make(map[string]LogStoreInfo),
	}
}

// Update applies the incoming heartbeat message to the LogState with the
// specified tick used as the timestamp.
func (s *LogState) Update(hb pb.LogStoreHeartbeat, tick uint64) {
	s.updateStores(hb, tick)
	s.updateShards(hb)
}

func (s *LogState) updateStores(hb pb.LogStoreHeartbeat, tick uint64) {
	storeInfo, ok := s.Stores[hb.UUID]
	if !ok {
		storeInfo = LogStoreInfo{}
	}
	storeInfo.Tick = tick
	storeInfo.RaftAddress = hb.RaftAddress
	storeInfo.ServiceAddress = hb.ServiceAddress
	storeInfo.GossipAddress = hb.GossipAddress
	storeInfo.Shards = hb.Shards
	s.Stores[hb.UUID] = storeInfo
}

func (s *LogState) updateShards(hb pb.LogStoreHeartbeat) {
	for _, incoming := range hb.Shards {
		recorded, ok := s.Shards[incoming.ShardID]
		if !ok {
			recorded = pb.LogShardInfo{
				ShardID:  incoming.ShardID,
				Replicas: make(map[uint64]string),
			}
		}

		if incoming.Epoch > recorded.Epoch {
			recorded.Epoch = incoming.Epoch
			recorded.Replicas = incoming.Replicas
		} else if incoming.Epoch == recorded.Epoch && incoming.Epoch > 0 {
			if !reflect.DeepEqual(recorded.Replicas, incoming.Replicas) {
				plog.Panicf("inconsistent replicas, %+v, %+v, nil: %t, nil: %t",
					recorded.Replicas, incoming.Replicas,
					recorded.Replicas == nil, incoming.Replicas == nil)
			}
		}

		if incoming.Term > recorded.Term && incoming.LeaderID != NoLeader {
			recorded.Term = incoming.Term
			recorded.LeaderID = incoming.LeaderID
		}

		s.Shards[incoming.ShardID] = recorded
	}
}
