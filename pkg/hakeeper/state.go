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

	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// FIXME: dragonboat should have a public value indicating what is NoLeader node id
const (
	// NoLeader is the replica ID of the leader node.
	NoLeader uint64 = 0
)

// DNShardInfo contins information on a list of shards.
type DNShardInfo struct {
	Tick   uint64
	Shards []logservice.DNShardInfo
}

// DNState contains all DN details known to the HAKeeper.
type DNState struct {
	// Stores is keyed by DN store UUID, it contains details found on each DN
	// store.
	Stores map[string]DNShardInfo
}

// NewDNState creates a new DNState.
func NewDNState() DNState {
	return DNState{
		Stores: make(map[string]DNShardInfo),
	}
}

// Update applies the incoming DNStoreHeartbeat into HAKeeper. Tick is the
// current tick of the HAKeeper which can be used as the timestamp of the
// heartbeat.
func (s *DNState) Update(hb logservice.DNStoreHeartbeat, tick uint64) {
	shardInfo, ok := s.Stores[hb.UUID]
	if !ok {
		shardInfo = DNShardInfo{}
	}
	shardInfo.Tick = tick
	shardInfo.Shards = hb.Shards
	s.Stores[hb.UUID] = shardInfo
}

// LogShardInfo contains information of all replicas found on a Log store.
type LogShardInfo struct {
	Tick           uint64
	RaftAddress    string
	ServiceAddress string
	GossipAddress  string
	Shards         []logservice.LogShardInfo
}

type LogState struct {
	// Shards is keyed by ShardID, it contains details aggregated from all Log
	// stores. Each logservice.LogShardInfo here contains data aggregated from
	// different replicas and thus reflect a more accurate description on each
	// shard.
	Shards map[uint64]logservice.LogShardInfo
	// Stores is keyed by log store UUID, it contains details found on each store.
	// Each LogShardInfo here reflects what was last reported by each Log store.
	Stores map[string]LogShardInfo
}

// NewLogState creates a new LogState.
func NewLogState() LogState {
	return LogState{
		Shards: make(map[uint64]logservice.LogShardInfo),
		Stores: make(map[string]LogShardInfo),
	}
}

// Update applies the incoming heartbeat message to the LogState with the
// specified tick used as the timestamp.
func (s *LogState) Update(hb logservice.LogStoreHeartbeat, tick uint64) {
	s.updateStores(hb, tick)
	s.updateShards(hb)
}

func (s *LogState) updateStores(hb logservice.LogStoreHeartbeat, tick uint64) {
	shardInfo, ok := s.Stores[hb.UUID]
	if !ok {
		shardInfo = LogShardInfo{}
	}
	shardInfo.Tick = tick
	shardInfo.RaftAddress = hb.RaftAddress
	shardInfo.ServiceAddress = hb.ServiceAddress
	shardInfo.GossipAddress = hb.GossipAddress
	shardInfo.Shards = hb.Shards
	s.Stores[hb.UUID] = shardInfo
}

func (s *LogState) updateShards(hb logservice.LogStoreHeartbeat) {
	for _, incoming := range hb.Shards {
		recorded, ok := s.Shards[incoming.ShardID]
		if !ok {
			recorded = logservice.LogShardInfo{
				ShardID:  incoming.ShardID,
				Replicas: make(map[uint64]string),
			}
		}

		if incoming.Epoch > recorded.Epoch {
			recorded.Epoch = incoming.Epoch
			recorded.Replicas = incoming.Replicas
		} else if incoming.Epoch == recorded.Epoch {
			if !reflect.DeepEqual(recorded.Replicas, incoming.Replicas) {
				plog.Panicf("inconsistent replicas, %+v, %+v",
					recorded.Replicas, incoming.Replicas)
			}
		}

		if incoming.Term > recorded.Term && incoming.LeaderID != NoLeader {
			recorded.Term = incoming.Term
			recorded.LeaderID = incoming.LeaderID
		}

		s.Shards[incoming.ShardID] = recorded
	}
}
