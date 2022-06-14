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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func TestDNStateUpdate(t *testing.T) {
	s := NewDNState()
	hb := logservice.DNStoreHeartbeat{
		UUID: "uuid1",
		Shards: []logservice.DNShardInfo{
			{ShardID: 1, ReplicaID: 1},
			{ShardID: 2, ReplicaID: 1},
			{ShardID: 3, ReplicaID: 1},
		},
	}
	s.Update(hb, 1)
	assert.Equal(t, 1, len(s.Stores))
	dninfo, ok := s.Stores[hb.UUID]
	assert.True(t, ok)
	assert.Equal(t, uint64(1), dninfo.Tick)
	require.Equal(t, 3, len(dninfo.Shards))
	assert.Equal(t, hb.Shards, dninfo.Shards)

	hb = logservice.DNStoreHeartbeat{
		UUID: "uuid2",
		Shards: []logservice.DNShardInfo{
			{ShardID: 100, ReplicaID: 1},
		},
	}
	s.Update(hb, 2)

	hb = logservice.DNStoreHeartbeat{
		UUID: "uuid1",
		Shards: []logservice.DNShardInfo{
			{ShardID: 1, ReplicaID: 1},
			{ShardID: 3, ReplicaID: 1},
			{ShardID: 4, ReplicaID: 1},
			{ShardID: 100, ReplicaID: 1},
		},
	}
	s.Update(hb, 2)
	assert.Equal(t, 2, len(s.Stores))
	dninfo, ok = s.Stores[hb.UUID]
	assert.True(t, ok)
	assert.Equal(t, uint64(2), dninfo.Tick)
	require.Equal(t, 4, len(dninfo.Shards))
	assert.Equal(t, hb.Shards, dninfo.Shards)
}

func TestUpdateLogStateStore(t *testing.T) {
	s := NewLogState()
	hb := logservice.LogStoreHeartbeat{
		UUID:           "uuid1",
		RaftAddress:    "localhost:9090",
		ServiceAddress: "localhost:9091",
		GossipAddress:  "localhost:9092",
		Shards: []logservice.LogShardInfo{
			{
				ShardID: 100,
				Replicas: map[uint64]string{
					200: "localhost:8000",
					300: "localhost:9000",
				},
				Epoch:    200,
				LeaderID: 200,
				Term:     10,
			},
			{
				ShardID: 101,
				Replicas: map[uint64]string{
					201: "localhost:8000",
					301: "localhost:9000",
				},
				Epoch:    202,
				LeaderID: 201,
				Term:     30,
			},
		},
	}
	s.Update(hb, 3)

	assert.Equal(t, 1, len(s.Stores))
	lsinfo, ok := s.Stores[hb.UUID]
	require.True(t, ok)
	assert.Equal(t, uint64(3), lsinfo.Tick)
	assert.Equal(t, hb.RaftAddress, lsinfo.RaftAddress)
	assert.Equal(t, hb.ServiceAddress, lsinfo.ServiceAddress)
	assert.Equal(t, hb.GossipAddress, lsinfo.GossipAddress)
	assert.Equal(t, 2, len(lsinfo.Shards))
	assert.Equal(t, hb.Shards, lsinfo.Shards)

	require.Equal(t, 2, len(s.Shards))
	shard1, ok := s.Shards[100]
	assert.True(t, ok)
	assert.Equal(t, hb.Shards[0], shard1)
	shard2, ok := s.Shards[101]
	assert.True(t, ok)
	assert.Equal(t, hb.Shards[1], shard2)

	hb2 := logservice.LogStoreHeartbeat{
		UUID:           "uuid1",
		RaftAddress:    "localhost:9090",
		ServiceAddress: "localhost:9091",
		GossipAddress:  "localhost:9092",
		Shards: []logservice.LogShardInfo{
			{
				ShardID: 100,
				Replicas: map[uint64]string{
					200: "localhost:8000",
					300: "localhost:9000",
					400: "localhost:10000",
				},
				Epoch:    201,
				LeaderID: 400,
				Term:     20,
			},
			{
				ShardID: 101,
				Replicas: map[uint64]string{
					201: "localhost:8000",
				},
				Epoch:    200,
				LeaderID: NoLeader,
				Term:     100,
			},
		},
	}
	s.Update(hb2, 4)

	assert.Equal(t, 1, len(s.Stores))
	lsinfo, ok = s.Stores[hb.UUID]
	require.True(t, ok)
	assert.Equal(t, uint64(4), lsinfo.Tick)
	assert.Equal(t, hb2.RaftAddress, lsinfo.RaftAddress)
	assert.Equal(t, hb2.ServiceAddress, lsinfo.ServiceAddress)
	assert.Equal(t, hb2.GossipAddress, lsinfo.GossipAddress)
	assert.Equal(t, 2, len(lsinfo.Shards))
	assert.Equal(t, hb2.Shards, lsinfo.Shards)

	require.Equal(t, 2, len(s.Shards))
	shard1, ok = s.Shards[100]
	assert.True(t, ok)
	assert.Equal(t, hb2.Shards[0], shard1)
	shard2, ok = s.Shards[101]
	assert.True(t, ok)
	// shard2 didn't change to hb2.Shard[1]
	assert.Equal(t, hb.Shards[1], shard2)
}
