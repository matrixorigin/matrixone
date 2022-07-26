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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestParseDNState(t *testing.T) {
	expiredTick := uint64(10)
	// construct current tick in order to make heartbeat tick expired
	cfg := hakeeper.Config{}
	cfg.Fill()
	currTick := cfg.ExpiredTick(expiredTick, cfg.DnStoreTimeout) + 1

	// 1. no working dn stores
	{
		dnState := pb.DNState{
			Stores: map[string]pb.DNStoreInfo{
				"expired1": {
					Tick: expiredTick,
					Shards: []pb.DNShardInfo{
						mockDnShardMeta(10, 12),
					},
				},
				"expired2": {
					Tick: expiredTick,
					Shards: []pb.DNShardInfo{
						mockDnShardMeta(11, 13),
					},
				},
			},
		}

		stores, shards := parseDnState(cfg, dnState, currTick)

		// check stores
		require.Equal(t, len(stores.WorkingStores()), 0)
		require.Equal(t, len(stores.ExpiredStores()), 2)

		// check shards
		shardIDs := shards.listShards()
		require.Equal(t, len(shardIDs), 2)

		shard10, err := shards.getShard(10)
		require.NoError(t, err)
		require.NotNil(t, shard10)
		require.Equal(t, len(shard10.workingReplicas()), 0)
		require.Equal(t, len(shard10.expiredReplicas()), 1)

		_, err = shards.getShard(100)
		require.Error(t, err)
	}

	// 2. verbose running shard replica
	{
		dnState := pb.DNState{
			Stores: map[string]pb.DNStoreInfo{
				"expired1": {
					Tick: expiredTick,
					Shards: []pb.DNShardInfo{
						mockDnShardMeta(10, 11),
						mockDnShardMeta(14, 17),
					},
				},
				"working1": {
					Tick: currTick,
					Shards: []pb.DNShardInfo{
						mockDnShardMeta(12, 13),
					},
				},
				"working2": {
					Tick: currTick,
					Shards: []pb.DNShardInfo{
						mockDnShardMeta(14, 15),
						mockDnShardMeta(12, 16),
					},
				},
			},
		}

		stores, shards := parseDnState(cfg, dnState, currTick)

		// check stores
		require.Equal(t, len(stores.WorkingStores()), 2)
		require.Equal(t, len(stores.ExpiredStores()), 1)

		// check shards
		shardIDs := shards.listShards()
		require.Equal(t, len(shardIDs), 3)

		shard10, err := shards.getShard(10)
		require.NoError(t, err)
		require.NotNil(t, shard10)
		require.Equal(t, len(shard10.workingReplicas()), 0)
		require.Equal(t, len(shard10.expiredReplicas()), 1)

		shard12, err := shards.getShard(12)
		require.NoError(t, err)
		require.NotNil(t, shard12)
		require.Equal(t, len(shard12.workingReplicas()), 2)
		require.Equal(t, len(shard12.expiredReplicas()), 0)

		shard14, err := shards.getShard(14)
		require.NoError(t, err)
		require.NotNil(t, shard14)
		require.Equal(t, len(shard14.workingReplicas()), 1)
		require.Equal(t, len(shard14.expiredReplicas()), 1)
	}
}

func mockDnShardMeta(shardID, replicaID uint64) pb.DNShardInfo {
	return pb.DNShardInfo{
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
}
