// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardservice

import (
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/stretchr/testify/require"
)

// newServiceWithShardCache builds a bare service whose read cache already
// holds the given shards, so HasAllLocalReplicas can run without a shard
// server round trip.
func newServiceWithShardCache(
	serviceID string,
	tableID uint64,
	metadata pb.ShardsMetadata,
	shards []pb.TableShard,
) *service {
	s := &service{cfg: Config{ServiceID: serviceID}}
	s.cache.read.Store(&readCache{
		shards: map[uint64]shardsCache{
			tableID: {
				metadata: metadata,
				shards:   shards,
			},
		},
	})
	return s
}

// Issue #25874: HasAllLocalReplicas used to drop the isLocalReplica result
// and unconditionally counted the first replica of every shard as local, so
// a table whose shards only have replicas on other CNs was reported as fully
// local.
func TestAuditHasAllLocalReplicasRejectsRemoteOnlyShards(t *testing.T) {
	tableID := uint64(42)
	metadata := pb.ShardsMetadata{ShardsCount: 2}
	shards := []pb.TableShard{
		{
			ShardID: 1,
			Replicas: []pb.ShardReplica{
				{CN: "cn-remote-1", State: pb.ReplicaState_Running},
			},
		},
		{
			ShardID: 2,
			Replicas: []pb.ShardReplica{
				{CN: "cn-remote-2", State: pb.ReplicaState_Running},
			},
		},
	}

	s := newServiceWithShardCache("cn-local", tableID, metadata, shards)
	allLocal, err := s.HasAllLocalReplicas(tableID)
	require.NoError(t, err)
	require.False(t, allLocal,
		"remote-only replicas must not be counted as local replicas")
}

func TestHasAllLocalReplicasSemantics(t *testing.T) {
	tableID := uint64(42)
	running := pb.ReplicaState_Running

	t.Run("every shard has a local replica", func(t *testing.T) {
		s := newServiceWithShardCache(
			"cn-local",
			tableID,
			pb.ShardsMetadata{ShardsCount: 2},
			[]pb.TableShard{
				{ShardID: 1, Replicas: []pb.ShardReplica{
					{CN: "cn-remote-1", State: running},
					{CN: "cn-local", State: running},
				}},
				{ShardID: 2, Replicas: []pb.ShardReplica{
					{CN: "cn-local", State: running},
				}},
			},
		)
		allLocal, err := s.HasAllLocalReplicas(tableID)
		require.NoError(t, err)
		require.True(t, allLocal)
	})

	t.Run("one shard remote only", func(t *testing.T) {
		s := newServiceWithShardCache(
			"cn-local",
			tableID,
			pb.ShardsMetadata{ShardsCount: 2},
			[]pb.TableShard{
				{ShardID: 1, Replicas: []pb.ShardReplica{
					{CN: "cn-local", State: running},
				}},
				{ShardID: 2, Replicas: []pb.ShardReplica{
					{CN: "cn-remote-1", State: running},
					{CN: "cn-remote-2", State: running},
				}},
			},
		)
		allLocal, err := s.HasAllLocalReplicas(tableID)
		require.NoError(t, err)
		require.False(t, allLocal)
	})

	t.Run("shard with no replicas is not fully local", func(t *testing.T) {
		s := newServiceWithShardCache(
			"cn-local",
			tableID,
			pb.ShardsMetadata{ShardsCount: 2},
			[]pb.TableShard{
				{ShardID: 1, Replicas: []pb.ShardReplica{
					{CN: "cn-local", State: running},
				}},
				{ShardID: 2},
			},
		)
		allLocal, err := s.HasAllLocalReplicas(tableID)
		require.NoError(t, err)
		require.False(t, allLocal)
	})
}
