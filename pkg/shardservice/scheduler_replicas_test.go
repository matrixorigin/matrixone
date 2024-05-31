// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"testing"
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/stretchr/testify/require"
)

func TestScheduleReplicas(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			r.heartbeat("cn1", nil)

			t1 := newTestTableWithReplicas(1, 1, 1, 2)
			r.add(t1)

			require.NoError(t, newAllocateScheduler().schedule(r))

			r.heartbeat("cn1", []pb.TableShard{t1.shards[0]})

			require.Equal(t, pb.ReplicaState_Running, t1.shards[0].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Running, t1.shards[0].Replicas[1].State)

			r.heartbeat("cn2", nil)
			require.NoError(t, newBalanceScheduler(1, newFreezeFilter(time.Second)).schedule(r))
			require.Equal(t, pb.ReplicaState_Moving, t1.shards[0].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Running, t1.shards[0].Replicas[1].State)
			require.Equal(t, pb.ReplicaState_Allocated, t1.shards[0].Replicas[2].State)

			s := t1.shards[0].Clone()
			s.Replicas = []pb.ShardReplica{s.Replicas[2]}
			r.heartbeat("cn2", []pb.TableShard{s})

			require.Equal(t, pb.ReplicaState_Moving, t1.shards[0].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Running, t1.shards[0].Replicas[1].State)
			require.Equal(t, pb.ReplicaState_Running, t1.shards[0].Replicas[2].State)

			require.NoError(t, newReplicaScheduler(newFreezeFilter(time.Second)).schedule(r))
			require.Equal(t, pb.ReplicaState_Tombstone, t1.shards[0].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Running, t1.shards[0].Replicas[1].State)
			require.Equal(t, pb.ReplicaState_Running, t1.shards[0].Replicas[2].State)

			s = t1.shards[0].Clone()
			s.Replicas = []pb.ShardReplica{s.Replicas[1]}
			r.heartbeat("cn1", []pb.TableShard{s})
			require.Equal(t, pb.ReplicaState_Running, t1.shards[0].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Running, t1.shards[0].Replicas[1].State)
		},
	)
}
