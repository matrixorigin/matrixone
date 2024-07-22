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

	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/stretchr/testify/require"
)

func TestScheduleAllocate(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2,cn3",
		func(r *rt) {
			r.heartbeat("cn1", nil)
			r.heartbeat("cn2", nil)
			r.heartbeat("cn3", nil)

			t1 := newTestTable(1, 1, 3)
			r.add(t1)

			expectCNs := []string{"cn1", "cn2", "cn3"}
			expectStates := []pb.ReplicaState{
				pb.ReplicaState_Allocated,
				pb.ReplicaState_Allocated,
				pb.ReplicaState_Allocated,
			}
			expectReplicaVersions := []uint64{
				t1.shards[0].Replicas[0].Version + 1,
				t1.shards[1].Replicas[0].Version + 1,
				t1.shards[2].Replicas[0].Version + 1,
			}

			s := newAllocateScheduler()
			require.NoError(t, s.schedule(r))
			require.Equal(t, false, t1.needAllocate())

			for i := 0; i < 3; i++ {
				require.Equal(t, expectStates[i], t1.shards[i].Replicas[0].State)
				require.Equal(t, expectCNs[i], t1.shards[i].Replicas[0].CN)
				require.Equal(t, expectReplicaVersions[i], t1.shards[i].Replicas[0].Version)
			}

			require.Equal(t, 1, len(r.cns["cn1"].incompleteOps))
			require.Equal(t, 1, len(r.cns["cn2"].incompleteOps))
			require.Equal(t, 1, len(r.cns["cn3"].incompleteOps))
		},
	)
}

func TestScheduleAllocateWithShardNotEnough(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			r.heartbeat("cn1", nil)

			t1 := newTestTable(1, 1, 3)
			r.add(t1)

			expectCNs := []string{"cn1", "cn1", "cn1"}
			expectStates := []pb.ReplicaState{
				pb.ReplicaState_Allocated,
				pb.ReplicaState_Allocated,
				pb.ReplicaState_Allocated,
			}
			expectReplicaVersions := []uint64{
				t1.shards[0].Replicas[0].Version + 1,
				t1.shards[1].Replicas[0].Version + 1,
				t1.shards[2].Replicas[0].Version + 1,
			}

			s := newAllocateScheduler()
			require.NoError(t, s.schedule(r))

			for i := 0; i < 3; i++ {
				require.Equal(t, expectStates[i], t1.shards[i].Replicas[0].State)
				require.Equal(t, expectCNs[i], t1.shards[i].Replicas[0].CN)
				require.Equal(t, expectReplicaVersions[i], t1.shards[i].Replicas[0].Version)
			}

			require.Equal(t, 3, len(r.cns["cn1"].incompleteOps))
		},
	)
}

func TestScheduleAllocateWithNoCN(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {

			t1 := newTestTable(1, 1, 1)
			r.add(t1)

			s := newAllocateScheduler()
			require.NoError(t, s.schedule(r))
			require.True(t, t1.needAllocate())
		},
	)
}
