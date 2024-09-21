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

func TestScheduleDown(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			r.heartbeat("cn1", nil)
			r.heartbeat("cn2", nil)

			t1 := newTestTable(1, 1, 2)
			r.add(t1)
			require.NoError(t, newAllocateScheduler().schedule(r))

			r.env.(*env).cluster.RemoveCN("cn1")

			t2 := newTestTable(1, 1, 1)
			r.add(t2)

			require.NoError(t, newDownScheduler().schedule(r))
			require.Equal(t, 1, len(r.cns))
			require.Equal(t, pb.ReplicaState_Allocating, t1.shards[0].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Allocated, t1.shards[1].Replicas[0].State)

			require.Equal(t, pb.ReplicaState_Allocating, t2.shards[0].Replicas[0].State)
			require.True(t, t2.needAllocate())
		},
	)
}
