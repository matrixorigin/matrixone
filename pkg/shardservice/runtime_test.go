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

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/stretchr/testify/require"
)

func TestAdd(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			r.heartbeat("cn1", nil)
			t1 := newTestTable(1, 1, 1)
			r.add(t1)
			require.Equal(t, 1, len(r.tables))
			require.Equal(t, 0, len(r.cns["cn1"].incompleteOps))
		},
	)
}

func TestAddWithOldVersion(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			r.heartbeat("cn1", nil)

			t1 := newTestTable(1, 1, 1)
			r.add(t1)

			t1.allocate("cn1", 0, 0)
			t1 = newTestTable(1, 1, 1)
			r.add(t1)
			require.Equal(t, 1, len(r.tables))
			require.Equal(t, 0, len(r.cns["cn1"].incompleteOps))

			t1 = newTestTable(1, 0, 1)
			r.add(t1)
			require.Equal(t, 1, len(r.tables))
			require.Equal(t, 0, len(r.cns["cn1"].incompleteOps))
		},
	)
}

func TestAddWithNewVersion(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			r.heartbeat("cn1", nil)

			t1 := newTestTable(1, 1, 3)
			r.add(t1)

			t1.allocate("cn1", 0, 0)
			t1.allocate("cn1", 1, 0)
			s1 := t1.shards[0]
			r1 := s1.Replicas[0]
			s1.Replicas = nil

			s2 := t1.shards[1]
			r2 := s2.Replicas[0]
			s2.Replicas = nil

			t1 = newTestTable(1, 2, 3)
			r.add(t1)
			require.Equal(t, 1, len(r.tables))
			require.Equal(t, 2, len(r.cns["cn1"].incompleteOps))
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteReplica, TableShard: s1, Replica: r1}, r.cns["cn1"].incompleteOps[0])
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteReplica, TableShard: s2, Replica: r2}, r.cns["cn1"].incompleteOps[1])
			for i, s := range t1.shards {
				require.Equal(t, s, r.tables[t1.id].shards[i])
			}
		},
	)
}

func TestDelete(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			r.heartbeat("cn1", nil)
			t1 := newTestTable(1, 1, 1)
			r.add(t1)

			r.delete(t1.id)
			require.Equal(t, 0, len(r.tables))
			require.Equal(t, 0, len(r.cns["cn1"].incompleteOps))
		},
	)
}

func TestDeleteWithAllocated(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			r.heartbeat("cn1", nil)
			t1 := newTestTable(1, 1, 3)
			r.add(t1)

			t1.allocate("cn1", 0, 0)
			t1.allocate("cn1", 1, 0)

			s1 := t1.shards[0]
			r1 := s1.Replicas[0]
			s1.Replicas = nil

			s2 := t1.shards[1]
			r2 := s2.Replicas[0]
			s2.Replicas = nil

			r.delete(t1.id)
			require.Equal(t, 0, len(r.tables))
			require.Equal(t, 2, len(r.cns["cn1"].incompleteOps))
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteReplica, TableShard: s1, Replica: r1}, r.cns["cn1"].incompleteOps[0])
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteReplica, TableShard: s2, Replica: r2}, r.cns["cn1"].incompleteOps[1])
		},
	)
}

func TestAllocate(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			t1 := newTestTable(1, 1, 3)
			t1.allocate("cn1", 0, 0)
			t1.allocate("cn2", 1, 0)

			require.Equal(t, pb.ReplicaState_Allocated, t1.shards[0].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Allocated, t1.shards[1].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Allocating, t1.shards[2].Replicas[0].State)

			require.Equal(t, uint64(2), t1.shards[0].Replicas[0].Version)
			require.Equal(t, uint64(2), t1.shards[1].Replicas[0].Version)
			require.Equal(t, uint64(1), t1.shards[2].Replicas[0].Version)

			require.Equal(t, uint32(1), t1.shards[0].Version)
			require.Equal(t, uint32(1), t1.shards[1].Version)
			require.Equal(t, uint32(1), t1.shards[2].Version)

			require.Equal(t, "cn1", t1.shards[0].Replicas[0].CN)
			require.Equal(t, "cn2", t1.shards[1].Replicas[0].CN)
			require.Equal(t, "", t1.shards[2].Replicas[0].CN)
		},
	)
}

func TestAllocateWithMultiReplicas(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			t1 := newTestTableWithReplicas(1, 1, 1, 2)
			t1.allocate("cn1", 0, 0)
			t1.allocate("cn2", 0, 1)

			require.Equal(t, pb.ReplicaState_Allocated, t1.shards[0].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Allocated, t1.shards[0].Replicas[1].State)

			require.Equal(t, uint64(2), t1.shards[0].Replicas[0].Version)
			require.Equal(t, uint64(2), t1.shards[0].Replicas[1].Version)

			require.Equal(t, uint32(1), t1.shards[0].Version)

			require.Equal(t, "cn1", t1.shards[0].Replicas[0].CN)
			require.Equal(t, "cn2", t1.shards[0].Replicas[1].CN)
		},
	)
}

func TestNeedAllocate(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			t1 := newTestTable(1, 1, 3)
			require.True(t, t1.needAllocate())

			t1.shards[0].Replicas[0].State = pb.ReplicaState_Tombstone
			require.True(t, t1.needAllocate())
		},
	)
}

func TestNeedAllocateWithMultiReplicas(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			t1 := newTestTableWithReplicas(1, 1, 1, 2)
			require.True(t, t1.needAllocate())

			t1.shards[0].Replicas[0].State = pb.ReplicaState_Tombstone
			require.True(t, t1.needAllocate())
		},
	)
}

func TestGetReplicaCount(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			t1 := newTestTableWithReplicas(1, 1, 1, 3)
			t1.allocate("cn1", 0, 0)
			t1.allocate("cn2", 0, 1)
			t1.allocate("cn2", 0, 2)

			require.Equal(t, 0, t1.getReplicaCount("cn3"))
			require.Equal(t, 1, t1.getReplicaCount("cn1"))
			require.Equal(t, 2, t1.getReplicaCount("cn2"))
		},
	)
}

func TestValid(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			t1 := newTestTable(1, 1, 1)
			s1 := t1.shards[0]

			require.True(t, t1.valid(s1, s1.Replicas[0]))

			target := s1.Replicas[0]
			target.Version--
			require.False(t, t1.valid(s1, target))

			target = s1.Replicas[0]
			target.Version--
			require.False(t, t1.valid(s1, target))

			func() {
				defer func() {
					if err := recover(); err == nil {
						require.Fail(t, "must panic")
					}
				}()

				target = s1.Replicas[0]
				target.Version++
				t1.valid(s1, target)
			}()

			func() {
				defer func() {
					if err := recover(); err == nil {
						require.Fail(t, "must panic")
					}
				}()

				target := s1
				target.Version++
				t1.valid(target, target.Replicas[0])
			}()
		},
	)
}

func TestMove(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			t1 := newTestTable(1, 1, 2)
			t1.allocate("cn1", 0, 0)
			t1.allocate("cn2", 1, 0)
			t1.shards[0].Replicas[0].State = pb.ReplicaState_Running
			t1.shards[1].Replicas[0].State = pb.ReplicaState_Running

			v := t1.shards[0]
			expect := v.Replicas[0]
			expect.CN = "cn1"
			expect.State = pb.ReplicaState_Moving

			_, new, _ := t1.move(func(s string) bool { return s == "cn1" }, func(_ string) string { return "cn2" })

			require.Equal(t, expect, t1.shards[0].Replicas[0])

			expect.ReplicaID = t1.nextReplicaID() - 1
			expect.CN = "cn2"
			expect.State = pb.ReplicaState_Allocated
			require.Equal(t, expect, new)
		},
	)
}

func TestAllocateCompleted(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			t1 := newTestTable(1, 1, 3)
			t1.allocate("cn1", 0, 0)
			t1.allocate("cn2", 1, 0)
			t1.allocate("cn2", 2, 0)

			s1 := t1.shards[0].Clone()
			s2 := t1.shards[1].Clone()
			s3 := t1.shards[2].Clone()

			t1.allocateCompleted(s1, s1.Replicas[0])
			require.Equal(t, pb.ReplicaState_Running, t1.shards[0].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Allocated, t1.shards[1].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Allocated, t1.shards[2].Replicas[0].State)

			t1.allocateCompleted(s2, s2.Replicas[0])
			require.Equal(t, pb.ReplicaState_Running, t1.shards[0].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Running, t1.shards[1].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Allocated, t1.shards[2].Replicas[0].State)

			s3.Replicas[0].Version--
			t1.allocateCompleted(s3, s3.Replicas[0])
			require.Equal(t, pb.ReplicaState_Running, t1.shards[0].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Running, t1.shards[1].Replicas[0].State)
			require.Equal(t, pb.ReplicaState_Allocated, t1.shards[2].Replicas[0].State)
		},
	)
}

func TestHeartbeat(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			require.Equal(t, 0, len(r.cns))
			require.Empty(t, r.heartbeat("cn1", nil))
			require.Equal(t, 1, len(r.cns))
		},
	)
}

func TestHeartbeatWithDownCN(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			r.heartbeat("cn1", nil)
			r.cns["cn1"].down()

			ops := r.heartbeat("cn1", nil)
			require.Equal(t, 1, len(ops))
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteAll}, ops[0])
		},
	)
}

func TestHeartbeatWithUnknownCN(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			ops := r.heartbeat("cn2", nil)
			require.Equal(t, 1, len(ops))
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteAll}, ops[0])
		},
	)
}

func TestHeartbeatWithOp(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			r.heartbeat("cn1", nil)

			t1 := newTestTable(1, 1, 3)
			r.add(t1)
			t1.allocate("cn1", 0, 0)
			t1.allocate("cn1", 1, 0)

			s1 := t1.shards[0].Clone()
			r1 := s1.Replicas[0]

			s2 := t1.shards[1].Clone()
			r2 := s2.Replicas[0]

			r.addOpLocked("cn1", newAddReplicaOp(s1, s1.Replicas[0]))
			r.addOpLocked("cn1", newAddReplicaOp(s2, s2.Replicas[0]))

			ops := r.heartbeat("cn1", nil)
			require.Equal(t, 2, len(ops))

			s1.Replicas = nil
			s2.Replicas = nil
			require.Equal(t, pb.Operator{Type: pb.OpType_AddReplica, TableShard: s1, Replica: r1}, ops[0])
			require.Equal(t, pb.Operator{Type: pb.OpType_AddReplica, TableShard: s2, Replica: r2}, ops[1])
		},
	)
}

func TestHeartbeatWithOpCompleted(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			r.heartbeat("cn1", nil)

			t1 := newTestTable(1, 1, 3)
			r.add(t1)
			t1.allocate("cn1", 0, 0)
			t1.allocate("cn1", 1, 0)

			s1 := t1.shards[0].Clone()
			s2 := t1.shards[1].Clone()

			r.addOpLocked("cn1", newAddReplicaOp(s1, s1.Replicas[0]))
			r.addOpLocked("cn1", newAddReplicaOp(s2, s2.Replicas[0]))

			ops := r.heartbeat("cn1", []pb.TableShard{s1})
			require.Equal(t, pb.ReplicaState_Running, r.tables[t1.id].shards[0].Replicas[0].State)

			r2 := s2.Replicas[0]
			s2.Replicas = nil
			require.Equal(t, 1, len(ops))
			require.Equal(t, pb.Operator{Type: pb.OpType_AddReplica, TableShard: s2, Replica: r2}, ops[0])

			require.Equal(t, 1, len(r.cns["cn1"].incompleteOps))
			require.Equal(t, pb.Operator{Type: pb.OpType_AddReplica, TableShard: s2, Replica: r2}, r.cns["cn1"].incompleteOps[0])
		},
	)
}

func TestHeartbeatWithStaleShard(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			r.heartbeat("cn1", nil)

			t1 := newTestTable(1, 1, 3)
			r.add(t1)
			t1.allocate("cn1", 0, 0)
			t1.allocate("cn1", 1, 0)
			s1 := t1.shards[0].Clone()
			s2 := t1.shards[1].Clone()

			t1.allocate("cn2", 1, 0)

			ops := r.heartbeat("cn1", []pb.TableShard{s1, s2})
			require.Equal(t, 1, len(ops))

			r2 := s2.Replicas[0]
			s2.Replicas = nil
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteReplica, TableShard: s2, Replica: r2}, ops[0])
			require.Equal(t, 1, len(r.cns["cn1"].incompleteOps))
		},
	)
}

func TestHeartbeatWithStaleShardAndMultiReplicas(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			r.heartbeat("cn1", nil)

			t1 := newTestTableWithReplicas(1, 1, 1, 2)
			r.add(t1)
			t1.allocate("cn1", 0, 0)
			t1.allocate("cn1", 0, 1)

			s1 := t1.shards[0].Clone()

			t1.allocate("cn2", 0, 0)
			t1.allocate("cn2", 0, 1)

			ops := r.heartbeat("cn1", []pb.TableShard{s1})
			require.Equal(t, 2, len(ops))

			r1 := s1.Replicas[0]
			r2 := s1.Replicas[1]
			s1.Replicas = nil
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteReplica, TableShard: s1, Replica: r1}, ops[0])
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteReplica, TableShard: s1, Replica: r2}, ops[1])
			require.Equal(t, 2, len(r.cns["cn1"].incompleteOps))
		},
	)
}

func TestHeartbeatWithTableNotExists(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			t1 := newTestTable(1, 1, 1)
			t1.allocate("cn1", 0, 0)
			s1 := t1.shards[0]

			ops := r.heartbeat("cn1", []pb.TableShard{s1})
			require.Equal(t, 1, len(r.cns))
			require.Equal(t, 1, len(ops))
			require.Equal(t, pb.Operator{Type: pb.OpType_CreateTable, TableID: 1}, ops[0])
			require.Equal(t, 0, len(r.cns["cn1"].incompleteOps))
		},
	)
}

func TestGetAvailableCNsLocked(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2,cn3",
		func(r *rt) {
			t1 := newTestTable(1, 1, 1)

			values := r.getAvailableCNsLocked(t1)
			require.Equal(t, 0, len(values))

			r.heartbeat("cn1", nil)
			values = r.getAvailableCNsLocked(t1)
			require.Equal(t, 1, len(values))

			r.heartbeat("cn2", nil)
			values = r.getAvailableCNsLocked(t1)
			require.Equal(t, 2, len(values))
			require.Equal(t, "cn1", values[0].id)
			require.Equal(t, "cn2", values[1].id)

			r.heartbeat("cn3", nil)
			values = r.getAvailableCNsLocked(t1)
			require.Equal(t, 3, len(values))
			require.Equal(t, "cn1", values[0].id)
			require.Equal(t, "cn2", values[1].id)
			require.Equal(t, "cn3", values[2].id)
		},
	)
}

func TestGetAvailableCNsLockedWithFilters(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2,cn3",
		func(r *rt) {
			r.heartbeat("cn1", nil)
			r.heartbeat("cn2", nil)
			r.heartbeat("cn3", nil)

			t1 := newTestTable(1, 1, 3)
			r.add(t1)

			t1.allocate("cn1", 0, 0)
			t1.allocate("cn2", 1, 0)
			t1.allocate("cn3", 2, 0)

			t1.shards[1].Replicas[0].State = pb.ReplicaState_Running
			t1.shards[2].Replicas[0].State = pb.ReplicaState_Running

			f2 := newFreezeFilter(time.Minute)
			f2.freeze["cn3"] = time.Now()

			values := r.getAvailableCNsLocked(t1, newStateFilter(), f2)
			require.Equal(t, 1, len(values))
			require.Equal(t, "cn2", values[0].id)
		},
	)
}

func TestGetDownCNsLocked(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2,cn3",
		func(r *rt) {
			r.heartbeat("cn1", nil)
			r.heartbeat("cn2", nil)

			r.env.(*env).cluster.RemoveCN("cn2")

			m := make(map[string]struct{})
			r.getDownCNsLocked(m)
			require.Equal(t, 1, len(m))
			require.Contains(t, m, "cn2")

			for k := range m {
				delete(m, k)
			}
			r.getDownCNsLocked(m)
			require.Equal(t, 0, len(m))
		},
	)
}

func runRuntimeTest(
	cluster string,
	fn func(*rt),
) {
	initTestCluster(cluster)
	r := newRuntime(NewEnv(sid, ""), runtime.ServiceRuntime(sid).Logger())
	fn(r)
}

func newTestTable(
	id uint64,
	version uint32,
	count uint32,
) *table {
	shardIDs := make([]uint64, 0, count)
	for i := uint64(1); i <= uint64(count); i++ {
		shardIDs = append(shardIDs, i)
	}
	return newTestTableWithAll(
		0,
		id,
		pb.Policy_Hash,
		version,
		count,
		1,
		shardIDs,
	)
}

func newTestTableWithReplicas(
	id uint64,
	version uint32,
	count uint32,
	replicas uint32,
) *table {
	shardIDs := make([]uint64, 0, count)
	for i := uint64(1); i <= uint64(count); i++ {
		shardIDs = append(shardIDs, i)
	}
	return newTestTableWithAll(
		0,
		id,
		pb.Policy_Hash,
		version,
		count,
		replicas,
		shardIDs,
	)
}

func newTestTableWithAll(
	accountID uint64,
	id uint64,
	policy pb.Policy,
	version uint32,
	shardsCount uint32,
	maxReplicaCount uint32,
	shardIDs []uint64,
) *table {
	metadata := pb.ShardsMetadata{
		AccountID:       accountID,
		ShardsCount:     shardsCount,
		Policy:          policy,
		Version:         version,
		MaxReplicaCount: maxReplicaCount,
		ShardIDs:        shardIDs,
	}
	return newTable(
		id,
		metadata,
		1,
	)
}
