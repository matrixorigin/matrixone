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

			t1.allocate(0, "cn1")
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

			t1.allocate(0, "cn1")
			t1.allocate(1, "cn1")
			s1 := t1.shards[0]
			s2 := t1.shards[1]

			t1 = newTestTable(1, 2, 3)
			r.add(t1)
			require.Equal(t, 1, len(r.tables))
			require.Equal(t, 2, len(r.cns["cn1"].incompleteOps))
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteShard, TableShard: s1}, r.cns["cn1"].incompleteOps[0])
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteShard, TableShard: s2}, r.cns["cn1"].incompleteOps[1])
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

			t1.allocate(0, "cn1")
			t1.allocate(1, "cn1")

			s1 := t1.shards[0]
			s2 := t1.shards[1]

			r.delete(t1.id)
			require.Equal(t, 0, len(r.tables))
			require.Equal(t, 2, len(r.cns["cn1"].incompleteOps))
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteShard, TableShard: s1}, r.cns["cn1"].incompleteOps[0])
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteShard, TableShard: s2}, r.cns["cn1"].incompleteOps[1])
		},
	)
}

func TestAllocate(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			t1 := newTestTable(1, 1, 3)
			t1.allocate(0, "cn1")
			t1.allocate(1, "cn2")

			require.Equal(t, pb.ShardState_Allocated, t1.shards[0].State)
			require.Equal(t, pb.ShardState_Allocated, t1.shards[1].State)
			require.Equal(t, pb.ShardState_Allocating, t1.shards[2].State)

			require.Equal(t, uint64(2), t1.shards[0].BindVersion)
			require.Equal(t, uint64(2), t1.shards[1].BindVersion)
			require.Equal(t, uint64(1), t1.shards[2].BindVersion)

			require.Equal(t, uint32(1), t1.shards[0].ShardsVersion)
			require.Equal(t, uint32(1), t1.shards[1].ShardsVersion)
			require.Equal(t, uint32(1), t1.shards[2].ShardsVersion)

			require.Equal(t, "cn1", t1.shards[0].CN)
			require.Equal(t, "cn2", t1.shards[1].CN)
			require.Equal(t, "", t1.shards[2].CN)
		},
	)
}

func TestNeedAllocate(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			t1 := newTestTable(1, 1, 3)
			require.True(t, t1.needAllocate())

			t1.allocated = true
			require.False(t, t1.needAllocate())

			t1.shards[0].State = pb.ShardState_Tombstone
			require.True(t, t1.needAllocate())
		},
	)
}

func TestGetShardsCount(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			t1 := newTestTable(1, 1, 4)
			t1.allocate(0, "cn1")
			t1.allocate(1, "cn2")
			t1.allocate(2, "cn2")

			require.Equal(t, 0, t1.getShardsCount("cn3"))
			require.Equal(t, 1, t1.getShardsCount("cn1"))
			require.Equal(t, 2, t1.getShardsCount("cn2"))
		},
	)
}

func TestValid(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			t1 := newTestTable(1, 1, 1)
			s1 := t1.shards[0]

			require.True(t, t1.valid(s1))

			target := s1
			target.BindVersion--
			require.False(t, t1.valid(target))

			target = s1
			target.ShardsVersion--
			require.False(t, t1.valid(target))

			func() {
				defer func() {
					if err := recover(); err == nil {
						require.Fail(t, "must panic")
					}
				}()

				target = s1
				target.BindVersion++
				t1.valid(target)
			}()

			func() {
				defer func() {
					if err := recover(); err == nil {
						require.Fail(t, "must panic")
					}
				}()

				target = s1
				target.ShardsVersion++
				t1.valid(target)
			}()
		},
	)
}

func TestMove(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			t1 := newTestTable(1, 1, 2)
			t1.allocate(0, "cn1")
			t1.allocate(1, "cn2")
			t1.shards[0].State = pb.ShardState_Running
			t1.shards[1].State = pb.ShardState_Running

			v := t1.shards[0]

			old, new := t1.moveLocked("cn1", "cn2")

			expect := v
			expect.CN = "cn1"
			expect.State = pb.ShardState_Tombstone
			require.Equal(t, expect, old)

			expect = v
			expect.BindVersion++
			expect.CN = "cn2"
			expect.State = pb.ShardState_Allocated
			require.Equal(t, expect, new)
		},
	)
}

func TestAllocateCompleted(t *testing.T) {
	runRuntimeTest(
		"cn1,cn2",
		func(r *rt) {
			t1 := newTestTable(1, 1, 3)
			t1.allocate(0, "cn1")
			t1.allocate(1, "cn2")
			t1.allocate(2, "cn2")

			s1 := t1.shards[0]
			s2 := t1.shards[1]
			s3 := t1.shards[2]

			t1.allocateCompleted(s1)
			require.Equal(t, pb.ShardState_Running, t1.shards[0].State)
			require.Equal(t, pb.ShardState_Allocated, t1.shards[1].State)
			require.Equal(t, pb.ShardState_Allocated, t1.shards[2].State)

			t1.allocateCompleted(s2)
			require.Equal(t, pb.ShardState_Running, t1.shards[0].State)
			require.Equal(t, pb.ShardState_Running, t1.shards[1].State)
			require.Equal(t, pb.ShardState_Allocated, t1.shards[2].State)

			s3.BindVersion--
			t1.allocateCompleted(s3)
			require.Equal(t, pb.ShardState_Running, t1.shards[0].State)
			require.Equal(t, pb.ShardState_Running, t1.shards[1].State)
			require.Equal(t, pb.ShardState_Allocated, t1.shards[2].State)
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
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteALL}, ops[0])
		},
	)
}

func TestHeartbeatWithUnknownCN(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			ops := r.heartbeat("cn2", nil)
			require.Equal(t, 1, len(ops))
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteALL}, ops[0])
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
			t1.allocate(0, "cn1")
			t1.allocate(1, "cn1")

			s1 := t1.shards[0]
			s2 := t1.shards[1]

			r.addOpLocked("cn1", newAddOp(s1))
			r.addOpLocked("cn1", newAddOp(s2))

			ops := r.heartbeat("cn1", nil)
			require.Equal(t, 2, len(ops))
			require.Equal(t, pb.Operator{Type: pb.OpType_AddShard, TableShard: s1}, ops[0])
			require.Equal(t, pb.Operator{Type: pb.OpType_AddShard, TableShard: s2}, ops[1])
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
			t1.allocate(0, "cn1")
			t1.allocate(1, "cn1")

			s1 := t1.shards[0]
			s2 := t1.shards[1]

			r.addOpLocked("cn1", newAddOp(s1))
			r.addOpLocked("cn1", newAddOp(s2))

			ops := r.heartbeat("cn1", []pb.TableShard{s1})
			require.Equal(t, pb.ShardState_Running, r.tables[t1.id].shards[0].State)
			require.Equal(t, 1, len(ops))
			require.Equal(t, pb.Operator{Type: pb.OpType_AddShard, TableShard: s2}, ops[0])
			require.Equal(t, 1, len(r.cns["cn1"].incompleteOps))
			require.Equal(t, pb.Operator{Type: pb.OpType_AddShard, TableShard: s2}, r.cns["cn1"].incompleteOps[0])
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
			t1.allocate(0, "cn1")
			t1.allocate(1, "cn1")
			s1 := t1.shards[0]
			s2 := t1.shards[1]

			t1.allocate(1, "cn2")

			ops := r.heartbeat("cn1", []pb.TableShard{s1, s2})
			require.Equal(t, 1, len(ops))
			require.Equal(t, pb.Operator{Type: pb.OpType_DeleteShard, TableShard: s2}, ops[0])
			require.Equal(t, 0, len(r.cns["cn1"].incompleteOps))
		},
	)
}

func TestHeartbeatWithTableNotExists(t *testing.T) {
	runRuntimeTest(
		"cn1",
		func(r *rt) {
			t1 := newTestTable(1, 1, 1)
			t1.allocate(0, "cn1")
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

			t1.allocate(0, "cn1")
			t1.allocate(1, "cn2")
			t1.allocate(2, "cn3")

			t1.shards[1].State = pb.ShardState_Running
			t1.shards[2].State = pb.ShardState_Running

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
	r := newRuntime(NewEnv(""))
	fn(r)
}

func newTestTable(
	id uint64,
	version uint32,
	count uint32,
) *table {
	return newTestTableWithAll(
		0,
		id,
		pb.Policy_Hash,
		version,
		count,
		nil,
	)
}

func newTestTableWithAll(
	tenantID uint32,
	id uint64,
	policy pb.Policy,
	version uint32,
	shardsCount uint32,
	physicalShardIDs []uint64,
) *table {
	table := &table{
		metadata: pb.TableShards{
			TenantID:    tenantID,
			ShardsCount: shardsCount,
			Policy:      policy,
			Version:     version,
		},
		id:        id,
		allocated: false,
	}
	table.shards = make([]pb.TableShard, 0, shardsCount)
	for i := uint32(0); i < shardsCount; i++ {
		table.shards = append(table.shards,
			pb.TableShard{
				TableID:       id,
				State:         pb.ShardState_Allocating,
				BindVersion:   1,
				ShardsVersion: version,
				ShardID:       uint64(i),
			})
	}
	if policy == pb.Policy_Partition {
		if len(physicalShardIDs) != len(table.shards) {
			panic("partition and shard count not match")
		}
		for i, id := range physicalShardIDs {
			table.shards[i].ShardID = id
		}
	}
	return table
}
