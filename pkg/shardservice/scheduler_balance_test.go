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

var (
	factory = func(
		id uint64,
		cn string,
		op pb.OpType,
	) func(t *table) pb.Operator {
		return func(t *table) pb.Operator {
			s := t.shards[id].Clone()
			s.Replicas[0].CN = cn
			switch op {
			case pb.OpType_AddReplica:
				s.Replicas[0].ReplicaID = t.replicaID
				s.Replicas[0].State = pb.ReplicaState_Allocated
			}
			r := s.Replicas[0]
			s.Replicas = nil
			return pb.Operator{
				Type:       op,
				TableShard: s,
				Replica:    r,
			}
		}
	}

	addOp = func(
		id uint64,
		cn string,
	) func(t *table) pb.Operator {
		return factory(id, cn, pb.OpType_AddReplica)
	}
)

func TestScheduleBalance(t *testing.T) {
	cases := []struct {
		name                  string
		cluster               string
		tables                []uint64
		shards                []uint32
		allocateCN            []string
		balanceCN             []string
		freezeCN              []string
		freezeCNTime          []time.Time
		expectAllocateCN      []map[string]int
		expectAfterBalancesCN []map[string]int
		expectOp              []map[string][]func(t *table) pb.Operator
	}{
		{
			name:       "balance shard between 2 cn",
			cluster:    "cn1,cn2",
			tables:     []uint64{1},
			shards:     []uint32{2},
			allocateCN: []string{"cn1"},
			balanceCN:  []string{"cn1", "cn2"},
			expectAllocateCN: []map[string]int{
				{
					"cn1": 2,
				},
			},
			expectAfterBalancesCN: []map[string]int{
				{
					"cn1": 2,
					"cn2": 1,
				},
			},
			expectOp: []map[string][]func(t *table) pb.Operator{
				{
					"cn2": {
						addOp(0, "cn2"),
					},
				},
			},
		},
		{
			name:       "can only balance 1 table once",
			cluster:    "cn1,cn2",
			tables:     []uint64{1, 2},
			shards:     []uint32{2, 2},
			allocateCN: []string{"cn1"},
			balanceCN:  []string{"cn1", "cn2"},
			expectAllocateCN: []map[string]int{
				{
					"cn1": 2,
				},
				{
					"cn1": 2,
				},
			},
			expectAfterBalancesCN: []map[string]int{
				{
					"cn1": 2,
					"cn2": 1,
				},
				{
					"cn1": 2,
				},
			},
			expectOp: []map[string][]func(t *table) pb.Operator{
				{
					"cn2": {
						addOp(0, "cn2"),
					},
				},
				{
					"cn1": {},
					"cn2": {},
				},
			},
		},
		{
			name:       "max-min < 2, cannot balance",
			cluster:    "cn1,cn2",
			tables:     []uint64{1},
			shards:     []uint32{3},
			allocateCN: []string{"cn1", "cn2"},
			balanceCN:  []string{"cn1", "cn2"},
			expectAllocateCN: []map[string]int{
				{
					"cn1": 2,
					"cn2": 1,
				},
			},
			expectAfterBalancesCN: []map[string]int{
				{
					"cn1": 2,
					"cn2": 1,
				},
			},
			expectOp: []map[string][]func(t *table) pb.Operator{
				{
					"cn1": {},
					"cn2": {},
				},
			},
		},
		{
			name:         "freeze cn cannot balance",
			cluster:      "cn1,cn2",
			tables:       []uint64{1},
			shards:       []uint32{2},
			allocateCN:   []string{"cn1"},
			balanceCN:    []string{"cn1", "cn2"},
			freezeCN:     []string{"cn2"},
			freezeCNTime: []time.Time{time.Now()},
			expectAllocateCN: []map[string]int{
				{
					"cn1": 2,
				},
			},
			expectAfterBalancesCN: []map[string]int{
				{
					"cn1": 2,
				},
			},
			expectOp: []map[string][]func(t *table) pb.Operator{
				{
					"cn1": {},
					"cn2": {},
				},
			},
		},
		{
			name:         "freeze cn timeout, can balance",
			cluster:      "cn1,cn2",
			tables:       []uint64{1},
			shards:       []uint32{2},
			allocateCN:   []string{"cn1"},
			balanceCN:    []string{"cn1", "cn2"},
			freezeCN:     []string{"cn2"},
			freezeCNTime: []time.Time{time.Now().Add(-time.Hour)},
			expectAllocateCN: []map[string]int{
				{
					"cn1": 2,
				},
			},
			expectAfterBalancesCN: []map[string]int{
				{
					"cn1": 2,
					"cn2": 1,
				},
			},
			expectOp: []map[string][]func(t *table) pb.Operator{
				{
					"cn2": {
						addOp(0, "cn2"),
					},
				},
			},
		},
	}

	for _, c := range cases {
		runRuntimeTest(
			c.cluster,
			func(r *rt) {
				// setup init cn
				for _, cn := range c.allocateCN {
					r.heartbeat(cn, nil)
				}

				// setup table
				for i, id := range c.tables {
					r.add(newTestTable(id, 1, c.shards[i]))
				}

				s1 := newAllocateScheduler()
				s2 := newBalanceScheduler(1, newFreezeFilter(time.Second), withBalanceOrder(c.tables))

				check := func(m []map[string]int) {
					for i, id := range c.tables {
						expectCNs := m[i]
						table := r.tables[id]
						for cn, count := range expectCNs {
							actualCount := 0
							for _, s := range table.shards {
								for _, r := range s.Replicas {
									if r.CN == cn {
										actualCount++
									}
								}
							}
							require.Equal(t, count, actualCount, "%s, %s", c.name, cn)
						}
					}
				}

				// allocate
				require.NoError(t, s1.schedule(r))
				check(c.expectAllocateCN)

				// reset state
				for _, id := range c.tables {
					table := r.tables[id]
					for i := range table.shards {
						table.shards[i].Replicas[0].State = pb.ReplicaState_Running
					}
				}
				for _, cn := range r.cns {
					cn.incompleteOps = cn.incompleteOps[:0]
				}

				// setup new cn
				for _, cn := range c.balanceCN {
					r.heartbeat(cn, nil)
				}

				for i, cn := range c.freezeCN {
					s2.(*balanceScheduler).freezeFilter.freeze[cn] = c.freezeCNTime[i]
				}

				// balance
				require.NoError(t, s2.schedule(r))
				check(c.expectAfterBalancesCN)

				// check op
				for i, id := range c.tables {
					m := c.expectOp[i]
					table := r.tables[id]
					for cn, fns := range m {
						ops := make([]pb.Operator, 0, len(fns))
						for _, fn := range fns {
							ops = append(ops, fn(table))
						}

						actualOps := make([]pb.Operator, 0, len(r.cns[cn].incompleteOps))
						for _, op := range r.cns[cn].incompleteOps {
							if op.TableShard.TableID == id {
								actualOps = append(actualOps, op)
							}
						}

						require.Equal(t, ops, actualOps, "table %d, %s", id, c.name)
					}
				}
			},
		)
	}
}
