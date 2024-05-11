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
	"sort"
	"sync"

	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"go.uber.org/zap"
)

// rt is the information about the Shards of the MO cluster maintained
// on the ShardServer, which contains information about which CNs are
// available, which Shards' Tables are available, the distribution of
// Shards and CNs, and so on.
type rt struct {
	sync.RWMutex
	env    Env
	tables map[uint64]*table
	cns    map[string]*cn
}

func newRuntime(env Env) *rt {
	return &rt{
		env:    env,
		tables: make(map[uint64]*table, 256),
		cns:    make(map[string]*cn, 256),
	}
}

// heartbeat each cn node periodically reports the table shards it manages
// to the shard server.
//
// Based on the reported information, the shard server will determine whether
// the operator sent to the corresponding cn is complete or not.
//
// When shard server restarts, all table shards information is lost. The table
// shard metadata is persistent data, and the shard and CN binding metadata is
// dynamically changing data from runtime.
//
// When the shard server discovers that the table's shards metadata is missing
// from memory, it returns a CreateTable operator that allows the corresponding
// CN.
func (r *rt) heartbeat(
	cn string,
	shards []pb.TableShard,
) []pb.Operator {
	if !r.env.HasCN(cn) {
		return []pb.Operator{newDeleteAllOp()}
	}

	r.Lock()
	defer r.Unlock()

	c, ok := r.cns[cn]
	if !ok {
		c = r.newCN(cn)
		r.cns[cn] = c
	}
	if c.isDown() {
		return []pb.Operator{newDeleteAllOp()}
	}

	ops := c.closeCompletedOp(
		shards,
		func(s pb.TableShard) {
			if t, ok := r.tables[s.TableID]; ok {
				t.allocateCompleted(s)
			}
		},
	)

	for _, s := range shards {
		if t, ok := r.tables[s.TableID]; ok {
			if !t.valid(s) {
				ops = append(ops, newDeleteOp(s))
			}
		} else {
			ops = append(ops, newCreateTableOp(s.TableID))
		}
	}
	return ops
}

func (r *rt) add(t *table) {
	r.Lock()
	defer r.Unlock()
	old, ok := r.tables[t.id]
	if !ok {
		r.tables[t.id] = t
		return
	}

	// shards not changed
	if old.metadata.Version >= t.metadata.Version {
		return
	}

	r.deleteTableLocked(old)
	r.tables[t.id] = t
}

func (r *rt) delete(id uint64) {
	r.Lock()
	defer r.Unlock()

	table, ok := r.tables[id]
	if !ok {
		return
	}
	r.deleteTableLocked(table)
	delete(r.tables, id)
}

func (r *rt) deleteTableLocked(t *table) {
	getLogger().Info("remove table shards",
		zap.Uint64("table", t.id),
		tableShardsField("shards", t.metadata),
		tableShardSliceField("binds", t.shards))

	for _, bind := range t.shards {
		if bind.CN == "" {
			continue
		}
		r.addOpLocked(bind.CN, newDeleteOp(bind))
	}
}

func (r *rt) newCN(id string) *cn {
	return &cn{
		id:    id,
		state: pb.CNState_Up,
	}
}

func (r *rt) getAvailableCNsLocked(
	t *table,
	filters ...filter,
) []*cn {
	var cns []*cn
	for _, cn := range r.cns {
		if cn.available(t.metadata.TenantID, r.env) {
			cns = append(cns, cn)
		}
	}
	sort.Slice(cns, func(i, j int) bool {
		return cns[i].id < cns[j].id
	})
	for _, f := range filters {
		cns = f.filter(r, cns)
		if len(cns) == 0 {
			return nil
		}
	}
	return cns
}

func (r *rt) hasNotRunningShardLocked(cn string) bool {
	for _, t := range r.tables {
		for _, s := range t.shards {
			if s.CN == cn && s.State != pb.ShardState_Running {
				return true
			}
		}
	}
	return false
}

func (r *rt) addOpLocked(
	cn string,
	ops ...pb.Operator,
) {
	if c, ok := r.cns[cn]; ok {
		c.addOps(ops...)
	}
}

func (r *rt) getDownCNsLocked(downCNs map[string]struct{}) {
	for _, cn := range r.cns {
		if !cn.isDown() && r.env.HasCN(cn.id) {
			continue
		}

		cn.down()
		delete(r.cns, cn.id)
		downCNs[cn.id] = struct{}{}
	}
}

type table struct {
	id        uint64
	metadata  pb.TableShards
	shards    []pb.TableShard
	allocated bool
}

func (t *table) allocate(
	i int,
	cn string,
) {
	t.shards[i].CN = cn
	t.shards[i].State = pb.ShardState_Allocated
	t.shards[i].BindVersion++
}

func (t *table) needAllocate() bool {
	if !t.allocated {
		return true
	}
	for _, s := range t.shards {
		if s.State == pb.ShardState_Tombstone {
			return true
		}
	}
	return false
}

func (t *table) getShardsCount(cn string) int {
	count := 0
	for _, s := range t.shards {
		if s.CN == cn {
			count++
		}
	}
	return count
}

func (t *table) valid(target pb.TableShard) bool {
	for _, current := range t.shards {
		if current.ShardID != target.ShardID {
			continue
		}
		if current.ShardsVersion > target.ShardsVersion ||
			current.BindVersion > target.BindVersion {
			return false
		}

		if target.ShardsVersion > current.ShardsVersion ||
			target.BindVersion > current.BindVersion {
			panic("BUG: receive newer shard version than current")
		}
		return true
	}
	return false
}

func (t *table) moveLocked(
	from,
	to string,
) (pb.TableShard, pb.TableShard) {
	for i := range t.shards {
		if t.shards[i].CN == from &&
			t.shards[i].State == pb.ShardState_Running {
			old := t.shards[i]
			old.State = pb.ShardState_Tombstone

			t.shards[i].CN = to
			t.shards[i].BindVersion++
			t.shards[i].State = pb.ShardState_Allocated
			return old, t.shards[i]
		}
	}
	panic("cannot find running shard")
}

func (t *table) allocateCompleted(s pb.TableShard) {
	for i := range t.shards {
		if t.shards[i].Same(s) &&
			t.shards[i].State == pb.ShardState_Allocated {
			t.shards[i].State = pb.ShardState_Running
		}
	}
}

type cn struct {
	id            string
	state         pb.CNState
	incompleteOps []pb.Operator
	notifyOps     []pb.Operator
}

func (c *cn) available(
	tenantID uint32,
	env Env,
) bool {
	if c.state == pb.CNState_Down {
		return false
	}
	return env.Available(tenantID, c.id)
}

func (c *cn) isDown() bool {
	return c.state == pb.CNState_Down
}

func (c *cn) down() {
	c.state = pb.CNState_Down
	c.incompleteOps = c.incompleteOps[:0]
}

func (c *cn) addOps(ops ...pb.Operator) {
	c.incompleteOps = append(c.incompleteOps, ops...)
}

func (c *cn) closeCompletedOp(
	shards []pb.TableShard,
	apply func(pb.TableShard),
) []pb.Operator {
	has := func(s pb.TableShard) bool {
		for _, shard := range shards {
			if s.Same(shard) {
				return true
			}
		}
		return false
	}

	ops := c.incompleteOps[:0]
	c.notifyOps = c.notifyOps[:0]
	for _, op := range c.incompleteOps {
		switch op.Type {
		case pb.OpType_DeleteShard:
			if !has(op.TableShard) {
				continue
			}
			ops = append(ops, op)
			c.notifyOps = append(c.notifyOps, op)
		case pb.OpType_AddShard:
			if has(op.TableShard) {
				apply(op.TableShard)
				continue
			}
			ops = append(ops, op)
			c.notifyOps = append(c.notifyOps, op)
		}
	}
	c.incompleteOps = ops
	return c.notifyOps
}
