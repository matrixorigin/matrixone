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

	"github.com/matrixorigin/matrixone/pkg/common/log"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

// rt is the information about the Shards of the MO cluster maintained
// on the ShardServer, which contains information about which CNs are
// available, which Shards' Tables are available, the distribution of
// Shards and CNs, and so on.
type rt struct {
	sync.RWMutex
	logger *log.MOLogger
	env    Env
	tables map[uint64]*table
	cns    map[string]*cn
}

func newRuntime(
	env Env,
	logger *log.MOLogger,
) *rt {
	return &rt{
		logger: logger,
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
		r.logger.Info("cn added", zap.String("cn", cn))
	}
	if c.isDown() {
		return []pb.Operator{newDeleteAllOp()}
	}

	c.closeCompletedOp(
		shards,
		func(
			shard pb.TableShard,
			replica pb.ShardReplica,
		) {
			if t, ok := r.tables[shard.TableID]; ok {
				t.allocateCompleted(shard, replica)
			}
		},
		func(
			shard pb.TableShard,
			replica pb.ShardReplica,
		) {
			if t, ok := r.tables[shard.TableID]; ok {
				i, j, ok := t.findReplica(shard, replica)
				if ok {
					t.shards[i].Replicas = append(t.shards[i].Replicas[:j], t.shards[i].Replicas[j+1:]...)
				}
			}
		},
	)

	if c.isPause() &&
		len(shards) == 0 &&
		!r.env.Draining(cn) {
		c.resume()
	}

	var ops []pb.Operator
	for _, s := range shards {
		if t, ok := r.tables[s.TableID]; ok {
			for _, r := range s.Replicas {
				if !t.valid(s, r) {
					c.addOps(newDeleteReplicaOp(s, r))
				}
			}
		} else {
			ops = append(ops, newCreateTableOp(s.TableID))
		}
	}
	if len(ops) == 0 &&
		len(c.incompleteOps) == 0 {
		return nil
	}

	return append(ops, c.incompleteOps...)
}

func (r *rt) add(
	t *table,
) {
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
	r.logger.Info("new shard table added", zap.String("metadata", t.metadata.String()))
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

func (r *rt) get(
	id uint64,
) []pb.TableShard {
	r.RLock()
	defer r.RUnlock()

	table, ok := r.tables[id]
	if !ok || table.needAllocate() {
		return nil
	}

	shards := make([]pb.TableShard, 0, len(table.shards))
	shards = append(shards, table.shards...)
	for i, s := range shards {
		shards[i].Replicas = make([]pb.ShardReplica, 0, len(s.Replicas))
		shards[i].Replicas = append(shards[i].Replicas, s.Replicas...)
	}
	return shards
}

func (r *rt) deleteTableLocked(t *table) {
	r.logger.Info("remove table shards",
		zap.Uint64("table", t.id),
		tableShardsField("shards", t.metadata),
		tableShardSliceField("binds", t.shards))

	for _, shard := range t.shards {
		for _, replica := range shard.Replicas {
			if replica.CN == "" {
				continue
			}
			r.addOpLocked(
				replica.CN,
				newDeleteReplicaOp(shard, replica),
			)
		}
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
		if cn.available(t.metadata.AccountID, r.env) &&
			!cn.isPause() {
			cns = append(cns, cn)
		}
	}
	return r.filterCN(cns, filters...)
}

func (r *rt) filterCN(
	cns []*cn,
	filters ...filter,
) []*cn {
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

func (r *rt) hasNotRunningShardLocked(
	cn string,
) bool {
	for _, t := range r.tables {
		for _, s := range t.shards {
			for _, r := range s.Replicas {
				if r.CN == cn &&
					r.State != pb.ReplicaState_Running {
					return true
				}
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
		r.logger.Info("cn removed", zap.String("cn", cn.id))
	}
}

func (r *rt) getPausedCNLocked(cns map[string]struct{}) {
	for _, cn := range r.cns {
		if cn.isPause() {
			cns[cn.id] = struct{}{}
			continue
		}

		if r.env.Draining(cn.id) {
			cn.pause()
			cns[cn.id] = struct{}{}
			continue
		}
	}
}

func (t *table) newShardReplicas(
	count int,
	initVersion uint64,
) []pb.ShardReplica {
	replicas := make([]pb.ShardReplica, 0, count)
	for i := 0; i < count; i++ {
		replicas = append(
			replicas,
			pb.ShardReplica{
				ReplicaID: t.nextReplicaID(),
				State:     pb.ReplicaState_Allocating,
				Version:   initVersion,
			},
		)
	}
	return replicas
}

type table struct {
	id        uint64
	metadata  pb.ShardsMetadata
	shards    []pb.TableShard
	replicaID uint64
}

func newTable(
	id uint64,
	metadata pb.ShardsMetadata,
	initReplicaVersion uint64,
) *table {
	t := &table{
		metadata: metadata,
		id:       id,
	}
	if metadata.MaxReplicaCount == 0 {
		metadata.MaxReplicaCount = 1
	}

	t.shards = make([]pb.TableShard, 0, metadata.ShardsCount)
	for i := uint32(0); i < metadata.ShardsCount; i++ {
		t.shards = append(
			t.shards,
			pb.TableShard{
				Policy:   metadata.Policy,
				TableID:  id,
				Version:  metadata.Version,
				Replicas: t.newShardReplicas(int(metadata.MaxReplicaCount), initReplicaVersion),
			},
		)
	}
	if len(metadata.ShardIDs) != len(t.shards) {
		panic("partition and shard count not match")
	}
	for i, id := range metadata.ShardIDs {
		t.shards[i].ShardID = id
	}
	return t
}

func (t *table) allocate(
	cn string,
	shardIdx int,
	replicaIdx int,
) {
	t.shards[shardIdx].Replicas[replicaIdx].CN = cn
	t.shards[shardIdx].Replicas[replicaIdx].State = pb.ReplicaState_Allocated
	t.shards[shardIdx].Replicas[replicaIdx].Version++
}

func (t *table) needAllocate() bool {
	for _, s := range t.shards {
		for _, r := range s.Replicas {
			if r.State == pb.ReplicaState_Allocating {
				return true
			}
		}
	}
	return false
}

func (t *table) getMoveCompletedReplica() (int, int, bool) {
	for i, s := range t.shards {
		if t.metadata.MaxReplicaCount >= uint32(len(s.Replicas)) {
			continue
		}

		index := -1
		movingCount := 0
		runningCount := 0
		for j, r := range s.Replicas {
			if r.State == pb.ReplicaState_Moving {
				index = j
				movingCount++
			} else if r.State == pb.ReplicaState_Running {
				runningCount++
			}
		}
		if movingCount == 1 &&
			runningCount == len(s.Replicas)-1 {
			return i, index, true
		}
	}
	return 0, 0, false
}

func (t *table) getReplicaCount(cn string) int {
	count := 0
	for _, s := range t.shards {
		for _, r := range s.Replicas {
			if r.CN == cn {
				count++
			}
		}
	}
	return count
}

func (t *table) valid(
	target pb.TableShard,
	replica pb.ShardReplica,
) bool {
	for _, current := range t.shards {
		if current.ShardID != target.ShardID {
			continue
		}
		if current.Version > target.Version {
			return false
		}

		if target.Version > current.Version {
			panic("BUG: receive newer shard version than current")
		}

		for _, r := range current.Replicas {
			if r.ReplicaID != replica.ReplicaID {
				continue
			}

			if r.Version > replica.Version {
				return false
			}

			if replica.Version > r.Version {
				panic("BUG: receive newer shard replica version than current")
			}
		}
		return true
	}
	return false
}

func (t *table) move(
	from func(string) bool,
	to func(string) string,
) (pb.TableShard, pb.ShardReplica, bool) {
	for i := range t.shards {
		for j := range t.shards[i].Replicas {
			if from(t.shards[i].Replicas[j].CN) &&
				t.shards[i].Replicas[j].State == pb.ReplicaState_Running {
				t.shards[i].Replicas[j].State = pb.ReplicaState_Moving
				r := t.addReplica(
					i,
					to(t.shards[i].Replicas[j].CN),
					t.shards[i].Replicas[j].Version,
				)
				return t.shards[i], r, true
			}
		}
	}
	return pb.TableShard{}, pb.ShardReplica{}, false
}

func (t *table) addReplica(
	i int,
	cn string,
	version uint64,
) pb.ShardReplica {
	r := pb.ShardReplica{
		ReplicaID: t.nextReplicaID(),
		State:     pb.ReplicaState_Allocated,
		Version:   version,
		CN:        cn,
	}
	t.shards[i].Replicas = append(
		t.shards[i].Replicas,
		r,
	)
	return r
}

func (t *table) allocateCompleted(
	shard pb.TableShard,
	replica pb.ShardReplica,
) {
	i, j, ok := t.findReplica(shard, replica)
	if ok && t.shards[i].Replicas[j].State == pb.ReplicaState_Allocated {
		t.shards[i].Replicas[j].State = pb.ReplicaState_Running
	}
}

func (t *table) findReplica(
	shard pb.TableShard,
	replica pb.ShardReplica,
) (int, int, bool) {
	for i, s := range t.shards {
		if !shard.Same(s) {
			continue
		}

		for j, r := range s.Replicas {
			if !replica.Same(r) {
				continue
			}
			return i, j, true
		}
		break
	}
	return 0, 0, false
}

func (t *table) nextReplicaID() uint64 {
	t.replicaID++
	return t.replicaID
}

type cn struct {
	id            string
	state         pb.CNState
	incompleteOps []pb.Operator
	notifyOps     []pb.Operator
}

func (c *cn) available(
	tenantID uint64,
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

func (c *cn) pause() {
	c.state = pb.CNState_Pause
}

func (c *cn) resume() {
	c.state = pb.CNState_Up
}

func (c *cn) isPause() bool {
	return c.state == pb.CNState_Pause
}

func (c *cn) addOps(
	ops ...pb.Operator,
) {
	for _, op := range ops {
		if !c.hasSame(op) {
			c.incompleteOps = append(c.incompleteOps, op)
			switch op.Type {
			case pb.OpType_AddReplica:
				v2.AddReplicaOperatorCounter.Inc()
			case pb.OpType_DeleteReplica:
				v2.DeleteReplicaOperatorCounter.Inc()
			}
		}
	}
}

func (c *cn) hasSame(
	op pb.Operator,
) bool {
	for _, old := range c.incompleteOps {
		if old.TableID == op.TableID &&
			old.Type == op.Type {
			switch op.Type {
			case pb.OpType_DeleteReplica, pb.OpType_AddReplica:
				if old.TableShard.Same(op.TableShard) &&
					old.Replica.Same(op.Replica) {
					return true
				}
			default:
				return true
			}
		}
	}
	return false
}

func (c *cn) closeCompletedOp(
	shards []pb.TableShard,
	applyAdd func(pb.TableShard, pb.ShardReplica),
	applyDelete func(pb.TableShard, pb.ShardReplica),
) {
	has := func(
		s pb.TableShard,
		r pb.ShardReplica,
	) bool {
		for _, shard := range shards {
			if s.Same(shard) {
				for _, replica := range shard.Replicas {
					if r.Same(replica) {
						return true
					}
				}
				return false
			}
		}
		return false
	}

	ops := c.incompleteOps[:0]
	c.notifyOps = c.notifyOps[:0]
	for _, op := range c.incompleteOps {
		switch op.Type {
		case pb.OpType_DeleteReplica:
			if !has(op.TableShard, op.Replica) {
				applyDelete(op.TableShard, op.Replica)
				continue
			}
			ops = append(ops, op)
			c.notifyOps = append(c.notifyOps, op)
		case pb.OpType_AddReplica:
			if has(op.TableShard, op.Replica) {
				applyAdd(op.TableShard, op.Replica)
				continue
			}
			ops = append(ops, op)
			c.notifyOps = append(c.notifyOps, op)
		}
	}
	c.incompleteOps = ops
}
