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

package tnservice

import (
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

const (
	TnStoreCapacity = 32
)

// ShardMapper used to get log shard ID for tn shard
type ShardMapper interface {
	getLogShardID(tnShardID uint64) (uint64, error)
}

// tnShardToLogShard implements interface `ShardMapper`
type tnShardToLogShard map[uint64]uint64

// parseClusterInfo parses information from `pb.ClusterInfo`
func parseClusterInfo(cluster pb.ClusterInfo) tnShardToLogShard {
	m := make(map[uint64]uint64)
	for _, r := range cluster.TNShards {
		// warning with duplicated tn shard ID
		m[r.ShardID] = r.LogShardID
	}
	return m
}

// getLogShardID implements interface `ShardMapper`
func (d tnShardToLogShard) getLogShardID(tnShardID uint64) (uint64, error) {
	if logShardID, ok := d[tnShardID]; ok {
		return logShardID, nil
	}
	return 0, moerr.NewInvalidStateNoCtxf("shard %d not recorded", tnShardID)
}

// parseTnState parses cluster tn state.
func parseTnState(cfg hakeeper.Config,
	tnState pb.TNState, currTick uint64,
) (*util.ClusterStores, *reportedShards) {
	stores := util.NewClusterStores()
	shards := newReportedShards()

	for storeID, storeInfo := range tnState.Stores {
		expired := false
		if cfg.TNStoreExpired(storeInfo.Tick, currTick) {
			expired = true
		}

		store := util.NewStore(storeID, len(storeInfo.Shards), TnStoreCapacity)
		if expired {
			stores.RegisterExpired(store)
		} else {
			stores.RegisterWorking(store)
		}

		for _, shard := range storeInfo.Shards {
			replica := newReplica(shard.ReplicaID, shard.ShardID, storeID)
			shards.registerReplica(replica, expired)
		}
	}

	return stores, shards
}

// checkReportedState generates Operators for reported state.
// NB: the order of list is deterministic.
func checkReportedState(
	service string,
	rs *reportedShards,
	mapper ShardMapper,
	workingStores []*util.Store,
	idAlloc util.IDAllocator,
) []*operator.Operator {
	var ops []*operator.Operator

	reported := rs.listShards()
	// keep order of all shards deterministic
	sort.Slice(reported, func(i, j int) bool {
		return reported[i] < reported[j]
	})

	for _, shardID := range reported {
		shard, err := rs.getShard(shardID)
		if err != nil {
			// error should be always nil
			panic(fmt.Sprintf("shard `%d` not register", shardID))
		}

		steps := checkShard(service, shard, mapper, workingStores, idAlloc)
		// avoid Operator with nil steps
		if len(steps) > 0 {
			ops = append(ops,
				operator.NewOperator("tnservice", shardID, operator.NoopEpoch, steps...),
			)
		}
	}

	runtime.ServiceRuntime(service).Logger().Debug(fmt.Sprintf("construct %d operators for reported tn shards", len(ops)))

	return ops
}

// checkInitiatingShards generates Operators for newly-created shards.
// NB: the order of list is deterministic.
func checkInitiatingShards(
	service string,
	rs *reportedShards,
	mapper ShardMapper,
	workingStores []*util.Store,
	idAlloc util.IDAllocator,
	cluster pb.ClusterInfo,
	cfg hakeeper.Config,
	currTick uint64,
) []*operator.Operator {
	// update the registered newly-created shards
	for _, record := range cluster.TNShards {
		shardID := record.ShardID
		_, err := rs.getShard(shardID)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrShardNotReported) {
				// if a shard not reported, register it,
				// and launch its replica after a while.
				getCheckState(service).waitingShards.register(shardID, currTick)
			}
			continue
		}
		// shard reported via heartbeat, no need to wait
		getCheckState(service).waitingShards.remove(shardID)
	}

	// list newly-created shards which had been waiting for a while
	expired := getCheckState(service).waitingShards.listEligibleShards(func(start uint64) bool {
		return cfg.TNStoreExpired(start, currTick)
	})

	var ops []*operator.Operator
	for _, id := range expired {
		steps := checkShard(service, newTnShard(id), mapper, workingStores, idAlloc)
		if len(steps) > 0 { // avoid Operator with nil steps
			ops = append(ops,
				operator.NewOperator("tnservice", id, operator.NoopEpoch, steps...),
			)
		}
	}

	runtime.ServiceRuntime(service).Logger().Debug(fmt.Sprintf("construct %d operators for initiating tn shards", len(ops)))
	if getCheckState(service).bootstrapping && len(ops) != 0 {
		getCheckState(service).bootstrapping = false
	}

	return ops
}

type earliestTick struct {
	tick uint64
}

// initialShards records all fresh tn shards.
type initialShards struct {
	sid    string
	shards map[uint64]earliestTick
}

func newInitialShards(sid string) *initialShards {
	return &initialShards{
		sid:    sid,
		shards: make(map[uint64]earliestTick),
	}
}

// register records initial shard with its oldest tick.
func (w *initialShards) register(shardID, currTick uint64) bool {
	if earliest, ok := w.shards[shardID]; ok {
		if currTick >= earliest.tick {
			return false
		}
	}
	// newly registered or updated with older tick
	w.shards[shardID] = earliestTick{tick: currTick}
	return true
}

// remove deletes shard from the recorded fresh shards.
func (w *initialShards) remove(shardID uint64) bool {
	if _, ok := w.shards[shardID]; ok {
		delete(w.shards, shardID)
		return true
	}
	return false
}

// listEligibleShards lists all shards that `fn` returns true.
// NB: the order of list isn't deterministic.
func (w *initialShards) listEligibleShards(fn func(tick uint64) bool) []uint64 {
	ids := make([]uint64, 0)
	for id, earliest := range w.shards {
		if getCheckState(w.sid).bootstrapping || fn(earliest.tick) {
			ids = append(ids, id)
		}
	}
	return ids
}

// clear clears all record.
func (w *initialShards) clear() {
	w.shards = make(map[uint64]earliestTick)
}

// reportedShards collects all reported tn shards.
type reportedShards struct {
	shards   map[uint64]*tnShard
	shardIDs []uint64
}

func newReportedShards() *reportedShards {
	return &reportedShards{
		shards: make(map[uint64]*tnShard),
	}
}

// registerReplica collects tn shard replicas by their status.
func (rs *reportedShards) registerReplica(replica *tnReplica, expired bool) {
	shardID := replica.shardID
	if _, ok := rs.shards[shardID]; !ok {
		rs.shardIDs = append(rs.shardIDs, shardID)
		rs.shards[shardID] = newTnShard(shardID)
	}
	rs.shards[shardID].register(replica, expired)
}

// listShards lists all the shard IDs.
// NB: the returned order isn't deterministic.
func (rs *reportedShards) listShards() []uint64 {
	return rs.shardIDs
}

// getShard returns tn shard by shard ID.
func (rs *reportedShards) getShard(shardID uint64) (*tnShard, error) {
	if shard, ok := rs.shards[shardID]; ok {
		return shard, nil
	}
	return nil, moerr.NewShardNotReportedNoCtx("", shardID)
}

// tnShard records metadata for tn shard.
type tnShard struct {
	shardID uint64
	expired []*tnReplica
	working []*tnReplica
}

func newTnShard(shardID uint64) *tnShard {
	return &tnShard{
		shardID: shardID,
	}
}

// register collects tn shard replica.
func (s *tnShard) register(replica *tnReplica, expired bool) {
	if expired {
		s.expired = append(s.expired, replica)
	} else {
		s.working = append(s.working, replica)
	}
}

// workingReplicas returns all working replicas.
// NB: the returned order isn't deterministic.
func (s *tnShard) workingReplicas() []*tnReplica {
	return s.working
}

// workingReplicas returns all expired replicas.
// NB: the returned order isn't deterministic.
func (s *tnShard) expiredReplicas() []*tnReplica {
	return s.expired
}

// tnReplica records metadata for tn shard replica
type tnReplica struct {
	replicaID uint64
	shardID   uint64
	storeID   string
}

func newReplica(
	replicaID, shardID uint64, storeID string,
) *tnReplica {
	return &tnReplica{
		replicaID: replicaID,
		shardID:   shardID,
		storeID:   storeID,
	}
}
