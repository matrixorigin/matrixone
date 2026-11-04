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
	"cmp"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

type state struct {
	// waitingShards makes check logic stateful.
	waitingShards *initialShards

	// bootstrapping indicates the tn is bootstrapping.
	// When tn checker finds a new tn shard should be added,
	// it waits for a while if bootstrapping is false to avoid thrashing.
	// If bootstrapping is true, tn checker will construct create tn shard command immediately.
	// This flag helps to accelarate cluster bootstrapping.
	bootstrapping bool
}

func InitCheckState(
	sid string,
) {
	s := &state{
		waitingShards: newInitialShards(sid),
		bootstrapping: true,
	}

	runtime.ServiceRuntime(sid).SetGlobalVariables("log_service_init_state", s)
}

func getCheckState(
	sid string,
) *state {
	s, ok := runtime.ServiceRuntime(sid).GetGlobalVariables("log_service_init_state")
	if !ok {
		panic("log service init state not found: <" + sid + ">")
	}
	return s.(*state)
}

type tnServiceChecker struct {
	hakeeper.CheckerCommonFields
	tnState    pb.TNState
	controller *operator.Controller
}

func NewTNServiceChecker(
	commonFields hakeeper.CheckerCommonFields,
	tnState pb.TNState,
	controller *operator.Controller,
) hakeeper.ModuleChecker {
	return &tnServiceChecker{
		CheckerCommonFields: commonFields,
		tnState:             tnState,
		controller:          controller,
	}
}

// Check checks tn state and generate operator for expired tn store.
// The less shard ID, the higher priority.
// NB: the returned order should be deterministic.
func (c *tnServiceChecker) Check() []*operator.Operator {
	stores, reportedShards := parseTnState(c.Cfg, c.tnState, c.CurrentTick)
	runtime.ServiceRuntime(c.ServiceID).Logger().Debug("reported tn shards in cluster",
		zap.Any("dn shard IDs", reportedShards.shardIDs),
		zap.Any("dn shards", reportedShards.shards),
	)
	for _, node := range stores.ExpiredStores() {
		runtime.ServiceRuntime(c.ServiceID).Logger().Info("node is expired", zap.String("uuid", node.ID))
	}
	pending := c.pendingStarts(stores, reportedShards)
	if len(stores.WorkingStores()) < 1 {
		runtime.ServiceRuntime(c.ServiceID).Logger().Warn("no working tn stores")
		return nil
	}

	mapper := parseClusterInfo(c.Cluster)

	var operators []*operator.Operator
	for _, record := range c.Cluster.TNShards {
		if op := pending[record.ShardID]; op != nil {
			operators = append(operators, op)
		}
	}
	slices.SortFunc(operators, func(a, b *operator.Operator) int {
		return cmp.Compare(a.ShardID(), b.ShardID())
	})

	// 1. check reported tn state
	operators = append(operators,
		checkReportedState(c.ServiceID, reportedShards, mapper, stores.WorkingStores(), c.Alloc, pending)...,
	)

	// 2. check expected tn state
	operators = append(operators,
		checkInitiatingShards(
			c.ServiceID,
			reportedShards,
			mapper,
			stores.WorkingStores(),
			c.Alloc,
			c.Cluster,
			c.Cfg,
			c.CurrentTick, pending)...,
	)

	if c.User.Username != "" {
		for _, store := range stores.WorkingStores() {
			if !c.tnState.Stores[store.ID].TaskServiceCreated {
				operators = append(operators, operator.CreateTaskServiceOp("",
					store.ID, pb.TNService, c.User))
			}
		}
	}

	return operators
}

// pendingStarts reconciles the controller's existing start attempts with real
// reports. The per-check map is an index, not another owner of retry state.
func (c *tnServiceChecker) pendingStarts(stores *util.ClusterStores, reports *reportedShards) map[uint64]*operator.Operator {
	pending := make(map[uint64]*operator.Operator)
	working := make(map[string]*util.Store, len(stores.WorkingStores()))
	for _, store := range stores.WorkingStores() {
		working[store.ID] = store
	}
	for _, record := range c.Cluster.TNShards {
		// Removal mutates the registered slice, so walk a snapshot of its pointers.
		for _, op := range slices.Clone(c.controller.GetOperators(record.ShardID)) {
			steps := op.OpSteps()
			if len(steps) != 1 {
				continue
			}
			start, ok := steps[0].(operator.AddTnReplica)
			if !ok {
				continue
			}
			shard, err := reports.getShard(record.ShardID)
			reported := err == nil && len(shard.workingReplicas()) != 0
			if op.IsEnd() || working[start.StoreID] == nil || reported || pending[record.ShardID] != nil {
				// Another replica ID can already own this shard: the receiver is
				// shard-keyed and cannot replace it by replaying an obsolete start.
				c.controller.RemoveOperator(op)
				continue
			}
			pending[record.ShardID] = op
			working[start.StoreID].Length++
		}
	}
	return pending
}

// schedule generator operator as much as possible
// NB: the returned order should be deterministic.
func checkShard(
	service string,
	shard *tnShard,
	mapper ShardMapper,
	workingStores []*util.Store,
	idAlloc util.IDAllocator,
) []operator.OpStep {
	switch len(shard.workingReplicas()) {
	case 0: // need add replica
		newReplicaID, ok := idAlloc.Next()
		if !ok {
			runtime.ServiceRuntime(service).Logger().Warn("fail to allocate replica ID")
			return nil
		}

		target, err := consumeLeastSpareStore(workingStores)
		if err != nil {
			runtime.ServiceRuntime(service).Logger().Warn("no working tn stores")
			return nil
		}

		logShardID, err := mapper.getLogShardID(shard.shardID)
		if err != nil {
			runtime.ServiceRuntime(service).Logger().Warn("shard not registered", zap.Uint64("ShardID", shard.shardID))
			return nil
		}

		s := newAddStep(
			target, shard.shardID, newReplicaID, logShardID,
		)
		runtime.ServiceRuntime(service).Logger().Info(s.String())
		return []operator.OpStep{s}

	case 1: // ignore expired replicas
		return nil

	default: // remove extra working replicas
		replicas := extraWorkingReplicas(shard)
		steps := make([]operator.OpStep, 0, len(replicas))

		logShardID, err := mapper.getLogShardID(shard.shardID)
		if err != nil {
			runtime.ServiceRuntime(service).Logger().Warn("shard not registered", zap.Uint64("ShardID", shard.shardID))
			return nil
		}

		for _, r := range replicas {
			s := newRemoveStep(
				r.storeID, r.shardID, r.replicaID, logShardID,
			)
			runtime.ServiceRuntime(service).Logger().Info(s.String())
			steps = append(steps, s)
		}
		return steps
	}
}

// newAddStep constructs operator to launch a tn shard replica
func newAddStep(target string, shardID, replicaID, logShardID uint64) operator.OpStep {
	return operator.AddTnReplica{
		StoreID:    target,
		ShardID:    shardID,
		ReplicaID:  replicaID,
		LogShardID: logShardID,
	}
}

// newRemoveStep constructs operator to remove a tn shard replica
func newRemoveStep(target string, shardID, replicaID, logShardID uint64) operator.OpStep {
	return operator.RemoveTnReplica{
		StoreID:    target,
		ShardID:    shardID,
		ReplicaID:  replicaID,
		LogShardID: logShardID,
	}
}

// expiredReplicas return all expired replicas.
// NB: the returned order should be deterministic.
func expiredReplicas(shard *tnShard) []*tnReplica {
	expired := shard.expiredReplicas()
	// less replica first
	slices.SortFunc(expired, func(a, b *tnReplica) int {
		return cmp.Compare(a.replicaID, b.replicaID)
	})
	return expired
}

// extraWorkingReplicas return all working replicas except the largest.
// NB: the returned order should be deterministic.
func extraWorkingReplicas(shard *tnShard) []*tnReplica {
	working := shard.workingReplicas()
	if len(working) == 0 {
		return working
	}

	// less replica first
	slices.SortFunc(working, func(a, b *tnReplica) int {
		return cmp.Compare(a.replicaID, b.replicaID)
	})

	return working[0 : len(working)-1]
}

// consumeLeastSpareStore consume a slot from the least spare tn store.
// If there are multiple tn store with the same least slots,
// the store with less ID would be chosen.
// NB: the returned result should be deterministic.
func consumeLeastSpareStore(working []*util.Store) (string, error) {
	if len(working) == 0 {
		return "", moerr.NewNoWorkingStoreNoCtx()
	}

	// the least shards, the higher priority
	slices.SortFunc(working, func(a, b *util.Store) int {
		return cmp.Compare(a.Length, b.Length)
	})

	// stores with the same Length
	var leastStores []*util.Store

	least := working[0].Length
	for i := 0; i < len(working); i++ {
		store := working[i]
		if least != store.Length {
			break
		}
		leastStores = append(leastStores, store)
	}
	slices.SortFunc(leastStores, func(a, b *util.Store) int {
		return cmp.Compare(a.ID, b.ID)
	})

	// consume a slot from this tn store
	leastStores[0].Length += 1

	return leastStores[0].ID, nil
}
