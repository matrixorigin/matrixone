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

package logservice

import (
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/mohae/deepcopy"
	"go.uber.org/zap"
)

type logServiceChecker struct {
	hakeeper.CheckerCommonFields
	logState            pb.LogState
	tnState             pb.TNState
	executing           operator.ExecutingReplicas
	executingNonVoting  operator.ExecutingReplicas
	nonVotingReplicaNum uint64
	nonVotingLocality   pb.Locality
	workingStores       map[string]pb.Locality
	expiredStores       []string
	standbyEnabled      bool
}

func NewLogServiceChecker(
	commonFields hakeeper.CheckerCommonFields,
	logState pb.LogState,
	tnState pb.TNState,
	executing operator.ExecutingReplicas,
	executingNonVoting operator.ExecutingReplicas,
	nonVotingReplicaNum uint64,
	nonVotingLocality pb.Locality,
	standbyEnabled bool,
) hakeeper.ModuleChecker {
	working, expired := parseLogStores(commonFields.Cfg, logState, commonFields.CurrentTick)
	for _, node := range expired {
		runtime.ServiceRuntime(commonFields.ServiceID).Logger().Info(
			"node is expired", zap.String("uuid", node),
		)
	}
	return &logServiceChecker{
		CheckerCommonFields: commonFields,
		logState:            logState,
		tnState:             tnState,
		executing:           executing,
		executingNonVoting:  executingNonVoting,
		nonVotingReplicaNum: nonVotingReplicaNum,
		nonVotingLocality:   nonVotingLocality,
		workingStores:       working,
		expiredStores:       expired,
		standbyEnabled:      standbyEnabled,
	}
}

func getNumOfLogReplicas(logShards []metadata.LogShardRecord, shardID uint64) uint64 {
	var numOfLogReplicas uint64
	for _, logShareRec := range logShards {
		if logShareRec.ShardID == shardID {
			numOfLogReplicas = logShareRec.NumberOfReplicas
			break
		}
	}
	return numOfLogReplicas
}

type operatorChecker interface {
	check() []*operator.Operator
}

type checker struct {
	lc *logServiceChecker
	ss []*stats
}

// checkToBootstrap is different with checkToAdd. checkToBootstrap checks
// if there are some start replicas with initial members, in which case,
// there are no the original replicas.
type checkToBootstrap struct {
	checker
}

func (cb *checkToBootstrap) check() []*operator.Operator {
	if len(cb.ss) != 2 { // do some checks
		return nil
	}
	normalStats := cb.ss[0]

	var ops []*operator.Operator
	for shardID, replicasToAdd := range normalStats.toAdd {
		if _, ok := cb.lc.executing.Bootstrapping[shardID]; ok {
			continue
		}

		numOfLogReplicas := getNumOfLogReplicas(
			cb.lc.Cluster.LogShards,
			shardID,
		)
		// Cannot get the number of log replicas, do nothing.
		if numOfLogReplicas == 0 {
			continue
		}

		// Check if the number of toAdd replicas equals to number of log
		// replicas in config. If it is false, should add replicas but
		// not bootstrap shard.
		if uint64(normalStats.toAdd[shardID]) != numOfLogReplicas {
			continue
		}

		initialMembers := make(map[uint64]string)
		working := deepcopy.Copy(cb.lc.workingStores).(map[string]pb.Locality)
		var ordered []struct {
			store     string
			replicaID uint64
		}
		for replicasToAdd > 0 {
			bestStore := selectStore(
				cb.lc.logState.Shards[shardID],
				working,
				pb.Locality{},
			)
			if bestStore == "" {
				runtime.ServiceRuntime(cb.lc.ServiceID).Logger().Error(
					"cannot get store for bootstrap shard",
					zap.Uint64("shardID", shardID),
				)
				break
			}
			newReplicaID, ok := cb.lc.Alloc.Next()
			if !ok {
				runtime.ServiceRuntime(cb.lc.ServiceID).Logger().Error(
					"cannot alloc new replica ID",
					zap.Uint64("shardID", shardID),
				)
				return nil
			}
			initialMembers[newReplicaID] = bestStore
			ordered = append(ordered, struct {
				store     string
				replicaID uint64
			}{store: bestStore, replicaID: newReplicaID})
			replicasToAdd--
			delete(working, bestStore)
		}

		if uint64(len(initialMembers)) != numOfLogReplicas {
			runtime.ServiceRuntime(cb.lc.ServiceID).Logger().Error(
				"length of initial members is not equal to num of log replicas",
				zap.Uint64("shardID", shardID),
			)
			return nil
		}

		for _, item := range ordered {
			ops = append(ops, operator.CreateBootstrapShard(
				fmt.Sprintf("create bootstrap shard %d", shardID),
				item.store,
				shardID,
				item.replicaID,
				initialMembers,
			))
		}

	}
	return ops
}

// checkToAdd checks if there are some replicas need to add to existing
// shard and there are already some replicas in the shards.
type checkToAdd struct {
	checker
}

func (ca *checkToAdd) check() []*operator.Operator {
	if len(ca.ss) != 2 { // do some checks
		return nil
	}
	normalStats := ca.ss[0]

	var ops []*operator.Operator
	checkFn := func(
		toAdd map[uint64]uint32,
		adding map[uint64][]uint64,
		createFn func(
			uuid string,
			shardInfo pb.LogShardInfo,
			replicaID uint64,
		) (*operator.Operator, error),
		locality pb.Locality,
	) {
		for shardID, replicasToAdd := range toAdd {
			numOfLogReplicas := getNumOfLogReplicas(
				ca.lc.Cluster.LogShards,
				shardID,
			)
			// Cannot get the number of log replicas, do nothing.
			if numOfLogReplicas == 0 {
				continue
			}

			// Check if the number of toAdd replicas equals to number of log
			// replicas in config. If it is true, should bootstrap the replicas
			// but not add replicas.
			if uint64(normalStats.toAdd[shardID]) == numOfLogReplicas {
				continue
			}

			working := deepcopy.Copy(ca.lc.workingStores).(map[string]pb.Locality)
			for replicasToAdd > uint32(len(adding[shardID])) {
				bestStore := selectStore(
					ca.lc.logState.Shards[shardID],
					ca.lc.workingStores,
					locality,
				)
				if bestStore == "" {
					runtime.ServiceRuntime(ca.lc.ServiceID).Logger().Error(
						"cannot get store for adding replica",
						zap.Uint64("shardID", shardID),
					)
					break
				}
				newReplicaID, ok := ca.lc.Alloc.Next()
				if !ok {
					runtime.ServiceRuntime(ca.lc.ServiceID).Logger().Error(
						"cannot alloc new replica ID",
						zap.Uint64("shardID", shardID),
					)
					return
				}
				if op, err := createFn(
					bestStore,
					ca.lc.logState.Shards[shardID],
					newReplicaID,
				); err != nil {
					runtime.ServiceRuntime(ca.lc.ServiceID).Logger().Error(
						"create add replica operator failed", zap.Error(err))
					// may be no more stores, skip this shard
					break
				} else {
					ops = append(ops, op)
					replicasToAdd--
					delete(working, bestStore)
				}
			}
		}
	}

	for _, s := range ca.ss {
		fn := operator.CreateAddReplica
		locality := pb.Locality{}
		adding := ca.lc.executing.Adding
		if s.nonVoting {
			fn = operator.CreateAddNonVotingReplica
			adding = ca.lc.executingNonVoting.Adding
			locality = ca.lc.nonVotingLocality
		}
		checkFn(s.toAdd, adding, fn, locality)
	}
	return ops
}

type checkToRemove struct {
	checker
}

func (cr *checkToRemove) check() []*operator.Operator {
	var ops []*operator.Operator
	checkFn := func(
		toRemove map[uint64][]replica,
		removing map[uint64][]uint64,
		createFn func(
			uuid string,
			shardInfo pb.LogShardInfo,
		) (*operator.Operator, error),
	) {
		for shardID, replicasToRemove := range toRemove {
			for _, toRemoveReplica := range replicasToRemove {
				if contains(removing[shardID], toRemoveReplica.replicaID) {
					continue
				}
				if op, err := createFn(
					toRemoveReplica.uuid,
					cr.lc.logState.Shards[toRemoveReplica.shardID],
				); err != nil {
					runtime.ServiceRuntime(cr.lc.ServiceID).Logger().Error(
						"create remove replica operator failed", zap.Error(err))
					// skip this replica
					continue
				} else {
					ops = append(ops, op)
				}
			}
		}
	}
	for _, s := range cr.ss {
		fn := operator.CreateRemoveReplica
		removing := cr.lc.executing.Removing
		if s.nonVoting {
			fn = operator.CreateRemoveNonVotingReplica
			removing = cr.lc.executingNonVoting.Removing
		}
		checkFn(s.toRemove, removing, fn)
	}
	return ops
}

type checkToStart struct {
	checker
}

func (cs *checkToStart) check() []*operator.Operator {
	var ops []*operator.Operator
	checkFn := func(
		toStart []replica,
		starting map[uint64][]uint64,
		createFn func(brief, uuid string, shardID, replicaID uint64) *operator.Operator,
	) {
		for _, replicaToStart := range toStart {
			if contains(starting[replicaToStart.shardID], replicaToStart.replicaID) {
				continue
			}
			ops = append(ops, createFn(
				"",
				replicaToStart.uuid,
				replicaToStart.shardID,
				replicaToStart.replicaID,
			))
		}
	}
	for _, s := range cs.ss {
		fn := operator.CreateStartReplica
		starting := cs.lc.executing.Starting
		if s.nonVoting {
			fn = operator.CreateStartNonVotingReplica
			starting = cs.lc.executingNonVoting.Starting
		}
		checkFn(s.toStart, starting, fn)
	}
	return ops
}

type checkZombie struct {
	checker
}

func (cz *checkZombie) check() []*operator.Operator {
	var ops []*operator.Operator
	for _, s := range cz.ss {
		for _, zombie := range s.zombies {
			ops = append(ops, operator.CreateKillZombie("",
				zombie.uuid, zombie.shardID, zombie.replicaID))
		}
	}
	return ops
}

type checkTaskService struct {
	checker
}

func (t *checkTaskService) check() []*operator.Operator {
	var ops []*operator.Operator
	if t.lc.User.Username != "" {
		for store := range t.lc.workingStores {
			if !t.lc.logState.Stores[store].TaskServiceCreated {
				ops = append(ops, operator.CreateTaskServiceOp("",
					store, pb.LogService, t.lc.User))
			}
		}
	}
	return ops
}

type checkAddShard struct {
	checker
	standbyEnabled bool
}

func (t *checkAddShard) check() []*operator.Operator {
	if !t.standbyEnabled {
		return nil
	}

	// TODO(volgariver6): standby shard is the third shard:
	//   0: hakeeper
	//   1: tn data shard
	//   2: standby data shard
	// So, check if there are three shards already.
	requiredShardNum := 3

	// the shard num is equal to the required, return directly.
	if len(t.lc.logState.Shards) == requiredShardNum {
		return nil
	}

	// there is no working store yet.
	if len(t.lc.workingStores) == 0 {
		return nil
	}

	stores := make([]string, 0, len(t.lc.workingStores))
	for store := range t.lc.workingStores {
		stores = append(stores, store)
	}

	sort.Slice(stores, func(i, j int) bool {
		_, ok1 := t.lc.logState.Stores[stores[i]]
		_, ok2 := t.lc.logState.Stores[stores[j]]
		if !ok1 && !ok2 {
			return false
		}
		if !ok1 && ok2 {
			return false
		}
		if ok1 && !ok2 {
			return true
		}
		return t.lc.logState.Stores[stores[i]].Tick >
			t.lc.logState.Stores[stores[j]].Tick
	})

	var maxShardID uint64
	for shardID := range t.lc.logState.Shards {
		if shardID > maxShardID {
			maxShardID = shardID
		}
	}
	for _, storeInfo := range t.lc.tnState.Stores {
		for _, shardInfo := range storeInfo.Shards {
			if shardInfo.ShardID > maxShardID {
				maxShardID = shardInfo.ShardID
			}
		}
	}
	return []*operator.Operator{
		operator.CreateAddShardOp("", stores[0], maxShardID+1),
	}
}

func (c *logServiceChecker) Check() (operators []*operator.Operator) {
	normalStats, nonVotingStats := parseLogShards(
		c.Cluster,
		c.logState,
		c.expiredStores,
		c.nonVotingReplicaNum,
	)
	commonChecker := checker{
		lc: c,
		ss: []*stats{
			normalStats,
			nonVotingStats,
		},
	}
	checkers := []operatorChecker{
		&checkToBootstrap{checker: commonChecker},
		&checkToAdd{checker: commonChecker},
		&checkToRemove{checker: commonChecker},
		&checkToStart{checker: commonChecker},
		&checkZombie{checker: commonChecker},
		&checkTaskService{checker: commonChecker},
		&checkAddShard{
			checker:        commonChecker,
			standbyEnabled: c.standbyEnabled,
		},
	}
	for _, ck := range checkers {
		operators = append(operators, ck.check()...)
	}
	return operators
}

func contains[T comparable](slice []T, v T) bool {
	for i := range slice {
		if slice[i] == v {
			return true
		}
	}
	return false
}
