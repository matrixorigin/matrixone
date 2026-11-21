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
	"cmp"
	"fmt"
	"slices"

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
	lc                  *logServiceChecker
	ss                  []*stats
	membershipScheduled map[uint64]struct{}
}

func (c checker) membershipChangeInFlight(shardID uint64) bool {
	if _, ok := c.membershipScheduled[shardID]; ok {
		return true
	}
	return len(c.lc.executing.Adding[shardID]) > 0 ||
		len(c.lc.executing.Removing[shardID]) > 0 ||
		len(c.lc.executingNonVoting.Adding[shardID]) > 0 ||
		len(c.lc.executingNonVoting.Removing[shardID]) > 0
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
			executorCandidates []string,
		) (*operator.Operator, error),
		locality pb.Locality,
	) {
		shardIDs := make([]uint64, 0, len(toAdd))
		for shardID := range toAdd {
			shardIDs = append(shardIDs, shardID)
		}
		slices.Sort(shardIDs)
		for _, shardID := range shardIDs {
			if ca.membershipChangeInFlight(shardID) {
				continue
			}
			replicasToAdd := toAdd[shardID]
			numOfLogReplicas := getNumOfLogReplicas(ca.lc.Cluster.LogShards, shardID)
			if numOfLogReplicas == 0 || uint64(normalStats.toAdd[shardID]) == numOfLogReplicas {
				continue
			}
			shardInfo := ca.lc.logState.Shards[shardID]
			if !hasVotingMajority(shardInfo, ca.lc.logState.Stores, ca.lc.expiredStores) {
				continue
			}
			executorCandidates := reportingVotingExecutors(shardInfo, ca.lc.logState.Stores, ca.lc.expiredStores)
			if len(executorCandidates) == 0 {
				continue
			}
			if replicasToAdd <= uint32(len(adding[shardID])) {
				continue
			}
			bestStore := selectStore(shardInfo, ca.lc.workingStores, locality)
			if bestStore == "" {
				runtime.ServiceRuntime(ca.lc.ServiceID).Logger().Error(
					"cannot get store for adding replica", zap.Uint64("shardID", shardID))
				continue
			}
			newReplicaID, ok := ca.lc.Alloc.Next()
			if !ok {
				runtime.ServiceRuntime(ca.lc.ServiceID).Logger().Error(
					"cannot alloc new replica ID", zap.Uint64("shardID", shardID))
				return
			}
			op, err := createFn(bestStore, shardInfo, newReplicaID, executorCandidates)
			if err != nil {
				runtime.ServiceRuntime(ca.lc.ServiceID).Logger().Error(
					"create add replica operator failed", zap.Error(err))
				continue
			}
			ops = append(ops, op)
			if ca.membershipScheduled == nil {
				ca.membershipScheduled = make(map[uint64]struct{})
			}
			ca.membershipScheduled[shardID] = struct{}{}
		}
	}

	for _, s := range ca.ss {
		fn := func(uuid string, shardInfo pb.LogShardInfo, replicaID uint64, candidates []string) (*operator.Operator, error) {
			return operator.CreateAddReplicaWithCandidates(uuid, shardInfo, replicaID, candidates)
		}
		locality := pb.Locality{}
		adding := ca.lc.executing.Adding
		if s.nonVoting {
			fn = func(uuid string, shardInfo pb.LogShardInfo, replicaID uint64, candidates []string) (*operator.Operator, error) {
				return operator.CreateAddNonVotingReplicaWithCandidates(uuid, shardInfo, replicaID, candidates)
			}
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
		voting bool,
		createFn func(uuid string, shardInfo pb.LogShardInfo, executorCandidates []string) (*operator.Operator, error),
	) {
		shardIDs := make([]uint64, 0, len(toRemove))
		for shardID := range toRemove {
			shardIDs = append(shardIDs, shardID)
		}
		slices.Sort(shardIDs)
		for _, shardID := range shardIDs {
			if cr.membershipChangeInFlight(shardID) {
				continue
			}
			replicasToRemove := toRemove[shardID]
			shardInfo := cr.lc.logState.Shards[shardID]
			if !hasVotingMajority(shardInfo, cr.lc.logState.Stores, cr.lc.expiredStores) {
				continue
			}
			if voting {
				eligible := replicasToRemove[:0]
				for _, candidate := range replicasToRemove {
					if canRemoveVotingReplica(
						shardInfo, cr.lc.logState.Stores, cr.lc.expiredStores, candidate,
					) {
						eligible = append(eligible, candidate)
					}
				}
				replicasToRemove = eligible
			} else {
				slices.SortFunc(replicasToRemove, func(a, b replica) int {
					return cmp.Compare(a.replicaID, b.replicaID)
				})
			}
			executorCandidates := reportingVotingExecutors(shardInfo, cr.lc.logState.Stores, cr.lc.expiredStores)
			if len(executorCandidates) == 0 {
				continue
			}
			for _, toRemoveReplica := range replicasToRemove {
				if contains(removing[shardID], toRemoveReplica.replicaID) {
					continue
				}
				op, err := createFn(toRemoveReplica.uuid, shardInfo, executorCandidates)
				if err != nil {
					runtime.ServiceRuntime(cr.lc.ServiceID).Logger().Error(
						"create remove replica operator failed", zap.Error(err))
					continue
				}
				ops = append(ops, op)
				if cr.membershipScheduled == nil {
					cr.membershipScheduled = make(map[uint64]struct{})
				}
				cr.membershipScheduled[shardID] = struct{}{}
				break
			}
		}
	}
	for _, s := range cr.ss {
		fn := func(uuid string, shardInfo pb.LogShardInfo, candidates []string) (*operator.Operator, error) {
			return operator.CreateRemoveReplicaWithCandidates(uuid, shardInfo, candidates)
		}
		removing := cr.lc.executing.Removing
		if s.nonVoting {
			fn = func(uuid string, shardInfo pb.LogShardInfo, candidates []string) (*operator.Operator, error) {
				return operator.CreateRemoveNonVotingReplicaWithCandidates(uuid, shardInfo, candidates)
			}
			removing = cr.lc.executingNonVoting.Removing
		}
		checkFn(s.toRemove, removing, !s.nonVoting, fn)
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

	slices.SortFunc(stores, func(a, b string) int {
		_, ok1 := t.lc.logState.Stores[a]
		_, ok2 := t.lc.logState.Stores[b]
		if !ok1 && !ok2 {
			return 0
		}
		if !ok1 && ok2 {
			return 1
		}
		if ok1 && !ok2 {
			return -1
		}
		return cmp.Compare(t.lc.logState.Stores[b].Tick,
			t.lc.logState.Stores[a].Tick)
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
		lc:                  c,
		ss:                  []*stats{normalStats, nonVotingStats},
		membershipScheduled: make(map[uint64]struct{}),
	}
	checkers := []operatorChecker{
		&checkToBootstrap{checker: commonChecker},
		&checkToRemove{checker: commonChecker},
		&checkToAdd{checker: commonChecker},
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
