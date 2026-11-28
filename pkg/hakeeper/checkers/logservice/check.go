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
	lc                 *logServiceChecker
	ss                 []*stats
	membershipSelected map[uint64]uint64 // selected removal replica, or zero for an add
}

func (c checker) membershipChangeInFlight(shardID uint64) bool {
	_, selected := c.membershipSelected[shardID]
	return selected || len(c.lc.executing.Adding[shardID]) > 0 ||
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
		createFn func(
			uuid string,
			shardInfo pb.LogShardInfo,
			replicaID uint64,
			target string,
		) (*operator.Operator, error),
		locality pb.Locality,
		change pb.ConfigChangeType,
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

			shard := ca.lc.logState.Shards[shardID]
			bestStore := selectStore(shard, ca.lc.workingStores, locality)
			if bestStore == "" {
				runtime.ServiceRuntime(ca.lc.ServiceID).Logger().Error(
					"cannot get store for adding replica", zap.Uint64("shardID", shardID))
				continue
			}
			targets := hakeeper.LogMembershipTargets(ca.lc.Cfg, ca.lc.logState, ca.lc.CurrentTick,
				change, pb.Replica{ShardID: shardID, UUID: bestStore, Epoch: shard.Epoch})
			if len(targets) == 0 {
				continue
			}
			newReplicaID, ok := ca.lc.Alloc.Next()
			if !ok {
				runtime.ServiceRuntime(ca.lc.ServiceID).Logger().Error(
					"cannot alloc new replica ID", zap.Uint64("shardID", shardID))
				return
			}
			op, err := createFn(bestStore, shard, newReplicaID, targets[0])
			if err != nil {
				runtime.ServiceRuntime(ca.lc.ServiceID).Logger().Error(
					"create add replica operator failed", zap.Error(err))
				continue
			}
			ops = append(ops, op)
			ca.membershipSelected[shardID] = 0
		}
	}

	for _, s := range ca.ss {
		fn := operator.CreateAddReplica
		change := pb.AddReplica
		locality := pb.Locality{}
		if s.nonVoting {
			fn = operator.CreateAddNonVotingReplica
			change = pb.AddNonVotingReplica
			locality = ca.lc.nonVotingLocality
		}
		checkFn(s.toAdd, fn, locality, change)
	}
	return ops
}

type checkToRemove struct {
	checker
}

func replicaIsStarting(voting, nonVoting operator.ExecutingReplicas, shardID, replicaID uint64) bool {
	return slices.Contains(voting.Starting[shardID], replicaID) ||
		slices.Contains(nonVoting.Starting[shardID], replicaID)
}

func (cr *checkToRemove) check() []*operator.Operator {
	var ops []*operator.Operator
	checkFn := func(
		toRemove map[uint64][]replica,
		createFn func(
			uuid string,
			shardInfo pb.LogShardInfo,
			target string,
		) (*operator.Operator, error),
		change pb.ConfigChangeType,
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
			shard := cr.lc.logState.Shards[shardID]
			reported := func(r replica) bool {
				_, live := cr.lc.workingStores[r.uuid]
				return live && !hakeeper.LogReplicaIncarnationChanged(r.replicaID, r.uuid, shard.ReplicaStoreIncarnations, cr.lc.logState.Stores) &&
					hakeeper.LogReplicaReported(shardID, r.replicaID, change == pb.RemoveNonVotingReplica, cr.lc.logState.Stores[r.uuid].Replicas)
			}
			slices.SortFunc(replicasToRemove, func(a, b replica) int {
				aReported, bReported := reported(a), reported(b)
				if aReported != bReported {
					if aReported {
						return 1
					}
					return -1
				}
				return cmp.Compare(a.replicaID, b.replicaID)
			})
			for _, toRemoveReplica := range replicasToRemove {
				if replicaIsStarting(cr.lc.executing, cr.lc.executingNonVoting, shardID, toRemoveReplica.replicaID) {
					continue
				}
				targets := hakeeper.LogMembershipTargets(cr.lc.Cfg, cr.lc.logState, cr.lc.CurrentTick,
					change, pb.Replica{ShardID: shardID, ReplicaID: toRemoveReplica.replicaID, UUID: toRemoveReplica.uuid, Epoch: shard.Epoch})
				if len(targets) == 0 {
					continue
				}
				if op, err := createFn(
					toRemoveReplica.uuid,
					cr.lc.logState.Shards[toRemoveReplica.shardID],
					targets[0],
				); err != nil {
					runtime.ServiceRuntime(cr.lc.ServiceID).Logger().Error(
						"create remove replica operator failed", zap.Error(err))
					// skip this replica
					continue
				} else {
					ops = append(ops, op)
					cr.membershipSelected[shardID] = toRemoveReplica.replicaID
					break
				}
			}
		}
	}
	for _, s := range cr.ss {
		fn := operator.CreateRemoveReplica
		change := pb.RemoveReplica
		if s.nonVoting {
			fn = operator.CreateRemoveNonVotingReplica
			change = pb.RemoveNonVotingReplica
		}
		checkFn(s.toRemove, fn, change)
	}
	return ops
}

type checkToStart struct {
	checker
}

func (cs *checkToStart) check() []*operator.Operator {
	var ops []*operator.Operator
	checkFn := func(
		stats *stats,
		starting map[uint64][]uint64,
		createFn func(brief, uuid string, shardID, replicaID uint64) *operator.Operator,
	) {
		for _, replicaToStart := range stats.toStart {
			if _, live := cs.lc.workingStores[replicaToStart.uuid]; !live {
				continue
			}
			if contains(starting[replicaToStart.shardID], replicaToStart.replicaID) {
				continue
			}
			if contains(cs.lc.executing.Removing[replicaToStart.shardID], replicaToStart.replicaID) ||
				contains(cs.lc.executingNonVoting.Removing[replicaToStart.shardID], replicaToStart.replicaID) {
				continue
			}
			// Only a selected removal conflicts; another surplus replica can
			// still be started while this shard's membership change is pending.
			if id, selected := cs.membershipSelected[replicaToStart.shardID]; selected && id == replicaToStart.replicaID {
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
		checkFn(s, starting, fn)
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

	// Only registered shards satisfy the configured standby requirement.
	if len(t.lc.Cluster.LogShards) >= requiredShardNum {
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
		if byTick := cmp.Compare(t.lc.logState.Stores[b].Tick, t.lc.logState.Stores[a].Tick); byTick != 0 {
			return byTick
		}
		return cmp.Compare(a, b)
	})

	var maxShardID uint64
	for _, record := range t.lc.Cluster.LogShards {
		if record.ShardID > maxShardID {
			maxShardID = record.ShardID
		}
	}
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
	for _, s := range commonChecker.ss {
		if len(s.toAdd) != 0 || len(s.toRemove) != 0 {
			commonChecker.membershipSelected = make(map[uint64]uint64)
			break
		}
	}
	checkers := []operatorChecker{
		&checkToBootstrap{checker: commonChecker},
		&checkZombie{checker: commonChecker},
		&checkToRemove{checker: commonChecker},
		&checkToAdd{checker: commonChecker},
		&checkToStart{checker: commonChecker},
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
