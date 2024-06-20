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
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

type logServiceChecker struct {
	hakeeper.CheckerCommonFields
	infos               pb.LogState
	executing           operator.ExecutingReplicas
	executingNonVoting  operator.ExecutingReplicas
	nonVotingReplicaNum uint64
	nonVotingLocality   pb.Locality
	workingStores       map[string]pb.Locality
	expiredStores       []string
}

func NewLogServiceChecker(
	commonFields hakeeper.CheckerCommonFields,
	infos pb.LogState,
	executing operator.ExecutingReplicas,
	executingNonVoting operator.ExecutingReplicas,
	nonVotingReplicaNum uint64,
	nonVotingLocality pb.Locality,
) hakeeper.ModuleChecker {
	working, expired := parseLogStores(commonFields.Cfg, infos, commonFields.CurrentTick)
	for _, node := range expired {
		runtime.ServiceRuntime(commonFields.ServiceID).Logger().Info("node is expired", zap.String("uuid", node))
	}
	return &logServiceChecker{
		CheckerCommonFields: commonFields,
		infos:               infos,
		executing:           executing,
		executingNonVoting:  executingNonVoting,
		nonVotingReplicaNum: nonVotingReplicaNum,
		nonVotingLocality:   nonVotingLocality,
		workingStores:       working,
		expiredStores:       expired,
	}
}

type operatorChecker interface {
	check() []*operator.Operator
}

type checker struct {
	lc *logServiceChecker
	ss []*stats
}

type checkToAdd struct {
	checker
}

func (ca *checkToAdd) check() []*operator.Operator {
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
			for replicasToAdd > uint32(len(adding[shardID])) {
				bestStore := selectStore(ca.lc.infos.Shards[shardID], ca.lc.workingStores, locality)
				if bestStore == "" {
					runtime.ServiceRuntime(ca.lc.ServiceID).Logger().Error(
						"cannot get store for adding replica", zap.Uint64("shardID", shardID))
					break
				}
				newReplicaID, ok := ca.lc.Alloc.Next()
				if !ok {
					return
				}
				if op, err := createFn(bestStore, ca.lc.infos.Shards[shardID], newReplicaID); err != nil {
					runtime.ServiceRuntime(ca.lc.ServiceID).Logger().Error(
						"create add replica operator failed", zap.Error(err))
					// may be no more stores, skip this shard
					break
				} else {
					ops = append(ops, op)
					replicasToAdd--
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
				if op, err := createFn(toRemoveReplica.uuid, cr.lc.infos.Shards[toRemoveReplica.shardID]); err != nil {
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
			ops = append(ops, createFn("", replicaToStart.uuid, replicaToStart.shardID, replicaToStart.replicaID))
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
			if !t.lc.infos.Stores[store].TaskServiceCreated {
				ops = append(ops, operator.CreateTaskServiceOp("",
					store, pb.LogService, t.lc.User))
			}
		}
	}
	return ops
}

func (c *logServiceChecker) Check() (operators []*operator.Operator) {
	normalStats, nonVotingStats := parseLogShards(c.Cluster, c.infos, c.expiredStores, c.nonVotingReplicaNum)
	commonChecker := checker{
		lc: c,
		ss: []*stats{
			normalStats,
			nonVotingStats,
		},
	}
	checkers := []operatorChecker{
		&checkToAdd{commonChecker},
		&checkToRemove{commonChecker},
		&checkToStart{commonChecker},
		&checkZombie{commonChecker},
		&checkTaskService{commonChecker},
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
