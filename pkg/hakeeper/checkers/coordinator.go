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

package checkers

import (
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/cnservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/logservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/proxy"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/syshealth"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/tnservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

// Coordinator is assumed to be used in synchronous, single-threaded context.
type Coordinator struct {
	OperatorController *operator.Controller

	// Considering the context of `Coordinator`,
	// there is no need for a mutext to protect.
	teardown    bool
	teardownOps []*operator.Operator

	service string
	cfg     hakeeper.Config
}

func NewCoordinator(
	service string,
	cfg hakeeper.Config,
) *Coordinator {
	cfg.Fill()
	return &Coordinator{
		service:            service,
		OperatorController: operator.NewController(),
		cfg:                cfg,
	}
}

func (c *Coordinator) Check(alloc util.IDAllocator, state pb.CheckerState, standbyEnabled bool) []pb.ScheduleCommand {
	logState := state.LogState
	tnState := state.TNState
	cnState := state.CNState
	proxyState := state.ProxyState
	cluster := state.ClusterInfo
	currentTick := state.Tick
	user := state.TaskTableUser
	runtime.ServiceRuntime(c.service).Logger().Debug("hakeeper checker state",
		zap.Any("cluster information", cluster),
		zap.Any("log state", logState),
		zap.Any("dn state", tnState),
		zap.Any("cn state", cnState),
		zap.Uint64("current tick", currentTick),
	)

	defer func() {
		if !c.teardown {
			runtime.ServiceRuntime(c.service).Logger().Debug("MO is working.")
		}
	}()

	c.OperatorController.RemoveFinishedOperator(logState, tnState, cnState, proxyState)

	// if we've discovered unhealthy already, no need to keep alive anymore.
	if c.teardown {
		return c.OperatorController.Dispatch(c.teardownOps, logState, tnState, cnState, proxyState)
	}

	// check whether system health or not.
	if operators, health := syshealth.Check(c.cfg, cluster, tnState, logState, currentTick); !health {
		// TN starts have a report/target lifetime. Teardown ends their ownership
		// before ordinary checks stop running.
		for _, record := range cluster.TNShards {
			for _, op := range slices.Clone(c.OperatorController.GetOperators(record.ShardID)) {
				if steps := op.OpSteps(); len(steps) == 1 {
					if _, ok := steps[0].(operator.AddTnReplica); ok {
						c.OperatorController.RemoveOperator(op)
					}
				}
			}
		}
		c.teardown = true
		c.teardownOps = operators
		return c.OperatorController.Dispatch(c.teardownOps, logState, tnState, cnState, proxyState)
	}

	// system health, try to keep alive.
	executing := c.OperatorController.GetExecutingReplicas()
	executingNonVoting := c.OperatorController.GetNonVotingExecutingReplicas()

	operators := make([]*operator.Operator, 0)
	commonFields := hakeeper.NewCheckerCommonFields(
		c.service,
		c.cfg,
		alloc,
		cluster,
		user,
		currentTick,
	)
	checkers := []hakeeper.ModuleChecker{
		logservice.NewLogServiceChecker(
			commonFields,
			logState,
			tnState,
			executing,
			executingNonVoting,
			state.NonVotingReplicaNum,
			state.NonVotingLocality,
			standbyEnabled,
		),
		tnservice.NewTNServiceChecker(commonFields, tnState, c.OperatorController),
		cnservice.NewCNServiceChecker(commonFields, cnState),
		proxy.NewProxyServiceChecker(commonFields, proxyState),
	}
	for _, checker := range checkers {
		operators = append(operators, checker.Check()...)
	}
	// A TN retry must not replace suppressed, still-desired Log commands with
	// a TN-only legacy batch. Eligibility stays with the Log checker policy.
	if c.OperatorController.HasTNStartRetry(operators) {
		shardIDs := make([]uint64, 0, len(cluster.LogShards))
		for _, shard := range cluster.LogShards {
			shardIDs = append(shardIDs, shard.ShardID)
		}
		slices.Sort(shardIDs)
		var existing []*operator.Operator
		for _, shardID := range slices.Compact(shardIDs) {
			existing = append(existing, c.OperatorController.GetOperators(shardID)...)
		}
		pending := logservice.PendingOperators(commonFields, logState, state.NonVotingReplicaNum, existing)
		operators = append(pending, operators...)
	}
	return c.OperatorController.Dispatch(operators, logState, tnState, cnState, proxyState)
}
