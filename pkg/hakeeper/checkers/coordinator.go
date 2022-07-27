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
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/dnservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/logservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/syshealth"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// Coordinator is assumed to be used in synchronous, single-threaded context.
type Coordinator struct {
	OperatorController *operator.Controller

	// Considering the context of `Coordinator`,
	// there is no need for a mutext to protect.
	teardown    bool
	teardownOps []*operator.Operator

	cfg hakeeper.Config
}

func NewCoordinator(cfg hakeeper.Config) *Coordinator {
	cfg.Fill()
	return &Coordinator{
		OperatorController: operator.NewController(),
		cfg:                cfg,
	}
}

func (c *Coordinator) Check(alloc util.IDAllocator, cluster pb.ClusterInfo,
	dnState pb.DNState, logState pb.LogState, currentTick uint64) []pb.ScheduleCommand {

	c.OperatorController.RemoveFinishedOperator(logState, dnState)

	// if we've discovered unhealthy already, no need to keep alive anymore.
	if c.teardown {
		return c.OperatorController.Dispatch(c.teardownOps, logState, dnState)
	}

	// check whether system health or not.
	if operators, health := syshealth.Check(c.cfg, cluster, dnState, logState, currentTick); !health {
		c.teardown = true
		c.teardownOps = operators
		return c.OperatorController.Dispatch(c.teardownOps, logState, dnState)
	}

	// system health, try to keep alive.
	executing := c.OperatorController.GetExecutingReplicas()

	operators := make([]*operator.Operator, 0)
	operators = append(operators, logservice.Check(alloc, c.cfg, cluster, logState, executing, currentTick)...)
	operators = append(operators, dnservice.Check(alloc, c.cfg, dnState, currentTick)...)

	return c.OperatorController.Dispatch(operators, logState, dnState)
}
