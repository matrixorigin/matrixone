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
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/dnservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/logservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	hapb "github.com/matrixorigin/matrixone/pkg/pb/hakeeper"
)

type Coordinator struct {
	OperatorController *operator.Controller
}

func NewCoordinator() *Coordinator {
	return &Coordinator{OperatorController: operator.NewController()}
}

func (c *Coordinator) Check(alloc util.IDAllocator, cluster hapb.ClusterInfo,
	dn hapb.DNState, log hapb.LogState, currentTick uint64) []hapb.ScheduleCommand {

	c.OperatorController.RemoveFinishedOperator(dn, log)

	removing := c.OperatorController.GetRemovingReplicas()
	adding := c.OperatorController.GetAddingReplicas()

	operators := make([]*operator.Operator, 0)
	operators = append(operators, logservice.Check(alloc, cluster, log, removing, adding, currentTick)...)
	operators = append(operators, dnservice.Check(alloc, dn, currentTick)...)

	return c.OperatorController.Dispatch(operators, log, dn)
}
