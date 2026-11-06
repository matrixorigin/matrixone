// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"reflect"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ExecutionIntent describes one execution, not a reusable plan.
type ExecutionIntent struct {
	Placement schedule.LocalAccessIntent
	TimeZone  *time.Location
	UnixTime  int64
}

// MatchesProcess rejects changes to execution state used by candidate estimation.
func (intent ExecutionIntent) MatchesProcess(proc *process.Process) bool {
	if proc == nil || proc.Base == nil || intent.TimeZone != proc.GetSessionInfo().TimeZone || intent.UnixTime != proc.Base.UnixTime {
		return false
	}
	if proc.Base.SessionInfo.ApplySQLSelectLimit {
		resolve := proc.GetResolveVariableFunc()
		if resolve == nil {
			return false
		}
		value, err := resolve(SQLSelectLimitVariable, true, false)
		limit, ok := value.(uint64)
		if err != nil || !ok || limit != ^uint64(0) {
			return false
		}
	}
	return true
}

// PlanningInput is passed only through transparent root wrappers. Each
// optimizer attempt claims it once; it is never retained in the resulting plan.
type PlanningInput struct {
	root    *tree.Select
	intent  ExecutionIntent
	owner   CompilerContext
	claimed atomic.Bool
}

func NewPlanningInput(root *tree.Select, intent ExecutionIntent, owner CompilerContext) *PlanningInput {
	if root == nil || root.Select == nil || tree.ParameterCount(root) != 0 {
		return nil
	}
	return &PlanningInput{root: root, intent: intent, owner: owner}
}

func (input *PlanningInput) claim(root *tree.Select) bool {
	return input != nil && root != nil && input.root == root && input.claimed.CompareAndSwap(false, true)
}

func samePlanningCompiler(owner, current CompilerContext) bool {
	return owner != nil && current != nil && reflect.TypeOf(owner).Comparable() && owner == current
}

func (builder *QueryBuilder) borrowingCompiler() CompilerContext {
	if builder.planningInput == nil || !builder.borrowResolutionAllowed || !samePlanningCompiler(builder.planningInput.owner, builder.compCtx) {
		return nil
	}
	return builder.compCtx
}

func (builder *QueryBuilder) finishPlanningInput() {
	builder.clearScanBorrows()
	builder.planningInput = nil
	builder.borrowResolutionAllowed = false
}
