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

// ProcessAllowsIndexObservation proves that an optional cost observation sees
// the whole scan. It is independent of the observed comparison's domain.
func ProcessAllowsIndexObservation(proc *process.Process) bool {
	if proc == nil || proc.Base == nil {
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

// MatchesProcess checks the full execution snapshot required by a captured
// physical access, including the common observation prerequisites.
func (intent ExecutionIntent) MatchesProcess(proc *process.Process) bool {
	return proc != nil && proc.Base != nil && intent.TimeZone == proc.GetSessionInfo().TimeZone &&
		intent.UnixTime == proc.Base.UnixTime && ProcessAllowsIndexObservation(proc)
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
	if !PlanningInputMayUseIndex(root) || tree.ParameterCount(root) != 0 {
		return nil
	}
	return &PlanningInput{root: root, intent: intent, owner: owner}
}

// PlanningInputMayUseIndex cheaply excludes literal-only, scan-free roots.
// Uncertain syntax retains the complete parameter and bound-plan checks. This
// is an allocation fast path, never a positive candidate-admission proof.
func PlanningInputMayUseIndex(root *tree.Select) bool {
	if root == nil || root.Select == nil {
		return false
	}
	clause, ok := root.Select.(*tree.SelectClause)
	if !ok || root.With != nil || root.RewriteOption != nil || root.TimeWindow != nil || len(root.OrderBy) != 0 || root.Limit != nil ||
		root.RankOption != nil || root.Ep != nil || len(root.IntoVars) != 0 || root.SelectLockInfo != nil ||
		clause.Where != nil || clause.GroupBy != nil || clause.Having != nil ||
		len(clause.Windows) != 0 || len(clause.IntoVars) != 0 || clause.IntoExport != nil || len(clause.OrderByOriginalExprs) != 0 || len(clause.OrderBySourceProbes) != 0 {
		return true
	}
	if clause.From != nil && len(clause.From.Tables) != 0 {
		// The parser represents an omitted FROM as one unnamed table. Real
		// table references, including explicit DUAL, retain the normal proof.
		if len(clause.From.Tables) != 1 {
			return true
		}
		alias, ok := clause.From.Tables[0].(*tree.AliasedTableExpr)
		if !ok {
			return true
		}
		table, ok := alias.Expr.(*tree.TableName)
		if !ok || table.ObjectName != "" || table.ExplicitSchema || table.ExplicitCatalog || table.AtTsExpr != nil || table.IcebergRef != nil {
			return true
		}
	}
	for _, expr := range clause.Exprs {
		if _, literal := expr.Expr.(*tree.NumVal); !literal {
			return true
		}
	}
	return false
}

func (input *PlanningInput) claim(root *tree.Select) bool {
	return input != nil && root != nil && input.root == root && input.claimed.CompareAndSwap(false, true)
}

func samePlanningCompiler(owner, current CompilerContext) bool {
	return owner != nil && current != nil && reflect.TypeOf(owner).Comparable() && owner == current
}

func (builder *QueryBuilder) borrowingCompiler() CompilerContext {
	if builder.planningInput == nil || !samePlanningCompiler(builder.planningInput.owner, builder.compCtx) {
		return nil
	}
	return builder.compCtx
}

func (builder *QueryBuilder) finishPlanningInput() {
	builder.clearScanBorrows()
	builder.planningInput = nil
}
