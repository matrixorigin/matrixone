// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// tablelessSingletonProject is the common structural gate for flattening a
// singleton and classifying a dependent derived projection. It identifies one
// synthetic input row that survives its WHERE. Aggregate, pagination, window
// and ordinary VALUES inputs retain their own row-production semantics.
func (builder *QueryBuilder) tablelessSingletonProject(nodeID int32, allowFiltered bool) *plan.Node {
	if builder == nil || builder.qry == nil || nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return nil
	}
	project := builder.qry.Nodes[nodeID]
	if project.NodeType != plan.Node_PROJECT || len(project.Children) != 1 || !singletonNodeUnbounded(project) {
		return nil
	}
	input := builder.qry.Nodes[project.Children[0]]
	if input.NodeType == plan.Node_FILTER {
		// Moving the RHS of a comparison can change when and how often it
		// evaluates. Only inline a single side-effect-free value here.
		if !allowFiltered || input.FilterIsBarrier || len(input.Children) != 1 ||
			!singletonNodeUnbounded(input) || len(project.ProjectList) != 1 ||
			!builder.canInlineSingletonValue(project.ProjectList[0]) {
			return nil
		}
		child := builder.qry.Nodes[input.Children[0]]
		if !isTablelessSingletonInput(child) || int(nodeID) >= len(builder.ctxByNode) {
			return nil
		}
		scope := builder.ctxByNode[nodeID]
		if scope == nil || !slices.Equal(scope.whereFilters, input.FilterList) ||
			!singletonWhereIsTrue(builder, scope, scope.whereFilters) {
			return nil
		}
		input = child
	}
	if !isTablelessSingletonInput(input) {
		return nil
	}
	return project
}

func (builder *QueryBuilder) canInlineSingletonValue(value *Expr) bool {
	if col := value.GetCorr(); col != nil {
		// Flattening rebases this direct reference onto its owning row.
		return col.Depth == 1
	}
	return canReplaySelectorExpr(builder.GetContext(), value)
}

// Filtered singletons can replace a direct scalar value or equality RHS.
// Other consumers retain their original projection and evaluation demand.
func canInlineFilteredSingleton(sub *plan.SubqueryRef) bool {
	return isBitCountScalarRef(sub) || isEqualitySingletonComparison(sub)
}

func isEqualitySingletonComparison(sub *plan.SubqueryRef) bool {
	return sub.RowSize == 1 && (sub.Typ == plan.SubqueryRef_IN || sub.Typ == plan.SubqueryRef_ANY && sub.Op == "=")
}

func singletonNodeUnbounded(node *plan.Node) bool {
	return node.Limit == nil && node.Offset == nil && node.RankOption == nil
}

func isTablelessSingletonInput(node *plan.Node) bool {
	return node.NodeType == plan.Node_VALUE_SCAN && node.TableDef == nil &&
		len(node.FilterList) == 0 && singletonNodeUnbounded(node)
}

func (bc *BindContext) setWhereFilters(filters []*plan.Expr) {
	bc.whereFilters = filters
	bc.singletonWhereProofKnown = false
	bc.singletonWhereProofValue = false
	bc.singletonWhereProofNeedsContext = false
}

func singletonWhereIsTrue(builder *QueryBuilder, ctx *BindContext, filters []*Expr) bool {
	if ctx == nil {
		return false
	}
	if ctx.singletonWhereProofKnown {
		if ctx.singletonWhereProofValue && ctx.singletonWhereProofNeedsContext {
			if builder == nil || builder.compCtx == nil {
				return false
			}
			bindCtx := builder.GetContext()
			if bindCtx == nil || defines.IsNoEvaluationViewBinding(bindCtx) || bindCtx.Err() != nil {
				return false
			}
		}
		return ctx.singletonWhereProofValue
	}
	ctx.singletonWhereProofKnown = true
	ctx.singletonWhereProofValue = false
	ctx.singletonWhereProofNeedsContext = false
	var budget *speculativeConstantFoldBudget
	for _, filter := range filters {
		folded := filter
		if filter.GetLit() == nil {
			ctx.singletonWhereProofNeedsContext = true
			if builder == nil || builder.compCtx == nil {
				return false
			}
			bindCtx := builder.GetContext()
			if bindCtx == nil || defines.IsNoEvaluationViewBinding(bindCtx) || bindCtx.Err() != nil {
				return false
			}
			if budget == nil {
				proc := builder.compCtx.GetProcess()
				if proc == nil {
					return false
				}
				// The compiler may lend a process from an earlier statement.
				// Borrow its resources, using the current bind request's deadline.
				eval := proc.NewNoContextChildProc(0)
				eval.Ctx = bindCtx
				budget = &speculativeConstantFoldBudget{ctx: bindCtx}
				budget.integerFold = &singletonIntegerFold{
					eval: eval, budget: budget, workLeft: maxSingletonIntegerFoldWork,
				}
			}
			foldExpr := DeepCopyExpr(filter)
			foldConstantStringLengths(foldExpr, bindCtx, budget.integerFold)
			if !isSpeculativeConstantFoldSafe(foldExpr, budget) {
				return false
			}
			var err error
			folded, err = constantFoldForSingletonProof(foldExpr, budget.integerFold.eval)
			if err != nil || folded == nil || bindCtx.Err() != nil {
				return false
			}
		}
		if lit := folded.GetLit(); folded.Typ.Id != int32(types.T_bool) || lit == nil || lit.Isnull || !lit.GetBval() {
			return false
		}
	}
	ctx.singletonWhereProofValue = true
	return true
}
