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
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// indexCandidatePath is the bounded unary path eligible for observed index costing.
// Its fixed-size storage bounds the traversal without retaining the plan.
type indexCandidatePath struct {
	ids [64]int32
	len int
}

func traceIndexCandidatePath(query *plan.Query) (indexCandidatePath, bool) {
	var path indexCandidatePath
	if query == nil || query.StmtType != plan.Query_SELECT || len(query.Steps) != 1 || len(query.Nodes) > len(path.ids) {
		return path, false
	}
	var seen [64]bool
	for id := query.Steps[0]; ; {
		if id < 0 || int(id) >= len(query.Nodes) || seen[id] || path.len == len(path.ids) {
			return path, false
		}
		seen[id] = true
		node := query.Nodes[id]
		if node == nil || node.Limit != nil || node.Offset != nil || node.RankOption != nil ||
			len(node.OnList) != 0 || len(node.GroupBy) != 0 || len(node.AggList) != 0 ||
			len(node.WinSpecList) != 0 || len(node.OrderBy) != 0 || len(node.TblFuncExprList) != 0 || len(node.SourceStep) != 0 {
			return path, false
		}
		path.ids[path.len] = id
		path.len++
		if node.NodeType == plan.Node_TABLE_SCAN {
			return path, len(node.Children) == 0
		}
		if node.NodeType != plan.Node_PROJECT && node.NodeType != plan.Node_FILTER || len(node.Children) != 1 {
			return path, false
		}
		id = node.Children[0]
	}
}

func indexPathExpressionsSafe(query *plan.Query, path indexCandidatePath) bool {
	if len(query.Params) != 0 || path.len == 0 {
		return false
	}
	for _, id := range path.ids[:path.len-1] {
		node := query.Nodes[id]
		for _, expr := range node.ProjectList {
			if expr == nil || indexExpressionHasUnprovedSource(expr) || !isTruncationSafeRowExpr(expr) {
				return false
			}
		}
		for _, expr := range node.FilterList {
			if !indexPredicateEvaluationSafe(expr) {
				return false
			}
		}
	}
	return indexScanExpressionsSafe(query.Nodes[path.ids[path.len-1]])
}

// originalIndexProof carries only facts about the bound owner. It is not a
// certificate for a rewritten scan, catalog relation, or published access.
type originalIndexProof struct {
	fits, single bool
	remaining    int
	atom         indexComparison
}

// A builder analyzes at most one original unary path before publishing steps.
type originalIndexScan struct {
	scanID int32
	safe   bool
}

func proveOriginalIndexPath(ctx context.Context, query *plan.Query, path indexCandidatePath) originalIndexProof {
	proof := originalIndexProof{remaining: 4096}
	var predicate *plan.Expr
	multiple := false
	nodes := 0
	for _, id := range path.ids[:path.len] {
		if !spendIndexCostWork(ctx, &proof.remaining, 1) {
			return proof
		}
		node := query.Nodes[id]
		for _, list := range [][]*plan.Expr{node.ProjectList, node.FilterList, node.BlockFilterList} {
			if len(list) > 64 {
				return proof
			}
			for _, expr := range list {
				if !indexCostExpressionFits(ctx, expr, 0, &nodes, &proof.remaining) {
					return proof
				}
			}
		}
		for _, expr := range node.FilterList {
			if predicate != nil {
				multiple = true
			} else {
				predicate = expr
			}
		}
	}
	proof.fits = true
	if multiple {
		return proof
	}
	atom, ok := analyzeIndexComparison(ctx, predicate)
	if !ok {
		return proof
	}
	scan := query.Nodes[path.ids[path.len-1]]
	ref := atom.column.GetCol()
	table := scan.TableDef
	if len(scan.BindingTags) != 1 || ref.RelPos != scan.BindingTags[0] || table == nil || ref.ColPos < 0 || int(ref.ColPos) >= len(table.Cols) || table.Cols[ref.ColPos] == nil {
		return proof
	}
	column := table.Cols[ref.ColPos]
	proof.atom, proof.single = atom, column.Typ.Id == atom.column.Typ.Id && column.Typ.Scale == atom.column.Typ.Scale
	return proof
}

func originalIndexPathExcludesIndex(ctx context.Context, scan *plan.Node, proof originalIndexProof) bool {
	if ctx == nil || !proof.fits {
		return false
	}
	if !proof.single {
		return ctx.Err() == nil
	}
	if scan.TableDef == nil || len(scan.TableDef.Indexes) > engine.MaxIndexPlanningDefinitions {
		return false
	}
	budget := proof.remaining
	for _, index := range scan.TableDef.Indexes {
		if index == nil || !spendIndexCostWork(ctx, &budget, 1) {
			return false
		}
		if !catalog.IsRegularIndexAlgo(index.IndexAlgo) {
			continue
		}
		if !index.TableExist || len(index.Parts) == 0 || len(index.Parts[0]) == 0 || len(index.Parts[0]) > engine.MaxIndexPlanningNameBytes || !spendIndexCostWork(ctx, &budget, len(index.Parts[0])) {
			return false
		}
		pos, exists := scan.TableDef.Name2ColIndex[catalog.ResolveAlias(index.Parts[0])]
		if !exists || pos < 0 || int(pos) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[pos] == nil {
			return false
		}
		if pos == proof.atom.column.GetCol().ColPos {
			return false
		}
	}
	return ctx.Err() == nil
}

// collectOriginalIndexOwner keeps the conservative cache-lifetime result
// separate from the positive scan proof, while sharing their bound owner.
func (builder *QueryBuilder) collectOriginalIndexOwner(rootID int32) {
	if builder == nil {
		return
	}
	builder.originalIndexScan = originalIndexScan{}
	if builder.qry == nil || builder.compCtx == nil || builder.isReusablePlan() {
		return
	}
	input := builder.planningInput
	capture := input != nil && len(builder.qry.Steps) == 0
	owner := *builder.qry
	var root [1]int32
	if len(owner.Steps) == 0 {
		root[0] = rootID
		owner.Steps = root[:]
	} else if len(owner.Steps) != 1 || owner.Steps[0] != rootID {
		return
	}
	path, ok := traceIndexCandidatePath(&owner)
	if !ok || len(owner.Params) != 0 {
		return
	}
	ctx := builder.GetContext()
	var proof originalIndexProof
	proved := false
	originalProof := func() originalIndexProof {
		if !proved {
			proof = proveOriginalIndexPath(ctx, &owner, path)
			proved = true
		} else if ctx == nil || context.Cause(ctx) != nil {
			return originalIndexProof{}
		}
		return proof
	}
	if potentialContextDependentIndexPath(&owner, path) && !originalIndexPathExcludesIndex(ctx, owner.Nodes[path.ids[path.len-1]], originalProof()) {
		builder.qry.ExecutionDependentAccess = true
	}
	if capture && indexPathExpressionsSafe(&owner, path) {
		proof := originalProof()
		builder.originalIndexScan = originalIndexScan{scanID: path.ids[path.len-1], safe: proof.fits && proof.single}
	}
}
