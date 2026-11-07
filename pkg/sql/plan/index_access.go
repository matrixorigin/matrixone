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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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

// dependencyResult is local to the original-plan proof. Unknown must never be
// used as evidence that a later execution can reuse the selected access.
type dependencyResult uint8

const (
	dependencyNo dependencyResult = iota
	dependencyYes
	dependencyUnknown
)

type originalDependencyCheck struct {
	scan   *plan.Node
	budget int
}

func (check *originalDependencyCheck) spend(work int) bool {
	if work < 0 || check.budget < work {
		check.budget = 0
		return false
	}
	check.budget -= work
	return true
}

// indexedColumn checks any-part membership. When exclusionBudget is present it
// also proves, in the same bounded traversal, that no regular index has this
// column first. A negative position asks whether any indexed column is temporal.
func (check *originalDependencyCheck) indexedColumn(position int32, ctx context.Context, exclusionBudget *int) (dependencyResult, bool) {
	excludes := exclusionBudget != nil
	if check.scan == nil || check.scan.TableDef == nil || len(check.scan.BindingTags) == 0 {
		return dependencyNo, excludes
	}
	table := check.scan.TableDef
	if len(table.Indexes) > engine.MaxIndexPlanningDefinitions {
		return dependencyUnknown, false
	}
	seen, unknown := false, false
	for _, index := range table.Indexes {
		if index == nil {
			excludes, unknown = false, true
			continue
		}
		if excludes && !spendIndexCostWork(ctx, exclusionBudget, 1) {
			excludes = false
		}
		membershipFits := check.spend(1) && len(index.IndexAlgo) <= engine.MaxIndexPlanningNameBytes && check.spend(len(index.IndexAlgo))
		if !membershipFits {
			unknown = true
		}
		if !catalog.IsRegularIndexAlgo(index.IndexAlgo) {
			continue
		}
		if excludes {
			if !index.TableExist || len(index.Parts) == 0 || len(index.Parts[0]) == 0 || len(index.Parts[0]) > engine.MaxIndexPlanningNameBytes || !spendIndexCostWork(ctx, exclusionBudget, len(index.Parts[0])) {
				excludes = false
			} else {
				pos, ok := table.Name2ColIndex[catalog.ResolveAlias(index.Parts[0])]
				if !ok || pos < 0 || int(pos) >= len(table.Cols) || table.Cols[pos] == nil || pos == position {
					excludes = false
				}
			}
		}
		if !index.TableExist || !membershipFits {
			continue
		}
		if len(index.Parts) > engine.MaxIndexPlanningParts {
			unknown = true
			continue
		}
		for _, part := range index.Parts {
			if len(part) == 0 || len(part) > engine.MaxIndexPlanningNameBytes || !check.spend(1+len(part)) {
				unknown = true
				break
			}
			pos, ok := table.Name2ColIndex[catalog.ResolveAlias(part)]
			if !ok || pos < 0 || int(pos) >= len(table.Cols) || table.Cols[pos] == nil {
				unknown = true
				continue
			}
			if position >= 0 && pos == position {
				seen = true
			} else if position < 0 {
				oid := types.T(table.Cols[pos].Typ.Id)
				seen = seen || oid == types.T_datetime || oid == types.T_timestamp
			}
			if seen && !excludes {
				return dependencyYes, false
			}
		}
	}
	if excludes && (ctx == nil || context.Cause(ctx) != nil) {
		excludes = false
	}
	if seen {
		return dependencyYes, excludes
	}
	if unknown {
		return dependencyUnknown, excludes
	}
	return dependencyNo, excludes
}

// unprovedSource mirrors the existing source check but shares this proof's
// bound. An exhausted or opaque source cannot establish a negative result.
func (check *originalDependencyCheck) unprovedSource(expr *plan.Expr, depth int) dependencyResult {
	if depth > 64 || !check.spend(1) {
		return dependencyUnknown
	}
	if expr == nil || expr.GetP() != nil {
		return dependencyYes
	}
	if lit := expr.GetLit(); lit != nil && lit.Src != nil {
		return dependencyYes
	}
	var children []*plan.Expr
	if fn := expr.GetF(); fn != nil {
		children = fn.Args
	} else if list := expr.GetList(); list != nil {
		children = list.List
	}
	for _, child := range children {
		result := check.unprovedSource(child, depth+1)
		if result != dependencyNo {
			return result
		}
	}
	return dependencyNo
}

func (check *originalDependencyCheck) pair(column, bound *plan.Expr) dependencyResult {
	if column == nil || bound == nil || column.GetCol() == nil || check.scan == nil ||
		check.scan.TableDef == nil || len(check.scan.BindingTags) == 0 {
		return dependencyNo
	}
	ref, table := column.GetCol(), check.scan.TableDef
	if ref.RelPos != check.scan.BindingTags[0] || ref.ColPos < 0 || int(ref.ColPos) >= len(table.Cols) || table.Cols[ref.ColPos] == nil {
		return dependencyNo
	}
	oid, boundType := types.T(table.Cols[ref.ColPos].Typ.Id), types.T(bound.Typ.Id)
	if oid != types.T_datetime && oid != types.T_timestamp {
		return dependencyNo
	}
	if !isDatetimeTimestampTypePair(oid, boundType) {
		if boundType != types.T_any {
			return dependencyNo
		}
		source := check.unprovedSource(bound, 0)
		if source != dependencyYes {
			return source
		}
	}
	result, _ := check.indexedColumn(ref.ColPos, nil, nil)
	return result
}

func (check *originalDependencyCheck) expr(expr *plan.Expr, depth int) dependencyResult {
	if depth > 64 || !check.spend(1) {
		return dependencyUnknown
	}
	if expr == nil {
		return dependencyNo
	}
	var children []*plan.Expr
	switch value := expr.Expr.(type) {
	case *plan.Expr_F:
		if value.F == nil || value.F.Func == nil {
			return dependencyUnknown
		}
		fn := value.F
		id, _ := function.DecodeOverloadID(fn.Func.Obj)
		var pairs [2][2]*plan.Expr
		pairCount := 0
		switch id {
		case function.EQUAL, function.LESS_THAN, function.LESS_EQUAL, function.GREAT_THAN, function.GREAT_EQUAL:
			if len(fn.Args) == 2 {
				pairs = [2][2]*plan.Expr{{fn.Args[0], fn.Args[1]}, {fn.Args[1], fn.Args[0]}}
				pairCount = 2
			}
		case function.BETWEEN, function.IN_RANGE:
			if len(fn.Args) >= 3 {
				pairs = [2][2]*plan.Expr{{fn.Args[0], fn.Args[1]}, {fn.Args[0], fn.Args[2]}}
				pairCount = 2
			}
		case function.IN:
			if len(fn.Args) == 2 && fn.Args[1] != nil {
				result := check.pair(fn.Args[0], fn.Args[1])
				if result != dependencyNo {
					return result
				}
				if list := fn.Args[1].GetList(); list != nil {
					for _, item := range list.List {
						if !check.spend(1) {
							return dependencyUnknown
						}
						if result := check.pair(fn.Args[0], item); result != dependencyNo {
							return result
						}
					}
				}
			}
		}
		for _, pair := range pairs[:pairCount] {
			if result := check.pair(pair[0], pair[1]); result != dependencyNo {
				return result
			}
		}
		children = fn.Args
	case *plan.Expr_List:
		if value.List == nil {
			return dependencyUnknown
		}
		children = value.List.List
	case *plan.Expr_Lit:
		if value.Lit != nil && value.Lit.Src != nil {
			return check.expr(value.Lit.Src, depth+1)
		}
	case nil, *plan.Expr_Col, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Raw,
		*plan.Expr_Corr, *plan.Expr_T, *plan.Expr_Max, *plan.Expr_Vec,
		*plan.Expr_Fold, *plan.Expr_Slot:
		return dependencyNo
	default:
		return dependencyUnknown
	}
	for _, child := range children {
		if result := check.expr(child, depth+1); result != dependencyNo {
			return result
		}
	}
	return dependencyNo
}

// originalDependencyFallback is used only when the positive shape proof is
// incomplete or stopped. It never assumes that an unvisited expression is safe.
func originalDependencyFallback(query *plan.Query, path indexCandidatePath) bool {
	check := originalDependencyCheck{scan: query.Nodes[path.ids[path.len-1]], budget: 4096}
	if check.scan.TableDef == nil || len(check.scan.BindingTags) == 0 || len(check.scan.TableDef.Indexes) == 0 {
		return false
	}
	for _, id := range path.ids[:path.len] {
		for _, expr := range query.Nodes[id].FilterList {
			switch check.expr(expr, 0) {
			case dependencyYes:
				return true
			case dependencyUnknown:
				result, _ := check.indexedColumn(-1, nil, nil)
				return result != dependencyNo
			}
		}
	}
	return false
}

// originalSingleDependency combines the leading exclusion and any-part
// membership decisions for a complete original comparison.
func originalSingleDependency(ctx context.Context, scan *plan.Node, proof originalIndexProof) bool {
	if scan.TableDef == nil || len(scan.BindingTags) == 0 || len(scan.TableDef.Indexes) == 0 {
		return false
	}
	table, atom := scan.TableDef, proof.atom
	position := atom.column.GetCol().ColPos
	if !isDatetimeTimestampTypePair(types.T(table.Cols[position].Typ.Id), types.T(atom.bound.Typ.Id)) {
		return false
	}
	check := originalDependencyCheck{scan: scan, budget: 4096}
	exclusionBudget := proof.remaining
	membership, excludes := check.indexedColumn(position, ctx, &exclusionBudget)
	return !excludes && membership != dependencyNo
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
	scan := owner.Nodes[path.ids[path.len-1]]
	if !capture && (scan.TableDef == nil || len(scan.BindingTags) == 0 || len(scan.TableDef.Indexes) == 0) {
		return
	}
	var proof originalIndexProof
	if ctx != nil && context.Cause(ctx) == nil {
		proof = proveOriginalIndexPath(ctx, &owner, path)
	}
	stopped := ctx == nil || context.Cause(ctx) != nil
	if stopped || !proof.fits {
		if originalDependencyFallback(&owner, path) {
			builder.qry.ExecutionDependentAccess = true
		}
	} else if proof.single && originalSingleDependency(ctx, scan, proof) {
		builder.qry.ExecutionDependentAccess = true
	}
	if capture && proof.fits && proof.single && ctx != nil && context.Cause(ctx) == nil && indexPathExpressionsSafe(&owner, path) && context.Cause(ctx) == nil {
		builder.originalIndexScan = originalIndexScan{scanID: path.ids[path.len-1], safe: true}
	}
}
