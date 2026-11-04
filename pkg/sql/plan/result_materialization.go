// Copyright 2026 Matrix Origin
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

package plan

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func resultIntegerLiteralZero(expr *plan.Expr) bool {
	if expr == nil || expr.GetLit() == nil || expr.GetLit().Isnull {
		return false
	}
	switch v := expr.GetLit().Value.(type) {
	case *plan.Literal_U64Val:
		return v.U64Val == 0
	case *plan.Literal_I64Val:
		return v.I64Val == 0
	default:
		return false
	}
}

// MySQL can omit window execution for const-table inputs or an implicit
// global group (which emits one NULL row on empty input). This is not
// determined by result cardinality: LIMIT 1 above a window cannot omit it.
// A provably empty statement also skips materialization (including DISTINCT
// ROLLUP). Unknown bounds never prove either optimization. No statistics or
// observed row counts participate, and all state is local to this traversal.
type resultMaterialization struct {
	query            *plan.Query
	bound            func(int32) uint8
	constantProvider func(int32) bool
	values           []any
	scopes           []*BindContext
	usedValues       bool
}

func newResultMaterialization(query *plan.Query, values []any, scopes ...[]*BindContext) *resultMaterialization {
	r := &resultMaterialization{query: query, values: values}
	if len(scopes) > 0 {
		r.scopes = scopes[0]
	}
	const unknown = uint8(2) // bounds are saturated at 0, 1, or unknown/many
	memo := make(map[int32]uint8)
	valid := func(id int32) bool { return id >= 0 && int(id) < len(query.Nodes) && query.Nodes[id] != nil }
	var bound func(int32) uint8
	bound = func(id int32) uint8 {
		if !valid(id) {
			return unknown
		}
		if result, found := memo[id]; found {
			return result
		}
		memo[id] = unknown // cycles cannot establish a small bound
		node := query.Nodes[id]
		result := unknown
		child := func() uint8 {
			if len(node.Children) != 1 {
				return unknown
			}
			return bound(node.Children[0])
		}
		switch node.NodeType {
		case plan.Node_PROJECT, plan.Node_FILTER, plan.Node_SORT, plan.Node_DISTINCT, plan.Node_WINDOW, plan.Node_SINK, plan.Node_PARTITION:
			result = child()
			if node.NodeType == plan.Node_FILTER && len(node.Children) == 1 && valid(node.Children[0]) {
				agg := query.Nodes[node.Children[0]]
				if agg.NodeType == plan.Node_TABLE_SCAN {
					scan := *agg
					scan.FilterList = append(append([]*plan.Expr(nil), agg.FilterList...), node.FilterList...)
					if r.scanSingleRow(&scan) {
						result = min(result, 1)
					}
				}
				if agg.NodeType == plan.Node_JOIN && r.joinKeyFilter(agg, node.FilterList, 0) {
					result = min(result, 1)
				}
				if agg.NodeType == plan.Node_JOIN && agg.JoinType == plan.Node_MARK && len(agg.Children) == 2 &&
					bound(agg.Children[0]) <= 1 && r.constantProvider(agg.Children[1]) &&
					resultPositiveExistenceFilter(node.FilterList, agg) {
					result = bound(agg.Children[0])
				}
				if agg.NodeType == plan.Node_AGG && len(agg.GroupBy) == 0 && len(agg.Children) == 1 && bound(agg.Children[0]) == 0 {
					for _, filter := range node.FilterList {
						if emptyAggregateCountRejected(agg, filter) {
							result = 0
							break
						}
					}
				}
			}
		case plan.Node_AGG:
			input := child()
			if len(node.GroupBy) == 0 {
				result = 1
			} else if input == 0 {
				result = 0
			} else if !hasInactiveGroupingColumn(node.GroupingFlag) && !IsSortRollupOption(node.ExtraOptions) {
				result = input
			}
			if input == 0 && len(node.GroupBy) == 0 {
				for _, filter := range node.FilterList {
					if emptyAggregateCountRejected(node, filter) {
						result = 0
						break
					}
				}
			}
		case plan.Node_TABLE_SCAN:
			// At binding time names/tags still identify complete unique keys.
			// Capture this fact before physical pruning and index rewrites.
			if r.scanSingleRow(node) {
				result = 1
			}
		case plan.Node_VALUE_SCAN:
			if node.TableDef == nil {
				result = 1
			}
		case plan.Node_SINK_SCAN:
			if len(node.SourceStep) == 1 && node.SourceStep[0] >= 0 && int(node.SourceStep[0]) < len(query.Steps) {
				result = bound(query.Steps[node.SourceStep[0]])
			}
		case plan.Node_JOIN:
			if len(node.Children) == 2 {
				left, right := bound(node.Children[0]), bound(node.Children[1])
				reject := r.filtersRejectAll(node.OnList)
				switch node.JoinType {
				case plan.Node_INNER:
					if reject || left == 0 || right == 0 {
						result = 0
					} else {
						result = min(unknown, left*right)
					}
				case plan.Node_LEFT:
					if reject || right == 0 {
						result = left
					} else {
						result = min(unknown, left*right)
					}
				case plan.Node_RIGHT:
					if reject || left == 0 {
						result = right
					} else {
						result = min(unknown, left*right)
					}
				case plan.Node_SEMI:
					if reject || left == 0 || right == 0 {
						result = 0
					} else if r.constantProvider(node.Children[1]) {
						result = left
					}
				case plan.Node_ANTI, plan.Node_MARK:
					// A nonconstant EXISTS provider can require window execution
					// even when its preserved side is a single point lookup.
					// Its generated LIMIT/aggregate is not a source const-table fact.
					if reject || left == 0 || right == 0 {
						result = left
					} else if r.existenceConstant(node, nil) {
						result = left
					}
				case plan.Node_SINGLE:
					// Scalar evaluation does not add a MySQL FROM provider.
					result = left
				}
			}
		case plan.Node_UNION, plan.Node_UNION_ALL:
			if len(node.Children) > 0 {
				result = 0
				for _, id := range node.Children {
					result = min(unknown, result+bound(id))
				}
			}
		}
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_INNER && r.joinKeyFilter(node, nil, 0) {
			result = min(result, 1)
		}
		if r.filtersRejectAll(node.FilterList) {
			result = 0
		}
		result = min(result, resultPaginationBound(node.Limit, values))
		memo[id] = result
		return result
	}
	r.bound = bound
	type providerAddress struct {
		node  int32
		owner *BindContext
	}
	providers := make(map[providerAddress]bool)
	var provider func(int32, *BindContext) bool
	provider = func(id int32, owner *BindContext) bool {
		if !valid(id) || owner == nil {
			return false
		}
		address := providerAddress{id, owner}
		if value, ok := providers[address]; ok {
			return value
		}
		providers[address] = false
		block := r.blockOwner(id)
		if block == nil {
			return false
		}
		if block != owner && !block.integerPrecisionTransparentBoundary && r.materializedBlock(id, block) {
			return false
		}
		// Grouping/aggregate/HAVING subqueries are independent existence
		// consumers, not semijoin FROM providers. Their inputs need not be const.
		if block == owner && (len(owner.aggregates) > 0 || owner.hasHaving || !owner.isCorrelated && len(owner.groups) > 0) {
			providers[address] = true
			return true
		}
		n := query.Nodes[id]
		value := false
		switch n.NodeType {
		case plan.Node_TABLE_SCAN:
			value = r.scanSingleRow(n)
		case plan.Node_VALUE_SCAN:
			value = n.TableDef == nil
		case plan.Node_FILTER:
			if len(n.Children) == 1 && valid(n.Children[0]) && query.Nodes[n.Children[0]].NodeType == plan.Node_JOIN {
				value = r.joinKeyFilter(query.Nodes[n.Children[0]], n.FilterList, 0)
			}
			if len(n.Children) == 1 && valid(n.Children[0]) && query.Nodes[n.Children[0]].NodeType == plan.Node_TABLE_SCAN {
				scan := *query.Nodes[n.Children[0]]
				scan.FilterList = append(append([]*plan.Expr(nil), scan.FilterList...), n.FilterList...)
				value = r.scanSingleRow(&scan)
			}
		}
		if !value && len(n.Children) > 0 {
			value = true
			for _, child := range n.Children {
				value = value && provider(child, owner)
			}
		}
		providers[address] = value
		return value
	}
	r.constantProvider = func(id int32) bool { return provider(id, r.blockOwner(id)) }
	return r
}

func (r *resultMaterialization) blockOwner(id int32) *BindContext {
	if id < 0 || int(id) >= len(r.scopes) || r.scopes[id] == nil {
		return nil
	}
	return r.scopes[id].queryBlockOwner
}

func (r *resultMaterialization) materializedBlock(id int32, block *BindContext) bool {
	n := r.query.Nodes[id]
	return n.Limit != nil || n.Offset != nil || block.isDistinct || block.hasHaving ||
		len(block.aggregates) > 0 || len(block.groups) > 0 || len(block.windows) > 0 || len(block.times) > 0 ||
		n.NodeType == plan.Node_UNION || n.NodeType == plan.Node_UNION_ALL
}

// Filters above generated joins still constrain the preserved scan. Import
// literal/parameter constants, and the single output of an uncorrelated scalar,
// only through the actual typed JOIN route. Existence keeps its separate scope rule.
func (r *resultMaterialization) joinKeyFilter(join *plan.Node, filters []*plan.Expr, depth int) bool {
	if len(join.Children) != 2 || depth > len(r.query.Nodes) {
		return false
	}
	rightOwner := r.blockOwner(join.Children[1])
	scalar := rightOwner != nil && !rightOwner.isCorrelated && r.bound(join.Children[1]) <= 1 &&
		(join.JoinType == plan.Node_SINGLE || join.JoinType == plan.Node_INNER ||
			join.JoinType == plan.Node_LEFT && !r.joinOnUsesLeft(join))
	// A rejected outer-join provider cannot multiply its preserved input.
	admitted := scalar || join.JoinType == plan.Node_LEFT && (resultFiltersRejectAll(join.OnList) || r.bound(join.Children[1]) == 0)
	switch join.JoinType {
	case plan.Node_SEMI:
		admitted = r.constantProvider(join.Children[1])
	case plan.Node_ANTI, plan.Node_MARK:
		admitted = r.existenceConstant(join, filters)
	}
	if !admitted {
		// A positive correlated lookup can consume outer columns only after
		// the preserved input has independently established const access.
		positive := join.JoinType == plan.Node_SEMI || join.JoinType == plan.Node_MARK && resultPositiveExistenceFilter(filters, join)
		if !positive {
			return false
		}
		leftProofs := make([]*plan.Expr, 0, len(filters)+len(join.FilterList))
		for _, filter := range append(append([]*plan.Expr(nil), filters...), join.FilterList...) {
			leftProofs = append(leftProofs, resultProofExpr(filter, func(e *plan.Expr) *plan.Expr {
				route := r.keyJoinRoute(join, e)
				if route.childIdx == 0 && route.ref != nil {
					return &plan.Expr{Typ: e.Typ, Expr: &plan.Expr_Col{Col: route.ref}}
				}
				return e
			}))
		}
		leftConst := r.keyFilteredInput(join.Children[0], leftProofs, depth+1, r.blockOwner(join.NodeId))
		if !leftConst {
			return false
		}
		rightProofs := make([]*plan.Expr, 0, len(join.OnList))
		for _, predicate := range join.OnList {
			rightProofs = append(rightProofs, resultProofExpr(predicate, func(e *plan.Expr) *plan.Expr {
				route := r.keyJoinRoute(join, e)
				if route.childIdx == 0 {
					return makePlan2Int64ConstExprWithType(0)
				}
				if route.childIdx == 1 && route.ref != nil {
					return &plan.Expr{Typ: e.Typ, Expr: &plan.Expr_Col{Col: route.ref}}
				}
				return e
			}))
		}
		rightConst := r.keyFilteredInput(join.Children[1], rightProofs, depth+1, rightOwner)
		return rightConst
	}
	constraints := append(append([]*plan.Expr(nil), filters...), join.FilterList...)
	// ON constrains output rows only for an INNER join. Outer-join ON is not WHERE.
	if join.JoinType == plan.Node_INNER {
		constraints = append(constraints, join.OnList...)
	}
	proofs := make([]*plan.Expr, 0, len(constraints))
	for _, filter := range constraints {
		proofs = append(proofs, resultProofExpr(filter, func(e *plan.Expr) *plan.Expr {
			route := r.keyJoinRoute(join, e)
			if route.childIdx == 1 && scalar {
				// A statement-constant symbol, never a value installed in the plan.
				return makePlan2Int64ConstExprWithType(0)
			}
			if route.childIdx == 0 && route.ref != nil {
				return &plan.Expr{Typ: e.Typ, Expr: &plan.Expr_Col{Col: route.ref}}
			}
			return e
		}))
	}
	return r.keyFilteredInput(join.Children[0], proofs, depth+1, r.blockOwner(join.NodeId))
}

// Source proofs own logical binding references, not remapped JOIN output slots.
func (r *resultMaterialization) keyJoinRoute(join *plan.Node, e *plan.Expr) resultColumnJoinRoute {
	if len(r.scopes) > 0 && e.GetCol() != nil {
		return resultColumnJoinRoute{ref: e.GetCol(), childIdx: resultColumnJoinChildByBinding(r.query, join, e.GetCol())}
	}
	return resultColumnRouteAtJoin(r.query, join, e.GetCol(), true)
}

// Carry the same conjunct environment down the preserved input. Each scalar
// JOIN discharges only its own producer; later joins can then combine all key
// parts, including constants nested in deterministic scalar expressions.
func (r *resultMaterialization) keyFilteredInput(id int32, filters []*plan.Expr, depth int, owner *BindContext) bool {
	if id < 0 || int(id) >= len(r.query.Nodes) || depth > len(r.query.Nodes) {
		return false
	}
	n := r.query.Nodes[id]
	if n == nil {
		return false
	}
	// Forwarding a column does not license pushing an outer key predicate into
	// a materialized input. Its independent bound, if any, is handled by bound().
	if block := r.blockOwner(id); block != nil && block != owner && !block.integerPrecisionTransparentBoundary && r.materializedBlock(id, block) {
		return false
	}
	if n.NodeType == plan.Node_JOIN {
		return r.joinKeyFilter(n, filters, depth+1)
	}
	filters = append(append([]*plan.Expr(nil), filters...), n.FilterList...)
	if n.NodeType == plan.Node_TABLE_SCAN {
		scan := *n
		scan.FilterList = filters
		return r.scanSingleRow(&scan)
	}
	if !isResultColumnTransparentNode(n.NodeType) || len(n.Children) != 1 {
		return r.bound(id) <= 1
	}
	for i, filter := range filters {
		filters[i] = resultProofExpr(filter, func(e *plan.Expr) *plan.Expr {
			ref := e.GetCol()
			if len(r.scopes) > 0 && ref.ColPos >= 0 && int(ref.ColPos) < len(n.ProjectList) {
				for _, tag := range n.BindingTags {
					if ref.RelPos == tag {
						return n.ProjectList[ref.ColPos]
					}
				}
			}
			if projected := resultColumnProjectionAtNode(n, ref, true); projected != nil {
				return projected
			}
			return e
		})
	}
	return r.keyFilteredInput(n.Children[0], filters, depth+1, owner)
}

func resultProofExpr(e *plan.Expr, column func(*plan.Expr) *plan.Expr) *plan.Expr {
	if e.GetCol() != nil {
		return column(e)
	}
	fn := e.GetF()
	if fn == nil {
		return e
	}
	copyFn := *fn
	copyFn.Args = make([]*plan.Expr, len(fn.Args))
	for i, arg := range fn.Args {
		copyFn.Args[i] = resultProofExpr(arg, column)
	}
	copyExpr := *e
	copyExpr.Expr = &plan.Expr_F{F: &copyFn}
	return &copyExpr
}

func (r *resultMaterialization) existenceConstant(join *plan.Node, filters []*plan.Expr) bool {
	if len(join.Children) != 2 {
		return false
	}
	owner := r.blockOwner(join.Children[1])
	if owner == nil {
		return false
	}
	// A correlated global group remains an independently evaluated existence
	// consumer; it is not a row-producing semijoin FROM provider.
	if len(owner.aggregates) > 0 || owner.hasHaving {
		return true
	}
	correlated := owner.isCorrelated || resultMarkCorrelated(r.query, join)
	negative := join.JoinType == plan.Node_ANTI
	for _, filter := range filters {
		if fn := filter.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "not" && len(fn.Args) == 1 &&
			resultPositiveExistenceFilter(fn.Args, join) {
			negative = true
		}
	}
	if negative && !correlated {
		return true
	}
	return r.constantProvider(join.Children[1]) && (!resultMarkCorrelated(r.query, join) || resultPositiveExistenceFilter(filters, join))
}

func (r *resultMaterialization) columnNotNull(n *plan.Node, col *plan.ColRef) bool {
	if n.TableDef == nil || col.ColPos < 0 || int(col.ColPos) >= len(n.TableDef.Cols) {
		return false
	}
	def := n.TableDef.Cols[col.ColPos]
	if def.Typ.NotNullable {
		return true
	}
	if pk := n.TableDef.Pkey; pk != nil {
		if pk.PkeyColName == def.Name {
			return true
		}
		for _, name := range pk.Names {
			if name == def.Name {
				return true
			}
		}
	}
	return false
}

func (r *resultMaterialization) knownNonNull(e *plan.Expr) bool {
	if lit := e.GetLit(); lit != nil {
		return !lit.Isnull
	}
	if fn := e.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" && len(fn.Args) == 2 &&
		(types.T(e.Typ.Id).IsInteger() || types.T(e.Typ.Id) == types.T_bool) {
		return r.knownNonNull(fn.Args[0])
	}
	p := e.GetP()
	if p == nil || p.Pos < 0 || int(p.Pos) >= len(r.values) {
		return false
	}
	r.usedValues = true
	value := r.values[p.Pos]
	if typed, ok := value.(ParamValue); ok {
		value = typed.Value
	}
	return value != nil
}

func (r *resultMaterialization) elided(root int32, expr *plan.Expr) bool {
	if r.bound(root) == 0 {
		return true
	}
	// Resolve this selected value's producer, using the same typed slot/join
	// routing as result metadata classification. MIN/MAX is a terminal producer:
	// its input's inner windows belong to another query block, not this result.
	k := newResultColumnKinds(r.query)
	k.windowInputs = make(map[int32]bool)
	k.computed(root, expr)
	if len(k.windowInputs) == 0 {
		return false
	}
	for input := range k.windowInputs {
		if r.bound(input) > 1 {
			return false
		}
	}
	return true
}

func (r *resultMaterialization) joinOnUsesLeft(node *plan.Node) bool {
	var uses func(*plan.Expr) bool
	uses = func(e *plan.Expr) bool {
		if ref := e.GetCol(); ref != nil {
			if len(r.scopes) > 0 {
				return resultColumnJoinChildByBinding(r.query, node, ref) == 0
			}
			return ref.RelPos == 0
		}
		if fn := e.GetF(); fn != nil {
			for _, arg := range fn.Args {
				if uses(arg) {
					return true
				}
			}
		}
		return e.GetCorr() != nil
	}
	for _, e := range node.OnList {
		if uses(e) {
			return true
		}
	}
	return false
}

func resultMarkCorrelated(query *plan.Query, node *plan.Node) bool {
	k := newResultColumnKinds(query)
	var left func(*plan.Expr) bool
	left = func(e *plan.Expr) bool {
		if ref := e.GetCol(); ref != nil {
			if len(node.BindingTags) == 0 {
				return ref.RelPos == 0
			}
			return k.bindingChild(node.NodeId, node, ref) == 0
		}
		if fn := e.GetF(); fn != nil {
			for _, arg := range fn.Args {
				if left(arg) {
					return true
				}
			}
		}
		return e.GetCorr() != nil
	}
	for _, e := range node.OnList {
		if left(e) {
			return true
		}
	}
	return false
}

func resultPositiveExistenceFilter(filters []*plan.Expr, mark *plan.Node) bool {
	for _, e := range filters {
		fn := e.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "istrue" || len(fn.Args) != 1 {
			continue
		}
		ref := fn.Args[0].GetCol()
		if ref == nil {
			continue
		}
		if len(mark.BindingTags) > 0 {
			if ref.RelPos == mark.BindingTags[0] {
				return true
			}
			continue
		}
		if ref.RelPos == 0 && ref.ColPos >= 0 && int(ref.ColPos) < len(mark.ProjectList) {
			if output := mark.ProjectList[ref.ColPos].GetCol(); output != nil && output.RelPos == -1 {
				return true
			}
		}
	}
	return false
}

// Adapt the existing key-equality proof to either logical binding positions or
// the final pruned scan's physical positions, without mutating its table/filters.
func (r *resultMaterialization) scanSingleRow(node *plan.Node) bool {
	// Physical comparison/index rewrites may erase explicit-key transformations.
	// Only the binding owner can establish const access; result extraction may
	// still prove emptiness/pagination, but must not reconstruct lost eligibility.
	if node.TableDef == nil || len(r.scopes) == 0 {
		return false
	}
	scan := *node
	// Normalize only predicates which prove the same non-NULL key equality.
	// These private proof expressions are never installed in the executable plan.
	atoms := resultPredicateAtoms(node.FilterList)
	scan.FilterList = make([]*plan.Expr, len(atoms))
	for i, filter := range atoms {
		scan.FilterList[i] = filter
		fn := filter.GetF()
		if fn == nil || fn.Func == nil {
			continue
		}
		if (fn.Func.ObjName == "=" || fn.Func.ObjName == "<=>") && len(fn.Args) == 2 {
			copyFn := *fn
			copyFn.Args = append([]*plan.Expr(nil), fn.Args...)
			for side, arg := range copyFn.Args {
				if column := resultConstAccessColumn(arg); column != nil {
					copyFn.Args[side] = column
				}
			}
			fn = &copyFn
			scan.FilterList[i] = &plan.Expr{Typ: filter.Typ, Expr: &plan.Expr_F{F: fn}}
		}
		var args []*plan.Expr
		if fn.Func.ObjName == "<=>" && len(fn.Args) == 2 {
			for side := 0; side < 2; side++ {
				if col := fn.Args[side].GetCol(); col != nil && (r.columnNotNull(node, col) || r.knownNonNull(fn.Args[1-side])) {
					args = fn.Args
					break
				}
			}
		}
		if fn.Func.ObjName == "between" && len(fn.Args) == 3 && fn.Args[0].GetCol() != nil {
			lo, hi := fn.Args[1].GetLit(), fn.Args[2].GetLit()
			if lo != nil && hi != nil && !lo.Isnull && !hi.Isnull && fn.Args[1].Typ.Id == fn.Args[2].Typ.Id && fn.Args[1].Typ.Scale == fn.Args[2].Typ.Scale && proto.Equal(lo, hi) {
				args = fn.Args[:2]
			}
		}
		if args != nil {
			scan.FilterList[i] = &plan.Expr{Typ: filter.Typ, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "="}, Args: args}}}
		}
	}
	table := *node.TableDef
	if len(node.BindingTags) == 0 || table.Name2ColIndex == nil {
		table.Name2ColIndex = make(map[string]int32, len(table.Cols))
		for i, col := range table.Cols {
			table.Name2ColIndex[catalog.ResolveAlias(col.Name)] = int32(i)
		}
	}
	scan.TableDef = &table
	if len(scan.BindingTags) == 0 {
		scan.BindingTags = []int32{0}
	}
	if tableScanHasSingleRowFilter(&scan) {
		return true
	}
	// Physical composite-key rewriting may retain only the exact serialized
	// primary-key equality. A prefix equality is deliberately not sufficient.
	return table.Pkey != nil && table.Pkey.PkeyColName != "" &&
		filterListHasConstEqualityOnCols(scan.FilterList, &table, scan.BindingTags[0], []string{table.Pkey.PkeyColName})
}

// Injectivity is necessary but not sufficient for const-table access: MySQL
// does not use an explicitly transformed key as a direct field lookup.
func resultConstAccessColumn(e *plan.Expr) *plan.Expr {
	if e.GetCol() != nil {
		return e
	}
	for next := e; next.GetF() != nil; {
		fn := next.GetF()
		if fn.Func == nil || fn.Func.ObjName != "cast" || fn.SyntaxExplicitCast || len(fn.Args) != 2 {
			return nil
		}
		_, overload := function.DecodeOverloadID(fn.Func.Obj)
		if overload != 0 {
			return nil
		}
		next = fn.Args[0]
	}
	return integerComparisonCastColumn(e)
}

func resultExactRangeNumber(e *plan.Expr) (*big.Rat, bool) {
	if coefficient, scale, ok := exactDecimalCoefficient(e); ok {
		value, valid := new(big.Rat).SetString(coefficient.Format(scale))
		return value, valid
	}
	if !types.T(e.GetTyp().Id).IsInteger() || e.GetLit() == nil || e.GetLit().Isnull {
		return nil, false
	}
	var text string
	switch v := e.GetLit().Value.(type) {
	case *plan.Literal_I64Val:
		text = fmt.Sprint(v.I64Val)
	case *plan.Literal_U64Val:
		text = fmt.Sprint(v.U64Val)
	case *plan.Literal_I32Val:
		text = fmt.Sprint(v.I32Val)
	case *plan.Literal_U32Val:
		text = fmt.Sprint(v.U32Val)
	case *plan.Literal_I16Val:
		text = fmt.Sprint(v.I16Val)
	case *plan.Literal_U16Val:
		text = fmt.Sprint(v.U16Val)
	case *plan.Literal_I8Val:
		text = fmt.Sprint(v.I8Val)
	case *plan.Literal_U8Val:
		text = fmt.Sprint(v.U8Val)
	default:
		return nil, false
	}
	value, ok := new(big.Rat).SetString(text)
	return value, ok
}

func resultReversedExactBounds(lo, hi *plan.Expr) bool {
	if lo != nil && hi != nil && (types.T(lo.Typ.Id).IsDecimal() || types.T(hi.Typ.Id).IsDecimal()) {
		left, lok := resultExactRangeNumber(lo)
		right, rok := resultExactRangeNumber(hi)
		return lok && rok && left.Cmp(right) > 0
	}
	if lo.GetLit() == nil || hi.GetLit() == nil || lo.Typ.Id != hi.Typ.Id || !types.T(lo.Typ.Id).IsInteger() {
		return false
	}
	integer := func(e *plan.Expr) (uint64, bool, bool) {
		switch v := e.GetLit().Value.(type) {
		case *plan.Literal_I64Val:
			return uint64(v.I64Val), v.I64Val < 0, true
		case *plan.Literal_U64Val:
			return v.U64Val, false, true
		case *plan.Literal_I32Val:
			return uint64(v.I32Val), v.I32Val < 0, true
		case *plan.Literal_I16Val:
			return uint64(v.I16Val), v.I16Val < 0, true
		case *plan.Literal_I8Val:
			return uint64(v.I8Val), v.I8Val < 0, true
		case *plan.Literal_U32Val:
			return uint64(v.U32Val), false, true
		case *plan.Literal_U16Val:
			return uint64(v.U16Val), false, true
		case *plan.Literal_U8Val:
			return uint64(v.U8Val), false, true
		}
		return 0, false, false
	}
	l, ln, lok := integer(lo)
	h, hn, hok := integer(hi)
	return lok && hok && ((!ln && hn) || ln == hn && l > h)
}

// Push NOT through only exact three-valued Boolean identities before applying
// row-admission identities. In particular, OR NULL cannot be erased under NOT.
func resultPredicateNegation(e *plan.Expr, negate bool) *plan.Expr {
	if lit := e.GetLit(); lit != nil {
		if lit.Isnull || !negate {
			return e
		}
		if b, ok := lit.Value.(*plan.Literal_Bval); ok {
			return makePlan2BoolConstExprWithType(!b.Bval)
		}
	}
	fn := e.GetF()
	if fn != nil && fn.Func != nil {
		name := fn.Func.ObjName
		if name == "not" && len(fn.Args) == 1 {
			return resultPredicateNegation(fn.Args[0], !negate)
		}
		if name == "cast" && len(fn.Args) == 2 && types.T(e.Typ.Id) == types.T_bool {
			source := resultPredicateNegation(fn.Args[0], false)
			if lit := source.GetLit(); lit != nil && lit.Isnull {
				return &plan.Expr{Typ: e.Typ, Expr: &plan.Expr_Lit{Lit: lit}}
			}
		}
		if name == "istrue" && len(fn.Args) == 1 {
			source := resultPredicateNegation(fn.Args[0], false)
			if lit := source.GetLit(); lit != nil && lit.Isnull {
				return makePlan2BoolConstExprWithType(negate)
			}
		}
		if (name == "and" || name == "or") && len(fn.Args) == 2 {
			if negate {
				if name == "and" {
					name = "or"
				} else {
					name = "and"
				}
			}
			return &plan.Expr{Typ: e.Typ, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: name}, Args: []*plan.Expr{
				resultPredicateNegation(fn.Args[0], negate), resultPredicateNegation(fn.Args[1], negate),
			}}}}
		}
		if negate && len(fn.Args) == 2 {
			inverse := map[string]string{"=": "<>", "<>": "=", "!=": "=", "<": ">=", ">": "<=", "<=": ">", ">=": "<"}[name]
			if inverse != "" {
				return &plan.Expr{Typ: e.Typ, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: inverse}, Args: fn.Args}}}
			}
		}
	}
	if !negate {
		return e
	}
	return &plan.Expr{Typ: e.Typ, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "not"}, Args: []*plan.Expr{e}}}}
}

// Normalize row-admission predicates only, not arbitrary Boolean values.
// In WHERE/ON, X OR NULL and X OR FALSE admit exactly the rows admitted by X.
func resultPredicateAtoms(filters []*plan.Expr) []*plan.Expr {
	var normalize func(*plan.Expr) *plan.Expr
	normalize = func(e *plan.Expr) *plan.Expr {
		fn := e.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "or" || len(fn.Args) != 2 {
			return e
		}
		left, right := normalize(fn.Args[0]), normalize(fn.Args[1])
		reject := func(e *plan.Expr) bool {
			if IsFalseExpr(e) {
				return true
			}
			for fn := e.GetF(); fn != nil && fn.Func != nil && ((fn.Func.ObjName == "cast" && len(fn.Args) == 2) || (fn.Func.ObjName == "istrue" && len(fn.Args) == 1)); fn = e.GetF() {
				e = fn.Args[0]
			}
			return e.GetLit() != nil && e.GetLit().Isnull
		}
		if reject(left) {
			return right
		}
		if reject(right) || proto.Equal(left, right) {
			return left
		}
		return e
	}
	var atoms []*plan.Expr
	var collect func(*plan.Expr)
	collect = func(e *plan.Expr) {
		e = normalize(e)
		if fn := e.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "and" {
			for _, arg := range fn.Args {
				collect(arg)
			}
			return
		}
		atoms = append(atoms, e)
	}
	for _, filter := range filters {
		collect(resultPredicateNegation(filter, false))
	}
	return atoms
}

func (r *resultMaterialization) filtersRejectAll(filters []*plan.Expr) bool {
	proofs := make([]*plan.Expr, len(filters))
	var predicate func(*plan.Expr) *plan.Expr
	predicate = func(e *plan.Expr) *plan.Expr {
		fn := e.GetF()
		if fn == nil || fn.Func == nil {
			return e
		}
		copyFn := *fn
		copyFn.Args = append([]*plan.Expr(nil), fn.Args...)
		switch fn.Func.ObjName {
		case "and", "or":
			for i, arg := range copyFn.Args {
				copyFn.Args[i] = predicate(arg)
			}
		case "between", "<=", ">=", "<", ">":
			for i, arg := range copyFn.Args {
				copyFn.Args[i] = r.rangeValue(arg)
			}
		default:
			return e
		}
		copyExpr := *e
		copyExpr.Expr = &plan.Expr_F{F: &copyFn}
		return &copyExpr
	}
	for i, filter := range filters {
		proofs[i] = predicate(filter)
	}
	return resultFiltersRejectAll(proofs)
}

// Interpret only admitted integer/null transport, never execute expressions or
// guess string/float conversions. The source owner records value dependence.
func (r *resultMaterialization) rangeValue(e *plan.Expr) *plan.Expr {
	if p := e.GetP(); p != nil && p.Pos >= 0 && int(p.Pos) < len(r.values) {
		value := r.values[p.Pos]
		var wireType types.T
		wireInteger := false
		if typed, ok := value.(ParamValue); ok {
			value = typed.Value
			if typed.HasRuntimeType && typed.RuntimeType.Oid.IsDecimal() {
				if text, ok := value.(string); ok {
					domain, _, canonical, ok := PreparedDecimalRuntimeDomains(text)
					if !ok {
						return e
					}
					coefficient, err := types.ParseDecimal256(canonical, domain.Width, domain.Scale)
					if err != nil {
						return e
					}
					// Runtime canonical text may use an exponent; convert it to a
					// bounded exact plain decimal, not a floating-point spelling.
					decimal, err := makePlan2DecimalExprWithType(context.Background(), coefficient.Format(domain.Scale))
					if err == nil {
						r.usedValues = true
						return decimal
					}
					return e
				}
			}
			// COM_STMT_EXECUTE stores even numeric packets as text. Trust only
			// the admitted integer runtime domain, never a bare string spelling.
			if text, ok := value.(string); ok && typed.HasRuntimeType && typed.RuntimeType.Oid.IsInteger() {
				wireType = typed.RuntimeType.Oid
				bits := int(wireType.ToType().Size) * 8
				var err error
				if wireType.IsUnsignedInt() {
					value, err = strconv.ParseUint(text, 10, bits)
				} else {
					value, err = strconv.ParseInt(text, 10, bits)
				}
				if err != nil {
					return e
				}
				wireInteger = true
			}
		}
		var literal *plan.Literal
		source := types.T_int64
		switch v := value.(type) {
		case nil:
			literal = &plan.Literal{Isnull: true}
		case int8:
			source = types.T_int8
			literal = &plan.Literal{Value: &plan.Literal_I64Val{I64Val: int64(v)}}
		case int16:
			source = types.T_int16
			literal = &plan.Literal{Value: &plan.Literal_I64Val{I64Val: int64(v)}}
		case int32:
			source = types.T_int32
			literal = &plan.Literal{Value: &plan.Literal_I64Val{I64Val: int64(v)}}
		case int64:
			literal = &plan.Literal{Value: &plan.Literal_I64Val{I64Val: v}}
		case uint8:
			source = types.T_uint8
			literal = &plan.Literal{Value: &plan.Literal_U64Val{U64Val: uint64(v)}}
		case uint16:
			source = types.T_uint16
			literal = &plan.Literal{Value: &plan.Literal_U64Val{U64Val: uint64(v)}}
		case uint32:
			source = types.T_uint32
			literal = &plan.Literal{Value: &plan.Literal_U64Val{U64Val: uint64(v)}}
		case uint64:
			source = types.T_uint64
			literal = &plan.Literal{Value: &plan.Literal_U64Val{U64Val: v}}
		}
		if wireInteger {
			source = wireType
		}
		if literal != nil {
			target := types.T(e.Typ.Id)
			if literal.Isnull || target == source || integerDomainFits(source, target) {
				r.usedValues = true
				return &plan.Expr{Typ: e.Typ, Expr: &plan.Expr_Lit{Lit: literal}}
			}
			// SQL EXECUTE's generic marker is a TEXT transport envelope even
			// when its admitted user variable has an integer value.
			if target == types.T_text {
				r.usedValues = true
				return &plan.Expr{Typ: source.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: literal}}
			}
		}
	}
	if fn := e.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" && len(fn.Args) == 2 &&
		(types.T(e.Typ.Id).IsInteger() || types.T(e.Typ.Id).IsDecimal()) {
		source := r.rangeValue(fn.Args[0])
		if coefficient, scale, ok := exactDecimalCoefficient(source); ok && types.T(e.Typ.Id).IsDecimal() {
			precision := e.Typ.Width
			if precision == 0 {
				precision = types.T(e.Typ.Id).ToType().Width
			}
			if precision > 0 && precision <= types.T(e.Typ.Id).ToType().Width && e.Typ.Scale >= 0 && e.Typ.Scale <= precision {
				converted, err := types.ParseDecimal256(coefficient.Format(scale), precision, e.Typ.Scale)
				if err == nil {
					before, _ := new(big.Rat).SetString(coefficient.Format(scale))
					after, _ := new(big.Rat).SetString(converted.Format(e.Typ.Scale))
					if before != nil && after != nil && before.Cmp(after) == 0 {
						return source
					}
				}
			}
		}
		if lit := source.GetLit(); lit != nil {
			if lit.Isnull {
				return &plan.Expr{Typ: e.Typ, Expr: &plan.Expr_Lit{Lit: lit}}
			}
			if integerDomainFits(types.T(source.Typ.Id), types.T(e.Typ.Id)) ||
				types.T(source.Typ.Id).IsInteger() && localCTEIntegerLiteralFits(lit, types.T(e.Typ.Id)) {
				return &plan.Expr{Typ: e.Typ, Expr: &plan.Expr_Lit{Lit: lit}}
			}
			// The legacy TEXT -> DECIMAL -> integer parameter adapter is exact
			// only when the whole admitted integer domain fits its decimal carrier.
			if types.T(source.Typ.Id).IsInteger() && types.T(e.Typ.Id).IsDecimal() && e.Typ.Scale >= 0 {
				precision := e.Typ.Width
				if precision == 0 {
					precision = types.T(e.Typ.Id).ToType().Width
				}
				if precision <= types.T(e.Typ.Id).ToType().Width && integerDecimalDigits(types.T(source.Typ.Id)) <= precision-e.Typ.Scale {
					return source
				}
			}
		}
	}
	return e
}

func resultFiltersRejectAll(filters []*plan.Expr) bool {
	// BETWEEN's fallback uses >= AND <=; keep the same admission normalization.
	atoms := resultPredicateAtoms(filters)
	type column struct{ relation, position int32 }
	type interval struct{ lower, upper *plan.Expr }
	bounds := make(map[column]interval)
	for _, filter := range atoms {
		fn := filter.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) != 2 {
			continue
		}
		switch fn.Func.ObjName {
		case "=", "!=", "<>", "<", ">", "<=", ">=":
			for _, arg := range fn.Args {
				if lit := arg.GetLit(); lit != nil && lit.Isnull {
					return true
				}
			}
		}
		if fn.Func.ObjName != "<=" && fn.Func.ObjName != ">=" {
			continue
		}
		columnExpr := resultConstAccessColumn(fn.Args[0])
		ref := columnExpr.GetCol()
		if ref == nil {
			continue
		}
		if fn.Args[1].GetLit() == nil {
			if _, exact := resultExactRangeNumber(fn.Args[1]); !exact {
				continue
			}
		}
		key := column{ref.RelPos, ref.ColPos}
		bound := bounds[key]
		if fn.Func.ObjName == ">=" {
			if bound.lower == nil || resultReversedExactBounds(fn.Args[1], bound.lower) {
				bound.lower = fn.Args[1]
			}
		} else {
			if bound.upper == nil || resultReversedExactBounds(bound.upper, fn.Args[1]) {
				bound.upper = fn.Args[1]
			}
		}
		if resultReversedExactBounds(bound.lower, bound.upper) {
			return true
		}
		bounds[key] = bound
	}
	for _, filter := range atoms {
		// NULL and FALSE both reject a row in WHERE/HAVING/ON, but an outer
		// join still preserves its designated side (handled by the caller).
		if IsFalseExpr(filter) || filter.GetLit() != nil && filter.GetLit().Isnull {
			return true
		}
		if fn := filter.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "between" && len(fn.Args) == 3 {
			for _, arg := range fn.Args {
				if lit := arg.GetLit(); lit != nil && lit.Isnull {
					return true
				}
			}
			if resultReversedExactBounds(fn.Args[1], fn.Args[2]) {
				return true
			}
		}
	}
	return false
}

// Pagination treats admitted NULL and false as zero. Integer/boolean transport
// casts preserve zero/one; arbitrary expressions and unbound markers are unknown.
func resultPaginationBound(expr *plan.Expr, values []any) uint8 {
	if expr == nil {
		return 2
	}
	if lit := expr.GetLit(); lit != nil {
		if lit.Isnull {
			return 0
		}
		switch v := lit.Value.(type) {
		case *plan.Literal_U64Val:
			return uint8(min(v.U64Val, 2))
		case *plan.Literal_I64Val:
			if v.I64Val >= 0 {
				return uint8(min(v.I64Val, 2))
			}
		case *plan.Literal_Bval:
			if !v.Bval {
				return 0
			}
			return 1
		}
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" && len(fn.Args) == 2 &&
		(types.T(expr.Typ.Id).IsInteger() || types.T(expr.Typ.Id) == types.T_bool) {
		return resultPaginationBound(fn.Args[0], values)
	}
	param := expr.GetP()
	if param == nil || param.Pos < 0 || int(param.Pos) >= len(values) {
		return 2
	}
	value := values[param.Pos]
	valid, negative := validatePreparedPaginationValue(value)
	if !valid || negative {
		return 2
	}
	if typed, ok := value.(ParamValue); ok {
		value = typed.Value
	}
	if value == nil {
		return 0
	}
	if boolean, ok := value.(bool); ok {
		if !boolean {
			return 0
		}
		return 1
	}
	text, ok := value.(string)
	if !ok {
		text = fmt.Sprint(value)
	}
	text = strings.TrimLeft(strings.TrimPrefix(text, "-"), "0")
	if text == "" {
		return 0
	}
	if text == "1" {
		return 1
	}
	return 2
}

// Recognize a lowered HAVING COUNT(*) > 0 over an empty global aggregate.
// The remapped -2 relation addresses AGG slots, not its projected outputs.
func emptyAggregateCountRejected(agg *plan.Node, filter *plan.Expr) bool {
	fn := filter.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != ">" || len(fn.Args) != 2 || !resultIntegerLiteralZero(fn.Args[1]) {
		return false
	}
	col := fn.Args[0].GetCol()
	if col == nil || col.ColPos < 0 {
		return false
	}
	source := col
	if col.RelPos != -2 {
		if int(col.ColPos) >= len(agg.ProjectList) {
			return false
		}
		source = agg.ProjectList[col.ColPos].GetCol()
	}
	if source == nil || source.RelPos != -2 || source.ColPos < 0 || int(source.ColPos) >= len(agg.AggList) {
		return false
	}
	count := agg.AggList[source.ColPos].GetF()
	if count == nil || count.Func == nil {
		return false
	}
	id, _ := function.DecodeOverloadID(count.Func.Obj)
	return id == function.COUNT || id == function.STARCOUNT
}
