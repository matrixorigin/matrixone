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
	"fmt"
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
				if agg.NodeType == plan.Node_JOIN && r.joinKeyFilter(agg, node.FilterList) {
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
				reject := resultFiltersRejectAll(node.OnList)
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
					} else if r.constantProvider(node.Children[1]) && !resultMarkCorrelated(query, node) {
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
		if resultFiltersRejectAll(node.FilterList) {
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
		if block == owner && !owner.isCorrelated && (len(owner.aggregates) > 0 || len(owner.groups) > 0 || owner.hasHaving) {
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
func (r *resultMaterialization) joinKeyFilter(join *plan.Node, filters []*plan.Expr) bool {
	if len(join.Children) != 2 {
		return false
	}
	rightOwner := r.blockOwner(join.Children[1])
	scalar := rightOwner != nil && !rightOwner.isCorrelated && !resultMarkCorrelated(r.query, join) && r.bound(join.Children[1]) <= 1 &&
		(join.JoinType == plan.Node_LEFT || join.JoinType == plan.Node_SINGLE || join.JoinType == plan.Node_INNER)
	left := r.query.Nodes[join.Children[0]]
	if left == nil || left.NodeType != plan.Node_TABLE_SCAN {
		return false
	}
	scan := *left
	scan.FilterList = append([]*plan.Expr(nil), left.FilterList...)
	for _, filter := range filters {
		fn := filter.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 {
			continue
		}
		for side := 0; side < 2; side++ {
			column := fn.Args[side]
			if column.GetCol() == nil {
				column = integerComparisonCastColumn(column)
			}
			lhs := column.GetCol()
			if lhs == nil {
				continue
			}
			l := resultColumnRouteAtJoin(r.query, join, lhs, true)
			if l.childIdx != 0 || l.ref == nil {
				continue
			}
			value := fn.Args[1-side]
			if rhs := value.GetCol(); rhs != nil {
				route := resultColumnRouteAtJoin(r.query, join, rhs, true)
				if !scalar || route.childIdx != 1 {
					continue
				}
				// Private proof symbol for a once-evaluated scalar, never data.
				value = makePlan2Int64ConstExprWithType(0)
			} else if !isRuntimeConstExpr(value) {
				continue
			}
			key := &plan.Expr{Typ: fn.Args[side].Typ, Expr: &plan.Expr_Col{Col: l.ref}}
			scan.FilterList = append(scan.FilterList, &plan.Expr{Typ: filter.Typ, Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "="}, Args: []*plan.Expr{key, value},
			}}})
		}
	}
	if !r.scanSingleRow(&scan) {
		return false
	}
	switch join.JoinType {
	case plan.Node_LEFT, plan.Node_SINGLE, plan.Node_INNER:
		return scalar
	case plan.Node_SEMI:
		return r.constantProvider(join.Children[1])
	case plan.Node_ANTI, plan.Node_MARK:
		return r.constantProvider(join.Children[1]) && (!resultMarkCorrelated(r.query, join) ||
			join.JoinType == plan.Node_MARK && resultPositiveExistenceFilter(filters, join))
	}
	return false
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
	if node.TableDef == nil {
		return false
	}
	scan := *node
	// Normalize only predicates which prove the same non-NULL key equality.
	// These private proof expressions are never installed in the executable plan.
	scan.FilterList = make([]*plan.Expr, len(node.FilterList))
	for i, filter := range node.FilterList {
		scan.FilterList[i] = filter
		fn := filter.GetF()
		if fn == nil || fn.Func == nil {
			continue
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

func resultFiltersRejectAll(filters []*plan.Expr) bool {
	for _, filter := range filters {
		// NULL and FALSE both reject a row in WHERE/HAVING/ON, but an outer
		// join still preserves its designated side (handled by the caller).
		if IsFalseExpr(filter) || filter.GetLit() != nil && filter.GetLit().Isnull {
			return true
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
