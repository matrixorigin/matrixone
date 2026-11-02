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
func resultMaterializationElided(query *plan.Query, root int32, values []any) bool {
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
		case plan.Node_PROJECT, plan.Node_FILTER, plan.Node_SORT, plan.Node_DISTINCT, plan.Node_WINDOW, plan.Node_SINK:
			result = child()
			if node.NodeType == plan.Node_FILTER && len(node.Children) == 1 && valid(node.Children[0]) {
				agg := query.Nodes[node.Children[0]]
				if agg.NodeType == plan.Node_TABLE_SCAN {
					scan := *agg
					scan.FilterList = append(append([]*plan.Expr(nil), agg.FilterList...), node.FilterList...)
					if resultScanSingleRow(&scan) {
						result = min(result, 1)
					}
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
			} else if len(node.GroupingFlag) == 0 && !IsSortRollupOption(node.ExtraOptions) {
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
			if resultScanSingleRow(node) {
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
					}
				case plan.Node_ANTI, plan.Node_MARK:
					// A nonconstant EXISTS provider can require window execution
					// even when its preserved side is a single point lookup.
					// Its generated LIMIT/aggregate is not a source const-table fact.
					if reject || left == 0 || right == 0 {
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
	if bound(root) == 0 {
		return true
	}
	// Inspect the root block's window layer, not unrelated windows below an
	// aggregate/join/materialized source. Every window in this layer must be
	// redundant; an outer LIMIT must not hide an active inner window.
	found := false
	for depth := 0; depth < len(query.Nodes) && valid(root); depth++ {
		node := query.Nodes[root]
		switch node.NodeType {
		case plan.Node_PROJECT, plan.Node_FILTER, plan.Node_SORT, plan.Node_DISTINCT:
		case plan.Node_WINDOW:
			if len(node.Children) != 1 || bound(node.Children[0]) > 1 {
				return false
			}
			found = true
		default:
			return found
		}
		if len(node.Children) != 1 {
			return false
		}
		root = node.Children[0]
	}
	return false
}

// Adapt the existing key-equality proof to either logical binding positions or
// the final pruned scan's physical positions, without mutating its table/filters.
func resultScanSingleRow(node *plan.Node) bool {
	if node.TableDef == nil {
		return false
	}
	scan := *node
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
