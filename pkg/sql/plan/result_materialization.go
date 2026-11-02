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

// resultStaticallyEmpty proves emptiness from executable constants, never from
// cardinality estimates or observed data. A global aggregate is NOT empty on
// empty input; grouping-set expansion explicitly guards its COUNT(*) > 0 row.
// Memoization bounds work by the reachable query DAG, including shared inputs.
func resultStaticallyEmpty(query *plan.Query, root int32, parameters ...[]any) bool {
	var values []any
	if len(parameters) > 0 {
		values = parameters[0]
	}
	memo := make(map[int32]bool)
	var empty func(int32) bool
	empty = func(id int32) bool {
		if id < 0 || int(id) >= len(query.Nodes) || query.Nodes[id] == nil {
			return false
		}
		if result, found := memo[id]; found {
			return result
		}
		memo[id] = false // defensive cycle guard; cycles do not prove emptiness
		node := query.Nodes[id]
		result := resultLimitZero(node.Limit, values)
		for _, filter := range node.FilterList {
			result = result || IsFalseExpr(filter)
		}
		if !result {
			switch node.NodeType {
			case plan.Node_PROJECT, plan.Node_FILTER, plan.Node_SORT, plan.Node_DISTINCT, plan.Node_WINDOW:
				result = len(node.Children) == 1 && empty(node.Children[0])
				if !result && node.NodeType == plan.Node_FILTER && len(node.Children) == 1 &&
					node.Children[0] >= 0 && int(node.Children[0]) < len(query.Nodes) {
					child := query.Nodes[node.Children[0]]
					if child != nil && child.NodeType == plan.Node_AGG && len(child.GroupBy) == 0 && len(child.Children) == 1 && empty(child.Children[0]) {
						for _, filter := range node.FilterList {
							if emptyAggregateCountRejected(child, filter) {
								result = true
								break
							}
						}
					}
				}
			case plan.Node_AGG:
				if len(node.Children) == 1 && empty(node.Children[0]) {
					result = len(node.GroupBy) > 0
					// HAVING may be pushed into the aggregate's own filter list.
					// Its references address aggregate slots, not projected slots.
					if !result {
						for _, filter := range node.FilterList {
							if emptyAggregateCountRejected(node, filter) {
								result = true
								break
							}
						}
					}
				}
			case plan.Node_UNION, plan.Node_UNION_ALL:
				result = len(node.Children) > 0
				for _, child := range node.Children {
					result = result && empty(child)
				}
			}
		}
		memo[id] = result
		return result
	}
	return empty(root)
}

// Only admitted pagination values and their transport casts are inspected.
// Never evaluate arbitrary expressions or resolve an unbound prepare marker.
func resultLimitZero(expr *plan.Expr, values []any) bool {
	if expr == nil {
		return false
	}
	if resultIntegerLiteralZero(expr) {
		return true
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" && len(fn.Args) == 2 &&
		types.T(expr.Typ.Id).IsInteger() {
		return resultLimitZero(fn.Args[0], values)
	}
	param := expr.GetP()
	if param == nil || param.Pos < 0 || int(param.Pos) >= len(values) {
		return false
	}
	value := values[param.Pos]
	valid, negative := validatePreparedPaginationValue(value)
	if !valid || negative {
		return false
	}
	if typed, ok := value.(ParamValue); ok {
		value = typed.Value
	}
	if boolean, ok := value.(bool); ok {
		return !boolean
	}
	if value == nil {
		return false
	}
	// Validation above admits only integral types or integral protocol text.
	return strings.TrimLeft(strings.TrimPrefix(fmt.Sprint(value), "-"), "0") == ""
}

// Recognize the lowered grouping-set HAVING COUNT(*) > 0 over a statically
// empty global aggregate. Follow physical output slots, not names or SQL text.
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
	if col.RelPos != -2 { // -2 is the remapped AGG-result relation.
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
