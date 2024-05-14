// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

func (builder *QueryBuilder) mergeFiltersOnCompositeKey(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType != plan.Node_TABLE_SCAN {
		for _, childID := range node.Children {
			builder.mergeFiltersOnCompositeKey(childID)
		}

		return
	}

	if node.TableDef.Pkey == nil || len(node.TableDef.Pkey.Names) == 1 {
		return
	}

	newFilterList, rewrite := builder.doMergeFiltersOnCompositeKey(node.TableDef, node.BindingTags[0], node.FilterList...)
	if rewrite {
		node.FilterList = newFilterList
		calcScanStats(node, builder)
	}
}

func (builder *QueryBuilder) doMergeFiltersOnCompositeKey(tableDef *plan.TableDef, tableTag int32, filters ...*plan.Expr) ([]*plan.Expr, bool) {
	col2filter := make(map[int32]int)
	for i, expr := range filters {
		fn := expr.GetF()
		if fn == nil {
			continue
		}

		if fn.Func.ObjName == "=" {
			if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
				fn.Args[0], fn.Args[1] = fn.Args[1], fn.Args[0]
			}

			col := fn.Args[0].GetCol()
			if col == nil || !isRuntimeConstExpr(fn.Args[1]) {
				continue
			}

			col2filter[col.ColPos] = i
		} else if fn.Func.ObjName == "or" {
			var orArgs []*plan.Expr
			flattenLogicalExpressions(expr, "or", &orArgs)

			for j, subExpr := range orArgs {
				subFn := subExpr.GetF()
				if subFn == nil {
					continue
				}

				if subFn.Func.ObjName == "=" {
					newArgs, rewrite := builder.doMergeFiltersOnCompositeKey(tableDef, tableTag, subExpr)
					if rewrite {
						fn.Args[j] = newArgs[0]
					}
				} else if subFn.Func.ObjName == "and" {
					var andArgs []*plan.Expr
					flattenLogicalExpressions(subExpr, "and", &andArgs)

					newArgs, rewrite := builder.doMergeFiltersOnCompositeKey(tableDef, tableTag, andArgs...)
					if rewrite {
						if len(newArgs) == 1 {
							orArgs[j] = newArgs[0]
						} else {
							subFn.Args = newArgs
						}
					}
				}
			}

			fn.Args = orArgs
		}
	}

	numParts := len(tableDef.Pkey.Names)
	filterIdx := make([]int, 0, numParts)
	for _, part := range tableDef.Pkey.Names {
		colIdx := tableDef.Name2ColIndex[part]
		idx, ok := col2filter[colIdx]
		if !ok {
			break
		}

		filterIdx = append(filterIdx, idx)
	}

	if len(filterIdx) == 0 {
		return filters, false
	}

	serialArgs := make([]*plan.Expr, len(filterIdx))
	for i := range filterIdx {
		serialArgs[i] = filters[filterIdx[i]].GetF().Args[1]
	}
	rightArg, _ := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "serial", serialArgs)

	pkIdx := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	pkExpr := &plan.Expr{
		Typ: tableDef.Cols[pkIdx].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: tableTag,
				ColPos: pkIdx,
			},
		},
	}

	funcName := "="
	if len(filterIdx) < numParts {
		funcName = "prefix_eq"
	}
	compositePKFilter, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, []*plan.Expr{
		pkExpr,
		rightArg,
	})

	hitFilterSet := make(map[int]bool)
	for i := range filterIdx {
		hitFilterSet[filterIdx[i]] = true
	}

	newFilterList := make([]*plan.Expr, 0, len(filters)-len(filterIdx)+1)
	newFilterList = append(newFilterList, compositePKFilter)
	for i, filter := range filters {
		if !hitFilterSet[i] {
			newFilterList = append(newFilterList, filter)
		}
	}

	return newFilterList, true
}

func flattenLogicalExpressions(expr *plan.Expr, opName string, args *[]*plan.Expr) {
	fn := expr.GetF()
	if fn == nil || fn.Func.ObjName != opName {
		*args = append(*args, expr)
		return
	}

	for _, arg := range fn.Args {
		flattenLogicalExpressions(arg, opName, args)
	}
}
