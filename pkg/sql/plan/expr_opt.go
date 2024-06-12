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

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func (builder *QueryBuilder) mergeFiltersOnCompositeKey(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType != plan.Node_TABLE_SCAN {
		for _, childID := range node.Children {
			builder.mergeFiltersOnCompositeKey(childID)
		}

		return
	}

	if node.TableDef.Pkey == nil {
		return
	}

	newFilterList := builder.doMergeFiltersOnCompositeKey(node.TableDef, node.BindingTags[0], node.FilterList...)
	node.FilterList = newFilterList
	node.Stats = calcScanStats(node, builder)
	resetHashMapStats(node.Stats)
}

func (builder *QueryBuilder) doMergeFiltersOnCompositeKey(tableDef *plan.TableDef, tableTag int32, filters ...*plan.Expr) []*plan.Expr {
	pkIdx := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	col2filter := make(map[int32]int)
	numParts := len(tableDef.Pkey.Names)

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
		} else if fn.Func.ObjName == "in" {
			if fn.Args[0].GetCol() == nil {
				continue
			}

			col := fn.Args[0].GetCol()
			if _, ok := col2filter[col.ColPos]; ok {
				continue
			}

			col2filter[col.ColPos] = i
		} else if fn.Func.ObjName == "or" {
			var orArgs []*plan.Expr
			flattenLogicalExpressions(expr, "or", &orArgs)

			newOrArgs := make([]*plan.Expr, 0, len(orArgs))
			var inArgs []*plan.Expr
			var firstEquiExpr *plan.Expr
			pkFnName := "in"

			currColPos := int32(-1)
			for _, subExpr := range orArgs {
				subFn := subExpr.GetF()
				if subFn == nil {
					newOrArgs = append(newOrArgs, subExpr)
					continue
				}

				if subFn.Func.ObjName == "=" {
					if numParts > 1 {
						newArgs := builder.doMergeFiltersOnCompositeKey(tableDef, tableTag, subExpr)
						subExpr = newArgs[0]
					}
				} else if subFn.Func.ObjName == "and" {
					var andArgs []*plan.Expr
					flattenLogicalExpressions(subExpr, "and", &andArgs)

					newArgs := builder.doMergeFiltersOnCompositeKey(tableDef, tableTag, andArgs...)
					if len(newArgs) == 1 {
						subExpr = newArgs[0]
					} else {
						subFn.Args = newArgs
					}
				}

				mergedFn := subExpr.GetF()
				if mergedFn == nil || len(mergedFn.Args) != 2 || mergedFn.Args[0].GetCol() == nil || !isRuntimeConstExpr(mergedFn.Args[1]) {
					newOrArgs = append(newOrArgs, subExpr)
					continue
				}

				if currColPos == -1 {
					currColPos = mergedFn.Args[0].GetCol().ColPos
				} else if currColPos != mergedFn.Args[0].GetCol().ColPos {
					newOrArgs = append(newOrArgs, subExpr)
					continue
				}

				if len(inArgs) == 0 {
					firstEquiExpr = subExpr
				}

				switch mergedFn.Func.ObjName {
				case "=":
					inArg := mergedFn.Args[1]
					if inArg.GetF() != nil && inArg.GetF().Func.ObjName == "cast" {
						inArg = inArg.GetF().Args[0]
					}
					inArgs = append(inArgs, inArg)

				case "prefix_eq":
					inArgs = append(inArgs, mergedFn.Args[1])
					pkFnName = "prefix_in"

				default:
					newOrArgs = append(newOrArgs, subExpr)
				}
			}

			if len(inArgs) == 1 {
				newOrArgs = append(newOrArgs, firstEquiExpr)
			} else if len(inArgs) > 1 {
				leftExpr := firstEquiExpr.GetF().Args[0]
				inExpr, _ := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), pkFnName, []*plan.Expr{
					leftExpr,
					{
						Typ: leftExpr.Typ,
						Expr: &plan.Expr_List{
							List: &plan.ExprList{
								List: inArgs,
							},
						},
					},
				})

				newOrArgs = append(newOrArgs, inExpr)
			}

			if len(newOrArgs) == 1 {
				filters[i] = newOrArgs[0]
				colPos := firstEquiExpr.GetF().Args[0].GetCol().ColPos
				if colPos != pkIdx {
					col2filter[colPos] = i
				}
			} else {
				fn.Args = newOrArgs
			}
		}
	}

	if numParts == 1 {
		return filters
	}

	filterIdx := make([]int, 0, numParts)
	for _, part := range tableDef.Pkey.Names {
		colIdx := tableDef.Name2ColIndex[part]
		idx, ok := col2filter[colIdx]
		if !ok {
			break
		}

		filterIdx = append(filterIdx, idx)
		if filters[idx].GetF().Func.ObjName == "in" {
			break
		}
	}

	if len(filterIdx) == 0 {
		return filters
	}

	var compositePKFilter *plan.Expr
	pkExpr := &plan.Expr{
		Typ: tableDef.Cols[pkIdx].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: tableTag,
				ColPos: pkIdx,
			},
		},
	}

	lastFilter := filters[filterIdx[len(filterIdx)-1]]
	if lastFilter.GetF().Func.ObjName == "in" {
		serialArgs := make([]*plan.Expr, len(filterIdx)-1)
		for i := 0; i < len(filterIdx)-1; i++ {
			serialArgs[i] = filters[filterIdx[i]].GetF().Args[1]
		}

		inArgs := make([]*plan.Expr, len(lastFilter.GetF().Args[1].GetList().List))
		for i, lastArg := range lastFilter.GetF().Args[1].GetList().List {
			tmpSerialArgs := DeepCopyExprList(serialArgs)
			tmpSerialArgs = append(tmpSerialArgs, lastArg)
			rightArg, _ := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "serial", tmpSerialArgs)
			inArgs[i] = rightArg
		}

		funcName := "in"
		if len(filterIdx) < numParts {
			funcName = "prefix_in"
		}

		compositePKFilter, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, []*plan.Expr{
			pkExpr,
			{
				Typ: pkExpr.Typ,
				Expr: &plan.Expr_List{
					List: &plan.ExprList{
						List: inArgs,
					},
				},
			},
		})
	} else {
		serialArgs := make([]*plan.Expr, len(filterIdx))
		for i := range filterIdx {
			serialArgs[i] = filters[filterIdx[i]].GetF().Args[1]
		}
		rightArg, _ := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "serial", serialArgs)

		funcName := "="
		if len(filterIdx) < numParts {
			funcName = "prefix_eq"
		}

		compositePKFilter, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, []*plan.Expr{
			pkExpr,
			rightArg,
		})
	}

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

	return newFilterList
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
