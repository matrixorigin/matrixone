// Copyright 2024 Matrix Origin
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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

func (builder *QueryBuilder) detectFilterOnCompositePrimaryKey(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType != plan.Node_TABLE_SCAN {
		for _, childID := range node.Children {
			builder.detectFilterOnCompositePrimaryKey(childID)
		}

		return
	}

	if node.TableDef.Pkey == nil || len(node.TableDef.Pkey.Names) == 1 {
		return
	}

	col2filter := make(map[int32]int)
	for i, expr := range node.FilterList {
		fn := expr.GetF()
		if fn == nil {
			continue
		}

		if fn.Func.ObjName != "=" {
			continue
		}

		if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
			fn.Args[0], fn.Args[1] = fn.Args[1], fn.Args[0]
		}

		col := fn.Args[0].GetCol()
		if col == nil || !isRuntimeConstExpr(fn.Args[1]) {
			continue
		}

		col2filter[col.ColPos] = i
	}

	numParts := len(node.TableDef.Pkey.Names)
	filterIdx := make([]int, 0, numParts)
	for _, part := range node.TableDef.Pkey.Names {
		colIdx := node.TableDef.Name2ColIndex[part]
		idx, ok := col2filter[colIdx]
		if !ok {
			break
		}

		filterIdx = append(filterIdx, idx)
	}

	if len(filterIdx) == 0 {
		return
	}

	serialArgs := make([]*plan.Expr, len(filterIdx))
	for i := range filterIdx {
		serialArgs[i] = node.FilterList[filterIdx[i]].GetF().Args[1]
	}
	rightArg, _ := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "serial", serialArgs)

	pkIdx := node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
	pkExpr := &plan.Expr{
		Typ: DeepCopyType(node.TableDef.Cols[pkIdx].Typ),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: node.BindingTags[0],
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

	newFilterList := make([]*plan.Expr, 0, len(node.FilterList)-len(filterIdx)+1)
	newFilterList = append(newFilterList, compositePKFilter)
	for i, filter := range node.FilterList {
		if !hitFilterSet[i] {
			newFilterList = append(newFilterList, filter)
		}
	}

	node.FilterList = newFilterList
	calcScanStats(node, builder)
}
