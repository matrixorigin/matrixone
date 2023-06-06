// Copyright 2022 Matrix Origin
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
	"encoding/json"
	"fmt"
	"go/constant"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func NewQueryBuilder(queryType plan.Query_StatementType, ctx CompilerContext) *QueryBuilder {
	var mysqlCompatible bool

	mode, err := ctx.ResolveVariable("sql_mode", true, false)
	if err == nil {
		if modeStr, ok := mode.(string); ok {
			if !strings.Contains(modeStr, "ONLY_FULL_GROUP_BY") {
				mysqlCompatible = true
			}
		}
	}

	return &QueryBuilder{
		qry: &Query{
			StmtType: queryType,
		},
		compCtx:         ctx,
		ctxByNode:       []*BindContext{},
		nameByColRef:    make(map[[2]int32]string),
		nextTag:         0,
		mysqlCompatible: mysqlCompatible,
	}
}

func (builder *QueryBuilder) remapColRefForExpr(expr *Expr, colMap map[[2]int32][2]int32) error {
	switch ne := expr.Expr.(type) {
	case *plan.Expr_Col:
		mapID := [2]int32{ne.Col.RelPos, ne.Col.ColPos}
		if ids, ok := colMap[mapID]; ok {
			ne.Col.RelPos = ids[0]
			ne.Col.ColPos = ids[1]
			ne.Col.Name = builder.nameByColRef[mapID]
		} else {
			return moerr.NewParseError(builder.GetContext(), "can't find column %v in context's map %v", mapID, colMap)
		}

	case *plan.Expr_F:
		for _, arg := range ne.F.GetArgs() {
			err := builder.remapColRefForExpr(arg, colMap)
			if err != nil {
				return err
			}
		}
	case *plan.Expr_W:
		err := builder.remapColRefForExpr(ne.W.WindowFunc, colMap)
		if err != nil {
			return err
		}
		for _, arg := range ne.W.PartitionBy {
			err = builder.remapColRefForExpr(arg, colMap)
			if err != nil {
				return err
			}
		}
		for _, order := range ne.W.OrderBy {
			err = builder.remapColRefForExpr(order.Expr, colMap)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type ColRefRemapping struct {
	globalToLocal map[[2]int32][2]int32
	localToGlobal [][2]int32
}

func (m *ColRefRemapping) addColRef(colRef [2]int32) {
	m.globalToLocal[colRef] = [2]int32{0, int32(len(m.localToGlobal))}
	m.localToGlobal = append(m.localToGlobal, colRef)
}

func (builder *QueryBuilder) copyNode(ctx *BindContext, nodeId int32) int32 {
	node := builder.qry.Nodes[nodeId]
	newNode := DeepCopyNode(node)
	newNode.Children = make([]int32, 0, len(node.Children))
	for _, child := range node.Children {
		newNode.Children = append(newNode.Children, builder.copyNode(ctx, child))
	}
	newNodeId := builder.appendNode(newNode, ctx)
	return newNodeId
}

func (builder *QueryBuilder) remapAllColRefs(nodeID int32, colRefCnt map[[2]int32]int) (*ColRefRemapping, error) {
	node := builder.qry.Nodes[nodeID]

	remapping := &ColRefRemapping{
		globalToLocal: make(map[[2]int32][2]int32),
	}

	switch node.NodeType {
	case plan.Node_FUNCTION_SCAN:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, colRefCnt)
		}

		internalRemapping := &ColRefRemapping{
			globalToLocal: make(map[[2]int32][2]int32),
		}

		tag := node.BindingTags[0]
		newTableDef := &plan.TableDef{
			Name:          node.TableDef.Name,
			Defs:          node.TableDef.Defs,
			Name2ColIndex: node.TableDef.Name2ColIndex,
			Createsql:     node.TableDef.Createsql,
			TblFunc:       node.TableDef.TblFunc,
			TableType:     node.TableDef.TableType,
			Partition:     node.TableDef.Partition,
		}

		for i, col := range node.TableDef.Cols {
			globalRef := [2]int32{tag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			internalRemapping.addColRef(globalRef)

			newTableDef.Cols = append(newTableDef.Cols, col)
		}

		if len(newTableDef.Cols) == 0 {
			internalRemapping.addColRef([2]int32{tag, 0})
			newTableDef.Cols = append(newTableDef.Cols, node.TableDef.Cols[0])
		}

		node.TableDef = newTableDef

		for _, expr := range node.FilterList {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapColRefForExpr(expr, internalRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		for i, col := range node.TableDef.Cols {
			if colRefCnt[internalRemapping.localToGlobal[i]] == 0 {
				continue
			}

			remapping.addColRef(internalRemapping.localToGlobal[i])

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[internalRemapping.localToGlobal[i]],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(node.TableDef.Cols) == 0 {
				globalRef := [2]int32{tag, 0}
				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.TableDef.Cols[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: 0,
							Name:   builder.nameByColRef[globalRef],
						},
					},
				})
			} else {
				remapping.addColRef(internalRemapping.localToGlobal[0])
				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.TableDef.Cols[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: 0,
							Name:   builder.nameByColRef[internalRemapping.localToGlobal[0]],
						},
					},
				})
			}
		}
		childId := node.Children[0]
		childNode := builder.qry.Nodes[childId]

		if childNode.NodeType == plan.Node_VALUE_SCAN {
			break
		}
		for _, expr := range node.TblFuncExprList {
			increaseRefCnt(expr, colRefCnt)
		}
		childMap, err := builder.remapAllColRefs(childId, colRefCnt)

		if err != nil {
			return nil, err
		}

		for _, expr := range node.TblFuncExprList {
			decreaseRefCnt(expr, colRefCnt)
			err = builder.remapColRefForExpr(expr, childMap.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

	case plan.Node_TABLE_SCAN, plan.Node_MATERIAL_SCAN, plan.Node_EXTERNAL_SCAN:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, colRefCnt)
		}

		for _, expr := range node.BlockFilterList {
			increaseRefCnt(expr, colRefCnt)
		}

		internalRemapping := &ColRefRemapping{
			globalToLocal: make(map[[2]int32][2]int32),
		}

		tag := node.BindingTags[0]
		newTableDef := &plan.TableDef{
			TblId:         node.TableDef.TblId,
			Name:          node.TableDef.Name,
			Defs:          node.TableDef.Defs,
			Name2ColIndex: node.TableDef.Name2ColIndex,
			Createsql:     node.TableDef.Createsql,
			TblFunc:       node.TableDef.TblFunc,
			TableType:     node.TableDef.TableType,
			Partition:     node.TableDef.Partition,
			IsLocked:      node.TableDef.IsLocked,
			IsTemporary:   node.TableDef.IsTemporary,
			TableLockType: node.TableDef.TableLockType,
		}

		for i, col := range node.TableDef.Cols {
			globalRef := [2]int32{tag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			internalRemapping.addColRef(globalRef)

			newTableDef.Cols = append(newTableDef.Cols, col)
		}

		if len(newTableDef.Cols) == 0 {
			internalRemapping.addColRef([2]int32{tag, 0})
			newTableDef.Cols = append(newTableDef.Cols, node.TableDef.Cols[0])
		}

		node.TableDef = newTableDef

		for _, expr := range node.FilterList {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapColRefForExpr(expr, internalRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		for _, expr := range node.BlockFilterList {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapColRefForExpr(expr, internalRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		//for _, rfSpec := range node.RuntimeFilterList {
		//	err := builder.remapColRefForExpr(rfSpec.Expr, internalRemapping.globalToLocal)
		//	if err != nil {
		//		return nil, err
		//	}
		//}

		for i, col := range node.TableDef.Cols {
			if colRefCnt[internalRemapping.localToGlobal[i]] == 0 {
				continue
			}

			remapping.addColRef(internalRemapping.localToGlobal[i])

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[internalRemapping.localToGlobal[i]],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(node.TableDef.Cols) == 0 {
				globalRef := [2]int32{tag, 0}
				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.TableDef.Cols[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: 0,
							Name:   builder.nameByColRef[globalRef],
						},
					},
				})
			} else {
				remapping.addColRef(internalRemapping.localToGlobal[0])
				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.TableDef.Cols[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: 0,
							Name:   builder.nameByColRef[internalRemapping.localToGlobal[0]],
						},
					},
				})
			}
		}

	case plan.Node_INTERSECT, plan.Node_INTERSECT_ALL,
		plan.Node_UNION, plan.Node_UNION_ALL,
		plan.Node_MINUS, plan.Node_MINUS_ALL:

		thisTag := node.BindingTags[0]
		leftID := node.Children[0]
		rightID := node.Children[1]
		for i, expr := range node.ProjectList {
			increaseRefCnt(expr, colRefCnt)
			globalRef := [2]int32{thisTag, int32(i)}
			remapping.addColRef(globalRef)
		}

		rightNode := builder.qry.Nodes[rightID]
		if rightNode.NodeType == plan.Node_PROJECT {
			projectTag := rightNode.BindingTags[0]
			for i := range rightNode.ProjectList {
				increaseRefCnt(&plan.Expr{
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: projectTag,
							ColPos: int32(i),
						},
					}}, colRefCnt)
			}
		}

		internalMap := make(map[[2]int32][2]int32)

		leftRemapping, err := builder.remapAllColRefs(leftID, colRefCnt)
		if err != nil {
			return nil, err
		}
		for k, v := range leftRemapping.globalToLocal {
			internalMap[k] = v
		}

		_, err = builder.remapAllColRefs(rightID, colRefCnt)
		if err != nil {
			return nil, err
		}

		for _, expr := range node.ProjectList {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapColRefForExpr(expr, internalMap)
			if err != nil {
				return nil, err
			}
		}

	case plan.Node_JOIN:
		for _, expr := range node.OnList {
			increaseRefCnt(expr, colRefCnt)
		}

		internalMap := make(map[[2]int32][2]int32)

		leftID := node.Children[0]
		leftRemapping, err := builder.remapAllColRefs(leftID, colRefCnt)
		if err != nil {
			return nil, err
		}

		for k, v := range leftRemapping.globalToLocal {
			internalMap[k] = v
		}

		rightID := node.Children[1]
		rightRemapping, err := builder.remapAllColRefs(rightID, colRefCnt)
		if err != nil {
			return nil, err
		}

		for k, v := range rightRemapping.globalToLocal {
			internalMap[k] = [2]int32{1, v[1]}
		}

		for _, expr := range node.OnList {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapColRefForExpr(expr, internalMap)
			if err != nil {
				return nil, err
			}
		}

		//for _, rfSpec := range node.RuntimeFilterBuildList {
		//	err := builder.remapColRefForExpr(rfSpec.Expr, internalMap)
		//	if err != nil {
		//		return nil, err
		//	}
		//}

		childProjList := builder.qry.Nodes[leftID].ProjectList
		for i, globalRef := range leftRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		if node.JoinType == plan.Node_MARK {
			globalRef := [2]int32{node.BindingTags[0], 0}
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: &plan.Type{
					Id:          int32(types.T_bool),
					NotNullable: false,
				},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: 0,
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		} else {
			childProjList = builder.qry.Nodes[rightID].ProjectList
			for i, globalRef := range rightRemapping.localToGlobal {
				if colRefCnt[globalRef] == 0 {
					continue
				}

				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: childProjList[i].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 1,
							ColPos: int32(i),
							Name:   builder.nameByColRef[globalRef],
						},
					},
				})
			}
		}

		if len(node.ProjectList) == 0 && len(leftRemapping.localToGlobal) > 0 {
			globalRef := leftRemapping.localToGlobal[0]
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: builder.qry.Nodes[leftID].ProjectList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

	case plan.Node_AGG:
		for _, expr := range node.GroupBy {
			increaseRefCnt(expr, colRefCnt)
		}

		for _, expr := range node.AggList {
			increaseRefCnt(expr, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		groupTag := node.BindingTags[0]
		aggregateTag := node.BindingTags[1]
		groupSize := int32(len(node.GroupBy))

		for _, expr := range node.FilterList {
			builder.remapHavingClause(expr, groupTag, aggregateTag, groupSize)
		}

		for idx, expr := range node.GroupBy {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapColRefForExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{groupTag, int32(idx)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: int32(idx),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		for idx, expr := range node.AggList {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapColRefForExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{aggregateTag, int32(idx)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -2,
						ColPos: int32(idx) + groupSize,
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if groupSize > 0 {
				globalRef := [2]int32{groupTag, 0}
				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.GroupBy[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: -1,
							ColPos: 0,
							Name:   builder.nameByColRef[globalRef],
						},
					},
				})
			} else {
				globalRef := [2]int32{aggregateTag, 0}
				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.AggList[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: -2,
							ColPos: 0,
							Name:   builder.nameByColRef[globalRef],
						},
					},
				})
			}
		}

	case plan.Node_WINDOW:
		for _, expr := range node.WinSpecList {
			increaseRefCnt(expr, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		childProjList := builder.qry.Nodes[node.Children[0]].ProjectList
		i := 0
		for _, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
			i++
		}

		windowTag := node.BindingTags[0]

		for idx, expr := range node.WinSpecList {
			decreaseRefCnt(expr, colRefCnt)
			err = builder.remapColRefForExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{windowTag, int32(idx)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: int32(idx + i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

	case plan.Node_SORT:
		for _, orderBy := range node.OrderBy {
			increaseRefCnt(orderBy.Expr, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		for _, orderBy := range node.OrderBy {
			decreaseRefCnt(orderBy.Expr, colRefCnt)
			err := builder.remapColRefForExpr(orderBy.Expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		childProjList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 && len(childRemapping.localToGlobal) > 0 {
			globalRef := childRemapping.localToGlobal[0]
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

	case plan.Node_FILTER:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		for _, expr := range node.FilterList {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapColRefForExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		childProjList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(childRemapping.localToGlobal) > 0 {
				remapping.addColRef(childRemapping.localToGlobal[0])
			}

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			})
		}

	case plan.Node_PROJECT, plan.Node_MATERIAL:
		projectTag := node.BindingTags[0]

		var neededProj []int32

		for i, expr := range node.ProjectList {
			globalRef := [2]int32{projectTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			neededProj = append(neededProj, int32(i))
			increaseRefCnt(expr, colRefCnt)
		}

		if len(neededProj) == 0 {
			increaseRefCnt(node.ProjectList[0], colRefCnt)
			neededProj = append(neededProj, 0)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		var newProjList []*plan.Expr
		for _, needed := range neededProj {
			expr := node.ProjectList[needed]
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapColRefForExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{projectTag, needed}
			remapping.addColRef(globalRef)

			newProjList = append(newProjList, expr)
		}

		node.ProjectList = newProjList

	case plan.Node_DISTINCT:
		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		// Rewrite DISTINCT to AGG
		node.NodeType = plan.Node_AGG
		preNode := builder.qry.Nodes[node.Children[0]]
		node.GroupBy = make([]*Expr, len(preNode.ProjectList))
		node.ProjectList = make([]*Expr, len(preNode.ProjectList))

		for i, prjExpr := range preNode.ProjectList {
			node.GroupBy[i] = &plan.Expr{
				Typ: prjExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
					},
				},
			}

			node.ProjectList[i] = &plan.Expr{
				Typ: prjExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: int32(i),
					},
				},
			}
		}

		remapping = childRemapping

	case plan.Node_VALUE_SCAN:
		// VALUE_SCAN always have one column now
		if node.TableDef == nil { // like select 1,2
			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ:  &plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_C{C: &plan.Const{Value: &plan.Const_I64Val{I64Val: 0}}},
			})
		} else {
			internalRemapping := &ColRefRemapping{
				globalToLocal: make(map[[2]int32][2]int32),
			}

			tag := node.BindingTags[0]
			for i := range node.TableDef.Cols {
				globalRef := [2]int32{tag, int32(i)}
				if colRefCnt[globalRef] == 0 {
					continue
				}
				internalRemapping.addColRef(globalRef)
			}

			for i, col := range node.TableDef.Cols {
				if colRefCnt[internalRemapping.localToGlobal[i]] == 0 {
					continue
				}

				remapping.addColRef(internalRemapping.localToGlobal[i])

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: col.Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: int32(i),
							Name:   col.Name,
						},
					},
				})
			}
		}

	default:
		return nil, moerr.NewInternalError(builder.GetContext(), "unsupport node type")
	}

	node.BindingTags = nil

	return remapping, nil
}

func (builder *QueryBuilder) createQuery() (*Query, error) {
	for i, rootID := range builder.qry.Steps {
		rootID, _ = builder.pushdownFilters(rootID, nil, false)
		colRefCnt := make(map[[2]int32]int)
		builder.removeSimpleProjections(rootID, plan.Node_UNKNOWN, false, colRefCnt)
		ReCalcNodeStats(rootID, builder, true, true)
		rootID = builder.aggPushDown(rootID)
		ReCalcNodeStats(rootID, builder, true, false)
		rootID = builder.determineJoinOrder(rootID)
		ReCalcNodeStats(rootID, builder, true, false)
		rootID = builder.aggPullup(rootID, rootID)
		ReCalcNodeStats(rootID, builder, true, false)
		rootID = builder.pushdownSemiAntiJoins(rootID)
		builder.optimizeDistinctAgg(rootID)
		ReCalcNodeStats(rootID, builder, true, false)
		builder.applySwapRuleByStats(rootID, true)
		rewriteFilterListByStats(builder.GetContext(), rootID, builder)
		determinShuffleMethod(rootID, builder)
		builder.qry.Steps[i] = rootID

		// XXX: This will be removed soon, after merging implementation of all join operators
		builder.swapJoinChildren(rootID)

		//builder.generateRuntimeFilters(rootID)

		colRefCnt = make(map[[2]int32]int)
		rootNode := builder.qry.Nodes[rootID]
		resultTag := rootNode.BindingTags[0]
		for i := range rootNode.ProjectList {
			colRefCnt[[2]int32{resultTag, int32(i)}] = 1
		}

		_, err := builder.remapAllColRefs(rootID, colRefCnt)
		if err != nil {
			return nil, err
		}
	}
	return builder.qry, nil
}

func (builder *QueryBuilder) buildUnion(stmt *tree.UnionClause, astOrderBy tree.OrderBy, astLimit *tree.Limit, ctx *BindContext, isRoot bool) (int32, error) {
	var selectStmts []tree.Statement
	var unionTypes []plan.Node_NodeType

	// get Union selectStmts
	err := getUnionSelects(builder.GetContext(), stmt, &selectStmts, &unionTypes)
	if err != nil {
		return 0, err
	}

	if len(selectStmts) == 1 {
		switch sltStmt := selectStmts[0].(type) {
		case *tree.Select:
			if sltClause, ok := sltStmt.Select.(*tree.SelectClause); ok {
				sltClause.Distinct = true
				return builder.buildSelect(sltStmt, ctx, isRoot)
			} else {
				// rewrite sltStmt to select distinct * from (sltStmt) a
				tmpSltStmt := &tree.Select{
					Select: &tree.SelectClause{
						Distinct: true,

						Exprs: []tree.SelectExpr{
							{Expr: tree.StarExpr()},
						},
						From: &tree.From{
							Tables: tree.TableExprs{
								&tree.AliasedTableExpr{
									Expr: &tree.ParenTableExpr{
										Expr: sltStmt,
									},
									As: tree.AliasClause{
										Alias: "a",
									},
								},
							},
						},
					},
					Limit:   astLimit,
					OrderBy: astOrderBy,
				}
				return builder.buildSelect(tmpSltStmt, ctx, isRoot)
			}

		case *tree.SelectClause:
			if !sltStmt.Distinct {
				sltStmt.Distinct = true
			}
			return builder.buildSelect(&tree.Select{Select: sltStmt, Limit: astLimit, OrderBy: astOrderBy}, ctx, isRoot)
		}
	}

	// build selects
	var projectTypList [][]types.Type
	selectStmtLength := len(selectStmts)
	nodes := make([]int32, selectStmtLength)
	subCtxList := make([]*BindContext, selectStmtLength)
	var projectLength int
	var nodeID int32
	for idx, sltStmt := range selectStmts {
		subCtx := NewBindContext(builder, ctx)
		if slt, ok := sltStmt.(*tree.Select); ok {
			nodeID, err = builder.buildSelect(slt, subCtx, isRoot)
		} else {
			nodeID, err = builder.buildSelect(&tree.Select{Select: sltStmt}, subCtx, isRoot)
		}
		if err != nil {
			return 0, err
		}

		if idx == 0 {
			projectLength = len(builder.qry.Nodes[nodeID].ProjectList)
			projectTypList = make([][]types.Type, projectLength)
			for i := 0; i < projectLength; i++ {
				projectTypList[i] = make([]types.Type, selectStmtLength)
			}
		} else {
			if projectLength != len(builder.qry.Nodes[nodeID].ProjectList) {
				return 0, moerr.NewParseError(builder.GetContext(), "SELECT statements have different number of columns")
			}
		}

		for i, expr := range subCtx.results {
			projectTypList[i][idx] = makeTypeByPlan2Expr(expr)
		}
		subCtxList[idx] = subCtx
		nodes[idx] = nodeID
	}

	// reset all select's return Projection(type cast up)
	// we use coalesce function's type check&type cast rule
	for columnIdx, argsType := range projectTypList {
		// we don't cast null as any type in function
		// but we will cast null as some target type in union/intersect/minus
		var tmpArgsType []types.Type
		for _, typ := range argsType {
			if typ.Oid != types.T_any {
				tmpArgsType = append(tmpArgsType, typ)
			}
		}

		if len(tmpArgsType) > 0 {
			fGet, err := function.GetFunctionByName(builder.GetContext(), "coalesce", tmpArgsType)
			if err != nil {
				return 0, moerr.NewParseError(builder.GetContext(), "the %d column cann't cast to a same type", columnIdx)
			}
			argsCastType, _ := fGet.ShouldDoImplicitTypeCast()

			if len(argsCastType) > 0 && int(argsCastType[0].Oid) == int(types.T_datetime) {
				for i := 0; i < len(argsCastType); i++ {
					argsCastType[i].Scale = 0
				}
			}
			var targetType *plan.Type
			var targetArgType types.Type
			if len(argsCastType) == 0 {
				targetArgType = tmpArgsType[0]
			} else {
				targetArgType = argsCastType[0]
			}
			// if string union string, different length may cause error. use text type as the output
			if targetArgType.Oid == types.T_varchar || targetArgType.Oid == types.T_char {
				targetArgType = types.T_text.ToType()
			}

			if targetArgType.Oid == types.T_binary || targetArgType.Oid == types.T_varbinary {
				targetArgType = types.T_blob.ToType()
			}
			targetType = makePlan2Type(&targetArgType)

			for idx, tmpID := range nodes {
				if !argsType[idx].Eq(targetArgType) {
					node := builder.qry.Nodes[tmpID]
					if argsType[idx].Oid == types.T_any {
						node.ProjectList[columnIdx].Typ = targetType
					} else {
						node.ProjectList[columnIdx], err = appendCastBeforeExpr(builder.GetContext(), node.ProjectList[columnIdx], targetType)
						if err != nil {
							return 0, err
						}
					}
				}
			}
		}
	}

	firstSelectProjectNode := builder.qry.Nodes[nodes[0]]
	// set ctx's headings  projects  results
	ctx.headings = append(ctx.headings, subCtxList[0].headings...)

	getProjectList := func(tag int32, thisTag int32) []*plan.Expr {
		projectList := make([]*plan.Expr, len(firstSelectProjectNode.ProjectList))
		for i, expr := range firstSelectProjectNode.ProjectList {
			projectList[i] = &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tag,
						ColPos: int32(i),
					},
				},
			}
			builder.nameByColRef[[2]int32{thisTag, int32(i)}] = ctx.headings[i]
		}
		return projectList
	}

	// build intersect node first.  because intersect has higher precedence then UNION and MINUS
	var newNodes []int32
	var newUnionType []plan.Node_NodeType
	var lastTag int32
	newNodes = append(newNodes, nodes[0])
	for i := 1; i < len(nodes); i++ {
		utIdx := i - 1
		lastNewNodeIdx := len(newNodes) - 1
		if unionTypes[utIdx] == plan.Node_INTERSECT || unionTypes[utIdx] == plan.Node_INTERSECT_ALL {
			lastTag = builder.genNewTag()
			leftNodeTag := builder.qry.Nodes[newNodes[lastNewNodeIdx]].BindingTags[0]
			newNodeID := builder.appendNode(&plan.Node{
				NodeType:    unionTypes[utIdx],
				Children:    []int32{newNodes[lastNewNodeIdx], nodes[i]},
				BindingTags: []int32{lastTag},
				ProjectList: getProjectList(leftNodeTag, lastTag),
			}, ctx)
			newNodes[lastNewNodeIdx] = newNodeID
		} else {
			newNodes = append(newNodes, nodes[i])
			newUnionType = append(newUnionType, unionTypes[utIdx])
		}
	}

	// build UNION/MINUS node one by one
	lastNodeID := newNodes[0]
	for i := 1; i < len(newNodes); i++ {
		utIdx := i - 1
		lastTag = builder.genNewTag()
		leftNodeTag := builder.qry.Nodes[lastNodeID].BindingTags[0]

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    newUnionType[utIdx],
			Children:    []int32{lastNodeID, newNodes[i]},
			BindingTags: []int32{lastTag},
			ProjectList: getProjectList(leftNodeTag, lastTag),
		}, ctx)
	}

	// set ctx base on selects[0] and it's ctx
	ctx.groupTag = builder.genNewTag()
	ctx.aggregateTag = builder.genNewTag()
	ctx.projectTag = builder.genNewTag()
	for i, v := range ctx.headings {
		ctx.aliasMap[v] = int32(i)
		builder.nameByColRef[[2]int32{ctx.projectTag, int32(i)}] = v
	}
	for i, expr := range firstSelectProjectNode.ProjectList {
		ctx.projects = append(ctx.projects, &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: lastTag,
					ColPos: int32(i),
				},
			},
		})
	}
	havingBinder := NewHavingBinder(builder, ctx)
	projectionBinder := NewProjectionBinder(builder, ctx, havingBinder)

	// append a project node
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: ctx.projects,
		Children:    []int32{lastNodeID},
		BindingTags: []int32{ctx.projectTag},
	}, ctx)

	// append orderBy
	if astOrderBy != nil {
		orderBinder := NewOrderBinder(projectionBinder, nil)
		orderBys := make([]*plan.OrderBySpec, 0, len(astOrderBy))

		for _, order := range astOrderBy {
			expr, err := orderBinder.BindExpr(order.Expr)
			if err != nil {
				return 0, err
			}

			orderBy := &plan.OrderBySpec{
				Expr: expr,
				Flag: plan.OrderBySpec_INTERNAL,
			}

			switch order.Direction {
			case tree.Ascending:
				orderBy.Flag |= plan.OrderBySpec_ASC
			case tree.Descending:
				orderBy.Flag |= plan.OrderBySpec_DESC
			}

			switch order.NullsPosition {
			case tree.NullsFirst:
				orderBy.Flag |= plan.OrderBySpec_NULLS_FIRST
			case tree.NullsLast:
				orderBy.Flag |= plan.OrderBySpec_NULLS_LAST
			}

			orderBys = append(orderBys, orderBy)
		}

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{lastNodeID},
			OrderBy:  orderBys,
		}, ctx)
	}

	// append limit
	if astLimit != nil {
		node := builder.qry.Nodes[lastNodeID]

		limitBinder := NewLimitBinder(builder, ctx)
		if astLimit.Offset != nil {
			node.Offset, err = limitBinder.BindExpr(astLimit.Offset, 0, true)
			if err != nil {
				return 0, err
			}
		}
		if astLimit.Count != nil {
			node.Limit, err = limitBinder.BindExpr(astLimit.Count, 0, true)
			if err != nil {
				return 0, err
			}

			if cExpr, ok := node.Limit.Expr.(*plan.Expr_C); ok {
				if c, ok := cExpr.C.Value.(*plan.Const_I64Val); ok {
					ctx.hasSingleRow = c.I64Val == 1
				}
			}
		}
	}

	// append result PROJECT node
	if builder.qry.Nodes[lastNodeID].NodeType != plan.Node_PROJECT {
		for i := 0; i < len(ctx.projects); i++ {
			ctx.results = append(ctx.results, &plan.Expr{
				Typ: ctx.projects[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: ctx.projectTag,
						ColPos: int32(i),
					},
				},
			})
		}
		ctx.resultTag = builder.genNewTag()

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: ctx.results,
			Children:    []int32{lastNodeID},
			BindingTags: []int32{ctx.resultTag},
		}, ctx)
	} else {
		ctx.results = ctx.projects
	}

	// set heading
	if isRoot {
		builder.qry.Headings = append(builder.qry.Headings, ctx.headings...)
	}

	return lastNodeID, nil
}

func (builder *QueryBuilder) buildSelect(stmt *tree.Select, ctx *BindContext, isRoot bool) (int32, error) {
	// preprocess CTEs
	if stmt.With != nil {
		ctx.cteByName = make(map[string]*CTERef)
		maskedNames := make([]string, len(stmt.With.CTEs))

		for i := range stmt.With.CTEs {
			idx := len(stmt.With.CTEs) - i - 1
			cte := stmt.With.CTEs[idx]

			name := string(cte.Name.Alias)
			if _, ok := ctx.cteByName[name]; ok {
				return 0, moerr.NewSyntaxError(builder.GetContext(), "WITH query name %q specified more than once", name)
			}

			var maskedCTEs map[string]any
			if len(maskedNames) > 0 {
				maskedCTEs = make(map[string]any)
				for _, mask := range maskedNames {
					maskedCTEs[mask] = nil
				}
			}

			maskedNames = append(maskedNames, name)

			ctx.cteByName[name] = &CTERef{
				ast:        cte,
				maskedCTEs: maskedCTEs,
			}
		}

		// Try to do binding for CTE at declaration
		for _, cte := range stmt.With.CTEs {
			subCtx := NewBindContext(builder, ctx)
			subCtx.maskedCTEs = ctx.cteByName[string(cte.Name.Alias)].maskedCTEs

			var err error
			switch stmt := cte.Stmt.(type) {
			case *tree.Select:
				_, err = builder.buildSelect(stmt, subCtx, false)

			case *tree.ParenSelect:
				_, err = builder.buildSelect(stmt.Select, subCtx, false)

			default:
				err = moerr.NewParseError(builder.GetContext(), "unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL))
			}

			if err != nil {
				return 0, err
			}
		}
	}

	var clause *tree.SelectClause
	var valuesClause *tree.ValuesClause
	var nodeID int32
	var err error
	astOrderBy := stmt.OrderBy
	astLimit := stmt.Limit

	// strip parentheses
	// ((select a from t1)) order by b  [ is equal ] select a from t1 order by b
	// (((select a from t1)) order by b) [ is equal ] select a from t1 order by b
	//
	// (select a from t1 union/union all select aa from t2) order by a
	//       => we will strip parentheses, but order by only can use 'a' column from the union's output projectlist
	for {
		if selectClause, ok := stmt.Select.(*tree.ParenSelect); ok {
			if selectClause.Select.OrderBy != nil {
				if astOrderBy != nil {
					return 0, moerr.NewSyntaxError(builder.GetContext(), "multiple ORDER BY clauses not allowed")
				}
				astOrderBy = selectClause.Select.OrderBy
			}
			if selectClause.Select.Limit != nil {
				if astLimit != nil {
					return 0, moerr.NewSyntaxError(builder.GetContext(), "multiple LIMIT clauses not allowed")
				}
				astLimit = selectClause.Select.Limit
			}
			stmt = selectClause.Select
		} else {
			break
		}
	}

	switch selectClause := stmt.Select.(type) {
	case *tree.SelectClause:
		clause = selectClause
	case *tree.UnionClause:
		return builder.buildUnion(selectClause, astOrderBy, astLimit, ctx, isRoot)
	case *tree.ValuesClause:
		valuesClause = selectClause
	default:
		return 0, moerr.NewNYI(builder.GetContext(), "statement '%s'", tree.String(stmt, dialect.MYSQL))
	}

	var projectionBinder *ProjectionBinder
	var havingList []*plan.Expr
	var selectList tree.SelectExprs
	var resultLen int
	var havingBinder *HavingBinder

	if clause == nil {
		rowCount := len(valuesClause.Rows)
		if len(valuesClause.Rows) == 0 {
			return 0, moerr.NewInternalError(builder.GetContext(), "values statement have not rows")
		}
		colCount := len(valuesClause.Rows[0])
		for j := 1; j < rowCount; j++ {
			if len(valuesClause.Rows[j]) != colCount {
				return 0, moerr.NewInternalError(builder.GetContext(), fmt.Sprintf("have different column count in row '%v'", j))
			}
		}

		ctx.hasSingleRow = rowCount == 1
		rowSetData := &plan.RowsetData{
			Cols: make([]*plan.ColData, colCount),
		}
		tableDef := &plan.TableDef{
			TblId: 0,
			Name:  "",
			Cols:  make([]*plan.ColDef, colCount),
		}
		ctx.binder = NewWhereBinder(builder, ctx)
		for i := 0; i < colCount; i++ {
			rows := make([]*plan.Expr, rowCount)
			var colTyp *plan.Type
			var tmpArgsType []types.Type
			for j := 0; j < rowCount; j++ {
				planExpr, err := ctx.binder.BindExpr(valuesClause.Rows[j][i], 0, true)
				if err != nil {
					return 0, err
				}
				if planExpr.Typ.Id != int32(types.T_any) {
					tmpArgsType = append(tmpArgsType, makeTypeByPlan2Expr(planExpr))
				}
				rows[j] = planExpr
			}

			if len(tmpArgsType) > 0 {
				fGet, err := function.GetFunctionByName(builder.GetContext(), "coalesce", tmpArgsType)
				if err != nil {
					return 0, err
				}
				argsCastType, _ := fGet.ShouldDoImplicitTypeCast()
				if len(argsCastType) > 0 {
					colTyp = makePlan2Type(&argsCastType[0])
					for j := 0; j < rowCount; j++ {
						if rows[j].Typ.Id != int32(types.T_any) && rows[j].Typ.Id != colTyp.Id {
							rows[j], err = appendCastBeforeExpr(builder.GetContext(), rows[j], colTyp)
							if err != nil {
								return 0, err
							}
						}
					}
				}
			}
			if colTyp == nil {
				colTyp = rows[0].Typ
			}

			colName := fmt.Sprintf("column_%d", i) // like MySQL
			as := tree.NewCStr(colName, 0)
			selectList = append(selectList, tree.SelectExpr{
				Expr: &tree.UnresolvedName{
					NumParts: 1,
					Star:     false,
					Parts:    [4]string{colName, "", "", ""},
				},
				As: as,
			})
			ctx.headings = append(ctx.headings, colName)
			tableDef.Cols[i] = &plan.ColDef{
				ColId: 0,
				Name:  colName,
				Typ:   colTyp,
			}
			rowSetData.Cols[i] = &plan.ColData{
				Data: rows,
			}
		}
		nodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_VALUE_SCAN,
			RowsetData:  rowSetData,
			TableDef:    tableDef,
			BindingTags: []int32{builder.genNewTag()},
		}, ctx)

		err = builder.addBinding(nodeID, tree.AliasClause{
			Alias: "_ValueScan",
		}, ctx)
		if err != nil {
			return 0, err
		}
	} else {
		// build FROM clause
		nodeID, err = builder.buildFrom(clause.From.Tables, ctx)
		if err != nil {
			return 0, err
		}

		ctx.binder = NewWhereBinder(builder, ctx)
		// unfold stars and generate headings
		for _, selectExpr := range clause.Exprs {
			switch expr := selectExpr.Expr.(type) {
			case tree.UnqualifiedStar:
				cols, names, err := ctx.unfoldStar(builder.GetContext(), "", builder.compCtx.GetAccountId() == catalog.System_Account)
				if err != nil {
					return 0, err
				}
				selectList = append(selectList, cols...)
				ctx.headings = append(ctx.headings, names...)

			case *tree.UnresolvedName:
				if expr.Star {
					cols, names, err := ctx.unfoldStar(builder.GetContext(), expr.Parts[0], builder.compCtx.GetAccountId() == catalog.System_Account)
					if err != nil {
						return 0, err
					}
					selectList = append(selectList, cols...)
					ctx.headings = append(ctx.headings, names...)
				} else {
					if selectExpr.As != nil && !selectExpr.As.Empty() {
						ctx.headings = append(ctx.headings, string(selectExpr.As.Origin()))
					} else {
						ctx.headings = append(ctx.headings, expr.Parts[0])
					}

					newExpr, err := ctx.qualifyColumnNames(expr, nil, false)
					if err != nil {
						return 0, err
					}

					selectList = append(selectList, tree.SelectExpr{
						Expr: newExpr,
						As:   selectExpr.As,
					})
				}
			case *tree.NumVal:
				if expr.ValType == tree.P_null {
					expr.ValType = tree.P_nulltext
				}

				if selectExpr.As != nil && !selectExpr.As.Empty() {
					ctx.headings = append(ctx.headings, string(selectExpr.As.Origin()))
				} else {
					ctx.headings = append(ctx.headings, tree.String(expr, dialect.MYSQL))
				}
				newExpr, err := ctx.qualifyColumnNames(expr, nil, false)
				if err != nil {
					return 0, err
				}
				selectList = append(selectList, tree.SelectExpr{
					Expr: newExpr,
					As:   selectExpr.As,
				})
			default:
				if selectExpr.As != nil && !selectExpr.As.Empty() {
					ctx.headings = append(ctx.headings, string(selectExpr.As.Origin()))
				} else {
					for {
						if parenExpr, ok := expr.(*tree.ParenExpr); ok {
							expr = parenExpr.Expr
						} else {
							break
						}
					}
					ctx.headings = append(ctx.headings, tree.String(expr, dialect.MYSQL))
				}

				newExpr, err := ctx.qualifyColumnNames(expr, nil, false)
				if err != nil {
					return 0, err
				}

				selectList = append(selectList, tree.SelectExpr{
					Expr: newExpr,
					As:   selectExpr.As,
				})
			}
		}

		if len(selectList) == 0 {
			return 0, moerr.NewParseError(builder.GetContext(), "No tables used")
		}

		// rewrite right join to left join
		builder.rewriteRightJoinToLeftJoin(nodeID)

		if clause.Where != nil {
			whereList, err := splitAndBindCondition(clause.Where.Expr, ctx)
			if err != nil {
				return 0, err
			}

			var newFilterList []*plan.Expr
			var expr *plan.Expr

			for _, cond := range whereList {
				nodeID, expr, err = builder.flattenSubqueries(nodeID, cond, ctx)
				if err != nil {
					return 0, err
				}

				newFilterList = append(newFilterList, expr)
			}

			nodeID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{nodeID},
				FilterList: newFilterList,
			}, ctx)
		}

		ctx.groupTag = builder.genNewTag()
		ctx.aggregateTag = builder.genNewTag()
		ctx.projectTag = builder.genNewTag()
		ctx.windowTag = builder.genNewTag()

		// bind GROUP BY clause
		if clause.GroupBy != nil {
			groupBinder := NewGroupBinder(builder, ctx)
			for _, group := range clause.GroupBy {
				group, err = ctx.qualifyColumnNames(group, nil, false)
				if err != nil {
					return 0, err
				}

				_, err = groupBinder.BindExpr(group, 0, true)
				if err != nil {
					return 0, err
				}
			}
		}

		// bind HAVING clause
		havingBinder = NewHavingBinder(builder, ctx)
		if clause.Having != nil {
			ctx.binder = havingBinder
			havingList, err = splitAndBindCondition(clause.Having.Expr, ctx)
			if err != nil {
				return 0, err
			}
		}

		ctx.isDistinct = clause.Distinct
	}

	// bind SELECT clause (Projection List)
	projectionBinder = NewProjectionBinder(builder, ctx, havingBinder)
	ctx.binder = projectionBinder
	for i, selectExpr := range selectList {
		astExpr, err := ctx.qualifyColumnNames(selectExpr.Expr, nil, false)
		if err != nil {
			return 0, err
		}

		expr, err := projectionBinder.BindExpr(astExpr, 0, true)
		if err != nil {
			return 0, err
		}

		builder.nameByColRef[[2]int32{ctx.projectTag, int32(i)}] = tree.String(astExpr, dialect.MYSQL)

		if selectExpr.As != nil && !selectExpr.As.Empty() {
			ctx.aliasMap[selectExpr.As.ToLower()] = int32(len(ctx.projects))
		}
		ctx.projects = append(ctx.projects, expr)
	}

	resultLen = len(ctx.projects)
	for i, proj := range ctx.projects {
		exprStr := proj.String()
		if _, ok := ctx.projectByExpr[exprStr]; !ok {
			ctx.projectByExpr[exprStr] = int32(i)
		}
	}

	// bind ORDER BY clause
	var orderBys []*plan.OrderBySpec
	if astOrderBy != nil {
		orderBinder := NewOrderBinder(projectionBinder, selectList)
		orderBys = make([]*plan.OrderBySpec, 0, len(astOrderBy))

		for _, order := range astOrderBy {
			expr, err := orderBinder.BindExpr(order.Expr)
			if err != nil {
				return 0, err
			}

			orderBy := &plan.OrderBySpec{
				Expr: expr,
				Flag: plan.OrderBySpec_INTERNAL,
			}

			switch order.Direction {
			case tree.Ascending:
				orderBy.Flag |= plan.OrderBySpec_ASC
			case tree.Descending:
				orderBy.Flag |= plan.OrderBySpec_DESC
			}

			switch order.NullsPosition {
			case tree.NullsFirst:
				orderBy.Flag |= plan.OrderBySpec_NULLS_FIRST
			case tree.NullsLast:
				orderBy.Flag |= plan.OrderBySpec_NULLS_LAST
			}

			orderBys = append(orderBys, orderBy)
		}
	}

	// bind limit/offset clause
	var limitExpr *Expr
	var offsetExpr *Expr
	if astLimit != nil {
		limitBinder := NewLimitBinder(builder, ctx)
		if astLimit.Offset != nil {
			offsetExpr, err = limitBinder.BindExpr(astLimit.Offset, 0, true)
			if err != nil {
				return 0, err
			}

			if cExpr, ok := offsetExpr.Expr.(*plan.Expr_C); ok {
				if c, ok := cExpr.C.Value.(*plan.Const_I64Val); ok {
					if c.I64Val < 0 {
						return 0, moerr.NewSyntaxError(builder.GetContext(), "offset value must be nonnegative")
					}
				}
			}
		}
		if astLimit.Count != nil {
			limitExpr, err = limitBinder.BindExpr(astLimit.Count, 0, true)
			if err != nil {
				return 0, err
			}

			if cExpr, ok := limitExpr.Expr.(*plan.Expr_C); ok {
				if c, ok := cExpr.C.Value.(*plan.Const_I64Val); ok {
					if c.I64Val < 0 {
						return 0, moerr.NewSyntaxError(builder.GetContext(), "limit value must be nonnegative")
					}
					ctx.hasSingleRow = c.I64Val == 1
				}
			}
		}
	}

	if (len(ctx.groups) > 0 || len(ctx.aggregates) > 0) && len(projectionBinder.boundCols) > 0 {
		if !builder.mysqlCompatible {
			return 0, moerr.NewSyntaxError(builder.GetContext(), "column %q must appear in the GROUP BY clause or be used in an aggregate function", projectionBinder.boundCols[0])
		}

		// For MySQL compatibility, wrap bare ColRefs in any_value()
		for i, proj := range ctx.projects {
			ctx.projects[i] = builder.wrapBareColRefsInAnyValue(proj, ctx)
		}
	}

	// FIXME: delete this when SINGLE join is ready
	if len(ctx.groups) == 0 && len(ctx.aggregates) > 0 {
		ctx.hasSingleRow = true
	}

	// append AGG node
	if len(ctx.groups) > 0 || len(ctx.aggregates) > 0 {
		nodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_AGG,
			Children:    []int32{nodeID},
			GroupBy:     ctx.groups,
			AggList:     ctx.aggregates,
			BindingTags: []int32{ctx.groupTag, ctx.aggregateTag},
		}, ctx)

		if len(havingList) > 0 {
			var newFilterList []*plan.Expr
			var expr *plan.Expr

			for _, cond := range havingList {
				nodeID, expr, err = builder.flattenSubqueries(nodeID, cond, ctx)
				if err != nil {
					return 0, err
				}

				newFilterList = append(newFilterList, expr)
			}

			nodeID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{nodeID},
				FilterList: newFilterList,
			}, ctx)
		}

		for name, id := range ctx.groupByAst {
			builder.nameByColRef[[2]int32{ctx.groupTag, id}] = name
		}

		for name, id := range ctx.aggregateByAst {
			builder.nameByColRef[[2]int32{ctx.aggregateTag, id}] = name
		}
	}

	// append WINDOW node
	if len(ctx.windows) > 0 {
		nodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_WINDOW,
			Children:    []int32{nodeID},
			WinSpecList: ctx.windows,
			BindingTags: []int32{ctx.windowTag},
		}, ctx)

		for name, id := range ctx.windowByAst {
			builder.nameByColRef[[2]int32{ctx.windowTag, id}] = name
		}
	}

	// append PROJECT node
	for i, proj := range ctx.projects {
		nodeID, proj, err = builder.flattenSubqueries(nodeID, proj, ctx)
		if err != nil {
			return 0, err
		}

		ctx.projects[i] = proj
	}

	nodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: ctx.projects,
		Children:    []int32{nodeID},
		BindingTags: []int32{ctx.projectTag},
	}, ctx)

	// append DISTINCT node
	if ctx.isDistinct {
		nodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_DISTINCT,
			Children: []int32{nodeID},
		}, ctx)
	}

	// append SORT node (include limit, offset)
	if len(orderBys) > 0 {
		nodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{nodeID},
			OrderBy:  orderBys,
		}, ctx)
	}

	if limitExpr != nil || offsetExpr != nil {
		node := builder.qry.Nodes[nodeID]

		node.Limit = limitExpr
		node.Offset = offsetExpr
	}

	// append result PROJECT node
	if builder.qry.Nodes[nodeID].NodeType != plan.Node_PROJECT {
		for i := 0; i < resultLen; i++ {
			ctx.results = append(ctx.results, &plan.Expr{
				Typ: ctx.projects[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: ctx.projectTag,
						ColPos: int32(i),
					},
				},
			})
		}

		ctx.resultTag = builder.genNewTag()

		nodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: ctx.results,
			Children:    []int32{nodeID},
			BindingTags: []int32{ctx.resultTag},
		}, ctx)
	} else {
		ctx.results = ctx.projects
	}

	if isRoot {
		builder.qry.Headings = append(builder.qry.Headings, ctx.headings...)
	}

	return nodeID, nil
}

func (builder *QueryBuilder) appendStep(nodeID int32) int32 {
	stepPos := len(builder.qry.Steps)
	builder.qry.Steps = append(builder.qry.Steps, nodeID)
	return int32(stepPos)
}

func (builder *QueryBuilder) appendNode(node *plan.Node, ctx *BindContext) int32 {
	nodeID := int32(len(builder.qry.Nodes))
	node.NodeId = nodeID
	builder.qry.Nodes = append(builder.qry.Nodes, node)
	builder.ctxByNode = append(builder.ctxByNode, ctx)
	ReCalcNodeStats(nodeID, builder, false, true)
	return nodeID
}

func (builder *QueryBuilder) rewriteRightJoinToLeftJoin(nodeID int32) {
	node := builder.qry.Nodes[nodeID]

	for i := range node.Children {
		builder.rewriteRightJoinToLeftJoin(node.Children[i])
	}

	if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_RIGHT {
		node.JoinType = plan.Node_LEFT
		node.Children[0], node.Children[1] = node.Children[1], node.Children[0]
	}
}

func (builder *QueryBuilder) buildFrom(stmt tree.TableExprs, ctx *BindContext) (int32, error) {
	if len(stmt) == 1 {
		return builder.buildTable(stmt[0], ctx, -1, nil)
	}
	return 0, moerr.NewInternalError(ctx.binder.GetContext(), "stmt's length should be zero")
	// for now, stmt'length always be zero. if someday that change in parser, you should uncomment these codes
	// leftCtx := NewBindContext(builder, ctx)
	// leftChildID, err := builder.buildTable(stmt[0], leftCtx)
	// if err != nil {
	// 	return 0, err
	// }

	// for i := 1; i < len(stmt); i++ {
	// 	rightCtx := NewBindContext(builder, ctx)
	// 	rightChildID, err := builder.buildTable(stmt[i], rightCtx)
	// 	if err != nil {
	// 		return 0, err
	// 	}

	// 	leftChildID = builder.appendNode(&plan.Node{
	// 		NodeType: plan.Node_JOIN,
	// 		Children: []int32{leftChildID, rightChildID},
	// 		JoinType: plan.Node_INNER,
	// 	}, nil)

	// 	if i == len(stmt)-1 {
	// 		builder.ctxByNode[leftChildID] = ctx
	// 		err = ctx.mergeContexts(leftCtx, rightCtx)
	// 		if err != nil {
	// 			return 0, err
	// 		}
	// 	} else {
	// 		newCtx := NewBindContext(builder, ctx)
	// 		builder.ctxByNode[leftChildID] = newCtx
	// 		err = newCtx.mergeContexts(leftCtx, rightCtx)
	// 		if err != nil {
	// 			return 0, err
	// 		}
	// 		leftCtx = newCtx
	// 	}
	// }

	// return leftChildID, err
}

func (builder *QueryBuilder) buildTable(stmt tree.TableExpr, ctx *BindContext, preNodeId int32, leftCtx *BindContext) (nodeID int32, err error) {
	switch tbl := stmt.(type) {
	case *tree.Select:
		subCtx := NewBindContext(builder, ctx)
		nodeID, err = builder.buildSelect(tbl, subCtx, false)
		if subCtx.isCorrelated {
			return 0, moerr.NewNYI(builder.GetContext(), "correlated subquery in FROM clause")
		}

		if subCtx.hasSingleRow {
			ctx.hasSingleRow = true
		}

	case *tree.TableName:
		schema := string(tbl.SchemaName)
		table := string(tbl.ObjectName)
		if len(table) == 0 || table == "dual" { //special table name
			nodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_VALUE_SCAN,
			}, ctx)

			ctx.hasSingleRow = true

			break
		}

		if len(schema) == 0 {
			cteRef := ctx.findCTE(table)
			if cteRef != nil {
				subCtx := NewBindContext(builder, ctx)
				subCtx.maskedCTEs = cteRef.maskedCTEs
				subCtx.cteName = table
				//reset defaultDatabase
				if len(cteRef.defaultDatabase) > 0 {
					subCtx.defaultDatabase = cteRef.defaultDatabase
				}

				switch stmt := cteRef.ast.Stmt.(type) {
				case *tree.Select:
					nodeID, err = builder.buildSelect(stmt, subCtx, false)

				case *tree.ParenSelect:
					nodeID, err = builder.buildSelect(stmt.Select, subCtx, false)

				default:
					err = moerr.NewParseError(builder.GetContext(), "unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL))
				}

				if err != nil {
					return
				}

				if subCtx.hasSingleRow {
					ctx.hasSingleRow = true
				}

				cols := cteRef.ast.Name.Cols

				if len(cols) > len(subCtx.headings) {
					return 0, moerr.NewSyntaxError(builder.GetContext(), "table %q has %d columns available but %d columns specified", table, len(subCtx.headings), len(cols))
				}

				for i, col := range cols {
					subCtx.headings[i] = string(col)
				}

				break
			}
			schema = ctx.defaultDatabase
		}

		obj, tableDef := builder.compCtx.Resolve(schema, table)
		if tableDef == nil {
			return 0, moerr.NewParseError(builder.GetContext(), "table %q does not exist", table)
		}

		tableDef.Name2ColIndex = map[string]int32{}
		for i := 0; i < len(tableDef.Cols); i++ {
			tableDef.Name2ColIndex[tableDef.Cols[i].Name] = int32(i)
		}
		nodeType := plan.Node_TABLE_SCAN
		if tableDef.TableType == catalog.SystemExternalRel {
			nodeType = plan.Node_EXTERNAL_SCAN
			col := &ColDef{
				Name: catalog.ExternalFilePath,
				Typ: &plan.Type{
					Id:    int32(types.T_varchar),
					Width: types.MaxVarcharLen,
					Table: table,
				},
			}
			tableDef.Cols = append(tableDef.Cols, col)
		} else if tableDef.TableType == catalog.SystemViewRel {
			if yes, dbOfView, nameOfView := builder.compCtx.GetBuildingAlterView(); yes {
				currentDB := schema
				if currentDB == "" {
					currentDB = builder.compCtx.DefaultDatabase()
				}
				if dbOfView == currentDB && nameOfView == table {
					return 0, moerr.NewInternalError(builder.GetContext(), "there is a recursive reference to the view %s", nameOfView)
				}
			}
			// set view statment to CTE
			viewDefString := tableDef.ViewSql.View

			if viewDefString != "" {
				if ctx.cteByName == nil {
					ctx.cteByName = make(map[string]*CTERef)
				}

				viewData := ViewData{}
				err := json.Unmarshal([]byte(viewDefString), &viewData)
				if err != nil {
					return 0, err
				}

				originStmts, err := mysql.Parse(builder.GetContext(), viewData.Stmt, 1)
				if err != nil {
					return 0, err
				}
				viewStmt, ok := originStmts[0].(*tree.CreateView)

				// No createview stmt, check alterview stmt.
				if !ok {
					alterstmt, ok := originStmts[0].(*tree.AlterView)
					viewStmt = &tree.CreateView{}
					if !ok {
						return 0, moerr.NewParseError(builder.GetContext(), "can not get view statement")
					}
					viewStmt.Name = alterstmt.Name
					viewStmt.ColNames = alterstmt.ColNames
					viewStmt.AsSource = alterstmt.AsSource
				}

				viewName := viewStmt.Name.ObjectName
				var maskedCTEs map[string]any
				if len(ctx.cteByName) > 0 {
					maskedCTEs = make(map[string]any)
					for name := range ctx.cteByName {
						maskedCTEs[name] = nil
					}
				}
				defaultDatabase := viewData.DefaultDatabase
				if obj.PubInfo != nil {
					defaultDatabase = obj.SubscriptionName
				}
				ctx.cteByName[string(viewName)] = &CTERef{
					ast: &tree.CTE{
						Name: &tree.AliasClause{
							Alias: viewName,
							Cols:  viewStmt.ColNames,
						},
						Stmt: viewStmt.AsSource,
					},
					defaultDatabase: defaultDatabase,
					maskedCTEs:      maskedCTEs,
				}

				newTableName := tree.NewTableName(viewName, tree.ObjectNamePrefix{
					CatalogName:     tbl.CatalogName, // TODO unused now, if used in some code, that will be save in view
					SchemaName:      tree.Identifier(""),
					ExplicitCatalog: false,
					ExplicitSchema:  false,
				})
				return builder.buildTable(newTableName, ctx, preNodeId, leftCtx)
			}
		}

		nodeID = builder.appendNode(&plan.Node{
			NodeType:    nodeType,
			Stats:       nil,
			ObjRef:      obj,
			TableDef:    tableDef,
			BindingTags: []int32{builder.genNewTag()},
		}, ctx)

	case *tree.JoinTableExpr:
		if tbl.Right == nil {
			return builder.buildTable(tbl.Left, ctx, preNodeId, leftCtx)
		}
		return builder.buildJoinTable(tbl, ctx)

	case *tree.TableFunction:
		if tbl.Id() == "result_scan" {
			return builder.buildResultScan(tbl, ctx)
		}
		return builder.buildTableFunction(tbl, ctx, preNodeId, leftCtx)

	case *tree.ParenTableExpr:
		return builder.buildTable(tbl.Expr, ctx, preNodeId, leftCtx)

	case *tree.AliasedTableExpr: //allways AliasedTableExpr first
		if _, ok := tbl.Expr.(*tree.Select); ok {
			if tbl.As.Alias == "" {
				return 0, moerr.NewSyntaxError(builder.GetContext(), "subquery in FROM must have an alias: %T", stmt)
			}
		}

		nodeID, err = builder.buildTable(tbl.Expr, ctx, preNodeId, leftCtx)
		if err != nil {
			return
		}

		err = builder.addBinding(nodeID, tbl.As, ctx)

		//tableDef := builder.qry.Nodes[nodeID].GetTableDef()
		midNode := builder.qry.Nodes[nodeID]
		//if it is the non-sys account and reads the cluster table,
		//we add an account_id filter to make sure that the non-sys account
		//can only read its own data.

		if midNode.NodeType == plan.Node_TABLE_SCAN {
			dbName := midNode.ObjRef.SchemaName
			tableName := midNode.TableDef.Name
			currentAccountID := builder.compCtx.GetAccountId()
			acctName := builder.compCtx.GetUserName()
			if sub := builder.compCtx.GetQueryingSubscription(); sub != nil {
				currentAccountID = uint32(sub.AccountId)
				builder.qry.Nodes[nodeID].NotCacheable = true
			}
			if currentAccountID != catalog.System_Account {
				// add account filter for system table scan
				if dbName == catalog.MO_CATALOG && tableName == catalog.MO_DATABASE {
					modatabaseFilter := util.BuildMoDataBaseFilter(uint64(currentAccountID))
					ctx.binder = NewWhereBinder(builder, ctx)
					accountFilterExprs, err := splitAndBindCondition(modatabaseFilter, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				} else if dbName == catalog.MO_SYSTEM_METRICS && tableName == catalog.MO_METRIC {
					motablesFilter := util.BuildSysMetricFilter(acctName)
					ctx.binder = NewWhereBinder(builder, ctx)
					accountFilterExprs, err := splitAndBindCondition(motablesFilter, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				} else if dbName == catalog.MO_SYSTEM && tableName == catalog.MO_STATEMENT {
					motablesFilter := util.BuildSysStatementInfoFilter(acctName)
					ctx.binder = NewWhereBinder(builder, ctx)
					accountFilterExprs, err := splitAndBindCondition(motablesFilter, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				} else if dbName == catalog.MO_CATALOG && tableName == catalog.MO_TABLES {
					motablesFilter := util.BuildMoTablesFilter(uint64(currentAccountID))
					ctx.binder = NewWhereBinder(builder, ctx)
					accountFilterExprs, err := splitAndBindCondition(motablesFilter, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				} else if dbName == catalog.MO_CATALOG && tableName == catalog.MO_COLUMNS {
					moColumnsFilter := util.BuildMoColumnsFilter(uint64(currentAccountID))
					ctx.binder = NewWhereBinder(builder, ctx)
					accountFilterExprs, err := splitAndBindCondition(moColumnsFilter, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				} else if util.TableIsClusterTable(midNode.GetTableDef().GetTableType()) {
					ctx.binder = NewWhereBinder(builder, ctx)
					left := &tree.UnresolvedName{
						NumParts: 1,
						Parts:    tree.NameParts{util.GetClusterTableAttributeName()},
					}
					currentAccountID := builder.compCtx.GetAccountId()
					right := tree.NewNumVal(constant.MakeUint64(uint64(currentAccountID)), strconv.Itoa(int(currentAccountID)), false)
					right.ValType = tree.P_uint64
					//account_id = the accountId of the non-sys account
					accountFilter := &tree.ComparisonExpr{
						Op:    tree.EQUAL,
						Left:  left,
						Right: right,
					}
					accountFilterExprs, err := splitAndBindCondition(accountFilter, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				}
			}
		}
		return
	case *tree.StatementSource:
		return 0, moerr.NewParseError(builder.GetContext(), "unsupport table expr: %T", stmt)

	default:
		// Values table not support
		return 0, moerr.NewParseError(builder.GetContext(), "unsupport table expr: %T", stmt)
	}

	return
}

func (builder *QueryBuilder) genNewTag() int32 {
	builder.nextTag++
	return builder.nextTag
}

func (builder *QueryBuilder) addBinding(nodeID int32, alias tree.AliasClause, ctx *BindContext) error {
	node := builder.qry.Nodes[nodeID]

	var cols []string
	var colIsHidden []bool
	var types []*plan.Type
	var binding *Binding
	var table string

	if node.NodeType == plan.Node_TABLE_SCAN || node.NodeType == plan.Node_MATERIAL_SCAN || node.NodeType == plan.Node_EXTERNAL_SCAN || node.NodeType == plan.Node_FUNCTION_SCAN || node.NodeType == plan.Node_VALUE_SCAN {
		if node.NodeType == plan.Node_VALUE_SCAN && node.TableDef == nil {
			return nil
		}
		if len(alias.Cols) > len(node.TableDef.Cols) {
			return moerr.NewSyntaxError(builder.GetContext(), "table %q has %d columns available but %d columns specified", alias.Alias, len(node.TableDef.Cols), len(alias.Cols))
		}

		if alias.Alias != "" {
			table = string(alias.Alias)
		} else {
			if node.NodeType == plan.Node_FUNCTION_SCAN {
				return moerr.NewSyntaxError(builder.GetContext(), "Every table function must have an alias")
			}

			table = node.TableDef.Name
		}

		if _, ok := ctx.bindingByTable[table]; ok {
			return moerr.NewSyntaxError(builder.GetContext(), "table name %q specified more than once", table)
		}

		colLength := len(node.TableDef.Cols)
		cols = make([]string, colLength)
		colIsHidden = make([]bool, colLength)
		types = make([]*plan.Type, colLength)

		tag := node.BindingTags[0]

		for i, col := range node.TableDef.Cols {
			if i < len(alias.Cols) {
				cols[i] = string(alias.Cols[i])
			} else {
				cols[i] = col.Name
			}
			colIsHidden[i] = col.Hidden
			types[i] = col.Typ
			name := table + "." + cols[i]
			builder.nameByColRef[[2]int32{tag, int32(i)}] = name
		}

		binding = NewBinding(tag, nodeID, table, node.TableDef.TblId, cols, colIsHidden, types, util.TableIsClusterTable(node.TableDef.TableType))
	} else {
		// Subquery
		subCtx := builder.ctxByNode[nodeID]
		tag := subCtx.rootTag()
		headings := subCtx.headings
		projects := subCtx.projects

		if len(alias.Cols) > len(headings) {
			return moerr.NewSyntaxError(builder.GetContext(), "11111 table %q has %d columns available but %d columns specified", alias.Alias, len(headings), len(alias.Cols))
		}

		table = subCtx.cteName
		if len(alias.Alias) > 0 {
			table = string(alias.Alias)
		}
		if len(table) == 0 {
			table = fmt.Sprintf("mo_table_subquery_alias_%d", tag)
		}
		if _, ok := ctx.bindingByTable[table]; ok {
			return moerr.NewSyntaxError(builder.GetContext(), "table name %q specified more than once", table)
		}

		colLength := len(headings)
		cols = make([]string, colLength)
		colIsHidden = make([]bool, colLength)
		types = make([]*plan.Type, colLength)

		for i, col := range headings {
			if i < len(alias.Cols) {
				cols[i] = string(alias.Cols[i])
			} else {
				cols[i] = strings.ToLower(col)
			}
			types[i] = projects[i].Typ
			colIsHidden[i] = false
			name := table + "." + cols[i]
			builder.nameByColRef[[2]int32{tag, int32(i)}] = name
		}

		binding = NewBinding(tag, nodeID, table, 0, cols, colIsHidden, types, false)
	}

	ctx.bindings = append(ctx.bindings, binding)
	ctx.bindingByTag[binding.tag] = binding
	ctx.bindingByTable[binding.table] = binding

	for _, col := range binding.cols {
		if _, ok := ctx.bindingByCol[col]; ok {
			ctx.bindingByCol[col] = nil
		} else {
			ctx.bindingByCol[col] = binding
		}
	}

	ctx.bindingTree = &BindingTreeNode{
		binding: binding,
	}

	return nil
}

func (builder *QueryBuilder) buildJoinTable(tbl *tree.JoinTableExpr, ctx *BindContext) (int32, error) {
	var joinType plan.Node_JoinType

	switch tbl.JoinType {
	case tree.JOIN_TYPE_CROSS, tree.JOIN_TYPE_INNER, tree.JOIN_TYPE_NATURAL:
		joinType = plan.Node_INNER
	case tree.JOIN_TYPE_LEFT, tree.JOIN_TYPE_NATURAL_LEFT:
		joinType = plan.Node_LEFT
	case tree.JOIN_TYPE_RIGHT, tree.JOIN_TYPE_NATURAL_RIGHT:
		joinType = plan.Node_RIGHT
	case tree.JOIN_TYPE_FULL:
		joinType = plan.Node_OUTER
	}

	leftCtx := NewBindContext(builder, ctx)
	rightCtx := NewBindContext(builder, ctx)

	leftChildID, err := builder.buildTable(tbl.Left, leftCtx, -1, leftCtx)
	if err != nil {
		return 0, err
	}
	if _, ok := tbl.Right.(*tree.TableFunction); ok {
		return 0, moerr.NewSyntaxError(builder.GetContext(), "Every table function must have an alias")
	}
	rightChildID, err := builder.buildTable(tbl.Right, rightCtx, leftChildID, leftCtx)
	if err != nil {
		return 0, err
	}

	if builder.qry.Nodes[rightChildID].NodeType == plan.Node_FUNCTION_SCAN {
		if joinType != plan.Node_INNER {
			return 0, moerr.NewSyntaxError(builder.GetContext(), "table function can only be used in a inner join")
		}
	}

	err = ctx.mergeContexts(builder.GetContext(), leftCtx, rightCtx)
	if err != nil {
		return 0, err
	}

	nodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{leftChildID, rightChildID},
		JoinType: joinType,
	}, ctx)
	node := builder.qry.Nodes[nodeID]

	ctx.binder = NewTableBinder(builder, ctx)

	switch cond := tbl.Cond.(type) {
	case *tree.OnJoinCond:
		joinConds, err := splitAndBindCondition(cond.Expr, ctx)
		if err != nil {
			return 0, err
		}

		node.OnList = joinConds

	case *tree.UsingJoinCond:
		for _, col := range cond.Cols {
			expr, err := ctx.addUsingCol(string(col), joinType, leftCtx, rightCtx)
			if err != nil {
				return 0, err
			}

			node.OnList = append(node.OnList, expr)
		}

	default:
		if tbl.JoinType == tree.JOIN_TYPE_NATURAL || tbl.JoinType == tree.JOIN_TYPE_NATURAL_LEFT || tbl.JoinType == tree.JOIN_TYPE_NATURAL_RIGHT {
			leftCols := make(map[string]any)
			for _, binding := range leftCtx.bindings {
				for i, col := range binding.cols {
					if binding.colIsHidden[i] {
						continue
					}
					leftCols[col] = nil
				}
			}

			var usingCols []string
			for _, binding := range rightCtx.bindings {
				for _, col := range binding.cols {
					if _, ok := leftCols[col]; ok {
						usingCols = append(usingCols, col)
					}
				}
			}

			for _, col := range usingCols {
				expr, err := ctx.addUsingCol(col, joinType, leftCtx, rightCtx)
				if err != nil {
					return 0, err
				}

				node.OnList = append(node.OnList, expr)
			}
		}
	}

	return nodeID, nil
}

func (builder *QueryBuilder) buildTableFunction(tbl *tree.TableFunction, ctx *BindContext, preNodeId int32, leftCtx *BindContext) (int32, error) {
	var (
		childId int32
		err     error
		nodeId  int32
	)

	if preNodeId == -1 {
		scanNode := &plan.Node{
			NodeType: plan.Node_VALUE_SCAN,
		}
		childId = builder.appendNode(scanNode, ctx)
		ctx.binder = NewTableBinder(builder, ctx)
	} else {
		ctx.binder = NewTableBinder(builder, leftCtx)
		childId = builder.copyNode(ctx, preNodeId)
	}

	exprs := make([]*plan.Expr, 0, len(tbl.Func.Exprs))
	for _, v := range tbl.Func.Exprs {
		curExpr, err := ctx.binder.BindExpr(v, 0, false)
		if err != nil {
			return 0, err
		}
		exprs = append(exprs, curExpr)
	}
	id := tbl.Id()
	switch id {
	case "unnest":
		nodeId, err = builder.buildUnnest(tbl, ctx, exprs, childId)
	case "generate_series":
		nodeId = builder.buildGenerateSeries(tbl, ctx, exprs, childId)
	case "meta_scan":
		nodeId, err = builder.buildMetaScan(tbl, ctx, exprs, childId)
	case "current_account":
		nodeId, err = builder.buildCurrentAccount(tbl, ctx, exprs, childId)
	case "metadata_scan":
		nodeId = builder.buildMetadataScan(tbl, ctx, exprs, childId)
	default:
		err = moerr.NewNotSupported(builder.GetContext(), "table function '%s' not supported", id)
	}
	return nodeId, err
}

func (builder *QueryBuilder) GetContext() context.Context {
	if builder == nil {
		return context.TODO()
	}
	return builder.compCtx.GetContext()
}

func (builder *QueryBuilder) checkExprCanPushdown(expr *Expr, node *Node) bool {
	switch node.NodeType {
	case plan.Node_FUNCTION_SCAN:
		if onlyContainsTag(expr, node.BindingTags[0]) {
			return true
		}
		for _, childId := range node.Children {
			if builder.checkExprCanPushdown(expr, builder.qry.Nodes[childId]) {
				return true
			}
		}
		return false
	case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN:
		return onlyContainsTag(expr, node.BindingTags[0])
	case plan.Node_JOIN:
		if containsTag(expr, builder.qry.Nodes[node.Children[0]].BindingTags[0]) && containsTag(expr, builder.qry.Nodes[node.Children[1]].BindingTags[0]) {
			return true
		}
		for _, childId := range node.Children {
			if builder.checkExprCanPushdown(expr, builder.qry.Nodes[childId]) {
				return true
			}
		}
		return false

	default:
		for _, childId := range node.Children {
			if builder.checkExprCanPushdown(expr, builder.qry.Nodes[childId]) {
				return true
			}
		}
		return false
	}
}
