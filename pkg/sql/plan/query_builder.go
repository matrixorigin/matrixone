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
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

func NewQueryBuilder(queryType plan.Query_StatementType, ctx CompilerContext, isPrepareStatement bool, skipStats bool) *QueryBuilder {
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
		compCtx:            ctx,
		ctxByNode:          []*BindContext{},
		nameByColRef:       make(map[[2]int32]string),
		nextTag:            0,
		mysqlCompatible:    mysqlCompatible,
		tag2Table:          make(map[int32]*TableDef),
		isPrepareStatement: isPrepareStatement,
		deleteNode:         make(map[uint64]int32),
		skipStats:          skipStats,
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
			var keys []string
			for k := range colMap {
				keys = append(keys, fmt.Sprintf("%v", k))
			}
			mapKeys := fmt.Sprintf("{ %s }", strings.Join(keys, ", "))
			return moerr.NewParseError(builder.GetContext(), "can't find column %v in context's map %s", mapID, mapKeys)
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
		//for _, arg := range ne.W.PartitionBy {
		//	err = builder.remapColRefForExpr(arg, colMap)
		//	if err != nil {
		//		return err
		//	}
		//}
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

func (builder *QueryBuilder) remapAllColRefs(nodeID int32, step int32, colRefCnt map[[2]int32]int, colRefBool map[[2]int32]bool, sinkColRef map[[2]int32]int) (*ColRefRemapping, error) {
	node := builder.qry.Nodes[nodeID]

	remapping := &ColRefRemapping{
		globalToLocal: make(map[[2]int32][2]int32),
	}

	switch node.NodeType {
	case plan.Node_FUNCTION_SCAN:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		internalRemapping := &ColRefRemapping{
			globalToLocal: make(map[[2]int32][2]int32),
		}

		tag := node.BindingTags[0]
		newTableDef := DeepCopyTableDef(node.TableDef, false)

		for i, col := range node.TableDef.Cols {
			globalRef := [2]int32{tag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			internalRemapping.addColRef(globalRef)

			newTableDef.Cols = append(newTableDef.Cols, DeepCopyColDef(col))
		}

		if len(newTableDef.Cols) == 0 {
			internalRemapping.addColRef([2]int32{tag, 0})
			newTableDef.Cols = append(newTableDef.Cols, DeepCopyColDef(node.TableDef.Cols[0]))
		}

		node.TableDef = newTableDef

		for _, expr := range node.FilterList {
			increaseRefCnt(expr, -1, colRefCnt)
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
						Name:   col.Name,
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
							Name:   node.TableDef.Cols[0].Name,
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
							Name:   node.TableDef.Cols[0].Name,
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
			increaseRefCnt(expr, 1, colRefCnt)
		}
		childMap, err := builder.remapAllColRefs(childId, step, colRefCnt, colRefBool, sinkColRef)

		if err != nil {
			return nil, err
		}

		for _, expr := range node.TblFuncExprList {
			increaseRefCnt(expr, -1, colRefCnt)
			err = builder.remapColRefForExpr(expr, childMap.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

	case plan.Node_TABLE_SCAN, plan.Node_MATERIAL_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_SOURCE_SCAN:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		for _, expr := range node.BlockFilterList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		for _, rfSpec := range node.RuntimeFilterProbeList {
			if rfSpec.Expr != nil {
				increaseRefCnt(rfSpec.Expr, 1, colRefCnt)
			}
		}

		internalRemapping := &ColRefRemapping{
			globalToLocal: make(map[[2]int32][2]int32),
		}

		tag := node.BindingTags[0]
		newTableDef := DeepCopyTableDef(node.TableDef, false)

		for i, col := range node.TableDef.Cols {
			globalRef := [2]int32{tag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			internalRemapping.addColRef(globalRef)

			newTableDef.Cols = append(newTableDef.Cols, DeepCopyColDef(col))
		}

		if len(newTableDef.Cols) == 0 {
			internalRemapping.addColRef([2]int32{tag, 0})
			newTableDef.Cols = append(newTableDef.Cols, DeepCopyColDef(node.TableDef.Cols[0]))
		}

		node.TableDef = newTableDef

		for _, expr := range node.FilterList {
			increaseRefCnt(expr, -1, colRefCnt)
			err := builder.remapColRefForExpr(expr, internalRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		for _, expr := range node.BlockFilterList {
			increaseRefCnt(expr, -1, colRefCnt)
			err := builder.remapColRefForExpr(expr, internalRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		for _, rfSpec := range node.RuntimeFilterProbeList {
			if rfSpec.Expr != nil {
				increaseRefCnt(rfSpec.Expr, -1, colRefCnt)
				err := builder.remapColRefForExpr(rfSpec.Expr, internalRemapping.globalToLocal)
				if err != nil {
					return nil, err
				}
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

	case plan.Node_INTERSECT, plan.Node_INTERSECT_ALL,
		plan.Node_UNION, plan.Node_UNION_ALL,
		plan.Node_MINUS, plan.Node_MINUS_ALL:

		thisTag := node.BindingTags[0]
		leftID := node.Children[0]
		rightID := node.Children[1]
		for i, expr := range node.ProjectList {
			increaseRefCnt(expr, 1, colRefCnt)
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
					}}, 1, colRefCnt)
			}
		}

		internalMap := make(map[[2]int32][2]int32)

		leftRemapping, err := builder.remapAllColRefs(leftID, step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}
		for k, v := range leftRemapping.globalToLocal {
			internalMap[k] = v
		}

		_, err = builder.remapAllColRefs(rightID, step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		for _, expr := range node.ProjectList {
			increaseRefCnt(expr, -1, colRefCnt)
			err := builder.remapColRefForExpr(expr, internalMap)
			if err != nil {
				return nil, err
			}
		}

	case plan.Node_JOIN:
		for _, expr := range node.OnList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		internalMap := make(map[[2]int32][2]int32)

		leftID := node.Children[0]
		leftRemapping, err := builder.remapAllColRefs(leftID, step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		for k, v := range leftRemapping.globalToLocal {
			internalMap[k] = v
		}

		rightID := node.Children[1]
		rightRemapping, err := builder.remapAllColRefs(rightID, step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		for k, v := range rightRemapping.globalToLocal {
			internalMap[k] = [2]int32{1, v[1]}
		}

		for _, expr := range node.OnList {
			increaseRefCnt(expr, -1, colRefCnt)
			err := builder.remapColRefForExpr(expr, internalMap)
			if err != nil {
				return nil, err
			}
		}

		childProjList := builder.qry.Nodes[leftID].ProjectList
		for i, globalRef := range leftRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)
			if node.JoinType == plan.Node_RIGHT {
				childProjList[i].Typ.NotNullable = false
			}
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
				Typ: plan.Type{
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

				if node.JoinType == plan.Node_LEFT {
					childProjList[i].Typ.NotNullable = false
				}

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
			increaseRefCnt(expr, 1, colRefCnt)
		}

		for _, expr := range node.AggList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
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
			increaseRefCnt(expr, -1, colRefCnt)
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
			increaseRefCnt(expr, -1, colRefCnt)
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

		child := builder.qry.Nodes[node.Children[0]]
		if child.NodeType == plan.Node_TABLE_SCAN && len(child.FilterList) == 0 && len(node.GroupBy) == 0 && child.Limit == nil && child.Offset == nil {
			child.AggList = make([]*Expr, 0, len(node.AggList))
			for _, agg := range node.AggList {
				switch agg.GetF().Func.ObjName {
				case "starcount", "count", "min", "max":
					child.AggList = append(child.AggList, DeepCopyExpr(agg))
				default:
					child.AggList = nil
				}
				if child.AggList == nil {
					break
				}
			}
		}

	case plan.Node_SAMPLE:
		groupTag := node.BindingTags[0]
		sampleTag := node.BindingTags[1]
		increaseRefCntForExprList(node.GroupBy, 1, colRefCnt)
		increaseRefCntForExprList(node.AggList, 1, colRefCnt)

		// the result order of sample will follow [group by columns, sample columns, other columns].
		// and the projection list needs to be based on the result order.
		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		for _, expr := range node.FilterList {
			builder.remapHavingClause(expr, groupTag, sampleTag, int32(len(node.GroupBy)))
		}

		// deal with group col and sample col.
		for i, expr := range node.GroupBy {
			increaseRefCnt(expr, -1, colRefCnt)
			err = builder.remapColRefForExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{groupTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: int32(len(node.ProjectList)),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		offsetSize := int32(len(node.GroupBy))
		for i, expr := range node.AggList {
			increaseRefCnt(expr, -1, colRefCnt)
			err = builder.remapColRefForExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{sampleTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -2,
						ColPos: int32(i) + offsetSize,
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		offsetSize += int32(len(node.AggList))
		childProjectionList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjectionList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i) + offsetSize,
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

	case plan.Node_TIME_WINDOW:
		for _, expr := range node.AggList {
			increaseRefCnt(expr, 1, colRefCnt)
		}
		increaseRefCnt(node.OrderBy[0].Expr, 1, colRefCnt)

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		timeTag := node.BindingTags[0]

		for i, expr := range node.FilterList {
			builder.remapWindowClause(expr, timeTag, int32(i))
		}

		increaseRefCnt(node.OrderBy[0].Expr, -1, colRefCnt)
		err = builder.remapColRefForExpr(node.OrderBy[0].Expr, childRemapping.globalToLocal)
		if err != nil {
			return nil, err
		}

		idx := 0
		var wstart, wend *plan.Expr
		var i, j int
		for k, expr := range node.AggList {
			if e, ok := expr.Expr.(*plan.Expr_Col); ok {
				if e.Col.Name == TimeWindowStart {
					wstart = expr
					i = k
				}
				if e.Col.Name == TimeWindowEnd {
					wend = expr
					j = k
				}
				continue
			}
			increaseRefCnt(expr, -1, colRefCnt)
			err = builder.remapColRefForExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{timeTag, int32(k)}
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
			idx++
		}

		if wstart != nil {
			increaseRefCnt(wstart, -1, colRefCnt)

			globalRef := [2]int32{timeTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				break
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: wstart.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: int32(idx),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
			idx++
		}

		if wend != nil {
			increaseRefCnt(wend, -1, colRefCnt)

			globalRef := [2]int32{timeTag, int32(j)}
			if colRefCnt[globalRef] == 0 {
				break
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: wstart.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: int32(idx),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

	case plan.Node_WINDOW:
		for _, expr := range node.WinSpecList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
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

		windowTag := node.BindingTags[0]
		l := len(childProjList)

		for _, expr := range node.FilterList {
			builder.remapWindowClause(expr, windowTag, int32(l))
		}

		for _, expr := range node.WinSpecList {
			increaseRefCnt(expr, -1, colRefCnt)
			err = builder.remapColRefForExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{windowTag, int32(node.GetWindowIdx())}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: int32(l),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

	case plan.Node_FILL:

		//for _, expr := range node.AggList {
		//	increaseRefCnt(expr, 1, colRefCnt)
		//}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
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

		//for _, expr := range node.AggList {
		//	increaseRefCnt(expr, -1, colRefCnt)
		//	err = builder.remapColRefForExpr(expr, childRemapping.globalToLocal)
		//	if err != nil {
		//		return nil, err
		//	}
		//}

	case plan.Node_SORT, plan.Node_PARTITION:
		for _, orderBy := range node.OrderBy {
			increaseRefCnt(orderBy.Expr, 1, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		for _, orderBy := range node.OrderBy {
			increaseRefCnt(orderBy.Expr, -1, colRefCnt)
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
			increaseRefCnt(expr, 1, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		for _, expr := range node.FilterList {
			increaseRefCnt(expr, -1, colRefCnt)
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

	case plan.Node_SINK_SCAN, plan.Node_RECURSIVE_SCAN, plan.Node_RECURSIVE_CTE:
		tag := node.BindingTags[0]
		var newProjList []*plan.Expr

		for i, expr := range node.ProjectList {
			globalRef := [2]int32{tag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}
			newProjList = append(newProjList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: 0,
						ColPos: int32(i),
					},
				},
			})
			remapping.addColRef(globalRef)
			for _, sourceStep := range node.SourceStep {
				if sourceStep >= step {
					continue
				}
				colRefBool[[2]int32{sourceStep, int32(i)}] = true
			}

		}
		node.ProjectList = newProjList

	case plan.Node_SINK:
		childNode := builder.qry.Nodes[node.Children[0]]
		resultTag := childNode.BindingTags[0]
		for i := range childNode.ProjectList {
			if colRefBool[[2]int32{step, int32(i)}] {
				colRefCnt[[2]int32{resultTag, int32(i)}] = 1
			}
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}
		var newProjList []*plan.Expr
		for i, expr := range node.ProjectList {
			if !colRefBool[[2]int32{step, int32(i)}] {
				continue
			}
			sinkColRef[[2]int32{step, int32(i)}] = len(newProjList)
			newProjList = append(newProjList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: 0,
						ColPos: childRemapping.globalToLocal[[2]int32{resultTag, int32(i)}][1],
						// Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		node.ProjectList = newProjList

	case plan.Node_PROJECT, plan.Node_MATERIAL:
		projectTag := node.BindingTags[0]

		var neededProj []int32

		for i, expr := range node.ProjectList {
			globalRef := [2]int32{projectTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			neededProj = append(neededProj, int32(i))
			increaseRefCnt(expr, 1, colRefCnt)
		}

		if len(neededProj) == 0 {
			increaseRefCnt(node.ProjectList[0], 1, colRefCnt)
			neededProj = append(neededProj, 0)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		childProjList := builder.qry.Nodes[node.Children[0]].ProjectList
		var newProjList []*plan.Expr
		for _, needed := range neededProj {
			expr := node.ProjectList[needed]
			increaseRefCnt(expr, -1, colRefCnt)
			err := builder.remapColRefForExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			switch ne := expr.Expr.(type) {
			case *plan.Expr_Col:
				expr.Typ.NotNullable = childProjList[ne.Col.ColPos].Typ.NotNullable
			}

			globalRef := [2]int32{projectTag, needed}
			remapping.addColRef(globalRef)

			newProjList = append(newProjList, expr)
		}

		node.ProjectList = newProjList

	case plan.Node_DISTINCT:
		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
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
		node.NotCacheable = true
		// VALUE_SCAN always have one column now
		if node.TableDef == nil { // like select 1,2
			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}},
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

	case plan.Node_LOCK_OP:
		preNode := builder.qry.Nodes[node.Children[0]]
		pkexpr := &plan.Expr{
			Typ: node.LockTargets[0].GetPrimaryColTyp(),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: node.BindingTags[1],
					ColPos: node.LockTargets[0].PrimaryColIdxInBat,
				},
			},
		}
		oldPos := [2]int32{node.BindingTags[1], node.LockTargets[0].PrimaryColIdxInBat}
		increaseRefCnt(pkexpr, 1, colRefCnt)
		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		tableDef := node.GetTableDef()
		if tableDef.Partition != nil {
			partitionIdx := len(preNode.ProjectList)
			partitionExpr := DeepCopyExpr(tableDef.Partition.PartitionExpression)
			preNode.ProjectList = append(preNode.ProjectList, partitionExpr)

			partTableIDs, _ := getPartTableIdsAndNames(builder.compCtx, preNode.GetObjRef(), tableDef)
			node.LockTargets[0].IsPartitionTable = true
			node.LockTargets[0].PartitionTableIds = partTableIDs
			node.LockTargets[0].FilterColIdxInBat = int32(partitionIdx)
		}

		if newPos, ok := childRemapping.globalToLocal[oldPos]; ok {
			node.LockTargets[0].PrimaryColIdxInBat = newPos[1]
		}
		increaseRefCnt(pkexpr, -1, colRefCnt)

		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: preNode.ProjectList[i].Typ,
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
				Typ: preNode.ProjectList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			})
		}

	default:
		return nil, moerr.NewInternalError(builder.GetContext(), "unsupport node type")
	}

	node.BindingTags = nil

	return remapping, nil
}

func (builder *QueryBuilder) markSinkProject(nodeID int32, step int32, colRefBool map[[2]int32]bool) {
	node := builder.qry.Nodes[nodeID]

	switch node.NodeType {
	case plan.Node_SINK_SCAN, plan.Node_RECURSIVE_SCAN, plan.Node_RECURSIVE_CTE:
		for _, i := range node.SourceStep {
			if i >= step {
				for _, expr := range node.ProjectList {
					colRefBool[[2]int32{i, expr.GetCol().ColPos}] = true
				}
			}
		}
	default:
		for i := range node.Children {
			builder.markSinkProject(node.Children[i], step, colRefBool)
		}
	}
}

func (builder *QueryBuilder) rewriteStarApproxCount(nodeID int32) {
	node := builder.qry.Nodes[nodeID]

	switch node.NodeType {
	case plan.Node_AGG:
		if len(node.GroupBy) == 0 && len(node.AggList) == 1 {
			if agg := node.AggList[0].GetF(); agg != nil && agg.Func.ObjName == "approx_count" {
				if len(node.Children) == 1 {
					child := builder.qry.Nodes[node.Children[0]]
					if child.NodeType == plan.Node_TABLE_SCAN && len(child.FilterList) == 0 {
						agg.Func.ObjName = "sum"
						fr, _ := function.GetFunctionByName(context.TODO(), "sum", []types.Type{types.T_int64.ToType()})
						agg.Func.Obj = fr.GetEncodedOverloadID()
						agg.Args[0] = &plan.Expr{
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: 0,
									ColPos: Metadata_Rows_Cnt_Pos,
								},
							},
						}

						var exprs []*plan.Expr
						str := child.ObjRef.SchemaName + "." + child.TableDef.Name
						exprs = append(exprs, &plan.Expr{
							Typ: Type{
								Id:          int32(types.T_varchar),
								NotNullable: true,
								Width:       int32(len(str)),
							},
							Expr: &plan.Expr_Lit{
								Lit: &plan.Literal{
									Value: &plan.Literal_Sval{
										Sval: str,
									},
								},
							},
						})
						str = child.TableDef.Cols[0].Name
						exprs = append(exprs, &plan.Expr{
							Typ: Type{
								Id:          int32(types.T_varchar),
								NotNullable: true,
								Width:       int32(len(str)),
							},
							Expr: &plan.Expr_Lit{
								Lit: &plan.Literal{
									Value: &plan.Literal_Sval{
										Sval: str,
									},
								},
							},
						})
						scanNode := &plan.Node{
							NodeType: plan.Node_VALUE_SCAN,
						}
						childId := builder.appendNode(scanNode, nil)
						node.Children[0] = builder.buildMetadataScan(nil, nil, exprs, childId)
						child = builder.qry.Nodes[node.Children[0]]
						switch expr := agg.Args[0].Expr.(type) {
						case *plan.Expr_Col:
							expr.Col.RelPos = child.BindingTags[0]
							agg.Args[0].Typ = child.TableDef.Cols[expr.Col.ColPos].Typ
						}
					}
				}
			}
		}
	default:
		for i := range node.Children {
			builder.rewriteStarApproxCount(node.Children[i])
		}
	}
}

func (builder *QueryBuilder) createQuery() (*Query, error) {
	colRefBool := make(map[[2]int32]bool)
	sinkColRef := make(map[[2]int32]int)

	builder.parseOptimizeHints()
	for i, rootID := range builder.qry.Steps {
		builder.skipStats = builder.canSkipStats()
		builder.rewriteDistinctToAGG(rootID)
		builder.rewriteEffectlessAggToProject(rootID)
		rootID, _ = builder.pushdownFilters(rootID, nil, false)
		builder.mergeFiltersOnCompositeKey(rootID)
		err := foldTableScanFilters(builder.compCtx.GetProcess(), builder.qry, rootID)
		if err != nil {
			return nil, err
		}
		builder.optimizeDateFormatExpr(rootID)

		builder.pushdownLimitToTableScan(rootID)

		colRefCnt := make(map[[2]int32]int)
		builder.countColRefs(rootID, colRefCnt)
		builder.removeSimpleProjections(rootID, plan.Node_UNKNOWN, false, colRefCnt)

		rewriteFilterListByStats(builder.GetContext(), rootID, builder)
		ReCalcNodeStats(rootID, builder, true, true, true)
		builder.determineBuildAndProbeSide(rootID, true)
		determineHashOnPK(rootID, builder)
		tagCnt := make(map[int32]int)
		rootID = builder.removeEffectlessLeftJoins(rootID, tagCnt)
		ReCalcNodeStats(rootID, builder, true, false, true)
		builder.pushdownTopThroughLeftJoin(rootID)
		ReCalcNodeStats(rootID, builder, true, false, true)

		rootID = builder.aggPushDown(rootID)
		ReCalcNodeStats(rootID, builder, true, false, true)
		rootID = builder.determineJoinOrder(rootID)
		colMap := make(map[[2]int32]int)
		colGroup := make([]int, 0)
		builder.removeRedundantJoinCond(rootID, colMap, colGroup)
		ReCalcNodeStats(rootID, builder, true, false, true)
		rootID = builder.applyAssociativeLaw(rootID)
		builder.determineBuildAndProbeSide(rootID, true)
		rootID = builder.aggPullup(rootID, rootID)
		ReCalcNodeStats(rootID, builder, true, false, true)
		rootID = builder.pushdownSemiAntiJoins(rootID)
		builder.optimizeDistinctAgg(rootID)
		ReCalcNodeStats(rootID, builder, true, false, true)
		builder.determineBuildAndProbeSide(rootID, true)

		builder.qry.Steps[i] = rootID

		// XXX: This will be removed soon, after merging implementation of all hash-join operators
		builder.swapJoinChildren(rootID)
		ReCalcNodeStats(rootID, builder, true, false, true)

		builder.partitionPrune(rootID)

		builder.optimizeLikeExpr(rootID)
		rootID = builder.applyIndices(rootID, colRefCnt, make(map[[2]int32]*plan.Expr))
		ReCalcNodeStats(rootID, builder, true, false, true)

		determineHashOnPK(rootID, builder)
		determineShuffleMethod(rootID, builder)
		determineShuffleMethod2(rootID, -1, builder)
		// after determine shuffle, be careful when calling ReCalcNodeStats again.
		// needResetHashMapStats should always be false from here

		builder.generateRuntimeFilters(rootID)
		ReCalcNodeStats(rootID, builder, true, false, false)
		builder.forceJoinOnOneCN(rootID, false)
		// after this ,never call ReCalcNodeStats again !!!

		if builder.isForUpdate {
			reCheckifNeedLockWholeTable(builder)
		}

		builder.handleMessgaes(rootID)

		builder.rewriteStarApproxCount(rootID)

		rootNode := builder.qry.Nodes[rootID]

		for j := range rootNode.ProjectList {
			colRefBool[[2]int32{int32(i), int32(j)}] = false
			if i == len(builder.qry.Steps)-1 {
				colRefBool[[2]int32{int32(i), int32(j)}] = true
			}
		}
	}

	for i := range builder.qry.Steps {
		rootID := builder.qry.Steps[i]
		builder.markSinkProject(rootID, int32(i), colRefBool)
	}

	for i := len(builder.qry.Steps) - 1; i >= 0; i-- {
		rootID := builder.qry.Steps[i]
		rootNode := builder.qry.Nodes[rootID]
		resultTag := rootNode.BindingTags[0]
		colRefCnt := make(map[[2]int32]int)
		for j := range rootNode.ProjectList {
			colRefCnt[[2]int32{resultTag, int32(j)}] = 1
		}
		_, err := builder.remapAllColRefs(rootID, int32(i), colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}
	}

	//for i := 1; i < len(builder.qry.Steps); i++ {
	//	builder.remapSinkScanColRefs(builder.qry.Steps[i], int32(i), sinkColRef)
	//}
	builder.hintQueryType()
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
		subCtx.unionSelect = subCtx.initSelect
		if slt, ok := sltStmt.(*tree.Select); ok {
			nodeID, err = builder.buildSelect(slt, subCtx, isRoot)
		} else {
			nodeID, err = builder.buildSelect(&tree.Select{Select: sltStmt}, subCtx, isRoot)
		}
		if err != nil {
			return 0, err
		}
		ctx.views = append(ctx.views, subCtx.views...)

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
			var targetType plan.Type
			var targetArgType types.Type
			if len(argsCastType) == 0 {
				targetArgType = tmpArgsType[0]
				// if string union string, different length may cause error.
				if targetArgType.Oid == types.T_varchar || targetArgType.Oid == types.T_char {
					for _, typ := range argsType {
						if targetArgType.Width < typ.Width {
							targetArgType.Width = typ.Width
						}
					}
				}
			} else {
				targetArgType = argsCastType[0]
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
		ctx.aliasMap[v] = &aliasItem{
			idx: int32(i),
		}
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

			if cExpr, ok := node.Limit.Expr.(*plan.Expr_Lit); ok {
				if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
					ctx.hasSingleRow = c.U64Val == 1
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

	if ctx.initSelect {
		lastNodeID = appendSinkNodeWithTag(builder, ctx, lastNodeID, ctx.sinkTag)
	}

	// set heading
	if isRoot {
		builder.qry.Headings = append(builder.qry.Headings, ctx.headings...)
	}

	return lastNodeID, nil
}

const NameGroupConcat = "group_concat"
const NameClusterCenters = "cluster_centers"

func (bc *BindContext) generateForceWinSpecList() ([]*plan.Expr, error) {
	windowsSpecList := make([]*plan.Expr, 0, len(bc.aggregates))
	j := 0

	if len(bc.windows) < 1 {
		panic("no winspeclist to be used to force")
	}

	for i := range bc.aggregates {
		windowExpr := DeepCopyExpr(bc.windows[j])
		windowSpec := windowExpr.GetW()
		if windowSpec == nil {
			panic("no winspeclist to be used to force")
		}
		windowSpec.WindowFunc = DeepCopyExpr(bc.aggregates[i])
		windowExpr.Typ = bc.aggregates[i].Typ
		windowSpec.Name = bc.aggregates[i].GetF().Func.ObjName

		if windowSpec.Name == NameGroupConcat {
			if j < len(bc.windows)-1 {
				j++
			}
			// NOTE: no need to include NameClusterCenters
		} else {
			windowSpec.OrderBy = nil
		}
		windowsSpecList = append(windowsSpecList, windowExpr)
	}

	return windowsSpecList, nil
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

			var maskedCTEs map[string]bool
			if len(maskedNames) > 0 {
				maskedCTEs = make(map[string]bool)
				for _, mask := range maskedNames {
					maskedCTEs[mask] = true
				}
			}

			maskedNames = append(maskedNames, name)

			ctx.cteByName[name] = &CTERef{
				ast:         cte,
				isRecursive: stmt.With.IsRecursive,
				maskedCTEs:  maskedCTEs,
			}
		}

		// Try to do binding for CTE at declaration
		for _, cte := range stmt.With.CTEs {

			table := string(cte.Name.Alias)
			cteRef := ctx.cteByName[table]

			var err error
			var s *tree.Select
			switch stmt := cte.Stmt.(type) {
			case *tree.Select:
				s = stmt

			case *tree.ParenSelect:
				s = stmt.Select

			default:
				return 0, moerr.NewParseError(builder.GetContext(), "unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL))
			}

			var left *tree.SelectStatement
			var stmts []tree.SelectStatement
			left, err = builder.splitRecursiveMember(&s.Select, table, &stmts)
			if err != nil {
				return 0, err
			}
			isR := len(stmts) > 0

			if isR && !cteRef.isRecursive {
				return 0, moerr.NewParseError(builder.GetContext(), "not declare RECURSIVE: '%v'", tree.String(stmt, dialect.MYSQL))
			} else if !isR {
				subCtx := NewBindContext(builder, ctx)
				subCtx.normalCTE = true
				subCtx.cteName = table
				subCtx.maskedCTEs = cteRef.maskedCTEs
				cteRef.isRecursive = false

				oldSnapshot := builder.compCtx.GetSnapshot()
				builder.compCtx.SetSnapshot(subCtx.snapshot)
				nodeID, err := builder.buildSelect(s, subCtx, false)
				builder.compCtx.SetSnapshot(oldSnapshot)
				if err != nil {
					return 0, err
				}
				if len(cteRef.ast.Name.Cols) > 0 && len(cteRef.ast.Name.Cols) != len(builder.qry.Nodes[nodeID].ProjectList) {
					return 0, moerr.NewSyntaxError(builder.GetContext(), "table %q has %d columns available but %d columns specified", table, len(builder.qry.Nodes[nodeID].ProjectList), len(cteRef.ast.Name.Cols))
				}
				ctx.views = append(ctx.views, subCtx.views...)
			} else {
				initCtx := NewBindContext(builder, ctx)
				initCtx.initSelect = true
				initCtx.sinkTag = builder.genNewTag()
				initCtx.isTryBindingCTE = true
				initLastNodeID, err := builder.buildSelect(&tree.Select{Select: *left}, initCtx, false)
				if err != nil {
					return 0, err
				}
				if len(cteRef.ast.Name.Cols) > 0 && len(cteRef.ast.Name.Cols) != len(builder.qry.Nodes[initLastNodeID].ProjectList) {
					return 0, moerr.NewSyntaxError(builder.GetContext(), "table %q has %d columns available but %d columns specified", table, len(builder.qry.Nodes[initLastNodeID].ProjectList), len(cteRef.ast.Name.Cols))
				}
				//ctx.views = append(ctx.views, initCtx.views...)

				recursiveNodeId := initLastNodeID
				for _, r := range stmts {
					subCtx := NewBindContext(builder, ctx)
					subCtx.maskedCTEs = cteRef.maskedCTEs
					subCtx.recSelect = true
					subCtx.sinkTag = builder.genNewTag()
					subCtx.isTryBindingCTE = true
					subCtx.cteByName = make(map[string]*CTERef)
					subCtx.cteByName[table] = cteRef
					err = builder.addBinding(initLastNodeID, *cteRef.ast.Name, subCtx)
					if err != nil {
						return 0, err
					}
					sourceStep := builder.appendStep(recursiveNodeId)
					nodeID := appendRecursiveScanNode(builder, subCtx, sourceStep, subCtx.sinkTag)
					subCtx.recRecursiveScanNodeId = nodeID
					recursiveNodeId, err = builder.buildSelect(&tree.Select{Select: r}, subCtx, false)
					if err != nil {
						return 0, err
					}
					//ctx.views = append(ctx.views, subCtx.views...)
				}
				builder.qry.Steps = builder.qry.Steps[:0]
			}
		}
	}

	var clause *tree.SelectClause
	var valuesClause *tree.ValuesClause
	var nodeID int32
	var err error
	astOrderBy := stmt.OrderBy
	astLimit := stmt.Limit
	astTimeWindow := stmt.TimeWindow

	if stmt.SelectLockInfo != nil && stmt.SelectLockInfo.LockType == tree.SelectLockForUpdate {
		builder.isForUpdate = true
	}

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
		if builder.isForUpdate {
			return 0, moerr.NewInternalError(builder.GetContext(), "not support select union for update")
		}
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
	var lockNode *plan.Node
	var notCacheable bool

	if clause == nil {
		proc := builder.compCtx.GetProcess()
		rowCount := len(valuesClause.Rows)
		if len(valuesClause.Rows) == 0 {
			return 0, moerr.NewInternalError(builder.GetContext(), "values statement have not rows")
		}
		bat := batch.NewWithSize(len(valuesClause.Rows[0]))
		strTyp := plan.Type{
			Id:          int32(types.T_text),
			NotNullable: false,
		}
		strColTyp := makeTypeByPlan2Type(strTyp)
		strColTargetTyp := &plan.Expr{
			Typ: strTyp,
			Expr: &plan.Expr_T{
				T: &plan.TargetType{},
			},
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
			vec := proc.GetVector(types.T_text.ToType())
			bat.Vecs[i] = vec
			rowSetData.Cols[i] = &plan.ColData{}
			for j := 0; j < rowCount; j++ {
				if err := vector.AppendBytes(vec, nil, true, proc.Mp()); err != nil {
					bat.Clean(proc.Mp())
					return 0, err
				}
				planExpr, err := ctx.binder.BindExpr(valuesClause.Rows[j][i], 0, true)
				if err != nil {
					return 0, err
				}
				planExpr, err = forceCastExpr2(builder.GetContext(), planExpr, strColTyp, strColTargetTyp)
				if err != nil {
					return 0, err
				}
				rowSetData.Cols[i].Data = append(rowSetData.Cols[i].Data, &plan.RowsetExpr{
					RowPos: int32(j),
					Expr:   planExpr,
					Pos:    -1,
				})
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
				Typ:   strTyp,
			}
		}
		bat.SetRowCount(rowCount)
		nodeUUID, _ := uuid.NewV7()
		nodeID = builder.appendNode(&plan.Node{
			NodeType:     plan.Node_VALUE_SCAN,
			RowsetData:   rowSetData,
			TableDef:     tableDef,
			BindingTags:  []int32{builder.genNewTag()},
			Uuid:         nodeUUID[:],
			NotCacheable: true,
		}, ctx)
		if builder.isPrepareStatement {
			proc.SetPrepareBatch(bat)
		} else {
			proc.SetValueScanBatch(nodeUUID, bat)
		}

		err = builder.addBinding(nodeID, tree.AliasClause{
			Alias: "_ValueScan",
		}, ctx)
		if err != nil {
			return 0, err
		}
	} else {
		if ctx.recSelect && clause.Distinct {
			return 0, moerr.NewParseError(builder.GetContext(), "not support DISTINCT in recursive cte")
		}
		// build FROM clause
		nodeID, err = builder.buildFrom(clause.From.Tables, ctx)
		if err != nil {
			return 0, err
		}

		ctx.binder = NewWhereBinder(builder, ctx)
		if !ctx.isTryBindingCTE {
			if ctx.initSelect {
				clause.Exprs = append(clause.Exprs, makeZeroRecursiveLevel())
			} else if ctx.recSelect {
				clause.Exprs = append(clause.Exprs, makePlusRecursiveLevel(ctx.cteName))
			}
		}
		// unfold stars and generate headings
		selectList, err = appendSelectList(builder, ctx, selectList, clause.Exprs...)
		if err != nil {
			return 0, err
		}

		if len(selectList) == 0 {
			return 0, moerr.NewParseError(builder.GetContext(), "No tables used")
		}

		if builder.isForUpdate {
			tableDef := builder.qry.Nodes[nodeID].GetTableDef()
			pkPos, pkTyp := getPkPos(tableDef, false)
			lockTarget := &plan.LockTarget{
				TableId:            tableDef.TblId,
				PrimaryColIdxInBat: int32(pkPos),
				PrimaryColTyp:      pkTyp,
				Block:              true,
				RefreshTsIdxInBat:  -1, //unsupport now
				FilterColIdxInBat:  -1, //unsupport now
			}
			lockNode = &Node{
				NodeType:    plan.Node_LOCK_OP,
				Children:    []int32{nodeID},
				TableDef:    tableDef,
				LockTargets: []*plan.LockTarget{lockTarget},
				BindingTags: []int32{builder.genNewTag(), builder.qry.Nodes[nodeID].BindingTags[0]},
			}

			if astLimit == nil {
				nodeID = builder.appendNode(lockNode, ctx)
			}
		}

		// rewrite right join to left join
		builder.rewriteRightJoinToLeftJoin(nodeID)

		if !ctx.isTryBindingCTE && ctx.recSelect {
			f := &tree.FuncExpr{
				Func:  tree.FuncName2ResolvableFunctionReference(tree.SetUnresolvedName(moCheckRecursionLevelFun)),
				Exprs: tree.Exprs{tree.NewComparisonExpr(tree.LESS_THAN, tree.SetUnresolvedName(ctx.cteName, moRecursiveLevelCol), tree.NewNumValWithType(constant.MakeInt64(moDefaultRecursionMax), fmt.Sprintf("%d", moDefaultRecursionMax), false, tree.P_int64))},
			}
			if clause.Where != nil {
				clause.Where = &tree.Where{Type: tree.AstWhere, Expr: tree.NewAndExpr(clause.Where.Expr, f)}
			} else {
				clause.Where = &tree.Where{Type: tree.AstWhere, Expr: f}
			}
		}
		if clause.Where != nil {
			whereList, err := splitAndBindCondition(clause.Where.Expr, NoAlias, ctx)
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

			for _, filter := range newFilterList {
				if detectedExprWhetherTimeRelated(filter) {
					notCacheable = true
				}
			}

			nodeID = builder.appendNode(&plan.Node{
				NodeType:     plan.Node_FILTER,
				Children:     []int32{nodeID},
				FilterList:   newFilterList,
				NotCacheable: notCacheable,
			}, ctx)
		}

		ctx.groupTag = builder.genNewTag()
		ctx.aggregateTag = builder.genNewTag()
		ctx.projectTag = builder.genNewTag()
		ctx.windowTag = builder.genNewTag()
		ctx.sampleTag = builder.genNewTag()
		if astTimeWindow != nil {
			ctx.timeTag = builder.genNewTag() // ctx.timeTag > 0
		}

		// Preprocess aliases
		for i := range selectList {
			selectList[i].Expr, err = ctx.qualifyColumnNames(selectList[i].Expr, NoAlias)
			if err != nil {
				return 0, err
			}

			builder.nameByColRef[[2]int32{ctx.projectTag, int32(i)}] = tree.String(selectList[i].Expr, dialect.MYSQL)

			if selectList[i].As != nil && !selectList[i].As.Empty() {
				ctx.aliasMap[selectList[i].As.ToLower()] = &aliasItem{
					idx:     int32(i),
					astExpr: selectList[i].Expr,
				}
			}
		}

		// bind GROUP BY clause
		if clause.GroupBy != nil {
			if ctx.recSelect {
				return 0, moerr.NewParseError(builder.GetContext(), "not support group by in recursive cte: '%v'", tree.String(&clause.GroupBy, dialect.MYSQL))
			}
			groupBinder := NewGroupBinder(builder, ctx)
			for _, group := range clause.GroupBy {
				group, err = ctx.qualifyColumnNames(group, AliasAfterColumn)
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
			if ctx.recSelect {
				return 0, moerr.NewParseError(builder.GetContext(), "not support having in recursive cte: '%v'", tree.String(clause.Having, dialect.MYSQL))
			}
			ctx.binder = havingBinder
			havingList, err = splitAndBindCondition(clause.Having.Expr, AliasAfterColumn, ctx)
			if err != nil {
				return 0, err
			}
		}

		ctx.isDistinct = clause.Distinct
	}

	// bind SELECT clause (Projection List)
	projectionBinder = NewProjectionBinder(builder, ctx, havingBinder)
	if err = bindProjectionList(ctx, projectionBinder, selectList); err != nil {
		return 0, err
	}

	resultLen = len(ctx.projects)
	for i, proj := range ctx.projects {
		exprStr := proj.String()
		if _, ok := ctx.projectByExpr[exprStr]; !ok {
			ctx.projectByExpr[exprStr] = int32(i)
		}

		if exprCol, ok := proj.Expr.(*plan.Expr_Col); ok {
			if col := exprCol.Col; col != nil {
				if binding, ok := ctx.bindingByTag[col.RelPos]; ok {
					col.DbName = binding.db
					col.TblName = binding.table
				}
			}
		}

		if !notCacheable {
			if detectedExprWhetherTimeRelated(proj) {
				notCacheable = true
			}
		}
	}

	// bind TIME WINDOW
	var fillType plan.Node_FillType
	var fillVals, fillCols []*Expr
	var inteval, sliding *Expr
	var orderBy *plan.OrderBySpec
	if astTimeWindow != nil {
		h := projectionBinder.havingBinder
		col, err := ctx.qualifyColumnNames(astTimeWindow.Interval.Col, NoAlias)
		if err != nil {
			return 0, err
		}
		r := col.(*tree.UnresolvedName)
		table := r.Parts[1]
		schema := ctx.defaultDatabase
		if len(r.Parts[2]) > 0 {
			schema = r.Parts[2]
		}

		// snapshot to fix
		pk := builder.compCtx.GetPrimaryKeyDef(schema, table, Snapshot{TS: &timestamp.Timestamp{}})
		if len(pk) > 1 || pk[0].Name != r.Parts[0] {
			return 0, moerr.NewNotSupported(builder.GetContext(), "%s is not primary key in time window", tree.String(col, dialect.MYSQL))
		}
		h.insideAgg = true
		expr, err := h.BindExpr(col, 0, true)
		if err != nil {
			return 0, err
		}
		h.insideAgg = false
		if expr.Typ.Id != int32(types.T_timestamp) {
			return 0, moerr.NewNotSupported(builder.GetContext(), "the type of %s must be timestamp in time window", tree.String(col, dialect.MYSQL))
		}
		orderBy = &plan.OrderBySpec{
			Expr: expr,
			Flag: plan.OrderBySpec_INTERNAL | plan.OrderBySpec_ASC | plan.OrderBySpec_NULLS_FIRST,
		}

		name := tree.SetUnresolvedName("interval")
		arg2 := tree.NewNumValWithType(constant.MakeString(astTimeWindow.Interval.Unit), astTimeWindow.Interval.Unit, false, tree.P_char)
		itr := &tree.FuncExpr{
			Func:  tree.FuncName2ResolvableFunctionReference(name),
			Exprs: tree.Exprs{astTimeWindow.Interval.Val, arg2},
		}
		inteval, err = projectionBinder.BindExpr(itr, 0, true)
		if err != nil {
			return 0, err
		}

		if astTimeWindow.Sliding != nil {
			arg2 = tree.NewNumValWithType(constant.MakeString(astTimeWindow.Sliding.Unit), astTimeWindow.Sliding.Unit, false, tree.P_char)
			sld := &tree.FuncExpr{
				Func:  tree.FuncName2ResolvableFunctionReference(name),
				Exprs: tree.Exprs{astTimeWindow.Sliding.Val, arg2},
			}
			sliding, err = projectionBinder.BindExpr(sld, 0, true)
			if err != nil {
				return 0, err
			}
		}

		if astTimeWindow.Fill != nil && astTimeWindow.Fill.Mode != tree.FillNone && astTimeWindow.Fill.Mode != tree.FillNull {
			switch astTimeWindow.Fill.Mode {
			case tree.FillPrev:
				fillType = plan.Node_PREV
			case tree.FillNext:
				fillType = plan.Node_NEXT
			case tree.FillValue:
				fillType = plan.Node_VALUE
			case tree.FillLinear:
				fillType = plan.Node_LINEAR
			}
			var v *Expr
			if astTimeWindow.Fill.Val != nil {
				v, err = projectionBinder.BindExpr(astTimeWindow.Fill.Val, 0, true)
				if err != nil {
					return 0, err
				}
			}

			for _, t := range ctx.times {
				if e, ok := t.Expr.(*plan.Expr_Col); ok {
					if e.Col.Name == TimeWindowStart || e.Col.Name == TimeWindowEnd {
						continue
					}
				}
				if astTimeWindow.Fill.Val != nil {
					e, err := appendCastBeforeExpr(builder.GetContext(), v, t.Typ)
					if err != nil {
						return 0, err
					}
					fillVals = append(fillVals, e)
				}
				fillCols = append(fillCols, t)
			}
			if astTimeWindow.Fill.Mode == tree.FillLinear {
				for i, e := range ctx.timeAsts {
					b := &tree.BinaryExpr{
						Op: tree.DIV,
						Left: &tree.ParenExpr{
							Expr: &tree.BinaryExpr{
								Op:    tree.PLUS,
								Left:  e,
								Right: e,
							},
						},
						Right: tree.NewNumValWithType(constant.MakeInt64(2), "2", false, tree.P_int64),
					}
					v, err = projectionBinder.BindExpr(b, 0, true)
					if err != nil {
						return 0, err
					}
					col, err := appendCastBeforeExpr(builder.GetContext(), v, fillCols[i].Typ)
					if err != nil {
						return 0, err
					}
					fillVals = append(fillVals, col)
				}
			}
		}
	}

	// bind ORDER BY clause
	var orderBys []*plan.OrderBySpec
	if astOrderBy != nil {
		if ctx.recSelect {
			return 0, moerr.NewParseError(builder.GetContext(), "not support order by in recursive cte: '%v'", tree.String(&astOrderBy, dialect.MYSQL))
		}
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
		}
		if astLimit.Count != nil {
			limitExpr, err = limitBinder.BindExpr(astLimit.Count, 0, true)
			if err != nil {
				return 0, err
			}

			if cExpr, ok := limitExpr.Expr.(*plan.Expr_Lit); ok {
				if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
					ctx.hasSingleRow = c.U64Val == 1
				}
			}
		}
		if builder.isForUpdate {
			nodeID = builder.appendNode(lockNode, ctx)
		}
	}

	// return err if it's not a legal SAMPLE function.
	if err = validSample(ctx, builder); err != nil {
		return 0, err
	}

	// sample can ignore these check because it supports the sql like 'select a, b, sample(c) from t group by a' whose b is not in group by clause.
	if !ctx.sampleFunc.hasSampleFunc {
		if (len(ctx.groups) > 0 || len(ctx.aggregates) > 0 || len(ctx.times) > 0) && len(projectionBinder.boundCols) > 0 {
			if !builder.mysqlCompatible {
				return 0, moerr.NewSyntaxError(builder.GetContext(), "column %q must appear in the GROUP BY clause or be used in an aggregate function", projectionBinder.boundCols[0])
			}

			// For MySQL compatibility, wrap bare ColRefs in any_value()
			for i, proj := range ctx.projects {
				ctx.projects[i] = builder.wrapBareColRefsInAnyValue(proj, ctx)
			}
		}
	}

	if ctx.forceWindows {
		ctx.tmpGroups = ctx.groups
		ctx.windows, _ = ctx.generateForceWinSpecList()
		ctx.aggregates = nil
		ctx.groups = nil

		for i := range ctx.projects {
			ctx.projects[i] = DeepProcessExprForGroupConcat(ctx.projects[i], ctx)
		}

		for i := range havingList {
			havingList[i] = DeepProcessExprForGroupConcat(havingList[i], ctx)
		}

		for i := range orderBys {
			orderBys[i].Expr = DeepProcessExprForGroupConcat(orderBys[i].Expr, ctx)
		}

	}

	// FIXME: delete this when SINGLE join is ready
	if len(ctx.groups) == 0 && len(ctx.aggregates) > 0 {
		ctx.hasSingleRow = true
	}

	// append AGG node or SAMPLE node
	if ctx.sampleFunc.hasSampleFunc {
		nodeID = builder.appendNode(generateSamplePlanNode(ctx, nodeID), ctx)

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

		for name, id := range ctx.sampleByAst {
			builder.nameByColRef[[2]int32{ctx.sampleTag, id}] = name
		}
	} else {
		if len(ctx.groups) > 0 || len(ctx.aggregates) > 0 {
			if ctx.recSelect {
				return 0, moerr.NewInternalError(builder.GetContext(), "not support aggregate function recursive cte")
			}
			if builder.isForUpdate {
				return 0, moerr.NewInternalError(builder.GetContext(), "not support select aggregate function for update")
			}
			if ctx.forceWindows {
			} else {
				nodeID = builder.appendNode(&plan.Node{
					NodeType:    plan.Node_AGG,
					Children:    []int32{nodeID},
					GroupBy:     ctx.groups,
					AggList:     ctx.aggregates,
					BindingTags: []int32{ctx.groupTag, ctx.aggregateTag},
				}, ctx)
			}
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
	}

	// append TIME WINDOW node
	if len(ctx.times) > 0 {
		if ctx.recSelect {
			return 0, moerr.NewInternalError(builder.GetContext(), "not support time window in recursive cte")
		}
		nodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_TIME_WINDOW,
			Children:    []int32{nodeID},
			AggList:     ctx.times,
			BindingTags: []int32{ctx.timeTag},
			OrderBy:     []*plan.OrderBySpec{orderBy},
			Interval:    inteval,
			Sliding:     sliding,
		}, ctx)

		for name, id := range ctx.timeByAst {
			builder.nameByColRef[[2]int32{ctx.timeTag, id}] = name
		}

		if astTimeWindow.Fill != nil {
			nodeID = builder.appendNode(&plan.Node{
				NodeType:    plan.Node_FILL,
				Children:    []int32{nodeID},
				AggList:     fillCols,
				BindingTags: []int32{ctx.timeTag},
				FillType:    fillType,
				FillVal:     fillVals,
			}, ctx)
		}
	}

	// append WINDOW node
	if len(ctx.windows) > 0 {
		if ctx.recSelect {
			return 0, moerr.NewInternalError(builder.GetContext(), "not support window function in recursive cte")
		}

		for i, w := range ctx.windows {
			e := w.GetW()
			if len(e.PartitionBy) > 0 {
				partitionBy := make([]*plan.OrderBySpec, 0, len(e.PartitionBy))
				for _, p := range e.PartitionBy {
					partitionBy = append(partitionBy, &plan.OrderBySpec{
						Expr: p,
						Flag: plan.OrderBySpec_INTERNAL,
					})
				}
				nodeID = builder.appendNode(&plan.Node{
					NodeType:    plan.Node_PARTITION,
					Children:    []int32{nodeID},
					OrderBy:     partitionBy,
					BindingTags: []int32{ctx.windowTag},
				}, ctx)
			}
			nodeID = builder.appendNode(&plan.Node{
				NodeType:    plan.Node_WINDOW,
				Children:    []int32{nodeID},
				WinSpecList: []*Expr{w},
				WindowIdx:   int32(i),
				BindingTags: []int32{ctx.windowTag},
			}, ctx)
		}

		for name, id := range ctx.windowByAst {
			builder.nameByColRef[[2]int32{ctx.windowTag, id}] = name
		}

		if ctx.forceWindows {
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
		NodeType:     plan.Node_PROJECT,
		ProjectList:  ctx.projects,
		Children:     []int32{nodeID},
		BindingTags:  []int32{ctx.projectTag},
		NotCacheable: notCacheable,
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

	if (ctx.initSelect || ctx.recSelect) && !ctx.unionSelect {
		nodeID = appendSinkNodeWithTag(builder, ctx, nodeID, ctx.sinkTag)
	}

	if isRoot {
		builder.qry.Headings = append(builder.qry.Headings, ctx.headings...)
	}

	return nodeID, nil
}

func DeepProcessExprForGroupConcat(expr *Expr, ctx *BindContext) *Expr {
	if expr == nil {
		return nil
	}
	switch item := expr.Expr.(type) {
	case *plan.Expr_Col:
		if item.Col.RelPos == ctx.groupTag {
			expr = DeepCopyExpr(ctx.tmpGroups[item.Col.ColPos])
		}
		if item.Col.RelPos == ctx.aggregateTag {
			item.Col.RelPos = ctx.windowTag
		}

	case *plan.Expr_F:
		for i, arg := range item.F.Args {
			item.F.Args[i] = DeepProcessExprForGroupConcat(arg, ctx)
		}
	case *plan.Expr_W:
		for i, p := range item.W.PartitionBy {
			item.W.PartitionBy[i] = DeepProcessExprForGroupConcat(p, ctx)
		}
		for i, o := range item.W.OrderBy {
			item.W.OrderBy[i].Expr = DeepProcessExprForGroupConcat(o.Expr, ctx)
		}

	case *plan.Expr_Sub:
		DeepProcessExprForGroupConcat(item.Sub.Child, ctx)

	case *plan.Expr_Corr:
		if item.Corr.RelPos == ctx.groupTag {
			expr = DeepCopyExpr(ctx.tmpGroups[item.Corr.ColPos])
		}
		if item.Corr.RelPos == ctx.aggregateTag {
			item.Corr.RelPos = ctx.windowTag
		}
	case *plan.Expr_List:
		for i, ie := range item.List.List {
			item.List.List[i] = DeepProcessExprForGroupConcat(ie, ctx)
		}
	}
	return expr

}

func appendSelectList(
	builder *QueryBuilder,
	ctx *BindContext,
	selectList tree.SelectExprs, exprs ...tree.SelectExpr) (tree.SelectExprs, error) {
	accountId, err := builder.compCtx.GetAccountId()
	if err != nil {
		return nil, err
	}
	for _, selectExpr := range exprs {
		switch expr := selectExpr.Expr.(type) {
		case tree.UnqualifiedStar:
			cols, names, err := ctx.unfoldStar(builder.GetContext(), "", accountId == catalog.System_Account)
			if err != nil {
				return nil, err
			}
			for i, name := range names {
				if ctx.finalSelect && name == moRecursiveLevelCol {
					continue
				}
				selectList = append(selectList, cols[i])
				ctx.headings = append(ctx.headings, name)
			}

		case *tree.SampleExpr:
			var err error
			if err = ctx.sampleFunc.GenerateSampleFunc(expr); err != nil {
				return nil, err
			}

			oldLen := len(selectList)
			columns, isStar := expr.GetColumns()
			if isStar {
				if selectList, err = appendSelectList(builder, ctx, selectList, tree.SelectExpr{Expr: tree.UnqualifiedStar{}}); err != nil {
					return nil, err
				}
			} else {
				for _, column := range columns {
					if selectList, err = appendSelectList(builder, ctx, selectList, tree.SelectExpr{Expr: column}); err != nil {
						return nil, err
					}
				}
			}

			// deal with alias.
			sampleCount := len(selectList) - oldLen
			if selectExpr.As != nil && !selectExpr.As.Empty() {
				if sampleCount != 1 {
					return nil, moerr.NewSyntaxError(builder.GetContext(), "sample multi columns cannot have alias")
				}
				ctx.headings[len(ctx.headings)-1] = selectExpr.As.Origin()
				selectList[len(selectList)-1].As = selectExpr.As
			}

			ctx.sampleFunc.SetStartOffset(oldLen, sampleCount)

		case *tree.UnresolvedName:
			if expr.Star {
				cols, names, err := ctx.unfoldStar(builder.GetContext(), expr.Parts[0], accountId == catalog.System_Account)
				if err != nil {
					return nil, err
				}
				selectList = append(selectList, cols...)
				ctx.headings = append(ctx.headings, names...)
			} else {
				if selectExpr.As != nil && !selectExpr.As.Empty() {
					ctx.headings = append(ctx.headings, selectExpr.As.Origin())
				} else if expr.CStrParts[0] != nil {
					ctx.headings = append(ctx.headings, expr.CStrParts[0].Compare())
				} else {
					ctx.headings = append(ctx.headings, expr.Parts[0])
				}

				newExpr, err := ctx.qualifyColumnNames(expr, NoAlias)
				if err != nil {
					return nil, err
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
				ctx.headings = append(ctx.headings, selectExpr.As.Origin())
			} else {
				ctx.headings = append(ctx.headings, tree.String(expr, dialect.MYSQL))
			}

			selectList = append(selectList, tree.SelectExpr{
				Expr: expr,
				As:   selectExpr.As,
			})
		default:
			if selectExpr.As != nil && !selectExpr.As.Empty() {
				ctx.headings = append(ctx.headings, selectExpr.As.Origin())
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

			newExpr, err := ctx.qualifyColumnNames(expr, NoAlias)
			if err != nil {
				return nil, err
			}

			selectList = append(selectList, tree.SelectExpr{
				Expr: newExpr,
				As:   selectExpr.As,
			})
		}
	}
	return selectList, nil
}

func bindProjectionList(
	ctx *BindContext,
	projectionBinder *ProjectionBinder,
	selectList tree.SelectExprs) error {
	ctx.binder = projectionBinder

	sampleRangeLeft, sampleRangeRight := ctx.sampleFunc.start, ctx.sampleFunc.start+ctx.sampleFunc.offset
	if ctx.sampleFunc.hasSampleFunc {
		// IF sample function exists, we should bind the sample column first.
		sampleList, err := ctx.sampleFunc.BindSampleColumn(ctx, projectionBinder, selectList[sampleRangeLeft:sampleRangeRight])
		if err != nil {
			return err
		}

		for i := range selectList[:sampleRangeLeft] {
			expr, err := projectionBinder.BindExpr(selectList[i].Expr, 0, true)
			if err != nil {
				return err
			}
			ctx.projects = append(ctx.projects, expr)
		}
		ctx.projects = append(ctx.projects, sampleList...)
	}

	for i := range selectList[sampleRangeRight:] {
		expr, err := projectionBinder.BindExpr(selectList[i].Expr, 0, true)
		if err != nil {
			return err
		}

		ctx.projects = append(ctx.projects, expr)
	}
	return nil
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
	ReCalcNodeStats(nodeID, builder, false, true, true)
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

func (builder *QueryBuilder) splitRecursiveMember(stmt *tree.SelectStatement, name string, stmts *[]tree.SelectStatement) (*tree.SelectStatement, error) {
	ok, left, err := builder.checkRecursiveCTE(stmt, name, stmts)
	if !ok || err != nil {
		return left, err
	}
	return builder.splitRecursiveMember(left, name, stmts)
}

func (builder *QueryBuilder) checkRecursiveCTE(left *tree.SelectStatement, name string, stmts *[]tree.SelectStatement) (bool, *tree.SelectStatement, error) {
	if u, ok := (*left).(*tree.UnionClause); ok {
		if !u.All {
			return false, left, nil
		}

		rt, ok := u.Right.(*tree.SelectClause)
		if !ok {
			return false, left, nil
		}

		count, err := builder.checkRecursiveTable(rt.From.Tables[0], name)
		if err != nil && count > 0 {
			return false, left, err
		}
		if count == 0 {
			return false, left, nil
		}
		if count > 1 {
			return false, left, moerr.NewParseError(builder.GetContext(), "unsupport multiple recursive table expr in recursive CTE: %T", left)
		}
		*stmts = append(*stmts, u.Right)
		return true, &u.Left, nil
	}
	return false, left, nil
}

func (builder *QueryBuilder) checkRecursiveTable(stmt tree.TableExpr, name string) (int, error) {
	switch tbl := stmt.(type) {
	case *tree.Select:
		return 0, moerr.NewParseError(builder.GetContext(), "unsupport SUBQUERY in recursive CTE: %T", stmt)

	case *tree.TableName:
		table := string(tbl.ObjectName)
		if table == name {
			return 1, nil
		}
		return 0, nil

	case *tree.JoinTableExpr:
		var err, err0 error
		if tbl.JoinType == tree.JOIN_TYPE_LEFT || tbl.JoinType == tree.JOIN_TYPE_RIGHT || tbl.JoinType == tree.JOIN_TYPE_NATURAL_LEFT || tbl.JoinType == tree.JOIN_TYPE_NATURAL_RIGHT {
			err0 = moerr.NewParseError(builder.GetContext(), "unsupport LEFT, RIGHT or OUTER JOIN in recursive CTE: %T", stmt)
		}
		c1, err1 := builder.checkRecursiveTable(tbl.Left, name)
		c2, err2 := builder.checkRecursiveTable(tbl.Right, name)
		if err0 != nil {
			err = err0
		} else if err1 != nil {
			err = err1
		} else {
			err = err2
		}
		c := c1 + c2
		return c, err

	case *tree.TableFunction:
		return 0, nil

	case *tree.ParenTableExpr:
		return builder.checkRecursiveTable(tbl.Expr, name)

	case *tree.AliasedTableExpr:
		return builder.checkRecursiveTable(tbl.Expr, name)

	default:
		return 0, nil
	}
}

func getSelectTree(s *tree.Select) *tree.Select {
	switch stmt := s.Select.(type) {
	case *tree.ParenSelect:
		return getSelectTree(stmt.Select)
	default:
		return s
	}
}

func (builder *QueryBuilder) buildTable(stmt tree.TableExpr, ctx *BindContext, preNodeId int32, leftCtx *BindContext) (nodeID int32, err error) {
	switch tbl := stmt.(type) {
	case *tree.Select:
		if builder.isForUpdate {
			return 0, moerr.NewInternalError(builder.GetContext(), "not support select from derived table for update")
		}
		subCtx := NewBindContext(builder, ctx)
		nodeID, err = builder.buildSelect(tbl, subCtx, false)
		if subCtx.isCorrelated {
			return 0, moerr.NewNYI(builder.GetContext(), "correlated subquery in FROM clause")
		}

		if subCtx.hasSingleRow {
			ctx.hasSingleRow = true
		}
		ctx.views = append(ctx.views, subCtx.views...)

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

		if len(schema) == 0 && ctx.normalCTE && table == ctx.cteName {
			return 0, moerr.NewParseError(builder.GetContext(), "In recursive query block of Recursive Common Table Expression %s, the recursive table must be referenced only once, and not in any subquery", table)
		} else if len(schema) == 0 {
			cteRef := ctx.findCTE(table)
			if cteRef != nil {
				if ctx.recSelect {
					nodeID = ctx.recRecursiveScanNodeId
					return
				}

				var s *tree.Select
				switch stmt := cteRef.ast.Stmt.(type) {
				case *tree.Select:
					s = getSelectTree(stmt)
				case *tree.ParenSelect:
					s = getSelectTree(stmt.Select)
				default:
					err = moerr.NewParseError(builder.GetContext(), "unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL))
					return
				}

				var left *tree.SelectStatement
				var stmts []tree.SelectStatement
				left, err = builder.splitRecursiveMember(&s.Select, table, &stmts)
				if err != nil {
					return 0, err
				}
				isR := len(stmts) > 0

				if isR && !cteRef.isRecursive {
					err = moerr.NewParseError(builder.GetContext(), "not declare RECURSIVE: '%v'", tree.String(stmt, dialect.MYSQL))
				} else if !isR {
					subCtx := NewBindContext(builder, ctx)
					subCtx.maskedCTEs = cteRef.maskedCTEs
					subCtx.cteName = table
					subCtx.snapshot = cteRef.snapshot
					//reset defaultDatabase
					if len(cteRef.defaultDatabase) > 0 {
						subCtx.defaultDatabase = cteRef.defaultDatabase
					}
					cteRef.isRecursive = false

					oldSnapshot := builder.compCtx.GetSnapshot()
					builder.compCtx.SetSnapshot(subCtx.snapshot)
					nodeID, err = builder.buildSelect(s, subCtx, false)
					builder.compCtx.SetSnapshot(oldSnapshot)
					if err != nil {
						return
					}

					if subCtx.hasSingleRow {
						ctx.hasSingleRow = true
					}
					ctx.views = append(ctx.views, subCtx.views...)

					cols := cteRef.ast.Name.Cols

					if len(cols) > len(subCtx.headings) {
						return 0, moerr.NewSyntaxError(builder.GetContext(), "table %q has %d columns available but %d columns specified", table, len(subCtx.headings), len(cols))
					}

					for i, col := range cols {
						subCtx.headings[i] = string(col)
					}
				} else {
					if len(s.OrderBy) > 0 {
						return 0, moerr.NewParseError(builder.GetContext(), "not support ORDER BY in recursive cte")
					}
					// initial statement
					initCtx := NewBindContext(builder, ctx)
					initCtx.initSelect = true
					initCtx.sinkTag = builder.genNewTag()
					initLastNodeID, err1 := builder.buildSelect(&tree.Select{Select: *left}, initCtx, false)
					if err1 != nil {
						err = err1
						return
					}
					projects := builder.qry.Nodes[builder.qry.Nodes[initLastNodeID].Children[0]].ProjectList
					// recursive statement
					recursiveLastNodeID := initLastNodeID
					initSourceStep := int32(len(builder.qry.Steps))
					recursiveSteps := make([]int32, len(stmts))
					recursiveNodeIDs := make([]int32, len(stmts))
					if len(cteRef.ast.Name.Cols) > 0 {
						cteRef.ast.Name.Cols = append(cteRef.ast.Name.Cols, moRecursiveLevelCol)
					}

					for i, r := range stmts {
						subCtx := NewBindContext(builder, ctx)
						subCtx.maskedCTEs = cteRef.maskedCTEs
						subCtx.cteName = table
						if len(cteRef.defaultDatabase) > 0 {
							subCtx.defaultDatabase = cteRef.defaultDatabase
						}
						subCtx.recSelect = true
						subCtx.sinkTag = initCtx.sinkTag
						subCtx.cteByName = make(map[string]*CTERef)
						subCtx.cteByName[table] = cteRef
						err = builder.addBinding(initLastNodeID, *cteRef.ast.Name, subCtx)
						if err != nil {
							return
						}
						_ = builder.appendStep(recursiveLastNodeID)
						subCtx.recRecursiveScanNodeId = appendRecursiveScanNode(builder, subCtx, initSourceStep, subCtx.sinkTag)
						recursiveNodeIDs[i] = subCtx.recRecursiveScanNodeId
						recursiveSteps[i] = int32(len(builder.qry.Steps))
						recursiveLastNodeID, err = builder.buildSelect(&tree.Select{Select: r}, subCtx, false)
						if err != nil {
							return
						}
						// some check
						n := builder.qry.Nodes[builder.qry.Nodes[recursiveLastNodeID].Children[0]]
						if len(projects) != len(n.ProjectList) {
							return 0, moerr.NewParseError(builder.GetContext(), "recursive cte %s projection error", table)
						}
						for i := range n.ProjectList {
							projTyp := projects[i].GetTyp()
							n.ProjectList[i], err = makePlan2CastExpr(builder.GetContext(), n.ProjectList[i], projTyp)
							if err != nil {
								return
							}
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
					}
					// union all statement
					var limitExpr *Expr
					var offsetExpr *Expr
					if s.Limit != nil {
						limitBinder := NewLimitBinder(builder, ctx)
						if s.Limit.Offset != nil {
							offsetExpr, err = limitBinder.BindExpr(s.Limit.Offset, 0, true)
							if err != nil {
								return 0, err
							}
						}
						if s.Limit.Count != nil {
							limitExpr, err = limitBinder.BindExpr(s.Limit.Count, 0, true)
							if err != nil {
								return 0, err
							}

							if cExpr, ok := limitExpr.Expr.(*plan.Expr_Lit); ok {
								if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
									ctx.hasSingleRow = c.U64Val == 1
								}
							}
						}
					}

					_ = builder.appendStep(recursiveLastNodeID)
					nodeID = appendCTEScanNode(builder, ctx, initSourceStep, initCtx.sinkTag)
					if limitExpr != nil || offsetExpr != nil {
						node := builder.qry.Nodes[nodeID]
						node.Limit = limitExpr
						node.Offset = offsetExpr
					}
					for i := 0; i < len(recursiveSteps); i++ {
						builder.qry.Nodes[nodeID].SourceStep = append(builder.qry.Nodes[nodeID].SourceStep, recursiveSteps[i])
					}
					curStep := int32(len(builder.qry.Steps))
					for _, id := range recursiveNodeIDs {
						// builder.qry.Nodes[id].SourceStep = append(builder.qry.Nodes[id].SourceStep, curStep)
						builder.qry.Nodes[id].SourceStep[0] = curStep
					}
					unionAllLastNodeID := appendSinkNodeWithTag(builder, ctx, nodeID, ctx.sinkTag)
					builder.qry.Nodes[unionAllLastNodeID].RecursiveSink = true

					// final statement
					ctx.finalSelect = true
					ctx.sinkTag = initCtx.sinkTag
					err = builder.addBinding(initLastNodeID, *cteRef.ast.Name, ctx)
					if err != nil {
						return
					}
					sourceStep := builder.appendStep(unionAllLastNodeID)
					nodeID = appendSinkScanNodeWithTag(builder, ctx, sourceStep, initCtx.sinkTag)
					// builder.qry.Nodes[nodeID].SourceStep = append(builder.qry.Nodes[nodeID].SourceStep, initSourceStep)
				}

				break
			}
			schema = ctx.defaultDatabase
		}

		if tbl.AtTsExpr != nil {
			ctx.snapshot, err = builder.resolveTsHint(tbl.AtTsExpr)
			if err != nil {
				return 0, err
			}
		}
		snapshot := ctx.snapshot
		if snapshot == nil {
			snapshot = &Snapshot{TS: &timestamp.Timestamp{}}
		}

		// TODO
		schema, err = databaseIsValid(schema, builder.compCtx, *snapshot)
		if err != nil {
			return 0, err
		}

		// TODO
		obj, tableDef := builder.compCtx.Resolve(schema, table, *snapshot)
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
				Typ: plan.Type{
					Id:    int32(types.T_varchar),
					Width: types.MaxVarcharLen,
					Table: table,
				},
			}
			tableDef.Cols = append(tableDef.Cols, col)
		} else if tableDef.TableType == catalog.SystemSourceRel {
			nodeType = plan.Node_SOURCE_SCAN
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

				originStmts, err := mysql.Parse(builder.GetContext(), viewData.Stmt, 1, 0)
				defer func() {
					for _, s := range originStmts {
						s.Free()
					}
				}()
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
				var maskedCTEs map[string]bool
				if len(ctx.cteByName) > 0 {
					maskedCTEs = make(map[string]bool)
					for name := range ctx.cteByName {
						maskedCTEs[name] = true
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
					snapshot:        snapshot,
				}
				// consist with frontend.genKey()
				ctx.views = append(ctx.views, schema+"#"+table)

				newTableName := tree.NewTableName(viewName, tree.ObjectNamePrefix{
					CatalogName:     tbl.CatalogName, // TODO unused now, if used in some code, that will be save in view
					SchemaName:      tree.Identifier(""),
					ExplicitCatalog: false,
					ExplicitSchema:  false,
				}, nil)
				return builder.buildTable(newTableName, ctx, preNodeId, leftCtx)
			}
		}

		nodeID = builder.appendNode(&plan.Node{
			NodeType:     nodeType,
			Stats:        nil,
			ObjRef:       obj,
			TableDef:     tableDef,
			BindingTags:  []int32{builder.genNewTag()},
			ScanSnapshot: snapshot,
		}, ctx)

	case *tree.JoinTableExpr:
		if tbl.Right == nil {
			return builder.buildTable(tbl.Left, ctx, preNodeId, leftCtx)
		} else if builder.isForUpdate {
			return 0, moerr.NewInternalError(builder.GetContext(), "not support select from join table for update")
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
			currentAccountID, err := builder.compCtx.GetAccountId()
			if err != nil {
				return 0, err
			}
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
					accountFilterExprs, err := splitAndBindCondition(modatabaseFilter, NoAlias, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				} else if dbName == catalog.MO_SYSTEM_METRICS && (tableName == catalog.MO_METRIC || tableName == catalog.MO_SQL_STMT_CU) {
					motablesFilter := util.BuildSysMetricFilter(acctName)
					ctx.binder = NewWhereBinder(builder, ctx)
					accountFilterExprs, err := splitAndBindCondition(motablesFilter, NoAlias, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				} else if dbName == catalog.MO_SYSTEM && tableName == catalog.MO_STATEMENT {
					motablesFilter := util.BuildSysStatementInfoFilter(acctName)
					ctx.binder = NewWhereBinder(builder, ctx)
					accountFilterExprs, err := splitAndBindCondition(motablesFilter, NoAlias, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				} else if dbName == catalog.MO_CATALOG && tableName == catalog.MO_TABLES {
					motablesFilter := util.BuildMoTablesFilter(uint64(currentAccountID))
					ctx.binder = NewWhereBinder(builder, ctx)
					accountFilterExprs, err := splitAndBindCondition(motablesFilter, NoAlias, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				} else if dbName == catalog.MO_CATALOG && tableName == catalog.MO_COLUMNS {
					moColumnsFilter := util.BuildMoColumnsFilter(uint64(currentAccountID))
					ctx.binder = NewWhereBinder(builder, ctx)
					accountFilterExprs, err := splitAndBindCondition(moColumnsFilter, NoAlias, ctx)
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
					right := tree.NewNumVal(constant.MakeUint64(uint64(currentAccountID)), strconv.Itoa(int(currentAccountID)), false)
					right.ValType = tree.P_uint64
					//account_id = the accountId of the non-sys account
					accountFilter := &tree.ComparisonExpr{
						Op:    tree.EQUAL,
						Left:  left,
						Right: right,
					}
					accountFilterExprs, err := splitAndBindCondition(accountFilter, NoAlias, ctx)
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

func (builder *QueryBuilder) genNewMsgTag() (ret int32) {
	// start from 1, and 0 means do not handle with message
	builder.nextMsgTag++
	return builder.nextMsgTag
}

func (builder *QueryBuilder) addBinding(nodeID int32, alias tree.AliasClause, ctx *BindContext) error {
	node := builder.qry.Nodes[nodeID]

	var cols []string
	var colIsHidden []bool
	var types []*plan.Type
	var defaultVals []string
	var binding *Binding
	var table string
	if node.NodeType == plan.Node_TABLE_SCAN || node.NodeType == plan.Node_MATERIAL_SCAN || node.NodeType == plan.Node_EXTERNAL_SCAN || node.NodeType == plan.Node_FUNCTION_SCAN || node.NodeType == plan.Node_VALUE_SCAN || node.NodeType == plan.Node_SINK_SCAN || node.NodeType == plan.Node_RECURSIVE_SCAN || node.NodeType == plan.Node_SOURCE_SCAN {
		if (node.NodeType == plan.Node_VALUE_SCAN || node.NodeType == plan.Node_SINK_SCAN || node.NodeType == plan.Node_RECURSIVE_SCAN) && node.TableDef == nil {
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
			if node.NodeType == plan.Node_RECURSIVE_SCAN || node.NodeType == plan.Node_SINK_SCAN {
				return nil
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
		defaultVals = make([]string, colLength)

		tag := node.BindingTags[0]

		for i, col := range node.TableDef.Cols {
			if i < len(alias.Cols) {
				cols[i] = string(alias.Cols[i])
			} else {
				cols[i] = col.Name
			}
			colIsHidden[i] = col.Hidden
			types[i] = &col.Typ
			if col.Default != nil {
				defaultVals[i] = col.Default.OriginString
			}
			name := table + "." + cols[i]
			builder.nameByColRef[[2]int32{tag, int32(i)}] = name
		}

		binding = NewBinding(tag, nodeID, node.TableDef.DbName, table, node.TableDef.TblId, cols, colIsHidden, types,
			util.TableIsClusterTable(node.TableDef.TableType), defaultVals)
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
		defaultVals = make([]string, colLength)

		for i, col := range headings {
			if i < len(alias.Cols) {
				cols[i] = string(alias.Cols[i])
			} else {
				cols[i] = strings.ToLower(col)
			}
			types[i] = &projects[i].Typ
			colIsHidden[i] = false
			defaultVals[i] = ""
			name := table + "." + cols[i]
			builder.nameByColRef[[2]int32{tag, int32(i)}] = name
		}

		binding = NewBinding(tag, nodeID, "", table, 0, cols, colIsHidden, types, false, defaultVals)
	}

	ctx.bindings = append(ctx.bindings, binding)
	ctx.bindingByTag[binding.tag] = binding
	ctx.bindingByTable[binding.table] = binding

	if node.NodeType != plan.Node_RECURSIVE_SCAN && node.NodeType != plan.Node_SINK_SCAN {
		for _, col := range binding.cols {
			if _, ok := ctx.bindingByCol[col]; ok {
				ctx.bindingByCol[col] = nil
			} else {
				ctx.bindingByCol[col] = binding
			}
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
	case tree.JOIN_TYPE_CROSS_L2:
		joinType = plan.Node_L2
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
	ctx.views = append(ctx.views, leftCtx.views...)

	if _, ok := tbl.Right.(*tree.TableFunction); ok {
		return 0, moerr.NewSyntaxError(builder.GetContext(), "Every table function must have an alias")
	}
	rightChildID, err := builder.buildTable(tbl.Right, rightCtx, leftChildID, leftCtx)
	if err != nil {
		return 0, err
	}
	ctx.views = append(ctx.views, rightCtx.views...)

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
		joinConds, err := splitAndBindCondition(cond.Expr, NoAlias, ctx)
		if err != nil {
			return 0, err
		}
		node.OnList = joinConds

	case *tree.UsingJoinCond:
		if tbl.JoinType == tree.JOIN_TYPE_CROSS_L2 {
			for _, col := range cond.Cols {
				expr, err := ctx.addUsingColForCrossL2(string(col), joinType, leftCtx, rightCtx)
				if err != nil {
					return 0, err
				}
				node.OnList = append(node.OnList, expr)
			}
		} else {
			for _, col := range cond.Cols {
				expr, err := ctx.addUsingCol(string(col), joinType, leftCtx, rightCtx)
				if err != nil {
					return 0, err
				}
				node.OnList = append(node.OnList, expr)
			}
		}
	default:
		if tbl.JoinType == tree.JOIN_TYPE_NATURAL || tbl.JoinType == tree.JOIN_TYPE_NATURAL_LEFT || tbl.JoinType == tree.JOIN_TYPE_NATURAL_RIGHT {
			leftCols := make(map[string]bool)
			for _, binding := range leftCtx.bindings {
				for i, col := range binding.cols {
					if binding.colIsHidden[i] {
						continue
					}
					leftCols[col] = true
				}
			}

			var usingCols []string
			for _, binding := range rightCtx.bindings {
				for _, col := range binding.cols {
					if leftCols[col] {
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
	case "processlist", "mo_sessions":
		nodeId, err = builder.buildProcesslist(tbl, ctx, exprs, childId)
	case "mo_configurations":
		nodeId, err = builder.buildMoConfigurations(tbl, ctx, exprs, childId)
	case "mo_locks":
		nodeId, err = builder.buildMoLocks(tbl, ctx, exprs, childId)
	case "mo_transactions":
		nodeId, err = builder.buildMoTransactions(tbl, ctx, exprs, childId)
	case "mo_cache":
		nodeId, err = builder.buildMoCache(tbl, ctx, exprs, childId)
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
	case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_SOURCE_SCAN:
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

func (builder *QueryBuilder) resolveTsHint(tsExpr *tree.AtTimeStamp) (snapshot *Snapshot, err error) {
	if tsExpr == nil {
		return
	}

	conds := splitAstConjunction(tsExpr.Expr)
	if len(conds) != 1 {
		err = moerr.NewParseError(builder.GetContext(), "invalid timestamp hint")
		return
	}

	binder := NewDefaultBinder(builder.GetContext(), nil, nil, plan.Type{}, nil)
	binder.builder = builder
	defExpr, err := binder.BindExpr(conds[0], 0, false)
	if err != nil {
		return
	}
	exprLit, ok := defExpr.Expr.(*plan.Expr_Lit)
	if !ok {
		err = moerr.NewParseError(builder.GetContext(), "invalid timestamp hint")
		return
	}

	var tenant *SnapshotTenant
	if bgSnapshot := builder.compCtx.GetSnapshot(); IsSnapshotValid(bgSnapshot) {
		tenant = &SnapshotTenant{
			TenantName: bgSnapshot.Tenant.TenantName,
			TenantID:   bgSnapshot.Tenant.TenantID,
		}
	}

	switch lit := exprLit.Lit.Value.(type) {
	case *plan.Literal_Sval:
		if tsExpr.Type == tree.ATTIMESTAMPTIME {
			var ts time.Time
			if ts, err = time.Parse("2006-01-02 15:04:05.999999999", lit.Sval); err != nil {
				return
			}

			tsNano := ts.UTC().UnixNano()
			if tsNano <= 0 {
				err = moerr.NewInvalidArg(builder.GetContext(), "invalid timestamp value", lit.Sval)
				return
			}

			if time.Now().UTC().UnixNano()-tsNano <= options.DefaultGCTTL.Nanoseconds() && 0 <= time.Now().UTC().UnixNano()-tsNano {
				snapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: tsNano}, Tenant: tenant}
			} else {
				var valid bool
				if valid, err = builder.compCtx.CheckTimeStampValid(tsNano); err != nil {
					return
				}

				if !valid {
					err = moerr.NewInvalidArg(builder.GetContext(), "invalid timestamp value, no corresponding snapshot ", lit.Sval)
					return
				}

				snapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: tsNano}, Tenant: tenant}
			}
		} else if tsExpr.Type == tree.ATTIMESTAMPSNAPSHOT {
			return builder.compCtx.ResolveSnapshotWithSnapshotName(lit.Sval)
		} else if tsExpr.Type == tree.ATMOTIMESTAMP {
			var ts timestamp.Timestamp
			if ts, err = timestamp.ParseTimestamp(lit.Sval); err != nil {
				return
			}

			snapshot = &Snapshot{TS: &ts, Tenant: tenant}
		} else {
			err = moerr.NewInvalidArg(builder.GetContext(), "invalid timestamp hint type", tsExpr.Type.String())
			return
		}
	case *plan.Literal_I64Val:
		if tsExpr.Type == tree.ATTIMESTAMPTIME {
			if lit.I64Val <= 0 {
				err = moerr.NewInvalidArg(builder.GetContext(), "invalid timestamp value", lit.I64Val)
				return
			}

			snapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: lit.I64Val}, Tenant: tenant}
		} else {
			err = moerr.NewInvalidArg(builder.GetContext(), "invalid timestamp hint for snapshot hint", lit.I64Val)
			return
		}
	default:
		err = moerr.NewInvalidArg(builder.GetContext(), "invalid input expr ", tsExpr.Expr.String())
	}

	return
}

func IsSnapshotValid(snapshot *Snapshot) bool {
	if snapshot == nil {
		return false
	}

	if snapshot.TS == nil || snapshot.TS.Equal(timestamp.Timestamp{PhysicalTime: 0, LogicalTime: 0}) {
		return false
	}

	return true
}
