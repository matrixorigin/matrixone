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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func buildInsertPlans(
	builder *QueryBuilder, bindCtx *BindContext,
	objRef *ObjectRef, tableDef *TableDef,
	ctx CompilerContext,
	lastNodeId int32) (*Query, error) {

	// add plan: -> preinsert -> sink
	// for normal insert. the idx are index of tableDef.Cols
	idx := make([]int32, len(tableDef.Cols))
	for i := range tableDef.Cols {
		idx[i] = int32(i)
	}
	sourceStep, err := makePreInsertPlan(builder, bindCtx, idx, lastNodeId, tableDef)
	if err != nil {
		return nil, err
	}

	//make insert plans
	err = makeInsertPlan(ctx, builder, bindCtx, objRef, tableDef, sourceStep)
	if err != nil {
		return nil, err
	}

	query, err := builder.createQuery()
	return query, err
}

func buildOnDuplicateKeyPlans(builder *QueryBuilder, bindCtx *BindContext, info *dmlSelectInfo) (*Query, error) {

	query, err := builder.createQuery()
	return query, err
}

// makePreInsertPlan  build preinsert plan.
// sink_scan -> join[unique key] -> predelete[build partition] -> [u1]lock -> [u1]filter -> [u1]delete -> [o1]lock -> [o1]filter -> [o1]delete
func buildDeletePlans(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, tableDef *TableDef, beginIdx int, sourceStep int32, currentStep int32) (int32, error) {
	var err error
	sinkScanTag := builder.genNewTag()
	sinkScanProjection := make([]*Expr, len(tableDef.Cols))
	for i, col := range tableDef.Cols {
		sinkScanProjection[i] = &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: sinkScanTag,
					ColPos: int32(beginIdx + i),
					Name:   col.Name,
				},
			},
		}
	}
	sinkScanNode := &Node{
		NodeType:    plan.Node_SINK_SCAN,
		CurrentStep: currentStep,
		BindingTags: []int32{sinkScanTag},
		ProjectList: sinkScanProjection,
		SourceStep:  sourceStep,
	}
	lastNodeId := builder.appendNode(sinkScanNode, bindCtx)

	// append join node to get unique key table's row_id/pk for delete
	lastNodeId, err = appendJoinNodeForGetRowIdOfUniqueKey(builder, bindCtx, tableDef, lastNodeId)
	if err != nil {
		return -1, err
	}

	// delete unique table
	beginIdx := len(tableDef.Cols)
	for i, indexdef := range tableDef.Indexes {
		if indexdef.Unique {
			_, idxTableDef := ctx.Resolve(builder.compCtx.DefaultDatabase(), indexdef.IndexTableName)
			lastNodeId, err = makeOneDeletePlan(builder, bindCtx, idxTableDef, beginIdx, lastNodeId)
			if err != nil {
				return -1, err
			}
			beginIdx = beginIdx + 2 // row_id & pk
		}
	}

	// delete origin table

	return currentStep, nil
}

// makeOneDeletePlan
// predelete[build partition] -> lock -> filter -> delete
func makeOneDeletePlan(builder *QueryBuilder, bindCtx *BindContext, tableDef *TableDef, beginIdx int, lastNodeId int32) (int32, error) {
	// todo: append predelete node to build partition id

	// todo: append lock & filter

	// append delete node
	deleteTag := builder.genNewTag()
	deleteNode := &Node{
		NodeType:    plan.Node_DELETE,
		BindingTags: []int32{deleteTag},
		Children:    []int32{lastNodeId},
		//deleteCtx  store index of row_id
	}
	lastNodeId = builder.appendNode(deleteNode, bindCtx)

	return lastNodeId, nil
}

// makePreInsertPlan  build preinsert plan.
// xx -> preinsert -> sink
func makePreInsertPlan(builder *QueryBuilder, bindCtx *BindContext, idx []int32, lastNodeId int32, tableDef *TableDef) (int32, error) {
	preTag := builder.qry.Nodes[lastNodeId].BindingTags[0]
	preInsertTag := builder.genNewTag()
	preInsertProjection := getProjectionByTableDef(tableDef, preTag, preInsertTag)
	sinkProjection := getProjectionByPreProjection(preInsertProjection, preInsertTag)

	preInsertNode := &Node{
		NodeType:    plan.Node_PRE_INSERT,
		Children:    []int32{lastNodeId},
		BindingTags: []int32{preInsertTag},
		PreInsertCtx: &plan.PreInsertCtx{
			Idx: idx,
		},
		ProjectList: preInsertProjection,
	}
	lastNodeId = builder.appendNode(preInsertNode, bindCtx)

	sinkTag := builder.genNewTag()
	sinkNode := &Node{
		NodeType:    plan.Node_SINK,
		Children:    []int32{lastNodeId},
		BindingTags: []int32{sinkTag},
		ProjectList: sinkProjection,
	}
	lastNodeId = builder.appendNode(sinkNode, bindCtx)
	sourceStep := builder.appendStep(lastNodeId)

	return sourceStep, nil
}

// makePreInsertUkPlan  build preinsert plan.
// sink_scan -> preinsert_uk -> sink
func makePreInsertUkPlan(builder *QueryBuilder, bindCtx *BindContext, tableDef *TableDef, sourceStep int32, indexIdx int) (int32, error) {
	sinkScanTag := builder.genNewTag()
	sinkScanProjection := getProjectionByTableDef(tableDef, sinkScanTag, sinkScanTag)
	sinkScanNode := &Node{
		NodeType:    plan.Node_SINK_SCAN,
		CurrentStep: sourceStep + 1,
		BindingTags: []int32{sinkScanTag},
		ProjectList: sinkScanProjection,
		SourceStep:  sourceStep,
	}
	lastNodeId := builder.appendNode(sinkScanNode, bindCtx)

	preInsertUkTag := builder.genNewTag()
	var sinkProjection []*Expr
	var preInsertUkProjection []*Expr
	var useColumns []*Expr
	var pkColumn *Expr
	idxDef := tableDef.Indexes[indexIdx]
	partsMap := make(map[string]struct{})
	for _, part := range idxDef.Parts {
		partsMap[part] = struct{}{}
	}
	for i, col := range tableDef.Cols {
		if _, ok := partsMap[col.Name]; ok {
			useColumns = append(useColumns, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: sinkScanTag,
						ColPos: int32(i),
						Name:   col.Name,
					},
				},
			})
			preInsertUkProjection = append(preInsertUkProjection, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: preInsertUkTag,
						ColPos: int32(i),
						Name:   col.Name,
					},
				},
			})
		}
	}

	pkPos := getPkPos(tableDef)
	// have pk
	if pkPos > -1 {
		lastIdx := int32(len(preInsertUkProjection))
		col := tableDef.Cols[pkPos]
		pkColumn = &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: sinkScanTag,
					ColPos: lastIdx,
					Name:   col.Name,
				},
			},
		}
		preInsertUkProjection = append(preInsertUkProjection, &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: preInsertUkTag,
					ColPos: lastIdx,
					Name:   col.Name,
				},
			},
		})
	}
	preInsertUkNode := &Node{
		NodeType:    plan.Node_PRE_INSERT_UK,
		Children:    []int32{lastNodeId},
		BindingTags: []int32{preInsertUkTag},
		PreInsertUkCtx: &plan.PreInsertUkCtx{
			Columns:  useColumns,
			PkColumn: pkColumn,
		},
		ProjectList: preInsertUkProjection,
	}
	lastNodeId = builder.appendNode(preInsertUkNode, bindCtx)

	sinkTag := builder.genNewTag()
	sinkNode := &Node{
		NodeType:    plan.Node_SINK,
		Children:    []int32{lastNodeId},
		BindingTags: []int32{sinkTag},
		ProjectList: sinkProjection,
	}
	lastNodeId = builder.appendNode(sinkNode, bindCtx)
	sourceStep = builder.appendStep(lastNodeId)

	return sourceStep, nil
}

// makeInsertPlan  build insert plan for one table
/**
[o]sink_scan -> lock -> filter -> [project(if update)] -> join(check fk) -> filter -> insert -> sink  // insert origin table
			[u1]sink_scan->preinsert_uk->sink
				[u1]sink_scan -> lock -> filter -> insert
				[u1]sink_scan -> group_by -> filter  //check if pk is unique in rows
				[u1]sink_scan -> join -> filter	// check if pk is unique in rows & snapshot
			[u2]sink_scan->preinsert_uk[根据计算过的0，4，生成一个新的batch]->sink
				[u2]sink_scan -> lock -> filter -> insert
				[u2]sink_scan -> group_by -> filter  //check if pk is unique in rows
				[u2]sink_scan -> join -> filter	// check if pk is unique in rows & snapshot
[o]sink_scan -> group_by -> filter  //check if pk is unique in rows
[o]sink_scan -> join -> filter	// check if pk is unique in rows & snapshot
*/
func makeInsertPlan(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, objRef *ObjectRef, tableDef *TableDef, sourceStep int32) error {
	// make plan : sink_scan -> lock -> filter -> [project(if update)] -> join(check fk) -> filter -> insert
	var lastNodeId int32
	var err error
	{
		sinkScanTag := builder.genNewTag()
		sinkScanProjection := getProjectionByTableDef(tableDef, sinkScanTag, sinkScanTag)
		sinkScanNode := &Node{
			NodeType:    plan.Node_SINK_SCAN,
			CurrentStep: sourceStep + 1,
			SourceStep:  sourceStep,
			BindingTags: []int32{sinkScanTag},
			ProjectList: sinkScanProjection,
		}
		lastNodeId = builder.appendNode(sinkScanNode, bindCtx)

		// todo: append lock & filter nodes

		// if table have fk. then append join node & filter node
		if len(tableDef.Fkeys) > 0 {
			lastNodeId, err = appendJoinNodeForParentFkCheck(builder, bindCtx, tableDef, lastNodeId)
			if err != nil {
				return err
			}
			lastNode := builder.qry.Nodes[lastNodeId]
			lastTag := builder.qry.Nodes[lastNodeId].BindingTags[0]
			beginIdx := len(lastNode.ProjectList) - len(tableDef.Fkeys)

			//get filter exprs
			rowIdTyp := types.T_Rowid.ToType()
			filters := make([]*Expr, len(tableDef.Fkeys))
			errExpr := makePlan2StringConstExprWithType("Cannot add or update a child row: a foreign key constraint fails")
			for i := range tableDef.Fkeys {
				colExpr := &plan.Expr{
					Typ: makePlan2Type(&rowIdTyp),
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: lastTag,
							ColPos: int32(beginIdx + i),
							Name:   catalog.Row_ID,
						},
					},
				}
				nullCheckExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "isnull", []*Expr{colExpr})
				if err != nil {
					return err
				}
				filterExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "assert", []*Expr{nullCheckExpr, errExpr})
				if err != nil {
					return err
				}
				filters[i] = filterExpr
			}

			// append filter node
			filterNode := &Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{lastNodeId},
				FilterList: filters,
			}
			lastNodeId = builder.appendNode(filterNode, bindCtx)
		}

		insertNode := &Node{
			NodeType: plan.Node_INSERT,
			ObjRef:   objRef,
			Children: []int32{lastNodeId},
		}
		lastNodeId = builder.appendNode(insertNode, bindCtx)
		builder.appendStep(lastNodeId)
	}

	// todo: make plan: sink_scan -> group_by -> filter  //check if pk is unique in rows

	// todo: make plan: sink_scan -> join -> filter	// check if pk is unique in rows & snapshot

	// append plan for the hidden tables of unique keys
	for i, indexdef := range tableDef.Indexes {
		if indexdef.Unique {
			idxRef, idxTableDef := ctx.Resolve(builder.compCtx.DefaultDatabase(), indexdef.IndexTableName)
			//make preinsert_uk plan,  to build the batch to insert
			newSourceStep, err := makePreInsertUkPlan(builder, bindCtx, tableDef, sourceStep, i)
			if err != nil {
				return err
			}

			//make insert plans(for unique key table)
			err = makeInsertPlan(ctx, builder, bindCtx, idxRef, idxTableDef, newSourceStep)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func appendJoinNodeForParentFkCheck(builder *QueryBuilder, bindCtx *BindContext, tableDef *TableDef, baseNodeId int32) (int32, error) {
	typMap := make(map[string]*plan.Type)
	id2name := make(map[uint64]string)
	name2pos := make(map[string]int)
	for i, col := range tableDef.Cols {
		typMap[col.Name] = col.Typ
		id2name[col.ColId] = col.Name
		name2pos[col.Name] = i
	}

	baseNode := builder.qry.Nodes[baseNodeId]
	baseNodeTag := baseNode.BindingTags[0]
	lastNodeId := baseNodeId
	projectList := getProjectionByPreProjection(baseNode.ProjectList, baseNodeTag)

	for _, fk := range tableDef.Fkeys {
		_, parentTableDef := builder.compCtx.ResolveById(fk.ForeignTbl)
		parentPosMap := make(map[string]int32)
		parentTypMap := make(map[string]*plan.Type)
		parentId2name := make(map[uint64]string)
		for idx, col := range parentTableDef.Cols {
			parentPosMap[col.Name] = int32(idx)
			parentTypMap[col.Name] = col.Typ
			parentId2name[col.ColId] = col.Name
		}

		// append table scan node
		joinCtx := NewBindContext(builder, bindCtx)
		rightCtx := NewBindContext(builder, joinCtx)
		astTblName := tree.NewTableName(tree.Identifier(parentTableDef.Name), tree.ObjectNamePrefix{})
		rightId, err := builder.buildTable(astTblName, rightCtx, -1, nil)
		if err != nil {
			return -1, err
		}
		rightTag := builder.qry.Nodes[rightId].BindingTags[0]

		// build join conds
		joinConds := make([]*Expr, len(fk.Cols))
		for i, colId := range fk.ForeignCols {
			for _, col := range parentTableDef.Cols {
				if col.ColId == colId {
					parentColumnName := col.Name
					childColumnName := id2name[fk.Cols[i]]

					leftExpr := &Expr{
						Typ: typMap[childColumnName],
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: baseNodeTag,
								ColPos: int32(name2pos[childColumnName]),
							},
						},
					}
					rightExpr := &plan.Expr{
						Typ: parentTypMap[parentColumnName],
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: rightTag,
								ColPos: parentPosMap[parentColumnName],
							},
						},
					}
					condExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{leftExpr, rightExpr})
					if err != nil {
						return -1, err
					}
					joinConds[i] = condExpr
					break
				}
			}
		}

		// append project
		projectList = append(projectList, &Expr{
			Typ: parentTypMap[catalog.Row_ID],
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: rightTag,
					ColPos: parentPosMap[catalog.Row_ID],
					Name:   catalog.Row_ID,
				},
			},
		})

		// append join node
		leftCtx := builder.ctxByNode[lastNodeId]
		err = joinCtx.mergeContexts(builder.GetContext(), leftCtx, rightCtx)
		if err != nil {
			return -1, err
		}
		lastNodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{lastNodeId, rightId},
			JoinType: plan.Node_LEFT,
			OnList:   joinConds,
		}, joinCtx)
		bindCtx.binder = NewTableBinder(builder, bindCtx)
	}

	// append projection node. to make sure we can get the columns
	projectCtx := NewBindContext(builder, bindCtx)
	lastTag := builder.genNewTag()
	lastNodeId = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: projectList,
		Children:    []int32{lastNodeId},
		BindingTags: []int32{lastTag},
	}, projectCtx)

	return lastNodeId, nil
}

func getPkPos(tableDef *TableDef) int {
	if tableDef.Pkey == nil {
		return -1
	}
	pkName := tableDef.Pkey.PkeyColName
	if pkName == catalog.CPrimaryKeyColName {
		return len(tableDef.Cols)
	}
	for i, col := range tableDef.Cols {
		if col.Name == pkName {
			return i
		}
	}
	return -1
}

func getProjectionByPreProjection(preNodeProjection []*Expr, tag int32) []*Expr {
	projection := make([]*Expr, len(preNodeProjection))
	for i, expr := range preNodeProjection {
		name := ""
		if col, ok := expr.Expr.(*plan.Expr_Col); ok {
			name = col.Col.Name
		}
		projection[i] = &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tag,
					ColPos: int32(i),
					Name:   name,
				},
			},
		}
	}
	return projection
}

func getProjectionByTableDef(tableDef *TableDef, preTag int32, nowTag int32) []*Expr {
	var projection []*Expr
	for i, col := range tableDef.Cols {
		projection = append(projection, &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: preTag,
					ColPos: int32(i),
					Name:   col.Name,
				},
			},
		})
	}

	lastIdx := int32(len(tableDef.Cols))
	if tableDef.Pkey != nil && tableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		projection = append(projection, &plan.Expr{
			Typ: makeHiddenColTyp(),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: nowTag,
					ColPos: lastIdx,
					Name:   catalog.CPrimaryKeyColName,
				},
			},
		})
	} else if tableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
		projection = append(projection, &plan.Expr{
			Typ: makeHiddenColTyp(),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: nowTag,
					ColPos: lastIdx,
					Name:   tableDef.ClusterBy.Name,
				},
			},
		})
	}
	return projection
}

func appendJoinNodeForGetRowIdOfUniqueKey(builder *QueryBuilder, bindCtx *BindContext, tableDef *TableDef, baseNodeId int32) (int32, error) {
	var projectList []*Expr
	lastNodeId := baseNodeId
	typMap := make(map[string]*plan.Type)
	posMap := make(map[string]int)
	for idx, col := range tableDef.Cols {
		posMap[col.Name] = idx
		typMap[col.Name] = col.Typ
	}

	for _, indexdef := range tableDef.Indexes {
		if indexdef.Unique {
			// append table_scan node
			joinCtx := NewBindContext(builder, bindCtx)
			rightCtx := NewBindContext(builder, joinCtx)
			astTblName := tree.NewTableName(tree.Identifier(indexdef.IndexTableName), tree.ObjectNamePrefix{})
			rightId, err := builder.buildTable(astTblName, rightCtx, -1, nil)
			if err != nil {
				return -1, err
			}
			rightTag := builder.qry.Nodes[rightId].BindingTags[0]
			baseTag := builder.qry.Nodes[baseNodeId].BindingTags[0]
			rightTableDef := builder.qry.Nodes[rightId].TableDef

			var rightRowIdPos int32 = -1
			var rightIdxPos int32 = -1 //it's also a primary key.
			for colIdx, col := range rightTableDef.Cols {
				if col.Name == catalog.Row_ID {
					rightRowIdPos = int32(colIdx)
				} else if col.Name == catalog.IndexTableIndexColName {
					rightIdxPos = int32(colIdx)
				}
			}

			// append projection
			projectList = append(projectList, &plan.Expr{
				Typ: rightTableDef.Cols[rightRowIdPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: rightTag,
						ColPos: rightRowIdPos,
					},
				},
			})

			projectList = append(projectList, &plan.Expr{
				Typ: rightTableDef.Cols[rightIdxPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: rightTag,
						ColPos: rightIdxPos,
					},
				},
			})

			rightExpr := &plan.Expr{
				Typ: rightTableDef.Cols[rightIdxPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: rightTag,
						ColPos: rightIdxPos,
					},
				},
			}

			// append join node
			var joinConds []*Expr
			var leftExpr *Expr
			partsLength := len(indexdef.Parts)
			if partsLength == 1 {
				orginIndexColumnName := indexdef.Parts[0]
				typ := typMap[orginIndexColumnName]
				leftExpr = &Expr{
					Typ: typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: baseTag,
							ColPos: int32(posMap[orginIndexColumnName]),
						},
					},
				}
			} else {
				args := make([]*Expr, partsLength)
				for i, column := range indexdef.Parts {
					typ := typMap[column]
					args[i] = &plan.Expr{
						Typ: typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: baseTag,
								ColPos: int32(posMap[column]),
							},
						},
					}
				}
				leftExpr, err = bindFuncExprImplByPlanExpr(builder.GetContext(), "serial", args)
				if err != nil {
					return -1, err
				}
			}

			condExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{leftExpr, rightExpr})
			if err != nil {
				return -1, err
			}
			joinConds = []*Expr{condExpr}

			leftCtx := builder.ctxByNode[lastNodeId]
			err = joinCtx.mergeContexts(builder.GetContext(), leftCtx, rightCtx)
			if err != nil {
				return -1, err
			}
			lastNodeId = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: []int32{lastNodeId, rightId},
				JoinType: plan.Node_LEFT,
				OnList:   joinConds,
			}, joinCtx)
			bindCtx.binder = NewTableBinder(builder, bindCtx)
		}
	}
	return lastNodeId, nil
}
