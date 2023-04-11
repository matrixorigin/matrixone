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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildInsertPlans(builder *QueryBuilder, bindCtx *BindContext, objRef *ObjectRef, tableDef *TableDef) error {
	// if table have parent table. add left join to query & append filter node after query
	if len(tableDef.Fkeys) > 0 {
		//
	}

	for _, indexdef := range tableDef.Indexes {
		if indexdef.Unique {
			idxRef := &plan.ObjectRef{
				SchemaName: builder.compCtx.DefaultDatabase(),
				ObjName:    indexdef.IndexTableName,
			}
			//把
		}
	}

	return nil
}

func buildOnDuplicateKeyPlans(builder *QueryBuilder, bindCtx *BindContext, info *dmlSelectInfo) error {
	return nil
}

func buildDeletePlans(builder *QueryBuilder, bindCtx *BindContext, info *dmlSelectInfo) error {

	return nil
}

func appendJoinNodeForParentFkCheck(builder *QueryBuilder, bindCtx *BindContext, info *dmlSelectInfo, tableDef *TableDef) error {
	parentIdx := make(map[string]int32)
	for _, fk := range tableDef.Fkeys {
		// in update statement. only add left join logic when update the column in foreign key
		if info.typ == "update" {
			updateRefColumn := false
			for _, colId := range fk.Cols {
				updateName := id2name[colId]
				if _, ok := info.tblInfo.updateKeys[rewriteIdx][updateName]; ok {
					updateRefColumn = true
					break
				}
			}
			if !updateRefColumn {
				continue
			}
		}

		// insert statement, we will alsways check parent ref
		for _, colId := range fk.Cols {
			updateName := id2name[colId]
			parentIdx[updateName] = info.idx
		}

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
			return err
		}
		rightTag := builder.qry.Nodes[rightId].BindingTags[0]
		baseNodeTag := builder.qry.Nodes[baseNodeId].BindingTags[0]
		// needRecursionCall := false

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
								ColPos: int32(newColPosMap[childColumnName]),
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
						return err
					}
					joinConds[i] = condExpr
					break
				}
			}
		}

		// append project
		info.projectList = append(info.projectList, &plan.Expr{
			Typ: parentTypMap[catalog.Row_ID],
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: rightTag,
					ColPos: parentPosMap[catalog.Row_ID],
				},
			},
		})
		info.idx = info.idx + 1

		// append join node
		leftCtx := builder.ctxByNode[info.rootId]
		err = joinCtx.mergeContexts(builder.GetContext(), leftCtx, rightCtx)
		if err != nil {
			return err
		}
		newRootId := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{info.rootId, rightId},
			JoinType: plan.Node_LEFT,
			OnList:   joinConds,
		}, joinCtx)
		bindCtx.binder = NewTableBinder(builder, bindCtx)
		info.rootId = newRootId
	}

	info.parentIdx = append(info.parentIdx, parentIdx)
}

// makeInsertPlan  build insert plan for one table
// sink_scan -> preinsert -> sink
//
//	-> sink_scan -> lock -> filter -> insert  //do insert
//	-> sink_scan -> group_by -> filter  //check pk is unique in rows
//	-> sink_scan -> join -> filter	// check pk is uniue in rows & snapshot
func makeInsertPlan(builder *QueryBuilder, bindCtx *BindContext, idx []int32, pkIdx int32, objRef *ObjectRef, sourceStep int32) error {
	// make plan:sink_scan -> preinsert -> sink
	{
		sinkScanNode := &Node{
			NodeType:    plan.Node_SINK_SCAN,
			CurrentStep: sourceStep + 1,
			SourceStep:  sourceStep,
		}
		lastNodeId := builder.appendNode(sinkScanNode, bindCtx)

		preInsertNode := &Node{
			NodeType: plan.Node_PRE_INSERT,
			Children: []int32{lastNodeId},
			PreInsertCtx: &plan.PreInsertCtx{
				Idx:   idx,
				PkIdx: pkIdx,
			},
		}
		lastNodeId = builder.appendNode(preInsertNode, bindCtx)

		sinkNode := &Node{
			NodeType: plan.Node_SINK,
			Children: []int32{lastNodeId},
		}
		lastNodeId = builder.appendNode(sinkNode, bindCtx)
		sourceStep = builder.appendStep(lastNodeId)
	}

	// make plan : sink_scan -> lock -> filter -> insert  //do insert
	{
		sinkScanNode := &Node{
			NodeType:    plan.Node_SINK_SCAN,
			CurrentStep: sourceStep + 1,
			SourceStep:  sourceStep,
		}
		lastNodeId := builder.appendNode(sinkScanNode, bindCtx)

		// todo: append lock & filter nodes

		insertNode := &Node{
			NodeType: plan.Node_INSERT,
			ObjRef:   objRef,
			Children: []int32{lastNodeId},
		}
		lastNodeId = builder.appendNode(insertNode, bindCtx)
		builder.appendStep(lastNodeId)
	}

	// todo: make plan: -> sink_scan -> group_by -> filter  //check pk is unique in rows
	// todo: make plan: -> sink_scan -> join -> filter	// check pk is uniue in rows & snapshot

	return nil
}

// makeDeletePlan  build delete plan for one table
// sink_scan -> predelete -> lock -> filter -> delete
// idx : indexes of row_id & pk column in the batch from last Node of the pre step
func makeDeletePlan(builder *QueryBuilder, bindCtx *BindContext, idx []int32, objRef *ObjectRef, sourceStep int32) error {
	currentStep := sourceStep + 1
	sinkScanNode := &Node{
		NodeType:    plan.Node_SINK_SCAN,
		CurrentStep: currentStep,
		SourceStep:  sourceStep,
	}
	lastNodeId := builder.appendNode(sinkScanNode, bindCtx)

	preDeleteNode := &Node{
		NodeType: plan.Node_PRE_DELETE,
		Children: []int32{lastNodeId},
		PreDeleteCtx: &plan.PreDeleteCtx{
			Idx: idx,
		},
	}
	lastNodeId = builder.appendNode(preDeleteNode, bindCtx)

	//todo append lock & filter

	deleteNode := &Node{
		NodeType: plan.Node_DELETE,
		Children: []int32{lastNodeId},
		ObjRef:   objRef,
	}
	lastNodeId = builder.appendNode(deleteNode, bindCtx)

	builder.appendStep(lastNodeId)

	return nil
}
