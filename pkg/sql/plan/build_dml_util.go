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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"golang.org/x/exp/slices"
)

var CNPrimaryCheck = false

var dmlPlanCtxPool = sync.Pool{
	New: func() any {
		return &dmlPlanCtx{}
	},
}
var deleteNodeInfoPool = sync.Pool{
	New: func() any {
		return &deleteNodeInfo{}
	},
}

func getDmlPlanCtx() *dmlPlanCtx {
	ctx := dmlPlanCtxPool.Get().(*dmlPlanCtx)
	ctx.objRef = nil
	ctx.tableDef = nil
	ctx.beginIdx = 0
	ctx.sourceStep = 0
	ctx.isMulti = false
	ctx.updateColLength = 0
	ctx.rowIdPos = 0
	ctx.insertColPos = ctx.insertColPos[:0]
	ctx.updateColPosMap = nil
	ctx.allDelTableIDs = nil
	ctx.isFkRecursionCall = false
	ctx.lockTable = false
	ctx.checkInsertPkDup = false
	ctx.pkFilterExprs = ctx.pkFilterExprs[:0]
	ctx.updatePkCol = true
	return ctx
}

func putDmlPlanCtx(ctx *dmlPlanCtx) {
	dmlPlanCtxPool.Put(ctx)
}

func getDeleteNodeInfo() *deleteNodeInfo {
	info := deleteNodeInfoPool.Get().(*deleteNodeInfo)
	info.objRef = nil
	info.tableDef = nil
	info.IsClusterTable = false
	info.deleteIndex = 0
	info.partTableIDs = info.partTableIDs[:0]
	info.partTableNames = info.partTableNames[:0]
	info.partitionIdx = 0
	info.addAffectedRows = false
	info.pkPos = 0
	info.pkTyp = nil
	info.lockTable = false
	return info
}

func putDeleteNodeInfo(info *deleteNodeInfo) {
	deleteNodeInfoPool.Put(info)
}

type dmlPlanCtx struct {
	objRef            *ObjectRef
	tableDef          *TableDef
	beginIdx          int
	sourceStep        int32
	isMulti           bool
	needAggFilter     bool
	updateColLength   int
	rowIdPos          int
	insertColPos      []int
	updateColPosMap   map[string]int
	allDelTableIDs    map[uint64]struct{}
	isFkRecursionCall bool //if update plan was recursion called by parent table( ref foreign key), we do not check parent's foreign key contraint
	lockTable         bool //we need lock table in stmt: delete from tbl
	checkInsertPkDup  bool //if we need check for duplicate values in insert batch.  eg:insert into t values (1).  load data will not check
	updatePkCol       bool //if update stmt will update the primary key or one of pks
	pkFilterExprs     []*Expr
}

// information of deleteNode, which is about the deleted table
type deleteNodeInfo struct {
	objRef          *ObjectRef
	tableDef        *TableDef
	IsClusterTable  bool
	deleteIndex     int      // The array index position of the rowid column
	partTableIDs    []uint64 // Align array index with the partition number
	partTableNames  []string // Align array index with the partition number
	partitionIdx    int      // The array index position of the partition expression column
	addAffectedRows bool
	pkPos           int
	pkTyp           *plan.Type
	lockTable       bool
}

// buildInsertPlans  build insert plan.
func buildInsertPlans(
	ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext,
	objRef *ObjectRef, tableDef *TableDef, lastNodeId int32,
	checkInsertPkDup bool, pkFilterExpr []*Expr) error {

	// add plan: -> preinsert -> sink
	lastNodeId = appendPreInsertNode(builder, bindCtx, objRef, tableDef, lastNodeId, false)

	lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
	sourceStep := builder.appendStep(lastNodeId)

	// make insert plans
	insertBindCtx := NewBindContext(builder, nil)
	err := makeInsertPlan(ctx, builder, insertBindCtx, objRef, tableDef, 0, sourceStep, true, false, checkInsertPkDup, true, pkFilterExpr)
	return err
}

// buildUpdatePlans  build update plan.
func buildUpdatePlans(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, updatePlanCtx *dmlPlanCtx) error {
	var err error
	// sink_scan -> project -> [agg] -> [filter] -> sink
	lastNodeId := appendSinkScanNode(builder, bindCtx, updatePlanCtx.sourceStep)
	lastNodeId, err = makePreUpdateDeletePlan(ctx, builder, bindCtx, updatePlanCtx, lastNodeId)
	if err != nil {
		return err
	}
	lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
	nextSourceStep := builder.appendStep(lastNodeId)
	updatePlanCtx.sourceStep = nextSourceStep

	// build delete plans
	err = buildDeletePlans(ctx, builder, bindCtx, updatePlanCtx)
	if err != nil {
		return err
	}

	// sink_scan -> project -> preinsert -> sink
	lastNodeId = appendSinkScanNode(builder, bindCtx, updatePlanCtx.sourceStep)
	lastNode := builder.qry.Nodes[lastNodeId]
	newCols := make([]*ColDef, 0, len(updatePlanCtx.tableDef.Cols))
	oldRowIdPos := len(updatePlanCtx.tableDef.Cols) - 1
	for _, col := range updatePlanCtx.tableDef.Cols {
		if col.Hidden && col.Name != catalog.FakePrimaryKeyColName {
			continue
		}
		newCols = append(newCols, col)
	}
	updatePlanCtx.tableDef.Cols = newCols
	insertColLength := len(updatePlanCtx.insertColPos) + 1
	projectProjection := make([]*Expr, insertColLength)
	for i, idx := range updatePlanCtx.insertColPos {
		name := ""
		if col, ok := lastNode.ProjectList[idx].Expr.(*plan.Expr_Col); ok {
			name = col.Col.Name
		}
		projectProjection[i] = &plan.Expr{
			Typ: lastNode.ProjectList[idx].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					ColPos: int32(idx),
					Name:   name,
				},
			},
		}
	}
	projectProjection[insertColLength-1] = &plan.Expr{
		Typ: lastNode.ProjectList[oldRowIdPos].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: int32(oldRowIdPos),
				Name:   catalog.Row_ID,
			},
		},
	}

	//append project node
	projectNode := &Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeId},
		ProjectList: projectProjection,
	}
	lastNodeId = builder.appendNode(projectNode, bindCtx)
	//append preinsert node
	lastNodeId = appendPreInsertNode(builder, bindCtx, updatePlanCtx.objRef, updatePlanCtx.tableDef, lastNodeId, true)

	//append sink node
	lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
	sourceStep := builder.appendStep(lastNodeId)

	// build insert plan.
	insertBindCtx := NewBindContext(builder, nil)
	err = makeInsertPlan(ctx, builder, insertBindCtx, updatePlanCtx.objRef, updatePlanCtx.tableDef, updatePlanCtx.updateColLength,
		sourceStep, false, updatePlanCtx.isFkRecursionCall, updatePlanCtx.checkInsertPkDup, updatePlanCtx.updatePkCol, updatePlanCtx.pkFilterExprs)

	return err
}

func getStepByNodeId(builder *QueryBuilder, nodeId int32) int {
	for step, stepNodeId := range builder.qry.Steps {
		if stepNodeId == nodeId {
			return step
		}
	}
	return -1
}

// buildDeletePlans  build preinsert plan.
/*
[o1]sink_scan -> join[u1] -> sink
	[u1]sink_scan -> lock -> delete -> [mergedelete] ...  // if it's delete stmt. do delete u1
	[u1]sink_scan -> preinsert_uk -> sink ...  // if it's update stmt. do update u1
[o1]sink_scan -> join[u2] -> sink
	[u2]sink_scan -> lock -> delete -> [mergedelete] ...  // if it's delete stmt. do delete u2
	[u2]sink_scan -> preinsert_uk -> sink ...  // if it's update stmt. do update u2
[o1]sink_scan -> predelete[get partition] -> lock -> delete -> [mergedelete]

[o1]sink_scan -> join[f1 semi join c1 on c1.fid=f1.id, get f1.id] -> filter(assert(isempty(id)))   // if have refChild table with no action
[o1]sink_scan -> join[f1 inner join c2 on f1.id = c2.fid, 取c2.*, null] -> sink ...(like update)   // if have refChild table with set null
[o1]sink_scan -> join[f1 inner join c4 on f1.id = c4.fid, get c3.*] -> sink ...(like delete)   // delete stmt: if have refChild table with cascade
[o1]sink_scan -> join[f1 inner join c4 on f1.id = c4.fid, get c3.*, update cols] -> sink ...(like update)   // update stmt: if have refChild table with cascade
*/
func buildDeletePlans(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, delCtx *dmlPlanCtx) error {
	if sinkOrUnionNodeId, ok := builder.deleteNode[delCtx.tableDef.TblId]; ok {
		sinkOrUnionNode := builder.qry.Nodes[sinkOrUnionNodeId]
		if sinkOrUnionNode.NodeType == plan.Node_SINK {
			step := getStepByNodeId(builder, sinkOrUnionNodeId)
			if step == -1 || delCtx.sourceStep == -1 {
				panic("steps should not be -1")
			}

			oldDelPlanSinkScanNodeId := appendSinkScanNode(builder, bindCtx, int32(step))
			thisDelPlanSinkScanNodeId := appendSinkScanNode(builder, bindCtx, delCtx.sourceStep)
			unionProjection := getProjectionByLastNode(builder, sinkOrUnionNodeId)
			unionNode := &plan.Node{
				NodeType:    plan.Node_UNION,
				Children:    []int32{oldDelPlanSinkScanNodeId, thisDelPlanSinkScanNodeId},
				ProjectList: unionProjection,
			}
			unionNodeId := builder.appendNode(unionNode, bindCtx)
			newSinkNodeId := appendSinkNode(builder, bindCtx, unionNodeId)
			endStep := builder.appendStep(newSinkNodeId)
			for i, n := range builder.qry.Nodes {
				if n.NodeType == plan.Node_SINK_SCAN && n.SourceStep[0] == int32(step) && i != int(oldDelPlanSinkScanNodeId) {
					n.SourceStep[0] = endStep
				}
			}
			builder.deleteNode[delCtx.tableDef.TblId] = unionNodeId
		} else {
			// todo : we need make union operator to support more than two children.
			panic("unsuport more than two plans to delete one table")
			// thisDelPlanSinkScanNodeId := appendSinkScanNode(builder, bindCtx, delCtx.sourceStep)
			// sinkOrUnionNode.Children = append(sinkOrUnionNode.Children, thisDelPlanSinkScanNodeId)
		}
		return nil
	} else {
		builder.deleteNode[delCtx.tableDef.TblId] = builder.qry.Steps[delCtx.sourceStep]
	}
	isUpdate := delCtx.updateColLength > 0

	// delete unique table
	hasUniqueKey := haveUniqueKey(delCtx.tableDef)
	if hasUniqueKey {
		uniqueDeleteIdx := len(delCtx.tableDef.Cols) + delCtx.updateColLength
		typMap := make(map[string]*plan.Type)
		posMap := make(map[string]int)
		for idx, col := range delCtx.tableDef.Cols {
			posMap[col.Name] = idx
			typMap[col.Name] = col.Typ
		}
		for idx, indexdef := range delCtx.tableDef.Indexes {
			if indexdef.Unique {
				uniqueObjRef, uniqueTableDef := builder.compCtx.Resolve(delCtx.objRef.SchemaName, indexdef.IndexTableName)
				if uniqueTableDef == nil {
					return moerr.NewNoSuchTable(builder.GetContext(), delCtx.objRef.SchemaName, indexdef.IndexTableName)
				}

				lastNodeId := appendSinkScanNode(builder, bindCtx, delCtx.sourceStep)

				lastNodeId, err := appendDeleteUniqueTablePlan(builder, bindCtx, uniqueObjRef, uniqueTableDef,
					indexdef, typMap, posMap, lastNodeId, delCtx.updateColLength)
				if err != nil {
					return err
				}

				uniqueTblPkPos := uniqueDeleteIdx + 1
				uniqueTblPkTyp := uniqueTableDef.Cols[0].Typ
				if isUpdate {
					// do it like simple update
					lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
					newSourceStep := builder.appendStep(lastNodeId)
					// delete uk plan
					{
						//sink_scan -> lock -> delete
						lastNodeId = appendSinkScanNode(builder, bindCtx, newSourceStep)
						delNodeInfo := makeDeleteNodeInfo(builder.compCtx, uniqueObjRef, uniqueTableDef, uniqueDeleteIdx, -1, false, uniqueTblPkPos, uniqueTblPkTyp, delCtx.lockTable)
						lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo)
						putDeleteNodeInfo(delNodeInfo)
						if err != nil {
							return err
						}
						builder.appendStep(lastNodeId)
					}
					// insert uk plan
					{
						lastNodeId = appendSinkScanNode(builder, bindCtx, newSourceStep)
						lastProject := builder.qry.Nodes[lastNodeId].ProjectList
						projectProjection := make([]*Expr, len(delCtx.tableDef.Cols))
						for j, uCols := range delCtx.tableDef.Cols {
							if nIdx, ok := delCtx.updateColPosMap[uCols.Name]; ok {
								projectProjection[j] = lastProject[nIdx]
							} else {
								if uCols.Name == catalog.Row_ID {
									// replace the origin table's row_id with unique table's row_id
									projectProjection[j] = lastProject[len(lastProject)-2]
								} else {
									projectProjection[j] = lastProject[j]
								}
							}
						}
						projectNode := &Node{
							NodeType:    plan.Node_PROJECT,
							Children:    []int32{lastNodeId},
							ProjectList: projectProjection,
						}
						lastNodeId = builder.appendNode(projectNode, bindCtx)
						preUKStep, err := appendPreInsertUkPlan(builder, bindCtx, delCtx.tableDef, lastNodeId, idx, true, uniqueTableDef)
						if err != nil {
							return err
						}

						insertUniqueTableDef := DeepCopyTableDef(uniqueTableDef)
						for j, col := range insertUniqueTableDef.Cols {
							if col.Name == catalog.Row_ID {
								insertUniqueTableDef.Cols = append(insertUniqueTableDef.Cols[:j], insertUniqueTableDef.Cols[j+1:]...)
							}
						}
						err = makeInsertPlan(ctx, builder, bindCtx, uniqueObjRef, insertUniqueTableDef, 1, preUKStep, false, false, true, true, nil)
						if err != nil {
							return err
						}
					}
				} else {
					// it's more simple for delete hidden unique table .so we append nodes after the plan. not recursive call buildDeletePlans
					delNodeInfo := makeDeleteNodeInfo(builder.compCtx, uniqueObjRef, uniqueTableDef, uniqueDeleteIdx, -1, false, uniqueTblPkPos, uniqueTblPkTyp, delCtx.lockTable)
					lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo)
					putDeleteNodeInfo(delNodeInfo)
					if err != nil {
						return err
					}
					builder.appendStep(lastNodeId)
				}
			}
		}
	}

	// delete origin table
	lastNodeId := appendSinkScanNode(builder, bindCtx, delCtx.sourceStep)
	partExprIdx := -1
	if delCtx.tableDef.Partition != nil {
		partExprIdx = len(delCtx.tableDef.Cols) + delCtx.updateColLength
		lastNodeId = appendPreDeleteNode(builder, bindCtx, delCtx.objRef, delCtx.tableDef, lastNodeId)
	}
	pkPos, pkTyp := getPkPos(delCtx.tableDef, false)
	delNodeInfo := makeDeleteNodeInfo(ctx, delCtx.objRef, delCtx.tableDef, delCtx.rowIdPos, partExprIdx, true, pkPos, pkTyp, delCtx.lockTable)
	lastNodeId, err := makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo)
	putDeleteNodeInfo(delNodeInfo)
	if err != nil {
		return err
	}
	builder.appendStep(lastNodeId)

	// if some table references to this table
	if len(delCtx.tableDef.RefChildTbls) > 0 {
		nameTypMap := make(map[string]*plan.Type)
		idNameMap := make(map[uint64]string)
		nameIdxMap := make(map[string]int32)
		for idx, col := range delCtx.tableDef.Cols {
			nameTypMap[col.Name] = col.Typ
			idNameMap[col.ColId] = col.Name
			nameIdxMap[col.Name] = int32(idx)
		}
		baseProject := getProjectionByLastNode(builder, lastNodeId)

		for _, tableId := range delCtx.tableDef.RefChildTbls {
			// stmt: delete p, c from child_tbl c join parent_tbl p on c.pid = p.id , skip
			if _, existInDelTable := delCtx.allDelTableIDs[tableId]; existInDelTable {
				continue
			}

			childObjRef, childTableDef := builder.compCtx.ResolveById(tableId)
			childPosMap := make(map[string]int32)
			childTypMap := make(map[string]*plan.Type)
			childId2name := make(map[uint64]string)
			childProjectList := make([]*Expr, len(childTableDef.Cols))
			childForJoinProject := make([]*Expr, len(childTableDef.Cols))
			childRowIdPos := -1
			for idx, col := range childTableDef.Cols {
				childPosMap[col.Name] = int32(idx)
				childTypMap[col.Name] = col.Typ
				childId2name[col.ColId] = col.Name
				childProjectList[idx] = &Expr{
					Typ: col.Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							ColPos: int32(idx),
							Name:   col.Name,
						},
					},
				}
				childForJoinProject[idx] = &Expr{
					Typ: col.Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 1,
							ColPos: int32(idx),
							Name:   col.Name,
						},
					},
				}
				if col.Name == catalog.Row_ID {
					childRowIdPos = idx
				}
			}

			for _, fk := range childTableDef.Fkeys {
				if fk.ForeignTbl == delCtx.tableDef.TblId {
					// update stmt: update the columns do not contains ref key, skip
					if isUpdate {
						updateRefColumn := false
						for _, colId := range fk.ForeignCols {
							updateName := idNameMap[colId]
							if _, ok := delCtx.updateColPosMap[updateName]; ok {
								updateRefColumn = true
								break
							}
						}
						if !updateRefColumn {
							continue
						}
					}

					// build join conds
					joinConds := make([]*Expr, len(fk.Cols))
					rightConds := make([]*Expr, len(fk.Cols))
					leftConds := make([]*Expr, len(fk.Cols))
					// use for join's projection & filter's condExpr
					var oneLeftCond *Expr
					var oneLeftCondName string
					updateChildColPosMap := make(map[string]int)
					updateChildColExpr := make([]*Expr, len(fk.Cols))         // use for update
					insertColPos := make([]int, 0, len(childTableDef.Cols)-1) // use for update
					childColLength := len(childTableDef.Cols)
					for i, colId := range fk.Cols {
						for _, col := range childTableDef.Cols {
							if col.ColId == colId {
								childColumnName := col.Name
								originColumnName := idNameMap[fk.ForeignCols[i]]

								leftExpr := &Expr{
									Typ: nameTypMap[originColumnName],
									Expr: &plan.Expr_Col{
										Col: &plan.ColRef{
											RelPos: 0,
											ColPos: int32(nameIdxMap[originColumnName]),
											Name:   originColumnName,
										},
									},
								}
								if pos, ok := delCtx.updateColPosMap[originColumnName]; ok {
									updateChildColExpr[i] = &Expr{
										Typ: baseProject[pos].Typ,
										Expr: &plan.Expr_Col{
											Col: &plan.ColRef{
												RelPos: 0,
												ColPos: int32(pos),
												Name:   originColumnName,
											},
										},
									}
								} else {
									updateChildColExpr[i] = leftExpr
								}
								rightExpr := &plan.Expr{
									Typ: childTypMap[childColumnName],
									Expr: &plan.Expr_Col{
										Col: &plan.ColRef{
											RelPos: 1,
											ColPos: int32(childPosMap[childColumnName]),
											Name:   childColumnName,
										},
									},
								}
								updateChildColPosMap[childColumnName] = childColLength + i
								condExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{leftExpr, rightExpr})
								if err != nil {
									return err
								}
								rightConds[i] = rightExpr
								leftConds[i] = leftExpr
								oneLeftCond = leftExpr
								oneLeftCondName = originColumnName
								joinConds[i] = condExpr

								break
							}
						}
					}

					for idx, col := range childTableDef.Cols {
						if col.Name != catalog.Row_ID {
							if pos, ok := updateChildColPosMap[col.Name]; ok {
								insertColPos = append(insertColPos, pos)
							} else {
								insertColPos = append(insertColPos, idx)
							}
						}
					}

					var refAction plan.ForeignKeyDef_RefAction
					if isUpdate {
						refAction = fk.OnUpdate
					} else {
						refAction = fk.OnDelete
					}

					lastNodeId = appendSinkScanNode(builder, bindCtx, delCtx.sourceStep)

					switch refAction {
					case plan.ForeignKeyDef_NO_ACTION, plan.ForeignKeyDef_RESTRICT, plan.ForeignKeyDef_SET_DEFAULT:
						// plan : sink_scan -> join(f1 semi join c1 & get f1's col) -> filter(assert(isempty(f1's col)))
						rightId := builder.appendNode(&plan.Node{
							NodeType:    plan.Node_TABLE_SCAN,
							Stats:       &plan.Stats{},
							ObjRef:      childObjRef,
							TableDef:    childTableDef,
							ProjectList: childProjectList,
						}, bindCtx)

						lastNodeId = builder.appendNode(&plan.Node{
							NodeType:    plan.Node_JOIN,
							Children:    []int32{lastNodeId, rightId},
							JoinType:    plan.Node_SEMI,
							OnList:      joinConds,
							ProjectList: []*Expr{oneLeftCond},
						}, bindCtx)

						colExpr := &Expr{
							Typ: oneLeftCond.Typ,
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									Name: oneLeftCondName,
								},
							},
						}
						errExpr := makePlan2StringConstExprWithType("Cannot delete or update a parent row: a foreign key constraint fails")
						isEmptyExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "isempty", []*Expr{colExpr})
						if err != nil {
							return err
						}
						assertExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "assert", []*Expr{isEmptyExpr, errExpr})
						if err != nil {
							return err
						}
						filterNode := &Node{
							NodeType:    plan.Node_FILTER,
							Children:    []int32{lastNodeId},
							FilterList:  []*Expr{assertExpr},
							ProjectList: getProjectionByLastNode(builder, lastNodeId),
							IsEnd:       true,
						}
						lastNodeId = builder.appendNode(filterNode, bindCtx)
						builder.appendStep(lastNodeId)

					case plan.ForeignKeyDef_SET_NULL:
						// plan : sink_scan -> join[f1 inner join c1 on f1.id = c1.fid, get c1.* & null] -> project -> sink   then + updatePlans
						rightId := builder.appendNode(&plan.Node{
							NodeType:    plan.Node_TABLE_SCAN,
							Stats:       &plan.Stats{},
							ObjRef:      childObjRef,
							TableDef:    DeepCopyTableDef(childTableDef),
							ProjectList: childProjectList,
						}, bindCtx)
						lastNodeId = builder.appendNode(&plan.Node{
							NodeType:    plan.Node_JOIN,
							Children:    []int32{lastNodeId, rightId},
							JoinType:    plan.Node_INNER,
							OnList:      joinConds,
							ProjectList: childForJoinProject,
						}, bindCtx)
						// inner join cannot dealwith null expr in projectList. so we append a project node
						projectProjection := getProjectionByLastNode(builder, lastNodeId)
						for _, e := range rightConds {
							projectProjection = append(projectProjection, &plan.Expr{
								Typ: e.Typ,
								Expr: &plan.Expr_C{
									C: &Const{
										Isnull: true,
									},
								},
							})
						}
						lastNodeId = builder.appendNode(&Node{
							NodeType:    plan.Node_PROJECT,
							Children:    []int32{lastNodeId},
							ProjectList: projectProjection,
						}, bindCtx)
						lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
						newSourceStep := builder.appendStep(lastNodeId)

						upPlanCtx := getDmlPlanCtx()
						upPlanCtx.objRef = childObjRef
						upPlanCtx.tableDef = childTableDef
						upPlanCtx.updateColLength = len(rightConds)
						upPlanCtx.isMulti = false
						upPlanCtx.rowIdPos = childRowIdPos
						upPlanCtx.sourceStep = newSourceStep
						upPlanCtx.beginIdx = 0
						upPlanCtx.updateColPosMap = updateChildColPosMap
						upPlanCtx.allDelTableIDs = map[uint64]struct{}{}
						upPlanCtx.insertColPos = insertColPos
						upPlanCtx.isFkRecursionCall = true

						err = buildUpdatePlans(ctx, builder, bindCtx, upPlanCtx)
						putDmlPlanCtx(upPlanCtx)
						if err != nil {
							return err
						}

					case plan.ForeignKeyDef_CASCADE:
						rightId := builder.appendNode(&plan.Node{
							NodeType:    plan.Node_TABLE_SCAN,
							Stats:       &plan.Stats{},
							ObjRef:      childObjRef,
							TableDef:    childTableDef,
							ProjectList: childProjectList,
						}, bindCtx)

						if isUpdate {
							// update stmt get plan : sink_scan -> join[f1 inner join c1 on f1.id = c1.fid, get c1.* & update cols] -> sink   then + updatePlans
							joinProjection := childForJoinProject
							joinProjection = append(joinProjection, updateChildColExpr...)
							lastNodeId = builder.appendNode(&plan.Node{
								NodeType:    plan.Node_JOIN,
								Children:    []int32{lastNodeId, rightId},
								JoinType:    plan.Node_INNER,
								OnList:      joinConds,
								ProjectList: joinProjection,
							}, bindCtx)
							lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
							newSourceStep := builder.appendStep(lastNodeId)

							upPlanCtx := getDmlPlanCtx()
							upPlanCtx.objRef = childObjRef
							upPlanCtx.tableDef = DeepCopyTableDef(childTableDef)
							upPlanCtx.updateColLength = len(rightConds)
							upPlanCtx.isMulti = false
							upPlanCtx.rowIdPos = childRowIdPos
							upPlanCtx.sourceStep = newSourceStep
							upPlanCtx.beginIdx = 0
							upPlanCtx.updateColPosMap = updateChildColPosMap
							upPlanCtx.insertColPos = insertColPos
							upPlanCtx.allDelTableIDs = map[uint64]struct{}{}
							upPlanCtx.isFkRecursionCall = true

							err = buildUpdatePlans(ctx, builder, bindCtx, upPlanCtx)
							putDmlPlanCtx(upPlanCtx)
							if err != nil {
								return err
							}
						} else {
							// delete stmt get plan : sink_scan -> join[f1 inner join c1 on f1.id = c1.fid, get c1.*] -> sink   then + deletePlans
							lastNodeId = builder.appendNode(&plan.Node{
								NodeType:    plan.Node_JOIN,
								Children:    []int32{lastNodeId, rightId},
								JoinType:    plan.Node_INNER,
								OnList:      joinConds,
								ProjectList: childForJoinProject,
							}, bindCtx)
							lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
							newSourceStep := builder.appendStep(lastNodeId)

							//make deletePlans
							allDelTableIDs := make(map[uint64]struct{})
							allDelTableIDs[childTableDef.TblId] = struct{}{}
							upPlanCtx := getDmlPlanCtx()
							upPlanCtx.objRef = childObjRef
							upPlanCtx.tableDef = childTableDef
							upPlanCtx.updateColLength = 0
							upPlanCtx.isMulti = false
							upPlanCtx.rowIdPos = childRowIdPos
							upPlanCtx.sourceStep = newSourceStep
							upPlanCtx.beginIdx = 0
							upPlanCtx.allDelTableIDs = allDelTableIDs

							err := buildDeletePlans(ctx, builder, bindCtx, upPlanCtx)
							putDmlPlanCtx(upPlanCtx)
							if err != nil {
								return err
							}
						}
					}

				}
			}
		}
	}

	return nil
}

// makeInsertPlan  build insert plan for one table
/**
[o]sink_scan -> lock(retry when err) -> insert
[u1]sink_scan -> preinsert_uk -> sink
	[u1]sink_scan -> lock -> insert
	[u1]sink_scan -> group_by -> filter      // check pk by insert rows
	[u1]sink_scan -> join（u1） -> filter      // check pk by rows in origin table
[u2]sink_scan -> preinsert_uk -> sink
	[u2]sink_scan -> lock -> insert
	[u2]sink_scan -> group_by -> filter      // check pk by insert rows
	[u2]sink_scan -> join（u2） -> filter      // check pk by rows in origin table
[o]sink_scan -> group_by -> filter      // check pk by insert rows
[o]sink_scan -> join（o） -> filter      // check pk by rows in origin table
[o]sink_scan -> join(join fk) -> filter  // check foreign key
*/
func makeInsertPlan(
	ctx CompilerContext,
	builder *QueryBuilder,
	bindCtx *BindContext,
	objRef *ObjectRef,
	tableDef *TableDef,
	updateColLength int,
	sourceStep int32,
	addAffectedRows bool,
	isFkRecursionCall bool,
	checkInsertPkDup bool,
	updatePkCol bool,
	pkFilterExprs []*Expr,
) error {
	var lastNodeId int32
	var err error

	// make plan : sink_scan -> lock -> insert
	{
		lastNodeId = appendSinkScanNode(builder, bindCtx, sourceStep)
		// Get table partition information
		paritionTableIds, paritionTableNames := getPartTableIdsAndNames(ctx, objRef, tableDef)
		partitionIdx := -1
		// append project node
		projectProjection := getProjectionByLastNode(builder, lastNodeId)
		if len(projectProjection) > len(tableDef.Cols) || tableDef.Partition != nil {
			if len(projectProjection) > len(tableDef.Cols) {
				projectProjection = projectProjection[:len(tableDef.Cols)]
			}
			partitionIdx = len(tableDef.Cols)
			if tableDef.Partition != nil {
				partitionExpr := DeepCopyExpr(tableDef.Partition.PartitionExpression)
				projectProjection = append(projectProjection, partitionExpr)
			}

			projectNode := &Node{
				NodeType:    plan.Node_PROJECT,
				Children:    []int32{lastNodeId},
				ProjectList: projectProjection,
			}
			lastNodeId = builder.appendNode(projectNode, bindCtx)
		}

		// append insert node
		insertProjection := getProjectionByLastNode(builder, lastNodeId)
		// in this case. insert columns in front of batch
		if len(insertProjection) > len(tableDef.Cols) {
			insertProjection = insertProjection[:len(tableDef.Cols)]
		}

		insertNode := &Node{
			NodeType: plan.Node_INSERT,
			Children: []int32{lastNodeId},
			ObjRef:   objRef,
			InsertCtx: &plan.InsertCtx{
				Ref:                 objRef,
				AddAffectedRows:     addAffectedRows,
				IsClusterTable:      tableDef.TableType == catalog.SystemClusterRel,
				TableDef:            tableDef,
				PartitionTableIds:   paritionTableIds,
				PartitionTableNames: paritionTableNames,
				PartitionIdx:        int32(partitionIdx),
			},
			ProjectList: insertProjection,
		}
		lastNodeId = builder.appendNode(insertNode, bindCtx)
		builder.appendStep(lastNodeId)
	}

	// append plan for the hidden tables of unique keys
	if updateColLength == 0 {
		for idx, indexdef := range tableDef.Indexes {
			if indexdef.Unique {
				idxRef, idxTableDef := ctx.Resolve(objRef.SchemaName, indexdef.IndexTableName)
				// remove row_id
				for i, col := range idxTableDef.Cols {
					if col.Name == catalog.Row_ID {
						idxTableDef.Cols = append(idxTableDef.Cols[:i], idxTableDef.Cols[i+1:]...)
						break
					}
				}

				lastNodeId = appendSinkScanNode(builder, bindCtx, sourceStep)
				newSourceStep, err := appendPreInsertUkPlan(builder, bindCtx, tableDef, lastNodeId, idx, false, idxTableDef)
				if err != nil {
					return err
				}

				err = makeInsertPlan(ctx, builder, bindCtx, idxRef, idxTableDef, 0, newSourceStep, false, false, checkInsertPkDup, true, nil)
				if err != nil {
					return err
				}
			}
		}
	}

	// if table have fk. then append join node & filter node
	// sink_scan -> join -> filter
	if !isFkRecursionCall && len(tableDef.Fkeys) > 0 {
		lastNodeId = appendSinkScanNode(builder, bindCtx, sourceStep)

		lastNodeId, err = appendJoinNodeForParentFkCheck(builder, bindCtx, tableDef, lastNodeId)
		if err != nil {
			return err
		}
		lastNode := builder.qry.Nodes[lastNodeId]
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
						ColPos: int32(beginIdx + i),
						Name:   catalog.Row_ID,
					},
				},
			}
			nullCheckExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "isnotnull", []*Expr{colExpr})
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
			NodeType:    plan.Node_FILTER,
			Children:    []int32{lastNodeId},
			FilterList:  filters,
			ProjectList: getProjectionByLastNode(builder, lastNodeId),
		}
		lastNodeId = builder.appendNode(filterNode, bindCtx)

		builder.appendStep(lastNodeId)
	}

	// make plan: sink_scan -> group_by -> filter  //check if pk is unique in rows
	if checkInsertPkDup {
		if pkPos, pkTyp := getPkPos(tableDef, true); pkPos != -1 {
			lastNodeId = appendSinkScanNode(builder, bindCtx, sourceStep)
			pkColExpr := &plan.Expr{
				Typ: pkTyp,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						ColPos: int32(pkPos),
						Name:   tableDef.Pkey.PkeyColName,
					},
				},
			}
			lastNodeId, err = appendAggCountGroupByColExpr(builder, bindCtx, lastNodeId, pkColExpr)
			if err != nil {
				return err
			}

			countType := types.T_int64.ToType()
			countColExpr := &plan.Expr{
				Typ: makePlan2Type(&countType),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						Name: tableDef.Pkey.PkeyColName,
					},
				},
			}

			eqCheckExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{MakePlan2Int64ConstExprWithType(1), countColExpr})
			if err != nil {
				return err
			}
			varcharType := types.T_varchar.ToType()
			varcharExpr, err := makePlan2CastExpr(builder.GetContext(), &Expr{
				Typ: tableDef.Cols[pkPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{ColPos: 1, Name: tableDef.Cols[pkPos].Name},
				},
			}, makePlan2Type(&varcharType))
			if err != nil {
				return err
			}
			filterExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "assert", []*Expr{eqCheckExpr, varcharExpr, makePlan2StringConstExprWithType(tableDef.Cols[pkPos].Name)})
			if err != nil {
				return err
			}
			filterNode := &Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{lastNodeId},
				FilterList: []*Expr{filterExpr},
				IsEnd:      true,
			}
			lastNodeId = builder.appendNode(filterNode, bindCtx)
			builder.appendStep(lastNodeId)
		}
	}

	// make plan: sink_scan -> join -> filter	// check if pk is unique in rows & snapshot
	if CNPrimaryCheck {
		if pkPos, pkTyp := getPkPos(tableDef, true); pkPos != -1 {
			isUpdate := updateColLength > 0
			rfTag := builder.genNewTag()

			if isUpdate && updatePkCol { // update stmt && pk included in update cols
				lastNodeId = appendSinkScanNode(builder, bindCtx, sourceStep)
				rowIdDef := MakeRowIdColDef()
				tableDef.Cols = append(tableDef.Cols, rowIdDef)
				scanTableDef := DeepCopyTableDef(tableDef)

				colPos := make(map[int32]int32)
				colPos[int32(pkPos)] = 0
				// if len(pkFilterExprs) > 0 {
				// 	for _, e := range pkFilterExprs {
				// 		getColPos(e, colPos)
				// 	}
				// }
				var newCols []*ColDef
				rowIdIdx := len(scanTableDef.Cols) - 1
				for idx, col := range scanTableDef.Cols {
					if _, ok := colPos[int32(idx)]; ok {
						colPos[int32(idx)] = int32(len(newCols))
						newCols = append(newCols, col)
					}
					if col.Name == catalog.Row_ID {
						colPos[int32(idx)] = int32(len(newCols))
						newCols = append(newCols, col)
					}
				}
				scanTableDef.Cols = newCols

				scanPkExpr := &Expr{
					Typ: pkTyp,
					Expr: &plan.Expr_Col{
						Col: &ColRef{
							ColPos: colPos[int32(pkPos)],
							Name:   tableDef.Pkey.PkeyColName,
						},
					},
				}
				scanRowIdExpr := &Expr{
					Typ: rowIdDef.Typ,
					Expr: &plan.Expr_Col{
						Col: &ColRef{
							ColPos: colPos[int32(rowIdIdx)],
							Name:   rowIdDef.Name,
						},
					},
				}
				scanNode := &Node{
					NodeType:    plan.Node_TABLE_SCAN,
					Stats:       &plan.Stats{},
					ObjRef:      objRef,
					TableDef:    scanTableDef,
					ProjectList: []*Expr{scanPkExpr, scanRowIdExpr},
					RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{
						{
							Tag: rfTag,
							Expr: &plan.Expr{
								Typ: DeepCopyType(pkTyp),
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 0,
										ColPos: 0,
										Name:   tableDef.Pkey.PkeyColName,
									},
								},
							},
						},
					},
				}
				rightId := builder.appendNode(scanNode, bindCtx)
				// if len(pkFilterExprs) > 0 {
				// 	for _, e := range pkFilterExprs {
				// 		resetColPos(e, colPos)
				// 	}
				// 	blockFilters := make([]*Expr, len(pkFilterExprs))
				// 	for i, e := range pkFilterExprs {
				// 		blockFilters[i] = DeepCopyExpr(e)
				// 	}
				// 	scanNode.FilterList = pkFilterExprs
				// 	scanNode.BlockFilterList = blockFilters
				// }

				pkColExpr := &Expr{
					Typ: pkTyp,
					Expr: &plan.Expr_Col{
						Col: &ColRef{
							RelPos: 1,
							ColPos: int32(pkPos),
							Name:   tableDef.Pkey.PkeyColName,
						},
					},
				}
				rightExpr := &Expr{
					Typ: pkTyp,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							Name: tableDef.Pkey.PkeyColName,
						},
					},
				}
				condExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{pkColExpr, rightExpr})
				if err != nil {
					return err
				}
				rightRowIdExpr := &Expr{
					Typ: rowIdDef.Typ,
					Expr: &plan.Expr_Col{
						Col: &ColRef{
							ColPos: 1,
							Name:   rowIdDef.Name,
						},
					},
				}
				rowIdExpr := &Expr{
					Typ: rowIdDef.Typ,
					Expr: &plan.Expr_Col{
						Col: &ColRef{
							RelPos: 1,
							ColPos: int32(len(tableDef.Cols) - 1),
							Name:   rowIdDef.Name,
						},
					},
				}

				joinNode := &plan.Node{
					NodeType:    plan.Node_JOIN,
					Children:    []int32{rightId, lastNodeId},
					JoinType:    plan.Node_RIGHT,
					OnList:      []*Expr{condExpr},
					ProjectList: []*Expr{rowIdExpr, rightRowIdExpr, pkColExpr},
					RuntimeFilterBuildList: []*plan.RuntimeFilterSpec{
						{
							Tag: rfTag,
							Expr: &plan.Expr{
								Typ: DeepCopyType(pkTyp),
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 0,
										ColPos: 0,
									},
								},
							},
						},
					},
				}
				lastNodeId = builder.appendNode(joinNode, bindCtx)

				// append agg node.
				aggGroupBy := []*Expr{
					{
						Typ: rowIdExpr.Typ,
						Expr: &plan.Expr_Col{
							Col: &ColRef{
								ColPos: 0,
								Name:   catalog.Row_ID,
							},
						}},
					{
						Typ: rowIdExpr.Typ,
						Expr: &plan.Expr_Col{
							Col: &ColRef{
								ColPos: 1,
								Name:   catalog.Row_ID,
							},
						}},
					{
						Typ: pkColExpr.Typ,
						Expr: &plan.Expr_Col{
							Col: &ColRef{
								ColPos: 2,
								Name:   tableDef.Pkey.PkeyColName,
							},
						}},
				}
				aggProject := []*Expr{
					{
						Typ: rowIdExpr.Typ,
						Expr: &plan.Expr_Col{
							Col: &ColRef{
								RelPos: 1,
								ColPos: 0,
								Name:   catalog.Row_ID,
							},
						}},
					{
						Typ: rowIdExpr.Typ,
						Expr: &plan.Expr_Col{
							Col: &ColRef{
								RelPos: 1,
								ColPos: 1,
								Name:   catalog.Row_ID,
							},
						}},
					{
						Typ: pkColExpr.Typ,
						Expr: &plan.Expr_Col{
							Col: &ColRef{
								RelPos: 1,
								ColPos: 2,
								Name:   tableDef.Pkey.PkeyColName,
							},
						}},
				}
				aggNode := &Node{
					NodeType:    plan.Node_AGG,
					Children:    []int32{lastNodeId},
					GroupBy:     aggGroupBy,
					ProjectList: aggProject,
				}
				lastNodeId = builder.appendNode(aggNode, bindCtx)

				// append filter node
				filterExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "not_in_rows", []*Expr{
					{
						Typ: rowIdExpr.Typ,
						Expr: &plan.Expr_Col{
							Col: &ColRef{ColPos: 1, Name: catalog.Row_ID},
						},
					},
					{
						Typ: rowIdExpr.Typ,
						Expr: &plan.Expr_Col{
							Col: &ColRef{ColPos: 0, Name: catalog.Row_ID},
						},
					},
				})
				if err != nil {
					return err
				}
				colExpr := &Expr{
					Typ: rowIdDef.Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							Name: rowIdDef.Name,
						},
					},
				}

				lastNodeId = builder.appendNode(&Node{
					NodeType:   plan.Node_FILTER,
					Children:   []int32{lastNodeId},
					FilterList: []*Expr{filterExpr},
					ProjectList: []*Expr{
						colExpr,
						{
							Typ: tableDef.Cols[pkPos].Typ,
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{ColPos: 2, Name: tableDef.Cols[pkPos].Name},
							},
						},
					},
				}, bindCtx)

				// append assert node
				isEmptyExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "isempty", []*Expr{colExpr})
				if err != nil {
					return err
				}

				varcharType := types.T_varchar.ToType()
				varcharExpr, err := makePlan2CastExpr(builder.GetContext(), &Expr{
					Typ: tableDef.Cols[pkPos].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{ColPos: 1, Name: tableDef.Cols[pkPos].Name},
					},
				}, makePlan2Type(&varcharType))
				if err != nil {
					return err
				}
				assertExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "assert", []*Expr{isEmptyExpr, varcharExpr, makePlan2StringConstExprWithType(tableDef.Cols[pkPos].Name)})
				if err != nil {
					return err
				}
				lastNodeId = builder.appendNode(&Node{
					NodeType:   plan.Node_FILTER,
					Children:   []int32{lastNodeId},
					FilterList: []*Expr{assertExpr},
					IsEnd:      true,
				}, bindCtx)
				builder.appendStep(lastNodeId)
			}

			if !isUpdate && !builder.qry.LoadTag { // insert stmt but not load
				if !checkInsertPkDup && pkFilterExprs != nil {
					scanTableDef := DeepCopyTableDef(tableDef)
					// scanTableDef.Cols = []*ColDef{scanTableDef.Cols[pkPos]}
					pkNameMap := make(map[string]int)
					for i, n := range tableDef.Pkey.Names {
						pkNameMap[n] = i
					}
					newCols := make([]*ColDef, len(scanTableDef.Pkey.Names))
					for _, def := range scanTableDef.Cols {
						if i, ok := pkNameMap[def.Name]; ok {
							newCols[i] = def
						}
					}
					if len(newCols) > 1 {
						for _, col := range scanTableDef.Cols {
							if col.Name == scanTableDef.Pkey.PkeyColName {
								newCols = append(newCols, col)
								break
							}
						}
					}
					scanTableDef.Cols = newCols
					scanNode := &plan.Node{
						NodeType: plan.Node_TABLE_SCAN,
						Stats:    &plan.Stats{},
						ObjRef:   objRef,
						TableDef: scanTableDef,
						ProjectList: []*Expr{{
							Typ: pkTyp,
							Expr: &plan.Expr_Col{
								Col: &ColRef{
									ColPos: int32(len(newCols) - 1),
									Name:   tableDef.Pkey.PkeyColName,
								},
							},
						}},
					}

					scanNode.FilterList = pkFilterExprs
					blockFilterList := make([]*Expr, len(pkFilterExprs))
					for i, e := range pkFilterExprs {
						blockFilterList[i] = DeepCopyExpr(e)
					}
					lastNodeId = builder.appendNode(scanNode, bindCtx)
					scanNode.BlockFilterList = blockFilterList
				} else {
					lastNodeId = appendSinkScanNode(builder, bindCtx, sourceStep)
					scanTableDef := DeepCopyTableDef(tableDef)
					scanTableDef.Cols = []*ColDef{scanTableDef.Cols[pkPos]}
					scanNode := &plan.Node{
						NodeType: plan.Node_TABLE_SCAN,
						Stats:    &plan.Stats{},
						ObjRef:   objRef,
						TableDef: scanTableDef,
						ProjectList: []*Expr{{
							Typ: pkTyp,
							Expr: &plan.Expr_Col{
								Col: &ColRef{
									ColPos: 0,
									Name:   tableDef.Pkey.PkeyColName,
								},
							},
						}},
						RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{
							{
								Tag: rfTag,
								Expr: &plan.Expr{
									Typ: DeepCopyType(pkTyp),
									Expr: &plan.Expr_Col{
										Col: &plan.ColRef{
											RelPos: 0,
											ColPos: 0,
											Name:   tableDef.Pkey.PkeyColName,
										},
									},
								},
							},
						},
					}
					rightId := builder.appendNode(scanNode, bindCtx)

					leftExpr := &Expr{
						Typ: pkTyp,
						Expr: &plan.Expr_Col{
							Col: &ColRef{
								RelPos: 1,
								ColPos: int32(pkPos),
								Name:   tableDef.Pkey.PkeyColName,
							},
						},
					}
					rightExpr := &Expr{
						Typ: pkTyp,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								Name: tableDef.Pkey.PkeyColName,
							},
						},
					}
					condExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{rightExpr, leftExpr})
					if err != nil {
						return err
					}

					joinNode := &plan.Node{
						NodeType: plan.Node_JOIN,
						Children: []int32{rightId, lastNodeId},
						//Children: []int32{lastNodeId, rightId},
						JoinType:    plan.Node_SEMI,
						OnList:      []*Expr{condExpr},
						BuildOnLeft: true,
						ProjectList: []*Expr{leftExpr},
						RuntimeFilterBuildList: []*plan.RuntimeFilterSpec{
							{
								Tag: rfTag,
								Expr: &plan.Expr{
									Typ: DeepCopyType(pkTyp),
									Expr: &plan.Expr_Col{
										Col: &plan.ColRef{
											RelPos: 0,
											ColPos: 0,
										},
									},
								},
							},
						},
					}
					lastNodeId = builder.appendNode(joinNode, bindCtx)
				}

				colExpr := &Expr{
					Typ: pkTyp,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							Name: tableDef.Pkey.PkeyColName,
						},
					},
				}

				isEmptyExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "isempty", []*Expr{colExpr})
				if err != nil {
					return err
				}

				varcharType := types.T_varchar.ToType()
				varcharExpr, err := makePlan2CastExpr(builder.GetContext(), &Expr{
					Typ: tableDef.Cols[pkPos].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{ColPos: 0, Name: tableDef.Cols[pkPos].Name},
					},
				}, makePlan2Type(&varcharType))
				if err != nil {
					return err
				}

				assertExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "assert", []*Expr{isEmptyExpr, varcharExpr, makePlan2StringConstExprWithType(tableDef.Cols[pkPos].Name)})
				if err != nil {
					return err
				}
				filterNode := &Node{
					NodeType:   plan.Node_FILTER,
					Children:   []int32{lastNodeId},
					FilterList: []*Expr{assertExpr},
					IsEnd:      true,
				}
				lastNodeId = builder.appendNode(filterNode, bindCtx)
				builder.appendStep(lastNodeId)
			}
		}
	}

	return nil
}

// makeOneDeletePlan
// lock -> delete
func makeOneDeletePlan(
	builder *QueryBuilder,
	bindCtx *BindContext,
	lastNodeId int32,
	delNodeInfo *deleteNodeInfo,
) (int32, error) {
	// append lock
	lockTarget := &plan.LockTarget{
		TableId:            delNodeInfo.tableDef.TblId,
		PrimaryColIdxInBat: int32(delNodeInfo.pkPos),
		PrimaryColTyp:      delNodeInfo.pkTyp,
		RefreshTsIdxInBat:  -1, //unsupport now
		FilterColIdxInBat:  int32(delNodeInfo.partitionIdx),
		LockTable:          delNodeInfo.lockTable,
	}

	if delNodeInfo.tableDef.Partition != nil {
		lockTarget.IsPartitionTable = true
		lockTarget.PartitionTableIds = delNodeInfo.partTableIDs
	}

	lockNode := &Node{
		NodeType:    plan.Node_LOCK_OP,
		Children:    []int32{lastNodeId},
		LockTargets: []*plan.LockTarget{lockTarget},
	}
	lastNodeId = builder.appendNode(lockNode, bindCtx)

	// append delete node
	deleteNode := &Node{
		NodeType: plan.Node_DELETE,
		Children: []int32{lastNodeId},
		// ProjectList: getProjectionByLastNode(builder, lastNodeId),
		DeleteCtx: &plan.DeleteCtx{
			RowIdIdx:            int32(delNodeInfo.deleteIndex),
			Ref:                 delNodeInfo.objRef,
			CanTruncate:         false,
			AddAffectedRows:     delNodeInfo.addAffectedRows,
			IsClusterTable:      delNodeInfo.IsClusterTable,
			PartitionTableIds:   delNodeInfo.partTableIDs,
			PartitionTableNames: delNodeInfo.partTableNames,
			PartitionIdx:        int32(delNodeInfo.partitionIdx),
			PrimaryKeyIdx:       int32(delNodeInfo.pkPos),
		},
	}
	lastNodeId = builder.appendNode(deleteNode, bindCtx)

	return lastNodeId, nil
}

func getProjectionByLastNode(builder *QueryBuilder, lastNodeId int32) []*Expr {
	lastNode := builder.qry.Nodes[lastNodeId]
	projLength := len(lastNode.ProjectList)
	if projLength == 0 {
		return getProjectionByLastNode(builder, lastNode.Children[0])
	}
	projection := make([]*Expr, len(lastNode.ProjectList))
	for i, expr := range lastNode.ProjectList {
		name := ""
		if col, ok := expr.Expr.(*plan.Expr_Col); ok {
			name = col.Col.Name
		}
		projection[i] = &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(i),
					Name:   name,
				},
			},
		}
	}
	return projection
}

func getProjectionByLastNodeWithTag(builder *QueryBuilder, lastNodeId, tag int32) []*Expr {
	lastNode := builder.qry.Nodes[lastNodeId]
	projLength := len(lastNode.ProjectList)
	if projLength == 0 {
		return getProjectionByLastNodeWithTag(builder, lastNode.Children[0], tag)
	}
	projection := make([]*Expr, len(lastNode.ProjectList))
	for i, expr := range lastNode.ProjectList {
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

func haveUniqueKey(tableDef *TableDef) bool {
	for _, indexdef := range tableDef.Indexes {
		if indexdef.Unique {
			return true
		}
	}
	return false
}

// makeDeleteNodeInfo Get `DeleteNode` based on TableDef
func makeDeleteNodeInfo(ctx CompilerContext, objRef *ObjectRef, tableDef *TableDef,
	deleteIdx int, partitionIdx int, addAffectedRows bool, pkPos int, pkTyp *Type, lockTable bool) *deleteNodeInfo {
	delNodeInfo := getDeleteNodeInfo()
	delNodeInfo.objRef = objRef
	delNodeInfo.tableDef = tableDef
	delNodeInfo.deleteIndex = deleteIdx
	delNodeInfo.partitionIdx = partitionIdx
	delNodeInfo.addAffectedRows = addAffectedRows
	delNodeInfo.IsClusterTable = tableDef.TableType == catalog.SystemClusterRel
	delNodeInfo.pkPos = pkPos
	delNodeInfo.pkTyp = pkTyp
	delNodeInfo.lockTable = lockTable

	if tableDef.Partition != nil {
		partTableIds := make([]uint64, tableDef.Partition.PartitionNum)
		partTableNames := make([]string, tableDef.Partition.PartitionNum)
		for i, partition := range tableDef.Partition.Partitions {
			_, partTableDef := ctx.Resolve(objRef.SchemaName, partition.PartitionTableName)
			partTableIds[i] = partTableDef.TblId
			partTableNames[i] = partition.PartitionTableName
		}
		delNodeInfo.partTableIDs = partTableIds
		delNodeInfo.partTableNames = partTableNames
	}
	return delNodeInfo
}

// Get sub tableIds and table names of the partition table
func getPartTableIdsAndNames(ctx CompilerContext, objRef *ObjectRef, tableDef *TableDef) ([]uint64, []string) {
	var partTableIds []uint64
	var partTableNames []string
	if tableDef.Partition != nil {
		partTableIds = make([]uint64, tableDef.Partition.PartitionNum)
		partTableNames = make([]string, tableDef.Partition.PartitionNum)
		for i, partition := range tableDef.Partition.Partitions {
			_, partTableDef := ctx.Resolve(objRef.SchemaName, partition.PartitionTableName)
			partTableIds[i] = partTableDef.TblId
			partTableNames[i] = partition.PartitionTableName
		}
	}
	return partTableIds, partTableNames
}

func appendSinkScanNode(builder *QueryBuilder, bindCtx *BindContext, sourceStep int32) int32 {
	lastNodeId := builder.qry.Steps[sourceStep]
	// lastNode := builder.qry.Nodes[lastNodeId]
	sinkScanProject := getProjectionByLastNode(builder, lastNodeId)
	sinkScanNode := &Node{
		NodeType:    plan.Node_SINK_SCAN,
		SourceStep:  []int32{sourceStep},
		ProjectList: sinkScanProject,
	}
	lastNodeId = builder.appendNode(sinkScanNode, bindCtx)
	return lastNodeId
}

func appendSinkScanNodeWithTag(builder *QueryBuilder, bindCtx *BindContext, sourceStep, tag int32) int32 {
	lastNodeId := builder.qry.Steps[sourceStep]
	// lastNode := builder.qry.Nodes[lastNodeId]
	sinkScanProject := getProjectionByLastNodeWithTag(builder, lastNodeId, tag)
	sinkScanNode := &Node{
		NodeType:    plan.Node_SINK_SCAN,
		SourceStep:  []int32{sourceStep},
		ProjectList: sinkScanProject,
		BindingTags: []int32{tag},
	}
	lastNodeId = builder.appendNode(sinkScanNode, bindCtx)
	return lastNodeId
}

func appendRecursiveScanNode(builder *QueryBuilder, bindCtx *BindContext, sourceStep, tag int32) int32 {
	lastNodeId := builder.qry.Steps[sourceStep]
	// lastNode := builder.qry.Nodes[lastNodeId]
	recursiveScanProject := getProjectionByLastNodeWithTag(builder, lastNodeId, tag)
	recursiveScanNode := &Node{
		NodeType:    plan.Node_RECURSIVE_SCAN,
		SourceStep:  []int32{sourceStep},
		ProjectList: recursiveScanProject,
		BindingTags: []int32{tag},
	}
	lastNodeId = builder.appendNode(recursiveScanNode, bindCtx)
	return lastNodeId
}

func appendCTEScanNode(builder *QueryBuilder, bindCtx *BindContext, sourceStep, tag int32) int32 {
	lastNodeId := builder.qry.Steps[sourceStep]
	// lastNode := builder.qry.Nodes[lastNodeId]
	recursiveScanProject := getProjectionByLastNodeWithTag(builder, lastNodeId, tag)
	recursiveScanNode := &Node{
		NodeType:    plan.Node_RECURSIVE_CTE,
		SourceStep:  []int32{sourceStep},
		ProjectList: recursiveScanProject,
		BindingTags: []int32{tag},
	}
	lastNodeId = builder.appendNode(recursiveScanNode, bindCtx)
	return lastNodeId
}

func appendSinkNode(builder *QueryBuilder, bindCtx *BindContext, lastNodeId int32) int32 {
	sinkProject := getProjectionByLastNode(builder, lastNodeId)
	sinkNode := &Node{
		NodeType:    plan.Node_SINK,
		Children:    []int32{lastNodeId},
		ProjectList: sinkProject,
	}
	lastNodeId = builder.appendNode(sinkNode, bindCtx)
	return lastNodeId
}

func appendSinkNodeWithTag(builder *QueryBuilder, bindCtx *BindContext, lastNodeId, tag int32) int32 {
	sinkProject := getProjectionByLastNodeWithTag(builder, lastNodeId, tag)
	sinkNode := &Node{
		NodeType:    plan.Node_SINK,
		Children:    []int32{lastNodeId},
		ProjectList: sinkProject,
		BindingTags: []int32{tag},
	}
	lastNodeId = builder.appendNode(sinkNode, bindCtx)
	return lastNodeId
}

func appendAggCountGroupByColExpr(builder *QueryBuilder, bindCtx *BindContext, lastNodeId int32, colExpr *plan.Expr) (int32, error) {
	aggExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "starcount", []*Expr{colExpr})
	if err != nil {
		return -1, err
	}

	countType := types.T_int64.ToType()
	groupByNode := &Node{
		NodeType: plan.Node_AGG,
		Children: []int32{lastNodeId},
		GroupBy:  []*Expr{colExpr},
		AggList:  []*Expr{aggExpr},
		ProjectList: []*Expr{
			{
				Typ: makePlan2Type(&countType),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -2,
						ColPos: 1,
					},
				},
			},
			{
				Typ: colExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -2,
						ColPos: 0,
					},
				},
			}},
	}
	lastNodeId = builder.appendNode(groupByNode, bindCtx)
	return lastNodeId, nil
}

func getPkPos(tableDef *TableDef, ignoreFakePK bool) (int, *Type) {
	if tableDef.Pkey == nil {
		return -1, nil
	}
	pkName := tableDef.Pkey.PkeyColName
	// if pkName == catalog.CPrimaryKeyColName {
	// 	return len(tableDef.Cols) - 1, makeHiddenColTyp()
	// }
	for i, col := range tableDef.Cols {
		if col.Name == pkName {
			if ignoreFakePK && col.Name == catalog.FakePrimaryKeyColName {
				continue
			}
			return i, col.Typ
		}
	}
	return -1, nil
}

func getRowIdPos(tableDef *TableDef) int {
	for i, col := range tableDef.Cols {
		if col.Name == catalog.Row_ID {
			return i
		}
	}
	return -1
}

func getHiddenColumnForPreInsert(tableDef *TableDef) ([]*Type, []string) {
	var typs []*Type
	var names []string
	if tableDef.Pkey != nil && tableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		typs = append(typs, makeHiddenColTyp())
		names = append(names, catalog.CPrimaryKeyColName)
	} else if tableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
		typs = append(typs, makeHiddenColTyp())
		names = append(names, tableDef.ClusterBy.Name)
	}
	return typs, names
}

// appendPreDeleteNode  build predelete node.
func appendPreDeleteNode(builder *QueryBuilder, bindCtx *BindContext, objRef *ObjectRef, tableDef *TableDef, lastNodeId int32) int32 {
	projection := getProjectionByLastNode(builder, lastNodeId)
	partitionExpr := DeepCopyExpr(tableDef.Partition.PartitionExpression)
	projection = append(projection, partitionExpr)

	preDeleteNode := &Node{
		NodeType:    plan.Node_PRE_DELETE,
		ObjRef:      objRef,
		Children:    []int32{lastNodeId},
		ProjectList: projection,
	}
	return builder.appendNode(preDeleteNode, bindCtx)
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

	//for stmt:  update c1 set ref_col = null where col > 0;
	//we will skip foreign key constraint check when set null
	projectProjection := getProjectionByLastNode(builder, baseNodeId)
	baseNodeId = builder.appendNode(&Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{baseNodeId},
		ProjectList: projectProjection,
	}, bindCtx)

	var filterConds []*Expr
	for _, fk := range tableDef.Fkeys {
		for _, colId := range fk.Cols {
			for fIdx, col := range tableDef.Cols {
				if col.ColId == colId {
					colExpr := &Expr{
						Typ: col.Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								ColPos: int32(fIdx),
								Name:   col.Name,
							},
						},
					}
					condExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "isnotnull", []*Expr{colExpr})
					if err != nil {
						return -1, err
					}
					filterConds = append(filterConds, condExpr)
				}
			}
		}
	}
	baseNodeId = builder.appendNode(&Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{baseNodeId},
		FilterList: filterConds,
		// ProjectList: projectProjection,
	}, bindCtx)

	lastNodeId := baseNodeId
	for _, fk := range tableDef.Fkeys {
		parentObjRef, parentTableDef := builder.compCtx.ResolveById(fk.ForeignTbl)
		parentPosMap := make(map[string]int32)
		parentTypMap := make(map[string]*plan.Type)
		parentId2name := make(map[uint64]string)
		for idx, col := range parentTableDef.Cols {
			parentPosMap[col.Name] = int32(idx)
			parentTypMap[col.Name] = col.Typ
			parentId2name[col.ColId] = col.Name
		}
		projectList := getProjectionByLastNode(builder, lastNodeId)

		// append table scan node
		scanNodeProject := make([]*Expr, len(parentTableDef.Cols))
		for colIdx, col := range parentTableDef.Cols {
			scanNodeProject[colIdx] = &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						ColPos: int32(colIdx),
						Name:   col.Name,
					},
				},
			}
		}
		rightId := builder.appendNode(&plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			Stats:       &plan.Stats{},
			ObjRef:      parentObjRef,
			TableDef:    parentTableDef,
			ProjectList: scanNodeProject,
		}, bindCtx)

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
								RelPos: 0,
								ColPos: int32(name2pos[childColumnName]),
								Name:   childColumnName,
							},
						},
					}
					rightExpr := &plan.Expr{
						Typ: parentTypMap[parentColumnName],
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: 1,
								ColPos: parentPosMap[parentColumnName],
								Name:   parentColumnName,
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
					RelPos: 1,
					ColPos: parentPosMap[catalog.Row_ID],
					Name:   catalog.Row_ID,
				},
			},
		})

		// append join node
		lastNodeId = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_JOIN,
			Children:    []int32{lastNodeId, rightId},
			JoinType:    plan.Node_LEFT,
			OnList:      joinConds,
			ProjectList: projectList,
		}, bindCtx)
	}

	return lastNodeId, nil
}

// appendPreInsertNode  append preinsert node
func appendPreInsertNode(builder *QueryBuilder, bindCtx *BindContext,
	objRef *ObjectRef, tableDef *TableDef,
	lastNodeId int32, isUpdate bool) int32 {

	preInsertProjection := getProjectionByLastNode(builder, lastNodeId)
	hiddenColumnTyp, hiddenColumnName := getHiddenColumnForPreInsert(tableDef)

	hashAutoCol := false
	for _, col := range tableDef.Cols {
		if col.Typ.AutoIncr {
			// for insert allways set true when col.Typ.AutoIncr
			// todo for update
			hashAutoCol = true
			break
		}
	}
	if len(hiddenColumnTyp) > 0 {
		if isUpdate {
			rowIdProj := preInsertProjection[len(preInsertProjection)-1]
			preInsertProjection = preInsertProjection[:len(preInsertProjection)-1]
			for i, typ := range hiddenColumnTyp {
				preInsertProjection = append(preInsertProjection, &plan.Expr{
					Typ: typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: -1,
						ColPos: int32(i),
						Name:   hiddenColumnName[i],
					}},
				})
			}
			preInsertProjection = append(preInsertProjection, rowIdProj)
		} else {
			for i, typ := range hiddenColumnTyp {
				preInsertProjection = append(preInsertProjection, &plan.Expr{
					Typ: typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: -1,
						ColPos: int32(i),
						Name:   hiddenColumnName[i],
					}},
				})
			}
		}
	}

	preInsertNode := &Node{
		NodeType:    plan.Node_PRE_INSERT,
		Children:    []int32{lastNodeId},
		ProjectList: preInsertProjection,
		PreInsertCtx: &plan.PreInsertCtx{
			Ref:        objRef,
			TableDef:   DeepCopyTableDef(tableDef),
			HasAutoCol: hashAutoCol,
			IsUpdate:   isUpdate,
		},
	}
	lastNodeId = builder.appendNode(preInsertNode, bindCtx)

	// append hidden column to tableDef
	if tableDef.Pkey != nil && tableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		tableDef.Cols = append(tableDef.Cols, tableDef.Pkey.CompPkeyCol)
	}
	if tableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
		tableDef.Cols = append(tableDef.Cols, tableDef.ClusterBy.CompCbkeyCol)
	}

	// Get table partition information
	partTableIds, _ := getPartTableIdsAndNames(builder.compCtx, objRef, tableDef)
	// append project node
	projectProjection := getProjectionByLastNode(builder, lastNodeId)
	partitionIdx := -1

	if tableDef.Partition != nil {
		partitionIdx = len(projectProjection)
		partitionExpr := DeepCopyExpr(tableDef.Partition.PartitionExpression)
		projectProjection = append(projectProjection, partitionExpr)

		projectNode := &Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{lastNodeId},
			ProjectList: projectProjection,
		}
		lastNodeId = builder.appendNode(projectNode, bindCtx)
	}

	if lockNodeId, ok := appendLockNode(
		builder,
		bindCtx,
		lastNodeId,
		tableDef,
		false,
		false,
		partitionIdx,
		partTableIds,
	); ok {
		lastNodeId = lockNodeId
	}

	return lastNodeId
}

// appendPreInsertUkPlan  build preinsert plan.
// sink_scan -> preinsert_uk -> sink
func appendPreInsertUkPlan(
	builder *QueryBuilder,
	bindCtx *BindContext,
	tableDef *TableDef,
	lastNodeId int32,
	indexIdx int,
	isUpddate bool,
	uniqueTableDef *TableDef) (int32, error) {
	var useColumns []int32
	idxDef := tableDef.Indexes[indexIdx]
	partsMap := make(map[string]struct{})
	for _, part := range idxDef.Parts {
		partsMap[part] = struct{}{}
	}
	for i, col := range tableDef.Cols {
		if _, ok := partsMap[col.Name]; ok {
			useColumns = append(useColumns, int32(i))
		}
	}

	pkColumn, originPkType := getPkPos(tableDef, false)
	var ukType *Type
	if len(idxDef.Parts) == 1 {
		ukType = tableDef.Cols[useColumns[0]].Typ
	} else {
		ukType = &Type{
			Id:    int32(types.T_varchar),
			Width: types.MaxVarcharLen,
		}
	}
	var preinsertUkProjection []*Expr
	preinsertUkProjection = append(preinsertUkProjection, &plan.Expr{
		Typ: ukType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: -1,
				ColPos: 0,
				Name:   catalog.IndexTableIndexColName,
			},
		},
	})
	preinsertUkProjection = append(preinsertUkProjection, &plan.Expr{
		Typ: originPkType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: -1,
				ColPos: 1,
				Name:   catalog.IndexTablePrimaryColName,
			},
		},
	})
	if isUpddate {
		lastProjection := builder.qry.Nodes[lastNodeId].ProjectList
		originRowIdIdx := len(lastProjection) - 1
		preinsertUkProjection = append(preinsertUkProjection, &plan.Expr{
			Typ: lastProjection[originRowIdIdx].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(originRowIdIdx),
					Name:   catalog.Row_ID,
				},
			},
		})
	}

	preInsertUkNode := &Node{
		NodeType:    plan.Node_PRE_INSERT_UK,
		Children:    []int32{lastNodeId},
		ProjectList: preinsertUkProjection,
		PreInsertUkCtx: &plan.PreInsertUkCtx{
			Columns:  useColumns,
			PkColumn: int32(pkColumn),
			PkType:   originPkType,
			UkType:   ukType,
			TableDef: tableDef,
		},
	}
	lastNodeId = builder.appendNode(preInsertUkNode, bindCtx)

	if lockNodeId, ok := appendLockNode(
		builder,
		bindCtx,
		lastNodeId,
		uniqueTableDef,
		false,
		false,
		-1,
		nil,
	); ok {
		lastNodeId = lockNodeId
	}

	lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
	sourceStep := builder.appendStep(lastNodeId)

	return sourceStep, nil
}

func appendDeleteUniqueTablePlan(
	builder *QueryBuilder,
	bindCtx *BindContext,
	uniqueObjRef *ObjectRef,
	uniqueTableDef *TableDef,
	indexdef *IndexDef,
	typMap map[string]*plan.Type,
	posMap map[string]int,
	baseNodeId int32,
	updateColLength int,
) (int32, error) {
	lastNodeId := baseNodeId
	var err error
	projectList := getProjectionByLastNode(builder, lastNodeId)

	var rightRowIdPos int32 = -1
	var rightPkPos int32 = -1
	scanNodeProject := make([]*Expr, len(uniqueTableDef.Cols))
	for colIdx, col := range uniqueTableDef.Cols {
		if col.Name == catalog.Row_ID {
			rightRowIdPos = int32(colIdx)
		} else if col.Name == catalog.IndexTableIndexColName {
			rightPkPos = int32(colIdx)
		}
		scanNodeProject[colIdx] = &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					ColPos: int32(colIdx),
					Name:   col.Name,
				},
			},
		}
	}
	rightId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       &plan.Stats{},
		ObjRef:      uniqueObjRef,
		TableDef:    uniqueTableDef,
		ProjectList: scanNodeProject,
	}, bindCtx)

	// append projection
	projectList = append(projectList, &plan.Expr{
		Typ: uniqueTableDef.Cols[rightRowIdPos].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: rightRowIdPos,
				Name:   catalog.Row_ID,
			},
		},
	}, &plan.Expr{
		Typ: uniqueTableDef.Cols[rightPkPos].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: rightPkPos,
				Name:   catalog.IndexTableIndexColName,
			},
		},
	})

	rightExpr := &plan.Expr{
		Typ: uniqueTableDef.Cols[rightPkPos].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: rightPkPos,
				Name:   catalog.IndexTableIndexColName,
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
					RelPos: 0,
					ColPos: int32(posMap[orginIndexColumnName]),
					Name:   orginIndexColumnName,
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
						RelPos: 0,
						ColPos: int32(posMap[column]),
						Name:   column,
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

	lastNodeId = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_JOIN,
		Children:    []int32{lastNodeId, rightId},
		JoinType:    plan.Node_LEFT,
		OnList:      joinConds,
		ProjectList: projectList,
	}, bindCtx)

	return lastNodeId, nil
}

// makePreUpdateDeletePlan
// sink_scan -> project -> [agg] -> [filter] -> sink
func makePreUpdateDeletePlan(
	ctx CompilerContext,
	builder *QueryBuilder,
	bindCtx *BindContext,
	delCtx *dmlPlanCtx,
	lastNodeId int32,
) (int32, error) {
	// lastNodeId := appendSinkScanNode(builder, bindCtx, delCtx.sourceStep)
	lastNode := builder.qry.Nodes[lastNodeId]

	// append project Node to fetch the columns of this table
	// in front of this projectList are update cols
	projectProjection := make([]*Expr, len(delCtx.tableDef.Cols)+delCtx.updateColLength)
	for i, col := range delCtx.tableDef.Cols {
		projectProjection[i] = &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(delCtx.beginIdx + i),
					Name:   col.Name,
				},
			},
		}
	}
	offset := len(delCtx.tableDef.Cols)
	for i := 0; i < delCtx.updateColLength; i++ {
		idx := delCtx.beginIdx + offset + i
		name := ""
		if col, ok := lastNode.ProjectList[idx].Expr.(*plan.Expr_Col); ok {
			name = col.Col.Name
		}
		projectProjection[offset+i] = &plan.Expr{
			Typ: lastNode.ProjectList[idx].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(idx),
					Name:   name,
				},
			},
		}
	}
	projectNode := &Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeId},
		ProjectList: projectProjection,
	}
	lastNodeId = builder.appendNode(projectNode, bindCtx)

	//when update multi table. we append agg node:
	//eg: update t1, t2 set t1.a= t1.a+1 where t2.b >10
	//eg: update t2, (select a from t2) as tt set t2.a= t2.a+1 where t2.b >10
	if delCtx.needAggFilter {
		lastNode := builder.qry.Nodes[lastNodeId]
		groupByExprs := make([]*Expr, len(delCtx.tableDef.Cols))
		aggNodeProjection := make([]*Expr, len(lastNode.ProjectList))
		for i := 0; i < len(delCtx.tableDef.Cols); i++ {
			e := lastNode.ProjectList[i]
			name := ""
			if col, ok := e.Expr.(*plan.Expr_Col); ok {
				name = col.Col.Name
			}
			groupByExprs[i] = &plan.Expr{
				Typ: e.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   name,
					},
				},
			}
			aggNodeProjection[i] = &plan.Expr{
				Typ: e.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: int32(i),
						Name:   name,
					},
				},
			}
		}
		offset := len(delCtx.tableDef.Cols)
		aggList := make([]*Expr, delCtx.updateColLength)
		for i := 0; i < delCtx.updateColLength; i++ {
			pos := offset + i
			e := lastNode.ProjectList[pos]
			name := ""
			if col, ok := e.Expr.(*plan.Expr_Col); ok {
				name = col.Col.Name
			}
			baseExpr := &plan.Expr{
				Typ: e.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(pos),
						Name:   name,
					},
				},
			}
			aggExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "any_value", []*Expr{baseExpr})
			if err != nil {
				return -1, err
			}
			aggList[i] = aggExpr
			aggNodeProjection[pos] = &plan.Expr{
				Typ: e.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -2,
						ColPos: int32(pos),
						Name:   name,
					},
				},
			}
		}

		aggNode := &Node{
			NodeType:    plan.Node_AGG,
			Children:    []int32{lastNodeId},
			GroupBy:     groupByExprs,
			AggList:     aggList,
			ProjectList: aggNodeProjection,
		}
		lastNodeId = builder.appendNode(aggNode, bindCtx)

		// we need filter null in left join/right join
		// eg: UPDATE stu s LEFT JOIN class c ON s.class_id = c.id SET s.class_name = 'test22', c.stu_name = 'test22';
		// we can not let null rows in batch go to insert Node
		rowIdExpr := &plan.Expr{
			Typ: aggNodeProjection[delCtx.rowIdPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(delCtx.rowIdPos),
					Name:   catalog.Row_ID,
				},
			},
		}
		nullCheckExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "isnotnull", []*Expr{rowIdExpr})
		if err != nil {
			return -1, err
		}
		filterProjection := getProjectionByLastNode(builder, lastNodeId)
		filterNode := &Node{
			NodeType:    plan.Node_FILTER,
			Children:    []int32{lastNodeId},
			FilterList:  []*Expr{nullCheckExpr},
			ProjectList: filterProjection,
		}
		lastNodeId = builder.appendNode(filterNode, bindCtx)
	}

	// lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
	// nextSourceStep := builder.appendStep(lastNodeId)

	return lastNodeId, nil
}

// func getColPos(expr *Expr, colPos map[int32]int32) {
// 	switch e := expr.Expr.(type) {
// 	case *plan.Expr_Col:
// 		colPos[e.Col.ColPos] = 0
// 	case *plan.Expr_F:
// 		for _, arg := range e.F.Args {
// 			getColPos(arg, colPos)
// 		}
// 	}
// }

// func resetColPos(expr *Expr, colPos map[int32]int32) {
// 	switch e := expr.Expr.(type) {
// 	case *plan.Expr_Col:
// 		e.Col.ColPos = colPos[e.Col.ColPos]
// 	case *plan.Expr_F:
// 		for _, arg := range e.F.Args {
// 			resetColPos(arg, colPos)
// 		}
// 	}
// }

func appendLockNode(
	builder *QueryBuilder,
	bindCtx *BindContext,
	lastNodeId int32,
	tableDef *TableDef,
	lockTable bool,
	block bool,
	partitionIdx int,
	partTableIDs []uint64,
) (int32, bool) {
	// if do not lock table without pk. you can change to:
	// pkPos, pkTyp := getPkPos(tableDef, true)
	pkPos, pkTyp := getPkPos(tableDef, false)
	if pkPos == -1 {
		return -1, false
	}

	if builder.qry.LoadTag && !lockTable {
		return -1, false
	}

	lockTarget := &plan.LockTarget{
		TableId:            tableDef.TblId,
		PrimaryColIdxInBat: int32(pkPos),
		PrimaryColTyp:      pkTyp,
		RefreshTsIdxInBat:  -1, //unsupport now
		LockTable:          lockTable,
		Block:              block,
	}

	if !lockTable && tableDef.Partition != nil {
		lockTarget.IsPartitionTable = true
		lockTarget.FilterColIdxInBat = int32(partitionIdx)
		lockTarget.PartitionTableIds = partTableIDs
	}

	lockNode := &Node{
		NodeType:    plan.Node_LOCK_OP,
		Children:    []int32{lastNodeId},
		LockTargets: []*plan.LockTarget{lockTarget},
	}
	lastNodeId = builder.appendNode(lockNode, bindCtx)
	return lastNodeId, true
}

type sinkMeta struct {
	step  int
	scans []*sinkScanMeta
}

type sinkScanMeta struct {
	step           int
	nodeId         int32
	sinkNodeId     int32
	preNodeId      int32
	preNodeIsUnion bool //if preNode is Union, one sinkScan to one sink is fine
}

func reduceSinkSinkScanNodes(qry *Query) {
	if len(qry.Steps) == 1 {
		return
	}
	stepMaps := make(map[int]int32)
	sinks := make(map[int32]*sinkMeta)
	for i, nodeId := range qry.Steps {
		stepMaps[i] = nodeId
		collectSinkAndSinkScanMeta(qry, sinks, i, nodeId, -1)
	}

	// merge one sink to one sinkScan
	pointToNodeMap := make(map[int32][]int32)
	for sinkNodeId, meta := range sinks {
		if len(meta.scans) == 1 && !meta.scans[0].preNodeIsUnion {
			// one sink to one sinkScan
			sinkNode := qry.Nodes[sinkNodeId]
			sinkScanPreNode := qry.Nodes[meta.scans[0].preNodeId]
			sinkScanPreNode.Children = sinkNode.Children
			delete(stepMaps, meta.step)
		} else {
			for _, scanMeta := range meta.scans {
				if _, ok := pointToNodeMap[sinkNodeId]; !ok {
					pointToNodeMap[sinkNodeId] = []int32{scanMeta.nodeId}
				} else {
					pointToNodeMap[sinkNodeId] = append(pointToNodeMap[sinkNodeId], scanMeta.nodeId)
				}
			}
		}
	}

	newStepLength := len(stepMaps)
	if len(qry.Steps) > newStepLength {
		// reset steps & some sinkScan's sourceStep
		newSteps := make([]int32, 0, newStepLength)
		keys := make([]int, 0, newStepLength)
		for key := range stepMaps {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			nodeId := stepMaps[key]
			newStepIdx := len(newSteps)
			newSteps = append(newSteps, nodeId)
			if sinkScanNodeIds, ok := pointToNodeMap[nodeId]; ok {
				for _, sinkScanNodeId := range sinkScanNodeIds {
					qry.Nodes[sinkScanNodeId].SourceStep = []int32{int32(newStepIdx)}
				}
			}
		}
		qry.Steps = newSteps
	}
}

func collectSinkAndSinkScanMeta(
	qry *Query,
	sinks map[int32]*sinkMeta,
	oldStep int,
	nodeId int32,
	preNodeId int32) {
	node := qry.Nodes[nodeId]

	if node.NodeType == plan.Node_SINK {
		if _, ok := sinks[nodeId]; !ok {
			sinks[nodeId] = &sinkMeta{
				step:  oldStep,
				scans: make([]*sinkScanMeta, 0, len(qry.Steps)),
			}
		} else {
			sinks[nodeId].step = oldStep
		}
	} else if node.NodeType == plan.Node_SINK_SCAN {
		sinkNodeId := qry.Steps[node.SourceStep[0]]
		if _, ok := sinks[sinkNodeId]; !ok {
			sinks[sinkNodeId] = &sinkMeta{
				step:  -1,
				scans: make([]*sinkScanMeta, 0, len(qry.Steps)),
			}
		}

		meta := &sinkScanMeta{
			step:           oldStep,
			nodeId:         nodeId,
			sinkNodeId:     sinkNodeId,
			preNodeId:      preNodeId,
			preNodeIsUnion: qry.Nodes[preNodeId].NodeType == plan.Node_UNION,
		}
		sinks[sinkNodeId].scans = append(sinks[sinkNodeId].scans, meta)
	}

	for _, childId := range node.Children {
		collectSinkAndSinkScanMeta(qry, sinks, oldStep, childId, nodeId)
	}

}
