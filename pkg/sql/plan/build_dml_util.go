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
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	"golang.org/x/exp/slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

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
	ctx.updatePkCol = true
	ctx.partitionInfos = make(map[uint64]*partSubTableInfo)
	return ctx
}

func putDmlPlanCtx(ctx *dmlPlanCtx) {
	var x dmlPlanCtx
	*ctx = x
	dmlPlanCtxPool.Put(ctx)
}

func getDeleteNodeInfo() *deleteNodeInfo {
	info := deleteNodeInfoPool.Get().(*deleteNodeInfo)
	return info
}

func putDeleteNodeInfo(info *deleteNodeInfo) {
	var x deleteNodeInfo
	*info = x
	deleteNodeInfoPool.Put(info)
}

type dmlPlanCtx struct {
	objRef                 *ObjectRef
	tableDef               *TableDef
	beginIdx               int
	sourceStep             int32
	isMulti                bool
	needAggFilter          bool
	updateColLength        int
	rowIdPos               int
	insertColPos           []int
	updateColPosMap        map[string]int
	allDelTableIDs         map[uint64]struct{}
	allDelTables           map[FkReferKey]struct{}
	isFkRecursionCall      bool //if update plan was recursion called by parent table( ref foreign key), we do not check parent's foreign key contraint
	lockTable              bool //we need lock table in stmt: delete from tbl
	checkInsertPkDup       bool //if we need check for duplicate values in insert batch.  eg:insert into t values (1).  load data will not check
	updatePkCol            bool //if update stmt will update the primary key or one of pks
	pkFilterExprs          []*Expr
	isDeleteWithoutFilters bool
	partitionInfos         map[uint64]*partSubTableInfo // key: Main Table Id, value: Partition sub table information
}

type partSubTableInfo struct {
	partTableIDs   []uint64 // Align array index with the partition number
	partTableNames []string // Align partition subtable names with partition numbers
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
	indexTableNames []string
	foreignTbl      []uint64
	addAffectedRows bool // for hidden table, should not update affect Rows, e.g. delete 1 row from table t with schema like a int, b unique key, c key, affact rows should be 1 instead of 3
	pkPos           int
	pkTyp           plan.Type
	lockTable       bool
}

// buildInsertPlans  build insert plan.
func buildInsertPlans(
	ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, stmt *tree.Insert,
	objRef *ObjectRef, tableDef *TableDef, lastNodeId int32, ifExistAutoPkCol bool,
	insertWithoutUniqueKeyMap map[string]bool,
	ifInsertFromUniqueColMap map[string]bool,
) error {

	var err error
	var insertColsNameFromStmt []string
	var pkFilterExpr []*Expr
	var newPartitionExpr *Expr
	if stmt != nil {
		insertColsNameFromStmt, err = getInsertColsFromStmt(ctx.GetContext(), stmt, tableDef)
		if err != nil {
			return err
		}

		// try to build pk filter epxr for origin table
		if canUsePkFilter(builder, ctx, stmt, tableDef, insertColsNameFromStmt, nil) {
			pkLocationMap := newLocationMap(tableDef, nil)
			// The insert statement subplan with a primary key has undergone manual column pruning in advance,
			// so the partition expression needs to be remapped and judged whether partition pruning can be performed
			newPartitionExpr = remapPartitionExpr(builder, tableDef, pkLocationMap.getPkOrderInValues(insertColsNameFromStmt))
			if pkFilterExpr, err = getPkValueExpr(builder, ctx, tableDef, pkLocationMap, insertColsNameFromStmt); err != nil {
				return err
			}
		}
	}

	// add plan: -> preinsert -> sink
	lastNodeId = appendPreInsertNode(builder, bindCtx, objRef, tableDef, lastNodeId, false)

	lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
	sourceStep := builder.appendStep(lastNodeId)

	// make insert plans for origin table and related index table
	insertBindCtx := NewBindContext(builder, nil)
	updateColLength := 0
	updatePkCol := true
	addAffectedRows := true
	isFkRecursionCall := false
	ifNeedCheckPkDup := !builder.qry.LoadTag
	var indexSourceColTypes []*plan.Type
	var fuzzymessage *OriginTableMessageForFuzzy
	return buildInsertPlansWithRelatedHiddenTable(stmt, ctx, builder, insertBindCtx, objRef, tableDef,
		updateColLength, sourceStep, addAffectedRows, isFkRecursionCall, updatePkCol, pkFilterExpr,
		newPartitionExpr, ifExistAutoPkCol, ifNeedCheckPkDup, indexSourceColTypes, fuzzymessage, insertWithoutUniqueKeyMap, ifInsertFromUniqueColMap)
}

// buildUpdatePlans  build update plan.
func buildUpdatePlans(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, updatePlanCtx *dmlPlanCtx, addAffectedRows bool) error {
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
	projectList := make([]*Expr, insertColLength)
	for i, idx := range updatePlanCtx.insertColPos {
		name := ""
		if col, ok := lastNode.ProjectList[idx].Expr.(*plan.Expr_Col); ok {
			name = col.Col.Name
		}
		projectList[i] = &plan.Expr{
			Typ: lastNode.ProjectList[idx].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					ColPos: int32(idx),
					Name:   name,
				},
			},
		}
	}
	projectList[insertColLength-1] = &plan.Expr{
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
		ProjectList: projectList,
	}
	lastNodeId = builder.appendNode(projectNode, bindCtx)
	//append preinsert node
	lastNodeId = appendPreInsertNode(builder, bindCtx, updatePlanCtx.objRef, updatePlanCtx.tableDef, lastNodeId, true)

	//append sink node
	lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
	sourceStep := builder.appendStep(lastNodeId)

	// build insert plan.
	insertBindCtx := NewBindContext(builder, nil)
	var partitionExpr *Expr
	ifExistAutoPkCol := false
	ifNeedCheckPkDup := true
	var indexSourceColTypes []*plan.Type
	var fuzzymessage *OriginTableMessageForFuzzy
	return buildInsertPlansWithRelatedHiddenTable(nil, ctx, builder, insertBindCtx, updatePlanCtx.objRef, updatePlanCtx.tableDef,
		updatePlanCtx.updateColLength, sourceStep, addAffectedRows, updatePlanCtx.isFkRecursionCall, updatePlanCtx.updatePkCol,
		updatePlanCtx.pkFilterExprs, partitionExpr, ifExistAutoPkCol, ifNeedCheckPkDup, indexSourceColTypes, fuzzymessage, nil, nil)
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

	// delete unique/secondary index table
	// Refer to this PR:https://github.com/matrixorigin/matrixone/pull/12093
	// we have build SK using UK code path. So we might see UK in function signature even thought it could be for
	// both UK and SK. To handle SK case, we will have flags to indicate if it's UK or SK.
	canTruncate := delCtx.isDeleteWithoutFilters

	accountId, err := ctx.GetAccountId()
	if err != nil {
		return err
	}

	enabled, err := IsForeignKeyChecksEnabled(ctx)
	if err != nil {
		return err
	}

	if enabled && len(delCtx.tableDef.RefChildTbls) > 0 ||
		delCtx.tableDef.ViewSql != nil ||
		(util.TableIsClusterTable(delCtx.tableDef.GetTableType()) && accountId != catalog.System_Account) ||
		delCtx.objRef.PubInfo != nil {
		canTruncate = false
	}

	// create delete index plans here see build_dml_index_util.go
	err = buildDeleteIndexPlans(ctx, builder, bindCtx, delCtx)
	if err != nil {
		return err
	}

	// delete origin table
	lastNodeId := appendSinkScanNode(builder, bindCtx, delCtx.sourceStep)
	partExprIdx := -1
	if delCtx.tableDef.Partition != nil {
		partExprIdx = len(delCtx.tableDef.Cols) + delCtx.updateColLength
		lastNodeId = appendPreDeleteNode(builder, bindCtx, delCtx.objRef, delCtx.tableDef, lastNodeId)
	}
	pkPos, pkTyp := getPkPos(delCtx.tableDef, false)
	delNodeInfo := makeDeleteNodeInfo(ctx, delCtx.objRef, delCtx.tableDef, delCtx.rowIdPos, partExprIdx, true, pkPos, pkTyp, delCtx.lockTable, delCtx.partitionInfos)
	lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo, false, false, canTruncate)
	putDeleteNodeInfo(delNodeInfo)
	if err != nil {
		return err
	}
	builder.appendStep(lastNodeId)

	// if some table references to this table
	if enabled && len(delCtx.tableDef.RefChildTbls) > 0 {
		nameTypMap := make(map[string]*plan.Type)
		idNameMap := make(map[uint64]string)
		nameIdxMap := make(map[string]int32)
		for idx, col := range delCtx.tableDef.Cols {
			nameTypMap[col.Name] = &col.Typ
			idNameMap[col.ColId] = col.Name
			nameIdxMap[col.Name] = int32(idx)
		}
		baseProject := getProjectionByLastNode(builder, lastNodeId)

		for _, tableId := range delCtx.tableDef.RefChildTbls {
			// stmt: delete p, c from child_tbl c join parent_tbl p on c.pid = p.id , skip
			if _, existInDelTable := delCtx.allDelTableIDs[tableId]; existInDelTable {
				continue
			}

			//delete data in parent table may trigger some actions in the child table
			var childObjRef *ObjectRef
			var childTableDef *TableDef
			if tableId == 0 {
				//fk self refer
				childObjRef = delCtx.objRef
				childTableDef = delCtx.tableDef
			} else {
				childObjRef, childTableDef = builder.compCtx.ResolveById(tableId, nil)
			}
			childPosMap := make(map[string]int32)
			childTypMap := make(map[string]*plan.Type)
			childId2name := make(map[uint64]string)
			childProjectList := make([]*Expr, len(childTableDef.Cols))
			childForJoinProject := make([]*Expr, len(childTableDef.Cols))
			childRowIdPos := -1
			for idx, col := range childTableDef.Cols {
				childPosMap[col.Name] = int32(idx)
				childTypMap[col.Name] = &col.Typ
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
				//child table fk self refer
				//if the child table in the delete table list, something must be done
				fkSelfReferCond := fk.ForeignTbl == 0 &&
					childTableDef.TblId == delCtx.tableDef.TblId
				if fk.ForeignTbl == delCtx.tableDef.TblId || fkSelfReferCond {
					// update stmt: update the columns do not contain ref key, skip
					updateRefColumn := make(map[string]int32)
					if isUpdate {
						for _, colId := range fk.ForeignCols {
							updateName := idNameMap[colId]
							if uIdx, ok := delCtx.updateColPosMap[updateName]; ok {
								updateRefColumn[updateName] = int32(uIdx)
							}
						}
						if len(updateRefColumn) == 0 {
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
					childTablePkMap := make(map[string]struct{})
					for _, name := range childTableDef.Pkey.Names {
						childTablePkMap[name] = struct{}{}
					}
					var updatePk bool
					for i, colId := range fk.Cols {
						for _, col := range childTableDef.Cols {
							if col.ColId == colId {
								childColumnName := col.Name
								originColumnName := idNameMap[fk.ForeignCols[i]]

								leftExpr := &Expr{
									Typ: *nameTypMap[originColumnName],
									Expr: &plan.Expr_Col{
										Col: &plan.ColRef{
											RelPos: 0,
											ColPos: nameIdxMap[originColumnName],
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
									Typ: *childTypMap[childColumnName],
									Expr: &plan.Expr_Col{
										Col: &plan.ColRef{
											RelPos: 1,
											ColPos: childPosMap[childColumnName],
											Name:   childColumnName,
										},
									},
								}
								updateChildColPosMap[childColumnName] = childColLength + i
								if _, exists := childTablePkMap[childColumnName]; exists {
									updatePk = true
								}
								condExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{leftExpr, rightExpr})
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
						if col.Name != catalog.Row_ID && col.Name != catalog.CPrimaryKeyColName {
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
					// deal with case:  update t1 set a = a.  then do not need to check constraint
					if isUpdate {
						var filterExpr, tmpExpr *Expr
						for updateName, newIdx := range updateRefColumn {
							oldIdx := nameIdxMap[updateName]
							tmpExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "!=", []*Expr{{
								Typ: *nameTypMap[updateName],
								Expr: &plan.Expr_Col{
									Col: &ColRef{
										ColPos: oldIdx,
										Name:   updateName,
									},
								},
							}, {
								Typ: *nameTypMap[updateName],
								Expr: &plan.Expr_Col{
									Col: &ColRef{
										ColPos: newIdx,
										Name:   updateName,
									},
								},
							}})
							if err != nil {
								return nil
							}
							if filterExpr == nil {
								filterExpr = tmpExpr
							} else {
								filterExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*Expr{filterExpr, tmpExpr})
								if err != nil {
									return nil
								}
							}
						}
						lastNodeId = builder.appendNode(&plan.Node{
							NodeType:   plan.Node_FILTER,
							Children:   []int32{lastNodeId},
							FilterList: []*Expr{filterExpr},
						}, bindCtx)
					}

					switch refAction {
					case plan.ForeignKeyDef_NO_ACTION, plan.ForeignKeyDef_RESTRICT, plan.ForeignKeyDef_SET_DEFAULT:
						// plan : sink_scan -> join(f1 semi join c1 & get f1's col) -> filter(assert(isempty(f1's col)))
						/*
							CORNER CASE: for the reason of the deep copy
								create table t1(a int unique key,b int, foreign key fk1(b) references t1(a));
								insert into t1 values (1,1);
								insert into t1 values (2,1);
								insert into t1 values (3,2);

								update t1 set a = NULL where a = 4;
								--> ERROR 20101 (HY000): internal error: unexpected input batch for column expression
						*/
						copiedTableDef := DeepCopyTableDef(childTableDef, true)
						rightId := builder.appendNode(&plan.Node{
							NodeType:    plan.Node_TABLE_SCAN,
							Stats:       &plan.Stats{},
							ObjRef:      childObjRef,
							TableDef:    copiedTableDef,
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
						isEmptyExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "isempty", []*Expr{colExpr})
						if err != nil {
							return err
						}
						assertExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "assert", []*Expr{isEmptyExpr, errExpr})
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
							TableDef:    DeepCopyTableDef(childTableDef, true),
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
								Expr: &plan.Expr_Lit{
									Lit: &Const{
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

						lastNodeId = appendAggNodeForFkJoin(builder, bindCtx, lastNodeId)

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
						upPlanCtx.updatePkCol = updatePk

						err = buildUpdatePlans(ctx, builder, bindCtx, upPlanCtx, false)
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

						//skip cascade for fk self refer
						if !fkSelfReferCond {
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
								lastNodeId = appendAggNodeForFkJoin(builder, bindCtx, lastNodeId)
								newSourceStep := builder.appendStep(lastNodeId)

								upPlanCtx := getDmlPlanCtx()
								upPlanCtx.objRef = childObjRef
								upPlanCtx.tableDef = DeepCopyTableDef(childTableDef, true)
								upPlanCtx.updateColLength = len(rightConds)
								upPlanCtx.isMulti = false
								upPlanCtx.rowIdPos = childRowIdPos
								upPlanCtx.sourceStep = newSourceStep
								upPlanCtx.beginIdx = 0
								upPlanCtx.updateColPosMap = updateChildColPosMap
								upPlanCtx.insertColPos = insertColPos
								upPlanCtx.allDelTableIDs = map[uint64]struct{}{}
								upPlanCtx.isFkRecursionCall = true
								upPlanCtx.updatePkCol = updatePk

								err = buildUpdatePlans(ctx, builder, bindCtx, upPlanCtx, false)
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
	}

	return nil
}

// appendAggNodeForFkJoin append agg node. to deal with these case:
// create table f (a int, b int, primary key(a,b));
// insert into f values (1,1),(1,2),(1,3),(2,3);
// create table c (a int primary key, f_a int, constraint fa_ck foreign key(f_a) REFERENCES f(a) on delete SET NULL on update SET NULL);
// insert into c values (1,1),(2,1),(3,2);
// update f set a = 10 where b=1;    we need update c only once for 2 rows. not three times for 6 rows.
func appendAggNodeForFkJoin(builder *QueryBuilder, bindCtx *BindContext, lastNodeId int32) int32 {
	groupByList := getProjectionByLastNode(builder, lastNodeId)
	aggProject := make([]*Expr, len(groupByList))
	for i, e := range groupByList {
		aggProject[i] = &Expr{
			Typ: e.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: -2,
					ColPos: int32(i),
				},
			},
		}
	}
	lastNodeId = builder.appendNode(&Node{
		NodeType:    plan.Node_AGG,
		GroupBy:     groupByList,
		Children:    []int32{lastNodeId},
		ProjectList: aggProject,
	}, bindCtx)
	lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)

	return lastNodeId
}

// makeOneInsertPlan generates plan branch for insert one table
// sink_scan -> lock -> insert
// sink_scan -> join -> filter (if table have fk. then append join node & filter node)
// sink_scan -> Fuzzyfilter -- (if need to check pk duplicate)
// table_scan -----^
func makeOneInsertPlan(
	ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, objRef *ObjectRef, tableDef *TableDef,
	updateColLength int, sourceStep int32, addAffectedRows bool, isFkRecursionCall bool, updatePkCol bool,
	pkFilterExprs []*Expr, partitionExpr *Expr, ifExistAutoPkCol bool, ifCheckPkDup bool, ifInsertFromUnique bool,
	indexSourceColTypes []*plan.Type, fuzzymessage *OriginTableMessageForFuzzy,
) (err error) {

	// make plan : sink_scan -> lock -> insert
	appendPureInsertBranch(ctx, builder, bindCtx, objRef, tableDef, sourceStep, addAffectedRows)

	// if table have fk. then append join node & filter node
	// sink_scan -> join -> filter
	if err = appendForeignConstrantPlan(builder, bindCtx, tableDef, objRef, sourceStep, isFkRecursionCall); err != nil {
		return err
	}

	// there will be some cases that no need to check if primary key is duplicate
	//  case 1: For SQL that contains on duplicate update
	//  case 2: the only primary key is auto increment type
	//  case 3: create hidden table for secondary index

	isSecondaryHidden := strings.Contains(tableDef.Name, catalog.SecondaryIndexTableNamePrefix)
	if isSecondaryHidden {
		return nil
	}

	if ifCheckPkDup {
		if err = appendPrimaryConstraintPlan(builder, bindCtx, tableDef, objRef, partitionExpr, pkFilterExprs,
			indexSourceColTypes, sourceStep, updateColLength > 0, updatePkCol, ifInsertFromUnique, fuzzymessage); err != nil {
			return err
		}
	}

	return nil
}

// appendPureInsertBranch appends the pure insert branch to the query builder.
// It includes the sink scan node, project node (if necessary), and insert node.
// The last node ID of the branch is returned.
func appendPureInsertBranch(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, objRef *ObjectRef, tableDef *TableDef, sourceStep int32, addAffectedRows bool) {
	lastNodeId := appendSinkScanNode(builder, bindCtx, sourceStep)

	// Get table partition information
	paritionTableIds, paritionTableNames := getPartTableIdsAndNames(ctx, objRef, tableDef)
	partitionIdx := -1

	// append project node if necessary
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
		TableDef: tableDef,
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

// makeOneDeletePlan
// lock -> delete
func makeOneDeletePlan(
	builder *QueryBuilder,
	bindCtx *BindContext,
	lastNodeId int32,
	delNodeInfo *deleteNodeInfo,
	isUK bool, // is delete unique key hidden table
	isSK bool,
	canTruncate bool,
) (int32, error) {
	if isUK || isSK {

		// For the hidden table of the secondary index, there will be no null situation, only unique key hidden table need this filter
		if isUK {
			// append filter
			rowIdTyp := types.T_Rowid.ToType()
			rowIdColExpr := &plan.Expr{
				Typ: makePlan2Type(&rowIdTyp),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						ColPos: int32(delNodeInfo.deleteIndex),
					},
				},
			}
			filterExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "is_not_null", []*Expr{rowIdColExpr})
			if err != nil {
				return -1, err
			}
			filterNode := &Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{lastNodeId},
				FilterList: []*plan.Expr{filterExpr},
			}
			lastNodeId = builder.appendNode(filterNode, bindCtx)
			// append lock
			lockTarget := &plan.LockTarget{
				TableId:            delNodeInfo.tableDef.TblId,
				PrimaryColIdxInBat: int32(delNodeInfo.pkPos),
				PrimaryColTyp:      delNodeInfo.pkTyp,
				RefreshTsIdxInBat:  -1, //unsupport now
				// FilterColIdxInBat:  int32(delNodeInfo.partitionIdx),
				LockTable: delNodeInfo.lockTable,
			}
			// if delNodeInfo.tableDef.Partition != nil {
			// 	lockTarget.IsPartitionTable = true
			// 	lockTarget.PartitionTableIds = delNodeInfo.partTableIDs
			// }
			lockNode := &Node{
				NodeType:    plan.Node_LOCK_OP,
				Children:    []int32{lastNodeId},
				LockTargets: []*plan.LockTarget{lockTarget},
			}
			lastNodeId = builder.appendNode(lockNode, bindCtx)
		}
	}
	truncateTable := &plan.TruncateTable{}
	if canTruncate {
		tableDef := delNodeInfo.tableDef
		truncateTable.Table = tableDef.Name
		truncateTable.TableId = tableDef.TblId
		truncateTable.Database = delNodeInfo.objRef.SchemaName
		truncateTable.IndexTableNames = delNodeInfo.indexTableNames
		truncateTable.PartitionTableNames = delNodeInfo.partTableNames
		truncateTable.ForeignTbl = delNodeInfo.foreignTbl
		truncateTable.ClusterTable = &plan.ClusterTable{
			IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
		}
		truncateTable.IsDelete = true
	}
	// append delete node
	deleteNode := &Node{
		NodeType: plan.Node_DELETE,
		Children: []int32{lastNodeId},
		// ProjectList: getProjectionByLastNode(builder, lastNodeId),
		DeleteCtx: &plan.DeleteCtx{
			TableDef:            delNodeInfo.tableDef,
			RowIdIdx:            int32(delNodeInfo.deleteIndex),
			Ref:                 delNodeInfo.objRef,
			CanTruncate:         canTruncate,
			AddAffectedRows:     delNodeInfo.addAffectedRows,
			IsClusterTable:      delNodeInfo.IsClusterTable,
			PartitionTableIds:   delNodeInfo.partTableIDs,
			PartitionTableNames: delNodeInfo.partTableNames,
			PartitionIdx:        int32(delNodeInfo.partitionIdx),
			PrimaryKeyIdx:       int32(delNodeInfo.pkPos),
			TruncateTable:       truncateTable,
		},
	}
	lastNodeId = builder.appendNode(deleteNode, bindCtx)

	return lastNodeId, nil
}

func getProjectionByLastNodeForRightJoin(builder *QueryBuilder, lastNodeId int32) []*Expr {
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
					RelPos: 1,
					ColPos: int32(i),
					Name:   name,
				},
			},
		}
	}
	return projection
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
					RelPos: lastNode.BindingTags[0],
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

func haveSecondaryKey(tableDef *TableDef) bool {
	for _, indexdef := range tableDef.Indexes {
		if !indexdef.Unique && indexdef.TableExist {
			return true
		}
	}
	return false
}

// Check if the unique key is the primary key of the table
// When the unqiue key meet the following conditions, it is the primary key of the table
// 1. There is no primary key in the table.
// 2. The unique key is the only unique key of the table.
// 3. The columns of the unique key are not null by default.
func isPrimaryKey(tableDef *TableDef, colNames []string) bool {
	// Ensure there is no real primary key in the table.
	// FakePrimaryKeyColName is for tables without a primary key.
	// So we need to exclude FakePrimaryKeyColName.
	if len(tableDef.Pkey.Names) != 1 {
		return false
	}
	if tableDef.Pkey.Names[0] != catalog.FakePrimaryKeyColName {
		return false
	}
	// Ensure the unique key is the only unique key of the table.
	uniqueKeyCount := 0
	for _, indexdef := range tableDef.Indexes {
		if indexdef.Unique {
			uniqueKeyCount++
		}
	}
	// All the columns of the unique key are not null by default.
	if uniqueKeyCount == 1 {
		for _, col := range tableDef.Cols {
			for _, colName := range colNames {
				if col.Name == colName {
					if col.Default.NullAbility {
						return false
					}
				}
			}
		}
		return true
	}
	return false
}

// Check if the unique key is the multiple primary key of the table
// When the unique key contains more than one column, it is the multiple primary key of the table.
func isMultiplePriKey(indexdef *plan.IndexDef) bool {
	return len(indexdef.Parts) > 1
}

// makeDeleteNodeInfo Get `DeleteNode` based on TableDef
func makeDeleteNodeInfo(ctx CompilerContext, objRef *ObjectRef, tableDef *TableDef,
	deleteIdx int, partitionIdx int, addAffectedRows bool, pkPos int, pkTyp Type, lockTable bool, partitionInfos map[uint64]*partSubTableInfo) *deleteNodeInfo {
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
		if partSubs := partitionInfos[tableDef.GetTblId()]; partSubs != nil {
			delNodeInfo.partTableIDs = partSubs.partTableIDs
			delNodeInfo.partTableNames = partSubs.partTableNames
		} else {
			partTableIds := make([]uint64, tableDef.Partition.PartitionNum)
			partTableNames := make([]string, tableDef.Partition.PartitionNum)
			for i, partition := range tableDef.Partition.Partitions {
				_, partTableDef := ctx.Resolve(objRef.SchemaName, partition.PartitionTableName, nil)
				partTableIds[i] = partTableDef.TblId
				partTableNames[i] = partition.PartitionTableName
			}
			delNodeInfo.partTableIDs = partTableIds
			delNodeInfo.partTableNames = partTableNames
			partitionInfos[tableDef.GetTblId()] = &partSubTableInfo{
				partTableIDs:   partTableIds,
				partTableNames: partTableNames,
			}
		}

	}
	if tableDef.Fkeys != nil {
		for _, fk := range tableDef.Fkeys {
			delNodeInfo.foreignTbl = append(delNodeInfo.foreignTbl, fk.ForeignTbl)
		}
	}
	if tableDef.Indexes != nil {
		for _, indexdef := range tableDef.Indexes {
			if indexdef.TableExist {
				if catalog.IsRegularIndexAlgo(indexdef.IndexAlgo) {
					delNodeInfo.indexTableNames = append(delNodeInfo.indexTableNames, indexdef.IndexTableName)
				} else if catalog.IsIvfIndexAlgo(indexdef.IndexAlgo) {
					// apply deletes only for entries table.
					if indexdef.IndexAlgoTableType == catalog.SystemSI_IVFFLAT_TblType_Entries {
						delNodeInfo.indexTableNames = append(delNodeInfo.indexTableNames, indexdef.IndexTableName)
					}
				} else if catalog.IsMasterIndexAlgo(indexdef.IndexAlgo) {
					delNodeInfo.indexTableNames = append(delNodeInfo.indexTableNames, indexdef.IndexTableName)
				}
			}
		}
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
			_, partTableDef := ctx.Resolve(objRef.SchemaName, partition.PartitionTableName, nil)
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
		TableDef:    &TableDef{Name: bindCtx.cteName},
	}
	b := bindCtx.bindings[0]
	sinkScanNode.TableDef.Cols = make([]*ColDef, len(b.cols))
	for i, col := range b.cols {
		sinkScanNode.TableDef.Cols[i] = &ColDef{
			Name:   col,
			Hidden: b.colIsHidden[i],
			Typ:    *b.types[i],
		}
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
		TableDef:    &TableDef{Name: bindCtx.cteName},
	}
	b := bindCtx.bindings[0]
	recursiveScanNode.TableDef.Cols = make([]*ColDef, len(b.cols))
	for i, col := range b.cols {
		recursiveScanNode.TableDef.Cols[i] = &ColDef{
			Name:   col,
			Hidden: b.colIsHidden[i],
			Typ:    *b.types[i],
		}
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

// func appendFuzzyFilterByColExpf(builder *QueryBuilder, bindCtx *BindContext, lastNodeId int32) (int32, error) {
// 	fuzzyFilterNode := &Node{
// 		NodeType:    plan.Node_FUZZY_FILTER,
// 		Children:    []int32{lastNodeId},
// 	}
// 	lastNodeId = builder.appendNode(fuzzyFilterNode, bindCtx)
// 	return lastNodeId, nil
// }

func appendAggCountGroupByColExpr(builder *QueryBuilder, bindCtx *BindContext, lastNodeId int32, colExpr *plan.Expr) (int32, error) {
	aggExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "starcount", []*Expr{colExpr})
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

func getPkPos(tableDef *TableDef, ignoreFakePK bool) (int, Type) {
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
	return -1, Type{}
}

func getRowIdPos(tableDef *TableDef) int {
	for i, col := range tableDef.Cols {
		if col.Name == catalog.Row_ID {
			return i
		}
	}
	return -1
}

func getHiddenColumnForPreInsert(tableDef *TableDef) ([]Type, []string) {
	var typs []Type
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

func appendJoinNodeForParentFkCheck(builder *QueryBuilder, bindCtx *BindContext, objRef *ObjectRef, tableDef *TableDef, baseNodeId int32) (int32, error) {
	typMap := make(map[string]plan.Type)
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
					condExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "isnotnull", []*Expr{colExpr})
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
		if fk.ForeignTbl == 0 {
			//skip fk self refer
			continue
		}

		fkeyId2Idx := make(map[uint64]int)
		for i, colId := range fk.ForeignCols {
			fkeyId2Idx[colId] = i
		}

		parentObjRef, parentTableDef := builder.compCtx.ResolveById(fk.ForeignTbl, nil)
		if parentTableDef == nil {
			return -1, moerr.NewInternalErrorf(builder.GetContext(), "parent table %d not found", fk.ForeignTbl)
		}
		newTableDef := DeepCopyTableDef(parentTableDef, false)
		joinConds := make([]*plan.Expr, 0)
		for _, col := range parentTableDef.Cols {
			if fkIdx, ok := fkeyId2Idx[col.ColId]; ok {
				rightPos := len(newTableDef.Cols)
				newTableDef.Cols = append(newTableDef.Cols, DeepCopyColDef(col))

				parentColumnName := col.Name
				childColumnName := id2name[fk.Cols[fkIdx]]

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
					Typ: col.Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 1,
							ColPos: int32(rightPos),
							Name:   parentColumnName,
						},
					},
				}
				condExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{leftExpr, rightExpr})
				if err != nil {
					return -1, err
				}
				joinConds = append(joinConds, condExpr)
			}
		}

		parentTableDef = newTableDef

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

		projectList := getProjectionByLastNode(builder, lastNodeId)

		// append project
		projectList = append(projectList, &Expr{
			Typ: parentTableDef.Cols[0].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 1,
					ColPos: 0,
					Name:   parentTableDef.Cols[0].Name,
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

	if lastNodeId == baseNodeId {
		//all fk are fk self refer
		return -1, nil
	}

	return lastNodeId, nil
}

func makeCompPkeyExpr(tableDef *plan.TableDef, name2ColIndex map[string]int32) *plan.Expr {
	if tableDef.Pkey.CompPkeyCol == nil {
		return nil
	}
	args := make([]*plan.Expr, len(tableDef.Pkey.Names))
	for i, name := range tableDef.Pkey.Names {
		colPos := name2ColIndex[name]
		args[i] = &plan.Expr{
			Typ: tableDef.Cols[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &ColRef{
					ColPos: colPos,
				},
			},
		}
	}

	typ := types.T_varchar.ToType()
	varcharTyp := MakePlan2Type(&typ)
	return &plan.Expr{
		Typ: varcharTyp,
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     function.SerialFunctionEncodeID,
					ObjName: function.SerialFunctionName,
				},
				Args: args,
			},
		},
	}
}

func makeClusterByExpr(tableDef *plan.TableDef, name2ColIndex map[string]int32) *plan.Expr {
	if tableDef.ClusterBy == nil {
		return nil
	}
	clusterBy := tableDef.ClusterBy.Name
	if clusterBy == "" || !util.JudgeIsCompositeClusterByColumn(clusterBy) {
		return nil
	}

	names := util.SplitCompositeClusterByColumnName(clusterBy)
	args := make([]*plan.Expr, len(names))
	for i, name := range names {
		colPos := name2ColIndex[name]
		args[i] = &plan.Expr{
			Typ: tableDef.Cols[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &ColRef{
					ColPos: colPos,
				},
			},
		}
	}
	typ := types.T_varchar.ToType()
	varcharTyp := MakePlan2Type(&typ)
	return &plan.Expr{
		Typ: varcharTyp,
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     function.SerialFunctionEncodeID,
					ObjName: function.SerialFunctionName,
				},
				Args: args,
			},
		},
	}
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

	name2ColIndex := make(map[string]int32, len(tableDef.Cols))
	for i, col := range tableDef.Cols {
		name2ColIndex[col.Name] = int32(i)
	}
	preInsertNode := &Node{
		NodeType:    plan.Node_PRE_INSERT,
		Children:    []int32{lastNodeId},
		ProjectList: preInsertProjection,
		PreInsertCtx: &plan.PreInsertCtx{
			Ref:           objRef,
			TableDef:      DeepCopyTableDef(tableDef, true),
			HasAutoCol:    hashAutoCol,
			IsUpdate:      isUpdate,
			CompPkeyExpr:  makeCompPkeyExpr(tableDef, name2ColIndex),
			ClusterByExpr: makeClusterByExpr(tableDef, name2ColIndex),
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

	if !isUpdate {
		if lockNodeId, ok := appendLockNode(
			builder,
			bindCtx,
			lastNodeId,
			tableDef,
			false,
			false,
			partitionIdx,
			partTableIds,
			isUpdate,
		); ok {
			lastNodeId = lockNodeId
		}
	}

	return lastNodeId
}

// appendPreInsertPlan  build preinsert plan.
// sink_scan -> [preinsert_uk | preinsert_sk] -> sink
func appendPreInsertPlan(
	builder *QueryBuilder,
	bindCtx *BindContext,
	tableDef *TableDef,
	lastNodeId int32,
	indexIdx int,
	isUpddate bool,
	uniqueTableDef *TableDef,
	isUK bool) (int32, error) {
	/********
	NOTE: make sure to make the major change applied to secondary index, to IVFFLAT index as well.
	Else IVFFLAT index would fail
	********/

	var useColumns []int32
	idxDef := tableDef.Indexes[indexIdx]
	colsMap := make(map[string]int)

	for i, col := range tableDef.Cols {
		colsMap[col.Name] = i
	}
	for _, part := range idxDef.Parts {
		part = catalog.ResolveAlias(part)
		if i, ok := colsMap[part]; ok {
			useColumns = append(useColumns, int32(i))
		}
	}

	pkColumn, originPkType := getPkPos(tableDef, false)
	lastNodeId = recomputeMoCPKeyViaProjection(builder, bindCtx, tableDef, lastNodeId, pkColumn)

	var ukType Type
	if len(idxDef.Parts) == 1 {
		ukType = tableDef.Cols[useColumns[0]].Typ
	} else {
		ukType = Type{
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
	//TODO: once everything works, rename all the UK to a more generic name that means UK and SK.
	// ie preInsert -> preInsertIKNode
	// NOTE: we have build secondary index by reusing the whole code flow of Unique Index.
	// This would be done in a separate PR after verifying the correctness of the current code.
	var preInsert *Node
	if isUK {
		preInsert = &Node{
			NodeType:    plan.Node_PRE_INSERT_UK,
			Children:    []int32{lastNodeId},
			ProjectList: preinsertUkProjection,
			PreInsertUkCtx: &plan.PreInsertUkCtx{
				Columns:  useColumns,
				PkColumn: int32(pkColumn),
				PkType:   originPkType,
				UkType:   ukType,
			},
		}
	} else {
		// NOTE: We don't defined PreInsertSkCtx. Instead, we use PreInsertUkCtx for both UK and SK since there
		// is no difference in the contents.
		preInsert = &Node{
			NodeType:    plan.Node_PRE_INSERT_SK,
			Children:    []int32{lastNodeId},
			ProjectList: preinsertUkProjection,
			PreInsertSkCtx: &plan.PreInsertUkCtx{
				Columns:  useColumns,
				PkColumn: int32(pkColumn),
				PkType:   originPkType,
				UkType:   ukType,
			},
		}
	}
	lastNodeId = builder.appendNode(preInsert, bindCtx)

	if isUK {
		if lockNodeId, ok := appendLockNode(
			builder,
			bindCtx,
			lastNodeId,
			uniqueTableDef,
			false,
			false,
			-1,
			nil,
			isUpddate,
		); ok {
			lastNodeId = lockNodeId
		}
	}

	lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
	sourceStep := builder.appendStep(lastNodeId)

	return sourceStep, nil
}

func appendDeleteIndexTablePlanWithoutFilters(
	builder *QueryBuilder,
	bindCtx *BindContext,
	uniqueObjRef *ObjectRef,
	uniqueTableDef *TableDef,
) (int32, error) {
	scanNodeProject := make([]*Expr, len(uniqueTableDef.Cols))
	for colIdx, col := range uniqueTableDef.Cols {
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
	lastNodeId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       &plan.Stats{},
		ObjRef:      uniqueObjRef,
		TableDef:    uniqueTableDef,
		ProjectList: scanNodeProject,
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
			aggExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "any_value", []*Expr{baseExpr})
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
		nullCheckExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "isnotnull", []*Expr{rowIdExpr})
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

	// lock old pk for delete statement
	partExprIdx := -1
	lastProjectList := getProjectionByLastNode(builder, lastNodeId)
	originProjectListLen := len(lastProjectList)
	if delCtx.tableDef.Partition != nil {
		partExprIdx = len(delCtx.tableDef.Cols) + delCtx.updateColLength
		lastNodeId = appendPreDeleteNode(builder, bindCtx, delCtx.objRef, delCtx.tableDef, lastNodeId)
		lastProjectList = getProjectionByLastNode(builder, lastNodeId)
	}
	pkPos, pkTyp := getPkPos(delCtx.tableDef, false)
	delNodeInfo := makeDeleteNodeInfo(ctx, delCtx.objRef, delCtx.tableDef, delCtx.rowIdPos, partExprIdx, true, pkPos, pkTyp, delCtx.lockTable, delCtx.partitionInfos)

	lockTarget := &plan.LockTarget{
		TableId:            delCtx.tableDef.TblId,
		PrimaryColIdxInBat: int32(pkPos),
		PrimaryColTyp:      pkTyp,
		RefreshTsIdxInBat:  -1,
		LockTable:          false,
	}
	if delCtx.tableDef.Partition != nil {
		lockTarget.IsPartitionTable = true
		lockTarget.FilterColIdxInBat = int32(delNodeInfo.partitionIdx)
		lockTarget.PartitionTableIds = delNodeInfo.partTableIDs
	}
	lockNode := &Node{
		NodeType:    plan.Node_LOCK_OP,
		Children:    []int32{lastNodeId},
		LockTargets: []*plan.LockTarget{lockTarget},
	}
	lastNodeId = builder.appendNode(lockNode, bindCtx)

	//lock new pk for update statement (if update pk)
	if delCtx.updateColLength > 0 && delCtx.updatePkCol && delCtx.tableDef.Pkey != nil {
		newPkPos := int32(0)
		partitionColIdx := int32(len(lastProjectList))

		// for compound primary key, we need append hidden pk column to the project list
		if delCtx.tableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
			pkColExpr := make([]*Expr, len(delCtx.tableDef.Pkey.Names))
			for i, colName := range delCtx.tableDef.Pkey.Names {
				colIdx := 0
				var colTyp *Type
				if idx, exists := delCtx.updateColPosMap[colName]; exists {
					colIdx = idx
					colTyp = &lastProjectList[idx].Typ
				} else {
					for idx, col := range delCtx.tableDef.Cols {
						if col.Name == colName {
							colIdx = idx
							colTyp = &col.Typ
							break
						}
					}
				}
				pkColExpr[i] = &Expr{
					Typ:  *colTyp,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(colIdx)}},
				}
			}
			cpPkExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", pkColExpr)
			if err != nil {
				return -1, err
			}
			lastProjectList = append(lastProjectList, cpPkExpr)
			// if table have partition, we need append partition expr to projectList
			if delCtx.tableDef.Partition != nil {
				partitionExpr := DeepCopyExpr(delCtx.tableDef.Partition.PartitionExpression)
				resetPartitionExprPos(partitionExpr, delCtx.tableDef, delCtx.updateColPosMap)
				lastProjectList = append(lastProjectList, partitionExpr)
			}
			projNode := &Node{
				NodeType:    plan.Node_PROJECT,
				Children:    []int32{lastNodeId},
				ProjectList: lastProjectList,
			}
			lastNodeId = builder.appendNode(projNode, bindCtx)

			newPkPos = partitionColIdx
			partitionColIdx += 1
		} else {
			// one pk col, just use update pos
			for k, v := range delCtx.updateColPosMap {
				if k == delCtx.tableDef.Pkey.PkeyColName {
					newPkPos = int32(v)
					break
				}
			}
			// if table have partition, we need append project node to get partition
			if delCtx.tableDef.Partition != nil {
				partitionExpr := DeepCopyExpr(delCtx.tableDef.Partition.PartitionExpression)
				resetPartitionExprPos(partitionExpr, delCtx.tableDef, delCtx.updateColPosMap)
				lastProjectList := append(lastProjectList, partitionExpr)
				projNode := &Node{
					NodeType:    plan.Node_PROJECT,
					Children:    []int32{lastNodeId},
					ProjectList: lastProjectList,
				}
				lastNodeId = builder.appendNode(projNode, bindCtx)
			}
		}

		lockTarget := &plan.LockTarget{
			TableId:            delCtx.tableDef.TblId,
			PrimaryColIdxInBat: newPkPos,
			PrimaryColTyp:      pkTyp,
			RefreshTsIdxInBat:  -1, //unsupport now
			LockTable:          false,
		}
		if delCtx.tableDef.Partition != nil {
			lockTarget.IsPartitionTable = true
			lockTarget.FilterColIdxInBat = partitionColIdx
			lockTarget.PartitionTableIds = delNodeInfo.partTableIDs
		}
		lockNode := &Node{
			NodeType:    plan.Node_LOCK_OP,
			Children:    []int32{lastNodeId},
			LockTargets: []*plan.LockTarget{lockTarget},
		}
		lastNodeId = builder.appendNode(lockNode, bindCtx)
	}

	if len(lastProjectList) > originProjectListLen {
		projectList := lastProjectList[0:originProjectListLen]
		projNode := &Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{lastNodeId},
			ProjectList: projectList,
		}
		lastNodeId = builder.appendNode(projNode, bindCtx)
	}

	return lastNodeId, nil
}

func resetPartitionExprPos(expr *Expr, tableDef *TableDef, updateColPos map[string]int) {
	colPos := make(map[int32]int32)
	for idx, col := range tableDef.Cols {
		if newIdx, exists := updateColPos[col.Name]; exists {
			colPos[int32(idx)] = int32(newIdx)
		} else {
			colPos[int32(idx)] = int32(idx)
		}
	}
	resetColPos(expr, colPos)
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

func resetColPos(expr *Expr, colPos map[int32]int32) {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		e.Col.ColPos = colPos[e.Col.ColPos]
	case *plan.Expr_F:
		for _, arg := range e.F.Args {
			resetColPos(arg, colPos)
		}
	}
}

func appendLockNode(
	builder *QueryBuilder,
	bindCtx *BindContext,
	lastNodeId int32,
	tableDef *TableDef,
	lockTable bool,
	block bool,
	partitionIdx int,
	partTableIDs []uint64,
	isUpdate bool,
) (int32, bool) {
	if !isUpdate && tableDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return -1, false
	}
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
	recursive      bool
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
		if len(meta.scans) == 1 && !meta.scans[0].preNodeIsUnion && !meta.scans[0].recursive {
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
					if len(qry.Nodes[sinkScanNodeId].SourceStep) > 1 {
						qry.Nodes[sinkScanNodeId].SourceStep[0] = int32(newStepIdx)
					} else {
						qry.Nodes[sinkScanNodeId].SourceStep = []int32{int32(newStepIdx)}
					}
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
	} else if node.NodeType == plan.Node_SINK_SCAN || node.NodeType == plan.Node_RECURSIVE_CTE || node.NodeType == plan.Node_RECURSIVE_SCAN {
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
			recursive:      len(node.SourceStep) > 1 || node.NodeType == plan.Node_RECURSIVE_CTE,
		}
		sinks[sinkNodeId].scans = append(sinks[sinkNodeId].scans, meta)
	}

	for _, childId := range node.Children {
		collectSinkAndSinkScanMeta(qry, sinks, oldStep, childId, nodeId)
	}

}

// constraintNameAreWhiteSpaces does not include empty name
func constraintNameAreWhiteSpaces(constraint string) bool {
	return len(constraint) != 0 && len(strings.TrimSpace(constraint)) == 0
}

// GenConstraintName yields uuid for the constraint name
func GenConstraintName() string {
	constraintId, _ := uuid.NewV7()
	return constraintId.String()
}

// adjustConstraintName updates a suitable name for the constraint.
// throw error if the user input all white space name.
// regenerate a new name if the user input nothing.
func adjustConstraintName(ctx context.Context, def *tree.ForeignKey) error {
	//user add a constraint name
	if constraintNameAreWhiteSpaces(def.ConstraintSymbol) {
		return moerr.NewErrWrongNameForIndex(ctx, def.ConstraintSymbol)
	} else {
		if len(def.ConstraintSymbol) == 0 {
			def.ConstraintSymbol = GenConstraintName()
		}
	}
	return nil
}

func runSql(ctx CompilerContext, sql string) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(ctx.GetProcess().GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}
	proc := ctx.GetProcess()
	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithDatabase(proc.GetSessionInfo().Database).
		WithTimeZone(proc.GetSessionInfo().TimeZone).
		WithAccountID(proc.GetSessionInfo().AccountId)
	return exec.Exec(proc.GetTopContext(), sql, opts)
}

/*
Example on FkReferKey and FkReferDef:

	In database `test`:

		create table t1(a int,primary key(a));

		create table t2(b int, constraint c1 foreign key(b) references t1(a));

	So, the structure FkReferDef below denotes such relationships : test.t2(b) -> test.t1(a)
	FkReferKey holds : db = test, tbl = t2

*/

// FkReferKey holds the database and table name of the foreign key
type FkReferKey struct {
	Db  string //fk database name
	Tbl string //fk table name
}

// FkReferDef holds the definition & details of the foreign key
type FkReferDef struct {
	Db       string //fk database name
	Tbl      string //fk table name
	Name     string //fk constraint name
	Col      string //fk column name, letter case: lower
	ReferCol string //referenced column name, letter case: lower
	OnDelete string //on delete action
	OnUpdate string //on update action
}

func (fk FkReferDef) String() string {
	return fmt.Sprintf("%s.%s %s %s => %s",
		fk.Db, fk.Tbl, fk.Name, fk.Col, fk.ReferCol)
}

// GetSqlForFkReferredTo returns the query that retrieves the fk relationships
// that refer to the table
func GetSqlForFkReferredTo(db, table string) string {
	return fmt.Sprintf(
		"select "+
			"db_name, "+
			"table_name, "+
			"constraint_name, "+
			"column_name, "+
			"refer_column_name, "+
			"on_delete, "+
			"on_update "+
			"from "+
			"`mo_catalog`.`mo_foreign_keys` "+
			"where "+
			"refer_db_name = '%s' and refer_table_name = '%s' "+
			" and "+
			"(db_name != '%s' or db_name = '%s' and table_name != '%s') "+
			"order by db_name, table_name, constraint_name;",
		db, table, db, db, table)
}

// GetFkReferredTo returns the foreign key relationships that refer to the table
func GetFkReferredTo(ctx CompilerContext, db, table string) (map[FkReferKey]map[string][]*FkReferDef, error) {
	//exclude fk self reference
	sql := GetSqlForFkReferredTo(db, table)
	res, err := runSql(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	ret := make(map[FkReferKey]map[string][]*FkReferDef)
	const dbIdx = 0
	const tblIdx = 1
	const nameIdx = 2
	const colIdx = 3
	const referColIdx = 4
	const deleteIdx = 5
	const updateIdx = 6
	if res.Batches != nil {
		for _, batch := range res.Batches {
			if batch != nil &&
				batch.Vecs[0] != nil &&
				batch.Vecs[0].Length() > 0 {
				for i := 0; i < batch.Vecs[0].Length(); i++ {
					fk := &FkReferDef{
						Db:       string(batch.Vecs[dbIdx].GetBytesAt(i)),
						Tbl:      string(batch.Vecs[tblIdx].GetBytesAt(i)),
						Name:     string(batch.Vecs[nameIdx].GetBytesAt(i)),
						Col:      string(batch.Vecs[colIdx].GetBytesAt(i)),
						ReferCol: string(batch.Vecs[referColIdx].GetBytesAt(i)),
						OnDelete: string(batch.Vecs[deleteIdx].GetBytesAt(i)),
						OnUpdate: string(batch.Vecs[updateIdx].GetBytesAt(i)),
					}
					key := FkReferKey{Db: fk.Db, Tbl: fk.Tbl}
					var constraint map[string][]*FkReferDef
					var ok bool
					if constraint, ok = ret[key]; !ok {
						constraint = make(map[string][]*FkReferDef)
						ret[key] = constraint
					}
					constraint[fk.Name] = append(constraint[fk.Name], fk)
				}
			}
		}
	}
	return ret, nil
}

func convertIntoReferAction(s string) plan.ForeignKeyDef_RefAction {
	switch strings.ToLower(s) {
	case "cascade":
		return plan.ForeignKeyDef_CASCADE
	case "restrict":
		return plan.ForeignKeyDef_RESTRICT
	case "set null":
		fallthrough
	case "set_null":
		return plan.ForeignKeyDef_SET_NULL
	case "no_action":
		fallthrough
	case "no action":
		return plan.ForeignKeyDef_NO_ACTION
	case "set_default":
		fallthrough
	case "set default":
		return plan.ForeignKeyDef_SET_DEFAULT
	default:
		return plan.ForeignKeyDef_RESTRICT
	}
}

// getSqlForAddFk returns the insert sql that adds a fk relationship
// into the mo_foreign_keys table
func getSqlForAddFk(db, table string, data *FkData) string {
	row := make([]string, 16)
	rows := 0
	sb := strings.Builder{}
	sb.WriteString("insert into `mo_catalog`.`mo_foreign_keys`  ")
	sb.WriteString(" values ")
	for childIdx, childCol := range data.Cols.Cols {
		row[0] = data.Def.Name
		row[1] = "0"
		row[2] = db
		row[3] = "0"
		row[4] = table
		row[5] = "0"
		row[6] = childCol
		row[7] = "0"
		row[8] = data.ParentDbName
		row[9] = "0"
		row[10] = data.ParentTableName
		row[11] = "0"
		row[12] = data.ColsReferred.Cols[childIdx]
		row[13] = "0"
		row[14] = data.Def.OnDelete.String()
		row[15] = data.Def.OnUpdate.String()
		{
			if rows > 0 {
				sb.WriteByte(',')
			}
			rows++
			sb.WriteByte('(')
			for j, col := range row {
				if j > 0 {
					sb.WriteByte(',')
				}
				sb.WriteByte('\'')
				sb.WriteString(col)
				sb.WriteByte('\'')
			}
			sb.WriteByte(')')
		}
	}
	return sb.String()
}

// getSqlForDeleteTable returns the delete sql that deletes all the fk relationships from mo_foreign_keys
// on the table
func getSqlForDeleteTable(db, tbl string) string {
	sb := strings.Builder{}
	sb.WriteString("delete from `mo_catalog`.`mo_foreign_keys` where ")
	sb.WriteString(fmt.Sprintf(
		"db_name = '%s' and table_name = '%s'", db, tbl))
	return sb.String()
}

// getSqlForDeleteConstraint returns the delete sql that deletes the fk constraint from mo_foreign_keys
// on the table
func getSqlForDeleteConstraint(db, tbl, constraint string) string {
	sb := strings.Builder{}
	sb.WriteString("delete from `mo_catalog`.`mo_foreign_keys` where ")
	sb.WriteString(fmt.Sprintf(
		"constraint_name = '%s' and db_name = '%s' and table_name = '%s'",
		constraint, db, tbl))
	return sb.String()
}

// getSqlForDeleteDB returns the delete sql that deletes all the fk relationships from mo_foreign_keys
// on the database
func getSqlForDeleteDB(db string) string {
	sb := strings.Builder{}
	sb.WriteString("delete from `mo_catalog`.`mo_foreign_keys` where ")
	sb.WriteString(fmt.Sprintf("db_name = '%s'", db))
	return sb.String()
}

// getSqlForRenameTable returns the sqls that rename the table of all fk relationships in mo_foreign_keys
func getSqlForRenameTable(db, oldName, newName string) (ret []string) {
	sb := strings.Builder{}
	sb.WriteString("update `mo_catalog`.`mo_foreign_keys` ")
	sb.WriteString(fmt.Sprintf("set table_name = '%s' ", newName))
	sb.WriteString(fmt.Sprintf("where db_name = '%s' and table_name = '%s' ; ", db, oldName))
	ret = append(ret, sb.String())

	sb.Reset()
	sb.WriteString("update `mo_catalog`.`mo_foreign_keys` ")
	sb.WriteString(fmt.Sprintf("set refer_table_name = '%s' ", newName))
	sb.WriteString(fmt.Sprintf("where refer_db_name = '%s' and refer_table_name = '%s' ; ", db, oldName))
	ret = append(ret, sb.String())
	return
}

// getSqlForRenameColumn returns the sqls that rename the column of all fk relationships in mo_foreign_keys
func getSqlForRenameColumn(db, table, oldName, newName string) (ret []string) {
	sb := strings.Builder{}
	sb.WriteString("update `mo_catalog`.`mo_foreign_keys` ")
	sb.WriteString(fmt.Sprintf("set column_name = '%s' ", newName))
	sb.WriteString(fmt.Sprintf("where db_name = '%s' and table_name = '%s' and column_name = '%s' ; ",
		db, table, oldName))
	ret = append(ret, sb.String())

	sb.Reset()
	sb.WriteString("update `mo_catalog`.`mo_foreign_keys` ")
	sb.WriteString(fmt.Sprintf("set refer_column_name = '%s' ", newName))
	sb.WriteString(fmt.Sprintf("where refer_db_name = '%s' and refer_table_name = '%s' and refer_column_name = '%s' ; ",
		db, table, oldName))
	ret = append(ret, sb.String())
	return
}

// getSqlForCheckHasDBRefersTo returns the sql that checks if the database has any foreign key relationships
// that refer to it.
func getSqlForCheckHasDBRefersTo(db string) string {
	sb := strings.Builder{}
	sb.WriteString("select count(*) > 0 from `mo_catalog`.`mo_foreign_keys` ")
	sb.WriteString(fmt.Sprintf("where refer_db_name = '%s' and db_name != '%s';", db, db))
	return sb.String()
}

// fkBannedDatabase denotes the databases that forbid the foreign keys
// you can not define fk in these databases or
// define fk refers to these databases.
// for simplicity of the design
var fkBannedDatabase = map[string]bool{
	catalog.MO_CATALOG:         true,
	catalog.MO_SYSTEM:          true,
	catalog.MO_SYSTEM_METRICS:  true,
	catalog.MOTaskDB:           true,
	sysview.InformationDBConst: true,
	sysview.MysqlDBConst:       true,
	trace.DebugDB:              true,
}

// IsFkBannedDatabase denotes the database should not have any
// foreign keys
func IsFkBannedDatabase(db string) bool {
	if _, has := fkBannedDatabase[db]; has {
		return true
	}
	return false
}

// IsForeignKeyChecksEnabled returns the system variable foreign_key_checks is true or false
func IsForeignKeyChecksEnabled(ctx CompilerContext) (bool, error) {
	if disable := ctx.GetContext().Value(defines.DisableFkCheck{}); disable != nil {
		return !disable.(bool), nil
	}

	value, err := ctx.ResolveVariable("foreign_key_checks", true, false)
	if err != nil {
		return false, err
	}
	if value == nil {
		return true, nil
	}
	if v, ok := value.(int64); ok {
		return v == 1, nil
	} else if v1, ok := value.(int8); ok {
		return v1 == 1, nil
	} else {
		return false, moerr.NewInternalErrorf(ctx.GetContext(), "invalid  %v ", value)
	}
}
