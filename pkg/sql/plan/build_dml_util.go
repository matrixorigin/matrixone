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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

type dmlPlanCtx struct {
	objRef          *ObjectRef
	tableDef        *TableDef
	beginIdx        int
	sourceStep      int32
	isMulti         bool
	updateColLength int
	rowIdPos        int
	insertColPos    []int
	updateColPosMap map[string]int
}

func buildOnDuplicateKeyPlans(builder *QueryBuilder, bindCtx *BindContext, info *dmlSelectInfo) error {
	return nil
}

// buildInsertPlans  build insert plan.
func buildInsertPlans(
	ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext,
	objRef *ObjectRef, tableDef *TableDef, lastNodeId int32) error {

	// add plan: -> preinsert -> sink
	lastNodeId = appendPreInsertNode(builder, bindCtx, objRef, tableDef, lastNodeId, true)
	lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
	sourceStep := builder.appendStep(lastNodeId)

	// make insert plans
	insertBindCtx := NewBindContext(builder, nil)
	err := makeInsertPlan(ctx, builder, insertBindCtx, objRef, tableDef, 0, sourceStep, true)
	return err
}

// buildUpdatePlans  build update plan.
func buildUpdatePlans(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, updatePlanCtx *dmlPlanCtx) error {
	// build delete plans
	lastNodeId, err := buildDeletePlans(ctx, builder, bindCtx, updatePlanCtx)
	if err != nil {
		return err
	}

	// -> project -> preinsert -> sink
	lastNode := builder.qry.Nodes[lastNodeId]
	newCols := make([]*ColDef, 0, len(updatePlanCtx.tableDef.Cols))
	for _, col := range updatePlanCtx.tableDef.Cols {
		if col.Hidden {
			continue
		}
		newCols = append(newCols, col)
	}
	updatePlanCtx.tableDef.Cols = newCols

	projectProjection := make([]*Expr, len(updatePlanCtx.insertColPos))
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
	err = makeInsertPlan(ctx, builder, insertBindCtx, updatePlanCtx.objRef, updatePlanCtx.tableDef, updatePlanCtx.updateColLength, sourceStep, false)

	return err
}

// buildDeletePlans  build preinsert plan.
// sink_scan -> project -> predelete[build partition] -> join[unique key] ->  [u1]lock -> [u1]filter -> [u1]delete -> [o1]lock -> [o1]filter -> [o1]delete
func buildDeletePlans(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, delCtx *dmlPlanCtx) (int32, error) {
	var err error
	lastNodeId := appendSinkScanNode(builder, bindCtx, delCtx.sourceStep)
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
	if delCtx.isMulti {
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

	isPartitionTable := false
	partExprIdx := -1
	if delCtx.tableDef.Partition != nil {
		isPartitionTable = true
		partExprIdx = len(delCtx.tableDef.Cols) + delCtx.updateColLength
		lastNodeId = appendPreDeleteNode(builder, bindCtx, delCtx.objRef, delCtx.tableDef, lastNodeId)
	}

	hasUniqueKey := haveUniqueKey(delCtx.tableDef)
	if hasUniqueKey {
		// append join node to get unique key table's row_id/pk for delete
		lastNodeId, err = appendJoinNodeForGetRowIdOfUniqueKey(builder, bindCtx, delCtx.objRef, delCtx.tableDef, lastNodeId, delCtx.updateColLength)
		if err != nil {
			return -1, err
		}

		// delete unique table
		uniqueDeleteIdx := len(delCtx.tableDef.Cols) + delCtx.updateColLength
		if isPartitionTable {
			uniqueDeleteIdx = uniqueDeleteIdx + 1
		}
		for _, indexdef := range delCtx.tableDef.Indexes {
			if indexdef.Unique {
				_, idxTableDef := ctx.Resolve(delCtx.objRef.SchemaName, indexdef.IndexTableName)
				idxObjRef := &ObjectRef{
					SchemaName: delCtx.objRef.SchemaName,
					ObjName:    indexdef.IndexTableName,
					Obj:        int64(idxTableDef.TblId),
				}

				// getNode information about delete table
				delNodeInfo := makeDeleteNodeInfo(ctx, idxObjRef, idxTableDef, uniqueDeleteIdx, -1, false, false)
				lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo)
				if err != nil {
					return -1, err
				}
				uniqueDeleteIdx = uniqueDeleteIdx + 2 // row_id & pk
			}
		}
	}

	// delete origin table
	// getNode information about delete table
	isEnd := len(delCtx.insertColPos) == 0 // if it's not update plan, the delete node will be end of pipeline, we need free mem
	delNodeInfo := makeDeleteNodeInfo(ctx, delCtx.objRef, delCtx.tableDef, delCtx.rowIdPos, partExprIdx, true, isEnd)
	lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo)
	if err != nil {
		return -1, err
	}

	return lastNodeId, nil
}

// makeInsertPlan  build insert plan for one table
/**
[o]sink_scan -> lock(retry when err) -> join(join fk) -> filter -> project  -> insert [-> sink]
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
*/
func makeInsertPlan(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext,
	objRef *ObjectRef, tableDef *TableDef, updateColLength int,
	sourceStep int32, addAffectedRows bool) error {

	// append hidden column to tableDef
	if tableDef.Pkey != nil && tableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		tableDef.Cols = append(tableDef.Cols, MakeHiddenColDefByName(catalog.CPrimaryKeyColName))
	}
	if tableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
		tableDef.Cols = append(tableDef.Cols, MakeHiddenColDefByName(tableDef.ClusterBy.Name))
	}

	var lastNodeId int32
	var err error

	hasUniqueKey := haveUniqueKey(tableDef)

	// make plan : sink_scan -> lock -> join(check fk) -> filter -> insert -> [sink]
	{
		lastNodeId = appendSinkScanNode(builder, bindCtx, sourceStep)

		// todo: append lock

		// if table have fk. then append join node & filter node
		if len(tableDef.Fkeys) > 0 {
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

		}

		// append project node
		projectProjection := getProjectionByLastNode(builder, lastNodeId)
		if len(projectProjection) > len(tableDef.Cols) || tableDef.Partition != nil {
			if len(projectProjection) > len(tableDef.Cols) {
				projectProjection = projectProjection[:len(tableDef.Cols)]
			}
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
		// Get table partition information
		partitionIdx, paritionTableIds := getPartSubTableIdsAndPartIndex(ctx, objRef, tableDef)

		insertNode := &Node{
			NodeType: plan.Node_INSERT,
			Children: []int32{lastNodeId},
			ObjRef:   objRef,
			InsertCtx: &plan.InsertCtx{
				Ref:               objRef,
				AddAffectedRows:   addAffectedRows,
				IsClusterTable:    tableDef.TableType == catalog.SystemClusterRel,
				TableDef:          tableDef,
				PartitionTableIds: paritionTableIds,
				PartitionIdx:      int32(partitionIdx),
				IsEnd:             !hasUniqueKey, // if is end of pipeline. we need free mem
			},
			ProjectList: insertProjection,
		}
		lastNodeId = builder.appendNode(insertNode, bindCtx)

		if hasUniqueKey {
			// append sink
			lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
			uniqueSourceStep := builder.appendStep(lastNodeId)

			// append plan for the hidden tables of unique keys
			for i, indexdef := range tableDef.Indexes {
				if indexdef.Unique {
					idxRef, idxTableDef := ctx.Resolve(builder.compCtx.DefaultDatabase(), indexdef.IndexTableName)
					// remove row_id
					for i, col := range idxTableDef.Cols {
						if col.Name == catalog.Row_ID {
							idxTableDef.Cols = append(idxTableDef.Cols[:i], idxTableDef.Cols[i+1:]...)
							break
						}
					}

					//make preinsert_uk plan,  to build the batch to insert
					newSourceStep, err := appendPreInsertUkNode(builder, bindCtx, tableDef, uniqueSourceStep, i)
					if err != nil {
						return err
					}

					// make insert plans(for unique key table)
					err = makeInsertPlan(ctx, builder, bindCtx, idxRef, idxTableDef, 0, newSourceStep, false)
					if err != nil {
						return err
					}
				}
			}
		} else {
			builder.appendStep(lastNodeId)
		}
	}

	// todo: make plan: sink_scan -> group_by -> filter  //check if pk is unique in rows

	// todo: make plan: sink_scan -> join -> filter	// check if pk is unique in rows & snapshot

	return nil
}

// makeOneDeletePlan
// lock -> delete
func makeOneDeletePlan(builder *QueryBuilder, bindCtx *BindContext, lastNodeId int32, delNodeInfo *deleteNodeInfo) (int32, error) {
	// todo: append lock

	// append delete node
	deleteNode := &Node{
		NodeType:    plan.Node_DELETE,
		Children:    []int32{lastNodeId},
		ProjectList: getProjectionByLastNode(builder, lastNodeId),
		DeleteCtx: &plan.DeleteCtx{
			RowIdIdx:          int32(delNodeInfo.deleteIndex),
			Ref:               delNodeInfo.objRef,
			CanTruncate:       false,
			AddAffectedRows:   delNodeInfo.addAffectedRows,
			IsClusterTable:    delNodeInfo.IsClusterTable,
			PartitionTableIds: delNodeInfo.partTableIDs,
			PartitionIdx:      int32(delNodeInfo.partitionIdx),
		},
	}
	lastNodeId = builder.appendNode(deleteNode, bindCtx)

	return lastNodeId, nil
}

func getProjectionByLastNode(builder *QueryBuilder, lastNodeId int32) []*Expr {
	lastNode := builder.qry.Nodes[lastNodeId]
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
	deleteIdx int, partitionIdx int, addAffectedRows bool, isEnd bool) *deleteNodeInfo {
	delNodeInfo := &deleteNodeInfo{
		objRef:          objRef,
		tableDef:        tableDef,
		deleteIndex:     deleteIdx,
		partitionIdx:    partitionIdx,
		addAffectedRows: addAffectedRows,
		IsClusterTable:  tableDef.TableType == catalog.SystemClusterRel,
		isEnd:           isEnd,
	}

	if tableDef.Partition != nil {
		partTableIds := make([]uint64, tableDef.Partition.PartitionNum)
		for i, partition := range tableDef.Partition.Partitions {
			_, partTableDef := ctx.Resolve(objRef.SchemaName, partition.PartitionTableName)
			partTableIds[i] = partTableDef.TblId
		}
		delNodeInfo.partTableIDs = partTableIds
	}
	return delNodeInfo
}

// information of deleteNode, which is about the deleted table
type deleteNodeInfo struct {
	objRef          *ObjectRef
	tableDef        *TableDef
	IsClusterTable  bool
	deleteIndex     int      // The array index position of the rowid column
	partTableIDs    []uint64 // Align array index with the partition number
	partitionIdx    int      // The array index position of the partition expression column
	addAffectedRows bool
	isEnd           bool
}

// Calculate the offset index of partition expression, and the sub tableIds of the partition table
func getPartSubTableIdsAndPartIndex(ctx CompilerContext, objRef *ObjectRef, tableDef *TableDef) (int, []uint64) {
	var partitionIdx int
	var partTableIds []uint64
	if tableDef.Partition != nil {
		partTableIds = make([]uint64, tableDef.Partition.PartitionNum)
		for i, partition := range tableDef.Partition.Partitions {
			_, partTableDef := ctx.Resolve(objRef.SchemaName, partition.PartitionTableName)
			partTableIds[i] = partTableDef.TblId
		}
		//Calculate Partition Expression Location
		partitionIdx = len(tableDef.Cols)
	} else {
		partitionIdx = -1
	}
	return partitionIdx, partTableIds
}

func appendSinkScanNode(builder *QueryBuilder, bindCtx *BindContext, sourceStep int32) int32 {
	lastNodeId := builder.qry.Steps[sourceStep]
	// lastNode := builder.qry.Nodes[lastNodeId]
	sinkScanProject := getProjectionByLastNode(builder, lastNodeId)
	sinkScanNode := &Node{
		NodeType:    plan.Node_SINK_SCAN,
		SourceStep:  sourceStep,
		ProjectList: sinkScanProject,
	}
	lastNodeId = builder.appendNode(sinkScanNode, bindCtx)
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

func getPkPos(tableDef *TableDef) int {
	if tableDef.Pkey == nil {
		return -1
	}
	pkName := tableDef.Pkey.PkeyColName
	if pkName == catalog.CPrimaryKeyColName {
		return len(tableDef.Cols) - 1
	}
	for i, col := range tableDef.Cols {
		if col.Name == pkName {
			return i
		}
	}
	return -1
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

// appendPreDeleteNode  build predelete plan.
// sink_scan -> project -> predelete[process partition]
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

	preInsertNode := &Node{
		NodeType:    plan.Node_PRE_INSERT,
		Children:    []int32{lastNodeId},
		ProjectList: preInsertProjection,
		PreInsertCtx: &plan.PreInsertCtx{
			Ref:        objRef,
			TableDef:   tableDef,
			HasAutoCol: hashAutoCol,
			IsUpdate:   isUpdate,
		},
	}
	lastNodeId = builder.appendNode(preInsertNode, bindCtx)

	return lastNodeId
}

// appendPreInsertUkNode  build preinsert plan.
// sink_scan -> preinsert_uk -> sink
func appendPreInsertUkNode(builder *QueryBuilder, bindCtx *BindContext, tableDef *TableDef, sourceStep int32, indexIdx int) (int32, error) {
	lastNodeId := appendSinkScanNode(builder, bindCtx, sourceStep)

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

	pkColumn := int32(getPkPos(tableDef))
	var pkType *Type
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
	if pkColumn > -1 {
		preinsertUkProjection = append(preinsertUkProjection, &plan.Expr{
			Typ: tableDef.Cols[pkColumn].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: -1,
					ColPos: 1,
					Name:   catalog.IndexTableIndexColName,
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
			PkColumn: pkColumn,
			PkType:   pkType,
			UkType:   ukType,
		},
	}
	lastNodeId = builder.appendNode(preInsertUkNode, bindCtx)

	lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
	sourceStep = builder.appendStep(lastNodeId)

	return sourceStep, nil
}

func appendJoinNodeForGetRowIdOfUniqueKey(builder *QueryBuilder, bindCtx *BindContext, objRef *ObjectRef, tableDef *TableDef, baseNodeId int32, updateColLength int) (int32, error) {
	lastNodeId := baseNodeId
	typMap := make(map[string]*plan.Type)
	posMap := make(map[string]int)
	for idx, col := range tableDef.Cols {
		posMap[col.Name] = idx
		typMap[col.Name] = col.Typ
	}
	// var returnCtx *BindContext
	var err error

	for _, indexdef := range tableDef.Indexes {
		if indexdef.Unique {
			// append table_scan node
			uniqueObjRef, uniqueTableDef := builder.compCtx.Resolve(objRef.SchemaName, indexdef.IndexTableName)
			if uniqueTableDef == nil {
				return -1, moerr.NewNoSuchTable(builder.GetContext(), objRef.SchemaName, indexdef.IndexTableName)
			}
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
		}
	}

	return lastNodeId, nil
}
