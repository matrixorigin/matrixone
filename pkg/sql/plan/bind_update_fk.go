// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	planutil "github.com/matrixorigin/matrixone/pkg/sql/util"
)

const (
	foreignKeyNoReferencedRowAssert  = "fk_no_referenced_row"
	foreignKeyRowIsReferencedAssert  = "fk_row_is_referenced"
	foreignKeyAmbiguousMappingAssert = "fk_ambiguous_parent_mapping"
)

func (builder *QueryBuilder) updateInputProjectNode(nodeID int32) *plan.Node {
	node := builder.qry.Nodes[nodeID]
	for node.NodeType == plan.Node_PRE_INSERT && len(node.ProjectList) == 0 && len(node.Children) == 1 {
		node = builder.qry.Nodes[node.Children[0]]
	}
	return node
}

func (builder *QueryBuilder) appendUpdateForeignKeyChecks(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	lastNodeID int32,
	selectNodeTag int32,
	selectNode *plan.Node,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	targetRowNumberPos []int32,
	targetBranchActivePos []int32,
	deferRepeatedPhysicalTargetMerge bool,
) (int32, int32, *plan.Node, error) {
	enabled, err := IsForeignKeyChecksEnabled(builder.compCtx)
	if err != nil {
		return 0, 0, nil, err
	}
	if !enabled {
		return lastNodeID, selectNodeTag, selectNode, nil
	}

	targetCountByTableID := make(map[uint64]int)
	for i, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) > 0 {
			targetCountByTableID[dmlCtx.tableDefs[i].TblId]++
		}
	}
	handledFinalTable := make(map[uint64]struct{})
	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}
		repeatedPhysicalTarget := targetCountByTableID[tableDef.TblId] > 1
		if repeatedPhysicalTarget && !deferRepeatedPhysicalTargetMerge {
			if _, handled := handledFinalTable[tableDef.TblId]; handled {
				continue
			}
			handledFinalTable[tableDef.TblId] = struct{}{}
		}

		alias := dmlCtx.aliases[i]
		var targetSelected *plan.Expr
		if targetRowNumberPos[i] >= 0 {
			targetSelected, err = builder.buildTargetSelectedExpr(
				selectNodeTag, selectNode, targetRowNumberPos[i], targetBranchActivePos[i])
			if err != nil {
				return 0, 0, nil, err
			}
		}
		if !repeatedPhysicalTarget || !deferRepeatedPhysicalTargetMerge {
			lastNodeID, selectNodeTag, err = builder.appendUpdateParentForeignKeyChecks(
				bindCtx,
				tableDef,
				alias,
				lastNodeID,
				selectNodeTag,
				oldColName2Idx,
				newColName2Idx,
				targetSelected,
				false,
			)
			if err != nil {
				return 0, 0, nil, err
			}
			selectNode = builder.updateInputProjectNode(lastNodeID)
		}
		if targetRowNumberPos[i] >= 0 {
			targetSelected, err = builder.buildTargetSelectedExpr(
				selectNodeTag, selectNode, targetRowNumberPos[i], targetBranchActivePos[i])
			if err != nil {
				return 0, 0, nil, err
			}
		}
		affectedFks := affectedUpdateChildFks(tableDef, alias, newColName2Idx)
		if len(affectedFks) == 0 {
			continue
		}

		validatedFks := make([]*plan.ForeignKeyDef, 0, len(affectedFks))
		for _, fk := range affectedFks {
			if fk.ForeignTbl != 0 {
				validatedFks = append(validatedFks, fk)
			}
		}
		if len(validatedFks) == 0 {
			continue
		}

		fkTableDef := *tableDef
		fkTableDef.Fkeys = make([]*plan.ForeignKeyDef, len(validatedFks))
		for j, fk := range validatedFks {
			fkTableDef.Fkeys[j] = DeepCopyFkey(fk)
		}

		sourceNode := selectNode
		projLen := len(sourceNode.ProjectList)
		projectTypes := make([]plan.Type, projLen)
		for j, expr := range sourceNode.ProjectList {
			projectTypes[j] = expr.Typ
		}

		var oks []*plan.Expr
		lastNodeID, oks, err = builder.appendModernChildFkMarkOks(
			bindCtx,
			&fkTableDef,
			lastNodeID,
			selectNodeTag,
			func(colName string) int32 {
				qualifiedName := alias + "." + colName
				if pos, ok := newColName2Idx[qualifiedName]; ok {
					return pos
				}
				return oldColName2Idx[qualifiedName]
			},
		)
		if err != nil {
			return 0, 0, nil, err
		}

		assertConds := make([]*plan.Expr, len(oks))
		errExpr := makePlan2StringConstExprWithType(
			"Cannot add or update a child row: a foreign key constraint fails",
		)
		errTypeExpr := makePlan2StringConstExprWithType(foreignKeyNoReferencedRowAssert)
		for j, fk := range validatedFks {
			unchanged, buildErr := builder.buildUpdateFkUnchangedExpr(
				tableDef,
				fk,
				alias,
				selectNodeTag,
				oldColName2Idx,
				newColName2Idx,
				sourceNode,
			)
			if buildErr != nil {
				return 0, 0, nil, buildErr
			}
			ok := oks[j]
			if unchanged != nil {
				ok, err = BindFuncExprImplByPlanExpr(
					builder.GetContext(),
					"or",
					[]*plan.Expr{ok, unchanged},
				)
				if err != nil {
					return 0, 0, nil, err
				}
			}
			if targetSelected != nil {
				notSelected, buildErr := BindFuncExprImplByPlanExpr(
					builder.GetContext(), "not", []*plan.Expr{DeepCopyExpr(targetSelected)})
				if buildErr != nil {
					return 0, 0, nil, buildErr
				}
				ok, buildErr = BindFuncExprImplByPlanExpr(
					builder.GetContext(), "or", []*plan.Expr{notSelected, ok})
				if buildErr != nil {
					return 0, 0, nil, buildErr
				}
			}
			assertConds[j], err = BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"assert",
				[]*plan.Expr{ok, DeepCopyExpr(errExpr), DeepCopyExpr(errTypeExpr)},
			)
			if err != nil {
				return 0, 0, nil, err
			}
		}

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:   plan.Node_FILTER,
			Children:   []int32{lastNodeID},
			FilterList: assertConds,
		}, bindCtx)

		validatedTag := builder.genNewBindTag()
		projectList := make([]*plan.Expr, projLen)
		for j := range projectList {
			projectList[j] = &plan.Expr{
				Typ: projectTypes[j],
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: int32(j),
					},
				},
			}
		}
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: projectList,
			Children:    []int32{lastNodeID},
			BindingTags: []int32{validatedTag},
		}, bindCtx)

		selectNodeTag = validatedTag
		selectNode = builder.qry.Nodes[lastNodeID]
	}

	return lastNodeID, selectNodeTag, selectNode, nil
}

// appendMergedPhysicalTargetChildForeignKeyChecks filters UPDATE IGNORE
// candidates against the final child-key image. Alias-local checks run before
// the physical-row merge and cannot validate a composite key assembled from
// assignments made through sibling aliases.
func (builder *QueryBuilder) appendMergedPhysicalTargetChildForeignKeyChecks(
	bindCtx *BindContext,
	tableDef *plan.TableDef,
	alias string,
	lastNodeID int32,
	selectNodeTag int32,
	selectNode *plan.Node,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
) (int32, int32, *plan.Node, error) {
	enabled, err := IsForeignKeyChecksEnabled(builder.compCtx)
	if err != nil || !enabled {
		return lastNodeID, selectNodeTag, selectNode, err
	}
	affectedFks := affectedUpdateChildFks(tableDef, alias, newColName2Idx)
	validatedFks := make([]*plan.ForeignKeyDef, 0, len(affectedFks))
	for _, fk := range affectedFks {
		if fk.ForeignTbl != 0 {
			validatedFks = append(validatedFks, fk)
		}
	}
	if len(validatedFks) == 0 {
		return lastNodeID, selectNodeTag, selectNode, nil
	}

	fkTableDef := *tableDef
	fkTableDef.Fkeys = make([]*plan.ForeignKeyDef, len(validatedFks))
	for i, fk := range validatedFks {
		fkTableDef.Fkeys[i] = DeepCopyFkey(fk)
	}
	projectTypes := make([]plan.Type, len(selectNode.ProjectList))
	for i, expr := range selectNode.ProjectList {
		projectTypes[i] = expr.Typ
	}
	lastNodeID, oks, err := builder.appendModernChildFkMarkOks(
		bindCtx,
		&fkTableDef,
		lastNodeID,
		selectNodeTag,
		func(colName string) int32 {
			qualifiedName := alias + "." + colName
			if pos, updated := newColName2Idx[qualifiedName]; updated {
				return pos
			}
			return oldColName2Idx[qualifiedName]
		},
	)
	if err != nil {
		return 0, 0, nil, err
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:        plan.Node_FILTER,
		Children:        []int32{lastNodeID},
		FilterList:      oks,
		FilterIsBarrier: true,
	}, bindCtx)

	validatedTag := builder.genNewBindTag()
	projectList := make([]*plan.Expr, len(projectTypes))
	for i, typ := range projectTypes {
		projectList[i] = &plan.Expr{
			Typ: typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: selectNodeTag,
				ColPos: int32(i),
			}},
		}
	}
	selectNode = &plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		ProjectList: projectList,
		BindingTags: []int32{validatedTag},
	}
	lastNodeID = builder.appendNode(selectNode, bindCtx)
	return lastNodeID, validatedTag, selectNode, nil
}

// appendMergedPhysicalTargetParentRestrictChecks drops an UPDATE IGNORE alias
// candidate that changes a referenced parent key while matching child rows
// still exist. Mutating actions remain attached to the final accepted image so
// CASCADE and SET NULL execute once per physical row.
func (builder *QueryBuilder) appendMergedPhysicalTargetParentRestrictChecks(
	bindCtx *BindContext,
	tableDef *plan.TableDef,
	alias string,
	lastNodeID int32,
	selectNodeTag int32,
	selectNode *plan.Node,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
) (int32, int32, *plan.Node, error) {
	enabled, err := IsForeignKeyChecksEnabled(builder.compCtx)
	if err != nil || !enabled || tableDef == nil || len(tableDef.RefChildTbls) == 0 {
		return lastNodeID, selectNodeTag, selectNode, err
	}

	affected, err := builder.collectUpdateParentForeignKeys(bindCtx, tableDef, alias, newColName2Idx)
	if err != nil {
		return 0, 0, nil, err
	}
	for _, affectedFK := range affected {
		switch affectedFK.fk.OnUpdate {
		case plan.ForeignKeyDef_RESTRICT, plan.ForeignKeyDef_NO_ACTION,
			plan.ForeignKeyDef_SET_DEFAULT:
			lastNodeID, selectNodeTag, err = builder.appendUpdateParentRestrictCheck(
				bindCtx, tableDef, alias, affectedFK, lastNodeID, selectNodeTag,
				oldColName2Idx, newColName2Idx, nil, true, true)
			if err != nil {
				return 0, 0, nil, err
			}
			selectNode = builder.updateInputProjectNode(lastNodeID)
		case plan.ForeignKeyDef_CASCADE, plan.ForeignKeyDef_SET_NULL:
			if affectedFK.childTableDef.TblId != tableDef.TblId {
				continue
			}
			lastNodeID, selectNodeTag, err = builder.appendUpdateParentRestrictCheck(
				bindCtx, tableDef, alias, affectedFK, lastNodeID, selectNodeTag,
				oldColName2Idx, newColName2Idx, nil, true, false)
			if err != nil {
				return 0, 0, nil, err
			}
			selectNode = builder.updateInputProjectNode(lastNodeID)
		}
	}
	return lastNodeID, selectNodeTag, selectNode, nil
}

// appendMergedPhysicalTargetParentForeignKeyChecks emits one parent action for
// each repeated physical target after UPDATE IGNORE has selected its final
// constraint-safe candidate. Emitting actions on alias-local rows would either
// duplicate the child write or cascade only one component of a composite key.
// Self-referencing mutating actions were already treated as restrictive during
// candidate selection, so no legacy action remains to emit for them here.
func (builder *QueryBuilder) appendMergedPhysicalTargetParentForeignKeyChecks(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	lastNodeID int32,
	selectNodeTag int32,
	selectNode *plan.Node,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	targetRowNumberPos []int32,
	targetBranchActivePos []int32,
) (int32, int32, *plan.Node, error) {
	enabled, err := IsForeignKeyChecksEnabled(builder.compCtx)
	if err != nil || !enabled {
		return lastNodeID, selectNodeTag, selectNode, err
	}
	targetsByTableID := make(map[uint64][]int)
	var tableOrder []uint64
	for i, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) == 0 {
			continue
		}
		tableID := dmlCtx.tableDefs[i].TblId
		if len(targetsByTableID[tableID]) == 0 {
			tableOrder = append(tableOrder, tableID)
		}
		targetsByTableID[tableID] = append(targetsByTableID[tableID], i)
	}
	for _, tableID := range tableOrder {
		targets := targetsByTableID[tableID]
		if len(targets) < 2 {
			continue
		}
		ownerIdx := targets[0]
		var targetSelected *plan.Expr
		if targetRowNumberPos[ownerIdx] >= 0 {
			targetSelected, err = builder.buildTargetSelectedExpr(
				selectNodeTag,
				selectNode,
				targetRowNumberPos[ownerIdx],
				targetBranchActivePos[ownerIdx],
			)
			if err != nil {
				return 0, 0, nil, err
			}
		}
		lastNodeID, selectNodeTag, err = builder.appendUpdateParentForeignKeyChecks(
			bindCtx,
			dmlCtx.tableDefs[ownerIdx],
			dmlCtx.aliases[ownerIdx],
			lastNodeID,
			selectNodeTag,
			oldColName2Idx,
			newColName2Idx,
			targetSelected,
			true,
		)
		if err != nil {
			return 0, 0, nil, err
		}
		selectNode = builder.updateInputProjectNode(lastNodeID)
	}
	return lastNodeID, selectNodeTag, selectNode, nil
}

func (builder *QueryBuilder) updateMayDependOnForeignKeys(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	newColName2Idx map[string]int32,
) (bool, error) {
	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}
		alias := dmlCtx.aliases[i]
		if len(affectedUpdateChildFks(tableDef, alias, newColName2Idx)) > 0 {
			return true, nil
		}
		if tableDef == nil || len(tableDef.RefChildTbls) == 0 {
			continue
		}

		parentColIDToName := make(map[uint64]string, len(tableDef.Cols))
		for _, col := range tableDef.Cols {
			parentColIDToName[col.ColId] = col.Name
		}
		visited := make(map[uint64]struct{}, len(tableDef.RefChildTbls))
		for _, childTableID := range tableDef.RefChildTbls {
			if childTableID == 0 {
				childTableID = tableDef.TblId
			}
			if _, ok := visited[childTableID]; ok {
				continue
			}
			visited[childTableID] = struct{}{}

			_, childTableDef, err := builder.compCtx.ResolveById(childTableID, bindCtx.snapshot)
			if err != nil {
				return false, err
			}
			if childTableDef == nil {
				return false, moerr.NewInternalErrorf(
					builder.GetContext(), "foreign-key child table %d not found", childTableID)
			}
			for _, fk := range childTableDef.Fkeys {
				referencesCurrentTable := fk.ForeignTbl == tableDef.TblId ||
					(fk.ForeignTbl == 0 && childTableDef.TblId == tableDef.TblId)
				if !referencesCurrentTable {
					continue
				}
				for _, parentColID := range fk.ForeignCols {
					if _, ok := newColName2Idx[alias+"."+parentColIDToName[parentColID]]; ok {
						return true, nil
					}
				}
			}
		}
	}
	return false, nil
}

func affectedUpdateChildFks(
	tableDef *plan.TableDef,
	alias string,
	newColName2Idx map[string]int32,
) []*plan.ForeignKeyDef {
	if tableDef == nil {
		return nil
	}

	colIDToName := make(map[uint64]string, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		colIDToName[col.ColId] = col.Name
	}

	affected := make([]*plan.ForeignKeyDef, 0, len(tableDef.Fkeys))
	for _, fk := range tableDef.Fkeys {
		for _, childColID := range fk.Cols {
			if _, ok := newColName2Idx[alias+"."+colIDToName[childColID]]; ok {
				affected = append(affected, fk)
				break
			}
		}
	}
	return affected
}

type updateParentForeignKey struct {
	childObjRef   *plan.ObjectRef
	childTableDef *plan.TableDef
	fk            *plan.ForeignKeyDef
}

// validateDistinctUpdateForeignKeyMutationTargets protects the Stage-1
// distinct-table planner invariant: one statement may have only one physical
// write path for a table. Parent actions are separate MULTI_UPDATE steps, so a
// child that is also an explicit target, or is reached from two parent targets,
// cannot be safely planned until those paths share one final-row merger.
func (builder *QueryBuilder) validateDistinctUpdateForeignKeyMutationTargets(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
) error {
	enabled, err := IsForeignKeyChecksEnabled(builder.compCtx)
	if err != nil || !enabled {
		return err
	}

	type writeOrigin struct {
		description    string
		actionParentID uint64
	}
	writeOrigins := make(map[uint64]writeOrigin)
	for targetIdx, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) == 0 {
			continue
		}
		tableID := dmlCtx.tableDefs[targetIdx].TblId
		if _, exists := writeOrigins[tableID]; !exists {
			writeOrigins[tableID] = writeOrigin{
				description: fmt.Sprintf("explicit target '%s'", dmlCtx.aliases[targetIdx]),
			}
		}
	}

	for targetIdx, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) == 0 {
			continue
		}
		parentTableDef := dmlCtx.tableDefs[targetIdx]
		if parentTableDef == nil || len(parentTableDef.RefChildTbls) == 0 {
			continue
		}

		parentColIDToName := make(map[uint64]string, len(parentTableDef.Cols))
		for _, col := range parentTableDef.Cols {
			parentColIDToName[col.ColId] = col.Name
		}
		visitedChildren := make(map[uint64]struct{}, len(parentTableDef.RefChildTbls))
		for _, childTableID := range parentTableDef.RefChildTbls {
			if childTableID == 0 {
				childTableID = parentTableDef.TblId
			}
			if _, visited := visitedChildren[childTableID]; visited {
				continue
			}
			visitedChildren[childTableID] = struct{}{}

			childObjRef, childTableDef, resolveErr := builder.compCtx.ResolveById(
				childTableID, bindCtx.snapshot)
			if resolveErr != nil {
				return resolveErr
			}
			if childTableDef == nil {
				return moerr.NewInternalErrorf(
					builder.GetContext(), "foreign-key child table %d not found", childTableID)
			}

			hasMutation := false
			for _, fk := range childTableDef.Fkeys {
				referencesParent := fk.ForeignTbl == parentTableDef.TblId ||
					(fk.ForeignTbl == 0 && childTableDef.TblId == parentTableDef.TblId)
				if !referencesParent ||
					(fk.OnUpdate != plan.ForeignKeyDef_CASCADE && fk.OnUpdate != plan.ForeignKeyDef_SET_NULL) {
					continue
				}
				for _, parentColID := range fk.ForeignCols {
					if _, updated := updateCols[parentColIDToName[parentColID]]; updated {
						hasMutation = true
						break
					}
				}
				if hasMutation {
					break
				}
			}
			if !hasMutation || childTableID == parentTableDef.TblId {
				// Self-referencing actions already take the established legacy route.
				continue
			}

			origin := fmt.Sprintf("foreign key action from '%s'", dmlCtx.aliases[targetIdx])
			if previousOrigin, exists := writeOrigins[childTableID]; exists {
				if previousOrigin.actionParentID == parentTableDef.TblId {
					// Sibling aliases of one physical parent row feed one merged
					// parent-key transition and therefore one child mutation path.
					continue
				}
				childName := childTableDef.Name
				if childObjRef != nil && childObjRef.ObjName != "" {
					childName = childObjRef.ObjName
				}
				return newUpdatePlannerRouteError(
					updatePlannerRejected,
					updateRouteReasonForeignKey,
					moerr.NewNotSupportedf(
						builder.GetContext(),
						"overlapping update paths for table '%s': %s and %s",
						childName,
						previousOrigin.description,
						origin,
					),
				)
			}
			writeOrigins[childTableID] = writeOrigin{
				description:    origin,
				actionParentID: parentTableDef.TblId,
			}
		}
	}
	return nil
}

func (builder *QueryBuilder) appendUpdateParentForeignKeyChecks(
	bindCtx *BindContext,
	tableDef *plan.TableDef,
	alias string,
	lastNodeID int32,
	selectNodeTag int32,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	targetSelected *plan.Expr,
	skipSelfReferencingActions bool,
) (int32, int32, error) {
	if tableDef == nil || len(tableDef.RefChildTbls) == 0 {
		return lastNodeID, selectNodeTag, nil
	}

	affected, err := builder.collectUpdateParentForeignKeys(bindCtx, tableDef, alias, newColName2Idx)
	if err != nil {
		return 0, 0, err
	}
	if len(affected) == 0 {
		return lastNodeID, selectNodeTag, nil
	}
	// PREPARE may happen before the execution transaction exists. Keep a marker
	// in the serialized plan so compile/run can enforce the actual transaction
	// mode even when the binder could not observe it.
	builder.qry.DetectSqls = append(builder.qry.DetectSqls, "UPDATE_PARENT_PLAN:")
	if proc := builder.compCtx.GetProcess(); proc != nil {
		if txnOp := proc.GetTxnOperator(); txnOp != nil && !txnOp.Txn().IsPessimistic() {
			return 0, 0, newUpdatePlannerRouteError(
				updatePlannerRejected,
				updateRouteReasonForeignKey,
				moerr.NewNotSupported(
					builder.GetContext(),
					"updating a referenced parent key in an optimistic transaction",
				),
			)
		}
	}

	lastNodeID, selectNodeTag, err = builder.appendUpdateParentKeyLocks(
		bindCtx,
		tableDef,
		alias,
		affected,
		lastNodeID,
		selectNodeTag,
		oldColName2Idx,
		newColName2Idx,
	)
	if err != nil {
		return 0, 0, err
	}

	for _, affectedFK := range affected {
		if affectedFK.childTableDef.TblId == tableDef.TblId {
			switch affectedFK.fk.OnUpdate {
			case plan.ForeignKeyDef_RESTRICT,
				plan.ForeignKeyDef_NO_ACTION,
				plan.ForeignKeyDef_SET_DEFAULT:
			case plan.ForeignKeyDef_CASCADE, plan.ForeignKeyDef_SET_NULL:
				if skipSelfReferencingActions {
					continue
				}
				return 0, 0, newLegacyUpdatePlannerRouteError(
					updateRouteReasonForeignKey,
					moerr.NewUnsupportedDML(
						builder.GetContext(),
						"self-referencing parent foreign key action",
					),
				)
			}
		}
		switch affectedFK.fk.OnUpdate {
		case plan.ForeignKeyDef_RESTRICT,
			plan.ForeignKeyDef_NO_ACTION,
			plan.ForeignKeyDef_SET_DEFAULT:
			var err error
			lastNodeID, selectNodeTag, err = builder.appendUpdateParentRestrictCheck(
				bindCtx,
				tableDef,
				alias,
				affectedFK,
				lastNodeID,
				selectNodeTag,
				oldColName2Idx,
				newColName2Idx,
				targetSelected,
				false,
				true,
			)
			if err != nil {
				return 0, 0, err
			}
		case plan.ForeignKeyDef_CASCADE, plan.ForeignKeyDef_SET_NULL:
		default:
			return 0, 0, moerr.NewInternalErrorf(
				builder.GetContext(),
				"unsupported foreign key ON UPDATE action %s",
				affectedFK.fk.OnUpdate.String(),
			)
		}
	}

	mutations := make([]updateParentForeignKey, 0, len(affected))
	for _, affectedFK := range affected {
		if skipSelfReferencingActions && affectedFK.childTableDef.TblId == tableDef.TblId {
			continue
		}
		if affectedFK.fk.OnUpdate == plan.ForeignKeyDef_CASCADE ||
			affectedFK.fk.OnUpdate == plan.ForeignKeyDef_SET_NULL {
			mutations = append(mutations, affectedFK)
		}
	}
	if len(mutations) == 0 {
		return lastNodeID, selectNodeTag, nil
	}

	mutationByChild := make(map[uint64]struct{}, len(mutations))
	for _, mutation := range mutations {
		if _, exists := mutationByChild[mutation.childTableDef.TblId]; exists {
			return 0, 0, newLegacyUpdatePlannerRouteError(
				updateRouteReasonForeignKey,
				moerr.NewUnsupportedDML(
					builder.GetContext(),
					"multiple parent foreign key actions targeting the same child table",
				),
			)
		}
		mutationByChild[mutation.childTableDef.TblId] = struct{}{}
		if err := builder.validateModernUpdateParentMutation(bindCtx, tableDef, mutation); err != nil {
			return 0, 0, err
		}
	}

	sourceSinkID := appendSinkNodeWithTag(builder, bindCtx, lastNodeID, selectNodeTag)
	sourceStep := builder.appendStep(sourceSinkID)
	for _, mutation := range mutations {
		if err := builder.appendUpdateParentMutation(
			bindCtx,
			tableDef,
			alias,
			mutation,
			sourceStep,
			selectNodeTag,
			oldColName2Idx,
			newColName2Idx,
			targetSelected,
		); err != nil {
			return 0, 0, err
		}
	}
	lastNodeID = builder.appendTaggedSinkScan(bindCtx, sourceStep, selectNodeTag)
	return lastNodeID, selectNodeTag, nil
}

func (builder *QueryBuilder) collectUpdateParentForeignKeys(
	bindCtx *BindContext,
	tableDef *plan.TableDef,
	alias string,
	newColName2Idx map[string]int32,
) ([]updateParentForeignKey, error) {
	parentColIDToName := make(map[uint64]string, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		parentColIDToName[col.ColId] = col.Name
	}

	affected := make([]updateParentForeignKey, 0)
	visited := make(map[uint64]struct{}, len(tableDef.RefChildTbls))
	for _, childTableID := range tableDef.RefChildTbls {
		if childTableID == 0 {
			childTableID = tableDef.TblId
		}
		if _, ok := visited[childTableID]; ok {
			continue
		}
		visited[childTableID] = struct{}{}

		childObjRef, childTableDef, err := builder.compCtx.ResolveById(childTableID, bindCtx.snapshot)
		if err != nil {
			return nil, err
		}
		if childTableDef == nil {
			return nil, moerr.NewInternalErrorf(
				builder.GetContext(),
				"foreign-key child table %d not found",
				childTableID,
			)
		}
		if err := validateTableIndexDefinitions(childTableDef); err != nil {
			return nil, err
		}

		for _, fk := range childTableDef.Fkeys {
			referencesCurrentTable := fk.ForeignTbl == tableDef.TblId ||
				(fk.ForeignTbl == 0 && childTableDef.TblId == tableDef.TblId)
			if !referencesCurrentTable {
				continue
			}
			for _, parentColID := range fk.ForeignCols {
				if _, ok := newColName2Idx[alias+"."+parentColIDToName[parentColID]]; ok {
					affected = append(affected, updateParentForeignKey{
						childObjRef:   childObjRef,
						childTableDef: childTableDef,
						fk:            fk,
					})
					break
				}
			}
		}
	}
	sort.SliceStable(affected, func(i, j int) bool {
		if affected[i].childTableDef.TblId != affected[j].childTableDef.TblId {
			return affected[i].childTableDef.TblId < affected[j].childTableDef.TblId
		}
		return affected[i].fk.Name < affected[j].fk.Name
	})
	return affected, nil
}

func (builder *QueryBuilder) appendUpdateParentKeyLocks(
	bindCtx *BindContext,
	parentTableDef *plan.TableDef,
	parentAlias string,
	affected []updateParentForeignKey,
	lastNodeID int32,
	selectNodeTag int32,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
) (int32, int32, error) {
	parentObjRef, resolvedParent, err := builder.compCtx.ResolveById(parentTableDef.TblId, bindCtx.snapshot)
	if err != nil {
		return 0, 0, err
	}
	if resolvedParent == nil {
		return 0, 0, moerr.NewInternalErrorf(
			builder.GetContext(), "foreign-key parent table %d not found", parentTableDef.TblId)
	}

	rowProject := getProjectionByLastNodeWithTag(builder, lastNodeID, selectNodeTag)
	lockTag := builder.genNewBindTag()
	lockProject := append([]*plan.Expr(nil), rowProject...)
	lockTargets := make([]*plan.LockTarget, 0, len(affected)*2)
	tableLocked := false
	validIndexes, _ := getValidIndexes(parentTableDef)

	for _, affectedFK := range affected {
		referencedNames, buildErr := updateParentColNames(
			builder.GetContext(), parentTableDef, affectedFK.fk.ForeignCols)
		if buildErr != nil {
			return 0, 0, buildErr
		}

		lockTableDef := parentTableDef
		lockObjRef := parentObjRef
		lockTable := false
		var matchedIndex *plan.IndexDef
		var pkeyNames []string
		if parentTableDef.Pkey != nil {
			pkeyNames = parentTableDef.Pkey.Names
			if len(pkeyNames) == 0 && parentTableDef.Pkey.PkeyColName != "" {
				pkeyNames = []string{parentTableDef.Pkey.PkeyColName}
			}
		}
		if !updateForeignKeyPartsEqual(pkeyNames, referencedNames) {
			for _, idxDef := range validIndexes {
				if idxDef.Unique && updateForeignKeyPartsEqual(idxDef.Parts, referencedNames) {
					matchedIndex = idxDef
					break
				}
			}
			if matchedIndex == nil {
				lockTable = true
			} else {
				lockObjRef, lockTableDef, buildErr = builder.compCtx.ResolveIndexTableByRef(
					parentObjRef, matchedIndex.IndexTableName, bindCtx.snapshot)
				if buildErr != nil {
					return 0, 0, buildErr
				}
			}
		}
		if lockTable && tableLocked {
			continue
		}

		for _, useNew := range []bool{false, true} {
			keyParts := make([]*plan.Expr, len(referencedNames))
			for i, colName := range referencedNames {
				pos := oldColName2Idx[parentAlias+"."+colName]
				if useNew {
					if newPos, ok := newColName2Idx[parentAlias+"."+colName]; ok {
						pos = newPos
					}
				}
				keyParts[i] = &plan.Expr{
					Typ: rowProject[pos].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: selectNodeTag, ColPos: pos,
					}},
				}
			}

			lockExpr := keyParts[0]
			if !lockTable {
				if matchedIndex != nil {
					prefixLengths, prefixErr := catalog.IndexPrefixLengthsFromParamsWithError(
						matchedIndex.IndexAlgoParams)
					if prefixErr != nil {
						return 0, 0, prefixErr
					}
					for i := range keyParts {
						keyParts[i], buildErr = builder.makeIndexPartExprFromInputExpr(
							keyParts[i], referencedNames[i], prefixLengths)
						if buildErr != nil {
							return 0, 0, buildErr
						}
					}
				}
				if len(keyParts) > 1 || (matchedIndex != nil && indexTableStoresSerializedKey(matchedIndex)) {
					lockExpr, buildErr = BindFuncExprImplByPlanExpr(
						builder.GetContext(), "serial", keyParts)
					if buildErr != nil {
						return 0, 0, buildErr
					}
				}
			}
			lockProject = append(lockProject, lockExpr)
			_, lockTyp := getPkPos(lockTableDef, false)
			lockTargets = append(lockTargets, &plan.LockTarget{
				TableId: lockTableDef.TblId, ObjRef: lockObjRef,
				PrimaryColIdxInBat: int32(len(lockProject) - 1), PrimaryColRelPos: lockTag,
				PrimaryColTyp: lockTyp, Mode: lockpb.LockMode_Exclusive, LockTable: lockTable,
			})
			if lockTable {
				tableLocked = true
				break
			}
		}
	}
	sortForeignKeyLockTargets(lockTargets, map[uint64]struct{}{parentTableDef.TblId: {}})

	lockInputID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{lastNodeID},
		ProjectList: lockProject, BindingTags: []int32{lockTag},
	}, bindCtx)
	lockOutput := getProjectionByLastNodeWithTag(builder, lockInputID, lockTag)
	lockNodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_LOCK_OP, Children: []int32{lockInputID},
		TableDef: resolvedParent, LockTargets: lockTargets,
	}, bindCtx)
	lockedNodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{lockNodeID},
		ProjectList: append([]*plan.Expr(nil), lockOutput[:len(rowProject)]...),
		BindingTags: []int32{selectNodeTag},
	}, bindCtx)
	lockedSinkID := appendSinkNodeWithTag(builder, bindCtx, lockedNodeID, selectNodeTag)
	lockedStep := builder.appendStep(lockedSinkID)
	return builder.appendTaggedSinkScan(bindCtx, lockedStep, selectNodeTag), selectNodeTag, nil
}

func updateParentColNames(
	ctx context.Context,
	tableDef *plan.TableDef,
	colIDs []uint64,
) ([]string, error) {
	idToName := make(map[uint64]string, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		idToName[col.ColId] = col.Name
	}
	names := make([]string, len(colIDs))
	for i, colID := range colIDs {
		name, ok := idToName[colID]
		if !ok {
			return nil, moerr.NewInternalErrorf(ctx, "foreign-key parent column %d not found", colID)
		}
		names[i] = name
	}
	return names, nil
}

func updateForeignKeyPartsEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if catalog.ResolveAlias(left[i]) != catalog.ResolveAlias(right[i]) {
			return false
		}
	}
	return true
}

func sortForeignKeyLockTargets(lockTargets []*plan.LockTarget, baseTableIDs map[uint64]struct{}) {
	sort.SliceStable(lockTargets, func(i, j int) bool {
		_, leftBase := baseTableIDs[lockTargets[i].TableId]
		_, rightBase := baseTableIDs[lockTargets[j].TableId]
		if leftBase != rightBase {
			return leftBase
		}
		leftName, rightName := "", ""
		if lockTargets[i].ObjRef != nil {
			leftName = lockTargets[i].ObjRef.ObjName
		}
		if lockTargets[j].ObjRef != nil {
			rightName = lockTargets[j].ObjRef.ObjName
		}
		if leftName != rightName {
			return leftName < rightName
		}
		if lockTargets[i].TableId != lockTargets[j].TableId {
			return lockTargets[i].TableId < lockTargets[j].TableId
		}
		if lockTargets[i].LockTable != lockTargets[j].LockTable {
			return !lockTargets[i].LockTable
		}
		if lockTargets[i].PrimaryColIdxInBat != lockTargets[j].PrimaryColIdxInBat {
			return lockTargets[i].PrimaryColIdxInBat < lockTargets[j].PrimaryColIdxInBat
		}
		return lockTargets[i].PrimaryColRelPos < lockTargets[j].PrimaryColRelPos
	})
}

func updateParentReferenceIsUnique(
	ctx context.Context,
	parentTableDef *plan.TableDef,
	foreignCols []uint64,
) (bool, error) {
	referencedNames, err := updateParentColNames(ctx, parentTableDef, foreignCols)
	if err != nil {
		return false, err
	}
	if parentTableDef.Pkey != nil {
		pkeyNames := parentTableDef.Pkey.Names
		if len(pkeyNames) == 0 && parentTableDef.Pkey.PkeyColName != "" {
			pkeyNames = []string{parentTableDef.Pkey.PkeyColName}
		}
		if updateForeignKeyPartsEqual(pkeyNames, referencedNames) {
			return true, nil
		}
	}
	validIndexes, _ := getValidIndexes(parentTableDef)
	for _, idxDef := range validIndexes {
		if idxDef.Unique && updateForeignKeyPartsEqual(idxDef.Parts, referencedNames) {
			return true, nil
		}
	}
	return false, nil
}

func (builder *QueryBuilder) validateModernUpdateParentMutation(
	bindCtx *BindContext,
	parentTableDef *plan.TableDef,
	affectedFK updateParentForeignKey,
) error {
	childTableDef := affectedFK.childTableDef
	ensureName2ColIndexForReplace(childTableDef)
	if err := builder.validateModernUpdateParentRowClosure(parentTableDef, affectedFK); err != nil {
		return err
	}
	if childTableDef.TblId == parentTableDef.TblId {
		return newLegacyUpdatePlannerRouteError(
			updateRouteReasonForeignKey,
			moerr.NewUnsupportedDML(builder.GetContext(), "self-referencing parent foreign key action"),
		)
	}
	_, hasIrregularIndex := getValidIndexes(childTableDef)
	if hasIrregularIndex {
		return moerr.NewNotSupported(
			builder.GetContext(),
			"parent foreign key action on child table with irregular indexes",
		)
	}
	if childTableDef.Partition != nil {
		return moerr.NewNotSupported(
			builder.GetContext(),
			"parent foreign key action on partitioned child table",
		)
	}

	childColIDToName := make(map[uint64]string, len(childTableDef.Cols))
	for _, col := range childTableDef.Cols {
		childColIDToName[col.ColId] = col.Name
	}
	primaryNames := make(map[string]struct{}, len(childTableDef.Pkey.Names)+1)
	primaryNames[childTableDef.Pkey.PkeyColName] = struct{}{}
	for _, name := range childTableDef.Pkey.Names {
		primaryNames[name] = struct{}{}
	}
	for _, childColID := range affectedFK.fk.Cols {
		if _, ok := primaryNames[childColIDToName[childColID]]; ok {
			return newLegacyUpdatePlannerRouteError(
				updateRouteReasonForeignKey,
				moerr.NewUnsupportedDML(
					builder.GetContext(),
					"parent foreign key action changing child primary key",
				),
			)
		}
	}

	updatedChildCols := make(map[uint64]struct{}, len(affectedFK.fk.Cols))
	for _, childColID := range affectedFK.fk.Cols {
		updatedChildCols[childColID] = struct{}{}
	}
	for _, col := range childTableDef.Cols {
		if col.GeneratedCol != nil {
			updatedChildCols[col.ColId] = struct{}{}
		}
	}
	for _, outgoingFK := range childTableDef.Fkeys {
		if outgoingFK == affectedFK.fk {
			continue
		}
		for _, childColID := range outgoingFK.Cols {
			if _, changed := updatedChildCols[childColID]; changed {
				return moerr.NewNotSupported(
					builder.GetContext(),
					"parent foreign key action changing child column constrained by another foreign key",
				)
			}
		}
	}
	visited := make(map[uint64]struct{}, len(childTableDef.RefChildTbls))
	for _, grandchildID := range childTableDef.RefChildTbls {
		if grandchildID == 0 {
			grandchildID = childTableDef.TblId
		}
		if _, ok := visited[grandchildID]; ok {
			continue
		}
		visited[grandchildID] = struct{}{}
		_, grandchildDef, err := builder.compCtx.ResolveById(grandchildID, bindCtx.snapshot)
		if err != nil {
			return err
		}
		for _, fk := range grandchildDef.Fkeys {
			referencesChild := fk.ForeignTbl == childTableDef.TblId ||
				(fk.ForeignTbl == 0 && grandchildDef.TblId == childTableDef.TblId)
			if !referencesChild {
				continue
			}
			for _, referencedColID := range fk.ForeignCols {
				if _, changed := updatedChildCols[referencedColID]; changed {
					return moerr.NewNotSupported(
						builder.GetContext(),
						"recursive parent foreign key action graph",
					)
				}
			}
		}
	}
	return nil
}

func (builder *QueryBuilder) validateModernUpdateParentRowClosure(
	parentTableDef *plan.TableDef,
	affectedFK updateParentForeignKey,
) error {
	childTableDef := affectedFK.childTableDef
	childColByID := make(map[uint64]*plan.ColDef, len(childTableDef.Cols))
	parentColByID := make(map[uint64]*plan.ColDef, len(parentTableDef.Cols))
	updatedChildNames := make(map[string]struct{}, len(affectedFK.fk.Cols))
	for _, col := range childTableDef.Cols {
		childColByID[col.ColId] = col
		if col.GeneratedCol != nil || col.OnUpdate != nil {
			return builder.newUnsupportedUpdateParentRowClosureError()
		}
	}
	for _, col := range parentTableDef.Cols {
		parentColByID[col.ColId] = col
	}
	for i, childColID := range affectedFK.fk.Cols {
		childCol := childColByID[childColID]
		if childCol == nil || i >= len(affectedFK.fk.ForeignCols) {
			return moerr.NewInternalError(builder.GetContext(), "invalid parent foreign key action columns")
		}
		updatedChildNames[childCol.Name] = struct{}{}
		if affectedFK.fk.OnUpdate != plan.ForeignKeyDef_CASCADE {
			continue
		}
		parentCol := parentColByID[affectedFK.fk.ForeignCols[i]]
		if parentCol == nil {
			return moerr.NewInternalError(builder.GetContext(), "invalid parent foreign key action columns")
		}
		if parentCol.Typ.Id != childCol.Typ.Id ||
			parentCol.Typ.Width != childCol.Typ.Width ||
			parentCol.Typ.Scale != childCol.Typ.Scale ||
			parentCol.Typ.Enumvalues != childCol.Typ.Enumvalues {
			return builder.newUnsupportedUpdateParentRowClosureError()
		}
	}
	for _, idxDef := range childTableDef.Indexes {
		if !idxDef.Unique {
			continue
		}
		for _, part := range idxDef.Parts {
			if _, changed := updatedChildNames[catalog.ResolveAlias(part)]; changed {
				return builder.newUnsupportedUpdateParentRowClosureError()
			}
		}
	}
	if childTableDef.ClusterBy != nil &&
		planutil.JudgeIsCompositeClusterByColumn(childTableDef.ClusterBy.Name) {
		for _, colName := range planutil.SplitCompositeClusterByColumnName(childTableDef.ClusterBy.Name) {
			if _, changed := updatedChildNames[colName]; changed {
				return builder.newUnsupportedUpdateParentRowClosureError()
			}
		}
	}
	return nil
}

func (builder *QueryBuilder) newUnsupportedUpdateParentRowClosureError() error {
	return newUpdatePlannerRouteError(
		updatePlannerRejected,
		updateRouteReasonForeignKey,
		moerr.NewNotSupported(
			builder.GetContext(),
			"parent foreign key action requires complete child update row closure",
		),
	)
}

func (builder *QueryBuilder) appendUpdateParentMutation(
	bindCtx *BindContext,
	parentTableDef *plan.TableDef,
	parentAlias string,
	affectedFK updateParentForeignKey,
	sourceStep int32,
	sourceTag int32,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	targetSelected *plan.Expr,
) error {
	parentNodeID := builder.appendTaggedSinkScan(bindCtx, sourceStep, sourceTag)
	parentNode := builder.qry.Nodes[parentNodeID]
	unchanged, err := builder.buildUpdateReferencedKeyUnchangedExpr(
		parentTableDef,
		affectedFK.fk,
		parentAlias,
		sourceTag,
		oldColName2Idx,
		newColName2Idx,
		parentNode,
	)
	if err != nil {
		return err
	}
	filter := DeepCopyExpr(targetSelected)
	if unchanged != nil {
		changed, bindErr := BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"not",
			[]*plan.Expr{unchanged},
		)
		if bindErr != nil {
			return bindErr
		}
		if filter == nil {
			filter = changed
		} else {
			filter, bindErr = BindFuncExprImplByPlanExpr(
				builder.GetContext(), "and", []*plan.Expr{filter, changed})
			if bindErr != nil {
				return bindErr
			}
		}
	}
	if filter != nil {
		parentNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_FILTER, Children: []int32{parentNodeID}, FilterList: []*plan.Expr{filter},
		}, bindCtx)
	}
	parentColIDToName := make(map[uint64]string, len(parentTableDef.Cols))
	for _, col := range parentTableDef.Cols {
		parentColIDToName[col.ColId] = col.Name
	}

	uniqueReference, err := updateParentReferenceIsUnique(
		builder.GetContext(), parentTableDef, affectedFK.fk.ForeignCols)
	if err != nil {
		return err
	}
	childTableDef := affectedFK.childTableDef
	childTag := builder.genNewBindTag()
	builder.addNameByColRef(childTag, childTableDef)
	childNodeID := builder.appendNode(&plan.Node{
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     childTableDef,
		ObjRef:       affectedFK.childObjRef,
		BindingTags:  []int32{childTag},
		ScanSnapshot: bindCtx.snapshot,
	}, bindCtx)

	childColIDToPos := make(map[uint64]int32, len(childTableDef.Cols))
	for i, col := range childTableDef.Cols {
		childColIDToPos[col.ColId] = int32(i)
	}

	joinPreds := make([]*plan.Expr, len(affectedFK.fk.Cols))
	newChildExprs := make(map[int32]*plan.Expr, len(affectedFK.fk.Cols))
	for i, childColID := range affectedFK.fk.Cols {
		parentColName := parentColIDToName[affectedFK.fk.ForeignCols[i]]
		oldParentPos := oldColName2Idx[parentAlias+"."+parentColName]
		newParentPos := oldParentPos
		if pos, ok := newColName2Idx[parentAlias+"."+parentColName]; ok {
			newParentPos = pos
		}
		childPos := childColIDToPos[childColID]
		oldParentExpr := &plan.Expr{
			Typ: parentNode.ProjectList[oldParentPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: sourceTag,
				ColPos: oldParentPos,
			}},
		}
		childExpr := &plan.Expr{
			Typ: childTableDef.Cols[childPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: childTag,
				ColPos: childPos,
			}},
		}
		joinPreds[i], err = BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"=",
			[]*plan.Expr{oldParentExpr, childExpr},
		)
		if err != nil {
			return err
		}

		switch affectedFK.fk.OnUpdate {
		case plan.ForeignKeyDef_CASCADE:
			newChildExprs[childPos] = &plan.Expr{
				Typ: childTableDef.Cols[childPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: sourceTag,
					ColPos: newParentPos,
				}},
			}
		case plan.ForeignKeyDef_SET_NULL:
			col := childTableDef.Cols[childPos]
			if col.Default != nil && !col.Default.NullAbility {
				return moerr.NewConstraintViolation(
					builder.GetContext(),
					fmt.Sprintf("Column '%s' cannot be null", col.Name),
				)
			}
			newChildExprs[childPos] = &plan.Expr{
				Typ:  col.Typ,
				Expr: &plan.Expr_Lit{Lit: &Const{Isnull: true}},
			}
		}
	}

	joinNodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{parentNodeID, childNodeID},
		JoinType: plan.Node_INNER,
		OnList:   joinPreds,
	}, bindCtx)
	if !uniqueReference {
		mappingTag := builder.genNewBindTag()
		mappingProjection := make([]*plan.Expr, 0, len(childTableDef.Cols)+len(newChildExprs))
		for i, col := range childTableDef.Cols {
			mappingProjection = append(mappingProjection, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: childTag,
					ColPos: int32(i),
				}},
			})
		}
		mappingNewChildExprs := make(map[int32]*plan.Expr, len(newChildExprs))
		for _, childColID := range affectedFK.fk.Cols {
			childPos := childColIDToPos[childColID]
			newPos := int32(len(mappingProjection))
			mappingProjection = append(mappingProjection, newChildExprs[childPos])
			mappingNewChildExprs[childPos] = &plan.Expr{
				Typ: childTableDef.Cols[childPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: mappingTag,
					ColPos: newPos,
				}},
			}
		}
		joinNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{joinNodeID},
			ProjectList: mappingProjection,
			BindingTags: []int32{mappingTag},
		}, bindCtx)
		childTag = mappingTag
		newChildExprs = mappingNewChildExprs

		childRowIDPos := childTableDef.Name2ColIndex[catalog.Row_ID]
		childRowIDExpr := &plan.Expr{
			Typ: childTableDef.Cols[childRowIDPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: childTag,
				ColPos: childRowIDPos,
			}},
		}
		mappingIdentity := []*plan.Expr{childRowIDExpr}
		if affectedFK.fk.OnUpdate == plan.ForeignKeyDef_CASCADE {
			for _, childColID := range affectedFK.fk.Cols {
				mappingIdentity = append(mappingIdentity, newChildExprs[childColIDToPos[childColID]])
			}
		}
		joinNodeID, err = builder.appendRowNumberMappingGuardNode(
			bindCtx, joinNodeID, mappingIdentity, "", "")
		if err != nil {
			return err
		}
		if affectedFK.fk.OnUpdate == plan.ForeignKeyDef_CASCADE {
			joinNodeID, err = builder.appendRowNumberMappingGuardNode(
				bindCtx,
				joinNodeID,
				[]*plan.Expr{childRowIDExpr},
				"parent foreign key action has an ambiguous non-unique referenced-key mapping",
				foreignKeyAmbiguousMappingAssert,
			)
			if err != nil {
				return err
			}
		}
	}

	type mutationIndex struct {
		def      *plan.IndexDef
		objRef   *plan.ObjectRef
		tableDef *plan.TableDef
		tag      int32
	}
	validIndexes, _ := getValidIndexes(childTableDef)
	indexes := make([]mutationIndex, 0, len(validIndexes))
	for _, idxDef := range validIndexes {
		indexAffected := false
		for _, part := range idxDef.Parts {
			colPos, ok := childTableDef.Name2ColIndex[catalog.ResolveAlias(part)]
			if ok {
				_, indexAffected = newChildExprs[colPos]
			}
			if indexAffected {
				break
			}
		}
		if !indexAffected {
			continue
		}
		idxObjRef, idxTableDef, resolveErr := builder.compCtx.ResolveIndexTableByRef(
			affectedFK.childObjRef,
			idxDef.IndexTableName,
			bindCtx.snapshot,
		)
		if resolveErr != nil {
			return resolveErr
		}
		ensureName2ColIndexForReplace(idxTableDef)
		idxTag := builder.genNewBindTag()
		builder.addNameByColRef(idxTag, idxTableDef)
		idxScanID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     idxTableDef,
			ObjRef:       idxObjRef,
			BindingTags:  []int32{idxTag},
			ScanSnapshot: bindCtx.snapshot,
		}, bindCtx)

		oldKeyExpr, buildErr := builder.buildUpdateMutationIndexKey(
			childTableDef,
			idxDef,
			childTag,
			nil,
		)
		if buildErr != nil {
			return buildErr
		}
		lookupName := indexLookupColumnName(idxDef)
		lookupPos := idxTableDef.Name2ColIndex[lookupName]
		storedKeyExpr := &plan.Expr{
			Typ: idxTableDef.Cols[lookupPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: idxTag,
				ColPos: lookupPos,
			}},
		}
		indexMatch, buildErr := BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"=",
			[]*plan.Expr{oldKeyExpr, storedKeyExpr},
		)
		if buildErr != nil {
			return buildErr
		}
		joinType := plan.Node_INNER
		if idxDef.Unique {
			joinType = plan.Node_LEFT
		}
		joinNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{joinNodeID, idxScanID},
			JoinType: joinType,
			OnList:   []*plan.Expr{indexMatch},
		}, bindCtx)
		indexes = append(indexes, mutationIndex{
			def:      idxDef,
			objRef:   idxObjRef,
			tableDef: idxTableDef,
			tag:      idxTag,
		})
	}

	actionTag := builder.genNewBindTag()
	actionProjection := make([]*plan.Expr, len(childTableDef.Cols))
	for i, col := range childTableDef.Cols {
		if newExpr, ok := newChildExprs[int32(i)]; ok {
			actionProjection[i] = newExpr
			continue
		}
		actionProjection[i] = &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: childTag,
				ColPos: int32(i),
			}},
		}
	}
	finalReplacements := make(map[int32]*plan.Expr, len(newChildExprs))
	for pos, expr := range newChildExprs {
		finalReplacements[pos] = expr
	}

	type mutationIndexPositions struct {
		oldRowID int32
		oldKey   int32
		newKey   int32
	}
	indexPositions := make([]mutationIndexPositions, len(indexes))
	for i, idx := range indexes {
		oldRowIDPos := idx.tableDef.Name2ColIndex[catalog.Row_ID]
		oldKeyPos := idx.tableDef.Name2ColIndex[indexLookupColumnName(idx.def)]
		indexPositions[i].oldRowID = int32(len(actionProjection))
		actionProjection = append(actionProjection, &plan.Expr{
			Typ: idx.tableDef.Cols[oldRowIDPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: idx.tag,
				ColPos: oldRowIDPos,
			}},
		})
		indexPositions[i].oldKey = int32(len(actionProjection))
		actionProjection = append(actionProjection, &plan.Expr{
			Typ: idx.tableDef.Cols[oldKeyPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: idx.tag,
				ColPos: oldKeyPos,
			}},
		})
		newKeyExpr, buildErr := builder.buildUpdateMutationIndexKey(
			childTableDef,
			idx.def,
			childTag,
			finalReplacements,
		)
		if buildErr != nil {
			return buildErr
		}
		indexPositions[i].newKey = int32(len(actionProjection))
		actionProjection = append(actionProjection, newKeyExpr)
	}
	actionNodeID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{joinNodeID},
		ProjectList: actionProjection,
		BindingTags: []int32{actionTag},
	}, bindCtx)

	rowIDPos := childTableDef.Name2ColIndex[catalog.Row_ID]
	pkPos := childTableDef.Name2ColIndex[childTableDef.Pkey.PkeyColName]
	insertCols := make([]plan.ColRef, 0, len(childTableDef.Cols)-1)
	for i, col := range childTableDef.Cols {
		if col.Name == catalog.Row_ID {
			continue
		}
		insertCols = append(insertCols, plan.ColRef{RelPos: actionTag, ColPos: int32(i)})
	}
	lockTarget := &plan.LockTarget{
		TableId:            childTableDef.TblId,
		ObjRef:             affectedFK.childObjRef,
		PrimaryColIdxInBat: pkPos,
		PrimaryColRelPos:   actionTag,
		PrimaryColTyp:      actionProjection[pkPos].Typ,
	}
	lockTargets := []*plan.LockTarget{lockTarget}
	updateCtxList := []*plan.UpdateCtx{{
		ObjRef:             affectedFK.childObjRef,
		TableDef:           childTableDef,
		InsertCols:         insertCols,
		IgnoreAffectedRows: true,
		DeleteCols: []plan.ColRef{
			{RelPos: actionTag, ColPos: rowIDPos},
			{RelPos: actionTag, ColPos: pkPos},
		},
	}}
	for i, idx := range indexes {
		positions := indexPositions[i]
		updateCtxList = append(updateCtxList, &plan.UpdateCtx{
			ObjRef:             idx.objRef,
			TableDef:           idx.tableDef,
			IgnoreAffectedRows: true,
			InsertCols: []plan.ColRef{
				{RelPos: actionTag, ColPos: positions.newKey},
				{RelPos: actionTag, ColPos: pkPos},
			},
			DeleteCols: []plan.ColRef{
				{RelPos: actionTag, ColPos: positions.oldRowID},
				{RelPos: actionTag, ColPos: positions.oldKey},
			},
		})
		if idx.def.Unique {
			lockTargets = append(lockTargets,
				&plan.LockTarget{
					TableId:            idx.tableDef.TblId,
					ObjRef:             idx.objRef,
					PrimaryColIdxInBat: positions.oldKey,
					PrimaryColRelPos:   actionTag,
					PrimaryColTyp:      actionProjection[positions.oldKey].Typ,
				},
				&plan.LockTarget{
					TableId:            idx.tableDef.TblId,
					ObjRef:             idx.objRef,
					PrimaryColIdxInBat: positions.newKey,
					PrimaryColRelPos:   actionTag,
					PrimaryColTyp:      actionProjection[positions.newKey].Typ,
				},
			)
		}
	}
	sortForeignKeyLockTargets(lockTargets, map[uint64]struct{}{childTableDef.TblId: {}})
	actionNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		Children:    []int32{actionNodeID},
		TableDef:    childTableDef,
		BindingTags: []int32{actionTag},
		LockTargets: lockTargets,
	}, bindCtx)
	actionNodeID = builder.appendNode(&plan.Node{
		NodeType:      plan.Node_MULTI_UPDATE,
		Children:      []int32{actionNodeID},
		BindingTags:   []int32{builder.genNewBindTag()},
		UpdateCtxList: updateCtxList,
	}, bindCtx)
	builder.appendStep(actionNodeID)
	builder.qry.HasForeignKeyAction = true
	return nil
}

func (builder *QueryBuilder) appendRowNumberMappingGuardNode(
	bindCtx *BindContext,
	lastNodeID int32,
	partitionByExprs []*plan.Expr,
	duplicateErrorMessage string,
	duplicateErrorType string,
) (int32, error) {
	if len(partitionByExprs) == 0 {
		return lastNodeID, nil
	}

	partitionBy := make([]*plan.OrderBySpec, 0, len(partitionByExprs))
	for _, expr := range partitionByExprs {
		partitionBy = append(partitionBy, &plan.OrderBySpec{
			Expr: expr,
			Flag: plan.OrderBySpec_INTERNAL,
		})
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType: plan.Node_PARTITION,
		Children: []int32{lastNodeID},
		OrderBy:  partitionBy,
	}, bindCtx)

	rowNumberFunc, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "row_number", nil)
	if err != nil {
		return 0, err
	}
	windowTag := builder.genNewBindTag()
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType: plan.Node_WINDOW,
		Children: []int32{lastNodeID},
		WinSpecList: []*plan.Expr{{
			Typ: rowNumberFunc.Typ,
			Expr: &plan.Expr_W{W: &plan.WindowSpec{
				WindowFunc:  rowNumberFunc,
				Name:        "row_number",
				PartitionBy: partitionByExprs,
				Frame: &plan.FrameClause{
					Type: plan.FrameClause_ROWS,
					Start: &plan.FrameBound{
						Type:      plan.FrameBound_PRECEDING,
						UnBounded: true,
					},
					End: &plan.FrameBound{
						Type:      plan.FrameBound_FOLLOWING,
						UnBounded: true,
					},
				},
			}},
		}},
		WindowIdx:   0,
		BindingTags: []int32{windowTag},
	}, bindCtx)

	rowNumberCol := &plan.Expr{
		Typ: rowNumberFunc.Typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: windowTag,
			ColPos: 0,
			Name:   "__mo_fk_mapping_row_number",
		}},
	}
	keepFirstRowExpr, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(), "=", []*plan.Expr{rowNumberCol, makePlan2Int64ConstExprWithType(1)})
	if err != nil {
		return 0, err
	}
	guardExpr := keepFirstRowExpr
	if duplicateErrorType != "" {
		guardExpr, err = BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"assert",
			[]*plan.Expr{
				keepFirstRowExpr,
				makePlan2StringConstExprWithType(duplicateErrorMessage),
				makePlan2StringConstExprWithType(duplicateErrorType),
			},
		)
		if err != nil {
			return 0, err
		}
	}
	return builder.appendNode(&plan.Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{lastNodeID},
		FilterList: []*plan.Expr{guardExpr},
	}, bindCtx), nil
}

func (builder *QueryBuilder) buildUpdateMutationIndexKey(
	tableDef *plan.TableDef,
	idxDef *plan.IndexDef,
	tableTag int32,
	replacements map[int32]*plan.Expr,
) (*plan.Expr, error) {
	if isSpatialIndexDef(idxDef) {
		pkPos := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
		if replacement, ok := replacements[pkPos]; ok {
			return DeepCopyExpr(replacement), nil
		}
		return &plan.Expr{
			Typ: tableDef.Cols[pkPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: tableTag,
				ColPos: pkPos,
			}},
		}, nil
	}

	prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
	if err != nil {
		return nil, err
	}
	args := make([]*plan.Expr, len(idxDef.Parts))
	for i, part := range idxDef.Parts {
		colName := catalog.ResolveAlias(part)
		colPos := tableDef.Name2ColIndex[colName]
		inputExpr, ok := replacements[colPos]
		if ok {
			inputExpr = DeepCopyExpr(inputExpr)
		} else {
			inputExpr = &plan.Expr{
				Typ: tableDef.Cols[colPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: tableTag,
					ColPos: colPos,
				}},
			}
		}
		args[i], err = builder.makeIndexPartExprFromInputExpr(
			inputExpr,
			colName,
			prefixLengths,
		)
		if err != nil {
			return nil, err
		}
	}
	if !indexTableStoresSerializedKey(idxDef) {
		return args[0], nil
	}
	funcName := "serial"
	if !idxDef.Unique {
		funcName = "serial_full"
	}
	return BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, args)
}

func (builder *QueryBuilder) appendUpdateParentRestrictCheck(
	bindCtx *BindContext,
	parentTableDef *plan.TableDef,
	parentAlias string,
	affectedFK updateParentForeignKey,
	lastNodeID int32,
	selectNodeTag int32,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	targetSelected *plan.Expr,
	filterInvalid bool,
	allowSelfReferencingNull bool,
) (int32, int32, error) {
	sourceNode := builder.updateInputProjectNode(lastNodeID)
	sourceTypes := make([]plan.Type, len(sourceNode.ProjectList))
	for i, expr := range sourceNode.ProjectList {
		sourceTypes[i] = expr.Typ
	}

	parentColIDToName := make(map[uint64]string, len(parentTableDef.Cols))
	for _, col := range parentTableDef.Cols {
		parentColIDToName[col.ColId] = col.Name
	}
	childColIDToPos := make(map[uint64]int32, len(affectedFK.childTableDef.Cols))
	for i, col := range affectedFK.childTableDef.Cols {
		childColIDToPos[col.ColId] = int32(i)
	}

	childTag := builder.genNewBindTag()
	builder.addNameByColRef(childTag, affectedFK.childTableDef)
	childScanID := builder.appendNode(&plan.Node{
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     affectedFK.childTableDef,
		ObjRef:       affectedFK.childObjRef,
		BindingTags:  []int32{childTag},
		ScanSnapshot: bindCtx.snapshot,
	}, bindCtx)

	joinPreds := make([]*plan.Expr, len(affectedFK.fk.Cols))
	for i, childColID := range affectedFK.fk.Cols {
		parentColName := parentColIDToName[affectedFK.fk.ForeignCols[i]]
		parentPos := oldColName2Idx[parentAlias+"."+parentColName]
		childPos := childColIDToPos[childColID]
		parentExpr := &plan.Expr{
			Typ: sourceNode.ProjectList[parentPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: selectNodeTag,
				ColPos: parentPos,
			}},
		}
		childExpr := &plan.Expr{
			Typ: affectedFK.childTableDef.Cols[childPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: childTag,
				ColPos: childPos,
			}},
		}
		var err error
		joinPreds[i], err = BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"=",
			[]*plan.Expr{parentExpr, childExpr},
		)
		if err != nil {
			return 0, 0, err
		}
	}

	markNodeID, markExpr, err := builder.insertMarkJoin(
		lastNodeID,
		childScanID,
		joinPreds,
		nil,
		false,
		bindCtx,
	)
	if err != nil {
		return 0, 0, err
	}

	unchanged, err := builder.buildUpdateReferencedKeyUnchangedExpr(
		parentTableDef,
		affectedFK.fk,
		parentAlias,
		selectNodeTag,
		oldColName2Idx,
		newColName2Idx,
		sourceNode,
	)
	if err != nil {
		return 0, 0, err
	}
	noChild, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(),
		"not",
		[]*plan.Expr{markExpr},
	)
	if err != nil {
		return 0, 0, err
	}
	ok := noChild
	if unchanged != nil {
		ok, err = BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"or",
			[]*plan.Expr{unchanged, noChild},
		)
		if err != nil {
			return 0, 0, err
		}
	}
	if allowSelfReferencingNull && affectedFK.childTableDef.TblId == parentTableDef.TblId {
		var newKeyHasNull *plan.Expr
		for _, parentColID := range affectedFK.fk.ForeignCols {
			parentColName := parentColIDToName[parentColID]
			parentPos := newColName2Idx[parentAlias+"."+parentColName]
			newKeyExpr := &plan.Expr{
				Typ: sourceNode.ProjectList[parentPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: selectNodeTag,
					ColPos: parentPos,
				}},
			}
			isNull, buildErr := BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"isnull",
				[]*plan.Expr{newKeyExpr},
			)
			if buildErr != nil {
				return 0, 0, buildErr
			}
			if newKeyHasNull == nil {
				newKeyHasNull = isNull
				continue
			}
			newKeyHasNull, buildErr = BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"or",
				[]*plan.Expr{newKeyHasNull, isNull},
			)
			if buildErr != nil {
				return 0, 0, buildErr
			}
		}
		if newKeyHasNull != nil {
			ok, err = BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"or",
				[]*plan.Expr{ok, newKeyHasNull},
			)
			if err != nil {
				return 0, 0, err
			}
		}
	}
	if targetSelected != nil {
		notSelected, buildErr := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "not", []*plan.Expr{DeepCopyExpr(targetSelected)})
		if buildErr != nil {
			return 0, 0, buildErr
		}
		ok, buildErr = BindFuncExprImplByPlanExpr(
			builder.GetContext(), "or", []*plan.Expr{notSelected, ok})
		if buildErr != nil {
			return 0, 0, buildErr
		}
	}
	filterExpr := ok
	if !filterInvalid {
		filterExpr, err = BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"assert",
			[]*plan.Expr{
				ok,
				makePlan2StringConstExprWithType(
					"Cannot delete or update a parent row: a foreign key constraint fails",
				),
				makePlan2StringConstExprWithType(foreignKeyRowIsReferencedAssert),
			},
		)
		if err != nil {
			return 0, 0, err
		}
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{markNodeID},
		FilterList: []*plan.Expr{filterExpr},
	}, bindCtx)

	validatedTag := builder.genNewBindTag()
	projectList := make([]*plan.Expr, len(sourceTypes))
	for i, typ := range sourceTypes {
		projectList[i] = &plan.Expr{
			Typ: typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: selectNodeTag,
				ColPos: int32(i),
			}},
		}
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: projectList,
		Children:    []int32{lastNodeID},
		BindingTags: []int32{validatedTag},
	}, bindCtx)
	return lastNodeID, validatedTag, nil
}

func (builder *QueryBuilder) buildUpdateReferencedKeyUnchangedExpr(
	parentTableDef *plan.TableDef,
	fk *plan.ForeignKeyDef,
	parentAlias string,
	selectNodeTag int32,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	selectNode *plan.Node,
) (*plan.Expr, error) {
	parentColIDToName := make(map[uint64]string, len(parentTableDef.Cols))
	for _, col := range parentTableDef.Cols {
		parentColIDToName[col.ColId] = col.Name
	}

	var unchanged *plan.Expr
	for _, parentColID := range fk.ForeignCols {
		qualifiedName := parentAlias + "." + parentColIDToName[parentColID]
		newPos, updated := newColName2Idx[qualifiedName]
		if !updated {
			continue
		}
		oldPos := oldColName2Idx[qualifiedName]
		oldExpr := &plan.Expr{
			Typ: selectNode.ProjectList[oldPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: selectNodeTag,
				ColPos: oldPos,
			}},
		}
		newExpr := &plan.Expr{
			Typ: selectNode.ProjectList[newPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: selectNodeTag,
				ColPos: newPos,
			}},
		}
		equal, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"<=>",
			[]*plan.Expr{oldExpr, newExpr},
		)
		if err != nil {
			return nil, err
		}
		if unchanged == nil {
			unchanged = equal
			continue
		}
		unchanged, err = BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"and",
			[]*plan.Expr{unchanged, equal},
		)
		if err != nil {
			return nil, err
		}
	}
	return unchanged, nil
}

func (builder *QueryBuilder) buildUpdateFkUnchangedExpr(
	tableDef *plan.TableDef,
	fk *plan.ForeignKeyDef,
	alias string,
	selectNodeTag int32,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	selectNode *plan.Node,
) (*plan.Expr, error) {
	colIDToName := make(map[uint64]string, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		colIDToName[col.ColId] = col.Name
	}

	var unchanged *plan.Expr
	for _, childColID := range fk.Cols {
		qualifiedName := alias + "." + colIDToName[childColID]
		newPos, updated := newColName2Idx[qualifiedName]
		if !updated {
			continue
		}
		oldPos := oldColName2Idx[qualifiedName]
		oldExpr := &plan.Expr{
			Typ: selectNode.ProjectList[oldPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: selectNodeTag,
					ColPos: oldPos,
				},
			},
		}
		newExpr := &plan.Expr{
			Typ: selectNode.ProjectList[newPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: selectNodeTag,
					ColPos: newPos,
				},
			},
		}
		equal, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"<=>",
			[]*plan.Expr{oldExpr, newExpr},
		)
		if err != nil {
			return nil, err
		}
		if unchanged == nil {
			unchanged = equal
			continue
		}
		unchanged, err = BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"and",
			[]*plan.Expr{unchanged, equal},
		)
		if err != nil {
			return nil, err
		}
	}
	return unchanged, nil
}
