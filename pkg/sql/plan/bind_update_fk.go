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
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
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
	physicalTargetActivePos []int32,
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
		canonicalRepeatedTarget := repeatedPhysicalTarget && !deferRepeatedPhysicalTargetMerge &&
			(len(tableDef.Fkeys) > 0 || len(tableDef.RefChildTbls) > 0)
		if repeatedPhysicalTarget && !deferRepeatedPhysicalTargetMerge {
			if _, handled := handledFinalTable[tableDef.TblId]; handled {
				continue
			}
			handledFinalTable[tableDef.TblId] = struct{}{}
		}

		alias := dmlCtx.aliases[i]
		var targetSelected *plan.Expr
		if canonicalRepeatedTarget {
			var canonicalErr error
			lastNodeID, selectNodeTag, selectNode, targetSelected, canonicalErr =
				builder.appendRepeatedPhysicalTargetCanonicalProjection(
					bindCtx,
					dmlCtx,
					tableDef,
					alias,
					lastNodeID,
					selectNodeTag,
					selectNode,
					oldColName2Idx,
					newColName2Idx,
					targetRowNumberPos,
					targetBranchActivePos,
				)
			if canonicalErr != nil {
				return 0, 0, nil, canonicalErr
			}
		} else if targetRowNumberPos[i] >= 0 {
			targetSelected, err = builder.buildTargetSelectedExpr(
				selectNodeTag, selectNode, targetRowNumberPos[i], targetBranchActivePos[i])
			if err != nil {
				return 0, 0, nil, err
			}
		}
		if !repeatedPhysicalTarget || !deferRepeatedPhysicalTargetMerge {
			selfTargetSelectors := &updateSelfTargetSelectors{
				targetRowNumberPos:      targetRowNumberPos,
				targetActivePos:         targetBranchActivePos,
				targetRowIDPos:          make([]int32, len(dmlCtx.tableDefs)),
				physicalTargetActivePos: physicalTargetActivePos,
			}
			for targetIdx := range selfTargetSelectors.targetRowIDPos {
				selfTargetSelectors.targetRowIDPos[targetIdx] = -1
			}
			for targetIdx, targetDef := range dmlCtx.tableDefs {
				if len(dmlCtx.updateCol2Expr[targetIdx]) > 0 && targetDef.TblId == tableDef.TblId {
					selfTargetSelectors.targetIndexes = append(
						selfTargetSelectors.targetIndexes, targetIdx)
					selfTargetSelectors.targetRowIDPos[targetIdx] =
						oldColName2Idx[dmlCtx.aliases[targetIdx]+"."+catalog.Row_ID]
				}
			}
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
				selfTargetSelectors,
				nil,
			)
			if err != nil {
				return 0, 0, nil, err
			}
			selectNode = builder.updateInputProjectNode(lastNodeID)
		}
		if targetRowNumberPos[i] >= 0 {
			activePos := targetBranchActivePos[i]
			if canonicalRepeatedTarget {
				activePos = physicalTargetActivePos[i]
			}
			targetSelected, err = builder.buildTargetSelectedExpr(
				selectNodeTag, selectNode, targetRowNumberPos[i], activePos)
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
			nil,
			nil,
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

type updateSelfMutationRows struct {
	nodeID         int32
	tag            int32
	rootSourceStep int32
}

type updateAffectedRowsColumn struct {
	pos int32
}

type updateForeignKeyActionEdgeKey struct {
	parentTableID uint64
	childTableID  uint64
	signature     string
}

func makeUpdateForeignKeyActionEdgeKey(
	parentTableID uint64,
	childTableID uint64,
	fk *plan.ForeignKeyDef,
) updateForeignKeyActionEdgeKey {
	return updateForeignKeyActionEdgeKey{
		parentTableID: parentTableID,
		childTableID:  childTableID,
		signature: fmt.Sprintf(
			"%s/%v/%v", fk.Name, fk.Cols, fk.ForeignCols),
	}
}

type updateSelfTargetSelectors struct {
	targetIndexes           []int
	targetRowNumberPos      []int32
	targetActivePos         []int32
	targetRowIDPos          []int32
	physicalTargetActivePos []int32
}

// appendRepeatedPhysicalTargetCanonicalProjection gives FK checks one physical
// row image when several writable aliases refer to the same table. Each alias
// keeps its own semantic active column, while the parent action is eligible when
// any alias is active. Old/new values are selected from the active alias rather
// than implicitly taking the first alias in planner order.
func (builder *QueryBuilder) appendRepeatedPhysicalTargetCanonicalProjection(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	tableDef *plan.TableDef,
	canonicalAlias string,
	lastNodeID int32,
	inputTag int32,
	inputNode *plan.Node,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	targetRowNumberPos []int32,
	targetActivePos []int32,
) (int32, int32, *plan.Node, *plan.Expr, error) {
	targetIndexes := make([]int, 0, 2)
	selectors := make([]*plan.Expr, 0, 2)
	for targetIdx, targetDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[targetIdx]) == 0 || targetDef.TblId != tableDef.TblId {
			continue
		}
		if targetRowNumberPos[targetIdx] < 0 {
			return 0, 0, nil, nil, moerr.NewInternalError(
				builder.GetContext(), "repeated physical UPDATE target selector is unavailable")
		}
		selected, err := builder.buildTargetSelectedExpr(
			inputTag, inputNode, targetRowNumberPos[targetIdx], targetActivePos[targetIdx])
		if err != nil {
			return 0, 0, nil, nil, err
		}
		targetIndexes = append(targetIndexes, targetIdx)
		selectors = append(selectors, selected)
	}
	if len(selectors) == 0 {
		return 0, 0, nil, nil, moerr.NewInternalError(
			builder.GetContext(), "repeated physical UPDATE target has no writable alias")
	}

	project := make([]*plan.Expr, len(inputNode.ProjectList))
	for pos, expr := range inputNode.ProjectList {
		project[pos] = &plan.Expr{
			Typ:  expr.Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: inputTag, ColPos: int32(pos)}},
		}
	}
	buildSelectedValue := func(values []*plan.Expr) (*plan.Expr, error) {
		value := DeepCopyExpr(values[len(values)-1])
		for idx := len(values) - 2; idx >= 0; idx-- {
			var err error
			value, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "case", []*plan.Expr{
				DeepCopyExpr(selectors[idx]), DeepCopyExpr(values[idx]), value,
			})
			if err != nil {
				return nil, err
			}
		}
		return value, nil
	}
	for _, col := range tableDef.Cols {
		// ROWID has no CASE overload. Self-action matching and root exclusion
		// preserve every alias ROWID independently through targetRowIDPos.
		if col.Name == catalog.Row_ID {
			continue
		}
		oldValues := make([]*plan.Expr, len(targetIndexes))
		updatedValues := make([]*plan.Expr, len(targetIndexes))
		changed := false
		for idx, targetIdx := range targetIndexes {
			qualifiedName := dmlCtx.aliases[targetIdx] + "." + col.Name
			oldPos, ok := oldColName2Idx[qualifiedName]
			if !ok {
				return 0, 0, nil, nil, moerr.NewInternalErrorf(
					builder.GetContext(), "repeated physical UPDATE old column %s is unavailable", col.Name)
			}
			oldValues[idx] = &plan.Expr{
				Typ:  inputNode.ProjectList[oldPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: inputTag, ColPos: oldPos}},
			}
			if newPos, updated := newColName2Idx[qualifiedName]; updated {
				changed = true
				updatedValues[idx] = &plan.Expr{
					Typ:  inputNode.ProjectList[newPos].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: inputTag, ColPos: newPos}},
				}
			}
		}
		oldValue, err := buildSelectedValue(oldValues)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		oldColName2Idx[canonicalAlias+"."+col.Name] = int32(len(project))
		project = append(project, oldValue)
		if changed {
			// Assignments from overlapping aliases merge column-by-column. An
			// active alias that did not update this column must not mask another
			// active alias's explicit value.
			newValue := DeepCopyExpr(oldValue)
			for idx := len(updatedValues) - 1; idx >= 0; idx-- {
				if updatedValues[idx] == nil {
					continue
				}
				var buildErr error
				newValue, buildErr = BindFuncExprImplByPlanExpr(builder.GetContext(), "case", []*plan.Expr{
					DeepCopyExpr(selectors[idx]), DeepCopyExpr(updatedValues[idx]), newValue,
				})
				if buildErr != nil {
					return 0, 0, nil, nil, buildErr
				}
			}
			newColName2Idx[canonicalAlias+"."+col.Name] = int32(len(project))
			project = append(project, newValue)
		}
	}
	physicalSelected := DeepCopyExpr(selectors[0])
	for idx := 1; idx < len(selectors); idx++ {
		var err error
		physicalSelected, err = BindFuncExprImplByPlanExpr(
			builder.GetContext(), "or", []*plan.Expr{physicalSelected, DeepCopyExpr(selectors[idx])})
		if err != nil {
			return 0, 0, nil, nil, err
		}
	}
	physicalSelectedPos := int32(len(project))
	project = append(project, physicalSelected)
	outputTag := builder.genNewBindTag()
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		ProjectList: project,
		BindingTags: []int32{outputTag},
	}, bindCtx)
	outputNode := builder.qry.Nodes[lastNodeID]
	return lastNodeID, outputTag, outputNode, &plan.Expr{
		Typ:  outputNode.ProjectList[physicalSelectedPos].Typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: outputTag, ColPos: physicalSelectedPos}},
	}, nil
}

// validateDistinctUpdateForeignKeyMutationTargets builds the complete mutating
// ON UPDATE action graph before any action MULTI_UPDATE step is appended. The
// current planner can fold self actions into their physical parent's writer,
// but it cannot merge row images produced by independent parent paths. Reject
// those converging paths rather than emitting multiple writers for one table.
func (builder *QueryBuilder) validateDistinctUpdateForeignKeyMutationTargets(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	newColName2Idx map[string]int32,
) error {
	enabled, err := IsForeignKeyChecksEnabled(builder.compCtx)
	if err != nil || !enabled {
		return err
	}

	type writeOrigin struct {
		description string
	}
	type actionRoot struct {
		tableDef    *plan.TableDef
		changedCols map[uint64]struct{}
		origin      string
	}

	writeOrigins := make(map[uint64]writeOrigin)
	rootsByTableID := make(map[uint64]*actionRoot)
	rootOrder := make([]uint64, 0)
	for targetIdx, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) == 0 {
			continue
		}
		tableDef := dmlCtx.tableDefs[targetIdx]
		if tableDef == nil {
			continue
		}
		root := rootsByTableID[tableDef.TblId]
		if root == nil {
			root = &actionRoot{
				tableDef:    tableDef,
				changedCols: make(map[uint64]struct{}),
				origin:      fmt.Sprintf("explicit target '%s'", dmlCtx.aliases[targetIdx]),
			}
			rootsByTableID[tableDef.TblId] = root
			rootOrder = append(rootOrder, tableDef.TblId)
			writeOrigins[tableDef.TblId] = writeOrigin{description: root.origin}
		}
		alias := dmlCtx.aliases[targetIdx]
		for _, col := range tableDef.Cols {
			if _, changed := newColName2Idx[alias+"."+col.Name]; changed {
				root.changedCols[col.ColId] = struct{}{}
			}
		}
	}

	collectAffected := func(
		parentTableDef *plan.TableDef,
		changedCols map[uint64]struct{},
	) ([]updateParentForeignKey, error) {
		if parentTableDef == nil || len(parentTableDef.RefChildTbls) == 0 || len(changedCols) == 0 {
			return nil, nil
		}
		affected := make([]updateParentForeignKey, 0)
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
				return nil, resolveErr
			}
			if childTableDef == nil {
				return nil, moerr.NewInternalErrorf(
					builder.GetContext(), "foreign-key child table %d not found", childTableID)
			}
			for _, fk := range childTableDef.Fkeys {
				referencesParent := fk.ForeignTbl == parentTableDef.TblId ||
					(fk.ForeignTbl == 0 && childTableDef.TblId == parentTableDef.TblId)
				if !referencesParent ||
					(fk.OnUpdate != plan.ForeignKeyDef_CASCADE && fk.OnUpdate != plan.ForeignKeyDef_SET_NULL) {
					continue
				}
				for _, parentColID := range fk.ForeignCols {
					if _, changed := changedCols[parentColID]; changed {
						affected = append(affected, updateParentForeignKey{
							childObjRef: childObjRef, childTableDef: childTableDef, fk: fk,
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

	multipleActionsError := func() error {
		return newRejectedUpdatePlannerRouteError(
			updateRouteReasonForeignKey,
			moerr.NewUnsupportedDML(
				builder.GetContext(), "multiple parent foreign key actions targeting the same child table"),
		)
	}
	var walkActionGraph func(*plan.TableDef, map[uint64]struct{}) error
	walkActionGraph = func(
		parentTableDef *plan.TableDef,
		changedCols map[uint64]struct{},
	) error {
		handledSelfEdges := make(map[updateForeignKeyActionEdgeKey]struct{})
		for {
			affected, collectErr := collectAffected(parentTableDef, changedCols)
			if collectErr != nil {
				return collectErr
			}
			addedSelfChange := false
			for _, action := range affected {
				if action.childTableDef.TblId != parentTableDef.TblId {
					continue
				}
				edgeKey := makeUpdateForeignKeyActionEdgeKey(
					parentTableDef.TblId, action.childTableDef.TblId, action.fk)
				if _, handled := handledSelfEdges[edgeKey]; handled {
					continue
				}
				if len(handledSelfEdges) > 0 {
					return multipleActionsError()
				}
				handledSelfEdges[edgeKey] = struct{}{}
				for _, childColID := range action.fk.Cols {
					if _, changed := changedCols[childColID]; !changed {
						changedCols[childColID] = struct{}{}
						addedSelfChange = true
					}
				}
			}
			if !addedSelfChange {
				break
			}
		}

		affected, collectErr := collectAffected(parentTableDef, changedCols)
		if collectErr != nil {
			return collectErr
		}
		childrenFromParent := make(map[uint64]struct{})
		for _, action := range affected {
			childTableID := action.childTableDef.TblId
			if childTableID == parentTableDef.TblId {
				continue
			}
			if _, duplicate := childrenFromParent[childTableID]; duplicate {
				return multipleActionsError()
			}
			childrenFromParent[childTableID] = struct{}{}

			actionOrigin := fmt.Sprintf(
				"foreign key action '%s' from table '%s'",
				action.fk.Name,
				parentTableDef.Name,
			)
			if previousOrigin, exists := writeOrigins[childTableID]; exists {
				childName := action.childTableDef.Name
				if action.childObjRef != nil && action.childObjRef.ObjName != "" {
					childName = action.childObjRef.ObjName
				}
				return newUpdatePlannerRouteError(
					updatePlannerRejected,
					updateRouteReasonForeignKey,
					moerr.NewNotSupportedf(
						builder.GetContext(),
						"overlapping update paths for table '%s': %s and %s",
						childName,
						previousOrigin.description,
						actionOrigin,
					),
				)
			}
			writeOrigins[childTableID] = writeOrigin{description: actionOrigin}
			childChangedCols := make(map[uint64]struct{}, len(action.fk.Cols))
			for _, childColID := range action.fk.Cols {
				childChangedCols[childColID] = struct{}{}
			}
			if err := walkActionGraph(action.childTableDef, childChangedCols); err != nil {
				return err
			}
		}
		return nil
	}

	for _, tableID := range rootOrder {
		root := rootsByTableID[tableID]
		if err := walkActionGraph(root.tableDef, root.changedCols); err != nil {
			return err
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
	selfTargetSelectors *updateSelfTargetSelectors,
	excludedMutationEdges map[updateForeignKeyActionEdgeKey]struct{},
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
	if builder.updateParentActionStack == nil {
		builder.updateParentActionStack = make(map[uint64]int)
	}
	continuingCurrentWriter := excludedMutationEdges != nil
	if builder.updateParentActionStack[tableDef.TblId] > 0 && !continuingCurrentWriter {
		return 0, 0, newRejectedUpdatePlannerRouteError(
			updateRouteReasonForeignKey,
			moerr.NewUnsupportedDML(builder.GetContext(), "cyclic parent foreign key action graph"),
		)
	}
	if !continuingCurrentWriter {
		builder.updateParentActionStack[tableDef.TblId]++
		defer func() {
			builder.updateParentActionStack[tableDef.TblId]--
		}()
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
			edgeKey := makeUpdateForeignKeyActionEdgeKey(
				tableDef.TblId, affectedFK.childTableDef.TblId, affectedFK.fk)
			if _, excluded := excludedMutationEdges[edgeKey]; excluded {
				continue
			}
			mutations = append(mutations, affectedFK)
		}
	}
	if len(mutations) == 0 {
		return lastNodeID, selectNodeTag, nil
	}

	mutationByChild := make(map[uint64]struct{}, len(mutations))
	for _, mutation := range mutations {
		if _, exists := mutationByChild[mutation.childTableDef.TblId]; exists {
			return 0, 0, newRejectedUpdatePlannerRouteError(
				updateRouteReasonForeignKey,
				moerr.NewUnsupportedDML(
					builder.GetContext(),
					"multiple parent foreign key actions targeting the same child table",
				),
			)
		}
		mutationByChild[mutation.childTableDef.TblId] = struct{}{}
		if err := builder.validateModernUpdateParentMutation(tableDef, mutation); err != nil {
			return 0, 0, err
		}
	}

	recursiveExcludedEdges := make(map[updateForeignKeyActionEdgeKey]struct{},
		len(excludedMutationEdges)+len(mutations))
	for edgeKey := range excludedMutationEdges {
		recursiveExcludedEdges[edgeKey] = struct{}{}
	}
	for _, mutation := range mutations {
		recursiveExcludedEdges[makeUpdateForeignKeyActionEdgeKey(
			tableDef.TblId, mutation.childTableDef.TblId, mutation.fk)] = struct{}{}
	}

	remainingMutations := make([]updateParentForeignKey, 0, len(mutations))
	for _, mutation := range mutations {
		if mutation.childTableDef.TblId != tableDef.TblId {
			remainingMutations = append(remainingMutations, mutation)
			continue
		}
		// A self action writes the same physical table as the root UPDATE. Fold
		// its non-root transition rows into the root stream so one MULTI_UPDATE
		// owns every row and regular hidden-index maintenance stays atomic.
		sourceSinkID := appendSinkNodeWithTag(builder, bindCtx, lastNodeID, selectNodeTag)
		builder.qry.Nodes[sourceSinkID].ExtraOptions = materialized.CTESinkOption
		sourceStep := builder.appendStep(sourceSinkID)
		// Keep every materialized source at two readers. The root source feeds
		// the final root branch and this action-source copy; the copy then feeds
		// the parent mapping and explicit-root exclusion joins.
		actionSourceID := builder.appendTaggedSinkScan(bindCtx, sourceStep, selectNodeTag)
		actionSourceSinkID := appendSinkNodeWithTag(
			builder, bindCtx, actionSourceID, selectNodeTag)
		builder.qry.Nodes[actionSourceSinkID].ExtraOptions = materialized.CTESinkOption
		actionSourceStep := builder.appendStep(actionSourceSinkID)
		selfRows := &updateSelfMutationRows{rootSourceStep: sourceStep}
		if err := builder.appendUpdateParentMutation(
			bindCtx,
			tableDef,
			alias,
			mutation,
			actionSourceStep,
			selectNodeTag,
			oldColName2Idx,
			newColName2Idx,
			targetSelected,
			true,
			selfRows,
			selfTargetSelectors,
			recursiveExcludedEdges,
		); err != nil {
			return 0, 0, err
		}
		lastNodeID = selfRows.nodeID
		selectNodeTag = selfRows.tag
		if selfTargetSelectors != nil {
			targetSelected = nil
			for _, targetIdx := range selfTargetSelectors.targetIndexes {
				rowNumberPos := selfTargetSelectors.targetRowNumberPos[targetIdx]
				if rowNumberPos < 0 {
					continue
				}
				var buildErr error
				targetSelected, buildErr = builder.buildTargetSelectedExpr(
					selectNodeTag,
					builder.updateInputProjectNode(lastNodeID),
					rowNumberPos,
					selfTargetSelectors.physicalTargetActivePos[targetIdx],
				)
				if buildErr != nil {
					return 0, 0, buildErr
				}
				break
			}
		}
	}
	if len(remainingMutations) == 0 {
		return lastNodeID, selectNodeTag, nil
	}

	sourceSinkID := appendSinkNodeWithTag(builder, bindCtx, lastNodeID, selectNodeTag)
	builder.qry.Nodes[sourceSinkID].ExtraOptions = materialized.CTESinkOption
	sourceStep := builder.appendStep(sourceSinkID)
	for _, mutation := range remainingMutations {
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
			false,
			nil,
			nil,
			nil,
		); err != nil {
			return 0, 0, err
		}
	}
	lastNodeID = builder.appendTaggedSinkScan(bindCtx, sourceStep, selectNodeTag)
	return lastNodeID, selectNodeTag, nil
}

func (builder *QueryBuilder) appendUpdateRetagProject(
	bindCtx *BindContext,
	lastNodeID int32,
	inputTag int32,
	outputTag int32,
) int32 {
	inputProject := getProjectionByLastNodeWithTag(builder, lastNodeID, inputTag)
	projectList := make([]*plan.Expr, len(inputProject))
	for i, expr := range inputProject {
		projectList[i] = &plan.Expr{Typ: expr.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: inputTag, ColPos: int32(i),
		}}}
	}
	return builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{lastNodeID},
		ProjectList: projectList, BindingTags: []int32{outputTag},
	}, bindCtx)
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
	parentTableDef *plan.TableDef,
	affectedFK updateParentForeignKey,
) error {
	childTableDef := affectedFK.childTableDef
	ensureName2ColIndexForReplace(childTableDef)
	if err := builder.validateModernUpdateParentRowClosure(parentTableDef, affectedFK); err != nil {
		return err
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
	selfReferencing bool,
	selfRows *updateSelfMutationRows,
	selfTargetSelectors *updateSelfTargetSelectors,
	excludedMutationEdges map[updateForeignKeyActionEdgeKey]struct{},
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

	primaryKeyChanged := false
	primaryNames := make(map[string]struct{}, len(childTableDef.Pkey.Names)+1)
	for _, name := range childTableDef.Pkey.Names {
		primaryNames[catalog.ResolveAlias(name)] = struct{}{}
	}
	if len(childTableDef.Pkey.Names) <= 1 && childTableDef.Pkey.PkeyColName != "" {
		primaryNames[catalog.ResolveAlias(childTableDef.Pkey.PkeyColName)] = struct{}{}
	}
	for childPos := range newChildExprs {
		if _, isPrimary := primaryNames[catalog.ResolveAlias(childTableDef.Cols[childPos].Name)]; isPrimary {
			primaryKeyChanged = true
			break
		}
	}
	joinNodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{parentNodeID, childNodeID},
		JoinType: plan.Node_INNER,
		OnList:   joinPreds,
	}, bindCtx)
	var selfAffectedRowsExprs []*plan.Expr
	if selfReferencing {
		// Collapse the child scan and parent transition into one binding before
		// the explicit-root LEFT join. Keeping the parent value under its old
		// binding lets join reordering place that binding on a nullable side when
		// the later join is compiled, which turns a valid CASCADE value into NULL.
		actionBaseTag := builder.genNewBindTag()
		actionBaseProject := make([]*plan.Expr, 0, len(childTableDef.Cols)+len(newChildExprs))
		for colPos, col := range childTableDef.Cols {
			actionBaseProject = append(actionBaseProject, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: childTag, ColPos: int32(colPos),
				}},
			})
		}
		actionPositions := make([]int, 0, len(newChildExprs))
		for colPos := range newChildExprs {
			actionPositions = append(actionPositions, int(colPos))
		}
		sort.Ints(actionPositions)
		physicalActionExprs := make(map[int32]*plan.Expr, len(newChildExprs))
		for _, rawColPos := range actionPositions {
			colPos := int32(rawColPos)
			physicalPos := int32(len(actionBaseProject))
			actionBaseProject = append(actionBaseProject, newChildExprs[colPos])
			physicalActionExprs[colPos] = &plan.Expr{
				Typ: childTableDef.Cols[colPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: actionBaseTag, ColPos: physicalPos,
				}},
			}
		}
		joinNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_PROJECT, Children: []int32{joinNodeID},
			ProjectList: actionBaseProject, BindingTags: []int32{actionBaseTag},
		}, bindCtx)
		actionBaseSinkID := appendSinkNodeWithTag(builder, bindCtx, joinNodeID, actionBaseTag)
		if builder.preserveSinkProjection == nil {
			builder.preserveSinkProjection = make(map[int32]struct{})
		}
		builder.preserveSinkProjection[actionBaseSinkID] = struct{}{}
		actionBaseStep := builder.appendStep(actionBaseSinkID)
		joinNodeID = builder.appendTaggedSinkScan(bindCtx, actionBaseStep, actionBaseTag)
		if builder.preserveScanProjection == nil {
			builder.preserveScanProjection = make(map[int32]struct{})
		}
		builder.preserveScanProjection[joinNodeID] = struct{}{}
		childTag = actionBaseTag
		newChildExprs = physicalActionExprs

		explicitRootID := builder.appendTaggedSinkScan(bindCtx, sourceStep, sourceTag)
		explicitRootTag := builder.genNewBindTag()
		explicitRootID = builder.appendUpdateRetagProject(
			bindCtx, explicitRootID, sourceTag, explicitRootTag)
		if targetSelected != nil {
			explicitFilter := DeepCopyExpr(targetSelected)
			replaceColRefTag(explicitFilter, sourceTag, explicitRootTag)
			explicitRootID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_FILTER, Children: []int32{explicitRootID},
				FilterList: []*plan.Expr{explicitFilter},
			}, bindCtx)
		}
		childRowIDPos := childTableDef.Name2ColIndex[catalog.Row_ID]
		rootRowIDPos := oldColName2Idx[parentAlias+"."+catalog.Row_ID]
		rootRowIDPositions := []int32{rootRowIDPos}
		if selfTargetSelectors != nil && len(selfTargetSelectors.targetIndexes) > 0 {
			rootRowIDPositions = rootRowIDPositions[:0]
			for _, targetIdx := range selfTargetSelectors.targetIndexes {
				if pos := selfTargetSelectors.targetRowIDPos[targetIdx]; pos >= 0 {
					rootRowIDPositions = append(rootRowIDPositions, pos)
				}
			}
		}
		var matchExplicitRoot *plan.Expr
		for _, candidateRowIDPos := range rootRowIDPositions {
			match, buildErr := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
				{Typ: childTableDef.Cols[childRowIDPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: childTag, ColPos: childRowIDPos,
				}}},
				{Typ: parentNode.ProjectList[candidateRowIDPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: explicitRootTag, ColPos: candidateRowIDPos,
				}}},
			})
			if buildErr != nil {
				return buildErr
			}
			if matchExplicitRoot == nil {
				matchExplicitRoot = match
			} else {
				matchExplicitRoot, buildErr = BindFuncExprImplByPlanExpr(
					builder.GetContext(), "or", []*plan.Expr{matchExplicitRoot, match})
				if buildErr != nil {
					return buildErr
				}
			}
		}
		joinNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN, Children: []int32{joinNodeID, explicitRootID},
			JoinType: plan.Node_LEFT, OnList: []*plan.Expr{matchExplicitRoot},
		}, bindCtx)
		matchedRoot, buildErr := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "isnotnull", []*plan.Expr{{
				Typ: parentNode.ProjectList[rootRowIDPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: explicitRootTag, ColPos: rootRowIDPos,
				}},
			}})
		if buildErr != nil {
			return buildErr
		}
		multiTargetSelectors := false
		if selfTargetSelectors != nil {
			for _, targetIdx := range selfTargetSelectors.targetIndexes {
				if selfTargetSelectors.targetRowNumberPos[targetIdx] >= 0 {
					multiTargetSelectors = true
					break
				}
			}
		}
		if multiTargetSelectors {
			selfAffectedRowsExprs = make([]*plan.Expr, 0, len(selfTargetSelectors.targetIndexes))
			for _, targetIdx := range selfTargetSelectors.targetIndexes {
				activePos := selfTargetSelectors.targetActivePos[targetIdx]
				selfAffectedRowsExprs = append(selfAffectedRowsExprs, &plan.Expr{
					Typ: parentNode.ProjectList[activePos].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: explicitRootTag, ColPos: activePos,
					}},
				})
			}
		} else {
			selfAffectedRowsExprs = []*plan.Expr{DeepCopyExpr(matchedRoot)}
		}
		for colPos, col := range childTableDef.Cols {
			if col.Name == catalog.Row_ID {
				continue
			}
			if _, foreignKeyActionValue := newChildExprs[int32(colPos)]; foreignKeyActionValue {
				// The referential action overrides the explicit root image for its
				// child columns, matching the legacy self-cascade contract.
				continue
			}
			qualifiedName := parentAlias + "." + col.Name
			rootPos := oldColName2Idx[qualifiedName]
			if newPos, updated := newColName2Idx[qualifiedName]; updated {
				rootPos = newPos
			}
			rootValue := &plan.Expr{
				Typ: parentNode.ProjectList[rootPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: explicitRootTag, ColPos: rootPos,
				}},
			}
			childValue := &plan.Expr{
				Typ:  col.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: childTag, ColPos: int32(colPos)}},
			}
			mergedValue, mergeErr := BindFuncExprImplByPlanExpr(
				builder.GetContext(), "if", []*plan.Expr{DeepCopyExpr(matchedRoot), rootValue, childValue})
			if mergeErr != nil {
				return mergeErr
			}
			newChildExprs[int32(colPos)] = mergedValue
			if _, isPrimary := primaryNames[catalog.ResolveAlias(col.Name)]; isPrimary {
				primaryKeyChanged = true
			}
		}
	}
	if primaryKeyChanged && len(childTableDef.Pkey.Names) > 1 {
		primaryParts := make([]*plan.Expr, len(childTableDef.Pkey.Names))
		for i, name := range childTableDef.Pkey.Names {
			partPos := childTableDef.Name2ColIndex[catalog.ResolveAlias(name)]
			if replacement, updated := newChildExprs[partPos]; updated {
				primaryParts[i] = DeepCopyExpr(replacement)
			} else {
				primaryParts[i] = &plan.Expr{
					Typ:  childTableDef.Cols[partPos].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: childTag, ColPos: partPos}},
				}
			}
		}
		compositePrimary, buildErr := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "serial", primaryParts)
		if buildErr != nil {
			return buildErr
		}
		newChildExprs[childTableDef.Name2ColIndex[childTableDef.Pkey.PkeyColName]] = compositePrimary
	}
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
		replacementPositions := make([]int, 0, len(newChildExprs))
		for childPos := range newChildExprs {
			replacementPositions = append(replacementPositions, int(childPos))
		}
		sort.Ints(replacementPositions)
		for _, rawChildPos := range replacementPositions {
			childPos := int32(rawChildPos)
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

	var oldChildExprs map[int32]*plan.Expr
	joinNodeID, childTag, newChildExprs, oldChildExprs, selfAffectedRowsExprs, err =
		builder.appendRecursiveUpdateParentMutations(
			bindCtx,
			childTableDef,
			joinNodeID,
			childTag,
			newChildExprs,
			selfAffectedRowsExprs,
			excludedMutationEdges,
		)
	if err != nil {
		return err
	}
	if selfReferencing {
		if selfRows == nil {
			return moerr.NewInternalError(
				builder.GetContext(), "self-referencing parent action output is unavailable")
		}
		// Keep the action row image positional across the root/action merger.
		// Without a materialization boundary, projection folding can compose the
		// parent-source expression through both self joins and bind it to the
		// nullable side of the explicit-root LEFT join.
		actionImageSinkID := appendSinkNodeWithTag(builder, bindCtx, joinNodeID, childTag)
		builder.qry.Nodes[actionImageSinkID].ExtraOptions = materialized.CTESinkOption
		if builder.preserveSinkProjection == nil {
			builder.preserveSinkProjection = make(map[int32]struct{})
		}
		builder.preserveSinkProjection[actionImageSinkID] = struct{}{}
		actionImageStep := builder.appendStep(actionImageSinkID)
		joinNodeID = builder.appendTaggedSinkScan(bindCtx, actionImageStep, childTag)
		if builder.preserveScanProjection == nil {
			builder.preserveScanProjection = make(map[int32]struct{})
		}
		builder.preserveScanProjection[joinNodeID] = struct{}{}
		selfRows.nodeID, selfRows.tag, err = builder.appendUpdateSelfMutationRows(
			bindCtx,
			childTableDef,
			parentAlias,
			selfRows.rootSourceStep,
			sourceTag,
			oldColName2Idx,
			newColName2Idx,
			joinNodeID,
			childTag,
			newChildExprs,
			oldChildExprs,
			selfAffectedRowsExprs,
			selfTargetSelectors,
		)
		if err == nil {
			builder.qry.HasForeignKeyAction = true
		}
		return err
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
		if !indexAffected && !primaryKeyChanged {
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

		oldKeyExpr, buildErr := builder.buildUpdateMutationIndexLookupIdentity(
			childTableDef,
			idxDef,
			childTag,
			oldChildExprs,
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
		if idxDef.Unique || primaryKeyChanged {
			// A PK cascade must preserve the base child row even when a nullable
			// secondary-index key has no hidden row. Missing old index state only
			// suppresses index deletion; it must not suppress the child mutation.
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
	pkPos := childTableDef.Name2ColIndex[childTableDef.Pkey.PkeyColName]
	deletePkPos := pkPos
	if primaryKeyChanged {
		deletePkPos = int32(len(actionProjection))
		oldPkExpr, ok := oldChildExprs[pkPos]
		if !ok {
			return moerr.NewInternalError(
				builder.GetContext(), "parent foreign key action old primary key is unavailable")
		}
		actionProjection = append(actionProjection, DeepCopyExpr(oldPkExpr))
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
	if deletePkPos != pkPos {
		lockTargets = append(lockTargets, &plan.LockTarget{
			TableId:            childTableDef.TblId,
			ObjRef:             affectedFK.childObjRef,
			PrimaryColIdxInBat: deletePkPos,
			PrimaryColRelPos:   actionTag,
			PrimaryColTyp:      actionProjection[deletePkPos].Typ,
		})
	}
	updateCtxList := []*plan.UpdateCtx{{
		ObjRef:             affectedFK.childObjRef,
		TableDef:           childTableDef,
		InsertCols:         insertCols,
		IgnoreAffectedRows: true,
		DeleteCols: []plan.ColRef{
			{RelPos: actionTag, ColPos: rowIDPos},
			{RelPos: actionTag, ColPos: deletePkPos},
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

// appendUpdateSelfMutationRows converts the non-root self-cascade rows back to
// the root UPDATE's positional schema, then unions both row sets. The ordinary
// UPDATE planner therefore owns base-table and hidden-index maintenance for one
// physical table in one MULTI_UPDATE node.
func (builder *QueryBuilder) appendUpdateSelfMutationRows(
	bindCtx *BindContext,
	tableDef *plan.TableDef,
	alias string,
	sourceStep int32,
	sourceTag int32,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	actionNodeID int32,
	actionTag int32,
	newChildExprs map[int32]*plan.Expr,
	oldChildExprs map[int32]*plan.Expr,
	actionAffectedRowsExprs []*plan.Expr,
	selfTargetSelectors *updateSelfTargetSelectors,
) (int32, int32, error) {
	if len(actionAffectedRowsExprs) == 0 {
		return 0, 0, moerr.NewInternalError(
			builder.GetContext(), "self-referencing parent action affected-row selector is unavailable")
	}
	rootNodeID := builder.appendTaggedSinkScan(bindCtx, sourceStep, sourceTag)
	rootInput := getProjectionByLastNodeWithTag(builder, rootNodeID, sourceTag)
	rootTag := builder.genNewBindTag()
	rootProject := make([]*plan.Expr, len(rootInput), len(rootInput)+len(newChildExprs))
	for pos, expr := range rootInput {
		rootProject[pos] = &plan.Expr{
			Typ:  expr.Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: sourceTag, ColPos: int32(pos)}},
		}
	}
	replacementPositions := make([]int, 0, len(newChildExprs))
	for colPos := range newChildExprs {
		replacementPositions = append(replacementPositions, int(colPos))
	}
	sort.Ints(replacementPositions)
	for _, rawColPos := range replacementPositions {
		colPos := int32(rawColPos)
		qualifiedName := alias + "." + tableDef.Cols[colPos].Name
		if _, exists := newColName2Idx[qualifiedName]; exists {
			continue
		}
		oldPos, exists := oldColName2Idx[qualifiedName]
		if !exists {
			return 0, 0, moerr.NewInternalErrorf(
				builder.GetContext(), "self-referencing parent action column %s is unavailable",
				tableDef.Cols[colPos].Name)
		}
		newColName2Idx[qualifiedName] = int32(len(rootProject))
		rootProject = append(rootProject, &plan.Expr{
			Typ: rootInput[oldPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: sourceTag, ColPos: oldPos,
			}},
		})
	}
	affectedRowsPos := int32(-1)
	physicalActivePos := int32(-1)
	multiTarget := false
	if selfTargetSelectors != nil {
		for _, targetIdx := range selfTargetSelectors.targetIndexes {
			if targetIdx >= 0 && targetIdx < len(selfTargetSelectors.targetRowNumberPos) &&
				selfTargetSelectors.targetRowNumberPos[targetIdx] >= 0 {
				multiTarget = true
				break
			}
		}
	}
	if multiTarget {
		var rootPhysicalActive *plan.Expr
		for _, targetIdx := range selfTargetSelectors.targetIndexes {
			activePos := selfTargetSelectors.targetActivePos[targetIdx]
			if activePos < 0 || int(activePos) >= len(rootProject) {
				return 0, 0, moerr.NewInternalError(
					builder.GetContext(), "self-referencing target active selector is unavailable")
			}
			activeExpr := DeepCopyExpr(rootProject[activePos])
			if rootPhysicalActive == nil {
				rootPhysicalActive = activeExpr
				continue
			}
			var buildErr error
			rootPhysicalActive, buildErr = BindFuncExprImplByPlanExpr(
				builder.GetContext(), "or", []*plan.Expr{rootPhysicalActive, activeExpr})
			if buildErr != nil {
				return 0, 0, buildErr
			}
		}
		if rootPhysicalActive == nil {
			return 0, 0, moerr.NewInternalError(
				builder.GetContext(), "self-referencing physical target selector is unavailable")
		}
		physicalActivePos = int32(len(rootProject))
		rootProject = append(rootProject, rootPhysicalActive)
		for _, targetIdx := range selfTargetSelectors.targetIndexes {
			selfTargetSelectors.physicalTargetActivePos[targetIdx] = physicalActivePos
		}
	} else if selfTargetSelectors != nil {
		affectedRowsPos = int32(len(rootProject))
		rootProject = append(rootProject, makePlan2BoolConstExprWithType(true))
	}
	rootNodeID = builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{rootNodeID},
		ProjectList: rootProject, BindingTags: []int32{rootTag},
	}, bindCtx)

	actionInput := getProjectionByLastNodeWithTag(builder, actionNodeID, actionTag)
	actionProject := make([]*plan.Expr, len(rootProject))
	for pos, expr := range rootProject {
		actionProject[pos] = nullUpdateProjectionExpr(expr.Typ)
	}
	for colPos, col := range tableDef.Cols {
		qualifiedName := alias + "." + col.Name
		oldPos, ok := oldColName2Idx[qualifiedName]
		if !ok || oldPos < 0 || int(oldPos) >= len(actionProject) {
			return 0, 0, moerr.NewInternalErrorf(
				builder.GetContext(), "self-referencing parent action old column %s is unavailable", col.Name)
		}
		oldExpr, changed := oldChildExprs[int32(colPos)]
		if !changed {
			oldExpr = &plan.Expr{
				Typ:  col.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: actionTag, ColPos: int32(colPos)}},
			}
		}
		actionProject[oldPos] = DeepCopyExpr(oldExpr)

		newPos, changed := newColName2Idx[qualifiedName]
		if !changed {
			continue
		}
		if newPos < 0 || int(newPos) >= len(actionProject) {
			return 0, 0, moerr.NewInternalErrorf(
				builder.GetContext(), "self-referencing parent action new column %s is unavailable", col.Name)
		}
		newExpr, changed := newChildExprs[int32(colPos)]
		if !changed {
			newExpr = &plan.Expr{
				Typ:  actionInput[colPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: actionTag, ColPos: int32(colPos)}},
			}
		}
		actionProject[newPos] = DeepCopyExpr(newExpr)
	}
	if multiTarget {
		for selectorIdx, targetIdx := range selfTargetSelectors.targetIndexes {
			rowNumberPos := selfTargetSelectors.targetRowNumberPos[targetIdx]
			activePos := selfTargetSelectors.targetActivePos[targetIdx]
			if rowNumberPos < 0 || int(rowNumberPos) >= len(actionProject) ||
				activePos < 0 || int(activePos) >= len(actionProject) {
				return 0, 0, moerr.NewInternalError(
					builder.GetContext(), "self-referencing target selector is unavailable")
			}
			if selectorIdx >= len(actionAffectedRowsExprs) {
				return 0, 0, moerr.NewInternalError(
					builder.GetContext(), "self-referencing alias selector is unavailable")
			}
			actionProject[rowNumberPos] = makePlan2Int64ConstExprWithType(1)
			actionProject[activePos] = DeepCopyExpr(actionAffectedRowsExprs[selectorIdx])
		}
		actionProject[physicalActivePos] = makePlan2BoolConstExprWithType(true)
	} else if affectedRowsPos >= 0 {
		actionProject[affectedRowsPos] = DeepCopyExpr(actionAffectedRowsExprs[0])
	}
	actionOutputTag := builder.genNewBindTag()
	actionNodeID = builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{actionNodeID},
		ProjectList: actionProject, BindingTags: []int32{actionOutputTag},
	}, bindCtx)
	actionSinkID := appendSinkNodeWithTag(builder, bindCtx, actionNodeID, actionOutputTag)
	builder.qry.Nodes[actionSinkID].ExtraOptions = materialized.CTESinkOption
	if builder.preserveSinkProjection == nil {
		builder.preserveSinkProjection = make(map[int32]struct{})
	}
	builder.preserveSinkProjection[actionSinkID] = struct{}{}
	actionStep := builder.appendStep(actionSinkID)
	if builder.preserveScanProjection == nil {
		builder.preserveScanProjection = make(map[int32]struct{})
	}
	actionExclusionID := builder.appendTaggedSinkScan(bindCtx, actionStep, actionOutputTag)
	builder.preserveScanProjection[actionExclusionID] = struct{}{}
	actionExclusionTag := builder.genNewBindTag()
	actionExclusionID = builder.appendUpdateRetagProject(
		bindCtx, actionExclusionID, actionOutputTag, actionExclusionTag)
	rowIDPos, ok := oldColName2Idx[alias+"."+catalog.Row_ID]
	if !ok {
		return 0, 0, moerr.NewInternalError(
			builder.GetContext(), "self-referencing parent action rowid is unavailable")
	}
	rootRowIDPositions := []int32{rowIDPos}
	if selfTargetSelectors != nil && len(selfTargetSelectors.targetIndexes) > 0 {
		rootRowIDPositions = rootRowIDPositions[:0]
		for _, targetIdx := range selfTargetSelectors.targetIndexes {
			if pos := selfTargetSelectors.targetRowIDPos[targetIdx]; pos >= 0 {
				rootRowIDPositions = append(rootRowIDPositions, pos)
			}
		}
	}
	var rootIsAction *plan.Expr
	for _, candidateRowIDPos := range rootRowIDPositions {
		match, buildErr := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			{
				Typ: rootProject[candidateRowIDPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: rootTag, ColPos: candidateRowIDPos,
				}},
			},
			{
				Typ: actionProject[rowIDPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: actionExclusionTag, ColPos: rowIDPos,
				}},
			},
		})
		if buildErr != nil {
			return 0, 0, buildErr
		}
		if rootIsAction == nil {
			rootIsAction = match
		} else {
			rootIsAction, buildErr = BindFuncExprImplByPlanExpr(
				builder.GetContext(), "or", []*plan.Expr{rootIsAction, match})
			if buildErr != nil {
				return 0, 0, buildErr
			}
		}
	}
	rootNodeID = builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN, Children: []int32{rootNodeID, actionExclusionID},
		JoinType: plan.Node_ANTI, OnList: []*plan.Expr{rootIsAction},
	}, bindCtx)
	rootMergedTag := builder.genNewBindTag()
	rootNodeID = builder.appendUpdateRetagProject(
		bindCtx, rootNodeID, rootTag, rootMergedTag)
	rootTag = rootMergedTag
	rootSinkID := appendSinkNodeWithTag(builder, bindCtx, rootNodeID, rootTag)
	builder.preserveSinkProjection[rootSinkID] = struct{}{}
	rootStep := builder.appendStep(rootSinkID)
	rootNodeID = builder.appendTaggedSinkScan(bindCtx, rootStep, rootTag)
	builder.preserveScanProjection[rootNodeID] = struct{}{}
	rootInputTag := builder.genNewBindTag()
	rootNodeID = builder.appendUpdateRetagProject(bindCtx, rootNodeID, rootTag, rootInputTag)

	actionNodeID = builder.appendTaggedSinkScan(bindCtx, actionStep, actionOutputTag)
	builder.preserveScanProjection[actionNodeID] = struct{}{}
	actionInputTag := builder.genNewBindTag()
	actionNodeID = builder.appendUpdateRetagProject(
		bindCtx, actionNodeID, actionOutputTag, actionInputTag)

	unionTag := builder.genNewBindTag()
	unionProject := make([]*plan.Expr, len(rootProject))
	for pos, expr := range rootProject {
		unionProject[pos] = &plan.Expr{
			Typ:  expr.Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rootInputTag, ColPos: int32(pos)}},
		}
	}
	unionNodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_UNION_ALL, Children: []int32{rootNodeID, actionNodeID},
		ProjectList: unionProject, BindingTags: []int32{unionTag},
	}, bindCtx)
	validRow, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "isnotnull", []*plan.Expr{{
		Typ:  rootProject[rowIDPos].Typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: unionTag, ColPos: rowIDPos}},
	}})
	if err != nil {
		return 0, 0, err
	}
	unionNodeID = builder.appendNode(&plan.Node{
		NodeType: plan.Node_FILTER, Children: []int32{unionNodeID}, FilterList: []*plan.Expr{validRow},
	}, bindCtx)
	outputTag := builder.genNewBindTag()
	unionNodeID = builder.appendUpdateRetagProject(bindCtx, unionNodeID, unionTag, outputTag)
	if affectedRowsPos >= 0 {
		if builder.updateAffectedRowsCols == nil {
			builder.updateAffectedRowsCols = make(map[uint64]updateAffectedRowsColumn)
		}
		builder.updateAffectedRowsCols[tableDef.TblId] = updateAffectedRowsColumn{
			pos: affectedRowsPos,
		}
	}
	return unionNodeID, outputTag, nil
}

func (builder *QueryBuilder) appendRecursiveUpdateParentMutations(
	bindCtx *BindContext,
	tableDef *plan.TableDef,
	lastNodeID int32,
	inputTag int32,
	replacements map[int32]*plan.Expr,
	affectedRowsExprs []*plan.Expr,
	excludedMutationEdges map[updateForeignKeyActionEdgeKey]struct{},
) (int32, int32, map[int32]*plan.Expr, map[int32]*plan.Expr, []*plan.Expr, error) {
	if tableDef == nil || len(replacements) == 0 {
		return 0, 0, nil, nil, nil, moerr.NewInternalError(
			builder.GetContext(), "parent foreign key action row image is incomplete")
	}

	actionTag := builder.genNewBindTag()
	projectList := make([]*plan.Expr, len(tableDef.Cols))
	oldColName2Idx := make(map[string]int32, len(tableDef.Cols))
	newColName2Idx := make(map[string]int32, len(replacements))
	alias := tableDef.Name
	for i, col := range tableDef.Cols {
		qualifiedName := alias + "." + col.Name
		oldColName2Idx[qualifiedName] = int32(i)
		if replacement, updated := replacements[int32(i)]; updated {
			projectList[i] = DeepCopyExpr(replacement)
			newColName2Idx[qualifiedName] = int32(i)
			continue
		}
		projectList[i] = &plan.Expr{
			Typ:  col.Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: inputTag, ColPos: int32(i)}},
		}
	}

	replacementPositions := make([]int, 0, len(replacements))
	for pos := range replacements {
		replacementPositions = append(replacementPositions, int(pos))
	}
	sort.Ints(replacementPositions)
	for _, rawPos := range replacementPositions {
		pos := int32(rawPos)
		col := tableDef.Cols[pos]
		oldColName2Idx[alias+"."+col.Name] = int32(len(projectList))
		projectList = append(projectList, &plan.Expr{
			Typ:  col.Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: inputTag, ColPos: pos}},
		})
	}
	affectedRowsPos := int32(len(projectList))
	for _, affectedRowsExpr := range affectedRowsExprs {
		projectList = append(projectList, DeepCopyExpr(affectedRowsExpr))
	}

	lastNodeID = builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{lastNodeID},
		ProjectList: projectList, BindingTags: []int32{actionTag},
	}, bindCtx)
	checkedNodeID, checkedTag := lastNodeID, actionTag
	if len(tableDef.Checks) > 0 {
		var err error
		checkedNodeID, err = appendCheckConstraintPlanWithColLookup(
			builder,
			bindCtx,
			tableDef,
			checkedNodeID,
			checkedTag,
			func(colName string) (int32, bool) {
				pos, ok := tableDef.Name2ColIndex[colName]
				return pos, ok
			},
			false,
		)
		if err != nil {
			return 0, 0, nil, nil, nil, err
		}
	}
	var err error
	checkedNodeID, checkedTag, err = builder.appendUpdateParentForeignKeyChecks(
		bindCtx,
		tableDef,
		alias,
		checkedNodeID,
		checkedTag,
		oldColName2Idx,
		newColName2Idx,
		nil,
		false,
		nil,
		excludedMutationEdges,
	)
	if err != nil {
		return 0, 0, nil, nil, nil, err
	}
	checkedProject := getProjectionByLastNodeWithTag(builder, checkedNodeID, checkedTag)
	newExprs := make(map[int32]*plan.Expr, len(replacements))
	oldExprs := make(map[int32]*plan.Expr, len(replacements))
	for pos := range replacements {
		qualifiedName := alias + "." + tableDef.Cols[pos].Name
		newPos := newColName2Idx[qualifiedName]
		oldPos := oldColName2Idx[qualifiedName]
		newExprs[pos] = &plan.Expr{
			Typ:  checkedProject[newPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: checkedTag, ColPos: newPos}},
		}
		oldExprs[pos] = &plan.Expr{
			Typ:  checkedProject[oldPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: checkedTag, ColPos: oldPos}},
		}
	}
	checkedAffectedRowsExprs := make([]*plan.Expr, len(affectedRowsExprs))
	for idx := range affectedRowsExprs {
		pos := affectedRowsPos + int32(idx)
		checkedAffectedRowsExprs[idx] = &plan.Expr{
			Typ: checkedProject[pos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: checkedTag, ColPos: pos,
			}},
		}
	}
	return checkedNodeID, checkedTag, newExprs, oldExprs, checkedAffectedRowsExprs, nil
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

func (builder *QueryBuilder) buildUpdateMutationIndexLookupIdentity(
	tableDef *plan.TableDef,
	idxDef *plan.IndexDef,
	tableTag int32,
	replacements map[int32]*plan.Expr,
) (*plan.Expr, error) {
	if !isSpatialIndexDef(idxDef) {
		return builder.buildUpdateMutationIndexKey(tableDef, idxDef, tableTag, replacements)
	}
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

func (builder *QueryBuilder) buildUpdateMutationIndexKey(
	tableDef *plan.TableDef,
	idxDef *plan.IndexDef,
	tableTag int32,
	replacements map[int32]*plan.Expr,
) (*plan.Expr, error) {
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
