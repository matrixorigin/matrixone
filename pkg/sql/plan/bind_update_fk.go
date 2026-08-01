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
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	planutil "github.com/matrixorigin/matrixone/pkg/sql/util"
)

const foreignKeyNoReferencedRowAssert = "fk_no_referenced_row"

func (builder *QueryBuilder) updateInputProjectNode(nodeID int32) *plan.Node {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType == plan.Node_PRE_INSERT && len(node.Children) == 1 {
		return builder.qry.Nodes[node.Children[0]]
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
) (int32, int32, *plan.Node, error) {
	enabled, err := IsForeignKeyChecksEnabled(builder.compCtx)
	if err != nil {
		return 0, 0, nil, err
	}
	if !enabled {
		return lastNodeID, selectNodeTag, selectNode, nil
	}

	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		alias := dmlCtx.aliases[i]
		lastNodeID, selectNodeTag, err = builder.appendUpdateParentForeignKeyChecks(
			bindCtx,
			tableDef,
			alias,
			lastNodeID,
			selectNodeTag,
			oldColName2Idx,
			newColName2Idx,
		)
		if err != nil {
			return 0, 0, nil, err
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

func (builder *QueryBuilder) appendUpdateParentForeignKeyChecks(
	bindCtx *BindContext,
	tableDef *plan.TableDef,
	alias string,
	lastNodeID int32,
	selectNodeTag int32,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
) (int32, int32, error) {
	if tableDef == nil || len(tableDef.RefChildTbls) == 0 {
		return lastNodeID, selectNodeTag, nil
	}

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
			return 0, 0, err
		}
		if childTableDef == nil {
			return 0, 0, moerr.NewInternalErrorf(
				builder.GetContext(),
				"foreign-key child table %d not found",
				childTableID,
			)
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

	for _, affectedFK := range affected {
		if affectedFK.childTableDef.TblId == tableDef.TblId {
			switch affectedFK.fk.OnUpdate {
			case plan.ForeignKeyDef_RESTRICT,
				plan.ForeignKeyDef_NO_ACTION,
				plan.ForeignKeyDef_SET_DEFAULT:
			case plan.ForeignKeyDef_CASCADE, plan.ForeignKeyDef_SET_NULL:
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
		); err != nil {
			return 0, 0, err
		}
	}
	lastNodeID = builder.appendTaggedSinkScan(bindCtx, sourceStep, selectNodeTag)
	return lastNodeID, selectNodeTag, nil
}

func (builder *QueryBuilder) validateModernUpdateParentMutation(
	bindCtx *BindContext,
	parentTableDef *plan.TableDef,
	affectedFK updateParentForeignKey,
) error {
	childTableDef := affectedFK.childTableDef
	ensureName2ColIndexForReplace(childTableDef)
	parentColByID := make(map[uint64]*plan.ColDef, len(parentTableDef.Cols))
	for _, col := range parentTableDef.Cols {
		parentColByID[col.ColId] = col
	}
	for _, parentColID := range affectedFK.fk.ForeignCols {
		if col := parentColByID[parentColID]; col != nil && col.Typ.AutoIncr {
			return newLegacyUpdatePlannerRouteError(
				updateRouteReasonForeignKey,
				moerr.NewUnsupportedDML(
					builder.GetContext(),
					"parent foreign key action changing auto-increment referenced key",
				),
			)
		}
	}
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
	return moerr.NewNotSupported(
		builder.GetContext(),
		"parent foreign key action requires complete child update row closure",
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
	if unchanged != nil {
		changed, bindErr := BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"not",
			[]*plan.Expr{unchanged},
		)
		if bindErr != nil {
			return bindErr
		}
		parentNodeID = builder.appendNode(&plan.Node{
			NodeType:   plan.Node_FILTER,
			Children:   []int32{parentNodeID},
			FilterList: []*plan.Expr{changed},
		}, bindCtx)
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

	parentColIDToName := make(map[uint64]string, len(parentTableDef.Cols))
	for _, col := range parentTableDef.Cols {
		parentColIDToName[col.ColId] = col.Name
	}
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

	type mutationIndex struct {
		def      *plan.IndexDef
		objRef   *plan.ObjectRef
		tableDef *plan.TableDef
		tag      int32
	}
	indexes := make([]mutationIndex, 0, len(childTableDef.Indexes))
	for _, idxDef := range childTableDef.Indexes {
		if !catalog.IsRegularIndexAlgo(idxDef.IndexAlgo) {
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
	sort.SliceStable(lockTargets, func(i, j int) bool {
		if lockTargets[i].TableId != lockTargets[j].TableId {
			return lockTargets[i].TableId < lockTargets[j].TableId
		}
		if lockTargets[i].PrimaryColIdxInBat != lockTargets[j].PrimaryColIdxInBat {
			return lockTargets[i].PrimaryColIdxInBat < lockTargets[j].PrimaryColIdxInBat
		}
		return lockTargets[i].PrimaryColRelPos < lockTargets[j].PrimaryColRelPos
	})
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
	if affectedFK.childTableDef.TblId == parentTableDef.TblId {
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
	assertExpr, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(),
		"assert",
		[]*plan.Expr{
			ok,
			makePlan2StringConstExprWithType(
				"Cannot delete or update a parent row: a foreign key constraint fails",
			),
		},
	)
	if err != nil {
		return 0, 0, err
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{markNodeID},
		FilterList: []*plan.Expr{assertExpr},
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
