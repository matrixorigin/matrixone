// Copyright 2021 Matrix Origin
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
	"maps"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	planutil "github.com/matrixorigin/matrixone/pkg/sql/util"
)

func (builder *QueryBuilder) makeUpdatedClusterByExpr(
	alias string,
	tableDef *plan.TableDef,
	selectNode *plan.Node,
	selectNodeTag int32,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
) (*plan.Expr, error) {
	if tableDef.ClusterBy == nil || !planutil.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
		return nil, nil
	}

	clusterByCols := planutil.SplitCompositeClusterByColumnName(tableDef.ClusterBy.Name)
	args := make([]*plan.Expr, len(clusterByCols))
	clusterByUpdated := false
	for i, colName := range clusterByCols {
		qualifiedName := alias + "." + colName
		colPos, ok := oldColName2Idx[qualifiedName]
		if !ok {
			return nil, moerr.NewInternalErrorf(
				builder.GetContext(),
				"bind update err, can not find cluster by column %s",
				colName,
			)
		}
		if updatedPos, ok := newColName2Idx[qualifiedName]; ok {
			colPos = updatedPos
			clusterByUpdated = true
		}
		args[i] = &plan.Expr{
			Typ: selectNode.ProjectList[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: selectNodeTag,
					ColPos: colPos,
					Name:   colName,
				},
			},
		}
	}
	if !clusterByUpdated {
		return nil, nil
	}
	return BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", args)
}

func (builder *QueryBuilder) bindUpdate(stmt *tree.Update, bindCtx *BindContext) (int32, error) {
	if err := validateUpdateWindowFunctions(builder.compCtx, stmt); err != nil {
		return 0, err
	}

	dmlCtx := NewDMLContext()
	err := dmlCtx.ResolveUpdateTables(builder.compCtx, stmt)
	if err != nil {
		return 0, err
	}
	if err = builder.validateDistinctUpdateForeignKeyMutationTargets(bindCtx, dmlCtx); err != nil {
		return 0, err
	}
	targetAliases := make([]string, len(dmlCtx.tableDefs))
	for i, updateCol2Expr := range dmlCtx.updateCol2Expr {
		if len(updateCol2Expr) > 0 {
			targetAliases[i] = dmlCtx.aliases[i]
		}
	}
	if err = validateUpdateTargetSubqueries(
		builder.compCtx, stmt, dmlCtx.objRefs, dmlCtx.tableDefs, targetAliases,
	); err != nil {
		return 0, err
	}
	updatedTargetCount := 0
	physicalTargetCounts := make(map[uint64]int)
	hasRepeatedPhysicalTarget := false
	for i := range dmlCtx.aliases {
		if len(dmlCtx.updateCol2Expr[i]) > 0 {
			updatedTargetCount++
			tableID := dmlCtx.tableDefs[i].TblId
			physicalTargetCounts[tableID]++
			hasRepeatedPhysicalTarget = hasRepeatedPhysicalTarget || physicalTargetCounts[tableID] > 1
		}
	}
	if err = validateRepeatedPhysicalTargetPrimaryKeyUpdate(builder.GetContext(), dmlCtx); err != nil {
		return 0, err
	}
	routeUnsupported := func(reason updatePlannerRouteReason, routeErr error) error {
		if hasRepeatedPhysicalTarget {
			return newUpdatePlannerRouteError(updatePlannerRejected, reason, routeErr)
		}
		return newLegacyUpdatePlannerRouteError(reason, routeErr)
	}
	if stmt.HasReturning() {
		if len(dmlCtx.tableDefs) != 1 {
			return 0, returningNotSupported(builder, "multi-table UPDATE")
		}
		if err = validateReturningTarget(builder, dmlCtx.tableDefs[0], dmlCtx.objRefs[0]); err != nil {
			return 0, err
		}
	}
	onDuplicateAction := plan.Node_FAIL
	if stmt.Ignore {
		onDuplicateAction = plan.Node_IGNORE
	}

	var selectList []tree.SelectExpr
	oldColName2Idx := make(map[string]int32)
	newColName2Idx := make(map[string]int32)
	updateAutoIncrCols := make([]bool, len(dmlCtx.aliases))
	colOffsets := make([]int32, len(dmlCtx.aliases))
	updateNumericTargets := make(map[int32]Type)
	inlineIrregularIndexes := make([][]*plan.IndexDef, len(dmlCtx.aliases))

	for i, alias := range dmlCtx.aliases {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		tableDef := dmlCtx.tableDefs[i]
		if err := validateTableRegularIndexPrefixMetadata(tableDef); err != nil {
			return 0, err
		}
		colOffsets[i] = int32(len(selectList))
		useColInPartExpr := make(map[string]bool)

		// append  table.* to project list
		for _, col := range tableDef.Cols {
			oldColName2Idx[alias+"."+col.Name] = int32(len(selectList))
			e := tree.NewUnresolvedName(tree.NewCStr(alias, bindCtx.lower), tree.NewCStr(col.Name, 1))
			selectList = append(selectList, tree.SelectExpr{
				Expr: e,
			})
		}

		var legacyIrregularRoute bool
		inlineIrregularIndexes[i], legacyIrregularRoute, err = classifyIrregularIndexesForUpdate(
			builder.GetContext(), tableDef, dmlCtx.updateCol2Expr[i])
		if err != nil {
			if stmt.HasReturning() {
				if feature := returningUpdatePlannerFeature(err); feature != "" {
					return 0, returningNotSupported(builder, feature)
				}
			}
			return 0, err
		}
		if legacyIrregularRoute {
			return 0, routeUnsupported(
				updateRouteReasonIrregularIndex,
				moerr.NewUnsupportedDML(builder.GetContext(), "update vector/full-text index"),
			)
		}
		validIndexes, _ := getValidIndexes(tableDef)
		tableDef.Indexes = validIndexes

		for colName, updateExpr := range dmlCtx.updateCol2Expr[i] {
			// Check: cannot update a generated column (unless SET gen_col = DEFAULT)
			isGenCol := false
			for _, colDef := range tableDef.Cols {
				if colDef.Name == colName && colDef.GeneratedCol != nil {
					isGenCol = true
					break
				}
			}
			if isGenCol {
				if _, ok := updateExpr.(*tree.DefaultVal); ok {
					// SET gen_col = DEFAULT is allowed — silently remove from update set
					delete(dmlCtx.updateCol2Expr[i], colName)
					continue
				}
				return 0, moerr.NewInvalidInputf(builder.compCtx.GetContext(), "the value specified for generated column '%s' in table '%s' is not allowed", colName, tableDef.Name)
			}

			if !dmlCtx.updatePartCol[i] {
				if _, ok := useColInPartExpr[colName]; ok {
					dmlCtx.updatePartCol[i] = true
				}
			}

			for _, colDef := range tableDef.Cols {
				if colDef.Name == colName {
					if isEnumOrSetPlanType(&colDef.Typ) {
						updateExpr, err = wrapAstExprForMySQLSpecialType(builder.GetContext(), colDef.Typ, updateExpr)
						if err != nil {
							return 0, err
						}
					}

					if colDef.Typ.AutoIncr {
						if constExpr, ok := updateExpr.(*tree.NumVal); ok {
							if constExpr.ValType == tree.P_null {
								return 0, moerr.NewConstraintViolation(builder.compCtx.GetContext(), fmt.Sprintf("Column '%s' cannot be null", colName))
							}
						}

						updateAutoIncrCols[i] = true
					}
				}
			}

			oldPos := oldColName2Idx[alias+"."+colName]
			if typ := tableDef.Cols[tableDef.Name2ColIndex[colName]].Typ; isNumericAssignmentTarget(typ) {
				updateNumericTargets[oldPos] = typ
			}
			newColName2Idx[alias+"."+colName] = oldPos
			oldColName2Idx[alias+"."+colName] = int32(len(selectList))
			selectList = append(selectList, selectList[oldPos])
			selectList[oldPos] = tree.SelectExpr{Expr: updateExpr}
		}
	}
	coalesceRepeatedPhysicalTargetIrregularIndexes(dmlCtx, inlineIrregularIndexes)
	// Merge target table list with PostgreSQL-style FROM sources so that the
	// inner SELECT can resolve column references against both, while dmlCtx
	// still tracks only the target tables. buildFrom requires a single
	// TableExpr, so cross-join target and the FROM-clause join tree here.
	selectFromTables := stmt.Tables
	if stmt.From != nil && len(stmt.From.Tables) > 0 {
		joined := tree.TableExpr(stmt.Tables[0])
		for _, src := range stmt.From.Tables {
			joined = &tree.JoinTableExpr{Left: joined, Right: src, JoinType: tree.JOIN_TYPE_CROSS}
		}
		selectFromTables = tree.TableExprs{joined}
	}

	selectAst := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: selectList,
			From: &tree.From{
				Tables: selectFromTables,
			},
			Where: stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
		With:    stmt.With,
	}
	bindCtx.numericProjectionTypes = make([]Type, len(selectList))
	for pos, typ := range updateNumericTargets {
		bindCtx.numericProjectionTypes[pos] = typ
	}

	lastNodeID, err := builder.bindSelect(selectAst, bindCtx, false)
	if err != nil {
		return 0, err
	}

	selectNode := builder.qry.Nodes[lastNodeID]
	selectNodeTag := selectNode.BindingTags[0]
	isMultiTargetUpdate := updatedTargetCount > 1
	if isMultiTargetUpdate {
		if builder.updateTargetScans == nil {
			builder.updateTargetScans = make(map[int32]struct{})
		}
		for i, alias := range dmlCtx.aliases {
			if len(dmlCtx.updateCol2Expr[i]) == 0 {
				continue
			}
			rowIDPos := oldColName2Idx[alias+"."+catalog.Row_ID]
			if col := selectNode.ProjectList[rowIDPos].GetCol(); col != nil {
				if scanID, ok := builder.tag2NodeID[col.RelPos]; ok {
					builder.updateTargetScans[scanID] = struct{}{}
				}
			}
		}
	}
	targetBranchActivePos := make([]int32, len(dmlCtx.aliases))
	targetRowNumberPos := make([]int32, len(dmlCtx.aliases))
	for i := range targetBranchActivePos {
		targetBranchActivePos[i] = -1
		targetRowNumberPos[i] = -1
	}
	if isMultiTargetUpdate {
		for i, alias := range dmlCtx.aliases {
			if len(dmlCtx.updateCol2Expr[i]) == 0 {
				continue
			}
			identityPos := oldColName2Idx[alias+"."+catalog.Row_ID]
			activeExpr, buildErr := BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"isnotnull",
				[]*plan.Expr{DeepCopyExpr(selectNode.ProjectList[identityPos])},
			)
			if buildErr != nil {
				return 0, buildErr
			}
			targetBranchActivePos[i] = int32(len(selectNode.ProjectList))
			selectNode.ProjectList = append(selectNode.ProjectList, activeExpr)
			lastNodeID, targetRowNumberPos[i], err = builder.appendTargetRowNumberBelowAssignmentProject(
				bindCtx,
				lastNodeID,
				selectNode,
				identityPos,
			)
			if err != nil {
				return 0, err
			}
		}
	}
	guardTargetAssignmentEvaluation := isMultiTargetUpdate || len(dmlCtx.aliases) > updatedTargetCount

	for i, alias := range dmlCtx.aliases {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		tableDef := dmlCtx.tableDefs[i]

		for originPos, col := range tableDef.Cols {
			if colPos, ok := newColName2Idx[alias+"."+col.Name]; ok {
				updateExpr := selectNode.ProjectList[colPos]
				if isDefaultValExpr(updateExpr) { // set col = default
					updateExpr, err = getDefaultExpr(builder.GetContext(), col)
					if err != nil {
						return 0, err
					}
				}
				if !col.Typ.AutoIncr && !guardTargetAssignmentEvaluation {
					err = checkNotNull(builder.GetContext(), updateExpr, tableDef, col)
					if err != nil {
						return 0, err
					}
				}
				if col != nil && isEnumPlanType(&col.Typ) {
					selectNode.ProjectList[colPos], err = funcCastForEnumType(builder.GetContext(), updateExpr, col.Typ)
					if err != nil {
						return 0, err
					}
				} else if col != nil && isSetPlanType(&col.Typ) {
					selectNode.ProjectList[colPos], err = funcCastForSetType(builder.GetContext(), updateExpr, col.Typ)
					if err != nil {
						return 0, err
					}
				} else if col != nil && isGeometryPlanType(&col.Typ) {
					selectNode.ProjectList[colPos], err = funcCastForGeometryType(builder.GetContext(), updateExpr, col.Typ)
					if err != nil {
						return 0, err
					}
				} else {
					selectNode.ProjectList[colPos], err = builder.forceProjectedAssignmentCastExpr(
						updateExpr, updateExpr, col.Typ, stmt.Ignore)
					if err != nil {
						return 0, err
					}
				}

				// The updated column's OLD value was appended to the project list as
				// the raw column scan, which for ENUM/SET/geometry columns is a
				// VARCHAR display value (cast_index_to_value for ENUM). Index tables
				// store the typed value (e.g. the T_enum index), and INSERT casts the
				// value before building index keys (bind_insert.go). Cast the OLD
				// value the same way so UPDATE index-maintenance joins build keys in
				// the stored typed representation; otherwise the join compares VARCHAR
				// against the typed index key and either fails to bind (nil-pointer
				// panic) or silently misses the row (update does not take effect).
				if oldPos, ok := oldColName2Idx[alias+"."+col.Name]; ok && oldPos != colPos {
					oldExpr := selectNode.ProjectList[oldPos]
					if isEnumPlanType(&col.Typ) {
						selectNode.ProjectList[oldPos], err = funcCastForEnumType(builder.GetContext(), oldExpr, col.Typ)
					} else if isSetPlanType(&col.Typ) {
						selectNode.ProjectList[oldPos], err = funcCastForSetType(builder.GetContext(), oldExpr, col.Typ)
					} else if isGeometryPlanType(&col.Typ) {
						selectNode.ProjectList[oldPos], err = funcCastForGeometryType(builder.GetContext(), oldExpr, col.Typ)
					}
					if err != nil {
						return 0, err
					}
				}
			} else {
				if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
					newDefExpr := DeepCopyExpr(col.OnUpdate.Expr)
					err = replaceFuncId(builder.GetContext(), newDefExpr)
					if err != nil {
						return 0, err
					}

					oldPos := oldColName2Idx[alias+"."+col.Name]
					newColName2Idx[alias+"."+col.Name] = oldPos
					oldColName2Idx[alias+"."+col.Name] = int32(len(selectNode.ProjectList))
					selectNode.ProjectList = append(selectNode.ProjectList, selectNode.ProjectList[oldPos])
					selectNode.ProjectList[oldPos] = newDefExpr
				}

				if isEnumPlanType(&col.Typ) {
					selectNode.ProjectList[originPos], err = funcCastForEnumType(builder.GetContext(), selectNode.ProjectList[originPos], col.Typ)
					if err != nil {
						return 0, err
					}
				} else if isSetPlanType(&col.Typ) {
					selectNode.ProjectList[originPos], err = funcCastForSetType(builder.GetContext(), selectNode.ProjectList[originPos], col.Typ)
					if err != nil {
						return 0, err
					}
				} else if isGeometryPlanType(&col.Typ) {
					selectNode.ProjectList[originPos], err = funcCastForGeometryType(builder.GetContext(), selectNode.ProjectList[originPos], col.Typ)
					if err != nil {
						return 0, err
					}
				}
			}
		}

	}

	if guardTargetAssignmentEvaluation {
		for i, alias := range dmlCtx.aliases {
			if len(dmlCtx.updateCol2Expr[i]) == 0 {
				continue
			}
			targetSelected, buildErr := builder.buildTargetSelectedBelowAssignmentProject(
				selectNode,
				oldColName2Idx[alias+"."+catalog.Row_ID],
				targetRowNumberPos[i],
			)
			if buildErr != nil {
				return 0, buildErr
			}
			for _, col := range dmlCtx.tableDefs[i].Cols {
				qualifiedName := alias + "." + col.Name
				newPos, updated := newColName2Idx[qualifiedName]
				if !updated {
					continue
				}
				oldPos, ok := oldColName2Idx[qualifiedName]
				if !ok {
					continue
				}
				selectNode.ProjectList[newPos], buildErr = builder.guardTargetLocalExpr(
					targetSelected,
					selectNode.ProjectList[newPos],
					selectNode.ProjectList[oldPos],
				)
				if buildErr != nil {
					return 0, buildErr
				}
			}
			if targetBranchActivePos[i] >= 0 {
				selectNode.ProjectList[targetBranchActivePos[i]] = DeepCopyExpr(targetSelected)
			}
		}
	}

	if !isMultiTargetUpdate && len(dmlCtx.aliases) > updatedTargetCount {
		for i, alias := range dmlCtx.aliases {
			if len(dmlCtx.updateCol2Expr[i]) == 0 {
				continue
			}
			rowIDPos, ok := oldColName2Idx[alias+"."+catalog.Row_ID]
			if !ok {
				return 0, moerr.NewInternalErrorf(
					builder.GetContext(),
					"bind update err, can not find row_id for target %s",
					alias,
				)
			}
			eligible, buildErr := BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"isnotnull",
				[]*plan.Expr{{
					Typ: selectNode.ProjectList[rowIDPos].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: rowIDPos,
					}},
				}},
			)
			if buildErr != nil {
				return 0, buildErr
			}
			lastNodeID = builder.appendNode(&plan.Node{
				NodeType:        plan.Node_FILTER,
				Children:        []int32{lastNodeID},
				FilterList:      []*plan.Expr{eligible},
				ProjectList:     getProjectionByLastNodeIfAvailable(builder, lastNodeID),
				FilterIsBarrier: true,
			}, bindCtx)
			break
		}
	}

	assignedColsByTarget := collectPhysicalTargetAssignedCols(dmlCtx, newColName2Idx)
	deferRepeatedPhysicalTargetMerge := stmt.Ignore &&
		hasRepeatedPhysicalUpdateTarget(dmlCtx)
	generatedOwnerByTableID := make(map[uint64]int)
	physicalTargetActivePos := append([]int32(nil), targetBranchActivePos...)
	lastNodeID, selectNode, selectNodeTag, err = builder.mergeSamePhysicalTargetAssignments(
		bindCtx,
		lastNodeID,
		selectNode,
		selectNodeTag,
		dmlCtx,
		oldColName2Idx,
		newColName2Idx,
		targetBranchActivePos,
		assignedColsByTarget,
		colOffsets,
		!deferRepeatedPhysicalTargetMerge,
		deferRepeatedPhysicalTargetMerge,
		false,
	)
	if err != nil {
		return 0, err
	}

	if isMultiTargetUpdate {
		for i, alias := range dmlCtx.aliases {
			if len(dmlCtx.updateCol2Expr[i]) == 0 {
				continue
			}
			rowIDPos, ok := oldColName2Idx[alias+"."+catalog.Row_ID]
			if !ok {
				return 0, moerr.NewInternalErrorf(
					builder.GetContext(),
					"bind update err, can not find row_id for target %s",
					alias,
				)
			}
			lastNodeID, selectNode, selectNodeTag, targetRowNumberPos[i], err =
				builder.appendTargetRowNumberNode(
					bindCtx,
					lastNodeID,
					selectNode,
					selectNodeTag,
					rowIDPos,
					targetBranchActivePos[i],
				)
			if err != nil {
				return 0, err
			}
		}
	}
	if !isMultiTargetUpdate && updateHasMultipleSourceTables(stmt) {
		lastNodeID, selectNode, selectNodeTag, err = builder.appendUpdateFromDedupNode(
			bindCtx, lastNodeID, selectNode, selectNodeTag, dmlCtx, oldColName2Idx, newColName2Idx)
		if err != nil {
			return 0, err
		}
	}

	for i, alias := range dmlCtx.aliases {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		tableDef := dmlCtx.tableDefs[i]
		if deferRepeatedPhysicalTargetMerge {
			if _, exists := generatedOwnerByTableID[tableDef.TblId]; exists {
				continue
			}
			generatedOwnerByTableID[tableDef.TblId] = i
		}

		// Recompute generated columns after UPDATE FROM dedup so generated
		// expressions read the same deduped base values that will be written.
		for _, col := range tableDef.Cols {
			if col.GeneratedCol == nil {
				continue
			}
			genExpr := builder.applyGeneratedColumnAssignmentCast(
				DeepCopyExpr(col.GeneratedCol.Expr),
				stmt.Ignore,
			)
			genExpr = substituteColRefsInExpr(genExpr, selectNode.ProjectList, colOffsets[i])

			oldPos := oldColName2Idx[alias+"."+col.Name]
			newColName2Idx[alias+"."+col.Name] = oldPos
			if assignedColsByTarget[i] == nil {
				assignedColsByTarget[i] = make(map[string]struct{})
			}
			assignedColsByTarget[i][col.Name] = struct{}{}
			oldColName2Idx[alias+"."+col.Name] = int32(len(selectNode.ProjectList))
			selectNode.ProjectList = append(selectNode.ProjectList, selectNode.ProjectList[oldPos])
			selectNode.ProjectList[oldPos] = genExpr
			if isMultiTargetUpdate {
				targetSelected, buildErr := builder.buildTargetSelectedBelowAssignmentProject(
					selectNode,
					oldColName2Idx[alias+"."+catalog.Row_ID],
					targetRowNumberPos[i],
				)
				if buildErr != nil {
					return 0, buildErr
				}
				selectNode.ProjectList[oldPos], buildErr = builder.guardTargetLocalExpr(
					targetSelected,
					selectNode.ProjectList[oldPos],
					selectNode.ProjectList[oldColName2Idx[alias+"."+col.Name]],
				)
				if buildErr != nil {
					return 0, buildErr
				}
			}
		}
	}
	mayDependOnForeignKeys, err := builder.updateMayDependOnForeignKeys(
		bindCtx, dmlCtx, newColName2Idx)
	if err != nil {
		return 0, err
	}
	if mayDependOnForeignKeys {
		// The plan shape and planner route depend on foreign_key_checks.
		// Preserve that dependency even while checks are disabled so prepared
		// and generic plan caches rebuild after either session-state transition.
		builder.qry.HasForeignKeyAction = true
	}
	for i, tableDef := range dmlCtx.tableDefs {
		if updateAutoIncrCols[i] {
			rowIDPos := oldColName2Idx[dmlCtx.aliases[i]+"."+catalog.Row_ID]
			preInsertTag := builder.genNewBindTag()
			preInsertProject := make([]*plan.Expr, len(selectNode.ProjectList))
			for pos, expr := range selectNode.ProjectList {
				preInsertProject[pos] = &plan.Expr{
					Typ: expr.Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: int32(pos),
					}},
				}
			}
			preInsertNode := &plan.Node{
				NodeType:    plan.Node_PRE_INSERT,
				Children:    []int32{lastNodeID},
				ProjectList: preInsertProject,
				BindingTags: []int32{preInsertTag},
				PreInsertCtx: &plan.PreInsertCtx{
					Ref:                dmlCtx.objRefs[i],
					TableDef:           tableDef,
					HasAutoCol:         true,
					ColOffset:          colOffsets[i],
					IsNewUpdate:        true,
					HasTargetSelector:  targetRowNumberPos[i] >= 0,
					TargetRowNumberCol: targetRowNumberPos[i],
					TargetActiveCol:    targetBranchActivePos[i],
					TargetRowIdCol:     rowIDPos,
				},
			}
			lastNodeID = builder.appendNode(preInsertNode, bindCtx)
			selectNode = preInsertNode
			selectNodeTag = preInsertTag
		}
	}

	if guardTargetAssignmentEvaluation {
		lastNodeID, err = builder.appendSelectedTargetNotNullAssertions(
			bindCtx,
			dmlCtx,
			lastNodeID,
			selectNodeTag,
			selectNode,
			oldColName2Idx,
			newColName2Idx,
			targetRowNumberPos,
			targetBranchActivePos,
		)
		if err != nil {
			return 0, err
		}
	}

	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 || len(tableDef.Checks) == 0 {
			continue
		}
		alias := dmlCtx.aliases[i]
		var targetEligible *plan.Expr
		if targetRowNumberPos[i] >= 0 {
			targetEligible, err = builder.buildTargetSelectedExpr(
				selectNodeTag,
				selectNode,
				targetRowNumberPos[i],
				targetBranchActivePos[i],
			)
			if err != nil {
				return 0, err
			}
		} else {
			selectorPos, found := oldColName2Idx[alias+"."+catalog.Row_ID]
			if !found {
				return 0, moerr.NewInternalErrorf(
					builder.GetContext(),
					"bind update err, can not find row_id for target %s",
					alias,
				)
			}
			selectorCol := &plan.Expr{
				Typ: selectNode.ProjectList[selectorPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: selectNodeTag,
					ColPos: selectorPos,
				}},
			}
			targetEligible, err = BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"isnotnull",
				[]*plan.Expr{selectorCol},
			)
			if err != nil {
				return 0, err
			}
		}
		lastNodeID, err = appendCheckConstraintPlanWithColLookupAndEligibility(
			builder,
			bindCtx,
			tableDef,
			lastNodeID,
			selectNodeTag,
			func(colName string) (int32, bool) {
				qualifiedName := alias + "." + colName
				if colPos, updated := newColName2Idx[qualifiedName]; updated {
					return colPos, true
				}
				colPos, found := oldColName2Idx[qualifiedName]
				return colPos, found
			},
			stmt.Ignore,
			targetEligible,
		)
		if err != nil {
			return 0, err
		}
	}

	lastNodeID, selectNodeTag, selectNode, err = builder.appendUpdateForeignKeyChecks(
		bindCtx,
		dmlCtx,
		lastNodeID,
		selectNodeTag,
		selectNode,
		oldColName2Idx,
		newColName2Idx,
		targetRowNumberPos,
		targetBranchActivePos,
	)
	if err != nil {
		return 0, err
	}

	if stmt.HasReturning() {
		tableDef := dmlCtx.tableDefs[0]
		alias := dmlCtx.aliases[0]
		colPos := make(map[string]int32, len(tableDef.Cols))
		for _, col := range tableDef.Cols {
			qualifiedName := alias + "." + col.Name
			pos, ok := oldColName2Idx[qualifiedName]
			if !ok {
				return 0, moerr.NewInternalErrorf(
					builder.GetContext(), "DML RETURNING cannot locate old image column %s", col.Name,
				)
			}
			if newPos, ok := newColName2Idx[qualifiedName]; ok {
				pos = newPos
			}
			colPos[strings.ToLower(col.Name)] = pos
		}
		lastNodeID = builder.materializeReturningSource(
			bindCtx, lastNodeID, selectNodeTag, tableDef, dmlCtx.objRefs[0], tableDef.Name, alias, colPos,
		)
		selectNode = builder.qry.Nodes[lastNodeID]
		selectNodeTag = selectNode.BindingTags[0]
	}

	idxScanNodes := make([][]*plan.Node, len(dmlCtx.tableDefs))
	pkNeedUpdate := make([]bool, len(dmlCtx.tableDefs))
	idxNeedUpdate := make([][]bool, len(dmlCtx.tableDefs))
	aliasIdxNeedConstraintCheck := make([][]bool, len(dmlCtx.tableDefs))
	updatePkOrUk := false

	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		alias := dmlCtx.aliases[i]

		for _, colName := range tableDef.Pkey.Names {
			if _, ok := newColName2Idx[alias+"."+colName]; ok {
				pkNeedUpdate[i] = true
				updatePkOrUk = true
				break
			}
		}

		idxNeedUpdate[i] = make([]bool, len(tableDef.Indexes))

		for j, idxDef := range tableDef.Indexes {
			for _, colName := range idxDef.Parts {
				if _, ok := assignedColsByTarget[i][catalog.ResolveAlias(colName)]; ok {
					idxNeedUpdate[i][j] = true
					updatePkOrUk = true
					break
				}
			}
		}
		aliasIdxNeedConstraintCheck[i] = append([]bool(nil), idxNeedUpdate[i]...)
	}
	coalesceRepeatedPhysicalTargetRegularIndexes(dmlCtx, idxNeedUpdate)

	if updatePkOrUk {
		newProjTag := builder.genNewBindTag()
		newProjList := make([]*plan.Expr, len(selectNode.ProjectList))
		for i := range selectNode.ProjectList {
			newProjList[i] = &plan.Expr{
				Typ: selectNode.ProjectList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: int32(i),
					},
				},
			}
		}

		newProjNode := &plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: newProjList,
			Children:    []int32{lastNodeID},
			BindingTags: []int32{newProjTag},
		}
		lastNodeID = builder.appendNode(newProjNode, bindCtx)

		makeUpdateIndexPartExpr := func(colPos int32, partName string, prefixLengths map[string]int) (*plan.Expr, error) {
			partName = catalog.ResolveAlias(partName)
			inputExpr := &plan.Expr{
				Typ: selectNode.ProjectList[colPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: colPos,
						Name:   partName,
					},
				},
			}
			return builder.makeIndexPartExprFromInputExpr(inputExpr, partName, prefixLengths)
		}
		guardTargetDedupExpr := func(targetIdx int, expr *plan.Expr) (*plan.Expr, error) {
			rowNumberExpr := &plan.Expr{
				Typ: selectNode.ProjectList[targetRowNumberPos[targetIdx]].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: targetRowNumberPos[targetIdx],
					},
				},
			}
			isSelectedExpr, err := BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"=",
				[]*plan.Expr{rowNumberExpr, MakePlan2Int64ConstExprWithType(1)},
			)
			if err != nil {
				return nil, err
			}
			activeExpr := &plan.Expr{
				Typ: selectNode.ProjectList[targetBranchActivePos[targetIdx]].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: targetBranchActivePos[targetIdx],
					},
				},
			}
			isSelectedExpr, err = BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"and",
				[]*plan.Expr{isSelectedExpr, activeExpr},
			)
			if err != nil {
				return nil, err
			}
			nullExpr := &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{Isnull: true},
				},
			}
			guardedExpr, err := BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"if",
				[]*plan.Expr{isSelectedExpr, expr, nullExpr},
			)
			if err != nil {
				return nil, err
			}
			return guardedExpr, nil
		}
		dedupKeyPos := make(map[string]int32)

		for i, tableDef := range dmlCtx.tableDefs {
			if len(dmlCtx.updateCol2Expr[i]) == 0 {
				continue
			}

			alias := dmlCtx.aliases[i]

			if pkNeedUpdate[i] {
				if len(tableDef.Pkey.Names) > 1 {
					newColName2Idx[alias+"."+catalog.CPrimaryKeyColName] = int32(len(newProjNode.ProjectList))
					args := make([]*plan.Expr, len(tableDef.Pkey.Names))

					for j, colName := range tableDef.Pkey.Names {
						colPos := int32(oldColName2Idx[alias+"."+colName])
						if updateIdx, ok := newColName2Idx[alias+"."+colName]; ok {
							colPos = int32(updateIdx)
						}

						args[j] = &plan.Expr{
							Typ: selectNode.ProjectList[colPos].Typ,
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: selectNodeTag,
									ColPos: colPos,
								},
							},
						}
					}

					newPkExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", args)
					if isMultiTargetUpdate {
						newPkExpr, err = guardTargetDedupExpr(i, newPkExpr)
						if err != nil {
							return 0, err
						}
						dedupKeyPos[alias+"."+tableDef.Pkey.PkeyColName] =
							int32(len(newProjNode.ProjectList))
					}
					newProjNode.ProjectList = append(newProjNode.ProjectList, newPkExpr)
				} else if isMultiTargetUpdate {
					key := alias + "." + tableDef.Pkey.PkeyColName
					colPos := newColName2Idx[key]
					pkExpr := &plan.Expr{
						Typ: selectNode.ProjectList[colPos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{RelPos: selectNodeTag, ColPos: colPos},
						},
					}
					pkExpr, err = guardTargetDedupExpr(i, pkExpr)
					if err != nil {
						return 0, err
					}
					dedupKeyPos[key] = int32(len(newProjNode.ProjectList))
					newProjNode.ProjectList = append(newProjNode.ProjectList, pkExpr)
				}

				scanTag := builder.genNewBindTag()
				scanNodeID := builder.appendNode(&plan.Node{
					NodeType:     plan.Node_TABLE_SCAN,
					TableDef:     tableDef,
					ObjRef:       dmlCtx.objRefs[i],
					BindingTags:  []int32{scanTag},
					ScanSnapshot: bindCtx.snapshot,
				}, bindCtx)

				pkPos := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
				pkTyp := tableDef.Cols[pkPos].Typ
				leftExpr := &plan.Expr{
					Typ: pkTyp,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: scanTag,
							ColPos: pkPos,
						},
					},
				}

				key := alias + "." + tableDef.Pkey.PkeyColName
				newPkPos := newColName2Idx[key]
				if pos, ok := dedupKeyPos[key]; ok {
					newPkPos = pos
				}
				rightExpr := &plan.Expr{
					Typ: pkTyp,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{RelPos: newProjTag, ColPos: newPkPos},
					},
				}
				joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
					leftExpr,
					rightExpr,
				})

				var dedupColName string
				dedupColTypes := make([]plan.Type, len(tableDef.Pkey.Names))

				if len(tableDef.Pkey.Names) == 1 {
					dedupColName = tableDef.Pkey.Names[0]
				} else {
					dedupColName = "(" + strings.Join(tableDef.Pkey.Names, ",") + ")"
				}

				for j, part := range tableDef.Pkey.Names {
					dedupColTypes[j] = tableDef.Cols[tableDef.Name2ColIndex[part]].Typ
				}

				dedupJoinNode := &plan.Node{
					NodeType:          plan.Node_JOIN,
					Children:          []int32{scanNodeID, lastNodeID},
					JoinType:          plan.Node_DEDUP,
					OnList:            []*plan.Expr{joinCond},
					OnDuplicateAction: onDuplicateAction,
					DedupColName:      dedupColName,
					DedupColTypes:     dedupColTypes,
					DedupJoinCtx: &plan.DedupJoinCtx{
						OldColList: []plan.ColRef{
							{
								RelPos: newProjTag,
								ColPos: oldColName2Idx[alias+"."+tableDef.Pkey.PkeyColName],
							},
						},
					},
				}

				lastNodeID = builder.appendNode(dedupJoinNode, bindCtx)
			}

			idxScanNodes[i] = make([]*plan.Node, len(tableDef.Indexes))

			for j, idxDef := range tableDef.Indexes {
				if !idxDef.Unique || !aliasIdxNeedConstraintCheck[i][j] {
					continue
				}

				idxObjRef, idxTableDef, err := builder.compCtx.ResolveIndexTableByRef(dmlCtx.objRefs[i], idxDef.IndexTableName, bindCtx.snapshot)
				if err != nil {
					return 0, err
				}
				idxTag := builder.genNewBindTag()
				builder.addNameByColRef(idxTag, idxTableDef)

				idxScanNode := &plan.Node{
					NodeType:     plan.Node_TABLE_SCAN,
					TableDef:     idxTableDef,
					ObjRef:       idxObjRef,
					BindingTags:  []int32{idxTag},
					ScanSnapshot: bindCtx.snapshot,
				}
				idxTableNodeID := builder.appendNode(idxScanNode, bindCtx)

				prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
				if err != nil {
					return 0, err
				}

				if len(idxDef.Parts) > 1 {
					oldColName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName] = int32(len(newProjNode.ProjectList))
					oldArgs := make([]*plan.Expr, len(idxDef.Parts))

					for j, colName := range idxDef.Parts {
						colName = catalog.ResolveAlias(colName)
						colPos, ok := oldColName2Idx[alias+"."+colName]
						if !ok {
							return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind update err, can not find colName = %s", colName)
						}
						oldArgs[j], err = makeUpdateIndexPartExpr(colPos, colName, prefixLengths)
						if err != nil {
							return 0, err
						}
					}

					oldUkExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", oldArgs)
					newProjNode.ProjectList = append(newProjNode.ProjectList, oldUkExpr)

					newColName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName] = int32(len(newProjNode.ProjectList))
					newArgs := make([]*plan.Expr, len(idxDef.Parts))

					for partPos, colName := range idxDef.Parts {
						colName = catalog.ResolveAlias(colName)
						colPos, ok := oldColName2Idx[alias+"."+colName]
						if !ok {
							return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind update err, can not find colName = %s", colName)
						}
						if updateIdx, ok := newColName2Idx[alias+"."+colName]; ok {
							colPos = updateIdx
						}

						newArgs[partPos], err = makeUpdateIndexPartExpr(colPos, colName, prefixLengths)
						if err != nil {
							return 0, err
						}
					}

					newUkExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", newArgs)
					if isMultiTargetUpdate {
						newUkExpr, err = guardTargetDedupExpr(i, newUkExpr)
						if err != nil {
							return 0, err
						}
						dedupKeyPos[idxTableDef.Name+"."+catalog.IndexTableIndexColName] =
							int32(len(newProjNode.ProjectList))
					}
					newProjNode.ProjectList = append(newProjNode.ProjectList, newUkExpr)
				} else {
					partName := catalog.ResolveAlias(idxDef.Parts[0])
					if prefixLengths[partName] > 0 {
						oldColName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName] = int32(len(newProjNode.ProjectList))
						oldPartPos, ok := oldColName2Idx[alias+"."+partName]
						if !ok {
							return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind update err, can not find colName = %s", partName)
						}
						oldPartExpr, err := makeUpdateIndexPartExpr(oldPartPos, partName, prefixLengths)
						if err != nil {
							return 0, err
						}
						newProjNode.ProjectList = append(newProjNode.ProjectList, oldPartExpr)

						newColName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName] = int32(len(newProjNode.ProjectList))
						newPartPos, ok := newColName2Idx[alias+"."+partName]
						if !ok {
							newPartPos = oldColName2Idx[alias+"."+partName]
						}
						newPartExpr, err := makeUpdateIndexPartExpr(newPartPos, partName, prefixLengths)
						if err != nil {
							return 0, err
						}
						if isMultiTargetUpdate {
							newPartExpr, err = guardTargetDedupExpr(i, newPartExpr)
							if err != nil {
								return 0, err
							}
							dedupKeyPos[idxTableDef.Name+"."+catalog.IndexTableIndexColName] =
								int32(len(newProjNode.ProjectList))
						}
						newProjNode.ProjectList = append(newProjNode.ProjectList, newPartExpr)
					} else {
						oldColName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName] = oldColName2Idx[alias+"."+partName]
						newColName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName] = newColName2Idx[alias+"."+partName]
						if isMultiTargetUpdate {
							key := idxTableDef.Name + "." + catalog.IndexTableIndexColName
							colPos := newColName2Idx[key]
							newPartExpr := &plan.Expr{
								Typ: selectNode.ProjectList[colPos].Typ,
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{RelPos: selectNodeTag, ColPos: colPos},
								},
							}
							newPartExpr, err = guardTargetDedupExpr(i, newPartExpr)
							if err != nil {
								return 0, err
							}
							dedupKeyPos[key] = int32(len(newProjNode.ProjectList))
							newProjNode.ProjectList = append(newProjNode.ProjectList, newPartExpr)
						}
					}
				}

				rightPkPos := idxTableDef.Name2ColIndex[catalog.IndexTableIndexColName]
				pkTyp := idxTableDef.Cols[rightPkPos].Typ

				leftExpr := &plan.Expr{
					Typ: pkTyp,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: idxTag,
							ColPos: rightPkPos,
						},
					},
				}

				key := idxTableDef.Name + "." + catalog.IndexTableIndexColName
				newIdxPos := newColName2Idx[key]
				if pos, ok := dedupKeyPos[key]; ok {
					newIdxPos = pos
				}
				rightExpr := &plan.Expr{
					Typ: pkTyp,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{RelPos: newProjTag, ColPos: newIdxPos},
					},
				}
				joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
					leftExpr,
					rightExpr,
				})

				var dedupColName string
				dedupColTypes := make([]plan.Type, len(idxDef.Parts))

				if len(idxDef.Parts) == 1 {
					dedupColName = catalog.ResolveAlias(idxDef.Parts[0])
				} else {
					dedupColName = "(" + strings.Join(idxDef.Parts, ",") + ")"
				}

				for j, part := range idxDef.Parts {
					dedupColTypes[j] = tableDef.Cols[tableDef.Name2ColIndex[catalog.ResolveAlias(part)]].Typ
				}

				dedupJoinNode := &plan.Node{
					NodeType:          plan.Node_JOIN,
					Children:          []int32{idxTableNodeID, lastNodeID},
					JoinType:          plan.Node_DEDUP,
					OnList:            []*plan.Expr{joinCond},
					OnDuplicateAction: onDuplicateAction,
					DedupColName:      dedupColName,
					DedupColTypes:     dedupColTypes,
					DedupJoinCtx: &plan.DedupJoinCtx{
						OldColList: []plan.ColRef{
							{
								RelPos: newProjTag,
								ColPos: oldColName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName],
							},
						},
					},
				}

				lastNodeID = builder.appendNode(dedupJoinNode, bindCtx)
			}
		}

		selectNodeTag = newProjTag
		selectNode = newProjNode
	}

	if deferRepeatedPhysicalTargetMerge {
		mergeInputTag := builder.genNewBindTag()
		mergeInputProject := make([]*plan.Expr, len(selectNode.ProjectList))
		for pos, expr := range selectNode.ProjectList {
			mergeInputProject[pos] = &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: selectNodeTag,
					ColPos: int32(pos),
				}},
			}
		}
		selectNode = &plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{lastNodeID},
			ProjectList: mergeInputProject,
			BindingTags: []int32{mergeInputTag},
		}
		lastNodeID = builder.appendNode(selectNode, bindCtx)
		selectNodeTag = mergeInputTag
		lastNodeID, selectNode, selectNodeTag, err = builder.mergeSamePhysicalTargetAssignments(
			bindCtx,
			lastNodeID,
			selectNode,
			selectNodeTag,
			dmlCtx,
			oldColName2Idx,
			newColName2Idx,
			targetBranchActivePos,
			assignedColsByTarget,
			colOffsets,
			true,
			true,
			true,
		)
		if err != nil {
			return 0, err
		}
		groupActiveProject := make([]*plan.Expr, len(selectNode.ProjectList))
		for pos, expr := range selectNode.ProjectList {
			groupActiveProject[pos] = &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: selectNodeTag,
					ColPos: int32(pos),
				}},
			}
		}
		targetsByTableID := make(map[uint64][]int)
		var physicalTableOrder []uint64
		for targetIdx, updateCols := range dmlCtx.updateCol2Expr {
			if len(updateCols) == 0 {
				continue
			}
			tableID := dmlCtx.tableDefs[targetIdx].TblId
			if len(targetsByTableID[tableID]) == 0 {
				physicalTableOrder = append(physicalTableOrder, tableID)
			}
			targetsByTableID[tableID] = append(targetsByTableID[tableID], targetIdx)
		}
		for _, tableID := range physicalTableOrder {
			targets := targetsByTableID[tableID]
			var groupActiveExpr *plan.Expr
			for _, targetIdx := range targets {
				activePos := targetBranchActivePos[targetIdx]
				activeExpr := &plan.Expr{
					Typ: selectNode.ProjectList[activePos].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: activePos,
					}},
				}
				if groupActiveExpr == nil {
					groupActiveExpr = activeExpr
					continue
				}
				groupActiveExpr, err = BindFuncExprImplByPlanExpr(
					builder.GetContext(),
					"or",
					[]*plan.Expr{groupActiveExpr, activeExpr},
				)
				if err != nil {
					return 0, err
				}
			}
			groupActivePos := int32(len(groupActiveProject))
			groupActiveProject = append(groupActiveProject, groupActiveExpr)
			for _, targetIdx := range targets {
				physicalTargetActivePos[targetIdx] = groupActivePos
			}
		}
		groupActiveTag := builder.genNewBindTag()
		selectNode = &plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{lastNodeID},
			ProjectList: groupActiveProject,
			BindingTags: []int32{groupActiveTag},
		}
		lastNodeID = builder.appendNode(selectNode, bindCtx)
		selectNodeTag = groupActiveTag
		for _, tableID := range physicalTableOrder {
			targets := targetsByTableID[tableID]
			ownerIdx := targets[0]
			rowIDPos := oldColName2Idx[dmlCtx.aliases[ownerIdx]+"."+catalog.Row_ID]
			var groupRowNumberPos int32
			lastNodeID, selectNode, selectNodeTag, groupRowNumberPos, err =
				builder.appendTargetRowNumberNode(
					bindCtx,
					lastNodeID,
					selectNode,
					selectNodeTag,
					rowIDPos,
					physicalTargetActivePos[ownerIdx],
				)
			if err != nil {
				return 0, err
			}
			for _, targetIdx := range targets {
				targetRowNumberPos[targetIdx] = groupRowNumberPos
			}
		}
		for tableID, ownerIdx := range generatedOwnerByTableID {
			tableDef := dmlCtx.tableDefs[ownerIdx]
			ownerAlias := dmlCtx.aliases[ownerIdx]
			for _, col := range tableDef.Cols {
				if col.GeneratedCol == nil {
					continue
				}
				genExpr := builder.applyGeneratedColumnAssignmentCast(
					DeepCopyExpr(col.GeneratedCol.Expr),
					stmt.Ignore,
				)
				genExpr = substituteColRefsInExpr(genExpr, selectNode.ProjectList, colOffsets[ownerIdx])
				generatedPos, ok := newColName2Idx[ownerAlias+"."+col.Name]
				if !ok || generatedPos < 0 || int(generatedPos) >= len(selectNode.ProjectList) {
					return 0, moerr.NewInternalErrorf(
						builder.GetContext(),
						"bind update err, can not find generated column %s for target %s",
						col.Name,
						ownerAlias,
					)
				}
				selectNode.ProjectList[generatedPos] = genExpr
				for targetIdx, targetDef := range dmlCtx.tableDefs {
					if targetDef.TblId == tableID && len(dmlCtx.updateCol2Expr[targetIdx]) > 0 {
						newColName2Idx[dmlCtx.aliases[targetIdx]+"."+col.Name] = generatedPos
					}
				}
			}
		}
	}

	// join index tables to get old RowID
	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		alias := dmlCtx.aliases[i]

		for j, idxDef := range tableDef.Indexes {
			if !pkNeedUpdate[i] && !idxNeedUpdate[i][j] {
				continue
			}

			idxObjRef, idxTableDef, err := builder.compCtx.ResolveIndexTableByRef(dmlCtx.objRefs[i], idxDef.IndexTableName, bindCtx.snapshot)
			if err != nil {
				return 0, err
			}
			idxTag := builder.genNewBindTag()
			builder.addNameByColRef(idxTag, idxTableDef)

			idxScanNodes[i][j] = &plan.Node{
				NodeType:     plan.Node_TABLE_SCAN,
				TableDef:     idxTableDef,
				ObjRef:       idxObjRef,
				BindingTags:  []int32{idxTag},
				ScanSnapshot: bindCtx.snapshot,
			}
			idxTableNodeID := builder.appendNode(idxScanNodes[i][j], bindCtx)

			lookupColName := indexLookupColumnName(idxDef)
			rightPkPos := idxTableDef.Name2ColIndex[lookupColName]
			pkTyp := idxTableDef.Cols[rightPkPos].Typ

			rightExpr := &plan.Expr{
				Typ: pkTyp,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTag,
						ColPos: rightPkPos,
					},
				},
			}

			var leftExpr *plan.Expr
			if isSpatialIndexDef(idxDef) {
				colPos := oldColName2Idx[alias+"."+tableDef.Pkey.PkeyColName]
				leftExpr = &plan.Expr{
					Typ: selectNode.ProjectList[colPos].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: selectNodeTag,
							ColPos: colPos,
						},
					},
				}
			} else {
				args := make([]*plan.Expr, len(idxDef.Parts))

				prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
				if err != nil {
					return 0, err
				}

				var colPos int32
				var ok bool
				for k, colName := range idxDef.Parts {
					colName = catalog.ResolveAlias(colName)
					if colPos, ok = oldColName2Idx[alias+"."+colName]; !ok {
						errMsg := fmt.Sprintf("bind update err, can not find colName = %s", colName)
						return 0, moerr.NewInternalError(builder.GetContext(), errMsg)
					}
					inputExpr := &plan.Expr{
						Typ: selectNode.ProjectList[colPos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: selectNodeTag,
								ColPos: colPos,
								Name:   colName,
							},
						},
					}
					args[k], err = builder.makeIndexPartExprFromInputExpr(inputExpr, colName, prefixLengths)
					if err != nil {
						return 0, err
					}
				}

				leftExpr = args[0]
				if indexTableStoresSerializedKey(idxDef) {
					funcName := "serial"
					if !idxDef.Unique {
						funcName = "serial_full"
					}
					leftExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, args)
				}
			}

			joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
				leftExpr,
				rightExpr,
			})

			joinType := plan.Node_LEFT
			if !isMultiTargetUpdate && !idxDef.Unique && !isSpatialIndexDef(idxDef) {
				joinType = plan.Node_INNER
			}
			lastNodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: []int32{lastNodeID, idxTableNodeID},
				JoinType: joinType,
				OnList:   []*plan.Expr{joinCond},
			}, bindCtx)
		}
	}

	lockTargets := make([]*plan.LockTarget, 0)
	updateCtxList := make([]*plan.UpdateCtx, 0)

	finalProjTag := builder.genNewBindTag()
	finalColName2Idx := make(map[string]int32)
	var finalProjList []*plan.Expr
	targetRowNumberFinalPos := make([]int32, len(dmlCtx.aliases))
	targetActiveFinalPos := make([]int32, len(dmlCtx.aliases))
	physicalTargetActiveFinalPos := make([]int32, len(dmlCtx.aliases))
	targetUpdateCtxIdx := make([]int32, len(dmlCtx.aliases))
	targetOldPkFinalPos := make([]int32, len(dmlCtx.aliases))
	physicalTargetOwner := make([]int, len(dmlCtx.aliases))
	for i := range physicalTargetOwner {
		physicalTargetOwner[i] = -1
		targetUpdateCtxIdx[i] = -1
	}
	ownerByTableID := make(map[uint64]int)
	for i, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) == 0 {
			continue
		}
		tableID := dmlCtx.tableDefs[i].TblId
		owner, ok := ownerByTableID[tableID]
		if !ok {
			owner = i
			ownerByTableID[tableID] = owner
		}
		physicalTargetOwner[i] = owner
	}

	finalProjNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		BindingTags: []int32{finalProjTag},
	}
	lastNodeID = builder.appendNode(finalProjNode, bindCtx)

	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		alias := dmlCtx.aliases[i]
		insertCols := make([]plan.ColRef, len(tableDef.Cols)-1)
		updatedClusterByExpr, err := builder.makeUpdatedClusterByExpr(
			alias,
			tableDef,
			selectNode,
			selectNodeTag,
			oldColName2Idx,
			newColName2Idx,
		)
		if err != nil {
			return 0, err
		}

		for j, col := range tableDef.Cols {
			finalColIdx := len(finalProjList)

			if col.Name != catalog.Row_ID {
				insertCols[j].RelPos = finalProjTag
				insertCols[j].ColPos = int32(finalColIdx)
			}

			var finalExpr *plan.Expr
			if updatedClusterByExpr != nil && col.Name == tableDef.ClusterBy.Name {
				finalExpr = updatedClusterByExpr
			} else {
				colIdx := oldColName2Idx[alias+"."+col.Name]
				if updateIdx, ok := newColName2Idx[alias+"."+col.Name]; ok {
					colIdx = updateIdx
				}
				finalExpr = &plan.Expr{
					Typ: selectNode.ProjectList[colIdx].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: selectNodeTag,
							ColPos: colIdx,
						},
					},
				}
			}
			finalColName2Idx[alias+"."+col.Name] = int32(finalColIdx)
			finalProjList = append(finalProjList, finalExpr)
		}

		oldPkPos := finalColName2Idx[alias+"."+tableDef.Pkey.PkeyColName]
		newPkPos := oldPkPos
		if updateIdx, ok := newColName2Idx[alias+"."+tableDef.Pkey.PkeyColName]; ok {
			oldPkPos = int32(len(finalProjList))
			finalProjList = append(finalProjList, &plan.Expr{
				Typ: selectNode.ProjectList[updateIdx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: oldColName2Idx[alias+"."+tableDef.Pkey.PkeyColName],
					},
				},
			})
		}
		targetOldPkFinalPos[i] = oldPkPos
		if isMultiTargetUpdate {
			targetRowNumberFinalPos[i] = int32(len(finalProjList))
			rowNumberPos := targetRowNumberPos[i]
			finalProjList = append(finalProjList, &plan.Expr{
				Typ: selectNode.ProjectList[rowNumberPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: rowNumberPos,
					},
				},
			})
			targetActiveFinalPos[i] = int32(len(finalProjList))
			finalProjList = append(finalProjList, &plan.Expr{
				Typ: selectNode.ProjectList[targetBranchActivePos[i]].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: targetBranchActivePos[i],
					},
				},
			})
			physicalTargetActiveFinalPos[i] = targetActiveFinalPos[i]
			if deferRepeatedPhysicalTargetMerge &&
				physicalTargetActivePos[i] != targetBranchActivePos[i] {
				physicalTargetActiveFinalPos[i] = int32(len(finalProjList))
				finalProjList = append(finalProjList, &plan.Expr{
					Typ: selectNode.ProjectList[physicalTargetActivePos[i]].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: selectNodeTag,
							ColPos: physicalTargetActivePos[i],
						},
					},
				})
			}
		}

		updateCtx := &plan.UpdateCtx{
			ObjRef:     dmlCtx.objRefs[i],
			TableDef:   tableDef,
			InsertCols: insertCols,
			DeleteCols: []plan.ColRef{
				{
					RelPos: finalProjTag,
					ColPos: finalColName2Idx[alias+"."+catalog.Row_ID],
				},
				{
					RelPos: finalProjTag,
					ColPos: oldPkPos,
				},
			},
		}
		if tableDef.Partition != nil && len(tableDef.Partition.PartitionDefs) > 0 {
			partitionColName := getPartitionColName(tableDef.Partition.PartitionDefs[0].Def)
			if partitionColPos, ok := finalColName2Idx[alias+"."+partitionColName]; ok {
				oldPartitionColPos := partitionColPos
				if _, updated := newColName2Idx[alias+"."+partitionColName]; updated {
					if partitionColName == tableDef.Pkey.PkeyColName {
						oldPartitionColPos = oldPkPos
					} else {
						oldPartitionColPos = int32(len(finalProjList))
						oldSelectPos := oldColName2Idx[alias+"."+partitionColName]
						finalProjList = append(finalProjList, &plan.Expr{
							Typ: selectNode.ProjectList[oldSelectPos].Typ,
							Expr: &plan.Expr_Col{Col: &plan.ColRef{
								RelPos: selectNodeTag,
								ColPos: oldSelectPos,
							}},
						})
					}
				}
				updateCtx.PartitionCols = []plan.ColRef{{
					RelPos: finalProjTag,
					ColPos: oldPartitionColPos,
				}}
				if oldPartitionColPos != partitionColPos {
					updateCtx.PartitionCols = append(updateCtx.PartitionCols, plan.ColRef{
						RelPos: finalProjTag,
						ColPos: partitionColPos,
					})
				}
			}
		}
		appendMainContext := !isMultiTargetUpdate || physicalTargetOwner[i] == i
		if isMultiTargetUpdate && appendMainContext {
			updateCtx.DedupByTargetRowId = true
			targetUpdateCtxIdx[i] = int32(len(updateCtxList))
			updateCtx.TargetUpdateCtxIdx = targetUpdateCtxIdx[i]
			updateCtx.DeleteCols = append(updateCtx.DeleteCols, plan.ColRef{
				RelPos: finalProjTag,
				ColPos: targetRowNumberFinalPos[i],
			})
			updateCtx.DeleteCols = append(updateCtx.DeleteCols, plan.ColRef{
				RelPos: finalProjTag,
				ColPos: physicalTargetActiveFinalPos[i],
			})
		} else if isMultiTargetUpdate {
			targetUpdateCtxIdx[i] = targetUpdateCtxIdx[physicalTargetOwner[i]]
		}
		if appendMainContext {
			updateCtxList = append(updateCtxList, updateCtx)
			lockTargets = append(lockTargets, &plan.LockTarget{
				TableId:            tableDef.TblId,
				ObjRef:             dmlCtx.objRefs[i],
				PrimaryColIdxInBat: int32(newPkPos),
				PrimaryColRelPos:   finalProjTag,
				PrimaryColTyp:      finalProjList[newPkPos].Typ,
			})
			if newPkPos != oldPkPos {
				lockTargets = append(lockTargets, &plan.LockTarget{
					TableId:            tableDef.TblId,
					ObjRef:             dmlCtx.objRefs[i],
					PrimaryColIdxInBat: int32(oldPkPos),
					PrimaryColRelPos:   finalProjTag,
					PrimaryColTyp:      finalProjList[oldPkPos].Typ,
				})
			}
		}

		for j, idxNode := range idxScanNodes[i] {
			if !pkNeedUpdate[i] && !idxNeedUpdate[i][j] {
				continue
			}

			insertCols := make([]plan.ColRef, 2)
			deleteCols := make([]plan.ColRef, 2)

			idxNodeTag := idxNode.BindingTags[0]

			oldIdx := len(finalProjList)
			rowIDIdx := idxNode.TableDef.Name2ColIndex[catalog.Row_ID]
			finalProjList = append(finalProjList, &plan.Expr{
				Typ: idxNode.TableDef.Cols[rowIDIdx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxNodeTag,
						ColPos: rowIDIdx,
					},
				},
			})
			deleteCols[0].RelPos = finalProjTag
			deleteCols[0].ColPos = int32(oldIdx)

			oldIdx = len(finalProjList)
			lookupColName := indexLookupColumnName(tableDef.Indexes[j])
			idxColIdx := idxNode.TableDef.Name2ColIndex[lookupColName]
			finalProjList = append(finalProjList, &plan.Expr{
				Typ: idxNode.TableDef.Cols[idxColIdx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxNodeTag,
						ColPos: idxColIdx,
					},
				},
			})
			deleteCols[1].RelPos = finalProjTag
			deleteCols[1].ColPos = int32(oldIdx)

			newIdx := oldIdx

			idxDef := tableDef.Indexes[j]
			prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
			if err != nil {
				return 0, err
			}
			if !idxDef.Unique || idxNeedUpdate[i][j] {
				var newIdxExpr *plan.Expr
				if !indexTableStoresSerializedKey(idxDef) {
					realColName := indexPrimaryPartName(idxDef)
					colPos := int32(oldColName2Idx[alias+"."+realColName])
					if updateIdx, ok := newColName2Idx[alias+"."+realColName]; ok {
						colPos = int32(updateIdx)
					}
					inputExpr := &plan.Expr{
						Typ: selectNode.ProjectList[colPos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: selectNodeTag,
								ColPos: colPos,
								Name:   realColName,
							},
						},
					}
					newIdxExpr, err = builder.makeIndexPartExprFromInputExpr(inputExpr, realColName, prefixLengths)
					if err != nil {
						return 0, err
					}
				} else {
					args := make([]*plan.Expr, len(idxDef.Parts))

					for k, colName := range idxDef.Parts {
						realColName := catalog.ResolveAlias(colName)
						colPos := int32(oldColName2Idx[alias+"."+realColName])
						if updateIdx, ok := newColName2Idx[alias+"."+realColName]; ok {
							colPos = int32(updateIdx)
						}
						args[k] = &plan.Expr{
							Typ: selectNode.ProjectList[colPos].Typ,
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: selectNodeTag,
									ColPos: colPos,
									Name:   realColName,
								},
							},
						}
						args[k], err = builder.makeIndexPartExprFromInputExpr(args[k], realColName, prefixLengths)
						if err != nil {
							return 0, err
						}
					}

					funcName := "serial"
					if !idxDef.Unique {
						funcName = "serial_full"
					}
					newIdxExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, args)
				}

				newIdx = len(finalProjList)
				finalProjList = append(finalProjList, newIdxExpr)
			}

			insertCols[0].RelPos = finalProjTag
			insertCols[0].ColPos = int32(newIdx)

			insertCols[1].RelPos = finalProjTag
			insertCols[1].ColPos = finalColName2Idx[alias+"."+tableDef.Pkey.PkeyColName]

			updateCtx := &plan.UpdateCtx{
				ObjRef:     idxNode.ObjRef,
				TableDef:   idxNode.TableDef,
				InsertCols: insertCols,
				DeleteCols: deleteCols,
			}
			if isMultiTargetUpdate {
				updateCtx.TargetUpdateCtxIdx = targetUpdateCtxIdx[i]
			}
			updateCtxList = append(updateCtxList, updateCtx)

			if idxDef.Unique {
				lockTargets = append(lockTargets, &plan.LockTarget{
					TableId:            idxNode.TableDef.TblId,
					ObjRef:             idxNode.ObjRef,
					PrimaryColIdxInBat: int32(oldIdx),
					PrimaryColRelPos:   finalProjTag,
					PrimaryColTyp:      finalProjList[oldIdx].Typ,
				})
				if idxNeedUpdate[i][j] {
					lockTargets = append(lockTargets, &plan.LockTarget{
						TableId:            idxNode.TableDef.TblId,
						ObjRef:             idxNode.ObjRef,
						PrimaryColIdxInBat: int32(newIdx),
						PrimaryColRelPos:   finalProjTag,
						PrimaryColTyp:      finalProjList[newIdx].Typ,
					})
				}
			}
		}
	}

	finalProjNode.ProjectList = finalProjList
	if isMultiTargetUpdate {
		for targetIdx, owner := range physicalTargetOwner {
			if owner < 0 {
				continue
			}
			ownerCtxIdx := targetUpdateCtxIdx[owner]
			if ownerCtxIdx < 0 || int(ownerCtxIdx) >= len(updateCtxList) {
				return 0, moerr.NewInternalError(
					builder.GetContext(),
					"invalid multi-target update physical owner context",
				)
			}
			updateCtxList[ownerCtxIdx].AffectedRowsCols = append(
				updateCtxList[ownerCtxIdx].AffectedRowsCols,
				plan.ColRef{
					RelPos: finalProjTag,
					ColPos: targetActiveFinalPos[targetIdx],
				},
			)
		}
	}
	sort.SliceStable(lockTargets, func(i, j int) bool {
		if lockTargets[i].TableId != lockTargets[j].TableId {
			return lockTargets[i].TableId < lockTargets[j].TableId
		}
		return lockTargets[i].PrimaryColIdxInBat < lockTargets[j].PrimaryColIdxInBat
	})

	// Synchronous irregular indexes share the exact final row image with the
	// base-table MULTI_UPDATE. Their stale entries are deleted by the immutable
	// old PK and rebuilt after createQuery from this materialized step. Async
	// indexes are deliberately absent and remain CDC-only.
	irregularBaseStep := int32(-1)
	for _, indexes := range inlineIrregularIndexes {
		if len(indexes) > 0 {
			globalSinkID := appendSinkNodeWithTag(builder, bindCtx, lastNodeID, finalProjTag)
			irregularBaseStep = builder.appendStep(globalSinkID)
			lastNodeID = builder.appendTaggedSinkScan(bindCtx, irregularBaseStep, finalProjTag)
			break
		}
	}
	for i, indexes := range inlineIrregularIndexes {
		if len(indexes) == 0 {
			continue
		}
		alias := dmlCtx.aliases[i]
		tableDef := dmlCtx.tableDefs[i]
		localProjTag := builder.genNewBindTag()
		localProjList, deletePkPos := buildIrregularUpdateTargetProjection(
			alias, tableDef, finalProjTag, finalProjList, finalColName2Idx, targetOldPkFinalPos[i])
		rowNumberPos := int32(-1)
		activePos := int32(-1)
		if isMultiTargetUpdate {
			rowNumberPos = int32(len(localProjList))
			globalRowNumberPos := targetRowNumberFinalPos[i]
			localProjList = append(localProjList, &plan.Expr{
				Typ: finalProjList[globalRowNumberPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: finalProjTag,
					ColPos: globalRowNumberPos,
				}},
			})
			activePos = rowNumberPos + 1
			globalActivePos := physicalTargetActiveFinalPos[i]
			localProjList = append(localProjList, &plan.Expr{
				Typ: finalProjList[globalActivePos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: finalProjTag,
					ColPos: globalActivePos,
				}},
			})
		}
		localSourceID := builder.appendTaggedSinkScan(bindCtx, irregularBaseStep, finalProjTag)
		localProjID := builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{localSourceID},
			ProjectList: localProjList,
			BindingTags: []int32{localProjTag},
		}, bindCtx)
		_, err = builder.appendOnDupIrregularMaintSource(
			bindCtx,
			localProjID,
			localProjTag,
			deletePkPos,
			localProjList[deletePkPos].Typ,
			rowNumberPos,
			activePos,
			indexes,
			tableDef,
			dmlCtx.objRefs[i],
		)
		if err != nil {
			return 0, err
		}
		builder.irregularUpdateMaints = append(
			builder.irregularUpdateMaints,
			irregularUpdateMaintenance{
				sourceStep:  builder.irregularMaintSourceStep,
				deleteStep:  builder.irregularMaintDeleteStep,
				deletePkPos: builder.irregularMaintDeletePkPos,
				deletePkTyp: builder.irregularMaintDeletePkTyp,
				indexes:     builder.irregularMaintIndexes,
				tableDef:    builder.irregularMaintTableDef,
				objRef:      builder.irregularMaintObjRef,
			},
		)
	}

	dmlNode := &plan.Node{
		NodeType:      plan.Node_MULTI_UPDATE,
		BindingTags:   []int32{builder.genNewBindTag()},
		UpdateCtxList: updateCtxList,
	}

	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		Children:    []int32{lastNodeID},
		TableDef:    dmlCtx.tableDefs[0],
		BindingTags: []int32{builder.genNewBindTag()},
		LockTargets: lockTargets,
	}, bindCtx)
	reCheckifNeedLockWholeTable(builder)

	dmlNode.Children = append(dmlNode.Children, lastNodeID)
	lastNodeID = builder.appendNode(dmlNode, bindCtx)

	return lastNodeID, err
}

func coalesceRepeatedPhysicalTargetIrregularIndexes(
	dmlCtx *DMLContext,
	inlineIrregularIndexes [][]*plan.IndexDef,
) {
	ownerByTableID := make(map[uint64]int)
	indexesByTableID := make(map[uint64]map[string]*plan.IndexDef)
	for targetIdx, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) == 0 {
			continue
		}
		tableID := dmlCtx.tableDefs[targetIdx].TblId
		if _, ok := ownerByTableID[tableID]; !ok {
			ownerByTableID[tableID] = targetIdx
		}
		if indexesByTableID[tableID] == nil {
			indexesByTableID[tableID] = make(map[string]*plan.IndexDef)
		}
		for _, indexDef := range inlineIrregularIndexes[targetIdx] {
			key := indexDef.IndexName + "\x00" + indexDef.IndexTableName
			indexesByTableID[tableID][key] = indexDef
		}
		inlineIrregularIndexes[targetIdx] = nil
	}
	for tableID, ownerIdx := range ownerByTableID {
		keys := make([]string, 0, len(indexesByTableID[tableID]))
		for key := range indexesByTableID[tableID] {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			inlineIrregularIndexes[ownerIdx] = append(
				inlineIrregularIndexes[ownerIdx],
				indexesByTableID[tableID][key],
			)
		}
	}
}

func coalesceRepeatedPhysicalTargetRegularIndexes(
	dmlCtx *DMLContext,
	idxNeedUpdate [][]bool,
) {
	ownerByTableID := make(map[uint64]int)
	ownerIndexPos := make(map[uint64]map[string]int)
	for targetIdx, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) == 0 {
			continue
		}
		tableID := dmlCtx.tableDefs[targetIdx].TblId
		if _, ok := ownerByTableID[tableID]; ok {
			continue
		}
		ownerByTableID[tableID] = targetIdx
		ownerIndexPos[tableID] = make(map[string]int)
		for indexPos, indexDef := range dmlCtx.tableDefs[targetIdx].Indexes {
			key := indexDef.IndexName + "\x00" + indexDef.IndexTableName
			ownerIndexPos[tableID][key] = indexPos
		}
	}
	for targetIdx, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) == 0 {
			continue
		}
		tableID := dmlCtx.tableDefs[targetIdx].TblId
		ownerIdx := ownerByTableID[tableID]
		if targetIdx == ownerIdx {
			continue
		}
		for indexPos, needsUpdate := range idxNeedUpdate[targetIdx] {
			if !needsUpdate {
				continue
			}
			indexDef := dmlCtx.tableDefs[targetIdx].Indexes[indexPos]
			key := indexDef.IndexName + "\x00" + indexDef.IndexTableName
			if ownerPos, ok := ownerIndexPos[tableID][key]; ok {
				idxNeedUpdate[ownerIdx][ownerPos] = true
			}
		}
		clear(idxNeedUpdate[targetIdx])
	}
}

func validateRepeatedPhysicalTargetPrimaryKeyUpdate(ctx context.Context, dmlCtx *DMLContext) error {
	targetsByTableID := make(map[uint64][]int)
	for targetIdx, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) == 0 {
			continue
		}
		tableDef := dmlCtx.tableDefs[targetIdx]
		targetsByTableID[tableDef.TblId] = append(targetsByTableID[tableDef.TblId], targetIdx)
	}
	for _, targets := range targetsByTableID {
		if len(targets) < 2 {
			continue
		}
		for _, targetIdx := range targets {
			tableDef := dmlCtx.tableDefs[targetIdx]
			if tableDef.Pkey == nil {
				continue
			}
			for _, pkName := range tableDef.Pkey.Names {
				if _, updated := dmlCtx.updateCol2Expr[targetIdx][pkName]; updated {
					return moerr.NewMultiUpdateKeyConflict(
						ctx,
						dmlCtx.aliases[targets[0]],
						dmlCtx.aliases[targets[1]],
					)
				}
			}
		}
	}
	return nil
}

func hasRepeatedPhysicalUpdateTarget(dmlCtx *DMLContext) bool {
	seen := make(map[uint64]struct{})
	for targetIdx, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) == 0 {
			continue
		}
		tableID := dmlCtx.tableDefs[targetIdx].TblId
		if _, ok := seen[tableID]; ok {
			return true
		}
		seen[tableID] = struct{}{}
	}
	return false
}

func collectPhysicalTargetAssignedCols(
	dmlCtx *DMLContext,
	newColName2Idx map[string]int32,
) map[int]map[string]struct{} {
	assignedColsByTarget := make(map[int]map[string]struct{})
	for targetIdx, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) == 0 {
			continue
		}
		alias := dmlCtx.aliases[targetIdx]
		for _, col := range dmlCtx.tableDefs[targetIdx].Cols {
			if _, assigned := newColName2Idx[alias+"."+col.Name]; !assigned {
				continue
			}
			if assignedColsByTarget[targetIdx] == nil {
				assignedColsByTarget[targetIdx] = make(map[string]struct{})
			}
			assignedColsByTarget[targetIdx][col.Name] = struct{}{}
		}
	}
	return assignedColsByTarget
}

func (builder *QueryBuilder) mergeSamePhysicalTargetAssignments(
	bindCtx *BindContext,
	lastNodeID int32,
	selectNode *plan.Node,
	selectNodeTag int32,
	dmlCtx *DMLContext,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	targetBranchActivePos []int32,
	assignedColsByTarget map[int]map[string]struct{},
	colOffsets []int32,
	mergeRepeatedPhysicalTargets bool,
	exclusiveTargetBranches bool,
	validateMergedUnique bool,
) (int32, *plan.Node, int32, error) {
	targetsByTableID := make(map[uint64][]int)
	var tableIDs []uint64
	for i, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) > 0 {
			tableID := dmlCtx.tableDefs[i].TblId
			if len(targetsByTableID[tableID]) == 0 {
				tableIDs = append(tableIDs, tableID)
			}
			targetsByTableID[tableID] = append(targetsByTableID[tableID], i)
		}
	}

	if len(tableIDs) < 2 {
		hasRepeatedPhysicalTarget := false
		for _, targets := range targetsByTableID {
			if len(targets) > 1 {
				hasRepeatedPhysicalTarget = true
				break
			}
		}
		if !hasRepeatedPhysicalTarget {
			return lastNodeID, selectNode, selectNodeTag, nil
		}
	}
	if len(tableIDs) == 0 {
		return lastNodeID, selectNode, selectNodeTag, nil
	}
	// Reserve the final-row positions needed by every alias before branching.
	// Every physical-target branch must expose exactly the same schema to the
	// outer UNION ALL, including columns assigned through a sibling alias.
	for _, tableID := range tableIDs {
		targets := targetsByTableID[tableID]
		if len(targets) < 2 {
			continue
		}
		updatedCols := make(map[string]struct{})
		for _, targetIdx := range targets {
			for colName := range dmlCtx.updateCol2Expr[targetIdx] {
				updatedCols[colName] = struct{}{}
			}
		}
		for _, targetIdx := range targets {
			alias := dmlCtx.aliases[targetIdx]
			for colName := range updatedCols {
				key := alias + "." + colName
				if _, exists := newColName2Idx[key]; exists {
					continue
				}
				oldPos := oldColName2Idx[key]
				newColName2Idx[key] = oldPos
				oldColName2Idx[key] = int32(len(selectNode.ProjectList))
				selectNode.ProjectList = append(
					selectNode.ProjectList,
					DeepCopyExpr(selectNode.ProjectList[oldPos]),
				)
			}
		}
	}

	sourceSinkID := appendSinkNode(builder, bindCtx, lastNodeID)
	if builder.preserveSinkProjection == nil {
		builder.preserveSinkProjection = make(map[int32]struct{})
	}
	builder.preserveSinkProjection[sourceSinkID] = struct{}{}
	sourceStep := builder.appendStep(sourceSinkID)

	targetGroups := make([][]int, 0, len(tableIDs))
	if mergeRepeatedPhysicalTargets {
		for _, tableID := range tableIDs {
			targetGroups = append(targetGroups, targetsByTableID[tableID])
		}
	} else {
		for _, tableID := range tableIDs {
			for _, targetIdx := range targetsByTableID[tableID] {
				targetGroups = append(targetGroups, []int{targetIdx})
			}
		}
	}
	sort.SliceStable(targetGroups, func(i, j int) bool {
		return len(targetGroups[i]) < len(targetGroups[j])
	})
	branchIDs := make([]int32, 0, len(targetGroups))
	branchNodes := make([]*plan.Node, 0, len(targetGroups))
	for _, targets := range targetGroups {
		var branchID int32
		var branchNode *plan.Node
		var err error
		if len(targets) > 1 {
			if validateMergedUnique {
				branchID, branchNode, err = builder.mergeRepeatedPhysicalTargetAssignmentsWithUniqueFallback(
					bindCtx,
					sourceStep,
					selectNode,
					dmlCtx,
					targets,
					oldColName2Idx,
					newColName2Idx,
					assignedColsByTarget,
					colOffsets,
					targetBranchActivePos,
				)
			} else {
				branchID, branchNode, err = builder.mergeSamePhysicalTargetAssignmentsAcrossTuples(
					bindCtx,
					sourceStep,
					selectNode,
					dmlCtx,
					targets,
					targets,
					oldColName2Idx,
					newColName2Idx,
					assignedColsByTarget,
					targetBranchActivePos,
				)
			}
		} else {
			branchID, branchNode, err = builder.projectPhysicalTargetSource(
				bindCtx,
				sourceStep,
				selectNode,
				dmlCtx,
				targets[0],
				oldColName2Idx,
				newColName2Idx,
				targetBranchActivePos,
				exclusiveTargetBranches,
			)
		}
		if err != nil {
			return 0, nil, 0, err
		}
		branchIDs = append(branchIDs, branchID)
		branchNodes = append(branchNodes, branchNode)
	}

	if len(branchIDs) > 1 {
		for i := range branchIDs {
			branchSinkID := appendSinkNode(builder, bindCtx, branchIDs[i])
			if builder.preserveSinkProjection == nil {
				builder.preserveSinkProjection = make(map[int32]struct{})
			}
			builder.preserveSinkProjection[branchSinkID] = struct{}{}
			branchStep := builder.appendStep(branchSinkID)
			branchScanID := builder.appendPositionalSinkScan(bindCtx, branchStep)
			branchTag := builder.genNewBindTag()
			branchProject := make([]*plan.Expr, len(branchNodes[i].ProjectList))
			for pos, expr := range branchNodes[i].ProjectList {
				branchProject[pos] = &plan.Expr{
					Typ: expr.Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{RelPos: 0, ColPos: int32(pos)},
					},
				}
			}
			materializedNode := &plan.Node{
				NodeType:    plan.Node_PROJECT,
				Children:    []int32{branchScanID},
				ProjectList: branchProject,
				BindingTags: []int32{branchTag},
			}
			branchIDs[i] = builder.appendNode(materializedNode, bindCtx)
			branchNodes[i] = materializedNode
		}
	}

	unionID := branchIDs[0]
	unionNode := branchNodes[0]
	unionInputTag := unionNode.BindingTags[0]
	for branchIdx := 1; branchIdx < len(branchIDs); branchIdx++ {
		unionProject := make([]*plan.Expr, len(unionNode.ProjectList))
		for pos, expr := range unionNode.ProjectList {
			unionProject[pos] = &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{RelPos: unionInputTag, ColPos: int32(pos)},
				},
			}
		}
		unionTag := builder.genNewBindTag()
		unionNode = &plan.Node{
			NodeType:    plan.Node_UNION_ALL,
			Children:    []int32{unionID, branchIDs[branchIdx]},
			ProjectList: unionProject,
			BindingTags: []int32{unionTag},
		}
		unionID = builder.appendNode(unionNode, bindCtx)
		unionInputTag = unionTag
	}
	return unionID, unionNode, unionInputTag, nil
}

func (builder *QueryBuilder) appendPositionalSinkScan(bindCtx *BindContext, sourceStep int32) int32 {
	scanID := appendSinkScanNode(builder, bindCtx, sourceStep)
	if builder.preserveScanProjection == nil {
		builder.preserveScanProjection = make(map[int32]struct{})
	}
	builder.preserveScanProjection[scanID] = struct{}{}
	if builder.positionalSinkScans == nil {
		builder.positionalSinkScans = make(map[int32]struct{})
	}
	builder.positionalSinkScans[scanID] = struct{}{}
	return scanID
}

func nullUpdateProjectionExpr(typ plan.Type) *plan.Expr {
	expr := makePlan2NullConstExprWithType()
	expr.Typ = typ
	expr.Typ.NotNullable = false
	return expr
}

// projectPhysicalTargetSource isolates one physical target from the shared join
// stream. Other targets are NULL so their independent selectors ignore this
// branch after all physical-target streams are combined with UNION ALL.
func (builder *QueryBuilder) projectPhysicalTargetSource(
	bindCtx *BindContext,
	sourceStep int32,
	selectNode *plan.Node,
	dmlCtx *DMLContext,
	targetIdx int,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	targetBranchActivePos []int32,
	exclusiveTarget bool,
) (int32, *plan.Node, error) {
	scanID := builder.appendPositionalSinkScan(bindCtx, sourceStep)
	project := make([]*plan.Expr, len(selectNode.ProjectList))
	for pos, expr := range selectNode.ProjectList {
		project[pos] = &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: 0, ColPos: int32(pos)},
			},
		}
	}
	copyPos := func(pos int32) {
		project[pos] = &plan.Expr{
			Typ: selectNode.ProjectList[pos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: 0, ColPos: pos},
			},
		}
	}
	alias := dmlCtx.aliases[targetIdx]
	for _, col := range dmlCtx.tableDefs[targetIdx].Cols {
		copyPos(oldColName2Idx[alias+"."+col.Name])
		if pos, ok := newColName2Idx[alias+"."+col.Name]; ok {
			copyPos(pos)
		}
	}
	targetRowIDPos := oldColName2Idx[alias+"."+catalog.Row_ID]
	targetRowIDExpr := DeepCopyExpr(project[targetRowIDPos])
	for otherIdx, activePos := range targetBranchActivePos {
		if activePos < 0 {
			continue
		}
		if exclusiveTarget && otherIdx != targetIdx {
			project[activePos] = makePlan2BoolConstExprWithType(false)
			continue
		}
		operator := "isnull"
		if otherIdx == targetIdx {
			operator = "isnotnull"
		}
		activeExpr, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			operator,
			[]*plan.Expr{DeepCopyExpr(targetRowIDExpr)},
		)
		if err != nil {
			return 0, nil, err
		}
		if otherIdx == targetIdx {
			inputActiveExpr := &plan.Expr{
				Typ: selectNode.ProjectList[activePos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: 0,
					ColPos: activePos,
				}},
			}
			activeExpr, err = BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"and",
				[]*plan.Expr{inputActiveExpr, activeExpr},
			)
			if err != nil {
				return 0, nil, err
			}
		}
		project[activePos] = activeExpr
	}
	tag := builder.genNewBindTag()
	node := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{scanID},
		ProjectList: project,
		BindingTags: []int32{tag},
	}
	return builder.appendNode(node, bindCtx), node, nil
}

type physicalTargetContribution struct {
	targetIdx int
	colName   string
	valuePos  int32
	markerPos int32
}

type physicalTargetFiller struct {
	sourcePos    int32
	canonicalPos int32
	outputPos    []int32
}

// mergeRepeatedPhysicalTargetAssignmentsWithUniqueFallback builds one
// candidate row for every target-prefix after alias-local IGNORE checks have
// run. A later candidate carries every still-active earlier alias assignment.
// Final UNIQUE checks remove invalid combined images, and the highest surviving
// prefix is selected. This preserves safe sibling aliases without ever writing
// the same physical RowID more than once.
func (builder *QueryBuilder) mergeRepeatedPhysicalTargetAssignmentsWithUniqueFallback(
	bindCtx *BindContext,
	sourceStep int32,
	selectNode *plan.Node,
	dmlCtx *DMLContext,
	targets []int,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	assignedColsByTarget map[int]map[string]struct{},
	colOffsets []int32,
	targetBranchActivePos []int32,
) (int32, *plan.Node, error) {
	baseOldColName2Idx := maps.Clone(oldColName2Idx)
	baseNewColName2Idx := maps.Clone(newColName2Idx)
	candidateIDs := make([]int32, 0, len(targets))
	candidateNodes := make([]*plan.Node, 0, len(targets))
	for end := range targets {
		prefix := targets[:end+1]
		candidateOldColName2Idx := maps.Clone(baseOldColName2Idx)
		candidateNewColName2Idx := maps.Clone(baseNewColName2Idx)
		candidateID, candidateNode, err := builder.mergeSamePhysicalTargetAssignmentsAcrossTuples(
			bindCtx,
			sourceStep,
			selectNode,
			dmlCtx,
			targets,
			prefix,
			candidateOldColName2Idx,
			candidateNewColName2Idx,
			assignedColsByTarget,
			targetBranchActivePos,
		)
		if err != nil {
			return 0, nil, err
		}
		err = builder.recomputeMergedPhysicalTargetGeneratedColumns(
			candidateNode,
			dmlCtx,
			targets[0],
			candidateNewColName2Idx,
			colOffsets,
		)
		if err != nil {
			return 0, nil, err
		}
		candidateID, candidateNode, err = builder.appendMergedPhysicalTargetUniqueChecks(
			bindCtx,
			candidateID,
			candidateNode,
			dmlCtx,
			prefix,
			candidateOldColName2Idx,
			candidateNewColName2Idx,
			assignedColsByTarget,
		)
		if err != nil {
			return 0, nil, err
		}
		candidateTag := candidateNode.BindingTags[0]
		priorityProject := make([]*plan.Expr, 0, len(candidateNode.ProjectList)+1)
		for pos, expr := range candidateNode.ProjectList {
			priorityProject = append(priorityProject, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: candidateTag,
					ColPos: int32(pos),
				}},
			})
		}
		priorityProject = append(priorityProject, MakePlan2Int64ConstExprWithType(int64(end)))
		priorityTag := builder.genNewBindTag()
		candidateNode = &plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{candidateID},
			ProjectList: priorityProject,
			BindingTags: []int32{priorityTag},
		}
		candidateID = builder.appendNode(candidateNode, bindCtx)
		candidateIDs = append(candidateIDs, candidateID)
		candidateNodes = append(candidateNodes, candidateNode)
		if end == len(targets)-1 {
			clear(oldColName2Idx)
			maps.Copy(oldColName2Idx, candidateOldColName2Idx)
			clear(newColName2Idx)
			maps.Copy(newColName2Idx, candidateNewColName2Idx)
		}
	}

	unionID := candidateIDs[0]
	unionNode := candidateNodes[0]
	unionTag := unionNode.BindingTags[0]
	for idx := 1; idx < len(candidateIDs); idx++ {
		project := make([]*plan.Expr, len(unionNode.ProjectList))
		for pos, expr := range unionNode.ProjectList {
			project[pos] = &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: unionTag,
					ColPos: int32(pos),
				}},
			}
		}
		unionTag = builder.genNewBindTag()
		unionNode = &plan.Node{
			NodeType:    plan.Node_UNION_ALL,
			Children:    []int32{unionID, candidateIDs[idx]},
			ProjectList: project,
			BindingTags: []int32{unionTag},
		}
		unionID = builder.appendNode(unionNode, bindCtx)
	}

	ownerAlias := dmlCtx.aliases[targets[0]]
	rowIDPos := oldColName2Idx[ownerAlias+"."+catalog.Row_ID]
	priorityPos := int32(len(unionNode.ProjectList) - 1)
	return builder.selectHighestPhysicalTargetCandidate(
		bindCtx, unionID, unionNode, unionTag, rowIDPos, priorityPos)
}

func (builder *QueryBuilder) recomputeMergedPhysicalTargetGeneratedColumns(
	selectNode *plan.Node,
	dmlCtx *DMLContext,
	ownerIdx int,
	newColName2Idx map[string]int32,
	colOffsets []int32,
) error {
	tableDef := dmlCtx.tableDefs[ownerIdx]
	ownerAlias := dmlCtx.aliases[ownerIdx]
	for _, col := range tableDef.Cols {
		if col.GeneratedCol == nil {
			continue
		}
		generatedPos, ok := newColName2Idx[ownerAlias+"."+col.Name]
		if !ok || generatedPos < 0 || int(generatedPos) >= len(selectNode.ProjectList) {
			return moerr.NewInternalErrorf(
				builder.GetContext(),
				"bind update err, can not find generated column %s for target %s",
				col.Name,
				ownerAlias,
			)
		}
		genExpr := builder.applyGeneratedColumnAssignmentCast(
			DeepCopyExpr(col.GeneratedCol.Expr),
			true,
		)
		selectNode.ProjectList[generatedPos] = substituteColRefsInExpr(
			genExpr,
			selectNode.ProjectList,
			colOffsets[ownerIdx],
		)
	}
	return nil
}

func (builder *QueryBuilder) appendMergedPhysicalTargetUniqueChecks(
	bindCtx *BindContext,
	lastNodeID int32,
	selectNode *plan.Node,
	dmlCtx *DMLContext,
	targets []int,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	assignedColsByTarget map[int]map[string]struct{},
) (int32, *plan.Node, error) {
	ownerIdx := targets[0]
	tableDef := dmlCtx.tableDefs[ownerIdx]
	ownerAlias := dmlCtx.aliases[ownerIdx]
	baseWidth := len(selectNode.ProjectList)
	updatedCols := make(map[string]struct{})
	for _, targetIdx := range targets {
		for colName := range assignedColsByTarget[targetIdx] {
			updatedCols[colName] = struct{}{}
		}
	}
	selectNodeTag := selectNode.BindingTags[0]

	for _, idxDef := range tableDef.Indexes {
		if !idxDef.Unique {
			continue
		}

		idxObjRef, idxTableDef, err := builder.compCtx.ResolveIndexTableByRef(
			dmlCtx.objRefs[ownerIdx], idxDef.IndexTableName, bindCtx.snapshot)
		if err != nil {
			return 0, nil, err
		}
		prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
		if err != nil {
			return 0, nil, err
		}
		makePartExpr := func(pos int32, partName string) (*plan.Expr, error) {
			input := &plan.Expr{
				Typ: selectNode.ProjectList[pos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: selectNodeTag,
					ColPos: pos,
					Name:   partName,
				}},
			}
			return builder.makeIndexPartExprFromInputExpr(input, partName, prefixLengths)
		}
		oldParts := make([]*plan.Expr, len(idxDef.Parts))
		newParts := make([]*plan.Expr, len(idxDef.Parts))
		for partPos, rawPart := range idxDef.Parts {
			part := catalog.ResolveAlias(rawPart)
			oldPos, ok := oldColName2Idx[ownerAlias+"."+part]
			if !ok {
				return 0, nil, moerr.NewInternalErrorf(
					builder.GetContext(), "bind update err, can not find colName = %s", part)
			}
			newPos := oldPos
			if _, assigned := updatedCols[part]; assigned {
				if pos, exists := newColName2Idx[ownerAlias+"."+part]; exists {
					newPos = pos
				}
			}
			oldParts[partPos], err = makePartExpr(oldPos, part)
			if err != nil {
				return 0, nil, err
			}
			newParts[partPos], err = makePartExpr(newPos, part)
			if err != nil {
				return 0, nil, err
			}
		}
		oldKey := oldParts[0]
		newKey := newParts[0]
		if len(idxDef.Parts) > 1 {
			oldKey, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", oldParts)
			if err != nil {
				return 0, nil, err
			}
			newKey, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", newParts)
			if err != nil {
				return 0, nil, err
			}
		}
		project := make([]*plan.Expr, 0, len(selectNode.ProjectList)+2)
		for pos, expr := range selectNode.ProjectList {
			project = append(project, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: selectNodeTag,
					ColPos: int32(pos),
				}},
			})
		}
		oldKeyPos := int32(len(project))
		project = append(project, oldKey)
		newKeyPos := int32(len(project))
		project = append(project, newKey)
		projectTag := builder.genNewBindTag()
		projectNode := &plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{lastNodeID},
			ProjectList: project,
			BindingTags: []int32{projectTag},
		}
		lastNodeID = builder.appendNode(projectNode, bindCtx)

		idxTag := builder.genNewBindTag()
		idxScanID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     idxTableDef,
			ObjRef:       idxObjRef,
			BindingTags:  []int32{idxTag},
			ScanSnapshot: bindCtx.snapshot,
		}, bindCtx)
		idxKeyPos := idxTableDef.Name2ColIndex[catalog.IndexTableIndexColName]
		left := &plan.Expr{
			Typ: idxTableDef.Cols[idxKeyPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: idxTag,
				ColPos: idxKeyPos,
			}},
		}
		right := &plan.Expr{
			Typ: projectNode.ProjectList[newKeyPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: projectTag,
				ColPos: newKeyPos,
			}},
		}
		joinCond, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "=", []*plan.Expr{left, right})
		if err != nil {
			return 0, nil, err
		}
		dedupTypes := make([]plan.Type, len(idxDef.Parts))
		for partPos, part := range idxDef.Parts {
			dedupTypes[partPos] = tableDef.Cols[tableDef.Name2ColIndex[catalog.ResolveAlias(part)]].Typ
		}
		dedupName := catalog.ResolveAlias(idxDef.Parts[0])
		if len(idxDef.Parts) > 1 {
			dedupName = "(" + strings.Join(idxDef.Parts, ",") + ")"
		}
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:          plan.Node_JOIN,
			Children:          []int32{idxScanID, lastNodeID},
			JoinType:          plan.Node_DEDUP,
			OnList:            []*plan.Expr{joinCond},
			OnDuplicateAction: plan.Node_IGNORE,
			DedupColName:      dedupName,
			DedupColTypes:     dedupTypes,
			DedupJoinCtx: &plan.DedupJoinCtx{OldColList: []plan.ColRef{{
				RelPos: projectTag,
				ColPos: oldKeyPos,
			}}},
		}, bindCtx)
		selectNode = projectNode
		selectNodeTag = projectTag
	}

	if len(selectNode.ProjectList) == 0 {
		return lastNodeID, selectNode, nil
	}
	output := make([]*plan.Expr, baseWidth)
	for pos := range output {
		expr := selectNode.ProjectList[pos]
		output[pos] = &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: selectNodeTag,
				ColPos: int32(pos),
			}},
		}
	}
	outputTag := builder.genNewBindTag()
	outputNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		ProjectList: output,
		BindingTags: []int32{outputTag},
	}
	return builder.appendNode(outputNode, bindCtx), outputNode, nil
}

func (builder *QueryBuilder) selectHighestPhysicalTargetCandidate(
	bindCtx *BindContext,
	lastNodeID int32,
	selectNode *plan.Node,
	selectNodeTag int32,
	rowIDPos int32,
	priorityPos int32,
) (int32, *plan.Node, error) {
	colExpr := func(pos int32) *plan.Expr {
		return &plan.Expr{
			Typ: selectNode.ProjectList[pos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: selectNodeTag,
				ColPos: pos,
			}},
		}
	}
	rowIDExpr := colExpr(rowIDPos)
	priorityExpr := colExpr(priorityPos)
	windowTag := builder.genNewBindTag()
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType: plan.Node_PARTITION,
		Children: []int32{lastNodeID},
		OrderBy: []*plan.OrderBySpec{
			{Expr: DeepCopyExpr(rowIDExpr), Flag: plan.OrderBySpec_INTERNAL},
			{Expr: DeepCopyExpr(priorityExpr), Flag: plan.OrderBySpec_DESC | plan.OrderBySpec_INTERNAL},
		},
		BindingTags: []int32{windowTag},
	}, bindCtx)
	rowNumberFunc, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "row_number", nil)
	if err != nil {
		return 0, nil, err
	}
	rowNumberExpr := &plan.Expr{
		Typ: rowNumberFunc.Typ,
		Expr: &plan.Expr_W{W: &plan.WindowSpec{
			WindowFunc: rowNumberFunc,
			Name:       "row_number",
			PartitionBy: []*plan.Expr{
				DeepCopyExpr(rowIDExpr),
			},
			OrderBy: []*plan.OrderBySpec{{
				Expr: DeepCopyExpr(priorityExpr),
				Flag: plan.OrderBySpec_DESC | plan.OrderBySpec_INTERNAL,
			}},
			Frame: &plan.FrameClause{
				Type:  plan.FrameClause_ROWS,
				Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
				End:   &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, UnBounded: true},
			},
		}},
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_WINDOW,
		Children:    []int32{lastNodeID},
		WinSpecList: []*plan.Expr{rowNumberExpr},
		WindowIdx:   0,
		BindingTags: []int32{windowTag},
	}, bindCtx)
	rowNumberRef := &plan.Expr{
		Typ: rowNumberFunc.Typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: windowTag,
			ColPos: 0,
		}},
	}
	selected, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(), "=", []*plan.Expr{rowNumberRef, MakePlan2Int64ConstExprWithType(1)})
	if err != nil {
		return 0, nil, err
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{lastNodeID},
		FilterList: []*plan.Expr{selected},
	}, bindCtx)
	project := make([]*plan.Expr, priorityPos)
	for pos := range project {
		project[pos] = colExpr(int32(pos))
	}
	projectTag := builder.genNewBindTag()
	projectNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		ProjectList: project,
		BindingTags: []int32{projectTag},
	}
	return builder.appendNode(projectNode, bindCtx), projectNode, nil
}

// mergeSamePhysicalTargetAssignmentsAcrossTuples first selects one complete
// source tuple per alias and target Rowid. Only then does it combine assignments
// made through different aliases, so multiple SET expressions from one alias
// can never be assembled from different source tuples.
func (builder *QueryBuilder) mergeSamePhysicalTargetAssignmentsAcrossTuples(
	bindCtx *BindContext,
	sourceStep int32,
	selectNode *plan.Node,
	dmlCtx *DMLContext,
	targets []int,
	sourceTargets []int,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	assignedColsByTarget map[int]map[string]struct{},
	targetBranchActivePos []int32,
) (int32, *plan.Node, error) {
	tableDef := dmlCtx.tableDefs[targets[0]]
	updatedColsSet := make(map[string]struct{})
	var contributions []physicalTargetContribution
	for _, targetIdx := range targets {
		for colName := range assignedColsByTarget[targetIdx] {
			updatedColsSet[colName] = struct{}{}
			contributions = append(contributions, physicalTargetContribution{
				targetIdx: targetIdx,
				colName:   colName,
			})
		}
	}
	sort.SliceStable(contributions, func(i, j int) bool {
		if contributions[i].targetIdx != contributions[j].targetIdx {
			return contributions[i].targetIdx < contributions[j].targetIdx
		}
		return contributions[i].colName < contributions[j].colName
	})
	updatedCols := make([]string, 0, len(updatedColsSet))
	for colName := range updatedColsSet {
		updatedCols = append(updatedCols, colName)
	}
	sort.Strings(updatedCols)

	// Canonical layout: old physical row, followed by a value and an explicit
	// assignment marker for each alias/column contribution.
	canonicalTypes := make([]plan.Type, 0, len(tableDef.Cols)+2*len(contributions))
	for _, col := range tableDef.Cols {
		canonicalTypes = append(canonicalTypes, col.Typ)
	}
	for i := range contributions {
		colIdx := tableDef.Name2ColIndex[contributions[i].colName]
		contributions[i].valuePos = int32(len(canonicalTypes))
		canonicalTypes = append(canonicalTypes, tableDef.Cols[colIdx].Typ)
		contributions[i].markerPos = int32(len(canonicalTypes))
		canonicalTypes = append(canonicalTypes, plan.Type{Id: int32(types.T_bool)})
	}
	currentTableID := tableDef.TblId
	fillerByTableCol := make(map[string]*physicalTargetFiller)
	var fillers []*physicalTargetFiller
	for targetIdx, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) == 0 || dmlCtx.tableDefs[targetIdx].TblId == currentTableID {
			continue
		}
		otherDef := dmlCtx.tableDefs[targetIdx]
		otherAlias := dmlCtx.aliases[targetIdx]
		for _, col := range otherDef.Cols {
			fillerKey := fmt.Sprintf("%d.%s", otherDef.TblId, col.Name)
			filler := fillerByTableCol[fillerKey]
			if filler == nil {
				filler = &physicalTargetFiller{
					sourcePos:    oldColName2Idx[otherAlias+"."+col.Name],
					canonicalPos: int32(len(canonicalTypes)),
				}
				fillerByTableCol[fillerKey] = filler
				fillers = append(fillers, filler)
				canonicalTypes = append(canonicalTypes, col.Typ)
			}
			filler.outputPos = append(filler.outputPos, oldColName2Idx[otherAlias+"."+col.Name])
			if newPos, ok := newColName2Idx[otherAlias+"."+col.Name]; ok {
				filler.outputPos = append(filler.outputPos, newPos)
			}
		}
	}
	branchIDs := make([]int32, 0, len(targets))
	branchTags := make([]int32, 0, len(targets))
	for _, sourceIdx := range sourceTargets {
		sourceAlias := dmlCtx.aliases[sourceIdx]
		branchScanID := builder.appendPositionalSinkScan(bindCtx, sourceStep)
		branchScanTag := builder.genNewBindTag()
		builder.qry.Nodes[branchScanID].BindingTags = []int32{branchScanTag}
		activePos := targetBranchActivePos[sourceIdx]
		activeExpr := &plan.Expr{
			Typ: selectNode.ProjectList[activePos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: branchScanTag,
				ColPos: activePos,
			}},
		}
		branchScanID = builder.appendNode(&plan.Node{
			NodeType:   plan.Node_FILTER,
			Children:   []int32{branchScanID},
			FilterList: []*plan.Expr{activeExpr},
		}, bindCtx)
		rowIDPos := oldColName2Idx[sourceAlias+"."+catalog.Row_ID]
		dedupInputProject := make([]*plan.Expr, 0, len(selectNode.ProjectList)+1)
		for pos, expr := range selectNode.ProjectList {
			dedupInputProject = append(dedupInputProject, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{RelPos: branchScanTag, ColPos: int32(pos)},
				},
			})
		}
		rowIDColExpr := &plan.Expr{
			Typ: selectNode.ProjectList[rowIDPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: branchScanTag, ColPos: rowIDPos},
			},
		}
		rowIDPartitionExpr, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"greatest",
			[]*plan.Expr{rowIDColExpr, DeepCopyExpr(rowIDColExpr)},
		)
		if err != nil {
			return 0, nil, err
		}
		dedupInputProject = append(dedupInputProject, rowIDPartitionExpr)
		dedupInputTag := builder.genNewBindTag()
		dedupInputNode := &plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{branchScanID},
			ProjectList: dedupInputProject,
			BindingTags: []int32{dedupInputTag},
		}
		dedupInputID := builder.appendNode(dedupInputNode, bindCtx)
		dedupID, dedupNode, dedupTag, err := builder.appendRowNumberDedupNode(
			bindCtx,
			dedupInputID,
			dedupInputNode,
			dedupInputTag,
			[]int32{int32(len(selectNode.ProjectList))},
		)
		if err != nil {
			return 0, nil, err
		}
		branchTag := builder.genNewBindTag()
		branchColExpr := func(pos int32) *plan.Expr {
			return &plan.Expr{
				Typ: dedupNode.ProjectList[pos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{RelPos: dedupTag, ColPos: pos},
				},
			}
		}
		branchProject := make([]*plan.Expr, 0, len(canonicalTypes))
		for _, col := range tableDef.Cols {
			sourcePos := oldColName2Idx[sourceAlias+"."+col.Name]
			branchProject = append(
				branchProject,
				branchColExpr(sourcePos),
			)
		}
		for _, contribution := range contributions {
			if contribution.targetIdx == sourceIdx {
				branchProject = append(
					branchProject,
					branchColExpr(newColName2Idx[sourceAlias+"."+contribution.colName]),
					makePlan2BoolConstExprWithType(true),
				)
			} else {
				colIdx := tableDef.Name2ColIndex[contribution.colName]
				branchProject = append(
					branchProject,
					nullUpdateProjectionExpr(tableDef.Cols[colIdx].Typ),
					nullUpdateProjectionExpr(plan.Type{Id: int32(types.T_bool)}),
				)
			}
		}
		for _, filler := range fillers {
			if sourceIdx == targets[0] {
				branchProject = append(branchProject, branchColExpr(filler.sourcePos))
			} else {
				branchProject = append(
					branchProject,
					nullUpdateProjectionExpr(canonicalTypes[filler.canonicalPos]),
				)
			}
		}
		branchIDs = append(branchIDs, builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{dedupID},
			ProjectList: branchProject,
			BindingTags: []int32{branchTag},
		}, bindCtx))
		branchTags = append(branchTags, branchTag)
	}

	unionID := branchIDs[0]
	unionInputTag := branchTags[0]
	for branchIdx := 1; branchIdx < len(branchIDs); branchIdx++ {
		unionProject := make([]*plan.Expr, len(canonicalTypes))
		for pos, typ := range canonicalTypes {
			unionProject[pos] = &plan.Expr{
				Typ: typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{RelPos: unionInputTag, ColPos: int32(pos)},
				},
			}
		}
		unionTag := builder.genNewBindTag()
		unionID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_UNION_ALL,
			Children:    []int32{unionID, branchIDs[branchIdx]},
			ProjectList: unionProject,
			BindingTags: []int32{unionTag},
		}, bindCtx)
		unionInputTag = unionTag
	}
	unionTag := unionInputTag

	rowIDCanonicalPos := int32(tableDef.Name2ColIndex[catalog.Row_ID])
	groupExpr := &plan.Expr{
		Typ: canonicalTypes[rowIDCanonicalPos],
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: unionTag, ColPos: rowIDCanonicalPos},
		},
	}
	groupTag := builder.genNewBindTag()
	aggTag := builder.genNewBindTag()
	aggList := make([]*plan.Expr, 0, len(canonicalTypes)-1)
	canonicalAggPos := make([]int32, len(canonicalTypes))
	for pos, typ := range canonicalTypes {
		if int32(pos) == rowIDCanonicalPos {
			canonicalAggPos[pos] = -1
			continue
		}
		input := &plan.Expr{
			Typ: typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: unionTag, ColPos: int32(pos)},
			},
		}
		aggExpr, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"any_value",
			[]*plan.Expr{input},
		)
		if err != nil {
			return 0, nil, err
		}
		canonicalAggPos[pos] = int32(len(aggList))
		aggList = append(aggList, aggExpr)
	}
	aggID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_AGG,
		Children:    []int32{unionID},
		GroupBy:     []*plan.Expr{groupExpr},
		AggList:     aggList,
		BindingTags: []int32{groupTag, aggTag},
		SpillMem:    builder.aggSpillMem,
	}, bindCtx)

	canonicalResultExpr := func(pos int32) *plan.Expr {
		typ := canonicalTypes[pos]
		if pos == rowIDCanonicalPos {
			return &plan.Expr{
				Typ: typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{RelPos: groupTag, ColPos: 0},
				},
			}
		}
		return &plan.Expr{
			Typ: typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: aggTag, ColPos: canonicalAggPos[pos]},
			},
		}
	}

	finalProject := make([]*plan.Expr, len(selectNode.ProjectList))
	for pos, expr := range selectNode.ProjectList {
		finalProject[pos] = nullUpdateProjectionExpr(expr.Typ)
	}
	for _, filler := range fillers {
		for _, outputPos := range filler.outputPos {
			finalProject[outputPos] = canonicalResultExpr(filler.canonicalPos)
		}
	}
	rowIDExpr := canonicalResultExpr(rowIDCanonicalPos)
	activeMarkerByTarget := make(map[int]int32, len(targets))
	for _, contribution := range contributions {
		if _, ok := activeMarkerByTarget[contribution.targetIdx]; !ok {
			activeMarkerByTarget[contribution.targetIdx] = contribution.markerPos
		}
	}
	for targetIdx, activePos := range targetBranchActivePos {
		if activePos < 0 {
			continue
		}
		activeInput := DeepCopyExpr(rowIDExpr)
		operator := "isnull"
		if markerPos, ok := activeMarkerByTarget[targetIdx]; ok {
			activeInput = canonicalResultExpr(markerPos)
			operator = "isnotnull"
		}
		activeExpr, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			operator,
			[]*plan.Expr{activeInput},
		)
		if err != nil {
			return 0, nil, err
		}
		finalProject[activePos] = activeExpr
	}
	finalUpdatedExprs := make(map[string]*plan.Expr, len(updatedCols))
	for _, colName := range updatedCols {
		finalExpr := canonicalResultExpr(int32(tableDef.Name2ColIndex[colName]))
		for _, contribution := range contributions {
			if contribution.colName != colName {
				continue
			}
			var err error
			finalExpr, err = BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"if",
				[]*plan.Expr{
					canonicalResultExpr(contribution.markerPos),
					canonicalResultExpr(contribution.valuePos),
					finalExpr,
				},
			)
			if err != nil {
				return 0, nil, err
			}
		}
		finalUpdatedExprs[colName] = finalExpr
	}
	for _, targetIdx := range targets {
		alias := dmlCtx.aliases[targetIdx]
		for colPos, col := range tableDef.Cols {
			finalProject[oldColName2Idx[alias+"."+col.Name]] =
				canonicalResultExpr(int32(colPos))
		}
		for _, colName := range updatedCols {
			key := alias + "." + colName
			targetPos, exists := newColName2Idx[key]
			if !exists {
				oldPos := oldColName2Idx[key]
				oldColName2Idx[key] = int32(len(finalProject))
				finalProject = append(finalProject, canonicalResultExpr(int32(tableDef.Name2ColIndex[colName])))
				targetPos = oldPos
				newColName2Idx[key] = targetPos
			}
			finalProject[targetPos] = DeepCopyExpr(finalUpdatedExprs[colName])
		}
	}

	projectTag := builder.genNewBindTag()
	projectNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{aggID},
		ProjectList: finalProject,
		BindingTags: []int32{projectTag},
	}
	projectID := builder.appendNode(projectNode, bindCtx)
	return projectID, projectNode, nil
}

func buildIrregularUpdateTargetProjection(
	alias string,
	tableDef *plan.TableDef,
	finalProjTag int32,
	finalProjList []*plan.Expr,
	finalColName2Idx map[string]int32,
	oldPkGlobalPos int32,
) ([]*plan.Expr, int32) {
	localProjList := make([]*plan.Expr, 0, len(tableDef.Cols)+1)
	for _, colDef := range tableDef.Cols {
		globalPos := finalColName2Idx[alias+"."+colDef.Name]
		localProjList = append(localProjList, &plan.Expr{
			Typ: finalProjList[globalPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: finalProjTag,
				ColPos: globalPos,
			}},
		})
	}

	newPkGlobalPos := finalColName2Idx[alias+"."+tableDef.Pkey.PkeyColName]
	deletePkPos := int32(tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName])
	if oldPkGlobalPos != newPkGlobalPos {
		deletePkPos = int32(len(localProjList))
		localProjList = append(localProjList, &plan.Expr{
			Typ: finalProjList[oldPkGlobalPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: finalProjTag,
				ColPos: oldPkGlobalPos,
			}},
		})
	}
	return localProjList, deletePkPos
}

// appendTargetRowNumberBelowAssignmentProject places the target-local
// row_number window below the SELECT projection that evaluates SET expressions.
// The projection can then use the window result as a lazy IF condition, so an
// inactive or deduplicated candidate never evaluates its target-local value.
func (builder *QueryBuilder) appendTargetRowNumberBelowAssignmentProject(
	bindCtx *BindContext,
	lastNodeID int32,
	selectNode *plan.Node,
	targetRowIDPos int32,
) (int32, int32, error) {
	if selectNode.NodeType != plan.Node_PROJECT || len(selectNode.Children) != 1 {
		return 0, 0, moerr.NewInternalError(
			builder.GetContext(),
			"multi-target UPDATE assignment input must be a single-child project",
		)
	}
	targetRowIDExpr := DeepCopyExpr(selectNode.ProjectList[targetRowIDPos])
	targetActiveExpr, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(),
		"isnotnull",
		[]*plan.Expr{DeepCopyExpr(targetRowIDExpr)},
	)
	if err != nil {
		return 0, 0, err
	}
	partitionByExprs := []*plan.Expr{
		DeepCopyExpr(targetActiveExpr),
		DeepCopyExpr(targetRowIDExpr),
	}
	partitionBy := make([]*plan.OrderBySpec, 0, len(partitionByExprs))
	for _, expr := range partitionByExprs {
		partitionBy = append(partitionBy, &plan.OrderBySpec{
			Expr: expr,
			Flag: plan.OrderBySpec_INTERNAL,
		})
	}
	windowTag := builder.genNewBindTag()
	partitionID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PARTITION,
		Children:    []int32{selectNode.Children[0]},
		OrderBy:     partitionBy,
		BindingTags: []int32{windowTag},
	}, bindCtx)

	rowNumberFunc, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "row_number", nil)
	if err != nil {
		return 0, 0, err
	}
	rowNumberExpr := &plan.Expr{
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
	}
	windowID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_WINDOW,
		Children:    []int32{partitionID},
		WinSpecList: []*plan.Expr{rowNumberExpr},
		WindowIdx:   0,
		BindingTags: []int32{windowTag},
	}, bindCtx)
	selectNode.Children[0] = windowID

	rowNumberPos := int32(len(selectNode.ProjectList))
	selectNode.ProjectList = append(selectNode.ProjectList, &plan.Expr{
		Typ: rowNumberFunc.Typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: windowTag,
			ColPos: 0,
			Name:   "__mo_multi_target_update_row_number",
		}},
	})
	return lastNodeID, rowNumberPos, nil
}

func (builder *QueryBuilder) buildTargetSelectedBelowAssignmentProject(
	selectNode *plan.Node,
	targetRowIDPos int32,
	targetRowNumberPos int32,
) (*plan.Expr, error) {
	targetActive, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(),
		"isnotnull",
		[]*plan.Expr{DeepCopyExpr(selectNode.ProjectList[targetRowIDPos])},
	)
	if err != nil || targetRowNumberPos < 0 {
		return targetActive, err
	}
	selected, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(),
		"=",
		[]*plan.Expr{
			DeepCopyExpr(selectNode.ProjectList[targetRowNumberPos]),
			MakePlan2Int64ConstExprWithType(1),
		},
	)
	if err != nil {
		return nil, err
	}
	return BindFuncExprImplByPlanExpr(
		builder.GetContext(),
		"and",
		[]*plan.Expr{selected, targetActive},
	)
}

func (builder *QueryBuilder) guardTargetLocalExpr(
	targetSelected *plan.Expr,
	newExpr *plan.Expr,
	oldExpr *plan.Expr,
) (*plan.Expr, error) {
	return BindFuncExprImplByPlanExpr(
		builder.GetContext(),
		"if",
		[]*plan.Expr{DeepCopyExpr(targetSelected), newExpr, DeepCopyExpr(oldExpr)},
	)
}

func (builder *QueryBuilder) appendSelectedTargetNotNullAssertions(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	lastNodeID int32,
	selectNodeTag int32,
	selectNode *plan.Node,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	targetRowNumberPos []int32,
	targetActivePos []int32,
) (int32, error) {
	for i, alias := range dmlCtx.aliases {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}
		var targetSelected *plan.Expr
		var err error
		if targetRowNumberPos[i] >= 0 {
			targetSelected, err = builder.buildTargetSelectedExpr(
				selectNodeTag,
				selectNode,
				targetRowNumberPos[i],
				targetActivePos[i],
			)
		} else {
			rowIDPos, ok := oldColName2Idx[alias+"."+catalog.Row_ID]
			if !ok {
				return 0, moerr.NewInternalErrorf(
					builder.GetContext(),
					"bind update err, can not find row_id for target %s",
					alias,
				)
			}
			targetSelected, err = BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"isnotnull",
				[]*plan.Expr{{
					Typ: selectNode.ProjectList[rowIDPos].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: rowIDPos,
					}},
				}},
			)
		}
		if err != nil {
			return 0, err
		}

		assertions := make([]*plan.Expr, 0)
		for _, col := range dmlCtx.tableDefs[i].Cols {
			if col.Default == nil || col.Default.NullAbility {
				continue
			}
			newPos, updated := newColName2Idx[alias+"."+col.Name]
			if !updated {
				continue
			}
			isNotNull, buildErr := BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"isnotnull",
				[]*plan.Expr{{
					Typ: selectNode.ProjectList[newPos].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: newPos,
					}},
				}},
			)
			if buildErr != nil {
				return 0, buildErr
			}
			notSelected, buildErr := BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"not",
				[]*plan.Expr{DeepCopyExpr(targetSelected)},
			)
			if buildErr != nil {
				return 0, buildErr
			}
			pass, buildErr := BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"or",
				[]*plan.Expr{notSelected, isNotNull},
			)
			if buildErr != nil {
				return 0, buildErr
			}
			assertion, buildErr := BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"_check_constraint_assert",
				[]*plan.Expr{
					pass,
					makePlan2StringConstExprWithType(fmt.Sprintf("Column '%s' cannot be null", col.Name)),
				},
			)
			if buildErr != nil {
				return 0, buildErr
			}
			assertions = append(assertions, assertion)
		}
		if len(assertions) > 0 {
			lastNodeID = builder.appendNode(&plan.Node{
				NodeType:    plan.Node_ASSERT,
				Children:    []int32{lastNodeID},
				FilterList:  assertions,
				ProjectList: getProjectionByLastNodeIfAvailable(builder, lastNodeID),
			}, bindCtx)
		}
	}
	return lastNodeID, nil
}

func irregularIndexAffectedByUpdate(
	tableDef *plan.TableDef,
	idxDef *plan.IndexDef,
	updateCols map[string]tree.Expr,
) (bool, error) {
	columnUpdated := func(colName string) bool {
		colName = catalog.ResolveAlias(colName)
		if _, ok := updateCols[colName]; ok {
			return true
		}
		if tableDef == nil {
			return false
		}
		colPos, ok := tableDef.Name2ColIndex[colName]
		return ok && colPos >= 0 && int(colPos) < len(tableDef.Cols) && tableDef.Cols[colPos].OnUpdate != nil
	}

	for _, part := range idxDef.Parts {
		if columnUpdated(part) {
			return true, nil
		}
	}

	p, ok := indexplugin.Get(idxDef.IndexAlgo)
	if !ok {
		for _, colName := range indexDefIncludedColumnsBestEffort(idxDef) {
			if columnUpdated(colName) {
				return true, nil
			}
		}
		return false, nil
	}
	rewriteHook, ok := p.Plan().(planplugin.UpdateColumnRewriteHook)
	if !ok {
		return false, nil
	}
	affectedCols := make(map[string]struct{}, len(updateCols))
	for colName := range updateCols {
		affectedCols[colName] = struct{}{}
	}
	if tableDef != nil {
		for _, col := range tableDef.Cols {
			if col.OnUpdate != nil {
				affectedCols[col.Name] = struct{}{}
			}
		}
	}
	for colName := range affectedCols {
		affected, err := rewriteHook.UpdateColumnRequiresIndexRewrite(tableDef, idxDef, colName)
		if err != nil {
			return false, err
		}
		if affected {
			return true, nil
		}
	}
	return false, nil
}

// classifyIrregularIndexesForUpdate separates synchronous inline maintenance
// from CDC-only indexes using plugin metadata. MASTER has no plugin, but shares
// the modern synchronous maintenance pipeline with the plugin-backed indexes.
// The bool return preserves the legacy route only for an affected irregular
// algorithm that has not migrated to either mechanism. MASTER indexes delete
// by the old source PK and rebuild from the final row image, so changing the
// base-table PK is handled by the same maintenance pipeline. Plugin-backed
// synchronous full-text/vector indexes retain their existing PK-update
// restriction until their complete hidden-table groups support that contract.
func classifyIrregularIndexesForUpdate(
	ctx context.Context,
	tableDef *plan.TableDef,
	updateCols map[string]tree.Expr,
) (inline []*plan.IndexDef, legacyRoute bool, err error) {
	if tableDef == nil || len(updateCols) == 0 {
		return nil, false, nil
	}

	pkUpdated := primaryKeyUpdated(tableDef, updateCols)
	affectedSyncGroups := make(map[string]bool)
	for _, idxDef := range tableDef.Indexes {
		if catalog.IsRegularIndexAlgo(idxDef.IndexAlgo) {
			continue
		}
		affected, err := irregularIndexAffectedByUpdate(tableDef, idxDef, updateCols)
		if err != nil {
			return nil, false, err
		}

		p, ok := indexplugin.Get(idxDef.IndexAlgo)
		if !ok {
			if !isModernMaintainedIrregularAlgo(idxDef.IndexAlgo) {
				if affected || pkUpdated {
					return nil, true, nil
				}
				continue
			}
			if affected || pkUpdated {
				affectedSyncGroups[idxDef.IndexName+"\x00"+idxDef.IndexTableName] = true
			}
			continue
		}
		desc := p.Catalog().SyncDescriptor()
		if desc.AlwaysAsync {
			continue
		}
		async, err := catalog.IndexParamAsync(idxDef.IndexAlgoParams)
		if err != nil {
			return nil, false, err
		}
		if async {
			continue
		}
		if pkUpdated {
			return nil, false, newUpdatePlannerRouteError(
				updatePlannerRejected,
				updateRouteReasonIrregularIndex,
				moerr.NewUnsupportedDML(
					ctx,
					"update primary key on a table with a synchronous full-text/vector index"),
			)
		}
		if affected {
			affectedSyncGroups[idxDef.IndexName+"\x00"+idxDef.IndexTableName] = true
		}
	}

	for _, idxDef := range tableDef.Indexes {
		if !idxDef.TableExist || !affectedSyncGroups[idxDef.IndexName+"\x00"+idxDef.IndexTableName] {
			continue
		}
		inline = append(inline, idxDef)
	}
	return inline, false, nil
}

func updateHasMultipleSourceTables(stmt *tree.Update) bool {
	if stmt.From != nil && len(stmt.From.Tables) > 0 {
		return true
	}
	if len(stmt.Tables) > 1 {
		return true
	}
	return len(stmt.Tables) == 1 && tableExprContainsJoin(stmt.Tables[0])
}

func primaryKeyUpdated(tableDef *plan.TableDef, updateCols map[string]tree.Expr) bool {
	if tableDef == nil || tableDef.Pkey == nil || len(updateCols) == 0 {
		return false
	}
	for _, colName := range tableDef.Pkey.Names {
		if _, ok := updateCols[catalog.ResolveAlias(colName)]; ok {
			return true
		}
	}
	if tableDef.Pkey.PkeyColName != "" && tableDef.Pkey.PkeyColName != catalog.CPrimaryKeyColName {
		_, ok := updateCols[catalog.ResolveAlias(tableDef.Pkey.PkeyColName)]
		return ok
	}
	return false
}

// appendTargetRowNumberNode appends an independent row_number() window for one
// updated target. Unlike appendRowNumberDedupNode, it deliberately keeps every
// input row: each MULTI_UPDATE context later consumes only its own row_number=1
// rows. This is required for multi-target UPDATE because one shared global
// filter cannot independently choose representatives for two different Rowids.
func (builder *QueryBuilder) appendTargetRowNumberNode(
	bindCtx *BindContext,
	lastNodeID int32,
	selectNode *plan.Node,
	selectNodeTag int32,
	targetRowIDPos int32,
	targetActivePos int32,
) (int32, *plan.Node, int32, int32, error) {
	childColExpr := func(pos int32) *plan.Expr {
		e := selectNode.ProjectList[pos]
		name := ""
		if col, ok := e.Expr.(*plan.Expr_Col); ok {
			name = col.Col.Name
		}
		return &plan.Expr{
			Typ: e.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: selectNodeTag,
					ColPos: pos,
					Name:   name,
				},
			},
		}
	}

	targetRowIDExpr := childColExpr(targetRowIDPos)
	targetActiveExpr := childColExpr(targetActivePos)
	partitionBy := []*plan.OrderBySpec{
		{
			Expr: DeepCopyExpr(targetActiveExpr),
			Flag: plan.OrderBySpec_INTERNAL,
		},
		{
			Expr: DeepCopyExpr(targetRowIDExpr),
			Flag: plan.OrderBySpec_INTERNAL,
		},
	}
	windowTag := builder.genNewBindTag()
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PARTITION,
		Children:    []int32{lastNodeID},
		OrderBy:     partitionBy,
		BindingTags: []int32{windowTag},
	}, bindCtx)

	rowNumberFunc, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "row_number", nil)
	if err != nil {
		return 0, nil, 0, 0, err
	}
	rowNumberExpr := &plan.Expr{
		Typ: rowNumberFunc.Typ,
		Expr: &plan.Expr_W{
			W: &plan.WindowSpec{
				WindowFunc: rowNumberFunc,
				Name:       "row_number",
				PartitionBy: []*plan.Expr{
					DeepCopyExpr(targetActiveExpr),
					DeepCopyExpr(targetRowIDExpr),
				},
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
			},
		},
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_WINDOW,
		Children:    []int32{lastNodeID},
		WinSpecList: []*plan.Expr{rowNumberExpr},
		WindowIdx:   0,
		BindingTags: []int32{windowTag},
	}, bindCtx)

	rowNumberProjectPos := int32(len(selectNode.ProjectList))
	projectTag := builder.genNewBindTag()
	projectList := make([]*plan.Expr, 0, len(selectNode.ProjectList)+1)
	for pos := range selectNode.ProjectList {
		projectList = append(projectList, childColExpr(int32(pos)))
	}
	projectList = append(projectList, &plan.Expr{
		Typ: rowNumberFunc.Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: windowTag,
				ColPos: 0,
				Name:   "__mo_multi_target_update_row_number",
			},
		},
	})
	projectNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		ProjectList: projectList,
		BindingTags: []int32{projectTag},
	}
	lastNodeID = builder.appendNode(projectNode, bindCtx)
	return lastNodeID, projectNode, projectTag, rowNumberProjectPos, nil
}

func (builder *QueryBuilder) buildTargetSelectedExpr(
	tag int32,
	node *plan.Node,
	rowNumberPos int32,
	activePos int32,
) (*plan.Expr, error) {
	rowNumberExpr := &plan.Expr{
		Typ:  node.ProjectList[rowNumberPos].Typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: rowNumberPos}},
	}
	selected, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(),
		"=",
		[]*plan.Expr{rowNumberExpr, MakePlan2Int64ConstExprWithType(1)},
	)
	if err != nil {
		return nil, err
	}
	activeExpr := &plan.Expr{
		Typ:  node.ProjectList[activePos].Typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: activePos}},
	}
	return BindFuncExprImplByPlanExpr(
		builder.GetContext(),
		"and",
		[]*plan.Expr{selected, activeExpr},
	)
}

func (builder *QueryBuilder) appendUpdateFromDedupNode(
	bindCtx *BindContext,
	lastNodeID int32,
	selectNode *plan.Node,
	selectNodeTag int32,
	dmlCtx *DMLContext,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
) (int32, *plan.Node, int32, error) {
	// Dedup duplicate source matches by partitioning on the target row's
	// physical identity (row_id), not on the whole old target row. Partitioning
	// on every old column is unsafe: GEOMETRY32 (T_geometry32) has no comparator
	// in pkg/compare so Node_PARTITION would build a nil comparator and crash,
	// float columns compare NaN != NaN so duplicate matches against a target row
	// holding NaN stop being recognized as duplicates, and two distinct rows
	// whose columns happen to be equal would be wrongly merged into one. row_id
	// is stable, unique, and always comparable. For multi-target UPDATE ... FROM
	// the key is the combination of every updated target table's row_id.
	partitionColPositions := make([]int32, 0)
	for i, alias := range dmlCtx.aliases {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		if pos, ok := oldColName2Idx[alias+"."+catalog.Row_ID]; ok {
			partitionColPositions = append(partitionColPositions, pos)
		}
	}

	return builder.appendRowNumberDedupNode(bindCtx, lastNodeID, selectNode, selectNodeTag, partitionColPositions)
}

func (builder *QueryBuilder) appendRowNumberDedupNode(
	bindCtx *BindContext,
	lastNodeID int32,
	selectNode *plan.Node,
	selectNodeTag int32,
	partitionColPositions []int32,
) (int32, *plan.Node, int32, error) {
	return builder.appendRowNumberGuardNode(
		bindCtx, lastNodeID, selectNode, selectNodeTag, partitionColPositions, "", "")
}

func (builder *QueryBuilder) appendRowNumberGuardNode(
	bindCtx *BindContext,
	lastNodeID int32,
	selectNode *plan.Node,
	selectNodeTag int32,
	partitionColPositions []int32,
	duplicateErrorMessage string,
	duplicateErrorType string,
) (int32, *plan.Node, int32, error) {
	partitionByExprs := make([]*plan.Expr, 0, len(partitionColPositions))
	childColExpr := func(pos int32) *plan.Expr {
		e := selectNode.ProjectList[pos]
		name := ""
		if col, ok := e.Expr.(*plan.Expr_Col); ok {
			name = col.Col.Name
		}
		return &plan.Expr{
			Typ: e.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: selectNodeTag,
					ColPos: pos,
					Name:   name,
				},
			},
		}
	}
	for _, pos := range partitionColPositions {
		partitionByExprs = append(partitionByExprs, childColExpr(pos))
	}
	if len(partitionByExprs) == 0 {
		return lastNodeID, selectNode, selectNodeTag, nil
	}

	windowTag := builder.genNewBindTag()
	partitionBy := make([]*plan.OrderBySpec, 0, len(partitionByExprs))
	for _, expr := range partitionByExprs {
		partitionBy = append(partitionBy, &plan.OrderBySpec{
			Expr: expr,
			Flag: plan.OrderBySpec_INTERNAL,
		})
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PARTITION,
		Children:    []int32{lastNodeID},
		OrderBy:     partitionBy,
		BindingTags: []int32{windowTag},
	}, bindCtx)

	rowNumberFunc, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "row_number", nil)
	if err != nil {
		return 0, nil, 0, err
	}
	rowNumberExpr := &plan.Expr{
		Typ: rowNumberFunc.Typ,
		Expr: &plan.Expr_W{
			W: &plan.WindowSpec{
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
			},
		},
	}
	rowNumberIdx := int32(0)
	rowNumberProjectPos := int32(len(selectNode.ProjectList))
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_WINDOW,
		Children:    []int32{lastNodeID},
		WinSpecList: []*plan.Expr{rowNumberExpr},
		WindowIdx:   rowNumberIdx,
		BindingTags: []int32{windowTag},
	}, bindCtx)

	windowProjectTag := builder.genNewBindTag()
	windowProjectList := make([]*plan.Expr, 0, len(selectNode.ProjectList)+1)
	for pos := range selectNode.ProjectList {
		windowProjectList = append(windowProjectList, childColExpr(int32(pos)))
	}
	windowProjectList = append(windowProjectList, &plan.Expr{
		Typ: rowNumberFunc.Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: windowTag,
				ColPos: rowNumberIdx,
				Name:   "__mo_update_from_dedup_row_number",
			},
		},
	})
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		ProjectList: windowProjectList,
		BindingTags: []int32{windowProjectTag},
	}, bindCtx)

	rowNumberCol := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: windowProjectTag,
				ColPos: rowNumberProjectPos,
				Name:   "__mo_update_from_dedup_row_number",
			},
		},
	}
	keepFirstRowExpr, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(), "=", []*plan.Expr{rowNumberCol, makePlan2Int64ConstExprWithType(1)})
	if err != nil {
		return 0, nil, 0, err
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
			return 0, nil, 0, err
		}
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{lastNodeID},
		FilterList: []*plan.Expr{guardExpr},
	}, bindCtx)

	projectList := make([]*plan.Expr, len(selectNode.ProjectList))
	for pos, e := range selectNode.ProjectList {
		name := ""
		if col, ok := e.Expr.(*plan.Expr_Col); ok {
			name = col.Col.Name
		}
		projectList[pos] = &plan.Expr{
			Typ: e.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: windowProjectTag,
					ColPos: int32(pos),
					Name:   name,
				},
			},
		}
	}

	projectNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		ProjectList: projectList,
		BindingTags: []int32{builder.genNewBindTag()},
	}
	lastNodeID = builder.appendNode(projectNode, bindCtx)
	return lastNodeID, projectNode, projectNode.BindingTags[0], nil
}
