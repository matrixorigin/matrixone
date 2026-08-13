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
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
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
	if err = rejectRepeatedPhysicalUpdateTargets(builder.GetContext(), dmlCtx); err != nil {
		return 0, err
	}
	if err = builder.validateDistinctUpdateForeignKeyMutationTargets(bindCtx, dmlCtx); err != nil {
		return 0, err
	}
	for _, tableDef := range dmlCtx.tableDefs {
		if IsMaterializedViewTableDef(tableDef) && builder.GetContext().Value(defines.MaterializedViewRefreshKey{}) == nil {
			return 0, moerr.NewUnsupportedDML(builder.GetContext(), "update materialized view")
		}
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
			return 0, newLegacyUpdatePlannerRouteError(
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
	updatedTargetCount := 0
	for i := range dmlCtx.aliases {
		if len(dmlCtx.updateCol2Expr[i]) > 0 {
			updatedTargetCount++
		}
	}
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
	targetActivePos := make([]int32, len(dmlCtx.aliases))
	targetRowNumberPos := make([]int32, len(dmlCtx.aliases))
	for i := range dmlCtx.aliases {
		targetActivePos[i] = -1
		targetRowNumberPos[i] = -1
	}
	if isMultiTargetUpdate {
		for i, alias := range dmlCtx.aliases {
			if len(dmlCtx.updateCol2Expr[i]) == 0 {
				continue
			}
			rowIDPos := oldColName2Idx[alias+"."+catalog.Row_ID]
			activeExpr, buildErr := BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"isnotnull",
				[]*plan.Expr{DeepCopyExpr(selectNode.ProjectList[rowIDPos])},
			)
			if buildErr != nil {
				return 0, buildErr
			}
			targetActivePos[i] = int32(len(selectNode.ProjectList))
			selectNode.ProjectList = append(selectNode.ProjectList, activeExpr)
			lastNodeID, targetRowNumberPos[i], err = builder.appendTargetRowNumberBelowAssignmentProject(
				bindCtx,
				lastNodeID,
				selectNode,
				rowIDPos,
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

	if stmt.Ignore && isMultiTargetUpdate {
		lastNodeID, selectNode, selectNodeTag, err = builder.splitDistinctUpdateTargetBranches(
			bindCtx,
			lastNodeID,
			selectNode,
			dmlCtx,
			targetActivePos,
		)
		if err != nil {
			return 0, err
		}
	}

	if !isMultiTargetUpdate && stmt.From != nil && len(stmt.From.Tables) > 0 {
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
			preInsertCtx := &plan.PreInsertCtx{
				Ref:         dmlCtx.objRefs[i],
				TableDef:    tableDef,
				HasAutoCol:  true,
				ColOffset:   colOffsets[i],
				IsNewUpdate: true,
			}
			if isMultiTargetUpdate {
				preInsertCtx.HasTargetSelector = true
				preInsertCtx.TargetRowNumberCol = targetRowNumberPos[i]
				preInsertCtx.TargetActiveCol = targetActivePos[i]
				preInsertCtx.TargetRowIdCol = oldColName2Idx[dmlCtx.aliases[i]+"."+catalog.Row_ID]
			}
			lastNodeID = builder.appendNode(&plan.Node{
				NodeType:     plan.Node_PRE_INSERT,
				Children:     []int32{lastNodeID},
				PreInsertCtx: preInsertCtx,
			}, bindCtx)
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
			targetActivePos,
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
				targetActivePos[i],
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
		targetActivePos,
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
				if _, ok := newColName2Idx[alias+"."+catalog.ResolveAlias(colName)]; ok {
					idxNeedUpdate[i][j] = true
					updatePkOrUk = true
					break
				}
			}
		}
	}

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
			activePos := targetActivePos[targetIdx]
			activeExpr := &plan.Expr{
				Typ:  selectNode.ProjectList[activePos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectNodeTag, ColPos: activePos}},
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
				if !idxDef.Unique || !idxNeedUpdate[i][j] {
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

					for j, colName := range idxDef.Parts {
						colName = catalog.ResolveAlias(colName)
						colPos, ok := oldColName2Idx[alias+"."+colName]
						if !ok {
							return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind update err, can not find colName = %s", colName)
						}
						if updateIdx, ok := newColName2Idx[alias+"."+colName]; ok {
							colPos = updateIdx
						}

						newArgs[j], err = makeUpdateIndexPartExpr(colPos, colName, prefixLengths)
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
							newPartPos = oldPartPos
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
	targetUpdateCtxIdx := make([]int32, len(dmlCtx.aliases))
	targetOldPkFinalPos := make([]int32, len(dmlCtx.aliases))

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
			activePos := targetActivePos[i]
			finalProjList = append(finalProjList, &plan.Expr{
				Typ: selectNode.ProjectList[activePos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: selectNodeTag,
					ColPos: activePos,
				}},
			})
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
		if isMultiTargetUpdate {
			updateCtx.DedupByTargetRowId = true
			targetUpdateCtxIdx[i] = int32(len(updateCtxList))
			updateCtx.TargetUpdateCtxIdx = targetUpdateCtxIdx[i]
			updateCtx.DeleteCols = append(updateCtx.DeleteCols, plan.ColRef{
				RelPos: finalProjTag,
				ColPos: targetRowNumberFinalPos[i],
			})
			updateCtx.DeleteCols = append(updateCtx.DeleteCols, plan.ColRef{
				RelPos: finalProjTag,
				ColPos: targetRowNumberFinalPos[i] + 1,
			})
		}
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
			if idxDef.Unique {
				if idxNeedUpdate[i][j] {
					newPos := newColName2Idx[idxNode.TableDef.Name+"."+catalog.IndexTableIndexColName]
					newIdxExpr := &plan.Expr{
						Typ: selectNode.ProjectList[newPos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: selectNodeTag,
								ColPos: newPos,
							},
						},
					}

					newIdx = len(finalProjList)
					finalProjList = append(finalProjList, newIdxExpr)
				}
			} else {
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
			// Acquire every base/regular-index lock before the final row image is
			// fanned out to synchronous irregular-index maintenance. LOCK_OP is a
			// pass-through gate here, so force remapping to keep the complete image
			// needed by the shared SINK and RETURNING.
			lastNodeID = builder.appendNode(&plan.Node{
				NodeType:    plan.Node_LOCK_OP,
				Children:    []int32{lastNodeID},
				TableDef:    dmlCtx.tableDefs[0],
				BindingTags: []int32{finalProjTag},
				LockTargets: lockTargets,
			}, bindCtx)
			if builder.preserveLockProjection == nil {
				builder.preserveLockProjection = make(map[int32]struct{})
			}
			builder.preserveLockProjection[lastNodeID] = struct{}{}
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
			localProjList = append(localProjList, &plan.Expr{
				Typ: finalProjList[globalRowNumberPos+1].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: finalProjTag,
					ColPos: globalRowNumberPos + 1,
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

	if irregularBaseStep < 0 {
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_LOCK_OP,
			Children:    []int32{lastNodeID},
			TableDef:    dmlCtx.tableDefs[0],
			BindingTags: []int32{builder.genNewBindTag()},
			LockTargets: lockTargets,
		}, bindCtx)
	}
	reCheckifNeedLockWholeTable(builder)

	dmlNode.Children = append(dmlNode.Children, lastNodeID)
	lastNodeID = builder.appendNode(dmlNode, bindCtx)

	return lastNodeID, err
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

// rejectRepeatedPhysicalUpdateTargets keeps the first-stage multi-target
// implementation limited to distinct physical tables. The legacy planner can
// panic after matching rows from repeated writable aliases, so unsupported
// shapes must fail before either planner constructs an executable plan.
// Read-only aliases do not count: only aliases with SET assignments are targets.
func rejectRepeatedPhysicalUpdateTargets(ctx context.Context, dmlCtx *DMLContext) error {
	seen := make(map[uint64]string)
	for i, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) == 0 {
			continue
		}
		tableID := dmlCtx.tableDefs[i].TblId
		if previousAlias, ok := seen[tableID]; ok {
			return newUpdatePlannerRouteError(
				updatePlannerRejected,
				updateRouteReasonMultiTarget,
				moerr.NewNotSupportedf(
					ctx,
					"updating the same physical table through aliases '%s' and '%s'",
					previousAlias,
					dmlCtx.aliases[i],
				),
			)
		}
		seen[tableID] = dmlCtx.aliases[i]
	}
	return nil
}

// splitDistinctUpdateTargetBranches isolates UPDATE IGNORE constraint checks.
// A duplicate-key conflict disables only the branch for that physical target;
// sibling targets continue through their own branch and can still be written.
func (builder *QueryBuilder) splitDistinctUpdateTargetBranches(
	bindCtx *BindContext,
	lastNodeID int32,
	selectNode *plan.Node,
	dmlCtx *DMLContext,
	targetActivePos []int32,
) (int32, *plan.Node, int32, error) {
	sourceSinkID := appendSinkNode(builder, bindCtx, lastNodeID)
	if builder.preserveSinkProjection == nil {
		builder.preserveSinkProjection = make(map[int32]struct{})
	}
	builder.preserveSinkProjection[sourceSinkID] = struct{}{}
	sourceStep := builder.appendStep(sourceSinkID)

	branchIDs := make([]int32, 0, len(targetActivePos))
	branchNodes := make([]*plan.Node, 0, len(targetActivePos))
	for targetIdx, updateCols := range dmlCtx.updateCol2Expr {
		if len(updateCols) == 0 {
			continue
		}
		scanID := appendSinkScanNode(builder, bindCtx, sourceStep)
		if builder.positionalSinkScans == nil {
			builder.positionalSinkScans = make(map[int32]struct{})
		}
		builder.positionalSinkScans[scanID] = struct{}{}

		project := make([]*plan.Expr, len(selectNode.ProjectList))
		for pos, expr := range selectNode.ProjectList {
			project[pos] = &plan.Expr{
				Typ:  expr.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: int32(pos)}},
			}
		}
		for otherIdx, activePos := range targetActivePos {
			if activePos < 0 || otherIdx == targetIdx {
				continue
			}
			project[activePos] = makePlan2BoolConstExprWithType(false)
		}
		branchTag := builder.genNewBindTag()
		branchNode := &plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{scanID},
			ProjectList: project,
			BindingTags: []int32{branchTag},
		}
		branchID := builder.appendNode(branchNode, bindCtx)

		branchSinkID := appendSinkNode(builder, bindCtx, branchID)
		builder.preserveSinkProjection[branchSinkID] = struct{}{}
		branchStep := builder.appendStep(branchSinkID)
		materializedScanID := appendSinkScanNode(builder, bindCtx, branchStep)
		builder.positionalSinkScans[materializedScanID] = struct{}{}
		materializedTag := builder.genNewBindTag()
		materializedProject := make([]*plan.Expr, len(project))
		for pos, expr := range project {
			materializedProject[pos] = &plan.Expr{
				Typ:  expr.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: int32(pos)}},
			}
		}
		materializedNode := &plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{materializedScanID},
			ProjectList: materializedProject,
			BindingTags: []int32{materializedTag},
		}
		branchIDs = append(branchIDs, builder.appendNode(materializedNode, bindCtx))
		branchNodes = append(branchNodes, materializedNode)
	}
	if len(branchIDs) == 0 {
		return 0, nil, 0, moerr.NewInternalError(builder.GetContext(), "multi-target UPDATE has no writable target")
	}

	unionID := branchIDs[0]
	unionNode := branchNodes[0]
	for i := 1; i < len(branchIDs); i++ {
		unionTag := builder.genNewBindTag()
		unionProject := make([]*plan.Expr, len(unionNode.ProjectList))
		for pos, expr := range unionNode.ProjectList {
			unionProject[pos] = &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: unionNode.BindingTags[0],
					ColPos: int32(pos),
				}},
			}
		}
		unionNode = &plan.Node{
			NodeType:    plan.Node_UNION_ALL,
			Children:    []int32{unionID, branchIDs[i]},
			ProjectList: unionProject,
			BindingTags: []int32{unionTag},
		}
		unionID = builder.appendNode(unionNode, bindCtx)
	}
	return unionID, unionNode, unionNode.BindingTags[0], nil
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
