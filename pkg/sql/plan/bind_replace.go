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
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func (builder *QueryBuilder) bindReplace(stmt *tree.Replace, bindCtx *BindContext) (int32, error) {
	dmlCtx := NewDMLContext()
	// REPLACE has its own conflict handling; bypass the generic FK table rejection
	// in ResolveTables so FK tables can use the modern operator-based path.
	origCtx := builder.GetContext()
	builder.compCtx.SetContext(context.WithValue(origCtx, defines.IgnoreForeignKey{}, true))
	err := dmlCtx.ResolveTables(builder.compCtx, tree.TableExprs{stmt.Table}, nil, nil, true)
	builder.compCtx.SetContext(origCtx)
	if err != nil {
		return 0, err
	}

	// Capture irregular (IVF/fulltext/master) indexes before appendNodesForReplaceStmt
	// strips them from the 1:1 dedup+MULTI_UPDATE plan; REPLACE maintains them with
	// the same modern delete-old + insert-new sink-fanout as ODKU (issue #25000).
	// MASTER now has full synchronous modern maintenance (delete-by-pk + insert),
	// same as IVF/fulltext. HNSW/CAGRA/IVF-PQ are cron-maintained.
	tableDef := dmlCtx.tableDefs[0]
	if err := validateTableRegularIndexPrefixMetadata(tableDef); err != nil {
		return 0, err
	}

	irregularIndexes := getIrregularIndexes(tableDef)

	lastNodeID, colName2Idx, skipUniqueIdx, err := builder.initInsertReplaceStmt(bindCtx, stmt.Rows, stmt.Columns, dmlCtx.objRefs[0], dmlCtx.tableDefs[0], true, stmt.IsSetFormat)
	if err != nil {
		return 0, err
	}
	fkChecksEnabled, err := IsForeignKeyChecksEnabled(builder.compCtx)
	if err != nil {
		return 0, err
	}
	replaceOrdinalPos := int32(-1)
	if fkChecksEnabled && hasOrderedSelfReferentialAction(tableDef) {
		lastNodeID, replaceOrdinalPos, err = builder.appendReplaceSourceOrdinal(bindCtx, lastNodeID)
		if err != nil {
			return 0, err
		}
	}
	return builder.appendDedupAndMultiUpdateNodesForBindReplaceWithOrdinal(
		bindCtx, dmlCtx, lastNodeID, colName2Idx, skipUniqueIdx, irregularIndexes, replaceOrdinalPos)
}

func hasOrderedSelfReferentialAction(tableDef *plan.TableDef) bool {
	for _, fk := range tableDef.Fkeys {
		if fk.ForeignTbl == 0 &&
			(fk.OnDelete == plan.ForeignKeyDef_CASCADE || fk.OnDelete == plan.ForeignKeyDef_SET_NULL) {
			return true
		}
	}
	return false
}

func (builder *QueryBuilder) appendReplaceSourceOrdinal(
	bindCtx *BindContext, lastNodeID int32,
) (int32, int32, error) {
	inputNode := builder.qry.Nodes[lastNodeID]
	if len(inputNode.BindingTags) != 1 {
		return 0, 0, moerr.NewInternalError(
			builder.GetContext(), "self-referencing REPLACE source has no binding tag")
	}
	inputTag := inputNode.BindingTags[0]
	rowNumberFunc, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "row_number", nil)
	if err != nil {
		return 0, 0, err
	}
	windowTag := builder.genNewBindTag()
	windowID := builder.appendNode(&plan.Node{
		NodeType:  plan.Node_WINDOW,
		Children:  []int32{lastNodeID},
		WindowIdx: 0,
		WinSpecList: []*plan.Expr{{
			Typ: rowNumberFunc.Typ,
			Expr: &plan.Expr_W{W: &plan.WindowSpec{
				WindowFunc: rowNumberFunc,
				Name:       "row_number",
				Frame: &plan.FrameClause{
					Type:  plan.FrameClause_ROWS,
					Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
					End:   &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, UnBounded: true},
				},
			}},
		}},
		BindingTags: []int32{windowTag},
	}, bindCtx)

	ordinalPos := int32(len(inputNode.ProjectList))
	outputTag := builder.genNewBindTag()
	projection := getProjectionByLastNodeWithTag(builder, lastNodeID, inputTag)
	projection = append(projection, &plan.Expr{
		Typ: rowNumberFunc.Typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: windowTag, ColPos: 0, Name: "__mo_replace_row_number",
		}},
	})
	return builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{windowID},
		ProjectList: projection, BindingTags: []int32{outputTag},
	}, bindCtx), ordinalPos, nil
}

func (builder *QueryBuilder) applyOrderedSelfActionsFromConflicts(
	bindCtx *BindContext,
	lastNodeID int32,
	inputTag int32,
	tableDef *plan.TableDef,
	newColName2Idx map[string]int32,
	oldColName2Idx map[string][2]int32,
	ordinalPos int32,
) (int32, int32, error) {
	inputProjection := getProjectionByLastNodeWithTag(builder, lastNodeID, inputTag)
	rowWidth := len(inputProjection)
	if ordinalPos < 0 || int(ordinalPos) >= rowWidth {
		return 0, 0, moerr.NewInternalError(
			builder.GetContext(), "self-referencing REPLACE ordinal is unavailable")
	}
	oldRowID, ok := oldColName2Idx[tableDef.Name+"."+catalog.Row_ID]
	if !ok || int(oldRowID[1]) >= rowWidth {
		return 0, 0, moerr.NewInternalError(
			builder.GetContext(), "self-referencing REPLACE old RowID is unavailable")
	}

	materialize := func(nodeID, tag int32) int32 {
		sinkID := appendSinkNodeWithTag(builder, bindCtx, nodeID, tag)
		builder.qry.Nodes[sinkID].ExtraOptions = materialized.CTESinkOption
		if builder.preserveSinkProjection == nil {
			builder.preserveSinkProjection = make(map[int32]struct{})
		}
		builder.preserveSinkProjection[sinkID] = struct{}{}
		return builder.appendStep(sinkID)
	}

	selfActions := make([]*plan.ForeignKeyDef, 0, len(tableDef.Fkeys))
	for _, fk := range tableDef.Fkeys {
		if fk.ForeignTbl == 0 &&
			(fk.OnDelete == plan.ForeignKeyDef_CASCADE || fk.OnDelete == plan.ForeignKeyDef_SET_NULL) {
			selfActions = append(selfActions, fk)
		}
	}
	step := materialize(lastNodeID, inputTag)
	currentTag := inputTag
	for actionIdx, fk := range selfActions {
		leftTag := builder.genNewBindTag()
		rightTag := builder.genNewBindTag()
		leftID := builder.appendTaggedSinkScan(bindCtx, step, leftTag)
		rightID := builder.appendTaggedSinkScan(bindCtx, step, rightTag)
		predicates := make([]*plan.Expr, 0, len(fk.Cols)+2)
		for i, childColID := range fk.Cols {
			childName := colIDToName(tableDef, childColID)
			parentName := colIDToName(tableDef, fk.ForeignCols[i])
			if childName == "" || parentName == "" {
				return 0, 0, moerr.NewInternalError(
					builder.GetContext(), "self-referencing REPLACE column is unavailable")
			}
			childPos, childOK := newColName2Idx[tableDef.Name+"."+childName]
			parentRef, parentOK := oldColName2Idx[tableDef.Name+"."+parentName]
			if !childOK || !parentOK || int(childPos) >= rowWidth || int(parentRef[1]) >= rowWidth {
				return 0, 0, moerr.NewInternalError(
					builder.GetContext(), "self-referencing REPLACE conflict mapping is incomplete")
			}
			childExpr := &plan.Expr{Typ: inputProjection[childPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: leftTag, ColPos: childPos,
			}}}
			parentExpr := &plan.Expr{Typ: inputProjection[parentRef[1]].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: rightTag, ColPos: parentRef[1],
			}}}
			predicate, bindErr := BindFuncExprImplByPlanExpr(
				builder.GetContext(), "=", []*plan.Expr{childExpr, parentExpr})
			if bindErr != nil {
				return 0, 0, bindErr
			}
			predicates = append(predicates, predicate)
		}
		leftOrdinal := &plan.Expr{Typ: inputProjection[ordinalPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: leftTag, ColPos: ordinalPos,
		}}}
		rightOrdinal := &plan.Expr{Typ: inputProjection[ordinalPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: rightTag, ColPos: ordinalPos,
		}}}
		later, bindErr := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "<", []*plan.Expr{leftOrdinal, rightOrdinal})
		if bindErr != nil {
			return 0, 0, bindErr
		}
		predicates = append(predicates, later)
		oldRowIDExpr := &plan.Expr{Typ: inputProjection[oldRowID[1]].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: rightTag, ColPos: oldRowID[1],
		}}}
		hasConflict, bindErr := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "isnotnull", []*plan.Expr{oldRowIDExpr})
		if bindErr != nil {
			return 0, 0, bindErr
		}
		predicates = append(predicates, hasConflict)

		joinID, marker, bindErr := builder.insertMarkJoin(
			leftID, rightID, predicates, nil, false, bindCtx)
		if bindErr != nil {
			return 0, 0, bindErr
		}
		nextProjection := make([]*plan.Expr, rowWidth)
		for pos := range nextProjection {
			nextProjection[pos] = &plan.Expr{Typ: inputProjection[pos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: leftTag, ColPos: int32(pos),
			}}}
		}
		positionsToNull := make(map[int32]struct{})
		if fk.OnDelete == plan.ForeignKeyDef_CASCADE {
			for _, pos := range newColName2Idx {
				if pos >= 0 && int(pos) < rowWidth {
					positionsToNull[pos] = struct{}{}
				}
			}
		} else {
			actionCols := make(map[string]struct{}, len(fk.Cols))
			for _, childColID := range fk.Cols {
				childName := colIDToName(tableDef, childColID)
				actionCols[childName] = struct{}{}
				positionsToNull[newColName2Idx[tableDef.Name+"."+childName]] = struct{}{}
			}
			// The derived index key was computed before the action. Null every
			// affected key image as well, otherwise MULTI_UPDATE reinserts the old
			// child key and leaves a ghost secondary/unique-index entry.
			for _, idxDef := range tableDef.Indexes {
				affected := false
				for _, part := range idxDef.Parts {
					if _, ok := actionCols[catalog.ResolveAlias(part)]; ok {
						affected = true
						break
					}
				}
				if affected {
					if pos, ok := newColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName]; ok {
						positionsToNull[pos] = struct{}{}
					}
				}
			}
		}
		for pos := range positionsToNull {
			nullExpr := &plan.Expr{Typ: nextProjection[pos].Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
			nextProjection[pos], bindErr = BindFuncExprImplByPlanExpr(
				builder.GetContext(), "if", []*plan.Expr{DeepCopyExpr(marker), nullExpr, nextProjection[pos]})
			if bindErr != nil {
				return 0, 0, bindErr
			}
		}
		nextTag := builder.genNewBindTag()
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_PROJECT, Children: []int32{joinID},
			ProjectList: nextProjection, BindingTags: []int32{nextTag},
		}, bindCtx)
		if actionIdx == len(selfActions)-1 {
			return lastNodeID, nextTag, nil
		}
		step = materialize(lastNodeID, nextTag)
		currentTag = nextTag
	}

	lastNodeID = builder.appendTaggedSinkScan(bindCtx, step, currentTag)
	return lastNodeID, currentTag, nil
}

func colIDToName(tableDef *plan.TableDef, colID uint64) string {
	for _, col := range tableDef.Cols {
		if col.ColId == colID {
			return col.Name
		}
	}
	return ""
}

func (builder *QueryBuilder) appendDedupAndMultiUpdateNodesForBindReplace(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	lastNodeID int32,
	colName2Idx map[string]int32,
	skipUniqueIdx []bool,
	irregularIndexes []*plan.IndexDef,
) (int32, error) {
	return builder.appendDedupAndMultiUpdateNodesForBindReplaceWithOrdinal(
		bindCtx, dmlCtx, lastNodeID, colName2Idx, skipUniqueIdx, irregularIndexes, -1)
}

func (builder *QueryBuilder) appendDedupAndMultiUpdateNodesForBindReplaceWithOrdinal(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	lastNodeID int32,
	colName2Idx map[string]int32,
	skipUniqueIdx []bool,
	irregularIndexes []*plan.IndexDef,
	replaceOrdinalPos int32,
) (int32, error) {
	objRef := dmlCtx.objRefs[0]
	tableDef := dmlCtx.tableDefs[0]
	pkName := tableDef.Pkey.PkeyColName

	isFakePK := pkName == catalog.FakePrimaryKeyColName

	selectNode := builder.qry.Nodes[lastNodeID]
	selectTag := selectNode.BindingTags[0]

	// Validate the final replacement-row image before looking up or deleting any
	// conflicting old rows. appendNodesForReplaceStmt has already applied defaults,
	// assignment casts, generated columns, and PRE_INSERT processing, so CHECK sees
	// the same values that MULTI_UPDATE would write.
	var err error
	lastNodeID, err = appendCheckConstraintPlan(
		builder,
		bindCtx,
		tableDef,
		lastNodeID,
		selectTag,
		colName2Idx,
		false,
	)
	if err != nil {
		return 0, err
	}
	selectNode = builder.qry.Nodes[lastNodeID]

	// Enforce child->parent foreign keys on the inserted image with the same
	// row-scoped per-FK MARK-join assert the modern INSERT path uses. REPLACE always
	// inserts the new row (after deleting any conflicting row), so asserting the new
	// row's FKs covers both the insert-only and the conflict-replace cases: a missing
	// parent fails the statement, a NULL FK column satisfies MATCH SIMPLE, and a
	// self-referencing FK is left to the post-execution DetectSql in
	// bindAndOptimizeReplaceQuery.
	if fkEnabled, fkErr := builder.modernInsertFkCheckEnabled(tableDef); fkErr != nil {
		return 0, fkErr
	} else if fkEnabled {
		var assertErr error
		if lastNodeID, selectTag, assertErr = builder.buildModernChildFkAssert(bindCtx, tableDef, lastNodeID, selectTag,
			func(colName string) int32 { return colName2Idx[tableDef.Name+"."+colName] }); assertErr != nil {
			return 0, assertErr
		}
		selectNode = builder.qry.Nodes[lastNodeID]
	}

	fullProjTag := builder.genNewBindTag()
	fullProjList := make([]*plan.Expr, 0, len(selectNode.ProjectList)+len(tableDef.Cols))
	for i, expr := range selectNode.ProjectList {
		fullProjList = append(fullProjList, &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: selectTag,
					ColPos: int32(i),
				},
			},
		})
	}

	idxObjRefs := make([]*plan.ObjectRef, len(tableDef.Indexes))
	idxTableDefs := make([]*plan.TableDef, len(tableDef.Indexes))

	oldColName2Idx := make(map[string][2]int32, len(tableDef.Cols)+len(tableDef.Indexes)*2)

	// Check whether the table has any unique secondary index.
	// For fake PK tables with no unique indexes (no PK, no UK), REPLACE behaves like INSERT.
	// Skip the LEFT JOIN to avoid a cross join (empty join condition) that would incorrectly
	// match and delete all existing rows.
	hasUniqueIdx := false
	for i, idxDef := range tableDef.Indexes {
		if idxDef.Unique && !skipUniqueIdx[i] {
			hasUniqueIdx = true
			break
		}
	}
	needsOldIndexMaintenance := !isFakePK || hasUniqueIdx
	buildParentFKActions := len(tableDef.RefChildTbls) > 0
	if buildParentFKActions {
		enabled, err := IsForeignKeyChecksEnabled(builder.compCtx)
		if err != nil {
			return 0, err
		}
		buildParentFKActions = enabled
	}

	// get old columns from existing main table
	//
	// Real-PK path: skip the LEFT JOIN entirely. The old columns are filled as
	// NULL placeholders here and later captured on-the-fly by the PK DEDUP JOIN
	// from the same main-table scan that performs conflict detection
	// (OldColCaptureList is populated below). This merges the two main-table
	// scans (LEFT JOIN + DEDUP JOIN) into one.
	//
	// Fake-PK tables take a separate branch: the main-table scan there
	// co-exists with index-table scans, so no merge is possible and we keep the
	// legacy LEFT JOIN path unchanged.
	// Merged-scan only works when every index is single-part. Multi-part
	// indexes require serial(old_c1, old_c2, ...) which needs an intermediate
	// PROJECT after capture — deferred to a follow-up PR.
	hasMultiPartIdx := false
	if !isFakePK {
		for _, idxDef := range tableDef.Indexes {
			if len(idxDef.Parts) > 1 {
				hasMultiPartIdx = true
				break
			}
		}
	}
	// Merged-scan is disabled when the table has unique secondary indexes because
	// a unique-key conflict (without a PK conflict) requires the LEFT JOIN to
	// retrieve old-row columns for deletion. The merged-scan path only captures
	// old columns on PK conflict, leaving them NULL when only a UK conflicts.
	useMergedMainScan := !isFakePK && !hasMultiPartIdx && !hasUniqueIdx
	if isFakePK && !hasUniqueIdx {
		// No PK/UK: use NULL expressions for old columns so MULTI_UPDATE only inserts
		for _, col := range tableDef.Cols {
			oldColName2Idx[tableDef.Name+"."+col.Name] = [2]int32{fullProjTag, int32(len(fullProjList))}
			fullProjList = append(fullProjList, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{Isnull: true},
				},
			})
		}

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: fullProjList,
			Children:    []int32{lastNodeID},
			BindingTags: []int32{fullProjTag},
		}, bindCtx)
	} else if useMergedMainScan {
		// Real-PK path: fill fullProjList old-col slots with NULL literals.
		// The PK DEDUP JOIN below will capture the real values from its own
		// main-table scan via OldColCaptureList. Only tables with exclusively
		// single-part indexes reach here (see hasMultiPartIdx guard above), so
		// no serial() slots are needed.
		for _, col := range tableDef.Cols {
			oldColName2Idx[tableDef.Name+"."+col.Name] = [2]int32{fullProjTag, int32(len(fullProjList))}
			fullProjList = append(fullProjList, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{Isnull: true},
				},
			})
		}

		var err error
		for i, idxDef := range tableDef.Indexes {
			if skipUniqueIdx[i] && !needsOldIndexMaintenance {
				continue
			}
			idxObjRefs[i], idxTableDefs[i], err = builder.compCtx.ResolveIndexTableByRef(objRef, idxDef.IndexTableName, bindCtx.snapshot)
			if err != nil {
				return 0, err
			}
			ensureName2ColIndexForReplace(idxTableDefs[i])

			// Spatial indexes look up the old index-table row via the primary
			// column (indexLookupColumnName returns IndexTablePrimaryColName).
			// Map it to the main-table PK so capture resolves to the correct
			// column.
			oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTablePrimaryColName] = oldColName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]

			if !indexTableStoresSerializedKey(idxDef) {
				// Single-part (non-serialized): alias the idx-col lookup to
				// the raw captured column. Use indexPrimaryPartName to
				// resolve aliases consistently with the legacy path.
				oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName] = oldColName2Idx[tableDef.Name+"."+indexPrimaryPartName(idxDef)]
			}
			// Multi-part non-spatial indexes are excluded by hasMultiPartIdx guard above.
		}

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: fullProjList,
			Children:    []int32{lastNodeID},
			BindingTags: []int32{fullProjTag},
		}, bindCtx)
	} else {
		oldScanTag := builder.genNewBindTag()

		builder.addNameByColRef(oldScanTag, tableDef)

		oldScanNodeID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     CloneTableDefForPlan(tableDef, true),
			ObjRef:       objRef,
			BindingTags:  []int32{oldScanTag},
			ScanSnapshot: bindCtx.snapshot,
		}, bindCtx)

		for i, col := range tableDef.Cols {
			oldColName2Idx[tableDef.Name+"."+col.Name] = [2]int32{fullProjTag, int32(len(fullProjList))}
			fullProjList = append(fullProjList, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: oldScanTag,
						ColPos: int32(i),
					},
				},
			})
		}

		for i, idxDef := range tableDef.Indexes {
			if skipUniqueIdx[i] && !needsOldIndexMaintenance {
				continue
			}
			prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
			if err != nil {
				return 0, err
			}
			idxObjRefs[i], idxTableDefs[i], err = builder.compCtx.ResolveIndexTableByRef(objRef, idxDef.IndexTableName, bindCtx.snapshot)
			if err != nil {
				return 0, err
			}
			ensureName2ColIndexForReplace(idxTableDefs[i])
			oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTablePrimaryColName] = oldColName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]

			if !indexTableStoresSerializedKey(idxDef) {
				partName := indexPrimaryPartName(idxDef)
				if prefixLengths[partName] > 0 {
					colIdx := tableDef.Name2ColIndex[partName]
					partExpr := &plan.Expr{
						Typ: tableDef.Cols[colIdx].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{RelPos: oldScanTag, ColPos: colIdx},
						},
					}
					idxExpr, err := builder.makeIndexPartExprFromInputExpr(partExpr, partName, prefixLengths)
					if err != nil {
						return 0, err
					}
					oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName] = [2]int32{
						fullProjTag, int32(len(fullProjList)),
					}
					fullProjList = append(fullProjList, idxExpr)
				} else {
					oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName] = oldColName2Idx[tableDef.Name+"."+partName]
				}
			} else {
				args := make([]*plan.Expr, len(idxDef.Parts))
				for j, part := range idxDef.Parts {
					partName := catalog.ResolveAlias(part)
					colIdx := tableDef.Name2ColIndex[partName]
					args[j] = &plan.Expr{
						Typ: tableDef.Cols[colIdx].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: oldScanTag,
								ColPos: colIdx,
							},
						},
					}
					if prefixLengths[partName] > 0 {
						args[j], err = builder.makeIndexPartExprFromInputExpr(args[j], partName, prefixLengths)
						if err != nil {
							return 0, err
						}
					}
				}

				idxExpr := args[0]
				if len(idxDef.Parts) > 1 {
					funcName := "serial"
					if !idxDef.Unique {
						funcName = "serial_full"
					}
					idxExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, args)
				}

				oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName] = [2]int32{fullProjTag, int32(len(fullProjList))}
				fullProjList = append(fullProjList, idxExpr)
			}
		}

		// Build the LEFT JOIN ON list: for real-PK tables the PK equality OR'd
		// with one (AND-of-parts) condition per unique key; for fake-PK tables
		// (no real PK) the OR of one condition per unique key. An old row
		// conflicting on the PK or ANY unique key is fetched in a single join.
		// A single new row may match several old rows (fan-out); the conflicting
		// old rows are all deleted and the new row inserted once, handled by the
		// keep-last / delete-marker logic in hashbuild downstream.
		var joinConds []*plan.Expr
		if isFakePK {
			// Fake-PK tables previously joined on only the first unique key,
			// missing conflicts on the others; OR one condition per unique key.
			for i, idxDef := range tableDef.Indexes {
				if !idxDef.Unique || skipUniqueIdx[i] {
					continue
				}
				prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
				if err != nil {
					return 0, err
				}
				var ukPartConds []*plan.Expr
				for _, part := range idxDef.Parts {
					colName := catalog.ResolveAlias(part)
					colIdx := tableDef.Name2ColIndex[colName]
					colTyp := tableDef.Cols[colIdx].Typ
					lExpr := &plan.Expr{
						Typ: colTyp,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: selectTag,
								ColPos: colName2Idx[tableDef.Name+"."+colName],
							},
						},
					}
					rExpr := &plan.Expr{
						Typ: colTyp,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: oldScanTag,
								ColPos: colIdx,
							},
						},
					}
					if prefixLengths[colName] > 0 {
						lExpr, err = builder.makeIndexPartExprFromInputExpr(lExpr, colName, prefixLengths)
						if err != nil {
							return 0, err
						}
						rExpr, err = builder.makeIndexPartExprFromInputExpr(rExpr, colName, prefixLengths)
						if err != nil {
							return 0, err
						}
					}
					partCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{lExpr, rExpr})
					ukPartConds = append(ukPartConds, partCond)
				}
				if len(ukPartConds) == 0 {
					continue
				}
				ukCond := ukPartConds[0]
				for _, c := range ukPartConds[1:] {
					ukCond, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*plan.Expr{ukCond, c})
				}
				joinConds = append(joinConds, ukCond)
			}
		} else {
			pkPos := tableDef.Name2ColIndex[pkName]
			pkTyp := tableDef.Cols[pkPos].Typ
			leftExpr := &plan.Expr{
				Typ: pkTyp,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectTag,
						ColPos: colName2Idx[tableDef.Name+"."+pkName],
					},
				},
			}
			rightExpr := &plan.Expr{
				Typ: pkTyp,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: oldScanTag,
						ColPos: pkPos,
					},
				},
			}
			pkCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{leftExpr, rightExpr})
			joinConds = append(joinConds, pkCond)

			for i, idxDef := range tableDef.Indexes {
				if !idxDef.Unique || skipUniqueIdx[i] {
					continue
				}
				prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
				if err != nil {
					return 0, err
				}
				var ukPartConds []*plan.Expr
				for _, part := range idxDef.Parts {
					colName := catalog.ResolveAlias(part)
					colIdx := tableDef.Name2ColIndex[colName]
					colTyp := tableDef.Cols[colIdx].Typ
					lExpr := &plan.Expr{
						Typ: colTyp,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: selectTag,
								ColPos: colName2Idx[tableDef.Name+"."+colName],
							},
						},
					}
					rExpr := &plan.Expr{
						Typ: colTyp,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: oldScanTag,
								ColPos: colIdx,
							},
						},
					}
					if prefixLengths[colName] > 0 {
						lExpr, err = builder.makeIndexPartExprFromInputExpr(lExpr, colName, prefixLengths)
						if err != nil {
							return 0, err
						}
						rExpr, err = builder.makeIndexPartExprFromInputExpr(rExpr, colName, prefixLengths)
						if err != nil {
							return 0, err
						}
					}
					partCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{lExpr, rExpr})
					ukPartConds = append(ukPartConds, partCond)
				}
				var ukCond *plan.Expr
				if len(ukPartConds) == 1 {
					ukCond = ukPartConds[0]
				} else {
					ukCond = ukPartConds[0]
					for _, c := range ukPartConds[1:] {
						ukCond, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*plan.Expr{ukCond, c})
					}
				}
				joinConds = append(joinConds, ukCond)
			}
		}

		var joinOnList []*plan.Expr
		if len(joinConds) == 1 {
			joinOnList = joinConds
		} else if len(joinConds) > 1 {
			combined := joinConds[0]
			for _, c := range joinConds[1:] {
				combined, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*plan.Expr{combined, c})
			}
			joinOnList = []*plan.Expr{combined}
		}

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{lastNodeID, oldScanNodeID},
			JoinType: plan.Node_LEFT,
			OnList:   joinOnList,
		}, bindCtx)

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: fullProjList,
			Children:    []int32{lastNodeID},
			BindingTags: []int32{fullProjTag},
		}, bindCtx)
	}

	oldMainRowIDPos := oldColName2Idx[tableDef.Name+"."+catalog.Row_ID]
	oldMainPKPos := oldColName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]
	replaceDedupOldColList := func(first [2]int32) []plan.ColRef {
		oldCols := make([]plan.ColRef, 0, 3+len(tableDef.Indexes))
		seen := make(map[[2]int32]struct{}, 3+len(tableDef.Indexes))
		appendOldCol := func(pos [2]int32) {
			if _, ok := seen[pos]; ok {
				return
			}
			seen[pos] = struct{}{}
			oldCols = append(oldCols, plan.ColRef{
				RelPos: pos[0],
				ColPos: pos[1],
			})
		}
		appendOldCol(first)
		appendOldCol(oldMainRowIDPos)
		appendOldCol(oldMainPKPos)
		if buildParentFKActions {
			// Parent-side FK actions consume the actual old row selected by the
			// REPLACE conflict joins. Preserve every base column so FKs that
			// reference any UNIQUE key can reuse the delete action planner.
			for _, col := range tableDef.Cols {
				if pos, ok := oldColName2Idx[tableDef.Name+"."+col.Name]; ok {
					appendOldCol(pos)
				}
			}
		}
		for i, idxDef := range tableDef.Indexes {
			if idxTableDefs[i] == nil {
				continue
			}
			if pos, ok := oldColName2Idx[idxTableDefs[i].Name+"."+indexLookupColumnName(idxDef)]; ok {
				appendOldCol(pos)
			}
		}
		return oldCols
	}

	// detect primary key confliction (skip for fake PK tables)
	if !isFakePK {
		scanTag := builder.genNewBindTag()

		// handle primary/unique key confliction
		builder.addNameByColRef(scanTag, tableDef)

		scanNodeID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     CloneTableDefForPlan(tableDef, true),
			ObjRef:       objRef,
			BindingTags:  []int32{scanTag},
			ScanSnapshot: bindCtx.snapshot,
		}, bindCtx)
		pkPos := tableDef.Name2ColIndex[pkName]
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

		rightExpr := &plan.Expr{
			Typ: pkTyp,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: fullProjTag,
					ColPos: colName2Idx[tableDef.Name+"."+pkName],
				},
			},
		}

		joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			rightExpr,
			leftExpr,
		})

		var dedupColName string
		dedupColTypes := make([]plan.Type, len(tableDef.Pkey.Names))

		if len(tableDef.Pkey.Names) == 1 {
			dedupColName = tableDef.Pkey.Names[0]
		} else {
			dedupColName = "(" + strings.Join(tableDef.Pkey.Names, ",") + ")"
		}

		for i, part := range tableDef.Pkey.Names {
			dedupColTypes[i] = tableDef.Cols[tableDef.Name2ColIndex[part]].Typ
		}

		oldPkPos := oldColName2Idx[tableDef.Name+"."+pkName]

		dedupJoinCtx := &plan.DedupJoinCtx{
			DedupBuildKeepLast: true,
		}
		if useMergedMainScan {
			// Merged-scan mode: only capture the old columns that downstream
			// actually needs (RowID, PK, and index-key columns), not every
			// main-table column. The remaining NULL placeholders in fullProjList
			// stay as-is — they are never read by MULTI_UPDATE.
			requiredOldCols := make(map[string]struct{}, 2+len(tableDef.Indexes))
			requiredOldCols[catalog.Row_ID] = struct{}{}
			requiredOldCols[tableDef.Pkey.PkeyColName] = struct{}{}
			for i, idxDef := range tableDef.Indexes {
				if skipUniqueIdx[i] && !needsOldIndexMaintenance {
					continue
				}
				if !indexTableStoresSerializedKey(idxDef) {
					requiredOldCols[indexPrimaryPartName(idxDef)] = struct{}{}
				}
			}
			captureList := make([]plan.OldColCapture, 0, len(requiredOldCols))
			for i, col := range tableDef.Cols {
				if buildParentFKActions {
					requiredOldCols[col.Name] = struct{}{}
				}
				if _, needed := requiredOldCols[col.Name]; !needed {
					continue
				}
				placeholderPos := oldColName2Idx[tableDef.Name+"."+col.Name]
				captureList = append(captureList, plan.OldColCapture{
					BuildPlaceholder: plan.ColRef{
						RelPos: placeholderPos[0],
						ColPos: placeholderPos[1],
					},
					ProbeSource: plan.ColRef{
						RelPos: scanTag,
						ColPos: int32(i),
					},
				})
			}
			dedupJoinCtx.OldColCaptureList = captureList
		} else {
			// Legacy DelRows path: used when merged-scan is disabled (e.g.
			// tables with multi-part indexes).
			dedupJoinCtx.OldColList = replaceDedupOldColList(oldPkPos)
		}

		dedupJoinNode := &plan.Node{
			NodeType:          plan.Node_JOIN,
			Children:          []int32{scanNodeID, lastNodeID},
			JoinType:          plan.Node_DEDUP,
			OnList:            []*plan.Expr{joinCond},
			OnDuplicateAction: plan.Node_FAIL,
			DedupColName:      dedupColName,
			DedupColTypes:     dedupColTypes,
			DedupJoinCtx:      dedupJoinCtx,
		}

		lastNodeID = builder.appendNode(dedupJoinNode, bindCtx)
	}

	// detect unique key confliction
	for i, idxDef := range tableDef.Indexes {
		// A unique index whose key is statically NULL for this statement never conflicts
		// and is not stored, so it drives no conflict probe (matches the INSERT path).
		if !idxDef.Unique || skipUniqueIdx[i] {
			continue
		}

		idxTag := builder.genNewBindTag()
		builder.addNameByColRef(idxTag, idxTableDefs[i])

		idxScanNode := &plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     idxTableDefs[i],
			ObjRef:       idxObjRefs[i],
			BindingTags:  []int32{idxTag},
			ScanSnapshot: bindCtx.snapshot,
		}
		idxTableNodeID := builder.appendNode(idxScanNode, bindCtx)

		idxPkPos := idxTableDefs[i].Name2ColIndex[catalog.IndexTableIndexColName]
		pkTyp := idxTableDefs[i].Cols[idxPkPos].Typ

		leftExpr := &plan.Expr{
			Typ: pkTyp,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag,
					ColPos: idxPkPos,
				},
			},
		}

		rightExpr := &plan.Expr{
			Typ: pkTyp,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: fullProjTag,
					ColPos: colName2Idx[idxTableDefs[i].Name+"."+catalog.IndexTableIndexColName],
				},
			},
		}

		joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			leftExpr,
			rightExpr,
		})

		var dedupColName string
		dedupColTypes := make([]plan.Type, len(idxDef.Parts))

		if len(idxDef.Parts) == 1 {
			dedupColName = idxDef.Parts[0]
		} else {
			dedupColName = "("
			for j, part := range idxDef.Parts {
				if j == 0 {
					dedupColName += catalog.ResolveAlias(part)
				} else {
					dedupColName += "," + catalog.ResolveAlias(part)
				}
			}
			dedupColName += ")"
		}

		for j, part := range idxDef.Parts {
			dedupColTypes[j] = tableDef.Cols[tableDef.Name2ColIndex[catalog.ResolveAlias(part)]].Typ
		}

		oldPkPos := oldColName2Idx[idxTableDefs[i].Name+"."+catalog.IndexTableIndexColName]

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:          plan.Node_JOIN,
			Children:          []int32{idxTableNodeID, lastNodeID},
			JoinType:          plan.Node_DEDUP,
			OnList:            []*plan.Expr{joinCond},
			OnDuplicateAction: plan.Node_FAIL,
			DedupColName:      dedupColName,
			DedupColTypes:     dedupColTypes,
			DedupJoinCtx: &plan.DedupJoinCtx{
				DedupBuildKeepLast: true,
				OldColList:         replaceDedupOldColList(oldPkPos),
			},
		}, bindCtx)
	}

	// Give the dedup output an explicit, flat owner before adding the old index
	// rows. DEDUP is probe-pass-through and otherwise has no ProjectList of its
	// own, which makes later dual-scan materialization unable to describe the
	// physical row layout.
	if replaceOrdinalPos >= 0 {
		flatTag := builder.genNewBindTag()
		flatProjection := make([]*plan.Expr, len(fullProjList))
		for pos, expr := range fullProjList {
			flatProjection[pos] = &plan.Expr{Typ: expr.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: fullProjTag, ColPos: int32(pos),
			}}}
		}
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_PROJECT, Children: []int32{lastNodeID},
			ProjectList: flatProjection, BindingTags: []int32{flatTag},
		}, bindCtx)
		for name, oldRef := range oldColName2Idx {
			if oldRef[0] == fullProjTag {
				oldColName2Idx[name] = [2]int32{flatTag, oldRef[1]}
			}
		}
		fullProjTag = flatTag
		fullProjList = flatProjection
	}

	// get old RowID for index tables
	for i, idxDef := range tableDef.Indexes {
		if skipUniqueIdx[i] && !needsOldIndexMaintenance {
			continue
		}
		idxTag := builder.genNewBindTag()
		builder.addNameByColRef(idxTag, idxTableDefs[i])

		idxScanNode := &plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     idxTableDefs[i],
			ObjRef:       idxObjRefs[i],
			BindingTags:  []int32{idxTag},
			ScanSnapshot: bindCtx.snapshot,
		}
		idxTableNodeID := builder.appendNode(idxScanNode, bindCtx)
		if replaceOrdinalPos < 0 {
			oldColName2Idx[idxTableDefs[i].Name+"."+catalog.Row_ID] = [2]int32{
				idxTag, idxTableDefs[i].Name2ColIndex[catalog.Row_ID],
			}
		}

		lookupColName := indexLookupColumnName(idxDef)
		idxPkPos := idxTableDefs[i].Name2ColIndex[lookupColName]
		pkTyp := idxTableDefs[i].Cols[idxPkPos].Typ

		leftExpr := &plan.Expr{
			Typ: pkTyp,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag,
					ColPos: idxPkPos,
				},
			},
		}

		oldPkPos := oldColName2Idx[idxTableDefs[i].Name+"."+lookupColName]
		if replaceOrdinalPos < 0 {
			oldColName2Idx[idxTableDefs[i].Name+"."+lookupColName] = [2]int32{
				idxTag, idxTableDefs[i].Name2ColIndex[lookupColName],
			}
		}

		rightExpr := &plan.Expr{
			Typ: pkTyp,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: oldPkPos[0],
					ColPos: oldPkPos[1],
				},
			},
		}

		joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			leftExpr,
			rightExpr,
		})

		if replaceOrdinalPos < 0 {
			lastNodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: []int32{lastNodeID, idxTableNodeID},
				JoinType: plan.Node_LEFT,
				OnList:   []*plan.Expr{joinCond},
			}, bindCtx)
			continue
		}

		joinProjection := make([]*plan.Expr, 0, len(fullProjList)+len(idxTableDefs[i].Cols))
		for pos, expr := range fullProjList {
			joinProjection = append(joinProjection, &plan.Expr{Typ: expr.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: fullProjTag, ColPos: int32(pos),
			}}})
		}
		for pos, col := range idxTableDefs[i].Cols {
			joinProjection = append(joinProjection, &plan.Expr{Typ: col.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: idxTag, ColPos: int32(pos), Name: col.Name,
			}}})
		}
		joinTag := builder.genNewBindTag()
		joinID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN, Children: []int32{lastNodeID, idxTableNodeID},
			JoinType: plan.Node_LEFT, OnList: []*plan.Expr{joinCond},
		}, bindCtx)
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_PROJECT, Children: []int32{joinID},
			ProjectList: joinProjection, BindingTags: []int32{joinTag},
		}, bindCtx)
		for name, oldRef := range oldColName2Idx {
			if oldRef[0] == fullProjTag {
				oldColName2Idx[name] = [2]int32{joinTag, oldRef[1]}
			}
		}
		baseWidth := int32(len(fullProjList))
		oldColName2Idx[idxTableDefs[i].Name+"."+catalog.Row_ID] = [2]int32{
			joinTag, baseWidth + idxTableDefs[i].Name2ColIndex[catalog.Row_ID],
		}
		oldColName2Idx[idxTableDefs[i].Name+"."+lookupColName] = [2]int32{
			joinTag, baseWidth + idxTableDefs[i].Name2ColIndex[lookupColName],
		}
		fullProjTag = joinTag
		fullProjList = joinProjection
	}

	if replaceOrdinalPos >= 0 {
		conflictInputTag := fullProjTag
		lastNodeID, fullProjTag, err = builder.applyOrderedSelfActionsFromConflicts(
			bindCtx, lastNodeID, fullProjTag, tableDef, colName2Idx,
			oldColName2Idx, replaceOrdinalPos)
		if err != nil {
			return 0, err
		}
		for name, oldRef := range oldColName2Idx {
			if oldRef[0] == conflictInputTag {
				oldColName2Idx[name] = [2]int32{fullProjTag, oldRef[1]}
			}
		}
		fullProjList = builder.qry.Nodes[lastNodeID].ProjectList
	}

	lockTargets := make([]*plan.LockTarget, 0)
	updateCtxList := make([]*plan.UpdateCtx, 0)

	finalProjTag := builder.genNewBindTag()
	finalProjList := make([]*plan.Expr, 0, len(tableDef.Cols)+len(tableDef.Indexes)*2)
	var newPkIdx int32

	// Position (within finalProjList) of the matched old row's PK, used to key the
	// irregular-index entries delete. For REPLACE the conflict may be on a non-PK
	// unique key, so the deleted row's PK can differ from the inserted row's PK.
	var replaceOldPkPos int32
	var replaceOldPkTyp plan.Type
	var replaceOldParentPos []int32
	oldParentColFinalPos := make(map[string]int32)

	{
		insertCols := make([]plan.ColRef, len(tableDef.Cols)-1)
		deleteCols := make([]plan.ColRef, 2)

		for i, col := range tableDef.Cols {
			finalColIdx := len(finalProjList)

			if col.Name != catalog.Row_ID {
				insertCols[i].RelPos = finalProjTag
				insertCols[i].ColPos = int32(finalColIdx)
			}

			colIdx := colName2Idx[tableDef.Name+"."+col.Name]
			finalProjList = append(finalProjList, &plan.Expr{
				Typ: fullProjList[colIdx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: fullProjTag,
						ColPos: int32(colIdx),
					},
				},
			})

			if col.Name == tableDef.Pkey.PkeyColName {
				newPkIdx = int32(finalColIdx)
			}
		}

		lockTargets = append(lockTargets, &plan.LockTarget{
			TableId:            tableDef.TblId,
			ObjRef:             objRef,
			PrimaryColIdxInBat: newPkIdx,
			PrimaryColRelPos:   finalProjTag,
			PrimaryColTyp:      finalProjList[newPkIdx].Typ,
		})
		insertPkColIdx := int32(-1)
		for i, col := range insertCols {
			if col.ColPos == newPkIdx {
				insertPkColIdx = int32(i)
				break
			}
		}
		if insertPkColIdx < 0 {
			panic("replace main table primary key column not found in insert columns")
		}

		oldRowIdPos := oldColName2Idx[tableDef.Name+"."+catalog.Row_ID]
		deleteCols[0].RelPos = finalProjTag
		deleteCols[0].ColPos = int32(len(finalProjList))
		finalProjList = append(finalProjList, &plan.Expr{
			Typ: fullProjList[oldRowIdPos[1]].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: fullProjTag,
					ColPos: oldRowIdPos[1],
				},
			},
		})
		oldParentColFinalPos[catalog.Row_ID] = deleteCols[0].ColPos

		oldPkPos := oldColName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]
		deleteCols[1].RelPos = finalProjTag
		deleteCols[1].ColPos = int32(len(finalProjList))
		replaceOldPkPos = int32(len(finalProjList))
		replaceOldPkTyp = fullProjList[oldPkPos[1]].Typ
		if useMergedMainScan {
			// Merged-scan mode runs only when the table has no unique secondary
			// key, so every REPLACE conflict is a PRIMARY-key conflict and the
			// matched old row's PK equals the new row's PK. The captured old-PK
			// placeholder is not reliably materialized into the irregular-index
			// delete sink here, so key that delete on the (immutable) new PK at its
			// natural position instead. (The base-table MULTI_UPDATE delete keeps
			// using the captured old PK via deleteCols below.)
			replaceOldPkPos = newPkIdx
			replaceOldPkTyp = finalProjList[newPkIdx].Typ
		}
		lockTargets = append(lockTargets, &plan.LockTarget{
			TableId:            tableDef.TblId,
			ObjRef:             objRef,
			PrimaryColIdxInBat: int32(len(finalProjList)),
			PrimaryColRelPos:   finalProjTag,
			PrimaryColTyp:      finalProjList[newPkIdx].Typ,
		})
		finalProjList = append(finalProjList, &plan.Expr{
			Typ: fullProjList[oldPkPos[1]].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: fullProjTag,
					ColPos: oldPkPos[1],
				},
			},
		})
		oldParentColFinalPos[tableDef.Pkey.PkeyColName] = replaceOldPkPos
		updateCtxList = append(updateCtxList, &plan.UpdateCtx{
			ObjRef:                objRef,
			TableDef:              tableDef,
			InsertCols:            insertCols,
			DeleteCols:            deleteCols,
			SkipInsertOnNullPk:    true,
			InsertPkColIdx:        insertPkColIdx,
			CountDeleteAffectRows: true,
		})
	}

	orderedIndexPos := make([]int, len(tableDef.Indexes))
	for i := range orderedIndexPos {
		orderedIndexPos[i] = i
	}
	slices.SortStableFunc(orderedIndexPos, func(left, right int) int {
		return strings.Compare(
			tableDef.Indexes[left].IndexTableName,
			tableDef.Indexes[right].IndexTableName,
		)
	})
	for _, i := range orderedIndexPos {
		idxDef := tableDef.Indexes[i]
		if skipUniqueIdx[i] && !needsOldIndexMaintenance {
			continue
		}
		insertCols := make([]plan.ColRef, 2)
		deleteCols := make([]plan.ColRef, 2)

		newIdxPos := colName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName]
		prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
		if err != nil {
			return 0, err
		}
		partName := indexPrimaryPartName(idxDef)
		if indexTableStoresSerializedKey(idxDef) || prefixLengths[partName] > 0 {
			idxExpr := &plan.Expr{
				Typ: fullProjList[newIdxPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: fullProjTag,
						ColPos: newIdxPos,
					},
				},
			}
			newIdxPos = int32(len(finalProjList))
			finalProjList = append(finalProjList, idxExpr)
		}

		oldRowIdPos := int32(len(finalProjList))
		oldRowIDKey := idxTableDefs[i].Name + "." + catalog.Row_ID
		oldColRef, ok := oldColName2Idx[oldRowIDKey]
		if !ok {
			return 0, moerr.NewInternalErrorf(builder.GetContext(),
				"bind replace err, can not find old index rowid colName = %s", oldRowIDKey)
		}
		rowIdExpr := &plan.Expr{
			Typ: idxTableDefs[i].Cols[idxTableDefs[i].Name2ColIndex[catalog.Row_ID]].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: oldColRef[0],
					ColPos: oldColRef[1],
				},
			},
		}
		finalProjList = append(finalProjList, rowIdExpr)

		oldIdxPos := int32(len(finalProjList))
		lookupColName := indexLookupColumnName(idxDef)
		lookupColIdx := idxTableDefs[i].Name2ColIndex[lookupColName]
		oldLookupKey := idxTableDefs[i].Name + "." + lookupColName
		oldColRef, ok = oldColName2Idx[oldLookupKey]
		if !ok {
			return 0, moerr.NewInternalErrorf(builder.GetContext(),
				"bind replace err, can not find old index lookup colName = %s", oldLookupKey)
		}
		idxExpr := &plan.Expr{
			Typ: idxTableDefs[i].Cols[lookupColIdx].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: oldColRef[0],
					ColPos: oldColRef[1],
				},
			},
		}
		finalProjList = append(finalProjList, idxExpr)
		if len(idxDef.Parts) == 1 && !indexTableStoresSerializedKey(idxDef) && prefixLengths[partName] == 0 {
			oldParentColFinalPos[catalog.ResolveAlias(idxDef.Parts[0])] = oldIdxPos
		}

		insertCols[0].RelPos = finalProjTag
		insertCols[0].ColPos = int32(newIdxPos)
		insertCols[1].RelPos = finalProjTag
		insertCols[1].ColPos = newPkIdx

		deleteCols[0].RelPos = finalProjTag
		deleteCols[0].ColPos = oldRowIdPos
		deleteCols[1].RelPos = finalProjTag
		deleteCols[1].ColPos = int32(oldIdxPos)

		updateCtxList = append(updateCtxList, &plan.UpdateCtx{
			ObjRef:     idxObjRefs[i],
			TableDef:   idxTableDefs[i],
			InsertCols: insertCols,
			DeleteCols: deleteCols,
		})

		if idxDef.Unique {
			if !skipUniqueIdx[i] {
				lockTargets = append(lockTargets, &plan.LockTarget{
					TableId:            idxTableDefs[i].TblId,
					ObjRef:             idxObjRefs[i],
					PrimaryColIdxInBat: int32(newIdxPos),
					PrimaryColRelPos:   finalProjTag,
					PrimaryColTyp:      finalProjList[newIdxPos].Typ,
				})
			}
			lockTargets = append(lockTargets, &plan.LockTarget{
				TableId:            idxTableDefs[i].TblId,
				ObjRef:             idxObjRefs[i],
				PrimaryColIdxInBat: int32(oldIdxPos),
				PrimaryColRelPos:   finalProjTag,
				PrimaryColTyp:      finalProjList[oldIdxPos].Typ,
			})
		}
	}

	if buildParentFKActions {
		// Append the auxiliary old-parent image after every existing DML/index
		// column. Native index keys rely on the legacy prefix positions above.
		replaceOldParentPos = make([]int32, len(tableDef.Cols))
		for i, col := range tableDef.Cols {
			if finalPos, ok := oldParentColFinalPos[col.Name]; ok {
				replaceOldParentPos[i] = finalPos
				continue
			}
			oldPos, ok := oldColName2Idx[tableDef.Name+"."+col.Name]
			if !ok {
				return 0, moerr.NewInternalErrorf(builder.GetContext(),
					"bind replace err, can not find old parent column %s", col.Name)
			}
			replaceOldParentPos[i] = int32(len(finalProjList))
			finalProjList = append(finalProjList, &plan.Expr{
				Typ: fullProjList[oldPos[1]].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: fullProjTag,
					ColPos: oldPos[1],
				}},
			})
		}
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		ProjectList: finalProjList,
		BindingTags: []int32{finalProjTag},
	}, bindCtx)

	// REPLACE into an irregular-index table: the finalProj image carries both the
	// new row (base columns) and the matched old row's PK, so materialize it once
	// and let the main plan, the insert maintenance (new entries) and the delete
	// maintenance (drop the old entries, keyed by the old PK) all read it.
	if len(irregularIndexes) > 0 && replaceOldPkPos >= 0 {
		lastNodeID = builder.appendOnDupIrregularMaintSource(
			bindCtx, lastNodeID, finalProjTag, replaceOldPkPos, replaceOldPkTyp,
			irregularIndexes, tableDef, objRef)
	}

	if len(lockTargets) > 0 && !buildParentFKActions {
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_LOCK_OP,
			Children: []int32{lastNodeID},
			TableDef: tableDef,
			// LOCK_OP is a pass-through node. Keep the projection tag so a
			// following shared SINK can remap every requested column correctly.
			BindingTags: []int32{finalProjTag},
			LockTargets: lockTargets,
		}, bindCtx)
		reCheckifNeedLockWholeTable(builder)
	}

	if len(replaceOldParentPos) > 0 {
		// Execute parent-side FK actions from the same evaluated and locked old-row
		// set consumed by MULTI_UPDATE. This supports VALUES parameters/functions
		// and REPLACE SELECT/TABLE without serializing their AST into background SQL.
		evaluatedSinkID := appendSinkNode(builder, bindCtx, lastNodeID)
		if builder.preserveSinkProjection == nil {
			builder.preserveSinkProjection = make(map[int32]struct{})
		}
		builder.preserveSinkProjection[evaluatedSinkID] = struct{}{}
		evaluatedStep := builder.appendStep(evaluatedSinkID)

		lockedSourceID := appendSinkScanNode(builder, bindCtx, evaluatedStep)
		builder.qry.Nodes[lockedSourceID].BindingTags = []int32{finalProjTag}
		lockedSourceID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_LOCK_OP,
			Children:    []int32{lockedSourceID},
			TableDef:    tableDef,
			BindingTags: []int32{finalProjTag},
			LockTargets: lockTargets,
		}, bindCtx)
		if builder.preserveLockProjection == nil {
			builder.preserveLockProjection = make(map[int32]struct{})
		}
		builder.preserveLockProjection[lockedSourceID] = struct{}{}
		reCheckifNeedLockWholeTable(builder)

		sharedSinkID := appendSinkNode(builder, bindCtx, lockedSourceID)
		builder.preserveSinkProjection[sharedSinkID] = struct{}{}
		sharedStep := builder.appendStep(sharedSinkID)

		actionSourceID := appendSinkScanNode(builder, bindCtx, sharedStep)
		actionInputTag := builder.genNewBindTag()
		builder.qry.Nodes[actionSourceID].BindingTags = []int32{actionInputTag}
		actionTag := builder.genNewBindTag()
		actionProjection := make([]*plan.Expr, len(tableDef.Cols))
		for i, col := range tableDef.Cols {
			actionProjection[i] = &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: actionInputTag,
					ColPos: replaceOldParentPos[i],
				}},
			}
		}
		actionSourceID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{actionSourceID},
			ProjectList: actionProjection,
			BindingTags: []int32{actionTag},
		}, bindCtx)
		actionSinkID := appendSinkNode(builder, bindCtx, actionSourceID)
		builder.preserveSinkProjection[actionSinkID] = struct{}{}
		actionStep := builder.appendStep(actionSinkID)

		delCtx := getDmlPlanCtx()
		delCtx.objRef = objRef
		delCtx.tableDef = tableDef
		delCtx.sourceStep = actionStep
		delCtx.rowIdPos = int(tableDef.Name2ColIndex[catalog.Row_ID])
		delCtx.allDelTableIDs = map[uint64]struct{}{tableDef.TblId: {}}
		delCtx.skipTargetDelete = true
		delCtx.replaceOldRowsStep = actionStep
		delCtx.replaceTargetTableID = tableDef.TblId
		delCtx.replaceOldRowIDPos = int32(tableDef.Name2ColIndex[catalog.Row_ID])
		err := buildDeletePlans(builder.compCtx, builder, bindCtx, delCtx)
		putDmlPlanCtx(delCtx)
		if err != nil {
			return 0, err
		}

		lastNodeID = appendSinkScanNode(builder, bindCtx, sharedStep)
		builder.qry.Nodes[lastNodeID].BindingTags = []int32{finalProjTag}
	}

	cycleFks, err := builder.replaceCycleForeignKeys(tableDef)
	if err != nil {
		return 0, err
	}
	if len(cycleFks) > 0 {
		encoded, encodeErr := builder.encodeReplaceCycleCheck(objRef.SchemaName, tableDef, cycleFks)
		if encodeErr != nil {
			return 0, encodeErr
		}
		pkName := catalog.ResolveAlias(tableDef.Pkey.Names[0])
		materializedSinkID := appendSinkNodeWithTag(builder, bindCtx, lastNodeID, finalProjTag)
		builder.qry.Nodes[materializedSinkID].ExtraOptions = materialized.CTESinkOption
		if builder.preserveSinkProjection == nil {
			builder.preserveSinkProjection = make(map[int32]struct{})
		}
		builder.preserveSinkProjection[materializedSinkID] = struct{}{}
		materializedStep := builder.appendStep(materializedSinkID)

		postTag := builder.genNewBindTag()
		postSourceID := builder.appendTaggedSinkScan(bindCtx, materializedStep, postTag)
		postID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_POSTDML, Children: []int32{postSourceID},
			PostDmlCtx: &plan.PostDmlCtx{
				Ref: objRef, PrimaryKeyIdx: tableDef.Name2ColIndex[pkName],
				PrimaryKeyName: pkName, ReplaceCycleCheck: encoded,
			},
		}, bindCtx)
		builder.appendStep(postID)

		writeTag := builder.genNewBindTag()
		lastNodeID = builder.appendTaggedSinkScan(bindCtx, materializedStep, writeTag)
		for i := range updateCtxList {
			for j := range updateCtxList[i].InsertCols {
				updateCtxList[i].InsertCols[j].RelPos = writeTag
			}
			for j := range updateCtxList[i].DeleteCols {
				updateCtxList[i].DeleteCols[j].RelPos = writeTag
			}
		}
	}

	return builder.appendNode(&plan.Node{
		NodeType:      plan.Node_MULTI_UPDATE,
		Children:      []int32{lastNodeID},
		BindingTags:   []int32{builder.genNewBindTag()},
		UpdateCtxList: updateCtxList,
	}, bindCtx), nil
}

type replaceCycleCheckColumn struct {
	Name string `json:"name"`
	Pos  int32  `json:"pos"`
}

type replaceCycleCheckFK struct {
	ParentSchema string   `json:"parent_schema"`
	ParentTable  string   `json:"parent_table"`
	ChildCols    []string `json:"child_cols"`
	ParentCols   []string `json:"parent_cols"`
}

type replaceCycleCheckConfig struct {
	ChildSchema string                    `json:"child_schema"`
	ChildTable  string                    `json:"child_table"`
	PrimaryKey  []replaceCycleCheckColumn `json:"primary_key"`
	ForeignKeys []replaceCycleCheckFK     `json:"foreign_keys"`
}

func (builder *QueryBuilder) encodeReplaceCycleCheck(
	childSchema string, tableDef *plan.TableDef, cycleFks []*plan.ForeignKeyDef,
) (string, error) {
	config := replaceCycleCheckConfig{ChildSchema: childSchema, ChildTable: tableDef.Name}
	for _, name := range tableDef.Pkey.Names {
		name = catalog.ResolveAlias(name)
		config.PrimaryKey = append(config.PrimaryKey, replaceCycleCheckColumn{
			Name: name, Pos: tableDef.Name2ColIndex[name],
		})
	}
	for _, fk := range cycleFks {
		parentRef, parentDef, err := builder.compCtx.ResolveById(fk.ForeignTbl, nil)
		if err != nil {
			return "", err
		}
		if parentRef == nil || parentDef == nil {
			return "", moerr.NewInternalErrorf(builder.GetContext(),
				"foreign-key parent table %d is unavailable", fk.ForeignTbl)
		}
		item := replaceCycleCheckFK{
			ParentSchema: parentRef.SchemaName,
			ParentTable:  parentDef.Name,
		}
		for i, childID := range fk.Cols {
			item.ChildCols = append(item.ChildCols, colIDToName(tableDef, childID))
			item.ParentCols = append(item.ParentCols, colIDToName(parentDef, fk.ForeignCols[i]))
		}
		config.ForeignKeys = append(config.ForeignKeys, item)
	}
	encoded, err := json.Marshal(config)
	return string(encoded), err
}

func (builder *QueryBuilder) replaceCycleForeignKeys(tableDef *plan.TableDef) ([]*plan.ForeignKeyDef, error) {
	cycleFks := make([]*plan.ForeignKeyDef, 0, len(tableDef.Fkeys))
	for _, fk := range tableDef.Fkeys {
		if fk.ForeignTbl == 0 {
			continue
		}
		pending := []uint64{fk.ForeignTbl}
		seen := make(map[uint64]struct{})
		closesCycle := false
		for len(pending) > 0 && !closesCycle {
			tableID := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			if tableID == tableDef.TblId {
				closesCycle = true
				break
			}
			if tableID == 0 {
				continue
			}
			if _, ok := seen[tableID]; ok {
				continue
			}
			seen[tableID] = struct{}{}
			_, parentDef, err := builder.compCtx.ResolveById(tableID, nil)
			if err != nil {
				return nil, err
			}
			if parentDef == nil {
				continue
			}
			for _, parentFK := range parentDef.Fkeys {
				pending = append(pending, parentFK.ForeignTbl)
			}
		}
		if closesCycle {
			cycleFks = append(cycleFks, fk)
		}
	}
	return cycleFks, nil
}

func ensureName2ColIndexForReplace(tableDef *TableDef) {
	if len(tableDef.Name2ColIndex) > 0 {
		return
	}
	tableDef.Name2ColIndex = make(map[string]int32, len(tableDef.Cols))
	for colIdx, col := range tableDef.Cols {
		tableDef.Name2ColIndex[col.Name] = int32(colIdx)
	}
}

func (builder *QueryBuilder) appendNodesForReplaceStmt(
	bindCtx *BindContext,
	lastNodeID int32,
	tableDef *TableDef,
	objRef *ObjectRef,
	insertColToExpr map[string]*Expr,
) (int32, map[string]int32, []bool, error) {
	colCount := len(tableDef.Cols)
	colName2Idx := make(map[string]int32, colCount+len(tableDef.Indexes)*2)
	hasAutoCol := false
	for _, col := range tableDef.Cols {
		if col.Typ.AutoIncr {
			hasAutoCol = true
			break
		}
	}

	projList1 := make([]*plan.Expr, 0, colCount-1)
	projList2 := make([]*plan.Expr, 0, colCount-1)
	projTag1 := builder.genNewBindTag()
	preInsertTag := builder.genNewBindTag()

	var (
		compPkeyExpr  *plan.Expr
		clusterByExpr *plan.Expr
	)

	columnIsNull := make(map[string]bool, colCount)
	hasCompClusterBy := tableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name)
	colIdxToProjPos := make(map[int32]int32, colCount)
	genColIdxToProj1Pos := make(map[int]int, colCount)
	genColIdxToProj2Pos := make(map[int]int, colCount)
	generatedColIdxs := make([]int, 0)

	for i, col := range tableDef.Cols {
		if oldExpr, exists := insertColToExpr[col.Name]; exists {
			if !col.Typ.AutoIncr && replaceExprAlwaysStaticNull(oldExpr, builder.qry, 0) {
				columnIsNull[col.Name] = true
			}
			colIdxToProjPos[int32(i)] = int32(len(projList1))
			projList2 = append(projList2, &plan.Expr{
				Typ: oldExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: projTag1,
						ColPos: int32(len(projList1)),
					},
				},
			})
			projList1 = append(projList1, oldExpr)
		} else if col.Name == catalog.Row_ID {
			continue
		} else if col.Name == catalog.CPrimaryKeyColName {
			compPkeyExpr = makeCompPkeyExpr(tableDef, tableDef.Name2ColIndex)
			projList2 = append(projList2, &plan.Expr{
				Typ: compPkeyExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: preInsertTag,
						ColPos: 0,
					},
				},
			})
		} else if hasCompClusterBy && col.Name == tableDef.ClusterBy.Name {
			clusterByExpr = makeClusterByExpr(tableDef, tableDef.Name2ColIndex)
			projList2 = append(projList2, &plan.Expr{
				Typ: clusterByExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: preInsertTag,
						ColPos: 0,
					},
				},
			})
		} else if col.GeneratedCol != nil {
			// MatrixOne currently materializes both STORED and VIRTUAL generated columns on write.
			// Defer them until base/default columns are in projList1 so forward references resolve.
			genColIdxToProj1Pos[i] = len(projList1)
			genColIdxToProj2Pos[i] = len(projList2)
			generatedColIdxs = append(generatedColIdxs, i)
			projList1 = append(projList1, nil)
			projList2 = append(projList2, nil)
		} else {
			defExpr, err := getDefaultExpr(builder.GetContext(), col)
			if err != nil {
				return 0, nil, nil, err
			}

			if !col.Typ.AutoIncr {
				if lit := defExpr.GetLit(); lit != nil {
					if lit.Isnull {
						columnIsNull[col.Name] = true
					}
				}
			}

			colIdxToProjPos[int32(i)] = int32(len(projList1))
			projList2 = append(projList2, &plan.Expr{
				Typ: defExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: projTag1,
						ColPos: int32(len(projList1)),
					},
				},
			})
			projList1 = append(projList1, defExpr)
		}

		colName2Idx[tableDef.Name+"."+col.Name] = int32(i)
	}

	for _, i := range generatedColIdxs {
		col := tableDef.Cols[i]
		genExpr := builder.applyGeneratedColumnAssignmentCast(
			DeepCopyExpr(col.GeneratedCol.Expr),
			false,
		)
		inlineGeneratedColExpr(genExpr, colIdxToProjPos, projList1)
		proj1Pos := genColIdxToProj1Pos[i]
		projList1[proj1Pos] = genExpr
		pos := int32(proj1Pos)
		colIdxToProjPos[int32(i)] = pos
		projList2[genColIdxToProj2Pos[i]] = &plan.Expr{
			Typ: genExpr.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: projTag1,
					ColPos: pos,
				},
			},
		}
	}

	validIndexes, _ := getValidIndexes(tableDef)
	tableDef.Indexes = validIndexes

	skipUniqueIdx := make([]bool, len(tableDef.Indexes))
	pkName := tableDef.Pkey.PkeyColName
	pkPos := tableDef.Name2ColIndex[pkName]
	for i, idxDef := range tableDef.Indexes {
		// A unique index encodes its key with serial(...), which is NULL as soon as ANY
		// part is NULL; such a key never conflicts (MySQL: NULL never conflicts on a
		// unique key) and is never stored in the index table. Skip it when ANY part is
		// statically NULL, not only when every part is. Non-unique indexes use
		// serial_full (NULL preserved) and are never skipped here.
		skipUniqueIdx[i] = false
		if idxDef.Unique {
			for _, part := range idxDef.Parts {
				if columnIsNull[catalog.ResolveAlias(part)] {
					skipUniqueIdx[i] = true
					break
				}
			}
		}

		idxTableName := idxDef.IndexTableName
		colName2Idx[idxTableName+"."+catalog.IndexTablePrimaryColName] = pkPos
		prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
		if err != nil {
			return 0, nil, nil, err
		}
		if !indexTableStoresSerializedKey(idxDef) {
			partName := indexPrimaryPartName(idxDef)
			partPos := colName2Idx[tableDef.Name+"."+partName]
			if prefixLengths[partName] > 0 {
				idxExpr, err := builder.makeIndexPartExprFromInputExpr(projList2[partPos], partName, prefixLengths)
				if err != nil {
					return 0, nil, nil, err
				}
				colName2Idx[idxTableName+"."+catalog.IndexTableIndexColName] = int32(len(projList2))
				projList2 = append(projList2, idxExpr)
			} else {
				colName2Idx[idxTableName+"."+catalog.IndexTableIndexColName] = partPos
			}
		} else {
			argsLen := len(idxDef.Parts)
			args := make([]*plan.Expr, argsLen)

			var colPos int32
			var ok bool
			for k := 0; k < argsLen; k++ {
				if colPos, ok = colName2Idx[tableDef.Name+"."+catalog.ResolveAlias(idxDef.Parts[k])]; !ok {
					errMsg := fmt.Sprintf("bind insert err, can not find colName = %s", idxDef.Parts[k])
					return 0, nil, nil, moerr.NewInternalError(builder.GetContext(), errMsg)
				}
				partName := catalog.ResolveAlias(idxDef.Parts[k])
				if prefixLengths[partName] > 0 {
					args[k], err = builder.makeIndexPartExprFromInputExpr(projList2[colPos], partName, prefixLengths)
					if err != nil {
						return 0, nil, nil, err
					}
				} else {
					args[k] = DeepCopyExpr(projList2[colPos])
				}
			}

			funcName := "serial"
			if !idxDef.Unique {
				funcName = "serial_full"
			}
			idxExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, args)
			colName2Idx[idxTableName+"."+catalog.IndexTableIndexColName] = int32(len(projList2))
			projList2 = append(projList2, idxExpr)
		}
	}

	tmpCtx := NewBindContext(builder, bindCtx)
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: projList1,
		Children:    []int32{lastNodeID},
		BindingTags: []int32{projTag1},
	}, tmpCtx)

	if hasAutoCol || compPkeyExpr != nil || clusterByExpr != nil {
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_PRE_INSERT,
			Children: []int32{lastNodeID},
			PreInsertCtx: &plan.PreInsertCtx{
				Ref:           objRef,
				TableDef:      tableDef,
				HasAutoCol:    hasAutoCol,
				CompPkeyExpr:  compPkeyExpr,
				ClusterByExpr: clusterByExpr,
			},
			BindingTags: []int32{preInsertTag},
		}, tmpCtx)
	}

	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: projList2,
		Children:    []int32{lastNodeID},
		BindingTags: []int32{builder.genNewBindTag()},
	}, tmpCtx)

	return lastNodeID, colName2Idx, skipUniqueIdx, nil
}

func replaceExprAlwaysStaticNull(expr *plan.Expr, query *plan.Query, depth int) bool {
	if expr == nil || query == nil || depth > 32 {
		return false
	}
	if lit := expr.GetLit(); lit != nil {
		return lit.GetIsnull()
	}
	if colRef := expr.GetCol(); colRef != nil {
		node := replaceNodeByTag(query, colRef.GetRelPos())
		if node == nil {
			return false
		}
		colPos := int(colRef.GetColPos())
		switch node.GetNodeType() {
		case plan.Node_PROJECT:
			if colPos < 0 || colPos >= len(node.GetProjectList()) {
				return false
			}
			return replaceExprAlwaysStaticNull(node.GetProjectList()[colPos], query, depth+1)
		case plan.Node_VALUE_SCAN:
			rowsetData := node.GetRowsetData()
			if rowsetData == nil || colPos < 0 || colPos >= len(rowsetData.GetCols()) {
				return false
			}
			colData := rowsetData.GetCols()[colPos]
			if colData == nil || len(colData.GetData()) == 0 {
				return false
			}
			for _, rowExpr := range colData.GetData() {
				if rowExpr == nil || !replaceExprAlwaysStaticNull(rowExpr.GetExpr(), query, depth+1) {
					return false
				}
			}
			return true
		}
	}
	if fn := expr.GetF(); fn != nil {
		args := fn.GetArgs()
		if len(args) == 1 && replaceFunctionPreservesNull(fn) {
			return replaceExprAlwaysStaticNull(args[0], query, depth+1)
		}
	}
	return false
}

func replaceNodeByTag(query *plan.Query, tag int32) *plan.Node {
	for _, node := range query.GetNodes() {
		for _, bindingTag := range node.GetBindingTags() {
			if bindingTag == tag {
				return node
			}
		}
	}
	return nil
}

func replaceFunctionPreservesNull(fn *plan.Function) bool {
	if fn == nil || fn.GetFunc() == nil {
		return false
	}
	return strings.Contains(strings.ToLower(fn.GetFunc().GetObjName()), "cast")
}
