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
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
	targetDB := string(stmt.TargetDatabaseName)
	targetTable := string(stmt.TargetTableName)
	if targetTable == "" {
		target := stmt.Table.(*tree.TableName)
		targetDB = string(target.SchemaName)
		targetTable = string(target.ObjectName)
	}
	if targetDB == "" {
		targetDB = builder.compCtx.DefaultDatabase()
	}
	if err = validateInsertColumnQualifiers(
		builder.GetContext(), stmt.ColumnNames, targetDB, targetTable, builder.compCtx.GetLowerCaseTableNames(),
	); err != nil {
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

	return builder.appendDedupAndMultiUpdateNodesForBindReplace(bindCtx, dmlCtx, lastNodeID, colName2Idx, skipUniqueIdx, irregularIndexes)
}

func (builder *QueryBuilder) appendReplaceConflictLookup(
	bindCtx *BindContext,
	lastNodeID int32,
	selectTag int32,
	fullProjTag int32,
	selectNode *plan.Node,
	objRef *plan.ObjectRef,
	tableDef *plan.TableDef,
	idxObjRefs []*plan.ObjectRef,
	idxTableDefs []*plan.TableDef,
	colName2Idx map[string]int32,
	skipUniqueIdx []bool,
	oldColName2Idx map[string][2]int32,
	needsOldIndexMaintenance bool,
) (int32, []*plan.Expr, error) {
	branchCount := 0
	if tableDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
		branchCount++
	}
	for i, idxDef := range tableDef.Indexes {
		if idxDef.Unique && !skipUniqueIdx[i] {
			branchCount++
		}
	}

	replacementColumnCount := len(selectNode.ProjectList)
	var sourceOrdinalType plan.Type
	sourceOrdinalPos := int32(-1)
	sourceStep := int32(-1)
	if branchCount > 1 {
		// Every lookup branch is a LEFT JOIN so an insert-only source row survives,
		// but the same source/old-row pair may be found by several constraints.
		// Carry a per-input ordinal through UNION DISTINCT: it removes only those
		// duplicate candidates, without collapsing equal source rows that the
		// downstream keep-last logic must still see.
		rowNumberFunc, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "row_number", nil)
		if err != nil {
			return 0, nil, err
		}
		sourceOrdinalType = rowNumberFunc.Typ
		ordinalTag := builder.genNewBindTag()
		windowID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_WINDOW,
			Children: []int32{lastNodeID},
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
			WindowIdx:   0,
			BindingTags: []int32{ordinalTag},
		}, bindCtx)

		sourceTag := builder.genNewBindTag()
		sourceProjection := make([]*plan.Expr, 0, replacementColumnCount+1)
		for i, expr := range selectNode.ProjectList {
			sourceProjection = append(sourceProjection, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: selectTag,
					ColPos: int32(i),
				}},
			})
		}
		sourceOrdinalPos = int32(len(sourceProjection))
		sourceProjection = append(sourceProjection, &plan.Expr{
			Typ: rowNumberFunc.Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: ordinalTag,
				ColPos: 0,
			}},
		})
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{windowID},
			ProjectList: sourceProjection,
			BindingTags: []int32{sourceTag},
		}, bindCtx)
		selectTag = sourceTag

		sourceSinkID := appendSinkNode(builder, bindCtx, lastNodeID)
		sourceStep = builder.appendStep(sourceSinkID)
	}
	newSource := func() (int32, int32) {
		if sourceStep < 0 {
			return lastNodeID, selectTag
		}
		tag := builder.genNewBindTag()
		nodeID := appendSinkScanNode(builder, bindCtx, sourceStep)
		builder.qry.Nodes[nodeID].BindingTags = []int32{tag}
		return nodeID, tag
	}
	newMainScan := func() (int32, int32) {
		tag := builder.genNewBindTag()
		builder.addNameByColRef(tag, tableDef)
		nodeID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     CloneTableDefForPlan(tableDef, true),
			ObjRef:       objRef,
			BindingTags:  []int32{tag},
			ScanSnapshot: bindCtx.snapshot,
		}, bindCtx)
		return nodeID, tag
	}

	ordinalOutputPos := int32(-1)
	buildProjection := func(sourceTag, oldScanTag int32) ([]*plan.Expr, error) {
		projection := make([]*plan.Expr, 0, replacementColumnCount+len(tableDef.Cols)+len(tableDef.Indexes)+1)
		for i, expr := range selectNode.ProjectList[:replacementColumnCount] {
			projection = append(projection, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: sourceTag,
					ColPos: int32(i),
				}},
			})
		}
		for i, col := range tableDef.Cols {
			oldColName2Idx[tableDef.Name+"."+col.Name] = [2]int32{fullProjTag, int32(len(projection))}
			projection = append(projection, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: oldScanTag,
					ColPos: int32(i),
				}},
			})
		}

		for i, idxDef := range tableDef.Indexes {
			if skipUniqueIdx[i] && !needsOldIndexMaintenance {
				continue
			}
			prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
			if err != nil {
				return nil, err
			}
			oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTablePrimaryColName] =
				oldColName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]

			if !indexTableStoresSerializedKey(idxDef) {
				partName := indexPrimaryPartName(idxDef)
				if prefixLengths[partName] == 0 {
					oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName] =
						oldColName2Idx[tableDef.Name+"."+partName]
					continue
				}
				colIdx := tableDef.Name2ColIndex[partName]
				partExpr := &plan.Expr{
					Typ: tableDef.Cols[colIdx].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: oldScanTag,
						ColPos: colIdx,
					}},
				}
				idxExpr, err := builder.makeIndexPartExprFromInputExpr(partExpr, partName, prefixLengths)
				if err != nil {
					return nil, err
				}
				oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName] =
					[2]int32{fullProjTag, int32(len(projection))}
				projection = append(projection, idxExpr)
				continue
			}

			args := make([]*plan.Expr, len(idxDef.Parts))
			for j, part := range idxDef.Parts {
				partName := catalog.ResolveAlias(part)
				colIdx := tableDef.Name2ColIndex[partName]
				args[j] = &plan.Expr{
					Typ: tableDef.Cols[colIdx].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: oldScanTag,
						ColPos: colIdx,
					}},
				}
				if prefixLengths[partName] > 0 {
					args[j], err = builder.makeIndexPartExprFromInputExpr(args[j], partName, prefixLengths)
					if err != nil {
						return nil, err
					}
				}
			}
			idxExpr := args[0]
			if len(args) > 1 {
				funcName := "serial"
				if !idxDef.Unique {
					funcName = "serial_full"
				}
				idxExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, args)
			}
			oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName] =
				[2]int32{fullProjTag, int32(len(projection))}
			projection = append(projection, idxExpr)
		}
		if sourceOrdinalPos >= 0 {
			ordinalOutputPos = int32(len(projection))
			projection = append(projection, &plan.Expr{
				Typ: sourceOrdinalType,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: sourceTag,
					ColPos: sourceOrdinalPos,
				}},
			})
		}

		return projection, nil
	}

	branchIDs := make([]int32, 0, branchCount)
	branchTags := make([]int32, 0, branchCount)
	appendBranch := func(sourceTag, oldScanNodeID, oldScanTag int32) error {
		branchTag := builder.genNewBindTag()
		projection, err := buildProjection(sourceTag, oldScanTag)
		if err != nil {
			return err
		}
		branchIDs = append(branchIDs, builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{oldScanNodeID},
			ProjectList: projection,
			BindingTags: []int32{branchTag},
		}, bindCtx))
		branchTags = append(branchTags, branchTag)
		return nil
	}

	if tableDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
		sourceNodeID, sourceTag := newSource()
		oldScanNodeID, oldScanTag := newMainScan()
		pkPos := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
		newPkPos := colName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]
		condition, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			{Typ: tableDef.Cols[pkPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: sourceTag, ColPos: newPkPos}}},
			{Typ: tableDef.Cols[pkPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: oldScanTag, ColPos: pkPos}}},
		})
		joinID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{sourceNodeID, oldScanNodeID},
			JoinType: plan.Node_LEFT,
			OnList:   []*plan.Expr{condition},
		}, bindCtx)
		if err := appendBranch(sourceTag, joinID, oldScanTag); err != nil {
			return 0, nil, err
		}
	}

	for i, idxDef := range tableDef.Indexes {
		if !idxDef.Unique || skipUniqueIdx[i] {
			continue
		}
		sourceNodeID, sourceTag := newSource()
		idxTag := builder.genNewBindTag()
		builder.addNameByColRef(idxTag, idxTableDefs[i])
		idxScanID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     idxTableDefs[i],
			ObjRef:       idxObjRefs[i],
			BindingTags:  []int32{idxTag},
			ScanSnapshot: bindCtx.snapshot,
		}, bindCtx)
		newIndexPos, ok := colName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName]
		if !ok {
			return 0, nil, moerr.NewInternalErrorf(builder.GetContext(),
				"bind replace err, can not find new unique index key for %s", idxDef.IndexTableName)
		}
		idxKeyPos := idxTableDefs[i].Name2ColIndex[catalog.IndexTableIndexColName]
		idxKeyTyp := idxTableDefs[i].Cols[idxKeyPos].Typ
		indexCondition, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			{Typ: idxKeyTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: sourceTag, ColPos: newIndexPos}}},
			{Typ: idxKeyTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: idxTag, ColPos: idxKeyPos}}},
		})
		indexJoinID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{sourceNodeID, idxScanID},
			JoinType: plan.Node_LEFT,
			OnList:   []*plan.Expr{indexCondition},
		}, bindCtx)

		oldScanNodeID, oldScanTag := newMainScan()
		idxPrimaryPos := idxTableDefs[i].Name2ColIndex[catalog.IndexTablePrimaryColName]
		oldPkPos := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
		oldPkTyp := tableDef.Cols[oldPkPos].Typ
		mainCondition, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			{Typ: oldPkTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: idxTag, ColPos: idxPrimaryPos}}},
			{Typ: oldPkTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: oldScanTag, ColPos: oldPkPos}}},
		})
		mainJoinID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{indexJoinID, oldScanNodeID},
			JoinType: plan.Node_LEFT,
			OnList:   []*plan.Expr{mainCondition},
		}, bindCtx)
		if err := appendBranch(sourceTag, mainJoinID, oldScanTag); err != nil {
			return 0, nil, err
		}
	}

	if len(branchIDs) == 0 {
		return 0, nil, moerr.NewInternalError(builder.GetContext(), "bind replace err, no conflict lookup branch")
	}
	if len(branchIDs) == 1 {
		builder.qry.Nodes[branchIDs[0]].BindingTags[0] = fullProjTag
		return branchIDs[0], builder.qry.Nodes[branchIDs[0]].ProjectList, nil
	}

	unionID := branchIDs[0]
	unionInputTag := branchTags[0]
	for branchIdx := 1; branchIdx < len(branchIDs); branchIdx++ {
		leftNode := builder.qry.Nodes[unionID]
		rightNode := builder.qry.Nodes[branchIDs[branchIdx]]
		unionProjection := make([]*plan.Expr, len(leftNode.ProjectList))
		for i, expr := range leftNode.ProjectList {
			unionProjection[i] = &plan.Expr{
				Typ: setOperationOutputType(plan.Node_UNION, expr.Typ, rightNode.ProjectList[i].Typ),
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: unionInputTag,
					ColPos: int32(i),
				}},
			}
		}
		unionTag := builder.genNewBindTag()
		unionID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_UNION,
			Children:    []int32{unionID, branchIDs[branchIdx]},
			ProjectList: unionProjection,
			BindingTags: []int32{unionTag},
		}, bindCtx)
		unionInputTag = unionTag
	}

	// UNION DISTINCT is unordered. Restore source order before the DEDUP joins so
	// duplicate VALUES rows retain REPLACE's existing keep-last semantics.
	orderedUnionID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{unionID},
		OrderBy: []*plan.OrderBySpec{{
			Expr: &plan.Expr{
				Typ: sourceOrdinalType,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: unionInputTag,
					ColPos: ordinalOutputPos,
				}},
			},
			Flag: plan.OrderBySpec_ASC | plan.OrderBySpec_INTERNAL,
		}},
		SpillMem: builder.sortSpillMem,
	}, bindCtx)
	finalProjection := make([]*plan.Expr, ordinalOutputPos)
	for i := range finalProjection {
		finalProjection[i] = &plan.Expr{
			Typ: builder.qry.Nodes[unionID].ProjectList[i].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: unionInputTag,
				ColPos: int32(i),
			}},
		}
	}
	finalID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{orderedUnionID},
		ProjectList: finalProjection,
		BindingTags: []int32{fullProjTag},
	}, bindCtx)
	return finalID, finalProjection, nil
}

func (builder *QueryBuilder) appendDedupAndMultiUpdateNodesForBindReplace(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	lastNodeID int32,
	colName2Idx map[string]int32,
	skipUniqueIdx []bool,
	irregularIndexes []*plan.IndexDef,
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
	for i, idxDef := range tableDef.Indexes {
		if skipUniqueIdx[i] && !needsOldIndexMaintenance {
			continue
		}
		idxObjRefs[i], idxTableDefs[i], err = builder.compCtx.ResolveIndexTableByRef(
			objRef, idxDef.IndexTableName, bindCtx.snapshot)
		if err != nil {
			return 0, err
		}
		ensureName2ColIndexForReplace(idxTableDefs[i])
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
	// one incoming row can conflict with different old rows through different keys.
	// Those tables use one equality-only lookup branch per constraint below.
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

		for i, idxDef := range tableDef.Indexes {
			if skipUniqueIdx[i] && !needsOldIndexMaintenance {
				continue
			}

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
		lastNodeID, fullProjList, err = builder.appendReplaceConflictLookup(
			bindCtx,
			lastNodeID,
			selectTag,
			fullProjTag,
			selectNode,
			objRef,
			tableDef,
			idxObjRefs,
			idxTableDefs,
			colName2Idx,
			skipUniqueIdx,
			oldColName2Idx,
			needsOldIndexMaintenance,
		)
		if err != nil {
			return 0, err
		}
	}
	oldMainRowIDPos := oldColName2Idx[tableDef.Name+"."+catalog.Row_ID]
	oldMainPKPos := oldColName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]
	buildParentFKActions := len(tableDef.RefChildTbls) > 0
	if buildParentFKActions {
		enabled, err := IsForeignKeyChecksEnabled(builder.compCtx)
		if err != nil {
			return 0, err
		}
		buildParentFKActions = enabled
	}
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

		oldColName2Idx[idxTableDefs[i].Name+"."+catalog.Row_ID] = [2]int32{idxTag, idxTableDefs[i].Name2ColIndex[catalog.Row_ID]}

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
		oldColName2Idx[idxTableDefs[i].Name+"."+lookupColName] = [2]int32{idxTag, idxTableDefs[i].Name2ColIndex[lookupColName]}

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

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{lastNodeID, idxTableNodeID},
			JoinType: plan.Node_LEFT,
			OnList:   []*plan.Expr{joinCond},
		}, bindCtx)
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
		lastNodeID, err = builder.appendOnDupIrregularMaintSource(
			bindCtx, lastNodeID, finalProjTag, replaceOldPkPos, replaceOldPkTyp,
			-1, -1, -1,
			irregularIndexes, nil, -1, nil, tableDef, objRef)
		if err != nil {
			return 0, err
		}
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
		applyLockTableFallback(builder)
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
		applyLockTableFallback(builder)

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
		err := buildDeletePlans(builder.compCtx, builder, bindCtx, delCtx)
		putDmlPlanCtx(delCtx)
		if err != nil {
			return 0, err
		}

		lastNodeID = appendSinkScanNode(builder, bindCtx, sharedStep)
		builder.qry.Nodes[lastNodeID].BindingTags = []int32{finalProjTag}
	}

	// Self-referencing FK constraint checks are handled by DetectSqls (generated in
	// bindAndOptimizeReplaceQuery) which run after the REPLACE execution to verify
	// that no child rows reference deleted parent rows.

	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:      plan.Node_MULTI_UPDATE,
		Children:      []int32{lastNodeID},
		BindingTags:   []int32{builder.genNewBindTag()},
		UpdateCtxList: updateCtxList,
	}, bindCtx)

	return lastNodeID, nil
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
