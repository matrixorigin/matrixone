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
	"strings"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"go.uber.org/zap"
)

func (builder *QueryBuilder) bindInsert(stmt *tree.Insert, bindCtx *BindContext) (int32, error) {
	dmlCtx := NewDMLContext()
	// Allow FK tables on the modern insert path: bypass the generic FK-table
	// rejection in ResolveTables via IgnoreForeignKey, then enforce parent
	// existence via DetectSqls generated in bindAndOptimizeInsertQuery.
	origCtx := builder.GetContext()
	builder.compCtx.SetContext(context.WithValue(origCtx, defines.IgnoreForeignKey{}, true))
	err := dmlCtx.ResolveTables(builder.compCtx, tree.TableExprs{stmt.Table}, stmt.With, nil, true)
	builder.compCtx.SetContext(origCtx)
	if err != nil {
		return 0, err
	}

	if stmt.IsRestore {
		builder.isRestore = true
		if stmt.IsRestoreByTs {
			builder.isRestoreByTs = true
		}
		oldSnapshot := builder.compCtx.GetSnapshot()
		builder.compCtx.SetSnapshot(&Snapshot{
			Tenant: &plan.SnapshotTenant{
				TenantName: "xxx",
				TenantID:   stmt.FromDataTenantID,
			},
		})
		defer func() {
			builder.compCtx.SetSnapshot(oldSnapshot)
		}()
	}

	// Pass WITH clause from INSERT to SELECT if present
	if stmt.With != nil && stmt.Rows != nil && stmt.Rows.With == nil {
		stmt.Rows.With = stmt.With
	}

	// Tables carrying irregular (IVF/fulltext) indexes stay on the modern path:
	// appendNodesForInsertStmt strips those indexes from the 1:1 dedup+MULTI_UPDATE
	// plan (which only covers the base table and regular indexes), so capture them
	// here before the strip. Their computed 1:N maintenance (tokenize / nearest
	// centroid) is appended after createQuery from the materialized new-row image.
	tableDef := dmlCtx.tableDefs[0]
	irregularIndexes := getIrregularIndexes(tableDef)

	lastNodeID, colName2Idx, skipUniqueIdx, err := builder.initInsertReplaceStmt(bindCtx, stmt.Rows, stmt.Columns, dmlCtx.objRefs[0], dmlCtx.tableDefs[0], false)
	if err != nil {
		return 0, err
	}

	// Materialize the new-row image into a sink so it feeds both the modern main
	// plan and the irregular-index maintenance sub-plans; the dedup path now reads
	// a sink-scan of that image instead of the raw projection.
	lastNodeID = builder.appendIrregularMaintSource(bindCtx, lastNodeID, irregularIndexes, tableDef, dmlCtx.objRefs[0])

	return builder.appendDedupAndMultiUpdateNodesForBindInsert(bindCtx, dmlCtx, lastNodeID, colName2Idx, skipUniqueIdx, stmt.OnDuplicateUpdate)
}

func (builder *QueryBuilder) canSkipDedup(tableDef *plan.TableDef) bool {
	if builder.optimizerHints != nil && builder.optimizerHints.skipDedup == 1 {
		return true
	}

	if builder.qry.LoadTag || builder.isRestore {
		return true
	}

	return catalog.IsSecondaryIndexTable(tableDef.Name)
}

func getValidIndexes(tableDef *plan.TableDef) (indexes []*plan.IndexDef, hasIrregularIndex bool) {
	if tableDef == nil || len(tableDef.Indexes) == 0 {
		return
	}

	for _, idxDef := range tableDef.Indexes {
		if !catalog.IsRegularIndexAlgo(idxDef.IndexAlgo) {
			hasIrregularIndex = true
			continue
		}

		if !idxDef.TableExist {
			continue
		}

		colMap := make(map[string]bool)
		for _, part := range idxDef.Parts {
			colMap[part] = true
		}

		notCoverPk := false
		for _, part := range tableDef.Pkey.Names {
			if !colMap[part] {
				notCoverPk = true
				break
			}
		}

		if notCoverPk {
			indexes = append(indexes, idxDef)
		}
	}

	return
}

// getIrregularIndexes returns the existing irregular (IVF/fulltext/hnsw) index
// definitions of tableDef. These are stripped from the modern dedup+MULTI_UPDATE
// plan (see appendNodesForInsertStmt) and instead maintained by dedicated
// post-createQuery sub-plans built from the materialized new-row image.
func getIrregularIndexes(tableDef *plan.TableDef) []*plan.IndexDef {
	if tableDef == nil || len(tableDef.Indexes) == 0 {
		return nil
	}
	var irregular []*plan.IndexDef
	for _, idxDef := range tableDef.Indexes {
		if idxDef.TableExist && !catalog.IsRegularIndexAlgo(idxDef.IndexAlgo) {
			irregular = append(irregular, idxDef)
		}
	}
	return irregular
}

// appendIrregularMaintSource materializes the modern new-row image (the projList2
// PROJECT produced by appendNodesForInsertStmt: tableDef.Cols order minus Row_ID,
// with auto-increment / composite-pk already assigned by PreInsert) into a SINK
// step. The image must be shared rather than re-derived, because re-running the
// source would assign different auto-increment values, so both the modern main
// plan (base table + regular indexes) and the irregular-index (IVF/fulltext)
// maintenance sub-plans consume sink-scans of the same step.
//
// It records the maintenance context on the builder and returns the node the
// dedup path must consume in place of newRowImageID: a passthrough PROJECT over
// a sink-scan (a PROJECT so the dedup can safely extend its projection list with
// composite unique-index lock keys without mutating the SINK_SCAN). When the
// table has no irregular indexes it is a no-op and returns newRowImageID.
func (builder *QueryBuilder) appendIrregularMaintSource(
	bindCtx *BindContext,
	newRowImageID int32,
	irregularIndexes []*plan.IndexDef,
	tableDef *plan.TableDef,
	objRef *plan.ObjectRef,
) int32 {
	if len(irregularIndexes) == 0 {
		return newRowImageID
	}

	sinkTag := builder.genNewBindTag()
	sinkID := appendSinkNodeWithTag(builder, bindCtx, newRowImageID, sinkTag)
	maintStep := builder.appendStep(sinkID)

	scanID := builder.appendImageSinkScanNode(bindCtx, maintStep, sinkTag, tableDef)

	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{scanID},
		ProjectList: getProjectionByLastNodeWithTag(builder, scanID, projTag),
		BindingTags: []int32{projTag},
	}, bindCtx)

	// Maintenance reads tableDef.Cols by position, so keep the full column set but
	// swap in the irregular indexes that the main plan stripped.
	maintTableDef := *tableDef
	maintTableDef.Indexes = irregularIndexes
	builder.irregularMaintSourceStep = maintStep
	builder.irregularMaintIndexes = irregularIndexes
	builder.irregularMaintTableDef = &maintTableDef
	builder.irregularMaintObjRef = objRef

	return projID
}

// appendImageSinkScanNode builds a SINK_SCAN over the materialized new-row image
// step. Unlike appendSinkScanNodeWithTag it derives TableDef.Cols from the base
// table definition (tableDef.Cols minus Row_ID, matching the projList2 image
// layout) rather than from bindCtx.bindings[0], because on the insert/load bind
// context the first binding describes the source rows, not the new-row image.
func (builder *QueryBuilder) appendImageSinkScanNode(
	bindCtx *BindContext, sourceStep, tag int32, tableDef *plan.TableDef,
) int32 {
	sinkNodeID := builder.qry.Steps[sourceStep]
	projList := getProjectionByLastNodeWithTag(builder, sinkNodeID, tag)

	cols := make([]*ColDef, 0, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		if col.Name == catalog.Row_ID {
			continue
		}
		cols = append(cols, &ColDef{Name: col.Name, Hidden: col.Hidden, Typ: col.Typ})
	}

	scanNode := &plan.Node{
		NodeType:    plan.Node_SINK_SCAN,
		SourceStep:  []int32{sourceStep},
		ProjectList: projList,
		BindingTags: []int32{tag},
		TableDef:    &plan.TableDef{Name: tableDef.Name, Cols: cols},
	}
	return builder.appendNode(scanNode, bindCtx)
}

// buildIrregularIndexMaintenance appends, after createQuery, the synchronous
// maintenance sub-plans for the irregular (IVF/fulltext) indexes recorded by
// appendIrregularMaintSource. Each sub-plan reads the materialized new-row image
// via a sink-scan and reuses the same leaf primitives the legacy insert path uses
// (buildPreInsertMultiTableIndexes for IVF, buildPreInsertFullTextIndex for
// fulltext). The caller is responsible for the subsequent reduceSinkSinkScanNodes
// + tempOptimizeForDML post-processing.
func (builder *QueryBuilder) buildIrregularIndexMaintenance(bindCtx *BindContext) error {
	tableDef := builder.irregularMaintTableDef
	objRef := builder.irregularMaintObjRef
	sourceStep := builder.irregularMaintSourceStep

	multiTableIndexes := make(map[string]*MultiTableIndex)
	for idx, indexdef := range tableDef.Indexes {
		if !indexdef.TableExist {
			continue
		}
		switch {
		case catalog.IsIvfIndexAlgo(indexdef.IndexAlgo):
			if _, ok := multiTableIndexes[indexdef.IndexName]; !ok {
				multiTableIndexes[indexdef.IndexName] = &MultiTableIndex{
					IndexAlgo:       catalog.ToLower(indexdef.IndexAlgo),
					IndexAlgoParams: indexdef.IndexAlgoParams,
					IndexDefs:       make(map[string]*IndexDef),
				}
			}
			multiTableIndexes[indexdef.IndexName].IndexDefs[catalog.ToLower(indexdef.IndexAlgoTableType)] = indexdef
		case catalog.IsFullTextIndexAlgo(indexdef.IndexAlgo):
			if err := buildPreInsertFullTextIndex(nil, builder.compCtx, builder, bindCtx, objRef,
				tableDef, 0, sourceStep, nil, indexdef, idx, nil); err != nil {
				return err
			}
		}
		// hnsw maintenance is not handled here yet (tracked as D4).
	}

	if len(multiTableIndexes) > 0 {
		if err := buildPreInsertMultiTableIndexes(builder.compCtx, builder, bindCtx, objRef,
			tableDef, sourceStep, multiTableIndexes); err != nil {
			return err
		}
	}

	return nil
}

// finishIrregularIndexMaintenance appends the irregular-index maintenance
// sub-plans after createQuery and runs the same post-processing the legacy insert
// path uses (reduceSinkSinkScanNodes + tempOptimizeForDML). It is a no-op when the
// table has no irregular indexes. Shared by the modern INSERT and LOAD paths.
func (builder *QueryBuilder) finishIrregularIndexMaintenance(query *plan.Query, bindCtx *BindContext) error {
	if len(builder.irregularMaintIndexes) == 0 {
		return nil
	}
	if err := builder.buildIrregularIndexMaintenance(bindCtx); err != nil {
		return err
	}
	reduceSinkSinkScanNodes(query)
	builder.tempOptimizeForDML()
	reCheckifNeedLockWholeTable(builder)
	return nil
}

// buildOnDupTargetPkResolution builds the conflict-resolution subgraph for
// real-PK INSERT ... ON DUPLICATE KEY UPDATE so a unique-key conflict updates the
// existing row (MySQL-aligned) instead of raising a duplicate-entry error.
//
// Treating PRIMARY as the 0th index, it LEFT JOINs a primary-key existence probe
// plus every usable unique index, then projects
//
//	target_pk = coalesce(pk_probe, uk1_pri, uk2_pri, ...)
//
// in PK > unique-key definition order. A NULL unique-key value never matches its
// index, so it contributes no candidate (MySQL: NULL never conflicts). The
// returned project re-projects every original incoming column at its original
// position and appends target_pk at the end; the caller rebinds selectTag to it
// and keys the main DEDUP-update join on target_pk. Conflicting rows then carry a
// non-NULL target_pk (UPDATE) while genuinely new rows carry NULL (INSERT) — the
// exact predicate the existing createIfExpr masking already keys on, so the
// per-unique-key FAIL dedup is preserved untouched as in-batch duplicate
// protection (two brand-new rows sharing a new unique-key value still error).
//
// It returns the new top node id, the new select binding tag, and the target_pk
// column position within the new project list.
func (builder *QueryBuilder) buildOnDupTargetPkResolution(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	tableDef *plan.TableDef,
	lastNodeID int32,
	selectTag int32,
	colName2Idx map[string]int32,
	skipUniqueIdx []bool,
) (int32, int32, int32, error) {
	objRef := dmlCtx.objRefs[0]
	pkName := tableDef.Pkey.PkeyColName
	pkColIdx := tableDef.Name2ColIndex[pkName]
	pkTyp := tableDef.Cols[pkColIdx].Typ
	incomingPkPos := colName2Idx[tableDef.Name+"."+pkName]

	selectNode := builder.qry.Nodes[lastNodeID]
	candExprs := make([]*plan.Expr, 0, len(tableDef.Indexes)+1)

	// cand0: primary-key existence probe. A lightweight LEFT JOIN against the main
	// table on the primary key; project the probe's pk (NULL when the row's pk does
	// not yet exist). PK is the highest-priority candidate.
	probeTag := builder.genNewBindTag()
	builder.addNameByColRef(probeTag, tableDef)
	probeScanID := builder.appendNode(&plan.Node{
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     tableDef,
		ObjRef:       objRef,
		BindingTags:  []int32{probeTag},
		ScanSnapshot: bindCtx.snapshot,
	}, bindCtx)

	probeCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
		{Typ: pkTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: probeTag, ColPos: pkColIdx}}},
		{Typ: pkTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectTag, ColPos: incomingPkPos}}},
	})
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{lastNodeID, probeScanID},
		JoinType: plan.Node_LEFT,
		OnList:   []*plan.Expr{probeCond},
	}, bindCtx)
	candExprs = append(candExprs, &plan.Expr{
		Typ:  pkTyp,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: probeTag, ColPos: pkColIdx}},
	})

	// candi: each usable unique index. LEFT JOIN the index table on its index
	// column = the incoming unique-key value; project the index's primary column
	// (the conflicting row's primary key).
	for i, idxDef := range tableDef.Indexes {
		if !idxDef.Unique || skipUniqueIdx[i] {
			continue
		}

		idxObjRef, idxTableDef, err := builder.compCtx.ResolveIndexTableByRef(objRef, idxDef.IndexTableName, bindCtx.snapshot)
		if err != nil {
			return 0, 0, 0, err
		}

		idxTag := builder.genNewBindTag()
		builder.addNameByColRef(idxTag, idxTableDef)
		idxScanID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     idxTableDef,
			ObjRef:       idxObjRef,
			BindingTags:  []int32{idxTag},
			ScanSnapshot: bindCtx.snapshot,
		}, bindCtx)

		idxColPos := idxTableDef.Name2ColIndex[indexLookupColumnName(idxDef)]
		idxColTyp := idxTableDef.Cols[idxColPos].Typ
		priColPos := idxTableDef.Name2ColIndex[catalog.IndexTablePrimaryColName]
		priColTyp := idxTableDef.Cols[priColPos].Typ

		// The incoming unique-key value: the raw column for a single-part index, or
		// the serial composite already materialized in colName2Idx.
		var incomingValPos int32
		if len(idxDef.Parts) == 1 {
			incomingValPos = colName2Idx[tableDef.Name+"."+catalog.ResolveAlias(idxDef.Parts[0])]
		} else {
			incomingValPos = colName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName]
		}

		joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			{Typ: idxColTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: idxTag, ColPos: idxColPos}}},
			{Typ: selectNode.ProjectList[incomingValPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectTag, ColPos: incomingValPos}}},
		})
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{lastNodeID, idxScanID},
			JoinType: plan.Node_LEFT,
			OnList:   []*plan.Expr{joinCond},
		}, bindCtx)
		candExprs = append(candExprs, &plan.Expr{
			Typ:  priColTyp,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: idxTag, ColPos: priColPos}},
		})
	}

	// Re-project every original incoming column at its original position, then
	// append target_pk = coalesce(cand0, cand1, ...). Positions [0, len) are
	// preserved so colName2Idx stays valid; target_pk lands at len.
	newTag := builder.genNewBindTag()
	newProjList := make([]*plan.Expr, 0, len(selectNode.ProjectList)+1)
	for i, expr := range selectNode.ProjectList {
		newProjList = append(newProjList, &plan.Expr{
			Typ:  expr.Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectTag, ColPos: int32(i)}},
		})
	}
	targetPkPos := int32(len(newProjList))

	var targetPkExpr *plan.Expr
	if len(candExprs) == 1 {
		targetPkExpr = candExprs[0]
	} else {
		var err error
		targetPkExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "coalesce", candExprs)
		if err != nil {
			return 0, 0, 0, err
		}
	}
	newProjList = append(newProjList, targetPkExpr)

	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: newProjList,
		Children:    []int32{lastNodeID},
		BindingTags: []int32{newTag},
	}, bindCtx)

	return lastNodeID, newTag, targetPkPos, nil
}

func (builder *QueryBuilder) appendDedupAndMultiUpdateNodesForBindInsert(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	lastNodeID int32,
	colName2Idx map[string]int32,
	skipUniqueIdx []bool,
	astUpdateExprs tree.UpdateExprs,
) (int32, error) {
	tableDef := dmlCtx.tableDefs[0]
	pkName := tableDef.Pkey.PkeyColName
	isFakePK := pkName == catalog.FakePrimaryKeyColName

	// For fake-PK tables (no explicit primary key), the first usable unique
	// index plays the role of the primary key for conflict detection and for
	// carrying ON DUPLICATE KEY UPDATE. firstUniqueIdxPos == -1 means the table
	// has neither a real PK nor any usable unique key.
	firstUniqueIdxPos := -1
	for i, idxDef := range tableDef.Indexes {
		if idxDef.Unique && !skipUniqueIdx[i] {
			firstUniqueIdxPos = i
			break
		}
	}

	// Vector/text index tables (ivfflat/hnsw/cagra/fulltext) carry an algo-specific
	// TableType ("metadata"/"hnsw_meta"/"cagra_meta"/"fulltext", ...) that is neither
	// SystemOrdinaryRel ("r") nor SystemIndexRel ("i"). A plain INSERT into such a
	// table is rejected here and falls back to the legacy builder. However, index
	// maintenance issues an ON DUPLICATE KEY UPDATE against the index *metadata*
	// table (a plain real-PK key/value table, see ddl_index_algo.go), which the
	// legacy ODKU operator used to handle. The legacy ODKU operator has been removed,
	// so let such an ODKU through to the modern dedup+multi-update path: the metadata
	// table is a normal real-PK table that the modern path handles correctly.
	isOnDupUpdate := len(astUpdateExprs) > 0 &&
		!(len(astUpdateExprs) == 1 && astUpdateExprs[0] == nil)
	if !isOnDupUpdate &&
		tableDef.TableType != catalog.SystemOrdinaryRel &&
		tableDef.TableType != catalog.SystemIndexRel {
		return 0, moerr.NewUnsupportedDML(builder.GetContext(), "insert into vector/text index table")
	}

	var (
		err         error
		onDupAction plan.Node_OnDuplicateAction
	)

	selectNode := builder.qry.Nodes[lastNodeID]
	selectTag := selectNode.BindingTags[0]
	scanTag := builder.genNewBindTag()
	updateExprs := make(map[string]*plan.Expr)

	if len(astUpdateExprs) == 0 {
		onDupAction = plan.Node_FAIL
	} else if len(astUpdateExprs) == 1 && astUpdateExprs[0] == nil {
		onDupAction = plan.Node_IGNORE
	} else if isFakePK && firstUniqueIdxPos < 0 {
		// No primary key and no unique key: a duplicate can never occur, so
		// ON DUPLICATE KEY UPDATE degenerates to a plain INSERT.
		onDupAction = plan.Node_FAIL
	} else {
		onDupAction = plan.Node_UPDATE

		binder := NewOndupUpdateBinder(builder.GetContext(), builder, bindCtx, scanTag, selectTag, tableDef)
		var updateExpr *plan.Expr
		for _, astUpdateExpr := range astUpdateExprs {
			colName := astUpdateExpr.Names[0].ColName()
			colDef := tableDef.Cols[tableDef.Name2ColIndex[colName]]
			astExpr := astUpdateExpr.Expr

			if colDef.GeneratedCol != nil {
				if _, ok := astExpr.(*tree.DefaultVal); ok {
					// Keep generated columns out of the explicit update set.
					// They are recomputed below from the final row image.
					continue
				}
				return 0, moerr.NewInvalidInputf(builder.compCtx.GetContext(),
					"the value specified for generated column '%s' in table '%s' is not allowed",
					colName, tableDef.Name)
			}

			if _, ok := astExpr.(*tree.DefaultVal); ok {
				if colDef.Typ.AutoIncr {
					return 0, moerr.NewUnsupportedDML(builder.compCtx.GetContext(), "auto_increment default value")
				}

				updateExpr, err = getDefaultExpr(builder.GetContext(), colDef)
				if err != nil {
					return 0, err
				}
			} else {
				updateExpr, err = binder.BindExpr(astExpr, 0, true)
				if err != nil {
					return 0, err
				}
			}

			updateExpr, err = forceCastExpr(builder.GetContext(), updateExpr, colDef.Typ)
			if err != nil {
				return 0, err
			}
			updateExprs[colDef.Name] = updateExpr
		}

		for _, col := range tableDef.Cols {
			if col.OnUpdate != nil && col.OnUpdate.Expr != nil && updateExprs[col.Name] == nil {
				newDefExpr := DeepCopyExpr(col.OnUpdate.Expr)
				err = replaceFuncId(builder.GetContext(), newDefExpr)
				if err != nil {
					return 0, err
				}

				updateExprs[col.Name] = newDefExpr
			}
		}

		// Recompute generated columns from the final updated row image, so
		// ON DUPLICATE KEY UPDATE stays consistent with regular UPDATE behavior.
		finalRowExprs := make([]*plan.Expr, len(tableDef.Cols))
		for i, col := range tableDef.Cols {
			if expr, ok := updateExprs[col.Name]; ok {
				finalRowExprs[i] = expr
				continue
			}
			finalRowExprs[i] = &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanTag,
						ColPos: int32(i),
						Name:   col.Name,
					},
				},
			}
		}
		for i, col := range tableDef.Cols {
			if col.GeneratedCol == nil {
				continue
			}
			genExpr := substituteColRefsInExpr(col.GeneratedCol.Expr, finalRowExprs, 0)
			finalRowExprs[i] = genExpr
			updateExprs[col.Name] = genExpr
		}
	}

	for _, part := range tableDef.Pkey.Names {
		if _, ok := updateExprs[part]; ok {
			// Generated columns are auto-recomputed, not explicitly updated by the user.
			// Allow them in PK even though they appear in updateExprs.
			if idx, exists := tableDef.Name2ColIndex[part]; exists && tableDef.Cols[idx].GeneratedCol != nil {
				continue
			}
			return 0, moerr.NewUnsupportedDML(builder.GetContext(), "update primary key on duplicate")
		}
	}

	idxNeedUpdate := make([]bool, len(tableDef.Indexes))
	for i, idxDef := range tableDef.Indexes {
		for _, part := range idxDef.Parts {
			resolved := catalog.ResolveAlias(part)
			if _, ok := updateExprs[resolved]; ok {
				// Skip generated columns in unique key check (auto-recomputed, not user-set)
				if idx, exists := tableDef.Name2ColIndex[resolved]; exists && tableDef.Cols[idx].GeneratedCol != nil {
					continue
				}
				if idxDef.Unique {
					return 0, moerr.NewUnsupportedDML(builder.GetContext(), "update unique key on duplicate")
				} else {
					idxNeedUpdate[i] = true
					break
				}
			}
		}
	}

	// Materialize lock keys for composite unique indexes in advance.
	// This guarantees the lock target can find __mo_index_idx_col in colName2Idx.
	for i, idxDef := range tableDef.Indexes {
		if !idxDef.Unique || skipUniqueIdx[i] || len(idxDef.Parts) <= 1 {
			continue
		}
		lockColName := idxDef.IndexTableName + "." + catalog.IndexTableIndexColName
		if _, ok := colName2Idx[lockColName]; ok {
			continue
		}
		args := make([]*plan.Expr, len(idxDef.Parts))
		for k := range idxDef.Parts {
			partName := catalog.ResolveAlias(idxDef.Parts[k])
			partPos, ok := colName2Idx[tableDef.Name+"."+partName]
			if !ok {
				return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", partName)
			}
			args[k] = DeepCopyExpr(selectNode.ProjectList[partPos])
		}
		lockExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", args)
		colName2Idx[lockColName] = int32(len(selectNode.ProjectList))
		selectNode.ProjectList = append(selectNode.ProjectList, lockExpr)
	}

	// real-PK ON DUPLICATE KEY UPDATE: resolve a single UPDATE target up front so a
	// cross-row unique-key conflict updates the existing row (MySQL-aligned) rather
	// than erroring. The resolved target_pk re-keys the main DEDUP-update join
	// below; the per-unique-key FAIL dedup is kept as in-batch duplicate protection.
	useTargetPk := !isFakePK && onDupAction == plan.Node_UPDATE &&
		firstUniqueIdxPos >= 0 && !builder.canSkipDedup(tableDef)
	targetPkPos := int32(-1)
	if useTargetPk {
		oldSelectTag := selectTag
		lastNodeID, selectTag, targetPkPos, err = builder.buildOnDupTargetPkResolution(
			bindCtx, dmlCtx, tableDef, lastNodeID, selectTag, colName2Idx, skipUniqueIdx)
		if err != nil {
			return 0, err
		}
		selectNode = builder.qry.Nodes[lastNodeID]

		// The update expressions (e.g. VALUES(col)) were bound against the original
		// select tag; the resolution project re-projects every incoming column at
		// its original position under the new tag, so retarget those references.
		for _, updateExpr := range updateExprs {
			replaceColRefTag(updateExpr, oldSelectTag, selectTag)
		}
	}

	objRef := dmlCtx.objRefs[0]
	idxObjRefs := make([]*plan.ObjectRef, len(tableDef.Indexes))
	idxTableDefs := make([]*plan.TableDef, len(tableDef.Indexes))

	//lock main table
	lockTargets := make([]*plan.LockTarget, 0, len(tableDef.Indexes)+1)
	for _, col := range tableDef.Cols {
		if col.Name == pkName && pkName != catalog.FakePrimaryKeyColName {
			lockTarget := &plan.LockTarget{
				TableId:            tableDef.TblId,
				ObjRef:             DeepCopyObjectRef(objRef),
				PrimaryColIdxInBat: colName2Idx[tableDef.Name+"."+col.Name],
				PrimaryColRelPos:   selectTag,
				PrimaryColTyp:      col.Typ,
			}
			lockTargets = append(lockTargets, lockTarget)
			break
		}
	}
	// lock unique key table
	for i, idxDef := range tableDef.Indexes {
		if !idxDef.Unique || skipUniqueIdx[i] {
			continue
		}
		idxObjRef, idxTableDef, err := builder.compCtx.ResolveIndexTableByRef(dmlCtx.objRefs[0], idxDef.IndexTableName, bindCtx.snapshot)
		if err != nil {
			return 0, err
		}
		var pkIdxInBat int32

		if len(idxDef.Parts) == 1 {
			var ok bool
			pkIdxInBat, ok = colName2Idx[tableDef.Name+"."+idxDef.Parts[0]]
			if !ok {
				return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", idxDef.Parts[0])
			}
		} else {
			lockColName := idxDef.IndexTableName + "." + catalog.IndexTableIndexColName
			var ok bool
			pkIdxInBat, ok = colName2Idx[lockColName]
			if !ok {
				return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", lockColName)
			}
		}
		lockTarget := &plan.LockTarget{
			TableId:            idxTableDef.TblId,
			ObjRef:             idxObjRef,
			PrimaryColIdxInBat: pkIdxInBat,
			PrimaryColRelPos:   selectTag,
			PrimaryColTyp:      selectNode.ProjectList[int(pkIdxInBat)].Typ,
		}
		lockTargets = append(lockTargets, lockTarget)
	}
	if len(lockTargets) > 0 {
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_LOCK_OP,
			Children:    []int32{lastNodeID},
			TableDef:    tableDef,
			BindingTags: []int32{builder.genNewBindTag()},
			LockTargets: lockTargets,
		}, bindCtx)
		reCheckifNeedLockWholeTable(builder)
	}

	/*
		say, we have a table x with schema `(a int, b int, c int, d int, e int, primary key (a, b), key idxb (b), key idxc(c), unique key idxd (d))`
		and inserted rows are:
		1 2 3 4 5
		10 20 30 40 50
		100 200 300 400 500

		and then we execute the following statement:
		explain phyplan verbose insert into x values (10,20,300,400,500), (3,3,7,7,7),(4,4,8,8,8),(1,2,51,52,53) on duplicate key update c = values(c);

		after pk dedup update, the output of the dedup(update) join is like
		scanTag                      selectTag
		---------------------------|---------------------------------
		x.c, x.(a,b), x.__mo_rowid,  v.a, v.b, v.c, v.d, v.e, v.(a,b)

		3, (1,2), rid1,    1,  2,  51,  4,  5,  (1,2)
		10, (10,20), rid2, 10, 20, 300, 40, 50, (10,20)
		null, null, null,  3,  3,  7,   7,  7,  (3,3)
		null, null, null,  4,  4,  8,   8,  8,  (4,4)

		we want to check dedup and insert on column d, using (null, null, 7, 8), instead of (4, 40, 7, 8)

		so, before unique dedup, a new project node is added to include new projections for the index tables.
		for every unique index, we have two projections, for idxd(d), we have:
			- proj1 : __mo_index_pri_col -> x.__mo_rowid
			- proj2 : __mo_index_idx_col -> if(isnull(x.(a,b)), x.d, null)

		also, we want insert on idxb(b) using (null, null, 3, 4), instead of (2, 20, 3, 4)
			- actually, the insert value is (null, null, compose(3,(3,3)), compose(4,(4,4)))
			- this will be done in final project node
	*/

	// Create the IF expression: if scanTag pkPos column is null, use colSpecificExpr, otherwise null
	// where colSpecificExpr has input rows that are not overlap with the target table.
	createIfExpr := func(colSpecificExpr *plan.Expr) *plan.Expr {
		scanPkPos := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
		scanPkTyp := tableDef.Cols[scanPkPos].Typ
		scanPkColExpr := &plan.Expr{
			Typ: scanPkTyp,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanTag,
					ColPos: scanPkPos,
				},
			},
		}

		isNullExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "isnull", []*plan.Expr{scanPkColExpr})

		// Create NULL constant
		nullExpr := &plan.Expr{
			Typ: colSpecificExpr.Typ,
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Isnull: true,
				},
			},
		}

		ifExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "if", []*plan.Expr{isNullExpr, colSpecificExpr, nullExpr})
		return ifExpr
	}

	appendedUniqueProjs := make(map[string]*plan.Expr, len(tableDef.Indexes)/2)

	// prepare base unique projections for unique indexes, even dedup is skipped for some cases, like load mode
	for i, idxDef := range tableDef.Indexes {
		if skipUniqueIdx[i] || !idxDef.Unique {
			continue
		}

		// prepare two projections for the unique index: `__mo_index_idx_col` and `__mo_index_pri_col`
		idxTableName := idxDef.IndexTableName
		idxPriColName := idxTableName + "." + catalog.IndexTablePrimaryColName
		idxIdxColName := idxTableName + "." + catalog.IndexTableIndexColName
		pkPos := colName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]

		// __mo_index_pri_col projection for primary key
		idxPrimaryColExpr := &plan.Expr{
			Typ: selectNode.ProjectList[pkPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: selectTag,
					ColPos: pkPos,
				},
			},
		}
		appendedUniqueProjs[idxPriColName] = idxPrimaryColExpr

		// __mo_index_idx_col projection for index columns
		argsLen := len(idxDef.Parts)
		var idxIndexColExpr *plan.Expr
		if argsLen == 1 {
			colFullName := tableDef.Name + "." + idxDef.Parts[0]
			idxIndexColExpr = &plan.Expr{
				Typ: selectNode.ProjectList[colName2Idx[colFullName]].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectTag,
						ColPos: colName2Idx[colFullName],
					},
				},
			}
		} else {
			args := make([]*plan.Expr, argsLen)
			var colPos int32
			var ok bool
			for k := range argsLen {
				if colPos, ok = colName2Idx[tableDef.Name+"."+catalog.ResolveAlias(idxDef.Parts[k])]; !ok {
					return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", idxDef.Parts[k])
				}
				args[k] = &plan.Expr{
					Typ: selectNode.ProjectList[colPos].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: selectTag,
							ColPos: colPos,
						},
					},
				}
			}

			idxIndexColExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", args)
		}
		appendedUniqueProjs[idxIdxColName] = idxIndexColExpr
	}

	// handle primary/unique key confliction
	//
	// ON DUPLICATE KEY UPDATE always needs the dedup-update join to fetch the old
	// row image, even on tables canSkipDedup would otherwise skip (e.g. the index
	// metadata table, a secondary-index-named real-PK table that index maintenance
	// upserts a version counter into). Plain index-maintenance inserts (onDupAction
	// != UPDATE) keep skipping dedup as before.
	if builder.canSkipDedup(tableDef) && onDupAction != plan.Node_UPDATE {
		// load do not handle primary/unique key confliction
		for i, idxDef := range tableDef.Indexes {
			if skipUniqueIdx[i] {
				continue
			}

			idxObjRefs[i], idxTableDefs[i], err = builder.compCtx.ResolveIndexTableByRef(objRef, idxDef.IndexTableName, bindCtx.snapshot)
			if err != nil {
				return 0, err
			}
		}
	} else {

		var (
			option             *plan.AlterCopyOpt
			skipPkDedup        bool
			skipUniqueIdxDedup map[string]bool
		)

		if v := builder.compCtx.GetContext().Value(defines.AlterCopyOpt{}); v != nil {
			option = v.(*plan.AlterCopyOpt)
			if option.TargetTableName == tableDef.Name {
				logutil.Info("alter copy dedup exec",
					zap.String("tableDef", tableDef.Name),
					zap.Any("option", option),
				)
				skipPkDedup = option.SkipPkDedup
				skipUniqueIdxDedup = option.SkipUniqueIdxDedup
			}
		}

		// dedup#1:handle pk dedup
		//
		// For real-PK tables this de-duplicates on the primary key. For fake-PK
		// tables that carry ON DUPLICATE KEY UPDATE, the first usable unique
		// index plays the PK role here (main-table scan + unique-key join), so
		// the dedup-update join can read the full old row image from the main
		// table. That unique index is then skipped in the unique-key dedup loop
		// below (its conflict is already handled as an UPDATE here, not a FAIL).
		pkRoleIdxPos := -1
		if isFakePK && onDupAction == plan.Node_UPDATE {
			pkRoleIdxPos = firstUniqueIdxPos
		}

		if !skipPkDedup && (!isFakePK || pkRoleIdxPos >= 0) {
			builder.addNameByColRef(scanTag, tableDef)

			scanNodeID := builder.appendNode(&plan.Node{
				NodeType:     plan.Node_TABLE_SCAN,
				TableDef:     tableDef,
				ObjRef:       objRef,
				BindingTags:  []int32{scanTag},
				ScanSnapshot: bindCtx.snapshot,
			}, bindCtx)

			// Determine the dedup key columns and the join condition.
			//   - real PK: join on the (possibly composite) PK column `pkName`.
			//   - fake PK: join on each part of the chosen unique index.
			var dedupKeyNames []string
			var joinCond *plan.Expr
			if pkRoleIdxPos >= 0 {
				idxDef := tableDef.Indexes[pkRoleIdxPos]
				for _, part := range idxDef.Parts {
					partName := catalog.ResolveAlias(part)
					dedupKeyNames = append(dedupKeyNames, partName)
					partPos := tableDef.Name2ColIndex[partName]
					partTyp := tableDef.Cols[partPos].Typ
					eqExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
						{
							Typ:  partTyp,
							Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: partPos}},
						},
						{
							Typ:  partTyp,
							Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectTag, ColPos: colName2Idx[tableDef.Name+"."+partName]}},
						},
					})
					if joinCond == nil {
						joinCond = eqExpr
					} else {
						joinCond, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*plan.Expr{joinCond, eqExpr})
					}
				}
			} else {
				dedupKeyNames = tableDef.Pkey.Names
				pkPos := tableDef.Name2ColIndex[pkName]
				pkTyp := tableDef.Cols[pkPos].Typ
				// Key the dedup-update join on the resolved target_pk so a cross-row
				// unique-key conflict lands on the existing row's UPDATE; otherwise
				// dedup on the raw incoming primary key as before.
				rightPkPos := colName2Idx[tableDef.Name+"."+pkName]
				if useTargetPk {
					rightPkPos = targetPkPos
				}
				joinCond, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
					{
						Typ:  pkTyp,
						Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: pkPos}},
					},
					{
						Typ:  pkTyp,
						Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectTag, ColPos: rightPkPos}},
					},
				})
			}

			var dedupColName string
			if len(dedupKeyNames) == 1 {
				dedupColName = dedupKeyNames[0]
			} else {
				dedupColName = "(" + strings.Join(dedupKeyNames, ",") + ")"
			}
			dedupColTypes := make([]plan.Type, len(dedupKeyNames))
			for j, part := range dedupKeyNames {
				dedupColTypes[j] = tableDef.Cols[tableDef.Name2ColIndex[part]].Typ
			}

			dedupJoinNode := &plan.Node{
				NodeType:          plan.Node_JOIN,
				Children:          []int32{scanNodeID, lastNodeID},
				JoinType:          plan.Node_DEDUP,
				OnList:            []*plan.Expr{joinCond},
				OnDuplicateAction: onDupAction,
				DedupColName:      dedupColName,
				DedupColTypes:     dedupColTypes,
			}

			if onDupAction == plan.Node_UPDATE {
				oldColList := make([]plan.ColRef, len(tableDef.Cols))
				for i := range tableDef.Cols {
					oldColList[i].RelPos = scanTag
					oldColList[i].ColPos = int32(i)
				}

				updateColIdxList := make([]int32, 0, len(astUpdateExprs))
				updateColExprList := make([]*plan.Expr, 0, len(astUpdateExprs))
				for colName, updateExpr := range updateExprs {
					updateColIdxList = append(updateColIdxList, tableDef.Name2ColIndex[colName])
					updateColExprList = append(updateColExprList, updateExpr)
				}

				dedupJoinNode.DedupJoinCtx = &plan.DedupJoinCtx{
					OldColList:        oldColList,
					UpdateColIdxList:  updateColIdxList,
					UpdateColExprList: updateColExprList,
				}
			}

			lastNodeID = builder.appendNode(dedupJoinNode, bindCtx)
		}

		// dedup#2:handle unique key dedup
		for i, idxDef := range tableDef.Indexes {

			// step 0: skip if necessary
			// input column is null
			if skipUniqueIdx[i] {
				continue
			}

			// we will clone this index data to new index table, skip insert.
			if option != nil && option.SkipIndexesCopy[idxDef.IndexName] {
				continue
			}

			// for the following cases, prepare for inserting data into the index table
			idxObjRefs[i], idxTableDefs[i], err = builder.compCtx.ResolveIndexTableByRef(objRef, idxDef.IndexTableName, bindCtx.snapshot)
			if err != nil {
				return 0, err
			}

			if !idxDef.Unique {
				continue
			}

			// This optimization skips unnecessary unique index deduplication
			// during ALTER TABLE COPY operations, but only after index resolution completes
			// since the copy process depends on the finalized idxTableDefs list
			skipUniqueDedupByAlterCopy := skipUniqueIdxDedup != nil && skipUniqueIdxDedup[idxDef.IndexName]

			idxTableName := idxDef.IndexTableName
			idxPriColName := idxTableName + "." + catalog.IndexTablePrimaryColName
			idxIdxColName := idxTableName + "." + catalog.IndexTableIndexColName

			// if update on duplicate key is disabled, we don't need to create the if expression for the primary key
			// insert all data is ok
			if !skipUniqueDedupByAlterCopy && onDupAction == plan.Node_UPDATE {
				idxPrimaryColExpr := createIfExpr(appendedUniqueProjs[idxPriColName])
				appendedUniqueProjs[idxPriColName] = idxPrimaryColExpr
			}

			// __mo_index_idx_col projection for index columns
			if !skipUniqueDedupByAlterCopy && onDupAction == plan.Node_UPDATE {
				idxIndexColExpr := createIfExpr(appendedUniqueProjs[idxIdxColName])
				appendedUniqueProjs[idxIdxColName] = idxIndexColExpr
			}

			if skipUniqueDedupByAlterCopy {
				continue
			}

			// The unique index acting as the PK role already performs dedup +
			// update via the main-table dedup join above. Its index-table
			// projections (__mo_index_pri_col / __mo_index_idx_col) are still
			// prepared above so MULTI_UPDATE maintains the index table, but we
			// must not add a second (FAIL) dedup join for it here.
			if i == pkRoleIdxPos {
				continue
			}

			// step 2: append unique dedup join on the `__mo_index_idx_col` if expression
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

			rightExpr := DeepCopyExpr(appendedUniqueProjs[idxTableDefs[i].Name+"."+catalog.IndexTableIndexColName])

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

			idxOnDupAction := onDupAction
			if onDupAction == plan.Node_UPDATE {
				idxOnDupAction = plan.Node_FAIL
			}

			lastNodeID = builder.appendNode(&plan.Node{
				NodeType:          plan.Node_JOIN,
				Children:          []int32{idxTableNodeID, lastNodeID},
				JoinType:          plan.Node_DEDUP,
				OnList:            []*plan.Expr{joinCond},
				OnDuplicateAction: idxOnDupAction,
				DedupColName:      dedupColName,
				DedupColTypes:     dedupColTypes,
			}, bindCtx)

		}
	}

	newProjLen := len(selectNode.ProjectList) + len(appendedUniqueProjs)
	for _, idxDef := range tableDef.Indexes {
		if !idxDef.Unique {
			newProjLen++
		}
	}

	if onDupAction == plan.Node_UPDATE {
		newProjLen++
	}

	delColName2Idx := make(map[string][2]int32)

	if newProjLen > len(selectNode.ProjectList) {
		newProjList := make([]*plan.Expr, 0, newProjLen)
		finalProjTag := builder.genNewBindTag()
		pkPos := colName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]

		// input batch columns
		for i, expr := range selectNode.ProjectList {
			newProjList = append(newProjList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectTag,
						ColPos: int32(i),
					},
				},
			})
		}

		// append row_id column for delete rows in main table if having 'on duplicate key update' clause
		// the expr is mapped to the x.__mo_rowid column in the previous example.
		if onDupAction == plan.Node_UPDATE {
			delColName2Idx[tableDef.Name+"."+catalog.Row_ID] = [2]int32{finalProjTag, int32(len(newProjList))}
			rowIDIdx := tableDef.Name2ColIndex[catalog.Row_ID]
			rowIDCol := tableDef.Cols[rowIDIdx]

			newProjList = append(newProjList, &plan.Expr{
				Typ: rowIDCol.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanTag,
						ColPos: rowIDIdx,
					},
				},
			})
		}

		// append projections for secondary index tables
		for i, idxDef := range tableDef.Indexes {
			if idxDef.Unique {
				continue
			}

			// append projections for secondary index tables
			idxTableName := idxDef.IndexTableName

			serialIdxPkExpr := func(idxDef *plan.IndexDef) (*plan.Expr, error) {
				if !indexTableStoresSerializedKey(idxDef) {
					colName := tableDef.Name + "." + indexPrimaryPartName(idxDef)
					colPos, ok := colName2Idx[colName]
					if !ok {
						return nil, moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", indexPrimaryPartName(idxDef))
					}
					return &plan.Expr{
						Typ: selectNode.ProjectList[colPos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: selectTag,
								ColPos: colPos,
							},
						},
					}, nil
				}

				var colPos int32
				args := make([]*plan.Expr, len(idxDef.Parts))
				var ok bool
				for k, part := range idxDef.Parts {
					if colPos, ok = colName2Idx[tableDef.Name+"."+catalog.ResolveAlias(part)]; !ok {
						return nil, moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", part)
					}
					// build from selectTag because we want to insert the row into the index table
					args[k] = &plan.Expr{
						// projectListAfterDedup just appends elements on the tail, so here use selectNode is ok
						Typ: selectNode.ProjectList[colPos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: selectTag,
								ColPos: colPos,
							},
						},
					}
				}
				return BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", args)
			}

			projForAppendAllInputRows := func() error {
				// write all rows
				// append __mo_index_pri_col projection for primary key
				colName2Idx[idxTableName+"."+catalog.IndexTablePrimaryColName] = pkPos
				// append __mo_index_idx_col projection for index columns, normal serial_full expression
				idxExpr, err := serialIdxPkExpr(idxDef)
				if err != nil {
					return err
				}
				colName2Idx[idxTableName+"."+catalog.IndexTableIndexColName] = int32(len(newProjList))
				newProjList = append(newProjList, idxExpr)
				return nil
			}

			if idxNeedUpdate[i] {
				if err := projForAppendAllInputRows(); err != nil {
					return 0, err
				}

				/*
					we have a update clause to change this column, so we need to delete the row from the index table

					same example as above, the idxc(c) will be updated.
					```quote
					after pk dedup update, the output of the dedup(update) join is like
					scanTag                      selectTag
					---------------------------|---------------------------------
					x.c, x.(a,b), x.__mo_rowid,  v.a, v.b, v.c, v.d, v.e, v.(a,b)

					3, (1,2), rid1,    1,  2,  51,  4,  5,  (1,2)
					10, (10,20), rid2, 10, 20, 300, 40, 50, (10,20)
					null, null, null,  3,  3,  7,   7,  7,  (3,3)
					null, null, null,  4,  4,  8,   8,  8,  (4,4)
					```

					this projection is the pk to be deleted
					serial_full(3, (1,2)), serial_full(10, (10,20)), serial_full(null, null), serial_full(null, null)
				*/

				var delIdxExpr *plan.Expr
				if !indexTableStoresSerializedKey(idxDef) {
					colPos, ok := tableDef.Name2ColIndex[indexPrimaryPartName(idxDef)]
					if !ok {
						return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", indexPrimaryPartName(idxDef))
					}
					delIdxExpr = &plan.Expr{
						Typ: selectNode.ProjectList[colPos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: scanTag,
								ColPos: colPos,
							},
						},
					}
				} else {
					delArgs := make([]*plan.Expr, len(idxDef.Parts))

					var colPos int32
					var ok bool
					for k, part := range idxDef.Parts {
						if colPos, ok = tableDef.Name2ColIndex[catalog.ResolveAlias(part)]; !ok {
							return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", part)
						}

						delArgs[k] = &plan.Expr{
							Typ: selectNode.ProjectList[colPos].Typ,
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									// use scanTag because we want to delete the row that is not null.
									RelPos: scanTag,
									ColPos: colPos,
								},
							},
						}
					}

					delIdxExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", delArgs)
				}
				delColName2Idx[idxTableName+"."+catalog.IndexTableIndexColName] = [2]int32{finalProjTag, int32(len(newProjList))}
				newProjList = append(newProjList, delIdxExpr)
				if isSpatialIndexDef(idxDef) {
					oldPkPos := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
					delColName2Idx[idxTableName+"."+catalog.IndexTablePrimaryColName] = [2]int32{finalProjTag, int32(len(newProjList))}
					newProjList = append(newProjList, &plan.Expr{
						Typ: selectNode.ProjectList[oldPkPos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: scanTag,
								ColPos: int32(oldPkPos),
							},
						},
					})
				}
			} else if onDupAction != plan.Node_UPDATE {
				if err := projForAppendAllInputRows(); err != nil {
					return 0, err
				}
			} else {
				// the secondary index columns are not updated, so we need to append the brand new rows into the index table without deleting any rows.
				idxPrimaryColExpr := &plan.Expr{
					Typ: selectNode.ProjectList[pkPos].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: selectTag,
							ColPos: pkPos,
						},
					},
				}
				idxPrimaryColExpr = createIfExpr(idxPrimaryColExpr)
				colName2Idx[idxTableName+"."+catalog.IndexTablePrimaryColName] = int32(len(newProjList))
				newProjList = append(newProjList, idxPrimaryColExpr)

				idxExpr, err := serialIdxPkExpr(idxDef)
				if err != nil {
					return 0, err
				}
				idxExpr = createIfExpr(idxExpr)
				colName2Idx[idxTableName+"."+catalog.IndexTableIndexColName] = int32(len(newProjList))
				newProjList = append(newProjList, idxExpr)
			}
		}

		// append unique index projections
		for colName, expr := range appendedUniqueProjs {
			colName2Idx[colName] = int32(len(newProjList))
			newProjList = append(newProjList, expr)
		}

		selectTag = finalProjTag
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: newProjList,
			Children:    []int32{lastNodeID},
			BindingTags: []int32{selectTag},
		}, bindCtx)
	}

	if onDupAction == plan.Node_UPDATE {
		for i, idxDef := range tableDef.Indexes {
			if !idxNeedUpdate[i] {
				continue
			}

			/*
				In previous step, a delete pk projection has been added to the final project list.
				now, we need to join the index table to fetch the right rowid.
			*/

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

			idxTableName := idxDef.IndexTableName
			delColName2Idx[idxTableName+"."+catalog.Row_ID] = [2]int32{idxTag, idxTableDefs[i].Name2ColIndex[catalog.Row_ID]}
			delPkIdx := delColName2Idx[idxTableName+"."+lookupColName]

			rightExpr := &plan.Expr{
				Typ: pkTyp,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: delPkIdx[0],
						ColPos: delPkIdx[1],
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
	}

	dmlNode := &plan.Node{
		NodeType:    plan.Node_MULTI_UPDATE,
		BindingTags: []int32{builder.genNewBindTag()},
	}

	insertCols := make([]plan.ColRef, len(tableDef.Cols)-1)
	updateCtx := &plan.UpdateCtx{
		ObjRef:     objRef,
		TableDef:   tableDef,
		InsertCols: insertCols,
	}

	for i, col := range tableDef.Cols {
		if col.Name == catalog.Row_ID {
			continue
		}

		insertCols[i].RelPos = selectTag
		insertCols[i].ColPos = colName2Idx[tableDef.Name+"."+col.Name]
	}

	if onDupAction == plan.Node_UPDATE {
		deleteCols := make([]plan.ColRef, 2)
		updateCtx.DeleteCols = deleteCols

		delRowIDIdx := delColName2Idx[tableDef.Name+"."+catalog.Row_ID]
		deleteCols[0].RelPos = delRowIDIdx[0]
		deleteCols[0].ColPos = delRowIDIdx[1]

		deleteCols[1].RelPos = selectTag
		deleteCols[1].ColPos = colName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]
	}

	dmlNode.UpdateCtxList = append(dmlNode.UpdateCtxList, updateCtx)

	for i, idxTableDef := range idxTableDefs {
		if idxTableDef == nil {
			continue
		}

		idxInsertCols := make([]plan.ColRef, len(idxTableDef.Cols)-1)
		updateCtx := &plan.UpdateCtx{
			ObjRef:     idxObjRefs[i],
			TableDef:   idxTableDef,
			InsertCols: idxInsertCols,
		}

		for j, col := range idxTableDef.Cols {
			if col.Name == catalog.Row_ID {
				continue
			}

			idxInsertCols[j].RelPos = selectTag
			idxInsertCols[j].ColPos = int32(colName2Idx[idxTableDef.Name+"."+col.Name])
		}

		if idxNeedUpdate[i] {
			deleteCols := make([]plan.ColRef, 2)
			updateCtx.DeleteCols = deleteCols

			delRowIDIdx := delColName2Idx[idxTableDef.Name+"."+catalog.Row_ID]
			deleteCols[0].RelPos = delRowIDIdx[0]
			deleteCols[0].ColPos = delRowIDIdx[1]

			delPkIdx := delColName2Idx[idxTableDef.Name+"."+indexLookupColumnName(tableDef.Indexes[i])]
			deleteCols[1].RelPos = delPkIdx[0]
			deleteCols[1].ColPos = delPkIdx[1]
		}

		dmlNode.UpdateCtxList = append(dmlNode.UpdateCtxList, updateCtx)
	}

	dmlNode.Children = append(dmlNode.Children, lastNodeID)
	lastNodeID = builder.appendNode(dmlNode, bindCtx)

	return lastNodeID, nil
}

// getInsertColsFromStmt retrieves the list of column names to be inserted into a table
// based on the given INSERT statement and table definition.
// If the INSERT statement does not specify the columns, all columns except the fake primary key column
// will be included in the list.
// If the INSERT statement specifies the columns, it validates the column names against the table definition
// and returns an error if any of the column names are invalid.
// The function returns the list of insert columns and an error, if any.
func (builder *QueryBuilder) getInsertColsFromStmt(astCols tree.IdentifierList, tableDef *TableDef) ([]string, error) {
	var insertColNames []string
	colToIdx := make(map[string]int)
	for i, col := range tableDef.Cols {
		colToIdx[strings.ToLower(col.Name)] = i
	}
	if astCols == nil {
		for _, col := range tableDef.Cols {
			if !col.Hidden && col.GeneratedCol == nil {
				insertColNames = append(insertColNames, col.Name)
			}
		}
	} else {
		for _, column := range astCols {
			colName := strings.ToLower(string(column))
			idx, ok := colToIdx[colName]
			if !ok {
				return nil, moerr.NewBadFieldError(builder.GetContext(), colName, tableDef.Name)
			}
			if tableDef.Cols[idx].GeneratedCol != nil {
				return nil, moerr.NewInvalidInputf(builder.GetContext(), "the value specified for generated column '%s' in table '%s' is not allowed", colName, tableDef.Name)
			}
			insertColNames = append(insertColNames, tableDef.Cols[idx].Name)
		}
	}
	return insertColNames, nil
}

// stripGeneratedDefaultCols removes generated columns from the INSERT column list
// when their corresponding VALUES are all DEFAULT. This supports MySQL-compatible
// syntax: INSERT INTO t(gen_col) VALUES(DEFAULT).
// Non-DEFAULT values for generated columns still produce an error.
func (builder *QueryBuilder) stripGeneratedDefaultCols(astCols tree.IdentifierList, astRows *tree.Select, tableDef *plan.TableDef) (tree.IdentifierList, error) {
	// Find positions of generated columns in the explicit column list
	genPositions := make(map[int]bool)
	for i, col := range astCols {
		colName := strings.ToLower(string(col))
		if idx, ok := tableDef.Name2ColIndex[colName]; ok {
			if tableDef.Cols[idx].GeneratedCol != nil {
				genPositions[i] = true
			}
		}
	}

	if len(genPositions) == 0 {
		return astCols, nil
	}

	// For ValuesClause, validate that all values at generated column positions are DEFAULT
	vc, isValues := astRows.Select.(*tree.ValuesClause)
	if !isValues {
		// For INSERT...SELECT with generated columns in column list, block it
		for pos := range genPositions {
			colName := string(astCols[pos])
			return nil, moerr.NewInvalidInputf(builder.GetContext(),
				"the value specified for generated column '%s' in table '%s' is not allowed",
				colName, tableDef.Name)
		}
	}

	for rowIdx, row := range vc.Rows {
		if row == nil {
			continue // all-defaults row
		}
		for pos := range genPositions {
			if pos >= len(row) {
				return nil, moerr.NewWrongValueCountOnRow(builder.GetContext(), rowIdx+1)
			}
			if _, ok := row[pos].(*tree.DefaultVal); !ok {
				colName := string(astCols[pos])
				return nil, moerr.NewInvalidInputf(builder.GetContext(),
					"the value specified for generated column '%s' in table '%s' is not allowed",
					colName, tableDef.Name)
			}
		}
	}

	// Strip generated columns from column list and values.
	// Build a new ValuesClause to avoid mutating the original AST
	// (important for PREPARE + multiple EXECUTE).
	newCols := make(tree.IdentifierList, 0, len(astCols)-len(genPositions))
	for i, col := range astCols {
		if !genPositions[i] {
			newCols = append(newCols, col)
		}
	}
	newRows := make([]tree.Exprs, len(vc.Rows))
	for j, row := range vc.Rows {
		if row == nil {
			continue
		}
		newRow := make(tree.Exprs, 0, len(row)-len(genPositions))
		for i, val := range row {
			if !genPositions[i] {
				newRow = append(newRow, val)
			}
		}
		newRows[j] = newRow
	}
	newVC := *vc
	newVC.Rows = newRows
	astRows.Select = &newVC

	return newCols, nil
}

func (builder *QueryBuilder) initInsertReplaceStmt(bindCtx *BindContext, astRows *tree.Select, astCols tree.IdentifierList, objRef *plan.ObjectRef, tableDef *plan.TableDef, isReplace bool) (int32, map[string]int32, []bool, error) {
	var (
		lastNodeID int32
		err        error
	)

	// var uniqueCheckOnAutoIncr string
	var insertColumns []string

	// Strip generated columns with DEFAULT values from INSERT column list.
	// MySQL allows INSERT INTO t(gen_col) VALUES(DEFAULT) — silently ignore those columns.
	cleanedCols := astCols
	if astCols != nil {
		cleanedCols, err = builder.stripGeneratedDefaultCols(astCols, astRows, tableDef)
		if err != nil {
			return 0, nil, nil, err
		}
	}

	//var ifInsertFromUniqueColMap map[string]bool
	if insertColumns, err = builder.getInsertColsFromStmt(cleanedCols, tableDef); err != nil {
		return 0, nil, nil, err
	}

	var astSelect *tree.Select
	switch selectImpl := astRows.Select.(type) {
	// rewrite 'insert into tbl values (1,1)' to 'insert into tbl select * from (values row(1,1))'
	case *tree.ValuesClause:
		isAllDefault := false
		if selectImpl.Rows[0] == nil {
			isAllDefault = true
		}
		if isAllDefault {
			for j, row := range selectImpl.Rows {
				if row != nil {
					return 0, nil, nil, moerr.NewWrongValueCountOnRow(builder.GetContext(), j+1)
				}
			}
		} else {
			colCount := len(insertColumns)
			for j, row := range selectImpl.Rows {
				if len(row) != colCount {
					return 0, nil, nil, moerr.NewWrongValueCountOnRow(builder.GetContext(), j+1)
				}
			}
		}

		// example1:insert into a values ();
		// but it does not work at the case:
		// insert into a(a) values (); insert into a values (0),();
		if isAllDefault && astCols != nil {
			return 0, nil, nil, moerr.NewInvalidInput(builder.GetContext(), "insert values does not match the number of columns")
		}
		lastNodeID, err = builder.buildValueScan(isAllDefault, bindCtx, tableDef, selectImpl, insertColumns)
		if err != nil {
			return 0, nil, nil, err
		}

	case *tree.SelectClause:
		astSelect = astRows

		subCtx := NewBindContext(builder, bindCtx)
		lastNodeID, err = builder.bindSelect(astSelect, subCtx, false)
		if err != nil {
			return 0, nil, nil, err
		}
		//ifInsertFromUniqueColMap = make(map[string]bool)

	case *tree.ParenSelect:
		astSelect = selectImpl.Select

		subCtx := NewBindContext(builder, bindCtx)
		lastNodeID, err = builder.bindSelect(astSelect, subCtx, false)
		if err != nil {
			return 0, nil, nil, err
		}
		// ifInsertFromUniqueColMap = make(map[string]bool)

	default:
		return 0, nil, nil, moerr.NewInvalidInput(builder.GetContext(), "insert has unknown select statement")
	}

	if err = builder.addBinding(lastNodeID, tree.AliasClause{Alias: derivedTableName}, bindCtx); err != nil {
		return 0, nil, nil, err
	}

	lastNode := builder.qry.Nodes[lastNodeID]
	if len(insertColumns) != len(lastNode.ProjectList) {
		return 0, nil, nil, moerr.NewInvalidInput(builder.GetContext(), "insert values does not match the number of columns")
	}

	selectTag := lastNode.BindingTags[0]

	insertColToExpr := make(map[string]*plan.Expr)
	for i, column := range insertColumns {
		colIdx := tableDef.Name2ColIndex[column]
		projExpr := &plan.Expr{
			Typ: lastNode.ProjectList[i].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: selectTag,
					ColPos: int32(i),
				},
			},
		}
		if isEnumPlanType(&tableDef.Cols[colIdx].Typ) {
			projExpr, err = funcCastForEnumType(builder.GetContext(), projExpr, tableDef.Cols[colIdx].Typ)
			if err != nil {
				return 0, nil, nil, err
			}
		} else if isSetPlanType(&tableDef.Cols[colIdx].Typ) {
			projExpr, err = funcCastForSetType(builder.GetContext(), projExpr, tableDef.Cols[colIdx].Typ)
			if err != nil {
				return 0, nil, nil, err
			}
		} else if isGeometryPlanType(&tableDef.Cols[colIdx].Typ) {
			projExpr, err = funcCastForGeometryType(builder.GetContext(), projExpr, tableDef.Cols[colIdx].Typ)
			if err != nil {
				return 0, nil, nil, err
			}
		} else {
			projExpr, err = forceCastExpr(builder.GetContext(), projExpr, tableDef.Cols[colIdx].Typ)
			if err != nil {
				return 0, nil, nil, err
			}
		}
		insertColToExpr[column] = projExpr
	}

	if isReplace {
		return builder.appendNodesForReplaceStmt(bindCtx, lastNodeID, tableDef, objRef, insertColToExpr)
	} else {
		return builder.appendNodesForInsertStmt(bindCtx, lastNodeID, tableDef, objRef, insertColToExpr)
	}
}

func (builder *QueryBuilder) appendNodesForInsertStmt(
	bindCtx *BindContext,
	lastNodeID int32,
	tableDef *TableDef,
	objRef *ObjectRef,
	insertColToExpr map[string]*Expr,
) (int32, map[string]int32, []bool, error) {
	colName2Idx := make(map[string]int32)
	hasAutoCol := false
	for _, col := range tableDef.Cols {
		if col.Typ.AutoIncr {
			hasAutoCol = true
			break
		}
	}

	projList1 := make([]*plan.Expr, 0, len(tableDef.Cols)-1)
	projList2 := make([]*plan.Expr, 0, len(tableDef.Cols)-1)
	projTag1 := builder.genNewBindTag()
	preInsertTag := builder.genNewBindTag()

	var (
		compPkeyExpr  *plan.Expr
		clusterByExpr *plan.Expr
	)

	columnIsNull := make(map[string]bool)
	hasCompClusterBy := tableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name)
	colIdxToProjPos := make(map[int32]int32)
	genColIdxToProj1Pos := make(map[int]int)
	genColIdxToProj2Pos := make(map[int]int)
	generatedColIdxs := make([]int, 0)

	for i, col := range tableDef.Cols {
		if oldExpr, exists := insertColToExpr[col.Name]; exists {
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
			//args := make([]*plan.Expr, len(tableDef.Pkey.Names))
			//
			//for k, part := range tableDef.Pkey.Names {
			//	args[k] = DeepCopyExpr(insertColToExpr[part])
			//}
			//
			//compPkeyExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", args)
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
			//names := util.SplitCompositeClusterByColumnName(tableDef.ClusterBy.Name)
			//args := make([]*plan.Expr, len(names))
			//
			//for k, part := range names {
			//	args[k] = DeepCopyExpr(insertColToExpr[part])
			//}
			//
			//clusterByExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", args)
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
		genExpr := DeepCopyExpr(col.GeneratedCol.Expr)
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

	// Skip irregular (vector / fulltext) indexes here; they are maintained
	// asynchronously by cron, the same way REPLACE handles them. This lets
	// tables carrying such indexes use the modern insert path instead of
	// falling back to the legacy one.
	validIndexes, _ := getValidIndexes(tableDef)
	tableDef.Indexes = validIndexes

	skipUniqueIdx := make([]bool, len(tableDef.Indexes))
	for i, idxDef := range tableDef.Indexes {
		if !idxDef.Unique {
			continue
		}

		skipUniqueIdx[i] = true
		for _, part := range idxDef.Parts {
			if !columnIsNull[catalog.ResolveAlias(part)] {
				skipUniqueIdx[i] = false
				break
			}
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

func (builder *QueryBuilder) buildValueScan(
	isAllDefault bool,
	bindCtx *BindContext,
	tableDef *TableDef,
	stmt *tree.ValuesClause,
	colNames []string,
) (int32, error) {
	var err error

	proc := builder.compCtx.GetProcess()
	lastTag := builder.genNewBindTag()
	colCount := len(colNames)
	rowsetData := &plan.RowsetData{
		Cols: make([]*plan.ColData, colCount),
	}
	for i := 0; i < colCount; i++ {
		rowsetData.Cols[i] = new(plan.ColData)
	}
	valueScanTableDef := &plan.TableDef{
		TblId: 0,
		Name:  "",
		Cols:  make([]*plan.ColDef, colCount),
	}
	projectList := make([]*plan.Expr, colCount)

	for i, colName := range colNames {
		col := tableDef.Cols[tableDef.Name2ColIndex[colName]]
		colTyp := makeTypeByPlan2Type(col.Typ)
		targetTyp := &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_T{
				T: &plan.TargetType{},
			},
		}
		var defExpr *plan.Expr
		if isAllDefault {
			defExpr, err := getDefaultExpr(builder.GetContext(), col)
			if err != nil {
				return 0, err
			}
			defExpr, err = forceCastExpr2(builder.GetContext(), defExpr, colTyp, targetTyp)
			if err != nil {
				return 0, err
			}
			rowsetData.Cols[i].Data = make([]*plan.RowsetExpr, len(stmt.Rows))
			for j := range stmt.Rows {
				rowsetData.Cols[i].Data[j] = &plan.RowsetExpr{
					Expr: defExpr,
				}
			}
		} else {
			binder := NewDefaultBinder(builder.GetContext(), nil, nil, col.Typ, nil)
			binder.builder = builder
			for _, r := range stmt.Rows {
				if nv, ok := r[i].(*tree.NumVal); ok && !isEnumOrSetPlanType(&col.Typ) && !isTypedArrayPlanType(&col.Typ) {
					expr, err := MakeInsertValueConstExpr(proc, nv, &colTyp)
					if err != nil {
						return 0, err
					}
					if expr != nil {
						rowsetData.Cols[i].Data = append(rowsetData.Cols[i].Data, &plan.RowsetExpr{
							Expr: expr,
						})
						continue
					}
				}

				if _, ok := r[i].(*tree.DefaultVal); ok {
					defExpr, err = getDefaultExpr(builder.GetContext(), col)
					if err != nil {
						return 0, err
					}
				} else {
					defExpr, err = binder.BindExpr(r[i], 0, true)
					if err != nil {
						return 0, err
					}
					if isEnumPlanType(&col.Typ) {
						defExpr, err = funcCastForEnumType(builder.GetContext(), defExpr, col.Typ)
						if err != nil {
							return 0, err
						}
					} else if isSetPlanType(&col.Typ) {
						defExpr, err = funcCastForSetType(builder.GetContext(), defExpr, col.Typ)
						if err != nil {
							return 0, err
						}
					} else if isGeometryPlanType(&col.Typ) {
						defExpr, err = funcCastForGeometryType(builder.GetContext(), defExpr, col.Typ)
						if err != nil {
							return 0, err
						}
					}
				}
				defExpr, err = forceCastExpr2(builder.GetContext(), defExpr, colTyp, targetTyp)
				if err != nil {
					return 0, err
				}
				rowsetData.Cols[i].Data = append(rowsetData.Cols[i].Data, &plan.RowsetExpr{
					Expr: defExpr,
				})
			}
		}
		colName := fmt.Sprintf("column_%d", i) // like MySQL
		valueScanTableDef.Cols[i] = &plan.ColDef{
			ColId: 0,
			Name:  colName,
			Typ:   col.Typ,
		}
		expr := &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: lastTag,
					ColPos: int32(i),
				},
			},
		}
		projectList[i] = expr
	}

	rowsetData.RowCount = int32(len(stmt.Rows))
	nodeId, _ := uuid.NewV7()
	scanNode := &plan.Node{
		NodeType:    plan.Node_VALUE_SCAN,
		RowsetData:  rowsetData,
		TableDef:    valueScanTableDef,
		BindingTags: []int32{lastTag},
		Uuid:        nodeId[:],
	}
	nodeID := builder.appendNode(scanNode, bindCtx)
	if err = builder.addBinding(nodeID, tree.AliasClause{Alias: "_valuescan"}, bindCtx); err != nil {
		return 0, err
	}

	lastTag = builder.genNewBindTag()
	nodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: projectList,
		Children:    []int32{nodeID},
		BindingTags: []int32{lastTag},
	}, bindCtx)

	return nodeID, nil
}
