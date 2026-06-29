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

	// Capture irregular (IVF/fulltext/master) indexes before appendNodesForInsertStmt
	// strips them from the 1:1 dedup+MULTI_UPDATE plan (which only covers the base
	// table and regular indexes). Their computed 1:N maintenance is appended after
	// createQuery from the materialized new-row image. HNSW/CAGRA/IVF-PQ are cron-
	// maintained and ride the modern path with no inline sub-plan.
	tableDef := dmlCtx.tableDefs[0]

	irregularIndexes := getIrregularIndexes(tableDef)

	lastNodeID, colName2Idx, skipUniqueIdx, err := builder.initInsertReplaceStmt(bindCtx, stmt.Rows, stmt.Columns, dmlCtx.objRefs[0], dmlCtx.tableDefs[0], false)
	if err != nil {
		return 0, err
	}

	// The irregular-index maintenance source is set up inside
	// appendDedupAndMultiUpdateNodesForBindInsert, where the resolved conflict
	// action (plain insert vs ON DUPLICATE KEY UPDATE) is known: plain insert
	// feeds the pre-dedup new-row image; ODKU feeds the post-merge final image
	// plus an old-row image for dropping stale entries.
	return builder.appendDedupAndMultiUpdateNodesForBindInsert(bindCtx, dmlCtx, lastNodeID, colName2Idx, skipUniqueIdx, stmt.OnDuplicateUpdate, irregularIndexes)
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

func (builder *QueryBuilder) makeIndexPartExpr(
	inputTag int32,
	colPos int32,
	colName string,
	colType plan.Type,
	prefixLengths map[string]int,
) (*plan.Expr, error) {
	expr := &plan.Expr{
		Typ: colType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: inputTag,
				ColPos: colPos,
				Name:   colName,
			},
		},
	}

	length := prefixLengths[colName]
	if length <= 0 {
		return expr, nil
	}

	prefixExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "substring", []*plan.Expr{
		expr,
		makePlan2Int64ConstExprWithType(1),
		makePlan2Int64ConstExprWithType(int64(length)),
	})
	if err != nil {
		return nil, err
	}

	if prefixType, ok := indexTableKeyTypeForPrefix(colType); ok {
		prefixExpr, err = appendCastBeforeExpr(builder.GetContext(), prefixExpr, prefixType)
		if err != nil {
			return nil, err
		}
	}
	return prefixExpr, nil
}

func (builder *QueryBuilder) makeIndexPartExprFromInputExpr(
	inputExpr *plan.Expr,
	colName string,
	prefixLengths map[string]int,
) (*plan.Expr, error) {
	expr := DeepCopyExpr(inputExpr)
	length := prefixLengths[colName]
	if length <= 0 {
		return expr, nil
	}

	prefixExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "substring", []*plan.Expr{
		expr,
		makePlan2Int64ConstExprWithType(1),
		makePlan2Int64ConstExprWithType(int64(length)),
	})
	if err != nil {
		return nil, err
	}

	if prefixType, ok := indexTableKeyTypeForPrefix(inputExpr.Typ); ok {
		prefixExpr, err = appendCastBeforeExpr(builder.GetContext(), prefixExpr, prefixType)
		if err != nil {
			return nil, err
		}
	}
	return prefixExpr, nil
}

func (builder *QueryBuilder) makeInsertIndexPartExpr(
	selectNode *plan.Node,
	selectTag int32,
	tableDef *plan.TableDef,
	colName2Idx map[string]int32,
	part string,
	prefixLengths map[string]int,
) (*plan.Expr, error) {
	partName := catalog.ResolveAlias(part)
	colPos, ok := colName2Idx[tableDef.Name+"."+partName]
	if !ok {
		return nil, moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", partName)
	}
	return builder.makeIndexPartExpr(selectTag, colPos, partName, selectNode.ProjectList[colPos].Typ, prefixLengths)
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

// isModernMaintainedIrregularAlgo reports whether an irregular index algo has full
// synchronous modern maintenance (both insert and delete sub-plans). IVF, fulltext,
// and MASTER qualify (the master index table has an independent __mo_index_pri_col
// origin-pk column, so delete joins on origin pk — same pattern as fulltext joins on
// doc_id). HNSW/CAGRA/IVF-PQ are maintained asynchronously by cron (idxcron, off the
// base-table CDC) and need no inline sub-plan.
func isModernMaintainedIrregularAlgo(algo string) bool {
	return catalog.IsIvfIndexAlgo(algo) || catalog.IsFullTextIndexAlgo(algo) ||
		catalog.IsMasterIndexAlgo(algo)
}

// getIrregularIndexes returns the existing IVF/fulltext/master index definitions of
// tableDef. These are stripped from the modern dedup+MULTI_UPDATE plan (see
// appendNodesForInsertStmt) and instead maintained by dedicated post-createQuery
// sub-plans built from the materialized new-row image. HNSW/CAGRA/IVF-PQ are excluded
// because cron maintains them off the base-table CDC.
func getIrregularIndexes(tableDef *plan.TableDef) []*plan.IndexDef {
	if tableDef == nil || len(tableDef.Indexes) == 0 {
		return nil
	}
	var irregular []*plan.IndexDef
	for _, idxDef := range tableDef.Indexes {
		if idxDef.TableExist && isModernMaintainedIrregularAlgo(idxDef.IndexAlgo) {
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
// modernInsertFkCheckEnabled reports whether the modern plain-INSERT path should
// run the row-scoped child→parent foreign-key check: FK checks are enabled and the
// table has at least one non-self-referencing foreign key. Self-referencing FKs are
// still enforced post-execution via genSqlsForCheckFKSelfRefer.
func (builder *QueryBuilder) modernInsertFkCheckEnabled(tableDef *plan.TableDef) (bool, error) {
	hasChildParent := false
	for _, fk := range tableDef.Fkeys {
		if fk.ForeignTbl != 0 {
			hasChildParent = true
			break
		}
	}
	if !hasChildParent {
		return false, nil
	}
	return IsForeignKeyChecksEnabled(builder.compCtx)
}

func (builder *QueryBuilder) appendIrregularMaintSource(
	bindCtx *BindContext,
	newRowImageID int32,
	irregularIndexes []*plan.IndexDef,
	tableDef *plan.TableDef,
	objRef *plan.ObjectRef,
	forceMaterialize bool,
) int32 {
	// forceMaterialize materializes the image even with no irregular indexes, so
	// the row-scoped FK check can read the same new-row image (irregularMaintIndexes
	// is empty, so finishIrregularIndexMaintenance stays a no-op).
	if len(irregularIndexes) == 0 && !forceMaterialize {
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

// appendTaggedSinkScan builds a SINK_SCAN over sourceStep that re-exposes every
// column of the sink under the given tag. Unlike appendImageSinkScanNode it
// derives TableDef.Cols from the sink node's own projection (not a base table
// definition), so it works for the ON DUPLICATE KEY UPDATE final-image sink whose
// column layout is the dedup-update projection rather than tableDef.Cols.
func (builder *QueryBuilder) appendTaggedSinkScan(bindCtx *BindContext, sourceStep, tag int32) int32 {
	sinkNodeID := builder.qry.Steps[sourceStep]
	sinkNode := builder.qry.Nodes[sinkNodeID]
	projList := getProjectionByLastNodeWithTag(builder, sinkNodeID, tag)

	cols := make([]*ColDef, len(sinkNode.ProjectList))
	for i, e := range sinkNode.ProjectList {
		cols[i] = &ColDef{Name: fmt.Sprintf("col_%d", i), Typ: e.Typ}
	}

	scanNode := &plan.Node{
		NodeType:    plan.Node_SINK_SCAN,
		SourceStep:  []int32{sourceStep},
		ProjectList: projList,
		BindingTags: []int32{tag},
		TableDef:    &plan.TableDef{Name: bindCtx.cteName, Cols: cols},
	}
	return builder.appendNode(scanNode, bindCtx)
}

// appendOnDupIrregularMaintSource materializes the ON DUPLICATE KEY UPDATE final
// merged image (the finalProj PROJECT, tagged finalProjTag) into a SINK so that
// the main plan and the irregular-index maintenance can share it without
// re-deriving (the DEDUP-join operator computes the merge at runtime, so the
// merged values are only available downstream of it):
//
//   - the main plan (the idxNeedUpdate joins + MULTI_UPDATE that follow) keeps
//     reading finalProjTag refs via a sink-scan that reuses the same tag;
//   - both the insert maintenance (new entries) and the delete maintenance (drop
//     the old entries of the conflicting rows) read the same materialized step.
//     Its leading columns are the base table columns in tableDef.Cols order (minus
//     Row_ID), the layout the leaf builders index by PK / indexed-column position.
//     The PK is immutable under ODKU, so deleting by the final-image PK removes
//     exactly the stale entries and is a no-op for non-conflicting rows.
//
// It records the maintenance context on the builder and returns the main-plan
// sink-scan the caller must continue from.
func (builder *QueryBuilder) appendOnDupIrregularMaintSource(
	bindCtx *BindContext,
	finalProjNodeID, finalProjTag, deletePkPos int32, deletePkTyp plan.Type,
	irregularIndexes []*plan.IndexDef,
	tableDef *plan.TableDef,
	objRef *plan.ObjectRef,
) int32 {
	sinkID := appendSinkNodeWithTag(builder, bindCtx, finalProjNodeID, finalProjTag)
	joinStep := builder.appendStep(sinkID)

	maintTableDef := *tableDef
	maintTableDef.Indexes = irregularIndexes
	builder.irregularMaintSourceStep = joinStep
	builder.irregularMaintDeleteStep = joinStep
	builder.irregularMaintDeletePkPos = deletePkPos
	builder.irregularMaintDeletePkTyp = deletePkTyp
	builder.irregularMaintIndexes = irregularIndexes
	builder.irregularMaintTableDef = &maintTableDef
	builder.irregularMaintObjRef = objRef

	return builder.appendTaggedSinkScan(bindCtx, joinStep, finalProjTag)
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

	// ON DUPLICATE KEY UPDATE: drop the conflicting rows' old entries first, before
	// re-inserting the final-image entries, so a deletion keyed by the (immutable)
	// PK does not remove the freshly inserted ones.
	if builder.irregularMaintDeleteStep >= 0 {
		if err := builder.buildIrregularIndexDeleteMaintenance(bindCtx); err != nil {
			return err
		}
	}

	// During a copy-based ALTER TABLE, an irregular index whose columns are not
	// affected by the change is shallow-cloned into the new table (see
	// cloneUnaffectedIndexes in compile/alter.go) rather than rebuilt. The data
	// copy runs as a normal INSERT, so skip sync maintenance for such indexes here
	// — exactly as the regular-index path skips them via SkipIndexesCopy — to avoid
	// inserting every row's entries twice (cloned + rebuilt).
	var alterCopyOpt *plan.AlterCopyOpt
	if v := builder.compCtx.GetContext().Value(defines.AlterCopyOpt{}); v != nil {
		if opt, ok := v.(*plan.AlterCopyOpt); ok && opt.TargetTableName == tableDef.Name {
			alterCopyOpt = opt
		}
	}

	multiTableIndexes := make(map[string]*MultiTableIndex)
	// A multi-column FULLTEXT(a, b) is stored as one IndexDef per column sharing a
	// single index table; buildPreInsertFullTextIndex tokenizes all of the index's
	// columns in one call, so it must run once per index table, not once per
	// column, otherwise every row's entries are inserted twice.
	seenFullTextTbls := make(map[string]bool)
	for idx, indexdef := range tableDef.Indexes {
		if !indexdef.TableExist {
			continue
		}
		if alterCopyOpt != nil && alterCopyOpt.SkipIndexesCopy[indexdef.IndexName] {
			// cloned by the ALTER, not rebuilt by this copy insert
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
			if seenFullTextTbls[indexdef.IndexTableName] {
				continue
			}
			seenFullTextTbls[indexdef.IndexTableName] = true
			if err := buildPreInsertFullTextIndex(nil, builder.compCtx, builder, bindCtx, objRef,
				tableDef, 0, sourceStep, nil, indexdef, idx, nil); err != nil {
				return err
			}
		case catalog.IsMasterIndexAlgo(indexdef.IndexAlgo):
			if err := buildPreInsertMasterIndex(nil, builder.compCtx, builder, bindCtx, objRef,
				tableDef, sourceStep, nil, indexdef, idx); err != nil {
				return err
			}
		}
		// HNSW/CAGRA/IVF-PQ are absent here but on purpose — cron maintains them
		// off the base-table CDC.
	}

	if len(multiTableIndexes) > 0 {
		if err := buildPreInsertMultiTableIndexes(builder.compCtx, builder, bindCtx, objRef,
			tableDef, sourceStep, multiTableIndexes); err != nil {
			return err
		}
	}

	return nil
}

// buildIrregularIndexDeleteMaintenance appends, after createQuery, the sub-plans
// that drop the stale irregular-index entries of the rows a conflict-resolving DML
// (ON DUPLICATE KEY UPDATE / REPLACE) touched. It reads the shared materialized
// step directly (a nested sink over it confuses the post-createQuery stats pass; a
// direct sink-scan resolves the same way the insert maintenance does) and keys the
// delete on the recorded PK position: the immutable PK for ODKU, or the matched
// old row's PK for REPLACE. Non-conflicting rows carry a NULL key and match
// nothing. Both IVF and fulltext use purpose-built deletes that project only the
// index columns into the join output (never the base vector/text), so they are
// robust to the materialized layout.
func (builder *QueryBuilder) buildIrregularIndexDeleteMaintenance(bindCtx *BindContext) error {
	tableDef := builder.irregularMaintTableDef

	// The delete-PK position was recorded against the pre-prune materialized image.
	// createQuery's column pruning can drop unreferenced columns ahead of it and
	// renumber the survivors, so the recorded position may be larger than the
	// post-prune sink. Only when it would index out of range do we translate it
	// through sinkColRef to its surviving position; in range we keep it as-is
	// (an unconditional remap mis-keys the in-range REPLACE delete by one column).
	if builder.sinkColRef != nil {
		sinkNode := builder.qry.Nodes[builder.qry.Steps[builder.irregularMaintDeleteStep]]
		if int(builder.irregularMaintDeletePkPos) >= len(sinkNode.ProjectList) {
			if newPos, ok := builder.sinkColRef[[2]int32{builder.irregularMaintDeleteStep, builder.irregularMaintDeletePkPos}]; ok {
				builder.irregularMaintDeletePkPos = int32(newPos)
			}
		}
	}

	ivfIndexes := make(map[string]*MultiTableIndex)
	// As in the insert path, a multi-column fulltext index is several IndexDefs
	// over one index table; its stale entries must be dropped once, not once per
	// indexed column.
	seenFullTextTbls := make(map[string]bool)
	for _, indexdef := range tableDef.Indexes {
		if !indexdef.TableExist {
			continue
		}
		switch {
		case catalog.IsFullTextIndexAlgo(indexdef.IndexAlgo):
			if seenFullTextTbls[indexdef.IndexTableName] {
				continue
			}
			seenFullTextTbls[indexdef.IndexTableName] = true
			if err := builder.buildIrregularFulltextDeleteByPk(bindCtx, indexdef); err != nil {
				return err
			}
		case catalog.IsIvfIndexAlgo(indexdef.IndexAlgo):
			if _, ok := ivfIndexes[indexdef.IndexName]; !ok {
				ivfIndexes[indexdef.IndexName] = &MultiTableIndex{
					IndexAlgo:       catalog.ToLower(indexdef.IndexAlgo),
					IndexAlgoParams: indexdef.IndexAlgoParams,
					IndexDefs:       make(map[string]*IndexDef),
				}
			}
			ivfIndexes[indexdef.IndexName].IndexDefs[catalog.ToLower(indexdef.IndexAlgoTableType)] = indexdef
		case catalog.IsMasterIndexAlgo(indexdef.IndexAlgo):
			if err := builder.buildIrregularMasterDeleteByPk(bindCtx, indexdef); err != nil {
				return err
			}
		}
	}

	for _, mti := range ivfIndexes {
		if err := builder.buildIrregularIvfDeleteByPk(bindCtx, mti); err != nil {
			return err
		}
	}

	return nil
}

// deletePkColExpr returns the base-table PK column of the materialized maintenance
// step, the key the stale index entries are matched against.
func (builder *QueryBuilder) deletePkColExpr(relPos int32) *plan.Expr {
	return &plan.Expr{
		Typ:  builder.irregularMaintDeletePkTyp,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: relPos, ColPos: builder.irregularMaintDeletePkPos}},
	}
}

// buildIrregularIvfDeleteByPk drops the stale IVF entries keyed by the recorded PK
// position. It projects only the entries row_id + compound pk into the join output
// (the join key is the integer PK, never the vector), so it is robust to the
// materialized layout and never copies the base vector through the hash join.
func (builder *QueryBuilder) buildIrregularIvfDeleteByPk(bindCtx *BindContext, multiTableIndex *MultiTableIndex) error {
	async, err := catalog.IsIndexAsync(multiTableIndex.IndexAlgoParams)
	if err != nil {
		return err
	}
	if async {
		return nil
	}

	objRef := builder.irregularMaintObjRef
	entriesObjRef, entriesTableDef, err := builder.compCtx.ResolveIndexTableByRef(
		objRef, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexTableName, nil)
	if err != nil {
		return err
	}
	if entriesTableDef == nil {
		return moerr.NewNoSuchTable(builder.GetContext(), objRef.SchemaName,
			multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexName)
	}

	// entries scan projecting [row_id, cpk, origin_pk] (positions 0,1,2).
	scanCols := make([]*ColDef, 0, 3)
	scanProj := make([]*plan.Expr, 3)
	var rowidTyp, cpkTyp, orgPkTyp plan.Type
	for _, col := range entriesTableDef.Cols {
		var slot int
		switch col.Name {
		case catalog.Row_ID:
			slot, rowidTyp = 0, col.Typ
		case catalog.CPrimaryKeyColName:
			slot, cpkTyp = 1, col.Typ
		case catalog.SystemSI_IVFFLAT_TblCol_Entries_pk:
			slot, orgPkTyp = 2, col.Typ
		default:
			continue
		}
		scanProj[slot] = &plan.Expr{
			Typ:  col.Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(len(scanCols)), Name: col.Name}},
		}
		scanCols = append(scanCols, col)
	}
	scanTableDef := DeepCopyTableDef(entriesTableDef, false)
	scanTableDef.Cols = scanCols
	ivfScanID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       &plan.Stats{},
		ObjRef:      entriesObjRef,
		TableDef:    scanTableDef,
		ProjectList: scanProj,
	}, bindCtx)

	// direct sink-scan of the shared materialized image (no intermediate sink).
	srcScan := appendSinkScanNode(builder, bindCtx, builder.irregularMaintDeleteStep)

	// join entries (left) with the image (right) on origin_pk == old/final PK.
	cond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
		{Typ: orgPkTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 2}}},
		builder.deletePkColExpr(1),
	})
	if err != nil {
		return err
	}
	joinID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER,
		Children: []int32{ivfScanID, srcScan},
		OnList:   []*plan.Expr{cond},
		ProjectList: []*plan.Expr{
			{Typ: rowidTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: catalog.Row_ID}}},
			{Typ: cpkTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1, Name: catalog.CPrimaryKeyColName}}},
		},
	}, bindCtx)

	delNodeInfo := makeDeleteNodeInfo(builder.compCtx, entriesObjRef, entriesTableDef, 0, false, 1, cpkTyp, false)
	lastID, err := makeOneDeletePlan(builder, bindCtx, joinID, delNodeInfo, false, true, false)
	putDeleteNodeInfo(delNodeInfo)
	if err != nil {
		return err
	}
	builder.appendStep(lastID)
	return nil
}

// buildIrregularFulltextDeleteByPk drops the stale fulltext index rows keyed by the
// recorded PK position (the index rows are keyed by doc_id == base PK). It mirrors
// the IVF delete: join the index table on doc_id == old/final PK and project only
// the index row_id + fake pk into the output.
func (builder *QueryBuilder) buildIrregularFulltextDeleteByPk(bindCtx *BindContext, indexdef *plan.IndexDef) error {
	async, err := catalog.IsIndexAsync(indexdef.IndexAlgoParams)
	if err != nil {
		return err
	}
	if async {
		return nil
	}

	objRef := builder.irregularMaintObjRef
	indexObjRef, indexTableDef, err := builder.compCtx.ResolveIndexTableByRef(objRef, indexdef.IndexTableName, nil)
	if err != nil {
		return err
	}
	if indexTableDef == nil {
		return moerr.NewNoSuchTable(builder.GetContext(), objRef.SchemaName, indexdef.IndexName)
	}

	// index scan projecting [row_id, doc_id, fake_pk] (positions 0,1,2).
	scanCols := make([]*ColDef, 0, 3)
	scanProj := make([]*plan.Expr, 3)
	var rowidTyp, docIdTyp, fakePkTyp plan.Type
	for _, col := range indexTableDef.Cols {
		var slot int
		switch col.Name {
		case catalog.Row_ID:
			slot, rowidTyp = 0, col.Typ
		case catalog.FullTextIndex_TabCol_Id:
			slot, docIdTyp = 1, col.Typ
		case catalog.FakePrimaryKeyColName:
			slot, fakePkTyp = 2, col.Typ
		default:
			continue
		}
		scanProj[slot] = &plan.Expr{
			Typ:  col.Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(len(scanCols)), Name: col.Name}},
		}
		scanCols = append(scanCols, col)
	}
	scanTableDef := DeepCopyTableDef(indexTableDef, false)
	scanTableDef.Cols = scanCols
	idxScanID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       &plan.Stats{},
		ObjRef:      indexObjRef,
		TableDef:    scanTableDef,
		ProjectList: scanProj,
	}, bindCtx)

	srcScan := appendSinkScanNode(builder, bindCtx, builder.irregularMaintDeleteStep)

	// join index (left) with the image (right) on doc_id == old/final PK.
	cond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
		{Typ: docIdTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1}}},
		builder.deletePkColExpr(1),
	})
	if err != nil {
		return err
	}
	joinID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER,
		Children: []int32{idxScanID, srcScan},
		OnList:   []*plan.Expr{cond},
		ProjectList: []*plan.Expr{
			{Typ: rowidTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: catalog.Row_ID}}},
			{Typ: fakePkTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 2, Name: catalog.FakePrimaryKeyColName}}},
		},
	}, bindCtx)

	delNodeInfo := makeDeleteNodeInfo(builder.compCtx, indexObjRef, indexTableDef, 0, false, 1, fakePkTyp, false)
	lastID, err := makeOneDeletePlan(builder, bindCtx, joinID, delNodeInfo, false, true, false)
	putDeleteNodeInfo(delNodeInfo)
	if err != nil {
		return err
	}
	builder.appendStep(lastID)
	return nil
}

// buildIrregularMasterDeleteByPk drops the stale MASTER index rows keyed by the
// recorded base-table PK. The master index table has an independent
// __mo_index_pri_col origin-pk column, so the delete joins directly on
// origin-pk == old/final PK — no re-tokenization needed. Mirrors the fulltext
// delete (join on doc_id) but uses __mo_index_idx_col as the indexed primary key
// for the DELETE plan.
func (builder *QueryBuilder) buildIrregularMasterDeleteByPk(bindCtx *BindContext, indexdef *plan.IndexDef) error {
	objRef := builder.irregularMaintObjRef
	indexObjRef, indexTableDef, err := builder.compCtx.ResolveIndexTableByRef(objRef, indexdef.IndexTableName, nil)
	if err != nil {
		return err
	}
	if indexTableDef == nil {
		return moerr.NewNoSuchTable(builder.GetContext(), objRef.SchemaName, indexdef.IndexName)
	}

	// index scan projecting [row_id, __mo_index_pri_col, __mo_index_idx_col].
	scanCols := make([]*ColDef, 0, 3)
	scanProj := make([]*plan.Expr, 3)
	var rowidTyp, priColTyp, idxColTyp plan.Type
	for _, col := range indexTableDef.Cols {
		var slot int
		switch col.Name {
		case catalog.Row_ID:
			slot, rowidTyp = 0, col.Typ
		case catalog.MasterIndexTablePrimaryColName:
			slot, priColTyp = 1, col.Typ
		case catalog.MasterIndexTableIndexColName:
			slot, idxColTyp = 2, col.Typ
		default:
			continue
		}
		scanProj[slot] = &plan.Expr{
			Typ:  col.Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(len(scanCols)), Name: col.Name}},
		}
		scanCols = append(scanCols, col)
	}
	scanTableDef := DeepCopyTableDef(indexTableDef, false)
	scanTableDef.Cols = scanCols
	idxScanID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       &plan.Stats{},
		ObjRef:      indexObjRef,
		TableDef:    scanTableDef,
		ProjectList: scanProj,
	}, bindCtx)

	srcScan := appendSinkScanNode(builder, bindCtx, builder.irregularMaintDeleteStep)

	// join index (left) with the image (right) on __mo_index_pri_col == old/final PK.
	cond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
		{Typ: priColTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1}}},
		builder.deletePkColExpr(1),
	})
	if err != nil {
		return err
	}
	joinID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER,
		Children: []int32{idxScanID, srcScan},
		OnList:   []*plan.Expr{cond},
		ProjectList: []*plan.Expr{
			{Typ: rowidTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: catalog.Row_ID}}},
			{Typ: idxColTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 2, Name: catalog.MasterIndexTableIndexColName}}},
		},
	}, bindCtx)

	// Master index table has __mo_index_idx_col as PRIMARY KEY, so the delete keys
	// on it directly (pkPos points to the index column in the join output).
	delNodeInfo := makeDeleteNodeInfo(builder.compCtx, indexObjRef, indexTableDef, 0, false, 1, idxColTyp, false)
	lastID, err := makeOneDeletePlan(builder, bindCtx, joinID, delNodeInfo, false, true, false)
	putDeleteNodeInfo(delNodeInfo)
	if err != nil {
		return err
	}
	builder.appendStep(lastID)
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
	if len(builder.irregularMaintIndexes) > 0 {
		if err := builder.buildIrregularIndexMaintenance(bindCtx); err != nil {
			return err
		}
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

		// The incoming unique-key value, matching what the hidden index table
		// stores: for a single-part index the prefix-aware value (a substring for a
		// prefix index like UNIQUE KEY u(col(4)), otherwise the raw column); for a
		// composite index the serial composite already materialized in colName2Idx.
		// Using the raw column for a prefix index would miss conflicts whose stored
		// prefix keys collide (e.g. existing 'abcdxxxx' vs incoming 'abcdyyyy').
		var incomingValExpr *plan.Expr
		if len(idxDef.Parts) == 1 {
			prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
			if err != nil {
				return 0, 0, 0, err
			}
			partName := catalog.ResolveAlias(idxDef.Parts[0])
			incomingValPos := colName2Idx[tableDef.Name+"."+partName]
			incomingValExpr, err = builder.makeIndexPartExpr(selectTag, incomingValPos, partName,
				selectNode.ProjectList[incomingValPos].Typ, prefixLengths)
			if err != nil {
				return 0, 0, 0, err
			}
		} else {
			incomingValPos := colName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName]
			incomingValExpr = &plan.Expr{
				Typ:  selectNode.ProjectList[incomingValPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectTag, ColPos: incomingValPos}},
			}
		}

		joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			{Typ: idxColTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: idxTag, ColPos: idxColPos}}},
			incomingValExpr,
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

// appendModernChildFkMarkOks appends, for every non-self-referencing foreign key, a
// MARK join against the parent table and returns one boolean "ok" expression per FK,
// true when the parent row exists or any FK column is NULL (MATCH SIMPLE). A MARK join
// emits exactly one output row per input row, so — unlike a LEFT join — it never
// multiplies child rows when the referenced parent column is non-unique (e.g. a FK to
// the leading prefix of a composite primary key). The joins are binding-tagged, so the
// result survives the full optimizer and is robust to the appended index-helper
// columns a unique-key child carries. childColPos maps a child FK column name to its
// position under selectTag.
func (builder *QueryBuilder) appendModernChildFkMarkOks(
	bindCtx *BindContext,
	tableDef *plan.TableDef,
	lastNodeID int32,
	selectTag int32,
	childColPos func(colName string) int32,
) (int32, []*plan.Expr, error) {
	selectNode := builder.qry.Nodes[lastNodeID]

	id2name := make(map[uint64]string, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		id2name[col.ColId] = col.Name
	}

	oks := make([]*plan.Expr, 0, len(tableDef.Fkeys))
	for _, fk := range tableDef.Fkeys {
		if fk.ForeignTbl == 0 {
			continue // self-referencing FK handled post-execution via DetectSql
		}
		parentObjRef, parentTableDef, err := builder.compCtx.ResolveById(fk.ForeignTbl, bindCtx.snapshot)
		if err != nil {
			return 0, nil, err
		}
		if parentTableDef == nil {
			return 0, nil, moerr.NewInternalErrorf(builder.GetContext(), "parent table %d not found", fk.ForeignTbl)
		}

		parentTag := builder.genNewBindTag()
		builder.addNameByColRef(parentTag, parentTableDef)
		parentScanID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     parentTableDef,
			ObjRef:       parentObjRef,
			BindingTags:  []int32{parentTag},
			ScanSnapshot: bindCtx.snapshot,
		}, bindCtx)

		parentColId2Pos := make(map[uint64]int, len(parentTableDef.Cols))
		for i, col := range parentTableDef.Cols {
			parentColId2Pos[col.ColId] = i
		}

		joinPreds := make([]*plan.Expr, 0, len(fk.Cols))
		nullConds := make([]*plan.Expr, 0, len(fk.Cols))
		for k, childColId := range fk.Cols {
			childPos := childColPos(id2name[childColId])
			childTyp := selectNode.ProjectList[childPos].Typ
			parentPos := parentColId2Pos[fk.ForeignCols[k]]
			parentTyp := parentTableDef.Cols[parentPos].Typ

			childExpr := &plan.Expr{Typ: childTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectTag, ColPos: int32(childPos)}}}
			parentExpr := &plan.Expr{Typ: parentTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: parentTag, ColPos: int32(parentPos)}}}
			cond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{childExpr, parentExpr})
			if err != nil {
				return 0, nil, err
			}
			joinPreds = append(joinPreds, cond)
			nullCond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "isnull", []*plan.Expr{DeepCopyExpr(childExpr)})
			if err != nil {
				return 0, nil, err
			}
			nullConds = append(nullConds, nullCond)
		}

		markNodeID, markExpr, err := builder.insertMarkJoin(lastNodeID, parentScanID, joinPreds, nil, false, bindCtx)
		if err != nil {
			return 0, nil, err
		}
		lastNodeID = markNodeID

		// Row is valid when the parent exists (MARK marker is TRUE) or any FK column is
		// NULL (constraint not enforced under MATCH SIMPLE).
		ok := markExpr
		for _, nc := range nullConds {
			ok, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*plan.Expr{ok, nc})
			if err != nil {
				return 0, nil, err
			}
		}
		oks = append(oks, ok)
	}

	return lastNodeID, oks, nil
}

// buildModernChildFkAssert enforces the modern plain-INSERT child→parent foreign-key
// check entirely in the data flow: for every row whose foreign key has no matching
// parent the statement fails. It is binding-tagged (so it survives the full optimizer
// and is robust to a unique-key child's appended index-helper columns) and uses MARK
// joins (so a FK to a non-unique parent column never multiplies the inserted rows),
// replacing the post-createQuery appendForeignConstrantPlan over the materialized
// image. A NULL foreign-key value satisfies the constraint (MATCH SIMPLE); a
// self-referencing FK (ForeignTbl == 0) is left to the post-execution DetectSql.
//
// childColPos maps a child FK column name to its position under selectTag. It
// re-projects the original columns under a fresh tag (positions preserved, so
// colName2Idx stays valid) and returns that tag.
func (builder *QueryBuilder) buildModernChildFkAssert(
	bindCtx *BindContext,
	tableDef *plan.TableDef,
	lastNodeID int32,
	selectTag int32,
	childColPos func(colName string) int32,
) (int32, int32, error) {
	projLen := len(builder.qry.Nodes[lastNodeID].ProjectList)
	childTyps := make([]plan.Type, projLen)
	for i, e := range builder.qry.Nodes[lastNodeID].ProjectList {
		childTyps[i] = e.Typ
	}

	lastNodeID, oks, err := builder.appendModernChildFkMarkOks(bindCtx, tableDef, lastNodeID, selectTag, childColPos)
	if err != nil {
		return 0, 0, err
	}
	if len(oks) == 0 {
		return lastNodeID, selectTag, nil // only self-referencing FKs: nothing to assert here
	}

	errExpr := makePlan2StringConstExprWithType("Cannot add or update a child row: a foreign key constraint fails")
	assertConds := make([]*plan.Expr, len(oks))
	for i, ok := range oks {
		assertConds[i], err = BindFuncExprImplByPlanExpr(builder.GetContext(), "assert", []*plan.Expr{ok, DeepCopyExpr(errExpr)})
		if err != nil {
			return 0, 0, err
		}
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{lastNodeID},
		FilterList: assertConds,
	}, bindCtx)

	newTag := builder.genNewBindTag()
	newProjList := make([]*plan.Expr, projLen)
	for i := 0; i < projLen; i++ {
		newProjList[i] = &plan.Expr{Typ: childTyps[i], Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectTag, ColPos: int32(i)}}}
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: newProjList,
		Children:    []int32{lastNodeID},
		BindingTags: []int32{newTag},
	}, bindCtx)
	return lastNodeID, newTag, nil
}

// buildInsertIgnoreFkFilter implements INSERT IGNORE child→parent foreign-key
// semantics on the modern path: instead of asserting it drops the rows whose foreign
// key has no matching parent, keeping the rest — MySQL's row-skip behaviour. It shares
// the binding-tagged MARK-join existence check with buildModernChildFkAssert (so a FK
// to a non-unique parent column never multiplies the surviving rows), then keeps a row
// only when every FK is satisfied. A NULL foreign-key value satisfies the constraint
// (MATCH SIMPLE) and is kept; a self-referencing FK (ForeignTbl == 0) is left to the
// post-execution DetectSql. The new-row column layout (and colName2Idx positions) is
// preserved under the returned binding tag.
func (builder *QueryBuilder) buildInsertIgnoreFkFilter(
	bindCtx *BindContext,
	tableDef *plan.TableDef,
	lastNodeID int32,
	selectTag int32,
	colName2Idx map[string]int32,
) (int32, int32, error) {
	projLen := len(builder.qry.Nodes[lastNodeID].ProjectList)
	childTyps := make([]plan.Type, projLen)
	for i, e := range builder.qry.Nodes[lastNodeID].ProjectList {
		childTyps[i] = e.Typ
	}

	lastNodeID, oks, err := builder.appendModernChildFkMarkOks(bindCtx, tableDef, lastNodeID, selectTag,
		func(colName string) int32 { return colName2Idx[tableDef.Name+"."+colName] })
	if err != nil {
		return 0, 0, err
	}
	if len(oks) == 0 {
		return lastNodeID, selectTag, nil // only self-referencing FKs: nothing to drop here
	}

	// Keep a row only when every foreign key is satisfied (FilterList ANDs its conds).
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{lastNodeID},
		FilterList: oks,
	}, bindCtx)

	newTag := builder.genNewBindTag()
	newProjList := make([]*plan.Expr, projLen)
	for i := 0; i < projLen; i++ {
		newProjList[i] = &plan.Expr{Typ: childTyps[i], Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectTag, ColPos: int32(i)}}}
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: newProjList,
		Children:    []int32{lastNodeID},
		BindingTags: []int32{newTag},
	}, bindCtx)
	return lastNodeID, newTag, nil
}

func (builder *QueryBuilder) appendDedupAndMultiUpdateNodesForBindInsert(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	lastNodeID int32,
	colName2Idx map[string]int32,
	skipUniqueIdx []bool,
	astUpdateExprs tree.UpdateExprs,
	irregularIndexes []*plan.IndexDef,
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
		// No primary key and no unique key: a duplicate can never occur, so the
		// modern dedup+MULTI_UPDATE has no key to represent ON DUPLICATE KEY
		// UPDATE. The statement is semantically a plain INSERT, but a prepared
		// statement still carries the update clause's parameter markers, which
		// the modern plan would drop (parameters are collected from the bound
		// plan tree). Defer this degenerate corner to the legacy planner, which
		// keeps the parameters and inserts the row. Signalled distinctly so only
		// this case (not real PK/unique-key ODKU) is allowed to fall back.
		return 0, moerr.NewUnsupportedDML(builder.GetContext(), noPkOnDupUpdateCause)
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

	// Irregular-index (IVF/fulltext) maintenance source for plain INSERT/LOAD (no
	// conflict resolution): materialize the pre-dedup new-row image and let the
	// dedup read a sink-scan of it, so the same image feeds the post-createQuery
	// insert maintenance. ON DUPLICATE KEY UPDATE (onDupAction == UPDATE) is
	// handled later from the final merged image, after the dedup-update join,
	// where old entries are also dropped.
	// Row-scoped child→parent foreign-key handling. Both validate only the
	// statement's own rows, so neither false-positives on rows inserted earlier
	// under FOREIGN_KEY_CHECKS=0 nor scales with table size:
	//   - plain INSERT asserts parent existence in the data flow and fails the
	//     statement on a missing parent (buildModernChildFkAssert);
	//   - INSERT IGNORE drops the offending rows in the data flow instead of
	//     asserting (buildInsertIgnoreFkFilter), so the MULTI_UPDATE inserts only
	//     the surviving rows — MySQL's row-skip semantics.
	// ON DUPLICATE KEY UPDATE runs its own in-plan assert over the final merged
	// image (handled earlier, with appendOnDupIrregularMaintSource).
	if onDupAction == plan.Node_FAIL {
		fkEnabled, err := builder.modernInsertFkCheckEnabled(tableDef)
		if err != nil {
			return 0, err
		}
		if fkEnabled {
			// Assert parent existence in the data flow (binding-tagged, robust to a
			// unique-key child's appended index-helper columns) instead of the
			// post-createQuery appendForeignConstrantPlan over the materialized image.
			if lastNodeID, selectTag, err = builder.buildModernChildFkAssert(bindCtx, tableDef, lastNodeID, selectTag,
				func(colName string) int32 { return colName2Idx[tableDef.Name+"."+colName] }); err != nil {
				return 0, err
			}
			selectNode = builder.qry.Nodes[lastNodeID]
		}
	} else if onDupAction == plan.Node_IGNORE {
		fkEnabled, err := builder.modernInsertFkCheckEnabled(tableDef)
		if err != nil {
			return 0, err
		}
		if fkEnabled {
			if lastNodeID, selectTag, err = builder.buildInsertIgnoreFkFilter(bindCtx, tableDef, lastNodeID, selectTag, colName2Idx); err != nil {
				return 0, err
			}
			selectNode = builder.qry.Nodes[lastNodeID]
		}
	}
	if onDupAction != plan.Node_UPDATE && len(irregularIndexes) > 0 {
		lastNodeID = builder.appendIrregularMaintSource(bindCtx, lastNodeID, irregularIndexes, tableDef, dmlCtx.objRefs[0], false)
		selectNode = builder.qry.Nodes[lastNodeID]
		selectTag = selectNode.BindingTags[0]
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

	// Materialize lock keys for composite and prefix unique indexes in advance.
	// This guarantees the lock target can find __mo_index_idx_col in colName2Idx.
	for i, idxDef := range tableDef.Indexes {
		prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
		if err != nil {
			return 0, err
		}
		if !idxDef.Unique || skipUniqueIdx[i] || (len(idxDef.Parts) <= 1 && len(prefixLengths) == 0) {
			continue
		}
		lockColName := idxDef.IndexTableName + "." + catalog.IndexTableIndexColName
		if _, ok := colName2Idx[lockColName]; ok {
			continue
		}

		var lockExpr *plan.Expr
		if len(idxDef.Parts) == 1 {
			partName := catalog.ResolveAlias(idxDef.Parts[0])
			partPos, ok := colName2Idx[tableDef.Name+"."+partName]
			if !ok {
				return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", partName)
			}
			lockExpr, err = builder.makeIndexPartExprFromInputExpr(selectNode.ProjectList[partPos], partName, prefixLengths)
			if err != nil {
				return 0, err
			}
		} else {
			args := make([]*plan.Expr, len(idxDef.Parts))
			for k := range idxDef.Parts {
				partName := catalog.ResolveAlias(idxDef.Parts[k])
				partPos, ok := colName2Idx[tableDef.Name+"."+partName]
				if !ok {
					return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", partName)
				}
				args[k], err = builder.makeIndexPartExprFromInputExpr(selectNode.ProjectList[partPos], partName, prefixLengths)
				if err != nil {
					return 0, err
				}
			}
			lockExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", args)
		}
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

		prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
		if err != nil {
			return 0, err
		}
		if len(idxDef.Parts) == 1 && len(prefixLengths) == 0 {
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
		prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
		if err != nil {
			return 0, err
		}
		if argsLen == 1 {
			idxIndexColExpr, err = builder.makeInsertIndexPartExpr(selectNode, selectTag, tableDef, colName2Idx, idxDef.Parts[0], prefixLengths)
			if err != nil {
				return 0, err
			}
		} else {
			args := make([]*plan.Expr, argsLen)
			for k := range argsLen {
				args[k], err = builder.makeInsertIndexPartExpr(selectNode, selectTag, tableDef, colName2Idx, idxDef.Parts[k], prefixLengths)
				if err != nil {
					return 0, err
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
				prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
				if err != nil {
					return nil, err
				}
				if !indexTableStoresSerializedKey(idxDef) {
					return builder.makeInsertIndexPartExpr(selectNode, selectTag, tableDef, colName2Idx, indexPrimaryPartName(idxDef), prefixLengths)
				}

				args := make([]*plan.Expr, len(idxDef.Parts))
				for k, part := range idxDef.Parts {
					var err error
					args[k], err = builder.makeInsertIndexPartExpr(selectNode, selectTag, tableDef, colName2Idx, part, prefixLengths)
					if err != nil {
						return nil, err
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
				prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
				if err != nil {
					return 0, err
				}
				if !indexTableStoresSerializedKey(idxDef) {
					colPos, ok := tableDef.Name2ColIndex[indexPrimaryPartName(idxDef)]
					if !ok {
						return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", indexPrimaryPartName(idxDef))
					}
					delIdxExpr, err = builder.makeIndexPartExpr(scanTag, colPos, indexPrimaryPartName(idxDef), tableDef.Cols[colPos].Typ, prefixLengths)
					if err != nil {
						return 0, err
					}
				} else {
					delArgs := make([]*plan.Expr, len(idxDef.Parts))

					var colPos int32
					var ok bool
					for k, part := range idxDef.Parts {
						if colPos, ok = tableDef.Name2ColIndex[catalog.ResolveAlias(part)]; !ok {
							return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", part)
						}

						delArgs[k], err = builder.makeIndexPartExpr(scanTag, colPos, catalog.ResolveAlias(part), tableDef.Cols[colPos].Typ, prefixLengths)
						if err != nil {
							return 0, err
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

		// ON DUPLICATE KEY UPDATE: materialize the final merged image (this PROJECT)
		// so the main plan, the irregular-index maintenance, and the row-scoped
		// child→parent foreign-key check can all read it. The dedup PK is immutable,
		// so old index entries (and the FK columns) are keyed by the same PK.
		//
		// The FK check runs over this final image (the statement's own rows), not a
		// whole-table DetectSql, so it never false-positives on unrelated orphan rows
		// (e.g. inserted earlier under FOREIGN_KEY_CHECKS=0) and its cost does not
		// scale with table size. It is deferred to finishIrregularIndexMaintenance
		// (post-createQuery) like the plain-INSERT FK check.
		odkuNeedFkCheck, err := builder.modernInsertFkCheckEnabled(tableDef)
		if err != nil {
			return 0, err
		}
		// ON DUPLICATE KEY UPDATE enforces child->parent FKs in the data flow with the
		// same per-FK MARK-join check as INSERT, over this final merged image. Each FK is
		// asserted independently (its parent exists OR one of that FK's own columns is
		// NULL), correct MATCH SIMPLE -- unlike the old appendForeignConstrantPlan, which
		// first applied one global isnotnull pre-filter across ALL FK columns and so
		// skipped every FK's validation as soon as ANY FK column on the row was NULL.
		// Unlike plain INSERT this does NOT re-project to a fresh tag: ODKU's downstream
		// MULTI_UPDATE reads both the merged image (finalProjTag) and the delete columns
		// (delColName2Idx) from this subtree, which a re-project would hide.
		if onDupAction == plan.Node_UPDATE && odkuNeedFkCheck {
			var oks []*plan.Expr
			if lastNodeID, oks, err = builder.appendModernChildFkMarkOks(bindCtx, tableDef, lastNodeID, finalProjTag,
				func(colName string) int32 { return colName2Idx[tableDef.Name+"."+colName] }); err != nil {
				return 0, err
			}
			if len(oks) > 0 {
				fkErrExpr := makePlan2StringConstExprWithType("Cannot add or update a child row: a foreign key constraint fails")
				assertConds := make([]*plan.Expr, len(oks))
				for i, ok := range oks {
					if assertConds[i], err = BindFuncExprImplByPlanExpr(builder.GetContext(), "assert", []*plan.Expr{ok, DeepCopyExpr(fkErrExpr)}); err != nil {
						return 0, err
					}
				}
				lastNodeID = builder.appendNode(&plan.Node{
					NodeType:   plan.Node_FILTER,
					Children:   []int32{lastNodeID},
					FilterList: assertConds,
				}, bindCtx)
				selectNode = builder.qry.Nodes[lastNodeID]
			}
		}
		if len(irregularIndexes) > 0 {
			// ODKU cannot change the PK, so the stale entries are keyed by the same
			// PK the final image carries at its natural position.
			odkuPkPos, odkuPkTyp := getPkPos(tableDef, false)
			lastNodeID = builder.appendOnDupIrregularMaintSource(
				bindCtx, lastNodeID, finalProjTag, int32(odkuPkPos), odkuPkTyp,
				irregularIndexes, tableDef, dmlCtx.objRefs[0])
			selectNode = builder.qry.Nodes[lastNodeID]
		}
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

	lastNodeID, err = appendCheckConstraintPlan(builder, bindCtx, tableDef, lastNodeID, selectTag, colName2Idx)
	if err != nil {
		return 0, err
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

	// colName2Idx values written in this loop are final select/projection
	// positions. CHECK filters and DML update contexts consume those positions,
	// not table definition ordinals.
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
			colName2Idx[tableDef.Name+"."+col.Name] = int32(len(projList2) - 1)
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
			colName2Idx[tableDef.Name+"."+col.Name] = int32(len(projList2) - 1)
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
			colName2Idx[tableDef.Name+"."+col.Name] = int32(len(projList2) - 1)
		} else if col.GeneratedCol != nil {
			// MatrixOne currently materializes both STORED and VIRTUAL generated columns on write.
			// Defer them until base/default columns are in projList1 so forward references resolve.
			genColIdxToProj1Pos[i] = len(projList1)
			genColIdxToProj2Pos[i] = len(projList2)
			generatedColIdxs = append(generatedColIdxs, i)
			projList1 = append(projList1, nil)
			projList2 = append(projList2, nil)
			colName2Idx[tableDef.Name+"."+col.Name] = int32(len(projList2) - 1)
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
			colName2Idx[tableDef.Name+"."+col.Name] = int32(len(projList2) - 1)
		}
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

// valuesExprIsFuncCall reports whether a VALUES item is, or transparently wraps
// (through parentheses or a cast), a function call. Such expressions must be
// bound without the destination column type so the function's literal arguments
// bind by their own types — e.g. st_point(116.3975, 39.9087),
// (st_point(116.3975, 39.9087)) or cast(st_point(...) as point) into a geometry
// column. A bare literal (or a literal wrapped in a unary minus / cast) is not a
// function call and still binds against the column type.
func valuesExprIsFuncCall(e tree.Expr) bool {
	for {
		switch v := e.(type) {
		case *tree.ParenExpr:
			e = v.Expr
		case *tree.CastExpr:
			e = v.Expr
		case *tree.FuncExpr:
			return true
		default:
			return false
		}
	}
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
			// A function-call value expression must be bound without the
			// destination column type. The DefaultBinder pushes its type down to
			// nested literals, so binding st_point(116.3975, 39.9087) against a
			// GEOMETRY column would type the float arguments as GEOMETRY and break
			// the function's overload resolution. A function's arguments bind by
			// their own types, and the result is cast to the column type below
			// (funcCastFor*Type / forceCastExpr2). Other value expressions (a
			// negative literal like -1.5, a cast, etc.) still bind against the
			// column type, which a literal value legitimately adopts.
			funcBinder := NewDefaultBinder(builder.GetContext(), nil, nil, plan.Type{}, nil)
			funcBinder.builder = builder
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
					valueBinder := binder
					if valuesExprIsFuncCall(r[i]) {
						// function call (possibly wrapped in parens / a cast):
						// bind its arguments by their own types, not the
						// destination column type
						valueBinder = funcBinder
					}
					defExpr, err = valueBinder.BindExpr(r[i], 0, true)
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
