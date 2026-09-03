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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"go.uber.org/zap"
)

func (builder *QueryBuilder) bindInsert(stmt *tree.Insert, bindCtx *BindContext) (int32, error) {
	// This flag is populated by initInsertReplaceStmt only for a plain
	// INSERT ... SELECT.  Reset it before binding so a QueryBuilder cannot leak
	// the proof into a later DML path (for example LOAD or REPLACE).
	builder.insertInputKeysUnique = false
	// INSERT IGNORE (OnDuplicateUpdate == [nil]) downgrades over-length
	// CHAR/VARCHAR writes to truncation instead of rejection.
	builder.isInsertIgnore = len(stmt.OnDuplicateUpdate) == 1 && stmt.OnDuplicateUpdate[0] == nil
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
	dmlCtx.targetDBName = targetDB
	dmlCtx.targetTableName = targetTable
	if err = validateInsertColumnQualifiers(
		builder.GetContext(), stmt.ColumnNames, targetDB, targetTable, builder.compCtx.GetLowerCaseTableNames(),
	); err != nil {
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
	if tableDef.TableType == catalog.SystemClusterRel {
		if stmt.Overwrite {
			return 0, moerr.NewNotSupported(builder.GetContext(), "INSERT OVERWRITE currently supports Iceberg table mappings")
		}
		if len(stmt.PartitionValues) > 0 {
			return 0, moerr.NewNotSupported(builder.GetContext(), "INSERT PARTITION value syntax currently supports Iceberg INSERT OVERWRITE only")
		}
	}
	if err := validateTableRegularIndexPrefixMetadata(tableDef); err != nil {
		return 0, err
	}
	if stmt.HasReturning() {
		if err := validateReturningTarget(builder, tableDef, dmlCtx.objRefs[0]); err != nil {
			return 0, err
		}
	}

	irregularIndexes := getIrregularIndexes(tableDef)

	lastNodeID, colName2Idx, skipUniqueIdx, err := builder.initInsertReplaceStmt(bindCtx, stmt.Rows, stmt.Columns, dmlCtx.objRefs[0], dmlCtx.tableDefs[0], false, false)
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

		// If a regular index table exists, DML must maintain it even when the
		// index parts include the full primary key. The optimizer can still choose
		// that hidden table for leading-prefix predicates.
		indexes = append(indexes, idxDef)
	}

	return
}

// isModernMaintainedIrregularAlgo reports whether an irregular index algorithm has
// the modern maintenance plumbing (both insert and delete sub-plans). IVF,
// fulltext, and MASTER qualify (the master index table has an independent
// __mo_index_pri_col origin-pk column, so delete joins on origin pk — same pattern
// as fulltext joins on doc_id). Per-index async parameters are filtered later by
// splitIrregularIndexesByUpdatedColumns before an ODKU-only inline branch is built.
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

func irregularIndexGroupKey(indexdef *plan.IndexDef) string {
	if indexdef.IndexName != "" {
		return strings.ToLower(indexdef.IndexName)
	}
	// IndexName is expected for user indexes. Keep malformed/legacy metadata
	// isolated by its physical identity instead of grouping every empty name.
	return strings.ToLower(indexdef.IndexAlgo + "\x00" + indexdef.IndexTableName)
}

type irregularIndexValueChangeFilter struct {
	groupKey string
	columns  []string
}

// buildIrregularIndexValueChangeFilters returns the affected logical index
// groups whose plugin can prove that unchanged stored values imply unchanged
// hidden-index state. Every physical definition in a group must support the
// proof; otherwise the whole group keeps the conservative rebuild path.
func buildIrregularIndexValueChangeFilters(
	tableDef *plan.TableDef,
	indexes []*plan.IndexDef,
) ([]irregularIndexValueChangeFilter, error) {
	groups := make(map[string][]*plan.IndexDef, len(indexes))
	groupOrder := make([]string, 0, len(indexes))
	for _, indexdef := range indexes {
		key := irregularIndexGroupKey(indexdef)
		if _, ok := groups[key]; !ok {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(groups[key], indexdef)
	}

	filters := make([]irregularIndexValueChangeFilter, 0, len(groups))
	for _, key := range groupOrder {
		columnSet := make(map[string]struct{})
		supported := true
		for _, indexdef := range groups[key] {
			plugin, ok := indexplugin.Get(indexdef.IndexAlgo)
			if !ok {
				supported = false
				break
			}
			hook, ok := plugin.Plan().(planplugin.DMLMaintenanceNoOpHook)
			if !ok {
				supported = false
				break
			}
			columns, canProve, err := hook.DMLMaintenanceNoOpColumns(tableDef, indexdef)
			if err != nil {
				return nil, err
			}
			if !canProve || len(columns) == 0 {
				supported = false
				break
			}
			for _, column := range columns {
				columnSet[catalog.ResolveAlias(column)] = struct{}{}
			}
		}
		if !supported {
			continue
		}

		columns := make([]string, 0, len(columnSet))
		for _, column := range tableDef.Cols {
			if _, ok := columnSet[column.Name]; ok {
				columns = append(columns, column.Name)
				delete(columnSet, column.Name)
			}
		}
		if len(columns) == 0 || len(columnSet) != 0 {
			continue
		}
		filters = append(filters, irregularIndexValueChangeFilter{groupKey: key, columns: columns})
	}
	return filters, nil
}

// splitIrregularIndexesByUpdatedColumns partitions complete logical indexes,
// not individual physical IndexDefs. A multi-column fulltext index and a
// multi-table vector index are represented by several definitions with one
// IndexName; if any definition references a possibly updated column, every
// definition in that logical index must follow the delete-and-rebuild path.
// Logical indexes maintained asynchronously by CDC are omitted from both
// results; their leaf builders deliberately emit no inline maintenance plan.
func splitIrregularIndexesByUpdatedColumns(
	tableDef *plan.TableDef,
	indexes []*plan.IndexDef,
	possiblyChangedCols map[string]struct{},
) (affected, insertOnly []*plan.IndexDef, err error) {
	if len(indexes) == 0 {
		return nil, nil, nil
	}

	affectedGroups := make(map[string]bool, len(indexes))
	asyncGroups := make(map[string]bool, len(indexes))
	for _, indexdef := range indexes {
		async, asyncErr := indexplugin.IsAsync(indexdef.IndexAlgo, indexdef.IndexAlgoParams)
		if asyncErr != nil {
			return nil, nil, asyncErr
		}
		if async {
			asyncGroups[irregularIndexGroupKey(indexdef)] = true
		}
	}
	syncIndexes := make([]*plan.IndexDef, 0, len(indexes))
	for _, indexdef := range indexes {
		if asyncGroups[irregularIndexGroupKey(indexdef)] {
			continue
		}
		syncIndexes = append(syncIndexes, indexdef)
		key := irregularIndexGroupKey(indexdef)
		affected, affectedErr := irregularIndexAffectedByUpdatedColumnNames(tableDef, indexdef, possiblyChangedCols)
		if affectedErr != nil {
			return nil, nil, affectedErr
		}
		if affected {
			affectedGroups[key] = true
		}
	}

	for _, indexdef := range syncIndexes {
		if affectedGroups[irregularIndexGroupKey(indexdef)] {
			affected = append(affected, indexdef)
		} else {
			insertOnly = append(insertOnly, indexdef)
		}
	}
	return affected, insertOnly, nil
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
		if fk == nil {
			return false, moerr.NewInternalError(builder.GetContext(), "malformed foreign-key metadata")
		}
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

func odkuNonSelfForeignKeys(ctx context.Context, tableDef *plan.TableDef, enabled bool) (*plan.TableDef, error) {
	filtered := *tableDef
	filtered.Checks = nil
	filtered.Fkeys = nil
	if !enabled {
		return &filtered, nil
	}
	for _, fk := range tableDef.Fkeys {
		if fk == nil {
			return nil, moerr.NewInternalError(ctx, "ON DUPLICATE KEY UPDATE has malformed foreign-key metadata")
		}
		if fk.ForeignTbl != 0 {
			filtered.Fkeys = append(filtered.Fkeys, fk)
		}
	}
	return &filtered, nil
}

func odkuUpdatedNotNullColumns(
	tableDef *plan.TableDef,
	updateColIdxList []int32,
	updateColExprList []*plan.Expr,
) []int32 {
	seen := make(map[int32]struct{}, len(updateColIdxList))
	result := make([]int32, 0, len(updateColIdxList))
	for i, colIdx := range updateColIdxList {
		if colIdx < 0 || int(colIdx) >= len(tableDef.Cols) {
			continue
		}
		col := tableDef.Cols[colIdx]
		if col.Default == nil || col.Default.NullAbility ||
			strings.HasPrefix(col.Name, catalog.PrefixCBColName) {
			continue
		}
		// Most ODKU assignments into NOT NULL columns are already proven
		// non-null (for example v = VALUES(v)). Keep those on the one-row-per-key
		// fast path; action validation is needed only when an expression can
		// actually produce NULL before a later action restores the final image.
		if i < len(updateColExprList) && updateColExprList[i] != nil &&
			updateColExprList[i].Typ.NotNullable {
			continue
		}
		if _, exists := seen[colIdx]; exists {
			continue
		}
		seen[colIdx] = struct{}{}
		result = append(result, colIdx)
	}
	return result
}

func (builder *QueryBuilder) appendODKUActionNotNullAssertions(
	bindCtx *BindContext,
	tableDef *plan.TableDef,
	lastNodeID int32,
	selectTag int32,
	colIdxList []int32,
) (int32, error) {
	assertions := make([]*plan.Expr, 0, len(colIdxList))
	for _, colIdx := range colIdxList {
		col := tableDef.Cols[colIdx]
		colType := col.Typ
		// The action stream can temporarily contain NULL even though the target
		// schema is NOT NULL. Do not let expression folding use the destination
		// declaration to reduce isnotnull(action_value) to a constant true.
		colType.NotNullable = false
		colExpr := &plan.Expr{
			Typ:  colType,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectTag, ColPos: colIdx}},
		}
		isNotNull, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "isnotnull", []*plan.Expr{colExpr})
		if err != nil {
			return 0, err
		}
		assertion, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "_check_constraint_assert", []*plan.Expr{
				isNotNull,
				makePlan2StringConstExprWithType(fmt.Sprintf("Column '%s' cannot be null", col.Name)),
			})
		if err != nil {
			return 0, err
		}
		assertions = append(assertions, assertion)
	}
	if len(assertions) == 0 {
		return lastNodeID, nil
	}
	return builder.appendNode(&plan.Node{
		NodeType:        plan.Node_FILTER,
		Children:        []int32{lastNodeID},
		FilterList:      assertions,
		FilterIsBarrier: true,
	}, bindCtx), nil
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
	// DML RETURNING can read the same final new-row image (irregularMaintIndexes is
	// empty, so finishIrregularIndexMaintenance stays a no-op).
	if len(irregularIndexes) == 0 && !forceMaterialize {
		return newRowImageID
	}

	sinkTag := builder.genNewBindTag()
	sinkID := appendSinkNodeWithTag(builder, bindCtx, newRowImageID, sinkTag)
	if forceMaterialize {
		builder.preserveReturningSinkProjection(sinkID)
	}
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
	builder.irregularMaintValueChangedSourceSteps = nil
	builder.irregularMaintTableDef = &maintTableDef
	builder.irregularMaintObjRef = objRef
	if forceMaterialize {
		colPos := make(map[string]int32, len(tableDef.Cols))
		var imagePos int32
		for _, col := range tableDef.Cols {
			if col.Name == catalog.Row_ID {
				continue
			}
			colPos[strings.ToLower(col.Name)] = imagePos
			imagePos++
		}
		builder.recordReturningSource(maintStep, tableDef, objRef, tableDef.Name, tableDef.Name, colPos)
	}

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
//   - for ODKU, affected indexes use a derivative containing only rows whose
//     final image physically changed, with value-aware plugins narrowing each
//     logical index to rows whose indexed values changed;
//   - unaffected indexes use a shared derivative step filtered by old Row_ID IS
//     NULL, so only genuinely new rows reach their insert maintenance.
//
// Both steps keep the same projection layout. Their leading columns are the base
// table columns in tableDef.Cols order (minus Row_ID), the layout the leaf builders
// index by PK / indexed-column position. For affected indexes, the PK is immutable
// under ODKU, so deleting by the final-image PK removes exactly the stale entries
// and is a no-op for non-conflicting rows.
//
// It records the maintenance context on the builder and returns the main-plan
// sink-scan the caller must continue from.
func (builder *QueryBuilder) appendOnDupIrregularMaintSource(
	bindCtx *BindContext,
	finalProjNodeID, finalProjTag, deletePkPos int32, deletePkTyp plan.Type,
	targetRowNumberPos, targetActivePos, physicalChangedPos int32,
	irregularIndexes, insertOnlyIndexes []*plan.IndexDef,
	newRowMarkerPos int32,
	valueChangeMarkerPosByGroup map[string]int32,
	tableDef *plan.TableDef,
	objRef *plan.ObjectRef,
) (int32, error) {
	sinkID := appendSinkNodeWithTag(builder, bindCtx, finalProjNodeID, finalProjTag)
	joinStep := builder.appendStep(sinkID)
	maintStep := joinStep
	if targetRowNumberPos >= 0 {
		selectedScanID := builder.appendTaggedSinkScan(bindCtx, joinStep, finalProjTag)
		selectedScan := builder.qry.Nodes[selectedScanID]
		selected, err := builder.buildTargetSelectedExpr(
			finalProjTag, selectedScan, targetRowNumberPos, targetActivePos)
		if err != nil {
			return 0, err
		}
		selectedID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_FILTER, Children: []int32{selectedScanID}, FilterList: []*plan.Expr{selected},
		}, bindCtx)
		selectedSinkID := appendSinkNodeWithTag(builder, bindCtx, selectedID, finalProjTag)
		maintStep = builder.appendStep(selectedSinkID)
	}
	insertOnlyBaseStep := maintStep
	// A changed-row derivative is useful only to affected indexes. Creating it
	// for an insert-only maintenance plan leaves an orphan SINK step, which the
	// compiler correctly rejects because it has no receiver.
	if physicalChangedPos >= 0 && len(irregularIndexes) > 0 {
		changedScanID := builder.appendTaggedSinkScan(bindCtx, maintStep, finalProjTag)
		changedScan := builder.qry.Nodes[changedScanID]
		if int(physicalChangedPos) >= len(changedScan.ProjectList) ||
			changedScan.ProjectList[physicalChangedPos].Typ.Id != int32(types.T_bool) {
			return 0, moerr.NewInternalError(builder.GetContext(),
				"ON DUPLICATE KEY UPDATE cannot locate the physical-change marker for irregular index maintenance")
		}
		changedID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_FILTER,
			Children: []int32{changedScanID},
			FilterList: []*plan.Expr{{
				Typ: changedScan.ProjectList[physicalChangedPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: finalProjTag,
					ColPos: physicalChangedPos,
				}},
			}},
		}, bindCtx)
		changedSinkID := appendSinkNodeWithTag(builder, bindCtx, changedID, finalProjTag)
		maintStep = builder.appendStep(changedSinkID)
	}

	insertOnlyStep := int32(-1)
	if len(insertOnlyIndexes) > 0 {
		// Unaffected indexes need only new rows. Derive them before the physical-
		// change filter: every new row is already marked changed, and avoiding the
		// redundant filter keeps this branch's plan and runtime work minimal.
		newRowsScanID := builder.appendTaggedSinkScan(bindCtx, insertOnlyBaseStep, finalProjTag)
		newRowsScan := builder.qry.Nodes[newRowsScanID]
		if newRowMarkerPos < 0 || int(newRowMarkerPos) >= len(newRowsScan.ProjectList) {
			return 0, moerr.NewInternalError(builder.GetContext(),
				"ON DUPLICATE KEY UPDATE cannot locate the old-row marker for irregular index maintenance")
		}
		oldRowMarker := &plan.Expr{
			Typ: newRowsScan.ProjectList[newRowMarkerPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: finalProjTag,
				ColPos: newRowMarkerPos,
			}},
		}
		isNewRow, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "isnull", []*plan.Expr{oldRowMarker})
		if err != nil {
			return 0, err
		}
		newRowsID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_FILTER, Children: []int32{newRowsScanID}, FilterList: []*plan.Expr{isNewRow},
		}, bindCtx)
		newRowsSinkID := appendSinkNodeWithTag(builder, bindCtx, newRowsID, finalProjTag)
		insertOnlyStep = builder.appendStep(newRowsSinkID)
	}

	valueChangedSteps := make(map[string]int32, len(valueChangeMarkerPosByGroup))
	valueChangedGroupKeys := make([]string, 0, len(valueChangeMarkerPosByGroup))
	for groupKey := range valueChangeMarkerPosByGroup {
		valueChangedGroupKeys = append(valueChangedGroupKeys, groupKey)
	}
	slices.Sort(valueChangedGroupKeys)
	for _, groupKey := range valueChangedGroupKeys {
		markerPos := valueChangeMarkerPosByGroup[groupKey]
		changedRowsScanID := builder.appendTaggedSinkScan(bindCtx, insertOnlyBaseStep, finalProjTag)
		changedRowsScan := builder.qry.Nodes[changedRowsScanID]
		if newRowMarkerPos < 0 || int(newRowMarkerPos) >= len(changedRowsScan.ProjectList) ||
			markerPos < 0 || int(markerPos) >= len(changedRowsScan.ProjectList) {
			return 0, moerr.NewInternalError(builder.GetContext(),
				"ON DUPLICATE KEY UPDATE cannot locate an irregular index value-change marker")
		}
		oldRowMarker := &plan.Expr{
			Typ: changedRowsScan.ProjectList[newRowMarkerPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: finalProjTag,
				ColPos: newRowMarkerPos,
			}},
		}
		isNewRow, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "isnull", []*plan.Expr{oldRowMarker})
		if err != nil {
			return 0, err
		}
		valueChanged := &plan.Expr{
			Typ: changedRowsScan.ProjectList[markerPos].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: finalProjTag,
				ColPos: markerPos,
			}},
		}
		eligible, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(), "or", []*plan.Expr{isNewRow, valueChanged})
		if err != nil {
			return 0, err
		}
		changedRowsID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_FILTER, Children: []int32{changedRowsScanID}, FilterList: []*plan.Expr{eligible},
		}, bindCtx)
		changedRowsSinkID := appendSinkNodeWithTag(builder, bindCtx, changedRowsID, finalProjTag)
		valueChangedSteps[groupKey] = builder.appendStep(changedRowsSinkID)
	}

	maintTableDef := *tableDef
	maintTableDef.Indexes = irregularIndexes
	builder.irregularMaintSourceStep = maintStep
	builder.irregularMaintDeleteStep = maintStep
	builder.irregularMaintDeletePkPos = deletePkPos
	builder.irregularMaintDeletePkTyp = deletePkTyp
	builder.irregularMaintIndexes = irregularIndexes
	builder.irregularMaintInsertOnlySourceStep = insertOnlyStep
	builder.irregularMaintInsertOnlyIndexes = insertOnlyIndexes
	builder.irregularMaintValueChangedSourceSteps = valueChangedSteps
	builder.irregularMaintTableDef = &maintTableDef
	builder.irregularMaintObjRef = objRef

	return builder.appendTaggedSinkScan(bindCtx, joinStep, finalProjTag), nil
}

// buildIrregularIndexMaintenance appends, after createQuery, the synchronous
// maintenance sub-plans for the irregular (IVF/fulltext) indexes recorded by
// appendIrregularMaintSource. Each sub-plan reads the materialized new-row image
// via a sink-scan and reuses the same leaf primitives the legacy insert path uses
// (buildPreInsertMultiTableIndexes for IVF, buildPreInsertFullTextIndex for
// fulltext). The caller is responsible for the subsequent reduceSinkSinkScanNodes
// + tempOptimizeForDML post-processing.
func (builder *QueryBuilder) buildIrregularIndexMaintenance(bindCtx *BindContext) error {
	// ON DUPLICATE KEY UPDATE: drop the conflicting rows' old entries first, before
	// re-inserting the final-image entries, so a deletion keyed by the (immutable)
	// PK does not remove the freshly inserted ones.
	if builder.irregularMaintDeleteStep >= 0 && len(builder.irregularMaintIndexes) > 0 {
		if err := builder.buildIrregularIndexDeleteMaintenance(bindCtx); err != nil {
			return err
		}
	}
	if builder.irregularMaintSkipInsert {
		return nil
	}
	if len(builder.irregularMaintIndexes) > 0 {
		if err := builder.buildIrregularIndexInsertMaintenance(
			bindCtx,
			builder.irregularMaintSourceStep,
			builder.irregularMaintTableDef,
		); err != nil {
			return err
		}
	}
	if len(builder.irregularMaintInsertOnlyIndexes) > 0 {
		if builder.irregularMaintInsertOnlySourceStep < 0 {
			return moerr.NewInternalError(builder.GetContext(),
				"missing new-row source for insert-only irregular index maintenance")
		}
		insertOnlyTableDef := *builder.irregularMaintTableDef
		insertOnlyTableDef.Indexes = builder.irregularMaintInsertOnlyIndexes
		if err := builder.buildIrregularIndexInsertMaintenance(
			bindCtx,
			builder.irregularMaintInsertOnlySourceStep,
			&insertOnlyTableDef,
		); err != nil {
			return err
		}
	}
	return nil
}

func (builder *QueryBuilder) buildIrregularIndexInsertMaintenance(
	bindCtx *BindContext,
	sourceStep int32,
	tableDef *plan.TableDef,
) error {
	objRef := builder.irregularMaintObjRef

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

	multiTableIndexesBySource := make(map[int32]map[string]*MultiTableIndex)
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
		indexSourceStep := builder.irregularMaintenanceSourceStep(indexdef, sourceStep)
		switch {
		case catalog.IsIvfIndexAlgo(indexdef.IndexAlgo):
			groupKey := irregularIndexGroupKey(indexdef)
			multiTableIndexes := multiTableIndexesBySource[indexSourceStep]
			if multiTableIndexes == nil {
				multiTableIndexes = make(map[string]*MultiTableIndex)
				multiTableIndexesBySource[indexSourceStep] = multiTableIndexes
			}
			if _, ok := multiTableIndexes[groupKey]; !ok {
				multiTableIndexes[groupKey] = &MultiTableIndex{
					IndexAlgo:       catalog.ToLower(indexdef.IndexAlgo),
					IndexAlgoParams: indexdef.IndexAlgoParams,
					IndexDefs:       make(map[string]*IndexDef),
				}
			}
			multiTableIndexes[groupKey].IndexDefs[catalog.ToLower(indexdef.IndexAlgoTableType)] = indexdef
		case catalog.IsFullTextIndexAlgo(indexdef.IndexAlgo):
			if seenFullTextTbls[indexdef.IndexTableName] {
				continue
			}
			seenFullTextTbls[indexdef.IndexTableName] = true
			if err := buildPreInsertFullTextIndex(nil, builder.compCtx, builder, bindCtx, objRef,
				tableDef, 0, indexSourceStep, nil, indexdef, idx, nil); err != nil {
				return err
			}
		case catalog.IsMasterIndexAlgo(indexdef.IndexAlgo):
			if err := buildPreInsertMasterIndex(nil, builder.compCtx, builder, bindCtx, objRef,
				tableDef, indexSourceStep, nil, indexdef, idx); err != nil {
				return err
			}
		}
		// HNSW/CAGRA/IVF-PQ are absent here but on purpose — cron maintains them
		// off the base-table CDC.
	}

	multiTableIndexSourceSteps := make([]int32, 0, len(multiTableIndexesBySource))
	for step := range multiTableIndexesBySource {
		multiTableIndexSourceSteps = append(multiTableIndexSourceSteps, step)
	}
	slices.Sort(multiTableIndexSourceSteps)
	for _, step := range multiTableIndexSourceSteps {
		if err := buildPreInsertMultiTableIndexes(builder.compCtx, builder, bindCtx, objRef,
			tableDef, step, multiTableIndexesBySource[step]); err != nil {
			return err
		}
	}

	return nil
}

func (builder *QueryBuilder) irregularMaintenanceSourceStep(indexdef *plan.IndexDef, fallback int32) int32 {
	if step, ok := builder.irregularMaintValueChangedSourceSteps[irregularIndexGroupKey(indexdef)]; ok {
		return step
	}
	return fallback
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

	ivfIndexes := make(map[string]*MultiTableIndex)
	ivfSourceSteps := make(map[string]int32)
	// As in the insert path, a multi-column fulltext index is several IndexDefs
	// over one index table; its stale entries must be dropped once, not once per
	// indexed column.
	seenFullTextTbls := make(map[string]bool)
	for _, indexdef := range tableDef.Indexes {
		if !indexdef.TableExist {
			continue
		}
		sourceStep := builder.irregularMaintenanceSourceStep(indexdef, builder.irregularMaintDeleteStep)
		switch {
		case catalog.IsFullTextIndexAlgo(indexdef.IndexAlgo):
			if seenFullTextTbls[indexdef.IndexTableName] {
				continue
			}
			seenFullTextTbls[indexdef.IndexTableName] = true
			if err := builder.buildIrregularFulltextDeleteByPk(bindCtx, indexdef, sourceStep); err != nil {
				return err
			}
		case catalog.IsIvfIndexAlgo(indexdef.IndexAlgo):
			groupKey := irregularIndexGroupKey(indexdef)
			if _, ok := ivfIndexes[groupKey]; !ok {
				ivfIndexes[groupKey] = &MultiTableIndex{
					IndexAlgo:       catalog.ToLower(indexdef.IndexAlgo),
					IndexAlgoParams: indexdef.IndexAlgoParams,
					IndexDefs:       make(map[string]*IndexDef),
				}
				ivfSourceSteps[groupKey] = sourceStep
			}
			ivfIndexes[groupKey].IndexDefs[catalog.ToLower(indexdef.IndexAlgoTableType)] = indexdef
		case catalog.IsMasterIndexAlgo(indexdef.IndexAlgo):
			if err := builder.buildIrregularMasterDeleteByPk(bindCtx, indexdef, sourceStep); err != nil {
				return err
			}
		}
	}

	ivfIndexKeys := make([]string, 0, len(ivfIndexes))
	for groupKey := range ivfIndexes {
		ivfIndexKeys = append(ivfIndexKeys, groupKey)
	}
	slices.Sort(ivfIndexKeys)
	for _, groupKey := range ivfIndexKeys {
		mti := ivfIndexes[groupKey]
		if err := builder.buildIrregularIvfDeleteByPk(bindCtx, mti, ivfSourceSteps[groupKey]); err != nil {
			return err
		}
	}

	return nil
}

// deletePkColExpr returns the base-table PK column of the materialized maintenance
// step, the key the stale index entries are matched against.
func (builder *QueryBuilder) deletePkColExpr(relPos, sourceStep int32) *plan.Expr {
	deletePkPos := builder.irregularMaintDeletePkPos
	// The delete-PK position was recorded against the pre-prune materialized
	// image. Translate it only when pruning made that position out of range; an
	// unconditional remap mis-keys the in-range REPLACE delete by one column.
	if builder.sinkColRef != nil {
		sinkNode := builder.qry.Nodes[builder.qry.Steps[sourceStep]]
		if int(deletePkPos) >= len(sinkNode.ProjectList) {
			if newPos, ok := builder.sinkColRef[[2]int32{sourceStep, deletePkPos}]; ok {
				deletePkPos = int32(newPos)
			}
		}
	}
	return &plan.Expr{
		Typ:  builder.irregularMaintDeletePkTyp,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: relPos, ColPos: deletePkPos}},
	}
}

// buildIrregularIvfDeleteByPk drops the stale IVF entries keyed by the recorded PK
// position. It projects only the entries row_id + compound pk into the join output
// (the join key is the integer PK, never the vector), so it is robust to the
// materialized layout and never copies the base vector through the hash join.
func (builder *QueryBuilder) buildIrregularIvfDeleteByPk(bindCtx *BindContext, multiTableIndex *MultiTableIndex, sourceStep int32) error {
	async, err := catalog.IndexParamAsync(multiTableIndex.IndexAlgoParams)
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
	scanTableDef := CloneTableDefForPlan(entriesTableDef, false)
	scanTableDef.Cols = scanCols
	ivfScanID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       &plan.Stats{},
		ObjRef:      entriesObjRef,
		TableDef:    scanTableDef,
		ProjectList: scanProj,
	}, bindCtx)

	// direct sink-scan of the shared materialized image (no intermediate sink).
	srcScan := appendSinkScanNode(builder, bindCtx, sourceStep)

	// join entries (left) with the image (right) on origin_pk == old/final PK.
	cond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
		{Typ: orgPkTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 2}}},
		builder.deletePkColExpr(1, sourceStep),
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
func (builder *QueryBuilder) buildIrregularFulltextDeleteByPk(bindCtx *BindContext, indexdef *plan.IndexDef, sourceStep int32) error {
	async, err := indexplugin.IsAsync(indexdef.IndexAlgo, indexdef.IndexAlgoParams)
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
	scanTableDef := CloneTableDefForPlan(indexTableDef, false)
	scanTableDef.Cols = scanCols
	idxScanID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       &plan.Stats{},
		ObjRef:      indexObjRef,
		TableDef:    scanTableDef,
		ProjectList: scanProj,
	}, bindCtx)

	srcScan := appendSinkScanNode(builder, bindCtx, sourceStep)

	// join index (left) with the image (right) on doc_id == old/final PK.
	cond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
		{Typ: docIdTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1}}},
		builder.deletePkColExpr(1, sourceStep),
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
func (builder *QueryBuilder) buildIrregularMasterDeleteByPk(bindCtx *BindContext, indexdef *plan.IndexDef, sourceStep int32) error {
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
	scanTableDef := CloneTableDefForPlan(indexTableDef, false)
	scanTableDef.Cols = scanCols
	idxScanID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       &plan.Stats{},
		ObjRef:      indexObjRef,
		TableDef:    scanTableDef,
		ProjectList: scanProj,
	}, bindCtx)

	srcScan := appendSinkScanNode(builder, bindCtx, sourceStep)

	// join index (left) with the image (right) on __mo_index_pri_col == old/final PK.
	cond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
		{Typ: priColTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1}}},
		builder.deletePkColExpr(1, sourceStep),
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
	if len(builder.irregularMaintIndexes) == 0 && len(builder.irregularMaintInsertOnlyIndexes) == 0 && len(builder.irregularUpdateMaints) == 0 {
		return nil
	}
	if len(builder.irregularUpdateMaints) > 0 {
		for _, maint := range builder.irregularUpdateMaints {
			builder.irregularMaintSourceStep = maint.sourceStep
			builder.irregularMaintDeleteStep = maint.deleteStep
			builder.irregularMaintDeletePkPos = maint.deletePkPos
			builder.irregularMaintDeletePkTyp = maint.deletePkTyp
			builder.irregularMaintIndexes = maint.indexes
			builder.irregularMaintInsertOnlySourceStep = maint.insertOnlySourceStep
			builder.irregularMaintInsertOnlyIndexes = maint.insertOnlyIndexes
			builder.irregularMaintValueChangedSourceSteps = maint.valueChangedSourceSteps
			builder.irregularMaintTableDef = maint.tableDef
			builder.irregularMaintObjRef = maint.objRef
			if err := builder.buildIrregularIndexMaintenance(bindCtx); err != nil {
				return err
			}
		}
	} else if len(builder.irregularMaintIndexes) > 0 || len(builder.irregularMaintInsertOnlyIndexes) > 0 {
		if err := builder.buildIrregularIndexMaintenance(bindCtx); err != nil {
			return err
		}
	}
	reduceSinkSinkScanNodes(query)
	builder.tempOptimizeForDML()
	builder.determineShuffleForDMLSteps()
	applyLockTableFallback(builder)
	return nil
}

func (builder *QueryBuilder) determineShuffleForDMLSteps() {
	// Irregular-index maintenance is appended after createQuery, while the normal
	// shuffle passes run inside createQuery. tempOptimizeForDML recalculates every
	// final step with HashmapStats reset, so rerun the passes for the complete,
	// reduced graph after statistics are final; otherwise both existing joins and
	// late IVF/fulltext joins retain Shuffle=false and build a broadcast hash table.
	for i := range builder.qry.Steps {
		rootID := builder.qry.Steps[i]
		determineShuffleMethodAfterRemap(rootID, builder)
		determineShuffleMethod2(rootID, -1, builder)
		builder.forceJoinOnOneCN(rootID, false)
	}

	// createQuery generates runtime filters after the final shuffle decision.
	// Repeat that pass only after every late DML step has finalized shuffle:
	// shuffle hashbuild requires a (PASS) runtime-filter spec even when the probe
	// side is a SINK_SCAN and no data filtering can be pushed into it.
	for i := range builder.qry.Steps {
		builder.generateRuntimeFilters(builder.qry.Steps[i])
	}
}

// buildOnDupTargetPkResolution builds the ordered conflict-resolution subgraph
// for INSERT ... ON DUPLICATE KEY UPDATE. Treating a real PRIMARY as constraint
// zero, it probes every constraint against the pre-statement snapshot, then a
// single PRE_INSERT_UK arbiter resolves rows in input order against both those
// probe targets and keys published by earlier INSERT actions in this statement.
// UPDATE actions publish no incoming keys: ODKU rejects UNIQUE-key assignments,
// so those candidate values never become stored state.
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
	incomingProjectList []*plan.Expr,
) (int32, int32, int32, error) {
	objRef := dmlCtx.objRefs[0]
	pkName := tableDef.Pkey.PkeyColName
	pkColIdx := tableDef.Name2ColIndex[pkName]
	pkTyp := tableDef.Cols[pkColIdx].Typ
	incomingPkPos := colName2Idx[tableDef.Name+"."+pkName]

	keyExprs := make([]*plan.Expr, 0, len(tableDef.Indexes)+1)
	targetExprs := make([]*plan.Expr, 0, len(tableDef.Indexes)+1)

	if pkName != catalog.FakePrimaryKeyColName {
		probeTag := builder.genNewBindTag()
		builder.addNameByColRef(probeTag, tableDef)
		probeScanID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     tableDef,
			ObjRef:       objRef,
			BindingTags:  []int32{probeTag},
			ScanSnapshot: bindCtx.snapshot,
		}, bindCtx)
		inputPK := &plan.Expr{Typ: pkTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: selectTag, ColPos: incomingPkPos,
		}}}
		existingPK := &plan.Expr{Typ: pkTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: probeTag, ColPos: pkColIdx,
		}}}
		var err error
		inputPK, err = bindPrimaryKeyIdentityExpr(builder, inputPK, pkTyp)
		if err != nil {
			return 0, 0, 0, err
		}
		existingPK, err = bindPrimaryKeyIdentityExpr(builder, existingPK, pkTyp)
		if err != nil {
			return 0, 0, 0, err
		}
		probeCond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			existingPK, DeepCopyExpr(inputPK),
		})
		if err != nil {
			return 0, 0, 0, err
		}
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{lastNodeID, probeScanID},
			JoinType: plan.Node_LEFT,
			OnList:   []*plan.Expr{probeCond},
		}, bindCtx)
		keyExprs = append(keyExprs, inputPK)
		targetExprs = append(targetExprs, &plan.Expr{
			Typ: pkTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: probeTag, ColPos: pkColIdx}},
		})
	}

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
				incomingProjectList[incomingValPos].Typ, prefixLengths)
			if err != nil {
				return 0, 0, 0, err
			}
		} else {
			incomingValPos := colName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName]
			incomingValExpr = &plan.Expr{
				Typ:  incomingProjectList[incomingValPos].Typ,
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
		keyExprs = append(keyExprs, DeepCopyExpr(incomingValExpr))
		targetExprs = append(targetExprs, &plan.Expr{
			Typ:  priColTyp,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: idxTag, ColPos: priColPos}},
		})
	}

	if len(keyExprs) == 0 || len(keyExprs) != len(targetExprs) {
		return 0, 0, 0, moerr.NewInternalError(builder.GetContext(),
			"ODKU target arbitration requires at least one unique constraint")
	}

	projectTag := builder.genNewBindTag()
	projectList := make([]*plan.Expr, 0, len(incomingProjectList)+2*len(keyExprs))
	for i, expr := range incomingProjectList {
		projectList = append(projectList, &plan.Expr{
			Typ:  expr.Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectTag, ColPos: int32(i)}},
		})
	}
	keyColumns := make([]int32, len(keyExprs))
	targetColumns := make([]int32, len(targetExprs))
	for i := range keyExprs {
		keyColumns[i] = int32(len(projectList))
		projectList = append(projectList, keyExprs[i])
		targetColumns[i] = int32(len(projectList))
		projectList = append(projectList, targetExprs[i])
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: projectList,
		Children:    []int32{lastNodeID},
		BindingTags: []int32{projectTag},
	}, bindCtx)

	outputTag := builder.genNewBindTag()
	outputProject := make([]*plan.Expr, 0, len(incomingProjectList)+1)
	for i, expr := range incomingProjectList {
		outputProject = append(outputProject, &plan.Expr{
			Typ: expr.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: projectTag, ColPos: int32(i)}},
		})
	}
	targetPkPos := int32(len(outputProject))
	outputProject = append(outputProject, &plan.Expr{
		Typ: pkTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: projectTag, ColPos: incomingPkPos}},
	})
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PRE_INSERT_UK,
		Children:    []int32{lastNodeID},
		ProjectList: outputProject,
		BindingTags: []int32{outputTag},
		PreInsertUkCtx: &plan.PreInsertUkCtx{
			PkColumn:              incomingPkPos,
			PkType:                pkTyp,
			OdkuTargetArbitration: true,
			KeyColumns:            keyColumns,
			TargetColumns:         targetColumns,
			OutputColumns:         int32(len(incomingProjectList)),
		},
	}, bindCtx)
	return lastNodeID, outputTag, targetPkPos, nil
}

// appendModernChildFkMarkOks appends, for every non-self-referencing foreign key, a
// MARK join against the parent table and returns one boolean "ok" expression per FK,
// true when the parent row exists or any FK column is NULL (MATCH SIMPLE). A MARK join
// emits exactly one output row per input row, so — unlike a LEFT join — it never
// multiplies child rows when the referenced parent column is non-unique (e.g. a FK to
// the leading prefix of a composite primary key). The joins are binding-tagged, so the
// result survives the full optimizer and is robust to the appended index-helper
// columns a unique-key child carries. childColPos maps a child FK column name to its
// position under selectTag. lockRows is false only for ODKU's transient action
// validation stream; its retained final action is locked and revalidated later.
func (builder *QueryBuilder) appendModernChildFkMarkOks(
	bindCtx *BindContext,
	tableDef *plan.TableDef,
	lastNodeID int32,
	selectTag int32,
	childColPos func(colName string) int32,
	lockRows bool,
) (int32, []*plan.Expr, error) {
	selectNode := builder.updateInputProjectNode(lastNodeID)
	inputTypes := make([]plan.Type, len(selectNode.ProjectList))
	for i, expr := range selectNode.ProjectList {
		inputTypes[i] = expr.Typ
	}

	id2name := make(map[uint64]string, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		id2name[col.ColId] = col.Name
	}

	nonSelfFks := make([]*plan.ForeignKeyDef, 0, len(tableDef.Fkeys))
	for _, fk := range tableDef.Fkeys {
		if fk.ForeignTbl != 0 {
			nonSelfFks = append(nonSelfFks, fk)
		}
	}
	if len(nonSelfFks) == 0 {
		return lastNodeID, nil, nil
	}
	lockForeignKeys := lockRows
	if lockForeignKeys {
		if proc := builder.compCtx.GetProcess(); proc != nil {
			if txnOp := proc.GetTxnOperator(); txnOp != nil {
				lockForeignKeys = txnOp.Txn().IsPessimistic()
			}
		}
	}

	parentColNames := func(parent *plan.TableDef, colIDs []uint64) ([]string, error) {
		idToName := make(map[uint64]string, len(parent.Cols))
		for _, col := range parent.Cols {
			idToName[col.ColId] = col.Name
		}
		names := make([]string, len(colIDs))
		for i, id := range colIDs {
			var ok bool
			if names[i], ok = idToName[id]; !ok {
				return nil, moerr.NewInternalErrorf(builder.GetContext(),
					"foreign-key parent column %d not found", id)
			}
		}
		return names, nil
	}
	partsEqual := func(parts, names []string) bool {
		if len(parts) != len(names) {
			return false
		}
		for i := range parts {
			if catalog.ResolveAlias(parts[i]) != names[i] {
				return false
			}
		}
		return true
	}
	validationFks := nonSelfFks
	type foreignKeyLock struct {
		tableDef  *plan.TableDef
		objRef    *plan.ObjectRef
		expr      *plan.Expr
		typ       plan.Type
		lockTable bool
	}
	foreignKeyLocks := make([]foreignKeyLock, 0, len(nonSelfFks))
	if lockForeignKeys {
		type orderedFK struct {
			fk  *plan.ForeignKeyDef
			key string
		}
		ordered := make([]orderedFK, 0, len(nonSelfFks))
		for _, fk := range nonSelfFks {
			_, parentTableDef, err := builder.compCtx.ResolveById(fk.ForeignTbl, bindCtx.snapshot)
			if err != nil {
				return 0, nil, err
			}
			if parentTableDef == nil {
				return 0, nil, moerr.NewInternalErrorf(builder.GetContext(), "parent table %d not found", fk.ForeignTbl)
			}
			referencedNames, err := parentColNames(parentTableDef, fk.ForeignCols)
			if err != nil {
				return 0, nil, err
			}
			pkeyNames := []string(nil)
			if parentTableDef.Pkey != nil {
				pkeyNames = parentTableDef.Pkey.Names
				if len(pkeyNames) == 0 && parentTableDef.Pkey.PkeyColName != "" {
					pkeyNames = []string{parentTableDef.Pkey.PkeyColName}
				}
			}
			// Base-table locks sort before hidden-index locks, matching the parent
			// REPLACE pre-phase. Hidden targets then use the physical index-table
			// name as the stable order shared by both sides.
			targetKey := "0:"
			if !partsEqual(pkeyNames, referencedNames) {
				if err := validateTableIndexDefinitions(parentTableDef); err != nil {
					return 0, nil, err
				}
				for _, idxDef := range parentTableDef.Indexes {
					if idxDef.Unique && partsEqual(idxDef.Parts, referencedNames) {
						targetKey = "1:" + idxDef.IndexTableName
						break
					}
				}
			}
			ordered = append(ordered, orderedFK{
				fk:  fk,
				key: fmt.Sprintf("%020d:%s", fk.ForeignTbl, targetKey),
			})
		}
		slices.SortStableFunc(ordered, func(left, right orderedFK) int {
			return strings.Compare(left.key, right.key)
		})
		nonSelfFks = make([]*plan.ForeignKeyDef, len(ordered))
		for i := range ordered {
			nonSelfFks[i] = ordered[i].fk
		}
	}

	if lockForeignKeys {
		for _, fk := range nonSelfFks {
			parentObjRef, parentTableDef, err := builder.compCtx.ResolveById(fk.ForeignTbl, bindCtx.snapshot)
			if err != nil {
				return 0, nil, err
			}
			if parentTableDef == nil {
				return 0, nil, moerr.NewInternalErrorf(builder.GetContext(), "parent table %d not found", fk.ForeignTbl)
			}
			referencedNames, err := parentColNames(parentTableDef, fk.ForeignCols)
			if err != nil {
				return 0, nil, err
			}
			childExprs := make([]*plan.Expr, len(fk.Cols))
			for i, childColID := range fk.Cols {
				pos := childColPos(id2name[childColID])
				childTyp := tableDef.Cols[tableDef.Name2ColIndex[id2name[childColID]]].Typ
				if pos >= 0 && int(pos) < len(inputTypes) {
					childTyp = inputTypes[pos]
				}
				childExpr := &plan.Expr{Typ: childTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: selectTag, ColPos: int32(pos),
				}}}
				var parentCol *plan.ColDef
				for _, col := range parentTableDef.Cols {
					if col.ColId == fk.ForeignCols[i] {
						parentCol = col
						break
					}
				}
				if parentCol == nil {
					return 0, nil, moerr.NewInternalErrorf(builder.GetContext(),
						"foreign-key parent column %s not found", referencedNames[i])
				}
				childExprs[i], err = makePlan2AssignmentCastExpr(
					builder.GetContext(), childExpr, parentCol.Typ)
				if err != nil {
					return 0, nil, err
				}
			}

			var lockExpr *plan.Expr
			var lockTableDef *plan.TableDef
			var lockObjRef *plan.ObjectRef
			lockTable := false
			var pkeyNames []string
			if parentTableDef.Pkey != nil {
				pkeyNames = parentTableDef.Pkey.Names
				if len(pkeyNames) == 0 && parentTableDef.Pkey.PkeyColName != "" {
					pkeyNames = []string{parentTableDef.Pkey.PkeyColName}
				}
			}
			if partsEqual(pkeyNames, referencedNames) {
				lockTableDef = parentTableDef
				lockObjRef = parentObjRef
				if len(childExprs) == 1 {
					lockExpr = childExprs[0]
				} else {
					lockExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", childExprs)
					if err != nil {
						return 0, nil, err
					}
				}
			} else {
				if err := validateTableIndexDefinitions(parentTableDef); err != nil {
					return 0, nil, err
				}
				var matchedIndex *plan.IndexDef
				for _, idxDef := range parentTableDef.Indexes {
					if idxDef.Unique && partsEqual(idxDef.Parts, referencedNames) {
						matchedIndex = idxDef
						break
					}
				}
				if matchedIndex == nil {
					// Legacy schemas may reference a non-unique prefix of a composite key.
					// Such a reference has no physical point-lock key, so serialize it with
					// parent mutations before the validation scan using a shared table lock.
					lockTableDef = parentTableDef
					lockObjRef = parentObjRef
					lockExpr = childExprs[0]
					lockTable = true
				} else {
					lockObjRef, lockTableDef, err = builder.compCtx.ResolveIndexTableByRef(
						parentObjRef, matchedIndex.IndexTableName, bindCtx.snapshot)
					if err != nil {
						return 0, nil, err
					}
					prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(matchedIndex.IndexAlgoParams)
					if err != nil {
						return 0, nil, err
					}
					keyParts := make([]*plan.Expr, len(childExprs))
					for i, expr := range childExprs {
						keyParts[i], err = builder.makeIndexPartExprFromInputExpr(expr, referencedNames[i], prefixLengths)
						if err != nil {
							return 0, nil, err
						}
					}
					if indexTableStoresSerializedKey(matchedIndex) {
						lockExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", keyParts)
						if err != nil {
							return 0, nil, err
						}
					} else {
						lockExpr = keyParts[0]
					}
				}
			}
			lockPkPos, lockTyp := getPkPos(lockTableDef, false)
			if lockPkPos < 0 {
				return 0, nil, moerr.NewInternalErrorf(builder.GetContext(),
					"foreign-key lock table %s has no primary key", lockTableDef.Name)
			}
			foreignKeyLocks = append(foreignKeyLocks, foreignKeyLock{
				tableDef:  lockTableDef,
				objRef:    lockObjRef,
				expr:      lockExpr,
				typ:       lockTyp,
				lockTable: lockTable,
			})
		}
	}

	if lockForeignKeys {
		rowProject := getProjectionByLastNodeWithTag(builder, lastNodeID, selectTag)
		lockTag := builder.genNewBindTag()
		lockProject := slices.Clone(rowProject)
		lockTargets := make([]*plan.LockTarget, 0, len(foreignKeyLocks))
		for _, fkLock := range foreignKeyLocks {
			lockProject = append(lockProject, fkLock.expr)
			lockTargets = append(lockTargets, &plan.LockTarget{
				TableId: fkLock.tableDef.TblId, ObjRef: fkLock.objRef,
				PrimaryColIdxInBat: int32(len(lockProject) - 1), PrimaryColRelPos: lockTag,
				PrimaryColTyp: fkLock.typ, Mode: lockpb.LockMode_Shared, LockTable: fkLock.lockTable,
			})
		}
		baseTableIDs := make(map[uint64]struct{}, len(nonSelfFks))
		for _, fk := range nonSelfFks {
			baseTableIDs[fk.ForeignTbl] = struct{}{}
		}
		sortForeignKeyLockTargets(lockTargets, baseTableIDs)
		lockInputID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_PROJECT, Children: []int32{lastNodeID},
			ProjectList: lockProject, BindingTags: []int32{lockTag},
		}, bindCtx)
		lockOutput := getProjectionByLastNodeWithTag(builder, lockInputID, lockTag)
		lockNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_LOCK_OP, Children: []int32{lockInputID},
			TableDef: foreignKeyLocks[0].tableDef, LockTargets: lockTargets,
		}, bindCtx)
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_PROJECT, Children: []int32{lockNodeID},
			ProjectList: slices.Clone(lockOutput[:len(rowProject)]), BindingTags: []int32{selectTag},
		}, bindCtx)

		// Materialize the row image only after every referenced key has been locked.
		// The final validation/DML step consumes this single sink dependency, avoiding
		// unsupported multi-hop sink chains while preserving lock-before-scan ordering.
		lockSinkID := appendSinkNodeWithTag(builder, bindCtx, lastNodeID, selectTag)
		lockStep := builder.appendStep(lockSinkID)
		lastNodeID = builder.appendTaggedSinkScan(bindCtx, lockStep, selectTag)
		selectNode = builder.qry.Nodes[lastNodeID]
	}
	oks := make([]*plan.Expr, 0, len(nonSelfFks))
	for _, fk := range validationFks {
		parentObjRef, parentTableDef, err := builder.compCtx.ResolveById(fk.ForeignTbl, bindCtx.snapshot)
		if err != nil {
			return 0, nil, err
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
			childTyp := tableDef.Cols[tableDef.Name2ColIndex[id2name[childColId]]].Typ
			if childPos >= 0 && int(childPos) < len(selectNode.ProjectList) {
				childTyp = selectNode.ProjectList[childPos].Typ
			}
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

	lastNodeID, oks, err := builder.appendModernChildFkMarkOks(bindCtx, tableDef, lastNodeID, selectTag, childColPos, true)
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
		func(colName string) int32 { return colName2Idx[tableDef.Name+"."+colName] }, true)
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

func (builder *QueryBuilder) appendInsertIgnoreMultiDedup(
	bindCtx *BindContext,
	tableDef *plan.TableDef,
	objRef *plan.ObjectRef,
	lastNodeID int32,
	selectTag int32,
	selectNode *plan.Node,
	colName2Idx map[string]int32,
	skipUniqueIdx []bool,
	appendedUniqueProjs map[string]*plan.Expr,
	idxObjRefs []*plan.ObjectRef,
	idxTableDefs []*plan.TableDef,
) (int32, int32, *plan.Node, error) {
	baseWidth := len(selectNode.ProjectList)
	keyExprs := make([]*plan.Expr, 0, len(tableDef.Indexes)+1)
	conflictExprs := make([]*plan.Expr, 0, len(tableDef.Indexes)+1)

	if tableDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
		scanTag := builder.genNewBindTag()
		builder.addNameByColRef(scanTag, tableDef)
		scanNodeID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     tableDef,
			ObjRef:       objRef,
			BindingTags:  []int32{scanTag},
			ScanSnapshot: bindCtx.snapshot,
		}, bindCtx)

		pkName := tableDef.Pkey.PkeyColName
		pkPos := tableDef.Name2ColIndex[pkName]
		inputPkPos := colName2Idx[tableDef.Name+"."+pkName]
		inputPK := &plan.Expr{Typ: tableDef.Cols[pkPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: selectTag, ColPos: inputPkPos,
		}}}
		existingPK := &plan.Expr{Typ: tableDef.Cols[pkPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: scanTag, ColPos: pkPos,
		}}}
		var err error
		inputPK, err = bindPrimaryKeyIdentityExpr(builder, inputPK, inputPK.Typ)
		if err != nil {
			return 0, 0, nil, err
		}
		existingPK, err = bindPrimaryKeyIdentityExpr(builder, existingPK, existingPK.Typ)
		if err != nil {
			return 0, 0, nil, err
		}
		keyExprs = append(keyExprs, DeepCopyExpr(inputPK))
		joinCond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{inputPK, existingPK})
		if err != nil {
			return 0, 0, nil, err
		}
		var markExpr *plan.Expr
		lastNodeID, markExpr, err = builder.insertMarkJoin(lastNodeID, scanNodeID, []*plan.Expr{joinCond}, nil, false, bindCtx)
		if err != nil {
			return 0, 0, nil, err
		}
		conflictExprs = append(conflictExprs, markExpr)
	}

	for i, idxDef := range tableDef.Indexes {
		if skipUniqueIdx[i] {
			continue
		}
		var err error
		idxObjRefs[i], idxTableDefs[i], err = builder.compCtx.ResolveIndexTableByRef(
			objRef, idxDef.IndexTableName, bindCtx.snapshot)
		if err != nil {
			return 0, 0, nil, err
		}
		if !idxDef.Unique {
			continue
		}

		idxTag := builder.genNewBindTag()
		builder.addNameByColRef(idxTag, idxTableDefs[i])
		idxScanNodeID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     idxTableDefs[i],
			ObjRef:       idxObjRefs[i],
			BindingTags:  []int32{idxTag},
			ScanSnapshot: bindCtx.snapshot,
		}, bindCtx)
		lookupColName := indexLookupColumnName(idxDef)
		lookupPos := idxTableDefs[i].Name2ColIndex[lookupColName]
		existingKey := &plan.Expr{Typ: idxTableDefs[i].Cols[lookupPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: idxTag, ColPos: lookupPos,
		}}}
		idxKeyName := idxDef.IndexTableName + "." + catalog.IndexTableIndexColName
		inputKey := DeepCopyExpr(appendedUniqueProjs[idxKeyName])
		keyExprs = append(keyExprs, DeepCopyExpr(inputKey))
		joinCond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{inputKey, existingKey})
		if err != nil {
			return 0, 0, nil, err
		}
		var markExpr *plan.Expr
		lastNodeID, markExpr, err = builder.insertMarkJoin(lastNodeID, idxScanNodeID, []*plan.Expr{joinCond}, nil, false, bindCtx)
		if err != nil {
			return 0, 0, nil, err
		}
		conflictExprs = append(conflictExprs, markExpr)
	}

	if len(keyExprs) < 2 {
		return 0, 0, nil, moerr.NewInternalError(builder.GetContext(),
			"INSERT IGNORE multi-key dedup requires at least two unique constraints")
	}
	projectTag := builder.genNewBindTag()
	projectList := make([]*plan.Expr, 0, baseWidth+2*len(keyExprs))
	for i, expr := range selectNode.ProjectList {
		projectList = append(projectList, &plan.Expr{Typ: expr.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: selectTag, ColPos: int32(i),
		}}})
	}
	keyColumns := make([]int32, len(keyExprs))
	conflictColumns := make([]int32, len(conflictExprs))
	for i, expr := range keyExprs {
		keyColumns[i] = int32(len(projectList))
		projectList = append(projectList, expr)
		conflictColumns[i] = int32(len(projectList))
		projectList = append(projectList, conflictExprs[i])
	}
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		ProjectList: projectList,
		BindingTags: []int32{projectTag},
	}, bindCtx)

	outputTag := builder.genNewBindTag()
	outputProject := make([]*plan.Expr, baseWidth)
	for i, expr := range selectNode.ProjectList {
		outputProject[i] = &plan.Expr{Typ: expr.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: projectTag, ColPos: int32(i),
		}}}
	}
	arbiterNode := &plan.Node{
		NodeType:    plan.Node_PRE_INSERT_UK,
		Children:    []int32{lastNodeID},
		ProjectList: outputProject,
		BindingTags: []int32{outputTag},
		PreInsertUkCtx: &plan.PreInsertUkCtx{
			InsertIgnoreMultiDedup: true,
			KeyColumns:             keyColumns,
			ConflictColumns:        conflictColumns,
			OutputColumns:          int32(baseWidth),
		},
	}
	lastNodeID = builder.appendNode(arbiterNode, bindCtx)
	return lastNodeID, outputTag, arbiterNode, nil
}

// collectGeneratedColumnDependents returns the set of columns that may change
// when any column in seed is assigned during ODKU. Generated columns are
// expanded to a fixed point so a generated column can depend on another
// generated column. The returned names are canonical table column names.
func collectGeneratedColumnDependents(ctx context.Context, tableDef *plan.TableDef, seed map[string]struct{}) (map[string]struct{}, error) {
	possiblyChanged := make(map[string]struct{}, len(seed))
	for name := range seed {
		resolved := catalog.ResolveAlias(name)
		colIdx, ok := tableDef.Name2ColIndex[resolved]
		if !ok || colIdx < 0 || int(colIdx) >= len(tableDef.Cols) {
			return nil, moerr.NewInternalErrorf(ctx,
				"cannot resolve generated column dependency seed %q", name)
		}
		possiblyChanged[tableDef.Cols[colIdx].Name] = struct{}{}
	}

	for {
		expanded := false
		for _, col := range tableDef.Cols {
			if col.GeneratedCol == nil {
				continue
			}
			references := collectRefColPos(col.GeneratedCol.Expr)
			for _, pos := range references {
				if pos < 0 || int(pos) >= len(tableDef.Cols) {
					return nil, moerr.NewInternalErrorf(ctx,
						"invalid generated column reference position %d for column %q", pos, col.Name)
				}
			}
			if _, alreadyChanged := possiblyChanged[col.Name]; alreadyChanged {
				continue
			}
			for _, pos := range references {
				if _, sourceChanged := possiblyChanged[tableDef.Cols[pos].Name]; sourceChanged {
					possiblyChanged[col.Name] = struct{}{}
					expanded = true
					break
				}
			}
		}
		if !expanded {
			return possiblyChanged, nil
		}
	}
}

func columnPossiblyChanged(tableDef *plan.TableDef, possiblyChanged map[string]struct{}, name string) bool {
	resolved := catalog.ResolveAlias(name)
	if colIdx, ok := tableDef.Name2ColIndex[resolved]; ok && colIdx >= 0 && int(colIdx) < len(tableDef.Cols) {
		resolved = tableDef.Cols[colIdx].Name
	}
	_, ok := possiblyChanged[resolved]
	return ok
}

// odkuAffectedActionConstraints returns only the row-level constraints whose
// result can change during the ordered ODKU assignment stream.  Validating an
// unrelated constraint is not merely wasted work: it can reject a historical
// row that was admitted while that constraint (for example FK checks) was
// disabled.  Generated-column dependencies are already present in
// possiblyChanged.
func odkuAffectedActionConstraints(
	ctx context.Context,
	tableDef *plan.TableDef,
	possiblyChanged map[string]struct{},
	includeForeignKeys bool,
) (*plan.TableDef, error) {
	filtered := *tableDef
	filtered.Checks = nil
	filtered.Fkeys = nil

	for _, check := range tableDef.Checks {
		if check == nil || check.Check == nil {
			return nil, moerr.NewInternalError(ctx, "ON DUPLICATE KEY UPDATE has malformed CHECK metadata")
		}
		for _, pos := range collectRefColPos(check.Check) {
			if pos < 0 || int(pos) >= len(tableDef.Cols) {
				return nil, moerr.NewInternalErrorf(ctx,
					"ON DUPLICATE KEY UPDATE CHECK %s references invalid column position %d",
					check.Name, pos)
			}
			if _, changed := possiblyChanged[tableDef.Cols[pos].Name]; changed {
				filtered.Checks = append(filtered.Checks, check)
				break
			}
		}
	}

	if !includeForeignKeys {
		return &filtered, nil
	}
	colNameByID := make(map[uint64]string, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		colNameByID[col.ColId] = col.Name
	}
	for _, fk := range tableDef.Fkeys {
		if fk == nil {
			return nil, moerr.NewInternalError(ctx, "ON DUPLICATE KEY UPDATE has malformed foreign-key metadata")
		}
		if fk.ForeignTbl == 0 { // self FKs remain statement-level
			continue
		}
		if len(fk.Cols) == 0 {
			return nil, moerr.NewInternalErrorf(ctx,
				"ON DUPLICATE KEY UPDATE foreign key %s has no child columns", fk.Name)
		}
		for _, colID := range fk.Cols {
			name, ok := colNameByID[colID]
			if !ok {
				return nil, moerr.NewInternalErrorf(ctx,
					"ON DUPLICATE KEY UPDATE cannot locate foreign-key column %d", colID)
			}
			if _, changed := possiblyChanged[name]; changed {
				filtered.Fkeys = append(filtered.Fkeys, fk)
				break
			}
		}
	}
	return &filtered, nil
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
	// Cluster and temporary tables are ordinary user DML targets even though their
	// durable relkind is distinct; accept their catalog markers (or the temporary
	// table's session-scoped resolution bit) without admitting any of the internal
	// index table types.
	isOnDupUpdate := len(astUpdateExprs) > 0 &&
		!(len(astUpdateExprs) == 1 && astUpdateExprs[0] == nil)
	isRegularDMLTarget := tableDef.TableType == catalog.SystemOrdinaryRel ||
		tableDef.TableType == catalog.SystemIndexRel ||
		tableDef.TableType == catalog.SystemClusterRel ||
		tableDef.TableType == catalog.SystemTemporaryTable ||
		tableDef.IsTemporary
	if !isOnDupUpdate &&
		!isRegularDMLTarget {
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
	// Keep the executable assignment stream separate from updateExprs. SQL
	// assignments are ordered and a target may occur more than once; the map is
	// only the final per-column summary used by index/no-op planning.
	updateColIdxList := make([]int32, 0, len(astUpdateExprs))
	updateColExprList := make([]*plan.Expr, 0, len(astUpdateExprs))
	possiblyChangedCols := make(map[string]struct{})
	autoUpdateCols := make(map[string]bool)
	allExplicitAssignmentsSkipped := false

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

		binder := NewOndupUpdateBinder(
			builder.GetContext(), builder, bindCtx, scanTag, selectTag, tableDef,
			dmlCtx.targetDBName, dmlCtx.targetTableName, builder.compCtx.GetLowerCaseTableNames(),
		)
		var updateExpr *plan.Expr
		for _, astUpdateExpr := range astUpdateExprs {
			colName := astUpdateExpr.Names[0].ColName()
			colIdx, ok := tableDef.Name2ColIndex[colName]
			if !ok {
				return 0, moerr.NewBadFieldErrorf(builder.GetContext(), "invalid input: column '%s' does not exist", astUpdateExpr.Names[0].ColNameOrigin())
			}
			colDef := tableDef.Cols[colIdx]
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
				updateExpr, err = binder.BindAssignmentExpr(astExpr, colDef.Typ)
				if err != nil {
					return 0, err
				}
			}

			// Flink's MySQL JDBC dialect includes every column in the update
			// clause. When PRIMARY is the only conflict arbiter, pk = VALUES(pk)
			// is necessarily a no-op because the incoming and existing primary
			// keys are equal. Do not turn that dialect-generated assignment into
			// a physical primary-key update. If a secondary UNIQUE key can select
			// the conflicting row, the incoming primary key may differ, so retain
			// the existing rejection for that ambiguous case.
			if firstUniqueIdxPos < 0 &&
				slices.Contains(tableDef.Pkey.Names, colDef.Name) &&
				isOnDupIncomingColumn(updateExpr, selectTag, int32(colIdx)) {
				continue
			}

			updateExpr, err = builder.forceAssignmentCastExpr(updateExpr, colDef.Typ, false)
			if err != nil {
				return 0, err
			}
			updateExprs[colDef.Name] = updateExpr
			updateColIdxList = append(updateColIdxList, colIdx)
			updateColExprList = append(updateColExprList, updateExpr)
		}
		allExplicitAssignmentsSkipped = len(updateExprs) == 0

		for _, col := range tableDef.Cols {
			if col.OnUpdate != nil && col.OnUpdate.Expr != nil && updateExprs[col.Name] == nil {
				newDefExpr := DeepCopyExpr(col.OnUpdate.Expr)
				err = replaceFuncId(builder.GetContext(), newDefExpr)
				if err != nil {
					return 0, err
				}

				updateExprs[col.Name] = newDefExpr
				updateColIdxList = append(updateColIdxList, tableDef.Name2ColIndex[col.Name])
				updateColExprList = append(updateColExprList, newDefExpr)
				autoUpdateCols[col.Name] = true
			}
		}

		for i, updateExpr := range updateColExprList {
			lastNodeID, updateExpr, err = builder.flattenSubqueries(lastNodeID, updateExpr, bindCtx)
			if err != nil {
				return 0, err
			}
			updateColExprList[i] = updateExpr
			updateExprs[tableDef.Cols[updateColIdxList[i]].Name] = updateExpr
		}

		// Keep the dependency set separate from updateExprs: updateExprs is the
		// final per-column summary and receives every generated expression below,
		// while possiblyChangedCols describes which keys may need maintenance.
		seedCols := make(map[string]struct{}, len(updateExprs))
		for colName := range updateExprs {
			seedCols[colName] = struct{}{}
		}
		possiblyChangedCols, err = collectGeneratedColumnDependents(
			builder.GetContext(), tableDef, seedCols,
		)
		if err != nil {
			return 0, err
		}

		// Generated expressions are appended to the same ordered stream and read
		// the current row image. Do not inline earlier assignments: doing so would
		// re-evaluate volatile expressions and would break left-to-right semantics.
		for i, col := range tableDef.Cols {
			if col.GeneratedCol == nil {
				continue
			}
			genExpr := builder.applyGeneratedColumnAssignmentCast(
				DeepCopyExpr(col.GeneratedCol.Expr),
				builder.isInsertIgnore,
			)
			replaceColRefTag(genExpr, 0, scanTag)
			updateExprs[col.Name] = genExpr
			updateColIdxList = append(updateColIdxList, int32(i))
			updateColExprList = append(updateColExprList, genExpr)
		}
	}

	for _, part := range tableDef.Pkey.Names {
		if columnPossiblyChanged(tableDef, possiblyChangedCols, part) {
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
	// INSERT IGNORE applies CHECK constraints as a FILTER. Materialize unique-key
	// lock columns before that filter so its pass-through projection preserves the
	// physical vectors consumed by LockOp.
	lockKeysMaterializedBeforeChecks := false
	if onDupAction == plan.Node_IGNORE && len(tableDef.Checks) > 0 {
		needsLockKeyProjection, err := hasMaterializedInsertUniqueLockKey(tableDef, skipUniqueIdx)
		if err != nil {
			return 0, err
		}
		if needsLockKeyProjection {
			lastNodeID, selectTag, selectNode = builder.appendInsertLockKeyProjection(
				bindCtx,
				lastNodeID,
			)
			if err = builder.materializeInsertUniqueLockKeys(tableDef, skipUniqueIdx, colName2Idx, selectNode); err != nil {
				return 0, err
			}
			lockKeysMaterializedBeforeChecks = true
		}
	}

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
		lastNodeID, err = appendCheckConstraintPlan(
			builder,
			bindCtx,
			tableDef,
			lastNodeID,
			selectTag,
			colName2Idx,
			true,
		)
		if err != nil {
			return 0, err
		}
		selectNode = builder.qry.Nodes[lastNodeID]

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
	if onDupAction != plan.Node_UPDATE && (len(irregularIndexes) > 0 || builder.returningRequested) {
		lastNodeID = builder.appendIrregularMaintSource(bindCtx, lastNodeID, irregularIndexes, tableDef, dmlCtx.objRefs[0], builder.returningRequested)
		selectNode = builder.qry.Nodes[lastNodeID]
		selectTag = selectNode.BindingTags[0]
	}

	idxNeedUpdate := make([]bool, len(tableDef.Indexes))
	for i, idxDef := range tableDef.Indexes {
		for _, part := range idxDef.Parts {
			if columnPossiblyChanged(tableDef, possiblyChangedCols, part) {
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
	if !lockKeysMaterializedBeforeChecks {
		if err = builder.materializeInsertUniqueLockKeys(tableDef, skipUniqueIdx, colName2Idx, selectNode); err != nil {
			return 0, err
		}
	}

	// ODKU with secondary UNIQUE constraints resolves one target identity per
	// action, including conflicts created by earlier input rows. The target_pk
	// re-keys the main DEDUP-update join below for both real and synthetic PKs.
	useTargetPk := onDupAction == plan.Node_UPDATE &&
		firstUniqueIdxPos >= 0 && !builder.canSkipDedup(tableDef)
	targetPkPos := int32(-1)
	affectedRowsInputPos := int32(-1)
	physicalChangedRowsInputPos := int32(-1)
	actionFinalInputPos := int32(-1)
	var odkuFkEligibilityInputPos []int32
	var odkuActionFkEligibilityInputPos []int32
	odkuCheckInsertEligibilityInputPos := int32(-1)
	odkuNeedFkCheck := false
	odkuActionConstraintDef := tableDef
	odkuAllForeignKeyDef := tableDef
	odkuFinalOnlyCheckDef := *tableDef
	odkuFinalOnlyCheckDef.Checks = nil
	odkuFinalOnlyCheckDef.Fkeys = nil
	odkuNotNullColIdxList := odkuUpdatedNotNullColumns(
		tableDef, updateColIdxList, updateColExprList)
	if onDupAction == plan.Node_UPDATE {
		fkChecksEnabled, err := builder.modernInsertFkCheckEnabled(tableDef)
		if err != nil {
			return 0, err
		}
		odkuActionConstraintDef, err = odkuAffectedActionConstraints(
			builder.GetContext(), tableDef, possiblyChangedCols, fkChecksEnabled)
		if err != nil {
			return 0, err
		}
		odkuAllForeignKeyDef, err = odkuNonSelfForeignKeys(
			builder.GetContext(), tableDef, fkChecksEnabled)
		if err != nil {
			return 0, err
		}
		odkuNeedFkCheck = len(odkuAllForeignKeyDef.Fkeys) > 0
		affectedChecks := make(map[*plan.CheckDef]struct{}, len(odkuActionConstraintDef.Checks))
		for _, check := range odkuActionConstraintDef.Checks {
			affectedChecks[check] = struct{}{}
		}
		for _, check := range tableDef.Checks {
			if _, affected := affectedChecks[check]; !affected {
				odkuFinalOnlyCheckDef.Checks = append(odkuFinalOnlyCheckDef.Checks, check)
			}
		}
	}
	emitODKUActionRows := onDupAction == plan.Node_UPDATE &&
		(len(odkuActionConstraintDef.Checks) > 0 || len(odkuActionConstraintDef.Fkeys) > 0 || len(odkuNotNullColIdxList) > 0)
	appendODKUFinalOnlyChecks := func(nodeID, inputTag int32) (int32, error) {
		if len(odkuFinalOnlyCheckDef.Checks) == 0 {
			return nodeID, nil
		}
		if odkuCheckInsertEligibilityInputPos < 0 {
			return 0, moerr.NewInternalError(builder.GetContext(),
				"ON DUPLICATE KEY UPDATE CHECK eligibility column is missing")
		}
		eligible := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: inputTag, ColPos: odkuCheckInsertEligibilityInputPos,
			}},
		}
		return appendCheckConstraintPlanWithColLookupAndEligibility(
			builder, bindCtx, &odkuFinalOnlyCheckDef, nodeID, inputTag,
			func(colName string) (int32, bool) {
				pos, ok := colName2Idx[tableDef.Name+"."+colName]
				return pos, ok
			}, false, eligible)
	}
	odkuActionValidationApplied := false
	if onDupAction == plan.Node_UPDATE {
		affectedRowsInputPos = int32(len(selectNode.ProjectList))
		selectNode.ProjectList = append(selectNode.ProjectList, makePlan2Uint64ConstExprWithType(1))
		physicalChangedRowsInputPos = int32(len(selectNode.ProjectList))
		selectNode.ProjectList = append(selectNode.ProjectList, MakePlan2BoolConstExprWithType(true))
		if emitODKUActionRows {
			actionFinalInputPos = int32(len(selectNode.ProjectList))
			selectNode.ProjectList = append(selectNode.ProjectList, MakePlan2BoolConstExprWithType(true))
		}
		if odkuNeedFkCheck {
			actionFKs := make(map[*plan.ForeignKeyDef]struct{}, len(odkuActionConstraintDef.Fkeys))
			for _, fk := range odkuActionConstraintDef.Fkeys {
				actionFKs[fk] = struct{}{}
			}
			for _, fk := range odkuAllForeignKeyDef.Fkeys {
				pos := int32(len(selectNode.ProjectList))
				odkuFkEligibilityInputPos = append(
					odkuFkEligibilityInputPos, pos)
				if _, affected := actionFKs[fk]; affected {
					odkuActionFkEligibilityInputPos = append(odkuActionFkEligibilityInputPos, pos)
				}
				selectNode.ProjectList = append(selectNode.ProjectList, MakePlan2BoolConstExprWithType(true))
			}
		}
		if len(odkuFinalOnlyCheckDef.Checks) > 0 {
			odkuCheckInsertEligibilityInputPos = int32(len(selectNode.ProjectList))
			selectNode.ProjectList = append(selectNode.ProjectList, MakePlan2BoolConstExprWithType(true))
		}
	}
	if useTargetPk {
		oldSelectTag := selectTag
		// Append action metadata before target resolution so the stateful arbiter's
		// resolved target remains the final output column. PRE_INSERT_UK emits a
		// physical batch directly rather than evaluating its Plan.ProjectList; adding
		// metadata after this node would shift the runtime target away from the column
		// consumed by the main DEDUP join.
		lastNodeID, selectTag, targetPkPos, err = builder.buildOnDupTargetPkResolution(
			bindCtx, dmlCtx, tableDef, lastNodeID, selectTag, colName2Idx, skipUniqueIdx, selectNode.ProjectList)
		if err != nil {
			return 0, err
		}
		selectNode = builder.qry.Nodes[lastNodeID]

		// The update expressions (e.g. VALUES(col)) were bound against the original
		// select tag; the resolution project re-projects every incoming column at
		// its original position under the new tag, so retarget those references.
		for _, updateExpr := range updateColExprList {
			replaceColRefTag(updateExpr, oldSelectTag, selectTag)
		}
	}

	objRef := dmlCtx.objRefs[0]
	idxObjRefs := make([]*plan.ObjectRef, len(tableDef.Indexes))
	idxTableDefs := make([]*plan.TableDef, len(tableDef.Indexes))

	//lock main table
	lockTargets := make([]*plan.LockTarget, 0, len(tableDef.Indexes)+1)
	pkInTableCols := false
	for _, col := range tableDef.Cols {
		if col.Name == pkName {
			pkInTableCols = true
			break
		}
	}
	mainLockColPos := int32(-1)
	if useTargetPk {
		// The arbiter resolves a secondary-UNIQUE conflict to the existing
		// base row's identity. Lock that resolved identity, not the incoming PK:
		// the latter can name a different row and therefore cannot serialize the
		// UPDATE. Synthetic-PK tables need the same base-row lock once the hidden
		// identity has been resolved.
		mainLockColPos = targetPkPos
	} else if pkName != catalog.FakePrimaryKeyColName && pkInTableCols {
		var ok bool
		mainLockColPos, ok = colName2Idx[tableDef.Name+"."+pkName]
		if !ok {
			return 0, moerr.NewInternalErrorf(builder.GetContext(),
				"bind insert err, can not find primary key projection %s", pkName)
		}
	}
	if mainLockColPos >= 0 {
		if int(mainLockColPos) >= len(selectNode.ProjectList) {
			return 0, moerr.NewInternalErrorf(builder.GetContext(),
				"bind insert err, invalid primary key projection %d", mainLockColPos)
		}
		lockTargets = append(lockTargets, &plan.LockTarget{
			TableId:            tableDef.TblId,
			ObjRef:             DeepCopyObjectRef(objRef),
			PrimaryColIdxInBat: mainLockColPos,
			PrimaryColRelPos:   selectTag,
			// Type the lock from the actual pipeline column. This also covers
			// composite PK encodings, whose synthetic storage column need not be
			// present in TableDef.Cols.
			PrimaryColTyp: selectNode.ProjectList[mainLockColPos].Typ,
			// LOAD owns the target table for the whole statement. Mark only
			// the base-table target so compile can acquire it once before the
			// pipeline; unique-index targets keep their row-level checks.
			LockTable: builder.qry.LoadTag,
		})
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
			partName := catalog.ResolveAlias(idxDef.Parts[0])
			pkIdxInBat, ok = colName2Idx[tableDef.Name+"."+partName]
			if !ok {
				return 0, moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", partName)
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
		lockTag := builder.genNewBindTag()
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_LOCK_OP,
			Children:    []int32{lastNodeID},
			TableDef:    tableDef,
			BindingTags: []int32{lockTag},
			LockTargets: lockTargets,
		}, bindCtx)
		if useTargetPk {
			if builder.preserveLockProjection == nil {
				builder.preserveLockProjection = make(map[int32]struct{})
			}
			builder.preserveLockProjection[lastNodeID] = struct{}{}
		}
		applyLockTableFallback(builder)
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
	uniqueConstraintCount := 0
	if !isFakePK {
		uniqueConstraintCount++
	}
	for i, idxDef := range tableDef.Indexes {
		if idxDef.Unique && !skipUniqueIdx[i] {
			uniqueConstraintCount++
		}
	}
	useInsertIgnoreMultiDedup := onDupAction == plan.Node_IGNORE &&
		uniqueConstraintCount > 1 && !builder.canSkipDedup(tableDef)
	if useInsertIgnoreMultiDedup {
		oldSelectTag := selectTag
		lastNodeID, selectTag, selectNode, err = builder.appendInsertIgnoreMultiDedup(
			bindCtx, tableDef, objRef, lastNodeID, selectTag, selectNode, colName2Idx,
			skipUniqueIdx, appendedUniqueProjs, idxObjRefs, idxTableDefs)
		if err != nil {
			return 0, err
		}
		for _, expr := range appendedUniqueProjs {
			replaceColRefTag(expr, oldSelectTag, selectTag)
		}
	} else if builder.canSkipDedup(tableDef) && onDupAction != plan.Node_UPDATE {
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
		if isFakePK && onDupAction == plan.Node_UPDATE && !useTargetPk {
			pkRoleIdxPos = firstUniqueIdxPos
		}

		if !skipPkDedup && (!isFakePK || pkRoleIdxPos >= 0 || useTargetPk) {
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
				leftPK := &plan.Expr{
					Typ:  pkTyp,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: pkPos}},
				}
				rightPK := &plan.Expr{
					Typ:  pkTyp,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectTag, ColPos: rightPkPos}},
				}
				leftPK, err = bindPrimaryKeyIdentityExpr(builder, leftPK, pkTyp)
				if err != nil {
					return 0, err
				}
				rightPK, err = bindPrimaryKeyIdentityExpr(builder, rightPK, pkTyp)
				if err != nil {
					return 0, err
				}
				joinCond, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{leftPK, rightPK})
				if err != nil {
					return 0, err
				}
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
				DedupInputKeysUnique: onDupAction == plan.Node_FAIL &&
					!isFakePK && builder.insertInputKeysUnique,
			}

			if onDupAction == plan.Node_UPDATE {
				oldColList := make([]plan.ColRef, len(tableDef.Cols))
				for i := range tableDef.Cols {
					oldColList[i].RelPos = scanTag
					oldColList[i].ColPos = int32(i)
				}

				noopSkipSeeds := make(map[string]struct{}, len(autoUpdateCols))
				for name := range autoUpdateCols {
					noopSkipSeeds[name] = struct{}{}
				}
				noopSkipCols, err := collectGeneratedColumnDependents(
					builder.GetContext(), tableDef, noopSkipSeeds,
				)
				if err != nil {
					return 0, err
				}
				updateCheckColIdxList := make([]int32, 0, len(updateExprs))
				for i, col := range tableDef.Cols {
					if col.Name == catalog.Row_ID || col.Hidden {
						continue
					}
					if _, skipped := noopSkipCols[col.Name]; skipped {
						continue
					}
					if _, written := updateExprs[col.Name]; written {
						updateCheckColIdxList = append(updateCheckColIdxList, int32(i))
					}
				}
				countFoundRows := false
				if proc := builder.compCtx.GetProcess(); proc != nil && proc.GetSessionInfo() != nil {
					countFoundRows = !proc.GetSessionInfo().CountUpdateChangedRows
				}
				dedupJoinNode.DedupJoinCtx = &plan.DedupJoinCtx{
					OldColList:             oldColList,
					UpdateColIdxList:       updateColIdxList,
					UpdateColExprList:      updateColExprList,
					AffectedRowsCol:        &plan.ColRef{RelPos: selectTag, ColPos: affectedRowsInputPos},
					PhysicalChangedRowsCol: &plan.ColRef{RelPos: selectTag, ColPos: physicalChangedRowsInputPos},
					UpdateCheckColIdxList:  updateCheckColIdxList,
					CountFoundRows:         countFoundRows,
					EmitActionRows:         emitODKUActionRows,
				}
				if emitODKUActionRows {
					dedupJoinNode.DedupJoinCtx.ActionFinalCol = &plan.ColRef{
						RelPos: selectTag, ColPos: actionFinalInputPos,
					}
				}
				if odkuNeedFkCheck {
					colByID := make(map[uint64]int32, len(tableDef.Cols))
					for i, col := range tableDef.Cols {
						colByID[col.ColId] = int32(i)
					}
					for fkIdx, fk := range odkuAllForeignKeyDef.Fkeys {
						check := plan.ODKUForeignKeyCheck{EligibilityCol: &plan.ColRef{
							RelPos: selectTag, ColPos: odkuFkEligibilityInputPos[fkIdx],
						}}
						for _, colID := range fk.Cols {
							pos, ok := colByID[colID]
							if !ok {
								return 0, moerr.NewInternalErrorf(builder.GetContext(),
									"ON DUPLICATE KEY UPDATE cannot locate foreign-key column %d", colID)
							}
							check.ColIdxList = append(check.ColIdxList, pos)
						}
						dedupJoinNode.DedupJoinCtx.ForeignKeyChecks = append(
							dedupJoinNode.DedupJoinCtx.ForeignKeyChecks, check)
					}
				}
				if odkuCheckInsertEligibilityInputPos >= 0 {
					pkPos, ok := tableDef.Name2ColIndex[pkName]
					if !ok || pkPos < 0 || int(pkPos) >= len(tableDef.Cols) {
						return 0, moerr.NewInternalError(builder.GetContext(),
							"ON DUPLICATE KEY UPDATE cannot locate primary-key eligibility column")
					}
					// Reuse the existing tuple-eligibility transport for the one
					// statement-level bit needed by unaffected CHECKs. The primary
					// key cannot be updated by ODKU, so it is eligible exactly for a
					// newly inserted group and false for an existing target.
					dedupJoinNode.DedupJoinCtx.ForeignKeyChecks = append(
						dedupJoinNode.DedupJoinCtx.ForeignKeyChecks, plan.ODKUForeignKeyCheck{
							ColIdxList: []int32{pkPos}, EligibilityCol: &plan.ColRef{
								RelPos: selectTag, ColPos: odkuCheckInsertEligibilityInputPos,
							},
						})
				}
			}

			lastNodeID = builder.appendNode(dedupJoinNode, bindCtx)
			if emitODKUActionRows {
				if len(odkuActionConstraintDef.Checks) > 0 {
					lastNodeID, err = appendCheckConstraintPlan(
						builder, bindCtx, odkuActionConstraintDef, lastNodeID, selectTag, colName2Idx, false)
					if err != nil {
						return 0, err
					}
				}
				lastNodeID, err = builder.appendODKUActionNotNullAssertions(
					bindCtx, tableDef, lastNodeID, selectTag, odkuNotNullColIdxList)
				if err != nil {
					return 0, err
				}
				if len(odkuActionConstraintDef.Fkeys) > 0 {
					var oks []*plan.Expr
					lastNodeID, oks, err = builder.appendModernChildFkMarkOks(
						bindCtx, odkuActionConstraintDef, lastNodeID, selectTag,
						func(colName string) int32 { return colName2Idx[tableDef.Name+"."+colName] }, false)
					if err != nil {
						return 0, err
					}
					if len(oks) != len(odkuActionFkEligibilityInputPos) {
						return 0, moerr.NewInternalError(builder.GetContext(),
							"ON DUPLICATE KEY UPDATE foreign-key eligibility count mismatch")
					}
					assertConds := make([]*plan.Expr, len(oks))
					fkErrExpr := makePlan2StringConstExprWithType(
						"Cannot add or update a child row: a foreign key constraint fails")
					for i, ok := range oks {
						eligible := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true}, Expr: &plan.Expr_Col{Col: &plan.ColRef{
							RelPos: selectTag, ColPos: odkuActionFkEligibilityInputPos[i],
						}}}
						ok, err = guardConstraintByEligibility(builder.GetContext(), ok, eligible)
						if err != nil {
							return 0, err
						}
						assertConds[i], err = BindFuncExprImplByPlanExpr(
							builder.GetContext(), "assert", []*plan.Expr{ok, DeepCopyExpr(fkErrExpr)})
						if err != nil {
							return 0, err
						}
					}
					lastNodeID = builder.appendNode(&plan.Node{
						NodeType: plan.Node_FILTER, Children: []int32{lastNodeID}, FilterList: assertConds,
						FilterIsBarrier: true,
					}, bindCtx)
				}
				lastNodeID = builder.appendNode(&plan.Node{
					NodeType: plan.Node_FILTER,
					Children: []int32{lastNodeID},
					FilterList: []*plan.Expr{{
						Typ:  plan.Type{Id: int32(types.T_bool), NotNullable: true},
						Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectTag, ColPos: actionFinalInputPos}},
					}},
					FilterIsBarrier: true,
				}, bindCtx)
				lastNodeID, err = appendODKUFinalOnlyChecks(lastNodeID, selectTag)
				if err != nil {
					return 0, err
				}
				odkuActionValidationApplied = true
			}
		}
		if emitODKUActionRows && !odkuActionValidationApplied {
			if len(odkuActionConstraintDef.Checks) > 0 {
				lastNodeID, err = appendCheckConstraintPlan(
					builder, bindCtx, odkuActionConstraintDef, lastNodeID, selectTag, colName2Idx, false)
				if err != nil {
					return 0, err
				}
			}
			lastNodeID, err = builder.appendODKUActionNotNullAssertions(
				bindCtx, tableDef, lastNodeID, selectTag, odkuNotNullColIdxList)
			if err != nil {
				return 0, err
			}
			if len(odkuActionConstraintDef.Fkeys) > 0 {
				lastNodeID, selectTag, err = builder.buildModernChildFkAssert(
					bindCtx, odkuActionConstraintDef, lastNodeID, selectTag,
					func(colName string) int32 { return colName2Idx[tableDef.Name+"."+colName] })
				if err != nil {
					return 0, err
				}
				selectNode = builder.qry.Nodes[lastNodeID]
			}
			lastNodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_FILTER,
				Children: []int32{lastNodeID},
				FilterList: []*plan.Expr{{
					Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: selectTag, ColPos: actionFinalInputPos,
					}},
				}},
				FilterIsBarrier: true,
			}, bindCtx)
			lastNodeID, err = appendODKUFinalOnlyChecks(lastNodeID, selectTag)
			if err != nil {
				return 0, err
			}
		}
		if onDupAction == plan.Node_UPDATE && !emitODKUActionRows {
			lastNodeID, err = appendODKUFinalOnlyChecks(lastNodeID, selectTag)
			if err != nil {
				return 0, err
			}
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

	// ODKU no-op guard: drop rows where every column the update actually writes
	// is NULL-safe-equal between the old image (scanTag) and the final written
	// value the dedup-update join materialized into the new image (selectTag).
	// MySQL returns affected-rows=0 for such rows.
	// Placed before the final PROJECT so scanTag columns survive column remapping.
	if onDupAction == plan.Node_UPDATE {
		// Columns excluded from the no-op equality chain: implicit ON UPDATE
		// columns (whose new value always advances) plus any generated column
		// that transitively derives from such a column — otherwise the recomputed
		// generated value would defeat the no-op guard even when the user's
		// explicit update changed nothing. A generated column whose source is a
		// user-updated column is still caught by that source column's own <=>.
		noopSkipSeeds := make(map[string]struct{}, len(autoUpdateCols))
		for name := range autoUpdateCols {
			noopSkipSeeds[name] = struct{}{}
		}
		noopSkipCols, err := collectGeneratedColumnDependents(
			builder.GetContext(), tableDef, noopSkipSeeds,
		)
		if err != nil {
			return 0, err
		}
		var allColsEqual *plan.Expr
		for i, col := range tableDef.Cols {
			if col.Name == catalog.Row_ID || col.Hidden {
				continue
			}
			if _, skipped := noopSkipCols[col.Name]; skipped {
				continue
			}
			// Only compare columns the update actually writes. A column absent from
			// updateExprs keeps its old value and is trivially unchanged, so it must
			// be excluded — otherwise an immutable key column resolved through a
			// secondary UNIQUE conflict (where the incoming PK differs from the
			// existing row's PK) would spuriously fail the equality chain and turn a
			// no-op update into a counted one.
			if _, written := updateExprs[col.Name]; !written {
				continue
			}
			// Compare the old value against the FINAL written value already
			// materialized by the dedup-update join, not a fresh evaluation of the
			// assignment expression. The join evaluates each update expression once
			// and writes the result back into the new-image (selectTag) column at
			// colName2Idx; re-executing it here would double-evaluate non-
			// deterministic assignments (e.g. v = floor(rand()*2)), so the no-op
			// check could disagree with the value actually stored.
			newColPos, ok := colName2Idx[tableDef.Name+"."+col.Name]
			if !ok {
				continue
			}
			oldColExpr := &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanTag,
						ColPos: int32(i),
					},
				},
			}
			newColExpr := &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectTag,
						ColPos: newColPos,
					},
				},
			}
			eqExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "<=>", []*plan.Expr{oldColExpr, newColExpr})
			if allColsEqual == nil {
				allColsEqual = eqExpr
			} else {
				allColsEqual, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*plan.Expr{allColsEqual, eqExpr})
			}
		}
		if allColsEqual != nil || allExplicitAssignmentsSkipped {
			// The dedup-join output also carries non-conflicting rows, whose old
			// image is all-NULL. Such a row must always be inserted. Conversely, if
			// every explicit assignment was removed as a semantic no-op, an existing
			// row must be dropped without evaluating implicit ON UPDATE expressions.
			// The old rowid distinguishes those two cases without comparing incoming
			// values that are not physically updated.
			rowIDIdx := tableDef.Name2ColIndex[catalog.Row_ID]
			oldRowIDExpr := &plan.Expr{
				Typ: tableDef.Cols[rowIDIdx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanTag,
						ColPos: rowIDIdx,
					},
				},
			}
			noOldRowExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "isnull", []*plan.Expr{oldRowIDExpr})
			keepExpr := noOldRowExpr
			if allColsEqual != nil {
				// NULL-safe equality is true for an all-NULL new-row image too, so
				// retain the rowid branch while keeping genuine updates whose compared
				// columns differ.
				notEqualExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "not", []*plan.Expr{allColsEqual})
				keepExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*plan.Expr{noOldRowExpr, notEqualExpr})
			}
			lastNodeID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{lastNodeID},
				FilterList: []*plan.Expr{keepExpr},
			}, bindCtx)
		}
	}

	var affectedIrregularIndexes, insertOnlyIrregularIndexes []*plan.IndexDef
	var irregularValueChangeFilters []irregularIndexValueChangeFilter
	if onDupAction == plan.Node_UPDATE && len(irregularIndexes) > 0 {
		affectedIrregularIndexes, insertOnlyIrregularIndexes, err =
			splitIrregularIndexesByUpdatedColumns(tableDef, irregularIndexes, possiblyChangedCols)
		if err != nil {
			return 0, err
		}
		irregularValueChangeFilters, err = buildIrregularIndexValueChangeFilters(
			tableDef, affectedIrregularIndexes)
		if err != nil {
			return 0, err
		}
	}

	newProjLen := len(selectNode.ProjectList) + len(appendedUniqueProjs) + len(irregularValueChangeFilters)
	for _, idxDef := range tableDef.Indexes {
		if !idxDef.Unique {
			newProjLen++
		}
	}

	if onDupAction == plan.Node_UPDATE {
		newProjLen++
	}

	delColName2Idx := make(map[string][2]int32)
	valueChangeMarkerPosByGroup := make(map[string]int32, len(irregularValueChangeFilters))

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

		// Keep one boolean per value-aware logical index, not the old indexed
		// values themselves. It is computed while scanTag is still available and
		// later filters both delete and rebuild maintenance to new or changed rows.
		for _, valueFilter := range irregularValueChangeFilters {
			var allEqual *plan.Expr
			for _, columnName := range valueFilter.columns {
				oldColPos, ok := tableDef.Name2ColIndex[columnName]
				if !ok {
					return 0, moerr.NewInternalErrorf(builder.GetContext(),
						"ON DUPLICATE KEY UPDATE cannot locate irregular index column %s", columnName)
				}
				newColPos, ok := colName2Idx[tableDef.Name+"."+columnName]
				if !ok {
					return 0, moerr.NewInternalErrorf(builder.GetContext(),
						"ON DUPLICATE KEY UPDATE cannot locate final irregular index column %s", columnName)
				}
				oldCol := &plan.Expr{
					Typ: tableDef.Cols[oldColPos].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: scanTag,
						ColPos: oldColPos,
					}},
				}
				newCol := &plan.Expr{
					Typ: tableDef.Cols[oldColPos].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: selectTag,
						ColPos: newColPos,
					}},
				}
				equal, bindErr := BindFuncExprImplByPlanExpr(
					builder.GetContext(), "<=>", []*plan.Expr{oldCol, newCol})
				if bindErr != nil {
					return 0, bindErr
				}
				if allEqual == nil {
					allEqual = equal
				} else {
					allEqual, bindErr = BindFuncExprImplByPlanExpr(
						builder.GetContext(), "and", []*plan.Expr{allEqual, equal})
					if bindErr != nil {
						return 0, bindErr
					}
				}
			}
			if allEqual == nil {
				continue
			}
			valueChanged, bindErr := BindFuncExprImplByPlanExpr(
				builder.GetContext(), "not", []*plan.Expr{allEqual})
			if bindErr != nil {
				return 0, bindErr
			}
			valueChangeMarkerPosByGroup[valueFilter.groupKey] = int32(len(newProjList))
			newProjList = append(newProjList, valueChanged)
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
		if onDupAction == plan.Node_UPDATE && odkuNeedFkCheck {
			var oks []*plan.Expr
			lastNodeID, oks, err = builder.appendModernChildFkMarkOks(
				bindCtx, odkuAllForeignKeyDef, lastNodeID, finalProjTag,
				func(colName string) int32 { return colName2Idx[tableDef.Name+"."+colName] }, true)
			if err != nil {
				return 0, err
			}
			if len(oks) != len(odkuFkEligibilityInputPos) {
				return 0, moerr.NewInternalError(builder.GetContext(),
					"ON DUPLICATE KEY UPDATE foreign-key eligibility count mismatch")
			}
			assertConds := make([]*plan.Expr, len(oks))
			fkErrExpr := makePlan2StringConstExprWithType(
				"Cannot add or update a child row: a foreign key constraint fails")
			for i, ok := range oks {
				eligible := &plan.Expr{
					Typ: newProjList[odkuFkEligibilityInputPos[i]].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: finalProjTag, ColPos: odkuFkEligibilityInputPos[i],
					}},
				}
				ok, err = guardConstraintByEligibility(builder.GetContext(), ok, eligible)
				if err != nil {
					return 0, err
				}
				assertConds[i], err = BindFuncExprImplByPlanExpr(
					builder.GetContext(), "assert", []*plan.Expr{ok, DeepCopyExpr(fkErrExpr)})
				if err != nil {
					return 0, err
				}
			}
			lastNodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_FILTER, Children: []int32{lastNodeID}, FilterList: assertConds,
				FilterIsBarrier: true,
			}, bindCtx)
			selectNode = builder.qry.Nodes[lastNodeID]
		}
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
		// ON DUPLICATE KEY UPDATE enforces child->parent FKs in the data flow with the
		// same per-FK MARK-join check as INSERT, over this final merged image. Each FK is
		// asserted independently (its parent exists OR one of that FK's own columns is
		// NULL), correct MATCH SIMPLE -- unlike the old appendForeignConstrantPlan, which
		// first applied one global isnotnull pre-filter across ALL FK columns and so
		// skipped every FK's validation as soon as ANY FK column on the row was NULL.
		// Unlike plain INSERT this does NOT re-project to a fresh tag: ODKU's downstream
		// MULTI_UPDATE reads both the merged image (finalProjTag) and the delete columns
		// (delColName2Idx) from this subtree, which a re-project would hide.
		if onDupAction == plan.Node_UPDATE && len(irregularIndexes) > 0 {
			// ODKU cannot change the PK, so the stale entries are keyed by the same
			// PK the final image carries at its natural position.
			odkuPkPos, odkuPkTyp := getPkPos(tableDef, false)
			oldRowIDRef, ok := delColName2Idx[tableDef.Name+"."+catalog.Row_ID]
			if !ok {
				return 0, moerr.NewInternalError(builder.GetContext(),
					"ON DUPLICATE KEY UPDATE cannot locate the old row id for irregular index maintenance")
			}
			lastNodeID, err = builder.appendOnDupIrregularMaintSource(
				bindCtx, lastNodeID, finalProjTag, int32(odkuPkPos), odkuPkTyp,
				-1, -1, physicalChangedRowsInputPos,
				affectedIrregularIndexes, insertOnlyIrregularIndexes, oldRowIDRef[1],
				valueChangeMarkerPosByGroup,
				tableDef, dmlCtx.objRefs[0])
			if err != nil {
				return 0, err
			}
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
		updateCtx.AffectedRowsWeightCol = &plan.ColRef{
			RelPos: selectTag, ColPos: affectedRowsInputPos,
		}
		updateCtx.PhysicalChangedRowsCol = &plan.ColRef{
			RelPos: selectTag, ColPos: physicalChangedRowsInputPos,
		}

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
			updateCtx.PhysicalChangedRowsCol = &plan.ColRef{
				RelPos: selectTag, ColPos: physicalChangedRowsInputPos,
			}
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

	if onDupAction != plan.Node_IGNORE && onDupAction != plan.Node_UPDATE {
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
	}

	dmlNode.Children = append(dmlNode.Children, lastNodeID)
	lastNodeID = builder.appendNode(dmlNode, bindCtx)

	return lastNodeID, nil
}

// appendInsertLockKeyProjection creates an executable row image for computed
// unique-index lock keys. The new PROJECT reads from the preceding PROJECT's
// binding tag, so lock-key expressions never self-reference their own output.
func (builder *QueryBuilder) appendInsertLockKeyProjection(
	bindCtx *BindContext,
	lastNodeID int32,
) (int32, int32, *plan.Node) {
	selectTag := builder.genNewBindTag()
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		ProjectList: getProjectionByLastNodeWithTag(builder, lastNodeID, selectTag),
		BindingTags: []int32{selectTag},
	}, bindCtx)
	return lastNodeID, selectTag, builder.qry.Nodes[lastNodeID]
}

// materializeInsertUniqueLockKeys appends the computed lock key for each
// composite or prefix unique index to the insert row image. It must run while
// selectNode is an executable PROJECT, because LockOp consumes the resulting
// physical vectors rather than expressions in a pass-through FILTER.
func (builder *QueryBuilder) materializeInsertUniqueLockKeys(
	tableDef *TableDef,
	skipUniqueIdx []bool,
	colName2Idx map[string]int32,
	selectNode *plan.Node,
) error {
	for i, idxDef := range tableDef.Indexes {
		prefixLengths, materialize, err := insertUniqueLockKeyPrefixLengths(idxDef, skipUniqueIdx[i])
		if err != nil {
			return err
		}
		if !materialize {
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
				return moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", partName)
			}
			lockExpr, err = builder.makeIndexPartExprFromInputExpr(selectNode.ProjectList[partPos], partName, prefixLengths)
			if err != nil {
				return err
			}
		} else {
			args := make([]*plan.Expr, len(idxDef.Parts))
			for k := range idxDef.Parts {
				partName := catalog.ResolveAlias(idxDef.Parts[k])
				partPos, ok := colName2Idx[tableDef.Name+"."+partName]
				if !ok {
					return moerr.NewInternalErrorf(builder.GetContext(), "bind insert err, can not find colName = %s", partName)
				}
				args[k], err = builder.makeIndexPartExprFromInputExpr(selectNode.ProjectList[partPos], partName, prefixLengths)
				if err != nil {
					return err
				}
			}
			lockExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", args)
		}

		colName2Idx[lockColName] = int32(len(selectNode.ProjectList))
		selectNode.ProjectList = append(selectNode.ProjectList, lockExpr)
	}
	return nil
}

func hasMaterializedInsertUniqueLockKey(tableDef *TableDef, skipUniqueIdx []bool) (bool, error) {
	for i, idxDef := range tableDef.Indexes {
		_, materialize, err := insertUniqueLockKeyPrefixLengths(idxDef, skipUniqueIdx[i])
		if err != nil {
			return false, err
		}
		if materialize {
			return true, nil
		}
	}
	return false, nil
}

func insertUniqueLockKeyPrefixLengths(idxDef *IndexDef, skip bool) (map[string]int, bool, error) {
	prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
	if err != nil {
		return nil, false, err
	}
	return prefixLengths, idxDef.Unique && !skip &&
		(len(idxDef.Parts) > 1 || len(prefixLengths) > 0), nil
}

// isOnDupIncomingColumn reports whether an ON DUPLICATE KEY UPDATE expression
// is the unmodified incoming value of the target column (VALUES(col)). The
// expression is checked after binding so normal identifier validation has
// already taken place.
func isOnDupIncomingColumn(expr *plan.Expr, selectTag, colPos int32) bool {
	col := expr.GetCol()
	return col != nil && col.RelPos == selectTag && col.ColPos == colPos
}

// getInsertColsFromStmt retrieves the list of column names to be inserted into a table
// based on the given INSERT statement and table definition.
// If the INSERT statement does not specify the columns, all columns except the fake primary key column
// will be included in the list.
// If the INSERT statement specifies the columns, it validates the column names against the table definition
// and returns an error if any of the column names are invalid.
// The function returns the list of insert columns and an error, if any.
func (builder *QueryBuilder) getInsertColsFromStmt(astCols tree.IdentifierList, tableDef *TableDef) ([]string, error) {
	if err := builder.rejectDuplicateInsertColumns(astCols); err != nil {
		return nil, err
	}

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

func (builder *QueryBuilder) rejectDuplicateInsertColumns(astCols tree.IdentifierList) error {
	seen := make(map[string]struct{}, len(astCols))
	for _, column := range astCols {
		columnName := string(column)
		key := strings.ToLower(columnName)
		if _, ok := seen[key]; ok {
			return moerr.NewFieldSpecifiedTwice(builder.GetContext(), columnName)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// stripGeneratedDefaultCols removes generated columns from the INSERT column list
// when their corresponding VALUES are all DEFAULT. This supports MySQL-compatible
// syntax: INSERT INTO t(gen_col) VALUES(DEFAULT).
// Non-DEFAULT values for generated columns still produce an error.
func (builder *QueryBuilder) stripGeneratedDefaultCols(astCols tree.IdentifierList, astRows *tree.Select, tableDef *plan.TableDef) (tree.IdentifierList, error) {
	// Validate the original list before generated columns can be removed. This
	// preserves the SQL-level duplicate-column contract independently of the
	// generated-column DEFAULT rewrite below.
	if err := builder.rejectDuplicateInsertColumns(astCols); err != nil {
		return nil, err
	}

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

func (builder *QueryBuilder) initInsertReplaceStmt(bindCtx *BindContext, astRows *tree.Select, astCols tree.IdentifierList, objRef *plan.ObjectRef, tableDef *plan.TableDef, isReplace bool, colRefAsDefault bool) (int32, map[string]int32, []bool, error) {
	var (
		lastNodeID int32
		err        error
	)
	builder.insertInputKeysUnique = false

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
		lastNodeID, err = builder.buildValueScan(isAllDefault, colRefAsDefault, bindCtx, tableDef, selectImpl, insertColumns)
		if err != nil {
			return 0, nil, nil, err
		}

	case *tree.SelectClause, *tree.UnionClause:
		astSelect = astRows

		subCtx := NewBindContext(builder, bindCtx)
		subCtx.numericProjectionTypes = insertProjectionTypes(insertColumns, tableDef)
		lastNodeID, err = builder.bindSelect(astSelect, subCtx, false)
		if err != nil {
			return 0, nil, nil, err
		}
		if !isReplace && builder.localProtocolEnablesRightDedupInputKeysUnique() {
			builder.insertInputKeysUnique = builder.proveInsertInputKeysUnique(lastNodeID, insertColumns, tableDef)
		}
		//ifInsertFromUniqueColMap = make(map[string]bool)

	case *tree.ParenSelect:
		astSelect = selectImpl.Select

		subCtx := NewBindContext(builder, bindCtx)
		subCtx.numericProjectionTypes = insertProjectionTypes(insertColumns, tableDef)
		lastNodeID, err = builder.bindSelect(astSelect, subCtx, false)
		if err != nil {
			return 0, nil, nil, err
		}
		if !isReplace && builder.localProtocolEnablesRightDedupInputKeysUnique() {
			builder.insertInputKeysUnique = builder.proveInsertInputKeysUnique(lastNodeID, insertColumns, tableDef)
		}
		// ifInsertFromUniqueColMap = make(map[string]bool)

	default:
		return 0, nil, nil, moerr.NewInvalidInput(builder.GetContext(), "insert has unknown select statement")
	}

	if err = builder.addBinding(lastNodeID, tree.AliasClause{Alias: derivedTableName}, bindCtx); err != nil {
		return 0, nil, nil, err
	}

	return builder.appendInsertReplaceSourceCasts(bindCtx, lastNodeID, insertColumns, objRef, tableDef, isReplace)
}

// castInsertSourceColumn casts one bound source column to its target column
// type with INSERT assignment semantics (ENUM/SET/GEOMETRY aware). sourceExpr is
// the projection the column came from, used to recognize display-value
// projections of MySQL special types.
func (builder *QueryBuilder) castInsertSourceColumn(projExpr, sourceExpr *plan.Expr, colDef *plan.ColDef) (*plan.Expr, error) {
	typ := colDef.Typ
	switch {
	case isEnumPlanType(&typ):
		return funcCastForEnumType(builder.GetContext(), projExpr, typ)
	case isSetPlanType(&typ):
		return funcCastForSetType(builder.GetContext(), projExpr, typ)
	case isGeometryPlanType(&typ):
		return funcCastForGeometryType(builder.GetContext(), projExpr, typ)
	default:
		return builder.forceProjectedAssignmentCastExpr(projExpr, sourceExpr, typ, builder.isInsertIgnore)
	}
}

// appendInsertReplaceSourceCasts takes the bound source of an INSERT/REPLACE
// (a node whose projection yields one column per entry of insertColumns, in
// order), casts every column to its target column type and appends the
// pre-insert nodes (defaults, auto-increment, composite keys, ...) plus the
// per-table dedup/write nodes. It is shared by single-table INSERT/REPLACE and
// by every target of a multi-table INSERT.
func (builder *QueryBuilder) appendInsertReplaceSourceCasts(bindCtx *BindContext, lastNodeID int32, insertColumns []string, objRef *plan.ObjectRef, tableDef *plan.TableDef, isReplace bool) (int32, map[string]int32, []bool, error) {
	var err error
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
		projExpr, err = builder.castInsertSourceColumn(projExpr, lastNode.ProjectList[i], tableDef.Cols[colIdx])
		if err != nil {
			return 0, nil, nil, err
		}
		insertColToExpr[column] = projExpr
	}

	if isReplace {
		return builder.appendNodesForReplaceStmt(bindCtx, lastNodeID, tableDef, objRef, insertColToExpr)
	} else {
		return builder.appendNodesForInsertStmt(bindCtx, lastNodeID, tableDef, objRef, insertColToExpr)
	}
}

// localProtocolEnablesRightDedupInputKeysUnique reads the deployment-managed
// rollout gate. It stays false until every participating CN can honor the
// lookup-only operator bit, because the planner's memory proof depends on it.
func (builder *QueryBuilder) localProtocolEnablesRightDedupInputKeysUnique() bool {
	if builder == nil || builder.compCtx == nil {
		return false
	}
	proc := builder.compCtx.GetProcess()
	if proc == nil {
		return false
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return false
	}
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	return ok && valid && version >= defines.MORPCVersion21
}

// insertSourceColumn identifies an output column that is still a direct
// column of one source table.  Keeping this lineage separate from expression
// rewriting makes the uniqueness proof fail closed for casts, functions, and
// row-multiplying operators. table and colPos identify the origin;
// outputTag advances at each PROJECT so stacked projections remain traceable.
type insertSourceColumn struct {
	table     *plan.TableDef
	colPos    int32
	outputTag int32
}

// proveInsertInputKeysUnique proves that every incoming target-PK key is
// unique by tracing a row-preserving INSERT ... SELECT plan back to a source
// table's explicit primary key.  The proof is deliberately conservative: only
// direct column projections over a single table scan are accepted.
func (builder *QueryBuilder) proveInsertInputKeysUnique(
	sourceNodeID int32,
	insertColumns []string,
	targetTable *plan.TableDef,
) bool {
	targetPK, ok := insertPrimaryKeyColumnPositions(targetTable)
	if !ok || len(insertColumns) == 0 {
		return false
	}

	lineage, sourceTable, ok := builder.insertSourceLineage(sourceNodeID)
	if !ok || sourceTable == nil || len(lineage) != len(insertColumns) {
		return false
	}
	sourcePK, ok := insertPrimaryKeyColumnPositions(sourceTable)
	if !ok {
		return false
	}

	// The source PK must be present in the target key.  A target composite key
	// such as (id, col1) is therefore safe when the source PK is (id), while a
	// target key (id) is not safe for a source whose PK is (id, col1).
	targetKeySourceCols := make(map[int32]struct{}, len(targetPK))
	for _, targetPos := range targetPK {
		if targetPos < 0 || int(targetPos) >= len(targetTable.Cols) || targetTable.Cols[targetPos] == nil {
			return false
		}
		name := targetTable.Cols[targetPos].Name
		inputPos := -1
		for i, insertColumn := range insertColumns {
			if strings.EqualFold(catalog.ResolveAlias(insertColumn), name) {
				if inputPos >= 0 {
					return false
				}
				inputPos = i
			}
		}
		if inputPos < 0 || inputPos >= len(lineage) {
			return false
		}
		if lineage[inputPos].table != sourceTable {
			return false
		}
		sourcePos := lineage[inputPos].colPos
		if sourcePos < 0 || int(sourcePos) >= len(sourceTable.Cols) || sourceTable.Cols[sourcePos] == nil ||
			!insertKeyTypesCompatible(sourceTable.Cols[sourcePos].Typ, targetTable.Cols[targetPos].Typ) {
			// The assignment projection may cast the source key to the target
			// type after this proof runs. Only identical key encodings are
			// accepted here; narrowing casts could collapse distinct source PKs.
			return false
		}
		targetKeySourceCols[sourcePos] = struct{}{}
	}
	for _, sourcePos := range sourcePK {
		if _, ok := targetKeySourceCols[sourcePos]; !ok {
			return false
		}
	}
	return true
}

// insertSourceLineage accepts only row-preserving FILTER and SORT operators;
// PROJECT is accepted only when every output is a direct child column
// reference.
func (builder *QueryBuilder) insertSourceLineage(nodeID int32) ([]insertSourceColumn, *plan.TableDef, bool) {
	if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return nil, nil, false
	}
	node := builder.qry.Nodes[nodeID]
	if node == nil {
		return nil, nil, false
	}

	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		if len(node.Children) != 0 || node.TableDef == nil || len(node.TableDef.Cols) == 0 ||
			len(node.BindingTags) != 1 {
			return nil, nil, false
		}
		sourceTag := node.BindingTags[0]
		if len(node.ProjectList) == 0 {
			lineage := make([]insertSourceColumn, len(node.TableDef.Cols))
			for i := range node.TableDef.Cols {
				lineage[i] = insertSourceColumn{table: node.TableDef, colPos: int32(i), outputTag: sourceTag}
			}
			return lineage, node.TableDef, true
		}
		lineage := make([]insertSourceColumn, len(node.ProjectList))
		for i, expr := range node.ProjectList {
			col := expr.GetCol()
			if col == nil || col.ColPos < 0 || int(col.ColPos) >= len(node.TableDef.Cols) {
				return nil, nil, false
			}
			if col.RelPos != sourceTag {
				return nil, nil, false
			}
			lineage[i] = insertSourceColumn{table: node.TableDef, colPos: col.ColPos, outputTag: sourceTag}
		}
		return lineage, node.TableDef, true

	case plan.Node_FILTER, plan.Node_SORT:
		if len(node.Children) != 1 || len(node.ProjectList) != 0 {
			return nil, nil, false
		}
		return builder.insertSourceLineage(node.Children[0])

	case plan.Node_PROJECT:
		if len(node.Children) != 1 || len(node.ProjectList) == 0 || len(node.BindingTags) != 1 {
			return nil, nil, false
		}
		childID := node.Children[0]
		childLineage, sourceTable, ok := builder.insertSourceLineage(childID)
		if !ok {
			return nil, nil, false
		}
		lineage := make([]insertSourceColumn, len(node.ProjectList))
		for i, expr := range node.ProjectList {
			col := expr.GetCol()
			if col == nil || col.ColPos < 0 || int(col.ColPos) >= len(childLineage) {
				return nil, nil, false
			}
			if col.RelPos != childLineage[col.ColPos].outputTag {
				return nil, nil, false
			}
			lineage[i] = childLineage[col.ColPos]
			lineage[i].outputTag = node.BindingTags[0]
		}
		return lineage, sourceTable, true
	}

	return nil, nil, false
}

func insertPrimaryKeyColumnPositions(table *plan.TableDef) ([]int32, bool) {
	if table == nil || table.Pkey == nil || table.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return nil, false
	}
	names := table.Pkey.Names
	if len(names) == 0 && table.Pkey.PkeyColName != "" && table.Pkey.PkeyColName != catalog.CPrimaryKeyColName {
		names = []string{table.Pkey.PkeyColName}
	}
	if len(names) == 0 {
		return nil, false
	}
	positions := make([]int32, 0, len(names))
	for _, name := range names {
		name = catalog.ResolveAlias(name)
		pos, ok := table.Name2ColIndex[name]
		if !ok {
			for i, col := range table.Cols {
				if col != nil && strings.EqualFold(col.Name, name) {
					pos = int32(i)
					ok = true
					break
				}
			}
		}
		if !ok || pos < 0 || int(pos) >= len(table.Cols) {
			return nil, false
		}
		positions = append(positions, pos)
	}
	return positions, true
}

func insertKeyTypesCompatible(source, target plan.Type) bool {
	// Table records column provenance, not physical representation. Keys from
	// different tables remain compatible when their encoded type fields match.
	return isSameColumnType(source, target)
}

// isNumericAssignmentTarget reports whether a DML target column type may seed the
// numeric assignment context. A non-numeric target keeps the default binding path,
// because InferNumericParameterType falls back to float64 for a non-numeric outer
// type and would otherwise mistype a bare prepared parameter.
func isNumericAssignmentTarget(typ Type) bool {
	return makeTypeByPlan2Type(typ).IsNumeric()
}

func insertProjectionTypes(insertColumns []string, tableDef *plan.TableDef) []Type {
	// only numeric targets may seed the numeric assignment context; a zero
	// Type keeps the projection binder on the default binding path
	targets := make([]Type, len(insertColumns))
	for i, column := range insertColumns {
		typ := tableDef.Cols[tableDef.Name2ColIndex[column]].Typ
		if isNumericAssignmentTarget(typ) {
			targets[i] = typ
		}
	}
	return targets
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
		genExpr := builder.applyGeneratedColumnAssignmentCast(
			DeepCopyExpr(col.GeneratedCol.Expr),
			builder.isInsertIgnore,
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
// (through parentheses or a cast), a function call. For non-numeric destinations
// such expressions bind without the destination type so arguments keep their
// own domains — e.g. st_point(116.3975, 39.9087), (st_point(...)) or a cast of
// st_point into a geometry column. Numeric destinations instead use assignment-
// aware overload resolution. A bare literal (or a literal wrapped in a unary
// minus / cast) is not a function call.
func valuesExprIsFuncCall(e tree.Expr) bool {
	for {
		switch v := e.(type) {
		case *tree.ParenExpr:
			e = v.Expr
		case *tree.CastExpr:
			e = v.Expr
		case *tree.FuncExpr:
			// mod() is arithmetic and shares the numeric-context binder with the
			// binary operators (see numericAstFunctionName); every other function
			// binds its arguments by their own types.
			return numericAstFunctionName(v) != "mod"
		default:
			return false
		}
	}
}

func (builder *QueryBuilder) buildValueScan(
	isAllDefault bool,
	colRefAsDefault bool,
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
			defExpr, err = builder.forceCastExpr2(defExpr, colTyp, targetTyp, builder.isInsertIgnore)
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
			var binder, funcBinder Binder
			if colRefAsDefault {
				// REPLACE ... SET col = expr: an RHS reference to a target-table
				// column is evaluated as DEFAULT(col), including inside functions.
				replaceBinder := NewReplaceValueBinder(builder.GetContext(), builder, nil, col.Typ, tableDef)
				binder = replaceBinder

				funcReplaceBinder := NewReplaceValueBinder(builder.GetContext(), builder, nil, plan.Type{}, tableDef)
				funcBinder = funcReplaceBinder
			} else {
				defaultBinder := NewDefaultBinder(builder.GetContext(), nil, nil, col.Typ, nil)
				defaultBinder.builder = builder
				binder = defaultBinder

				// Non-numeric destinations need a target-free function binder. The
				// DefaultBinder would otherwise push GEOMETRY into st_point's float
				// arguments and break overload resolution. Numeric destinations use
				// the target-aware binder below, which resolves each function argument
				// from overload metadata before the final assignment cast.
				defaultFuncBinder := NewDefaultBinder(builder.GetContext(), nil, nil, plan.Type{}, nil)
				defaultFuncBinder.builder = builder
				funcBinder = defaultFuncBinder
			}
			for _, r := range stmt.Rows {
				if nv, ok := r[i].(*tree.NumVal); ok && builder.isInsertIgnore {
					expr, handled, err := makeInsertIgnoreMySQLSpecialTypeConstExpr(builder.GetContext(), nv, col.Typ)
					if err != nil {
						return 0, err
					}
					if handled {
						rowsetData.Cols[i].Data = append(rowsetData.Cols[i].Data, &plan.RowsetExpr{Expr: expr})
						continue
					}
				}
				if nv, ok := r[i].(*tree.NumVal); ok && !isEnumOrSetPlanType(&col.Typ) && !isTypedArrayPlanType(&col.Typ) {
					expr, err := MakeInsertValueConstExpr(proc, nv, &colTyp, builder.isInsertIgnore)
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
					boundWithNumericContext := false
					if isNumericAssignmentTarget(col.Typ) {
						if builder.isPrepareStatement {
							// Analyze prepared functions with the target-free binder. The
							// explicit numeric context below supplies the assignment domain only
							// to compatible numeric arguments.
							var scan numericAstTypeScan
							switch numericBinder := funcBinder.(type) {
							case *DefaultBinder:
								scan, err = numericBinder.numericAstTypesInternal(r[i], 0, numericBinder.numericAstColumnResolver())
							case *ReplaceValueBinder:
								scan, err = numericBinder.numericAstTypesInternal(r[i], 0, numericBinder.numericAstColumnResolver())
							}
							if err != nil {
								return 0, err
							}
							if scan.hasParam {
								switch numericBinder := funcBinder.(type) {
								case *DefaultBinder:
									defExpr, err = numericBinder.bindNumericExprWithContext(r[i], 0, &col.Typ)
								case *ReplaceValueBinder:
									defExpr, err = numericBinder.bindNumericExprWithContext(r[i], 0, &col.Typ)
								}
								if err != nil {
									return 0, err
								}
								boundWithNumericContext = defExpr != nil
							}
						}
						// A function without dynamic parameters must retain its normal
						// argument domain (for example LENGTH(100000.5)).
						if !boundWithNumericContext && valuesExprIsFuncCall(r[i]) {
							valueBinder = funcBinder
						}
					} else if valuesExprIsFuncCall(r[i]) {
						// Geometry and other non-numeric functions need their
						// arguments to bind independently of the destination type.
						valueBinder = funcBinder
					}
					if !boundWithNumericContext {
						defExpr, err = valueBinder.BindExpr(r[i], 0, true)
						if err != nil {
							return 0, err
						}
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
				defExpr, err = builder.forceCastExpr2(defExpr, colTyp, targetTyp, builder.isInsertIgnore)
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
