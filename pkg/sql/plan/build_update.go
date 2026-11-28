// Copyright 2021 - 2022 Matrix Origin
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
	"slices"
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

// isSyncMaintainedIrregularIndex reports whether idxDef is an irregular (full-text / vector)
// index that is maintained SYNCHRONOUSLY — inline in the DML rather than asynchronously via CDC.
// Such an index keys its hidden table(s) by the source primary key, so an in-place primary-key
// change would strand those entries and go stale (#25617). Routed through the plugin registry so
// no per-algo list is hardcoded here: an always-async algorithm (HNSW/CAGRA/IVFPQ) is never
// synchronous; otherwise async-ness comes from the index's `async` param (IVFFLAT/FULLTEXT are
// synchronous by default).
func isSyncMaintainedIrregularIndex(idxDef *plan.IndexDef) bool {
	if catalog.IsRegularIndexAlgo(idxDef.IndexAlgo) {
		return false
	}
	p, ok := indexplugin.Get(catalog.ToLower(idxDef.IndexAlgo))
	if !ok {
		return false
	}
	if p.Catalog().SyncDescriptor().AlwaysAsync {
		return false
	}
	async, _ := catalog.IndexParamAsync(idxDef.IndexAlgoParams)
	return !async
}

func buildTableUpdate(stmt *tree.Update, ctx CompilerContext, isPrepareStmt bool) (p *Plan, err error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildUpdateHistogram.Observe(time.Since(start).Seconds())
	}()
	tblInfo, err := getUpdateTableInfo(ctx, stmt)
	if err != nil {
		return nil, err
	}
	targetAliases := make([]string, len(tblInfo.tableDefs))
	for alias, idx := range tblInfo.alias {
		targetAliases[idx] = alias
	}
	if err = validateUpdateTargetSubqueries(ctx, stmt, tblInfo.objRef, tblInfo.tableDefs, targetAliases); err != nil {
		return nil, err
	}

	// A synchronously-maintained FULLTEXT/IVF index keys its hidden table(s) by the source
	// primary key but is updated inline; an UPDATE that changes a primary key column would
	// leave those entries stranded at the old key, so the index goes stale / exposes dead rows
	// (#25617). HNSW and async IVF reconcile through CDC and are unaffected. Reject the PK
	// update here in the table-update path (the only path that maintains irregular indexes;
	// bindUpdate bails to it) rather than silently corrupting the index. Treat an index whose
	// async-ness cannot be determined as synchronous (fail safe).
	for i, tableDef := range tblInfo.tableDefs {
		if tableDef.Pkey == nil || len(tblInfo.updateKeys[i]) == 0 {
			continue
		}
		hasSyncIrregularIndex := false
		for _, idxDef := range tableDef.Indexes {
			if isSyncMaintainedIrregularIndex(idxDef) {
				hasSyncIrregularIndex = true
				break
			}
		}
		if !hasSyncIrregularIndex {
			continue
		}
		for _, pkColName := range tableDef.Pkey.Names {
			if _, ok := tblInfo.updateKeys[i][pkColName]; ok {
				return nil, moerr.NewUnsupportedDML(ctx.GetContext(), "update primary key on a table with a synchronous full-text/vector index")
			}
		}
	}

	// new logic
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, isPrepareStmt, false)
	builder.roleRuleRowLocalWrite = stmt.RewriteOption != nil && len(stmt.RewriteOption.RoleRules) != 0
	queryBindCtx := NewBindContext(builder, nil)
	if hasRepeatedUpdateTarget(tblInfo) {
		return buildRepeatedUpdatePlan(stmt, ctx, builder, queryBindCtx, tblInfo)
	}
	lastNodeId, updatePlanCtxs, err := selectUpdateTables(builder, queryBindCtx, stmt, tblInfo, nil)
	if err != nil {
		return nil, err
	}
	err = rewriteUpdateQueryLastNode(builder, updatePlanCtxs, lastNodeId, stmt.Ignore)
	if err != nil {
		return nil, err
	}
	rewriteGeneratedColumnsForUpdate(builder, updatePlanCtxs, lastNodeId, stmt.Ignore)
	if stmt.From != nil && len(stmt.From.Tables) > 0 && tblInfo.needAggFilter {
		lastNode := builder.qry.Nodes[lastNodeId]
		lastNodeId, _, _, err = builder.appendRowNumberDedupNode(
			queryBindCtx,
			lastNodeId,
			lastNode,
			lastNode.BindingTags[0],
			fallbackUpdateFromDedupPartitionCols(updatePlanCtxs),
		)
		if err != nil {
			return nil, err
		}
		// Duplicate source matches are now deduped by the row_number() window
		// above, so the per-table plan must skip the any_value aggregation. Keep
		// needAggFilter set, though: it also drives the join-target NULL-row
		// filter (row_id IS NOT NULL) that must survive for joined-target
		// UPDATE ... FROM.
		for _, updatePlanCtx := range updatePlanCtxs {
			updatePlanCtx.dedupByRowNumber = true
		}
	}

	sourceStep := builder.appendStep(lastNodeId)
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}

	// if len(updatePlanCtxs) == 1 && updatePlanCtxs[0].updatePkCol {
	// 	pkFilterExpr := getPkFilterExpr(builder, lastNodeId, updatePlanCtxs[0], tblInfo.tableDefs[0])
	// 	updatePlanCtxs[0].pkFilterExprs = pkFilterExpr
	// }

	builder.qry.Steps = append(builder.qry.Steps[:sourceStep], builder.qry.Steps[sourceStep+1:]...)

	// append sink node
	lastNodeId = appendSinkNode(builder, queryBindCtx, lastNodeId)
	sourceStep = builder.appendStep(lastNodeId)

	beginIdx := 0
	checkMode := assertUpdateCheck
	if stmt.Ignore {
		checkMode = filterUpdateCheck
	}
	var detectSqls []string
	for i, tableDef := range tblInfo.tableDefs {
		upPlanCtx := updatePlanCtxs[i]
		upPlanCtx.beginIdx = beginIdx
		upPlanCtx.sourceStep = sourceStep
		upPlanCtx.checkMode = checkMode

		updateBindCtx := NewBindContext(builder, nil)
		beginIdx = beginIdx + upPlanCtx.updateColLength + len(tableDef.Cols)
		allAsyncSources := false
		if len(tblInfo.tableDefs) > 1 && len(tableDef.Indexes) > 0 {
			for j, other := range tblInfo.tableDefs {
				if i != j && other.TblId == tableDef.TblId && len(tblInfo.updateKeys[j]) > 0 {
					allAsyncSources = true
					break
				}
			}
		}
		err = buildUpdatePlans(ctx, builder, updateBindCtx, upPlanCtx, false, allAsyncSources)
		if err != nil {
			return nil, err
		}
		putDmlPlanCtx(upPlanCtx)
		sqls, err := genSqlsForCheckFKSelfRefer(ctx.GetContext(),
			tblInfo.objRef[i].SchemaName, tableDef.Name, tableDef.Cols, tableDef.Fkeys)
		if err != nil {
			return nil, err
		}
		detectSqls = append(detectSqls, sqls...)
	}
	if err != nil {
		return nil, err
	}
	query.DetectSqls = detectSqls
	reduceSinkSinkScanNodes(query)
	builder.tempOptimizeForDML()
	reCheckifNeedLockWholeTable(builder)
	query.StmtType = plan.Query_UPDATE
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

// hasRepeatedUpdateTarget is called after generated-column DEFAULT assignments
// and aliases without writable columns have been removed from the target list.
func hasRepeatedUpdateTarget(info *dmlTableInfo) bool {
	if len(info.tableDefs) < 2 {
		return false
	}
	seen := make(map[uint64]struct{}, len(info.tableDefs))
	for _, table := range info.tableDefs {
		if _, exists := seen[table.TblId]; exists {
			return true
		}
		seen[table.TblId] = struct{}{}
	}
	return false
}

func isDefaultValExpr(e *Expr) bool {
	if ce, ok := e.Expr.(*plan.Expr_Lit); ok {
		_, isDefVal := ce.Lit.Value.(*plan.Literal_Defaultval)
		return isDefVal
	}
	return false
}

func rewriteUpdateQueryLastNode(builder *QueryBuilder, planCtxs []*dmlPlanCtx, lastNodeId int32, isIgnore bool) error {
	var err error

	lastNode := builder.qry.Nodes[lastNodeId]

	idx := 0
	for _, planCtx := range planCtxs {
		tableDef := planCtx.tableDef

		for colIdx, col := range tableDef.Cols {
			if offset, ok := planCtx.updateColPosMap[col.Name]; ok {
				pos := idx + offset
				posExpr := lastNode.ProjectList[pos]
				if isDefaultValExpr(posExpr) { // set col = default
					lastNode.ProjectList[pos], err = getDefaultExpr(builder.GetContext(), col)
					if err != nil {
						return err
					}
					posExpr = lastNode.ProjectList[pos]
				}
				err = checkNotNull(builder.GetContext(), posExpr, tableDef, col)
				if err != nil {
					return err
				}
				if col != nil && isEnumPlanType(&col.Typ) {
					lastNode.ProjectList[pos], err = funcCastForEnumType(builder.GetContext(), posExpr, col.Typ)
					if err != nil {
						return err
					}
				} else if col != nil && isSetPlanType(&col.Typ) {
					lastNode.ProjectList[pos], err = funcCastForSetType(builder.GetContext(), posExpr, col.Typ)
					if err != nil {
						return err
					}
				} else if col != nil && isGeometryPlanType(&col.Typ) {
					lastNode.ProjectList[pos], err = funcCastForGeometryType(builder.GetContext(), posExpr, col.Typ)
					if err != nil {
						return err
					}
				} else {
					lastNode.ProjectList[pos], err = builder.forceProjectedAssignmentCastExpr(
						posExpr, posExpr, col.Typ, isIgnore)
					if err != nil {
						return err
					}
				}

			} else {
				pos := idx + colIdx
				if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
					newDefExpr := DeepCopyExpr(col.OnUpdate.Expr)
					err = replaceFuncId(builder.GetContext(), newDefExpr)
					if err != nil {
						return err
					}
					lastNode.ProjectList[pos] = newDefExpr
				}

				if col != nil && isEnumPlanType(&col.Typ) {
					lastNode.ProjectList[pos], err = funcCastForEnumType(builder.GetContext(), lastNode.ProjectList[pos], col.Typ)
					if err != nil {
						return err
					}
				} else if col != nil && isSetPlanType(&col.Typ) {
					lastNode.ProjectList[pos], err = funcCastForSetType(builder.GetContext(), lastNode.ProjectList[pos], col.Typ)
					if err != nil {
						return err
					}
				} else if col != nil && isGeometryPlanType(&col.Typ) {
					lastNode.ProjectList[pos], err = funcCastForGeometryType(builder.GetContext(), lastNode.ProjectList[pos], col.Typ)
					if err != nil {
						return err
					}
				} else {
					lastNode.ProjectList[pos], err = builder.forceProjectedAssignmentCastExpr(
						lastNode.ProjectList[pos], lastNode.ProjectList[pos], col.Typ, isIgnore)
					if err != nil {
						return err
					}
				}
			}
		}
		idx += planCtx.updateColLength + len(tableDef.Cols)
	}
	return nil
}

func rewriteGeneratedColumnsForUpdate(
	builder *QueryBuilder,
	planCtxs []*dmlPlanCtx,
	lastNodeId int32,
	isIgnore bool,
) {
	selectNode := builder.qry.Nodes[lastNodeId]
	tableBase := int32(0)
	for _, upPlanCtx := range planCtxs {
		tableDef := upPlanCtx.tableDef
		hasGenerated := false
		for _, col := range tableDef.Cols {
			if col.GeneratedCol != nil {
				hasGenerated = true
				break
			}
		}
		if hasGenerated {
			baseLookup := make([]*plan.Expr, len(tableDef.Cols))
			for ci, col := range tableDef.Cols {
				if newOff, ok := upPlanCtx.updateColPosMap[col.Name]; ok {
					baseLookup[ci] = selectNode.ProjectList[tableBase+int32(newOff)]
				} else {
					baseLookup[ci] = selectNode.ProjectList[tableBase+int32(ci)]
				}
			}
			for ci, col := range tableDef.Cols {
				if col.GeneratedCol == nil {
					continue
				}
				if _, alreadySet := upPlanCtx.updateColPosMap[col.Name]; alreadySet {
					// SET on a stored generated column was rejected earlier
					// (or dropped for SET = DEFAULT); should not happen here.
					continue
				}
				genExpr := builder.applyGeneratedColumnAssignmentCast(
					DeepCopyExpr(col.GeneratedCol.Expr),
					isIgnore,
				)
				genExpr = substituteColRefsInExpr(genExpr, baseLookup, 0)
				insertPos := int(tableBase) + len(tableDef.Cols) + upPlanCtx.updateColLength
				selectNode.ProjectList = append(selectNode.ProjectList, nil)
				copy(selectNode.ProjectList[insertPos+1:], selectNode.ProjectList[insertPos:])
				selectNode.ProjectList[insertPos] = genExpr
				newOffset := int32(insertPos) - tableBase
				upPlanCtx.updateColPosMap[col.Name] = int(newOffset)
				upPlanCtx.updateColLength++
				baseLookup[ci] = genExpr
			}
		}
		tableBase += int32(len(tableDef.Cols) + upPlanCtx.updateColLength)
	}
}

// affectedGeneratedColumns returns the dependency closure in table column order.
// A generated column can only reference earlier generated columns.
func affectedGeneratedColumns(table *TableDef, names []string) ([]int, error) {
	changed := make([]bool, len(table.Cols))
	for _, name := range names {
		col, ok := table.Name2ColIndex[name]
		if !ok || col < 0 || int(col) >= len(changed) {
			return nil, moerr.NewInternalErrorNoCtx("invalid updated column for generated dependencies")
		}
		changed[col] = true
	}
	var generated []int
	for i, col := range table.Cols {
		if col.GeneratedCol == nil || col.GeneratedCol.Expr == nil {
			continue
		}
		for _, dependency := range collectRefColPos(col.GeneratedCol.Expr) {
			if dependency < 0 || int(dependency) >= len(changed) {
				return nil, moerr.NewInternalErrorNoCtx("invalid generated UPDATE dependency")
			}
			if changed[dependency] {
				generated = append(generated, i)
				changed[i] = true
				break
			}
		}
	}
	return generated, nil
}

// appendGeneratedUpdateColumn materializes one value so subsequent generated
// columns and the write can read the same result.
func appendGeneratedUpdateColumn(
	builder *QueryBuilder, bindCtx *BindContext, last int32, inputTag int32, col *ColDef, rowPositions []int, isIgnore bool,
) (int32, int) {
	input := getProjectionByLastNode(builder, last)
	if inputTag != 0 {
		for _, expr := range input {
			expr.GetCol().RelPos = inputTag
		}
	}
	value := generatedUpdateExpr(builder, col, input, rowPositions, isIgnore)
	valuePos := len(input)
	node := &Node{
		NodeType: plan.Node_PROJECT, Children: []int32{last}, ProjectList: append(input, value),
	}
	if inputTag != 0 {
		node.BindingTags = []int32{builder.genNewBindTag()}
	}
	last = builder.appendNode(node, bindCtx)
	return last, valuePos
}

func generatedUpdateExpr(builder *QueryBuilder, col *ColDef, input []*Expr, rowPositions []int, isIgnore bool) *Expr {
	lookup := make([]*Expr, len(rowPositions))
	for i, pos := range rowPositions {
		lookup[i] = input[pos]
	}
	value := builder.applyGeneratedColumnAssignmentCast(DeepCopyExpr(col.GeneratedCol.Expr), isIgnore)
	return substituteColRefsInExpr(value, lookup, 0)
}

func appendForeignKeyGeneratedUpdate(
	builder *QueryBuilder, bindCtx *BindContext, last int32, rowTag int32, update *dmlPlanCtx,
) (int32, int32, error) {
	table := update.tableDef
	hasGenerated := false
	for _, col := range table.Cols {
		if col.GeneratedCol == nil {
			continue
		}
		hasGenerated = true
		if _, assigned := update.updateColPosMap[col.Name]; assigned {
			last, err := appendRejectedForeignKeyUpdate(builder, bindCtx, last, rowTag, update.rowIdPos,
				fmt.Sprintf("foreign key action assigning to generated column '%s'", col.Name))
			return last, rowTag, err
		}
	}
	if !hasGenerated {
		return last, rowTag, nil
	}

	names := make([]string, 0, len(update.updateColPosMap))
	for name := range update.updateColPosMap {
		names = append(names, name)
	}
	generated, err := affectedGeneratedColumns(table, names)
	if err != nil || len(generated) == 0 {
		return last, rowTag, err
	}
	rowPositions := make([]int, len(table.Cols))
	for i, col := range table.Cols {
		rowPositions[i] = i
		if pos, assigned := update.updateColPosMap[col.Name]; assigned {
			rowPositions[i] = pos
		}
	}
	for _, i := range generated {
		col := table.Cols[i]
		changesPK := table.Pkey != nil && slices.Contains(table.Pkey.Names, col.Name)
		if reason := unsupportedGeneratedKeyChange(update, col, changesPK); reason != "" {
			// After this assertion, later columns and the writer can use the
			// existing value. Keep the key out of the index update assignments.
			last, err = appendUnchangedGeneratedKeyCheck(builder, bindCtx, last, rowTag, col, i, rowPositions, reason)
			if err != nil {
				return -1, rowTag, err
			}
			continue
		}
		last, rowPositions[i] = appendGeneratedUpdateColumn(builder, bindCtx, last, rowTag, col, rowPositions, false)
		if rowTag != 0 {
			rowTag = builder.qry.Nodes[last].BindingTags[0]
		}
		update.updatePkCol = update.updatePkCol || changesPK
		update.updateColPosMap[col.Name] = rowPositions[i]
		update.updateColLength++
	}
	last, err = appendGeneratedOutgoingFKChecks(builder, bindCtx, last, rowTag, table, generated, rowPositions)
	return last, rowTag, err
}

// Recursive writes trust the triggering FK's assignments. A final outgoing
// key containing NULL needs no parent match. A changed, non-NULL generated
// key cannot rely on the triggering FK's check.
func appendGeneratedOutgoingFKChecks(
	builder *QueryBuilder, bindCtx *BindContext, last int32, rowTag int32,
	table *TableDef, generated, rowPositions []int,
) (int32, error) {
	var projection []*Expr
	var assertions []*Expr
	ctx := builder.GetContext()
	for _, fk := range table.Fkeys {
		var generatedName string
		for _, i := range generated {
			col := table.Cols[i]
			if rowPositions[i] != i && slices.Contains(fk.Cols, col.ColId) {
				generatedName = col.Name
				break
			}
		}
		if generatedName == "" {
			continue
		}
		if projection == nil {
			projection = getProjectionByLastNodeWithTag(builder, last, rowTag)
		}
		nullableParts := make([]*Expr, 0, len(fk.Cols)+1)
		unchangedParts := make([]*Expr, 0, len(fk.Cols))
		for i, col := range table.Cols {
			if !slices.Contains(fk.Cols, col.ColId) {
				continue
			}
			final := projection[rowPositions[i]]
			isNull, err := BindFuncExprImplByPlanExpr(ctx, "isnull", []*Expr{DeepCopyExpr(final)})
			if err != nil {
				return -1, err
			}
			nullableParts = append(nullableParts, isNull)
			if rowPositions[i] != i {
				// Like ordinary FK checks, use SQL equality, not the index encoding.
				equal, err := BindFuncExprImplByPlanExpr(ctx, "<=>", []*Expr{
					DeepCopyExpr(projection[i]), DeepCopyExpr(final),
				})
				if err != nil {
					return -1, err
				}
				unchangedParts = append(unchangedParts, equal)
			}
		}
		unchanged, err := combinePlanConjunction(ctx, unchangedParts)
		if err != nil {
			return -1, err
		}
		allowed, err := combinePlanDisjunction(ctx, append(nullableParts, unchanged))
		if err != nil {
			return -1, err
		}
		assert, err := bindForeignKeyUpdateAssertion(ctx, allowed,
			fmt.Sprintf("foreign key action changing generated column '%s' constrained by another foreign key", generatedName))
		if err != nil {
			return -1, err
		}
		assertions = append(assertions, assert)
	}
	if len(assertions) == 0 {
		return last, nil
	}
	if rowTag != 0 {
		projection = nil // column remapping builds the executable projection
	}
	return builder.appendDMLRowFilter(bindCtx, last, assertions, projection), nil
}

// bindGeneratedPhysicalKeyEqual checks whether the existing encoded key can
// stay in place when this write path cannot maintain the index.
func bindGeneratedPhysicalKeyEqual(ctx context.Context, oldValue, newValue *Expr, typ *Type) (*Expr, error) {
	keys := []*Expr{oldValue, newValue}
	for i, key := range keys {
		var err error
		if isGeometryPlanType(typ) {
			// Preserve the raw WKB, including GEOMETRY32.
			keys[i], err = forceCastExpr(ctx, key, Type{Id: int32(types.T_blob)})
		} else {
			// Reuse the index encoding to preserve exact float and vector bits.
			keys[i], err = BindFuncExprImplByPlanExpr(ctx, "serial_full", []*Expr{key})
		}
		if err != nil {
			return nil, err
		}
	}
	return BindFuncExprImplByPlanExpr(ctx, "<=>", keys)
}

func appendUnchangedGeneratedKeyCheck(
	builder *QueryBuilder, bindCtx *BindContext, last int32, rowTag int32,
	col *ColDef, colIdx int, rowPositions []int, reason string,
) (int32, error) {
	projection := getProjectionByLastNodeWithTag(builder, last, rowTag)
	value := generatedUpdateExpr(builder, col, projection, rowPositions, false)
	ctx := builder.GetContext()
	unchanged, err := bindGeneratedPhysicalKeyEqual(ctx, DeepCopyExpr(projection[colIdx]), value, &col.Typ)
	if err != nil {
		return -1, err
	}
	assert, err := bindForeignKeyUpdateAssertion(ctx, unchanged, reason)
	if err != nil {
		return -1, err
	}
	if rowTag != 0 {
		projection = nil // column remapping builds the executable projection
	}
	return builder.appendDMLRowFilter(bindCtx, last, []*Expr{assert}, projection), nil
}

func unsupportedGeneratedKeyChange(update *dmlPlanCtx, col *ColDef, changesPK bool) string {
	table := update.tableDef
	if update.preserveUpdateSourceProjection {
		// REPLACE builds this plan before remapping. The index writer
		// expects the caller to have already remapped the input.
		for _, index := range table.Indexes {
			if index.TableExist && slices.ContainsFunc(index.Parts, func(part string) bool {
				return catalog.ResolveAlias(part) == col.Name
			}) {
				return "REPLACE foreign key action changing an indexed generated column"
			}
		}
		if changesPK {
			return "REPLACE foreign key action changing a generated primary key"
		}
	}
	if changesPK && slices.ContainsFunc(table.Indexes, isSyncMaintainedIrregularIndex) {
		return "update primary key on a table with a synchronous full-text/vector index"
	}
	return ""
}

func bindForeignKeyUpdateAssertion(ctx context.Context, condition *Expr, reason string) (*Expr, error) {
	return BindFuncExprImplByPlanExpr(ctx, "assert", []*Expr{
		condition, makePlan2StringConstExprWithType(reason), makePlan2StringConstExprWithType("fk_action_not_supported"),
	})
}

// Replace the child write with an assertion over its matched rows.
func appendRejectedForeignKeyUpdate(
	builder *QueryBuilder, bindCtx *BindContext, last int32, rowTag int32, rowID int, reason string,
) (int32, error) {
	projection := getProjectionByLastNode(builder, last)
	projection[rowID].GetCol().RelPos = rowTag
	absent, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "isnull", []*Expr{projection[rowID]})
	if err != nil {
		return -1, err
	}
	assert, err := bindForeignKeyUpdateAssertion(builder.GetContext(), absent, reason)
	if err != nil {
		return -1, err
	}
	builder.appendStep(builder.appendDMLRowFilter(bindCtx, last, []*Expr{assert}, projection))
	return -1, nil
}

// fallbackUpdateFromDedupPartitionCols returns the projection positions used to
// dedup duplicate source matches on the fallback (buildTableUpdate) path. The
// key is each updated target table's row_id, a stable row identity, rather than
// the whole old target row: partitioning on every old column would crash on
// GEOMETRY32 (no comparator), miss dedup on float columns holding NaN
// (NaN != NaN), and wrongly merge distinct rows whose columns happen to match.
func fallbackUpdateFromDedupPartitionCols(planCtxs []*dmlPlanCtx) []int32 {
	partitionCols := make([]int32, 0)
	offset := int32(0)
	for _, planCtx := range planCtxs {
		if planCtx.updateColLength > 0 && planCtx.rowIdPos >= 0 {
			partitionCols = append(partitionCols, offset+int32(planCtx.rowIdPos))
		}
		offset += int32(len(planCtx.tableDef.Cols) + planCtx.updateColLength)
	}
	return partitionCols
}

func selectUpdateTables(builder *QueryBuilder, bindCtx *BindContext, stmt *tree.Update, tableInfo *dmlTableInfo, repeated *repeatedUpdateAssignments) (int32, []*dmlPlanCtx, error) {
	// Merge target table list with PostgreSQL-style FROM sources so that the
	// inner SELECT can resolve column references against both. tableInfo only
	// tracks targets, so FROM-clause tables remain read-only join sources.
	selectFromTables := stmt.Tables
	if stmt.From != nil && len(stmt.From.Tables) > 0 {
		joined := tree.TableExpr(stmt.Tables[0])
		for _, src := range stmt.From.Tables {
			joined = &tree.JoinTableExpr{Left: joined, Right: src, JoinType: tree.JOIN_TYPE_CROSS}
		}
		selectFromTables = tree.TableExprs{joined}
	}
	fromTables := &tree.From{
		Tables: selectFromTables,
	}
	var err error
	var selectList []tree.SelectExpr
	legacyNumericTargets := make(map[int]Type)

	var aliasList = make([]string, len(tableInfo.alias))
	for alias, i := range tableInfo.alias {
		aliasList[i] = alias
	}

	updatePlanCtxs := make([]*dmlPlanCtx, len(aliasList))
	if repeated != nil {
		repeated.source = newRepeatedUpdateSource(&plan.Node{})
		repeated.roles = make([]*repeatedUpdateRole, len(aliasList))
		repeated.positions = make(map[int]*repeatedUpdateAssignment)
	}
	for i, alias := range aliasList {
		tableDef := tableInfo.tableDefs[i]
		updateKeys := tableInfo.updateKeys[i]
		if repeated != nil {
			repeated.roles[i] = repeated.source.newRole(builder.genNewBindTag(), len(tableDef.Cols))
		}

		// append  table.* to project list
		rowIdPos := -1
		for idx, col := range tableDef.Cols {
			e := tree.NewUnresolvedName(tree.NewCStr(alias, bindCtx.lower), tree.NewCStr(col.Name, 1))
			e.StoredValue = isEnumOrSetPlanType(&col.Typ)
			selectList = append(selectList, tree.SelectExpr{
				Expr: e,
			})
			if col.Name == catalog.Row_ID {
				rowIdPos = idx
			}
		}

		// add update expr to project list
		updateColPosMap := make(map[string]int)
		offset := len(tableDef.Cols)
		updatePkCol := false
		updatePkColCount := 0
		var pkNameMap = make(map[string]struct{})
		if tableDef.Pkey != nil {
			for _, pkName := range tableDef.Pkey.Names {
				pkNameMap[pkName] = struct{}{}
			}
		}

		// Emit the update columns in a deterministic order. updateKeys is a Go
		// map, so ranging it directly appended the update exprs to the project
		// list (and recorded updateColPosMap) in random order across runs — the
		// per-target column-block half of the same nondeterministic-layout bug
		// the table-order fix in getUpdateTableInfo addresses. Sorting the column
		// names emits exactly the same set in a stable order. (Order only affects
		// layout, not correctness: downstream looks columns up by name via
		// updateColPosMap, not by position.)
		updateColNames := make([]string, 0, len(updateKeys))
		for colName := range updateKeys {
			updateColNames = append(updateColNames, colName)
		}
		sort.Strings(updateColNames)
		for _, colName := range updateColNames {
			updateKey := updateKeys[colName]
			for _, coldef := range tableDef.Cols {
				if coldef.Name == colName {
					if isNumericAssignmentTarget(coldef.Typ) {
						legacyNumericTargets[len(selectList)] = coldef.Typ
					}
					_, directColumn := unwrapParenExpr(updateKey).(*tree.UnresolvedName)
					if isEnumOrSetPlanType(&coldef.Typ) && !(repeated != nil && directColumn) {
						updateKey, err = wrapAstExprForMySQLSpecialType(builder.GetContext(), coldef.Typ, updateKey)
						if err != nil {
							return 0, nil, err
						}
					}
				}
			}
			if repeated != nil {
				repeated.positions[len(selectList)] = &repeatedUpdateAssignment{role: repeated.roles[i]}
			}
			selectList = append(selectList, tree.SelectExpr{
				Expr: updateKey,
			})
			updateColPosMap[colName] = offset
			if _, ok := pkNameMap[colName]; ok {
				updatePkColCount++
			}
			offset++
		}

		// we don't known if update pk if tableDef.Pkey is nil. just set true and let check pk dup work
		updatePkCol = updatePkColCount > 0

		// append  table.* to project list
		upPlanCtx := getDmlPlanCtx()
		upPlanCtx.objRef = tableInfo.objRef[i]
		upPlanCtx.tableDef = tableDef
		upPlanCtx.updateColLength = len(updateKeys)
		upPlanCtx.isMulti = tableInfo.isMulti
		upPlanCtx.needAggFilter = tableInfo.needAggFilter
		upPlanCtx.rowIdPos = rowIdPos
		upPlanCtx.updateColPosMap = updateColPosMap
		upPlanCtx.allDelTableIDs = map[uint64]struct{}{}
		upPlanCtx.allDelTables = map[FkReferKey]struct{}{}
		upPlanCtx.checkInsertPkDup = true
		upPlanCtx.updatePkCol = updatePkCol

		updatePlanCtxs[i] = upPlanCtx
	}

	selectAst := &tree.Select{
		Select: &tree.SelectClause{
			Distinct: false,
			Exprs:    selectList,
			From:     fromTables,
			Where:    stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
		With:    stmt.With,
	}
	bindCtx.numericProjectionTypes = make([]Type, len(selectList))
	for pos, typ := range legacyNumericTargets {
		bindCtx.numericProjectionTypes[pos] = typ
	}
	if repeated != nil {
		repeated.width = len(selectList)
		repeated.source.project.ProjectList = make([]*plan.Expr, repeated.width)
		bindCtx.repeatedUpdateAssignments = repeated
		defer func() { bindCtx.repeatedUpdateAssignments = nil }()
	}

	lastNodeId, err := builder.bindSelect(selectAst, bindCtx, false)
	if err != nil {
		return -1, nil, err
	}
	if repeated != nil {
		repeated.source.project = builder.qry.Nodes[lastNodeId]
	}

	return lastNodeId, updatePlanCtxs, nil
}

// todo:  seems this filter do not make any sense for check pk dup in update statement
//        need more research
// func getPkFilterExpr(builder *QueryBuilder, lastNodeId int32, upCtx *dmlPlanCtx, tableDef *TableDef) []*Expr {
// 	node := builder.qry.Nodes[lastNodeId]
// 	var pkNameMap = make(map[string]int)
// 	for idx, pkName := range tableDef.Pkey.Names {
// 		pkNameMap[pkName] = idx
// 	}

// 	filterExpr := make([]*Expr, len(tableDef.Pkey.Names))
// 	for colName, colIdx := range upCtx.updateColPosMap {
// 		if pkIdx, ok := pkNameMap[colName]; ok {
// 			switch e := node.ProjectList[colIdx].Expr.(type) {
// 			case *plan.Expr_C:
// 			case *plan.Expr_F:
// 				if e.F.Func.ObjName != "cast" {
// 					return nil
// 				}
// 				if _, isConst := e.F.Args[0].Expr.(*plan.Expr_C); !isConst {
// 					return nil
// 				}
// 			default:
// 				return nil
// 			}

// 			expr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{{
// 				Typ: node.ProjectList[colIdx].Typ,
// 				Expr: &plan.Expr_Col{
// 					Col: &ColRef{
// 						ColPos: int32(pkIdx),
// 						Name:   tableDef.Pkey.PkeyColName,
// 					},
// 				},
// 			}, node.ProjectList[colIdx]})
// 			if err != nil {
// 				return nil
// 			}
// 			filterExpr[pkIdx] = expr
// 		}
// 	}
// 	return filterExpr
// }
