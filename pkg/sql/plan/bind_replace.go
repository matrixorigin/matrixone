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

	// Collect the irregular (vector) indexes that must be maintained synchronously
	// BEFORE initInsertReplaceStmt strips them from tableDef.Indexes. The modern
	// REPLACE path otherwise drops these indexes silently, leaving newly inserted /
	// replaced rows unsearchable until the next cron reindex (issue #25000).
	syncIvfIndexes, err := collectSyncIvfMultiIndexes(dmlCtx.tableDefs[0])
	if err != nil {
		return 0, err
	}
	syncFulltextIndexes, err := collectSyncFulltextIndexes(dmlCtx.tableDefs[0])
	if err != nil {
		return 0, err
	}

	lastNodeID, colName2Idx, skipUniqueIdx, err := builder.initInsertReplaceStmt(bindCtx, stmt.Rows, stmt.Columns, dmlCtx.objRefs[0], dmlCtx.tableDefs[0], true)
	if err != nil {
		return 0, err
	}

	return builder.appendDedupAndMultiUpdateNodesForBindReplace(bindCtx, dmlCtx, lastNodeID, colName2Idx, skipUniqueIdx, syncIvfIndexes, syncFulltextIndexes)
}

// deferredReplaceIvfCtx captures everything needed to build the synchronous
// ivfflat entries maintenance sub-plans for a REPLACE, deferred until after
// createQuery. A REPLACE inserts new entries for every incoming row (insert side)
// and deletes the old entries of every conflicting row (delete side). Both sides
// read the same processed-new-rows sink: on a primary-key conflict the new PK
// equals the replaced row's PK, so deleting the entries whose org_pk matches an
// incoming PK removes exactly the stale entries; a brand-new PK matches no entry
// and deletes nothing.
type deferredReplaceIvfCtx struct {
	objRef            *plan.ObjectRef
	tableDef          *plan.TableDef
	multiTableIndexes map[string]*MultiTableIndex
	fulltextIndexes   []*plan.IndexDef
	newRowsSourceStep int32
}

// drainDeferredReplaceIvf builds the ivfflat entries-insert sub-plans collected
// while binding REPLACE. It must be called AFTER createQuery: the sub-plans use
// the legacy sink-based builders (positional column references, hand-optimized),
// which would panic / be mis-rewritten if run through createQuery's per-step
// optimize+remap pipeline — the same reason the legacy insert path builds its
// index plans after createQuery.
func (builder *QueryBuilder) drainDeferredReplaceIvf() error {
	if len(builder.deferredReplaceIvf) == 0 {
		return nil
	}

	stepCountBefore := len(builder.qry.Steps)
	for _, ivf := range builder.deferredReplaceIvf {
		if len(ivf.multiTableIndexes) > 0 {
			// delete side: remove the stale entries of the conflicting rows first.
			if err := builder.buildReplaceIvfDelete(ivf); err != nil {
				return err
			}
			// insert side: add entries for every incoming row.
			ivfBindCtx := NewBindContext(builder, nil)
			if err := buildPreInsertMultiTableIndexes(builder.compCtx, builder, ivfBindCtx,
				ivf.objRef, ivf.tableDef, ivf.newRowsSourceStep, ivf.multiTableIndexes); err != nil {
				return err
			}
		}

		// fulltext: one POSTDML node per index deletes the conflicting rows' stale
		// inverted entries (by doc id = the incoming PK) and re-tokenizes the new
		// rows, all reading the same processed-new-rows sink.
		if err := builder.buildReplaceFulltext(ivf); err != nil {
			return err
		}
	}
	builder.deferredReplaceIvf = nil

	// Finalize ONLY the newly appended index steps, the same lightweight pass the
	// legacy DML paths apply via tempOptimizeForDML (stats + hash-map / joinmap
	// message tags). The main REPLACE step was already fully finalized by
	// createQuery and must not be reprocessed.
	for i := stepCountBefore; i < len(builder.qry.Steps); i++ {
		rootID := builder.qry.Steps[i]
		ReCalcNodeStats(rootID, builder, true, false, true)
		builder.handleHashMapMessages(rootID)
	}
	return nil
}

// buildReplaceIvfDelete appends the ivfflat entries-delete sub-plan that removes
// the stale index entries of the rows a REPLACE conflicts on. It reads the
// processed new rows (the same sink the insert side uses) and projects them into a
// tableDef.Cols layout carrying the incoming primary key (NULL for the other
// columns), then feeds the legacy buildDeleteMultiTableIndexes (isUpdate=false),
// which joins the entries table on org_pk = the incoming PK and deletes the match.
//
// On a primary-key conflict the incoming PK equals the replaced row's PK, so its
// old entry (org_pk == that PK) is deleted; the insert side then re-adds the entry
// for the new vector. A brand-new PK matches no entry and deletes nothing.
func (builder *QueryBuilder) buildReplaceIvfDelete(ivf *deferredReplaceIvfCtx) error {
	tableDef := ivf.tableDef
	delBindCtx := NewBindContext(builder, nil)

	// SINK_SCAN the processed new rows (projList2 layout: tableDef.Cols order with
	// Row_ID dropped, so the primary key sits at its tableDef.Cols index).
	scanID := appendSinkScanNode(builder, delBindCtx, ivf.newRowsSourceStep)

	pkColIdx := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	oldRowProj := make([]*plan.Expr, len(tableDef.Cols))
	for i, col := range tableDef.Cols {
		if int32(i) == pkColIdx {
			oldRowProj[i] = &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{RelPos: 0, ColPos: pkColIdx},
				},
			}
		} else {
			oldRowProj[i] = &plan.Expr{
				Typ:  col.Typ,
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
			}
		}
	}
	projID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{scanID},
		ProjectList: oldRowProj,
	}, delBindCtx)

	sinkID := appendSinkNode(builder, delBindCtx, projID)
	oldRowsStep := builder.appendStep(sinkID)

	delCtx := &dmlPlanCtx{
		objRef:          ivf.objRef,
		tableDef:        tableDef,
		sourceStep:      oldRowsStep,
		updateColLength: 0,
	}
	return buildDeleteMultiTableIndexes(builder.compCtx, builder, delBindCtx, delCtx, ivf.multiTableIndexes)
}

// buildReplaceFulltext appends, for each synchronously-maintained fulltext index,
// a single POSTDML node that maintains the inverted-index table for the REPLACE.
// It reads the processed new rows and, with both delete and insert enabled,
// removes each row's stale inverted entries keyed by the incoming primary key
// (doc id) and then re-tokenizes the new row. For a brand-new PK the delete part
// matches nothing and only the insert part takes effect.
func (builder *QueryBuilder) buildReplaceFulltext(ivf *deferredReplaceIvfCtx) error {
	for idx, indexdef := range ivf.fulltextIndexes {
		ftBindCtx := NewBindContext(builder, nil)
		indexObjRef, indexTableDef, err := builder.compCtx.ResolveIndexTableByRef(ivf.objRef, indexdef.IndexTableName, nil)
		if err != nil {
			return err
		}
		if indexTableDef == nil {
			return moerr.NewNoSuchTable(builder.GetContext(), ivf.objRef.SchemaName, indexdef.IndexName)
		}
		const (
			isDelete               = true
			isInsert               = true
			isDeleteWithoutFilters = false
		)
		if err := buildPostDmlFullTextIndex(builder.compCtx, builder, ftBindCtx,
			indexObjRef, indexTableDef, ivf.tableDef, ivf.newRowsSourceStep,
			indexdef, idx, isDelete, isInsert, isDeleteWithoutFilters); err != nil {
			return err
		}
	}
	return nil
}

// collectSyncIvfMultiIndexes groups the non-async ivfflat index definitions of a
// table by index name (metadata / centroids / entries), mirroring the grouping in
// buildInsertPlansWithRelatedHiddenTable. These are the irregular indexes the
// modern REPLACE path must maintain synchronously. Async-maintained indexes
// (catalog.IsIndexAsync) are left to the cron and excluded here, exactly as the
// legacy insert/update paths do.
func collectSyncIvfMultiIndexes(tableDef *plan.TableDef) (map[string]*MultiTableIndex, error) {
	res := make(map[string]*MultiTableIndex)
	for _, indexdef := range tableDef.Indexes {
		if !indexdef.TableExist || !catalog.IsIvfIndexAlgo(indexdef.IndexAlgo) {
			continue
		}
		async, err := catalog.IsIndexAsync(indexdef.IndexAlgoParams)
		if err != nil {
			return nil, err
		}
		if async {
			continue
		}
		if _, ok := res[indexdef.IndexName]; !ok {
			res[indexdef.IndexName] = &MultiTableIndex{
				IndexAlgo:       catalog.ToLower(indexdef.IndexAlgo),
				IndexAlgoParams: indexdef.IndexAlgoParams,
				IndexDefs:       make(map[string]*IndexDef),
			}
		}
		res[indexdef.IndexName].IndexDefs[catalog.ToLower(indexdef.IndexAlgoTableType)] = indexdef
	}
	return res, nil
}

// collectSyncFulltextIndexes returns the non-async fulltext index definitions of a
// table that the modern REPLACE path must maintain synchronously. Async indexes
// are left to the cron, exactly as the legacy insert/update paths do.
func collectSyncFulltextIndexes(tableDef *plan.TableDef) ([]*plan.IndexDef, error) {
	var res []*plan.IndexDef
	for _, indexdef := range tableDef.Indexes {
		if !indexdef.TableExist || !catalog.IsFullTextIndexAlgo(indexdef.IndexAlgo) {
			continue
		}
		async, err := catalog.IsIndexAsync(indexdef.IndexAlgoParams)
		if err != nil {
			return nil, err
		}
		if async {
			continue
		}
		res = append(res, indexdef)
	}
	return res, nil
}

// sinkNewRowsForReplace materializes the processed new-row projection into a SINK
// step and returns the step id together with a SINK_SCAN node that re-exposes the
// rows under the original binding tag. The caller keeps using the same tag for the
// main conflict-resolution branch, while index-maintenance sub-plans read the same
// step via their own SINK_SCAN — guaranteeing the new rows are computed once.
func (builder *QueryBuilder) sinkNewRowsForReplace(bindCtx *BindContext, nodeID, tag int32) (int32, int32) {
	sinkProj := getProjectionByLastNode(builder, nodeID)
	sinkID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_SINK,
		Children:    []int32{nodeID},
		ProjectList: sinkProj,
	}, bindCtx)
	sourceStep := builder.appendStep(sinkID)

	scanProj := make([]*plan.Expr, len(sinkProj))
	for i, e := range sinkProj {
		scanProj[i] = &plan.Expr{
			Typ: e.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tag,
					ColPos: int32(i),
				},
			},
		}
	}
	scanID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_SINK_SCAN,
		SourceStep:  []int32{sourceStep},
		ProjectList: scanProj,
		BindingTags: []int32{tag},
	}, bindCtx)
	return sourceStep, scanID
}

func (builder *QueryBuilder) appendDedupAndMultiUpdateNodesForBindReplace(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	lastNodeID int32,
	colName2Idx map[string]int32,
	skipUniqueIdx []bool,
	syncIvfIndexes map[string]*MultiTableIndex,
	syncFulltextIndexes []*plan.IndexDef,
) (int32, error) {
	objRef := dmlCtx.objRefs[0]
	tableDef := dmlCtx.tableDefs[0]
	pkName := tableDef.Pkey.PkeyColName

	isFakePK := pkName == catalog.FakePrimaryKeyColName

	selectNode := builder.qry.Nodes[lastNodeID]
	selectTag := selectNode.BindingTags[0]

	// Synchronous ivfflat maintenance (issue #25000): the processed new-row
	// projection must be materialized once and shared, so that auto-increment /
	// generated columns are evaluated a single time and the same values flow into
	// both the main conflict-resolution branch and the index-entries insert branch.
	// Sink it and point the main branch at a SINK_SCAN carrying the same binding
	// tag (so every downstream ColRef on selectTag stays valid). The main branch
	// copies the entire new-row projection into fullProjList below, so remap keeps
	// all sink columns and the tableDef.Cols layout the entries sub-plan relies on
	// is preserved. The entries-insert sub-plan itself is built AFTER createQuery
	// (see drainDeferredReplaceIvf), exactly like the legacy insert path, because
	// it is hand-built with positional column refs and must not be re-optimized.
	if len(syncIvfIndexes) > 0 || len(syncFulltextIndexes) > 0 {
		newRowsStep, newScanID := builder.sinkNewRowsForReplace(bindCtx, lastNodeID, selectTag)
		lastNodeID = newScanID
		selectNode = builder.qry.Nodes[lastNodeID]

		builder.deferredReplaceIvf = append(builder.deferredReplaceIvf, &deferredReplaceIvfCtx{
			objRef:            objRef,
			tableDef:          tableDef,
			newRowsSourceStep: newRowsStep,
			multiTableIndexes: syncIvfIndexes,
			fulltextIndexes:   syncFulltextIndexes,
		})
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
	for _, idxDef := range tableDef.Indexes {
		if idxDef.Unique {
			hasUniqueIdx = true
			break
		}
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
			TableDef:     tableDef,
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

		var err error
		for i, idxDef := range tableDef.Indexes {
			idxObjRefs[i], idxTableDefs[i], err = builder.compCtx.ResolveIndexTableByRef(objRef, idxDef.IndexTableName, bindCtx.snapshot)
			if err != nil {
				return 0, err
			}
			ensureName2ColIndexForReplace(idxTableDefs[i])
			oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTablePrimaryColName] = oldColName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]

			if !indexTableStoresSerializedKey(idxDef) {
				oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName] = oldColName2Idx[tableDef.Name+"."+indexPrimaryPartName(idxDef)]
			} else {
				args := make([]*plan.Expr, len(idxDef.Parts))
				for j, part := range idxDef.Parts {
					colIdx := tableDef.Name2ColIndex[catalog.ResolveAlias(part)]
					args[j] = &plan.Expr{
						Typ: tableDef.Cols[colIdx].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: oldScanTag,
								ColPos: colIdx,
							},
						},
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
			for _, idxDef := range tableDef.Indexes {
				if !idxDef.Unique {
					continue
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

			for _, idxDef := range tableDef.Indexes {
				if !idxDef.Unique {
					continue
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
			TableDef:     tableDef,
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
			for _, idxDef := range tableDef.Indexes {
				if !indexTableStoresSerializedKey(idxDef) {
					requiredOldCols[indexPrimaryPartName(idxDef)] = struct{}{}
				}
			}
			captureList := make([]plan.OldColCapture, 0, len(requiredOldCols))
			for i, col := range tableDef.Cols {
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
		if !idxDef.Unique {
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

		oldPkPos := oldColName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]
		deleteCols[1].RelPos = finalProjTag
		deleteCols[1].ColPos = int32(len(finalProjList))
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

		updateCtxList = append(updateCtxList, &plan.UpdateCtx{
			ObjRef:             objRef,
			TableDef:           tableDef,
			InsertCols:         insertCols,
			DeleteCols:         deleteCols,
			SkipInsertOnNullPk: true,
			InsertPkColIdx:     insertPkColIdx,
		})
	}

	for i, idxDef := range tableDef.Indexes {
		insertCols := make([]plan.ColRef, 2)
		deleteCols := make([]plan.ColRef, 2)

		newIdxPos := colName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName]
		if indexTableStoresSerializedKey(idxDef) {
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
			lockTargets = append(lockTargets, &plan.LockTarget{
				TableId:            idxTableDefs[i].TblId,
				ObjRef:             idxObjRefs[i],
				PrimaryColIdxInBat: int32(newIdxPos),
				PrimaryColRelPos:   finalProjTag,
				PrimaryColTyp:      finalProjList[newIdxPos].Typ,
			}, &plan.LockTarget{
				TableId:            idxTableDefs[i].TblId,
				ObjRef:             idxObjRefs[i],
				PrimaryColIdxInBat: int32(oldIdxPos),
				PrimaryColRelPos:   finalProjTag,
				PrimaryColTyp:      finalProjList[oldIdxPos].Typ,
			})
		}
	}

	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		ProjectList: finalProjList,
		BindingTags: []int32{finalProjTag},
	}, bindCtx)

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

	validIndexes, _ := getValidIndexes(tableDef)
	tableDef.Indexes = validIndexes

	skipUniqueIdx := make([]bool, len(tableDef.Indexes))
	pkName := tableDef.Pkey.PkeyColName
	pkPos := tableDef.Name2ColIndex[pkName]
	for i, idxDef := range tableDef.Indexes {
		skipUniqueIdx[i] = true
		for _, part := range idxDef.Parts {
			if !columnIsNull[catalog.ResolveAlias(part)] {
				skipUniqueIdx[i] = false
				break
			}
		}

		idxTableName := idxDef.IndexTableName
		colName2Idx[idxTableName+"."+catalog.IndexTablePrimaryColName] = pkPos
		if !indexTableStoresSerializedKey(idxDef) {
			colName2Idx[idxTableName+"."+catalog.IndexTableIndexColName] = colName2Idx[tableDef.Name+"."+indexPrimaryPartName(idxDef)]
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
				args[k] = DeepCopyExpr(projList2[colPos])
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
