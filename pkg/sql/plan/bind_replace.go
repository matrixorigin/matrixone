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

	lastNodeID, colName2Idx, skipUniqueIdx, err := builder.initInsertReplaceStmt(bindCtx, stmt.Rows, stmt.Columns, dmlCtx.objRefs[0], dmlCtx.tableDefs[0], true)
	if err != nil {
		return 0, err
	}

	return builder.appendDedupAndMultiUpdateNodesForBindReplace(bindCtx, dmlCtx, lastNodeID, colName2Idx, skipUniqueIdx)
}

func (builder *QueryBuilder) appendDedupAndMultiUpdateNodesForBindReplace(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	lastNodeID int32,
	colName2Idx map[string]int32,
	skipUniqueIdx []bool,
) (int32, error) {
	objRef := dmlCtx.objRefs[0]
	tableDef := dmlCtx.tableDefs[0]
	pkName := tableDef.Pkey.PkeyColName

	isFakePK := pkName == catalog.FakePrimaryKeyColName

	selectNode := builder.qry.Nodes[lastNodeID]
	selectTag := selectNode.BindingTags[0]

	idxObjRefs := make([]*plan.ObjectRef, len(tableDef.Indexes))
	idxTableDefs := make([]*plan.TableDef, len(tableDef.Indexes))

	oldColName2Idx := make(map[string][2]int32, len(tableDef.Cols)+len(tableDef.Indexes)*2)

	// hasUniqueIdx drives three things below:
	//   - For fake-PK tables: when false (no PK, no UK), REPLACE behaves like INSERT
	//     and we skip the LEFT JOIN entirely (would otherwise become a cross join).
	//   - For real-PK tables: when true, the merged-scan optimization is disabled so
	//     that a UK-only conflict (no PK conflict) can still locate the old row via
	//     the legacy LEFT JOIN. Merged scan only captures old columns on a PK match.
	//   - When true, a per-input-row uuid column is materialized below and used as
	//     the MULTI_UPDATE insert-dedup group id. This collapses LEFT-JOIN OR
	//     fan-out (each fan-out copy of one input row shares the same uuid) while
	//     keeping two genuinely distinct VALUES rows independent — including the
	//     edge case where both rows have identical content (uuid is volatile and
	//     issued per row at the projection that produces it).
	hasUniqueIdx := false
	for _, idxDef := range tableDef.Indexes {
		if idxDef.Unique {
			hasUniqueIdx = true
			break
		}
	}

	// When the OR'd LEFT JOIN path is in use, materialize a uuid column on the
	// new-row pipeline BEFORE the LEFT JOIN OR so fan-out copies share the same
	// uuid value. Use a Node_PROJECT (uuid() is volatile so the PROJECT is not
	// eligible for removeSimpleProjections folding — it stays in the tree and
	// is evaluated once per source row).
	var rowUUIDColPosInSelect int32 = -1
	if hasUniqueIdx {
		uuidExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "uuid", nil)
		if err != nil {
			return 0, err
		}
		uuidProjTag := builder.genNewBindTag()
		uuidProjList := make([]*plan.Expr, 0, len(selectNode.ProjectList)+1)
		for i, expr := range selectNode.ProjectList {
			uuidProjList = append(uuidProjList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectTag,
						ColPos: int32(i),
					},
				},
			})
		}
		rowUUIDColPosInSelect = int32(len(uuidProjList))
		uuidProjList = append(uuidProjList, uuidExpr)

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: uuidProjList,
			Children:    []int32{lastNodeID},
			BindingTags: []int32{uuidProjTag},
		}, bindCtx)
		selectNode = builder.qry.Nodes[lastNodeID]
		selectTag = uuidProjTag
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
	// Merged-scan only handles PK conflicts (it captures old columns from the PK
	// DEDUP JOIN). A UK-only conflict produces no PK match, leaving the old
	// columns NULL and surfacing the conflict as a Duplicate Entry. Disable
	// merged-scan whenever the table has any unique secondary index so the
	// legacy LEFT JOIN path below (with OR'd PK/UK conditions) can pick up
	// UK-only conflicts.
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

		// Build the LEFT JOIN ON list as (optional PK cond) OR
		// (UK_1 AND-of-parts) OR (UK_2 AND-of-parts) OR ... so that an old row
		// conflicting on PK OR any unique key is fetched in a single join.
		//
		// When several conditions match different old rows for the same new
		// row, the LEFT JOIN fans out the new row by N. MULTI_UPDATE then uses
		// ReplaceGroupIdIdx (set below) to keep only the first occurrence of
		// each new-row group on the insert side; deletes still see every
		// fan-out row so all matched old rows are removed.
		buildUKCondition := func(idxDef *plan.IndexDef) *plan.Expr {
			var parts []*plan.Expr
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
				parts = append(parts, partCond)
			}
			if len(parts) == 0 {
				return nil
			}
			combined := parts[0]
			for _, c := range parts[1:] {
				combined, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*plan.Expr{combined, c})
			}
			return combined
		}

		var joinConds []*plan.Expr
		if !isFakePK {
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
		}
		for _, idxDef := range tableDef.Indexes {
			if !idxDef.Unique {
				continue
			}
			if ukCond := buildUKCondition(idxDef); ukCond != nil {
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

	// detect primary key confliction (skip for fake PK tables).
	//
	// When hasUniqueIdx, the LEFT JOIN above already discovers the conflicting
	// old row via OR'd PK/UK conditions and emits one pipeline row per matched
	// old row. The PK DEDUP JOIN here is then both redundant (PK conflict is
	// already represented in the LEFT JOIN match) and incorrect: its build
	// hashmap is fed by the post-fan-out pipeline, which legitimately contains
	// repeated PK values, and Node_FAIL would raise DuplicateEntry on them.
	// Skip it; engine-level PK constraint catches the rare case where the user
	// supplies the same PK value twice in one REPLACE batch.
	if !isFakePK && !hasUniqueIdx {
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

		dedupJoinCtx := &plan.DedupJoinCtx{}
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
			dedupJoinCtx.OldColList = []plan.ColRef{
				{
					RelPos: oldPkPos[0],
					ColPos: oldPkPos[1],
				},
			}
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

	// detect unique key confliction.
	//
	// Skipped for the same reason as the PK DEDUP JOIN above when hasUniqueIdx:
	// the LEFT JOIN OR already captured the conflict and the post-fan-out
	// pipeline contains repeated UK values that would otherwise trip Node_FAIL.
	for i, idxDef := range tableDef.Indexes {
		if !idxDef.Unique || hasUniqueIdx {
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
				OldColList: []plan.ColRef{
					{
						RelPos: oldPkPos[0],
						ColPos: oldPkPos[1],
					},
				},
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
			ObjRef:     objRef,
			TableDef:   tableDef,
			InsertCols: insertCols,
			DeleteCols: deleteCols,
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
		oldColRef := oldColName2Idx[idxDef.IndexTableName+"."+catalog.Row_ID]
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
		oldColRef = oldColName2Idx[idxDef.IndexTableName+"."+lookupColName]
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

	// When the table has a unique secondary index (or PK + UK combo), the
	// LEFT JOIN above ORs PK and per-UK equality conditions to find conflicts.
	// One new row that conflicts on N different old rows fans out into N
	// pipeline rows. Pin a per-input-row uuid (materialized in the PROJECT
	// inserted above selectNode) as the MULTI_UPDATE insert-dedup group id:
	//   - Fan-out copies share the same uuid (LEFT JOIN replicates the LHS
	//     row verbatim), so only the first occurrence survives the operator's
	//     dedup and the new row is inserted exactly once.
	//   - Two genuinely distinct VALUES rows each get an independent uuid —
	//     even when their content is bit-for-bit identical — so neither is
	//     silently dropped.
	//
	// The col reference is bumped by the MULTI_UPDATE refcount pass and
	// remapped like InsertCols so the upstream PROJECT preserves it.
	var replaceGroupIdCol *plan.ColRef
	if hasUniqueIdx {
		uuidColPosInFullProj := rowUUIDColPosInSelect
		groupIdColPos := int32(len(finalProjList))
		finalProjList = append(finalProjList, &plan.Expr{
			Typ: fullProjList[uuidColPosInFullProj].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: fullProjTag,
					ColPos: uuidColPosInFullProj,
				},
			},
		})
		replaceGroupIdCol = &plan.ColRef{
			RelPos: finalProjTag,
			ColPos: groupIdColPos,
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
		NodeType:          plan.Node_MULTI_UPDATE,
		Children:          []int32{lastNodeID},
		BindingTags:       []int32{builder.genNewBindTag()},
		UpdateCtxList:     updateCtxList,
		ReplaceGroupIdCol: replaceGroupIdCol,
	}, bindCtx)

	return lastNodeID, nil
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
