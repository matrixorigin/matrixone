// Copyright 2026 Matrix Origin
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

// Package plan implements the IVF-FLAT plugin's plan-layer hooks:
//
//   - BuildSecondaryIndexDefs — schema.go (Phase 4d)
//   - CanApply, ApplyForSort  — this file + context.go + helpers.go (Phase 4e)
//   - DMLSyncTableTypes, BuildPreInsertSyncPlan, BuildDeleteSyncPlan
//                             — Phase 4f (still stubs below)
//
// The ANN rewrite is the largest of any vector-index plugin's plan-layer
// work because IVF-FLAT supports three search modes (auto / pre / post)
// plus an auto-mode "two-scan" rewrite that splits the plan into a coarse
// pass + a refining pass. See:
//
//   - PrepareContext (context.go) — mode resolution, adaptive nprobe.
//   - ApplyForSort (this file)    — table-function node + join wiring.
//   - helpers.go                  — pure utilities (refsColumn,
//                                   buildPkExprFromNode, etc.).
package plan

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/vectorplan"
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"
)

// Compile-time interface check.
var _ planplugin.Hooks = Hooks{}

// Hooks implements plugin/plan.Hooks for IVF-FLAT.
type Hooks struct{}

// CanApply is the non-destructive probe used by detectVectorGuard to
// gate scan-node protection. Should reach the same true/false verdict
// as ApplyForSort, but without mutating any plan state. For IVF-FLAT we
// run PrepareContext (pure) and report whether it produced a context.
func (Hooks) CanApply(pb vectorplan.PlanBuilder, vecCtx *vectorplan.VectorSortContext, mti *vectorplan.MultiTableIndexRef) (bool, error) {
	ctx, err := PrepareContext(pb, vecCtx, mti)
	if err != nil {
		return false, err
	}
	return ctx != nil, nil
}

// ApplyForSort rewrites the query plan to use the IVF-FLAT index for the
// captured `ORDER BY <distfn>(col, v) LIMIT k` pattern. Returns:
//
//	newNodeID — the root of the rewritten sub-plan (or `nodeID` unchanged)
//	applied   — true if the rewrite was performed; false if this index
//	            cannot satisfy the query (PrepareContext returned nil)
//	err       — non-nil only on hard errors
//
// Lifted verbatim from applyIndicesForSortUsingIvfflat
// (pkg/sql/plan/apply_indices_ivfflat.go:342).
func (Hooks) ApplyForSort(
	pb vectorplan.PlanBuilder,
	vecCtx *vectorplan.VectorSortContext,
	mti *vectorplan.MultiTableIndexRef,
	nodeID int32,
	opts vectorplan.ApplyForSortOpts,
) (int32, bool, error) {
	if vecCtx == nil || vecCtx.SortNode == nil || vecCtx.ScanNode == nil {
		return nodeID, false, nil
	}

	ctx := pb.CtxByNode(nodeID)
	projNode := vecCtx.ProjNode
	sortNode := vecCtx.SortNode
	scanNode := vecCtx.ScanNode
	childNode := vecCtx.ChildNode
	orderExpr := vecCtx.OrderExpr
	limit := vecCtx.Limit

	ivfCtx, err := PrepareContext(pb, vecCtx, mti)
	if err != nil || ivfCtx == nil {
		return nodeID, false, err
	}

	// Explicitly set Mode to "auto" if it was chosen by default — the
	// executor's isAdaptiveVectorSearch test consults the RankOption.
	if ivfCtx.IsAutoMode && (vecCtx.RankOption == nil || vecCtx.RankOption.Mode == "") {
		if vecCtx.RankOption == nil {
			vecCtx.RankOption = &plan.RankOption{}
		}
		vecCtx.RankOption.Mode = "auto"
		if sortNode.RankOption == nil {
			sortNode.RankOption = vecCtx.RankOption
		}
		if scanNode.RankOption == nil {
			scanNode.RankOption = vecCtx.RankOption
		}
		if projNode.RankOption == nil {
			projNode.RankOption = vecCtx.RankOption
		}
	}

	tableConfigStr := fmt.Sprintf(`{"db": "%s", "src": "%s", "metadata":"%s", "index":"%s", "threads_search": %d,
			"entries": "%s", "nprobe" : %d, "pktype" : %d, "pkey" : "%s", "part" : "%s", "parttype" : %d, "orig_func_name": "%s"}`,
		scanNode.ObjRef.SchemaName,
		scanNode.TableDef.Name,
		ivfCtx.MetaDef.IndexTableName,
		ivfCtx.IdxDef.IndexTableName,
		ivfCtx.NThread,
		ivfCtx.EntriesDef.IndexTableName,
		uint(ivfCtx.NProbe),
		ivfCtx.PkType.Id,
		scanNode.TableDef.Pkey.PkeyColName,
		ivfCtx.IdxDef.Parts[0],
		ivfCtx.PartType.Id,
		ivfCtx.OrigFuncName)

	// Build the ivf_search FUNCTION_SCAN node.
	tableFuncTag := pb.GenNewBindTag()
	tableFuncNode := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name:  IVFFLATSearchFuncName,
				Param: []byte(ivfCtx.Params),
			},
			Cols: vectorplan.DeepCopyColDefList(IVFFLATSearchColDefs),
		},
		BindingTags: []int32{tableFuncTag},
		Children:    vectorplan.VectorSearchProviderChildren(vecCtx),
		TblFuncExprList: []*plan.Expr{
			{
				Typ: plan.Type{
					Id: int32(types.T_varchar),
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_Sval{
							Sval: tableConfigStr,
						},
					},
				},
			},
			vectorplan.DeepCopyExpr(ivfCtx.VecLitArg),
		},
	}
	tableFuncNodeID := pb.AppendNode(tableFuncNode, ctx)

	if err := pb.AddBinding(tableFuncNodeID, tree.AliasClause{Alias: tree.Identifier("mo_ivf_alias_0")}, ctx); err != nil {
		return 0, false, err
	}

	// Rewrite the pkid column type to the parent table's actual PK type.
	tableFuncNode.TableDef.Cols[0].Typ = ivfCtx.PkType

	newFilterList, distRange := pb.GetDistRangeFromFilters(scanNode.FilterList, ivfCtx.PartPos, ivfCtx.OrigFuncName, ivfCtx.VecLitArg)
	scanNode.FilterList = newFilterList

	// Pushdown limit to the table function. When residual filters remain
	// AND we're in post-mode, over-fetch so post-filter has enough
	// candidates to satisfy the LIMIT.
	limitExpr := vectorplan.DeepCopyExpr(limit)
	if len(scanNode.FilterList) > 0 && !ivfCtx.PushdownEnabled {
		if limitConst := limit.GetLit(); limitConst != nil {
			originalLimit := limitConst.GetU64Val()
			overFetchFactor := vectorplan.CalculateFilteredPostModeOverFetchFactor(originalLimit)
			newLimit := uint64(float64(originalLimit) * overFetchFactor)
			if newLimit < originalLimit+10 {
				newLimit = originalLimit + 10
			}

			if ivfCtx.IsAutoMode {
				logutil.Debugf(
					"Auto mode over-fetch: original_limit=%d, factor=%.2f, filter_count=%d",
					originalLimit, overFetchFactor, len(scanNode.FilterList),
				)
				logutil.Debugf(
					"Auto mode over-fetch result: original_limit=%d, new_limit=%d",
					originalLimit, newLimit,
				)
			} else {
				logutil.Debugf(
					"Vector mode over-fetch: mode=post, original_limit=%d, factor=%.2f, filter_count=%d, new_limit=%d",
					originalLimit, overFetchFactor, len(scanNode.FilterList), newLimit,
				)
			}

			limitExpr = &plan.Expr{
				Typ: limit.Typ,
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value: &plan.Literal_U64Val{
							U64Val: newLimit,
						},
					},
				},
			}
		}
	}

	tableFuncNode.IndexReaderParam = &plan.IndexReaderParam{
		Limit:        limitExpr,
		OrigFuncName: ivfCtx.OrigFuncName,
		DistRange:    distRange,
	}

	// Build the join graph. Pre-mode (pushdown) is a nested two-scan
	// rewrite; post-mode is a single ivf-then-table join.
	var joinRootID int32
	pushdownEnabled := ivfCtx.PushdownEnabled && len(scanNode.FilterList) > 0

	if pushdownEnabled {
		joinRootID, err = applyPreMode(pb, ivfCtx, ctx, scanNode, tableFuncNode, tableFuncNodeID, tableFuncTag, opts.ColRefCnt, opts.IdxColMap)
		if err != nil {
			return nodeID, false, err
		}
		if joinRootID < 0 {
			return nodeID, false, nil
		}
	} else {
		joinRootID = applyPostMode(pb, ivfCtx, ctx, scanNode, tableFuncNodeID, tableFuncTag)
	}

	// Keep FilterList on scanNode so filters are applied during the
	// table scan. Clear Limit/Offset since they go on the SORT below.
	scanNode.Limit = nil
	scanNode.Offset = nil

	orderByScore := []*plan.OrderBySpec{
		{
			Expr: &plan.Expr{
				Typ: tableFuncNode.TableDef.Cols[1].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag,
						ColPos: 1,
					},
				},
			},
			Flag: vecCtx.SortDirection,
		},
	}

	sortByID := pb.AppendNode(&plan.Node{
		NodeType:   plan.Node_SORT,
		Children:   []int32{joinRootID},
		OrderBy:    orderByScore,
		Limit:      limit,
		Offset:     vectorplan.DeepCopyExpr(sortNode.Offset),
		RankOption: vectorplan.DeepCopyRankOption(vecCtx.RankOption),
	}, ctx)

	projNode.Children[0] = sortByID

	if childNode != nil {
		sortIdx := orderExpr.GetCol().ColPos
		projMap := make(map[[2]int32]*plan.Expr)
		for i, proj := range childNode.ProjectList {
			if i == int(sortIdx) {
				projMap[[2]int32{childNode.BindingTags[0], int32(i)}] = vectorplan.DeepCopyExpr(orderByScore[0].Expr)
			} else {
				projMap[[2]int32{childNode.BindingTags[0], int32(i)}] = proj
			}
		}
		pb.ReplaceColumnsForNode(projNode, projMap)
	}

	return nodeID, true, nil
}

// applyPostMode is the simple plan shape: a single inner JOIN between
// the source scan and the ivf_search function, joining on PK equality.
// `mode != "pre"` or no remaining filters → post mode.
func applyPostMode(
	pb vectorplan.PlanBuilder,
	ivfCtx *IndexContext,
	ctx vectorplan.BindContext,
	scanNode *plan.Node,
	tableFuncNodeID, tableFuncTag int32,
) int32 {
	wherePkEqPk, _ := pb.BindFuncByName("=", []*plan.Expr{
		{
			Typ: ivfCtx.PkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanNode.BindingTags[0],
					ColPos: ivfCtx.PkPos,
				},
			},
		},
		{
			Typ: ivfCtx.PkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tableFuncTag,
					ColPos: 0,
				},
			},
		},
	})
	return pb.AppendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{scanNode.NodeId, tableFuncNodeID},
		JoinType: plan.Node_INNER,
		OnList:   []*plan.Expr{wherePkEqPk},
	}, ctx)
}

// applyPreMode builds the two-scan rewrite used when filter pushdown is
// enabled AND filters remain on the scan. The plan shape:
//
//	JOIN(
//	  outerScan,                  -- original table, regular-index-optimized
//	  JOIN(
//	    ivf_search,                -- runtime-filter probe-side
//	    secondScanProject,         -- clone of outerScan, projects PK only,
//	  )                            -- BloomFilter build-side
//	)
//
// Returns (joinRootID, nil) on success, (-1, nil) when we should bail
// out of the rewrite (e.g. PK extraction failed for the second scan).
func applyPreMode(
	pb vectorplan.PlanBuilder,
	ivfCtx *IndexContext,
	ctx vectorplan.BindContext,
	scanNode, tableFuncNode *plan.Node,
	tableFuncNodeID, tableFuncTag int32,
	colRefCnt map[[2]int32]int,
	idxColMap map[[2]int32]*plan.Expr,
) (int32, error) {
	// Clone the original scan as the second scan (inner side of the
	// BloomFilter join).
	secondScanNodeID := pb.CopyNode(ctx, scanNode.NodeId)
	secondScanNode := pb.Query().Nodes[secondScanNodeID]
	oldTag := secondScanNode.BindingTags[0]
	pb.RebindScanNode(secondScanNode)
	newTag := secondScanNode.BindingTags[0]

	// Carry the optimizer maps onto the new binding tag so a regular-index
	// rewrite on the second scan can still find what it needs.
	if oldTag != newTag {
		for key, value := range colRefCnt {
			if key[0] == oldTag {
				colRefCnt[[2]int32{newTag, key[1]}] = value
			}
		}
		for key, value := range idxColMap {
			if key[0] == oldTag {
				idxColMap[[2]int32{newTag, key[1]}] = vectorplan.DeepCopyExpr(value)
			}
		}
	}

	if canApplyRegularIndex(secondScanNode) {
		// Strip filters that touch only the vector column — the cloned
		// scan only needs to emit PKs for the BloomFilter join.
		var cleaned []*plan.Expr
		for _, expr := range secondScanNode.FilterList {
			if refsColumn(expr, newTag, ivfCtx.PartPos) {
				continue
			}
			cleaned = append(cleaned, expr)
		}
		secondScanNode.FilterList = cleaned

		// Build a minimal colRefCnt for the second scan so index-only
		// planning still works.
		secondColRefCnt := make(map[[2]int32]int)
		secondColRefCnt[[2]int32{newTag, ivfCtx.PkPos}] = 1
		for _, expr := range secondScanNode.FilterList {
			extractColRefs(expr, newTag, secondColRefCnt)
		}
		secondScanNodeID = pb.ApplyIndicesForFilters(secondScanNodeID, secondScanNode, secondColRefCnt, idxColMap)
	}

	// Drop limit/offset in the inner subtree — the BloomFilter join must
	// see the full PK set, not a truncated subset.
	ClearLimitOffsetInSubtree(pb.Query(), secondScanNodeID)

	secondProjectTag := pb.GenNewBindTag()
	secondPkExpr := buildPkExprFromNode(pb, secondScanNodeID, ivfCtx.PkType, scanNode.TableDef.Pkey.PkeyColName)
	if secondPkExpr == nil {
		// Bail: an optimized second-scan subtree without a stable PK
		// would wire stale bindings into the join.
		return -1, nil
	}
	secondProjectNodeID := pb.AppendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{secondScanNodeID},
		ProjectList: []*plan.Expr{secondPkExpr},
		BindingTags: []int32{secondProjectTag},
	}, ctx)

	// Inner join: (ivf_search ⋈ secondProject on tableFunc.pkid = scan.pk).
	innerJoinOn, _ := pb.BindFuncByName("=", []*plan.Expr{
		{
			Typ: ivfCtx.PkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tableFuncTag,
					ColPos: 0,
				},
			},
		},
		{
			Typ: ivfCtx.PkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: secondProjectTag,
					ColPos: 0,
				},
			},
		},
	})
	innerJoinNodeID := pb.AppendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{tableFuncNodeID, secondProjectNodeID},
		JoinType: plan.Node_INNER,
		OnList:   []*plan.Expr{innerJoinOn},
	}, ctx)

	// BloomFilter runtime filter between the inner join's two children.
	rfTag := pb.GenNewMsgTag()
	buildExpr := &plan.Expr{
		Typ: ivfCtx.PkType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: secondProjectTag,
				ColPos: 0,
			},
		},
	}
	buildSpec := vectorplan.MakeRuntimeFilter(rfTag, false, 0, buildExpr, false)
	buildSpec.UseBloomFilter = true
	innerJoinNode := pb.Query().Nodes[innerJoinNodeID]
	innerJoinNode.RuntimeFilterBuildList = []*plan.RuntimeFilterSpec{buildSpec}

	probeExpr := &plan.Expr{
		Typ: ivfCtx.PkType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: tableFuncTag,
				ColPos: 0,
			},
		},
	}
	probeSpec := vectorplan.MakeRuntimeFilter(rfTag, false, 0, probeExpr, false)
	probeSpec.UseBloomFilter = true
	tableFuncNode.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{probeSpec}

	// Outer scan: temporarily suspend the scan-protection guard so the
	// regular-index optimizer can layer secondary-index rewrite onto
	// the row-fetch side of the outer join.
	outerScanNodeID := scanNode.NodeId
	if canApplyRegularIndex(scanNode) {
		pb.WithSuspendedScanProtection(scanNode.NodeId, func() {
			outerScanNodeID = pb.ApplyIndicesForFilters(scanNode.NodeId, scanNode, colRefCnt, idxColMap)
		})
	}

	outerPkExpr := buildPkExprFromNode(pb, outerScanNodeID, ivfCtx.PkType, scanNode.TableDef.Pkey.PkeyColName)
	if outerPkExpr == nil && outerScanNodeID != scanNode.NodeId {
		// Regular-index rewrite produced an unsupported subtree shape.
		// Fall back to the unoptimized scan rather than wiring stale
		// bindings.
		logutil.Debugf("IVF outer PK fallback: optimized node %d -> original scan %d", outerScanNodeID, scanNode.NodeId)
		outerScanNodeID = scanNode.NodeId
		outerPkExpr = buildPkExprFromNode(pb, outerScanNodeID, ivfCtx.PkType, scanNode.TableDef.Pkey.PkeyColName)
	}
	if outerPkExpr == nil {
		return -1, nil
	}

	// Outer join: optimized outer subtree ⋈ inner ivf join on PK.
	outerOn, _ := pb.BindFuncByName("=", []*plan.Expr{
		vectorplan.DeepCopyExpr(outerPkExpr),
		{
			Typ: ivfCtx.PkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tableFuncTag,
					ColPos: 0,
				},
			},
		},
	})
	outerJoinNodeID := pb.AppendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{outerScanNodeID, innerJoinNodeID},
		JoinType: plan.Node_INNER,
		OnList:   []*plan.Expr{outerOn},
	}, ctx)

	// IN-list runtime filter on the outer join:
	//   build side: inner ivf join (smaller set with real PKs)
	//   probe side: outer scan (block / row pruning at scan stage)
	rfTag2 := pb.GenNewMsgTag()
	outerHasProbeRuntimeFilter := false
	outerProbeNodeID := FindScanNodeByTag(pb.Query(), outerScanNodeID, outerPkExpr.GetCol().RelPos)
	if outerProbeNodeID >= 0 {
		probeSpec2 := vectorplan.MakeRuntimeFilter(rfTag2, false, 0, vectorplan.DeepCopyExpr(outerPkExpr), false)
		pb.Query().Nodes[outerProbeNodeID].RuntimeFilterProbeList = append(
			pb.Query().Nodes[outerProbeNodeID].RuntimeFilterProbeList, probeSpec2)
		outerHasProbeRuntimeFilter = true
	}

	buildExpr2 := &plan.Expr{
		Typ: ivfCtx.PkType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: -1,
				ColPos: 0,
			},
		},
	}
	const unlimitedInFilterCard = int32(1<<31 - 1)
	buildSpec2 := vectorplan.MakeRuntimeFilter(rfTag2, false, unlimitedInFilterCard, buildExpr2, false)
	if outerHasProbeRuntimeFilter {
		outerJoinNode := pb.Query().Nodes[outerJoinNodeID]
		outerJoinNode.RuntimeFilterBuildList = append(outerJoinNode.RuntimeFilterBuildList, buildSpec2)
	}

	return outerJoinNodeID, nil
}

// DMLSyncTableTypes — Phase 4f will return the entries-table type.
// Until then, pkg/sql/plan/build_dml_util.go drives sync via the inline
// IVFFLAT case.
func (Hooks) DMLSyncTableTypes() []string {
	return nil
}

// BuildPreInsertSyncPlan — stub until Phase 4f lifts
// appendPreInsertSkVectorPlan + the IVFFLAT arm of
// buildPreInsertMultiTableIndexes.
func (Hooks) BuildPreInsertSyncPlan(
	_ vectorplan.PlanBuilder, _ vectorplan.BindContext,
	_ vectorplan.DMLInsertContext, _ *vectorplan.MultiTableIndexRef,
) error {
	return nil
}

// BuildDeleteSyncPlan — stub until Phase 4f lifts the IVFFLAT arm of
// buildDeleteMultiTableIndexes.
func (Hooks) BuildDeleteSyncPlan(
	_ vectorplan.PlanBuilder, _ vectorplan.BindContext,
	_ vectorplan.DMLDeleteContext, _ *vectorplan.MultiTableIndexRef,
) error {
	return nil
}
