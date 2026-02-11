// Copyright 2024 Matrix Origin
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
	"fmt"
	"math"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

type ivfIndexContext struct {
	vecCtx          *vectorSortContext
	metaDef         *plan.IndexDef
	idxDef          *plan.IndexDef
	entriesDef      *plan.IndexDef
	vecLitArg       *plan.Expr
	origFuncName    string
	partPos         int32
	partType        plan.Type
	pkPos           int32
	pkType          plan.Type
	params          string
	nThread         int64
	nProbe          int64
	pushdownEnabled bool

	// Phase 1: Auto mode support
	isAutoMode      bool   // Whether in auto mode
	initialStrategy string // Initial strategy selected in auto mode ("pre" or "post")
}

// shouldUseForceMode determines if force mode (full table scan) should be used
// based on table size and LIMIT value.
//
// Rule: Use force mode when table_rows < LIMIT × 2
//
// Rationale:
//   - For very small datasets, index overhead (metadata reading, distance calculation)
//     exceeds the benefit of using an index
//   - Full table scan is faster and guarantees 100% recall
//   - Does not rely on filter selectivity estimation (which may be inaccurate)
//
// Returns true if force mode should be used, false otherwise.
func (builder *QueryBuilder) shouldUseForceMode(vecCtx *vectorSortContext) bool {
	scanNode := vecCtx.scanNode
	stats := scanNode.Stats

	// Get table row count and selectivity from statistics
	var tableCnt float64
	var selectivity float64 = 1.0

	if stats != nil {
		tableCnt = stats.TableCnt
		if stats.Selectivity > 0 && stats.Selectivity < 1 {
			selectivity = stats.Selectivity
		}
	}

	// If no statistics available, conservatively use post mode
	if tableCnt <= 0 {
		return false
	}

	// Get LIMIT value
	limitExpr := vecCtx.limit
	if limitExpr == nil {
		return false
	}

	limitConst := limitExpr.GetLit()
	if limitConst == nil {
		return false
	}

	limitVal := float64(limitConst.GetU64Val())
	if limitVal <= 0 {
		return false
	}

	// Rule: Estimated rows after filtering < LIMIT × 2
	// For small result sets, brute force (force mode) is more reliable and often faster
	estimatedRows := tableCnt * selectivity
	threshold := limitVal * 2.0

	if tableCnt < threshold || estimatedRows < threshold {
		logutil.Debugf(
			"Auto mode: small dataset or high selectivity detected, table_rows=%.0f, selectivity=%.4f, estimated_rows=%.0f, limit=%.0f, threshold=%.0f",
			tableCnt, selectivity, estimatedRows, limitVal, threshold,
		)
		return true
	}

	return false
}

// resolveVectorSearchMode resolves the vector search mode based on user input and configuration.
// Returns:
//   - mode: The actual mode to use ("pre", "post", or "force")
//   - isAutoMode: Whether auto mode is enabled
//   - shouldDisableIndex: Whether vector index should be disabled
func (builder *QueryBuilder) resolveVectorSearchMode(
	vecCtx *vectorSortContext,
	enableVectorPrefilterByDefault bool,
	enableVectorAutoModeByDefault bool,
) (mode string, isAutoMode bool, shouldDisableIndex bool) {

	// 1. Parse user-specified mode
	var userMode string
	if vecCtx.rankOption != nil && vecCtx.rankOption.Mode != "" {
		userMode = vecCtx.rankOption.Mode
	}

	// 2. Handle force mode: disable vector index
	if userMode == "force" {
		return "force", false, true
	}

	// 3. Handle auto mode
	if userMode == "auto" || (userMode == "" && enableVectorAutoModeByDefault) {
		isAutoMode = true

		// Phase 2: Check if this is a very small dataset
		if builder.shouldUseForceMode(vecCtx) {
			logutil.Debugf("Auto mode: small dataset, selected 'force'")
			return "force", isAutoMode, true
		}

		// Default to post mode for normal cases
		logutil.Debugf("Auto mode: normal case, selected 'post'")
		mode = "post"
		return mode, isAutoMode, false
	}

	// 4. Handle explicitly specified pre/post mode
	if userMode == "pre" || userMode == "post" {
		return userMode, false, false
	}

	// 5. No mode specified: use default behavior
	if enableVectorPrefilterByDefault {
		mode = "pre"
	} else {
		mode = "post"
	}

	return mode, false, false
}

func (builder *QueryBuilder) calculateAdaptiveNprobe(baseNprobe int64, stats *plan.Stats, totalLists int64) int64 {
	// 1. If no statistics or invalid selectivity, keep as is
	if stats == nil || stats.Selectivity <= 0 || stats.Selectivity >= 1 {
		return baseNprobe
	}

	// 2. Calculate compensation factor (square root smoothing)
	// Square root is used to prevent nprobe from growing too fast and causing excessive overhead
	compensation := math.Sqrt(1.0 / stats.Selectivity)

	// 3. Calculate adaptive nprobe
	adaptiveNprobe := int64(math.Ceil(float64(baseNprobe) * compensation))

	// 4. Boundary handling: not less than base value, not more than total lists
	adaptiveNprobe = max(adaptiveNprobe, baseNprobe)
	adaptiveNprobe = min(adaptiveNprobe, totalLists)

	return adaptiveNprobe
}

func (builder *QueryBuilder) prepareIvfIndexContext(vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex) (*ivfIndexContext, error) {
	if vecCtx == nil || multiTableIndex == nil {
		return nil, nil
	}
	if vecCtx.distFnExpr == nil {
		return nil, nil
	}

	// Check if vector pre-filter pushdown should be enabled by default
	// This session variable changes the default vector search behavior
	var enableVectorPrefilterByDefault bool
	if val, err := builder.compCtx.ResolveVariable("enable_vector_prefilter_by_default", true, false); err == nil && val != nil {
		if v, ok := val.(int8); ok && v == 1 {
			enableVectorPrefilterByDefault = true
		}
	}

	var enableVectorAutoModeByDefault bool
	if val, err := builder.compCtx.ResolveVariable("enable_vector_auto_mode_by_default", true, false); err == nil && val != nil {
		if v, ok := val.(int8); ok && v == 1 {
			enableVectorAutoModeByDefault = true
		}
	}

	// Resolve vector search mode
	mode, isAutoMode, shouldDisableIndex := builder.resolveVectorSearchMode(
		vecCtx,
		enableVectorPrefilterByDefault,
		enableVectorAutoModeByDefault,
	)

	// If index should be disabled (force mode), return nil
	if shouldDisableIndex {
		return nil, nil
	}

	// Log auto mode activation
	if isAutoMode {
		logutil.Debugf("Vector search auto mode enabled, initial strategy: %s", mode)
	}

	metaDef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata]
	idxDef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids]
	entriesDef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries]
	if metaDef == nil || idxDef == nil || entriesDef == nil {
		return nil, nil
	}

	opTypeAst, err := sonic.Get([]byte(metaDef.IndexAlgoParams), catalog.IndexAlgoParamOpType)
	if err != nil {
		return nil, nil
	}
	opType, err := opTypeAst.StrictString()
	if err != nil {
		return nil, nil
	}

	// Get total lists for nprobe boundary handling
	var totalLists int64 = -1
	if listsAst, err2 := sonic.Get([]byte(metaDef.IndexAlgoParams), catalog.IndexAlgoParamLists); err2 == nil {
		if lists, err3 := listsAst.Int64(); err3 == nil {
			totalLists = lists
		}
	}

	origFuncName := vecCtx.distFnExpr.Func.ObjName
	if opType != metric.DistFuncOpTypes[origFuncName] {
		return nil, nil
	}

	keyPart := idxDef.Parts[0]
	partPos := vecCtx.scanNode.TableDef.Name2ColIndex[keyPart]
	_, vecLitArg, found := builder.getArgsFromDistFn(vecCtx.distFnExpr, partPos)
	if !found {
		return nil, nil
	}

	nThread, err := builder.compCtx.ResolveVariable("ivf_threads_search", true, false)
	if err != nil {
		return nil, err
	}

	nProbe := int64(5)
	if nProbeIf, err := builder.compCtx.ResolveVariable("probe_limit", true, false); err != nil {
		return nil, err
	} else if nProbeIf != nil {
		val, ok := nProbeIf.(int64)
		if !ok {
			return nil, moerr.NewInternalErrorNoCtx("ResolveVariable: probe_limit is not int64")
		}
		nProbe = val
	}

	// Phase 4: Dynamic nprobe amplification for auto mode
	// Only applied if mode is "post" (pushdown disabled) and totalLists is available
	if isAutoMode && mode == "post" && totalLists > 0 {
		oldNProbe := nProbe
		nProbe = builder.calculateAdaptiveNprobe(
			nProbe,
			vecCtx.scanNode.Stats,
			totalLists,
		)
		if nProbe != oldNProbe {
			logutil.Debugf("Auto mode: adjusted nprobe from %d to %d (selectivity: %.4f)",
				oldNProbe, nProbe, vecCtx.scanNode.Stats.Selectivity)
		}
	}

	pkPos := vecCtx.scanNode.TableDef.Name2ColIndex[vecCtx.scanNode.TableDef.Pkey.PkeyColName]
	pkType := vecCtx.scanNode.TableDef.Cols[pkPos].Typ
	partType := vecCtx.scanNode.TableDef.Cols[partPos].Typ

	return &ivfIndexContext{
		vecCtx:          vecCtx,
		metaDef:         metaDef,
		idxDef:          idxDef,
		entriesDef:      entriesDef,
		vecLitArg:       vecLitArg,
		origFuncName:    origFuncName,
		partPos:         partPos,
		partType:        partType,
		pkPos:           pkPos,
		pkType:          pkType,
		params:          idxDef.IndexAlgoParams,
		nThread:         nThread.(int64),
		nProbe:          nProbe,
		pushdownEnabled: (mode == "pre"),

		// Phase 1: Auto mode fields
		isAutoMode:      isAutoMode,
		initialStrategy: mode,
	}, nil
}

func (builder *QueryBuilder) getDistRangeFromFilters(filters []*plan.Expr, ivfCtx *ivfIndexContext) ([]*plan.Expr, *plan.DistRange) {
	var distRange *plan.DistRange

	currIdx := 0
	for _, filter := range filters {
		var (
			vecLit string
			fdist  *plan.Function
		)

		f := filter.GetF()
		if f == nil || len(f.Args) != 2 {
			goto NO_RANGE
		}

		fdist = f.Args[0].GetF()
		if fdist == nil || len(fdist.Args) != 2 {
			goto NO_RANGE
		}

		if partCol := fdist.Args[0].GetCol(); partCol == nil || partCol.ColPos != ivfCtx.partPos {
			goto NO_RANGE
		}

		if fdist.Func.ObjName != ivfCtx.origFuncName {
			goto NO_RANGE
		}

		vecLit = fdist.Args[1].GetLit().GetVecVal()
		if vecLit == "" || vecLit != ivfCtx.vecLitArg.GetLit().GetVecVal() {
			goto NO_RANGE
		}

		switch f.Func.ObjName {
		case "<":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			distRange.UpperBoundType = plan.BoundType_EXCLUSIVE
			distRange.UpperBound = f.Args[1]

		case "<=":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			distRange.UpperBoundType = plan.BoundType_INCLUSIVE
			distRange.UpperBound = f.Args[1]

		case ">":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			distRange.LowerBoundType = plan.BoundType_EXCLUSIVE
			distRange.LowerBound = f.Args[1]

		case ">=":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			distRange.LowerBoundType = plan.BoundType_INCLUSIVE
			distRange.LowerBound = f.Args[1]

		default:
			goto NO_RANGE
		}

		continue

	NO_RANGE:
		filters[currIdx] = filter
		currIdx++
	}

	return filters[:currIdx], distRange
}

func (builder *QueryBuilder) applyIndicesForSortUsingIvfflat(nodeID int32, vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {

	if vecCtx == nil || vecCtx.sortNode == nil || vecCtx.scanNode == nil {
		return nodeID, nil
	}

	ctx := builder.ctxByNode[nodeID]
	projNode := vecCtx.projNode
	sortNode := vecCtx.sortNode
	scanNode := vecCtx.scanNode
	childNode := vecCtx.childNode
	orderExpr := vecCtx.orderExpr
	limit := vecCtx.limit

	ivfCtx, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	if err != nil || ivfCtx == nil {
		return nodeID, err
	}

	// Phase 1: Explicitly set Mode to "auto" if it was chosen by default
	// This ensures isAdaptiveVectorSearch returns true
	if ivfCtx.isAutoMode && (vecCtx.rankOption == nil || vecCtx.rankOption.Mode == "") {
		if vecCtx.rankOption == nil {
			vecCtx.rankOption = &plan.RankOption{}
		}
		vecCtx.rankOption.Mode = "auto"

		// Sync back to nodes
		if sortNode.RankOption == nil {
			sortNode.RankOption = vecCtx.rankOption
		}
		if scanNode.RankOption == nil {
			scanNode.RankOption = vecCtx.rankOption
		}
		if projNode.RankOption == nil {
			projNode.RankOption = vecCtx.rankOption
		}
	}

	tableConfigStr := fmt.Sprintf(`{"db": "%s", "src": "%s", "metadata":"%s", "index":"%s", "threads_search": %d,
			"entries": "%s", "nprobe" : %d, "pktype" : %d, "pkey" : "%s", "part" : "%s", "parttype" : %d, "orig_func_name": "%s"}`,
		scanNode.ObjRef.SchemaName,
		scanNode.TableDef.Name,
		ivfCtx.metaDef.IndexTableName,
		ivfCtx.idxDef.IndexTableName,
		ivfCtx.nThread,
		ivfCtx.entriesDef.IndexTableName,
		uint(ivfCtx.nProbe),
		ivfCtx.pkType.Id,
		scanNode.TableDef.Pkey.PkeyColName,
		ivfCtx.idxDef.Parts[0],
		ivfCtx.partType.Id,
		ivfCtx.origFuncName)

	// build ivf_search table function node
	tableFuncTag := builder.genNewBindTag()
	tableFuncNode := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name:  kIVFSearchFuncName,
				Param: []byte(ivfCtx.params),
			},
			Cols: DeepCopyColDefList(kIVFSearchColDefs),
		},
		BindingTags: []int32{tableFuncTag},
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
			DeepCopyExpr(ivfCtx.vecLitArg),
		},
	}
	tableFuncNodeID := builder.appendNode(tableFuncNode, ctx)

	err = builder.addBinding(tableFuncNodeID, tree.AliasClause{Alias: tree.Identifier("mo_ivf_alias_0")}, ctx)
	if err != nil {
		return 0, err
	}

	// change doc_id type to the primary type here
	tableFuncNode.TableDef.Cols[0].Typ = ivfCtx.pkType

	newFilterList, distRange := builder.getDistRangeFromFilters(scanNode.FilterList, ivfCtx)
	scanNode.FilterList = newFilterList

	// pushdown limit to Table Function
	// When there are filters, over-fetch to get more candidates
	// This ensures we have enough candidates after filtering
	limitExpr := DeepCopyExpr(limit)
	if len(scanNode.FilterList) > 0 {
		// Over-fetch strategy: dynamically adjust factor based on limit size
		// Smaller limits need more over-fetching due to higher variance
		if limitConst := limit.GetLit(); limitConst != nil {
			originalLimit := limitConst.GetU64Val()

			// Choose over-fetch strategy based on mode
			var overFetchFactor float64
			if ivfCtx.isAutoMode {
				// Auto mode: use enhanced over-fetch strategy
				overFetchFactor = calculateAutoModeOverFetchFactor(
					originalLimit,
					scanNode.Stats,
				)

				// Log auto mode over-fetch calculation
				logutil.Debugf(
					"Auto mode over-fetch: original_limit=%d, factor=%.2f, filter_count=%d",
					originalLimit, overFetchFactor, len(scanNode.FilterList),
				)
			} else {
				// Non-auto mode: use conservative default strategy
				overFetchFactor = calculatePostFilterOverFetchFactor(originalLimit)
			}

			newLimit := max(uint64(float64(originalLimit)*overFetchFactor), originalLimit+10)

			// Log final over-fetch result for auto mode
			if ivfCtx.isAutoMode {
				logutil.Debugf(
					"Auto mode over-fetch result: original_limit=%d, new_limit=%d",
					originalLimit, newLimit,
				)
			}

			limitExpr = &Expr{
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
		OrigFuncName: ivfCtx.origFuncName,
		DistRange:    distRange,
	}

	// Determine join structure based on rankOption.mode:
	//   mode != "pre": JOIN( scanNode, ivf_search )
	//   mode == "pre": JOIN( scanNode, JOIN(ivf_search, secondScan) )
	var joinRootID int32

	pushdownEnabled := ivfCtx.pushdownEnabled && len(scanNode.FilterList) > 0

	if pushdownEnabled {
		// secondScanNode: copy original scanNode for JOIN(ivf, table)
		secondScanNodeID := builder.copyNode(ctx, scanNode.NodeId)
		secondScanNode := builder.qry.Nodes[secondScanNodeID]
		baseSecondScan := secondScanNode

		oldTag := secondScanNode.BindingTags[0]
		builder.rebindScanNode(secondScanNode)
		newTag := secondScanNode.BindingTags[0]

		// Update colRefCnt and idxColMap to reflect the new binding tag
		// This is essential for index optimization to work correctly on the rebound node
		if oldTag != newTag {
			for key, value := range colRefCnt {
				if key[0] == oldTag {
					colRefCnt[[2]int32{newTag, key[1]}] = value
				}
			}
			for key, value := range idxColMap {
				if key[0] == oldTag {
					idxColMap[[2]int32{newTag, key[1]}] = DeepCopyExpr(value)
				}
			}
		}

		if builder.canApplyRegularIndex(secondScanNode) {
			optimizedSecondScanID := builder.applyIndicesForFilters(secondScanNodeID, secondScanNode, colRefCnt, idxColMap)
			secondScanNodeID = optimizedSecondScanID
			secondScanNode = builder.qry.Nodes[secondScanNodeID]
		}

		// Otherwise BloomFilter will only see the truncated primary key set, causing data loss.
		secondScanNode.Limit = nil
		secondScanNode.Offset = nil

		// Add a PROJECT node above secondScanNode to output only the primary key column
		secondProjectTag := builder.genNewBindTag()
		secondPkExpr := builder.buildPkExprFromNode(secondScanNodeID, ivfCtx.pkType, scanNode.TableDef.Pkey.PkeyColName)
		if secondPkExpr == nil {
			secondPkExpr = &plan.Expr{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: baseSecondScan.BindingTags[0],
						ColPos: ivfCtx.pkPos,
						Name:   scanNode.TableDef.Cols[ivfCtx.pkPos].Name,
					},
				},
			}
		}
		secondProjectNodeID := builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{secondScanNodeID},
			ProjectList: []*plan.Expr{secondPkExpr},
			BindingTags: []int32{secondProjectTag},
		}, ctx)

		// inner join: (ivf_search table function JOIN second table project)
		innerJoinOn, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag,
						ColPos: 0, // tf.pkid
					},
				},
			},
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: secondProjectTag,
						ColPos: 0, // only pk column from second scan
					},
				},
			},
		})

		innerJoinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{tableFuncNodeID, secondProjectNodeID},
			JoinType: plan.Node_INNER,
			OnList:   []*Expr{innerJoinOn},
			// Don't set Limit/Offset on JOIN - they should be applied after SORT
		}, ctx)

		// Construct BloomFilter type runtime filter for inner join + table function
		rfTag := builder.genNewMsgTag()

		// build side: primary key from secondScanNode (consistent with BloomFilter build column)
		buildExpr := &plan.Expr{
			Typ: ivfCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: secondProjectTag,
					ColPos: 0,
				},
			},
		}
		buildSpec := MakeRuntimeFilter(rfTag, false, 0, buildExpr, false)
		buildSpec.UseBloomFilter = true
		innerJoinNode := builder.qry.Nodes[innerJoinNodeID]
		innerJoinNode.RuntimeFilterBuildList = []*plan.RuntimeFilterSpec{buildSpec}

		// probe side: pkid column from table function
		probeExpr := &plan.Expr{
			Typ: ivfCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tableFuncTag,
					ColPos: 0,
				},
			},
		}
		probeSpec := MakeRuntimeFilter(rfTag, false, 0, probeExpr, false)
		probeSpec.UseBloomFilter = true
		tableFuncNode.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{probeSpec}

		// outer join: original table JOIN (inner ivf join)
		outerOn, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanNode.BindingTags[0],
						ColPos: ivfCtx.pkPos, // tbl.pk
					},
				},
			},
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag, // tf pkid from inner join subtree
						ColPos: 0,
					},
				},
			},
		})

		outerJoinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{scanNode.NodeId, innerJoinNodeID},
			JoinType: plan.Node_INNER,
			OnList:   []*Expr{outerOn},
			// Don't set Limit/Offset on JOIN - they should be applied after SORT
		}, ctx)

		// Manually construct a runtime filter for outer join:
		//   - build side: right child inner join (smaller set, contains actual pkid)
		//   - probe side: left child table scan (original table), performs block/row pruning at scan stage.
		// Note:
		//   1) We don't use BloomFilter here, but use the existing IN-list runtime filter pipeline;
		//   2) UpperLimit is set to avoid all filters being degraded to PASS due to 0.
		rfTag2 := builder.genNewMsgTag()

		// probe: primary key column from original table (scanNode left child)
		probeExpr2 := &plan.Expr{
			Typ: ivfCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanNode.BindingTags[0],
					ColPos: ivfCtx.pkPos,
				},
			},
		}
		probeSpec2 := MakeRuntimeFilter(rfTag2, false, 0, DeepCopyExpr(probeExpr2), false)
		scanNode.RuntimeFilterProbeList = append(scanNode.RuntimeFilterProbeList, probeSpec2)

		// build: placeholder column, HashBuild will generate IN-list based on build side join key's UniqueJoinKeys[0]
		buildExpr2 := &plan.Expr{
			Typ: ivfCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: -1,
					ColPos: 0,
				},
			},
		}

		// Set inLimit to "unlimited" to ensure this runtime filter won't be disabled due to upper limit.
		// Use int32 max value directly here.
		const unlimitedInFilterCard = int32(1<<31 - 1)
		buildSpec2 := MakeRuntimeFilter(rfTag2, false, unlimitedInFilterCard, buildExpr2, false)

		outerJoinNode := builder.qry.Nodes[outerJoinNodeID]
		outerJoinNode.RuntimeFilterBuildList = append(outerJoinNode.RuntimeFilterBuildList, buildSpec2)

		// Outer join doesn't add extra project, let global column pruning optimizer handle it
		joinRootID = outerJoinNodeID
	} else {
		// JOIN( table, ivf )
		wherePkEqPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanNode.BindingTags[0],
						ColPos: ivfCtx.pkPos, // tbl.pk
					},
				},
			},
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag,
						ColPos: 0, // tf.pkid
					},
				},
			},
		})

		joinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{scanNode.NodeId, tableFuncNodeID},
			JoinType: plan.Node_INNER,
			OnList:   []*Expr{wherePkEqPk},
			// Don't set Limit/Offset on JOIN - they should be applied after SORT
		}, ctx)

		// In non-nested mode, outer join also doesn't add extra project, let optimizer handle column pruning
		joinRootID = joinNodeID
	}

	// Keep FilterList on scanNode so filters are applied during table scan
	// Clear Limit/Offset from scanNode since they should be applied after SORT
	scanNode.Limit = nil
	scanNode.Offset = nil

	// Create SortBy, still sort directly by table function's score, let remap map ColRef to corresponding output column
	orderByScore := []*OrderBySpec{
		{
			Expr: &plan.Expr{
				Typ: tableFuncNode.TableDef.Cols[1].Typ, // score column
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag,
						ColPos: 1, // score column
					},
				},
			},
			Flag: vecCtx.sortDirection,
		},
	}

	sortByID := builder.appendNode(&plan.Node{
		NodeType:   plan.Node_SORT,
		Children:   []int32{joinRootID},
		OrderBy:    orderByScore,
		Limit:      limit,                         // Apply LIMIT after sorting
		Offset:     DeepCopyExpr(sortNode.Offset), // Apply OFFSET after sorting
		RankOption: DeepCopyRankOption(vecCtx.rankOption),
	}, ctx)

	projNode.Children[0] = sortByID

	if childNode != nil {
		sortIdx := orderExpr.GetCol().ColPos
		projMap := make(map[[2]int32]*plan.Expr)
		for i, proj := range childNode.ProjectList {
			if i == int(sortIdx) {
				projMap[[2]int32{childNode.BindingTags[0], int32(i)}] = DeepCopyExpr(orderByScore[0].Expr)
			} else {
				projMap[[2]int32{childNode.BindingTags[0], int32(i)}] = proj
			}
		}

		replaceColumnsForNode(projNode, projMap)
	}

	return nodeID, nil
}

func (builder *QueryBuilder) buildPkExprFromNode(nodeID int32, pkType plan.Type, pkName string) *plan.Expr {
	if builder == nil || nodeID < 0 {
		return nil
	}
	node := builder.qry.Nodes[nodeID]
	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		if node.TableDef == nil || len(node.BindingTags) == 0 {
			return nil
		}
		colIdx, ok := node.TableDef.Name2ColIndex[pkName]
		if !ok {
			colIdx = node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
		}
		return &plan.Expr{
			Typ: pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: node.BindingTags[0],
					ColPos: colIdx,
					Name:   pkName,
				},
			},
		}
	case plan.Node_PROJECT:
		for _, expr := range node.ProjectList {
			if col := expr.GetCol(); col != nil {
				if builder.getColName(col) == pkName {
					return DeepCopyExpr(expr)
				}
			}
		}
		if len(node.Children) > 0 {
			return builder.buildPkExprFromNode(node.Children[0], pkType, pkName)
		}
	case plan.Node_JOIN:
		if len(node.Children) > 0 {
			return builder.buildPkExprFromNode(node.Children[0], pkType, pkName)
		}
	default:
		if len(node.Children) > 0 {
			return builder.buildPkExprFromNode(node.Children[0], pkType, pkName)
		}
	}
	return nil
}

func (builder *QueryBuilder) getColName(col *plan.ColRef) string {
	if col == nil {
		return ""
	}
	if builder == nil || builder.nameByColRef == nil {
		return col.Name
	}
	if name := builder.nameByColRef[[2]int32{col.RelPos, col.ColPos}]; name != "" {
		return name
	}
	return col.Name
}

func (builder *QueryBuilder) rebindScanNode(scanNode *plan.Node) {
	if scanNode == nil || len(scanNode.BindingTags) == 0 {
		return
	}
	oldTag := scanNode.BindingTags[0]
	newTag := builder.genNewBindTag()
	scanNode.BindingTags[0] = newTag
	builder.addNameByColRef(newTag, scanNode.TableDef)
	for _, expr := range scanNode.FilterList {
		replaceColRefTag(expr, oldTag, newTag)
	}
	// Also update BlockFilterList, which was copied from the original scanNode
	// and still contains references to the old binding tag
	for _, expr := range scanNode.BlockFilterList {
		replaceColRefTag(expr, oldTag, newTag)
	}
}

func replaceColRefTag(expr *plan.Expr, oldTag, newTag int32) {
	if expr == nil {
		return
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if impl.Col.RelPos == oldTag {
			impl.Col.RelPos = newTag
		}
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			replaceColRefTag(arg, oldTag, newTag)
		}
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			replaceColRefTag(sub, oldTag, newTag)
		}
	}
}

func (builder *QueryBuilder) canApplyRegularIndex(node *plan.Node) bool {
	if node == nil || node.TableDef == nil {
		return false
	}
	colCnt := len(node.TableDef.Cols)
	if colCnt == 0 {
		return false
	}
	for _, expr := range node.FilterList {
		if !colRefsWithin(expr, colCnt) {
			return false
		}
	}
	return len(node.FilterList) > 0
}

func colRefsWithin(expr *plan.Expr, colCnt int) bool {
	if expr == nil {
		return true
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return int(impl.Col.ColPos) < colCnt
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			if !colRefsWithin(arg, colCnt) {
				return false
			}
		}
		return true
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			if !colRefsWithin(sub, colCnt) {
				return false
			}
		}
		return true
	default:
		return true
	}
}
