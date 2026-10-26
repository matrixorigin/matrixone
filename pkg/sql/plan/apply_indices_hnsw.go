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
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	indexcoverage "github.com/matrixorigin/matrixone/pkg/indexplugin/coverage"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	hnswplan "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type hnswIndexContext struct {
	vecCtx       *vectorSortContext
	metaDef      *plan.IndexDef
	idxDef       *plan.IndexDef
	vecLitArg    *plan.Expr
	origFuncName string
	partPos      int32
	pkPos        int32
	pkType       plan.Type
	params       string
	nThread      int64
}

// hnswIndexCoversSnapshot is the correctness gate for the HNSW top-k rewrite.
// The rewrite is an inner join between ANN candidates and the base table; an
// async generation that is behind the source can therefore silently lose rows.
// A false result deliberately leaves the original exact scan in place.
func (builder *QueryBuilder) hnswIndexCoversSnapshot(scanNode *plan.Node, metaDef, idxDef *plan.IndexDef) bool {
	if builder == nil || builder.compCtx == nil || scanNode == nil ||
		scanNode.TableDef == nil || idxDef == nil {
		return false
	}
	if builder.GetContext().Value(defines.HnswSnapshotFallbackKey{}) == true {
		return false
	}
	proc := builder.compCtx.GetProcess()
	if proc == nil {
		return false
	}
	txn := proc.GetTxnOperator()
	if txn == nil {
		// Synthetic planner contexts used by unit tests do not have a transaction
		// and cannot execute a query, so retain the existing plan-construction
		// behavior for those contexts only.
		return true
	}
	if scanNode.TableDef.TblId == 0 || scanNode.ObjRef == nil ||
		proc.GetSessionInfo() == nil || proc.GetSessionInfo().StorageEngine == nil {
		return false
	}

	sp := sqlexec.NewSqlProcess(proc)
	scanSnapshotTS := sp.ApplyScanSnapshot(scanNode.ScanSnapshot)
	readTxn := txn
	if scanSnapshotTS != nil {
		readTxn = txn.CloneSnapshotOp(*scanSnapshotTS)
	}

	ctx := proc.GetTopContext()
	if acctID, err := sp.EffectiveAccountID(); err == nil {
		ctx = defines.AttachAccountId(ctx, acctID)
	}
	_, _, rel, err := proc.GetSessionInfo().StorageEngine.GetRelationById(
		ctx, readTxn, scanNode.TableDef.TblId)
	if err != nil {
		logutil.Debugf("hnsw index coverage: resolve source relation failed for %s: %v", idxDef.IndexName, err)
		return false
	}
	commitTSProvider, ok := rel.(engine.SourceCommitTSProvider)
	if !ok {
		return false
	}

	readTS := types.TimestampToTS(txn.SnapshotTS())
	if scanSnapshotTS != nil {
		readTS = types.TimestampToTS(*scanSnapshotTS)
	}
	req := indexcoverage.Request{
		CNUUID:             proc.GetService(),
		Txn:                txn,
		TableID:            scanNode.TableDef.TblId,
		IndexDef:           metaDef,
		Snapshot:           readTS,
		IndexStorageTable:  idxDef.IndexTableName,
		IndexMetadataDB:    scanNode.ObjRef.SchemaName,
		IndexMetadataTable: metaDef.IndexTableName,
		ScanSnapshotTS:     scanSnapshotTS,
		SourceCommitTS: func(ctx context.Context, mustExceed types.TS) (types.TS, error) {
			return commitTSProvider.SourceCommitTS(ctx, mustExceed)
		},
	}
	covered, _, err := indexplugin.CoversSnapshot(ctx, idxDef.IndexAlgo, req)
	if err != nil {
		logutil.Debugf("hnsw index coverage check failed for %s: %v", idxDef.IndexName, err)
	}
	return covered
}
func (builder *QueryBuilder) prepareHnswIndexContext(vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex) (*hnswIndexContext, error) {
	if vecCtx == nil || multiTableIndex == nil {
		return nil, nil
	}
	if vecCtx.distFnExpr == nil {
		return nil, nil
	}

	// RankOption.Mode controls vector index behavior:
	// - "force": Disable vector index, force full table scan (for debugging/comparison)
	// - nil/other: Enable vector index with default behavior
	if vecCtx.rankOption != nil && vecCtx.rankOption.Mode == "force" {
		return nil, nil
	}
	rewriteAllowed, err := builder.validateVectorIndexSortRewrite(vecCtx)
	if err != nil || !rewriteAllowed {
		return nil, err
	}

	metaDef := multiTableIndex.IndexDefs[catalog.Hnsw_TblType_Metadata]
	idxDef := multiTableIndex.IndexDefs[catalog.Hnsw_TblType_Storage]
	if metaDef == nil || idxDef == nil {
		return nil, nil
	}
	if !builder.hnswIndexCoversSnapshot(vecCtx.scanNode, metaDef, idxDef) {
		return nil, nil
	}

	params, err := decodeVectorIndexAlgoParams(metaDef.IndexAlgoParams)
	if err != nil {
		return nil, nil
	}
	opType, ok := vectorIndexStringParam(params, catalog.IndexAlgoParamOpType)
	if !ok {
		return nil, nil
	}

	origFuncName := vecCtx.distFnExpr.Func.ObjName
	// An index serves this distance function when its op_type is metric-equivalent to the
	// query's, not only when it is the canonical one — vector_l2_ops and vector_l2sq_ops
	// build the same index and both answer l2_distance / l2_distance_sq (#25966).
	if !metric.OpTypeServesDistFunc(opType, origFuncName) {
		return nil, nil
	}

	if len(idxDef.Parts) == 0 {
		return nil, nil
	}
	keyPart := idxDef.Parts[0]
	partPos := vecCtx.scanNode.TableDef.Name2ColIndex[keyPart]
	var vecLitArg *plan.Expr
	var found bool
	if vecCtx.vecArgExpr != nil {
		_, vecLitArg, found = builder.getArgsFromDistFnForJoin(
			vecCtx.distFnExpr,
			partPos,
			vecCtx.scanNode.BindingTags[0],
		)
	} else {
		_, vecLitArg, found = builder.getArgsFromDistFn(vecCtx.distFnExpr, partPos)
	}
	if !found {
		return nil, nil
	}

	pkPos := vecCtx.scanNode.TableDef.Name2ColIndex[vecCtx.scanNode.TableDef.Pkey.PkeyColName]
	pkType := vecCtx.scanNode.TableDef.Cols[pkPos].Typ

	nThread, err := builder.resolveInt64Variable("hnsw_threads_search", 0)
	if err != nil {
		return nil, err
	}

	// Checked once the index is known to serve the query: another index on the table may.
	if err := checkVectorSearchMode(builder.GetContext(), vecCtx, catalog.MoIndexHnswAlgo.ToString()); err != nil {
		return nil, err
	}
	return &hnswIndexContext{
		vecCtx:       vecCtx,
		metaDef:      metaDef,
		idxDef:       idxDef,
		vecLitArg:    vecLitArg,
		origFuncName: origFuncName,
		partPos:      partPos,
		pkPos:        pkPos,
		pkType:       pkType,
		params:       idxDef.IndexAlgoParams,
		nThread:      nThread,
	}, nil
}

// resolveInt64Variable reads an int64 system variable. Some execution
// contexts (stored procedures, Starlark mo.sql) resolve variables through a
// background session that can return nil for a variable it does not carry,
// so a nil value falls back to the variable's default instead of panicking
// on a type assertion.
func (builder *QueryBuilder) resolveInt64Variable(name string, def int64) (int64, error) {
	value, err := builder.compCtx.ResolveVariable(name, true, false)
	if err != nil {
		return 0, err
	}
	if value == nil {
		return def, nil
	}
	v, ok := value.(int64)
	if !ok {
		return 0, moerr.NewInternalErrorNoCtxf("ResolveVariable: %s is not int64 but %T", name, value)
	}
	return v, nil
}

func (builder *QueryBuilder) applyIndicesForSortUsingHnsw(nodeID int32, vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex, idxColMap map[[2]int32]*plan.Expr) (int32, error) {
	return builder.applyIndicesForSortUsingHnswWithColRefCnt(nodeID, vecCtx, multiTableIndex, nil, idxColMap)
}

// hnswUsesPreFilter reports whether the hnsw search of vecCtx is restricted to the keys
// of a filtered copy of its scan: under a membership join, a fulltext MATCH filter, or
// mode=pre or mode=iterative with a filter.
func (builder *QueryBuilder) hnswUsesPreFilter(vecCtx *vectorSortContext) bool {
	if vecCtx.hasMembership || builder.scanHasMatchedFullTextFilter(vecCtx.scanNode) {
		return true
	}
	mode := vectorRankMode(vecCtx)
	return (mode == "pre" || mode == "iterative") && len(vecCtx.scanNode.FilterList) > 0
}

// buildAdaptiveHnswTop builds mode=auto as ADAPTIVE_TOP [post, pre, force]. The pre
// branch is left out when it would equal post: without a filter, or with a fulltext
// MATCH filter, under which post already searches the matching keys. A region that is
// small, not replay-safe or not supported by ADAPTIVE_TOP runs force.
func (builder *QueryBuilder) buildAdaptiveHnswTop(nodeID int32, vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {
	branch := func(root int32, branchCtx *vectorSortContext, branchMap map[[2]int32]*plan.Expr) (int32, error) {
		return builder.applyIndicesForSortUsingHnswWithColRefCnt(root, branchCtx, multiTableIndex, colRefCnt, branchMap)
	}
	withPre := len(vecCtx.scanNode.FilterList) > 0 && !builder.scanHasMatchedFullTextFilter(vecCtx.scanNode)
	return builder.buildAdaptiveModeTop(nodeID, vecCtx, idxColMap, branch, withPre)
}

func (builder *QueryBuilder) applyIndicesForSortUsingHnswWithColRefCnt(nodeID int32, vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {

	if !hasCompleteVectorPagination(vecCtx) || vecCtx.sortNode == nil || vecCtx.scanNode == nil {
		return nodeID, nil
	}

	ctx := builder.ctxByNode[nodeID]
	scanNode := vecCtx.scanNode
	childNode := vecCtx.childNode
	orderExpr := vecCtx.orderExpr
	limit := vecCtx.limit

	hnswCtx, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	if err != nil || hnswCtx == nil {
		return nodeID, err
	}
	if vectorRankMode(vecCtx) == "auto" {
		return builder.buildAdaptiveHnswTop(nodeID, vecCtx, multiTableIndex, colRefCnt, idxColMap)
	}

	// With a residual filter the hnsw search is JOIN-ed against the scan and the
	// filter drops index candidates, so execution over-fetches the candidate
	// budget from k (#26869). A PRE search returns only qualifying keys.
	usePreFilter := builder.hnswUsesPreFilter(vecCtx)
	if usePreFilter && (vecCtx.providerNodeID >= 0 || !builder.vectorPreFilterReplaySafe(vecCtx)) {
		// A per-row query vector runs the search in APPLY, which receives no membership
		// runtime filter, and a sequence or volatile filter cannot run in both PRE scans:
		// leave the query to the exact sort.
		return nodeID, nil
	}
	postFilterOverFetch := len(scanNode.FilterList) > 0 && !usePreFilter

	var exactThreshold int64
	if usePreFilter {
		if exactThreshold, err = builder.resolveInt64Variable("hnsw_prefilter_exact_threshold", 100000); err != nil {
			return 0, err
		}
	}
	algoOptions, err := hnswplan.EncodeScanOptions(hnswplan.ScanOptions{
		ThreadsSearch:           hnswCtx.nThread,
		Iterative:               usePreFilter && vectorRankMode(vecCtx) == "iterative",
		PrefilterExactThreshold: exactThreshold,
	})
	if err != nil {
		return 0, err
	}

	// JOIN between source table and the hnsw index search
	tableFuncTag := builder.genNewBindTag()
	tableFuncNode := &plan.Node{
		NodeType: plan.Node_INDEX_SEARCH_SCAN,
		// The search reads the whole cached index; it is not partitioned, so it
		// runs in one local scope (IndexSearchScanPartitioned).
		Stats:  &plan.Stats{},
		ObjRef: DeepCopyObjectRef(scanNode.ObjRef),
		TableDef: &plan.TableDef{
			Name:      scanNode.TableDef.Name,
			TableType: "vector_index_scan",
			Cols:      DeepCopyColDefList(hnswplan.HNSWSearchColDefs),
		},
		BindingTags: []int32{tableFuncTag},
		Children:    vectorSearchProviderChildren(vecCtx),
		// Named-snapshot read TS; DeepCopySnapshot(nil) is nil (#27927).
		ScanSnapshot: DeepCopySnapshot(scanNode.ScanSnapshot),
		IndexSearchScan: &plan.IndexSearchScan{
			SourceTable:         DeepCopyObjectRef(scanNode.ObjRef),
			SourceTableDef:      DeepCopyTableDef(scanNode.TableDef, true),
			ScanSnapshot:        DeepCopySnapshot(scanNode.ScanSnapshot),
			Index:               DeepCopyIndexDef(hnswCtx.idxDef),
			QueryPayload:        DeepCopyExpr(hnswCtx.vecLitArg),
			DistanceFunction:    hnswCtx.origFuncName,
			Direction:           vecCtx.sortDirection,
			CandidateLimit:      DeepCopyExpr(limit),
			PostFilterOverFetch: postFilterOverFetch,
			AlgoOptions:         algoOptions,
			HiddenTables: []*plan.IndexHiddenTableRef{
				{Role: catalog.Hnsw_TblType_Metadata, Object: &plan.ObjectRef{SchemaName: scanNode.ObjRef.SchemaName, ObjName: hnswCtx.metaDef.IndexTableName}},
				{Role: catalog.Hnsw_TblType_Storage, Object: &plan.ObjectRef{SchemaName: scanNode.ObjRef.SchemaName, ObjName: hnswCtx.idxDef.IndexTableName}},
			},
		},
	}
	setIndexSearchScanKnobs(tableFuncNode.IndexSearchScan, vecCtx)
	tableFuncNodeID := builder.appendNode(tableFuncNode, ctx)

	err = builder.addBinding(tableFuncNodeID, tree.AliasClause{Alias: tree.Identifier(indexSearchAlias(ctx, "mo_hnsw_alias"))}, ctx, "")
	if err != nil {
		return 0, err
	}
	// A provider row supplies the query vector: run the search per row.
	candidateNodeID := tableFuncNodeID
	if len(tableFuncNode.Children) == 1 {
		providerNodeID := tableFuncNode.Children[0]
		tableFuncNode.Children = nil
		candidateNodeID = builder.appendNode(&plan.Node{
			NodeType:  plan.Node_APPLY,
			Children:  []int32{providerNodeID, tableFuncNodeID},
			ApplyType: plan.Node_CROSSAPPLY,
			Stats:     DeepCopyStats(tableFuncNode.Stats),
		}, ctx)
	}

	var joinNodeID int32
	if usePreFilter {
		preJoinID, ok, err := builder.buildVectorPreFilterJoin(vecCtx, vectorPreFilterInput{
			ctx:             ctx,
			scanNode:        scanNode,
			colRefCnt:       colRefCnt,
			idxColMap:       idxColMap,
			pkPos:           hnswCtx.pkPos,
			pkType:          hnswCtx.pkType,
			tableFuncTag:    tableFuncTag,
			tableFuncNodeID: tableFuncNodeID,
			candidateNodeID: candidateNodeID,
		})
		if err != nil || !ok {
			return nodeID, err
		}
		joinNodeID = preJoinID
	} else {
		// oncond
		wherePkEqPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
			{
				Typ: hnswCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanNode.BindingTags[0],
						ColPos: hnswCtx.pkPos, // tbl.pk
					},
				},
			},
			{
				Typ: hnswCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag, // last idxTbl (may be join) relPos
						ColPos: 0,            // idxTbl.pk
					},
				},
			},
		})

		joinNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{scanNode.NodeId, candidateNodeID},
			JoinType: plan.Node_INNER,
			OnList:   []*Expr{wherePkEqPk},
			// Don't set Limit/Offset on JOIN - they should be applied after SORT
		}, ctx)
	}

	// Keep FilterList on scanNode so filters are applied during table scan
	// Clear Limit/Offset from scanNode since they should be applied after SORT
	scanNode.Limit = nil
	scanNode.Offset = nil

	// Create SortBy with distance column from table function
	orderByScore := []*OrderBySpec{
		{
			Expr: &Expr{
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
	resultLimit, resultOffset := vectorResultPagination(vecCtx)

	sortByID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{joinNodeID},
		OrderBy:  orderByScore,
		Limit:    resultLimit,
		Offset:   resultOffset,
		SpillMem: builder.sortSpillMem,
	}, ctx)

	// Anchored at the PROJECT above the Top-K, or at the Top-K sort itself when a
	// consumer (outer ORDER BY, join) sits between it and any project.
	remap := vectorRemapForChildProject(childNode, orderExpr, orderByScore[0].Expr, nil)
	return builder.spliceVectorRewrite(vecCtx, nodeID, sortByID, remap, idxColMap), nil
}

func (builder *QueryBuilder) getArgsFromDistFn(distFnExpr *plan.Function, partPos int32) (key *plan.Expr, value *plan.Expr, found bool) {

	if _, ok := metric.DistFuncOpTypes[distFnExpr.Func.ObjName]; !ok {
		return
	}

	distFnArgs := distFnExpr.Args
	// Accept any vector element type (f32/f64 and the narrow bf16/f16/int8/uint8),
	// so a direct ivf index on a narrow-base column also pushes down rather than
	// brute-forcing.
	if !types.T(distFnArgs[0].Typ.GetId()).IsArrayRelate() {
		return
	}

	if distFnArgs[1].GetCol() != nil {
		if distFnArgs[0].GetCol() != nil {
			return
		}

		distFnArgs[0], distFnArgs[1] = distFnArgs[1], distFnArgs[0]
	}

	vecColArg, _ := ConstantFold(batch.EmptyForConstFoldBatch, distFnArgs[0], builder.compCtx.GetProcess(), false, true)
	if vecColArg != nil {
		distFnArgs[0] = vecColArg
	}
	vecLitArg, _ := ConstantFold(batch.EmptyForConstFoldBatch, distFnArgs[1], builder.compCtx.GetProcess(), false, true)
	if vecLitArg != nil {
		distFnArgs[1] = vecLitArg
	}

	if vecColArg.GetCol() == nil {
		return
	}
	if !function.IsConstant(vecLitArg, true) {
		return
	}

	vecLitArg.Typ = vecColArg.Typ

	if vecColArg.GetCol().ColPos != partPos {
		return
	}

	return vecColArg, vecLitArg, true
}

func (builder *QueryBuilder) getArgsFromDistFnForJoin(
	distFnExpr *plan.Function,
	partPos int32,
	scanTag int32,
) (key *plan.Expr, value *plan.Expr, found bool) {
	if _, ok := metric.DistFuncOpTypes[distFnExpr.Func.ObjName]; !ok {
		return
	}

	distFnArgs := distFnExpr.Args
	// Accept any vector element type (f32/f64 and the narrow bf16/f16/int8/uint8),
	// mirroring the direct-match getArgsFromDistFn, so IVFFLAT/HNSW vector JOINs on a
	// narrow-base column also push down rather than brute-forcing.
	if !types.T(distFnArgs[0].Typ.GetId()).IsArrayRelate() {
		return
	}

	if col := distFnArgs[0].GetCol(); col != nil && col.RelPos == scanTag && col.ColPos == partPos {
		distFnArgs[1].Typ = distFnArgs[0].Typ
		return distFnArgs[0], distFnArgs[1], true
	}
	if col := distFnArgs[1].GetCol(); col != nil && col.RelPos == scanTag && col.ColPos == partPos {
		distFnArgs[0].Typ = distFnArgs[1].Typ
		return distFnArgs[1], distFnArgs[0], true
	}
	return
}
