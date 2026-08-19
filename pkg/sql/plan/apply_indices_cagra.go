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

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	cagraplan "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

type cagraIndexContext struct {
	vecCtx       *vectorSortContext
	metaDef      *plan.IndexDef
	idxDef       *plan.IndexDef
	vecLitArg    *plan.Expr
	origFuncName string
	partPos      int32
	partType     plan.Type
	pkPos        int32
	pkType       plan.Type
	params       string
	nThread      int64
	batchWindow  int64
	gpuMultiSim  int64
}

func (builder *QueryBuilder) prepareCagraIndexContext(vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex) (*cagraIndexContext, error) {
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

	metaDef := multiTableIndex.IndexDefs[catalog.Cagra_TblType_Metadata]
	idxDef := multiTableIndex.IndexDefs[catalog.Cagra_TblType_Storage]
	if metaDef == nil || idxDef == nil {
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

	origFuncName := vecCtx.distFnExpr.Func.ObjName
	// An index serves this distance function when its op_type is metric-equivalent to the
	// query's, not only when it is the canonical one — vector_l2_ops and vector_l2sq_ops
	// build the same index and both answer l2_distance / l2_distance_sq (#25966).
	if !metric.OpTypeServesDistFunc(opType, origFuncName) {
		return nil, nil
	}

	keyPart := idxDef.Parts[0]
	partPos := vecCtx.scanNode.TableDef.Name2ColIndex[keyPart]
	partType := vecCtx.scanNode.TableDef.Cols[partPos].Typ
	_, vecLitArg, found := builder.getArgsFromDistFn(vecCtx.distFnExpr, partPos)
	if !found {
		return nil, nil
	}

	pkPos := vecCtx.scanNode.TableDef.Name2ColIndex[vecCtx.scanNode.TableDef.Pkey.PkeyColName]
	pkType := vecCtx.scanNode.TableDef.Cols[pkPos].Typ

	nThread, err := builder.compCtx.ResolveVariable("cagra_threads_search", true, false)
	if err != nil {
		return nil, err
	}

	batchWindow, err := builder.compCtx.ResolveVariable("cagra_batch_window", true, false)
	if err != nil {
		return nil, err
	}

	gpuMultiSim, err := builder.compCtx.ResolveVariable("gpu_multi_simulation", true, false)
	if err != nil {
		return nil, err
	}

	return &cagraIndexContext{
		vecCtx:       vecCtx,
		metaDef:      metaDef,
		idxDef:       idxDef,
		vecLitArg:    vecLitArg,
		origFuncName: origFuncName,
		partPos:      partPos,
		partType:     partType,
		pkPos:        pkPos,
		pkType:       pkType,
		params:       idxDef.IndexAlgoParams,
		nThread:      nThread.(int64),
		batchWindow:  batchWindow.(int64),
		gpuMultiSim:  gpuMultiSim.(int64),
	}, nil
}

func (builder *QueryBuilder) applyIndicesForSortUsingCagra(nodeID int32, vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex, idxColMap map[[2]int32]*plan.Expr) (int32, error) {

	if !hasCompleteVectorPagination(vecCtx) || vecCtx.sortNode == nil || vecCtx.scanNode == nil {
		return nodeID, nil
	}

	ctx := builder.ctxByNode[nodeID]
	projNode := vecCtx.projNode
	scanNode := vecCtx.scanNode
	childNode := vecCtx.childNode
	orderExpr := vecCtx.orderExpr
	limit := vecCtx.limit

	cagraCtx, err := builder.prepareCagraIndexContext(vecCtx, multiTableIndex)
	if err != nil || cagraCtx == nil {
		return nodeID, err
	}

	// Filters that prune candidates after the search require the search to
	// over-fetch (fetch k' > k) so k rows still survive. The over-fetch is done
	// once at EXECUTE by the TVF (flag below), for literal AND parameterized
	// limits alike. Any filter here is a conservative trigger — pushed-down
	// filters merely over-fetch a little, while residual/peeled ones need it.
	postFilterOverFetch := len(scanNode.FilterList) > 0

	// The TVF must NOT over-fetch again at EXECUTE: node.Limit below already carries
	// the over-fetched k', for the parameterized case as much as the literal one, so
	// a second application would compound the factor. The flag stays in the config
	// for the other algorithms and for older plans that still rely on it.
	const runtimeOverFetch = false

	tblCfgStr := fmt.Sprintf(`{"db": "%s", "src": "%s", "metadata":"%s", "index":"%s", "threads_search": %d, "orig_func_name": "%s", "batch_window": %d, "gpu_multi_simulation": %d, "parttype": %d, "post_filter_overfetch": %t}`,
		scanNode.ObjRef.SchemaName,
		scanNode.TableDef.Name,
		cagraCtx.metaDef.IndexTableName,
		cagraCtx.idxDef.IndexTableName,
		cagraCtx.nThread,
		cagraCtx.origFuncName,
		cagraCtx.batchWindow,
		cagraCtx.gpuMultiSim,
		cagraCtx.partType.Id,
		runtimeOverFetch)

	// Predicate pushdown on INCLUDE columns and the primary key: peel
	// filters that reference only INCLUDE columns (or the PK, routed to
	// host_ids via the __mo_pk_host_id virtual column) into a JSON array
	// passed as the cagra_search 3rd arg. Unserializable/mixed predicates
	// stay on the TABLE_SCAN.
	includeCols, err := parseIncludedColumnsFromParams(cagraCtx.idxDef.IndexAlgoParams)
	if err != nil {
		return nodeID, err
	}
	pkColName := ""
	if scanNode.TableDef.Pkey != nil {
		pkColName = scanNode.TableDef.Pkey.PkeyColName
	}
	if len(includeCols) > 0 {
		logutil.Debugf("CAGRA pushdown: INCLUDE columns = %v, scan filters = %d",
			includeCols, len(scanNode.FilterList))
	}
	predsJSON, peeled, residualFilters, err := buildFilterPredicateJSON(
		scanNode.FilterList, scanNode, includeCols, pkColName, false)
	if err != nil {
		return nodeID, err
	}
	if predsJSON != "" {
		logutil.Debugf("CAGRA pushdown: peeled %d filter(s), %d residual, preds_json = %s",
			len(peeled), len(residualFilters), predsJSON)
		scanNode.FilterList = residualFilters
	}

	// JOIN between source table and cagra_search table function
	tableFuncTag := builder.genNewBindTag()
	tableFuncExprs := []*plan.Expr{
		makePlan2StringConstExprWithType(tblCfgStr),
		DeepCopyExpr(cagraCtx.vecLitArg),
	}
	if predsJSON != "" {
		tableFuncExprs = append(tableFuncExprs, makePlan2StringConstExprWithType(predsJSON))
	}
	tableFuncNode := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name:  cagraplan.CAGRASearchFuncName,
				Param: []byte(cagraCtx.params),
			},
			Cols: DeepCopyColDefList(cagraplan.CAGRASearchColDefs),
		},
		BindingTags:     []int32{tableFuncTag},
		TblFuncExprList: tableFuncExprs,
	}
	tableFuncNodeID := builder.appendNode(tableFuncNode, ctx)

	err = builder.addBinding(tableFuncNodeID, tree.AliasClause{Alias: tree.Identifier("mo_cagra_alias_0")}, ctx)
	if err != nil {
		return 0, err
	}

	// Peel `distfn(col, vec) <op> K` predicates off the scan FilterList and
	// re-attach them — rewritten to reference the table function's score
	// column — on tableFuncNode.FilterList. Node_FUNCTION_SCAN applies them
	// via compileRestrict (compile.go:1351), so the base table scan no longer
	// recomputes the distance kernel brute-force after the JOIN.
	scoreColType := tableFuncNode.TableDef.Cols[1].Typ
	newScanFilters, peeledDistFilters := builder.peelAndRewriteDistFnFilters(
		scanNode.FilterList, cagraCtx.partPos, cagraCtx.origFuncName,
		cagraCtx.vecLitArg, tableFuncTag, scoreColType)
	scanNode.FilterList = newScanFilters
	if len(peeledDistFilters) > 0 {
		logutil.Debugf("CAGRA pushdown: peeled %d distance predicate(s) onto table function FilterList",
			len(peeledDistFilters))
		tableFuncNode.FilterList = append(tableFuncNode.FilterList, peeledDistFilters...)
	}

	// Rewrite any SELECT-side `origFuncName(ec, vec)` calls in the surrounding
	// projections to reference the table function's score column directly, so
	// the user's `... AS dist` does not re-run the distance kernel on every
	// scanned row.
	{
		scanTag := scanNode.BindingTags[0]
		if projNode != nil {
			replaceDistFnExprsWithScoreCol(projNode.ProjectList, scanTag,
				cagraCtx.partPos, cagraCtx.origFuncName, cagraCtx.vecLitArg,
				tableFuncTag, scoreColType)
		}
		if childNode != nil {
			replaceDistFnExprsWithScoreCol(childNode.ProjectList, scanTag,
				cagraCtx.partPos, cagraCtx.origFuncName, cagraCtx.vecLitArg,
				tableFuncTag, scoreColType)
		}
	}

	// The raw candidate limit (k) is carried on IndexReaderParam.Limit; the TVF
	// takes its budget from there and over-fetches k -> k' at EXECUTE when a
	// residual filter or a peeled distance-range bound prunes candidates
	// (post_filter_overfetch flag).
	tableFuncNode.IndexReaderParam = &plan.IndexReaderParam{
		Limit:          DeepCopyExpr(limit),
		OrigFuncName:   cagraCtx.origFuncName,
		OverFetchLimit: overFetchDisplayLimit(limit, postFilterOverFetch, false),
	}
	// node.Limit carries the OVER-FETCHED budget k', never the raw k. The #26869
	// bug was a plan-level top truncating candidates to k before the post-filter
	// JOIN; truncating at k' instead is exactly the budget the search wants, so the
	// fix holds while node.Limit stays the channel an old CN can still read.
	//
	// This matters because a vector provider child gives the FUNCTION_SCAN a child,
	// so compileTableFunction attaches the search operator to already-compiled child
	// scopes that may be Remote: during a rolling upgrade a new coordinator can ship
	// it to a pre-change CN, whose Prepare reads arg.Limit alone and would default to
	// one candidate on a nil. Because k' is computed by an expression built from
	// functions that long predate any CN we can be mixed with, this needs no protocol
	// capability and no low-version fallback. See BuildOverFetchLimitExpr.
	overFetchLimit, err := BuildOverFetchLimitExpr(builder.GetContext(), limit, false)
	if err != nil {
		return 0, err
	}
	if postFilterOverFetch {
		tableFuncNode.Limit = overFetchLimit
	} else {
		tableFuncNode.Limit = DeepCopyExpr(limit)
	}

	// oncond
	wherePkEqPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		{
			Typ: cagraCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanNode.BindingTags[0],
					ColPos: cagraCtx.pkPos, // tbl.pk
				},
			},
		},
		{
			Typ: cagraCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tableFuncTag, // last idxTbl (may be join) relPos
					ColPos: 0,            // idxTbl.pk
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
	}, ctx)

	// Anchored at the PROJECT above the Top-K, or at the Top-K sort itself when a
	// consumer (outer ORDER BY, join) sits between it and any project.
	remap := vectorRemapForChildProject(childNode, orderExpr, orderByScore[0].Expr, nil)
	return builder.spliceVectorRewrite(vecCtx, nodeID, sortByID, remap, idxColMap), nil
}

/*
func (builder *QueryBuilder) getArgsFromDistFn(distFnExpr *plan.Function, partPos int32) (key *plan.Expr, value *plan.Expr, found bool) {

	if _, ok := metric.DistFuncOpTypes[distFnExpr.Func.ObjName]; !ok {
		return
	}

	distFnArgs := distFnExpr.Args
	if distFnArgs[0].Typ.GetId() != int32(types.T_array_float32) && distFnArgs[0].Typ.GetId() != int32(types.T_array_float64) {
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
	if !rule.IsConstant(vecLitArg, true) {
		return
	}

	vecLitArg.Typ = vecColArg.Typ

	if vecColArg.GetCol().ColPos != partPos {
		return
	}

	return vecColArg, vecLitArg, true
}
*/
