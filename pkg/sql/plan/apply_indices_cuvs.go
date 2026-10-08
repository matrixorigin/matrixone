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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// cuvsSearchScan is the per-algorithm part of a cagra or ivfpq sort rewrite.
type cuvsSearchScan struct {
	algo         string
	alias        string
	metaDef      *plan.IndexDef
	idxDef       *plan.IndexDef
	metaRole     string
	storageRole  string
	cols         []*plan.ColDef
	vecLitArg    *plan.Expr
	origFuncName string
	partPos      int32
	pkPos        int32
	pkType       plan.Type
	// algoOptions returns the algo_options bytes carrying filterJSON.
	algoOptions func(filterJSON string) ([]byte, error)
}

// applyCuvsIndexSearchScan rewrites the vector sort of vecCtx into
// SORT(JOIN(scan, INDEX_SEARCH_SCAN)) on pk, ordered by the search score.
func (builder *QueryBuilder) applyCuvsIndexSearchScan(nodeID int32, vecCtx *vectorSortContext, idxColMap map[[2]int32]*plan.Expr, s cuvsSearchScan) (int32, error) {
	ctx := builder.ctxByNode[nodeID]
	projNode := vecCtx.projNode
	scanNode := vecCtx.scanNode
	childNode := vecCtx.childNode

	// Any scan filter prunes candidates after the search, so execution
	// over-fetches the candidate budget from k (#26869).
	postFilterOverFetch := len(scanNode.FilterList) > 0

	// Filters that reference only INCLUDE columns or the primary key are
	// serialized to JSON and applied by the search on the device; the rest
	// stay on the scan.
	includeCols, err := parseIncludedColumnsFromParams(s.idxDef.IndexAlgoParams)
	if err != nil {
		return nodeID, err
	}
	pkColName := ""
	if scanNode.TableDef.Pkey != nil {
		pkColName = scanNode.TableDef.Pkey.PkeyColName
	}
	predsJSON, peeled, residualFilters, err := buildFilterPredicateJSON(
		scanNode.FilterList, scanNode, includeCols, pkColName, false)
	if err != nil {
		return nodeID, err
	}
	if predsJSON != "" {
		logutil.Debugf("%s pushdown: peeled %d filter(s), %d residual, preds_json = %s",
			s.algo, len(peeled), len(residualFilters), predsJSON)
		scanNode.FilterList = residualFilters
	}

	algoOptions, err := s.algoOptions(predsJSON)
	if err != nil {
		return 0, err
	}

	searchTag := builder.genNewBindTag()
	searchNode := &plan.Node{
		NodeType: plan.Node_INDEX_SEARCH_SCAN,
		// The search reads the whole cached index; it is not partitioned, so it
		// runs in one local scope (IndexSearchScanPartitioned).
		Stats:  &plan.Stats{},
		ObjRef: DeepCopyObjectRef(scanNode.ObjRef),
		TableDef: &plan.TableDef{
			Name:      scanNode.TableDef.Name,
			TableType: "vector_index_scan",
			Cols:      DeepCopyColDefList(s.cols),
		},
		BindingTags: []int32{searchTag},
		// Named-snapshot read TS; DeepCopySnapshot(nil) is nil (#27927).
		ScanSnapshot: DeepCopySnapshot(scanNode.ScanSnapshot),
		IndexSearchScan: &plan.IndexSearchScan{
			SourceTable:         DeepCopyObjectRef(scanNode.ObjRef),
			SourceTableDef:      DeepCopyTableDef(scanNode.TableDef, true),
			ScanSnapshot:        DeepCopySnapshot(scanNode.ScanSnapshot),
			Index:               DeepCopyIndexDef(s.idxDef),
			QueryPayload:        DeepCopyExpr(s.vecLitArg),
			DistanceFunction:    s.origFuncName,
			Direction:           vecCtx.sortDirection,
			CandidateLimit:      DeepCopyExpr(vecCtx.limit),
			PostFilterOverFetch: postFilterOverFetch,
			AlgoOptions:         algoOptions,
			HiddenTables: []*plan.IndexHiddenTableRef{
				{Role: s.metaRole, Object: &plan.ObjectRef{SchemaName: scanNode.ObjRef.SchemaName, ObjName: s.metaDef.IndexTableName}},
				{Role: s.storageRole, Object: &plan.ObjectRef{SchemaName: scanNode.ObjRef.SchemaName, ObjName: s.idxDef.IndexTableName}},
			},
		},
	}
	searchNodeID := builder.appendNode(searchNode, ctx)

	err = builder.addBinding(searchNodeID, tree.AliasClause{Alias: tree.Identifier(s.alias)}, ctx)
	if err != nil {
		return 0, err
	}

	// `distfn(col, vec) <op> K` predicates move from the scan to the search
	// node, rewritten to its score column.
	scoreColType := searchNode.TableDef.Cols[1].Typ
	newScanFilters, peeledDistFilters := builder.peelAndRewriteDistFnFilters(
		scanNode.FilterList, s.partPos, s.origFuncName,
		s.vecLitArg, searchTag, scoreColType)
	scanNode.FilterList = newScanFilters
	if len(peeledDistFilters) > 0 {
		logutil.Debugf("%s pushdown: peeled %d distance predicate(s) onto the index search scan",
			s.algo, len(peeledDistFilters))
		searchNode.FilterList = append(searchNode.FilterList, peeledDistFilters...)
	}

	// SELECT-side `origFuncName(col, vec)` calls read the score column.
	scanTag := scanNode.BindingTags[0]
	if projNode != nil {
		replaceDistFnExprsWithScoreCol(projNode.ProjectList, scanTag,
			s.partPos, s.origFuncName, s.vecLitArg, searchTag, scoreColType)
	}
	if childNode != nil {
		replaceDistFnExprsWithScoreCol(childNode.ProjectList, scanTag,
			s.partPos, s.origFuncName, s.vecLitArg, searchTag, scoreColType)
	}

	wherePkEqPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		{
			Typ: s.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanTag,
					ColPos: s.pkPos,
				},
			},
		},
		{
			Typ: s.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: searchTag,
					ColPos: 0,
				},
			},
		},
	})

	joinNodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{scanNode.NodeId, searchNodeID},
		JoinType: plan.Node_INNER,
		OnList:   []*Expr{wherePkEqPk},
	}, ctx)

	// Limit/Offset apply after the SORT.
	scanNode.Limit = nil
	scanNode.Offset = nil

	orderByScore := []*OrderBySpec{
		{
			Expr: &Expr{
				Typ: scoreColType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: searchTag,
						ColPos: 1,
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
	remap := vectorRemapForChildProject(childNode, vecCtx.orderExpr, orderByScore[0].Expr, nil)
	return builder.spliceVectorRewrite(vecCtx, nodeID, sortByID, remap, idxColMap), nil
}
