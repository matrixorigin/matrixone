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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

type ivfpqIndexContext struct {
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
	batchWindow  int64
	nProbe       int64
}

func (builder *QueryBuilder) prepareIvfpqIndexContext(vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex) (*ivfpqIndexContext, error) {
	if vecCtx == nil || multiTableIndex == nil {
		return nil, nil
	}
	if vecCtx.distFnExpr == nil {
		return nil, nil
	}

	if vecCtx.rankOption != nil && vecCtx.rankOption.Mode == "force" {
		return nil, nil
	}

	rewriteAllowed, err := builder.validateVectorIndexSortRewrite(vecCtx)
	if err != nil || !rewriteAllowed {
		return nil, err
	}

	metaDef := multiTableIndex.IndexDefs[catalog.Ivfpq_TblType_Metadata]
	idxDef := multiTableIndex.IndexDefs[catalog.Ivfpq_TblType_Storage]
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
	if opType != metric.DistFuncOpTypes[origFuncName] {
		return nil, nil
	}

	keyPart := idxDef.Parts[0]
	partPos := vecCtx.scanNode.TableDef.Name2ColIndex[keyPart]
	_, vecLitArg, found := builder.getArgsFromDistFn(vecCtx.distFnExpr, partPos)
	if !found {
		return nil, nil
	}

	pkPos := vecCtx.scanNode.TableDef.Name2ColIndex[vecCtx.scanNode.TableDef.Pkey.PkeyColName]
	pkType := vecCtx.scanNode.TableDef.Cols[pkPos].Typ

	nThread, err := builder.compCtx.ResolveVariable("ivfpq_threads_search", true, false)
	if err != nil {
		return nil, err
	}

	batchWindow, err := builder.compCtx.ResolveVariable("ivfpq_batch_window", true, false)
	if err != nil {
		return nil, err
	}

	nProbe := int64(20)
	if nProbeIf, err2 := builder.compCtx.ResolveVariable("probe_limit", true, false); err2 != nil {
		return nil, err2
	} else if nProbeIf != nil {
		nProbe = nProbeIf.(int64)
	}

	return &ivfpqIndexContext{
		vecCtx:       vecCtx,
		metaDef:      metaDef,
		idxDef:       idxDef,
		vecLitArg:    vecLitArg,
		origFuncName: origFuncName,
		partPos:      partPos,
		pkPos:        pkPos,
		pkType:       pkType,
		params:       idxDef.IndexAlgoParams,
		nThread:      nThread.(int64),
		batchWindow:  batchWindow.(int64),
		nProbe:       nProbe,
	}, nil
}

func (builder *QueryBuilder) applyIndicesForSortUsingIvfpq(nodeID int32, vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex) (int32, error) {

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

	ivfpqCtx, err := builder.prepareIvfpqIndexContext(vecCtx, multiTableIndex)
	if err != nil || ivfpqCtx == nil {
		return nodeID, err
	}

	tblCfgStr := fmt.Sprintf(`{"db": "%s", "src": "%s", "metadata":"%s", "index":"%s", "threads_search": %d, "orig_func_name": "%s", "batch_window": %d, "nprobe": %d}`,
		scanNode.ObjRef.SchemaName,
		scanNode.TableDef.Name,
		ivfpqCtx.metaDef.IndexTableName,
		ivfpqCtx.idxDef.IndexTableName,
		ivfpqCtx.nThread,
		ivfpqCtx.origFuncName,
		ivfpqCtx.batchWindow,
		ivfpqCtx.nProbe)

	// Predicate pushdown on INCLUDE columns: peel filters that reference
	// only INCLUDE columns into a JSON array passed as the ivfpq_search
	// 3rd arg. Unserializable/mixed predicates stay on the TABLE_SCAN.
	includeCols, err := parseIncludedColumnsFromParams(ivfpqCtx.idxDef.IndexAlgoParams)
	if err != nil {
		return nodeID, err
	}
	if len(includeCols) > 0 {
		logutil.Debugf("IVFPQ pushdown: INCLUDE columns = %v, scan filters = %d",
			includeCols, len(scanNode.FilterList))
	}
	predsJSON, peeled, residualFilters, err := buildFilterPredicateJSON(
		scanNode.FilterList, scanNode, includeCols)
	if err != nil {
		return nodeID, err
	}
	if predsJSON != "" {
		logutil.Debugf("IVFPQ pushdown: peeled %d filter(s), %d residual, preds_json = %s",
			len(peeled), len(residualFilters), predsJSON)
		scanNode.FilterList = residualFilters
	}

	// JOIN between source table and ivfpq_search table function
	tableFuncTag := builder.genNewBindTag()
	tableFuncExprs := []*plan.Expr{
		{
			Typ: plan.Type{
				Id: int32(types.T_varchar),
			},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_Sval{
						Sval: tblCfgStr,
					},
				},
			},
		},
		DeepCopyExpr(ivfpqCtx.vecLitArg),
	}
	if predsJSON != "" {
		tableFuncExprs = append(tableFuncExprs, makePlan2StringConstExprWithType(predsJSON))
	}
	tableFuncNode := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name:  kIVFPQSearchFuncName,
				Param: []byte(ivfpqCtx.params),
			},
			Cols: DeepCopyColDefList(kIVFPQSearchColDefs),
		},
		BindingTags:     []int32{tableFuncTag},
		TblFuncExprList: tableFuncExprs,
	}
	tableFuncNodeID := builder.appendNode(tableFuncNode, ctx)

	err = builder.addBinding(tableFuncNodeID, tree.AliasClause{Alias: tree.Identifier("mo_ivfpq_alias_0")}, ctx)
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
		scanNode.FilterList, ivfpqCtx.partPos, ivfpqCtx.origFuncName,
		ivfpqCtx.vecLitArg, tableFuncTag, scoreColType)
	scanNode.FilterList = newScanFilters
	if len(peeledDistFilters) > 0 {
		logutil.Debugf("IVFPQ pushdown: peeled %d distance predicate(s) onto table function FilterList",
			len(peeledDistFilters))
		tableFuncNode.FilterList = append(tableFuncNode.FilterList, peeledDistFilters...)
	}

	// Rewrite any SELECT-side `origFuncName(ec, vec)` calls in the surrounding
	// projections to reference the table function's score column directly, so
	// the user's `... AS dist` does not re-run the distance kernel on every
	// scanned row.
	{
		scanTag := scanNode.BindingTags[0]
		replaceDistFnExprsWithScoreCol(projNode.ProjectList, scanTag,
			ivfpqCtx.partPos, ivfpqCtx.origFuncName, ivfpqCtx.vecLitArg,
			tableFuncTag, scoreColType)
		if childNode != nil {
			replaceDistFnExprsWithScoreCol(childNode.ProjectList, scanTag,
				ivfpqCtx.partPos, ivfpqCtx.origFuncName, ivfpqCtx.vecLitArg,
				tableFuncTag, scoreColType)
		}
	}

	// pushdown limit to Table Function; over-fetch if residual filters OR a
	// peeled distance-range bound will prune the result set further.
	if len(scanNode.FilterList) > 0 || len(peeledDistFilters) > 0 {
		if limitConst := limit.GetLit(); limitConst != nil {
			originalLimit := limitConst.GetU64Val()
			overFetchFactor := calculatePostFilterOverFetchFactor(originalLimit)
			newLimit := max(uint64(float64(originalLimit)*overFetchFactor), originalLimit+10)
			tableFuncNode.Limit = &Expr{
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
		} else {
			tableFuncNode.Limit = DeepCopyExpr(limit)
		}
	} else {
		tableFuncNode.Limit = DeepCopyExpr(limit)
	}

	// oncond
	wherePkEqPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		{
			Typ: ivfpqCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanNode.BindingTags[0],
					ColPos: ivfpqCtx.pkPos,
				},
			},
		},
		{
			Typ: ivfpqCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tableFuncTag,
					ColPos: 0,
				},
			},
		},
	})

	joinNodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{scanNode.NodeId, tableFuncNodeID},
		JoinType: plan.Node_INNER,
		OnList:   []*Expr{wherePkEqPk},
	}, ctx)

	scanNode.Limit = nil
	scanNode.Offset = nil

	// Create SortBy with distance column from table function
	orderByScore := []*OrderBySpec{
		{
			Expr: &Expr{
				Typ: tableFuncNode.TableDef.Cols[1].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag,
						ColPos: 1,
					},
				},
			},
			Flag: vecCtx.sortDirection,
		},
	}

	sortByID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{joinNodeID},
		OrderBy:  orderByScore,
		Limit:    limit,
		Offset:   DeepCopyExpr(sortNode.Offset),
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
