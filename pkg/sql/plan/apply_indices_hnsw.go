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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
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

	metaDef := multiTableIndex.IndexDefs[catalog.Hnsw_TblType_Metadata]
	idxDef := multiTableIndex.IndexDefs[catalog.Hnsw_TblType_Storage]
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

	nThread, err := builder.compCtx.ResolveVariable("hnsw_threads_search", true, false)
	if err != nil {
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
		nThread:      nThread.(int64),
	}, nil
}

func (builder *QueryBuilder) applyIndicesForSortUsingHnsw(nodeID int32, vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex) (int32, error) {

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

	hnswCtx, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	if err != nil || hnswCtx == nil {
		return nodeID, err
	}

	tblCfgStr := fmt.Sprintf(`{"db": "%s", "src": "%s", "metadata":"%s", "index":"%s", "threads_search": %d, "orig_func_name": "%s"}`,
		scanNode.ObjRef.SchemaName,
		scanNode.TableDef.Name,
		hnswCtx.metaDef.IndexTableName,
		hnswCtx.idxDef.IndexTableName,
		hnswCtx.nThread,
		hnswCtx.origFuncName)

	// JOIN between source table and hnsw_search table function
	tableFuncTag := builder.genNewBindTag()
	tableFuncNode := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name:  kHNSWSearchFuncName,
				Param: []byte(hnswCtx.params),
			},
			Cols: DeepCopyColDefList(kHNSWSearchColDefs),
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
							Sval: tblCfgStr,
						},
					},
				},
			},
			DeepCopyExpr(hnswCtx.vecLitArg),
		},
	}
	tableFuncNodeID := builder.appendNode(tableFuncNode, ctx)

	err = builder.addBinding(tableFuncNodeID, tree.AliasClause{Alias: tree.Identifier("mo_hnsw_alias_0")}, ctx)
	if err != nil {
		return 0, err
	}

	// pushdown limit to Table Function
	// When there are filters, over-fetch to get more candidates
	// This ensures we have enough candidates after filtering
	if len(scanNode.FilterList) > 0 {
		// Over-fetch strategy: dynamically adjust factor based on limit size
		// Smaller limits need more over-fetching due to higher variance
		if limitConst := limit.GetLit(); limitConst != nil {
			originalLimit := limitConst.GetU64Val()

			// Use shared function to calculate over-fetch factor
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
			// If limit is not a constant, just copy it
			tableFuncNode.Limit = DeepCopyExpr(limit)
		}
	} else {
		// No filters, use original limit
		tableFuncNode.Limit = DeepCopyExpr(limit)
	}

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

	sortByID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{joinNodeID},
		OrderBy:  orderByScore,
		Limit:    limit,                         // Apply LIMIT after sorting
		Offset:   DeepCopyExpr(sortNode.Offset), // Apply OFFSET after sorting
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
