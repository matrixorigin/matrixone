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

// Package plan implements the HNSW plugin's plan-layer hooks.
//
// HNSW has one extra wrinkle compared to CAGRA / IVF-PQ: the ORDER BY can
// reach the scan through a JOIN (handled by
// buildVectorSortContextThroughJoin in pkg/sql/plan). When that happens
// vecCtx.VecArgExpr is non-nil and PrepareContext must use
// GetArgsFromDistFnForJoin instead of GetArgsFromDistFn.
// VectorSearchProviderChildren also returns non-nil children for the
// hnsw_search FUNCTION_SCAN.
//
// Body lifted from pkg/sql/plan/apply_indices_hnsw.go (now deleted).
package plan

import (
	"fmt"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/vectorplan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"
)

type Hooks struct{}

var _ planplugin.Hooks = Hooks{}

// CanApply is the non-destructive probe used by detectVectorGuard.
func (Hooks) CanApply(pb vectorplan.PlanBuilder, vecCtx *vectorplan.VectorSortContext, mti *vectorplan.MultiTableIndexRef) (bool, error) {
	ctx, err := PrepareContext(pb, vecCtx, mti)
	if err != nil {
		return false, err
	}
	return ctx != nil, nil
}

// ApplyForSort rewrites `SELECT … ORDER BY distfn(col, v) LIMIT k` to use
// the HNSW index. Lifted from applyIndicesForSortUsingHnsw
// (was pkg/sql/plan/apply_indices_hnsw.go:122).
//
// opts.ColRefCnt / IdxColMap are unused by HNSW (only IVF-FLAT's
// auto-mode rewrite consults them).
func (Hooks) ApplyForSort(
	pb vectorplan.PlanBuilder,
	vecCtx *vectorplan.VectorSortContext,
	mti *vectorplan.MultiTableIndexRef,
	nodeID int32,
	_ vectorplan.ApplyForSortOpts,
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

	hnswCtx, err := PrepareContext(pb, vecCtx, mti)
	if err != nil || hnswCtx == nil {
		return nodeID, false, err
	}

	tblCfgStr := fmt.Sprintf(`{"db": "%s", "src": "%s", "metadata":"%s", "index":"%s", "threads_search": %d, "orig_func_name": "%s"}`,
		scanNode.ObjRef.SchemaName,
		scanNode.TableDef.Name,
		hnswCtx.metaDef.IndexTableName,
		hnswCtx.idxDef.IndexTableName,
		hnswCtx.nThread,
		hnswCtx.origFuncName)

	// JOIN between source table and hnsw_search table function.
	tableFuncTag := pb.GenNewBindTag()
	tableFuncNode := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name:  HNSWSearchFuncName,
				Param: []byte(hnswCtx.params),
			},
			Cols: vectorplan.DeepCopyColDefList(HNSWSearchColDefs),
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
							Sval: tblCfgStr,
						},
					},
				},
			},
			vectorplan.DeepCopyExpr(hnswCtx.vecLitArg),
		},
	}
	tableFuncNodeID := pb.AppendNode(tableFuncNode, ctx)

	if err := pb.AddBinding(tableFuncNodeID, tree.AliasClause{Alias: tree.Identifier("mo_hnsw_alias_0")}, ctx); err != nil {
		return 0, false, err
	}

	// pushdown limit; over-fetch on residual filters.
	if len(scanNode.FilterList) > 0 {
		if limitConst := limit.GetLit(); limitConst != nil {
			originalLimit := limitConst.GetU64Val()
			overFetchFactor := vectorplan.CalculatePostFilterOverFetchFactor(originalLimit)
			newLimit := max(uint64(float64(originalLimit)*overFetchFactor), originalLimit+10)
			tableFuncNode.Limit = &plan.Expr{
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
			tableFuncNode.Limit = vectorplan.DeepCopyExpr(limit)
		}
	} else {
		tableFuncNode.Limit = vectorplan.DeepCopyExpr(limit)
	}

	wherePkEqPk, _ := pb.BindFuncByName("=", []*plan.Expr{
		{
			Typ: hnswCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanNode.BindingTags[0],
					ColPos: hnswCtx.pkPos,
				},
			},
		},
		{
			Typ: hnswCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tableFuncTag,
					ColPos: 0,
				},
			},
		},
	})

	joinNodeID := pb.AppendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{scanNode.NodeId, tableFuncNodeID},
		JoinType: plan.Node_INNER,
		OnList:   []*plan.Expr{wherePkEqPk},
	}, ctx)

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
		NodeType: plan.Node_SORT,
		Children: []int32{joinNodeID},
		OrderBy:  orderByScore,
		Limit:    limit,
		Offset:   vectorplan.DeepCopyExpr(sortNode.Offset),
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

// hnswIndexContext is the per-query HNSW rewrite scratchpad.
type hnswIndexContext struct {
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

func (c *hnswIndexContext) OrigFuncName() string  { return c.origFuncName }
func (c *hnswIndexContext) PartPos() int32        { return c.partPos }
func (c *hnswIndexContext) PkPos() int32          { return c.pkPos }
func (c *hnswIndexContext) Params() string        { return c.params }
func (c *hnswIndexContext) NThread() int64        { return c.nThread }
func (c *hnswIndexContext) VecLitArg() *plan.Expr { return c.vecLitArg }

// PrepareContext is the lifted body of prepareHnswIndexContext
// (was pkg/sql/plan/apply_indices_hnsw.go:43).
func PrepareContext(pb vectorplan.PlanBuilder, vecCtx *vectorplan.VectorSortContext, mti *vectorplan.MultiTableIndexRef) (*hnswIndexContext, error) {
	if vecCtx == nil || mti == nil {
		return nil, nil
	}
	if vecCtx.DistFnExpr == nil {
		return nil, nil
	}
	if vecCtx.RankOption != nil && vecCtx.RankOption.Mode == "force" {
		return nil, nil
	}

	rewriteAllowed, err := pb.ValidateVectorIndexSortRewrite(vecCtx)
	if err != nil || !rewriteAllowed {
		return nil, err
	}

	metaDef := mti.IndexDefs[catalog.Hnsw_TblType_Metadata]
	idxDef := mti.IndexDefs[catalog.Hnsw_TblType_Storage]
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

	origFuncName := vecCtx.DistFnExpr.Func.ObjName
	if opType != metric.DistFuncOpTypes[origFuncName] {
		return nil, nil
	}

	keyPart := idxDef.Parts[0]
	partPos := vecCtx.ScanNode.TableDef.Name2ColIndex[keyPart]
	var vecLitArg *plan.Expr
	var found bool
	if vecCtx.VecArgExpr != nil {
		_, vecLitArg, found = pb.GetArgsFromDistFnForJoin(vecCtx.DistFnExpr, partPos, vecCtx.ScanNode.BindingTags[0])
	} else {
		_, vecLitArg, found = pb.GetArgsFromDistFn(vecCtx.DistFnExpr, partPos)
	}
	if !found {
		return nil, nil
	}

	pkPos := vecCtx.ScanNode.TableDef.Name2ColIndex[vecCtx.ScanNode.TableDef.Pkey.PkeyColName]
	pkType := vecCtx.ScanNode.TableDef.Cols[pkPos].Typ

	nThread, err := pb.ResolveVariable("hnsw_threads_search", true, false)
	if err != nil {
		return nil, err
	}

	return &hnswIndexContext{
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
