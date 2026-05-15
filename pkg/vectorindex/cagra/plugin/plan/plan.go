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

// Package plan implements the CAGRA plugin's plan-layer hooks.
// See pkg/vectorindex/ivfpq/plugin/plan for the canonical template and the
// facade-pattern explanation.
//
// Body lifted from pkg/sql/plan/apply_indices_cagra.go (now deleted).
package plan

import (
	"fmt"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/vectorplan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"
)

// Hooks implements plugin/plan.Hooks for CAGRA.
type Hooks struct{}

// Compile-time interface check.
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
// the CAGRA index. Lifted from applyIndicesForSortUsingCagra
// (was pkg/sql/plan/apply_indices_cagra.go:118).
func (Hooks) ApplyForSort(
	pb vectorplan.PlanBuilder,
	vecCtx *vectorplan.VectorSortContext,
	mti *vectorplan.MultiTableIndexRef,
	nodeID int32,
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

	cagraCtx, err := PrepareContext(pb, vecCtx, mti)
	if err != nil || cagraCtx == nil {
		return nodeID, false, err
	}

	tblCfgStr := fmt.Sprintf(`{"db": "%s", "src": "%s", "metadata":"%s", "index":"%s", "threads_search": %d, "orig_func_name": "%s", "batch_window": %d}`,
		scanNode.ObjRef.SchemaName,
		scanNode.TableDef.Name,
		cagraCtx.metaDef.IndexTableName,
		cagraCtx.idxDef.IndexTableName,
		cagraCtx.nThread,
		cagraCtx.origFuncName,
		cagraCtx.batchWindow)

	includeCols, err := vectorplan.ParseIncludedColumnsFromParams(cagraCtx.idxDef.IndexAlgoParams)
	if err != nil {
		return nodeID, false, err
	}
	pkColName := ""
	if scanNode.TableDef.Pkey != nil {
		pkColName = scanNode.TableDef.Pkey.PkeyColName
	}
	if len(includeCols) > 0 {
		logutil.Debugf("CAGRA pushdown: INCLUDE columns = %v, scan filters = %d",
			includeCols, len(scanNode.FilterList))
	}
	predsJSON, peeled, residualFilters, err := vectorplan.BuildFilterPredicateJSON(
		scanNode.FilterList, scanNode, includeCols, pkColName)
	if err != nil {
		return nodeID, false, err
	}
	if predsJSON != "" {
		logutil.Debugf("CAGRA pushdown: peeled %d filter(s), %d residual, preds_json = %s",
			len(peeled), len(residualFilters), predsJSON)
		scanNode.FilterList = residualFilters
	}

	tableFuncTag := pb.GenNewBindTag()
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
		vectorplan.DeepCopyExpr(cagraCtx.vecLitArg),
	}
	if predsJSON != "" {
		tableFuncExprs = append(tableFuncExprs, vectorplan.MakePlan2StringConstExprWithType(predsJSON))
	}
	tableFuncNode := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name:  CAGRASearchFuncName,
				Param: []byte(cagraCtx.params),
			},
			Cols: vectorplan.DeepCopyColDefList(CAGRASearchColDefs),
		},
		BindingTags:     []int32{tableFuncTag},
		TblFuncExprList: tableFuncExprs,
	}
	tableFuncNodeID := pb.AppendNode(tableFuncNode, ctx)

	if err := pb.AddBinding(tableFuncNodeID, tree.AliasClause{Alias: tree.Identifier("mo_cagra_alias_0")}, ctx); err != nil {
		return 0, false, err
	}

	scoreColType := tableFuncNode.TableDef.Cols[1].Typ
	newScanFilters, peeledDistFilters := pb.PeelAndRewriteDistFnFilters(
		scanNode.FilterList, cagraCtx.partPos, cagraCtx.origFuncName,
		cagraCtx.vecLitArg, tableFuncTag, scoreColType)
	scanNode.FilterList = newScanFilters
	if len(peeledDistFilters) > 0 {
		logutil.Debugf("CAGRA pushdown: peeled %d distance predicate(s) onto table function FilterList",
			len(peeledDistFilters))
		tableFuncNode.FilterList = append(tableFuncNode.FilterList, peeledDistFilters...)
	}

	{
		scanTag := scanNode.BindingTags[0]
		vectorplan.ReplaceDistFnExprsWithScoreCol(projNode.ProjectList, scanTag,
			cagraCtx.partPos, cagraCtx.origFuncName, cagraCtx.vecLitArg,
			tableFuncTag, scoreColType)
		if childNode != nil {
			vectorplan.ReplaceDistFnExprsWithScoreCol(childNode.ProjectList, scanTag,
				cagraCtx.partPos, cagraCtx.origFuncName, cagraCtx.vecLitArg,
				tableFuncTag, scoreColType)
		}
	}

	if len(scanNode.FilterList) > 0 || len(peeledDistFilters) > 0 {
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
			Typ: cagraCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanNode.BindingTags[0],
					ColPos: cagraCtx.pkPos,
				},
			},
		},
		{
			Typ: cagraCtx.pkType,
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

// DMLSyncEntriesTable: CAGRA uses CDC for index maintenance, not synchronous
// plan-time DML sync.
func (Hooks) DMLSyncEntriesTable() string { return "" }

// SupportsSyncDML: CAGRA does not participate in
// buildPreInsertMultiTableIndexes / buildDeleteMultiTableIndexes.
func (Hooks) SupportsSyncDML() bool { return false }

// cagraIndexContext is the per-query CAGRA rewrite scratchpad, lifted from
// pkg/sql/plan/apply_indices_cagra.go. Unexported; tests use the getters
// below.
type cagraIndexContext struct {
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
}

func (c *cagraIndexContext) OrigFuncName() string  { return c.origFuncName }
func (c *cagraIndexContext) PartPos() int32        { return c.partPos }
func (c *cagraIndexContext) PkPos() int32          { return c.pkPos }
func (c *cagraIndexContext) Params() string        { return c.params }
func (c *cagraIndexContext) NThread() int64        { return c.nThread }
func (c *cagraIndexContext) BatchWindow() int64    { return c.batchWindow }
func (c *cagraIndexContext) VecLitArg() *plan.Expr { return c.vecLitArg }

// PrepareContext is the lifted body of prepareCagraIndexContext
// (was pkg/sql/plan/apply_indices_cagra.go:43).
func PrepareContext(pb vectorplan.PlanBuilder, vecCtx *vectorplan.VectorSortContext, mti *vectorplan.MultiTableIndexRef) (*cagraIndexContext, error) {
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

	metaDef := mti.IndexDefs[catalog.Cagra_TblType_Metadata]
	idxDef := mti.IndexDefs[catalog.Cagra_TblType_Storage]
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
	_, vecLitArg, found := pb.GetArgsFromDistFn(vecCtx.DistFnExpr, partPos)
	if !found {
		return nil, nil
	}

	pkPos := vecCtx.ScanNode.TableDef.Name2ColIndex[vecCtx.ScanNode.TableDef.Pkey.PkeyColName]
	pkType := vecCtx.ScanNode.TableDef.Cols[pkPos].Typ

	nThread, err := pb.ResolveVariable("cagra_threads_search", true, false)
	if err != nil {
		return nil, err
	}
	batchWindow, err := pb.ResolveVariable("cagra_batch_window", true, false)
	if err != nil {
		return nil, err
	}

	return &cagraIndexContext{
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
	}, nil
}
