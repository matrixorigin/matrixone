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

// Package plan implements the IVF-PQ plugin's plan-layer hooks.
//
// Scope: anything that runs during plan tree construction — building the
// hidden-table schema for CREATE INDEX (schema.go), rewriting an ANN
// ORDER BY query to use the index (this file), and per-algo DML sync
// metadata (this file).
//
// # The facade pattern
//
// This package cannot import pkg/sql/plan (the SQL layer blank-imports the
// plugin for init() registration, so the reverse direction is a cycle).
// Instead the plugin operates against pkg/sql/plan/vectorplan, a leaf
// sub-package both sides import:
//
//	vectorplan.PlanBuilder          — interface, implemented by
//	                                  *plan.QueryBuilder. Carries every
//	                                  bind-tag / node-assembly primitive
//	                                  the lifted body needs.
//	vectorplan.VectorSortContext    — captured ORDER BY context (exported
//	                                  mirror of plan.vectorSortContext).
//	vectorplan.MultiTableIndexRef   — exported mirror of
//	                                  plan.MultiTableIndex.
//	vectorplan.CompilerContext      — narrow CompilerContext surface
//	                                  (just GetContext()).
//	vectorplan.{DeepCopyExpr,
//	            BuildFilterPredicateJSON,
//	            MakePlan2StringConstExprWithType,
//	            ReplaceDistFnExprsWithScoreCol,
//	            ParseIncludedColumnsFromParams,
//	            CalculatePostFilterOverFetchFactor,
//	            CreateIndexDef,
//	            MakeHiddenColDefByName,
//	            ValidateIncludeColumns}
//	                                — function variables populated by
//	                                  pkg/sql/plan's init(). The plugin
//	                                  calls them as ordinary functions.
//	(IVF-PQ-specific table-function metadata —
//	 IVFPQSearchFuncName / IVFPQSearchColDefs — lives next door in
//	 tablefunc.go now that the table-function builders moved out of
//	 pkg/sql/plan into this package.)
//
// Adding a new algorithm: if your rewrite body needs a helper that lives
// in pkg/sql/plan, add it as a function variable in vectorplan and
// populate it from pkg/sql/plan/plugin_builder.go's init().
//
// The body here is the IVF-PQ ANN rewrite — lifted in full from
// pkg/sql/plan/apply_indices_ivfpq.go (now deleted). It depends only on
// vectorplan, pkg/pb/plan, pkg/catalog, pkg/vectorindex/metric, and
// stdlib.
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

// Hooks implements plugin/plan.Hooks for IVF-PQ.
//
// The framework requires five methods on this type:
//
//	BuildSecondaryIndexDefs   — see schema.go in this package
//	CanApply                  — this file, non-destructive ANN probe
//	ApplyForSort              — this file, ANN query rewrite
//	DMLSyncEntriesTable       — this file, sync DML metadata
//	SupportsSyncDML           — this file, sync DML opt-in
//
// If you add a method to plugin/plan/hooks.go and forget to implement it
// here, the `var _ planplugin.Hooks = Hooks{}` interface check fails the
// build.
type Hooks struct{}

// Compile-time interface check.
var _ planplugin.Hooks = Hooks{}

// CanApply is the non-destructive probe used by detectVectorGuard
// (pkg/sql/plan/apply_indices.go) to mark a scan node as protected from
// other optimizers before the actual ANN rewrite runs. Should reach the
// same true/false verdict as ApplyForSort would, but without mutating any
// plan state. For IVF-PQ we just run PrepareContext (which is pure) and
// report whether it produces a context.
func (Hooks) CanApply(pb vectorplan.PlanBuilder, vecCtx *vectorplan.VectorSortContext, mti *vectorplan.MultiTableIndexRef) (bool, error) {
	ctx, err := PrepareContext(pb, vecCtx, mti)
	if err != nil {
		return false, err
	}
	return ctx != nil, nil
}

// ApplyForSort rewrites `SELECT … ORDER BY distfn(col, v) LIMIT k` to use
// the IVF-PQ index. Called from pkg/sql/plan/apply_indices.go after
// CanApply has already returned true.
//
// Returns:
//
//	newNodeID — root of the rewritten sub-plan (or `nodeID` if unchanged)
//	applied   — true if a rewrite was performed; false means this index
//	            cannot satisfy the query (op_type mismatch, force-mode
//	            bypass, etc.) — the caller falls back to exact sort
//	err       — non-nil only on hard errors; "cannot apply" is signaled
//	            via applied=false, not err
//
// opts.ColRefCnt / IdxColMap are unused by IVF-PQ (only IVF-FLAT's
// auto-mode rewrite consults them).
//
// What the rewrite does:
//  1. Resolves the ORDER BY's distance function against the index's
//     op_type (l2 / inner_product / cosine). Mismatch → applied=false.
//  2. Builds an `ivfpq_search` table-function node with the index
//     metadata, vector literal, and any predicate-pushdown JSON.
//  3. JOINs that table function with the source scan on PK = PK.
//  4. Rewrites surrounding distance-function expressions to reference
//     the table function's score column (avoids re-computing the kernel
//     for every scanned row).
//  5. Pushes LIMIT down to the table function, over-fetching when
//     residual filters or peeled distance bounds remain.
//
// Lifted from applyIndicesForSortUsingIvfpq (was at
// pkg/sql/plan/apply_indices_ivfpq.go:124).
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

	ivfpqCtx, err := PrepareContext(pb, vecCtx, mti)
	if err != nil || ivfpqCtx == nil {
		return nodeID, false, err
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

	// Predicate pushdown on INCLUDE columns and the primary key: peel
	// filters that reference only INCLUDE columns (or the PK, routed to
	// host_ids via the __mo_pk_host_id virtual column) into a JSON array
	// passed as the ivfpq_search 3rd arg. Unserializable/mixed predicates
	// stay on the TABLE_SCAN.
	includeCols, err := vectorplan.ParseIncludedColumnsFromParams(ivfpqCtx.idxDef.IndexAlgoParams)
	if err != nil {
		return nodeID, false, err
	}
	pkColName := ""
	if scanNode.TableDef.Pkey != nil {
		pkColName = scanNode.TableDef.Pkey.PkeyColName
	}
	if len(includeCols) > 0 {
		logutil.Debugf("IVFPQ pushdown: INCLUDE columns = %v, scan filters = %d",
			includeCols, len(scanNode.FilterList))
	}
	predsJSON, peeled, residualFilters, err := vectorplan.BuildFilterPredicateJSON(
		scanNode.FilterList, scanNode, includeCols, pkColName)
	if err != nil {
		return nodeID, false, err
	}
	if predsJSON != "" {
		logutil.Debugf("IVFPQ pushdown: peeled %d filter(s), %d residual, preds_json = %s",
			len(peeled), len(residualFilters), predsJSON)
		scanNode.FilterList = residualFilters
	}

	// JOIN between source table and ivfpq_search table function
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
		vectorplan.DeepCopyExpr(ivfpqCtx.vecLitArg),
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
				Name:  IVFPQSearchFuncName,
				Param: []byte(ivfpqCtx.params),
			},
			Cols: vectorplan.DeepCopyColDefList(IVFPQSearchColDefs),
		},
		BindingTags:     []int32{tableFuncTag},
		TblFuncExprList: tableFuncExprs,
	}
	tableFuncNodeID := pb.AppendNode(tableFuncNode, ctx)

	if err := pb.AddBinding(tableFuncNodeID, tree.AliasClause{Alias: tree.Identifier("mo_ivfpq_alias_0")}, ctx); err != nil {
		return 0, false, err
	}

	// Peel `distfn(col, vec) <op> K` predicates off the scan FilterList and
	// re-attach them — rewritten to reference the table function's score
	// column — on tableFuncNode.FilterList.
	scoreColType := tableFuncNode.TableDef.Cols[1].Typ
	newScanFilters, peeledDistFilters := pb.PeelAndRewriteDistFnFilters(
		scanNode.FilterList, ivfpqCtx.partPos, ivfpqCtx.origFuncName,
		ivfpqCtx.vecLitArg, tableFuncTag, scoreColType)
	scanNode.FilterList = newScanFilters
	if len(peeledDistFilters) > 0 {
		logutil.Debugf("IVFPQ pushdown: peeled %d distance predicate(s) onto table function FilterList",
			len(peeledDistFilters))
		tableFuncNode.FilterList = append(tableFuncNode.FilterList, peeledDistFilters...)
	}

	// Rewrite any SELECT-side `origFuncName(ec, vec)` calls in the surrounding
	// projections to reference the table function's score column directly.
	{
		scanTag := scanNode.BindingTags[0]
		vectorplan.ReplaceDistFnExprsWithScoreCol(projNode.ProjectList, scanTag,
			ivfpqCtx.partPos, ivfpqCtx.origFuncName, ivfpqCtx.vecLitArg,
			tableFuncTag, scoreColType)
		if childNode != nil {
			vectorplan.ReplaceDistFnExprsWithScoreCol(childNode.ProjectList, scanTag,
				ivfpqCtx.partPos, ivfpqCtx.origFuncName, ivfpqCtx.vecLitArg,
				tableFuncTag, scoreColType)
		}
	}

	// Pushdown limit to Table Function; over-fetch if residual filters OR a
	// peeled distance-range bound will prune the result set further.
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

	// On-cond: scan.pk = tableFunc.pk
	wherePkEqPk, _ := pb.BindFuncByName("=", []*plan.Expr{
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

// ivfpqIndexContext is the per-query IVF-PQ rewrite scratchpad, lifted from
// pkg/sql/plan/apply_indices_ivfpq.go.
//
// Kept unexported because callers outside this package never construct one
// directly — they get it back from PrepareContext as an opaque handle and
// pass it to ApplyForSort. The getter methods below let tests inspect the
// resolved fields.
type ivfpqIndexContext struct {
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

// Accessors so external tests can inspect the resolved fields without
// exporting the struct itself.
func (c *ivfpqIndexContext) OrigFuncName() string  { return c.origFuncName }
func (c *ivfpqIndexContext) PartPos() int32        { return c.partPos }
func (c *ivfpqIndexContext) PkPos() int32          { return c.pkPos }
func (c *ivfpqIndexContext) Params() string        { return c.params }
func (c *ivfpqIndexContext) NThread() int64        { return c.nThread }
func (c *ivfpqIndexContext) BatchWindow() int64    { return c.batchWindow }
func (c *ivfpqIndexContext) NProbe() int64         { return c.nProbe }
func (c *ivfpqIndexContext) VecLitArg() *plan.Expr { return c.vecLitArg }

// PrepareContext is the lifted body of prepareIvfpqIndexContext.
func PrepareContext(pb vectorplan.PlanBuilder, vecCtx *vectorplan.VectorSortContext, mti *vectorplan.MultiTableIndexRef) (*ivfpqIndexContext, error) {
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

	metaDef := mti.IndexDefs[catalog.Ivfpq_TblType_Metadata]
	idxDef := mti.IndexDefs[catalog.Ivfpq_TblType_Storage]
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

	nThread, err := pb.ResolveVariable("ivfpq_threads_search", true, false)
	if err != nil {
		return nil, err
	}
	batchWindow, err := pb.ResolveVariable("ivfpq_batch_window", true, false)
	if err != nil {
		return nil, err
	}
	nProbe := int64(20)
	if nProbeIf, err2 := pb.ResolveVariable("probe_limit", true, false); err2 != nil {
		return nil, err2
	} else if nProbeIf != nil {
		nProbe = nProbeIf.(int64)
	}

	return &ivfpqIndexContext{
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
