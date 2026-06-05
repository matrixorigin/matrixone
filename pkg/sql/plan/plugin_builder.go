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
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// init publishes schema/tablefunc helper bodies to the planplugin
// package as function variables. Plugin schema.go and tablefunc.go call
// these (cross-package) instead of importing pkg/sql/plan directly,
// which would create a cycle.
func init() {
	planplugin.CreateIndexDef = CreateIndexDef
	planplugin.MakeHiddenColDefByName = MakeHiddenColDefByName
	planplugin.ValidateIncludeColumns = validateIncludeColumnsForPlugin
	planplugin.DeepCopyColDefList = DeepCopyColDefList
}

// validateIncludeColumnsForPlugin adapts validateIncludeColumns to the
// narrower planplugin.CompilerContext the plugin uses. Dispatch always
// passes a real *plan.CompilerContext, so the assertion is total at
// call time.
func validateIncludeColumnsForPlugin(ctx planplugin.CompilerContext,
	includeCols []*tree.UnresolvedName, colMap map[string]*plan.ColDef,
	vecColName, pkeyName string, supportedTypes []types.T) error {
	return validateIncludeColumns(ctx.(CompilerContext), includeCols, colMap, vecColName, pkeyName, supportedTypes)
}

// *QueryBuilder satisfies planplugin.PlanBuilder. Compile-time check
// catches signature drift.
var _ planplugin.PlanBuilder = (*QueryBuilder)(nil)

// keep "context" import live (used by GetContext signature on PlanBuilder).
var _ = context.TODO

// Three base facade methods needed by per-plugin tablefunc.go to
// construct FUNCTION_SCAN nodes.

func (builder *QueryBuilder) GenNewBindTag() int32 { return builder.genNewBindTag() }

func (builder *QueryBuilder) AppendNode(node *plan.Node, ctx planplugin.BindContext) int32 {
	bc, _ := ctx.(*BindContext)
	return builder.appendNode(node, bc)
}

// GetContext is the *QueryBuilder.GetContext defined in query_builder.go.

// Per-algo redirect methods. The plugin's Hooks.ApplyForSort body is a
// one-liner that calls one of these; this method then converts the
// exported types back to internal and invokes the real
// `applyIndicesForSortUsing<Algo>` body in pkg/sql/plan.
//
// All bodies share the same shape: convert vctx + mti, call internal
// method (which returns (int32, error)), report applied=(nodeID != newID).

func (builder *QueryBuilder) ApplyIndicesForSortUsingHnsw(vctx *planplugin.VectorSortContext, mti *planplugin.MultiTableIndexRef, nodeID int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	vc, m := fromPlanplugin(vctx, mti)
	newID, err := builder.applyIndicesForSortUsingHnsw(nodeID, vc, m)
	return newID, newID != nodeID, err
}

func (builder *QueryBuilder) ApplyIndicesForSortUsingCagra(vctx *planplugin.VectorSortContext, mti *planplugin.MultiTableIndexRef, nodeID int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	vc, m := fromPlanplugin(vctx, mti)
	newID, err := builder.applyIndicesForSortUsingCagra(nodeID, vc, m)
	return newID, newID != nodeID, err
}

func (builder *QueryBuilder) ApplyIndicesForSortUsingIvfpq(vctx *planplugin.VectorSortContext, mti *planplugin.MultiTableIndexRef, nodeID int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	vc, m := fromPlanplugin(vctx, mti)
	newID, err := builder.applyIndicesForSortUsingIvfpq(nodeID, vc, m)
	return newID, newID != nodeID, err
}

func (builder *QueryBuilder) ApplyIndicesForSortUsingIvfflat(vctx *planplugin.VectorSortContext, mti *planplugin.MultiTableIndexRef, nodeID int32, opts planplugin.ApplyForSortOpts) (int32, bool, error) {
	vc, m := fromPlanplugin(vctx, mti)
	newID, err := builder.applyIndicesForSortUsingIvfflat(nodeID, vc, m, opts.ColRefCnt, opts.IdxColMap)
	return newID, newID != nodeID, err
}

// CanApply<Algo> — non-destructive probe used by detectVectorGuard. We
// fold it into the prepareXxxIndexContext probe — returns true if the
// context is non-nil (i.e. the index can satisfy this query).

func (builder *QueryBuilder) CanApplyHnsw(vctx *planplugin.VectorSortContext, mti *planplugin.MultiTableIndexRef) (bool, error) {
	vc, m := fromPlanplugin(vctx, mti)
	ctx, err := builder.prepareHnswIndexContext(vc, m)
	if err != nil {
		return false, err
	}
	return ctx != nil, nil
}

func (builder *QueryBuilder) CanApplyCagra(vctx *planplugin.VectorSortContext, mti *planplugin.MultiTableIndexRef) (bool, error) {
	vc, m := fromPlanplugin(vctx, mti)
	ctx, err := builder.prepareCagraIndexContext(vc, m)
	if err != nil {
		return false, err
	}
	return ctx != nil, nil
}

func (builder *QueryBuilder) CanApplyIvfpq(vctx *planplugin.VectorSortContext, mti *planplugin.MultiTableIndexRef) (bool, error) {
	vc, m := fromPlanplugin(vctx, mti)
	ctx, err := builder.prepareIvfpqIndexContext(vc, m)
	if err != nil {
		return false, err
	}
	return ctx != nil, nil
}

func (builder *QueryBuilder) CanApplyIvfflat(vctx *planplugin.VectorSortContext, mti *planplugin.MultiTableIndexRef) (bool, error) {
	vc, m := fromPlanplugin(vctx, mti)
	ctx, err := builder.prepareIvfIndexContext(vc, m)
	if err != nil {
		return false, err
	}
	return ctx != nil, nil
}

// toPlanplugin converts the internal pkg/sql/plan types to the exported
// plugin-facing types so the centralised dispatch in apply_indices.go can
// hand a *VectorSortContext / *MultiTableIndexRef to p.Plan().CanApply /
// p.Plan().ApplyForSort. Inverse of fromPlanplugin.
func toPlanplugin(vc *vectorSortContext, m *MultiTableIndex) (*planplugin.VectorSortContext, *planplugin.MultiTableIndexRef) {
	var vctx *planplugin.VectorSortContext
	if vc != nil {
		vctx = &planplugin.VectorSortContext{
			ProjNode:       vc.projNode,
			SortNode:       vc.sortNode,
			ScanNode:       vc.scanNode,
			ChildNode:      vc.childNode,
			OrderExpr:      vc.orderExpr,
			DistFnExpr:     vc.distFnExpr,
			SortDirection:  vc.sortDirection,
			Limit:          vc.limit,
			RankOption:     vc.rankOption,
			ProviderNodeID: vc.providerNodeID,
			VecArgExpr:     vc.vecArgExpr,
		}
	}
	var mti *planplugin.MultiTableIndexRef
	if m != nil {
		mti = &planplugin.MultiTableIndexRef{
			IndexAlgo:       m.IndexAlgo,
			IndexAlgoParams: m.IndexAlgoParams,
			IndexDefs:       m.IndexDefs,
		}
	}
	return vctx, mti
}

// fromPlanplugin converts the exported plugin-facing types back to
// internal pkg/sql/plan types so the real body methods can take them
// directly.
func fromPlanplugin(vctx *planplugin.VectorSortContext, mti *planplugin.MultiTableIndexRef) (*vectorSortContext, *MultiTableIndex) {
	var vc *vectorSortContext
	if vctx != nil {
		vc = &vectorSortContext{
			projNode:       vctx.ProjNode,
			sortNode:       vctx.SortNode,
			scanNode:       vctx.ScanNode,
			childNode:      vctx.ChildNode,
			orderExpr:      vctx.OrderExpr,
			distFnExpr:     vctx.DistFnExpr,
			sortDirection:  vctx.SortDirection,
			limit:          vctx.Limit,
			rankOption:     vctx.RankOption,
			providerNodeID: vctx.ProviderNodeID,
			vecArgExpr:     vctx.VecArgExpr,
		}
	}
	var m *MultiTableIndex
	if mti != nil {
		m = &MultiTableIndex{
			IndexAlgo:       mti.IndexAlgo,
			IndexAlgoParams: mti.IndexAlgoParams,
			IndexDefs:       mti.IndexDefs,
		}
	}
	return vc, m
}
