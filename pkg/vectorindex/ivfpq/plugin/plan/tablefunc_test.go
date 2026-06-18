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

// IVF-PQ table-function builder + param tests. Mirror of the CAGRA
// suite in pkg/vectorindex/cagra/plugin/plan; see that file for the
// mock-builder rationale.

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type stubPlanBuilder struct {
	ctx     context.Context
	nodes   []*plan.Node
	nextTag int32
}

func newStubPlanBuilder() *stubPlanBuilder {
	return &stubPlanBuilder{ctx: context.Background()}
}

func (b *stubPlanBuilder) GetContext() context.Context { return b.ctx }

func (b *stubPlanBuilder) GenNewBindTag() int32 {
	b.nextTag++
	return b.nextTag
}

func (b *stubPlanBuilder) AppendNode(node *plan.Node, _ planplugin.BindContext) int32 {
	id := int32(len(b.nodes))
	b.nodes = append(b.nodes, node)
	return id
}

func (b *stubPlanBuilder) ApplyIndicesForSortUsingHnsw(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("not used in tablefunc tests")
}
func (b *stubPlanBuilder) ApplyIndicesForSortUsingCagra(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("not used in tablefunc tests")
}
func (b *stubPlanBuilder) ApplyIndicesForSortUsingIvfpq(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("not used in tablefunc tests")
}
func (b *stubPlanBuilder) ApplyIndicesForSortUsingIvfflat(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("not used in tablefunc tests")
}
func (b *stubPlanBuilder) CanApplyHnsw(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("not used in tablefunc tests")
}
func (b *stubPlanBuilder) CanApplyCagra(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("not used in tablefunc tests")
}
func (b *stubPlanBuilder) CanApplyIvfpq(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("not used in tablefunc tests")
}
func (b *stubPlanBuilder) CanApplyIvfflat(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("not used in tablefunc tests")
}

var _ planplugin.PlanBuilder = (*stubPlanBuilder)(nil)

func init() {
	if planplugin.DeepCopyColDefList == nil {
		planplugin.DeepCopyColDefList = func(in []*plan.ColDef) []*plan.ColDef { return in }
	}
}

func newStringNumValFn(s string) *tree.FuncExpr {
	nv := tree.NewNumVal[string](s, s, false, tree.P_char)
	return &tree.FuncExpr{Exprs: tree.Exprs{nv}}
}

func newNonNumValFn() *tree.FuncExpr {
	un := tree.NewUnresolvedName(tree.NewCStr("col", 0))
	return &tree.FuncExpr{Exprs: tree.Exprs{un}}
}

func makeBuildArgs(n int) []*plan.Expr {
	out := make([]*plan.Expr, 0, n)
	out = append(out, &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "{}"}}},
	})
	for i := 1; i < n; i++ {
		out = append(out, &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: int64(i)}}},
		})
	}
	return out
}

func makeNumValTblFunc(s string) *tree.TableFunction {
	nv := tree.NewNumVal[string](s, s, false, tree.P_char)
	return &tree.TableFunction{Func: &tree.FuncExpr{Exprs: tree.Exprs{nv}}}
}

func TestGetIvfpqParams_OK(t *testing.T) {
	b := newStubPlanBuilder()
	out, err := getIvfpqParams(b, newStringNumValFn(`{"lists":"4"}`))
	require.NoError(t, err)
	require.Equal(t, `{"lists":"4"}`, out)
}

func TestGetIvfpqParams_Error(t *testing.T) {
	b := newStubPlanBuilder()
	_, err := getIvfpqParams(b, newNonNumValFn())
	require.Error(t, err)
}

func TestBuildIvfpqCreate_TooFewArgs(t *testing.T) {
	b := newStubPlanBuilder()
	_, err := buildIvfpqCreate(b, makeNumValTblFunc(`{}`), nil, makeBuildArgs(3), nil)
	require.Error(t, err)
}

func TestBuildIvfpqCreate_BadParams(t *testing.T) {
	b := newStubPlanBuilder()
	un := tree.NewUnresolvedName(tree.NewCStr("x", 0))
	tbl := &tree.TableFunction{Func: &tree.FuncExpr{Exprs: tree.Exprs{un}}}
	_, err := buildIvfpqCreate(b, tbl, nil, makeBuildArgs(4), nil)
	require.Error(t, err)
}

func TestBuildIvfpqCreate_OK(t *testing.T) {
	b := newStubPlanBuilder()
	id, err := buildIvfpqCreate(b, makeNumValTblFunc(`{"lists":"4"}`), nil, makeBuildArgs(4), nil)
	require.NoError(t, err)
	node := b.nodes[id]
	require.Equal(t, plan.Node_FUNCTION_SCAN, node.NodeType)
	require.Equal(t, IVFPQCreateFuncName, node.TableDef.TblFunc.Name)
	require.Len(t, node.TblFuncExprList, 3)
	require.True(t, node.TableDef.TblFunc.IsSingle)
}

func TestBuildIvfpqSearch_BadArgCount(t *testing.T) {
	b := newStubPlanBuilder()
	_, err := buildIvfpqSearch(b, makeNumValTblFunc(`{}`), nil, makeBuildArgs(2), nil)
	require.Error(t, err)
	_, err = buildIvfpqSearch(b, makeNumValTblFunc(`{}`), nil, makeBuildArgs(5), nil)
	require.Error(t, err)
}

func TestBuildIvfpqSearch_BadParams(t *testing.T) {
	b := newStubPlanBuilder()
	un := tree.NewUnresolvedName(tree.NewCStr("x", 0))
	tbl := &tree.TableFunction{Func: &tree.FuncExpr{Exprs: tree.Exprs{un}}}
	_, err := buildIvfpqSearch(b, tbl, nil, makeBuildArgs(3), nil)
	require.Error(t, err)
}

func TestBuildIvfpqSearch_OK(t *testing.T) {
	for _, n := range []int{3, 4} {
		b := newStubPlanBuilder()
		id, err := buildIvfpqSearch(b, makeNumValTblFunc(`{"lists":"4"}`), nil, makeBuildArgs(n), nil)
		require.NoError(t, err)
		node := b.nodes[id]
		require.Equal(t, plan.Node_FUNCTION_SCAN, node.NodeType)
		require.Equal(t, IVFPQSearchFuncName, node.TableDef.TblFunc.Name)
		require.Len(t, node.TblFuncExprList, n-1)
	}
}
