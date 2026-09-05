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

// IVFFLAT table-function builder + param tests, and the plan.go redirects.
// Mirror of the IVF-PQ suite in pkg/vectorindex/ivfpq/plugin/plan.
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

// stubPlanBuilder records appended nodes. The four Apply*/CanApply* variants panic
// with their algorithm name; the redirect tests assert the recovered value.
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
	panic("hnsw")
}
func (b *stubPlanBuilder) ApplyIndicesForSortUsingCagra(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("cagra")
}
func (b *stubPlanBuilder) ApplyIndicesForSortUsingIvfpq(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("ivfpq")
}
func (b *stubPlanBuilder) ApplyIndicesForSortUsingIvfflat(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("ivfflat")
}
func (b *stubPlanBuilder) CanApplyHnsw(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("hnsw")
}
func (b *stubPlanBuilder) CanApplyCagra(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("cagra")
}
func (b *stubPlanBuilder) CanApplyIvfpq(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("ivfpq")
}
func (b *stubPlanBuilder) CanApplyIvfflat(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("ivfflat")
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

// --- tablefunc.go ----------------------------------------------------------

func TestGetIvfflatTblFuncParams_OK(t *testing.T) {
	out, err := getIvfflatTblFuncParams(newStubPlanBuilder(), newStringNumValFn(`{"lists":"4"}`))
	require.NoError(t, err)
	require.Equal(t, `{"lists":"4"}`, out)
}

// A non-literal first argument is rejected.
func TestGetIvfflatTblFuncParams_Error(t *testing.T) {
	_, err := getIvfflatTblFuncParams(newStubPlanBuilder(), newNonNumValFn())
	require.Error(t, err)
}

func TestBuildIvfflatCreate_TooFewArgs(t *testing.T) {
	_, err := buildIvfflatCreate(newStubPlanBuilder(), makeNumValTblFunc(`{}`), nil, makeBuildArgs(1), nil)
	require.Error(t, err)
}

func TestBuildIvfflatCreate_BadParams(t *testing.T) {
	un := tree.NewUnresolvedName(tree.NewCStr("x", 0))
	tbl := &tree.TableFunction{Func: &tree.FuncExpr{Exprs: tree.Exprs{un}}}
	_, err := buildIvfflatCreate(newStubPlanBuilder(), tbl, nil, makeBuildArgs(2), nil)
	require.Error(t, err)
}

// The happy path appends a FUNCTION_SCAN, moves the first argument into Param,
// and marks the node single-threaded (centroid computation is not parallel).
func TestBuildIvfflatCreate_OK(t *testing.T) {
	b := newStubPlanBuilder()
	id, err := buildIvfflatCreate(b, makeNumValTblFunc(`{"lists":"4"}`), nil, makeBuildArgs(3), nil)
	require.NoError(t, err)
	node := b.nodes[id]
	require.Equal(t, plan.Node_FUNCTION_SCAN, node.NodeType)
	require.Equal(t, IVFFLATCreateFuncName, node.TableDef.TblFunc.Name)
	require.Equal(t, `{"lists":"4"}`, string(node.TableDef.TblFunc.Param))
	require.Len(t, node.TblFuncExprList, 2)
	require.True(t, node.TableDef.TblFunc.IsSingle)
}

// --- plan.go: the three redirects ------------------------------------------

// Each hook reaches the ivfflat method on the builder, not a sibling's.
func TestHooks_Redirects(t *testing.T) {
	for _, c := range []struct {
		name string
		call func()
	}{
		{"CanApply", func() {
			_, _ = Hooks{}.CanApply(newStubPlanBuilder(), &planplugin.VectorSortContext{}, &planplugin.MultiTableIndexRef{})
		}},
		{"ApplyForSort", func() {
			_, _, _ = Hooks{}.ApplyForSort(newStubPlanBuilder(), &planplugin.VectorSortContext{}, &planplugin.MultiTableIndexRef{}, 0, planplugin.ApplyForSortOpts{})
		}},
		{"BuildLogicalSearch", func() {
			_, _, _ = Hooks{}.BuildLogicalSearch(newStubPlanBuilder(), &planplugin.VectorSortContext{}, &planplugin.MultiTableIndexRef{}, 0, planplugin.ApplyForSortOpts{})
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func() { require.Equal(t, "ivfflat", recover()) }()
			c.call()
		})
	}
}
