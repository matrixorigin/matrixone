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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/vectorplan"
	"github.com/stretchr/testify/require"
)

// buildIvfpqCreate / buildIvfpqSearch are the registered IVF-PQ
// table-function builders (lifted to pkg/vectorindex/ivfpq/plugin/plan).
// The shims keep these tests readable; the registry lookup is the public
// contract the dispatch at query_builder.go uses too.
func buildIvfpqCreate(b *QueryBuilder, tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	fn, _ := vectorplan.TableFunc("ivfpq_create")
	return fn(b, tbl, ctx, exprs, children)
}

func buildIvfpqSearch(b *QueryBuilder, tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	fn, _ := vectorplan.TableFunc("ivfpq_search")
	return fn(b, tbl, ctx, exprs, children)
}

func newStringNumValFn(s string) *tree.FuncExpr {
	nv := tree.NewNumVal[string](s, s, false, tree.P_char)
	return &tree.FuncExpr{Exprs: tree.Exprs{nv}}
}

func newNonNumValFn() *tree.FuncExpr {
	// UnresolvedName is not a NumVal — triggers the error branch.
	un := tree.NewUnresolvedName(tree.NewCStr("col", 0))
	return &tree.FuncExpr{Exprs: tree.Exprs{un}}
}

func TestGetCagraParams_OK(t *testing.T) {
	var b *QueryBuilder // GetContext on nil QueryBuilder returns context.TODO()
	out, err := b.getCagraParams(newStringNumValFn(`{"m":"32"}`))
	require.NoError(t, err)
	require.Equal(t, `{"m":"32"}`, out)
}

func TestGetCagraParams_Error(t *testing.T) {
	var b *QueryBuilder
	_, err := b.getCagraParams(newNonNumValFn())
	require.Error(t, err)
}

// (TestGetIvfpqParams_OK / _Error were deleted when getIvfpqParams moved
// into pkg/vectorindex/ivfpq/plugin/plan and became unexported. The
// TestBuildIvfpq{Create,Search}_BadParams tests below exercise the same
// error path through the registered builder.)

// makeBuildArgs builds the n-element exprs slice the build* functions take.
// First entry is a NumVal (param string); the rest are placeholder int64
// literals — only the count matters for the input-validation paths.
func makeBuildArgs(t *testing.T, n int) []*plan.Expr {
	t.Helper()
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

// makeNumValTblFunc wraps a NumVal in a *tree.TableFunction so that
// builder.getCagraParams / getIvfpqParams will succeed.
func makeNumValTblFunc(s string) *tree.TableFunction {
	nv := tree.NewNumVal[string](s, s, false, tree.P_char)
	return &tree.TableFunction{Func: &tree.FuncExpr{Exprs: tree.Exprs{nv}}}
}

func TestBuildCagraCreate_TooFewArgs(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(b, nil)
	_, err := b.buildCagraCreate(makeNumValTblFunc(`{}`), ctx, makeBuildArgs(t, 3), nil)
	require.Error(t, err)
}

func TestBuildCagraCreate_BadParams(t *testing.T) {
	// First expr is not a NumVal → getCagraParams errors out.
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(b, nil)

	un := tree.NewUnresolvedName(tree.NewCStr("x", 0))
	tbl := &tree.TableFunction{Func: &tree.FuncExpr{Exprs: tree.Exprs{un}}}
	_, err := b.buildCagraCreate(tbl, ctx, makeBuildArgs(t, 4), nil)
	require.Error(t, err)
}

func TestBuildCagraCreate_OK(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(b, nil)
	id, err := b.buildCagraCreate(makeNumValTblFunc(`{"m":"32"}`), ctx, makeBuildArgs(t, 4), nil)
	require.NoError(t, err)
	require.Equal(t, int32(0), id)
	node := b.qry.Nodes[id]
	require.Equal(t, plan.Node_FUNCTION_SCAN, node.NodeType)
	require.Equal(t, kCAGRACreateFuncName, node.TableDef.TblFunc.Name)
	// First arg was peeled off as Param; remaining 3 attach to TblFuncExprList.
	require.Len(t, node.TblFuncExprList, 3)
	require.True(t, node.TableDef.TblFunc.IsSingle, "create runs single-thread")
}

func TestBuildCagraSearch_BadArgCount(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(b, nil)
	// 2 is not 3 or 4 → error
	_, err := b.buildCagraSearch(makeNumValTblFunc(`{}`), ctx, makeBuildArgs(t, 2), nil)
	require.Error(t, err)
	// 5 is not 3 or 4 → error
	_, err = b.buildCagraSearch(makeNumValTblFunc(`{}`), ctx, makeBuildArgs(t, 5), nil)
	require.Error(t, err)
}

func TestBuildCagraSearch_BadParams(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(b, nil)
	un := tree.NewUnresolvedName(tree.NewCStr("x", 0))
	tbl := &tree.TableFunction{Func: &tree.FuncExpr{Exprs: tree.Exprs{un}}}
	_, err := b.buildCagraSearch(tbl, ctx, makeBuildArgs(t, 3), nil)
	require.Error(t, err)
}

func TestBuildCagraSearch_OK(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(b, nil)
	for _, n := range []int{3, 4} {
		id, err := b.buildCagraSearch(makeNumValTblFunc(`{"m":"32"}`), ctx, makeBuildArgs(t, n), nil)
		require.NoError(t, err)
		node := b.qry.Nodes[id]
		require.Equal(t, plan.Node_FUNCTION_SCAN, node.NodeType)
		require.Equal(t, kCAGRASearchFuncName, node.TableDef.TblFunc.Name)
		require.Len(t, node.TblFuncExprList, n-1, "first arg is peeled into Param")
	}
}

func TestBuildIvfpqCreate_TooFewArgs(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(b, nil)
	_, err := buildIvfpqCreate(b,makeNumValTblFunc(`{}`), ctx, makeBuildArgs(t, 3), nil)
	require.Error(t, err)
}

func TestBuildIvfpqCreate_BadParams(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(b, nil)
	un := tree.NewUnresolvedName(tree.NewCStr("x", 0))
	tbl := &tree.TableFunction{Func: &tree.FuncExpr{Exprs: tree.Exprs{un}}}
	_, err := buildIvfpqCreate(b,tbl, ctx, makeBuildArgs(t, 4), nil)
	require.Error(t, err)
}

func TestBuildIvfpqCreate_OK(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(b, nil)
	id, err := buildIvfpqCreate(b,makeNumValTblFunc(`{"lists":"4"}`), ctx, makeBuildArgs(t, 4), nil)
	require.NoError(t, err)
	node := b.qry.Nodes[id]
	require.Equal(t, plan.Node_FUNCTION_SCAN, node.NodeType)
	require.Equal(t, "ivfpq_create", node.TableDef.TblFunc.Name)
	require.Len(t, node.TblFuncExprList, 3)
	require.True(t, node.TableDef.TblFunc.IsSingle)
}

func TestBuildIvfpqSearch_BadArgCount(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(b, nil)
	_, err := buildIvfpqSearch(b,makeNumValTblFunc(`{}`), ctx, makeBuildArgs(t, 2), nil)
	require.Error(t, err)
	_, err = buildIvfpqSearch(b,makeNumValTblFunc(`{}`), ctx, makeBuildArgs(t, 5), nil)
	require.Error(t, err)
}

func TestBuildIvfpqSearch_BadParams(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(b, nil)
	un := tree.NewUnresolvedName(tree.NewCStr("x", 0))
	tbl := &tree.TableFunction{Func: &tree.FuncExpr{Exprs: tree.Exprs{un}}}
	_, err := buildIvfpqSearch(b,tbl, ctx, makeBuildArgs(t, 3), nil)
	require.Error(t, err)
}

func TestBuildIvfpqSearch_OK(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(b, nil)
	for _, n := range []int{3, 4} {
		id, err := buildIvfpqSearch(b,makeNumValTblFunc(`{"lists":"4"}`), ctx, makeBuildArgs(t, n), nil)
		require.NoError(t, err)
		node := b.qry.Nodes[id]
		require.Equal(t, plan.Node_FUNCTION_SCAN, node.NodeType)
		require.Equal(t, "ivfpq_search", node.TableDef.TblFunc.Name)
		require.Len(t, node.TblFuncExprList, n-1)
	}
}
