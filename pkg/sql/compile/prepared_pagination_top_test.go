// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/stretchr/testify/require"
)

func TestPreparedPaginationTopRebindsCurrentBounds(t *testing.T) {
	c := newMergeTopFallbackTestCompile(t)
	c.isPrepare = false
	params := vector.NewVec(types.T_text.ToType())
	c.proc.SetPrepareParams(params)
	t.Cleanup(func() {
		c.proc.SetPrepareParams(nil)
		params.Free(c.proc.Mp())
		c.proc.Free()
	})
	bound := func(pos int32) *plan.Expr {
		param := &plan.Expr{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: pos}}}
		target := &plan.Expr{Typ: plan.Type{Id: int32(types.T_uint64)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}}
		expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "cast", []*plan.Expr{param, target})
		require.NoError(t, err)
		return expr
	}
	limit, skip := bound(0), bound(1)
	key := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	scan := &plan.Node{NodeType: plan.Node_TABLE_SCAN, TableDef: &plan.TableDef{}, ProjectList: []*plan.Expr{key}}
	sort := &plan.Node{NodeType: plan.Node_SORT, Children: []int32{0}, Limit: limit, Offset: skip, OrderBy: []*plan.OrderBySpec{{Expr: key}}}
	query := &plan.Query{StmtType: plan.Query_SELECT, Nodes: []*plan.Node{scan, sort}}
	c.pn = &plan.Plan{IsPrepare: true, Plan: &plan.Plan_Query{Query: query}}

	tests := []struct {
		name, limit, offset string
		nullAt              int
		want                uint64
		optimized           bool
	}{
		{name: "zero offset", limit: "7", offset: "0", nullAt: -1, want: 7, optimized: true},
		{name: "positive offset", limit: "7", offset: "1", nullAt: -1, want: 8, optimized: true},
		{name: "threshold", limit: "7", offset: "16377", nullAt: -1, want: 16384, optimized: true},
		{name: "above threshold", limit: "7", offset: "16378", nullAt: -1},
		{name: "uint64 overflow", limit: "18446744073709551615", offset: "1", nullAt: -1},
		{name: "zero limit", limit: "0", offset: "1", nullAt: -1},
		{name: "null limit", limit: "", offset: "1", nullAt: 0},
		{name: "null offset", limit: "7", offset: "", nullAt: 1},
		{name: "negative offset", limit: "7", offset: "-1", nullAt: -1},
		{name: "invalid offset", limit: "7", offset: "bad", nullAt: -1},
		{name: "again after fallback", limit: "3", offset: "5", nullAt: -1, want: 8, optimized: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params.Reset(types.T_text.ToType())
			for i, value := range []string{tc.limit, tc.offset} {
				require.NoError(t, vector.AppendBytes(params, []byte(value), i == tc.nullAt, c.proc.Mp()))
			}
			got, ok := c.preparedPaginationTop(sort)
			require.Equal(t, tc.optimized, ok)
			if ok {
				require.Equal(t, tc.want, got)
			}
			require.Same(t, limit, sort.Limit)
			require.Same(t, skip, sort.Offset)
		})
	}

	params.Reset(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("7"), false, c.proc.Mp()))
	require.NoError(t, vector.AppendBytes(params, []byte("1"), false, c.proc.Mp()))
	sort.Limit = plan2.MakePlan2Uint64ConstExprWithType(7)
	got, ok := c.preparedPaginationTop(sort)
	require.True(t, ok)
	require.Equal(t, uint64(8), got)
	sort.Limit, sort.Offset = limit, plan2.MakePlan2Uint64ConstExprWithType(1)
	got, ok = c.preparedPaginationTop(sort)
	require.True(t, ok)
	require.Equal(t, uint64(8), got)
	sort.Offset = skip
	scope := newMergeTopFallbackTestScope(c)
	result := c.compileSort(sort, []*Scope{scope})
	require.Len(t, result, 1)
	t.Cleanup(func() {
		result[0].FreeOperator(c)
		result[0].release()
	})
	offsetOp, ok := result[0].RootOp.(*offset.Offset)
	require.True(t, ok)
	require.Same(t, skip, offsetOp.OffsetExpr)
	topOp, ok := offsetOp.GetChildren(0).(*top.Top)
	require.True(t, ok)
	require.Equal(t, uint64(8), topOp.Limit.GetLit().GetU64Val())

	c.isPrepare = true
	_, ok = c.preparedPaginationTop(sort)
	require.False(t, ok, "PREPARE cannot freeze a later execution's value")
	c.isPrepare = false
	c.pn.IsPrepare = false
	_, ok = c.preparedPaginationTop(sort)
	require.False(t, ok)
}

func TestPreparedPaginationTopBoundMatchesRuntimeConversion(t *testing.T) {
	c := newMergeTopFallbackTestCompile(t)
	params := vector.NewVec(types.T_text.ToType())
	c.proc.SetPrepareParams(params)
	t.Cleanup(func() {
		c.proc.SetPrepareParams(nil)
		params.Free(c.proc.Mp())
		c.proc.Free()
	})
	cast := func(source *plan.Expr, typ types.T) *plan.Expr {
		target := &plan.Expr{Typ: plan.Type{Id: int32(typ)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}}
		result, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "cast", []*plan.Expr{source, target})
		require.NoError(t, err)
		return result
	}
	for _, tc := range []struct {
		name, value string
		typ         types.T
		kind        vector.PrepareParamKind
	}{
		{name: "signed narrow", value: "127", typ: types.T_int8, kind: vector.PrepareParamInteger},
		{name: "unsigned narrow", value: "255", typ: types.T_uint8, kind: vector.PrepareParamInteger},
		{name: "signed large", value: "16384", typ: types.T_int64, kind: vector.PrepareParamInteger},
		{name: "unsigned large", value: "16384", typ: types.T_uint64, kind: vector.PrepareParamInteger},
		{name: "boolean false", value: "0", typ: types.T_bool, kind: vector.PrepareParamBoolean},
		{name: "boolean true", value: "1", typ: types.T_bool, kind: vector.PrepareParamBoolean},
		{name: "decimal text", value: "0007", typ: types.T_uint64, kind: vector.PrepareParamNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params.Reset(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(params, []byte(tc.value), false, c.proc.Mp()))
			c.proc.SetPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{tc.kind})
			param := &plan.Expr{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
			expr := cast(cast(param, tc.typ), types.T_uint64)
			got, dynamic, ok := preparedPaginationTopBound(c.proc, expr)
			require.True(t, ok)
			require.True(t, dynamic)
			vec, free, warned, err := rule.EvaluateConstantExpression(c.proc, expr, batch.EmptyForConstFoldBatch)
			if free != nil {
				defer free()
			}
			require.NoError(t, err)
			require.False(t, warned)
			require.False(t, vec.IsNull(0))
			require.Equal(t, got, vector.GetFixedAtWithTypeCheck[uint64](vec, 0))
		})
	}
}

func TestPreparedPaginationTopBoundRejectsUnprovedConversions(t *testing.T) {
	c := newMergeTopFallbackTestCompile(t)
	params := vector.NewVec(types.T_text.ToType())
	c.proc.SetPrepareParams(params)
	t.Cleanup(func() {
		c.proc.SetPrepareParams(nil)
		params.Free(c.proc.Mp())
		c.proc.Free()
	})
	require.NoError(t, vector.AppendBytes(params, []byte("128"), false, c.proc.Mp()))
	param := &plan.Expr{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	cast := func(source *plan.Expr, typ types.T) *plan.Expr {
		target := &plan.Expr{Typ: plan.Type{Id: int32(typ)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}}
		result, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "cast", []*plan.Expr{source, target})
		require.NoError(t, err)
		return result
	}
	limit := cast(cast(param, types.T_int8), types.T_uint64)
	_, _, ok := preparedPaginationTopBound(c.proc, limit)
	require.False(t, ok, "a narrowing cast must not have its error hidden")
	params.Reset(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("127"), false, c.proc.Mp()))
	value, dynamic, ok := preparedPaginationTopBound(c.proc, limit)
	require.True(t, ok)
	require.True(t, dynamic)
	require.Equal(t, uint64(127), value)
	limit.GetF().SyntaxExplicitCast = true
	_, _, ok = preparedPaginationTopBound(c.proc, limit)
	require.False(t, ok, "explicit casts retain their runtime diagnostic owner")
	limit.GetF().SyntaxExplicitCast = false
	c.proc.SetPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{vector.PrepareParamFloat})
	_, _, ok = preparedPaginationTopBound(c.proc, limit)
	require.False(t, ok, "float transport is not an integer conversion proof")
	c.proc.SetPrepareParamsWithMetadata(params, []bool{true}, nil)
	_, _, ok = preparedPaginationTopBound(c.proc, limit)
	require.False(t, ok, "binary-number transport is not decimal text")
	c.proc.SetPrepareParamsWithMetadata(params, nil, []bool{true})
	_, _, ok = preparedPaginationTopBound(c.proc, limit)
	require.False(t, ok, "binary-string conversion retains its runtime owner")
}
