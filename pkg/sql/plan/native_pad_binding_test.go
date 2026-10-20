// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/stretchr/testify/require"
)

func TestNativePADPlanAndPreFoldPersistence(t *testing.T) {
	proc := newPlanTestProcess(t)
	ctx := NewMockCompilerContext(false, proc)
	ctx.SetContext(context.WithValue(t.Context(), collationBindingRevisionKey{}, uint32(1)))
	for _, sql := range []string{
		"select n_name collate utf8mb4_general_ci from nation",
		"select n_name from nation where n_name collate utf8mb4_general_ci in ('a','z')",
		"prepare native_stmt from select ? collate utf8mb4_general_ci = 'A'",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			features, err := pb.RequiredRemoteExpressionFeatures(p)
			require.NoError(t, err)
			require.True(t, features.NativePADCollation)
		})
	}
	for _, sql := range []string{
		"create table native_guard (b bool default ('a' collate utf8mb4_general_ci = 'A'))",
		"create table native_guard (a int, b bool generated always as ('a' collate utf8mb4_general_ci = 'A') stored)",
		"create table native_guard (a int, check ('a' collate utf8mb4_general_ci = 'A'))",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, false)
			require.ErrorContains(t, err, "native PAD collation persistence is disabled")
			require.Nil(t, p)
		})
	}
}

func TestNativePADColumnCastPreservesSource(t *testing.T) {
	proc := newPlanTestProcess(t)
	ctx := context.WithValue(t.Context(), collationBindingRevisionKey{}, uint32(1))
	typ := types.NewWithCharset(types.T_varchar, 16, 0, types.CharsetUTF8)
	column := &Expr{Typ: typ.PlanType(), Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 0}}}
	original := DeepCopyExpr(column)
	ast := &tree.CollateExpr{Expr: tree.NewUnresolvedName(tree.NewCStr("c", 1)), Collation: "utf8mb4_general_ci"}
	impl := NewMockBinder(gomock.NewController(t))
	impl.EXPECT().BindExpr(ast.Expr, int32(0), false).Return(column, nil)
	binder := &baseBinder{sysCtx: ctx, impl: impl}
	bound, err := binder.bindCollateExpr(ast, 0)
	require.NoError(t, err)
	require.NotNil(t, bound.GetF(), "改变同OID的revision也必须是真正CAST")
	require.True(t, proto.Equal(original, column))
	predicate, err := BindFuncExprImplByPlanExpr(ctx, "=", []*Expr{bound, makePlan2StringConstExprWithType("a")})
	require.NoError(t, err)
	executor, err := colexec.NewExpressionExecutor(proc, predicate)
	require.NoError(t, err)
	defer executor.Free()
	bat := batch.NewWithSize(1)
	defer bat.Clean(proc.Mp())
	bat.Vecs[0] = vector.NewVec(typ)
	for i, value := range []string{"Á", "a ", "z", ""} {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(value), i == 3, proc.Mp()))
	}
	bat.SetRowCount(4)
	for i := 0; i < 2; i++ {
		value, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, false, false}, vector.MustFixedColNoTypeCheck[bool](value))
		require.True(t, value.IsNull(3))
		require.Equal(t, typ, *bat.Vecs[0].GetType())
		require.Equal(t, "Á", string(bat.Vecs[0].GetBytesAt(0)))
		executor.ResetForNextQuery()
	}
	bat.Vecs[0].Free(proc.Mp())
	bat.Vecs[0] = vector.NewVec(typ)
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte{0xff}, false, proc.Mp()))
	bat.SetRowCount(1)
	_, err = executor.Eval(proc, []*batch.Batch{bat}, nil)
	require.ErrorContains(t, err, "UTF-8")
	require.Equal(t, typ, *bat.Vecs[0].GetType())
	require.Equal(t, []byte{0xff}, bat.Vecs[0].GetBytesAt(0))
}

func TestNativePADPreparedBindingReuse(t *testing.T) {
	proc := newPlanTestProcess(t)
	ctx := context.WithValue(t.Context(), collationBindingRevisionKey{}, uint32(1))
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select ? collate utf8mb4_general_ci = ?", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	for _, source := range []types.Type{types.T_text.ToType(), types.T_varbinary.ToType(), types.T_any.ToType()} {
		bindings := withPreparedSourceBindings(ctx, []PreparedSourceBinding{
			{Position: 0, Type: source}, {Position: 1, Type: types.T_text.ToType()},
		})
		bound, err := NewDefaultBinder(bindings, nil, nil, Type{}, nil).BindExpr(ast, 0, false)
		require.NoError(t, err)
		for _, arg := range bound.GetF().Args {
			require.Equal(t, uint32(3), arg.Typ.Charset)
			require.Equal(t, uint32(1), arg.Typ.CollationVersion)
		}
		p := &Plan{Plan: &pb.Plan_Query{Query: &pb.Query{Steps: []int32{0}, Nodes: []*pb.Node{{
			NodeType: pb.Node_VALUE_SCAN, ProjectList: []*Expr{bound},
		}}}}}
		before := proto.Clone(p)
		for _, text := range []string{"Á ", "z"} {
			func() {
				values := []any{ParamValue{Value: text, IsBin: source.Charset == types.CharsetBinary}, ParamValue{Value: "a"}}
				if source.Oid == types.T_any {
					values[0] = nil
				}
				filled, err := FillValuesOfParamsInPlan(bindings, p, values)
				require.NoError(t, err)
				require.True(t, proto.Equal(before, p))
				executor, err := colexec.NewExpressionExecutor(proc, filled.GetQuery().Nodes[0].ProjectList[0])
				require.NoError(t, err)
				defer executor.Free()
				result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
				require.NoError(t, err)
				if source.Oid == types.T_any {
					require.True(t, result.IsConstNull())
				} else {
					require.Equal(t, text == "Á ", vector.MustFixedColNoTypeCheck[bool](result)[0])
				}
			}()
		}
	}
}

func TestNativePADFoldedVectorPreservesDeclaration(t *testing.T) {
	proc := newPlanTestProcess(t)
	typ := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8)
	typ.CollationVersion = types.CollationVersionV1
	vec := vector.NewVec(typ)
	defer vec.Free(proc.Mp())
	declared := typ.PlanType()
	declared.NotNullable = true
	declared.CollationCoercibilitySet = true
	declared.CollationCoercibility = 0
	require.Equal(t, declared, rule.FoldedVectorType(declared, vec), "fold不可丢失显式COLLATE优先级")
}

func TestNativePADFoldedListPublishesDomain(t *testing.T) {
	proc := newPlanTestProcess(t)
	ctx := context.WithValue(t.Context(), collationBindingRevisionKey{}, uint32(1))
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select 'Á' collate utf8mb4_general_ci in ('a','z')", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	bound, err := NewDefaultBinder(ctx, nil, nil, Type{}, nil).BindExpr(ast, 0, false)
	require.NoError(t, err)
	list := bound.GetF().Args[1]
	require.NotNil(t, list.GetList())
	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(list), proc, false, true)
	require.NoError(t, err)
	node := &pb.Node{ProjectList: []*Expr{DeepCopyExpr(list)}}
	rule.NewConstantFold(false).Apply(node, nil, proc)
	for _, expr := range []*Expr{folded, node.ProjectList[0]} {
		require.NotNil(t, expr.GetVec())
		require.Equal(t, int32(types.T_varchar), expr.Typ.Id)
		require.Equal(t, uint32(3), expr.Typ.Charset)
		require.Equal(t, uint32(1), expr.Typ.CollationVersion)
		features, err := pb.RequiredRemoteExpressionFeatures(expr)
		require.NoError(t, err)
		require.True(t, features.NativePADCollation, "folded carrier仍必须要求native协议")
		func() {
			vec := vector.NewVec(types.T_any.ToType())
			defer vec.Free(proc.Mp())
			require.NoError(t, vec.UnmarshalBinary(expr.GetVec().Data))
			require.Equal(t, expr.Typ.Charset, uint32(vec.GetType().Charset))
			require.Equal(t, expr.Typ.CollationVersion, uint32(vec.GetType().CollationVersion))
			require.Equal(t, "a", string(vec.GetBytesAt(0)), "载体仍保存原值而非权重")
			require.Equal(t, "z", string(vec.GetBytesAt(1)))
		}()
	}
}

func TestNativePADPersistedBinderRestoresMode(t *testing.T) {
	ctx := context.WithValue(t.Context(), collationBindingRevisionKey{}, uint32(1))
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select 'a' collate utf8mb4_general_ci", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	binder := NewDefaultBinder(ctx, nil, nil, Type{}, nil)
	_, err = binder.bindPersistedExpr(ast, 0, false)
	require.ErrorContains(t, err, "native PAD collation persistence is disabled")
	require.False(t, binder.persistedExpressionBinding)
	require.False(t, binder.persistedFormatCompatibility)
	bound, err := binder.BindExpr(ast, 0, false)
	require.NoError(t, err)
	require.Equal(t, uint32(1), bound.Typ.CollationVersion)
	legacy, err := NewDefaultBinder(WithCollationBindingRevision(t.Context(), 0), nil, nil, Type{}, nil).BindExpr(ast, 0, false)
	require.NoError(t, err)
	require.Zero(t, legacy.Typ.CollationVersion, "历史来源不继承当前显式请求的默认代际")
	for _, invalid := range []any{uint32(2), "1"} {
		bad := context.WithValue(t.Context(), collationBindingRevisionKey{}, invalid)
		_, err := NewDefaultBinder(bad, nil, nil, Type{}, nil).BindExpr(ast, 0, false)
		require.ErrorContains(t, err, "unsupported collation binding revision")
	}
}

func TestNativePADBindingGeneration(t *testing.T) {
	for _, name := range []string{"utf8mb4_general_ci", "utf8_general_ci", "utf8mb3_general_ci", "utf8mb4_bin", "utf8_bin", "utf8mb3_bin"} {
		t.Run(name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select '😀A ' collate "+name, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			for _, binding := range []struct {
				name     string
				ctx      context.Context
				revision uint32
			}{
				{"public", t.Context(), 1},
				{"historical", WithCollationBindingRevision(t.Context(), 0), 0},
				{"native_replay", WithCollationBindingRevision(t.Context(), 1), 1},
			} {
				t.Run(binding.name, func(t *testing.T) {
					expr, err := NewDefaultBinder(binding.ctx, nil, nil, Type{}, nil).BindExpr(ast, 0, false)
					require.NoError(t, err)
					require.Equal(t, binding.revision, expr.Typ.CollationVersion)
					require.Contains(t, []uint32{2, 3}, expr.Typ.Charset, "兼容别名不变成严格三字节域")
					require.True(t, expr.Typ.CollationCoercibilitySet)
					require.Zero(t, expr.Typ.CollationCoercibility)
				})
			}
		})
	}
	for _, control := range []struct {
		sql      string
		revision uint32
	}{
		{"'a'", 0},
		{"_binary'a'", 0},
		{"'a' collate utf8mb4_0900_ai_ci", 0},
		{"'a' collate utf8mb4_unicode_ci", 1},
	} {
		t.Run(control.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select "+control.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			expr, err := NewDefaultBinder(t.Context(), nil, nil, Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			require.Equal(t, control.revision, expr.Typ.CollationVersion)
		})
	}
}

func TestNativePADBoundExpressionSemantics(t *testing.T) {
	proc := newPlanTestProcess(t)
	ctx := t.Context()
	for _, tc := range []struct {
		sql  string
		want bool
	}{
		{"'Á ' collate utf8mb4_general_ci = 'a'", true},
		{"'a ' collate utf8mb4_bin = 'a'", true},
		{"'A' collate utf8mb4_bin = 'a'", false},
		{"'A' collate utf8mb4_0900_ai_ci = 'a'", false},
		{"_binary'a ' = _binary'a'", false},
		{"'Á' collate utf8mb4_unicode_ci = 'a'", true},
		{"'Á' collate utf8_general_ci in ('a','z')", true},
		{"'Á' collate utf8mb3_general_ci not in ('a','z')", false},
		{"'Á' collate utf8mb4_general_ci between 'a' and 'b'", true},
		{"least('a' collate utf8mb4_general_ci,'B') = 'a'", true},
		{"greatest('a' collate utf8mb4_general_ci,'B') = 'b'", true},
		{"coalesce(NULL,'a' collate utf8mb4_general_ci) = 'A'", true},
		{"case when true then 'a' collate utf8mb4_general_ci else cast('bad' as signed) end = 'A'", true},
		{"('Á' collate utf8mb4_general_ci,1) in (('a',1),('z',2))", true},
		{"case when true then 'a' collate utf8mb4_general_ci else cast(_binary 0xff as char) collate utf8mb4_general_ci end = 'A'", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select "+tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(ctx, nil, nil, Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			folded, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(bound), proc, false, true)
			require.NoError(t, err)
			node := &pb.Node{ProjectList: []*Expr{DeepCopyExpr(bound)}}
			rule.NewConstantFold(false).Apply(node, nil, proc)
			for _, expr := range []*Expr{bound, folded, node.ProjectList[0]} {
				func() {
					executor, err := colexec.NewExpressionExecutor(proc, expr)
					require.NoError(t, err)
					defer executor.Free()
					value, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
					require.NoError(t, err)
					require.False(t, value.IsNull(0))
					require.Equal(t, tc.want, vector.MustFixedColNoTypeCheck[bool](value)[0])
				}()
			}
		})
	}
}
