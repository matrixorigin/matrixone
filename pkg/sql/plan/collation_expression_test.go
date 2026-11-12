// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBinaryCollatePreservesByteDomain(t *testing.T) {
	for _, tc := range []struct {
		name  string
		oid   types.T
		width int32
	}{
		{"binary", types.T_binary, 7},
		{"binary unknown width", types.T_binary, -1},
		{"varbinary", types.T_varbinary, 7},
		{"blob", types.T_blob, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := types.New(tc.oid, tc.width, 0)
			source := &Expr{Typ: makePlan2Type(&typ), Expr: &pb.Expr_Col{Col: &pb.ColRef{}}}
			before := source.Typ
			ast := &tree.CollateExpr{Expr: tree.NewUnresolvedName(tree.NewCStr("b", 1)), Collation: "binary"}
			impl := NewMockBinder(gomock.NewController(t))
			impl.EXPECT().BindExpr(ast.Expr, int32(0), false).Return(source, nil)
			binder := baseBinder{sysCtx: context.Background(), impl: impl}
			bound, err := binder.bindCollateExpr(ast, 0)
			require.NoError(t, err)
			require.Equal(t, before.Id, bound.Typ.Id)
			require.Equal(t, before.Width, bound.Typ.Width)
			require.Equal(t, before, source.Typ)
			require.Equal(t, "binary", ast.Collation)
			require.True(t, bound.Typ.CollationCoercibilitySet)
			require.Zero(t, bound.Typ.CollationCoercibility)
			if tc.width < 0 {
				return
			}
			for _, name := range []string{"bit_and", "bit_or", "bit_xor"} {
				aggregate, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*Expr{bound})
				require.NoError(t, err)
				expected := bound.Typ
				expected.NotNullable = true
				require.Equal(t, expected, aggregate.Typ)
			}
		})
	}
}

func TestBitwiseAggregateCollationProvenance(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"bit_and", "bit_or", "bit_xor"} {
		t.Run(name, func(t *testing.T) {
			for _, oid := range []types.T{types.T_binary, types.T_varbinary, types.T_blob} {
				t.Run(oid.String(), func(t *testing.T) {
					typ := oid.ToType()
					if oid == types.T_varbinary {
						typ.Width = 511
					}
					arg := &Expr{Typ: makePlan2Type(&typ), Expr: &pb.Expr_Col{Col: &pb.ColRef{}}}
					arg.Typ.CollationCoercibilitySet = true
					arg.Typ.CollationCoercibility = 0
					expected := arg.Typ
					result, err := BindFuncExprImplByPlanExpr(ctx, name, []*Expr{arg})
					require.NoError(t, err)
					require.Equal(t, expected, arg.Typ)
					expected.NotNullable = true
					require.Equal(t, expected, result.Typ)

					// Consuming a binary declaration may change representation,
					// but must not demote the aggregate's explicit collation.
					binder := baseBinder{sysCtx: ctx}
					consumed, err := binder.bindStringSourceDeclaration("=", []*Expr{result})
					require.NoError(t, err)
					consumedType := expected
					if oid == types.T_binary {
						consumedType.Id = int32(types.T_varbinary) // no fixed-width padding in the representation cast
					}
					require.Equal(t, consumedType, consumed[0].Typ)
					require.Equal(t, expected, result.Typ, "the source declaration remains immutable")
				})
			}
			text := makePlan2StringConstExprWithType("3")
			text.Typ.CollationCoercibilitySet = true
			text.Typ.CollationCoercibility = 4
			result, err := BindFuncExprImplByPlanExpr(ctx, name, []*Expr{text})
			require.NoError(t, err)
			require.Equal(t, int32(types.T_uint64), result.Typ.Id)
			require.False(t, result.Typ.CollationCoercibilitySet)
			for _, args := range [][]*Expr{nil, {text, text}} {
				_, err := BindFuncExprImplByPlanExpr(ctx, name, args)
				require.Error(t, err)
			}
		})
	}
}

func TestExplicitCollationConflicts(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	left := "_utf8mb4'a' collate utf8mb4_bin"
	right := "_utf8mb4'b' collate utf8mb4_general_ci"
	for _, sql := range []string{
		"select " + left + " = " + right,
		"select concat(" + left + "," + right + ")",
		"select case when true then " + left + " else " + right + " end",
		"select coalesce(" + left + "," + right + ")",
		"select least(" + left + "," + right + ")",
		"select " + left + " union all select " + right,
		"select '5' in ('5' collate utf8mb4_bin, '7' collate utf8mb4_general_ci)",
		"select '5' collate utf8mb4_bin in ('7' collate utf8mb4_general_ci, 5)",
		"select '5' between '0' collate utf8mb4_bin and '9' collate utf8mb4_general_ci",
		"select cast(null as char) between '0' collate utf8mb4_bin and '9' collate utf8mb4_general_ci",
		"values row(" + left + "), row(" + right + ")",
		"select least('a' collate utf8mb4_bin,'bb') = 'a' collate utf8mb4_general_ci",
		"select greatest('a' collate utf8mb4_bin,'bb') = 'bb' collate utf8mb4_general_ci",
		"select least('a' collate utf8mb4_bin,'b') = 'a' collate utf8mb4_general_ci",
		"select name_const('x','a' collate utf8mb4_unicode_ci) = 'a' collate utf8mb4_bin",
		"select bit_and(substring(cast(X'03' as binary(1)),1,511) collate binary) = (_utf8mb4'a' collate utf8mb4_bin)",
		"select bit_or(substring(cast(X'03' as binary(1)),1,511) collate binary) = (_utf8mb4'a' collate utf8mb4_bin)",
		"select bit_xor(substring(cast(X'03' as binary(1)),1,511) collate binary) = (_utf8mb4'a' collate utf8mb4_bin)",
		"select least(coalesce(cast('a' as char(3)) collate utf8mb4_bin,'a'),coalesce(cast('bb' as char(3)),'bb')) = 'a' collate utf8mb4_general_ci",
		"select greatest(coalesce(cast('a' as char(3)) collate utf8mb4_bin,'a'),coalesce(cast('bb' as char(3)),'bb')) = 'bb' collate utf8mb4_general_ci",
		"select least(ifnull(cast('a' as char(3)) collate utf8mb4_bin,'a'),ifnull(cast('b' as char(4)),'b')) = 'a' collate utf8mb4_general_ci",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(ctx, stmt, false, nil)
			require.Error(t, err)
			var sqlErr *moerr.Error
			require.ErrorAs(t, err, &sqlErr)
			require.Equal(t, uint16(1267), sqlErr.MySQLCode())
		})
	}
}

func TestExpressionCollationAdmission(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		code uint16
	}{
		{"select 'a' collate nonexistent_ci", 1273},
		{"select _binary'a' collate utf8mb4_unicode_ci", 1253},
		{"select null collate utf8mb4_unicode_ci", 1253},
		{"select 'a' collate ascii_bin", 1105},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			_, err = NewDefaultBinder(t.Context(), nil, nil, Type{}, nil).BindExpr(ast, 0, false)
			var sqlErr *moerr.Error
			require.ErrorAs(t, err, &sqlErr)
			require.Equal(t, tc.code, sqlErr.MySQLCode())
		})
	}
}

func TestExpressionCollationAssignmentAdmission(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	build := func(sql string) (*Plan, error) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		return BuildPlan(compiler, stmt, false, nil)
	}
	created, err := build("create table collation_assign(id int primary key,t varchar(4),b varbinary(4))")
	require.NoError(t, err)
	table := created.GetDdl().GetCreateTable().TableDef
	table.Cols = append(table.Cols, &ColDef{Name: catalog.Row_ID, Hidden: true, Typ: Type{Id: int32(types.T_Rowid)}})
	compiler.tables["collation_assign"] = table
	compiler.objects["collation_assign"] = &ObjectRef{ObjName: "collation_assign", SchemaName: "tpch"}
	compiler.isDml = true
	for _, operation := range []string{"insert", "replace"} {
		for _, destination := range []string{"t", "b"} {
			for _, tc := range []struct {
				expression string
				valid      bool
			}{
				{"_binary'a' collate 'binary'", true},
				{"'a' collate utf8mb4_unicode_ci", true},
				{"cast(_binary'a' as char(4)) collate utf8mb4_unicode_ci", true},
				{"_binary'a' collate utf8mb4_unicode_ci", false},
				{"'a' collate 'binary'", false},
			} {
				sql := operation + " into collation_assign(id," + destination + ") values(1," + tc.expression + ")"
				t.Run(sql, func(t *testing.T) {
					_, err := build(sql)
					if tc.valid {
						require.NoError(t, err)
					} else {
						var sqlErr *moerr.Error
						require.ErrorAs(t, err, &sqlErr)
						require.Equal(t, uint16(1253), sqlErr.MySQLCode())
					}
				})
			}
		}
	}
}

func TestExpressionCollationUTF8CharsetAdmission(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	build := func(sql string) (*Plan, error) {
		t.Helper()
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		return BuildPlan(compiler, stmt, false, nil)
	}
	created, err := build("create table collation_charset(s3 varchar(4) collate utf8_unicode_ci,s4 varchar(4) collate utf8mb4_unicode_ci,legacy varchar(4))")
	require.NoError(t, err)
	compiler.tables["collation_charset"] = created.GetDdl().GetCreateTable().TableDef
	compiler.objects["collation_charset"] = &ObjectRef{ObjName: "collation_charset", SchemaName: "tpch"}
	original := proto.Clone(compiler.tables["collation_charset"])
	for _, tc := range []struct {
		sql   string
		valid bool
	}{
		{"select s3 collate utf8_unicode_ci from collation_charset", true},
		{"select s3 collate utf8mb3_unicode_ci from collation_charset", true},
		{"select s4 collate utf8mb4_unicode_ci from collation_charset", true},
		{"select _utf8mb4'a' collate utf8mb4_unicode_ci", true},
		{"select 1 collate utf8_unicode_ci", true},
		{"prepare p from 'select ? collate utf8_unicode_ci'", true},
		{"select s3 collate utf8mb4_unicode_ci from collation_charset", false},
		{"select s4 collate utf8_unicode_ci from collation_charset", false},
		{"select _utf8mb4'a' collate utf8_unicode_ci", false},
		{"select 'a' collate utf8mb3_unicode_ci", false},
		{"select s3 collate utf8_general_ci from collation_charset", false},
		{"select _binary'a' collate utf8_bin", false},
		{"select null collate utf8_general_ci", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := build(tc.sql)
			if tc.valid {
				require.NoError(t, err)
			} else {
				var sqlErr *moerr.Error
				require.ErrorAs(t, err, &sqlErr)
				require.Equal(t, uint16(1253), sqlErr.MySQLCode())
			}
		})
	}
	for _, alias := range []struct {
		name     string
		identity uint32
	}{
		{"utf8_general_ci", uint32(types.CharsetUTF8)},
		{"utf8mb3_general_ci", uint32(types.CharsetUTF8)},
		{"utf8_bin", uint32(types.CharsetUTF8MB4Bin)},
		{"utf8mb3_bin", uint32(types.CharsetUTF8MB4Bin)},
	} {
		for _, source := range []string{"'a'", "legacy", "s4"} {
			sql := "select " + source + " collate " + alias.name + " from collation_charset"
			t.Run(sql, func(t *testing.T) {
				p, err := build(sql)
				require.NoError(t, err)
				query := p.GetQuery()
				expr := query.Nodes[query.Steps[0]].ProjectList[0]
				require.Equal(t, alias.identity, expr.Typ.Charset)
				require.Zero(t, expr.Typ.CollationVersion)
				require.True(t, expr.Typ.CollationCoercibilitySet)
				require.Zero(t, expr.Typ.CollationCoercibility)
			})
		}
	}
	require.True(t, proto.Equal(original, compiler.tables["collation_charset"]), "COLLATE admission must not relabel the source columns")
}

func TestExpressionCollationAssignmentHintRestored(t *testing.T) {
	typ := types.New(types.T_varchar, 4, 0).PlanType()
	for _, original := range []bool{false, true} {
		binder := NewDefaultBinder(t.Context(), nil, nil, typ, nil)
		binder.suppressDefaultValueBindType = original
		bind := func(input string) (*Expr, error) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select "+input, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			return binder.BindExpr(ast, 0, false)
		}
		for _, tc := range []struct {
			input string
			valid bool
		}{
			{"_binary'a' collate 'binary'", true},
			{"(_binary'a' collate 'binary') collate 'binary'", true},
			{"_binary'a' collate utf8mb4_unicode_ci", false},
		} {
			_, err := bind(tc.input)
			if tc.valid {
				require.NoError(t, err)
			} else {
				var sqlErr *moerr.Error
				require.ErrorAs(t, err, &sqlErr)
				require.Equal(t, uint16(1253), sqlErr.MySQLCode())
			}
			require.Equal(t, original, binder.suppressDefaultValueBindType)
		}
		_, err := bind("nonexistent_collation_function('a') collate utf8mb4_unicode_ci")
		require.Error(t, err)
		require.Equal(t, original, binder.suppressDefaultValueBindType)
		plain, err := bind("_binary'z'")
		require.NoError(t, err)
		if original {
			require.Equal(t, int32(types.T_varbinary), plain.Typ.Id)
		} else {
			require.Equal(t, typ.Id, plain.Typ.Id)
			require.Equal(t, typ.Width, plain.Typ.Width)
		}
	}
}

func TestExpressionCollationComparisonCanonicalization(t *testing.T) {
	for _, tc := range []struct {
		name string
		oid  types.T
		cast func(context.Context, *Expr) (*Expr, error)
	}{
		{"promoted_pad_space", types.T_varchar, appendPadSpaceComparisonCastIfNeeded},
		{"char_window_key", types.T_char, appendPadSpaceWindowKeyCastIfNeeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := types.NewWithCharset(tc.oid, 8, 0, types.CharsetUTF8MB4Bin).PlanType()
			typ.PadSpace = true
			expressionCollation{charset: typ.Charset, rank: 1, conflict: true}.apply(&typ)
			source := &Expr{Typ: typ, Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 0}}}
			original := DeepCopyExpr(source)
			bound, err := tc.cast(t.Context(), source)
			require.NoError(t, err)
			require.True(t, isCastOverload(bound, 2))
			require.False(t, bound.Typ.PadSpace)
			require.Equal(t, typ.CollationCoercibility, bound.Typ.CollationCoercibility)
			require.True(t, bound.Typ.CollationCoercibilitySet)
			require.True(t, bound.Typ.CollationMergeConflict)
			require.True(t, proto.Equal(original, source), "canonicalizing a shared source must not relabel it")

			// A caller's resolved target wins; the fallback only fills provenance
			// lost when converting a runtime type back to a plan type.
			selected := bound.Typ
			selected.CollationCoercibility, selected.CollationMergeConflict = 0, false
			rebound, err := appendComparisonCastBeforeExpr(t.Context(), source, selected)
			require.NoError(t, err)
			require.Zero(t, rebound.Typ.CollationCoercibility)
			require.False(t, rebound.Typ.CollationMergeConflict)
			toNumber := types.T_int64.ToType().PlanType()
			numeric, err := appendComparisonCastBeforeExpr(t.Context(), source, toNumber)
			require.NoError(t, err)
			require.False(t, numeric.Typ.CollationCoercibilitySet)
			require.True(t, proto.Equal(original, source))
		})
	}
}

func TestExpressionCollationPreparedParameterConstraint(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select (?) collate utf8mb4_unicode_ci = ?", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	proc := newPlanTestProcess(t)
	for _, sourceType := range []types.Type{types.T_text.ToType(), types.T_varbinary.ToType(), types.T_any.ToType()} {
		ctx := withPreparedSourceBindings(t.Context(), []PreparedSourceBinding{
			{Position: 0, Type: sourceType}, {Position: 1, Type: types.T_text.ToType()},
		})
		bound, err := NewDefaultBinder(ctx, nil, nil, Type{}, nil).BindExpr(ast, 0, false)
		require.NoError(t, err)
		for _, arg := range bound.GetF().Args {
			require.Equal(t, uint32(types.CharsetUTF8MB4UnicodeCI), arg.Typ.Charset)
			require.Equal(t, uint32(1), arg.Typ.CollationVersion)
		}
		values := []any{ParamValue{Value: "Á", IsBin: sourceType.Charset == types.CharsetBinary}, ParamValue{Value: "a"}}
		if sourceType.Oid == types.T_any {
			values[0] = nil
		}
		reusable := &Plan{Plan: &pb.Plan_Query{Query: &pb.Query{
			Steps: []int32{0}, Nodes: []*pb.Node{{NodeType: pb.Node_VALUE_SCAN, ProjectList: []*Expr{bound}}},
		}}}
		original := proto.Clone(reusable)
		filled, err := FillValuesOfParamsInPlan(ctx, reusable, values)
		require.NoError(t, err)
		require.True(t, proto.Equal(original, reusable))
		executor, err := colexec.NewExpressionExecutor(proc, filled.GetQuery().Nodes[0].ProjectList[0])
		require.NoError(t, err)
		func() {
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			if sourceType.Oid == types.T_any {
				require.True(t, result.IsConstNull())
			} else {
				require.True(t, vector.MustFixedColNoTypeCheck[bool](result)[0])
			}
		}()
	}
}

func TestExpressionCollationDerivedMetadata(t *testing.T) {
	for _, sql := range []string{
		"select concat('a','b')",
		"select case when true then 'a' else 'b' end",
		"select coalesce('a','b')",
		"select lower('a')",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(t.Context(), nil, nil, Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			require.True(t, bound.Typ.CollationCoercibilitySet)
			require.Equal(t, uint32(4), bound.Typ.CollationCoercibility)
			require.Zero(t, bound.Typ.CollationVersion)
		})
	}
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	for _, sql := range []string{
		"select 'Á' collate utf8mb4_unicode_ci as v union all select 'a'",
		"values row('Á' collate utf8mb4_unicode_ci), row('a')",
		"select ? collate utf8mb4_unicode_ci = ?",
	} {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		built, err := BuildPlan(ctx, stmt, true, nil)
		stmt.Free()
		require.NoError(t, err)
		encoded, err := proto.Marshal(built)
		require.NoError(t, err)
		var transported pb.Plan
		require.NoError(t, proto.Unmarshal(encoded, &transported))
		require.True(t, proto.Equal(built, &transported), sql)
		found := false
		for _, node := range transported.GetQuery().Nodes {
			for _, expr := range node.ProjectList {
				require.NoError(t, pb.VisitExprTree(expr, func(e *Expr) error {
					if e.Typ.CollationCoercibilitySet && e.Typ.CollationCoercibility == 0 {
						found = true
						require.Equal(t, uint32(types.CharsetUTF8MB4UnicodeCI), e.Typ.Charset)
						require.Equal(t, uint32(1), e.Typ.CollationVersion)
					}
					return nil
				}))
			}
		}
		require.True(t, found, sql)
	}
}

func TestExpressionCollationSubqueryProvenance(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	native := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
	native.CollationVersion = 1
	compiler.tables["collation_scalar"] = &TableDef{Name: "collation_scalar", Cols: []*ColDef{
		{Name: "s", Typ: native.PlanType()},
	}}
	compiler.objects["collation_scalar"] = &ObjectRef{ObjName: "collation_scalar", SchemaName: "tpch"}
	for _, tc := range []struct {
		sql  string
		rank uint32
	}{
		{"select (select s from collation_scalar)", 2},
		{"select (select 'a')", 4},
		{"select (select 'a' collate utf8mb4_unicode_ci)", 0},
		{"select (select cast('a' collate utf8mb4_unicode_ci as char(20)))", 2},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			builder := NewQueryBuilder(pb.Query_SELECT, compiler, false, true)
			ctx := NewBindContext(builder, nil)
			binder := NewProjectionBinder(builder, ctx, nil)
			ctx.binder = binder
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			sub, err := binder.BindExpr(ast, 0, false)
			require.NoError(t, err)
			require.NotNil(t, sub.GetSub())
			require.True(t, sub.Typ.CollationCoercibilitySet)
			require.Equal(t, tc.rank, sub.Typ.CollationCoercibility)
			if tc.rank == 2 && sub.Typ.CollationVersion == 1 {
				bound, err := BindFuncExprImplByPlanExpr(t.Context(), "=", []*Expr{sub, makePlan2StringConstExprWithType("a")})
				require.NoError(t, err)
				for _, arg := range bound.GetF().Args {
					require.Equal(t, uint32(types.CharsetUTF8MB4UnicodeCI), arg.Typ.Charset)
					require.Equal(t, uint32(1), arg.Typ.CollationVersion)
				}
			}
		})
	}
}

func TestExpressionCollationPromotionProvenance(t *testing.T) {
	for _, name := range []string{"least", "greatest"} {
		for _, right := range []string{"b", "bb"} {
			sql := "select " + name + "('a' collate utf8mb4_bin,'" + right + "')"
			t.Run(sql, func(t *testing.T) {
				stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
				bound, err := NewDefaultBinder(t.Context(), nil, nil, Type{}, nil).BindExpr(ast, 0, false)
				require.NoError(t, err)
				require.True(t, bound.Typ.CollationCoercibilitySet)
				require.Zero(t, bound.Typ.CollationCoercibility)
				for _, arg := range bound.GetF().Args {
					require.True(t, arg.Typ.CollationCoercibilitySet)
					require.Zero(t, arg.Typ.CollationCoercibility)
				}
			})
		}
	}
}

func TestExpressionCollationWindowTraversal(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"select lag('a') over () collate utf8mb4_unicode_ci, ? collate utf8mb4_unicode_ci", 1)
	require.NoError(t, err)
	defer stmt.Free()
	items := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs
	name, found := findNestedWindowFuncName(items[0].Expr)
	require.True(t, found)
	require.Equal(t, "lag", name)
	require.True(t, rollupWindowExprContainsWindow(items[0].Expr))
	require.True(t, hasWindowFrameParam(items[1].Expr))
	require.False(t, hasWindowFrameParam(items[0].Expr))

	stmt, err = parsers.ParseOne(t.Context(), dialect.MYSQL,
		"select sum(n_nationkey) as s, rank() over (order by sum(n_nationkey)) collate utf8mb4_unicode_ci as r "+
			"from nation group by n_regionkey with rollup", 1)
	require.NoError(t, err)
	defer stmt.Free()
	optimizer := NewMockOptimizer(true, newPlanTestProcess(t))
	built, err := BuildPlan(optimizer.CurrentContext(), stmt, false, nil)
	require.NoError(t, err)
	require.Equal(t, 1, countReachableNodeType(built.GetQuery(), pb.Node_WINDOW))
}

func TestExpressionCollationSemantics(t *testing.T) {
	proc := newPlanTestProcess(t)
	for _, tc := range []struct {
		input string
		want  string
	}{
		{"'Á' collate utf8mb4_unicode_ci = 'a'", "true"},
		{"'a' = 'Á' collate utf8mb4_unicode_ci", "true"},
		{"'a ' collate utf8mb4_unicode_ci = 'a'", "true"},
		{"_binary'a ' = _binary'a'", "false"},
		{"'Á' collate utf8mb4_unicode_ci in ('a','z')", "true"},
		{"'Á' collate utf8mb4_unicode_ci between 'a' and 'b'", "true"},
		{"5 between '0' collate utf8mb4_bin and '9' collate utf8mb4_general_ci", "true"},
		{"10 between '0' collate utf8mb4_bin and '9' collate utf8mb4_general_ci", "false"},
		{"'5' collate utf8mb4_bin between '0' collate utf8mb4_general_ci and 9", "true"},
		{"5 in ('5' collate utf8mb4_bin, '7' collate utf8mb4_general_ci)", "true"},
		{"5 not in ('5' collate utf8mb4_bin, '7' collate utf8mb4_general_ci)", "false"},
		{"5 in ('4' collate utf8mb4_bin, '7' collate utf8mb4_general_ci)", "false"},
		{"cast(null as signed) between '0' collate utf8mb4_bin and '9' collate utf8mb4_general_ci", "NULL"},
		{"least('a' collate utf8mb4_unicode_ci,'B')", "a"},
		{"greatest('a' collate utf8mb4_unicode_ci,'B')", "B"},
		{"concat('a' collate utf8mb4_unicode_ci,'B')", "aB"},
		{"cast('Áz' as char(1) character set utf8mb4) collate utf8mb4_unicode_ci = 'a'", "true"},
		{"convert('Áz', char(1) character set utf8mb3) collate utf8mb4_unicode_ci = 'a'", "true"},
		{"cast('A' collate utf8mb4_unicode_ci as char(1) character set binary) = _binary'a'", "false"},
		{"convert('A' collate utf8mb4_unicode_ci, char character set binary) = _binary'a'", "false"},
		{"coalesce(NULL,'a' collate utf8mb4_unicode_ci) = 'A'", "true"},
		{"case when true then 'a' collate utf8mb4_unicode_ci else cast('bad' as signed) end = 'A'", "true"},
		{"lower('Á' collate utf8mb4_unicode_ci) = 'a'", "true"},
		{"1 collate utf8mb4_unicode_ci = '1'", "true"},
		{"_binary'Á' = 'a' collate utf8mb4_unicode_ci", "true"},
		{"'Á' collate utf8mb4_unicode_ci <=> 'a'", "true"},
		{"'Á' collate utf8mb4_unicode_ci <> 'a'", "false"},
		{"'Á' collate utf8mb4_unicode_ci <= 'a'", "true"},
		{"'Á' collate utf8mb4_unicode_ci >= 'a'", "true"},
		{"'Á' collate utf8mb4_unicode_ci < 'b'", "true"},
		{"'z' collate utf8mb4_unicode_ci > 'a'", "true"},
		{"'Á' collate utf8mb4_unicode_ci not in ('a','z')", "false"},
		{"('Á' collate utf8mb4_unicode_ci,1) in (('a',1),('z',2))", "true"},
		{"if(true,'Á' collate utf8mb4_unicode_ci,'z') = 'a'", "true"},
		{"ifnull('Á' collate utf8mb4_unicode_ci,'z') = 'a'", "true"},
		{"concat_ws('-', 'Á' collate utf8mb4_unicode_ci,'b') = 'a-b'", "true"},
		// Issue #31175: pattern operators honor the resolved collation.
		{"'a' like 'A'", "true"},
		{"'ABC' like 'abc'", "true"},
		{"'ABC' like 'a%'", "true"},
		{"'a' not like 'A'", "false"},
		{"'abc' regexp 'ABC'", "true"},
		{"'abc' regexp '[A-Z]+'", "true"},
		{"'Apple' like 'apple' collate utf8mb4_unicode_ci", "true"},
		{"'Apple' regexp 'apple' collate utf8mb4_unicode_ci", "true"},
		{"'a' like 'A' collate utf8mb4_bin", "false"},
		{"'abc' regexp 'ABC' collate utf8mb4_bin", "false"},
		{"_binary'a' like _binary'A'", "false"},
		{"_binary'abc' regexp _binary'ABC'", "false"},
		// Issue #31175 review: U+03A3/U+03C3/U+03C2 share one fold orbit
		// under a ci collation, matching LOCATE/INSTR/POSITION and MySQL's
		// utf8mb4_general_ci weight 0x03A3.
		{"'ς' like 'Σ'", "true"},
		{"'σ' like 'ς'", "true"},
		{"'Σ' like 'σ'", "true"},
		{"'ς' regexp 'Σ'", "true"},
		{"'σ' regexp 'ς'", "true"},
		{"find_in_set('ς','σ,Σ') = 1", "true"},
		// Issue #31175 review: MySQL aggregates every LIKE argument, ESCAPE
		// included, so a COLLATE on the ESCAPE character wins the resolution
		// and the comparison turns case-sensitive. A REGEXP_LIKE match_type
		// never participates (MySQL resolves it from the subject and pattern
		// only), so a COLLATE there can neither conflict nor win.
		{"'a' like 'A' escape ('!' collate utf8mb4_bin)", "false"},
		{"'a' like 'A' escape ('!' collate utf8mb4_general_ci)", "true"},
		{"regexp_like('a' collate utf8mb4_unicode_ci, 'A', 'i' collate utf8mb4_bin)", "true"},
		{"regexp_like('a' collate utf8mb4_unicode_ci, 'A', 'm' collate utf8mb4_bin)", "true"},
		// Issue #31175 review: LIKE compares one pattern character against
		// one value character by the resolved collation's weights
		// (my_wildcmp_uca). utf8mb4_general_ci folds accented and dotless-i
		// pairs (á=a, ı=I, ß=s); utf8mb4_unicode_ci compares UCA 4.0.0
		// primary weight streams (á=a) while ı keeps its own weight and æ
		// is not ae. LIKE never spans an expansion across characters, so
		// under unicode_ci ß matches neither s nor ss, unlike FIND_IN_SET
		// and =, which compare whole collation keys. REGEXP keeps its
		// case-only fold, so accented letters stay distinct there.
		{"'á' like 'a'", "true"},
		{"'áb' like 'a_'", "true"},
		{"'ı' like 'I'", "true"},
		{"'ß' like 's'", "true"},
		{"'ß' like 'ss'", "false"},
		{"'á' like 'a' collate utf8mb4_unicode_ci", "true"},
		{"'ı' like 'I' collate utf8mb4_unicode_ci", "false"},
		{"'ß' like 'ss' collate utf8mb4_unicode_ci", "false"},
		{"'ss' like 'ß' collate utf8mb4_unicode_ci", "false"},
		{"'ß' like 's' collate utf8mb4_unicode_ci", "false"},
		{"'œ' like 'oe' collate utf8mb4_unicode_ci", "false"},
		{"'æ' like 'ae' collate utf8mb4_unicode_ci", "false"},
		{"'ß' like '_' collate utf8mb4_unicode_ci", "true"},
		{"'ß' like '__' collate utf8mb4_unicode_ci", "false"},
		{"'xßy' like '%ss%' collate utf8mb4_unicode_ci", "false"},
		{"'xßy' like '%s%' collate utf8mb4_unicode_ci", "false"},
		{"'xssy' like '%ss%' collate utf8mb4_unicode_ci", "true"},
		{"'á' regexp 'a'", "false"},
		{"'á' regexp 'a' collate utf8mb4_unicode_ci", "false"},
		{"find_in_set('á','a,b') = 1", "true"},
		{"find_in_set('ß','ss,x') = 0", "true"},
		{"find_in_set('ß','ss,x' collate utf8mb4_unicode_ci) = 1", "true"},
		{"find_in_set('ı','I,x') = 1", "true"},
		{"find_in_set('ı','I,x' collate utf8mb4_unicode_ci) = 0", "true"},
		// Issue #31175 review: ILIKE keeps its complete Unicode simple-fold
		// equivalence on every domain: the Kelvin and Angstrom signs fold
		// with their letters, and the sigma orbit stays together.
		{"'K' ilike 'k'", "true"},
		{"'K' ilike 'K'", "true"},
		{"'Å' ilike 'å'", "true"},
		{"'Å' ilike 'Å'", "true"},
		{"'ς' ilike 'Σ'", "true"},
		{"'xKz' ilike '%k%'", "true"},
		{"'K_k' ilike 'k!_k' escape '!'", "true"},
		// Issue #31175 review (values probed on a MySQL 8.0.44 oracle): an
		// explicit COLLATE wins coercibility aggregation over a binary string
		// operand in either operand order, so LIKE and FIND_IN_SET compare the
		// converted binary value under that collation, exactly as the =
		// comparator above does. With no explicit collation the binary operand
		// keeps the byte-wise test, and an explicit utf8mb4_bin keeps it too.
		// The equal-coercibility tie keeps MySQL's byte-wise rule in either
		// operand order: only a strictly stronger (explicit or implicit)
		// collation converts the binary literal's domain.
		{"'abc' like _binary'ABC%'", "false"},
		{"find_in_set('A',_binary'a,b') = 0", "true"},
		{"_binary'abc' like ('ABC%' collate utf8mb4_general_ci)", "true"},
		{"('abc' collate utf8mb4_general_ci) like _binary'ABC%'", "true"},
		{"_binary'abc' like ('ABC%' collate utf8mb4_unicode_ci)", "true"},
		{"('abc' collate utf8mb4_unicode_ci) like _binary'ABC%'", "true"},
		{"_binary'abc' like 'ABC%'", "false"},
		{"_binary'abc' like ('ABC%' collate utf8mb4_bin)", "false"},
		{"_binary'abc' like ('abc%' collate utf8mb4_general_ci)", "true"},
		{"_binary'xbc' like ('ABC%' collate utf8mb4_general_ci)", "false"},
		{"find_in_set(_binary'A','a,b') = 0", "true"},
		{"find_in_set(_binary'A','a,b' collate utf8mb4_general_ci) = 1", "true"},
		{"find_in_set('A' collate utf8mb4_general_ci, _binary'a,b') = 1", "true"},
		{"find_in_set(_binary'A','a,b' collate utf8mb4_unicode_ci) = 1", "true"},
		{"find_in_set('A' collate utf8mb4_unicode_ci, _binary'a,b') = 1", "true"},
		{"find_in_set(_binary'a','a,b' collate utf8mb4_general_ci) = 1", "true"},
		{"find_in_set(_binary'z','a,b' collate utf8mb4_general_ci) = 0", "true"},
		{"find_in_set(_binary'A','A,b' collate utf8mb4_bin) = 1", "true"},
		// A hexadecimal string literal carries the same binary string domain
		// as a _binary introducer, and the MySQL 8.0.44 oracle gives it the
		// same treatment: an explicit ci COLLATE folds, no COLLATE stays
		// byte-wise.
		{"0x616263 like ('ABC%' collate utf8mb4_general_ci)", "true"},
		{"0x616263 like 'ABC%'", "false"},
		{"find_in_set(0x41,'a,b' collate utf8mb4_general_ci) = 1", "true"},
		{"find_in_set(0x41,'a,b') = 0", "true"},
		{"find_in_set('a' collate utf8mb4_unicode_ci, 'A,b') = 1", "true"},
		// A CAST(x AS BINARY) expression carries the same binary string
		// domain as a _binary literal, and the MySQL 8.0.44 oracle applies
		// the same coercibility rule: an explicit ci COLLATE on either
		// operand wins strictly and folds, an equal-coercibility tie or an
		// explicit utf8mb4_bin stays byte-wise. The conversion cast that
		// delivers the value must preserve it intact (a T_binary source
		// carries width -1, which must not truncate) and the folded result
		// must keep the selected text domain instead of reclaiming the
		// CAST's binary provenance (issue #31175 round-21 review).
		{"cast('abc' as binary) like ('ABC%' collate utf8mb4_general_ci)", "true"},
		{"('abc' collate utf8mb4_general_ci) like cast('ABC%' as binary)", "true"},
		{"cast('abc' as binary) like ('ABC%' collate utf8mb4_unicode_ci)", "true"},
		{"('abc' collate utf8mb4_unicode_ci) like cast('ABC%' as binary)", "true"},
		{"cast('abc' as binary) like 'ABC%'", "false"},
		{"cast('abc' as binary) like ('ABC%' collate utf8mb4_bin)", "false"},
		{"cast('abc' as binary) like ('abc%' collate utf8mb4_general_ci)", "true"},
		{"cast('xyz' as binary) like ('ABC%' collate utf8mb4_general_ci)", "false"},
		{"find_in_set(cast('A' as binary),'a,b' collate utf8mb4_general_ci) = 1", "true"},
		{"find_in_set('A' collate utf8mb4_general_ci,cast('a,b' as binary)) = 1", "true"},
		{"find_in_set(cast('A' as binary),'a,b' collate utf8mb4_unicode_ci) = 1", "true"},
		{"find_in_set('A' collate utf8mb4_unicode_ci,cast('a,b' as binary)) = 1", "true"},
		{"find_in_set(cast('A' as binary),'a,b') = 0", "true"},
		{"find_in_set(cast('a' as binary),'a,b' collate utf8mb4_general_ci) = 1", "true"},
		{"find_in_set(cast('z' as binary),'a,b' collate utf8mb4_general_ci) = 0", "true"},
		{"find_in_set(cast('a' as binary),'a,b' collate utf8mb4_bin) = 1", "true"},
		// Equality and IN over the CAST shape behave exactly as their pure
		// literal counterparts: utf8mb4_unicode_ci folds, while explicit
		// utf8mb4_general_ci on literals keeps the pre-existing equality
		// parity gap ('ALICE' = 'alice' COLLATE utf8mb4_general_ci is 0 on
		// the base commit as well; that gap is scoped to a follow-up issue
		// and is unchanged by this repair).
		{"cast('ALICE' as binary) = ('Alice' collate utf8mb4_general_ci)", "false"},
		{"('Alice' collate utf8mb4_general_ci) = cast('ALICE' as binary)", "false"},
		{"cast('ALICE' as binary) = ('Alice' collate utf8mb4_unicode_ci)", "true"},
		{"('Alice' collate utf8mb4_unicode_ci) = cast('ALICE' as binary)", "true"},
		{"cast('ALICE' as binary) = 'Alice'", "false"},
		{"cast('ALICE' as binary) = ('Alice' collate utf8mb4_bin)", "false"},
		{"cast('ALICE' as binary) in ('Alice' collate utf8mb4_general_ci)", "false"},
		{"cast('ALICE' as binary) in ('Alice')", "false"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select "+tc.input, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(t.Context(), nil, nil, Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			folded, err := ConstantFold(batch.EmptyForConstFoldBatch, bound, proc, false, true)
			require.NoError(t, err)
			// IN and deferred CASE can deliberately remain executable after
			// folding. Both forms must produce the same independent SQL oracle.
			for _, expression := range []*Expr{bound, folded} {
				func() {
					executor, err := colexec.NewExpressionExecutor(proc, expression)
					require.NoError(t, err)
					defer executor.Free()
					value, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
					require.NoError(t, err)
					if tc.want == "NULL" {
						require.True(t, value.IsNull(0))
					} else if tc.want == "true" || tc.want == "false" {
						require.Equal(t, tc.want == "true", vector.MustFixedColNoTypeCheck[bool](value)[0])
					} else {
						require.Equal(t, tc.want, string(value.GetBytesAt(0)))
					}
				}()
			}
		})
	}
}

// Issue #31175 review: a _binary introducer mixed with an explicit text
// collation keeps MySQL's error 3995 rejection for REGEXP. Collation
// normalization must not convert the binary operand away before the regexp
// compatibility check can see its original domain, in either operand order.
func TestRegexpBinaryIntroducerKeepsCharsetMismatch(t *testing.T) {
	for _, input := range []string{
		"_binary'abc' regexp ('ABC' collate utf8mb4_general_ci)",
		"('ABC' collate utf8mb4_general_ci) regexp _binary'abc'",
		"_binary'abc' regexp ('ABC' collate utf8mb4_unicode_ci)",
		"('ABC' collate utf8mb4_unicode_ci) regexp _binary'abc'",
		"regexp_like(_binary'abc', 'ABC' collate utf8mb4_general_ci)",
		"regexp_like('ABC' collate utf8mb4_unicode_ci, _binary'abc')",
		// A CAST(x AS BINARY) operand is the same binary/ci mixture and is
		// rejected identically in both operand orders (MySQL 8.0.44 parity).
		"cast('abc' as binary) regexp ('ABC' collate utf8mb4_general_ci)",
		"('ABC' collate utf8mb4_general_ci) regexp cast('abc' as binary)",
		"cast('abc' as binary) regexp 'ABC'",
		"'abc' regexp cast('ABC' as binary)",
		"regexp_like(cast('abc' as binary), 'ABC' collate utf8mb4_general_ci)",
		"regexp_like('ABC' collate utf8mb4_general_ci, cast('abc' as binary))",
	} {
		t.Run(input, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select "+input, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			_, err = NewDefaultBinder(t.Context(), nil, nil, Type{}, nil).BindExpr(ast, 0, false)
			require.Error(t, err)
			require.Contains(t, err.Error(), "cannot be used in conjunction")
		})
	}
}

// Issue #31175 review: the selected comparison collation must reach the
// pattern matchers even when every operand is an implicit version-zero
// column. With a utf8mb4_general_ci column and a utf8mb4_bin column the
// resolver selects the _bin collation, so LIKE, REGEXP and FIND_IN_SET must
// compare case-sensitively in either operand order instead of folding from
// whichever operand they happen to read. Every other function keeps its
// legacy operand identities for exactly this operand mix.
func TestPatternConsumersDeliverSelectedCollation(t *testing.T) {
	proc := newPlanTestProcess(t)
	generalType := types.NewWithCharset(types.T_varchar, 30, 0, types.CharsetUTF8)
	binType := types.NewWithCharset(types.T_varchar, 30, 0, types.CharsetUTF8MB4Bin)
	newCol := func(typ types.Type, pos int32) *Expr {
		return &Expr{Typ: typ.PlanType(), Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: pos}}}
	}
	for _, tc := range []struct {
		name string
		fn   string
		args []*Expr
	}{
		{"like ci subject", "like", []*Expr{newCol(generalType, 0), newCol(binType, 1)}},
		{"like bin subject", "like", []*Expr{newCol(binType, 1), newCol(generalType, 0)}},
		{"regexp ci subject", "reg_match", []*Expr{newCol(generalType, 0), newCol(binType, 1)}},
		{"regexp bin subject", "reg_match", []*Expr{newCol(binType, 1), newCol(generalType, 0)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound, err := BindFuncExprImplByPlanExpr(t.Context(), tc.fn, tc.args)
			require.NoError(t, err)
			for _, arg := range bound.GetF().Args {
				require.Equal(t, uint32(types.CharsetUTF8MB4Bin), arg.Typ.Charset,
					"the selected _bin collation must be delivered to %s", tc.fn)
			}
			executor, err := colexec.NewExpressionExecutor(proc, bound)
			require.NoError(t, err)
			defer executor.Free()
			bat := batch.NewWithSize(2)
			defer bat.Clean(proc.Mp())
			bat.Vecs[0] = vector.NewVec(generalType)
			bat.Vecs[1] = vector.NewVec(binType)
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("A"), false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("Alice"), false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("a"), false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("Alice"), false, proc.Mp()))
			bat.SetRowCount(2)
			result, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
			require.NoError(t, err)
			require.Equal(t, []bool{false, true}, vector.MustFixedColNoTypeCheck[bool](result),
				"the _bin selection compares case-sensitively")
		})
	}
	for _, tc := range []struct {
		name       string
		args       []*Expr
		generalVal string
		binVal     string
	}{
		{"find_in_set bin needle", []*Expr{newCol(binType, 1), newCol(generalType, 0)}, "a,b", "A"},
		{"find_in_set ci needle", []*Expr{newCol(generalType, 0), newCol(binType, 1)}, "A", "a,b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound, err := BindFuncExprImplByPlanExpr(t.Context(), "find_in_set", tc.args)
			require.NoError(t, err)
			for _, arg := range bound.GetF().Args {
				require.Equal(t, uint32(types.CharsetUTF8MB4Bin), arg.Typ.Charset,
					"the selected _bin collation must be delivered to find_in_set")
			}
			executor, err := colexec.NewExpressionExecutor(proc, bound)
			require.NoError(t, err)
			defer executor.Free()
			bat := batch.NewWithSize(2)
			defer bat.Clean(proc.Mp())
			bat.Vecs[0] = vector.NewVec(generalType)
			bat.Vecs[1] = vector.NewVec(binType)
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(tc.generalVal), false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(tc.binVal), false, proc.Mp()))
			bat.SetRowCount(1)
			result, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
			require.NoError(t, err)
			require.Equal(t, []int32{0}, vector.MustFixedColNoTypeCheck[int32](result),
				"the _bin selection keeps membership byte-wise")
		})
	}
	// The identical operand mix through a non-pattern function keeps its
	// legacy behavior: equality is deliberately outside the delivery. The
	// binder's own comparison merge unifies the operand types for "=", so
	// pin the observable behavior instead of the representation: this mix
	// compares case-sensitively in both operand orders.
	for _, tc := range []struct {
		name string
		args []*Expr
	}{
		{"equal ci left", []*Expr{newCol(generalType, 0), newCol(binType, 1)}},
		{"equal bin left", []*Expr{newCol(binType, 1), newCol(generalType, 0)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound, err := BindFuncExprImplByPlanExpr(t.Context(), "=", tc.args)
			require.NoError(t, err)
			executor, err := colexec.NewExpressionExecutor(proc, bound)
			require.NoError(t, err)
			defer executor.Free()
			bat := batch.NewWithSize(2)
			defer bat.Clean(proc.Mp())
			bat.Vecs[0] = vector.NewVec(generalType)
			bat.Vecs[1] = vector.NewVec(binType)
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("A"), false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("Alice"), false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("a"), false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("Alice"), false, proc.Mp()))
			bat.SetRowCount(2)
			result, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
			require.NoError(t, err)
			require.Equal(t, []bool{false, true}, vector.MustFixedColNoTypeCheck[bool](result),
				"equality keeps its legacy case-sensitive comparison for this mix")
		})
	}
}

// Issue #31175 review: an implicit case-insensitive column and CAST(x AS
// BINARY) both carry implicit coercibility, and MySQL lets the binary
// operand absorb that tie. LIKE/ILIKE/FIND_IN_SET therefore compare
// byte-wise and the regexp functions keep their error 3995 rejection,
// exactly the routing these matchers had before they joined collation
// resolution; equality keeps the generic 1267 verdict it already had. The
// restored admission must leave the operands unconverted (a delivery cast on
// the column would break projection and prefix-index use) and must not
// absorb text/text conflicts. FIND_IN_SET's overload matcher preserves each
// string operand's domain, so a non-constant binary operand (a parameter or
// an unfolded CAST, exercised below) keeps the byte-wise routing instead of
// losing its domain to a VARCHAR conversion cast.
func TestPatternBinaryColumnCastTieRestored(t *testing.T) {
	proc := newPlanTestProcess(t)
	compiler := NewMockCompilerContext(false, proc)
	build := func(sql string) (*Plan, error) {
		t.Helper()
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		return BuildPlan(compiler, stmt, false, nil)
	}
	created, err := build("create table collation_binary_tie(s varchar(20) collate utf8mb4_unicode_ci, g varchar(20))")
	require.NoError(t, err)
	compiler.tables["collation_binary_tie"] = created.GetDdl().GetCreateTable().TableDef
	compiler.objects["collation_binary_tie"] = &ObjectRef{ObjName: "collation_binary_tie", SchemaName: "tpch"}
	for _, tc := range []struct {
		sql  string
		code uint16
	}{
		{"select s like cast('alice%' as binary) from collation_binary_tie", 0},
		{"select cast('alice%' as binary) like s from collation_binary_tie", 0},
		{"select s not like cast('alice%' as binary) from collation_binary_tie", 0},
		{"select find_in_set(s, cast('alice,bob' as binary)) from collation_binary_tie", 0},
		{"select find_in_set(cast('Alice' as binary), s) from collation_binary_tie", 0},
		{"select s ilike cast('alice%' as binary) from collation_binary_tie", 0},
		{"select cast('alice%' as binary) ilike s from collation_binary_tie", 0},
		// The regexp functions are admitted through the same tie, and their
		// own compatibility check keeps MySQL's error 3995 rejection for the
		// binary/ci mix; the generic 1267 verdict must not replace it.
		{"select s regexp cast('^alice' as binary) from collation_binary_tie", 3995},
		{"select cast('Alice' as binary) regexp s from collation_binary_tie", 3995},
		{"select regexp_like(s, cast('^alice' as binary)) from collation_binary_tie", 3995},
		// A version-zero general_ci column never lost the tie.
		{"select g like cast('alice%' as binary) from collation_binary_tie", 0},
		{"select find_in_set(g, cast('alice,bob' as binary)) from collation_binary_tie", 0},
		// An explicit COLLATE on the text side still wins the resolution and
		// converts the binary operand; a COLLATE on the binary CAST itself
		// keeps its 1253 rejection.
		{"select (s collate utf8mb4_unicode_ci) like cast('alice%' as binary) from collation_binary_tie", 0},
		{"select s like cast('alice%' as binary) collate utf8mb4_unicode_ci from collation_binary_tie", 1253},
		// ESCAPE participates in LIKE's aggregation, so the pairwise order
		// decides the tie exactly the way MySQL aggregates: the binary
		// operand absorbs every operand it meets, but a text/text conflict
		// reached before it stays illegal.
		{"select s like cast('a%' as binary) escape g from collation_binary_tie", 0},
		{"select s like g escape cast('!' as binary) from collation_binary_tie", 1267},
		// Equality and text/text mixes keep the generic verdict.
		{"select s = cast('ALICE' as binary) from collation_binary_tie", 1267},
		{"select s like g from collation_binary_tie", 1267},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := build(tc.sql)
			if tc.code == 0 {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			var sqlErr *moerr.Error
			require.ErrorAs(t, err, &sqlErr)
			require.Equal(t, tc.code, sqlErr.MySQLCode())
		})
	}
	// The admitted predicate keeps its operands unconverted: the column
	// stays a bare column reference and the CAST keeps its own expression,
	// so the plan shape is the pre-aggregation one.
	t.Run("no delivery cast wraps the column", func(t *testing.T) {
		p, err := build("select s like cast('alice%' as binary) from collation_binary_tie")
		require.NoError(t, err)
		query := p.GetQuery()
		like := query.Nodes[query.Steps[0]].ProjectList[0]
		require.Equal(t, "like", like.GetF().Func.ObjName)
		require.NotNil(t, like.GetF().Args[0].GetCol(), "the column operand must stay a bare column reference")
		require.NotNil(t, like.GetF().Args[1].GetF(), "the CAST operand keeps its own expression")
	})
	uciType := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
	col := &Expr{Typ: uciType.PlanType(), Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 0}}}
	bindConstant := func(expr string) *Expr {
		t.Helper()
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select "+expr, 1)
		require.NoError(t, err)
		defer stmt.Free()
		ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
		bound, err := NewDefaultBinder(t.Context(), nil, nil, Type{}, nil).BindExpr(ast, 0, false)
		require.NoError(t, err)
		return bound
	}
	newBatch := func(value string) *batch.Batch {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(uciType)
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(value), false, proc.Mp()))
		bat.SetRowCount(1)
		return bat
	}
	// The admitted tie routes byte-wise through the binary operand's string
	// domain in either operand order; ILIKE keeps its fold-on-every-domain
	// behavior, exactly as before the matchers joined resolution.
	for _, tc := range []struct {
		name string
		fn   string
		args []*Expr
		want bool
	}{
		{"like ci column / binary cast", "like", []*Expr{col, bindConstant("cast('alice%' as binary)")}, false},
		{"like binary cast / ci column", "like", []*Expr{bindConstant("cast('alice%' as binary)"), col}, false},
		{"like exact-case control", "like", []*Expr{col, bindConstant("cast('Alice%' as binary)")}, true},
		{"like non-match control", "like", []*Expr{col, bindConstant("cast('z%' as binary)")}, false},
		{"ilike folds on the binary domain", "ilike", []*Expr{col, bindConstant("cast('alice%' as binary)")}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound, err := BindFuncExprImplByPlanExpr(t.Context(), tc.fn, tc.args)
			require.NoError(t, err)
			executor, err := colexec.NewExpressionExecutor(proc, bound)
			require.NoError(t, err)
			defer executor.Free()
			bat := newBatch("Alice")
			defer bat.Clean(proc.Mp())
			result, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
			require.NoError(t, err)
			require.Equal(t, []bool{tc.want}, vector.MustFixedColNoTypeCheck[bool](result))
		})
	}
	// The unfolded CAST operands evaluate at runtime exactly like a prepared
	// CAST(? AS BINARY) parameter: the overload matcher must preserve the
	// binary string domain so membership stays byte-wise in either operand
	// order, matching the pre-aggregation executor and MySQL.
	for _, tc := range []struct {
		name string
		args []*Expr
		want int32
	}{
		{"find_in_set ci needle / binary list", []*Expr{col, bindConstant("cast('alice,bob' as binary)")}, 0},
		{"find_in_set binary needle / ci list", []*Expr{bindConstant("cast('Alice' as binary)"), col}, 1},
		{"find_in_set exact-case control", []*Expr{col, bindConstant("cast('Alice,bob' as binary)")}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound, err := BindFuncExprImplByPlanExpr(t.Context(), "find_in_set", tc.args)
			require.NoError(t, err)
			executor, err := colexec.NewExpressionExecutor(proc, bound)
			require.NoError(t, err)
			defer executor.Free()
			bat := newBatch("Alice")
			defer bat.Clean(proc.Mp())
			result, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
			require.NoError(t, err)
			require.Equal(t, []int32{tc.want}, vector.MustFixedColNoTypeCheck[int32](result))
		})
	}
	// The regexp compatibility check still sees the operands' original
	// domains, so the mix keeps MySQL's error 3995 rejection rather than
	// the generic 1267 verdict the admission replaced.
	t.Run("regexp keeps the 3995 rejection", func(t *testing.T) {
		_, err := BindFuncExprImplByPlanExpr(t.Context(), "reg_match",
			[]*Expr{col, bindConstant("cast('^alice' as binary)")})
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot be used in conjunction")
		var sqlErr *moerr.Error
		require.ErrorAs(t, err, &sqlErr)
		require.Equal(t, uint16(3995), sqlErr.MySQLCode())
	})
}

func TestExpressionCollationResolution(t *testing.T) {
	ctx := context.Background()
	native := expressionCollation{charset: uint32(types.CharsetUTF8MB4UnicodeCI), version: 1, rank: 0}
	bin := expressionCollation{charset: uint32(types.CharsetUTF8MB4Bin), rank: 4}
	general := expressionCollation{charset: uint32(types.CharsetUTF8), rank: 4}
	for _, candidates := range [][]expressionCollation{{bin, general, native}, {native, bin, general}, {general, native, bin}} {
		selected, active, err := resolveExpressionCollation(ctx, "concat", candidates)
		require.NoError(t, err)
		require.True(t, active)
		require.Equal(t, native, selected)
	}
	// An equal-rank semantic-version mix does not silently upgrade old data.
	_, _, err := resolveExpressionCollation(ctx, "=", []expressionCollation{
		{charset: uint32(types.CharsetUTF8), rank: 2},
		{charset: uint32(types.CharsetUTF8MB4UnicodeCI), version: 1, rank: 2},
	})
	require.Error(t, err)
	_, active, err := resolveExpressionCollation(ctx, "=", []expressionCollation{bin, general})
	require.NoError(t, err)
	require.False(t, active, "legacy-only expressions retain their historical domain merge")

	column := &Expr{Typ: Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB4UnicodeCI), CollationVersion: 1},
		Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 0, ColPos: 0}}}
	literal := makePlan2StringConstExprWithType("a")
	originalColumn, originalLiteral := DeepCopyExpr(column), DeepCopyExpr(literal)
	bound, err := BindFuncExprImplByPlanExpr(ctx, "=", []*Expr{column, literal})
	require.NoError(t, err)
	require.True(t, proto.Equal(originalColumn, column))
	require.True(t, proto.Equal(originalLiteral, literal))
	for _, arg := range bound.GetF().Args {
		require.Equal(t, uint32(types.CharsetUTF8MB4UnicodeCI), arg.Typ.Charset)
		require.Equal(t, uint32(1), arg.Typ.CollationVersion)
	}
	require.NotNil(t, bound.GetF().Args[0].GetCol(), "unchanged native columns stay visible to safe indexed access")
	oldSubquery := &Expr{Typ: column.Typ, Expr: &pb.Expr_Sub{Sub: &pb.SubqueryRef{Typ: pb.SubqueryRef_SCALAR}}}
	bound, err = BindFuncExprImplByPlanExpr(ctx, "=", []*Expr{oldSubquery, literal})
	require.NoError(t, err)
	for _, arg := range bound.GetF().Args {
		require.Equal(t, uint32(types.CharsetUTF8MB4UnicodeCI), arg.Typ.Charset)
		require.Equal(t, uint32(1), arg.Typ.CollationVersion)
	}
}

func TestImplicitSameCharsetTextBinWinsAcrossCollationRevisions(t *testing.T) {
	ctx := context.Background()
	unicode := expressionCollation{charset: uint32(types.CharsetUTF8MB4UnicodeCI), version: 1, rank: 2}
	general := expressionCollation{charset: uint32(types.CharsetUTF8), rank: 2}
	bin := expressionCollation{charset: uint32(types.CharsetUTF8MB4Bin), rank: 2}
	for _, candidates := range [][]expressionCollation{
		{unicode, bin}, {bin, unicode},
		{unicode, general, bin}, {bin, general, unicode},
	} {
		selected, active, err := resolveExpressionCollation(ctx, "=", candidates)
		require.NoError(t, err)
		require.True(t, active)
		require.Equal(t, bin, selected)
	}

	// An explicit conflict, a different charset, or two CI revisions cannot
	// borrow the implicit same-charset _bin precedence.
	_, _, err := resolveExpressionCollation(ctx, "=", []expressionCollation{
		{charset: unicode.charset, version: 1, rank: 0},
		{charset: bin.charset, rank: 0},
	})
	require.Error(t, err)
	_, _, err = resolveExpressionCollation(ctx, "=", []expressionCollation{
		{charset: uint32(types.CharsetUTF8MB3UnicodeCI), version: 1, rank: 2}, bin,
	})
	require.Error(t, err)
	_, _, err = resolveExpressionCollation(ctx, "=", []expressionCollation{unicode, general})
	require.Error(t, err)
	selected, active, err := resolveExpressionCollation(ctx, "=", []expressionCollation{
		{charset: unicode.charset, version: 1, rank: 0}, bin,
	})
	require.NoError(t, err)
	require.True(t, active)
	require.Equal(t, uint32(types.CharsetUTF8MB4UnicodeCI), selected.charset)
	require.Equal(t, uint32(1), selected.version)
}

func TestExpressionCollationVectorAndReuse(t *testing.T) {
	proc := newPlanTestProcess(t)
	for _, nullable := range []bool{false, true} {
		t.Run(map[bool]string{false: "values", true: "null"}[nullable], func(t *testing.T) {
			typ := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
			typ.CollationVersion = 1
			col := &Expr{Typ: typ.PlanType(), Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 0}}}
			expr, err := BindFuncExprImplByPlanExpr(t.Context(), "=", []*Expr{col, makePlan2StringConstExprWithType("a")})
			require.NoError(t, err)
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			bat := batch.NewWithSize(1)
			defer bat.Clean(proc.Mp())
			bat.Vecs[0] = vector.NewVec(typ)
			for i, value := range []string{"Á", "a ", "z"} {
				require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(value), nullable && i == 1, proc.Mp()))
			}
			bat.SetRowCount(3)
			for iteration := 0; iteration < 2; iteration++ {
				result, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
				require.NoError(t, err)
				require.Equal(t, []bool{true, !nullable, false}, vector.MustFixedColNoTypeCheck[bool](result))
				require.Equal(t, nullable, result.GetNulls().Contains(1))
				executor.ResetForNextQuery()
			}
		})
	}
}
