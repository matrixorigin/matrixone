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
		"values row(" + left + "), row(" + right + ")",
		"select least('a' collate utf8mb4_bin,'bb') = 'a' collate utf8mb4_general_ci",
		"select greatest('a' collate utf8mb4_bin,'bb') = 'bb' collate utf8mb4_general_ci",
		"select least('a' collate utf8mb4_bin,'b') = 'a' collate utf8mb4_general_ci",
		"select name_const('x','a' collate utf8mb4_unicode_ci) = 'a' collate utf8mb4_bin",
		"select least(coalesce(cast('a' as char(3)) collate utf8mb4_bin,'a'),coalesce(cast('bb' as char(3)),'bb')) = 'a' collate utf8mb4_general_ci",
		"select greatest(coalesce(cast('a' as char(3)) collate utf8mb4_bin,'a'),coalesce(cast('bb' as char(3)),'bb')) = 'bb' collate utf8mb4_general_ci",
		"select least(ifnull(cast('a' as char(3)) collate utf8mb4_bin,'a'),ifnull(cast('b' as char(4)),'b')) = 'a' collate utf8mb4_general_ci",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(ctx, stmt, false)
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
		return BuildPlan(compiler, stmt, false)
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
		built, err := BuildPlan(ctx, stmt, true)
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
	built, err := BuildPlan(optimizer.CurrentContext(), stmt, false)
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
		{"least('a' collate utf8mb4_unicode_ci,'B')", "a"},
		{"greatest('a' collate utf8mb4_unicode_ci,'B')", "B"},
		{"concat('a' collate utf8mb4_unicode_ci,'B')", "aB"},
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
					if tc.want == "true" || tc.want == "false" {
						require.Equal(t, tc.want == "true", vector.MustFixedColNoTypeCheck[bool](value)[0])
					} else {
						require.Equal(t, tc.want, string(value.GetBytesAt(0)))
					}
				}()
			}
		})
	}
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
