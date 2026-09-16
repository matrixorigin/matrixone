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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestIntegerArgumentMySQLSourceEvaluation(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	for _, tc := range []struct{ input, want string }{
		{"1.5e0", "a.b"}, {"2.5e0", "a.b"}, {"1.5", "a.b"}, {"2.5", "a.b.c"},
		{"-2.5e0", "c.d"}, {"-2.5", "b.c.d"},
		{"cast(1.5 as double)", "a"}, {"cast(-1.5 as double)", "d"},
		{"cast(1.5 as double)+0e0", "a.b"}, {"abs(cast(1.5 as double))", "a.b"},
		{"if(true,cast(1.5 as double),1.5e0)", "a"}, {"if(false,cast(1.5 as double),1.5e0)", "a.b"},
		{"if(true,2.5,2.5e0)", "a.b.c"}, {"if(false,2.5,2.5e0)", "a.b"},
		{"case when true then 2.5 else 2.5e0 end", "a.b.c"},
		{"case 1 when 1 then cast(1.5 as double) else 1.5e0 end", "a"},
		{"coalesce(cast(1.5 as double),1.5e0)", "a.b"},
		{"ifnull(cast(1.5 as double),0e0)", "a.b"},
		{"nullif(cast(1.5 as double),0e0)", "a"},
		{"if(true,ifnull(cast(1.5 as double),0e0),0)", "a.b"},
		{"'1.9tail'", "a"}, {"'bad'", ""}, {"true", "a"},
		{"cast('9007199254740993' as decimal(20,0))", "a.b.c.d"},
		{"case when false then cast('9223372036854775808' as unsigned) else 2 end", "a.b"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select substring_index('a.b.c.d','.',"+tc.input+")", 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			require.Equal(t, int32(types.T_int64), bound.GetF().Args[2].Typ.Id)
			_, overload := function.DecodeOverloadID(bound.GetF().Func.Obj)
			require.Equal(t, int32(2), overload)
			folded, err := ConstantFold(batch.EmptyForConstFoldBatch, bound, proc, false, true)
			require.NoError(t, err)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, folded, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.False(t, result.IsConstNull())
			require.Equal(t, tc.want, result.GetStringAt(0))
		})
	}
}

func TestIntegerArgumentSharedConsumers(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().QueryId = []string{"q1", "q2", "q3"}
	for _, tc := range []struct{ expr, want string }{
		{"period_add(202401,1.5e0)", "202403"},
		{"period_add(202401,cast(1.5 as double))", "202402"},
		{"period_add(202401,2.5)", "202404"},
		{"period_diff(202402.5,202401)", "2"},
		{"left('abcde',1.5e0)", "ab"},
		{"right('abcde',1.5e0)", "de"},
		{"substring('abcde',1.5e0,1.5e0)", "bc"},
		{"substr('abcde',cast(1.5 as double),1)", "a"},
		{"mid('abcde',2.5)", "cde"},
		{"lpad('x',1.5e0,'a')", "ax"},
		{"rpad('x',1.5e0,'a')", "xa"},
		{"insert('abcd',1.5e0,1.5e0,'X')", "aXd"},
		{"locate('b','abcbd',1.5e0)", "2"},
		{"repeat('a',1.5e0)", "aa"},
		{"length(space(1.5e0))", "2"},
		{"elt(1.5e0,'a','b','c')", "b"},
		{"round(1234,-1.5e0)", "1200"},
		{"round(1234,cast(-1.5 as double))", "1230"},
		{"round(1234,-2.5)", "1000"},
		{"round(1.2345e0,1.5e0)", "1.23"},
		{"round(1.2345e0,cast(1.5 as double))", "1.2"},
		{"truncate(1.2399e0,2.5)", "1.239"},
		{"ceil(12345,-1.5e0)", "12400"},
		{"ceiling(12345,cast(-1.5 as double))", "12350"},
		{"floor(12345,-1.5e0)", "12300"},
		{"floor(12345,cast(-1.5 as double))", "12340"},
		{"regexp_instr('a.b.c','[.]',1,1.5e0)", "4"},
		{"regexp_instr('a.b.c','[.]',1,cast(1.5 as double))", "2"},
		{"regexp_instr('a.b.c','[.]',1,1,0.5e0)", "2"},
		{"regexp_instr('a.b.c','[.]',1,1,0.5)", "3"},
		{"regexp_replace('a.b.c','[.]','X',1,1.5e0)", "a.bXc"},
		{"regexp_substr('a1b2c3','[0-9]',1,2.5)", "3"},
		{"regexp_substr('a1b2c3','[0-9]',1,2.5e0)", "2"},
		{"from_days(738886.5)", "2023-01-02"},
		{"from_days(738886.5e0)", "2023-01-01"},
		{"week('2024-01-07',2.5)", "1"},
		{"yearweek('2024-01-07',2.5e0)", "202401"},
		{"length(random_bytes(1.5e0))", "2"},
		{"length(random_bytes(cast(1.5 as double)))", "1"},
		{"length(sha2('abc','256tail'))", "64"},
		{"l1_norm(subvector(cast('[1,2,3,4]' as vecf32),2.5,1.5e0))", "7"},
		{"case when true then ceiling(12345,cast(-1.5 as double)) else round(1,1e40) end", "12350"},
		{"last_query_id(-1.5e0)", "q2"},
		{"last_query_id(cast(-1.5 as double))", "q3"},
		{"hex(1.5)", "2"},
		{"hex(2.5e0)", "2"},
		{"hex(cast(1.5 as double))", "1"},
		{"hex(if(true,2.5,2.5e0))", "3"},
		{"hex(ifnull(cast(1.5 as double),0e0))", "2"},
		{"hex('1.5')", "312E35"},
		{"hex(0x0001)", "0001"},
		{"hex(char(65.5))", "42"},
		{"hex(char(65.5e0))", "42"},
		{"hex(char(cast(65.5 as double)))", "41"},
		{"hex(char(-1,18446744073709551615))", "FFFFFFFFFFFFFFFF"},
		{"hex(char('18446744073709551615'))", "FFFFFFFF"},
		{"hex(char('9223372036854775808'))", "00"},
		{"hex(char(if(true,18446744073709551615,-1)))", "FFFFFFFF"},
		{"hex(char(if(false,18446744073709551615,-2)))", "FFFFFFFE"},
		{"hex(char(if(false,cast('9223372036854775808' as decimal(20,0)),18446744073709551615)))", "FFFFFFFF"},
		{"make_set('18446744073709551615','a','b')", "a,b"},
		{"export_set('-1','Y','N',',',3)", "Y,Y,Y"},
		{"hex(char(0xffffffffffffffff))", "FFFFFFFF"},
		{"hex(char(if(true,18446744073709551615,null)))", "FFFFFFFF"},
		{"make_set(1.5,'a','b','c')", "b"},
		{"make_set(cast(1.5 as double),'a','b','c')", "a"},
		{"make_set(-1,'a','b','c')", "a,b,c"},
		{"make_set(18446744073709551615,'a','b','c')", "a,b,c"},
		{"export_set(1.5,'Y','N',',',3)", "N,Y,N"},
		{"export_set(-1,'Y','N',',',2.5e0)", "Y,Y"},
		{"export_set(18446744073709551615,'Y','N',',',2.5)", "Y,Y,Y"},
		{"conv('10',2.5,10)", "3"},
		{"conv('10',2.5e0,10)", "2"},
		{"conv('15',10,cast(16.9 as double))", "F"},
		{"conv(1.5,10,8)", "1"},
		{"split_part('a.b.c','.',1.5e0)", "b"},
		{"split_part('a.b.c','.',cast(1.5 as double))", "a"},
		{"maketime(12.5,58.5,30.5)", "13:59:30.5"},
		{"maketime(12.5e0,58.5e0,30.5e0)", "12:58:30.500000"},
		{"maketime(cast(12.5 as double),cast(58.5 as double),cast(30.5 as double))", "12:58:30.500000"},
		{"maketime('12.9tail','58.9tail','30.5tail')", "12:58:30.500000"},
		{"maketime(cast('00:00:12.5' as time(6)),1,2)", "13:01:02"},
		{"maketime(if(true,12.5,12.5e0),if(false,58.5,58.5e0),30.5)", "13:58:30.5"},
		{"maketime(coalesce(cast(12.5 as double),0e0),1,2)", "12:01:02"},
		{"maketime(9223372036854775807,0,0)", "838:59:59"},
		{"format(1234.5,1.5)", "1,234.50"},
		{"format(1234.5,1.5e0)", "1,234.50"},
		{"format(1234.5,cast(1.5 as double))", "1,234.5"},
		{"format(1234.5,'1.9tail')", "1,234.5"},
		{"format(2.5,0)", "3"},
		{"format(2.5e0,0)", "2"},
		{"format('2.5',0)", "2"},
		{"format(1234.5,cast('00:00:01.5' as time(6)))", "1,234.50"},
		{"format(1234.5,if(true,2.5,2.5e0))", "1,234.500"},
		{"format(1234.5,coalesce(cast(1.5 as double),0e0))", "1,234.50"},
		{"makedate(2024.5,2.5)", "2025-01-03"},
		{"makedate(2024.5e0,2.5e0)", "2024-01-02"},
		{"makedate(2024,cast(1.5 as double))", "2024-01-01"},
		{"makedate('2024tail','2.9tail')", "2024-01-02"},
		{"makedate(2024,cast('00:00:01.5' as time(6)))", "2024-01-02"},
		{"makedate(2024,if(true,2.5,2.5e0))", "2024-01-03"},
		{"makedate(2024,coalesce(cast(1.5 as double),0e0))", "2024-01-02"},
		{"makedate(cast('00002024-0000-0000-0000-000000000000' as uuid),1)", "2024-01-01"},
		{"coalesce(makedate(2024,4294967297),'NULL')", "NULL"},
		{"coalesce(makedate(2024,NULL),'NULL')", "NULL"},
		{"coalesce(makedate(NULL,1),'NULL')", "NULL"},
		{"timestampadd(day,2.5,'2024-01-01')", "2024-01-04"},
		{"timestampadd(day,2.5e0,'2024-01-01')", "2024-01-03"},
		{"timestampadd(day,cast(1.5 as double),'2024-01-01')", "2024-01-02"},
		{"timestampadd(day,'1.9tail','2024-01-01')", "2024-01-02"},
		{"timestampadd(month,1.5e0,'2024-01-31')", "2024-03-31"},
		{"timestampadd(microsecond,1.5e0,'2024-01-01')", "2024-01-01 00:00:00.000002"},
		{"timestampadd(day,if(true,2.5,2.5e0),'2024-01-01')", "2024-01-04"},
		{"sha2('abc',cast('00:02:55.5' as time(6)))", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{"sha2('abc',if(true,cast('00:02:55.5' as time(6)),1e40))", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{"coalesce(sha2('abc',cast('2024-01-01' as date)),'NULL')", "NULL"},
		{"sha2('abc',cast('00000256-0000-0000-0000-000000000000' as uuid))", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{"coalesce(sha2('abc',cast('9999-12-31 23:59:59.999999' as datetime(6))),'NULL')", "NULL"},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select cast("+tc.expr+" as char)", 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			bound, err = ConstantFold(batch.EmptyForConstFoldBatch, bound, proc, false, true)
			require.NoError(t, err)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, bound, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.want, result.GetStringAt(0))
		})
	}
}

func TestIntegerArgumentEnumDisplayProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.T_enum.ToType()
	storage := &Expr{Typ: makePlan2Type(&typ), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_EnumVal{EnumVal: 2}}}}
	storage.Typ.Enumvalues = "20.5,10.5"
	display, err := BindFuncExprImplByPlanExpr(proc.Ctx, moEnumCastIndexToValueFun, []*Expr{makePlan2StringConstExprWithType("20.5,10.5"), storage})
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		args []*Expr
		want string
	}{
		{"left", []*Expr{makePlan2StringConstExprWithType("abcd"), display}, "ab"},
		{"hex", []*Expr{display}, "31302E35"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound, err := BindFuncExprImplByPlanExpr(proc.Ctx, tc.name, tc.args)
			require.NoError(t, err)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, bound, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.want, result.GetStringAt(0))
		})
	}
}

func TestIntegerArgumentRejectedTemporalType(t *testing.T) {
	ctx := context.Background()
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select left('abc',cast('00:00:01.5' as time(6)))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	_, err = NewDefaultBinder(ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
	require.Error(t, err, "source type is rejected before execution, not merely on a non-NULL row")
}

func TestIntegerArgumentAdditionalErrors(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, expression := range []string{
		"regexp_instr('a.b.c','[.]',1,1,127.5e0)",
		"sha2('abc','9223372036854775808')",
		"random_bytes(0)", "random_bytes(1025)",
		"hex(cast('9223372036854775808' as decimal(20,0)))",
		"char(cast('9223372036854775808' as decimal(20,0)))",
		"char(if(true,cast('9223372036854775808' as decimal(20,0)),18446744073709551615))",
		"char('18446744073709551616')", "char('-9223372036854775809')",
		"hex(char(length(substring_index('a.b','.',18446744073709551615))))",
		"export_set(1,'Y','N',',',18446744073709551615)",
		"split_part('a.b.c','.',4294967296)",
		"timestampadd(day,18446744073709551615,'2024-01-01')",
		"timestampadd(day,1e40,'2024-01-01')",
		"makedate(2024,cast('9223372036854775808' as decimal(20,0)))",
		"makedate(2024,'9223372036854775808')",
		"makedate(1e40,1)",
		"format(1,cast('9223372036854775808' as decimal(20,0)))",
		"format(1,'9223372036854775808')",
		"maketime(cast('9223372036854775808' as decimal(20,0)),0,0)",
		"maketime('9223372036854775808',0,0)",
		"maketime(12,cast('9223372036854775808' as decimal(20,0)),0)",
		"case when false then ceiling(12345,cast(-1.5 as double)) else round(1,1e40) end",
	} {
		t.Run(expression, func(t *testing.T) {
			stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, "select "+expression, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(proc.Ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			folded, err := ConstantFold(batch.EmptyForConstFoldBatch, bound, proc, false, true)
			if err == nil {
				executor, buildErr := colexec.NewExpressionExecutor(proc, folded)
				require.NoError(t, buildErr)
				defer executor.Free()
				_, err = executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			}
			require.Error(t, err)
		})
	}
}

func TestIntegerArgumentSourceContextScope(t *testing.T) {
	ctx := context.Background()
	for _, sql := range []string{
		"select substring_index('a.b.c','.',2.5)",
		"select substring_index('a.b.c','.',no_such_integer_source())",
	} {
		t.Run(sql, func(t *testing.T) {
			binder := NewDefaultBinder(ctx, nil, nil, planpb.Type{Id: int32(types.T_varchar), Width: 64}, nil)
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			_, err = binder.BindExpr(ast, 0, false)
			if sql == "select substring_index('a.b.c','.',2.5)" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.False(t, binder.integerArgumentSourceContext)
			stmt2, err := parsers.ParseOne(ctx, dialect.MYSQL, "select 2.5", 1)
			require.NoError(t, err)
			defer stmt2.Free()
			ast2 := stmt2.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			ordinary, err := binder.BindExpr(ast2, 0, false)
			require.NoError(t, err)
			require.Equal(t, int32(types.T_varchar), ordinary.Typ.Id, "ordinary assignment context must be restored")
		})
	}
}

func TestIntegerArgumentBoundSources(t *testing.T) {
	ctx := context.Background()
	for _, source := range []types.T{types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128, types.T_decimal256, types.T_uint64, types.T_bit, types.T_varchar} {
		t.Run(source.String(), func(t *testing.T) {
			column := &planpb.Expr{Typ: planpb.Type{Id: int32(source), Scale: 1}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
			args := []*Expr{makePlan2StringConstExprWithType("a.b.c"), makePlan2StringConstExprWithType("."), column}
			original := DeepCopyExpr(column)
			bound, err := BindFuncExprImplByPlanExpr(ctx, "substring_index", args)
			require.NoError(t, err)
			cast := bound.GetF().Args[2]
			require.Equal(t, int32(types.T_int64), cast.Typ.Id)
			_, overload := function.DecodeOverloadID(cast.GetF().Func.Obj)
			require.Equal(t, function.IntegerArgumentCastOverload, overload)
			require.Equal(t, original, column)
			require.Same(t, column, args[2])
		})
	}
}

func TestIntegerArgumentBoundSelection(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	for _, selected := range []bool{true, false} {
		t.Run(fmt.Sprint(selected), func(t *testing.T) {
			decimal, err := makePlan2DecimalExprWithType(ctx, "2.5")
			require.NoError(t, err)
			common, err := BindFuncExprImplByPlanExpr(ctx, "case", []*Expr{makePlan2BoolConstExprWithType(selected), decimal, makePlan2Float64ConstExprWithType(2.5)})
			require.NoError(t, err)
			original := DeepCopyExpr(common)
			bound, err := BindFuncExprImplByPlanExpr(ctx, "substring_index", []*Expr{makePlan2StringConstExprWithType("a.b.c.d"), makePlan2StringConstExprWithType("."), common})
			require.NoError(t, err)
			require.Equal(t, original, common)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, bound, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			want := "a.b"
			if selected {
				want = "a.b.c"
			}
			defer free()
			require.Equal(t, want, result.GetStringAt(0))
		})
	}
}

func TestIntegerArgumentPreparedRuntimeCandidates(t *testing.T) {
	for _, tc := range []struct {
		query     string
		positions []int32
	}{
		{`select substring_index(?,".",?),period_add(?,?),left(?,?)`, []int32{1, 2, 3, 5}},
		{`select substring_index("a.b.c",".",cast(? as double))`, []int32{0}},
		{`select substring_index("a.b.c",".",if(?,?,?))`, []int32{1, 2}},
		{`select substring_index(?,".",2)`, nil},
		{`select round(1.2345e0,?),regexp_instr("a.b.c","[.]",1,?),length(random_bytes(?)),sha2("a",?),week("2021-01-03",?)`, []int32{0, 1, 2, 3, 4}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, "prepare integer_source from '"+tc.query+"'")
			require.NoError(t, err)
			require.Equal(t, tc.positions, PreparedPlanNumericFallbackParamPositions(prepared.GetDcl().GetPrepare().Plan))
		})
	}
}

func TestIntegerArgumentMixedPreparedSources(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		query string
		value any
		typ   types.T
		want  string
		null  bool
	}{
		{`select hex(?)`, "1.5", types.T_float64, "2", false},
		{`select hex(?)`, "1.5", types.T_any, "312E35", false},
		{`select hex(?)`, "18446744073709551615", types.T_uint64, "FFFFFFFFFFFFFFFF", false},
		{`select hex(char(?))`, "65.5", types.T_float64, "42", false},
		{`select hex(char(?))`, "65.9tail", types.T_any, "41", false},
		{`select hex(char(?))`, "18446744073709551615", types.T_uint64, "FFFFFFFF", false},
		{`select hex(char(if(true,?,null)))`, "18446744073709551615", types.T_uint64, "FFFFFFFF", false},
		{`select hex(char(ifnull(cast(? as double),0e0)))`, "65.5", types.T_float64, "42", false},
		{`select hex(char(cast(? as double)))`, "65.5", types.T_float64, "41", false},
		{`select hex(?)`, nil, types.T_any, "", true},
		{`select timestampadd(day,?,"2024-01-01")`, "1.5", types.T_float64, "2024-01-03", false},
		{`select timestampadd(day,?,"2024-01-01")`, "1.5", types.T_any, "2024-01-02", false},
		{`select timestampadd(day,cast(? as double),"2024-01-01")`, "1.5", types.T_float64, "2024-01-02", false},
		{`select timestampadd(day,?,"2024-01-01")`, nil, types.T_any, "", true},
		{`select sha2("abc",cast(? as time(6)))`, "00:02:55.5", types.T_any, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", false},
		{`select sha2("abc",cast(? as time(6)))`, nil, types.T_any, "", true},
	} {
		t.Run(tc.query+"/"+tc.typ.String(), func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, "prepare mixed_integer from '"+tc.query+"'")
			require.NoError(t, err)
			original := prepared.GetDcl().GetPrepare().Plan
			require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(original))
			bound, changed, err := FillValuesOfParamsInPlanWithPreparedNumericOverload(proc.Ctx, original, []any{ParamValue{Value: tc.value, IsBinaryProtocol: true, RuntimeType: tc.typ.ToType(), HasRuntimeType: tc.typ != types.T_any}})
			require.NoError(t, err)
			require.True(t, changed)
			params := vector.NewVec(types.T_text.ToType())
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			require.NoError(t, vector.AppendBytes(params, []byte(fmt.Sprint(tc.value)), tc.value == nil, proc.Mp()))
			proc.SetPrepareParams(params)
			query := bound.GetQuery()
			root := query.Nodes[query.Steps[0]]
			require.Len(t, root.ProjectList, 1)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, root.ProjectList[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			if tc.null {
				require.True(t, result.IsConstNull() || result.IsNull(0))
			} else {
				require.Equal(t, tc.want, result.GetStringAt(0))
			}
		})
	}
}

func TestIntegerArgumentPreparedSourceEvaluation(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	for _, explicitReal := range []bool{false, true} {
		marker := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
		var source *Expr = marker
		if explicitReal {
			var err error
			typ := types.T_float64.ToType()
			source, err = appendExplicitCastBeforeExpr(ctx, source, makePlan2Type(&typ))
			require.NoError(t, err)
		}
		prepared, err := BindFuncExprImplByPlanExpr(ctx, "substring_index", []*Expr{makePlan2StringConstExprWithType("a.b.c.d"), makePlan2StringConstExprWithType("."), source})
		require.NoError(t, err)
		original := DeepCopyExpr(prepared)
		for _, tc := range []struct {
			name           string
			param          ParamValue
			want, castWant string
		}{
			{"SQL DOUBLE", ParamValue{Value: "1.5", SourceType: types.T_float64.ToType(), HasSourceType: true}, "a.b", "a"},
			{"SQL DECIMAL", ParamValue{Value: "2.5", SourceType: types.New(types.T_decimal64, 2, 1), HasSourceType: true}, "a.b.c", "a.b"},
			{"SQL text", ParamValue{Value: "1.9", SourceType: types.T_varchar.ToType(), HasSourceType: true}, "a", "a"},
			{"binary DOUBLE", ParamValue{Value: float64(1.5), RuntimeType: types.T_float64.ToType(), HasRuntimeType: true, IsBinaryProtocol: true}, "a.b", "a"},
			{"binary numeric spelling is text", ParamValue{Value: "1.5", IsBinaryProtocol: true}, "a", "a"},
		} {
			t.Run(fmt.Sprintf("explicit=%v/%s", explicitReal, tc.name), func(t *testing.T) {
				rule := NewResetParamRefRule(ctx, []*Expr{makePlan2StringConstExprWithType("placeholder")})
				rule.SetParamValues([]any{tc.param})
				rewritten, err := rule.ApplyExpr(DeepCopyExpr(prepared))
				require.NoError(t, err)
				result, free, err := colexec.GetReadonlyResultFromExpression(proc, rewritten, []*batch.Batch{batch.EmptyForConstFoldBatch})
				require.NoError(t, err)
				defer free()
				want := tc.want
				if explicitReal {
					want = tc.castWant
				}
				require.Equal(t, want, result.GetStringAt(0))
				require.Equal(t, original, prepared)
			})
		}
	}
}

func TestIntegerArgumentRuntimeSourcePreservesNestedPrivateCast(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	marker := &Expr{Typ: planpb.Type{Id: int32(types.T_text)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	logical, err := appendIntegerArgument(ctx, marker, types.T_int64, false)
	require.NoError(t, err)
	physical := types.T_int8.ToType()
	adapter, err := appendCastBeforeExprWithOverload(ctx, logical, makePlan2Type(&physical), 0)
	require.NoError(t, err)
	rule := NewResetParamRefRule(ctx, []*Expr{makePlan2StringConstExprWithType("placeholder")})
	rule.SetParamValues([]any{ParamValue{Value: float64(1.5), IsBinaryProtocol: true, HasRuntimeType: true, RuntimeType: types.T_float64.ToType()}})
	rebound, err := rule.integerArgumentRuntimeSource(adapter)
	require.NoError(t, err)
	require.True(t, isIntegerArgumentCast(rebound.GetF().Args[0]), "a physical adapter must not turn private coercion into ordinary CAST")
	result, free, err := colexec.GetReadonlyResultFromExpression(proc, rebound, []*batch.Batch{batch.EmptyForConstFoldBatch})
	require.NoError(t, err)
	defer free()
	require.Equal(t, int8(2), vector.GetFixedAtNoTypeCheck[int8](result, 0))
}

func TestIntegerArgumentOverflowIsNotSaturation(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	for _, input := range []string{"9223372036854775808", "-9223372036854775809", "cast('18446744073709551615' as unsigned)", "'9223372036854775808'", "1e30"} {
		t.Run(input, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, fmt.Sprintf("select substring_index('a.b','.',%s)", input), 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			_, err = ConstantFold(batch.EmptyForConstFoldBatch, bound, proc, false, true)
			require.Error(t, err)
		})
	}
}
