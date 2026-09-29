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

package plan

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type divPrecisionCompilerContext struct {
	CompilerContext
	increment int64
}

func (c divPrecisionCompilerContext) ResolveVariable(name string, system, global bool) (any, error) {
	if name == "div_precision_increment" && system && !global {
		return c.increment, nil
	}
	return c.CompilerContext.ResolveVariable(name, system, global)
}

func TestQueryBuilderCarriesDivPrecisionIncrementIntoBinding(t *testing.T) {
	decimalType := types.New(types.T_decimal64, 10, 2)
	newColumn := func(position int32) *Expr {
		return &Expr{
			Typ: makePlan2Type(&decimalType),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: 0,
				ColPos: position,
			}},
		}
	}

	for _, test := range []struct {
		increment int64
		want      types.Type
	}{
		{increment: 0, want: types.New(types.T_decimal128, 12, 2)},
		{increment: 4, want: types.New(types.T_decimal128, 16, 6)},
		{increment: 10, want: types.New(types.T_decimal128, 22, 12)},
		{increment: 30, want: types.New(types.T_decimal256, 42, 30)},
	} {
		t.Run(test.want.String(), func(t *testing.T) {
			compiler := divPrecisionCompilerContext{
				CompilerContext: NewMockCompilerContext(true),
				increment:       test.increment,
			}
			builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, true)
			expr, err := BindFuncExprImplByPlanExpr(
				builder.GetContext(), "/", []*Expr{newColumn(0), newColumn(1)})
			require.NoError(t, err)
			require.Equal(t, test.want, makeTypeByPlan2Expr(expr))
		})
	}
}

func TestDDLDivisionBindersUseSessionPrecision(t *testing.T) {
	compiler := NewMockCompilerContext(false)
	// The mock has no internal SQL executor for FK reverse lookups. An existing
	// table entry keeps that unrelated catalog query out of this binding test.
	compiler.tables["t"] = &planpb.TableDef{Name: "t"}
	proc := compiler.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldFloor, hadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion97)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion97)
	const ddl = "create table t(a decimal(10,2), b decimal(10,2), " +
		"q decimal(30,12) generated always as (a/b) stored, " +
		"check(a/b > 0.3333333))"
	stmt, err := mysql.ParseOne(t.Context(), ddl, 1)
	require.NoError(t, err)
	defer stmt.Free()
	ctx := divPrecisionCompilerContext{CompilerContext: compiler, increment: 10}
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion96)
	_, err = BuildPlan(ctx, stmt, false)
	require.ErrorContains(t, err, "protocol version 97")
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion97)

	findDivision := func(expr *planpb.Expr) *planpb.Expr {
		var visit func(*planpb.Expr) *planpb.Expr
		visit = func(e *planpb.Expr) *planpb.Expr {
			if f := e.GetF(); f != nil {
				fid, _ := function.DecodeOverloadID(f.Func.Obj)
				if fid == function.DIV {
					return e
				}
				for _, arg := range f.Args {
					if div := visit(arg); div != nil {
						return div
					}
				}
			}
			return nil
		}
		return visit(expr)
	}

	for _, tc := range []struct {
		increment int64
		want      types.Type
	}{
		{0, types.New(types.T_decimal128, 12, 2)},
		{4, types.New(types.T_decimal128, 16, 6)},
		{10, types.New(types.T_decimal128, 22, 12)},
		{30, types.New(types.T_decimal256, 42, 30)},
	} {
		t.Run(fmt.Sprintf("increment_%d", tc.increment), func(t *testing.T) {
			ctx := divPrecisionCompilerContext{CompilerContext: compiler, increment: tc.increment}
			built, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			table := built.GetDdl().GetCreateTable().GetTableDef()
			require.NotNil(t, table)
			encoded, err := proto.Marshal(table)
			require.NoError(t, err)
			var restored planpb.TableDef
			require.NoError(t, proto.Unmarshal(encoded, &restored))
			require.Len(t, restored.Checks, 1)
			original := proto.Clone(&restored).(*planpb.TableDef)
			sensitive, err := AnalyzeTableDumpBindings(ctx, &restored, &restored)
			require.NoError(t, err)
			require.True(t, sensitive)
			require.True(t, proto.Equal(original, &restored), "DUMP analysis mutated a schema with a CHECK")
			for _, expr := range []*planpb.Expr{restored.Cols[2].GeneratedCol.Expr, restored.Checks[0].Check} {
				division := findDivision(expr)
				require.NotNil(t, division)
				require.Equal(t, tc.want, makeTypeByPlan2Expr(division))
			}
		})
	}

	t.Run("generated column positions in dump bindings", func(t *testing.T) {
		for _, tc := range []struct {
			name, columns string
			aPos, bPos    int32
		}{
			{"first", "q decimal(30,12) generated always as (a/b) stored, a decimal(10,2), b decimal(10,2)", 1, 2},
			{"middle", "a decimal(10,2), q decimal(30,12) generated always as (a/b) stored, b decimal(10,2)", 0, 2},
			{"last", "a decimal(10,2), b decimal(10,2), q decimal(30,12) generated always as (a/b) stored", 0, 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				stmt, parseErr := mysql.ParseOne(t.Context(), "create table t("+tc.columns+")", 1)
				require.NoError(t, parseErr)
				defer stmt.Free()
				build := func(increment int64) *planpb.TableDef {
					planned, buildErr := BuildPlan(divPrecisionCompilerContext{
						CompilerContext: compiler, increment: increment,
					}, stmt, false)
					require.NoError(t, buildErr)
					return planned.GetDdl().GetCreateTable().GetTableDef()
				}
				source := build(10)
				target := build(0)
				var generated *planpb.Expr
				for _, col := range source.Cols {
					if col.Name == "q" {
						generated = col.GeneratedCol.Expr
						break
					}
				}
				require.NotNil(t, generated)
				division := findDivision(generated)
				require.NotNil(t, division)
				require.Equal(t, tc.aPos, division.GetF().Args[0].GetCol().ColPos)
				require.Equal(t, tc.bPos, division.GetF().Args[1].GetCol().ColPos)
				sensitive, analyzeErr := AnalyzeTableDumpBindings(compiler, source, source)
				require.NoError(t, analyzeErr)
				require.True(t, sensitive)
				_, analyzeErr = AnalyzeTableDumpBindings(compiler, target, source)
				require.NoError(t, analyzeErr)
			})
		}
	})

	t.Run("mixed persisted bindings", func(t *testing.T) {
		mixedDDL := "create table t(a decimal(10,2), b decimal(10,2), " +
			"q decimal(30,12) default (a/b), " +
			"r decimal(30,12) generated always as (a/b) stored)"
		mixedStmt, parseErr := mysql.ParseOne(t.Context(), mixedDDL, 1)
		require.NoError(t, parseErr)
		defer mixedStmt.Free()
		at := func(increment int64) *planpb.TableDef {
			p, buildErr := BuildPlan(divPrecisionCompilerContext{
				CompilerContext: compiler, increment: increment,
			}, mixedStmt, false)
			require.NoError(t, buildErr)
			return p.GetDdl().GetCreateTable().GetTableDef()
		}
		source := DeepCopyTableDef(at(4), true)
		source.Cols[2].Default = at(10).Cols[2].Default
		target := at(4)
		originalSource := proto.Clone(source).(*planpb.TableDef)
		originalTarget := proto.Clone(target).(*planpb.TableDef)
		sensitive, analyzeErr := AnalyzeTableDumpBindings(compiler, source, source)
		require.NoError(t, analyzeErr)
		require.True(t, sensitive)
		sensitive, analyzeErr = AnalyzeTableDumpBindings(compiler, target, source)
		require.NoError(t, analyzeErr)
		require.True(t, sensitive)
		sensitive, analyzeErr = AnalyzeTableDumpBindings(compiler, target, nil)
		require.NoError(t, analyzeErr)
		require.True(t, sensitive)
		require.True(t, proto.Equal(originalSource, source), "DUMP analysis mutated the source schema")
		require.True(t, proto.Equal(originalTarget, target), "DUMP analysis mutated the target schema")
		t.Run("tampered bound tree", func(t *testing.T) {
			tampered := DeepCopyTableDef(source, true)
			tampered.Cols[2].Default.Expr = &planpb.Expr{
				Typ: planpb.Type{Id: int32(types.T_decimal128), Width: 30, Scale: 12},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
					Value: &planpb.Literal_I64Val{I64Val: 1},
				}},
			}
			_, analyzeErr := AnalyzeTableDumpBindings(compiler, target, tampered)
			require.ErrorContains(t, analyzeErr, "cannot verify bound default expression")
		})
		t.Run("reversed mixed bindings", func(t *testing.T) {
			reversed := DeepCopyTableDef(at(10), true)
			reversed.Cols[2].Default = at(4).Cols[2].Default
			_, analyzeErr := AnalyzeTableDumpBindings(compiler, at(0), reversed)
			require.NoError(t, analyzeErr)
		})
	})
	t.Run("nested and folded division", func(t *testing.T) {
		stmt, parseErr := mysql.ParseOne(t.Context(),
			"create table t(a decimal(10,2), b decimal(10,2), "+
				"q decimal(30,12) default ((a/b)/3), "+
				"r decimal(30,12) default (cast(1 as decimal(10,2))/cast(3 as decimal(10,2))))", 1)
		require.NoError(t, parseErr)
		defer stmt.Free()
		built, buildErr := BuildPlan(divPrecisionCompilerContext{
			CompilerContext: compiler, increment: 10,
		}, stmt, false)
		require.NoError(t, buildErr)
		definition := built.GetDdl().GetCreateTable().GetTableDef()
		nested := DeepCopyTableDef(definition, true)
		nested.Cols[3].Default = nil
		sensitive, analyzeErr := AnalyzeTableDumpBindings(compiler, nested, nested)
		require.NoError(t, analyzeErr)
		require.True(t, sensitive)
		folded := DeepCopyTableDef(definition, true)
		folded.Cols[2].Default = nil
		sensitive, analyzeErr = AnalyzeTableDumpBindings(compiler, folded, folded)
		require.NoError(t, analyzeErr)
		require.True(t, sensitive)
	})
	t.Run("parser mode profiles", func(t *testing.T) {
		origin := `(a / b > 0) and ('x\\y' || 'z') = 'x\\yz'`
		formatted := make(map[string]string)
		for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES", "PIPES_AS_CONCAT", "NO_BACKSLASH_ESCAPES,PIPES_AS_CONCAT"} {
			stmt, ast, visitor, parseErr := parseTableDumpExpression(t.Context(), origin, mode)
			require.NoError(t, parseErr, mode)
			require.True(t, visitor.hasDivision, mode)
			formatted[mode] = tree.String(ast, dialect.MYSQL)
			stmt.Free()
		}
		require.NotEqual(t, formatted[""], formatted["PIPES_AS_CONCAT"])
		require.NotEqual(t, formatted[""], formatted["NO_BACKSLASH_ESCAPES"])
	})
	t.Run("division on update is not accepted by SQL grammar", func(t *testing.T) {
		_, parseErr := mysql.ParseOne(t.Context(),
			"create table t(a decimal(10,2), b decimal(10,2), "+
				"q decimal(30,12) on update (a/b))", 1)
		require.Error(t, parseErr)
	})
}

func BenchmarkAnalyzeTableDumpBindingsChecks(b *testing.B) {
	compiler := NewMockCompilerContext(false)
	compiler.tables["t"] = &planpb.TableDef{Name: "t"}
	rt := moruntime.ServiceRuntime(compiler.GetProcess().GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldFloor, hadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
	b.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		}
		if hadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion97)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion97)
	ctx := divPrecisionCompilerContext{CompilerContext: compiler, increment: 10}
	for _, count := range []int{10, 100} {
		b.Run(fmt.Sprintf("checks=%d", count), func(b *testing.B) {
			var sql strings.Builder
			sql.WriteString("create table t(a decimal(10,2), b decimal(10,2)")
			for i := range count {
				fmt.Fprintf(&sql, ", constraint c%d check(a/b > 0)", i)
			}
			sql.WriteByte(')')
			stmt, err := mysql.ParseOne(b.Context(), sql.String(), 1)
			require.NoError(b, err)
			b.Cleanup(stmt.Free)
			built, err := BuildPlan(ctx, stmt, false)
			require.NoError(b, err)
			def := built.GetDdl().GetCreateTable().GetTableDef()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, err := AnalyzeTableDumpBindings(ctx, def, def)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestPreparedDivisionSpecializationUsesPrecisionIncrementContext(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		`prepare div_precision from 'select ? / 2 as q'`)
	require.NoError(t, err)

	ctx := function.WithDivPrecisionIncrement(context.Background(), 10)
	bound, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
		ctx,
		prepared.GetDcl().GetPrepare().Plan,
		[]any{ParamValue{
			Value:         "1.00",
			SourceType:    types.New(types.T_decimal64, 10, 2),
			HasSourceType: true,
		}},
	)
	require.NoError(t, err)
	require.True(t, specialized)
	division := findPlanFunctionExpr(bound, "/")
	require.NotNil(t, division)
	require.Equal(t, types.New(types.T_decimal128, 20, 12), makeTypeByPlan2Expr(division))
}

func TestDivisionSQLBoundaries(t *testing.T) {
	for _, test := range []struct {
		name      string
		increment int64
		sql       string
		width     int32
		scale     int32
		want      string
		wantError bool
	}{
		{
			name: "promoted negative divisor", increment: 4,
			sql: "select cast('18446744073709551616' as decimal(38,0)) / " +
				"cast('-18446744073709551616' as decimal(20,0))",
			width: 42, scale: 4, want: "-1.0000",
		},
		{
			name: "maximum inline divisor", increment: 4,
			sql: "select cast('18446744073709551615' as decimal(38,0)) / " +
				"cast('18446744073709551615' as decimal(20,0))",
			width: 42, scale: 4, want: "1.0000",
		},
		{
			name: "time numerator increment zero", increment: 0,
			sql:   "select cast('00:00:01.000' as time(3)) / cast(3 as decimal(10,0))",
			width: 17, scale: 3, want: "0.333",
		},
		{
			name: "time numerator increment ten", increment: 10,
			sql:   "select cast('00:00:01.000' as time(3)) / cast(3 as decimal(10,0))",
			width: 27, scale: 13, want: "0.3333333333333",
		},
		{
			name: "time numerator promoted", increment: 30,
			sql:   "select cast('00:00:01.000' as time(3)) / cast(3 as decimal(10,0))",
			width: 47, scale: 30, want: "0.333333333333333333333333333333",
		},
		{
			name: "datetime six digit default", increment: 4,
			sql:   "select cast('2020-01-01 23:59:59.999999' as datetime(6)) / 10",
			width: 24, scale: 10, want: "2020010123595.9999999000",
		},
		{
			name: "datetime six digit wide", increment: 30,
			sql:   "select cast('2020-01-01 23:59:59.999999' as datetime(6)) / 10",
			width: 50, scale: 30, want: "2020010123595.9999999" + strings.Repeat("0", 23),
		},
		{
			name: "time six digit conversion clamps before division", increment: 4,
			sql:   "select cast('2562047787:59:59.999999' as time(6)) / cast(1 as signed)",
			width: 24, scale: 10, want: "8385959.0000000000",
		},
		{
			name: "clamped time decimal64 divisor", increment: 4,
			sql:   "select cast('2562047787:59:59.999999' as time(6)) / cast(1 as decimal(10,0))",
			width: 24, scale: 10, want: "8385959.0000000000",
		},
		{
			name: "clamped time decimal128 divisor", increment: 4,
			sql:   "select cast('2562047787:59:59.999999' as time(6)) / cast(1 as decimal(20,0))",
			width: 24, scale: 10, want: "8385959.0000000000",
		},
		{
			name: "time six digit reversed", increment: 4,
			sql:   "select cast(1 as signed) / cast('2562047787:59:59.999999' as time(6))",
			width: 29, scale: 4, want: "0.0000",
		},
		{
			name: "negative time conversion clamps before division", increment: 4,
			sql:   "select cast('-2562047787:59:59.999999' as time(6)) / cast(1 as signed)",
			width: 24, scale: 10, want: "-8385959.0000000000",
		},
		{
			name: "date with decimal divisor", increment: 4,
			sql:   "select cast('2020-01-01' as date) / cast(2 as decimal(10,0))",
			width: 12, scale: 4, want: "10100050.5000",
		},
		{
			name: "year with decimal divisor", increment: 4,
			sql:   "select cast(2020 as year) / cast(2 as decimal(10,0))",
			width: 8, scale: 4, want: "1010.0000",
		},
		{
			name: "date stays decimal128 at increment thirty", increment: 30,
			sql:   "select cast('2020-01-01' as date) / cast(2 as decimal(10,0))",
			width: 38, scale: 30, want: "10100050.5" + strings.Repeat("0", 29),
		},
		{
			name: "year stays decimal128 at increment thirty", increment: 30,
			sql:   "select cast(2020 as year) / cast(2 as decimal(10,0))",
			width: 34, scale: 30, want: "1010." + strings.Repeat("0", 30),
		},
		{
			name: "timestamp six digit", increment: 4,
			sql:   "select cast('2020-01-01 23:59:59.999999' as timestamp(6)) / 10",
			width: 24, scale: 10, want: "2020010123595.9999999000",
		},
		{
			name: "time divisor increment ten", increment: 10,
			sql:   "select cast(1 as decimal(10,2)) / cast('00:00:03' as time)",
			width: 20, scale: 12, want: "0.333333333333",
		},
		{
			name:      "shifted numerator",
			increment: 30,
			sql: "select cast('3" + strings.Repeat("0", 46) + "' as decimal(47,0)) / " +
				"cast('1" + strings.Repeat("0", 46) + "' as decimal(47,0))",
			width: 65, scale: 30, want: "3." + strings.Repeat("0", 30),
		},
		{
			name:      "divisor alignment",
			increment: 30,
			sql: "select cast('28" + strings.Repeat("0", 45) + "' as decimal(47,0)) / " +
				"cast('1" + strings.Repeat("0", 46) + "' as decimal(47,0))",
			width: 65, scale: 30, want: "2.8" + strings.Repeat("0", 29),
		},
		{
			name:      "highest valid precision",
			increment: 0,
			sql: "select cast('" + strings.Repeat("9", 38) + "' as decimal(38,0)) / " +
				"cast('0." + strings.Repeat("0", 26) + "1' as decimal(38,27))",
			width: 65, scale: 0, want: strings.Repeat("9", 38) + strings.Repeat("0", 27),
		},
		{
			name:      "declared precision overflow",
			increment: 0,
			sql: "select cast('1" + strings.Repeat("0", 37) + "' as decimal(38,0)) / " +
				"cast('0." + strings.Repeat("0", 29) + "1' as decimal(38,30))",
			wantError: true,
		},
		{
			name:      "high input scale",
			increment: 0,
			sql:       "select cast('0.1' as decimal(38,37)) / cast('2' as decimal(1,0))",
			width:     38, scale: 30, want: "0.05" + strings.Repeat("0", 28),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			mock := NewMockCompilerContext(true)
			mock.GetProcessFunc = func() *process.Process { return proc }
			ctx := divPrecisionCompilerContext{CompilerContext: mock, increment: test.increment}
			statement, err := mysql.ParseOne(t.Context(), test.sql, 1)
			require.NoError(t, err)
			defer statement.Free()
			plan, err := BuildPlan(ctx, statement, false)
			require.NoError(t, err)
			query := plan.GetQuery()
			expr := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
			result, free, err := colexec.GetReadonlyResultFromNoColumnExpression(proc, expr)
			if free != nil {
				defer free()
			}
			if test.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.width, result.GetType().Width)
			require.Equal(t, test.scale, result.GetType().Scale)
			if result.GetType().Oid == types.T_decimal128 {
				require.Equal(t, test.want, vector.MustFixedColWithTypeCheck[types.Decimal128](result)[0].Format(test.scale))
			} else {
				require.Equal(t, test.want, vector.MustFixedColWithTypeCheck[types.Decimal256](result)[0].Format(test.scale))
			}
		})
	}
}
