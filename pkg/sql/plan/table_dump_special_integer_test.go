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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/gogo/protobuf/proto"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// The fixture was produced by BuildPlan at
// b1b68925d7f6fb32153e788b5285f424222e85ca, NOT by this branch's legacy
// rebind mode. It retains the original full protobuf trees for both owners.
func TestTableDumpExactBaseSpecialIntegerBindings(t *testing.T) {
	data, err := os.ReadFile("testdata/CLAUDE_special_integer_baseline.json")
	require.NoError(t, err)
	require.Equal(t, "a624286d794db4371b0a43af016d3c989a6e7420ccd5e0a4b51659688956cfd1", fmt.Sprintf("%x", sha256.Sum256(data)))
	var fixtures map[string][]byte
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 8)
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	compiler.tables["t"] = &planpb.TableDef{Name: "t"}
	ctx := divPrecisionCompilerContext{CompilerContext: compiler, increment: 4}
	for name, sql := range map[string]string{
		"format_number":    "format(a/b,2)",
		"format_precision": "format(a,b/2)",
		"format_locale":    "format(a,b/2,'en_US')",
		"makedate_year":    "makedate(a/b,1)",
		"makedate_day":     "makedate(2024,a/2)",
		"maketime_hour":    "cast(maketime(a/2,1,1.25) as varchar)",
		"maketime_minute":  "cast(maketime(1,a/2,1.25) as varchar)",
		"maketime_seconds": "cast(maketime(a/2,1,1.25) as varchar)",
	} {
		t.Run(name, func(t *testing.T) {
			var source planpb.TableDef
			require.NoError(t, proto.Unmarshal(fixtures[name], &source))
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
				"create table t(a decimal(10,2), b decimal(10,2), g varchar(100) generated always as ("+sql+") stored, check ("+sql+" is not null))", 1)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			target := built.GetDdl().GetCreateTable().GetTableDef()
			// The fixture predates table-prefixed CHECK naming: freshly planned
			// tables name anonymous CHECKs <table>_chk_<n> while the dump
			// captured the historic __mo_chk_<n>. Name identity is irrelevant to
			// this test's division-sensitivity analysis, so align the rebuilt
			// declaration names with the persisted ones.
			for i := range target.Checks {
				if i < len(source.Checks) && source.Checks[i] != nil && target.Checks[i] != nil {
					target.Checks[i].Name = source.Checks[i].Name
				}
			}
			snapshot := proto.Clone(&source)
			_, err = AnalyzeTableDumpBindings(compiler, &source, &source)
			require.NoError(t, err)
			_, err = AnalyzeTableDumpBindings(compiler, target, &source)
			require.NoError(t, err)
			require.True(t, proto.Equal(snapshot, &source))
			tampered := proto.Clone(&source).(*planpb.TableDef)
			tampered.Cols[2].GeneratedCol.Expr = &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "forged"}}}}
			_, err = AnalyzeTableDumpBindings(compiler, target, tampered)
			require.ErrorContains(t, err, "cannot verify bound generated expression")
		})
	}
}

func TestTableDumpLegacyTimeWidthCompatibility(t *testing.T) {
	old := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_time), Scale: 2}}
	rebound := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_time), Width: 2, Scale: 2}}
	require.True(t, sameTableDumpBoundExpression(old, rebound))
	rebound.Typ.Scale = 3
	require.False(t, sameTableDumpBoundExpression(old, rebound))
	rebound.Typ.Scale = 2
	rebound.Typ.Width = 4
	require.False(t, sameTableDumpBoundExpression(old, rebound))
}

func TestTableDumpLegacySpecialIntegerBindings(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	compiler.tables["t"] = &planpb.TableDef{Name: "t"}
	rt := moruntime.ServiceRuntime(compiler.GetProcess().GetService())
	old, exists := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, old)
		} else if value, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, value)
		}
	})
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCLatestVersion)
	for _, tc := range []struct{ name, expression, resultType string }{
		{"format numeric", "format(a/b,2)", "varchar(100)"},
		{"format precision", "format(1.125,a/b)", "varchar(100)"},
		{"format locale", "format(a/b,2,'en_US')", "varchar(100)"},
		{"makedate year", "makedate(a/b,1)", "varchar(100)"},
		{"makedate day", "makedate(2024,a/b)", "varchar(100)"},
		{"maketime hour", "cast(maketime(a/b,1,1.5) as varchar)", "varchar(100)"},
		{"maketime minute", "cast(maketime(1,a/b,1.5) as varchar)", "varchar(100)"},
		{"maketime seconds", "cast(maketime(a/b,1,2.5) as varchar)", "varchar(100)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := "create table t(a decimal(10,2), b decimal(10,2), g " + tc.resultType +
				" generated always as (" + tc.expression + ") stored, check (" + tc.expression + " is not null))"
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			build := func(legacy bool) *planpb.TableDef {
				var ctx CompilerContext = compiler
				if legacy {
					ctx = tableDumpBindContext{CompilerContext: compiler, ctx: function.WithLegacySpecialConsumers(compiler.GetContext()), increment: 4}
				}
				built, buildErr := BuildPlan(ctx, stmt, false)
				require.NoError(t, buildErr)
				return built.GetDdl().GetCreateTable().GetTableDef()
			}
			source, target := build(true), build(false)
			features, err := planpb.RequiredRemoteExpressionFeatures(source)
			require.NoError(t, err)
			require.False(t, features.IntegerParameterCoercion, "legacy source must not contain new private integer CASTs")
			require.False(t, proto.Equal(source.Cols[2].GeneratedCol.Expr, target.Cols[2].GeneratedCol.Expr))
			data, err := proto.Marshal(source)
			require.NoError(t, err)
			var restored planpb.TableDef
			require.NoError(t, proto.Unmarshal(data, &restored))
			snapshot := proto.Clone(&restored)
			sensitive, err := AnalyzeTableDumpBindings(compiler, &restored, &restored)
			require.NoError(t, err)
			require.True(t, sensitive)
			require.True(t, proto.Equal(snapshot, &restored))
			_, err = AnalyzeTableDumpBindings(compiler, target, &restored)
			require.NoError(t, err)
			tampered := proto.Clone(&restored).(*planpb.TableDef)
			tampered.Cols[2].GeneratedCol.Expr = &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "forged"}}}}
			_, err = AnalyzeTableDumpBindings(compiler, target, tampered)
			require.ErrorContains(t, err, "cannot verify bound generated expression")
		})
	}
	for _, kind := range []string{"default", "on_update"} {
		t.Run(kind, func(t *testing.T) {
			call := "format(a/b,2)"
			if kind == "on_update" {
				call = "format(cast(1 as decimal(10,2))/cast(2 as decimal(10,2)),2)"
			}
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
				"select "+call, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			typ := planpb.Type{Id: int32(types.T_varchar), Width: 100}
			cols := []*planpb.ColDef{
				{Name: "a", Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}},
				{Name: "b", Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}},
				{Name: "g", Typ: typ},
			}
			build := func(legacy bool) *planpb.TableDef {
				ctx := compiler.GetContext()
				if legacy {
					ctx = function.WithLegacySpecialConsumers(ctx)
				}
				col := &tree.ColumnTableDef{Name: tree.NewUnresolvedColName("g")}
				def := &planpb.TableDef{Cols: []*planpb.ColDef{cols[0], cols[1], {Name: "g", Typ: typ}}}
				switch kind {
				case "default":
					col.Attributes = []tree.ColumnAttribute{&tree.AttributeDefault{Expr: ast}}
					bound, bindErr := buildDefaultExprWithColumns(ctx, col, typ, compiler.GetProcess(), def.Cols)
					require.NoError(t, bindErr)
					def.Cols[2].Default = bound
				case "on_update":
					col.Attributes = []tree.ColumnAttribute{&tree.AttributeOnUpdate{Expr: ast}}
					bound, bindErr := buildOnUpdate(ctx, col, typ, compiler.GetProcess())
					require.NoError(t, bindErr)
					def.Cols[2].OnUpdate = bound
				}
				return def
			}
			source, target := build(true), build(false)
			features, err := planpb.RequiredRemoteExpressionFeatures(source)
			require.NoError(t, err)
			require.False(t, features.IntegerParameterCoercion)
			_, err = AnalyzeTableDumpBindings(compiler, source, source)
			require.NoError(t, err)
			_, err = AnalyzeTableDumpBindings(compiler, target, source)
			require.NoError(t, err)
		})
	}
}

// These complete definitions were produced by BuildPlan at exact pre-PR
// b24d1b311555e0b6f3d56eb37f9a2e9617e9a5b7, with the entire PR Go delta
// removed through an overlay. They were not authored by compatibility mode.
func TestTableDumpExactBaseTimestampAddBindings(t *testing.T) {
	data, err := os.ReadFile("testdata/timestampadd_second_baseline.json")
	require.NoError(t, err)
	require.Equal(t, "f72b807991d70d7a8f5c78425297527c678d0220b0c158e3556b5e2017753568", fmt.Sprintf("%x", sha256.Sum256(data)))
	var fixtures map[string][]byte
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 2)
	// This direct, division-free tree was independently captured from the exact
	// published main binder, not synthesized by a compatibility context.
	directData, err := os.ReadFile("testdata/timestampadd_direct_baseline.json")
	require.NoError(t, err)
	require.Equal(t, "b4377672e0a97f09f81fac985143c89caf30ea08b363831bbf42faaa3d9c8f97", fmt.Sprintf("%x", sha256.Sum256(directData)))
	var direct struct {
		Source     string `json:"source"`
		DDL        string `json:"ddl"`
		Definition []byte `json:"definition"`
	}
	require.NoError(t, json.Unmarshal(directData, &direct))
	require.Equal(t, "3fc00709eccc277c175c7a09eed2605ca1aaee29", direct.Source)
	require.NotContains(t, direct.DDL, "/")
	fixtures["direct_decimal"] = direct.Definition
	proc := newPlanTestProcess(t)
	compiler := NewMockCompilerContext(false, proc)
	compiler.tables["t"] = &planpb.TableDef{Name: "t"}
	ctx := divPrecisionCompilerContext{CompilerContext: compiler, increment: 4}
	for name, interval := range map[string]string{"decimal_division": "a/2", "modern_format": "format(a/2,2)", "direct_decimal": "a"} {
		t.Run(name, func(t *testing.T) {
			var source planpb.TableDef
			require.NoError(t, proto.Unmarshal(fixtures[name], &source))
			expression := "timestampadd(second," + interval + ",cast('2024-01-01' as datetime(6)))"
			check := expression + " is not null"
			if name == "direct_decimal" {
				check = expression + " < cast('2024-01-01 00:01:00' as datetime(6))"
			}
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table t(a decimal(10,2),g datetime(6) generated always as ("+expression+") stored,check ("+check+"))", 1)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			target := built.GetDdl().GetCreateTable().GetTableDef()
			require.Len(t, target.Checks, len(source.Checks))
			for i := range target.Checks {
				target.Checks[i].Name = source.Checks[i].Name
			}
			features, err := planpb.RequiredRemoteExpressionFeatures(target)
			require.NoError(t, err)
			require.True(t, features.TimestampAddSecondIntervals)
			legacyFeatures, err := planpb.RequiredRemoteExpressionFeatures(&source)
			require.NoError(t, err)
			require.False(t, legacyFeatures.TimestampAddSecondIntervals)
			if name == "direct_decimal" {
				t.Run("dump_before_protocol_promotion", func(t *testing.T) {
					setPlanTestGlobalVariable(t, proc.GetService(), moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion118)
					sensitive, err := AnalyzeTableDumpBindings(compiler, &source, &source)
					require.NoError(t, err)
					require.True(t, sensitive, "a denied current candidate cannot prove binding independence")
				})
			}
			snapshot := proto.Clone(&source)
			for _, tc := range []struct {
				name           string
				target, source *planpb.TableDef
			}{{"dump_old", &source, &source}, {"restore_old", target, &source}, {"current", target, target}} {
				t.Run(tc.name, func(t *testing.T) {
					sensitive, err := AnalyzeTableDumpBindings(compiler, tc.target, tc.source)
					require.NoError(t, err)
					require.True(t, sensitive, "capture classification must compare complete eras even after a current match")
				})
			}
			require.True(t, proto.Equal(snapshot, &source))
			sensitive, err := AnalyzeTableDumpBindings(compiler, target, nil)
			require.NoError(t, err)
			require.True(t, sensitive, "v1 cannot disambiguate historical bindings")
			// Restored trees retain old execution; new DDL keeps fractional seconds.
			for _, tc := range []struct {
				name       string
				definition *planpb.TableDef
				want       string
			}{{"legacy_execution", &source, map[string]string{"decimal_division": "2024-01-01 00:00:02.000000", "modern_format": "2024-01-01 00:00:01.000000", "direct_decimal": "2024-01-01 00:00:02.000000"}[name]}, {"new_execution", target, "2024-01-01 00:00:01.500000"}} {
				t.Run(tc.name, func(t *testing.T) {
					input := batch.NewWithSize(1)
					defer input.Clean(proc.Mp())
					input.Vecs[0] = vector.NewVec(types.New(types.T_decimal64, 10, 2))
					value := types.Decimal64(300)
					if name == "direct_decimal" {
						value = 150
					}
					require.NoError(t, vector.AppendFixed(input.Vecs[0], value, false, proc.Mp()))
					input.SetRowCount(1)
					executor, err := colexec.NewExpressionExecutor(proc, tc.definition.Cols[1].GeneratedCol.Expr)
					require.NoError(t, err)
					defer executor.Free()
					result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
					require.NoError(t, err)
					require.Equal(t, tc.want, vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0).String2(6))
				})
			}
			for _, change := range []string{"unit", "interval", "overload", "check"} {
				t.Run("reject_"+change, func(t *testing.T) {
					forged := proto.Clone(&source).(*planpb.TableDef)
					expr := forged.Cols[1].GeneratedCol.Expr
					if change == "check" {
						expr = forged.Checks[0].Check
					}
					var find func(*planpb.Expr) *planpb.Function
					find = func(e *planpb.Expr) *planpb.Function {
						if f := e.GetF(); f != nil {
							if f.Func.ObjName == "timestampadd" {
								return f
							}
							for _, a := range f.Args {
								if found := find(a); found != nil {
									return found
								}
							}
						}
						return nil
					}
					fn := find(expr)
					require.NotNil(t, fn)
					switch change {
					case "unit":
						fn.Args[0] = MakePlan2StringConstExprWithType("DAY")
					case "overload":
						fn.Func.Obj++
					default:
						fn.Args[1] = MakePlan2Int64ConstExprWithType(123)
					}
					_, err := AnalyzeTableDumpBindings(compiler, target, forged)
					require.ErrorContains(t, err, "cannot verify bound")
				})
			}
		})
	}
}

func TestTableDumpDirectDefaultUsesHistoricalEra(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	setPlanTestGlobalVariable(t, compiler.GetProcess().GetService(), moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCLatestVersion))
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table t(d datetime(6) default(timestampadd(second,cast(1.50 as decimal(10,2)),cast('2024-01-01' as datetime(6)))))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	current, err := BuildPlan(compiler, stmt, false)
	require.NoError(t, err)
	legacyCtx := tableDumpBindContext{CompilerContext: compiler, ctx: function.WithLegacyTimestampAddSecond(compiler.GetContext()), increment: 4}
	legacy, err := BuildPlan(legacyCtx, stmt, false)
	require.NoError(t, err)
	currentDef, legacyDef := current.GetDdl().GetCreateTable().TableDef, legacy.GetDdl().GetCreateTable().TableDef
	require.False(t, sameTableDumpBoundExpression(currentDef.Cols[0].Default.Expr, legacyDef.Cols[0].Default.Expr))
	for _, source := range []*planpb.TableDef{currentDef, legacyDef, nil} {
		sensitive, err := AnalyzeTableDumpBindings(compiler, currentDef, source)
		require.NoError(t, err)
		require.True(t, sensitive)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := proto.Clone(currentDef)
	_, err = AnalyzeTableDumpBindings(tableDumpBindContext{CompilerContext: compiler, ctx: ctx, increment: 4}, currentDef, currentDef)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, proto.Equal(before, currentDef))
}

// Captured by the real pre-v121 binder at 0f9b9cc6c4d7d8afa19a24624f5c3aa019614eb1.
// This fixture is independent of the compatibility mode under test.
func TestTableDumpExactPreDecimalDivBindings(t *testing.T) {
	data, err := os.ReadFile("testdata/decimal_div_pre121_baseline.json")
	require.NoError(t, err)
	require.Equal(t, "441b72a7d111cad67b3a148eecd06ecd587990b87cb1f9054c2011cb60c666d9", fmt.Sprintf("%x", sha256.Sum256(data)))
	var fixtures map[string][]byte
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 2)
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	compiler.tables["t"] = &planpb.TableDef{Name: "t"}
	for name, sql := range map[string]string{
		"signed":   "create table t(a decimal(10,2), b decimal(10,2), g bigint generated always as (a div b) stored, check(a div b > 0))",
		"unsigned": "create table t(a decimal(10,2), b bigint unsigned, g bigint generated always as (a div b) stored, check(a div b > 0))",
	} {
		t.Run(name, func(t *testing.T) {
			var source planpb.TableDef
			require.NoError(t, proto.Unmarshal(fixtures[name], &source))
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(compiler, stmt, false)
			require.NoError(t, err)
			target := built.GetDdl().GetCreateTable().GetTableDef()
			require.Len(t, target.Checks, len(source.Checks))
			for i := range target.Checks {
				target.Checks[i].Name = source.Checks[i].Name
			}
			oldNodes := tableDumpIntegerDivNodes(source.Cols[2].GeneratedCol.Expr)
			newNodes := tableDumpIntegerDivNodes(target.Cols[2].GeneratedCol.Expr)
			require.Len(t, oldNodes, 1)
			require.Len(t, newNodes, 1)
			_, oldOverload := function.DecodeOverloadID(oldNodes[0].GetF().Func.Obj)
			_, newOverload := function.DecodeOverloadID(newNodes[0].GetF().Func.Obj)
			require.Equal(t, int32(0), oldOverload)
			require.Equal(t, int32(types.T_int64), oldNodes[0].Typ.Id)
			require.Zero(t, oldNodes[0].Typ.Width)
			require.Positive(t, newNodes[0].Typ.Width)
			if name == "unsigned" {
				require.Equal(t, int32(function.DecimalIntegerDivUnsignedOverload), newOverload)
				require.Equal(t, int32(types.T_uint64), newNodes[0].Typ.Id)
			} else {
				require.Equal(t, int32(0), newOverload)
			}
			snapshot := proto.Clone(&source)
			sensitive, err := AnalyzeTableDumpBindings(compiler, &source, &source)
			require.NoError(t, err)
			require.True(t, sensitive)
			_, err = AnalyzeTableDumpBindings(compiler, target, &source)
			require.NoError(t, err)
			require.True(t, proto.Equal(snapshot, &source))
			tampered := proto.Clone(&source).(*planpb.TableDef)
			tampered.Checks[0].Check = &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bool)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}}}}
			_, err = AnalyzeTableDumpBindings(compiler, target, tampered)
			require.ErrorContains(t, err, "cannot verify bound check expression")
		})
	}
}

func tableDumpIntegerDivNodes(expr *planpb.Expr) []*planpb.Expr {
	var result []*planpb.Expr
	var visit func(*planpb.Expr)
	visit = func(e *planpb.Expr) {
		if e == nil {
			return
		}
		if f := e.GetF(); f != nil {
			id, _ := function.DecodeOverloadID(f.Func.Obj)
			if id == function.INTEGER_DIV {
				result = append(result, e)
			}
			for _, arg := range f.Args {
				visit(arg)
			}
		}
	}
	visit(expr)
	return result
}

func TestTableDumpRejectsMixedDecimalDivEra(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	compiler.tables["t"] = &planpb.TableDef{Name: "t"}
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"create table t(a decimal(10,2),b bigint unsigned,check((a div b)+(a div b)>0))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	build := func(ctx CompilerContext) *planpb.TableDef {
		p, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		return p.GetDdl().GetCreateTable().GetTableDef()
	}
	current := build(compiler)
	legacy := build(tableDumpBindContext{CompilerContext: compiler, ctx: function.WithLegacyDecimalIntegerDiv(compiler.GetContext()), increment: 4})
	for _, source := range []*planpb.TableDef{current, legacy} {
		_, err := AnalyzeTableDumpBindings(compiler, current, source)
		require.NoError(t, err)
	}
	oldNodes := tableDumpIntegerDivNodes(legacy.Checks[0].Check)
	require.Len(t, oldNodes, 2)
	mixed := proto.Clone(current).(*planpb.TableDef)
	newNodes := tableDumpIntegerDivNodes(mixed.Checks[0].Check)
	require.Len(t, newNodes, 2)
	require.False(t, proto.Equal(oldNodes[0], newNodes[0]))
	*newNodes[0] = *proto.Clone(oldNodes[0]).(*planpb.Expr)
	before := proto.Clone(mixed)
	_, err = AnalyzeTableDumpBindings(compiler, current, mixed)
	require.ErrorContains(t, err, "cannot verify bound check expression")
	require.True(t, proto.Equal(before, mixed))
}

func TestTableDumpDecimalDivEraComposition(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	compiler.tables["t"] = &planpb.TableDef{Name: "t"}
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"create table t(a decimal(10,2),b bigint unsigned,g datetime(6) generated always as(timestampadd(second,(a div b)+0.5,cast('2024-01-01' as datetime(6)))) stored)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	var target *planpb.TableDef
	for _, era := range []struct {
		name string
		ctx  context.Context
	}{
		{"current", compiler.GetContext()},
		{"before_fractional", function.WithLegacyTimestampAddSecond(compiler.GetContext())},
		{"before_div", function.WithLegacyTemporalNullability(function.WithLegacyDecimalIntegerDiv(compiler.GetContext()))},
	} {
		t.Run(era.name, func(t *testing.T) {
			ctx := tableDumpBindContext{CompilerContext: compiler, ctx: era.ctx, increment: 4}
			built, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			source := built.GetDdl().GetCreateTable().GetTableDef()
			// Independent typed contracts: published post-8dd main retains
			// integer SECOND without function 601, but its string DATETIME
			// conversion is nullable. Pre-DIV carries the old metadata.
			casts, normalizers := 0, 0
			var inspect func(*planpb.Expr)
			inspect = func(e *planpb.Expr) {
				if f := e.GetF(); f != nil {
					fid, oid := function.DecodeOverloadID(f.Func.Obj)
					if fid == function.TIMESTAMPADD_SECOND_INTERVAL {
						normalizers++
					}
					if fid == function.TIMESTAMPADD {
						if era.name == "current" {
							require.Equal(t, "MICROSECOND", f.Args[0].GetLit().GetSval())
						} else {
							require.Equal(t, "second", f.Args[0].GetLit().GetSval())
							require.Equal(t, int32(types.T_int64), f.Args[1].Typ.Id)
						}
					}
					if fid == function.CAST && (oid == 0 || oid == 1) && len(f.Args) == 2 && types.T(f.Args[0].Typ.Id).IsMySQLString() && types.T(f.Args[1].Typ.Id) == types.T_datetime {
						casts++
						require.Equal(t, era.name == "before_div", e.Typ.NotNullable)
						require.Equal(t, e.Typ.NotNullable, f.Args[1].Typ.NotNullable)
					}
					for _, arg := range f.Args {
						inspect(arg)
					}
				}
			}
			inspect(source.Cols[2].GeneratedCol.Expr)
			require.Positive(t, casts)
			if era.name == "current" {
				require.Equal(t, 1, normalizers)
			} else {
				require.Zero(t, normalizers)
			}
			if target == nil {
				target = source
			}
			before := proto.Clone(source)
			sensitive, err := AnalyzeTableDumpBindings(compiler, target, source)
			require.NoError(t, err)
			require.True(t, sensitive)
			require.True(t, proto.Equal(before, source))
		})
	}
}

// Genuine incoming f9271d9780 bindings, before main 8dd623e7 changed nullability.
func TestTableDumpPreTemporalNullabilityBindings(t *testing.T) {
	data, err := os.ReadFile("testdata/temporal_nullability_pre8dd_baseline.json")
	require.NoError(t, err)
	var fixtures map[string][]byte
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 8)
	exprs := map[string]string{"date": "date(a)", "datetime": "cast(a as datetime)", "implicit": "date_format(a,'%Y-%m-%d')", "fractional": "timestampadd(second,1.5,cast(a as datetime))", "date_constant": "date('2024-01-01')", "datetime_constant": "cast('2024-01-01' as datetime)", "implicit_constant": "date_format('2024-01-01','%Y-%m-%d')", "fractional_constant": "timestampadd(second,1.5,cast('2024-01-01' as datetime))"}
	for name, raw := range fixtures {
		t.Run(name, func(t *testing.T) {
			var source planpb.TableDef
			require.NoError(t, proto.Unmarshal(raw, &source))
			typ := "datetime"
			if name == "date" || name == "date_constant" {
				typ = "date"
			}
			if name == "implicit" || name == "implicit_constant" {
				typ = "varchar(64)"
			}
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table t(a varchar(32) not null,g "+typ+" generated always as ("+exprs[name]+") stored,check("+exprs[name]+" is not null))", 1)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			target := built.GetDdl().GetCreateTable().GetTableDef()
			require.Len(t, target.Checks, len(source.Checks))
			for i := range target.Checks {
				target.Checks[i].Name = source.Checks[i].Name
			}
			before := proto.Clone(target)
			_, err = AnalyzeTableDumpBindings(ctx, target, target)
			require.NoError(t, err)
			_, err = AnalyzeTableDumpBindings(ctx, target, &source)
			require.NoError(t, err)
			if len(name) > 9 && name[len(name)-9:] == "_constant" {
				require.False(t, proto.Equal(source.Cols[1].GeneratedCol.Expr, target.Cols[1].GeneratedCol.Expr))
				// Nullability is still exact provenance. A forged source-literal
				// bit is impossible under both whole binding eras.
				forged := proto.Clone(&source).(*planpb.TableDef)
				var corrupt func(*planpb.Expr) bool
				corrupt = func(expr *planpb.Expr) bool {
					if lit := expr.GetLit(); lit != nil && expr.Typ.NotNullable && types.T(expr.Typ.Id).IsMySQLString() {
						expr.Typ.NotNullable = false
						return true
					}
					if f := expr.GetF(); f != nil {
						for _, a := range f.Args {
							if corrupt(a) {
								return true
							}
						}
					}
					return false
				}
				require.True(t, corrupt(forged.Cols[1].GeneratedCol.Expr))
				_, err = AnalyzeTableDumpBindings(ctx, target, forged)
				require.ErrorContains(t, err, "cannot verify bound generated")
			}
			require.True(t, proto.Equal(before, target))
		})
	}
}

func TestTableDumpRejectsMixedTemporalNullability(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table t(g varchar(64) generated always as (concat(cast('2024-01-01' as datetime),cast('2024-02-01' as datetime))) stored)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	target := built.GetDdl().GetCreateTable().GetTableDef()
	forged := proto.Clone(target).(*planpb.TableDef)
	var casts []*planpb.Expr
	var collect func(*planpb.Expr)
	collect = func(e *planpb.Expr) {
		if f := e.GetF(); f != nil {
			fid, oid := function.DecodeOverloadID(f.Func.Obj)
			if fid == function.CAST && (oid == 0 || oid == 1) && len(f.Args) == 2 && types.T(f.Args[0].Typ.Id).IsMySQLString() && types.T(f.Args[1].Typ.Id) == types.T_datetime {
				casts = append(casts, e)
			}
			for _, a := range f.Args {
				collect(a)
			}
		}
	}
	collect(forged.Cols[0].GeneratedCol.Expr)
	require.Len(t, casts, 2)
	for _, e := range casts {
		require.False(t, e.Typ.NotNullable)
		require.False(t, e.GetF().Args[1].Typ.NotNullable)
	}
	// One complete old cast and one complete current cast cannot come from a
	// single binding epoch, even though each subtree is independently valid.
	casts[0].Typ.NotNullable = true
	casts[0].GetF().Args[1].Typ.NotNullable = true
	before := proto.Clone(forged)
	_, err = AnalyzeTableDumpBindings(ctx, target, forged)
	require.ErrorContains(t, err, "cannot verify bound generated")
	require.True(t, proto.Equal(before, forged))
}
