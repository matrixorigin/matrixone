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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/gogo/protobuf/proto"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
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
	compiler := NewMockCompilerContext(false)
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

func TestTableDumpLegacySpecialIntegerBindings(t *testing.T) {
	compiler := NewMockCompilerContext(false)
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
