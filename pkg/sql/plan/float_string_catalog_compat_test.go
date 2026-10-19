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
	"strings"
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
	"github.com/stretchr/testify/require"
)

// Produced by BuildPlan with the exact func_cast.go and string_result_domain.go
// from 6d27391f7f6705ea08f5fee571336d546568edbb overlaid on the PR dependencies.
// These bytes are not produced by the historical binding implementation tested here.
func legacyFloatTable(t *testing.T, name string) *planpb.TableDef {
	t.Helper()
	data, err := os.ReadFile("testdata/mysql_float_legacy_bindings.json")
	require.NoError(t, err)
	require.Equal(t, "9b52e9451bbfd2a0dfb3407d0e67a9ed08f4a99261d6fb06924b89f0d59a8539", fmt.Sprintf("%x", sha256.Sum256(data)))
	var fixtures map[string][]byte
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Contains(t, fixtures, name)
	def := new(planpb.TableDef)
	require.NoError(t, proto.Unmarshal(fixtures[name], def))
	return def
}

func floatCatalogCompiler(t *testing.T) *MockCompilerContext {
	t.Helper()
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	compiler.tables["t"] = &planpb.TableDef{Name: "t"}
	rt := moruntime.ServiceRuntime(compiler.GetProcess().GetService())
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolAuthoringFloor} {
		old, exists := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if exists {
				rt.SetGlobalVariables(key, old)
			} else if v, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, v)
			}
		})
		rt.SetGlobalVariables(key, defines.MORPCLatestVersion)
	}
	return compiler
}

func TestLegacyFloatStringCatalogExecution(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{"generated", "-0.0000000000000012345678901234568"},
		{"nested", "'-0.0000000000000012345678901234568'"},
		{"default", "-0.0000000000000012345678901234568"},
		{"explicit", "-0.00000000000000123456789012345"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler := floatCatalogCompiler(t)
			source := legacyFloatTable(t, tc.name)
			snapshot := proto.Clone(source)
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, true)
			var expr *planpb.Expr
			var err error
			if tc.name == "default" {
				expr, err = getDefaultExprForAssignment(compiler.GetContext(), execution.Cols[1], compiler.GetProcess(), false)
			} else {
				expr, err = builder.applyGeneratedColumnAssignmentCast(DeepCopyExpr(execution.Cols[1].GeneratedCol.Expr), false)
			}
			require.NoError(t, err)
			proc := compiler.GetProcess()
			exec, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			t.Cleanup(exec.Free)
			bat := batch.NewWithSize(1)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			bat.Vecs[0] = vector.NewVec(types.T_float64.ToType())
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], -1.2345678901234567e-15, false, proc.Mp()))
			bat.SetRowCount(1)
			result, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, result.GetStringAt(0))
			require.True(t, proto.Equal(snapshot, source), "resolver migration must not mutate shared catalog storage")
			migrated := proto.Clone(execution)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, proto.Equal(migrated, execution), "migration must be idempotent")
		})
	}
}

func TestLegacyFloatStringMissingExplicitProvenance(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	for _, name := range []string{"generated", "explicit"} {
		t.Run(name, func(t *testing.T) {
			source := legacyFloatTable(t, name)
			require.NoError(t, planpb.VisitExprTree(source.Cols[1].GeneratedCol.Expr, func(expr *planpb.Expr) error {
				if fn := expr.GetF(); fn != nil {
					fn.SyntaxExplicitCast = false
				}
				return nil
			}))
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			widths := []int32{}
			require.NoError(t, planpb.VisitExprTree(execution.Cols[1].GeneratedCol.Expr, func(expr *planpb.Expr) error {
				fn := expr.GetF()
				if fn != nil && len(fn.Args) == 2 && fn.Args[0].Typ.Id == int32(types.T_float64) &&
					(expr.Typ.Id == int32(types.T_varchar) || expr.Typ.Id == int32(types.T_char)) {
					widths = append(widths, expr.Typ.Width)
				}
				return nil
			}))
			if name == "generated" {
				require.Equal(t, []int32{34}, widths)
			} else {
				require.Equal(t, []int32{32}, widths)
			}
		})
	}
}

func TestLegacyFloatStringCatalogMultipleOwners(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	source := legacyFloatTable(t, "default")
	origin := "concat(cast('-1.2345678901234567e-15' as double))"
	stmt, ast, _, err := parseTableDumpExpression(t.Context(), origin, "")
	require.NoError(t, err)
	defer stmt.Free()
	// Internal ownership proof: today's SQL grammar restricts ON UPDATE to
	// datetime functions. Build this second owner directly to verify that a
	// later replacement merges into the already detached column wrapper.
	item := tableDumpExpression{kind: "on_update", name: "g", col: source.Cols[1]}
	old, err := bindFloatTableDumpExpression(compiler, source, item, ast, 4, false, true)
	require.NoError(t, err)
	source.Cols[1].OnUpdate = &planpb.OnUpdate{Expr: old, OriginString: origin}
	snapshot := proto.Clone(source)
	execution := CloneTableDefForPlan(source, true)
	require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
	require.NotSame(t, source.Cols[1].Default, execution.Cols[1].Default)
	require.NotSame(t, source.Cols[1].OnUpdate, execution.Cols[1].OnUpdate)
	for _, expr := range []*planpb.Expr{execution.Cols[1].Default.Expr, execution.Cols[1].OnUpdate.Expr} {
		require.False(t, hasLegacyFloatStringCast(expr))
	}
	require.True(t, proto.Equal(snapshot, source))
}

func TestLegacyFloatStringCatalogProofBounds(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	source := legacyFloatTable(t, "generated")
	source.Cols[1].GeneratedCol.OriginString = strings.Repeat("a", tableDumpMaxExpressionText+1)
	require.ErrorContains(t, MigrateLegacyFloatStringTableDef(compiler, CloneTableDefForPlan(source, true)), "text exceeds limit")
	source = legacyFloatTable(t, "default")
	stored := makePlan2StringConstExprWithType("stored value")
	stored.GetLit().Src = source.Cols[1].Default.Expr
	source.Cols[1].Default.Expr = stored
	snapshot := proto.Clone(source)
	require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, source))
	require.True(t, proto.Equal(snapshot, source), "folded literal values and their provenance remain unchanged")
}

func TestLegacyFloatStringCatalogCollationProvenance(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	for _, tc := range []struct {
		name   string
		mutate func(*planpb.Type)
	}{
		{"recorded", nil},
		{"historical", func(typ *planpb.Type) { typ.CollationCoercibility, typ.CollationCoercibilitySet = 0, false }},
		{"explicit_rank", func(typ *planpb.Type) { typ.CollationCoercibility = 0 }},
		{"different_rank", func(typ *planpb.Type) { typ.CollationCoercibility = 4 }},
		{"invalid_unset_rank", func(typ *planpb.Type) { typ.CollationCoercibilitySet = false }},
		{"merge_conflict", func(typ *planpb.Type) { typ.CollationMergeConflict = true }},
		{"charset", func(typ *planpb.Type) { typ.Charset = uint32(types.CharsetUTF8MB4Bin) }},
		{"collation_revision", func(typ *planpb.Type) { typ.CollationVersion = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := legacyFloatTable(t, "generated")
			col := source.Cols[1]
			stmt, ast, _, err := parseTableDumpExpression(t.Context(), col.GeneratedCol.OriginString, "")
			require.NoError(t, err)
			defer stmt.Free()
			item := tableDumpExpression{kind: "generated", name: col.Name, col: col}
			bound, err := bindTableDumpExpression(compiler, source, item, ast, 0, false, true)
			require.NoError(t, err)
			col.GeneratedCol.Expr = bound
			concat := bound.GetF().Args[0]
			require.Equal(t, "concat", concat.GetF().Func.ObjName)
			require.True(t, concat.Typ.CollationCoercibilitySet)
			require.Equal(t, uint32(2), concat.Typ.CollationCoercibility)
			recorded := DeepCopyExpr(bound)
			if tc.mutate != nil {
				tc.mutate(&concat.Typ)
			}
			require.Equal(t, tc.mutate == nil, sameTableDumpBoundExpression(recorded, bound), "ordinary equality must retain provenance")
			snapshot := proto.Clone(source)
			accept := tc.name == "recorded" || tc.name == "historical"
			candidateBefore := proto.Clone(recorded)
			require.Equal(t, accept, sameHistoricalFloatBoundExpression(bound, recorded))
			require.True(t, proto.Equal(candidateBefore, recorded), "DUMP candidate comparison must not mutate its input")
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, proto.Equal(snapshot, source))
			if !accept {
				require.True(t, proto.Equal(source, execution), "contradictory provenance must reject the repair")
			} else {
				require.False(t, hasLegacyFloatStringCast(execution.Cols[1].GeneratedCol.Expr))
			}
		})
	}
}

func TestLegacyFloatStringCatalogNativeProvenance(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"create table t(a double, g varchar(100) collate utf8mb4_unicode_ci generated always as (concat(a/1) collate utf8mb4_unicode_ci) stored)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(compiler, stmt, false)
	require.NoError(t, err)
	source := built.GetDdl().GetCreateTable().GetTableDef()
	col := source.Cols[1]
	parsed, ast, _, err := parseTableDumpExpression(t.Context(), col.GeneratedCol.OriginString, "")
	require.NoError(t, err)
	defer parsed.Free()
	item := tableDumpExpression{kind: "generated", name: col.Name, col: col}
	bound, err := bindTableDumpExpression(compiler, source, item, ast, 0, false, true)
	require.NoError(t, err)
	col.GeneratedCol.Expr = bound
	require.NotEmpty(t, tableDumpExpressions(source))
	require.True(t, hasLegacyFloatStringCast(bound))
	_, err = AnalyzeTableDumpBindings(compiler, source, source)
	require.NoError(t, err, "recorded native provenance remains admissible")
	edited := false
	require.NoError(t, planpb.VisitExprTree(bound, func(expr *planpb.Expr) error {
		if !edited && expr.Typ.CollationVersion == uint32(types.CollationVersionV1) && expr.Typ.CollationCoercibilitySet {
			expr.Typ.CollationCoercibility, expr.Typ.CollationCoercibilitySet = 0, false
			edited = true
		}
		return nil
	}))
	require.True(t, edited)
	snapshot := proto.Clone(source)
	_, err = AnalyzeTableDumpBindings(compiler, source, source)
	require.ErrorContains(t, err, "cannot verify bound generated expression")
	execution := CloneTableDefForPlan(source, true)
	require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
	require.True(t, proto.Equal(source, execution), "missing native provenance cannot authorize repair")
	require.True(t, proto.Equal(snapshot, source))
}

func TestLegacyFloatStringCatalogCanceled(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	ctx, cancel := context.WithCancel(compiler.GetContext())
	cancel()
	compiler.SetContext(ctx)
	source := legacyFloatTable(t, "generated")
	execution := CloneTableDefForPlan(source, true)
	require.ErrorIs(t, MigrateLegacyFloatStringTableDef(compiler, execution), context.Canceled)
	require.True(t, proto.Equal(source, execution), "canceled resolution must not publish a repair")
}

const floatDivisionDDL = "create table t(a decimal(10,2), b decimal(10,2), g varchar(100) generated always as (concat(cast(a/b as double))) stored, check (length(concat(cast(a/b as double))) < 100))"

func TestTableDumpLegacyFloatStringBindings(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	source := legacyFloatTable(t, "division")
	snapshot := proto.Clone(source)
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, floatDivisionDDL, 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(compiler, stmt, false)
	require.NoError(t, err)
	target := built.GetDdl().GetCreateTable().GetTableDef()
	for _, def := range []*planpb.TableDef{source, target} {
		sensitive, err := AnalyzeTableDumpBindings(compiler, def, source)
		require.NoError(t, err)
		require.True(t, sensitive)
	}
	// Both actual predecessor bytes and a marked legacy-width binder era must
	// be admitted by the DUMP owner; the per-marker guard matrix is tested once
	// through its shared candidate comparison above.
	marked := proto.Clone(source).(*planpb.TableDef)
	col := marked.Cols[2]
	legacyStmt, legacyAST, _, err := parseTableDumpExpression(t.Context(), col.GeneratedCol.OriginString, "")
	require.NoError(t, err)
	defer legacyStmt.Free()
	col.GeneratedCol.Expr, err = bindTableDumpExpression(compiler, marked,
		tableDumpExpression{kind: "generated", name: col.Name, col: col}, legacyAST, 0, false, true)
	require.NoError(t, err)
	markedBefore := proto.Clone(marked)
	sensitive, err := AnalyzeTableDumpBindings(compiler, target, marked)
	require.NoError(t, err)
	require.True(t, sensitive)
	require.True(t, proto.Equal(markedBefore, marked))
	require.True(t, proto.Equal(snapshot, source))
	execution := CloneTableDefForPlan(source, true)
	require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
	require.True(t, sameTableDumpBoundExpression(target.Cols[2].GeneratedCol.Expr, execution.Cols[2].GeneratedCol.Expr))
	require.True(t, sameTableDumpBoundExpression(target.Checks[0].Check, execution.Checks[0].Check))
	require.True(t, proto.Equal(snapshot, source), "CHECK wrappers must also be detached")
	for _, mutation := range []string{"width", "overload", "literal", "reference", "explicit provenance", "explicit coercibility"} {
		t.Run(mutation, func(t *testing.T) {
			tampered := proto.Clone(source).(*planpb.TableDef)
			root := tampered.Cols[2].GeneratedCol.Expr
			edited := false
			require.NoError(t, planpb.VisitExprTree(root, func(expr *planpb.Expr) error {
				if edited {
					return nil
				}
				fn := expr.GetF()
				if mutation == "reference" && expr.GetCol() != nil {
					expr.GetCol().ColPos = 1
					edited = true
				}
				if fn == nil || len(fn.Args) != 2 || fn.Args[0].Typ.Id != int32(types.T_float64) || expr.Typ.Id != int32(types.T_varchar) {
					return nil
				}
				switch mutation {
				case "width":
					expr.Typ.Width = 31
					fn.Args[1].Typ.Width = 31
				case "overload":
					fn.Func.Obj = 0
				case "literal":
					fn.Args[0] = makePlan2Float64ConstExprWithType(1)
				case "explicit provenance":
					fn.SyntaxExplicitCast = true
				case "explicit coercibility":
					expr.Typ.CollationCoercibility, expr.Typ.CollationCoercibilitySet = 0, true
				default:
					return nil
				}
				edited = true
				return nil
			}))
			require.True(t, edited)
			_, err := AnalyzeTableDumpBindings(compiler, target, tampered)
			require.ErrorContains(t, err, "cannot verify bound generated expression")
			untouched := proto.Clone(tampered)
			execution := CloneTableDefForPlan(tampered, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, proto.Equal(untouched, tampered), "failed origin proof must preserve storage")
			require.True(t, proto.Equal(tampered.Cols[2].GeneratedCol.Expr, execution.Cols[2].GeneratedCol.Expr),
				"a contradictory origin must not authorize execution-tree changes")
		})
	}
}
