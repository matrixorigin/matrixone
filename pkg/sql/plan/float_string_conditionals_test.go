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
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

// As in the literal-refinement fixture, BuildPlan used the exact CAST and
// string-bound source from 6d27391f7f6705ea08f5fee571336d546568edbb overlaid on
// the PR dependencies, independently of the migration's historical binder.
func TestLegacyFloatStringConditionals(t *testing.T) {
	data, err := os.ReadFile("testdata/mysql_float_legacy_conditionals.json")
	require.NoError(t, err)
	require.Equal(t, "6961e83b59afe5c854e21a4864cb9062060529f266f2b400f7d162ce014ac78a", fmt.Sprintf("%x", sha256.Sum256(data)))
	var fixtures map[string][]byte
	require.NoError(t, json.Unmarshal(data, &fixtures))
	const full = "-0.0000000000000012345678901234568"
	const narrow = "-0.00000000000000123456789012345"
	for _, tc := range []struct {
		name     string
		want     [3]string
		nullLast bool
	}{
		{"coalesce", [3]string{full, "1.5", ""}, false},
		{"default", [3]string{full, "1.5", ""}, false},
		{"default_assignment", [3]string{}, false},
		{"direct_coalesce", [3]string{full, "1.5", ""}, false},
		{"if", [3]string{full, "fallback", "fallback"}, false},
		{"case", [3]string{full, "", ""}, false},
		{"ifnull", [3]string{full, "1.5", "fallback"}, false},
		{"nested", [3]string{"8901234568", "1.5", ""}, false},
		{"nested_conditionals", [3]string{full, "1.5", "fallback"}, false},
		{"reverse", [3]string{full, "1.5", ""}, false},
		{"numeric_sibling", [3]string{full, "1.5", "0"}, false},
		{"explicit_source", [3]string{narrow, "1.5", ""}, false},
		{"explicit_sibling", [3]string{full, "fallback", "fallback"}, false},
		{"explicit_sibling_missing", [3]string{full, "fallback", "fallback"}, false},
		{"outer_explicit", [3]string{narrow, "1.5", ""}, false},
		{"assignment_width", [3]string{}, false},
		{"nullif", [3]string{full, "1.5", ""}, true},
		{"greatest", [3]string{full, "1.5", ""}, true},
		{"least", [3]string{full, "1.5", ""}, true},
		{"case_multiple", [3]string{full, "positive", "nil"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler := floatCatalogCompiler(t)
			fixture := strings.TrimSuffix(tc.name, "_missing")
			if tc.name == "default_assignment" {
				fixture = "default"
			}
			require.Contains(t, fixtures, fixture)
			source := new(planpb.TableDef)
			require.NoError(t, proto.Unmarshal(fixtures[fixture], source))
			if strings.HasSuffix(tc.name, "_missing") {
				require.NoError(t, planpb.VisitExprTree(source.Cols[1].GeneratedCol.Expr, func(expr *planpb.Expr) error {
					if fn := expr.GetF(); fn != nil {
						fn.SyntaxExplicitCast = false
					}
					return nil
				}))
			}
			if tc.name == "default_assignment" {
				col := source.Cols[1]
				col.Typ.Width = 32
				stmt, ast, _, err := parseTableDumpExpression(t.Context(), col.Default.OriginString, "")
				require.NoError(t, err)
				defer stmt.Free()
				col.Default.Expr, err = bindTableDumpExpression(compiler, source,
					tableDumpExpression{kind: "default", name: col.Name, col: col}, ast, 0, false, true)
				require.NoError(t, err)
			}
			snapshot := proto.Clone(source)
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, true)
			var expr *planpb.Expr
			if strings.HasPrefix(tc.name, "default") {
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
			for i, a := range []float64{-1.2345678901234567e-15, 1.5, 0} {
				require.NoError(t, vector.AppendFixed(bat.Vecs[0], a, i == 2, proc.Mp()))
			}
			bat.SetRowCount(3)
			result, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
			if strings.Contains(tc.name, "assignment") {
				// Assignment is strict; explicit CHAR(32) above intentionally
				// truncates. Widening the inner expression must not widen either
				// declared boundary or bypass assignment validation.
				require.ErrorContains(t, err, "Src length 34 is larger than Dest length 32")
			} else {
				require.NoError(t, err)
				for i, want := range tc.want {
					require.Equal(t, i == 2 && tc.nullLast, result.GetNulls().Contains(uint64(i)), "row %d nullness", i)
					if i != 2 || !tc.nullLast {
						require.Equal(t, want, result.GetStringAt(i), "row %d", i)
					}
				}
			}
			col := source.Cols[1]
			origin, kind := "", "generated"
			var migratedExpr *planpb.Expr
			if strings.HasPrefix(tc.name, "default") {
				origin, kind = col.Default.OriginString, "default"
				migratedExpr = execution.Cols[1].Default.Expr
			} else {
				origin = col.GeneratedCol.OriginString
				migratedExpr = execution.Cols[1].GeneratedCol.Expr
			}
			stmt, ast, _, err := parseTableDumpExpression(t.Context(), origin, "")
			require.NoError(t, err)
			defer stmt.Free()
			fresh, err := bindTableDumpExpression(compiler, source, tableDumpExpression{kind: kind, name: "g", col: col}, ast, 0, false)
			require.NoError(t, err)
			require.True(t, sameTableDumpBoundExpression(fresh, migratedExpr), "migrated tree must match ordinary binding")
			require.True(t, proto.Equal(snapshot, source))
			migrated := proto.Clone(execution)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, proto.Equal(migrated, execution))
		})
	}
}

func TestLegacyFloatStringRebindGuards(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	t.Run("budget", func(t *testing.T) {
		expr := legacyFloatTable(t, "generated").Cols[1].GeneratedCol.Expr
		snapshot := proto.Clone(expr)
		budget := &tableDumpBindingBudget{visits: tableDumpMaxCandidateVisits - 1}
		_, err := widenVerifiedFloatStringExpr(t.Context(), expr, budget)
		require.ErrorContains(t, err, "binding exceeds limit")
		require.True(t, proto.Equal(snapshot, expr), "budget must be checked before binding or mutating the subtree")
	})
	t.Run("reject_partial_sibling_repair", func(t *testing.T) {
		first := legacyFloatTable(t, "generated").Cols[1].GeneratedCol.Expr
		unsupported := DeepCopyExpr(first)
		require.NoError(t, planpb.VisitExprTree(unsupported, func(expr *planpb.Expr) error {
			if fn := expr.GetF(); fn != nil && fn.Func.ObjName == "concat" {
				fn.Func.ObjName = "unsupported_catalog_function"
			}
			return nil
		}))
		expr, err := BindFuncExprImplByPlanExpr(compiler.GetContext(), "concat", []*planpb.Expr{first, unsupported})
		require.NoError(t, err)
		_, err = widenVerifiedFloatStringExpr(compiler.GetContext(), expr, &tableDumpBindingBudget{})
		require.ErrorIs(t, err, errLegacyFloatStringRebind, "a repaired sibling cannot hide a failed local proof")
	})
}

func TestLegacyFloatStringBindingProfiles(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	for _, increment := range []int64{0, 8} {
		t.Run(fmt.Sprint(increment), func(t *testing.T) {
			source := legacyFloatTable(t, "division")
			source.Checks = nil
			col := source.Cols[2]
			item := tableDumpExpression{kind: "generated", name: col.Name, col: col}
			stmt, ast, _, err := parseTableDumpExpression(t.Context(), col.GeneratedCol.OriginString, "")
			require.NoError(t, err)
			defer stmt.Free()
			old, err := bindTableDumpExpression(compiler, source, item, ast, increment, false, true)
			require.NoError(t, err)
			col.GeneratedCol.Expr = old
			require.True(t, hasLegacyFloatStringCast(old))
			snapshot := proto.Clone(source)
			fresh, err := bindTableDumpExpression(compiler, source, item, ast, increment, false)
			require.NoError(t, err)
			defaultProfile, err := bindTableDumpExpression(compiler, source, item, ast, 4, false)
			require.NoError(t, err)
			require.False(t, sameTableDumpBoundExpression(fresh, defaultProfile), "profile must differ from the ordinary default")
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, sameTableDumpBoundExpression(fresh, execution.Cols[2].GeneratedCol.Expr))
			require.True(t, proto.Equal(snapshot, source))
		})
	}
}
