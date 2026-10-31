// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func buildFoldedDefaultFixture(t *testing.T, table, ddl string) *plan.TableDef {
	t.Helper()
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	compiler.tables[table] = &plan.TableDef{Name: table}
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, ddl, 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(tableDumpBindContext{CompilerContext: compiler, ctx: compiler.GetContext(), increment: 4}, stmt, false)
	require.NoError(t, err)
	return built.GetDdl().GetCreateTable().GetTableDef()
}

// defaultConcatArgs returns the variadic concat arguments of a catalog
// default expression: the outer strict cast wraps the single concat node.
func defaultConcatArgs(t *testing.T, expr *plan.Expr) []*plan.Expr {
	t.Helper()
	outer := expr.GetF()
	require.NotNil(t, outer, "catalog default expressions wrap in a strict cast")
	inner := outer.Args[0].GetF()
	require.NotNil(t, inner, "the folded-default fixture keeps a concat node")
	require.Equal(t, "concat", inner.Func.ObjName)
	return inner.Args
}

// findTimeStringFunction returns the TIME() string conversion function node
// of a stored expression, when constant folding preserved it.
func findTimeStringFunction(expr *plan.Expr) *plan.Expr {
	if f := expr.GetF(); f != nil {
		fid, oid := function.DecodeOverloadID(f.Func.Obj)
		if fid == function.TIME && oid >= 5 && oid <= 8 {
			return expr
		}
		for _, arg := range f.Args {
			if hit := findTimeStringFunction(arg); hit != nil {
				return hit
			}
		}
	}
	return nil
}

// TestTableDumpTimeFoldedDefaultEra covers the folded-default shape from
// review 5691976414: a pre-fix catalog persisted
// concat(time('12:00:00'),date('2024-01-01'),a) with the folded TIME-derived
// literal NOT NULL while the DATE-derived literal was already nullable.
// Constant folding removes the TIME function node and replaces it with a
// literal that preserves the conversion's nullability metadata, so
// applicability of the intermediate era must come from the SQL origin, and
// the dump consumer must re-verify the published folded metadata.
func TestTableDumpTimeFoldedDefaultEra(t *testing.T) {
	const table = "fd"
	def := buildFoldedDefaultFixture(t, table,
		"create table fd(a varchar(25),b varchar(64) default concat(time('12:00:00'),date('2024-01-01'),a))")
	args := defaultConcatArgs(t, def.Cols[1].Default.GetExpr())
	timeLit, dateLit := args[0], args[1]
	require.NotNil(t, timeLit.GetLit(), "time('...') folds into a literal inside concat")
	require.NotNil(t, dateLit.GetLit(), "date('...') folds into a literal inside concat")
	require.True(t, types.T(timeLit.Typ.Id).IsMySQLString())
	// Current binding folds both conversions with the row-local NULL
	// synthesis contract.
	require.False(t, timeLit.Typ.NotNullable)
	require.False(t, dateLit.Typ.NotNullable)
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	compiler.tables[table] = &plan.TableDef{Name: table}
	_, err := AnalyzeTableDumpBindings(compiler, def, def)
	require.NoError(t, err)

	persisted := proto.Clone(def).(*plan.TableDef)
	persistedArgs := defaultConcatArgs(t, persisted.Cols[1].Default.GetExpr())
	// The published intermediate-era metadata: the TIME-derived literal is
	// NOT NULL while the DATE-derived literal stays nullable.
	persistedArgs[0].Typ.NotNullable = true
	_, err = AnalyzeTableDumpBindings(compiler, persisted, persisted)
	require.NoError(t, err)
}

// TestTableDumpTimeJSONEraDefault covers the post-JSON/pre-TIME window from
// review 5691976414: concat(time('839:00:00'),json_length('null'),a)
// persisted by a pre-fix catalog folds json_length with current JSON
// semantics while the TIME string conversion stays NOT NULL (the
// warning-producing literal keeps its function node). The legacy-JSON
// rewrite must not consume the only intermediate-era candidate for that
// default; the post-JSON/pre-TIME combination stays available independently.
func TestTableDumpTimeJSONEraDefault(t *testing.T) {
	const table = "jd"
	const ddl = "create table jd(a varchar(25),b varchar(64) default concat(time('839:00:00'),json_length('null'),a))"
	def := buildFoldedDefaultFixture(t, table, ddl)
	args := defaultConcatArgs(t, def.Cols[1].Default.GetExpr())
	timeNode := findTimeStringFunction(args[0])
	require.NotNil(t, timeNode, "the warning-producing time literal keeps its function node")
	require.False(t, timeNode.Typ.NotNullable)
	jsonLit := args[1]
	require.NotNil(t, jsonLit.GetLit(), "json_length('null') folds with current JSON semantics")
	require.True(t, jsonLit.Typ.NotNullable)
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	compiler.tables[table] = &plan.TableDef{Name: table}
	_, err := AnalyzeTableDumpBindings(compiler, def, def)
	require.NoError(t, err)

	// The published intermediate-era fixture binds the same DDL under the
	// dump-origin era: the conversion, its derived cast metadata and the
	// fold carry the pre-fix binding, while json_length keeps current
	// JSON-era semantics.
	eraCompiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	eraCompiler.tables[table] = &plan.TableDef{Name: table}
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, ddl, 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(tableDumpBindContext{
		CompilerContext: eraCompiler,
		ctx:             function.WithLegacyTimeNullability(eraCompiler.GetContext()),
		increment:       4,
	}, stmt, false)
	require.NoError(t, err)
	persisted := built.GetDdl().GetCreateTable().GetTableDef()
	persistedArgs := defaultConcatArgs(t, persisted.Cols[1].Default.GetExpr())
	persistedTime := findTimeStringFunction(persistedArgs[0])
	require.NotNil(t, persistedTime)
	require.True(t, persistedTime.Typ.NotNullable)
	require.True(t, persistedArgs[0].Typ.NotNullable)
	require.NotNil(t, persistedArgs[1].GetLit(), "current JSON semantics fold the stored value")
	_, err = AnalyzeTableDumpBindings(eraCompiler, persisted, persisted)
	require.NoError(t, err)
}

// TestReviewFixturesFromFrozenBase replays the stored-default fixtures
// regenerated on the pre-PR frozen base itself (testdata/frozen5691976414,
// see PROVENANCE.md); the artifacts leave the frozen base's own published
// metadata untouched. fd's folded TIME-derived literal is NOT NULL while the
// DATE-derived literal is nullable, and jd keeps the TIME conversion node and
// its cast metadata NOT NULL with json_length folded under current JSON
// semantics. Both production consumers must re-verify them: the DUMP export
// path calls AnalyzeTableDumpBindings(ctx, def, def) and the LOAD path calls
// AnalyzeTableDumpBindings(ctx, targetDef, restoredDef) with the catalog
// target and the manifest-restored expressions (frontend/table_dump.go).
func TestReviewFixturesFromFrozenBase(t *testing.T) {
	for _, table := range []string{"fd", "jd"} {
		t.Run(table, func(t *testing.T) {
			ddl := "create table " + table +
				"(a varchar(25),b varchar(64) default concat(time('12:00:00'),date('2024-01-01'),a))"
			if table == "jd" {
				ddl = "create table jd(a varchar(25),b varchar(64) default concat(time('839:00:00'),json_length('null'),a))"
			}
			blob, err := os.ReadFile(filepath.Join("testdata", "frozen5691976414", table+".tabledef.pb"))
			require.NoError(t, err)
			fixture := new(plan.TableDef)
			require.NoError(t, fixture.Unmarshal(blob))
			stored := fixture.Cols[1].Default.GetExpr()
			require.NotNil(t, stored)
			args := defaultConcatArgs(t, stored)
			// The frozen-base published bits are acceptance evidence and
			// must arrive here untouched.
			if table == "fd" {
				require.NotNil(t, args[0].GetLit())
				require.True(t, args[0].Typ.NotNullable, "frozen fd TIME-derived literal is NOT NULL")
				require.NotNil(t, args[1].GetLit())
				require.False(t, args[1].Typ.NotNullable, "frozen fd DATE-derived literal is nullable")
			} else {
				require.NotNil(t, findTimeStringFunction(args[0]), "frozen jd keeps the conversion node")
				require.True(t, findTimeStringFunction(args[0]).Typ.NotNullable)
				require.True(t, args[0].Typ.NotNullable, "frozen jd casts keep NOT NULL metadata")
				require.NotNil(t, args[1].GetLit())
			}
			compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
			compiler.tables[table] = &plan.TableDef{Name: table}

			// DUMP export consumer: AnalyzeTableDumpBindings(ctx, def, def).
			_, err = AnalyzeTableDumpBindings(compiler, fixture, fixture)
			require.NoError(t, err)

			// LOAD consumer: the catalog target rebuilt by the current
			// binder carries the historical stored default; the
			// manifest-restored def carries the same expression set. The
			// production call shape AnalyzeTableDumpBindings(targetDef,
			// restoredDef) must verify it.
			current := buildFoldedDefaultFixture(t, table, ddl)
			target := proto.Clone(current).(*plan.TableDef)
			target.Cols[1].Default.Expr = proto.Clone(stored).(*plan.Expr)
			restored := proto.Clone(fixture).(*plan.TableDef)
			_, err = AnalyzeTableDumpBindings(compiler, target, restored)
			require.NoError(t, err)

			// Current-shape control: the same DDL bound by the current
			// binder still self-verifies.
			_, err = AnalyzeTableDumpBindings(compiler, current, current)
			require.NoError(t, err)

			// Historical-era control (fd): the pre-8dd published shape with
			// both folded literals NOT NULL re-verifies through the
			// historical era candidate.
			if table == "fd" {
				oldest := proto.Clone(fixture).(*plan.TableDef)
				oldestArgs := defaultConcatArgs(t, oldest.Cols[1].Default.GetExpr())
				oldestArgs[1].Typ.NotNullable = true
				_, err = AnalyzeTableDumpBindings(compiler, oldest, oldest)
				require.NoError(t, err)
			}

			// Forged metadata stays rejected: a nullable column reference
			// bound NOT NULL matches no era.
			forged := proto.Clone(fixture).(*plan.TableDef)
			forgedArgs := defaultConcatArgs(t, forged.Cols[1].Default.GetExpr())
			forgedArgs[len(forgedArgs)-1].Typ.NotNullable = true
			_, err = AnalyzeTableDumpBindings(compiler, forged, forged)
			require.ErrorContains(t, err, "table dump cannot verify bound")
		})
	}
}
