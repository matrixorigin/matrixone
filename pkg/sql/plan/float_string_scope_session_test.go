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
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func legacyFloatScopeTable(t *testing.T, sql string, precision int32) (*MockCompilerContext, *planpb.TableDef) {
	t.Helper()
	compiler := floatCatalogCompiler(t)
	proc := compiler.GetProcess()
	proc.GetSessionInfo().TimeZone = time.FixedZone("creation", 8*3600)
	proc.GetSessionInfo().LCTimeNames = "de_DE"
	creation := function.WithDivPrecisionIncrement(function.WithLegacyFloatStringFormatting(function.WithLegacyFloatStringBounds(compiler.GetContext())), precision)
	compiler.SetContext(creation)
	proc.Ctx = creation
	fallback := *compiler
	compiler.ResolveVariableFunc = func(name string, system, global bool) (any, error) {
		if name == "div_precision_increment" {
			return int64(precision), nil
		}
		return fallback.ResolveVariable(name, system, global)
	}
	// The ordinary CREATE owner binds against the pre-catalog column scope.
	created := tableDumpDDLCreate(t, compiler, sql)
	catalog := tableDumpDDLCatalog(created)
	compiler.ResolveVariableFunc = nil
	compiler.SetContext(context.Background())
	proc.Ctx = compiler.GetContext()
	proc.GetSessionInfo().TimeZone = time.UTC
	proc.GetSessionInfo().LCTimeNames = "en_US"
	return compiler, catalog
}

func executeLegacyFloatScope(t *testing.T, compiler *MockCompilerContext, def *planpb.TableDef) string {
	t.Helper()
	col := tableDumpDDLColumn(t, def, "g")
	expr := col.Default.GetExpr()
	if col.GeneratedCol != nil {
		expr = col.GeneratedCol.Expr
	}
	require.NotNil(t, expr)
	proc := compiler.GetProcess()
	exec, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer exec.Free()
	bat := batch.NewWithSize(1)
	defer bat.Clean(proc.Mp())
	bat.Vecs[0] = vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], -1.2345678901234567e-15, false, proc.Mp()))
	bat.SetRowCount(1)
	result, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	return result.GetStringAt(0)
}

func TestLegacyFloatCatalogColumnScopes(t *testing.T) {
	const full = "-0.0000000000000012345678901234568"
	for _, tc := range []struct{ name, input, output string }{
		{"nullable_generated", "double", "varchar(100) generated always as (concat(a)) stored"},
		{"not_null_generated", "double not null", "varchar(100) generated always as (concat(a)) stored"},
		{"primary_generated", "double primary key", "varchar(100) generated always as (concat(a)) stored"},
		{"not_null_default", "double not null", "varchar(100) default(concat(a))"},
		{"primary_default", "double primary key", "varchar(100) default(concat(a))"},
		{"target_not_null", "double not null", "varchar(100) not null default(concat(a))"},
		{"repeated_not_null", "double not null", "varchar(100) default(concat(a,a))"},
		{"explicit_control", "double not null", "varchar(100) default(concat(cast(a as char(32))))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler, source := legacyFloatScopeTable(t, "create table t(a "+tc.input+",g "+tc.output+")", 4)
			before := proto.Clone(source)
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			want := full
			if tc.name == "explicit_control" {
				want = full[:32]
			}
			if tc.name == "repeated_not_null" {
				want += full
			}
			require.Equal(t, want, executeLegacyFloatScope(t, compiler, execution))
			require.True(t, proto.Equal(before, source))
			migrated := proto.Clone(execution)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, proto.Equal(migrated, execution))
			require.Equal(t, tableDumpDDLColumn(t, source, "a").Typ, tableDumpDDLColumn(t, execution, "a").Typ, "catalog declarations retain NOT NULL")
			// ALTER can author against the catalog scope; retain that complete
			// witness too, rather than forcing every expression into CREATE scope.
			previous := compiler.GetContext()
			legacy := function.WithLegacyFloatStringBounds(function.WithLegacyFloatStringFormatting(previous))
			compiler.SetContext(legacy)
			compiler.GetProcess().Ctx = legacy
			alter := tableDumpDDLCatalogAuthored(t, compiler, source, "create table t(a "+tc.input+",g "+tc.output+")")
			compiler.SetContext(previous)
			compiler.GetProcess().Ctx = previous
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, alter))
			require.Equal(t, want, executeLegacyFloatScope(t, compiler, alter))
			if tc.name == "repeated_not_null" {
				mixed := proto.Clone(source).(*planpb.TableDef)
				edited := false
				require.NoError(t, planpb.VisitExprTree(tableDumpDDLColumn(t, mixed, "g").Default.Expr, func(e *planpb.Expr) error {
					if e.GetCol() != nil && !edited {
						e.Typ.NotNullable = true
						edited = true
					}
					return nil
				}))
				require.True(t, edited)
				before := proto.Clone(mixed)
				require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, mixed))
				require.True(t, proto.Equal(before, mixed), "a mixture of column scopes cannot authorize repair")
			}
		})
	}
}

func TestLegacyFloatPrecisionSessionBudget(t *testing.T) {
	const division = "cast(1 as decimal(10,0))/cast(7 as decimal(10,0))"
	const full = "-0.0000000000000012345678901234568"
	for _, tc := range []struct {
		name, input, suffix, sessionSuffix string
		precision                          int32
		sameZone                           bool
	}{
		{"precision4", "double", "from_unixtime(0)", "1970-01-01 08:00:00", 4, false},
		{"precision30", "double", "from_unixtime(0)", "1970-01-01 08:00:00", 30, false},
		{"same_zone30", "double", "from_unixtime(0)", "1970-01-01 08:00:00", 30, true},
		{"combined_scope_locale30", "double not null", "date_format(from_unixtime(0),'%M %H')", "Januar 08", 30, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler, source := legacyFloatScopeTable(t, "create table t(a "+tc.input+",g varchar(200) default(concat(a,"+tc.suffix+","+division+")))", tc.precision)
			if tc.sameZone {
				compiler.GetProcess().GetSessionInfo().TimeZone = time.FixedZone("reading", 8*3600)
			}
			zone := compiler.GetProcess().GetSessionInfo().TimeZone
			before := proto.Clone(source)
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			suffix := "0.1429"
			if tc.precision == 30 {
				suffix = "0.142857142857142857142857142857"
			}
			require.Equal(t, full+tc.sessionSuffix+suffix, executeLegacyFloatScope(t, compiler, execution))
			require.True(t, proto.Equal(before, source))
			require.Same(t, zone, compiler.GetProcess().GetSessionInfo().TimeZone)
		})
	}
}

func TestLegacyFloatSessionCandidatePruning(t *testing.T) {
	compiler, source := legacyFloatScopeTable(t, "create table t(a double,g varchar(200) default(concat(a,from_unixtime(0),cast(1 as decimal(10,0))/cast(7 as decimal(10,0)))))", 30)
	col := tableDumpDDLColumn(t, source, "g")
	item := tableDumpExpression{kind: "default", name: col.Name, col: col, expr: col.Default.Expr, origin: col.Default.OriginString}
	stmt, ast, _, err := parseTableDumpExpression(compiler.GetContext(), item.origin, "")
	require.NoError(t, err)
	defer stmt.Free()
	dependency := &tableDumpDefaultSession{captureUnfolded: true}
	bound, err := bindTableDumpExpression(compiler, source, item, ast, 0, tableDumpBindingLegacyFloat|tableDumpBindingLegacyFloatFormatting, dependency)
	require.NoError(t, err)
	possible, err := legacyFloatSessionCandidateMayMatch(compiler.GetContext(), item.expr, bound, dependency.unfolded, &tableDumpBindingBudget{})
	require.NoError(t, err)
	require.False(t, possible, "an independent wrong-precision fraction cannot be fixed by a timezone")
	// A whole folded session-dependent subtree cannot be aligned with its
	// children: differing values remain candidates, never false negatives.
	stored := makePlan2StringConstExprWithType("January", false)
	folded := makePlan2StringConstExprWithType("Januar", false)
	// ConstantFold preserves the source's bound type, not the rendered length.
	folded.Typ = stored.Typ
	ambiguous := &planpb.Expr{Typ: stored.Typ, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "monthname"}, Args: []*planpb.Expr{makePlan2DateConstExprWithType(0)}}}}
	possible, err = legacyFloatSessionCandidateMayMatch(compiler.GetContext(), stored, folded, ambiguous, &tableDumpBindingBudget{})
	require.NoError(t, err)
	require.True(t, possible)
	_, err = legacyFloatSessionCandidateMayMatch(compiler.GetContext(), item.expr, bound, dependency.unfolded, &tableDumpBindingBudget{visits: tableDumpMaxCandidateVisits})
	require.ErrorContains(t, err, "binding exceeds limit")
	canceled, cancel := context.WithCancel(compiler.GetContext())
	cancel()
	_, err = legacyFloatSessionCandidateMayMatch(canceled, item.expr, bound, dependency.unfolded, &tableDumpBindingBudget{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestLegacyFloatNestedPrecisionSessionBudget(t *testing.T) {
	const division = "cast(1 as decimal(10,0))/cast(7 as decimal(10,0))"
	for _, tc := range []struct {
		name, input, expression, sessionSuffix, extra string
		precision                                     int32
	}{
		{"nested30", "double", "concat(from_unixtime(0)," + division + ")", "1970-01-01 08:00:00", "", 30},
		{"nested4", "double", "concat(from_unixtime(0)," + division + ")", "1970-01-01 08:00:00", "", 4},
		{"deeper30", "double", "concat(concat(from_unixtime(0)," + division + "),'!')", "1970-01-01 08:00:00", "!", 30},
		{"scope_locale30", "double not null", "concat(date_format(from_unixtime(0),'%M %H')," + division + ")", "Januar 08", "", 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler, source := legacyFloatScopeTable(t, "create table t(a "+tc.input+",g varchar(200) default(concat(a,"+tc.expression+")))", tc.precision)
			before := proto.Clone(source)
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			fraction := "0.1429"
			if tc.precision == 30 {
				fraction = "0.142857142857142857142857142857"
			}
			require.Equal(t, "-0.0000000000000012345678901234568"+tc.sessionSuffix+fraction+tc.extra, executeLegacyFloatScope(t, compiler, execution))
			require.True(t, proto.Equal(before, source))
			migrated := proto.Clone(execution)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, proto.Equal(migrated, execution))
			require.Same(t, time.UTC, compiler.GetProcess().GetSessionInfo().TimeZone)
			require.Equal(t, "en_US", compiler.GetProcess().GetSessionInfo().LCTimeNames)
			if tc.name == "nested30" {
				// The wrong precision collapses the nested CONCAT into one
				// literal, but its bound width still proves it impossible.
				col := tableDumpDDLColumn(t, source, "g")
				item := tableDumpExpression{kind: "default", name: col.Name, col: col, expr: col.Default.Expr, origin: col.Default.OriginString}
				stmt, ast, _, err := parseTableDumpExpression(compiler.GetContext(), item.origin, "")
				require.NoError(t, err)
				defer stmt.Free()
				for _, increment := range []int64{0, 30} {
					dependency := &tableDumpDefaultSession{captureUnfolded: true}
					bound, err := bindTableDumpExpression(compiler, source, item, ast, increment, tableDumpBindingLegacyFloat|tableDumpBindingLegacyFloatFormatting, dependency)
					require.NoError(t, err)
					original := proto.Clone(bound)
					possible, err := legacyFloatSessionCandidateMayMatch(compiler.GetContext(), item.expr, bound, dependency.unfolded, &tableDumpBindingBudget{})
					require.NoError(t, err)
					require.Equal(t, increment == 30, possible, "matching precision stays eligible despite different folded session values")
					require.True(t, proto.Equal(original, bound))
				}
			}
		})
	}
}
