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
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func legacyFloatScopeTable(t *testing.T, sql string, precision int32, creationZones ...*time.Location) (*MockCompilerContext, *planpb.TableDef) {
	t.Helper()
	compiler := floatCatalogCompiler(t)
	proc := compiler.GetProcess()
	proc.GetSessionInfo().TimeZone = time.FixedZone("creation", 8*3600)
	if len(creationZones) != 0 {
		proc.GetSessionInfo().TimeZone = creationZones[0]
	}
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

func TestLegacyFloatOpaqueSessionWitnesses(t *testing.T) {
	const division = "cast(1 as decimal(10,0))/cast(7 as decimal(10,0))"
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	for _, tc := range []struct {
		name, input, expression, suffix string
		precision                       int32
		zone                            *time.Location
		sameZone                        bool
	}{
		{"quarter_hour", "double", "cast(concat(from_unixtime(0)," + division + ") as char(100))", "1970-01-01 05:45:00", 30, time.FixedZone("creation", 5*3600+45*60), false},
		{"precision4", "double", "cast(concat(from_unixtime(0)," + division + ") as char(100))", "1970-01-01 08:00:00", 4, time.FixedZone("creation", 8*3600), false},
		{"minute4", "double", "cast(concat(from_unixtime(0)," + division + ") as char(100))", "1970-01-01 08:01:00", 4, time.FixedZone("creation", 8*3600+60), false},
		{"minute30", "double", "cast(concat(from_unixtime(0)," + division + ") as char(100))", "1970-01-01 08:01:00", 30, time.FixedZone("creation", 8*3600+60), false},
		{"negative_minute4", "double", "cast(concat(from_unixtime(0)," + division + ") as char(100))", "1969-12-31 10:01:00", 4, time.FixedZone("creation", -(13*3600 + 59*60)), false},
		{"upper_minute30", "double", "cast(concat(from_unixtime(0)," + division + ") as char(100))", "1970-01-01 13:59:00", 30, time.FixedZone("creation", 13*3600+59*60), false},
		{"upper_minute4", "double", "cast(concat(from_unixtime(0)," + division + ") as char(100))", "1970-01-01 13:59:00", 4, time.FixedZone("creation", 13*3600+59*60), false},
		{"minute_locale30", "double not null", "cast(concat(date_format(from_unixtime(0),'%M %H:%i')," + division + ") as char(100))", "Januar 08:01", 30, time.FixedZone("creation", 8*3600+60), false},
		{"same_zone", "double", "cast(concat(from_unixtime(0)," + division + ") as char(100))", "1970-01-01 08:00:00", 30, time.FixedZone("creation", 8*3600), true},
		{"scope_locale", "double not null", "cast(concat(date_format(from_unixtime(0),'%M %H')," + division + ") as char(100))", "Januar 08", 30, time.FixedZone("creation", 8*3600), false},
		// Different winter/summer offsets require the named-rule fallback;
		// no fixed numeric offset can reproduce both frozen timestamps.
		{"named_rules", "double", "cast(concat(from_unixtime(1704067200),from_unixtime(1719792000)," + division + ") as char(100))", "2024-01-01 01:00:002024-07-01 02:00:00", 30, berlin, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler, source := legacyFloatScopeTable(t, "create table t(a "+tc.input+",g varchar(200) default(concat(a,"+tc.expression+")))", tc.precision, tc.zone)
			if tc.sameZone {
				compiler.GetProcess().GetSessionInfo().TimeZone = tc.zone
			}
			readingZone := compiler.GetProcess().GetSessionInfo().TimeZone
			before := proto.Clone(source)
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			fraction := "0.142857142857142857142857142857"
			if tc.precision == 4 {
				fraction = "0.1429"
			}
			require.Equal(t, "-0.0000000000000012345678901234568"+tc.suffix+fraction, executeLegacyFloatScope(t, compiler, execution))
			require.True(t, proto.Equal(before, source))
			if tc.name == "minute4" || tc.name == "minute30" {
				_, err := AnalyzeTableDumpBindings(compiler, execution, nil)
				require.NoError(t, err, "DUMP must admit the repaired default")
				_, err = AnalyzeTableDumpBindings(compiler, execution, execution)
				require.NoError(t, err, "LOAD must verify the supplied expression independently")
				if tc.name == "minute4" {
					forged := proto.Clone(execution).(*planpb.TableDef)
					require.NoError(t, planpb.VisitExprTree(tableDumpDDLColumn(t, forged, "g").Default.Expr, func(e *planpb.Expr) error {
						if e.GetCol() != nil {
							e.Typ.NotNullable = !e.Typ.NotNullable
						}
						return nil
					}))
					_, err = AnalyzeTableDumpBindings(compiler, execution, forged)
					require.Error(t, err, "a correct folded-island hint cannot admit forged row metadata")
					_, err = analyzeTableDumpBindingsWithCandidateVisitLimit(compiler, execution, nil, 128)
					require.ErrorContains(t, err, "binding exceeds limit")
				}
			}
			migrated := proto.Clone(execution)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, proto.Equal(migrated, execution))
			require.Same(t, readingZone, compiler.GetProcess().GetSessionInfo().TimeZone)
			require.Equal(t, "en_US", compiler.GetProcess().GetSessionInfo().LCTimeNames)
			if tc.name == "minute4" || tc.name == "minute30" || tc.name == "upper_minute4" {
				// Measure the complete proof, not just the cheap hint, under
				// the original budget so future candidate growth stays visible.
				proof := newTableDumpProofProcess(compiler)
				defer proof.Free()
				proofCtx := tableDumpBindContext{CompilerContext: compiler, ctx: compiler.GetContext(), proc: proof}
				col := tableDumpDDLColumn(t, source, "g")
				budget := &tableDumpBindingBudget{}
				bound, err := rebindLegacyFloatStringExpression(proofCtx, source, tableDumpExpression{kind: "default", name: col.Name, col: col, expr: col.Default.Expr, origin: col.Default.OriginString}, budget)
				require.NoError(t, err)
				require.NotNil(t, bound)
				t.Logf("complete origin proof visits: %d / %d", budget.visits, tableDumpMaxCandidateVisits)
				require.Less(t, budget.visits, tableDumpMaxCandidateVisits*3/4)
			}
		})
	}
}

func TestLegacyFloatTableSessionWitnesses(t *testing.T) {
	const expression = "concat(a,cast(concat(from_unixtime(0),cast(1 as decimal(10,0))/cast(7 as decimal(10,0))) as char(100)))"
	const full = "-0.0000000000000012345678901234568"
	for _, mixed := range []bool{false, true} {
		name := "duplicate_defaults"
		if mixed {
			name = "distinct_origins_and_creation_settings"
		}
		t.Run(name, func(t *testing.T) {
			compiler, source := legacyFloatScopeTable(t, "create table t(a double,g varchar(200) default("+expression+"),h varchar(200) default("+expression+"))", 4, time.FixedZone("creation", 8*3600+60))
			wantH := full + "1970-01-01 08:01:000.1429"
			if mixed {
				// ALTER can introduce a different origin and session. A previous
				// hint must not replace h's own precision, SQL or frozen values.
				otherExpression := "concat(a,cast(concat(from_unixtime(120),cast(1 as decimal(10,0))/cast(7 as decimal(10,0))) as char(100)))"
				_, other := legacyFloatScopeTable(t, "create table t(a double,h varchar(200) default("+otherExpression+"))", 30, time.FixedZone("other", 8*3600))
				tableDumpDDLColumn(t, source, "h").Default = tableDumpDDLColumn(t, other, "h").Default
				wantH = full + "1970-01-01 08:02:000.142857142857142857142857142857"
			}
			before := proto.Clone(source)
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.Equal(t, full+"1970-01-01 08:01:000.1429", executeLegacyFloatScope(t, compiler, execution, "g"))
			require.Equal(t, wantH, executeLegacyFloatScope(t, compiler, execution, "h"))
			sensitive, err := AnalyzeTableDumpBindings(compiler, execution, nil)
			require.NoError(t, err)
			require.True(t, sensitive)
			_, err = AnalyzeTableDumpBindings(compiler, execution, execution)
			require.NoError(t, err)
			forged := proto.Clone(execution).(*planpb.TableDef)
			require.NoError(t, planpb.VisitExprTree(tableDumpDDLColumn(t, forged, "h").Default.Expr, func(e *planpb.Expr) error {
				if e.GetCol() != nil {
					e.Typ.NotNullable = !e.Typ.NotNullable
				}
				return nil
			}))
			_, err = AnalyzeTableDumpBindings(compiler, execution, forged)
			require.Error(t, err, "the first DEFAULT's witness cannot authorize another payload")
			_, err = analyzeTableDumpBindingsWithCandidateVisitLimit(compiler, execution, nil, 128)
			require.ErrorContains(t, err, "binding exceeds limit")
			migrated := proto.Clone(execution)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, proto.Equal(migrated, execution))
			require.True(t, proto.Equal(before, source))
			require.Same(t, time.UTC, compiler.GetProcess().GetSessionInfo().TimeZone)
			require.Equal(t, "en_US", compiler.GetProcess().GetSessionInfo().LCTimeNames)
			if !mixed {
				forgedSource := proto.Clone(source).(*planpb.TableDef)
				later := tableDumpDDLColumn(t, forgedSource, "h").Default.Expr
				require.NoError(t, planpb.VisitExprTree(later, func(e *planpb.Expr) error {
					if e.GetCol() != nil {
						e.Typ.NotNullable = !e.Typ.NotNullable
					}
					return nil
				}))
				laterBefore := proto.Clone(later)
				// A mismatched catalog may be left alone or hit the work cap;
				// neither outcome may borrow g's proof to rewrite h.
				_ = MigrateLegacyFloatStringTableDef(compiler, forgedSource)
				require.False(t, hasLegacyFloatStringCast(tableDumpDDLColumn(t, forgedSource, "g").Default.Expr))
				require.True(t, proto.Equal(laterBefore, tableDumpDDLColumn(t, forgedSource, "h").Default.Expr))
			}

			budget := &tableDumpBindingBudget{visits: tableDumpMaxCandidateVisits - 1}
			budget.rememberSession(time.FixedZone("known", 8*3600+60), "de_DE")
			col := tableDumpDDLColumn(t, source, "h")
			bound, err := rebindLegacyFloatStringExpression(compiler, source, tableDumpExpression{kind: "default", name: col.Name, col: col, expr: col.Default.Expr, origin: col.Default.OriginString}, budget)
			require.ErrorContains(t, err, "binding exceeds limit")
			require.Nil(t, bound, "hints cannot bypass the shared work cap")
		})
	}
}

func TestLegacyFloatAlteredSessionWitnesses(t *testing.T) {
	const expression = "concat(a,cast(concat(from_unixtime(0),cast(1 as decimal(10,0))/cast(7 as decimal(10,0))) as char(100)))"
	type alteredSessionWitness struct {
		name, suffix, altered string
		offset                int
		precision             int32
	}
	cases := []alteredSessionWitness{
		{"next_minute", "1970-01-01 08:02:00", expression, 8*3600 + 2*60, 4},
		{"changed_precision", "1970-01-01 08:02:00", expression, 8*3600 + 2*60, 30},
		{"earlier_witness", "1969-12-31 10:01:00", expression, -(13*3600 + 59*60), 4},
		{"distant_witness", "1970-01-01 13:59:00", expression, 13*3600 + 59*60, 4},
		{"distinct_source", "1970-01-01 08:04:00", strings.Replace(expression, "from_unixtime(0)", "from_unixtime(120)", 1), 8*3600 + 2*60, 4},
		{"distinct_source_precision8", "1970-01-01 08:04:00", strings.Replace(expression, "from_unixtime(0)", "from_unixtime(120)", 1), 8*3600 + 2*60, 8},
		{"distinct_source_precision", "1970-01-01 08:04:00", strings.Replace(expression, "from_unixtime(0)", "from_unixtime(120)", 1), 8*3600 + 2*60, 30},
		{"distinct_source_earlier", "1969-12-31 10:03:00", strings.Replace(expression, "from_unixtime(0)", "from_unixtime(120)", 1), -(13*3600 + 59*60), 4},
		{"distinct_source_distant", "1970-01-01 14:01:00", strings.Replace(expression, "from_unixtime(0)", "from_unixtime(120)", 1), 13*3600 + 59*60, 4},
	}
	for precision := int32(0); precision <= 30; precision++ {
		if precision == 4 || precision == 8 || precision == 30 {
			continue // Already independently named above.
		}
		cases = append(cases, alteredSessionWitness{
			name: fmt.Sprintf("distinct_source_precision%d", precision), suffix: "1970-01-01 08:04:00",
			altered: strings.Replace(expression, "from_unixtime(0)", "from_unixtime(120)", 1),
			offset:  8*3600 + 2*60, precision: precision,
		})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compiler, source := legacyFloatScopeTable(t, "create table t(a double,g varchar(200) default("+expression+"),h varchar(200) default("+expression+"))", 4, time.FixedZone("creation", 8*3600+60))
			func() {
				proc := compiler.GetProcess()
				reading := compiler.GetContext()
				defer func() {
					compiler.SetContext(reading)
					proc.Ctx = reading
					compiler.ResolveVariableFunc = nil
					proc.GetSessionInfo().TimeZone = time.UTC
				}()
				creation := function.WithDivPrecisionIncrement(function.WithLegacyFloatStringFormatting(function.WithLegacyFloatStringBounds(reading)), tc.precision)
				compiler.SetContext(creation)
				proc.Ctx = creation
				proc.GetSessionInfo().TimeZone = time.FixedZone("alter", tc.offset)
				fallback := *compiler
				compiler.ResolveVariableFunc = func(name string, system, global bool) (any, error) {
					if name == "div_precision_increment" {
						return int64(tc.precision), nil
					}
					return fallback.ResolveVariable(name, system, global)
				}
				stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "alter table t alter column h set default("+tc.altered+")", 1)
				require.NoError(t, err)
				defer stmt.Free()
				altered := CloneTableDefForPlan(source, true)
				_, err = AlterColumn(compiler, &planpb.AlterTable{TableDef: source, CopyTableDef: altered}, stmt.(*tree.AlterTable).Options[0].(*tree.AlterTableAlterColumnClause), initAlterTableContext(source, altered, ""))
				require.NoError(t, err)
				source = altered
			}()
			before := proto.Clone(source)
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			const full = "-0.0000000000000012345678901234568"
			require.Equal(t, full+"1970-01-01 08:01:000.1429", executeLegacyFloatScope(t, compiler, execution, "g"))
			// Exact rational rounding is independent of the SQL decimal/format owners.
			fraction := new(big.Rat).SetFrac64(1, 7).FloatString(int(tc.precision))
			if tc.precision == 4 {
				fraction = "0.1429"
			}
			if tc.precision == 8 {
				fraction = "0.14285714"
			}
			if tc.precision == 30 {
				fraction = "0.142857142857142857142857142857"
			}
			require.Equal(t, full+tc.suffix+fraction, executeLegacyFloatScope(t, compiler, execution, "h"))
			require.True(t, proto.Equal(before, source))

			_, err := AnalyzeTableDumpBindings(compiler, execution, nil)
			require.NoError(t, err)
			_, err = AnalyzeTableDumpBindings(compiler, execution, execution)
			require.NoError(t, err)
			require.Same(t, time.UTC, compiler.GetProcess().GetSessionInfo().TimeZone)
			require.Equal(t, "en_US", compiler.GetProcess().GetSessionInfo().LCTimeNames)
			migrated := proto.Clone(execution)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, proto.Equal(migrated, execution))
			proof := newTableDumpProofProcess(compiler)
			defer proof.Free()
			proofCtx := tableDumpBindContext{CompilerContext: compiler, ctx: compiler.GetContext(), proc: proof}
			budget := &tableDumpBindingBudget{}
			for item := range persistedTableExpressions(source) {
				if !hasLegacyFloatStringCast(item.expr) {
					continue
				}
				bound, err := rebindLegacyFloatStringExpression(proofCtx, source, item, budget)
				require.NoError(t, err)
				require.NotNil(t, bound)
			}
			t.Logf("complete table proof: visits=%d memo entries=%d charged bytes=%d", budget.visits, budget.probeMemo.entries, budget.probeMemo.bytes)
			require.Less(t, budget.visits, tableDumpMaxCandidateVisits)
			require.LessOrEqual(t, budget.probeMemo.entries, legacyFloatProbeMemoEntries)
			require.LessOrEqual(t, budget.probeMemo.bytes, legacyFloatProbeMemoBytes)
		})
	}
}

func TestLegacyFloatProbeMemoIsolation(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	budget := &tableDumpBindingBudget{}
	first := legacyFloatSessionIsland{source: makePlan2StringConstExprWithType("first"), want: makePlan2StringConstExprWithType("first")}
	require.NoError(t, budget.prepareSessionIsland(&first, 0))
	probe := &legacyFloatSessionProbe{islands: []legacyFloatSessionIsland{first}}
	matched, err := probe.matches(compiler, budget)
	require.NoError(t, err)
	require.True(t, matched)
	probe.islands[0].want = makePlan2StringConstExprWithType("different")
	require.NoError(t, budget.prepareSessionIsland(&probe.islands[0], 0))
	matched, err = probe.matches(compiler, budget)
	require.NoError(t, err)
	require.False(t, matched, "a shared evaluation cannot substitute another expected value")
	// Even a colliding fingerprint is only a hint; the full literal still
	// differs and must be rejected after decoding the retained result.
	session := legacyFloatProbeSession{compiler.GetProcess().GetSessionInfo().TimeZone, function.TemporalLocaleNameForProcess(compiler.GetProcess())}
	probe.islands[0].wantValue = first.memo.results[session].value
	matched, err = probe.matches(compiler, budget)
	require.NoError(t, err)
	require.False(t, matched)
	second := legacyFloatSessionIsland{source: makePlan2StringConstExprWithType("different"), want: makePlan2StringConstExprWithType("different")}
	require.NoError(t, budget.prepareSessionIsland(&second, 0))
	probe.islands[0] = second
	matched, err = probe.matches(compiler, budget)
	require.NoError(t, err)
	require.True(t, matched, "distinct residuals must not share evaluations")
	partial := legacyFloatSessionIsland{
		source: &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0, Name: "a"}}},
		want:   first.want,
	}
	require.NoError(t, budget.prepareSessionIsland(&partial, 0))
	entries := budget.probeMemo.entries
	probe.islands[0] = partial
	matched, err = probe.matches(compiler, budget)
	require.NoError(t, err)
	require.False(t, matched)
	require.Equal(t, entries, budget.probeMemo.entries, "partial folds are not cached")

	for _, capKind := range []string{"bytes", "entries"} {
		t.Run(capKind, func(t *testing.T) {
			full := &tableDumpBindingBudget{}
			if capKind == "bytes" {
				full.probeMemo.bytes = legacyFloatProbeMemoBytes
			} else {
				full.probeMemo.entries = legacyFloatProbeMemoEntries
			}
			island := legacyFloatSessionIsland{source: second.source, want: second.want}
			require.NoError(t, full.prepareSessionIsland(&island, 0))
			fallback := &legacyFloatSessionProbe{islands: []legacyFloatSessionIsland{island}}
			matched, err := fallback.matches(compiler, full)
			require.NoError(t, err)
			require.True(t, matched, "full optional storage must retain uncached evaluation")
			require.LessOrEqual(t, full.probeMemo.bytes, legacyFloatProbeMemoBytes)
			require.LessOrEqual(t, full.probeMemo.entries, legacyFloatProbeMemoEntries)
		})
	}
	canceled, cancel := context.WithCancel(compiler.GetContext())
	cancel()
	compiler.SetContext(canceled)
	_, err = probe.matches(compiler, budget)
	require.ErrorIs(t, err, context.Canceled, "cached results must not hide cancellation")
}

func TestLegacyFloatProbeShapePriority(t *testing.T) {
	for _, affinity := range []string{"preferred", "wrong", "all", "none"} {
		t.Run(affinity, func(t *testing.T) {
			compiler := floatCatalogCompiler(t)
			budget := &tableDumpBindingBudget{}
			value := makePlan2StringConstExprWithType("matching")
			wrong := legacyFloatSessionIsland{source: value, want: makePlan2StringConstExprWithType("tampered")}
			right := legacyFloatSessionIsland{source: value, want: value}
			require.NoError(t, budget.prepareSessionIsland(&wrong, 0))
			require.NoError(t, budget.prepareSessionIsland(&right, 0))
			first := &legacyFloatSessionProbe{islands: []legacyFloatSessionIsland{wrong}, searches: []tableDumpSessionCandidate{{increment: 4}}}
			second := &legacyFloatSessionProbe{islands: []legacyFloatSessionIsland{right}, searches: []tableDumpSessionCandidate{{increment: 8}}}
			if affinity == "preferred" {
				// Both islands match; only ordering chooses the second candidate.
				first.islands[0] = right
			}
			first.readingShape = affinity == "wrong" || affinity == "all"
			second.readingShape = affinity == "preferred" || affinity == "all"
			visits := 0
			zones := func(yield func(*time.Location) bool) {
				for i := 0; i < 200; i++ {
					visits++
					if !yield(time.UTC) {
						return
					}
				}
			}
			found, err := matchLegacyFloatSessionProbes(compiler, time.UTC, "en_US", zones, []*legacyFloatSessionProbe{first, second}, budget, 200,
				func(search tableDumpSessionCandidate, _ *time.Location, _ string) (*planpb.Expr, error) {
					require.Equal(t, int64(8), search.increment, "an island/shape hint cannot replace this candidate's full binding")
					return value, nil
				}, func(_ tableDumpSessionCandidate, bound *planpb.Expr) (bool, error) { return bound == value, nil })
			require.NoError(t, err)
			require.True(t, found)
			require.Less(t, budget.visits, 200)
			if affinity == "wrong" {
				require.Greater(t, visits, 1, "a wrong affinity must spend its bounded prefix then fall through")
			} else {
				require.Equal(t, 1, visits)
			}
			if affinity == "all" || affinity == "none" {
				// If no full binding matches, an all-tied preferred pass would
				// duplicate the entire inventory before the fair pass.
				passes := 0
				oneZone := func(yield func(*time.Location) bool) { passes++; yield(time.UTC) }
				found, err = matchLegacyFloatSessionProbes(compiler, time.UTC, "en_US", oneZone, []*legacyFloatSessionProbe{first, second}, &tableDumpBindingBudget{}, 200,
					func(tableDumpSessionCandidate, *time.Location, string) (*planpb.Expr, error) { return value, nil },
					func(tableDumpSessionCandidate, *planpb.Expr) (bool, error) { return false, nil })
				require.NoError(t, err)
				require.False(t, found)
				require.Equal(t, 1, passes, "all/none ties must keep one ordinary fair pass")
			}
		})
	}
}

func TestLegacyFloatLiteralFingerprintZeros(t *testing.T) {
	for _, oid := range []types.T{types.T_float32, types.T_float64} {
		positive := &planpb.Expr{Typ: planpb.Type{Id: int32(oid)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{}}}
		negative := proto.Clone(positive).(*planpb.Expr)
		if oid == types.T_float32 {
			positive.GetLit().Value = &planpb.Literal_Fval{Fval: 0}
			negative.GetLit().Value = &planpb.Literal_Fval{Fval: float32(math.Copysign(0, -1))}
		} else {
			positive.GetLit().Value = &planpb.Literal_Dval{Dval: 0}
			negative.GetLit().Value = &planpb.Literal_Dval{Dval: math.Copysign(0, -1)}
		}
		before := proto.Clone(negative)
		require.True(t, sameLegacyFloatOrigin(positive, negative))
		a, err := legacyFloatLiteralFingerprint(positive)
		require.NoError(t, err)
		b, err := legacyFloatLiteralFingerprint(negative)
		require.NoError(t, err)
		require.Equal(t, a, b, "fast rejection must preserve protobuf equality")
		require.True(t, proto.Equal(before, negative))
		if oid == types.T_float32 {
			require.True(t, math.Signbit(float64(negative.GetLit().GetFval())))
		} else {
			require.True(t, math.Signbit(negative.GetLit().GetDval()))
		}
	}
}

func TestTableDumpSessionHintBounds(t *testing.T) {
	budget := &tableDumpBindingBudget{}
	for i := 0; i <= tableDumpMaxSessionHints; i++ {
		zone := time.FixedZone("hint", i*60)
		budget.rememberSession(zone, "en_US")
		budget.rememberSession(zone, "en_US")
	}
	require.Len(t, budget.sessionHints, tableDumpMaxSessionHints)
	fresh := &tableDumpBindingBudget{}
	found, err := fresh.matchSessionHints(time.UTC, "en_US", func(*time.Location, string) (*planpb.Expr, error) {
		t.Fatal("a separate table must not inherit session hints")
		return nil, nil
	}, func(*planpb.Expr) (bool, error) { return true, nil })
	require.NoError(t, err)
	require.False(t, found)
	_, err = budget.matchSessionHints(time.UTC, "en_US", func(*time.Location, string) (*planpb.Expr, error) {
		return nil, context.Canceled
	}, func(*planpb.Expr) (bool, error) {
		t.Fatal("canceled hint binding cannot authorize a match")
		return false, nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestLegacyFloatSessionSearchFairness(t *testing.T) {
	zone := time.FixedZone("reading", 0)
	witness := time.FixedZone("witness", 3600)
	zones := func(yield func(*time.Location) bool) {
		for i := 0; i < 10; i++ {
			if !yield(witness) {
				return
			}
		}
	}
	sessions := []tableDumpSessionCandidate{
		{increment: 0, dependency: tableDumpDefaultSession{timeZone: true}},
		{increment: 30, dependency: tableDumpDefaultSession{timeZone: true}},
	}
	want := makePlan2StringConstExprWithType("verified witness")
	calls := 0
	found, err := matchLegacyFloatSessions(zone, "en_US", zones, sessions,
		func(search tableDumpSessionCandidate, _ *time.Location, _ string) (*planpb.Expr, error) {
			calls++
			require.LessOrEqual(t, calls, 2, "an earlier candidate must not monopolize the search")
			if search.increment == 30 {
				return want, nil
			}
			return nil, nil
		}, func(_ tableDumpSessionCandidate, bound *planpb.Expr) bool { return bound == want })
	require.NoError(t, err)
	require.True(t, found)
	calls = 0
	_, err = matchLegacyFloatSessions(zone, "en_US", zones, sessions,
		func(tableDumpSessionCandidate, *time.Location, string) (*planpb.Expr, error) {
			calls++
			return nil, context.Canceled
		}, func(tableDumpSessionCandidate, *planpb.Expr) bool {
			t.Fatal("fatal errors cannot authorize a match")
			return false
		})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}

func TestLegacyFloatSessionProbeProofBoundary(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	want := makePlan2StringConstExprWithType("hint")
	probe := &legacyFloatSessionProbe{
		islands:  []legacyFloatSessionIsland{{source: want, want: want}},
		searches: []tableDumpSessionCandidate{{increment: 0}, {increment: 30}},
	}
	calls := 0
	found, err := matchLegacyFloatSessionProbes(compiler, time.UTC, "en_US", newTableDumpTimeZones(time.LoadLocation), []*legacyFloatSessionProbe{probe}, &tableDumpBindingBudget{}, tableDumpMaxCandidateVisits,
		func(search tableDumpSessionCandidate, _ *time.Location, _ string) (*planpb.Expr, error) {
			calls++
			if search.increment == 30 {
				return want, nil
			}
			return nil, nil
		}, func(_ tableDumpSessionCandidate, bound *planpb.Expr) (bool, error) { return bound == want, nil })
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 2, calls, "a hint cannot authorize repair or discard another binder era")
	_, err = matchLegacyFloatSessionProbes(compiler, time.UTC, "en_US", newTableDumpTimeZones(time.LoadLocation), []*legacyFloatSessionProbe{probe}, &tableDumpBindingBudget{}, tableDumpMaxCandidateVisits,
		func(tableDumpSessionCandidate, *time.Location, string) (*planpb.Expr, error) {
			return nil, context.Canceled
		},
		func(tableDumpSessionCandidate, *planpb.Expr) (bool, error) {
			t.Fatal("cancelled binding cannot match")
			return false, nil
		})
	require.ErrorIs(t, err, context.Canceled)
	_, err = matchLegacyFloatSessionProbes(compiler, time.UTC, "en_US", newTableDumpTimeZones(time.LoadLocation), []*legacyFloatSessionProbe{probe}, &tableDumpBindingBudget{}, tableDumpMaxCandidateVisits,
		func(tableDumpSessionCandidate, *time.Location, string) (*planpb.Expr, error) { return want, nil },
		func(tableDumpSessionCandidate, *planpb.Expr) (bool, error) { return false, context.DeadlineExceeded })
	require.ErrorIs(t, err, context.DeadlineExceeded, "fatal full-proof errors must propagate immediately")
	_, err = probe.matches(compiler, &tableDumpBindingBudget{visits: tableDumpMaxCandidateVisits - 1})
	require.ErrorContains(t, err, "binding exceeds limit")
	probe.islands[0].want = makePlan2StringConstExprWithType("tampered")
	matched, err := probe.matches(compiler, &tableDumpBindingBudget{})
	require.NoError(t, err)
	require.False(t, matched)
}

func TestLegacyFloatSessionProjectionMatchesBinding(t *testing.T) {
	const plainSQL = "create table t(a double,g varchar(200) default(concat(a,cast(concat(from_unixtime(0),cast(1 as decimal(10,0))/cast(7 as decimal(10,0))) as char(100)))))"
	zone := time.FixedZone("creation", 8*3600+60)
	for _, precision := range []int32{4, 30} {
		for _, sql := range []string{plainSQL,
			"create table t(a double,g varchar(200) default(concat(a,cast(concat(date_format(from_unixtime(0),'%M %H:%i'),cast(hour(from_unixtime(0)) as double)*cast('1e20' as double)) as char(100)))))",
		} {
			compiler, source := legacyFloatScopeTable(t, sql, precision, zone)
			col := tableDumpDDLColumn(t, source, "g")
			item := tableDumpExpression{kind: "default", name: col.Name, col: col, expr: col.Default.Expr, origin: col.Default.OriginString}
			stmt, ast, _, err := parseTableDumpExpression(compiler.GetContext(), item.origin, "")
			require.NoError(t, err)
			defer stmt.Free()
			budget := &tableDumpBindingBudget{}
			for _, format := range []tableDumpBindingMode{0, tableDumpBindingLegacyFloatFormatting} {
				search := tableDumpSessionCandidate{increment: int64(precision), mode: tableDumpBindingLegacyFloat | format}
				dependency := &tableDumpDefaultSession{captureUnfolded: true}
				folded, err := bindTableDumpExpression(compiler, source, item, ast, search.increment, search.mode, dependency)
				require.NoError(t, err)
				original := proto.Clone(dependency.unfolded)
				probe, err := prepareLegacyFloatSessionProbe(compiler, item.expr, folded, dependency.unfolded, search, budget)
				require.NoError(t, err)
				require.NotNil(t, probe)
				if precision == 4 && sql == plainSQL && format == 0 {
					require.True(t, probe.readingShape)
					unknown := DeepCopyExpr(folded)
					require.NoError(t, planpb.VisitExprTree(unknown, func(e *planpb.Expr) error {
						if literal := e.GetLit(); literal != nil {
							if _, ok := literal.Value.(*planpb.Literal_Sval); ok {
								literal.Isnull = true
							}
						}
						return nil
					}))
					groups := []*legacyFloatSessionProbe{}
					keys := make(map[string]int)
					require.NoError(t, addLegacyFloatSessionProbe(compiler, item.expr, unknown, dependency.unfolded, search, &groups, keys, budget))
					require.Len(t, groups, 1)
					require.False(t, groups[0].readingShape, "unknown reading values provide no priority")
					compiler.GetProcess().GetSessionInfo().TimeZone = zone
					possible, err := groups[0].matches(compiler, budget)
					require.NoError(t, err)
					require.True(t, possible, "unknown reading shape must not reject the true creation witness")
					compiler.GetProcess().GetSessionInfo().TimeZone = time.UTC
					require.NoError(t, addLegacyFloatSessionProbe(compiler, item.expr, folded, dependency.unfolded, search, &groups, keys, budget))
					require.Len(t, groups, 1)
					require.True(t, groups[0].readingShape, "an equivalent positive observation may guide the shared probe")
					require.Len(t, groups[0].searches, 2, "grouping must retain every original binding candidate")
				} else if sql != plainSQL {
					require.False(t, probe.readingShape, "locale/formatting changes can invalidate the reading shape without excluding a witness")
				}
				for _, tz := range []*time.Location{time.UTC, zone} {
					compiler.GetProcess().GetSessionInfo().TimeZone = tz
					for _, lc := range []string{"en_US", "de_DE", "en_US"} {
						compiler.GetProcess().GetSessionInfo().LCTimeNames = lc
						bound, err := bindTableDumpExpression(compiler, source, item, ast, search.increment, search.mode, &tableDumpDefaultSession{})
						require.NoError(t, err)
						possible, err := probe.matches(compiler, budget)
						require.NoError(t, err)
						require.Equal(t, sameLegacyFloatOrigin(item.expr, bound), possible, "sql=%s format=%d zone=%v locale=%s", sql, format, tz, lc)
					}
				}
				compiler.GetProcess().GetSessionInfo().TimeZone = time.UTC
				compiler.GetProcess().GetSessionInfo().LCTimeNames = "en_US"
				require.True(t, proto.Equal(original, dependency.unfolded))
				// An already bound TIMESTAMP literal needs original SQL binding,
				// rather than replaying a reading-session-dependent value.
				timestamp := makePlan2TimestampConstExprWithType(0)
				probe, err = prepareLegacyFloatSessionProbe(compiler, timestamp, timestamp, timestamp, search, &tableDumpBindingBudget{})
				require.NoError(t, err)
				require.Nil(t, probe)
			}
		}
	}
}

func TestLegacyFloatLiveSourceAlterReplay(t *testing.T) {
	const sql = "create table t(a double,g varchar(200) default(concat(a,cast(concat(from_unixtime(0),cast(1 as decimal(10,0))/cast(7 as decimal(10,0))) as char(100)))))"
	compiler, stored := legacyFloatScopeTable(t, sql, 4, time.FixedZone("creation", 8*3600+60))
	before := proto.Clone(stored)
	target := CloneTableDefForPlan(stored, true)
	require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, target))
	require.NoError(t, MigrateLegacyCatalogTableDef(compiler.GetContext(), compiler.GetProcess(), target))
	target.Name = "copy"
	ctx, err := WithAlterTableDDLReplay(compiler.GetContext(), "db", stored, target)
	require.NoError(t, err)
	require.Nil(t, ddlReplayForTable(ctx, "copy").columns["g"].defaultExpr, "raw engine metadata and repaired plan cannot authorize replay")
	live := CloneTableDefForPlan(stored, true)
	require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, live))
	require.NoError(t, MigrateLegacyCatalogTableDef(compiler.GetContext(), compiler.GetProcess(), live))
	ctx, err = WithAlterTableDDLReplay(compiler.GetContext(), "db", live, target)
	require.NoError(t, err)
	require.NotNil(t, ddlReplayForTable(ctx, "copy").columns["g"].defaultExpr)
	compiler.SetContext(ctx)
	replayed := tableDumpDDLCreate(t, compiler, "create table copy(a double,g varchar(200) default(concat(a,cast(concat(from_unixtime(0),cast(1 as decimal(10,0))/cast(7 as decimal(10,0))) as char(100)))))")
	require.Equal(t, "-0.00000000000000123456789012345681970-01-01 08:01:000.1429", executeLegacyFloatScope(t, compiler, replayed))
	require.True(t, proto.Equal(before, stored), "live canonicalization must not mutate the engine catalog")
	// Already canonical metadata stays an exact replay witness too.
	canonical := proto.Clone(live).(*planpb.TableDef)
	require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, live))
	require.NoError(t, MigrateLegacyCatalogTableDef(compiler.GetContext(), compiler.GetProcess(), live))
	require.True(t, proto.Equal(canonical, live))
	ctx, err = WithAlterTableDDLReplay(compiler.GetContext(), "db", live, target)
	require.NoError(t, err)
	require.NotNil(t, ddlReplayForTable(ctx, "copy").columns["g"].defaultExpr)
}

func executeLegacyFloatScope(t *testing.T, compiler *MockCompilerContext, def *planpb.TableDef, names ...string) string {
	t.Helper()
	name := "g"
	if len(names) != 0 {
		name = names[0]
	}
	col := tableDumpDDLColumn(t, def, name)
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
		{"cast30", "double", "cast(concat(from_unixtime(0)," + division + ") as char(100))", "1970-01-01 08:00:00", "", 30},
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
