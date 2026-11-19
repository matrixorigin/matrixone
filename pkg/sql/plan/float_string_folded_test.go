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
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Public BuildPlan fixtures produced with exact e47f45ca predecessor CAST,
// string-bound and ByteJson rendering sources overlaid on current dependencies.
// Neither compatibility binder nor its proof-time formatter creates these bytes.
// current_era_mixed uses only the predecessor bound with current scalar/JSON
// formatting, covering catalogs authored before this PR fixed the width.
func TestLegacyFloatStringFoldedDefaults(t *testing.T) {
	data, err := os.ReadFile("testdata/mysql_float_legacy_folded.json")
	require.NoError(t, err)
	fixtures := map[string][]byte{}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	const full = "-0.0000000000000012345678901234568"
	for _, tc := range []struct {
		name, suffix string
		mixed        bool
	}{
		{"mixed_exponent", "1E+20", true}, {"mixed_small", "1E-14", true},
		{"mixed_float", "1E+20", true}, {"mixed_explicit", "1E+20", true},
		{"mixed_nested", "", true}, {"mixed_json", "[1e20]", true},
		{"folded_concat", "1E+20", false}, {"folded_explicit", "1E+2", false},
		{"folded_small", "-1E-14", false}, {"folded_float", "1E+20", false},
		{"folded_json", "[100000000000000000000]", false}, {"literal_control", "1E+20", false},
		{"mixed_outer_explicit", "1E+20", true},
		{"mixed_json_text", "[100000000000000000000]", true},
		{"folded_pretty", "{\n  \"k\": [\n    100000000000000000000\n  ]\n}", false},
		{"folded_value", "100000000000000000000", false},
		{"folded_row", "[[100000000000000000000]]", false},
		{"folded_key", `{"[100000000000000000000]": "1"}`, false},
		{"current_era_mixed", full[:32], true},
		{"folded_pretty_text", "[\n  100000000000000000000\n]", false},
		{"folded_value_text", "100000000000000000000", false},
		{"text_unquote_control", "[1e20]", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler := floatCatalogCompiler(t)
			source := new(planpb.TableDef)
			require.NoError(t, proto.Unmarshal(fixtures[tc.name], source))
			snapshot := proto.Clone(source)
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			if !tc.mixed || tc.name == "mixed_explicit" {
				require.True(t, proto.Equal(source.Cols[1].Default.Expr, execution.Cols[1].Default.Expr), "folded values and explicit limits must remain unchanged")
			}
			expr, err := getDefaultExprForAssignment(compiler.GetContext(), execution.Cols[1], execution.DbName, compiler.GetProcess(), false)
			require.NoError(t, err)
			proc := compiler.GetProcess()
			exec, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			t.Cleanup(exec.Free)
			bat := batch.NewWithSize(1)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			bat.Vecs[0] = vector.NewVec(types.T_float64.ToType())
			for i, v := range []float64{-1.2345678901234567e-15, 1.5, 0} {
				require.NoError(t, vector.AppendFixed(bat.Vecs[0], v, i == 2, proc.Mp()))
			}
			bat.SetRowCount(3)
			result, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
			require.NoError(t, err)
			for row, prefix := range []string{full, "1.5", ""} {
				if tc.mixed && row == 2 {
					require.True(t, result.GetNulls().Contains(uint64(row)))
					continue
				}
				want := tc.suffix
				if tc.mixed {
					if tc.name == "mixed_explicit" && row == 0 {
						prefix = full[:32]
					}
					want = prefix + tc.suffix
					if tc.name == "mixed_nested" {
						want = prefix + "1E+20"
						if len(want) > 15 {
							want = want[len(want)-15:]
						}
					}
				}
				require.Equal(t, want, result.GetStringAt(row), "row %d", row)
			}
			require.True(t, proto.Equal(snapshot, source))
			migrated := proto.Clone(execution)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, proto.Equal(migrated, execution))
		})
	}
}

func TestTableDumpLegacyFoldedDefaults(t *testing.T) {
	data, err := os.ReadFile("testdata/mysql_float_legacy_folded.json")
	require.NoError(t, err)
	fixtures := map[string][]byte{}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	for name, data := range fixtures {
		t.Run(name, func(t *testing.T) {
			compiler := floatCatalogCompiler(t)
			source := new(planpb.TableDef)
			require.NoError(t, proto.Unmarshal(data, source))
			snapshot := proto.Clone(source)
			_, err := AnalyzeTableDumpBindings(compiler, source, nil)
			require.NoError(t, err, "DUMP must admit exact historical folds")
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			_, err = AnalyzeTableDumpBindings(compiler, source, execution)
			require.NoError(t, err, "LOAD must admit migrated mixed-era defaults")
			require.True(t, proto.Equal(snapshot, source))
			tampered := proto.Clone(source).(*planpb.TableDef)
			// Contradict the complete SQL origin, rather than accepting arbitrary old literals.
			tampered.Cols[1].Default.OriginString = "('unrelated')"
			_, err = AnalyzeTableDumpBindings(compiler, tampered, nil)
			require.ErrorContains(t, err, "cannot verify bound default expression")
			if strings.HasPrefix(name, "mixed_") && name != "mixed_json" {
				tampered = proto.Clone(source).(*planpb.TableDef)
				edited := false
				require.NoError(t, planpb.VisitExprTree(tampered.Cols[1].Default.Expr, func(e *planpb.Expr) error {
					if lit := e.GetLit(); lit != nil && !lit.Isnull && lit.GetSval() != "" && !edited {
						lit.Value = &planpb.Literal_Sval{Sval: "forged"}
						edited = true
					}
					return nil
				}))
				require.True(t, edited)
				before := proto.Clone(tampered)
				require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, tampered))
				require.True(t, proto.Equal(before, tampered), "forged folded sibling cannot authorize a repair")
			}
		})
	}
}

func TestFrozenFloatDefaultProofControls(t *testing.T) {
	data, err := os.ReadFile("testdata/mysql_float_legacy_folded.json")
	require.NoError(t, err)
	fixtures := map[string][]byte{}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	source := new(planpb.TableDef)
	require.NoError(t, proto.Unmarshal(fixtures["mixed_exponent"], source))
	compiler := floatCatalogCompiler(t)
	col := source.Cols[1]
	item := tableDumpExpression{kind: "default", name: col.Name, col: col, expr: col.Default.Expr, origin: col.Default.OriginString}
	budget := &tableDumpBindingBudget{}
	repaired, err := rebindLegacyFloatStringExpression(compiler, source, item, budget)
	require.NoError(t, err)
	require.NotNil(t, repaired)
	// Exhaust work in the late witness stage, rather than just the parser gate.
	tight := &tableDumpBindingBudget{visits: tableDumpMaxCandidateVisits - budget.visits + 1}
	_, err = rebindLegacyFloatStringExpression(compiler, source, item, tight)
	require.ErrorContains(t, err, "binding exceeds limit")
	parent := compiler.GetProcess()
	before := parent.Ctx
	sink := &process.WarningProbe{}
	parent.WarningSink = sink
	proof := floatStringProofProcess(function.WithLegacyFloatStringFormatting(before), parent)
	require.True(t, function.LegacyFloatStringFormatting(proof.Ctx))
	proof.WarningSink.(*process.WarningProbe).AppendWarningCount(1)
	require.False(t, sink.Warned())
	require.Equal(t, before, parent.Ctx)
	ordinary := floatStringProofProcess(before, parent)
	require.False(t, function.LegacyFloatStringFormatting(ordinary.Ctx))
	ordinary.WarningSink.(*process.WarningProbe).AppendWarningCount(1)
	require.False(t, sink.Warned())
	canceled, cancel := context.WithCancel(before)
	cancel()
	_, _, err = freezeVerifiedFloatDefault(canceled, col.Default.Expr, col.Default.Expr, col.Default.Expr, proof, &tableDumpBindingBudget{})
	require.ErrorIs(t, err, context.Canceled)
}

// BuildPlan's historical formatting/bounds owners create the stored trees. The
// migration oracle below is the exact execution result, not a second rebind.
func TestLegacyFloatDefaultSessionWitnesses(t *testing.T) {
	const full = "-0.0000000000000012345678901234568"
	for _, tc := range []struct {
		name, suffix, want string
		explicit           bool
	}{
		{"timezone", "from_unixtime(0)", "1970-01-01 08:00:00", false},
		{"locale", "monthname(cast('2020-01-01' as date))", "Januar", false},
		{"combined", "date_format(from_unixtime(0), '%M %H')", "Januar 08", false},
		{"explicit", "from_unixtime(0)", "1970-01-01 08:00:00", true},
		{"json_cast", "cast(cast('1e20' as json) as char)", "100000000000000000000", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler := floatCatalogCompiler(t)
			proc := compiler.GetProcess()
			proc.GetSessionInfo().TimeZone = time.FixedZone("creation", 8*3600)
			proc.GetSessionInfo().LCTimeNames = "de_DE"
			creation := function.WithLegacyFloatStringBounds(function.WithLegacyFloatStringFormatting(context.WithValue(compiler.GetContext(), historicalFloatDefaultFoldingKey{}, true)))
			compiler.SetContext(creation)
			proc.Ctx = creation
			value := "a"
			if tc.explicit {
				value = "cast(a as char(32))"
			}
			stmt, err := parsers.ParseOne(creation, dialect.MYSQL, "create table t(a double, g varchar(100) default (concat("+value+","+tc.suffix+")))", 1)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(compiler, stmt, false, nil)
			require.NoError(t, err)
			source := built.GetDdl().GetCreateTable().TableDef
			require.Equal(t, !tc.explicit, hasLegacyFloatStringCast(source.Cols[1].Default.Expr))
			snapshot := proto.Clone(source)
			reading := context.Background()
			compiler.SetContext(reading)
			proc.Ctx = reading
			proc.GetSessionInfo().TimeZone = time.UTC
			// A resolver overrides the snapshot; witness search must isolate both.
			proc.GetSessionInfo().LCTimeNames = "ja_JP"
			proc.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
				if name == "lc_time_names" {
					return "en_US", nil
				}
				return nil, nil
			})
			warnings := &process.WarningProbe{}
			proc.WarningSink = warnings
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			expr, err := getDefaultExprForAssignment(reading, execution.Cols[1], execution.DbName, proc, false)
			require.NoError(t, err)
			exec, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer exec.Free()
			bat := batch.NewWithSize(1)
			defer bat.Clean(proc.Mp())
			bat.Vecs[0] = vector.NewVec(types.T_float64.ToType())
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], -1.2345678901234567e-15, false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], 0, true, proc.Mp()))
			bat.SetRowCount(2)
			result, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
			require.NoError(t, err)
			prefix := full
			if tc.explicit {
				prefix = full[:32]
			}
			require.Equal(t, prefix+tc.want, result.GetStringAt(0))
			require.True(t, result.GetNulls().Contains(1))
			require.True(t, proto.Equal(snapshot, source))
			migrated := proto.Clone(execution)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, proto.Equal(migrated, execution))
			require.Same(t, time.UTC, proc.GetSessionInfo().TimeZone)
			require.Equal(t, "ja_JP", proc.GetSessionInfo().LCTimeNames)
			require.Equal(t, reading, proc.Ctx)
			require.False(t, warnings.Warned())
			_, err = AnalyzeTableDumpBindings(compiler, source, execution)
			require.NoError(t, err)
			if tc.name == "timezone" {
				col := source.Cols[1]
				item := tableDumpExpression{kind: "default", name: col.Name, col: col, expr: col.Default.Expr, origin: col.Default.OriginString}
				stmt, _, visit, err := parseTableDumpExpression(reading, item.origin, tableDumpParserModesForOrigin(item.origin)[0])
				require.NoError(t, err)
				stmt.Free()
				// Two cheap era probes fit; the first alternate session must hit
				// the same total candidate budget instead of starting a new one.
				_, err = rebindLegacyFloatStringExpression(compiler, source, item, &tableDumpBindingBudget{visits: tableDumpMaxCandidateVisits - 2*visit.nodes})
				require.ErrorContains(t, err, "binding exceeds limit")
				require.Same(t, time.UTC, proc.GetSessionInfo().TimeZone)
				require.Equal(t, "ja_JP", proc.GetSessionInfo().LCTimeNames)
			}
			if tc.name == "locale" {
				forged := proto.Clone(source).(*planpb.TableDef)
				require.True(t, tamperTableDumpDefaultValue(forged.Cols[1].Default.Expr))
				before := proto.Clone(forged)
				require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, forged))
				require.True(t, proto.Equal(before, forged), "unreachable stored constants cannot authorize widening")
			}
		})
	}
}

func TestHistoricalFloatJSONFoldingAdmission(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	stmt, err := parsers.ParseOne(compiler.GetContext(), dialect.MYSQL,
		"create table t(g varchar(100) default(json_unquote(json_array(cast('1e20' as double)))))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(compiler, stmt, false, nil)
	require.NoError(t, err)
	def := built.GetDdl().GetCreateTable().TableDef
	expr := def.Cols[0].Default.Expr
	require.NotNil(t, expr.GetF(), "ordinary binding preserves JSON admission identity")
	proc := compiler.GetProcess()
	before := proc.Ctx
	proof := floatStringProofProcess(context.WithValue(before, historicalFloatDefaultFoldingKey{}, true), proc)
	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(expr), proof, false, true)
	require.NoError(t, err)
	require.NotNil(t, folded.GetLit(), "the historical era can reconstruct old folded literals")
	require.Equal(t, before, proc.Ctx)
	// New JSON consumer overloads retain lazy/admission semantics even inside
	// historical proof. A historical candidate cannot erase a new overload ID.
	jsonArg := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_json)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Jsonval{Jsonval: "[]"}}}}
	consumer := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "elt", Obj: function.EncodeOverloadID(function.ELT, 1)},
		Args: []*planpb.Expr{makePlan2Int64ConstExprWithType(1), jsonArg},
	}}}
	unchanged, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(consumer), proof, false, true)
	require.NoError(t, err)
	require.True(t, proto.Equal(consumer, unchanged))
}

// cancelAfterErrChecks cancels its parent on the Nth Err() check so a test can
// land cancellation inside a deterministic late path without any production
// hook. remaining <= 0 disables firing (calibration pass counts calls only).
type cancelAfterErrChecks struct {
	context.Context
	calls     int64
	remaining int64
	cancel    context.CancelFunc
}

func (c *cancelAfterErrChecks) Err() error {
	c.calls++
	if c.remaining > 0 {
		c.remaining--
		if c.remaining == 0 {
			c.cancel()
		}
	}
	return c.Context.Err()
}

// The proof bind inside finishLegacyFloatStringExpression must be charged to
// the same candidate budget as every earlier candidate: a budget that still
// fits exactly the pre-proof work is exhausted by the new proof work and must
// reject with the shared limit instead of silently skipping the proof.
// Cancellation observed at the end of the real proof/current-bind path must
// surface as context.Canceled and publish nothing.
func TestLegacyFloatStringProofBindGuards(t *testing.T) {
	data, err := os.ReadFile("testdata/mysql_float_legacy_folded.json")
	require.NoError(t, err)
	fixtures := map[string][]byte{}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	newSource := func() *planpb.TableDef {
		source := new(planpb.TableDef)
		require.NoError(t, proto.Unmarshal(fixtures["mixed_exponent"], source))
		return source
	}
	itemOf := func(source *planpb.TableDef) tableDumpExpression {
		col := source.Cols[1]
		return tableDumpExpression{kind: "default", name: col.Name, col: col, expr: col.Default.Expr, origin: col.Default.OriginString}
	}
	t.Run("budget_charged", func(t *testing.T) {
		source := newSource()
		snapshot := proto.Clone(source)
		compiler := floatCatalogCompiler(t)
		fresh := &tableDumpBindingBudget{}
		repaired, err := rebindLegacyFloatStringExpression(compiler, source, itemOf(source), fresh)
		require.NoError(t, err)
		require.NotNil(t, repaired)
		total := fresh.visits
		stmt, _, visit, err := parseTableDumpExpression(t.Context(), itemOf(source).origin, "")
		require.NoError(t, err)
		defer stmt.Free()
		require.Positive(t, visit.nodes)
		require.Greater(t, total, visit.nodes)
		// Headroom exactly total-nodes fits the pre-proof work (the proof
		// charge is visit.nodes and the proof bind adds more), so the new
		// proof work must be what pushes this run over the shared limit.
		tight := &tableDumpBindingBudget{visits: tableDumpMaxCandidateVisits - total + visit.nodes}
		rejected := newSource()
		_, err = rebindLegacyFloatStringExpression(compiler, rejected, itemOf(rejected), tight)
		require.ErrorContains(t, err, "binding exceeds limit")
		require.True(t, proto.Equal(snapshot, rejected), "rejected proof must not publish")
	})
	t.Run("cancel_at_proof_path", func(t *testing.T) {
		calibrator := floatCatalogCompiler(t)
		probeCtx, probeCancel := context.WithCancel(calibrator.GetContext())
		defer probeCancel()
		probe := &cancelAfterErrChecks{Context: probeCtx, cancel: probeCancel}
		calibrator.SetContext(probe)
		calibSource := newSource()
		calibRepaired, err := rebindLegacyFloatStringExpression(calibrator, calibSource, itemOf(calibSource), &tableDumpBindingBudget{})
		require.NoError(t, err)
		require.NotNil(t, calibRepaired)
		require.Positive(t, probe.calls)
		// Fire on the last Err() check of the same deterministic path: the
		// candidate verification has already completed, so the cancellation
		// lands inside the real proof/current-bind window.
		interrupted := floatCatalogCompiler(t)
		intCtx, intCancel := context.WithCancel(interrupted.GetContext())
		defer intCancel()
		interrupted.SetContext(&cancelAfterErrChecks{Context: intCtx, cancel: intCancel, remaining: probe.calls})
		intSource := newSource()
		intSnapshot := proto.Clone(intSource)
		_, err = rebindLegacyFloatStringExpression(interrupted, intSource, itemOf(intSource), &tableDumpBindingBudget{})
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, proto.Equal(intSnapshot, intSource), "canceled proof must not publish")
	})
}

// TestExplicitCastValidLiteralNullability75 pins the corrected
// explicitBinaryToTextCastMaySynthesizeNull contract (function.go): an
// explicit CAST into non-binary-charset char/varchar/text synthesizes NULL
// with Warning 1300 only when the runtime row bytes carry binary-domain
// evidence AND fail utf8.Valid (func_cast.go binarySourceToText). A non-NULL
// literal with exactly known valid UTF-8 bytes therefore can never become
// NULL, while invalid-UTF8 literals, arbitrary text columns and runtime
// string semantic state sources stay conservative. Before the correction the
// positive cases below inferred nullable (CTAS authored DEFAULT NULL), which
// is the dtype/varchar_ddl_width.sql and sysview DEFAULT_ENCRYPTION
// regression shape.
func TestExplicitCastValidLiteralNullability75(t *testing.T) {
	opt := NewMockOptimizer(false, newPlanTestProcess(t))

	// Schema consumer (CTAS): the literal cast column must be authored NOT NULL.
	ctasPlan, err := runOneStmt(opt, t, "create table explicit_cast_literal_ctas75 as select cast('old' as varchar(65535)) as v")
	require.NoError(t, err)
	ddl := ctasPlan.GetDdl()
	require.NotNil(t, ddl)
	ct := ddl.GetCreateTable()
	require.NotNil(t, ct)
	var vcol *planpb.ColDef
	for _, c := range ct.TableDef.Cols {
		if c.Name == "v" {
			vcol = c
		}
	}
	require.NotNil(t, vcol, "CTAS column v must exist in the actual schema")
	require.NotNil(t, vcol.Default)
	require.False(t, vcol.Default.NullAbility)

	// Projection consumer: valid-UTF8 non-NULL literal casts project NOT NULL.
	// Covers the sysview literal shapes CAST('NO' AS varchar(3)) and
	// CAST('def' AS varchar(64)).
	for _, sql := range []string{
		"select cast('old' as varchar(65535))",
		"select cast('NO' as varchar(3))",
		"select cast('def' as varchar(64))",
		// Binary-domain literal with exactly known valid UTF-8 bytes (hex
		// literal kept raw under an explicit text cast per
		// base_binder.binaryLiteralUnderExplicitTextCast): NOT NULL.
		"select cast(x'6F6C64' as char)",
	} {
		p, err := runOneStmt(opt, t, sql)
		require.NoError(t, err)
		q := p.GetQuery()
		require.NotNil(t, q)
		root := q.Nodes[q.Steps[0]]
		require.NotEmpty(t, root.ProjectList, sql)
		require.True(t, root.ProjectList[0].Typ.NotNullable, sql)
	}

	// Conservative controls: these must keep nullable inference.
	// 1. coalesce over an arbitrary text column: the runtime bytes come from a
	//    column whose provenance the planner cannot audit.
	// 2. unhex produces runtime bytes that may be invalid UTF-8.
	// 3. a string function over a column keeps the runtime defense even when
	//    its bytes would be valid: the planner cannot prove provenance. (The
	//    nested cast(cast('old' as binary) ...) shape is deliberately NOT
	//    used: constant folding may reduce it to a valid-bytes literal, so it
	//    cannot serve as a certain non-literal source.)
	// 4. a NULL literal stays NULL.
	// 5. a literal with invalid UTF-8 bytes (hex literal kept raw under an
	//    explicit text cast) can hit the executor NULL guard.
	for _, sql := range []string{
		"select cast(coalesce(n_name, '') as varchar(64)) from nation",
		"select cast(unhex('FF') as char)",
		"select cast(concat(n_name, n_name) as char) from nation",
		"select cast(null as varchar(64))",
		"select cast(x'FF' as char)",
	} {
		p, err := runOneStmt(opt, t, sql)
		require.NoError(t, err)
		q := p.GetQuery()
		require.NotNil(t, q)
		root := q.Nodes[q.Steps[0]]
		require.NotEmpty(t, root.ProjectList, sql)
		require.False(t, root.ProjectList[0].Typ.NotNullable, sql)
	}
}

// TestCreateViewCastLiteralNullability87 closes the schema-consumer gap left
// by TestExplicitCastValidLiteralNullability75: the CREATE VIEW producer
// (build_ddl.go buildCreateView -> genViewTableDef -> buildCTASDefaultForView,
// which authors ColDef.Default.NullAbility from the SELECT expression type)
// must persist NullAbility=false for the sysview literal-cast shapes that
// SHOW COLUMNS / DESC / information_schema consumers read back:
// CAST('NO' AS varchar(3)) (SCHEMATA.DEFAULT_ENCRYPTION), CAST('def' AS
// varchar(64)) (KEY_COLUMN_USAGE.CONSTRAINT_CATALOG/TABLE_CATALOG in BOTH
// UNION ALL branches) and CAST('def' AS varchar(512))
// (TABLE_PRIVILEGES.TABLE_CATALOG). A runtime-unknown text source must stay
// conservative. This pins the CURRENT plan producer only; view column
// metadata persisted into an existing data dir by older binaries is a
// separate identity and is not recreated by this test.
func TestCreateViewCastLiteralNullability87(t *testing.T) {
	buildView := func(t *testing.T, sql string) *planpb.TableDef {
		t.Helper()
		ctx := &rootSQLCompilerContext{
			MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)),
			rootSQL:             sql,
		}
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		p, err := BuildPlan(ctx, stmt, false, nil)
		require.NoError(t, err)
		cv := p.GetDdl().GetCreateView()
		require.NotNil(t, cv, sql)
		return cv.GetTableDef()
	}
	colNullability := func(t *testing.T, def *planpb.TableDef, name string) bool {
		t.Helper()
		for _, c := range def.GetCols() {
			if c.Name == name {
				require.NotNil(t, c.Default, name)
				return c.Default.NullAbility
			}
		}
		require.FailNow(t, "column missing from authored view schema", name)
		return true
	}

	// Single-projection literal casts: the SCHEMATA.DEFAULT_ENCRYPTION,
	// KEY_COLUMN_USAGE constant catalog and TABLE_PRIVILEGES.TABLE_CATALOG
	// shapes must all be authored NOT NULL.
	t.Run("literal_cast_columns_not_null", func(t *testing.T) {
		def := buildView(t, "create view cast_literal_view87 as select "+
			"cast('NO' as varchar(3)) as default_encryption, "+
			"cast('def' as varchar(64)) as constraint_catalog, "+
			"cast('def' as varchar(512)) as table_catalog")
		require.False(t, colNullability(t, def, "default_encryption"))
		require.False(t, colNullability(t, def, "constraint_catalog"))
		require.False(t, colNullability(t, def, "table_catalog"))
	})

	// UNION ALL two-branch constant casts (the KEY_COLUMN_USAGE shape): both
	// branches are known non-NULL literals, so the merged view columns must
	// stay NOT NULL.
	t.Run("union_all_literal_cast_not_null", func(t *testing.T) {
		def := buildView(t, "create view cast_literal_union_view87 as "+
			"select cast('def' as varchar(64)) as constraint_catalog, cast('def' as varchar(64)) as table_catalog "+
			"union all "+
			"select cast('def' as varchar(64)), cast('def' as varchar(64))")
		require.False(t, colNullability(t, def, "constraint_catalog"))
		require.False(t, colNullability(t, def, "table_catalog"))
	})

	// Conservative control: a runtime-unknown text source keeps nullable
	// inference in the authored view schema.
	t.Run("runtime_unknown_source_stays_nullable", func(t *testing.T) {
		def := buildView(t, "create view cast_literal_unknown_view87 as "+
			"select cast(coalesce(n_name, '') as varchar(64)) as v from nation")
		require.True(t, colNullability(t, def, "v"))
	})
}
