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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"os"
	"strings"
	"testing"
	"time"

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
			built, err := BuildPlan(compiler, stmt, false)
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
	built, err := BuildPlan(compiler, stmt, false)
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
