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
	"encoding/json"
	"os"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestTableDumpBindingModeFlagsAreIndependent(t *testing.T) {
	flags := []tableDumpBindingMode{
		tableDumpBindingPreTemporalNullability,
		tableDumpBindingPreUnixTimestampDatetime,
		tableDumpBindingPreJSON,
		tableDumpBindingPreIncompleteTemporal,
		tableDumpBindingDDLColumnScope,
		tableDumpBindingPreIntegerDecimal,
		tableDumpBindingPreDecimalPrecision,
		tableDumpBindingLegacyFloat,
		tableDumpBindingLegacyFloatFormatting,
		tableDumpBindingFrozenFloatDefaults,
		tableDumpBindingPreJSONFolding,
	}
	var seen tableDumpBindingMode
	for _, flag := range flags {
		require.NotZero(t, flag)
		require.Zero(t, flag&(flag-1), "an era flag must select one policy")
		require.Zero(t, seen&flag, "independent decimal and float eras must not alias")
		require.Zero(t, flag&0x3f, "era flags must not replace a base binding mode")
		seen |= flag
	}
}

// Reuse complete protobufs authored by the actual base planner, not trees
// reconstructed by the historical binding implementation under test.
func TestTableDumpPersistedDecimalBindings(t *testing.T) {
	for _, file := range []string{
		"legacy_decimal_catalog_63a964a82f.json",
		"legacy_decimal_between_63a964a82f.json",
		"legacy_decimal_in_range_63a964a82f.json",
		"legacy_decimal_arithmetic_6c1942f3cf.json",
	} {
		t.Run(file, func(t *testing.T) {
			data, err := os.ReadFile("testdata/" + file)
			require.NoError(t, err)
			var fixtures map[string][]byte
			require.NoError(t, json.Unmarshal(data, &fixtures))
			require.NotEmpty(t, fixtures)
			for name, wire := range fixtures {
				t.Run(name, func(t *testing.T) {
					var source pb.TableDef
					require.NoError(t, source.Unmarshal(wire))
					before := proto.Clone(&source)
					compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
					_, err := AnalyzeTableDumpBindings(compiler, &source, &source)
					require.NoError(t, err, "DUMP must verify the original historical tree")
					target := DeepCopyTableDef(&source, true)
					require.NoError(t, MigrateLegacyCatalogTableDef(t.Context(), compiler.GetProcess(), target))
					_, err = AnalyzeTableDumpBindings(compiler, target, &source)
					require.NoError(t, err, "LOAD must verify an old payload against current execution bindings")
					if name == "signed_compare" {
						for _, width := range []int32{17, 19} {
							forged := DeepCopyTableDef(&source, true)
							forged.Checks[0].Check.GetF().Args[0].Typ.Width = width
							_, err = AnalyzeTableDumpBindings(compiler, target, forged)
							require.ErrorContains(t, err, "cannot verify bound check", "normalizing an arbitrary width must not establish trust")
						}
					}
					if name == "check_product" {
						for _, width := range []int32{31, 37} {
							forged := DeepCopyTableDef(&source, true)
							product := forged.Checks[0].Check.GetF().Args[0]
							require.Equal(t, "*", product.GetF().Func.ObjName)
							product.Typ.Width = width
							_, err = AnalyzeTableDumpBindings(compiler, target, forged)
							require.ErrorContains(t, err, "cannot verify bound check", "a partial precision match must not establish trust")
						}
						_, err = analyzeTableDumpBindingsWithCandidateVisitLimit(compiler, &source, &source, 1)
						require.ErrorContains(t, err, "expression binding exceeds limit")
					}
					require.True(t, proto.Equal(before, &source), "verification must preserve catalog bytes")
				})
			}
		})
	}
}

func TestTableDumpDecimalPrecisionWitnessExcludesIntegerDefaults(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	def := tableDumpDDLCreate(t, compiler, "create table integer_default(a bigint default 7)")
	require.False(t, tableDumpHasLegacyDecimalPrecision(def.Cols[0].Default.Expr))
	// A single literal visits the current and existing temporal-nullability
	// eras once each. No precision or integer-coercion era is added.
	_, err := analyzeTableDumpBindingsWithCandidateVisitLimit(compiler, def, def, 2)
	require.NoError(t, err, "an unaffected default must not spend extra historical-candidate work")
	_, err = analyzeTableDumpBindingsWithCandidateVisitLimit(compiler, def, def, 1)
	require.ErrorContains(t, err, "expression binding exceeds limit")
	require.False(t, tableDumpHasLegacyDecimalPrecision(nil))
	malformed := &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{ObjName: "*"}, Args: []*pb.Expr{nil, nil}}}}
	require.False(t, tableDumpHasLegacyDecimalPrecision(malformed))
	decimal := &pb.Expr{Typ: pb.Type{Id: int32(types.T_decimal64)}}
	product := &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{ObjName: "*"}, Args: []*pb.Expr{decimal, decimal}}}}
	require.True(t, tableDumpHasLegacyDecimalPrecision(&pb.Expr{Expr: &pb.Expr_List{List: &pb.ExprList{List: []*pb.Expr{nil, product}}}}))
	require.True(t, tableDumpHasLegacyDecimalPrecision(&pb.Expr{Expr: &pb.Expr_Lit{Lit: &pb.Literal{Src: product}}}))
}
