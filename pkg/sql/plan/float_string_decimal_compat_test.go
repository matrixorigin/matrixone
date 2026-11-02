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
	"encoding/json"
	"os"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// These complete TableDefs were authored and self-verified by the unmodified
// planner at 861a2c4cb68961d8b6b01f18e0aa6ac52a3a505f, before either correction.
func TestLegacyFloatAndDecimalPrecisionCatalogComposition(t *testing.T) {
	data, err := os.ReadFile("testdata/legacy_float_decimal_861a2c.json")
	require.NoError(t, err)
	var fixtures map[string][]byte
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 3)
	for name, wire := range fixtures {
		t.Run(name, func(t *testing.T) {
			compiler := floatCatalogCompiler(t)
			var source planpb.TableDef
			require.NoError(t, source.Unmarshal(wire))
			before := proto.Clone(&source)
			expression := tableDumpExpressions(&source)[0].expr
			require.Equal(t, name != "folded_default", hasLegacyFloatStringCast(expression))
			_, err := AnalyzeTableDumpBindings(compiler, &source, &source)
			require.NoError(t, err, "DUMP must verify both historical policies together")
			owned := CloneTableDefForPlan(&source, true)
			migrate := func() {
				require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, owned))
				require.NoError(t, MigrateLegacyCatalogTableDef(compiler.GetContext(), compiler.GetProcess(), owned))
			}
			migrate()
			require.False(t, hasLegacyFloatStringCast(tableDumpExpressions(owned)[0].expr), "mixed decimal precision must not silently skip float repair")
			if name == "folded_default" {
				require.True(t, proto.Equal(before, owned), "stored folded values must remain unchanged")
			} else {
				proc := compiler.GetProcess()
				executor, err := colexec.NewExpressionExecutor(proc, tableDumpExpressions(owned)[0].expr)
				require.NoError(t, err)
				t.Cleanup(executor.Free)
				input := batch.NewWithSize(2)
				t.Cleanup(func() { input.Clean(proc.Mp()) })
				input.Vecs[0] = vector.NewVec(types.New(types.T_decimal64, 15, 2))
				input.Vecs[1] = vector.NewVec(types.T_float64.ToType())
				require.NoError(t, vector.AppendFixed(input.Vecs[0], types.Decimal64(125), false, proc.Mp()))
				require.NoError(t, vector.AppendFixed(input.Vecs[1], -1.2345678901234567e-15, false, proc.Mp()))
				input.SetRowCount(1)
				result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				require.Equal(t, "1.5625-0.0000000000000012345678901234568", result.GetStringAt(0))
			}
			require.True(t, proto.Equal(before, &source), "migration must preserve shared catalog bytes")
			once := proto.Clone(owned)
			migrate()
			require.True(t, proto.Equal(once, owned), "migration must be idempotent")
			_, err = AnalyzeTableDumpBindings(compiler, owned, &source)
			require.NoError(t, err, "LOAD must verify the historical payload against migrated bindings")
			_, err = AnalyzeTableDumpBindings(compiler, owned, owned)
			require.NoError(t, err, "migrated catalog must remain independently verifiable")
			for _, target := range []*planpb.TableDef{&source, owned} {
				if name == "folded_default" {
					continue
				}
				forged := DeepCopyTableDef(target, true)
				changed := false
				require.NoError(t, planpb.VisitExprTree(tableDumpExpressions(forged)[0].expr, func(e *planpb.Expr) error {
					if fn := e.GetF(); fn != nil && fn.Func.GetObjName() == "*" {
						e.Typ.Width = 37
						changed = true
					}
					return nil
				}))
				require.True(t, changed)
				_, err = AnalyzeTableDumpBindings(compiler, target, forged)
				require.Error(t, err, "float repair must not authorize forged decimal precision")
			}
		})
	}
}

func TestLegacyFloatAndIntegerDecimalCatalogComposition(t *testing.T) {
	for _, declaration := range []string{
		"generated always as (concat(a,b > cast('00:00:01' as time))) stored",
		"default(concat(a,b > cast('00:00:01' as time)))",
	} {
		t.Run(declaration, func(t *testing.T) {
			compiler := floatCatalogCompiler(t)
			legacy := function.WithLegacyIntegerDecimalCoercions(function.WithLegacyFloatStringFormatting(function.WithLegacyFloatStringBounds(compiler.GetContext())))
			compiler.SetContext(legacy)
			compiler.GetProcess().Ctx = legacy
			// Bind the whole historical tree through the original owners. The
			// catalog then adds NOT NULL, as it does for a real CREATE.
			source := tableDumpDDLCatalog(tableDumpDDLCreate(t, compiler,
				"create table t(a double not null,b bigint unsigned,g varchar(200) "+declaration+")"))
			compiler.SetContext(context.Background())
			proc := compiler.GetProcess()
			proc.Ctx = compiler.GetContext()
			before := proto.Clone(source)
			var oldDecimal bool
			for _, item := range tableDumpExpressions(source) {
				require.NoError(t, planpb.VisitExprTree(item.expr, func(e *planpb.Expr) error {
					oldDecimal = oldDecimal || insufficientCatalogIntegerCast(e)
					return nil
				}))
			}
			require.True(t, oldDecimal, "fixture must retain the historical insufficient integer coercion")
			_, err := AnalyzeTableDumpBindings(compiler, source, nil)
			require.NoError(t, err, "DUMP must compose both historical eras")
			owned := CloneTableDefForPlan(source, true)
			migrate := func() {
				require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, owned))
				require.NoError(t, MigrateLegacyCatalogTableDef(compiler.GetContext(), proc, owned))
			}
			migrate()
			require.True(t, proto.Equal(before, source))
			once := proto.Clone(owned)
			migrate()
			require.True(t, proto.Equal(once, owned))
			_, err = AnalyzeTableDumpBindings(compiler, owned, nil)
			require.NoError(t, err)
			require.True(t, proto.Equal(once, owned), "DUMP only normalizes independently rebound candidates")
			col := tableDumpDDLColumn(t, owned, "g")
			expr := col.Default.GetExpr()
			if col.GeneratedCol != nil {
				expr = col.GeneratedCol.Expr
			}
			exec, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer exec.Free()
			input := batch.NewWithSize(2)
			defer input.Clean(proc.Mp())
			input.Vecs[0] = vector.NewVec(types.T_float64.ToType())
			input.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
			require.NoError(t, vector.AppendFixed(input.Vecs[0], -1.2345678901234567e-15, false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(input.Vecs[1], uint64(1000000000000000000), false, proc.Mp()))
			input.SetRowCount(1)
			result, err := exec.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err, "integer comparison must not retain DECIMAL(18,0)")
			require.Equal(t, "-0.00000000000000123456789012345681", result.GetStringAt(0))
			for _, target := range []*planpb.TableDef{source, owned} {
				forged := proto.Clone(target).(*planpb.TableDef)
				var changed bool
				require.NoError(t, planpb.VisitExprTree(tableDumpExpressions(forged)[0].expr, func(e *planpb.Expr) error {
					if e.GetCol() != nil && e.GetCol().ColPos == 1 {
						e.Typ.NotNullable = true
						changed = true
					}
					return nil
				}))
				require.True(t, changed)
				_, err = AnalyzeTableDumpBindings(compiler, target, forged)
				require.Error(t, err, "mixed column scopes cannot authorize DUMP payloads")
			}
		})
	}
}
