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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// Public BuildPlan generated these definitions with the exact CAST/width source
// from e47f45ca6548e198e73ecb242521047bfd449850, independently of migration.
func TestLegacyFloatStringMembership(t *testing.T) {
	data, err := os.ReadFile("testdata/mysql_float_legacy_membership.json")
	require.NoError(t, err)
	var fixtures map[string][]byte
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 9)
	for name, data := range fixtures {
		t.Run(name, func(t *testing.T) {
			compiler := floatCatalogCompiler(t)
			source := new(planpb.TableDef)
			require.NoError(t, proto.Unmarshal(data, source))
			snapshot := proto.Clone(source)
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			col := source.Cols[2]
			stmt, ast, _, err := parseTableDumpExpression(t.Context(), col.GeneratedCol.OriginString, "")
			require.NoError(t, err)
			defer stmt.Free()
			fresh, err := bindFloatTableDumpExpression(compiler, source, tableDumpExpression{kind: "generated", name: "g", col: col}, ast, 0, false)
			require.NoError(t, err)
			proc := compiler.GetProcess()
			bat := batch.NewWithSize(2)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			bat.Vecs[0] = vector.NewVec(types.T_float64.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			for row := 0; row < 3; row++ {
				a := -1.2345678901234567e-15
				c := "-0.0000000000000012345678901234568"
				if row == 1 {
					a = 1.5
					c = "other"
				}
				require.NoError(t, vector.AppendFixed(bat.Vecs[0], a, row == 2, proc.Mp()))
				require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(c), false, proc.Mp()))
			}
			bat.SetRowCount(3)
			eval := func(expr *planpb.Expr) *vector.Vector {
				exec, err := colexec.NewExpressionExecutor(proc, expr)
				require.NoError(t, err)
				t.Cleanup(exec.Free)
				v, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
				require.NoError(t, err)
				return v
			}
			got, want := eval(execution.Cols[2].GeneratedCol.Expr), eval(fresh)
			gotValues, wantValues := vector.MustFixedColNoTypeCheck[bool](got), vector.MustFixedColNoTypeCheck[bool](want)
			expected := !strings.HasPrefix(name, "not_in") && name != "in_explicit"
			if name == "nullable_explicit" {
				require.True(t, got.GetNulls().Contains(0), "NULL alternative and explicit narrow miss remain UNKNOWN")
			} else {
				require.False(t, got.GetNulls().Contains(0))
				require.Equal(t, expected, gotValues[0], "long float membership must use the complete string")
			}
			for row := 0; row < 3; row++ {
				require.Equal(t, want.GetNulls().Contains(uint64(row)), got.GetNulls().Contains(uint64(row)), "row %d nullness", row)
				if !want.GetNulls().Contains(uint64(row)) {
					require.Equal(t, wantValues[row], gotValues[row], "row %d", row)
				}
			}
			require.True(t, sameTableDumpBoundExpression(fresh, execution.Cols[2].GeneratedCol.Expr))
			require.True(t, proto.Equal(snapshot, source))
			migrated := proto.Clone(execution)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, proto.Equal(migrated, execution))
		})
	}
}

func unaffectedFloatDefaults(tb testing.TB) (*MockCompilerContext, *planpb.TableDef) {
	tb.Helper()
	compiler := floatCatalogCompiler(tb)
	cols := make([]string, 128)
	for i := range cols {
		cols[i] = fmt.Sprintf("c%d int default null", i)
	}
	stmt, err := parsers.ParseOne(tb.Context(), dialect.MYSQL, "create table t("+strings.Join(cols, ",")+")", 1)
	require.NoError(tb, err)
	defer stmt.Free()
	built, err := BuildPlan(compiler, stmt, false)
	require.NoError(tb, err)
	def := built.GetDdl().GetCreateTable().GetTableDef()
	defaults := 0
	for _, col := range def.Cols {
		if col.Default != nil && col.Default.Expr != nil {
			defaults++
			require.False(tb, hasLegacyFloatStringCast(col.Default.Expr))
		}
	}
	require.Equal(tb, 128, defaults)
	return compiler, def
}

func TestLegacyFloatStringUnaffectedDefaults(t *testing.T) {
	compiler, source := unaffectedFloatDefaults(t)
	snapshot := proto.Clone(source)
	require.Zero(t, testing.AllocsPerRun(10, func() { require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, source)) }), "unaffected defaults must not materialize candidate slices")
	require.True(t, proto.Equal(snapshot, source))
}

var floatStringBenchmarkTable *planpb.TableDef

func BenchmarkLegacyFloatStringUnaffectedDefaults(b *testing.B) {
	compiler, source := unaffectedFloatDefaults(b)
	for _, migrate := range []bool{false, true} {
		name := "clone"
		if migrate {
			name = "clone_and_migrate"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cloned := CloneTableDefForPlan(source, true)
				if migrate {
					if err := MigrateLegacyFloatStringTableDef(compiler, cloned); err != nil {
						b.Fatal(err)
					}
				}
				floatStringBenchmarkTable = cloned
				if len(cloned.Cols) != len(source.Cols) {
					b.Fatal("lost columns")
				}
			}
		})
	}
}
