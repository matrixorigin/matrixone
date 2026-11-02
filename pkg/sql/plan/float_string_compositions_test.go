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
	"strings"
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

// BuildPlan generated these bytes using official main 838cc78aa1ef's CAST and
// string-bound implementation, independently of the historical repair binder.
func TestLegacyFloatStringCompositions(t *testing.T) {
	data, err := os.ReadFile("testdata/mysql_float_legacy_compositions.json")
	require.NoError(t, err)
	var fixtures map[string][]byte
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 7)
	const full = "-0.0000000000000012345678901234568"
	for name, data := range fixtures {
		t.Run(name, func(t *testing.T) {
			compiler := floatCatalogCompiler(t)
			source := new(planpb.TableDef)
			require.NoError(t, proto.Unmarshal(data, source))
			snapshot := proto.Clone(source)
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			col := source.Cols[4]
			stmt, ast, _, err := parseTableDumpExpression(t.Context(), col.GeneratedCol.OriginString, "")
			require.NoError(t, err)
			defer stmt.Free()
			fresh, err := bindFloatTableDumpExpression(compiler, source, tableDumpExpression{kind: "generated", name: "g", col: col}, ast, 0, false)
			require.NoError(t, err)
			require.True(t, sameTableDumpBoundExpression(fresh, execution.Cols[4].GeneratedCol.Expr), "repair must reproduce catalog binding, including persisted overloads")
			checkFunctions := func(expr *planpb.Expr) {
				require.NoError(t, planpb.VisitExprTree(expr, func(e *planpb.Expr) error {
					if f := e.GetF(); f != nil && f.Func != nil {
						id, overload := function.DecodeOverloadID(f.Func.Obj)
						if id == function.FORMAT {
							require.Equal(t, int32(len(f.Args)-2), overload)
						}
						if f.Func.ObjName == "find_in_set" {
							require.Len(t, f.Args, 3)
						}
					}
					return nil
				}))
			}
			checkFunctions(execution.Cols[4].GeneratedCol.Expr)
			proc := compiler.GetProcess()
			bat := batch.NewWithSize(4)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			bat.Vecs[0] = vector.NewVec(types.T_float64.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
			bat.Vecs[2] = vector.NewVec(makeTypeByPlan2Type(source.Cols[2].Typ))
			bat.Vecs[3] = vector.NewVec(types.T_date.ToType())
			for row := 0; row < 3; row++ {
				require.NoError(t, vector.AppendFixed(bat.Vecs[0], -1.2345678901234567e-15, row == 2, proc.Mp()))
				require.NoError(t, vector.AppendFixed(bat.Vecs[1], int32(1), row == 1, proc.Mp()))
				require.NoError(t, vector.AppendFixed(bat.Vecs[2], uint64(3), false, proc.Mp()))
				require.NoError(t, vector.AppendFixed(bat.Vecs[3], types.DateFromCalendar(2010, 2, 3), false, proc.Mp()))
			}
			bat.SetRowCount(3)
			builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, true)
			eval := func(expr *planpb.Expr) *vector.Vector {
				expr, err = builder.applyGeneratedColumnAssignmentCast(DeepCopyExpr(expr), false)
				require.NoError(t, err)
				exec, err := colexec.NewExpressionExecutor(proc, expr)
				require.NoError(t, err)
				t.Cleanup(exec.Free)
				v, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
				require.NoError(t, err)
				return v
			}
			got, want := eval(execution.Cols[4].GeneratedCol.Expr), eval(fresh)
			for row := 0; row < 3; row++ {
				require.Equal(t, want.GetNulls().Contains(uint64(row)), got.GetNulls().Contains(uint64(row)))
				if !want.GetNulls().Contains(uint64(row)) {
					require.Equal(t, want.GetStringAt(row), got.GetStringAt(row))
				}
			}
			require.True(t, strings.HasPrefix(got.GetStringAt(0), full), "complete DOUBLE text must survive sibling reconstruction")
			if name == "format_sibling" {
				require.Equal(t, full+"1.00", got.GetStringAt(0))
			}
			if name == "find_set_sibling" {
				require.Equal(t, full+"2", got.GetStringAt(0))
			}
			require.True(t, proto.Equal(snapshot, source))
			migrated := proto.Clone(execution)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, proto.Equal(migrated, execution))
		})
	}
}
