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
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

// BuildPlan produced these fixtures with func_cast.go and string_result_domain.go
// from 6d27391f7f6705ea08f5fee571336d546568edbb overlaid on the PR dependencies,
// independently of the migration's historical binder.
func TestLegacyFloatStringRefinements(t *testing.T) {
	data, err := os.ReadFile("testdata/mysql_float_legacy_refinements.json")
	require.NoError(t, err)
	require.Equal(t, "a4494abee549868267eaea5335e191921f1e7ffb5fb2c0aa5f1053566d92fbcc", fmt.Sprintf("%x", sha256.Sum256(data)))
	var fixtures map[string][]byte
	require.NoError(t, json.Unmarshal(data, &fixtures))
	const full = "-0.0000000000000012345678901234568"
	for _, tc := range []struct{ name, want string }{
		{"right", "8901234568"},
		{"repeat", full + full},
		{"left", "-0.0000000"},
		{"substring", "8901234568"},
		{"alias", "8901234568"},
		{"lpad", "xxxxxx" + full},
		{"rpad", full + "xxxxxx"},
		{"nested", "8901234568"},
		{"cast_length", "8901234568"},
		{"dynamic", "8901234568"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler := floatCatalogCompiler(t)
			require.Contains(t, fixtures, tc.name)
			source := new(planpb.TableDef)
			require.NoError(t, proto.Unmarshal(fixtures[tc.name], source))
			snapshot := proto.Clone(source)
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			gpos, inputs := 1, 1
			if tc.name == "dynamic" {
				gpos, inputs = 2, 2
			}
			builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, true)
			expr, err := builder.applyGeneratedColumnAssignmentCast(DeepCopyExpr(execution.Cols[gpos].GeneratedCol.Expr), false)
			require.NoError(t, err)
			proc := compiler.GetProcess()
			exec, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			t.Cleanup(exec.Free)
			bat := batch.NewWithSize(inputs)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			bat.Vecs[0] = vector.NewVec(types.T_float64.ToType())
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], -1.2345678901234567e-15, false, proc.Mp()))
			if inputs == 2 {
				bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
				require.NoError(t, vector.AppendFixed(bat.Vecs[1], int32(10), false, proc.Mp()))
			}
			bat.SetRowCount(1)
			result, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, result.GetStringAt(0))
			col := source.Cols[gpos]
			stmt, ast, _, err := parseTableDumpExpression(t.Context(), col.GeneratedCol.OriginString, "")
			require.NoError(t, err)
			defer stmt.Free()
			fresh, err := bindTableDumpExpression(compiler, source, tableDumpExpression{kind: "generated", name: "g", col: col}, ast, 0, false)
			require.NoError(t, err)
			require.True(t, sameTableDumpBoundExpression(fresh, execution.Cols[gpos].GeneratedCol.Expr), "migrated metadata must match ordinary binding")
			require.True(t, proto.Equal(snapshot, source))
			migrated := proto.Clone(execution)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.True(t, proto.Equal(migrated, execution))
		})
	}
}
