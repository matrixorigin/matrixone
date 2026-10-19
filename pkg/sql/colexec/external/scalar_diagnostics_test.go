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

package external

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestFilePruningPreservesScalarDiagnostics(t *testing.T) {
	for _, diagnostic := range []bool{false, true} {
		t.Run(map[bool]string{false: "pure_filepath", true: "format_locale"}[diagnostic], func(t *testing.T) {
			proc := testutil.NewProcess(t)
			probe := &process.WarningProbe{}
			proc.WarningSink = probe
			file := "/data/file.csv"
			left := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: catalog.ExternalFilePath}}}
			right := plan2.MakePlan2StringConstExprWithType(file)
			if diagnostic {
				var err error
				left, err = plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "format", []*plan.Expr{
					plan2.MakePlan2Int64ConstExprWithType(1), plan2.MakePlan2Int64ConstExprWithType(2), left,
				})
				require.NoError(t, err)
				right = plan2.MakePlan2StringConstExprWithType("1.00")
			}
			predicate, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*plan.Expr{left, right})
			require.NoError(t, err)
			table := makeTableDef(catalog.ExternalFilePath)
			table.Cols[0].ColId = catalog.ExternalFilePathColId
			node := &plan.Node{TableDef: table, FilterList: []*plan.Expr{predicate}, ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_EXTERNAL_TB)}}
			rule.NewConstantFold(false).Apply(node, nil, proc)
			predicate = node.FilterList[0]
			files, sizes, rows, err := FilterFileList(proc.Ctx, node, proc, []string{file}, []int64{32})
			require.NoError(t, err)
			require.Equal(t, []string{file}, files)
			require.Equal(t, []int64{32}, sizes)
			require.False(t, probe.Warned(), "file admission must not publish row diagnostics")
			if diagnostic {
				require.Equal(t, []*plan.Expr{predicate}, rows, "every admitted row still owns its warning")
			} else {
				require.Empty(t, rows, "pure filepath predicates still prune at file level")
			}
			require.Equal(t, []*plan.Expr{predicate}, node.FilterList, "the reusable plan must remain intact")
		})
	}
}
