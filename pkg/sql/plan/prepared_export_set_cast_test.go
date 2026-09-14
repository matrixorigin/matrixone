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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestPreparedExportSetSetBranchExplicitCast(t *testing.T) {
	for _, tc := range []struct {
		sql     string
		numeric bool
	}{
		{`select cast(max(?) as char) as x from nation union all select 2`, false},
		{`select max(?) as x from nation union all select cast(2 as char)`, false},
		{`select cast(2 as char) as x union select max(?) from nation`, false},
		{`select cast(max(?) as decimal(4,1)) as x from nation union all select 2`, true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, `prepare s from "select export_set((`+tc.sql+` order by x desc limit 1), 'Y','N','',4)"`)
			require.NoError(t, err)
			cached := prepared.GetDcl().GetPrepare().Plan
			before := cached.String()
			for _, binary := range []bool{false, true} {
				for _, typ := range []types.Type{types.T_float64.ToType(), types.New(types.T_decimal64, 4, 1), types.T_float64.ToType()} {
					filled, err := FillValuesOfParamsInPlan(context.Background(), cached, []any{ParamValue{Value: "10.5", SourceType: typ, HasSourceType: !binary, RuntimeType: typ, HasRuntimeType: binary, IsBinaryProtocol: binary}})
					require.NoError(t, err)
					export := findPlanFunctionExpr(filled, "export_set")
					require.NotNil(t, export)
					if tc.numeric {
						require.Equal(t, int32(types.T_decimal64), export.GetF().Args[0].Typ.Id)
					} else {
						require.Equal(t, int32(types.T_int64), export.GetF().Args[0].Typ.Id)
					}
					found := false
					for _, node := range filled.GetQuery().Nodes {
						if !preparedSetOperation(node) {
							continue
						}
						found = true
						output := node.ProjectList[0].Typ
						if tc.numeric {
							require.True(t, types.T(output.Id).IsDecimal())
						} else {
							require.True(t, types.T(output.Id).IsMySQLString(), output.String())
						}
						for _, child := range node.Children {
							input := filled.GetQuery().Nodes[child].ProjectList[0].Typ
							require.True(t, samePreparedSetOperationType(output, input), "output %s input %s", output.String(), input.String())
						}
					}
					require.True(t, found)
					require.Equal(t, before, cached.String())
				}
			}
		})
	}
}
