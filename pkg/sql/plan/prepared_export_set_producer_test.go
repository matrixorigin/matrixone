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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedExportSetProducerDomains(t *testing.T) {
	queries := []string{
		`select export_set((select ? as x union all select 1 order by x desc limit 1), 'Y','N','',4)`,
		`select export_set((select 1 as x union all select ? order by x desc limit 1), 'Y','N','',4)`,
		`select export_set((select ? as x union select 1 order by x desc limit 1), 'Y','N','',4)`,
		`select export_set((select max(?) from nation), 'Y','N','',4)`,
		`select export_set((select max(x) from (select max(?) as x from nation) d), 'Y','N','',4)`,
		`select export_set((select max(y) from (select max(x) as y from (select max(?) as x from nation) d) e), 'Y','N','',4)`,
		`select export_set((select first_value(x) over () from (select max(?) as x from nation) d limit 1), 'Y','N','',4)`,
		`select export_set((select max(?) as x from nation union all select 2 order by x desc limit 1), 'Y','N','',4)`,
		`select export_set((select 2 as x union select max(?) from nation order by x desc limit 1), 'Y','N','',4)`,
		`select export_set((select max(?) as x from nation union all select min(?) from nation order by x desc limit 1), 'Y','N','',4)`,
		`select export_set((select max(y) from (select first_value(x) over () as y from (select max(?) as x from nation) d) e), 'Y','N','',4)`,
		`select export_set((select min(?) from nation group by n_regionkey limit 1), 'Y','N','',4)`,
		`select export_set((select x from (select ? as x from nation group by x) d limit 1), 'Y','N','',4)`,
		`select export_set((select x from (select ? as x union all select 1) a union all select 3 order by x desc limit 1), 'Y','N','',4)`,
		`select export_set((select ? as x union all select ? order by x desc limit 1), 'Y','N','',4)`,
		`select export_set((select ? as x intersect select 1 limit 1), 'Y','N','',4)`,
		`select export_set((select ? as x minus select 1 limit 1), 'Y','N','',4)`,
	}
	nested := `select max(?) as x from nation`
	for depth := 0; depth < 12; depth++ {
		nested = `select max(x) as x from (` + nested + `) d`
	}
	queries = append(queries, `select export_set((`+nested+`), 'Y','N','',4)`)
	for _, sql := range queries {
		t.Run(sql, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, `prepare s from "`+sql+`"`)
			require.NoError(t, err)
			cached := prepared.GetDcl().GetPrepare().Plan
			snapshot := cached.String()
			require.True(t, PreparedPlanNeedsRuntimeSpecialization(cached), "binary execution must select specialization")
			for _, binary := range []bool{false, true} {
				for _, typ := range []types.Type{types.New(types.T_decimal64, 4, 1), types.T_float64.ToType(), types.New(types.T_decimal256, 65, 1), types.New(types.T_decimal64, 4, 1)} {
					param := ParamValue{Value: "2.5", SourceType: typ, HasSourceType: !binary, RuntimeType: typ, HasRuntimeType: binary, IsBinaryProtocol: binary}
					params := make([]any, strings.Count(sql, "?"))
					for i := range params {
						params[i] = param
					}
					filled, err := FillValuesOfParamsInPlan(context.Background(), cached, params)
					require.NoError(t, err)
					export := findPlanFunctionExpr(filled, "export_set")
					require.NotNil(t, export)
					if typ.Oid.IsDecimal() {
						require.True(t, types.T(export.GetF().Args[0].Typ.Id).IsDecimal(), export.String())
					} else {
						require.Equal(t, int32(typ.Oid), export.GetF().Args[0].Typ.Id, export.String())
					}
					for _, node := range filled.GetQuery().Nodes {
						if node.NodeType == planpb.Node_UNION || node.NodeType == planpb.Node_UNION_ALL ||
							node.NodeType == planpb.Node_INTERSECT || node.NodeType == planpb.Node_MINUS {
							for col, output := range node.ProjectList {
								for _, child := range node.Children {
									input := filled.GetQuery().Nodes[child].ProjectList[col]
									require.Equal(t, output.Typ.Id, input.Typ.Id, "set branch OID")
									require.Equal(t, output.Typ.Scale, input.Typ.Scale, "set branch scale")
									require.Equal(t, output.Typ.Width, input.Typ.Width, "set branch width")
								}
							}
						}
						if node.NodeType == planpb.Node_WINDOW {
							for _, win := range node.WinSpecList {
								require.Equal(t, export.GetF().Args[0].Typ.Id, win.Typ.Id)
								require.Equal(t, win.Typ.Id, win.GetW().WindowFunc.Typ.Id, "window vector ABI")
							}
						}
						if node.NodeType == planpb.Node_AGG && len(node.AggList) > 0 && (strings.Contains(sql, "max(?)") || strings.Contains(sql, "min(?)")) {
							require.Equal(t, export.GetF().Args[0].Typ.Id, node.AggList[0].Typ.Id, "consumer must use aggregate result")
						}
					}
					require.Equal(t, snapshot, cached.String(), "cached plan must remain immutable")
				}
			}
		})
	}
}
