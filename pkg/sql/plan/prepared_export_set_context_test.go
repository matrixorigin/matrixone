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

func TestPreparedExportSetExpressionContexts(t *testing.T) {
	const export = `export_set((select max(?) from nation),'Y','N','',4)`
	queries := map[string]string{
		"where":              `select n_nationkey from nation where ` + export + `='NYNN' order by n_nationkey`,
		"having":             `select count(*) from nation having ` + export + `='NYNN'`,
		"group":              `select ` + export + ` as bits, count(*) from nation group by bits`,
		"order":              `select n_nationkey from nation order by ` + export + `, n_nationkey`,
		"aggregate_argument": `select max(` + export + `) from nation`,
		"window_partition":   `select row_number() over (partition by export_set(x,'Y','N','',4) order by x) from (select max(?) as x from nation) d`,
		"window_argument":    `select first_value(export_set(x,'Y','N','',4)) over () from (select max(?) as x from nation) d`,
		"join":               `select n.n_nationkey from nation n join (select max(?) as x from nation) d on export_set(x,'Y','N','',4)='NYNN' order by n.n_nationkey`,
	}
	for name, value := range map[string]string{
		"coalesce":                  `coalesce(x,1.0)`,
		"nullif":                    `nullif(x,1.0)`,
		"coalesce_nullif":           `coalesce(nullif(x,0),1.0)`,
		"nullif_coalesce":           `nullif(coalesce(x,1.0),0)`,
		"coalesce_text":             `coalesce(x,'1.0')`,
		"coalesce_explicit_char":    `coalesce(x,cast(1.0 as char))`,
		"coalesce_explicit_double":  `coalesce(x,cast(1.0 as double))`,
		"coalesce_scientific":       `coalesce(x,1e0)`,
		"coalesce_explicit_decimal": `coalesce(x,cast(1.0 as decimal(6,2)))`,
		"nullif_explicit_char":      `nullif(cast(coalesce(x,1.0) as char),'0')`,
	} {
		queries[name] = `select export_set((select ` + value + ` from (select max(?) as x from nation) d),'Y','N','',4)`
	}
	for name, sql := range queries {
		t.Run(name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, `prepare s from "`+sql+`"`)
			require.NoError(t, err)
			cached := prepared.GetDcl().GetPrepare().Plan
			before := cached.String()
			if name == "nullif_coalesce" {
				filled, err := FillValuesOfParamsInPlan(context.Background(), cached, []any{nil})
				require.NoError(t, err)
				comparison := findPlanFunctionExpr(filled, "=")
				require.NotNil(t, comparison)
				require.Equal(t, int32(types.T_float64), comparison.GetF().Args[0].Typ.Id, "NULL marker must not reinstate a strict integer comparison")
				require.Equal(t, before, cached.String())
			}
			require.True(t, PreparedPlanNeedsRuntimeSpecialization(cached))
			for _, binary := range []bool{false, true} {
				for _, typ := range []types.Type{types.T_float64.ToType(), types.New(types.T_decimal64, 4, 1), types.New(types.T_decimal256, 65, 1), types.T_float64.ToType()} {
					params := make([]any, strings.Count(sql, "?"))
					for i := range params {
						params[i] = ParamValue{Value: "2.5", SourceType: typ, HasSourceType: !binary, RuntimeType: typ, HasRuntimeType: binary, IsBinaryProtocol: binary}
					}
					filled, err := FillValuesOfParamsInPlan(context.Background(), cached, params)
					require.NoError(t, err)
					found := 0
					seen := make(map[int32]bool)
					var inspect func(int32)
					inspect = func(nodeID int32) {
						if seen[nodeID] {
							return
						}
						seen[nodeID] = true
						node := filled.GetQuery().Nodes[nodeID]
						for _, child := range node.Children {
							inspect(child)
						}
						require.NoError(t, planpb.VisitExpressionsInOwner(node, func(root *planpb.Expr) error {
							return planpb.VisitExprTree(root, func(expr *planpb.Expr) error {
								if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "export_set" {
									found++
									if name == "coalesce_text" || name == "coalesce_explicit_char" || name == "nullif_explicit_char" {
										require.Equal(t, int32(types.T_int64), fn.Args[0].Typ.Id, expr.String())
									} else if name == "coalesce_explicit_double" || name == "coalesce_scientific" {
										require.Equal(t, int32(types.T_float64), fn.Args[0].Typ.Id, expr.String())
									} else if typ.Oid.IsDecimal() {
										require.True(t, types.T(fn.Args[0].Typ.Id).IsDecimal(), expr.String())
									} else {
										require.Equal(t, int32(typ.Oid), fn.Args[0].Typ.Id, expr.String())
									}
								}
								return nil
							})
						}))
					}
					for _, step := range filled.GetQuery().Steps {
						inspect(step)
					}
					require.Positive(t, found, "prepared %s\nfilled %s", before, filled.String())
					require.Equal(t, before, cached.String())
				}
			}
		})
	}
}
