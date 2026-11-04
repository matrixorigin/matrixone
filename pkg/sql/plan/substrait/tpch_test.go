// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package substrait

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	planbuilder "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestExportExtractSemanticBoundary(t *testing.T) {
	for _, tc := range []struct {
		expression string
		eligible   bool
	}{
		{"extract(year from l_shipdate)", true},
		{"extract(month from l_shipdate)", true},
		{"extract(day from l_shipdate)", true},
		{"extract(quarter from l_shipdate)", false},
		{"extract(week from l_shipdate)", false}, // MO mode 0, backend ISO
		{"extract(year_month from l_shipdate)", false},
		{"extract(year from l_comment)", false}, // tolerant text vs no overload
		{"extract(week from l_comment)", false},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			mock := planbuilder.NewMockOptimizer(false, newPlanTestProcess(t))
			statements, err := parsers.Parse(t.Context(), dialect.MYSQL,
				"select "+tc.expression+" from lineitem", 1)
			require.NoError(t, err)
			defer statements[0].Free()
			query, err := mock.Optimize(statements[0])
			require.NoError(t, err)
			for _, node := range query.Nodes {
				if node != nil && node.TableDef != nil && node.ObjRef != nil {
					node.TableDef.DbId, node.TableDef.TblId, node.ObjRef.Obj = 7, 42, 42
				}
			}
			candidate, err := ExportEmbeddedMO(query, NewEmbeddedExportProfile(0))
			if tc.eligible {
				require.NoError(t, err)
				require.NotNil(t, candidate)
			} else {
				require.Nil(t, candidate)
				require.True(t, IsNotEligible(err), "%v", err)
				reason, ok := NotEligibleReason(err)
				require.True(t, ok)
				require.Equal(t, EligibilityExpression, reason)
			}
			embedded, embeddedErr := ExportEmbeddedMO(query, NewEmbeddedExportProfile(31))
			if tc.eligible && tc.expression != "extract(quarter from l_shipdate)" {
				require.NoError(t, embeddedErr)
				require.NotNil(t, embedded)
			} else {
				require.Nil(t, embedded)
				require.True(t, IsNotEligible(embeddedErr), "%v", embeddedErr)
			}
		})
	}
}

func TestEmbeddedDATEExtractGuardsZeroDate(t *testing.T) {
	for _, unit := range []string{"year", "month", "day"} {
		for _, required := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", unit, required), func(t *testing.T) {
				args := []*planpb.Expr{
					{Typ: planpb.Type{Id: int32(types.T_varchar), NotNullable: true}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: unit}}}},
					{Typ: planpb.Type{Id: int32(types.T_date), NotNullable: required}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
				}
				bound, err := function.GetFunctionByName(t.Context(), "extract", []types.Type{types.T_varchar.ToType(), types.T_date.ToType()})
				require.NoError(t, err)
				result := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64), NotNullable: function.DeduceNotNullable(bound.GetEncodedOverloadID(), args)}}
				call := &planpb.Function{Func: &planpb.ObjectRef{ObjName: "extract", Obj: bound.GetEncodedOverloadID()}, Args: args}
				e := exporter{profile: NewEmbeddedExportProfile(31)}
				wire, err := e.extractExpr(result, call, []int{1})
				require.NoError(t, err)
				guard := wire.GetIfThen()
				require.NotNil(t, guard)
				require.Len(t, guard.Ifs, 1)
				equality := guard.Ifs[0].If.GetScalarFunction()
				require.Equal(t, e.functions["equal"], equality.FunctionReference)
				require.Equal(t, types.ZeroDate.DaysSinceUnixEpoch(), equality.Arguments[1].GetValue().GetLiteral().GetDate())
				require.Equal(t, int64(0), guard.Ifs[0].Then.GetLiteral().GetI64())
				require.NotNil(t, guard.Else.GetCast().Type.GetI64())
				component := guard.Else.GetCast().Input.GetCast()
				require.NotNil(t, component.Type.GetI32())
				require.Equal(t, e.functions[unit], component.Input.GetScalarFunction().FunctionReference)

			})
		}
	}
}
