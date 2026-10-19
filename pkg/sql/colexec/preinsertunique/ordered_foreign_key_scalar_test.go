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

package preinsertunique

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestOrderedFKTupleScalarTypes(t *testing.T) {
	for _, oid := range []types.T{types.T_int64, types.T_uuid, types.T_year, types.T_decimal256} {
		for _, constant := range []bool{false, true} {
			t.Run(oid.String()+map[bool]string{false: "/flat", true: "/const"}[constant], func(t *testing.T) {
				proc := testutil.NewProc(t)
				t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
				typ := oid.ToType()
				if oid == types.T_decimal256 {
					typ = types.New(oid, 65, 0)
				}
				parent := batch.NewWithSize(1)
				parent.Vecs[0] = vector.NewVec(typ)
				t.Cleanup(func() { parent.Clean(proc.Mp()) })
				v := parent.Vecs[0]
				switch oid {
				case types.T_int64:
					for _, x := range []int64{11, 12, 11} {
						require.NoError(t, vector.AppendFixed(v, x, false, proc.Mp()))
					}
				case types.T_uuid:
					for _, s := range []string{"00000000-0000-0000-0000-000000000011", "00000000-0000-0000-0000-000000000012", "00000000-0000-0000-0000-000000000011"} {
						x, err := types.ParseUuid(s)
						require.NoError(t, err)
						require.NoError(t, vector.AppendFixed(v, x, false, proc.Mp()))
					}
				case types.T_year:
					for _, x := range []types.MoYear{2020, 2021, 2020} {
						require.NoError(t, vector.AppendFixed(v, x, false, proc.Mp()))
					}
				case types.T_decimal256:
					for _, s := range []string{"10000000000000000000000000000000000000000000000000000000000000011", "10000000000000000000000000000000000000000000000000000000000000012", "10000000000000000000000000000000000000000000000000000000000000011"} {
						x, err := types.ParseDecimal256(s, 65, 0)
						require.NoError(t, err)
						require.NoError(t, vector.AppendFixed(v, x, false, proc.Mp()))
					}
				}
				parent.SetRowCount(3)
				tuple := vector.NewVec(typ)
				defer tuple.Free(proc.Mp())
				require.NoError(t, tuple.UnionOne(v, 0, proc.Mp()))
				if constant {
					tuple.ToConst()
					tuple.SetLength(3)
				}
				expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*plan.Expr{
					{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "id"}}},
					{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}}},
				})
				require.NoError(t, err)
				bound, err := bindOrderedFKTuple(proc, expr, []*vector.Vector{tuple})
				require.NoError(t, err, "runtime values must not require a protobuf Literal carrier")
				require.NotNil(t, expr.GetF().Args[1].GetCol(), "binding must not mutate prepared metadata")
				if constant {
					selected, err := orderedFKValueExpr(proc, tuple, 2)
					require.NoError(t, err)
					require.Equal(t, bound.GetF().Args[1], selected, "constant input uses physical row zero")
				}
				tuple.CleanOnlyData() // The bound value must not borrow reusable input storage.
				executor, err := colexec.NewExpressionExecutor(proc, bound)
				require.NoError(t, err)
				defer executor.Free()
				result, err := executor.Eval(proc, []*batch.Batch{parent}, nil)
				require.NoError(t, err)
				require.Equal(t, []bool{true, false, true}, vector.MustFixedColNoTypeCheck[bool](result))
				filter, executors, err := prepareOrderedFKFilter(proc, bound)
				require.NoError(t, err)
				defer freeOrderedFKExecutors(executors)
				require.NotNil(t, filter)

				selected, err := orderedFKValueExpr(proc, v, 1)
				require.NoError(t, err)
				require.Equal(t, typ.PlanType(), selected.Typ)
				if payload := selected.GetVec(); payload != nil {
					decoded := vector.NewVec(types.T_any.ToType())
					require.NoError(t, decoded.UnmarshalBinary(payload.Data))
					require.True(t, decoded.IsConst())
					require.Equal(t, 1, decoded.Length())
					require.Equal(t, typ, *decoded.GetType())
					decoded.Free(proc.Mp())
				}
				selectedPredicate := plan2.DeepCopyExpr(expr)
				selectedPredicate.GetF().Args[1] = selected
				selectedExecutor, err := colexec.NewExpressionExecutor(proc, selectedPredicate)
				require.NoError(t, err)
				defer selectedExecutor.Free()
				selectedResult, err := selectedExecutor.Eval(proc, []*batch.Batch{parent}, nil)
				require.NoError(t, err)
				require.Equal(t, []bool{false, true, false}, vector.MustFixedColNoTypeCheck[bool](selectedResult))

				null := vector.NewConstNull(typ, 3, proc.Mp())
				defer null.Free(proc.Mp())
				nullValue, err := orderedFKValueExpr(proc, null, 2)
				require.NoError(t, err)
				require.True(t, nullValue.GetLit().GetIsnull())
			})
		}
	}
}
