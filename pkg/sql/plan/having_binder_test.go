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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestRemapAggToTimeWindowResultAggMarksPartialSum(t *testing.T) {
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_uint64.ToType()} {
		t.Run(typ.Oid.String(), func(t *testing.T) {
			expr := &plan.Expr{
				Typ: plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale},
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{
							Obj:     function.AggSumOverloadID,
							ObjName: "sum",
						},
						Args: []*plan.Expr{{
							Typ: plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale},
						}},
					},
				},
			}

			got, err := (&HavingBinder{}).remapAggToTimeWindowResultAgg(expr)
			require.NoError(t, err)
			require.Equal(t, "sum_tw_result", got.Expr.(*plan.Expr_F).F.Func.ObjName)
			require.Equal(t, int32(typ.Oid), got.Typ.Id)
		})
	}
}
