// Copyright 2021 Matrix Origin
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

package readutil

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

func TestReaderSetIndexParamDoesNotPreallocateDistHeap(t *testing.T) {
	r := &reader{}
	limit := ^uint64(0)

	vectorCol := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_array_float32), Width: 2},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{ColPos: 3},
		},
	}
	vectorLit := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_array_float32), Width: 2, NotNullable: true},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_VecVal{
					VecVal: string(types.ArrayToBytes[float32]([]float32{0, 0})),
				},
			},
		},
	}
	orderExpr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_float64), NotNullable: true},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: metric.DistFn_L2Distance},
				Args: []*plan.Expr{
					vectorCol,
					vectorLit,
				},
			},
		},
	}
	param := &plan.IndexReaderParam{
		OrderBy: []*plan.OrderBySpec{{Expr: orderExpr}},
		Limit:   plan2.MakePlan2Uint64ConstExprWithType(limit),
	}

	require.NotPanics(t, func() {
		r.SetIndexParam(param)
	})
	require.NotNil(t, r.orderByLimit)
	require.Equal(t, limit, r.orderByLimit.Limit)
	require.Zero(t, len(r.orderByLimit.DistHeap))
	require.Zero(t, cap(r.orderByLimit.DistHeap))
}
