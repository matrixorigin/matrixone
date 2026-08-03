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
	limit := uint64(^uint(0) >> 1)

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
		OrderBy:      []*plan.OrderBySpec{{Expr: orderExpr}},
		Limit:        plan2.MakePlan2Uint64ConstExprWithType(limit),
		OrigFuncName: metric.DistFn_L2Distance,
		DistRange: &plan.DistRange{
			LowerBoundType: plan.BoundType_INCLUSIVE,
			LowerBound: &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_float64)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: -1}}},
			},
			UpperBoundType: plan.BoundType_INCLUSIVE,
			UpperBound: &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_float64)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: 2}}},
			},
		},
	}

	require.NotPanics(t, func() {
		r.SetIndexParam(param)
	})
	require.NotNil(t, r.orderByLimit)
	require.Equal(t, limit, r.orderByLimit.Limit)
	require.Equal(t, plan.BoundType_UNBOUNDED, r.orderByLimit.LowerBoundType)
	require.Equal(t, plan.BoundType_INCLUSIVE, r.orderByLimit.UpperBoundType)
	require.Equal(t, float64(4), r.orderByLimit.UpperBound)
	require.Zero(t, len(r.orderByLimit.DistHeap))
	require.Zero(t, cap(r.orderByLimit.DistHeap))
}

func TestReaderSetIndexParamSupportsOrderedLimit(t *testing.T) {
	r := &reader{}
	param := &plan.IndexReaderParam{
		OrderBy: []*plan.OrderBySpec{
			{
				Expr: &plan.Expr{
					Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{ColPos: 1},
					},
				},
				Flag: plan.OrderBySpec_DESC,
			},
		},
		Limit: plan2.MakePlan2Uint64ConstExprWithType(8),
	}

	r.SetIndexParam(param)

	require.NotNil(t, r.orderByLimit)
	require.True(t, r.orderByLimit.OrderedLimit)
	require.True(t, r.orderByLimit.Desc)
	require.Equal(t, int32(1), r.orderByLimit.ColPos)
	require.Equal(t, uint64(8), r.orderByLimit.Limit)
	require.Nil(t, r.orderByLimit.NumVec)
}

func TestReaderSetIndexParamIgnoresUnevaluatedLimit(t *testing.T) {
	validOrderBy := []*plan.OrderBySpec{{
		Expr: &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}},
		},
	}}
	params := []*plan.IndexReaderParam{{
		OrderBy: validOrderBy,
		Limit: &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_uint64)},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		},
	}, {
		OrderBy: validOrderBy,
		Limit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Isnull: true,
			Value:  &plan.Literal_U64Val{U64Val: 8},
		}}},
	}, {
		OrderBy: validOrderBy,
		Limit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_I64Val{I64Val: 8},
		}}},
	}, {
		OrderBy: []*plan.OrderBySpec{{
			Expr: nil,
		}},
		Limit: plan2.MakePlan2Uint64ConstExprWithType(8),
	}, {
		OrderBy: []*plan.OrderBySpec{nil},
		Limit:   plan2.MakePlan2Uint64ConstExprWithType(8),
	}, {
		OrderBy: validOrderBy,
		Limit:   plan2.MakePlan2Uint64ConstExprWithType(0),
	}, {
		OrderBy: validOrderBy,
		Limit:   plan2.MakePlan2Uint64ConstExprWithType(uint64(^uint(0)>>1) + 1),
	}}

	for _, param := range params {
		r := &reader{}
		require.NotPanics(t, func() { r.SetIndexParam(param) })
		require.Nil(t, r.orderByLimit)
	}
}
