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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func approxPercentileValueColumn() *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			ColPos: 0,
			Name:   "v",
		}},
	}
}

func TestBindApproxPercentileRequiresStableNonNullPercentile(t *testing.T) {
	ctx := context.Background()
	percentileColumn := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_float64)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			ColPos: 1,
			Name:   "p",
		}},
	}

	_, err := BindFuncExprImplByPlanExpr(ctx, "approx_percentile", []*planpb.Expr{
		approxPercentileValueColumn(),
		makePlan2NullConstExprWithType(),
	})
	require.ErrorContains(t, err, "percentile argument of approx_percentile must be a non-null constant")

	_, err = BindFuncExprImplByPlanExpr(ctx, "approx_percentile", []*planpb.Expr{
		approxPercentileValueColumn(),
		nil,
	})
	require.ErrorContains(t, err, "percentile argument of approx_percentile must be a non-null constant")

	_, err = BindFuncExprImplByPlanExpr(ctx, "approx_percentile", []*planpb.Expr{
		approxPercentileValueColumn(),
		percentileColumn,
	})
	require.ErrorContains(t, err, "percentile argument of approx_percentile must be a non-null constant")
}

func TestBindApproxPercentileAcceptsExecutionConstants(t *testing.T) {
	ctx := context.Background()
	parameter := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_float64)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	foldable, err := BindFuncExprImplByPlanExpr(ctx, "+", []*planpb.Expr{
		makePlan2Float64ConstExprWithType(0.4),
		makePlan2Float64ConstExprWithType(0.1),
	})
	require.NoError(t, err)

	for _, percentile := range []*planpb.Expr{
		makePlan2Float64ConstExprWithType(0.95),
		foldable,
		parameter,
	} {
		_, err = BindFuncExprImplByPlanExpr(ctx, "approx_percentile", []*planpb.Expr{
			approxPercentileValueColumn(),
			percentile,
		})
		require.NoError(t, err)
	}
}
