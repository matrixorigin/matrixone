// Copyright 2024 Matrix Origin
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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestComparisonTypeCastOptimization tests that comparison operators avoid casting columns
// when comparing with constants to preserve index usage
func TestComparisonTypeCastOptimization(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		op             string
		colType        types.T
		constType      types.T
		constValue     interface{}
		shouldOptimize bool
		expectedType   types.T
	}{
		// INT vs INT - should optimize
		{
			name:           "int32 = int64",
			op:             "=",
			colType:        types.T_int32,
			constType:      types.T_int64,
			shouldOptimize: true,
			expectedType:   types.T_int32,
		},
		// INT vs DECIMAL with zero fractional part - should optimize
		// INT vs DECIMAL with zero fractional part - should NOT optimize
		// We keep DECIMAL128 to preserve semantics
		{
			name:           "int32 = 9.0",
			op:             "=",
			colType:        types.T_int32,
			constType:      types.T_decimal128,
			constValue:     "9.0",
			shouldOptimize: false,
			expectedType:   types.T_decimal128,
		},
		// INT vs DECIMAL with non-zero fractional part - should NOT optimize
		{
			name:           "int32 = 9.000002",
			op:             "=",
			colType:        types.T_int32,
			constType:      types.T_decimal128,
			constValue:     "9.000002",
			shouldOptimize: false,
			expectedType:   types.T_decimal128,
		},
		// FLOAT32 vs DECIMAL - should optimize
		{
			name:           "float32 = 9.0",
			op:             "=",
			colType:        types.T_float32,
			constType:      types.T_decimal128,
			shouldOptimize: true,
			expectedType:   types.T_float32,
		},
		// FLOAT64 vs DECIMAL - should optimize
		{
			name:           "float64 = 9.0",
			op:             "=",
			colType:        types.T_float64,
			constType:      types.T_decimal128,
			shouldOptimize: true,
			expectedType:   types.T_float64,
		},
		// FLOAT vs FLOAT - should optimize
		{
			name:           "float32 = float64",
			op:             "=",
			colType:        types.T_float32,
			constType:      types.T_float64,
			shouldOptimize: true,
			expectedType:   types.T_float32,
		},
		// DECIMAL vs DECIMAL - should optimize
		{
			name:           "decimal64 = decimal128",
			op:             "=",
			colType:        types.T_decimal64,
			constType:      types.T_decimal128,
			shouldOptimize: true,
			expectedType:   types.T_decimal64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create column expression
			colExpr := &plan.Expr{
				Typ: plan.Type{Id: int32(tt.colType)},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			}

			// Create constant expression
			constExpr := &plan.Expr{
				Typ: plan.Type{Id: int32(tt.constType)},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
					},
				},
			}

			// Set constant value based on type
			if tt.constType == types.T_decimal128 && tt.constValue != nil {
				// Parse decimal value
				dec, scale, err := types.Parse128(tt.constValue.(string))
				require.NoError(t, err)
				constExpr.Expr.(*plan.Expr_Lit).Lit.Value = &plan.Literal_Decimal128Val{
					Decimal128Val: &plan.Decimal128{
						A: int64(dec.B0_63),
						B: int64(dec.B64_127),
					},
				}
				constExpr.Typ.Scale = scale
				constExpr.Typ.Width = 38
			}

			// Call BindFuncExprImplByPlanExpr
			result, err := BindFuncExprImplByPlanExpr(ctx, tt.op, []*plan.Expr{colExpr, constExpr})
			require.NoError(t, err)
			require.NotNil(t, result)

			// Check if optimization was applied by examining the function arguments
			funcExpr := result.GetF()
			require.NotNil(t, funcExpr)

			// The optimization should result in both arguments having the column's type
			if tt.shouldOptimize {
				// Both arguments should be cast to column type
				require.Equal(t, int32(tt.expectedType), funcExpr.Args[0].Typ.Id,
					"Left argument should have column type")
				require.Equal(t, int32(tt.expectedType), funcExpr.Args[1].Typ.Id,
					"Right argument should have column type")
			}
		})
	}
}
