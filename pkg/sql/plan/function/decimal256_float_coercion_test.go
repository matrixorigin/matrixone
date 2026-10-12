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

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestDecimal256FloatCoercionIsSymmetric(t *testing.T) {
	decimal := types.New(types.T_decimal256, 65, 30)

	for _, floatType := range []types.T{types.T_float32, types.T_float64} {
		for _, reverse := range []bool{false, true} {
			name := floatType.String() + "_left"
			left, right := floatType.ToType(), decimal
			if reverse {
				name = floatType.String() + "_right"
				left, right = decimal, floatType.ToType()
			}

			t.Run(name, func(t *testing.T) {
				wantLeftScale, wantRightScale := int32(0), int32(16)
				if reverse {
					wantLeftScale, wantRightScale = wantRightScale, wantLeftScale
				}

				for _, rule := range []struct {
					name string
					fn   func(types.Type, types.Type) (bool, types.Type, types.Type)
				}{
					{name: "arithmetic and comparison", fn: fixedTypeCastRule1},
					{name: "division and integer division", fn: fixedTypeCastRule2},
				} {
					t.Run(rule.name, func(t *testing.T) {
						hasCast, gotLeft, gotRight := rule.fn(left, right)
						require.True(t, hasCast)
						require.Equal(t, types.T_float64, gotLeft.Oid)
						require.Equal(t, types.T_float64, gotRight.Oid)
						require.Equal(t, wantLeftScale, gotLeft.Scale)
						require.Equal(t, wantRightScale, gotRight.Scale)
					})
				}
			})
		}
	}
}

func TestDecimal256FloatOperatorsResolveBothOrders(t *testing.T) {
	decimal := types.New(types.T_decimal256, 65, 30)
	ctx := context.Background()

	for _, floatType := range []types.T{types.T_float32, types.T_float64} {
		for _, reverse := range []bool{false, true} {
			name := floatType.String() + "_left"
			left, right := floatType.ToType(), decimal
			if reverse {
				name = floatType.String() + "_right"
				left, right = decimal, floatType.ToType()
			}

			t.Run(name, func(t *testing.T) {
				for _, operator := range []string{"+", "-", "*", "/", "%", "div"} {
					t.Run(operator, func(t *testing.T) {
						resolved, err := GetFunctionByName(ctx, operator, []types.Type{left, right})
						require.NoError(t, err)

						targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
						require.True(t, shouldCast)
						require.Len(t, targets, 2)
						require.Equal(t, types.T_float64, targets[0].Oid)
						require.Equal(t, types.T_float64, targets[1].Oid)
						if operator == "div" {
							require.Equal(t, types.T_int64, resolved.GetReturnType().Oid)
						} else {
							require.Equal(t, types.T_float64, resolved.GetReturnType().Oid)
						}
					})
				}

				for _, operator := range []string{"=", "<"} {
					t.Run("comparison "+operator, func(t *testing.T) {
						resolved, err := GetFunctionByName(ctx, operator, []types.Type{left, right})
						require.NoError(t, err)
						targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
						require.True(t, shouldCast)
						require.Equal(t, types.T_float64, targets[0].Oid)
						require.Equal(t, types.T_float64, targets[1].Oid)
						require.Equal(t, types.T_bool, resolved.GetReturnType().Oid)
					})
				}
			})
		}
	}
}

func TestResolveDecimal256FloatOperatorsBothOrders(t *testing.T) {
	decimal := types.New(types.T_decimal256, 65, 30)

	for _, floatType := range []types.T{types.T_float32, types.T_float64} {
		for _, reverse := range []bool{false, true} {
			name := floatType.String() + "_left"
			left, right := floatType.ToType(), decimal
			if reverse {
				name = floatType.String() + "_right"
				left, right = decimal, floatType.ToType()
			}

			t.Run(name, func(t *testing.T) {
				for _, operator := range []struct {
					name string
					op   numericBinaryOp
					ret  types.T
				}{
					{name: "+", op: numericOpAdd, ret: types.T_float64},
					{name: "-", op: numericOpSub, ret: types.T_float64},
					{name: "*", op: numericOpMul, ret: types.T_float64},
					{name: "/", op: numericOpDiv, ret: types.T_float64},
					{name: "%", op: numericOpMod, ret: types.T_float64},
					{name: "div", op: numericOpIntegerDiv, ret: types.T_int64},
				} {
					t.Run(operator.name, func(t *testing.T) {
						resolved, ok := resolveNumericBinaryTypes(operator.op, left, right, nil)
						require.True(t, ok)
						require.Equal(t, types.T_float64, resolved.left.Oid)
						require.Equal(t, types.T_float64, resolved.right.Oid)
						require.Equal(t, operator.ret, resolved.result.Oid)
					})
				}
			})
		}
	}

	_, _, _, ok := ResolveNumericBinaryTypes("+", types.T_varchar.ToType(), decimal, nil)
	require.False(t, ok)
}

func TestDecimal256FloatConditionalComparisonsResolve(t *testing.T) {
	decimal := types.New(types.T_decimal256, 65, 30)
	ctx := context.Background()

	for _, args := range [][]types.Type{
		{types.T_float64.ToType(), decimal, decimal},
		{decimal, types.T_float64.ToType(), types.T_float64.ToType()},
	} {
		resolved, err := GetFunctionByName(ctx, "between", args)
		require.NoError(t, err)
		targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
		require.True(t, shouldCast)
		require.Len(t, targets, 3)
		for _, target := range targets {
			require.Equal(t, types.T_float64, target.Oid)
		}

		inRangeArgs := append(append([]types.Type{}, args...), types.T_uint8.ToType())
		resolved, err = GetFunctionByName(ctx, "in_range", inRangeArgs)
		require.NoError(t, err)
		targets, shouldCast = resolved.ShouldDoImplicitTypeCast()
		require.True(t, shouldCast)
		require.Len(t, targets, 4)
		for _, target := range targets[:3] {
			require.Equal(t, types.T_float64, target.Oid)
		}
		require.Equal(t, types.T_uint8, targets[3].Oid)
	}
}
