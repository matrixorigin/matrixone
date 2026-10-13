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

func TestDecimalComparisonPreservesIntegralCapacity(t *testing.T) {
	for _, tc := range []struct {
		name              string
		left, right, want types.Type
	}{
		{"fractional alignment", types.New(types.T_decimal64, 6, 5), types.New(types.T_decimal64, 5, 4), types.New(types.T_decimal64, 6, 5)},
		{"different integral capacities", types.New(types.T_decimal64, 6, 5), types.New(types.T_decimal64, 6, 4), types.New(types.T_decimal64, 7, 5)},
		{"promote to decimal128", types.New(types.T_decimal64, 18, 0), types.New(types.T_decimal64, 18, 1), types.New(types.T_decimal128, 19, 1)},
		{"promote to decimal256", types.New(types.T_decimal128, 38, 0), types.New(types.T_decimal128, 38, 1), types.New(types.T_decimal256, 39, 1)},
		{"mixed physical widths", types.New(types.T_decimal64, 6, 5), types.New(types.T_decimal128, 19, 4), types.New(types.T_decimal128, 20, 5)},
		{"integer domain", types.T_int64.ToType(), types.New(types.T_decimal64, 6, 5), types.New(types.T_decimal128, 24, 5)},
		{"maximum precision", types.New(types.T_decimal256, 75, 0), types.New(types.T_decimal256, 76, 1), types.New(types.T_decimal256, 76, 1)},
		{"unspecified precision", types.New(types.T_decimal64, 0, 0), types.New(types.T_decimal64, 18, 1), types.New(types.T_decimal128, 19, 1)},
		{"high precision overlapping values", types.New(types.T_decimal256, 65, 0), types.New(types.T_decimal256, 65, 30), types.New(types.T_decimal256, 76, 30)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, op := range []string{"=", "<=>", "in_range"} {
				for _, args := range [][]types.Type{{tc.left, tc.right}, {tc.right, tc.left}} {
					if op == "in_range" {
						args = append(args, tc.right, types.T_uint8.ToType())
					}
					resolved, err := GetFunctionByName(context.Background(), op, args)
					require.NoError(t, err, "%s %v", op, args)
					targets, cast := resolved.ShouldDoImplicitTypeCast()
					require.True(t, cast, "%s %v", op, args)
					count := len(args)
					if op == "in_range" {
						count--
						require.Equal(t, args[3], targets[3])
					}
					for _, target := range targets[:count] {
						require.Equal(t, tc.want, target, "%s %v", op, args)
					}
				}
			}
		})
	}
}

func TestDecimalComparisonAlignmentControls(t *testing.T) {
	for _, op := range []string{"=", "<=>", "in_range"} {
		for _, tc := range []struct {
			name      string
			args      []types.Type
			wantError bool
		}{
			{"same scale", []types.Type{types.New(types.T_decimal64, 6, 4), types.New(types.T_decimal64, 5, 4)}, false},
			{"bounded physical domain", []types.Type{types.New(types.T_decimal256, 76, 0), types.New(types.T_decimal256, 76, 1)}, false},
		} {
			t.Run(op+"/"+tc.name, func(t *testing.T) {
				args := append([]types.Type{}, tc.args...)
				if op == "in_range" {
					args = append(args, args[1], types.T_uint8.ToType())
				}
				resolved, err := GetFunctionByName(context.Background(), op, args)
				if tc.wantError {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				targets, cast := resolved.ShouldDoImplicitTypeCast()
				if tc.name == "bounded physical domain" {
					require.True(t, cast)
					for _, target := range targets[:2] {
						require.Equal(t, types.New(types.T_decimal256, 76, 1), target)
					}
				} else if cast {
					require.Equal(t, args, targets)
				}
			})
		}
	}
}

func TestDecimalComparisonNullAndInvalidInputs(t *testing.T) {
	for _, op := range []string{"=", "<=>"} {
		for _, args := range [][]types.Type{
			{}, {types.T_decimal64.ToType()},
			{types.T_uuid.ToType(), types.T_decimal64.ToType()},
		} {
			_, err := GetFunctionByName(context.Background(), op, args)
			require.Error(t, err)
		}
		resolved, err := GetFunctionByName(context.Background(), op, []types.Type{types.T_any.ToType(), types.New(types.T_decimal64, 6, 5)})
		require.NoError(t, err)
		targets, cast := resolved.ShouldDoImplicitTypeCast()
		require.True(t, cast)
		require.Equal(t, []types.Type{types.New(types.T_decimal64, 6, 5), types.New(types.T_decimal64, 6, 5)}, targets)
	}
	resolved, err := GetFunctionByName(context.Background(), "in_range", []types.Type{
		types.T_any.ToType(), types.New(types.T_decimal64, 6, 5), types.New(types.T_decimal64, 5, 4), types.T_uint8.ToType(),
	})
	require.NoError(t, err)
	targets, cast := resolved.ShouldDoImplicitTypeCast()
	require.True(t, cast)
	for _, target := range targets[:3] {
		require.Equal(t, types.New(types.T_decimal64, 6, 5), target)
	}
}

func TestDecimalInRangeAlignsEveryOperand(t *testing.T) {
	for _, args := range [][]types.Type{
		{types.New(types.T_decimal64, 4, 2), types.New(types.T_decimal64, 1, 1), types.New(types.T_decimal64, 3, 1)},
		{types.New(types.T_decimal64, 1, 1), types.New(types.T_decimal64, 4, 2), types.New(types.T_decimal64, 3, 1)},
		{types.New(types.T_decimal64, 1, 1), types.New(types.T_decimal64, 3, 1), types.New(types.T_decimal64, 4, 2)},
	} {
		args = append(args, types.T_uint8.ToType())
		resolved, err := GetFunctionByName(context.Background(), "in_range", args)
		require.NoError(t, err)
		targets, cast := resolved.ShouldDoImplicitTypeCast()
		require.True(t, cast)
		for _, target := range targets[:3] {
			require.Equal(t, types.New(types.T_decimal64, 4, 2), target)
		}
	}
}
