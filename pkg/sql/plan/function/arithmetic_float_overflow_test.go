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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestFloatArithmeticOverflowChecks(t *testing.T) {
	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "float64 subtraction",
			run: func() error {
				_, err := subFloat64WithOverflowCheck(context.Background(), math.MaxFloat64, -math.MaxFloat64)
				return err
			},
		},
		{
			name: "float64 multiplication",
			run: func() error {
				_, err := mulFloat64WithOverflowCheck(context.Background(), math.MaxFloat64, 2)
				return err
			},
		},
		{
			name: "float32 subtraction",
			run: func() error {
				_, err := subFloat32WithOverflowCheck(context.Background(), math.MaxFloat32, -math.MaxFloat32)
				return err
			},
		},
		{
			name: "float32 multiplication",
			run: func() error {
				_, err := mulFloat32WithOverflowCheck(context.Background(), math.MaxFloat32, 2)
				return err
			},
		},
		{
			name: "float64 division",
			run: func() error {
				_, err := divFloat64WithOverflowCheck(context.Background(), math.MaxFloat64, math.SmallestNonzeroFloat64)
				return err
			},
		},
		{
			name: "float32 division",
			run: func() error {
				_, err := divFloat32WithOverflowCheck(context.Background(), math.MaxFloat32, math.SmallestNonzeroFloat32)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.run()
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange))
		})
	}
}

func TestFloatArithmeticOverflowChecksKeepFiniteAndExplicitInfinity(t *testing.T) {
	result, err := subFloat64WithOverflowCheck(context.Background(), 5, 3)
	require.NoError(t, err)
	require.Equal(t, float64(2), result)

	result, err = mulFloat64WithOverflowCheck(context.Background(), math.Inf(1), 2)
	require.NoError(t, err)
	require.True(t, math.IsInf(result, 1))

	result, err = addFloat64WithOverflowCheck(context.Background(), math.Inf(1), 2)
	require.NoError(t, err)
	require.True(t, math.IsInf(result, 1))
}

func TestDecimalArithmeticErrorMapping(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	decimalType := types.T_decimal128.ToType()
	decimalType.Width = 38
	inputs := []*vector.Vector{
		mustNewConstFixed(t, decimalType, types.Decimal128{B0_63: math.MaxUint64, B64_127: math.MaxInt64}, proc),
		mustNewConstFixed(t, decimalType, types.Decimal128{B0_63: 1}, proc),
	}
	defer inputs[0].Free(proc.Mp())
	defer inputs[1].Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(decimalType, proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))

	err := decimalBatchArith[types.Decimal128, types.Decimal128](inputs, result, proc, 1, d128Add, nil)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange))
}

func mustNewConstFixed[T any](t *testing.T, typ types.Type, value T, proc *process.Process) *vector.Vector {
	t.Helper()
	vec, err := vector.NewConstFixed(typ, value, 1, proc.Mp())
	require.NoError(t, err)
	return vec
}
