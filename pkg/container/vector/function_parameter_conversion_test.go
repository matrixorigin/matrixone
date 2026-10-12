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

package vector

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestConvertedParameterReuse(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		oid                  types.T
		scale                int32
		coefficient, changed uint64
	}{
		{"decimal64", types.T_decimal64, 2, 123, 250},
		{"float32", types.T_float32, 7, 12500000, 25000000},
		{"float64", types.T_float64, 16, 12500000000000000, 25000000000000000},
		{"native_decimal128", types.T_decimal128, 2, 123, 250},
	} {
		for _, constant := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/constant=%v", tc.name, constant), func(t *testing.T) {
				mp := mpool.MustNewZeroNoFixed()
				t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
				typ := tc.oid.ToType()
				if tc.oid == types.T_decimal64 || tc.oid == types.T_decimal128 {
					typ.Scale = 2
				}
				input := NewVec(typ)
				defer input.Free(mp)
				for i := 0; i < 2; i++ {
					switch tc.oid {
					case types.T_decimal64:
						require.NoError(t, AppendFixed(input, types.Decimal64(123), false, mp))
					case types.T_float32:
						require.NoError(t, AppendFixed(input, float32(1.25), false, mp))
					case types.T_float64:
						require.NoError(t, AppendFixed(input, float64(1.25), false, mp))
					case types.T_decimal128:
						require.NoError(t, AppendFixed(input, types.Decimal128{B0_63: 123}, false, mp))
					}
					if constant {
						input.SetClass(CONSTANT)
						input.SetLength(2)
						break
					}
				}
				nilInput := NewConstNull(typ, 2, mp)
				defer nilInput.Free(mp)
				result := NewFunctionResultWrapper(types.T_decimal128.ToType(), mp)
				defer result.Free()
				result.UseOptFunctionParamFrame(1)
				wantType := types.T_decimal128.ToType()
				wantType.Width, wantType.Scale = 38, tc.scale
				check := func(want uint64, nullable bool) {
					t.Helper()
					parameter := OptGetParamFromWrapper[types.Decimal128](result, 0, input)
					require.Equal(t, wantType, parameter.GetType())
					require.Same(t, input, parameter.GetSourceVector())
					require.Equal(t, typ, *input.GetType())
					for row := uint64(0); row < 2; row++ {
						value, isNull := parameter.GetValue(row)
						require.Equal(t, nullable && row == 1, isNull)
						if !isNull {
							require.Equal(t, types.Decimal128{B0_63: want}, value)
						}
					}
					// Native D128 controls must retain the successful, allocation-free reuse path.
					require.Equal(t, tc.oid == types.T_decimal128, ReuseFunctionFixedTypeParameter(input, parameter))
				}
				nullToValue := func() {
					t.Helper()
					old := OptGetParamFromWrapper[types.Decimal128](result, 0, nilInput)
					require.True(t, ReuseFunctionFixedTypeParameter(nilInput, old))
					require.False(t, ReuseFunctionFixedTypeParameter(input, old))
					_, isNull := old.GetValue(0)
					require.True(t, isNull)
					require.Equal(t, typ, old.GetType())
					require.Same(t, nilInput, old.GetSourceVector())
					fresh := OptGetParamFromWrapper[types.Decimal128](result, 0, input)
					require.NotSame(t, old, fresh)
					require.Same(t, nilInput, old.GetSourceVector())
					_, isNull = old.GetValue(0)
					require.True(t, isNull)
				}
				nullToValue()
				check(tc.coefficient, false)
				check(tc.coefficient, false)
				switch tc.oid {
				case types.T_decimal64:
					MustFixedColWithTypeCheck[types.Decimal64](input)[0] = 250
				case types.T_float32:
					MustFixedColWithTypeCheck[float32](input)[0] = 2.5
				case types.T_float64:
					MustFixedColWithTypeCheck[float64](input)[0] = 2.5
				case types.T_decimal128:
					MustFixedColWithTypeCheck[types.Decimal128](input)[0] = types.Decimal128{B0_63: 250}
				}
				if !constant {
					nulls.Add(input.GetNulls(), 1)
				}
				check(tc.changed, !constant)
				nullToValue()
				check(tc.changed, !constant)
			})
		}
	}
}
