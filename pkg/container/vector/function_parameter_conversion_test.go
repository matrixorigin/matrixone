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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConvertedParameterReuse(t *testing.T) {
	for _, tc := range []struct {
		name        string
		oid         types.T
		scale       int32
		coefficient uint64
	}{
		{"decimal64", types.T_decimal64, 2, 123},
		{"float32", types.T_float32, 7, 12500000},
		{"float64", types.T_float64, 16, 12500000000000000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			for _, constant := range []bool{false, true} {
				func() {
					typ := tc.oid.ToType()
					if tc.oid == types.T_decimal64 {
						typ.Width = 18
						typ.Scale = 2
					}
					var input *Vector
					var err error
					if constant {
						switch tc.oid {
						case types.T_decimal64:
							input, err = NewConstFixed(typ, types.Decimal64(123), 1, mp)
						case types.T_float32:
							input, err = NewConstFixed(typ, float32(1.25), 1, mp)
						case types.T_float64:
							input, err = NewConstFixed(typ, float64(1.25), 1, mp)
						}
						require.NoError(t, err)
					} else {
						input = NewVec(typ)
					}
					defer input.Free(mp)
					if !constant {
						switch tc.oid {
						case types.T_decimal64:
							err = AppendFixed(input, types.Decimal64(123), false, mp)
						case types.T_float32:
							err = AppendFixed(input, float32(1.25), false, mp)
						case types.T_float64:
							err = AppendFixed(input, float64(1.25), false, mp)
						}
						require.NoError(t, err)
					}
					result := NewFunctionResultWrapper(types.T_decimal128.ToType(), mp)
					defer result.Free()
					result.UseOptFunctionParamFrame(1)
					wantType := types.T_decimal128.ToType()
					wantType.Width = 38
					wantType.Scale = tc.scale
					for call := 0; call < 2; call++ {
						parameter := OptGetParamFromWrapper[types.Decimal128](result, 0, input)
						value, isNull := parameter.GetValue(0)
						require.Equal(t, types.Decimal128{B0_63: tc.coefficient}, value, "constant=%v call=%d", constant, call)
						require.False(t, isNull)
						require.True(t, wantType == parameter.GetType())
						require.Same(t, input, parameter.GetSourceVector())
						require.True(t, typ == *input.GetType())
					}
				}()
				require.Zero(t, mp.CurrNB())
			}
		})
	}
}
