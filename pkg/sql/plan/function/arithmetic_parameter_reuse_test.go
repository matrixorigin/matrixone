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

package function

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMixedDecimalPlusReuse(t *testing.T) {
	proc := testutil.NewProcess(nil)
	t.Cleanup(func() {
		proc.Base.FileService.Close(proc.Ctx)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	dt := types.New(types.T_decimal64, 18, 2)
	rt := types.New(types.T_decimal128, 38, 16)
	for _, constant := range []bool{false, true} {
		for _, nullSide := range []int{0, 1} {
			t.Run(fmt.Sprintf("constant=%v/null_side=%d", constant, nullSide), func(t *testing.T) {
				a := vector.NewVec(dt)
				defer a.Free(proc.Mp())
				require.NoError(t, vector.AppendFixed(a, types.Decimal64(123), false, proc.Mp()))
				b := vector.NewVec(types.T_float64.ToType())
				defer b.Free(proc.Mp())
				require.NoError(t, vector.AppendFixed(b, float64(2), false, proc.Mp()))
				if constant {
					a.SetClass(vector.CONSTANT)
					b.SetClass(vector.CONSTANT)
				}
				inputs := []*vector.Vector{a, b}
				nilInput := vector.NewConstNull(*inputs[nullSide].GetType(), 1, proc.Mp())
				defer nilInput.Free(proc.Mp())
				rs := vector.NewFunctionResultWrapper(rt, proc.Mp())
				defer rs.Free()
				for call, isNull := range []bool{true, false, false, true, false} {
					parameters := []*vector.Vector{a, b}
					if isNull {
						parameters[nullSide] = nilInput
					}
					require.NoError(t, rs.PreExtendAndReset(1))
					require.NoError(t, plusFn(parameters, rs, proc, 1, nil), "call %d", call)
					out := rs.GetResultVector()
					require.Equal(t, rt, *out.GetType())
					require.Equal(t, 1, out.Length())
					require.Equal(t, isNull, out.IsNull(0), "call %d", call)
					if !isNull {
						require.True(t, out.GetNulls().IsEmpty())
						require.Equal(t, types.Decimal128{B0_63: 32300000000000000}, vector.MustFixedColWithTypeCheck[types.Decimal128](out)[0])
					}
				}
			})
		}
	}
}
