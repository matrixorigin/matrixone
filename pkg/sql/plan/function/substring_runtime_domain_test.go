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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSubstringExactBinaryRuntimeDomain(t *testing.T) {
	for _, oid := range []types.T{types.T_binary, types.T_varbinary} {
		for _, arity := range []int{2, 3} {
			t.Run(fmt.Sprintf("%s/%d", oid, arity), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				mp := proc.Mp()
				input := makeBinaryStringTestInput(t, proc, oid.ToType(), [][]byte{[]byte("你好"), []byte("你好"), nil}, []types.RuntimeStringDomain{types.RuntimeStringText, types.RuntimeStringBinary, types.RuntimeStringText})
				defer input.Free(mp)
				input.GetNulls().Add(2)
				two := makeBinaryStringInt64Input(t, proc, []int64{2, 2, 2})
				defer two.Free(mp)
				left, err := GetFunctionByName(proc.Ctx, "left", []types.Type{*input.GetType(), *two.GetType()})
				require.NoError(t, err)
				source, err := RunFunctionDirectly(proc, left.GetEncodedOverloadID(), []*vector.Vector{input, two}, 3)
				require.NoError(t, err)
				defer source.Free(mp)
				require.Equal(t, types.T_varbinary, source.GetType().Oid)
				// LEFT normalizes BINARY to VARBINARY; exercise the exact BINARY
				// registrations separately using the original typed vector.
				if oid == types.T_binary {
					source = input
				}
				require.Equal(t, []byte("你好"), source.GetBytesAt(0))
				require.Equal(t, types.RuntimeStringText, source.GetRuntimeStringDomainAt(0))
				inputs := []*vector.Vector{source, two}
				if arity == 3 {
					one := makeBinaryStringInt64Input(t, proc, []int64{1, 1, 1})
					defer one.Free(mp)
					inputs = append(inputs, one)
				}
				inputTypes := make([]types.Type, len(inputs))
				for i, input := range inputs {
					inputTypes[i] = *input.GetType()
				}
				resolved, err := GetFunctionByName(proc.Ctx, "substring", inputTypes)
				require.NoError(t, err)
				_, cast := resolved.ShouldDoImplicitTypeCast()
				require.False(t, cast)
				output, err := RunFunctionDirectly(proc, resolved.GetEncodedOverloadID(), inputs, 3)
				require.NoError(t, err)
				defer output.Free(mp)
				require.Equal(t, types.T_varbinary, output.GetType().Oid)
				_, overloadIndex := DecodeOverloadID(resolved.GetEncodedOverloadID())
				wantOverload := int32(7 + 2*(arity-2))
				if oid == types.T_varbinary {
					wantOverload++
				}
				require.Equal(t, wantOverload, overloadIndex)
				require.Equal(t, []byte("好"), output.GetBytesAt(0))
				wantBinary := []byte{0xbd}
				if oid == types.T_binary && arity == 2 {
					wantBinary = []byte{0xbd, 0xa0, 0xe5, 0xa5, 0xbd}
				}
				require.Equal(t, wantBinary, output.GetBytesAt(1))
				require.Equal(t, types.RuntimeStringText, output.GetRuntimeStringDomainAt(0))
				require.NotEqual(t, types.RuntimeStringText, output.GetRuntimeStringDomainAt(1))
				require.True(t, output.IsNull(2))

				overload, err := GetFunctionById(proc.Ctx, resolved.GetEncodedOverloadID())
				require.NoError(t, err)
				exec, _, _, _ := overload.GetExecuteMethod()
				masked := vector.NewFunctionResultWrapper(resolved.GetReturnType(), mp)
				defer masked.Free()
				require.NoError(t, masked.PreExtendAndReset(3))
				require.NoError(t, exec(inputs, masked, proc, 3, &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, false}}))
				require.Equal(t, []byte("好"), masked.GetResultVector().GetBytesAt(0))
				require.Equal(t, types.RuntimeStringText, masked.GetResultVector().GetRuntimeStringDomainAt(0))
				require.True(t, masked.GetResultVector().IsNull(1))
				require.True(t, masked.GetResultVector().IsNull(2))
			})
		}
	}
}
