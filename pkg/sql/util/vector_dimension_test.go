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

package util

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestVectorLiteralAndParameterDimensions(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float32, types.T_array_float64, types.T_array_bf16,
		types.T_array_float16, types.T_array_int8, types.T_array_uint8} {
		for _, width := range []int32{1, 2, types.MaxArrayDimension} {
			t.Run(fmt.Sprintf("%s/%d", oid, width), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				typ := types.New(oid, width, 0)
				literal := tree.NewNumVal("[1,2]", "[1,2]", false, tree.P_char)
				_, value, err := SetInsertValueString(proc, literal, &typ)
				if width == 1 {
					require.ErrorContains(t, err, "expected vector dimension 1 != actual dimension 2")
				} else {
					require.NoError(t, err)
					require.Len(t, value, 2*typ.GetArrayElementSize())
				}
				vec, err := GenVectorByVarValue(proc, typ, "[1,2]")
				if vec != nil {
					t.Cleanup(func() { vec.Free(proc.Mp()) })
				}
				if width == 1 {
					require.ErrorContains(t, err, "expected vector dimension 1 != actual dimension 2")
				} else {
					require.NoError(t, err)
					require.Equal(t, value, vec.GetBytesAt(0))
				}
				_, _, err = SetInsertValueString(proc, tree.NewNumVal("[bad]", "[bad]", false, tree.P_char), &typ)
				require.Error(t, err)
				if width == types.MaxArrayDimension {
					for _, n := range []int{types.MaxArrayDimension, types.MaxArrayDimension + 1} {
						payload := make([]byte, n*typ.GetArrayElementSize())
						_, err = arrayUserVariableValueToBytes(typ, payload)
						if n == types.MaxArrayDimension {
							require.NoError(t, err)
						} else {
							require.ErrorContains(t, err, "exceeds maximum")
						}
					}
				}
			})
		}
	}
}
