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

func TestVectorSelectionDimensions(t *testing.T) {
	oids := []types.T{types.T_array_float32, types.T_array_float64, types.T_array_bf16,
		types.T_array_float16, types.T_array_int8, types.T_array_uint8}
	for _, name := range []string{"coalesce", "least", "greatest"} {
		for _, oid := range oids {
			t.Run(name+"/"+oid.String(), func(t *testing.T) {
				typ := types.New(oid, 2, 0)
				for _, inputs := range [][]types.Type{
					{typ, typ}, {types.T_any.ToType(), typ, typ}, {typ, typ, types.T_any.ToType()},
					{oid.ToType(), oid.ToType()},
				} {
					resolved, err := GetFunctionByName(t.Context(), name, inputs)
					require.NoError(t, err)
					require.Equal(t, inputs[1], resolved.GetReturnType())
				}
				for _, inputs := range [][]types.Type{
					{typ, types.New(oid, 3, 0)}, {types.New(oid, 3, 0), typ},
					{typ, types.T_any.ToType(), types.New(oid, 3, 0)},
					{typ, oid.ToType()}, {oid.ToType(), typ},
				} {
					_, err := GetFunctionByName(t.Context(), name, inputs)
					require.Error(t, err, "%v", inputs)
				}
			})
		}
	}
	for _, oid := range []types.T{types.T_array_float32, types.T_array_float64} {
		for _, width := range []int32{2, types.MaxArrayDimension} {
			resolved, err := GetFunctionByName(t.Context(), "sqrt", []types.Type{types.New(oid, width, 0)})
			require.NoError(t, err)
			require.Equal(t, types.New(types.T_array_float64, width, 0), resolved.GetReturnType())
		}
	}
}

func TestCoalesceVectorCommonType(t *testing.T) {
	oids := []types.T{types.T_array_float32, types.T_array_float64, types.T_array_bf16,
		types.T_array_float16, types.T_array_int8, types.T_array_uint8}
	for _, left := range oids {
		for _, right := range oids {
			t.Run(left.String()+"/"+right.String(), func(t *testing.T) {
				inputs := []types.Type{types.T_any.ToType(), types.New(left, 2, 0), types.New(right, 2, 0)}
				resolved, err := GetFunctionByName(t.Context(), "coalesce", inputs)
				switch {
				case left == right:
					require.NoError(t, err)
					require.Equal(t, inputs[1], resolved.GetReturnType())
				case left == types.T_array_float32 && right == types.T_array_float64,
					left == types.T_array_float64 && right == types.T_array_float32:
					require.NoError(t, err)
					require.Equal(t, types.New(types.T_array_float64, 2, 0), resolved.GetReturnType())
				default:
					require.Error(t, err)
				}
			})
		}
		for _, other := range []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()} {
			resolved, err := GetFunctionByName(t.Context(), "coalesce", []types.Type{other, types.New(left, 2, 0)})
			if other.Oid == types.T_varchar {
				require.NoError(t, err)
				require.Equal(t, types.New(left, 2, 0), resolved.GetReturnType())
			} else {
				require.Error(t, err)
			}
		}
	}
}

func TestVectorCastPayloadBounds(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float32, types.T_array_float64, types.T_array_bf16,
		types.T_array_float16, types.T_array_int8, types.T_array_uint8} {
		for _, blob := range []bool{false, true} {
			invalidCases := []string{"maximum"}
			if oid.ToType().GetArrayElementSize() > 1 {
				invalidCases = append(invalidCases, "alignment")
			}
			for _, invalid := range invalidCases {
				t.Run(fmt.Sprintf("%s/blob=%t/%s", oid, blob, invalid), func(t *testing.T) {
					proc := testutil.NewProcess(t)
					target := oid.ToType()
					source := target
					if blob {
						source = types.T_blob.ToType()
					}
					size := target.GetArrayElementSize()
					n := size*2 - 1
					message := "not aligned"
					if invalid == "maximum" {
						n = size * (types.MaxArrayDimension + 1)
						message = "exceeds maximum"
					}
					input := vector.NewVec(source)
					t.Cleanup(func() { input.Free(proc.Mp()) })
					require.NoError(t, vector.AppendBytes(input, make([]byte, n), false, proc.Mp()))
					targetVec := vector.NewVec(target)
					t.Cleanup(func() { targetVec.Free(proc.Mp()) })
					fn, err := GetFunctionByName(proc.Ctx, "cast", []types.Type{source, target})
					require.NoError(t, err)
					out, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input, targetVec}, 1)
					if out != nil {
						t.Cleanup(func() { out.Free(proc.Mp()) })
					}
					require.ErrorContains(t, err, message)
				})
			}
		}
	}
}

func TestVectorCastActualDimension(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float32, types.T_array_float64, types.T_array_bf16,
		types.T_array_float16, types.T_array_int8, types.T_array_uint8} {
		for _, blob := range []bool{false, true} {
			for _, width := range []int32{2, 3, types.MaxArrayDimension} {
				t.Run(fmt.Sprintf("%s/blob=%t/width=%d", oid, blob, width), func(t *testing.T) {
					proc := testutil.NewProcess(t)
					target := types.New(oid, width, 0)
					source := types.New(oid, 2, 0)
					if blob {
						source = types.T_blob.ToType()
					}
					input := vector.NewVec(source)
					t.Cleanup(func() { input.Free(proc.Mp()) })
					// 元数据标为 2 维，实际载荷为 3 维；验证不能相信源宽度。
					payload := make([]byte, 3*target.GetArrayElementSize())
					require.NoError(t, vector.AppendBytes(input, nil, true, proc.Mp()))
					require.NoError(t, vector.AppendBytes(input, payload, false, proc.Mp()))
					targetVec := vector.NewVec(target)
					t.Cleanup(func() { targetVec.Free(proc.Mp()) })
					fn, err := GetFunctionByName(proc.Ctx, "cast", []types.Type{source, target})
					require.NoError(t, err)
					out, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input, targetVec}, 2)
					if out != nil {
						t.Cleanup(func() { out.Free(proc.Mp()) })
					}
					if width == 2 {
						require.ErrorContains(t, err, "expected vector dimension 2 != actual dimension 3")
					} else {
						require.NoError(t, err)
						require.True(t, out.GetNulls().Contains(0))
						require.Equal(t, payload, out.GetBytesAt(1))
					}
				})
			}
		}
	}
}
