// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestInnerProductMathematicalSemantics(t *testing.T) {
	encode := func(typ types.T, values []float32) []byte {
		switch typ {
		case types.T_array_float32:
			return types.ArrayToBytes(values)
		case types.T_array_float64:
			out := make([]float64, len(values))
			for i, v := range values {
				out[i] = float64(v)
			}
			return types.ArrayToBytes(out)
		case types.T_array_bf16:
			return types.ArrayToBytes(types.Float32ToBF16Slice(values))
		case types.T_array_float16:
			return types.ArrayToBytes(types.Float32ToFloat16Slice(values))
		case types.T_array_int8:
			out := make([]int8, len(values))
			for i, v := range values {
				out[i] = int8(v)
			}
			return types.ArrayToBytes(out)
		case types.T_array_uint8:
			out := make([]uint8, len(values))
			for i, v := range values {
				out[i] = uint8(v)
			}
			return types.ArrayToBytes(out)
		default:
			panic("unsupported test vector type")
		}
	}
	for _, typ := range []types.T{types.T_array_float32, types.T_array_float64, types.T_array_bf16,
		types.T_array_float16, types.T_array_int8, types.T_array_uint8} {
		for _, shape := range []string{"constants", "batch", "reversed_batch", "columns", "null_column", "null_constant", "dimension_error", "negative"} {
			if shape == "negative" && typ == types.T_array_uint8 {
				continue
			}
			t.Run(typ.String()+"/"+shape, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				mp := proc.Mp()
				fn, err := GetFunctionByName(proc.Ctx, "inner_product", []types.Type{typ.ToType(), typ.ToType()})
				require.NoError(t, err)
				constVec := func(values []float32) *vector.Vector {
					v, err := vector.NewConstBytes(typ.ToType(), encode(typ, values), 3, mp)
					require.NoError(t, err)
					t.Cleanup(func() { v.Free(mp) })
					return v
				}
				column := func(rows [][]float32, nullLast bool) *vector.Vector {
					v := vector.NewVec(typ.ToType())
					t.Cleanup(func() { v.Free(mp) })
					for i, row := range rows {
						require.NoError(t, vector.AppendBytes(v, encode(typ, row), nullLast && i == 2, mp))
					}
					return v
				}
				query := []float32{1, 2, 3}
				rows := [][]float32{{4, 5, 6}, {1, 2, 3}, {0, 0, 0}}
				left := constVec(query)
				right := column(rows, shape == "null_column")
				want := []float64{32, 14, 0}
				switch shape {
				case "constants":
					right = constVec(rows[0])
					want = []float64{32, 32, 32}
				case "reversed_batch":
					left, right = right, left
				case "columns":
					left = column([][]float32{query, query, query}, false)
				case "null_constant":
					left = vector.NewConstNull(typ.ToType(), 3, mp)
					t.Cleanup(func() { left.Free(mp) })
				case "dimension_error":
					right = constVec([]float32{4, 5})
				case "negative":
					right = constVec([]float32{-4, -5, -6})
					want = []float64{-32, -32, -32}
				}
				out, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{left, right}, 3)
				if out != nil {
					t.Cleanup(func() { out.Free(mp) })
				}
				if shape == "dimension_error" {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				for row, expected := range want {
					if shape == "null_constant" || shape == "null_column" && row == 2 {
						require.True(t, out.IsNull(uint64(row)))
					} else {
						require.False(t, out.IsNull(uint64(row)))
						require.Equal(t, expected, vector.GetFixedAtNoTypeCheck[float64](out, row))
					}
				}
			})
		}
	}
}
