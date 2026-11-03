// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

// Same positive values and timing domain across narrow controls, wide kernels
// and cast pipelines. The Decimal reference remains the pre-optimization kernel,
// compiled in the same binary. This measures batch work, not whole SQL throughput.
func BenchmarkIntegerAddMulBatch(b *testing.B) {
	for _, multiply := range []bool{false, true} {
		for _, path := range []string{"narrow_control", "wide_signed_kernel", "wide_mixed_kernel", "mixed_decimal_reference", "signed_cast_pipeline", "mixed_cast_pipeline", "mixed_decimal_cast_pipeline"} {
			shapes := []string{"dense"}
			if path == "wide_mixed_kernel" || path == "mixed_decimal_reference" {
				shapes = append(shapes, "negative", "nullable", "masked")
			}
			for _, shape := range shapes {
				b.Run(fmt.Sprintf("multiply=%t/%s/%s", multiply, path, shape), func(b *testing.B) {
					const rows = 8192
					proc := testutil.NewProcess(b)
					defer proc.Free()
					a8 := vector.NewVec(types.T_int8.ToType())
					defer a8.Free(proc.Mp())
					b8 := vector.NewVec(types.T_int8.ToType())
					defer b8.Free(proc.Mp())
					u8 := vector.NewVec(types.T_uint8.ToType())
					defer u8.Free(proc.Mp())
					a64 := vector.NewVec(types.T_int64.ToType())
					defer a64.Free(proc.Mp())
					b64 := vector.NewVec(types.T_int64.ToType())
					defer b64.Free(proc.Mp())
					u64 := vector.NewVec(types.T_uint64.ToType())
					defer u64.Free(proc.Mp())
					active := make([]bool, rows)
					for i := 0; i < rows; i++ {
						a, v := int8(1+i%7), uint8(1+i%3)
						if shape == "negative" && i%2 == 0 {
							a = -a
							v = 10
							if multiply {
								v = 0
							}
						}
						isNull := shape == "nullable" && i%4 == 0
						active[i] = shape != "masked" || i%4 != 0
						for _, err := range []error{
							vector.AppendFixed(a8, a, isNull, proc.Mp()), vector.AppendFixed(b8, int8(v), false, proc.Mp()),
							vector.AppendFixed(u8, v, false, proc.Mp()), vector.AppendFixed(a64, int64(a), isNull, proc.Mp()),
							vector.AppendFixed(b64, int64(v), false, proc.Mp()), vector.AppendFixed(u64, uint64(v), false, proc.Mp()),
						} {
							require.NoError(b, err)
						}
					}
					var selected *FunctionSelectList
					if shape == "masked" {
						selected = &FunctionSelectList{AnyNull: true, SelectList: active}
					}
					mixed := path == "wide_mixed_kernel" || path == "mixed_decimal_reference" || path == "mixed_cast_pipeline" || path == "mixed_decimal_cast_pipeline"
					casts := path == "signed_cast_pipeline" || path == "mixed_cast_pipeline" || path == "mixed_decimal_cast_pipeline"
					resultType := types.T_int64.ToType()
					params := []*vector.Vector{a64, b64}
					fn := plusFn
					if multiply {
						fn = multiFn
					}
					if path == "narrow_control" {
						params = []*vector.Vector{a8, b8}
						resultType = types.T_int8.ToType()
					}
					if mixed {
						params = []*vector.Vector{a64, u64}
						resultType = types.T_uint64.ToType()
						fn = mixedUnsignedPlusFn
						if multiply {
							fn = mixedUnsignedMultiFn
						}
					}
					if path == "mixed_decimal_reference" || path == "mixed_decimal_cast_pipeline" {
						fn = mixedUnsignedDecimalReference(multiply)
					}
					result := vector.NewFunctionResultWrapper(resultType, proc.Mp())
					defer result.Free()
					leftResult := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
					defer leftResult.Free()
					rt := types.T_int64.ToType()
					if mixed {
						rt = types.T_uint64.ToType()
					}
					rightResult := vector.NewFunctionResultWrapper(rt, proc.Mp())
					defer rightResult.Free()
					leftTarget := vector.NewVec(types.T_int64.ToType())
					defer leftTarget.Free(proc.Mp())
					rightTarget := vector.NewVec(rt)
					defer rightTarget.Free(proc.Mp())
					rightSource := b8
					if mixed {
						rightSource = u8
					}
					castLeftParams := []*vector.Vector{a8, leftTarget}
					castRightParams := []*vector.Vector{rightSource, rightTarget}
					run := func() error {
						if casts {
							if err := leftResult.PreExtendAndReset(rows); err != nil {
								return err
							}
							if err := rightResult.PreExtendAndReset(rows); err != nil {
								return err
							}
							if err := NewCast(castLeftParams, leftResult, proc, rows, nil); err != nil {
								return err
							}
							if err := NewCast(castRightParams, rightResult, proc, rows, nil); err != nil {
								return err
							}
							params[0] = leftResult.GetResultVector()
							params[1] = rightResult.GetResultVector()
						}
						if err := result.PreExtendAndReset(rows); err != nil {
							return err
						}
						return fn(params, result, proc, rows, selected)
					}
					require.NoError(b, run())
					got := result.GetResultVector()
					require.Equal(b, rows, got.Length())
					// Independent numerical checks stay outside timing, including cancellation
					// and zero products of negative signed inputs.
					for row := 0; row < rows; row++ {
						wantNull := !active[row] || a64.IsNull(uint64(row))
						require.Equal(b, wantNull, got.IsNull(uint64(row)))
						if wantNull {
							continue
						}
						s, u := vector.GetFixedAtNoTypeCheck[int64](a64, row), vector.GetFixedAtNoTypeCheck[uint64](u64, row)
						want, ok := mixedUnsignedOracle(s, u, multiply)
						require.True(b, ok)
						var value uint64
						switch resultType.Oid {
						case types.T_int8:
							value = uint64(vector.GetFixedAtNoTypeCheck[int8](got, row))
						case types.T_int64:
							value = uint64(vector.GetFixedAtNoTypeCheck[int64](got, row))
						default:
							value = vector.GetFixedAtNoTypeCheck[uint64](got, row)
						}
						require.Equal(b, want, value)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if err := run(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
