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
	"context"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Time full, nonconstant batches through the real cast entry points. NULL is a
// separate control. Setup, independent rational verification and cleanup are
// outside timing; ns/op includes result reset and conversion of all 8192 rows.
func BenchmarkFloatDecimalVectors(b *testing.B) {
	const rows = 8192
	for _, tc := range []struct {
		name   string
		values []float64
		scale  int32
	}{
		{"ordinary", []float64{12345.6789, 12.3456789}, 6},
		{"integral", []float64{12345, 12346}, 6},
		{"tie", []float64{1.005, 1.015}, 2},
		{"near_tie", []float64{math.Nextafter(1.005, 0), math.Nextafter(1.015, 0)}, 2},
		{"tiny", []float64{1e-20, 2e-20}, 6},
		{"large", []float64{123456789012.34567, 234567890123.45678}, 6},
		{"high_scale", []float64{0.12345678901234568, 0.23456789012345678}, 18},
		{"scale19", []float64{1e-20, 2e-20}, 19},
		{"scale20", []float64{1e-20, 2e-20}, 20},
		{"scale38", []float64{1e-20, 2e-20}, 38},
		{"null", []float64{0}, 6},
	} {
		for _, oid := range []types.T{types.T_decimal64, types.T_decimal128} {
			if oid == types.T_decimal64 && tc.scale > 18 {
				continue
			}
			for _, explicit := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/%s/explicit=%t", tc.name, oid, explicit), func(b *testing.B) {
					mp := mpool.MustNewZeroNoFixed()
					proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
					defer func() {
						proc.Free()
						defer mpool.DeleteMPool(mp)
						require.Zero(b, mp.CurrNB())
						require.Zero(b, mp.OnHeapCurrNB())
					}()
					width := int32(18)
					if oid == types.T_decimal128 {
						width = 38
					}
					target := types.New(oid, width, tc.scale)
					from := vector.NewVec(types.T_float64.ToType())
					defer from.Free(mp)
					for row := 0; row < rows; row++ {
						x := tc.values[(row/2)%len(tc.values)]
						if row%2 != 0 {
							x = -x
						}
						require.NoError(b, vector.AppendFixed(from, x, tc.name == "null", mp))
					}
					to := vector.NewConstNull(target, rows, mp)
					defer to.Free(mp)
					result := vector.NewFunctionResultWrapper(target, mp)
					defer result.Free()
					parameters := []*vector.Vector{from, to}
					cast := NewCast
					if explicit {
						cast = NewExplicitCast
					}
					run := func() error {
						if err := result.PreExtendAndReset(rows); err != nil {
							return err
						}
						return cast(parameters, result, proc, rows, nil)
					}
					require.NoError(b, run())
					verify := func() {
						got := result.GetResultVector()
						require.Equal(b, rows, got.Length())
						for row := 0; row < rows; row++ {
							require.Equal(b, tc.name == "null", got.IsNull(uint64(row)))
							if tc.name == "null" {
								continue
							}
							x := vector.GetFixedAtWithTypeCheck[float64](from, row)
							r, ok := new(big.Rat).SetString(strconv.FormatFloat(math.Abs(x), 'e', -1, 64))
							require.True(b, ok)
							n := new(big.Int).Mul(r.Num(), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(tc.scale)), nil))
							q, rem := new(big.Int), new(big.Int)
							q.QuoRem(n, r.Denom(), rem)
							if rem.Lsh(rem, 1).Cmp(r.Denom()) >= 0 {
								q.Add(q, big.NewInt(1))
							}
							want := types.Decimal128{B0_63: q.Uint64(), B64_127: new(big.Int).Rsh(q, 64).Uint64()}
							if x < 0 {
								want = want.Minus()
							}
							if oid == types.T_decimal64 {
								require.Equal(b, types.Decimal64(want.B0_63), vector.GetFixedAtWithTypeCheck[types.Decimal64](got, row))
							} else {
								require.Equal(b, want, vector.GetFixedAtWithTypeCheck[types.Decimal128](got, row))
							}
						}
					}
					verify()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if err := run(); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/row")
					verify()
				})
			}
		}
	}
}
