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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Uses only pre-existing owners so the identical fixture compiles on clean main.
// Setup stays outside timing; the loop measures dispatch and arithmetic work.
func BenchmarkDecimalNativeDispatch(b *testing.B) {
	for _, family := range []string{"fused", "multiply", "add64", "sub64", "add128", "sub128", "cast"} {
		for _, n := range []int{1, 15, 31, 32, 33, 64, 65, 128, 129, 255, 256, 257, 8192} {
			shapes := []string{"flat"}
			if family == "fused" || family == "multiply" {
				shapes = append(shapes, "wide_first", "wide_late")
			}
			shapes = append(shapes, "null")
			if family == "cast" {
				shapes = append(shapes, "const")
			}
			if family != "fused" && family != "cast" {
				shapes = append(shapes, "left_const", "right_const")
			}
			if family == "fused" && n == 1 {
				shapes = append(shapes, "left_const", "right_const")
			}
			for _, shape := range shapes {
				b.Run(fmt.Sprintf("%s/n%d/%s", family, n, shape), func(b *testing.B) {
					a, c := make([]types.Decimal64, n), make([]types.Decimal64, n)
					aa, cc, out := make([]types.Decimal128, n), make([]types.Decimal128, n), make([]types.Decimal128, n)
					out64 := make([]types.Decimal64, n)
					np := new(nulls.Nulls)
					for i := range a {
						a[i] = 123456
						c[i] = 5
						aa[i] = types.Decimal128FromInt64(123456)
						cc[i] = types.Decimal128FromInt64(5)
					}
					if shape == "wide_first" {
						a[0] = types.Decimal64(1 << 40)
					}
					if shape == "wide_late" {
						a[n-1] = types.Decimal64(1 << 40)
					}
					if shape == "null" {
						np.Add(uint64(n / 2))
					}
					if shape == "left_const" {
						a = a[:1]
						aa = aa[:1]
					}
					if shape == "right_const" {
						c = c[:1]
						cc = cc[:1]
					}
					var run func()
					switch family {
					case "fused":
						mp := mpool.MustNewZero()
						b.Cleanup(func() { mpool.DeleteMPool(mp) })
						x, y, r := vector.NewVec(types.New(types.T_decimal64, 15, 2)), vector.NewVec(types.New(types.T_decimal64, 15, 2)), vector.NewVec(types.New(types.T_decimal128, 38, 4))
						b.Cleanup(func() { x.Free(mp); y.Free(mp); r.Free(mp) })
						for i := 0; i < n; i++ {
							if err := vector.AppendFixed(x, a[i], shape == "null" && i == n/2, mp); err != nil {
								b.Fatal(err)
							}
							if err := vector.AppendFixed(y, c[i], false, mp); err != nil {
								b.Fatal(err)
							}
						}
						if shape == "left_const" || shape == "right_const" {
							v, value := x, a[0]
							if shape == "right_const" {
								v, value = y, c[0]
							}
							cv, err := vector.NewConstFixed(types.New(types.T_decimal64, 15, 2), value, n, mp)
							if err != nil {
								b.Fatal(err)
							}
							v.Free(mp)
							if shape == "left_const" {
								x = cv
							} else {
								y = cv
							}
						}
						if err := r.PreExtend(n, mp); err != nil {
							b.Fatal(err)
						}
						r.SetLength(n)
						run = func() { TryDecimalMulSub64(x, y, 100, r) }
					case "multiply":
						run = func() {
							if err := d64Mul(a, c, out, 2, 2, np); err != nil {
								b.Fatal(err)
							}
						}
					case "add64":
						run = func() {
							if err := d64Add(a, c, out64, 2, 2, np); err != nil {
								b.Fatal(err)
							}
						}
					case "sub64":
						run = func() {
							if err := d64Sub(a, c, out64, 2, 2, np); err != nil {
								b.Fatal(err)
							}
						}
					case "add128":
						run = func() { d128AddSameScale(aa, cc, out, np) }
					case "sub128":
						run = func() { d128SubSameScale(aa, cc, out, np) }
					case "cast":
						mp := mpool.MustNewZero()
						b.Cleanup(func() { mpool.DeleteMPool(mp) })
						var src *vector.Vector
						if shape == "const" {
							var err error
							src, err = vector.NewConstFixed(types.New(types.T_decimal64, 15, 2), a[0], n, mp)
							if err != nil {
								b.Fatal(err)
							}
							b.Cleanup(func() { src.Free(mp) })
						} else {
							src = vector.NewVec(types.New(types.T_decimal64, 15, 2))
							b.Cleanup(func() { src.Free(mp) })
							if err := vector.AppendFixedList(src, a, nil, mp); err != nil {
								b.Fatal(err)
							}
							if shape == "null" {
								src.GetNulls().Add(uint64(n / 2))
							}
						}
						result := vector.MustFunctionResult[types.Decimal128](vector.NewFunctionResultWrapper(types.New(types.T_decimal128, 38, 2), mp))
						b.Cleanup(result.Free)
						if err := result.PreExtendAndReset(n); err != nil {
							b.Fatal(err)
						}
						from := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](src)
						run = func() {
							if err := decimal64ToDecimal128Array(from, result, n, nil); err != nil {
								b.Fatal(err)
							}
						}
					}
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						run()
					}
				})
			}
		}
	}
}
