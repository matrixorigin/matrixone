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

package simdkernels

import (
	"math"
	"math/rand"
	"testing"
)

func makeD64Pair(n int, seed int64) ([]uint64, []uint64) {
	r := rand.New(rand.NewSource(seed))
	a := make([]uint64, n)
	b := make([]uint64, n)
	for i := 0; i < n; i++ {
		a[i] = uint64(r.Int63() - r.Int63())
		// 25% chance b shares value with a (test equality path)
		if r.Intn(4) == 0 {
			b[i] = a[i]
		} else {
			b[i] = uint64(r.Int63() - r.Int63())
		}
	}
	return a, b
}

func edgeD64Pairs() (a, b []uint64) {
	vals := []int64{
		math.MinInt64, math.MinInt64 + 1, -1 << 32, -1, 0, 1, 1 << 32,
		math.MaxInt64 - 1, math.MaxInt64,
	}
	for _, x := range vals {
		for _, y := range vals {
			a = append(a, uint64(x))
			b = append(b, uint64(y))
		}
	}
	return
}

func TestD64Eq(t *testing.T) {
	cases := [][2][]uint64{}
	for _, n := range []int{0, 1, 3, 4, 7, 8, 15, 16, 17, 64, 1023} {
		a, b := makeD64Pair(n, int64(n)+1)
		cases = append(cases, [2][]uint64{a, b})
	}
	a, b := edgeD64Pairs()
	cases = append(cases, [2][]uint64{a, b})

	for ci, c := range cases {
		a, b := c[0], c[1]
		out := make([]bool, len(a))
		D64Eq(a, b, out)
		for i := range a {
			want := a[i] == b[i]
			if out[i] != want {
				t.Fatalf("case %d idx %d: D64Eq(%x,%x)=%v want %v", ci, i, a[i], b[i], out[i], want)
			}
		}
	}
}

func TestD64Lt(t *testing.T) {
	cases := [][2][]uint64{}
	for _, n := range []int{0, 1, 3, 4, 7, 8, 15, 16, 17, 64, 1023} {
		a, b := makeD64Pair(n, int64(n)+101)
		cases = append(cases, [2][]uint64{a, b})
	}
	a, b := edgeD64Pairs()
	cases = append(cases, [2][]uint64{a, b})

	for ci, c := range cases {
		a, b := c[0], c[1]
		out := make([]bool, len(a))
		D64Lt(a, b, out)
		for i := range a {
			want := int64(a[i]) < int64(b[i])
			if out[i] != want {
				t.Fatalf("case %d idx %d: D64Lt(%d,%d)=%v want %v",
					ci, i, int64(a[i]), int64(b[i]), out[i], want)
			}
		}
	}
}

func benchD64Cmp(b *testing.B, fn func(a, b []uint64, out []bool), n int) {
	x, y := makeD64Pair(n, 42)
	out := make([]bool, n)
	b.ResetTimer()
	b.SetBytes(int64(n) * 16)
	for i := 0; i < b.N; i++ {
		fn(x, y, out)
	}
}

func BenchmarkD64Eq_Scalar(b *testing.B) {
	for _, n := range []int{16, 64, 256, 1024, 4096} {
		b.Run(itoa(n), func(b *testing.B) { benchD64Cmp(b, scalarD64Eq, n) })
	}
}
func BenchmarkD64Eq_Dispatch(b *testing.B) {
	for _, n := range []int{16, 64, 256, 1024, 4096} {
		b.Run(itoa(n), func(b *testing.B) { benchD64Cmp(b, D64Eq, n) })
	}
}
func BenchmarkD64Lt_Scalar(b *testing.B) {
	for _, n := range []int{16, 64, 256, 1024, 4096} {
		b.Run(itoa(n), func(b *testing.B) { benchD64Cmp(b, scalarD64Lt, n) })
	}
}
func BenchmarkD64Lt_Dispatch(b *testing.B) {
	for _, n := range []int{16, 64, 256, 1024, 4096} {
		b.Run(itoa(n), func(b *testing.B) { benchD64Cmp(b, D64Lt, n) })
	}
}
