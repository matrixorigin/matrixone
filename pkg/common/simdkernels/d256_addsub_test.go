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

//go:build goexperiment.simd && amd64

package simdkernels

import (
	"math"
	"math/rand/v2"
	"testing"

	"golang.org/x/sys/cpu"
)

// All slice lengths below count Decimal256 elements (each backed by 4 uint64).

type d256UncheckedImpl struct {
	name string
	fn   func(a, b, r []uint64)
}

type d256CheckedImpl struct {
	name string
	fn   func(a, b, r []uint64) int
}

func d256Sizes() []int {
	return []int{0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 35, 63, 64, 127, 128, 1023, 2048}
}

func makeRandD256(n int, seed uint64) []uint64 {
	rng := rand.New(rand.NewPCG(seed, seed^0xDEADBEEFCAFEBABE))
	out := make([]uint64, 4*n)
	for i := range out {
		out[i] = rng.Uint64()
	}
	return out
}

// makeRand256SmallSigned clears the high bit of the top word so 256-bit
// add/sub of any two such values cannot overflow signed.
func makeRand256SmallSigned(n int, seed uint64) []uint64 {
	out := makeRandD256(n, seed)
	for i := 3; i < len(out); i += 4 {
		out[i] &= 0x3FFFFFFFFFFFFFFF
	}
	return out
}

func TestD256AddVariants(t *testing.T) {
	impls := []d256UncheckedImpl{
		{"scalar", scalarD256AddUnchecked},
		{"avx2", avx2D256AddUnchecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d256UncheckedImpl{"avx512", avx512D256AddUnchecked})
	}
	for _, n := range d256Sizes() {
		a := makeRandD256(n, uint64(n)*7+1)
		b := makeRandD256(n, uint64(n)*11+3)
		want := make([]uint64, 4*n)
		scalarD256AddUnchecked(a, b, want)
		for _, impl := range impls {
			got := make([]uint64, 4*n)
			impl.fn(a, b, got)
			for i := 0; i < 4*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d slot=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}
}

func TestD256SubVariants(t *testing.T) {
	impls := []d256UncheckedImpl{
		{"scalar", scalarD256SubUnchecked},
		{"avx2", avx2D256SubUnchecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d256UncheckedImpl{"avx512", avx512D256SubUnchecked})
	}
	for _, n := range d256Sizes() {
		a := makeRandD256(n, uint64(n)*13+5)
		b := makeRandD256(n, uint64(n)*17+9)
		want := make([]uint64, 4*n)
		scalarD256SubUnchecked(a, b, want)
		for _, impl := range impls {
			got := make([]uint64, 4*n)
			impl.fn(a, b, got)
			for i := 0; i < 4*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d slot=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}
}

func TestD256AddCheckedVariants(t *testing.T) {
	impls := []d256CheckedImpl{
		{"scalar", scalarD256AddChecked},
		{"avx2", avx2D256AddChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d256CheckedImpl{"avx512", avx512D256AddChecked})
	}

	// 1) No-overflow random inputs.
	for _, n := range d256Sizes() {
		a := makeRand256SmallSigned(n, uint64(n)*19+7)
		b := makeRand256SmallSigned(n, uint64(n)*23+11)
		want := make([]uint64, 4*n)
		if got := scalarD256AddChecked(a, b, want); got != -1 {
			t.Fatalf("setup: scalar overflow at %d for masked input n=%d", got, n)
		}
		for _, impl := range impls {
			got := make([]uint64, 4*n)
			if idx := impl.fn(a, b, got); idx != -1 {
				t.Fatalf("%s n=%d: spurious overflow at %d", impl.name, n, idx)
			}
			for i := 0; i < 4*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d slot=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}

	// 2) Inject MaxInt256 + 1 overflow at varying positions.
	for _, n := range []int{4, 8, 9, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 3, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			a := make([]uint64, 4*n)
			b := make([]uint64, 4*n)
			j := pos << 2
			a[j] = math.MaxUint64
			a[j+1] = math.MaxUint64
			a[j+2] = math.MaxUint64
			a[j+3] = uint64(math.MaxInt64) // a = MaxInt256
			b[j] = 1
			// b = 1
			for _, impl := range impls {
				got := make([]uint64, 4*n)
				idx := impl.fn(a, b, got)
				if idx != pos {
					t.Fatalf("%s n=%d inject pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}

	// 3) Carry propagation cascading w0→w1→w2→w3.
	for _, n := range []int{4, 8, 16, 17, 33} {
		a := make([]uint64, 4*n)
		b := make([]uint64, 4*n)
		for i := 0; i < n; i++ {
			j := i << 2
			a[j] = math.MaxUint64
			a[j+1] = math.MaxUint64
			a[j+2] = math.MaxUint64
			a[j+3] = uint64(i) // small positive top
			b[j] = 1
		}
		want := make([]uint64, 4*n)
		scalarD256AddChecked(a, b, want)
		for _, impl := range impls {
			got := make([]uint64, 4*n)
			if idx := impl.fn(a, b, got); idx != -1 {
				t.Fatalf("%s carry n=%d: spurious overflow at %d", impl.name, n, idx)
			}
			for i := 0; i < 4*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s carry n=%d slot=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}
}

func TestD256SubCheckedVariants(t *testing.T) {
	impls := []d256CheckedImpl{
		{"scalar", scalarD256SubChecked},
		{"avx2", avx2D256SubChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d256CheckedImpl{"avx512", avx512D256SubChecked})
	}

	for _, n := range d256Sizes() {
		a := makeRand256SmallSigned(n, uint64(n)*29+13)
		b := makeRand256SmallSigned(n, uint64(n)*31+17)
		want := make([]uint64, 4*n)
		if got := scalarD256SubChecked(a, b, want); got != -1 {
			t.Fatalf("setup: scalar overflow at %d for n=%d", got, n)
		}
		for _, impl := range impls {
			got := make([]uint64, 4*n)
			if idx := impl.fn(a, b, got); idx != -1 {
				t.Fatalf("%s n=%d: spurious overflow at %d", impl.name, n, idx)
			}
			for i := 0; i < 4*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d slot=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}

	// Inject MinInt256 - 1 overflow at varying positions.
	for _, n := range []int{4, 8, 9, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 3, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			a := make([]uint64, 4*n)
			b := make([]uint64, 4*n)
			j := pos << 2
			// a = MinInt256
			a[j+3] = 1 << 63
			b[j] = 1 // b = 1
			for _, impl := range impls {
				got := make([]uint64, 4*n)
				idx := impl.fn(a, b, got)
				if idx != pos {
					t.Fatalf("%s n=%d inject pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}

	// Borrow propagation cascading w0→w1→w2→w3.
	for _, n := range []int{4, 8, 16, 17, 33} {
		a := make([]uint64, 4*n)
		b := make([]uint64, 4*n)
		for i := 0; i < n; i++ {
			j := i << 2
			a[j+3] = uint64(i + 10) // safely positive after borrow
			b[j] = 1
		}
		want := make([]uint64, 4*n)
		scalarD256SubChecked(a, b, want)
		for _, impl := range impls {
			got := make([]uint64, 4*n)
			if idx := impl.fn(a, b, got); idx != -1 {
				t.Fatalf("%s borrow n=%d: spurious overflow at %d", impl.name, n, idx)
			}
			for i := 0; i < 4*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s borrow n=%d slot=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

var d256BenchSizes = []int{16, 64, 256, 1024, 4096}

func benchD256Unchecked(b *testing.B, fn func(a, bb, r []uint64), n int) {
	a := makeRandD256(n, 1)
	bb := makeRandD256(n, 2)
	r := make([]uint64, 4*n)
	b.SetBytes(int64(n) * 32 * 3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(a, bb, r)
	}
}

func benchD256Checked(b *testing.B, fn func(a, bb, r []uint64) int, n int) {
	a := makeRand256SmallSigned(n, 1)
	bb := makeRand256SmallSigned(n, 2)
	r := make([]uint64, 4*n)
	b.SetBytes(int64(n) * 32 * 3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = fn(a, bb, r)
	}
}

func BenchmarkD256AddUnchecked(b *testing.B) {
	for _, n := range d256BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD256Unchecked(b, scalarD256AddUnchecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD256Unchecked(b, avx2D256AddUnchecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD256Unchecked(b, avx512D256AddUnchecked, n) })
		}
	}
}

func BenchmarkD256SubUnchecked(b *testing.B) {
	for _, n := range d256BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD256Unchecked(b, scalarD256SubUnchecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD256Unchecked(b, avx2D256SubUnchecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD256Unchecked(b, avx512D256SubUnchecked, n) })
		}
	}
}

func BenchmarkD256AddChecked(b *testing.B) {
	for _, n := range d256BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD256Checked(b, scalarD256AddChecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD256Checked(b, avx2D256AddChecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD256Checked(b, avx512D256AddChecked, n) })
		}
	}
}

func BenchmarkD256SubChecked(b *testing.B) {
	for _, n := range d256BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD256Checked(b, scalarD256SubChecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD256Checked(b, avx2D256SubChecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD256Checked(b, avx512D256SubChecked, n) })
		}
	}
}

// ---------------------------------------------------------------------------
// Scalar-broadcast tests (D256 = 4 uint64/elem; scalar = (s0, s1, s2, s3)).
// ---------------------------------------------------------------------------

type d256ScalarVUImpl struct {
	name string
	fn   func(s0, s1, s2, s3 uint64, v, r []uint64)
}

type d256ScalarVCImpl struct {
	name string
	fn   func(s0, s1, s2, s3 uint64, v, r []uint64) int
}

type d256VScalarUImpl struct {
	name string
	fn   func(v []uint64, s0, s1, s2, s3 uint64, r []uint64)
}

type d256VScalarCImpl struct {
	name string
	fn   func(v []uint64, s0, s1, s2, s3 uint64, r []uint64) int
}

func d256Scalars() []struct{ s0, s1, s2, s3 uint64 } {
	return []struct{ s0, s1, s2, s3 uint64 }{
		{0, 0, 0, 0},
		{1, 0, 0, 0},
		{math.MaxUint64, 0, 0, 0},
		{0, 0, 0, 1},
		{0xDEADBEEFCAFEBABE, 0x123456789ABCDEF0, 0xFEEDFACEDEADBEEF, 0x0123456789ABCDEF},
		{math.MaxUint64, math.MaxUint64, math.MaxUint64, uint64(math.MaxInt64)},
		{0, 0, 0, 1 << 63},
	}
}

func TestD256AddScalarVariants(t *testing.T) {
	impls := []d256ScalarVUImpl{
		{"scalar", scalarD256AddScalarUnchecked},
		{"avx2", avx2D256AddScalarUnchecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d256ScalarVUImpl{"avx512", avx512D256AddScalarUnchecked})
	}
	for _, n := range d256Sizes() {
		v := makeRandD256(n, uint64(n)*37+1)
		for si, s := range d256Scalars() {
			want := make([]uint64, 4*n)
			scalarD256AddScalarUnchecked(s.s0, s.s1, s.s2, s.s3, v, want)
			for _, impl := range impls {
				got := make([]uint64, 4*n)
				impl.fn(s.s0, s.s1, s.s2, s.s3, v, got)
				for i := 0; i < 4*n; i++ {
					if got[i] != want[i] {
						t.Fatalf("%s n=%d scalar#%d slot=%d: got %x want %x", impl.name, n, si, i, got[i], want[i])
					}
				}
			}
		}
	}
}

func TestD256SubScalarVariants(t *testing.T) {
	impls := []d256VScalarUImpl{
		{"scalar", scalarD256SubScalarUnchecked},
		{"avx2", avx2D256SubScalarUnchecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d256VScalarUImpl{"avx512", avx512D256SubScalarUnchecked})
	}
	for _, n := range d256Sizes() {
		v := makeRandD256(n, uint64(n)*41+3)
		for si, s := range d256Scalars() {
			want := make([]uint64, 4*n)
			scalarD256SubScalarUnchecked(v, s.s0, s.s1, s.s2, s.s3, want)
			for _, impl := range impls {
				got := make([]uint64, 4*n)
				impl.fn(v, s.s0, s.s1, s.s2, s.s3, got)
				for i := 0; i < 4*n; i++ {
					if got[i] != want[i] {
						t.Fatalf("%s n=%d scalar#%d slot=%d: got %x want %x", impl.name, n, si, i, got[i], want[i])
					}
				}
			}
		}
	}
}

func TestD256ScalarSubVariants(t *testing.T) {
	impls := []d256ScalarVUImpl{
		{"scalar", scalarD256ScalarSubUnchecked},
		{"avx2", avx2D256ScalarSubUnchecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d256ScalarVUImpl{"avx512", avx512D256ScalarSubUnchecked})
	}
	for _, n := range d256Sizes() {
		v := makeRandD256(n, uint64(n)*43+5)
		for si, s := range d256Scalars() {
			want := make([]uint64, 4*n)
			scalarD256ScalarSubUnchecked(s.s0, s.s1, s.s2, s.s3, v, want)
			for _, impl := range impls {
				got := make([]uint64, 4*n)
				impl.fn(s.s0, s.s1, s.s2, s.s3, v, got)
				for i := 0; i < 4*n; i++ {
					if got[i] != want[i] {
						t.Fatalf("%s n=%d scalar#%d slot=%d: got %x want %x", impl.name, n, si, i, got[i], want[i])
					}
				}
			}
		}
	}
}

func TestD256AddScalarCheckedVariants(t *testing.T) {
	impls := []d256ScalarVCImpl{
		{"scalar", scalarD256AddScalarChecked},
		{"avx2", avx2D256AddScalarChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d256ScalarVCImpl{"avx512", avx512D256AddScalarChecked})
	}

	smallScalars := []struct{ s0, s1, s2, s3 uint64 }{
		{0, 0, 0, 0},
		{1, 0, 0, 0},
		{0xDEADBEEFCAFEBABE, 0x123456789ABCDEF0, 0xFEEDFACEDEADBEEF, 0x0123456789ABCDEF},
	}
	for _, n := range d256Sizes() {
		v := makeRand256SmallSigned(n, uint64(n)*47+7)
		for si, s := range smallScalars {
			want := make([]uint64, 4*n)
			if got := scalarD256AddScalarChecked(s.s0, s.s1, s.s2, s.s3, v, want); got != -1 {
				t.Fatalf("setup overflow at %d for n=%d scalar#%d", got, n, si)
			}
			for _, impl := range impls {
				got := make([]uint64, 4*n)
				if idx := impl.fn(s.s0, s.s1, s.s2, s.s3, v, got); idx != -1 {
					t.Fatalf("%s n=%d scalar#%d: spurious overflow at %d", impl.name, n, si, idx)
				}
				for i := 0; i < 4*n; i++ {
					if got[i] != want[i] {
						t.Fatalf("%s n=%d scalar#%d slot=%d: got %x want %x", impl.name, n, si, i, got[i], want[i])
					}
				}
			}
		}
	}

	// Inject overflow: scalar = 1, v[pos] = MaxInt256.
	for _, n := range []int{4, 8, 9, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 3, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			v := make([]uint64, 4*n)
			j := pos << 2
			v[j] = math.MaxUint64
			v[j+1] = math.MaxUint64
			v[j+2] = math.MaxUint64
			v[j+3] = uint64(math.MaxInt64)
			for _, impl := range impls {
				got := make([]uint64, 4*n)
				idx := impl.fn(1, 0, 0, 0, v, got)
				if idx != pos {
					t.Fatalf("%s n=%d pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}
}

func TestD256SubScalarCheckedVariants(t *testing.T) {
	impls := []d256VScalarCImpl{
		{"scalar", scalarD256SubScalarChecked},
		{"avx2", avx2D256SubScalarChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d256VScalarCImpl{"avx512", avx512D256SubScalarChecked})
	}

	smallScalars := []struct{ s0, s1, s2, s3 uint64 }{
		{0, 0, 0, 0},
		{1, 0, 0, 0},
		{0xDEADBEEFCAFEBABE, 0x123456789ABCDEF0, 0xFEEDFACEDEADBEEF, 0x0123456789ABCDEF},
	}
	for _, n := range d256Sizes() {
		v := makeRand256SmallSigned(n, uint64(n)*53+11)
		for si, s := range smallScalars {
			want := make([]uint64, 4*n)
			if got := scalarD256SubScalarChecked(v, s.s0, s.s1, s.s2, s.s3, want); got != -1 {
				t.Fatalf("setup overflow at %d for n=%d scalar#%d", got, n, si)
			}
			for _, impl := range impls {
				got := make([]uint64, 4*n)
				if idx := impl.fn(v, s.s0, s.s1, s.s2, s.s3, got); idx != -1 {
					t.Fatalf("%s n=%d scalar#%d: spurious overflow at %d", impl.name, n, si, idx)
				}
				for i := 0; i < 4*n; i++ {
					if got[i] != want[i] {
						t.Fatalf("%s n=%d scalar#%d slot=%d: got %x want %x", impl.name, n, si, i, got[i], want[i])
					}
				}
			}
		}
	}

	// Inject: v[pos] = MinInt256, scalar = 1.
	for _, n := range []int{4, 8, 9, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 3, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			v := make([]uint64, 4*n)
			j := pos << 2
			v[j+3] = 1 << 63
			for _, impl := range impls {
				got := make([]uint64, 4*n)
				idx := impl.fn(v, 1, 0, 0, 0, got)
				if idx != pos {
					t.Fatalf("%s n=%d pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}
}

func TestD256ScalarSubCheckedVariants(t *testing.T) {
	impls := []d256ScalarVCImpl{
		{"scalar", scalarD256ScalarSubChecked},
		{"avx2", avx2D256ScalarSubChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d256ScalarVCImpl{"avx512", avx512D256ScalarSubChecked})
	}

	smallScalars := []struct{ s0, s1, s2, s3 uint64 }{
		{0, 0, 0, 0},
		{1, 0, 0, 0},
		{0xDEADBEEFCAFEBABE, 0x123456789ABCDEF0, 0xFEEDFACEDEADBEEF, 0x0123456789ABCDEF},
	}
	for _, n := range d256Sizes() {
		v := makeRand256SmallSigned(n, uint64(n)*59+13)
		for si, s := range smallScalars {
			want := make([]uint64, 4*n)
			if got := scalarD256ScalarSubChecked(s.s0, s.s1, s.s2, s.s3, v, want); got != -1 {
				t.Fatalf("setup overflow at %d for n=%d scalar#%d", got, n, si)
			}
			for _, impl := range impls {
				got := make([]uint64, 4*n)
				if idx := impl.fn(s.s0, s.s1, s.s2, s.s3, v, got); idx != -1 {
					t.Fatalf("%s n=%d scalar#%d: spurious overflow at %d", impl.name, n, si, idx)
				}
				for i := 0; i < 4*n; i++ {
					if got[i] != want[i] {
						t.Fatalf("%s n=%d scalar#%d slot=%d: got %x want %x", impl.name, n, si, i, got[i], want[i])
					}
				}
			}
		}
	}

	// Inject: scalar = MinInt256, v[pos] = 1 ⇒ MinInt256-1 overflows.
	for _, n := range []int{4, 8, 9, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 3, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			v := make([]uint64, 4*n)
			j := pos << 2
			v[j] = 1
			for _, impl := range impls {
				got := make([]uint64, 4*n)
				idx := impl.fn(0, 0, 0, 1<<63, v, got)
				if idx != pos {
					t.Fatalf("%s n=%d pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}
}

func benchD256AddScalarU(b *testing.B, fn func(s0, s1, s2, s3 uint64, v, r []uint64), n int) {
	v := makeRandD256(n, 1)
	r := make([]uint64, 4*n)
	b.SetBytes(int64(n) * 32 * 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(0xDEADBEEFCAFEBABE, 0x123456789ABCDEF0, 0xFEEDFACEDEADBEEF, 0x0123456789ABCDEF, v, r)
	}
}

func benchD256SubScalarU(b *testing.B, fn func(v []uint64, s0, s1, s2, s3 uint64, r []uint64), n int) {
	v := makeRandD256(n, 1)
	r := make([]uint64, 4*n)
	b.SetBytes(int64(n) * 32 * 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(v, 0xDEADBEEFCAFEBABE, 0x123456789ABCDEF0, 0xFEEDFACEDEADBEEF, 0x0123456789ABCDEF, r)
	}
}

func BenchmarkD256AddScalarUnchecked(b *testing.B) {
	for _, n := range d256BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD256AddScalarU(b, scalarD256AddScalarUnchecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD256AddScalarU(b, avx2D256AddScalarUnchecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD256AddScalarU(b, avx512D256AddScalarUnchecked, n) })
		}
	}
}

func BenchmarkD256SubScalarUnchecked(b *testing.B) {
	for _, n := range d256BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD256SubScalarU(b, scalarD256SubScalarUnchecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD256SubScalarU(b, avx2D256SubScalarUnchecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD256SubScalarU(b, avx512D256SubScalarUnchecked, n) })
		}
	}
}

func BenchmarkD256ScalarSubUnchecked(b *testing.B) {
	for _, n := range d256BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD256AddScalarU(b, scalarD256ScalarSubUnchecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD256AddScalarU(b, avx2D256ScalarSubUnchecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD256AddScalarU(b, avx512D256ScalarSubUnchecked, n) })
		}
	}
}
