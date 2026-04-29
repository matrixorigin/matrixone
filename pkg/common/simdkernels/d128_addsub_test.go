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

// All slice lengths below count Decimal128 elements (each backed by 2 uint64).

type d128UncheckedImpl struct {
	name string
	fn   func(a, b, r []uint64)
}

type d128CheckedImpl struct {
	name string
	fn   func(a, b, r []uint64) int
}

func d128Sizes() []int {
	return []int{0, 1, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 35, 63, 64, 127, 128, 1023, 4096}
}

func makeRandD128(n int, seed uint64) []uint64 {
	rng := rand.New(rand.NewPCG(seed, seed^0xDEADBEEFCAFEBABE))
	out := make([]uint64, 2*n)
	for i := range out {
		out[i] = rng.Uint64()
	}
	return out
}

// makeRand128SmallSigned produces N elements whose high words have their
// MSB cleared, so 128-bit add/sub of any two such values cannot overflow
// (both signs are non-negative; 2^126 + 2^126 ≪ 2^127).
func makeRand128SmallSigned(n int, seed uint64) []uint64 {
	out := makeRandD128(n, seed)
	for i := 1; i < len(out); i += 2 {
		out[i] &= 0x3FFFFFFFFFFFFFFF
	}
	return out
}

func TestD128AddVariants(t *testing.T) {
	impls := []d128UncheckedImpl{
		{"scalar", scalarD128AddUnchecked},
		{"avx2", avx2D128AddUnchecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d128UncheckedImpl{"avx512", avx512D128AddUnchecked})
	}
	for _, n := range d128Sizes() {
		a := makeRandD128(n, uint64(n)*7+1)
		b := makeRandD128(n, uint64(n)*11+3)
		want := make([]uint64, 2*n)
		scalarD128AddUnchecked(a, b, want)
		for _, impl := range impls {
			got := make([]uint64, 2*n)
			impl.fn(a, b, got)
			for i := 0; i < 2*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d slot=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}
}

func TestD128SubVariants(t *testing.T) {
	impls := []d128UncheckedImpl{
		{"scalar", scalarD128SubUnchecked},
		{"avx2", avx2D128SubUnchecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d128UncheckedImpl{"avx512", avx512D128SubUnchecked})
	}
	for _, n := range d128Sizes() {
		a := makeRandD128(n, uint64(n)*13+5)
		b := makeRandD128(n, uint64(n)*17+9)
		want := make([]uint64, 2*n)
		scalarD128SubUnchecked(a, b, want)
		for _, impl := range impls {
			got := make([]uint64, 2*n)
			impl.fn(a, b, got)
			for i := 0; i < 2*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d slot=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}
}

func TestD128AddCheckedVariants(t *testing.T) {
	impls := []d128CheckedImpl{
		{"scalar", scalarD128AddChecked},
		{"avx2", avx2D128AddChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d128CheckedImpl{"avx512", avx512D128AddChecked})
	}

	// 1) No-overflow random inputs.
	for _, n := range d128Sizes() {
		a := makeRand128SmallSigned(n, uint64(n)*19+7)
		b := makeRand128SmallSigned(n, uint64(n)*23+11)
		want := make([]uint64, 2*n)
		if got := scalarD128AddChecked(a, b, want); got != -1 {
			t.Fatalf("setup: scalar overflow at %d for masked input n=%d", got, n)
		}
		for _, impl := range impls {
			got := make([]uint64, 2*n)
			if idx := impl.fn(a, b, got); idx != -1 {
				t.Fatalf("%s n=%d: spurious overflow at %d", impl.name, n, idx)
			}
			for i := 0; i < 2*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d slot=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}

	// 2) Inject a single overflow (MaxInt128 + 1) at varying positions.
	for _, n := range []int{4, 8, 9, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 3, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			a := make([]uint64, 2*n)
			b := make([]uint64, 2*n)
			j := pos << 1
			a[j] = math.MaxUint64
			a[j+1] = uint64(math.MaxInt64) // a = MaxInt128 (positive max)
			b[j] = 1
			b[j+1] = 0 // b = 1
			for _, impl := range impls {
				got := make([]uint64, 2*n)
				idx := impl.fn(a, b, got)
				if idx != pos {
					t.Fatalf("%s n=%d inject pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}

	// 3) Carry-propagation correctness: aLo = MaxUint64, aHi small; bLo = 1.
	// Result should have lo = 0, hi = aHi+1 — no signed overflow.
	for _, n := range []int{4, 8, 16, 17, 33} {
		a := make([]uint64, 2*n)
		b := make([]uint64, 2*n)
		for i := 0; i < n; i++ {
			j := i << 1
			a[j] = math.MaxUint64
			a[j+1] = uint64(i)
			b[j] = 1
			b[j+1] = 0
		}
		want := make([]uint64, 2*n)
		scalarD128AddChecked(a, b, want)
		for _, impl := range impls {
			got := make([]uint64, 2*n)
			if idx := impl.fn(a, b, got); idx != -1 {
				t.Fatalf("%s carry n=%d: spurious overflow at %d", impl.name, n, idx)
			}
			for i := 0; i < 2*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s carry n=%d slot=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}
}

func TestD128SubCheckedVariants(t *testing.T) {
	impls := []d128CheckedImpl{
		{"scalar", scalarD128SubChecked},
		{"avx2", avx2D128SubChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d128CheckedImpl{"avx512", avx512D128SubChecked})
	}

	for _, n := range d128Sizes() {
		a := makeRand128SmallSigned(n, uint64(n)*29+13)
		b := makeRand128SmallSigned(n, uint64(n)*31+17)
		want := make([]uint64, 2*n)
		if got := scalarD128SubChecked(a, b, want); got != -1 {
			t.Fatalf("setup: scalar overflow at %d for n=%d", got, n)
		}
		for _, impl := range impls {
			got := make([]uint64, 2*n)
			if idx := impl.fn(a, b, got); idx != -1 {
				t.Fatalf("%s n=%d: spurious overflow at %d", impl.name, n, idx)
			}
			for i := 0; i < 2*n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d slot=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}

	// Inject MinInt128 - 1 overflow at varying positions.
	for _, n := range []int{4, 8, 9, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 3, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			a := make([]uint64, 2*n)
			b := make([]uint64, 2*n)
			j := pos << 1
			a[j] = 0
			a[j+1] = 1 << 63 // a = MinInt128
			b[j] = 1
			b[j+1] = 0 // b = 1
			for _, impl := range impls {
				got := make([]uint64, 2*n)
				idx := impl.fn(a, b, got)
				if idx != pos {
					t.Fatalf("%s n=%d inject pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}

	// Borrow propagation: aLo = 0, aHi small; bLo = 1 ⇒ lo=Max, hi=aHi-1.
	for _, n := range []int{4, 8, 16, 17, 33} {
		a := make([]uint64, 2*n)
		b := make([]uint64, 2*n)
		for i := 0; i < n; i++ {
			j := i << 1
			a[j] = 0
			a[j+1] = uint64(i + 10) // safely positive and > 0 after borrow
			b[j] = 1
			b[j+1] = 0
		}
		want := make([]uint64, 2*n)
		scalarD128SubChecked(a, b, want)
		for _, impl := range impls {
			got := make([]uint64, 2*n)
			if idx := impl.fn(a, b, got); idx != -1 {
				t.Fatalf("%s borrow n=%d: spurious overflow at %d", impl.name, n, idx)
			}
			for i := 0; i < 2*n; i++ {
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

var d128BenchSizes = []int{32, 128, 512, 2048, 8192}

func benchD128Unchecked(b *testing.B, fn func(a, bb, r []uint64), n int) {
	a := makeRandD128(n, 1)
	bb := makeRandD128(n, 2)
	r := make([]uint64, 2*n)
	b.SetBytes(int64(n) * 16 * 3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(a, bb, r)
	}
}

func benchD128Checked(b *testing.B, fn func(a, bb, r []uint64) int, n int) {
	a := makeRand128SmallSigned(n, 1)
	bb := makeRand128SmallSigned(n, 2)
	r := make([]uint64, 2*n)
	b.SetBytes(int64(n) * 16 * 3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = fn(a, bb, r)
	}
}

func BenchmarkD128AddUnchecked(b *testing.B) {
	for _, n := range d128BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD128Unchecked(b, scalarD128AddUnchecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD128Unchecked(b, avx2D128AddUnchecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD128Unchecked(b, avx512D128AddUnchecked, n) })
		}
	}
}

func BenchmarkD128SubUnchecked(b *testing.B) {
	for _, n := range d128BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD128Unchecked(b, scalarD128SubUnchecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD128Unchecked(b, avx2D128SubUnchecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD128Unchecked(b, avx512D128SubUnchecked, n) })
		}
	}
}

func BenchmarkD128AddChecked(b *testing.B) {
	for _, n := range d128BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD128Checked(b, scalarD128AddChecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD128Checked(b, avx2D128AddChecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD128Checked(b, avx512D128AddChecked, n) })
		}
	}
}

func BenchmarkD128SubChecked(b *testing.B) {
	for _, n := range d128BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD128Checked(b, scalarD128SubChecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD128Checked(b, avx2D128SubChecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD128Checked(b, avx512D128SubChecked, n) })
		}
	}
}

// ---------------------------------------------------------------------------
// Scalar-broadcast tests.
// ---------------------------------------------------------------------------

type d128ScalarVUImpl struct {
	name string
	fn   func(slo, shi uint64, v, r []uint64)
}

type d128ScalarVCImpl struct {
	name string
	fn   func(slo, shi uint64, v, r []uint64) int
}

type d128VScalarUImpl struct {
	name string
	fn   func(v []uint64, slo, shi uint64, r []uint64)
}

type d128VScalarCImpl struct {
	name string
	fn   func(v []uint64, slo, shi uint64, r []uint64) int
}

func d128Scalars() []struct{ lo, hi uint64 } {
	return []struct{ lo, hi uint64 }{
		{0, 0},
		{1, 0},
		{math.MaxUint64, 0},
		{0, 1},
		{0xDEADBEEFCAFEBABE, 0x123456789ABCDEF0},
		{math.MaxUint64, uint64(math.MaxInt64)},
		{0, 1 << 63},
	}
}

func TestD128AddScalarVariants(t *testing.T) {
	impls := []d128ScalarVUImpl{
		{"scalar", scalarD128AddScalarUnchecked},
		{"avx2", avx2D128AddScalarUnchecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d128ScalarVUImpl{"avx512", avx512D128AddScalarUnchecked})
	}
	for _, n := range d128Sizes() {
		v := makeRandD128(n, uint64(n)*37+1)
		for si, s := range d128Scalars() {
			want := make([]uint64, 2*n)
			scalarD128AddScalarUnchecked(s.lo, s.hi, v, want)
			for _, impl := range impls {
				got := make([]uint64, 2*n)
				impl.fn(s.lo, s.hi, v, got)
				for i := 0; i < 2*n; i++ {
					if got[i] != want[i] {
						t.Fatalf("%s n=%d scalar#%d slot=%d: got %x want %x", impl.name, n, si, i, got[i], want[i])
					}
				}
			}
		}
	}
}

func TestD128SubScalarVariants(t *testing.T) {
	impls := []d128VScalarUImpl{
		{"scalar", scalarD128SubScalarUnchecked},
		{"avx2", avx2D128SubScalarUnchecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d128VScalarUImpl{"avx512", avx512D128SubScalarUnchecked})
	}
	for _, n := range d128Sizes() {
		v := makeRandD128(n, uint64(n)*41+3)
		for si, s := range d128Scalars() {
			want := make([]uint64, 2*n)
			scalarD128SubScalarUnchecked(v, s.lo, s.hi, want)
			for _, impl := range impls {
				got := make([]uint64, 2*n)
				impl.fn(v, s.lo, s.hi, got)
				for i := 0; i < 2*n; i++ {
					if got[i] != want[i] {
						t.Fatalf("%s n=%d scalar#%d slot=%d: got %x want %x", impl.name, n, si, i, got[i], want[i])
					}
				}
			}
		}
	}
}

func TestD128ScalarSubVariants(t *testing.T) {
	impls := []d128ScalarVUImpl{
		{"scalar", scalarD128ScalarSubUnchecked},
		{"avx2", avx2D128ScalarSubUnchecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d128ScalarVUImpl{"avx512", avx512D128ScalarSubUnchecked})
	}
	for _, n := range d128Sizes() {
		v := makeRandD128(n, uint64(n)*43+5)
		for si, s := range d128Scalars() {
			want := make([]uint64, 2*n)
			scalarD128ScalarSubUnchecked(s.lo, s.hi, v, want)
			for _, impl := range impls {
				got := make([]uint64, 2*n)
				impl.fn(s.lo, s.hi, v, got)
				for i := 0; i < 2*n; i++ {
					if got[i] != want[i] {
						t.Fatalf("%s n=%d scalar#%d slot=%d: got %x want %x", impl.name, n, si, i, got[i], want[i])
					}
				}
			}
		}
	}
}

func TestD128AddScalarCheckedVariants(t *testing.T) {
	impls := []d128ScalarVCImpl{
		{"scalar", scalarD128AddScalarChecked},
		{"avx2", avx2D128AddScalarChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d128ScalarVCImpl{"avx512", avx512D128AddScalarChecked})
	}

	// 1) No-overflow: small-signed v plus small-signed scalars.
	smallScalars := []struct{ lo, hi uint64 }{
		{0, 0},
		{0xDEADBEEFCAFEBABE, 0x0123456789ABCDEF},
		{1, 0},
		{math.MaxUint64, 0x0FFFFFFFFFFFFFFF},
	}
	for _, n := range d128Sizes() {
		v := makeRand128SmallSigned(n, uint64(n)*47+7)
		for si, s := range smallScalars {
			want := make([]uint64, 2*n)
			if got := scalarD128AddScalarChecked(s.lo, s.hi, v, want); got != -1 {
				t.Fatalf("setup overflow at %d for n=%d scalar#%d", got, n, si)
			}
			for _, impl := range impls {
				got := make([]uint64, 2*n)
				if idx := impl.fn(s.lo, s.hi, v, got); idx != -1 {
					t.Fatalf("%s n=%d scalar#%d: spurious overflow at %d", impl.name, n, si, idx)
				}
				for i := 0; i < 2*n; i++ {
					if got[i] != want[i] {
						t.Fatalf("%s n=%d scalar#%d slot=%d: got %x want %x", impl.name, n, si, i, got[i], want[i])
					}
				}
			}
		}
	}

	// 2) Inject overflow: scalar = 1, v[pos] = MaxInt128.
	for _, n := range []int{4, 8, 9, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 3, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			v := make([]uint64, 2*n)
			j := pos << 1
			v[j] = math.MaxUint64
			v[j+1] = uint64(math.MaxInt64)
			for _, impl := range impls {
				got := make([]uint64, 2*n)
				idx := impl.fn(1, 0, v, got)
				if idx != pos {
					t.Fatalf("%s n=%d pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}
}

func TestD128SubScalarCheckedVariants(t *testing.T) {
	impls := []d128VScalarCImpl{
		{"scalar", scalarD128SubScalarChecked},
		{"avx2", avx2D128SubScalarChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d128VScalarCImpl{"avx512", avx512D128SubScalarChecked})
	}

	smallScalars := []struct{ lo, hi uint64 }{
		{0, 0},
		{1, 0},
		{0xDEADBEEFCAFEBABE, 0x0123456789ABCDEF},
		{math.MaxUint64, 0x0FFFFFFFFFFFFFFF},
	}
	for _, n := range d128Sizes() {
		v := makeRand128SmallSigned(n, uint64(n)*53+11)
		for si, s := range smallScalars {
			want := make([]uint64, 2*n)
			if got := scalarD128SubScalarChecked(v, s.lo, s.hi, want); got != -1 {
				t.Fatalf("setup overflow at %d for n=%d scalar#%d", got, n, si)
			}
			for _, impl := range impls {
				got := make([]uint64, 2*n)
				if idx := impl.fn(v, s.lo, s.hi, got); idx != -1 {
					t.Fatalf("%s n=%d scalar#%d: spurious overflow at %d", impl.name, n, si, idx)
				}
				for i := 0; i < 2*n; i++ {
					if got[i] != want[i] {
						t.Fatalf("%s n=%d scalar#%d slot=%d: got %x want %x", impl.name, n, si, i, got[i], want[i])
					}
				}
			}
		}
	}

	// Inject overflow: v[pos] = MinInt128, scalar = 1 ⇒ MinInt128 - 1 overflows.
	for _, n := range []int{4, 8, 9, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 3, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			v := make([]uint64, 2*n)
			j := pos << 1
			v[j] = 0
			v[j+1] = 1 << 63
			for _, impl := range impls {
				got := make([]uint64, 2*n)
				idx := impl.fn(v, 1, 0, got)
				if idx != pos {
					t.Fatalf("%s n=%d pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}
}

func TestD128ScalarSubCheckedVariants(t *testing.T) {
	impls := []d128ScalarVCImpl{
		{"scalar", scalarD128ScalarSubChecked},
		{"avx2", avx2D128ScalarSubChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d128ScalarVCImpl{"avx512", avx512D128ScalarSubChecked})
	}

	smallScalars := []struct{ lo, hi uint64 }{
		{0, 0},
		{1, 0},
		{0xDEADBEEFCAFEBABE, 0x0123456789ABCDEF},
		{math.MaxUint64, 0x0FFFFFFFFFFFFFFF},
	}
	for _, n := range d128Sizes() {
		v := makeRand128SmallSigned(n, uint64(n)*59+13)
		for si, s := range smallScalars {
			want := make([]uint64, 2*n)
			if got := scalarD128ScalarSubChecked(s.lo, s.hi, v, want); got != -1 {
				t.Fatalf("setup overflow at %d for n=%d scalar#%d", got, n, si)
			}
			for _, impl := range impls {
				got := make([]uint64, 2*n)
				if idx := impl.fn(s.lo, s.hi, v, got); idx != -1 {
					t.Fatalf("%s n=%d scalar#%d: spurious overflow at %d", impl.name, n, si, idx)
				}
				for i := 0; i < 2*n; i++ {
					if got[i] != want[i] {
						t.Fatalf("%s n=%d scalar#%d slot=%d: got %x want %x", impl.name, n, si, i, got[i], want[i])
					}
				}
			}
		}
	}

	// Inject overflow: scalar = MinInt128, v[pos] = 1 ⇒ MinInt128 - 1 overflows.
	for _, n := range []int{4, 8, 9, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 3, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			v := make([]uint64, 2*n)
			j := pos << 1
			v[j] = 1
			v[j+1] = 0
			for _, impl := range impls {
				got := make([]uint64, 2*n)
				idx := impl.fn(0, 1<<63, v, got)
				if idx != pos {
					t.Fatalf("%s n=%d pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}
}

func benchD128AddScalarU(b *testing.B, fn func(slo, shi uint64, v, r []uint64), n int) {
	v := makeRandD128(n, 1)
	r := make([]uint64, 2*n)
	b.SetBytes(int64(n) * 16 * 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(0xDEADBEEFCAFEBABE, 0x0123456789ABCDEF, v, r)
	}
}

func benchD128SubScalarU(b *testing.B, fn func(v []uint64, slo, shi uint64, r []uint64), n int) {
	v := makeRandD128(n, 1)
	r := make([]uint64, 2*n)
	b.SetBytes(int64(n) * 16 * 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(v, 0xDEADBEEFCAFEBABE, 0x0123456789ABCDEF, r)
	}
}

func BenchmarkD128AddScalarUnchecked(b *testing.B) {
	for _, n := range d128BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD128AddScalarU(b, scalarD128AddScalarUnchecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD128AddScalarU(b, avx2D128AddScalarUnchecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD128AddScalarU(b, avx512D128AddScalarUnchecked, n) })
		}
	}
}

func BenchmarkD128SubScalarUnchecked(b *testing.B) {
	for _, n := range d128BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD128SubScalarU(b, scalarD128SubScalarUnchecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD128SubScalarU(b, avx2D128SubScalarUnchecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD128SubScalarU(b, avx512D128SubScalarUnchecked, n) })
		}
	}
}

func BenchmarkD128ScalarSubUnchecked(b *testing.B) {
	for _, n := range d128BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD128AddScalarU(b, scalarD128ScalarSubUnchecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD128AddScalarU(b, avx2D128ScalarSubUnchecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD128AddScalarU(b, avx512D128ScalarSubUnchecked, n) })
		}
	}
}

func TestD128SumReduceVariants(t *testing.T) {
	impls := []struct {
		name string
		fn   func([]uint64) (uint64, uint64)
	}{
		{"scalar", scalarD128SumReduce},
	}
	if cpu.X86.HasAVX2 {
		impls = append(impls, struct {
			name string
			fn   func([]uint64) (uint64, uint64)
		}{"avx2", avx2D128SumReduce})
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, struct {
			name string
			fn   func([]uint64) (uint64, uint64)
		}{"avx512", avx512D128SumReduce})
	}

	for _, n := range d128Sizes() {
		v := makeRandD128(n, uint64(n)*23+1)
		var refLo, refHi uint64
		for i := 0; i < n; i++ {
			j := i << 1
			c := uint64(0)
			if v[j]+refLo < refLo {
				c = 1
			}
			refLo += v[j]
			refHi += v[j+1] + c
		}
		for _, im := range impls {
			lo, hi := im.fn(v)
			if lo != refLo || hi != refHi {
				t.Fatalf("%s n=%d: got (%x,%x) want (%x,%x)", im.name, n, lo, hi, refLo, refHi)
			}
		}
	}
}

func BenchmarkD128SumReduce(b *testing.B) {
	for _, n := range d128BenchSizes {
		v := makeRandD128(n, 1)
		b.Run("scalar/n="+itoa(n), func(b *testing.B) {
			b.SetBytes(int64(n) * 16)
			for i := 0; i < b.N; i++ {
				_, _ = scalarD128SumReduce(v)
			}
		})
		if cpu.X86.HasAVX2 {
			b.Run("avx2/n="+itoa(n), func(b *testing.B) {
				b.SetBytes(int64(n) * 16)
				for i := 0; i < b.N; i++ {
					_, _ = avx2D128SumReduce(v)
				}
			})
		}
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) {
				b.SetBytes(int64(n) * 16)
				for i := 0; i < b.N; i++ {
					_, _ = avx512D128SumReduce(v)
				}
			})
		}
	}
}
