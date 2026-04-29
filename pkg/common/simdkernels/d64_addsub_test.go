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

// ---------------------------------------------------------------------------
// Correctness: compare every available impl (scalar, AVX2, AVX-512 if built)
// against the scalar reference on random + edge-case inputs at sizes that
// exercise the unrolled main loop, the 4-/8-wide cleanup, and the scalar
// tail (e.g. 35 = 32+3 or 4+...+3).
// ---------------------------------------------------------------------------

type d64UncheckedImpl struct {
	name string
	fn   func(a, b, r []uint64)
}

type d64CheckedImpl struct {
	name string
	fn   func(a, b, r []uint64) int
}

func d64Sizes() []int {
	return []int{0, 1, 3, 4, 7, 8, 15, 16, 17, 31, 32, 33, 35, 63, 64, 127, 128, 255, 256, 1023, 4096}
}

func makeRandD64(n int, seed uint64) []uint64 {
	rng := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
	out := make([]uint64, n)
	for i := range out {
		out[i] = rng.Uint64()
	}
	return out
}

// edgeInputs returns a small input pair tuned to provoke add+sub overflows
// at a few specific positions (so tests cover both the "no overflow" fast
// path and the rescan slow path).
func edgeInputs() (a, b []uint64) {
	a = []uint64{
		0, 1, math.MaxUint64,
		uint64(math.MaxInt64), uint64(math.MaxInt64),
		1 << 63, 1 << 63,
		42, 100, 200,
	}
	b = []uint64{
		0, math.MaxUint64, 1,
		1, uint64(math.MaxInt64),
		uint64(math.MaxInt64) + 1, 1,
		58, 200, 100,
	}
	return
}

func TestD64AddVariants(t *testing.T) {
	impls := []d64UncheckedImpl{
		{"scalar", scalarD64AddUnchecked},
		{"avx2", avx2D64AddUnchecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d64UncheckedImpl{"avx512", avx512D64AddUnchecked})
	}
	for _, n := range d64Sizes() {
		a := makeRandD64(n, uint64(n)*7+1)
		b := makeRandD64(n, uint64(n)*11+3)
		want := make([]uint64, n)
		scalarD64AddUnchecked(a, b, want)
		for _, impl := range impls {
			got := make([]uint64, n)
			impl.fn(a, b, got)
			for i := 0; i < n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d i=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}
}

func TestD64SubVariants(t *testing.T) {
	impls := []d64UncheckedImpl{
		{"scalar", scalarD64SubUnchecked},
		{"avx2", avx2D64SubUnchecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d64UncheckedImpl{"avx512", avx512D64SubUnchecked})
	}
	for _, n := range d64Sizes() {
		a := makeRandD64(n, uint64(n)*13+5)
		b := makeRandD64(n, uint64(n)*17+9)
		want := make([]uint64, n)
		scalarD64SubUnchecked(a, b, want)
		for _, impl := range impls {
			got := make([]uint64, n)
			impl.fn(a, b, got)
			for i := 0; i < n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d i=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}
}

func TestD64AddCheckedVariants(t *testing.T) {
	impls := []d64CheckedImpl{
		{"scalar", scalarD64AddChecked},
		{"avx2", avx2D64AddChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d64CheckedImpl{"avx512", avx512D64AddChecked})
	}

	// 1) Edge inputs: contains a known overflow at index 5 (MinInt64 + (MaxInt64+1)).
	ea, eb := edgeInputs()
	wantR := make([]uint64, len(ea))
	wantIdx := scalarD64AddChecked(ea, eb, wantR)
	for _, impl := range impls {
		gotR := make([]uint64, len(ea))
		gotIdx := impl.fn(ea, eb, gotR)
		// First-overflow position must match scalar reference.
		if gotIdx != wantIdx {
			t.Fatalf("%s edge: idx got %d want %d", impl.name, gotIdx, wantIdx)
		}
		// Values up to (and including) the first overflow must match.
		end := len(ea)
		if wantIdx >= 0 {
			end = wantIdx + 1
		}
		for i := 0; i < end; i++ {
			if gotR[i] != wantR[i] {
				t.Fatalf("%s edge i=%d: got %x want %x", impl.name, i, gotR[i], wantR[i])
			}
		}
	}

	// 2) No-overflow random inputs: clear sign bits to avoid spurious overflows.
	for _, n := range d64Sizes() {
		a := makeRandD64(n, uint64(n)*19+7)
		b := makeRandD64(n, uint64(n)*23+11)
		for i := range a {
			a[i] &= 0x3FFFFFFFFFFFFFFF
			b[i] &= 0x3FFFFFFFFFFFFFFF
		}
		want := make([]uint64, n)
		if got := scalarD64AddChecked(a, b, want); got != -1 {
			t.Fatalf("setup: scalar reported overflow at %d for masked input n=%d", got, n)
		}
		for _, impl := range impls {
			got := make([]uint64, n)
			if idx := impl.fn(a, b, got); idx != -1 {
				t.Fatalf("%s n=%d: spurious overflow at %d", impl.name, n, idx)
			}
			for i := 0; i < n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d i=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}

	// 3) Single overflow injected at varying positions (covers both vector
	// body and scalar tail of every impl).
	for _, n := range []int{8, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			a := make([]uint64, n)
			b := make([]uint64, n)
			a[pos] = uint64(math.MaxInt64)
			b[pos] = 1
			for _, impl := range impls {
				got := make([]uint64, n)
				idx := impl.fn(a, b, got)
				if idx != pos {
					t.Fatalf("%s n=%d inject pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}
}

func TestD64SubCheckedVariants(t *testing.T) {
	impls := []d64CheckedImpl{
		{"scalar", scalarD64SubChecked},
		{"avx2", avx2D64SubChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d64CheckedImpl{"avx512", avx512D64SubChecked})
	}

	// No-overflow random inputs: mask to 62 bits so a-b stays in range.
	for _, n := range d64Sizes() {
		a := makeRandD64(n, uint64(n)*29+13)
		b := makeRandD64(n, uint64(n)*31+17)
		for i := range a {
			a[i] &= 0x3FFFFFFFFFFFFFFF
			b[i] &= 0x3FFFFFFFFFFFFFFF
		}
		want := make([]uint64, n)
		if got := scalarD64SubChecked(a, b, want); got != -1 {
			t.Fatalf("setup: scalar overflow at %d for n=%d", got, n)
		}
		for _, impl := range impls {
			got := make([]uint64, n)
			if idx := impl.fn(a, b, got); idx != -1 {
				t.Fatalf("%s n=%d: spurious overflow at %d", impl.name, n, idx)
			}
			for i := 0; i < n; i++ {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d i=%d: got %x want %x", impl.name, n, i, got[i], want[i])
				}
			}
		}
	}

	// Inject MinInt64 - 1 overflow at varying positions.
	for _, n := range []int{8, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			a := make([]uint64, n)
			b := make([]uint64, n)
			a[pos] = 1 << 63
			b[pos] = 1
			for _, impl := range impls {
				got := make([]uint64, n)
				idx := impl.fn(a, b, got)
				if idx != pos {
					t.Fatalf("%s n=%d inject pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Benchmarks: per-impl × per-size. Standard go test -bench output.
// ---------------------------------------------------------------------------

var d64BenchSizes = []int{64, 256, 1024, 4096, 16384}

func benchD64Unchecked(b *testing.B, fn func(a, bb, r []uint64), n int) {
	a := makeRandD64(n, 1)
	bb := makeRandD64(n, 2)
	r := make([]uint64, n)
	b.SetBytes(int64(n) * 8 * 3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(a, bb, r)
	}
}

func benchD64Checked(b *testing.B, fn func(a, bb, r []uint64) int, n int) {
	a := makeRandD64(n, 1)
	bb := makeRandD64(n, 2)
	for i := range a {
		a[i] &= 0x3FFFFFFFFFFFFFFF
		bb[i] &= 0x3FFFFFFFFFFFFFFF
	}
	r := make([]uint64, n)
	b.SetBytes(int64(n) * 8 * 3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = fn(a, bb, r)
	}
}

func BenchmarkD64AddUnchecked(b *testing.B) {
	for _, n := range d64BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD64Unchecked(b, scalarD64AddUnchecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD64Unchecked(b, avx2D64AddUnchecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD64Unchecked(b, avx512D64AddUnchecked, n) })
		}
	}
}

func BenchmarkD64SubUnchecked(b *testing.B) {
	for _, n := range d64BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD64Unchecked(b, scalarD64SubUnchecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD64Unchecked(b, avx2D64SubUnchecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD64Unchecked(b, avx512D64SubUnchecked, n) })
		}
	}
}

func BenchmarkD64AddChecked(b *testing.B) {
	for _, n := range d64BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD64Checked(b, scalarD64AddChecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD64Checked(b, avx2D64AddChecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD64Checked(b, avx512D64AddChecked, n) })
		}
	}
}

func BenchmarkD64SubChecked(b *testing.B) {
	for _, n := range d64BenchSizes {
		b.Run("scalar/n="+itoa(n), func(b *testing.B) { benchD64Checked(b, scalarD64SubChecked, n) })
		b.Run("avx2/n="+itoa(n), func(b *testing.B) { benchD64Checked(b, avx2D64SubChecked, n) })
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) { benchD64Checked(b, avx512D64SubChecked, n) })
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// ---------------------------------------------------------------------------
// Scalar-broadcast tests: validate AVX2/AVX-512 broadcast variants against
// the scalar reference for both Unchecked (results) and Checked (results +
// first-overflow index) shapes.
// ---------------------------------------------------------------------------

type d64ScalarUncheckedImpl struct {
	name string
	fn   func(s uint64, v, r []uint64)
}

type d64ScalarUncheckedImplR struct {
	name string
	fn   func(v []uint64, s uint64, r []uint64)
}

type d64ScalarCheckedImpl struct {
	name string
	fn   func(s uint64, v, r []uint64) int
}

type d64ScalarCheckedImplR struct {
	name string
	fn   func(v []uint64, s uint64, r []uint64) int
}

func d64ScalarSamples() []uint64 {
	return []uint64{
		0, 1, 42,
		uint64(math.MaxInt64), uint64(math.MaxInt64) - 1,
		1 << 63, // MinInt64
		(1 << 63) + 1,
		math.MaxUint64,
	}
}

func TestD64AddScalarVariants(t *testing.T) {
	uncheckedImpls := []d64ScalarUncheckedImpl{
		{"scalar", scalarD64AddScalarUnchecked},
		{"avx2", avx2D64AddScalarUnchecked},
	}
	if cpu.X86.HasAVX512 {
		uncheckedImpls = append(uncheckedImpls, d64ScalarUncheckedImpl{"avx512", avx512D64AddScalarUnchecked})
	}
	for _, n := range d64Sizes() {
		v := makeRandD64(n, uint64(n)*31+13)
		for _, s := range d64ScalarSamples() {
			want := make([]uint64, n)
			scalarD64AddScalarUnchecked(s, v, want)
			for _, impl := range uncheckedImpls {
				got := make([]uint64, n)
				impl.fn(s, v, got)
				for i := 0; i < n; i++ {
					if got[i] != want[i] {
						t.Fatalf("AddScalarUnchecked %s n=%d s=%x i=%d: got %x want %x",
							impl.name, n, s, i, got[i], want[i])
					}
				}
			}
		}
	}
}

func TestD64SubScalarVariants(t *testing.T) {
	uncheckedImpls := []d64ScalarUncheckedImplR{
		{"scalar", scalarD64SubScalarUnchecked},
		{"avx2", avx2D64SubScalarUnchecked},
	}
	if cpu.X86.HasAVX512 {
		uncheckedImpls = append(uncheckedImpls, d64ScalarUncheckedImplR{"avx512", avx512D64SubScalarUnchecked})
	}
	for _, n := range d64Sizes() {
		v := makeRandD64(n, uint64(n)*37+17)
		for _, s := range d64ScalarSamples() {
			want := make([]uint64, n)
			scalarD64SubScalarUnchecked(v, s, want)
			for _, impl := range uncheckedImpls {
				got := make([]uint64, n)
				impl.fn(v, s, got)
				for i := 0; i < n; i++ {
					if got[i] != want[i] {
						t.Fatalf("SubScalarUnchecked %s n=%d s=%x i=%d: got %x want %x",
							impl.name, n, s, i, got[i], want[i])
					}
				}
			}
		}
	}
}

func TestD64ScalarSubVariants(t *testing.T) {
	uncheckedImpls := []d64ScalarUncheckedImpl{
		{"scalar", scalarD64ScalarSubUnchecked},
		{"avx2", avx2D64ScalarSubUnchecked},
	}
	if cpu.X86.HasAVX512 {
		uncheckedImpls = append(uncheckedImpls, d64ScalarUncheckedImpl{"avx512", avx512D64ScalarSubUnchecked})
	}
	for _, n := range d64Sizes() {
		v := makeRandD64(n, uint64(n)*41+19)
		for _, s := range d64ScalarSamples() {
			want := make([]uint64, n)
			scalarD64ScalarSubUnchecked(s, v, want)
			for _, impl := range uncheckedImpls {
				got := make([]uint64, n)
				impl.fn(s, v, got)
				for i := 0; i < n; i++ {
					if got[i] != want[i] {
						t.Fatalf("ScalarSubUnchecked %s n=%d s=%x i=%d: got %x want %x",
							impl.name, n, s, i, got[i], want[i])
					}
				}
			}
		}
	}
}

func TestD64AddScalarCheckedVariants(t *testing.T) {
	impls := []d64ScalarCheckedImpl{
		{"scalar", scalarD64AddScalarChecked},
		{"avx2", avx2D64AddScalarChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d64ScalarCheckedImpl{"avx512", avx512D64AddScalarChecked})
	}

	// 1) No-overflow random (mask sign bit) inputs.
	for _, n := range d64Sizes() {
		v := makeRandD64(n, uint64(n)*43+23)
		for i := range v {
			v[i] &= 0x3FFFFFFFFFFFFFFF
		}
		for _, s := range []uint64{0, 1, 42, 0x3FFFFFFFFFFFFFFF} {
			want := make([]uint64, n)
			if got := scalarD64AddScalarChecked(s, v, want); got != -1 {
				t.Fatalf("setup: scalar reported overflow at %d for masked input n=%d s=%x", got, n, s)
			}
			for _, impl := range impls {
				got := make([]uint64, n)
				if idx := impl.fn(s, v, got); idx != -1 {
					t.Fatalf("AddScalarChecked %s n=%d s=%x: spurious overflow at %d", impl.name, n, s, idx)
				}
				for i := 0; i < n; i++ {
					if got[i] != want[i] {
						t.Fatalf("AddScalarChecked %s n=%d s=%x i=%d: got %x want %x",
							impl.name, n, s, i, got[i], want[i])
					}
				}
			}
		}
	}

	// 2) Inject a single overflow at varying positions, with s = MaxInt64.
	s := uint64(math.MaxInt64)
	for _, n := range []int{8, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			v := make([]uint64, n)
			v[pos] = 1 // s + 1 = MaxInt64+1 → overflow
			for _, impl := range impls {
				got := make([]uint64, n)
				idx := impl.fn(s, v, got)
				if idx != pos {
					t.Fatalf("AddScalarChecked %s n=%d inject pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}
}

func TestD64SubScalarCheckedVariants(t *testing.T) {
	impls := []d64ScalarCheckedImplR{
		{"scalar", scalarD64SubScalarChecked},
		{"avx2", avx2D64SubScalarChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d64ScalarCheckedImplR{"avx512", avx512D64SubScalarChecked})
	}

	// 1) No-overflow random (mask sign bit) inputs.
	for _, n := range d64Sizes() {
		v := makeRandD64(n, uint64(n)*47+29)
		for i := range v {
			v[i] &= 0x3FFFFFFFFFFFFFFF
		}
		for _, s := range []uint64{0, 1, 42, 0x3FFFFFFFFFFFFFFF} {
			want := make([]uint64, n)
			if got := scalarD64SubScalarChecked(v, s, want); got != -1 {
				t.Fatalf("setup: scalar reported overflow at %d for masked input n=%d s=%x", got, n, s)
			}
			for _, impl := range impls {
				got := make([]uint64, n)
				if idx := impl.fn(v, s, got); idx != -1 {
					t.Fatalf("SubScalarChecked %s n=%d s=%x: spurious overflow at %d", impl.name, n, s, idx)
				}
				for i := 0; i < n; i++ {
					if got[i] != want[i] {
						t.Fatalf("SubScalarChecked %s n=%d s=%x i=%d: got %x want %x",
							impl.name, n, s, i, got[i], want[i])
					}
				}
			}
		}
	}

	// 2) Inject overflow: v[pos] = MinInt64, s = 1 → MinInt64 - 1 → overflow.
	s := uint64(1)
	for _, n := range []int{8, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			v := make([]uint64, n)
			v[pos] = 1 << 63 // MinInt64
			for _, impl := range impls {
				got := make([]uint64, n)
				idx := impl.fn(v, s, got)
				if idx != pos {
					t.Fatalf("SubScalarChecked %s n=%d inject pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}
}

func TestD64ScalarSubCheckedVariants(t *testing.T) {
	impls := []d64ScalarCheckedImpl{
		{"scalar", scalarD64ScalarSubChecked},
		{"avx2", avx2D64ScalarSubChecked},
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, d64ScalarCheckedImpl{"avx512", avx512D64ScalarSubChecked})
	}

	// 1) No-overflow random (mask sign bit) inputs.
	for _, n := range d64Sizes() {
		v := makeRandD64(n, uint64(n)*53+31)
		for i := range v {
			v[i] &= 0x3FFFFFFFFFFFFFFF
		}
		for _, s := range []uint64{0, 1, 42, 0x3FFFFFFFFFFFFFFF} {
			want := make([]uint64, n)
			if got := scalarD64ScalarSubChecked(s, v, want); got != -1 {
				t.Fatalf("setup: scalar reported overflow at %d for masked input n=%d s=%x", got, n, s)
			}
			for _, impl := range impls {
				got := make([]uint64, n)
				if idx := impl.fn(s, v, got); idx != -1 {
					t.Fatalf("ScalarSubChecked %s n=%d s=%x: spurious overflow at %d", impl.name, n, s, idx)
				}
				for i := 0; i < n; i++ {
					if got[i] != want[i] {
						t.Fatalf("ScalarSubChecked %s n=%d s=%x i=%d: got %x want %x",
							impl.name, n, s, i, got[i], want[i])
					}
				}
			}
		}
	}

	// 2) Inject overflow: s = MinInt64, v[pos] = 1 → MinInt64 - 1 → overflow.
	s := uint64(1 << 63) // MinInt64
	for _, n := range []int{8, 16, 17, 33, 35, 64} {
		for _, pos := range []int{0, 1, 4, 7, 8, n - 1} {
			if pos < 0 || pos >= n {
				continue
			}
			v := make([]uint64, n)
			v[pos] = 1
			for _, impl := range impls {
				got := make([]uint64, n)
				idx := impl.fn(s, v, got)
				if idx != pos {
					t.Fatalf("ScalarSubChecked %s n=%d inject pos=%d: got idx %d", impl.name, n, pos, idx)
				}
			}
		}
	}
}

// makeRandD64Bounded returns Decimal64-range values: |x| < 10^18.
func makeRandD64Bounded(n int, seed uint64) []uint64 {
	rng := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
	const maxAbs uint64 = 1_000_000_000_000_000_000
	out := make([]uint64, n)
	for i := range out {
		x := int64(rng.Uint64N(maxAbs))
		if rng.IntN(2) == 1 {
			x = -x
		}
		out[i] = uint64(x)
	}
	return out
}

func TestD64SumReduceToD128Variants(t *testing.T) {
	impls := []struct {
		name string
		fn   func([]uint64) (uint64, uint64)
	}{
		{"scalar", scalarD64SumReduceToD128},
	}
	if cpu.X86.HasAVX2 {
		impls = append(impls, struct {
			name string
			fn   func([]uint64) (uint64, uint64)
		}{"avx2", avx2D64SumReduceToD128})
	}
	if cpu.X86.HasAVX512 {
		impls = append(impls, struct {
			name string
			fn   func([]uint64) (uint64, uint64)
		}{"avx512", avx512D64SumReduceToD128})
	}

	for _, n := range d64Sizes() {
		v := makeRandD64Bounded(n, uint64(n)*23+1)
		refLo, refHi := scalarD64SumReduceToD128(v)
		for _, im := range impls {
			lo, hi := im.fn(v)
			if lo != refLo || hi != refHi {
				t.Fatalf("%s n=%d: got (%x,%x) want (%x,%x)", im.name, n, lo, hi, refLo, refHi)
			}
		}
	}
}

func BenchmarkD64SumReduceToD128(b *testing.B) {
	for _, n := range d64BenchSizes {
		v := makeRandD64Bounded(n, 1)
		b.Run("scalar/n="+itoa(n), func(b *testing.B) {
			b.SetBytes(int64(n) * 8)
			for i := 0; i < b.N; i++ {
				_, _ = scalarD64SumReduceToD128(v)
			}
		})
		if cpu.X86.HasAVX2 {
			b.Run("avx2/n="+itoa(n), func(b *testing.B) {
				b.SetBytes(int64(n) * 8)
				for i := 0; i < b.N; i++ {
					_, _ = avx2D64SumReduceToD128(v)
				}
			})
		}
		if cpu.X86.HasAVX512 {
			b.Run("avx512/n="+itoa(n), func(b *testing.B) {
				b.SetBytes(int64(n) * 8)
				for i := 0; i < b.N; i++ {
					_, _ = avx512D64SumReduceToD128(v)
				}
			})
		}
	}
}
