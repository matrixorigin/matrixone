// Copyright 2022 Matrix Origin
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

package docfilter

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const benchFpProbability = 0.001

// benchCbitmapCap forces cbitmap construction regardless of the production
// MaxCbitmapBits, so the benchmark measures cbitmap at every N (the dense bench
// keys reach ~2N, which exceeds the production cap at large N). It is a
// measurement knob only; production uses MaxCbitmapBits.
const benchCbitmapCap = uint64(1) << 31

// mustCbitmap builds a dense cbitmap (uncapped) for benchmarking; the dense
// bench keys are always feasible under benchCbitmapCap.
func mustCbitmap(tb testing.TB, v *vector.Vector) []byte {
	out, ok, err := buildCbitmapBytesCap(v, benchCbitmapCap, false)
	if err != nil || !ok {
		tb.Fatalf("build cbitmap: ok=%v err=%v", ok, err)
	}
	return out
}

// benchSizes are candidate-set cardinalities N (the number of doc_ids the
// filter is built from). The whole point of the bitset is cheaper build at
// large N, so we sweep a range.
var benchSizes = []int{100000, 1000000, 10000000, 20000000}

// makeInt64Vec builds an int64 doc_id vector of n distinct (odd) keys.
func makeInt64Vec(tb testing.TB, mp *mpool.MPool, n int) *vector.Vector {
	v := vector.NewVec(types.T_int64.ToType())
	for i := 0; i < n; i++ {
		if err := vector.AppendFixed(v, int64(2*i+1), false, mp); err != nil {
			tb.Fatal(err)
		}
	}
	return v
}

// makeProbeVec builds a block of `rows` doc_ids, half present (odd, in the
// filter) and half absent (even, not in the filter) — a realistic 50% hit mix
// for a block scan.
func makeProbeVec(tb testing.TB, mp *mpool.MPool, rows int) *vector.Vector {
	v := vector.NewVec(types.T_int64.ToType())
	for i := 0; i < rows; i++ {
		val := int64(2*i + 1) // odd: present
		if i%2 == 0 {
			val = int64(2 * i) // even: absent
		}
		if err := vector.AppendFixed(v, val, false, mp); err != nil {
			tb.Fatal(err)
		}
	}
	return v
}

// BenchmarkBuild compares building + serializing the doc_id filter structures
// (bloom / dense cbitmap / C CRoaring) from the same N integer doc_ids. Reports
// the serialized size too.
func BenchmarkBuild(b *testing.B) {
	mp := mpool.MustNewZero()
	for _, n := range benchSizes {
		keyvec := makeInt64Vec(b, mp, n)

		b.Run(fmt.Sprintf("bloom/N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			var sz int
			for i := 0; i < b.N; i++ {
				bf := bloomfilter.NewCBloomFilterWithProbability(int64(n), benchFpProbability)
				bf.AddVector(keyvec)
				out, err := bf.Marshal()
				if err != nil {
					b.Fatal(err)
				}
				sz = len(out)
				bf.Free()
			}
			b.ReportMetric(float64(sz), "bytes")
		})

		// cbitmap: a DENSE bitset indexed by the doc_id value, built + serialized
		// in C directly from the column buffer (one cgo call). Sized to the max
		// key (here ~2N), so size = O(max value), not O(N) — only viable for
		// dense/bounded integer PKs within MaxCbitmapBits. Keys are odd values
		// in [1, 2N), so it is skipped once 2N exceeds the cap (CRoaring's regime).
		b.Run(fmt.Sprintf("cbitmap/N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			var sz int
			for i := 0; i < b.N; i++ {
				sz = len(mustCbitmap(b, keyvec))
			}
			b.ReportMetric(float64(sz), "bytes")
		})

		// CRoaring (C roaring64): compact O(N) AND C-speed; reads the vector
		// directly in C.
		b.Run(fmt.Sprintf("croaring/N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			var sz int
			for i := 0; i < b.N; i++ {
				out, err := BuildCRoaringBytes(keyvec)
				if err != nil {
					b.Fatal(err)
				}
				sz = len(out)
			}
			b.ReportMetric(float64(sz), "bytes")
		})

		keyvec.Free(mp)
	}
}

// BenchmarkTestVector compares probing a block of doc_ids (50% hit) against the
// prebuilt structures (bloom / cbitmap / C CRoaring).
func BenchmarkTestVector(b *testing.B) {
	mp := mpool.MustNewZero()
	const probeRows = 8192
	for _, n := range benchSizes {
		keyvec := makeInt64Vec(b, mp, n)
		probe := makeProbeVec(b, mp, probeRows)

		bf := bloomfilter.NewCBloomFilterWithProbability(int64(n), benchFpProbability)
		bf.AddVector(keyvec)
		crf, err := NewCRoaringFilter(must(BuildCRoaringBytes(keyvec)))
		if err != nil {
			b.Fatal(err)
		}

		b.Run(fmt.Sprintf("bloom/N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				bf.TestVector(probe, nil)
			}
		})
		b.Run(fmt.Sprintf("cbitmap/N=%d", n), func(b *testing.B) {
			cbf, err := NewCbitmapFilter(mustCbitmap(b, keyvec))
			if err != nil {
				b.Fatal(err)
			}
			defer cbf.Free()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				cbf.TestVector(probe, nil)
			}
		})
		b.Run(fmt.Sprintf("croaring/N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				crf.TestVector(probe, nil)
			}
		})

		bf.Free()
		crf.Free()
		keyvec.Free(mp)
		probe.Free(mp)
	}
}

// BenchmarkTestSingle compares per-key Test([]byte) probes (the committed-entry
// path uses Test row-by-row).
func BenchmarkTestSingle(b *testing.B) {
	mp := mpool.MustNewZero()
	for _, n := range benchSizes {
		keyvec := makeInt64Vec(b, mp, n)
		probe := makeProbeVec(b, mp, 1024)
		raws := make([][]byte, probe.Length())
		for i := range raws {
			raws[i] = probe.GetRawBytesAt(i)
		}

		bf := bloomfilter.NewCBloomFilterWithProbability(int64(n), benchFpProbability)
		bf.AddVector(keyvec)
		crf, err := NewCRoaringFilter(must(BuildCRoaringBytes(keyvec)))
		if err != nil {
			b.Fatal(err)
		}

		b.Run(fmt.Sprintf("bloom/N=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, r := range raws {
					_ = bf.Test(r)
				}
			}
		})
		b.Run(fmt.Sprintf("cbitmap/N=%d", n), func(b *testing.B) {
			cbf, err := NewCbitmapFilter(mustCbitmap(b, keyvec))
			if err != nil {
				b.Fatal(err)
			}
			defer cbf.Free()
			for i := 0; i < b.N; i++ {
				for _, r := range raws {
					_ = cbf.Test(r)
				}
			}
		})
		b.Run(fmt.Sprintf("croaring/N=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, r := range raws {
					_ = crf.Test(r)
				}
			}
		})

		bf.Free()
		crf.Free()
		keyvec.Free(mp)
		probe.Free(mp)
	}
}

// makeClusteredVec builds `runs` consecutive ranges of runLen ids each (gap =
// runLen between runs, ~50% global density). Contiguous runs are run_optimize's
// sweet spot (e.g. a BETWEEN range or a sequential-PK slice).
func makeClusteredVec(tb testing.TB, mp *mpool.MPool, runs, runLen int) *vector.Vector {
	v := vector.NewVec(types.T_int64.ToType())
	for r := 0; r < runs; r++ {
		base := int64(r) * int64(2*runLen)
		for j := 0; j < runLen; j++ {
			if err := vector.AppendFixed(v, base+int64(j), false, mp); err != nil {
				tb.Fatal(err)
			}
		}
	}
	return v
}

// BenchmarkRunOptimize compares building a CRoaring filter without vs with the
// run_optimize pass on clustered (contiguous-run) id sets. The time delta is
// run_optimize's cost; the "bytes" metric shows the size it saves.
func BenchmarkRunOptimize(b *testing.B) {
	mp := mpool.MustNewZero()
	cases := []struct {
		runs, runLen int
	}{
		{1, 1000000}, // one contiguous 1M range
		{100, 10000}, // 100 runs of 10k
		{10000, 100}, // 10k runs of 100
		{1000000, 1}, // fully scattered (no runs — worst case for run_optimize)
	}
	for _, tc := range cases {
		v := makeClusteredVec(b, mp, tc.runs, tc.runLen)
		name := fmt.Sprintf("runs=%d/len=%d", tc.runs, tc.runLen)

		b.Run("plain/"+name, func(b *testing.B) {
			b.ReportAllocs()
			var sz int
			for i := 0; i < b.N; i++ {
				out, err := buildCRoaringBytes(v, false)
				if err != nil {
					b.Fatal(err)
				}
				sz = len(out)
			}
			b.ReportMetric(float64(sz), "bytes")
		})
		b.Run("runopt/"+name, func(b *testing.B) {
			b.ReportAllocs()
			var sz int
			for i := 0; i < b.N; i++ {
				out, err := buildCRoaringBytes(v, true)
				if err != nil {
					b.Fatal(err)
				}
				sz = len(out)
			}
			b.ReportMetric(float64(sz), "bytes")
		})
		v.Free(mp)
	}
}

// BenchmarkTestVectorRunOpt compares probing a block of keys against a plain
// vs run-optimized CRoaring filter (clustered data). Shows whether converting
// containers to run-length encoding changes membership-probe latency.
func BenchmarkTestVectorRunOpt(b *testing.B) {
	mp := mpool.MustNewZero()
	const probeRows = 8192
	cases := []struct {
		runs, runLen int
	}{
		{1, 1000000},
		{100, 10000},
		{10000, 100},
	}
	for _, tc := range cases {
		v := makeClusteredVec(b, mp, tc.runs, tc.runLen)
		maxKey := int64(tc.runs) * int64(2*tc.runLen)
		step := maxKey / int64(probeRows)
		if step < 1 {
			step = 1
		}
		// Spread probes across the whole range so many containers are touched
		// (≈50% hit, since runs occupy ~half the range).
		probe := vector.NewVec(types.T_int64.ToType())
		for i := 0; i < probeRows; i++ {
			if err := vector.AppendFixed(probe, int64(i)*step, false, mp); err != nil {
				b.Fatal(err)
			}
		}
		name := fmt.Sprintf("runs=%d/len=%d", tc.runs, tc.runLen)

		plain, err := NewCRoaringFilter(must(buildCRoaringBytes(v, false)))
		if err != nil {
			b.Fatal(err)
		}
		runopt, err := NewCRoaringFilter(must(buildCRoaringBytes(v, true)))
		if err != nil {
			b.Fatal(err)
		}

		b.Run("plain/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				plain.TestVector(probe, nil)
			}
		})
		b.Run("runopt/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				runopt.TestVector(probe, nil)
			}
		})

		plain.Free()
		runopt.Free()
		probe.Free(mp)
		v.Free(mp)
	}
}

// BenchmarkCbitmapOffset compares building a dense cbitmap without vs with the
// base offset for high-but-narrow id sets (fixed count, increasing base, span
// fixed at 2*count). Without offset the size is base/8 (grows with the absolute
// id); with offset it is span/8 (stays tiny). Shows the size + build win.
func BenchmarkCbitmapOffset(b *testing.B) {
	mp := mpool.MustNewZero()
	const count = 1000 // span = 2*count = 2000, regardless of base
	for _, base := range []int64{1_000_000, 10_000_000, 100_000_000} {
		v := vector.NewVec(types.T_int64.ToType())
		for i := 0; i < count; i++ {
			if err := vector.AppendFixed(v, base+int64(i*2), false, mp); err != nil {
				b.Fatal(err)
			}
		}
		name := fmt.Sprintf("base=%d/count=%d", base, count)

		b.Run("off/"+name, func(b *testing.B) {
			b.ReportAllocs()
			var sz int
			for i := 0; i < b.N; i++ {
				out, ok, err := buildCbitmapBytesCap(v, benchCbitmapCap, false)
				if err != nil || !ok {
					b.Fatalf("ok=%v err=%v", ok, err)
				}
				sz = len(out)
			}
			b.ReportMetric(float64(sz), "bytes")
		})
		b.Run("on/"+name, func(b *testing.B) {
			b.ReportAllocs()
			var sz int
			for i := 0; i < b.N; i++ {
				out, ok, err := buildCbitmapBytesCap(v, benchCbitmapCap, true)
				if err != nil || !ok {
					b.Fatalf("ok=%v err=%v", ok, err)
				}
				sz = len(out)
			}
			b.ReportMetric(float64(sz), "bytes")
		})
		v.Free(mp)
	}
}

// BenchmarkTestVectorOffset compares probing a block of keys against a cbitmap
// built without vs with the base offset (high-but-narrow set). Probes are
// localized near the candidate window, so this confirms the offset doesn't slow
// the probe (the tiny offset bitmap is L1-resident; the large value-indexed one
// is only touched in one region).
func BenchmarkTestVectorOffset(b *testing.B) {
	mp := mpool.MustNewZero()
	const count = 1000
	const probeRows = 8192
	for _, base := range []int64{1_000_000, 10_000_000, 100_000_000} {
		v := vector.NewVec(types.T_int64.ToType())
		for i := 0; i < count; i++ {
			if err := vector.AppendFixed(v, base+int64(i*2), false, mp); err != nil {
				b.Fatal(err)
			}
		}
		// probe spans [base, base+probeRows): even offsets < 2*count hit, rest miss.
		probe := vector.NewVec(types.T_int64.ToType())
		for i := 0; i < probeRows; i++ {
			if err := vector.AppendFixed(probe, base+int64(i), false, mp); err != nil {
				b.Fatal(err)
			}
		}
		name := fmt.Sprintf("base=%d", base)

		dataOff, ok, err := buildCbitmapBytesCap(v, benchCbitmapCap, false)
		if err != nil || !ok {
			b.Fatalf("off ok=%v err=%v", ok, err)
		}
		off, err := NewCbitmapFilter(dataOff)
		if err != nil {
			b.Fatal(err)
		}
		dataOn, ok, err := buildCbitmapBytesCap(v, benchCbitmapCap, true)
		if err != nil || !ok {
			b.Fatalf("on ok=%v err=%v", ok, err)
		}
		on, err := NewCbitmapFilter(dataOn)
		if err != nil {
			b.Fatal(err)
		}

		b.Run("off/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				off.TestVector(probe, nil)
			}
		})
		b.Run("on/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				on.TestVector(probe, nil)
			}
		})

		off.Free()
		on.Free()
		v.Free(mp)
		probe.Free(mp)
	}
}
