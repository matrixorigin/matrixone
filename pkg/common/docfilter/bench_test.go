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

// benchSizes are candidate-set cardinalities N (the number of doc_ids the
// filter is built from). The whole point of the bitset is cheaper build at
// large N, so we sweep a range.
var benchSizes = []int{100000, 1000000, 10000000}

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

// mustCbitmap builds + serializes a dense cbitmap, failing the bench if the id
// range is not feasible (the bench keys are dense, so it always should be).
func mustCbitmap(tb testing.TB, v *vector.Vector) []byte {
	out, ok, err := BuildCbitmapBytes(v)
	if err != nil || !ok {
		tb.Fatalf("build cbitmap: ok=%v err=%v", ok, err)
	}
	return out
}

// BenchmarkBuild compares building + serializing a CBloomFilter vs a roaring64
// bitset from the same N integer doc_ids. Reports the serialized size too.
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

		b.Run(fmt.Sprintf("bitset/N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			var sz int
			for i := 0; i < b.N; i++ {
				bm := BuildBitset(keyvec)
				out, err := MarshalBitset(bm)
				if err != nil {
					b.Fatal(err)
				}
				sz = len(out)
			}
			b.ReportMetric(float64(sz), "bytes")
		})

		// cbitmap: a DENSE bitset indexed by the doc_id value, built + serialized
		// in C directly from the column buffer (one cgo call). Sized to the max
		// key (here ~2N), so size = O(max value), not O(N) — only viable for
		// dense/bounded integer PKs. Keys are odd values in [1, 2N).
		b.Run(fmt.Sprintf("cbitmap/N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			var sz int
			for i := 0; i < b.N; i++ {
				out := mustCbitmap(b, keyvec)
				sz = len(out)
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

// BenchmarkTestVector compares probing a block of doc_ids (50% hit) against a
// prebuilt CBloomFilter vs roaring64 bitset.
func BenchmarkTestVector(b *testing.B) {
	mp := mpool.MustNewZero()
	const probeRows = 8192
	for _, n := range benchSizes {
		keyvec := makeInt64Vec(b, mp, n)
		probe := makeProbeVec(b, mp, probeRows)

		bf := bloomfilter.NewCBloomFilterWithProbability(int64(n), benchFpProbability)
		bf.AddVector(keyvec)
		rf := &RoaringFilter{bm: BuildBitset(keyvec)}
		cbf, err := NewCbitmapFilter(mustCbitmap(b, keyvec))
		if err != nil {
			b.Fatal(err)
		}
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
		b.Run(fmt.Sprintf("bitset/N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rf.TestVector(probe, nil)
			}
		})
		b.Run(fmt.Sprintf("cbitmap/N=%d", n), func(b *testing.B) {
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
		cbf.Free()
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
		rf := &RoaringFilter{bm: BuildBitset(keyvec)}
		cbf, err := NewCbitmapFilter(mustCbitmap(b, keyvec))
		if err != nil {
			b.Fatal(err)
		}
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
		b.Run(fmt.Sprintf("bitset/N=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, r := range raws {
					_ = rf.Test(r)
				}
			}
		})
		b.Run(fmt.Sprintf("cbitmap/N=%d", n), func(b *testing.B) {
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
		cbf.Free()
		crf.Free()
		keyvec.Free(mp)
		probe.Free(mp)
	}
}
