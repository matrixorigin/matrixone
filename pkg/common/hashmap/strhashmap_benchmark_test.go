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

package hashmap

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var benchmarkStrIterator Iterator

// These benchmarks call only the production iterator API so the same file can
// be run unchanged on the exact base and PR head. Do not emulate the old
// iterator layout in a helper: its inline scratch fields affect allocation
// counts and are part of the baseline being measured.
func BenchmarkNewStrHashIterator(b *testing.B) {
	mp := mpool.MustNewZero()
	hashMap, err := NewStrHashMap(false, mp)
	if err != nil {
		b.Fatal(err)
	}
	defer hashMap.Free()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkStrIterator = hashMap.NewIterator()
	}
}

func BenchmarkStrHashIteratorFirstFind(b *testing.B) {
	for _, count := range []int{1, 2, 8, 16, 256} {
		b.Run(fmt.Sprintf("rows-%d", count), func(b *testing.B) {
			mp := mpool.MustNewZero()
			hashMap, err := NewStrHashMap(false, mp)
			if err != nil {
				b.Fatal(err)
			}
			defer hashMap.Free()
			vec := newVector(count, types.T_varchar.ToType(), mp, false, nil)
			defer vec.Free(mp)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				itr := hashMap.NewIterator()
				if _, _, err := itr.Find(0, count, []*vector.Vector{vec}); err != nil {
					b.Fatal(err)
				}
				benchmarkStrIterator = itr
			}
		})
	}
}

func BenchmarkStrHashIteratorReuseFind(b *testing.B) {
	for _, count := range []int{1, 2, 8, 16, 256} {
		b.Run(fmt.Sprintf("rows-%d", count), func(b *testing.B) {
			mp := mpool.MustNewZero()
			hashMap, err := NewStrHashMap(false, mp)
			if err != nil {
				b.Fatal(err)
			}
			defer hashMap.Free()
			vec := newVector(count, types.T_varchar.ToType(), mp, false, nil)
			defer vec.Free(mp)
			itr := hashMap.NewIterator()
			if _, _, err := itr.Find(0, count, []*vector.Vector{vec}); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := itr.Find(0, count, []*vector.Vector{vec}); err != nil {
					b.Fatal(err)
				}
			}
			benchmarkStrIterator = itr
		})
	}
}
