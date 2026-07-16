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

package fulltext2

import (
	"math/rand"
	"testing"
)

func benchResults(n, buckets int) []Result {
	rng := rand.New(rand.NewSource(7))
	rs := make([]Result, n)
	for i := range rs {
		rs[i] = Result{Pk: int64(rng.Intn(1 << 30)), Score: float64(rng.Intn(buckets))}
	}
	return rs
}

// n=45688 k=10 is the observed 'the' whole-corpus stopword shape. The FastMaxHeap
// selection is O(n) with zero per-candidate allocation regardless of the score
// distribution — Distinct (varied BM25 scores) and Tied (all-identical) both stay flat,
// unlike the old bounded-min-heap that allocated a sortKey string per candidate.
func BenchmarkTopK_Distinct(b *testing.B) {
	rs := benchResults(45688, 45688)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topKResults(rs, 10)
	}
}
func BenchmarkTopK_Tied(b *testing.B) {
	rs := benchResults(45688, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topKResults(rs, 10)
	}
}
