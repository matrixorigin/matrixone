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
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTopKResultsValidTopK fuzzes topKResults over many sizes, k values, and score
// distributions (including heavy ties). The contract is deliberately NOT a byte-for-byte
// match of a full sort — equal scores are equally relevant and their order is
// unspecified. It IS: (1) length == min(k, n); (2) output ordered by score descending;
// (3) a *valid* top-k — no returned doc scores below any doc that was left out. That is
// exactly "correct top-k by score, arbitrary tie-break".
func TestTopKResultsValidTopK(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, scoreBuckets := range []int{1, 2, 5, 50, 100000} {
		for _, n := range []int{0, 1, 2, 3, 7, 8, 9, 33, 200, 1000} {
			for _, k := range []int{0, 1, 2, 8, 10, n - 1, n, n + 5} {
				results := make([]Result, n)
				for i := range results {
					results[i] = Result{
						Pk:    int64(rng.Intn(1 << 30)),
						Score: float32(rng.Intn(scoreBuckets)),
					}
				}
				got := topKResults(results, k)

				wantLen := k
				if wantLen > n {
					wantLen = n
				}
				if k <= 0 {
					wantLen = 0
				}
				require.Lenf(t, got, wantLen, "n=%d k=%d buckets=%d", n, k, scoreBuckets)

				// (2) ordered by score descending.
				for i := 1; i < len(got); i++ {
					require.GreaterOrEqualf(t, got[i-1].Score, got[i].Score,
						"not score-desc at %d (n=%d k=%d buckets=%d)", i, n, k, scoreBuckets)
				}
				if len(got) == 0 {
					continue
				}
				// (3) valid top-k: the minimum returned score is >= the k-th largest
				// score overall, i.e. nothing better than a returned doc was dropped.
				allScores := make([]float64, n)
				for i := range results {
					allScores[i] = float64(results[i].Score)
				}
				sort.Sort(sort.Reverse(sort.Float64Slice(allScores)))
				kth := allScores[len(got)-1] // k-th largest overall (got has min(k,n) items)
				minReturned := math.Inf(1)
				for _, r := range got {
					if float64(r.Score) < minReturned {
						minReturned = float64(r.Score)
					}
				}
				require.GreaterOrEqualf(t, minReturned, kth,
					"dropped a higher-scoring doc (n=%d k=%d buckets=%d)", n, k, scoreBuckets)
			}
		}
	}
}
