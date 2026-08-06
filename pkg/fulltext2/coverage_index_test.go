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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestSortKey pins the injective tie-break key for every pk shape: []byte/string pass through,
// the temporal types key by their underlying int64 (so DATETIME(6) sub-second precision is not
// truncated), and everything else falls to %v.
func TestSortKey(t *testing.T) {
	require.Equal(t, "raw", sortKey([]byte("raw")))
	require.Equal(t, "str", sortKey("str"))
	require.Equal(t, strconv.FormatInt(9_000_001, 10), sortKey(types.Datetime(9_000_001)))
	require.Equal(t, strconv.FormatInt(42, 10), sortKey(types.Time(42)))
	require.Equal(t, strconv.FormatInt(7, 10), sortKey(types.Timestamp(7)))
	require.Equal(t, "123", sortKey(int64(123))) // default %v
}

// TestBoundedTopK covers the min-heap bounded top-k: it retains only the k highest scores, a
// push below the current k-th best is dropped, and k<=0 is a no-op.
func TestBoundedTopK(t *testing.T) {
	b := newBoundedTopK(2)
	b.push("a", 1.0, nil)
	b.push("b", 3.0, nil)
	require.Equal(t, 2, b.len())
	b.push("c", 2.0, nil) // evicts "a" (1.0), keeps {b:3, c:2}
	b.push("d", 0.5, nil) // below the k-th best → dropped
	require.Equal(t, 2, b.len())

	res := b.resultsDescScaled(1.0)
	require.Len(t, res, 2)
	require.Equal(t, "b", res[0].Pk) // score-descending
	require.Equal(t, "c", res[1].Pk)
	require.Greater(t, res[0].Score, res[1].Score)

	// scale multiplies the retained partial scores.
	scaled := b.resultsDescScaled(2.0)
	require.Equal(t, float32(6.0), scaled[0].Score)

	// k<=0 → push is a no-op.
	z := newBoundedTopK(0)
	z.push("x", 5.0, nil)
	require.Equal(t, 0, z.len())
}

// TestTopKResults covers the FastMaxHeap selection: empty/k<=0 → nil, k>len is clamped, and the
// output is score-descending.
func TestTopKResults(t *testing.T) {
	require.Nil(t, topKResults(nil, 3))
	require.Nil(t, topKResults([]Result{{Pk: "a", Score: 1}}, 0))

	in := []Result{{Pk: "a", Score: 1}, {Pk: "b", Score: 3}, {Pk: "c", Score: 2}}
	out := topKResults(in, 2)
	require.Len(t, out, 2)
	require.Equal(t, "b", out[0].Pk) // highest first
	require.Equal(t, "c", out[1].Pk)

	// k > len is clamped to len.
	all := topKResults(in, 10)
	require.Len(t, all, 3)
	require.Equal(t, "b", all[0].Pk)
}
