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
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestConcurrentSearchQuery is the concurrency contract behind Fulltext2Search /
// VectorIndexCache: a single loaded Index is shared (RW-locked) across concurrent
// queries, so SearchQuery must be pure-read — no mutation of segments, the FST, the
// loaded posting slices, or the liveness maps. It builds a loaded (serialized ->
// deserialized, i.e. FST + materialized `loaded` path) Index and hammers it from
// many goroutines with NL-phrase, boolean, and prefix queries at once, asserting
// every goroutine gets the SAME result as a single-threaded baseline. Run under
// -race, a shared write on the query path trips the detector.
func TestConcurrentSearchQuery(t *testing.T) {
	// A loaded multi-segment Index (base + a tail delta) so liveness/global-stats and
	// both the FST get() and the prefix Iterator() paths are exercised concurrently.
	bb := NewBuilder("base", int32(types.T_int64))
	feed(t, bb, int64(0), "quick", "brown", "fox")
	feed(t, bb, int64(1), "quick", "brown", "dog")
	feed(t, bb, int64(2), "lazy", "fox", "sleeps")
	base := loadedSeg(t, bb)
	base.Recency = 0

	tb := NewBuilder("tail", int32(types.T_int64))
	feed(t, tb, int64(3), "brown", "bear", "lazy")
	feed(t, tb, int64(4), "quick", "quick", "fox")
	tail := loadedSeg(t, tb)
	tail.Recency = 1

	idx := NewIndex([]*Segment{base, tail}, nil)

	type query struct {
		pattern string
		boolean bool
	}
	queries := []query{
		{"quick brown fox", false}, // NL exact phrase (FST get)
		{"fox", false},             // single term
		{"+quick +fox", true},      // boolean AND
		{"+quick -fox", true},      // boolean AND-NOT
		{"quic*", true},            // boolean prefix (FST Iterator)
		{"lazy", false},            // term hitting both segments
	}

	// Single-threaded baseline: the canonical (pk-set) answer for each query.
	baseline := make([]map[any]struct{}, len(queries))
	for i, q := range queries {
		res, err := idx.SearchQuery([]byte(q.pattern), q.boolean, ParserDefault, BM25, 100)
		require.NoError(t, err, "baseline %q", q.pattern)
		m := make(map[any]struct{}, len(res))
		for _, r := range res {
			m[r.Pk] = struct{}{}
		}
		baseline[i] = m
	}

	const goroutines, iters = 24, 200
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := 0; it < iters; it++ {
				q := queries[it%len(queries)]
				res, err := idx.SearchQuery([]byte(q.pattern), q.boolean, ParserDefault, BM25, 100)
				if err != nil {
					errCh <- fmt.Errorf("query %q: %w", q.pattern, err)
					return
				}
				want := baseline[it%len(queries)]
				if len(res) != len(want) {
					errCh <- fmt.Errorf("query %q: got %d hits, want %d", q.pattern, len(res), len(want))
					return
				}
				for _, r := range res {
					if _, ok := want[r.Pk]; !ok {
						errCh <- fmt.Errorf("query %q: unexpected pk %v", q.pattern, r.Pk)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}
