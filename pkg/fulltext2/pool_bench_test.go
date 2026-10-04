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
	"bytes"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// benchWandSegment builds a loaded segment with `nterms` distinct terms Zipf-ish
// spread across `ndocs` docs (so several terms span many blocks), then returns the
// deserialized (mmap-shaped) segment the query path actually runs against.
func benchWandSegment(b *testing.B, ndocs, nterms int) (*Segment, []clause) {
	b.Helper()
	terms := make([]string, nterms)
	for i := range terms {
		terms[i] = "term" + string(rune('a'+i%26)) + string(rune('0'+i/26))
	}
	rng := rand.New(rand.NewSource(11))
	bld := NewBuilder("bench", int32(types.T_int64))
	for d := 0; d < ndocs; d++ {
		pos := int32(0)
		// each doc gets a handful of terms; earlier terms are more common (block spread).
		for w := 0; w < 6; w++ {
			ti := int(rng.ExpFloat64()*float64(nterms)/4) % nterms
			if ti < 0 {
				ti = -ti
			}
			if e := bld.Add(terms[ti], pos, int64(d)); e != nil {
				b.Fatal(e)
			}
			pos += int32(len(terms[ti])) + 1
		}
	}
	seg, err := bld.Finish()
	if err != nil {
		b.Fatal(err)
	}
	blob, err := seg.Serialize()
	if err != nil {
		b.Fatal(err)
	}
	loaded, err := Deserialize("bench", bytes.NewReader(blob))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = loaded.dict.Close() })

	// a 4-term disjunction (4 wandIters => 4 pooled block-buffer pairs per query).
	cs := []clause{
		{kind: clauseTerm, terms: []string{terms[0]}, weight: 1},
		{kind: clauseTerm, terms: []string{terms[1]}, weight: 1},
		{kind: clauseTerm, terms: []string{terms[2]}, weight: 1},
		{kind: clauseTerm, terms: []string{terms[3]}, weight: 1},
	}
	return loaded, cs
}

// BenchmarkSearchWANDAlloc reports allocs/op for the full searchWAND path. With the
// sync.Pool for cursor block buffers, the per-query BlockSize []int64/[]uint8 pairs are
// recycled, so they do NOT show up as steady-state allocations.
func BenchmarkSearchWANDAlloc(b *testing.B) {
	seg, cs := benchWandSegment(b, 20000, 40)
	gs := (&Index{segments: []*Segment{seg}, globalN: seg.N}).newGlobalStats()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = seg.searchWAND(cs, BM25, 10, nil, gs)
	}
}

// BenchmarkSearchWANDParallel exercises the pool under concurrency — the case where
// per-query buffer churn would otherwise outrun the GC. -race over this proves the
// Get/Put lifecycle has no cross-goroutine buffer sharing.
func BenchmarkSearchWANDParallel(b *testing.B) {
	seg, cs := benchWandSegment(b, 20000, 40)
	gs := (&Index{segments: []*Segment{seg}, globalN: seg.N}).newGlobalStats()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = seg.searchWAND(cs, BM25, 10, nil, gs)
		}
	})
}

// BenchmarkStreamAlloc reports allocs/op for the no-LIMIT streaming path. With typed
// ColumnBuffer the per-pk boxing is gone: a batch allocates only its Data/scores buffers
// (a few allocs per 8192 rows) instead of one boxed interface per doc.
func BenchmarkStreamAlloc(b *testing.B) {
	seg, cs := benchWandSegment(b, 20000, 40)
	idx := NewIndex([]*Segment{seg}, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink := newStreamSink(idx, false, nil, func(o *vectorindex.SearchOutput) error { PutColumnBuffer(o.Keys); return nil })
		_ = idx.streamDisjunction(cs, BM25, nil, sink)
	}
}
