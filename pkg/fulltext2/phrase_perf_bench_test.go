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

// Perf follow-up Phase 0 — BASELINE measurement (no engine change) for the two phrase
// memory concerns:
//
//	A. pooled phrase-buffer position retention / per-block make() churn
//	   (segment.go fillBlockPositions), surfaced under concurrent phrase load.
//	B. no-LIMIT phrase materializing globalN results (stream.go StreamQuery falls back
//	   to SearchQuery(globalN)) vs the disjunctive path that streams heap-free.
package fulltext2

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// benchPhraseIndex builds a LOADED (serialize→deserialize, so the phrase cursor takes
// the posRaw/fillBlockPositions decode path, not the build-side flat view) single-segment
// index of nDocs docs, each holding `repeats` copies of the phrase "alpha beta" — so every
// term has `repeats` positions per doc (drives the per-doc position arrays) and the phrase
// matches every doc.
func benchPhraseIndex(b *testing.B, nDocs, repeats int) *Index {
	b.Helper()
	bb := NewBuilder("bench", int32(types.T_int64))
	for d := 0; d < nDocs; d++ {
		pos := int32(0)
		pk := int64(d)
		for r := 0; r < repeats; r++ {
			if err := bb.Add("alpha", pos, pk); err != nil {
				b.Fatal(err)
			}
			pos += 6
			if err := bb.Add("beta", pos, pk); err != nil {
				b.Fatal(err)
			}
			pos += 5
		}
	}
	seg, err := bb.Finish()
	if err != nil {
		b.Fatal(err)
	}
	blob, err := seg.Serialize()
	if err != nil {
		b.Fatal(err)
	}
	loaded, err := Deserialize(seg.Id, bytes.NewReader(blob))
	if err != nil {
		b.Fatal(err)
	}
	return NewIndex([]*Segment{loaded}, nil)
}

// BenchmarkPhraseSearchConcurrent — concern A. Repeated NL-phrase top-k under parallel
// load; the alloc/op figure captures the phrase-cursor position churn + pooled-buffer
// behavior. High `repeats` ⇒ long per-doc position lists.
func BenchmarkPhraseSearchConcurrent(b *testing.B) {
	idx := benchPhraseIndex(b, 2000, 60)
	pat := []byte("alpha beta")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res, err := idx.SearchQuery(pat, false /*NL phrase*/, ParserDefault, BM25, 10, nil)
			if err != nil {
				b.Fatal(err)
			}
			_ = res
		}
	})
}

// BenchmarkPhraseSparseIntersection isolates doc alignment. Alpha occurs only in even
// docs and beta only in odd docs, so the two large posting lists never share a doc and
// no positional block should be decoded by the lazy phrase cursor.
func BenchmarkPhraseSparseIntersection(b *testing.B) {
	bb := NewBuilder("phrase-sparse", int32(types.T_int64))
	for d := 0; d < 50000; d++ {
		term := "alpha"
		if d&1 != 0 {
			term = "beta"
		}
		if err := bb.Add(term, 0, int64(d)); err != nil {
			b.Fatal(err)
		}
	}
	seg, err := bb.Finish()
	if err != nil {
		b.Fatal(err)
	}
	blob, err := seg.Serialize()
	if err != nil {
		b.Fatal(err)
	}
	loaded, err := Deserialize("phrase-sparse", bytes.NewReader(blob))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = loaded.dict.Close() })
	idx := NewIndex([]*Segment{loaded}, nil)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := idx.SearchQuery([]byte("alpha beta"), false, ParserDefault, BM25, 10, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(res) != 0 {
			b.Fatalf("expected no matches, got %d", len(res))
		}
	}
}

// BenchmarkPhraseStreamNoLimit — concern B, the materializing fallback: a no-LIMIT NL
// phrase that matches ALL docs, so StreamQuery goes through SearchQuery(globalN).
func BenchmarkPhraseStreamNoLimit(b *testing.B) {
	idx := benchPhraseIndex(b, 20000, 3)
	pat := []byte("alpha beta")
	emit := func(o *vectorindex.SearchOutput) error {
		PutColumnBuffer(o.Keys) // recycle like the real consumer
		return nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := idx.StreamQuery(pat, false /*NL phrase → materializing fallback*/, ParserDefault, BM25, nil, false, emit); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDisjunctionStreamNoLimit — concern B CONTROL: the SAME pattern + corpus as a
// boolean OR (disjunctive), which streams heap-free (streamWAND, no globalN slice). The
// gap between this and BenchmarkPhraseStreamNoLimit is the materialization cost Phase 2
// targets.
func BenchmarkDisjunctionStreamNoLimit(b *testing.B) {
	idx := benchPhraseIndex(b, 20000, 3)
	pat := []byte("alpha beta")
	emit := func(o *vectorindex.SearchOutput) error {
		PutColumnBuffer(o.Keys)
		return nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := idx.StreamQuery(pat, true /*boolean OR → heap-free stream*/, ParserDefault, BM25, nil, false, emit); err != nil {
			b.Fatal(err)
		}
	}
}
