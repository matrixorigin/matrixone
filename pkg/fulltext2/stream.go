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
	"encoding/binary"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// streamBatch is the max rows a streamSink buffers before flushing to emit
// (matches bm25's streamBatch).
const streamBatch = 8192

// streamSink batches (pk, score) results into a TYPED, box-free vectorindex.ColumnBuffer
// and flushes to emit in bounded chunks, so a no-LIMIT query returns every matching doc
// WITHOUT materializing them all and WITHOUT boxing a pk per doc. On an emit error it
// records it and stops (the walk checks stopped and bails).
type streamSink struct {
	emit    func(keys *vectorindex.ColumnBuffer, distances []float64) error
	keys    vectorindex.ColumnBuffer
	scores  []float64
	fixedW  int // fixed pk width; 0 ⇒ varlena. Cached from keys.Type.
	err     error
	stopped bool
}

// newStreamSink resolves the index's pk type (all segments share it) into the typed
// batch. Callers create it only after the globalN==0 early-out, so segments is non-empty.
func newStreamSink(idx *Index, emit func(keys *vectorindex.ColumnBuffer, distances []float64) error) *streamSink {
	pkType := idx.segments[0].PkType
	w, fixed := fixedPkByteWidth(pkType)
	if !fixed {
		w = 0
	}
	return &streamSink{emit: emit, keys: vectorindex.ColumnBuffer{Type: types.T(pkType)}, fixedW: w}
}

// pushPk appends the pk at (seg, ord) to the typed batch WITHOUT boxing, decoding it
// straight from the segment docmap into ColumnBuffer.Data.
func (s *streamSink) pushPk(seg *Segment, ord int64, score float32) {
	if s.stopped {
		return
	}
	seg.appendPkTo(&s.keys, s.fixedW, ord)
	s.scores = append(s.scores, float64(score))
	if s.keys.N >= streamBatch {
		s.flush()
	}
}

// pushAny appends an already-boxed pk (the materialized-fallback path: NL phrase / full
// boolean) by re-encoding its concrete value into ColumnBuffer.Data — no NEW box.
func (s *streamSink) pushAny(pk any, score float32) {
	if s.stopped {
		return
	}
	appendAnyPk(&s.keys, s.fixedW, pk)
	s.scores = append(s.scores, float64(score))
	if s.keys.N >= streamBatch {
		s.flush()
	}
}

func (s *streamSink) flush() {
	if s.stopped || s.keys.N == 0 {
		return
	}
	// Hand THIS batch to emit as a distinct object (a struct copy: the Data backing is
	// shared, but the sink then starts a fresh batch so it never mutates the emitted one).
	batch := s.keys
	if e := s.emit(&batch, s.scores); e != nil {
		s.err = e
		s.stopped = true
		return
	}
	s.keys = vectorindex.ColumnBuffer{Type: batch.Type} // fresh Data on next append
	s.scores = nil
}

// appendPkTo decodes the pk at ord from seg's docmap into k.Data with no boxing: a
// fixed-width pk contributes its width bytes; a varlena pk its [u32 len][content] entry.
func (s *Segment) appendPkTo(k *vectorindex.ColumnBuffer, fixedW int, ord int64) {
	if s.pks != nil { // build-side segment: re-encode the already-boxed value
		appendAnyPk(k, fixedW, s.pks[ord])
		return
	}
	off := int(s.pkOffsets[ord])
	l := int(binary.LittleEndian.Uint32(s.pkRaw[off:]))
	if fixedW > 0 {
		k.Data = append(k.Data, s.pkRaw[off+4:off+4+fixedW]...) // width bytes, no len prefix
	} else {
		k.Data = append(k.Data, s.pkRaw[off:off+4+l]...) // [u32 len][content] verbatim
	}
	k.N++
}

// streamWAND does a heap-free document-at-a-time OR merge over one segment: it visits
// every matching doc in ord order, scores it (over the SHOULD terms present, on the
// GLOBAL corpus stats), and pushes it to the sink — no top-K heap, no WAND pruning,
// no full-result materialization. allow (liveness ∧ WHERE prefilter) drops
// non-admissible docs. The no-LIMIT case wants every match; ranking is done by the
// upstream ORDER BY score node. Mirrors bm25's streamInto.
func (s *Segment) streamWAND(clauses []clause, algo ScoreAlgo, gs *globalStats, allow Membership, sink *streamSink) {
	avgDocLen := gs.avgdl(s)
	iters := s.buildWandIters(clauses, algo, gs, avgDocLen)
	defer releaseWandIters(iters) // recycle the per-cursor block buffers
	if len(iters) == 0 {
		return
	}
	for !sink.stopped {
		minDoc := int64(math.MaxInt64)
		for _, it := range iters {
			if d := it.doc(); d < minDoc {
				minDoc = d
			}
		}
		if minDoc == math.MaxInt64 {
			break // all cursors exhausted
		}
		if allowed(allow, minDoc) {
			var score float32
			for _, it := range iters {
				if it.doc() == minDoc {
					// tf-aware, same as searchWAND / evalClause (fulltext2 keeps real tf).
					score += it.weight * s.scoreTerm(algo, float64(it.tf()), it.idf2, minDoc, avgDocLen)
				}
			}
			sink.pushPk(s, minDoc, score)
		}
		for _, it := range iters {
			if it.doc() == minDoc {
				it.idx++
				it.refresh() // keep the cached doc() in sync with idx
			}
		}
	}
}

// StreamQuery answers a no-LIMIT MATCH by streaming every matching (pk, score) to emit
// in bounded batches instead of materializing the whole result set (the ranking is
// done by the upstream ORDER BY score node). A pure disjunction of single terms — the
// ranked-retrieval shape — streams heap-free via streamWAND across all segments
// (global stats + per-segment liveness). Any other shape (NL phrase, a full boolean
// with MUST/MUST-NOT/phrase) has an intrinsically materialized candidate set, so it
// runs the normal search unbounded and pushes the result through the same sink — a
// uniform emit interface, and still no separate paginated copy in the caller.
func (idx *Index) StreamQuery(pattern []byte, boolean bool, parser string, algo ScoreAlgo, filter docfilter.MembershipFilter, emit func(keys *vectorindex.ColumnBuffer, distances []float64) error) error {
	if idx.globalN == 0 {
		return nil
	}
	sink := newStreamSink(idx, emit)
	p := normalizeParser(parser)

	// Resolve the query the same way SearchQuery does, but prefer the streaming
	// disjunctive path when the resolved query is a pure OR of single terms.
	var q BoolQuery
	var haveQ bool
	pat := string(pattern)
	if boolean {
		var err error
		if q, err = buildBooleanQuery(pat, p); err != nil {
			return err
		}
		haveQ = true
	}
	// NL mode (haveQ stays false) is an exact positional phrase, not a pure disjunction,
	// so it falls through to the materializing SearchQuery below (same phrase path).

	if haveQ {
		if terms, ok := disjunctiveTerms(q); ok {
			return idx.streamDisjunction(terms, algo, filter, sink)
		}
	}

	// Non-disjunctive: fall back to the materializing search (unbounded k) and push its
	// results through the sink. The candidate set is intrinsic to these query shapes.
	results, err := idx.SearchQuery(pattern, boolean, parser, algo, int(idx.globalN), filter)
	if err != nil {
		return err
	}
	for _, r := range results {
		sink.pushAny(r.Pk, r.Score)
		if sink.err != nil {
			return sink.err
		}
	}
	sink.flush()
	return sink.err
}

// StreamBagOfWords is the no-LIMIT IN BM25 MODE path: it tokenizes the pattern into a
// pure disjunction of its tokens (bm25-style bag-of-words) and streams the ranked hits
// heap-free via streamWAND — the position-free analogue of StreamQuery's disjunctive
// branch, but a CJK run is tokenized to OR terms instead of a positional phrase, so it
// works on a POSITION_FREE index.
func (idx *Index) StreamBagOfWords(pattern []byte, parser string, algo ScoreAlgo, filter docfilter.MembershipFilter, emit func(keys *vectorindex.ColumnBuffer, distances []float64) error) error {
	if idx.globalN == 0 {
		return nil
	}
	q, err := buildBagOfWordsQuery(string(pattern), normalizeParser(parser))
	if err != nil {
		return err
	}
	terms, ok := disjunctiveTerms(q)
	if !ok {
		return nil // no resolvable tokens → nothing to stream
	}
	return idx.streamDisjunction(terms, algo, filter, newStreamSink(idx, emit))
}

// streamDisjunction streams a pure OR of terms heap-free across all segments (global
// corpus stats + per-segment liveness), pushing every (pk, score) into sink and
// flushing at the end. Shared by StreamQuery's boolean disjunctive branch and
// StreamBagOfWords' IN BM25 MODE path — a change to the disjunctive walk lands once.
func (idx *Index) streamDisjunction(terms []clause, algo ScoreAlgo, filter docfilter.MembershipFilter, sink *streamSink) error {
	gs := idx.newGlobalStats()
	for si, seg := range idx.segments {
		allow := andAllow(mkAllow(seg, filter), &livenessMembership{idx: idx, si: si})
		seg.streamWAND(terms, algo, gs, allow, sink)
		if sink.err != nil {
			return sink.err
		}
	}
	sink.flush()
	return sink.err
}
