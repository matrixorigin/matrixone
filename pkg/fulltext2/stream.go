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
	emit   func(out *vectorindex.SearchOutput) error
	keys   *vectorindex.ColumnBuffer // pooled; nil until the first push after a flush
	pkType types.T
	scores []float32
	// includeCols is COLUMN-MAJOR: one nullable ColumnBuffer per FULL index INCLUDE column
	// (segment-include order), each covering the whole batch — the box-free analogue of the
	// old per-row [][]any. nil until the first push after a flush; len == len(includeTypes)
	// when wantInclude. Not pooled (per-batch, potentially varied types); the consumer copies
	// each out via AppendColumnBuffer and drops it.
	includeCols  []*vectorindex.ColumnBuffer
	includeTypes []int32 // FULL index INCLUDE column types (drives includeCols alloc)
	fixedW       int     // fixed pk width; 0 ⇒ varlena
	err          error
	stopped      bool
	wantInclude  bool // covered fast path: also carry each doc's INCLUDE values through emit
	// scoreRange drops a scored doc outside the pushed relevance interval before it is
	// batched, so out-of-range rows never leave the TVF. nil ⇒ keep everything.
	scoreRange *ScoreRange
}

// newStreamSink resolves the index's pk type (all segments share it). The batch buffer
// itself is fetched from the pool lazily on the first push. Callers create it only after
// the globalN==0 early-out, so segments is non-empty. wantInclude turns on the covered
// fast path: each pushed doc also carries its INCLUDE values (box-free) through emit.
func newStreamSink(idx *Index, wantInclude bool, filter *prefilter, emit func(out *vectorindex.SearchOutput) error) *streamSink {
	pkType := idx.segments[0].PkType
	w, fixed := fixedPkByteWidth(pkType)
	if !fixed {
		w = 0
	}
	s := &streamSink{emit: emit, pkType: types.T(pkType), fixedW: w, wantInclude: wantInclude}
	if filter != nil {
		s.scoreRange = filter.scoreRange
	}
	if wantInclude {
		s.includeTypes = idx.segments[0].includeTypes
	}
	return s
}

// ensure lazily fetches a pooled batch buffer of the sink's pk type, and (when the covered
// path is on) allocates one typed, nullable ColumnBuffer per FULL INCLUDE column.
func (s *streamSink) ensure() {
	if s.keys == nil {
		s.keys = GetColumnBuffer(s.pkType)
	}
	if s.wantInclude && s.includeCols == nil {
		s.includeCols = make([]*vectorindex.ColumnBuffer, len(s.includeTypes))
		for c, t := range s.includeTypes {
			s.includeCols[c] = &vectorindex.ColumnBuffer{Type: types.T(t)}
		}
	}
}

// appendIncludes appends doc (seg, ord)'s INCLUDE values to the per-column buffers box-free.
// On a malformed segment it records the error and stops the sink so no torn batch is emitted.
func (s *streamSink) appendIncludes(seg *Segment, ord int64) {
	for c := range s.includeCols {
		if err := seg.appendIncludeTo(s.includeCols[c], c, ord); err != nil {
			s.err = err
			s.stopped = true
			return
		}
	}
}

// pushPk appends the pk at (seg, ord) to the typed batch WITHOUT boxing, decoding it
// straight from the segment docmap into ColumnBuffer.Data.
func (s *streamSink) pushPk(seg *Segment, ord int64, score float32) {
	if s.stopped || !s.scoreRange.contains(score) {
		return
	}
	s.ensure()
	seg.appendPkTo(s.keys, s.fixedW, ord)
	s.scores = append(s.scores, score)
	if s.wantInclude {
		s.appendIncludes(seg, ord)
		if s.stopped {
			return
		}
	}
	if s.keys.N >= streamBatch {
		s.flush()
	}
}

// pushAny appends an already-boxed pk (the materialized-fallback path: NL phrase / full
// boolean) by re-encoding its concrete value into ColumnBuffer.Data — no NEW box. When the
// covered fast path is on, the INCLUDE values are pulled box-free from (seg, ord).
func (s *streamSink) pushAny(seg *Segment, ord int64, pk any, score float32) {
	if s.stopped || !s.scoreRange.contains(score) {
		return
	}
	s.ensure()
	appendAnyPk(s.keys, s.fixedW, pk)
	s.scores = append(s.scores, score)
	if s.wantInclude {
		s.appendIncludes(seg, ord)
		if s.stopped {
			return
		}
	}
	if s.keys.N >= streamBatch {
		s.flush()
	}
}

func (s *streamSink) flush() {
	if s.stopped || s.keys == nil || s.keys.N == 0 {
		return
	}
	// Hand the batch to the consumer as a *SearchOutput — the SAME box-free container
	// SearchInto fills on the LIMIT path, so both share one consumer (Dists is float32,
	// matching the T_float32 score column). This SearchOutput is producer-owned per batch;
	// the consumer recycles out.Keys (pooled) after copying and drops Dists/Include.
	out := &vectorindex.SearchOutput{Keys: s.keys, Dists: s.scores, Include: s.includeCols}
	if e := s.emit(out); e != nil {
		s.err = e
		s.stopped = true
		PutColumnBuffer(s.keys) // emit didn't hand it off; recycle here
		s.keys = nil
		s.includeCols = nil
		return
	}
	// Ownership of this batch passes to the consumer, which PutColumnBuffers the pk buffer
	// back once it has copied Data out (the include buffers are not pooled, just dropped);
	// the sink lazily Gets fresh ones on the next push.
	s.keys = nil
	s.scores = nil
	s.includeCols = nil
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
// done by the upstream ORDER BY score node). Every shape streams ONE SEGMENT AT A TIME
// with peak Go heap O(one segment), never O(globalN): a pure disjunction of single terms
// (the ranked-retrieval shape) streams heap-free via streamWAND; an NL phrase via
// streamPhrase; and a full boolean with MUST/MUST-NOT/mixed-phrase via streamBoolean
// (each segment's dense evalBoolean, freed before the next). All share the same sink — a
// uniform emit interface, and no separate paginated copy in the caller.
func (idx *Index) StreamQuery(pattern []byte, boolean bool, parser string, algo ScoreAlgo, filter *prefilter, wantInclude bool, emit func(out *vectorindex.SearchOutput) error) error {
	if idx.globalN == 0 {
		return nil
	}
	sink := newStreamSink(idx, wantInclude, filter, emit)
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
	} else {
		// NL mode = exact positional phrase. Stream it ONE SEGMENT AT A TIME (peak =
		// one segment's phrase matches, freed before the next) instead of the old
		// SearchQuery(globalN) fallback, whose bounded top-K sized to globalN
		// materialized the whole live corpus for a low-selectivity phrase.
		slots, err := phraseSlots(pat, p)
		if err != nil {
			return err
		}
		return idx.streamPhrase(slots, algo, filter, sink)
	}

	// Boolean, non-disjunctive (MUST / MUST-NOT / mixed phrase): stream each live match
	// through the sink one segment at a time via evalBoolean. Peak Go heap is O(one
	// segment) (bounded by its capacity), NOT O(globalN) — so a low-selectivity boolean
	// (e.g. +common on millions of rows) no longer materializes the whole live corpus as
	// a top-K sized to globalN before emitting. Ranking is the upstream ORDER BY node's.
	return idx.streamBoolean(q, algo, filter, sink)
}

// streamPhrase emits every live phrase match (pk, score) through sink WITHOUT a global
// top-K heap or a globalN result slice — the streaming analogue of SearchPhrase for the
// no-LIMIT NL path. A phrase has ONE idf² (a positive constant, so it does not change
// rank but does set the absolute score to match the LIMIT path), which needs the total
// live df, so this makes two cheap passes: pass 1 counts the live phrase df; pass 2
// scores + streams. Peak Go heap is O(one segment's matchPhrase hits) (freed before the
// next segment), NOT O(globalN). Mirrors SearchPhrase's scoring (partial × idf²) exactly.
func (idx *Index) streamPhrase(slots []phraseSlot, algo ScoreAlgo, filter *prefilter, sink *streamSink) error {
	if idx.globalN == 0 || len(slots) == 0 {
		return nil
	}
	// Pass 1: distinct live phrase df (filter-independent, matching SearchPhrase's df).
	df := 0
	for si, seg := range idx.segments {
		for _, h := range seg.matchPhrase(slots) {
			if idx.isLive(si, h.ord) {
				df++
			}
		}
	}
	if df == 0 {
		return nil
	}
	idf2 := float32(idfSquared(idx.globalN, df))

	// Pass 2: score + stream each live, WHERE-admitted match in doc order per segment.
	for si, seg := range idx.segments {
		allow := mkAllow(seg, filter)
		for _, h := range seg.matchPhrase(slots) {
			if !idx.isLive(si, h.ord) {
				continue
			}
			if allowed(allow, h.ord) {
				partial := seg.scoreTerm(algo, float64(h.tf), 1.0, h.ord, idx.globalAvgDocLen)
				sink.pushAny(seg, h.ord, seg.pk(h.ord), partial*idf2)
				if sink.err != nil {
					return sink.err
				}
			}
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
func (idx *Index) StreamBagOfWords(pattern []byte, parser string, algo ScoreAlgo, filter *prefilter, wantInclude bool, emit func(out *vectorindex.SearchOutput) error) error {
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
	return idx.streamDisjunction(terms, algo, filter, newStreamSink(idx, wantInclude, filter, emit))
}

// streamDisjunction streams a pure OR of terms heap-free across all segments (global
// corpus stats + per-segment liveness), pushing every (pk, score) into sink and
// flushing at the end. Shared by StreamQuery's boolean disjunctive branch and
// StreamBagOfWords' IN BM25 MODE path — a change to the disjunctive walk lands once.
func (idx *Index) streamDisjunction(terms []clause, algo ScoreAlgo, filter *prefilter, sink *streamSink) error {
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
