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

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
)

// streamBatch is the max rows a streamSink buffers before flushing to emit
// (matches bm25's streamBatch).
const streamBatch = 8192

// streamSink batches (pk, score) results and flushes them to emit in bounded chunks,
// so a no-LIMIT query returns every matching doc WITHOUT ever materializing them all.
// On an emit error it records it and stops (the walk checks stopped and bails), so a
// cancelled consumer terminates the walk promptly. Mirrors bm25's streamSink.
type streamSink struct {
	emit    func(keys []any, distances []float64) error
	keys    []any
	scores  []float64
	err     error
	stopped bool
}

func (s *streamSink) push(pk any, score float32) {
	if s.stopped {
		return
	}
	s.keys = append(s.keys, pk)
	s.scores = append(s.scores, float64(score))
	if len(s.keys) >= streamBatch {
		s.flush()
	}
}

func (s *streamSink) flush() {
	if s.stopped || len(s.keys) == 0 {
		return
	}
	if e := s.emit(s.keys, s.scores); e != nil {
		s.err = e
		s.stopped = true
		return
	}
	// Hand ownership of the batch to emit; the next batch reallocates on append.
	s.keys = nil
	s.scores = nil
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
			sink.push(s.pk(minDoc), score)
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
func (idx *Index) StreamQuery(pattern []byte, boolean bool, parser string, algo ScoreAlgo, filter docfilter.MembershipFilter, emit func(keys []any, distances []float64) error) error {
	if idx.globalN == 0 {
		return nil
	}
	sink := &streamSink{emit: emit}
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
		sink.push(r.Pk, r.Score)
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
func (idx *Index) StreamBagOfWords(pattern []byte, parser string, algo ScoreAlgo, filter docfilter.MembershipFilter, emit func(keys []any, distances []float64) error) error {
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
	return idx.streamDisjunction(terms, algo, filter, &streamSink{emit: emit})
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
