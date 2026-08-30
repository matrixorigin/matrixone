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
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// The json index probe.
//
// The optimizer turns `json_extract_string(j,'$.foo') = 'bar'` into a MATCH
// carrying a PROBE PAYLOAD instead of a text pattern: a set of exact tuple terms
// plus a set of inclusive term ranges, whose UNION the document must intersect.
//
// It rides the ordinary MATCH surface (fulltext_match → fulltext2_search) so the
// whole scan-to-TVF rewrite is reused, and is told apart by its mode. The
// payload is binary — tuple terms contain 0x00 and BOOLEAN-mode metacharacters —
// so it must never reach the pattern parser, which is exactly what the distinct
// mode prevents.

// JSONProbeMode is the fulltext_match mode that means "the pattern is a probe
// payload, not text". It sits far above the tree.FULLTEXT_* grammar modes: no
// SQL surface produces it, the optimizer synthesizes it directly.
const JSONProbeMode int64 = 1000

// JSONProbePayload is the decoded probe: the document qualifies if it holds any
// Term, or any term inside any Range.
type JSONProbePayload struct {
	Terms  []string
	Ranges [][2]string // {lo, hi}, both inclusive
}

// EncodeJSONProbePayload serializes the probe. Everything is length-prefixed
// because terms are arbitrary bytes; no separator would be safe.
func EncodeJSONProbePayload(terms []string, ranges [][2]string) string {
	var b []byte
	var hdr [4]byte
	put := func(s string) {
		binary.LittleEndian.PutUint32(hdr[:], uint32(len(s)))
		b = append(b, hdr[:]...)
		b = append(b, s...)
	}
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(terms)))
	b = append(b, hdr[:]...)
	for _, t := range terms {
		put(t)
	}
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(ranges)))
	b = append(b, hdr[:]...)
	for _, r := range ranges {
		put(r[0])
		put(r[1])
	}
	return string(b)
}

// DecodeJSONProbePayload reverses EncodeJSONProbePayload. A malformed payload is
// reported rather than silently treated as an empty probe: an empty probe
// matches nothing, which would drop every row instead of failing loudly.
func DecodeJSONProbePayload(s string) (JSONProbePayload, bool) {
	var p JSONProbePayload
	i := 0
	u32 := func() (int, bool) {
		if i+4 > len(s) {
			return 0, false
		}
		v := int(binary.LittleEndian.Uint32([]byte(s[i : i+4])))
		i += 4
		return v, v >= 0
	}
	str := func() (string, bool) {
		n, ok := u32()
		if !ok || i+n > len(s) {
			return "", false
		}
		v := s[i : i+n]
		i += n
		return v, true
	}
	nt, ok := u32()
	if !ok {
		return p, false
	}
	for range nt {
		t, ok := str()
		if !ok {
			return JSONProbePayload{}, false
		}
		p.Terms = append(p.Terms, t)
	}
	nr, ok := u32()
	if !ok {
		return p, false
	}
	for range nr {
		lo, ok1 := str()
		hi, ok2 := str()
		if !ok1 || !ok2 {
			return JSONProbePayload{}, false
		}
		p.Ranges = append(p.Ranges, [2]string{lo, hi})
	}
	if i != len(s) {
		return JSONProbePayload{}, false
	}
	return p, true
}

// maxProbeTermExpansion bounds the MATERIALIZING expansion used by the bounded
// top-k path. The streaming path has no such limit because it never holds more
// than one term; this cap exists so that if a probe with ranges ever reaches
// top-k (the planner cannot push a LIMIT onto a probe, so it should not) it
// fails loudly instead of expanding a whole vocabulary into clauses.
const maxProbeTermExpansion = 4096

// resolveJSONProbeTerms expands a probe into a concrete term list.
//
// This is the MATERIALIZING form, used only by the bounded top-k path. Ranges
// are expanded against EVERY segment and unioned into one global list: the
// disjunctive walk takes one shared term list, and a term absent from a segment
// simply contributes no postings.
//
// The streaming path does NOT use this — see streamJSONProbeDocs, which visits
// one term at a time and never builds this list.
func (idx *Index) resolveJSONProbeTerms(p JSONProbePayload) ([]string, error) {
	seen := make(map[string]struct{}, len(p.Terms))
	out := make([]string, 0, len(p.Terms))
	add := func(t string) error {
		if t == "" {
			return nil
		}
		if _, dup := seen[t]; dup {
			return nil
		}
		if len(out) >= maxProbeTermExpansion {
			return moerr.NewInternalErrorNoCtx(
				"fulltext2: json probe range expands past the term cap; this probe must be streamed, not top-k'd")
		}
		seen[t] = struct{}{}
		out = append(out, t)
		return nil
	}
	for _, t := range p.Terms {
		if err := add(t); err != nil {
			return nil, err
		}
	}
	for _, r := range p.Ranges {
		for _, seg := range idx.segments {
			if err := seg.forEachTermInRange(r[0], r[1], add); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// streamJSONProbeDocs streams every doc the probe matches, WITHOUT materializing
// the matched term set.
//
// This is what makes an inequality probe affordable. A range over a
// high-cardinality key covers most of that key's vocabulary, and the ranked walk
// (streamWAND over one clause per term) holds a posting cursor and a block-sized
// decode buffer for EVERY term at once — O(vocabulary x segments) live. But a
// probe is a PREFILTER: it needs doc ids, not a ranking. Nothing orders by its
// score, so the terms never have to be merged, and each can be walked and
// dropped in turn. Peak cost is one term and one block buffer.
//
// A doc holding several terms of the range is therefore emitted once per term.
// That is intentional: de-duplication belongs to the GROUP BY the planner puts
// over this scan (addJSONFulltextProbes), not to a second bespoke copy of it
// here. The aggregate already spills and is already tested.
func (idx *Index) streamJSONProbeDocs(p JSONProbePayload, filter *prefilter, sink *streamSink) error {
	var docs [BlockSize]int64
	for si, seg := range idx.segments {
		allow := andAllow(mkAllow(seg, filter), &livenessMembership{idx: idx, si: si})

		// emit walks ONE term's postings a block at a time.
		emit := func(term string) error {
			if sink.stopped {
				return errProbeStopped
			}
			pl, ok := seg.lookup(term)
			if !ok {
				return nil
			}
			for b := range pl.nblk() {
				n := pl.fillBlockDocs(b, docs[:])
				for _, ord := range docs[:n] {
					if !allowed(allow, ord) {
						continue
					}
					// score 0: a probe has no ranking, and nothing above it
					// reads the score column.
					sink.pushPk(seg, ord, 0)
					if sink.stopped {
						return errProbeStopped
					}
				}
			}
			return sink.err
		}

		if err := probeWalk(seg, p, emit); err != nil {
			if errors.Is(err, errProbeStopped) {
				sink.flush()
				return sink.err
			}
			return err
		}
		if sink.err != nil {
			return sink.err
		}
	}
	sink.flush()
	return sink.err
}

// errProbeStopped unwinds the term walk when the sink has stopped (a cancelled
// query or a full downstream). It never escapes streamJSONProbeDocs.
var errProbeStopped = errors.New("fulltext2: json probe stream stopped")

// probeWalk visits every term the probe selects in one segment: its exact terms,
// then each range streamed from the term dictionary.
func probeWalk(seg *Segment, p JSONProbePayload, fn func(term string) error) error {
	for _, t := range p.Terms {
		if err := fn(t); err != nil {
			return err
		}
	}
	for _, r := range p.Ranges {
		if err := seg.forEachTermInRange(r[0], r[1], fn); err != nil {
			return err
		}
	}
	return nil
}

// buildJSONProbeQuery turns a probe into a pure disjunction of single terms —
// the same shape buildBagOfWordsQuery produces, so it routes through the very
// same disjunctive walk (searchWAND / streamDisjunction) with no new search
// machinery.
func (idx *Index) buildJSONProbeQuery(pattern []byte) (BoolQuery, error) {
	p, ok := DecodeJSONProbePayload(string(pattern))
	if !ok {
		return BoolQuery{}, moerr.NewInternalErrorNoCtx("fulltext2: malformed json probe payload")
	}
	terms, err := idx.resolveJSONProbeTerms(p)
	if err != nil {
		return BoolQuery{}, err
	}
	var q BoolQuery
	for _, t := range terms {
		q.should = append(q.should, clause{kind: clauseTerm, terms: []string{t}, weight: 1})
	}
	return q, nil
}

// SearchJSONProbe answers a probe with a top-k search.
func (idx *Index) SearchJSONProbe(pattern []byte, algo ScoreAlgo, k int, filter *prefilter) ([]Result, error) {
	if k <= 0 || idx.globalN == 0 {
		return nil, nil
	}
	q, err := idx.buildJSONProbeQuery(pattern)
	if err != nil {
		return nil, err
	}
	if len(q.should) == 0 {
		// no term in the index can satisfy the probe: no row qualifies
		return nil, nil
	}
	return idx.SearchBoolean(q, algo, k, filter)
}

// StreamJSONProbe answers a probe by streaming every matching doc, the
// no-pushed-LIMIT path a probe always takes (the planner cannot push a LIMIT
// onto a probe: the retained predicate keeps the scan's filter list non-empty).
//
// Unlike StreamBagOfWords this does NOT go through the disjunctive walk. A probe
// is unranked, so its terms need no merging and are walked one at a time —
// see streamJSONProbeDocs. algo is unused for the same reason, and is kept only
// so the probe matches the other stream entry points.
func (idx *Index) StreamJSONProbe(pattern []byte, _ ScoreAlgo, filter *prefilter, wantInclude bool,
	emit func(out *vectorindex.SearchOutput) error) error {
	if idx.globalN == 0 {
		return nil
	}
	p, ok := DecodeJSONProbePayload(string(pattern))
	if !ok {
		return moerr.NewInternalErrorNoCtx("fulltext2: malformed json probe payload")
	}
	return idx.streamJSONProbeDocs(p, filter, newStreamSink(idx, wantInclude, filter, emit))
}
