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

// resolveJSONProbeTerms expands a probe into the concrete term list to search.
//
// Ranges are expanded against EVERY segment and unioned into one global list,
// rather than per segment. streamDisjunction already walks all segments with one
// shared term list, and a term absent from a segment simply contributes no
// postings — so the union costs nothing but keeps this off the hot path and out
// of the segment walk.
func (idx *Index) resolveJSONProbeTerms(p JSONProbePayload) ([]string, error) {
	seen := make(map[string]struct{}, len(p.Terms))
	out := make([]string, 0, len(p.Terms))
	add := func(t string) {
		if t == "" {
			return
		}
		if _, dup := seen[t]; dup {
			return
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	for _, t := range p.Terms {
		add(t)
	}
	for _, r := range p.Ranges {
		for _, seg := range idx.segments {
			terms, err := seg.termRangeTerms(r[0], r[1])
			if err != nil {
				return nil, err
			}
			for _, t := range terms {
				add(t)
			}
		}
	}
	return out, nil
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
// no-pushed-LIMIT path. Mirrors StreamBagOfWords: same disjunctive walk, only
// the term list is built from the probe instead of tokenized text.
func (idx *Index) StreamJSONProbe(pattern []byte, algo ScoreAlgo, filter *prefilter, wantInclude bool,
	emit func(out *vectorindex.SearchOutput) error) error {
	if idx.globalN == 0 {
		return nil
	}
	q, err := idx.buildJSONProbeQuery(pattern)
	if err != nil {
		return err
	}
	terms, ok := disjunctiveTerms(q)
	if !ok {
		return nil // no resolvable terms -> nothing to stream
	}
	return idx.streamDisjunction(terms, algo, filter, newStreamSink(idx, wantInclude, filter, emit))
}
