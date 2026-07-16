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
	"container/heap"

	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
)

// Boolean-mode search (MATCH … AGAINST('…' IN BOOLEAN MODE)).
//
// MatrixOne/MySQL boolean semantics, mapped onto the positional segment:
//   - +clause  → MUST   (AND): the doc must contain it.
//   - -clause  → MUST NOT (the doc must not contain it).
//   - >clause  → SHOULD, impact ×1.1 (boost).
//   - <clause  → SHOULD, impact ×0.9 (demote).
//   - ~clause  → ADJUST, impact ×−1.0: present ⇒ LOWERS the score but does NOT
//     by itself include the doc (a noise-word penalty).
//   - clause   → SHOULD (OR): contributes to the score; at least one SHOULD (or
//     MUST) must match.
//
// A clause is a single TERM, a contiguous "PHRASE", a word* PREFIX, or a ( )
// GROUP whose score is the MAX of its child sub-scorers (§6) and whose presence
// is the union of its children (OR). Groups nest and may carry any prefix.
//
// This engine owns EXECUTION; the query front-end (fulltext.ParsePattern →
// operator tree) is reused at plugin-integration time and translated into these
// clauses. ParseBoolean here is the self-contained parser used to exercise the
// evaluator directly, tokenizing with the index's tokenizer for consistency with
// the build.

type clauseKind int

const (
	clauseTerm   clauseKind = iota // a single term
	clausePhrase                   // a contiguous phrase of terms
	clausePrefix                   // a word* prefix (terms[0] is the prefix)
	clauseGroup                    // ( ) — OR of children, score = max child
)

// Impact multipliers for the > < ~ weight operators (§6).
const (
	weightGreater = 1.1
	weightLess    = 0.9
	weightTilde   = -1.0
)

type clause struct {
	kind     clauseKind
	terms    []string // leaf (term / phrase / prefix)
	children []clause // group
	weight   float64  // impact multiplier (1.0 default; > < ~ change it)
}

// BoolQuery is a parsed boolean-mode query. adjust holds the ~ clauses, which
// adjust score without driving inclusion.
type BoolQuery struct {
	must    []clause // +
	mustNot []clause // -
	should  []clause // bare / > / <
	adjust  []clause // ~
}

// SearchBoolean evaluates a boolean query and returns up to k hits, score desc
// (ties by ascending doc ord). A doc matches when every MUST clause is present,
// no MUST-NOT clause is present, and — with no MUST — at least one SHOULD clause
// is present. Score sums the MUST + SHOULD contributions, then applies ~ ADJUST
// penalties to the matched docs.
//
// A pure disjunction of single terms (bare/>/< SHOULD terms, no MUST/MUST-NOT/~,
// no phrase/prefix/group) is routed to the WAND top-k (searchWAND), which
// early-terminates via term max-impact bounds and returns the identical ranking
// as the full scan. Everything else uses the full evaluator.
func (s *Segment) SearchBoolean(q BoolQuery, algo ScoreAlgo, k int, allow Membership, gs *globalStats) ([]Result, error) {
	if k <= 0 || s.N == 0 || (len(q.must) == 0 && len(q.should) == 0) {
		return nil, nil
	}
	if terms, ok := disjunctiveTerms(q); ok {
		return s.searchWAND(terms, algo, k, allow, gs), nil
	}
	return s.searchBooleanFull(q, algo, k, allow, gs)
}

// disjunctiveTerms reports whether q is a pure OR of single-term SHOULD clauses
// (the WAND-eligible shape) and returns those clauses.
func disjunctiveTerms(q BoolQuery) ([]clause, bool) {
	if len(q.must) != 0 || len(q.mustNot) != 0 || len(q.adjust) != 0 || len(q.should) == 0 {
		return nil, false
	}
	for _, c := range q.should {
		if c.kind != clauseTerm {
			return nil, false
		}
	}
	return q.should, true
}

func (s *Segment) searchBooleanFull(q BoolQuery, algo ScoreAlgo, k int, allow Membership, gs *globalStats) ([]Result, error) {
	avgDocLen := gs.avgdl(s)

	mustMaps, err := s.evalClauses(q.must, algo, avgDocLen, gs)
	if err != nil {
		return nil, err
	}
	shouldMaps, err := s.evalClauses(q.should, algo, avgDocLen, gs)
	if err != nil {
		return nil, err
	}
	adjustMaps, err := s.evalClauses(q.adjust, algo, avgDocLen, gs)
	if err != nil {
		return nil, err
	}
	notMaps, err := s.evalClauses(q.mustNot, algo, avgDocLen, gs)
	if err != nil {
		return nil, err
	}
	mustNot := make(map[int64]struct{})
	for _, m := range notMaps {
		for ord := range m {
			mustNot[ord] = struct{}{}
		}
	}

	// Candidate docs + accumulated MUST score.
	cand := make(map[int64]float64)
	if len(mustMaps) > 0 {
		for ord, sc0 := range mustMaps[0] { // intersection of all MUST clauses
			total, ok := sc0, true
			for _, m := range mustMaps[1:] {
				v, present := m[ord]
				if !present {
					ok = false
					break
				}
				total += v
			}
			if ok {
				cand[ord] = total
			}
		}
	} else {
		for _, m := range shouldMaps { // union of SHOULD clauses
			for ord := range m {
				if _, seen := cand[ord]; !seen {
					cand[ord] = 0
				}
			}
		}
	}
	// SHOULD scores (optional when MUST present), then ~ ADJUST penalties — both
	// only on docs already in the candidate set.
	for ord := range cand {
		for _, m := range shouldMaps {
			if v, present := m[ord]; present {
				cand[ord] += v
			}
		}
		for _, m := range adjustMaps {
			if v, present := m[ord]; present {
				cand[ord] += v
			}
		}
	}

	// Bounded top-k via a min-heap keyed by score (ascending-ord tiebreak), instead of
	// sorting the ENTIRE candidate set: a common term/ngram matches nearly every doc, so
	// an O(n log n) reflection-based sort of all candidates to keep k dominated the CPU
	// profile. This is O(n log k), typed (no reflect), O(k) memory, and yields the exact
	// same top-k (score desc, ties by ascending ord) as the full sort.
	h := &minScoreHeap{}
	for ord, score := range cand {
		if _, bad := mustNot[ord]; bad {
			continue
		}
		if !allowed(allow, ord) { // WHERE prefilter (nil = allow all)
			continue
		}
		if h.Len() < k {
			heap.Push(h, scoredDoc{ord, score})
			continue
		}
		// Strictly better, or ties the worst kept score with a smaller ord (the
		// score-desc / ord-asc tiebreak the full sort produced). Replace-root + Fix
		// (one sift) rather than Pop+Push (two).
		if root := (*h)[0]; score > root.score || (score == root.score && ord < root.ord) {
			(*h)[0] = scoredDoc{ord, score}
			heap.Fix(h, 0)
		}
	}
	return heapToResults(s, h), nil
}

func (s *Segment) evalClauses(cs []clause, algo ScoreAlgo, avgDocLen float64, gs *globalStats) ([]map[int64]float64, error) {
	out := make([]map[int64]float64, len(cs))
	for i, c := range cs {
		m, err := s.evalClause(c, algo, avgDocLen, gs)
		if err != nil {
			return nil, err
		}
		out[i] = m
	}
	return out, nil
}

// evalClause returns the weighted (doc ord → score) contribution of one clause.
// A present doc is always a key (even at score 0, so MUST/presence is exact); an
// absent term yields an empty map. The clause weight (> < ~) scales the result.
// Term / prefix / phrase clauses all score with the global corpus idf (gs) — a phrase
// clause via the cross-segment phrase df (gs.phraseDf), so ranking is consistent
// across a base + CDC tail.
func (s *Segment) evalClause(c clause, algo ScoreAlgo, avgDocLen float64, gs *globalStats) (map[int64]float64, error) {
	raw := make(map[int64]float64)
	switch c.kind {
	case clauseTerm:
		if pl, ok := s.lookup(c.terms[0]); ok {
			idf2 := gs.idfFor(s, c.terms[0], pl)
			docs, tfs := pl.materializeDocIDs(), pl.materializeTfs()
			for i, ord := range docs {
				raw[ord] = s.scoreTerm(algo, float64(tfs[i]), idf2, ord, avgDocLen)
			}
		}
	case clausePhrase:
		var hits []docTf
		if gs != nil {
			hits = gs.phraseHits(s, c.terms) // memoized; shared with gs.phraseDf
		} else {
			hits = s.matchPhrase(c.terms)
		}
		idf2 := gs.phraseIdfFor(s, c.terms, len(hits))
		for _, h := range hits {
			raw[h.ord] = s.scoreTerm(algo, float64(h.tf), idf2, h.ord, avgDocLen)
		}
	case clausePrefix:
		terms, err := s.prefixTerms(c.terms[0])
		if err != nil {
			return nil, err
		}
		for _, t := range terms { // combined impact = MAX over expanded terms (§6)
			pl, ok := s.lookup(t)
			if !ok {
				continue
			}
			idf2 := gs.idfFor(s, t, pl)
			docs, tfs := pl.materializeDocIDs(), pl.materializeTfs()
			for i, ord := range docs {
				sc := s.scoreTerm(algo, float64(tfs[i]), idf2, ord, avgDocLen)
				if cur, seen := raw[ord]; !seen || sc > cur {
					raw[ord] = sc
				}
			}
		}
	case clauseGroup:
		for _, ch := range c.children { // OR of children, score = MAX (§6)
			m, err := s.evalClause(ch, algo, avgDocLen, gs)
			if err != nil {
				return nil, err
			}
			for ord, sc := range m {
				if cur, seen := raw[ord]; !seen || sc > cur {
					raw[ord] = sc
				}
			}
		}
	}
	if c.weight != 1.0 {
		for ord := range raw {
			raw[ord] *= c.weight
		}
	}
	return raw, nil
}

// prefixTerms expands a word* prefix to its matching terms, over the loaded FST
// or the build-side sorted key list.
func (s *Segment) prefixTerms(prefix string) ([]string, error) {
	if s.dict != nil {
		return s.dict.prefixTerms(prefix)
	}
	return s.PrefixRange(prefix), nil
}

// SearchBooleanText parses query in boolean mode (tokenizing with tok, the index's
// tokenizer) and evaluates it — the convenience entry mirroring
// MATCH(col) AGAINST('query' IN BOOLEAN MODE).
func (s *Segment) SearchBooleanText(query []byte, tok tokenizer.Tokenizer, algo ScoreAlgo, k int) ([]Result, error) {
	q, err := ParseBoolean(query, tok)
	if err != nil {
		return nil, err
	}
	return s.SearchBoolean(q, algo, k, nil, nil) // convenience entry: no prefilter, local stats
}

// ---- parser ----

// bucket classifies a clause by its prefix operator.
type bucket int

const (
	bShould bucket = iota
	bMust
	bMustNot
	bAdjust
)

func bucketOf(prefix byte) bucket {
	switch prefix {
	case '+':
		return bMust
	case '-':
		return bMustNot
	case '~':
		return bAdjust
	default: // '>' '<' 0
		return bShould
	}
}

func weightOf(prefix byte) float64 {
	switch prefix {
	case '>':
		return weightGreater
	case '<':
		return weightLess
	case '~':
		return weightTilde
	default:
		return 1.0
	}
}

// ParseBoolean parses a boolean-mode query string into clauses, tokenizing each
// leaf's text with tok. Surface: leading +/-/>/</~ operators, "quoted phrases",
// trailing * (prefix), and ( ) groups (nestable). A bareword that tokenizes to
// several terms (e.g. a CJK compound) becomes a contiguous phrase.
func ParseBoolean(query []byte, tok tokenizer.Tokenizer) (BoolQuery, error) {
	var q BoolQuery
	for _, rc := range scanBoolean(string(query)) {
		c, ok, err := rawToClause(rc, tok)
		if err != nil {
			return q, err
		}
		if !ok {
			continue
		}
		switch bucketOf(rc.prefix) {
		case bMust:
			q.must = append(q.must, c)
		case bMustNot:
			q.mustNot = append(q.mustNot, c)
		case bAdjust:
			q.adjust = append(q.adjust, c)
		default:
			q.should = append(q.should, c)
		}
	}
	return q, nil
}

// rawToClause builds a clause (with its impact weight) from a raw scanned clause,
// recursing into groups. ok=false when it tokenizes to nothing.
func rawToClause(rc rawClause, tok tokenizer.Tokenizer) (clause, bool, error) {
	w := weightOf(rc.prefix)
	if rc.group {
		var children []clause
		for _, ch := range rc.children { // group members are OR'd; their prefix only sets their own weight
			cc, ok, err := rawToClause(ch, tok)
			if err != nil {
				return clause{}, false, err
			}
			if ok {
				children = append(children, cc)
			}
		}
		if len(children) == 0 {
			return clause{}, false, nil
		}
		return clause{kind: clauseGroup, children: children, weight: w}, true, nil
	}
	terms, err := tokenizeToTerms([]byte(rc.text), tok)
	if err != nil {
		return clause{}, false, err
	}
	if len(terms) == 0 {
		return clause{}, false, nil
	}
	switch {
	case rc.quoted:
		return clause{kind: clausePhrase, terms: terms, weight: w}, true, nil
	case rc.star:
		return clause{kind: clausePrefix, terms: terms[:1], weight: w}, true, nil
	case len(terms) == 1:
		return clause{kind: clauseTerm, terms: terms, weight: w}, true, nil
	default:
		return clause{kind: clausePhrase, terms: terms, weight: w}, true, nil
	}
}

type rawClause struct {
	prefix   byte // + - > < ~ or 0
	quoted   bool
	star     bool
	group    bool
	text     string
	children []rawClause
}

func isPrefixChar(c byte) bool {
	return c == '+' || c == '-' || c == '>' || c == '<' || c == '~'
}

// scanBoolean splits a boolean query into raw clauses at the top level.
func scanBoolean(q string) []rawClause {
	cs, _ := scanRange(q, 0)
	return cs
}

// scanRange scans clauses from index i until end-of-string or an unmatched ')'
// (a group close), returning the clauses and the index just past the stop point.
func scanRange(q string, i int) ([]rawClause, int) {
	var out []rawClause
	n := len(q)
	for i < n {
		for i < n && q[i] == ' ' {
			i++
		}
		if i >= n {
			break
		}
		if q[i] == ')' {
			return out, i + 1 // end of this group
		}
		var rc rawClause
		if isPrefixChar(q[i]) {
			rc.prefix = q[i]
			i++
		}
		switch {
		case i < n && q[i] == '(':
			rc.group = true
			i++
			rc.children, i = scanRange(q, i)
		case i < n && q[i] == '"':
			rc.quoted = true
			i++
			start := i
			for i < n && q[i] != '"' {
				i++
			}
			rc.text = q[start:i]
			if i < n {
				i++ // closing quote
			}
		default:
			start := i
			for i < n && q[i] != ' ' && q[i] != ')' {
				i++
			}
			word := q[start:i]
			if len(word) > 0 && word[len(word)-1] == '*' {
				rc.star = true
				word = word[:len(word)-1]
			}
			rc.text = word
		}
		if rc.group || rc.text != "" {
			out = append(out, rc)
		}
	}
	return out, i
}
