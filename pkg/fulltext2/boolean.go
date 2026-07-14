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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
)

// Boolean-mode search (MATCH … AGAINST('…' IN BOOLEAN MODE)).
//
// MatrixOne/MySQL boolean semantics, mapped onto the positional segment:
//   - +clause  → MUST   (AND): the doc must contain it.
//   - -clause  → MUST NOT (the doc must not contain it).
//   - clause   → SHOULD (OR): contributes to the score; at least one SHOULD must
//     match when there is no MUST.
//
// A clause is one of: a single TERM, a contiguous "PHRASE", or a word* PREFIX.
// (The `> < ~` weight operators and nested ( ) groups map onto the same clause
// machinery and land in a later slice; this slice covers + - " " * and bare OR.)
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
)

type clause struct {
	kind  clauseKind
	terms []string
}

// BoolQuery is a parsed boolean-mode query.
type BoolQuery struct {
	must    []clause // +
	mustNot []clause // -
	should  []clause // bare
}

// SearchBoolean evaluates a boolean query over the segment and returns up to k
// hits, score desc (ties by ascending doc ord). A doc matches when every MUST
// clause is present, no MUST-NOT clause is present, and — if there is no MUST —
// at least one SHOULD clause is present. Score sums the MUST and SHOULD
// contributions.
func (s *Segment) SearchBoolean(q BoolQuery, algo ScoreAlgo, k int) ([]Result, error) {
	if k <= 0 || s.N == 0 || (len(q.must) == 0 && len(q.should) == 0) {
		return nil, nil
	}
	avgDocLen := s.avgDocLenOrMean()

	mustMaps, err := s.evalClauses(q.must, algo, avgDocLen)
	if err != nil {
		return nil, err
	}
	shouldMaps, err := s.evalClauses(q.should, algo, avgDocLen)
	if err != nil {
		return nil, err
	}
	// MUST-NOT contributes presence only.
	mustNot := make(map[int64]struct{})
	notMaps, err := s.evalClauses(q.mustNot, algo, avgDocLen)
	if err != nil {
		return nil, err
	}
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
	// Add SHOULD scores (optional when MUST is present).
	for ord := range cand {
		for _, m := range shouldMaps {
			if v, present := m[ord]; present {
				cand[ord] += v
			}
		}
	}

	// Drop MUST-NOT docs; emit in ascending ord so the score-desc sort keeps an
	// ascending-ord tiebreak.
	ords := make([]int64, 0, len(cand))
	for ord := range cand {
		if _, bad := mustNot[ord]; bad {
			continue
		}
		ords = append(ords, ord)
	}
	sort.Slice(ords, func(a, b int) bool { return ords[a] < ords[b] })

	results := make([]Result, len(ords))
	for i, ord := range ords {
		results[i] = Result{Pk: s.pks[ord], Score: cand[ord]}
	}
	sort.SliceStable(results, func(a, b int) bool { return results[a].Score > results[b].Score })
	if len(results) > k {
		results = results[:k]
	}
	return results, nil
}

func (s *Segment) evalClauses(cs []clause, algo ScoreAlgo, avgDocLen float64) ([]map[int64]float64, error) {
	out := make([]map[int64]float64, len(cs))
	for i, c := range cs {
		m, err := s.evalClause(c, algo, avgDocLen)
		if err != nil {
			return nil, err
		}
		out[i] = m
	}
	return out, nil
}

// evalClause returns the (doc ord → score) contribution of one clause. A present
// doc is always a key (even with score 0, so MUST/presence is exact); an absent
// term yields an empty map.
func (s *Segment) evalClause(c clause, algo ScoreAlgo, avgDocLen float64) (map[int64]float64, error) {
	out := make(map[int64]float64)
	switch c.kind {
	case clauseTerm:
		pl, ok := s.lookup(c.terms[0])
		if !ok {
			return out, nil
		}
		idf2 := idfSquared(s.N, pl.df())
		for i, ord := range pl.docIDs {
			out[ord] = s.scoreTerm(algo, float64(pl.tfs[i]), idf2, ord, avgDocLen)
		}
	case clausePhrase:
		hits := s.matchPhrase(c.terms)
		idf2 := idfSquared(s.N, len(hits))
		for _, h := range hits {
			out[h.ord] = s.scoreTerm(algo, float64(h.tf), idf2, h.ord, avgDocLen)
		}
	case clausePrefix:
		terms, err := s.prefixTerms(c.terms[0])
		if err != nil {
			return nil, err
		}
		// Combined impact = MAX over the expanded terms (§6).
		for _, t := range terms {
			pl, ok := s.lookup(t)
			if !ok {
				continue
			}
			idf2 := idfSquared(s.N, pl.df())
			for i, ord := range pl.docIDs {
				sc := s.scoreTerm(algo, float64(pl.tfs[i]), idf2, ord, avgDocLen)
				if cur, seen := out[ord]; !seen || sc > cur {
					out[ord] = sc
				}
			}
		}
	}
	return out, nil
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
	return s.SearchBoolean(q, algo, k)
}

// ParseBoolean parses a boolean-mode query string into clauses, tokenizing each
// clause's text with tok. Supported surface: leading +/- (MUST / MUST-NOT),
// "quoted phrases", and a trailing * (prefix). A bareword that tokenizes to
// several terms (e.g. a CJK compound) becomes a contiguous phrase.
func ParseBoolean(query []byte, tok tokenizer.Tokenizer) (BoolQuery, error) {
	var q BoolQuery
	for _, rc := range scanBoolean(string(query)) {
		terms, err := tokenizeToTerms([]byte(rc.text), tok)
		if err != nil {
			return q, err
		}
		if len(terms) == 0 {
			continue
		}
		var c clause
		switch {
		case rc.quoted:
			c = clause{kind: clausePhrase, terms: terms}
		case rc.star:
			c = clause{kind: clausePrefix, terms: terms[:1]}
		case len(terms) == 1:
			c = clause{kind: clauseTerm, terms: terms}
		default:
			c = clause{kind: clausePhrase, terms: terms}
		}
		switch rc.prefix {
		case '+':
			q.must = append(q.must, c)
		case '-':
			q.mustNot = append(q.mustNot, c)
		default:
			q.should = append(q.should, c)
		}
	}
	return q, nil
}

type rawClause struct {
	prefix byte // '+', '-', or 0
	quoted bool
	star   bool
	text   string
}

// scanBoolean splits a boolean query into raw clauses at the top level: an
// optional +/- prefix, then either a "quoted phrase" or a whitespace-delimited
// bareword (a trailing * marks a prefix). Term tokenization happens later.
func scanBoolean(q string) []rawClause {
	var out []rawClause
	i, n := 0, len(q)
	for i < n {
		for i < n && q[i] == ' ' {
			i++
		}
		if i >= n {
			break
		}
		var rc rawClause
		if q[i] == '+' || q[i] == '-' {
			rc.prefix = q[i]
			i++
		}
		if i < n && q[i] == '"' {
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
		} else {
			start := i
			for i < n && q[i] != ' ' {
				i++
			}
			word := q[start:i]
			if len(word) > 0 && word[len(word)-1] == '*' {
				rc.star = true
				word = word[:len(word)-1]
			}
			rc.text = word
		}
		if rc.text != "" || rc.quoted {
			out = append(out, rc)
		}
	}
	return out
}
