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

	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
)

// Doc is one source row to index: its primary key and the text of the indexed
// column(s).
type Doc struct {
	Pk   any
	Text []byte
}

// tokenWord decodes the length-prefixed word out of a tokenizer.Token
// (TokenBytes[0] is the byte length; the word follows). The tokenizer already
// lowercases and truncates to MAX_TOKEN_SIZE.
func tokenWord(tk tokenizer.Token) string {
	n := int(tk.TokenBytes[0])
	return string(tk.TokenBytes[1 : 1+n])
}

// BuildSegmentFromDocs tokenizes each doc with tok and builds an in-memory
// (build-side) segment: per-term posting lists with per-doc token positions,
// the docmap (pk + token-count length), and AvgDocLen. Docs are indexed by their
// slice position (the doc ord), so pks/docLen are ord-aligned.
//
// It is the minimal build path used to produce queryable segments (tests, and
// the seed for the eventual CDC/sync build sink). Positions are the tokenizer's
// TokenPos, so phrase adjacency in a query matches adjacency in the source text.
func BuildSegmentFromDocs(id string, pkType int32, docs []Doc, tok tokenizer.Tokenizer) (*Segment, error) {
	s := NewSegment(id, pkType)
	s.pks = make([]any, len(docs))
	s.docLen = make([]int32, len(docs))

	// term -> ascending list of (doc ord, positions-in-that-doc). Built in doc
	// order, so each term's list is ascending by ord (SearchPhrase relies on it).
	type entry struct {
		ord       int64
		positions []int32
	}
	global := make(map[string][]entry)

	for ord, d := range docs {
		s.pks[ord] = d.Pk
		local := make(map[string][]int32) // term -> positions in THIS doc
		var ntok int32
		for tk, err := range tok.Tokenize(d.Text) {
			if err != nil {
				return nil, err
			}
			w := tokenWord(tk)
			local[w] = append(local[w], tk.TokenPos)
			ntok++
		}
		s.docLen[ord] = ntok
		for w, pos := range local {
			global[w] = append(global[w], entry{int64(ord), pos})
		}
	}

	terms := make(map[string]*termPostings, len(global))
	for w, entries := range global {
		tp := &termPostings{
			docIDs:    make([]int64, len(entries)),
			tfs:       make([]uint8, len(entries)),
			positions: make([][]int32, len(entries)),
		}
		maxTf := 0
		minDocLen := int32(math.MaxInt32)
		for i, e := range entries {
			tp.docIDs[i] = e.ord
			tf := len(e.positions)
			if tf > MaxCappedTf {
				tf = MaxCappedTf
			}
			tp.tfs[i] = uint8(tf)
			tp.positions[i] = e.positions
			if tf > maxTf {
				maxTf = tf
			}
			if dl := s.docLen[e.ord]; dl < minDocLen {
				minDocLen = dl
			}
		}
		tp.maxTf = uint8(maxTf)
		tp.minDocLen = minDocLen
		terms[w] = tp
	}

	s.N = int64(len(docs))
	s.setTerms(terms)
	s.AvgDocLen = meanDocLen(s.docLen)
	return s, nil
}

// SearchText tokenizes query with tok (the same tokenizer used to build the
// segment) and runs an NL exact-phrase search — the convenience entry point that
// mirrors MATCH(col) AGAINST('query') in natural-language mode.
func (s *Segment) SearchText(query []byte, tok tokenizer.Tokenizer, algo ScoreAlgo, k int) ([]Result, error) {
	terms, err := tokenizeToTerms(query, tok)
	if err != nil {
		return nil, err
	}
	return s.SearchPhrase(terms, algo, k), nil
}

// tokenizeToTerms flattens a tokenizer stream into the ordered term slice a
// phrase query needs.
func tokenizeToTerms(text []byte, tok tokenizer.Tokenizer) ([]string, error) {
	var terms []string
	for tk, err := range tok.Tokenize(text) {
		if err != nil {
			return nil, err
		}
		terms = append(terms, tokenWord(tk))
	}
	return terms, nil
}
