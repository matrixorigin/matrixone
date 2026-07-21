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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

// buildOpts carries optional build settings shared by the build entry points.
type buildOpts struct {
	positionFree bool // drop the positional payload (keep the FST + docID/tf postings)
}

// BuildOpt configures a segment build (functional-options, so existing callers are
// unaffected).
type BuildOpt func(*buildOpts)

// WithPositionFree builds a POSITION-FREE segment: term frequencies are still derived
// (so BM25 ranking is unchanged), but the per-token positional payload — the largest
// section (~half the segment) — is omitted and the FST term dictionary is kept. Phrase
// / NL exact-match is therefore unavailable on such a segment (bag-of-words retrieval
// only). Mirrors the footprint of a position-free bm25 index while keeping the FST.
func WithPositionFree() BuildOpt { return func(o *buildOpts) { o.positionFree = true } }

func applyBuildOpts(opts []BuildOpt) buildOpts {
	var bo buildOpts
	for _, o := range opts {
		o(&bo)
	}
	return bo
}

// buildEntry is one (doc ord, positions-in-that-doc) occurrence of a term, collected
// in ascending doc order (SearchPhrase relies on the ordering).
type buildEntry struct {
	ord       int64
	positions []int32
}

// assembleTerms turns the collected per-term (ord, positions) lists into termPostings.
// When positionFree, tf is still derived from the occurrence count but the positional
// payload is dropped (tp.positions left nil) — Serialize then writes an empty positions
// section while the FST/docID/tf sections are unchanged.
func assembleTerms(global map[string][]buildEntry, docLen []int32, positionFree bool) map[string]*termPostings {
	terms := make(map[string]*termPostings, len(global))
	for w, entries := range global {
		tp := &termPostings{
			docIDs: make([]int64, len(entries)),
			tfs:    make([]uint8, len(entries)),
		}
		if !positionFree {
			tp.positions = make([][]int32, len(entries))
		}
		for i, e := range entries {
			tp.docIDs[i] = e.ord
			tf := len(e.positions)
			if tf > MaxCappedTf {
				tf = MaxCappedTf
			}
			tp.tfs[i] = uint8(tf)
			if !positionFree {
				tp.positions[i] = e.positions
			}
		}
		// Derive the raw term-level + Block-Max score-UB fields (mirrors the load
		// path), so a build-side segment gets the same WAND block-skip bounds.
		deriveTermStats(tp, docLen)
		terms[w] = tp
	}
	return terms
}

// BuildSegmentFromDocs tokenizes each doc with tok and builds an in-memory
// (build-side) segment: per-term posting lists with per-doc token positions,
// the docmap (pk + token-count length), and AvgDocLen. Docs are indexed by their
// slice position (the doc ord), so pks/docLen are ord-aligned.
//
// It is the minimal build path used to produce queryable segments (tests, and
// the seed for the eventual CDC/sync build sink). Positions are the tokenizer's
// TokenPos, so phrase adjacency in a query matches adjacency in the source text.
func BuildSegmentFromDocs(id string, pkType int32, docs []Doc, tok tokenizer.Tokenizer, opts ...BuildOpt) (*Segment, error) {
	bo := applyBuildOpts(opts)
	s := NewSegment(id, pkType)
	s.pks = make([]any, len(docs))
	s.docLen = make([]int32, len(docs))

	// term -> ascending list of (doc ord, positions-in-that-doc). Built in doc
	// order, so each term's list is ascending by ord (SearchPhrase relies on it).
	global := make(map[string][]buildEntry)

	for ord, d := range docs {
		s.pks[ord] = d.Pk
		local := make(map[string][]int32) // term -> positions in THIS doc
		var ntok int32
		for tk, err := range tok.Tokenize(d.Text) {
			if err != nil {
				return nil, err
			}
			w := tokenWord(tk)
			local[w] = append(local[w], tk.BytePos)
			ntok++
		}
		s.docLen[ord] = ntok
		for w, pos := range local {
			global[w] = append(global[w], buildEntry{int64(ord), pos})
		}
	}

	terms := assembleTerms(global, s.docLen, bo.positionFree)

	s.N = int64(len(docs))
	s.setTerms(terms)
	s.AvgDocLen = meanDocLen(s.docLen)
	return s, nil
}

// SearchText tokenizes query with tok (the same tokenizer used to build the
// segment) and runs an NL exact-phrase search — the convenience entry point that
// mirrors MATCH(col) AGAINST('query') in natural-language mode.
func (s *Segment) SearchText(query []byte, tok tokenizer.Tokenizer, algo ScoreAlgo, k int) ([]Result, error) {
	return s.SearchPhrase(tokenizePhraseSlots(tok, query), algo, k), nil
}

// TokenizedDoc is a document already tokenized into an ordered term slice (the
// term at index i has token position i). Used by the build path that tokenizes
// via the execution-side fulltext2_tokenize TVF (datalink/json/parsers resolved
// there) and only assembles the segment here.
type TokenizedDoc struct {
	Pk        any
	Terms     []string
	Positions []int32 // byte position of Terms[i] (parallel to Terms)
}

// BuildSegmentFromTokenized builds a segment from pre-tokenized docs (positions
// are term indices). It is the assembly half of BuildSegmentFromDocs without the
// tokenizer — the terms already came from fulltext2_tokenize at execution.
func BuildSegmentFromTokenized(id string, pkType int32, docs []TokenizedDoc, opts ...BuildOpt) (*Segment, error) {
	bo := applyBuildOpts(opts)
	s := NewSegment(id, pkType)
	s.pks = make([]any, len(docs))
	s.docLen = make([]int32, len(docs))

	global := make(map[string][]buildEntry)

	for ord, d := range docs {
		s.pks[ord] = d.Pk
		local := make(map[string][]int32)
		for i, w := range d.Terms {
			pos := int32(i) // fall back to token index when Positions is unset
			if i < len(d.Positions) {
				pos = d.Positions[i] // byte position
			}
			local[w] = append(local[w], pos)
		}
		s.docLen[ord] = int32(len(d.Terms))
		for w, pos := range local {
			global[w] = append(global[w], buildEntry{int64(ord), pos})
		}
	}

	terms := assembleTerms(global, s.docLen, bo.positionFree)

	s.N = int64(len(docs))
	s.setTerms(terms)
	s.AvgDocLen = meanDocLen(s.docLen)
	return s, nil
}

// DefaultBuildCapacity floors a non-positive segment DOC capacity for the streaming
// build paths (base CREATE build and the CDC tail), so a segment is sealed and
// spilled every ~1M docs and peak build memory stays bounded to one segment even
// when max_index_capacity is unset. Splitting a logically "unbounded" base into
// 1M-doc sub-indexes is transparent to queries (the Index already spans segments).
const DefaultBuildCapacity int64 = 1000000

// DefaultPostingCapacity floors a non-positive segment POSTING capacity
// (max_postings_capacity). Per-segment build memory tracks the number of
// accumulated postings (one term occurrence ≈ a term-string ref + an int32
// position ≈ 30 B of Go heap, ~2× at assembly), NOT the doc count — a doc can hold
// one token or tens of thousands, so a doc-only seal lets a long-document corpus
// (e.g. wiki articles ≈ 1k tokens each) blow past any memory bound. 8M postings ≈
// a ~512 MB peak build heap per segment; a segment seals on whichever cap
// (docs OR postings) is reached first, so build memory is bounded regardless of
// doc shape. See ReachedSegmentCap.
const DefaultPostingCapacity int64 = 8_000_000

// ReachedSegmentCap reports whether an open streaming Builder has hit either the
// doc cap (max_index_capacity, bounds the docmap/liveOrd heap for a many-tiny-docs
// index) or the posting cap (max_postings_capacity, bounds the positional posting
// heap for a few-long-docs index). Non-positive caps fall back to their defaults.
// Every streaming build path (CREATE TVF, CDC TailBuilder, MERGE/REBUILD compaction)
// seals on this single predicate so peak build memory is one bounded segment.
func ReachedSegmentCap(b *Builder, docCap, postingCap int64) bool {
	if docCap <= 0 {
		docCap = DefaultBuildCapacity
	}
	if postingCap <= 0 {
		postingCap = DefaultPostingCapacity
	}
	return int64(b.NumDocs()) >= docCap || int64(b.NumPostings()) >= postingCap
}

// Builder accumulates a token stream fed in (word, pk) order — the positional
// analogue of bm25's wand.Builder, with the SAME API (NewBuilder / Add / Finish /
// FinishSegments) so the two can share a core later. Each Add appends one token
// occurrence to its document (position = the doc's running token count), so the
// caller feeds a document's tokens contiguously and in order.
type Builder struct {
	id       string
	pkType   int32
	ordMap   map[any]int64 // normalized pk -> ord
	docs     []TokenizedDoc
	postings int64      // total term occurrences Add'd (one Add == one posting)
	opts     []BuildOpt // carried into FinishSegments (e.g. WithPositionFree)
}

// NewBuilder creates a Builder for an index id and source pk type (types.T). Pass
// WithPositionFree() to build position-free segments.
func NewBuilder(id string, pkType int32, opts ...BuildOpt) *Builder {
	return &Builder{id: id, pkType: pkType, ordMap: make(map[any]int64), opts: opts}
}

// docOrd returns the dense ord for a pk, assigning one on first sight (mirrors
// wand.Builder.docOrd).
func (b *Builder) docOrd(pk any) int64 {
	key := builderKey(pk)
	if o, ok := b.ordMap[key]; ok {
		return o
	}
	o := int64(len(b.docs))
	b.ordMap[key] = o
	b.docs = append(b.docs, TokenizedDoc{Pk: builderCopyPk(pk)})
	return o
}

// Add records one (word, byte-position, doc) token occurrence. Tokens of a document
// must be fed contiguously and in order; pos is the token's BYTE position in the doc
// (so a positional phrase query can match by byte offset, consistent with the base
// build's tk.BytePos and classic fulltext's byte-position phrase JOIN).
func (b *Builder) Add(word string, pos int32, pk any) error {
	if word == "" {
		return moerr.NewInternalErrorNoCtx("fulltext2 builder: empty word")
	}
	ord := b.docOrd(pk)
	b.docs[ord].Terms = append(b.docs[ord].Terms, word)
	b.docs[ord].Positions = append(b.docs[ord].Positions, pos)
	b.postings++
	return nil
}

// NumDocs returns the number of distinct documents added so far.
func (b *Builder) NumDocs() int { return len(b.docs) }

// NumPostings returns the number of term occurrences Add'd so far (one Add == one
// posting). It is the memory-correlated dimension the streaming build paths seal
// on (max_postings_capacity) — see ReachedSegmentCap.
func (b *Builder) NumPostings() int { return int(b.postings) }

// Finish produces a single-segment index (no capacity limit).
func (b *Builder) Finish() (*Segment, error) {
	segs, err := b.FinishSegments(0)
	if err != nil {
		return nil, err
	}
	return segs[0], nil
}

// FinishSegments finalizes the build into one or more segments, each holding at
// most capacity documents (capacity <= 0 => a single segment). Segment i is built
// under SubIndexId(b.id, i); the caller may override Segment.Id. Mirrors
// wand.Builder.FinishSegments (contiguous doc-ord range split).
func (b *Builder) FinishSegments(capacity int64) ([]*Segment, error) {
	n := int64(len(b.docs))
	if n == 0 {
		return nil, nil
	}
	if capacity <= 0 || n <= capacity {
		seg, err := BuildSegmentFromTokenized(SubIndexId(b.id, 0), b.pkType, b.docs, b.opts...)
		if err != nil {
			return nil, err
		}
		return []*Segment{seg}, nil
	}
	var segs []*Segment
	i := 0
	for lo := int64(0); lo < n; lo += capacity {
		hi := lo + capacity
		if hi > n {
			hi = n
		}
		seg, err := BuildSegmentFromTokenized(SubIndexId(b.id, i), b.pkType, b.docs[lo:hi], b.opts...)
		if err != nil {
			return nil, err
		}
		segs = append(segs, seg)
		i++
	}
	return segs, nil
}

// builderKey / builderCopyPk mirror wand.go's normalizeKey / copyPk so a []byte
// pk is a stable map key and safely retained.
func builderKey(pk any) any {
	if b, ok := pk.([]byte); ok {
		return string(b)
	}
	return pk
}

func builderCopyPk(pk any) any {
	if b, ok := pk.([]byte); ok {
		c := make([]byte, len(b))
		copy(c, b)
		return c
	}
	return pk
}

// tokenizeToTerms flattens a tokenizer stream into the ordered term slice a
// phrase query needs.
func tokenizeToTerms(text []byte, tok tokenizer.Tokenizer) ([]string, error) {
	// token count is unknown up front; estimate from input length to avoid regrowth.
	terms := make([]string, 0, len(text)/4)
	for tk, err := range tok.Tokenize(text) {
		if err != nil {
			return nil, err
		}
		terms = append(terms, tokenWord(tk))
	}
	return terms, nil
}
