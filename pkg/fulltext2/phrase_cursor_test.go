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
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestPhraseCursorMatchesFallback proves the block-cursor phrase intersection
// (matchPhraseCursor) returns EXACTLY what the materialize fallback returns, across many
// blocks (> BlockSize docs, so the cursor crosses block boundaries, skips non-overlapping
// blocks, and decodes positions per block) — on both a build-side and a serialized/loaded
// segment. The fallback is the trusted oracle; any divergence is a cursor bug.
func TestPhraseCursorMatchesFallback(t *testing.T) {
	phrases := []string{
		"中文學習", "學習教材", "遠東兒童", "兒童中文", "教學指引",
		"中文學習教材", "生字卡片", "初學者適合", "短篇小說", "樂趣無窮",
	}
	// 500 docs (~4 blocks). Each doc concatenates a deterministic rotating subset of the
	// phrases (some repeated → tf>1), so phrases appear in scattered, block-spanning docs.
	docs := make([]Doc, 0, 500)
	for i := 0; i < 500; i++ {
		var b bytes.Buffer
		for k := 0; k < 3; k++ {
			b.WriteString(phrases[(i+k*3)%len(phrases)])
			b.WriteByte(' ')
		}
		if i%5 == 0 { // repeat one phrase → occurrence count > 1
			b.WriteString(phrases[i%len(phrases)])
		}
		docs = append(docs, Doc{int64(i + 1), b.Bytes()})
	}

	seg, err := BuildSegmentFromDocsParser("cur", int32(types.T_int64), docs, ParserNgram)
	require.NoError(t, err)
	blob, err := seg.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("cur", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })

	// Multi-trigram queries (each decomposes to >= 2 exact slots → the cursor path).
	queries := []string{
		"中文學習", "學習教材", "中文學習教材", "遠東兒童中文", "教學指引",
		"初學者適合", "短篇小說樂趣", "生字卡片", "兒童中文學習", "不存在的詞語",
	}
	key := func(h []docTf) []string {
		out := make([]string, len(h))
		for i, d := range h {
			out[i] = fmt.Sprintf("%d:%d", d.ord, d.tf)
		}
		return out
	}

	for _, name := range []string{"build", "loaded"} {
		sg := seg
		if name == "loaded" {
			sg = loaded
		}
		t.Run(name, func(t *testing.T) {
			for _, q := range queries {
				slots, err := phraseSlots(q, ParserNgram)
				require.NoError(t, err)
				if len(slots) < 2 {
					continue // cursor path is multi-slot only
				}
				cur := sg.matchPhraseCursor(slots)
				fb := sg.matchPhraseFallback(slots)
				require.ElementsMatch(t, key(fb), key(cur), "query %q (ord:tf must match the oracle)", q)
			}
		})
	}
}

func TestPhraseCursorDefersPositionsUntilRequested(t *testing.T) {
	data, err := syntheticCorpus(t).Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("lazy-pos", bytes.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })

	tp, ok := loaded.lookup("alpha")
	require.True(t, ok)
	c := newPhraseCursor(tp, 0)
	defer releasePhraseCursors([]*phraseCursor{c})

	require.Equal(t, -1, c.curDocBlk)
	require.Equal(t, -1, c.curPosBlk)
	first := c.doc()
	require.NotEqual(t, int64(math.MaxInt64), first)
	require.Equal(t, 0, c.curDocBlk)
	require.Equal(t, -1, c.curPosBlk, "reading doc IDs must not decode positions")

	c.skipTo(first + 1)
	require.Equal(t, -1, c.curPosBlk, "skipTo must not decode positions")
	if !c.atEnd() {
		_ = c.positions()
		require.Equal(t, c.idx/BlockSize, c.curPosBlk)
	}
}

// TestPhraseCursorBlockBoundaryParityAndScores keeps the cursor and the
// materializing fallback on the same semantic contract at the two boundaries
// around a posting block.  The sparse phrase has only six matches, while the
// dense phrase crosses several blocks and has repeated occurrences.  Both
// build-side and serialized/loaded postings must preserve the fallback's
// membership, term frequency, and float32 score bits under both scorers.
func TestPhraseCursorBlockBoundaryParityAndScores(t *testing.T) {
	const nDocs = 3*BlockSize + 5
	docs := make([]Doc, nDocs)
	for i := range docs {
		text := "sparse filler dense"
		switch {
		case i == BlockSize-1 || i == BlockSize || i == BlockSize+1 ||
			i == 2*BlockSize-1 || i == 2*BlockSize || i == 2*BlockSize+1:
			// sparse and dense occur in every document, so these doc ords
			// are also the posting ords immediately before, at, and after
			// two actual posting-block boundaries. Only these documents
			// contain the sparse phrase.
			text = "sparse dense"
		case i == 2*BlockSize+2:
			text = "dense sparse" // reversed control
		case i%3 == 1:
			text = "sparse filler dense alpha gamma"
		case i%3 == 2:
			text = "sparse filler dense beta alpha"
		case i%3 == 0:
			text = "sparse filler dense alpha beta alpha beta"
		}
		docs[i] = Doc{int64(i + 1), []byte(text)}
	}

	build, err := BuildSegmentFromDocsParser("phrase-boundary", int32(types.T_int64), docs, ParserDefault)
	require.NoError(t, err)
	blob, err := build.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("phrase-boundary-loaded", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })
	for _, term := range []string{"sparse", "dense"} {
		require.Equal(t, nDocs, build.terms[term].df(), "%s build posting must span all blocks", term)
		loadedPosting, ok := loaded.lookup(term)
		require.True(t, ok)
		require.Equal(t, nDocs, loadedPosting.df(), "%s loaded posting must span all blocks", term)
	}

	type queryCase struct {
		pattern string
		empty   bool
	}
	queries := []queryCase{
		{pattern: "alpha beta"},
		{pattern: "alpha beta alpha beta"},
		{pattern: "sparse dense"},
		{pattern: "dense sparse"},
		{pattern: "alpha missing", empty: true},
		{pattern: "missing term", empty: true},
	}
	expectedBoundaryHits := map[string]map[int64]int{
		"sparse dense": {
			int64(BlockSize - 1):   1,
			int64(BlockSize):       1,
			int64(BlockSize + 1):   1,
			int64(2*BlockSize - 1): 1,
			int64(2 * BlockSize):   1,
			int64(2*BlockSize + 1): 1,
		},
		"dense sparse": {int64(2*BlockSize + 2): 1},
	}

	hitKey := func(hits []docTf) map[int64]int {
		got := make(map[int64]int, len(hits))
		for _, h := range hits {
			got[h.ord] = h.tf
		}
		return got
	}

	// Build the expected score independently from SearchPhrase: the fallback
	// supplies membership/tf, while this reference applies the documented
	// float64 intermediates and narrows only at the Result boundary.
	fallbackScoreBits := func(s *Segment, hits []docTf, algo ScoreAlgo) map[int64]uint32 {
		if len(hits) == 0 {
			return nil
		}
		df := len(hits)
		if int64(df) > s.N {
			df = int(s.N)
		}
		idf := math.Log10(float64(s.N) / float64(df))
		idf2 := idf * idf
		var totalDocLen int64
		for _, docLen := range s.docLen {
			totalDocLen += int64(docLen)
		}
		avgDocLen := float64(totalDocLen) / float64(len(s.docLen))
		want := make(map[int64]uint32, len(hits))
		for _, h := range hits {
			tf := float64(h.tf)
			score := tf * idf2
			if algo == BM25 {
				norm := 1 - bm25B + bm25B*float64(s.docLen[h.ord])/avgDocLen
				score = idf2 * tf * (bm25K1 + 1) / (tf + bm25K1*norm)
			}
			want[s.pk(h.ord).(int64)] = math.Float32bits(float32(score))
		}
		return want
	}

	for _, tc := range queries {
		t.Run(tc.pattern, func(t *testing.T) {
			slots, err := phraseSlots(tc.pattern, ParserDefault)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(slots), 2, "query must exercise the multi-slot phrase cursor")
			for _, variant := range []struct {
				name string
				seg  *Segment
			}{
				{name: "build", seg: build},
				{name: "loaded", seg: loaded},
			} {
				t.Run(variant.name, func(t *testing.T) {
					cursor := variant.seg.matchPhraseCursor(slots)
					fallback := variant.seg.matchPhraseFallback(slots)
					fallbackKeys := hitKey(fallback)
					cursorKeys := hitKey(cursor)
					require.Len(t, fallbackKeys, len(fallback), "fallback phrase hits must have unique ords")
					require.Len(t, cursorKeys, len(cursor), "cursor phrase hits must have unique ords")
					require.Equal(t, fallbackKeys, cursorKeys, "cursor/fallback membership and tf")
					if expected, ok := expectedBoundaryHits[tc.pattern]; ok {
						require.Equal(t, expected, fallbackKeys, "fixture must hit the intended posting boundaries")
					}

					for _, algo := range []ScoreAlgo{TfIdf, BM25} {
						got := variant.seg.SearchPhrase(slots, algo, nDocs+1)
						if tc.empty {
							require.Empty(t, got)
							continue
						}
						gotBits := make(map[int64]uint32, len(got))
						for _, r := range got {
							gotBits[r.Pk.(int64)] = math.Float32bits(r.Score)
						}
						require.Len(t, gotBits, len(got), "phrase results must have unique PKs")
						require.Equal(t, fallbackScoreBits(variant.seg, fallback, algo), gotBits,
							"algo=%d: fallback score bits", algo)
					}
				})
			}
		})
	}
}

// TestPhraseCursorDefersPositionsAcrossBlocks proves that advancing a cursor
// across blocks keeps the positional payload compressed until a matching
// document actually needs verification, and that a decoded block is reused
// until the cursor enters another position block.
func TestPhraseCursorDefersPositionsAcrossBlocks(t *testing.T) {
	const nDocs = 3 * BlockSize
	docs := make([]Doc, nDocs)
	for i := range docs {
		prefix := strings.Repeat("pad ", i%5)
		docs[i] = Doc{int64(i), []byte(prefix + "alpha beta")}
	}
	build, err := BuildSegmentFromDocsParser("lazy-boundary", int32(types.T_int64), docs, ParserDefault)
	require.NoError(t, err)
	blob, err := build.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("lazy-boundary-loaded", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })

	for _, variant := range []struct {
		name string
		seg  *Segment
	}{
		{name: "build", seg: build},
		{name: "loaded", seg: loaded},
	} {
		t.Run(variant.name, func(t *testing.T) {
			tp, ok := variant.seg.lookup("alpha")
			require.True(t, ok)
			c := newPhraseCursor(tp, 0)
			defer releasePhraseCursors([]*phraseCursor{c})

			require.Equal(t, int64(0), c.doc())
			require.Equal(t, 0, c.curDocBlk)
			require.Equal(t, -1, c.curPosBlk)

			c.skipTo(int64(BlockSize))
			require.Equal(t, int64(BlockSize), c.doc())
			require.Equal(t, 1, c.curDocBlk)
			require.Equal(t, -1, c.curPosBlk, "skipTo across a block must not decode positions")
			first := append([]int32(nil), c.positions()...)
			require.Equal(t, 1, c.curPosBlk)
			require.NotEmpty(t, first, "the controlled source mutation must change a real position list")
			// Corrupt the source for the current block after the first
			// decode. A second positions() call must still return the
			// cached data; if it decodes again, this becomes empty.
			if tp.positions != nil {
				tp.positions[BlockSize] = nil
			} else {
				off := tp.blockPosOff[1]
				require.Less(t, int(off), len(tp.posRaw))
				// A loaded segment's posRaw is a read-only mmap. Replace
				// the view with a private copy before corrupting it.
				tp.posRaw = append([]byte(nil), tp.posRaw...)
				tp.posRaw[off] = 0
			}
			require.Equal(t, first, c.positions(), "repeated positions must reuse the decoded block")

			c.skipTo(int64(2 * BlockSize))
			require.Equal(t, int64(2*BlockSize), c.doc())
			require.Equal(t, 2, c.curDocBlk)
			require.Equal(t, 1, c.curPosBlk, "doc-only advancement must retain the prior position block")
			second := append([]int32(nil), c.positions()...)
			require.NotEmpty(t, second)
			require.NotEqual(t, first, second, "different blocks must carry different source positions")
			require.Equal(t, 2, c.curPosBlk)
		})
	}
}
