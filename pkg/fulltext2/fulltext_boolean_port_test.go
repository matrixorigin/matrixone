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

// COMPLETE port of classic fulltext's boolean-mode tests
// (pkg/fulltext/fulltext_test.go: TestFullText{Or,PlusPlus,PlusOr,Minus,Tilda,
// 1,2,3,5,Group,JoinGroupTilda,GroupTilda,Star,Phrase}). The classic tests unit-
// test Pattern.Eval with synthetic per-term-frequency docvecs; here each docvec is
// reconstructed into a real corpus (word repeated tf times) and fulltext2's
// boolean membership is asserted against the SAME expected doc set — proving
// MySQL boolean-mode parity through fulltext2's own parser + WAND evaluator.
package fulltext2

import (
	"sort"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/stretchr/testify/require"
)

// tfSegment reconstructs a classic docvec corpus: doc id contains words[j]
// repeated tf[j] times (space-joined). Docs are added in ascending-id order.
func tfSegment(t *testing.T, words []string, docs map[int64][]int) *Segment {
	ids := make([]int64, 0, len(docs))
	for id := range docs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ds := make([]Doc, 0, len(ids))
	for _, id := range ids {
		var b strings.Builder
		for j, w := range words {
			for k := 0; k < docs[id][j]; k++ {
				if b.Len() > 0 {
					b.WriteByte(' ')
				}
				b.WriteString(w)
			}
		}
		ds = append(ds, Doc{id, []byte(b.String())})
	}
	seg, err := BuildSegmentFromDocs("port", int32(types.T_int64), ds, tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	return seg
}

// apple/banana corpus: doc 0 has both, 1 apple-only, 11/12 banana-only.
func abSeg(t *testing.T) *Segment {
	return tfSegment(t, []string{"apple", "banana"}, map[int64][]int{
		0: {2, 2}, 1: {3, 0}, 11: {0, 3}, 12: {0, 4},
	})
}

// we/are/so/happy corpus (TestFullText1 shape): one term per doc-group.
func washSeg(t *testing.T) *Segment {
	return tfSegment(t, []string{"we", "are", "so", "happy"}, map[int64][]int{
		0: {2, 0, 0, 0}, 1: {3, 0, 0, 0},
		10: {0, 2, 0, 0}, 11: {0, 3, 0, 0}, 12: {0, 4, 0, 0},
		20: {0, 0, 5, 0}, 21: {0, 0, 6, 0}, 22: {0, 0, 7, 0}, 23: {0, 0, 8, 0},
		30: {0, 0, 0, 1}, 31: {0, 0, 0, 2}, 32: {0, 0, 0, 3}, 33: {0, 0, 0, 4},
	})
}

// ---- OR / +/- / ~ (apple-banana corpus) ----

// TestFullTextOr: "apple banana" — pure OR; every doc with apple OR banana.
func TestPortFullTextOr(t *testing.T) {
	require.ElementsMatch(t, []any{int64(0), int64(1), int64(11), int64(12)},
		bquery(t, abSeg(t), "apple banana"))
}

// TestFullTextPlusOr: "+apple banana" — apple MUST, banana SHOULD → {0,1}.
func TestPortFullTextPlusOr(t *testing.T) {
	require.ElementsMatch(t, []any{int64(0), int64(1)}, bquery(t, abSeg(t), "+apple banana"))
}

// TestFullTextMinus: "+apple -banana" — apple MUST, banana MUST-NOT → {1}.
func TestPortFullTextMinus(t *testing.T) {
	require.Equal(t, []any{int64(1)}, bquery(t, abSeg(t), "+apple -banana"))
}

// TestFullTextPlusPlus (classic apple/orange): "+apple -orange" — orange is in
// doc 0 (docvec col 1), so doc 0 is excluded; only doc 1 matches.
func TestPortFullTextPlusMinus(t *testing.T) {
	seg := tfSegment(t, []string{"apple", "orange"}, map[int64][]int{
		0: {2, 2}, 1: {3, 0}, 11: {0, 3}, 12: {0, 4},
	})
	require.Equal(t, []any{int64(1)}, bquery(t, seg, "+apple -orange"))
}

// "+apple +banana" — both MUST → only doc 0.
func TestPortFullTextPlusPlus(t *testing.T) {
	require.Equal(t, []any{int64(0)}, bquery(t, abSeg(t), "+apple +banana"))
}

// TestFullTextTilda: "+apple ~banana" — apple MUST, banana a score penalty, not an
// exclusion → docs 0 and 1 both match, the banana-free doc 1 ranked first.
func TestPortFullTextTilda(t *testing.T) {
	got := bqueryOrdered(t, abSeg(t), "+apple ~banana")
	require.ElementsMatch(t, []any{int64(0), int64(1)}, got)
	require.Equal(t, int64(1), got[0])
}

// ---- multi-term MUST/SHOULD/MUST-NOT (we/are/so/happy corpus) ----

// TestFullText1: "we are so happy" — all SHOULD (OR); every doc matches. Uses
// the Index path (k=100) since all 13 docs match — more than bquery's top-10.
func TestPortFullText1AllOr(t *testing.T) {
	idx := NewIndex([]*Segment{washSeg(t)}, nil)
	require.ElementsMatch(t,
		[]any{int64(0), int64(1), int64(10), int64(11), int64(12),
			int64(20), int64(21), int64(22), int64(23),
			int64(30), int64(31), int64(32), int64(33)},
		boolIDs(t, idx, "", "we are so happy"))
}

// TestFullText2: "+we +are +so +happy" — all MUST; only the doc with all four.
func TestPortFullText2AllMust(t *testing.T) {
	seg := tfSegment(t, []string{"we", "are", "so", "happy"}, map[int64][]int{
		0: {2, 2, 2, 2}, 1: {3, 0, 0, 0}, 11: {0, 3, 0, 0}, 12: {0, 4, 0, 0},
		21: {0, 0, 6, 0}, 22: {0, 0, 7, 0}, 23: {0, 0, 8, 0},
		31: {0, 0, 0, 2}, 32: {0, 0, 0, 3}, 33: {0, 0, 0, 4},
	})
	require.Equal(t, []any{int64(0)}, bquery(t, seg, "+we +are +so +happy"))
}

// TestFullText3: "+we -are -so -happy" — we MUST, the rest MUST-NOT → doc 1 only
// (doc 0 has 'are', so it is excluded).
func TestPortFullText3MustNot(t *testing.T) {
	seg := tfSegment(t, []string{"we", "are", "so", "happy"}, map[int64][]int{
		0: {2, 2, 0, 0}, 1: {3, 0, 0, 0}, 11: {0, 3, 0, 0}, 12: {0, 4, 0, 0},
		20: {0, 0, 5, 0}, 21: {0, 0, 6, 0}, 22: {0, 0, 7, 0}, 23: {0, 0, 8, 0},
		30: {0, 0, 0, 1}, 31: {0, 0, 0, 2}, 32: {0, 0, 0, 3}, 33: {0, 0, 0, 4},
	})
	require.Equal(t, []any{int64(1)}, bquery(t, seg, "+we -are -so -happy"))
}

// TestFullText5: "we are so +happy" — happy MUST but no doc contains it → empty.
func TestPortFullText5NoMatch(t *testing.T) {
	seg := tfSegment(t, []string{"we", "are", "so", "happy"}, map[int64][]int{
		0: {2, 0, 0, 0}, 1: {3, 0, 0, 0}, 11: {0, 3, 0, 0}, 12: {0, 4, 0, 0},
	})
	require.Empty(t, bquery(t, seg, "we are so +happy"))
}

// ---- groups + weighted / tilda groups (we/are/so corpus) ----

// wasSeg: doc 0 has we+are, doc 1 has we+so, 11/12 are-only, 20-23 so-only.
func wasSeg(t *testing.T) *Segment {
	return tfSegment(t, []string{"we", "are", "so"}, map[int64][]int{
		0: {2, 2, 0}, 1: {3, 0, 5}, 11: {0, 3, 0}, 12: {0, 4, 0},
		20: {0, 0, 5}, 21: {0, 0, 6}, 22: {0, 0, 7}, 23: {0, 0, 8},
	})
}

// TestFullTextGroup: "+we +(<are >so)" — we MUST and (are demoted OR so boosted)
// present → {0,1} (the are-only / so-only docs lack 'we').
func TestPortFullTextGroup(t *testing.T) {
	require.ElementsMatch(t, []any{int64(0), int64(1)}, bquery(t, wasSeg(t), "+we +(<are >so)"))
}

// TestFullTextGroupTilda: "+we ~(<are >so)" — we MUST, the group only adjusts
// score (no inclusion/exclusion) → still {0,1}.
func TestPortFullTextGroupTilda(t *testing.T) {
	require.ElementsMatch(t, []any{int64(0), int64(1)}, bquery(t, wasSeg(t), "+we ~(<are >so)"))
}

// TestFullTextJoinGroupTilda: "+we +also ~(<are >so)" — we MUST and also MUST →
// only the docs carrying both (0,1); the ~group merely adjusts score.
func TestPortFullTextJoinGroupTilda(t *testing.T) {
	seg := tfSegment(t, []string{"we", "also", "are", "so"}, map[int64][]int{
		0: {2, 2, 2, 0}, 1: {2, 2, 0, 5}, 11: {0, 0, 3, 0}, 12: {0, 0, 4, 0},
		20: {0, 0, 0, 5}, 21: {0, 0, 0, 6}, 22: {0, 0, 0, 7}, 23: {0, 0, 0, 8},
	})
	require.ElementsMatch(t, []any{int64(0), int64(1)}, bquery(t, seg, "+we +also ~(<are >so)"))
}

// ---- prefix + phrase ----

// TestFullTextStar: "apple*" — prefix matches apple and applesauce, not banana.
func TestPortFullTextStar(t *testing.T) {
	seg, err := BuildSegmentFromDocs("star", int32(types.T_int64), []Doc{
		{int64(0), []byte("apple")},
		{int64(1), []byte("applesauce pie")},
		{int64(2), []byte("banana bread")},
	}, tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	require.ElementsMatch(t, []any{int64(0), int64(1)}, bquery(t, seg, "apple*"))
}

// TestFullTextPhrase: "\"we are so happy\"" — exact contiguous phrase; only doc 0
// has the four words adjacent in order.
func TestPortFullTextPhrase(t *testing.T) {
	seg, err := BuildSegmentFromDocs("phrase", int32(types.T_int64), []Doc{
		{int64(0), []byte("we are so happy")},
		{int64(1), []byte("we are happy so")},
		{int64(2), []byte("we are so")},
		{int64(3), []byte("so happy we are")},
	}, tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	require.Equal(t, []any{int64(0)}, bquery(t, seg, `"we are so happy"`))
}
