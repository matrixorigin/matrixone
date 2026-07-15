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

// Boolean-mode PARITY with classic fulltext (pkg/fulltext/fulltext_test.go):
// fulltext2 uses its own parser + WAND evaluator, but must produce the same
// MySQL boolean-mode membership as the classic engine for the same queries
// (+apple banana, +apple -banana, +apple ~banana, nested +(...), CJK +/-).
package fulltext2

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/stretchr/testify/require"
)

// classicCorpus mirrors the apple/banana doc shape used by classic fulltext's
// TestFullTextPlusOr / TestFullTextMinus / TestFullTextTilda (doc 0 has both,
// doc 1 apple-only, docs 11/12 banana-only), plus a matrix/origin group set.
func classicCorpus(t *testing.T) *Segment {
	docs := []Doc{
		{int64(0), []byte("apple banana")},
		{int64(1), []byte("apple apple apple")},
		{int64(11), []byte("banana banana banana")},
		{int64(12), []byte("banana banana banana banana")},
		{int64(20), []byte("matrix origin")},
		{int64(21), []byte("matrix one")},
		{int64(22), []byte("matrix two")},
		{int64(23), []byte("matrix alone")},
		{int64(24), []byte("origin standalone")},
	}
	s, err := BuildSegmentFromDocs("classic", int32(types.T_int64), docs, tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	return s
}

// TestParityPlusOr: "+apple banana" — apple MUST, banana SHOULD (OR). Docs with
// apple match (0,1); banana-only docs (11,12) do NOT. (classic TestFullTextPlusOr)
func TestParityPlusOr(t *testing.T) {
	require.ElementsMatch(t, []any{int64(0), int64(1)}, bquery(t, classicCorpus(t), "+apple banana"))
}

// TestParityMinus: "+apple -banana" — apple MUST, banana MUST-NOT. Only doc 1
// (apple without banana). (classic TestFullTextMinus)
func TestParityMinus(t *testing.T) {
	require.Equal(t, []any{int64(1)}, bquery(t, classicCorpus(t), "+apple -banana"))
}

// TestParityTilde: "+apple ~banana" — apple MUST, banana a penalty (not an
// exclusion): docs 0 AND 1 match, 0 ranked below 1. (classic TestFullTextTilda)
func TestParityTilde(t *testing.T) {
	got := bqueryOrdered(t, classicCorpus(t), "+apple ~banana")
	require.ElementsMatch(t, []any{int64(0), int64(1)}, got)
	require.Equal(t, int64(1), got[0], "the banana-free doc ranks first under ~")
}

// TestParityNestedGroup: "+matrix +(origin (one two))" — matrix AND (origin OR
// one OR two). Nested groups flatten to an OR. (classic TestPatternBoolean shape)
func TestParityNestedGroup(t *testing.T) {
	require.ElementsMatch(t, []any{int64(20), int64(21), int64(22)},
		bquery(t, classicCorpus(t), "+matrix +(origin (one two))"))
}

// TestParityPlusPlus: "+apple +banana" — both MUST → only doc 0.
func TestParityPlusPlus(t *testing.T) {
	require.Equal(t, []any{int64(0)}, bquery(t, classicCorpus(t), "+apple +banana"))
}
