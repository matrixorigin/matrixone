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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestJSONValueExactMatch pins the json_value parser's contract to classic fulltext:
// each json leaf value is ONE whole atomic token (no ngram, punctuation/CJK kept), and
// a query operand matches a WHOLE value exactly — never a substring/bag. This is the
// divergence fixed in parity with classic (tokenize.go stores the whole value; sql.go
// queries `word = value`).
func TestJSONValueExactMatch(t *testing.T) {
	docs := []Doc{
		{int64(1), []byte(`{"a":"redbluebrownyelloworange","b":"winter"}`)},
		{int64(2), []byte(`{"a":"red","b":"winter"}`)},
		{int64(3), []byte(`{"a":"中文學習教材"}`)},
		{int64(4), []byte(`{"a":"happybirthday.happy-birthday_happybirthday"}`)},
	}

	run := func(t *testing.T, idx *Index) {
		bl := func(q string) []any {
			rs, err := idx.SearchQuery([]byte(q), true, ParserJSONValue, BM25, 100, nil)
			require.NoError(t, err)
			return resultIDs(rs)
		}
		nl := func(q string) []any {
			rs, err := idx.SearchQuery([]byte(q), false, ParserJSONValue, BM25, 100, nil)
			require.NoError(t, err)
			return resultIDs(rs)
		}

		// Whole-value exact match — including punctuation and CJK values as ONE token.
		require.ElementsMatch(t, []any{int64(1)}, bl("redbluebrownyelloworange"))
		require.ElementsMatch(t, []any{int64(3)}, bl("中文學習教材"))
		require.ElementsMatch(t, []any{int64(4)}, bl("happybirthday.happy-birthday_happybirthday"))

		// "red" is a WHOLE value only in doc 2 — NOT a substring hit on doc 1.
		require.ElementsMatch(t, []any{int64(2)}, bl("red"))
		require.ElementsMatch(t, []any{int64(2)}, nl("red"))

		// Distinct-value OR / multi-operand MUST.
		require.ElementsMatch(t, []any{int64(1), int64(2)}, bl("winter")) // both docs carry value "winter"
		require.ElementsMatch(t, []any{int64(2)}, bl("+red +winter"))     // exact red AND exact winter

		// No bag-of-words / no ngram: a substring of a value never matches.
		require.Empty(t, bl("bluebrown"), "substring of doc1's value is not a whole value")
		require.Empty(t, bl("中文學習"), "substring of doc3's value is not a whole value")
		require.Empty(t, bl("happybirthday"), "substring of doc4's value is not a whole value")
	}

	seg, err := BuildSegmentFromDocsParser("jv", int32(types.T_int64), docs, ParserJSONValue)
	require.NoError(t, err)
	t.Run("build-side", func(t *testing.T) { run(t, NewIndex([]*Segment{seg}, nil)) })

	blob, err := seg.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("jv", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })
	t.Run("loaded-side", func(t *testing.T) { run(t, NewIndex([]*Segment{loaded}, nil)) })
}
