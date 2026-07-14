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

	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/stretchr/testify/require"
)

func bquery(t *testing.T, s *Segment, q string) []any {
	t.Helper()
	rs, err := s.SearchBooleanText([]byte(q), tokenizer.NewSimpleTokenizer(), TfIdf, 10)
	require.NoError(t, err)
	return pkslice(rs)
}

// TestBooleanMode covers the + - " " * and bare-OR surface over the shared
// fulltext corpus.
func TestBooleanMode(t *testing.T) {
	s := fulltextCorpus(t)

	// MUST (AND)
	require.ElementsMatch(t, []any{int64(1), int64(2), int64(4)}, bquery(t, s, "+quick +fox"))
	// MUST + MUST-NOT
	require.Equal(t, []any{int64(2)}, bquery(t, s, "+quick -jumps"))
	// SHOULD (OR) minus MUST-NOT
	require.ElementsMatch(t, []any{int64(2), int64(4)}, bquery(t, s, "fox -dog"))
	// a required absent term → no matches
	require.Empty(t, bquery(t, s, "+fox +cat"))

	// quoted phrase (bare) and required phrase
	require.ElementsMatch(t, []any{int64(1), int64(3)}, bquery(t, s, `"lazy dog"`))
	require.ElementsMatch(t, []any{int64(1), int64(2), int64(4)}, bquery(t, s, `+"quick brown fox"`))

	// bare OR of two separate words
	require.ElementsMatch(t, []any{int64(3), int64(4)}, bquery(t, s, "sleeps high"))

	// prefix: qui* → quick
	require.ElementsMatch(t, []any{int64(1), int64(2), int64(4)}, bquery(t, s, "qui*"))
	// prefix in a MUST, combined with another MUST: (day|dog) AND fox → docs 1,5
	require.ElementsMatch(t, []any{int64(1), int64(5)}, bquery(t, s, "+d* +fox"))
}

// TestBooleanNonContiguousPhrase: a phrase clause still requires adjacency.
func TestBooleanNonContiguousPhrase(t *testing.T) {
	s := fulltextCorpus(t)
	// "quick fox" as a phrase is never adjacent (brown between) → no matches,
	// even though both terms exist.
	require.Empty(t, bquery(t, s, `+"quick fox"`))
}

// TestBooleanOnLoadedSegment exercises the loaded (FST) path, including prefix
// expansion over the FST iterator.
func TestBooleanOnLoadedSegment(t *testing.T) {
	data, err := fulltextCorpus(t).Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("seg", bytes.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })

	require.Equal(t, []any{int64(2)}, bquery(t, loaded, "+quick -jumps"))
	require.ElementsMatch(t, []any{int64(1), int64(2), int64(4)}, bquery(t, loaded, "qui*"))
	require.ElementsMatch(t, []any{int64(1), int64(5)}, bquery(t, loaded, "+d* +fox"))
}

// TestScanBoolean checks the top-level clause scanner directly.
func TestScanBoolean(t *testing.T) {
	got := scanBoolean(`+quick -"lazy dog" fox*`)
	require.Len(t, got, 3)
	require.Equal(t, rawClause{prefix: '+', text: "quick"}, got[0])
	require.Equal(t, rawClause{prefix: '-', quoted: true, text: "lazy dog"}, got[1])
	require.Equal(t, rawClause{star: true, text: "fox"}, got[2])
}
