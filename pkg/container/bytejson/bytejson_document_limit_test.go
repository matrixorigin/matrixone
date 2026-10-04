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

package bytejson

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func nestedJSONArrayForLimit(depth int, leaf string) string {
	return strings.Repeat("[", depth) + leaf + strings.Repeat("]", depth)
}

func TestParseFromByteSliceWithDepthLimit(t *testing.T) {
	atLimit := []byte(nestedJSONArrayForLimit(JSONDocumentMaxNestingDepth, "1"))
	value, err := ParseFromByteSliceWithDepthLimit(atLimit, JSONDocumentMaxNestingDepth)
	require.NoError(t, err)
	require.Equal(t, TpCodeArray, value.Type)

	tooDeep := []byte(nestedJSONArrayForLimit(JSONDocumentMaxNestingDepth+1, "1"))
	_, err = ParseFromByteSliceWithDepthLimit(tooDeep, JSONDocumentMaxNestingDepth)
	require.ErrorContains(t, err, "json document nesting depth exceeds 100")
	require.True(t, IsJSONDocumentDepthError(err))
}

func TestParseFromByteSliceWithDepthLimitRejectsInvalidLimit(t *testing.T) {
	_, err := ParseFromByteSliceWithDepthLimit([]byte(`[1]`), 0)
	require.ErrorContains(t, err, "max depth must be positive")

	_, err = ParseFromByteSliceWithDepthLimit([]byte(`[1]`), -1)
	require.ErrorContains(t, err, "max depth must be positive")
}

func TestParseFromByteSliceWithDepthLimitStopsBeforeWideSuffix(t *testing.T) {
	wideLeaf := strings.Repeat("x", 256<<10)
	input := []byte(strings.Repeat("[", JSONDocumentMaxNestingDepth+1) + `"` + wideLeaf + `"` +
		strings.Repeat("]", JSONDocumentMaxNestingDepth+1))
	parser := parser{src: input, maxDepth: JSONDocumentMaxNestingDepth}
	_, err := parser.do()
	require.ErrorContains(t, err, "json document nesting depth exceeds 100")
	require.Greater(t, parser.tz.Remaining(), len(wideLeaf),
		"depth rejection must occur before tokenizing the wide leaf")
}

func TestParseFromByteSliceWithDepthLimitMatchesExistingParser(t *testing.T) {
	tests := [][]byte{
		[]byte(`null`),
		[]byte(`{"a":[1,true,"x"]}`),
		[]byte(`{"a":`),
		[]byte(`[1,]`),
		[]byte(`1 2`),
	}

	for _, input := range tests {
		want, wantErr := ParseFromByteSlice(input)
		got, gotErr := ParseFromByteSliceWithDepthLimit(input, JSONDocumentMaxNestingDepth)
		require.Equal(t, wantErr != nil, gotErr != nil, "input %q", input)
		if wantErr == nil {
			require.Equal(t, want.Type, got.Type, "input %q", input)
			require.Equal(t, want.Data, got.Data, "input %q", input)
		}
	}
}

func TestValidateJSONDocumentDepth(t *testing.T) {
	atLimit, err := ParseFromByteSlice([]byte(nestedJSONArrayForLimit(JSONDocumentMaxNestingDepth, "1")))
	require.NoError(t, err)
	require.NoError(t, ValidateJSONDocumentDepth(atLimit))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		require.NoError(t, ValidateJSONDocumentDepth(atLimit))
	}))

	tooDeep, err := ParseFromByteSlice([]byte(nestedJSONArrayForLimit(JSONDocumentMaxNestingDepth+1, "1")))
	require.NoError(t, err)
	require.ErrorContains(t, ValidateJSONDocumentDepth(tooDeep), "json document nesting depth exceeds 100")
}

func BenchmarkParseFromByteSliceWithDepthLimitDeepWide(b *testing.B) {
	wideLeaf := strings.Repeat("x", 256<<10)
	input := []byte(strings.Repeat("[", JSONDocumentMaxNestingDepth+1) + `"` + wideLeaf + `"` +
		strings.Repeat("]", JSONDocumentMaxNestingDepth+1))
	b.ReportAllocs()
	b.SetBytes(int64(len(input)))
	for b.Loop() {
		_, err := ParseFromByteSliceWithDepthLimit(input, JSONDocumentMaxNestingDepth)
		if err == nil {
			b.Fatal("expected depth error")
		}
	}
}
