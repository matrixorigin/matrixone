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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// collect renders each leaf as "path|tag|kind:value" so a test can assert the
// whole stream in one comparison.
func collect(t *testing.T, doc string, withPath bool) []string {
	t.Helper()
	bj, err := ParseFromString(doc)
	require.NoError(t, err)
	out := make([]string, 0, 8)
	for l := range bj.TokenizeLeaves(withPath) {
		var v string
		switch l.Kind {
		case LeafString:
			v = "s:" + string(l.Str)
		case LeafInt64:
			v = fmt.Sprintf("i:%d", l.I64)
		case LeafUint64:
			v = fmt.Sprintf("u:%d", l.U64)
		case LeafFloat64:
			v = fmt.Sprintf("f:%g", l.F64)
		}
		out = append(out, fmt.Sprintf("%s|%s|%s", string(l.AncestorPath), string(l.Tag), v))
	}
	return out
}

// The defect this whole change exists to fix: TokenizeValue cannot tell
// {"b":"XXX"} from {"c":"XXX"}, TokenizeLeaves can.
func TestTokenizeLeavesCarriesTheKey(t *testing.T) {
	require.Equal(t, []string{"a|b|s:XXX", "a|c|s:YYY"},
		collect(t, `{"a":{"b":"XXX","c":"YYY"}}`, true))

	// same values, different keys => different leaves
	b := collect(t, `{"b":"XXX"}`, true)
	c := collect(t, `{"c":"XXX"}`, true)
	require.NotEqual(t, b, c)

	// ...whereas the value-only tokenizer reports them identically
	valuesOf := func(doc string) []string {
		bj, err := ParseFromString(doc)
		require.NoError(t, err)
		var out []string
		for tk := range bj.TokenizeValue(false) {
			n := int(tk.TokenBytes[0])
			out = append(out, string(tk.TokenBytes[1:1+n]))
		}
		return out
	}
	require.Equal(t, valuesOf(`{"b":"XXX"}`), valuesOf(`{"c":"XXX"}`))
}

func TestTokenizeLeavesTypesArePreserved(t *testing.T) {
	// numbers must NOT be stringified: the whole point of the tuple encoding is
	// that a float leaf stays a float so it can be range-scanned.
	//
	// Note the order: ByteJson stores object members sorted BY KEY, so leaves
	// come out f, i, s regardless of how the document was written. The index
	// build relies on that determinism — two spellings of the same document must
	// produce the same term stream.
	got := collect(t, `{"s":"x","i":-7,"f":3.5}`, false)
	require.Equal(t, []string{"|f|f:3.5", "|i|i:-7", "|s|s:x"}, got)

	require.Equal(t, got, collect(t, `{"f":3.5,"s":"x","i":-7}`, false))
}

func TestTokenizeLeavesNestedPath(t *testing.T) {
	require.Equal(t, []string{"a.b|c|i:1"}, collect(t, `{"a":{"b":{"c":1}}}`, true))
	// top-level member has no ancestor path
	require.Equal(t, []string{"|a|i:1"}, collect(t, `{"a":1}`, true))
}

// Array elements inherit the enclosing key and its ancestor path; the subscript
// is below the tag and is deliberately not represented.
func TestTokenizeLeavesArraysInheritTheKey(t *testing.T) {
	require.Equal(t, []string{"|a|i:1", "|a|i:2"}, collect(t, `{"a":[1,2]}`, true))
	require.Equal(t, []string{"a|b|i:1", "a|b|i:2"},
		collect(t, `{"a":[{"b":1},{"b":2}]}`, true))
	// nested arrays collapse the same way
	require.Equal(t, []string{"|a|i:1", "|a|i:2"}, collect(t, `{"a":[[1],[2]]}`, true))
}

// withPath=false must produce the same leaves, minus the path — a leaf-only
// index and a full-path index must agree on tag and value.
func TestTokenizeLeavesWithoutPathMatchesWithPath(t *testing.T) {
	const doc = `{"a":{"b":"X","c":[1,2]},"d":2.5}`
	with := collect(t, doc, true)
	without := collect(t, doc, false)
	require.Equal(t, len(with), len(without))
	for i := range with {
		// strip the leading "path|" from the with-path rendering
		require.Equal(t, without[i][1:], with[i][len(with[i])-len(without[i])+1:])
	}
}

// Literals and non-scalar containers are skipped, matching TokenizeValue.
func TestTokenizeLeavesSkipsLiterals(t *testing.T) {
	require.Equal(t, []string{"|b|i:1"}, collect(t, `{"a":null,"b":1,"c":true,"d":false}`, true))
	require.Empty(t, collect(t, `{}`, true))
	require.Empty(t, collect(t, `[]`, true))
}

// A bare scalar document has no key at all.
func TestTokenizeLeavesRootScalar(t *testing.T) {
	require.Equal(t, []string{"||i:5"}, collect(t, `5`, true))
	require.Equal(t, []string{"||s:hi"}, collect(t, `"hi"`, true))
}

// Early stop must not walk the rest of the document.
func TestTokenizeLeavesEarlyStop(t *testing.T) {
	bj, err := ParseFromString(`{"a":1,"b":2,"c":3}`)
	require.NoError(t, err)
	n := 0
	for range bj.TokenizeLeaves(true) {
		n++
		break
	}
	require.Equal(t, 1, n)
}

// The path buffer is reused across leaves; a caller that keeps the slice must
// see a correct value DURING iteration (the contract Str/AncestorPath document).
func TestTokenizeLeavesPathIsCorrectDuringIteration(t *testing.T) {
	bj, err := ParseFromString(`{"a":{"x":1},"bb":{"y":2}}`)
	require.NoError(t, err)
	seen := make([]string, 0, 2)
	for l := range bj.TokenizeLeaves(true) {
		seen = append(seen, string(l.AncestorPath)+"/"+string(l.Tag))
	}
	// "bb" is longer than "a": a stale buffer would leak the previous path's tail
	require.Equal(t, []string{"a/x", "bb/y"}, seen)
}
