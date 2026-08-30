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
func collect(t *testing.T, doc string) []string {
	t.Helper()
	bj, err := ParseFromString(doc)
	require.NoError(t, err)
	out := make([]string, 0, 8)
	for l := range bj.TokenizeLeaves() {
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
		out = append(out, fmt.Sprintf("%s|%s", string(l.Tag), v))
	}
	return out
}

// Array elements inherit the enclosing key and its ancestor path; the subscript
// is below the tag and is deliberately not represented.
func TestTokenizeLeavesArraysInheritTheKey(t *testing.T) {
	require.Equal(t, []string{"a|i:1", "a|i:2"}, collect(t, `{"a":[1,2]}`))
	require.Equal(t, []string{"b|i:1", "b|i:2"},
		collect(t, `{"a":[{"b":1},{"b":2}]}`))
	// nested arrays collapse the same way
	require.Equal(t, []string{"a|i:1", "a|i:2"}, collect(t, `{"a":[[1],[2]]}`))
}

// Literals and non-scalar containers are skipped, matching TokenizeValue.
func TestTokenizeLeavesSkipsLiterals(t *testing.T) {
	require.Equal(t, []string{"b|i:1"}, collect(t, `{"a":null,"b":1,"c":true,"d":false}`))
	require.Empty(t, collect(t, `{}`))
	require.Empty(t, collect(t, `[]`))
}

// A bare scalar document has no key at all.
func TestTokenizeLeavesRootScalar(t *testing.T) {
	require.Equal(t, []string{"|i:5"}, collect(t, `5`))
	require.Equal(t, []string{"|s:hi"}, collect(t, `"hi"`))
}

// Early stop must not walk the rest of the document.
func TestTokenizeLeavesEarlyStop(t *testing.T) {
	bj, err := ParseFromString(`{"a":1,"b":2,"c":3}`)
	require.NoError(t, err)
	n := 0
	for range bj.TokenizeLeaves() {
		n++
		break
	}
	require.Equal(t, 1, n)
}
