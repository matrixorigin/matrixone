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
	"testing"

	"github.com/stretchr/testify/require"
)

// A string scan gets $."a.b" wrong; the canonical parse must not.
func TestTerminalKey(t *testing.T) {
	for _, tc := range []struct {
		path, key string
		ok        bool
	}{
		{"$.foo", "foo", true},
		{"$.a.b.c", "c", true},
		{`$."a.b"`, "a.b", true}, // the quoted-key case a string split breaks
		{`$.x."a.b"`, "a.b", true},
		{"$.a[0]", "a", true}, // trailing subscript keeps the enclosing key
		{"$.a[0].b", "b", true},
		{"$.a[0][1]", "a", true},
		{"$", "", false},
		{"$[0]", "", false},
		{"$.a[*]", "", false}, // non-deterministic
		{"$.a.*", "", false},
		{"$**.b", "", false},
	} {
		p, err := ParseJsonPath(tc.path)
		if err != nil {
			require.False(t, tc.ok, "%s: parse error %v", tc.path, err)
			continue
		}
		k, ok := p.TerminalKey()
		require.Equal(t, tc.ok, ok, tc.path)
		if tc.ok {
			require.Equal(t, tc.key, k, tc.path)
		}
	}
}
