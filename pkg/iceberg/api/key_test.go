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

package api

import "testing"

func TestLengthPrefixedKey(t *testing.T) {
	for _, tc := range []struct {
		parts []string
		want  string
	}{
		{nil, ""}, {[]string{""}, "0:"}, {[]string{"ab", "c"}, "2:ab1:c"},
		{[]string{"a", "bc"}, "1:a2:bc"}, {[]string{"中", "a:b\x00"}, "3:中4:a:b\x00"},
	} {
		if got := LengthPrefixedKey(tc.parts...); got != tc.want {
			t.Fatalf("%q: got %q, want %q", tc.parts, got, tc.want)
		}
	}
}
