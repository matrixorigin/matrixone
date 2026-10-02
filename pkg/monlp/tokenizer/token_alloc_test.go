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

package tokenizer

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSimpleTokenizeAllocations guards against a per-token heap allocation: outputLatin must write the
// lowercased token into the fixed TokenBytes buffer, not materialize a fresh slice per token (#29271).
// Tokenizing 256 words must cost a small constant number of allocations (the iterator closure + the
// per-call simpleState), not one-or-two per token. ASCII (no fold / fold) and CJK are the controls.
func TestSimpleTokenizeAllocations(t *testing.T) {
	for _, tc := range []struct{ name, text string }{
		{"ascii_lower", strings.Repeat("hello world database search ", 64)},
		{"ascii_upper", strings.Repeat("HELLO WORLD DATABASE SEARCH ", 64)},
		{"cjk", strings.Repeat("苹果香蕉水果数据库", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tok := NewSimpleTokenizer()
			input := []byte(tc.text)
			allocs := testing.AllocsPerRun(20, func() {
				for _, err := range tok.Tokenize(input) {
					if err != nil {
						panic(err)
					}
				}
			})
			t.Logf("%s: %.1f allocs for 256 words", tc.name, allocs)
			require.LessOrEqualf(t, allocs, 8.0,
				"%s: %.1f allocs for 256 words — per-token allocation regressed (#29271)", tc.name, allocs)
		})
	}
}
