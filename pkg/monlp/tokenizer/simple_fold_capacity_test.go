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
	"unicode/utf8"
)

// Case folding can EXPAND a Latin run past the fixed token buffer: U+023A (Ⱥ, 2
// bytes) lowercases to U+2C65 (ⱥ, 3 bytes). A run of 8×U+023A is 16 input bytes
// (well under the cap before folding) but folds to 24 bytes. outputLatin must cap
// the FOLDED token at MAX_TOKEN_SIZE, so TokenBytes[0] never claims more than the
// 23-byte payload the buffer holds. Before the fix it stored length 24, and a
// reader panicked slicing TokenBytes[1:25] from the 24-byte array (#29271 P2).
func TestLatinFoldExpandCapacity29271(t *testing.T) {
	// 8×U+023A folds to 8×U+2C65 (24B) and is re-capped to 7×U+2C65 (21B).
	checkTokenize(t, strings.Repeat("Ⱥ", 8), []Token{
		makeToken(strings.Repeat("ⱥ", 7), 0),
	})
	// 7×U+023A folds to exactly 21B and fits -- the boundary control, unchanged by
	// the fix (0 rows on both base and head in the bug report).
	checkTokenize(t, strings.Repeat("Ⱥ", 7), []Token{
		makeToken(strings.Repeat("ⱥ", 7), 0),
	})

	// Invariant across expand lengths and mixed runs: the stored length never
	// exceeds the buffer, reading the declared payload never slices out of range,
	// and no partial multi-byte rune is stored.
	inputs := []string{
		strings.Repeat("Ⱥ", 8),
		strings.Repeat("Ⱥ", 9),
		strings.Repeat("Ⱥ", 12),
		strings.Repeat("Ⱥ", 40),
		"a" + strings.Repeat("Ⱥ", 11),
		strings.Repeat("Ⱥ", 10) + "z",
		"abcⱥȺßẞﬀ" + strings.Repeat("Ⱥ", 6),
	}
	for _, in := range inputs {
		for _, tk := range tokenize([]byte(in)) {
			slen := int(tk.TokenBytes[0])
			if slen > MAX_TOKEN_SIZE {
				t.Fatalf("tokenize(%q): stored length %d exceeds MAX_TOKEN_SIZE %d", in, slen, MAX_TOKEN_SIZE)
			}
			// Must not panic, and the declared payload must be a complete UTF-8 string.
			word := string(tk.TokenBytes[1 : slen+1])
			if !utf8.ValidString(word) {
				t.Fatalf("tokenize(%q): stored token %q is not valid UTF-8", in, word)
			}
		}
	}
}
