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
	"bytes"
	"strings"
	"testing"
)

// refNormalizeLatin is the normalization CONTRACT: cap the raw Latin run at MAX_TOKEN_SIZE, lowercase
// it, then re-cap the folded bytes with TruncateLatinToken. This is the exact byte sequence the index
// stores and the query looks up (#29271 / #29276). The allocation-free normalizeLatinTokenInto (and the
// NormalizeLatinToken wrapper over it) must be byte-identical to it.
func refNormalizeLatin(raw []byte) []byte {
	bs := TruncateLatinToken(raw)
	return TruncateLatinToken([]byte(strings.ToLower(string(bs))))
}

// TestNormalizeLatinTokenMatchesContract pins NormalizeLatinToken byte-for-byte to refNormalizeLatin.
// It covers ASCII over/at the cap, 2-byte runes, mixed width, and -- the case a rune-boundary re-cap
// would get wrong -- expanding folds (U+023A->U+2C65) followed by a 2-byte rune whose last byte lands
// on the cap, where TruncateLatinToken drops the trailing char.
func TestNormalizeLatinTokenMatchesContract(t *testing.T) {
	for _, raw := range [][]byte{
		[]byte(""),
		[]byte("hello"),
		[]byte("HELLO"),
		[]byte("MixedCASEword"),
		[]byte(strings.Repeat("b", 26)),       // ASCII over the cap
		[]byte(strings.Repeat("a", 23)),       // exactly at the cap
		[]byte(strings.Repeat("x", 22)),       // under the cap
		[]byte(strings.Repeat("я", 12)),       // 2-byte runes over the cap
		[]byte("a" + strings.Repeat("я", 12)), // mixed-width boundary
		[]byte(strings.Repeat("é", 13)),       // 2-byte runes, over cap
		[]byte(strings.Repeat("Ⱥ", 7)),        // expanding fold, fits
		[]byte(strings.Repeat("Ⱥ", 8)),        // expanding fold over the cap
		[]byte(strings.Repeat("Ⱥ", 9)),
		[]byte(strings.Repeat("Ⱥ", 7) + strings.Repeat("я", 4)), // expanding + 2-byte landing on the cap
		[]byte(strings.Repeat("Ⱥ", 6) + strings.Repeat("я", 5)),
		[]byte(strings.Repeat("Ⱥ", 3) + strings.Repeat("é", 6)),
	} {
		want := refNormalizeLatin(raw)
		got := NormalizeLatinToken(raw)
		if !bytes.Equal(got, want) {
			t.Errorf("NormalizeLatinToken(%q) = %q (%dB); want %q (%dB)", raw, got, len(got), want, len(want))
		}
	}
}
