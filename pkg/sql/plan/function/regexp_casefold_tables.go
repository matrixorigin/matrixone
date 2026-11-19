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

package function

// Code generated from golang.org/x/text v0.37.0 cases.Fold; DO NOT EDIT by
// hand. This is every full case folding whose result has more than one rune,
// including U+1E9E (LATIN CAPITAL LETTER SHARP S): ICU 78.3
// UREGEX_CASE_INSENSITIVE matches it against both U+00DF and "ss" (probed
// for issue #31175; the earlier exclusion was a misprobe of the reference
// behavior). The reverse index regexpMultiCharFoldSources maps each fold
// image back to its sources; some images have several sources (for example
// "ss" from U+00DF and U+1E9E, and "st" from U+FB05 and U+FB06).
var regexpMultiCharFold = map[rune]string{
	0x00DF: "ss",
	0x0130: "i̇",
	0x0149: "ʼn",
	0x01F0: "ǰ",
	0x0390: "ΐ",
	0x03B0: "ΰ",
	0x0587: "եւ",
	0x1E96: "ẖ",
	0x1E97: "ẗ",
	0x1E98: "ẘ",
	0x1E99: "ẙ",
	0x1E9A: "aʾ",
	0x1E9E: "ss",
	0x1F50: "ὐ",
	0x1F52: "ὒ",
	0x1F54: "ὔ",
	0x1F56: "ὖ",
	0x1F80: "ἀι",
	0x1F81: "ἁι",
	0x1F82: "ἂι",
	0x1F83: "ἃι",
	0x1F84: "ἄι",
	0x1F85: "ἅι",
	0x1F86: "ἆι",
	0x1F87: "ἇι",
	0x1F88: "ἀι",
	0x1F89: "ἁι",
	0x1F8A: "ἂι",
	0x1F8B: "ἃι",
	0x1F8C: "ἄι",
	0x1F8D: "ἅι",
	0x1F8E: "ἆι",
	0x1F8F: "ἇι",
	0x1F90: "ἠι",
	0x1F91: "ἡι",
	0x1F92: "ἢι",
	0x1F93: "ἣι",
	0x1F94: "ἤι",
	0x1F95: "ἥι",
	0x1F96: "ἦι",
	0x1F97: "ἧι",
	0x1F98: "ἠι",
	0x1F99: "ἡι",
	0x1F9A: "ἢι",
	0x1F9B: "ἣι",
	0x1F9C: "ἤι",
	0x1F9D: "ἥι",
	0x1F9E: "ἦι",
	0x1F9F: "ἧι",
	0x1FA0: "ὠι",
	0x1FA1: "ὡι",
	0x1FA2: "ὢι",
	0x1FA3: "ὣι",
	0x1FA4: "ὤι",
	0x1FA5: "ὥι",
	0x1FA6: "ὦι",
	0x1FA7: "ὧι",
	0x1FA8: "ὠι",
	0x1FA9: "ὡι",
	0x1FAA: "ὢι",
	0x1FAB: "ὣι",
	0x1FAC: "ὤι",
	0x1FAD: "ὥι",
	0x1FAE: "ὦι",
	0x1FAF: "ὧι",
	0x1FB2: "ὰι",
	0x1FB3: "αι",
	0x1FB4: "άι",
	0x1FB6: "ᾶ",
	0x1FB7: "ᾶι",
	0x1FBC: "αι",
	0x1FC2: "ὴι",
	0x1FC3: "ηι",
	0x1FC4: "ήι",
	0x1FC6: "ῆ",
	0x1FC7: "ῆι",
	0x1FCC: "ηι",
	0x1FD2: "ῒ",
	0x1FD3: "ΐ",
	0x1FD6: "ῖ",
	0x1FD7: "ῗ",
	0x1FE2: "ῢ",
	0x1FE3: "ΰ",
	0x1FE4: "ῤ",
	0x1FE6: "ῦ",
	0x1FE7: "ῧ",
	0x1FF2: "ὼι",
	0x1FF3: "ωι",
	0x1FF4: "ώι",
	0x1FF6: "ῶ",
	0x1FF7: "ῶι",
	0x1FFC: "ωι",
	0xFB00: "ff",
	0xFB01: "fi",
	0xFB02: "fl",
	0xFB03: "ffi",
	0xFB04: "ffl",
	0xFB05: "st",
	0xFB06: "st",
	0xFB13: "մն",
	0xFB14: "մե",
	0xFB15: "մի",
	0xFB16: "վն",
	0xFB17: "մխ",
}

// regexpMultiCharFoldSources is built once from regexpMultiCharFold.
var regexpMultiCharFoldSources = func() map[string][]rune {
	m := make(map[string][]rune, len(regexpMultiCharFold))
	for src, image := range regexpMultiCharFold {
		m[image] = append(m[image], src)
	}
	for _, sources := range m {
		sortRunes(sources)
	}
	return m
}()

func sortRunes(rs []rune) {
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && rs[j] < rs[j-1]; j-- {
			rs[j], rs[j-1] = rs[j-1], rs[j]
		}
	}
}

// Code generated from golang.org/x/text v0.37.0 cases.Fold versus
// unicode.ToLower; DO NOT EDIT by hand. regexpSingleCharFoldExtras maps a
// single-rune full-fold image d to every rune x whose full fold is d and
// which the unicode.ToLower closure misses (x is an extra of d exactly when
// cases.Fold(x) is the single rune d, x != d, and ToLower(x) != ToLower(d);
// the Cherokee fold pairs fold in both directions, so each partner's
// ToLower orbit already reaches the other and neither qualifies). The
// vendored regexp2 engine's caseless comparison is that ToLower closure
// (parser.go/runner.go), so a rewritten literal rune d must list these
// extras case-sensitively or it stops matching them — including the
// identical text (ς rewrites to σ, which ToLower-matches σ and Σ but not
// ς). Every listed extra has a singleton ToLower orbit (probed), so the
// (?-i:x) emission adds exactly that rune. Go regexp's simple-fold orbit
// reaches all of these runes, so the RE2 route ignores this table.
var regexpSingleCharFoldExtras = map[rune][]rune{
	0x0073: {0x017f},
	0x03b2: {0x03d0},
	0x03b5: {0x03f5},
	0x03b8: {0x03d1},
	0x03b9: {0x0345, 0x1fbe},
	0x03ba: {0x03f0},
	0x03bc: {0x00b5},
	0x03c0: {0x03d6},
	0x03c1: {0x03f1},
	0x03c3: {0x03c2},
	0x03c6: {0x03d5},
	0x0432: {0x1c80},
	0x0434: {0x1c81},
	0x043e: {0x1c82},
	0x0441: {0x1c83},
	0x0442: {0x1c84, 0x1c85},
	0x044a: {0x1c86},
	0x0463: {0x1c87},
	0x1e61: {0x1e9b},
	0xa64b: {0x1c88},
}
