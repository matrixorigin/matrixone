// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collation

import (
	"context"
	"encoding/binary"
	"slices"
	"strings"
	"unicode/utf8"
)

// SQL LIKE pattern matching on collation character weights. MySQL compares
// LIKE operands one pattern character against one string character using the
// collation's per-character weights (my_wildcmp_uca in strings/ctype-uca.cc),
// so an accented or dotted character matches exactly when the resolved
// collation assigns it the same weights. A character that expands to several
// weights (for example U+00DF under utf8mb4_unicode_ci) still compares as a
// single character: LIKE never spans an expansion across characters, unlike
// the whole-string collation-key comparison used by = and FIND_IN_SET
// (issue #31175 review).

// LikeMalformedByteWeight is the shared synthetic weight of one malformed
// byte in LIKE comparison. It sits above every valid character weight (all
// of which fit in uint16) and is deliberately one value for every malformed
// byte: the legacy regex matcher decodes each malformed byte as U+FFFD, so
// a malformed pattern byte matches a malformed subject byte. The whole-key
// fallback in general_ci.go keeps per-byte weights instead, because a sort
// comparator must stay transitive.
const LikeMalformedByteWeight = 0x10000

// LikeUnweightedIdentityBase is the base of the synthetic identity weights
// used for characters that have no collation table weight at all. MySQL's
// my_uca_charcmp falls back to code-point identity for such characters
// (wc > maxchar or a weightless page), so the LIKE matcher must compare
// them by their code points instead of equating them through the shared
// 0xFFFD sentinel weight (issue #31175 review). The base sits above every
// real weight (uint16) and above LikeMalformedByteWeight, and identity
// weights never collide because they add the full code point.
const LikeUnweightedIdentityBase = 0x20000

// CharWeightFunc appends the comparison weights of the first character in
// value to dst and returns the extended slice and the character's length in
// bytes. Malformed bytes receive LikeMalformedByteWeight and advance one byte.
type CharWeightFunc func(dst []uint32, value []byte) ([]uint32, int)

// GeneralCICharWeight weighs one character with utf8mb4_general_ci. Every
// character produces exactly one weight, which is also its canonical fold
// rune.
func GeneralCICharWeight(dst []uint32, value []byte) ([]uint32, int) {
	if r, size := utf8.DecodeRune(value); r == utf8.RuneError && size == 1 {
		return append(dst, LikeMalformedByteWeight), 1
	}
	w, size := NextGeneralCIWeight(value)
	return append(dst, w), size
}

// UCA400CharWeight weighs one character with the pinned UCA 4.0.0 table.
// ID224 has no contractions, so per-character weights are context-free;
// expansions (for example U+00DF weighing as "ss") stay attached to their
// single character. A character with no table weight (every supplementary
// character, marked by the pinned table with the 0xFFFD sentinel) falls
// back to a synthetic per-code-point identity weight, exactly as MySQL's
// my_uca_charcmp compares code points when my_char_weight_addr has no
// entry; the genuine U+FFFD has a real table weight and never takes this
// path (issue #31175 review).
func UCA400CharWeight(dst []uint32, value []byte) ([]uint32, int) {
	r, size := utf8.DecodeRune(value)
	if r == utf8.RuneError && size == 1 {
		return append(dst, LikeMalformedByteWeight), 1
	}
	weights := uca400Weights(nil, value[:size])
	if len(weights) == 2 && weights[0] == 0xFF && weights[1] == 0xFD {
		return append(dst, LikeUnweightedIdentityBase+uint32(r)), size
	}
	for i := 0; i+1 < len(weights); i += 2 {
		dst = append(dst, uint32(weights[i])<<8|uint32(weights[i+1]))
	}
	return dst, size
}

// GeneralCIWeightRune exposes the utf8mb4_general_ci weight of one valid
// rune for matchers that compare weight streams as rune sequences.
func GeneralCIWeightRune(r rune) uint32 {
	return utf8mb4GeneralCIWeight(r)
}

// AppendGeneralCIFold appends the UTF-8 encoding of every character's
// utf8mb4_general_ci weight. The result is a one-to-one rune mapping of
// value, so byte-oriented LIKE paths compare exactly the collation's
// per-character weight stream (issue #31175 review).
func AppendGeneralCIFold(dst, value []byte) []byte {
	for len(value) > 0 {
		r, size := utf8.DecodeRune(value)
		if r == utf8.RuneError && size == 1 {
			dst = utf8.AppendRune(dst, LikeMalformedByteWeight)
			value = value[1:]
			continue
		}
		dst = utf8.AppendRune(dst, rune(utf8mb4GeneralCIWeight(r)))
		value = value[size:]
	}
	return dst
}

type LikeTokenKind uint8

const (
	// LikeTokenLiteral is a maximal run of pattern characters compared by
	// weight stream.
	LikeTokenLiteral LikeTokenKind = iota
	// LikeTokenAnyOne is the '_' wildcard: exactly one character.
	LikeTokenAnyOne
	// LikeTokenAnySeq is the '%' wildcard: any run of whole characters.
	LikeTokenAnySeq
)

// LikeToken is one SQL LIKE pattern atom. A literal token carries one
// comparison-weight stream per pattern character, so the matcher compares
// exactly one pattern character against one value character the way
// my_wildcmp_uca does.
type LikeToken struct {
	Kind  LikeTokenKind
	Chars [][]uint32
}

// TokenizeLikePattern splits a SQL LIKE pattern into wildcard and literal
// atoms under the escape rules, weighing literal characters with charWeight.
// A trailing escape character quotes itself, as in the existing matchers.
// Consecutive '%' wildcards merge into one token: "%%" matches exactly the
// same values as "%". Tokenization reports ctx cancellation.
func TokenizeLikePattern(ctx context.Context, pattern []byte, escape rune, escapeEnabled bool, charWeight CharWeightFunc) ([]LikeToken, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var tokens []LikeToken
	var literal [][]uint32
	flush := func() {
		if len(literal) > 0 {
			tokens = append(tokens, LikeToken{Kind: LikeTokenLiteral, Chars: literal})
			literal = nil
		}
	}
	escaped := false
	steps := 0
	for len(pattern) > 0 {
		if steps++; steps%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		r, size := utf8.DecodeRune(pattern)
		if escaped {
			weights, _ := charWeight(nil, pattern[:size])
			literal = append(literal, weights)
			escaped = false
			pattern = pattern[size:]
			continue
		}
		switch {
		case escapeEnabled && r == escape:
			escaped = true
			pattern = pattern[size:]
		case r == '_':
			flush()
			tokens = append(tokens, LikeToken{Kind: LikeTokenAnyOne})
			pattern = pattern[size:]
		case r == '%':
			flush()
			if n := len(tokens); n == 0 || tokens[n-1].Kind != LikeTokenAnySeq {
				tokens = append(tokens, LikeToken{Kind: LikeTokenAnySeq})
			}
			pattern = pattern[size:]
		default:
			weights, _ := charWeight(nil, pattern[:size])
			literal = append(literal, weights)
			pattern = pattern[size:]
		}
	}
	if escaped {
		var raw [utf8.UTFMax]byte
		n := utf8.EncodeRune(raw[:], escape)
		weights, _ := charWeight(nil, raw[:n])
		literal = append(literal, weights)
	}
	flush()
	return tokens, nil
}

// MatchLikePattern reports whether value matches the tokenized pattern:
// a literal pattern character consumes exactly one value character and the
// two compare by their collation weight streams, '_' consumes exactly one
// character, and '%' consumes any run of whole characters. One character
// with several weights (an expansion such as U+00DF) therefore matches only
// a single pattern character with the same weights and is never split or
// spanned by a wildcard.
//
// Patterns with at most one '%' have determined literal positions and match
// directly. Patterns with more '%' split into fixed-width segments between
// the wildcards, which a leftmost greedy search locates one after another:
// each segment search continues where the previous segment ended, so the
// subject is scanned once (KMP for a pure-literal segment, a bit-parallel
// Shift-And for a segment containing '_'). Expanding '%' into every subject
// position and retrying the literal runs from each was quadratic (issue
// #31175 review). Matching reports ctx cancellation.
func MatchLikePattern(ctx context.Context, tokens []LikeToken, value []byte, charWeight CharWeightFunc) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	var chars [][]uint32
	steps := 0
	for len(value) > 0 {
		if steps++; steps%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return false, err
			}
		}
		var weights []uint32
		var size int
		weights, size = charWeight(weights, value)
		chars = append(chars, weights)
		value = value[size:]
	}
	n := len(chars)

	// Patterns with at most one AnySeq have determined literal positions:
	// without AnySeq the whole pattern has one fixed width, and with one
	// AnySeq the tokens before it must match at the start and the tokens
	// after it at the end. Matching those shapes directly avoids expanding
	// the AnySeq into every subject position and retrying the full literal
	// run from each, which was quadratic (issue #31175 review).
	anySeq := 0
	anySeqIdx := -1
	for i, token := range tokens {
		if token.Kind == LikeTokenAnySeq {
			anySeq++
			anySeqIdx = i
		}
	}
	if anySeq == 0 {
		width := 0
		for _, token := range tokens {
			width += likeTokenWidth(token)
		}
		if width != n {
			return false, nil
		}
		return matchLikeFixedSpan(ctx, tokens, chars, 0)
	}
	if anySeq == 1 {
		prefix, suffix := tokens[:anySeqIdx], tokens[anySeqIdx+1:]
		prefixWidth, suffixWidth := 0, 0
		for _, token := range prefix {
			prefixWidth += likeTokenWidth(token)
		}
		for _, token := range suffix {
			suffixWidth += likeTokenWidth(token)
		}
		if prefixWidth+suffixWidth > n {
			return false, nil
		}
		if ok, err := matchLikeFixedSpan(ctx, prefix, chars, 0); err != nil || !ok {
			return ok, err
		}
		return matchLikeFixedSpan(ctx, suffix, chars, n-suffixWidth)
	}

	return matchLikeSegments(ctx, tokens, chars)
}

// likeSegment is the fixed-width token run between two '%' wildcards: a
// literal run and '_' placeholders whose total character width is known, so
// its occurrence position determines its end position.
type likeSegment struct {
	tokens []LikeToken
	width  int
}

// matchLikeSegments matches a pattern with two or more AnySeq tokens. The
// pattern is the segments separated by the wildcards, with optional leading
// and trailing '%'. A leftmost greedy search is complete: if any match
// exists with segment end positions q1 <= q2 <= ..., the greedy choice pi
// (the leftmost occurrence of segment i after p(i-1)) satisfies pi <= qi at
// every step, because a segment's match depends only on its own position
// and earlier placement never reduces the room left for later segments.
func matchLikeSegments(ctx context.Context, tokens []LikeToken, chars [][]uint32) (bool, error) {
	n := len(chars)
	leading := tokens[0].Kind == LikeTokenAnySeq
	trailing := tokens[len(tokens)-1].Kind == LikeTokenAnySeq

	var segments []likeSegment
	var cur []LikeToken
	width := 0
	flush := func() {
		if len(cur) > 0 {
			segments = append(segments, likeSegment{tokens: cur, width: width})
			cur = nil
			width = 0
		}
	}
	totalWidth := 0
	for _, token := range tokens {
		if token.Kind == LikeTokenAnySeq {
			flush()
			continue
		}
		cur = append(cur, token)
		width += likeTokenWidth(token)
	}
	flush()
	for _, seg := range segments {
		totalWidth += seg.width
	}
	if totalWidth > n {
		return false, nil
	}

	start := 0
	first := 0
	if !leading {
		if ok, err := matchLikeFixedSpan(ctx, segments[0].tokens, chars, 0); err != nil || !ok {
			return ok, err
		}
		start = segments[0].width
		first = 1
	}
	end := len(segments)
	limit := n
	if !trailing {
		// The last segment must end at n; the earlier ones must fit before
		// it. The greedy search leaves them the maximal room.
		limit = n - segments[len(segments)-1].width
		end--
	}
	for i := first; i < end; i++ {
		segEnd, ok, err := searchLikeSegment(ctx, segments[i], chars, start, limit)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		start = segEnd
	}
	if !trailing {
		return matchLikeFixedSpan(ctx, segments[len(segments)-1].tokens, chars, limit)
	}
	return true, nil
}

// searchLikeSegment finds the leftmost occurrence of seg among chars whose
// start is at least from and whose end is at most limit, returning the end
// position. A pure-literal segment is located by KMP over the weight
// streams; a segment mixing literals and '_' uses a bit-parallel Shift-And
// whose mask memory is budgeted, with a longest-literal anchor search plus
// verification as the over-budget fallback. A segment of only '_'
// wildcards matches any width run at once.
func searchLikeSegment(ctx context.Context, seg likeSegment, chars [][]uint32, from, limit int) (int, bool, error) {
	if from+seg.width > limit {
		return 0, false, nil
	}
	if len(seg.tokens) == 1 && seg.tokens[0].Kind == LikeTokenLiteral {
		return kmpLikeLiteral(ctx, seg.tokens[0].Chars, chars, from, limit)
	}
	literals := 0
	for _, token := range seg.tokens {
		if token.Kind == LikeTokenLiteral {
			literals++
		}
	}
	if literals == 0 {
		return from + seg.width, true, nil
	}
	if shiftAndLikeSegmentFits(seg) {
		return shiftAndLikeSegment(ctx, seg, chars, from, limit)
	}
	return anchorLikeSegmentSearch(ctx, seg, chars, from, limit)
}

// kmpLikeLiteral is Knuth-Morris-Pratt over per-character weight streams,
// returning the end of the leftmost occurrence of pat inside
// chars[from:limit].
func kmpLikeLiteral(ctx context.Context, pat [][]uint32, chars [][]uint32, from, limit int) (int, bool, error) {
	m := len(pat)
	fail := make([]int, m)
	steps := 0
	for i := 1; i < m; i++ {
		if steps++; steps%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, false, err
			}
		}
		j := fail[i-1]
		for j > 0 && !slices.Equal(pat[i], pat[j]) {
			j = fail[j-1]
		}
		if slices.Equal(pat[i], pat[j]) {
			j++
		}
		fail[i] = j
	}
	j := 0
	for i := from; i < limit; i++ {
		if steps++; steps%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, false, err
			}
		}
		for j > 0 && !slices.Equal(chars[i], pat[j]) {
			j = fail[j-1]
		}
		if slices.Equal(chars[i], pat[j]) {
			j++
		}
		if j == m {
			return i + 1, true, nil
		}
	}
	return 0, false, nil
}

// likeShiftAndMaxMaskWords bounds the total Shift-And mask memory per
// segment search (32768 uint64 words = 256KiB).
const likeShiftAndMaxMaskWords = 32768

// shiftAndLikeSegmentFits reports whether the segment's Shift-And masks
// stay inside the word budget: one mask word-slice per distinct literal
// weight vector plus the wildcard mask.
func shiftAndLikeSegmentFits(seg likeSegment) bool {
	words := (seg.width + 63) / 64
	distinct := make(map[string]struct{})
	for _, token := range seg.tokens {
		if token.Kind != LikeTokenLiteral {
			continue
		}
		for _, weights := range token.Chars {
			distinct[likeWeightsKey(weights)] = struct{}{}
		}
	}
	return (len(distinct)+1)*words <= likeShiftAndMaxMaskWords
}

// shiftAndLikeSegment runs the bit-parallel Shift-And search for a segment
// with '_' positions: bit j of the state is live when the segment's first
// j+1 positions match the subject ending at the current character. Each
// distinct literal weight vector maps to the mask of positions expecting
// it; '_' positions are set in every effective mask. The scan stops at the
// first complete match, which is the leftmost occurrence, and checks ctx
// every 1024 subject characters.
func shiftAndLikeSegment(ctx context.Context, seg likeSegment, chars [][]uint32, from, limit int) (int, bool, error) {
	w := seg.width
	words := (w + 63) / 64
	masks := make(map[string][]uint64)
	wild := make([]uint64, words)
	pos := 0
	for _, token := range seg.tokens {
		if token.Kind == LikeTokenAnyOne {
			wild[pos/64] |= 1 << uint(pos%64)
			pos++
			continue
		}
		for _, weights := range token.Chars {
			key := likeWeightsKey(weights)
			mask, ok := masks[key]
			if !ok {
				mask = make([]uint64, words)
				masks[key] = mask
			}
			mask[pos/64] |= 1 << uint(pos%64)
			pos++
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	topWord, topBit := (w-1)/64, uint((w-1)%64)
	d := make([]uint64, words)
	steps := 0
	for i := from; i < limit; i++ {
		if steps++; steps%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, false, err
			}
		}
		carry := uint64(1)
		for x := 0; x < words; x++ {
			next := d[x] >> 63
			d[x] = (d[x] << 1) | carry
			carry = next
		}
		if mask, ok := masks[likeWeightsKey(chars[i])]; ok {
			for x := 0; x < words; x++ {
				d[x] &= mask[x] | wild[x]
			}
		} else {
			for x := 0; x < words; x++ {
				d[x] &= wild[x]
			}
		}
		if i-from+1 >= w && d[topWord]&(uint64(1)<<topBit) != 0 {
			return i + 1, true, nil
		}
	}
	return 0, false, nil
}

// anchorLikeSegmentSearch is the over-budget fallback for segments whose
// Shift-And masks would exceed the word budget: it KMP-searches the
// segment's longest literal token and verifies each candidate position
// against the whole segment. The leftmost verified candidate is the
// leftmost occurrence.
func anchorLikeSegmentSearch(ctx context.Context, seg likeSegment, chars [][]uint32, from, limit int) (int, bool, error) {
	anchor := -1
	anchorOff, anchorLen := 0, 0
	off := 0
	for i, token := range seg.tokens {
		if token.Kind == LikeTokenLiteral && len(token.Chars) > anchorLen {
			anchor = i
			anchorOff = off
			anchorLen = len(token.Chars)
		}
		off += likeTokenWidth(token)
	}
	steps := 0
	for i := from + anchorOff; ; {
		if steps++; steps%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, false, err
			}
		}
		end, ok, err := kmpLikeLiteral(ctx, seg.tokens[anchor].Chars, chars, i, limit)
		if err != nil || !ok {
			return 0, false, err
		}
		start := end - anchorLen - anchorOff
		if start < from || start+seg.width > limit {
			return 0, false, nil
		}
		ok, err = matchLikeFixedSpan(ctx, seg.tokens, chars, start)
		if err != nil {
			return 0, false, err
		}
		if ok {
			return start + seg.width, true, nil
		}
		i = start + anchorOff + 1
	}
}

// likeWeightsKey encodes one character's weight stream as a map key.
func likeWeightsKey(weights []uint32) string {
	var b strings.Builder
	b.Grow(4 * len(weights))
	var buf [4]byte
	for _, w := range weights {
		binary.LittleEndian.PutUint32(buf[:], w)
		b.Write(buf[:])
	}
	return b.String()
}

// likeTokenWidth is the number of value characters one token consumes when
// its position is determined: a literal consumes its characters, '_' one
// character. AnySeq has no fixed width and never reaches this helper.
func likeTokenWidth(token LikeToken) int {
	if token.Kind == LikeTokenLiteral {
		return len(token.Chars)
	}
	return 1
}

// matchLikeFixedSpan compares tokens that contain no AnySeq against the
// value characters starting at chars[start]. The caller guarantees the
// total token width fits, so every AnyOne position is valid. Literal runs
// compare through likeCharsEqual, which bounds the work between ctx checks.
func matchLikeFixedSpan(ctx context.Context, tokens []LikeToken, chars [][]uint32, start int) (bool, error) {
	pos := start
	for _, token := range tokens {
		if token.Kind != LikeTokenLiteral {
			pos++
			continue
		}
		width := len(token.Chars)
		eq, err := likeCharsEqual(ctx, token.Chars, chars[pos:pos+width])
		if err != nil {
			return false, err
		}
		if !eq {
			return false, nil
		}
		pos += width
	}
	return true, nil
}

// likeCharsEqual compares one literal run character by character and checks
// ctx every 1024 characters, so a long literal comparison cannot run
// unbounded without observing cancellation (issue #31175 review).
func likeCharsEqual(ctx context.Context, pattern, value [][]uint32) (bool, error) {
	for i := range pattern {
		if i > 0 && i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return false, err
			}
		}
		if !slices.Equal(pattern[i], value[i]) {
			return false, nil
		}
	}
	return true, nil
}
