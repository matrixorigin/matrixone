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

package collation

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"
)

// A segment whose distinct literal weight vectors would blow the Shift-And
// mask budget must take the longest-literal anchor fallback and still
// return exact segment semantics (issue #31175 review).
func TestMatchLikeSegmentAnchorFallback(t *testing.T) {
	// 92 runes with pairwise distinct UCA 4.0.0 primary weights.
	alphabet := "abcdefghijklmnopqrstuvwxyz0123456789" +
		"αβγδεζηθικλμνξοπρστυφχψω" +
		"абвгдежзийклмнопрстуфхцчшщъыьэюя"
	if utf8.RuneCountInString(alphabet) != 92 {
		t.Fatalf("alphabet must hold 92 runes, got %d", utf8.RuneCountInString(alphabet))
	}
	literal := strings.Repeat(alphabet, 327) // 30,084 characters
	tokens, err := TokenizeLikePattern(context.Background(),
		[]byte("%_"+literal+"%"), '\\', true, UCA400CharWeight)
	if err != nil {
		t.Fatal(err)
	}
	// The pattern is one 30,085-wide segment: '_' + the literal run.
	if len(tokens) != 4 || tokens[1].Kind != LikeTokenAnyOne || tokens[2].Kind != LikeTokenLiteral {
		t.Fatalf("unexpected tokenization: %+v", tokens)
	}
	seg := likeSegment{tokens: tokens[1:3], width: 1 + len(tokens[2].Chars)}
	if shiftAndLikeSegmentFits(seg) {
		t.Fatal("this segment must exceed the Shift-And mask budget and take the anchor fallback")
	}

	// The '_' consumes the first subject character, the anchored literal
	// the rest: the whole pattern matches.
	subject := "q" + literal + "z"
	match, err := MatchLikePattern(context.Background(), tokens, []byte(subject), UCA400CharWeight)
	if err != nil || !match {
		t.Errorf("anchor-fallback segment must match its exact occurrence: %v %v", match, err)
	}

	// One replaced rune breaks every occurrence of the anchor literal, so
	// verification never sees a candidate and the pattern cannot match.
	runes := []rune(subject)
	runes[1+15000] = '!'
	match, err = MatchLikePattern(context.Background(), tokens, []byte(string(runes)), UCA400CharWeight)
	if err != nil || match {
		t.Errorf("a corrupted anchor literal must not match: %v %v", match, err)
	}
}
