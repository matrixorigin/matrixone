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

import (
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/cases"
)

// TestExpandRegexpCaseFoldRunsRows pins the exact rewrite for the shapes the
// expansion must produce, including the verbatim fast paths and fallbacks
// (issue #31175 review).
func TestExpandRegexpCaseFoldRunsRows(t *testing.T) {
	rows := []struct {
		name string
		pat  string
		ci   bool
		want string
	}{
		{"fold pair forward", "ss", true, `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"fold pair backward", "ß", true, `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"affixed run", "aß", true, `(?:ass|a(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"multi source image", "ﬅ", true, `(?:st|(?:st|(?-i:ﬅ)|(?-i:ﬆ)))`},
		{"verbatim without images", "hello", true, "hello"},
		{"case-normalized without images", "Hello", true, "hello"},
		{"case sensitive keeps bytes", "ß", false, "ß"},
		{"capital sharp s folds", "ẞ", true, `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"capital sharp s inside run", "xẞy", true, `(?:xssy|x(?:ss|(?-i:ß)|(?-i:ẞ))y)`},
		{"quantified fold source", "ß+", true, `(?-i:(?:ß|ẞ))+`},
		{"quantified fold source lazy", "ß+?", true, `(?-i:(?:ß|ẞ))+?`},
		{"quantified fold source interval", "ß{2}", true, `(?-i:(?:ß|ẞ)){2}`},
		{"quantified capital sharp s", "ẞ+", true, `(?-i:(?:ß|ẞ))+`},
		{"quantified ordinary char", "s{2}", true, "s{2}"},
		{"quantified orbit char", "ſ+", true, "ſ+"},
		{"escape splits runs", `s\nss`, true, `s\n` + `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"class untouched", "[ß]ss", true, "[ß]" + `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"alternation", "ss|ß", true, `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))` + "|" + `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"scoped off verbatim", "(?-i:ss)", true, "(?-i:ss)"},
		{"scoped on expands", "(?i:ss)", false, "(?i:" + `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))` + ")"},
		{"anchor boundaries", "^ß$", true, "^" + `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))` + "$"},
		{"dot terminates run", "ß.", true, `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))` + "."},
		// Round-6 F1: named backreference tokens keep their exact bytes so
		// the reference name keeps its case.
		{"named reference preserved", `(?<X>a)\k<X>`, true, `(?<X>a)\k<X>`},
		{"named reference mixed case", `(?<Ss>a)\k<Ss>`, true, `(?<Ss>a)\k<Ss>`},
		{"named reference P form", `(?P<N>x)\k<N>`, true, `(?P<N>x)\k<N>`},
		{"named reference quote form", `(?'N'x)\k'N'`, true, `(?'N'x)\k'N'`},
		{"deprecated angle reference", `(?<N>x)\<N>`, true, `(?<N>x)\<N>`},
		{"deprecated quote reference", `(?<N>x)\'N'`, true, `(?<N>x)\'N'`},
		{"unterminated reference verbatim", `a\k<X`, true, `a\k<X`},
		{"reference after fold run", `ß\k<N>`, true, `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))` + `\k<N>`},
		// Round-6 F2: a quantified fold-table atom emits its whole
		// single-character orbit, not only its own character.
		{"quantified greek pair", "ᾈ+", true, `(?-i:(?:ᾀ|ᾈ))+`},
		{"quantified greek pair interval", "ᾀ{2}", true, `(?-i:(?:ᾀ|ᾈ)){2}`},
		{"quantified greek pair sensitive", "ᾈ+", false, `ᾈ+`},
		// Round-6 F4: \Q...\E, \x{..} and \xNN literal text decodes into the
		// literal stream; undecodable escapes stay verbatim. Round 9 decodes
		// whitespace and '#' text as well (re-escaped under free-spacing
		// mode), so quoted text folds like any other literal.
		{"quoted literal decodes", `\Qss\E`, true, `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"quoted literal unterminated", `\Qss`, true, `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"quoted metacharacter re-escapes", `\Q(\E`, true, `\(`},
		{"quoted whitespace decodes", `\Qa b\E`, true, `a b`},
		{"hex literal decodes", `\x{DF}`, true, `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"hex two digit decodes", `s\x{73}`, true, `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"hex pair joins run", `\xDF\xDF`, true, `(?:ssss|ss(?:ss|(?-i:ß)|(?-i:ẞ))|s(?:ss|(?-i:ß)|(?-i:ẞ))s|(?:ss|(?-i:ß)|(?-i:ẞ))ss|(?:ss|(?-i:ß)|(?-i:ẞ))(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"hex case sensitive decodes", `\x{DF}`, false, `ß`},
		{"hex invalid stays verbatim", `\x{ZZ}`, true, `\x{ZZ}`},
		{"hex surrogate stays verbatim", `\x{D800}`, true, `\x{D800}`},
		{"digit backreference verbatim", `(ss)\1`, true, `((?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ))))\1`},
		// Round-9 F1: free-spacing trivia (unescaped whitespace, # line
		// comments) and (?#...) comments are dropped so the literal pieces
		// around them fold as one run, exactly as the engines fold the
		// comment-stripped run; the fold expansion itself must never
		// contain a '#', which would swallow the generated parentheses.
		{"free-spacing comment after run", "(?x)ss#comment", true, "(?x)" + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"free-spacing joins across comment", "(?x)ss # hi\nss", true, "(?x)" + `(?:[sſ][sſ][sſ][sſ]|[sſ][sſ](?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ))|[sſ](?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ))[sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ))[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ))(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"free-spacing joins run", "(?x)s s", true, "(?x)" + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"free-spacing tab dropped", "(?x)a\tb", true, "(?x)ab"},
		{"free-spacing scoped off", "(?x)a(?-x:b c)d", true, "(?x)a(?-x:b c)d"},
		{"free-spacing header scoped off", "(?x)a(?-x)b c", true, "(?x)a(?-x)b c"},
		{"free-spacing quantifier binds", "(?x)ß {2}", true, "(?x)" + `(?-i:(?:ß|ẞ)){2}`},
		{"free-spacing fold pair joins", "(?x)ß ß", true, "(?x)" + `(?:[sſ][sſ][sſ][sſ]|[sſ][sſ](?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ))|[sſ](?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ))[sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ))[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ))(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"free-spacing case header", "(?xi)ss # c\nx", false, "(?xi)" + `(?:[sſ][sſ]x|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ))x)`},
		{"hash literal without free-spacing", "ss # not-a-comment", true, `(?:ss # not-a-comment|(?:ss|(?-i:ß)|(?-i:ẞ)) # not-a-comment)`},
		{"comment joins run insensitive", "a(?#c)b", true, "ab"},
		{"comment joins fold run", "s(?#ß)s", true, `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"unterminated comment verbatim", "a(?#cb", true, "a(?#cb"},
		{"comment joins run sensitive", "a(?#c)b", false, "ab"},
		{"free-spacing comment and space", "(?x)a(?#c) b", true, "(?x)ab"},
		// Round-9 F2: the \g<name>, \g'name' and \g{name} reference forms
		// keep their exact bytes exactly like \k, so a mixed-case reference
		// name still resolves.
		{"named reference g angle", `(?<X>a)\g<X>`, true, `(?<X>a)\g<X>`},
		{"named reference g quote", `(?<X>a)\g'X'`, true, `(?<X>a)\g'X'`},
		{"named reference g brace", `(?<X>a)\g{X}`, true, `(?<X>a)\g{X}`},
		{"named reference g mixed case", `(?<Ss>a)\g<Ss>`, true, `(?<Ss>a)\g<Ss>`},
		{"unterminated g reference verbatim", `a\g<X`, true, `a\g<X`},
		{"g reference after fold run", `ß\g<N>`, true, `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))` + `\g<N>`},
		// Round-9 F3: quoted or escaped whitespace and '#' decode into the
		// literal stream and fold with the surrounding run; under
		// free-spacing mode they are re-emitted as \x{..} escapes so they
		// stay literal.
		{"quoted hash decodes", `\Qss#\E`, true, `(?:ss#|(?:ss|(?-i:ß)|(?-i:ẞ))#)`},
		{"quoted whitespace folds around", `\Qss ss\E`, true, `(?:ss ss|ss (?:ss|(?-i:ß)|(?-i:ẞ))|(?:ss|(?-i:ß)|(?-i:ẞ)) ss|(?:ss|(?-i:ß)|(?-i:ẞ)) (?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"free-spacing quoted whitespace escapes", `(?x)\Qss ss\E`, true, `(?x)(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))\x{20}(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"free-spacing quoted hash escapes", `(?x)\Qss#\E`, true, `(?x)(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))\x{23}`},
		{"hex space decodes", `\x{20}`, true, ` `},
		{"free-spacing hex space escapes", `(?x)a\x{20}b`, true, `(?x)a\x{20}b`},
		{"free-spacing hex tab escapes", `(?x)a\x09b`, true, `(?x)a\x{09}b`},
		{"free-spacing hex hash escapes", `(?x)a\x{23}b`, true, `(?x)a\x{23}b`},
		{"quoted newline folds around", "\\Qss\nss\\E", true, "(?:ss\nss|ss\n" + `(?:ss|(?-i:ß)|(?-i:ẞ))` + "|" + `(?:ss|(?-i:ß)|(?-i:ẞ))` + "\nss|" + `(?:ss|(?-i:ß)|(?-i:ẞ))` + "\n" + `(?:ss|(?-i:ß)|(?-i:ẞ))` + ")"},
		{"free-spacing escaped space verbatim", `(?x)a\ b`, true, `(?x)a\ b`},
		// Round-10 F1: escapes whose payload could be mistaken for foldable
		// literal text stay complete verbatim tokens — \c control
		// designators, octal escapes and multi-digit backreferences — while
		// the text after them still folds. The token boundaries mirror the
		// regexp2 parser's own escape grammar. \uXXXX/\UXXXXXXXX atoms the
		// regexp2 engine decodes were superseded in round 18: they decode
		// into one literal atom with the same per-character fold orbit as
		// the literal spelling (U+FF41's orbit mates are all covered by the
		// engine's caseless closure, so the completed atom is just the
		// decoded character — behavior-identical to the engine decoding
		// the verbatim escape itself). The full pin set is
		// TestExpandRegexpCaseFoldRunsUnicodeEscapeAtom; a bare \u keeps
		// the RE2 route, which has no \u escape, so it stays verbatim.
		{"unicode escape U decodes to atom", `\U0000FF41`, true, `ａ`},
		{"unicode escape U run folds after", `\U0000FF41ss`, true, `ａ` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"unicode escape u run folds after (RE2 route)", `\u00FFss`, true, `\u00FF` + `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"control escape designator verbatim", `\cSs`, true, `\cS[sſ]`},
		{"control escape run folds after", `\cSss`, true, `\cS` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"octal escape run folds after", `\012ss`, true, `\012` + `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"multi-digit reference complete", `\11ss`, true, `\11` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"digit reference then run", `(ss)\1ss`, true, `((?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ))))\1` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		// Round-10 F2: the inline-option grammar tracks the engine's letters
		// and transitions — uppercase 'I'/'X' (optionFromCode) and '+'
		// re-enabling (scanOptions) — so a header the engine honors updates
		// the rewrite modes instead of leaving them stale.
		{"uppercase I scoped off", `(?-I)A`, true, `(?-I)A`},
		{"uppercase I scoped off run", `(?-I)ss`, true, `(?-I)ss`},
		{"uppercase I enables fold", `(?I)ss`, false, `(?I)` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"plus reenables after dash", `(?-i+I)ss`, true, `(?-i+I)` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"plus enables fold", `(?+i)ss`, false, `(?+i)` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"uppercase X drops comment", `(?X)ss#comment`, true, `(?X)` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"uppercase X joins run", `(?X)s s`, true, `(?X)` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"uppercase X scoped off", `(?X)a(?-X:b c)d`, true, `(?X)a(?-X:b c)d`},
		{"mixed case header transitions", `(?Xi-mS)ss`, true, `(?Xi-mS)` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		// Round-10 F3: an empty quoted segment carries no text, so it is
		// transparent to folding — the literal pieces around it join one run
		// and a following quantifier binds across it, exactly as the engines
		// treat the empty segment.
		{"quoted empty joins run", `^s\Q\Es$`, true, `^` + `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))` + `$`},
		{"quoted empty mid run", `a\Q\Eb`, true, `ab`},
		{"quoted empty unterminated", `a\Q`, true, `a`},
		{"quoted empty quantifier binds", `s\Q\E+`, true, `s+`},
		{"quoted empty interval binds", `ss\Q\E{2}`, true, `ss{2}`},
		{"quoted empty case sensitive", `^s\Q\Es$`, false, `^ss$`},
		{"quoted empty alone unchanged", `\Q\E`, true, `\Q\E`},
		// Round-11 F1: a nonzero digit starts a decimal escape whose
		// complete digit run is one engine token (the regexp2 parser's
		// scanDecimal consumes it before choosing backreference vs
		// octal/literal fallback), so \18 names group 18 — not group 1
		// with a literal '8'. Digits never fold; the run after the token
		// still does.
		{"decimal reference 18 complete", `(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)\18ss`, true, `(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)\18` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"decimal reference 19 complete", `(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)(s)\19ss`, true, `(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)([sſ])\19` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"decimal reference octal fallback complete", `(a)(b)\18ss`, true, `(a)(b)\18` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"decimal reference case sensitive", `(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)\18ss`, false, `(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)\18ss`},
		// Round-11 F2: the short property form \pX / \PX is one escape
		// token on both engines (Go regexp's \pN and the vendored parser's
		// parseProperty), so its letter never joins the fold run.
		{"short property complete", `\pLss`, true, `\pL` + `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"short property negated complete", `\PLss`, true, `\PL` + `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		{"braced property unchanged", `\p{L}ss`, true, `\p{L}` + `(?:ss|(?:ss|(?-i:ß)|(?-i:ẞ)))`},
		// Round-11 F3: on the regexp2 route a '-[' at a non-first position
		// nests a subtraction class (scanCharSet recurses, then requires
		// the outer ']'), so the class continues past the nested ']'. Go
		// regexp has no nesting and closes at the first ']'; the RE2 pin
		// keeps that reading byte-identical.
		{"subtraction class complete", `[a-z-[bc]]ss(?#x)`, true, `[a-z-[bc]]` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"subtraction recursion complete", `[a-z-[b-c-[d]]]ss(?#x)`, true, `[a-z-[b-c-[d]]]` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"subtraction after literal bracket", `[]-[a]]ss(?#x)`, true, `[]-[a]]` + `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`},
		{"subtraction class case sensitive", `[a-z-[bc]]ss(?#x)`, false, `[a-z-[bc]]ss`},
		{"RE2 class closes at first bracket", `[a-z-[bc]]ss`, true, `[a-z-[bc]` + `(?:\]ss|\](?:ss|(?-i:ß)|(?-i:ẞ)))`},
		// Round-12 F1: (?> is a complete atomic-group opener on the regexp2
		// route. The '>' must not join a fold run: copying only "(?" left
		// ">ss" to fold, and the vendored parser read the result as a
		// conditional whose empty branches zero-matched every subject.
		{"atomic group complete", `(?>ss)`, true, `(?>(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ))))`},
		{"atomic group affixed", `^(?>ss)x$`, true, `^(?>(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ))))x$`},
		{"atomic group nested", `(?>(?>ss))`, true, `(?>(?>(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))))`},
		{"atomic group quantified", `(?>ss)+`, true, `(?>(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ))))+`},
		{"atomic group case sensitive", `(?>ss)`, false, `(?>ss)`},
		{"atomic group no fold run", `(?>a+)ab`, true, `(?>a+)ab`},
		// Round-12 F2: on the regexp2 route the caseless orbit is Go's
		// unicode.ToLower closure, which misses full-fold sources such as ς
		// (ToLower(ς)=ς, not σ). The fold image rune lists those sources as
		// extra class members (regexpSingleCharFoldExtras), so even the
		// identical-text self-match survives the rewrite. The RE2 route's
		// simple-fold orbit is complete, so those pins stay byte-identical.
		{"orbit final sigma self", `^ς$(?#x)`, true, `^[σς]$`},
		{"orbit final sigma run", `ςς(?#x)`, true, `[σς][σς]`},
		{"orbit long s self", `^ſ$(?#x)`, true, `^[sſ]$`},
		{"orbit micro sign self", `^µ$(?#x)`, true, `^[μµ]$`},
		{"orbit cyrillic self", `^ᲀ$(?#x)`, true, `^[вᲀ]$`},
		{"orbit image block iota", `ᾀ(?#x)`, true, `(?:ἀ[ιͅι]|(?:ἀ[ιͅι]|(?-i:ᾀ)|(?-i:ᾈ)))`},
		{"orbit run RE2 unchanged", `ςς`, true, `σσ`},
		{"orbit case sensitive", `^ς$(?#x)`, false, `^ς$`},
		// Round-16 F2: the regexp2 orbit completion now covers explicit
		// character classes and quantified atoms as well. A class gains
		// the fold-orbit members the engine's ToLower closure cannot
		// reach ([ς] gains σ, [a-z] gains ſ), a negated class closes the
		// same way, and a quantified atom with a single-rune fold image
		// emits the orbit class. A class containing a subtraction set
		// stays entirely verbatim: the engine's caseless subtraction
		// algebra is its own per-character ToLower fiber (round-8
		// contract), and completing the orbit on either side changes it
		// — completing the exclusion set would erase raw minuend members
		// ([s-[ſ]] would collapse to empty), completing the minuend can
		// revive a class the fiber keeps dead ([s-[S]] would gain ſ).
		// Members the closure already covers (ẞ against [ß], K against
		// [k]) keep the exact bytes, as does a multi-character fold
		// source in a class (ICU's class string closure stays a
		// documented divergence).
		{"class orbit final sigma", `^[ς]$(?#x)`, true, `^[ςσ]$`},
		{"class orbit sigma", `^[σ]$(?#x)`, true, `^[σς]$`},
		{"class orbit negated", `^[^ς]$(?#x)`, true, `^[^ςσ]$`},
		{"class orbit range long s", `^[a-z]$(?#x)`, true, `^[a-zſ]$`},
		{"class orbit range upper long s", `^[A-Z]$(?#x)`, true, `^[A-Zsſ]$`},
		{"class orbit greek range", `^[Α-Ω]$(?#x)`, true, `^[Α-Ωµͅβεθικμπρςσφϐϑϕϖϰϱϵι]$`},
		{"class orbit hex escape", `^[\x{3C2}]$(?#x)`, true, `^[\x{3C2}σ]$`},
		// Round-17 F2: \uXXXX and \UXXXXXXXX class members decode into the
		// fold-orbit computation exactly like literal and \x-decoded
		// members (ICU 78.3 probed, probe_icu17.log rows U1-U18), so the
		// case equivalence of a class no longer depends on the escape
		// spelling. This supersedes the round-16 pin that kept [\u03C2]
		// byte-identical — the old value encoded exactly the deficiency
		// the round-17 repair removes. Malformed escapes (\u with fewer
		// than four hex digits, \U without eight) stay opaque so the
		// engine's malformed-escape error or literal fallback is
		// preserved verbatim.
		{"class orbit unicode escape decoded", `^[\u03C2]$(?#x)`, true, `^[\u03C2σ]$`},
		{"class orbit unicode escape negated", `^[^\u03C2]$(?#x)`, true, `^[^\u03C2σ]$`},
		{"class orbit unicode escape s", `^[\u0073]$(?#x)`, true, `^[\u0073ſ]$`},
		{"class orbit unicode escape long s", `^[\u017F]$(?#x)`, true, `^[\u017Fs]$`},
		{"class orbit unicode escape range", `^[\u0041-\u005A]$(?#x)`, true, `^[\u0041-\u005Asſ]$`},
		{"class orbit unicode escape mixed range", `^[\u0041-Z]$(?#x)`, true, `^[\u0041-Zsſ]$`},
		{"class orbit capital-U escape decoded", `^[\U000003C2]$(?#x)`, true, `^[\U000003C2σ]$`},
		{"class orbit unicode escape sharp s covered", `^[\u00DF]$(?#x)`, true, `^[\u00DF]$`},
		{"class orbit unicode escape case sensitive", `^[\u03C2]$(?#x)`, false, `^[\u03C2]$`},
		{"class malformed unicode escape opaque", `^[\u12]$(?#x)`, true, `^[\u12]$`},
		{"class malformed unicode escape hex opaque", `^[\uZZZZ]$(?#x)`, true, `^[\uZZZZ]$`},
		{"class malformed capital-U fallback opaque", `^[\U0041]$(?#x)`, true, `^[\U0041]$`},
		// A genuine closer after an escaped bracket still augments.
		{"genuine closer escaped bracket still augments", `(?=)[s\\]`, true, `(?=)[s\\ſ]`},
		{"class orbit sharp s covered", `^[ß]$(?#x)`, true, `^[ß]$`},
		{"class orbit capital sharp s covered", `^[ẞ]$(?#x)`, true, `^[ẞ]$`},
		{"class orbit kelvin covered", `^[k]$(?#x)`, true, `^[k]$`},
		{"class orbit subtraction set", `^[α-ω-[σ]]$(?#x)`, true, `^[α-ω-[σ]]$`},
		{"class orbit subtraction long s", `^[a-z-[bc]]$(?#x)`, true, `^[a-z-[bc]]$`},
		{"class multi-char source verbatim", `^[ﬀ]$(?#x)`, true, `^[ﬀ]$`},
		{"class orbit case sensitive", `^[ς]$(?#x)`, false, `^[ς]$`},
		{"quantified atom orbit sigma", `^ς+$(?#x)`, true, `^[σς]+$`},
		{"quantified atom orbit count", `^ς{2}$(?#x)`, true, `^[σς]{2}$`},
		{"quantified atom orbit long s", `^s+$(?#x)`, true, `^[sſ]+$`},
		{"quantified atom orbit long s source", `^ſ+$(?#x)`, true, `^[sſ]+$`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			got, err := expandRegexpCaseFoldRuns(row.pat, row.ci)
			require.NoError(t, err)
			require.Equal(t, row.want, got)
		})
	}
	// Invalid UTF-8 stays opaque: the rewrite declines it unchanged so the
	// engine reports its own verdict for the original bytes.
	invalid := "ss\xffss"
	gotInvalid, err := expandRegexpCaseFoldRuns(invalid, true)
	require.NoError(t, err)
	require.Equal(t, invalid, gotInvalid)
	// Round-6 F3: runs past the flat enumeration cap expand exactly (divide
	// and conquer) instead of falling back to the verbatim bytes; the
	// expanded pattern keeps the boundary-exact full-fold semantics.
	longRun := strings.Repeat("s", 80)
	expanded, err := expandRegexpCaseFoldRuns(longRun, true)
	require.NoError(t, err)
	require.NotEqual(t, longRun, expanded)
	require.LessOrEqual(t, len(expanded), regexpCaseFoldMaxRunBytes)
	reg, err := regexp.Compile("(?i)^" + expanded + "$")
	require.NoError(t, err)
	require.True(t, reg.MatchString(strings.Repeat("ß", 40)))
	require.False(t, reg.MatchString(strings.Repeat("ß", 39)+"s"))
	require.False(t, reg.MatchString(strings.Repeat("ß", 40)+"s"))
	// A run whose exact expansion would exceed the per-run byte bound fails
	// the rewrite with the bounded resource-limit error: emitting the
	// verbatim bytes would silently narrow the promised full-fold match set
	// (issue #31175 design, invariant 5).
	hugeRun := strings.Repeat("s", 20000)
	_, err = expandRegexpCaseFoldRuns(hugeRun, true)
	require.Error(t, err)
	require.Contains(t, err.Error(),
		"regexp case-fold resource limit exceeded: literal-run emission budget of 131072 bytes")
	// Round-12 F2: the orbit class emission ([sſ], four bytes per position)
	// keeps the regexp2 route's factored long-run expansion exact and within
	// the per-run byte bound, just like the RE2 route above.
	longRun2 := strings.Repeat("s", 80) + "(?#x)"
	expanded2, err := expandRegexpCaseFoldRuns(longRun2, true)
	require.NoError(t, err)
	require.NotEqual(t, longRun2, expanded2)
	require.LessOrEqual(t, len(expanded2), regexpCaseFoldMaxRunBytes)
	t.Logf("regexp2-route 80-s expansion bytes: %d", len(expanded2))
}

// TestExpandRegexpCaseFoldRunsUnterminatedClass pins the round-17 F1
// repair: an unterminated class keeps its exact bytes so both engines
// report the same unterminated-class error as for the unrewritten
// pattern. Augmenting it would parse a body whose final escape is
// truncated (the escaped ']' is a member, not the closing delimiter) —
// on the pre-repair revision every ci row below panicked the expansion
// with an index-out-of-range read, so these rows live in their own
// function to keep that red signal from masking other failures. (?=)
// forces the regexp2 route; the RE2-route row scans without nesting.
func TestExpandRegexpCaseFoldRunsUnterminatedClass(t *testing.T) {
	rows := []struct {
		name string
		pat  string
		ci   bool
		want string
	}{
		{"escaped bracket", `(?=)[s\]`, true, `(?=)[s\]`},
		{"negated escaped bracket", `(?=)[^s\]`, true, `(?=)[^s\]`},
		{"range escaped bracket", `(?=)[a-z\]`, true, `(?=)[a-z\]`},
		{"trailing backslash", `(?=)[s\`, true, `(?=)[s\`},
		{"open class", `(?=)[s`, true, `(?=)[s`},
		{"unterminated nested subtraction", `(?=)[a-z-[bc\]`, true, `(?=)[a-z-[bc\]`},
		{"RE2 route", `[s\]`, true, `[s\]`},
		{"case sensitive", `(?=)[s\]`, false, `(?=)[s\]`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			got, err := expandRegexpCaseFoldRuns(row.pat, row.ci)
			require.NoError(t, err)
			require.Equal(t, row.want, got)
		})
	}
}

// TestExpandRegexpCaseFoldRunsUnicodeEscapeAtom pins the round-18 F1
// repair: a \uXXXX or \UXXXXXXXX escape the regexp2 engine decodes into
// one literal atom receives the same per-character fold orbit as the
// literal spelling, because the engine's caseless ToLower fiber misses
// orbit members (ſ for \u0073, σ for \u03C2). The completed atom stays a
// single token — it never joins the surrounding literal run, so the
// multi-character fold boundary (ss against \u00DF) is preserved — and a
// following quantifier binds it. Malformed escapes, the case-sensitive
// route and the RE2 route keep the exact bytes so the engine's own
// verdict stands. (?#x) forces the regexp2 route and is dropped as trivia;
// the \U spelling and (?x) route there on their own.
func TestExpandRegexpCaseFoldRunsUnicodeEscapeAtom(t *testing.T) {
	rows := []struct {
		name string
		pat  string
		ci   bool
		want string
	}{
		{"sigma final", `^\u03C2$(?#x)`, true, `^[σς]$`},
		{"sigma small", `^\u03C3$(?#x)`, true, `^[σς]$`},
		{"sigma capital", `^\u03A3$(?#x)`, true, `^[σς]$`},
		{"long s", `^\u0073$(?#x)`, true, `^[sſ]$`},
		{"long s source", `^\u017F$(?#x)`, true, `^[sſ]$`},
		{"capital s", `^\u0053$(?#x)`, true, `^[sſ]$`},
		{"capital-U spelling", `^\U000003C2$`, true, `^[σς]$`},
		{"sharp s per character", `^\u00DF$(?#x)`, true, `^(?-i:(?:ß|ẞ))$`},
		{"capital sharp s per character", `^\u1E9E$(?#x)`, true, `^(?-i:(?:ß|ẞ))$`},
		{"no orbit", `^\u0041$(?#x)`, true, `^A$`},
		{"decoded metachar stays literal", `^\u002E$(?#x)`, true, `^\.$`},
		{"quantified", `^\u03C2+$(?#x)`, true, `^[σς]+$`},
		{"interval", `^a\u03C2{2}$(?#x)`, true, `^a[σς]{2}$`},
		{"adjacent atoms", `^\u03C2\u03C3$(?#x)`, true, `^[σς][σς]$`},
		{"literal run around", `^x\u03C2y$(?#x)`, true, `^x[σς]y$`},
		{"malformed short hex", `^\u12$(?#x)`, true, `^\u12$`},
		// A non-hex payload is not decoded either: the escape token
		// scanner consumes only \ and the doomed payload letters re-scan
		// as a foldable run, so the rewrite emits the \u token verbatim
		// followed by the run's fold image; the engine reports its
		// bad-escape error for the rewrite exactly as for the source.
		{"malformed non-hex", `^\uZZZZ$(?#x)`, true, `^\uzzzz$`},
		{"malformed surrogate", `^\uD800$(?#x)`, true, `^\uD800$`},
		{"case sensitive", `^\u03C2$(?#x)`, false, `^\u03C2$`},
		{"RE2 route verbatim", `^\u03C2$`, true, `^\u03C2$`},
		{"free-spacing decoded space", `(?x)^\u0020$`, true, `(?x)^\x{20}$`},
		{"free-spacing decoded hash", `(?x)^\u0023$`, true, `(?x)^\x{23}$`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			got, err := expandRegexpCaseFoldRuns(row.pat, row.ci)
			require.NoError(t, err)
			require.Equal(t, row.want, got)
		})
	}
}

// TestRegexpCaseFoldTableConsistency pins the generated fold table against
// the vendored x/text full case folding: every multi-rune folding, U+1E9E
// included, must be a table entry, and nothing else may be one. U+1E9E's
// presence is the ICU 78.3 caseless-matching behavior probed for issue
// #31175 (it matches U+00DF and "ss").
func TestRegexpCaseFoldTableConsistency(t *testing.T) {
	folder := cases.Fold()
	for r := rune(0); r <= utf8.MaxRune; r++ {
		if !utf8.ValidRune(r) {
			continue
		}
		folded := folder.String(string(r))
		_, inTable := regexpMultiCharFold[r]
		if utf8.RuneCountInString(folded) > 1 {
			require.True(t, inTable, "missing multi-rune folding U+%04X -> %q", r, folded)
			require.Equal(t, folded, regexpMultiCharFold[r], "fold image U+%04X", r)
		} else {
			require.False(t, inTable, "unexpected table entry U+%04X", r)
		}
	}
	// The reverse index lists every source of each image.
	for src, image := range regexpMultiCharFold {
		require.Contains(t, regexpMultiCharFoldSources[image], src)
	}
}

// TestRegexpPatternEnablesCaseFold pins the inline-flag detector used by the
// expansion gate.
func TestRegexpPatternEnablesCaseFold(t *testing.T) {
	for _, tc := range []struct {
		pat  string
		want bool
	}{
		{"ss", false},
		{"(?i)ss", true},
		{"(?i:ss)", true},
		{"(?ms)x", false},
		{"(?msi:x)", true},
		{"(?-i:ss)", false},
		{"(?P<name>i)", false},
		{"(?<name>i)", false},
		{"(?", false},
	} {
		require.Equal(t, tc.want, regexpPatternEnablesCaseFold(tc.pat), "pattern %q", tc.pat)
	}
}

// TestExpandRegexpCaseFoldRunsTotalBudget pins the whole-pattern emission
// budget (round-13 pre-PR review, revised for the resource-limit error
// contract): the 1MiB bound is enforced while the pattern is emitted, so a
// pattern of many medium runs is abandoned as soon as the budget is
// exhausted instead of materializing the complete oversized expansion
// before the deferred length check discards it (10,000 80-s runs built
// 689,589,999 bytes, about 658MiB). Exhaustion fails the rewrite with the
// bounded resource-limit error; the sibling alternatives carry a distinct
// numeric suffix because byte-identical eligible duplicates are removed
// before expansion (design invariant 5), which would collapse the farm.
func TestExpandRegexpCaseFoldRunsTotalBudget(t *testing.T) {
	run80 := strings.Repeat("s", 80)
	runs := func(k int) string {
		parts := make([]string, k)
		for i := range parts {
			parts[i] = run80 + strconv.Itoa(i)
		}
		return strings.Join(parts, "|") + "(?#x)"
	}
	// Boundary-exact acceptance, derived from the measured per-run emissions
	// so the pin stays exact if the emission shape changes: the farm expands
	// to the sum of the run emissions plus separator bytes (the trailing
	// comment is trivia and emits nothing).
	emission := func(k int) int {
		total := k - 1
		for i := 0; i < k; i++ {
			out, err := expandRegexpCaseFoldRuns(run80+strconv.Itoa(i)+"(?#x)", true)
			require.NoError(t, err)
			total += len(out)
		}
		return total
	}
	kAcc := 1
	for emission(kAcc+1) <= regexpCaseFoldMaxExpandedBytes {
		kAcc++
	}
	require.Greater(t, kAcc, 1)
	require.Greater(t, emission(kAcc+1), regexpCaseFoldMaxExpandedBytes)
	accept := runs(kAcc)
	expanded, err := expandRegexpCaseFoldRuns(accept, true)
	require.NoError(t, err)
	require.NotEqual(t, accept, expanded)
	require.LessOrEqual(t, len(expanded), regexpCaseFoldMaxExpandedBytes)
	// One more run crosses the bound: the rewrite fails with the bounded
	// resource-limit error naming the whole-pattern emission budget.
	reject := runs(kAcc + 1)
	_, err = expandRegexpCaseFoldRuns(reject, true)
	require.Error(t, err)
	require.Contains(t, err.Error(),
		"regexp case-fold resource limit exceeded: pattern emission budget of 1048576 bytes")
	// The review counterexample: 10,000 medium distinct runs (848,894 raw
	// bytes, admitted by the 1MiB raw-pattern gate) must fail with bounded
	// emission work instead of building about 658MiB first. The byte
	// measurement bounds the work performed, not only the result.
	big := runs(10000)
	require.Equal(t, 848894, len(big))
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err = expandRegexpCaseFoldRuns(big, true)
	runtime.ReadMemStats(&after)
	require.Error(t, err)
	require.Contains(t, err.Error(),
		"regexp case-fold resource limit exceeded: pattern emission budget of 1048576 bytes")
	t.Logf("10,000-run overflow expansion allocated %d bytes", after.TotalAlloc-before.TotalAlloc)
	require.LessOrEqual(t, after.TotalAlloc-before.TotalAlloc, uint64(32<<20))
}

// TestExpandRegexpCaseFoldRunsWorkBudget pins the emission-construction
// work budget (round-14 pre-PR review, revised for the resource-limit error
// contract). Every byte built while computing a run's expansion is charged
// against the shared 16MiB budget, including discarded attempts. Under the
// fail-fast contract a run whose exact expansion exceeds the per-run
// emission bound aborts the whole rewrite after that run's bounded charge:
// the review counterexample (10,000 100-s runs whose discarded per-run
// attempts allocated about 25GB of intermediates before the unbudgeted code
// returned) can no longer multiply per-run costs, because the first
// over-bound run fails the rewrite. The sibling alternatives carry a
// distinct numeric suffix because byte-identical eligible duplicates are
// removed before expansion (design invariant 5), which would collapse the
// farm.
func TestExpandRegexpCaseFoldRunsWorkBudget(t *testing.T) {
	run100 := strings.Repeat("s", 100)
	farm := func(k int) string {
		parts := make([]string, k)
		for i := range parts {
			parts[i] = run100 + strconv.Itoa(i)
		}
		return strings.Join(parts, "|") + "(?#x)"
	}
	// Even one 100-s run fails: its exact expansion (about 1.5MB) exceeds
	// the 128KiB per-run emission bound, so the rewrite raises the bounded
	// resource-limit error instead of emitting the verbatim run bytes.
	_, err := expandRegexpCaseFoldRuns(farm(1), true)
	require.Error(t, err)
	require.Contains(t, err.Error(),
		"regexp case-fold resource limit exceeded: literal-run emission budget of 131072 bytes")
	// The review counterexample: 9,000 distinct 100-s runs (943,894 raw
	// bytes, admitted by the 1MiB raw-pattern gate) aborts at the first
	// over-bound run with bounded work — about one run's exact-emission
	// charge — instead of allocating about 25GB of discarded attempts.
	big := farm(9000)
	require.Equal(t, 943894, len(big))
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err = expandRegexpCaseFoldRuns(big, true)
	runtime.ReadMemStats(&after)
	require.Error(t, err)
	require.Contains(t, err.Error(),
		"regexp case-fold resource limit exceeded: literal-run emission budget of 131072 bytes")
	t.Logf("9,000-distinct-run expansion allocated %d bytes", after.TotalAlloc-before.TotalAlloc)
	require.LessOrEqual(t, after.TotalAlloc-before.TotalAlloc, uint64(32<<20))
	// Below the budgets the rewrite stays exact: distinct 80-s runs and a
	// trailing foldable run fold fully (the trailing comment is trivia and
	// emits nothing).
	run80 := strings.Repeat("s", 80)
	got, err := expandRegexpCaseFoldRuns(run80+"0|"+run80+"1|ss(?#x)", true)
	require.NoError(t, err)
	exp80a, err := expandRegexpCaseFoldRuns(run80+"0(?#x)", true)
	require.NoError(t, err)
	exp80b, err := expandRegexpCaseFoldRuns(run80+"1(?#x)", true)
	require.NoError(t, err)
	tail := `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`
	require.Equal(t, exp80a+"|"+exp80b+"|"+tail, got)
	// The shared work budget aborts the rewrite with the bounded error
	// naming the work budget, and the first failure wins. Seeded directly
	// because the public entry reaches the emission and per-run bounds
	// first: the work budget is the backstop for construction paths whose
	// cost outgrows their emission (discarded segmentation walks,
	// quantified atoms, class augmentation).
	exp := &regexpCaseFoldExpander{ci: true, remain: regexpCaseFoldMaxExpandedBytes, workRemain: 8}
	exp.chargeWork(9)
	require.True(t, exp.failed)
	require.Error(t, exp.failReason)
	require.Contains(t, exp.failReason.Error(),
		"regexp case-fold resource limit exceeded: rewrite work budget of 16777216 bytes")
	exp.chargeWork(1)
	require.Contains(t, exp.failReason.Error(), "rewrite work budget of 16777216 bytes")
}

// TestRegexp2CaseFoldIdenticalAlternativesDedup pins invariant 5 of the
// issue #31175 design (r5): byte-identical sibling alternatives free of
// captures, references and unscoped option setters, evaluated under the
// same effective inline-option state, are duplicates the rewrite removes
// before expansion; alternation idempotence keeps the match set unchanged.
// The binding engine bound for long fold runs on the regexp2 route is the
// compiled backtracking-state limit (4096 tracks), not the translated-size
// limit: one 80-s fold run expands to about 68,958 bytes and compiles past
// the track bound (a 40-s run compiles at 1,439 tracks), so the round-14
// four-alternative comment-bearing shape — deduplicated to two distinct
// branches by the trailing (?#x) — raises the bounded resource-limit error
// under 'i' even after deduplication. Below the bound the deduplicated
// form folds fully.
func TestRegexp2CaseFoldIdenticalAlternativesDedup(t *testing.T) {
	alt := strings.Repeat("s", 80)
	pat := alt + "|" + alt + "|" + alt + "|" + alt + "(?#x)"
	require.Equal(t, 328, len(pat))
	// The duplicate alternatives are removed before expansion — the
	// trailing comment joins the last branch, keeping it distinct — but
	// the two remaining 80-s fold runs still compile past the regexp2
	// backtracking-state bound, so the faithful expansion raises the
	// bounded resource-limit error instead of trading down to the
	// engine's per-character caseless comparison (design error contract).
	rs := &regexpSet{}
	_, err := rs.regularLike(pat, alt, "i")
	require.Error(t, err)
	require.Contains(t, err.Error(), "regexp case-fold resource limit exceeded:")
	require.Contains(t, err.Error(), "backtracking state exceeds")
	// Case-sensitive control: identical text matches; the rewrite never
	// applies.
	rsC := &regexpSet{}
	matchedC, err := rsC.regularLike(pat, alt, "c")
	require.NoError(t, err)
	require.True(t, matchedC)
	// Below the compiled-state bound the deduplicated form folds fully on
	// the regexp2 route: four identical 12-s alternatives collapse to the
	// comment-bearing pair, and six ß equal twelve s under ICU full case
	// folding, exactly as the MySQL oracle.
	short := strings.Repeat("s", 12)
	shortPat := short + "|" + short + "|" + short + "|" + short + "(?#x)"
	rsShort := &regexpSet{}
	shortMatched, err := rsShort.regularLike(shortPat, strings.Repeat("ß", 6), "i")
	require.NoError(t, err)
	require.True(t, shortMatched)
	// A pattern whose expansion stays inside the admission limits still
	// folds fully.
	rsSmall := &regexpSet{}
	smallMatched, err := rsSmall.regularLike("ss(?#x)", "ß", "i")
	require.NoError(t, err)
	require.True(t, smallMatched)
}

// TestRE2CaseFoldIdenticalAlternativesDedup pins the same deduplication
// contract on the RE2 route, which has no compiled backtracking-state
// bound: four identical 80-s alternatives collapse to a single branch
// before expansion, the 44,502-byte expansion is admitted, and forty ß
// match eighty s under 'i' — the MySQL-oracle outcome that the regexp2
// route can only deliver below its 4096-track compiled-state bound
// (design r5, invariant 5).
func TestRE2CaseFoldIdenticalAlternativesDedup(t *testing.T) {
	alt := strings.Repeat("s", 80)
	pat := alt + "|" + alt + "|" + alt + "|" + alt
	rs := &regexpSet{}
	matched, err := rs.regularLike(pat, strings.Repeat("ß", 40), "i")
	require.NoError(t, err)
	require.True(t, matched)
	rsC := &regexpSet{}
	matchedC, err := rsC.regularLike(pat, alt, "c")
	require.NoError(t, err)
	require.True(t, matchedC)
}

// TestRegexp2CaseFoldExpandedAdmissionResourceLimit pins the flipped
// round-14 silent-fallback pin on the regexp2 route: when the faithful
// full-fold expansion of a caseless pattern exceeds a rewrite or engine
// bound even after duplicate removal, the match raises the bounded
// resource-limit error instead of trading down to the engine's
// per-character caseless comparison (design error contract). Twenty
// distinct 80-s alternatives expand past the 1MiB whole-pattern emission
// budget while the raw pattern passes every gate without the expansion, so
// the bounded error — never a narrowed per-character verdict — is the
// outcome.
func TestRegexp2CaseFoldExpandedAdmissionResourceLimit(t *testing.T) {
	run80 := strings.Repeat("s", 80)
	parts := make([]string, 20)
	for i := range parts {
		parts[i] = run80 + strconv.Itoa(i)
	}
	pat := strings.Join(parts, "|") + "(?#x)"
	rs := &regexpSet{}
	_, err := rs.regularLike(pat, run80+"0", "i")
	require.Error(t, err)
	require.Contains(t, err.Error(), "regexp case-fold resource limit exceeded:")
	// The case-sensitive route never expands and keeps the base verdict.
	rsC := &regexpSet{}
	matchedC, err := rsC.regularLike(pat, run80+"0", "c")
	require.NoError(t, err)
	require.True(t, matchedC)
}

// TestDedupRegexpIdenticalAlternatives pins the exact eligibility and
// deletion rules of the sibling-alternative deduplication (issue #31175
// design r4, invariant 5): byte-identical branches under the same
// effective inline-option state collapse with the first occurrence kept;
// captures, references, unscoped option setters and any intervening
// unscoped setter block the deletion; scoped setters do not; malformed
// inputs return unchanged.
func TestDedupRegexpIdenticalAlternatives(t *testing.T) {
	rows := []struct {
		name string
		pat  string
		want string
	}{
		{"top level pair", `a|a`, `a`},
		{"top level triple", `a|a|a`, `a`},
		{"first occurrence kept", `a|b|a`, `a|b`},
		{"group pair", `(?:a|a)`, `(?:a)`},
		{"distinct siblings kept", `a|b`, `a|b`},
		{"anchors are branch text", `^a|^a`, `^a`},
		{"class pipe is not a separator", `[a|]|[a|]`, `[a|]`},
		{"comment pipe is not a separator", `(?#a|b)x|(?#a|b)x`, `(?#a|b)x`},
		{"intervening unscoped setter retains", `^(?:ss|(?i)a|ss)$`, `^(?:ss|(?i)a|ss)$`},
		{"scoped setter exempt", `^(?:ss|(?i:a)|ss)$`, `^(?:ss|(?i:a))$`},
		{"setter before the group does not block", `(?i)(?:ss|ss)`, `(?i)(?:ss)`},
		{"unscoped setter inside duplicate retains", `(?:(?i)a|(?i)a)`, `(?:(?i)a|(?i)a)`},
		{"capture-bearing retained", `^((ss)|(ss))\3$`, `^((ss)|(ss))\3$`},
		{"backreference-bearing retained", `(?:a|\1)`, `(?:a|\1)`},
		{"named reference retained", `(?:(?<n>a)|\k<n>)`, `(?:(?<n>a)|\k<n>)`},
		{"unterminated group bails", `(?:a|a`, `(?:a|a`},
		{"unbalanced closer bails", `a|a)`, `a|a)`},
		{"unterminated class bails", `[a|[a|`, `[a|[a|`},
		{"free-spacing header before group", `(?x)(?:a|a)`, `(?x)(?:a)`},
		// Flags of constructs nested inside groups fold into the enclosing
		// branch when the group closes, so a duplicate outer branch whose
		// text contains a nested capture, reference, or unscoped setter is
		// retained exactly as if the construct sat at the outer level.
		{"nested capture retained", `b(?:x(c)y)|b(?:x(c)y)`, `b(?:x(c)y)|b(?:x(c)y)`},
		{"nested reference retained", `^(a)(?:q(?:\1)|q(?:\1))$`, `^(a)(?:q(?:\1)|q(?:\1))$`},
		{"nested unscoped setter retained", `q(?:x(?i)y)|q(?:x(?i)y)`, `q(?:x(?i)y)|q(?:x(?i)y)`},
		{"nested scoped setter exempt", `q(?:x(?i:i)y)|q(?:x(?i:i)y)`, `q(?:x(?i:i)y)`},
		// An unscoped setter one level down blocks siblings of its own
		// alternation but does not mark the enclosing branch's level.
		{"nested setter blocks own level", `(?:(?i)|a|a)`, `(?:(?i)|a)`},
		{"nested duplicate collapse", `(?:(?:a|a))`, `(?:(?:a))`},
		{"duplicate enclosing group", `(?:a|a)b|(?:a|a)b`, `(?:a)b`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			require.Equal(t, row.want,
				dedupRegexpIdenticalAlternatives(row.pat, requiresRegexp2Pattern(row.pat),
					regexpCaseFoldMaxDedupWorkBytes))
		})
	}
}

// TestDedupRegexpIdenticalAlternativesWorkBound pins the linear charged-work
// model and the abandonment contract of the deduplication prepass (issue
// #31175, implementation review on the r5 head): the prepass runs before the
// rewrite budgets are initialized, so it carries its own bound. The scan
// charges the pattern length once, each alternation closure charges the
// branch-text bytes it hashes, and the close-time flag folding charges one
// unit per branch; exhausting the budget abandons the prepass and returns
// the source pattern byte for byte, discarding any recorded deletions.
// Before the bound, 65,536 identical siblings forced (N-1)(N-2)/2 setter
// rescans and 32,768 nested captures forced a full ancestor walk per group.
func TestDedupRegexpIdenticalAlternativesWorkBound(t *testing.T) {
	dedup := func(pat string, budget int) string {
		return dedupRegexpIdenticalAlternatives(pat, requiresRegexp2Pattern(pat), budget)
	}
	// Exact charged totals: "a|a" charges 3 (scan) + 1 + 1 (hashed branch
	// bytes) = 5; "(?:a|a)" charges 7 (scan) + 1 + 1 (hashes) + 2 (folded
	// branches) = 11. One unit below the total abandons the prepass.
	for _, row := range []struct {
		name   string
		pat    string
		budget int
		want   string
	}{
		{"top-level exact budget", `a|a`, 5, `a`},
		{"top-level one below budget", `a|a`, 4, `a|a`},
		{"top-level zero budget", `a|a`, 0, `a|a`},
		{"group exact budget", `(?:a|a)`, 11, `(?:a)`},
		{"group one below budget", `(?:a|a)`, 10, `(?:a|a)`},
	} {
		t.Run(row.name, func(t *testing.T) {
			require.Equal(t, row.want, dedup(row.pat, row.budget))
		})
	}

	// The review's probe shapes complete far below the production budget:
	// 65,536 identical siblings charge 196,607 (scan) + 131,072 (hashes) =
	// 327,679 and collapse to one branch; 32,768 nested captures around one
	// duplicate pair charge 65,539 (scan) + 2 (hashes) + 32,769 (folded
	// branches) = 98,310 and still deduplicate.
	const alts = 65536
	flat := strings.Repeat("ss|", alts-1) + "ss"
	require.Equal(t, "ss", dedup(flat, regexpCaseFoldMaxDedupWorkBytes))
	const depth = 32768
	nested := strings.Repeat("(", depth) + "a|a" + strings.Repeat(")", depth)
	require.Equal(t, strings.Repeat("(", depth)+"a"+strings.Repeat(")", depth),
		dedup(nested, regexpCaseFoldMaxDedupWorkBytes))

	// Deeply nested alternations re-hash the enclosing branch text at every
	// level, so charged work grows with depth x pattern length; the budget
	// abandons the prepass and returns the source byte for byte (partial
	// deletions discarded) where a shallow control of the same shape still
	// deduplicates its core pair. The depth-2000 shape would charge about
	// 410 MB of branch hashing without the bound.
	nest := func(depth, pad int) string {
		pat := "(?:a|a)"
		for i := 0; i < depth; i++ {
			pat = "(?:" + strings.Repeat("x", pad) + "|" + strings.Repeat("y", pad) + pat + ")"
		}
		return pat
	}
	deep := nest(2000, 100)
	require.Equal(t, deep, dedup(deep, regexpCaseFoldMaxDedupWorkBytes))
	shallow := nest(50, 100)
	wantShallow := strings.Replace(shallow, "(?:a|a)", "(?:a)", 1)
	require.Equal(t, wantShallow, dedup(shallow, regexpCaseFoldMaxDedupWorkBytes))
}

// TestRegexpCaseFoldDedupEligibility pins the invariant-5 eligibility
// boundary on both engine routes (issue #31175 design r4): a duplicate is
// removed only when it is byte-identical to the kept occurrence, free of
// captures, backreferences and unscoped option setters, AND evaluated
// under the same effective inline-option state. An intervening sibling's
// unscoped setter changes the duplicate's ambient state: in
// ^(?:ss|(?i)a|ss)$ under a case-sensitive ambient mode only the second
// "ss" inherits (?i) and matches SS and ß, so deleting it would silently
// narrow the match set. Scoped (?flags:...) groups restore the prior state
// at group end, so the scoped control still deduplicates.
func TestRegexpCaseFoldDedupEligibility(t *testing.T) {
	// Intervening unscoped setter: the duplicate is retained, so SS and ß
	// match through the second "ss" branch under match_type 'c' — on the
	// RE2 route and on the lookahead-prefixed regexp2 route alike.
	for _, pat := range []string{`^(?:ss|(?i)a|ss)$`, `(?=)^(?:ss|(?i)a|ss)$`} {
		for _, subject := range []string{"ss", "SS", "ß"} {
			rs := &regexpSet{}
			matched, err := rs.regularLike(pat, subject, "c")
			require.NoError(t, err, "pattern %q subject %q", pat, subject)
			require.True(t, matched, "pattern %q subject %q", pat, subject)
		}
		rsNo := &regexpSet{}
		matchedNo, err := rsNo.regularLike(pat, "b", "c")
		require.NoError(t, err, "pattern %q", pat)
		require.False(t, matchedNo, "pattern %q", pat)
	}
	// Scoped setter control: (?i:...) restores the enclosing state at group
	// end, so both "ss" occurrences share one effective option state and
	// the duplicate is removed — SS and ß do not match under 'c', exactly
	// as for the single-occurrence form.
	for _, pat := range []string{`^(?:ss|(?i:a)|ss)$`, `(?=)^(?:ss|(?i:a)|ss)$`} {
		rsSS := &regexpSet{}
		matchedSS, err := rsSS.regularLike(pat, "ss", "c")
		require.NoError(t, err, "pattern %q", pat)
		require.True(t, matchedSS, "pattern %q", pat)
		for _, subject := range []string{"SS", "ß"} {
			rs := &regexpSet{}
			matched, err := rs.regularLike(pat, subject, "c")
			require.NoError(t, err, "pattern %q subject %q", pat, subject)
			require.False(t, matched, "pattern %q subject %q", pat, subject)
		}
	}
	// Equal-state duplicates are removed without changing the match set:
	// ss|ss|ss|ss behaves exactly like ss under 'i' over the fold matrix
	// (the MySQL oracle matches ß, ẞ and STRAßE through the full fold).
	subjects := map[string]bool{
		"ss": true, "SS": true, "Ss": true, "s": false,
		"ß": true, "ẞ": true, "STRASSE": true, "STRAßE": true, "STRASE": false,
	}
	for subject, want := range subjects {
		rsDup := &regexpSet{}
		gotDup, err := rsDup.regularLike("ss|ss|ss|ss", subject, "i")
		require.NoError(t, err, "subject %q", subject)
		require.Equal(t, want, gotDup, "deduplicated pattern, subject %q", subject)
		rsOne := &regexpSet{}
		gotOne, err := rsOne.regularLike("ss", subject, "i")
		require.NoError(t, err, "subject %q", subject)
		require.Equal(t, gotOne, gotDup, "subject %q", subject)
	}
	// Capture-bearing duplicates are retained: group numbering and
	// participation are user-observable, so ^((ss)|(ss))\3$ keeps both
	// groups and the backreference can match through the second one.
	rsCap := &regexpSet{}
	capMatched, err := rsCap.regularLike(`^((ss)|(ss))\3$`, "ssss", "c")
	require.NoError(t, err)
	require.True(t, capMatched)
}

// TestRE2CaseFoldAdmissionResourceLimit pins the composition of the
// case-fold rewrite with the RE2 admission bound under the resource-limit
// error contract (round-15 pre-PR review, revised): nested capture groups
// around a foldable run sit just inside Go regexp's parse-tree height
// bound (regexp/syntax maxHeight=1000), and the expansion's extra levels
// push the pattern over it. The faithful full-fold form cannot be evaluated
// within the engine bound, so the match raises the bounded resource-limit
// error instead of silently trading down to the engine's per-character
// caseless comparison (design error contract). A pattern the unexpanded
// admission already rejects keeps the source verdict.
func TestRE2CaseFoldAdmissionResourceLimit(t *testing.T) {
	nested := func(n int) string {
		return strings.Repeat("(", n) + "ss" + strings.Repeat(")", n)
	}
	// The measured boundary on Go 1.27.1: the unexpanded admission accepts
	// 999 nested captures around 'ss' (parse-tree height 1000) and rejects
	// from 1000; the expanded form is already rejected from 998.
	deep := nested(999)
	require.Equal(t, 2000, len(deep))
	// The faithful expansion exceeds the engine bound, so the caseless
	// match raises the bounded resource-limit error: the stable prefix
	// names the contract, the underlying admission detail follows it.
	rs := &regexpSet{}
	_, err := rs.regularLike(deep, "ss", "i")
	require.Error(t, err)
	require.Contains(t, err.Error(),
		"regexp case-fold resource limit exceeded: expanded-pattern admission:")
	rsFold := &regexpSet{}
	_, err = rsFold.regularLike(deep, "ß", "i")
	require.Error(t, err)
	require.Contains(t, err.Error(),
		"regexp case-fold resource limit exceeded: expanded-pattern admission:")
	// Case-sensitive control: identical text matches; the expansion never
	// applies under 'c'.
	rsC := &regexpSet{}
	matchedC, err := rsC.regularLike(deep, "ss", "c")
	require.NoError(t, err)
	require.True(t, matchedC)
	// The accept side of the nesting boundary still folds fully: 997 nested
	// captures leave room for the expansion, so the one-character subject
	// matches through the ß alternative.
	rsShallow := &regexpSet{}
	shallowMatched, err := rsShallow.regularLike(nested(997), "ß", "i")
	require.NoError(t, err)
	require.True(t, shallowMatched)
	// A pattern the unexpanded admission already rejects keeps the source
	// verdict: the base admission's error quotes the original pattern,
	// never the expanded form, and carries no resource-limit prefix.
	rsDeep := &regexpSet{}
	_, err = rsDeep.regularLike(nested(1000), "ss", "i")
	require.Error(t, err)
	require.Contains(t, err.Error(), "nests too deeply")
	require.NotContains(t, err.Error(), "(?-i:ß)")
	require.NotContains(t, err.Error(), "resource limit")
}

// TestRegexp2CaseFoldSourceQuantifierVerdict pins the composition of the
// case-fold rewrite with the regexp2 source-pattern validation (round-16
// pre-PR review finding F1): the vendored engine rejects a malformed
// repeat such as "ss{" with 'invalid input: invalid regular expression
// quantifier' (ICU: U_REGEX_BAD_INTERVAL), but the fold expansion escaped
// the unmatched '{' into a literal, so the rewritten rule compiled and
// matched — the rewrite silenced a syntax error the unexpanded admission
// raises. The admission now validates the source rule before the rewrite
// can escape it, so the malformed pattern keeps the source verdict under
// every match type.
func TestRegexp2CaseFoldSourceQuantifierVerdict(t *testing.T) {
	malformed := []string{
		"ss{(?#x)",    // unmatched '{'
		"ss{2(?#x)",   // unterminated count
		"ss{a}(?#x)",  // non-numeric interval body
		"ss{,5}(?#x)", // leading-comma interval
	}
	for _, pat := range malformed {
		for _, mt := range []string{"i", "c"} {
			rs := &regexpSet{}
			_, err := rs.regularLike(pat, "ss{", mt)
			require.Error(t, err, "pattern %q match_type %q", pat, mt)
			require.Contains(t, err.Error(), "invalid regular expression quantifier",
				"pattern %q match_type %q", pat, mt)
		}
	}
	// A well-formed repeat whose minimum exceeds its maximum still fails
	// inside the engine (ICU: U_REGEX_MAX_LT_MIN); the source validation
	// passes it through. Identical verdict before and after the repair.
	rsBounds := &regexpSet{}
	_, err := rsBounds.regularLike("ss{2,1}(?#x)", "ss", "i")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid repeat count")
	// The correctly escaped control keeps matching the literal brace,
	// case-folded under 'i' and byte-exact under 'c'.
	for _, mt := range []string{"i", "c"} {
		rs := &regexpSet{}
		matched, err := rs.regularLike(`ss\{(?#x)`, "ss{", mt)
		require.NoError(t, err)
		require.True(t, matched)
	}
	rsFold := &regexpSet{}
	foldMatched, err := rsFold.regularLike(`ss\{(?#x)`, "SS{", "i")
	require.NoError(t, err)
	require.True(t, foldMatched)
	rsC := &regexpSet{}
	upperC, err := rsC.regularLike(`ss\{(?#x)`, "SS{", "c")
	require.NoError(t, err)
	require.False(t, upperC)
	// The RE2 route reads the unmatched '{' as a literal character (Go
	// regexp has no interval error for it): a pre-existing engine
	// divergence, unchanged by this repair.
	rsRE2 := &regexpSet{}
	re2Matched, err := rsRE2.regularLike("ss{", "SS{", "i")
	require.NoError(t, err)
	require.True(t, re2Matched)
}
