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
			require.Equal(t, row.want, expandRegexpCaseFoldRuns(row.pat, row.ci))
		})
	}
	// Invalid UTF-8 and oversized expansions return the input unchanged.
	invalid := "ss\xffss"
	require.Equal(t, invalid, expandRegexpCaseFoldRuns(invalid, true))
	// Round-6 F3: runs past the flat enumeration cap expand exactly (divide
	// and conquer) instead of falling back to the verbatim bytes; the
	// expanded pattern keeps the boundary-exact full-fold semantics.
	longRun := strings.Repeat("s", 80)
	expanded := expandRegexpCaseFoldRuns(longRun, true)
	require.NotEqual(t, longRun, expanded)
	require.LessOrEqual(t, len(expanded), regexpCaseFoldMaxRunBytes)
	reg, err := regexp.Compile("(?i)^" + expanded + "$")
	require.NoError(t, err)
	require.True(t, reg.MatchString(strings.Repeat("ß", 40)))
	require.False(t, reg.MatchString(strings.Repeat("ß", 39)+"s"))
	require.False(t, reg.MatchString(strings.Repeat("ß", 40)+"s"))
	// A run whose exact expansion would exceed the per-run byte bound still
	// falls back to its verbatim bytes, documented in the issue contract.
	hugeRun := strings.Repeat("s", 20000)
	require.Equal(t, hugeRun, expandRegexpCaseFoldRuns(hugeRun, true))
	// Round-12 F2: the orbit class emission ([sſ], four bytes per position)
	// keeps the regexp2 route's factored long-run expansion exact and within
	// the per-run byte bound, just like the RE2 route above.
	longRun2 := strings.Repeat("s", 80) + "(?#x)"
	expanded2 := expandRegexpCaseFoldRuns(longRun2, true)
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
			require.Equal(t, row.want, expandRegexpCaseFoldRuns(row.pat, row.ci))
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
			require.Equal(t, row.want, expandRegexpCaseFoldRuns(row.pat, row.ci))
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
// budget (round-13 pre-PR review): the 1MiB bound is enforced while the
// pattern is emitted, so a pattern of many medium runs is abandoned as soon
// as the budget is exhausted instead of materializing the complete oversized
// expansion before the deferred length check discards it (10,000 80-s runs
// built 689,589,999 bytes, about 658MiB). The accept/reject boundary itself
// is unchanged.
func TestExpandRegexpCaseFoldRunsTotalBudget(t *testing.T) {
	run80 := strings.Repeat("s", 80)
	runs := func(k int) string {
		parts := make([]string, k)
		for i := range parts {
			parts[i] = run80
		}
		return strings.Join(parts, "|") + "(?#x)"
	}
	// Boundary-exact acceptance, derived from the measured single-run
	// emission so the pin stays exact if the emission shape changes: k runs
	// expand to k*len(one)+(k-1) separator bytes (the trailing comment is
	// trivia and emits nothing).
	one := expandRegexpCaseFoldRuns(run80+"(?#x)", true)
	require.NotEqual(t, run80+"(?#x)", one)
	per := len(one) + 1
	kAcc := (regexpCaseFoldMaxExpandedBytes + 1) / per
	require.Greater(t, kAcc, 0)
	require.LessOrEqual(t, kAcc*per-1, regexpCaseFoldMaxExpandedBytes)
	require.Greater(t, (kAcc+1)*per-1, regexpCaseFoldMaxExpandedBytes)
	accept := runs(kAcc)
	expanded := expandRegexpCaseFoldRuns(accept, true)
	require.NotEqual(t, accept, expanded)
	require.LessOrEqual(t, len(expanded), regexpCaseFoldMaxExpandedBytes)
	// One more run crosses the bound: the input comes back verbatim, the
	// documented whole-pattern fallback.
	reject := runs(kAcc + 1)
	require.Equal(t, reject, expandRegexpCaseFoldRuns(reject, true))
	// The review counterexample: 10,000 medium runs (810,004 raw bytes,
	// admitted by the 1MiB raw-pattern gate) must fall back with bounded
	// emission work instead of building about 658MiB first. The returned
	// value is still the verbatim fallback; the byte measurement bounds the
	// work performed, not only the result.
	big := runs(10000)
	require.Equal(t, 810004, len(big))
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got := expandRegexpCaseFoldRuns(big, true)
	runtime.ReadMemStats(&after)
	require.Equal(t, big, got)
	t.Logf("10,000-run overflow expansion allocated %d bytes", after.TotalAlloc-before.TotalAlloc)
	require.LessOrEqual(t, after.TotalAlloc-before.TotalAlloc, uint64(32<<20))
}

// TestExpandRegexpCaseFoldRunsWorkBudget pins the emission-construction
// work budget (round-14 pre-PR review): the per-run and whole-pattern
// emission bounds multiply when a pattern carries many runs whose exact
// per-run expansion is discarded — 10,000 100-s runs joined by '|' plus a
// trailing comment (raw 1,010,004 bytes, admitted by the 1MiB raw-pattern
// gate) allocated about 25GB of intermediate concatenations before
// returning, because every discarded per-run attempt was free and the
// emitted result (verbatim runs, comment trivia dropped) stayed below the
// whole-pattern emission bound. The work budget charges every
// emission-construction byte, including discarded attempts, so the rewrite
// is abandoned with bounded work and the original pattern is used
// unchanged. Below the budget the rewrite is byte-identical.
func TestExpandRegexpCaseFoldRunsWorkBudget(t *testing.T) {
	run100 := strings.Repeat("s", 100)
	farm := func(k int) string {
		parts := make([]string, k)
		for i := range parts {
			parts[i] = run100
		}
		return strings.Join(parts, "|") + "(?#x)"
	}
	// Below the budget each run keeps its verbatim bytes (its exact
	// expansion exceeds the per-run bound) and the trailing comment trivia
	// is dropped — byte-identical to the unbudgeted rewrite. One 100-s run
	// charges 1,546,452 work bytes, so ten runs (15,464,520) still fit the
	// 16MiB budget: this is the accept side of the work boundary.
	small := farm(10)
	require.Equal(t, 1014, len(small))
	verbatimRuns := make([]string, 10)
	for i := range verbatimRuns {
		verbatimRuns[i] = run100
	}
	require.Equal(t, strings.Join(verbatimRuns, "|"), expandRegexpCaseFoldRuns(small, true))
	// A foldable run after the discarded runs still folds exactly.
	tail := `(?:[sſ][sſ]|(?:[sſ][sſ]|(?-i:ß)|(?-i:ẞ)))`
	withTailParts := make([]string, 0, 11)
	withTailParts = append(withTailParts, verbatimRuns...)
	withTailParts = append(withTailParts, "ss")
	withTail := strings.Join(withTailParts, "|") + "(?#x)"
	wantTail := make([]string, 0, 11)
	wantTail = append(wantTail, verbatimRuns...)
	wantTail = append(wantTail, tail)
	require.Equal(t, strings.Join(wantTail, "|"), expandRegexpCaseFoldRuns(withTail, true))
	// The reject side of the work boundary: eleven runs would charge
	// 17,010,972 bytes, so the budget runs out during the eleventh run and
	// the whole-pattern fallback returns the input verbatim (the comment
	// is preserved), where the unbudgeted code returned the 1,110-byte
	// rewrite after building every discarded attempt.
	eleven := farm(11)
	require.Equal(t, 1115, len(eleven))
	require.Equal(t, eleven, expandRegexpCaseFoldRuns(eleven, true))
	// The review counterexample: 10,000 discarded per-run attempts must
	// abandon the rewrite with bounded work instead of allocating about
	// 25GB of intermediates. The whole-pattern fallback returns the input
	// verbatim; the unbudgeted code returned the 1,009,999-byte rewrite
	// (verbatim runs, comment dropped) after the full allocation.
	big := farm(10000)
	require.Equal(t, 1010004, len(big))
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got := expandRegexpCaseFoldRuns(big, true)
	runtime.ReadMemStats(&after)
	require.Equal(t, big, got)
	t.Logf("10,000-discarded-run expansion allocated %d bytes", after.TotalAlloc-before.TotalAlloc)
	require.LessOrEqual(t, after.TotalAlloc-before.TotalAlloc, uint64(256<<20))
}

// TestRegexp2CaseFoldAdmissionFallback pins the composition of the
// case-fold rewrite with the regexp2 admission limits (round-14 pre-PR
// review): the expansion can push a pattern the unexpanded admission
// accepted over a downstream size limit (the 1MiB translated bound here),
// which turned a base-admitted REGEXP_LIKE into an error. The admission
// retries without the expansion, so the pattern keeps the verdict the base
// gave it — matching through the engine's per-character caseless
// comparison instead of the full-fold expansion.
func TestRegexp2CaseFoldAdmissionFallback(t *testing.T) {
	alt := strings.Repeat("s", 80)
	pat := alt + "|" + alt + "|" + alt + "|" + alt + "(?#x)"
	require.Equal(t, 328, len(pat))
	// The expanded rule needs about 4*68,958 bytes and its translation
	// exceeds the 1MiB translated bound; unexpanded it is admitted.
	// Case-insensitive matching keeps the base verdict: a match, no error.
	rs := &regexpSet{}
	matched, err := rs.regularLike(pat, alt, "i")
	require.NoError(t, err)
	require.True(t, matched)
	// The fallback matches per character: a subject needing the full fold
	// no longer matches. The expansion trades down to the base admission
	// verdict, never to an error.
	rsFold := &regexpSet{}
	foldMatched, err := rsFold.regularLike(pat, strings.Repeat("ß", 40), "i")
	require.NoError(t, err)
	require.False(t, foldMatched)
	// Case-sensitive control: identical text matches; the expansion never
	// applies.
	rsC := &regexpSet{}
	matchedC, err := rsC.regularLike(pat, alt, "c")
	require.NoError(t, err)
	require.True(t, matchedC)
	// A pattern whose expansion stays inside the admission limits still
	// folds fully.
	rsSmall := &regexpSet{}
	smallMatched, err := rsSmall.regularLike("ss(?#x)", "ß", "i")
	require.NoError(t, err)
	require.True(t, smallMatched)
}

// TestRE2CaseFoldAdmissionFallback pins the composition of the case-fold
// rewrite with the RE2 admission bound (round-15 pre-PR review): nested
// capture groups around a foldable run sit just inside Go regexp's
// parse-tree height bound (regexp/syntax maxHeight=1000), and the
// expansion's extra levels pushed the pattern over it, turning a
// base-admitted REGEXP_LIKE into 'expression nests too deeply'. The
// admission retries without the expansion, so the pattern keeps the verdict
// the base gave it — matching through the engine's per-character caseless
// comparison instead of the full-fold expansion. A pattern the unexpanded
// admission rejects stays rejected, with the base error.
func TestRE2CaseFoldAdmissionFallback(t *testing.T) {
	nested := func(n int) string {
		return strings.Repeat("(", n) + "ss" + strings.Repeat(")", n)
	}
	// The measured boundary on Go 1.27.1: the unexpanded admission accepts
	// 999 nested captures around 'ss' (parse-tree height 1000) and rejects
	// from 1000; the expanded form was already rejected from 998.
	deep := nested(999)
	require.Equal(t, 2000, len(deep))
	// The deep pattern keeps the base verdict: a caseless match, no error.
	rs := &regexpSet{}
	matched, err := rs.regularLike(deep, "ss", "i")
	require.NoError(t, err)
	require.True(t, matched)
	rsUpper := &regexpSet{}
	matchedUpper, err := rsUpper.regularLike(deep, "SS", "i")
	require.NoError(t, err)
	require.True(t, matchedUpper)
	// The fallback matches per character: a subject needing the full fold
	// no longer matches. The expansion trades down to the base admission
	// verdict, never to an error.
	rsFold := &regexpSet{}
	foldMatched, err := rsFold.regularLike(deep, "ß", "i")
	require.NoError(t, err)
	require.False(t, foldMatched)
	// Case-sensitive control: identical text matches; the expansion never
	// applies.
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
	// A pattern the unexpanded admission already rejects stays rejected,
	// and the surfaced error is the base admission's: it quotes the
	// original pattern, never the expanded form.
	rsDeep := &regexpSet{}
	_, err = rsDeep.regularLike(nested(1000), "ss", "i")
	require.Error(t, err)
	require.Contains(t, err.Error(), "nests too deeply")
	require.NotContains(t, err.Error(), "(?-i:ß)")
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
