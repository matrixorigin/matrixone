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
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"golang.org/x/text/cases"
)

// Case-insensitive REGEXP literal matching follows MySQL, whose ICU engine
// compares literals by full Unicode case folding: with match_type 'i' (or an
// embedded (?i) scope), the literal run "ss" matches "ß" and "STRASSE"
// matches "Straße", because full folding maps ß to ss. Neither RE2 nor the
// regexp2 compatibility engine folds across characters on its own; their
// case-insensitive modes compare single characters by simple case mapping.
// This file rewrites maximal unquantified literal runs of a pattern into
// bounded alternations that accept exactly the subject strings whose full
// case fold equals the run's fold image, reproducing ICU's behavior with the
// engines here (issue #31175 review):
//
//   - Every pattern character maps to its full fold image (ß to ss, ﬀ to ff,
//     İ to i+U+0307, other characters to their single-rune fold). The
//     concatenation is the run's fold image F.
//   - F is segmented into single-rune blocks (emitted as ordinary literals
//     that inherit the engine's case-insensitive per-character orbit) and
//     fold-image blocks (emitted as (?:image|(?-i:source)) alternations).
//     Sources are case-sensitive so the engine's simple-fold partners of a
//     source cannot leak in extra characters; the reverse fold index lists
//     every full-fold source explicitly ("ss" from ß and U+1E9E, which ICU
//     78.3 matches against both ß and "ss", probed for issue #31175).
//   - Quantified atoms keep their single-character case orbit: a table
//     character emits the sources of its fold image as a case-sensitive
//     alternation (ᾈ+ matches ᾀ, its case pair; ß+ and ẞ+ each match both ß
//     and ẞ, the sources of "ss"). ICU matches quantified atoms per
//     character without spanning a subject's fold expansion, and so does
//     this rewrite. A single-rune fold image keeps the engine's caseless
//     orbit, which on the regexp2 route is the unicode.ToLower closure:
//     an image with extras in regexpSingleCharFoldExtras is emitted as
//     the same explicit class the literal-run path uses (ς+ becomes
//     [σς]+, accepting σ, ς and Σ).
//   - Character classes keep their syntax and members on both routes; on
//     the case-insensitive regexp2 route a class additionally gains the
//     fold-orbit members the engine's ToLower class closure misses ([ς]
//     becomes [ςσ], [a-z] becomes [a-zſ]), and a nested subtraction set
//     is completed recursively, reproducing ICU's per-character case
//     closure of the whole class expression.
//   - Escapes that carry plain literal text (\Q...\E, \x{..}, \xNN) are
//     decoded into the literal stream so they fold like any other literal;
//     decoded regex metacharacters are re-escaped, and decoded whitespace
//     or '#' is re-emitted as a \x{..} escape under free-spacing mode.
//     Every other escape, character classes, anchors and groups are left
//     untouched, and named backreference tokens (\k<name>, the \g variants
//     and the deprecated angle/quote forms) are preserved verbatim so the
//     reference name keeps its case.
//   - Free-spacing mode ((?x), scoped like the case flag) and comments are
//     pattern trivia: unescaped whitespace, # line comments and (?#...)
//     comments are dropped, so the literal pieces around them fold as one
//     run exactly as ICU folds the comment-stripped run. Quoted or escaped
//     whitespace and '#' stay literal through the \x{..} re-emission above.
//
// The rewrite is deliberately conservative. A run is emitted verbatim only
// where no full-fold promise exists: when it needs no expansion (the common
// case) or when the pattern is not valid UTF-8. When the faithful full-fold
// form of fold-relevant content would exceed one of the byte bounds below —
// even after eligible identical alternatives are deduplicated — the rewrite
// fails with the bounded resource-limit error instead of silently narrowing
// the match set (issue #31175 design, invariants 5 and 6).
const (
	// regexpCaseFoldMaxRunAlternatives bounds the segmentations a run's
	// flat enumeration materializes before switching to the exact
	// divide-and-conquer emission.
	regexpCaseFoldMaxRunAlternatives = 64
	// regexpCaseFoldMaxRunBytes bounds one run's total expanded emission.
	// The exact divide-and-conquer form stays below this bound for any
	// realistic run (a fully foldable run of roughly 250 characters);
	// a larger pathological run fails the rewrite with the bounded
	// resource-limit error.
	regexpCaseFoldMaxRunBytes = 128 << 10
	// regexpCaseFoldMaxExpandedBytes bounds the whole rewritten pattern. The
	// budget is enforced while the pattern is emitted (see
	// regexpCaseFoldExpander.remain); on overflow the rewrite fails with
	// the bounded resource-limit error.
	regexpCaseFoldMaxExpandedBytes = 1 << 20
	// regexpCaseFoldMaxWorkBytes bounds the emission-construction work one
	// rewrite may perform. Every byte built while computing a run's
	// expansion is charged against it, including attempts discarded because
	// the run's expansion exceeded the per-run bound: the per-run and
	// whole-pattern emission bounds multiply otherwise (10,000 discarded
	// 100-s runs allocated about 25GB of intermediate concatenations before
	// returning, round-14 pre-PR review). On exhaustion the rewrite fails
	// with the bounded resource-limit error.
	regexpCaseFoldMaxWorkBytes = 16 << 20
	// regexpCaseFoldMaxDedupWorkBytes bounds the alternative-deduplication
	// prepass that runs before the budgets above are initialized. The
	// prepass charges the pattern length once for the scan, one unit per
	// branch for the close-time flag folding, and the branch-text bytes
	// each alternation closure hashes, so many identical siblings cannot
	// multiply the pattern length by the sibling count and deeply nested
	// alternations cannot multiply it by the nesting depth. On exhaustion
	// the prepass abandons and returns the source pattern unchanged:
	// deduplication is an emission-size optimization, the engines keep
	// their own verdicts, and the budgets above still guard the rewrite.
	regexpCaseFoldMaxDedupWorkBytes = 4 << 20
)

// regexpCaseFoldResourceLimitf builds the bounded resource-limit error a
// case-insensitive pattern raises when its faithful full-fold form exceeds
// one of the rewrite budgets above, or — at the engine admission layer —
// one of the downstream engine bounds. The message prefix is stable: BVT
// pins it with @regex (issue #31175 design error contract).
func regexpCaseFoldResourceLimitf(format string, args ...any) error {
	return moerr.NewInternalErrorNoCtxf("regexp case-fold resource limit exceeded: "+format, args...)
}

// regexpCaseFolder is the full Unicode case folder backing the fold images.
// cases.Caser is immutable and safe for concurrent use.
var regexpCaseFolder = cases.Fold()

// regexpCaseFoldApplies reports whether a pattern should receive full-fold
// literal expansion: never for binary operands, and otherwise when the
// effective match_type requests case-insensitive matching or the pattern
// itself contains a group that enables it.
func regexpCaseFoldApplies(pat, pureMatchType string, binary bool) bool {
	if binary {
		return false
	}
	if strings.ContainsRune(pureMatchType, 'i') {
		return true
	}
	return regexpPatternEnablesCaseFold(pat)
}

// regexpPatternEnablesCaseFold scans for inline flag groups that switch on
// case-insensitive matching, for example (?i) or (?mi:...). Flag parsing
// stops at ':' or ')'; an 'i' or 'I' enables folding unless a '-' negated it,
// and a '+' clears the negation, mirroring the regexp2 parser's scanOptions
// (optionFromCode accepts both letter cases).
func regexpPatternEnablesCaseFold(pat string) bool {
	rest := pat
	for {
		i := strings.Index(rest, "(?")
		if i < 0 {
			return false
		}
		rest = rest[i+2:]
		negated := false
		for j := 0; j < len(rest); j++ {
			c := rest[j]
			switch {
			case c == '-':
				negated = true
			case c == '+':
				negated = false
			case (c == 'i' || c == 'I') && !negated:
				return true
			case c == ':' || c == ')':
				j = len(rest)
			case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
			default:
				j = len(rest)
			}
		}
	}
}

// expandRegexpCaseFoldRuns rewrites the maximal literal runs of pat that sit
// in case-insensitive scope, starting from ambientCI, so they match by full
// case folding. Eligible identical sibling alternatives are deduplicated
// before expansion (invariant 5 of the issue #31175 design). When the
// faithful expansion exceeds a rewrite budget, the rewrite fails and the
// bounded resource-limit error is returned: the match set is never silently
// narrowed by falling back to the engines' per-character caseless orbit.
// The emission bound is enforced during emission, so an overflowing pattern
// is abandoned as soon as the budget is exhausted instead of after the
// complete oversized expansion was built; the work bound likewise abandons
// the rewrite once the charged emission-construction work is exhausted, so
// runs whose per-run expansions are discarded cannot multiply the bounds.
// A pattern that is not valid UTF-8 carries no fold promise and is returned
// unchanged.
func expandRegexpCaseFoldRuns(pat string, ambientCI bool) (string, error) {
	if !utf8.ValidString(pat) {
		return pat, nil
	}
	regexp2 := requiresRegexp2Pattern(pat)
	pat = dedupRegexpIdenticalAlternatives(pat, regexp2, regexpCaseFoldMaxDedupWorkBytes)
	exp := &regexpCaseFoldExpander{ci: ambientCI, regexp2: regexp2, remain: regexpCaseFoldMaxExpandedBytes, workRemain: regexpCaseFoldMaxWorkBytes}
	out := exp.expand(pat)
	if exp.failed {
		return "", exp.failReason
	}
	if out == "" {
		// Only an empty pattern expands to nothing without a failure.
		return pat, nil
	}
	return out, nil
}

// dedupRegexpIdenticalAlternatives removes sibling alternatives that are
// byte-identical to the first occurrence in the same alternation when the
// duplicate is dedup-eligible (issue #31175 design, invariant 5): it
// contains no capturing group (numbered or named), no backreference or
// named/relative reference, and no unscoped inline-option setter, and no
// unscoped option setter appears between the kept occurrence and the
// duplicate at the alternation's own level. An unscoped setter applies
// from its point to the end of the enclosing group, so it changes the
// effective inline-option state of every later sibling — in
// ^(?:ss|(?i)a|ss)$ under a case-sensitive ambient mode only the second
// "ss" inherits (?i) and matches SS and ß, and deleting it would silently
// narrow the match set. Conservatively any intervening unscoped setter
// makes the duplicate ineligible, even when the setters would net out;
// scoped (?flags:...) groups restore the prior state at group end and do
// not affect siblings. The first occurrence is kept, so branch order and
// positional inline-option state are preserved. Source capture numbering,
// names and participation are user-observable (regexp2 backreferences,
// REGEXP_REPLACE replacement references, the regexp2CaptureMapping
// contract), so capture-bearing, reference-bearing, or option-setting
// branches are always retained: alternation idempotence alone never
// justifies deleting them.
//
// A pattern whose structure cannot be parsed cleanly — unbalanced groups,
// an unterminated class, comment, group header or trailing escape, or a
// group construct outside the recognized set — is returned unchanged, so
// the engines keep their own verdicts byte for byte.
//
// The prepass is linear in the pattern length and bounded by workBudget
// (regexpCaseFoldMaxDedupWorkBytes in production): the scan examines every
// byte once, each construct marks only its innermost branch in O(1) — a
// closing group folds the flags of its branches into the enclosing branch
// in one pass, so deeply nested captures, references and option setters
// cost O(1) per construct instead of an ancestor walk per construct — and
// each alternation closure makes a single pass over its branches with an
// incremental most-recent-setter index, so many identical siblings cost
// O(1) per sibling instead of a rescan of the intervening branches. The
// scan charges the pattern length once, the close-time folding charges one
// unit per branch, and the closures charge the branch-text bytes they
// hash; exhausting workBudget abandons the prepass and returns the source
// pattern unchanged, discarding any recorded deletions. Abandonment is
// always safe: deduplication only shrinks the later emission, the engines
// keep their own verdicts, and the emission/work budgets of the rewrite
// still guard the result.
func dedupRegexpIdenticalAlternatives(pat string, regexp2 bool, workBudget int) string {
	type branch struct {
		sep   int // offset of the '|' that opened the branch (-1 for the first)
		start int // offset of the branch text
		end   int
		// capture/reference/setterAny record constructs anywhere in the
		// branch text; setterTop records an unscoped inline-option setter
		// directly at the alternation's own level, which changes the
		// effective option state of every later sibling.
		capture, reference, setterAny, setterTop bool
	}
	type frame struct {
		branches []branch
		cur      branch
		x        bool // free-spacing state in effect inside this frame
	}

	// The scan charges the pattern length once up front; the close-time
	// flag folding and the alternation closures charge as they run.
	workRemain := workBudget - len(pat)
	if workRemain < 0 {
		return pat
	}
	abandoned := false
	charge := func(n int) bool {
		workRemain -= n
		if workRemain < 0 {
			abandoned = true
		}
		return !abandoned
	}

	frames := make([]frame, 1, 8)
	frames[0].cur.sep, frames[0].cur.start = -1, 0
	var deletions [][2]int

	// Construct marks land on the innermost frame's current branch only;
	// closing a group folds the flags of everything it contains into the
	// enclosing branch (see the ')' case), which is exactly the set of
	// branches whose text contains the construct. Deeply nested constructs
	// therefore cost O(1) each instead of an ancestor walk per construct.
	markCapture := func() {
		frames[len(frames)-1].cur.capture = true
	}
	markReference := func() {
		frames[len(frames)-1].cur.reference = true
	}
	markSetter := func() {
		top := &frames[len(frames)-1].cur
		top.setterAny = true
		top.setterTop = true
	}
	// closeAlternation analyzes one completed frame and records the
	// deletions of its eligible duplicate branches in a single pass: kept
	// maps each branch text to its first occurrence, and lastSetter tracks
	// the most recent branch carrying an unscoped setter at this level, so
	// the intervening-setter check costs O(1) per duplicate instead of a
	// rescan of the branches between the kept occurrence and the
	// duplicate. Each branch's text bytes are charged once for the hash.
	closeAlternation := func(f *frame) {
		all := f.branches
		if len(all) < 2 {
			return
		}
		kept := make(map[string]int, len(all))
		lastSetter := -1
		for j := 0; j < len(all); j++ {
			if !charge(all[j].end - all[j].start) {
				return
			}
			text := pat[all[j].start:all[j].end]
			i, seen := kept[text]
			if seen {
				b := all[j]
				if !b.capture && !b.reference && !b.setterAny && lastSetter <= i {
					deletions = append(deletions, [2]int{b.sep, b.end})
				}
			} else {
				kept[text] = j
			}
			if all[j].setterTop {
				lastSetter = j
			}
		}
	}

	i := 0
	for i < len(pat) {
		switch pat[i] {
		case '\\':
			if n := scanRegexpNamedReference(pat[i:]); n > 0 {
				markReference()
				i += n
				continue
			}
			if len(pat[i:]) < 2 {
				// A trailing lone escape is malformed: keep the pattern
				// unchanged for the engines' own verdict.
				return pat
			}
			if c := pat[i+1]; c >= '1' && c <= '9' {
				// A nonzero digit starts a decimal escape the engines may
				// resolve as a backreference (the regexp2 scanDecimal
				// consumes the complete digit run first, so \18 names
				// group 18): never delete a branch carrying one.
				markReference()
				j := i + 2
				for j < len(pat) && pat[j] >= '0' && pat[j] <= '9' {
					j++
				}
				i = j
				continue
			}
			i += skipRegexpEscape(pat[i:])
		case '[':
			n, closed := skipRegexpClass(pat[i:], regexp2)
			if !closed {
				return pat
			}
			i += n
		case '|':
			f := &frames[len(frames)-1]
			f.cur.end = i
			f.branches = append(f.branches, f.cur)
			f.cur = branch{sep: i, start: i + 1}
			i++
		case ')':
			if len(frames) == 1 {
				// Unbalanced close parenthesis: malformed.
				return pat
			}
			f := &frames[len(frames)-1]
			f.cur.end = i
			f.branches = append(f.branches, f.cur)
			closeAlternation(f)
			// The closed group is one element of the enclosing frame's
			// current branch: fold the flags of everything it contains
			// into that branch in one pass. setterTop is level-local — an
			// unscoped setter applies only to the siblings of its own
			// alternation — so it does not propagate.
			var hasCapture, hasReference, hasSetter bool
			for k := range f.branches {
				if !charge(1) {
					break
				}
				b := &f.branches[k]
				hasCapture = hasCapture || b.capture
				hasReference = hasReference || b.reference
				hasSetter = hasSetter || b.setterAny
			}
			if abandoned {
				return pat
			}
			frames = frames[:len(frames)-1]
			parent := &frames[len(frames)-1].cur
			parent.capture = parent.capture || hasCapture
			parent.reference = parent.reference || hasReference
			parent.setterAny = parent.setterAny || hasSetter
			i++
		case '#':
			if frames[len(frames)-1].x {
				// Free-spacing line comment: structural characters inside
				// it are trivia.
				if j := strings.IndexByte(pat[i:], '\n'); j >= 0 {
					i += j + 1
				} else {
					i = len(pat)
				}
				continue
			}
			i++
		case '(':
			rest := pat[i:]
			switch {
			case strings.HasPrefix(rest, "(?#"):
				// A (?#...) comment is trivia in every mode.
				j := strings.IndexByte(rest[3:], ')')
				if j < 0 {
					return pat
				}
				i += 3 + j + 1
			case strings.HasPrefix(rest, "(?:"), strings.HasPrefix(rest, "(?>"),
				strings.HasPrefix(rest, "(?="), strings.HasPrefix(rest, "(?!"):
				top := frames[len(frames)-1]
				frames = append(frames, frame{x: top.x, cur: branch{sep: -1, start: i + 3}})
				i += 3
			case strings.HasPrefix(rest, "(?<="), strings.HasPrefix(rest, "(?<!"):
				top := frames[len(frames)-1]
				frames = append(frames, frame{x: top.x, cur: branch{sep: -1, start: i + 4}})
				i += 4
			case strings.HasPrefix(rest, "(?<"), strings.HasPrefix(rest, "(?'"), strings.HasPrefix(rest, "(?P<"):
				// Named capturing group: scan the name to its terminator.
				var nameStart int
				var close byte
				switch {
				case strings.HasPrefix(rest, "(?<"):
					nameStart, close = 3, '>'
				case strings.HasPrefix(rest, "(?'"):
					nameStart, close = 3, '\''
				default:
					nameStart, close = 4, '>'
				}
				j := strings.IndexByte(rest[nameStart:], close)
				if j < 0 {
					return pat
				}
				markCapture()
				top := frames[len(frames)-1]
				frames = append(frames, frame{x: top.x, cur: branch{sep: -1, start: i + nameStart + j + 1}})
				i += nameStart + j + 1
			case !strings.HasPrefix(rest, "(?"):
				// Plain capturing group.
				markCapture()
				top := frames[len(frames)-1]
				frames = append(frames, frame{x: top.x, cur: branch{sep: -1, start: i + 1}})
				i++
			default:
				// (?flags) or (?flags:...): flags are ASCII letters with
				// '-' and '+' transitions; only the free-spacing letter
				// changes this scanner's state. Any other header shape is
				// left for the engines' own verdict.
				j := 2
				negated := false
				x := frames[len(frames)-1].x
				handled := false
				for j < len(rest) && !handled {
					c := rest[j]
					switch {
					case c == '-':
						negated = true
					case c == '+':
						negated = false
					case c == 'x' || c == 'X':
						x = !negated
					case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
					case c == ')':
						markSetter()
						frames[len(frames)-1].x = x
						i += j + 1
						handled = true
					case c == ':':
						frames = append(frames, frame{x: x, cur: branch{sep: -1, start: i + j + 1}})
						i += j + 1
						handled = true
					default:
						return pat
					}
					j++
				}
				if !handled {
					// Unterminated option header.
					return pat
				}
			}
		default:
			i++
		}
	}
	if len(frames) != 1 {
		// Unclosed group: malformed.
		return pat
	}
	frames[0].cur.end = len(pat)
	frames[0].branches = append(frames[0].branches, frames[0].cur)
	closeAlternation(&frames[0])
	if abandoned || len(deletions) == 0 {
		return pat
	}
	sort.Slice(deletions, func(a, b int) bool { return deletions[a][0] < deletions[b][0] })
	var out strings.Builder
	out.Grow(len(pat))
	pos := 0
	lastEnd := 0
	for _, d := range deletions {
		if d[0] < lastEnd {
			// A span nested inside an already deleted outer branch.
			continue
		}
		out.WriteString(pat[pos:d[0]])
		pos = d[1]
		lastEnd = d[1]
	}
	out.WriteString(pat[pos:])
	return out.String()
}

// regexpCaseFoldExpander tracks the case-insensitive and free-spacing modes
// of the enclosing group scope. Both come from inline option headers ((?i),
// (?x), (?-x:...) and friends) and restore at the matching close
// parenthesis, mirroring the engines' option stacks.
type regexpCaseFoldExpander struct {
	ci     bool
	x      bool
	scopes []regexpCaseFoldScope
	// regexp2 records which engine grammar the pattern follows. The
	// vendored regexp2 parser nests character classes for subtraction
	// ([a-z-[bc]]), while Go regexp has no nesting. Every production call
	// site routes the pattern by requiresRegexp2Pattern before invoking
	// the rewrite (the RE2 site sees only patterns it clears, the regexp2
	// site only patterns it raises), so the same verdict decides the flag.
	regexp2 bool
	// remain is the remaining whole-pattern emission budget, seeded from
	// regexpCaseFoldMaxExpandedBytes. Every append to the output builder
	// goes through the write helpers; the append that would exceed the
	// budget is refused and failed is set, so expand stops and reports
	// failure. This decides the same accept/reject boundary the deferred
	// post-expansion length check enforced, but as the bytes are emitted:
	// a pattern of many medium runs no longer materializes the complete
	// oversized expansion (hundreds of MiB) before it is discarded.
	remain int
	// workRemain is the remaining emission-construction work budget,
	// seeded from regexpCaseFoldMaxWorkBytes. Every byte built while
	// computing a run's expansion is charged against it, including
	// attempts that are later discarded: a run whose exact expansion
	// exceeds the per-run bound fails the rewrite, but the concatenations
	// that attempt built are real work, and a pattern of many such runs
	// otherwise multiplies the per-run cost without limit. Exhaustion
	// sets failed, so the rewrite aborts and the caller raises the bounded
	// resource-limit error, exactly like the emission budget.
	workRemain int
	failed     bool
	// failReason is the bounded resource-limit error naming the first
	// budget whose exhaustion set failed.
	failReason error
}

// fail marks the rewrite failed with the bounded resource-limit error
// naming the exhausted budget. The first failure wins.
func (e *regexpCaseFoldExpander) fail(err error) {
	if e.failed {
		return
	}
	e.failed = true
	e.failReason = err
}

// regexpCaseFoldScope is one saved enclosing-group mode pair.
type regexpCaseFoldScope struct {
	ci bool
	x  bool
}

func (e *regexpCaseFoldExpander) pushScope(ci, x bool) {
	e.scopes = append(e.scopes, regexpCaseFoldScope{ci: e.ci, x: e.x})
	e.ci = ci
	e.x = x
}

func (e *regexpCaseFoldExpander) popScope() {
	if n := len(e.scopes); n > 0 {
		e.ci = e.scopes[n-1].ci
		e.x = e.scopes[n-1].x
		e.scopes = e.scopes[:n-1]
	}
}

// write appends s to out within the remaining whole-pattern emission
// budget. The append that would exceed the budget is refused instead:
// failed is set, the builder keeps its prior bytes, and expansion aborts at
// the next check. Because every append is accounted, the refusal happens
// exactly when the completed expansion would exceed
// regexpCaseFoldMaxExpandedBytes — the same boundary the deferred length
// check enforced.
func (e *regexpCaseFoldExpander) write(out *strings.Builder, s string) {
	if e.failed {
		return
	}
	if len(s) > e.remain {
		e.fail(regexpCaseFoldResourceLimitf("pattern emission budget of %d bytes", regexpCaseFoldMaxExpandedBytes))
		return
	}
	out.WriteString(s)
	e.remain -= len(s)
}

// writeByte is the single-byte form of write.
func (e *regexpCaseFoldExpander) writeByte(out *strings.Builder, b byte) {
	if e.failed {
		return
	}
	if e.remain < 1 {
		e.fail(regexpCaseFoldResourceLimitf("pattern emission budget of %d bytes", regexpCaseFoldMaxExpandedBytes))
		return
	}
	out.WriteByte(b)
	e.remain--
}

// writeRune is the rune form of write; r is always a valid rune here
// (invalid input aborts the scan before any emission).
func (e *regexpCaseFoldExpander) writeRune(out *strings.Builder, r rune) {
	if e.failed {
		return
	}
	if utf8.RuneLen(r) > e.remain {
		e.fail(regexpCaseFoldResourceLimitf("pattern emission budget of %d bytes", regexpCaseFoldMaxExpandedBytes))
		return
	}
	out.WriteRune(r)
	e.remain -= utf8.RuneLen(r)
}

// chargeWork accounts n bytes of emission-construction work against the
// shared work budget: bytes built while computing a run's expansion,
// including attempts that are later discarded. Exhaustion sets failed, so
// the rewrite aborts at the next check and the caller raises the bounded
// resource-limit error — the same contract the emission budget has.
func (e *regexpCaseFoldExpander) chargeWork(n int) {
	if e.failed {
		return
	}
	if n > e.workRemain {
		e.fail(regexpCaseFoldResourceLimitf("rewrite work budget of %d bytes", regexpCaseFoldMaxWorkBytes))
		return
	}
	e.workRemain -= n
}

// isRegexpFreeSpacingWhitespace reports exactly the character set the
// engines discard as free-spacing trivia under (?x): the regexp2 parser's
// isSpace table (space, tab, LF, VT, FF, CR).
func isRegexpFreeSpacingWhitespace(r rune) bool {
	switch r {
	case ' ', '\t', '\n', '\v', '\f', '\r':
		return true
	}
	return false
}

func (e *regexpCaseFoldExpander) expand(pat string) string {
	var out strings.Builder
	out.Grow(len(pat) + len(pat)/4)
	var run []rune
	flush := func() {
		if len(run) > 0 && !e.failed {
			e.write(&out, e.emitCaseFoldRun(run, e.ci, e.regexp2))
			run = run[:0]
		}
	}
	i := 0
	for i < len(pat) {
		if e.failed {
			// The emission budget is exhausted: abandon the rewrite so the
			// caller falls back to the original pattern.
			return ""
		}
		r, size := utf8.DecodeRuneInString(pat[i:])
		if r == utf8.RuneError && size == 1 {
			return ""
		}
		if e.x {
			// Free-spacing trivia is transparent to the engines: unescaped
			// whitespace and # line comments are discarded before matching.
			// Dropping them here (instead of folding them into a literal
			// run) keeps the emitted alternation free of characters the
			// engine would reinterpret — a folded-in comment would swallow
			// the generated parentheses — and lets the literal pieces
			// around the trivia fold as one run, exactly as ICU folds the
			// comment-stripped run (probed: (?xi)"s s" matches ß).
			if isRegexpFreeSpacingWhitespace(r) {
				i += size
				continue
			}
			if r == '#' {
				for i < len(pat) && pat[i] != '\n' {
					i++
				}
				continue
			}
		}
		if r == '(' && strings.HasPrefix(pat[i:], "(?#") {
			// A (?#...) comment is trivia in every mode (the engines
			// discard it between tokens), so the literal pieces around it
			// fold as one run; the run is deliberately not flushed. An
			// unterminated comment stays verbatim so the engine reports
			// the same error it would for the unrewritten pattern.
			if j := strings.IndexByte(pat[i+3:], ')'); j >= 0 {
				i += 3 + j + 1
				continue
			}
			flush()
			e.write(&out, pat[i:])
			i = len(pat)
			continue
		}
		switch r {
		case '\\':
			if n := scanRegexpNamedReference(pat[i:]); n > 0 {
				// \k<name>, \k'name', the \g<name>, \g'name' and \g{name}
				// forms, and the deprecated \<name> and \'name' forms must
				// keep their exact bytes: the engines resolve reference
				// names case-sensitively, so folding the name would break
				// the reference.
				flush()
				e.write(&out, pat[i:i+n])
				i += n
				continue
			}
			if text, n, ok := decodeRegexpLiteralEscape(pat[i:]); ok {
				// Literal text carried by \Q...\E, \x{..} or \xNN joins the
				// literal stream; regex metacharacters are flushed and
				// re-escaped so they keep their literal meaning. Under
				// free-spacing mode, decoded whitespace and '#' are flushed
				// and re-emitted as \x{..} escapes, which stay literal in
				// every mode; splitting the run at such characters loses no
				// expansion because no fold image contains whitespace or
				// '#'.
				for _, dr := range text {
					switch {
					case dr == '\\' || strings.ContainsRune(`.*+?()|[]{}^$`, dr):
						flush()
						e.write(&out, quoteRegexpLiteral(dr))
					case e.x && (dr == '#' || isRegexpFreeSpacingWhitespace(dr)):
						flush()
						e.write(&out, fmt.Sprintf(`\x{%02X}`, dr))
					default:
						run = append(run, dr)
					}
				}
				i += n
				continue
			}
			if e.ci && e.regexp2 {
				if dr, n, ok := decodeRegexpClassUnicodeEscape(pat[i:]); ok {
					// A \uXXXX or \UXXXXXXXX escape the regexp2 engine decodes
					// into one literal atom: give the decoded character the
					// same per-character fold orbit its literal spelling gets,
					// because the engine's caseless ToLower fiber misses orbit
					// members (ſ for \u0073, σ for \u03C2 — the parser decodes
					// and lowercases the atom while the runner only lowercases
					// the subject). The atom stays a single token: it never
					// joins the surrounding literal run, so a multi-character
					// fold image still does not fold ('ss' against \u00DF
					// stays false, the documented boundary), and a following
					// quantifier binds the completed atom exactly as before.
					// Malformed escapes fall through to the verbatim path so
					// the engine keeps its own error verdict.
					flush()
					var atomEmission string
					if e.x && (dr == '#' || isRegexpFreeSpacingWhitespace(dr)) {
						// Free-spacing discards pattern whitespace and '#'
						// text, so the literal is re-emitted as \x{..}, which
						// stays literal in every mode; these characters have
						// no case orbit to complete.
						atomEmission = fmt.Sprintf(`\x{%02X}`, dr)
					} else {
						atomEmission = emitCaseFoldQuantifiedAtom(dr, e.ci, e.regexp2)
					}
					e.chargeWork(len(atomEmission))
					e.write(&out, atomEmission)
					i += n
					continue
				}
			}
			flush()
			n := skipRegexpEscape(pat[i:])
			e.write(&out, pat[i:i+n])
			i += n
		case '(':
			flush()
			n := e.scanRegexpGroup(pat[i:], &out)
			i += n
		case ')':
			flush()
			e.writeByte(&out, ')')
			e.popScope()
			i += size
		case '[':
			flush()
			n, closed := skipRegexpClass(pat[i:], e.regexp2)
			if closed {
				e.emitCaseFoldClass(&out, pat[i:i+n])
			} else {
				// An unterminated class keeps its exact bytes so both
				// engines report the same unterminated-class error as
				// for the unrewritten pattern (parser.go's
				// ErrUnterminatedBracket). Augmenting it would parse a
				// body whose final escape can be truncated.
				e.write(&out, pat[i:i+n])
			}
			i += n
		case '|', '^', '$', '.':
			flush()
			e.writeRune(&out, r)
			i += size
		case '*', '+', '?':
			n := scanRegexpRepeat(pat[i:])
			e.emitQuantified(&out, &run, pat[i:i+n])
			i += n
		case '{':
			if n := scanRegexpInterval(pat[i:]); n > 0 {
				e.emitQuantified(&out, &run, pat[i:i+n])
				i += n
			} else {
				run = append(run, r)
				i += size
			}
		default:
			run = append(run, r)
			i += size
		}
	}
	flush()
	if e.failed {
		return ""
	}
	return out.String()
}

// emitQuantified splits the last rune of the pending literal run off as the
// quantified atom, emits the remaining run, then the atom with its
// quantifier.
func (e *regexpCaseFoldExpander) emitQuantified(out *strings.Builder, run *[]rune, quantifier string) {
	if len(*run) == 0 {
		// No literal to quantify: copy the quantifier verbatim and let the
		// engine report the malformed pattern as before.
		e.write(out, quantifier)
		return
	}
	atom := (*run)[len(*run)-1]
	*run = (*run)[:len(*run)-1]
	if len(*run) > 0 && !e.failed {
		e.write(out, e.emitCaseFoldRun(*run, e.ci, e.regexp2))
		*run = (*run)[:0]
	}
	atomEmission := emitCaseFoldQuantifiedAtom(atom, e.ci, e.regexp2)
	e.chargeWork(len(atomEmission))
	e.write(out, atomEmission)
	e.write(out, quantifier)
}

// emitCaseFoldQuantifiedAtom emits one quantified literal character. ICU
// matches quantified atoms per character with full-fold caseless semantics:
// the atom accepts every single character whose full fold equals the atom's
// own, but never spans a multi-character fold expansion of the subject. The
// accepted set for a fold-table character is exactly the sources of its
// fold image (ᾈ+ matches its case pair ᾀ, ß+ and ẞ+ each match both ß and
// ẞ), emitted case-sensitively so the engines' differing simple-fold orbits
// cannot leak characters outside that source set. A character with a
// single-rune fold image keeps the engine's caseless orbit; on the regexp2
// route that orbit is the unicode.ToLower closure, which misses the fold
// sources listed in regexpSingleCharFoldExtras (ς full-folds to σ but
// ToLower(ς)=ς), so such an image is emitted as the same explicit class
// the literal-run path uses (ς+ becomes [σς]+).
func emitCaseFoldQuantifiedAtom(atom rune, ci, regexp2 bool) string {
	if !ci {
		return string(atom)
	}
	if image, ok := regexpMultiCharFold[atom]; ok {
		sources := regexpMultiCharFoldSources[image]
		if len(sources) == 1 {
			return "(?-i:" + quoteRegexpLiteral(sources[0]) + ")"
		}
		var b strings.Builder
		b.WriteString("(?-i:(?:")
		for i, src := range sources {
			if i > 0 {
				b.WriteByte('|')
			}
			b.WriteString(quoteRegexpLiteral(src))
		}
		b.WriteString("))")
		return b.String()
	}
	if regexp2 {
		folded := regexpCaseFolder.String(string(atom))
		if utf8.RuneCountInString(folded) == 1 {
			image, _ := utf8.DecodeRuneInString(folded)
			if len(regexpSingleCharFoldExtras[image]) > 0 {
				var b bytes.Buffer
				writeCaseFoldSingleRune(&b, image, true)
				return b.String()
			}
		}
	}
	return quoteRegexpLiteral(atom)
}

// emitCaseFoldRun expands one maximal literal run. ci must be the
// case-insensitive state in effect for the whole run; runs always terminate
// at tokens that could change it. regexp2 selects the engine grammar whose
// caseless orbit the emission must preserve (Go regexp's simple-fold orbit
// is complete; the vendored regexp2 engine compares through unicode.ToLower,
// which does not reach every full-fold source — see
// regexpSingleCharFoldExtras). It returns an empty string when the shared
// work budget is exhausted mid-expansion; the caller then aborts the whole
// rewrite through the failed flag.
func (e *regexpCaseFoldExpander) emitCaseFoldRun(run []rune, ci, regexp2 bool) string {
	if !ci || len(run) == 0 {
		return string(run)
	}
	foldRunes := make([]rune, 0, len(run))
	for _, r := range run {
		if image, ok := regexpMultiCharFold[r]; ok {
			foldRunes = append(foldRunes, []rune(image)...)
			continue
		}
		folded := regexpCaseFolder.String(string(r))
		if utf8.RuneCountInString(folded) != 1 {
			// A multi-rune fold the generated table does not know. The
			// table-consistency test pins this unreachable; if a future
			// table change ever reaches it, failing the rewrite keeps the
			// full-fold promise instead of silently narrowing the match
			// set to the engines' per-character orbit.
			e.fail(moerr.NewInternalErrorNoCtxf("regexp case-fold rewrite: unsupported multi-rune fold for U+%04X", r))
			return ""
		}
		foldedRune, _ := utf8.DecodeRuneInString(folded)
		foldRunes = append(foldRunes, foldedRune)
	}
	foldImage := string(foldRunes)
	if !regexpFoldImagePresent(foldRunes) {
		// No expansion source participates: the fold image compared through
		// the engine's per-character orbit is exactly the run's fold
		// semantics. On the RE2 route that orbit is the complete simple-fold
		// closure, so the common path rewrites nothing except
		// case-normalizing, which the ambient case-insensitive flag makes
		// behavior-identical. On the regexp2 route the orbit is the
		// unicode.ToLower closure, which misses the fold sources listed in
		// regexpSingleCharFoldExtras (ς full-folds to σ but ToLower(ς)=ς):
		// without them the rewritten run would lose even the identical-text
		// self-match (ς against ^ς$ became σ against σ).
		if regexp2 {
			return e.caseFoldOrbitRunString(foldRunes)
		}
		return foldImage
	}
	return e.enumerateCaseFoldRuns(run, foldRunes, regexp2)
}

// caseFoldOrbitRunString emits foldRunes on the regexp2 route, wrapping each
// rune whose full-fold sources exceed the engine's ToLower orbit. Runes
// without extras emit exactly as before, so an image with no listed rune
// stays byte-identical.
func (e *regexpCaseFoldExpander) caseFoldOrbitRunString(foldRunes []rune) string {
	var b bytes.Buffer
	b.Grow(len(foldRunes) * 2)
	for _, r := range foldRunes {
		writeCaseFoldSingleRune(&b, r, true)
	}
	s := b.String()
	e.chargeWork(len(s))
	return s
}

// enumerateCaseFoldRuns emits a pattern that accepts exactly the subject
// strings whose per-character full case fold equals the run's fold image.
// Runs with few enough segmentations are enumerated flat; beyond the
// segmentation cap the exact divide-and-conquer emission factors the
// alternation, which stays exact for any realistic run length. A run whose
// exact emission would exceed regexpCaseFoldMaxRunBytes fails the whole
// rewrite with the bounded resource-limit error: falling back to the
// original run bytes would silently narrow the promised full-fold match
// set to the engines' per-character caseless orbit (issue #31175 design,
// invariant 5). It also returns an empty string when the shared work
// budget is exhausted; the caller then aborts the whole rewrite.
func (e *regexpCaseFoldExpander) enumerateCaseFoldRuns(run, foldRunes []rune, regexp2 bool) string {
	if emissions, ok := e.enumerateCaseFoldSegmentations(foldRunes, regexp2); ok {
		total := 0
		for _, s := range emissions {
			total += len(s)
		}
		if total <= regexpCaseFoldMaxRunBytes {
			return e.joinCaseFoldAlternation(emissions)
		}
	}
	if e.failed {
		// The work budget ran out inside the flat enumeration: abandon the
		// whole rewrite instead of starting the exact emission.
		return ""
	}
	if exact, ok := e.emitExactCaseFoldAlternation(foldRunes, 0, len(foldRunes), regexp2); ok {
		return exact
	}
	if e.failed {
		return ""
	}
	e.fail(regexpCaseFoldResourceLimitf("literal-run emission budget of %d bytes", regexpCaseFoldMaxRunBytes))
	return ""
}

// joinCaseFoldAlternation joins emissions into a (?:a|b) group, charging
// the joined bytes to the shared work budget. A single emission returns
// unchanged. When the join itself exhausts the budget the joined string is
// still returned with failed set; the caller's emission write refuses it,
// so the rewrite aborts as with every other work failure.
func (e *regexpCaseFoldExpander) joinCaseFoldAlternation(emissions []string) string {
	if len(emissions) == 1 {
		return emissions[0]
	}
	var out strings.Builder
	out.WriteString("(?:")
	for i, s := range emissions {
		if i > 0 {
			out.WriteByte('|')
		}
		out.WriteString(s)
	}
	out.WriteByte(')')
	s := out.String()
	e.chargeWork(len(s))
	return s
}

// enumerateCaseFoldSegmentations enumerates segmentations of the fold image
// into single-rune blocks and fold-image blocks. It reports failure when the
// segmentation count or one emission exceeds the run bounds. Every buffer
// write and every materialized emission is charged to the shared work
// budget, including branches the walk later truncates; budget exhaustion
// also reports failure, with the failed flag set so the whole rewrite
// aborts instead of falling through to the exact emission.
func (e *regexpCaseFoldExpander) enumerateCaseFoldSegmentations(foldRunes []rune, regexp2 bool) ([]string, bool) {
	var emissions []string
	seen := make(map[string]bool)
	var walk func(pos int, b *bytes.Buffer) bool
	walk = func(pos int, b *bytes.Buffer) bool {
		if e.failed {
			return false
		}
		if pos == len(foldRunes) {
			s := b.String()
			e.chargeWork(len(s))
			if e.failed {
				return false
			}
			if !seen[s] {
				seen[s] = true
				emissions = append(emissions, s)
			}
			return len(emissions) <= regexpCaseFoldMaxRunAlternatives
		}
		// Single-rune block.
		mark := b.Len()
		writeCaseFoldSingleRune(b, foldRunes[pos], regexp2)
		e.chargeWork(b.Len() - mark)
		if e.failed || b.Len() > regexpCaseFoldMaxRunBytes {
			return false
		}
		if !walk(pos+1, b) {
			return false
		}
		b.Truncate(mark)
		// Fold-image blocks starting here.
		for _, image := range regexpFoldImagesAt(foldRunes, pos) {
			mark := b.Len()
			writeCaseFoldImageBlock(b, image, regexp2)
			e.chargeWork(b.Len() - mark)
			if e.failed || b.Len() > regexpCaseFoldMaxRunBytes {
				return false
			}
			if !walk(pos+utf8.RuneCountInString(image), b) {
				return false
			}
			b.Truncate(mark)
		}
		return true
	}
	var b bytes.Buffer
	if !walk(0, &b) || len(emissions) == 0 || len(emissions) > regexpCaseFoldMaxRunAlternatives {
		return nil, false
	}
	return emissions, true
}

// emitExactCaseFoldAlternation builds the segmentation alternation of
// foldRunes[lo:hi] in factored form: every segmentation either keeps the
// midpoint m=⌊(lo+hi)/2⌋ on a block boundary, which is exactly the
// concatenation of the two half alternations, or has precisely one
// fold-image block straddling m (blocks span at most three runes, so they
// start at m-1 or m-2). Recursing on both forms yields the complete set
// without materializing it, so the emission size grows with the run length
// instead of the segmentation count. It reports failure when the emission
// would exceed regexpCaseFoldMaxRunBytes. Every concatenation is charged to
// the shared work budget, including the candidates a size failure discards;
// budget exhaustion also reports failure, with the failed flag set so the
// whole rewrite aborts.
func (e *regexpCaseFoldExpander) emitExactCaseFoldAlternation(foldRunes []rune, lo, hi int, regexp2 bool) (string, bool) {
	if e.failed {
		return "", false
	}
	if hi-lo <= 7 {
		// A span this short has at most 44 segmentations, so the flat
		// enumeration always succeeds.
		emissions, ok := e.enumerateCaseFoldSegmentations(foldRunes[lo:hi], regexp2)
		if !ok {
			return "", false
		}
		return e.joinCaseFoldAlternation(emissions), true
	}
	m := lo + (hi-lo)/2
	left, ok := e.emitExactCaseFoldAlternation(foldRunes, lo, m, regexp2)
	if !ok {
		return "", false
	}
	right, ok := e.emitExactCaseFoldAlternation(foldRunes, m, hi, regexp2)
	if !ok {
		return "", false
	}
	seen := make(map[string]bool)
	parts := make([]string, 0, 4)
	noStraddle := left + right
	e.chargeWork(len(noStraddle))
	if e.failed {
		return "", false
	}
	parts = append(parts, noStraddle)
	seen[noStraddle] = true
	size := len(noStraddle)
	if size > regexpCaseFoldMaxRunBytes {
		return "", false
	}
	for s := m - 2; s < m; s++ {
		if s < lo {
			continue
		}
		for _, image := range regexpFoldImagesAt(foldRunes, s) {
			end := s + utf8.RuneCountInString(image)
			if end <= m || end > hi {
				continue
			}
			pre, ok := e.emitExactCaseFoldAlternation(foldRunes, lo, s, regexp2)
			if !ok {
				return "", false
			}
			post, ok := e.emitExactCaseFoldAlternation(foldRunes, end, hi, regexp2)
			if !ok {
				return "", false
			}
			cs := pre + regexpCaseFoldImageBlockString(image, regexp2) + post
			e.chargeWork(len(cs))
			if e.failed {
				return "", false
			}
			if seen[cs] {
				continue
			}
			seen[cs] = true
			parts = append(parts, cs)
			size += len(cs)
			if size > regexpCaseFoldMaxRunBytes {
				return "", false
			}
		}
	}
	return e.joinCaseFoldAlternation(parts), true
}

// regexpFoldImagePresent reports whether any known multi-rune fold image
// occurs in foldRunes.
func regexpFoldImagePresent(foldRunes []rune) bool {
	for pos := range foldRunes {
		if len(regexpFoldImagesAt(foldRunes, pos)) > 0 {
			return true
		}
	}
	return false
}

// regexpFoldImagesAt returns every fold image that matches foldRunes
// starting at pos, longest first so segmentations prefer wider blocks.
func regexpFoldImagesAt(foldRunes []rune, pos int) []string {
	images := regexpFoldImagesByLead[foldRunes[pos]]
	if len(images) == 0 {
		return nil
	}
	var matched []string
	for _, image := range images {
		imageRunes := []rune(image)
		if pos+len(imageRunes) > len(foldRunes) {
			continue
		}
		match := true
		for j, r := range imageRunes {
			if foldRunes[pos+j] != r {
				match = false
				break
			}
		}
		if match {
			matched = append(matched, image)
		}
	}
	return matched
}

// regexpFoldImagesByLead indexes fold images by their first rune, longest
// image first.
var regexpFoldImagesByLead = func() map[rune][]string {
	byLead := make(map[rune][]string)
	for image := range regexpMultiCharFoldSources {
		lead, _ := utf8.DecodeRuneInString(image)
		byLead[lead] = append(byLead[lead], image)
	}
	for _, images := range byLead {
		for i := 1; i < len(images); i++ {
			for j := i; j > 0 && len(images[j]) > len(images[j-1]); j-- {
				images[j], images[j-1] = images[j-1], images[j]
			}
		}
	}
	return byLead
}()

// writeCaseFoldSingleRune emits a single-rune fold block. The rune inherits
// the engine's case-insensitive per-character orbit. On the regexp2 route
// that orbit is the unicode.ToLower closure, which does not reach every
// full-fold source (ς full-folds to σ but ToLower(ς)=ς, so σ would not
// match ς — not even the identical text). The sources the orbit misses are
// listed as extra class members from regexpSingleCharFoldExtras: the
// parser's addLowercase replaces each singleton member by its ToLower image
// and the runner lowers the subject before the set check, and every listed
// extra has a singleton ToLower orbit (probed), so [d extras...] accepts
// exactly the orbit of d plus the extras — compact and exact. The RE2 route
// needs nothing: its simple-fold orbit already reaches every source.
func writeCaseFoldSingleRune(b *bytes.Buffer, r rune, regexp2 bool) {
	extras := regexpSingleCharFoldExtras[r]
	if !regexp2 || len(extras) == 0 {
		b.WriteString(quoteRegexpLiteral(r))
		return
	}
	b.WriteByte('[')
	writeRegexpClassLiteral(b, r)
	for _, x := range extras {
		writeRegexpClassLiteral(b, x)
	}
	b.WriteByte(']')
}

// writeRegexpClassLiteral emits one rune as a character-class member,
// escaping the members with class meaning. Every rune the fold tables can
// supply here is a letter or combining mark that needs no escape; the
// escapes keep the helper correct for any rune.
func writeRegexpClassLiteral(b *bytes.Buffer, r rune) {
	switch r {
	case '\\', ']', '[', '^', '-':
		b.WriteByte('\\')
	}
	b.WriteString(string(r))
}

// emitCaseFoldClass writes one character class. On the case-insensitive
// regexp2 route the class is augmented with the per-character fold orbit
// the engine's class closure misses (see augmentRegexpClassCaseFold);
// every other route and every shape the augmentation does not safely
// understand keeps the exact bytes.
func (e *regexpCaseFoldExpander) emitCaseFoldClass(out *strings.Builder, classText string) {
	if !e.ci || !e.regexp2 {
		e.write(out, classText)
		return
	}
	aug, ok := augmentRegexpClassCaseFold(classText)
	if !ok {
		e.write(out, classText)
		return
	}
	if aug != classText {
		e.chargeWork(len(aug))
	}
	e.write(out, aug)
}

// augmentRegexpClassCaseFold returns classText with the fold-orbit members
// added that the vendored regexp2 engine's caseless class closure cannot
// reach, or ok == false when the class shape is one the augmentation does
// not model (the caller then copies the class verbatim).
//
// The engine closes a caseless class through unicode.ToLower
// (parser.go addLowercase): a singleton member s accepts ToLower(s), and a
// range member keeps its bounds plus the lowercase image intervals of its
// lcTable walk. ICU instead closes a class per character by full case
// folding, so [ς] caselessly matches σ and Σ, [a-z] matches ſ, and [ﬅ]
// matches ﬆ (ICU 78.3 probed for issue #31175). The engine reaches most of
// the orbit itself (ToLower(Σ)=σ, ToLower(ẞ)=ß); the members it misses are
// exactly the fold sources whose ToLower differs from the fold image —
// the regexpSingleCharFoldExtras runes and the multi-character fold
// sources. Adding those runes as plain members is semantically exact:
// every added rune x satisfies ToLower(x) == x or has its ToLower partner
// added alongside, so the engine's closure of the augmented class accepts
// exactly the orbit characters (and, redundantly but identically,
// characters its lcTable image intervals already accepted).
//
// Members are parsed with the engine's own grammar (scanCharSet): a ']' in
// first position is literal, ranges are member-'-'-member, and a trailing
// '-' stays literal by inserting the additions before it. Plain runes, the
// hex escapes \x{..}/\xNN and the unicode escapes \uXXXX/\UXXXXXXXX are
// decoded as members; octal/control escapes, categories, POSIX classes
// and \Q...\E stay opaque, and a class whose opaque text sits in range
// position is left entirely verbatim so the engine reports the same
// verdict as for the unrewritten pattern. Malformed escapes (a \u with
// fewer than four hex digits, a \U without eight, a value above
// 0x10FFFF) stay opaque as well, preserving the engine's
// malformed-escape error or literal fallback.
//
// A class containing a subtraction set ('-[' at a non-first position) is
// left entirely verbatim. The vendored engine's caseless subtraction
// algebra is its own per-character unicode.ToLower fiber (round-8 review
// contract, TestRegexpReviewForkFoldSubtraction): orbit completion on
// either side of the subtraction changes it — completing the exclusion
// set would erase raw minuend members ([s-[ſ]] would collapse from {s} to
// empty, reviving the bogus empty-class exemption the multiline
// start-position contract seals), and completing the minuend can revive a
// class the fork keeps dead ([s-[S]] would gain {ſ}). ICU's per-set case
// closure gives different verdicts for these corners (ς does not match
// [α-ω-[σ]] and ſ matches [a-z-[bc]] in ICU 78.3, both probed); those
// stay documented divergences in the vendored engine's favor.
func augmentRegexpClassCaseFold(classText string) (string, bool) {
	if len(classText) < 2 || classText[0] != '[' || classText[len(classText)-1] != ']' {
		return "", false
	}
	hdr := 1
	if hdr < len(classText)-1 && classText[hdr] == '^' {
		hdr++
	}
	body := classText[hdr : len(classText)-1]
	members, insertOff, subOff, ok := parseRegexpClassFoldMembers(body)
	if !ok || subOff >= 0 {
		return "", false
	}
	var addsText bytes.Buffer
	for _, x := range regexpClassFoldOrbitAdds(members) {
		writeRegexpClassLiteral(&addsText, x)
	}
	newBody := body[:insertOff] + addsText.String() + body[insertOff:]
	return classText[:hdr] + newBody + "]", true
}

// regexpClassFoldMember is one parsed character-class member: an opaque
// unit the augmentation does not decode (category and property escapes,
// POSIX classes, non-hex escapes), a singleton rune, or an inclusive rune
// range. Opaque members never fire orbit groups; they keep their bytes.
type regexpClassFoldMember struct {
	lo, hi rune
	opaque bool
}

// regexpClassUnit is one scanned class unit. kind reports how the engine
// treats it: classUnitContinue members (categories, POSIX classes, the \-
// literal) are added directly and skipped over the range logic by the
// engine's continue, classUnitRune is a decodable single character that may
// sit in range position, and classUnitEscape is an escape the engine
// decodes but this augmentation does not (range-eligible in the engine, so
// a class using it as a range endpoint is left verbatim).
type regexpClassUnit struct {
	r       rune
	n       int
	kind    regexpClassUnitKind
	escaped bool
}

type regexpClassUnitKind int

const (
	classUnitRune regexpClassUnitKind = iota
	classUnitContinue
	classUnitEscape
)

// scanRegexpClassUnit measures the class unit starting at body[i].
func scanRegexpClassUnit(body string, i int) regexpClassUnit {
	switch body[i] {
	case '\\':
		n := skipRegexpEscape(body[i:])
		if n == 2 {
			// n == 2 guarantees body[i+1] exists (a lone truncated
			// backslash yields n == 1).
			switch body[i+1] {
			case 'd', 'D', 's', 'S', 'w', 'W', '-':
				return regexpClassUnit{kind: classUnitContinue, n: n, escaped: true}
			}
		}
		if i+1 < len(body) && (body[i+1] == 'p' || body[i+1] == 'P') {
			return regexpClassUnit{kind: classUnitContinue, n: n, escaped: true}
		}
		if r, m, ok := decodeRegexpClassHexEscape(body[i:]); ok && m == n {
			return regexpClassUnit{r: r, n: n, kind: classUnitRune, escaped: true}
		}
		if r, m, ok := decodeRegexpClassUnicodeEscape(body[i:]); ok && m == n {
			return regexpClassUnit{r: r, n: n, kind: classUnitRune, escaped: true}
		}
		return regexpClassUnit{kind: classUnitEscape, n: n, escaped: true}
	case '[':
		if i+1 < len(body) && body[i+1] == ':' {
			if j := strings.Index(body[i+2:], ":]"); j >= 0 {
				return regexpClassUnit{kind: classUnitContinue, n: 2 + j + 2}
			}
		}
		return regexpClassUnit{r: '[', n: 1, kind: classUnitRune}
	default:
		r, size := utf8.DecodeRuneInString(body[i:])
		return regexpClassUnit{r: r, n: size, kind: classUnitRune}
	}
}

// decodeRegexpClassHexEscape decodes the \x{..} and \xNN class escapes —
// the same hex subset decodeRegexpLiteralEscape decodes outside classes —
// returning the rune, the consumed byte count and whether the escape was
// decoded. Undecodable escapes stay opaque for the verbatim path.
func decodeRegexpClassHexEscape(pat string) (rune, int, bool) {
	if len(pat) < 4 || pat[0] != '\\' || pat[1] != 'x' {
		return 0, 0, false
	}
	if pat[2] == '{' {
		i := strings.IndexByte(pat[3:], '}')
		if i < 0 {
			return 0, 0, false
		}
		hexDigits := pat[3 : 3+i]
		if len(hexDigits) == 0 || len(hexDigits) > 6 {
			return 0, 0, false
		}
		v, err := strconv.ParseUint(hexDigits, 16, 32)
		if err != nil {
			return 0, 0, false
		}
		r := rune(v)
		if !utf8.ValidRune(r) {
			return 0, 0, false
		}
		return r, 3 + i + 1, true
	}
	if isRegexpHexDigit(pat[2]) && isRegexpHexDigit(pat[3]) {
		v, _ := strconv.ParseUint(pat[2:4], 16, 32)
		return rune(v), 4, true
	}
	return 0, 0, false
}

// decodeRegexpClassUnicodeEscape decodes the \uXXXX and \UXXXXXXXX class
// escapes, mirroring the engine's own acceptance: the vendored parser's
// scanCharEscape reads \u followed by exactly four hex digits, and
// translateRegexp2CharClass maps a well-formed \UXXXXXXXX token to
// \x{........}, whose scanHexUntilBrace rejects values above 0x10FFFF.
// Decoded members join the fold-orbit computation exactly like literal
// and \x-decoded members, so the case equivalence of a class no longer
// depends on the escape spelling (ICU 78.3 probed: [\u03C2] caselessly
// matches σ and Σ, [^\u03C2] excludes them, and [\u0041-\u005A] matches
// ſ). A token outside those shapes (\u with fewer than four hex digits,
// \U without eight, a value above 0x10FFFF, a surrogate) stays opaque:
// the engine then reports its malformed-escape error — or reads \U as a
// literal 'U' — and the verbatim bytes preserve that verdict.
func decodeRegexpClassUnicodeEscape(pat string) (rune, int, bool) {
	if len(pat) < 2 || pat[0] != '\\' {
		return 0, 0, false
	}
	width := 0
	switch pat[1] {
	case 'u':
		width = 6
	case 'U':
		width = 10
	}
	if width == 0 || len(pat) < width {
		return 0, 0, false
	}
	for i := 2; i < width; i++ {
		if !isRegexpHexDigit(pat[i]) {
			return 0, 0, false
		}
	}
	v, err := strconv.ParseUint(pat[2:width], 16, 32)
	if err != nil {
		return 0, 0, false
	}
	r := rune(v)
	if !utf8.ValidRune(r) {
		return 0, 0, false
	}
	return r, width, true
}

// parseRegexpClassFoldMembers parses the members of a class body (the text
// between the '[' header — including any '^' — and the closing ']'),
// mirroring the vendored parser's scanCharSet member boundaries. It
// returns the members, the body offset at which orbit additions are
// inserted (before a subtraction or a trailing literal '-', else at the
// end), the body offset of the subtraction's '-' or -1, and ok == false
// when an opaque unit sits in range position (the engine decodes or
// rejects text the augmentation cannot model, so the class keeps its
// verdict verbatim).
func parseRegexpClassFoldMembers(body string) (members []regexpClassFoldMember, insertOff, subOff int, ok bool) {
	subOff = -1
	first := true
	lastDashOff := -1
	i := 0
scan:
	for i < len(body) {
		// A '-' opening a subtraction: current position, non-first, and
		// followed by '[' (scanCharSet's not-in-range subtraction path).
		if body[i] == '-' && !first && i+1 < len(body) && body[i+1] == '[' {
			subOff = i
			break
		}
		u := scanRegexpClassUnit(body, i)
		switch u.kind {
		case classUnitContinue:
			// Categories, POSIX classes and the \- literal are added
			// directly and skip the range logic (the engine's continue).
			members = append(members, regexpClassFoldMember{opaque: true})
			lastDashOff = -1
			i += u.n
		case classUnitEscape:
			next := i + u.n
			if next < len(body) && body[next] == '-' && next+1 < len(body) && body[next+1] != ']' {
				// An opaque escape in range position: the engine decodes
				// the endpoint, the augmentation cannot.
				return nil, 0, 0, false
			}
			members = append(members, regexpClassFoldMember{opaque: true})
			lastDashOff = -1
			i = next
		default:
			next := i + u.n
			if next < len(body) && body[next] == '-' && next+1 < len(body) && body[next+1] != ']' {
				if body[next+1] == '[' {
					// A range candidate that is really a subtraction:
					// the rune stays a singleton member (scanCharSet).
					members = append(members, regexpClassFoldMember{lo: u.r, hi: u.r})
					subOff = next
					break scan
				}
				hi := scanRegexpClassUnit(body, next+1)
				if hi.kind != classUnitRune {
					// A range with an opaque or category endpoint: the
					// engine decodes the escape or rejects the category.
					return nil, 0, 0, false
				}
				if u.r <= hi.r {
					members = append(members, regexpClassFoldMember{lo: u.r, hi: hi.r})
				}
				// A reversed range keeps its bytes; the engine rejects
				// it exactly as for the unrewritten pattern.
				lastDashOff = -1
				i = next + 1 + hi.n
			} else {
				members = append(members, regexpClassFoldMember{lo: u.r, hi: u.r})
				if u.r == '-' && !u.escaped {
					lastDashOff = i
				} else {
					lastDashOff = -1
				}
				i = next
			}
		}
		first = false
	}
	insertOff = len(body)
	switch {
	case subOff >= 0:
		insertOff = subOff
	case lastDashOff >= 0:
		// A trailing literal '-' must stay trailing: inserting after it
		// would turn it into a range operator.
		insertOff = lastDashOff
	}
	return members, insertOff, subOff, true
}

// regexpClassFoldOrbitAdds computes the fold-orbit runes a class is
// missing. Each orbit group fires when one of its runes is a raw member of
// the class (a singleton or inside a range); every group rune the class
// does not already accept is added. A member already accepts x when
// ToLower(x) equals a singleton's ToLower image, lies inside a range, or
// equals the ToLower image of an addition made so far — the engine's own
// acceptance formula after its ToLower class closure. Members are
// evaluated hubs first (runes with ToLower(x) == x) so the lowercase hub
// of a group is added before its case variants are checked against it.
// The result is sorted and deduplicated for deterministic emission.
func regexpClassFoldOrbitAdds(members []regexpClassFoldMember) []rune {
	var adds []rune
	for _, group := range regexpClassFoldOrbitGroups {
		fired := false
		for _, g := range group {
			if regexpClassFoldRawMember(g, members) {
				fired = true
				break
			}
		}
		if !fired {
			continue
		}
		for pass := 0; pass < 2; pass++ {
			for _, x := range group {
				if (unicode.ToLower(x) == x) != (pass == 0) {
					continue
				}
				if regexpClassFoldRawMember(x, members) {
					continue
				}
				if regexpClassFoldCovered(x, members, adds) {
					continue
				}
				adds = append(adds, x)
			}
		}
	}
	if len(adds) == 0 {
		return nil
	}
	sortRunes(adds)
	return dedupeSortedFoldRunes(adds)
}

// regexpClassFoldRawMember reports whether x is literally a member of the
// class: equal to a singleton or inside a range.
func regexpClassFoldRawMember(x rune, members []regexpClassFoldMember) bool {
	for _, m := range members {
		if !m.opaque && m.lo <= x && x <= m.hi {
			return true
		}
	}
	return false
}

// regexpClassFoldCovered reports whether the engine's caseless closure of
// the class (with the additions made so far) already accepts x: the
// engine lowers the subject before the set check, so x is accepted when
// ToLower(x) equals a singleton member's ToLower image or lies inside a
// range member, or when it equals an added rune's ToLower image.
func regexpClassFoldCovered(x rune, members []regexpClassFoldMember, adds []rune) bool {
	tl := unicode.ToLower(x)
	for _, m := range members {
		if m.opaque {
			continue
		}
		if m.lo == m.hi {
			if unicode.ToLower(m.lo) == tl {
				return true
			}
		} else if m.lo <= tl && tl <= m.hi {
			return true
		}
	}
	for _, a := range adds {
		if unicode.ToLower(a) == tl {
			return true
		}
	}
	return false
}

// simpleFoldCycle returns the simple-fold orbit of r: r followed by
// repeated unicode.SimpleFold until the cycle closes.
func simpleFoldCycle(r rune) []rune {
	cycle := []rune{r}
	for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
		cycle = append(cycle, f)
	}
	return cycle
}

// regexpClassFoldOrbitGroups lists the per-character full-fold orbit
// groups the regexp2 engine's ToLower class closure can miss: for every
// single-rune fold image in regexpSingleCharFoldExtras the image's
// simple-fold cycle plus the extras themselves, and for every
// multi-character fold image its source set (ﬅ and ﬆ share the "st" image
// but ToLower reaches neither from the other). Groups are built once in a
// deterministic order (sorted fold images, sorted members) so emission
// does not depend on map iteration.
var regexpClassFoldOrbitGroups = func() [][]rune {
	var groups [][]rune
	seen := make(map[rune]bool)
	keys := make([]rune, 0, len(regexpSingleCharFoldExtras))
	for d := range regexpSingleCharFoldExtras {
		keys = append(keys, d)
	}
	sortRunes(keys)
	for _, d := range keys {
		set := make(map[rune]bool)
		for _, r := range simpleFoldCycle(d) {
			set[r] = true
		}
		for _, x := range regexpSingleCharFoldExtras[d] {
			set[x] = true
		}
		group := make([]rune, 0, len(set))
		for r := range set {
			group = append(group, r)
		}
		sortRunes(group)
		if len(group) > 1 && !seen[group[0]] {
			seen[group[0]] = true
			groups = append(groups, group)
		}
	}
	images := make([]string, 0, len(regexpMultiCharFoldSources))
	for image := range regexpMultiCharFoldSources {
		images = append(images, image)
	}
	sort.Strings(images)
	for _, image := range images {
		sources := regexpMultiCharFoldSources[image]
		if len(sources) > 1 && !seen[sources[0]] {
			seen[sources[0]] = true
			groups = append(groups, sources)
		}
	}
	return groups
}()

// dedupeSortedFoldRunes removes adjacent duplicates from a sorted slice.
func dedupeSortedFoldRunes(rs []rune) []rune {
	out := rs[:1]
	for _, r := range rs[1:] {
		if r != out[len(out)-1] {
			out = append(out, r)
		}
	}
	return out
}

// writeCaseFoldImageBlock emits (?:image|(?-i:source)...) for one fold
// image. The image branch inherits the ambient case-insensitive orbit; the
// source branches stay case-sensitive so engine-specific simple-fold
// partners of a source do not leak in characters the full-fold source set
// does not list. On the regexp2 route each image rune also carries its
// ToLower-orbit extras (ἀι must accept ἀ followed by U+0345 or U+1FBE, both
// full-fold sources of ι that ToLower misses).
func writeCaseFoldImageBlock(b *bytes.Buffer, image string, regexp2 bool) {
	b.WriteString("(?:")
	for _, r := range image {
		writeCaseFoldSingleRune(b, r, regexp2)
	}
	for _, src := range regexpMultiCharFoldSources[image] {
		b.WriteString("|(?-i:")
		writeRegexpLiteralString(b, string(src))
		b.WriteByte(')')
	}
	b.WriteByte(')')
}

// regexpCaseFoldImageBlockString is writeCaseFoldImageBlock as a string.
func regexpCaseFoldImageBlockString(image string, regexp2 bool) string {
	var b bytes.Buffer
	writeCaseFoldImageBlock(&b, image, regexp2)
	return b.String()
}

// quoteRegexpLiteral escapes one rune for use as a literal in a pattern.
func quoteRegexpLiteral(r rune) string {
	if strings.ContainsRune(`\.*+?()|[]{}^$`, r) {
		return "\\" + string(r)
	}
	return string(r)
}

// scanRegexpNamedReference returns the byte length of the named
// backreference starting at pat[0] == '\\', or 0 when pat does not start
// one. The \k<name> and \k'name' forms are recognized together with the
// \g<name>, \g'name' and \g{name} forms and the deprecated \<name> and
// \'name' forms, matching the escape-token recognition the engines share
// (regexpEscapeTokenLen; the \g forms reach regexp2 through the
// translator's \k rewrite). An unterminated reference consumes the rest of
// the pattern verbatim; the engine reports the malformation exactly as it
// would for the unrewritten pattern.
func scanRegexpNamedReference(pat string) int {
	if len(pat) < 2 {
		return 0
	}
	var start int
	var close byte
	switch pat[1] {
	case 'k':
		if len(pat) < 3 || (pat[2] != '<' && pat[2] != '\'') {
			return 0
		}
		start = 3
		close = pat[2]
		if close == '<' {
			close = '>'
		}
	case 'g':
		if len(pat) < 3 || (pat[2] != '<' && pat[2] != '\'' && pat[2] != '{') {
			return 0
		}
		start = 3
		close = pat[2]
		switch close {
		case '<':
			close = '>'
		case '{':
			close = '}'
		}
	case '<':
		start, close = 2, '>'
	case '\'':
		start, close = 2, '\''
	default:
		return 0
	}
	if i := strings.IndexByte(pat[start:], close); i >= 0 {
		return start + i + 1
	}
	return len(pat)
}

// decodeRegexpLiteralEscape decodes an escape that carries plain literal
// text — \Q...\E (terminated or running to the pattern's end), \x{...} and
// \xNN — into the literal it represents, returning the decoded text, the
// consumed byte count and whether the escape was decoded. Undecodable
// escapes (invalid hex, surrogate or out-of-range code points) are left for
// the verbatim path so the engine sees the original bytes. An empty quoted
// segment (\Q\E, or \Q at the pattern's end) decodes to empty text: the
// engines treat it as no token at all, so the literal pieces around it fold
// as one run and a following quantifier binds across it, exactly as if the
// segment were absent. Decoded whitespace and '#' are returned like any
// other text: the expander joins them into the literal run outside
// free-spacing mode and re-emits them as \x{..} escapes inside it, so quoted
// text full-folds like any other literal in every mode.
func decodeRegexpLiteralEscape(pat string) (string, int, bool) {
	if len(pat) < 2 {
		return "", 0, false
	}
	switch pat[1] {
	case 'Q':
		body := pat[2:]
		var content string
		var n int
		if i := strings.Index(body, `\E`); i >= 0 {
			content = body[:i]
			n = 2 + i + 2
		} else {
			content = body
			n = len(pat)
		}
		return content, n, true
	case 'x':
		if len(pat) > 2 && pat[2] == '{' {
			i := strings.IndexByte(pat[2:], '}')
			if i < 0 {
				return "", 0, false
			}
			hexDigits := pat[3 : 2+i]
			if len(hexDigits) == 0 || len(hexDigits) > 6 {
				return "", 0, false
			}
			v, err := strconv.ParseUint(hexDigits, 16, 32)
			if err != nil {
				return "", 0, false
			}
			r := rune(v)
			if !utf8.ValidRune(r) {
				return "", 0, false
			}
			return string(r), 2 + i + 1, true
		}
		if len(pat) >= 4 && isRegexpHexDigit(pat[2]) && isRegexpHexDigit(pat[3]) {
			v, _ := strconv.ParseUint(pat[2:4], 16, 32)
			return string(rune(v)), 4, true
		}
		return "", 0, false
	}
	return "", 0, false
}

func isRegexpHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func writeRegexpLiteralString(b *bytes.Buffer, s string) {
	for _, r := range s {
		b.WriteString(quoteRegexpLiteral(r))
	}
}

// scanRegexpGroup copies a group opener into out and updates the
// case-insensitive and free-spacing states. It returns the consumed byte
// count; an unterminated construct copies the '(' alone and lets the engine
// report it. ((?#...) comments never reach this scanner: the main loop
// drops them as trivia before the group dispatch.)
func (e *regexpCaseFoldExpander) scanRegexpGroup(pat string, out *strings.Builder) int {
	if len(pat) < 2 || pat[1] != '?' {
		e.writeByte(out, '(')
		e.pushScope(e.ci, e.x)
		return 1
	}
	// (?P<name>...), (?<name>...), (?'name'...) and lookarounds carry no
	// case flags; the name must be copied verbatim so its case is preserved.
	named := strings.HasPrefix(pat, "(?P<") || strings.HasPrefix(pat, "(?'") ||
		(strings.HasPrefix(pat, "(?<") && !strings.HasPrefix(pat, "(?<=") && !strings.HasPrefix(pat, "(?<!"))
	if named || strings.HasPrefix(pat, "(?=") || strings.HasPrefix(pat, "(?!") ||
		strings.HasPrefix(pat, "(?<=") || strings.HasPrefix(pat, "(?<!") {
		end := 3
		switch {
		case strings.HasPrefix(pat, "(?<="), strings.HasPrefix(pat, "(?<!"):
			end = 4
		case strings.HasPrefix(pat, "(?'"):
			if i := strings.IndexByte(pat[3:], '\''); i >= 0 {
				end = 3 + i + 1
			} else {
				end = len(pat)
			}
		case named:
			if i := strings.IndexByte(pat, '>'); i >= 0 {
				end = i + 1
			} else {
				end = len(pat)
			}
		}
		e.write(out, pat[:end])
		e.pushScope(e.ci, e.x)
		return end
	}
	// (?>...) atomic groups carry no flags and no name; the '>' is part of
	// the group header, not literal text. Copy the complete opener so a
	// foldable literal run starting the body cannot absorb the '>' into a
	// spurious (?flags) header ((?>ss) must not become a conditional whose
	// empty branches match every subject). The body opens a plain scope and
	// stays atomic because the header bytes reach the engine intact.
	if strings.HasPrefix(pat, "(?>") {
		e.write(out, "(?>")
		e.pushScope(e.ci, e.x)
		return 3
	}
	// (?flags) or (?flags:...): flags are ASCII letters with '-' and '+'
	// transitions. Only the case and free-spacing letters change this
	// rewriter's state; the other letters are consumed so combined headers
	// such as (?ix:...) stay synchronized. The regexp2 parser's
	// optionFromCode accepts the uppercase mates 'I' and 'X' (and scanOptions
	// accepts '+' to re-enable), so they are tracked exactly like the
	// lowercase forms — a header the engine honors must not leave this
	// rewriter with a stale mode.
	j := 2
	negated := false
	ci := e.ci
	x := e.x
	for j < len(pat) {
		c := pat[j]
		switch {
		case c == '-':
			negated = true
		case c == '+':
			negated = false
		case c == 'i' || c == 'I':
			ci = !negated
		case c == 'x' || c == 'X':
			x = !negated
		case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
		case c == ')':
			e.write(out, pat[:j+1])
			e.ci = ci
			e.x = x
			return j + 1
		case c == ':':
			e.write(out, pat[:j+1])
			e.pushScope(ci, x)
			return j + 1
		default:
			e.write(out, pat[:j])
			e.pushScope(e.ci, e.x)
			return j
		}
		j++
	}
	e.write(out, "(?")
	e.pushScope(e.ci, e.x)
	return 2
}

// skipRegexpEscape returns the byte length of the escape sequence starting
// at pat[0] == '\\'. It is used for escapes that are neither named
// references nor decodable literal text; those keep their exact bytes.
func skipRegexpEscape(pat string) int {
	if len(pat) < 2 {
		return len(pat)
	}
	switch pat[1] {
	case 'Q':
		if i := strings.Index(pat[2:], `\E`); i >= 0 {
			return 2 + i + 2
		}
		return len(pat)
	case 'x':
		if len(pat) > 2 && pat[2] == '{' {
			if i := strings.IndexByte(pat[2:], '}'); i >= 0 {
				return 2 + i + 1
			}
			return len(pat)
		}
		if len(pat) >= 4 {
			return 4
		}
		return len(pat)
	case 'p', 'P':
		if len(pat) > 2 && pat[2] == '{' {
			if i := strings.IndexByte(pat[2:], '}'); i >= 0 {
				return 2 + i + 1
			}
			return len(pat)
		}
		// Short form \pX / \PX: both engines accept a one-character
		// property name (Go regexp's \pN and the vendored parser's
		// parseProperty), so the letter belongs to the escape token.
		// Folding it as literal text would corrupt the property.
		if len(pat) > 2 {
			_, size := utf8.DecodeRuneInString(pat[2:])
			return 2 + size
		}
		return len(pat)
	case 'u', 'U', 'c', '0':
		// \uXXXX, \UXXXXXXXX, \cX and octal escapes carry a payload that
		// must stay attached to the escape: folding it as literal text
		// would corrupt the escape — a hex payload containing FF would
		// sprout the ff-ligature alternation, a control designator would
		// fold into the run. The payload is not decoded (the engines give
		// these escapes their own meaning), it is only kept verbatim.
		// regexpEscapeTokenLen mirrors the regexp2 parser's own token
		// boundaries (scanCharEscape and scanOctal).
		return regexpEscapeTokenLen(pat, 0)
	case '1', '2', '3', '4', '5', '6', '7', '8', '9':
		// A nonzero digit starts a decimal escape: the regexp2 parser's
		// scanDecimal consumes the complete digit run before choosing
		// between a backreference and the octal/literal fallback
		// (scanBasicBackslash), so \18 names group 18 — not group 1 with a
		// literal '8'. The whole run must stay attached to the escape:
		// folding the trailing digits as literal text would split one
		// engine token in two. regexpEscapeTokenLen cannot decide this
		// because its octal-first branch stops before an '8' or '9';
		// digits never case-fold, so the maximal run loses no expansion.
		i := 2
		for i < len(pat) && pat[i] >= '0' && pat[i] <= '9' {
			i++
		}
		return i
	}
	_, size := utf8.DecodeRuneInString(pat[1:])
	return 1 + size
}

// skipRegexpClass returns the byte length of the character class starting at
// pat[0] == '[' and whether the scan found a genuine closing ']'. An
// unterminated class returns the remaining input length with closed false
// (an escaped ']' is a member, not a delimiter); the caller then keeps the
// exact bytes so the engines report the class error themselves. The
// regexp2 grammar nests classes for subtraction: a '-[' at a non-first
// position makes the parser scan the nested set recursively and then
// require the outer ']' (scanCharSet), so on that route the nested set is
// consumed before the outer class can close. Go regexp has no nesting —
// its first unescaped ']' closes the class — which is what the scanner
// does with regexp2 false.
func skipRegexpClass(pat string, regexp2 bool) (int, bool) {
	i := 1
	if i < len(pat) && pat[i] == '^' {
		i++
	}
	first := true
	if i < len(pat) && pat[i] == ']' {
		// A ']' in first position is a literal member in both grammars
		// (non-ECMAScript mode) and cannot close the class.
		i++
		first = false
	}
	for i < len(pat) {
		switch pat[i] {
		case '\\':
			i += skipRegexpEscape(pat[i:])
		case ']':
			return i + 1, true
		case '[':
			if i+1 < len(pat) && pat[i+1] == ':' {
				if j := strings.Index(pat[i+2:], ":]"); j >= 0 {
					i += 2 + j + 2
					break
				}
			}
			i++
		case '-':
			if regexp2 && !first && i+1 < len(pat) && pat[i+1] == '[' {
				// Subtraction: the '-' and the nested set are one
				// construct, so the nested ']' must not close the outer
				// class. The recursion also covers nested subtraction
				// ([a-z-[b-c-[d]]]).
				n, _ := skipRegexpClass(pat[i+1:], regexp2)
				i += 1 + n
			} else {
				i++
			}
		default:
			i++
		}
		first = false
	}
	return len(pat), false
}

// scanRegexpRepeat returns the byte length of a quantifier starting with
// '*', '+' or '?', including a lazy or possessive suffix.
func scanRegexpRepeat(pat string) int {
	n := 1
	if n < len(pat) && (pat[n] == '?' || pat[n] == '+') {
		n++
	}
	return n
}

// scanRegexpInterval returns the byte length of a {n}, {n,} or {n,m}
// quantifier at pat[0] == '{', including a lazy or possessive suffix, or 0
// when pat does not start a valid interval (the '{' is then a literal).
func scanRegexpInterval(pat string) int {
	i := 1
	for i < len(pat) && pat[i] >= '0' && pat[i] <= '9' {
		i++
	}
	if i == 1 {
		return 0
	}
	if i < len(pat) && pat[i] == ',' {
		i++
		for i < len(pat) && pat[i] >= '0' && pat[i] <= '9' {
			i++
		}
	}
	if i >= len(pat) || pat[i] != '}' {
		return 0
	}
	i++
	if i < len(pat) && (pat[i] == '?' || pat[i] == '+') {
		i++
	}
	return i
}
