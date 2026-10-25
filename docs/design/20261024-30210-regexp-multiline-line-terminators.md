# REGEXP Multiline Wide Line Terminators (MySQL Compatibility)

Status: Design Rev 11, 2026-10-26; design review PASSED. The distinct
design-stage review of Rev 10 (sha256
5dcab7b28d863dbec65e003b5db3503fdbc3ee1c1374d4622e9ae737bf3315b4) closed
with decision PASS on 2026-10-25 under the independent local design
reviewer; the complete decision chain is recorded in the Design review
record section below. Rev 11 is a status-record-only revision: outside
this Status paragraph, the new Design review record section and the
Rev 11 change-log entry, it is identical to Rev 10, so every approved
invariant, decision, rejected alternative and validation plan item is
unchanged and the PASS applies to the implemented revision. Rev 1 (sha256
67bf2ac81e3dd4cce57dc602e429857adc207ab4501a1bd06262e54b8cdd4ed9), Rev 2
(sha256 6e81eb6a9fb6ed0b54877c7bbafeae73fc1164053a887b08488163eace2f9865),
Rev 3 (sha256 c72a777d0ef29f97698b6c91f7e95d61839be39900ac2929030a44964d58771d),
Rev 4 (sha256 27ac7f565475136bd420e326fa0fcd1b2757adddd704674be7499815d6087555),
Rev 5 (sha256 356e7a551e44384ad1dfa11187f374b4b299b81d0b5185769653be7d4d3ecbfe),
Rev 6 (sha256 ecd1994ce9dbf4d33c045349cb5b46866fbe44b3b586218b02ee807342adc97b),
Rev 7 (sha256 c9dcd248b54203856b592edac1044c428cffe76a00d200dc9e7addda003afbe2),
Rev 8 (sha256 c293bc6b93114b6939a8341e4e8bac6bb8896dfc58cea71c71f49f8ec763ddc9)
and Rev 9 (sha256 0b4241c39a43daf99b7422a4690e48c3aa97d2ab7ae5ff84b02fd721e216486f)
each received design-stage REQUEST_CHANGES; all findings are verified below
and addressed.
Earlier rounds (condensed): Rev 2 split the CRLF truth table per assertion
(caret holds between CR and LF mid-match; only dollar excludes that interior)
and removed binary byte 0x85 as a NEL terminator (MySQL presents binary
operands to ICU as CP-1252, so 0x85 is U+2026). Rev 3 added the
search-candidate contract (the ICU START_LINE find loop never offers the CRLF
interior as a match BEGIN, so caret-initial `^\n` does not match there even
though the mid-match caret opcode accepts it) and measured that a regexp2
pattern rewrite inflates TrackCount (7 -> 23), turning admitted workloads
into errors. Rev 4 replaced the leading-token rule with ICU's STRUCTURAL
start analysis (matchStartType), proved the fixed 24x admission multiplier
unprovable (anchor count is unbounded by TrackCount: `(?m)` + 32 literal
`^$` pairs + `(?=\n)` still has TrackCount 7), and replaced the regexp2
pattern rewrite with a native line mode in a vendored regexp2, making
admission preservation an identity by construction.

Rev 5 changed the base under this design: the review-round-5 report records
PR #30646 MERGED (merge commit c3e962df0a7efd7202ab9cbc37ce739f7c8a883a;
base of this revision is origin/main 6fb6531432aaec8c1fbd98a30364941559e0e1d6,
which contains it). The merged delivery implements an earlier form of this
fix: the private RE2 engine `pkg/sql/plan/function/internal/regexp` with a
wide line mode, `getPureMatchType` keeping `u`, a keyed matcher cache,
original-context offset search, VT/FF absent from every classifier, binary
byte 0x85 classified as a terminator in both binary modes, the caret
excluding the CR|LF interior, no search-candidate structural analysis, and
the regexp2 path repaired by the anchor rewrite
(`rewriteRegexpWideMultilineAnchors`, func_builtin_regexp.go:3146-3244)
whose admission regression this design measured in production (below).
The design therefore amends the merged implementation in place; every
retained merged decision is explicitly listed and every amended one
carries new evidence. The three Rev 5 design-review findings
(singleton-class literalChar lowering; pre-optimization classification
capture; node-type lookaround/atomic/backreference rules) were verified
against primary sources and live ICU measurement and addressed in Rev 6;
their full narratives and evidence pins are preserved in the Rev 6
change-log entry and remain part of this design. The three Rev 6
design-review findings are verified against primary sources and live ICU
77.1 measurement before acting (see the Rev 7 change-log entry):

(F1) Capture regexp2 structure before its eager parser reductions. Rev 6
derived the regexp2 descriptor by walking a pre-Reduce RegexNode tree
during code construction. The pinned fork performs reduction EAGERLY at
child insertion, long before code construction: regexNode.addChild calls
child.reduce() and stores the reduced node (fork syntax/tree.go:168-173),
and the parser routes every atom, quantified unit, group body and
alternation through addChild while parsing (fork syntax/parser.go:
addConcatenate ~2007-2013, addConcatenate3 wrapping the unit with
makeQuantifier ~2016-2021, addGroup ~2051-2066). Noncapturing-group
erasure, literal merging and singleton-set lowering are therefore
complete before syntax.Compile and syntax.Write ever see the tree: no
tree walk at code-construction time can distinguish `^\nx` from
`^(?:\n)x` (probe pair S06/S13), exactly as the review states. Rev 7
captures the structure at the point where it still exists: a PARSER-EVENT
recorder inside the vendored parser observes atoms with their fold state,
set emission with singleton information, quantifier min/max application,
group open/close, alternation branches and anchors as parse events,
BEFORE the eager reductions consume them. The recorded event stream is
retained as immutable metadata alongside the compiled pattern and feeds
the shared classification core below; parsing, reduction, codegen and
every admission input are untouched (invariant 9 holds by construction).
(Rev 8 supersedes this capture point for a different reason: the SQL
translation destroys quantifier flavor before any in-parser hook - see
the round-7 finding F1 narrative and alternative 2g. The
capture-before-reduction requirement itself stands and is met one stage
earlier, at the original pattern text.)

(F2) Derive string exemption from all possible initial paths. ICU's
matchStartType (icu-77.1-regexcmp.cpp:2691-3188) walks the compiled ops
carrying currentLen = the MINIMUM match length over every path reaching
the current op: forward jumps and state saves propagate the current
minimum to their targets (JMP/STATE_SAVE cases, 2954-3006), and an op
contributes to the start classification only when some minimum-length-0
path reaches it (currentLen == 0). numInitialStrings is incremented by 1
ONLY for a case-sensitive literal string (URX_STRING case, 3007-3035,
which also records fInitialStringIdx/fInitialStringLen); every other
initial content op - single chars, sets of any kind, case-insensitive
strings, property classes, any-char, backreferences - adds 2 (ONECHAR
2770-2778, SETREF 2780-2790, ONECHAR_I 2902-2922, STRING_I 3020-3043;
BACKREF is zero-width for this walk, 2747-2748). The ladder (3155-3188)
selects START_STRING only when numInitialStrings == 1 AND minMatchLen >
0. Live probes on the installed ICU 77.1 (icu-rev7-probe.log, exit 0)
pin the branch semantics the Rev 6 first-literal-run rule could not
express: `^(?:\nx|y)` FALSE on `a\r\nxb` (row R1 - the alternation's
second branch contributes initial content, numInitialStrings = 3),
`^\nx(?:|y)` TRUE at [2,4) (R2 - the alternation sits after the
completed string, so it is not initial), `^(?:\nx|\nx)` FALSE (D4 - two
initial strings, even identical), `^(?:\nxy|\nxz)` FALSE (D5),
`^\nx(?:y|z)` TRUE at [2,5) (D10), and `^(?:\nxy|)` TRUE only as the
EMPTY match [0,0) (D13 - one initial string but minMatchLen == 0, so the
ladder's minMatchLen > 0 condition blocks the exemption). The Rev 6 rule
(first maximal contiguous run, prefix-only early termination) exempts R1
and cannot express D13; both falsifications are measured before acting.

(F3) Account for quantifier lowering in structural classification.
compileInlineInterval (icu-77.1-regexcmp.cpp:2506-2560) deletes a
zero-upper-count unit AFTER the quantifier machinery has already split
the pending literal run, deletes a whole zero-count block together with
its capture bookkeeping, and inlines small single-op intervals as min
mandatory copies plus (max-min) state-save-guarded optional copies, while
compileInterval (2441-2504) emits CTR_INIT/CTR_LOOP for larger or complex
bodies. fixLiterals(true) (1898-1922; called from blockTopLoc 2139-2169
at every quantifier) SPLITS the pending literal run so the quantifier
binds to the last code point only, and matchStartType's CTR_INIT case
(3046-3070) forwards the current minimum past a min==0 loop while the
loop body is still walked linearly once. Live probes pin every
consequence: `x{0}^\n` FALSE on `a\r\nb` (R3 - zero-count deletion
leaves the caret atStart), `^x{0}y` TRUE at [3,4) on `a\r\nyb` and
FALSE on `a\r\nb` (R3b/R3c), `^(?:\nx)+` TRUE at [2,4) (R4) and
`^(?:\nx){2,3}` TRUE at [2,6) (D7 - complex min>0 loop bodies count
once and CAN seed the exemption), `^\nx{1}` FALSE (D1), `^\nx?` FALSE
(D15) and `^\nx+` FALSE (D16) - any quantifier on the run's last char
splits it off the string, even {1} or ? - `^\nx{0}y` FALSE (D2 - the
split precedes the deletion), `^(?:\nx){1}` TRUE at [2,4) (D11 - {1} on
a group is identity), `^\n{2}x` FALSE versus `^\n\nx` TRUE at [2,5) on
the same subject `a\r\n\nxb` (D8/D9 - inlined single-char copies never
form a string; a textually adjacent run does), `^(?:\nx)*` TRUE only as
the empty match [0,0) (D6/D6-mu - minMatchLen == 0 blocks the string),
and `x{0,2}^\n` TRUE at [2,3) (D12/D12b; FALSE under mu, D12-mu - an
optional-count prefix is control-flow, so the caret is a bare mid-pattern
one matching at the interior). The Rev 6 rules (quantifiers are
unconditional atStart destroyers/run flushers, and quantified runs never
exempt) are falsified in both directions: D1/D15/D16 show the split
destroys exemption that a flat run model would keep, and R4/D7/D11 show
min>0 looped runs exempt that the blanket rule would deny.

The two Rev 7 design-review findings are likewise verified against
primary sources and live ICU 77.1 measurement before acting (see the
Rev 8 change-log entry):

(F1, round 7) Preserve quantifier flavor in classification lowering.
Rev 7 recorded quantifiers as bare {min,max} and deleted zero-count
units universally, stating lazy forms classify identically. ICU does
not: the dispatch at icu-77.1-regexcmp.cpp:1047-1092 tries inline
elimination only for ORDINARY intervals (doInterval: compileInlineInterval
first, compileInterval as fallback), while LAZY intervals ALWAYS compile
a counted loop (doNGInterval: compileInterval(URX_CTR_INIT_NG,
URX_CTR_LOOP_NG) directly, never inline) and POSSESSIVE intervals ALWAYS
compile blockTopLoc + counted loop wrapped in STO_SP/LD_SP
(doPossessiveInterval, never inline). matchStartType treats CTR_INIT
and CTR_INIT_NG in one case (3046-3071): atStart is cleared, with
min==0 loops still forwarding the current minimum to the loop end;
STO_SP/LD_SP are in the atStart-neutral zero-width class (2751-2754,
"Doesn't change what can match"). Live probes (icu-rev8-probe.log, exit
0) pin the discrimination on `a\r\nb` under m: `x{0}^\n` FALSE (Q1 -
ordinary zero-count deletes the unit, the caret stays line-anchoring),
but `x{0}?^\n` and `x{0}+^\n` TRUE at [2,3) (Q2/Q3 - the kept counted
loop clears atStart, so the caret is a bare mid-pattern one asserting
at the CRLF interior); all three FALSE under mu (Q1-mu/Q2-mu/Q3-mu) and
identical under the routing lookahead prefix (Q1r/Q2r/Q3r). The review
is also correct that the regexp2 SQL translation destroys the flavor
before any in-parser recorder could see it: requiresRegexp2Pattern
routes possessive forms (`*+`, `++`, `?+`, `{n,m}+`;
regexp_icu.go:260-268) to regexp2, where translateRegexp2Pattern wraps
the quantified atom as `(?>atom{n,m})` (regexp_icu.go:843-846 comment;
quantifier branch ~1007-1025). The recorded event stream must therefore
be taken from the ORIGINAL (pre-translation) pattern text on the regexp2
route as well, carrying flavor explicitly.

(F2, round 7) Model literal-run sealing before singleton character
classes. Rev 7 merged textually adjacent literals, including singleton
classes lowered through literalChar. ICU seals the pending literal run
at every compiler boundary FIRST: doSetBegin calls fixLiterals(false)
when a class OPENs (icu-77.1-regexcmp.cpp:1653-1655), before compileSet
at case 1 lowers a singleton set through literalChar (2410-2420);
doSetMatchMode seals at inline flag sets (1494-1505, "using the
not-yet changed match mode") even when the flags do not change or do
not affect literals; doMatchModeParen seals at scoped-flag group opens
(1507-1527); group opens/closes of every kind (465, 509, 582,
619-714, handleCloseParen 2192), alternation (doOrOperator 398),
property classes (1281), backreferences (1326), dot-any (1119) and
pattern end (doExit 1275) seal likewise; a quantifier instead SPLITS
the run so it binds to the last code point only (blockTopLoc calls
fixLiterals(true), 2139-2169). Emission is per sealed run
(1933-1958): a case-sensitive run of >= 2 code points becomes
URX_STRING (the only +1 initial content, accumulator 3001-3020), a
single char becomes URX_ONECHAR (+2), case-folded content becomes
URX_ONECHAR_I/URX_STRING_I (+2, accumulator 2902-2922/3020-3043). Live
probes (icu-rev8-probe.log, exit 0) pin the directional pairs Rev 7's
adjacency rule cannot express on `a\r\nxb` under m: `^[\n]x` TRUE at
[2,4) (C1 - class open seals an empty pending run, the lowered
singleton joins the NEW pending run with x), but `^\n[x]` FALSE (C2)
and `^[\n][x]` FALSE (C3); the flag-boundary seals without literal
relevance `^\n(?s)x` FALSE (C4), `^\n(?s:x)` FALSE (C5), `^\n(?i)x`
FALSE (C6), the plain noncapture-group boundary `^\n(?:x)` FALSE (C7),
while the empty-pending seal control `(?s)^\nx` is TRUE at [2,4) (C9);
all three m-class rows FALSE under mu (C1-mu/C2-mu/C3-mu) and the
distinction holds under the routing lookahead prefix (C1r true [2,4);
C2r/C3r/C4r/C5r false). Rev 7's adjacency rule grants the exemption to
all of C2-C7 and is falsified on every one.

The three Rev 8 design-review findings are likewise verified against
primary sources and live ICU 77.1 measurement before acting (see the
Rev 9 change-log entry):

(F1, round 8) Preserve quantifier FORM as well as bounds and flavor.
Rev 8 recorded {min,max,flavor}, which makes the OPERATOR forms
indistinguishable: `+` and `{1,}` carry the same triple. ICU compiles
the forms differently. A plus loop keeps the quantified unit FIRST and
appends its loop control AFTER the body (doPlus' general case:
`appendOp(URX_JMP_SAV, topLoc)` after the body ops,
icu-77.1-regexcmp.cpp:795-845, with STO_INP_LOC/JMP_SAV_X when the body
can match empty; doNGPlus' trailing STATE_SAVE, 847-856;
doPossessivePlus: an atStart-neutral STO_SP inserted at topLoc, then the
body, then trailing STATE_SAVE/JMP/LD_SP, 1371-1400), so
matchStartType reaches a caret inside the body BEFORE any
control-flow op - the caret keeps atStart and stays line-anchoring. An
interval places its loop INIT BEFORE the body (compileInterval's
CTR_INIT/CTR_INIT_NG at the block top, 2440-2504), which clears atStart
(accumulator CTR case, 3046-3071), so the same caret is a bare
mid-pattern one asserted at the CRLF interior. Star and question forms
lead with branch/save guards (doOpt 859-871, doNGOpt 873-895, doStar
899-986, doNGStar 988-1002, doPossessiveStar 1402-1433, doPossessiveOpt
1436-1463), which makes them FORM-EQUIVALENT to the {0,} and {0,1}
interval spellings. Live probes (icu-rev9-probe.log, exit 0) pin every
pair on `a\r\nb` under m: `(?:^\n)+` FALSE (V1 - the caret inside the
plus body is reached while atStart) vs `(?:^\n){1,}` TRUE at [2,3)
(V2); the lazy pair `(?:^\n)+?` FALSE (V3) vs `(?:^\n){1,}?` TRUE
[2,3) (V4); the possessive pair `(?:^\n)++` FALSE (V5) vs
`(?:^\n){1,}+` TRUE [2,3) (V6); all six FALSE under mu
(V2/V4/V6-mu measured; the V1/V3/V5-mu direction follows the same
LF-only caret); the routed variants agree (V1r false, V5r false,
V2r/V6r true [2,3)); and the star/question family `(?:^\n)*`,
`(?:^\n){0,}`, `(?:^\n)?`, `(?:^\n){0,1}` all match only the empty
string [0,0) (V7-V10 - leading guards in both spellings). The event
vocabulary therefore carries the quantifier FORM
(star|plus|question|interval) alongside min/max/flavor, and the shared
core performs FORM dispatch before the flavor dispatch of round 7.

(F2, round 8) Model empty character classes as failure events. Rev 8
resolved classes only into singleton-vs-greater. ICU's compileSet
resolves cardinality 0 as well: an empty resolved set emits
URX_BACKTRACK (case 0 of the switch at icu-77.1-regexcmp.cpp:2403-2437,
op appended at 2407), whose matchStartType case (2970-2975) reads the
length forwarded to the FOLLOWING op, contributes NOTHING to
numInitialStrings and marks its own path atStart-destroyed. Live probes
(icu-rev9-probe.log) on `a\r\nxb` under m:
`^(?:[^\x00-\x{10ffff}]|\nx)` is TRUE at [2,4) (E1 - the empty branch
is dead and contributes no initial content, so the ONE initial string
`\nx` still exempts; a consuming +2 or opaque treatment would raise
numInitialStrings to 3 and strip the exemption, which is the review's
exact counterexample); the negated singleton
`^[^\x00-\x09\x0b-\x{10ffff}]x` is TRUE at [2,4) (E2 - complement
arithmetic over the full range leaves exactly {LF}, so it lowers
through literalChar exactly like the `[\n]` of probe C1); the control
`^(?:[^\x00-\x{10ffff}]|x)` matches only `x` at [3,4) (E3 - the dead
branch never matches; the live branch matches at the genuine line
start); E1/E2 FALSE under mu, E3 TRUE [3,4) under mu; the routed
variants E1r/E2r TRUE [2,4) through the fork's braced-hex `\x{...}`
syntax (fork syntax/parser.go:1701-1707). Class close events therefore
RESOLVE cardinality 0/1/greater with complement arithmetic:
cardinality 0 records the failure event, 1 (negated or not) joins the
new pending run, greater stays a +2 consuming set.

(F3, round 8) Preserve ICU escape identity in classification metadata.
Both engines lower `\v` and `\x0b` to the same VT literal rune (Go
stdlib; the vendored regexp2 core does likewise), so a classifier that
lexes like the engines cannot see the distinction. ICU's compiler does
make it: doBackslashv (icu-77.1-regexcmp.cpp:1236-1239) seals the
pending literal run (fixLiterals(false)) and appends URX_BACKSLASH_V,
whose matchStartType case (2883-2897) is a consuming line-ending set
op at +2 - the spelling NEVER joins a literal run - while the
equivalent hex/octal/unicode escapes (`\x0b`) lower through
literalChar and DO join. Live probes (icu-rev9-probe.log) on
`a\r\n<VT>b` under m: `^\n\v` is FALSE (X1 - the pending run `\n` is
sealed as a one-char unit and the BACKSLASH_V op adds 2, so no initial
string exists) vs `^\n\x0b` TRUE at [2,4) (X2 - the pending run
`\n\x0b` is one initial string); both FALSE under mu (X1-mu/X2-mu);
routed X1r FALSE, X2r TRUE [2,4). The classification contract is
MySQL's, which is ICU's, so the shared event vocabulary records the
ICU-specific BACKSLASH_V event for `\v` on BOTH routes while the
engines keep their own consuming-atom semantics for `\v` matching -
the documented non-goal unchanged.

The single Rev 9 design-review finding is likewise verified against
primary sources and live ICU 77.1 measurement before acting (see the
Rev 10 change-log entry):

(F1, round 9) Seal the pending literal run at assertions and
backreferences, INDEPENDENTLY of their zero-width, atStart-preserving
and zero-min-length effects. Rev 9's event vocabulary and sealing rule
marked classes, flag sets, group boundaries, alternation, quantifier
splits and pattern end as run-sealing but omitted anchors, boundary
assertions and backreferences, and the review proved those are
separate effects that cannot be derived from atStart or length
behavior: ICU flushes the pending literal run via fixLiterals(false)
for caret (icu-77.1-regexcmp.cpp:1132), dollar (1147), `\A` (1161),
`\b` (1172), `\B` (1185), `\G` (1202), `\Z` (1261), `\z` (1266) and
for numbered/named backreferences (1326/1359; the fixLiterals default
argument is split=false, regexcmp.h:103), while the SAME opcodes stay
in the zero-width, atStart-preserving, min-length-0 accumulator class
(zero-width group 2742-2757; backreferences possibly EMPTY,
2752-2753, with maxMatchLength unbounded, 3548-3551; caret kinds
additionally record START_START/START_LINE only when reached atStart,
2759-2770). Live probes (icu-rev10-probe.log, exit 0; 24/24 rows)
pin the distinction with find/lookingAt pairs. On
`a\r\n<LF>xb` under m, `^\n$\nx` (A1) and `^\n\B\nx` (A2) find NO
match - the seal splits the run into the one-char unit `\n` plus the
later string `\nx`; the one-char head counts +2 initial units, so
stringExempt is lost and the START_LINE find loop offers only 0, 3, 4
(never the CRLF-interior 2), where no candidate completes the pattern
- although lookingAt(2) succeeds in the engine [2,5) (A1-la2/A2-la2;
the engine's own caret and `\B` accept the interior mid-match). The
adjacent-literal control `^\n\nx` (A3) keeps one run and matches
[2,5); the routed variants agree (A1r/A2r false, A3r true); all six
spellings are FALSE under mu. On `a\r\nxb` under m, `(?=())^\n\1x`
(B1) and the named form `(?=(?<e>))^\n\k<e>x` (B3) find FALSE -
the backreference seal splits the run into `\n` + `x`, two initial
units, exemption lost - although lookingAt(2) matches [2,4)
(B1-la2/B3-la2: the lookahead's empty capture is preserved by
URX_LA_END's frame copy-down, icu-77.1-rematch.cpp:3780-3811, and an
empty group's backreference matches zero-length, URX_BACKREF
3641-3680); the controls `(?=())^\nx` (B2) and `(?=(?<e>))^\nx` (B4)
match [2,4); all four FALSE under mu. Without the omitted seals the
shared core merges the surrounding literals into ONE initial run,
grants stringExempt and admits the interior matches - the review's
exact counterexamples, recorded as rejected alternative 2k. The ONE
shared seal/atStart/length event table in invariant 8 now names every
ICU literal-flush boundary with its three independent effects.

Owner: MatrixOne SQL function maintainers (`pkg/sql/plan/function`).
Issue: matrixorigin/matrixone#30210. Delivery: this worktree's issue branch.
Base: `origin/main` `6fb6531432aaec8c1fbd98a30364941559e0e1d6`
(worktree HEAD == origin/main tip; contains PR #30646's merge commit
c3e962df0a7efd7202ab9cbc37ce739f7c8a883a). Pinned Go toolchain: 1.27.1
(`go.mod`), which matches the private engine source.

## Design review record

The repository design-first review contract
(`.agents/skills/mo-dev/references/feature-design-review.md`) requires a
traceable reviewed revision and approval decision for this change. The
design-first phase was distinct and complete BEFORE implementation
review began; each decision is keyed to the reviewed revision's SHA-256,
so every decision verified the exact document at rest rather than moving
text. Recorded per the contract's compact review record:

- Change scope: the whole feature - private RE2 wide line mode
  (`pkg/sql/plan/function/internal/regexp`), maintained regexp2 line
  modes (`internal/regexp2`), the shared classifier
  (`internal/regexpclass`) and the SQL translation layer
  (`func_builtin_regexp.go`, `regexp_icu.go`), with the carried decision
  delta in `docs/design/20261024-regexp-wide-line-assertions.md`.
- Trigger: size (3,458 lines of non-vendored classifier/engine production
  code across 5 owning components) AND complexity (SQL compatibility
  contract with MySQL/ICU line semantics on a SQL hot path; a maintained
  private engine subsystem shared by two engines).
- Design: this document. Rev 1-9 (2026-10-24..2026-10-25): design-stage
  REQUEST_CHANGES; every finding was verified against primary sources and
  live ICU 77.1 measurement and addressed in the revision recorded in the
  change log (Rev 2..Rev 10 entries). Rev 10 (sha256
  `5dcab7b28d863dbec65e003b5db3503fdbc3ee1c1374d4622e9ae737bf3315b4`):
  design-stage PASS on 2026-10-25, independent local design reviewer, no
  remaining blocking finding.
- Blocking findings: none remaining.
- Decision log: the per-revision findings and resolutions are the change
  log below; accepted tradeoffs are the rejected alternatives (2a..2k) in
  Standards, precedent, and alternatives.
- Decision: PASS.
- Implementation deviations: none. The implementation-stage independent
  local reviews re-verified the invariant map against the implemented
  code in every round and retained the Rev 10 design PASS unchanged,
  ending in implementation approval with two non-blocking P3 findings;
  both are addressed at the delivered head (the vendored-copy
  synchronization procedure and maintained-change inventory in
  `internal/regexp2/README.md` + `PROVENANCE`, and the unseen-spelling
  cache-bound assertions in
  `internal/regexpclass/review_fixes_round9_test.go`).
- Rev 11 (this revision): publishes the PASS inside the document in
  response to the external review on PR #31125 finding the Status header
  still read "pending design review" and the PR carried no traceable
  approval record. No design content changed - every Rev 10 statement is
  retained verbatim outside the Status paragraph, this section and the
  Rev 11 change-log entry, so the Rev 10 PASS applies unchanged. This
  revision's own sha256 is pinned in the PR body and the review reply, by
  the same hash-pinning convention that pinned Rev 10's.

## Problem and evidence

MySQL 8.0 maps match_type `m` to ICU `UREGEX_MULTILINE` and `u` to
`UREGEX_UNIX_LINES`. Verified primary evidence (curl fetches, 2026-10-24,
copies under `state/artifacts/issue-30210/`):

- `mysql-server@mysql-8.0.46:sql/item_regexp_func.cc:67-87`:
  `case 'm': result |= UREGEX_MULTILINE;` and
  `case 'u': result |= UREGEX_UNIX_LINES;`
  (`'n' -> UREGEX_DOTALL`, `'i'/'c' -> UREGEX_CASE_INSENSITIVE`).
- Bundled ICU version: `mysql-server@mysql-8.0.46:extra/icu` is
  `icu-release-77-1` with `icudt77b` data (repository listing of the tag;
  `cmake/icu.cmake` only states the floor `MIN_ICU_VERSION_REQUIRED "55"`).
  The bundled source defines the compatibility oracle.
- ICU 77.1 `icu4c/source/i18n/rematch.cpp` assertion opcodes (copy
  `icu-77.1-rematch.cpp`):
  - `isLineTerminator(c)` (lines 59-62):
    `(c <= 0x0d && c >= 0x0a) || c == 0x85 || c == 0x2028 || c == 0x2029`,
    i.e. LF, VT, FF, CR, NEL, LS, PS.
  - `URX_CARET_M` (multiline `^`): true at subject start; otherwise the
    preceding character is a line terminator AND a following character
    exists (`fInputIdx < fAnchorLimit`). The caret therefore HOLDS between
    CR and LF of a CRLF pair (source comment: "TODO: what should be done
    with positions between a CR and LF?"), and does not hold at EOF after
    any trailing terminator.
  - `URX_DOLLAR_M` (multiline `$`): true at end of subject; otherwise the
    current character is a line terminator EXCEPT the LF of a CRLF pair
    (`!(c == 0x0a && prev == 0x0d)`). The dollar therefore FAILS between
    CR and LF.
  - `URX_CARET_M_UNIX` / `URX_DOLLAR_MD` (UNIX_LINES forms): caret after LF
    only; dollar before LF or at EOF with no CR exclusion, so `\r$\n` is a
    match under `mu`.
- Live ICU 77.1 probe (NEW in Rev 6; executable oracle for every disputed
  row): `state/artifacts/issue-30210/icu-rev6-probe.cpp` (sha256
  `7ab777bbc5dcdad13f30a57b66ac12bc7f510e7a2c49ae6d52a4b3493541883c`)
  compiled against the installed ICU 77.1 and driven through
  UREGEX_MULTILINE / UREGEX_UNIX_LINES + uregex_find (the same API MySQL's
  regexp_engine.cc uses); run log
  `state/artifacts/issue-30210/icu-rev6-probe.log` (sha256
  `4a395a50e247526206092ca28bd23c0537748a1ac314c29ae6e8ccf0238aadf3`,
  exit 0). Measured rows cited throughout: T02/T03 `^b` after VT/FF true
  [2,3); T10/T11 caret at EOF after CR/NEL false; T12 caret at EOF after
  LF false in ICU (MatrixOne keeps its LF-EOF legacy, invariant 4); S01
  `^\n` false; S03-S05 `(^\n)`, `(?:^\n)`, `\B^\n` false; S06 `^\nx` true
  [2,4); S07 mi folded false; S08 `^[\n]x` true [2,4); S09 S08-subject
  `a\r\nb` variant false; S10 `^[\nx]` true [3,4); S11 `^[\n\r]x` false;
  S12 `^[x]` sanity true; S13 `^(?:\n)x` false; S14 `^(?:\nx)` true [2,4);
  S15 `^(\n)x` false; S16 `^\n1` m true [2,4); S17 mi false; S18 `^\n|z`
  true [2,3); S19 `(?>(\r))^\n` true [1,3); S20 `(\r)^\n` true [1,3); S21
  `(?=\r)\r^\n` true [1,3); S22 `()\1^\n` on `x\n\n` true [2,3); S23 same
  on `a\r\nb` false; S24 `(?!(\r))^\n` false; S25 `(?=[\s\S])^[\n]x` true
  [2,4); S26 `(?=[\s\S])^(?:\n)x` false; S27 `(?=[\s\S])^\n1` m true [2,4);
  S28 mi false; P01-P04 positional find variants equal the whole-subject
  rows; D01-D03 dollar confirmations.
- ICU 77.1 compiler structural start analysis (copy
  `icu-77.1-regexcmp.cpp`, `RegexCompile::matchStartType`, lines 2692-3188):
  a boolean `atStart` begins true and is preserved across
  zero-width-progress ops: NOPs, capture open/close saves
  (`URX_START_CAPTURE`/`URX_END_CAPTURE`/`URX_STO_INP_LOC`), boundary
  assertions (`URX_BACKSLASH_B/BU/G/Z`), all dollar forms, backrefs
  (conservatively) and atomic-block stack ops (lines 2734-2757);
  lookaround blocks are skipped wholesale and also preserve `atStart`
  (lines 3088-3127). Any consuming op and any control-flow op
  (`URX_STATE_SAVE`, `URX_JMP`, `URX_BACKTRACK`, counter loops, lines
  2940-2990) clears `atStart`. A `URX_CARET_M`/`URX_CARET_M_UNIX` reached
  while `atStart` selects `fStartType = START_LINE` (lines 2765-2770).
  `doCaret` maps m -> `URX_CARET_M`, m+u -> `URX_CARET_M_UNIX`, u alone ->
  `URX_CARET` (lines 1129-1145); the dollar mapping m -> `URX_DOLLAR_M`,
  u alone -> `URX_DOLLAR_D`, m+u -> `URX_DOLLAR_MD` (lines 1146-1157).
  Singleton CHARACTER CLASSES are compiled through `literalChar` (the set
  handler's `case 1:` at lines 2410-2420), i.e. `[\n]` reaches
  matchStartType as the literal char LF, NOT as a set (finding F1).
- Start-type PRIORITY LADDER (lines 3160-3188): `START_START` first; then
  a SINGLE case-sensitive literal string with minMatchLen > 0
  (`numInitialStrings == 1`; one literal run of length >= 2 code units,
  contiguous in the compiled program - any intervening group save marker,
  or a set/quantifier/folded char, ends contiguity) takes `START_STRING`;
  then `START_LINE`; then char/set starts. A single consuming char is
  `URX_ONECHAR` (not a string), so `^\n` stays `START_LINE` while `^\nx`
  and - via literalChar lowering - `^[\n]x` take `START_STRING`; a
  group boundary AFTER the first literal splits the run (`^(?:\n)x` stays
  `START_LINE`, probe S13) while a boundary AFTER both keeps it
  (`^(?:\nx)` takes the string, probe S14); case-folded runs only seed
  initial chars (`URX_STRING_I`), so `mi` never selects `START_STRING`
  (probes S07/S17/S28).
- ICU search-candidate layer (above the assertion opcode): both
  `RegexMatcher::find()` (START_LINE at rematch.cpp lines 802-881) and
  `findUsingChunk()` (lines 1072-1142) generate candidates only at the
  anchor start and immediately AFTER a line terminator, and when that
  terminator is CR followed by LF they `startPos++` past the pair: the
  CRLF interior is never offered as a candidate match start. Candidates
  ARE offered after a lone CR, after LF, and after each other wide
  terminator. MySQL (`mysql-8.0.46-regexp_engine.cc` lines 56-99) drives
  evaluation through `uregex_find` / `uregex_findNext`, so this candidate
  policy is the live SQL search contract. Consequence: caret-INITIAL
  pattern `^\n` on `a\r\nb` does NOT match under `m`; consuming-prefix
  `\r^\n` still matches because its caret is evaluated after consuming CR,
  at a position the search never had to offer.
- MySQL binary facade `mysql-server@mysql-8.0.46:sql/regexp/regexp_facade.cc`
  (`faux_binary_charset = &my_charset_latin1`, applied to every binary
  operand regardless of case folding): binary bytes reach ICU as CP-1252
  characters, so byte 0x85 is U+2026 (horizontal ellipsis), never NEL, in
  both case modes.
- Admission accounting (verified against this worktree; logs
  `state/artifacts/issue-30210/admission-probe-rev4.log`,
  `state/artifacts/issue-30210/trackcount-probe-rev5.log` and the Rev 6
  production probe below): evaluation admission charges
  `(subjectRunes+1)*code.TrackCount` against
  `regexp2MaxEvaluationStates = 1<<20` (regexp_icu.go:2003, constants
  at lines 44-77) plus a memory total capped at 8<<20 bytes with 16 bytes
  per state; the cached-compiled estimate also depends on expression length
  (regexp_icu.go:1359-1371), and concurrent evaluations share the 64 MiB
  `regexp2ActiveSubjectBytes` accumulator with immediate rejection above it
  (regexp_icu.go:2158-2214).
- Production admission regression measured on the merged base (NEW in Rev
  6; premise probe `state/artifacts/issue-30210/trackcount-probe-rev6.log`,
  exit 0; harness
  `state/artifacts/issue-30210/harness/trackcount_probe_rev6_test.go`,
  sha256 `737c012589f3a7882ec6d922dab8df64ab6eec704e21f8cf25dae2693ce01b26e`;
  patterns composed through `regexpPatternWithPureMatchType` +
  `translateRegexp2Pattern` exactly as the production regexp2 route does):
  the merged wide-m anchor rewrite raises TrackCount for `(?m)^b(?=b)`
  from the plain 7 to 37 (translated expression length 7 -> 80 bytes), for
  the 32-pair anchor-dense pattern from 7 to 1671, and for bare `(?m)^`
  from 3 to 33; `mu` stays identical to plain (7). Boundary consequences:
  the anchor-dense 30,000-rune subject charges 50,131,671 states and is
  REJECTED (plain: 210,007, admitted); the wide-bearing 74,000-rune
  subject charges 2,738,037 states and is REJECTED (plain: 518,007,
  admitted); and the review case - the 50,000-rune subject with
  `^b(?=b)` under `m` - now ERRORS in production ("regular expression
  evaluation exceeds the maximum supported backtracking state"), the exact
  regression Rev 4 predicted for rewrites, live on merged main.

Issue #30210 measured MySQL 8.0.46 (utf8mb4) across CR, LF, CRLF, NEL, LS,
PS: with `m`, all six terminate a line; with `mu` only LF/CRLF do. VT/FF
complete the same contract (ICU isLineTerminator, probes T02/T03).

What the merged implementation (PR #30646, on this base) already delivers,
verified by reading the merged source and running harness rev8 (every rev7
row carried): the private
Go 1.27.1 engine `pkg/sql/plan/function/internal/regexp` with LineMode and
original-context offset search; `getPureMatchType` keeping `u`
(func_builtin_regexp.go:3343); the RE2 route prefix
(func_builtin_regexp.go:3258) and the cache key carrying matchType
(func_builtin_regexp.go:2078); wide text terminators {CR, NEL, LS, PS}
(lines.go:27-41) so the CR/NEL/LS/PS text rows, the `mu` rows and the
whole baseline contract are already green. What remains defective (the Rev
6 defect set, all red in the recorded rev6 run): VT/FF in every mode and
on both engine routes (text and binary, LIKE/INSTR/SUBSTR/REPLACE plus
dollar); the mid-match caret wrongly excluded between CR and LF
(exec.go:128-140; ICU URX_CARET_M and probes S19-S21 require it to hold);
binary byte 0x85 wrongly classified as a terminator in both binary modes
(lines.go:35-38; contradicts the CP-1252 facade evidence); the entire
search-candidate/structural-start layer missing on both engines (the
merged engines have no start classification, and the merged regexp2 path
inherits the red structural defect rows while adding the measured
production admission regression above); the merged UT pins that encode
these defects (pkg/sql/plan/function/regexp_multiline_test.go:103-110
binary `\x85` m TRUE; internal/regexp/lines_test.go:59-60 binary U+E085 /
U+2026 terminators) are amended by this design.

Reproduction on the merged base with the versioned harness (rev6,
`state/artifacts/issue-30210/harness/repro30210_rev6_test.go`, sha256
`0c6543ea1c52741e61d64dbaf3737478e1ec690562bf7da73e16fb529450a4ae`; run
log `state/artifacts/issue-30210/repro-base-go-unit-rev6.log`, exit 1 by
design). rev6 carries every rev5 sha256
`e79946cd478ac167e0ab2f8dc452ba136125d565a4c0763b96e5b9def1b334dc` row,
corrects the singleton-class rows per finding F1, adds the
pre-optimization distinguishing pairs of finding F2 and the
atomic/lookaround/backreference rows of finding F3, and repins the
admission arithmetic against the merged production composition. Red rows
on the merged base (each failing assertion was attributed one-by-one in
the run log):

- VT and FF on all four production entry points (LIKE/INSTR/SUBSTR/REPLACE)
  plus the dollar form, on the regexp2-routed probe, and in binary raw and
  folded modes (text VT/FF, binary VT/FF under `m`/`mi`; `mu` rows green).
- CRLF-interior mid-match caret on BOTH engine routes: `\r^\n` on `a\r\nb`
  with `m` is false (MySQL: true) while `\r$\n` stays true (the dollar
  side is already correct in the merged engine).
- Binary subjects: byte 0x85 answers TRUE under `m` and `mi` on the base
  (must be FALSE per the facade); binary CR rows green.
- Structural-start defect rows: the alternation family `^\n|z`
  (LIKE true, INSTR from position 2 = 3, SUBSTR = LF, REPLACE -> `a\rXb`);
  the literal-string rows `^\nx`, `^[\n]x` (finding F1:
  LIKE/INSTR/INSTR-pos2/SUBSTR/REPLACE plus the regexp2-routed
  `(?=[\s\S])^[\n]x`), `^(?:\nx)` and `^\n1` (m) all true in ICU
  (probes S06/S08/S14/S16/S25/S27) and false on base; the regexp2-routed
  `(?:^\n|z)(?=b)`; and the finding-F3-traversal rows `(\r)^\n`,
  `(?>(\r))^\n` (LIKE/INSTR 2/SUBSTR CRLF/REPLACE `aXb`) and
  `(?=\r)\r^\n` (all ICU true at [1,3), false on base).
- Production admission regression rows (NEW, all red): the 50,000-rune
  admitted-subject control now errors on the merged base (Rev 4 row);
  production-composed TrackCount pins read 37/1671/33 where the
  identity contract demands the plain 7/7/3; the wide-bearing 74,000-rune
  and anchor-dense 30,000-rune subjects are REJECTED on base where the
  plain-pattern arithmetic admits them.
- Green controls (must STAY green; all green in the recorded run):
  LF/CRLF rows, every `mu` row on both routes, the no-`m` baselines, the
  search-candidate guards (`^\n` on `a\r\nb` false on both routes and
  match_types; `^$` on `a\r` false; `^$` on `a\n` true per the documented
  LF-at-EOF legacy; INSTR `^\n` from position 2 = 0 while `^b` = 4), the
  structural guards (capture/scoped/boundary-prefixed carets, the VERIFIED
  non-singleton-set negative control `^[\n\r]x` false, case-folded runs
  `^\nx`/`^\n1` with `mi` false, group-split literal `^(?:\n)x` false,
  capture-split `^(\n)x` false, negative-lookahead-prefixed caret
  `(?!(\r))^\n` false, the empty-backreference negative control `()\1^\n`
  on `a\r\nb` false, caret-at-EOF after trailing CR/VT/FF/NEL false per
  ICU rows T10/T11, and the LF-at-EOF legacy surveillance row true), the
  positional `^[\nx]` position-pinning controls (LIKE true, INSTR = 4,
  SUBSTR = `x`, REPLACE -> `a\r\nXb`: line candidates reach only the
  completed line starts, never the CRLF interior - green on base because
  the base caret wrongly rejects the interior mid-match; post-fix they pin
  the candidate layer against over-offering), the empty-backreference
  positive control `()\1^\n` on `x\n\n` (true at [2,3), INSTR 3; base
  answers by accident and the fix must preserve it), the plain TrackCount
  pins 7/7/3/5, and the deterministic overlap/reject/release arithmetic of
  the 64 MiB global accumulator.

Root cause on the merged base (owner path):

1. Terminator classification: `isWideTerminator`
   (pkg/sql/plan/function/internal/regexp/lines.go:27-41) omits VT (0x0B)
   and FF (0x0C) in every wide mode (ICU isLineTerminator includes them;
   probes T02/T03) and classifies binary byte 0x85 - U+E085 in the raw
   identity alphabet, U+2026 in the CP-1252 folded alphabet - as a
   terminator in both binary modes (lines 36/38), contradicting the MySQL
   binary facade (invariant 6). ASCII bytes pass through the binary
   mapping unchanged (func_builtin_regexp.go:3064-3070, bytes < 0x80 are
   written through; only bytes >= 0x80 map to U+E000+byte), so binary
   CR/VT/FF arrive as U+000D/U+000B/U+000C
   and one unconditional set covers text and both binary alphabets.
2. Assertion evaluation: the merged caret fork (exec.go:128-140) requires
   `!(r1 == '\r' && r2 == '\n')`, excluding the CR|LF interior mid-match;
   ICU's URX_CARET_M accepts it whenever a character follows (probes
   S19-S21 confirm live). The merged dollar fork is already correct
   (excludes only LF preceded by CR; holds at EOF) and the LF-at-EOF caret
   legacy is already preserved; mu on both routes is already LF-only.
3. Search candidates: neither merged engine has a structural start
   analysis, so no line-start candidate policy exists at all; merged
   behavior at the CRLF interior is an accident of the (wrong) caret
   exclusion for LIKE and an open question for positional/search scans.
   The regexp2 route instead rewrites anchors
   (`rewriteRegexpWideMultilineAnchors`,
   func_builtin_regexp.go:3146-3152, invoked at 3244), which is the Rev
   3-4 falsified approach and is now measured regressing admission in
   production (evidence above). The classification metadata must be read
   BEFORE Go's parse/compile and regexp2's Reduce erase group boundaries,
   singleton-set lowering and fold state (finding F2), and lookaround vs
   consuming-atomic recognition must be node-type-based with exact
   Ref/Testref naming (finding F3). The merged private design
   (docs/design/20261024-regexp-wide-line-assertions.md, line 18) records
   the three contrary decisions (VT/FF out of scope; binary 0x85 a
   terminator; no assertion between CR and LF); this design supersedes
   them with the evidence above.

## Invariants

1. (`m` without `u`) Assertion truth tables, specified separately per ICU
   77.1 and applied wherever the assertion is EVALUATED MID-MATCH in the
   pattern, including inside `(?m:...)` scopes. Match-BEGIN eligibility is
   the separate search-candidate contract of invariant 8:
   - `^` evaluated mid-match holds at the start of the subject; or
     immediately after any wide terminator (LF, CR, VT, FF, NEL, LS, PS)
     when a character follows that position. Consequences: a caret reached
     after consuming input HOLDS between CR and LF of a CRLF pair (so
     `\r^\n` matches `a\r\nb`, and probes S19-S21 must hold); `^` does not
     hold at end of subject after a trailing CR/VT/FF/NEL/LS/PS (probes
     T10/T11). (After a trailing LF the legacy MatrixOne caret-at-EOF
     behavior is preserved; see invariant 4.) This amends the merged
     exec.go caret fork, which excludes the CR|LF interior.
   - `$` holds at end of subject; or immediately before any wide
     terminator EXCEPT before the LF of a CRLF pair. Consequences: `$`
     FAILS between CR and LF (that is the only surviving sense in which
     CRLF is atomic); `$` holds before a lone CR, LF, VT, FF, NEL, LS, PS.
     The merged dollar fork already implements this table; added VT/FF
     extend it.
   - Adjacent non-CRLF terminators (e.g. CR then VT) are two separate
     boundaries for both assertions.
2. (`mu`, and patterns without `m`) Behavior is byte-identical to today.
   `mu` (UREGEX_UNIX_LINES) keeps LF as the only line terminator with the
   Unix assertion forms (`\r$\n` matches, `\r^\n` does not); compiled
   default behavior stays equal to the pinned Go 1.27.1 standard library
   (differential proof). The merged base already preserves `u` through
   getPureMatchType and routes `mu` to the Unix tables on both engines;
   this revision keeps that and adds parity UTs.
3. Line assertions only. Consuming atoms (including `.` and `(?s:.)`), text
   anchors `\A`/`\z`, word boundaries, capture priorities, NUL-containing
   and invalid-UTF-8 subjects are unchanged for every mode. Recorded
   non-goal: ICU dot-all/dot classification (`.` not crossing
   VT/FF/NEL/LS/PS without `n`) is a distinct compatibility contract;
   changing consuming-atom semantics is out of this issue's scope and
   migration risk budget. The consuming patterns `\r^\n` and `\r$\n` pin
   invariant 1 without touching this non-goal.
4. End-of-input rules: for the NEW terminators (CR/VT/FF/NEL/LS/PS), ICU's
   rule is adopted: `^` requires a following character; `$` still holds at
   end of input regardless of the last character. Existing MatrixOne
   LF-at-EOF behavior (RE2's caret match after a trailing LF, including
   CRLF) is preserved unchanged; ICU would reject that caret (probe row
   T12), and changing long-standing LF-at-EOF behavior is out of this
   issue's scope. The LF deviation is restricted to the caret; `$` at EOF
   behaves identically in RE2 and ICU.
5. Whole-query contracts survive: original-subject match positions,
   character (not byte) indices for INSTR/SUBSTR (single-character
   separators place `b` at 3, CRLF at 4), occurrence counting including
   empty lines, replacement expansion and its aggregate output budget,
   NULL/error precedence (present pattern compiled before applicable
   NULL/range shortcuts), and binary subjects emitting original bytes.
6. Binary subjects never classify byte 0x85 as a line terminator. MySQL
   presents binary operands to ICU as CP-1252, so 0x85 is U+2026 in both
   case modes; MatrixOne's raw identity rune U+E085 and its case-folded
   U+2026 are both NON-terminators. The binary wide classifier is exactly
   {CR, VT, FF} plus the native LF; NEL/LS/PS are unreachable in binary
   alphabets. Since the binary mapping passes ASCII bytes through
   unchanged, CR/VT/FF arrive as U+000D/U+000B/U+000C in every alphabet,
   and the wide classifier needs no binary-only entries at all. This
   amends the merged lines.go binary modes (lines 35-38) and the merged
   UT pins that encode them (lines_test.go:59-60,
   regexp_multiline_test.go:103-110). (Supersedes Rev 2's byte-identity
   NEL rule.)
7. No user-visible change outside `m`-without-`u` multiline: no new errors,
   no subject-size caps, no per-row pattern reparse or subject scan on the
   regular path; regular execution keeps the linear-time guarantee for every
   accepted pattern. (The regexp2 path's existing per-subject machinery is
   shared but not newly charged to the regular path; see invariant 9.)
8. Search-candidate contract (structural start analysis replicating ICU's
   `matchStartType` + the `START_LINE` find loop through MySQL's
   `uregex_find`/`uregex_findNext`). Each engine classifies each pattern
   ONCE at compile time into an immutable descriptor {lineAnchoredStart,
   stringExempt} attached to the matcher; evaluation and admission see the
   ordinary optimized compiled program.
   - PRE-REDUCTION CAPTURE (round-5 finding F2; round-6 finding F1):
     classification reads the representations that still carry
     ICU-relevant structure, and the sixth review established that on the
     regexp2 path even the parsed tree is too late: the pinned fork
     reduces EAGERLY at child insertion (regexNode.addChild calls
     child.reduce() and stores the reduced node, fork
     syntax/tree.go:168-173; parser call sites addConcatenate /
     addConcatenate3 (makeQuantifier) / addGroup, fork syntax/parser.go
     ~2007-2066), so group erasure, literal merging and singleton-set
     lowering are complete before syntax.Compile and syntax.Write ever
     see the tree. Go's parser removes noncapturing/scoped groups during
     PARSE (stdlib syntax/parse.go:1410-1437), its compiler merges rune
     sequences and clears ineffective FoldCase (compile.go:95-108,
     277-280). The structure the ICU procedure reads - the group pairs
     S06/S13/S14, the fold pair S16/S17, the lookahead composites
     S25-S28 (all icu-rev6-probe.log), and the branch pairs
     R1/R2/D4/D5/D10/D13 plus the quantifier pairs
     R3/R3b/R4/D1/D2/D3/D7/D8/D9/D11/D15/D16 (icu-rev7-probe.log) -
     survives only in the pattern text and its parse-time EVENT stream.
     Classification therefore consumes ONE shared pre-lowering event
     vocabulary produced by ONE front end: a raw-pattern-text event
     scanner reading the ORIGINAL (pre-translation) pattern on BOTH
     routes (Plan items 1c and 4c) - on the regexp2 route this is
     required, because translateRegexp2Pattern rewrites possessive
     intervals into atomic groups (regexp_icu.go:1007-1025) and any
     later capture point loses the flavor (round-7 finding F1;
     alternative 2g). The scanner lexes the route's own dialect (Go
     regexp syntax on the RE2 route; the ICU/.NET construct superset
     exactly as requiresRegexp2Pattern detects it on the regexp2 route)
     and never parses for match semantics. The event vocabulary: literal
     code point with fold state (subject to the ICU escape identity
     rule below); class open/close with RESOLVED content (the OPEN is
     a run-sealing event; the close resolves the class with complement
     arithmetic over its members into CARDINALITY 0, 1 or greater,
     mirroring compileSet's switch at regexcmp.cpp:2403-2437:
     cardinality 0 is a FAILURE event - the empty set emits
     URX_BACKTRACK, 2407, contributing no run content and no initial
     content; cardinality 1 - including negated classes whose
     complement is one code point, e.g. `[^\x00-\x09\x0b-\x{10ffff}]`
     resolving to {LF} - lowers through literalChar (2416) and its
     char appends to the NEW pending run; greater cardinality is a
     consuming set event, +2 when initial, 2780-2790); group
     open/close with kind (capturing, noncapturing,
     scoped-flag, atomic, lookahead/lookbehind - every open and close a
     run-sealing event); inline flag set (run-sealing, doSetMatchMode
     regexcmp.cpp:1496, even when the flags are unchanged or literal-
     irrelevant); flag toggle with fold state; quantifier {FORM:
     star|plus|question|interval, min, max, flavor: ordinary|lazy|
     possessive} applied to the last unit or group (a run-SPLITTING
     event, blockTopLoc fixLiterals(true) at regexcmp.cpp:2141); the
     FORM is load-bearing (round-8 finding F1): a plus loop appends
     its control AFTER its body while an interval places its CTR_INIT
     BEFORE the body, so `+` and `{1,}` classify differently; ICU
     escape identity for shared spellings whose engine meanings
     differ: `\v` records the ICU BACKSLASH_V event - a run-SEALING,
     consuming, +2 content event (doBackslashv, regexcmp.cpp:
     1236-1239; accumulator 2883-2897) - NOT a literal code point,
     while the equivalent hex/octal/unicode escapes (`\x0b`) record
     literals that JOIN the pending run (`^\n\v` FALSE vs `^\n\x0b`
     TRUE [2,4), probes X1/X2; the engines keep their own consuming-
     atom semantics for `\v` - the documented non-goal);
     alternation branch open (run-sealing);
     lookahead/lookbehind body content (skipped blocks); backreference,
     numbered and named (run-SEALING - fixLiterals(false) at
     regexcmp.cpp:1326/1359 - while staying zero-width,
     atStart-preserving and possibly EMPTY in the accumulator,
     2752-2753: no initial content, min-length +0, max-length
     unbounded, 3548-3551; round-9 finding F1);
     conditional; anchor kind (wide/unix caret, absolute anchors, dollar
     kinds, boundary assertions - EVERY anchor and boundary kind
     run-SEALING via fixLiterals(false): caret regexcmp.cpp:1132,
     dollar 1147, `\A` 1161, `\b` 1172, `\B` 1185, `\G` 1202,
     `\Z` 1261, `\z` 1266 - while staying zero-width,
     atStart-preserving and min-length-0 in the zero-width accumulator
     group, 2742-2757; the caret kinds additionally record
     START_START/START_LINE only when reached atStart, 2759-2770;
     round-9 finding F1); pattern end (run-sealing, doExit
     regexcmp.cpp:1275). ONE shared classification core applies
     ICU's compile lowering and accumulator (below) to the event stream
     and emits the two descriptor flags. The compiled program sent to
     matching and every admission input is the engine-normal optimized
     one in every mode (invariant 9); the descriptor is metadata computed
     alongside, never an input to parse, reduce or codegen.
   - Shared classification core (mirroring ICU's compile lowering and
     the matchStartType accumulator exactly; identical on both paths).
     ATSTART CLASSES: `atStart` begins true and is PRESERVED across
     zero-width constructs - capture open/close and position-save
     markers, noncapturing and scoped-flag group CONTAINERS (the group
     boundary still seals the literal run below), boundary assertions
     (`\B^\n` qualifies), dollar assertions, backreferences
     (mirroring ICU's BACKREF class, probe S22) - and across SKIPPED
     lookaround blocks (`(?=b)^\n` qualifies; ICU LA_START skip with
     branch forwarding, 3084-3132); and by atomic/possessive state
     wrappers (STO_SP/LD_SP are in ICU's zero-width atStart-neutral
     class, accumulator 2751-2754, so `(?>` containers alone neither
     qualify nor disqualify); it is DESTROYED by any consuming
     atom outside a skipped lookaround and by any branch/save/loop
     control-flow event (alternation branches; counted-loop init and
     loop ops of EVERY flavor - CTR_INIT and CTR_INIT_NG share one
     accumulator case, 3046-3071, including EVERY min count including 0,
     so a lazy `x{0}?` or possessive `x{0}+` clears atStart even though
     it matches empty; plus-loop trailing controls - the JMP_SAV of
     doPlus, accumulator 2961-2967, and the STATE_SAVE of doNGPlus /
     doPossessivePlus, 2977-2990; optional-copy guards; conditional
     branches; failure events per the next sentence), so `^\n|z`, `x?^\n`,
     `x{0,2}^\n`, `(\r)^\n`, `(?>(\r))^\n` and `(?=\r)\r^\n` do NOT
     qualify (probes S18-S21, D12), neither do the caret prefixes of
     `x{0}?^\n` and `x{0}+^\n` (probes Q2/Q3 true at [2,3) - the kept
     counted loop makes the caret a bare mid-pattern one), while
     `x{0}^\n` DOES (R3/Q1: the ORDINARY zero-count unit produces no
     events at all after the lowering below - compileInlineInterval
     physically removes its ops, 2517-2530, so nothing precedes the
     caret).
     QUANTIFIER LOWERING (applied while folding the pre-lowering events
     into literal runs, mirroring regexcmp.cpp:795-1002, 1371-1463,
     1898-1922, 2139-2169, 2440-2560; FORM- and FLAVOR-DEPENDENT since
     round-8 finding F1 and round-7 finding F1): every
     quantifier first SPLITS a pending literal run so it binds to the
     last code point only (blockTopLoc's fixLiterals(true), 2141; the
     split-off prefix is sealed as its own run; a run of length 1 is a
     single-char unit); then FORM and flavor select the lowering.
     FORM dispatch (round-8 finding F1): a PLUS quantifier of any
     flavor keeps the quantified unit FIRST and appends its loop
     control AFTER the body (doPlus' general case appends
     URX_JMP_SAV at topLoc, 795-845; doNGPlus a trailing STATE_SAVE,
     847-856; doPossessivePlus inserts its atStart-neutral STO_SP at
     topLoc and appends STATE_SAVE/JMP/LD_SP after the body,
     1371-1400), so an initial caret INSIDE a plus body is reached
     while atStart and stays line-anchoring (`(?:^\n)+` FALSE, V1;
     lazy V3 and possessive V5 identical). A STAR or QUESTION of any
     flavor places its branch/save guard BEFORE the body (doOpt
     859-871, doNGOpt 873-895, doStar 899-986, doNGStar 988-1002,
     doPossessiveStar 1402-1433, doPossessiveOpt 1436-1463), which is
     FORM-EQUIVALENT to the {0,} and {0,1} interval spellings - the
     leading guard destroys atStart identically in both spellings
     (probes V7/V8/V9/V10 all match only the empty string [0,0)).
     An INTERVAL {min,max} of any flavor places its loop INIT BEFORE
     the body (compileInterval's CTR_INIT/CTR_INIT_NG at the block
     top, 2440-2504) or is inlined/eliminated per the flavor dispatch
     below, so `(?:^\n){1,}` - unlike `(?:^\n)+` - clears atStart
     before a caret inside the body, which becomes a bare mid-pattern
     caret asserted at the CRLF interior (TRUE [2,3), V2; lazy V4 and
     possessive V6 identical; mu FALSE; routed V2r/V6r identical).
     Rev 8's form-blind record ({min,max,flavor} only, plus and {1,}
     indistinguishable) is rejected by the V1-vs-V2, V3-vs-V4 and
     V5-vs-V6 pairs (alternative 2h). FLAVOR dispatch WITHIN the
     INTERVAL form: ORDINARY
     (doInterval, 1047-1053) tries inline elimination first
     (compileInlineInterval, 2506-2560): an unbounded or upper > 10
     count falls back to a counted loop; {0}/{0,0} DELETES the unit or
     whole group block together with its capture bookkeeping and
     releases no events; upper == 1 is inline even on a multi-op block;
     a single-op small {min,max} emits min mandatory copies plus
     (max-min) branch-guarded optional copies, each a separate
     single-char unit (D8/D9: `^\n{2}x` is two single chars, while the
     textually adjacent `^\n\nx` is one run); a min>0 quantifier on a
     GROUP emits the group body once as ordinary content wrapped in
     loop control events, and a min==0 loop additionally forwards the
     current minimum past the loop (CTR_INIT case 3046-3070) so content
     after it can still be initial. LAZY (doNGInterval, 1089-1092)
     ALWAYS emits a counted loop (CTR_INIT_NG/CTR_LOOP_NG directly,
     never inline): even {0}? keeps its loop events, whose init clears
     atStart while a min==0 init still forwards the current minimum
     past the loop (the shared CTR case). POSSESSIVE
     (doPossessiveInterval, 1055-1087) ALWAYS emits the counted loop
     (CTR_INIT/CTR_LOOP) wrapped in STO_SP/LD_SP state-save events
     (themselves atStart-neutral, accumulator 2751-2754), never inline:
     even {0}+ keeps the loop. The discrimination is load-bearing:
     `x{0}^\n` deletes its prefix and the caret stays line-anchoring
     (Q1 FALSE), while `x{0}?^\n` and `x{0}+^\n` keep a counted loop
     whose init clears atStart, so the caret is a bare mid-pattern one
     matching at the CRLF interior (Q2/Q3 TRUE at [2,3); routed Q2r/Q3r
     identical; mu rows FALSE - icu-rev8-probe.log). Rev 7's rule
     ("lazy forms classify identically", zero-count deletion universal)
     is rejected by these probes (alternative 2g).
     FAILURE EVENT (round-8 finding F2): an empty resolved class emits a
     failure event (URX_BACKTRACK, compileSet case 0 at
     regexcmp.cpp:2407). In the walk it contributes NO initial content
     (numInitialStrings is untouched), no literal run and a killed
     branch: the branch-forwarded minimum continues from whatever other
     paths supply past the failure op (accumulator BACKTRACK case
     2970-2975, which reads the length forwarded to the following op; a
     pattern whose alternatives reduce to one literal string therefore
     keeps its exemption - `^(?:[^\x00-\x{10ffff}]|\nx)` TRUE [2,4),
     probe E1). The event itself marks its own path atStart-destroyed
     (atStart = false on any path THROUGH the failure; such a path can
     never match at runtime), and the class OPEN still SEALS any
     pending run opened before it per the sealing rule below. Rev 8's
     omission (an empty class treated as consuming +2 or as opaque) is
     rejected by this probe (alternative 2i): the +2 counting would
     raise numInitialStrings to 3 and strip the exemption.
     INITIAL-STRING ACCUMULATOR (mirroring matchStartType 2691-3188):
     the walk carries currentLen, the MINIMUM match length over all
     paths reaching the current position; branch events propagate the
     current minimum into every alternative (the forwardedLength
     mechanics, 2954-3006), so an element is INITIAL iff some
     minimum-length-0 path reaches it, and a skipped lookaround's
     content is never initial. A case-sensitive literal RUN of >= 2
     code points encountered as initial content increments
     numInitialStrings by 1 (URX_STRING case, 3001-3020, which also
     records fInitialStringIdx/fInitialStringLen; the op itself is
     emitted only for runs of >= 2 code points, fixLiterals
     1933-1958); every other initial consuming content (single chars
     URX_ONECHAR, classes resolved to cardinality GREATER than one -
     negated or not (a negated class whose complement is ONE code
     point is a cardinality-1 singleton that joins the run, e.g. E2's
     `[^\x00-\x09\x0b-\x{10ffff}]` = {LF}), case-folded chars or
     folded runs URX_ONECHAR_I/URX_STRING_I, ICU-identity escape
     classes (URX_BACKSLASH_V for `\v` - accumulating like SETREF at
     +2, 2883-2897, never joining a run; round-8 finding F3),
     property classes,
     any-char) increments it by 2; zero-width constructs add nothing.
     A failure event (empty resolved class) contributes NOTHING to
     numInitialStrings per the FAILURE EVENT rule above.
     Runs are formed EXACTLY as ICU builds fLiteralChars with sealing
     at every compiler boundary (round-7 finding F2): literal events
     append to the pending run; a singleton class's lowered char
     appends to the NEW pending run that the class-OPEN seal just
     started (so `^[\n]x` forms one run `\nx`, while `^\n[x]` and
     `^[\n][x]` seal `\n` as a one-char unit first - probes C1/C2/C3
     and routed C1r/C2r/C3r); every sealing event named in the event
     vocabulary (class open, inline flag set even when literal-
     irrelevant, group open/close of every kind, alternation open,
     anchor and boundary assertion of EVERY kind - caret, dollar,
     `\A`, `\G`, `\Z`, `\z`, `\b`, `\B` - numbered and named
     backreference, the ICU-identity `\v`, quantifier split, pattern
     end) emits the pending run as ONE unit
     before the next event arrives: a sealed case-sensitive run of
     length >= 2 is a run unit, a sealed length-1 or folded run is a
     single-char/folded unit (emission 1933-1958). Flag and group
     boundaries therefore break exemption even without literal
     relevance (`^\n(?s)x`, `^\n(?s:x)`, `^\n(?i)x`, `^\n(?:x)` FALSE -
     C4/C5/C6/C7, routed C4r/C5r), while a seal with an empty pending
     run is a no-op (`(?s)^\nx` TRUE [2,4) - C9) and adjacency inside
     one pending run still merges (`^\nx`, `^(?:\nx)` exempt -
     S06/S14). Assertion and backreference seals break exemption the
     same way although the sealed constructs are zero-width,
     atStart-preserving and min-length-0 (round-9 finding F1):
     `^\n$\nx` and `^\n\B\nx` seal `\n` as a one-char unit before the
     tail `\nx`, so TWO initial units exist and the interior match is
     never offered (find FALSE on `a\r\n<LF>xb`, A1/A2 - although the
     engine itself matches at 2, A1-la2/A2-la2 [2,5); the
     adjacent-literal control `^\n\nx` keeps one run, exempts and
     matches [2,5), A3; routed A1r/A2r false, A3r true), and the
     numbered/named backreferences of `(?=())^\n\1x` and
     `(?=(?<e>))^\n\k<e>x` seal `\n` before `x` identically (find
     FALSE on `a\r\nxb`, B1/B3 - engine lookingAt(2) [2,4),
     B1-la2/B3-la2; the controls `(?=())^\nx` / `(?=(?<e>))^\nx`
     exempt and match [2,4), B2/B4). The wrapper-generated
     `(?m)`/`(?im)` prologues arrive
     with an empty pending run, so they are no-op seals (flag events
     only).
     THE ONE SHARED EVENT TABLE (round-9 finding F1): sealing,
     atStart behavior and content/length contributions are THREE
     INDEPENDENT effects per event kind; no column implies another
     (a zero-width, zero-length, atStart-preserving event can still
     seal - the assertions and backreferences below; a sealing event
     can still consume - the ICU-identity `\v`; a seal can stand
     next to content that joins the NEW run - the singleton class).
     The table is the complete literal-flush contract the scanner/core
     pair must reproduce (ICU cites in icu-77.1-regexcmp.cpp /
     icu-77.1-rematch.cpp unless noted):

| event kind | run SEAL (ICU literal-flush cite) | atStart effect (accumulator cite) | initial content; min/max length |
|------------|-----------------------------------|-----------------------------------|---------------------------------|
| literal code point | none - JOINS the pending run (literalChar 2410-2420) | destroys when consumed | pending run of >= 2 case-sensitive code points: ONE URX_STRING, +1 initial (accumulator 3001-3020; emission 1933-1958); sealed length-1 or folded run: +2 (ONECHAR/ONECHAR_I/STRING_I, 2902-2922/3020-3043); min +1 and max +1 per code point |
| class open | SEAL (flush before the set compiles; class-open seal boundary 1653-1655) | none by itself | none by itself |
| class close, cardinality 0 (complement arithmetic applied) | (the open already sealed) | destroys on its own dead path (BACKTRACK case 2970-2975); branch forwards its minimum from siblings | FAILURE event: no initial content, no length (round-8 F2) |
| class close, cardinality 1 (negated singletons included) | (the open already sealed) | destroys when consumed | lowered char JOINS the NEW pending run (literalChar 2410-2420); min +1; max +1 |
| class close, cardinality > 1 | (the open already sealed) | destroys when consumed | consuming set, +2 when initial (2780-2790); min +1; max +1 |
| group open/close of every kind (capture, named, noncapture, scoped-flag, atomic, lookaround) | SEAL at both boundaries (opens 465-714; close handleCloseParen 2192) | containers preserved (zero-width class); lookaround CONTENT skipped wholesale with branch forwarding (LA/LB skip 3084-3132); atomic bodies traversed | none from the boundary itself |
| inline flag set | SEAL even when literal-irrelevant (doSetMatchMode 1496) | preserved | none; min +0 |
| quantifier {FORM, min, max, flavor} | SPLIT, binds to the last code point (blockTopLoc fixLiterals(true), 2141) | form/flavor-dispatched (round-8 F1: plus control AFTER the body, interval init BEFORE, star/question leading guard) | per the invariant-8 lowering |
| alternation branch open | SEAL (doOrOperator 398) | branch/save controls destroy; minimum forwarded into every alternative (2954-3006) | per alternative content |
| anchor and boundary assertions (wide/unix caret, `\A`, `\G`, dollar kinds, `\Z`, `\z`, `\b`, `\B`) | SEAL - fixLiterals(false): caret 1132, dollar 1147, `\A` 1161, `\b` 1172, `\B` 1185, `\G` 1202, `\Z` 1261, `\z` 1266; the fixLiterals default is split=false (regexcmp.h:103) (round-9 F1) | preserved (zero-width group 2742-2757); caret kinds additionally record START_START / START_LINE only when reached atStart (2759-2770) | no initial content; min +0; max +0 |
| backreference, numbered or named | SEAL - fixLiterals(false) at 1326/1359 (round-9 F1) | preserved, possibly EMPTY (2752-2753; probes S22/S23) | no initial content; min +0; max UNBOUNDED (maxMatchLength 3548-3551) |
| ICU-identity `\v` (round-8 F3) | SEAL - doBackslashv 1236-1239 | destroys when consumed | consuming line-ending set, +2 (accumulator 2883-2897); min +1; max +1; never joins a run |
| pattern end | SEAL (doExit 1275) | n/a | n/a |

     SOUND STOPPING CONDITION: the classification is frozen once the
     minimum over live paths exceeds 0 AND atStart is false - from then
     on no element can be initial (currentLen can no longer reach 0 on
     any path), numInitialStrings cannot return to 1, and no later
     caret can become line-anchoring-initial; both outcomes are then
     final. The core is linear in the pattern size, runs once per
     compiled pattern at compile time, and computes the pattern
     minMatchLen over the whole pattern regardless of the early stop
     (the ladder needs it), so the early stop is an optimization, never
     a correctness input.
   - Descriptor derivation: `lineAnchoredStart` is true when a caret is
     reached while `atStart` holds. It is FORM-sensitive (round-8
     finding F1): a caret inside a PLUS-quantified body keeps atStart
     (the loop control follows the body), so `(?:^\n)+`, `(?:^\n)+?`
     and `(?:^\n)++` are lineAnchoredStart (V1/V3/V5 FALSE at the
     interior), while the same caret inside an INTERVAL body is
     preceded by the loop INIT and is NOT lineAnchoredStart:
     `(?:^\n){1,}`, `(?:^\n){1,}?` and `(?:^\n){1,}+` (V2/V4/V6 bare
     mid-pattern carets, TRUE at the interior).
     `stringExempt` is true exactly when
     numInitialStrings == 1 AND the pattern minMatchLen > 0 - the single
     initial content is one case-sensitive literal run and the pattern
     cannot match empty (`^\nx`, `^[\n]x`, `^(?:\nx)`, `^\nx(?:|y)`,
     `^(?:\nx)+`, `^(?:\nx){2,3}`, `^(?:\nx){1}`, `^\n\nx`,
     `(?s)^\nx`, the dead-branch alternative `^(?:[^\x00-\x{10ffff}]|\nx)`,
     the negated-singleton `^[^\x00-\x09\x0b-\x{10ffff}]x`, the
     hex-escape run `^\n\x0b` and the lookahead-prefixed
     no-backreference controls `(?=())^\nx` / `(?=(?<e>))^\nx` exempt per
     probes S06/S08/S14/R2/R4/D7/D11/D9/C9/E1/E2/X2/B2/B4; `^\n`, `^b`, `^(?:\n)x`,
     `^(\n)x`, `^[\nx]`, folded runs under mi, `^(?:\nx|y)`,
     `^(?:\nx|\nx)`, `^(?:\nxy|\nxz)`, `^(?:\nxy|)` (an empty branch
     forces minMatchLen 0), `^(?:\nx)*`, `^\nx{1}`, `^\nx?`, `^\nx+`,
     `^\nx{0}y`, `^x{0}y`, `^(?:\nx){0}y`, `^\n{2}x`, `^\n[x]`,
     `^[\n][x]`, `^\n(?s)x`, `^\n(?s:x)`, `^\n(?i)x`, `^\n(?:x)`, the
     ICU-identity escape `^\n\v`, the dead-branch single-char
     alternative `^(?:[^\x00-\x{10ffff}]|x)`, the assertion-sealed pairs
     `^\n$\nx` / `^\n\B\nx` (two initial units: the one-char `\n` head
     plus the `\nx` tail) and the backreference-sealed pairs
     `(?=())^\n\1x` / `(?=(?<e>))^\n\k<e>x` (units `\n` plus `x`) do
     not, per
     S13/S15/S07/S17/R1/D4/D5/D13/D6/D1/D15/D16/D2/R3b/D3/D8/C2/C3/C4/C5/C6/C7/X1/E3/A1/A2/B1/B3).
   - Policy: for a `lineAnchoredStart` pattern WITHOUT `stringExempt`, a
     match may not BEGIN at the CRLF interior position, on either engine
     path, for whole-subject searches (LIKE/SUBSTR/REPLACE and REPLACE
     occurrence iteration) and positional searches (INSTR/SUBSTR `pos`,
     REGEXP_REPLACE start), with the same exclusion relative to the left
     context. Candidates are still offered at position 0 and immediately
     after every completed terminator (a CRLF pair completes once, after
     the LF). Every other pattern keeps invariant 1 semantics at every
     evaluation position (the engine's normal candidate enumeration), so
     `\r^\n` matches, `^\n|z` honors the interior, `^\nx`-class patterns
     take the string start exactly as ICU's ladder, and `^[\nx]` matches
     at [3,4) only. `mu` needs no candidate policy: the CRLF interior is
     never a caret position under Unix lines anyway.
   - Engine mappings: ONE raw-pattern-text event scanner reads the
     ORIGINAL pattern on both paths (Plan items 1c/4c). On the RE2 path
     it lexes Go regexp syntax: literals and escapes (`\Q...\E` quoted
     runs, Perl class escapes, anchor escapes), class brackets resolving
     cardinality 0/1/greater with complement arithmetic at close (the
     OPEN seals; cardinality 0 records the failure event of round-8
     finding F2; cardinality 1 - negated singletons included - joins
     the new pending run), shared spellings whose engine meanings
     differ recorded by ICU identity (`\v` -> the BACKSLASH_V event of
     round-8 finding F3, never a run literal),
     capturing/noncapturing/scoped group opens and
     closes, `(?i)`/`(?-i)`/`(?m)` flag toggles, alternation,
     quantifiers with parsed FORM, min/max and flavor
     (star|plus|question|interval x ordinary|lazy|possessive, round-8
     finding F1), and anchors and boundary assertions of every kind
     (run-SEALING per round-9 finding F1 while zero-width,
     atStart-preserving and min-length-0; backreferences cannot occur
     in this dialect - they force the regexp2 route). On the
     regexp2 path it lexes the pre-translation ICU/.NET superset exactly
     as requiresRegexp2Pattern detects it (so a possessive `{n,m}+`,
     `*+`, `++` or `?+` records possessive flavor BEFORE
     translateRegexp2Pattern could rewrite it as `(?>atom{n,m})`,
     regexp_icu.go:1007-1025): lookahead/lookbehind opens record
     skipped-block events whose content can never become initial
     (mirroring ICU's LA/LB walk, 3084-3132); atomic `(?>` opens record
     transparent container events (bodies are walked and consume;
     probes S19/S20); named groups `(?<name>`/`(?P<name>` record
     capture-kind group events; backreferences (`\1`-`\9`, `\k<>`,
     `\g<>`/`\g''`) record atStart-preserving AND run-sealing
     backreference events
     (opcode Ref = 13; probes S22/S23; possibly empty, min-length +0,
     max-length unbounded - round-9 finding F1); conditionals `(?(...)...)`
     record control-flow events that destroy atStart conservatively
     (Testref = 37 appears only inside conditionals); lazy markers
     record lazy flavor and every quantifier records its FORM
     (star/plus/question/interval) alongside min/max; classes resolve
     with the same cardinality arithmetic as the RE2 dialect (the
     failure event included); `\v` records the ICU BACKSLASH_V event
     exactly as on the RE2 route (the classification contract is
     MySQL's, hence ICU's), while braced-hex escapes the fork parses
     (`\x{...}`, fork syntax/parser.go:1701-1707) and their octal/hex
     equivalents record the resolved literal code point; `\X`,
     `\U................`, `\cX`, free-spacing `(?x)` regions and
     `(?#)` comments
     are lexed to their event kinds; `\Q...\E` content records literal
     events. The Rev 6 node-type rules (ntRequire/ntPrevent skipped
     wholesale including nesting; ntGreedy/ntCapture traversed;
     ntTestgroup conservative; exact Ref/Testref naming) survive as
     these token-to-event-kind rules, so the round-5 F3 ambiguity
     (identical writer prologues for lookarounds and consuming atomic
     groups, fork syntax/writer.go:283-317) never arises - the scanner
     reads source intent, not generated code, and the round-6 F1
     requirement (capture before the fork's eager reductions) is met
     one stage earlier, at the text the reductions are produced FROM.
     The wrapper-generated `(?m)`/`(?im)` prologues pass through as
     flag events with an empty pending run (no-op seals, probe C9).
     The scanner is conservative on BOTH paths: a construct outside the
     mirrored rule set emits an opaque consuming+branch event (destroys
     atStart, adds 2 when initial), which can only suppress
     line-anchoring or the exemption and so degrades to today's
     candidate enumeration; the guard tables pin the direction.
9. Admission preservation by construction on the regexp2 path: the engine
   obtains wide assertions and the candidate policy natively (vendored
   engine, per-compiled-pattern line mode set before publication), so the
   compiled program IS the user pattern's compiled program; the
   classification descriptor is metadata computed alongside codegen, never
   an input to it. Every SQL-visible accept/reject decision input -
   pattern size, translated expression bytes, `code.TrackCount`,
   evaluation states, memory total, nullable/capture budgets,
   cached-compiled estimates and the per-evaluation global reservation -
   is therefore the same VALUE as the plain pattern for every subject:
   preserved as an identity, not bounded by a multiplier. There is no
   engine layer, no surcharge accounting, no adjacent-anchor dedupe, no
   per-subject expression selection - and no anchor rewrite (the merged
   rewrite's measured production deviation - TrackCount 7->37 for
   `(?m)^b(?=b)`, 7->1671 anchor-dense, 3->33 bare caret; the 30,000-rune
   subject at 50,131,671 states REJECTED, the 74,000-rune subject at
   2,738,037 REJECTED, the 50,000-rune review case erroring in production
   - is removed with it). The pinned evidence is equality: mode-identity
   UTs compile each pinned pattern in Unix and wide modes and demand
   identical TrackCount, code length and expression bytes; production-path
   identity UTs compose the translated expression through
   `regexpPatternWithPureMatchType` + `translateRegexp2Pattern` exactly as
   the route does and demand the plain TrackCount pins (7/7/3/5); the
   deterministic overlap/reject/release controls pin today's global
   reservation arithmetic as the contract. No admitted workload becomes an
   error, no rejected workload becomes accepted, and the rejection error
   classes are unchanged.

Oracle tables for the repair (equal the versioned harness rev10 assertions;
probe IDs T/S refer to icu-rev6-probe.log, R/D/P refer to
icu-rev7-probe.log, Q/C refer to icu-rev8-probe.log, V/E/X refer to
icu-rev9-probe.log and A/B (with -la2 lookingAt rows) refer to
icu-rev10-probe.log, the five live
ICU 77.1 runs cited in the change log).

Separator matrix, subject `a<sep>b`, pattern `^b`:

| sep | bytes/subject | m: LIKE/INSTR/SUBSTR/REPLACE | mu: LIKE |
|-----|---------------|------------------------------|----------|
| CR  | a 0D b        | 1 / 3 / b / a0DX             | 0 |
| LF  | a 0A b        | 1 / 3 / b / a0AX             | 1 |
| VT  | a 0B b        | 1 / 3 / b / a0BX             | 0 |
| FF  | a 0C b        | 1 / 3 / b / a0CX             | 0 |
| CRLF| a 0D0A b      | 1 / 4 / b / a0D0AX           | 1 |
| NEL | a C285 b      | 1 / 3 / b / aC285X           | 0 |
| LS  | a E280A8 b    | 1 / 3 / b / aE280A8X         | 0 |
| PS  | a E280A9 b    | 1 / 3 / b / aE280A9X         | 0 |

Assertion-split counterexamples (consuming patterns, subject `a\r\nb`;
regexp2 probe variants use a `bb` tail):

| pattern  | m    | mu   | basis                                        |
|----------|------|------|----------------------------------------------|
| `\r^\n`  | true | false| caret holds between CR and LF (URX_CARET_M); Unix caret is LF-only |
| `\r$\n`  | false| true | dollar fails between CR and LF (URX_DOLLAR_M); Unix dollar has no CR exclusion (URX_DOLLAR_MD) |

Binary controls (subject bytes `61 85 62` = `a`,0x85,`b`; byte 0B/0C rows
use the corresponding separators):

| subject       | match_type | expected | basis                                  |
|---------------|------------|----------|----------------------------------------|
| binary 0x85   | m          | false    | CP-1252 facade: 0x85 is U+2026, not NEL; amends merged lines.go:36/38 |
| binary 0x85   | mi         | false    | same mapping under case folding        |
| binary CR/VT/FF| m / mi    | true     | ASCII terminators pass through in every alphabet |
| binary CR/VT/FF| mu        | false    | Unix lines recognize LF only           |

Search-candidate controls (green guards on base; each discriminates a naive
wide-caret repair; ICU-derived unless noted):

| expression                                        | m     | mu    | basis |
|---------------------------------------------------|-------|-------|-------|
| LIKE `^\n` on `a\r\nb`                            | false | false | START_LINE candidate loop skips the CRLF interior (rematch.cpp find lines 802-881, 1072-1142); probe S01 |
| LIKE `^$` on `a\r`                                | false | false | URX_CARET_M requires a following character after a non-LF terminator; probe T10 |
| LIKE `^$` on `a\n`                                | true  | true  | documented LF-at-EOF caret legacy (deviation, invariant 4; ICU T12 would be false) |
| INSTR `^\n` on `a\r\nb` from pos 2                | 0     | 0     | positional search keeps the same candidate exclusion |
| INSTR `^b` on `a\r\nb` from pos 2                 | 4     | 4     | candidate after CRLF pair exists and matches |
| LIKE `^\n(?=b)` on `a\r\nbb` (regexp2-routed)    | false | false | identical candidate contract on the regexp2 path |
| LIKE `\r^\n` on `a\r\nb` (mid-match contrast)    | true  | false | consuming prefix reaches the interior mid-match (defect row, red on base) |

Structural-start guards (green on base - the current engines answer them
by accident; each one fails under a naive wide-caret repair, a token-level
start analysis, or classification on optimized engine output):

| expression                                      | expected | basis |
|-------------------------------------------------|----------|-------|
| LIKE `(^\n)` on `a\r\nb`, m                     | false    | capture markers preserve atStart -> START_LINE (probe S03) |
| LIKE `(?:^\n)` on `a\r\nb`, m                   | false    | zero-width group preserves atStart -> START_LINE (S04) |
| LIKE `(?m:^\n)` on `a\r\nb`, m                  | false    | scoped flag group - same |
| LIKE `\B^\n` on `a\r\nb`, m                     | false    | boundary assertion preserves atStart (S05) |
| LIKE `^[\n\r]x` on `a\r\nxb`, m                 | false    | VERIFIED non-singleton SETREF negative control (finding F1; probe S11): a real set is a char/set start that never overrides START_LINE here |
| LIKE `^\nx` on `a\r\nxb`, mi                    | false    | folded run seeds chars only; URX_STRING_I never selects START_STRING (S07) |
| LIKE `^\n1` on `a\r\n1b`, mi                    | false    | same fold rule, digit pair (S17; distinct from the m-true defect row) |
| LIKE `^(?:\n)x` on `a\r\nxb`, m                 | false    | group boundary splits the literal run (finding F2; probe S13) |
| LIKE `^(\n)x` on `a\r\nxb`, m                   | false    | capture boundary splits the run (S15) |
| LIKE `^\n\|z` on `a\r\nb`, mu                   | false    | Unix caret is LF-only at the interior |
| LIKE `(?!(\r))^\n` on `a\r\nb`, m               | false    | negative lookahead skipped; caret stays line-anchoring-initial (S24) |
| LIKE `()\1^\n` on `a\r\nb`, m                   | false    | empty backreference preserves atStart; CRLF interior is not a candidate (S23) |
| LIKE `^` on `a<sep>` for sep in {CR,VT,FF,NEL}  | false    | caret needs a following character after a non-LF terminator (T10/T11 + VT/FF analogs) |
| LIKE `^` on `a\n`                               | true     | LF-at-EOF legacy surveillance (ICU T12 false; MatrixOne keeps true, invariant 4) |
| INSTR `(^\n)` on `a\r\nb` from pos 2, m         | 0        | capture-preserved candidate exclusion, positional |
| LIKE `(^\n)(?=b)` on `a\r\nbb`, m (regexp2-routed) | false | same structural rule on the vendored engine |
| LIKE `^(?:\n)x(?=x)` on `a\r\nxb`, m (regexp2-routed) | false | F2 pair on the second engine |
| LIKE `^\n1(?=2)` on `a\r\n12`, mi (regexp2-routed)  | false | fold pair on the second engine |
| LIKE `(?=[\s\S])^(?:\n)x` on `a\r\nxb`, m       | false    | composite: skipped lookahead + split run (S26) |
| LIKE `(?=[\s\S])^\n1` on `a\r\n1b`, mi          | false    | composite fold (S28) |

Structural-start defects (red on base; live ICU 77.1 probe rows; the
`^[\nx]` family and `()\1^\n`-on-`x\n\n` are green-on-base controls that
pin the corrected contract against over-offering):

| expression                                      | expected | basis |
|-------------------------------------------------|----------|-------|
| LIKE `^\n\|z` on `a\r\nb`, m                    | true     | alternation destroys atStart -> set start offers the interior; caret holds there (S18) |
| INSTR `^\n\|z` on `a\r\nb` from pos 2, m        | 3        | same, positional search |
| SUBSTR `^\n\|z` on `a\r\nb`, m                  | LF       | same |
| REPLACE `^\n\|z` -> `X` on `a\r\nb`, m          | `a\rXb`  | same |
| LIKE `^\nx` on `a\r\nxb`, m                     | true     | start-string priority (literal run >= 2); interior candidate, truthful caret (S06) |
| LIKE `^[\n]x` on `a\r\nxb`, m                   | true     | finding F1: singleton set lowers to literal LF and joins the run (literalChar, regexcmp.cpp:2410-2420); probe S08 [2,4) |
| INSTR `^[\n]x` on `a\r\nxb`, m                  | 3        | same |
| INSTR `^[\n]x` on `a\r\nxb` from pos 2, m       | 3        | same, positional |
| SUBSTR `^[\n]x` on `a\r\nxb`, m                 | `\nx`    | same |
| REPLACE `^[\n]x` -> `X` on `a\r\nxb`, m         | `a\rXb`  | same |
| LIKE `(?=[\s\S])^[\n]x` on `a\r\nxb`, m (regexp2-routed) | true | F1 composite after skipped lookahead (S25) |
| LIKE `^[\nx]` on `a\r\nxb`, m                   | true     | non-singleton set: START_LINE candidates only; matches at [3,4) (S10) - control |
| INSTR `^[\nx]` on `a\r\nxb`, m                  | 4        | position pin: the line start at 3, never the interior (control) |
| SUBSTR `^[\nx]` on `a\r\nxb`, m                 | `x`      | same (control) |
| REPLACE `^[\nx]` -> `X` on `a\r\nxb`, m         | `a\r\nXb`| same (control) |
| LIKE `^(?:\nx)` on `a\r\nxb`, m                 | true     | run contiguous inside one group (S14) |
| INSTR `^(?:\nx)` on `a\r\nxb`, m                | 3        | same |
| LIKE `^\n1` on `a\r\n1b`, m                     | true     | case-sensitive 2-char run (S16) |
| LIKE `(?=[\s\S])^\n1` on `a\r\n1b`, m           | true     | composite (S27) |
| LIKE `()\1^\n` on `x\n\n`, m                    | true     | BACKREF preserves atStart; START_LINE offers position 2 (S22) - control |
| INSTR `()\1^\n` on `x\n\n`, m                   | 3        | same (control); pattern forces the regexp2 route |
| LIKE `(?:^\n\|z)(?=b)` on `a\r\nbb`, m (regexp2-routed) | true | same ladder on the vendored engine |
| LIKE `(\r)^\n` on `a\r\nb`, m                   | true     | capture body consumes CR; generic candidate search; mid-match caret holds (S20) |
| LIKE `(?>(\r))^\n` on `a\r\nb`, m               | true     | atomic body traversed and consumes CR (finding F3; probe S19 [1,3)) |
| INSTR `(?>(\r))^\n` on `a\r\nb`, m              | 2        | same |
| SUBSTR `(?>(\r))^\n` on `a\r\nb`, m             | `\r\n`   | same |
| REPLACE `(?>(\r))^\n` -> `X` on `a\r\nb`, m     | `aXb`    | same |
| LIKE `(?=\r)\r^\n` on `a\r\nb`, m               | true     | lookahead skipped (zero-width), literal CR consumes (S21) |

Branch-aware accumulation and quantifier-lowering defects (Rev 7; red on
base; probe rows R*/D* in icu-rev7-probe.log, measured on live ICU 77.1;
REPLACE replaces the first match via occurrence 1; positions are 1-based
SQL positions):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `^\nx(?:|y)` on `a\r\nxb`, m             | true     | one initial case-sensitive string completed before the alternation (R2, [2,4)) |
| INSTR `^\nx(?:|y)` on `a\r\nxb`, m            | 3        | positional pin |
| SUBSTR `^\nx(?:|y)` on `a\r\nxb`, m           | `\nx`   | same |
| REPLACE `^\nx(?:|y)` -> `X` on `a\r\nxb`, m   | `a\rXb` | same |
| LIKE `(?=[\s\S])^\nx(?:|y)` on `a\r\nxb`, m (regexp2-routed) | true | prefix-invariance (P2, [2,4)) |
| LIKE `^(?:\nx)+` on `a\r\nxb`, m              | true     | min>0 loop body counts once (R4, [2,4)) |
| INSTR `^(?:\nx)+` on `a\r\nxb`, m             | 3        | positional |
| SUBSTR / REPLACE as above                        | `\nx` / `a\rXb` | same |
| LIKE `^(?:\nx)+?` on `a\r\nxb`, m             | true     | lazy plus identical (D14) |
| LIKE `(?=[\s\S])^(?:\nx)+` on `a\r\nxb`, m (regexp2-routed) | true | (P4, [2,4)) |
| LIKE `^(?:\nx){2,3}` on `a\r\nx\nxb`, m      | true     | counted min>0 loop body counts (D7, [2,6)) |
| INSTR `^(?:\nx){2,3}` on `a\r\nx\nxb`, m     | 3        | positional |
| SUBSTR `^(?:\nx){2,3}` on `a\r\nx\nxb`, m    | `\nx\nx` | same |
| REPLACE `^(?:\nx){2,3}` -> `X` on `a\r\nx\nxb`, m | `a\rXb` | same |
| LIKE `(?=[\s\S])^(?:\nx){2,3}` on `a\r\nx\nxb`, m | true | routed variant of D7 |
| LIKE `^\nx(?:y|z)` on `a\r\nxyb`, m         | true     | alternation after the completed string (D10, [2,5)) |
| INSTR `^\nx(?:y|z)` on `a\r\nxyb`, m        | 3        | positional |
| SUBSTR `^\nx(?:y|z)` on `a\r\nxyb`, m       | `\nxy`  | same |
| REPLACE `^\nx(?:y|z)` -> `X` on `a\r\nxyb`, m | `a\rXb` | same |
| LIKE `^(?:\nx){1}` on `a\r\nxb`, m            | true     | {1} on a group is identity (D11, [2,4)) |
| LIKE `^\n\nx` on `a\r\n\nxb`, m             | true     | textually adjacent run merges (D9, [2,5)) |
| INSTR `^\n\nx` on `a\r\n\nxb`, m            | 3        | positional |
| SUBSTR `^\n\nx` on `a\r\n\nxb`, m           | `\n\nx` | same |
| REPLACE `^\n\nx` -> `X` on `a\r\n\nxb`, m   | `a\rXb` | same |
| LIKE `(?=[\s\S])^\n\nx` on `a\r\n\nxb`, m | true     | routed variant of D9 |
| LIKE `x{0,2}^\n` on `a\r\nb`, m               | true     | optional-count prefix is control-flow: bare mid-pattern caret holds at the interior (D12, [2,3)) |
| INSTR `x{0,2}^\n` on `a\r\nb`, m              | 3        | positional |
| SUBSTR `x{0,2}^\n` on `a\r\nb`, m             | `\n`    | same |
| REPLACE `x{0,2}^\n` -> `X` on `a\r\nb`, m     | `a\rXb` | same |
| LIKE `x{0,2}^\n` on `a\r\nx\nxb`, m          | true     | (D12b, [2,3)) |

Branch-aware accumulation and quantifier-lowering guards and controls
(green on base; each fails under the Rev 6 first-run rule, a flat
run-contiguity model with no quantifier split, or an unconditional
quantifier-destroyer rule; probe rows R*/D* in icu-rev7-probe.log):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `^(?:\nx|y)` on `a\r\nxb`, m             | false    | second branch contributes initial content, numInitialStrings = 3 (R1) |
| INSTR `^(?:\nx|y)` on `a\r\nxb`, m            | 0        | positional pin (over-exemption would return 3) |
| LIKE `(?=[\s\S])^(?:\nx|y)` on `a\r\nxb`, m (regexp2-routed) | false | (P1) |
| LIKE `^(?:\nx|\nx)` on `a\r\nxb`, m          | false    | two initial strings even when identical (D4) |
| LIKE `^(?:\nxy|\nxz)` on `a\r\nxyb`, m     | false    | two initial strings (D5) |
| LIKE `x{0}^\n` on `a\r\nb`, m                 | false    | zero-count deletion preserves atStart (R3) |
| LIKE `(?=[\s\S])x{0}^\n` on `a\r\nb`, m (regexp2-routed) | false | (P3) |
| LIKE `^x{0}y` on `a\r\nyb`, m                  | true     | {0} deletion leaves only y; matches at the genuine line start 3 (R3b, [3,4)) - control |
| INSTR `^x{0}y` on `a\r\nyb`, m                 | 4        | positional pin: the y, never the interior (control) |
| SUBSTR `^x{0}y` on `a\r\nyb`, m                | `y`      | same (control) |
| REPLACE `^x{0}y` -> `X` on `a\r\nyb`, m        | `a\r\nXb`| same (control) |
| LIKE `^x{0}y` on `a\r\nb`, m                   | false    | no y at any caret position (R3c) |
| LIKE `^\nx{1}` on `a\r\nxb`, m                | false    | any quantifier splits the pending run, even {1} (D1) |
| LIKE `^\nx?` on `a\r\nxb`, m                  | false    | ? splits the run (D15) |
| LIKE `^\nx+` on `a\r\nxb`, m                  | false    | + splits the run (D16) |
| LIKE `^\nx{0}y` on `a\r\nyb`, m               | false    | the split precedes the zero-count deletion (D2) |
| LIKE `^(?:\nx){0}y` on `a\r\nyb`, m           | true     | zero-count group deleted wholesale; y at line start 3 (D3, [3,4)) - control |
| INSTR `^(?:\nx){0}y` on `a\r\nyb`, m          | 4        | positional pin (control) |
| LIKE `^\n{2}x` on `a\r\n\nxb`, m             | false    | inlined single-char copies never form a run (D8) |
| LIKE `^(?:\nx)*` on `a\r\nxb`, m              | true ([0,0))| minMatchLen == 0 blocks the string exemption; empty match at position 0, never the interior (D6) - control |
| INSTR `^(?:\nx)*` on `a\r\nxb`, m             | 1        | position pin: empty match at 0, never [2,4) (control) |
| SUBSTR `^(?:\nx)*` on `a\r\nxb`, m            | `` (empty) | same (control) |
| LIKE `^(?:\nx)*` on `a\r\nxb`, mu             | true ([0,0))| unix mode identical (D6-mu) - control |
| LIKE `^(?:\nxy|)` on `a\r\nxyb`, m          | true ([0,0))| one initial string but the empty branch forces minMatchLen 0; empty match at position 0 only (D13) - control |
| INSTR `^(?:\nxy|)` on `a\r\nxyb`, m         | 1        | position pin: the empty match at position 0, never [2,5) (control) |
| SUBSTR `^(?:\nxy|)` on `a\r\nxyb`, m        | `` (empty) | same (control) |
| LIKE `^\nx(?:|y)` on `a\r\nxb`, mu            | false    | unix caret blocks the interior even for an exempt string (R2-mu) |
| LIKE `^(?:\nx){2,3}` on `a\r\nx\nxb`, mu     | false    | (D7-mu) |
| LIKE `^\n\nx` on `a\r\n\nxb`, mu            | false    | (D9-mu) |
| LIKE `x{0,2}^\n` on `a\r\nb`, mu              | false    | (D12-mu) |

Quantifier-flavor defects (Rev 8; red on base; probe rows Q* in
icu-rev8-probe.log, measured on live ICU 77.1; REPLACE replaces the
first match; positions are 1-based SQL positions):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `x{0}?^\n` on `a\r\nb`, m                | true     | lazy zero-count KEEPS a counted loop (doNGInterval, regexcmp.cpp:1089-1092); CTR_INIT_NG clears atStart, caret is a bare mid-pattern one (Q2, [2,3)) |
| INSTR `x{0}?^\n` on `a\r\nb`, m               | 3        | positional pin |
| SUBSTR `x{0}?^\n` on `a\r\nb`, m              | `\n`    | same |
| REPLACE `x{0}?^\n` -> `X` on `a\r\nb`, m      | `a\rXb` | same |
| LIKE `x{0}+^\n` on `a\r\nb`, m                | true     | possessive zero-count keeps STO_SP + counted loop (doPossessiveInterval, 1055-1087); CTR_INIT clears atStart, STO_SP/LD_SP atStart-neutral (Q3, [2,3)) |
| INSTR `x{0}+^\n` on `a\r\nb`, m               | 3        | positional pin |
| SUBSTR `x{0}+^\n` on `a\r\nb`, m              | `\n`    | same |
| REPLACE `x{0}+^\n` -> `X` on `a\r\nb`, m      | `a\rXb` | same |
| LIKE `(?=[\s\S])x{0}?^\n` on `a\r\nb`, m (regexp2-routed) | true | routing-prefix invariance (Q2r, [2,3)) |
| INSTR `(?=[\s\S])x{0}?^\n` on `a\r\nb`, m     | 3        | positional |
| SUBSTR / REPLACE as above                      | `\n` / `a\rXb` | same |
| LIKE `(?=[\s\S])x{0}+^\n` on `a\r\nb`, m (regexp2-routed) | true | possessive flavor recorded pre-translation, before the `(?>x{0})` rewrite (Q3r, [2,3)) |
| INSTR `(?=[\s\S])x{0}+^\n` on `a\r\nb`, m     | 3        | positional |
| SUBSTR / REPLACE as above                      | `\n` / `a\rXb` | same |

Quantifier-flavor guards and controls (green on base; contract FALSE
rows whose base FALSE already agrees; each discriminates against Rev
7's flavor-blind zero-count deletion):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `x{0}^\n` on `a\r\nb`, m                 | false    | ordinary zero-count deletes the unit, caret stays line-anchoring (Q1, carried R3) |
| INSTR `x{0}^\n` on `a\r\nb`, m                | 0        | positional pin |
| LIKE `(?=[\s\S])x{0}^\n` on `a\r\nb`, m (regexp2-routed) | false | (Q1r, carried P3) |
| LIKE `x{0}^\n` on `a\r\nb`, mu                | false    | (Q1-mu) |
| LIKE `x{0}?^\n` on `a\r\nb`, mu               | false    | unix caret never asserts at the interior even when offered (Q2-mu) |
| LIKE `x{0}+^\n` on `a\r\nb`, mu               | false    | (Q3-mu) |

Literal-run sealing defects (Rev 8; red on base; probe rows C* in
icu-rev8-probe.log, measured on live ICU 77.1):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `^[\n]x` on `a\r\nxb`, m                 | true     | class OPEN seals an EMPTY pending run, so the lowered singleton joins the NEW run with x (C1, [2,4)) - carried from S08, kept adjacent to its directional pair |
| INSTR `^[\n]x` on `a\r\nxb`, m                | 3        | positional pin |
| SUBSTR `^[\n]x` on `a\r\nxb`, m               | `\nx`   | same |
| REPLACE `^[\n]x` -> `X` on `a\r\nxb`, m       | `a\rXb` | same |
| LIKE `(?=[\s\S])^[\n]x` on `a\r\nxb`, m (regexp2-routed) | true | (C1r, carried S25, [2,4)) |
| INSTR `(?=[\s\S])^[\n]x` on `a\r\nxb`, m      | 3        | positional |
| LIKE `(?s)^\nx` on `a\r\nxb`, m               | true     | empty-pending seal is a no-op (C9, [2,4)) |
| INSTR `(?s)^\nx` on `a\r\nxb`, m              | 3        | positional |
| SUBSTR `(?s)^\nx` on `a\r\nxb`, m             | `\nx`   | same |
| REPLACE `(?s)^\nx` -> `X` on `a\r\nxb`, m     | `a\rXb` | same |

Literal-run sealing guards and controls (green on base; each is FALSE
under ICU's boundary sealing and would run TRUE under Rev 7's linear
adjacency merge, which merges the sealed runs):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `^\n[x]` on `a\r\nxb`, m                 | false    | class OPEN seals the pending `\n` as a one-char unit before the singleton lowers (doSetBegin, regexcmp.cpp:1653-1655) (C2) |
| INSTR `^\n[x]` on `a\r\nxb`, m                | 0        | positional pin (over-exemption would return 3) |
| LIKE `^[\n][x]` on `a\r\nxb`, m               | false    | the second class open seals the first lowered singleton (C3) |
| INSTR `^[\n][x]` on `a\r\nxb`, m              | 0        | positional pin |
| LIKE `(?=[\s\S])^\n[x]` on `a\r\nxb`, m (regexp2-routed) | false | routed invariance (C2r) |
| LIKE `(?=[\s\S])^[\n][x]` on `a\r\nxb`, m (regexp2-routed) | false | (C3r) |
| LIKE `^\n(?s)x` on `a\r\nxb`, m               | false    | inline flag set seals even with no literal relevance (doSetMatchMode, 1494-1505) (C4) |
| LIKE `^\n(?s:x)` on `a\r\nxb`, m              | false    | scoped-flag group open seals (doMatchModeParen, 1507-1527) (C5) |
| LIKE `^\n(?i)x` on `a\r\nxb`, m               | false    | flag set seals and ONECHAR_I is +2 (C6) |
| LIKE `^\n(?:x)` on `a\r\nxb`, m               | false    | a plain noncapture group open seals the pending run (C7) |
| LIKE `(?=[\s\S])^\n(?s)x` on `a\r\nxb`, m (regexp2-routed) | false | (C4r) |
| LIKE `(?=[\s\S])^\n(?s:x)` on `a\r\nxb`, m (regexp2-routed) | false | (C5r) |
| LIKE `^[\n]x` on `a\r\nxb`, mu                | false    | unix caret never asserts at the interior (C1-mu) |
| LIKE `^\n[x]` on `a\r\nxb`, mu                | false    | (C2-mu) |
| LIKE `^[\n][x]` on `a\r\nxb`, mu                | false    | (C3-mu) |

Quantifier-form defects (Rev 9; red on base; probe rows V* in
icu-rev9-probe.log, measured on live ICU 77.1; REPLACE replaces the
first match; positions are 1-based SQL positions; subject `a\r\nb`):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `(?:^\n){1,}` on `a\r\nb`, m               | true     | an interval places its loop INIT BEFORE the body; CTR_INIT clears atStart, so the caret is a bare mid-pattern one (compileInterval, regexcmp.cpp:2440-2504; accumulator 3046-3071) (V2, [2,3)) |
| INSTR `(?:^\n){1,}` on `a\r\nb`, m              | 3        | positional pin |
| SUBSTR `(?:^\n){1,}` on `a\r\nb`, m             | `\n`    | same |
| REPLACE `(?:^\n){1,}` -> `X` on `a\r\nb`, m     | `a\rXb` | same |
| LIKE `(?:^\n){1,}?` on `a\r\nb`, m             | true     | lazy interval keeps its own loop INIT (V4, [2,3)) |
| INSTR `(?:^\n){1,}?` on `a\r\nb`, m           | 3        | positional pin |
| SUBSTR / REPLACE as above                        | `\n` / `a\rXb` | same |
| LIKE `(?:^\n){1,}+` on `a\r\nb`, m             | true     | possessive interval keeps CTR_INIT plus atStart-neutral STO_SP/LD_SP (V6, [2,3)); natively regexp2-routed |
| INSTR `(?:^\n){1,}+` on `a\r\nb`, m           | 3        | positional pin |
| SUBSTR / REPLACE as above                        | `\n` / `a\rXb` | same |
| LIKE `(?=[\s\S])(?:^\n){1,}` on `a\r\nb`, m (regexp2-routed) | true | routing-prefix invariance (V2r, [2,3)) |
| INSTR `(?=[\s\S])(?:^\n){1,}` on `a\r\nb`, m | 3        | positional |
| LIKE `(?=[\s\S])(?:^\n){1,}+` on `a\r\nb`, m (regexp2-routed) | true | possessive FORM recorded pre-translation (V6r, [2,3)) |
| INSTR `(?=[\s\S])(?:^\n){1,}+` on `a\r\nb`, m | 3       | positional |

Quantifier-form guards and controls (green on base; contract FALSE or
empty-match rows whose base answers already agree; each discriminates
against Rev 8's form-blind {min,max,flavor} record, which cannot
separate the pairs; the equivalence rows pin that star/question and
their interval spellings classify identically):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `(?:^\n)+` on `a\r\nb`, m                  | false    | a plus loop appends its control AFTER the body (doPlus, regexcmp.cpp:795-845); the caret inside keeps atStart and stays line-anchoring (V1) |
| INSTR `(?:^\n)+` on `a\r\nb`, m                 | 0        | positional pin |
| LIKE `(?:^\n)+?` on `a\r\nb`, m               | false    | lazy plus trailing STATE_SAVE (doNGPlus, 847-856) (V3) |
| LIKE `(?:^\n)++` on `a\r\nb`, m               | false    | possessive plus: atStart-neutral STO_SP at the top, control after the body (doPossessivePlus, 1371-1400) (V5); natively regexp2-routed |
| LIKE `(?=[\s\S])(?:^\n)+` on `a\r\nb`, m (regexp2-routed) | false | (V1r) |
| LIKE `(?=[\s\S])(?:^\n)++` on `a\r\nb`, m (regexp2-routed) | false | (V5r) |
| LIKE `(?:^\n){1,}` on `a\r\nb`, mu            | false    | unix caret never asserts at the interior even when offered (V2-mu) |
| LIKE `(?:^\n){1,}?` on `a\r\nb`, mu          | false    | (V4-mu) |
| LIKE `(?:^\n){1,}+` on `a\r\nb`, mu          | false    | (V6-mu) |
| LIKE `(?:^\n)*` on `a\r\nb`, m                | true ([0,0)) | leading branch/save guard: FORM-EQUIVALENT to {0,} (doStar, 899-986); empty match at position 0, never the interior (V7) - control |
| INSTR `(?:^\n)*` on `a\r\nb`, m              | 1        | position pin (control) |
| LIKE `(?:^\n){0,}` on `a\r\nb`, m            | true ([0,0)) | interval spelling of the same classification (V8) - control |
| INSTR `(?:^\n){0,}` on `a\r\nb`, m          | 1        | position pin (control) |
| LIKE `(?:^\n)?` on `a\r\nb`, m               | true ([0,0)) | leading guard: FORM-EQUIVALENT to {0,1} (doOpt, 859-871) (V9) - control |
| INSTR `(?:^\n)?` on `a\r\nb`, m             | 1        | position pin (control) |
| LIKE `(?:^\n){0,1}` on `a\r\nb`, m          | true ([0,0)) | interval spelling (V10) - control |
| INSTR `(?:^\n){0,1}` on `a\r\nb`, m        | 1        | position pin (control) |

Empty-class defects (Rev 9; red on base; probe rows E* in
icu-rev9-probe.log, measured on live ICU 77.1; subject `a\r\nxb`):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `^(?:[^\x00-\x{10ffff}]|\nx)` on `a\r\nxb`, m | true | the empty resolved class emits a failure event (URX_BACKTRACK, compileSet case 0, regexcmp.cpp:2407) contributing NO initial content, so the sole initial string `\nx` still exempts (accumulator 2970-2975) (E1, [2,4)) |
| INSTR `^(?:[^\x00-\x{10ffff}]|\nx)` on `a\r\nxb`, m | 3   | positional pin |
| SUBSTR `^(?:[^\x00-\x{10ffff}]|\nx)` on `a\r\nxb`, m | `\nx` | same |
| REPLACE `^(?:[^\x00-\x{10ffff}]|\nx)` -> `X` on `a\r\nxb`, m | `a\rXb` | same |
| LIKE `^[^\x00-\x09\x0b-\x{10ffff}]x` on `a\r\nxb`, m | true | negated class whose complement is ONE code point: cardinality 1, lowers through literalChar and joins the new run exactly like `[\n]` (E2 = C1 with a negated spelling, [2,4)) |
| INSTR `^[^\x00-\x09\x0b-\x{10ffff}]x` on `a\r\nxb`, m | 3  | positional pin |
| SUBSTR / REPLACE as above                        | `\nx` / `a\rXb` | same |
| LIKE `(?=[\s\S])^(?:[^\x00-\x{10ffff}]|\nx)` on `a\r\nxb`, m (regexp2-routed) | true | routed invariance through the fork's braced-hex `\x{...}` syntax (parser.go:1701-1707) (E1r, [2,4)) |
| INSTR `(?=[\s\S])^(?:[^\x00-\x{10ffff}]|\nx)` on `a\r\nxb`, m | 3 | positional |
| LIKE `(?=[\s\S])^[^\x00-\x09\x0b-\x{10ffff}]x` on `a\r\nxb`, m (regexp2-routed) | true | (E2r, [2,4)) |
| INSTR `(?=[\s\S])^[^\x00-\x09\x0b-\x{10ffff}]x` on `a\r\nxb`, m | 3 | positional |

Empty-class guards and controls (green on base; each pins the failure
branch's own semantics and the mu direction):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `^(?:[^\x00-\x{10ffff}]|x)` on `a\r\nxb`, m | true    | the dead branch never matches; the live branch `x` matches at the genuine line start 3 (E3, [3,4)) - control |
| INSTR `^(?:[^\x00-\x{10ffff}]|x)` on `a\r\nxb`, m | 4      | positional pin: the x, never the interior (control) |
| SUBSTR `^(?:[^\x00-\x{10ffff}]|x)` on `a\r\nxb`, m | `x`    | same (control) |
| REPLACE `^(?:[^\x00-\x{10ffff}]|x)` -> `X` on `a\r\nxb`, m | `a\r\nXb` | same (control) |
| LIKE `^(?:[^\x00-\x{10ffff}]|x)` on `a\r\nxb`, mu | true   | LF line start 3 under unix lines as well (E3-mu, [3,4)) - control |
| INSTR `^(?:[^\x00-\x{10ffff}]|x)` on `a\r\nxb`, mu | 4     | positional (control) |
| LIKE `^(?:[^\x00-\x{10ffff}]|\nx)` on `a\r\nxb`, mu | false | unix caret blocks the interior even for an exempt string (E1-mu) |
| LIKE `^[^\x00-\x09\x0b-\x{10ffff}]x` on `a\r\nxb`, mu | false | (E2-mu) |

Escape-identity defects (Rev 9; red on base; probe rows X* in
icu-rev9-probe.log, measured on live ICU 77.1; subject
`a\r\n<VT>b` = bytes 61 0D 0A 0B 62):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `^\n\x0b` on `a\r\n<VT>b`, m              | true     | the hex escape records a literal that JOINS the pending run, forming the one initial string `\n\x0b` (literalChar) (X2, [2,4)) |
| INSTR `^\n\x0b` on `a\r\n<VT>b`, m            | 3        | positional pin |
| SUBSTR `^\n\x0b` on `a\r\n<VT>b`, m           | `\n\x0b` | same |
| REPLACE `^\n\x0b` -> `X` on `a\r\n<VT>b`, m   | `a\rXb`  | same |
| LIKE `(?=[\s\S])^\n\x0b` on `a\r\n<VT>b`, m (regexp2-routed) | true | (X2r, [2,4)) |
| INSTR `(?=[\s\S])^\n\x0b` on `a\r\n<VT>b`, m | 3        | positional |

Escape-identity guards and controls (green on base; each pins the ICU
BACKSLASH_V classification that engine-style literal lowering cannot
express):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `^\n\v` on `a\r\n<VT>b`, m                | false    | `\v` records the ICU BACKSLASH_V event (doBackslashv, regexcmp.cpp:1236-1239): the pending run `\n` is sealed as a one-char unit and the op adds 2, so no initial string exists (accumulator 2883-2897) (X1) |
| INSTR `^\n\v` on `a\r\n<VT>b`, m              | 0        | positional pin (over-exemption would return 3) |
| LIKE `(?=[\s\S])^\n\v` on `a\r\n<VT>b`, m (regexp2-routed) | false | (X1r) |
| LIKE `^\n\v` on `a\r\n<VT>b`, mu              | false    | (X1-mu) |
| LIKE `^\n\x0b` on `a\r\n<VT>b`, mu            | false    | (X2-mu) |

Assertion-seal defects (Rev 10; RED on base - the merged base wrongly
rejects the interior matches ICU admits through the string-driven search;
probe rows A3* in icu-rev10-probe.log, measured on live ICU 77.1;
subject `a\r\n<LF>xb` = bytes 61 0D 0A 0A 78 62, UTF-16 offsets equal
byte offsets):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `^\n\nx` on `a\r\n\nxb`, m                  | true     | adjacent-literal control: ONE pending run `\n\nx` keeps stringExempt; the string-driven search finds the run at 2 and the engine's truthful caret holds mid-CRLF (A3, [2,5)) |
| INSTR `^\n\nx` on `a\r\n\nxb`, m                 | 3        | positional pin |
| SUBSTR `^\n\nx` on `a\r\n\nxb`, m                | `\n\nx`  | same |
| REPLACE `^\n\nx` -> `X` on `a\r\n\nxb`, m        | `a\rXb`  | same |
| LIKE `(?=[\s\S])^\n\nx` on `a\r\n\nxb`, m (regexp2-routed) | true | routed invariance (A3r, [2,5)) |
| INSTR `(?=[\s\S])^\n\nx` on `a\r\n\nxb`, m       | 3        | positional |

Assertion-seal guards and controls (green on base - the merged base
already answers them false through its current conservative path; each
pins the ninth-review counterexample that a seal-less core - rejected
alternative 2k - would flip to true):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `^\n$\nx` on `a\r\n\nxb`, m                 | false    | dollar seals the pending run (fixLiterals(false), regexcmp.cpp:1147): the `\n` head is a one-char unit (+2), stringExempt lost, and the START_LINE loop offers only 0/3/4, never the CRLF interior 2 (A1); the engine itself matches at 2 (A1-la2 [2,5)) |
| INSTR `^\n$\nx` on `a\r\n\nxb`, m                | 0        | positional pin (the over-exemption would return 3) |
| SUBSTR `^\n$\nx` on `a\r\n\nxb`, m               | no match | same |
| REPLACE `^\n$\nx` -> `X` on `a\r\n\nxb`, m       | `a\r\n\nxb` (unchanged) | same |
| LIKE `^\n\B\nx` on `a\r\n\nxb`, m                | false    | boundary assertion seals identically (1185) (A2; engine A2-la2 [2,5)) |
| INSTR `^\n\B\nx` on `a\r\n\nxb`, m               | 0        | positional pin |
| SUBSTR / REPLACE for `^\n\B\nx`                  | no match / unchanged | same |
| LIKE `(?=[\s\S])^\n$\nx` on `a\r\n\nxb`, m (regexp2-routed) | false | routed invariance (A1r) |
| INSTR `(?=[\s\S])^\n$\nx` on `a\r\n\nxb`, m      | 0        | positional |
| LIKE `(?=[\s\S])^\n\B\nx` on `a\r\n\nxb`, m (regexp2-routed) | false | (A2r) |
| INSTR `(?=[\s\S])^\n\B\nx` on `a\r\n\nxb`, m     | 0        | positional |
| all six A-spellings on `a\r\n\nxb`, mu           | false    | unix caret blocks the CRLF interior in every spelling (A1-mu/A2-mu/A3-mu, routed A1r-mu/A2r-mu/A3r-mu) |

Backreference-seal defects (Rev 10; RED on base - the merged base wrongly
rejects them on the regexp2 route; probe rows B2/B4 in
icu-rev10-probe.log; subject `a\r\nxb`; every row is regexp2-routed):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `(?=())^\nx` on `a\r\nxb`, m                | true     | no backreference present: ONE pending run `\nx`, stringExempt holds, string-driven search admits the interior (B2, [2,4)) |
| INSTR `(?=())^\nx` on `a\r\nxb`, m               | 3        | positional pin |
| SUBSTR `(?=())^\nx` on `a\r\nxb`, m              | `\nx`    | same |
| REPLACE `(?=())^\nx` -> `X` on `a\r\nxb`, m      | `a\rXb`  | same |
| LIKE `(?=(?<e>))^\nx` on `a\r\nxb`, m            | true     | named-capture spelling of the same classification (B4, [2,4)) |
| INSTR `(?=(?<e>))^\nx` on `a\r\nxb`, m           | 3        | positional |

Backreference-seal guards and controls (green on base; each pins the
backreference seal a naive core would omit - the review's
counterexamples; probes B1/B3 vs B1-la2/B3-la2):

| expression                                       | expected | basis |
|--------------------------------------------------|----------|-------|
| LIKE `(?=())^\n\1x` on `a\r\nxb`, m              | false    | numbered backreference seals the pending run (fixLiterals(false), regexcmp.cpp:1326): `\n` head + `x` tail are two initial units, stringExempt lost (B1); the engine matches at 2 because the lookahead's empty capture is preserved (URX_LA_END frame copy-down, rematch.cpp:3780-3811) and an empty group's backreference matches zero-length (URX_BACKREF, 3641-3680) (B1-la2 [2,4)) |
| INSTR `(?=())^\n\1x` on `a\r\nxb`, m             | 0        | positional pin (the over-exemption would return 3) |
| LIKE `(?=(?<e>))^\n\k<e>x` on `a\r\nxb`, m       | false    | named backreference seals identically (1359) (B3; B3-la2 [2,4)) |
| INSTR `(?=(?<e>))^\n\k<e>x` on `a\r\nxb`, m      | 0        | positional |
| all four B-spellings on `a\r\nxb`, mu            | false    | unix lines reject the interior in numbered, named and both control forms (B1-mu..B4-mu) |

Lookaround/atomic/nesting and generated-prologue controls (engine UTs on
both engines; the SQL-pinnable rows live in the four tables above): node-
type recognition skips ntRequire/ntPrevent wholesale (nested lookarounds
included), traverses ntGreedy/ntCapture bodies so consuming content
destroys atStart, treats ntTestgroup conditionals as control-flow, maps
backreference nodes to Ref (preserve) while Testref may appear only inside
conditionals (destroy), and sees through the wrappers' generated flag
prologues (`(?m)^b` and `(?im)...` classify exactly like their bare user
pattern in mode m/mi).

Admission-preservation controls (user-visible decisions computed on the
original pattern; measured plain TrackCount 7 for `(?m)^b(?=b)`):

| subject length (runes) | expected outcome | basis |
|------------------------|------------------|-------|
| 50,000 (review case)   | LIKE true, no error | 350,007 states + ~5.7 MB budget: admitted pre-merge, must be admitted again (RED on merged base today: the rewrite errors it) |
| 74,000 (edge)          | LIKE true, no error | 518,007 states, total 8,353,712 bytes <= 8,388,608 |
| 150,000 (beyond)       | existing admission error ("regular expression evaluation exceeds the maximum supported backtracking state") | 1,050,007 states > 1,048,576 |

Admission arithmetic controls (green on base for the PLAIN pins and global
arithmetic; RED for the production-composed identity rows - that redness
IS the measured regression this design removes; production numbers from
`trackcount-probe-rev6.log`):

| control                              | expected | basis |
|--------------------------------------|----------|-------|
| TrackCount pin `(?m)^b(?=b)`         | 7        | identity UT: wide-mode compile must equal Unix-mode compile |
| TrackCount pin `(?m)` + 32 literal `^$` pairs + `(?=\n)` | 7 | native anchors add no tracks; anchor count is unbounded by TrackCount |
| TrackCount pin `(?m)^`               | 3        | identity |
| TrackCount pin `(?m)(?:^\n\|z)`      | 5        | identity |
| production composition `^b(?=b)`, m  | 7        | identity through `regexpPatternWithPureMatchType` + `translateRegexp2Pattern`; merged base reads 37 (red defect) |
| production composition 32-pair dense, m | 7     | same; merged base reads 1671 (red defect) |
| production composition `^b(?=b)`, mu | 7        | mu never rewrites (already green) |
| anchor-dense 30,000-rune subject (LF-led, contains CR) | no error | plain states 210,007 admitted; merged base charges 50,131,671 and rejects (red defect) |
| 74,000-rune wide-bearing subject, `^b(?=b)`, m | no error AND LIKE true | plain states 518,007 admitted; merged base charges 2,738,037 and rejects; the true-after-CR verdict is a defect row |
| overlap/reject/release arithmetic    | exact counter values | two overlapping 74k reservations admitted; the failing reservation does not register; releases restore the counter to baseline |

## Standards, precedent, and alternatives

Contract sources: the issue's measured MySQL 8.0.46 matrix, the verified
MySQL->ICU flag mapping, the bundled ICU 77.1 source, the live ICU 77.1
probe rows (`icu-rev6-probe.log`), and the MySQL binary facade (evidence
above).

Alternatives considered:

1. Line-split evaluation in the SQL layer (split subject at wide
   terminators, evaluate per line, reassemble positions). Rejected: it
   reparses per row, loses original positions/occurrence/empty-segment
   semantics, and the merged PR's lifecycle measured a 36.6x long-text
   slowdown and ~209 KiB extra per-batch allocation; multiple review rounds
   kept finding iteration, occurrence and anchor defects.
2. Route all wide-`m` matching to regexp2 with lookaround anchors. Rejected
   for the regular path: regexp2 carries an evaluation budget and subject
   limits, can error on inputs the regular engine accepts, introduces
   backtracking cost on a hot path, and violates invariant 7. It remains the
   mechanism only where regexp2 already owns the pattern (genuine ICU
   syntax), where lookarounds are natively supported and existing bounds
   already apply.
2a. (Rev 3-4 approach, FALSIFIED three times) Repair the regexp2 path with
   a source rewrite of `^`/`$` into lookarounds. Review round 3 showed the
   rewrite turns admitted workloads into errors (TrackCount 7 -> 23 moves a
   50,000-rune subject across the 1<<20 states cap). Review round 4 showed
   no constant multiplier can rescue it (anchor count is unbounded by
   TrackCount; the compiled-bytes term varies with expression length;
   expanded reservations reject previously admitted overlapping
   evaluations). Round 6 measures the same failure LIVE IN PRODUCTION: the
   merged base implements exactly this approach
   (`rewriteRegexpWideMultilineAnchors`, func_builtin_regexp.go:3146-3152,
   invoked by `regexpPatternWithPureMatchType` at 3244), and the Rev 6
   premise probe (`trackcount-probe-rev6.log`, exit 0) reads TrackCount
   7->37 for `(?m)^b(?=b)`, 7->1671 for the anchor-dense pattern and 3->33
   for `(?m)^`, the 30,000-rune boundary subject at 50,131,671 states
   REJECTED (plain 210,007 admitted), the 74,000-rune subject at
   2,738,037 REJECTED (plain 518,007 admitted), and the 50,000-rune review
   case erroring through the production entry (rev6 run log,
   TestIssue30210Rev4AdmissionPreservationControls). The rewrite is
   removed by Plan item 4, not patched: its failure mode is structural.
2b. (Rev 5 approach, REJECTED by measurement) Classify the structural
   start on OPTIMIZED engine output - the compiled Go prog instructions or
   regexp2 BmPrefix/Codes. Probes S06/S13/S14 and S16/S17 require
   distinctions both optimizers erase: Go parse removes noncapturing/scoped
   groups (stdlib syntax/parse.go:1410-1437) and compile clears ineffective
   FoldCase (compile.go:277-280); regexp2 Reduce strips group nodes and
   merges adjacent literals (fork syntax/tree.go:306-375, 461-468) and
   lowers singleton sets to one char (reduceSet; parser.go:1183 always
   emits a set node for `[...]`). Immediate rune-run and BmPrefix tests
   cannot distinguish `^\nx` (string priority) from `^(?:\n)x` (line
   policy) or a folded run from a case-sensitive one. Replaced by
   pre-optimization capture (invariant 8, Plan items 1c/4c). (The RE2
   syntax.Regexp tree is equally post-erasure - parse itself removes the
   groups - so classifying the parsed syntax tree is rejected for the same
   reason; only the raw pattern text carries the structure on that path.)
2c. (Rev 5 regexp2 lookaround rule, REJECTED by measurement) Recognize
   lookaround blocks by writer-emitted opcode prefixes (Setjump followed by
   Setmark/Lazybranch through the matching Forejump). The writer emits the
   same prefixes for consuming atomic groups with leading captures or
   alternations (fork syntax/writer.go:283-317), and probe S19
   (`(?>(\r))^\n` true at [1,3)) proves the atomic body must be traversed,
   not skipped. Replaced by node-type recognition captured at parse events
   with exact Ref (13) / Testref (37) naming (invariant 8; alternative
   2d explains why the pre-Reduce tree itself is unavailable).
2d. (Rev 6 regexp2 capture point, REJECTED by source inspection) Walk a
   pre-Reduce RegexNode tree during code construction (before
   syntax.Compile/Write). The pinned fork reduces eagerly at child
   insertion: regexNode.addChild calls child.reduce() and stores the
   reduced node (fork syntax/tree.go:168-173), and the parser routes
   every atom, quantified unit, group body and alternation through
   addChild while parsing (fork syntax/parser.go: addConcatenate
   ~2007-2013, addConcatenate3 ~2016-2021, addGroup ~2051-2066).
   Noncapturing groups, literal merges and singleton-set lowerings are
   gone before code construction, so the tree walk cannot distinguish
   `^\nx` from `^(?:\n)x` (S06/S13). Replaced by the parser-event
   recorder (invariant 8, Plan item 4c): parser-event metadata retained
   independently of the optimized tree, while ordinary reductions,
   generated code and admission inputs remain unchanged. (That recorder
   is itself superseded in Rev 8 by alternative 2g's pre-translation
   event scanner, because the SQL translation destroys quantifier
   flavor before the parser runs; the capture-before-reduction
   requirement stands and is met at the original pattern text.)
2e. (Rev 6 exemption rule, REJECTED by measurement) Derive stringExempt
   from the first maximal contiguous literal run with prefix-only early
   termination. Live ICU probes falsify it in both directions:
   `^(?:\nx|y)` is FALSE on `a\r\nxb` (R1 - the first run is `\nx`,
   yet the second branch also contributes initial content), while
   `^\nx(?:|y)` is TRUE at [2,4) (R2), `^(?:\nx|\nx)` is FALSE (D4 -
   counting, not same-string dedup), and `^(?:\nxy|)` matches only as
   the empty match (D13 - minMatchLen > 0 is a ladder condition). ICU
   accumulates over ALL minimum-length-0 paths with branch forwarding
   (matchStartType, icu-77.1-regexcmp.cpp:2691-3188); the exemption
   requires exactly one initial case-sensitive string and minMatchLen >
   0. Replaced by the shared accumulator (invariant 8).
2f. (Rev 6 quantifier rules, REJECTED by measurement) Treat quantifiers
   as unconditional atStart destroyers/run flushers and quantified runs
   as never exempting. Live probes falsify both halves: `x{0}^\n` is
   FALSE (R3 - a zero-count unit is removed before the walk, so nothing
   precedes the caret), `^(?:\nx)+` and `^(?:\nx){2,3}` are TRUE
   (R4/D7 - min>0 loop bodies count once and seed the exemption),
   `^(?:\nx){1}` is TRUE (D11), while `^\nx{1}`, `^\nx?`, `^\nx+`,
   `^\nx{0}y` and `^\n{2}x` are FALSE (D1/D15/D16/D2/D8 - any
   quantifier on a run's last char SPLITS the run first,
   fixLiterals(true) at regexcmp.cpp:1898-1922 called from blockTopLoc
   2139-2169; inlined single-char copies never reform a string).
   Replaced by the quantifier-lowering stage of the shared core
   (invariant 8).
2g. (Rev 7 front-end composition, REJECTED by source inspection and
   measurement) Keep TWO front ends: the RE2 raw-pattern-text scanner
   plus a parser-event recorder inside the vendored regexp2 parser fed
   its translated pattern, with quantifiers recorded as bare {min,max}
   and zero-count deletion applied flavor-blindly, and with literal runs
   merged by linear textual adjacency. Both premises fail on primary
   evidence. (i) Flavor: requiresRegexp2Pattern routes possessive forms
   to regexp2 (regexp_icu.go:260-268) and translateRegexp2Pattern wraps
   possessive intervals as `(?>atom{n,m})` before compile
   (regexp_icu.go:1007-1025), so any recorder fed the translated
   pattern can never see possessive flavor; and ICU's dispatch tries
   inline elimination ONLY for ordinary intervals (regexcmp.cpp:
   1047-1092 - lazy always compiles CTR_INIT_NG/CTR_LOOP_NG, possessive
   always STO_SP + CTR_INIT/CTR_LOOP + LD_SP), so flavor-blind
   zero-count deletion misclassifies the live probe rows `x{0}?^\n` and
   `x{0}+^\n` (both TRUE at [2,3) under m, Q2/Q3) as FALSE. (ii)
   Sealing: ICU seals the pending literal run at every compiler
   boundary (class open 1653-1655; inline and scoped flag sets
   1494-1527; group opens/closes of every kind 465-714/2192;
   alternation 398; pattern end 1275) BEFORE a singleton class lowers
   through literalChar (2410-2420), so linear adjacency exempts the
   live probe rows `^\n[x]`, `^[\n][x]`, `^\n(?s)x`, `^\n(?s:x)`,
   `^\n(?i)x` and `^\n(?:x)` (all FALSE under m, C2/C3/C4/C5/C6/C7)
   that ICU rejects. Replaced by ONE raw-pattern-text event scanner
   reading the ORIGINAL pre-translation pattern on both routes
   (invariant 8, Plan items 1c/4c), recording quantifier flavor and
   applying ICU's sealing and flavor-specific lowering in the shared
   core; the conservative opaque-event fallback is unchanged.
2h. (Rev 8 quantifier record, REJECTED by source inspection and
   measurement) Record quantifiers as {min,max,flavor} WITHOUT the
   operator FORM, lowering `+`, `*` and `?` exactly as the `{1,}`,
   `{0,}` and `{0,1}` interval spellings. Live ICU probes falsify the
   plus half in every flavor: `(?:^\n)+` is FALSE on `a\r\nb` under m
   (V1) while `(?:^\n){1,}` is TRUE at [2,3) (V2); the lazy pair
   `(?:^\n)+?` FALSE (V3) vs `(?:^\n){1,}?` TRUE [2,3) (V4) and the
   possessive pair `(?:^\n)++` FALSE (V5) vs `(?:^\n){1,}+` TRUE
   [2,3) (V6) agree, as do mu and routed rows. The compiler explains
   the split: plus loops append their control AFTER the body
   (regexcmp.cpp:795-856, 1371-1400), so the caret inside keeps
   atStart, while intervals place CTR_INIT BEFORE the body
   (2440-2504; accumulator 3046-3071), clearing it. The star/question
   half coincidentally agrees across spellings (leading guards in
   both; V7-V10 equivalence pins) but does not rescue the rule.
   Replaced by FORM-retaining quantifier events with FORM dispatch
   (invariant 8).
2i. (Rev 8 class events, REJECTED by source inspection and
   measurement) Record class events with singleton content but no
   ZERO-cardinality resolution and no complement arithmetic. ICU's
   compileSet switch (regexcmp.cpp:2403-2437) emits URX_BACKTRACK
   for an empty resolved set (2407); the accumulator BACKTRACK case
   (2970-2975) forwards the length of the following op and adds
   NOTHING to numInitialStrings while killing its own path. Live
   probes: `^(?:[^\x00-\x{10ffff}]|\nx)` is TRUE at [2,4) on
   `a\r\nxb` under m (E1) - treating the dead branch as consuming
   +2 or opaque raises numInitialStrings to 3 and strips the
   exemption, the exact outcome the probe contradicts; the negated
   singleton `^[^\x00-\x09\x0b-\x{10ffff}]x` is TRUE at [2,4) (E2)
   because complement arithmetic resolves it to {LF}, the C1
   equivalence. Replaced by cardinality-resolving class events with
   the failure event (invariant 8).
2j. (Rev 8 escape lowering, REJECTED by source inspection and
   measurement) Lower escapes the way the ENGINES do for matching -
   Go stdlib and the vendored core both fold `\v` and `\x0b` into
   the same VT literal rune - instead of the way ICU classifies.
   ICU emits URX_BACKSLASH_V for `\v` (doBackslashv,
   regexcmp.cpp:1236-1239: fixLiterals(false) seals the pending run
   first; the accumulator case 2883-2897 counts it +2 like a set),
   while `\x0b` lowers through literalChar and joins the run, so
   `^\n\v` is FALSE (X1) and `^\n\x0b` is TRUE at [2,4) (X2) on
   `a\r\n<VT>b` under m - a distinction engine-identical lowering
   cannot express. The classification contract is MySQL's, hence
   ICU's, so the event vocabulary preserves the ICU identity of
   shared spellings on both routes (invariant 8); the engines' own
   consuming-atom semantics for `\v` matching are unchanged (the
   documented non-goal).
2k. (Rev 9 sealing contract, REJECTED by source inspection and
   measurement) Treat run sealing as DERIVABLE from atStart and
   length effects - i.e. let zero-width, atStart-preserving,
   min-length-0 events (assertions, backreferences) pass WITHOUT
   sealing the pending literal run, and seal only the boundaries
   that those other effects already special-case. The three effects
   are independent in ICU: fixLiterals(false) is called for caret
   (regexcmp.cpp:1132), dollar (1147), `\A` (1161), `\b` (1172),
   `\B` (1185), `\G` (1202), `\Z` (1261), `\z` (1266) and
   numbered/named backreferences (1326/1359; default split=false,
   regexcmp.h:103), while the SAME opcodes stay zero-width,
   atStart-preserving and min-length-0 for the accumulator
   (2742-2757; backreferences possibly empty, 2752-2753; max
   unbounded, 3548-3551). Live ICU 77.1 probes falsify the
   conflation with find/lookingAt pairs: `^\n$\nx` (A1) and
   `^\n\B\nx` (A2) find NO match on `a\r\n<LF>xb` under m although
   the engine matches at 2 (A1-la2/A2-la2 [2,5)); the
   adjacent-literal control `^\n\nx` (A3) matches [2,5); routed
   A1r/A2r false, A3r true; all false under mu. `(?=())^\n\1x`
   (B1) and `(?=(?<e>))^\n\k<e>x` (B3) find NO match on `a\r\nxb`
   although lookingAt(2) matches [2,4) (B1-la2/B3-la2); controls
   `(?=())^\nx` / `(?=(?<e>))^\nx` match [2,4) (B2/B4); all false
   under mu. The conflated core merges the surrounding literals
   into ONE initial run, grants stringExempt and admits the
   interior matches ICU rejects. Replaced by the ONE shared
   seal/atStart/length event table of invariant 8, which names
   every literal-flush boundary with all three effects recorded
   independently.
3. Amend the merged private Go 1.27.1 regexp engine in place. SELECTED for
   the RE2 path: PR #30646 already delivered
   `pkg/sql/plan/function/internal/regexp` (pinned copy of stdlib 1.27.1
   `regexp.go`/`exec.go`/`backtrack.go`/`onepass.go` plus `lines.go`, an
   immutable per-compiled-pattern LineMode, and original-context offset
   search FindStringIndexAt/FindStringSubmatchIndexAt). This design keeps
   that engine and its provenance (BSD headers + LICENSE, README pinned
   content hashes) and amends exactly three pieces: the wide terminator
   classifier (VT/FF in, binary 0x85 out), the caret fork (CR|LF interior
   exclusion removed), and the addition of the invariant-8 descriptor +
   candidate policy. Parsing, compilation, executors, capture handling and
   pools stay upstream; the stdlib differential keeps proving the default
   mode.
4. Vendor a native line mode for regexp2. SELECTED for the regexp2 path
   (Plan item 4): a maintained private copy of the pinned fork (module
   replacement `github.com/XuPeng-SH/regexp2`
   `v1.10.1-0.20261004024751-718f43e031fe`, which is upstream
   `dlclark/regexp2` v1.10.0 plus the fastclock deadline fix in
   `fastclock.go`, 24 changed lines; MIT LICENSE kept at the package root;
   README records both upstream and fork pins, per-file content hashes and
   the sync procedure; the fuzz corpus directory is excluded) as
   `pkg/sql/plan/function/internal/regexp2`. It gains the same line-mode
   concept as the RE2 engine - mode-aware assertion evaluation, the
   invariant-8 descriptor computed from the parser-event stream recorded
   before the eager reductions (alternative 2d), and the begin-candidate
   policy - while parser reductions, compiler output and runner
   bookkeeping stay upstream. Because the mode never changes codegen, the
   compiled program is the user pattern's program in every mode and
   invariant 9 holds as an identity. `regexp_icu.go` remains the
   repository's only regexp2 importer (verified by repo-wide grep on this
   base), so the go.mod `require` + `replace` lines are dropped once the
   wrapper points at the vendored copy.

## Plan and execution model

Scope is exactly #30210: the regexp multiline line-terminator contract.
The plan amends the merged implementation in place; nothing below broadens
into the merged PR's other deliveries.

1. RE2 path (merged private engine amendments):
   a. `pkg/sql/plan/function/internal/regexp/lines.go`: extend
      `isWideTerminator` so VT (U+000B) and FF (U+000C) are terminators in
      every wide mode (they are ASCII runes in text and pass through the
      binary mapping unchanged), and REMOVE the binary-mode terminators
      U+E085 and U+2026 (invariant 6). Text keeps {CR, VT, FF, NEL, LS,
      PS}; both binary alphabets collapse to {CR, VT, FF} plus native LF.
   b. `exec.go` `lazyFlag.match`: caret becomes true at subject start,
     after LF (legacy, including at EOF), or after any wide terminator
     when a character follows - the `!(r1 == '\r' && r2 == '\n')` interior
     exclusion is REMOVED (ICU URX_CARET_M; probes S19-S21). Dollar keeps
     the merged table (EOF, or before LF not preceded by CR, or before any
     wide terminator - with VT/FF now included by 1a). Unix mode stays
     byte-identical to upstream.
   c. Structural start descriptor (invariant 8): a raw-pattern-text
     structural scanner in the private engine (new file `startclass.go`)
     emits the shared pre-lowering event vocabulary from Go regexp
     syntax - literals and escapes (`\Q...\E` quoted runs, Perl class
     escapes, anchor escapes) with the fold state in effect, class
     brackets resolving cardinality 0/1/greater with complement
     arithmetic (the class OPEN seals the pending
     literal run, round-7 finding F2; cardinality 0 records the failure
     event, round-8 finding F2; cardinality 1 - negated singletons
     included - joins the new run), ICU-identity escapes for shared
     spellings (`\v` records the BACKSLASH_V event, round-8 finding
     F3), capturing/noncapturing/scoped
     group opens and closes, `(?i)`/`(?-i)`/`(?m)` flag toggles (each
     inline flag SET seals the pending run even when no flag changes),
     alternation branch opens, quantifiers with parsed FORM, min/max AND
     FLAVOR (star/plus/question/interval x ordinary/lazy/possessive,
     round-7 finding F1 and round-8 finding F1), and
     anchors and boundary assertions of every kind (each a run-SEALING
     event per round-9 finding F1 while staying zero-width,
     atStart-preserving and min-length-0; backreferences cannot occur
     in this dialect - `requiresRegexp2Pattern` routes them away) - and
     feeds the SHARED classification core (both engines use
     the same core: the form- and flavor-dispatched quantifier-lowering
     stage -
     run split, plus-loop body-first placement with trailing control
     versus interval init-first placement (V1 vs V2), star/question
     leading guards form-equivalent to {0,}/{0,1} (V7-V10),
     ordinary-only zero-count deletion with counted-loop
     retention for lazy/possessive zero counts, inline copies,
     counted-loop body walk - and the branch-forwarded initial-string
     accumulator of invariant 8). It runs
     at `CompileWithLineMode` without relying on `syntax.Parse` output
     (which erases the same structure, round-5 finding F2), under the
     invariant-8 sound stopping condition - O(pattern prefix) once both
     flags are frozen, O(pattern) worst case, once per compile - and
     stores the descriptor on the compiled Regexp before publication.
   d. Begin-candidate policy: when `lineAnchoredStart && !stringExempt`
     and a wide mode, spawning a NEW initial match thread at a CRLF
     interior position (r1 == CR, r2 == LF) is suppressed in the NFA
     `step` initial-thread spawn, the backtrack candidate loop, the
     one-pass candidate loop and the offset searches
     (FindStringIndexAt/FindStringSubmatchIndexAt), mirroring Rev 5's
     init-mask; live threads stepping through the position keep invariant
     1 semantics. Patterns outside the classified pair (including every
     string-exempt pattern) keep the engine's normal candidate
     enumeration, and the engine's existing input prefix facts are
     untouched. Unix mode never masks (no-op).
2. Flag plumbing (MERGED, kept): `getPureMatchType`
   (func_builtin_regexp.go:3343) preserves `u`; the RE2 prefix composer
   (3258) and the regexp2 composer keep stripping flags correctly per
   route; `mu` users get the Unix tables on both engines; this revision
   adds parity UTs rather than new plumbing.
3. Matcher cache (MERGED, kept): the keyed cache already carries
   matchType (func_builtin_regexp.go:2078); restated UTs assert `m`/`mu`
   matchers never alias. Wide mode stays selected once per compiled
   matcher: text -> WideTextLines; binary (either case mode) -> the single
   binary wide mode (the merged WideBinaryFoldLines distinction collapses
   with the classifier unified - the LineMode enum may keep both names as
   aliases of one policy to bound the diff, pending review preference, but
   the semantics are one set {CR, VT, FF} + LF).
4. regexp2 path: remove the rewrite, vendor the engine, native line mode.
   a. Delete `rewriteRegexpWideMultilineAnchors` and its invocation in
      `regexpPatternWithPureMatchType` (func_builtin_regexp.go:3146-3152,
      3244): the composer returns the user pattern with flag handling
      only, so the admission owner's inputs are the user pattern again;
      its size-admission role and comments are kept.
   b. New private package `pkg/sql/plan/function/internal/regexp2` per
      alternative 4 (pinned fork, MIT LICENSE, README with upstream v1.10.0
      + XuPeng-SH fastclock pins + per-file hashes + sync procedure; fuzz
      corpus excluded; import paths move; go.mod require+replace dropped
      after `regexp_icu.go` switches; no other repository importer
      exists).
   c. Native line-mode mechanism in the vendored copy:
      - `LineMode` (`UnixLines`, `WideTextLines`, `WideBinaryLines`)
        stored on the compiled `Regexp` before publication; default
        `UnixLines` keeps the vendored library a byte-identical drop-in
        for its own test-suite differential.
      - Assertion evaluation (runner `syntax.Bol`/`syntax.Eol` cases,
        runner.go:511-525 in the pinned fork): mode-aware tables equal to
        invariant 1/4 (wide caret: subject start, preceding LF including
        at EOF by the documented legacy, or preceding wide terminator with
        a following character; wide dollar: EOF, before LF not preceded by
        CR, or before any wide terminator; mu LF-only). No new opcodes;
        the change is the comparison, so codegen output never moves.
      - Structural start descriptor (round-5 findings F2/F3, round-6
        finding F1, round-7 finding F1): the shared event scanner reads
        the ORIGINAL regexp2-route pattern BEFORE translateRegexp2Pattern
        rewrites it (the possessive `(?>atom{n,m})` rewrite at
        regexp_icu.go:1007-1025 would destroy quantifier flavor for any
        later capture point - alternative 2g), lexing the ICU/.NET
        construct superset into the shared event vocabulary: literals
        and escapes with fold state (`\v` records the ICU BACKSLASH_V
        event of round-8 finding F3 - the classification contract is
        MySQL's, hence ICU's - while braced-hex `\x{...}` escapes the
        fork parses, syntax/parser.go:1701-1707, and their
        hex/octal equivalents record the resolved literal), class
        open/close resolving cardinality 0/1/greater with complement
        arithmetic (open seals; cardinality 0 records the failure event
        of round-8 finding F2; cardinality 1 - negated singletons
        included - joins the new pending run), group open/close with kind
        (capture/named/noncapture/scoped-flag/atomic/lookahead/
        lookbehind), inline flag sets (sealing), alternation branches,
        quantifiers with parsed FORM AND min/max AND flavor
        (star/plus/question/interval x ordinary/lazy/possessive,
        round-8 finding F1), backreferences (Ref = 13;
        preserve atStart AND seal the pending run - round-9 finding F1;
        possibly empty: min-length +0, max-length unbounded) and
        conditionals (Testref = 37; destroy
        conservatively), plus the focal anchor tokens (every anchor and
        boundary kind sealing the pending run while preserving atStart
        and contributing no length - round-9 finding F1). Capturing at the
        source text satisfies round-6 F1 one stage earlier than the
        eager reductions (the fork reduces at insertion:
        tree.go:168-173, parser.go ~2007-2066 - alternative 2d): no
        fork representation is ever asked to supply structure it
        discarded. The event stream feeds the SAME shared
        classification core as the RE2 scanner, producing
        {lineAnchoredStart, stringExempt}, which is persisted onto
        `syntax.Code` alongside codegen. Translation, parsing,
        reduction, codegen itself, TrackCount and prefixes are
        untouched, so the fork's compiled output is identical per
        pattern in every mode (invariant 9 tripwire).
      - Begin-candidate policy (runner `scan`/`findFirstChar`,
        runner.go:103-172 and 1271-1355): priorities follow the existing
        machinery, which matches ICU's ladder - anchor fast paths first;
        then the Boyer-Moore literal prefix (`BmPrefix`, computed by
        `getPrefix` in syntax/prefix.go:337-400, which skips a leading
        Bol): a string-exempt pattern keeps today's prefix behavior (its
        candidate is the literal run; `^\nx` and `^[\n]x` class patterns
        evaluate their truthful interior caret, matching START_STRING).
        Otherwise, when `lineAnchoredStart` and a wide mode,
        `findFirstChar` advances from a failed position to the next LINE
        START (position 0 or the first position after a completed wide
        terminator, with CR followed by LF completing once after the LF)
        instead of bumping by one - the CRLF interior is never offered,
        equal to ICU's `startPos++` skip. Unix mode and non-anchored
        patterns use today's bump-by-one path unchanged.
      - Compilation sites (`compileRegexp2Expression`, regexp_icu.go:2055,
        and the `regexp2PrepareExpression` admission probe path) pass the
        matcher's immutable mode.
   d. Admission and reservations (invariant 9, by construction): the
      wrapper's translated expression, the compiled `Code` (Codes, Sets,
      TrackCount, prefixes, capsize) and every budget term in
      `regexp2EvaluationMemoryBudget` (regexp_icu.go:2003) plus the
      global reservation arithmetic (regexp_icu.go:2158-2214) depend only
      on the user pattern and subject; the mode touches none of them. No
      surcharges, no dedupe, no scaled ceilings, no per-subject expression
      selection and no subject classification scan exist. Mode-identity
      UTs demand identical TrackCount/code length/expression bytes between
      Unix and wide compiles of each pinned pattern AND between the
      production-composed and plain forms of the pinned route patterns
      (7/7/3/5); the harness's deterministic overlap/reject/release
      controls (carried by rev7) pin the global arithmetic as the contract (two overlapping
      74k evaluations admitted exactly as today; a failing reservation
      registers nothing; releases restore the baseline). Evaluation keeps
      the existing `regexp2MatchTimeout` and subject-size validation
      unchanged.
5. Binary domains (facade verified pre-merge and re-verified on this base,
   `func_builtin_regexp.go:1844, 3064-3070`): raw binary maps high
   bytes to identity runes U+E080..U+E0FF (0x85 -> U+E085) and passes
   ASCII bytes through (0x0D/0x0B/0x0C -> U+000D/U+000B/U+000C); binary
   case-fold maps high bytes via Windows-1252 (0x85 -> U+2026). Neither
   high-byte rune is a line terminator (invariant 6), so the classifier
   names neither after amendment; MySQL's CP-1252 facade makes the same
   byte arrive as U+2026 and non-breaking. The merged UT pins that record
   the old classification are amended in item 6.
6. Merged-test and merged-design reconciliation (evidence-backed amendment,
   not scope growth): `internal/regexp/lines_test.go:59-60` flips the
   binary U+E085/U+2026 rows to non-terminators and gains VT/FF rows for
   all alphabets; `pkg/sql/plan/function/regexp_multiline_test.go:103-110`
   drops `"\x85"` from the m-TRUE separator loop and pins binary 0x85
   FALSE under `m`/`mi` (mu stays FALSE); that suite gains VT/FF, the
   assertion-split rows, and its
   `rewriteRegexpWideMultilineAnchors`-specific expectations are removed
   with the function (long-pattern admission rows now assert identity). An
   addendum section in docs/design/20261024-regexp-wide-line-assertions.md
   records the three superseded decisions (VT/FF scope; binary 0x85; the
   CR|LF-interior assertion rule) with pointers to this document's
   evidence (probes T02/T03/S19-S21; facade; invariant 8), keeping that
   document's own history intact.
7. No persistence, wire, catalog or configuration changes. Other repository
   regexp users keep the standard library; only the SQL REGEXP owner paths
   named here change.

## Ownership and unhappy paths

- `regexpSet` (`func_builtin_regexp.go:2022`) remains the sole matcher
  owner/cache. The keyed cache (merged) cannot alias `m` and `mu`
  matchers; the restated UT proves it.
- Pattern admission errors (oversize, invalid syntax) surface before any
  evaluation; error mapping stays with the existing regexp2/RE2 compile
  diagnostics. Masked rows (NULL pattern/subject) remain unevaluated. The
  removed rewrite's dedicated errors disappear with it; no new error
  classes are introduced.
- The vendored regexp2 package changes assertion evaluation, compile-time
  classification metadata and candidate enumeration only; parsing,
  compilation output, runner bookkeeping, timeout and allocation behavior
  stay upstream. Failure modes are limited to the existing
  compile/admission/timeout errors. No goroutines, sockets, or retained
  per-row state are introduced; engine machine pools are upstream-owned.
  The vendored package's README owns provenance (upstream v1.10.0 + fork
  fastclock patch + per-file hashes + sync procedure), and `regexp_icu.go`
  is the only importer switching to it.
- Concurrent reuse of one compiled matcher across rows relies on the engine
  being goroutine-safe exactly like upstream (immutable compiled program +
  pooled machines); the concurrency UT covers both engines.
- `decodeBinaryRegexpText` keeps restoring original bytes in results;
  binary emission paths are untouched.
- Neither the classification event scanner (both route dialects) nor the
  descriptor ever rejects a pattern: any construct outside the mirrored
  rule set resolves conservatively (no line-anchoring, no exemption),
  which can only leave behavior at today's candidate enumeration;
  correctness of the conservative direction is pinned by the guard
  tables.

## Performance model

- Warm path: no parse, rewrite, splitter scan, or prefix inspection per row
  on the regular path. Wide-vs-Unix policy is a per-compiled-pattern
  constant folded into the shared assertion evaluation (rune comparisons);
  the candidate init-mask is two comparisons at thread spawn for
  line-anchored non-exempt patterns only.
- Compile time only: the event scanner is O(pattern prefix) once frozen
  and O(pattern) worst case on each route (lexing the original text,
  which both routes already read); the shared core walk is linear in the
  event count. All run once per compiled pattern (cache-hit-free exactly
  like today's compile). No per-row or per-subject classification exists
  on either path.
- Regular engine keeps upstream linear-time matching and machine pools; no
  subject-size gate, no full-input text copy. On the regexp2 path, which
  already recompiles per subject, the vendored engine adds only
  compile-time constants (the event recording plus core walk, a flag
  store) and a branch in `findFirstChar` and in the Bol/Eol cases. Admission inputs, budgets,
  reservations and per-subject runtime are identical to the plain pattern
  BY CONSTRUCTION (invariant 9); removing the rewrite also removes its
  per-compile expression expansion (measured 7 -> 80 bytes for
  `(?m)^b(?=b)`).
- Acceptance criteria (validation step): identical warmed vector fixtures on
  base and final implementation, three samples; short/long text and binary
  predicate medians within 1.25x of base, no additional per-row allocations
  and at most ~4 KiB/batch extra bytes.

## Validation matrix

Cheapest discriminating evidence first. The pinned oracle is harness rev10
(`state/artifacts/issue-30210/harness/repro30210_rev10_test.go`, sha256
`e0ad789f735d4c443f191d9fbf4e3206d44a4e8aa367ec6bbdd3971c6a304efb`), which
carries every rev9 row unchanged (24 test functions) and adds four Rev 10
blocks pinning the assertion-seal rows (A3/A3r red-on-base with full
LIKE/INSTR/SUBSTR/REPLACE pins for A3; A1/A2 with full pins, routed
A1r/A2r and the six-row mu family silent-green) and the
backreference-seal rows (B2/B4 red-on-base with full pins for B2; B1/B3
and the B1-mu..B4-mu family silent-green, all routed).
Superseded oracles: harness rev9 (sha256
`1b322b394fc9c71c40c7815105d706a5d6697107ecb2e14d4d64a02b6651f499`)
carried rev8 and added six Rev 9
blocks pinning the quantifier-form rows (V2/V4/V6 with full
LIKE/INSTR/SUBSTR/REPLACE pins, routed V2r/V6r, the V1/V3/V5/V1r/V5r
silent-green controls, the V2/V4/V6-mu guards and the V7-V10
star/question equivalence rows), the empty-class rows (E1/E2 with full
pins, routed E1r/E2r, the E3/E3-mu controls and the E1-mu/E2-mu guards)
and the escape-identity rows (X2 with full pins, routed X2r, the X1/X1r
silent-green controls and the X1-mu/X2-mu guards); harness rev8 (sha256
`4af413e5f7f91c92feb332ec0aec3923026169a6c4ccc3564142e935f9c82d36`)
carried rev7 and added the flavor and sealing rows (Q2/Q3 with full
pins, routed Q2r/Q3r, Q1 silent-green, the Q1-mu/Q2-mu/Q3-mu and
routed-Q1r guards; C1/C9 with full pins, routed C1r, the C2/C3
silent-greens with routed C2r/C3r, the C4-C7 and routed C4r/C5r
guards, the C1-mu/C2-mu/C3-mu guards); harness rev7 (sha256
`95cab3c10152663f9f8d67e3d4217f4d192142ba71f2a4750b29bb4a02a510f5`)
carried rev6 and added the branch-aware accumulation rows
(R1/R2/D4/D5/D10/D13, routed P1/P2), the quantifier-lowering rows
(R3/R3b/R3c/R4/D1/D2/D3/D6/D7/D8/D9/D11/D12/D12b/D14/D15/D16, routed
P3/P4) and the mu asymmetry rows, all still carried in rev10.
Implementation validation must run the rev10 revision red on the merged
base (recorded today,
`state/artifacts/issue-30210/repro-base-go-unit-rev10.log`, exit 1;
per-block attribution in the log: only the defect carriers fail - every
carried rev4-rev9 block reproduces the recorded rev9-log FAIL/PASS
attribution exactly (block status parity verified), plus the Rev 10
TestIssue30210Rev10AssertionSealDefects (A3 LIKE/INSTR/SUBSTR/REPLACE
and A3r LIKE/INSTR error lines; 6 lines total) and
TestIssue30210Rev10BackrefSealDefects (B2 LIKE/INSTR/SUBSTR/REPLACE and
B4 LIKE/INSTR error lines; 6 lines total) - while every guard,
including the new TestIssue30210Rev10AssertionSealGuards (A1/A2 with
full pins, routed A1r/A2r and the six-row mu family, all silent-green)
and TestIssue30210Rev10BackrefSealGuards (B1/B3 and the B1-mu..B4-mu
family, all silent-green), stays green) and
green on the implementation head; the rev9/rev8/rev7/rev6 red runs are
retained at
`repro-base-go-unit-rev9.log` /
`repro-base-go-unit-rev8.log` / `repro-base-go-unit-rev7.log` /
`repro-base-go-unit-rev6.log` (exit 1
each). Permanent tests encode the same assertions with explicit unicode
escapes. The premise probe
(`state/artifacts/issue-30210/harness/trackcount_probe_rev6_test.go`,
sha256 `737c012589f3a7882ec6d922dab8df64ab6eec704e21f8cf25dae2693ce01b26e`,
log `trackcount-probe-rev6.log`, exit 0) measures the merged rewrite's
production admission deviation and the plain pins (reused: inputs
unchanged on this base, terminal result recorded); the live ICU 77.1
probes pin every disputed classification row (rev6: source
`icu-rev6-probe.cpp` sha256
`7ab777bbc5dcdad13f30a57b66ac12bc7f510e7a2c49ae6d52a4b3493541883c`, log
sha256 `4a395a50e247526206092ca28bd23c0537748a1ac314c29ae6e8ccf0238aadf3`;
rev7: source `icu-rev7-probe.cpp` sha256
`eab24d13f3ca38b31a46a7fdb529851625b3c64b7cde00cd2ffde777e33b699a`, log
`icu-rev7-probe.log` sha256
`2d8d2547549e0ebc37234ae4c41eabf5ced45bed3a5eca8d65e69e90ac793961`;
rev8: source `icu-rev8-probe.cpp` sha256
`da68c47fbc7941d5475dac7e43c44590d5f3f36049a9d889dd92e107d7933366`, log
`icu-rev8-probe.log` sha256
`90535da4d0ee15822aae3a7b8e99279d2f3d76b728cf7bef16f98341aea613a4`;
rev9: source `icu-rev9-probe.cpp` sha256
`2cdb790b407a2b4dc306836ff222af65a95ae40f2b9156f69968eae309c58320`, log
`icu-rev9-probe.log` sha256
`4255b5b25a1853919dac778619fcdb2707892af5cb98e56e3002de7a10679be9`;
rev10: source `icu-rev10-probe.cpp` sha256
`0d7df5ce986f1f23464334b0be7f43acc56ffe900d9d423272f9c29ff918a8be`, log
`icu-rev10-probe.log` sha256
`11dc17a40e11713da76ceb13be28cdfb5878d85eca4024f08de2ddad30c63d5d`;
all exit 0); the warm build (`warm-build-rev6.log`, exit 0) is the
environmental prerequisite (reused: same base, toolchain and target).

1. Red/green UT (this issue's exact matrix plus VT/FF): LIKE/INSTR/SUBSTR/
   REPLACE x {CR,LF,VT,FF,CRLF,NEL,LS,PS} x {m, mu} through the production
   function entries, with the corrected per-separator INSTR oracle
   (3, CRLF 4).
2. Assertion-split counterexamples (pinned, on both engine paths):
   `\r^\n` on `a\r\nb` is true under `m` and false under `mu`; `\r$\n` on
   `a\r\nb` is false under `m` and true under `mu`. These are the consuming
   patterns that distinguish the ICU caret/dollar truth tables; a
   widen-everything or atomic-CRLF repair fails at least one row in each
   pair.
3. Binary CP-1252 controls: byte-0x85 binary subjects with `m` and `mi`
   remain non-matches for `^b` (RED on merged base, amended pins) while
   text NEL matches; binary CR/VT/FF rows match under `m`/`mi` and not
   under `mu`.
4. Search-candidate and structural-start controls (pinned): `^\n` on
   `a\r\nb` false on both paths and both match_types; `^$` on `a\r` false;
   `^$` on `a\n` true (legacy surveillance); INSTR from position 2 returns
   0 for `^\n` and 4 for `^b`; mid-match `\r^\n` remains true. The rev6
   structural blocks pin the invariant-8 classification against both
   failure shapes: the guards `(^\n)`, `(?:^\n)`, `(?m:^\n)`, `\B^\n`,
   `^[\n\r]x` (verified non-singleton SETREF), `^(?:\n)x`, `^(\n)x`,
   case-folded `^\nx`/`^\n1` (mi), `(?!(\r))^\n`, `()\1^\n` on `a\r\nb`,
   the caret-at-EOF rows after trailing CR/VT/FF/NEL, positional INSTR
   parity and the regexp2-routed variants must all STAY non-matching at
   the interior (a token-level analysis, classification on optimized
   output, or a missing candidate layer fails them), and the defect rows
   `^\n|z` (LIKE/INSTR/SUBSTR/REPLACE), `^\nx`, `^[\n]x` (all four entry
   points + positional + the regexp2-routed `(?=[\s\S])^[\n]x`),
   `^(?:\nx)`, `^\n1` (m), `(\r)^\n`, `(?>(\r))^\n`, `(?=\r)\r^\n` and
   `(?:^\n|z)(?=b)` must FLIP to the ICU answers; the green-on-base
   controls `^[\nx]` (LIKE true, INSTR 4, SUBSTR `x`, REPLACE `a\r\nXb`)
   and `()\1^\n` on `x\n\n` (true, INSTR 3) must NOT move
   (over-offering the interior or breaking backreference preservation
   fails them). Engine UTs cover the same contract on all candidate loops
   (NFA, backtrack, one-pass, offset search), including patterns whose
   caret is not initial (`x?^\n` matches at the interior, per ICU's
   generic find) and lookahead-prefixed carets on both engines.
5. Pre-optimization classification pairs (round-5 F2; both engines):
   `^\nx` (string) vs `^(?:\n)x` (line) vs `^(?:\nx)` (string); `^\n1`
   under `m` (string) vs `mi` (line); `^[\n]x` vs `^[\nx]` vs `^[\n\r]x`;
   the lookahead-prefixed composites S25-S28; the Rev 7 branch pairs
   `^(?:\nx|y)` (line, R1) vs `^\nx(?:|y)` (string, R2) vs
   `^(?:\nx|\nx)` (line, D4) vs `^(?:\nxy|\nxz)` (line, D5) vs
   `^\nx(?:y|z)` (string, D10) vs `^(?:\nxy|)` (empty-match only, D13);
   the Rev 7 quantifier pairs `x{0}^\n` (line, R3) vs `^(?:\nx)+`
   (string, R4) vs `^\nx{1}` (line, D1) vs `^\nx?`/`^\nx+` (line,
   D15/D16) vs `^\nx{0}y` (line, D2) vs `^(?:\nx){1}` (string, D11) vs
   `^(?:\nx){2,3}` (string, D7) vs `^\n{2}x` (line) vs `^\n\nx`
   (string, D8/D9) vs `x{0,2}^\n` (bare caret, D12); the Rev 8 flavor
   triple `x{0}^\n` (line, Q1) vs `x{0}?^\n` (bare caret, Q2) vs
   `x{0}+^\n` (bare caret, Q3) plus routed Q1r/Q2r/Q3r; the Rev 8
   sealing pairs `^[\n]x` (string, C1) vs `^\n[x]` (line, C2) vs
   `^[\n][x]` (line, C3) and `^\n(?s)x` /
   `^\n(?s:x)` / `^\n(?i)x` / `^\n(?:x)` (line, C4-C7) vs `(?s)^\nx`
   (string, C9); the Rev 9 form pairs `(?:^\n)+` (line, V1) vs
   `(?:^\n){1,}` (bare caret, V2) with the lazy pair V3/V4 and the
   possessive pair V5/V6, routed V1r/V2r/V5r/V6r, and the
   star/question equivalence rows V7/V8/V9/V10 (both spellings, empty
   match only); the Rev 9 empty-class rows
   `^(?:[^\x00-\x{10ffff}]|\nx)` (string, E1) vs
   `^[^\x00-\x09\x0b-\x{10ffff}]x` (string, E2, negated singleton) vs
   `^(?:[^\x00-\x{10ffff}]|x)` (line, E3, dead branch discarded); the
   Rev 9 escape-identity pair `^\n\v` (line, X1) vs `^\n\x0b`
   (string, X2), each with routed and mu rows; the Rev 10
   assertion-seal rows `^\n$\nx` (line, A1) vs `^\n\nx` (string, A3)
   and `^\n\B\nx` (line, A2) with routed A1r/A2r/A3r and the six-row
   mu family, and the Rev 10 backreference-seal rows
   `(?=())^\n\1x` (line, B1) vs `(?=())^\nx` (string, B2) and
   `(?=(?<e>))^\n\k<e>x` (line, B3) vs `(?=(?<e>))^\nx` (string,
   B4) with the B1-mu..B4-mu family; every pair pinned through
   LIKE and INSTR position oracles plus SUBSTR/REPLACE for the defect
   carriers. A classification that reads only
   optimized instructions, the parsed syntax tree, the fork's post-parse
   (already reduced) tree, the post-translation pattern text, BmPrefix
   output, a first-literal-run rule, a flat run-contiguity model without
   the quantifier split, a flavor-blind zero-count rule, a form-blind
   plus-equals-interval rule, a class model without cardinality-0
   resolution or complement arithmetic, an engine-identical escape
   lowering without the ICU BACKSLASH_V identity, a sealing model that
   treats sealing as derivable from atStart/length effects and leaves
   assertions or backreferences unsealed, or a
   linear-adjacency run rule without compiler-boundary sealing fails at
   least one row per pair on each engine.
6. regexp2 admission parity and identity (pinned): the
   50,000 / 74,000-rune subjects return true with no error and the
   150,000-rune subject returns the existing states error, identically on
   pre-merge main, the merged base AFTER this repair, and head; the
   wide-bearing 74,000-rune and anchor-dense 30,000-rune subjects keep the
   plain-pattern admission verdicts. Mode-identity UTs compile each pinned
   pattern (`(?m)^b(?=b)` 7 tracks, `(?m)`+32x`^$`+`(?=\n)` 7, `(?m)^` 3,
   `(?m)(?:^\n|z)` 5) in Unix and wide modes and demand identical
   TrackCount, `Codes` length and expression bytes; production-identity
   UTs demand the same pins through
   `regexpPatternWithPureMatchType` + `translateRegexp2Pattern`
   composition. The deterministic overlap/reject/release UT pins the
   global accumulator: two overlapping 74k reservations admitted exactly
   as today, a failing reservation registering nothing, releases restoring
   the baseline. These rows are RED on the merged base today only through
   the production-composed pins (the measured rewrite deviation); the
   plain pins are green on base and must stay green.
7. Engine suite: `GOWORK=off go test ./pkg/sql/plan/function/internal/regexp`
   and `-race`: stdlib differential (default mode), wide truth table over
   all eight separators and BOTH assertion kinds with the split CRLF
   interior (all executors), the invariant-8 classification table from the
   raw-pattern scanner (captures, scoped groups, singleton vs non-singleton
   classes, boundary prefixes, alternation, literal-run length, fold
   boundaries, `\Q...\E`, generated `(?m)`/`(?im)` prologues,
   branch-forwarded initial-path accumulation (R1/R2/D4/D5/D10/D13),
   flavor-dispatched quantifier lowering (run split, ordinary-only
   zero-count deletion with lazy/possessive counted-loop retention,
   inline copies, counted-loop bodies: R3/R4/D1/D2/D7/D8/D9/D11/D12/
   D15/D16 plus the Rev 8 flavor triple Q1/Q2/Q3) plus FORM dispatch
   (the Rev 9 pairs V1/V2, V3/V4, V5/V6 and the star/question
   equivalence V7-V10) and literal-run
   sealing at compiler boundaries (class opens with singleton lowering,
   inline flag sets with and without flag changes, group opens: the
   Rev 8 directional pairs C1/C2/C3, C4-C7 and C9) plus the Rev 9
   cardinality-resolved class events (empty-class failure E1/E3,
   negated singleton E2) and ICU escape identity (`\v` vs `\x0b`:
   X1/X2) plus the Rev 10 assertion/backreference seal rows (dollar
   and `\B` sealing the pending run while zero-width: A1/A2 vs A3
   with routed A1r/A2r/A3r and the mu family; numbered and named
   backreferences sealing while possibly-empty: B1/B3 vs B2/B4 with
   the mu family) per the shared seal/atStart/length event table), the
   init-mask on/off per classified shape, adjacent non-CRLF terminator
   pairs (e.g. `\r\v`), EOF exceptions (caret needs a following character
   after a wide terminator; LF-at-EOF caret legacy; dollar at EOF), binary
   alphabets (U+E085 and U+2026 non-terminators; VT/FF pass-through),
   scoped flags, offset-context search starting mid-line and between CR
   and LF, concurrent reuse.
8. Vendored-regexp2 engine suite: `GOWORK=off go test
   ./pkg/sql/plan/function/internal/regexp2/...` and `-race`: the fork's
   own test suite vendored unchanged as the Unix-mode differential
   baseline; new line-mode tests - wide truth tables for Bol/Eol over all
   separators with the CRLF interior split; the pre-translation
   event-scanner classification table (round-5 finding F3, round-6
   finding F1 and round-7 finding F1 explicitly): an event-stream UT
   asserts the scanner's sequence over the ORIGINAL pattern text for
   pinned patterns equals the expectation BEFORE
   translateRegexp2Pattern's possessive rewrite, parsing, or the fork's
   eager reductions can erase any of it; lookahead/lookbehind blocks
   skipped (including nested ones), atomic and capture bodies traversed
   (the `(?>(\r))^\n` / `(\r)^\n` / `(?=\r)\r^\n` contrast family),
   singleton vs non-singleton vs negated set events with the class-open
   seal (C1/C2/C3 pins), quantifier events with parsed FORM AND
   min/max AND
   flavor covering the lazy and possessive zero-count rows the
   translator would otherwise hide and the plus-vs-interval placement
   rows the form-blind record cannot separate (R3/R4/D1/D2/D11/D15
   pins plus
   Q1/Q2/Q3/Q1r/Q2r/Q3r plus V1/V2/V5/V6/V1r/V2r/V5r/V6r and the
   V7-V10 equivalence), cardinality-resolved class events including the
   empty-class failure event pinned through the fork's braced-hex
   `\x{...}` syntax (E1/E2/E1r/E2r with the E3 control), ICU escape
   identity (`\v` vs `\x0b`: X1/X2/X1r/X2r), assertion and
   backreference seals pinned through the scanner's event stream
   (A1/A2 vs A3 with routed A1r/A2r/A3r; B1/B3 vs B2/B4 - the engine
   semantics behind the lookingAt rows included: the lookahead's empty
   capture preserved at URX_LA_END and an empty reference matching
   zero-length), alternation-branch accumulation
   (R1/R2/D4/D5), backreference preservation vs conditional
   destruction - including EMPTY backreference controls
   (`()\1^\n` on both subjects), inline flag sets sealing even without
   a flag change (C4-C7/C9 pins), node-level fold boundaries, and the
   wrappers' generated prologues; the candidate policy
   vs BmPrefix interplay (`^\n` line-policy vs `^\nx` string-exempt vs
   `^[\n]x` string-exempt via the mirrored singleton rule vs
   `^(?:\n){2}x` line-policy - quantified runs do not exempt), positional
   search offsets, strict `mu` Unix behavior, timeout behavior under the
   fastclock patch, and concurrency.
9. Corner/combination behaviors: empty lines from consecutive separators,
   terminal separators at EOF (both assertion styles), occurrence counting
   and replacement expansion/aggregate budget over multiline subjects,
   `n`+`m` and `i`+`m` flag combinations, position arguments starting
   mid-line and mid-terminator (between CR and LF of a CRLF pair), and
   >1 MiB subjects on the regular path.
10. BVT (user-visible SQL): extend
    `test/distributed/cases/function/func_regular_like.test` (+result) and
    `test/distributed/cases/operator/regexp_operator.test` (+result) with
    the eight-separator matrix, the CRLF-interior consuming patterns, the
    search-candidate and structural-start controls, and the binary 0x85
    control across all four entry points and both match_types, run via
    mo-tester against an isolated instance (separate runtime dir and port
    plan; teardown verified by released listeners). Note: a test harness
    that cannot express raw VT/FF bytes in SQL files may need HEX/UNHEX or
    CHAR() constructors (e.g. CHAR(0x0B) with explicit charset), matching
    the issue's CONVERT(0x0D USING utf8mb4) technique; every generated
    result is inspected against these oracle tables.
11. Owning-package regression: full `pkg/sql/plan/function` tests (normal
    and `-race` for both internal engine packages), plus directly affected
    LIKE/positional/replacement consumers; incremental static checks
    (gofmt/vet/lint) on the changed closure per mo-dev G-STATIC-INCREMENTAL;
    the amended merged suites (lines_test.go, regexp_multiline_test.go and
    the rewrite-specific cases) run in the same pass.
12. Performance sanity per the Performance model acceptance criteria on the
    existing benchmark-shaped fixtures (anchored prefix, long text, binary):
    warmed fixtures byte-identical between base and head, medians within
    1.25x, no additional per-row allocations, and the compile-time
    classification scans bounded by pattern size (no per-row cost on either
    path).

## Open questions and risks

1. (Resolved in Rev 3) Binary case-fold byte 0x85: MySQL's regexp facade
   presents every binary operand as CP-1252 regardless of case folding
   (verified primary source), so 0x85 is U+2026 and never a terminator;
   binary m/mi controls in the validation matrix pin this. The merged
   implementation made the contrary choice deliberately; this design
   amends it and its UT pins with the facade evidence (finding per
   invariant 6, plus the red rev6 rows proving user-visible impact).
2. LF-at-EOF: existing MatrixOne RE2 behavior (caret matches after trailing
   LF) differs from ICU's no-phantom-line rule (probe row T12 false).
   Preserved deliberately (invariant 4) to avoid changing long-established
   behavior beyond this issue; restricted to the caret and to LF only, and
   carried by a named surveillance row so any future alignment is a
   separate scoped change.
3. Amending merged decisions: this revision changes behavior and test pins
   delivered by merged PR #30646 (VT/FF inclusion, binary 0x85 removal,
   CR|LF-interior caret, removal of the regexp2 anchor rewrite, plus the
   new candidate layer). Each amendment carries independent evidence
   (live ICU 77.1 probes, the facade source, the production admission
   measurement) and a reconciliation addendum is added to that PR's design
   document; reviewers of that PR are the natural reviewers here. No part
   of the merged UT/BVT outside the regexp multiline files is touched.
4. Engine provenance and maintenance: the private RE2 copy pins Go 1.27.1
   content hashes (merged); the vendored regexp2 pins upstream
   `dlclark/regexp2` v1.10.0 plus the XuPeng-SH fastclock patch (fork
   pseudo-version v1.10.1-0.20261004024751-718f43e031fe) with per-file
   hashes and a sync procedure in each package README. A Go toolchain
   upgrade re-derives the RE2 copy and re-runs the stdlib differential; an
   upstream regexp2 bump is a deliberate sync that re-applies the
   fastclock patch and the line-mode patch, re-runs the fork differential
   suite, and must keep the mode-identity UTs green (identical
   TrackCount/code per pattern across modes) - those tests are the
   tripwire if codegen starts depending on the mode. The vendored copy
   carries upstream's MIT LICENSE; nothing else in the repository imports
   regexp2.
5. Adjacent-territory note (scoped out): `u` without `m` selects
   `URX_CARET`/`URX_DOLLAR_D` in the ICU compiler; MatrixOne currently drops
   a lone `u` and keeps existing behavior. Invariant 2 keeps every mode
   without `m` byte-identical to today; nothing in this change depends on
   or alters that territory.
6. Residual fidelity risk (reduced in Rev 6 and Rev 7, recorded): every
   disputed classification row is now measured on live ICU 77.1 through
   the same API MySQL uses (`icu-rev6-probe.log`, 56 rows;
   `icu-rev7-probe.log`, 33 rows; both exit 0), and MySQL 8.0.46 bundles
   exactly ICU 77.1 (verified tag listing); remaining risk is limited to
   MySQL-layer differences outside uregex_find (none observed in the
   issue's own matrix, which matches ICU row-for-row on the overlapping
   cells). The pinned harness rows remain the executable contract; any
   row later contradicted by a live MySQL measurement is a targeted
   follow-up classified by the same `matchStartType` procedure, not a
   redesign.
7. Scanner fidelity risk (new mechanism): the raw-pattern event scanner
   must mirror Go regexp SYNTAX lexing on the RE2 route (escapes,
   `\Q...\E`, class complement WITH cardinality-0 resolution and
   negated-singleton complement arithmetic - round-8 finding F2, Perl
   classes, flag toggles, quantifier
   FORM/min/max/flavor parsing - round-8 finding F1, ICU escape identity
   for shared spellings such as `\v` vs `\x0b` - round-8 finding F3) and
   the ICU/.NET construct superset on the regexp2
   route (lookarounds, atomic and named groups, conditionals, `\k<>`/
   `\g`/numbered backreferences, `\X`, `\U`, `\c`, octal and braced-hex
   escapes, free-spacing and comments), and the shared core must mirror
   ICU's sealing - the complete literal-flush set of the shared
   seal/atStart/length event table, assertions and backreferences
   included (round-9 finding F1) - plus form- and flavor-specific
   lowering, failure events and
   branch forwarding.
   Mitigations: the scanner lexes for event kinds only (never for match
   semantics) with the routing trigger set aligned to
   requiresRegexp2Pattern, the core is validated against the live probe
   row sets (R1-R4, D1-D16, P1-P4, Q1-Q3 plus routed/mu, C1-C9 plus
   routed/mu, V1-V10 plus routed/mu, E1-E3 plus routed/mu, X1/X2 plus
   routed/mu, A1/A2/A3 and B1-B4 plus routed/mu) on both routes,
   resolves any out-of-model construct
   conservatively (no line-anchoring, no exemption - behavior degrades
   to today's candidate enumeration), and the validation matrix pins
   adversarial syntax rows (escapes of anchors, quoted literals,
   `\Q\r^\n\E`, merged-suite escape cases, sealed-boundary pairs) on
   both engines.
8. Metadata preservation risk (round-7 and round-8 findings): the
   descriptor
   must be computed from the ORIGINAL pre-translation pattern
   (translateRegexp2Pattern's possessive rewrite would hide flavor from
   any later capture point; regexp_icu.go:1007-1025) and the sealed-run
   boundaries must cover every fixLiterals call class (class open,
   flag sets, group boundaries, alternation, anchors and boundary
   assertions of EVERY kind, numbered/named backreferences, pattern
   end; quantifier split), while the event stream must additionally
   carry quantifier
   FORM (round-8 F1: plus vs interval placement is invisible in
   {min,max,flavor} alone), resolved class cardinalities including the
   empty-class failure event (round-8 F2) and ICU escape identity for
   shared spellings (round-8 F3: `\v` never a run literal). Mitigations:
   an event-stream UT asserts the recorded
   sequence - including FORM, flavor, class cardinality, escape identity
   and seal placement - for the pinned
   possessive/lazy/plus/interval/class/flag/escape patterns and the
   Rev 10 seal rows (the assertion pairs A1/A2 vs A3 and the
   backreference pairs B1/B3 vs B2/B4); the routed
   probe rows Q1r-Q3r,
   C1r-C5r, V1r/V2r/V5r/V6r, E1r/E2r, X1r/X2r and A1r/A2r/A3r pin
   identical
   classification through the translation path;
   the conservative opaque-event fallback bounds every lexing miss
   toward today's behavior (no anchoring, no exemption); and the
   translator gains a sync-tripwire UT re-asserting the pinned event
   sequences if its rewrite logic is ever touched.

## Change log

- Rev 1 (2026-10-24, sha256
  67bf2ac81e3dd4cce57dc602e429857adc207ab4501a1bd06262e54b8cdd4ed9):
  initial design after reproduction and root-cause analysis.
- Rev 2 (2026-10-24, sha256
  6e81eb6a9fb6ed0b54877c7bbafeae73fc1164053a887b08488163eace2f9865):
  correct the CRLF INSTR oracle to 4 and pin harness rev2 as the red/green
  oracle; extend the wide terminator set to the full ICU UREGEX_MULTILINE
  set (VT/FF included) with verified MySQL/ICU primary evidence; record the
  consuming-atom non-goal explicitly; document adjacent-terminator
  (non-CRLF) boundary behavior.
- Rev 3 (2026-10-24, sha256
  c72a777d0ef29f97698b6c91f7e95d61839be39900ac2929030a44964d58771d):
  split the CRLF truth table per assertion (caret holds between CR and LF
  mid-match; only the dollar excludes it) with harness rev3 (sha256
  b67464decffc899d0ef578aa6f7020af4f7079afebd363d9f2dd7be12eb4adc2)
  pinning the consuming-pattern counterexamples, and remove the binary
  0x85 NEL rule per the MySQL CP-1252 facade.
- Rev 4 (2026-10-24, sha256
  27ac7f565475136bd420e326fa0fcd1b2757adddd704674be7499815d6087555):
  add the search-candidate contract (START_LINE find loop never offers the
  CRLF interior) with harness rev4 (sha256
  61c1df8b97a92d55e88f06e188b2ce75f881fdea5147847ad9533f6b847f95d4)
  controls, and recompute every SQL-visible admission decision on the
  original user pattern after measuring the Rev 3 rewrite's TrackCount
  inflation.
- Rev 5 (2026-10-24, sha256
  356e7a551e44384ad1dfa11187f374b4b299b81d0b5185769653be7d4d3ecbfe):
  replace the first-token rule with ICU's structural start analysis on both
  engines; prove the fixed admission multiplier unprovable (premise probe
  `trackcount-probe-rev5.log`: 32 literal `^$` pairs still TrackCount 7);
  abandon the regexp2 pattern rewrite and vendor the pinned fork with a
  native line mode (admission preservation by construction); harness rev5
  (sha256 e79946cd478ac167e0ab2f8dc452ba136125d565a4c0763b96e5b9def1b334dc)
  pinned red on base `463803dfbb` (repro-base-go-unit-rev5.log, exit 1).
- Rev 6 (2026-10-24): address the fifth design review round; the base moved
  (PR #30646 merged, base now origin/main
  6fb6531432aaec8c1fbd98a30364941559e0e1d6), and all three findings are
  verified against primary sources and LIVE ICU 77.1 measurement before
  acting.
  (1) Singleton-class oracle corrected (finding F1): the installed ICU 77.1
  probe returns `^[\n]x` TRUE at [2,4) on `a\r\nxb` (row S08) because the
  compiler lowers singleton sets through literalChar
  (icu-77.1-regexcmp.cpp:2410-2420) and the joined 2-char run takes
  START_STRING priority; the row moves from guard to defect across all
  four entry points (LIKE/INSTR/INSTR-pos2/SUBSTR/REPLACE) plus the
  regexp2-routed composite (S25), and the negative SETREF control is now
  the VERIFIED non-singleton sets `^[\n\r]x` (S11 false; new guard) and
  `^[\nx]` (S10 true [3,4); positional controls pin INSTR 4/SUBSTR
  `x`/REPLACE `a\r\nXb` against over-offering).
  (2) Classification is captured BEFORE optimization (finding F2): probes
  require `^\nx` true vs `^(?:\n)x` false vs `^(?:\nx)` true (S06/S13/S14)
  and `^\n1` m-true vs mi-false (S16/S17, composites S27/S28); Go parse
  removes groups (stdlib syntax/parse.go:1410-1437) and compile clears
  ineffective FoldCase (compile.go:277-280), and the fork's Reduce strips
  groups, merges literals and lowers singleton sets (syntax/tree.go
  306-375/461-468; parser.go:1183), so invariant 8 now derives the
  descriptor from the pre-Reduce RegexNode tree (regexp2 path) and a
  prefix-scoped raw-pattern-text scanner (RE2 path) with an explicit
  mirrored singleton rule and group-boundary/fold/quantifier flush rules;
  both distinguishing-pair families are added to both engine validation
  plans.
  (3) regexp2 lookaround recognition is unambiguous by construction
  (finding F3): the writer emits identical Setjump prologues for
  lookarounds and consuming atomic groups (fork syntax/writer.go:283-317),
  while probes S19-S21 prove atomic bodies are traversed (`(?>(\r))^\n`
  and `(\r)^\n` and `(?=\r)\r^\n` all true at [1,3)) and lookarounds
  skipped; the mapping now names Ref (opcode 13; preserves atStart, probe
  S22/S23 pinned) and Testref (opcode 37; conditionals only, destroys
  conservatively), and the validation plan gains contrasting
  atomic/lookaround pairs, nested-block controls, empty-backreference
  controls and generated-prologue controls.
  Merged-base integration: the merged implementation is the new starting
  point (private RE2 engine, `u` preserved, keyed cache, offset search);
  amendments are VT/FF classification in every alphabet, binary 0x85
  removal (with the merged UT pins at lines_test.go:59-60 and
  regexp_multiline_test.go:103-110 flipped), the CR|LF-interior caret fix,
  the invariant-8 descriptor + candidate policy on both engines, and the
  REMOVAL of the merged regexp2 anchor rewrite, whose admission regression
  is now measured LIVE through the production composition (premise probe
  `trackcount-probe-rev6.log`, exit 0: TrackCount 7->37 for
  `(?m)^b(?=b)`, 7->1671 anchor-dense, 3->33 bare caret; dense 30k subject
  at 50,131,671 states REJECTED; 74k subject at 2,738,037 REJECTED; the
  50k review case erroring through the production entry) while `mu` stays
  identity (7). Alternative 2a records the third falsification.
  Harness rev6 (sha256
  `0c6543ea1c52741e61d64dbaf3737478e1ec690562bf7da73e16fb529450a4ae`)
  replaces rev5 as the pinned red/green oracle: recorded red run on the
  merged base `repro-base-go-unit-rev6.log` (exit 1; per-row attribution -
  only the defect carriers fail: VT/FF on both routes and in binary, the
  CRLF-interior caret on both routes, binary 0x85 m/mi, all structural
  defect rows, the 50k admission control, and the production-composed
  TrackCount/boundary pins; every guard, the green-on-base controls, the
  plain pins and the global arithmetic stay green). Live ICU probe source
  and log pinned (`icu-rev6-probe.cpp` sha256
  `7ab777bbc5dcdad13f30a57b66ac12bc7f510e7a2c49ae6d52a4b3493541883c`,
  `icu-rev6-probe.log` sha256
  `4a395a50e247526206092ca28bd23c0537748a1ac314c29ae6e8ccf0238aadf3`);
  warm build `warm-build-rev6.log` (exit 0).

- Rev 7 (2026-10-24): address the sixth design review round; all three
  findings are verified against primary sources and LIVE ICU 77.1
  measurement (icu-rev7-probe.log, 33 rows, exit 0) before acting.
  (1) Capture regexp2 structure before its eager parser reductions
      (finding F1): verified TRUE - the pinned fork reduces at child
      insertion (regexNode.addChild calls child.reduce() and stores the
      reduced node, fork syntax/tree.go:168-173; the parser routes every
      atom, quantified unit, group body and alternation through addChild
      while parsing, fork syntax/parser.go: addConcatenate ~2007-2013,
      addConcatenate3 ~2016-2021, addGroup ~2051-2066), so a pre-Reduce
      tree walk at code-construction time cannot exist and the round-6
      capture point is impossible as specified. Invariant 8 now
      classifies from PARSER-EVENT metadata recorded inside the vendored
      parser immediately before each reduction and retained alongside the
      compiled pattern; ordinary reductions, generated code and admission
      inputs remain unchanged (invariant 9). Alternative 2d records the
      rejected tree walk.
  (2) Derive string exemption from all possible initial paths (finding
      F2): verified TRUE by probes - `^(?:\nx|y)` FALSE on `a\r\nxb`
      (R1) while `^\nx(?:|y)` is TRUE at [2,4) (R2); the Rev 6
      first-literal-run rule exempts both. Invariant 8 now implements
      ICU's accumulator exactly: currentLen is the MINIMUM match length
      over all paths reaching an op (branch forwarding through jumps and
      state saves, icu-77.1-regexcmp.cpp:2954-3006); numInitialStrings
      counts initial case-sensitive literal strings at +1 (URX_STRING
      case 3007-3035) against every other initial content at +2; the
      ladder selects START_STRING only for numInitialStrings == 1 with
      minMatchLen > 0 (3155-3188). Distinguishing rows added to the
      oracle and both validation plans: same-value branch counterexample
      D4 (`^(?:\nx|\nx)` false - counting, not string dedup), D5
      (`^(?:\nxy|\nxz)` false), branch-after-string D10 (`^\nx(?:y|z)`
      true [2,5)), empty-branch minMatchLen case D13 (`^(?:\nxy|)`
      empty match [0,0) only), plus positional INSTR/SUBSTR/REPLACE pins
      and the regexp2-routed variants P1/P2 per the review action.
      Alternative 2e records the rejected first-run rule.
  (3) Account for quantifier lowering in structural classification
      (finding F3): verified TRUE by source and probes -
      compileInlineInterval (regexcmp.cpp:2506-2560) deletes zero-count
      content AFTER fixLiterals(true) has split the pending literal run
      (1898-1922, called from blockTopLoc 2139-2169 at every quantifier)
      and inlines small single-op intervals as mandatory plus
      state-save-guarded copies; compileInterval (2441-2504) emits
      counted loops whose bodies matchStartType walks once (CTR_INIT
      3046-3070 forwards the minimum past min==0 loops). Probe pins:
      `x{0}^\n` false (R3 - zero-count deletion preserves atStart),
      `^x{0}y` true [3,4) on `a\r\nyb` / false on `a\r\nb`
      (R3b/R3c), `^(?:\nx)+` true [2,4) (R4) and `^(?:\nx){2,3}` true
      [2,6) (D7 - min>0 loop bodies count once), `^\nx{1}` / `^\nx?` /
      `^\nx+` false (D1/D15/D16 - any quantifier on the run's last char
      splits it), `^\nx{0}y` false (D2 - the split precedes the
      deletion), `^(?:\nx){0}y` true [3,4) (D3 - wholesale group
      deletion), `^(?:\nx){1}` true [2,4) (D11), `^\n{2}x` false vs
      `^\n\nx` true [2,5) on the same subject (D8/D9 - inline copies
      never form a string; textual adjacency does), `^(?:\nx)*` empty
      [0,0) (D6/D6-mu - minMatchLen 0 blocks the string), `x{0,2}^\n`
      true [2,3) (D12/D12b; mu false D12-mu - the optional prefix is
      control-flow, so the caret is a bare mid-pattern one), plus mu
      asymmetry rows R2-mu/D7-mu/D9-mu false and routing-prefix
      invariance P1-P4. Alternative 2f records the rejected blanket
      rules.
  Mechanism consolidation: ONE shared classification core (quantifier
  lowering, branch-forwarded initial-string accumulator, atStart
  classes, priority ladder, sound stopping condition) consumes the
  shared event vocabulary; two front ends emit it (the RE2
  raw-pattern-text scanner; the vendored parser-event recorder). Probe
  artifact rev7 pinned: source icu-rev7-probe.cpp sha256
  `eab24d13f3ca38b31a46a7fdb529851625b3c64b7cde00cd2ffde777e33b699a`,
  log icu-rev7-probe.log sha256
  `2d8d2547549e0ebc37234ae4c41eabf5ced45bed3a5eca8d65e69e90ac793961`
  (exit 0, 33 rows). Harness rev7 (sha256
  `95cab3c10152663f9f8d67e3d4217f4d192142ba71f2a4750b29bb4a02a510f5`)
  extends rev6 with every R/D/P row and is recorded red on the same
  merged base (repro-base-go-unit-rev7.log, exit 1 by design; per-row
  attribution: the rev7 defect carriers fail - branch rows R2/P2/D10 and
  quantifier rows R4/D14/P4, D7 and its routed variant, D11, D9 and its
  routed variant, D12/D12b, with every positional INSTR/SUBSTR/REPLACE
  pin - while both rev7 guard blocks PASS (R1/D4/D5/D13/R2-mu;
  R3/R3b/R3c, the D1/D15/D16 quantifier-split pins, D2/D3/D8, D6/D6-mu
  and the mu asymmetry rows); the routed contract-false rows P1/P3 run
  inside the defect blocks and stay green on base (silent-green controls
  verified by construction: fatal require wrappers plus later red rows in
  the same blocks executed); the carried rev6 blocks reproduce the
  recorded rev6 attribution - defect carriers red, guards and arithmetic
  controls green). Warm build evidence reused unchanged per the
  validation-evidence contract (warm-build-rev6.log, exit 0; inputs
  unchanged: same base, toolchain and make cgo target).
- Rev 8 (2026-10-25): address the seventh design review round; both
  findings verified TRUE against primary sources AND live ICU 77.1
  measurement before the design was changed, with falsifying probe rows
  recorded for every rejected rule.
  (1) Preserve quantifier flavor in classification lowering (finding
      F1): verified TRUE - Rev 7 recorded quantifiers as bare
      {min,max} and deleted zero-count units universally, but ICU
      dispatches per flavor (regexcmp.cpp:1049-1092): ordinary
      intervals try compileInlineInterval (2506-2560: zero-count
      blocks deleted wholesale) with a compileInterval counted-loop
      fallback, while lazy intervals (doNGInterval 1089-1092) and
      possessive intervals (doPossessiveInterval 1055-1087, wrapped in
      atStart-neutral STO_SP/LD_SP with the accumulator cases at
      2751-2754 and 3046-3071) are ALWAYS compiled as counted loops,
      even at {0}. Live probes (icu-rev8-probe.log, 26/26 rows): on
      `a\r\nb` under m, `x{0}^\n` is false (Q1, carried R3) but
      `x{0}?^\n` and `x{0}+^\n` are true at [2,3) (Q2/Q3) - the
      counted loop destroys atStart, leaving a bare mid-pattern caret;
      mu rows and the routed `(?=[\s\S])` variants agree (Q1r-Q3r).
      Compounding this, the SQL translator rewrites possessive
      intervals into atomic groups (regexp_icu.go:1007-1025) BEFORE
      any parser sees them, while requiresRegexp2Pattern
      (regexp_icu.go:142) routes them, so the Rev 7 vendored
      parser-event recorder can never observe the flavor. Rev 8
      therefore moves capture one stage earlier on BOTH routes: ONE
      raw-pattern-text event scanner reads the ORIGINAL pre-translation
      pattern (RE2 dialect on the private-engine route, the ICU/.NET
      routing superset on the regexp2 route), records quantifiers as
      {min,max,flavor}, and the shared core performs flavor-dispatched
      lowering (ordinary-only zero-count deletion; lazy/possessive
      retained as atStart-destroying loops of any min). Rev 7's
      two-front-end composition is recorded as rejected alternative 2g
      with these falsifying rows.
  (2) Model literal-run sealing before singleton character classes
      (finding F2): verified TRUE - Rev 7 merged textually adjacent
      literals, but ICU seals the pending literal run at compiler
      boundaries (fixLiterals(false)): class OPEN (regexcmp.cpp:
      1653-1655, before compileSet lowers a singleton through
      literalChar at 2410-2420), inline flag sets (1494-1505, "using
      the not-yet changed match mode" - sealing even when no flag
      changes), scoped-flag group opens (1507-1527), plain group
      opens, alternation, properties, backreferences, dot, and pattern
      end. Live probes on `a\r\nxb` under m: `^[\n]x` true [2,4)
      (C1 - the singleton joins the NEW run, forming the exempt string
      `\nx`; carried S08) vs `^\n[x]` false (C2) vs `^[\n][x]` false
      (C3) - the directional pair Rev 7's linear-adjacency rule cannot
      separate; `^\n(?s)x` / `^\n(?s:x)` / `^\n(?i)x` / `^\n(?:x)`
      false (C4-C7: flag sets and group opens seal after the
      one-character run) vs `(?s)^\nx` true [2,4) (C9 - the seal
      precedes the run, which then forms intact); routed C1r-C5r and
      mu rows agree. The event vocabulary gains class open/close with
      singleton content (open seals), inline flag sets (always
      sealing), and group open/close by kind (sealing); sealed runs
      emit string units only at length >= 2 case-sensitive per
      emission 1933-1958, matching the accumulator 3001-3020 weights.
  Mechanism consolidation stands as Rev 7 specified, with the two
  changes above: ONE shared classification core (atStart classes,
  flavor-dispatched quantifier lowering, boundary-sealed run
  formation, branch-forwarded initial-string accumulator, sound
  stopping condition) consuming ONE pre-lowering event vocabulary
  emitted by ONE pre-translation raw-pattern event scanner per route
  dialect; conservative opaque-event fallback unchanged; invariant 9
  admission identity untouched (scanner and descriptor are
  classification-only). Probe artifact rev8 pinned: source
  icu-rev8-probe.cpp sha256
  `da68c47fbc7941d5475dac7e43c44590d5f3f36049a9d889dd92e107d7933366`,
  log icu-rev8-probe.log sha256
  `90535da4d0ee15822aae3a7b8e99279d2f3d76b728cf7bef16f98341aea613a4`
  (exit 0, 26/26 rows as expected). Harness rev8 (sha256
  `4af413e5f7f91c92feb332ec0aec3923026169a6c4ccc3564142e935f9c82d36`;
  18 test functions = 14 carried verbatim + 4 new blocks) is recorded
  red on the same merged base 6fb6531432 (repro-base-go-unit-rev8.log
  sha256 `e59d4c6906af9a6cae175d6e8bf71a37e8ec39116022e7908d9892cb3f622a18`,
  exit 1 by design; per-block attribution: the two new defect blocks
  fail with exactly the Q2/Q3/Q2r/Q3r and C1/C1r/C9 error lines while
  their Q1 and C2/C3/C2r/C3r silent-green controls raise none, both
  new guard blocks PASS, and every carried rev4/rev6/rev7 block
  reproduces the recorded attribution - defect carriers red, guards
  and arithmetic controls green). Warm build evidence reused unchanged
  per the validation-evidence contract (warm-build-rev6.log, exit 0;
  inputs unchanged: same base, toolchain and make cgo target).
- Rev 9 (2026-10-25): address the eighth design review round; all three
  findings verified TRUE against primary sources AND live ICU 77.1
  measurement (icu-rev9-probe.log, 32 rows, exit 0) before acting.
  (1) Preserve quantifier form as well as bounds and flavor (finding
      F1): verified TRUE - Rev 8 kept {min,max,flavor}, making `+` and
      `{1,}` indistinguishable, but ICU places a plus loop's control
      AFTER its body (doPlus 795-845; doNGPlus 847-856;
      doPossessivePlus 1371-1400 with its topLoc-inserted atStart-
      neutral STO_SP) while an interval places its CTR_INIT BEFORE the
      body (compileInterval 2440-2504; the accumulator CTR case
      3046-3071 clears atStart). Live probes on `a\r\nb` under m:
      `(?:^\n)+` false (V1) vs `(?:^\n){1,}` true [2,3) (V2); lazy
      `(?:^\n)+?` false (V3) vs `(?:^\n){1,}?` true [2,3) (V4);
      possessive `(?:^\n)++` false (V5) vs `(?:^\n){1,}+` true [2,3)
      (V6); V2/V4/V6 false under mu; routed V1r/V5r false vs V2r/V6r
      true [2,3); star/question forms lead with branch/save guards
      (859-1002, 1402-1463), form-equivalent to the {0,}/{0,1}
      spellings (V7/V8/V9/V10 all match only the empty string [0,0)).
      The event vocabulary gains quantifier FORM
      (star|plus|question|interval) and the shared core gains FORM
      dispatch (plus body-first with trailing control; star/question
      leading guards; interval init-first with the Rev 8 flavor
      dispatch inside the form). Alternative 2h records the falsified
      form-blind record.
  (2) Model empty character classes as failure events (finding F2):
      verified TRUE - compileSet's cardinality switch
      (regexcmp.cpp:2403-2437) emits URX_BACKTRACK for an empty
      resolved set (2407); the accumulator BACKTRACK case (2970-2975)
      forwards the following op's length, contributes NOTHING to
      numInitialStrings and marks its own path atStart-destroyed.
      Live probes on `a\r\nxb` under m: `^(?:[^\x00-\x{10ffff}]|\nx)`
      true [2,4) (E1 - one initial string survives the dead branch;
      a consuming +2 or opaque treatment raises numInitialStrings to
      3 and strips the exemption, the review's exact counterexample)
      and `^(?:[^\x00-\x{10ffff}]|x)` matches `x` at [3,4) only
      (E3 - control: dead branch never matches; genuine line start);
      the negated singleton `^[^\x00-\x09\x0b-\x{10ffff}]x` true
      [2,4) (E2 - complement arithmetic resolves it to exactly {LF},
      the C1 equivalence); E1/E2 false under mu, E3-mu [3,4); routed
      E1r/E2r true [2,4) through the fork's braced-hex `\x{...}`
      syntax (syntax/parser.go:1701-1707). Class close events now
      RESOLVE cardinality 0/1/greater with complement arithmetic:
      cardinality 0 records the failure event, 1 (negated singletons
      included) joins the new pending run, greater stays a +2 set.
      Alternative 2i records the falsified zero-cardinality omission.
  (3) Preserve ICU escape identity in classification metadata
      (finding F3): verified TRUE - Go lowers `\v` and `\x0b` to the
      same VT literal (the vendored core does likewise), but ICU
      emits URX_BACKSLASH_V for `\v` via doBackslashv
      (regexcmp.cpp:1236-1239: fixLiterals(false) seals the pending
      run) whose accumulator case (2883-2897) counts it +2 like a
      set, while `\x0b` lowers through literalChar and JOINS the run.
      Live probes on `a\r\n<VT>b` under m: `^\n\v` false (X1) vs
      `^\n\x0b` true [2,4) (X2); both false under mu
      (X1-mu/X2-mu); routed X1r false, X2r true [2,4). The
      classification contract is MySQL's, hence ICU's, so the shared
      event vocabulary records the ICU BACKSLASH_V event for `\v` on
      BOTH routes while the equivalent hex/octal/unicode and braced-hex
      escapes record run literals; the engines' own consuming-atom
      semantics for `\v` matching stay unchanged - the documented
      non-goal. Alternative 2j records the falsified engine-identical
      escape lowering.
  Mechanism consolidation stands as Rev 8 specified, with the three
  changes above: ONE shared classification core (atStart classes,
  FORM- and flavor-dispatched quantifier lowering, boundary-sealed
  run formation with cardinality-resolved class events and ICU
  escape identity, branch-forwarded initial-string accumulator with
  the failure event, sound stopping condition) consuming ONE
  pre-lowering event vocabulary emitted by ONE pre-translation
  raw-pattern event scanner per route dialect; conservative
  opaque-event fallback unchanged; invariant 9 admission identity
  untouched (scanner and descriptor remain classification-only).
  Probe artifact rev9 pinned: source icu-rev9-probe.cpp sha256
  `2cdb790b407a2b4dc306836ff222af65a95ae40f2b9156f69968eae309c58320`,
  log icu-rev9-probe.log sha256
  `4255b5b25a1853919dac778619fcdb2707892af5cb98e56e3002de7a10679be9`
  (exit 0, 32/32 rows as expected). Harness rev9 (sha256
  `1b322b394fc9c71c40c7815105d706a5d6697107ecb2e14d4d64a02b6651f499`;
  24 test functions = 18 carried verbatim + 6 new blocks) is recorded
  red on the same merged base 6fb6531432 (repro-base-go-unit-rev9.log
  sha256 `1e0f2ac0c22bf3df002e65f353125b277278c9215e27b018d5bd29a5365e677f`,
  exit 1 by design; per-block attribution: the three new defect blocks
  fail with exactly the V2/V4/V6 full-pin and V2r/V6r lines (16), the
  E1/E2 full-pin and E1r/E2r lines (12) and the X2/X2r lines (6),
  while their silent-green controls (V1/V3/V5/V1r/V5r; X1/X1r) raise
  none, all three new guard blocks PASS (the V2/V4/V6-mu family, the
  V7-V10 equivalence rows, the E3/E3-mu controls with E1-mu/E2-mu,
  X1-mu/X2-mu), and every carried rev4/rev6/rev7/rev8 block reproduces
  the recorded attribution exactly - defect carriers red including the
  carried 50k admission Error Trace, guards and arithmetic controls
  green). Warm build evidence reused unchanged per the
  validation-evidence contract (warm-build-rev6.log, exit 0; inputs
  unchanged: same base, toolchain and make cgo target).
- Rev 10 (2026-10-25): address the ninth design review round; the single
  finding verified TRUE against primary sources AND live ICU 77.1
  measurement (icu-rev10-probe.log, 24 rows, exit 0) before acting.
  (1) Assertions and backreferences seal the pending literal run
      INDEPENDENTLY of their atStart and length effects (finding F1):
      verified TRUE - Rev 9's sealing rule named classes, flag sets,
      group boundaries, alternation, quantifier splits and pattern end
      but omitted anchors, boundary assertions and backreferences, and
      those are separate effects that neither the atStart-preserving
      nor the zero-min-length class can derive: ICU flushes the run
      via fixLiterals(false) at caret (icu-77.1-regexcmp.cpp:1132),
      dollar (1147), `\A` (1161), `\b` (1172), `\B` (1185), `\G`
      (1202), `\Z` (1261), `\z` (1266) and numbered/named
      backreferences (1326/1359; default split=false, regexcmp.h:103),
      while the accumulator keeps the same opcodes zero-width,
      atStart-preserving and min-length-0 (zero-width group
      2742-2757; backreferences possibly EMPTY, 2752-2753; max
      unbounded, 3548-3551). The falsifying pairs are recorded as
      rejected alternative 2k.
  Mechanism (kept from Rev 9 unless noted): ONE shared classification
  core over ONE pre-translation event vocabulary per route dialect -
  now governed by the ONE shared seal/atStart/length event table of
  invariant 8, which names every ICU literal-flush boundary with the
  three effects recorded INDEPENDENTLY (a zero-width, zero-length,
  atStart-preserving event can still seal: the assertions and
  backreferences; a sealing event can still consume: the ICU-identity
  `\v`; a seal can stand next to content joining the NEW run: the
  singleton class). The event vocabulary marks every anchor/boundary
  kind and every backreference run-SEALING; the sealing rule, the
  descriptor exempt/non-exempt lists (`^\n$\nx` / `^\n\B\nx` /
  `(?=())^\n\1x` / `(?=(?<e>))^\n\k<e>x` non-exempt as two-unit
  classifications; `(?=())^\nx` / `(?=(?<e>))^\nx` exempt), the
  engine mappings, plan items 1c/4c, validation items 5/7/8 and the
  scanner/metadata risks pin the same contract. Oracle tables: the
  assertion-seal and backreference-seal families (defects RED on base
  - A3/A3r and B2/B4, which the merged base wrongly rejects through
  its current conservative path; guards silent-green - A1/A2/A1r/A2r
  plus the six-row mu family and B1/B3 plus B1-mu..B4-mu, which the
  base already answers false and the seal-less core of rejected
  alternative 2k would flip true). Conservative opaque-event fallback
  unchanged; invariant 9 admission identity untouched (scanner and
  descriptor remain classification-only).
  Probe artifact rev10 pinned: source icu-rev10-probe.cpp sha256
  `0d7df5ce986f1f23464334b0be7f43acc56ffe900d9d423272f9c29ff918a8be`,
  log icu-rev10-probe.log sha256
  `11dc17a40e11713da76ceb13be28cdfb5878d85eca4024f08de2ddad30c63d5d`
  (exit 0, 24/24 rows as expected: A1/A2 find FALSE vs A1-la2/A2-la2
  [2,5), A3/A3r TRUE [2,5), mu family FALSE; B1/B3 find FALSE vs
  B1-la2/B3-la2 [2,4), B2/B4 TRUE [2,4), mu family FALSE). Harness
  rev10 (sha256
  `e0ad789f735d4c443f191d9fbf4e3206d44a4e8aa367ec6bbdd3971c6a304efb`;
  28 test functions = 24 carried verbatim + 4 new blocks) is recorded
  red on the same merged base 6fb6531432 (repro-base-go-unit-rev10.log
  sha256 `e71b702f8968483cbcc6d1680e68bef63b347349fa6ad37ec8f7687a2714df03`,
  exit 1 by design; per-block attribution: the two new defect blocks
  fail with exactly the A3 LIKE/INSTR/SUBSTR/REPLACE + A3r LIKE/INSTR
  lines (6) and the B2 LIKE/INSTR/SUBSTR/REPLACE + B4 LIKE/INSTR lines
  (6), while their silent-green controls (A1/A2 with full pins,
  A1r/A2r and the six-row mu family; B1/B3 and B1-mu..B4-mu) raise
  none and both new guard blocks PASS, and every carried rev4-rev9
  block reproduces the rev9 log's FAIL/PASS status exactly (block
  status parity verified, including the carried 50k admission Error
  Trace). Warm build evidence reused unchanged per the
  validation-evidence contract (warm-build-rev6.log, exit 0; inputs
  unchanged: same base, toolchain and make cgo target).
- Rev 11 (2026-10-26): reconcile the publication gap raised by the
  external review on PR #31125 (mo-bot-local design-gate audit): the
  design-stage review of Rev 10 had closed with decision PASS on
  2026-10-25, but the Status header still read "pending design review"
  and the PR carried no traceable approval record. Record the complete
  decision chain in the new Design review record section and update the
  Status header. Status-record-only revision: no other text changed and
  every Rev 10 (sha256
  `5dcab7b28d863dbec65e003b5db3503fdbc3ee1c1374d4622e9ae737bf3315b4`)
  statement is retained verbatim, so the Rev 10 PASS applies unchanged.
  This revision's sha256 is pinned in the PR body and the review reply,
  by the established hash-pinning convention.
