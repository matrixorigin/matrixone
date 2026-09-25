# Bounded MySQL XML functions — issue #28306

Revision 4, issue #29329 overlapping-context correctness follow-up (2026-09-25).
Revision 3, issue #29329 numeric compatibility follow-up (2026-09-24).
Independent revision 3 design review: GPT-6 Astra, medium, PASS after the
fragment-validator boundary was included. Implementation PR: #29362.
Revision 2 parent-approved scope and limits (2026-09-21). Independent design:
gpt-6-astra, medium, session `01a0c0d3-1fd5-7c23-baab-6ed3431f7052`.
Revision 2 base: `b2f178defd7c`.

## Contract and scope

Add ExtractValue(xml, xpath) and UpdateXML(xml, xpath, replacement). This is a
documented **bounded compatibility subset**, not a complete XPath 1.0 engine.
Every accepted expression must have defined semantics; unsupported expressions
fail explicitly, never masquerading as an empty match. Unaffected XML bytes
must survive replacement exactly.

Support absolute/relative paths, `/`, `//`, lexical qualified names, wildcard,
attributes, self/parent, sequential predicates (positive positions, last(),
position()=integer, attribute existence/equality, direct-child literal equality),
node-set union with document-order deduplication, and top-level count(path).
Issue #29329 extends the same bounded evaluator with top-level sum(path),
numeric comparisons of count/sum/integers in `[-9223372036854775807, 9223372036854775807]`, and positional comparisons such as
`[position()=last()]`. `sum()` converts each selected element's
direct text records or selected attribute's value to numbers; an empty match
returns `0`. It does not add arbitrary arithmetic, XPath node-set comparisons,
or general scalar expressions. UpdateXML returns NULL for these scalar targets.
XPath decimal and out-of-range integer literals are explicitly unsupported;
XML text consumed by `sum()` has its own numeric conversion. Integer-literal
comparisons preserve precision rather than rounding through binary floats.
Numeric conversion for `sum()` accepts MySQL's ASCII leading whitespace,
including vertical tab and form feed, and then a numeric prefix. It stays
separate from XPath grammar whitespace. Shortest-round-trip rendering uses
fixed notation for magnitudes from `1e-15` to below `1e15`, and also when a
large value's significant digits extend beyond its decimal point; otherwise
it uses scientific notation without a `+` or leading exponent zeroes. This
keeps large fractional values such as `1000000000000000.1` distinct from
exact large values such as `1e15`. An out-of-range numeric text prefix
contributes `0` (as observed for `1e309` in MySQL); non-finite accumulated
sum results remain explicit errors under this bounded subset.
The fragment validator admits vertical tab and form feed in element text and
attribute values so the numeric converter can observe them. Other disallowed
control bytes, including NUL, keep their existing malformed-XML result; this
does not redefine the broader XML input contract.
Terminal text() is supported for extraction as a real terminal child-text
selector: `/P/text()` selects P's direct text records, while `/P//text()`
selects direct text records of P and of element descendants; `//text()` starts
at the synthetic document node. The existing text-record model is preserved,
so emitted nonempty records are space-joined without recursively forming an
XPath string value. Text-bearing `count()` paths remain explicitly unsupported.
UpdateXML maps a terminal `text()` target to its current XPath context and
replaces it only when that context is unique. It also supports element,
attribute and document-root replacement. Other axes, variables, arbitrary
arithmetic and scalar functions are explicitly unsupported. Invalid or
unsupported XPath is an error, not NULL.

Parent obtained the reference using an isolated MySQL 8.0.45 container with no
network or host port, then removed exactly that container. Oracle observations:

- Ordinary element extraction joins all direct text segments by spaces and does
  not recursively include descendant text. Mixed `x<b>y</b>z` extracts `x z`;
  text/CDATA/comment-separated `x,y,z` gives `x y z`. Explicit `//text()`
  selectors include direct text records from element descendants. Empty matches
  contribute no separator. Whitespace is preserved.
- Empty XML gives empty extraction; multiple roots and plain document text work.
  Entities (including unknown and numeric references) stay lexical, not decoded.
  CDATA delimiters are stripped. Prefixes match lexically, declared or not.
- Attribute/child literal predicates compare case-insensitively (Unicode simple
  case folding is this subset's declared comparison; no full SQL collation claim).
  Initial positions apply per parent: /a/b[1] across two a elements returns
  both first b's. `last()` sees the whole step's candidate stream, and each
  following predicate renumbers survivors in encounter order.
- Qualified XPath names are lexical only: an optional nonempty prefix and local
  name separated by one colon. Namespace declarations are not resolved; malformed
  QNames such as `/:a`, `/a:`, and `/a:b:c` are rejected as invalid XPath.
- Malformed XML, including DTD, gives NULL and warning1525. No entity expansion,
  external resolution, network, filesystem access or charset loader is allowed.
- Validate non-NULL XPath before document/replacement NULL tests. Bad XPath wins
  over NULL XML/replacement and bad XML. NULL XPath returns NULL without parsing
  XML. Empty XPath is an error. Masked rows do no validation and emit no warnings.
- A single update target is replaced using raw spans; replacement need not be
  valid XML. No/multiple matches return original bytes. `/` returns replacement.
  Attribute replacement includes its name/equals/quotes (`<a k="7">` -> `<a z>`).
  A scalar count/sum/comparison target yields NULL. Other scalar targets remain unsupported.
- Nonconstant column XPath is rejected, including one-row columns. Prepared
  parameters work and must be recompiled from each execution's current value.

Reference: https://dev.mysql.com/doc/refman/8.0/en/xml-functions.html . The issue
and parent oracle, rather than a generic XPath library, are the semantic oracle.
Reference session reported latin1 result charset; MO uses its existing string
result domain/connection encoding, not a new hardcoded latin1 codec.

## Architecture and alternatives

Private bounded iterative XML fragment tokenizer + XPath evaluator in function
package, synthetic document node, source byte spans and parent/child links.
Never reserialize XML. The tokenizer preserves raw entity text and qualified
names; a strict generic XML decoder would normalize/reject valid oracle inputs.
Avoid porting reference source (license/engine coupling). A general XPath
dependency adds adapter and cancellation/budget obligations; reconsider it if
scope expands, rather than silently growing this subset. Sample-specific string
splitting is rejected because it cannot preserve structure and positional scope.

Append function IDs581/582 without renumbering. Register arities2/3 and string
operands, nullable LONGTEXT results (MO T_text, MaxLongTextLen width): the
16 MiB output budget exceeds both VARCHAR and MEDIUMTEXT capacity. This uses
valid existing metadata for CTAS and clients; the runtime limit stays 16 MiB.
Revision 2 corrects revision 1's oversized VARCHAR declaration. volatile=true avoids
plan-time warnings and stale prepared values; null-synthesizing classification
preserves nullable metadata. Common binder checks execution-constant XPath
(literal, parameter, pure constant expression), not just vector.IsConst.
Compile XPath once per invoked batch; no cache surviving invocations. Result
wrapper owns output; inputs/program/row scratch never escape invocation. No
goroutines, shared state, IO, retries, new warning transport or native dependency.
Use current-attempt WarningAccumulator; bounded retained diagnostics with exact
warning counts. Append/allocation/cancel/limit errors propagate, not XML warnings.

## Accepted MO limits

XML/replacement8MiB each, output16MiB; XPath16KiB, AST1024 records, path128 steps;
XML depth256 and total node/attribute/text records65536; evaluation1,000,000
charged operations per row; conservative scratch admission32MiB, released per
row (program bounded for batch). Bound inputs before allocation, charge traversal,
predicate/string work and candidate copies, check cancellation during scanning
and evaluation. Arithmetic must not overflow. These are MO limits, not MySQL's.

## Deployment and risks

Additive function IDs: all executing CNs must be upgraded before use. Older CNs
reject unknown IDs; no existing opcode reinterpretation or new wire field.
Downgrade requires removing dependent views/prepared plans and repreparing.
Warning delivery reuses current transport. No storage/catalog migration.
R2 public result/error/metadata closure; bounded untrusted parsing requires abuse
and cancellation tests. No shared-state/race or distributed-protocol change.
Cost is bounded by input, record and work admission; no throughput claim.

## Revision 3 design decision and evidence

MySQL 8.0.44 in an isolated, network-disabled container returned
`1000000000000000.1` for `sum(/a)` on that element text, while the current
formatter returns `1.0000000000000001e15`. For `1e15`, both use `1e15`.
MySQL returned `2` for `sum(/a)` when the element text starts with vertical
tab or form feed followed by `2`; the current fragment validator rejects both
bytes before numeric conversion. MySQL also returned `4` for
`<a><b>1<c>2</c>3</b></a>` with `sum(/a/b)`, and `1` for text `1x`; these
confirm the existing direct-text and numeric-prefix decisions.

Keep one scalar evaluator and its existing per-row work budget. Admit vertical
tab and form feed through fragment validation, repair numeric text whitespace
admission and number rendering; leave XPath token whitespace and
position/predicate parsing unchanged. Add focused numeric
boundary cases to the existing oracle table and public function test, plus
one small SQL BVT result. Existing process, storage and wire contracts do
not change. The fallback is reverting the targeted fix commit.

## Revision 4: overlapping descendant contexts

Independent MySQL 8.0.44 checks for issue #29329 exposed two failures in the
same owner, the intermediate XPath step result. For
`<a><a><b>1</b></a></a>`, `count(//a//b)` and `sum(//a//b)` both return `2`:
the inner `b` occurs once under each `a` context. The explicit union
`sum(//a//b|//a//b)` returns `1`. For
`<r><a><a><b>1</b></a></a><a><b>2</b></a></r>`, the `//a` context encounter
order is outer, sibling, inner; `//a/*[1][position()=last()]` selects `b=1`.
The current evaluator rebuilds each step in node-ID order and discards repeated
occurrences before the next step, selecting `b=2` and undercounting aggregates.

Keep an ordered, duplicate-preserving occurrence stream within each individual
path. A step enumerates the current context stream in order; `//` expands each
context through descendant-or-self in its existing bounded tree walk. Child,
attribute, self and text axes append matching occurrences in encounter order.
Parent-axis `..` retains its existing document-order unique-node behavior for
that step. Positional predicates retain per-parent initial positions, whole-step
`last()` cardinality, and sequential renumbering of surviving occurrences.
Only an explicit union deduplicates the numeric result stream. Ordinary
`ExtractValue` continues to emit each text/attribute node once in document
order; `UpdateXML` uses the raw occurrence count and replaces only a unique
match, preserving raw byte spans. No public syntax, warnings or metadata change.

Each appended occurrence consumes the existing per-row work and scratch budget.
The 1,000,000-operation and 32 MiB scratch limits cap overlapping expansion;
no retained state, concurrency, IO or unbounded queue is introduced. Keep the
single-context, non-descendant predicate path in place and avoid a full-node
scan after every step. Add focused nested-plus-sibling, overlapping aggregate,
explicit-union, parent-axis and UpdateXML oracles to the current UT/BVT cases;
compare supported expressions against the independent MySQL result set and
measure the common 1,000-sibling path before/after. This is a semantic repair of
the existing bounded subset. Reverting this follow-up restores revision 3.

## Validation / acceptance

Baseline resolver rejection executed,1.404s, both names selected (task recordE4).
Implement oracle-backed pure XML/XPath and vector tests: fragments, text,
entities, cardinality, byte-preserving update, NULL/error precedence, positional
scope, union dedup, unsupported syntax, limits/cancellation, masks, constant and
varying XML, repeated invocation, warning counts. Registry/ID/type/nullability
and binder constant/column/prepared cases reuse existing fixtures. Focused tests,
owning function/planner packages, incremental gofmt/vet/lint required.
Public SQL BVT must use only parent-authorized isolated instance with actual
endpoint verification: warnings, prepared reuse, table input, CASE masking,
metadata and cleanup+same-instance repeat. No access to existing services.
Independent GPT-6 medium final review must reconcile full diff and evidence
before local commit; parent owns push/PR, no CI wait.
