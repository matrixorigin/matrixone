# Bounded MySQL XML functions — issue #28306

Revision 2, parent-approved scope and limits (2026-09-21). Independent design:
gpt-6-astra, medium, session `01a0c0d3-1fd5-7c23-baab-6ed3431f7052`.
Implementation PR: pending parent publication. Base: `b2f178defd7c`.

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
`[position()=last()]` (per parent). `sum()` converts each selected element's
direct text records or selected attribute's value to numbers; an empty match
returns `0`. It does not add arbitrary arithmetic, XPath node-set comparisons,
or general scalar expressions. UpdateXML returns NULL for these scalar targets.
XPath decimal and out-of-range integer literals are explicitly unsupported;
XML text consumed by `sum()` has its own numeric conversion. Integer-literal
comparisons preserve precision rather than rounding through binary floats.
Terminal text() is supported for extraction as a real terminal child-text
selector: `/P/text()` selects P's direct text records, while `/P//text()`
selects direct text records of P and of element descendants; `//text()` starts
at the synthetic document node. The existing text-record model is preserved,
so emitted nonempty records are space-joined without recursively forming an
XPath string value. Text-bearing `count()` paths and all UpdateXML text()
targets remain explicitly unsupported. UpdateXML supports element, attribute
and document-root replacement. Other axes, variables, arbitrary arithmetic and
scalar functions are explicitly unsupported. Invalid/unsupported XPath is an
error, not NULL.

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
  Positions apply per parent: /a/b[1] across two a elements returns both first b's.
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
