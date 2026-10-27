# Design-first decision, 2026-10-27

Scope: conditional exact stream Top-K within existing Draft PR #31407.
Trigger: planner/plugin/search ownership boundaries, optional plan JSON, cache
generation provenance, hot-path memory/cancellation behavior.

Reviewed design: [revision 1](fulltext2-conditional-stream-topk.md), SHA-256
`ae0c68d5e5c172d5f94ce8bfe03e2f7517fc7a991b6127449e36959185983cff`.
Source before implementation: `b366f221c3f69d038b66fe8857d4434e5f683354`,
combining existing PR head `42a5e13b249995009cc9cb2c4fddaea7706fd2ad`
and freshly fetched main `baa83f4b96e56ec214f92c68f7b56539b9e0fa45`.

Independent design reviewer: current-model agent fulltext_gap_code_audit.
Decision: PASS, no blocking design findings. Its source audit verified the
cache lock, readonly transaction/snapshot closure, source-bar contract, original
scorer/liveness, and byte-order VARCHAR comparator. Sensitive implementation
seams: derived cancellation context must reach the callback/collector; relation
lookup must use the original transaction without resetting shared handles;
load provenance must be invalidated before retry and published only after
successful unchanged readonly loading. Early-exit SourceCommitTS is acceptable
only when it proves C > B and rejects acceleration.

Root design decision: PASS for implementation under revision 1. The selected
stage preserves the existing exhaustive scorer; it does not enable WAND pruning
or remove the source join. The no-tail restriction deliberately limits first
rollout. Remaining work is implementation and empirical verification, not an
unresolved correctness decision. No production changes existed at approval.

Implementation owner: GPT-6.1-Sol, extra-high reasoning, with root responsible
for analysis, review, performance acceptance and delivery. Any material design
deviation must be reviewed before delivery. This design approval is not a claim
that the implementation passes tests or improves performance.

Risk map: planner eligibility/optional JSON R3; loaded-generation visibility
proof R3; query-local bounded collector and cancellation R3; existing row-key
and score-batch changes retain their previously mapped evidence. Final evidence
must cover every changed closure, including negative public SQL controls,
static/package checks, focused lifecycle/race tests and real same-data A/B.

## Implementation review before real-data execution

Root and the independent design reviewer inspected the full production closure.
The ordinary BM25 configuration does not carry a source table name; the reader
therefore proves source identity from the trusted scan specification and the
actual resolved relation. A first implementation's config-name check would have
prevented activation and was corrected before runtime testing. Source/spec
identity equality, source runtime-filter rejection and nil-process fallback are
now local guards. No correctness guard was relaxed to obtain activation.

The frozen source manifest passed normal owning-package tests/vet, public SQL
binding controls, concurrent-collector race repetition, owning-package race and
incremental lint. These do not establish actual runtime activation or speedup;
the next gate is bounded service-level correctness and full-corpus A/B.

Accepted cancellation scope: the 8192-visit bound applies to candidate traversal
and its segment boundaries. Existing tokenization and dirty-liveness document
frequency preprocessing remain unchanged on both the original stream and the
collector. Both call the same buildWandIters/globalStats.df/liveTermDF before
emission. The independent reviewer found no new repetition, lock, wait or
recursion in that stage. This PR does not claim an end-to-end cancellation time
bound for that pre-existing preprocessing; broadening it is separate work.

## Revision 2: executable default VARCHAR comparison identity

The d6ad5d2 real-service candidate preserved all 33 semantic controls and 63
small-query captures, but its index scan still emitted eight candidates for
LIMIT 3. This is not an activation or performance pass. The planner fixture
had explicitly used charset identity 0; ordinary live CREATE TABLE defaults to
CharsetUTF8 (identity 3), which the first whitelist excluded.

Before changing production code, root and the independent reviewer audited
identity 3 across Top/MergeTop (compare.NewOrder and strCompare), full sort,
merge/spill, scalar VARCHAR equality, hash join key encoding, runtime IN filters
and fulltext PK normalization. At revision 0 these paths retain byte order and
byte identity. CompareStringOrderValues returns bytes.Compare for non-UCA
identities; CHAR padding behavior does not apply to VARCHAR. The identity's
general-ci name and case-insensitive string helpers do not describe the
ORDER BY/PK equality implementations used here.

Design amendment: permit known identities 0..3 only, still requiring VARCHAR,
not-null PK, collation revision 0 and every original shape/visibility guard.
CHAR, UCA, unknown identities and future revisions remain excluded. This does
not change benchmark DDL, tokenization, scoring or SQL comparison behavior.
Independent decision: PASS. Root decision: PASS before implementation.

Required new evidence: planner binding carrying the actual default identity,
comparison tests with equal-score mixed-case/trailing-space/non-ASCII keys,
and fresh actual scan-output admission. Rebuild the candidate and repeat the
service-level controls before the full-corpus performance comparison. If
another guard prevents admission, investigate it without bypassing the proof.
