# JSON SQL order and persisted physical order

Version: 1 (proposed, independent design approval pending)

Owner: issue [#28039](https://github.com/matrixorigin/matrixone/issues/28039).
Implementation: [PR #28086](https://github.com/matrixorigin/matrixone/pull/28086).
Implementation snapshot inspected: `715dc8dfa531c0558355fe83bfa79da0ac9a798a`.
Pre-change reference: `6e5f82f568b1ec3e013e8d4ab9031b2360300b84`.

This is a distinct design-review phase. It does not approve the existing
implementation or authorize release. A reviewer must record the exact document
commit and an independent decision before implementation approval resumes.
The admission correction in section 4 is proposed work, not a description of
validation already performed by the inspected implementation.

## 1. Problem and scope

JSON comparisons put booleans in the wrong SQL precedence group. Correcting the
shared comparator also affects SQL ordering, peer partitioning, canonical hash
keys, and persisted cluster-key writers. This is a major contract separation:
the complete PR changes more than 500 production lines and crosses expression,
sorting, hashing, pruning, and storage boundaries. The design gate applies even
though the initiating issue is a bug.

The pre-change engine has **two** physical relations. Writers compare decoded
JSON using the old semantic relation. TAE `MergeAObj` and `mergeObjs` compare
varlena bytes using `sort.GenericLess[string]`. Those relations already disagree
for `[0,0]` and `[100]`. A legacy merger can produce the multi-row run
`[100], [0,0]`; changing future mergers to the writer relation violates the
new heap comparator's sorted-input precondition. Preserving the writer relation
alone is therefore insufficient for compatibility.

There is a second independent constraint: validating every descendant on each
SQL comparison makes a cross-type comparison proportional to document size.
Partial validation is not a solution: a malformed later child then changes its
document's comparison domain depending on the other operand.

Goals:

- Correct SQL precedence and preserve exact numeric equality and hash equality.
- Give arbitrary ByteJSON values a deterministic public comparison fallback.
- Keep direct valid comparisons independent of unrelated descendants after
  admission, with no per-expression full-column cache.
- Preserve each existing storage consumer's relation and encoded bytes.
- Prevent JSON byte-range statistics from excluding matching rows.

Non-goals: new `MIN/MAX(JSON)` support, JSON/SQL boolean coercion changes,
prepared-parameter coercion changes, encoding or wire-format changes, physical
run normalization, a migration, or automatic issue closure. `MIN/MAX(JSON)`
remains rejected; `MIN/MAX(NULL)` keeps its pre-existing result type.

## 2. SQL order and equality

The compatibility target is MySQL 8.0.45 for supported scalar/direct
comparisons. The [MySQL 8.0 comparison documentation](https://dev.mysql.com/doc/refman/8.0/en/json.html#json-comparison)
defines type precedence, exact comparison of JSON numbers, and SQL NULL
placement. Its nonscalar ORDER BY behavior is not a general array/object sort
oracle. MatrixOne's existing recursive array and object rules remain an explicit
MatrixOne behavior. `GROUP_CONCAT(... ORDER BY JSON)` is not an oracle.

Ascending representative classes are JSON null, number, string, object, array,
false, true, DATE, TIME, DATETIME, BIT, BLOB. Numeric encodings share one
semantic group. Strings compare decoded bytes. Arrays compare the first
different element, then length after an equal prefix. Objects retain the
existing count/key/value relation. SQL NULL is separate from JSON null;
null propagation, null-safe equality, and explicit NULLS FIRST/LAST remain owned
by SQL operators.

For all public inputs, comparison must be reflexive, antisymmetric and
transitive. For any pair that compares equal, canonical hash keys must match.
Numeric equivalence includes signed/unsigned/float/decimal representations;
the existing exact numeric normalizer must remain shared with hash keys.
Hash keys are equality keys, not an oracle for SQL ordering.

Valid documents precede invalid documents. Two invalid documents compare by
raw type then raw data, and canonical encoding uses a disjoint invalid marker
plus those exact bytes. Classification covers the entire document before a
semantic comparison may return on an early child. NaN/infinities, short fixed
numbers, invalid decimals, oversized literals, invalid child tags and
non-shortest length prefixes cannot enter the valid numeric/string domain.

BIT/BLOB validation and equality retain the existing raw and legacy persisted
representations, including historical base64 decoding behavior. Malformed
legacy binary tags that the existing binary resolver deliberately interprets as
raw BLOB retain that interpretation in both rank and hashing; the word
"malformed" in that resolver is not a second, pair-dependent document domain.
No `TpCode`, literal constant, or serialized value is reassigned.

## 3. Consumer contracts

| Consumer | Relation and validation owner |
| --- | --- |
| Public `CompareByteJson` | Full document validation, then SQL or raw fallback domain |
| `CompareByteJsonTrusted` | SQL relation, only after complete admission of immutable values |
| Direct SQL comparisons | Trusted path for admitted T_json values; selection/SQL NULL checks happen before decode |
| SQL ORDER BY | One complete classification per selected row in the ordering operation; trusted comparisons for valid rows |
| Partition/window peers | Same SQL equality as ordering, including numerically equal encodings |
| Canonical key size/append | Shared document validity and equality; invalid values cannot panic or alias valid keys |
| Physical writer sorting | Existing `CompareByteJsonPhysical` writer relation |
| TAE `MergeAObj` / `mergeObjs` | Existing raw `sort.GenericLess[string]` relation |
| Zone-map object/block/seek, membership and Bloom comparability | JSON value comparisons fail open; SQL null-count checks remain usable |

Do not infer global SQL order from physical JSON cluster-key order. In
particular, physical run metadata cannot bypass SQL ordering or enable JSON
value-range pruning. Existing writer/merger disagreement is preserved as an
existing limitation, not claimed to be fixed by this design.

## 4. Admission and ownership: required correction

The inspected head's claim that T_json is automatically validated by the type
layer is not established. `types.DecodeJson` delegates to `ByteJson.Unmarshal`,
which has a TODO for validation. `vector.NewConstBytes` and `AppendBytes` can
carry arbitrary JSON bytes. Vector framing and varlena offset checks do not
prove the validity of descendants. A type OID is not a validation certificate.

The proposed correction is to establish a payload invariant at actual JSON
admission boundaries, without adding a row-sized comparison cache or relying on
a boolean flag that becomes stale when bytes mutate:

1. Text parsers and typed JSON constructors own production of valid documents.
   Their tests must prove conformance to the same structural/numeric validity
   predicate used by public comparison and hashing.
2. Raw byte admission to a T_json vector validates a non-NULL document before
   publishing its row or count. Cover constant, single/bulk append, replacement,
   and writer-callback paths. Invalid input returns an existing invalid-input
   error; failed admission leaves visible rows and metadata unchanged.
3. Checked vector decode from disk/wire validates each non-NULL JSON payload
   after framing and varlena bounds validation, before binding/publishing the
   vector. Cover binary, copied, reader, selected-row and legacy decode paths.
   Do not treat checks for a different varlena type as JSON validation evidence.
4. Vector-to-vector union/copy may preserve the invariant without reparsing
   only when its source already satisfies it. Trusted decode and low-level
   builders need an explicit immutable validated source or a typed constructor
   proof. Enumerate those callers; a function name containing "Trusted" does
   not prove the precondition.
5. Borrowed ByteJson data and vector payload aliases must remain immutable
   while used by comparison, sorting or hashing. Code writing through an alias
   must re-establish validity before publishing the changed value. No new
   synchronization, long-lived state or cache ownership is introduced.

The initial implementation audit must include `NewConstBytes`, `SetBytesAt`,
`SetConstByteJsonEncoded`, `AppendBytes`, `AppendMultiBytes`, `AppendBytesList`,
`AppendBytesWithWriter`, their low-level varlena builders, vector unmarshal
variants, and source-to-destination union/copy paths. Validation must happen
once at the outer admission owner, not redundantly in each nested helper.
If any reachable unchecked route remains, the direct SQL trusted call is not
approved. Error-returning boundaries must reject corrupt retained/wire input;
the public comparator and canonical hash APIs still support raw fallback for
diagnostic/internal callers that deliberately supply arbitrary ByteJSON.

This tightens handling of corrupted input without changing valid encodings.
It is additional production work and requires approval of this design first.
It must not be silently declared implemented by a PR-body edit.

## 5. Alternatives and decisions

| Alternative | Correctness, compatibility and cost |
| --- | --- |
| Leave all callers on old comparator | Smallest change, but leaves the reported SQL result error |
| Switch SQL and all physical consumers to one new relation | Simple API, but incompatible with both retained writer and merger runs without versioned normalization/migration |
| Fully validate on every comparison | Correct public fallback; O(document size) per row/comparison even for different types, rejected for the SQL hot path |
| Validate only visited children | Fast on some valid inputs, but comparison cycles and compare/hash disagreement; rejected |
| Cache complete decoded columns per expression | Repeated allocation and lifetime/alias invalidation costs; explicitly rejected by CR |
| Preserve physical consumers and validate at admission | Proposed choice: bounded one-time input work, stateless hot comparator, explicit owner proofs required |

Accepted tradeoffs proposed for independent review: JSON value predicates may
scan more blocks; decoding raw JSON inputs incurs linear admission work; valid
documents do not acquire a new serialized version; pre-existing physical-order
divergence remains outside this SQL correction. Rejecting corrupt encoded input
is preferable to executing a comparator with violated preconditions.

## 6. Cost and failure behavior

Let B be total admitted JSON bytes, N the selected row count, D nesting depth,
and P the compared prefix. Admission is O(B) with no payload copy beyond the
destination's existing ownership requirement. Validation adds stack work
proportional to D. Existing nesting limits must be applied at untrusted
admission so adversarial encodings cannot exhaust the stack; the permitted
limit must remain compatible with existing supported JSON producers.

After admission, cross-rank literal/array comparisons are O(1) per selected row;
same-rank comparison is proportional to P. Exact decimal and legacy binary
decoding can still perform payload-dependent work. Preserve zero additional
allocations for large legacy BLOB comparison; do not generalize that claim to
all numeric representations. Ordering owns O(N) temporary classification and
permutation storage and releases it at the end of the operation. Direct
comparison owns no per-column decoded-value cache.

Validation failure precedes publication; append/replace failure preserves
pre-existing rows and metadata, and existing vector/mpool cleanup releases
temporary allocations. No goroutine, channel, RPC, retry loop, background
worker, file or cache is introduced. Cancellation/resource behavior remains
with the existing operator or admission caller. Tenant and authorization
boundaries are unchanged. Raw error messages must not include payload contents.

Measure both admission and steady-state expression costs. Moving validation
outside a benchmark timer is not evidence that its cost disappeared.

## 7. Mixed versions and release

The new code reads/writes the same bytes. Writers retain the old writer relation;
mergers retain the old raw relation on either side of an upgrade. No physical
order marker is inferred or rewritten. Backup/restore and restart do not need
a format conversion. Rolling back does not require rewriting values.

This does **not** promise corrected SQL results during arbitrary mixed-version
execution: older CNs retain old comparison/pruning semantics, and distributed
sorting can disagree when stages use different relations. Roll out to a
homogeneous set of query-executing CNs before accepting correctness results;
drain queries spanning the transition using the existing deployment procedure.
Do not claim that a physical compatibility UT proves mixed-version SQL
correctness. Rollback also restores the old SQL bug. No feature flag or new
deployment mechanism is introduced here.

Release remains blocked on independent design approval, implementation
conformance, required checks and QA. Keep issue #28039 open for versioned tester
evidence. Inspect ordinary query errors and compare correctness on the scalar
matrix after rollout; monitor latency/block reads with existing query/scan
instrumentation. A separate, approved migration design would be necessary to
unify physical relations in future.

## 8. Acceptance and current gaps

| Invariant | Required evidence |
| --- | --- |
| SQL precedence and peer equality | 144 ordered pairs, numeric encodings, false/true, recursive arrays/objects; direct operators and SQL ORDER BY/partition/window |
| Global invalid domain | Invalid later child versus same/cross rank; NaN payloads; short scalar/container; all order laws and equal-value/equal-key |
| Canonical key robustness | Size/append agreement and no panic for malformed scalar/nested encodings, nonminimal prefixes and legacy binary values |
| Trusted admission | Invalid second child and short numbers through checked constant/append/replace and all decode variants; reject before publication, or prove the public fallback path; valid inputs still reach the trusted path |
| Legacy merge compatibility | Actual multi-row base-generated raw run, then both current merger entrypoints; assert raw retained order; writer relation tested separately |
| Pruning correctness | Object/block/seek/membership fail open, including prefix blocks; null-count pruning retained |
| CPU/allocation bound | Same harness at base/final, both array/boolean directions, same-rank early exit, column/constant, column/column, SQL NULL; report admission separately |
| SQL integration | Normal mo-tester comparison for the canonical BVT, exact head and clean instance; scalar MySQL oracle, MatrixOne-specific nonscalar expectations |

The 715dc8d repair restores both merger dispatches and complete public
classification and adds focused regressions. Its direct-comparison trusted
admission claim remains an implementation gap. A source-matched CGo probe at
715dc8d confirms it: corrupt the second child of `[0,0]`, construct and serialize
a T_json vector, then call checked `UnmarshalBinary` and `lessThanFn` against
`[1,0]`. Admission succeeds and SQL returns true although public comparison
orders the invalid document after the valid one. Ten existing named CR
regressions passed in the same run; that does not refute this boundary failure.
The fixture deliberately supplies corrupt internal/wire bytes; it is not a
claim that public SQL constructs this encoding. The retained-run regression
currently exercises MergeAObj; source dispatch inspection of mergeObjs is
supporting evidence, not a substitute for the requested terminal test.

Before approval, independently review sections 2-7 and explicitly accept or
reject admission-time corruption errors, the no-migration physical policy and
the homogeneous-query rollout requirement. After approval, implement the
admission closure and remaining terminal probes, rerun affected validation and
regenerate the exact-content semantic preflight. An old preflight PASS or green
CI cannot close an uncovered input boundary.

## 9. Design review record

```text
Change scope: complete PR #28086, SQL/physical JSON comparison separation
Trigger: >500 production lines; expression/hash/order/storage boundaries;
         persistent compatibility and hot-path CPU cost
Design: json_sql_physical_order.md, version 1, proposed
Reviewed revision: pending independent review of the document commit
Blocking findings: independent approval; implementation admission invariant;
                   second physical merger terminal regression
Decision log: alternatives and proposed tradeoffs in sections 5-7
Decision: REQUEST_CHANGES (not an independent approval)
Implementation deviations: section 4 is not implemented at 715dc8d
```
