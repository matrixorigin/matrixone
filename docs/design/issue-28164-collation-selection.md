# #28164: independent collation implementation and key selection

Status: experimental selection, not a frozen production format. This supplements
[the replacement design](issue-28164-collation-key-reuse.md) and supersedes any
claim that the local `86c030d6b2` prototype froze production key bytes. No table
has been enabled and no production integration was expanded in this selection.

Machine-readable results, API option samples, sizes, benchmark records and raw
artifact hashes are in [selection evidence](issue-28164-selection-evidence.json).

## 1. Exact first-stage semantic scope

The recommended first stage covers the semantics already representable by MO's
current column metadata, using these canonical names:

| Canonical name | MySQL ID | Rules pinned for validation | Padding | Backend recommendation after comparison |
| --- | --- | --- | --- | --- |
| `binary` | 63 | Byte identity/order | NO PAD | Borrow existing bytes, without a transformation library call. |
| `utf8mb4_bin` | 46 | Unicode scalar order as observed in MySQL 8.0.45 | PAD SPACE | Scalar weights plus an explicit PAD adapter; C1 remains the leading compact candidate. |
| `utf8mb4_general_ci` | 45 | Legacy MySQL general-ci weights, **not UCA**; MySQL 8.0.45 reference | PAD SPACE | Reuse the existing MO/TiDB-derived mapping plus the validated adapter, subject to the remaining format gates. |

This is a scoped selection based on the results below, not a declaration that
general-ci is the final backend for every collation. The native comparison also
examined `utf8mb4_unicode_ci` (ID 224, legacy UCA 4.0/PAD SPACE),
`utf8mb4_0900_ai_ci` (ID 255, UCA 9.0/NO PAD), and `utf8mb4_0900_bin`
(ID 309, NO PAD). Vitess is a viable weight provider for those wider domains;
the current general-ci table cannot implement them. Adding them to phase one
would require distinct metadata identities and a wider SQL-consumer design.

Current `build_util.go` compatibility spellings are a separate contract:

- `utf8_bin`/`utf8mb3_bin` map to the text `_bin` class.
- `utf8_general_ci`/`utf8mb3_general_ci`, `utf8mb4_0900_ai_ci`,
  `latin1_swedish_ci`, and `ascii_general_ci` map to the general-ci class.
- MO stores UTF-8, so accepting a charset spelling does not establish MySQL's
  native utf8mb3/single-byte encoding and character-admission behavior.
- Other Unicode/0900 collation names in the inspected baseline are rejected.

Do not reinterpret persisted class 3 as native UCA 9.0 merely because a DDL dump
used the `0900_ai_ci` alias. The recommended new-format admission policy is to
require an explicit supported canonical semantic identity; misleading legacy
aliases must be resolved to that identity before rebuild/creation, or rejected.
Old tables retain their old format and metadata behavior. This admission policy
is a design decision to implement/review, not behavior already shipped by this PR.

## 2. Candidates and reproducible environment

- **Vitess:** Go module `vitess.io/vitess v0.22.1`, source commit
  `aafd40357555438f9df7b7e28afe7e0828502896` (the previously researched release).
  Its actual `Collate`, `WeightString`, `WeightStringLen` and `PadToMax` APIs were
  executed, not inferred from MO's comparator.
- **MO/TiDB-derived candidate:** the existing mapping lineage at TiDB commit
  `6cbbd222c786948379edc50ef8a8c37e485957c0`, extracted in local prototype
  `86c030d6b2`; existing MO comparison and the new PAD adapter were measured
  separately. No second production weight table is introduced by this design.
- **Oracle:** actual MySQL 8.0.45 aarch64, official image digest
  `sha256:4af1f8815716546f5b12410f7621f37f93db8dd11a184706ef59111930b8c2ff`, in a
  task-owned network-isolated container. All SQL comparisons name the collation.
- The pinned Vitess version failed to compile on MO's Go 1.26.4 because its
  `go/hack` Swiss-map runtime-layout assertion does not match that toolchain.
  The experiment used unmodified Vitess with Go 1.24.4. Both candidates in the
  comparative microbenchmark used that same Go 1.24.4 executable/toolchain.
  Directly adding this exact Vitess module to production is therefore not an
  accepted integration plan; portable subset extraction or another validated
  revision would need a separate dependency/toolchain assessment.

A full libc `strxfrm` port is not the default. Its process/locale contract is not
proof of the exact MySQL named semantics required here.

## 3. Native comparison results

The corpus contains 244 strings: controls including embedded NUL, trailing and
interior spaces, punctuation, case/accent pairs, composed/decomposed characters,
ligatures and multi-character equivalents, supplementary-plane characters,
long common prefixes and strings up to 1,000 characters. There are 59,536 ordered
pairs per collation, 357,216 total across the six named domains.

The table reports **equality errors / remaining order errors** versus SQL.
A mode with errors is a rejected invocation/adapter for SQL key identity; this
is not a claim that Vitess's caller-context-specific API is universally broken.

| Domain | Direct `Collate` or `WeightString(...,0)` | Trim input, then weight-0 | Fixed 1,002 input characters | Fixed-capacity `PadToMax` | Selected experimental PAD-stream / NO-PAD default |
| --- | ---: | ---: | ---: | ---: | ---: |
| general-ci | 16 / 182 | 0 / 368 | 0 / 0 | 0 / 0 | 0 / 0 |
| text `_bin` | 12 / 138 | 0 / 324 | 0 / 0 | 0 / 0 | 0 / 0 |
| unicode-ci | 404 / 320 | 252 / 340 | 408 / 0 | 0 / 0 | 0 / 0 |
| 0900-ai-ci | 0 / 0 | 168 / 24 | 0 / 0 | 0 / 0 | 0 / 0 |
| 0900-bin | 0 / 0 | 12 / 186 | 0 / 0 | 6 / 0 | 0 / 0 |
| binary | 0 / 0 | 12 / 186 | 0 / 0 | 6 / 0 | 0 / 0 |

The existing MO comparator reproduced the trimmed-input order errors: 368 for
general-ci and 324 for text `_bin`. It was a regression comparison, not the
compatibility oracle. MO/C1 and Vitess weights with the same scalar C1 adapter
both had zero mismatches for general-ci and text `_bin`. Vitess and MO mappings
also agreed with **65,536 native character-weight pairs**: all valid BMP scalars
and 2,048 supplementary samples.

Distinctive native counterexamples:

| Pair | general-ci | unicode-ci | 0900-ai-ci | binary |
| --- | --- | --- | --- | --- |
| empty vs NUL | greater | equal | equal | less |
| `Alpha` vs `Alpha ` | equal | equal | less | less |
| precomposed `é` vs `e` + combining acute | less | equal | equal | greater |
| `ß` vs `ss` | less | equal | equal | greater |

These rules cannot be collapsed into lowercasing or one universal general-ci
identity. They also explain why trimming source bytes is insufficient when a
collation can ignore characters or expand one character into several weights.

## 4. API length and padding decisions

1. `WeightString(...,0)` was independent of destination capacity when allowed
   to grow; it supplies unpadded weights. For the two native 0900 collations in
   this test it passed directly, without trimming. Binary can bypass the API.
2. Positive length truncates general-ci/text `_bin` when shorter than the input:
   `abc` and `abd` both become the key for `ab` at length 2. This produced 292
   equality errors for general-ci and 190 for text `_bin`. It must not be used
   to force a long lookup value into the column/index width.
3. Positive lengths were ignored by the tested 0900 implementations, as their
   NO PAD contract specifies. A single wrapper cannot assume identical length
   semantics across implementations.
4. Padding unicode-ci to a number of **source characters** is not canonical key
   identity: ignorable characters and expansions change the relationship between
   source length and weight length. The native empty/NUL equivalence is one
   counterexample to blindly using fixed character padding.
5. `PadToMax` binds output to destination capacity. Zero/insufficient capacity
   can produce empty output or grow the buffer and then pad to its incidental
   capacity, depending on implementation/input; it is not a universal truncation
   contract. In binary/0900-bin it merged empty with NUL, and `a` with `a`+NUL.
   Do not choose capacity from an individual row or mutable allocator size.
6. `WeightStringLen` also has implementation-specific preconditions: the UCA
   0900 implementation requires a multiple-of-four input-byte bound and panics
   otherwise. The experiment's corrected conservative bound used
   `max_source_characters * utf8.UTFMax`. This is a buffer-sizing bound, not a
   license to use that capacity as the logical identity of a value.

7. Source validation is a caller obligation. The tested general-ci
   `WeightString` stops at malformed UTF-8: `a` + byte `ff` + `b` produced the
   same weights as `a`, with no error return. Transformed text must therefore
   validate UTF-8 before calling the library. The C1 prototype rejects that
   input; raw binary continues to accept arbitrary bytes. This admission
   contract must not silently change old-table behavior.

Fixed-width PAD SPACE keys passed some modes but their cost is not acceptable
as an unmeasured default. The selected experiment avoids declared-width padding.

## 5. Format candidates and backend decision

Two experimental PAD encodings were compared:

- **C1, scalar compact:** the earlier prototype for the one-scalar-weight
  general-ci/text `_bin` domains. It places the implicit PAD end marker between
  low and high weights, encodes interior SPACE runs on the side of their next
  non-SPACE weight, and preserves the remaining scalar order. For ordinary
  ASCII, its payload is the trimmed length plus one byte and has no zeros.
- **T1, weight-stream adapter:** obtains the library's unpadded primary-weight
  sequence and exact SPACE weight. It canonicalizes trailing SPACE **weights**,
  not source bytes, and encodes the same low/SPACE/end/high ordering with fixed
  numeric weight framing. This handles the tested unicode-ci expansions and
  ignorables as well as general-ci/text `_bin`. It is not a generic promise for
  arbitrary multi-level collations or locale tailoring.

T1 plus unchanged NO-PAD default weights passed all 357,216 native pair cases.
C1 passed its supported scalar domains with either weight backend. First-stage
recommendation: keep C1 as the compact candidate and reuse MO's existing mapped
weights for general-ci, scalar weights for text `_bin`, and raw binary bytes.
This avoids importing an incompatible broad module without giving up any
verified semantics in the scoped first stage. Vitess remains the preferred
researched route if native UCA/0900 domains are added; that is a separate semantic
scope and metadata decision, not an alias change.

**Neither C1 nor T1 is frozen as a production on-disk version by this PR.**
Selection is conditional on closing the decoder/admission/consumer gates below.
In particular, the implementation choice for a narrow first stage does not
preselect general-ci for wider Unicode collations.

For `Alpha`, measured sizes (including actual tuple string framing/escaping):

| Representation | Payload bytes | Tuple field bytes |
| --- | ---: | ---: |
| Raw binary | 5 | 8 |
| C1 with either scalar backend | 6 | 9 |
| T1, general-ci | 16 | 24 |
| Vitess general-ci, fixed 1,002 characters | 2,004 | 3,009 |

Local Go 1.24.4 / Apple M1, preallocated-buffer microbenchmarks for a 16-byte
ASCII input (one run; not database throughput):

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Raw borrowed path | 3.017 | 0 | 0 |
| MO/general-ci + C1 | 104.9 | 0 | 0 |
| Vitess/general-ci + C1, prepared buffers | 154.8 | 0 | 0 |
| Vitess/general-ci + T1, prepared buffers | 182.5 | 0 | 0 |
| Vitess/unicode-ci + T1 | 303.4 | 80 | 1 |
| Vitess/0900-ai-ci default weights | 34.09 | 0 | 0 |

The larger 128-byte ASCII and 1,152-byte multibyte inputs, all modes and exact
outputs are retained in the evidence JSON. The unicode-ci iterator allocated
80 bytes even with prepared destination buffers in this pinned implementation.
Different domains perform different work; these numbers are not cross-domain
speedup claims.

## 6. Actual tuple and original-value boundary

An overlay-only experiment executed MO's actual `Packer`/`Unpack` without editing
production files. It verified 9,272 fields and 2,262,368 ordered pairs, including
numeric tuple parts before and after the key. All valid payloads round-tripped
as bytes and kept their candidate payload order through tuple escaping. This
proves framing; it does not rescue a candidate that already disagrees with SQL.
15,138 additional native comparisons verified prefix extraction **before**
weight transformation, using character units for text and byte units for binary.

**Error boundary FAIL:** existing generic `Unpack` accepted these three missing-
terminator sequences without error: `46 01`, `46 01 ff`, `46 01 00 ff`.
`decodeTupleTo` checks the type prefix but `decodeBytes`/`findTerminator` does not
require an actual terminator. The earlier C1-specific prototype decoder rejects
such inputs, but that does not establish safety for all existing tuple consumers.
This must be resolved in the later approved integration; it was not silently
patched as part of this selection-only task.

A native MySQL UNIQUE-index reference query returned `id=7, name=Alpha` for
`name='alpha '`, preserving original hex `416C706861`. MO's proposed transformed
storage has **not** passed that public SQL/backfill terminal. The design continues
to require original columns in the main row and fetching them when needed;
opaque decoded weights cannot become a covering-index projection of the text.

## 7. Compatibility and review decisions still required

Keep semantic identity/revision separate from physical codec revision:

- The column's supported semantic class and its pinned rules determine equality,
  ordering, padding and weights. A library upgrade is safe for existing keys only
  if it preserves that contract; a moving library version is not the contract.
- The relation/index's physical-format version determines raw legacy versus
  the chosen tuple payload encoding. C1 and T1 can represent the same semantics
  with different bytes; they cannot share one persisted format identifier.
- Neither descriptor belongs in every key. Reuse column metadata and verified
  durable relation/index metadata. Arbitrary table properties are dropped by
  the inspected disttae persistence path; `SchemaExtra.FeatureFlag` transport is
  a reuse candidate to review, not a speculative new registry.
- Changed semantic rules or key bytes require an explicit compatible rebuild or
  new-table path; never reinterpret old raw keys or silently promote a legacy
  `0900_ai_ci` alias to native UCA 9.0. Copy/restore, readers, writers and probes
  must preserve the same descriptors.

Before production format freeze and renewed integration, resolve: canonical-name
admission and alias migration policy; durable format/rules transport and old-node
admission; strict tuple malformed-input behavior; original-value retrieval;
complete writer/probe/lock/backfill closure; SQL comparator/hash/group agreement;
and actual persisted-object ZM/Bloom plus failure/restart validation. A passing
microbenchmark or native weight test does not close those gates.

The experiment followed `mo-dev`; the document/PR update follows
`matrixone-pr-workflow`. Existing unrelated worktrees and PR states were preserved.

Primary references:

- [Pinned Vitess API and padding contract](https://github.com/vitessio/vitess/blob/aafd40357555438f9df7b7e28afe7e0828502896/go/mysql/collations/colldata/collation.go)
- [Pinned Vitess scalar implementations](https://github.com/vitessio/vitess/blob/aafd40357555438f9df7b7e28afe7e0828502896/go/mysql/collations/colldata/unicode.go)
- [Pinned Vitess UCA implementations](https://github.com/vitessio/vitess/blob/aafd40357555438f9df7b7e28afe7e0828502896/go/mysql/collations/colldata/uca.go)
- [MySQL binary versus text _bin and padding](https://dev.mysql.com/doc/refman/8.0/en/charset-binary-collations.html)

- [MySQL UCA version definitions](https://dev.mysql.com/doc/refman/8.0/en/charset-unicode-sets.html)
