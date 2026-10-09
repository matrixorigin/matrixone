# UCA400 comparison-key foundation for #29206

## Scope

This document is the implementation contract for native
`utf8mb4_unicode_ci` / `utf8_unicode_ci` support in #29206. The native names
are admitted only as their versioned UCA400 identities (`RevisionV1` with the
legacy physical key format); they are never silently collapsed to
`general_ci`. Existing legacy identities and frozen comparison keys retain
their meaning.

The SQL admission boundary is deliberately narrow. Planner and execution
owners validate the versioned metadata before a plan is published, while
unknown revisions, key formats, and unsupported transport owners fail closed.
The integration is complete only when every consumer below observes the same
resolved comparison domain:

* scalar comparison, `IN`/`NOT IN`, grouping/hash/join, `ORDER BY`, and window
  peer/partition evaluation use the UCA key relation;
* generic `SERIAL` remains lossless and physical index serialization opts into
  the tagged UCA key explicitly;
* raw primary-key, index, and zone-map probes fail open to a residual/base
  scan when their persisted bytes cannot prove the UCA relation;
* malformed or out-of-repertoire values use a tagged raw fallback, so they
  cannot collide with valid UCA keys;
* the distributed charset/collation regression covers equality, membership,
  grouping, full and limited ordering, and window peers/partitions.

The two appended library domains share MySQL's partial UCA 4.0.0 primary
weights. The mb3 domain rejects supplementary characters before touching
scratch. The mb4 domain uses MySQL's legacy supplementary fallback, not
modern UCA ordering. Invalid UTF-8 is rejected by both domains. An opaque
key cannot recover original text or prove its source character repertoire.

## Backend and U4P1 framing

Reuse the already pinned Vitess v0.24.0 ID224 table (no contractions), not
`general_ci` or UCA 9.0. MySQL 8.4.11 differential testing found one table
discrepancy among every valid BMP scalar and 2,048 supplementary samples:
U+FFFF is ignorable in Vitess but has implicit weights FBC1 FFFF in MySQL.
The local adapter corrects only that scalar, preserving segment boundaries.
It does not modify the dependency or any existing domain.

Raw weight strings do not encode PAD SPACE order. U4P1 encodes each nonzero
uint16 primary weight relative to SPACE (0209):

| Weight/token | Bytes |
| --- | --- |
| Weight below SPACE | 10, high byte, low byte |
| Weight above SPACE | 30, high byte, low byte |
| Each pending SPACE before a lower weight | 19 |
| Each pending SPACE before a higher weight | 29 |
| End of string | 20 |

Trailing SPACE weights are implicit and omitted. Ignorable characters have
no weights; characters with SPACE-equivalent weights share padding behavior.
The end marker lies between lower/higher weights, so byte comparison models
implicit SPACE padding even for control characters and interior space runs.
Equal strings produce equal key bytes for future comparison/hash consumers.
`ValidateKey` checks framing, side-of-SPACE tags and marker runs, not whether
an arbitrary weight has a Unicode preimage.

The checked output-size upper bound is `54 * len(input) + 1`:
conservatively 18 elements/scalar times 3 encoded
bytes, counting each input byte as a scalar. Tests exhaust all Unicode scalar
values to verify the pinned table bound. A temporary raw-weight buffer uses
at most 36 bytes per input byte. Scratch does not overlap input; callers must
copy retained output before reuse. No resource lifetime extends beyond a call.

## Evidence and validation

`testdata/mysql_8_4_11_uca400.json.gz` was queried from MySQL Community Server
8.4.11 on macOS arm64 using a task-isolated Unix socket with networking off.
The generator checks server version/architecture and runs read-only queries.
The fixture includes exact raw weights, all 98 x 98 sample comparisons,
all valid BMP scalar weights and 2,048 supplementary scalar weights. It also
checks MySQL mb3 weights and comparisons for every BMP sample independently.
The Go tests do not require a running MySQL server. Fixture answers are not
derived from the implementation being tested.

Tests cover expansions, case/accent equivalence, combining/ignorable and
supplementary characters, SPACE-equivalent characters, NUL/control values,
prefixes, repeated SPACE, U+FFFF boundaries, scratch reuse, malformed UTF-8,
invalid key framing, size overflow, and mb3 repertoire. A separate U4P1
sample digest freezes bytes; existing V1 golden tests must remain unchanged.

## Versioned supported subset

This section is the versioned SQL integration contract for the first release of
the backend.  It is intentionally narrower than MySQL's complete collation
surface.  The maintainer approval recorded on PR #29601 applies to this
fenced subset; changing any item below requires another design review.

### Admitted operations

The native names may be used on character values for scalar comparison,
`IN`/`NOT IN`, grouping and hash/join keys, full and limited `ORDER BY`, and
window peer/partition evaluation.  Non-unique secondary indexes, cluster keys,
shuffle keys and their ordinary DML maintenance may use the tagged UCA physical
key.  The same semantic identity is carried through `SHOW CREATE TABLE`,
`INFORMATION_SCHEMA`, `LIKE`, checkpoint reconstruction and restore.

### Deliberately rejected operations

Native Unicode columns are not admitted as `PRIMARY KEY` or `UNIQUE` parts in
this release.  The rejection is applied consistently to inline and table-level
`CREATE TABLE` definitions, `CREATE UNIQUE INDEX`, `ALTER TABLE ADD`/`ADD
COLUMN ... UNIQUE`, and `MODIFY`/`CHANGE` that would convert an existing indexed
column to a native Unicode collation.  Existing binary/legacy primary and
unique keys remain supported.  Existing experimental catalogs that already
contain a native Unicode primary/unique key are not migrated by this PR; an
explicit rebuild and collision check is required before such a table can enter
the supported subset.

Malformed UTF-8, values outside the `utf8mb3` repertoire, unknown collation
revisions, and unknown physical-key formats fail closed at typed admission or
plan validation.  A failed `CREATE`/`ALTER` leaves the source catalog and its
legacy indexes usable.  No mixed-version deployment is claimed: a worker that
does not understand the versioned identity must reject the plan rather than
reinterpret it as `general_ci`.

### Restore and ownership rules

Persisted objects retain the charset, collation identity, UCA revision and
physical-key format independently of the local `Domain` enum.  Restore,
checkpoint replay and `LIKE` copy the metadata as a unit; legacy objects keep
their original bytes.  A tagged UCA key is borrowed only for the duration of a
comparison or hash probe unless the owning vector/index/hash table copies it.
The owner of a retained key is also responsible for releasing its vector or
hash-table allocation.  No backend scratch buffer or comparison key is shared
between statements, operators or CNs, and no background worker is introduced
by this feature.

## Resource and cost contract

Let `L` be the input byte length of one value.  The pinned UCA400 backend
provides the following hard per-value bounds, which are independent of the
number of rows in a batch:

| Buffer | Bound | Lifetime |
| --- | ---: | --- |
| decoded-weight scratch | `36 * L` bytes | one key construction |
| U4P1 key payload | `54 * L + 1` bytes | borrowed until the caller reuses scratch |
| tagged equality/physical key | `54 * L + 2` bytes | copied and owned by the retaining consumer |

The bounds are checked before backend work and are covered by the exhaustive
scalar fixture.  With the current nil-scratch adapters, one comparison can
temporarily materialize two operand keys and their weight buffers; that work is
short-lived and is proportional to the two input lengths.  It is not retained
across comparisons.  Hash sizing/encoding and shuffle may retain one tagged key
per row in their existing map/vector owner, so retained bytes are `O(rows * key
length)` and must be charged to that owner's existing memory budget.  Typed
write/copy validation may build and immediately discard a key; it must not
silently switch to a raw byte key on error.  A future scratch-aware adapter may
reduce heap churn, but it must preserve these bounds and ownership rules.

The acceptance requirement is linear scaling in input bytes and bounded peak
workspace, not parity with bytewise comparison.  A review run records
`ns/op`, `B/op` and `allocs/op` for representative 8/64/1024-byte values and
256/64-row batches for the sort, grouping/hash and shuffle consumers.  The run
must show no retained-memory growth after the operator/vector is released and
must exercise both the Unicode path and the binary/legacy fast path.  A result
that changes SQL equivalence, loses NULL/grouping semantics, or exceeds the
per-value bounds is a correctness failure even if its microbenchmark is fast.

### Exact-head consumer report

The following report was recorded on the exact PR head after adding the
consumer harnesses `BenchmarkUnicodeCollationSortConsumers`,
`BenchmarkUnicodeCollationHashConsumers`, and
`BenchmarkUnicodeCollationShuffleConsumers`:

```text
./.agents/skills/mo-dev/scripts/mo-cgo-test -run '^$' \
  -bench '^BenchmarkUnicodeCollation(Sort|Hash|Shuffle)Consumers$' \
  -benchmem -benchtime=100ms \
  ./pkg/sort ./pkg/sql/colexec/partition ./pkg/sql/colexec/shuffle
```

This run used Go 1.27.1 on an Apple M5 arm64 host.  Each cell below is
`ns/op / B/op / allocs/op`; `L/rows` is the input byte length and row count.
The legacy and binary controls take the existing bytewise fast path.  The
Unicode column exercises the UCA key path.  Hash rows include expression
evaluation, hash grouping and final materialization; the operator is created
and released on every benchmark iteration.  Sort and shuffle retain their
prepared input vector across iterations and release it after the case.

| Consumer | `L/rows` | legacy | binary | Unicode UCA |
| --- | ---: | ---: | ---: | ---: |
| sort | 8/256 | 7,349 / 0 / 0 | 7,112 / 0 / 0 | 421,627 / 587,328 / 19,152 |
| sort | 64/256 | 8,496 / 0 / 0 | 8,483 / 0 / 0 | 1,917,472 / 3,140,932 / 31,920 |
| sort | 1024/64 | 2,697 / 0 / 0 | 2,682 / 0 / 0 | 5,573,219 / 14,282,552 / 14,196 |
| grouping/hash | 8/256 | 165,868 / 105,260 / 526 | 159,864 / 104,770 / 526 | 288,899 / 280,930 / 6,157 |
| grouping/hash | 64/256 | 180,934 / 216,599 / 538 | 181,752 / 216,599 / 538 | 959,459 / 1,156,801 / 10,264 |
| grouping/hash | 1024/64 | 208,956 / 590,238 / 346 | 244,372 / 590,214 / 346 | 3,003,728 / 5,721,292 / 5,593 |
| shuffle | 8/256 | 1,345 / 0 / 0 | 1,316 / 0 / 0 | 34,617 / 47,104 / 1,536 |
| shuffle | 64/256 | 1,678 / 0 / 0 | 1,678 / 0 / 0 | 157,729 / 251,908 / 2,560 |
| shuffle | 1024/64 | 1,777 / 0 / 0 | 1,779 / 0 / 0 | 546,343 / 1,352,195 / 1,344 |

The Unicode allocations and bytes scale with both `L` and the number of rows;
the controls remain allocation-free in the prepared sort/shuffle path.  The
release checks are reproducible with:

```text
./.agents/skills/mo-dev/scripts/mo-cgo-test \
  -run 'TestUnicodeCollation(Sort|Hash|Shuffle)ConsumerRelease' -count=1 \
  ./pkg/sort ./pkg/sql/colexec/partition ./pkg/sql/colexec/shuffle
```

All three release tests pass and assert `mp.CurrNB() == 0` after every
operator/vector release (16 repetitions for each control).  Thus the report
records bounded per-value workspace, the retained-row/key scaling of the hash
consumer, and no retained-memory growth after release; it does not claim
bytewise performance parity for native UCA keys.

## Integration and rollout gates

PR #29601 is the integration point for the metadata/tuple, SQL-consumer, and
index work tracked by #29055/#29056/#29057. Persisted semantic identity and
key format remain independent of the local Domain enum. Legacy objects keep
their original identity and bytes; conversion requires an explicit rebuild
and collision check. Restore/SHOW/I_S preserve the native name and the
utf8mb3 repertoire contract.

Mixed-version plans and foreign pipeline owners still pass through
`RequireLegacyCollations`; they must carry the explicit UCA400 revision and
legacy physical key format. A missing or unknown version fails closed. The
required maintainer approval for this contract is recorded on PR #29601.

The release gate is the following semantic/cost matrix, run against the exact
head that changes the contract:

| Consumer | Required semantic evidence | Required resource evidence |
| --- | --- | --- |
| scalar/equality and membership | equality, `IN`/`NOT IN`, invalid-input and utf8mb3 rejection SQL cases | no tagged-key/raw-key alias; per-value bound |
| sort and window/partition | full/limited order, secondary keys, dense-rank peers and partition peers | sort `ns/op`, `B/op`, `allocs/op`; no scratch retained after the operator |
| grouping/hash and shuffle | equivalent values share one group/bucket; NULL and legacy controls remain unchanged | hash/shuffle allocation scales with retained rows and key bytes only |
| physical/non-unique index and restore | indexed/full-scan agreement before and after flush, `SHOW`/`LIKE`/checkpoint replay | retained physical keys owned and released by the existing vector/index owner |

The distributed charset/collation cases are the end-to-end oracle; targeted
package tests and the consumer benchmarks are supporting evidence, not a
replacement for the SQL result checks.  Native Unicode primary/unique key
support, migration of experimental catalogs, full mixed-version rollout and
byte-comparison performance parity remain outside this release gate.
