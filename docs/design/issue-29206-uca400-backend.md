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

## Integration and rollout gates

PR #29601 is the integration point for the metadata/tuple, SQL-consumer, and
index work tracked by #29055/#29056/#29057. Persisted semantic identity and
key format remain independent of the local Domain enum. Legacy objects keep
their original identity and bytes; conversion requires an explicit rebuild
and collision check. Restore/SHOW/I_S preserve the native name and the
utf8mb3 repertoire contract.

Mixed-version plans and foreign pipeline owners still pass through
`RequireLegacyCollations`; they must carry the explicit UCA400 revision and
legacy physical key format. A missing or unknown version fails closed. Before
merge, the maintainer approval for this contract should be recorded on PR
#29601 together with the distributed semantic regression result.
