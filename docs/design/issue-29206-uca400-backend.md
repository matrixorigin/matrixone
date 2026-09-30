# UCA400 comparison-key foundation for #29206

## Scope

This is a dependency of native `utf8mb4_unicode_ci` / `utf8_unicode_ci`
support, **not an SQL feature activation or a fix closing #29206**. No parser,
planner, persisted type ID, catalog, index writer, or admission gate changes.
Existing Domain values and frozen comparison keys retain their meaning.

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

## Subsequent integration gates

Reuse/reconcile the metadata/tuple, SQL-consumer and index work in
#29055/#29056/#29057; do not assume these drafts enable native collation.
Persist semantic identity and key format independently of the local Domain
enum. Comparison, grouping/hash/join, PK/UNIQUE/backfill/ODKU, index lookup,
persisted filtering, recovery and mixed-version admission must agree before
accepting either SQL name. Coordinate default inheritance with #29374.
Preserve old object identities and key bytes; conversions require explicit
rebuild and collision checks. Restore/SHOW/I_S and the utf8mb3 repertoire
contract also belong to that integration, not to this library foundation.
