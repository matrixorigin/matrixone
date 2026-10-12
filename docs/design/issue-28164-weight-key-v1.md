# Collation key V1: byte contract and independent validation

Parent design: #28842, merged as `76099145e701eb6c0412d3c3c73e8faf262fde3d`.
Validation baseline: the weight-backend checks in this PR are bound to this
branch and its source hashes. Integration `993957c232ac29e81009bb94275734b4f5d8786f`
is retained as historical evidence for tuple/decoder consumers only; it is not
current PR1 PASS. See `issue-28164-weight-key-v1-evidence.json` for the separate
source hashes, commands, terminal results and exact fixture identities.

This document freezes the **weight payload and tuple-field byte contract** for
the supported domains. It does not activate new tables or certify complete
SQL/index/DDL/storage consumers. The temporary production admission fence stays
closed; durable cluster admission and end-to-end acceptance remain separate.

## Scope and compatibility

- general-ci is required in the first product delivery. Native 0900 variants
  are independently tested and do not substitute for general-ci acceptance.
- Existing tables retain their old behavior and bytes after an upgrade. Only
  explicit successful rebuild switches them to the corrected semantics.
- Rebuild collisions reject migration and preserve the old table and rows.
  There is no automatic row selection, merge or deletion.
- This freezes an encoding, not the unfinished mechanism for propagating old
  versus new effective semantics through all SQL expressions.
- SQL `LIKE` is intentionally outside PR1. Weight equality and ordering do not
  prove wildcard semantics, so PR1 exports no wildcard matcher. PR3 owns the
  character-space implementation and its MySQL oracle matrix, including `%`,
  `_`, escapes, `ß`/`ss`, combining characters and long-pattern boundaries.
- Neither collation ID nor semantic/format version is embedded per key. Column
  and expression metadata supplies the domain; relation/index metadata supplies
  format. Missing format remains legacy (0). The current implementation symbol
  `PADSpaceKeyV1` (1) also selects the NO PAD domains; its name does not impose PAD.

## Dependency and existing storage-path compatibility

Vitess v0.24.0 brings a newer AWS SDK v2 dependency closure into the module
graph. This PR does not enable new SQL semantics, but the AWS SDK is already
used by MatrixOne's object-storage path, so the upgrade is treated as an
independent compatibility surface. `NewAwsSDKv2` explicitly selects
`RequestChecksumCalculationWhenRequired` and
`ResponseChecksumValidationWhenRequired`, preserving the pre-upgrade optional
checksum behavior while retaining checksums required by an operation. The
compatibility policy is covered by
`TestNewAwsSDKv2UsesCompatibilityChecksumPolicy`; existing small PUT, read,
unknown-size multipart and parallel multipart tests remain separate storage
path evidence. `DeleteObjects` checksum rejection is treated as a compatibility
signal: MatrixOne disables subsequent batch deletes and falls back to individual
`DeleteObject` calls, covered by `TestAwsDeleteMultiFallsBackToSinglesOnChecksumRejection`.
A live S3-compatible backend matrix is still NOT_RUN and is a required follow-up
before treating the dependency closure as production-ready.

## Normative payloads

| Domain | Padding | V1 payload |
| --- | --- | --- |
| binary / legacy pass-through | unchanged | Original bytes, including arbitrary invalid UTF-8 |
| utf8mb4_general_ci | PAD SPACE | Existing MO/TiDB-derived weights encoded using C1 below |
| utf8mb4_bin | PAD SPACE | Unicode scalar weights encoded using C1 below |
| utf8mb4_0900_ai_ci | NO PAD | Complete primary weight string from fixed Vitess v0.24.0, no fixed-character padding |
| utf8mb4_0900_bin | NO PAD | Validated UTF-8 input bytes, unchanged |

Vitess source commit: `e8d9fa81d351066497a8d99f92e9e9f2ecfd69d0`.
Module checksum: `h1:1beFkQ3hbozQv0pmuicu1LL22szH7yVwZR/fofGiXxk=`.
The ai_ci call is `WeightString(dst, input, 0)`, never `PadToMax`. For this
specific collation the output is primary weights in big-endian uint16 units;
there are no case/accent levels to append. Empty/ignorable input may yield an
empty payload. Changing the backend or mapping is subject to byte-compatibility
review; a changed V1 payload requires a new format and explicit migration.

C1 (PAD SPACE) is specified on numeric weights:

1. Defer a run of SPACE weights (decimal 32); discard a trailing run.
2. Before the next non-SPACE weight, emit byte `21` per deferred SPACE if the
   next weight is below 32, otherwise byte `23` per SPACE.
3. For a non-SPACE weight w: w < 32 emits w+1; 33 <= w < 128 emits w+3;
   w >= 128 emits its canonical UTF-8 scalar encoding.
4. End the payload with byte `22`.

Two-digit byte strings are hex; arithmetic above is decimal. This encodes
`low weight < low-side SPACE < end/PAD < high-side SPACE < high weight`.
The terminator represents the implicit SPACE suffix. Trimming spaces and using
ordinary lexicographic length order is not equivalent: empty sorts after NUL
under these PAD SPACE rules. Original spelling and leading length are absent.
C1 payloads contain no NUL; UCA/raw payloads may contain NUL.

## Tuple framing, prefix and decoding

One string field is `46 01 <escaped payload> 00`. Payload NUL becomes `00 FF`;
all other payload bytes are unchanged. Numeric/NULL tuple fields keep their
existing codecs. Bytewise tuple order must preserve resolved field order.
A UNIQUE key must never append raw spelling as a tie-breaker.

Prefix indexes first apply SQL character-prefix extraction to the original
value, then compute its weight. Slicing weight bytes is forbidden. Column
storage coercion and CHAR normalization happen before this API; neither this
API nor probe encoding truncates to declared column width.

| Domain / original | Payload | Complete string field |
| --- | --- | --- |
| general-ci empty / SPACE | `22` | `46012200` |
| general-ci NUL | `0122` | `4601012200` |
| general-ci Alpha / alpha+SPACE | `444f534b4422` | `4601444f534b442200` |
| general-ci a+SPACE+NUL | `44210122` | `46014421012200` |
| 0900-ai-ci Alpha | `1c471d771e0c1d181c47` | `46011c471d771e0c1d181c4700` |
| 0900-ai-ci é / e+combining acute | `1caa` | `46011caa00` |
| 0900-ai-ci empty | empty | `460100` |
| 0900-bin a+NUL | `6100` | `46016100ff00` |
| 0900-bin Alpha+SPACE | `416c70686120` | `4601416c7068612000` |

`StringKeyPart.Decode` consumes one field, returns owned bytes and the consumed
length. It reverses tuple escaping, not collation. Non-raw domains return
`Opaque=true` (including the conservatively treated 0900-bin path). Original
SQL values must come from the main row; decoded weights are never SQL text.

It rejects wrong type framing, unterminated fields/escapes, invalid C1 grammar,
invalid 0900-bin UTF-8, and odd-length UCA primary weights. Even-length UCA
payloads are opaque; the decoder does not prove they were generated by a valid
source string. A whole-tuple caller must validate field count, types and total
consumption. A prefix decoder cannot detect truncation that happens to form a
valid shorter field (e.g. cutting `00 FF` immediately after `00`). Storage
integrity checks remain the owner's responsibility; no checksum was added.

## Capacity and ownership

- `KeySizeUpperBound(n)` rejects negative lengths, unknown domains and integer
  overflow. Bounds: raw/0900-bin n; C1 4*n+1; 0900-ai-ci 16*n. The latter follows
  the pinned iterator's maximum of eight uint16 elements per code point,
  conservatively counting each UTF-8 byte as a code point.
- Exact field length is `3 + payload length + payload NUL count`. Fixed-buffer
  Encode checks capacity without overflowing arithmetic and appends no partial
  field on failure. A previously failed packer stays failed.
- These bounds are not storage key limits. Each SQL/vector/index owner must
  reject its own oversized result; no implicit truncation is permitted.
- Raw and 0900-bin keys borrow input. Other domains reuse caller scratch.
  Input and scratch must be disjoint; copy retained keys before reuse.
  Decode owns its result. No per-row locale state, metadata or C call is added.
- Invalid transformed UTF-8 fails before writing scratch or tuple bytes. Raw
  legacy data retains its previous malformed-byte behavior.

## Independent evidence and limits

The immutable historical `mysql_8_0_45.json.gz` remains unchanged. The extension
fixture records its SHA-256 and reuses only its existing answers. The new
`extend_oracle.py` queried a dedicated network-disabled, portless MySQL 8.0.45
(aarch64) container with the same pinned official image digest.

- 242 strings, 58,564 ordered pairs per collation, four domains: 234,256 answers.
  126,075 historical answers reused, 108,181 newly obtained from MySQL.
- Current Go 1.26.4/Vitess v0.24.0 results match all comparisons.
- Two native domains additionally match exact MySQL WEIGHT_STRING bytes for
  all 242 strings and length-delimited mapping digests over 65,536 scalars:
  all valid BMP scalars plus 2,048 supplementary samples.
- Historical general-ci/_bin mapping digests cover the same scalar corpus.
- Corpus includes controls/NUL, whitespace, accents, combining order,
  ignorables, expansions, supplementary/unassigned values, long strings and
  character prefixes. Tests also retain the independent 609,961-pair PAD model.
- The recorded integration baseline exercised real Packer tuples with numeric
  prefix/suffix, exact/one-short capacity, ownership, golden bytes and
  malformed fields. Those tuple/decoder checks are historical evidence for
  PR2 and are not implemented by this PR1 diff.
- Four corpus payload digests in `TestFrozenKeysMySQLOracle` lock V1 bytes.
  Those digests are MO format regression oracles, not independent MySQL results.
- The odd-length native-payload regression failed before the decoder repair
  and passed afterward. Fuzz and race are bounded evidence, not exhaustive
  proof over every Unicode string or corruption pattern.

The current PR1 package tests establish the weight-backend codec boundary only.
Tuple/decoder and executable consumer tests in the integration baseline are
listed as historical evidence; PR2 must rerun them against its own source.
General-ci/new-table semantics, all DML/index producers, real persisted
pruning, migration/rollback, mixed-version recovery, production performance,
the S3-compatible backend matrix and CI/QA remain NOT_RUN for this delivery.
#28164 is not closed.

Earlier local benchmark/fuzz records belong to their original versions and
remain in git history and `artifacts/issue-28164-d1`; they are not rebranded as
this validation. This contract supersedes that file's provisional and conflicting
claims about native 0900 admission. Current runs and raw oracle SQL/answers live
in `artifacts/issue-28164-freeze-20260916/`.
