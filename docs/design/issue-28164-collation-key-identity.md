# #28164 Collation-Aware Unique-Key Identity

- Status: Design revision 3; implementation increment in PR #28520; v2 remains gated and the full series is not complete
- Tracking issue: [#28164](https://github.com/matrixorigin/matrixone/issues/28164)
- Design revision: 3
- Frozen baseline: `c51bb4ed868219af720cb5c019fb103bc1e7bcc7`
- Scope: the complete string PK/UNIQUE identity contract; this PR carries the codec, metadata fence, planner key materialization, and guarded index probes. TN persistence, global comparison consumers, migration management, and rollout remain follow-up work.

## 1. Decision summary

`INSERT ... ON DUPLICATE KEY UPDATE` is not the owner of this defect. The
current system accepts a case-insensitive or PAD SPACE declaration, but the
existing-row probe, statement-local duplicate arbitration, lock key, hash
partition, and persisted unique-key index can still use the original bytes.
That allows two values which are equal in the declared comparison domain to
occupy different physical identities.

The selected direction is a shared, versioned equality-key codec. Every writer,
probe, lock, batch arbiter, persisted unique index, index build/rebuild, and
recovery reader for a given unique constraint must consume the same encoded key.
The user-visible column value remains unchanged and is stored separately. SQL
expression equality remains a separate contract from storage-image equality and
FULLTEXT posting identity.

Existing byte-oriented index structures may continue to store the encoded key as
opaque bytes. They do not need to implement Unicode collation themselves, but no
producer may bypass the codec. Range and zonemap optimizations are enabled only
when the codec has an ordering proof; otherwise the affected optimization falls
back to a safe scan or point lookup.

Existing relations are not silently reinterpreted. A future migration explicitly
validates the old relation, reports all equivalent-key collisions, builds the new
identity under a stop-write fence, and atomically publishes the new version.
There is no automatic row choice, data deletion, or online mixed-key double write.

## 2. Problem, evidence, and invariants

### 2.1 Observed behavior

The issue reproduction was originally reported on
`99ed717b769e261b842c2f17345bd65865fc1470`:

```sql
CREATE TABLE ci (
  id INT PRIMARY KEY,
  k VARCHAR(32) COLLATE utf8mb4_general_ci UNIQUE,
  v INT
);
INSERT INTO ci VALUES (1, 'Alpha', 10);
INSERT INTO ci VALUES (2, 'alpha', 20)
  ON DUPLICATE KEY UPDATE v = VALUES(v);
INSERT INTO ci VALUES (3, 'Alpha ', 30)
  ON DUPLICATE KEY UPDATE v = VALUES(v);
```

The reported result was three rows instead of one row `(1, 'Alpha', 30)`.
That is historical runtime evidence only until a current build is run. The
current-main baseline used for the executable evidence is frozen at
`c51bb4ed868219af720cb5c019fb103bc1e7bcc7`. The correction run
`artifacts/issue-28164-design/20260909T000000Z/` executes the complete sequence
once in each of three independent databases (`r1`, `r2`, and `r3`). Every
sample still leaves three rows, and a forced unique-index read returns the same
three physical values as a table scan. The earlier one-database run is retained
as historical evidence, but is not counted as the formal three-sample result.

The current run also exposes a wider contract gap that must be kept separate
from the physical-key work. `COLLATION(k)` reports `utf8mb4_general_ci`, but
both the column comparison and an explicit literal comparison return false for
`'Alpha'` versus `'alpha'`, and the same happens for the PAD SPACE pair
`'Alpha'` versus `'Alpha '`. `SHOW FULL COLUMNS` preserves the declared
collation, while `SHOW CREATE TABLE` does not render the column `COLLATE` clause.
This means a future implementation must decide and test the SQL comparison and
metadata-display contracts instead of assuming that a retained collation name
proves that its equality semantics already work.

### 2.2 Four identities that must not be conflated

For a value `x` and a fixed index comparison domain `D`, the implementation must
keep these meanings distinct:

1. **SQL equality**: the result of `=` or `<=>` after expression type
   resolution, coercibility, collation, and NULL rules.
2. **Unique-key identity**: the identity used by one declared PK/UNIQUE
   constraint, including each part's collation and prefix rule.
3. **Stored-value identity**: whether the final row image has the same bytes and
   type representation as the old image.
4. **FULLTEXT maintenance identity**: whether the same doc identity and complete
   tokenizer input produce the same hidden posting state.

The core unique-key law for non-NULL keys is:

```text
Equal_D(a, b)  <=>  Key_D(a) = Key_D(b)
Equal_D(a, b)  =>  Hash_D(a) = Hash_D(b)
```

The second relation is one-way because hash collisions are allowed. A fixed-size
hash is never the unique-key identity. A key containing a NULL part follows the
existing SQL rule that it does not conflict with another NULL-containing key;
the encoded NULL marker is still required to keep composite serialization
unambiguous.

The following implications are deliberately *not* valid:

```text
SQL equality             => stored bytes are equal
SQL equality             => FULLTEXT tokenizer input is equal
same encoded hash        => unique keys are equal
same byte sort order     => collation sort order
```

In particular, a future collation-aware `<=>` must not be reused as the
`#28066` FULLTEXT no-op proof. That proof needs an explicit storage-image
comparison for the final indexed columns and row identity.

### 2.3 Safety and liveness invariants

For every PK or UNIQUE constraint that opts into a non-bytewise domain:

1. Every candidate row is normalized after target type conversion and before
   conflict detection.
2. Existing-row lookup, current-statement arbitration, lock acquisition,
   commit-time uniqueness validation, delete-old-key, insert-new-key, index
   creation/rebuild, restart replay, backup/restore, and clone all use the same
   codec version and domain metadata.
3. A secondary-key conflict resolves to the same base-row identity that is
   subsequently locked and updated. No path may lock only the incoming primary
   key.
4. Two concurrent transactions committing equivalent non-NULL keys leave at
   most one committed key for that constraint. A failed loser leaves no partial
   row or hidden-key state.
5. A key-format mismatch, unsupported collation, malformed value, missing
   metadata, or unavailable codec fails closed before publishing a write.
6. A migration either publishes a complete new relation atomically or leaves the
   old relation usable; it never exposes a partially rebuilt unique index.
7. Replaying a committed migration is idempotent. Restart and retry cannot make
   an old key visible after the new version has been published.
8. A range/bloom/zonemap optimization may reject a candidate only when its
   ordering and encoding domain are proven compatible. Without that proof it
   must over-include or be disabled, never false-negative.

## 3. Supported comparison domains

The first implementation is intentionally narrower than “all MySQL collations”.
The table below is the product contract for the design; unsupported domains must
remain explicit rather than silently mapped to a different one.

| Declaration/domain | Key policy | Initial status |
| --- | --- | --- |
| `utf8mb4_general_ci` and the existing general-ci aliases | Unicode general-ci weight key, PAD SPACE handling, frozen weight/version policy | supported after v2 codec and migration |
| `utf8mb4_bin` / `utf8_bin` | UTF-8 text identity with PAD SPACE semantics; not a raw binary-string key | supported only after a dedicated PAD-space encoding proof |
| `binary`, `VARBINARY`, `BINARY` | exact bytes; no case folding and no text-space reinterpretation | retain bytewise v1 behavior |
| `CharsetLegacy=0` old text metadata | historical bytewise identity | retain and never reinterpret automatically |
| `utf8mb4_0900_ai_ci` compatibility alias | normalize to the existing general-ci-v1 PAD SPACE domain; never advertise native UCA 9.0/NO PAD | accepted only as a normalized compatibility alias; native 0900 is rejected |
| unsupported UCA names, JSON, DATALINK, unknown plugins | no safe key proof | reject or retain existing conservative behavior |

The general-ci weight table, malformed UTF-8 policy, supplementary-plane
handling, and key-version identifier are part of the codec version. `LOWER`,
`RTRIM`, locale-dependent library case folding, or a process-global mutable
collation table are not valid substitutes.

The normalized-domain policy is intentional. Current MatrixOne catalog metadata
stores `utf8mb4_0900_ai_ci` through the same `CharsetUTF8` identity as
general-ci, so an old relation cannot be reconstructed as native UCA 9.0. An
explicit migration therefore treats that stored identity as
`general-ci-v1`/PAD SPACE and records that normalized domain in the new key
metadata. A DDL request that requires native UCA 9.0, NO PAD, or preservation of
the original 0900 declaration is rejected with an ambiguity/unsupported-domain
error; it is never silently assigned the general-ci key. A future native 0900
codec requires a distinct collation/domain ID and its own metadata before it can
be enabled.

## 4. Key codec contract

### 4.1 Ownership and placement

The future implementation adds a dependency-light internal package
`pkg/common/collationkey`. It may depend on `pkg/container/types` for type and
charset identifiers, but it must not depend on planner, executor, aggregation,
or storage packages. The package owns the canonical domain IDs, weight table
version, malformed-input policy, and allocation-bounded encoders.

The planned interface is:

```go
type Domain struct {
    Type      TypeFamily
    Charset   uint8
    Collation uint16
    Prefix    uint32 // zero means no prefix; unit is defined by Domain
    Unit      PrefixUnit // characters for text, bytes for binary
    Width     uint16 // declared width for fixed-width numeric domains
    Scale     int16  // declared scale for decimal domains
}

type TypeFamily uint8

const (
    Text            TypeFamily = 1
    Binary          TypeFamily = 2
    SignedInteger   TypeFamily = 3
    UnsignedInteger TypeFamily = 4
    Decimal         TypeFamily = 5
)

type Part struct {
    Domain Domain
    Value  []byte
    Null   bool
}

func EncodePart(dst []byte, part Part) ([]byte, error)
func EncodeComposite(dst []byte, parts []Part) ([]byte, error)
func Equal(domain Domain, left, right []byte) (bool, error)
func HashEncoded(encoded []byte) uint64
```

The interface is internal and versioned with the on-disk key format. `Equal`
and `EncodePart` must be derived from one normalization routine; they may not
drift into separate implementations. `HashEncoded` hashes the complete encoded
identity, including domain/version framing, and is never used as the sole
conflict oracle.

### 4.2 Normalization order

All call sites follow this exact sequence:

```text
final SQL type conversion and length enforcement
  -> index prefix truncation (characters for text, bytes for binary)
  -> domain normalization (case/weight/PAD rule)
  -> length- and type-delimited composite framing
  -> byte hash, lock key, or persisted index key
```

The original user value remains in its base-table column. A prefix is applied to
the value visible to that key part, not to an already encoded weight stream.
Each part is framed with a NULL flag, domain-family ID, canonical parameter
descriptor, encoded length, and payload;
the composite key has an explicit part count. This prevents concatenation
ambiguity such as `(a,bc)` versus `(ab,c)`.

The v2 envelope is a byte sequence with the following frozen wire layout. All
integer fields are unsigned big-endian; no host-endian or Go ABI representation
is permitted:

```text
offset  size  field
0       4     magic ASCII "MOKY"
4       1     codec_version (2)
5       2     part_count (1..16, matching the current key-part limit)
7       ...   repeated part records:
              null_flag (1 byte, 0 or 1)
              type_tag (1 byte; one of the registered TypeFamily values)
              domain_id (2 bytes; stable normalization-family ID)
              parameter_length (2 bytes)
              domain_parameters (parameter_length bytes)
              payload_length (4 bytes)
              normalized_payload (payload_length bytes)
```

The v2 envelope therefore has this logical shape:

```text
magic | codec_version | part_count |
  repeated { null_flag | type_tag | domain_id | parameter_length |
             domain_parameters | payload_length | normalized_payload }
```

`domain_id` is a registry identifier for a normalization family, not a hash and
not a substitute for the complete `(charset, collation, prefix, prefix-unit,
type, width/scale, padding rule, and weight-version)` tuple. Each family fixes
the algorithm, charset/collation, padding and weight-version policy; its
`domain_parameters` carry every tuple field that varies per key part. The
canonical pair `(domain_id, domain_parameters)` is therefore the complete
domain identity. A family ID may be used for multiple parameter values only
when its registered parameter schema validates them; a different algorithm,
weight version, charset/collation policy, or parameter schema receives a new
registry ID. `type_tag` must agree with the registry entry. A NULL part carries
the same domain descriptor as a non-NULL part but has `null_flag=1`,
`payload_length=0`, and no payload; a non-NULL empty value has `null_flag=0`
and `payload_length=0`, so the two cases remain distinct. The codec rejects
`part_count=0`, a flag other than 0/1, an unknown or non-canonical parameter
descriptor, a parameter length greater than `math.MaxUint16`, a payload length
greater than `math.MaxUint32`, or an envelope larger than the fixed 64 MiB
allocation guard before allocating or publishing a key. A backend with a
smaller physical key limit rejects the key earlier with its existing key-length
error. Golden vectors cover each header, parameter schema, and boundary value;
no Go string representation may leak into the format. Invalid UTF-8 and
unsupported family/parameter pairs return an error before a key is published.

### 4.3 Registry, normalization, and golden vectors

The registry is an append-only manifest named `collationkey/domains-v1`. Family
IDs are assigned once, never reused for another normalization algorithm, and
are interpreted identically by every CN, TN, rebuild worker, backup tool, and
restore reader. Registry version 1 has the following family IDs and parameter
schemas:

| ID | Type/domain | Canonical parameter schema | Frozen policy |
| --- | --- | --- | --- |
| `0x0001` | text, UTF-8, general-ci | `u8(schema=1) \| u32(prefix) \| u8(unit=characters)` | `general-ci-v1`, PAD SPACE |
| `0x0002` | text, UTF-8, `_bin` | `u8(schema=1) \| u32(prefix) \| u8(unit=characters)` | UTF-8 bytes, PAD SPACE |
| `0x0003` | binary | `u8(schema=1) \| u32(prefix) \| u8(unit=bytes)` | exact bytes, no padding |
| `0x0101` | signed integer | `u8(schema=1) \| u16(declared_width)` | declared-width two's-complement bytes |
| `0x0102` | unsigned integer | `u8(schema=1) \| u16(declared_width)` | declared-width big-endian bytes |
| `0x0103` | decimal | `u8(schema=1) \| u16(declared_width) \| i16(scale)` | canonical sign/scale/coefficient encoding |

The manifest digest is deterministic: `SHA-256` of
`ASCII("MOKD") || u8(registry_version) ||` the entries sorted by ID, where each
entry is `u16(id) || u16(entry_length) || entry_bytes` in big-endian order.
`entry_bytes` contains the `type_tag`, fixed charset/collation/padding/weight
policy, and the canonical parameter schema plus its validation ranges; it does
not enumerate every legal prefix or declared width. The relation metadata
stores both `registry_version` and this 32-byte digest; a node or restore path
rejects a mismatch before opening the relation. ID zero and IDs absent from the
manifest are invalid. A decoder includes the parameter length and bytes in the
encoded identity, validates the family schema and all ranges before reading the
payload, and rejects a tuple that is not registered for that family.

The normalized payload is defined without a dependency on a process locale:

1. Type conversion and declared-length enforcement happen first. A text prefix
   counts Unicode code points and a binary prefix counts bytes. Numeric prefixes
   are rejected. Text input must be valid UTF-8 for v2; the legacy comparator's
   synthetic malformed-byte weights are not imported into the new format.
2. For `general-ci-v1`, trailing ASCII U+0020 is removed, then each decoded
   rune contributes one immutable weight from the copied
   `utf8mb4GeneralCIWeight` table. The table is named and frozen as
   `general-ci-v1`; it maps ASCII case variants to the same primary weight,
   maps `É`/`é` to `0x0045`, and maps every supplementary-plane rune to
   `0xFFFD`. Each weight is emitted as a four-byte big-endian unsigned value.
3. For the UTF-8 `_bin` domain, trailing U+0020 bytes are removed after the
   prefix is selected; all remaining UTF-8 bytes, including case and embedded
   NUL, are preserved. Binary values preserve every byte, including trailing
   spaces. Empty payloads are valid and remain distinct from NULL markers.
4. Signed and unsigned integers use the declared fixed width and big-endian
   two's-complement or unsigned representation. Decimal payloads contain a
   one-byte sign (`0` for non-negative, `1` for negative), a four-byte signed
   scale, a four-byte coefficient length, and the minimal big-endian coefficient
   bytes. Leading coefficient zeroes and trailing decimal zeroes are removed
   while decreasing scale; zero is always sign 0 and scale 0. Float, date/time,
   JSON, DATALINK, and unknown types are rejected for v2 until their domains
   have a separately registered identity.

5. The `type_tag`, family ID, and canonical parameter descriptor are written
   into the part record before the payload. A decoder checks the type/domain
   pair, parameter schema and ranges, lengths, and registry digest; it never
   guesses a type or parameter tuple from payload bytes.

The following vectors are normative byte-for-byte examples. Spaces in the
display are separators only:

| Input/domain | Encoded envelope (hex) |
| --- | --- |
| non-NULL `"Alpha"`, ID `0x0001` | `4D4F4B59 02 0001 00 01 0001 0006 010000000001 00000014 00000041 0000004C 00000050 00000048 00000041` |
| non-NULL `"alpha"`, ID `0x0001` | identical to `"Alpha"` |
| non-NULL `"Alpha "`, ID `0x0001` | identical to `"Alpha"` (PAD SPACE) |
| non-NULL `"A\\0"`, ID `0x0001` | `4D4F4B59 02 0001 00 01 0001 0006 010000000001 00000008 00000041 00000000` |
| non-NULL U+1F600, ID `0x0001` | `4D4F4B59 02 0001 00 01 0001 0006 010000000001 00000004 0000FFFD` |
| non-NULL `"Alpha"`, ID `0x0002` | `4D4F4B59 02 0001 00 01 0002 0006 010000000001 00000005 416C706861` |
| non-NULL `"Alpha "`, ID `0x0003` | `4D4F4B59 02 0001 00 02 0003 0006 010000000002 00000006 416C70686120` |
| NULL text part, ID `0x0001` | `4D4F4B59 02 0001 01 01 0001 0006 010000000001 00000000` |
| empty non-NULL text part, ID `0x0001` | `4D4F4B59 02 0001 00 01 0001 0006 010000000001 00000000` |
| `(signed int64 1, text "A")`, IDs `0x0101,0x0001` | `4D4F4B59 02 0002 00 03 0101 0003 010008 00000008 0000000000000001 00 01 0001 0006 010000000001 00000004 00000041` |

The one-line hex forms are obtained by removing separators; for example the
first vector is
`4D4F4B5902000100010001000601000000000100000014000000410000004C000000500000004800000041`.
The test implementation must include these vectors, the malformed-input error,
and the maximum-part/maximum-size rejection cases before a v2 relation can be
enabled.

### 4.4 Ordering and optimization

Equality identity and ordering identity are separate contracts. The v2 payload
must receive an ordering proof before it is used for an ordered range bound. A
point probe can use exact encoded bytes without such a proof. Until the proof is
available:

- unique conflict checks use exact encoded point keys;
- SQL predicates retain a typed collation comparator;
- hash joins, repartition, DISTINCT, and GROUP BY use a matching domain-aware
  hash/equality pair;
- affected zonemap, bloom, and range-pruning paths over-include or fall back;
- binary and legacy bytewise paths remain eligible for their existing bytewise
  optimizations.

## 5. Metadata and persistence contract

### 5.1 Version location

The encoding version belongs to the physical relation that owns the key, not to
the SQL expression alone. The next implementation PR adds the following
length-delimited protobuf message and fields (the design PR does not modify
the `.proto` files):

```protobuf
message UniqueKeyCodecVersion {
  // 0 retains legacy/bytewise behavior; absence is also legacy.
  uint32 value = 1;
  uint32 registry_version = 2;
  bytes registry_digest = 3;       // exactly 32 bytes for value 2
  uint32 max_encoded_key_bytes = 4; // fixed at 67108864 for value 2
}

// api.SchemaExtra, field 20
UniqueKeyCodecVersion unique_key_codec_version = 20;

// plan.TableDef, field 40
UniqueKeyCodecVersion unique_key_codec_version = 40;
```

The two field numbers are currently unused. Their wire type is length-delimited
so binaries that predate v2 skip the complete message and preserve unknown
fields on round-trip. Upgrade tests must assert that preservation; a catalog or
plan path that drops the field is incompatible and must be fenced before a v2
relation is published. The base table and every secondary UNIQUE hidden
relation carry their own message.

The physical relation that owns the key, not the SQL expression alone, carries
the version:

- the base table's primary-key relation carries the version for its physical PK;
- each secondary UNIQUE hidden relation carries its own version;
- the logical `IndexDef` continues to describe parts and algorithm and is not
  overloaded with a collation encoding version;
- non-unique and FULLTEXT relations do not opt into this key contract.

The next implementation PR copies the message through
`PlanDefsToExeDefs`, `DefsToSchema`, `SchemaToDefs`, table-schema
serialization, catalog replay, plan deep-copy, snapshot, clone, and restore.
The `value` values are:

```text
0 = absent/legacy relation; preserve historical bytewise behavior and make no
    v2 claim
1 = explicit bytewise key format
2 = collation-aware framed key format defined by this design revision
```

An absent message and an explicit value of 0 both retain legacy behavior; only
values 1 and 2 are opt-in formats.
The planner rejects a v2 unique constraint when the owning physical relation
does not report version 2 exactly. It also rejects a write when any CN/TN or
storage reader cannot advertise v2 capability. Existing `IndexAlgoParams.version`
continues to mean the FULLTEXT algorithm version and is not reused.

### 5.2 Capability publication and activation fence

The version message describes a relation; it is not a node capability. The next
implementation adds a typed capability and a replicated activation record (the
design PR does not edit `proto/*.proto`):

```protobuf
message UniqueKeyCodecCapability {
  uint32 readable_versions = 1;       // bit n means version n is readable
  uint32 writable_versions = 2;       // bit n means version n is writable
  uint32 registry_version = 3;
  bytes registry_digest = 4;           // exactly 32 bytes
  uint32 max_encoded_key_bytes = 5;
}

message UniqueKeyCodecActivation {
  enum Phase {
    DISABLED = 0;
    PREPARING = 1;
    ENABLED = 2;
    ABORTED = 3;
  }
  uint32 requested_version = 1;
  uint32 registry_version = 2;
  bytes registry_digest = 3;            // exactly 32 bytes
  uint64 generation = 4;                // monotonically increasing fence
  Phase phase = 5;
  map<string, uint64> cn_targets = 6;   // UUID -> heartbeat generation
  map<string, uint64> tn_targets = 7;
}
```

For the current `proto/logservice.proto` snapshot, the typed fields are
reserved as follows: `CNStoreHeartbeat.unique_key_codec_capability = 25`,
`TNStoreHeartbeat.unique_key_codec_capability = 15`,
`CNStoreInfo.unique_key_codec_capability = 27`,
`TNStoreInfo.unique_key_codec_capability = 14`, and
`CheckerState.unique_key_codec_activation = 19`. The implementation PR must
recheck these numbers against its exact base before editing the proto; a used
number is a hard stop, not an invitation to reuse another message's field.

Activation is a replicated state-machine transition, not a best-effort health
flag:

1. HAKeeper records `PREPARING` with a new `generation`, the requested version,
   registry version/digest, and the complete current CN/TN UUID target set.
2. Every target must publish a fresh heartbeat with matching readable and
   writable bits, registry digest, maximum key size, and an incarnation newer
   than the preparation record. The state machine moves to `ENABLED` only after
   all targets acknowledge the same generation. A missing, stale, mismatched, or
   reconnected node keeps the phase in `PREPARING`; it is never treated as
   implicitly compatible.
3. Planner admission checks relation metadata, local capability, and the
   activation generation. The serialized execution plan carries all three
   values. TN commit rechecks them immediately before mutation and rejects a
   missing/mismatched capability or generation before writing either base or
   sidecar data.
4. A restart or reconnect creates a new node incarnation and clears its target
   acknowledgement. Capability loss demotes the activation to `PREPARING` and
   rejects new v2 writes until the node is replaced or re-acknowledges. Existing
   committed v2 data is routed only to readers advertising the exact registry;
   an old reader is rejected or excluded, never allowed to guess from unknown
   protobuf fields.
5. `api.CloneExtra`, catalog-cache copies, plan deep copies, snapshot/restore,
   and replay copy the typed fields explicitly. Unknown-field retention by
   protobuf alone is not a compatibility mechanism because field-by-field clone
   helpers can drop it. Compatibility tests exercise every copy boundary and
   assert that a dropped capability or activation record fails closed.

The activation record is persisted with the catalog/checker state and is
replayed before scheduling writes. A v2 relation cannot be created while the
phase is `DISABLED`, and an `ABORTED` generation is never retried implicitly.
An old node may remain serving v1 relations while it is drained from the target
set, but it cannot read or write a v2 relation. Thus “field skipped” and “safe
to execute v2” are deliberately different decisions.

### 5.3 Physical key and row locator

The selected representation keeps the user-visible primary-key columns and
`PkeyColName` unchanged and stores the derived identity in a storage-owned
sidecar relation. No hidden logical column is exposed through SQL:

- A primary-key sidecar stores `KeyV2(K(pk)) -> base-row locator`.
- Each secondary UNIQUE hidden relation stores
  `KeyV2(K(index parts)) -> source table ID + row locator` and its own version,
  registry digest, and activation generation.
- The base row continues to hold the original bytes. Foreign keys, FULLTEXT
  doc-id/tokenizer input, user projections, and row-return paths read that
  original value; they never decode or substitute the sidecar bytes.
- The DML write projection computes final typed values, encodes each affected
  key, and inserts/deletes the sidecar entry atomically with the base-row
  mutation. A key update removes the old sidecar identity and inserts the new
  one in the same transaction.
- A probe encodes the incoming user value, performs an opaque point lookup, then
  resolves and snapshot-validates the returned base row under the same codec
  version. The locator is not the user PK and does not replace it. If compaction
  invalidates a row handle, the sidecar entry is resolved through the base
  table's current row handle/user PK or the operation fails closed and schedules
  a rebuild; it is never treated as a match for a different row.
- Migration builds the sidecar from one locked snapshot and atomically switches
  the sidecar metadata. The logical schema and all FK references remain stable;
  only the owning physical relation changes identity format.

This choice avoids changing `PkeyColName` or FULLTEXT's source-column contract,
while making the physical key bytes available to storage and lock owners. A
future implementation may choose a storage-native key encoding instead of a
literal sidecar table only if it preserves this same locator, version, and
atomic-switch contract; it must not introduce a SQL-visible generated column as
an accidental second source of truth.

### 5.4 Catalog and plan flow

DDL constructs the version on the physical table definition. The compile path
serializes it into `SchemaExtra`; the engine schema stores it with the relation
schema; catalog snapshots and logtail replay carry the same bytes. Every plan
load validates that the base relation and each referenced hidden UNIQUE relation
agree with the expected version before the first input row enters a write path.

Schema alterations that do not rebuild a unique relation preserve its version.
Adding a unique index to a legacy table creates the new relation in v1 unless a
completed migration explicitly selects v2; it must not imply that the base PK
has migrated. Dropping an index removes only its own physical relation after the
normal DDL ownership protocol.

### 5.5 Upgrade, downgrade, backup, and restore

Older binaries that do not understand v2 can read catalog rows only when no v2
relation is opened for writing. A capability fence prevents an old writer from
publishing a bytewise key into a v2 relation. Once v2 is published, downgrade to
a binary that cannot read v2 is unsupported; recovery uses a compatible binary
or a pre-migration backup.

Backups, snapshots, clone, and restore preserve the version field and reject a
relation whose key data and metadata version disagree. A restored v2 relation is
not downgraded by omission of an unknown field.

## 6. Explicit migration protocol

Migration is an explicit DDL/admin operation, not an implicit upgrade action.
The ordinary table lock is not a sufficient fence: the current lock operator
returns early for non-pessimistic transactions, and a lock acquired after a
snapshot would still validate stale data. The migration therefore owns a
persisted `UniqueKeyMigrationGate` for one physical relation:

```text
gate = { relation_id, migration_epoch, owner, owner_incarnation, claim_token,
         phase, phase_deadline, source_schema_epoch, source_snapshot_id,
         temp_relation_id, publication_txn_id, replay_generation,
         retired_replay_targets }
phase = OPEN | DRAINING | EXCLUSIVE | PUBLISHED | ABORTED
```

Every write producer (plain INSERT, IGNORE, ODKU, REPLACE, UPDATE, DELETE,
INSERT SELECT, LOAD DATA, bulk/direct/S3 ingestion, and partition routing) must
obtain a shared permit from this gate before opening its write transaction and
carry the `migration_epoch` through planning, lock acquisition, and TN commit.
An ingress that cannot carry the epoch is not compatible with v2 migration and
causes the migration request to fail before it changes the gate. Reads remain
allowed throughout.

The supported transaction contract is explicit. Autocommit writes and explicit
pessimistic transactions participate normally. An optimistic transaction that
was admitted before `DRAINING` may finish only with its recorded epoch and a
commit-time gate check; if the connector or executor cannot provide that check,
the transaction is aborted and the migration remains blocked. New write
transactions are rejected once `DRAINING` is durable. No code path relies on
the existing table-lock operator's pessimistic-only behavior.

The owner and recovery contract is persisted with the gate. `owner` is not a
process name: it is the tuple `(owner_uuid, owner_incarnation, claim_token)`.
Every side effect after `EXCLUSIVE`—snapshot creation, temporary writes,
publication, replay acknowledgement, and cleanup—must present the current
`migration_epoch` and `claim_token`. HAKeeper's DDL recovery coordinator is the
terminal recovery owner. It claims an expired phase with a compare-and-swap on
`relation_id`, `migration_epoch`, `phase`, and `claim_token`, then records a new
incarnation and token. A former owner that reconnects with a stale token may
observe state but cannot publish, release the gate, or delete a replacement's
temporary relation.

`phase_deadline` is set for `DRAINING`, `EXCLUSIVE` acquisition, temporary
build/validation, durable publication, and replay acknowledgement. A deadline
never silently restores v1. Before publication, the recovery owner first
persists `EXCLUSIVE -> ABORTED` with the temporary relation identity, then
releases the write fence; the old relation is immediately usable while an
idempotent cleanup owner retries deletion of the invisible temporary relation.
After publication, recovery never writes `ABORTED` and never selects v1 again.
It resolves `publication_txn_id` against the durable catalog/log record: a
committed record completes or replays `PUBLISHED`, while a durable
not-committed result permits a fenced abort and temporary cleanup. An outcome
that is still uncertain keeps writes rejected until the log barrier makes one
of those two results durable; retries use the same idempotency token.

Replay acknowledgement has a bounded target decision. A target that misses its
deadline is either reconnected with a new incarnation and must acknowledge the
same `replay_generation`, or is atomically marked in `retired_replay_targets`
by HAKeeper and fenced from v2 reads/writes. Once every non-retired target has
acknowledged the published generation, the recovery owner releases the gate and
hands the old relation to ordinary cleanup. A missing target is never waited on
forever and never treated as an implicit acknowledgement; if membership cannot
yet make the retirement decision, the relation stays `PUBLISHED` and v1 is not
re-enabled.

The operation proceeds as follows:

1. Resolve every indexed column's final type, prefix, normalized domain, and
   source schema epoch. Verify that all current CN/TN/HAKeeper targets are
   `ENABLED` for the requested codec and registry.
2. Persist `OPEN -> DRAINING` with a new monotonic `migration_epoch`. Admission
   rejects new writes and TN commit rejects a stale/missing epoch. Existing
   permits registered before the transition are allowed to commit or abort
   under the old epoch; the gate waits a bounded interval for the registry to
   become empty. On timeout it persists `ABORTED`, releases the gate, and leaves
   the old relation untouched.
3. Persist `DRAINING -> EXCLUSIVE`, acquire the single relation permit, and hold
   it from this point through snapshot, temporary build, collision validation,
   catalog publication, durable commit, and replay acknowledgement. The source
   snapshot is created *after* exclusive ownership. If any engine operation
   started a snapshot earlier, discard it and restart after the gate. A changed
   schema epoch, row-count/checksum watermark, or source snapshot identity
   invalidates the build and restarts it while the gate is still held.
4. Build a temporary v2 relation from that stable snapshot, encoding every row.
   Detect equivalent non-NULL keys before publication. Report a bounded sample
   and total count of conflicting source row identities; abort without choosing
   a winner or deleting rows. Validate row coverage, encoded-key uniqueness,
   source-to-key checksums, and forced-index versus table-scan results.
5. Atomically publish the new sidecar relation and version metadata in one
   catalog/DDL transition identified by `publication_txn_id`, persist
   `PUBLISHED`, and retain the old relation until the transition is durable and
   replay has acknowledged the same `replay_generation`. New writes remain
   rejected until the activation fence observes the published relation.
6. After restart/replay observes the committed transition, and every active
   target has acknowledged or been fenced/retired under the recovery contract,
   release the gate and hand the old relation to the ordinary DDL cleanup
   owner. Garbage collection is retryable and cannot make the old identity
   query-visible again.

Failure before publication persists `ABORTED` and leaves the old relation and
metadata usable; temporary-relation cleanup is separate, idempotent, and
retryable. Failure after publication is recovered by resolving and replaying
the committed catalog transition; the old relation is never selected as the
current unique identity. An uncertain publication outcome is not treated as an
abort until the durable log barrier proves that the idempotency token did not
commit. If a capability, ingress, or transaction-mode check cannot be proven,
the operation is rejected rather than weakening the fence. This is not online
double-write: writes are stopped while the source snapshot is copied, so there
is one linearization point and no divergent key format.

## 7. End-to-end producer and consumer map

The implementation review must keep this table synchronized with the code. A
path is not considered covered merely because a search found no `Charset`
reference.

The current source entry points and the future codec boundary are explicit:

| Current source entry | Producer/consumer today | Required v2 boundary and terminal error |
| --- | --- | --- |
| `pkg/sql/plan/bind_insert.go` ODKU binding | existing-row LEFT JOIN and ordered target selection | encode every PK/UNIQUE part before the probe; metadata/capability mismatch rejects the plan |
| `pkg/sql/colexec/preinsertunique/preinsertunique.go` | statement-local duplicate arbitration and serialized unique values | replace raw-value/hash identity with the framed key; collisions are checked by encoded bytes, not only a hash |
| `pkg/sql/colexec/lockop` and `pkg/sql/compile/lock_meta.go` | row/table lock acquisition | lock the encoded unique identity and resolved base locator; timeout/rollback releases the epoch permit |
| `pkg/sql/plan/build_ddl.go` (`buildUniqueIndexTable`) | secondary UNIQUE hidden relation creation | record relation-local codec metadata and encode the snapshot before publication; collision aborts the build |
| `pkg/vm/engine/disttae/txn_table.go` and TAE relation/index owners | physical PK/index read and write | sidecar point lookups consume opaque v2 bytes and verify the base locator at the same snapshot |
| `pkg/vm/engine/disttae/cache/types.go` and `pkg/pb/api/api.go` | catalog/schema clone and cache copies | explicitly copy typed version, registry, capability, and activation fields; a missing field fails closed |
| `pkg/sql/plan/primary_key_semantics.go` | uniqueness inference and bytewise legacy guards | retain legacy guards until relation version/domain proof is present; never infer from an unversioned key |
| `pkg/sql/plan/build_dml_util.go` FULLTEXT source projection | doc-id and tokenizer input from user columns | keep original user PK/text values; do not use or decode the unique sidecar key |

These entries are joined to the complete operation map below. The map covers
both the SQL-visible producer and the persistence/locking consumer, so a future
implementation cannot claim coverage from planner search results alone.

| Path | Required identity | Owner/decision | Failure behavior | Proof |
| --- | --- | --- | --- | --- |
| Plain INSERT / INSERT SELECT | encoded PK and UNIQUE keys | shared codec before probe and preinsert | fail closed before write | SQL BVT + operator UT |
| `INSERT IGNORE` | same key, different conflict action | same target arbiter | ignore only after encoded conflict | SQL BVT |
| ODKU | same lookup and statement-local key | ordered ODKU target arbiter | no partial update | original + same-batch BVT |
| REPLACE / UPDATE / DELETE | old-key deletion and new-key insertion | DML maintenance owner | rollback restores both images | DML UT/BVT |
| LOAD DATA / bulk / S3 / partition writes | batch key and lock identity | ingestion planner/executor | reject unsupported version | path-specific tests |
| Secondary UNIQUE hidden relation | framed key as physical PK | index relation builder/storage | version mismatch rejects | restart/rebuild test |
| PK relation | base relation version and source value | table schema/engine | old relation remains if migration fails | migration test |
| Index creation/rebuild | encode from base snapshot | DDL index builder | collision aborts build | migration/rebuild BVT |
| Existing-row probes | exact encoded point key | planner and execution lookup | safe over-include only | forced-index BVT |
| Statement-local dedup | domain-aware hash/equality | preinsert unique | collision checked by bytes | batch UT |
| Lock keys | encoded unique identity plus resolved base row | lock owner | timeout/rollback cleans prefix | two-session test |
| Commit-time dedup/replay | versioned encoded identity | TN/storage | mismatched capability aborts | restart/upgrade test |
| Migration admission | relation gate epoch on every write ingress | catalog/TN migration gate | unsupported ingress or stale epoch rejects before mutation | gate state-machine test |
| Capability/activation | relation version plus registry digest/generation | HAKeeper, CN/TN heartbeat | stale, missing, or reconnected node is fenced | rolling upgrade test |
| SQL equality/hash join/group/distinct | resolved expression domain | SQL function/hash owners | unsafe optimization disabled | query consistency tests |
| Bloom/zonemap/range filters | order-compatible key only | index/filter owner | over-include or scan fallback | forced/index-scan pair |
| FK and doc-id consumers | original user PK value | existing FK/FULLTEXT owners | no accidental re-encoding | regression tests |
| Backup/restore/clone/snapshot | version and physical-key bytes | persistence owner | reject mismatch | persistence matrix |

The current ODKU restriction against updating PK/UNIQUE columns remains. This
issue does not expand that unsupported semantic; it fixes the identity used to
detect a conflict in supported writes.

## 8. Why the alternatives are rejected

### ODKU-only comparison patch

Changing only the existing-row LEFT JOIN can fix the three-line reproduction but
leaves statement-local arbitration, locks, commit validation, index creation,
and restart on raw bytes. It cannot prove uniqueness and is rejected.

### Global SQL comparator rewrite

Changing every string `=`/`<=>` to a collation comparator without changing hash
keys and physical indexes can create hash-bucket misses and false-negative
pruning. Reinterpreting old raw indexes also makes existing equivalent rows
ambiguous. It is rejected.

### Shared versioned codec with explicit migration

This keeps the storage engine's byte primitives, centralizes the equivalence
contract, makes compatibility observable, and gives collisions a deterministic
operator-visible outcome. It is selected despite the migration cost because it
is the only option that closes the write, read, lock, and restart boundaries.

## 9. Validation contract for the implementation series

### Unit and planner tests

- golden codec vectors for case, accent, PAD SPACE, empty values, embedded NUL,
  supplementary characters, malformed input, and domain/version framing;
- `Equal_D`/encoded-key/hash consistency and composite ambiguity tests;
- prefix character-versus-byte boundaries and NULL-containing composite keys;
- ordered ODKU target selection, same-batch duplicate arbitration, generated
  key and alias handling, and reset/error cleanup;
- lock-key equality and resolved-target ownership for primary and secondary keys;
- hash join/partition, DISTINCT/GROUP BY, bloom and zonemap positive and
  negative cases;
- codec capability mismatch and malformed metadata fail-closed tests.

### SQL/BVT tests

Extend the existing charset and ODKU cases after the executable implementation:

1. Reproduce the three original statements and assert one row `(1,'Alpha',30)`.
2. Insert equivalent keys in one statement and assert a single unique identity.
3. Repeat with `INSERT IGNORE`, `REPLACE`, and UPDATE controls.
4. Compare binary, `_bin`, and NULL controls, including trailing spaces.
5. Compare forced-index and table-scan results after each operation.
6. Verify SQL-equivalent but byte-different text still triggers necessary base
   writes and FULLTEXT maintenance; do not use SQL equality as a posting no-op.
7. Run two sessions with barriers, rollback the winner/loser, and assert at most
   one committed identity.
8. Flush/restart, rebuild, backup/restore, and explicit migration; assert old
   collisions abort without data loss.
9. Execute text protocol and prepared statements with the same values and
   compare affected rows and final state.

### Migration state-machine acceptance

The implementation must also run deterministic barrier-driven cases over the
persisted gate; a wall-clock sleep is not an oracle:

1. Kill the owner after `EXCLUSIVE` and before publication. A recovery owner
   claims the same epoch with a new token, persists `ABORTED`, cleans the
   invisible temporary relation idempotently, and eventually admits writes to
   the old relation. The stale owner cannot publish or release the gate after
   it returns.
2. Stop the client after the publication request is durably submitted but
   before its result is known. Recovery uses the same `publication_txn_id` and
   log barrier to prove either `PUBLISHED` or `ABORTED`; it never exposes both
   identities and never rolls a committed publication back to v1.
3. Drop a replay target before its acknowledgement. The target is either
   reconnected with a new incarnation and acknowledges `replay_generation`, or
   HAKeeper fences and retires that incarnation. Once the remaining targets
   acknowledge, writes are admitted under v2; the missing target cannot write
   or be counted as an implicit acknowledgement.

### Performance and operational checks

Benchmark codec ns/op, allocations, encoded-key size, and composite width before
measuring batch INSERT/ODKU hit/miss paths. Report medians on the same machine;
do not put wall-clock thresholds into ordinary BVT. Verify that point conflict
checks remain proportional to input rows and constraints, not a full-table scan.

## 10. Design review record and completion status

| Field | Decision |
| --- | --- |
| Change scope | Design plus current-main baseline evidence; later implementation PR series |
| Design trigger | Crosses planner, executor, lock, storage, catalog, upgrade, and hot-path boundaries; persistent compatibility and concurrency triggers apply |
| Selected direction | Shared versioned codec with explicit stop-write migration |
| Public SQL/API change in this delivery | None |
| Production implementation | Not started by this design PR |
| QA decision | Required for implementation: user-visible uniqueness, persistence, concurrency, and compatibility behavior |
| Baseline reproduction | **PASS / REPRODUCED** on frozen `c51bb4ed…`; three independent databases in `20260909T000000Z` each leave three rows, and forced-index reads agree with table scans |
| Design review | **READY_FOR_MAINTAINER_REVIEW** for revision 3; maintainer approval is still pending |

Open review findings must be recorded against this exact revision. Any change to
codec domains, metadata location, migration linearization, or FULLTEXT/storage
comparison invalidates the review and requires a new design revision before
production code is written.

### 10.1 Review routing and record

The review is intentionally split by ownership; the author self-review below is
evidence, not maintainer approval.

| Revision/date | Reviewer role | Finding or decision | Disposition | Status |
| --- | --- | --- | --- | --- |
| r1 / 2026-09-08 | Author self-review | ODKU-only and global-comparator alternatives do not cover statement arbitration, locks, persistence, or old-index ambiguity | Kept rejected; selected shared versioned codec with explicit migration | Recorded, not approval |
| r1 / 2026-09-08 | Author self-review | Current main preserves the declared general-ci name but accepts case/trailing-space duplicates; `SHOW CREATE` also omits the column collation | Added as measured baseline evidence and a separate SQL/metadata contract to verify | Recorded, not approval |
| r1 / 2026-09-08 | Local design review | P1: the existing table lock does not fence non-pessimistic transactions or stale snapshots | Revision 2 adds a persisted `UniqueKeyMigrationGate`, ingress epoch, transaction drain/reject rules, post-fence snapshot, and lock-through-publication protocol in Section 6 | **ADDRESSED / maintainer review pending** |
| r1 / 2026-09-08 | Local design review | P1: capability fencing had no fields, activation state, or reconnect behavior; typed clone paths can drop unknown fields | Revision 2 defines typed capability/activation records, current free field reservations, generation acknowledgements, CN/TN admission and commit checks, and explicit copy-boundary requirements in Sections 5.1–5.2 | **ADDRESSED / maintainer review pending** |
| r1 / 2026-09-08 | Local design review | P1: user PK and encoded physical PK/locator were not selected | Revision 2 selects a storage-owned sidecar key with source-table/row locator; logical PK, FK, and FULLTEXT continue to use original values in Section 5.3 | **ADDRESSED / maintainer review pending** |
| r1 / 2026-09-08 | Local design review | P1: domain IDs, payload rules, type framing, and golden vectors were incomplete | Revision 2 freezes registry IDs/digest framing, type tags, string/numeric payload rules, malformed-input behavior, and normative byte vectors in Sections 4.1–4.3 | **ADDRESSED / maintainer review pending** |
| r1 / 2026-09-08 | Local design review | P2: `0900_ai_ci` rejection was impossible to implement from normalized catalog metadata | Revision 2 selects normalized general-ci-v1 migration for the compatibility alias and rejects native/identity-preserving 0900 requests with an explicit ambiguity error in Section 3 | **ADDRESSED / maintainer review pending** |
| r1 / 2026-09-08 | Local design review | P2: original reproduction was recorded only once | Added the three-independent-database correction run `20260909T000000Z`; the one-sample artifact remains historical and is not counted | **ADDRESSED / evidence PASS** |
| r3 / 2026-09-09 | XuPeng-SH | P1: EXCLUSIVE/build/publication/replay could strand the persisted stop-write gate after owner loss, uncertain commit, or a missing replay target | Revision 3 defines the recovery owner, claim-and-fence token, per-phase deadlines, pre-publication abort, durable publication outcome resolution, target retirement, and deterministic barrier-driven acceptance cases in Section 6 and Section 9 | **ADDRESSED / maintainer review pending** |
| r3 / 2026-09-09 | XuPeng-SH | P2: immutable domain IDs were declared complete tuples but the frozen registry had no identities for widths, scales, or prefixes | Revision 3 makes IDs normalization families and encodes/validates canonical per-part parameters in the v2 envelope; the family schema and parameter pair form the complete identity, with updated golden vectors in Sections 4.2–4.3 | **ADDRESSED / maintainer review pending** |
| r3 / pending | Planner/SQL owner | Equality, hash, ODKU target selection, and optimization fallback | Review Sections 2–4 and the producer/consumer table against the frozen vectors | PENDING_MAINTAINER_REVIEW |
| r3 / pending | Storage/transaction/catalog owner | Physical key bytes, lock/commit/replay, protobuf propagation, migration fence and recovery | Review Sections 5–7 and the migration protocol | PENDING_MAINTAINER_REVIEW |
| r3 / pending | QA/release owner | Upgrade, backup/restore, distributed capability fence, and acceptance matrix | Review Sections 5–6 and 9 before implementation PRs | PENDING_MAINTAINER_REVIEW |

Revision 3 closes the currently recorded local design findings and is complete
enough to start a separately reviewed implementation series. It is marked
ready for maintainer review, not approved: the named owners must still record
acceptance or new findings against this revision before production code is
written. The baseline reproduction is evidence of the current defect only;
`production_fix` and `qa_acceptance` remain not implemented/not run.

### 10.2 Implementation series status (PR1)

The first implementation delivery is intentionally limited to the dependency-
light codec foundation in `pkg/common/collationkey`. It does not change any
planner, executor, catalog, protobuf, storage, or SQL entry point. The package
therefore cannot make an existing relation collation-aware by itself; the
remaining metadata, capability, sidecar, query, DML, migration, and activation
deliveries remain required before the issue can be closed.

PR1 freezes the following implementation details without changing the approved
revision-3 contract:

- registry name `collationkey/domains-v1`, registry version `1`, codec envelope
  version `2`, and a maximum framed key size of `67108864` bytes;
- family IDs `0x0001` (UTF-8 general-ci-v1/PAD SPACE), `0x0002` (UTF-8 `_bin`/
  PAD SPACE), `0x0003` (exact binary), `0x0101` (signed integer), `0x0102`
  (unsigned integer), and `0x0103` (decimal);
- canonical parameter bytes use big-endian prefix/width fields and the schema
  bytes defined in Section 4.3; the immutable registry digest is
  `0c84115b0e4999cd90fd03c1fb4bedb3ed560a4e97e64f73840952c4e469feca`;
- all text input is validated as complete UTF-8 before prefixing, general-ci
  emits one four-byte big-endian weight per code point, and NULL/empty values
  retain distinct framed states;
- malformed descriptors, unknown families, invalid widths/scales, non-canonical
  decimal payloads, invalid UTF-8, and over-sized envelopes fail before a key is
  published; `HashEncoded` is only a bucket hint and never a uniqueness oracle.

The digest and golden vectors are package tests, not a claim that a relation has
adopted v2. Subsequent implementation PRs must copy the exact bytes and digest
through the approved metadata and capability boundaries before any production
writer is allowed to emit this format.

### 10.3 Implementation series status (PR3)

PR3 is a metadata transport and validation foundation only. It adds typed
`UniqueKeyCodecVersion` fields to the plan and schema-extra messages, reserves
typed capability/activation records in the logservice messages, and explicitly
copies the relation version through the current plan, engine, catalog-cache,
schema-extra, and deep-copy boundaries. The dependency-light codec package
validates the supported version, registry version/digest, and maximum encoded
key size, and separates readable from writable capability bits.

PR3 does not publish an activation state, admit a v2 relation, change a unique
index, or make any existing writer emit v2 bytes. Capability publication,
HAKeeper persistence, generation acknowledgements, plan/TN admission, and
activation recovery remain unimplemented until the distributed state-machine
and storage owners provide those consumers. A missing or absent field therefore
continues to mean legacy behavior; this PR must not be interpreted as a
production compatibility gate by itself.

### 10.4 Implementation series status (PR4)

PR4 adds the dependency-light row-locator envelope used by the planned
storage-owned UNIQUE sidecar. `pkg/common/collationkey` now defines
`RowLocator`, `EncodeLocator`, and `DecodeLocator`. The format is the fixed
big-endian sequence `MOKL | version(1) | relation_id(u64) |
partition_id(u64) | primary_key_length(u32) | original_primary_key_bytes`.
Relation identity is mandatory, the primary-key bytes are copied on decode, and
truncation, version, length, zero-relation, and the shared 64 MiB allocation
guard fail closed. Empty primary-key bytes remain valid; the locator never
stores a compaction-sensitive `__mo_rowid` and is not a replacement for the
user-visible primary-key value.

This PR is a storage contract and test foundation only. No catalog relation is
created, no existing hidden index changes type, and no planner, lock owner, TN
commit path, or migration command consumes the locator yet. Until those paths
are wired atomically with the base row and capability/activation checks, v2
remains disabled and all production writes retain their existing behavior.

### 10.5 Implementation series status (PR5)

PR5 adds a dependency-light comparison adapter in the planner function package.
`CollationKeyEqual` and `CollationKeyNullSafeEqual` evaluate an explicitly
aligned UTF-8 general-ci or UTF-8 `_bin` text pair through the same immutable
normalization used by the v2 codec. NULL-safe comparison keeps SQL `<=>`
semantics, while unsupported or legacy domains fail closed instead of silently
changing their bytewise behavior.

The adapter is deliberately not registered as a user-visible function and is
not called by the existing SQL equality operator, hash join, index probe, or
ODKU planner. This preserves the legacy contract until a relation's codec
version and comparison domain have been proven at every producer and consumer
boundary. PR5 therefore supplies executable comparison tests only; it does not
create a v2 relation, replace resident/spill key codecs, alter unique-index
bytes, or enable any writer. Sidecar, lock, query-routing, migration, and
activation work remain required before a production fix can be claimed.

### 10.6 Implementation series status (PR6)

PR6 adds the common planner admission check for relation-local codec metadata.
Legacy and explicitly bytewise relations continue through their existing DML
paths. A relation carrying a valid v2 metadata record is rejected at the
shared table-resolution boundary while the sidecar, capability, lock, commit,
query, and migration consumers are incomplete; malformed metadata fails as an
internal error. This prevents an early writer from emitting bytes that an
older reader could not interpret.

The check is a fail-closed safety fence, not a v2 enablement mechanism. It does
not change existing relation behavior, create a management command, or claim
that any DML entry point is v2-capable. The final implementation must replace
this temporary rejection only after all required producers and consumers share
the same activation generation and recovery protocol.

The same revision also rejects v2 relations at the ordinary table-scan binding
boundary. A planner that has not yet proved the versioned point-probe, scan,
join, and fallback consumers cannot open a v2 relation for SELECT or an index
lookup while allowing legacy bytewise readers to continue. Missing metadata and
the explicitly bytewise format remain unchanged; malformed v2 metadata fails
closed as an internal error. This read-side fence is intentionally a temporary
admission rule and is not a capability publication or storage-reader
implementation.

### 10.7 Implementation series status (v2 key materialization primitive)

The next runtime increment adds `INTERNAL_COLLATION_KEY_V2`, an unregistered
planner-owned vector primitive. Given a `VARCHAR`/`TEXT` value, a character
prefix length, and an explicit supported charset descriptor, it emits the
exact `collationkey.EncodePart` envelope used by the shared codec. It validates
the value type and charset before evaluating rows, preserves NULL as a NULL
result, rejects NULL or out-of-range descriptors, and propagates malformed
UTF-8 or unsupported-domain errors without publishing a partial key. The
result is a binary varlena value; the original user value is not overwritten.

The primitive is intentionally absent from `functionIdRegister`, so SQL text
cannot call or constant-fold it by name. A future v2 sidecar writer may build a
plan expression with the exported encoded overload ID only after relation
metadata, capability, activation generation, and migration gates have passed.
No catalog relation, unique-index probe, lock, transaction commit, query
consumer, or management command calls this primitive yet; the read/write
admission fences therefore continue to reject v2 relations. This increment is
an executable runtime foundation, not a user-visible fix or an enablement
claim.

### 10.8 Implementation series status (activation contract)

The activation increment adds a dependency-light `collationkey.Activation`
contract for the durable PREPARING/ENABLED/ABORTED state. It validates the
requested codec and registry digest, requires non-zero generation and explicit
CN/TN target incarnations, copies target maps on creation, and exposes a
single `Enable` operation that requires every target to acknowledge both read
and write support. Acknowledgements are matched by node identity and exact
incarnation, so a restarted or stale writer cannot satisfy a previous
generation. `Advance` cannot bypass that check and enabled/aborted generations
cannot be downgraded to disabled.

This is the state-machine contract and its executable unit tests only. It is
not yet wired to HAKeeper persistence, heartbeat publication, plan/TN commit
admission, recovery owner, or a management command. Until those consumers are
connected, the planner's v2 read/write rejection remains in force and no
relation can be enabled by this package alone.

### 10.9 Implementation series status (sidecar entry envelope)

The sidecar increment adds a dependency-light `MOKS` entry envelope for the
planned storage-owned mapping from a complete `MOKY` equality key to a stable
`MOKL` row locator. Both variable-length components are length-delimited and
validated before publication; decoding copies pooled input bytes and rejects
unknown versions, malformed keys, malformed locators, and trailing data.
The locator remains the original physical primary-key image plus relation and
partition identity, never a transient `__mo_rowid`.

This package is only a wire/validation contract. No catalog relation, hidden
index schema, point probe, lock, transaction writer, replay consumer, or
migration command uses it yet. The v2 read/write admission fences therefore
remain active, and this increment does not change legacy or user-visible SQL
behaviour.

### 10.10 Implementation series status (migration gate contract)

The migration increment adds a dependency-light `MigrationGate` state contract
for the explicit stop-write protocol. It records the relation and monotonic
epoch, fenced owner/incarnation/token, phase deadline, post-drain snapshot and
temporary relation identities, publication transaction, replay generation,
outstanding pre-drain permits, and per-target replay acknowledgements or
retirements. The executable transitions enforce OPEN → DRAINING → EXCLUSIVE →
PUBLISHED, allow pre-publication ABORTED recovery, require all permits to drain
before EXCLUSIVE, and make publication irreversible. Expired owners can be
replaced only with a different fencing identity; stale owners cannot mutate or
release a gate.

The state machine is not connected to HAKeeper, catalog persistence, SQL
write admission, TN commit, snapshot creation, or a management command. It
therefore does not stop real writes or migrate a relation; the planner's v2
read/write fences remain in force. This increment only freezes and tests the
failure/recovery contract that those production consumers must implement.

### 10.11 Capability propagation correction

The sidecar/migration increment also closes a metadata ownership gap in the
existing HAKeeper state adapter: CN and TN heartbeat capability messages are
now copied into their corresponding `CNStoreInfo`/`TNStoreInfo` records, with
the nested registry digest cloned rather than retained by alias. A subsequent
heartbeat that omits the capability clears stale state. This is necessary for
an eventual activation coordinator to make decisions from replicated state;
protobuf wire round-tripping alone was insufficient. It does not advertise v2
support from current nodes or connect activation to HAKeeper decisions.

The same increment adds explicit wire adapters from the replicated heartbeat
messages to the common capability/activation validators. A missing capability
is treated as unsupported, and a non-nil malformed activation is rejected;
target maps and registry bytes are copied. This prevents a protobuf transport
round-trip from becoming an accidental downgrade and gives the eventual
coordinator a single validation boundary, while still leaving the actual
heartbeat publication and activation state machine unconnected.

### 10.12 Durable HAKeeper activation state

The activation record is now also a field of `HAKeeperRSMState` (field 43) in
the logservice schema. The generated binding therefore carries the same typed
activation through RSM snapshots and Dragonboat recovery, while the existing
`StateQuery` response copies it into `CheckerState`. The query path returns a
deep copy, so callers cannot mutate replicated target maps or registry bytes.
Round-trip, snapshot/recovery, and state-query aliasing tests cover the new
boundary.

This is persistence and observation only. No RSM command changes the phase,
heartbeats do not advertise a new capability, and no planner, TN commit,
catalog, migration, or management entry point consumes the field yet. The
v2 admission fences remain enabled; a missing activation still means that
production v2 is disabled rather than legacy data being reinterpreted.

### 10.13 Replicated activation proposal boundary

The HAKeeper RSM now reserves `SetUniqueKeyCodecActivationUpdate` as an
internal replicated command whose payload is the typed activation record. A
PREPARING record may be installed from the disabled state (or from an older
ABORTED generation); an ENABLED record is accepted only when it matches the
current PREPARING identity and every targeted CN/TN heartbeat has a matching
non-zero incarnation plus both read and write capability. Enabled and aborted
generations cannot be cleared by a stale disable command. The RSM returns
deterministic pending/applied values and leaves state unchanged on a rejected
transition.

The capability wire record carries the node incarnation used for this exact
target match. Current CN/TN services still publish no v2 capability, and no
management client proposes this command, so the admission fences continue to
prevent production enablement. This command closes only the durable proposal
and capability-check boundary; it does not implement sidecar writes, query
consumers, TN commit validation, migration, or administrator authorization.

### 10.14 Planner/runtime key materialization boundary

The integration increment adds two planner-owned, unregistered primitives. A
single text part is materialized as `__mo_collation_key_v2(value, prefix,
charset)` and a composite key as repeated `(value, prefix, charset)` triplets.
Both expressions return the exact `MOKY` bytes from the shared codec; prefix
lengths are applied by the codec and are not implemented with a second
`substring`/`serial` rule. Composite keys emit a NULL result when any part is
NULL, preserving the existing UNIQUE NULL policy. The original user columns
remain in the row image.

For a v2 text primary key, DDL planning adds the hidden `__mo_cpkey` physical
identity column (or changes the existing composite identity column to binary)
and keeps the original columns in `Pkey.Names`. Insert/update projections fill
that column with the same single/composite `MOKY` expression. Thus the base
table's storage-level primary uniqueness is not left on raw user bytes; target
and foreign-key-facing expressions can still refer to the original source
columns.

When a v2 relation definition is already present, the planner uses these
expressions for unique-index projections, ODKU target probes, ordered dedup,
row-level lock keys, and regular unique-index delete/reinsert joins. Hidden
unique relations are typed as binary values and carry the same relation
metadata, so every connected edge compares the framed bytes rather than raw
text or an ad-hoc serial tuple. Composite primary-key comparisons use the same
framing over the source parts; the stored target lookup identity is the hidden
encoded primary-key column while the original source value remains available
for user-facing and foreign-key expressions.

The relation metadata now carries a non-zero activation generation. New text
PRIMARY/UNIQUE definitions are marked v2 only when a request context contains
an enabled, generation-matching activation and local CN/TN acknowledgement;
otherwise creation remains legacy. A table mixing a v2 candidate with an
unregistered key domain is rejected instead of silently creating a table-scoped
hybrid. Missing or invalid admission still fails closed at both read and write
planner boundaries. The HAKeeper command and storage/query/TN migration
consumers are not yet connected, so no user-visible v2 relation can be created
on the normal SQL path in this increment.

### 10.15 Transactional sidecar owner contract

`pkg/common/collationkey.SidecarStore` supplies the storage-owner contract used
by a future hidden-relation adapter. It indexes the complete encoded `MOKY`
bytes and stores a copied `MOKL` locator; hashes are never used as the unique
identity. `Begin` requires the same enabled activation generation and local
read/write acknowledgement as the planner. Per-key optimistic fencing rejects
a commit that races another transaction on any touched key, while unrelated
keys may commit independently. `Put` rejects a different locator for an
existing identity, `Delete` can require the expected old locator, and all
staged changes publish atomically or not at all. `Snapshot` and
`RestoreSidecarStore` validate ordering, relation identity, key envelopes, and
locator envelopes before exposing a recovered map. This package is a
dependency-light reference for TN/catalog integration; it is not a process
global index and is not itself connected to the production hidden-table write
path yet.

### 10.16 Implementation series status (PR8 planner and index-probe increment)

PR8 carries the first production-planner integration of the v2 identity bytes.
Primary-key and UNIQUE projections, ODKU/REPLACE/UPDATE/DELETE conflict probes,
foreign-key parent locks, and unique-index maintenance now materialize the
relation-local framed identity rather than falling back to the legacy `serial`
or raw text bytes. Composite and prefix parts are framed in declared order;
missing or invalid source positions fail closed. Range rewrites, index-only
projections that cannot reconstruct the original value, raw residual pushdown,
and runtime-filter rewrites are disabled for opaque v2 unique keys unless an
exact point-equality proof is available.

The change is deliberately still gated. The normal service capabilities do not
advertise the v2 codec, no SQL DDL or DML path can enable it, and there is no
TN/catalog sidecar persistence or commit-time validation yet. Global SQL
collation/hash/group consumers, bulk-ingestion paths, migration management,
and production heartbeat/activation wiring remain required before a v2 table
can be exposed. The existing `pkg/common/collationkey.SidecarStore` and
`MigrationGate` are reference contracts, not storage implementations. PR8
therefore supplies planner and fallback safety for the eventual format but is
not the complete #28164 fix; `production_fix` and `qa_acceptance` remain
incomplete and the issue stays open.
