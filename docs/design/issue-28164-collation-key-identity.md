# #28164 Collation-Aware Unique-Key Identity

- Status: Design draft; implementation not started
- Tracking issue: [#28164](https://github.com/matrixorigin/matrixone/issues/28164)
- Design revision: 1
- Frozen baseline: `c51bb4ed868219af720cb5c019fb103bc1e7bcc7`
- Scope: the complete string PK/UNIQUE identity contract; first delivery is design and baseline evidence only

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
current main baseline is frozen at
`c51bb4ed868219af720cb5c019fb103bc1e7bcc7` and was rechecked in the companion
run: it still leaves three rows, and a forced unique-index read returns the same
three physical values.

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
| `utf8mb4_0900_ai_ci` compatibility alias | must not be advertised as native UCA 9.0/NO PAD | reject for v2 until a native domain is specified |
| unsupported UCA names, JSON, DATALINK, unknown plugins | no safe key proof | reject or retain existing conservative behavior |

The general-ci weight table, malformed UTF-8 policy, supplementary-plane
handling, and key-version identifier are part of the codec version. `LOWER`,
`RTRIM`, locale-dependent library case folding, or a process-global mutable
collation table are not valid substitutes.

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
    Charset   uint8
    Collation uint16
    Prefix    uint32 // zero means no prefix; unit is defined by Domain
    Unit      PrefixUnit // characters for text, bytes for binary
}

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
Each part is framed with a NULL flag, domain ID, encoded length, and payload;
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
              domain_id (2 bytes)
              payload_length (4 bytes)
              normalized_payload (payload_length bytes)
```

The v2 envelope therefore has this logical shape:

```text
magic | codec_version | part_count |
  repeated { null_flag | domain_id | payload_length | normalized_payload }
```

`domain_id` is a registry identifier for the complete `(charset, collation,
prefix, prefix-unit, weight-version)` tuple; it is not a hash and cannot be
reused for a different tuple. A NULL part has `null_flag=1`,
`payload_length=0`, and no payload; a non-NULL empty value has `null_flag=0`
and `payload_length=0`, so the two cases remain distinct. The codec rejects
`part_count=0`, a flag other than 0/1, a payload length greater than
`math.MaxUint32`, or an envelope larger than the fixed 64 MiB allocation guard
before allocating or publishing a key. A backend with a smaller physical key
limit rejects the key earlier with its existing key-length error. Golden
vectors cover each header and boundary value; no Go string representation may
leak into the format. Invalid UTF-8 and unsupported domain IDs return an error
before a key is published.

### 4.3 Ordering and optimization

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
  // 0 is reserved for an explicitly bytewise relation; absence means legacy.
  uint32 value = 1;
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

### 5.2 Catalog and plan flow

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

### 5.3 Upgrade, downgrade, backup, and restore

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
It operates on one physical unique relation at a time under the existing table
write/DDL fence:

1. Read the source relation and resolve every indexed column's final type,
   prefix, and declared domain.
2. Verify that all participating services support the requested codec version.
3. Build a temporary v2 relation from a stable snapshot, encoding every row.
4. Detect equivalent non-NULL keys before publication. Report a bounded sample
   and total count of conflicting source row identities; abort without choosing
   a winner or deleting rows.
5. Validate row coverage, encoded-key uniqueness, source-to-key checksums, and
   forced-index versus table-scan results.
6. Atomically publish the new relation and version metadata in one catalog/DDL
   transition; retain the old relation until the transaction is durable.
7. After restart/replay observes the committed transition, garbage-collect the
   old relation through the ordinary DDL cleanup owner.

Failure before publication leaves the old relation and metadata usable. Failure
after publication is recovered by replaying the committed catalog transition;
the old relation is never made query-visible as the current unique identity.
Migration is not online double-write: new writes are fenced while the source
snapshot is copied, so there is one linearization point and no divergent key
formats.

## 7. End-to-end producer and consumer map

The implementation review must keep this table synchronized with the code. A
path is not considered covered merely because a search found no `Charset`
reference.

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
| Baseline reproduction | **PASS / REPRODUCED** on frozen `c51bb4ed…`; three rows remain after the issue statements, and the forced-index read agrees with the table scan |
| Design review | Pending maintainer review of this revision |

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
| r1 / pending | Planner/SQL owner | Equality, hash, ODKU target selection, and optimization fallback | Review Sections 2–4 and the producer/consumer table | PENDING_REVIEW |
| r1 / pending | Storage/transaction/catalog owner | Physical key bytes, lock/commit/replay, protobuf propagation, migration fence and recovery | Review Sections 5–7 and the migration protocol | PENDING_REVIEW |
| r1 / pending | QA/release owner | Upgrade, backup/restore, distributed capability fence, and acceptance matrix | Review Sections 6 and 9 before implementation PRs | PENDING_REVIEW |

The design is complete enough to start a separately reviewed implementation
series, but it is not marked approved until the named owners record findings and
dispositions against revision 1.
