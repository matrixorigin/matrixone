# Exact ENUM member domains on 4.2

Scope: PR #32723 / the ENUM-domain part of #30771 and #30776. Binary collation and unrelated HEX behavior are not closed by this change.

## 1. Contract and evidence

Accept comma-bearing ENUM labels, preserve their bytes/order/ordinals through storage, DML, metadata replay, views, ALTER, import, clone and restore. Preserve existing case-folding, numeric coercion, strict/IGNORE and ordinal-zero policies. SET keeps its existing comma prohibition. Ordinary 4.2 scalar display is unchanged.

The current comma-joined domain is irreversible: both ['a,b','c'] and ['a','b','c'] yield a,b,c. Published 4.2.1/4.2.8 actually store/read the latter interpretation: 'c' has ordinal 3, numeric 1/2 display 'a'/'b', and inserting literal 'a,b' fails. The original rel_createsql still contains the two-member declaration. Re-grouping from that SQL would reinterpret ordinal 2 and invalidate ordinal 3.

A real legacy domain ['2#3:a','b1:c'] has payload 2#3:a,b1:c, exactly the proposed new payload for ['a,b','c']. Payload-prefix detection is therefore forbidden. Released headers have the reserved type byte zero. Published 4.2.8 stores and reads 65,536-byte domain text despite attr_enum's declared varchar width; adding a 65,535-byte codec cap would be a regression.

Invariants:
- The authoritative domain is the pair (encoding, payload); neither field can be copied or committed independently.
- Existing encoding 0 always keeps its historical interpretation; startup never guesses original grouping or rewrites ordinals.
- Unknown encoding or malformed new framing is an error, never CSV fallback or an IGNORE-adjusted user value.
- A new domain reader owns framing; SQL formatting and lookup consume decoded labels.
- No new worker, global cache, feature flag, peer-version gate or catalog column is introduced.

## 2. Alternatives and choice

| Alternative | Assessment |
| --- | --- |
| Keep/reject comma input | Does not meet the required capability; remove the temporary ENUM rejection. |
| Escaped CSV with an explicit tag | Viable, but adds escape semantics for comma/backslash and needs careful control-byte handling; no benefit over byte lengths. |
| JSON/string prefix auto-detection | Unsafe collision with old labels. JSON strings also require an explicit policy for invalid UTF-8; prefix guessing is rejected. |
| New repeated member fields throughout plan/catalog/TAE | Viable with wider schema/migration work; duplicating a list and the existing string creates additional authoritative state. |
| Explicit tag plus counted byte framing in existing payload | Selected: collision-free, byte-preserving, bounded, no unescape copies, unchanged physical ordinals and catalog layout. |

New ordinary nonempty domains without comma retain encoding 0 and identical CSV bytes. New comma-bearing domains and the singleton empty domain use encoding 1. This is a representation rule, not query/data-size tuning. Other empty members, such as ['a',''], remain representable in encoding 0.

## 3. Encoding and common API

Encoding 0 is the existing CSV representation. No prefix, quote, JSON or backslash heuristic changes it.
Encoding 1 grammar is N# followed by N consecutive L:BYTES entries:
- N is canonical decimal, 1..types.MaxEnumLen (65,535).
- L is canonical nonnegative decimal byte length, not rune count.
- No signs, whitespace or leading zeroes except the single length digit 0.
- Read exactly L bytes, then the next length; require exact exhaustion after N entries.
- ['a,b','c'] encodes as 2#3:a,b1:c; [''] as 1#0:.
- Labels may contain any byte already supported by the input/storage path, including commas, colons, #, quotes, backslashes, CR/LF and NUL. The codec performs no Unicode normalization or UTF-8 replacement.

Put encoding constants, EncodeEnumMembers, DecodeEnumMembers and decoded-label comparison in pkg/container/types/enum.go or one adjacent enum-domain file. Encode returns payload/tag/error; Decode returns immutable label slices/error. Reuse the existing name/index/numeric lookup helpers; do not create a second parser or case-fold implementation.

Before allocating the label slice, validate N and N <= remainingBytes/2. Every parsed length is checked against remaining input before slicing; checked arithmetic prevents decimal/size overflow. The label slice is at most 65,535 string headers; strings borrow the already-owned payload. Encoder total length must fit both host int and the existing uint32 storage length. Do not invent a 64-KiB cap; retain existing request/memory budgets.

Decode 0 plus empty text as [''] only for complete-domain metadata comparison/formatting. Existing legacy empty-domain DML/coercion behavior is not silently reclassified: a generic temporary T_enum with empty metadata is not evidence of a defined domain. New [''] uses nonempty encoding-1 payload and works through normal logical-type recognition. Old empty declarations retain correct metadata; explicit new creation/reload can repair their old value-input limitation.

## 4. Type and persistence representation

- Add uint32 enum_encoding = 8 to proto/plan.proto; regenerate plan.pb.go with the pinned protoc/gogo toolchain. Omitted field means 0.
- Give types.Type.dummy2 the semantic name enumEncoding, retaining its uint8 position and total struct size; expose narrow accessors. It is meaningful only for T_enum.
- Keep Type.Marshal's 20-byte format: encoding occupies the existing reserved uint16 slot at offset 6, with canonical high byte zero. Raw EncodeType/DecodeType used by TAE retain the existing 16-byte layout and the corresponding byte at native offset 3.
- Width, Scale, Charset, OID and not-null bits are not encoding storage. Validate supported enum tags before narrowing plan values or writing a full domain; unknown values must not wrap to a known tag.
- engine.Attribute.Type carries the tag and EnumVlaues carries opaque payload. TAE ColDef.Type and EnumValues carry the same pair. No additional field or schema-version bump is needed in these existing containers.
- attr_enum stores opaque payload; atttyp stores the native type/tag. Existing column-tuple/DDL transactions atomically publish both. No global default-format flip or startup migration occurs.
- Type.Eq remains physical equality: ENUM is still uint16. Type.String and ordinary scalar metadata spelling remain unchanged.

Required propagation owners:
| Boundary | Required closure |
| --- | --- |
| AST -> plan | build_util.getTypeFromAst uses EncodeEnumMembers and removes comma rejection; SET normalization is unchanged. |
| plan/native conversions | makeTypeByPlan2Type/Expr, MakePlan2Type/Value and engine.PlanColsToExeCols preserve/validate the tag. |
| catalog -> plan | disttae/cache/catalog.go and txn_table.go copy native tag with payload; catalog tuples and TAE schema retain existing pair serialization. |
| manual copies | DeepCopyType, view/CTE projection type capture, index-table columns, FK/update copies and new ColDef constructors copy both fields. |
| compact mirrors | tree.HivePartColType and its builders/fillers carry the encoding alongside Enumvalues. |
| metadata/tools | Native formatter reads atttyp tag; SHOW CREATE reads plan tag; checkpointtool.TableColumn obtains it from actual type bytes and never trims/sniffs encoding-1 payload. |

Audit all current Enumvalues/EnumValues/EnumVlaues references by role, including reused JSON-array/SET metadata: only T_enum uses this codec. When converting a type into ordinary numeric/bitmap metadata, clear both enum payload and tag.

## 5. Runtime and planner contracts

Keep released function IDs and overload 0 unchanged for cast_index_to_value, cast_value_to_index and cast_index_value_to_index. Their two arguments mean encoding 0 permanently. Append overload 1 with (existing domain, existing value, int64 encoding), preserving the value operand at index 1. Do not reorder overloads.

Planner emits two arguments for encoding 0 and three for encoding 1. This preserves ordinary constant/vector shapes and the existing fast path. It is format semantics/released-plan compatibility, not a mixed-version route. The same codec/lookup owners serve both overloads. Metadata needs no new overload because its native type argument already contains the tag.

A constant payload/tag is decoded at most once per batch when actually used. Reuse batch-owned enumValueIndex and its current row/label thresholds; construct it from decoded labels and use actual label count, not comma count in encoded bytes. No cross-batch/global cache. Dynamic domain/tag inputs retain row-wise null semantics and validate the pair for each required row.

Preserve Function_STRICT/null/selection behavior. Domain framing/tag errors are fatal even under INSERT IGNORE. Only invalid user label/ordinal conversion follows existing IGNORE -> ordinal 0 behavior. Ordinal 0 displays as the existing empty error member; valid empty label at ordinal 1 is distinct numerically.

Raw storage may cross projections and UNION ALL only after proving one ordered source domain. UNION DISTINCT, INTERSECT and MINUS (including counted forms) compare visible values; hidden ordinal columns must not change those keys or multiplicities. At these boundaries use the existing visible-value assignment semantics instead of promoting storage provenance.

Update every enum expression constructor: base_binder display wrappers, make.go assignment casts, mysql_special_types literal/IGNORE/order-key/AST wrappers. Shared structural unwrapping recognizes existing two-argument wrappers and generated three-argument wrappers, with validated constant tag, keeping operand index 1. Arbitrary malformed/dynamic three-argument calls must not be unwrapped to bypass domain errors.

Known encoded constants are validated at their existing error-returning bind/metadata boundaries. Return malformed-domain errors normally; do not add panic paths or let a boolean optimization helper silently accept an invalid type.

## 6. Equality, ALTER and other readers

Use distinct contracts:
- Representation identity: tag + payload; required when deciding whether metadata bytes already match.
- Full-column logical equality: same decoded ordered labels, with same-tag/payload fast path. Use for branch/schema/FK compatibility; full old empty declaration and new [''] can be equal here.
- Cast elision: require existing complete logical-domain qualification first; generic T_enum/empty metadata cannot acquire a domain through comparison. Reused ordinal expressions receive the target's complete type metadata.
- Ordinal compatibility: a decoded old-label prefix of the new domain permits any already-supported append-only/no-copy treatment. Do not add a new ALTER optimization. Reorder/delete uses the existing checked value-remap/copy path.

Thus identical text with different tags is not automatically equal, and different encodings of the same labels are not automatically incompatible. An ALTER no-op decision must not omit a requested metadata-pair update merely because data ordinals need no rewrite. Failed remapping must preserve transaction/catalog/data atomicity.

Close these actual consumers:
- external.go validation AND append paths, Hive partition conversion/mirrors, Parquet string and numeric ENUM mappers: use the tag and common reader; do not decode a constant domain per row.
- mysql_special_types order reversibility and literal/IGNORE lookup, apply_indices_ivfflat AST reconstruction, ALTER type/default readers: remove direct Split over an encoded domain.
- buildNotNullColumnVal's implicit first-member backfill should use ordinal SQL constant 1 through the existing target cast, rather than interpolating a raw comma/quote-bearing label. Prove this with real ALTER ADD NOT NULL.
- frontend branch/hashdiff: carry the tag in existing type/layout metadata, compare full domains semantically and decode varchar-to-enum values with the common owner. Preserve ordinal 0; display text alone cannot distinguish an empty error member from a declared empty label. Non-injective reverse conversions must use existing raw-ordinal binding or reject ambiguity, never guess.
- checkpointtool: copy tag from actual native type, decode encoding 1 before rendering; preserve its legacy-format reader behavior for 0. Do not route encoding 1 through TrimSpace or SQL-looking-prefix heuristics.

OUTPUT AS consumes the already-frozen spool; it must not re-query changing source tables. Its existing bounded SQL buffer preserves other types' formatting. For ENUM columns, parse that buffer once and replace only numeric ENUM cells with an internal StoredEnumValue AST carrying the complete domain and ordinal. No SQL parser production creates this node. The target-aware binder checks ordered-domain equality, the supported encoding and ordinal range (including stored zero), then emits the existing typed Literal_EnumVal. Ordinary SQL numeric zero retains strict assignment behavior. Reuse BackgroundExec.ExecStmt, the existing UserInput.stmt path, and ordinary compilation/transactions; do not introduce a second writer, public SQL function, IGNORE, or mutable process flag. The caller frees each flush AST, computation wrappers borrow it, and all values/domain strings outlive spool batch reuse. Failed materialization keeps the existing destination-table cleanup owner.
- UNIQUE/FK/index column types, view/CTE provenance and clone/restore/backup definitions must retain the pair, including hidden physical and ordering columns.

## 7. Upgrade, recovery and external boundaries

Upgrade is complete-cluster shutdown only. No peer capability, MORPC gate, new catalog column or per-feature worker is added. Old type/plan data defaults to 0; persisted released two-argument expressions remain readable. New schema/WAL/checkpoint/snapshot records use existing type/payload containers.

Old ambiguous data retains released split semantics, including the observed c=3/a=1/b=2 mapping. Original createsql is evidence, not sufficient authority to rewrite live ordinals. There is no automatic repair. Recovery requires an operator-authorized mapping/export and explicit new-table/reload or checked ALTER; strict failed conversion must not commit partial changes.

New coded domains survive restart, snapshots, database/table clone, restore and backup. DDL reconstruction emits actual quoted labels via existing SQL-mode literal owners; it never emits framing as member text. Ordinary default/NBE and legacy hash formatting stay in their current owners.

Old binaries must not open new-format data. Cross-cluster publication/subscription/CDC consumers of encoding 1 must run a capable release and preserve the pair on existing schema APIs; old consumer compatibility is not claimed. Encoding-0 external behavior is unchanged. No runtime version-probe/gate is introduced to conceal this deployment requirement.

## 8. Verification and delivery gates

| Invariant | Cheapest decisive evidence |
| --- | --- |
| Codec exactness | Literal expected framing, encode/decode properties for comma/empty/quotes/CRLF/NUL/Unicode/raw-byte cases; bounded malformed/overflow/trailing/unknown-tag tests and short fuzz. |
| Collision isolation | Same payload 2#3:a,b1:c decodes to different expected labels under tags 0/1; released collision-looking labels remain unchanged. |
| Complete propagation | AST -> plan/protobuf/deepcopy -> engine/catalog/TAE -> plan round trips, both 20-byte and raw 16-byte type layouts; no field loss. |
| Runtime behavior | Real vectors, legacy overload 0 and new overload 1, constant/dynamic/null inputs, IGNORE user errors versus fatal domain errors, existing batch-index/case/numeric policy. |
| Public replay/DML | Extend maintained ENUM metadata BVT: comma labels, new singleton empty, default/NBE/prepared switching, COLUMN_TYPE and SHOW CREATE -> CREATE -> ordinal DML/HEX. Replace temporary comma-rejection expectations with positive support. |
| Schema operations | ALTER append/reorder/format transitions/default/backfill, strict failure atomicity, view/CTE, UNIQUE/FK, branch/hash, clone/snapshot/restore; exact labels AND ordinals. |
| Import/tooling | Real small CSV/JSON and Parquet ENUM paths, Hive partition type mirrors; checkpoint dump/replay of a new coded table. |
| Persistence/distribution | Released 4.2.1/4.2.8 stopped fixtures -> new restart: old ordinary/ambiguous/collision/empty metadata unchanged; new coded writes -> restart/restore. One real remote-plan/logtail case must exercise tag and overload transport, not just two idle CNs. |
| Performance | Same-source pre/post batch benchmarks for ordinary legacy names/numeric/fold lookups at small/large domains, allocations and existing index thresholds; new coded domain decode-once benchmark. Preserve ordinary two-argument path and investigate any reproducible material regression before acceptance. |
| Quality | Fresh owning tests/static for the actual changed closure, generated-proto reproducibility, scoped race only if implementation introduces shared mutable/lifecycle behavior. |

Evidence must identify the rebased source tree. Preserve upstream CSV/frontend behavior and the PR\'s TLS/external fixes. Performance and upgrade results for an older implementation do not qualify a changed codec or writer. Keep raw failures and report measured limits.
