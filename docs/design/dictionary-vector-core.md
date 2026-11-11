# Dictionary vector foundation

Owner: #32383; series: #32379. This implements the representation/ownership
stage of the [approved design at adebe845cb4](https://github.com/matrixorigin/matrixone/blob/adebe845cb4a77d97cf7dd0ae4b2efaf87dc224a/docs/design/20261109-dictionary-vector-execution.md).
It enables no production dictionary producers. Consumer capability, transport
normalization, expression migration and producer rollout are later stages.

`DICTIONARY` uses the existing reserved in-memory class value `2`. It owns an immutable
`DictionaryBase` and a validated `RowSelection`; both use the existing buffer
lease implementation. Freeze one base per input vector and share it among
partitions. Every successful handle `Retain` needs one `Release`; every returned
vector header belongs to one consumer and is freed normally with `Vector.Free`.

`FreezeVector` requires exclusive access to the source header and any mutable
physical slices handed out from it. It snapshots mutable row metadata and
promotes uniquely owned payload into leases without copying it. Ordinary source
reuse/mutation uses the existing COW methods. Legacy aliases and incompatible
accounts use an explicit accounted copy. Immutable external leases reserve
their full backing size through the existing MPool capacity lease; pin refusal
falls back to an accounted copy. A private base owns that pin until its last
dictionary reference is released. There is no cache or global pin registry.

Selections copy and validate incoming int64 indexes once, preserving duplicates
and order. Composition owns one accounted mapping to the original base, with no
dictionary chain. Empty output keeps no base or selection reference. NULL and
grouping bitmaps, string source/domain and prepared-value metadata live in the
private logical header; a constant payload's physical index zero is distinct
from its logical grouping coordinates.

Resolve `FixedReadView` or `VarlenaReadView` once per input batch. Their slices
are immutable and remain valid only while the input header is live. `AllNull`
must be checked before reading a constant NULL's empty physical value slice.
Public logical value getters resolve the mapping. Physical slice/pointer/area
accessors reject dictionaries and never flatten them implicitly.

`EnsureFlat` / `EnsureWritable` gather only selected rows and referenced string
payload transactionally under the destination pool/account. No new state is
published on allocation failure. Existing in-place shrink/shuffle APIs perform
COW before taking metadata aliases. Low-level physical mutation requires an
explicit writable materialization. Older window APIs return an error for a
dictionary; use an accounted logical selection or materialize explicitly.

`Size` estimates logical payload. `Allocated`, `BorrowedAccountedBytes` and
`DictionaryRetainedBytes` conservatively include the full retained backing;
shared references may therefore repeat a size estimate, while physical charges
and their final release belong to the backing owner. No selected-row fraction
relabels a physical allocation.

Current and legacy serializers reject dictionary headers before emitting bytes;
callers materialize logical rows before using those physical codecs. Existing
row-buffer encodings with legacy class byte 2 remain readable and decode as FLAT,
because those formats contain per-row values rather than a base and selection.
Whole-column, copied, streaming, V1 and object-window readers use the same physical
interpretation without rewriting shared input bytes. Existing class, bounds and
payload validation is retained. This stage adds no transport opcode or SQL type.

Validation covers flat/constant sources, selection composition/order/repetition,
NULL/grouping, inline/area strings and provenance, source reuse, legacy alias
copying, full external pin admission/fallback, failed COW, shared-owner races and
serialization rejection. SQL/BVT producers remain unreachable in this stage;
existing batch/Arrow consumers stay on flat/constant data. Performance acceptance
is the series' neutral foundation gate over Q1–Q22 plus Total, rather than a
producer speedup claim. Results belong in appended PR/issue comments.

## Lease-backed descriptor revision

Owner: #32383 / #32394. The requester approved the implementation plan after
choosing deterministic header savings plus the original neutral TPC-H gate;
a confirmed Q6 speedup is diagnostic rather than an additional adoption rule.
The rejected field-tail experiment remains separate.

Remove Vector.dictionary and preserve every ordinary Vector field offset.
For DICTIONARY only, the existing dataLease interface owns dictionaryVector, an
immutable descriptor with its own atomic reference count, base, selection and
logical-area summary. It implements the existing BufferLease interface; Bytes
returns nil because the dictionary has no contiguous physical payload.
AccountedBytes returns the checked immutable sum of base/selection retained
bounds. Ordinary data/area leases retain their existing contracts. No public
API, encoded class, schema, cache, registry or additional Vector field is added.

Construction retains the base and mapping only after metadata admission; the
private descriptor starts with one reference before publication. Empty selections
remain ordinary vectors without a descriptor. Retain fails after zero or before
counter overflow, even if callers separately keep the children alive. The unique
1-to-0 Release drops mapping and base exactly once; over-release is an invariant
failure. Descriptor members are immutable, so readers and accounting do not race
with final release. Physical payload charges stay with the original owner.

Free/reset clear dataLease and the dictionary class before releasing the
composite owner, so generic backing cleanup cannot release it again.
EnsureFlat/EnsureWritable publish their replacement only after selected payload
and metadata allocations succeed; the old descriptor is then retired. Failure
preserves the original representation. Physical accessors and old byte-window
or rebinding APIs continue rejecting DICTIONARY. Read views resolve the descriptor once.
BorrowedAccountedBytes must not add the dictionary bounds again after the data
lease already contributes them; Allocated retains its existing full-pin estimate
plus owned metadata.

Validation adds final-release, retained-holder, non-resurrection, overflow and
construction/COW failure cases using tiny existing fixtures; exercise descriptor
sharing under the race detector and real batch/Arrow/cleanup consumers. Preserve
the checked-empty regression test. Measure reflection layout against main and
forced heap header allocation: on the pinned amd64 Go1.27.1 profile, restore
480 bytes and the480-byte class instead of488 rounded to512. Run isolated all22
SF10/SF100 five-pair memory/disk comparisons, cold full suites excluded, unchanged
Holm/separate-Total/no-spill/swap/OOM rules, and the approved process CPU guard.
No dataset/result verification or numerical retries. Publish new source-pinned
results before collapsing superseded comments. Leave current failures visible
until a replacement is actually qualified.
