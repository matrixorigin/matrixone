# Dictionary vector foundation

Owner: #32383; series: #32379. This implements the representation/ownership
stage of the [approved design at adebe845cb4](https://github.com/matrixorigin/matrixone/blob/adebe845cb4a77d97cf7dd0ae4b2efaf87dc224a/docs/design/20261109-dictionary-vector-execution.md).
It enables no production dictionary producers. Consumer capability, transport
normalization, expression migration and producer rollout are later stages.

`DIST` is the existing reserved in-memory class. It now owns an immutable
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
decoders reject the reserved class on input. Explicitly materialized flat output
uses the existing format. This stage adds no transport opcode or SQL type.

Validation covers flat/constant sources, selection composition/order/repetition,
NULL/grouping, inline/area strings and provenance, source reuse, legacy alias
copying, full external pin admission/fallback, failed COW, shared-owner races and
serialization rejection. SQL/BVT producers remain unreachable in this stage;
existing batch/Arrow consumers stay on flat/constant data. Performance acceptance
is the series' neutral foundation gate over Q1–Q22 plus Total, rather than a
producer speedup claim. Results belong in appended PR/issue comments.
