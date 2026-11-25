# Dictionary compatibility boundaries

Owner: #32384; series: #32379. This stage builds on #32394 and the approved
engine-wide design at adebe845cb4a77d97cf7dd0ae4b2efaf87dc224a. It enables no
production dictionary producers and makes no speedup claim.

## Contract and implementation decisions

A consumer without a declared input capability receives flat/constant vectors.
An optional producer contract declares dictionary output after its projection and
supplies the immutable allocation selection for its normalization destination.
An optional consumer contract admits dictionaries per child edge. Unknown input
and external root destinations default to flat. During runtime preparation, after
operator decoding/duplication, configure a boundary only for a declared producer
whose destination does not admit dictionaries. No adapter node or transport
opcode is introduced, so operator identity, terminal-sender and leaf-reader
checks retain their current meaning. All-flat trees allocate no boundary.

The producer owns at most one normalization buffer in DictionaryOutputState.
Its Call invokes BeginCall to retire the previous borrowed output. Its
ExecProjection invokes Normalize after projection and before publication. Flat batches pass through without
column copying. The copy owns private vector headers and storage, copies logical
rows once, preserves constants and all vector metadata, and preserves batch
attributes, row count, recursive/shuffle markers and ExtraBuf. The source batch
and its expression-owned vectors are borrowed and never cleaned or mutated.
The protobuf boundary uses one physical value for constants (zero for NULL),
while retaining their logical length; this prevents slicing a scalar buffer by
its broadcast row count.
The boundary uses the current process pool and its declared destination account;
allocation failure publishes nothing and releases partial copies.

Normal cleanup delegates through vm.Reset/vm.Free: Reset retires the boundary
after the operator stops, and Free retires it before the operator can retire its
allocation account. Pipeline sender/receiver/loop cleanup, prepared generations,
compiler disposal and dependent native kernels use the same helpers. Repeated
cleanup is harmless. Preparation discards any previous execution buffer and
reconfigures capabilities; Call errors, EOF and partial preparation use the same owning cleanup.
Cancellation before Call retires output through terminal Reset/Free. No goroutine, wait, registry or cache is
added. The live bound is one logical batch per active incompatible producer edge,
plus the source backing until normal upstream release. Ordinary allocation
admission applies to the full copy peak.

## Consumer matrix and compatibility

| Destination | Current contract | Boundary |
| --- | --- | --- |
| expressions, filters, key/hash/aggregate/copy kernels | unknown/flat | producer edge before Call; migration is #32385 |
| order, windows, remaining joins, DML, external/native operators | unknown/flat | producer edge before Call |
| spill writers inside an unconverted operator | flat operator input | same edge; raw codecs continue rejecting dictionaries |
| frontend/output callbacks | unknown/flat | same edge before Output.Call |
| connector/dispatch and local/remote spool | unknown/flat | same edge before sending or serialization |
| decoded remote pipelines | same runtime capability setup | normal preparation after decode; no encoded adapter |
| direct vector/batch serializer APIs | physical flat only | caller uses explicit FlatCopy; raw rejection remains |
| pipeline root without an external capability | flat | root output boundary |
| converted operator accepting a specific child | declared dictionary | no normalization on that edge |

This is a closed default policy, not a claim that every physical accessor has
already been migrated. Native/function boundaries inside converted operators
must be handled in #32385 before those operators may declare support. Consumers
must not infer write permission from read capability. All future producers must
use canonical runtime preparation (including dependent native setup).

Wire/spill/protobuf remain unchanged flat formats; accepted legacy class-2 row buffers decode as FLAT. No SQL/catalog/storage migration is introduced. Disabling producers
restores ordinary execution. FlatCopy is explicit and transactional, never hidden
inside physical accessors or serializers.

An operator wrapper was rejected: executable OpType/type assertions, compiler
duplication and special cleanup rely on operator identity. In-place flattening
was rejected because batches and expression result headers are borrowed. Eager
normalization of every edge was rejected because it taxes all-flat pipelines.
The optional capability plus producer-owned output preserves these contracts
while ordinary OperatorBase layout and Exec remain identical to main.

## Delivery and validation

Draft development may stack on prerequisite branches before merge. Draft PRs
identify their parent and incremental diff. Production readiness, producer
activation and performance qualification remain isolated against actual main
after prerequisites merge; percentages are never added across PRs. This refines
the earlier merge-first development schedule without relaxing acceptance.

Small deterministic UTs cover repeated/reordered/NULL/string/constant logical
rows, mixed batches, borrowed-input preservation, wire round trips and unchanged
raw rejection, explicit accounts and failure cleanup. Runtime tests cover unknown
versus capable consumers, root normalization, flat pass-through, projection,
next-call retirement, reset/free/cancel/error and repeated prepare. Existing
pipeline cleanup and a real Output consumer close lifecycle/interface behavior.
No production SQL route can produce a dictionary at this stage, so dictionary
SQL BVT belongs to producer enablement; existing owning package tests cover the
unchanged flat route. Shared immutable core race evidence is reusable; boundary
state is one execution lane, with existing asynchronous-owner cleanup tests.
The neutral SF10/SF100 all-query gate remains required before leaving draft.

## Approved producer-owned revision

The requester approved this revision after SF10 disk Total regressed1.48%.
Restore main OperatorBase layout and vm.Exec exactly. Only dictionary producers
embed DictionaryOutputState, retire prior output at Call entry, and normalize
the final projected batch before publication. Configure direct producer edges
within preparation rather than a second whole-tree walk. Unknown consumers and
external roots require flat output; aware edges retain dictionary output.

State owns at most one accounted FlatCopy and is private to an execution lane.
Reset cleans after stopped work; Free cleans before account disposal. A cancelled
Exec that never calls the producer retires its prior result during terminal
cleanup. Preparation clears old configuration. No registry, adapter operator,
cache or opcode is added. No production producers are enabled in this stage.
Legacy class-2 row buffers retain the merged core FLAT decoding contract.

Validate concrete callbacks, projection, EOF/error/cancel, reset/free/reuse and
allocation failure. Verify Base layout and Exec source match main, then perform
fresh isolated all22/Total SF10/SF100 neutral acceptance with unchanged gates.
