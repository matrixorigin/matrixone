# #23684 shared Arrow substrate

This note records the reusable Arrow boundary introduced while implementing
Arrow IPC LOAD. It is intentionally narrower than either the static-file LOAD
protocol or the Python UDF/Flight design.

## Package boundaries

| Package | Owns | Must not own |
| --- | --- | --- |
| `pkg/container/arrowipc` | bounded IPC metadata framing, FlatBuffers graph validation, schema vector/depth/string limits, record node/buffer ranges, compression declarations, decoded-size limits | object listing, FileService identity, Flight invocation sequence, SQL type policy, authorization |
| `pkg/container/arrowbridge` | Arrow ArrayData lifetime retention, borrowed MO vector construction, transactional materialization, COW-compatible ownership, LOAD binding and conversion kernels, copy/pin statistics | file/range acquisition, Flight tokens, transaction completion, credentials, Python SDK metadata |
| `pkg/sql/colexec/external/arrowio` | IPC File/Stream discovery, footer and block planning, dictionary replay, FileService range leases, object identity | MO target-column binding, SQL write semantics, Flight protocol |
| `pkg/sql/compile/sidecarflight` | Sirius capability negotiation, metadata-version and exact result schema checks, Flight message sequencing, Sirius result conversion | generic IPC structural limits, static-file planning, Python UDF semantics |

`arrowipc` is the first trust pass for both file and Flight messages. A
consumer must still validate its envelope and protocol state after that pass.
In particular, structural success says nothing about an expected message kind,
metadata version, row cardinality, invocation epoch, sequence number, or SQL
type identity.

`arrowbridge.BindLoad` is explicitly an ingestion policy. LOAD permits checked
integer/float widening and selected temporal conversion. The Python UDF v1 ABI
requires an exact, versioned logical type descriptor, including timezone and
logical metadata. A future UDF binder must validate that descriptor before it
constructs a conversion plan; it must not call `BindLoad` as an ABI shortcut.

## Ownership contract

1. A transport validates framing and structure before Arrow-Go or a local
   decoder allocates from untrusted lengths.
2. The transport publishes immutable Arrow buffers with one physical lifetime
   root. File IPC attaches its `RangeLease` below the Arrow object graph.
3. `arrowbridge` retains `arrow.ArrayData`, never a FileService implementation.
   That retain transitively keeps the range and its statement capacity charge
   alive.
4. A borrowed MO vector owns exactly one retained backing per data, area, or
   validity component. Cleanup releases those owners exactly once.
5. Mutation and owned-only handoff materialize transactionally under the
   caller's allocation account. Failure leaves the borrowed source readable.

## Reuse status

The shared IPC validator is used by Arrow File/Stream LOAD and the existing
Sirius `sidecarflight` decoder. The container bridge is used by LOAD. This is
shared infrastructure, not a Python UDF implementation: the Function Catalog,
UDF operator, MO-to-Arrow encoder, exact SDK v1 binder, Flight invocation
envelope, trusted Supervisor, TOCTOU-safe output publication, and sandbox
runtime remain separate work.

## Local evidence

The final worktree was validated with the following owning-package and real
consumer tests:

```text
GOWORK=off go test -mod=readonly -count=1 ./pkg/container/arrowipc
GOWORK=off go test -mod=readonly -race -count=1 -timeout=180s \
  ./pkg/container/arrowipc
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=180s \
  ./pkg/container/arrowbridge
.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=1 -timeout=240s \
  ./pkg/container/arrowbridge
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=180s \
  ./pkg/sql/compile/sidecarflight
.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=1 -timeout=240s \
  ./pkg/sql/compile/sidecarflight
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=240s \
  ./pkg/sql/colexec/external/arrowio
.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=1 -timeout=300s \
  ./pkg/sql/colexec/external/arrowio
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=120s -run='^$' \
  -fuzz='^FuzzArrowIPCPlanningAndOpenNeverPanicOrLeak$' -fuzztime=20s \
  ./pkg/sql/colexec/external/arrowio
```

All completed successfully. The final fuzz run loaded 47 baseline inputs and
executed about 372,000 mutations. The full `pkg/sql/colexec/external` consumer
suite, including its local MinIO object-change path, and the public MySQL
protocol suite in `pkg/tests/arrowload` also passed. The formal distributed case
`test/distributed/cases/load_data/load_data_arrow.sql` passed 50/50 twice on one
clean instance built from the exact worktree.

The self-review added deterministic regressions for duplicate output indices
when attribute names are empty, metadata limits that cannot be raised by a
consumer, and cleanup of unpublished vectors when statement admission rejects
their first backing allocation.
