# Embedded Sirius development and execution

Embedded Sirius runs in `mo-service` using normal MO readers, a native C ABI
and bounded result batches. It does not need `mo-sirius-sidecar`, Flight
certificates, a manifest resolver, disabled TN GC or direct-TAE protection.
It is opt-in. Ordinary CPU builds and unhinted SQL remain unchanged.

## Build

Initialize the pinned dependencies and build the SDK with Sirius's frozen MO
Pixi profile:

```sh
git submodule update --init --recursive
git -C third_party/sirius fetch origin upstream-dev-merge
cd third_party/sirius
pixi run --frozen -e mo mo-build-embedding-sdk
cd ../..
pixi run --frozen --manifest-path third_party/sirius/pixi.toml -e mo \
  make -j8 MO_SIRIUS=1 MO_CL_CUDA=1 \
  SIRIUS_SDK="$PWD/third_party/sirius/build/mo/extension/sirius/embedding-sdk" \
  SIRIUS_MERGED_REF=origin/upstream-dev-merge
```

The release build verifies that the pinned Sirius revision is contained in
`SIRIUS_MERGED_REF`; this ref is resolved inside the Sirius submodule, not MO.

`MO_CL_CUDA=1` also enables MO's GPU vector operations. Omit it for a
Sirius-enabled build without those operators. Combined builds must use the
same activated Pixi prefix. Do not supply system CUDA paths. The host NVIDIA
driver and access to the GPU remain runtime requirements. Routine local
verification needs no Docker image build.

## Configure and query

Add this section to the CN configuration. `native-config-path` names the
Sirius YAML configuration; its GPU and pinned-host budgets must leave enough
room for every admitted input window and the result window.

```toml
[cn.sirius]
enabled = true
backend = "embedded"
input-mode = "mo"
native-config-path = "/absolute/path/to/sirius.yaml"
gpu-streams = 2
max-waiting-queries = 16
```

Submit a SELECT through MO's normal MySQL endpoint:

```sql
/*+ SIDECAR GPU */ SELECT n, s FROM example WHERE n > 0 ORDER BY n;
```

The existing statement hint selects Sirius; it does not imply a sidecar process
when the CN backend is `embedded`. Unsupported plans return an eligibility
error instead of running on CPU or Flight. Wide exact-decimal support is tracked
by [#28968](https://github.com/matrixorigin/matrixone/issues/28968); this reader
integration is not an all-22 TPC-H readiness claim. Prepared-query offload,
direct TAE, multi-CN producers and concurrent GPU queries are not enabled.

## Ownership and evidence

One GPU query executes at a time; at most sixteen competing requests wait
without readers. MO scan workers feed the existing bounded pipeline into a
synchronous publisher. Native credit is acquired before outgoing allocation.
Sirius coalesces batches on demand rather than buffering a complete table.
Input windows are 64 MiB per binding, and the result window is 64 MiB. Original
MO scan batches remain governed by the normal MO pipeline's bounds.

Result callbacks borrow a bounded Go copy until the callback returns. No query
result is collected by the adapter. Cancellation stops producers, joins their
pipelines and native work, then releases ownership. Fatal GPU failures seal the
runtime and can require restarting MO.

The terminal `Sirius embedded execution` log record contains the statement ID,
backend/input mode, stream count, source rows/bytes, first-row and wall latency,
terminal health and native execution statistics. Charged-byte statistics are
admission accounting, not physical process/GPU memory measurements. The record
does not contain SQL, credentials, row contents or object paths.

## Verify locally

Set `MO_SIRIUS_TEST_CONFIG` to a valid, bounded Sirius YAML configuration and
run the native tests through the same Pixi environment used for the build:

```sh
pixi run --frozen --manifest-path third_party/sirius/pixi.toml -e mo \
  env MO_SIRIUS=1 MO_CL_CUDA=1 \
  SIRIUS_SDK="$PWD/third_party/sirius/build/mo/extension/sirius/embedding-sdk" \
  MO_SIRIUS_TEST_CONFIG="/absolute/path/to/sirius.yaml" \
  .agents/skills/mo-dev/scripts/mo-cgo-test \
  -tags sirius_integration -count=1 -timeout=180s \
  -run '^(TestNativeBridgeDataAndCancellation|TestEmbeddedSiriusPublicMOReader)$' \
  ./pkg/sql/compile/siriusbridge ./pkg/tests/sqlintegration
```

The public test exercises MySQL results, NULLs, variable-width strings, filtering,
ordering/fetch, committed deletes and transaction-local writes at two streams.
The bridge test verifies native GPU work, cancellation and zero retained
input/result charges after cleanup. CPU-runnable tests separately prove that
blocked publication stops the table scan and that failed transfers release
their lease.
