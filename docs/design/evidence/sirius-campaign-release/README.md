# Release campaign evidence

The combined local acceptance validator passed on 2026-10-11. All 744 required
unprofiled timing executions and all 264 separate resource executions passed
exact schema, value, duplicate and ordering comparison for canonical Q1–Q22 at
SF1/SF10, with streams 1/2/4. Embedded execution completed GPU work without
fallback, with healthy terminal status and zero retained input/result credit.
Publication followed joined cleanup of the campaign's owned processes.

`acceptance.json` records `local_acceptance_complete: true` and
`acceptance_complete: false`. Required CI and reviewer approval remain external
gates. The unprovisioned Flight profiles remain N/A. This evidence does not change
the opt-in backend default or deliver E/F or Docker changes.

Both complete campaigns use the same immutable `provenance.json`: MatrixOne
`9814347c1ce38ec12f51412ef3176ede1501574d`, merged Sirius
`955b6881afa6e92d0995f43e3fe9c383d86981c0`, guarded release SDK
`29bca11a537abf00d481581a423023591dbabda9b0fdd73d06c4a697e8ce01ce`,
and the actual rebuilt combined Sirius/cuVS service and public runner hashes.
Dataset, SQL, hardware, frozen toolchain, controller and resource-limit identities
are recorded. Builds, other GPU work and dataset/source mutations did not overlap
measurements. Later delivery commits preserve these capture identities.

## Results and source scope

| Scale | Native MO median suite | Embedded GPU median suite |
| --- | ---: | ---: |
| SF1 | 1.804130 s | 4.789524 s |
| SF10 | 27.947777 s | 52.826967 s |

The GPU route was slower overall on this RTX 3070. Per-query medians, five full
suites, ten separate Q9 repetitions and labeled excluded controls/warmups remain
in `timing/`. Timing executions were unprofiled; resource timings are separate.

All six resource profiles (SF1/SF10 × streams 1/4/2) have zero unexplained warmed
allocation growth and reconciled logical pool baselines. `allocations.json`
contains the actual process-lifetime CUDA allocation/free results, including 45
ordered GPU spans per profile. Each baseline is crosslinked to the actual first
measured span and final excluded cleanup probe. QueryEnd pool observations remain
distinct from successor-window proof after engine close, physical allocated bytes,
residency and lifetime high-water marks. `resource/` retains all required query
records plus each phase's excluded warmups and probes. Raw profiler databases,
result rows, headings, SQL, credentials and environment snapshots are not published.

`lifecycle.json` maps ten cases to actual source, binary, execution-log hashes and
barriers: paused consumption, blocked input/result credit, cancellation before
admission/during work, queue saturation, writer failure, numeric error ownership,
healthy reuse and fatal quarantine. Fatal zero-credit closure is scoped to joined
test-owned process death; unsafe owners remain retained while that process lives.

`sanitizer.json` preserves four raw exit statuses of 99. The full GPU memory check
passed 34,509 assertions in 30 cases with one CUB EmptyKernel registration
diagnostic, matched by Native A's unchanged input path (30 assertions, one case).
The production ABI leak check passed 1,964 assertions in ten cases with one
41,386,248-byte allocation, matched by the initialization control (ten assertions,
one case) before GPU work. Ownership is the process-global cuDF default pinned
resource. Raw error/leak counts and hashes, exact controls, source/header hashes
and all six current zero-growth allocation profiles support that reconciliation.
No raw status is changed, suppression added or extra finding accepted.

`numeric-baselines.json` and the nine raw CSVs under `numeric/` retain fresh,
serial Native A/B/current executions with matched 262,144-element operands and
21 measured samples per case. Three rotations alternate A/B/current order; each
invocation uses three seconds of warmup and two excluded samples. All ten ratios
pass the 1.1 limit; the maximum is 1.026527 and checked-add64 is 0.843248. Actual
historical sources, binary/harness hashes and shared operand digest remain intact.

The lifecycle, sanitizer and numeric runs preceded the final collector fix and
qualified-name binder merge. Their reports explicitly retain the original
`execution_context`, actual proof identities and `reuse_basis`. Native source
`955b6881`, hardware, toolchain and operands were unchanged. The writer source
was unchanged; normal/race ownership tests remain valid. Fresh planner checks and
all 22 native preparations cover the changed binder. This qualification does not
relabel an old measurement as execution at the final MO source.

The earlier release timing attempt failed after 44 validated cells because the
workspace quota monitor raced with normal comparator-file retirement. The fix
stats once and tolerates only `FileNotFoundError`; permission/quota errors remain
fatal. The failed attempt remains separate. Both published campaigns restarted
in full at the recorded final source; no partial or diagnostic capture contributes.
Historical development evidence retains its original labels and contexts.

The subsequent authoritative main merge includes #29893's SHOW CREATE and
COLUMN_TYPE schema rendering plus CDC compatibility. `delivery-qualification.json`
records the exact source comparison and passing affected type/function/planner/
compiler/frontend suites. Canonical SELECT expressions, physical OID descriptors,
MySQL result packets, compiler readers, native bridge and campaign collectors are
unchanged. The capture is reused with that explicit source scope; its original
revision and binary hashes are preserved.

## Reproduce the artifact acceptance check

From the repository root, run:

```sh
python3 optools/sirius_acceptance.py \
  --timing docs/design/evidence/sirius-campaign-release/timing \
  --resource docs/design/evidence/sirius-campaign-release/resource \
  --allocations docs/design/evidence/sirius-campaign-release/allocations.json \
  --lifecycle docs/design/evidence/sirius-campaign-release/lifecycle.json \
  --sanitizer docs/design/evidence/sirius-campaign-release/sanitizer.json \
  --baselines docs/design/evidence/sirius-campaign-release/numeric-baselines.json
```

`checksums.json` binds every generated evidence file. The artifact check validates
captured records and proof relationships; it does not rerun GPU executions or
infer GitHub CI/reviewer approval. Raw source-owned test log hashes remain in the
reports; private raw logs and traces are retained outside the repository.
