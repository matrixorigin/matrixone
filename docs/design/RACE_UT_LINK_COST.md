# Reduce temporary race-test binary link cost

Related issue: #28419. Base: `049dad215c`.

## Global cost review and scope

The successful #29025 run [35170421812 / 105040731211](https://github.com/matrixorigin/matrixone/actions/runs/35170421812/job/105040731211)
spent about 47m28s in the race runner: light 12m54s, HNSW 39s,
issues 10m18s, embedded approximately 16m35s, then engine and heavy/plan.
Those are stage wall times, not pure test-body times. Admission totals overlap
the holder's work and must not be added to the stage critical path.

The review found an avoidable difference between the ordinary and sharded
test build paths. Go 1.26.4 `cmd/go/internal/test/test.go` sets
`pmain.Internal.OmitDebug = !testC && !testNeedBinary()`. Ordinary `go test`
already omits DWARF; our `go test -c` paths retain it, despite immediately
executing and then deleting those binaries. Building debugger metadata is
not part of the race-testing contract.

This is focused build maintenance, not a new scheduler or lifecycle model.
It does **not** resolve the complete global-runtime problem or establish a
whole-CI speedup. In particular, it does not remove the light-stage compile
cost, shorten SQL test bodies, or eliminate necessary cluster admission.

## Change and invariants

Pass `-ldflags=-w` only when compiling the engine, plan and optional embedded
prebuild test binaries. The [Go linker flag](https://pkg.go.dev/cmd/link#hdr-Command_Line)
omits DWARF; unlike `-s`, it does not request stripping the symbol table.

- Preserve `-race`, `matrixone_test`, `-short`, complete dynamic test discovery,
  every test assertion, process isolation and failure propagation.
- Preserve one runner, all stage/package/shard concurrency budgets, timeouts,
  cancellation, report ownership and native-library selection.
- Keep embedded prebuild disabled by default. Its outputs remain compile-only;
  the authoritative `go test` still executes the tests.
- Do not modify production binaries, normal local builds, SCA or coverage CI.
- Do not export linker options through `GOFLAGS` to unrelated subprocesses.

The tradeoff is intentional: these temporary CI executables no longer carry
DWARF for Delve/GDB variable inspection. Rebuild locally without this flag for
debugger use. Go stack traces and race reports use runtime function/line
metadata; they are independently checked below. This does not promise that
every external debugger or C-frame symbolizer is unaffected.

## Evidence

Local host: darwin/arm64, Go 1.26.4. Native artifacts built from matching inputs
using the controlled CGo wrapper. Both variants use identical native flags,
`-race -tags matrixone_test`, warm package compilation caches, and distinct
build IDs to force linking. Commands run sequentially; order reverses in the
second pair. Initial cache-warming runs are excluded.

| Pair | Default `go test -c` | With `-w` |
| --- | ---: | ---: |
| default, then candidate | 44.68s | 17.44s |
| candidate, then default | 31.18s | 16.89s |
| Total | 75.86s | 34.33s |

This is a **54.7% reduction in the measured local build-command time**, not
54.7% of CI. Plan binary size decreases from approximately 232 MiB to 190 MiB.
Peak RSS is not consistently lower, so no memory-reduction claim is made.
Linux/amd64 CI uses a different native linker and resource budget; neither
these percentages nor absolute seconds may be projected directly onto it.

The candidate plan race binary passed all 3,020 top-level tests with no top-level
skips. A separate intentional-race probe linked with `-w` failed as required,
printing the function name, source file and line; a runtime stack-frame probe
also retained function/file/line metadata. The probe is local evidence, not a
permanent intentionally failing test added to CI.

The engine candidate race binary also completed its full suite: 160 top-level
passes and three existing skips. The default and candidate plan binaries list
the same 3,020 top-level test names.

Scheduler tests inspect the real engine/plan compile commands, require the
race/tag/module/parallelism policy and propagate an injected build failure.
Existing embedded-prebuild success, failure, cancellation and report tests also
require the flag. No extra MO compilation is introduced into those UTs.

The first full optools race run, concurrent with the engine binary build,
timed out in the 15-second cancellation-diagnostics harness. Its isolated
single-test rerun passed in 1.350s. This is retained as a validation caveat, not
silently called a clean first pass or used to justify increasing a timeout.
After the engine run completed, the full optools race suite passed in 53.160s.
Incremental golangci-lint reported zero issues; optools vet, shell syntax,
gofmt and diff checks also passed.

## Delivery and remaining work

Compare engine/plan build checkpoints on the next normal CI run; do not launch
extra runners or wait for a performance claim before reporting this local
evidence. A passed CI alone does not prove overall latency improvement.
Rollback is removal of the compile-only flag; there is no persistent state,
cache format or production compatibility change.

Larger end-to-end gains still require separating compiler/linker work from test
execution in the light stage, and profiling the long cluster holders. The
existing directory-nonempty cache-seed check is not evidence of a useful race
cache hit. Changing that infrastructure needs its own bounded cache/runner
evidence; this PR does not silently enable the previously regressing overlap
strategy or four-runner sharding.
