# Incremental SCA contract

Owner: [CI/test cleanup #29249](https://github.com/matrixorigin/matrixone/issues/29249).
Implementation: `Makefile`'s `static-check-analysis` target and
`static-analysis.cjs`; coarse job scheduling stays in `change-scope.cjs`.

## Selection and failure behavior

PR CI compares immutable event base/head trees, after checking the checkout and
working tree. A shallow checkout may fetch the exact base object once, bounded
by 30 seconds. Missing history, fetch failure, unsupported events, dirty source,
or uncertain paths retain full analysis. Renames are additions plus deletions.
Go-reported source files must also be tracked regular files: Git ignores do not
prevent Go from compiling a file. Generated source and source symlinks retain full
analysis. No mutable branch tip, persisted plan, or changed-line filter is used.

Go supplies the complete production/test package graph under the same CGo and
read-only module environment used by the analyzers. A production Go edit selects
its whole owning package and transitive reverse importers, including test-only
importers. A test-only edit selects the whole owning package, whose tests both
analyzers already include. Removing a whole package or reaching half the graph
selects `./...`. Native, dependency, build, generator, fixture, and analyzer
configuration changes also retain the full scan.

Both pinned analyzers receive the identical selection. Graph verification or an
analyzer failure fails the target. An empty inventory/selection cannot pass.
`err-check` still runs over the complete source tree. Platform flags, concurrency
settings, and coverage instrumentation are unchanged by package selection.

`molint check` loads the selected packages and their dependencies through Go's
package graph. It checks authored imports and their module owners against that
fresh graph, then runs typed checks through Go vet. A module or analyzer change
retains the full scan. The configured golangci analyzers use package/import
analysis; Go and golangci continue to resolve dependency facts. Changes to
analyzer versions or rules require reviewing this package-local contract.

## Full checks and rollout

- `make static-check` remains the complete local/release gate.
- `make static-check-analysis` remains full outside GitHub Actions. CI without
  Node on PATH also runs complete graph validation and both analyzers, with an
  explicit fallback reason. This supports older CI during rollout.
- The SCA job can expose its existing runner-provided Node 24 runtime on PATH
  without downloading another SDK, enabling incremental analysis. Once Node is
  available, selector errors remain fatal.
- Selection and conservative fallback reasons are printed on stderr; stdout
  from the selector contains only package arguments.
- `SCA_FULL=1 make static-check-analysis` forces complete PR analysis.
- `make static-check-golangci` still populates the complete trusted analysis cache.
- License exemptions require the frozen checkout and PR head/base to match
  before and after file pagination, plus the validated configuration/tool recipe.
  Unavailable or changed inputs retain complete checks. The final required-check
  gate remains unchanged.

The first optimization keeps complete complementary BVT groups. General SQL
selection is deliberately deferred: existing logging verifiers consume other
cases' statements, and a directory alone is not a dependency/isolation proof.
The fulltext freshness case instead uses the existing periodic-sweep timing
control, checks successful fan-out, and restores the default before proceeding.
Its warm-cache, durable CDC, and exact-result oracles remain intact; no explicit
cache eviction substitutes for freshness.

## Validation boundaries

`node --test .github/ci/*.test.cjs` includes a real Go module/import graph and
executes the actual Make recipes with recording analyzers to prove argument and
failure propagation. Pinned real analyzers, full-scan fallback, and narrow package
closures must also be exercised before delivery. The freshness case requires
normal mo-tester comparison on a test-owned multi-CN cluster, a same-instance
rerun, and teardown checks. Report local and CI timings separately, including
cache state and planning overhead; selected-package counts alone are not a
wall-time measurement.
