# Native Artifact Generation and Worktree Reuse

- Status: implementation updated; independent design approval required
- Revision: 1
- Owner: PR [#27852](https://github.com/matrixorigin/matrixone/pull/27852)
- Review input: [review 5060075176](https://github.com/matrixorigin/matrixone/pull/27852#pullrequestreview-5060075176)
- Scope: developer/CI native builds and `mo-cgo-test` artifact reuse

## 1. Problem

MatrixOne's CGo tests need `libmo` plus `thirdparties/install`. Those outputs
are intentionally ignored by Git, so a linked worktree does not inherit a
primary worktree's expensive native build. Reuse is useful, but only if the
artifact generation exactly matches the worktree's native source, host, and
semantic build options.

The first implementation tracked source and a composite CPU/GPU/debug variant,
but left four independent truths:

1. Make targets decided who built thirdparties.
2. Environment variables decided some thirdparty and GPU semantics.
3. Shell loops published runtime libraries.
4. The provenance stamp decided whether another worktree could reuse them.

That split admitted concrete failures:

- a failed `cp`, `ln`, or `mv` could be hidden by a later loop iteration and
  return success with an incomplete runtime library set;
- `make -j2 thirdparties cgo` gave two different targets ownership of the same
  thirdparty build and staging paths;
- `MO_CL_SIMSIMD=1` changed usearch but aliased the ordinary CPU provenance;
- a stamped GPU debug build delegated optimized `-O3` sub-builds;
- the deterministic state and wrapper tests were not selected by the native
  CI path filter.

The design does not attempt a remote artifact cache, cross-host binary
portability, or concurrent builds from independent shell processes. Those are
different problems and are not required for safe linked-worktree reuse.

## 2. First-principles invariants

### I1 — one generation has one structured key

The reusable key is the tuple:

```text
tracked Makefile tree
tracked cgo tree
tracked thirdparties tree
host GOOS
host GOARCH
accelerator = cpu | gpu
optimization = release | debug
simsimd = 0 | 1
common native environment profile hash
accelerator-specific environment profile hash
```

The fields are stored independently. A future semantic native input must add a
new field and a transition test; it must not be folded into an undocumented
environment string.

SIMSIMD differs from accelerator and optimization because it changes a
thirdparty output. A SIMSIMD transition therefore requires `rebuild-all`;
accelerator or optimization changes require at least `rebuild-cgo`.

The environment profiles hash supported compiler/build overrides that can
change native bytes. The common profile includes `CC`, `CXX`, `AR`, C/C++/link
flags, MUSL, usearch/croaring/jemalloc overrides, and macOS SDK/deployment
selection. The accelerator profile additionally includes `CONDA_PREFIX`,
CUDA/target architecture, code-generation, host-compiler, and extra NVCC flags
for GPU builds. Keeping the profiles separate lets an accelerator/profile
switch rebuild CGo without needlessly rebuilding unaffected thirdparties. The
values are hashed to avoid putting arbitrary paths or flags in the stamp. Jobs
and download-source locations are excluded because they do not change accepted
output semantics; ONNX bytes are already checksum-pinned.

### I2 — one root Make invocation has one thirdparty owner

`cgo-native-thirdparties-internal` owns the thirdparty build when a native
consumer is requested. The public `thirdparties` goal owns it only when it is
invoked without a native consumer. If both goals appear on one command line,
the public goal waits for the requested native consumer and has no second
recipe.

A release root and a debug root cannot share one Make invocation because Make
target-specific variables would otherwise race to define the shared `cgo`
target. The invocation fails before writing artifacts.

This serializes owners inside one Make process. Two independent Make processes
writing the same checkout remain unsupported; callers must serialize them.

### I3 — publication is fail-closed and atomic per library

`cgo/mo-stage-native-libs` is the sole runtime-library staging owner. For each
changed regular file or symlink it:

1. creates `<destination>.tmp.<pid>` in the destination directory;
2. completely copies or links the temporary;
3. renames it over the destination as the publication linearization point;
4. clears the owned temporary name.

The script uses `set -eu`, reports any operation failure, and has one cleanup
trap for normal error, HUP, INT, and TERM. Therefore a failed operation cannot
be masked by a later item and cannot replace the previous complete file.

The runtime library directory is intentionally flat. Unexpected source or
destination directories fail closed rather than attempting a non-atomic
recursive replacement. The next owner removes stale generation-local
`<destination>.tmp.*` files before considering that destination.

### I4 — a stamp is a commit record, never an intention record

The state machine is:

```text
                    clean required outputs
no/invalid stamp ------------------------------+
       |                                        |
       v                                        v
   prepare -> .pending -> begin -> .building -> build -> record -> stamp
                    |              |                |
                    +-- failure ---+----------------+
                                   next prepare escalates cleanup
```

`prepare` calculates `local`, `reuse`, `rebuild-cgo`, or `rebuild-all` and
writes a pending marker. `begin` proves required cleanup and moves it to the
building marker. `record` verifies source/key/output stability and publishes
the stamp last. A pending/building marker means the generation is incomplete
and cannot be reused.

Stamp format 6 records the structured key. Older formats fail closed and cause
one full rebuild instead of guessing compatibility.

### I5 — advertised debug semantics reach every GPU compiler

Top-level GPU debug propagates to both sub-builds:

- `cgo/cuda`: `dbg=1`, `-g -G`, and `-O0` with no later `-O3`;
- `cgo/cuvs`: the `debug` target, `-O0 -g`, and no `-O3` in compile/link
  commands.

If a future GPU component cannot implement debug semantics, the build must
reject GPU debug before stamping it. Silently compiling an optimized component
under a debug key is forbidden.

## 3. Ownership and unhappy paths

| Resource/state | Owner | Completion point | Failure cleanup | Recovery |
| --- | --- | --- | --- | --- |
| thirdparty build in one root Make | native internal target or standalone public target | recursive Make success | recursive Make returns non-zero | next invocation rebuilds per provenance action |
| staged temp library | `mo-stage-native-libs` process | same-directory rename | EXIT/signal trap removes owned temp | next owner also recycles stale temps |
| pending marker | provenance `prepare` generation | rename to `.building` | retained as evidence | next `prepare` escalates cleanup |
| building marker | provenance `begin` generation | successful `record` | retained as evidence | next `prepare` escalates cleanup |
| reusable stamp | provenance `record` generation | atomic rename of complete stamp | temporary stamp removed by trap | invalid/mismatched stamp forces rebuild |

Q1 normal completion has one owner and one publication point. Q2 every
reachable error or signal returns non-zero and retains enough marker state for
the next invocation to clean conservatively. Q3 retained marker/temp state has
an automatic recycler; no human-only recovery is part of the supported path.

## 4. User and platform contracts

- macOS reuses `libmo.dylib`; Linux reuses `libmo.so`.
- CPU and GPU artifacts never alias.
- Release and debug artifacts never alias.
- SIMSIMD and non-SIMSIMD thirdparty artifacts never alias.
- Supported custom compiler/SDK/CUDA/flag profiles never alias the default or
  one another. Reuse requires the linked test to present the same profile.
- Dirty native inputs can build for local use but never publish reusable
  provenance.
- Exported/non-Git source keeps normal local incremental behavior and never
  publishes cross-worktree provenance.
- `make -n`, `make -t`, and `make -q` do not mutate generation state.
- Paths are quoted, but the existing `mo-cgo-test` primary-worktree reuse path
  rejects whitespace because Go linker flag composition cannot preserve that
  path safely. Local builds remain available.
- Cross-compilation and remote artifact transfer remain outside this contract.

## 5. Performance model

The common unchanged path does not copy runtime libraries. It compares each
flat staged file and keeps the destination mtime unchanged, avoiding needless
downstream relinks. Provenance hashes native outputs at preparation and final
commit so same-clock-tick or mtime-restored mutations cannot pass as reuse; a
valid unchanged generation still performs no compilation or cleanup.

SIMSIMD changes intentionally pay a full thirdparty rebuild because reusing
usearch compiled with different dispatch semantics is incorrect. CPU/GPU or
release/debug transitions clean only CGo outputs when thirdparty source and the
SIMSIMD field match.

No locks, goroutines, polling loops, background processes, or unbounded caches
are added. The staging scan is linear in the small top-level library set and
does not recursively traverse runtime output directories.

## 6. Alternatives considered

### Always rebuild in every worktree

Correct but makes focused CGo tests prohibitively slow and repeats large
thirdparty builds. Rejected because safe reuse can be proven locally.

### Symlink the whole native output tree

Fast but allows one worktree to mutate another worktree's active generation
and provides no source/key boundary. Rejected.

### Derive provenance only from Git commit IDs

Misses semantic environment inputs and ignored artifact corruption. Rejected.

### Lock all native builds globally

Would require stale-lock ownership, process identity, timeout, and filesystem
portability rules. It does not solve artifact identity. Rejected for this PR;
independent root Make processes remain an explicit unsupported concurrency
boundary.

### Remote/content-addressed artifact service

Potentially useful at fleet scale, but introduces trust, retention, toolchain,
and distribution concerns. Out of scope.

## 7. Validation and acceptance

Native Dependency Checks must run on Linux and macOS when either native build
code or the `mo-dev` CGo scripts change. Acceptance is:

1. shell syntax validation for provenance, staging, wrapper, and tests;
2. deterministic provenance transitions, including interrupted generations,
   content corruption, release/debug, CPU/GPU, SIMSIMD, and custom environment
   profiles;
3. staging fault injection for copy, rename, and signal failure, proving the
   old destination survives and temporary state is recycled;
4. `make -n -j2 thirdparties cgo` and `thirdparties debug` each expose exactly
   one thirdparty build owner;
5. mixed release/debug root goals fail before build execution;
6. GPU debug dry-runs propagate to CUDA and cuVS and contain `-O0` without
   `-O3`;
7. linked-worktree wrapper tests reject source, platform, accelerator, and
   SIMSIMD mismatches;
8. existing amd64/arm64 native container builds continue producing readable
   `libmo` and thirdparty libraries.

Real GPU compilation remains dependent on a CUDA/cuVS runner. The hermetic
tests prove configuration propagation on every PR; GPU-enabled CI or a
maintainer environment remains responsible for compiler/toolchain acceptance.

## 8. Rollback

The reuse feature can be rolled back by removing primary-worktree selection in
`mo-cgo-test`; local artifacts continue to work. Stamp format 6 files are
ignored build metadata and can be deleted safely. The atomic staging helper is
independently useful for normal builds and does not require cross-worktree
reuse.

## 9. Revision history

- Revision 1: replaces the composite variant with a structured key; establishes
  one Make owner; adds atomic staging and recovery; closes GPU debug semantics;
  and wires deterministic contracts into native CI.
