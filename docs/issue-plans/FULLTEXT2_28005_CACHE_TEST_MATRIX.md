# FULLTEXT2 #28005 cache test matrix

This matrix covers the cache contract added for #28005. It separates the
developer executable checks from deployment and QA checks that require a live
CN/Proxy topology. The tests are test-only; no product code or protocol is
changed.

## Frozen inputs

| Item | Value |
| --- | --- |
| MatrixOne source | `b922526ecb40f70e02f59e68b83ba3996a7325e5` |
| Native CGo artifact | `cgo/libmo.dylib`, SHA256 `3f68244e6ba8f75bc24947bd14ba7c0b41620ae9b8a5eed6e64c9d0b3c549cc9` |
| Candidate CGo wrapper | `.agents/skills/mo-dev/scripts/mo-cgo-test`, SHA256 `eab1f3b8d0622bec00e86b9f4ded7f1718e7d78f21f69b7442138ec9d1e981ea` |
| Test worktree | `/Users/violet/.codex/worktrees/ft2-qa-sql-20260911` |
| Runner checkout | `/tmp/ft2-qa-mo-tester-20260911`, SHA `334d3e124bbfbdcd6b5cc63439278d172c72b706` |
| Runner jar | SHA256 `f60ecd676ba52b8ea6b2d4f79130257929bab6057378141c90ce25aa30166a8c` |
| Cache key | The writer's exact `w.cfg.IndexTable`, `__store` in the unit fixture |
| CDC input | A non-empty `Fulltext2SqlWriter` INSERT encoded by `ToSql`, decoded and tokenized by the real `RunFulltext2`/`TailBuilder` |

## Acceptance mapping

| Case ID | Acceptance requirement | Executable entry and assertion | Developer result | QA remaining |
| --- | --- | --- | --- | --- |
| FT2-28005-CACHE-001 | A non-empty CDC flush must not evict or refresh a warm cache entry. | `pkg/iscp/fulltext2_consumer_cache_test.go`, `TestRunFulltext2KeepsWarmCacheOnNonEmptyCDCFlush`: real writer blob, real `RunFulltext2`, exact-key warm cache identity, recorded tail `INSERT`, watermark callback, and a commit channel barrier. A reader completes while commit is blocked; candidate `Load` and warm `Destroy` counters stay zero. | **PASS**. RC 0, `ok pkg/iscp 1.645s`; race RC 0, `ok pkg/iscp 2.782s`. Logs: `/tmp/ft2-cache-cgo-final3-20260911.log`, `/tmp/ft2-cache-race-final2-20260911.log`. | Live TKE must repeat with a real CN/Proxy and durable index tables, then verify the service's deployed cache generation. |
| FT2-28005-CACHE-002 | A later ordinary cache sweep may retire the entry and load a replacement. | Same test: explicitly expires the warm wrapper, calls `HouseKeeping`, asserts the original backend `Destroy` count is one, then searches with a replacement backend and asserts one replacement `Load` plus new identity. | **PASS**, included in the case above. | TKE must repeat after the permitted visibility boundary and record the generation/load transition. |
| FT2-28005-CACHE-003 | A base mmap acquired before a tail-load failure must be released before the load returns. | Linux-only `pkg/fulltext2/storage_mmap_linux_test.go`, `TestFulltext2SearchLoadTailFailureReleasesOwnedMmap`: valid serialized base, real `LoadAllBases`/`LoadFromStorage`, tail SQL failure, `/proc/self/maps` records the newly acquired `ft2idx` mapping before the failure and waits with a deadline for those exact lines to disappear. | **NOT_RUN (macOS)**. The Linux build and `/proc/self/maps` execution remain pending on a Linux CGo lane. The portable existing `TestFreeSegsReleasesMmap` was separately run PASS. | Run this case on Linux and retain the before/after map artifact; do not substitute RSS or a nil slice. |
| FT2-28005-CACHE-004 | Waiting-reader cancellation, failed replacement, and cleanup remain bounded. | Existing `pkg/vectorindex/cache` cases: `TestSearchCancelledReaderDoesNotWaitForAnUnrelatedReader`, `TestSearchIntoCancelledReaderDoesNotWaitForAnUnrelatedReader`, `TestVectorIndexCacheSearchRetriesSupersededLoad`, `TestVectorIndexCacheSearchIntoRetriesSupersededLoad`, and `TestVectorIndexSearchWaitersRetrySupersededLoad`. | **PASS**. RC 0, `ok pkg/vectorindex/cache 1.009s`; log `/tmp/ft2-cache-generic-20260911.log`. | QA should repeat the same cases in the target build and include cancellation during actual CN load/replacement. |
| FT2-28005-CACHE-005 | MERGE, REBUILD, CN restart, Proxy restart, query cancellation, and load failure must recover and return a correct first query. | Existing TKE lifecycle harness is the required entry; this worktree adds no fake substitute for those deployment transitions. | **NOT_RUN**. | Run each operation with an operation return, service recovery, and first-correct-query terminal record. |
| FT2-28005-CACHE-006 | 129 may report warm-read performance; CDC concurrency and lifecycle stay in integration. | mo-load/129 workload and adapter. No 129 changes are in this cache worktree. | **NOT_RUN**. | Establish the fixed-corpus warm-read baseline and report drift/ratio; do not claim a performance acceptance from this unit case. |

## Commands and evidence

The candidate-local CGo wrapper was used so its repository identity resolves to
this worktree. Raw output is retained in the paths above.

```sh
cd /Users/violet/.codex/worktrees/ft2-qa-sql-20260911
./.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 \
  -run '^TestRunFulltext2KeepsWarmCacheOnNonEmptyCDCFlush$' ./pkg/iscp
./.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=1 \
  -run '^TestRunFulltext2KeepsWarmCacheOnNonEmptyCDCFlush$' ./pkg/iscp
./.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 \
  -run '^(TestSearchCancelledReaderDoesNotWaitForAnUnrelatedReader|TestSearchIntoCancelledReaderDoesNotWaitForAnUnrelatedReader|TestVectorIndexCacheSearchRetriesSupersededLoad|TestVectorIndexCacheSearchIntoRetriesSupersededLoad|TestVectorIndexSearchWaitersRetrySupersededLoad)$' \
  ./pkg/vectorindex/cache
./.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 \
  -run '^TestFreeSegsReleasesMmap$' ./pkg/fulltext2
./.agents/skills/mo-dev/scripts/mo-cgo-test -vet=all -run '^$' -count=1 \
  ./pkg/iscp ./pkg/fulltext2 ./pkg/vectorindex/cache
```

The focused vet-only command returned **RC 0** (`/tmp/ft2-cache-vet-20260911.log`);
the `[no tests to run]` output is deliberately not counted as a test PASS.

The ordinary host `go test` path was attempted and is
`BLOCKED_ENVIRONMENT` because the local CGo symbols (`C.mo_cbitmap_*` and
`C.mo_croaring_*`) are unavailable without the wrapper/native artifacts. A
Linux cross-build with `CGO_ENABLED=0` was also not treated as evidence: its
platform-dependent `gojieba` and allocator CGo dependencies do not compile.

The five existing cache cancellation/replacement cases provide reusable
bounded-wait and cleanup coverage. The new #28005 case deliberately holds the
commit only until the warm read completes, releases the barrier in cleanup, and
joins `RunFulltext2` before destroying the test cache. The Linux mmap case has
the same explicit one-second observation deadline. No test infers cache
invalidation, mmap release, or correctness from elapsed time or RSS.

## Final consolidated execution

After the cleanup-order review repair, focused CGo execution exited 0 (`/tmp/ft2-cache-cgo-final7-20260911.log`). The final test file SHA256 is `ec4559a6e17622fbe73983f9b7bf988b4fbabc90189751793f852bb924fae29c`. Focused race sampling, 100 repetitions of the exact test under race, and the owning `pkg/iscp` race package all exited 0. Evidence: `/tmp/ft2-cache-adaptive-sample-20260911.jsonl`, `/tmp/ft2-cache-adaptive-stress-20260911.log`, `/tmp/ft2-consolidated-iscp-race-20260911.log`. The sample's per-test elapsed value rounded to zero; the adaptive count used its cap of 100 for the 30-second budget. Earlier focused logs above are historical, not the final cleanup revision's race binding.

A read-only Go overlay restoring the historical non-empty-CDC cache Remove returned exit 1 at the warm-cache Destroy assertion (`/tmp/ft2-cache-preflight-control-20260911/`). The overlay was never applied to the candidate. This establishes detection of the old invalidation mechanism; it does not substitute for deployed CDC or lifecycle execution.
