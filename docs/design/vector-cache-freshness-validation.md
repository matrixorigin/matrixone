# Index-cache freshness validation (#27632)

## Scope and provenance

- Base/merge base: `bde7e0cad87c3b125e92795afdd4dd8af0d4b115`.
- Design gate: GPT-6 medium approved design-only commit
  `4abb7a2a1bce7285ebc1e0cbd999f28151eb4152` before implementation; also approved
  deleting the obsolete mark-then-housekeeping state.
- Production source SHA-256 (`pkg/vectorindex/cache/cache.go`):
  `3dd9e5673e96514661ef2c7863b51dc41fc8b1184d71e6a5c30c63aceec83c71`.
- Candidate binary SHA-256:
  `5e4c5540755b531f6f94e7a15fb48f5b59b1697e6bc2868a7f9f356b580fe58c`.
  Built after the production patch using `make build-with-prebuilt-native`;
  the version string still names the design commit. Later changes were tests/docs.
- Linux/amd64, Go 1.26.4; repository-provenance-checked CPU native artifacts.
  All Go tests use `.agents/skills/mo-dev/scripts/mo-cgo-test`, explicit TMPDIR,
  `GOPROXY=https://proxy.golang.org,direct`, and readonly module mode. No native
  input, algorithm interface, SQL format, catalog format or GPU kernel changed.

## Change/risk and ownership map

| Closure | Risk / owner | Evidence |
| --- | --- | --- |
| Scheduling and immediate generation retirement | R3 / generic cache loop and eviction claimant | Channel/synctest UT, race, native two-CN QA |
| Checker lifetime, replacement and shutdown | R3 / entry RWMutex, expected identity, eviction CAS | Blocked-check/search/Destroy, ABA, running shutdown UT |
| Idle TTL and immutable snapshots | R2 / existing TTL claim and snapshot key | Retained TTL renewal oracle, snapshot sweep UT and SQL BVT |
| Warm-cache SQL consumer behavior | R2 / async HNSW result contract | F32/F64 native oracle and warm async BVT |
| Design/evidence artifacts | R0 | Exact design review and delivery diff |

Q1: the loop stops both tickers; the sweep releases read locks before eviction;
the existing claimant alone destroys an entry. Q2: metadata reads retain their
existing one-minute SQL timeout, while native searches can delay destruction;
there is no read-to-write lock upgrade or checker-to-same-entry callback cycle.
Q3: one sweep, one sequential retirement and one O(N) candidate slice per cache;
busy ticks do not create worker or retirement queues. These checks do not assert
a new bound on total native work or cache shutdown latency.

## Go and incremental static validation

- Focused selection: all seven `TestFreshness*` tests, snapshot sweep exemption,
  and TTL-renewal versus generation-eviction regression. Normal runs passed.
- Whole owning cache, normal: PASS (72.687s); final added shutdown test also
  independently exercised. Existing package evidence is reused because only its
  new test's acknowledgment barrier changed after that whole normal run.
- Race measurement: nine exact tests passed; per-test JSON elapsed was below
  resolution. `N=clamp(floor(30/T),1,100)` therefore selected 100 for each.
  All nine exact-name `-race -count=100 -timeout=120s` runs passed.
- Whole owning cache: `-race -count=1 -timeout=240s`, PASS (8.942s).
- Real CPU checker consumers: HNSW's uncheckable/error/content-change tests (3),
  FULLTEXT2's uncheckable/error tests (2), PASS with nonempty selections.
- Incremental SCA: derive changed Go packages from merge base plus all local and
  untracked Go files; gofmt, go vet and golangci-lint 2.6.2
  `--new-from-rev <base> ./pkg/vectorindex/cache`, PASS, 0 issues. Direct CPU
  consumers `./pkg/vectorindex/hnsw ./pkg/fulltext2`: vet/lint PASS, 0 issues.
  This is not a repository-wide SCA or CI-green claim.

The added running-shutdown test initially assumed the unbuffered done send also
acknowledged the receiver's later exit store. Race mode disproved that test
assumption. A separate loop-exited channel now provides the actual acknowledgment;
the production protocol was not broadened to make the test pass.

## Independent-process SQL QA

Both base and candidate used four separate `mo-service -cfg` processes: LOG,
TN, CN1 (reader port 27001), CN2 (writer port 27002). Shared DISK-V2 fileservice,
private role data directories, distinct service ports, no proxy. Same-process CN
fixtures were deliberately excluded because they share the process cache.

For each of VECF32(3) and VECF64(3):

1. Insert eleven rows `(id, '[id,0,0]')`, ids 1..11. Create HNSW `vector_l2_ops`,
   M=64, EF_CONSTRUCTION=200, EF_SEARCH=200, MAX_INDEX_CAPACITY=3, FORCE_SYNC.
   Assert four metadata models, and EXPLAIN includes `hnsw_search` on reader.
2. Warm reader with `ORDER BY l2_distance(v,'[7.25,0,0]') LIMIT 4`:
   expected ids `[7,8,6,9]`.
3. Writer transaction: update id11 to `[7.25,0,0]`, delete ids5/6, insert id12
   `[7.3,0,0]`, commit; `ALTER TABLE t ALTER REINDEX ix HNSW FORCE_SYNC`.
   Copy current rows into a separate unindexed oracle table.
4. Reader metadata must show a changed sorted checksum set and still four
   models. Writer indexed control and reader unindexed oracle must both return
   `[11,12,7,8]`. Poll the already-warm reader without resetting its cache.

| Binary / clean catalog round | F32 | F64 |
| --- | --- | --- |
| Base | Still `[7,8,9]` after 45s | Still `[7,8,9]` after 45s |
| Candidate 1 | Correct after 28.904s | Correct after 24.738s |
| Candidate 2 | Correct after 10.569s | Correct after 19.492s |

Times are measured from completion of the writer's publication/oracle SQL call,
not a strict commit-time SLA. Polling was one second with a 120s candidate cutoff.
Base observations prove the short-window difference, not permanent staleness.
Missing id6 in the old result is excluded by the current base-table join; it does
not prove that the ANN model was refreshed.

Both candidate rounds then matched the unindexed oracle for LIMIT 1/4/20/0 and
LIMIT 2 OFFSET 2, including deletion exclusion. A rolled-back UPDATE/DELETE left
the metadata checksum set and indexed answer unchanged. Three further warm reads
were stable. Each typed database was dropped in a finally cleanup before reuse.

## BVT and review status

Normal comparison (`method=run`, standard `nometa ignore pprof` flags) passed
twice on the same candidate cluster, with zero failures, ignores or abnormal cases:

| Case | Round 1 | Round 2 |
| --- | --- | --- |
| vector_hnsw_async.sql | 79/79, 55.847s | 79/79, 49.871s |
| vector_hnsw_snapshot.sql | 27/27, 14.073s | 27/27, 7.561s |

All four commands exited zero. Case-owned databases/snapshot were dropped by
each successful case; the final catalog check found zero test databases and no
snapshots. The same-instance second pass independently exercised clean recreation.

The async case reuses its existing three-row F32/F64 fixture, warms both models,
and polls all six unchanged INSERT/UPDATE/DELETE result oracles. The empty-index
fixture additionally polls all three endpoint results: its first metadata model
can precede the final CDC transaction. The 10k+10k bulk-fragment fixture is retained.
New result blocks use the documented metadata/column separators and no terminal
row separator on the final row. No generated result was blindly accepted.

Raw local logs, topology configs and the reusable SQL QA driver are retained in
`/home/xupeng/mo-worktrees/issue-27632-evidence` on the validation host.

GPT-6 medium final whole-change review inspected the raw logs and the complete
committed/local diff: no production-code blockers; CPU/native/BVT closure passed.
Final decision is **REQUEST_CHANGES solely for the unmet GPU validation gate**.
The mo-dev CGo/GPU contract requires the whole GPU test set when shared vector
code changes; unchanged GPU interfaces do not waive it. Push/PR is paused pending
a suitable GPU environment or an explicit exception from the user/policy owner.

Candidate processes were stopped in CN1+CN2, then TN, then LOG order. All four
process sessions exited zero and logged shutdown completion. No test-owned service
was left running. This supplements, rather than replaces, the deterministic
running-shutdown/checker concurrency test.

## Remaining operational limits

- Approximately 20x more checker calls; one metadata SQL per HNSW check, normally
  two per FULLTEXT2/CAGRA/IVF-PQ check. Slow catalog operations, many resident
  indexes, active readers and housekeeping can exceed the healthy 30s window.
- Existing true+error checker eviction is preserved; catalog failures may cause
  more reloads/logs. No hot-query metadata SQL was added.
- No CUDA compiler or GPU is available on this host. GPU-tagged compilation and
  CAGRA/IVF-PQ execution remain unverified; CPU evidence is not a GPU pass.
- No disk/wire migration: binary rollback restores the old polling cadence.
  Native shutdown evidence must stop CNs before TN/LOG; the baseline's simultaneous
  role termination caused a CN heartbeat deadline during cleanup, not a successful
  graceful-shutdown observation.
