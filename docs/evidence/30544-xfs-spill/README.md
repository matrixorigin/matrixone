# XFS join-spill validation evidence

This bundle supplies the retained execution evidence requested by
[PR 30637 review 5580983721](https://github.com/matrixorigin/matrixone/pull/30637#pullrequestreview-5580983721).
The runs took place on 2026-10-20, before publication of commit
`ef10036960a7d75edaa53bd5496502d2ddb64a21`. This is evidence publication,
not a claim that the tests were rerun for this documentation change.

## Evidence map

| Requirement | Retained evidence |
| --- | --- |
| Non-skipped production XFS allocation test | [xfs-writer-tests.log.txt](xfs-writer-tests.log.txt): `TestWritePreextendedXFSAllocation` and roundtrip PASS, three executions, no SKIP; Linux/amd64 and CPU recorded |
| Real automatic-capacity SQL before/after | [before](before-192MiB-pass.log.txt) and [after](after-192MiB-pass.log.txt): initial cap, sampled allocation/free space, terminal result and diagnostic PASS |
| Input, SQL, filesystem and assertions | [SQL diagnostic](issue30544_xfs_diagnostic_test.go.txt) and [historical launcher](run-xfs.sh.txt) |
| Diagnostic observation boundary | [shim](issue30544_diagnostic.go.txt): the SQL fixture calls only `Issue30544BudgetSnapshots`; its separate modeled-refresh function is unused |
| Source applicability | [source-identity.json](source-identity.json) and the equivalence command below |
| Command completion and cleanup | [execution-receipts.json](execution-receipts.json), SQL fixture assertions, and cleanup qualifications below |
| Limited performance observations | [writer log](xfs-writer-tests.log.txt), [benchmark source](append_bench_test.go.txt), [roomier launcher](run-xfs-roomy.sh.txt), and the four `roomy-sql` excerpts |

The SQL logs contain **extracted diagnostic lines**, copied byte for byte from
retained artifacts. They are not full verbose server logs. The writer log is
the complete retained test/benchmark output. Sources and historical shell
scripts carry `.txt` suffixes so this archive adds no compiled tests or automatic
execution. `SHA256SUMS` covers every other file in this directory.

## Filesystem, workload and execution

The SQL experiment used Linux/amd64, Go 1.27.1, four CPUs (`0,2,4,6`),
`GOMAXPROCS=4`, and `GOMEMLIMIT=4GiB`. Each variant received a fresh private
17 GiB sparse image formatted by `mkfs.xfs -f`, mounted with `mount -o loop`
in an `unshare --mount --propagation private` namespace. Ballast brought actual
initial available space to 16 GiB + 192 MiB (17,381,158,912 bytes).
Only LOCAL used this filesystem; CSV, SHARED and embedded-cluster state were
outside it. The initial automatic CN spill cap was 201,289,728 bytes in both
runs. No statfs substitution or explicit disk cap was used.

The fixture creates 400,000 unique rows with `a=i/1000`, `b=i%1000`, and
384 copies of `x`, into `(a int,b int,p varchar(384),primary key(a,b))`.
It selects one CN, `max_dop=4`, `join_spill_mem=1048576`, ordinary LOAD with
`parallel 'false'`, and `ProcessLimitationSpillSize=0`. EXPLAIN must contain
hash shuffle. A second SQL connection repeatedly runs
`select sum(result) from generate_series(1,100) g`, creating real processes
that invoke the production automatic-capacity refresh path. The observer
reads budget snapshots and actual file `st_size`, `st_blocks*512`, and statfs.

Historical compilation, from the dedicated source checkout, used the repository
wrapper (with the native build already complete):

```sh
env PATH=/usr/local/go/bin:$PATH GOPROXY=https://goproxy.cn,direct \
  GOMODCACHE=/home/github/go/pkg/mod GOCACHE=/home/github/.cache/go-build \
  GOMAXPROCS=4 GOFLAGS=-p=4 nice -n 10 taskset -c 0,2,4,6 \
  .agents/skills/mo-dev/scripts/mo-cgo-test -tags issue30544diag \
  -c -o ../xfs-before.test ./pkg/tests/issues/isolated
# Same command with -o ../xfs-after.test after applying the writer correction.
unshare --mount --propagation private bash ../run-xfs.sh before fail
unshare --mount --propagation private bash ../run-xfs.sh after pass
```

To reconstruct the historical fixture in an isolated checkout, copy the archived
SQL diagnostic to `pkg/tests/issues/isolated/issue30544_xfs_diagnostic_test.go`
and the shim to `pkg/vm/process/issue30544_diagnostic.go`. The launcher expects
both binaries one directory above the checkout. These are historical,
environment-specific commands requiring a private scratch filesystem and mount
privileges, not commands to run against an existing service.

The separate production writer test/benchmark command was:

```sh
env PATH=/usr/local/go/bin:$PATH GOMODCACHE=/home/github/go/pkg/mod \
  GOCACHE=/home/github/.cache/go-build GOMAXPROCS=4 GOFLAGS=-p=4 \
  TMPDIR=/data3/tmp/issue30544-fix/tmp nice -n 10 taskset -c 0,2,4,6 \
  .agents/skills/mo-dev/scripts/mo-cgo-test -v -count=1 -timeout=2m \
  -run '^TestWritePreextended' -bench '^BenchmarkAppendXFS$' \
  -benchtime=1025x -count=3 ./pkg/sql/colexec/spillio
```

The last `-count=3` is effective. The archived benchmark source was present as
`pkg/sql/colexec/spillio/append_bench_test.go`; TMPDIR was on the host XFS
filesystem. The allocation test checks XFS magic and would explicitly skip
another filesystem. Its retained output shows three actual PASS results.

## Results and cleanup

| Observation | Original writer | Preextended writer |
| --- | ---: | ---: |
| SQL result | 3015 admission error, rollback to zero rows | 400,000 rows |
| Terminal rejection | requested=117712, used=115348584, limit=115348584 | none |
| Sampled peak logical spill bytes | 114,461,448 | 176,386,048 |
| Sampled peak allocated bytes | 205,127,680 | 176,664,576 |
| Diagnostic terminal result | PASS, 5.43 s | PASS, 6.16 s |

Peak counters are independently sampled maxima, not an atomic filesystem/ledger
snapshot. The original stops early. Its LOAD duration cannot serve as a
throughput comparison with the completed patched LOAD.

The fixture asserts rollback on expected failure, or row count and checksums
`sum(a)=79800000`, `sum(b)=199800000` on success. Both paths then assert zero
spill disk/FD ledger usage, zero remaining spill files, and usable SQL via
`select 42`. These assertions execute **after** the `RESULT` log line and
**before** terminal PASS; their values were not individually printed in the
excerpts. The deferred database/cluster cleanup also runs before PASS.

The launcher installs an EXIT trap to unmount and remove its exact image.
Retained rollout receipts report `baseline_exit=0`, `fixed_exit=0`, and
`writer_exit=0`. At 03:43:13 UTC the recorded cleanup probe removed eight closed
fixture directories and queried remaining task mounts, images and test
processes; output reports `removed_closed_fixture_dirs 8` and no matching
mount/image/process entries. The receipt includes the printed production hashes.
The archived SQL excerpts themselves do not independently log this outer
cleanup; no fresh remote cleanup inspection is claimed here.

Historical paths in logs/scripts are `/data3/tmp/issue30544-fix`. A later task
record reports relocation to `/data4/issue30544-fix` at 04:02:43 UTC after
checking file hashes; this archive preserves the original paths. Full verbose
SQL logs were retained remotely, but are not included or freshly fetched here.

## Source identity and limits

The causal SQL comparison is original issue revision
`65bcea5d24610dde1a21bb9f4dea1efd82a8ce50` versus that revision plus the three
production-file correction. It preserves the historical full-row LOAD plan.
PR base `309f618995af4ee23e3b9149365f4af77264090d` already contains the later
key-only LOAD planning change. It would be incorrect to call these SQL runs
current-HEAD full-row LOAD executions.

The following comparison was rechecked while preparing this bundle and has an
empty diff (exit 0):

```sh
git diff --exit-code 65bcea5d24610dde1a21bb9f4dea1efd82a8ce50 \
  309f618995af4ee23e3b9149365f4af77264090d -- Makefile cgo thirdparties \
  pkg/sql/colexec/hashbuild/spill.go pkg/sql/colexec/spillutil/join_spill.go \
  pkg/sql/colexec/spillio
```

All three patched production-file SHA-256 values printed by the Linux runner
match PR head `ef10036960a7d75edaa53bd5496502d2ddb64a21`, as recorded in the
manifest. This source equivalence bridges the historical mechanism witness to
the current writer correction. Current-base package tests and public LOAD
PK/rollback/recovery evidence are separate; latest exact-head CI
[39366532580](https://github.com/matrixorigin/matrixone/actions/runs/39366532580)
passed. The earlier cancelled run is not a product failure.

The raw 64 KiB append benchmark median changed from 29.351 to 32.867 microseconds
(about 12% overhead). The two completed SQL comparison pairs used 16 GiB +
512 MiB initial free space so both versions could finish: before
1.593171879/1.418035806 s, after 1.303999257/1.459804525 s. All four diagnostics
passed. These bounded observations establish neither a stable speedup nor
large-scale throughput. No original TPCH SF1000 replay, adjacent-commit bisect,
or coverage of unusual XFS extent/realtime configurations is claimed.
