# P1 Linux probe execution review

Reviewed before dispatch on 2026-10-29. Manifest is
`native_rebase_probe_manifest.json`; supervisor SHA256 is
`fedd02bfe62477acc3b1f964861980cceef89cc44b63fd2e0bfcd889a7c187f3`.

Decision: PASS for the bounded prerequisite experiment, NOT production design
approval or P1 completion. Offline checks: 10 original model, 6 frozen-adapter
method and 4 wrapper boundary/terminal tests pass. The Go test is formatted but
has not run; local CGo command stopped at native-artifact admission.

Execution: only personal Ubuntu 100.89.164.105. Reserve the shared cooperative
lease atomically after verifying no owner/build/database actor and >=22 GiB
available memory / >=40 GiB disk. Stage exact source archive and Go test after
reservation. Start only this run's supervisor, which starts one owned worker.
Do not contact 129/55 or alter unrelated sources, processes or data.

Worker: 4 logical-CPU quota, 16 GiB memory, zero cgroup swap, 1200 second runtime
limit and process-group stop. Monitor available memory and free disk every five
seconds; stop for three samples below 4 GiB or any disk sample below 12 GiB.
Host swap alone is not a failure gate. No performance comparison is made.

Own one source tree and fresh output/cache. Reuse only the previously qualified
compiler/rootfs/module-cache toolchain and checksum-matched ONNX input. A native
library is built from selected main, not borrowed from the old binary. Build
and Go test output remain in build-test.log. Exact test and both subtest PASS
markers are required; exit zero or empty selection cannot qualify.

Success/error/timeout: stop exact owned worker, verify MainPID=0 and inactive or
failed terminal state, compare full lease identity, release only owned lease.
Keep source, failed output and raw logs. A failed stop/ownership check retains
the lock for observation; never force-release. Transfer or launch failure before
supervisor admission must be inspected and cleaned explicitly, not retried.

The existing MySQL transaction probe can only qualify clone/replay/edge/pin
composition. It does not prove future native coordination, commit failure,
cancellation, GC/drop races or crash recovery. No P2 work is authorized by PASS
of this probe alone. No wrapper changes are permitted after dispatch.

## R2 review after R1 terminal failure

R1 stopped before extraction, build or SQL: a Git archive directory header is
named `source`, not `source/`. Worker MainPID=0, ControlGroup empty, failed;
owned lease released. Preserve its original failure and source payload.

R2 admits exactly this root directory plus source descendants, and rejects
parent traversal/absolute/outside names. Two new tests cover the actual tar
shape and escapes; all 22 offline tests PASS. No SQL/resource changes.
Manifest: `native_rebase_probe_manifest_r2.json`; supervisor SHA256:
`89c2f2fd36c21318131242d679c820b46b1aecf7c4a3b2fde218b55f37c10c79`.
Decision: PASS for one corrected, separately identified probe. Old R1 is not
rewritten, and the R2 wrapper must not be changed while running.

## R3 review after module transport failure

R2 built the selected main's native libraries successfully, then Go failed in
module setup before compiling the test: Ubuntu could not reach proxy.golang.org
for the pinned cpegeric/usearch module. Worker stopped, ControlGroup empty,
lease released. No SQL prerequisite result exists; do not count setup FAIL as
a failed database assertion or PASS.

R3 adds the SAME version's zip/mod/info fetched by the Mac from the official Go
proxy to a private file proxy, with SHA256-bound inputs. Normal go.sum and sumdb
checks remain enabled. No go.mod/go.sum/source/test/SQL/resource changes.
22 offline checks remain PASS. Manifest: `native_rebase_probe_manifest_r3.json`;
supervisor SHA256 `ac4c6f1e12d2e12b84f788f7c69f7e85779c9751b856d5df7f51b488ad0b2384`.
Decision: PASS for one transport-corrected generation. Preserve R1/R2 failures.

## R4: separate commit-fault/restart qualification

R3 passed the real SQL test and both named scenarios; its worker/supervisor
are inactive/MainPID=0, ControlGroup empty, lease released. No P1 completion is
inferred. R4 adds `TestRebaseTransactionRestart` in the isolated owning package:
one private cluster, two two-row tables, existing CN commit-failure hook, a
physical client disconnect before commit, and stop/start on the same data.
It verifies committed data and U pins recover, failed/disconnected destinations
and pins do not. This is graceful service recovery, not process SIGKILL proof.

The shared SQL fixture and isolated fixture run in sequential go test processes;
the latter must not stop the former. Three minutes per test and outer worker
limits remain unchanged. Existing global fault state is restored lexically.
The positive P1 test is unchanged. Only a verified closed PASS R3 compiler cache
is copied, never modified in place; fresh sources and native libraries are built
again. No new source dependency, SQL API or memory quota is introduced.

22 offline checks PASS; new Go test is formatted but execution is pending.
Manifest: `native_rebase_probe_manifest_r4.json`; supervisor SHA256:
`9e5a7eec15eb301af01d8e9c0a71511a92005464a78be49217b2ac05776a732a`.
Decision: PASS for this bounded follow-on test, not production Rebase approval.

## R5: deterministic cancellation boundary

R4 passed both Go roots, including injected commit failure, disconnect and
durable restart. Worker stopped and lease released. R5 adds a separate SQL
fixture case using the EXISTING lifecycle admission hook and admissionPacer.
An autocommit/private-owner database clone pauses at its first child pin after
destination/edge creation. Another session verifies invisibility and commits a
source write, then issues server-side KILL QUERY before releasing the barrier.
Assert error, no destination/child-edge/pin residue and preserved source data.
Cancellation cleanup releases the barrier and joins; no sleep is an oracle.

No new hook, kernel implementation or quota change. Existing R4 tests and SQL
stay unchanged, and an independently copied closed R4 compiler cache is reused.
22 offline checks PASS. Manifest: `native_rebase_probe_manifest_r5.json`;
supervisor SHA256 `d29df811565516f0838606b5d528ad6a27f5f040de59ae1fb04df99aedb3a7cc`.
Decision: PASS for this last bounded substrate qualification; not a four-endpoint
executor, arbitrary GC-race proof or production design approval.

## Terminal review

R5 SQL cancellation/private-owner and prerequisite roots PASS. Its later
isolated cluster startup aborted with native double-free before fixture SQL;
the prior R4 restart PASS must not be promoted to a full R5/P1 PASS. Retain the
abort and stop unchanged retries. No evidence currently attributes this to a
product baseline bug versus the native runtime environment.

All 10 exact owned units are quiescent, no owned lock remains, and five selected
archives have local hash/member readback PASS. Delivery decision: P0 contract
and test scaffolding recorded; P1 PARTIAL/BLOCKED. Production implementation,
P2, public syntax, push/PR and broad benchmark work are NOT completed.
