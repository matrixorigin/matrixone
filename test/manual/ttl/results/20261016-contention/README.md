# Bounded contention in the foreground-write TTL regression

CI run 38757218414, Ubuntu/x86 job 116498799305 at PR head
`586d674988a619d9be7c0783f87c56800a8a6cb7` failed only
`TestTTLConcurrentForegroundWrites` (2.20s). The original job log records eight
consecutive internal-executor `txn need retry in rc mode` failures followed by
the test error. `ci-failure.txt` is the relevant whitespace-normalized excerpt.
Source: https://github.com/matrixorigin/matrixone/actions/runs/38757218414/job/116498799305
Coverage, static checks and active multi-CN BVT jobs passed for that head.

The public DeleteBatch contract limits each call to eight whole transactions.
Under sustained writes, exhausting that budget may return ErrTxnNeedRetry;
the scheduled worker can defer work. The concurrent test incorrectly demanded
success for every call during foreground contention. Accept only this typed
MatrixOne error in that phase and move to the next already-bounded iteration.
Do not broaden production retries or ignore foreground SQL errors. Other
DeleteBatch errors still fail. After joining all writers, every drain call
must succeed, and the original assertions still require exactly 400 odd IDs.
No production behavior, fixture count, row count or loop bound changes.

Validation (repository Go environment and CGo wrapper):

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/tests/sqlintegration -run '^TestTTLConcurrentForegroundWrites$' -count=20
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/sql/ttl -run '^TestTTL' -count=1
```

Both pass. The TTL unit selection includes the deterministic
TestTTLRetryBoundsAndCancellation oracle: exactly eight attempts under persistent
ErrTxnNeedRetry, one attempt for permanent errors, cancellation terminates retry.
The 20 native runs retain the real MySQL foreground writers and final SQL row
oracle; they are not evidence that every run exhausted retries. Existing full
native TTL results for the unchanged production source are retained under
20261015-merge. No new concurrency state or lifecycle was introduced, so no
new race stress selection is needed for this test expectation correction.
Configured package lint and vet results are retained; source/log hashes are
in manifest.json. The self-review scope is the single typed-error branch and
its unchanged channel, join, cleanup and final-drain owners.
